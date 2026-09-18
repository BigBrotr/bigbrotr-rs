#!/usr/bin/env python3
"""Prepare checksum-pinned native archive assets; never install at worker runtime."""
import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import stat
import tempfile
import urllib.request
import zipfile

ROOT = Path(__file__).resolve().parents[1]
REGISTRY = ROOT / 'contracts/release/archive-engine.json'


def native_target():
    targets = {('Linux', 'x86_64'): 'x86_64-unknown-linux-gnu',
               ('Linux', 'aarch64'): 'aarch64-unknown-linux-gnu',
               ('Darwin', 'arm64'): 'aarch64-apple-darwin',
               ('Darwin', 'x86_64'): 'x86_64-apple-darwin'}
    return targets[(platform.system(), platform.machine())]


def copy_verified(source, destination, expected):
    """Bound decompressed/downloaded work by the pinned size, then check all bytes."""
    digest, size = hashlib.sha256(), 0
    # A failed exclusive open confers no ownership: never unlink that path.
    output = destination.open('xb')
    try:
        with output:
            while block := source.read(min(65536, expected['bytes'] - size + 1)):
                size += len(block)
                if size > expected['bytes']:
                    raise ValueError('asset exceeds pinned size')
                digest.update(block)
                output.write(block)
        if size != expected['bytes'] or digest.hexdigest() != expected['sha256']:
            raise ValueError('asset size/hash mismatch')
        destination.chmod(0o444)
    except BaseException:
        destination.unlink()
        raise


def verify_file(path, expected):
    metadata = path.lstat()
    if not stat.S_ISREG(metadata.st_mode) or metadata.st_size != expected['bytes']:
        raise ValueError('asset is not a regular file of pinned size')
    digest, size = hashlib.sha256(), 0
    with path.open('rb') as source:
        while block := source.read(min(65536, expected['bytes'] - size + 1)):
            size += len(block)
            if size > expected['bytes']:
                raise ValueError('asset grew beyond pinned size')
            digest.update(block)
    if size != expected['bytes'] or digest.hexdigest() != expected['sha256']:
        raise ValueError('asset hash mismatch')


def obtain(asset, cache, download):
    destination = cache / asset['sha256']
    if not destination.exists():
        if not download:
            raise ValueError('pinned asset missing from offline cache: ' + asset['sha256'])
        request = urllib.request.Request(asset['url'], headers={'User-Agent': 'BigBrotr-native-bundle/1'})
        # Private per-attempt names cannot poison retries after process death.
        # Keep the temporary on the cache filesystem for atomic publication.
        with tempfile.TemporaryDirectory(prefix=asset['sha256'] + '-', suffix='.partial', dir=cache) as directory:
            temporary = Path(directory) / 'asset'
            with urllib.request.urlopen(request, timeout=45) as source:
                if not source.url.startswith('https://'):
                    raise ValueError('asset redirect is not HTTPS')
                copy_verified(source, temporary, asset)
            try:
                os.link(temporary, destination)
            except FileExistsError:
                # Another downloader may have won. Do not replace its path;
                # the same mandatory verification below decides whether to use it.
                pass
    verify_file(destination, asset)
    return destination


def extract_member(archive, name, destination, expected):
    matches = [entry for entry in archive.infolist() if entry.filename == name]
    if len(matches) != 1:
        raise ValueError('missing or duplicate selected ZIP member')
    member = matches[0]
    mode = member.external_attr >> 16
    if (member.is_dir() or stat.S_ISLNK(mode) or
            member.file_size != expected['bytes'] or '/' in name or '\\' in name):
        raise ValueError('invalid selected ZIP member')
    with archive.open(member) as source:
        copy_verified(source, destination, expected)


def verify_bundle(root, target):
    root = root.resolve(strict=True)
    selected = json.loads(REGISTRY.read_text())['platforms'][target]
    for kind in ['library', 'header']:
        asset = selected[kind]
        verify_file(root / 'lib' / asset['name'], asset)
    for asset in selected['extensions']:
        verify_file(root / 'extensions' / (asset['name'] + '.duckdb_extension'), asset)
    return selected


def prepare_bundle(output, target, cache, download=False):
    selected = json.loads(REGISTRY.read_text())['platforms'][target]
    output.mkdir(parents=True, exist_ok=False)
    cache.mkdir(parents=True, exist_ok=True)
    (output / 'lib').mkdir()
    (output / 'extensions').mkdir()
    archive_path = obtain(selected['archive'], cache, download)
    with zipfile.ZipFile(archive_path) as archive:
        for kind in ['library', 'header']:
            asset = selected[kind]
            extract_member(archive, asset['name'], output / 'lib' / asset['name'], asset)
    for asset in selected['extensions']:
        compressed = {'bytes': asset['compressed_bytes'], 'sha256': asset['compressed_sha256'],
                      'url': asset['url']}
        source_path = obtain(compressed, cache, download)
        with gzip.open(source_path, 'rb') as source:
            copy_verified(source, output / 'extensions' / (asset['name'] + '.duckdb_extension'), asset)
    verify_bundle(output, target)
    receipt = {'format': 'bigbrotr-native-archive-bundle/1', 'target': target,
               'registry_sha256': hashlib.sha256(REGISTRY.read_bytes()).hexdigest(),
               'assets': selected, 'status': 'Prepared and hashed; runtime qualification is separate'}
    (output / 'bundle.json').write_text(json.dumps(receipt, indent=2) + '\n')
    return receipt


def build_environment(bundle):
    """Exact linking and development test loader inputs; not inherited from a shell."""
    library = str(bundle.resolve(strict=True) / 'lib')
    loader = 'DYLD_LIBRARY_PATH' if platform.system() == 'Darwin' else 'LD_LIBRARY_PATH'
    return {'DUCKDB_LIB_DIR': library, 'DUCKDB_INCLUDE_DIR': library, loader: library,
            'BIGBROTR_ARCHIVE_TEST_BUNDLE': str(bundle.resolve(strict=True))}


def install_bundle(source, destination, target):
    selected = verify_bundle(source, target)
    for directory in ['lib', 'extensions']:
        (destination / directory).mkdir(exist_ok=False)
    for kind in ['library', 'header']:
        name = selected[kind]['name']
        shutil.copyfile(source / 'lib' / name, destination / 'lib' / name)
    for asset in selected['extensions']:
        name = asset['name'] + '.duckdb_extension'
        shutil.copyfile(source / 'extensions' / name, destination / 'extensions' / name)
    verify_bundle(destination, target)
    return selected


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--cache', required=True, type=Path)
    parser.add_argument('--target', default=native_target())
    parser.add_argument('--download', action='store_true', help='allow build-time HTTPS downloads')
    arguments = parser.parse_args()
    prepare_bundle(arguments.output.resolve(), arguments.target, arguments.cache.resolve(), arguments.download)
    print(arguments.output.resolve())
