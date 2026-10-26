#!/usr/bin/env python3
"""Assemble a private native candidate from explicit, already-built inputs."""
import argparse
from contextlib import contextmanager
import gzip
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import platform
import re
import shutil
import stat
import subprocess
import sys
import tarfile
import zlib

ROOT = Path(__file__).resolve().parents[2]
sys.dont_write_bytecode = True
sys.path.insert(0, str(ROOT))
from tools.archive_bundle import copy_verified, verify_bundle
from tools.build import git

TARGET = 'x86_64-unknown-linux-gnu'
REGISTRY = 'contracts/release/archive-engine.json'
ASSEMBLY_FILES = ('packaging/release/assemble.py', 'packaging/release/manifest.schema.json',
                  'packaging/release/README.md', 'tools/archive_bundle.py', 'tools/build.py', REGISTRY)
PUBLIC_FILES = ('LICENSE', 'packaging/archive-worker/notices/foldhash-0.1.5.txt',
                'packaging/archive-worker/notices/foldhash-0.2.0.txt',
                'packaging/archive-worker/notices/webpki-roots-1.0.10.txt',
                'packaging/archive-worker/notices/zlib-rs-0.6.8.txt',
                'vendor/libduckdb-sys/LICENSE')
# A finite inclusion hook, populated only with the agreed tracked journey files.
JOURNEY_FILES = ('config/recipes/acceptance.toml', 'docs/tutorials/first-event.md',
                 'packaging/compose/compose.yaml', 'packaging/compose/provision.sql',
                 'packaging/image/Dockerfile')
INFO_KEYS = {'archive_worker', 'capabilities', 'capability_scope', 'debug_info', 'format',
             'inputs_sha256', 'optimization', 'product', 'profile_assertion',
             'protocol_registry_format', 'protocols', 'rustc', 'schemas', 'source', 'target', 'version'}
INPUT_KEYS = {'Cargo.lock', 'rust-toolchain.toml', 'contracts/release/dependencies.json',
              'contracts/protocol/versions.json', 'contracts/protocol/nip01-event.json'}
OPEN_GATES = ['M1-first-launch', 'connected-role-readiness', 'Compose-installation',
              'Linux-arm64', 'release-authentication', 'complete-distribution-notices']


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(value, length=64):
    require(type(value) is str and re.fullmatch('[0-9a-f]{%d}' % length, value), 'invalid digest')
    return value


def relative(name):
    require(type(name) is str and name and '\\' not in name and '\x00' not in name,
            'invalid member name')
    path = PurePosixPath(name)
    require(not path.is_absolute() and all(x not in ('', '.', '..') for x in name.split('/')),
            'noncanonical member path')
    return name


def safe_path(path, *, directory=False):
    """Reject links in every existing component, including the supplied root."""
    path = Path(path)
    require('..' not in path.parts, 'parent traversal is not an input')
    path = path.absolute()
    for parent in reversed(path.parents):
        require(stat.S_ISDIR(parent.lstat().st_mode), 'non-directory or linked parent')
    metadata = path.lstat()
    require(stat.S_ISDIR(metadata.st_mode) if directory else stat.S_ISREG(metadata.st_mode),
            'input is not a regular file/directory')
    return path


@contextmanager
def regular(path):
    path = safe_path(path)
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, 'rb') as source:
        info = os.fstat(source.fileno())
        require(stat.S_ISREG(info.st_mode) and 0 < info.st_size <= 1024**3,
                'invalid selected file size/type')
        yield source, info


def identity(path):
    with regular(path) as (source, info):
        h, count = hashlib.sha256(), 0
        while block := source.read(65536):
            count += len(block)
            require(count <= info.st_size, 'selected file grew')
            h.update(block)
        require(count == info.st_size, 'selected file shrank')
        return {'bytes': count, 'sha256': h.hexdigest()}


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, 'duplicate JSON key')
        result[key] = value
    return result


def read_json(path):
    with regular(path) as (source, info):
        require(info.st_size <= 8 * 1024**2, 'metadata exceeds bound')
        raw = source.read(info.st_size + 1)
        require(len(raw) == info.st_size, 'metadata changed')
    value = json.loads(raw, object_pairs_hook=unique_object)
    require(type(value) is dict, 'metadata must be an object')
    return value, hashlib.sha256(raw).hexdigest()


def blob(value):
    require(type(value) is dict and set(value) == {'bytes', 'sha256'}, 'invalid file identity')
    require(type(value['bytes']) is int and 0 < value['bytes'] <= 1024**3, 'invalid byte length')
    digest(value['sha256'])
    return value


def executable(path):
    # No product execution: require the selected native ELF class/endianness/machine.
    with regular(path) as (source, info):
        header = source.read(20)
        require(len(header) == 20 and header[:7] == b'\x7fELF\x02\x01\x01'
                and int.from_bytes(header[18:20], 'little') == 62,
                'expected Linux x86_64 ELF')
        require(info.st_mode & 0o111 and not info.st_mode & 0o7000, 'invalid executable mode')


def artifact_inputs(artifacts, source_manifest):
    """Shared builder fields are mandatory; formatless lab fields have explicit names."""
    artifacts = safe_path(artifacts, directory=True)
    result, result_hash = read_json(artifacts / 'build-result.json')
    manifest, manifest_hash = read_json(source_manifest)
    require(set(manifest) in ({'format', 'revision', 'tree', 'archive_sha256', 'files_sha256'},
                             {'revision', 'tree', 'archive_sha256', 'files_sha256'}),
            'unknown source manifest shape')
    require(manifest.get('format', 'bigbrotr-source/1') == 'bigbrotr-source/1', 'source format')
    for field in ('revision', 'tree'):
        digest(manifest[field], 40)
    digest(manifest['archive_sha256'])
    files = manifest['files_sha256']
    require(type(files) is dict and 1 <= len(files) <= 20000, 'source file inventory bound')
    for name, sha in files.items():
        relative(name)
        digest(sha)
    require(result['source_manifest_sha256'] == manifest_hash, 'build/source manifest mismatch')
    info = result['build_info']
    require(type(info) is dict and set(info) == INFO_KEYS, 'unsupported build-info shape')
    require(info['format'] == 'bigbrotr-build/1' and info['product'] == 'BigBrotr'
            and info['target'] == TARGET and info['profile_assertion'] == 'release'
            and info['debug_info'] == 'false' and info['optimization'] == '3', 'unsupported build')
    require(info['source'] == {'state': 'builder-supplied', 'revision': manifest['revision'],
            'tree': manifest['tree'], 'dirty': False, 'manifest_sha256': manifest_hash},
            'build-info source differs from supplied manifest')
    require(info['source']['dirty'] is False, 'dirty source')
    require(set(info['inputs_sha256']) == INPUT_KEYS, 'build input keys')
    for name in INPUT_KEYS:
        require(info['inputs_sha256'][name] == files[name], 'build input mismatch: ' + name)
    require(info['capabilities'] == ['event-verification', 'config-static'],
            'capability expansion needs explicit qualification')
    for field in ('version', 'capability_scope', 'rustc'):
        require(type(info[field]) is str and 0 < len(info[field]) <= 4096, 'invalid build text')
    require(info['protocol_registry_format'] == 'bigbrotr-protocol-versions/1', 'protocol registry format')
    for field in ('protocols', 'schemas'):
        require(type(info[field]) is dict and 1 <= len(info[field]) <= 32
                and all(type(k) is str and 0 < len(k) <= 128
                        and type(v) is str and 0 < len(v) <= 256 for k, v in info[field].items()),
                'invalid version map')
    worker = blob(info['archive_worker'])
    standard = result.get('format') == 'bigbrotr-build-result/1'
    if standard:
        record = result['archive_worker']
        require({k: record[k] for k in ('bytes', 'sha256')} == worker, 'worker receipt mismatch')
        registry_hash, assets = record['registry_sha256'], record['assets']
        require(result['helper_sha256'] == files['tools/build.py'], 'build helper mismatch')
    else:
        require('format' not in result and result['source'] == manifest['revision']
                and result['tree'] == manifest['tree'], 'unsupported lab build receipt')
        require(result['archive_worker'] == worker and result['postcheck_status'] == 'PASS',
                'unverified lab worker')
        require(result['archive_verification'] == {'executable': worker,
                'format': 'bigbrotr-worker-verification/1', 'result': 'verified',
                'scope': 'bundle bytes only; no engine, catalog or readiness check', 'target': TARGET},
                'lab worker verification mismatch')
        require(result['lockfile_sha256'] == files['Cargo.lock'], 'lab lockfile mismatch')
        registry_hash, assets = result['archive_registry_sha256'], result['archive_assets']
    registry, current_hash = read_json(ROOT / REGISTRY)
    require(registry_hash == current_hash == files[REGISTRY], 'native registry/source mismatch')
    selected = registry['platforms'][TARGET]
    require(assets == selected, 'native assets differ from registry')
    for name in PUBLIC_FILES:
        require(identity(ROOT / name)['sha256'] == files[name], 'artifact notice differs: ' + name)
    native = result['native_toolchain']
    require(native['os'] == 'Linux' and native['architecture'] == 'x86_64', 'native target mismatch')
    require(set(native['tools']) == {'CC', 'CXX', 'AR', 'LIBCLANG_PATH', 'discovery'}, 'native tool keys')
    native_identity = {'os': native['os'], 'architecture': native['architecture'],
                       'tools_sha256': {k: digest(v['sha256']) for k, v in native['tools'].items()}}
    chosen = {'bigbrotr': identity(artifacts / 'bigbrotr'),
              'archive-worker/bigbrotr-archive-worker': worker}
    require(chosen['bigbrotr']['sha256'] == result['binary_sha256'], 'CLI hash mismatch')
    for kind in ('library', 'header'):
        asset = selected[kind]
        chosen['archive-worker/lib/' + relative(asset['name'])] = blob(
            {k: asset[k] for k in ('bytes', 'sha256')})
    for asset in selected['extensions']:
        chosen['archive-worker/extensions/' + relative(asset['name']) + '.duckdb_extension'] = blob(
            {k: asset[k] for k in ('bytes', 'sha256')})
    # Never recurse through a caller's build tree. Only the selected native subtree
    # is closed; ordinary build outputs and lab test binaries beside it are ignored.
    worker_root = safe_path(artifacts / 'archive-worker', directory=True)
    actual = set()
    def visit(directory):
        for child in directory.iterdir():
            mode = child.lstat().st_mode
            name = child.relative_to(artifacts).as_posix()
            if stat.S_ISDIR(mode):
                require(name in {'archive-worker/lib', 'archive-worker/extensions'}, 'extra native directory')
                visit(child)
            else:
                require(stat.S_ISREG(mode), 'non-regular native member')
                actual.add(name)
    visit(worker_root)
    # prepare_bundle writes this public receipt; install_bundle and the lab do not.
    # Validate it if present, but do not treat a receipt as a runtime asset.
    receipt_name = 'archive-worker/bundle.json'
    if receipt_name in actual:
        receipt, _ = read_json(artifacts / receipt_name)
        require(receipt == {'format': 'bigbrotr-native-archive-bundle/1', 'target': TARGET,
                'registry_sha256': registry_hash, 'assets': selected,
                'status': 'Prepared and hashed; runtime qualification is separate'}, 'native bundle receipt mismatch')
        actual.remove(receipt_name)
    require(actual == set(chosen) - {'bigbrotr'}, 'native closure has missing/extra files')
    for name, expected in chosen.items():
        require(identity(artifacts / name) == expected, 'selected member mismatch: ' + name)
    executable(artifacts / 'bigbrotr')
    executable(artifacts / 'archive-worker/bigbrotr-archive-worker')
    verify_bundle(worker_root, TARGET)
    return chosen, {'binary': chosen['bigbrotr'], 'source': info['source'], 'build_info': info,
                    'build_result_sha256': result_hash, 'source_archive_sha256': manifest['archive_sha256'],
                    'native_toolchain': native_identity, 'registry_sha256': registry_hash,
                    'build_receipt_kind': 'standard' if standard else 'archive-installed-lab'}


def assembly_identity(include_journey):
    require(not git(ROOT, 'status', '--porcelain', '--untracked-files=all'),
            'assembly checkout must be clean and committed')
    revision, tree = git(ROOT, 'rev-parse', 'HEAD'), git(ROOT, 'rev-parse', 'HEAD^{tree}')
    names = (*ASSEMBLY_FILES, *PUBLIC_FILES, *(JOURNEY_FILES if include_journey else ()))
    files = {}
    for name in names:
        actual = identity(ROOT / name)
        committed = subprocess.check_output(['git', '-C', str(ROOT), 'show', revision + ':' + name])
        require(hashlib.sha256(committed).hexdigest() == actual['sha256'], 'assembly input is not committed')
        files[name] = actual['sha256']
    return {'revision': revision, 'tree': tree, 'files_sha256': files,
            'packing': {'python': platform.python_version(), 'zlib': zlib.ZLIB_RUNTIME_VERSION,
                        'archive_format': 'ustar+gzip', 'gzip_level': 6}}


def json_bytes(value):
    return (json.dumps(value, sort_keys=True, indent=2, ensure_ascii=True) + '\n').encode()


def write_json(path, value):
    with path.open('xb') as output:
        output.write(json_bytes(value))
    path.chmod(0o444)


def pack(stage, destination):
    """Stable gzip/tar headers, sorted members, normalized permissions; no host metadata."""
    with destination.open('xb') as raw:
        with gzip.GzipFile(filename='', mode='wb', fileobj=raw, mtime=0, compresslevel=6) as compressed:
            with tarfile.open(fileobj=compressed, mode='w|', format=tarfile.USTAR_FORMAT) as archive:
                for path in sorted(stage.rglob('*')):
                    name = path.relative_to(stage).as_posix()
                    entry = tarfile.TarInfo(relative(name))
                    entry.uid = entry.gid = entry.mtime = 0
                    entry.uname = entry.gname = ''
                    entry.mode = stat.S_IMODE(path.lstat().st_mode)
                    if path.is_dir():
                        entry.type = tarfile.DIRTYPE
                        archive.addfile(entry)
                    else:
                        with regular(path) as (source, info):
                            entry.size = info.st_size
                            archive.addfile(entry, source)
    destination.chmod(0o444)


def assemble(artifacts, source_manifest, output, *, include_journey=False):
    require(type(include_journey) is bool, 'journey selection must be boolean')
    assembly = assembly_identity(include_journey)
    selected, provenance = artifact_inputs(artifacts, source_manifest)
    output = Path(output).absolute()
    safe_path(output.parent, directory=True)
    require(output.name not in ('', '.', '..') and '..' not in output.parts, 'invalid output path')
    # Never create output inside an input/source root or include output from a prior run.
    for root in (Path(artifacts).absolute(), ROOT, Path(source_manifest).absolute()):
        require(output != root and root not in output.parents, 'output overlaps inputs')
    output.mkdir(mode=0o700, exist_ok=False)
    try:
        stage = output / '.members'
        stage.mkdir(mode=0o755)
        members = {}
        def copy(source, name, expected, mode):
            relative(name)
            destination = stage / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            with regular(source) as (stream, _):
                copy_verified(stream, destination, expected)
            destination.chmod(mode)
            members[name] = dict(expected, mode=mode)
        for name, expected in selected.items():
            copy(Path(artifacts) / name, name, expected,
                 0o555 if name in ('bigbrotr', 'archive-worker/bigbrotr-archive-worker') else 0o444)
        for name in (REGISTRY, *PUBLIC_FILES, *(JOURNEY_FILES if include_journey else ())):
            expected = identity(ROOT / name)
            require(expected['sha256'] == assembly['files_sha256'][name], 'assembly member changed')
            copy(ROOT / name, name, expected, 0o444)
        write_json(stage / 'build-info.json', provenance['build_info'])
        members['build-info.json'] = dict(identity(stage / 'build-info.json'), mode=0o444)
        inventory = {'format': 'bigbrotr-bundle-members/1', 'files': members}
        write_json(stage / 'members.json', inventory)
        for path in stage.rglob('*'):
            if path.is_dir():
                path.chmod(0o755)
        archive = output / ('bigbrotr-' + TARGET + '.tar.gz')
        pack(stage, archive)
        manifest = {'format': 'bigbrotr-candidate-artifact/1', 'visibility': 'private',
                    'status': 'candidate-not-release', 'target': TARGET,
                    'bundle': dict(identity(archive), name=archive.name),
                    'inventory': dict(identity(stage / 'members.json'), name='members.json'),
                    'assembly': assembly, 'artifact': provenance,
                    'advertised_capabilities': provenance['build_info']['capabilities'],
                    'journey_files': list(JOURNEY_FILES) if include_journey else [],
                    'open_gates': OPEN_GATES}
        write_json(output / 'candidate-manifest.json', manifest)
        shutil.rmtree(stage)
        return manifest
    except BaseException:
        shutil.rmtree(output)
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--artifacts', required=True, type=Path)
    parser.add_argument('--source-manifest', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path, help='new private directory; never overwritten')
    parser.add_argument('--include-journey', action='store_true', help='include the fixed tracked recipe/tutorial set')
    args = parser.parse_args()
    try:
        assemble(args.artifacts, args.source_manifest, args.output, include_journey=args.include_journey)
    except (KeyError, TypeError, ValueError, OSError, subprocess.SubprocessError) as error:
        print('assembly failed: ' + str(error), file=sys.stderr)
        return 1
    print(args.output.absolute() / 'candidate-manifest.json')
    return 0


if __name__ == '__main__':
    sys.exit(main())
