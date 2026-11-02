"""Optional timestamp reuse from a trusted exact-source build, never target ownership.

The build/CI consumer supplies its actual selected compiler/target/profile/assets.
No compiled target, Cargo home, binary or runtime cache is read or restored here.
Fresh-target hosted CI gains nothing from timestamps alone; a separate trusted
cache owner must still qualify, lock and validate any compiled-output reuse.
"""
import hashlib
import json
import os
from pathlib import Path
import stat
import tarfile

from tools.build import regular_file_hash, source_inventory, verify_native_toolchain


CONTEXT_FILES = {'Cargo.toml', 'Cargo.lock', 'rust-toolchain.toml',
                 '.cargo/config', '.cargo/config.toml', 'tools/build.py',
                 'tools/archive_bundle.py', 'tools/native_source_timestamps.py',
                 'contracts/release/archive-engine.json'}


def relevant(name):
    return (name == 'vendor' or name.startswith('vendor/') or name in CONTEXT_FILES
            or (name.startswith(('apps/', 'crates/')) and name.endswith('/Cargo.toml')))


def inventory(source, archive, manifest):
    """Bind bytes, complete vendor directory membership and archive modes.

    The existing source manifest binds the whole export and its tar. Modes are
    read from that same authenticated tar, rather than trusting current chmod.
    Empty/new directories also invalidate the recursively watched native tree.
    """
    if source.is_symlink() or archive.is_symlink() or not source.is_dir():
        raise ValueError('invalid native timestamp source/archive')
    if (regular_file_hash(archive) != manifest['archive_sha256']
            or source_inventory(source) != manifest['files_sha256']):
        raise ValueError('native timestamp source/archive differs from its manifest')
    expected = {}
    with tarfile.open(archive) as bundle:
        for entry in bundle:
            path = Path(entry.name)
            if path.is_absolute() or '..' in path.parts or not (entry.isfile() or entry.isdir()):
                raise ValueError('unsupported source archive member')
            name = path.as_posix()
            if relevant(name):
                if name in expected:
                    raise ValueError('duplicate native timestamp archive member')
                digest = None
                if entry.isfile():
                    value = hashlib.sha256()
                    with bundle.extractfile(entry) as member:
                        for block in iter(lambda: member.read(1024 * 1024), b''):
                            value.update(block)
                    digest = value.hexdigest()
                expected[name] = ('directory' if entry.isdir() else 'file', entry.mode, digest)
    actual = {}
    native = source / 'vendor'
    if native.is_symlink() or not native.is_dir():
        raise ValueError('native vendor inputs are missing')
    paths = [native, *native.rglob('*')]
    paths += [source/name for name in manifest['files_sha256']
              if relevant(name) and not name.startswith('vendor/')]
    for path in paths:
        name = path.relative_to(source).as_posix()
        metadata = path.lstat()
        kind = ('directory' if stat.S_ISDIR(metadata.st_mode) else
                'file' if stat.S_ISREG(metadata.st_mode) and metadata.st_nlink == 1 else None)
        if kind is None:
            raise ValueError('native timestamp input is a link or special file')
        actual[name] = (kind, stat.S_IMODE(metadata.st_mode), manifest['files_sha256'].get(name))
    if actual != expected or not any(kind == 'file' and name.startswith('vendor/')
                                     for name, (kind, _, _) in actual.items()):
        raise ValueError('native input contents, membership or mode differs from its archive')
    return actual


def preserve_native_timestamps(source, archive, manifest, native, rustc, target, assets, reference,
                               *, profile='release'):
    """Preserve ONLY identical vendor input timestamps; mismatch refuses reuse.

    `reference` is a prior qualified tools/build.py directory, not a compiler
    cache. The caller keeps exclusive custody of its fresh source and any
    separately selected compiled cache. No unchanged-input assumption is made
    from a reference path, target directory name or claimed cache hit.
    """
    reference = Path(reference)
    if reference.is_symlink() or not reference.is_dir():
        raise ValueError('native source reference is not a build directory')
    reference = reference.resolve(strict=True)
    reference_manifest_path = reference / 'source-manifest.json'
    receipt_path = reference / 'build-result.json'
    anchors = {name: regular_file_hash(reference/name)
               for name in ['source-manifest.json', 'build-result.json']}
    try:
        previous = json.loads(reference_manifest_path.read_text())
        receipt = json.loads(receipt_path.read_text())
        info = receipt['build_info']
        expected_source = dict(state='builder-supplied', revision=previous['revision'],
                               tree=previous['tree'], dirty=False,
                               manifest_sha256=anchors['source-manifest.json'])
        if (manifest['format'] != 'bigbrotr-source/1'
                or previous['format'] != 'bigbrotr-source/1'
                or receipt['format'] != 'bigbrotr-build-result/1'
                or receipt['source_manifest_sha256'] != anchors['source-manifest.json']
                or info['source'] != expected_source
                or receipt['helper_sha256'] != previous['files_sha256']['tools/build.py']):
            raise ValueError('invalid native source reference identity')
        if (native != receipt['native_toolchain'] or rustc != info['rustc']
                or target != info['target'] or info['profile_assertion'] != profile
                or assets != receipt['archive_worker']['assets']
                or receipt['archive_worker']['registry_sha256']
                    != previous['files_sha256']['contracts/release/archive-engine.json']):
            raise ValueError('native source reference compiler/profile/target/assets differ')
    except (KeyError, TypeError, json.JSONDecodeError):
        raise ValueError('invalid native source reference metadata') from None
    verify_native_toolchain(native)
    current = inventory(source, archive, manifest)
    old_source = reference / 'source'
    old = inventory(old_source, reference/'source.tar', previous)
    if current != old:
        raise ValueError('native input contents, membership, modes or build configuration differ')
    names = [name for name in current if name == 'vendor' or name.startswith('vendor/')]
    # Validate every input and all context BEFORE changing any timestamp. Files
    # precede directories; parent directory mtimes are restored last.
    names.sort(key=lambda name: (current[name][0] == 'directory', -len(Path(name).parts), name))
    planned = {name: ((source/name).lstat(), (old_source/name).lstat().st_mtime_ns)
               for name in names}
    for name in names:
        path = source/name
        before, mtime = planned[name]
        # Do not follow a substituted link or mutate a substituted inode. The
        # caller's exclusive fresh-source custody remains required throughout.
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        try:
            now = os.fstat(descriptor)
            if ((now.st_dev, now.st_ino, now.st_mode, now.st_size, now.st_nlink,
                 now.st_mtime_ns, now.st_ctime_ns) !=
                    (before.st_dev, before.st_ino, before.st_mode, before.st_size, before.st_nlink,
                     before.st_mtime_ns, before.st_ctime_ns)):
                raise ValueError('native source identity changed before timestamp reuse')
            os.utime(descriptor, ns=(now.st_atime_ns, mtime))
        finally:
            os.close(descriptor)
    if (inventory(source, archive, manifest) != current
            or inventory(old_source, reference/'source.tar', previous) != old
            or any((source/name).lstat().st_mtime_ns != planned[name][1] for name in names)
            or any(regular_file_hash(reference/name) != expected for name, expected in anchors.items())):
        raise ValueError('native timestamp preparation changed its declared inputs')
    verify_native_toolchain(native)
    return {'status': 'preserved-identical-native-inputs',
            'reference_source_manifest_sha256': anchors['source-manifest.json'],
            'reference_revision': previous['revision'],
            'files': sum(current[name][0] == 'file' for name in names),
            'directories': sum(current[name][0] == 'directory' for name in names),
            'scope': 'timestamps only; no compiled-target ownership, cache hit or measured speedup'}
