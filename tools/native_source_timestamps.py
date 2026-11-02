"""Optional timestamp reuse from a trusted exact-source build, never target ownership.

The build/CI consumer supplies its actual selected compiler/target/profile/assets.
No compiled target, Cargo home, binary or runtime cache is read or restored here.
Fresh-target hosted CI gains nothing from timestamps alone; a separate trusted
cache owner must still qualify, lock and validate any compiled-output reuse.
"""
from contextlib import ExitStack, contextmanager
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


def identity(metadata):
    return (metadata.st_dev, metadata.st_ino, metadata.st_mode, metadata.st_nlink,
            metadata.st_uid, metadata.st_gid, metadata.st_rdev, metadata.st_size)


def unchanged(metadata):
    # Reads may change atime. No other field may change before mutation.
    return (*identity(metadata), metadata.st_mtime_ns, metadata.st_ctime_ns)


def require_kind(metadata, kind):
    if ((kind == 'directory' and not stat.S_ISDIR(metadata.st_mode))
            or (kind == 'file' and (not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1))):
        raise ValueError('native timestamp input is not its verified regular single-link file/directory')


def require_root(source, descriptor, verified):
    if (not source.is_absolute() or source.resolve(strict=True) != source
            or unchanged(source.lstat()) != unchanged(verified)
            or unchanged(os.fstat(descriptor)) != unchanged(verified)):
        raise ValueError('native timestamp source root changed or escaped')


@contextmanager
def opened_root(source, verified=None):
    if not source.is_absolute() or source.resolve(strict=True) != source:
        raise ValueError('native timestamp source root must be canonical')
    descriptor = os.open(source, os.O_RDONLY | os.O_NOFOLLOW | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        metadata = os.fstat(descriptor)
        require_kind(metadata, 'directory')
        require_root(source, descriptor, verified if verified is not None else metadata)
        yield descriptor, metadata
    finally:
        os.close(descriptor)


@contextmanager
def opened_input(root_descriptor, name, identities, kind, *, capture=False):
    """Keep the root-relative no-follow directory chain alive through utime.

    Each opened inode must match the identity retained DURING inventory. No new
    lstat after verification can bless a replacement as the mutation baseline.
    """
    with ExitStack() as descriptors:
        parent = root_descriptor
        chain = []
        parts = Path(name).parts
        for position, part in enumerate(parts):
            prefix = Path(*parts[:position+1]).as_posix()
            selected_kind = kind if position == len(parts)-1 else 'directory'
            flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK
            if selected_kind == 'directory':
                flags |= os.O_DIRECTORY
            child = os.open(part, flags, dir_fd=parent)
            descriptors.callback(os.close, child)
            metadata = os.fstat(child)
            require_kind(metadata, selected_kind)
            if prefix in identities:
                if unchanged(metadata) != unchanged(identities[prefix]):
                    raise ValueError('native source opened identity changed after verification')
            elif capture:
                identities[prefix] = metadata
            else:
                raise ValueError('native source directory was not verified')
            chain.append((parent, part, child, prefix, selected_kind))
            parent = child
        yield parent, chain


def require_chain(chain, identities):
    # Recheck named containment as well as held descriptors immediately before
    # mutation. A directory renamed/replaced by an escaping symlink is refused.
    for parent, part, child, prefix, kind in chain:
        named = os.stat(part, dir_fd=parent, follow_symlinks=False)
        held = os.fstat(child)
        require_kind(named, kind)
        require_kind(held, kind)
        if (unchanged(named) != unchanged(identities[prefix])
                or unchanged(held) != unchanged(identities[prefix])):
            raise ValueError('native source named/opened containment changed after verification')


def inventory(source, archive, manifest):
    """Bind archive bytes/modes and retain the identities verified through FDs.

    Full-source hashes and native directory membership are checked as before.
    Native bytes are additionally hashed on the same root-relative no-follow
    descriptor whose original metadata becomes the later mutation identity.
    """
    source = Path(source)
    if source.is_symlink() or archive.is_symlink() or not source.is_dir():
        raise ValueError('invalid native timestamp source/archive')
    with opened_root(source) as (root_descriptor, root_metadata):
        identities = {'': root_metadata}
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
        for path in sorted(paths, key=lambda path: (len(path.parts), path.as_posix())):
            name = path.relative_to(source).as_posix()
            if name not in expected:
                raise ValueError('native input membership differs from its archive')
            kind = expected[name][0]
            with opened_input(root_descriptor, name, identities, kind, capture=True) as (descriptor, chain):
                verified = identities[name]
                digest = None
                if kind == 'file':
                    value = hashlib.sha256()
                    for block in iter(lambda: os.read(descriptor, 1024 * 1024), b''):
                        value.update(block)
                    digest = value.hexdigest()
                require_chain(chain, identities)
                actual[name] = (kind, stat.S_IMODE(verified.st_mode), digest)
        require_root(source, root_descriptor, root_metadata)
        if actual != expected or not any(kind == 'file' and name.startswith('vendor/')
                                         for name, (kind, _, _) in actual.items()):
            raise ValueError('native input contents, membership or mode differs from its archive')
        return actual, identities


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
    current, identities = inventory(source, archive, manifest)
    old_source = reference / 'source'
    old, old_identities = inventory(old_source, reference/'source.tar', previous)
    if current != old:
        raise ValueError('native input contents, membership, modes or build configuration differ')
    names = [name for name in current if name == 'vendor' or name.startswith('vendor/')]
    # Files precede directories and ancestors are restored last, so every
    # directory in an active chain still has its original verified metadata.
    names.sort(key=lambda name: (current[name][0] == 'directory', -len(Path(name).parts), name))
    planned = {name: old_identities[name].st_mtime_ns for name in names}
    with opened_root(source, identities['']) as (root_descriptor, _):
        for name in names:
            with opened_input(root_descriptor, name, identities, current[name][0]) as (descriptor, chain):
                require_root(source, root_descriptor, identities[''])
                require_chain(chain, identities)
                now = os.fstat(descriptor)
                require_kind(now, current[name][0])
                if unchanged(now) != unchanged(identities[name]):
                    raise ValueError('native source identity changed before timestamp reuse')
                os.utime(descriptor, ns=(now.st_atime_ns, planned[name]))
    after, after_identities = inventory(source, archive, manifest)
    old_after, old_after_identities = inventory(old_source, reference/'source.tar', previous)
    if (after != current or old_after != old
            or identities.keys() != after_identities.keys()
            or old_identities.keys() != old_after_identities.keys()
            or any(identity(after_identities[name]) != identity(before)
                   or (after_identities[name].st_mtime_ns != planned[name] if name in planned
                       else unchanged(after_identities[name]) != unchanged(before))
                   for name, before in identities.items())
            or any(unchanged(old_after_identities[name]) != unchanged(before)
                   for name, before in old_identities.items())
            or any(regular_file_hash(reference/name) != expected for name, expected in anchors.items())):
        raise ValueError('native timestamp preparation changed its declared inputs')
    verify_native_toolchain(native)
    return {'status': 'preserved-identical-native-inputs',
            'reference_source_manifest_sha256': anchors['source-manifest.json'],
            'reference_revision': previous['revision'],
            'files': sum(current[name][0] == 'file' for name in names),
            'directories': sum(current[name][0] == 'directory' for name in names),
            'scope': 'timestamps only; no compiled-target ownership, cache hit or measured speedup'}
