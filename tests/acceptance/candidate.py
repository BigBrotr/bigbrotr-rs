"""Verify and extract the existing private candidate format without executing it.

The supplied manifest is a trusted local input, not a release signature. Keep
product provenance distinct from assembly and acceptance source identities.
Partial extraction is retained under the new destination after a failure.

The complete compressed size/hash is verified before stdlib gzip/tar parsing.
Member sizes and metadata are checked after parsing; stdlib may expand extended
metadata before those checks. This does not bound parser resources for a forged
trusted manifest selecting a hostile archive. Public hostile-artifact intake and
release authentication remain separate P08 gates.
"""
import hashlib
import importlib.util
import json
from pathlib import Path, PurePosixPath
import re
import subprocess
import tarfile

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    'candidate_format', ROOT / 'packaging/release/assemble.py')
format = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(format)


def stream_hash(stream):
    digest = hashlib.sha256()
    while block := stream.read(65536):
        digest.update(block)
    return digest.hexdigest()


def named_blob(value, name):
    format.require(type(value) is dict and set(value) == {'name', 'bytes', 'sha256'}
                   and value['name'] == name, 'unexpected named artifact')
    return format.blob({key: value[key] for key in ('bytes', 'sha256')})


def metadata(archive, entry):
    format.require(entry.isfile() and 0 < entry.size <= 8 * 1024**2,
                   'candidate metadata size/type')
    with archive.extractfile(entry) as stream:
        raw = stream.read(entry.size + 1)
    format.require(len(raw) == entry.size, 'candidate metadata changed')
    value = json.loads(raw, object_pairs_hook=format.unique_object)
    format.require(type(value) is dict, 'candidate metadata must be an object')
    return value, {'bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest()}


def closed(value, keys, label):
    format.require(type(value) is dict and set(value) == set(keys), label + ' shape')


def source_identity(value):
    closed(value, ('state', 'revision', 'tree', 'dirty', 'manifest_sha256'), 'source identity')
    format.require(value['state'] == 'builder-supplied' and value['dirty'] is False,
                   'unsupported source identity')
    for key in ('revision', 'tree'):
        format.digest(value[key], 40)
    format.digest(value['manifest_sha256'])


def validate_manifest(manifest):
    """Validate the existing closed schema without adding an intake dependency."""
    closed(manifest, ('format', 'visibility', 'status', 'target', 'bundle', 'inventory',
        'assembly', 'artifact', 'advertised_capabilities', 'journey_files', 'open_gates'),
        'candidate manifest')
    format.require(manifest['format'] == 'bigbrotr-candidate-artifact/1'
                   and manifest['visibility'] == 'private'
                   and manifest['status'] == 'candidate-not-release'
                   and manifest['target'] == format.TARGET, 'unsupported candidate')
    format.require(manifest['journey_files'] == list(format.JOURNEY_FILES),
                   'complete first-launch recipe is required')
    format.require(manifest['open_gates'] == format.OPEN_GATES, 'candidate open gates differ')
    assembly = manifest['assembly']
    closed(assembly, ('revision', 'tree', 'files_sha256', 'packing'), 'assembly')
    for key in ('revision', 'tree'):
        format.digest(assembly[key], 40)
    closed(assembly['files_sha256'], (*format.ASSEMBLY_FILES, *format.PUBLIC_FILES,
        *format.JOURNEY_FILES), 'assembly input inventory')
    for value in assembly['files_sha256'].values():
        format.digest(value)
    packing = assembly['packing']
    closed(packing, ('python', 'zlib', 'archive_format', 'gzip_level'), 'packing')
    format.require(type(packing['python']) is str
                   and re.fullmatch(r'[0-9]+\.[0-9]+\.[0-9]+', packing['python'])
                   and type(packing['zlib']) is str and 0 < len(packing['zlib']) <= 64
                   and packing['archive_format'] == 'ustar+gzip'
                   and type(packing['gzip_level']) is int and packing['gzip_level'] == 6,
                   'unsupported packing')
    provenance = manifest['artifact']
    closed(provenance, ('binary', 'source', 'build_info', 'build_result_sha256',
        'source_archive_sha256', 'native_toolchain', 'registry_sha256', 'build_receipt_kind'),
        'artifact provenance')
    format.blob(provenance['binary'])
    source_identity(provenance['source'])
    for key in ('build_result_sha256', 'source_archive_sha256', 'registry_sha256'):
        format.digest(provenance[key])
    format.require(provenance['build_receipt_kind'] in ('standard', 'archive-installed-lab'),
                   'unsupported build receipt')
    native = provenance['native_toolchain']
    closed(native, ('os', 'architecture', 'tools_sha256'), 'native toolchain')
    format.require(native['os'] == 'Linux' and native['architecture'] == 'x86_64',
                   'unsupported native toolchain')
    closed(native['tools_sha256'], ('CC', 'CXX', 'AR', 'LIBCLANG_PATH', 'discovery'), 'native tools')
    for value in native['tools_sha256'].values():
        format.digest(value)
    info = provenance['build_info']
    closed(info, format.INFO_KEYS, 'build info')
    format.require(info['format'] == 'bigbrotr-build/1' and info['product'] == 'BigBrotr'
                   and info['target'] == format.TARGET and info['profile_assertion'] == 'release'
                   and info['optimization'] == '3' and info['debug_info'] == 'false',
                   'unsupported candidate build')
    source_identity(info['source'])
    format.require(provenance['source'] == info['source'], 'candidate product source mismatch')
    format.require(manifest['advertised_capabilities'] == info['capabilities']
                   == ['event-verification', 'config-static'], 'candidate capability mismatch')
    closed(info['inputs_sha256'], format.INPUT_KEYS, 'build inputs')
    for value in info['inputs_sha256'].values():
        format.digest(value)
    for key in ('version', 'capability_scope', 'rustc'):
        format.require(type(info[key]) is str and 0 < len(info[key]) <= 4096, 'invalid build text')
    format.require(info['protocol_registry_format'] == 'bigbrotr-protocol-versions/1',
                   'unsupported protocol registry')
    for key in ('protocols', 'schemas'):
        value = info[key]
        format.require(type(value) is dict and 1 <= len(value) <= 32
                       and all(type(k) is str and 0 < len(k) <= 128
                               and type(v) is str and 0 < len(v) <= 256 for k, v in value.items()),
                       'invalid build version map')
    format.blob(info['archive_worker'])


def extraction_destination(destination, source_root, inputs):
    destination = Path(destination).absolute()
    format.require('..' not in destination.parts and destination.name not in ('', '.', '..'),
                   'unsafe extraction destination')
    format.safe_path(destination.parent, directory=True)
    format.require(destination != source_root and destination not in source_root.parents,
                   'extraction overlaps source root')
    if source_root in destination.parents:
        relative = destination.relative_to(source_root)
        format.require(len(relative.parts) >= 2 and relative.parts[0] == '.research',
                       'checkout extraction requires ignored .research')
        ignored = subprocess.run(['git', '-C', str(source_root), 'check-ignore', '--quiet',
            '--', str(destination)], capture_output=True, timeout=5)
        format.require(ignored.returncode == 0, 'checkout extraction must be ignored by Git')
    for path in inputs:
        format.require(destination != path and destination not in path.parents
                       and path not in destination.parents, 'extraction overlaps candidate inputs')
    format.require(not destination.exists() and not destination.is_symlink(),
                   'extraction destination already exists')
    return destination


def extract_candidate(manifest_path, destination, *, source_root):
    """Return checked provenance/root; never execute a candidate or start engines.

    Parents and inputs must remain immutable under the invoking user's custody.
    A fresh ignored .research descendant is allowed inside the source checkout.
    """
    manifest_path = format.safe_path(manifest_path)
    source_root = format.safe_path(source_root, directory=True)
    manifest, manifest_hash = format.read_json(manifest_path)
    validate_manifest(manifest)
    archive_name = 'bigbrotr-' + format.TARGET + '.tar.gz'
    expected_archive = named_blob(manifest['bundle'], archive_name)
    expected_inventory = named_blob(manifest['inventory'], 'members.json')
    provenance = manifest['artifact']
    info = provenance['build_info']

    # Derive the closed file set from the existing format and declared native
    # registry. Neither a tar member nor its inventory may authorize extra files.
    registry, registry_hash = format.read_json(source_root / format.REGISTRY)
    format.require(provenance['registry_sha256'] == registry_hash,
                   'candidate native registry differs from acceptance source')
    native = registry['platforms'][format.TARGET]
    expected = {'bigbrotr': format.blob(provenance['binary']),
                'archive-worker/bigbrotr-archive-worker': format.blob(info['archive_worker'])}
    for kind in ('library', 'header'):
        asset = native[kind]
        expected['archive-worker/lib/' + format.relative(asset['name'])] = format.blob(
            {key: asset[key] for key in ('bytes', 'sha256')})
    for asset in native['extensions']:
        expected['archive-worker/extensions/' + format.relative(asset['name'])
                 + '.duckdb_extension'] = format.blob(
                     {key: asset[key] for key in ('bytes', 'sha256')})
    for name in (format.REGISTRY, *format.PUBLIC_FILES, *format.JOURNEY_FILES):
        expected[name] = format.identity(source_root / name)
        format.require(expected[name]['sha256'] == manifest['assembly']['files_sha256'][name],
                       'candidate assembly input differs: ' + name)
    files = set(expected) | {'members.json', 'build-info.json'}
    directories = {str(parent) for name in files for parent in PurePosixPath(name).parents
                   if str(parent) != '.'}
    wanted_names = files | directories
    destination = extraction_destination(destination, source_root,
        (manifest_path, manifest_path.parent / archive_name))

    with format.regular(manifest_path.parent / archive_name) as (raw, stat):
        format.require(stat.st_size == expected_archive['bytes']
                       and stream_hash(raw) == expected_archive['sha256'],
                       'candidate archive identity mismatch')
        raw.seek(0)
        with tarfile.open(fileobj=raw, mode='r:gz') as archive:
            entries = {}
            block_size, record_size = tarfile.BLOCKSIZE, tarfile.RECORDSIZE
            end = 0
            for entry in archive:
                name = format.relative(entry.name)
                format.require(name in wanted_names and name not in entries,
                               'extra or duplicate candidate member')
                format.require(not entry.pax_headers and not entry.sparse,
                               'extended tar metadata is unsupported')
                format.require(entry.offset == end and entry.offset_data == entry.offset + block_size,
                               'extended or disordered tar header')
                archive.fileobj.seek(entry.offset)
                header = archive.fileobj.read(block_size)
                format.require(header[257:265] == b'ustar\x00' + b'00', 'candidate requires USTAR headers')
                end = entry.offset_data + ((entry.size + block_size - 1) // block_size) * block_size
                format.require(entry.uid == entry.gid == entry.mtime == 0
                               and entry.uname == entry.gname == '' and entry.linkname == ''
                               and entry.devmajor == entry.devminor == 0, 'unexpected tar ownership/link metadata')
                executable = name in ('bigbrotr', 'archive-worker/bigbrotr-archive-worker')
                if name in directories:
                    format.require(entry.isdir() and entry.mode == 0o755 and entry.size == 0,
                                   'invalid candidate directory')
                else:
                    maximum = expected[name]['bytes'] if name in expected else 8 * 1024**2
                    format.require(entry.type == tarfile.REGTYPE and 0 < entry.size <= maximum
                                   and entry.mode == (0o555 if executable else 0o444),
                                   'invalid candidate file type/size/mode')
                archive.fileobj.seek(entry.offset_data + entry.size)
                padding = end - entry.offset_data - entry.size
                format.require(archive.fileobj.read(padding) == bytes(padding),
                               'nonzero candidate member padding')
                entries[name] = entry
            format.require(set(entries) == wanted_names, 'missing candidate members')
            # TarFile stops at the first zero block; hidden entries after it are
            # not part of our closed inventory. Require the assembler's ending.
            padding = ((end + 2 * block_size + record_size - 1) // record_size) * record_size - end
            archive.fileobj.seek(end)
            format.require(archive.fileobj.read(padding + 1) == bytes(padding),
                           'invalid candidate tar ending')
            inventory, inventory_identity = metadata(archive, entries['members.json'])
            format.require(inventory_identity == expected_inventory
                           and set(inventory) == {'format', 'files'}
                           and inventory['format'] == 'bigbrotr-bundle-members/1'
                           and type(inventory['files']) is dict
                           and set(inventory['files']) == files - {'members.json'},
                           'candidate inventory mismatch')
            recorded_info, info_identity = metadata(archive, entries['build-info.json'])
            # Python equality aliases False/0 and integer/float values. Preserve
            # JSON scalar types when binding the separately supplied build-info.
            format.require(format.json_bytes(recorded_info) == format.json_bytes(info),
                           'bundled build-info differs from manifest')
            expected['build-info.json'] = info_identity
            for name, identity in expected.items():
                record = inventory['files'][name]
                format.require(type(record) is dict and set(record) == {'bytes', 'sha256', 'mode'}
                               and type(record['bytes']) is int and type(record['mode']) is int
                               and record == dict(identity, mode=entries[name].mode),
                               'candidate member binding differs: ' + name)
                format.require(entries[name].size == identity['bytes'],
                               'candidate member size differs: ' + name)
            expected['members.json'] = expected_inventory
            destination.mkdir(mode=0o700)
            for name in sorted(directories, key=lambda value: (value.count('/'), value)):
                (destination / name).mkdir(mode=0o755)
            for name in sorted(files):
                with archive.extractfile(entries[name]) as stream:
                    format.copy_verified(stream, destination / name, expected[name])
                (destination / name).chmod(entries[name].mode)
        raw.seek(0)
        format.require(stream_hash(raw) == expected_archive['sha256'],
                       'candidate archive changed during extraction')
    format.executable(destination / 'bigbrotr')
    format.executable(destination / 'archive-worker/bigbrotr-archive-worker')
    return {'manifest': manifest, 'manifest_sha256': manifest_hash,
            'bundle_root': str(destination), 'bundle_sha256': expected_archive['sha256']}
