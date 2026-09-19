#!/usr/bin/env python3
"""Exact-source build for reserved initialization; isolated native Linux lab only."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

sys.dont_write_bytecode = True
SOURCE = Path('/out/build/source')
sys.path.insert(0, str(SOURCE))
from tools.build import build_binaries, clean_environment, file_hash, native_toolchain
from tools.archive_bundle import build_environment, install_bundle
from tools.ci import execute

build, out, bundle = SOURCE.parent, Path('/out/native-tests'), Path('/out/archive-worker')
if sys.argv[1:] == ['--build-binaries']:
    identity = build_binaries(SOURCE, os.environ)
    (out / 'worker-identity.json').write_text(json.dumps(identity, indent=2) + '\n')
    sys.exit(0)
assert sys.argv[1:] in ([], ['--tls'], ['--publication'])
tls = sys.argv[1:] == ['--tls']
publication = sys.argv[1:] == ['--publication']
manifest = json.loads((build / 'source-manifest.json').read_text())


def source_files():
    return {str(p.relative_to(SOURCE)): file_hash(p) for p in sorted(SOURCE.rglob('*')) if p.is_file()}


assert source_files() == manifest['files_sha256']
bundle.mkdir()
install_bundle(Path('/assets'), bundle, 'x86_64-unknown-linux-gnu')
env = clean_environment()
selected, native = native_toolchain(env, 2)
env.update(selected)
env.update(build_environment(bundle))
env.update(CARGO_HOME=str(build / 'cargo-home'), CARGO_TARGET_DIR=str(build / 'target'),
           CARGO_NET_OFFLINE='true', BIGBROTR_SOURCE_REVISION=manifest['revision'],
           BIGBROTR_SOURCE_TREE=manifest['tree'], BIGBROTR_BUILD_PROFILE='release',
           BIGBROTR_SOURCE_MANIFEST_SHA256=file_hash(build / 'source-manifest.json'))
identity = dict(source=manifest['revision'], source_manifest_sha256=file_hash(build / 'source-manifest.json'),
                native_toolchain=native, scope='permanent local reservation and actual native initializer')
assert execute([('ordered-build', [sys.executable, __file__, '--build-binaries'])],
               out, SOURCE, env, identity, 1800)['status'] == 'PASS'
worker = json.loads((out / 'worker-identity.json').read_text())
env.update(BIGBROTR_ARCHIVE_WORKER_BYTES=str(worker['bytes']), BIGBROTR_ARCHIVE_WORKER_SHA256=worker['sha256'])
tls_fixture = ['--example', 'archive_receipt_probe'] if tls or publication else []
publication_fixture = ['-p', 'bigbrotr-archive-worker', '--example', 'archive_publication_probe', '--example', 'archive_read_probe', '--example', 'archive_coordinator_probe', '--example', 'archive_attempt_probe'] if publication else []
assert execute([
    ('fixture', ['cargo', 'build', '--locked', '--release', '-p', 'bigbrotr', '--example', 'catalog_schema_probe', '--example', 'catalog_initialization_probe', *tls_fixture, *publication_fixture]),
    ('parent-tests', ['cargo', 'test', '--locked', '--release', '-p', 'bigbrotr', '--lib', 'storage::archive', '--', '--nocapture']),
    ('cli-tests', ['cargo', 'test', '--locked', '--release', '-p', 'bigbrotr', '--bin', 'bigbrotr', '--test', 'catalog_entrypoint', '--test', 'control_runtime_entrypoint']),
    ('clippy', ['cargo', 'clippy', '--locked', '--release', '-p', 'bigbrotr', '-p', 'bigbrotr-archive-worker', '--lib', '--example', 'catalog_schema_probe', '--example', 'catalog_initialization_probe', *tls_fixture, *publication_fixture[2:], '--bin', 'bigbrotr', '--bin', 'bigbrotr-archive-worker', '--', '-D', 'warnings']),
], out / 'checks', SOURCE, env, identity, 1800)['status'] == 'PASS'
for src, dst in [('bigbrotr', out / 'bigbrotr'), ('bigbrotr-archive-worker', bundle / 'bigbrotr-archive-worker'),
                 ('examples/catalog_schema_probe', out / 'catalog-schema-probe'),
                 ('examples/catalog_initialization_probe', out / 'catalog-initialization-probe'),
                 *([('examples/archive_receipt_probe', out / 'archive-receipt-probe')] if tls or publication else []),
                 *([('examples/archive_publication_probe', out / 'archive-publication-probe'),
                    ('examples/archive_read_probe', out / 'archive-read-probe'),
                    ('examples/archive_coordinator_probe', out / 'archive-coordinator-probe'),
                    ('examples/archive_attempt_probe', out / 'archive-attempt-probe')] if publication else [])]:
    shutil.copy2(build / 'target/release' / src, dst)
    dst.chmod(0o500)
assert source_files() == manifest['files_sha256']
info = json.loads(subprocess.check_output([str(out / 'bigbrotr'), 'build-info'], env={}))
assert info['archive_worker'] == worker and info['source']['revision'] == manifest['revision']
assert info['source']['dirty'] is False
result = dict(identity, status='READY', control=file_hash(out / 'bigbrotr'), worker=worker['sha256'],
              probe=file_hash(out / 'catalog-schema-probe'), initializer=file_hash(out / 'catalog-initialization-probe'),
              build_info=info, source_unchanged=True)
if publication:
    result.update(publisher=file_hash(out / 'archive-publication-probe'), reader=file_hash(out / 'archive-read-probe'))
    result.update(coordinator_probe=file_hash(out/'archive-coordinator-probe'), attempt_probe=file_hash(out/'archive-attempt-probe'))
if tls or publication:
    result['receipt_reader'] = file_hash(out / 'archive-receipt-probe')
(out / 'ready.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps({'status': 'READY', 'source': manifest['revision']}), flush=True)
