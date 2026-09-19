#!/usr/bin/env python3
"""Build and exercise exact-source intent scans before actual Control startup cases."""
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
from tools.archive_bundle import build_environment
from tools.ci import execute
out = Path('/out/native-tests')
if sys.argv[1:] == ['--build-binaries']:
    identity = build_binaries(SOURCE, os.environ)
    (out / 'worker-identity.json').write_text(json.dumps(identity, indent=2) + '\n')
    sys.exit(0)
assert not sys.argv[1:]
build = SOURCE.parent
manifest = json.loads((build / 'source-manifest.json').read_text())
def inventory():
    return {str(p.relative_to(SOURCE)): file_hash(p) for p in sorted(SOURCE.rglob('*')) if p.is_file()}
assert inventory() == manifest['files_sha256']
env = clean_environment()
selected, native = native_toolchain(env, 2)
env.update(selected)
env.update(build_environment(Path('/assets')))
env.update(CARGO_HOME=str(build / 'cargo-home'), CARGO_TARGET_DIR=str(build / 'target'),
           CARGO_NET_OFFLINE='true', BIGBROTR_SOURCE_REVISION=manifest['revision'],
           BIGBROTR_SOURCE_TREE=manifest['tree'], BIGBROTR_BUILD_PROFILE='release',
           BIGBROTR_SOURCE_MANIFEST_SHA256=file_hash(build / 'source-manifest.json'))
identity = dict(source=manifest['revision'], native_toolchain=native,
                source_manifest_sha256=file_hash(build / 'source-manifest.json'))
assert execute([('build', [sys.executable, __file__, '--build-binaries'])], out, SOURCE, env, identity, 1800)['status'] == 'PASS'
worker = json.loads((out / 'worker-identity.json').read_text())
env.update(BIGBROTR_ARCHIVE_WORKER_BYTES=str(worker['bytes']), BIGBROTR_ARCHIVE_WORKER_SHA256=worker['sha256'])
assert execute([
    ('fixture', ['cargo', 'build', '--locked', '--release', '-p', 'bigbrotr', '--example', 'archive_attempt_probe', '--example', 'archive_coordinator_probe']),
    ('tests', ['cargo', 'test', '--locked', '--release', '-p', 'bigbrotr', '--bin', 'bigbrotr', '--test', 'archive_attempt', '--test', 'control_runtime_entrypoint', '--test', 'control_schema', '--test', 'catalog_entrypoint', '--test', 'config_entrypoint']),
    ('custody', ['cargo', 'test', '--locked', '--release', '-p', 'bigbrotr', '--lib', 'storage::archive']),
    ('clippy', ['cargo', 'clippy', '--locked', '--release', '-p', 'bigbrotr', '--lib', '--bin', 'bigbrotr', '--example', 'archive_attempt_probe', '--', '-D', 'warnings']),
], out / 'checks', SOURCE, env, identity, 1800)['status'] == 'PASS'
for name, target in [('bigbrotr', 'bigbrotr'), ('examples/archive_attempt_probe', 'archive-attempt-probe'), ('examples/archive_coordinator_probe', 'archive-coordinator-probe')]:
    shutil.copy2(build / 'target/release' / name, out / target)
    (out / target).chmod(0o500)
assert execute([('adapter', [str(out / 'archive-attempt-probe'), '--scan'])], out / 'adapter', SOURCE, env, identity, 180)['status'] == 'PASS'
assert inventory() == manifest['files_sha256']
info = json.loads(subprocess.check_output([str(out / 'bigbrotr'), 'build-info'], env={}))
assert info['source']['revision'] == manifest['revision'] and info['source']['dirty'] is False
assert info['archive_worker'] == worker
ready = dict(identity, status='READY', binary_sha256=file_hash(out / 'bigbrotr'),
             probe_sha256=file_hash(out / 'archive-attempt-probe'), build_info=info)
(out / 'ready.json').write_text(json.dumps(ready, indent=2) + '\n')
print(json.dumps(ready), flush=True)
