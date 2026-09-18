#!/usr/bin/env python3
"""Build exact-source catalog recovery probes, or run one finite native bootstrap."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

sys.dont_write_bytecode = True
SOURCE = Path('/out/build/source')
sys.path.insert(0, str(SOURCE))
from tools.build import clean_environment, native_toolchain, file_hash
from tools.archive_bundle import install_bundle, build_environment
from tools.ci import execute

build = SOURCE.parent
out = Path('/out/native-tests')
bundle = Path('/out/archive-worker')

if len(sys.argv) == 3 and sys.argv[1] == 'bootstrap':
    database = sys.argv[2]
    assert database.startswith('bbcat_') and all(c in 'abcdefghijklmnopqrstuvwxyz_0123456789' for c in database)
    archive = Path('/out/archive_' + database)
    archive.mkdir()
    temp = Path('/out/temp_' + database)
    temp.mkdir()
    env = clean_environment()
    env.update(LD_LIBRARY_PATH=str(bundle / 'lib'), BIGBROTR_ARCHIVE_PARENT_PID=str(os.getpid()))
    dsn = 'host=127.0.0.1 port=5432 dbname=' + database + ' user=BootstrapMixed password=' + Path('/secrets/BootstrapMixed').read_text() + ' sslmode=disable connect_timeout=5'
    result = subprocess.run([str(bundle / 'bigbrotr-archive-worker'), 'bootstrap-catalog', str(bundle), str(temp), str(128*1024**2), str(64*1024**2), '1'],
        input=json.dumps({'dsn': dsn, 'data_root': str(archive) + '/'}), text=True, capture_output=True, env=env, timeout=60)
    assert result.returncode == 0 and not result.stderr, 'native-bootstrap-refused'
    assert json.loads(result.stdout) == {'format': 'bigbrotr-native-bootstrap/1', 'status': 'initialized'}
    print(result.stdout, end='')
    sys.exit(0)

assert len(sys.argv) == 1
manifest = json.loads((build / 'source-manifest.json').read_text())
assert {str(p.relative_to(SOURCE)): file_hash(p) for p in sorted(SOURCE.rglob('*')) if p.is_file()} == manifest['files_sha256']
bundle.mkdir()
install_bundle(Path('/assets'), bundle, 'x86_64-unknown-linux-gnu')
env = clean_environment()
native_env, native = native_toolchain(env, 2)
env.update(native_env)
env.update(build_environment(bundle))
env.update(CARGO_HOME=str(build / 'cargo-home'), CARGO_TARGET_DIR=str(build / 'target'), LD_LIBRARY_PATH=str(bundle / 'lib'),
    BIGBROTR_SOURCE_REVISION=manifest['revision'], BIGBROTR_SOURCE_TREE=manifest['tree'],
    BIGBROTR_SOURCE_MANIFEST_SHA256=file_hash(build / 'source-manifest.json'), BIGBROTR_BUILD_PROFILE='release')
checks = [
    ('build', ['cargo', 'build', '--locked', '--offline', '--release', '-p', 'bigbrotr', '--example', 'archive_attempt_probe', '--example', 'catalog_recovery_probe']),
    ('worker', ['cargo', 'build', '--locked', '--offline', '--release', '-p', 'bigbrotr-archive-worker', '--bin', 'bigbrotr-archive-worker']),
    ('clippy', ['cargo', 'clippy', '--locked', '--offline', '--release', '-p', 'bigbrotr', '--example', 'catalog_recovery_probe', '--', '-D', 'warnings']),
]
result = execute(checks, out, SOURCE, env, {'source': manifest['revision'], 'native_toolchain': native}, 1800)
assert result['status'] == 'PASS'
worker = bundle / 'bigbrotr-archive-worker'
shutil.copy2(build / 'target/release/bigbrotr-archive-worker', worker)
worker.chmod(0o500)
probe = out / 'catalog-recovery-probe'
shutil.copy2(build / 'target/release/examples/catalog_recovery_probe', probe)
probe.chmod(0o500)
identity = {'source': manifest['revision'], 'worker': file_hash(worker), 'probe': file_hash(probe), 'native_toolchain': native,
    'control_probe': file_hash(build / 'target/release/examples/archive_attempt_probe')}
result = execute([('control', [str(build / 'target/release/examples/archive_attempt_probe')])], out / 'control', SOURCE, env, identity, 180)
assert result['status'] == 'PASS'
(out / 'ready.json').write_text(json.dumps(identity, indent=2) + '\n')
print(json.dumps({'status': 'READY', **identity}), flush=True)
