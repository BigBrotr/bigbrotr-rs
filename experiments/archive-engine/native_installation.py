#!/usr/bin/env python3
"""Exact-source Linux lab: shared ordered build and relocated installed verifier."""
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
from tools.archive_bundle import build_environment, install_bundle, verify_bundle
from tools.ci import execute
from installed_worker_cases import run as installed_cases

build = SOURCE.parent
out = Path('/out/native-tests')
package = Path('/out/package')
bundle = package / 'archive-worker'

if sys.argv[1:] == ['--build-binaries']:
    identity = build_binaries(SOURCE, os.environ)
    (out / 'worker-identity.json').write_text(json.dumps(identity, indent=2) + '\n')
    sys.exit(0)
assert not sys.argv[1:]
manifest = json.loads((build / 'source-manifest.json').read_text())


def source_files():
    return {str(p.relative_to(SOURCE)): file_hash(p) for p in sorted(SOURCE.rglob('*')) if p.is_file()}


assert source_files() == manifest['files_sha256']
bundle.mkdir(parents=True)
install_bundle(Path('/assets'), bundle, 'x86_64-unknown-linux-gnu')
env = clean_environment()
selected, native = native_toolchain(env, 2)
env.update(selected)
env.update(build_environment(bundle))
env.update(CARGO_HOME=str(build / 'cargo-home'), CARGO_TARGET_DIR=str(build / 'target'),
           CARGO_NET_OFFLINE='true', BIGBROTR_SOURCE_REVISION=manifest['revision'],
           BIGBROTR_SOURCE_TREE=manifest['tree'], BIGBROTR_BUILD_PROFILE='release',
           BIGBROTR_SOURCE_MANIFEST_SHA256=file_hash(build / 'source-manifest.json'))
identity = {'source': manifest['revision'], 'source_manifest_sha256': file_hash(build / 'source-manifest.json'),
            'native_toolchain': native, 'scope': 'shared ordered build with retained dependency/build cache; not a clean rebuild or release qualification'}
assert execute([('ordered-build', [sys.executable, __file__, '--build-binaries'])],
               out, SOURCE, env, identity, 1800)['status'] == 'PASS'
worker = json.loads((out / 'worker-identity.json').read_text())
for src, dst in [(build / 'target/release/bigbrotr', package / 'bigbrotr'),
                 (build / 'target/release/bigbrotr-archive-worker', bundle / 'bigbrotr-archive-worker')]:
    shutil.copy2(src, dst)
    dst.chmod(0o500)
binary = package / 'bigbrotr'
info = json.loads(subprocess.check_output([str(binary), 'build-info'], env={}))
assert info['archive_worker'] == worker
assert info['source'] == {'state': 'builder-supplied', 'revision': manifest['revision'],
                          'tree': manifest['tree'], 'dirty': False, 'manifest_sha256': identity['source_manifest_sha256']}
assert worker == {'bytes': (bundle / 'bigbrotr-archive-worker').stat().st_size,
                  'sha256': file_hash(bundle / 'bigbrotr-archive-worker')}
assets = verify_bundle(bundle, 'x86_64-unknown-linux-gnu')
baseline = Path('/baseline/bigbrotr')
before = subprocess.run([str(baseline), 'archive', 'verify-worker', '--root', str(bundle)],
                        env={}, capture_output=True, timeout=45)
assert before.returncode == 2 and b"unrecognized subcommand 'archive'" in before.stderr
baseline_result = {'binary_sha256': file_hash(baseline), 'exit_code': before.returncode,
                   'stdout': before.stdout.decode(), 'stderr': before.stderr.decode(),
                   'status': 'EXPECTED_FAIL', 'reason': 'installed verification command absent'}
(out / 'baseline.json').write_text(json.dumps(baseline_result, indent=2) + '\n')
cases = installed_cases(package, out / 'installed')
bound_env = dict(env, BIGBROTR_ARCHIVE_WORKER_BYTES=str(worker['bytes']),
                 BIGBROTR_ARCHIVE_WORKER_SHA256=worker['sha256'])
assert execute([
    ('command-tests', ['cargo', 'test', '--locked', '--release', '-p', 'bigbrotr', '--test', 'installed_worker', '--test', 'cli_contract']),
    ('asset-tests', ['cargo', 'test', '--locked', '--release', '-p', 'bigbrotr', '--lib', 'storage::archive::installation']),
    ('clippy', ['cargo', 'clippy', '--locked', '--release', '-p', 'bigbrotr', '--lib', '--bin', 'bigbrotr', '--test', 'installed_worker', '--', '-D', 'warnings']),
], out / 'checks', SOURCE, bound_env, identity, 900)['status'] == 'PASS'

# Real Cargo build scripts must reject incomplete/noncanonical assertions. Then
# change a valid hash in the same target directory to exercise Cargo invalidation.
negative = []
for name, changes, remove in [
    ('missing-bytes', {}, 'BIGBROTR_ARCHIVE_WORKER_BYTES'),
    ('missing-hash', {}, 'BIGBROTR_ARCHIVE_WORKER_SHA256'),
    ('zero-length', {'BIGBROTR_ARCHIVE_WORKER_BYTES': '0'}, None),
    ('noncanonical-length', {'BIGBROTR_ARCHIVE_WORKER_BYTES': '01'}, None),
    ('uppercase-hash', {'BIGBROTR_ARCHIVE_WORKER_SHA256': 'A' * 64}, None),
]:
    bad_env = dict(bound_env, **changes)
    if remove:
        del bad_env[remove]
    with (out / (name + '.log')).open('xb') as log:
        done = subprocess.run(['cargo', 'build', '--locked', '--release', '-p', 'bigbrotr', '--bin', 'bigbrotr'],
                              cwd=SOURCE, env=bad_env, stdout=log, stderr=subprocess.STDOUT, timeout=120)
    assert done.returncode != 0
    assert 'archive worker build identity' in (out / (name + '.log')).read_text()
    negative.append({'name': name, 'exit_code': done.returncode, 'status': 'PASS'})
wrong_env = dict(bound_env, BIGBROTR_ARCHIVE_WORKER_SHA256='0' * 64)
command = ['cargo', 'build', '--locked', '--release', '-p', 'bigbrotr', '--bin', 'bigbrotr']
assert execute([('rebind', command)], out / 'rebind', SOURCE, wrong_env, identity, 300)['status'] == 'PASS'
wrong = build / 'target/release/bigbrotr'
wrong_info = json.loads(subprocess.check_output([str(wrong), 'build-info'], env={}))
assert wrong_info['archive_worker']['sha256'] == '0' * 64
done = subprocess.run([str(wrong), 'archive', 'verify-worker', '--root', str(bundle)], env={}, capture_output=True, timeout=45)
assert done.returncode == 4 and not done.stdout and b'archive worker verification refused' in done.stderr
assert execute([('unbind', command)], out / 'unbind', SOURCE, env, identity, 300)['status'] == 'PASS'
unbound_info = json.loads(subprocess.check_output([str(wrong), 'build-info'], env={}))
assert 'archive_worker' in unbound_info and unbound_info['archive_worker'] is None
done = subprocess.run([str(wrong), 'archive', 'verify-worker', '--root', str(bundle)], env={}, capture_output=True, timeout=45)
assert done.returncode == 4 and not done.stdout and b'missing embedded worker identity' in done.stderr
assert execute([('restore', command)], out / 'restore', SOURCE, bound_env, identity, 300)['status'] == 'PASS'
assert file_hash(wrong) == file_hash(binary)
assert source_files() == manifest['files_sha256']
result = dict(identity, status='PASS', binary_sha256=file_hash(binary), worker=worker, build_info=info,
              assets=assets, installed_cases=len(cases['cases']), build_refusals=negative,
              cargo_rebinding_verified=True, cargo_unbinding_verified=True, source_unchanged=True)
(out / 'ready.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps({'status': 'PASS', 'installed_cases': len(cases['cases']), 'build_refusals': len(negative)}), flush=True)
