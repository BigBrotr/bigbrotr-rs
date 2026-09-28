#!/usr/bin/env python3
"""Native DNS/policy component qualification; not installed acquisition acceptance."""
import json
from pathlib import Path
import shutil
import sys
import subprocess

sys.dont_write_bytecode = True
SOURCE = Path('/out/build/source')
sys.path.insert(0, str(SOURCE))
from tools.build import clean_environment, file_hash, native_toolchain
from tools.ci import execute

build = SOURCE.parent
manifest = json.loads((build / 'source-manifest.json').read_text())


def sources():
    return {str(p.relative_to(SOURCE)): file_hash(p)
            for p in sorted(SOURCE.rglob('*')) if p.is_file()}


assert sources() == manifest['files_sha256']
env = clean_environment()
selected, native = native_toolchain(env, 2)
env.update(selected)
env.update(CARGO_HOME=str(build / 'cargo-home'), CARGO_TARGET_DIR=str(build / 'target'),
           CARGO_NET_OFFLINE='true', BIGBROTR_SOURCE_REVISION=manifest['revision'],
           BIGBROTR_SOURCE_TREE=manifest['tree'], BIGBROTR_BUILD_PROFILE='release',
           BIGBROTR_SOURCE_MANIFEST_SHA256=file_hash(build / 'source-manifest.json'))
identity = dict(source=manifest['revision'], tree=manifest['tree'], native_toolchain=native,
                lockfile_sha256=file_hash(SOURCE / 'Cargo.lock'),
                source_manifest_sha256=file_hash(build / 'source-manifest.json'),
                scope='Native actual TCP DNS and pure destination policy; no installed acquisition, TLS/WS, Control authority or capacity claim')
identity['rustc'] = subprocess.check_output(['rustc', '--version', '--verbose'], env=env, text=True).strip()
assert 'host: x86_64-unknown-linux-gnu' in identity['rustc']
assert identity['rustc'].startswith('rustc 1.98.1 ')
tests = {'network_policy': 9, 'network_dns': 9, 'network_dns_semantics': 14}
selectors = [arg for name in tests for arg in ['--test', name]]
checks = Path('/out/build-checks')
result = execute([
    ('compile', ['cargo', 'test', '--locked', '--offline', '--release', '-p', 'bigbrotr',
                 *selectors, '--no-run', '--message-format=json']),
    ('clippy', ['cargo', 'clippy', '--locked', '--offline', '--release', '-p', 'bigbrotr',
                '--lib', *selectors, '--', '-D', 'warnings']),
], checks, SOURCE, env, identity, 1800)
assert result['status'] == 'PASS'
artifacts = Path('/out/artifacts')
artifacts.mkdir()
executables = {}
for line in (checks / 'compile.log').read_text().splitlines():
    if not line.startswith('{'):
        continue
    message = json.loads(line)
    if message.get('reason') != 'compiler-artifact' or not message.get('executable'):
        continue
    name = message['target']['name']
    if name in tests and message['target']['kind'] == ['test']:
        assert name not in executables
        executable = Path(message['executable'])
        assert executable.is_relative_to(build / 'target') and not executable.is_symlink()
        shutil.copy2(executable, artifacts / name)
        executables[name] = file_hash(artifacts / name)
assert executables.keys() == tests.keys()
identity['executables_sha256'] = executables
result = execute([(name, [str(artifacts / name), '--test-threads=1']) for name in tests],
                 Path('/out/tests'), SOURCE, env, identity, 60)
assert result['status'] == 'PASS'
for name, count in tests.items():
    assert f'test result: ok. {count} passed; 0 failed; 0 ignored;' in (Path('/out/tests') / (name + '.log')).read_text()
assert sources() == manifest['files_sha256']
assert all(file_hash(artifacts / name) == digest for name, digest in executables.items())
Path('/out/qualification.json').write_text(json.dumps(
    dict(identity, status='PASS_SCOPED_NETWORK_COMPONENTS', tests=tests), indent=2) + '\n')
