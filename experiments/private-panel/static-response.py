#!/usr/bin/env python3
"""Scoped native TCP tests; run in an inventoried, network-none lab container."""
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True
source = Path('/out/build/source')
sys.path.insert(0, str(source))
from tools.build import clean_environment, native_toolchain, file_hash
from tools.archive_bundle import build_environment, install_bundle
from tools.ci import execute

build = source.parent
manifest = json.loads((build / 'source-manifest.json').read_text())


def originals_unchanged():
    return all((source / name).is_file() and file_hash(source / name) == digest
               for name, digest in manifest['files_sha256'].items())


assert originals_unchanged()
env = clean_environment()
selected, toolchain = native_toolchain(env, 2)
bundle = Path('/out/archive-worker')
bundle.mkdir()
install_bundle(Path('/assets'), bundle, 'x86_64-unknown-linux-gnu')
env.update(selected)
env.update(build_environment(bundle))
env.update(CARGO_HOME=str(build / 'cargo-home'), CARGO_TARGET_DIR=str(build / 'target'),
           CARGO_NET_OFFLINE='true', PYTHONDONTWRITEBYTECODE='1',
           BIGBROTR_SOURCE_REVISION=manifest['revision'],
           BIGBROTR_SOURCE_TREE=manifest['tree'], BIGBROTR_BUILD_PROFILE='release',
           BIGBROTR_SOURCE_MANIFEST_SHA256=file_hash(build / 'source-manifest.json'))
identity = dict(source=manifest['revision'], tree=manifest['tree'],
                cargo_lock=file_hash(source / 'Cargo.lock'), native_toolchain=toolchain,
                scope='Actual bounded static HTTP response, delivery gate and custody tests')
compiled = execute([
    ('compile', ['cargo', 'test', '--locked', '--offline', '--release', '-p', 'bigbrotr',
                 '--bin', 'bigbrotr', '--no-run', '--message-format=json']),
    ('clippy', ['cargo', 'clippy', '--locked', '--offline', '--release', '-p', 'bigbrotr',
                '--bin', 'bigbrotr', '--tests', '--', '-D', 'warnings']),
], Path('/out/compile-checks'), source, env, identity, 1800)
result = dict(identity, status='NOT_RUN', compilation=compiled)
if compiled['status'] == 'PASS':
    artifacts = []
    for line in Path('/out/compile-checks/compile.log').read_text().splitlines():
        if not line.startswith('{'):
            continue
        entry = json.loads(line)
        if (entry.get('reason') == 'compiler-artifact' and
                entry['target']['name'] == 'bigbrotr' and entry['profile']['test'] and
                entry.get('executable')):
            artifacts.append(Path(entry['executable']))
    assert len(artifacts) == 1
    executable = artifacts[0]
    assert executable.parent == build / 'target/release/deps'
    identity['test_executable_sha256'] = file_hash(executable)
    result = execute([('management', [str(executable), 'interfaces::management::'])],
                     Path('/out/socket-checks'), source, env, identity, 60)
    result['compilation'] = compiled
    log = Path('/out/socket-checks/management.log').read_text()
    result['expected_nonempty_tests'] = 'test result: ok. 18 passed; 0 failed; 0 ignored;' in log
    if not result['expected_nonempty_tests']:
        result['status'] = 'FAIL'
result['source_unchanged'] = originals_unchanged()
if not result['source_unchanged']:
    result['status'] = 'FAIL'
Path('/out/static-response-result.json').write_text(json.dumps(result, indent=2) + '\n')
raise SystemExit(0 if result['status'] == 'PASS' else 1)
