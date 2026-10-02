#!/usr/bin/env python3
"""Native worker/application admission checks and an authenticated installed CLI."""
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys

sys.dont_write_bytecode = True
SOURCE = Path('/out/build/source')
sys.path.insert(0, str(SOURCE))
from tools.archive_bundle import build_environment, install_bundle, native_target, verify_bundle
from tools.build import clean_environment, file_hash, native_toolchain
from tools.ci import execute

build = SOURCE.parent
manifest = json.loads((build / 'source-manifest.json').read_text())


def sources():
    return {str(p.relative_to(SOURCE)): file_hash(p)
            for p in sorted(SOURCE.rglob('*')) if p.is_file()}


assert sources() == manifest['files_sha256']
artifacts = Path('/out/artifacts')
artifacts.mkdir()
bundle = artifacts / 'archive-worker'
bundle.mkdir()
assets = install_bundle(Path('/archive-inputs'), bundle, native_target())
env = clean_environment()
selected, native = native_toolchain(env, 2)
env.update(selected)
env.update(build_environment(bundle))
env.update(CARGO_HOME=str(build / 'cargo-home'), CARGO_TARGET_DIR=str(build / 'target'),
           CARGO_NET_OFFLINE='true', BIGBROTR_SOURCE_REVISION=manifest['revision'],
           BIGBROTR_SOURCE_TREE=manifest['tree'], BIGBROTR_BUILD_PROFILE='release',
           BIGBROTR_SOURCE_MANIFEST_SHA256=file_hash(build / 'source-manifest.json'))
identity = dict(source=manifest['revision'], tree=manifest['tree'], native_toolchain=native,
                lockfile_sha256=file_hash(SOURCE / 'Cargo.lock'),
                source_manifest_sha256=file_hash(build / 'source-manifest.json'),
                scope='Separate application/child grants with actual native custody; installed publication remains separate')
identity['rustc'] = subprocess.check_output(['rustc', '--version', '--verbose'], env=env, text=True).strip()
assert 'host: x86_64-unknown-linux-gnu' in identity['rustc'] and identity['rustc'].startswith('rustc 1.98.1 ')
cargo = ['cargo', 'build', '--locked', '--offline', '--release']
result = execute([('worker', cargo + ['-p', 'bigbrotr-archive-worker', '--bin', 'bigbrotr-archive-worker'])],
                 Path('/out/worker-build'), SOURCE, env, identity, 1800)
assert result['status'] == 'PASS'
worker = build / 'target/release/bigbrotr-archive-worker'
worker_identity = dict(bytes=worker.stat().st_size, sha256=file_hash(worker))
shutil.copy2(worker, bundle / 'bigbrotr-archive-worker')
env.update(BIGBROTR_ARCHIVE_WORKER_BYTES=str(worker_identity['bytes']),
           BIGBROTR_ARCHIVE_WORKER_SHA256=worker_identity['sha256'])
targets = ['-p', 'bigbrotr', '--bin', 'bigbrotr', '--example', 'ingest_bootstrap_probe',
           '--example', 'archive_publication_probe']
steps = [('compile', cargo + targets),
         ('clippy', ['cargo', 'clippy', '--locked', '--offline', '--release', *targets,
                     '--lib', '--', '-D', 'warnings'])]
result = execute(steps, Path('/out/build-checks'), SOURCE, env, identity, 1800)
assert result['status'] == 'PASS'
for name, relative in [('bigbrotr', 'bigbrotr'),
                       ('ingest_bootstrap_probe', 'examples/ingest_bootstrap_probe'),
                       ('archive_publication_probe', 'examples/archive_publication_probe')]:
    shutil.copy2(build / 'target/release' / relative, artifacts / name)
info = json.loads(subprocess.check_output([str(artifacts / 'bigbrotr'), 'build-info'], env=env))
assert info['source'] == dict(state='builder-supplied', revision=manifest['revision'],
                            tree=manifest['tree'], dirty=False,
                            manifest_sha256=identity['source_manifest_sha256'])
assert info['archive_worker'] == worker_identity
verified = json.loads(subprocess.check_output([str(artifacts / 'bigbrotr'), 'archive',
                                              'verify-worker', '--root', str(bundle)], env=env))
assert verified['result'] == 'verified' and verified['executable'] == worker_identity
receipt = dict(identity, format='bigbrotr-registry-build/1', build_info=info,
               binary_sha256=file_hash(artifacts / 'bigbrotr'),
               probe_name='ingest_bootstrap_probe',
               probe_sha256=file_hash(artifacts / 'ingest_bootstrap_probe'),
               archive_worker=worker_identity, archive_assets=assets,
               publication_probe_sha256=file_hash(artifacts / 'archive_publication_probe'))
(artifacts / 'build-result.json').write_text(json.dumps(receipt, indent=2) + '\n')
tests = ['cargo', 'test', '--locked', '--offline', '--release', '-p', 'bigbrotr',
         '--lib', '--message-format=json']
result = execute([
    ('native-custody', tests + ['native_worker_retains_custody_and_separate_process_budgets_until_reaped',
                               '--', '--ignored', '--test-threads=1']),
    ('worker-budgets', tests + ['storage::archive::worker::tests', '--', '--test-threads=1']),
    ('cut-admission', tests + ['storage::archive::cut::tests', '--', '--test-threads=1']),
    ('initial-supervision', ['cargo', 'test', '--locked', '--offline', '--release',
        '-p', 'bigbrotr', '--bin', 'bigbrotr', '--message-format=json',
        'interfaces::serve::initial_ingest::', '--', '--test-threads=1']),
], Path('/out/budget-checks'), SOURCE, env, identity, 1800)
# Preserve the actual executable even on RED, independently of the mutable cache.
executables = {}
counts = {}
for check in result['checks']:
    path = Path('/out/budget-checks') / (check['name'] + '.log')
    if not path.exists():
        continue
    content = path.read_text()
    match = re.search(r'test result: ok\. ([1-9][0-9]*) passed; 0 failed; 0 ignored;', content)
    if match:
        counts[check['name']] = int(match.group(1))
    for line in content.splitlines():
        if not line.startswith('{'):
            continue
        item = json.loads(line)
        if item.get('reason') == 'compiler-artifact' and item.get('executable') \
                and item['profile']['test']:
            kind = item['target']['kind']
            assert kind in (['lib'], ['bin'])
            name = 'archive_worker_components' if kind == ['lib'] else 'initial_supervision_components'
            executable = item['executable']
            assert executables.get(name, executable) == executable
            executables[name] = executable
assert executables
for name, executable in executables.items():
    executable = Path(executable)
    assert executable.is_relative_to(build / 'target') and not executable.is_symlink()
    shutil.copy2(executable, artifacts / name)
receipt['test_executables_sha256'] = {name: file_hash(artifacts / name) for name in executables}
receipt['native_tests_passed'] = counts
(artifacts / 'build-result.json').write_text(json.dumps(receipt, indent=2) + '\n')
assert sources() == manifest['files_sha256']
assert verify_bundle(bundle, native_target()) == assets
assert file_hash(bundle / 'bigbrotr-archive-worker') == worker_identity['sha256']
assert result['status'] == 'PASS'
assert counts.get('native-custody') == 1 and counts.get('worker-budgets', 0) > 0
assert counts.get('cut-admission') == 1 and counts.get('initial-supervision', 0) >= 6
Path('/out/qualification.json').write_text(json.dumps(dict(identity, status='PASS_ARCHIVE_WORKER_BUDGETS'), indent=2) + '\n')
