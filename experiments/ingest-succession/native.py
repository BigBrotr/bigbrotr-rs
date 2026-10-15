#!/usr/bin/env python3
"""Exact-source codec or structural-lineage checks; no connected succession claim."""
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys

sys.dont_write_bytecode = True
SOURCE = Path('/out/build/source')
sys.path.insert(0, str(SOURCE))
from tools.build import clean_environment, file_hash, native_toolchain
from tools.ci import execute

build = SOURCE.parent
manifest = json.loads((build / 'source-manifest.json').read_text())
assert sys.argv[1:] in ([], ['lineage'], ['schema'])
lineage = sys.argv[1:] == ['lineage']
schema = sys.argv[1:] == ['schema']


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
                scope='Portable version2/schema3 codec, profile arithmetic and legacy handoff compatibility only; positive lineage, PostgreSQL and installed repeatable succession NOT_RUN')
identity['rustc'] = subprocess.check_output(['rustc', '--version', '--verbose'], env=env, text=True).strip()
if lineage:
    identity['scope'] = ('Read-only structural lineage, complete input census and selected legacy compatibility; '
                         'positive whole-lineage replay, PostgreSQL and installed repeatable succession NOT_RUN')
if schema:
    identity['scope'] = ('Additive migration catalog and early CLI checks; retained exact CLI for separate '
                         'PostgreSQL qualification. SQL and connected succession NOT_RUN')
assert identity['rustc'].startswith('rustc 1.98.1 ') and 'host: x86_64-unknown-linux-gnu' in identity['rustc']
checks = Path('/out/build-checks')
tests = [('rocks_succession', ['--test', 'rocks_succession']),
         ('rocks_manifest', ['--test', 'rocks_manifest']),
         ('rocks_keys', ['--test', 'rocks_keys']),
         ('rocks_store', ['--test', 'rocks_store',
                          'schema_three_is_inspectable_but_cannot_initialize_or_recreate_initial_candidate']),
         ('legacy_handoff', ['--lib', 'initial_handoff_tests']),
         ('legacy_bootstrap', ['--lib', 'bootstrap_tests']),
         ('legacy_closing', ['--lib', 'producer_closing_tests']),
         ('legacy_recovery', ['--lib', 'producer_recovery_tests'])]
if lineage:
    tests = [('lineage', ['--lib', 'lineage_tests']),
             ('request_inputs', ['--lib', 'request_input_tests']),
             ('legacy_handoff', ['--lib', 'initial_handoff_tests'])]
if schema:
    tests = [(name, ['--test', name]) for name in ['control_schema', 'migrate_entrypoint']]
steps = [('independent-fixture', ['python3', '-B', 'experiments/ingest-succession/generate.py'])]
steps += [(name, ['cargo', 'test', '--locked', '--offline', '--release', '-p', 'bigbrotr',
                  *selection, '--message-format=json', '--', '--test-threads=1'])
          for name, selection in tests]
clippy_targets = (['--bin', 'bigbrotr', '--test', 'control_schema', '--test', 'migrate_entrypoint']
                  if schema else ['--lib', '--test', 'rocks_succession', '--test', 'rocks_manifest',
                                  '--test', 'rocks_keys', '--test', 'rocks_store'])
steps += [('clippy', ['cargo', 'clippy', '--locked', '--offline', '--release', '-p', 'bigbrotr',
                     *clippy_targets, '--', '-D', 'warnings'])]
result = execute(steps, checks, SOURCE, env, identity, 1800)
# Preserve built test executables even when a later check fails. Cargo outputs
# are acceleration only; these copies and raw logs are the retained artifacts.
artifacts = Path('/out/artifacts')
artifacts.mkdir()
executables, counts, cli_paths = {}, {}, set()
for name, selection in tests:
    log = checks / (name + '.log')
    if not log.exists():
        continue
    content = log.read_text()
    summaries = re.findall(r'test result: ok\. ([1-9][0-9]*) passed; 0 failed; 0 ignored;', content)
    if summaries:
        assert len(summaries) == 1
        counts[name] = int(summaries[0])
    paths = set()
    kind = ['lib'] if selection[0] == '--lib' else ['test']
    for line in content.splitlines():
        if not line.startswith('{'):
            continue
        message = json.loads(line)
        if (schema and message.get('reason') == 'compiler-artifact' and message.get('executable')
                and message['target']['kind'] == ['bin'] and message['target']['name'] == 'bigbrotr'
                and not message['profile']['test']):
            cli_paths.add(message['executable'])
        if (message.get('reason') == 'compiler-artifact' and message.get('executable')
                and message['profile']['test'] and message['target']['kind'] == kind
                and (kind == ['lib'] or message['target']['name'] == name)):
            paths.add(message['executable'])
    if not paths:
        continue
    assert len(paths) == 1
    executable = Path(paths.pop())
    assert executable.is_relative_to(build / 'target') and not executable.is_symlink()
    with executable.open('rb') as stream:
        assert stream.read(4) == b'\x7fELF'
    retained_name = 'native_components' if kind == ['lib'] else name
    retained = artifacts / retained_name
    if not retained.exists():
        shutil.copy2(executable, retained)
    executables[retained_name] = file_hash(retained)
    assert executables[retained_name] == file_hash(executable)
if cli_paths:
    assert len(cli_paths) == 1
    executable = Path(cli_paths.pop())
    assert executable.is_relative_to(build / 'target') and not executable.is_symlink()
    with executable.open('rb') as stream:
        assert stream.read(4) == b'\x7fELF'
    shutil.copy2(executable, artifacts / 'bigbrotr')
    executables['bigbrotr'] = file_hash(artifacts / 'bigbrotr')
for tool in native['tools'].values():
    assert file_hash(Path(tool['path'])) == tool['sha256']
assert sources() == manifest['files_sha256']
receipt = dict(identity, status=result['status'], test_counts=counts,
               executables_sha256=executables,
               independent_fixture_sha256=file_hash(SOURCE / 'experiments/ingest-succession/portable-handoff-v2.json'))
(artifacts / 'component-result.json').write_text(json.dumps(receipt, indent=2) + '\n')
assert result['status'] == 'PASS'
assert set(counts) == {name for name, _ in tests}
assert set(executables) == {name if selection[0] == '--test' else 'native_components'
                            for name, selection in tests} | ({'bigbrotr'} if schema else set())
if schema:
    info = json.loads(subprocess.check_output([str(artifacts / 'bigbrotr'), 'build-info'], env=env))
    assert info['source'] == dict(state='builder-supplied', revision=manifest['revision'],
        tree=manifest['tree'], dirty=False, manifest_sha256=identity['source_manifest_sha256'])
    (artifacts / 'build-result.json').write_text(json.dumps(dict(identity,
        binary_sha256=executables['bigbrotr'], build_info=info), indent=2) + '\n')
qualification = dict(identity, status='PASS_SUCCESSION_SCHEMA_COMPONENTS' if schema
                     else 'PASS_SUCCESSION_LINEAGE_INSPECTION' if lineage
                     else 'PASS_SUCCESSION_CODEC_COMPONENTS', test_counts=counts)
(Path('/out') / 'qualification.json').write_text(json.dumps(qualification, indent=2) + '\n')
print(json.dumps(qualification))
