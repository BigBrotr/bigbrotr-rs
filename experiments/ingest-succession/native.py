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
from tools.archive_bundle import build_environment, install_bundle, native_target, verify_bundle

build = SOURCE.parent
manifest = json.loads((build / 'source-manifest.json').read_text())
assert sys.argv[1:] in ([], ['lineage'], ['schema'], ['replay'], ['history'], ['history-probe'], ['transition'], ['transition-probe'], ['writer'], ['writer-followup'], ['recovery'], ['installed'], ['archive-components'], ['archive-installed'], ['producer-progress'])
producer_progress = sys.argv[1:] == ['producer-progress']
archive_installed = sys.argv[1:] == ['archive-installed']
archive_components = sys.argv[1:] == ['archive-components']
installed = sys.argv[1:] == ['installed']
recovery = sys.argv[1:] == ['recovery']
writer_followup = sys.argv[1:] == ['writer-followup']
writer = sys.argv[1:] == ['writer'] or writer_followup
replay = sys.argv[1:] == ['replay']
history = sys.argv[1:] == ['history']
transition_probe = sys.argv[1:] == ['transition-probe']
probe = sys.argv[1:] == ['history-probe'] or transition_probe
transition = sys.argv[1:] == ['transition']
build_probe = probe or transition or writer or recovery or installed or archive_installed or producer_progress
lineage = sys.argv[1:] == ['lineage']
schema = sys.argv[1:] == ['schema'] or replay or history or build_probe


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
if replay:
    identity['scope'] = ('Ordered native lineage replay, affected legacy compatibility and schema/CLI build; '
                         'SQL, connected succession and installed journeys NOT_RUN')
if history:
    identity['scope'] = ('Typed PostgreSQL history state discriminators, native lineage/legacy handoff '
                         'components and schema/CLI build; connected history and succession NOT_RUN')
if probe:
    identity['scope'] = ('Exact finite connected-history probe and CLI compilation/Clippy only; '
                         'no Rust test or connected engine executed by this profile')
if transition:
    identity['scope'] = ('Native successor handoff/empty and PostgreSQL transition components, '
                         'CLI/probe compilation and Clippy; connected mutations and installed journeys NOT_RUN')
if transition_probe:
    identity['scope'] = ('Exact finite successor transition probe and CLI compilation/Clippy only; '
                         'no Rust test or connected engine executed by this profile')
if writer:
    identity['scope'] = ('Continuous native successor writer/frontier, shared admission/dictionary/input '
                         'and PostgreSQL activation/binding components; CLI/probe compilation and Clippy. '
                         'Packaged Archive-worker test, connected activation and installed journeys NOT_RUN')
if writer_followup:
    identity['scope'] = ('Only previously unexecuted dictionary/input and PostgreSQL activation components, '
                         'CLI/probe compilation and Clippy. Native admission/writer checks are not repeated; '
                         'reuse requires separately bound unchanged product inputs and raw per-test results.')
if recovery:
    identity['scope'] = ('Activated successor native/PG recovery and affected lineage/input/prefix/legacy '
                         'components, CLI/probe compilation and Clippy. Native fixtures use explicit '
                         'SQL activation/copy premises; connected recovery and installed journeys NOT_RUN.')
if installed:
    identity['scope'] = ('Installed successor CLI/owner supervision and entrypoint checks, CLI/probe '
                         'compilation and Clippy. Actual PostgreSQL/HTTP/Sync lifetimes NOT_RUN; '
                         'unchanged native/PG component evidence remains separately bound.')
if archive_components:
    identity['scope'] = ('Proof-bound historical pending reads, retained PostgreSQL frontier reconstruction, '
                         'and Archive receipt/membership/initialization library components plus library Clippy. '
                         'CLI helper composition, connected SQL/native Archive and installed catch-up NOT_RUN.')
if archive_installed:
    identity['scope'] = ('Exact packaged Archive worker and embedded CLI identity; retained frontier journal, '
                         'installed Serve composition and early CLI components plus Clippy. '
                         'Connected SQL/native Archive catch-up, read/export and restart NOT_RUN.')
if producer_progress:
    identity['scope'] = ('Typed producer progress reducers, native checkpoint witness/certification, '
                         'Sync attempt/prefix and schema/CLI components, CLI/probe compilation and Clippy. '
                         'Connected PostgreSQL progress, installed Sync/recovery and ACK NOT_RUN.')
assert identity['rustc'].startswith('rustc 1.98.1 ') and 'host: x86_64-unknown-linux-gnu' in identity['rustc']
artifacts = Path('/out/artifacts')
if archive_installed:
    artifacts.mkdir()
    bundle = artifacts / 'archive-worker'
    bundle.mkdir()
    assets = install_bundle(Path('/archive-inputs'), bundle, native_target())
    archive_environment = build_environment(bundle)
    env.update(archive_environment)
    identity.update(archive_assets=assets, archive_build_environment=archive_environment,
                    archive_registry_sha256=file_hash(SOURCE / 'contracts/release/archive-engine.json'))

    def archive_check_inputs(stage):
        observation = dict(stage=stage, status='RUNNING')
        try:
            observation.update(source_files_sha256=sources(),
                source_manifest_sha256=file_hash(build / 'source-manifest.json'),
                native_tools_sha256={name: file_hash(Path(tool['path'])) for name, tool in native['tools'].items()},
                input_assets=verify_bundle(Path('/archive-inputs'), native_target()),
                installed_assets=verify_bundle(bundle, native_target()))
            assert observation['source_files_sha256'] == manifest['files_sha256']
            assert observation['source_manifest_sha256'] == identity['source_manifest_sha256']
            assert observation['native_tools_sha256'] == {name: tool['sha256'] for name, tool in native['tools'].items()}
            assert observation['input_assets'] == observation['installed_assets'] == assets
            observation['status'] = 'PASS'
        except BaseException as error:
            observation.update(status='FAIL', error=type(error).__name__ + ': ' + str(error))
            raise
        finally:
            (artifacts / ('archive-inputs-' + stage + '.json')).write_text(json.dumps(observation, indent=2) + '\n')

    archive_check_inputs('before')
    worker_result = execute([('worker', ['cargo', 'build', '--locked', '--offline', '--release',
        '-p', 'bigbrotr-archive-worker', '--bin', 'bigbrotr-archive-worker', '--message-format=json'])],
        Path('/out/worker-build'), SOURCE, env, identity, 1800)
    worker_paths = set()
    worker_log = Path('/out/worker-build/worker.log')
    for line in worker_log.read_text().splitlines() if worker_log.exists() else []:
        if not line.startswith('{'):
            continue
        item = json.loads(line)
        if (item.get('reason') == 'compiler-artifact' and item.get('executable')
                and item['target']['kind'] == ['bin'] and item['target']['name'] == 'bigbrotr-archive-worker'
                and item['profile']['test'] is False):
            assert Path(item['target']['src_path']) == SOURCE / 'apps/bigbrotr-archive-worker/src/main.rs'
            worker_paths.add(item['executable'])
    worker_identity = None
    if worker_paths:
        assert len(worker_paths) == 1
        worker = Path(worker_paths.pop())
        assert worker.is_relative_to(build / 'target') and not worker.is_symlink()
        with worker.open('rb') as stream:
            assert stream.read(4) == b'\x7fELF'
        shutil.copy2(worker, bundle / 'bigbrotr-archive-worker')
        worker_identity = dict(bytes=worker.stat().st_size, sha256=file_hash(worker))
        assert file_hash(bundle / 'bigbrotr-archive-worker') == worker_identity['sha256']
    (artifacts / 'worker-result.json').write_text(json.dumps(dict(identity,
        status=worker_result['status'], archive_worker=worker_identity), indent=2) + '\n')
    archive_check_inputs('after-worker')
    assert worker_result['status'] == 'PASS' and worker_identity is not None
    identity['archive_worker'] = worker_identity
    env.update(BIGBROTR_ARCHIVE_WORKER_BYTES=str(worker_identity['bytes']),
               BIGBROTR_ARCHIVE_WORKER_SHA256=worker_identity['sha256'])
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
if replay:
    tests += [(name, ['--lib', name + '_tests']) for name in [
        'lineage', 'request_input', 'initial_handoff', 'dictionary_append',
        'producer_recovery', 'source_closure', 'activated_recovery', 'activated_source']]
if history:
    tests += [('successor_history', ['--lib', 'successor_history::tests'])]
    tests += [(name, ['--lib', name + '_tests']) for name in [
        'lineage', 'initial_handoff', 'initial_empty']]
if probe:
    tests = []
if transition:
    tests = [('lineage', ['--lib', 'lineage_tests']),
             ('successor_transition', ['--lib', 'successor_transition::tests'])]
if writer:
    # Admission owns nested replay/lineage/handoff/writer fixtures too; select it
    # once instead of repeating overlapping nested filters.
    tests = [(name, ['--lib', name]) for name in [
        'admission_tests', 'dictionary_append_tests', 'request_input_tests',
        'successor_transition']]
    if writer_followup:
        tests = tests[1:]
if recovery:
    # Lineage includes the successor writer/recovery groups. Activated recovery
    # includes its nested source/retirement groups; do not execute them twice.
    tests = [(name, ['--lib', name]) for name in [
        'lineage_tests', 'request_input_tests', 'dictionary_append_tests',
        'producer_recovery_tests', 'source_closure_tests', 'activated_recovery_tests',
        'successor_history::tests', 'successor_transition']]
if installed or archive_installed:
    tests = [('serve_owner', ['--bin', 'bigbrotr', 'interfaces::serve::']),
             ('control_runtime_entrypoint', ['--test', 'control_runtime_entrypoint'])]
    if archive_installed:
        tests.insert(0, ('frontier_intents', ['--lib', 'frontier_intents']))
if archive_components:
    tests = [(name, ['--lib', selection]) for name, selection in [
        ('historical_pending', 'lineage_pending_tests'),
        ('retained_frontier', 'archive_retained_frontier::tests'),
        ('archive_receipts', 'storage::archive::receipts::tests'),
        ('archive_membership', 'storage::archive::membership::tests'),
        ('archive_custody', 'storage::archive::initialization::tests')]]
if producer_progress:
    tests = [
        ('producer_progress', ['--lib', 'storage::postgres::control_owner::producer_progress::tests::']),
        ('producer_checkpoint', ['--lib', 'storage::rocks::store::producer_checkpoint::tests::']),
        ('certification', ['--lib', 'certification_tests']),
        ('sync_progress', ['--bin', 'bigbrotr', 'interfaces::serve::initial_ingest::sync::']),
        ('control_schema', ['--test', 'control_schema']),
        ('migrate_entrypoint', ['--test', 'migrate_entrypoint']),
    ]
steps = ([('probe_build', ['cargo', 'build', '--locked', '--offline', '--release', '-p', 'bigbrotr',
                          '--bin', 'bigbrotr', '--example', 'ingest_bootstrap_probe', '--message-format=json'])]
         if build_probe else [] if archive_components else
         [('independent-fixture', ['python3', '-B', 'experiments/ingest-succession/generate.py'])])
steps += [(name, ['cargo', 'test', '--locked', '--offline', '--release', '-p', 'bigbrotr',
                  *selection, '--message-format=json', '--', '--test-threads=1',
                  *(['--skip', 'signed_pending_reopens_and_reaches_the_supervised_native_worker_without_acknowledgement']
                    if writer and name == 'admission_tests' else [])])
          for name, selection in tests]
clippy_targets = (['--lib', '--bin', 'bigbrotr', '--test', 'control_schema', '--test', 'migrate_entrypoint']
                  if schema else ['--lib', '--test', 'rocks_succession', '--test', 'rocks_manifest',
                                  '--test', 'rocks_keys', '--test', 'rocks_store'])
if build_probe:
    clippy_targets = ['--lib', '--bin', 'bigbrotr', '--example', 'ingest_bootstrap_probe']
if producer_progress:
    clippy_targets += ['--test', 'control_schema', '--test', 'migrate_entrypoint']
if installed or archive_installed:
    clippy_targets += ['--test', 'control_runtime_entrypoint']
if archive_components:
    clippy_targets = ['--lib']
steps += [('clippy', ['cargo', 'clippy', '--locked', '--offline', '--release', '-p', 'bigbrotr',
                     *clippy_targets, '--', '-D', 'warnings'])]
result = execute(steps, checks, SOURCE, env, identity, 1800)
# Preserve built test executables even when a later check fails. Cargo outputs
# are acceleration only; these copies and raw logs are the retained artifacts.
if not archive_installed:
    artifacts.mkdir()
executables, counts, cli_paths = {}, {}, set()
for name, selection in (tests + [('probe_build', None)] if build_probe else tests):
    log = checks / (name + '.log')
    if not log.exists():
        continue
    content = log.read_text()
    summaries = re.findall(r'test result: ok\. ([1-9][0-9]*) passed; 0 failed; 0 ignored;', content)
    if summaries:
        assert len(summaries) == 1
        counts[name] = int(summaries[0])
    paths = set()
    kind = ['example'] if selection is None else [selection[0][2:]]
    target_name = 'ingest_bootstrap_probe' if selection is None else (
        selection[1] if selection[0] == '--bin' else name)
    for line in content.splitlines():
        if not line.startswith('{'):
            continue
        message = json.loads(line)
        if (schema and message.get('reason') == 'compiler-artifact' and message.get('executable')
                and message['target']['kind'] == ['bin'] and message['target']['name'] == 'bigbrotr'
                and not message['profile']['test']):
            cli_paths.add(message['executable'])
        if (message.get('reason') == 'compiler-artifact' and message.get('executable')
                and message['profile']['test'] == (selection is not None)
                and message['target']['kind'] == kind
                and (kind == ['lib'] or message['target']['name'] == target_name)):
            paths.add(message['executable'])
    if not paths:
        continue
    assert len(paths) == 1
    executable = Path(paths.pop())
    assert executable.is_relative_to(build / 'target') and not executable.is_symlink()
    with executable.open('rb') as stream:
        assert stream.read(4) == b'\x7fELF'
    retained_name = 'native_components' if kind == ['lib'] else (
        name if kind == ['bin'] else target_name)
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
archive_postcheck = None
if archive_installed:
    archive_check_inputs('after')
    assert file_hash(bundle / 'bigbrotr-archive-worker') == worker_identity['sha256']
    assert (bundle / 'bigbrotr-archive-worker').stat().st_size == worker_identity['bytes']
    # Retain every available ELF first. Postchecks also run if a later component
    # or Clippy failed, but never authorize a partial build as qualification.
    if 'bigbrotr' in executables:
        archive_postcheck = execute([
            ('build-info', [str(artifacts / 'bigbrotr'), 'build-info']),
            ('verify-worker', [str(artifacts / 'bigbrotr'), 'archive', 'verify-worker', '--root', str(bundle)]),
        ], Path('/out/archive-postchecks'), SOURCE, env, identity, 120)
        info, verified = None, None
        if archive_postcheck['status'] == 'PASS':
            info = json.loads(Path('/out/archive-postchecks/build-info.log').read_text())
            verified = json.loads(Path('/out/archive-postchecks/verify-worker.log').read_text())
        (artifacts / 'build-result.json').write_text(json.dumps(dict(identity,
            binary_sha256=executables['bigbrotr'], build_info=info, archive_verification=verified,
            postcheck_status=archive_postcheck['status']), indent=2) + '\n')
        assert archive_postcheck['status'] == 'PASS'
        assert info['source'] == dict(state='builder-supplied', revision=manifest['revision'],
            tree=manifest['tree'], dirty=False, manifest_sha256=identity['source_manifest_sha256'])
        assert info['archive_worker'] == worker_identity
        assert verified['result'] == 'verified' and verified['executable'] == worker_identity
        for name in ('Cargo.lock', 'rust-toolchain.toml', 'contracts/release/dependencies.json',
                     'contracts/protocol/versions.json', 'contracts/protocol/nip01-event.json'):
            assert info['inputs_sha256'][name] == manifest['files_sha256'][name]
for tool in native['tools'].values():
    assert file_hash(Path(tool['path'])) == tool['sha256']
assert sources() == manifest['files_sha256']
receipt = dict(identity, status=result['status'], test_counts=counts,
               executables_sha256=executables,
               independent_fixture_sha256=file_hash(SOURCE / 'experiments/ingest-succession/portable-handoff-v2.json'))
(artifacts / 'component-result.json').write_text(json.dumps(receipt, indent=2) + '\n')
assert result['status'] == 'PASS'
assert set(counts) == {name for name, _ in tests}
assert set(executables) == {'native_components' if selection[0] == '--lib' else name
                            for name, selection in tests} | ({'bigbrotr'} if schema else set()) | (
                                {'ingest_bootstrap_probe'} if build_probe else set())
if schema and not archive_installed:
    info = json.loads(subprocess.check_output([str(artifacts / 'bigbrotr'), 'build-info'], env=env))
    assert info['source'] == dict(state='builder-supplied', revision=manifest['revision'],
        tree=manifest['tree'], dirty=False, manifest_sha256=identity['source_manifest_sha256'])
    (artifacts / 'build-result.json').write_text(json.dumps(dict(identity,
        binary_sha256=executables['bigbrotr'], build_info=info), indent=2) + '\n')
if archive_installed:
    assert archive_postcheck is not None and archive_postcheck['status'] == 'PASS'
qualification = dict(identity, status='PASS_ARCHIVE_CATCH_UP_INSTALLED_COMPONENTS' if archive_installed
                     else 'PASS_ARCHIVE_CATCH_UP_COMPONENTS' if archive_components
                     else 'PASS_PRODUCER_PROGRESS_COMPONENTS' if producer_progress
                     else 'PASS_SUCCESSION_INSTALLED_COMPONENTS' if installed
                     else 'PASS_SUCCESSION_RECOVERY_COMPONENTS' if recovery
                     else 'PASS_SUCCESSION_WRITER_FOLLOWUP_COMPONENTS' if writer_followup
                     else 'PASS_SUCCESSION_WRITER_COMPONENTS' if writer
                     else 'PASS_SUCCESSION_TRANSITION_COMPONENTS' if transition
                     else 'PASS_SUCCESSION_TRANSITION_PROBE_BUILD' if transition_probe
                     else 'PASS_SUCCESSION_HISTORY_PROBE_BUILD' if probe
                     else 'PASS_SUCCESSION_HISTORY_COMPONENTS' if history
                     else 'PASS_SUCCESSION_REPLAY_SCHEMA_COMPONENTS' if replay
                     else 'PASS_SUCCESSION_SCHEMA_COMPONENTS' if schema
                     else 'PASS_SUCCESSION_LINEAGE_INSPECTION' if lineage
                     else 'PASS_SUCCESSION_CODEC_COMPONENTS', test_counts=counts)
(Path('/out') / 'qualification.json').write_text(json.dumps(qualification, indent=2) + '\n')
print(json.dumps(qualification))
