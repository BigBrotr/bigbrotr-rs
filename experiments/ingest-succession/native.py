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
assert sys.argv[1:] in ([], ['lineage'], ['schema'], ['replay'], ['history'], ['history-probe'], ['transition'], ['transition-probe'], ['writer'], ['writer-followup'], ['recovery'], ['installed'], ['archive-components'], ['archive-installed'], ['producer-progress'], ['producer-ack'], ['producer-ack-fault'], ['producer-floor'])
producer_floor = sys.argv[1:] == ['producer-floor']
producer_ack_fault = sys.argv[1:] == ['producer-ack-fault']
producer_ack = sys.argv[1:] == ['producer-ack']
producer_progress = sys.argv[1:] in (['producer-progress'], ['producer-ack'])
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
build_probe = probe or transition or writer or recovery or installed or archive_installed or producer_progress or producer_floor
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
                         'retained fact codec, captured-input/Sync prefix and schema/CLI components, '
                         'CLI/probe compilation and Clippy. '
                         'Connected PostgreSQL progress, installed Sync/recovery and ACK NOT_RUN.')
if producer_ack:
    identity['scope'] = ('Native ACK/noGC/credit mechanics and affected closing, original recovery, '
                         'legacy/activated/lineage, dictionary/input and installed consumer components; '
                         'schema40 catalog and CLI/probe compilation plus both Clippy selections. '
                         'Connected PG confirmation/ACK, installed two-slot traffic/loss/recovery, '
                         'resource capacity and Archive worker qualification NOT_RUN.')
if producer_floor:
    identity['scope'] = ('Non-destructive native compacted-prefix codec, outcome and scientific census '
                         'with affected lineage/input components; CLI/probe compilation and Clippy. '
                         'Connected PostgreSQL, installed compacted recovery, reclamation, '
                         'compacted successor compatibility and crash/restore qualification NOT_RUN.')
assert identity['rustc'].startswith('rustc 1.98.1 ') and 'host: x86_64-unknown-linux-gnu' in identity['rustc']
artifacts = Path('/out/artifacts')
if producer_ack_fault:
    # A new private test fixture needs only this ELF. Keep the installed product
    # artifacts and their qualification independent of this native fault case.
    identity['scope'] = ('Native ACK test-target compilation and library Clippy, then '
                         'one traced ACK fsync EIO and fresh-process replay. '
                         'No connected PG authority, media failure or power-loss qualification.')
    artifacts.mkdir()
    checks = Path('/out/build-checks')
    result = execute([
        ('compile', ['cargo', 'test', '--locked', '--offline', '--release', '-p',
                     'bigbrotr', '--lib', '--no-run', '--message-format=json']),
        ('clippy', ['cargo', 'clippy', '--locked', '--offline', '--release', '-p',
                    'bigbrotr', '--lib', '--', '-D', 'warnings']),
    ], checks, SOURCE, env, identity, 1800)
    paths = set()
    log = checks / 'compile.log'
    for line in log.read_text().splitlines() if log.exists() else []:
        if not line.startswith('{'):
            continue
        message = json.loads(line)
        if (message.get('reason') == 'compiler-artifact' and message.get('executable')
                and message['target']['name'] == 'bigbrotr'
                and message['target']['kind'] == ['lib'] and message['profile']['test']):
            assert Path(message['target']['src_path']) == SOURCE / 'apps/bigbrotr/src/lib.rs'
            paths.add(message['executable'])
    executables = {}
    if paths:
        assert len(paths) == 1
        executable = Path(paths.pop())
        assert executable.is_relative_to(build / 'target') and not executable.is_symlink()
        with executable.open('rb') as stream:
            assert stream.read(4) == b'\x7fELF'
        retained = artifacts / 'native_components'
        shutil.copy2(executable, retained)
        assert file_hash(retained) == file_hash(executable)
        executables['native_components'] = file_hash(retained)
    assert sources() == manifest['files_sha256']
    assert all(file_hash(Path(tool['path'])) == tool['sha256'] for tool in native['tools'].values())
    built = result['status'] == 'PASS' and set(executables) == {'native_components'}
    receipt = dict(identity, executables_sha256=executables,
                   status='PASS_PRODUCER_ACK_FAULT_BUILD_ONLY' if built else 'FAIL',
                   tests_executed=0, clippy_scope='library; cfg(test) fixture compiled, not linted')
    (artifacts / 'ack-fault-build.json').write_text(json.dumps(receipt, indent=2) + '\n')
    qualification = dict(receipt, status='INCOMPLETE' if built else 'FAIL',
                         runtime_status='NOT_RUN')
    qualification_path = Path('/out/qualification.json')
    qualification_path.write_text(json.dumps(qualification, indent=2) + '\n')
    assert built
    try:
        runtime = execute([('ack-fsync', [sys.executable, '-B',
            str(SOURCE / 'experiments/archive-engine/publication_sync_cases.py'),
            '--native-ack', '/out'])], Path('/out/fault-checks'), SOURCE, env, identity, 120)
        qualification['runtime_status'] = runtime['status']
        assert runtime['status'] == 'PASS'
        evidence = Path('/out/ack-sync-cases/result.json')
        assert json.loads(evidence.read_text())['status'] == 'PASS_NATIVE_ACK_FSYNC_EIO'
        assert file_hash(artifacts / 'native_components') == executables['native_components']
        assert sources() == manifest['files_sha256']
        qualification.update(status='PASS_PRODUCER_ACK_FAULT',
                             runtime_result_sha256=file_hash(evidence))
    except BaseException:
        qualification['status'] = 'FAIL'
        raise
    finally:
        qualification_path.write_text(json.dumps(qualification, indent=2) + '\n')
    print(json.dumps(qualification))
    sys.exit(0)
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
        ('producer_checkpoint', ['--lib', 'storage::rocks::store::admission_tests::producer_checkpoint_tests::']),
        ('certification', ['--lib', 'certification_tests']),
        ('captured_input', ['--lib', 'acquisition::preparation::tests::']),
        ('captured_binding', ['--lib', 'acquisition::session::preparation_tests::']),
        ('sync_progress', ['--bin', 'bigbrotr', 'interfaces::serve::initial_ingest::sync::']),
        ('fact_codec', ['--test', 'fact_codec']),
        ('evidence_intent', ['--test', 'evidence_intent']),
        ('control_schema', ['--test', 'control_schema']),
        ('migrate_entrypoint', ['--test', 'migrate_entrypoint']),
    ]
if producer_ack:
    # Acknowledged state changes every retained recovery boundary. The initial
    # reconstruction group includes activated/lineage/writer/recovery children;
    # select it once instead of overlapping those nested filters.
    tests += [(name, ['--lib', selection]) for name, selection in [
        ('producer_ack', 'producer_ack_tests'),
        ('producer_closing', 'producer_closing_tests'),
        ('producer_recovery', 'producer_recovery_tests'),
        ('source_closure', 'source_closure_tests'),
        ('initial_handoff', 'initial_handoff_tests'),
        ('initial_reconstruction', 'initial_reconstruction_tests'),
        ('dictionary_append', 'dictionary_append_tests'),
        ('bootstrap', 'bootstrap_tests'),
        ('request_inputs', 'request_input_tests'),
    ]]
    # Reuse the same retained CLI-test ELF; include changed Archive retention
    # and the existing Sync controls in one non-overlapping consumer selection.
    tests = [(name, ['--bin', 'bigbrotr', 'interfaces::serve::initial_ingest::']
              if name == 'sync_progress' else selection) for name, selection in tests]
if producer_floor:
    tests = [(name, ['--lib', selection]) for name, selection in [
        ('compacted_prefix', 'storage::rocks::store::admission_tests::compacted_prefix_tests::'),
        ('compacted_progress', 'storage::postgres::control_owner::producer_progress::recovery::tests::'),
        ('successor_history', 'storage::postgres::control_owner::successor_history::tests::'),
        ('successor_transition', 'storage::postgres::control_owner::successor_transition::tests::'),
        ('archive_retained_frontier', 'storage::postgres::control_owner::archive_retained_frontier::tests::'),
        ('initial_reconstruction', 'initial_reconstruction_tests'),
        ('request_inputs', 'request_input_tests'),
    ]]
    tests += [('claim_custody', ['--bin', 'bigbrotr',
               'interfaces::serve::initial_ingest::recovery::successor_family::tests::']),
              ('control_runtime_entrypoint', ['--test', 'control_runtime_entrypoint'])]
steps = ([('probe_build', ['cargo', 'build', '--locked', '--offline', '--release', '-p', 'bigbrotr',
                          '--bin', 'bigbrotr', '--example', 'ingest_bootstrap_probe', '--message-format=json'])]
         if build_probe else [] if archive_components else
         [('independent-fixture', ['python3', '-B', 'experiments/ingest-succession/generate.py'])])
steps += [(name, ['cargo', 'test', '--locked', '--offline', '--release', '-p',
                  'bigbrotr-model' if producer_progress and name in {'fact_codec', 'evidence_intent'} else 'bigbrotr',
                  *selection, '--message-format=json', '--', '--test-threads=1',
                  *(['--skip', 'storage::rocks::store::admission_tests::producer_ack_tests::native_ack_fsync_boundary']
                    if name in ('producer_ack', 'admission_tests') else []),
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
if installed or archive_installed or producer_floor:
    clippy_targets += ['--test', 'control_runtime_entrypoint']
if archive_components:
    clippy_targets = ['--lib']
steps += [('clippy', ['cargo', 'clippy', '--locked', '--offline', '--release', '-p', 'bigbrotr',
                     *clippy_targets, '--', '-D', 'warnings'])]
if producer_progress:
    steps += [('model_clippy', ['cargo', 'clippy', '--locked', '--offline', '--release',
                               '-p', 'bigbrotr-model', '--test', 'fact_codec', '--test', 'evidence_intent',
                               '--', '-D', 'warnings'])]
result = execute(steps, checks, SOURCE, env, identity, 1800)
# Preserve built test executables even when a later check fails. Cargo outputs
# are acceleration only; these copies and raw logs are the retained artifacts.
if not archive_installed:
    artifacts.mkdir()
executables, counts, cli_paths, test_names = {}, {}, set(), {}
for name, selection in (tests + [('probe_build', None)] if build_probe else tests):
    log = checks / (name + '.log')
    if not log.exists():
        continue
    content = log.read_text()
    summaries = re.findall(r'test result: ok\. ([1-9][0-9]*) passed; 0 failed; 0 ignored;', content)
    if summaries:
        assert len(summaries) == 1
        counts[name] = int(summaries[0])
        if producer_progress or producer_floor:
            test_names[name] = re.findall(r'^test (\S+) \.\.\. ok$', content, re.MULTILINE)
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
required_controls = {}
if producer_progress:
    # Literal expected controls, independent of Cargo's selected/nonempty count.
    # Missing or renamed critical checks require explicit profile reconciliation.
    required_controls = {
        'producer_progress': [
            'storage::postgres::control_owner::producer_progress::tests::' + name for name in [
                'local_prefix_cannot_skip_or_reorder_a_retained_event',
                'replacement_req_starts_a_new_local_prefix',
                'retained_capacity_is_independent_of_window_credit',
                'current_head_encoding_matches_independent_fixed_width_golden',
                'cumulative_digest_binds_proof_position_context_and_retained_closure']],
        'producer_checkpoint': [
            'storage::rocks::store::admission_tests::producer_checkpoint_tests::' + name for name in [
                'equal_native_receipts_from_another_root_cannot_mint_a_checkpoint_basis',
                'certification_basis_is_read_only_and_expires_when_native_state_changes',
                'ordinary_certification_and_needs_admission_do_not_create_a_checkpoint_witness',
                'rejected_signature_has_no_success_dependencies_or_mutation_identity',
                'retained_progress_reader_checks_exact_key_full_hash_and_original_frontier',
                'retained_progress_reader_refuses_short_grant_cancel_and_changed_sequence']],
        'captured_input': [
            'acquisition::preparation::tests::a_mismatched_intent_cannot_cross_either_opaque_preparation_handoff'],
        'captured_binding': [
            'acquisition::session::preparation_tests::preparation_binds_original_raw_event_receive_sample_and_certified_intent'],
        'sync_progress': [
            'interfaces::serve::initial_ingest::sync::tests::' + name for name in [
                'a_preparation_hole_keeps_later_admission_eligible_without_reopening_progress',
                'resource_and_context_failures_cannot_be_hidden_as_peer_refusals']],
        'fact_codec': [
            'retained_header_reader_matches_literal_frozen_source_identities',
            'retained_header_is_not_a_payload_integrity_or_validity_certificate'],
        'evidence_intent': [
            'exact_dictionary_request_matches_independent_literal_bytes_and_digest',
            'every_count_byte_and_scratch_allocation_is_caller_bounded'],
    }
if producer_ack:
    required_controls['producer_progress'].append(
        'storage::postgres::control_owner::producer_progress::tests::progress_grant_reserves_native_ack_before_any_dispatch')
    required_controls['producer_progress'].extend(
        'storage::postgres::control_owner::producer_progress::tests::' + name for name in [
            'mutation_handoff_after_a_hole_preserves_classified_prefix',
            'mutation_handoff_without_a_classified_head_does_not_create_one',
            'mutation_only_capture_cannot_checkpoint_proof_or_rejection_reads',
            'next_req_classification_keeps_pre_hole_counts_and_exact_replay'])
    required_controls['producer_ack'] = [
        'storage::rocks::store::admission_tests::producer_ack_tests::' + name for name in [
            'acknowledgement_writes_only_watermark_once_and_retains_every_original',
            'bootstrap_ack_releases_its_actual_slot_without_removing_dictionary_receipt',
            'two_slot_credit_comes_only_from_actual_ack_and_proof_only_does_not_ack',
            'acknowledgement_refuses_foreign_stale_overissued_and_unreserved_targets',
            'acknowledgement_postwrite_authority_loss_preserves_ack_and_sequence_mismatch_poison']]
    required_controls['sync_progress'].append(
        'interfaces::serve::initial_ingest::archive::tests::active_request_window_does_not_define_archive_retention')
    required_controls['control_schema'] = [
        'initial_preparation_retirement_activation_and_recovery_are_distinct_additive_protocols',
        'producer_progress_and_ack_preserve_old_floors_but_never_ignore_changed_protocols']
    required_controls['initial_reconstruction'] = [
        'storage::rocks::store::admission_tests::initial_reconstruction_tests::' + name for name in [
            'initial_writer_tests::activated_recovery_tests::activated_source_tests::activated_ack_handoff_requires_all_predecessor_originals_before_target',
            'lineage_tests::lineage_replay_tests::successor_handoff_tests::successor_writer_tests::successor_ack_updates_only_exact_admitted_frontier_and_keeps_replay']]
if producer_floor:
    required_controls['archive_retained_frontier'] = [
        'storage::postgres::control_owner::archive_retained_frontier::tests::' + name for name in [
            'only_complete_terminal_history_is_a_loader_prerequisite',
            'compacted_history_is_not_an_archive_frontier_prerequisite']]
    required_controls['successor_history'] = [
        'storage::postgres::control_owner::successor_history::tests::' + name for name in [
            'terminal_genesis_kind_comes_from_activation_even_at_zero',
            'pending_and_prepared_closing_do_not_certify_the_target',
            'activated_nonterminal_and_noncontiguous_heads_refuse',
            'positive_summary_binds_kind_epoch_count_cut_and_digest',
            'compacted_initial_summary_is_separate_from_original_replay']]
    required_controls['compacted_progress'] = [
        'storage::postgres::control_owner::producer_progress::recovery::tests::compacted_floor_requires_exact_retained_checkpoint']
    required_controls['compacted_prefix'] = [
        'storage::rocks::store::admission_tests::compacted_prefix_tests::' + name for name in [
            'compacted_floor_codec_has_exact_bytes_and_legacy_refusal',
            'compacted_prefix_validates_all_outcome_kinds_and_residual_originals_without_writes',
            'compacted_prefix_requires_pending_indices_and_complete_scientific_census',
            'compacted_outcome_reducer_refuses_changed_rows_and_incomplete_closure',
            'compacted_prefix_binds_custody_inputs_cancellation_and_grants',
            'compacted_outcomes_check_protection_batch_digest_and_residual_names',
            'compacted_outcomes_preserve_full_width_historical_policy_and_reject_row_mismatch']]
    required_controls['request_inputs'] = [
        'storage::rocks::store::request_input_tests::lineage_input_tests::' + name for name in [
            'compacted_input_set_validates_floor_and_required_bounds_separately',
            'mixed_inputs_accept_full_partial_and_no_residuals_with_exact_global_stamps',
            'one_valid_residual_cannot_mask_one_missing_required_original',
            'compacted_wrappers_check_present_residuals_and_preserve_missing_directory_refusal',
            'mixed_inputs_refuse_wrong_slot_epoch_sequence_and_noncanonical_names',
            'mixed_residual_check_binds_content_and_the_complete_required_spool',
            'staged_addition_excludes_only_exact_new_required_file',
            'recovery_tail_is_stamped_separately_and_never_supplies_required_credit',
            'all_present_classes_share_the_physical_file_limit',
            'required_residual_and_tail_bytes_share_the_existing_logical_byte_limit',
            'residual_files_keep_private_link_and_complete_content_checks',
            'mixed_input_scan_observes_cancellation_before_and_during_enumeration']]
    required_controls['initial_reconstruction'] = [
        'storage::rocks::store::admission_tests::initial_reconstruction_tests::lineage_tests::lineage_replay_tests::compacted_ancestor_tests::' + name for name in [
            'compacted_ancestor_reads_full_later_census_and_all_residual_shapes_without_writes',
            'compacted_ancestor_requires_exact_initial_anchor_and_refuses_later_floors',
            'compacted_ancestor_census_covers_epoch_two_and_later_correction_ancestry',
            'compacted_ancestor_requires_pending_science_and_later_originals',
            'compacted_ancestor_binds_root_sequence_sidecars_and_native_uncertainty',
            'compacted_ancestor_requires_grants_cancellation_and_complete_ordered_rows']]
    required_controls['initial_reconstruction'].extend(
        'storage::rocks::store::admission_tests::initial_reconstruction_tests::lineage_tests::lineage_replay_tests::compacted_ancestor_tests::compacted_history_tests::' + name for name in [
            'compacted_history_replays_later_originals_and_binds_initial_recorded_digest',
            'compacted_history_pending_requires_its_exact_owner_and_final_authority',
            'compacted_history_refuses_stale_completion_and_original_only_construction',
            'compacted_history_cannot_mint_handoff_gap_empty_or_activation_capabilities',
            'compacted_history_reuses_one_permit_for_full_census_and_canonical_rows',
            'compacted_history_correction_census_counts_reclaimed_requests_beyond_spool_cap'])
    required_controls['initial_reconstruction'].extend(
        'storage::rocks::store::admission_tests::initial_reconstruction_tests::initial_gap_tests::compacted_handoff_tests::' + name for name in [
            'compacted_handoff_exact_four_writes_and_zero_write_prepared_retry',
            'compacted_handoff_refuses_stale_custody_inputs_and_dispatch_without_writes',
            'compacted_handoff_requires_full_census_original_manifest_cut_and_digest',
            'compacted_handoff_wrong_owner_and_prepared_pending_context_refuse',
            'compacted_handoff_prepared_target_must_be_empty_open_and_census_complete',
            'compacted_candidate_binds_fresh_target_and_explicit_activation_only',
            'compacted_handoff_postwrite_authority_refusal_retains_exact_retry',
            'initial_preparation_selector_accepts_original_and_compacted_without_writes',
            'initial_preparation_selector_refuses_incompatible_head_and_census',
            'initial_preparation_selector_requires_epoch_two_budget_and_cancellation'])
    required_controls['claim_custody'] = [
        'interfaces::serve::initial_ingest::recovery::successor_family::tests::' + name for name in [
            'interrupted_claim_closes_native_before_retained_future',
            'claim_guard_covers_unwind_and_disarms_after_delivery']]
    required_controls['control_runtime_entrypoint'] = [
        'explicit_ingest_recovery_preserves_missing_store_before_credentials',
        'explicit_recovery_modes_conflict_before_configuration_io']
    required_controls['initial_reconstruction'].append(
        'storage::rocks::store::admission_tests::initial_reconstruction_tests::lineage_tests::lineage_replay_tests::compacted_ancestor_tests::compacted_later_ancestor_cannot_mint_initial_handoff')
    required_controls['initial_reconstruction'].extend(
        'storage::rocks::store::admission_tests::initial_reconstruction_tests::initial_writer_tests::activated_recovery_tests::compacted_activated_tests::' + name for name in [
            'compacted_activated_recovery_separates_ancestor_and_target_and_ack_preserves_all_bytes',
            'compacted_activated_pending_requires_exact_ancestor_owner_and_never_partially_advances',
            'compacted_activated_census_refuses_incompatible_floor_missing_science_and_required_originals',
            'compacted_activated_stamps_refuse_stale_sequence_sidecars_cancellation_and_small_grants',
            'compacted_activated_unreserved_tail_is_preserved_but_reserved_tail_cannot_ack',
            'compacted_activated_ack_target_rechecks_optional_tail_after_token_creation',
            'compacted_activated_context_binds_actual_root_and_cannot_mint_prepared_handoff',
            'compacted_activated_optional_range_keeps_the_physical_file_ceiling',
            'compacted_activated_logical_requests_can_exceed_physical_spool_limit'])
    required_controls['initial_reconstruction'].extend(
        'storage::rocks::store::admission_tests::initial_reconstruction_tests::initial_writer_tests::activated_recovery_tests::compacted_activated_tests::terminal_tests::' + name for name in [
            'compacted_activated_source_preserves_genuine_digest_event_mask_and_counts',
            'compacted_activated_source_confirmation_and_order_refusals_never_advance',
            'compacted_activated_retirement_observes_complete_closing_without_native_writes',
            'compacted_activated_source_cut_and_retirement_root_are_exact',
            'compacted_activated_source_rechecks_sidecars_sequence_cancel_and_science',
            'compacted_activated_source_preserves_optional_tail_but_refuses_reserved_gaps',
            'compacted_activated_source_ack_handoff_retains_final_input_seal'])
    required_controls['initial_reconstruction'].extend(
        'storage::rocks::store::admission_tests::initial_reconstruction_tests::initial_gap_tests::compacted_handoff_tests::compacted_empty_tests::' + name for name in [
            'compacted_empty_exact_one_write_preserves_science_and_fresh_closing_retry',
            'compacted_empty_requires_exact_canonical_origin_and_empty_two_entry_shape',
            'compacted_empty_rejects_foreign_root_and_changed_census_without_writes',
            'compacted_empty_full_effect_census_and_zero_target_are_required',
            'compacted_empty_grant_and_delivery_failure_preserve_exact_close_retry',
            'compacted_empty_original_cancellation_cannot_be_replaced_by_a_fresh_flag'])
    required_controls['successor_transition'] = [
        'storage::postgres::control_owner::successor_transition::tests::' + name for name in [
            'successor_preparation_is_one_shot_even_after_a_confirmed_observation',
            'successor_preparation_checks_scheduling_overflow_and_independent_request_window',
            'successor_plan_binding_requires_every_actual_control_coordinate',
            'successor_native_transition_reserves_data_native_and_item_together',
            'compacted_successor_requires_matching_private_check_and_retained_basis',
            'compacted_successor_check_retains_the_full_canonical_effect_grant']]
    required_controls['initial_reconstruction'].extend(
        'storage::rocks::store::admission_tests::initial_reconstruction_tests::lineage_tests::lineage_replay_tests::compacted_ancestor_tests::compacted_history_tests::compacted_successor_tests::' + name for name in [
            'compacted_successor_gap_four_puts_then_empty_one_put_and_fresh_zero_retry',
            'compacted_successor_view_binds_root_sequence_cancellation_and_effect_grant',
            'compacted_successor_residual_inputs_never_replace_required_later_originals',
            'compacted_successor_candidate_preserves_original_and_activation_refusals',
            'compacted_successor_dispatch_failure_keeps_exact_completed_retry'])
missing_controls = {name: sorted(set(required) - set(test_names.get(name, [])))
                    for name, required in required_controls.items()
                    if not set(required) <= set(test_names.get(name, []))}
receipt = dict(identity, status='FAIL' if missing_controls else result['status'], test_counts=counts,
               executables_sha256=executables,
               independent_fixture_sha256=file_hash(SOURCE / 'experiments/ingest-succession/portable-handoff-v2.json'))
if producer_progress or producer_floor:
    receipt.update(test_names=test_names, required_controls=required_controls,
                   missing_controls=missing_controls)
(artifacts / 'component-result.json').write_text(json.dumps(receipt, indent=2) + '\n')
assert receipt['status'] == 'PASS'
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
                     else 'PASS_COMPACTED_PREFIX_COMPONENTS' if producer_floor
                     else 'PASS_PRODUCER_ACK_COMPONENTS' if producer_ack
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
