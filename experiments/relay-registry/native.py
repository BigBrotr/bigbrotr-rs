#!/usr/bin/env python3
"""Exact-source native Control/Ingest CLI/probe builds; bounded profile checks."""
import json
from pathlib import Path
import shutil
import subprocess
import sys

sys.dont_write_bytecode = True
SOURCE = Path('/out/build/source')
sys.path.insert(0, str(SOURCE))
from tools.build import clean_environment, file_hash, native_toolchain
from tools.ci import execute

build = SOURCE.parent
manifest = json.loads((build/'source-manifest.json').read_text())
def sources():
    return {str(p.relative_to(SOURCE)):file_hash(p) for p in sorted(SOURCE.rglob('*')) if p.is_file()}
assert sources() == manifest['files_sha256']
env = clean_environment()
selected, native = native_toolchain(env, 2)
env.update(selected)
env.update(CARGO_HOME=str(build/'cargo-home'), CARGO_TARGET_DIR=str(build/'target'),
           CARGO_NET_OFFLINE='true', BIGBROTR_SOURCE_REVISION=manifest['revision'],
           BIGBROTR_SOURCE_TREE=manifest['tree'], BIGBROTR_BUILD_PROFILE='release',
           BIGBROTR_SOURCE_MANIFEST_SHA256=file_hash(build/'source-manifest.json'))
identity = dict(source=manifest['revision'], tree=manifest['tree'], native_toolchain=native,
                lockfile_sha256=file_hash(SOURCE/'Cargo.lock'),
                source_manifest_sha256=file_hash(build/'source-manifest.json'),
                scope='Operational endpoint registration build; no acquisition or release qualification')
identity['rustc'] = subprocess.check_output(['rustc','--version','--verbose'],env=env,text=True).strip()
assert 'host: x86_64-unknown-linux-gnu' in identity['rustc'] and identity['rustc'].startswith('rustc 1.98.1 ')
mode = sys.argv[1] if len(sys.argv) == 2 else 'registry'
assert len(sys.argv) in (1, 2) and mode in ('ingest-installed-live', 'ingest-live-refusals', 'ingest-live-guards', 'ingest-live', 'ingest-empty-installed', 'ingest-empty-preparer', 'ingest-empty-locks', 'ingest-empty-faults', 'ingest-gap-faults', 'ingest-history', 'ingest-recovery', 'dictionary-append', 'relay-arrival', 'relay-transfer', 'observation', 'registry', 'faults', 'enrollment', 'enrollment-faults', 'registration-faults', 'visibility', 'policy-versions', 'relay-source', 'relay-source-faults', 'source-transport', 'source-cleanup', 'source-races', 'acquisition-scope', 'acquisition-scope-faults', 'acquisition-scope-read', 'acquisition-binding', 'acquisition-binding-refusals', 'acquisition-binding-recovery', 'sync-arrival-policy', 'ingest-lease', 'initial-ingest', 'acquisition-jobs', 'acquisition-jobs-faults', 'acquisition-claims')
probe = {'ingest-installed-live':'ingest_bootstrap_probe', 'ingest-live-refusals':'ingest_bootstrap_probe', 'ingest-live-guards':'ingest_bootstrap_probe', 'ingest-live':'ingest_bootstrap_probe', 'ingest-empty-installed':'ingest_bootstrap_probe', 'ingest-empty-preparer':'ingest_bootstrap_probe', 'ingest-empty-locks':'ingest_bootstrap_probe', 'ingest-empty-faults':'ingest_bootstrap_probe', 'ingest-gap-faults':'ingest_bootstrap_probe', 'ingest-history':'ingest_bootstrap_probe', 'ingest-recovery':'ingest_bootstrap_probe', 'dictionary-append':'ingest_bootstrap_probe', 'relay-arrival':'ingest_bootstrap_probe', 'relay-transfer':'ingest_bootstrap_probe', 'observation':'ingest_bootstrap_probe', 'registry':'relay_registry_probe', 'faults':'relay_registry_fault_probe',
         'enrollment':'relay_enrollment_probe', 'enrollment-faults':'relay_enrollment_fault_probe', 'registration-faults':'relay_registration_fault_probe',
         'visibility':'ingest_bootstrap_probe', 'policy-versions':'ingest_bootstrap_probe', 'relay-source':'ingest_bootstrap_probe', 'relay-source-faults':'ingest_bootstrap_probe', 'source-transport':'ingest_bootstrap_probe', 'source-cleanup':'ingest_bootstrap_probe', 'source-races':'ingest_bootstrap_probe', 'acquisition-scope':'acquisition_scope_probe', 'acquisition-scope-faults':'acquisition_scope_probe', 'acquisition-scope-read':'acquisition_scope_probe', 'acquisition-binding':'ingest_bootstrap_probe', 'acquisition-binding-refusals':'ingest_bootstrap_probe', 'acquisition-binding-recovery':'ingest_bootstrap_probe', 'sync-arrival-policy':'ingest_bootstrap_probe', 'ingest-lease':'ingest_bootstrap_probe', 'initial-ingest':'ingest_bootstrap_probe', 'acquisition-jobs':'acquisition_job_probe', 'acquisition-jobs-faults':'acquisition_job_probe', 'acquisition-claims':'acquisition_job_probe'}[mode]
if mode == 'relay-source-faults':
    identity['scope'] = 'Source-bound admission/recovery fault probe build only; no engine qualification'
if mode == 'source-races':
    identity['scope'] = 'Actual source/policy lock probe build only; no engine qualification'
if mode == 'source-cleanup':
    identity['scope'] = 'Actual source cleanup fault probe build only; no engine qualification'
if mode == 'registration-faults':
    identity['scope'] = 'Authenticated registration fault probe build only; engine qualification is separate'
if mode == 'enrollment-faults':
    identity['scope'] = 'Actual enrollment fault probe build only; engine authority qualification is separate'
checks = Path('/out/build-checks')
steps = [
    ('compile', ['cargo','build','--locked','--offline','--release','-p','bigbrotr',
                 '--bin','bigbrotr','--example',probe]),
    ('clippy', ['cargo','clippy','--locked','--offline','--release','-p','bigbrotr','--lib',
                '--bin','bigbrotr','--example',probe,'--','-D','warnings']),
]
if mode == 'ingest-empty-installed':
    identity['scope'] = 'Installed empty-attempt retirement CLI/probe compile, actual entrypoint refusals and Clippy; unchanged storage/native tests retained at30a88ae; connected CLI qualification remains separate'
    steps.insert(1, ('entrypoints', ['cargo','test','--locked','--offline','--release','-p','bigbrotr',
                                   '--test','control_runtime_entrypoint','--message-format=json']))
    steps[-1][1][-3:-3] = ['--test', 'control_runtime_entrypoint']
if mode == 'ingest-installed-live':
    identity['scope'] = 'Installed live initial successor CLI and shared initial/recovery owner compile, explicit Serve grammar, actual entrypoint refusals and shared supervisor stop/join tests, Clippy; connected lifecycle/Sync qualification remains separate'
    steps[1:1] = [
        ('entrypoints', ['cargo','test','--locked','--offline','--release','-p','bigbrotr',
                         '--test','control_runtime_entrypoint','--message-format=json']),
        ('serve', ['cargo','test','--locked','--offline','--release','-p','bigbrotr',
                   '--bin','bigbrotr','interfaces::serve::tests::','--message-format=json']),
        ('supervision', ['cargo','test','--locked','--offline','--release','-p','bigbrotr',
                         '--bin','bigbrotr','interfaces::serve::initial_ingest::supervision::tests::',
                         '--message-format=json']),
    ]
    steps[-1][1][-3:-3] = ['--test', 'control_runtime_entrypoint']
if mode == 'ingest-live-refusals':
    identity['scope'] = 'Activated first-manifest and finite scheduling renewal refusal/expiry/COMMIT-loss probe compile and Clippy only; unchanged native135-test evidence retained atc640b2f; connected qualification remains separate'
if mode == 'ingest-live-guards':
    identity['scope'] = 'Actual preparing-Control activation SQL guard and live scheduling renewal probe compile and Clippy only; unchanged native135-test evidence retained atc640b2f; connected qualification remains separate'
if mode == 'ingest-empty-preparer':
    identity['scope'] = 'Finite genuine historical preparer claim probe build and Clippy only; unchanged native tests retained at30a88ae; actual two-process qualification remains separate'
if mode == 'ingest-empty-locks':
    identity['scope'] = 'Finite empty-retirement claim/expiry lock probe build and Clippy only; unchanged native tests retained at30a88ae; actual engine schedules remain separate'
if mode == 'ingest-empty-faults':
    identity['scope'] = 'Finite empty-retirement COMMIT-loss probe build and Clippy only; unchanged native tests retained at30a88ae; actual engine schedules remain separate'
if mode == 'ingest-gap-faults':
    identity['scope'] = 'Finite initial-gap policy/delivery-loss probe build and Clippy only; unchanged native tests retained at162caeb; actual engine schedules remain separate'
if mode in ('ingest-history', 'ingest-live'):
    identity['scope'] = 'Prepared-successor history, positive initial-gap completion and exact empty-attempt native retirement, affected replay, closed codecs and CLI refusals; connected PostgreSQL, separate-process qualification and activation remain separate'
    steps.insert(1, ('entrypoints', ['cargo','test','--locked','--offline','--release','-p','bigbrotr',
                                   '--test','control_runtime_entrypoint','--test','control_schema']))
    steps.insert(2, ('source-closure', ['cargo','test','--locked','--offline','--release','-p','bigbrotr',
                                      '--lib','source_closure_tests','--message-format=json','--','--test-threads=1']))
    steps[3:3] = [(name, ['cargo','test','--locked','--offline','--release','-p','bigbrotr',
                         '--lib',selection,'--message-format=json','--','--test-threads=1'])
                  for name, selection in [('producer-closing','producer_closing_tests'),
                                          ('producer-recovery','producer_recovery_tests'),
                                          ('initial-handoff','initial_handoff_tests'),
                                          ('initial-reconstruction','initial_reconstruction_tests'),
                                          ('bootstrap','bootstrap_tests'),
                                          ('dictionary-append','dictionary_append_tests')]]
    # Replay shares event internals. Exercise these original admission callers;
    # the packaged Archive worker needs its separately qualified mounted bundle.
    event_replays = [
        'actual_signed_event_commits_payload_scope_and_receive_with_dictionary_and_receipt',
        'all_fifteen_independently_signed_fixture_operations_match_native_inventory',
        'retained_replay_with_reduced_dependency_limit_is_not_corrupt_state',
        'inconsistent_counter_or_scope_inventory_cannot_recreate_scientific_state',
        'invalid_signature_has_only_strict_operational_detail_and_replays_after_reopen',
        'missing_receipt_or_retained_fact_is_never_a_new_request_or_successful_replay',
        'last_authority_check_and_bounded_outstanding_window_leave_no_partial_effects',
    ]
    steps[-1:-1] = [('event-replay', ['cargo','test','--locked','--offline','--release',
                                     '-p','bigbrotr','--lib','--message-format=json','--',
                                     '--test-threads=1', *event_replays])]
    steps[-1:-1] = [(name, ['cargo','test','--locked','--offline','--release','-p','bigbrotr',
                           '--test',name,'--message-format=json','--','--test-threads=1'])
                    for name in ['rocks_manifest', 'rocks_keys']]
if mode == 'ingest-live':
    identity['scope'] = 'Live initial native candidate and PostgreSQL activation consumer compile, affected native admission/replay/certification, codecs and schema/entrypoint tests; connected activation qualification remains separate'
    steps[-1:-1] = [('certification', ['cargo','test','--locked','--offline','--release',
                                      '-p','bigbrotr','--lib','certification_tests',
                                      '--message-format=json','--','--test-threads=1'])]
if mode == 'ingest-recovery':
    identity['scope'] = 'Exclusive historical native inspection; registry loader, installed recovery and successor remain separate'
    steps.insert(0, ('rocks-store', ['cargo','test','--locked','--offline','--release','-p','bigbrotr',
                                    '--test','rocks_store','--message-format=json','--','--test-threads=1']))
if mode == 'dictionary-append':
    identity['scope'] = 'Native dictionary append/replay components; connected and installed journeys remain separate'
    # These owners changed. Packaged Archive worker integration requires its
    # separate mounted bundle and is not part of this native component profile.
    steps[0:0] = [(name, ['cargo','test','--locked','--offline','--release','-p','bigbrotr',
                         '--lib',selection,'--message-format=json','--','--test-threads=1'])
                  for name, selection in [('dictionary-append', 'dictionary_append_tests'),
                                          ('bootstrap', 'bootstrap_tests'),
                                          ('recovery', 'producer_recovery_tests')]]
if mode == 'relay-arrival':
    identity['scope'] = 'Socket arrival fixture build only; unchanged retained/decode components already tested at cbc5f25'
if mode == 'acquisition-jobs-faults':
    identity['scope'] = 'Finite acquisition fault probe build only; actual engine schedules remain separate'
if mode in ('acquisition-jobs', 'acquisition-claims'):
    identity['scope'] = 'Typed finite acquisition acceptance build; engine, dispatch and installed qualification remain separate'
    steps.insert(1, ('commands', ['cargo','test','--locked','--offline','--release','-p','bigbrotr',
                                '--test','acquisition_jobs','--test','control_schema','--test','migrate_entrypoint']))
if mode == 'acquisition-claims':
    identity['scope'] = 'Exact finite queued claim build only; engine, custody recovery and dispatch remain separate'
if mode == 'initial-ingest':
    identity['scope'] = 'Explicit installed initial Ingest lifetime build; actual engines and interruption qualification remain separate'
    steps.insert(1, ('entrypoints', ['cargo','test','--locked','--offline','--release','-p','bigbrotr',
                                   '--test','control_runtime_entrypoint']))
    steps.insert(2, ('explicit', ['cargo','test','--locked','--offline','--release','-p','bigbrotr',
                                '--bin','bigbrotr','initial_ingest']))
    steps.insert(3, ('sessions', ['cargo','test','--locked','--offline','--release','-p','bigbrotr',
                                '--bin','bigbrotr','interfaces::management::']))
    steps.insert(4, ('registration', ['cargo','test','--locked','--offline','--release','-p','bigbrotr',
                                    '--test','relay_registration_command','--test','acquisition_scope_command','--test','management_jobs','--test','acquisition_jobs','--test','control_schema',
                                    '--test','migrate_entrypoint']))
if mode == 'ingest-lease':
    identity['scope'] = 'Same-Control initial Ingest scheduling renewal build; actual engine and installed lifetime qualification remain separate'
if mode == 'observation':
    identity['scope'] = 'Committed native observation/receive clock and actual session components; connected engines remain separate'
    steps[1:1] = [
        ('observations', ['cargo','test','--locked','--offline','--release','-p','bigbrotr',
                          '--lib','observation_tests','--message-format=json','--','--test-threads=1']),
        ('clock', ['cargo','test','--locked','--offline','--release','-p','bigbrotr',
                   '--lib','receive_clock','--message-format=json','--','--test-threads=1']),
        ('session', ['cargo','test','--locked','--offline','--release','-p','bigbrotr',
                     '--test','acquisition_session','--message-format=json','--','--test-threads=1']),
    ]
if mode == 'relay-transfer':
    identity['scope'] = 'Retained EVENT ownership and strict bounded decode; actual PG/Rocks/socket admission remains separate'
    steps[1:1] = [
        ('decode', ['cargo','test','--locked','--offline','--release','-p','bigbrotr-protocol',
                    '--test','event','--message-format=json']),
        ('sealed', ['cargo','test','--locked','--offline','--release','-p','bigbrotr',
                    '--doc','acquisition::session::ReceivedRelay']),
        ('session', ['cargo','test','--locked','--offline','--release','-p','bigbrotr',
                     '--test','acquisition_session','--message-format=json','--','--test-threads=1']),
    ]
    clippy = steps[-1][1]
    clippy[clippy.index('--'):clippy.index('--')] = ['-p','bigbrotr-protocol','--test','event','--test','acquisition_session']
if mode == 'registry':
    steps.insert(1, ('catalog', ['cargo','test','--locked','--offline','--release','-p','bigbrotr',
                 '--test','relay_registry','--test','control_schema','--test','migrate_entrypoint']))
if mode == 'enrollment':
    identity['scope'] = 'Initial no-auth relay enrollment and static routes; no installed acquisition or release qualification'
    steps.insert(1, ('enrollment', ['cargo','test','--locked','--offline','--release','-p','bigbrotr',
                 '--test','relay_enrollment_command','--test','network_route_selection',
                 '--test','network_policy','--test','control_schema','--test','migrate_entrypoint',
                 '--test','config_private_routes','--test','config_entrypoint','--test','policy_seed']))
if mode == 'visibility':
    identity['scope'] = 'Committed native visibility lookup; no acquisition permission or release qualification'
    steps.insert(1, ('visibility', ['cargo','test','--locked','--offline','--release','-p','bigbrotr',
                                  '--lib','visibility_tests','--','--test-threads=1']))
if mode == 'policy-versions':
    identity['scope'] = 'Policy v1/v2 and exact bans; no populated upgrade or acquisition permission'
    steps.insert(1, ('codec', ['cargo','test','--locked','--offline','--release','-p','bigbrotr',
                             '--bin','bigbrotr','config::policy']))
    steps.insert(2, ('bans', ['cargo','test','--locked','--offline','--release','-p','bigbrotr',
                            '--lib','network::bans']))
    steps.insert(3, ('headers', ['cargo','test','--locked','--offline','--release','-p','bigbrotr',
                               '--lib','runtime::policy_document']))
    steps.insert(4, ('entrypoints', ['cargo','test','--locked','--offline','--release','-p','bigbrotr',
                                   '--test','config_bans','--test','policy_seed','--test','config_entrypoint',
                                   '--test','network_policy']))
if mode == 'relay-source':
    identity['scope'] = 'Operational relay source bindings; no source grant or socket acquisition'
    steps.insert(1, ('catalog', ['cargo','test','--locked','--offline','--release','-p','bigbrotr',
                               '--test','control_schema','--test','migrate_entrypoint']))
if mode == 'source-transport':
    identity['scope'] = 'Finite current source transport and policy components; native engine scenarios remain separate'
    steps[1:1] = [
        ('network', ['cargo', 'test', '--locked', '--offline', '--release', '-p', 'bigbrotr',
                     '--test', 'network_direct', '--test', 'network_policy', '--test', 'network_route_selection']),
        ('visibility', ['cargo', 'test', '--locked', '--offline', '--release', '-p', 'bigbrotr',
                        '--lib', 'source_visibility_never']),
        ('policy', ['cargo', 'test', '--locked', '--offline', '--release', '-p', 'bigbrotr',
                    '--bin', 'bigbrotr', 'config::acquisition::tests']),
    ]
if mode == 'acquisition-scope-faults':
    identity['scope'] = 'Acquisition registration fault probe build only; no engine qualification'
if mode == 'acquisition-scope-read':
    identity['scope'] = 'Bounded immutable scope reader build; no engine or admission qualification'
    steps.insert(1, ('selector', ['cargo', 'test', '--locked', '--offline', '--release',
                                 '-p', 'bigbrotr-protocol', '--test', 'selector']))
if mode == 'sync-arrival-policy':
    identity['scope'] = 'Accepted Sync arrival policy and native consumer build; engine and installed qualification remain separate'
    steps.insert(1, ('policy', ['cargo', 'test', '--locked', '--offline', '--release', '-p', 'bigbrotr',
                              '--bin', 'bigbrotr', 'config::acquisition']))
if mode == 'acquisition-binding-recovery':
    identity['scope'] = 'Scoped concurrent policy and historical recovery probe build; actual engines remain separate'
if mode == 'acquisition-binding-refusals':
    identity['scope'] = 'Scoped signature and post-reservation refusal probe build; actual engines remain separate'
if mode == 'acquisition-binding':
    identity['scope'] = 'Scope-bound reservation/admission/certification build; engine qualification is separate'
    steps[1:1] = [
        ('membership', ['cargo', 'test', '--locked', '--offline', '--release', '-p', 'bigbrotr',
                        '--lib', 'acquisition::scope::tests']),
        ('catalog', ['cargo', 'test', '--locked', '--offline', '--release', '-p', 'bigbrotr',
                     '--test', 'control_schema', '--test', 'migrate_entrypoint']),
    ]
if mode == 'acquisition-scope':
    identity['scope'] = 'Immutable explicit scope registration build only; no engine or installed Sync qualification'
    steps.insert(1, ('catalog', ['cargo', 'test', '--locked', '--offline', '--release', '-p', 'bigbrotr',
                               '--test', 'control_schema', '--test', 'migrate_entrypoint']))
    steps.insert(2, ('scope-values', ['cargo', 'test', '--locked', '--offline', '--release',
                                    '-p', 'bigbrotr-model', '-p', 'bigbrotr-protocol',
                                    '--test', 'acquisition_scope', '--test', 'selector']))
result = execute(steps,checks,SOURCE,env,identity,1800)
assert result['status']=='PASS'
artifacts=Path('/out/artifacts'); artifacts.mkdir()
for name,relative in [('bigbrotr','bigbrotr'),(probe,'examples/'+probe)]:
    shutil.copy2(build/'target/release'/relative,artifacts/name)
info=json.loads(subprocess.check_output([str(artifacts/'bigbrotr'),'build-info'],env=env))
assert info['source']==dict(state='builder-supplied',revision=manifest['revision'],tree=manifest['tree'],
                           dirty=False,manifest_sha256=identity['source_manifest_sha256'])
receipt=dict(identity,format='bigbrotr-registry-build/1',binary_sha256=file_hash(artifacts/'bigbrotr'),
             probe_name=probe,probe_sha256=file_hash(artifacts/probe),build_info=info)
if mode in ('observation', 'relay-transfer'):
    executables = {}
    test_counts = [('observations', 6), ('clock', 3), ('session', 7)] if mode == 'observation' else [('decode', 10), ('session', 7)]
    if mode == 'relay-transfer':
        assert '1 passed; 0 failed; 0 ignored;' in (checks / 'sealed.log').read_text()
    for name, count in test_counts:
        content = (checks / (name + '.log')).read_text()
        assert f'test result: ok. {count} passed; 0 failed; 0 ignored;' in content
        for line in content.splitlines():
            if not line.startswith('{'): continue
            message = json.loads(line)
            if message.get('reason') != 'compiler-artifact' or not message.get('executable'): continue
            if not message['profile']['test']: continue
            target = message['target']
            if target['kind'] not in (['lib'], ['test']): continue
            source = Path(message['executable'])
            assert source.is_relative_to(build / 'target') and not source.is_symlink()
            filename = 'observation_components' if target['kind'] == ['lib'] else ('protocol_event' if target['name'] == 'event' else target['name'])
            if filename not in executables: shutil.copy2(source, artifacts / filename)
            digest = file_hash(artifacts / filename)
            assert digest == file_hash(source)
            executables[filename] = digest
    assert set(executables) == ({'observation_components', 'acquisition_session'} if mode == 'observation' else {'protocol_event', 'acquisition_session'})
    receipt['test_executables_sha256'] = executables
if mode in ('dictionary-append', 'ingest-recovery', 'ingest-history', 'ingest-live'):
    import re
    counts, executables = {}, set()
    selections = (['rocks-store'] if mode == 'ingest-recovery' else ['source-closure','producer-closing','producer-recovery','initial-handoff',
                                                                               'initial-reconstruction','bootstrap','dictionary-append','event-replay'] if mode in ('ingest-history', 'ingest-live')
                  else ['dictionary-append', 'bootstrap', 'recovery'])
    if mode == 'ingest-live':
        selections = [*selections, 'certification']
    target_kind = ['test'] if mode == 'ingest-recovery' else ['lib']
    retained_name = ('rocks_store' if mode == 'ingest-recovery' else 'source_closure_components' if mode in ('ingest-history', 'ingest-live')
                     else 'dictionary_components')
    for name in selections:
        content = (checks / (name + '.log')).read_text()
        summary = re.search(r'test result: ok\. ([1-9][0-9]*) passed; 0 failed; 0 ignored;', content)
        assert summary, 'missing executed native test result: ' + name
        counts[name] = int(summary.group(1))
        if mode in ('ingest-history', 'ingest-live') and name == 'event-replay':
            assert counts[name] == 7, 'incomplete focused native selection: ' + name
        if mode == 'ingest-live' and name == 'initial-reconstruction':
            for expected in [
                'local_writer_first_dictionary_request_is_an_append_and_not_bootstrap',
                'rejected_or_already_durable_request_one_keeps_zero_source_then_append_two_starts_one',
                'observed_successor_cannot_recreate_live_writer_custody',
                'candidate_requires_its_minted_target_local_root_current_proof_and_no_sidecars',
                'active_zero_source_is_explicit_and_rejects_stray_facts_pending_or_allocation_zero',
                'dictionary_grants_and_authority_refuse_before_write_without_losing_exact_retry',
            ]:
                assert '::initial_writer_tests::' + expected + ' ... ok' in content, expected
        for line in content.splitlines():
            if not line.startswith('{'): continue
            message = json.loads(line)
            if message.get('reason') == 'compiler-artifact' and message.get('executable') \
                    and message['profile']['test'] and message['target']['kind'] == target_kind:
                executables.add(message['executable'])
    assert len(executables) == 1
    source = Path(executables.pop())
    assert source.is_relative_to(build / 'target') and not source.is_symlink()
    shutil.copy2(source, artifacts / retained_name)
    digest = file_hash(artifacts / retained_name)
    assert digest == file_hash(source)
    receipt['test_executables_sha256'] = {retained_name: digest}
    receipt['native_tests_passed'] = counts
    if mode in ('ingest-history', 'ingest-live'):
        for name in ['rocks_manifest', 'rocks_keys']:
            content = (checks / (name + '.log')).read_text()
            summary = re.search(r'test result: ok\. ([1-9][0-9]*) passed; 0 failed; 0 ignored;', content)
            assert summary, 'missing executed codec result: ' + name
            counts[name] = int(summary.group(1))
            paths = set()
            for line in content.splitlines():
                if not line.startswith('{'): continue
                message = json.loads(line)
                if message.get('reason') == 'compiler-artifact' and message.get('executable') \
                        and message['profile']['test'] and message['target']['kind'] == ['test'] \
                        and message['target']['name'] == name:
                    paths.add(message['executable'])
            assert len(paths) == 1
            source = Path(paths.pop())
            assert source.is_relative_to(build / 'target') and not source.is_symlink()
            shutil.copy2(source, artifacts / name)
            digest = file_hash(artifacts / name)
            assert digest == file_hash(source)
            receipt['test_executables_sha256'][name] = digest
if mode in ('ingest-empty-installed', 'ingest-installed-live'):
    import re
    receipt['test_executables_sha256'] = {}
    receipt['entrypoint_tests_passed'] = 7
    expected = [('entrypoints', ['test'], 'control_runtime_entrypoint', 7)]
    if mode == 'ingest-installed-live':
        expected.extend([('serve', ['bin'], 'bigbrotr', 3),
                         ('supervision', ['bin'], 'bigbrotr', 5)])
    for step, kind, name, count in expected:
        content = (checks / (step + '.log')).read_text()
        summary = re.search(r'test result: ok\. ([1-9][0-9]*) passed; 0 failed; 0 ignored;', content)
        assert summary and int(summary.group(1)) == count, 'incomplete actual tests: ' + step
        paths = set()
        for line in content.splitlines():
            if not line.startswith('{'): continue
            message = json.loads(line)
            if message.get('reason') == 'compiler-artifact' and message.get('executable') \
                    and message['profile']['test'] and message['target']['kind'] == kind \
                    and message['target']['name'] == name:
                paths.add(message['executable'])
        assert len(paths) == 1
        executable = Path(paths.pop())
        assert executable.is_relative_to(build / 'target') and not executable.is_symlink()
        retained = name if kind == ['test'] else 'serve_components'
        shutil.copy2(executable, artifacts / retained)
        digest = file_hash(artifacts / retained)
        assert digest == file_hash(executable)
        receipt['test_executables_sha256'][retained] = digest
    if mode == 'ingest-installed-live':
        receipt['serve_tests_passed'] = 3
        receipt['supervision_tests_passed'] = 5
(artifacts/'build-result.json').write_text(json.dumps(receipt,indent=2)+'\n')
assert sources()==manifest['files_sha256']
status = {'ingest-installed-live':'PASS_INSTALLED_LIVE_SUCCESSOR_BUILD_ONLY', 'ingest-live-refusals':'PASS_LIVE_INITIAL_REFUSALS_PROBE_BUILD_ONLY', 'ingest-live-guards':'PASS_LIVE_INITIAL_GUARDS_PROBE_BUILD_ONLY', 'ingest-live':'PASS_LIVE_INITIAL_BUILD_ONLY', 'ingest-empty-installed':'PASS_INSTALLED_EMPTY_RETIREMENT_BUILD_ONLY', 'ingest-empty-preparer':'PASS_EMPTY_PREPARER_CLAIM_PROBE_BUILD_ONLY', 'ingest-empty-locks':'PASS_EMPTY_RETIREMENT_LOCK_PROBE_BUILD_ONLY', 'ingest-empty-faults':'PASS_EMPTY_RETIREMENT_FAULT_PROBE_BUILD_ONLY', 'ingest-gap-faults':'PASS_INITIAL_GAP_FAULT_PROBE_BUILD_ONLY', 'ingest-history':'PASS_INGEST_HISTORY_BUILD_ONLY', 'ingest-recovery':'PASS_INGEST_RECOVERY_INSPECTION', 'dictionary-append':'PASS_DICTIONARY_APPEND_COMPONENTS', 'relay-arrival':'PASS_RELAY_ARRIVAL_BUILD_ONLY', 'relay-transfer':'PASS_RELAY_TRANSFER_BUILD_ONLY', 'observation':'PASS_OBSERVATION_BUILD_ONLY', 'registry':'PASS_REGISTRY_BUILD_ONLY', 'faults':'PASS_REGISTRY_FAULT_BUILD_ONLY',
          'enrollment':'PASS_RELAY_ENROLLMENT_BUILD_ONLY',
          'enrollment-faults':'PASS_RELAY_ENROLLMENT_FAULT_BUILD_ONLY',
          'registration-faults':'PASS_RELAY_REGISTRATION_FAULT_BUILD_ONLY',
          'visibility':'PASS_INGEST_VISIBILITY_BUILD_ONLY', 'policy-versions':'PASS_POLICY_VERSIONS_BUILD_ONLY', 'relay-source':'PASS_RELAY_SOURCE_BUILD_ONLY',
          'relay-source-faults':'PASS_RELAY_SOURCE_FAULT_BUILD_ONLY',
          'source-transport':'PASS_SOURCE_TRANSPORT_BUILD_ONLY',
          'source-cleanup':'PASS_SOURCE_CLEANUP_BUILD_ONLY',
          'source-races':'PASS_SOURCE_RACES_BUILD_ONLY',
          'acquisition-scope':'PASS_ACQUISITION_SCOPE_BUILD_ONLY',
          'acquisition-scope-faults':'PASS_ACQUISITION_SCOPE_FAULT_BUILD_ONLY',
          'acquisition-scope-read':'PASS_ACQUISITION_SCOPE_READ_BUILD_ONLY',
          'acquisition-binding':'PASS_ACQUISITION_BINDING_BUILD_ONLY',
          'acquisition-binding-refusals':'PASS_ACQUISITION_BINDING_REFUSAL_BUILD_ONLY',
          'acquisition-binding-recovery':'PASS_ACQUISITION_BINDING_RECOVERY_BUILD_ONLY',
          'sync-arrival-policy':'PASS_SYNC_ARRIVAL_POLICY_BUILD_ONLY',
          'ingest-lease':'PASS_INITIAL_INGEST_LEASE_BUILD_ONLY',
          'initial-ingest':'PASS_INITIAL_INGEST_BUILD_ONLY',
          'acquisition-jobs':'PASS_ACQUISITION_JOBS_BUILD_ONLY',
          'acquisition-jobs-faults':'PASS_ACQUISITION_JOBS_FAULT_BUILD_ONLY',
          'acquisition-claims':'PASS_ACQUISITION_CLAIMS_BUILD_ONLY'}[mode]
Path('/out/qualification.json').write_text(json.dumps(dict(identity,status=status),indent=2)+'\n')
