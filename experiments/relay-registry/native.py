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
assert len(sys.argv) in (1, 2) and mode in ('registry', 'faults', 'enrollment', 'enrollment-faults', 'visibility', 'policy-versions', 'relay-source', 'relay-source-faults', 'source-transport', 'source-cleanup', 'source-races', 'acquisition-scope', 'acquisition-scope-faults', 'acquisition-scope-read', 'acquisition-binding', 'acquisition-binding-refusals', 'acquisition-binding-recovery', 'sync-arrival-policy', 'ingest-lease', 'initial-ingest', 'acquisition-jobs', 'acquisition-jobs-faults', 'acquisition-claims')
probe = {'registry':'relay_registry_probe', 'faults':'relay_registry_fault_probe',
         'enrollment':'relay_enrollment_probe', 'enrollment-faults':'relay_enrollment_fault_probe',
         'visibility':'ingest_bootstrap_probe', 'policy-versions':'ingest_bootstrap_probe', 'relay-source':'ingest_bootstrap_probe', 'relay-source-faults':'ingest_bootstrap_probe', 'source-transport':'ingest_bootstrap_probe', 'source-cleanup':'ingest_bootstrap_probe', 'source-races':'ingest_bootstrap_probe', 'acquisition-scope':'acquisition_scope_probe', 'acquisition-scope-faults':'acquisition_scope_probe', 'acquisition-scope-read':'acquisition_scope_probe', 'acquisition-binding':'ingest_bootstrap_probe', 'acquisition-binding-refusals':'ingest_bootstrap_probe', 'acquisition-binding-recovery':'ingest_bootstrap_probe', 'sync-arrival-policy':'ingest_bootstrap_probe', 'ingest-lease':'ingest_bootstrap_probe', 'initial-ingest':'ingest_bootstrap_probe', 'acquisition-jobs':'acquisition_job_probe', 'acquisition-jobs-faults':'acquisition_job_probe', 'acquisition-claims':'acquisition_job_probe'}[mode]
if mode == 'relay-source-faults':
    identity['scope'] = 'Source-bound admission/recovery fault probe build only; no engine qualification'
if mode == 'source-races':
    identity['scope'] = 'Actual source/policy lock probe build only; no engine qualification'
if mode == 'source-cleanup':
    identity['scope'] = 'Actual source cleanup fault probe build only; no engine qualification'
if mode == 'enrollment-faults':
    identity['scope'] = 'Actual enrollment fault probe build only; engine authority qualification is separate'
checks = Path('/out/build-checks')
steps = [
    ('compile', ['cargo','build','--locked','--offline','--release','-p','bigbrotr',
                 '--bin','bigbrotr','--example',probe]),
    ('clippy', ['cargo','clippy','--locked','--offline','--release','-p','bigbrotr','--lib',
                '--bin','bigbrotr','--example',probe,'--','-D','warnings']),
]
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
if mode == 'ingest-lease':
    identity['scope'] = 'Same-Control initial Ingest scheduling renewal build; actual engine and installed lifetime qualification remain separate'
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
(artifacts/'build-result.json').write_text(json.dumps(receipt,indent=2)+'\n')
assert sources()==manifest['files_sha256']
status = {'registry':'PASS_REGISTRY_BUILD_ONLY', 'faults':'PASS_REGISTRY_FAULT_BUILD_ONLY',
          'enrollment':'PASS_RELAY_ENROLLMENT_BUILD_ONLY',
          'enrollment-faults':'PASS_RELAY_ENROLLMENT_FAULT_BUILD_ONLY',
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
