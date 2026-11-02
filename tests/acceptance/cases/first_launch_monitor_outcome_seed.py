"""Closed test-only existing-outcome seed inputs; no production recovery API."""
import copy
import hashlib
import json
import os
from pathlib import Path

from cases.first_launch import digest, load, require
from cases import first_launch_passive_oracle as passive

TEST = 'storage::postgres::control_owner::monitor_outcome_seed::seed_exact_existing_outcome'
HOOK = '\n#[cfg(test)]\n#[path = "monitor_outcome_seed.rs"]\nmod monitor_outcome_seed;\n'
OWNER = 'apps/bigbrotr/src/storage/postgres/control_owner.rs'
FIXTURE = 'apps/bigbrotr/src/storage/postgres/monitor_outcome_seed.rs'
# config show deliberately hides all non-null endpoint and ban collections,
# including empty lists. This closed profile has exactly these sensitive keys.
REDACTED_POLICY_KEYS = ('monitor.active_write.endpoints','monitor.endpoints.urls',
    'relay_policy.banned','sync.endpoints.urls')


def encoded(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()


def diagnostic(value):
    if type(value) is int:
        require(value>=0, 'closed policy fixture has a signed diagnostic integer')
        return str(value)
    if type(value) is list:return [diagnostic(item) for item in value]
    if type(value) is dict:return {name:diagnostic(item) for name,item in value.items()}
    return value


def policy(source, report, config_hash):
    """Freeze literal policy plus explicit normalized recipe/Monitor overrides.

    Product config-show supplies an independent equality/digest check, never the
    expected settings. Diagnostic strings are produced only from our typed seed.
    No PostgreSQL input participates in this construction.
    """
    document=copy.deepcopy(load(source/'tests/fixtures/configuration/policy-v3.json'))
    document['settings'].update({
        'ingest.producer_outstanding_requests':32,'discoverer.events.enabled':False,
        'validator.enabled':False,'bootstrap.seed':False,
        'relay_policy.auto_enroll_validated_public':False,'relay_policy.candidate_validation':'disabled',
        'read.execution.max_memory_bytes':1024**3,'sync.enabled':False,'monitor.enabled':True,
        'monitor.checks':['open','read'],'monitor.store_checks':['open','read'],
        'monitor.check_timeout':15_000_000,'monitor.retries':1,
        'monitor.endpoints.mode':'fixed','monitor.endpoints.urls':['ws://127.0.0.1:7447/installed-fixture']})
    require(report['format']=='bigbrotr-config-static/1' and report['status']=='valid-static', 'seed config was not validated')
    require(report['document_sha256']==config_hash, 'seed deployment bytes differ')
    actual={name:row['value'] for name,row in report['settings'].items() if row['deployment_locked'] is False}
    shown=diagnostic(document['settings'])
    for name in REDACTED_POLICY_KEYS:shown[name]='[redacted]'
    require(encoded(actual)==encoded(shown), 'closed seed policy differs from complete normalized candidate')
    body=encoded(document);sha=hashlib.sha256(b'bigbrotr/product-policy/3\0'+body).hexdigest()
    require(report['initial_policy']['format']==document['format']
        and report['initial_policy']['sha256']==sha
        and report['initial_policy']['encoded_bytes']==str(len(body)), 'closed seed policy encoding differs')
    pair=dict(revision=1,document=document,sha256=sha)
    return dict(config=copy.deepcopy(pair),policy=copy.deepcopy(pair),deployment_sha256=config_hash,
        roots={name:row['value'] for name,row in report['settings'].items() if row['deployment_locked'] is True})


def registration(state, initialized, ids):
    """Freeze minted registration coordinates before selecting the measurement."""
    require(initialized['dataset_id']==ids['dataset'] and initialized['topology_generation']=='0', 'seed deployment coordinate differs')
    owner=passive.only(state['owner_epochs'],role='ingest')
    require(owner['dataset_id']=='\\x'+ids['dataset'] and owner['resource_id']=='\\x'+initialized['owner_resource_id']
        and owner['epoch']==1, 'seed original owner differs')
    producer=passive.only(state['producer_epochs'])
    require(producer['dataset_id']==owner['dataset_id'] and producer['producer_slot_id']=='\\x'+initialized['producer_slot_id'] and producer['producer_epoch']==1
        and producer['owner_incarnation']==owner['process_incarnation'], 'seed original producer differs')
    domains=sorted(state['allocation_domains'],key=lambda row:row['dictionary_kind'])
    require([row['dictionary_kind'] for row in domains]==list(range(1,10)), 'seed namespace profile differs')
    require(all(row['dataset_id']==owner['dataset_id'] and row['owner_resource_id']==owner['resource_id']
        and row['owner_epoch']==1 and row['owner_incarnation']==owner['process_incarnation'] for row in domains), 'seed namespace owner differs')
    be=lambda value,width=8:value.to_bytes(width,'big')
    manifest=bytes.fromhex('01010001000200010001')+bytes.fromhex(ids['dataset'])+be(0)
    manifest+=passive.raw(owner['resource_id'],16)+be(0,4)+be(1)+passive.raw(owner['process_incarnation'],16)
    manifest+=bytes.fromhex(initialized['recovery_generation'])+be(1)+bytes([1,9])
    manifest+=b''.join(be(row['dictionary_kind'],2)+passive.raw(row['allocation_domain'],16) for row in domains)
    require(len(manifest)==266 and all(passive.raw(row['manifest_sha256'],32)==hashlib.sha256(manifest).digest()
        for row in state['producer_requests']), 'seed manifest differs from original setup reservations')
    require([row['request_sequence'] for row in sorted(state['producer_requests'],key=lambda row:row['request_sequence'])]==[1,2],
        'seed binding must precede measurement reservation')
    return dict(dataset_id=ids['dataset'],instance_id=ids['instance'],producer_slot_id=initialized['producer_slot_id'],manifest_hex=manifest.hex())


def mounts(info, entry):
    """Exact two read-only test mounts; all standard app custody remains checked."""
    require(entry['service']=='app', 'seed must use the ordinary application recipe')
    paths=entry['monitor_outcome_seed']
    require(set(paths)=={'binary','request'}, 'unexpected seed mount input')
    actual={row['Destination']:row for row in info['Mounts']}
    expected={'/fixture/seed':Path(paths['binary']),'/fixture/request.json':Path(paths['request'])}
    require(all(actual.get(target,{}).get('RW') is False for target in expected), 'seed input mount writable')
    require(info['Config']['Entrypoint']==['/fixture/seed'] and info['Config']['Cmd']==[
        TEST,'--ignored','--exact','--nocapture','--test-threads=1'], 'unexpected test fixture command')
    require([value for value in info['Config']['Env'] if value.startswith('BIGBROTR_MONITOR_OUTCOME_SEED=')]
        ==['BIGBROTR_MONITOR_OUTCOME_SEED=/fixture/request.json'], 'seed request environment differs')
    return expected


def launch(runtime, request):
    require(all(digest(path)==runtime.seed_hashes[name] for name,path in runtime.seed_paths.items()), 'seed fixture inputs changed')
    entry=dict(name=runtime.project+'-monitor-outcome-seed',service='app',id=None,status='creating',
        monitor_outcome_seed=dict(binary=str(runtime.seed_paths['binary']),request=str(request)))
    require(runtime.docker('inspect',entry['name'],check=False).returncode!=0, 'seed container name already exists')
    runtime.resources['containers'].append(entry);runtime.record_resources()
    result=runtime.command([*runtime.compose,'run','-d','--no-deps','--pull','never','--name',entry['name'],
        '--volume',str(runtime.seed_paths['binary'])+':/fixture/seed:ro',
        '--volume',str(request)+':/fixture/request.json:ro',
        '--env','BIGBROTR_MONITOR_OUTCOME_SEED=/fixture/request.json','--entrypoint','/fixture/seed','app',
        TEST,'--ignored','--exact','--nocapture','--test-threads=1'])
    import re
    require(re.fullmatch('[0-9a-f]{64}',result.stdout.strip()), 'seed container ID missing')
    entry['id']=result.stdout.strip();runtime.inspect(entry)
    return entry


def parent_sources(manifest, parent, source):
    """Bind the private test suffix to unchanged independently built product inputs."""
    current=manifest['files_sha256'];baseline=parent['files_sha256']
    for name in (OWNER,FIXTURE):require(digest(source/name)==current[name], 'compiled seed owner differs from acceptance source')
    owner=(source/OWNER).read_bytes();suffix=HOOK.encode()
    require(owner.endswith(suffix), 'seed owner change is not the exact test-only hook')
    runtime_hash=hashlib.sha256(owner[:-len(suffix)]).hexdigest()
    owner_hash=hashlib.sha256(owner).hexdigest()
    require(baseline[OWNER] in (runtime_hash,owner_hash), 'seed changes the runtime owner')
    require((FIXTURE in baseline)==(baseline[OWNER]==owner_hash), 'parent seed fixture and test hook disagree')
    # Keep Cargo ancestry/configuration, external example paths and fixtures in
    # the equality fence too. Only these separately attributed harness owners
    # are outside the unchanged product/build-input comparison.
    harness=lambda name:name.startswith(('docs/','tests/acceptance/')) or name in (
        'experiments/ingest-succession/native.py','experiments/archive-engine/native_commit_proxy.py',
        'experiments/archive-engine/native_commit_proxy_tests.py')
    changed={name for name in set(current)|set(baseline) if name not in (OWNER,FIXTURE)
        and not harness(name) and current.get(name)!=baseline.get(name)}
    require(not changed, 'seed changes production or build inputs')


def artifact(compiled, candidate, source):
    """Existing private libtest artifact format, separately bound to the release."""
    compiled=Path(compiled).resolve(strict=True)
    manifest_path=compiled/'build/source-manifest.json'
    receipt_path=compiled/'artifacts/test-tool-artifact.json'
    binary=compiled/'artifacts/bigbrotr-library-tests'
    parent_path=compiled/'build/parent-product-source-manifest.json'
    require(all(path.is_file() and not path.is_symlink() and 0<path.stat().st_size<=limit
        for path,limit in ((manifest_path,8*1024**2),(receipt_path,8*1024**2),
            (parent_path,8*1024**2),(binary,1024**3))), 'seed artifact type or size differs')
    manifest,receipt=load(manifest_path),load(receipt_path)
    require(receipt['format']=='bigbrotr-test-tool-artifact/1'
        and receipt['kind']=='TEST_ONLY_COMPILATION_NOT_RELEASE'
        and receipt['test_source']==manifest['revision'] and receipt['test_tree']==manifest['tree']
        and receipt['source_manifest_sha256']==digest(manifest_path)
        and receipt['test_source_manifest']==manifest, 'seed fixture source identity differs')
    cargo=receipt['cargo_artifact']
    require(cargo['target']['name']=='bigbrotr' and cargo['target']['kind']==['lib']
        and cargo['target']['src_path']=='/out/build/source/apps/bigbrotr/src/lib.rs'
        and cargo['profile']['test'] is True, 'seed artifact is not the declared library test target')
    compilation=receipt['compilation'];native=compilation['native_toolchain']
    require(compilation['tests_executed']==0 and compilation['status']=='PASS_MONITOR_OUTCOME_SEED_BUILD_ONLY'
        and compilation['clippy_scope']=='all bigbrotr targets including cfg(test)'
        and compilation['lockfile_sha256']==manifest['files_sha256']['Cargo.lock']
        and compilation['source']==manifest['revision'] and compilation['tree']==manifest['tree']
        and compilation['source_manifest_sha256']==digest(manifest_path)
        and compilation['rustc']==candidate['artifact']['build_info']['rustc'], 'seed compilation provenance differs')
    require(dict(os=native['os'],architecture=native['architecture'],
        tools_sha256={name:tool['sha256'] for name,tool in native['tools'].items()})
        ==candidate['artifact']['native_toolchain'], 'seed compiler/native toolchain differs from product')
    require(receipt['artifact']['bytes']==binary.stat().st_size
        and receipt['artifact']['sha256']==digest(binary) and os.access(binary,os.X_OK), 'seed fixture ELF identity differs')
    parent=load(parent_path)
    require(parent['revision']==candidate['artifact']['source']['revision']
        and parent['tree']==candidate['artifact']['source']['tree']
        and digest(parent_path)==candidate['artifact']['source']['manifest_sha256'],
        'seed parent product identity differs')
    parent_sources(manifest,parent,source)
    # Python admission/oracles are attributed to the current acceptance source
    # by candidate()/source_unchanged(), not compiled into this Rust artifact.
    # Keep the closed policy input and every compiled fixture byte exact.
    name='tests/fixtures/configuration/policy-v3.json'
    require(digest(source/name)==manifest['files_sha256'][name], 'seed policy input changed')
    return dict(binary=binary,receipt=receipt_path,manifest=manifest_path,parent_manifest=parent_path),dict(
        source=manifest['revision'],tree=manifest['tree'],source_manifest_sha256=digest(manifest_path),
        receipt_sha256=digest(receipt_path),binary_sha256=digest(binary),test=TEST,
        parent_product=candidate['artifact']['source'])
