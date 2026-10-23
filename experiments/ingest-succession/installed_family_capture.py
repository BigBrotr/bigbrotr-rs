#!/usr/bin/env python3
"""Offline raw binding for installed successor lifetimes; no engine access.

Scientific state interpretation belongs only to installed_family_replay. This
reader binds its complete contexts to pinned final origins, process and HTTP
captures, native snapshots and actual retained input bytes. SQL preservation is
captured-run evidence; reading original files does not open the native engine.
"""
import argparse
import copy
import json
import math
from pathlib import Path
import re
import stat
import sys
import tomllib

sys.dont_write_bytecode = True
import history_replay as history
from retirement_capture_replay import physical, spool_checked

sha, same, only, instant = history.sha, history.same, history.only, history.instant
# Literal final endpoints, independently reconciled against retained result and
# verification files; never derive an original from the new installed output.
PINS = {'activated': {'name': 'activated',
               'run': 'ingest-activated-retirement-engine-9bdc86a',
               'subdirectory': 'bindings',
               'database': 'bb_scope_bind_9bdc86a_installed_activated_retirement_off',
               'status': 'PASS_INSTALLED_ACTIVATED_RETIREMENT_FIRST_WAVE',
               'source': '9bdc86a2316e0aa0d1078ededb80a8d9c9502a2f',
               'owner': '1979cce97962b0239e79e3fa47b2e5ae',
               'label': 'org.bigbrotr.acquisition-binding',
               'client_id': 'fe56e36ac2dd47fa41842cef7d6bbc8564e2032352c5097f68448047bb2c89c0',
               'source_clone': 'installed-activated-retirement-off',
               'final_case': 'installed-activated-retirement-off-retry',
               'catalog_sha256': 'baa02a5fc187770cd8b5782ab47feab4dd11d54667aa2737d19411db546e9276',
               'result_sha256': '48506a152b393854f2f50cbedfedb71b66e53ed9578753d7e6b4df3bd76c8e09',
               'snapshot': 'installed-activated-retirement-off-retry-after-sql.json',
               'snapshot_sha256': '7ac690057de370a00144ffd415bb1f504b05ff4b5599d6458b728820a98acbad',
               'native_file': 'installed-activated-retirement-off-retry-after-native.json',
               'native_sha256': 'cd49f8bfc44c5c08ae307f845f1aaf1da7f66079bf14f8ee54f9c4c1a3819238',
               'inputs_file': 'installed-activated-retirement-off-retry-after-inputs.json',
               'inputs_sha256': 'f59224bde2f441efa1b74bbdb64c65ae5a30bdd2f283f9d31b5dd5c3adb82589',
               'store': 'installed-activated-retirement-off/store',
               'origin_format': 'legacy-installed',
               'verification_file': 'installed-activated-retirement-off-retry-verification.json',
               'verification_sha256': 'b1e0b4eaada03b9c169592750d70285e8234abfa7ebbebc61877255b3b046d82'},
 'empty': {'name': 'empty',
           'run': 'ingest-empty-installed-engine-788f971',
           'subdirectory': 'bindings',
           'database': 'bb_scope_bind_788f971_installed_empty_normal',
           'status': 'PASS_INSTALLED_EMPTY_RETIREMENT',
           'source': '788f9711e19776c4441042747ef68d5f2057605f',
           'owner': 'a736fb2609186b694cf08d128696083b',
           'label': 'org.bigbrotr.acquisition-binding',
           'client_id': '40f8a4e5f9c86ed6c5c0600b1d47c3d3ecf24ac0382252326e1d2effad84b8ff',
           'source_clone': 'installed-empty-normal',
           'final_case': 'installed-empty-normal-terminal',
           'catalog_sha256': 'f275e865518402db42799a42f6571dfae4e7299e576b784c21a9c5d6e7ce3b1e',
           'result_sha256': 'ff1d2e111dc42ff4026c7e42985a7814dbf058eb0bd1d7ba92c5e269e7642d34',
           'snapshot': 'installed-empty-normal-terminal-after.json',
           'snapshot_sha256': '5cc9803df1c4f2093f56694ca08857bbdf62066c51f260a35637ce2208aa7579',
           'native_file': 'installed-empty-normal-terminal-after-native.json',
           'native_sha256': '53ca6c812d7ed8de07a0843af25b438794e5aa7b4f925f296c1c0de80c14097b',
           'inputs_file': 'installed-empty-normal-terminal-after-inputs.json',
           'inputs_sha256': 'c757c9978a581ccef16c249f0fd1d8d15c64d9996b4c02b3bea1830a86cddba0',
           'store': 'installed-empty-normal/store',
           'origin_format': 'legacy-installed',
           'verification_file': 'installed-empty-normal-terminal-verification.json',
           'verification_sha256': '3802cfce18da75320ccb2b9f6b5c49f11c077a2f0d258168c2610f6593f65187'},
 'later': {'name': 'later',
           'run': 'ingest-succession-retirement-fault-engine-58b79a4',
           'subdirectory': 'retirement-faults',
           'database': 'bb_successor_retirement_fault_58b79a4_before',
           'status': 'PASS_SUCCESSOR_RETIREMENT_FAULTS',
           'source': '58b79a49e59be77be554fff0be826dbe8ee20903',
           'owner': '3c545f54303dc8aafd23e00253f613e7',
           'label': 'org.bigbrotr.succession-retirement-fault',
           'client_id': 'e87ef2ecbe2387731f97add4dbd74998318c084d8fcb0916c98b530822ffd80c',
           'source_clone': 'before',
           'final_case': 'before-forward-fresh',
           'catalog_sha256': '062b2dcee8d28ce48461cca668dd3c9d20f94e3035041f1a817925fafa77263e',
           'result_sha256': '4690448e2ac9b565dea2fd1c5f3207831a3d0a77ef3dca5d7896d1928abfc03f',
           'snapshot': 'before-forward-fresh-after.json',
           'snapshot_sha256': '43ec972783bf1d82bad94ab954dd605e32d50da7f6db3acf48c95778280415b3',
           'native_file': 'before-forward-fresh-after-native.json',
           'native_sha256': '269bf396c8cfc09588405495364359df1fab33737036d30990469530aa5722bc',
           'inputs_file': 'before-forward-fresh-after-inputs.json',
           'inputs_sha256': 'c757c9978a581ccef16c249f0fd1d8d15c64d9996b4c02b3bea1830a86cddba0',
           'store': 'before/store'}}
CASES = ('activated-live3', 'empty-live3', 'later-live4')
CLONES = ('activated', 'empty', 'later')
STAGES = ('activated', 'prepared', 'submitted', 'renewed', 'before-stop')
STATUS = 'PASS_INSTALLED_SUCCESSOR_FAMILY'
PRESERVED = dict(rows_unchanged=True, catalog_unchanged=True, native_unchanged=True,
                 inputs_unchanged=True, physical_unchanged=True)
CONTEXT_FIELDS = {'case','before','after','native_before','native_after','inputs_before','inputs_after',
    'input_contents','snapshots','response','journey','fixture','server_window','backend',
    'catalog_before','catalog_after','fixture_input'}


def loads(value):
    def pairs(items):
        answer = {}
        for key, item in items:
            assert key not in answer, 'duplicate JSON key'
            answer[key] = item
        return answer
    def constant(value):
        raise AssertionError('nonfinite JSON number: ' + value)
    return json.loads(value, object_pairs_hook=pairs, parse_constant=constant)


def number(value, lower=0, upper=math.inf, *, integer=False):
    assert type(value) in ((int,) if integer else (int,float))
    assert math.isfinite(value) and lower <= value <= upper
    return value


def origin_checked(root, folder, name, declaration, raw, read):
    pin = PINS[name]
    assert set(declaration) == set(pin) | {'pins','files','inputs','native','previous_client','rows','catalog','preserved'}
    assert declaration['pins'] == pin
    assert all(declaration[k] == v for k,v in pin.items() if k != 'store')
    basis = root.parent / pin['run'] / pin['subdirectory']
    store = basis / pin['store']
    assert declaration['store'] == str(store)
    original = {}
    for key, digest in [('snapshot','snapshot_sha256'),('native_file','native_sha256'),('inputs_file','inputs_sha256')]:
        value = raw(basis / pin[key])
        assert sha(value) == pin[digest]
        original[key] = loads(value)
    rows,native,saved = [original[k] for k in ('snapshot','native_file','inputs_file')]
    inputs = saved.get('inputs',saved)
    result_bytes = raw(basis/'result.json')
    assert sha(result_bytes) == pin['result_sha256']
    prior = loads(result_bytes)
    for key in ('source','owner','label','status'):
        assert prior[key] == pin[key]
    assert all(prior[k] == [] for k in ('cleanup_errors','missing_owned','unrelated_changed'))
    assert declaration['previous_client'] == prior['client']
    if name in ('activated','empty'):
        assert set(prior['client']) == {'id','name'} and prior['client']['id'] == pin['client_id']
        group, = prior['cases']
        assert group['status'] == 'PASS'
        old = only(group['cases'],name=pin['source_clone'])
        attempt = only(old['attempts'],name=pin['final_case'])
        assert old['status'] == attempt['status'] == 'PASS'
        captured = raw(basis/pin['verification_file'])
        assert sha(captured) == pin['verification_sha256'] == attempt['verification_sha256']
        context = loads(captured)
        same(rows,context['after'])
        assert native == context['native_after'] and inputs == context['inputs_after']
        response = context['response']
        assert response['outcome'] == 'PROCESS_EXITED' and type(response['returncode']) is int and response['returncode'] == 0
        assert response['stderr'] == ''
        terminal = response['terminal']
        assert terminal['reaped'] is True and terminal['forced'] is False and terminal['exit_code'] == 0
        assert terminal['config'] == '/lab/'+pin['database']+'.toml' and terminal['prefix'] == '/lab/'+pin['final_case']
        stopped, = [loads(line) for line in response['stdout'].splitlines()]
        assert stopped['state'] == ('activated-initial-retired' if name == 'activated' else 'empty-attempt-retired')
        assert stopped['producer_epoch'] == '2' and stopped['native'] == 'closed' and stopped['owner_release'] == 'committed'
        assert stopped['ready'] is False
    else:
        assert prior['client'] == pin['client_id'] and prior['unexpected_containers'] == []
        old = only(prior['clones'],name=pin['source_clone'])
        assert old['status'] == 'PASS' and old['database'] == pin['database']
        assert old['final_case'] == pin['final_case'] and old['directory']+'/store' == pin['store']
        assert sha(old['final_catalog'].encode()) == pin['catalog_sha256']
        final = only(prior['cases'],name=pin['final_case'])
        assert final['status'] == 'PASS' and final['returncode'] == 0 and final['custody'] == 'OBSERVED_CLOSED_AND_JOINED'
    same(rows,declaration['rows']); same(rows,read(folder/(name+'-original.json')))
    assert native == declaration['native'] and inputs == declaration['inputs']
    assert sha(declaration['catalog'].encode()) == pin['catalog_sha256']
    assert physical(store,raw) == declaration['files']
    spool_checked(store/'producer-inputs-v1',inputs,raw,complete=True)
    if 'contents' in saved:
        assert {n:raw(store/'producer-inputs-v1'/n).hex() for n in inputs} == saved['contents']
    assert declaration['preserved'] == read(folder/(name+'-preserved.json')) == PRESERVED
    assert all(v is True for v in declaration['preserved'].values())
    return rows,native,inputs


def clone_checked(folder, clone, origin, report, raw, read, *, progress=None, commit_loss=False):
    name = clone['name']
    assert type(commit_loss) is bool and (not commit_loss or progress in ('split','ack'))
    original_name = 'activated' if commit_loss else name
    assert clone['origin'] == original_name and clone['directory'] == name and clone['status'] == 'PASS'
    prefix = 'bb_installed_successor_' if progress is None else 'bb_installed_progress_' + progress + '_'
    if commit_loss:
        from installed_progress_commit_oracle import profile_names, MODES
        assert name in MODES
        prefix = profile_names(ack=progress=='ack')['database_prefix']
    assert clone['database'] == prefix+report['source'][:7]+'_'+name.replace('-','_')
    assert clone['config'] == '/lab/'+clone['database']+'.toml'
    assert clone['final_case'] == (name+'-retry' if commit_loss else CASES[CLONES.index(name)])
    assert clone['final_backends'] == []
    same(origin[0],read(folder/(name+'-copy-before.json')))
    floor = {'activated':37,'empty':31,'later':38}[original_name]
    steps = [dict(name=f'{v:04d}_{history.MIGRATIONS[v][0]}.sql',sha256=history.MIGRATIONS[v][1],version=str(v)) for v in range(floor+1,39)]
    target = '38'
    if progress is not None:
        from installed_progress_replay import require_frozen_profile
        target = '39'
        steps.append(dict(name='0039_producer_progress.sql',
            sha256=require_frozen_profile()['migration_sha256'], version='39'))
        if progress=='ack':
            target='40'
            steps.append(dict(name='0040_producer_ack.sql',
                sha256=require_frozen_profile(ack=True)['migration_sha256'],version='40'))
    assert [v['action'] for v in clone['migrations']] == ['plan','apply','apply']
    for index, answer in enumerate(clone['migrations']):
        assert set(answer) == {'action','exit_code','stdout','stderr'}
        assert type(answer['exit_code']) is int and answer['exit_code'] == 0 and answer['stderr'] == ''
        assert loads(answer['stdout']) == dict(format='bigbrotr-control-migration/1',scope='postgres-control',target_version=target,
            source_version=str(floor) if index<2 else target,current_version=str(floor) if index==0 else target,
            status='planned' if index==0 else ('applied' if index==1 and steps else 'current'),
            applied_count=str(len(steps)) if index==1 else '0',steps=steps if index<2 else [],
            dataset_initialization='not-performed',release_upgrade_qualification='not-performed')
    grants = clone['grants']
    assert len(grants) == (5 if commit_loss else 3 if progress is not None else 2)
    assert all(set(g)=={'statement','stdout'} and not g['stdout'].strip() for g in grants)
    assert sha(grants[0]['statement'].encode()) == '36a16062c994adfe1ba7ace4c5e56d2ed942277b0e227ddd048e320939f0e588'
    assert grants[1]['statement'] == 'GRANT SELECT ON control.initial_ingest_activations TO bigbrotr_control;'
    if progress is not None:
        from installed_progress_replay import PROGRESS_GRANTS
        assert grants[-1]['statement'] == PROGRESS_GRANTS
        if commit_loss:
            assert [g['statement'] for g in grants[2:4]] == [
                'GRANT UPDATE ON control.producer_epochs TO bigbrotr_control;',
                'GRANT INSERT ON control.producer_outcomes TO bigbrotr_control;']
    assert clone['catalog_before'] == clone['catalog_after'] == clone['final_catalog']
    config = tomllib.loads(raw(folder/Path(clone['config']).name).decode())
    ds = only(origin[0]['datasets'])
    assert {k:config['postgres']['control'][k] for k in ('database','endpoint','user','password_ref')} == dict(
        database=clone['database'],endpoint='127.0.0.1:5432',user='bigbrotr_control',password_ref='file:/run/secrets/application')
    assert config['postgres']['control']['tls'] == dict(local_boundary='loopback',mode='local-plaintext')
    assert config['storage']['ingest_root'] == '/lab/'+name+'/store'
    assert config['topology']['dataset_id'] == ds['dataset_id'][2:] and config['topology']['instance_id'] == ds['instance_id'][2:]
    assert config['topology']['local_roles'] == ['control','ingest','sync','archive']
    assert config['runtime']['processes']['application'] == dict(memory_max_bytes='1536MiB',baseline_bytes='128MiB',payload_bytes='1GiB',native_bytes='256MiB',completion_reserve_bytes='32MiB')
    assert {k:config['runtime'][k] for k in ('lease_ttl','heartbeat_interval','renewal_timeout','authorization_ttl','graceful_shutdown_timeout')} == dict(
        lease_ttl='6s',heartbeat_interval='1s',renewal_timeout='1s',authorization_ttl='5s',graceful_shutdown_timeout='2s')
    assert config['administration']['enabled'] is False and config['administration']['listen']['port']==18081
    assert config['sync']['enabled'] is False and config['ingest']['producer_outstanding_requests']==(2 if progress=='ack' else 65)
    assert config['network']['dns']['resolver']=='127.0.0.1:9' and config['network']['tls']['extra_roots']==[]
    assert config['network']['private']['rules']==[dict(endpoint='ws://127.0.0.1:7447/installed-successor-fixture',cidr='127.0.0.1/32',port=7447)]
    for filename in (name+'-config-validation.json',clone['database']+'-installed-config-validation.json'):
        answer=read(folder/filename)
        assert answer['returncode']==0 and type(answer['returncode']) is int and answer['stderr']==''
        loads(answer['stdout'])
    migrated=read(folder/(name+'-migrated.json'))
    baseline = migrated
    if progress is not None:
        from installed_progress_replay import legacy_state
        baseline = legacy_state(migrated,ack=progress=='ack')
        assert all(migrated[t] == [] for t in
            ('producer_checkpoints','acquisition_progress','producer_checkpoint_dependencies'))
    if floor==38:
        same(origin[0],baseline)
    else:
        history.baseline(baseline,origin[1],origin[2],original_name,read(folder/(clone['final_case']+'-clock.json')))
    return migrated


def process_checked(folder, case, response, raw, read):
    name=case['name']; prefix=folder/name
    identity=read(Path(str(prefix)+'.pid.json'))
    assert set(identity)=={'pid','start_ticks','command','config','prefix'}
    number(identity['pid'],1,integer=True); number(identity['start_ticks'],1,integer=True)
    assert identity['config']==case['config'] and identity['prefix']=='/lab/'+name
    assert identity['command']==['/build/bigbrotr','serve','--file',case['config'],'--activate-successor','--roles','control,ingest,sync']
    terminal=read(Path(str(prefix)+'.terminal.json'))
    assert set(terminal)==set(identity)|{'exit_code','reaped','forced','elapsed_seconds'}
    assert all(terminal[k]==v for k,v in identity.items())
    assert terminal['exit_code']==0 and type(terminal['exit_code']) is int and terminal['reaped'] is True and terminal['forced'] is False
    number(terminal['elapsed_seconds'],0,60)
    assert set(response)=={'returncode','stdout','stderr','outcome','terminal'}
    assert response['terminal']==terminal and response['outcome']=='PROCESS_EXITED'
    assert type(response['returncode']) is int and response['returncode']==0
    for stream in ('stdout','stderr'):
        value=raw(Path(str(prefix)+'.'+stream))
        assert len(value)<=65536 and response[stream]==value.decode()
    assert response['stderr']==''
    activated,stopped=[loads(line) for line in response['stdout'].splitlines()]
    assert activated['format']==stopped['format']=='bigbrotr-ingest-successor/1' and activated['state']=='activated'
    assert stopped=={k:activated[k] for k in ('format','dataset_id','owner_resource_id','producer_slot_id','producer_epoch')}|dict(
        state='stopped',native='closed',owner_release='committed',source_closure='not-performed',ready=False)
    assert activated['ready'] is False and stopped['ready'] is False
    return identity,activated


def backend_checked(value, database, before, after):
    assert set(value)=={'absence','exact','all'} and value['all']==[]
    old={(r['resource_id'],r['epoch']) for r in before['owner_epochs']}
    current,=[r for r in after['owner_epochs'] if r['role']=='control' and (r['resource_id'],r['epoch']) not in old]
    expected=dict(pid=current['control_backend_pid'],backend_start=current['control_backend_start'],database=database,control_epoch=current['epoch'],present=False)
    assert value['exact']==[expected] and value['exact'][0]['present'] is False
    absence=value['absence']
    assert set(absence)=={'database','user','limit_seconds','status','attempts','seconds'}
    assert absence['database']==database and absence['user']=='bigbrotr_control' and absence['limit_seconds']==5
    assert type(absence['limit_seconds']) is int and absence['status']=='OBSERVED_ABSENT' and absence['attempts']
    elapsed=0
    for attempt in absence['attempts']:
        assert set(attempt)=={'seconds','count','backends'}
        elapsed=number(attempt['seconds'],elapsed,5)
        assert type(attempt['count']) is int and attempt['count']==len(attempt['backends']) and attempt['count'] in (0,1)
        for backend in attempt['backends']:
            assert set(backend)=={'pid','backend_start'} and type(backend['pid']) is int
            assert backend['pid']==current['control_backend_pid'] and instant(backend['backend_start'])==instant(current['control_backend_start'])
    assert absence['attempts'][-1]['backends']==[]
    number(absence['seconds'],elapsed,6)


def progress_transport_checked(directory, journey, snapshots, window, raw, read, *, hole=False, commit_loss=False, ack=False, mutation_loss=False):
    """Bind separated wire/capture phases; never certify a typed checkpoint.

    This is preparation for the opt-in profile. The default replay below does
    not call it and retains its original closed PASS predicate.
    """
    from live_replay import OLD_EVENT, NEW_EVENT
    assert type(hole) is bool
    assert type(commit_loss) is bool and not (hole and commit_loss)
    assert type(mutation_loss) is bool and (not mutation_loss or (ack and commit_loss))
    assert type(ack) is bool and (not ack or (not hole and (not commit_loss or mutation_loss)))
    assert read(directory/'events.json') == [OLD_EVENT, NEW_EVENT]
    result = journey['result']
    assert result == read(directory/'client-result.json')
    assert set(result) == {'status', 'login', 'logout', 'me', 'digests', 'observations',
        'job_status', 'relay_joined', 'unauthenticated', 'checkpoint_validation', 'native_proof_validation'} | (
            {'native_ack_validation'} if ack else set())
    if ack:
        from installed_progress_replay import ack_event_sequence
        assert result['native_ack_validation']=='NOT_RUN'
        assert read(directory/'progress-ack-events.json')==dict(format='bigbrotr-ack-events/1',events=ack_event_sequence(successor=True))
    assert result['status'] == 'CAPTURED_PROGRESS_TRANSPORT_ONLY'
    assert result['checkpoint_validation'] == result['native_proof_validation'] == 'NOT_RUN'
    assert result['relay_joined'] is True
    assert journey['terminal'] == read(directory/'client-exit.json') == dict(
        exit_code=0, reaped=True, outcome='PROCESS_EXITED')
    assert type(journey['terminal']['exit_code']) is int
    assert journey['stdout'] == raw(directory/'client.stdout').decode() == ''
    assert journey['stderr'] == raw(directory/'client.stderr').decode() == ''
    opened, closed = journey['opened'], journey['closed']
    assert (1 if mutation_loss else 2 if commit_loss else 3) <= len(opened) == len(closed) <= 128
    assert [p.name for p in sorted(directory.glob('relay-open-*.json'))] == [
        f'relay-open-{n:03}.json' for n in range(1, len(opened)+1)]
    assert [p.name for p in sorted(directory.glob('relay-closed-*.json'))] == [
        f'relay-closed-{n:03}.json' for n in range(1, len(closed)+1)]
    nonces, phases = set(), {}
    first_fields = {'number', 'connected_at_us', 'clock_resolution_us', 'request',
        'request_at_us', 'request_monotonic_ns'}
    for n, (first, last) in enumerate(zip(opened, closed, strict=True), 1):
        assert first == read(directory/f'relay-open-{n:03}.json')
        assert last == read(directory/f'relay-closed-{n:03}.json')
        assert set(first) == first_fields
        assert first['number'] == n and type(first['number']) is int
        assert all(last[k] == v for k,v in first.items())
        request = first['request']
        assert isinstance(request, list) and len(request) == 3
        assert request[0] == 'REQ' and request[2] == {'kinds':[1], 'limit':3}
        ident = request[1]
        assert re.fullmatch('[0-9a-f]{48}', ident) and int(ident[:32], 16)
        assert ident[:32] not in nonces
        nonces.add(ident[:32])
        number(first['connected_at_us'], window['before_us'], window['after_us'], integer=True)
        number(first['request_at_us'], first['connected_at_us'], window['after_us'], integer=True)
        number(last['closed_at_us'], first['request_at_us'], window['after_us'], integer=True)
        assert last['closed_at_us']-first['request_at_us'] < 3_000_000
        number(first['request_monotonic_ns'], 1, integer=True)
        number(last['closed_monotonic_ns'], first['request_monotonic_ns'], integer=True)
        assert last['closed_monotonic_ns']-first['request_monotonic_ns'] < 3_000_000_000
        number(first['clock_resolution_us'], 1, 1_000_000, integer=True)
        extra = {'frames', 'completion', 'closed_at_us', 'closed_monotonic_ns'}
        if 'progress_phase' not in last:
            assert set(last) == first_fields | extra
            assert last['frames'] == [] and last['completion'] == 'quiet-eof'
            continue
        phase = last['progress_phase']
        assert phase in ('mutation', 'proof-only') and phase not in phases
        phases[phase] = last
        extra |= {'progress_phase', 'event_ids', 'event_json_sha256', 'sent_at_us'}
        values = [NEW_EVENT, OLD_EVENT] if phase == 'mutation' else [NEW_EVENT]
        labels = ['distinct', 'predecessor'] if phase == 'mutation' else ['distinct-resend']
        if ack:
            values,labels=ack_event_sequence(successor=True),['signed']*3
        if hole and phase == 'mutation':
            values, labels = [{}, *values], ['preparation-hole', *labels]
        if phase == 'proof-only':
            extra.add('progress_release')
        assert set(last) == first_fields | extra
        assert last['event_ids'] == [value.get('id') for value in values]
        assert last['event_json_sha256'] == [sha(json.dumps(value, ensure_ascii=False,
            separators=(',', ':')).encode()) for value in values]
        assert last['frames'] == labels + ['EOSE']
        assert last['completion'] == 'request-and-websocket-close'
        number(last['sent_at_us'], first['request_at_us'], last['closed_at_us'], integer=True)
    if mutation_loss:
        assert list(phases)==['mutation'] and phases['mutation']['number']==1
        assert [s['stage'] for s in snapshots]==['activated','prepared']
        for name in ('progress-mutation-observation.json','progress-proof-only-observation.json','progress-release.json'):
            assert not (directory/name).exists() and not (directory/name).is_symlink()
        assert read(directory/'progress-context.json')==dict(format='bigbrotr-ack-context/1',kind='successor')
        assert not (directory/'relay-error.json').exists()
        return dict(status='CAPTURED_MUTATION_LOSS_RELAY',mutation_request=phases['mutation']['request'][1],
            quanta=len(closed),checkpoint_validation='NOT_RUN',native_proof_validation='NOT_RUN',
            sql_semantics='NOT_RUN',intermediate_cut='NOT_OBSERVED')
    assert list(phases) == ['mutation', 'proof-only']
    mutation, proof = phases['mutation'], phases['proof-only']
    assert mutation['number'] == 1 < proof['number'] <= len(closed)
    if not commit_loss:
        assert proof['number'] < len(closed)
    assert mutation['request'][1] != proof['request'][1]
    assert mutation['closed_monotonic_ns'] < proof['request_monotonic_ns']
    observations = {}
    for phase, wire in phases.items():
        if commit_loss and phase == 'proof-only':
            assert not (directory/'progress-proof-only-observation.json').exists()
            continue
        path = directory/f'progress-{phase}-observation.json'
        value = read(path)
        assert set(value) == {'format', 'phase', 'observation', 'wire'}
        assert value['format'] == 'bigbrotr-progress-transport-observation/1'
        assert value['phase'] == phase and value['wire'] == wire
        observation = value['observation']
        assert set(observation) == {'stage', 'server_us', 'observed_by_us', 'control', 'inputs'}
        assert observation['stage'] == 'progress-' + phase
        assert [s for s in snapshots if s['stage'] == observation['stage']] == [observation]
        number(observation['server_us'], wire['closed_at_us'], window['after_us'], integer=True)
        number(observation['observed_by_us'], observation['server_us'], window['after_us'], integer=True)
        assert isinstance(observation['control'], dict) and isinstance(observation['inputs'], dict)
        observations[phase] = observation
    assert observations['mutation']['observed_by_us'] <= proof['sent_at_us']
    if not commit_loss:
        assert observations['mutation']['inputs'] == observations['proof-only']['inputs']
    gate = read(directory/'progress-release.json')
    assert gate == dict(format='bigbrotr-progress-transport-release/1',
        mutation_request=mutation['request'][1],
        observation_sha256=sha(raw(directory/'progress-mutation-observation.json')))
    assert proof['progress_release'] == dict(gate,
        release_sha256=sha(raw(directory/'progress-release.json')))
    assert not (directory/'relay-error.json').exists()
    return dict(status='CAPTURED_SPLIT_RELAY', mutation_request=mutation['request'][1],
        proof_only_request=proof['request'][1], quanta=len(closed),
        captured_input_census_equal=None if commit_loss else True, checkpoint_validation='NOT_RUN',
        native_proof_validation='NOT_RUN', sql_semantics='NOT_RUN')


def journey_checked(directory, context, identity, window, raw, read, *, progress=False, commit_loss=False, mutation_loss=False):
    assert type(commit_loss) is bool and (not commit_loss or progress)
    assert type(mutation_loss) is bool and (not mutation_loss or commit_loss)
    journey=context['journey']; fixture=context['fixture']
    assert set(journey)=={'result','terminal','stdout','stderr','opened','closed'}
    assert journey['result']==read(directory/'client-result.json') and journey['terminal']==read(directory/'client-exit.json')
    assert journey['terminal']==dict(exit_code=0,reaped=True,outcome='PROCESS_EXITED') and journey['terminal']['reaped'] is True
    assert type(journey['terminal']['exit_code']) is int
    assert journey['stdout']==raw(directory/'client.stdout').decode()==''
    assert journey['stderr']==raw(directory/'client.stderr').decode()==''
    assert fixture['client_terminal']==journey['terminal'] and fixture['cleanup_errors']==[]
    result=journey['result']
    assert result['status']==('CAPTURED_PROGRESS_TRANSPORT_ONLY' if progress else 'PASS')
    assert result['relay_joined'] is True
    commands=[read(directory/f'command-{i}.json') for i in range(1,5)]
    assert commands==result['observations']
    assert sorted(p.name for p in directory.glob('command-*.json'))==[f'command-{i}.json' for i in range(1,5)]
    for stage,number_of_commands in [('prepared',3),('submitted',4),('commands-finished',4)]:
        assert read(directory/(stage+'.json'))==commands[:number_of_commands]
        assert raw(directory/(stage+'.continue'))==b'observed\n'
    assert context['fixture_input']==read(directory/'fixture.json')
    from live_replay import OLD_EVENT, NEW_EVENT
    assert read(directory/'events.json')==[OLD_EVENT,NEW_EVENT]
    opens=sorted(directory.glob('relay-open-*.json')); closes=sorted(directory.glob('relay-closed-*.json'))
    assert (1 if mutation_loss else 2)<=len(opens)==len(closes)<=128
    assert [p.name for p in opens]==[f'relay-open-{n:03d}.json' for n in range(1,len(opens)+1)]
    assert [p.name for p in closes]==[f'relay-closed-{n:03d}.json' for n in range(1,len(closes)+1)]
    assert journey['opened']==[read(p) for p in opens] and journey['closed']==[read(p) for p in closes]
    for opened,closed in zip(journey['opened'],journey['closed'],strict=True):
        assert all(closed[k]==v for k,v in opened.items())
        number(opened['connected_at_us'],window['before_us'],window['after_us'],integer=True)
        number(opened['request_at_us'],opened['connected_at_us'],window['after_us'],integer=True)
        number(closed['closed_at_us'],opened['request_at_us'],window['after_us'],integer=True)
        number(opened['request_monotonic_ns'],1,integer=True)
        number(closed['closed_monotonic_ns'],opened['request_monotonic_ns'],integer=True)
    if commit_loss:
        assert result['job_status'] is result['logout'] is None
        assert not (directory/'signal-observation.json').exists()
        assert not (directory/'relay-error.json').exists()
        return
    signal=read(directory/'signal-observation.json')
    assert signal==fixture['signal'] and signal['identity']==identity and signal['kind']=='SIGTERM'
    assert signal['closed_before_signal'] is False and signal['opened']==fixture['quiet']
    assert signal['opened'] in journey['opened']
    number(signal['signal_at_us'],signal['opened']['request_at_us'],window['after_us'],integer=True)
    number(signal['signal_returned_at_us'],signal['signal_at_us'],window['after_us'],integer=True)
    number(signal['signal_monotonic_ns'],signal['opened']['request_monotonic_ns'],integer=True)
    number(signal['signal_returned_monotonic_ns'],signal['signal_monotonic_ns'],integer=True)
    assert not (directory/'relay-error.json').exists()


def inventory_checked(before, after, client, postgres):
    """Reproduce inventory.projection's sole Health.Log normalization."""
    assert len({v['Id'] for v in before}) == len(before)
    assert client not in {v['Id'] for v in before}
    assert set(after) == {v['Id'] for v in before} | {client}
    for item in before:
        state = copy.deepcopy(item['State'])
        if 'Health' in state:
            state['Health'].pop('Log',None)
        expected = dict(Id=item['Id'],Name=item['Name'],Image=item['Image'],State=state,
            Mounts=sorted(item['Mounts'],key=lambda m:m['Destination']))
        if item['Id'] == postgres:
            assert state['Status']=='exited' and state['Running'] is False and state['Pid']==0
            assert all(after[postgres][k]==expected[k] for k in ('Id','Name','Image','Mounts'))
            assert item['HostConfig']['Memory']==1536*1024**2 and item['HostConfig']['NanoCpus']==2_000_000_000
        else:
            assert after[item['Id']]==expected, 'unrelated container projection changed'


def replay(root, *, progress=None):
    from installed_family_replay import verify, negative_controls
    assert progress in (None, 'split', 'hole', 'ack')
    ack=progress=='ack'
    selected_clones,selected_cases=(CLONES[:1],CASES[:1]) if ack else (CLONES,CASES)
    if progress is not None:
        import installed_progress_replay as progress_replay
        progress_replay.require_frozen_profile(ack=ack)
        assert progress in ('split','ack'), 'hole/refusal capture is not positive qualification'
    root=Path(root).resolve(strict=True)
    folder=root/('installed-family' if progress is None else 'installed-progress-' + progress)
    bound={}
    def raw(path):
        path=Path(path); assert stat.S_ISREG(path.lstat().st_mode), 'capture must be a regular file'
        value=path.read_bytes(); bound[str(path)]=sha(value); return value
    def read(path): return loads(raw(path))
    report=read(folder/'result.json')
    status = STATUS if progress is None else 'PASS_INSTALLED_PRODUCER_ACK' if ack else 'PASS_INSTALLED_PRODUCER_PROGRESS'
    label = 'org.bigbrotr.installed-successor' if progress is None else 'org.bigbrotr.installed-progress-' + progress
    assert report['status']==status and report['label']==label
    assert re.fullmatch('[0-9a-f]{40}',report['source']) and re.fullmatch('[0-9a-f]{40}',report['tree'])
    assert re.fullmatch('[0-9a-f]{32}',report['owner']) and re.fullmatch('[0-9a-f]{64}',report['client'])
    assert all(report[k]==[] for k in ('cleanup_errors','missing_owned','unrelated_changed','unexpected_containers'))
    assert [c['name'] for c in report['clones']]==list(selected_clones) and [c['name'] for c in report['cases']]==list(selected_cases)
    assert len(set(report['databases']))==len(selected_clones) and report['databases']==[c['database'] for c in report['clones']]
    assert not set(report['databases']) & set(report['database_names_before'])
    assert set(report['database_names_after'])==set(report['database_names_before'])|set(report['databases'])
    started,finished=instant(report['started_at']),instant(report['finished_at']); assert started<=finished
    assert set(report['origins'])==set(selected_clones)
    origins={n:origin_checked(root,folder,n,report['origins'][n],raw,read) for n in selected_clones}
    terminal=read(folder/'terminal-containers.json')
    inventory_checked(read(folder/'preflight-containers.json'),terminal,report['client'],report['postgres'])
    client=None
    for filename,ident in [('client-terminal.json',report['client']),('postgres-terminal.json',report['postgres'])]:
        value=read(folder/filename); assert value==terminal[ident] and value['Id']==ident
        state=value['State']; assert state['Status']=='exited' and state['Pid']==0
        assert all(state[k] is False for k in ('Running','Paused','Restarting','OOMKilled','Dead')) and not state['Error']
        assert type(state['ExitCode']) is int and state['ExitCode']==(143 if ident==report['client'] else 0)
        if ident==report['client']: client=value
    assert report['postgres']=='7177550705c49b14237e20790af411af58f62f22d110cd0dfa1afddaf78861ec'
    assert report['postgres_owner']=='5e69b0f060b1607c25230d1dbd3e5608'
    mount=only(client['Mounts'],Destination='/lab'); assert mount['RW'] is True and mount['Source']==str(folder)
    mount=only(client['Mounts'],Destination='/build'); assert mount['RW'] is False
    build=Path(mount['Source']); assert build.is_absolute()
    built,component=read(build/'build-result.json'),read(build/'component-result.json')
    qualification=read(build.parent/'qualification.json')
    allowed = ('PASS_SUCCESSION_TRANSITION_COMPONENTS',
        'PASS_SUCCESSION_HISTORY_PROBE_BUILD','PASS_SUCCESSION_TRANSITION_PROBE_BUILD',
        'PASS_SUCCESSION_RECOVERY_COMPONENTS','PASS_SUCCESSION_INSTALLED_COMPONENTS')
    assert qualification['status'] in (('PASS_PRODUCER_ACK_COMPONENTS',) if ack else ('PASS_PRODUCER_PROGRESS_COMPONENTS',) if progress else allowed)
    assert report['build']==built and report['component']==component and component['status']=='PASS'
    assert component['source']==built['source']==qualification['source']
    if progress is not None:
        assert built['source']==progress_replay.require_frozen_profile(ack=ack)['product_source'], 'unreviewed progress build source'
    for field in ('tree','source_manifest_sha256','lockfile_sha256'): assert component[field]==built[field]==qualification[field]
    manifest_bytes=raw(build.parent/'build/source-manifest.json'); assert sha(manifest_bytes)==built['source_manifest_sha256']
    manifest=loads(manifest_bytes); assert manifest['revision']==built['source'] and manifest['tree']==built['tree']
    for name,digest in component['executables_sha256'].items():
        assert Path(name).name==name and re.fullmatch('[a-z_]+',name) and sha(raw(build/name))==digest
    assert component['executables_sha256']['bigbrotr']==built['binary_sha256']
    assert {'bigbrotr','ingest_bootstrap_probe'}<=set(component['executables_sha256'])
    if ack:
        assert component['executables_sha256']==progress_replay.ACK_EXECUTABLES_SHA256
        assert set(component['executables_sha256'])=={'bigbrotr','ingest_bootstrap_probe','native_components',
            'sync_progress','fact_codec','evidence_intent','control_schema','migrate_entrypoint'}
    for name,digest in manifest['files_sha256'].items():
        if name.startswith(('apps/','crates/','migrations/')) or name in ('Cargo.toml','Cargo.lock','rust-toolchain.toml'):
            assert sha(raw(root/'source'/name))==digest
    for helper,relative in [('installed_family_client.py','experiments/ingest-succession/installed_family_client.py'),
                            ('installed_sync_client.py','experiments/ingest-bootstrap/installed_sync_client.py')]:
        assert raw(folder/helper)==raw(root/'source'/relative)
    if ack:
        assert raw(folder/'installed_progress_transport.py')==raw(root/'source/experiments/ingest-bootstrap/installed_progress_transport.py')
        provenance=read(folder/'progress-source.json')
        assert set(provenance)=={'source','revision','tree','build','files_sha256'}
        assert Path(provenance['source'])==root/'source' and Path(provenance['build'])==build
        assert (provenance['revision'],provenance['tree'])==(report['source'],report['tree'])
        actual=provenance['files_sha256'];compiled=manifest['files_sha256']
        assert actual and all(not Path(name).is_absolute() and '..' not in Path(name).parts for name in actual)
        for name,digest in actual.items(): assert sha(raw(root/'source'/name))==digest
        assert all(name.startswith(('docs/','experiments/')) or actual.get(name)==compiled.get(name)
            for name in set(actual)|set(compiled))
        assert actual['migrations/postgres/0040_producer_ack.sql']==progress_replay.ACK_MIGRATION_SHA256
    results=[]; prior_window=started
    for name,clone,case in zip(selected_clones,report['clones'],report['cases'],strict=True):
        migrated=clone_checked(folder,clone,origins[name],report,raw,read,progress=progress)
        expected=dict(name=clone['final_case'],clone=name,origin=name,operation='installed-live',predecessor=None,
            database=clone['database'],config=clone['config'],status='RUNNING',custody='OBSERVED_CLOSED_AND_JOINED')
        assert {k:case[k] for k in expected}==dict(expected,status='PASS')
        assert type(case['returncode']) is int and case['returncode']==0 and case['catalog_after']==clone['catalog_before']
        label=case['name']; item=lambda suffix: read(folder/(label+'-'+suffix+'.json'))
        context=item('verification')
        assert set(context)==CONTEXT_FIELDS | ({'progress_profile'} if progress else set()) | ({'ack_event_recipe','progress_gates'} if ack else set())
        assert context['case']==expected
        if progress:
            assert context['progress_profile'] == progress
        before,after,native,after_native,inputs,after_inputs=[item(s) for s in ('before','after','before-native','after-native','inputs','after-inputs')]
        for stage,observed_native in (('before',native),('after',after_native)):
            dumped=item(stage+'-dump-response')
            assert set(dumped)=={'returncode','stdout','stderr','seconds'}
            assert type(dumped['returncode']) is int and dumped['returncode']==0 and dumped['stderr']==''
            number(dumped['seconds'],0,120)
            assert loads(dumped['stdout'])==observed_native
        same(migrated,before); same(before,context['before']); same(after,context['after'])
        assert native==origins[name][1]==context['native_before'] and inputs==origins[name][2]==context['inputs_before']
        assert after_native==context['native_after'] and after_inputs==context['inputs_after']
        response=item('response'); assert response==context['response']
        identity,activated=process_checked(folder,case,response,raw,read)
        window=item('clock'); assert window==context['server_window']==case['server_window'] and set(window)=={'before_us','after_us'}
        number(window['before_us'],prior_window,finished,integer=True); number(window['after_us'],window['before_us'],finished,integer=True)
        prior_window=window['after_us']
        stages = list(STAGES)
        if progress:
            stages[2:2] = ['progress-mutation', 'progress-proof-only']
        snapshots=item('observations'); assert snapshots==context['snapshots'] and [s['stage'] for s in snapshots]==stages
        observed=window['before_us']
        directory=folder/name; previous_inputs=inputs
        for snapshot in snapshots:
            assert set(snapshot)=={'stage','server_us','observed_by_us','control','inputs'}
            number(snapshot['server_us'],observed,window['after_us'],integer=True)
            observed=number(snapshot['observed_by_us'],snapshot['server_us'],window['after_us'],integer=True)
            same(snapshot['control'],item(snapshot['stage']))
            assert all(snapshot['inputs'].get(k)==v for k,v in previous_inputs.items())
            assert all(after_inputs.get(k)==v for k,v in snapshot['inputs'].items())
            spool_checked(directory/'store/producer-inputs-v1',snapshot['inputs'],raw,complete=False)
            previous_inputs=snapshot['inputs']
        fixture=item('fixture'); assert fixture==context['fixture']
        journey_checked(directory,context,identity,window,raw,read,progress=progress is not None)
        if progress:
            transport = progress_transport_checked(directory, context['journey'], snapshots,
                window, raw, read,ack=ack)
            assert transport == case['progress_transport']
            if ack:
                assert context['ack_event_recipe']==raw(directory/'progress-ack-events.json').decode()
                assert read(directory/'progress-context.json')==dict(format='bigbrotr-ack-context/1',kind='successor')
                for gate,value in context['progress_gates'].items():
                    assert gate in ('progress-mutation-observation.json','progress-proof-only-observation.json','progress-release.json')
                    assert value==raw(directory/gate).decode()
        backend=item('backends'); assert backend==context['backend']
        assert item('control-backends')==backend['absence']; backend_checked(backend,case['database'],before,after)
        assert item('external')==dict(control=after,native=after_native,inputs=after_inputs,backend=backend,catalog=clone['catalog_before'])
        assert context['catalog_before']==context['catalog_after']==clone['catalog_before']
        assert item('custody')==dict(status='OBSERVED_CLOSED_AND_JOINED',database=case['database'],backend_absent=True,errors=[])
        assert not (folder/(label+'-external-unavailable.json')).exists()
        spool_checked(directory/'store/producer-inputs-v1',after_inputs,raw,complete=True)
        assert context['input_contents']=={n:raw(directory/'store/producer-inputs-v1'/n).hex() for n in after_inputs}
        same(after,read(folder/(name+'-final-sql.json')))
        if progress:
            result,controls=progress_replay.verify(context,ack=ack),progress_replay.controls(context,ack=ack)
            assert result['status']==('PASS_INSTALLED_PRODUCER_ACK' if ack else 'PASS_INSTALLED_PRODUCER_PROGRESS') and controls['status']=='PASS'
        else:
            result,controls=verify(context),negative_controls(context)
            assert result['status']==controls['status']=='PASS'
        assert case['oracle']==result and case['negative_controls']==controls
        results.append(dict(name=label,oracle=result,negative_controls=controls))
    if ack:
        from installed_progress_context_oracle import bind_ack_modules
        bind_ack_modules(provenance,raw)
    return dict(status=status+'_CAPTURE_REPLAY',source=report['source'],cases=results,bound_files=bound,
        original_preservation='Fresh original file census; SQL/catalog preservation is captured-run evidence only',
        scope=('Raw binding and complete window2 epoch3 ACK science; noGC, no full-window/loss/recovery qualification' if ack else
            'Raw binding and mandatory scientific replay of three installed lifetimes; no Archive, adoption, recovery, ACK/GC or full M1 claim'))


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root',type=Path)
    parser.add_argument('--output',type=Path)
    parser.add_argument('--progress',action='store_true')
    parser.add_argument('--producer-ack',action='store_true')
    args=parser.parse_args()
    assert not (args.progress and args.producer_ack)
    result=replay(args.root,progress='ack' if args.producer_ack else 'split' if args.progress else None)
    text=json.dumps(result,indent=2)+'\n'
    if args.output:
        with args.output.open('x') as stream: stream.write(text)
    else:
        print(text,end='')
