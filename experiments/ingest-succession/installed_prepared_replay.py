#!/usr/bin/env python3
"""Independent installed prepared recovery and raw-capture reader.

Old science is pinned to complete qualified endpoints. Setup transcripts are
checked separately and never relabelled successful recovery. Installed results
are interpreted from full external state, literal native deltas and real fresh
Control identity, not from the Rust report alone. No engine is opened here.
"""
import copy
import json
from pathlib import Path
import re
import stat
import sys
import tomllib

sys.dont_write_bytecode=True
import installed_prepared_model as model
import installed_prepared_setup as setup
from installed_prepared_client import argv
from installed_family_capture import loads,number,backend_checked,inventory_checked,physical,spool_checked

history=model.history
only,same,sha,encoded,instant=history.only,history.same,history.sha,history.encoded,history.instant
CLONES=('gap','open','closing','activated','legacy')
ORIGIN_FOR_CLONE=dict(gap='terminal',open='open',closing='closing',activated='activated',legacy='terminal')
SPECS=(('gap-prepare-loss','gap','prepare',None),('gap-retire','gap','empty','gap-prepare-loss'),
    ('gap-retry','gap','empty','gap-retire'),('gap-live','gap','live','gap-retry'),
    ('open-retire','open','empty',None),('open-retry','open','empty','open-retire'),('open-live','open','live','open-retry'),
    ('closing-empty-loss','closing','empty-setup',None),('closing-retire','closing','empty','closing-empty-loss'),
    ('closing-retry','closing','empty','closing-retire'),('activated-refused','activated','refused',None),
    ('legacy-refused','legacy','refused',None))
CASES=tuple(s[0] for s in SPECS)
STATUS='PASS_INSTALLED_PREPARED_SUCCESSOR_CYCLE'
ERRORS={'activated':'error: successor recovery unavailable: producer operation unavailable: Control(InvalidMetadata)\n',
        'legacy':'error: empty successor recovery requires a retained target epoch3 or later\n'}
CONTEXT_FIELDS={'case','before','after','native_before','native_after','inputs_before','inputs_after','input_contents',
    'snapshots','response','fixture','server_window','backend','catalog_before','catalog_after'}


def specification(case):
    name,clone,operation,previous=only([dict(name=n,clone=c,operation=o,previous=p) for n,c,o,p in SPECS],name=case['name']).values()
    assert case['clone']==clone and case['origin']==ORIGIN_FOR_CLONE[clone] and case['predecessor']==previous
    is_setup=operation in ('prepare','empty-setup')
    assert case['operation']==('empty' if operation=='empty-setup' else operation)
    assert re.fullmatch('[a-z0-9_]{1,63}',case['database']) and case['config']=='/lab/'+case['database']+'.toml'
    if is_setup:
        assert case['setup'] is True and case['metadata_bytes']==262144 and type(case['metadata_bytes']) is int
    else:assert not case.get('setup')
    return clone,operation,is_setup


def process_closed(response,case):
    """A reaped refusal is known custody, without inventing a success report."""
    assert set(response)=={'returncode','stdout','stderr','outcome','terminal'}
    refused=case['operation']=='refused';exitcode=4 if refused else 0
    assert response['outcome']=='PROCESS_EXITED' and type(response['returncode']) is int and response['returncode']==exitcode
    terminal=response['terminal']
    assert set(terminal)=={'pid','start_ticks','config','prefix','command','exit_code','reaped','forced','error','elapsed_seconds'}
    number(terminal['pid'],1,integer=True);number(terminal['start_ticks'],1,integer=True);number(terminal['elapsed_seconds'],0,60)
    assert terminal['config']==case['config'] and terminal['prefix']=='/lab/'+case['name']
    assert terminal['command']==argv(case['operation'],Path(case['config']))
    assert type(terminal['exit_code']) is int and terminal['exit_code']==exitcode
    assert terminal['reaped'] is True and terminal['forced'] is False and terminal['error'] is None
    if refused:
        assert response['stdout']=='' and response['stderr']==ERRORS[case['clone']]
        return []
    assert response['stderr']==''
    reports=[loads(line) for line in response['stdout'].splitlines()]
    if case['operation']=='live':
        activated,stopped=reports
        assert activated['format']=='bigbrotr-ingest-successor/1' and activated['state']=='activated'
        assert stopped=={k:activated[k] for k in ('format','dataset_id','owner_resource_id','producer_slot_id','producer_epoch')}|dict(
            state='stopped',native='closed',owner_release='committed',source_closure='not-performed',ready=False)
    else:
        report,=reports
        assert report['format']=='bigbrotr-ingest-recovery/1' and report['state']=='successor-empty-attempt-retired'
        assert report['native']=='closed' and report['owner_release']=='committed' and report['ready'] is False
    return reports


def verify(context,seed):
    assert set(context)==CONTEXT_FIELDS
    case=context['case'];clone,operation,is_setup=specification(case);assert not is_setup
    assert set(case)=={'name','clone','origin','operation','predecessor','database','config','status','custody'}
    assert case['status']=='RUNNING' and case['custody']=='OBSERVED_CLOSED_AND_JOINED'
    before,after,native,inputs=[context[k] for k in ('before','after','native_before','inputs_before')]
    window=context['server_window'];assert set(window)=={'before_us','after_us'}
    number(window['before_us'],1,integer=True);number(window['after_us'],window['before_us'],window['before_us']+120_000_000,integer=True)
    model.seed_checked(seed,case['origin'],window)
    assert set(before)==set(after)==set(seed[0]) and len(before)==75
    assert inputs==context['inputs_after']==seed[2] and set(context['input_contents'])==set(inputs)
    for name,value in context['input_contents'].items():
        content=bytes.fromhex(value);assert inputs[name]==dict(bytes=len(content),sha256=sha(content))
    if case['predecessor'] is None:same(before,seed[0]);assert native==seed[1]
    assert context['catalog_before']==context['catalog_after'] and context['fixture']['cleanup_errors']==[]
    reports=process_closed(context['response'],case)
    backend_checked(context['backend'],case['database'],before,after)
    m=model.Model(before,native,inputs,window)
    if operation!='live':
        assert context['snapshots']==[] and set(context['fixture'])=={'cleanup_errors'}
        m.control_end(after,released=operation!='refused')
        if operation=='refused':
            _,values,head,registered=model.coordinates(before,native)
            if clone=='activated':
                assert head==registered==4
                activation=only(before['ingest_activations'],target_epoch=4)
                assert activation['producer_epoch']==4
                assert only(before['producer_epochs'],producer_epoch=4)['reserved_through_request']==0
                assert values['receipts',(b'\x01\x31'+model.binary(m.slot)+model.be(4)).hex()]==(b'\x01\x31'+bytes(24)).hex()
            else:
                assert head==registered==2 and before['ingest_successions']==[]
                assert only(before['producer_epochs'],producer_epoch=2)['state']=='retired'
            same(m.pg,after)
        else:assert reports==[m.empty(after)]
        states=2
    else:
        assert clone in ('gap','open')
        snapshots=context['snapshots'];assert [s['stage'] for s in snapshots]==['activated','renewed','before-stop']
        at=window['before_us']
        for s in snapshots:
            assert set(s)=={'stage','server_us','observed_by_us','control','inputs'}
            number(s['server_us'],at,window['after_us'],integer=True)
            at=number(s['observed_by_us'],s['server_us'],window['after_us'],integer=True)
            assert s['inputs']==inputs
        m.window=dict(window,after_us=snapshots[0]['observed_by_us']);m.startup(snapshots[0]['control'],reports[0]);m.window=window
        head=4 if clone=='gap' else 5
        assert int(reports[0]['producer_epoch'])==head
        assert only(m.pg['ingest_successions'],target_epoch=head)['terminal_kind']==2
        initial=only(snapshots[0]['control']['owner_epochs'],role='ingest',epoch=head);previous=snapshots[0]['control']
        for s in snapshots[1:]:
            model.cycle.leases(m.pg,s['control'],previous,window,m.control['epoch'],head);same(m.pg,s['control']);previous=s['control']
        model.cycle.leases(m.pg,after,previous,window,m.control['epoch'],head,stopped=True);same(m.pg,after)
        renewal=context['fixture']['renewal']
        assert renewal==dict(initial_deadline_us=int(reports[0]['lease_expires_at_us']),observed_at_us=renewal['observed_at_us'],initial_target=initial,current=snapshots[1])
        number(renewal['observed_at_us'],max(int(reports[0]['lease_expires_at_us']),int(reports[0]['prepared_at_us'])+6_000_000)+1,snapshots[1]['server_us'],integer=True)
        current=only(snapshots[1]['control']['owner_epochs'],role='ingest',epoch=head)
        assert current['lease_expires_at_us']>initial['lease_expires_at_us'] and current['lease_expires_at_us']>snapshots[1]['observed_by_us']
        signal=context['fixture']['signal'];assert set(signal)=={'kind','identity','before_us','after_us'} and signal['kind']=='SIGTERM'
        identity={k:context['response']['terminal'][k] for k in ('pid','start_ticks','command','config','prefix')}
        assert signal['identity']==identity
        number(signal['before_us'],snapshots[-1]['observed_by_us'],window['after_us'],integer=True)
        number(signal['after_us'],signal['before_us'],window['after_us'],integer=True)
        assert set(context['fixture'])=={'cleanup_errors','renewal','signal'}
        states=5
    assert m.native==context['native_after'];same(m.pg,after)
    return dict(status='PASS_EXPECTED_REFUSAL' if operation=='refused' else 'PASS',case=case['name'],operation=operation,
        states=states,tables=75,native_rows=len(m.native['rows']),native_sequence=m.native['sequence'],input_files=len(inputs))


def negative_controls(context,seed):
    mutations=[]
    def add(name,change):
        value=copy.deepcopy(context);change(value);mutations.append((name,value))
    add('unknown-process',lambda c:c['response'].update(outcome='OUTCOME_UNKNOWN'))
    add('not-reaped',lambda c:c['response']['terminal'].update(reaped=False))
    add('forced-exit',lambda c:c['response']['terminal'].update(forced=True))
    add('wrong-command',lambda c:c['response']['terminal']['command'].append('--sync'))
    add('live-backend',lambda c:c['backend'].update(all=[{'pid':1}]))
    add('native-sequence',lambda c:c['native_after'].update(sequence=c['native_after']['sequence']+1))
    add('input-loss',lambda c:c['inputs_after'].pop(next(iter(c['inputs_after']))))
    add('old-copy-xmin',lambda c:c['after']['producer_outcomes'][0].update(xmin='4294967000'))
    add('catalog-change',lambda c:c.update(catalog_after=c['catalog_after']+'\nchanged'))
    if context['case']['operation']=='refused':
        add('wrong-refusal',lambda c:c['response'].update(stderr='error: timeout\n'))
        add('refusal-as-success',lambda c:c['response'].update(returncode=0))
    elif context['case']['operation']=='empty':
        add('missing-report',lambda c:c['response'].update(stdout=''))
        def altered(c,key):
            report=loads(c['response']['stdout']);report[key]=str(int(report[key])+1);c['response']['stdout']=json.dumps(report)+'\n'
        add('wrong-proof-through',lambda c:altered(c,'verified_through'))
        add('false-issued-request',lambda c:altered(c,'issued_through'))
        add('wrong-terminal-issuer',lambda c:c['after']['ingest_terminal_transactions'][-1].update(control_epoch=1))
    else:
        add('missing-renewal',lambda c:c['snapshots'].pop(1))
        add('signal-before-renewal',lambda c:c['fixture']['signal'].update(before_us=c['server_window']['before_us']))
    rejected=[]
    for name,value in mutations:
        try:verify(value,seed)
        except (AssertionError,KeyError,ValueError,TypeError,StopIteration):rejected.append(name)
        else:raise AssertionError('accepted installed counterexample: '+name)
    return dict(status='PASS',count=len(rejected),rejected=rejected)


def origin_checked(root, folder, name, declaration, raw, read):
    pin = model.PINS[name]
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
    if pin.get('origin_format')=='legacy-installed':
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
        assert stopped['state'] == 'activated-initial-retired'
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
    assert declaration['preserved'] == read(folder/(name+'-preserved.json')) == dict(rows_unchanged=True,catalog_unchanged=True,native_unchanged=True,inputs_unchanged=True,physical_unchanged=True)
    assert all(v is True for v in declaration['preserved'].values())
    return rows,native,inputs



def replay(root):
    """Rebind every context to actual files, final stores and exact predecessor."""
    root=Path(root).resolve(strict=True); folder=root/'installed-prepared'; bound={}
    def raw(path):
        path=Path(path); assert stat.S_ISREG(path.lstat().st_mode)
        value=path.read_bytes(); bound[str(path)]=sha(value); return value
    def read(path): return loads(raw(path))
    report=read(folder/'result.json')
    assert report['status']==STATUS and report['label']=='org.bigbrotr.installed-prepared'
    assert re.fullmatch('[0-9a-f]{40}',report['source']) and re.fullmatch('[0-9a-f]{40}',report['tree'])
    assert re.fullmatch('[0-9a-f]{32}',report['owner']) and re.fullmatch('[0-9a-f]{64}',report['client'])
    assert all(report[k]==[] for k in ('cleanup_errors','missing_owned','unrelated_changed','unexpected_containers'))
    assert [c['name'] for c in report['clones']]==list(CLONES) and [c['name'] for c in report['cases']]==list(CASES)
    assert report['databases']==[c['database'] for c in report['clones']] and len(set(report['databases']))==5
    assert not set(report['databases']) & set(report['database_names_before'])
    assert set(report['database_names_after'])==set(report['database_names_before'])|set(report['databases'])
    started,finished=instant(report['started_at']),instant(report['finished_at']); assert started<=finished
    assert set(report['origins'])==set(model.PINS)
    origins={name:origin_checked(root,folder,name,report['origins'][name],raw,read) for name in model.PINS}
    terminal=read(folder/'terminal-containers.json')
    inventory_checked(read(folder/'preflight-containers.json'),terminal,report['client'],report['postgres'])
    assert report['postgres']=='7177550705c49b14237e20790af411af58f62f22d110cd0dfa1afddaf78861ec'
    assert report['postgres_owner']=='5e69b0f060b1607c25230d1dbd3e5608'
    for filename,ident,exitcode in [('client-terminal.json',report['client'],143),('postgres-terminal.json',report['postgres'],0)]:
        value=read(folder/filename); assert value==terminal[ident] and value['Id']==ident
        state=value['State']; assert state['Status']=='exited' and state['Pid']==0
        assert all(state[k] is False for k in ('Running','Paused','Restarting','OOMKilled','Dead')) and not state['Error']
        assert type(state['ExitCode']) is int and state['ExitCode']==exitcode
    client=terminal[report['client']]
    mount=only(client['Mounts'],Destination='/lab'); assert mount['RW'] is True and mount['Source']==str(folder)
    mount=only(client['Mounts'],Destination='/build'); assert mount['RW'] is False
    build=Path(mount['Source']); assert build.is_absolute()
    built,component=read(build/'build-result.json'),read(build/'component-result.json')
    qualification=read(build.parent/'qualification.json')
    assert qualification['status'] in ('PASS_SUCCESSION_TRANSITION_COMPONENTS','PASS_SUCCESSION_HISTORY_PROBE_BUILD',
        'PASS_SUCCESSION_TRANSITION_PROBE_BUILD','PASS_SUCCESSION_RECOVERY_COMPONENTS','PASS_SUCCESSION_INSTALLED_COMPONENTS')
    assert report['build']==built and report['component']==component and component['status']=='PASS'
    assert component['source']==built['source']==qualification['source']
    for field in ('tree','source_manifest_sha256','lockfile_sha256'): assert component[field]==built[field]==qualification[field]
    data=raw(build.parent/'build/source-manifest.json'); assert sha(data)==built['source_manifest_sha256']
    manifest=loads(data); assert manifest['revision']==built['source'] and manifest['tree']==built['tree']
    for name,digest in component['executables_sha256'].items():
        assert Path(name).name==name and re.fullmatch('[a-z_]+',name) and sha(raw(build/name))==digest
    assert component['executables_sha256']['bigbrotr']==built['binary_sha256']
    assert {'bigbrotr','ingest_bootstrap_probe'}<=set(component['executables_sha256'])
    for name,digest in manifest['files_sha256'].items():
        if name.startswith(('apps/','crates/','migrations/')) or name in ('Cargo.toml','Cargo.lock','rust-toolchain.toml'):
            assert sha(raw(root/'source'/name))==digest
    assert raw(folder/'installed_prepared_client.py')==raw(root/'source/experiments/ingest-succession/installed_prepared_client.py')
    assert sha(raw(folder/'installed-prepared-proxy.py'))==setup.PROXY_SHA256
    assert raw(folder/'installed-prepared-proxy.py')==raw(root/'source/experiments/control-migrations/fault_proxy.py')
    results=[];previous_time=started
    for clone in report['clones']:
        name=clone['name'];origin=clone['origin'];seed=origins[origin]
        assert origin==ORIGIN_FOR_CLONE[name] and clone['directory']==name and clone['status']=='PASS'
        assert clone['database']=='bb_installed_prepared_'+report['source'][:7]+'_'+name
        assert clone['config']=='/lab/'+clone['database']+'.toml' and clone['final_backends']==[]
        specs=[s for s in SPECS if s[1]==name];assert clone['final_case']==specs[-1][0]
        same(seed[0],read(folder/(name+'-copy-before.json')))
        migrated=read(folder/(name+'-migrated.json'));seed=(migrated,*seed[1:])
        first_window=read(folder/(specs[0][0]+'-clock.json'))
        model.seed_checked(seed,origin,first_window)
        floor=37 if origin=='terminal' else 38
        steps=[dict(name=f'{v:04d}_{history.MIGRATIONS[v][0]}.sql',sha256=history.MIGRATIONS[v][1],version=str(v)) for v in range(floor+1,39)]
        assert [a['action'] for a in clone['migrations']]==['plan','apply','apply']
        for index,a in enumerate(clone['migrations']):
            assert set(a)=={'action','exit_code','stdout','stderr'} and type(a['exit_code']) is int and a['exit_code']==0 and a['stderr']==''
            assert loads(a['stdout'])==dict(format='bigbrotr-control-migration/1',scope='postgres-control',target_version='38',
                source_version=str(floor) if index<2 else '38',current_version=str(floor) if index==0 else '38',
                status='planned' if index==0 else ('applied' if index==1 and steps else 'current'),applied_count=str(len(steps)) if index==1 else '0',
                steps=steps if index<2 else [],dataset_initialization='not-performed',release_upgrade_qualification='not-performed')
        statements=[raw(root/'source/experiments/ingest-succession/schema_grants.sql').decode(),
            'GRANT SELECT ON control.initial_ingest_activations TO bigbrotr_control;',
            'GRANT UPDATE ON control.producer_epochs TO bigbrotr_control;',
            'GRANT INSERT ON control.producer_outcomes TO bigbrotr_control;']
        assert sha(statements[0].encode())=='36a16062c994adfe1ba7ace4c5e56d2ed942277b0e227ddd048e320939f0e588'
        assert clone['grants']==[dict(statement=s,stdout='') for s in statements]
        assert clone['catalog_before']==clone['catalog_after']==clone['final_catalog']
        expected_catalog='6aad32f83a7c62b071ec37852994df068afdcb7c334635b94aacf1d225a5718e' if origin=='terminal' else model.PINS[origin]['catalog_sha256']
        assert sha(clone['catalog_before'].encode())==expected_catalog
        config=tomllib.loads(raw(folder/Path(clone['config']).name).decode()); ds=only(seed[0]['datasets'])
        assert {k:config['postgres']['control'][k] for k in ('database','endpoint','user','password_ref')}==dict(
            database=clone['database'],endpoint='127.0.0.1:5432',user='bigbrotr_control',password_ref='file:/run/secrets/application')
        assert config['postgres']['control']['tls']==dict(local_boundary='loopback',mode='local-plaintext')
        assert config['storage']['ingest_root']=='/lab/'+name+'/store'
        assert config['topology']['dataset_id']==ds['dataset_id'][2:] and config['topology']['instance_id']==ds['instance_id'][2:]
        assert config['topology']['local_roles']==['control','ingest','sync','archive']
        assert config['runtime']['processes']['application']==dict(memory_max_bytes='1536MiB',baseline_bytes='128MiB',payload_bytes='1GiB',native_bytes='256MiB',completion_reserve_bytes='32MiB')
        assert {k:config['runtime'][k] for k in ('lease_ttl','heartbeat_interval','renewal_timeout','authorization_ttl','graceful_shutdown_timeout')}==dict(
            lease_ttl='6s',heartbeat_interval='1s',renewal_timeout='1s',authorization_ttl='5s',graceful_shutdown_timeout='2s')
        assert config['administration']['enabled'] is False and config['administration']['listen']['port']==18081
        assert config['sync']['enabled'] is False and config['ingest']['producer_outstanding_requests']==65
        assert config['network']['dns']['resolver']=='127.0.0.1:9' and config['network']['tls']['extra_roots']==[]
        assert config['network']['private']['rules']==[dict(endpoint='ws://127.0.0.1:7447/installed-successor-fixture',cidr='127.0.0.1/32',port=7447)]
        for filename in (name+'-config-validation.json',clone['database']+'-installed-config-validation.json'):
            a=read(folder/filename); assert type(a['returncode']) is int and a['returncode']==0 and a['stderr']==''; loads(a['stdout'])
        expected=seed
        for label,_,op,previous in specs:
            case=only(report['cases'],name=label);_,_,is_setup=specification(case)
            assert case['custody']=='OBSERVED_CLOSED_AND_JOINED' and case['catalog_after']==clone['catalog_before']
            assert case['database']==clone['database'] and case['config']==clone['config']
            item=lambda suffix:read(folder/(label+'-'+suffix+'.json'))
            before,after,native,after_native,inputs,after_inputs=[item(s) for s in ('before','after','before-native','after-native','inputs','after-inputs')]
            same(expected[0],before);assert native==expected[1] and inputs==expected[2]
            for stage,value in [('before',native),('after',after_native)]:
                a=item(stage+'-dump-response');assert set(a)=={'returncode','stdout','stderr','seconds'}
                assert type(a['returncode']) is int and a['returncode']==0 and a['stderr']==''
                number(a['seconds'],0,120);assert loads(a['stdout'])==value
            response=item('response');window=item('clock')
            assert window==case['server_window'];number(window['before_us'],previous_time,finished,integer=True)
            previous_time=number(window['after_us'],window['before_us'],finished,integer=True)
            assert type(case['returncode']) is int and case['returncode']==response['returncode']
            if is_setup:
                assert case['status']=='PASS_SETUP_COMMIT_UNKNOWN' and response['returncode']==0 and response['stderr']==''
                assert not any(k in response for k in ('error','orchestration_error','response_error','clock_error'))
                assert response.get('outcome','PROCESS_EXITED')=='PROCESS_EXITED'
                number(response['seconds'],0,120);assert case['response_bytes']==len(response['stdout'].encode())
                fault=item('fault');assert fault==case['fault'] and item('wire')==fault['wire']
                events=[loads(line) for line in response['stdout'].splitlines()]
                result=setup.verify(events,before,native,inputs,case,window,fault=fault)
                controls=setup.negative_controls(events,before,native,inputs,case,window,fault=fault)
                final=only(events,event='lineage-history-state',stage='final')
                same(after,{k:loads(v) for k,v in final['state']['control_tables']})
                assert after_native==dict(event='dump-state',rows=final['rows'],sequence=final['state']['sequence'])
                assert after_inputs==inputs and item('backends')==[]
                # Actual finite telemetry already binds both joined drivers and
                # the exact backend; retain and bind the independent absence file.
                backend=only(events,event='lineage-history-backend',present=True)
                absence=item('control-backends')
                backend_checked(dict(absence=absence,exact=[dict(pid=backend['pid'],backend_start=backend['backend_start'],
                    database=case['database'],control_epoch=backend['control_epoch'],present=False)],all=[]),case['database'],before,after)
            else:
                context=item('verification')
                expected_case={k:case[k] for k in ('name','clone','origin','operation','predecessor','database','config','custody')}|dict(status='RUNNING')
                assert context['case']==expected_case
                same(before,context['before']);same(after,context['after'])
                assert native==context['native_before'] and after_native==context['native_after']
                assert inputs==context['inputs_before'] and after_inputs==context['inputs_after']
                assert response==context['response'];process_closed(response,case)
                identity=read(folder/(label+'.pid.json'));ended=read(folder/(label+'.terminal.json'))
                assert set(identity)=={'pid','start_ticks','command','config','prefix'}
                assert all(ended[k]==v for k,v in identity.items()) and ended==response['terminal']
                for stream in ('stdout','stderr'):
                    data=raw(folder/(label+'.'+stream));assert len(data)<=65536 and data.decode()==response[stream]
                assert window==context['server_window'] and item('fixture')==context['fixture'] and item('observations')==context['snapshots']
                for s in context['snapshots']:same(s['control'],item(s['stage']))
                if op=='live':assert item('signal')==context['fixture']['signal']
                backend=item('backends');assert backend==context['backend'] and item('control-backends')==backend['absence']
                backend_checked(backend,case['database'],before,after)
                assert item('external')==dict(control=after,native=after_native,inputs=after_inputs,backend=backend,catalog=clone['catalog_before'])
                assert context['catalog_before']==context['catalog_after']==clone['catalog_before']
                assert set(item('provisional-sql'))==set(before) and item('provisional-inputs')==inputs
                assert context['input_contents']=={n:raw(folder/name/'store/producer-inputs-v1'/n).hex() for n in after_inputs}
                result,controls=verify(context,seed),negative_controls(context,seed)
                assert case['status']==result['status']
            assert item('custody')==dict(status='OBSERVED_CLOSED_AND_JOINED',database=case['database'],backend_absent=True,errors=[])
            assert not (folder/(label+'-external-unavailable.json')).exists()
            spool_checked(folder/name/'store/producer-inputs-v1',after_inputs,raw,complete=True)
            assert case['oracle']==result and case['negative_controls']==controls
            results.append(dict(name=label,oracle=result,negative_controls=controls));expected=(after,after_native,after_inputs)
        same(expected[0],read(folder/(name+'-final-sql.json')))
    return dict(status=STATUS+'_CAPTURE_REPLAY',source=report['source'],cases=results,bound_files=bound,
        original_preservation='Original physical/input bytes reread; original SQL/catalog preservation is captured-run evidence',
        scope='Two genuine setup faults, empty gap/OPEN/CLOSING retirement/retries, Serve4/5, activated/legacy refusals; no writer adoption or new job')


if __name__=='__main__':
    import argparse
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('root',type=Path);parser.add_argument('--output',type=Path)
    args=parser.parse_args();result=replay(args.root);rendered=json.dumps(result,indent=2)+'\n'
    if args.output:
        with args.output.open('x') as stream:stream.write(rendered)
    else:print(rendered,end='')
