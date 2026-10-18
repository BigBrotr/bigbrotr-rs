#!/usr/bin/env python3
"""Independent full-state installed-cycle reducer and offline raw-file binder.

All original SQL/native/input bytes are pinned. Model reuse is limited to the
qualified portable startup encoding and six-second lease rules; expected new
closures, whole-intent gaps and terminal attribution are derived here. No SQL,
subprocess, network or native engine operation is performed by this reader.
"""
import copy
import json
from pathlib import Path
import re
import stat
import sys
import tomllib

sys.dont_write_bytecode = True
import installed_cycle_model as model
from installed_family_capture import loads, number, backend_checked, inventory_checked, physical, spool_checked
from installed_cycle_client import argv

history = model.history
only, same, sha, encoded, instant = history.only, history.same, history.sha, history.encoded, history.instant
CLONES = ('activated', 'empty', 'later')
STEPS = ('recover', 'recover-retry', 'source', 'source-retry', 'abandon', 'abandon-retry', 'retire', 'retire-retry', 'live')
CASES = tuple(n + '-' + step for n in CLONES for step in STEPS)
STATUS = 'PASS_INSTALLED_SUCCESSOR_CYCLE'
STATES = dict(recover='successor-positive-outcomes-resolved', source='successor-source-closed',
    abandon='successor-acquisition-abandoned', retire='successor-retired')
CONTEXT_FIELDS = {'case','before','after','native_before','native_after','inputs_before','inputs_after',
    'input_contents','snapshots','response','fixture','server_window','backend','catalog_before','catalog_after'}


def seed_checked(seed, origin):
    assert len(seed) == 3 and len(seed[0]) == 75
    assert [sha(encoded(history.normalized(seed[0]))),sha(encoded(seed[1])),sha(encoded(seed[2]))] == model.SEED_HASHES[origin]
    return model.science(seed)


def specification(case):
    origin = case['origin']; assert origin in CLONES and case['clone'] == origin
    assert case['name'].startswith(origin + '-')
    step = case['name'][len(origin)+1:]; assert step in STEPS
    index = STEPS.index(step)
    assert case['predecessor'] == (origin + '-' + STEPS[index-1] if index else None)
    operation = step.removesuffix('-retry')
    assert case['operation'] == operation
    assert re.fullmatch('[a-z0-9_]{1,63}', case['database'])
    assert case['config'] == '/lab/' + case['database'] + '.toml'
    return origin, step, operation


def process_closed(response, case):
    assert set(response) == {'returncode','stdout','stderr','outcome','terminal'}
    assert response['outcome'] == 'PROCESS_EXITED' and type(response['returncode']) is int and response['returncode'] == 0
    assert response['stderr'] == ''
    terminal = response['terminal']
    assert set(terminal) == {'pid','start_ticks','config','prefix','command','exit_code','reaped','forced','error','elapsed_seconds'}
    number(terminal['pid'],1,integer=True); number(terminal['start_ticks'],1,integer=True)
    number(terminal['elapsed_seconds'],0,60)
    assert terminal['config'] == case['config'] and terminal['prefix'] == '/lab/' + case['name']
    assert terminal['command'] == argv(case['operation'], Path(case['config']))
    assert type(terminal['exit_code']) is int and terminal['exit_code'] == 0
    assert terminal['reaped'] is True and terminal['forced'] is False and terminal['error'] is None
    reports = [loads(line) for line in response['stdout'].splitlines()]
    if case['operation'] == 'live':
        activated, stopped = reports
        assert activated['format'] == 'bigbrotr-ingest-successor/1' and activated['state'] == 'activated'
        assert stopped == {k:activated[k] for k in ('format','dataset_id','owner_resource_id','producer_slot_id','producer_epoch')} | dict(
            state='stopped',native='closed',owner_release='committed',source_closure='not-performed',ready=False)
    else:
        report, = reports
        assert report['format'] == 'bigbrotr-ingest-recovery/1' and report['state'] == STATES[case['operation']]
        assert report['native'] == 'closed' and report['owner_release'] == 'committed' and report['ready'] is False
    return reports


def verify(context, seed):
    assert set(context) == CONTEXT_FIELDS
    case = context['case']; origin, step, operation = specification(case)
    assert set(case)=={'name','clone','origin','operation','predecessor','database','config','status','custody'}
    assert case['status']=='RUNNING'
    known = seed_checked(seed, origin)
    before, after = context['before'], context['after']
    native, inputs = context['native_before'], context['inputs_before']
    window = context['server_window']
    assert set(window) == {'before_us','after_us'}
    number(window['before_us'],1,integer=True); number(window['after_us'],window['before_us'],window['before_us']+120_000_000,integer=True)
    assert set(before) == set(after) == set(seed[0])
    assert inputs == context['inputs_after'] == seed[2]
    assert set(context['input_contents']) == set(inputs)
    for name, value in context['input_contents'].items():
        raw = bytes.fromhex(value)
        assert inputs[name] == dict(bytes=len(raw),sha256=sha(raw))
    assert context['catalog_before'] == context['catalog_after']
    assert context['fixture']['cleanup_errors'] == []
    assert case['custody'] == 'OBSERVED_CLOSED_AND_JOINED'
    reports = process_closed(context['response'],case)
    backend_checked(context['backend'],case['database'],before,after)
    if step == 'recover':
        same(seed[0],before); assert native == seed[1]
    # Exact native preimage for all eight finite modes; only first close changes it.
    expected_native = copy.deepcopy(seed[1])
    if step != 'recover':
        values = model.native_map(expected_native)
        key = ('receipts',(b'\x01\x31'+model.binary(only(seed[0]['initial_ingest_successions'])['producer_slot_id'])+model.be(known['head'])).hex())
        assert values[key] == (b'\x01\x31'+model.be(known['issued'])+bytes(16)).hex()
        values[key] = (b'\x02\x31'+model.be(known['issued'])+bytes(16)+b'\x01').hex()
        expected_native.update(rows=model.native_rows(values),sequence=expected_native['sequence']+1)
    assert native == expected_native
    m = model.Model(before,native,inputs,window)
    if operation != 'live':
        assert context['snapshots'] == [] and set(context['fixture']) == {'cleanup_errors'}
        m.completed_control(after)
        m.mutation(after,operation,known)
        report = dict(format='bigbrotr-ingest-recovery/1',state=STATES[operation],dataset_id=m.ds['dataset_id'][2:],
            owner_resource_id=m.resource[2:],producer_slot_id=m.slot[2:],producer_epoch=str(known['head']),
            issued_through=str(known['issued']),reserved_through=str(known['issued']),resolved_requests=str(known['issued']),
            verified_through=str(known['head']),native='closed',owner_release='committed',ready=False,
            source_closure='not-performed' if operation=='recover' else dict(last_complete_sequence=str(known['cut']),sha256=known['digest']))
        if operation=='abandon':
            report['acquisition_abandonment']=dict(jobs='1',reason='ingest_successor_abandoned',coverage='unknown')
        if operation=='retire':
            report['ingest_retirement']=dict(allocation_domains='9',acquisition_gaps='1',owner_release='committed',native_acknowledged='0',native_retired='0')
        assert reports == [report]
        states = 2
    else:
        snapshots = context['snapshots']
        assert [s['stage'] for s in snapshots] == ['activated','renewed','before-stop']
        at = window['before_us']
        for s in snapshots:
            assert set(s)=={'stage','server_us','observed_by_us','control','inputs'}
            number(s['server_us'],at,window['after_us'],integer=True)
            at=number(s['observed_by_us'],s['server_us'],window['after_us'],integer=True)
            assert s['inputs']==inputs
        m.window=dict(window,after_us=snapshots[0]['observed_by_us'])
        m.startup(snapshots[0]['control'],reports[0])
        m.window=window
        head = known['head']+1
        assert head == (5 if origin=='later' else 4)
        initial = only(snapshots[0]['control']['owner_epochs'],role='ingest',epoch=head)
        previous = snapshots[0]['control']
        for s in snapshots[1:]:
            model.leases(m.pg,s['control'],previous,window,m.control['epoch'],head)
            same(m.pg,s['control']); previous=s['control']
        model.leases(m.pg,after,previous,window,m.control['epoch'],head,stopped=True)
        same(m.pg,after)
        renewal = context['fixture']['renewal']
        assert renewal == dict(initial_deadline_us=int(reports[0]['lease_expires_at_us']),
            observed_at_us=renewal['observed_at_us'],initial_target=initial,current=snapshots[1])
        number(renewal['observed_at_us'],max(int(reports[0]['lease_expires_at_us']),int(reports[0]['prepared_at_us'])+6_000_000)+1,
               snapshots[1]['server_us'],integer=True)
        current=only(snapshots[1]['control']['owner_epochs'],role='ingest',epoch=head)
        assert current['lease_expires_at_us'] > initial['lease_expires_at_us']
        assert current['lease_expires_at_us'] > snapshots[1]['observed_by_us']
        signal=context['fixture']['signal']; assert set(signal)=={'kind','identity','before_us','after_us'} and signal['kind']=='SIGTERM'
        identity={k:context['response']['terminal'][k] for k in ('pid','start_ticks','command','config','prefix')}
        assert signal['identity']==identity
        number(signal['before_us'],snapshots[-1]['observed_by_us'],window['after_us'],integer=True)
        number(signal['after_us'],signal['before_us'],window['after_us'],integer=True)
        assert set(context['fixture'])=={'cleanup_errors','renewal','signal'}
        states=5
    assert m.native == context['native_after']
    # This whole-state comparison also preserves original e1..e4, sessions and
    # the expired/unclaimed a7 row. There is no new command or job in Serve.
    same(m.pg,after)
    return dict(status='PASS',case=case['name'],operation=operation,producer_epoch=str(known['head']+(operation=='live')),
        states=states,tables=75,source_digest=known['digest'],native_rows=len(m.native['rows']),
        native_sequence=m.native['sequence'],input_files=len(inputs))


def negative_controls(context, seed):
    mutations=[]
    def add(name, change):
        value=copy.deepcopy(context); change(value); mutations.append((name,value))
    add('unknown-process',lambda c:c['response'].update(outcome='OUTCOME_UNKNOWN'))
    add('not-joined',lambda c:c['response']['terminal'].update(reaped=False))
    add('wrong-command',lambda c:c['response']['terminal']['command'].append('--sync'))
    add('live-backend',lambda c:c['backend'].update(all=[{'pid':1}]))
    add('native-sequence',lambda c:c['native_after'].update(sequence=c['native_after']['sequence']+1))
    add('input-loss',lambda c:c['inputs_after'].pop(next(iter(c['inputs_after']))))
    add('old-copy-xmin',lambda c:c['after']['producer_outcomes'][0].update(xmin='4294967000'))
    add('old-command-reuse',lambda c:c['after']['management_commands'][0].update(xmin='4294967001'))
    add('missing-report',lambda c:c['response'].update(stdout=''))
    add('catalog-change',lambda c:c.update(catalog_after=c['catalog_after']+'\nchanged'))
    if context['case']['operation']=='live':
        add('missing-renewal',lambda c:c['snapshots'].pop(1))
        add('signal-before-renewal',lambda c:c['fixture']['signal'].update(before_us=c['server_window']['before_us']))
    elif context['case']['operation']=='abandon':
        add('false-coverage',lambda c:c['after']['acquisition_gaps'][-1].update(reason='complete'))
    elif context['case']['operation']=='retire':
        add('wrong-issuer',lambda c:c['after']['ingest_terminal_transactions'][-1].update(control_epoch=1))
    rejected=[]
    for name,value in mutations:
        try: verify(value,seed)
        except (AssertionError,KeyError,ValueError,TypeError,StopIteration): rejected.append(name)
        else: raise AssertionError('accepted negative control: '+name)
    return dict(status='PASS',count=len(rejected),rejected=rejected)


def replay(root):
    """Rebind every context to actual files, final stores and exact predecessor."""
    root=Path(root).resolve(strict=True); folder=root/'installed-cycle'; bound={}
    def raw(path):
        path=Path(path); assert stat.S_ISREG(path.lstat().st_mode)
        value=path.read_bytes(); bound[str(path)]=sha(value); return value
    def read(path): return loads(raw(path))
    report=read(folder/'result.json')
    assert report['status']==STATUS and report['label']=='org.bigbrotr.installed-successor-cycle'
    assert re.fullmatch('[0-9a-f]{40}',report['source']) and re.fullmatch('[0-9a-f]{40}',report['tree'])
    assert re.fullmatch('[0-9a-f]{32}',report['owner']) and re.fullmatch('[0-9a-f]{64}',report['client'])
    assert all(report[k]==[] for k in ('cleanup_errors','missing_owned','unrelated_changed','unexpected_containers'))
    assert [c['name'] for c in report['clones']]==list(CLONES) and [c['name'] for c in report['cases']]==list(CASES)
    assert report['databases']==[c['database'] for c in report['clones']] and len(set(report['databases']))==3
    assert not set(report['databases']) & set(report['database_names_before'])
    assert set(report['database_names_after'])==set(report['database_names_before'])|set(report['databases'])
    started,finished=instant(report['started_at']),instant(report['finished_at']); assert started<=finished
    assert set(report['origins'])==set(CLONES)
    origins={}
    preserved=dict(rows_unchanged=True,catalog_unchanged=True,native_unchanged=True,inputs_unchanged=True,physical_unchanged=True)
    for name in CLONES:
        pin=model.PINS[name]; declaration=report['origins'][name]
        assert set(declaration)==set(pin)|{'pins','files','inputs','native','previous_client','rows','catalog','preserved'}
        assert declaration['pins']==pin and all(declaration[k]==v for k,v in pin.items() if k!='store')
        basis=root.parent/pin['run']/pin['subdirectory']; store=basis/pin['store']
        assert declaration['store']==str(store)
        objects=[]
        for field,digest in [('snapshot','snapshot_sha256'),('native_file','native_sha256'),('inputs_file','inputs_sha256')]:
            data=raw(basis/pin[field]); assert sha(data)==pin[digest]; objects.append(loads(data))
        seed_checked(objects,name); origins[name]=tuple(objects)
        data=raw(basis/'result.json'); assert sha(data)==pin['result_sha256']; previous=loads(data)
        assert all(previous[k]==pin[k] for k in ('source','owner','label','status'))
        assert previous['client']==pin['client_id']==declaration['previous_client']
        assert all(previous[k]==[] for k in ('cleanup_errors','missing_owned','unrelated_changed','unexpected_containers'))
        original=only(previous['clones'],name=name); final=only(previous['cases'],name=pin['final_case'])
        assert original['database']==pin['database'] and original['directory']+'/store'==pin['store']
        assert original['final_case']==pin['final_case'] and original['status']==final['status']=='PASS'
        assert original['final_backends']==[] and final['custody']=='OBSERVED_CLOSED_AND_JOINED'
        assert type(final['returncode']) is int and final['returncode']==0
        assert sha(original['final_catalog'].encode())==pin['catalog_sha256']
        same(objects[0],declaration['rows']); same(objects[0],read(folder/(name+'-original.json')))
        assert objects[1]==declaration['native'] and objects[2]==declaration['inputs']
        assert sha(declaration['catalog'].encode())==pin['catalog_sha256']
        assert physical(store,raw)==declaration['files']
        spool_checked(store/'producer-inputs-v1',objects[2],raw,complete=True)
        assert declaration['preserved']==read(folder/(name+'-preserved.json'))==preserved
        assert all(v is True for v in declaration['preserved'].values())
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
    assert raw(folder/'installed_cycle_client.py')==raw(root/'source/experiments/ingest-succession/installed_cycle_client.py')
    results=[]; previous_time=started
    for clone in report['clones']:
        name=clone['name']; seed=origins[name]
        assert clone['origin']==clone['directory']==name and clone['status']=='PASS'
        assert clone['database']=='bb_installed_cycle_'+report['source'][:7]+'_'+name
        assert clone['config']=='/lab/'+clone['database']+'.toml'
        assert clone['final_case']==name+'-live' and clone['final_backends']==[]
        same(seed[0],read(folder/(name+'-copy-before.json')))
        same(seed[0],read(folder/(name+'-migrated.json')))
        assert [a['action'] for a in clone['migrations']]==['plan','apply','apply']
        for index,a in enumerate(clone['migrations']):
            assert set(a)=={'action','exit_code','stdout','stderr'} and type(a['exit_code']) is int
            assert a['exit_code']==0 and a['stderr']==''
            assert loads(a['stdout'])==dict(format='bigbrotr-control-migration/1',scope='postgres-control',target_version='38',
                source_version='38',current_version='38',status='planned' if index==0 else 'current',applied_count='0',
                steps=[],dataset_initialization='not-performed',release_upgrade_qualification='not-performed')
        statements=[raw(root/'source/experiments/ingest-succession/schema_grants.sql').decode(),
            'GRANT SELECT ON control.initial_ingest_activations TO bigbrotr_control;',
            'GRANT UPDATE ON control.producer_epochs TO bigbrotr_control;',
            'GRANT INSERT ON control.producer_outcomes TO bigbrotr_control;']
        assert sha(statements[0].encode())=='36a16062c994adfe1ba7ace4c5e56d2ed942277b0e227ddd048e320939f0e588'
        assert clone['grants']==[dict(statement=s,stdout='') for s in statements]
        assert clone['catalog_before']==clone['catalog_after']==clone['final_catalog']
        assert sha(clone['catalog_before'].encode())==model.PINS[name]['catalog_sha256']
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
        for step in STEPS:
            label=name+'-'+step; case=only(report['cases'],name=label)
            specification(case)
            assert case['status']=='PASS' and case['custody']=='OBSERVED_CLOSED_AND_JOINED'
            assert case['returncode']==0 and type(case['returncode']) is int and case['catalog_after']==clone['catalog_before']
            item=lambda suffix:read(folder/(label+'-'+suffix+'.json'))
            context=item('verification')
            expected_case={k:case[k] for k in ('name','clone','origin','operation','predecessor','database','config','custody')}|dict(status='RUNNING')
            assert context['case']==expected_case
            before,after,native,after_native,inputs,after_inputs=[item(s) for s in ('before','after','before-native','after-native','inputs','after-inputs')]
            same(expected[0],before); assert native==expected[1] and inputs==expected[2]
            same(before,context['before']); same(after,context['after'])
            assert native==context['native_before'] and after_native==context['native_after']
            assert inputs==context['inputs_before'] and after_inputs==context['inputs_after']
            for stage,value in [('before',native),('after',after_native)]:
                a=item(stage+'-dump-response'); assert set(a)=={'returncode','stdout','stderr','seconds'}
                assert type(a['returncode']) is int and a['returncode']==0 and a['stderr']==''
                number(a['seconds'],0,120); assert loads(a['stdout'])==value
            response=item('response'); assert response==context['response']; process_closed(response,case)
            identity=read(folder/(label+'.pid.json')); ended=read(folder/(label+'.terminal.json'))
            assert set(identity)=={'pid','start_ticks','command','config','prefix'}
            assert all(ended[k]==v for k,v in identity.items()) and ended==response['terminal']
            for stream in ('stdout','stderr'):
                data=raw(folder/(label+'.'+stream)); assert len(data)<=65536 and data.decode()==response[stream]
            window=item('clock'); assert window==context['server_window']==case['server_window']
            number(window['before_us'],previous_time,finished,integer=True)
            previous_time=number(window['after_us'],window['before_us'],finished,integer=True)
            assert item('fixture')==context['fixture'] and item('observations')==context['snapshots']
            for s in context['snapshots']: same(s['control'],item(s['stage']))
            if step=='live': assert item('signal')==context['fixture']['signal']
            backend=item('backends'); assert backend==context['backend'] and item('control-backends')==backend['absence']
            backend_checked(backend,case['database'],before,after)
            assert item('external')==dict(control=after,native=after_native,inputs=after_inputs,backend=backend,catalog=clone['catalog_before'])
            assert context['catalog_before']==context['catalog_after']==clone['catalog_before']
            assert item('custody')==dict(status='OBSERVED_CLOSED_AND_JOINED',database=case['database'],backend_absent=True,errors=[])
            assert not (folder/(label+'-external-unavailable.json')).exists()
            # Provisional observations are retained, never substituted for final.
            assert set(item('provisional-sql'))==set(before)
            assert item('provisional-inputs')==inputs
            spool_checked(folder/name/'store/producer-inputs-v1',after_inputs,raw,complete=True)
            assert context['input_contents']=={n:raw(folder/name/'store/producer-inputs-v1'/n).hex() for n in after_inputs}
            result,controls=verify(context,seed),negative_controls(context,seed)
            assert case['oracle']==result and case['negative_controls']==controls
            results.append(dict(name=label,oracle=result,negative_controls=controls))
            expected=(after,after_native,after_inputs)
        same(expected[0],read(folder/(name+'-final-sql.json')))
    return dict(status=STATUS+'_CAPTURE_REPLAY',source=report['source'],cases=results,bound_files=bound,
        original_preservation='Physical/input files reread; SQL/catalog preservation is captured-run evidence',
        scope='Installed lifecycle and new empty Serve lifetimes; no new job, Archive, input-tail repair or ACK/GC')


if __name__=='__main__':
    import argparse
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('root',type=Path);p.add_argument('--output',type=Path)
    a=p.parse_args(); result=replay(a.root); text=json.dumps(result,indent=2)+'\n'
    if a.output:
        with a.output.open('x') as stream:stream.write(text)
    else:print(text,end='')
