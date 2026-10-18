"""Independent full-state model: real held custody, then 126 issued + 3 planned.

Observed random IDs/times/xmins parameterize named operational rows only. Original
batch bytes, receipts, topology and all other tables come from literal science.
"""
import copy
import json
import math
from pathlib import Path
import re
from installed_archive_catch_up_fault_oracle import (prefix,control,Model,only,same,sha,be,binary,clock,fresh_xmin,instant,exclusion,normal)

MODES=('holder','held-refusal','cap-setup','cap-refusal')
ERRORS={'held-refusal':'error: Archive catch-up refused; preserve original state: Occupied\n',
    'cap-refusal':'error: Archive catch-up refused; preserve original state: "issued plus planned attempts exceed finite profile"\n'}
SECONDS={'holder':240,'cap-setup':1800,'held-refusal':120,'cap-refusal':4200}


def process_checked(answer,mode,database,config):
    assert answer['outcome']=='PROCESS_EXITED' and type(answer['returncode']) is int
    assert answer['returncode']==(4 if mode in ERRORS else 0)
    if mode in ERRORS:assert not answer['stdout'] and answer['stderr']==ERRORS[mode]
    else:assert not answer['stderr']
    t=answer['terminal'];name='attempt-'+mode
    assert t['reaped'] is True and t['forced'] is False and t['error'] is None and t['exit_code']==answer['returncode']
    assert t['prefix']=='/lab/'+name and t['config']==config
    assert all(type(t[k]) is int and t[k]>0 for k in ('pid','start_ticks'))
    assert type(t['elapsed_seconds']) in (int,float) and math.isfinite(t['elapsed_seconds']) and 0<t['elapsed_seconds']<=SECONDS[mode]+5
    cmd=['/build/bigbrotr','archive','catch-up','--file',config] if mode in ERRORS else [
        '/build/ingest_bootstrap_probe','successor-archive-attempt-'+('holder' if mode=='holder' else 'cap'),
        database,'/run/secrets/application',config,'262144']+(['/lab/attempt-holder.release'] if mode=='holder' else [])
    assert t['command']==cmd
    if mode not in ERRORS:
        events=[json.loads(s) for s in answer['stdout'].splitlines()]
        assert events[-1]==dict(event='archive-attempt-complete',mode='holder' if mode=='holder' else 'cap',native='closed',
            control='released-and-joined',observer='joined',coordinator='closed',new_attempts=0 if mode=='holder' else 122,provisioned=0,published=0)
        assert not any('timeout' in e['event'] or e.get('outcome')=='unknown' for e in events)
        assert [e for e in events if e['event']=='archive-attempt-native']==[dict(event='archive-attempt-native',state='closed')]
        assert [e for e in events if e['event']=='archive-attempt-driver']==[
            dict(event='archive-attempt-driver',driver=d,joined=True) for d in ('control','observer')]
        return events


def probe_control(model,observed,failed):
    """Same exact lifecycle, explicitly twenty-second qualification lease."""
    wanted=model.pg;old=copy.deepcopy(max((r for r in wanted['owner_epochs'] if r['role']=='control'),key=lambda r:r['epoch']))
    actual=only(observed['owner_epochs'],role='control',epoch=old['epoch']+1)
    role=only(observed['role_instances'],role_instance_id=actual['role_instance_id'])
    rid=model.identity(role['role_instance_id'][2:]);inc=model.identity(actual['process_incarnation'][2:])
    for at in (role['created_at_us'],actual['acquired_at_us'],actual['lease_expires_at_us']-20_000_000):clock(at,model.window)
    assert model.window['before_us']<=instant(actual['control_backend_start'])<=role['created_at_us']<=actual['acquired_at_us']<=actual['lease_expires_at_us']-20_000_000
    assert type(actual['control_backend_pid']) is int and actual['control_backend_pid']>0
    fresh_xmin(actual['xmin'],wanted);fresh_xmin(role['xmin'],wanted)
    if old['state']=='active':
        closed=only(observed['owner_epochs'],role='control',epoch=old['epoch']);prior=only(wanted['owner_epochs'],role='control',epoch=old['epoch'])
        oldrole=only(wanted['role_instances'],role_instance_id=old['role_instance_id']);r=only(observed['role_instances'],role_instance_id=old['role_instance_id'])
        for at in (closed['released_at_us'],r['retired_at_us']):clock(at,model.window)
        assert closed['released_at_us']<=r['retired_at_us']<=role['created_at_us'];fresh_xmin(closed['xmin'],wanted)
        assert r['xmin']==closed['xmin']
        digest=sha(b'bigbrotr/control-exclusion/1\0'+binary(old['dataset_id'])+binary(old['resource_id'])+be(old['epoch'])+binary(old['process_incarnation'])+binary(inc))
        prior.update(state='released',released_at_us=closed['released_at_us'],exclusion_method='storage_fenced',exclusion_digest='\\x'+digest,xmin=closed['xmin'])
        oldrole.update(retired_at_us=r['retired_at_us'],xmin=closed['xmin'])
    else:assert old['state']=='released'
    model.control=dict(old,epoch=old['epoch']+1,role_instance_id=rid,process_incarnation=inc,acquired_at_us=actual['acquired_at_us'],
        lease_expires_at_us=actual['lease_expires_at_us'],state='active',released_at_us=None,exclusion_method=None,exclusion_digest=None,
        control_backend_pid=actual['control_backend_pid'],control_backend_start=actual['control_backend_start'],xmin=actual['xmin'])
    if not failed:
        for at in (actual['released_at_us'],role['retired_at_us']):clock(at,model.window)
        assert actual['lease_expires_at_us']-20_000_000<=actual['released_at_us']<=role['retired_at_us']<actual['lease_expires_at_us']
        assert role['xmin']==actual['xmin']
        model.control.update(state='released',released_at_us=actual['released_at_us'],exclusion_method='storage_fenced',exclusion_digest=exclusion(model.control))
    assert actual==model.control;wanted['owner_epochs'].append(model.control)
    wanted['role_instances'].append(dict(role_instance_id=rid,dataset_id=model.ds['dataset_id'],role='control',created_at_us=role['created_at_us'],
        retired_at_us=None if failed else role['retired_at_us'],xmin=role['xmin']))


def issuance(model,observed,science):
    wanted=model.pg;issuer=model.control;template=only(wanted['archive_attempts'],ordinal=1);batch=science['batches'][4]
    assert (batch['epoch'],batch['topology'],batch['first'],batch['last'])==(3,2,1,4)
    assert len(wanted['archive_attempts'])==4 and len(observed['archive_attempts'])==126
    ids=[]
    for ordinal in range(5,127):
        actual=only(observed['archive_attempts'],ordinal=ordinal)
        attempt=model.identity(actual['attempt_id'][2:]);worker=model.identity(actual['worker_incarnation'][2:])
        clock(actual['issued_at_us'],model.window);fresh_xmin(actual['xmin'],wanted);ids.append(actual['xmin'])
        assert issuer['acquired_at_us']<=actual['issued_at_us']<(issuer['released_at_us'] or issuer['lease_expires_at_us'])
        wanted['archive_attempts'].append(dict(template,attempt_id=attempt,ordinal=ordinal,expected_predecessor=ordinal-1,topology_generation=2,
            batch_digest='\\x'+batch['archive_digest'],coordinator_incarnation=issuer['process_incarnation'],worker_incarnation=worker,
            issuer_epoch=issuer['epoch'],issuer_incarnation=issuer['process_incarnation'],issued_at_us=actual['issued_at_us'],xmin=actual['xmin']))
    assert len(set(ids))==122


def local_inputs(before,after,science):
    files=after['physical']['state']['files'];records=after['local_records'];expected=set(before['physical']['state']['files'])
    assert set(records)=={p for p,v in files.items() if v['sha256'] is not None}
    for p,raw in before['local_records'].items():assert records[p]==raw and files[p]==before['physical']['state']['files'][p]
    for p,raw in before['retained'].items():assert after['retained'][p]==raw
    binding=sha(bytes.fromhex(records['coordinator.json']));initial=json.loads(bytes.fromhex(after['retained']['data/.bigbrotr-initialization/intent.json']))
    batch=science['batches'][4];retained=copy.deepcopy(before['retained'])
    for row in sorted(after['control']['archive_attempts'],key=lambda r:r['ordinal'])[4:]:
        name=row['attempt_id'][2:];lock=files[name+'/custody.lock']
        raw=b'BBINPUT\x01'+bytes.fromhex(binding)+binary(row['attempt_id'])+binary(row['dataset_id'])+binary(row['catalog_id'])+binary(row['coordinator_incarnation'])+binary(row['worker_incarnation'])+be(row['ordinal'])+be(2)+bytes.fromhex(batch['archive_digest'])+be(1,4)+be(0,4)+be(3)+be(1)+b'\x01\x21'+be(4)+bytes.fromhex(batch['admission_digest'])
        raw+=bytes.fromhex(sha(raw));assert len(raw)==266 and records[name+'.inputs']==raw.hex()
        retained['state/'+name+'.inputs']=raw.hex()
        manifest=dict(format='bigbrotr-local-archive-custody/1',boot_id=initial['boot_id'],lock_device=lock['device'],lock_inode=lock['inode'],
            dataset_id=row['dataset_id'][2:],catalog_id=row['catalog_id'][2:],catalog_root=initial['catalog_root'],attempt_id=list(binary(row['attempt_id'])),
            coordinator_incarnation=row['coordinator_incarnation'][2:],worker_incarnation=row['worker_incarnation'][2:],publication_ordinal=row['ordinal'],topology_generation='2',batch_digest=batch['archive_digest'])
        assert bytes.fromhex(records[name+'/intent.json'])==json.dumps(manifest,separators=(',',':'),ensure_ascii=False).encode()
        assert records[name+'/custody.lock']=='' and row['coordinator_binding_sha256']=='\\x'+binding
        expected.update((name,name+'.inputs',name+'/intent.json',name+'/custody.lock'))
    assert set(files)==expected and after['retained']==retained and len(files)<=1024
    assert not any(p.startswith('frontier-') for p in set(files)-set(before['physical']['state']['files']))
    for p,raw in records.items():assert files[p]['mode']==0o600 and files[p]['links']==1 and files[p]['sha256']==sha(bytes.fromhex(raw))


def snapshots(events,before,after,science,window,cap):
    resources,=[e for e in events if e['event']=='archive-attempt-resources']
    expected=dict(event='archive-attempt-resources',mode='cap' if cap else 'holder',metadata_bytes=262144,native_bytes=128*1024**2,
        observation_bytes=32*1024**2,transport_bytes=4*1024**2,input_bytes=1024**2,items=16,operation_timeout_ms=10000,
        holder_timeout_ms=180000,new_attempts=122 if cap else 0,pending_bytes=resources['pending_bytes'])
    assert resources==expected and type(resources['pending_bytes']) is int and 9*1024**2<resources['pending_bytes']<10*1024**2
    current=max((r for r in after['control']['owner_epochs'] if r['role']=='control'),key=lambda r:r['epoch'])
    backend=[e for e in events if e['event']=='lineage-history-backend'];assert [e['present'] for e in backend]==[True,False]
    for e in backend:
        assert e==dict(event='lineage-history-backend',pid=current['control_backend_pid'],backend_start=e['backend_start'],
            database=e['database'],control_epoch=current['epoch'],present=e['present'])
        assert instant(e['backend_start'])==instant(current['control_backend_start']) and e['database']
    ds=only(before['control']['datasets'])
    config=only(before['control']['config_revisions'],revision=ds['config_revision'])
    policy=only(before['control']['policy_revisions'],revision=ds['policy_revision'])
    policies=[('acquired',None,None),('refreshed',None,None)]
    if cap:
        for epoch in range(1,4):
            policies.extend(('refreshed',epoch,r['request_sequence']) for r in sorted(before['control']['producer_requests'],key=lambda r:r['request_sequence']) if r['producer_epoch']==epoch)
            policies.append(('refreshed',epoch,None))
        policies += [('refreshed',None,None)]*2+[('attempt-refreshed',None,None)]*122
    assert [e for e in events if e['event']=='lineage-history-policy']==[
        dict(event='lineage-history-policy',phase=phase,epoch=epoch,sequence=sequence,control_epoch=current['epoch'],
            config_revision=ds['config_revision'],policy_revision=ds['policy_revision'],config_digest=config['document_digest'][2:],policy_digest=policy['document_digest'][2:])
        for phase,epoch,sequence in policies]
    states=[e for e in events if e['event']=='lineage-history-state'];assert states
    stages=[e['stage'] for e in states]
    assert stages[0]=='attempt-before-claim' and stages[-1]=='attempt-released'
    assert stages.count('attempt-before-release')==1
    for event in states:
        state=event['state'];tables={t:json.loads(raw) for t,raw in state['control_tables']}
        assert len(tables)==75 and set(tables)==set(before['control'])
        assert event['rows']==before['native']['rows'] and state['sequence']==before['native']['sequence'] and state['native_rows']==len(event['rows'])
        assert state['native_sha256']==sha(json.dumps(event['rows'],separators=(',',':'),ensure_ascii=False).encode())
        assert state['control_sha256']==sha(json.dumps(state['control_tables'],separators=(',',':'),ensure_ascii=False).encode())
        assert dict(state['originals'])==before['inputs']
        if event['stage']=='attempt-before-claim':same(tables,before['control']);continue
        if event['stage']=='attempt-released':same(tables,after['control']);continue
        model=Model(before['control'],before['native'],before['inputs'],window);probe_control(model,tables,True)
        if cap and event['stage'] in ('attempt-issued126','attempt-before-release'):issuance(model,tables,science)
        same(model.pg,tables)
    if cap:
        assert stages.count('attempt-issued126')==1
        proof,=[e for e in events if e['event']=='lineage-history-proof'];assert proof['verified_through']==3 and proof['matches_history'] is True and proof['admission'] is False
        basis=Model(before['control'],before['native'],before['inputs'],window);summaries=[]
        for epoch,kind,mask in ((1,'initial',8),(2,'activated',0),(3,'activated',6)):
            issued,cut,digest=basis.closure(epoch)
            summaries.append(dict(epoch=epoch,terminal_kind=kind,resolved_requests=issued,last_complete_sequence=cut,digest=digest,event_request_mask=mask))
        assert [e['summary'] for e in events if e['event']=='lineage-history-epoch']==summaries
        assert proof['last_epoch']==summaries[-1]
        expected_requests=[dict(event='lineage-history-request',producer_slot=r['producer_slot_id'][2:],epoch=r['producer_epoch'],
            sequence=r['request_sequence'],existing_copy_checked=True) for r in sorted(before['control']['producer_requests'],key=lambda r:(r['producer_epoch'],r['request_sequence']))]
        assert [e for e in events if e['event']=='lineage-history-request']==expected_requests
        loaded,=[e for e in events if e['event']=='lineage-history-loaded']
        assert loaded['state']=='Terminal' and loaded['native_epoch']==loaded['registered_epoch']==loaded['replay_through']==3
        assert loaded['pending_target'] is None and loaded['memberships']==6 and len(loaded['entries'])==3
        assert all(e['closing'] is True for e in loaded['entries'])
        batch=science['batches'][4];item,=[e for e in events if e['event']=='archive-attempt-input']
        assert item==dict(event='archive-attempt-input',first=dict(dataset_id=before['control']['datasets'][0]['dataset_id'][2:],shard_id=0,owner_epoch='3',sequence='1'),
            header=(b'\x01\x21'+be(4)+bytes.fromhex(batch['admission_digest'])).hex(),archive_sha256=batch['archive_digest'],topology=2)
        issued=[e for e in events if e['event']=='archive-attempt-issued'];assert len(issued)==122
        for e,ordinal in zip(issued,range(5,127)):
            row=only(after['control']['archive_attempts'],ordinal=ordinal)
            assert e==dict(event='archive-attempt-issued',ordinal=ordinal,attempt_id=row['attempt_id'][2:],worker_incarnation=row['worker_incarnation'][2:],
                coordinator_incarnation=row['coordinator_incarnation'][2:],registered=True,local_created=True,provisioned=False,published=False)
        assert len([e for e in events if e['event']=='lineage-history-policy' and e['phase']=='attempt-refreshed'])==122
    else:assert not any(e['event'] in ('archive-attempt-issued','lineage-history-proof','archive-attempt-input') for e in events)
    return len(states)


def unchanged(before,after,*,roles=False):
    for area in ('store','data','state','scratch'):
        assert {k:v for k,v in before['physical'][area].items() if k!='files'}=={k:v for k,v in after['physical'][area].items() if k!='files'}
    for key in ('catalog','native','inputs','contents'):assert before[key]==after[key],key
    assert before['physical']['data']==after['physical']['data']
    if roles:normal.roles(before,after,'catchup-retry')
    else:assert before['system_roles']==after['system_roles']


def external(phase,database,catalog):
    assert not phase['errors'] and phase['after'] is not None
    current=max((r for r in phase['after']['control']['owner_epochs'] if r['role']=='control'),key=lambda r:r['epoch'])
    assert phase['backend']==dict(exact=[dict(pid=current['control_backend_pid'],backend_start=current['control_backend_start'],database=database,present=False)],all={database:'0',catalog:'0'})
    assert set(phase['window'])=={'before_us','after_us'} and all(type(t) is int for t in phase['window'].values())
    assert 0<phase['window']['after_us']-phase['window']['before_us']<=(SECONDS[phase['mode']]+30)*1_000_000


def verify(value):
    assert value['status'] in ('RUNNING','PASS');revision,science,previous,count=prefix(value)
    ready=value['holder_ready'];before=ready['before'];after=ready['after']
    for key in ('control','native','inputs','contents','catalog','retained'):assert before[key]==previous[key]
    model=Model(before['control'],before['native'],before['inputs'],ready['window']);probe_control(model,after['control'],False);same(model.pg,after['control'])
    unchanged(before,after);assert before['local_records']==after['local_records'] and before['physical']['state']==after['physical']['state']
    events=process_checked(value['holder_response'],'holder',value['database'],value['config'])
    assert events[:len(ready['events'])]==ready['events'] and events[len(ready['events'])-1]['event']=='archive-attempt-ready'
    assert events[-2]==dict(event='archive-attempt-holder',state='released-and-recovered')
    assert len(events)==len(ready['events'])+2
    held,=[e for e in events if e['event']=='archive-attempt-held'];first=only(before['control']['archive_attempts'],ordinal=1)
    assert held==dict(event='archive-attempt-held',attempt_id=first['attempt_id'][2:],ordinal=1,state_root='/lab/archive-export/state',
        catalog_root='/lab/archive-export/data/',second_recover='Occupied')
    assert ready['events'][-1]==dict(event='archive-attempt-ready',native='closed',control='released-and-joined',observer='joined',coordinator='released-and-reacquired',gate='/lab/attempt-holder.release',ordinal=1)
    count+=snapshots(events,before,after,science,ready['window'],False)
    external(dict(mode='holder',errors=[],after=after,window=ready['window'],backend=ready['backend']),value['database'],value['setup_resource']['catalog_database'])
    assert [p['mode'] for p in value['attempts']]==['held-refusal','cap-setup','cap-refusal'];previous=after
    for phase in value['attempts']:
        mode=phase['mode'];before,after=phase['before'],phase['after']
        for key in ('control','catalog','native','inputs','contents','retained','local_records'):assert before[key]==previous[key],key
        assert phase['config']==value['config'] and phase['config_text']==value['steps'][-1]['config_text']
        events=process_checked(phase['response'],mode,value['database'],value['config'])
        external(phase,value['database'],value['setup_resource']['catalog_database'])
        model=Model(before['control'],before['native'],before['inputs'],phase['window'])
        if mode=='cap-setup':
            probe_control(model,after['control'],False);issuance(model,after['control'],science);local_inputs(before,after,science)
            count+=snapshots(events,before,after,science,phase['window'],True)
        else:
            control(model,after['control'],True)
            assert before['retained']==after['retained'] and before['local_records']==after['local_records'] and before['physical']['state']==after['physical']['state']
        same(model.pg,after['control']);unchanged(before,after,roles=mode=='cap-refusal')
        if mode=='held-refusal':assert phase['window']['before_us']<value['held_terminal_us']<=value['holder_release_us']<=phase['window']['after_us']
        if mode=='cap-refusal':assert len(after['control']['archive_attempts'])==126 and len(science['batches'])-4==3
        previous=after;count+=2
    return dict(status='PASS',installed_processes=12,finite_probes=2,control_tables=75,snapshots=count+2,
        holder='typed LocalAttempt with released global coordinator',issued=126,planned=3,total=129,new_catchup_attempts=0,
        new_receipts=0,new_frontier_revisions=0,original_frontier_revision=revision,scope='source-authored expected state; actual runtime required')


def negative_controls(value):
    verify(value);rejected=[]
    def check(name,change):
        candidate=copy.deepcopy(value);change(candidate)
        try:verify(candidate)
        except (AssertionError,KeyError,ValueError,TypeError,IndexError,StopIteration):rejected.append(name)
        else:raise AssertionError(name+' accepted')
    check('forced-holder',lambda v:v['holder_response']['terminal'].update(forced=True))
    check('holder-global-lock-retained',lambda v:v['holder_ready']['events'][-1].update(coordinator='held'))
    check('release-before-failure',lambda v:v.update(holder_release_us=v['held_terminal_us']-1))
    for index in range(3):
        check('unknown-'+str(index),lambda v,i=index:v['attempts'][i]['response'].update(outcome='OUTCOME_UNKNOWN'))
        check('live-backend-'+str(index),lambda v,i=index:v['attempts'][i]['backend']['exact'][0].update(present=True))
    check('wrong-cap-error',lambda v:v['attempts'][-1]['response'].update(stderr=ERRORS['held-refusal']))
    check('new-attempt127',lambda v:v['attempts'][-1]['after']['control']['archive_attempts'].append(dict(v['attempts'][-1]['after']['control']['archive_attempts'][-1],ordinal=127)))
    check('old-frontier-rewrite',lambda v:v['attempts'][-1]['after']['control']['archive_frontier_snapshots'][0].update(xmin='4294967000'))
    check('no-input-linkage',lambda v:v['attempts'][1]['after']['local_records'].pop(only(v['attempts'][1]['after']['control']['archive_attempts'],ordinal=5)['attempt_id'][2:]+'.inputs'))
    check('fabricated-coordinator',lambda v:only(v['attempts'][1]['after']['control']['archive_attempts'],ordinal=5).update(coordinator_incarnation='\\x'+'ab'*16))
    check('native-write',lambda v:v['attempts'][-1]['after']['native'].update(sequence=999999))
    check('replacement-physical-root',lambda v:v['attempts'][-1]['after']['physical']['store'].update(inode=999999))
    return dict(status='PASS',rejected=rejected,scope='assembled context controls, not runtime evidence')
