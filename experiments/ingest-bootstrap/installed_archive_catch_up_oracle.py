"""Independent complete-state same-root journey oracle; no runtime operations.

The initial signed recipe and later OLD/NEW signed recipe are independently
encoded. SQL expectations admit only explicit row/field transitions, preserving
all other tables, historical xmins, receipts and original input bytes.
"""
import copy
import json
import math
from pathlib import Path
import re
import sys

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'ingest-succession'))
from installed_archive_catch_up_model import *
from installed_archive_frontier import COUNTS
from installed_archive_catch_up_http import add_http
from installed_family_replay import OLD_EVENT,NEW_EVENT,instant
from installed_sync_oracle import expected as initial_native

STEPS=('abandon1','retire1','live2','abandon2','retire2','live3','source3','abandon3','retire3',
       'catchup','catchup-retry','ordinary','ordinary-restart')


def semantic(value):
    return {k:v for k,v in value.items() if k!='physical'}


def finite_report(model,step,actual):
    epoch=int(step[-1]);issued,cut,digest=model.closure(epoch)
    state={'abandon1':'acquisition-abandoned','retire1':'initial-retired',
        'abandon2':'activated-acquisition-abandoned','retire2':'activated-initial-retired',
        'source3':'successor-source-closed','abandon3':'successor-acquisition-abandoned','retire3':'successor-retired'}[step]
    wanted=dict(format='bigbrotr-ingest-recovery/1',state=state,dataset_id=model.ds['dataset_id'][2:],
        owner_resource_id=model.resource[2:],producer_slot_id=model.slot[2:],producer_epoch=str(epoch),
        issued_through=str(issued),reserved_through=str(issued),resolved_requests=str(issued),
        native='closed',owner_release='committed',ready=False,
        source_closure=dict(last_complete_sequence=str(cut),sha256=digest))
    if epoch==3: wanted['verified_through']='3'
    if step.startswith('abandon'):
        wanted['acquisition_abandonment']=dict(jobs='0' if epoch==2 else '1',
            reason=('initial_producer_abandoned','activated_initial_producer_abandoned','ingest_successor_abandoned')[epoch-1],coverage='unknown')
    if step.startswith('retire'):
        wanted['ingest_retirement']=dict(allocation_domains='9',acquisition_gaps='0' if epoch==2 else '1',
            owner_release='committed',native_acknowledged='0',native_retired='0')
    assert actual==wanted


def family(phase):
    before,after=phase['before'],phase['after']; window=phase['window']; fixture=phase['fixture']
    reports=[json.loads(s) for s in phase['response']['stdout'].splitlines()]
    snapshots=fixture['snapshots']
    assert [s['stage'] for s in snapshots]==['started','prepared','submitted','commands-finished','renewed','before-stop']
    model=Family(before['control'],before['native'],before['inputs'],window)
    model.startup(snapshots[0]['control'],reports[0]);assert model.plan['epoch']==3 and model.plan['limit']==32
    science=NetworkScience(model,model.plan)
    journey=fixture['journey']; first=journey['closed'][0]
    assert journey['terminal']==dict(exit_code=0,reaped=True,outcome='PROCESS_EXITED')
    assert journey['result']['status']=='PASS' and journey['result']['relay_joined'] is True
    assert 2<=len(journey['opened'])==len(journey['closed'])<=128
    assert first['frames']==['distinct','predecessor','distinct-resend','EOSE']
    assert first['event_ids']==[NEW_EVENT['id'],OLD_EVENT['id'],NEW_EVENT['id']]
    assert first['completion']=='request-and-websocket-close'
    for opened,closed in zip(journey['opened'],journey['closed']):
        assert all(closed[k]==v for k,v in opened.items())
        assert opened['request'][0]=='REQ' and opened['request'][2]==dict(kinds=[1],limit=3)
        assert 0<closed['closed_at_us']-opened['request_at_us']<3_000_000
    native=native_map(after['native'])
    def fact(n):
        raw=bytes.fromhex(native['facts',(b'\x01\x20'+science.source(3,n)).hex()])
        return decode(raw[34:].split(b'\0',1)[1])
    dictionary,recorded=fact(1)[4],fact(5)[4]
    received,resolution=fact(8)[5][1],fact(8)[5][3][1]
    old_received,old_recorded=fact(10)[5][4],fact(10)[4]
    for at in (dictionary,recorded,received,old_received,old_recorded):clock(at,window)
    assert resolution==first['clock_resolution_us'] and type(resolution) is int and 0<resolution<=1_000_000
    assert first['sent_at_us']-resolution<=received<=old_received<=first['closed_at_us']+resolution
    assert received<=recorded and old_received<=old_recorded
    science.network(True,dictionary,received,recorded,resolution,(old_received,old_recorded))
    assert after['native']==model.native and after['inputs']==model.inputs
    for name,raw in after['contents'].items():
        data=bytes.fromhex(raw);assert after['inputs'][name]==dict(bytes=len(data),sha256=sha(data))
        if '-0000000000000003-' in name: assert data==science.inputs[int(name[-21:-5],16)]
    assert snapshots[0]['inputs']==before['inputs']
    active=snapshots[0]['control'];previous=active
    for snap in snapshots[1:]+[dict(stage='final',control=after['control'],inputs=after['inputs'],after_us=window['after_us'])]:
        observed=snap['control'];wanted=copy.deepcopy(active)
        leases(wanted,observed,previous,window,model.control['epoch'],3,stopped=snap['stage']=='final')
        label='renewed' if snap['stage']=='commands-finished' else snap['stage']
        scope,enrollment=add_http(wanted,observed,before['control'],journey['result'],label,window,model.control)
        if snap['stage']!='prepared':admissions(wanted,observed,science,scope,enrollment,model.control,window)
        same(wanted,observed);previous=observed
        assert snap['inputs']==(before['inputs'] if snap['stage']=='prepared' else model.inputs)
    signal=fixture['signal'];quiet=signal['opened']
    assert quiet in journey['opened'] and quiet['number']>1 and quiet['request_at_us']>fixture['initial_deadline_us']
    assert signal['kind']=='SIGTERM' and signal['closed_before_signal'] is False
    assert quiet['request_at_us']<=signal['signal_at_us']<=signal['signal_returned_at_us']
    assert 0<=signal['signal_monotonic_ns']-quiet['request_monotonic_ns']<100_000_000
    selected=next(r for r in journey['closed'] if r['number']==quiet['number'])
    assert selected['completion']=='quiet-eof' and selected['closed_at_us']>=signal['signal_at_us']
    assert reports[1]=={k:reports[0][k] for k in ('format','dataset_id','owner_resource_id','producer_slot_id','producer_epoch')} | dict(state='stopped',native='closed',owner_release='committed',source_closure='not-performed',ready=False)
    return model


def cut_digest(dataset,topology,receipts,bounds,snapshot):
    data=b'bigbrotr/archive-cut-inspection/1\0'+binary(dataset)+be(topology)+be(len(bounds))
    for epoch,cut in bounds: data+=bytes(4)+be(epoch)+be(cut)
    data+=be(len(receipts))
    for r in receipts:
        data+=bytes.fromhex(r['dataset_id'])+be(r['schema_version'],2)+be(r['topology_generation'])
        data+=bytes.fromhex(r['batch_digest'])+be(r['record_count'])+bytes.fromhex(r['attempt_id'])+be(r['publication_ordinal'])+be(len(r['input_ranges']))
        for b in r['input_ranges']:data+=be(b['shard_id'],4)+be(b['owner_epoch'])+be(b['first_sequence'])+be(b['last_sequence'])
    return sha(data+b'\x01'+be(snapshot))


def archive(phase,science,initial_revision,*,compacted=False):
    assert type(compacted) is bool
    if compacted:
        assert initial_revision==0
        return archive_first(phase,science)
    before,after=phase['before'],phase['after']; observed=after['control']; window=phase['window']
    model=Model(before['control'],before['native'],before['inputs'],window);model.completed_control(observed)
    wanted=model.pg; control=model.control; ds=model.ds['dataset_id'];consumer=only(wanted['consumers']);catalog=consumer['consumer_id']
    retry=phase['step']=='catchup-retry'; receipts=[]
    for index,(attempt,batch) in enumerate(zip(sorted(observed['archive_attempts'],key=lambda r:r['ordinal']),science['batches']),1):
        receipts.append(dict(dataset_id=ds[2:].upper(),schema_version=1,topology_generation=batch['topology'],
            batch_digest=batch['archive_digest'].upper(),record_count=batch['last']-batch['first']+1,
            input_ranges=[dict(shard_id=0,owner_epoch=batch['epoch'],first_sequence=batch['first'],last_sequence=batch['last'])],
            attempt_id=attempt['attempt_id'][2:].upper(),publication_ordinal=index))
        if index<=4 or retry: continue
        old=only(before['control']['archive_attempts'],ordinal=1)
        for field in ('attempt_id','worker_incarnation'):model.identity(attempt[field][2:])
        assert attempt['coordinator_incarnation']==control['process_incarnation']
        # One newly held coordinator lifetime owns all three new workers.
        if index>5:assert attempt['coordinator_incarnation']==only(observed['archive_attempts'],ordinal=5)['coordinator_incarnation']
        clock(attempt['issued_at_us'],window);fresh_xmin(attempt['xmin'],wanted)
        assert control['acquired_at_us']<=attempt['issued_at_us']<control['released_at_us']
        wanted['archive_attempts'].append(dict(old,attempt_id=attempt['attempt_id'],ordinal=index,expected_predecessor=index-1,
            topology_generation=2,batch_digest='\\x'+batch['archive_digest'],coordinator_incarnation=attempt['coordinator_incarnation'],
            worker_incarnation=attempt['worker_incarnation'],issuer_epoch=control['epoch'],issuer_incarnation=control['process_incarnation'],
            issued_at_us=attempt['issued_at_us'],xmin=attempt['xmin']))
    assert len(observed['archive_attempts'])==len(receipts)==7
    if not retry:
        previous=only(wanted['owner_epochs'],role='archive');oldrole=only(wanted['role_instances'],role_instance_id=previous['role_instance_id'])
        oldactual=only(observed['owner_epochs'],role='archive',epoch=1);oldroleactual=only(observed['role_instances'],role_instance_id=oldrole['role_instance_id'])
        first_revision=only(observed['consumer_frontier_revisions'],frontier_revision=initial_revision+1)
        tx=first_revision['xmin'];fresh_xmin(tx,wanted)
        for at in (oldactual['released_at_us'],oldroleactual['retired_at_us']):clock(at,window)
        digest=cut_digest(ds,1,receipts[:4],[(1,12),(2,0)],8)
        excluded=sha(b'bigbrotr/archive-frontier-owner-exclusion/1\0'+binary(ds)+binary(catalog)+be(1)
            +binary(previous['process_incarnation'])+binary(control['resource_id'])+be(control['epoch'])
            +binary(control['process_incarnation'])+bytes.fromhex(digest))
        previous.update(state='released',released_at_us=oldactual['released_at_us'],exclusion_method='storage_fenced',exclusion_digest='\\x'+excluded,xmin=tx)
        oldrole.update(retired_at_us=oldroleactual['retired_at_us'],xmin=tx)
        owner=only(observed['owner_epochs'],role='archive',epoch=2);rid=model.identity(owner['role_instance_id'][2:])
        role=only(observed['role_instances'],role_instance_id=rid)
        for at in (role['created_at_us'],owner['acquired_at_us'],owner['lease_expires_at_us']-6_000_000):clock(at,window)
        assert oldactual['released_at_us']<=oldroleactual['retired_at_us']<=role['created_at_us']<=owner['acquired_at_us']
        finaltx=only(observed['consumer_frontier_revisions'],frontier_revision=initial_revision+3)['xmin']
        wanted['owner_epochs'].append(dict(previous,epoch=2,role_instance_id=rid,process_incarnation=control['process_incarnation'],
            acquired_at_us=owner['acquired_at_us'],lease_expires_at_us=owner['lease_expires_at_us'],state='active',released_at_us=None,
            exclusion_method=None,exclusion_digest=None,xmin=finaltx))
        wanted['role_instances'].append(dict(role_instance_id=rid,dataset_id=ds,role='archive',created_at_us=role['created_at_us'],retired_at_us=None,xmin=tx))
        original=only(wanted['archive_owner_sessions'])
        wanted['archive_owner_sessions'].append(dict(original,owner_epoch=2,owner_incarnation=control['process_incarnation'],
            issuer_epoch=control['epoch'],issuer_incarnation=control['process_incarnation'],xmin=tx))
        for offset,(topology,bounds,counts,selected) in enumerate([(1,[(1,12),(2,0)],[8,1,1,1,1],4),
            (2,[(1,12),(2,0),(3,0)],[8,1,1,1,1],4),(2,[(1,12),(2,0),(3,10)],[13,2,2,2,3],7)],1):
            revision=initial_revision+offset;r=only(observed['consumer_frontier_revisions'],frontier_revision=revision)
            at,tx=r['created_at_us'],r['xmin'];clock(at,window);fresh_xmin(tx,before['control'])
            assert control['acquired_at_us']<=at<=control['released_at_us']
            wanted['consumer_frontier_revisions'].append(dict(consumer_id=catalog,frontier_revision=revision,dataset_id=ds,
                consumer_role='archive',topology_generation=topology,previous_revision=revision-1,owner_resource_id=catalog,
                owner_epoch=2,owner_incarnation=control['process_incarnation'],created_at_us=at,sealed=True,xmin=tx))
            actual=only(observed['archive_frontier_snapshots'],frontier_revision=revision)
            operation=model.identity(actual['operation_id'][2:])
            wanted['archive_frontier_snapshots'].append(dict(operation_id=operation,consumer_id=catalog,frontier_revision=revision,
                dataset_id=ds,topology_generation=topology,catalog_id=catalog,owner_epoch=2,owner_incarnation=control['process_incarnation'],
                snapshot_id=8,request_digest='\\x'+cut_digest(ds,topology,receipts[:selected],bounds,8),**dict(zip(COUNTS,counts)),xmin=tx))
            for epoch,cut in bounds:
                batch=next((b for b in science['batches'] if b['epoch']==epoch and b['last']==cut),None)
                wanted['consumer_frontiers'].append(dict(consumer_id=catalog,frontier_revision=revision,dataset_id=ds,
                    topology_generation=topology,shard_id=0,owner_epoch=epoch,last_complete_sequence=cut,
                    attestation_kind='archive-batch' if cut else None,attestation_digest='\\x'+batch['archive_digest'] if cut else None,xmin=tx))
            for attempt in sorted(wanted['archive_attempts'],key=lambda r:r['ordinal'])[:selected]:
                wanted['archive_frontier_batches'].append(dict(consumer_id=catalog,frontier_revision=revision,attempt_id=attempt['attempt_id'],xmin=tx))
        consumer.update(current_revision=initial_revision+3,current_topology_generation=2,xmin=tx)
    same(wanted,observed)
    assert after['native']==before['native'] and after['inputs']==before['inputs'] and after['contents']==before['contents']
    snapshot=only(observed['archive_frontier_snapshots'],frontier_revision=initial_revision+3)
    report=json.loads(phase['response']['stdout'])
    assert report==dict(format='bigbrotr-archive-catch-up/1',dataset_id=ds[2:],catalog_id=catalog[2:],terminal_epoch=3,
        issued_attempts_before=7 if retry else 4,new_attempts=0 if retry else 3,frontier_revision=initial_revision+3,
        snapshot_id=8,operation_id=list(binary(snapshot['operation_id'])),request_digest=snapshot['request_digest'][2:],
        family_counts=[13,2,2,2,3],new_frontier_revisions=0 if retry else 3)
    actual=json.loads(phase['readback']['stdout'])
    assert actual==dict(format='bigbrotr-independent-installed-archive-read/1',tables={**science['tables'],'archive_commits':receipts})
    if retry:
        assert before['catalog']==after['catalog']
        assert before['physical']['data']==after['physical']['data'] and before['physical']['state']==after['physical']['state']
    else:
        from installed_archive_catch_up_catalog import verify_catalog
        verify_catalog(phase,science)
    return receipts


def archive_first(phase,science):
    """Virgin compacted publication; expected rows never come from Archive output."""
    before,after=phase['before'],phase['after']; observed=after['control'];window=phase['window']
    assert len(before['control'])==77 and len(science['batches'])==8
    assert [len(science['tables'][t]) for t in TABLES]==[12,4,4,4,4]
    model=Model(before['control'],before['native'],before['inputs'],window)
    model.completed_control(observed);wanted=model.pg;control=model.control;ds=model.ds['dataset_id']
    persistence=only(wanted['archive_persistence']);catalog=persistence['catalog_id']
    assert persistence['dataset_id']==ds and persistence['protocol_version']==1
    retry=phase['step']=='catchup-retry';assert phase['step'] in ('catchup','catchup-retry')
    retained_max=max(int(r['xmin']) for rows in before['control'].values() for r in rows)
    previous_xid=retained_max;previous_at=control['acquired_at_us'];receipts=[]
    attempts=sorted(observed['archive_attempts'],key=lambda r:r['ordinal'])
    assert len(attempts)==8
    for index,(attempt,batch) in enumerate(zip(attempts,science['batches']),1):
        assert batch['topology']==2
        receipts.append(dict(dataset_id=ds[2:].upper(),schema_version=1,topology_generation=2,
            batch_digest=batch['archive_digest'].upper(),record_count=batch['last']-batch['first']+1,
            input_ranges=[dict(shard_id=0,owner_epoch=batch['epoch'],first_sequence=batch['first'],last_sequence=batch['last'])],
            attempt_id=attempt['attempt_id'][2:].upper(),publication_ordinal=index))
        if retry:continue
        for field in ('attempt_id','worker_incarnation'):model.identity(attempt[field][2:])
        at,tx=attempt['issued_at_us'],attempt['xmin'];clock(at,window);fresh_xmin(tx,wanted)
        assert previous_xid<int(tx)<int(control['xmin'])
        assert previous_at<=at<control['released_at_us']<control['lease_expires_at_us']
        wanted['archive_attempts'].append(dict(attempt_id=attempt['attempt_id'],dataset_id=ds,catalog_id=catalog,
            ordinal=index,expected_predecessor=index-1,schema_version=1,topology_generation=2,
            batch_digest='\\x'+batch['archive_digest'],coordinator_incarnation=control['process_incarnation'],
            worker_incarnation=attempt['worker_incarnation'],issuer_resource=control['resource_id'],issuer_epoch=control['epoch'],
            issuer_role='control',issuer_incarnation=control['process_incarnation'],issued_at_us=at,
            coordinator_binding_sha256=persistence['coordinator_binding_sha256'],persistence_protocol=1,xmin=tx))
        previous_xid,previous_at=int(tx),at
    if not retry:
        assert not before['control']['archive_attempts']
        for table in ('consumers','consumer_frontier_revisions','consumer_frontiers','archive_frontier_snapshots',
                'archive_frontier_batches','archive_owner_sessions'):
            assert not wanted[table]
        assert not any(r['role']=='archive' for r in wanted['owner_epochs']+wanted['role_instances'])
        owner=only(observed['owner_epochs'],role='archive');roleid=model.identity(owner['role_instance_id'][2:])
        role=only(observed['role_instances'],role_instance_id=roleid)
        consumer=only(observed['consumers']);revision=only(observed['consumer_frontier_revisions'])
        snapshot=only(observed['archive_frontier_snapshots']);operation=model.identity(snapshot['operation_id'][2:])
        tx=revision['xmin'];fresh_xmin(tx,wanted);assert previous_xid<int(tx)<int(control['xmin'])
        for at in (role['created_at_us'],owner['acquired_at_us'],owner['lease_expires_at_us']-6_000_000,
                consumer['created_at_us'],revision['created_at_us']):clock(at,window)
        # archive_owner copies the already refreshed Control deadline; it does
        # not renew a six-second lease from its later INSERT clock.
        refreshed=owner['lease_expires_at_us']-6_000_000
        assert previous_at<=refreshed<=role['created_at_us']<=owner['acquired_at_us']<=consumer['created_at_us']<=revision['created_at_us']<=control['released_at_us']
        assert revision['created_at_us']<owner['lease_expires_at_us']
        wanted['owner_epochs'].append(dict(resource_id=catalog,epoch=1,dataset_id=ds,role='archive',
            role_instance_id=roleid,process_incarnation=control['process_incarnation'],acquired_at_us=owner['acquired_at_us'],
            lease_expires_at_us=owner['lease_expires_at_us'],state='active',released_at_us=None,exclusion_method=None,
            exclusion_digest=None,control_fence_version=None,control_backend_pid=None,control_backend_start=None,
            archive_frontier_protocol=1,xmin=tx))
        wanted['role_instances'].append(dict(role_instance_id=roleid,dataset_id=ds,role='archive',
            created_at_us=role['created_at_us'],retired_at_us=None,xmin=tx))
        wanted['archive_owner_sessions'].append(dict(dataset_id=ds,catalog_id=catalog,owner_epoch=1,owner_role='archive',
            owner_incarnation=control['process_incarnation'],coordinator_binding_sha256=persistence['coordinator_binding_sha256'],
            persistence_protocol=1,issuer_resource=control['resource_id'],issuer_epoch=control['epoch'],issuer_role='control',
            issuer_incarnation=control['process_incarnation'],xmin=tx))
        wanted['consumers'].append(dict(consumer_id=catalog,dataset_id=ds,consumer_role='archive',
            created_at_us=consumer['created_at_us'],current_revision=1,current_topology_generation=2,xmin=tx))
        wanted['consumer_frontier_revisions'].append(dict(consumer_id=catalog,frontier_revision=1,dataset_id=ds,
            consumer_role='archive',topology_generation=2,previous_revision=None,owner_resource_id=catalog,owner_epoch=1,
            owner_incarnation=control['process_incarnation'],created_at_us=revision['created_at_us'],sealed=True,xmin=tx))
        bounds=[(1,27),(2,1),(3,0)]
        wanted['archive_frontier_snapshots'].append(dict(operation_id=operation,consumer_id=catalog,frontier_revision=1,
            dataset_id=ds,topology_generation=2,catalog_id=catalog,owner_epoch=1,owner_incarnation=control['process_incarnation'],
            snapshot_id=9,request_digest='\\x'+cut_digest(ds,2,receipts,bounds,9),**dict(zip(COUNTS,[12,4,4,4,4])),xmin=tx))
        for epoch,cut in bounds:
            batch=next((b for b in science['batches'] if b['epoch']==epoch and b['last']==cut),None)
            wanted['consumer_frontiers'].append(dict(consumer_id=catalog,frontier_revision=1,dataset_id=ds,
                topology_generation=2,shard_id=0,owner_epoch=epoch,last_complete_sequence=cut,
                attestation_kind='archive-batch' if cut else None,attestation_digest='\\x'+batch['archive_digest'] if cut else None,xmin=tx))
        wanted['archive_frontier_batches']=[dict(consumer_id=catalog,frontier_revision=1,attempt_id=a['attempt_id'],xmin=tx) for a in attempts]
    same(wanted,observed)
    assert after['native']==before['native'] and after['inputs']==before['inputs'] and after['contents']==before['contents']
    snapshot=only(observed['archive_frontier_snapshots'])
    assert json.loads(phase['response']['stdout'])==dict(format='bigbrotr-archive-catch-up/1',dataset_id=ds[2:],
        catalog_id=catalog[2:],terminal_epoch=3,issued_attempts_before=8 if retry else 0,new_attempts=0 if retry else 8,
        frontier_revision=1,snapshot_id=9,operation_id=list(binary(snapshot['operation_id'])),request_digest=snapshot['request_digest'][2:],
        family_counts=[12,4,4,4,4],new_frontier_revisions=0 if retry else 1,
        recovery_basis='closed-producer-floor-v3',compacted_requests='7')
    assert json.loads(phase['readback']['stdout'])==dict(format='bigbrotr-independent-installed-archive-read/1',
        tables={**science['tables'],'archive_commits':receipts})
    if retry:
        assert before['catalog']==after['catalog']
        assert before['physical']['data']==after['physical']['data'] and before['physical']['state']==after['physical']['state']
    else:
        from installed_archive_catch_up_catalog import verify_catalog
        verify_catalog(phase,science,virgin=True)
    return receipts


def initial(value):
    from installed_archive_oracle import expected
    from installed_archive_frontier import validate,current_owner_view
    from installed_sync_oracle import SIGNED_SHA256
    state=value['initial_state'];report=value['initialized'];evidence=value['initial']
    event_raw=bytes.fromhex(value['initial_event_hex']);assert sha(event_raw)==SIGNED_SHA256
    event=json.loads(event_raw);assert event==OLD_EVENT
    peers=value['initial_transcript'];assert 2<=len(peers)<=128
    first=peers[0];assert first['frames']==['signed','forged-signature','duplicate','EOSE']
    dictionary,received,recorded=(evidence[k] for k in ('dictionary_recorded_us','event_received_us','event_recorded_us'))
    assert first['sent_at_us']<=received<=first['closed_at_us'] and received<=recorded
    view=current_owner_view(state['control'],value['initial_setup'])
    rows,inputs,receipts=initial_native(view,report,event,dictionary,received,recorded,first['clock_resolution_us'])
    assert state['native']['rows']==[dict(cf=cf,key=k.hex(),value=v.hex()) for cf,k,v in rows]
    assert type(state['native']['sequence']) is int and len(rows)==49 and 49<=state['native']['sequence']<128
    originals=[bytes.fromhex(v) for k,v in sorted(state['contents'].items())]
    assert originals==inputs and len(inputs)==4
    science=expected(view,report,event,dictionary,received,recorded,first['clock_resolution_us'])
    assert value['initial_science']=={k:science[k] for k in ('tables','batches')}
    validate(state['control'],state['catalog'],value['initial_setup'],science)
    for n,(request,outcome,raw,receipt) in enumerate(zip(sorted(state['control']['producer_requests'],key=lambda r:r['request_sequence']),
        sorted(state['control']['producer_outcomes'],key=lambda r:r['request_sequence']),inputs,receipts),1):
        assert request['producer_epoch']==outcome['producer_epoch']==1 and request['request_sequence']==outcome['request_sequence']==n
        assert request['input_bytes']==len(raw) and request['request_digest']==outcome['request_digest']=='\\x'+sha(raw)
        assert outcome['outcome_bytes']=='\\x'+receipt.hex() and outcome['outcome_sha256']=='\\x'+sha(receipt)
    consumer=only(state['control']['consumers']);revision=consumer['current_revision']
    assert 1<=revision<=4
    return revision


def retained(state,science):
    """Every actual .inputs identity/checksum is encoded independently."""
    control=state['control'];stored=state['retained'];name='data/.bigbrotr-initialization/intent.json'
    initialization=bytes.fromhex(stored[name]);expected={name:stored[name]}
    for row,batch in zip(sorted(control['archive_attempts'],key=lambda r:r['ordinal']),science['batches']):
        payload=(b'BBINPUT\x01'+bytes.fromhex(sha(initialization))+binary(row['attempt_id'])+binary(row['dataset_id'])+binary(row['catalog_id'])
            +binary(row['coordinator_incarnation'])+binary(row['worker_incarnation'])+be(row['ordinal'])+be(batch['topology'])
            +bytes.fromhex(batch['archive_digest'])+be(1,4)+be(0,4)+be(batch.get('epoch',1))+be(batch['first'])
            +b'\x01\x21'+be(batch['last'])+bytes.fromhex(batch['admission_digest']))
        raw=payload+bytes.fromhex(sha(payload));path='state/'+row['attempt_id'][2:]+'.inputs';expected[path]=raw.hex()
        meta=state['physical']['state']['files'][path.removeprefix('state/')]
        assert meta['mode']==0o600 and meta['links']==1 and meta['sha256']==sha(raw)
    assert stored==expected


def roles(before,after,phase,*,virgin_manager=None):
    """Only catch-up can issue workers; seals may rewrite role catalog xmin."""
    assert virgin_manager is None or (type(virgin_manager) is str and re.fullmatch('bb_archive_[0-9a-f]{24}',virgin_manager))
    if phase!='catchup':
        if phase=='catchup-retry':
            names={r['login_name'] for r in after['catalog']['bigbrotr_archive.attempts']}
            wanted=copy.deepcopy(before['system_roles'])
            for row in wanted['roles']:
                if row['rolname'] in names:row['xmin']=only(after['system_roles']['roles'],oid=row['oid'])['xmin']
            assert wanted==after['system_roles']
        else:assert before['system_roles']==after['system_roles']
        return
    wanted=copy.deepcopy(before['system_roles']);observed=after['system_roles']
    oldnames={r['login_name'] for r in before['catalog']['bigbrotr_archive.attempts']}
    for row in wanted['roles']:
        if row['rolname'] in oldnames:row['xmin']=only(observed['roles'],oid=row['oid'])['xmin']
    additions=[r for r in observed['roles'] if r['oid'] not in {p['oid'] for p in wanted['roles']}]
    attempts=[r for r in after['catalog']['bigbrotr_archive.attempts'] if r['ordinal']>(0 if virgin_manager else 4)]
    assert len(additions)==len(attempts)==(8 if virgin_manager else 3)
    assert {(r['rolname'],r['oid']) for r in additions}=={(r['login_name'],r['login_oid']) for r in attempts}
    for row in additions:
        if virgin_manager:
            assert not oldnames
            assert row==dict(oid=row['oid'],rolname=row['rolname'],xmin=row['xmin'],rolsuper=False,rolinherit=False,
                rolcreaterole=False,rolcreatedb=False,rolcanlogin=False,rolreplication=False,rolconnlimit=8,
                rolvaliduntil=None,rolbypassrls=False,credential_present=False)
            assert re.fullmatch('[1-9][0-9]*',str(row['oid'])) and re.fullmatch('[1-9][0-9]*',row['xmin'])
        else:
            old=only(before['system_roles']['roles'],rolname=next(iter(oldnames)))
            assert {k:v for k,v in row.items() if k not in ('oid','rolname','xmin')}=={k:v for k,v in old.items() if k not in ('oid','rolname','xmin')}
        assert not row['rolcanlogin'] and not row['credential_present']
    wanted['roles']+=additions;wanted['roles'].sort(key=lambda r:int(r['oid']))
    newmembers=[r for r in observed['members'] if r not in wanted['members']]
    assert len(newmembers)==len(additions) and {r['roleid'] for r in newmembers}=={r['oid'] for r in additions}
    # PostgreSQL 16 assigns each membership its own row OID.
    assert len({r['oid'] for r in observed['members']})==len(observed['members'])
    oldoids={r['oid'] for r in wanted['members']}
    if virgin_manager:
        manager=only(before['system_roles']['roles'],rolname=virgin_manager)
        bootstrap=only(before['system_roles']['roles'],rolname='postgres');assert bootstrap['rolsuper'] is True
        member_shape=dict(member=manager['oid'],grantor=bootstrap['oid'],admin_option=True,inherit_option=False,set_option=False)
    else:
        oldmember=next(r for r in wanted['members'] if r['roleid'] in {p['oid'] for p in before['system_roles']['roles'] if p['rolname'] in oldnames})
        member_shape={k:v for k,v in oldmember.items() if k not in ('oid','roleid','xmin')}
    for r in newmembers:
        assert str(r['oid']).isdecimal() and int(r['oid'])>0 and r['oid'] not in oldoids
        assert str(r['xmin']).isdecimal() and int(r['xmin'])>0
        assert {k:v for k,v in r.items() if k not in ('oid','roleid','xmin')}==member_shape
    wanted['members']+=newmembers;wanted['members'].sort(key=lambda r:tuple(int(r[k]) for k in ('roleid','member','grantor')))
    assert wanted==observed


def ordinary(phase,selection,science,receipts,prior):
    from installed_archive_catch_up_http import ordinary_http
    before,after=phase['before'],phase['after'];model=Model(before['control'],before['native'],before['inputs'],phase['window'])
    assert phase['fixture']['http_response']==dict(returncode=0,stdout='',stderr='')
    model.completed_control(after['control'])
    restart=phase['step']=='ordinary-restart'
    supplied=phase if not restart else dict(phase,prior_artifact_files=prior['artifact_files'],prior_artifact_contents=prior['artifact_contents'])
    ordinary_http(model,supplied,selection,restart);same(model.pg,after['control'])
    assert before['native']==after['native'] and before['inputs']==after['inputs'] and before['contents']==after['contents']
    assert before['catalog']==after['catalog'] and before['retained']==after['retained']
    for area in ('data','state'):assert before['physical'][area]==after['physical'][area]
    assert json.loads(phase['readback']['stdout'])==dict(format='bigbrotr-independent-installed-archive-read/1',
        tables={**science['tables'],'archive_commits':receipts})
    snapshots=phase['fixture']['snapshots'];assert [s['stage'] for s in snapshots]==['started','http-complete','before-stop']
    started=snapshots[0]['control'];wanted=copy.deepcopy(before['control'])
    actual=only(started['owner_epochs'],role='control',epoch=model.control['epoch'])
    role=only(started['role_instances'],role_instance_id=model.control['role_instance_id'])
    assert actual['lease_expires_at_us']<=model.control['lease_expires_at_us']
    wanted['owner_epochs'].append(dict(model.control,state='active',released_at_us=None,exclusion_method=None,exclusion_digest=None,
        lease_expires_at_us=actual['lease_expires_at_us'],xmin=actual['xmin']))
    wanted['role_instances'].append(dict(only(model.pg['role_instances'],role_instance_id=role['role_instance_id']),retired_at_us=None,xmin=role['xmin']))
    same(wanted,started)
    for snap in snapshots[1:]:
        wanted=copy.deepcopy(model.pg);current=only(snap['control']['owner_epochs'],role='control',epoch=model.control['epoch'])
        r=only(snap['control']['role_instances'],role_instance_id=model.control['role_instance_id'])
        only(wanted['owner_epochs'],role='control',epoch=model.control['epoch']).update(state='active',released_at_us=None,
            exclusion_method=None,exclusion_digest=None,lease_expires_at_us=current['lease_expires_at_us'],xmin=current['xmin'])
        only(wanted['role_instances'],role_instance_id=r['role_instance_id']).update(retired_at_us=None,xmin=r['xmin'])
        assert actual['lease_expires_at_us']<=current['lease_expires_at_us']<=model.control['lease_expires_at_us']
        same(wanted,snap['control'])
    assert all(s['inputs']==before['inputs'] for s in snapshots)
    ordinary_reports(model,phase)
    return model


def verify(value):
    from installed_archive_catch_up_client import argv
    assert value['status'] in ('RUNNING','PASS') and [s['step'] for s in value['steps']]==list(STEPS)
    baseline=value['initial_state'];revision=initial(value);previous=baseline;science=None;receipts=None;prior_read=None
    assert len(baseline['control'])==75
    expected_tables=set(baseline['control']);snapshot_count=1
    for phase in value['steps']:
        before,after=phase['before'],phase['after'];step=phase['step'];response=phase['response'];fixture=phase['fixture'];window=phase['window']
        assert phase['database']==value['database'] and phase['config']==value['config']
        assert semantic(before)==semantic(previous)
        for field in ('before_us','after_us'):assert type(window[field]) is int and window[field]>0
        assert 0<window['after_us']-window['before_us']<=120_000_000
        assert response['outcome']=='PROCESS_EXITED' and response['returncode']==0 and not response['stderr']
        terminal=response['terminal'];assert terminal['reaped'] is True and terminal['forced'] is False and terminal['error'] is None
        assert terminal['exit_code']==0 and terminal['command']==argv(phase['operation'],Path(phase['config']))
        assert terminal['config']==phase['config'] and terminal['prefix']=='/lab/catch-up-'+step
        assert type(terminal['pid']) is int and terminal['pid']>0 and type(terminal['start_ticks']) is int and terminal['start_ticks']>0
        assert type(terminal['elapsed_seconds']) in (int,float) and math.isfinite(terminal['elapsed_seconds']) and 0<terminal['elapsed_seconds']<=105
        assert not fixture['errors'] and fixture.get('client_joined',True) is True
        assert set(before['control'])==set(after['control'])==expected_tables
        for state in (before,after):
            assert set(state['inputs'])==set(state['contents'])
            for name,raw in state['contents'].items():
                data=bytes.fromhex(raw);assert state['inputs'][name]==dict(bytes=len(data),sha256=sha(data))
            for area in ('store','data','state','scratch'):
                identity={k:v for k,v in state['physical'][area].items() if k!='files'}
                assert identity=={k:v for k,v in baseline['physical'][area].items() if k!='files'}
        for snap in fixture['snapshots']:
            assert set(snap['control'])==expected_tables
            clock(snap['before_us'],window);clock(snap['after_us'],window);assert snap['before_us']<=snap['after_us']
        reports=[json.loads(line) for line in response['stdout'].splitlines()]
        if step in ('live2','live3','ordinary','ordinary-restart'):
            assert len(reports)==2 and reports[-1]['state']=='stopped'
            assert fixture['initial_report']==reports[0]
        else:assert len(reports)==1
        if step=='live3':
            model=family(phase);science=archive_science(after['native'])
        elif step=='live2':
            model=Model(before['control'],before['native'],before['inputs'],window);model.live2(after['control'],reports[0])
            assert model.native==after['native'] and model.inputs==after['inputs'] and before['contents']==after['contents']
            live2_observations(model,phase,reports)
        elif step in ('catchup','catchup-retry'):
            assert science is not None;receipts=archive(phase,science,revision)
        elif step.startswith('ordinary'):
            assert science is not None and receipts is not None
            model=ordinary(phase,value['read_setup']['selection'],science,receipts,prior_read)
            prior_read=phase
        else:
            model=Model(before['control'],before['native'],before['inputs'],window);model.finite(after['control'],step)
            assert model.native==after['native'] and model.inputs==after['inputs'] and before['contents']==after['contents']
            finite_report(model,step,reports[0])
        if not step.startswith('catchup'):
            assert before['catalog']==after['catalog'] and before['retained']==after['retained']
            for area in ('data','state'):assert before['physical'][area]==after['physical'][area]
        roles(before,after,step)
        current=max((r for r in after['control']['owner_epochs'] if r['role']=='control'),key=lambda r:r['epoch'])
        assert phase['backend']['exact']==[dict(pid=current['control_backend_pid'],backend_start=current['control_backend_start'],database=phase['database'],present=False)]
        assert phase['backend']['all']=={phase['database']:'0',value['setup_resource']['catalog_database']:'0'}
        if step in ('catchup','catchup-retry','ordinary','ordinary-restart'):
            assert phase['readback']['returncode']==0 and not phase['readback']['stderr'];retained(after,science)
        previous=after;snapshot_count+=2+len(fixture['snapshots'])
    window=value['read_setup']['login_window'];observations=window['observations']
    assert 1<=len(observations)<=67 and window['expires_at_us']==max(r['window_started_at_us']+60_000_000 for r in value['steps'][10]['after']['control']['login_buckets'])
    assert observations[-1]['server_us']>=window['expires_at_us'] and 0<=observations[-1]['elapsed_seconds']<=66
    assert all(a['server_us']<=b['server_us'] and a['elapsed_seconds']<=b['elapsed_seconds'] for a,b in zip(observations,observations[1:]))
    return dict(status='PASS',installed_processes=14,continuation_processes=13,control_tables=75,snapshots=snapshot_count,
        scientific_records=22,archive_receipts=7,family_counts=[13,2,2,2,3],initial_frontier_revision=revision,
        final_frontier_revision=revision+3,final_snapshot=8,export_exact_retry=True,same_physical_roots=True)


def negative_controls(value):
    """Coherent edits of complete observations must not hide unrelated changes."""
    verify(value);rejected=[]
    def check(label,mutation):
        altered=copy.deepcopy(value);mutation(altered)
        try:verify(altered)
        except (AssertionError,KeyError,IndexError,ValueError,TypeError,StopIteration):rejected.append(label)
        else:raise AssertionError('negative accepted: '+label)
    check('native-sequence',lambda v:v['steps'][8]['after']['native'].__setitem__('sequence',v['steps'][8]['after']['native']['sequence']+1))
    check('unknown-process',lambda v:v['steps'][9]['response'].__setitem__('outcome','OUTCOME_UNKNOWN'))
    check('forced-process',lambda v:v['steps'][9]['response']['terminal'].__setitem__('forced',True))
    check('wrong-physical-inode',lambda v:v['steps'][9]['after']['physical']['data'].__setitem__('inode',0))
    check('head-fact-payload',lambda v:v['steps'][5]['after']['native']['rows'][-1].__setitem__('value','00'))
    check('old-receipt-xmin',lambda v:v['steps'][9]['after']['control']['producer_outcomes'][0].__setitem__('xmin','1'))
    check('missing-zero-stream',lambda v:v['steps'][9]['after']['control']['consumer_frontiers'].pop())
    check('missing-archive-receipt',lambda v:v['steps'][9]['after']['control']['archive_frontier_batches'].pop())
    check('export-artifact-bytes',lambda v:v['steps'][11]['artifact_contents'].__setitem__(next(k for k in v['steps'][11]['artifact_contents'] if k.endswith('events.jsonl')),'00'))
    check('retained-export-pin',lambda v:next(r for r in v['steps'][12]['after']['control']['snapshot_leases'] if r['state']=='active').__setitem__('state','released'))
    check('live2-started-source',lambda v:v['steps'][2]['fixture']['snapshots'][0]['control']['source_streams'][-1].__setitem__('state','closed'))
    check('ordinary-failed-client',lambda v:v['steps'][11]['fixture']['http_response'].__setitem__('returncode',1))
    check('ordinary-me-idle',lambda v:v['steps'][11]['fixture']['http'][0]['me'].__setitem__('idle_expires_at_us','0'))
    check('membership-oid',lambda v:v['steps'][9]['after']['system_roles']['members'][-1].__setitem__('oid','0'))
    def wrong_null_count(v):
        phase=v['steps'][9];old={r['data_file_id'] for r in phase['before']['catalog']['public.ducklake_data_file']}
        row=next(r for r in phase['after']['catalog']['public.ducklake_file_column_stats'] if r['data_file_id'] not in old)
        row['null_count']+=1
    check('catalog-nonnull-count',wrong_null_count)
    def wrong_stopped(v):
        response=v['steps'][11]['response'];reports=[json.loads(s) for s in response['stdout'].splitlines()]
        reports[-1]['owner_epoch']='0';response['stdout']='\n'.join(map(json.dumps,reports))+'\n'
    check('ordinary-stopped-identity',wrong_stopped)
    # A one-row unrelated mutation in every table is tested explicitly. This is
    # a table-preservation check, not a claim of exhaustive semantic coverage.
    for table in sorted(value['initial_state']['control']):
        def perturb(v,t=table):
            rows=v['steps'][8]['after']['control'][t]
            if rows:rows[0]['xmin']='0'
            else:rows.append(dict(unexpected=True))
        check('unrelated-table-'+table,perturb)
    return dict(status='PASS',count=len(rejected),rejected=rejected)


def live2_observations(model,phase,reports):
    snapshots=phase['fixture']['snapshots'];assert [s['stage'] for s in snapshots]==['started','renewed','before-stop']
    started=snapshots[0]['control'];wanted=copy.deepcopy(model.pg)
    control=only(started['owner_epochs'],role='control',epoch=model.control['epoch'])
    target=only(started['owner_epochs'],role='ingest',epoch=2)
    role=only(started['role_instances'],role_instance_id=control['role_instance_id'])
    for name,epoch,actual in [('control',control['epoch'],control),('ingest',2,target)]:
        row=only(wanted['owner_epochs'],role=name,epoch=epoch)
        assert row['acquired_at_us']<actual['lease_expires_at_us']<=row['lease_expires_at_us']
        clock(actual['lease_expires_at_us']-6_000_000,phase['window'])
        assert re.fullmatch('[1-9][0-9]*',actual['xmin'])
        row.update(lease_expires_at_us=actual['lease_expires_at_us'],xmin=actual['xmin'])
        if name=='control':row.update(state='active',released_at_us=None,exclusion_method=None,exclusion_digest=None)
    only(wanted['role_instances'],role_instance_id=role['role_instance_id']).update(retired_at_us=None,xmin=role['xmin'])
    same(wanted,started);previous=started
    for snap in snapshots[1:]+[dict(control=phase['after']['control'],stage='final')]:
        wanted=copy.deepcopy(started)
        leases(wanted,snap['control'],previous,phase['window'],control['epoch'],2,stopped=snap['stage']=='final')
        same(wanted,snap['control']);previous=snap['control']
    assert all(s['inputs']==phase['before']['inputs'] for s in snapshots)
    initial=max(r['lease_expires_at_us'] for r in (control,target))
    assert phase['fixture']['initial_deadline_us']==initial and snapshots[-1]['before_us']>initial
    intent=only(model.pg['initial_ingest_successions']);activation=only(model.pg['initial_ingest_activations'])
    issued,cut,digest=Model(phase['before']['control'],phase['before']['native'],phase['before']['inputs'],phase['window']).closure(1)
    assert reports[0]==dict(format='bigbrotr-initial-successor/1',state='activated',dataset_id=model.ds['dataset_id'][2:],
        owner_resource_id=model.resource[2:],producer_slot_id=model.slot[2:],producer_epoch='2',process_incarnation=intent['target_incarnation'][2:],
        topology_generation='1',config_revision='1',policy_revision='1',config_sha256=intent['config_digest'][2:],policy_sha256=intent['policy_digest'][2:],
        outstanding_request_limit='32',created_at_us=str(intent['created_at_us']),initial_lease_expires_at_us=str(intent['lease_expires_at_us']),
        activated_at_us=str(activation['activated_at_us']),predecessor=dict(producer_epoch='1',issued_through=str(issued),reserved_through=str(issued),
            resolved_requests=str(issued),last_complete_sequence=str(cut),sha256=digest),native_profile=1,native_allowance_bytes=128*1024**2,
        acquisition='not-started',ready=False,listeners=[dict(kind='private-session-http',host='127.0.0.1',port=18081,profile='initial-source-commands')])
    assert reports[1]==dict(format='bigbrotr-initial-successor/1',state='stopped',native='closed',owner_release='committed',
        source_closure='not-performed',producer_epoch='2',ready=False)


def ordinary_reports(model,phase):
    started,stopped=[json.loads(line) for line in phase['response']['stdout'].splitlines()]
    control=model.control;catalog=only(model.pg['consumers'])['consumer_id'][2:]
    assert started==dict(format='bigbrotr-control-runtime/1',state='idle',role='control',dataset_id=model.ds['dataset_id'][2:],
        process_incarnation=control['process_incarnation'][2:],owner_resource_id=control['resource_id'][2:],owner_epoch=str(control['epoch']),
        config_revision='1',policy_revision='1',archive_intents=dict(state='observed',catalog_id=catalog,issued_through='7',observed_through='7',
            pages=1,resolution='not-performed',publication_authority='not-granted'),listeners=[dict(kind='private-session-http',host='127.0.0.1',
            port=18081,profile='trusted-host-loopback',private_assets=None)],native_stores=[],cursor_keyring=dict(state='not-selected',cursor_authority='not-granted'),
        export_execution='selected-under-qualification',authority_scope='Control custody; each private action and export independently reauthorized')
    assert stopped==dict(format='bigbrotr-control-runtime/1',state='stopped',role='control',owner_epoch=str(control['epoch']),policy_revision='1',release='committed')
    artifact=only(model.pg['artifacts'])['artifact_id'][2:];files=phase['artifact_files'];contents=phase['artifact_contents']
    paths={'destination.json','.artifact-'+artifact+'.lock',artifact,artifact+'/events.jsonl',artifact+'/manifest.json'}
    assert set(files)==paths and set(contents)==paths-{artifact}
    for path,entry in files.items():
        assert entry['mode']==(0o700 if path==artifact else 0o600)
        assert type(entry['inode']) is int and entry['inode']>0 and entry['links']>0
        if path!=artifact:assert entry['links']==1 and sha(bytes.fromhex(contents[path]))==entry['sha256']
        else:assert entry['sha256'] is None
    assert contents['.artifact-'+artifact+'.lock']==''
    assert sha(bytes.fromhex(contents['destination.json']))==only(model.pg['export_jobs'])['destination_binding_sha256'][2:]
