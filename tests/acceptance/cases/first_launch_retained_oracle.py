"""Literal finite64 boundary: independent input/native/typed progress science."""
import hashlib
import json
import re
from pathlib import Path, PurePosixPath
import tomllib

from cases import first_launch_index_oracle as index
from cases import first_launch_index_live_oracle as live
from cases import first_launch_index_topology_oracle as topology
from cases.first_launch_index_commit_oracle import control_exclusion
from cases import first_launch_restart as source


def boundary(state, wire, event, *, proof=False):
    """The live gate must precede proof release; a final row cannot replace it."""
    assert type(proof) is bool
    producer, = state['producer_epochs']
    assert producer['producer_epoch']==1 and producer['reserved_through_request']==64
    assert producer['outstanding_request_limit']==32
    for table in ('producer_requests','producer_outcomes'):
        assert sorted(row['request_sequence'] for row in state[table])==list(range(1,65))
        assert all(row['producer_epoch']==1 for row in state[table])
    progress, = state['acquisition_progress']
    expected = dict(progress_revision=62 if proof else 61,classified_events=62 if proof else 61,
        successful_events=62 if proof else 61,rejected_events=0,
        request_id='\\x'+wire['request'][1].encode().hex(),event_ordinal=1,wire_sequence=1,
        input_sha256='\\x'+hashlib.sha256(json.dumps(event,ensure_ascii=False,separators=(',',':')).encode()).hexdigest(),
        classification=5 if proof else 2,mutation_request=None if proof else 64)
    index.exact({key:progress[key] for key in expected},expected)
    head, = state['producer_checkpoints']
    assert head['complete_through_request']==64 and head['checkpoint_revision']==(65 if proof else 64)
    assert head['progress_jobs']==1 and head['dependency_count']==312 and head['xmin']==progress['xmin']
    if not proof:
        assert head['xmin']==index.only(state['producer_outcomes'],request_sequence=64)['xmin']
    return head


def wires_checked(wires, events):
    assert 22<=len(wires)<=128
    previous=None;nonces=set();proof=[];empties=0
    for number,wire in enumerate(wires,1):
        assert wire['number']==number and wire['request'][0]=='REQ'
        assert wire['request'][2]=={'kinds':[1],'limit':3}
        ident=wire['request'][1]
        assert re.fullmatch('[0-9a-f]{48}',ident) and int(ident[:32],16)
        assert ident[:32] not in nonces;nonces.add(ident[:32])
        assert wire['connected_at_us']<=wire['request_at_us']<=wire['closed_at_us']
        assert previous is None or previous<=wire['connected_at_us']
        previous=wire['closed_at_us']
        assert 0<=wire['closed_at_us']-wire['request_at_us']<3_000_000
        assert type(wire['clock_resolution_us']) is int and wire['clock_resolution_us']>=1
        if number<=20:
            phase,values='admission',events[(number-1)*3:number*3]
        elif number==21:
            phase,values='saturation',[events[60],events[61],events[0]]
        elif wire.get('retained_phase')=='proof-only':
            phase,values='proof-only',[events[0]];proof.append(wire)
        elif wire.get('retained_phase')=='empty':
            phase,values='empty',[];empties+=1
        else:
            assert wire['retained_quiet'] is True and wire['frames']==[] and wire['completion']=='quiet-eof'
            continue
        assert wire['retained_quiet'] is False and wire['retained_phase']==phase
        assert wire['completion']=='request-and-websocket-close'
        assert wire['frames']==['signed']*len(values)+['EOSE']
        assert wire['event_ids']==[event['id'] for event in values]
        assert wire['event_json_sha256']==[hashlib.sha256(json.dumps(event,ensure_ascii=False,
            separators=(',',':')).encode()).hexdigest() for event in values]
        assert wire['request_at_us']<=wire['sent_at_us']<=wire['closed_at_us']
    assert len(proof)==1 and empties<=100
    return wires[20],proof[0]


def finite_peer(value, wires, events, last_closed_sha256):
    """Finite peer end is a source boundary, never quiet cancellation proof."""
    assert set(value)=={'format','reason','connections','proof_sent','last_closed_monotonic_ns',
        'last_closed_sha256','listener_closed','completed_monotonic_ns'}
    assert value['format']=='bigbrotr-retained-relay-completion/1' and value['reason']=='connection_limit'
    assert type(value['connections']) is int and value['connections']==len(wires)==128
    assert value['proof_sent'] is True and value['listener_closed'] is True
    _,proof=wires_checked(wires,events)
    assert proof['completion']=='request-and-websocket-close'
    assert re.fullmatch('[0-9a-f]{64}',last_closed_sha256)
    assert value['last_closed_sha256']==last_closed_sha256
    assert type(value['last_closed_monotonic_ns']) is type(value['completed_monotonic_ns']) is int
    assert 0<value['last_closed_monotonic_ns']==wires[-1]['closed_monotonic_ns']<=value['completed_monotonic_ns']


def progress(state, wanted, events, wire, control, completed, *, proof):
    from installed_progress_context_oracle import typed
    from installed_sync_oracle import decode
    head=boundary(state,wire,events[0 if proof else 60],proof=proof)
    producer, = state['producer_epochs']
    job, = state['jobs']; detail, = state['acquisition_jobs']
    assert job['state']=='queued' and job['checkpoint_revision']==0 and job['request_key']=='\\x'+'b1'*16
    assert detail['job_id']==job['job_id']
    actual, = state['acquisition_progress']
    common={key:producer[key] for key in ('dataset_id','producer_slot_id','producer_epoch')}
    expected=dict(common,job_id=job['job_id'],scope_id=detail['scope_id'],
        progress_revision=62 if proof else 61,request_id='\\x'+wire['request'][1].encode().hex(),
        event_ordinal=1,wire_sequence=1,input_sha256='\\x'+source.sha(json.dumps(
            events[0 if proof else 60],ensure_ascii=False,separators=(',',':')).encode()),
        classification=5 if proof else 2,mutation_request=None if proof else 64,
        classified_events=62 if proof else 61,successful_events=62 if proof else 61,rejected_events=0,
        observed_at_us=actual['observed_at_us'],clock_resolution_us=wire['clock_resolution_us'],
        clock_anomaly_flags=4 if wire['clock_resolution_us']>1 else 0,xmin=actual['xmin'])
    dataset=index.raw(common['dataset_id'],16)
    domains={r['dictionary_kind']:index.raw(r['allocation_domain'],16) for r in state['allocation_domains']}
    ref=lambda kind,ordinal=1:[dataset,kind,domains[kind],ordinal]
    if proof:
        intent=typed.certification_digest(events[0],ref(4,3),ref(2),ref(2),
            [ref(1),ref(3),ref(5)],expected,wire).hex()
        assert all(actual['xmin']!=r['xmin'] for r in state['producer_outcomes'])
    else:
        intent=source.sha(wanted['inputs'][63])
        observation=decode(wanted['inputs'][63].split(b'\0',1)[1])[3][5]
        assert [expected['observed_at_us'],expected['clock_resolution_us'],expected['clock_anomaly_flags']]==[
            observation[0],observation[2][1],observation[2][3]]
    expected['intent_digest']='\\x'+intent
    source.clock(head['committed_at_us'],wire['sent_at_us'],completed)
    assert expected['observed_at_us']<=head['committed_at_us']<control['lease_expires_at_us']
    values={(cf,key.hex()):value.hex() for cf,key,value in wanted['native']}
    dependencies=[]
    receipts=[decode(receipt.split(b'\0',1)[1])[5] for receipt in wanted['receipts']]
    for n in range(1,313):
        key=b'\x01\x20'+dataset+index.be(0,4)+index.be(1)+index.be(n)
        value=bytes.fromhex(values['facts',key.hex()])
        reference=[[dataset,0,1,n],value[2:34]]
        first=next(k for k,refs in enumerate(receipts,1) if reference in refs)
        dependencies.append(dict(common,source_shard=0,source_epoch=1,source_sequence=n,
            payload_digest='\\x'+value[2:34].hex(),
            xmin=index.only(state['producer_outcomes'],request_sequence=first)['xmin']))
    expected_head=dict(common,progress_format=1,manifest_sha256='\\x'+source.sha(bytes.fromhex(values['default','0101'])),
        checkpoint_revision=65 if proof else 64,complete_through_request=64,progress_jobs=1,dependency_count=312,
        issuer_resource=control['resource_id'],issuer_epoch=control['epoch'],issuer_incarnation=control['process_incarnation'],
        committed_at_us=head['committed_at_us'],xmin=head['xmin'],checkpoint_digest='\\x'+'00'*32)
    expected_head['checkpoint_digest']='\\x'+typed.head_digest(expected_head,[expected],dependencies,
        lambda row:typed.native_reference(row,values)).hex()
    return typed.exact_snapshot(head,state['acquisition_progress'],sorted(state['producer_checkpoint_dependencies'],
        key=lambda row:(row['source_shard'],row['source_epoch'],row['source_sequence'])),
        expected_head,[expected],dependencies,values)


def science(state, before_proof, report, events, wires, dump, store, completed):
    from installed_archive_oracle import expected
    from installed_sync_oracle import decode
    from bootstrap_oracle import cbor
    saturation,proof=wires_checked(wires,events)
    original=source.originals(store,report['producer_slot_id'],1,64)
    actual=[(r['cf'],bytes.fromhex(r['key']),bytes.fromhex(r['value'])) for r in dump['rows']]
    assert dump['event']=='dump-state' and dump['sequence']==1081
    assert len(actual)==829 and actual==sorted(actual) and len({(cf,key) for cf,key,_ in actual})==829
    facts={int.from_bytes(key[-8:],'big'):value for cf,key,value in actual if cf=='facts' and key[:2]==b'\x01\x20'}
    def recorded(n):
        return decode(facts[n][34:].split(b'\0',1)[1])[4]
    job,_,attribution=source.attribution(state,'b1')
    dictionary_time=source.clock(recorded(4),job['created_at_us'],wires[0]['connected_at_us'])
    parameters=[];bindings={}
    for n,event in enumerate(events[:61]):
        wire=wires[n//3] if n<60 else saturation
        ordinal=n%3+1
        received=decode(original[n+3].split(b'\0',1)[1])[3][5][0]
        source.clock(received,wire['sent_at_us'],wire['closed_at_us'])
        at=source.clock(recorded(8+n*5),received,completed)
        body=[bytes.fromhex(event['id']),bytes.fromhex(event['pubkey']),event['created_at'],event['kind'],event['tags'],event['content']]
        digest=source.sha(b'bigbrotr/body/1\0'+cbor(body))
        parameters.append((event,received,at,wire['clock_resolution_us'],digest))
        bindings[n+4]=source.request_binding(event,wire,ordinal)
    first=parameters[0]
    wanted=expected(state,report,events[0],dictionary_time,first[1],first[2],first[3],
        native_options=dict(acknowledge=True,retained_boundary=True,additional_events=parameters[1:]))
    index.exact([(cf,k.hex(),v.hex()) for cf,k,v in actual],
                [(cf,k.hex(),v.hex()) for cf,k,v in wanted['native']])
    assert original==wanted['inputs'], 'all64 independently encoded original inputs'
    control=index.only(state['owner_epochs'],role='control')
    manifest=next(v for cf,k,v in wanted['native'] if cf=='default' and k==b'\x01\x01')
    source.requests(state,1,manifest,wanted['inputs'],wanted['receipts'],control,bindings,attribution)
    progress(before_proof,wanted,events,saturation,control,completed,proof=False)
    progress(state,wanted,events,proof,control,completed,proof=True)
    for table in ('producer_requests','producer_outcomes','producer_epochs','producer_checkpoint_dependencies',
                  'jobs','acquisition_jobs','job_admissions'):
        index.exact(before_proof[table],state[table])
    for batch in wanted['batches']:
        batch.update(epoch=1,topology=0)
    return wanted

# Reclamation extension of this same frozen64 workload. Product observations
# supply clocks and fresh identities; expected bytes remain literal encodings.
REFUSALS={
    'refuse-open-producer':('failed','producer operation unavailable: Control(InvalidMetadata)'),
    'refuse-corrupt-original':('failed','producer operation unavailable: Native(Input(Codec(IncompatibleSchema)))'),
    'refuse-reclamation-after-successor':('none','ingest registration failed: InvalidInput'),
    # Deliberately restoring only the old head over newer progress children
    # fails the retained SQL digest check before direct floor/head comparison.
    'mixed-checkpoint-refusal':('failed','producer operation unavailable: Progress(Backend)'),
}


def reclamation_refusal(label,stdout,stderr):
    claim,message=REFUSALS[label]
    assert stdout=='' and stderr=='error: '+message+'\n'
    return claim

def reclamation_fault(fault,terminal,mode):
    assert mode in ('before-forward','after-complete')
    assert terminal['exit']==0 and not terminal['stderr'] and not terminal['overflow']
    events=[json.loads(line) for line in terminal['stdout'].splitlines()]
    ended, = [r for r in events if r['event']=='terminal']
    observed, = [r for r in events if r['event']=='fault']
    index.exact(observed,fault)
    for event in (fault,ended):
        assert event['target']=='control-closed-reclamation' and event['mode']==mode
        assert event['profile']=='retained-catch-up'
        index.exact([event[k] for k in ('data_queries','data_commits','server_complete','client_complete','faults')],
            [1,1,int(mode=='after-complete'),0,1])
    assert ended['unexpected']==0

def retired_initial_native(science,dump):
    """One close-marker write over already authenticated initial science."""
    wanted={(cf,key.hex()):value.hex() for cf,key,value in science['native']}
    assert len(wanted)==829
    marker, = [key for cf,key in wanted if cf=='receipts' and key.startswith('0131')]
    wanted['receipts',marker]=(b'\x02\x31'+index.be(64)*2+index.be(0)+b'\x01').hex()
    expected=dict(event='dump-state',sequence=1082,rows=[dict(cf=cf,key=key,value=value)
        for (cf,key),value in sorted(wanted.items())])
    index.exact(dump,expected)


def live_compacted_publication(state):
    """Published cut10 is a consumer bound; an open source has no terminal cut."""
    live.live_owner_bindings(state)
    assert index.only(state['owner_epochs'],role='index',epoch=2)['state']=='active'
    consumer=index.only(state['consumers'],consumer_role='index')
    assert consumer['current_revision']==68 and consumer['current_topology_generation']==1
    bounds=sorted((r['owner_epoch'],r['last_complete_sequence']) for r in state['consumer_frontiers']
        if r['consumer_id']==consumer['consumer_id'] and r['frontier_revision']==68)
    assert bounds==[(1,312),(2,10)]
    assert index.only(state['source_streams'],owner_epoch=1)['state']=='closed'
    source=index.only(state['source_streams'],owner_epoch=2)
    assert source['state']=='open' and source['last_complete_sequence'] is None
    assert index.only(state['producer_epochs'],producer_epoch=2)['state']=='open'


def control_only(before,after,*,claim='released',backend_pid=None):
    """Exact science plus the ordinary Control claim/release/fencing protocol."""
    assert claim in ('released','failed','none') and set(before)==set(after)
    for table in before.keys()-{'owner_epochs','role_instances'}:
        index.exact(before[table],after[table])
    key=lambda row:(row['resource_id'],row['epoch'])
    previous={key(row):row for row in before['owner_epochs']}
    current={key(row):row for row in after['owner_epochs']}
    assert len(previous)==len(before['owner_epochs']) and len(current)==len(after['owner_epochs'])
    assert previous.keys()<=current.keys()
    owners=[row for ident,row in current.items() if ident not in previous]
    assert len(owners)==(0 if claim=='none' else 1)
    assert all(row['role']=='control' for row in owners)
    if owners:
        prior=max((r for r in before['owner_epochs'] if r['role']=='control'),key=lambda r:r['epoch'])
        owner=owners[0]
        changed={'epoch','role_instance_id','process_incarnation','acquired_at_us','lease_expires_at_us',
            'state','released_at_us','exclusion_method','exclusion_digest','control_backend_pid','control_backend_start','xmin'}
        index.exact({k:v for k,v in owner.items() if k not in changed},
            {k:v for k,v in prior.items() if k not in changed})
        assert owner['resource_id']==prior['resource_id'] and owner['dataset_id']==prior['dataset_id']
        assert owner['epoch']==prior['epoch']+1
        assert owner['process_incarnation'] not in {r['process_incarnation'] for r in before['owner_epochs']}
        assert owner['index_publication_protocol'] is None and owner['control_fence_version']==1
        assert type(owner['control_backend_pid']) is int and owner['control_backend_pid']>0
        assert isinstance(owner['control_backend_start'],str) and owner['control_backend_start']
        if backend_pid is not None:assert owner['control_backend_pid']==backend_pid
        assert prior['acquired_at_us']<=owner['acquired_at_us']<owner['lease_expires_at_us']
        assert owner['state']==('released' if claim=='released' else 'active')
        if claim=='released':
            assert type(owner['released_at_us']) is int and owner['released_at_us']>=owner['acquired_at_us']
            assert owner['exclusion_method']=='storage_fenced' and owner['exclusion_digest']==control_exclusion(owner,owner)
        else:assert owner['released_at_us'] is owner['exclusion_method'] is owner['exclusion_digest'] is None
    fenced={}
    for ident,old in previous.items():
        actual=current[ident]
        if actual==old:continue
        assert old['role']=='control' and old['state']!='released' and len(owners)==1
        successor=owners[0]
        assert successor['resource_id']==old['resource_id'] and successor['epoch']==old['epoch']+1
        assert actual['state']=='released' and actual['exclusion_method']=='storage_fenced'
        assert old['lease_expires_at_us']<=actual['released_at_us']<=successor['acquired_at_us']
        digest=(b'bigbrotr/control-exclusion/1\0'+index.raw(old['dataset_id'],16)+index.raw(old['resource_id'],16)
            +index.be(old['epoch'])+index.raw(old['process_incarnation'],16)+index.raw(successor['process_incarnation'],16))
        assert actual['exclusion_digest']=='\\x'+hashlib.sha256(digest).hexdigest()
        changed={'state','released_at_us','exclusion_method','exclusion_digest','xmin'}
        index.exact({k:v for k,v in old.items() if k not in changed},{k:v for k,v in actual.items() if k not in changed})
        assert actual['xmin']!=old['xmin']
        fenced[old['role_instance_id']]=actual
    assert len(fenced)<=1
    if owners:
        assert set(fenced)==(set() if prior['state']=='released' else {prior['role_instance_id']})
    old_roles={r['role_instance_id']:r for r in before['role_instances']}
    new_roles={r['role_instance_id']:r for r in after['role_instances']}
    assert old_roles.keys()<=new_roles.keys()
    assert fenced.keys()<=old_roles.keys()
    for ident,old in old_roles.items():
        actual=new_roles[ident]
        if old==actual and ident not in fenced:continue
        assert old!=actual and ident in fenced and old['retired_at_us'] is None
        assert fenced[ident]['released_at_us']<=actual['retired_at_us']<=owners[0]['acquired_at_us']
        assert actual['xmin']==fenced[ident]['xmin']
        index.exact({k:v for k,v in old.items() if k not in ('retired_at_us','xmin')},
            {k:v for k,v in actual.items() if k not in ('retired_at_us','xmin')})
    assert new_roles.keys()-old_roles.keys()=={r['role_instance_id'] for r in owners}
    for owner in owners:
        role=new_roles[owner['role_instance_id']]
        assert role['dataset_id']==owner['dataset_id'] and role['role']=='control'
        assert role['created_at_us']<=owner['acquired_at_us']
        if claim=='released':
            assert type(role['retired_at_us']) is int and role['retired_at_us']>=owner['released_at_us'] and role['xmin']==owner['xmin']
        else:assert role['retired_at_us'] is None
    return owners[0] if owners else None


def reclamation_floor(state,science):
    producer=index.only(state['producer_epochs'],producer_epoch=1)
    head=index.only(state['producer_checkpoints'],producer_epoch=1)
    assert producer['state']=='retired' and producer['reserved_through_request']==64
    assert head['progress_format']==1 and head['checkpoint_revision']==65 and head['complete_through_request']==64
    digest=source.closure(science['manifest'],producer['producer_slot_id'],1,science['inputs'],science['receipts'],312)
    assert producer['closure_digest']=='\\x'+digest
    return (b'\x03\x31'+index.be(64)*3+b'\x01'+hashlib.sha256(science['manifest']).digest()+index.be(312)
        +bytes.fromhex(digest)+index.be(65)+index.raw(head['checkpoint_digest'],32))


def compacted_history_control(before,after,startup):
    """One signed history login/logout, completed startup and two joined readers."""
    from cases.first_launch_index_restart_oracle import unchanged_except
    from installed_progress_replay import xid
    assert startup is not None
    readers=('snapshot_leases','archive_snapshot_pins','retention_claims')
    changed={'browser_sessions','login_buckets','management_audit',*readers}
    for state in (startup,after):
        unchanged_except(before,state,changed|{'owner_epochs','role_instances'})
    def authority_view(state):return dict(state,**{table:before[table] for table in changed})
    active=control_only(before,authority_view(startup),claim='failed')
    owner=control_only(before,authority_view(after))
    allowed={'state','released_at_us','exclusion_method','exclusion_digest','lease_expires_at_us','xmin'}
    index.exact({k:v for k,v in active.items() if k not in allowed},{k:v for k,v in owner.items() if k not in allowed})
    assert active['lease_expires_at_us']<=owner['lease_expires_at_us']
    role=index.only(startup['role_instances'],role_instance_id=owner['role_instance_id'])
    ended_role=index.only(after['role_instances'],role_instance_id=owner['role_instance_id'])
    index.exact(ended_role,dict(role,retired_at_us=ended_role['retired_at_us'],xmin=ended_role['xmin']))
    old_session=index.only(before['browser_sessions']);session=index.only(startup['browser_sessions'])
    final_session=index.only(after['browser_sessions'])
    account=index.only(before['local_accounts'])
    assert old_session['revoked_at_us'] is not None and session['revoked_at_us'] is None
    assert old_session['session_id']!=session['session_id']
    assert session['principal_id']==old_session['principal_id']==account['principal_id']
    assert session['dataset_id']==owner['dataset_id']
    assert owner['acquired_at_us']<=session['issued_at_us']<=session['last_seen_at_us']<=final_session['last_seen_at_us']<=final_session['revoked_at_us']<=owner['released_at_us']
    source.clock(session['last_seen_at_us'],session['issued_at_us'],active['lease_expires_at_us']-1)
    assert session['credential_revision']==account['credential_revision'] and session['authentication_revision']==account['authentication_revision']
    index.exact(final_session,dict(session,last_seen_at_us=final_session['last_seen_at_us'],
        idle_expires_at_us=min(session['expires_at_us'],final_session['last_seen_at_us']+session['idle_window_us']),
        revoked_at_us=final_session['revoked_at_us'],xmin=final_session['xmin']))
    for observed in (session,final_session):
        source.clock(observed['idle_window_us'],1,2**63-1)
        assert observed['idle_expires_at_us']==min(observed['expires_at_us'],observed['last_seen_at_us']+observed['idle_window_us'])
        assert observed['last_seen_at_us']<observed['idle_expires_at_us']<=observed['expires_at_us']
    # The mandatory pre-setup GET /me is the first touch after login. It cannot
    # revive either initial expiry, and its committed UPDATE has its own xmin
    # even when the server's microsecond timestamps happen to coincide.
    assert session['last_seen_at_us']<min(session['expires_at_us'],session['issued_at_us']+session['idle_window_us'])
    known_xmins={r['xmin'] for rows in before.values() for r in rows if 'xmin' in r}
    assert xid(session['xmin']) not in known_xmins and xid(final_session['xmin']) not in known_xmins
    index.exact(after['login_buckets'],startup['login_buckets'])
    assert len(before['login_buckets'])==len(startup['login_buckets'])==2
    for kind,(bucket,maximum) in source.login_buckets().items():
        old=index.only(before['login_buckets'],kind=kind);row=index.only(startup['login_buckets'],kind=kind)
        assert old['bucket']==bucket and old['dataset_id']==owner['dataset_id']
        index.exact(row,dict(old,attempts=row['attempts'],window_started_at_us=row['window_started_at_us'],xmin=row['xmin']))
        if row['window_started_at_us']==old['window_started_at_us']:
            assert row['attempts']==old['attempts']+1<=maximum
        else:
            assert row['attempts']==1 and row['window_started_at_us']-old['window_started_at_us']>=60_000_000
            source.clock(row['window_started_at_us'],owner['acquired_at_us'],session['issued_at_us'])
    for previous,current,action in ((before,startup,'browser.session.issue'),(startup,after,'browser.session.revoke')):
        old=previous['management_audit'];rows=current['management_audit']
        assert all(r in rows for r in old) and len(rows)==len(old)+1
        audit,=[r for r in rows if r not in old]
        assert any(index.raw(audit['audit_id'],16)) and audit['audit_id'] not in {r['audit_id'] for r in old}
        index.exact(audit,dict(audit_id=audit['audit_id'],dataset_id=owner['dataset_id'],actor_id=account['principal_id'],
            action=action,outcome='accepted',created_at_us=audit['created_at_us'],config_revision=1,policy_revision=1,
            subject_id=session['session_id'],reason=None,requester_database_role=None,requester_os_uid=None,xmin=audit['xmin']))
        if action=='browser.session.issue':
            source.clock(audit['created_at_us'],session['issued_at_us'],session['last_seen_at_us'])
            assert xid(audit['xmin']) not in known_xmins
            # Fresh fixed-profile PostgreSQL has no xid wrap: login completes
            # before /me begins its separate transaction, then logout follows.
            assert int(xid(audit['xmin']))<int(xid(session['xmin']))<int(xid(final_session['xmin']))
        else:
            source.clock(audit['created_at_us'],final_session['revoked_at_us'],owner['released_at_us'])
            assert audit['xmin']==final_session['xmin']
    prior={r['lease_id'] for r in before['snapshot_leases']}
    initial={r['lease_id'] for r in startup['snapshot_leases']}-prior
    final={r['lease_id'] for r in after['snapshot_leases']}-prior
    assert 1<=len(initial)<=2 and len(final-initial)==2
    catalog=index.only(after['consumers'],consumer_role='archive')['consumer_id']
    for state,new_ids in ((startup,initial),(after,final)):
        for table in readers:
            index.exact([r for r in state[table] if r['lease_id'] not in new_ids],before[table])
            assert len([r for r in state[table] if r['lease_id'] in new_ids])==len(new_ids)*(2 if table=='retention_claims' else 1)
        for ident in new_ids:
            lease=index.only(state['snapshot_leases'],lease_id=ident);pin=index.only(state['archive_snapshot_pins'],lease_id=ident)
            claims=sorted((r for r in state['retention_claims'] if r['lease_id']==ident),key=lambda r:r['owner_epoch'])
            index.exact([lease[k] for k in ('dataset_id','consumer_id','frontier_revision','topology_generation',
                'holder_kind','holder_job_id','holder_role_instance_id','holder_role','holder_incarnation','archive_pin_protocol',
                'state','lease_revision','release_reason')],
                [owner['dataset_id'],catalog,4,1,'role_instance',None,owner['role_instance_id'],'control',owner['process_incarnation'],1,'released',2,'reader_joined'])
            assert owner['acquired_at_us']<=lease['acquired_at_us']<=lease['released_at_us']<=owner['released_at_us']
            assert lease['updated_at_us']==lease['released_at_us'] and lease['acquired_at_us']<lease['expires_at_us']
            index.exact([pin[k] for k in ('dataset_id','consumer_id','frontier_revision','purpose','issuer_role',
                'issuer_resource','issuer_epoch','issuer_incarnation')],
                [owner['dataset_id'],catalog,4,'reader','control',owner['resource_id'],owner['epoch'],owner['process_incarnation']])
            index.exact([[r[k] for k in ('dataset_id','topology_generation','shard_id','owner_epoch','first_sequence','last_sequence',
                'purpose','state','release_reason')] for r in claims],
                [[owner['dataset_id'],1,0,epoch,1,cut,'reader','released','reader_joined'] for epoch,cut in ((1,312),(2,10))])
            assert all(lease['acquired_at_us']<=r['created_at_us']<=r['released_at_us']<=lease['released_at_us']
                and r['xmin']==lease['xmin'] for r in claims)


def reclaimed(before,after,native_before,native_after,science,report,*,retry=False):
    control_only(before,after)
    floor=reclamation_floor(before,science)
    slot=index.raw(index.only(before['producer_epochs'],producer_epoch=1)['producer_slot_id'],16)
    marker=b'\x01\x31'+slot+index.be(1)
    wanted={(r['cf'],r['key']):r['value'] for r in native_before['rows']}
    if not retry:
        retired_initial_native(science,native_before)
        assert wanted['receipts',marker.hex()]==(b'\x02\x31'+index.be(64)*2+index.be(0)+b'\x01').hex()
        for n in range(1,65):
            key=(b'\x01\x30'+slot+index.be(1)+index.be(n)).hex()
            assert wanted.pop(('receipts',key))==science['receipts'][n-1].hex()
        wanted['receipts',marker.hex()]=floor.hex()
    expected=[dict(cf=cf,key=key,value=value) for (cf,key),value in sorted(wanted.items())]
    index.exact(native_after['rows'],expected)
    assert native_after['sequence']==native_before['sequence']+(0 if retry else 65)
    assert len(native_after['rows'])==765
    expected_report=dict(format='bigbrotr-ingest-recovery/1',state='initial-reclaimed',
        dataset_id=index.only(before['datasets'])['dataset_id'][2:],
        owner_resource_id=index.only(before['producer_epochs'],producer_epoch=1)['owner_resource_id'][2:],
        producer_slot_id=slot.hex(),producer_epoch='1',retired_requests='64',removed_input_files='0' if retry else '64',
        native_sequence=str(native_after['sequence']),recovery_basis='closed-producer-floor-v3',native='compacted',
        owner_release='committed',ready=False)
    index.exact(report,expected_report)
    return floor


def successor_owner(bridge,after,manifest,handoff,job):
    """Anchor historical admission to the zero-request bridge, not recovery."""
    live.live_owner_bindings(bridge)
    for table in ('producer_requests','producer_outcomes','producer_checkpoints'):
        assert not [r for r in bridge[table] if r['producer_epoch']==2]
    for table in ('initial_ingest_activations','initial_ingest_successions'):
        index.exact(bridge[table],after[table])
    activation=index.only(bridge['initial_ingest_activations'])
    intent=index.only(bridge['initial_ingest_successions'])
    dataset=index.only(bridge['datasets'])
    for key in ('dataset_id','producer_slot_id','producer_epoch','owner_resource_id','target_epoch',
        'target_incarnation','target_role_instance_id','config_revision','config_digest','policy_revision',
        'policy_digest','previous_manifest_sha256','target_manifest_sha256','handoff_sha256',
        'prepared_by_resource','prepared_by_epoch','prepared_by_incarnation'):
        index.exact(activation[key],intent[key])
    for field in ('resource','epoch','incarnation'):
        index.exact(activation['prepared_by_'+field],activation['activated_by_'+field])
    assert activation['producer_epoch']==activation['target_epoch']==2
    assert intent['owner_role']=='ingest' and intent['prepared_by_role']=='control'
    assert activation['dataset_id']==dataset['dataset_id']==job['dataset_id']
    for kind in ('config','policy'):
        assert activation[kind+'_revision']==dataset[kind+'_revision']==1
    assert activation['target_manifest_sha256']=='\\x'+source.sha(manifest)
    assert activation['previous_manifest_sha256']=='\\x'+source.sha(index.raw(intent['previous_manifest']))
    assert activation['handoff_sha256']=='\\x'+source.sha(handoff)
    identity=dict(dataset_id=dataset['dataset_id'],role='control',resource_id=activation['activated_by_resource'],
        epoch=activation['activated_by_epoch'],process_incarnation=activation['activated_by_incarnation'])
    original=index.only(bridge['owner_epochs'],**identity)
    owner=index.only(after['owner_epochs'],**identity)
    assert owner['state']=='released' and owner['exclusion_method']=='storage_fenced'
    for key in ('role_instance_id','acquired_at_us'):
        index.exact(original[key],owner[key])
    assert original['acquired_at_us']<=activation['activated_at_us']<original['lease_expires_at_us']
    # Abandonment cancels the queued job under a later Control; it does not
    # transfer the immutable requests, outcomes or checkpoint to that owner.
    gap=index.only(after['acquisition_gaps'],job_id=job['job_id'])
    stream=index.only(after['source_streams'],shard_id=0,owner_epoch=2)
    assert gap['dataset_id']==stream['dataset_id']==dataset['dataset_id']
    assert gap['producer_slot_id']==activation['producer_slot_id']
    assert gap['producer_epoch']==gap['source_owner_epoch']==2 and gap['shard_id']==0
    assert stream['state']=='closed' and gap['last_complete_sequence']==stream['last_complete_sequence']==10
    assert gap['source_closure_digest']==stream['closure_digest']
    assert gap['owner_role']==job['owner_role']=='control'
    assert gap['reason']==job['reason_code']=='activated_initial_producer_abandoned'
    assert job['state']=='cancelled' and job['job_revision']==gap['previous_job_revision']+2
    assert job['completed_at_us']==gap['abandoned_at_us']
    for field in ('resource','epoch','incarnation'):
        assert gap['previous_owner_'+field]==activation['activated_by_'+field]
        assert gap['issuer_'+field]==job['owner_resource_id' if field=='resource' else 'owner_'+field]
    recovery=index.only(after['owner_epochs'],dataset_id=dataset['dataset_id'],role='control',
        resource_id=gap['issuer_resource'],epoch=gap['issuer_epoch'],process_incarnation=gap['issuer_incarnation'])
    assert recovery['resource_id']==owner['resource_id'] and recovery['epoch']>owner['epoch']
    assert recovery['process_incarnation']!=owner['process_incarnation']
    assert recovery['acquired_at_us']<=gap['abandoned_at_us']<recovery['lease_expires_at_us']
    assert max(stream['closed_at_us'],job['created_at_us'])<=gap['abandoned_at_us']
    return owner


def successor_science(before,after,bridge,first,wires,dump,store,completed):
    from types import SimpleNamespace
    from initial_succession_oracle import successor_manifest
    from installed_family_science import NetworkScience,decode
    from installed_archive_catch_up_model import archive_science
    from transition_replay import native_rows
    from live_replay import OLD_EVENT
    new_event=next(e['event'] for e in json.loads((Path(__file__).resolve().parents[3]/'tests/fixtures/acquisition/selector-events.json').read_text())['events'] if e['name']=='included')
    for table in ('producer_requests','producer_outcomes','producer_checkpoints','producer_checkpoint_dependencies',
        'acquisition_progress','producer_epochs','source_streams','allocation_domains','acquisition_gaps','jobs','acquisition_jobs'):
        assert all(row in after[table] for row in before[table]), 'initial retained PostgreSQL evidence changed: '+table
    assert all(row in after['owner_epochs'] for row in before['owner_epochs'] if row['role']=='ingest' and row['epoch']==1)
    assert first['event']==OLD_EVENT
    wire=source.successor_wire(wires,first['event'],new_event)
    intent=index.only(after['initial_ingest_successions'])
    domains=[index.only(after['allocation_domains'],owner_epoch=2,dictionary_kind=k)['allocation_domain'] for k in range(1,10)]
    source.fresh_successor(before,intent,domains)
    manifest=successor_manifest(first['manifest'],index.raw(intent['target_incarnation'],16),[index.raw(d,16) for d in domains])
    digest=source.closure(first['manifest'],intent['producer_slot_id'],1,first['inputs'],first['receipts'],312)
    handoff=(b'\x01\x0a'+index.raw(intent['producer_slot_id'],16)+index.be(1)+index.be(64)+index.be(312)
        +bytes.fromhex(digest)+index.be(len(first['manifest']),2)+first['manifest']+index.be(len(manifest),2)+manifest)
    assert index.raw(intent['previous_manifest'])==first['manifest'] and index.raw(intent['target_manifest'])==manifest
    assert index.raw(intent['handoff'])==handoff
    values={(r['cf'],r['key']):r['value'] for r in first['compacted_native']['rows']}
    ds=index.raw(index.only(after['datasets'])['dataset_id'],16)
    resource=index.raw(intent['owner_resource_id'],16);slot=index.raw(intent['producer_slot_id'],16)
    values['default','0101']=manifest.hex()
    values['default',(b'\x01\x0a'+ds+resource+index.be(1)).hex()]=handoff.hex()
    values['default',(b'\x01\x02'+ds+bytes(4)+index.be(2)).hex()]=(b'\x01\x02'+index.be(0)).hex()
    values['receipts',(b'\x01\x31'+slot+index.be(2)).hex()]=(b'\x01\x31'+bytes(24)).hex()
    model=SimpleNamespace(ds=index.only(after['datasets']),slot=intent['producer_slot_id'],legacy=intent,inputs={},
        native=dict(event='dump-state',rows=native_rows(values),sequence=first['compacted_native']['sequence']+4))
    plan=dict(epoch=2,topology=1,manifest=manifest,domains=domains)
    science=NetworkScience(model,plan,acknowledge=True,new_event=new_event,endpoint=source.ENDPOINT)
    originals=source.originals(store,slot.hex(),2,3)
    observed={(r['cf'],r['key']):bytes.fromhex(r['value']) for r in dump['rows']}
    def fact(n): return decode(observed['facts',(b'\x01\x20'+ds+bytes(4)+index.be(2)+index.be(n)).hex()][34:].split(b'\0',1)[1])
    job,detail,attribution=source.attribution(after,'f4')
    dictionary=source.clock(fact(1)[4],job['created_at_us'],wire['connected_at_us'])
    received=decode(originals[1].split(b'\0',1)[1])[3][5][0]
    resolution=wire['clock_resolution_us'];source.clock(received,wire['sent_at_us']-resolution,wire['closed_at_us']+resolution)
    recorded=source.clock(fact(6)[4],received,completed)
    old_received=decode(originals[2].split(b'\0',1)[1])[3][5][0]
    source.clock(old_received,received,wire['closed_at_us']+resolution)
    old_recorded=source.clock(fact(10)[4],old_received,completed)
    science.network(True,dictionary,received,recorded,resolution,(old_received,old_recorded))
    assert originals==list(science.inputs.values())
    # The final native closing marker is the existing one-write retirement
    # predecessor; all expected scientific/index/receipt bytes are independent.
    science.values['receipts',(b'\x01\x31'+slot+index.be(2)).hex()]=(b'\x02\x31'+index.be(3)*2+index.be(0)+b'\x01').hex()
    index.exact(dump['rows'],native_rows(science.values))
    assert dump['sequence']==model.native['sequence']+1
    owner=successor_owner(bridge,after,manifest,handoff,job)
    bindings={2:source.request_binding(new_event,wire,1),3:source.request_binding(first['event'],wire,2)}
    source.requests(after,2,manifest,originals,list(science.receipts.values()),owner,bindings,attribution)
    source.successor_progress(after,manifest,science,job,detail,owner,wire,new_event,completed)
    wanted=archive_science(dict(rows=native_rows(science.values)),live_epoch=2,retained_epoch2=True)
    wanted.update(manifest=manifest,inputs=originals,receipts=list(science.receipts.values()),event=new_event)
    return wanted


ARCHIVE_PUBLISHED_ORDINALS=(1,2,*range(4,69))
GROUPED_PUBLISHED_ORDINALS=(1,2,*range(5,70))
GROUPED_IMPORTED_ORDINALS=(1,2,*range(4,70))
ARCHIVE_SCHEDULES={'uninterrupted':(3,65),'unbound-publication-loss':(4,64),
    'grouped-publication-loss':(5,64)}
# These sets describe three declared cuts, never observed inventory lengths.
ARCHIVE_FAULT_PHASES={
    'unique':(4,'after-complete',(1,2,4),(1,2,4)),
    'grouped-first':(4,'before-forward',(1,2),(1,2,4)),
    'grouped-second':(5,'after-complete',(1,2,5),(1,2,4,5)),
}


def failed_archive_output(catalog,files,original):
    """Anchor failed B4 output after exclusion; empty is a valid observation."""
    failed={name:value for name,value in files.items() if name not in original}
    for name,item in failed.items():
        path=PurePosixPath(name)
        assert not path.is_absolute() and '..' not in path.parts
        assert re.fullmatch(r'main/[a-z_]+/prepared-[0-9a-f]{32}(?:/data\.parquet)?',name)
        assert item['mode']==(0o700 if item['sha256'] is None else 0o600)
        assert type(item['inode']) is int and item['inode']>0
        assert type(item['device']) is int and item['device']>=0
        if item['sha256'] is not None:
            assert item['links']==1 and re.fullmatch('[0-9a-f]{64}',item['sha256'])
    failed_output_preserved(catalog,files,failed)
    return failed


def failed_output_preserved(catalog,files,failed):
    tables={r['table_id']:r for r in catalog['public.ducklake_table']}
    registered=set()
    for row in catalog['public.ducklake_data_file']:
        table=tables[row['table_id']]
        schema=index.only(catalog['public.ducklake_schema'],schema_id=table['schema_id'])
        path=PurePosixPath(schema['path'])/table['path']/row['path']
        registered.add(str(path));registered.add(str(path.parent))
    assert not registered.intersection(failed),'failed output became registered science'
    for name,item in failed.items():index.exact(files[name],item)


def archive_file_membership(catalog,science,files,original,*,failed_output=None):
    """Every native batch has exactly its literal family/receipt files."""
    tables={r['table_name']:r for r in catalog['public.ducklake_table']}
    assert set(tables)==set(science['tables'])|{'archive_commits'}
    records=catalog['public.ducklake_data_file']
    paths=set(original)
    if failed_output is not None:
        failed_output_preserved(catalog,files,failed_output)
        paths.update(failed_output)
    assert len({r['data_file_id'] for r in records})==len(records)
    assert sorted(r['snapshot_id'] for r in catalog['public.ducklake_snapshot'])==list(range(len(science['batches'])+2))
    expected_files=0
    for snapshot,batch in enumerate(science['batches'],2):
        counts={'archive_commits':1}
        for name,rows in science['tables'].items():
            selected=[r for r in rows if r['source_record_id']['owner_epoch']==batch['epoch']
                and batch['first']<=r['source_record_id']['sequence']<=batch['last']]
            if selected:counts[name]=len(selected)
        selected=[r for r in records if r['begin_snapshot']==snapshot]
        assert len(selected)==len(counts)
        assert {r['table_id'] for r in selected}=={tables[name]['table_id'] for name in counts}
        tx=index.only(catalog['public.ducklake_snapshot'],snapshot_id=snapshot)['xmin']
        for name,count in counts.items():
            table=tables[name];row=index.only(selected,table_id=table['table_id'])
            assert row['end_snapshot'] is None and row['path_is_relative'] is True and row['file_format']=='parquet'
            assert row['record_count']==count and row['xmin']==tx
            assert re.fullmatch(r'prepared-[0-9a-f]{32}/data\.parquet',row['path'])
            assert 0<row['footer_size']<row['file_size_bytes']<=16*1024**2
            schema=index.only(catalog['public.ducklake_schema'],schema_id=table['schema_id'])
            path=PurePosixPath(schema['path'])/table['path']/row['path']
            assert not path.is_absolute() and '..' not in path.parts
            paths.add(str(path));paths.update(str(p) for p in path.parents if str(p)!='.')
            metadata=files[str(path)]
            assert metadata['mode']==0o600 and metadata['links']==1 and re.fullmatch('[0-9a-f]{64}',metadata['sha256'])
        expected_files+=len(counts)
    assert len(records)==expected_files and set(files)==paths
    for name,old in original.items():
        current=files[name]
        index.exact({k:v for k,v in current.items() if k!='links'},{k:v for k,v in old.items() if k!='links'})
        assert current['links']==old['links'] if old['sha256'] is not None else current['links']>=old['links']


def replacement_custody(state,science,records,files,initialization,*,ordinals=(3,4)):
    """Complete original/group inputs and journals, with each historical identity."""
    initial=json.loads(bytes.fromhex(initialization))
    binding=hashlib.sha256(bytes.fromhex(records['coordinator.json'])).digest()
    assert binding.hex()==index.raw(index.only(state['archive_attempts'],ordinal=3)['coordinator_binding_sha256'],32).hex()
    assert ordinals in ((3,4),(3,4,5))
    for ordinal in ordinals:
        row=index.only(state['archive_attempts'],ordinal=ordinal);batch=science['batches'][2]
        identity=index.raw(row['attempt_id'],16).hex();lock=files[identity+'/custody.lock']
        assert row['coordinator_binding_sha256']=='\\x'+binding.hex()
        assert records[identity+'/custody.lock']==''
        manifest=dict(format='bigbrotr-local-archive-custody/1',boot_id=initial['boot_id'],
            lock_device=lock['device'],lock_inode=lock['inode'],catalog_root=initial['catalog_root'],
            attempt_id=list(index.raw(row['attempt_id'],16)),publication_ordinal=ordinal,topology_generation='0')
        manifest.update({k:index.raw(row[k],size).hex() for k,size in (
            ('dataset_id',16),('catalog_id',16),('coordinator_incarnation',16),('worker_incarnation',16),('batch_digest',32))})
        index.exact(json.loads(bytes.fromhex(records[identity+'/intent.json'])),manifest)
        assert (batch['epoch'],batch['topology'],batch['first'],batch['last'])==(1,0,4,7)
        body=(b'BBINPUT\x01'+hashlib.sha256(bytes.fromhex(initialization)).digest()
            +b''.join(index.raw(row[k],16) for k in ('attempt_id','dataset_id','catalog_id','coordinator_incarnation','worker_incarnation'))
            +index.be(ordinal)+index.be(0)+bytes.fromhex(batch['archive_digest'])+index.be(1,4)
            +index.be(0,4)+index.be(1)+index.be(4)+b'\x01\x21'+index.be(7)+bytes.fromhex(batch['admission_digest']))
        assert bytes.fromhex(records[identity+'.inputs'])==body+hashlib.sha256(body).digest()
    for name,raw in records.items():
        assert files[name]['sha256']==hashlib.sha256(bytes.fromhex(raw)).hexdigest()


def unbound_catalog_config(text):
    """Bind the proxy's explicit identifiers before starting the real owner."""
    direct=tomllib.loads(text)
    assert direct['postgres']['catalog']['endpoint']==direct['postgres']['control']['endpoint']=='127.0.0.1:15432'
    assert [direct['postgres']['catalog'][k] for k in ('database','user','read_user')]==[
        'bigbrotr_catalog','bigbrotr_catalog','bigbrotr_archive_read']
    return direct


def unbound_fault_process(value,source,local_proxy,*,source_pins,candidate,mode='after-complete'):
    """Exact route/pins plus actual child/proxy/reader/backend termination."""
    from installed_archive_catch_up_fault_oracle import wire_checked,ERRORS
    wire=value['wire'];assert wire['status']=='OBSERVED_CLOSED_AND_JOINED'
    wire_checked(wire,'publication',source,None,local_proxy=local_proxy,mode=mode)
    expected=unbound_catalog_config(value['direct_config'])
    routed=tomllib.loads(value['routed_config'])
    expected['postgres']['catalog']['endpoint']='127.0.0.1:'+str(wire['ready']['port'])
    index.exact(routed,expected)
    assert value['restored_config']==value['direct_config']
    assert value['stdout']=='' and value['stderr']==ERRORS['publication-loss']
    assert value['wait_exit']=='4'
    terminal=value['terminal']
    assert terminal['Running'] is False and terminal['Pid']==0 and terminal['ExitCode']==4 and terminal['OOMKilled'] is False
    assert value['parent_absent'] is True and value['proxy_absent'] is True
    assert all(type(value['parent'][k]) is int and value['parent'][k]>0 for k in ('pid','start_ticks'))
    assert value['proxy_identity']==value['proxy_terminal_identity']
    assert all(type(value['proxy_identity'][k]) is int and value['proxy_identity'][k]>0 for k in ('pid','start_ticks'))
    assert value['backends']=={'bigbrotr_control':'0','bigbrotr_catalog':'0'}
    assert value['source_pins_before']==value['source_pins_after']==source_pins
    assert value['source_pins_before'][str(Path(local_proxy[1]).relative_to(value['source_root']))]==source
    assert value['candidate_before']==value['candidate_after']==candidate


def unbound_archive_fault(prefix,parked,before,after,catalog_before,catalog_after,census,science,*,phase='unique'):
    """Literal native/guard inventories at each cut; current frontier stays2."""
    from cases.first_launch_archive_outage_oracle import archived_prefix
    ordinal,mode,published,imported_ordinals=ARCHIVE_FAULT_PHASES[phase]
    third=archived_prefix(prefix,parked)
    archived_prefix(prefix,dict(before,archive_attempts=[r for r in before['archive_attempts'] if r['ordinal']<=3]))
    assert sorted(r['ordinal'] for r in before['archive_attempts'])==list(range(1,ordinal))
    assert sorted(r['ordinal'] for r in after['archive_attempts'])==list(range(1,ordinal+1))
    for row in before['archive_attempts']:
        index.exact(row,index.only(after['archive_attempts'],ordinal=row['ordinal']))
    view=dict(after,archive_attempts=before['archive_attempts'])
    owner=control_only(before,view,claim='failed')
    replacement=index.only(after['archive_attempts'],ordinal=ordinal)
    expected=dict(third,ordinal=ordinal,expected_predecessor=ordinal-1,attempt_id=replacement['attempt_id'],
        worker_incarnation=replacement['worker_incarnation'],coordinator_incarnation=owner['process_incarnation'],
        issuer_resource=owner['resource_id'],issuer_epoch=owner['epoch'],issuer_incarnation=owner['process_incarnation'],
        issued_at_us=replacement['issued_at_us'],xmin=replacement['xmin'])
    index.exact(replacement,expected)
    for key in ('attempt_id','worker_incarnation'):
        assert any(index.raw(replacement[key],16))
        assert replacement[key] not in {r[key] for r in before['archive_attempts']}
    assert replacement['worker_incarnation']!=replacement['coordinator_incarnation']
    assert replacement['coordinator_incarnation']!=third['coordinator_incarnation']
    source.clock(replacement['issued_at_us'],owner['acquired_at_us'],owner['lease_expires_at_us'])
    assert replacement['xmin'] not in {r['xmin'] for rows in before.values() for r in rows if 'xmin' in r}
    count,cut=(2,3) if phase=='grouped-first' else (3,7)
    selected=dict(batches=science['batches'][:count],tables={name:[r for r in rows
        if r['source_record_id']['owner_epoch']==1 and r['source_record_id']['sequence']<=cut]
        for name,rows in science['tables'].items()})
    assert [b['topology'] for b in selected['batches']]==[0]*count
    receipts=source.publication_receipts(after,selected,published_ordinals=published)
    index.exact(census,dict(format='bigbrotr-independent-installed-archive-read/1',
        tables={**selected['tables'],'archive_commits':receipts}))
    imports=catalog_after['bigbrotr_archive.attempts']
    assert sorted(r['ordinal'] for r in imports)==list(imported_ordinals)
    assert all(r in imports for r in catalog_before['bigbrotr_archive.attempts'])
    imported=index.only(imports,ordinal=ordinal)
    fields=('ordinal','attempt_id','coordinator_incarnation','worker_incarnation','topology_generation','batch_digest')
    index.exact(imported,dict({k:replacement[k] for k in fields},login_name='bb_archive_'+replacement['attempt_id'][2:],
        login_oid=imported['login_oid'],xmin=imported['xmin']))
    assert str(imported['login_oid']).isdecimal() and int(imported['login_oid'])>0
    assert str(imported['xmin']).isdecimal() and int(imported['xmin'])>0
    catalog_owner=index.only(catalog_after['bigbrotr_archive.owner'])
    index.exact(catalog_owner,dict(index.only(catalog_before['bigbrotr_archive.owner']),
        enabled=True,ordinal=ordinal,xmin=catalog_owner['xmin']))
    mutable={'bigbrotr_archive.attempts','bigbrotr_archive.owner',*[f'public.ducklake_{name}' for name in (
        'snapshot','snapshot_changes','data_file','file_column_stats','table_stats','table_column_stats')]}
    if phase=='grouped-first':mutable={'bigbrotr_archive.attempts','bigbrotr_archive.owner'}
    assert set(catalog_before)==set(catalog_after)
    for table in catalog_before.keys()-mutable:index.exact(catalog_before[table],catalog_after[table])
    for table in ('snapshot','snapshot_changes','data_file','file_column_stats'):
        assert all(r in catalog_after['public.ducklake_'+table] for r in catalog_before['public.ducklake_'+table])
    return selected


def compacted_archive_science(science):
    """Re-encode the closed recovery schedule without changing Index science."""
    import copy
    from bootstrap_oracle import cbor
    from installed_sync_oracle import decode
    batches=copy.deepcopy(science['batches'])
    ranges=[(1,1,2),(1,3,3),(1,4,7)]+[(1,8+5*n,12+5*n) for n in range(61)]+[(2,1,4),(2,5,9),(2,10,10)]
    index.exact([(b['epoch'],b['first'],b['last']) for b in batches],ranges)
    ds=bytes.fromhex(science['tables']['events'][0]['source_record_id']['dataset_id'])
    for batch in batches[3:64]:
        assert batch['topology']==0
        refs=[]
        for n,fact in zip(range(batch['first'],batch['last']+1),batch['facts']):
            preimage=bytes.fromhex(fact['preimage_hex'])
            identity=decode(preimage.split(b'\0',1)[1])[0]
            assert identity==[ds,0,1,n]
            refs.append([identity,hashlib.sha256(preimage).digest()])
        assert len(refs)==len(batch['facts'])==batch['last']-batch['first']+1
        batch['topology']=1
        batch['archive_digest']=source.sha(b'bigbrotr/archive-batch/1\0'+cbor([ds,1,1,
            [[0,1,batch['first'],batch['last']]],refs]))
    index.exact([b['topology'] for b in batches],[0]*3+[1]*64)
    return dict(science,batches=batches)


def compacted_archive(prefix,parked,before,after,report,science,catalog_before,catalog_after,*,schedule='uninterrupted'):
    """Exact known-unpublished intent3, sparse receipts and two new frontiers."""
    from cases.first_launch_archive_outage_oracle import archived_prefix,ARCHIVE_TABLES
    from installed_archive_catch_up_oracle import cut_digest
    issued_before,new_attempts=ARCHIVE_SCHEDULES[schedule]
    grouped=schedule=='grouped-publication-loss'
    published=GROUPED_PUBLISHED_ORDINALS if grouped else ARCHIVE_PUBLISHED_ORDINALS
    imported_ordinals=GROUPED_IMPORTED_ORDINALS if grouped else ARCHIVE_PUBLISHED_ORDINALS
    issued_total=69 if grouped else 68
    third=archived_prefix(prefix,parked)
    consumer=index.only(before['consumers'],consumer_role='archive');catalog=consumer['consumer_id']
    index.exact(consumer,index.only(prefix['consumers'],consumer_role='archive'))
    for table in ARCHIVE_TABLES+('consumer_frontier_revisions','consumer_frontiers'):
        selected=lambda state:[r for r in state[table] if r['consumer_id']==catalog]
        index.exact(selected(before),selected(parked))
        assert all(row in selected(after) for row in selected(before)),table
    for old in parked['archive_attempts']:
        index.exact(index.only(before['archive_attempts'],ordinal=old['ordinal']),old)
        index.exact(index.only(after['archive_attempts'],ordinal=old['ordinal']),old)
    for old in before['archive_attempts']:
        index.exact(index.only(after['archive_attempts'],ordinal=old['ordinal']),old)
    index.exact(sorted(r['ordinal'] for r in before['archive_attempts']),list(range(1,issued_before+1)))
    index.exact(sorted(r['ordinal'] for r in after['archive_attempts']),list(range(1,issued_total+1)))
    topology.preserve_consumer_identity(before,after,catalog)
    receipts=source.publication_receipts(after,science,published_ordinals=published)
    compacted_archive_control(before,after,receipts)
    imports=catalog_after['bigbrotr_archive.attempts']
    index.exact(sorted(r['ordinal'] for r in imports),list(imported_ordinals))
    assert all(row in imports for row in catalog_before['bigbrotr_archive.attempts'])
    for row in imports:
        attempt=index.only(after['archive_attempts'],ordinal=row['ordinal'])
        assert row['attempt_id']!=third['attempt_id'] and row['worker_incarnation']!=third['worker_incarnation']
        for field in ('attempt_id','batch_digest','topology_generation','coordinator_incarnation','worker_incarnation'):
            index.exact(row[field],attempt[field])
    failed=[index.only(after['archive_attempts'],ordinal=n)['attempt_id'] for n in ((3,4) if grouped else (3,))]
    assert not [r for r in after['archive_frontier_batches'] if r['attempt_id'] in failed]
    owner=index.only(catalog_after['bigbrotr_archive.owner'])
    index.exact([owner['enabled'],owner['ordinal']],[False,issued_total])
    assert index.only(after['consumers'],consumer_id=catalog)['current_revision']==4
    assert index.only(after['consumers'],consumer_id=catalog)['current_topology_generation']==1
    index.exact(sorted(r['frontier_revision'] for r in after['consumer_frontier_revisions'] if r['consumer_id']==catalog),[1,2,3,4])
    original=index.only(before['archive_frontier_snapshots'],frontier_revision=2)
    for revision,cuts,members,counts in ((3,[(1,3),(2,0)],receipts[:2],[original[k] for k in source.COUNTS]),
            (4,[(1,312),(2,10)],receipts,[73,62,62,62,63])):
        head=index.only(after['consumer_frontier_revisions'],consumer_id=catalog,frontier_revision=revision)
        assert head['previous_revision']==revision-1 and head['topology_generation']==1
        snapshot=index.only(after['archive_frontier_snapshots'],frontier_revision=revision)
        publisher=index.only(after['owner_epochs'],role='archive',epoch=2)
        index.exact(snapshot,dict(operation_id=snapshot['operation_id'],consumer_id=catalog,frontier_revision=revision,
            dataset_id=consumer['dataset_id'],catalog_id=catalog,topology_generation=1,snapshot_id=68,
            owner_epoch=2,owner_incarnation=publisher['process_incarnation'],request_digest=snapshot['request_digest'],
            **dict(zip(source.COUNTS,counts)),xmin=head['xmin']))
        assert any(index.raw(snapshot['operation_id'],16)) and snapshot['operation_id'] not in {r['operation_id'] for r in before['archive_frontier_snapshots']}
        assert snapshot['request_digest']=='\\x'+cut_digest(consumer['dataset_id'],1,members,cuts,68)
        bounds=sorted((r for r in after['consumer_frontiers'] if r['consumer_id']==catalog and r['frontier_revision']==revision),key=lambda r:r['owner_epoch'])
        index.exact([(r['shard_id'],r['topology_generation'],r['owner_epoch'],r['last_complete_sequence']) for r in bounds],
            [(0,1,epoch,cut) for epoch,cut in cuts])
        for bound in bounds:
            last=next((r for r in reversed(members) if r['input_ranges'][0]['owner_epoch']==bound['owner_epoch']),None)
            index.exact([bound['attestation_kind'],bound['attestation_digest']],
                ['archive-batch','\\x'+last['batch_digest'].lower()] if last else [None,None])
        membership=[r['attempt_id'] for r in after['archive_frontier_batches'] if r['consumer_id']==catalog and r['frontier_revision']==revision]
        index.exact(sorted(membership),sorted('\\x'+r['attempt_id'].lower() for r in members))
        for row in after['archive_frontier_batches']:
            if row['consumer_id']==catalog and row['frontier_revision']==revision:
                index.exact(row,dict(consumer_id=catalog,frontier_revision=revision,attempt_id=row['attempt_id'],xmin=head['xmin']))
    assert len({r['operation_id'] for r in after['archive_frontier_snapshots']})==len(after['archive_frontier_snapshots'])
    frontier=index.only(after['archive_frontier_snapshots'],frontier_revision=4)
    index.exact(report,dict(format='bigbrotr-archive-catch-up/1',dataset_id=consumer['dataset_id'][2:],catalog_id=catalog[2:],
        terminal_epoch=2,issued_attempts_before=issued_before,new_attempts=new_attempts,frontier_revision=4,snapshot_id=68,
        operation_id=list(index.raw(frontier['operation_id'],16)),request_digest=frontier['request_digest'][2:],
        family_counts=[73,62,62,62,63],new_frontier_revisions=2,recovery_basis='closed-producer-floor-v3',compacted_requests='64'))
    return receipts


def compacted_archive_control(before,after,receipts):
    """One fresh Control and exact Archive rebind/publications; all else stays exact."""
    from cases.first_launch_index_restart_oracle import unchanged_except
    archive_tables={'archive_attempts','archive_owner_sessions','consumers',
        'consumer_frontier_revisions','consumer_frontiers','archive_frontier_snapshots','archive_frontier_batches'}
    unchanged_except(before,after,archive_tables|{'owner_epochs','role_instances'})
    # Validate the existing Control claim/fence/release helper against a view
    # whose separately checked Archive transition is restored to its old rows.
    view=dict(after,**{table:before[table] for table in archive_tables})
    for table in ('owner_epochs','role_instances'):
        view[table]=[r for r in after[table] if r['role']!='archive']+[
            r for r in before[table] if r['role']=='archive']
    control=control_only(before,view)
    consumer=index.only(before['consumers'],consumer_role='archive');catalog=consumer['consumer_id']
    prior=sorted((r for r in before['owner_epochs'] if r['role']=='archive'),key=lambda r:r['epoch'])
    old=prior[-1];assert len(prior)==1 and old['epoch']==1 and old['state']=='active' and old['resource_id']==catalog
    owners=sorted((r for r in after['owner_epochs'] if r['role']=='archive'),key=lambda r:r['epoch'])
    assert len(owners)==len(prior)+1
    index.exact(owners[:-2],prior[:-1])
    stopped,new=owners[-2:]
    role=index.only(after['role_instances'],role_instance_id=new['role_instance_id'])
    old_role=index.only(before['role_instances'],role_instance_id=old['role_instance_id'])
    retired=index.only(after['role_instances'],role_instance_id=old['role_instance_id'])
    bridge=index.only(after['consumer_frontier_revisions'],consumer_id=catalog,frontier_revision=3)
    final=index.only(after['consumer_frontier_revisions'],consumer_id=catalog,frontier_revision=4)
    snapshot=index.only(after['archive_frontier_snapshots'],consumer_id=catalog,frontier_revision=3)
    tx=bridge['xmin']
    digest=(b'bigbrotr/archive-frontier-owner-exclusion/1\0'+index.raw(consumer['dataset_id'],16)
        +index.raw(catalog,16)+index.be(old['epoch'])+index.raw(old['process_incarnation'],16)
        +index.raw(control['resource_id'],16)+index.be(control['epoch'])
        +index.raw(control['process_incarnation'],16)+index.raw(snapshot['request_digest'],32))
    index.exact(stopped,dict(old,state='released',released_at_us=stopped['released_at_us'],
        exclusion_method='storage_fenced',exclusion_digest='\\x'+hashlib.sha256(digest).hexdigest(),xmin=tx))
    index.exact(retired,dict(old_role,retired_at_us=retired['retired_at_us'],xmin=tx))
    assert control['acquired_at_us']<=stopped['released_at_us']<=retired['retired_at_us']<=role['created_at_us']<=new['acquired_at_us']<=bridge['created_at_us']<=final['created_at_us']<=control['released_at_us']
    assert new['acquired_at_us']<new['lease_expires_at_us']<=control['lease_expires_at_us']
    assert new['role_instance_id'] not in {r['role_instance_id'] for r in before['role_instances']}
    index.exact(new,dict(old,epoch=old['epoch']+1,role_instance_id=role['role_instance_id'],
        process_incarnation=control['process_incarnation'],acquired_at_us=new['acquired_at_us'],
        lease_expires_at_us=new['lease_expires_at_us'],state='active',released_at_us=None,
        exclusion_method=None,exclusion_digest=None,xmin=final['xmin']))
    index.exact(role,dict(role_instance_id=new['role_instance_id'],dataset_id=consumer['dataset_id'],
        role='archive',created_at_us=role['created_at_us'],retired_at_us=None,xmin=tx))
    index.exact(sorted((r for r in after['role_instances'] if r['role']=='archive'),key=lambda r:r['role_instance_id']),
        sorted([retired,role],key=lambda r:r['role_instance_id']))
    sessions=before['archive_owner_sessions'];session=index.only(sessions,owner_epoch=old['epoch'])
    expected_sessions=sessions+[dict(session,owner_epoch=new['epoch'],
        owner_incarnation=control['process_incarnation'],issuer_resource=control['resource_id'],
        issuer_epoch=control['epoch'],issuer_incarnation=control['process_incarnation'],xmin=tx)]
    index.exact(sorted(after['archive_owner_sessions'],key=lambda r:r['owner_epoch']),
        sorted(expected_sessions,key=lambda r:r['owner_epoch']))
    known_xmins={r['xmin'] for rows in before.values() for r in rows if 'xmin' in r}
    assert tx!=final['xmin'] and not {tx,final['xmin']}.intersection(known_xmins)
    for table,added in (('archive_frontier_snapshots',2),('archive_frontier_batches',69),('consumer_frontiers',4)):
        selected=lambda state:[r for r in state[table] if r['consumer_id']==catalog]
        assert len(selected(after))==len(selected(before))+added
    for head in (bridge,final):
        index.exact(head,dict(consumer_id=catalog,frontier_revision=head['frontier_revision'],
            dataset_id=consumer['dataset_id'],consumer_role='archive',topology_generation=1,
            previous_revision=head['frontier_revision']-1,owner_resource_id=catalog,owner_epoch=new['epoch'],
            owner_incarnation=control['process_incarnation'],created_at_us=head['created_at_us'],sealed=True,xmin=head['xmin']))
        for table in ('archive_frontier_snapshots','consumer_frontiers','archive_frontier_batches'):
            rows=[r for r in after[table] if r['consumer_id']==catalog and r['frontier_revision']==head['frontier_revision']]
            assert rows and all(r['xmin']==head['xmin'] for r in rows)
        snap=index.only(after['archive_frontier_snapshots'],consumer_id=catalog,frontier_revision=head['frontier_revision'])
        assert snap['owner_epoch']==new['epoch'] and snap['owner_incarnation']==control['process_incarnation']
    assert index.only(after['consumers'],consumer_id=catalog)['xmin']==final['xmin']
    original=index.only(before['archive_attempts'],ordinal=3)
    issued=max(r['ordinal'] for r in before['archive_attempts'])
    new_xmins=set()
    for receipt in receipts:
        n=receipt['publication_ordinal']
        if n<=issued:continue
        row=index.only(after['archive_attempts'],ordinal=n)
        assert row['xmin'] not in known_xmins|new_xmins;new_xmins.add(row['xmin'])
        assert any(index.raw(row['attempt_id'],16)) and any(index.raw(row['worker_incarnation'],16))
        assert row['worker_incarnation']!=control['process_incarnation']
        source.clock(row['issued_at_us'],control['acquired_at_us'],control['released_at_us'])
        index.exact(row,dict(original,ordinal=n,expected_predecessor=n-1,attempt_id=row['attempt_id'],
            worker_incarnation=row['worker_incarnation'],coordinator_incarnation=control['process_incarnation'],
            topology_generation=receipt['topology_generation'],batch_digest='\\x'+receipt['batch_digest'].lower(),
            issuer_resource=control['resource_id'],issuer_epoch=control['epoch'],issuer_incarnation=control['process_incarnation'],
            issued_at_us=row['issued_at_us'],xmin=row['xmin']))
    assert len({r['attempt_id'] for r in after['archive_attempts']})==len(after['archive_attempts'])
    assert len({r['worker_incarnation'] for r in after['archive_attempts']})==len(after['archive_attempts'])
