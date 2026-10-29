"""Literal finite64 boundary: independent input/native/typed progress science."""
import hashlib
import json
import re
from pathlib import Path

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
