"""Strict PG C4/Native A3 and PG C3/Native I4 historical recovery boundaries."""
import hashlib
from cases import first_launch_index_oracle as inventory
from cases import first_launch_passive_oracle as passive


def _fault(wire, dataset, slot, *, mode, complete):
    assert wire['event']=='fault' and wire['target']=='control-measurement-checkpoint'
    assert wire['mode']==mode and wire['armed'] is True
    assert all(type(wire[k]) is int for k in ('data_queries','data_commits','server_complete',
        'client_complete','faults','measurement_updates_complete'))
    assert [wire[k] for k in ('data_queries','data_commits','server_complete','client_complete','faults',
        'measurement_updates_complete')]==[1,1,complete,0,1,1]
    assert wire['measurement_identity_sha256']==hashlib.sha256(bytes.fromhex(dataset+slot)).hexdigest()
    assert type(wire['backend_pid']) is int and wire['backend_pid']>0


def fault(wire, dataset, slot):
    _fault(wire,dataset,slot,mode='after-complete',complete=1)


def fault_before_forward(wire, dataset, slot):
    _fault(wire,dataset,slot,mode='before-forward',complete=0)


def original_measurement(state, initialized, selection, originals, decode):
    """Original request and dispatch attribution, independent of SQL outcome presence."""
    inventory.control_inventory(state)
    assert all(not state[name] for name in passive.EMPTY_TABLES)
    dispatch=passive.only(state['measurement_dispatches'])
    refs=passive.references(state,initialized)
    assert dispatch['dataset_id']=='\\x'+initialized['private_scope']['dataset_id']
    owner=passive.only(state['owner_epochs'],role='control',resource_id=dispatch['issuer_resource'],
        epoch=dispatch['issuer_epoch'],process_incarnation=dispatch['issuer_incarnation'])
    assert owner['dataset_id']==dispatch['dataset_id']
    assert passive.raw(dispatch['context_bytes'],294)==passive.context_bytes(refs)
    assert all(dispatch[key]=='\\x'+selection[key] for key in ('endpoint_id','access_scope_id','route_id'))
    assert dispatch['profile_version']==1 and dispatch['enrollment_generation']==1
    assert dispatch['config_revision']==dispatch['policy_revision']==1
    authority=dispatch['authority_ceiling_us']
    assert type(authority) is int and 0<authority<=5_000_000
    assert dispatch['dispatch_deadline_us']-dispatch['created_at_us']==authority
    assert len(originals)==4
    value=decode(originals[3].split(b'\0',1)[1])
    assert value[:3]==[[bytes.fromhex(initialized['producer_slot_id']),1,4],'admit-evidence',1]
    assert value[3][:3]==[1,passive.reference(refs['visibility_scope_id']),'measurement/1']
    measurement=value[3][3]
    assert measurement[0]==passive.raw(dispatch['attempt_id'],16)
    assert measurement[1]==[[0,passive.reference(refs['endpoint_id'])]]+[
        passive.reference(refs[k]) for k in ('observer_id','route_id','access_scope_id','method_id')]
    assert measurement[2]==passive.PROFILE+[authority]
    assert measurement[3][0]==passive.reference(refs['clock_id'])
    assert measurement[4][0]==2, 'known terminal measurement required; unknown/resolution cannot pass'
    requests=sorted(state['producer_requests'],key=lambda r:r['request_sequence'])
    assert [r['request_sequence'] for r in requests]==[1,2,3,4]
    assert all(r['acquisition_job_id'] is None and r['acquisition_scope_id'] is None for r in requests)
    assert all(r['monitor_attempt_id'] is None and r['monitor_resolution'] is None for r in requests[:3])
    request=requests[3]
    assert request['issuer_role']==dispatch['issuer_role']=='control'
    assert request['monitor_attempt_id']==dispatch['attempt_id'] and request['monitor_resolution'] is False
    for key in ('endpoint_id','access_scope_id','route_id'):
        assert request['relay_'+key]==dispatch[key]
    assert request['relay_enrollment_generation']==1
    for key in ('resource','epoch','incarnation'):
        assert request['issuer_'+key]==dispatch['issuer_'+key]
    return refs,measurement


def terminal(state, initialized, selection, originals, cbor, decode):
    """Original known-C4 case retains its initial-admission recorder checks."""
    refs,measurement=original_measurement(state,initialized,selection,originals,decode)
    dispatch=passive.only(state['measurement_dispatches'])
    requests=sorted(state['producer_requests'],key=lambda r:r['request_sequence'])
    outcomes=sorted(state['producer_outcomes'],key=lambda r:r['request_sequence'])
    assert [r['request_sequence'] for r in outcomes]==[1,2,3,4]
    request=requests[3];outcome=outcomes[3]
    for key in ('resource','epoch','incarnation'):
        assert request['issuer_'+key]==outcome['recorder_'+key]==dispatch['issuer_'+key]
    receipt=decode(passive.raw(outcome['outcome_bytes']).split(b'\0',1)[1])
    receipts=passive.retained(state,initialized,{'measurement':{'payload_digest':receipt[4][0][1].hex()}},
        refs,measurement,originals,cbor,decode)
    passive.checkpoint(state,initialized,receipts)
    return receipts


def retained_science(measurement, refs, authority, wire, event, before, after, cbor):
    """Adapt the original typed input to the same independent scientific checks."""
    attempt,context,profile,clock,state=measurement
    kind,terminal=state
    observed,ended,reason,teardown=terminal
    execution,opened,read,counters=observed
    destination,result,first=execution
    tcp,tls,websocket=opened
    flushed,valid,eose,verified,auth,closed=read
    def moment(value):
        utc,elapsed=value
        return dict(observed_at=str(utc),elapsed_us=str(elapsed))
    event_id,received,checked=valid
    payload=dict(attempt_id=attempt.hex(),subject_kind=0,
        **{key:refs[key] for key in ('endpoint_id','observer_id','route_id','access_scope_id','method_id')},
        profile=profile,clock_quality=dict(clock_id=refs['clock_id'],resolution_us=str(clock[1]),
            uncertainty_us=clock[2],anomaly_flags=clock[3]),state_kind=kind,prior_unknown=None,
        observed=dict(execution=dict(destination=destination,result=result[0],error=None,observed_at=str(first)),
            open=dict(tcp_connected=moment(tcp),tls_completed=tls,websocket_opened=moment(websocket)),
            read=dict(request_flushed=moment(flushed),first_valid_event=dict(event_id=event_id.hex(),
                received=moment(received),verified=moment(checked)),eose=moment(eose),
                eose_prefix_verified=verified,auth_observed=auth,correlated_closed=closed),
            counters=dict(zip(('rx_physical_frames','rx_messages','event_candidates','rx_ws_bytes',
                'rx_payload_bytes','tx_ws_bytes'),counters))),ended=moment(ended),
        termination=dict(kind=reason[0],phase=None,failure=None,limit_code=None),teardown=teardown)
    expected=passive.scientific_payload(payload,attempt.hex(),authority,refs,wire,event,before,after)
    assert cbor(expected)==cbor(measurement), 'original input has altered or additional scientific fields'
    return expected


def native_outcome(state, dump, initialized, selection, originals, cbor, decode, wire, event, before, after):
    """Observe only Fact10's independent clock scalar, never its receipt/digest."""
    eligible(state)
    producer=passive.only(state['producer_epochs'])
    assert producer['dataset_id']=='\\x'+initialized['private_scope']['dataset_id']
    assert producer['producer_slot_id']=='\\x'+initialized['producer_slot_id']
    refs,measurement=original_measurement(state,initialized,selection,originals,decode)
    assert type(before) is type(after) is int and -(2**63)<=before<=after<2**63
    dispatch=passive.only(state['measurement_dispatches'])
    measurement=retained_science(measurement,refs,dispatch['authority_ceiling_us'],wire,event,before,after,cbor)
    receipts=passive.retained_before_terminal(state,initialized,refs,measurement,originals,cbor,decode)
    passive.checkpoint_before_terminal(state,initialized,receipts)
    dataset=bytes.fromhex(initialized['private_scope']['dataset_id']);slot=bytes.fromhex(initialized['producer_slot_id'])
    source=[dataset,0,1,10]
    key=b'\x01\x20'+dataset+bytes(4)+(1).to_bytes(8,'big')+(10).to_bytes(8,'big')
    actual=passive.native_rows(dump)[('facts',key)]
    assert actual[34:].startswith(b'bigbrotr/fact/1\0')
    fields=decode(actual[34:].split(b'\0',1)[1])
    assert type(fields) is list and len(fields)==6
    recorded_at=fields[4]
    # Admission samples this separately from the retained Measurement. It is
    # an observed scalar inside the independently captured fixture window, not
    # the terminal timestamp or evidence of monotonic UTC ordering.
    assert type(recorded_at) is int and -(2**63)<=recorded_at<2**63 and before<=recorded_at<=after
    preimage=b'bigbrotr/fact/1\0'+cbor([source,'measurement',1,
        passive.reference(refs['visibility_scope_id']),recorded_at,measurement])
    digest=hashlib.sha256(preimage).digest()
    assert actual==b'\x01\x20'+digest+preimage, 'Native Fact10 differs from independently encoded original science'
    references={source[3]:[source,digest] for receipt in receipts for source,digest in receipt[4]}
    assert sorted(references)==list(range(1,10))
    final=[source,digest]
    receipt=[1,[slot,1,4],hashlib.sha256(originals[3]).digest(),
        [dataset,0,1,bytes.fromhex(initialized['recovery_generation']),1],
        [final],[references[number] for number in range(3,10)]+[final]]
    receipts=[*receipts,receipt]
    passive.native_physical(dump,initialized,receipts,cbor,
        passive.raw(state['producer_requests'][0]['manifest_sha256'],32),phase='reply-lost')
    return receipts


def repaired_terminal(state, initialized, selection, originals, receipts, cbor, decode):
    """Actual SQL copy must match the independently reconstructed original."""
    refs,measurement=original_measurement(state,initialized,selection,originals,decode)
    actual=passive.retained(state,initialized,{'measurement':{'payload_digest':receipts[3][4][0][1].hex()}},
        refs,measurement,originals,cbor,decode)
    assert [cbor(value) for value in actual]==[cbor(value) for value in receipts]
    passive.checkpoint(state,initialized,actual)
    return actual


def repaired(before, after, initialized, receipts, cbor, *, retry=False):
    _recovery_tables(before,after,pg_changed=True)
    recovered_owners(before,after,retry=retry)
    _repaired_sql(before,after,initialized,receipts,cbor,retry=retry,completed=True)


def _recovery_tables(before, after, *, pg_changed):
    inventory.control_inventory(before);inventory.control_inventory(after)
    changed={'owner_epochs','role_instances','producer_epochs'}
    if pg_changed:changed.update(('producer_outcomes','producer_checkpoints','producer_checkpoint_dependencies'))
    assert all(not after[name] for name in passive.EMPTY_TABLES)
    for table in inventory.CONTROL_TABLES-changed:
        assert after[table]==before[table], 'repair changed retained '+table


def _repaired_sql(before, after, initialized, receipts, cbor, *, retry, completed):
    if retry:
        passive.checkpoint(before,initialized,receipts)
        passive.checkpoint(after,initialized,receipts)
        for table in ('producer_outcomes','producer_checkpoints','producer_checkpoint_dependencies'):
            assert after[table]==before[table], 'exact repair retry rewrote '+table
        return
    passive.checkpoint_before_terminal(before,initialized,receipts[:3])
    passive.checkpoint(after,initialized,receipts)
    old=sorted(before['producer_outcomes'],key=lambda r:r['request_sequence'])
    new=sorted(after['producer_outcomes'],key=lambda r:r['request_sequence'])
    assert [row['request_sequence'] for row in old]==[1,2,3]
    assert [row['request_sequence'] for row in new]==[1,2,3,4] and new[:3]==old
    head,control=_advanced_head(before,after,receipts,completed=completed)
    for key,column in (('resource','resource_id'),('epoch','epoch'),('incarnation','process_incarnation')):
        assert head['issuer_'+key]==new[3]['recorder_'+key]==control[column]
    for row,field in ((head,'committed_at_us'),(new[3],'recorded_at_us')):
        upper=control['released_at_us'] if completed else control['lease_expires_at_us']
        assert type(row[field]) is int and control['acquired_at_us']<=row[field]<=upper
    encoded=b'bigbrotr/receipt/1\0'+cbor(receipts[3])
    expected=dict(old[2])
    expected.update(request_sequence=4,request_digest='\\x'+receipts[3][2].hex(),
        outcome_bytes='\\x'+encoded.hex(),outcome_sha256='\\x'+hashlib.sha256(encoded).hexdigest(),
        recorder_resource=control['resource_id'],recorder_epoch=control['epoch'],
        recorder_incarnation=control['process_incarnation'],recorded_at_us=new[3]['recorded_at_us'],xmin=head['xmin'])
    assert new[3]==expected


def _advanced_head(before, after, receipts, *, completed=True):
    prior=passive.only(before['producer_checkpoints']);head=passive.only(after['producer_checkpoints'])
    mutable={'checkpoint_revision','complete_through_request','dependency_count','checkpoint_digest',
        'committed_at_us','issuer_resource','issuer_epoch','issuer_incarnation','xmin'}
    assert {k:v for k,v in head.items() if k not in mutable}=={k:v for k,v in prior.items() if k not in mutable}
    assert head['xmin']!=prior['xmin']
    control=max((r for r in after['owner_epochs'] if r['role']=='control'),key=lambda r:r['epoch'])
    for key,column in (('resource','resource_id'),('epoch','epoch'),('incarnation','process_incarnation')):
        assert head['issuer_'+key]==control[column]
    upper=control['released_at_us'] if completed else control['lease_expires_at_us']
    assert type(head['committed_at_us']) is int and control['acquired_at_us']<=head['committed_at_us']<=upper
    old_dependencies=sorted(before['producer_checkpoint_dependencies'],key=lambda r:r['source_sequence'])
    dependencies=sorted(after['producer_checkpoint_dependencies'],key=lambda r:r['source_sequence'])
    assert dependencies[:9]==old_dependencies and len(dependencies)==10
    expected=dict(old_dependencies[-1],source_sequence=10,payload_digest='\\x'+receipts[3][4][0][1].hex(),xmin=head['xmin'])
    assert dependencies[-1]==expected
    return head,control


def seeded_terminal(state, initialized, selection, originals, receipts, cbor, decode, *, repaired):
    """Four actual SQL copies, independently reconstructed; C3 and C4 stay distinct."""
    refs,measurement=original_measurement(state,initialized,selection,originals,decode)
    actual=passive.retained(state,initialized,{'measurement':{'payload_digest':receipts[3][4][0][1].hex()}},
        refs,measurement,originals,cbor,decode)
    assert [cbor(value) for value in actual]==[cbor(value) for value in receipts]
    if repaired:passive.checkpoint_existing_outcome(state,initialized,actual)
    else:passive.checkpoint_before_terminal(state,initialized,actual[:3])
    return actual


def seeded_outcome(before, after, initialized, receipts, cbor):
    """The explicit test-only seed inserts only outcome4 and closes native custody."""
    _recovery_tables(before,after,pg_changed=True)
    recovered_owners(before,after)
    for table in ('producer_checkpoints','producer_checkpoint_dependencies'):
        assert after[table]==before[table], 'seed advanced '+table
    passive.checkpoint_before_terminal(before,initialized,receipts[:3])
    passive.checkpoint_before_terminal(after,initialized,receipts[:3])
    old=sorted(before['producer_outcomes'],key=lambda row:row['request_sequence'])
    new=sorted(after['producer_outcomes'],key=lambda row:row['request_sequence'])
    assert [row['request_sequence'] for row in old]==[1,2,3]
    assert [row['request_sequence'] for row in new]==[1,2,3,4] and new[:3]==old
    control=max((r for r in after['owner_epochs'] if r['role']=='control'),key=lambda row:row['epoch'])
    assert type(new[3]['recorded_at_us']) is int and control['acquired_at_us']<=new[3]['recorded_at_us']<=control['released_at_us']
    assert new[3]['xmin'] not in {row['xmin'] for row in old}
    encoded=b'bigbrotr/receipt/1\0'+cbor(receipts[3])
    expected=dict(old[2],request_sequence=4,request_digest='\\x'+receipts[3][2].hex(),
        outcome_bytes='\\x'+encoded.hex(),outcome_sha256='\\x'+hashlib.sha256(encoded).hexdigest(),
        recorder_resource=control['resource_id'],recorder_epoch=control['epoch'],
        recorder_incarnation=control['process_incarnation'],recorded_at_us=new[3]['recorded_at_us'],xmin=new[3]['xmin'])
    assert new[3]==expected


def repaired_existing(before, after, initialized, receipts, *, retry=False):
    _recovery_tables(before,after,pg_changed=True)
    _recovered_owners(before,after,initial_open=False,completed=True)
    assert after['producer_outcomes']==before['producer_outcomes'], 'repair rewrote the seeded outcome or original history'
    if retry:
        passive.checkpoint_existing_outcome(before,initialized,receipts)
        passive.checkpoint_existing_outcome(after,initialized,receipts)
        for table in ('producer_checkpoints','producer_checkpoint_dependencies'):
            assert after[table]==before[table], 'exact retry rewrote '+table
    else:
        passive.checkpoint_before_terminal(before,initialized,receipts[:3])
        passive.checkpoint_existing_outcome(after,initialized,receipts)
        head,control=_advanced_head(before,after,receipts)
        outcome=passive.only(after['producer_outcomes'],request_sequence=4)
        assert outcome['recorder_epoch']<control['epoch'] and outcome['xmin']!=head['xmin']


def repair_rollback(before, after, initialized, receipts):
    """Withheld repair COMMIT keeps the original C3 bytes/xmins, but closes custody."""
    _recovery_tables(before,after,pg_changed=False)
    _recovered_owners(before,after,initial_open=True,completed=False)
    passive.checkpoint_before_terminal(before,initialized,receipts[:3])
    passive.checkpoint_before_terminal(after,initialized,receipts[:3])


def repair_commit_loss(before, after, initialized, receipts, cbor):
    """Actual repair committed under fresh unreleased Control; ACK has not run."""
    _recovery_tables(before,after,pg_changed=True)
    _recovered_owners(before,after,initial_open=False,completed=False)
    _repaired_sql(before,after,initialized,receipts,cbor,retry=False,completed=False)


def repair_ack(before, after, initialized, receipts, cbor):
    """Fresh direct completion after uncertainty retains the committed C4 rows."""
    _recovery_tables(before,after,pg_changed=False)
    _recovered_owners(before,after,initial_open=False,completed=True)
    _repaired_sql(before,after,initialized,receipts,cbor,retry=True,completed=True)


def before_terminal(state, initialized, selection, originals, receipts, cbor, decode):
    refs,measurement=original_measurement(state,initialized,selection,originals,decode)
    actual=passive.retained_before_terminal(state,initialized,refs,measurement,originals,cbor,decode)
    assert actual==receipts[:3]
    passive.checkpoint_before_terminal(state,initialized,actual)


def eligible(state):
    """Native/PG exclusion must separately precede this historical observation."""
    ingest=passive.only(state['owner_epochs'],role='ingest')
    role=passive.only(state['role_instances'],role_instance_id=ingest['role_instance_id'])
    assert ingest['state'] in ('active','draining') and role['retired_at_us'] is None
    producer=passive.only(state['producer_epochs'])
    assert producer['state']=='open' and producer['producer_epoch']==1 and producer['reserved_through_request']==4


def recovered(before, after, *, retry=False):
    inventory.control_inventory(before);inventory.control_inventory(after)
    assert all(not after[name] for name in passive.EMPTY_TABLES)
    for table in inventory.CONTROL_TABLES-{'owner_epochs','role_instances','producer_epochs'}:
        assert after[table]==before[table], 'recovery changed retained '+table
    recovered_owners(before,after,retry=retry)


def recovered_owners(before, after, *, retry=False):
    """Shared historical exclusion, fresh Control and producer-close observations."""
    _recovered_owners(before,after,initial_open=not retry,completed=True)


def _recovered_owners(before, after, *, initial_open, completed):
    ingest=passive.only(before['owner_epochs'],role='ingest')
    assert passive.only(after['owner_epochs'],role='ingest')==ingest
    producer=passive.only(before['producer_epochs']);actual=passive.only(after['producer_epochs'])
    assert actual['state']=='closing'
    assert {k:v for k,v in actual.items() if k not in ('state','xmin')}=={
        k:v for k,v in producer.items() if k not in ('state','xmin')}
    if initial_open:assert producer['state']=='open' and actual['xmin']!=producer['xmin']
    else:assert producer['state']=='closing' and actual==producer
    old=sorted([r for r in before['owner_epochs'] if r['role']=='control'],key=lambda r:r['epoch'])
    controls=sorted([r for r in after['owner_epochs'] if r['role']=='control'],key=lambda r:r['epoch'])
    assert len(controls)==len(old)+1
    for prior in old:
        current=passive.only(controls,resource_id=prior['resource_id'],epoch=prior['epoch'])
        if prior['state']=='released':assert current==prior
        else:
            assert current['state']=='released' and current['exclusion_method']=='storage_fenced'
            assert len(passive.raw(current['exclusion_digest'],32))==32
            assert {k:v for k,v in current.items() if k not in ('state','released_at_us','exclusion_method','exclusion_digest','xmin')}=={
                k:v for k,v in prior.items() if k not in ('state','released_at_us','exclusion_method','exclusion_digest','xmin')}
    newest=controls[-1]
    assert newest['epoch']==old[-1]['epoch']+1 and newest['resource_id']==old[-1]['resource_id']
    assert newest['dataset_id']==ingest['dataset_id']
    assert newest['process_incarnation'] not in {r['process_incarnation'] for r in old}
    assert newest['role_instance_id'] not in {r['role_instance_id'] for r in before['role_instances']}
    assert newest['control_fence_version']==1
    assert newest['control_backend_pid']>0 and newest['control_backend_start'] is not None
    if completed:
        assert newest['state']=='released'
        assert newest['exclusion_method']=='storage_fenced' and len(passive.raw(newest['exclusion_digest'],32))==32
        assert newest['acquired_at_us']<=newest['released_at_us']<newest['lease_expires_at_us']
    else:
        assert newest['state']=='active' and newest['released_at_us'] is None
        assert newest['exclusion_method'] is newest['exclusion_digest'] is None
        assert newest['acquired_at_us']<newest['lease_expires_at_us']
    assert len(after['role_instances'])==len(before['role_instances'])+1
    for prior in before['role_instances']:
        current=passive.only(after['role_instances'],role_instance_id=prior['role_instance_id'])
        if prior['retired_at_us'] is not None or prior['role']!='control':assert current==prior
        else:
            assert current['retired_at_us'] is not None
            assert {k:v for k,v in current.items() if k not in ('retired_at_us','xmin')}=={
                k:v for k,v in prior.items() if k not in ('retired_at_us','xmin')}
    role=passive.only(after['role_instances'],role_instance_id=newest['role_instance_id'])
    assert role['role']=='control' and role['dataset_id']==newest['dataset_id']
    assert role['created_at_us']<=newest['acquired_at_us']
    if completed:assert newest['released_at_us']<=role['retired_at_us']
    else:assert role['retired_at_us'] is None


def native_transition(before, after, initialized, *, retry=False):
    _native_transition(before,after,initialized,writes=0 if retry else 2,unchanged=retry)


def native_repair_fault(before, after, initialized, *, committed):
    # close_producer_marker writes only on open->closing. Both failed replay
    # transactions return before acknowledgement; closing->closing is read-only.
    _native_transition(before,after,initialized,writes=0 if committed else 1,unchanged=committed)


def native_repair_ack(before, after, initialized):
    _native_transition(before,after,initialized,writes=1,unchanged=False)


def _native_transition(before, after, initialized, *, writes, unchanged):
    key='0131'+initialized['producer_slot_id']+'0000000000000001'
    select=lambda dump:{(r['cf'],r['key']):r['value'] for r in dump['rows']}
    old=select(before);new=select(after)
    assert len(old)==len(before['rows']) and len(new)==len(after['rows'])
    assert old.keys()==new.keys()
    assert {(cf,k):v for (cf,k),v in old.items() if (cf,k)!=('receipts',key)}=={
        (cf,k):v for (cf,k),v in new.items() if (cf,k)!=('receipts',key)}
    assert after['sequence']==before['sequence']+writes
    if unchanged:assert old==new


def completion(records, initialized):
    assert records==[dict(format='bigbrotr-ingest-recovery/1',state='positive-outcomes-resolved',
        dataset_id=initialized['dataset_id'],owner_resource_id=initialized['owner_resource_id'],
        producer_slot_id=initialized['producer_slot_id'],producer_epoch='1',issued_through='4',
        reserved_through='4',resolved_requests='4',native='closed',owner_release='committed',ready=False)]
