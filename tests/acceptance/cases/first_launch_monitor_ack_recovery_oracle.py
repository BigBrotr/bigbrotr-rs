"""One already committed Monitor checkpoint; positive historical ACK only."""
import hashlib
from cases import first_launch_index_oracle as inventory
from cases import first_launch_passive_oracle as passive


def fault(wire, dataset, slot):
    assert wire['event']=='fault' and wire['target']=='control-measurement-checkpoint'
    assert wire['mode']=='after-complete' and wire['armed'] is True
    assert [wire[k] for k in ('data_queries','data_commits','server_complete','client_complete','faults',
        'measurement_updates_complete')]==[1,1,1,0,1,1]
    assert wire['measurement_identity_sha256']==hashlib.sha256(bytes.fromhex(dataset+slot)).hexdigest()
    assert type(wire['backend_pid']) is int and wire['backend_pid']>0


def terminal(state, initialized, selection, originals, cbor, decode):
    """Original input/receipt binds the historical attempt, without a fake report."""
    inventory.control_inventory(state)
    assert all(not state[name] for name in passive.EMPTY_TABLES)
    dispatch=passive.only(state['measurement_dispatches'])
    refs=passive.references(state,initialized)
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
    outcomes=sorted(state['producer_outcomes'],key=lambda r:r['request_sequence'])
    assert [r['request_sequence'] for r in requests]==[1,2,3,4]==[r['request_sequence'] for r in outcomes]
    assert all(r['acquisition_job_id'] is None and r['acquisition_scope_id'] is None for r in requests)
    assert all(r['monitor_attempt_id'] is None and r['monitor_resolution'] is None for r in requests[:3])
    request=requests[3];outcome=outcomes[3]
    assert request['monitor_attempt_id']==dispatch['attempt_id'] and request['monitor_resolution'] is False
    for key in ('endpoint_id','access_scope_id','route_id'):
        assert request['relay_'+key]==dispatch[key]
    assert request['relay_enrollment_generation']==1
    for key in ('resource','epoch','incarnation'):
        assert request['issuer_'+key]==outcome['recorder_'+key]==dispatch['issuer_'+key]
    receipt=decode(passive.raw(outcome['outcome_bytes']).split(b'\0',1)[1])
    receipts=passive.retained(state,initialized,{'measurement':{'payload_digest':receipt[4][0][1].hex()}},
        refs,measurement,originals,cbor,decode)
    passive.checkpoint(state,initialized,receipts)
    return receipts


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
    ingest=passive.only(before['owner_epochs'],role='ingest')
    assert passive.only(after['owner_epochs'],role='ingest')==ingest
    producer=passive.only(before['producer_epochs']);actual=passive.only(after['producer_epochs'])
    assert actual['state']=='closing'
    assert {k:v for k,v in actual.items() if k not in ('state','xmin')}=={
        k:v for k,v in producer.items() if k not in ('state','xmin')}
    if retry:assert actual==producer
    else:assert producer['state']=='open' and actual['xmin']!=producer['xmin']
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
    assert newest['state']=='released' and newest['control_fence_version']==1
    assert newest['control_backend_pid']>0 and newest['control_backend_start'] is not None
    assert newest['exclusion_method']=='storage_fenced' and len(passive.raw(newest['exclusion_digest'],32))==32
    assert newest['acquired_at_us']<=newest['released_at_us']<newest['lease_expires_at_us']
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
    assert role['created_at_us']<=newest['acquired_at_us']<=newest['released_at_us']<=role['retired_at_us']


def native_transition(before, after, initialized, *, retry=False):
    key='0131'+initialized['producer_slot_id']+'0000000000000001'
    select=lambda dump:{(r['cf'],r['key']):r['value'] for r in dump['rows']}
    old=select(before);new=select(after)
    assert len(old)==len(before['rows']) and len(new)==len(after['rows'])
    assert old.keys()==new.keys()
    assert {(cf,k):v for (cf,k),v in old.items() if (cf,k)!=('receipts',key)}=={
        (cf,k):v for (cf,k),v in new.items() if (cf,k)!=('receipts',key)}
    assert after['sequence']==before['sequence']+(0 if retry else 2)
    if retry:assert old==new


def completion(records, initialized):
    assert records==[dict(format='bigbrotr-ingest-recovery/1',state='positive-outcomes-resolved',
        dataset_id=initialized['dataset_id'],owner_resource_id=initialized['owner_resource_id'],
        producer_slot_id=initialized['producer_slot_id'],producer_epoch='1',issued_through='4',
        reserved_through='4',resolved_requests='4',native='closed',owner_release='committed',ready=False)]
