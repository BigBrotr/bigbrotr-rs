"""Independent PG publication-COMMIT branches over the closed signed history."""
import hashlib

from cases import first_launch_index_oracle as index
from cases import first_launch_index_restart_oracle as restart

MODES = ('before-forward','after-complete')
OWNERS = {'owner_epochs','role_instances','index_owner_sessions'}
PUBLICATION = {'projection_chunks','index_frontier_plans','consumer_frontier_revisions','consumer_frontiers','consumers'}
RELEASE = {'state','released_at_us','exclusion_method','exclusion_digest','xmin'}


def control_exclusion(previous, successor):
    return '\\x'+hashlib.sha256(b'bigbrotr/control-exclusion/1\0'
        +index.raw(previous['dataset_id'],16)+index.raw(previous['resource_id'],16)
        +index.be(previous['epoch'])+index.raw(previous['process_incarnation'],16)
        +index.raw(successor['process_incarnation'],16)).hexdigest()


def owner_delta(before, after, epoch, *, failed=False, exclude_control=False):
    prior_index = index.only(before['owner_epochs'],role='index',epoch=epoch-1)
    prior_control = max((r for r in before['owner_epochs'] if r['role']=='control'),key=lambda r:r['epoch'])
    known = {r['role_instance_id'] for r in before['owner_epochs']}
    added = [r for r in after['owner_epochs'] if r['role_instance_id'] not in known]
    index.exact(sorted(r['role'] for r in added),['control','index'])
    current = index.only(added,role='control'); new_index = index.only(added,role='index')
    assert current['resource_id'] == prior_control['resource_id'] and current['dataset_id'] == prior_control['dataset_id']
    assert current['epoch'] == prior_control['epoch']+1 and new_index['epoch'] == epoch
    assert current['process_incarnation'] == new_index['process_incarnation']
    assert current['process_incarnation'] not in {r['process_incarnation'] for r in before['owner_epochs']}
    assert current['index_publication_protocol'] is None and new_index['index_publication_protocol'] == 1
    assert current['state'] == ('active' if failed else 'released')
    assert (current['released_at_us'] is None) == failed
    if failed:
        assert current['exclusion_method'] is current['exclusion_digest'] is None
    else:
        assert current['exclusion_method'] == 'storage_fenced'
        assert current['exclusion_digest'] == control_exclusion(current,current)
    changed = {prior_index['role_instance_id']}
    if exclude_control:
        assert prior_control['state'] == 'active' and prior_control['released_at_us'] is None
        changed.add(prior_control['role_instance_id'])
        actual = index.only(after['owner_epochs'],role_instance_id=prior_control['role_instance_id'])
        retired = index.only(after['role_instances'],role_instance_id=prior_control['role_instance_id'])
        assert actual['state'] == 'released' and actual['exclusion_method'] == 'storage_fenced'
        assert actual['exclusion_digest'] == control_exclusion(prior_control,current)
        assert actual['released_at_us'] is not None and retired['retired_at_us'] is not None
        assert actual['xmin'] == retired['xmin']
    else:
        assert prior_control['state'] == 'released'
    for table,mutable in (('owner_epochs',RELEASE),('role_instances',{'retired_at_us','xmin'})):
        for row in before[table]:
            actual = index.only(after[table],role_instance_id=row['role_instance_id'])
            fields = mutable if row['role_instance_id'] in changed else set()
            index.exact({k:v for k,v in row.items() if k not in fields},
                        {k:v for k,v in actual.items() if k not in fields})
    roles = [r for r in after['role_instances'] if r['role_instance_id'] not in {r['role_instance_id'] for r in before['role_instances']}]
    assert {r['role_instance_id'] for r in roles} == {r['role_instance_id'] for r in added} and len(roles) == 2
    for owner in added:
        role = index.only(roles,role_instance_id=owner['role_instance_id'])
        assert role['role'] == owner['role'] and role['dataset_id'] == owner['dataset_id']
        assert (role['retired_at_us'] is None) == (failed or owner['role']=='index')
        if owner['role']=='control' and not failed:assert role['xmin'] == owner['xmin']
    assert all(row in after['index_owner_sessions'] for row in before['index_owner_sessions'])
    return current


def held(before, state, root, backend, now_us):
    index.validate_metadata(state,root,owner_count=2,publication_fault='before-forward')
    restart.unchanged_except(before,state,OWNERS)
    owner = owner_delta(before,state,2,failed=True)
    index.exact(backend,dict(pid=owner['control_backend_pid'],backend_start=owner['control_backend_start'],
        usename='bigbrotr_control',datname='bigbrotr_control'))
    assert owner['acquired_at_us'] <= now_us < owner['lease_expires_at_us']
    assert now_us < index.only(state['owner_epochs'],role='index',epoch=2)['lease_expires_at_us']
    return owner


def retained_owners(before, after):
    index.exact(before['role_instances'],after['role_instances'])
    index.exact(before['index_owner_sessions'],after['index_owner_sessions'])
    assert len(before['owner_epochs']) == len(after['owner_epochs'])
    for row in before['owner_epochs']:
        current = index.only(after['owner_epochs'],role_instance_id=row['role_instance_id'])
        fields = {'lease_expires_at_us','xmin'} if row['state']=='active' and row['role'] in ('control','index') else set()
        index.exact({k:v for k,v in row.items() if k not in fields},{k:v for k,v in current.items() if k not in fields})
        if fields:
            assert current['lease_expires_at_us'] >= row['lease_expires_at_us']
            assert (current['xmin'] == row['xmin']) == (current['lease_expires_at_us'] == row['lease_expires_at_us'])


def publication_delta(before, after):
    """Earlier receipts stay byte-for-byte; only previously prepared chunks close."""
    for table in ('projection_plans','projection_plan_chunks','projection_chunk_bounds','index_frontier_plans',
                  'consumer_frontier_revisions','consumer_frontiers'):
        assert all(row in after[table] for row in before[table]),table
    for row in before['projection_chunks']:
        current = index.only(after['projection_chunks'],chunk_id=row['chunk_id'])
        fields = {'state','committed_at_us','engine_receipt_digest','xmin'} if row['state']=='prepared' else set()
        index.exact({k:v for k,v in row.items() if k not in fields},{k:v for k,v in current.items() if k not in fields})
    for row in before['consumers']:
        current = index.only(after['consumers'],consumer_id=row['consumer_id'])
        fields = {'current_revision','xmin'} if row['consumer_role']=='index' else set()
        index.exact({k:v for k,v in row.items() if k not in fields},{k:v for k,v in current.items() if k not in fields})


def fault(before, after, evidence, root):
    mode = evidence['mode']; assert mode in MODES
    index.validate_metadata(after,root,owner_count=2,publication_fault=mode)
    restart.unchanged_except(before,after,PUBLICATION|{'owner_epochs'})
    retained_owners(before,after)
    if mode=='before-forward':
        for table in PUBLICATION:index.exact(before[table],after[table])
    else:
        publication_delta(before,after)
    owner = index.only(before['owner_epochs'],role='control',state='active')
    consumer = index.only(before['consumers'],consumer_role='index')['consumer_id'][2:]
    for event in (evidence['selected'],evidence['held'],evidence['fault']):
        assert event['mode'] == mode and event['target'] == 'control-index-publication'
        assert event['hold_index_publication'] is True and event['backend_pid'] == owner['control_backend_pid']
        index.exact([event[k] for k in ('index_consumer','index_revision','index_previous_revision','index_topology')],[consumer,3,2,0])
    index.exact([evidence[k]['event'] for k in ('selected','held','fault')],['index-publication-update-complete','held','fault'])
    assert evidence['held']['stage'] == 'index-publication-update'
    for event in (evidence['selected'],evidence['held']):
        index.exact([event[k] for k in ('data_queries','data_commits','server_complete','client_complete','faults')],[1,0,0,0,0])
    index.exact([evidence['fault'][k] for k in ('data_queries','data_commits','server_complete','client_complete','faults')],
                [1,1,int(mode=='after-complete'),0,1])
    assert evidence['exit_code'] == 4 and evidence['commit_unknown'] is True
    assert evidence['success_record'] is False and evidence['owner_release_claimed'] is False
    assert evidence['parent_after'] is None and evidence['backend_absent'] is True
    assert evidence['custody_held']['busy'] is True and evidence['custody_joined']['busy'] is False
    index.exact(evidence['custody_held']['identity'],evidence['custody_joined']['identity'])
    assert 0 < evidence['allow_ns']-evidence['held_started_ns'] < evidence['operation_limit_us']*1000


def recovered(before, after, root, mode, *, retry=False):
    assert mode in MODES
    epoch = 4 if retry else 3
    index.validate_metadata(after,root,complete=True,owner_count=epoch,publication_fault=mode)
    mutable = OWNERS if retry else OWNERS|PUBLICATION|{'projection_plans','projection_plan_chunks','projection_chunk_bounds'}
    restart.unchanged_except(before,after,mutable)
    owner_delta(before,after,epoch,exclude_control=not retry)
    if not retry:
        publication_delta(before,after)
        session = index.only(after['index_owner_sessions'],owner_epoch=3)
        fourth = index.only(after['index_frontier_plans'],frontier_revision=4)
        assert int(session['xmin']) < int(fourth['xmin'])
        third = index.only(after['index_frontier_plans'],frontier_revision=3)
        if mode=='before-forward':assert int(session['xmin']) < int(third['xmin']) < int(fourth['xmin'])
        else:assert int(third['xmin']) < int(session['xmin'])


def report(value, state, mode, *, retry=False):
    generation = index.only(state['projection_generations'])
    index.exact(value,dict(format='bigbrotr-initial-index-catch-up/2',state='complete',
        dataset_id=generation['dataset_id'][2:],generation_id=generation['generation_id'][2:],
        storage_incarnation=generation['storage_incarnation'][2:],table_identity_digest=generation['table_identity_digest'][2:],
        previous_owner_epoch=3 if retry else 2,owner_epoch=4 if retry else 3,
        published_revision_before=4 if retry else 2+int(mode=='after-complete'),published_revision_after=4,
        source_through=12,published_through=12,recovered_pending=not retry and mode=='before-forward',
        source_epoch=1,source_cut=dict(topology_generation=0,bounds=[dict(shard_id=0,owner_epoch=1,last_complete_sequence=12)]),
        published_cut=dict(topology_generation=0,bounds=[dict(shard_id=0,owner_epoch=1,last_complete_sequence=12)]),
        index_handoff_revision=None,published_handoff=False,
        new_batches=0 if retry else 1,ready=False,acknowledgement='not-performed',
        ingest_retirement='not-performed',topology_transition='not-performed'))
