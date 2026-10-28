"""Closed installed backlog/rebind controls over the maintained Index oracle.

The original initial science and ten-family encoders remain the owners of bytes.
Every phase compares all82 Control tables, with explicit permitted transitions.
"""
from cases import first_launch_index_oracle as index
from cases.first_launch_restart import closure


def prefix_profile(state, config):
    index.accepted_profile(state)
    assert config['archive']['max_publish_delay'] == '1s'
    for table in ('config_revisions','policy_revisions'):
        document = index.only(state[table])['document']
        index.exact(document['settings']['archive.max_publish_delay'],1_000_000)


def backlog(state, root, *, prefix=False):
    index.validate_metadata(state, root)
    consumer = index.only(state['consumers'], consumer_role='index')
    assert consumer['current_revision'] == 2
    bound = index.only(state['consumer_frontiers'],consumer_id=consumer['consumer_id'],frontier_revision=2)
    assert bound['last_complete_sequence'] == 3
    index.exact([r['frontier_revision'] for r in sorted(state['projection_plans'],key=lambda r:r['frontier_revision'])],
                [1,2] if prefix else [1,2,3])
    archive = index.only(state['consumers'],consumer_role='archive')
    assert archive['current_revision'] == (2 if prefix else 4)
    bound = index.only(state['consumer_frontiers'],consumer_id=archive['consumer_id'],frontier_revision=archive['current_revision'])
    assert bound['last_complete_sequence'] == (3 if prefix else 12)
    assert len(state['producer_requests']) == len(state['producer_outcomes']) == (2 if prefix else 4)
    if prefix:
        assert not state['acquisition_scopes'] and not state['jobs']
    else:
        plan = index.only(state['projection_plans'],frontier_revision=3)
        assert (plan['source_first'],plan['source_last']) == (4,7)
        assert len([r for r in state['projection_chunks'] if r['state']=='prepared']) == 10


def unchanged_except(before, after, mutable):
    index.exact(sorted(before),sorted(after))
    assert len(before) == 82
    for table in before.keys() - mutable:
        index.exact(before[table],after[table])


def owner_changes(before, after, *, index_epoch=None):
    """One finite Control lifetime, optionally one positively fenced Index transfer."""
    changed_owner = index.only(before['owner_epochs'],role='index',epoch=index_epoch-1) if index_epoch else None
    changed_role = changed_owner['role_instance_id'] if changed_owner else None
    for table,key,allowed in [('owner_epochs','role_instance_id',changed_role),
                              ('role_instances','role_instance_id',changed_role)]:
        for row in before[table]:
            actual = index.only(after[table],**{key:row[key]})
            if row[key] == allowed:
                fields = {'state','released_at_us','exclusion_method','exclusion_digest','xmin'} if table=='owner_epochs' else {'retired_at_us','xmin'}
                index.exact({k:v for k,v in actual.items() if k not in fields},{k:v for k,v in row.items() if k not in fields})
            else:
                index.exact(actual,row)
    known = {r['role_instance_id'] for r in before['owner_epochs']}
    added = [r for r in after['owner_epochs'] if r['role_instance_id'] not in known]
    index.exact(sorted(r['role'] for r in added), ['control','index'] if index_epoch else ['control'])
    control = index.only(added,role='control')
    assert control['state'] == 'released' and control['released_at_us'] is not None
    assert control['epoch'] == max(r['epoch'] for r in before['owner_epochs'] if r['role']=='control')+1
    assert control['resource_id'] == index.only(before['owner_epochs'],role='control',epoch=control['epoch']-1)['resource_id']
    assert control['process_incarnation'] not in {r['process_incarnation'] for r in before['owner_epochs']}
    new_roles = [r for r in after['role_instances'] if r['role_instance_id'] not in {r['role_instance_id'] for r in before['role_instances']}]
    assert len(new_roles) == len(added)
    assert {r['role_instance_id'] for r in added} == {r['role_instance_id'] for r in new_roles}
    for row in added:
        role = index.only(new_roles,role_instance_id=row['role_instance_id'])
        assert role['role'] == row['role'] and role['dataset_id'] == row['dataset_id']
        assert row['process_incarnation'] == control['process_incarnation']
        assert row['index_publication_protocol'] == (1 if row['role']=='index' else None)
        if row['role']=='index':
            assert row['epoch'] == index_epoch and role['retired_at_us'] is None


def source_closed(before, after, report, science):
    unchanged_except(before,after,{'owner_epochs','role_instances','producer_epochs','source_streams'})
    owner_changes(before,after)
    prior = index.only(before['source_streams'])
    current = index.only(after['source_streams'])
    assert prior['state'] == 'open' and current['state'] == 'closed'
    original_producer = index.only(before['producer_epochs'])
    producer = index.only(after['producer_epochs'])
    # Fresh source closure first commits the producer's scheduling veto. This
    # is the native/PG closing barrier, not producer retirement or a new epoch.
    assert original_producer['state'] == 'open' and producer['state'] == 'closing'
    fields = {'state','xmin'}
    index.exact({k:v for k,v in original_producer.items() if k not in fields},
                {k:v for k,v in producer.items() if k not in fields})
    assert int(original_producer['xmin']) < int(producer['xmin']) < int(current['xmin'])
    expected = closure(science['manifest'],producer['producer_slot_id'],1,science['inputs'],science['receipts'],12)
    fields = {'state','last_complete_sequence','closure_digest','closed_at_us','xmin'}
    index.exact({k:v for k,v in prior.items() if k not in fields},{k:v for k,v in current.items() if k not in fields})
    assert current['last_complete_sequence'] == 12 and current['closure_digest'] == '\\x'+expected
    assert current['closed_at_us'] is not None
    index.exact(report,dict(format='bigbrotr-ingest-recovery/1',state='source-closed',dataset_id=current['dataset_id'][2:],
        owner_resource_id=current['owner_resource_id'][2:],producer_slot_id=producer['producer_slot_id'][2:],
        producer_epoch='1',issued_through='4',reserved_through='4',resolved_requests='4',
        native='closed',owner_release='committed',ready=False,
        source_closure=dict(last_complete_sequence='12',sha256=expected)))


def catch_up_report(report, state, *, retry=False):
    generation = index.only(state['projection_generations'])
    index.exact(report,dict(format='bigbrotr-initial-index-catch-up/2',state='complete',
        dataset_id=generation['dataset_id'][2:],generation_id=generation['generation_id'][2:],
        storage_incarnation=generation['storage_incarnation'][2:],table_identity_digest=generation['table_identity_digest'][2:],
        previous_owner_epoch=2 if retry else 1,owner_epoch=3 if retry else 2,
        published_revision_before=4 if retry else 2,published_revision_after=4,source_through=12,published_through=12,
        source_epoch=1,source_cut=dict(topology_generation=0,bounds=[dict(shard_id=0,owner_epoch=1,last_complete_sequence=12)]),
        published_cut=dict(topology_generation=0,bounds=[dict(shard_id=0,owner_epoch=1,last_complete_sequence=12)]),
        index_handoff_revision=None,published_handoff=False,
        recovered_pending=not retry,new_batches=0 if retry else 1,ready=False,
        acknowledgement='not-performed',ingest_retirement='not-performed',topology_transition='not-performed'))


def recovered(before, after, root, *, retry=False):
    epoch = 3 if retry else 2
    index.validate_metadata(after,root,complete=True,owner_count=epoch)
    mutable = {'owner_epochs','role_instances','index_owner_sessions'}
    if not retry:
        mutable.update({'projection_plans','projection_plan_chunks','projection_chunks','projection_chunk_bounds',
                        'index_frontier_plans','consumers','consumer_frontier_revisions','consumer_frontiers'})
    unchanged_except(before,after,mutable)
    owner_changes(before,after,index_epoch=epoch)
    assert all(row in after['index_owner_sessions'] for row in before['index_owner_sessions'])
    if not retry:
        backlog(before,root)
        for table in ('projection_plans','projection_plan_chunks','projection_chunk_bounds','index_frontier_plans',
                      'consumer_frontier_revisions','consumer_frontiers'):
            assert all(row in after[table] for row in before[table]), table
        for row in before['projection_chunks']:
            current = index.only(after['projection_chunks'],chunk_id=row['chunk_id'])
            if row['state']=='committed':
                index.exact(current,row)
            else:
                fields = {'state','committed_at_us','engine_receipt_digest','xmin'}
                index.exact({k:v for k,v in current.items() if k not in fields},
                            {k:v for k,v in row.items() if k not in fields})
        for row in before['consumers']:
            current = index.only(after['consumers'],consumer_id=row['consumer_id'])
            fields = {'current_revision','xmin'} if row['consumer_role']=='index' else set()
            index.exact({k:v for k,v in current.items() if k not in fields},{k:v for k,v in row.items() if k not in fields})
        # Rebind and publication are separate transactions; pending physical
        # verification cannot masquerade as the accepted PG revision3.
        session = index.only(after['index_owner_sessions'],owner_epoch=2)
        third = index.only(after['index_frontier_plans'],frontier_revision=3)
        fourth = index.only(after['index_frontier_plans'],frontier_revision=4)
        assert int(session['xmin']) < int(third['xmin']) < int(fourth['xmin'])
