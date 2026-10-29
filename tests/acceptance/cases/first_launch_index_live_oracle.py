"""Live successor observations over the existing independent topology science."""
import re

from cases import first_launch_index_oracle as index
from cases import first_launch_index_topology_oracle as topology
from cases import first_launch_index_restart_oracle as restart


def cut(dataset, through):
    assert type(through) is int and through in (0,10)
    return dict(dataset_id=index.raw(dataset,16).hex(),topology_generation='1',bounds=[
        dict(shard_id='0',owner_epoch=str(epoch),last_complete_sequence=str(last))
        for epoch,last in ((1,12),(2,through))])


def startup_index(value, generation):
    index.exact(value,dict(format='bigbrotr-live-initial-index/1',state='bridged',
        generation_id=index.raw(generation['generation_id'],16).hex(),
        storage_incarnation=index.raw(generation['storage_incarnation'],16).hex(),
        table_identity_digest=index.raw(generation['table_identity_digest'],32).hex(),
        previous_owner_epoch=1,owner_epoch=2,published_revision_before=4,published_revision_after=5,
        published_cut=cut(generation['dataset_id'],0),ready=False))


def retained_bridge(before, after, root):
    """A real zero bridge precedes any epoch2 request or physical batch."""
    index.validate_metadata(before,root,complete=True)
    assert set(before)==set(after) and len(after)==82
    generation=index.only(after['projection_generations'])
    index.exact(before['projection_generations'],after['projection_generations'])
    for table in index.INDEX_TABLES:
        assert all(row in after[table] for row in before[table]),table
        if table not in ('index_owner_sessions','index_frontier_handoffs'):
            index.exact(before[table],after[table])
    consumer=index.only(after['consumers'],consumer_role='index')
    topology.preserve_consumer_identity(before,after,consumer['consumer_id'])
    index.exact([consumer['current_revision'],consumer['current_topology_generation']],[5,1])
    index.exact(sorted(r['frontier_revision'] for r in after['consumer_frontier_revisions']
                      if r['consumer_id']==consumer['consumer_id']),[1,2,3,4,5])
    owners=sorted((r for r in after['owner_epochs'] if r['role']=='index'),key=lambda r:r['epoch'])
    index.exact([r['epoch'] for r in owners],[1,2])
    publisher=owners[1]
    assert publisher['state']=='active'
    control=index.only(after['owner_epochs'],role='control',state='active')
    ingest=index.only(after['owner_epochs'],role='ingest',epoch=2)
    assert control['process_incarnation']==publisher['process_incarnation']==ingest['process_incarnation']
    assert ingest['state']=='active'
    for epoch,producer_state,source_state,last in ((1,'retired','closed',12),(2,'open','open',None)):
        assert index.only(after['producer_epochs'],producer_epoch=epoch)['state']==producer_state
        source=index.only(after['source_streams'],owner_epoch=epoch)
        assert source['state']==source_state and source['last_complete_sequence']==last
    for table in ('producer_requests','producer_outcomes'):
        assert not [r for r in after[table] if r['producer_epoch']==2]
    binding=tuple(index.raw(generation[key]) for key in ('dataset_id','owner_resource_id','generation_id',
        'storage_incarnation','table_identity_digest','custody_binding_digest'))+(1,)
    handoff,_,bounds,_=topology.bridge_history(after,binding,publisher,generation)
    assert len(bounds)==6 and consumer['xmin']==handoff['xmin']
    commitments=[]
    for revision in range(1,5):
        plan=index.only(before['projection_plans'],frontier_revision=revision)
        receipt=index.only(before['index_frontier_plans'],frontier_revision=revision)
        commitments.append((revision,index.raw(plan['plan_digest'],32),index.raw(receipt['engine_receipt_digest'],32)))
    topology.recovery_sessions(after,binding,owners,commitments,handoff)
    return generation


def ready_roles(value, state, clock_before, clock_after):
    """Current full-vector physical verification while the real producer is open."""
    dataset=index.only(state['datasets'])
    control=index.only(state['owner_epochs'],role='control',state='active')
    generation=index.only(state['projection_generations'])
    assert index.only(state['producer_epochs'],producer_epoch=2)['state']=='open'
    source=index.only(state['source_streams'],owner_epoch=2)
    assert source['state']=='open' and source['last_complete_sequence'] is None
    expected=cut(dataset['dataset_id'],10)
    index.exact(sorted(value),sorted(('format','identity','observed_at_us','source','roles','capabilities','authority')))
    assert value['format']=='bigbrotr-role-status/1' and value['authority']=='observation-only'
    index.exact(value['identity'],dict(dataset_id=expected['dataset_id'],
        process_incarnation=index.raw(control['process_incarnation'],16).hex(),config_revision='1',policy_revision='1'))
    assert re.fullmatch('-?(0|[1-9][0-9]*)',value['observed_at_us'])
    observed=int(value['observed_at_us']);assert clock_before<=observed<=clock_after
    def fresh(observation):
        assert observation['freshness']=='fresh' and observation['reason']=='current'
        stamp=observation['time']
        assert set(stamp)=={'observed_at_us','age_us'} and stamp['observed_at_us']==str(observed)
        assert re.fullmatch('0|[1-9][0-9]*',stamp['age_us']) and 0<=int(stamp['age_us'])<30_000_000
    assert set(value['source'])=={'cut','freshness','time','reason'}
    index.exact(value['source']['cut'],expected);fresh(value['source'])
    index.exact(value['roles'],[dict(role=role,state='ready' if role=='control' else 'idle',reason='current')
                               for role in ('control','ingest','sync','index')])
    assert len(value['capabilities'])==3
    for cap,kind in zip(value['capabilities'],('signed_event_admission','finite_acquisition','index_publication')):
        assert set(cap)=={'capability','state','reason','freshness','time','published','generation','storage_incarnation'}
        assert cap['capability']==kind and cap['state']=='ready';fresh(cap)
        published=dict(revision='8',cut=expected) if kind=='index_publication' else None
        index.exact(cap['published'],published)
        for key in ('generation','storage_incarnation'):
            assert cap[key]==(index.raw(generation['generation_id' if key=='generation' else key],16).hex()
                              if kind=='index_publication' else None)


def publication_unchanged(before, after):
    """Stopping, closing and Archive catch-up preserve the live Index history."""
    for table in index.INDEX_TABLES:
        index.exact(before[table],after[table])
    consumer=index.only(before['consumers'],consumer_role='index')['consumer_id']
    for table in ('consumers','consumer_frontier_revisions','consumer_frontiers'):
        index.exact([r for r in before[table] if r['consumer_id']==consumer],
                    [r for r in after[table] if r['consumer_id']==consumer])


def no_work_rebind(before, after, report, science, schema, rows, *, owner_epoch):
    """A historical live bridge survives finite recovery without new publication."""
    assert type(owner_epoch) is int and owner_epoch in (3,4)
    restart.unchanged_except(before,after,{'owner_epochs','role_instances','index_owner_sessions'})
    restart.owner_changes(before,after,index_epoch=owner_epoch)
    assert all(row in after['index_owner_sessions'] for row in before['index_owner_sessions'])
    result=topology.published_history(after,science,schema,rows,owner_epoch=owner_epoch)
    generation=index.only(after['projection_generations'])
    # Finite reports use numeric cuts; management observations use decimal strings.
    numeric=dict(topology_generation=1,bounds=[dict(shard_id=0,owner_epoch=epoch,last_complete_sequence=last)
                                              for epoch,last in ((1,12),(2,10))])
    index.exact(report,dict(format='bigbrotr-initial-index-catch-up/2',state='complete',
        dataset_id=generation['dataset_id'][2:],generation_id=generation['generation_id'][2:],
        storage_incarnation=generation['storage_incarnation'][2:],table_identity_digest=generation['table_identity_digest'][2:],
        previous_owner_epoch=owner_epoch-1,owner_epoch=owner_epoch,published_revision_before=8,published_revision_after=8,
        source_epoch=2,source_through=10,published_through=10,source_cut=numeric,published_cut=numeric,
        index_handoff_revision=5,published_handoff=False,recovered_pending=False,new_batches=0,ready=False,
        acknowledgement='not-performed',ingest_retirement='not-performed',topology_transition='existing-ingest-handoff-verified'))
    result.update(exact_retry=True,historical_live_bridge=True)
    return result
