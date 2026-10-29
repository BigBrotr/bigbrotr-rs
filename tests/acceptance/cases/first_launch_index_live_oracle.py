"""Live successor observations over the existing independent topology science."""
import json
import re

from cases import first_launch_index_oracle as index
from cases import first_launch_index_topology_oracle as topology
from cases import first_launch_index_restart_oracle as restart

CONTROL_LOGIN = '''IFS= read -r live_index_password < /run/secrets/control || test -n "$live_index_password"
PGPASSWORD="$live_index_password"; export PGPASSWORD; unset live_index_password
exec psql -X -A -t -q --no-password -h 127.0.0.1 -p 5432 -U bigbrotr_control -d bigbrotr_control -v ON_ERROR_STOP=1 -v VERBOSITY=verbose'''
TOPOLOGY_REFUSAL = 'Index handoff publication requires current initial successor topology'


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


def append_only_history(before, after):
    """Retain each original publication row and its transaction identity exactly."""
    consumer=index.only(before['consumers'],consumer_role='index')['consumer_id']
    assert index.only(after['consumers'],consumer_role='index')['consumer_id']==consumer
    for table in index.INDEX_TABLES:
        assert all(row in after[table] for row in before[table]),table
    for table in ('consumer_frontier_revisions','consumer_frontiers'):
        historical=[r for r in before[table] if r['consumer_id']==consumer]
        assert all(row in after[table] for row in historical),table


def retained_bridge(before, after, root):
    """A real zero bridge precedes any epoch2 request or physical batch."""
    index.validate_metadata(before,root,complete=True)
    assert set(before)==set(after) and len(after)==82
    generation=index.only(after['projection_generations'])
    index.exact(before['projection_generations'],after['projection_generations'])
    append_only_history(before,after)
    for table in index.INDEX_TABLES:
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


def later_topology(before, after, report, *, stopped=False):
    """A genuine empty epoch3 adds lifecycle metadata, preserving all science."""
    assert type(stopped) is bool and set(before)==set(after) and len(after)==82
    mutable={'datasets','owner_epochs','role_instances','allocation_domains','producer_epochs',
             'source_streams','topology_generations','topology_streams','ingest_successions','ingest_activations'}
    for table in before.keys()-mutable:index.exact(before[table],after[table])
    old_dataset=index.only(before['datasets']);dataset=index.only(after['datasets'])
    assert old_dataset['topology_generation']==1 and dataset['topology_generation']==2
    for row in (old_dataset,dataset):assert row['config_revision']==row['policy_revision']==1
    index.exact({k:v for k,v in old_dataset.items() if k not in ('topology_generation','xmin')},
                {k:v for k,v in dataset.items() if k not in ('topology_generation','xmin')})
    counts=dict(owner_epochs=2,role_instances=2,allocation_domains=9,producer_epochs=1,
        source_streams=1,topology_generations=1,topology_streams=3,ingest_successions=1,ingest_activations=1)
    added={}
    for table,count in counts.items():
        assert all(row in after[table] for row in before[table]),table
        added[table]=[r for r in after[table] if r not in before[table]]
        assert len(added[table])==count,table
        assert all(r['dataset_id']==dataset['dataset_id'] for r in added[table]),table
    control=index.only(added['owner_epochs'],role='control');ingest=index.only(added['owner_epochs'],role='ingest')
    assert control['epoch']==max(r['epoch'] for r in before['owner_epochs'] if r['role']=='control')+1
    assert control['state']==('released' if stopped else 'active') and ingest['state']=='active'
    assert (control['released_at_us'] is not None)==stopped
    assert ingest['epoch']==3 and control['process_incarnation']==ingest['process_incarnation']
    assert ingest['resource_id']==index.only(before['owner_epochs'],role='ingest',epoch=2)['resource_id']
    assert control['process_incarnation'] not in {r['process_incarnation'] for r in before['owner_epochs']}
    index.exact(sorted(r['role'] for r in added['role_instances']),['control','ingest'])
    for owner in (control,ingest):
        role=index.only(added['role_instances'],role_instance_id=owner['role_instance_id'])
        assert role['role']==owner['role'] and role['dataset_id']==dataset['dataset_id']
        assert (role['retired_at_us'] is not None)==(stopped and owner['role']=='control')
    producer=index.only(added['producer_epochs']);source=index.only(added['source_streams'])
    predecessor=index.only(before['producer_epochs'],producer_epoch=2)
    closed=index.only(before['source_streams'],owner_epoch=2)
    assert predecessor['state']=='retired' and predecessor['reserved_through_request']==3
    assert closed['state']=='closed' and closed['last_complete_sequence']==10
    assert producer['producer_epoch']==producer['owner_epoch']==3 and producer['state']=='open'
    assert producer['reserved_through_request']==0 and producer['producer_slot_id']==predecessor['producer_slot_id']
    assert producer['outstanding_request_limit']==32
    assert producer['producer_role']==producer['owner_role']=='ingest'
    assert producer['producer_role_instance_id']==ingest['role_instance_id']
    assert producer['producer_incarnation']==producer['owner_incarnation']==ingest['process_incarnation']
    assert source['owner_epoch']==3 and source['state']=='open' and source['last_complete_sequence'] is None
    assert source['owner_resource_id']==ingest['resource_id'] and source['owner_incarnation']==ingest['process_incarnation']
    topology_row=index.only(added['topology_generations'])
    assert topology_row['generation']==2 and topology_row['previous_generation']==1 and topology_row['sealed'] is True
    index.exact(sorted((r['shard_id'],r['owner_epoch'],r['handoff_last_complete_sequence'])
                       for r in added['topology_streams']),[(0,1,12),(0,2,10),(0,3,None)])
    domains=added['allocation_domains']
    index.exact(sorted(r['dictionary_kind'] for r in domains),list(range(1,10)))
    assert len({r['allocation_domain'] for r in domains})==9
    assert {r['allocation_domain'] for r in domains}.isdisjoint(r['allocation_domain'] for r in before['allocation_domains'])
    assert all(r['owner_epoch']==3 and r['owner_resource_id']==ingest['resource_id']
        and r['owner_incarnation']==ingest['process_incarnation'] and r['state']=='active' for r in domains)
    succession=index.only(added['ingest_successions']);activation=index.only(added['ingest_activations'])
    for key,expected in dict(target_epoch=3,previous_epoch=2,previous_topology=1,target_topology=2,
        terminal_kind=1,issued_through=3,last_complete_sequence=10,closure_digest=closed['closure_digest'],
        target_incarnation=ingest['process_incarnation'],target_role_instance_id=ingest['role_instance_id'],
        owner_resource_id=ingest['resource_id'],producer_slot_id=predecessor['producer_slot_id']).items():
        index.exact(succession[key],expected)
    assert activation['target_epoch']==3 and activation['target_incarnation']==ingest['process_incarnation']
    assert activation['target_role_instance_id']==ingest['role_instance_id']
    assert activation['target_manifest_sha256']==succession['target_manifest_sha256']
    assert activation['handoff_sha256']==succession['handoff_sha256']
    assert activation['activated_by_epoch']==control['epoch'] and activation['activated_by_incarnation']==control['process_incarnation']
    for key,expected in dict(format='bigbrotr-ingest-successor/1',state='activated',
        dataset_id=dataset['dataset_id'][2:],owner_resource_id=ingest['resource_id'][2:],
        producer_slot_id=predecessor['producer_slot_id'][2:],producer_epoch='3',
        process_incarnation=ingest['process_incarnation'][2:],role_instance_id=ingest['role_instance_id'][2:],
        config_revision='1',policy_revision='1',outstanding_request_limit='32',acquisition='not-started',ready=False).items():
        index.exact(report[key],expected)
    index.exact(report['predecessor'],dict(producer_epoch='2',resolved_requests='3',
        last_complete_sequence='10',sha256=closed['closure_digest'][2:]))
    publication_unchanged(before,after)


def historical_sql(consumer, dataset):
    consumer=index.raw(consumer,16).hex();dataset=index.raw(dataset,16).hex()
    return fr'''BEGIN READ ONLY;
SELECT json_build_object('event','runtime_login','current_user',current_user,'session_user',session_user,
 'database',current_database(),'dataset_id','{dataset}','client_addr',inet_client_addr()::text,'read_only',current_setting('transaction_read_only'),
 'role_flags',(SELECT row_to_json(r) FROM (SELECT rolcanlogin,rolsuper,rolcreatedb,rolcreaterole,rolinherit,
 rolreplication,rolbypassrls FROM pg_roles WHERE rolname=current_user) r),
 'memberships',(SELECT count(*) FROM pg_auth_members WHERE member=(SELECT oid FROM pg_roles WHERE rolname=current_user)),
 'database_temp',has_database_privilege(current_database(),'TEMP'),'schema_create',has_schema_privilege('control','CREATE'),
 'dataset_topology',(SELECT topology_generation FROM control.datasets WHERE dataset_id=decode('{dataset}','hex')));
SELECT control.require_index_handoff_metadata(decode('{consumer}','hex'),5);
SELECT json_build_object('event','historical_metadata','consumer_id','{consumer}','frontier_revision',5,'status','PASS');
\set ON_ERROR_STOP off
SELECT control.require_index_handoff_authority(h) FROM control.index_frontier_handoffs h
 WHERE consumer_id=decode('{consumer}','hex') AND frontier_revision=5;
\echo authority_sqlstate :SQLSTATE
\set ON_ERROR_STOP on
ROLLBACK;
'''


def historical_sql_result(stdout, stderr, returncode, consumer, dataset):
    assert returncode==0
    lines=[line for line in stdout.splitlines() if line.strip()]
    assert len(lines)==3 and lines[2]=='authority_sqlstate 23514'
    identity=json.loads(lines[0]);metadata=json.loads(lines[1])
    index.exact(identity,dict(event='runtime_login',current_user='bigbrotr_control',session_user='bigbrotr_control',
        database='bigbrotr_control',dataset_id=index.raw(dataset,16).hex(),client_addr='127.0.0.1',read_only='on',role_flags=dict(rolcanlogin=True,
        rolsuper=False,rolcreatedb=False,rolcreaterole=False,rolinherit=False,rolreplication=False,rolbypassrls=False),
        memberships=0,database_temp=False,schema_create=False,dataset_topology=2))
    index.exact(metadata,dict(event='historical_metadata',consumer_id=index.raw(consumer,16).hex(),
                             frontier_revision=5,status='PASS'))
    errors=re.findall(r'^ERROR:\s+([A-Z0-9]{5}): (.*)$',stderr,re.M)
    index.exact(errors,[('23514',TOPOLOGY_REFUSAL)])
    assert len([line for line in stderr.splitlines() if line.startswith('ERROR:')])==1
    return dict(runtime_login=identity,historical_metadata=metadata,
        authority_refusal=dict(sqlstate='23514',message=TOPOLOGY_REFUSAL),transaction='read-only rolled back')
