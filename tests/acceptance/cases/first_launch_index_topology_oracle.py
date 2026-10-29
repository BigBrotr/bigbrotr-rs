"""Independent finite epoch1→2 Index expectations from the signed source model."""
import hashlib

from cases import first_launch_index_oracle as index
from cases import first_launch_index_restart_oracle as restart

# Existing signed initial event, distinct tagged successor event and predecessor
# resend: the original event has no second payload/receive/eligibility row.
COUNTS = (13,2,2,2,2,2,2,1,3,3)


def unchanged_index(before, after):
    """Source/Archive succession must leave the entire old Index state intact."""
    for table in index.INDEX_TABLES:
        index.exact(before[table],after[table])
    for table,field in (('consumers','consumer_role'),('consumer_frontier_revisions','consumer_role'),
                        ('owner_epochs','role'),('role_instances','role')):
        index.exact([r for r in before[table] if r[field]=='index'],
                    [r for r in after[table] if r[field]=='index'])
    consumer=index.only(before['consumers'],consumer_role='index')['consumer_id']
    index.exact([r for r in before['consumer_frontiers'] if r['consumer_id']==consumer],
                [r for r in after['consumer_frontiers'] if r['consumer_id']==consumer])


def physical_rows(actual, science, generation, schema):
    """Compare raw multiplicity and every typed field; no DISTINCT normalization."""
    rows = index.expected_rows(science,index.raw(generation,16),schema,counts=COUNTS)
    assert set(actual)==set(index.TARGETS)
    for name,columns,_ in schema.FAMILIES:
        expected = [{key:index.json_value(ty,value)
                     for (key,ty),value in zip(schema.COMMON+columns,row)} for row in rows[name]]
        index.exact(actual[name],expected)
    return rows


def preserve_consumer_identity(before, after, consumer_id):
    """Only the selected Index head may change; other consumers are exact."""
    for table in ('consumers','consumer_frontier_revisions','consumer_frontiers'):
        index.exact([r for r in before[table] if r['consumer_id']!=consumer_id],
                    [r for r in after[table] if r['consumer_id']!=consumer_id])
    allowed={'current_revision','current_topology_generation','xmin'}
    index.exact({k:v for k,v in index.only(before['consumers'],consumer_id=consumer_id).items() if k not in allowed},
                {k:v for k,v in index.only(after['consumers'],consumer_id=consumer_id).items() if k not in allowed})


def handoff_digest(binding, ingest, succession):
    """Literal current contract: one closed predecessor and one zero successor."""
    data=b'bigbrotr/index-initial-topology-handoff/1\0'+b''.join(binding[:4])+ingest
    data+=b''.join(binding[4:6])+bytes([binding[6]])+index.be(4)+succession
    for topology,bounds in ((0,[(0,1,12)]),(1,[(0,1,12),(0,2,0)])):
        data+=index.be(topology)+index.be(len(bounds),4)
        for shard,epoch,sequence in bounds:data+=index.be(shard,4)+index.be(epoch)+index.be(sequence)
    return hashlib.sha256(data).digest()


def catch_up_report(report, generation, *, retry):
    cut=dict(topology_generation=1,bounds=[dict(shard_id=0,owner_epoch=epoch,last_complete_sequence=last)
                                         for epoch,last in ((1,12),(2,10))])
    index.exact(report,dict(format='bigbrotr-initial-index-catch-up/2',state='complete',
        dataset_id=generation['dataset_id'][2:],generation_id=generation['generation_id'][2:],
        storage_incarnation=generation['storage_incarnation'][2:],table_identity_digest=generation['table_identity_digest'][2:],
        previous_owner_epoch=2 if retry else 1,owner_epoch=3 if retry else 2,
        published_revision_before=8 if retry else 4,published_revision_after=8,
        source_epoch=2,source_through=10,published_through=10,source_cut=cut,published_cut=cut,
        index_handoff_revision=5,published_handoff=not retry,recovered_pending=False,new_batches=0 if retry else 3,
        ready=False,acknowledgement='not-performed',ingest_retirement='not-performed',
        topology_transition='existing-ingest-handoff-verified'))


def batch_history(after, science, rows, binding, publisher, generation):
    """Verify batch metadata/engine receipts against independently encoded rows."""
    dataset,consumer_id,generation_id=(generation[key] for key in ('dataset_id','owner_resource_id','generation_id'))
    plans=sorted(after['projection_plans'],key=lambda r:r['frontier_revision'])
    handoff=index.only(after['index_frontier_handoffs'])
    commitments=[]
    for ordinal,(plan,batch) in enumerate(zip(plans,science['batches'])):
        revision=ordinal+1 if ordinal<4 else ordinal+2
        epoch,first,last=batch['epoch'],batch['first'],batch['last'];topology=epoch-1
        cuts=[(0,1,last)] if epoch==1 else [(0,1,12),(0,2,last)]
        previous=[(0,1,first-1)] if epoch==1 else [(0,1,12),(0,2,first-1)]
        chunks=[]
        for name in index.TARGETS:
            selected=[r for r in rows[name] if r[1][2]==epoch and first<=r[1][3]<=last]
            values=[[row[1],[name,row[6]],row] for row in selected]
            digest=index.digest('projection-chunk',[binding[2],name,1,[binding[0],topology,[list(x) for x in cuts]],0,values])
            chunks.append((len(selected),digest,digest))
        metadata=index.metadata_digest(binding,revision-1,(topology,previous),(topology,cuts),
            (bytes.fromhex(batch['admission_digest']),0,epoch,first,last),chunks)
        assert plan['plan_digest']=='\\x'+metadata.hex() and plan['source_batch_digest']=='\\x'+batch['admission_digest']
        engine=index.engine_digest(binding[4],metadata,chunks);commitments.append((revision,metadata,engine))
        receipt=index.only(after['index_frontier_plans'],frontier_revision=revision)
        if epoch==1:
            # The complete original generation was qualified before succession;
            # raw row/MVCC inclusion above preserves every historical receipt.
            continue
        fields=dict(dataset_id=dataset,consumer_id=consumer_id,generation_id=generation_id,frontier_revision=revision,
            expected_revision=revision-1,topology_generation=1,source_shard=0,source_owner_epoch=2,source_first=first,
            source_last=last,source_batch_digest='\\x'+batch['admission_digest'],plan_digest='\\x'+metadata.hex(),
            origin_epoch=2,origin_incarnation=publisher['process_incarnation'],prepared_at_us=plan['prepared_at_us'],sealed=True)
        index.exact({k:v for k,v in plan.items() if k!='xmin'},fields)
        common=dict(consumer_id=consumer_id,frontier_revision=revision,dataset_id=dataset,topology_generation=1)
        index.exact({k:v for k,v in receipt.items() if k not in ('xmin','committed_at_us')},dict(common,
            generation_id=generation_id,plan_digest=plan['plan_digest'],owner_epoch=2,
            owner_incarnation=publisher['process_incarnation'],engine_receipt_digest='\\x'+engine.hex()))
        assert handoff['committed_at_us']<=plan['prepared_at_us']<=receipt['committed_at_us']<publisher['lease_expires_at_us']
        for target,(count,digest,_) in zip(index.TARGETS,chunks):
            encoded='\\x'+digest.hex()
            member=index.only(after['projection_plan_chunks'],plan_digest=plan['plan_digest'],target=target)
            index.exact({k:v for k,v in member.items() if k!='xmin'},dict(generation_id=generation_id,
                plan_digest=plan['plan_digest'],target=target,chunk_id=encoded,partition_key='all',rows_digest=encoded))
            chunk=index.only(after['projection_chunks'],chunk_id=encoded)
            index.exact({k:v for k,v in chunk.items() if k!='xmin'},dict(generation_id=generation_id,chunk_id=encoded,
                dataset_id=dataset,storage_incarnation=generation['storage_incarnation'],table_identity_digest=generation['table_identity_digest'],
                target=target,target_schema_version=1,topology_generation=1,chunk_ordinal=revision,row_count=count,
                owner_resource_id=consumer_id,owner_epoch=2,owner_role='index',owner_incarnation=publisher['process_incarnation'],
                invalid_reason=None,state='committed',prepared_at_us=plan['prepared_at_us'],
                committed_at_us=receipt['committed_at_us'],engine_receipt_digest='\\x'+engine.hex()))
            assert chunk['xmin']==receipt['xmin']
            actual_bounds=sorted((r for r in after['projection_chunk_bounds'] if r['chunk_id']==encoded),key=lambda r:r['owner_epoch'])
            index.exact([{k:v for k,v in r.items() if k!='xmin'} for r in actual_bounds],
                [dict(generation_id=generation_id,chunk_id=encoded,dataset_id=dataset,topology_generation=1,
                      shard_id=s,owner_epoch=e,last_complete_sequence=n) for s,e,n in cuts])
    return commitments


def recovery_sessions(after, binding, owners, commitments, handoff):
    """Recompute each transferred owner's exact typed-history commitment."""
    generation=index.only(after['projection_generations'])
    dataset,consumer_id,generation_id=(generation[key] for key in ('dataset_id','owner_resource_id','generation_id'))
    owner_epoch=len(owners);bridge=index.raw(handoff['handoff_digest'],32)
    sessions=sorted(after['index_owner_sessions'],key=lambda r:r['owner_epoch'])
    index.exact([s['owner_epoch'] for s in sessions],list(range(1,owner_epoch+1)))
    for epoch,session in enumerate(sessions[1:],2):
        previous=owners[epoch-2];head=4 if epoch==2 else 8
        selected=[x for x in commitments if x[0]<=head]
        data=b'bigbrotr/index-recovery-observation/1\0'+b''.join(binding[:6])+bytes([binding[6]])
        data+=index.be(previous['epoch'])+index.raw(previous['process_incarnation'],16)+index.be(head)
        data+=b'\x01'+selected[-1][1]+b'\x00'
        for revision,metadata,engine in selected:
            if revision==6:
                data+=b'\x02'+index.be(4)+bridge+index.be(2)+index.raw(handoff['owner_incarnation'],16)
                data+=handoff['committed_at_us'].to_bytes(8,'big',signed=True)
            data+=b'\x00'+index.be(revision-1)+metadata+engine
        expected='\\x'+hashlib.sha256(data).hexdigest()
        index.exact([session[k] for k in ('previous_epoch','recovered_revision','recovered_plan','recovered_handoff','recovered_pending_plan','recovery_digest')],
            [epoch-1,head,'\\x'+selected[-1][1].hex(),None,None,expected])
        assert previous['state']=='released' and previous['exclusion_method']=='storage_fenced' and previous['exclusion_digest']==expected
        retired=index.only(after['role_instances'],role_instance_id=previous['role_instance_id'])
        assert previous['xmin']==retired['xmin']==session['xmin'] and retired['retired_at_us'] is not None
        current=owners[epoch-1]
        assert session['resource_id']==consumer_id and session['generation_id']==generation_id and session['dataset_id']==dataset
        assert session['owner_role']=='index' and session['owner_incarnation']==current['process_incarnation']
        control=index.only(after['owner_epochs'],role='control',resource_id=session['issuer_resource'],epoch=session['issuer_epoch'])
        assert session['issuer_role']=='control' and session['issuer_incarnation']==control['process_incarnation']==current['process_incarnation']


def catch_up(before, after, report, science, schema, root, actual, *, retry=False):
    """Closed seven-batch/one-bridge history, independently encoded per family."""
    assert type(retry) is bool
    index.exact(sorted(before),sorted(after));assert len(after)==82
    owner_epoch=3 if retry else 2
    if not retry:index.validate_metadata(before,root,complete=True)
    mutable={'owner_epochs','role_instances','index_owner_sessions'}
    if not retry:mutable.update(index.INDEX_TABLES+('index_frontier_handoffs','consumers','consumer_frontiers','consumer_frontier_revisions'))
    for table in before.keys()-mutable:index.exact(before[table],after[table])
    restart.owner_changes(before,after,index_epoch=owner_epoch)
    for table in ('index_owner_sessions','projection_plans','projection_plan_chunks','projection_chunks',
                  'projection_chunk_bounds','index_frontier_plans','index_frontier_handoffs',
                  'consumer_frontier_revisions','consumer_frontiers'):
        assert all(row in after[table] for row in before[table]),table
    index.exact(before['projection_generations'],after['projection_generations'])
    consumer_id=index.only(after['projection_generations'])['owner_resource_id']
    preserve_consumer_identity(before,after,consumer_id)
    result=published_history(after,science,schema,actual,owner_epoch=owner_epoch)
    catch_up_report(report,index.only(after['projection_generations']),retry=retry)
    result['exact_retry']=retry
    return result


def bridge_history(after, binding, publisher, generation):
    """Verify the one receipt-free topology bridge and its atomic zero bound."""
    dataset,consumer_id,generation_id=(generation[key] for key in ('dataset_id','owner_resource_id','generation_id'))
    revisions=[r for r in after['consumer_frontier_revisions'] if r['consumer_id']==consumer_id]
    bounds=[r for r in after['consumer_frontiers'] if r['consumer_id']==consumer_id]
    handoff=index.only(after['index_frontier_handoffs'])
    succession=index.only(after['initial_ingest_successions'])
    bridge=handoff_digest(binding,index.raw(succession['owner_resource_id'],16),index.raw(succession['handoff_sha256'],32))
    expected=dict(consumer_id=consumer_id,frontier_revision=5,expected_revision=4,dataset_id=dataset,
        topology_generation=1,generation_id=generation_id,handoff_digest='\\x'+bridge.hex(),
        ingest_resource=succession['owner_resource_id'],succession_digest=succession['handoff_sha256'],
        source_shard=0,predecessor_through=12,owner_epoch=2,owner_incarnation=publisher['process_incarnation'])
    index.exact({k:v for k,v in handoff.items() if k not in ('xmin','committed_at_us')},expected)
    assert publisher['acquired_at_us']<=handoff['committed_at_us']<publisher['lease_expires_at_us']
    bridge_bounds=sorted((r for r in bounds if r['frontier_revision']==5),key=lambda r:r['owner_epoch'])
    old_bound=index.only(bounds,frontier_revision=4)
    index.exact({k:v for k,v in bridge_bounds[0].items() if k not in ('xmin','frontier_revision','topology_generation')},
                {k:v for k,v in old_bound.items() if k not in ('xmin','frontier_revision','topology_generation')})
    index.exact([bridge_bounds[1][k] for k in ('shard_id','owner_epoch','last_complete_sequence','attestation_kind','attestation_digest')],
                [0,2,0,None,None])
    revision=index.only(revisions,frontier_revision=5)
    index.exact({k:v for k,v in revision.items() if k not in ('xmin','created_at_us')},dict(consumer_id=consumer_id,
        frontier_revision=5,dataset_id=dataset,consumer_role='index',topology_generation=1,previous_revision=4,
        owner_resource_id=consumer_id,owner_epoch=2,owner_incarnation=publisher['process_incarnation'],sealed=True))
    assert revision['xmin']==handoff['xmin'] and revision['created_at_us']==handoff['committed_at_us']
    assert len(bridge_bounds)==2 and all(r['xmin']==handoff['xmin'] for r in bridge_bounds)
    assert not [r for r in after['index_frontier_plans'] if r['frontier_revision']==5]
    assert not [r for r in after['projection_plans'] if r['frontier_revision']==5]
    return handoff,revisions,bounds,old_bound


def published_history(after, science, schema, actual, *, owner_epoch):
    """The same typed seven-batch history, whether published live or finitely."""
    assert type(owner_epoch) is int and owner_epoch in (2,3,4)
    generation=index.only(after['projection_generations'])
    binding=tuple(index.raw(generation[key]) for key in ('dataset_id','owner_resource_id','generation_id',
        'storage_incarnation','table_identity_digest','custody_binding_digest'))+(1,)
    dataset,consumer_id,generation_id=(generation[key] for key in ('dataset_id','owner_resource_id','generation_id'))
    consumer=index.only(after['consumers'],consumer_role='index')
    assert consumer['consumer_id']==consumer_id and consumer['dataset_id']==dataset
    index.exact([consumer['current_revision'],consumer['current_topology_generation']],[8,1])
    owners=sorted((r for r in after['owner_epochs'] if r['role']=='index'),key=lambda r:r['epoch'])
    index.exact([r['epoch'] for r in owners],list(range(1,owner_epoch+1)))
    publisher=owners[1]
    rows=physical_rows(actual,science,generation_id,schema)
    plans=sorted(after['projection_plans'],key=lambda r:r['frontier_revision'])
    index.exact([p['frontier_revision'] for p in plans],[1,2,3,4,6,7,8])
    index.exact([(b['epoch'],b['first'],b['last']) for b in science['batches']],
                [(1,1,2),(1,3,3),(1,4,7),(1,8,12),(2,1,4),(2,5,9),(2,10,10)])
    assert len(after['projection_plan_chunks'])==len(after['projection_chunks'])==70
    assert len(after['projection_chunk_bounds'])==100 and len(after['index_frontier_plans'])==7
    handoff,revisions,bounds,old_bound=bridge_history(after,binding,publisher,generation)
    index.exact(sorted(r['frontier_revision'] for r in revisions),list(range(1,9)))
    assert len(bounds)==12
    commitments=batch_history(after,science,rows,binding,publisher,generation)
    # Frontier publication is one transaction, including the bridge's carried
    # attestation and zero successor. No handoff fabricates a physical receipt.
    for revision in range(5,9):
        receipt=handoff if revision==5 else index.only(after['index_frontier_plans'],frontier_revision=revision)
        rev=index.only(revisions,frontier_revision=revision)
        index.exact({k:v for k,v in rev.items() if k not in ('xmin','created_at_us')},dict(consumer_id=consumer_id,
            frontier_revision=revision,dataset_id=dataset,consumer_role='index',topology_generation=1,previous_revision=revision-1,
            owner_resource_id=consumer_id,owner_epoch=2,owner_incarnation=publisher['process_incarnation'],sealed=True))
        assert rev['xmin']==receipt['xmin'] and rev['created_at_us']==receipt['committed_at_us']
        last=(0,4,9,10)[revision-5]
        current=sorted((r for r in bounds if r['frontier_revision']==revision),key=lambda r:r['owner_epoch'])
        for row,epoch,through in zip(current,(1,2),(12,last)):
            attestation=old_bound['attestation_digest'] if revision==5 and epoch==1 else None if not through else receipt['plan_digest']
            index.exact(row,dict(consumer_id=consumer_id,frontier_revision=revision,dataset_id=dataset,topology_generation=1,
                shard_id=0,owner_epoch=epoch,last_complete_sequence=through,attestation_kind='projection-chunk' if through else None,
                attestation_digest=attestation,xmin=receipt['xmin']))
        assert len(current)==2
    assert consumer['xmin']==index.only(after['index_frontier_plans'],frontier_revision=8)['xmin']
    recovery_sessions(after,binding,owners,commitments,handoff)
    return dict(format='bigbrotr-installed-index-topology/1',topology_generation=1,source_cuts=[[1,12],[2,10]],
        frontier_revision=8,handoff_revision=5,source_batches=7,committed_chunks=70,chunk_bounds=100,
        family_counts=dict(zip(index.TARGETS,COUNTS)),index_owner_epoch=owner_epoch,exact_retry=False,
        original_receipts='unchanged',all_typed_fields_equal=True,raw_multiplicity='exact',complete_M1='NOT_RUN')
