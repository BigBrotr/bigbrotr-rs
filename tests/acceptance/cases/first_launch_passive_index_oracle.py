"""Literal cut10 passive Index expectations, independent of the Rust row mapper.

The signed-event cut12 oracle remains a separate fixture. Its schema/commitment
encoders are reused here; its scientific expectations and PASS criteria are not.
"""
import hashlib

from cases import first_launch_index_oracle as index
from cases import first_launch_passive_oracle as passive

BATCH_MAX_ROWS = 6  # Request3 atomically admits all six descriptors at4..9.
COUNTS = (9,0,0,0,0,0,0,0,0,0,1)
RANGES = ((1,2),(3,3),(4,9),(10,10))
PLAN_COUNTS = ((2,0,0,0,0,0,0,0,0,0,0),
               (1,0,0,0,0,0,0,0,0,0,0),
               (6,0,0,0,0,0,0,0,0,0,0),
               (0,0,0,0,0,0,0,0,0,0,1))


def accepted_profile(state):
    dataset, config, policy = (passive.only(state[t]) for t in ('datasets','config_revisions','policy_revisions'))
    index.exact([dataset['config_revision'],dataset['policy_revision'],config['revision'],
        policy['revision'],policy['config_revision']],[1]*5)
    index.exact(config['document'],policy['document'])
    assert config['document_digest'] == policy['document_digest']
    assert policy['document']['format'] == 'bigbrotr-product-policy/3'
    settings = policy['document']['settings']
    for key,value in {'indexer.enabled':True,'indexer.batch_max_rows':6,'indexer.insert_workers':1,
            'indexer.query_memory':268435456,'sync.enabled':False,'sync.provenance':'local_first_witness',
            'monitor.enabled':True,'monitor.checks':['open','read'],'monitor.store_checks':['open','read'],
            'monitor.check_timeout':15000000,'monitor.retries':1}.items():
        index.exact(settings[key],value)


def source_completed(state, report, initialized, decode):
    """Positive retained receipts, before waiting for independent consumers."""
    outcomes=sorted(state['producer_outcomes'],key=lambda r:r['request_sequence'])
    requests=sorted(state['producer_requests'],key=lambda r:r['request_sequence'])
    assert [r['request_sequence'] for r in outcomes]==[1,2,3,4]
    assert [r['request_sequence'] for r in requests]==[1,2,3,4]
    dataset=bytes.fromhex(initialized['private_scope']['dataset_id'])
    slot=bytes.fromhex(initialized['producer_slot_id'])
    for number,(outcome,request,(first,last)) in enumerate(zip(outcomes,requests,RANGES),1):
        encoded=passive.raw(outcome['outcome_bytes'])
        assert encoded.startswith(b'bigbrotr/receipt/1\0') and outcome['outcome_kind']==1
        assert passive.raw(outcome['outcome_sha256'],32)==hashlib.sha256(encoded).digest()
        receipt=decode(encoded.split(b'\0',1)[1])
        assert receipt[:4]==[1,[slot,1,number],passive.raw(request['request_digest'],32),
            [dataset,0,1,bytes.fromhex(initialized['recovery_generation']),1]]
        assert len(receipt)==6 and len(receipt[4])==last-first+1
        for sequence,reference in zip(range(first,last+1),receipt[4]):
            assert len(reference)==2 and reference[0]==[dataset,0,1,sequence] and len(reference[1])==32
        if number==4:
            assert receipt[4][0][1].hex()==report['measurement']['payload_digest']
            assert receipt[2].hex()==report['request_digest']


def expected(state, initialized, refs, originals, receipts, document, measurement, encoded,
             actual, before_us, connected_us, schema, cbor, decode):
    """Reconstruct every source fact and compare its original receipt commitment.

    The first three times come from initial registration. The later dictionary
    time is one bounded observed scalar, then cryptographically bound to the
    independently specified six descriptors and the retained receipt. No other
    ClickHouse value supplies an expected scientific field.
    """
    assert tuple(name for name,_,_ in schema.FAMILIES) == index.TARGETS
    assert len(originals) == len(receipts) == 4
    dataset = bytes.fromhex(initialized['private_scope']['dataset_id'])
    slot = bytes.fromhex(initialized['producer_slot_id'])
    scope = passive.reference(refs['visibility_scope_id'])
    public = [*scope[:3],1]
    assert passive.reference(initialized['public_scope']) == public
    owner = passive.only(state['owner_epochs'],role='ingest',epoch=1)
    created = owner['acquired_at_us']
    assert type(created) is int and before_us <= created <= connected_us
    dictionary_rows = actual['dictionaries']
    assert len(dictionary_rows) == 9
    clock_row = passive.only(dictionary_rows,source=[dataset.hex(),0,1,4],discriminator=0)
    dictionary_time = clock_row['recorded_at']
    assert type(dictionary_time) is int and created <= dictionary_time <= connected_us
    first = [['dictionary',1,[[*public[:3],n],1,'visibility',1,{'class':label}]]
        for n,label in ((1,'public'),(2,'private'))]
    second = [['dictionary',1,[scope,1,'visibility',1,{'class':'private'}]]]
    dictionary_input = passive.dictionary_request(refs,initialized['producer_slot_id'],cbor)
    inputs = [b'bigbrotr/producer-request/1\0'+cbor([[slot,1,n],'admit-evidence',1,[1,visibility,entries]])
        for n,visibility,entries in ((1,public,first),(2,scope,second))]+[dictionary_input]
    assert originals[:3] == inputs
    entries = decode(dictionary_input.split(b'\0',1)[1])[3][2]
    assert len(entries) == 6
    facts = []
    for sequence,(entry,visibility,at) in enumerate(
            [(entry,public,created) for entry in first]+[(second[0],scope,created)]+
            [(entry,scope,dictionary_time) for entry in entries],1):
        assert entry[:2] == ['dictionary',1]
        facts.append([[dataset,0,1,sequence],'dictionary',1,visibility,at,entry[2]])
    fact = document['fact']
    measured = [[dataset,0,1,10],'measurement',1,scope,passive.integer(fact['recorded_at'],signed=True),measurement]
    assert encoded == b'bigbrotr/fact/1\0'+cbor(measured)
    facts.append(measured)
    # The surrounding passive journey has independently verified request4,
    # original request digests and all receipt/source/dependency identities.
    science = dict(batches=[])
    for receipt,(first,last) in zip(receipts,RANGES):
        selected = []
        for value,reference in zip(facts[first-1:last],receipt[4]):
            preimage = b'bigbrotr/fact/1\0'+cbor(value)
            assert reference == [value[0],hashlib.sha256(preimage).digest()]
            selected.append(dict(preimage_hex=preimage.hex()))
        assert len(selected) == last-first+1 == len(receipt[4])
        admission = [dataset,0,1,first,last,receipt[4]]
        science['batches'].append(dict(first=first,last=last,facts=selected,
            admission_digest=hashlib.sha256(b'bigbrotr/admission-batch/1\0'+cbor(admission)).hexdigest()))
    generation = index.raw(passive.only(state['projection_generations'])['generation_id'],16)
    rows = {name:[] for name in index.TARGETS}
    for source,family,version,visibility,at,payload in facts:
        common = [generation,source,version,visibility,at,
            hashlib.sha256(b'bigbrotr/fact/1\0'+cbor([source,family,version,visibility,at,payload])).digest(),0]
        if family == 'dictionary':
            reference,normalization,name,version,fields = payload
            values = [fields.get('visibility_scope_id' if n=='access_visibility_scope' else n)
                for n,_ in schema.DICTIONARY[5:]]
            rows['dictionaries'].append(common+[reference,normalization,name,version,
                index.digest('dictionary',payload),*values])
        else:
            attempt,context,profile,quality,state_value = payload
            assert state_value[0] == 2
            captured,ended,termination,teardown = state_value[1]
            execution,opened,read,counters = captured
            assert execution[1] == [0] and termination == [0]
            rows['measurement_facts'].append(common+[attempt,context[0][0],context[0][1],*context[1:],
                profile,quality,2,None,[execution[0],0,None,execution[2]],opened,read,counters,
                ended,[0,None,None,None],teardown])
    assert tuple(len(rows[n]) for n in index.TARGETS) == COUNTS
    return science,rows


def physical(actual, rows, schema):
    assert set(actual) == set(index.TARGETS)
    for name,columns,_ in schema.FAMILIES:
        expected = [{key:index.json_value(ty,value) for (key,ty),value in zip(schema.COMMON+columns,row)}
            for row in rows[name]]
        assert all(len(row)==len(schema.COMMON+columns) for row in rows[name])
        index.exact(actual[name],expected)


def publication(state, root, science, rows, schema):
    """One initial owner and exactly four complete passive plans, including zeros."""
    accepted_profile(state)
    identity,tables,custody = index.retained(root)
    index.table_identity(schema,root)
    dataset = passive.only(state['datasets'])['dataset_id']
    owners = [r for r in state['owner_epochs'] if r['role']=='index']
    owner, = owners
    generation = passive.only(state['projection_generations'])
    session = passive.only(state['index_owner_sessions'])
    assert {r['role'] for r in state['owner_epochs']} == {'control','ingest','archive','index'}
    assert owner['epoch'] == 1 and owner['state'] in ('active','released')
    for key,field in [('dataset','dataset_id'),('resource','owner_resource_id'),('generation','generation_id'),
            ('storage_incarnation','storage_incarnation'),('table_identity_digest','table_identity_digest')]:
        assert generation[field] == '\\x'+identity[key]
    assert dataset == generation['dataset_id'] and generation['custody_binding_digest'] == '\\x'+custody.hex()
    assert generation['index_publication_protocol'] == owner['index_publication_protocol'] == 1
    assert generation['provenance'] == 'local_first_witness' and generation['owner_role'] == 'index'
    assert owner['resource_id'] == session['resource_id'] == generation['owner_resource_id']
    assert owner['process_incarnation'] == generation['owner_incarnation'] == session['owner_incarnation']
    for key,value in dict(dataset_id=dataset,generation_id=generation['generation_id'],owner_epoch=1,
            owner_role='index',issuer_role='control',previous_epoch=None,recovery_digest=None,recovered_plan=None,
            recovered_handoff=None,recovered_pending_plan=None,recovered_revision=0).items():
        index.exact(session[key],value)
    assert generation['owner_epoch'] == 1
    control = passive.only(state['owner_epochs'],role='control',resource_id=session['issuer_resource'],epoch=session['issuer_epoch'])
    assert session['issuer_incarnation'] == control['process_incarnation'] == owner['process_incarnation']
    role = passive.only(state['role_instances'],role_instance_id=owner['role_instance_id'])
    assert role['role']=='index' and role['dataset_id']==dataset and role['retired_at_us'] is None
    consumers = state['consumers']
    assert len(consumers)==2 and {r['consumer_role'] for r in consumers}=={'archive','index'}
    consumer = passive.only(consumers,consumer_role='index')
    assert consumer['consumer_id']==owner['resource_id'] and consumer['dataset_id']==dataset
    index.exact([consumer['current_revision'],consumer['current_topology_generation']],[4,0])
    assert len(state['projection_plans'])==len(state['index_frontier_plans'])==4
    assert not state['index_frontier_handoffs']
    assert all(len(state[t])==44 for t in ('projection_chunks','projection_plan_chunks','projection_chunk_bounds'))
    consumer_ids = {r['consumer_id'] for r in consumers}
    assert all(r['consumer_id'] in consumer_ids for t in ('consumer_frontiers','consumer_frontier_revisions') for r in state[t])
    assert len(state['consumer_frontiers'])==len(state['consumer_frontier_revisions'])==8
    for selected_consumer in consumers:
        selected_id=selected_consumer['consumer_id']
        selected_role=selected_consumer['consumer_role']
        assert sorted(r['frontier_revision'] for r in state['consumer_frontiers'] if r['consumer_id']==selected_id)==[1,2,3,4]
        selected_revisions=[r for r in state['consumer_frontier_revisions'] if r['consumer_id']==selected_id]
        assert sorted(r['frontier_revision'] for r in selected_revisions)==[1,2,3,4]
        assert all(r['consumer_role']==selected_role for r in selected_revisions)
    revisions = [r for r in state['consumer_frontier_revisions'] if r['consumer_role']=='index']
    bounds = [r for r in state['consumer_frontiers'] if r['consumer_id']==consumer['consumer_id']]
    assert len(revisions)==len(bounds)==4
    binding = tuple(bytes.fromhex(identity[n]) for n in ('dataset','resource','generation','storage_incarnation','table_identity_digest'))+(custody,1)
    plans = sorted(state['projection_plans'],key=lambda r:r['frontier_revision'])
    for number,(plan,batch,counts) in enumerate(zip(plans,science['batches'],PLAN_COUNTS),1):
        first,last=RANGES[number-1]
        for key,value in dict(dataset_id=dataset,consumer_id=consumer['consumer_id'],generation_id=generation['generation_id'],
                frontier_revision=number,expected_revision=number-1,topology_generation=0,source_shard=0,source_owner_epoch=1,
                source_first=first,source_last=last,source_batch_digest='\\x'+batch['admission_digest'],
                origin_epoch=1,origin_incarnation=owner['process_incarnation'],sealed=True).items():
            index.exact(plan[key],value)
        members = [r for r in state['projection_plan_chunks'] if r['plan_digest']==plan['plan_digest']]
        assert len(members)==11 and {r['target'] for r in members}==set(index.TARGETS)
        chunks,selected=[],[]
        cut=[binding[0],0,[[0,1,last]]]
        for target,count in zip(index.TARGETS,counts):
            member=passive.only(members,target=target)
            selected_rows=[r for r in rows[target] if first<=r[1][3]<=last]
            assert len(selected_rows)==count
            digest=index.digest('projection-chunk',[binding[2],target,2,cut,0,[[r[1],[target,0],r] for r in selected_rows]])
            assert member['chunk_id']==member['rows_digest']=='\\x'+digest.hex()
            assert member['generation_id']==generation['generation_id'] and member['partition_key']=='all'
            chunk=passive.only(state['projection_chunks'],chunk_id=member['chunk_id'])
            for key,value in dict(generation_id=generation['generation_id'],dataset_id=dataset,storage_incarnation=generation['storage_incarnation'],
                    table_identity_digest=generation['table_identity_digest'],target=target,target_schema_version=2,topology_generation=0,
                    chunk_ordinal=number,row_count=count,owner_resource_id=owner['resource_id'],owner_epoch=1,owner_role='index',
                    owner_incarnation=owner['process_incarnation'],invalid_reason=None,state='committed').items():
                index.exact(chunk[key],value)
            bound=passive.only(state['projection_chunk_bounds'],chunk_id=member['chunk_id'])
            index.exact({k:v for k,v in bound.items() if k!='xmin'},dict(generation_id=generation['generation_id'],chunk_id=member['chunk_id'],
                dataset_id=dataset,topology_generation=0,shard_id=0,owner_epoch=1,last_complete_sequence=last))
            chunks.append((count,digest,digest));selected.append(chunk)
        digest=index.metadata_digest(binding,number-1,(0,[(0,1,first-1)]),(0,[(0,1,last)]),
            (bytes.fromhex(batch['admission_digest']),0,1,first,last),chunks)
        assert plan['plan_digest']=='\\x'+digest.hex()
        engine='\\x'+index.engine_digest(binding[4],digest,chunks).hex()
        published=passive.only(state['index_frontier_plans'],frontier_revision=number)
        for key,value in dict(plan_digest=plan['plan_digest'],engine_receipt_digest=engine,consumer_id=consumer['consumer_id'],
                generation_id=generation['generation_id'],dataset_id=dataset,topology_generation=0,owner_epoch=1,
                owner_incarnation=owner['process_incarnation']).items():
            index.exact(published[key],value)
        revision=passive.only(revisions,frontier_revision=number)
        bound=passive.only(bounds,frontier_revision=number)
        for key,value in dict(consumer_id=consumer['consumer_id'],dataset_id=dataset,topology_generation=0,owner_epoch=1,
                owner_resource_id=owner['resource_id'],owner_incarnation=owner['process_incarnation'],sealed=True,
                previous_revision=number-1 if number>1 else None).items():
            index.exact(revision[key],value)
        for key,value in dict(dataset_id=dataset,topology_generation=0,shard_id=0,owner_epoch=1,last_complete_sequence=last,
                attestation_kind='projection-chunk',attestation_digest=plan['plan_digest']).items():
            index.exact(bound[key],value)
        assert {r['engine_receipt_digest'] for r in selected}=={engine}
        assert {r['committed_at_us'] for r in selected}=={published['committed_at_us']}=={revision['created_at_us']}
        assert {r['xmin'] for r in [published,revision,bound,*selected]}=={published['xmin']}
        if number==4:assert consumer['xmin']==published['xmin']
    return generation
