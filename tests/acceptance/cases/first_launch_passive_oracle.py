"""Independent literal first passive attempt, retained request and publication checks."""
import hashlib
import re

PROFILE = [1,300000000,15000000,10000000,1,1,1,1,1,1,0,2097152,262144,64,32,3,
           4194304,65536,2000000,9,4096,64,0]
COUNTS = [9,0,0,0,0,1]
COUNT_COLUMNS = ('dictionary_count','payload_count','eligibility_count','receive_count','provenance_count','measurement_count')
EMPTY_TABLES = ('jobs','acquisition_jobs','acquisition_scopes','acquisition_progress','acquisition_quanta',
                'acquisition_gaps','measurement_dispatch_refusals')


def only(rows, **fields):
    result, = [row for row in rows if all(row[key] == value for key,value in fields.items())]
    return result


def integer(value, *, signed=False):
    assert type(value) is str and re.fullmatch('-?(0|[1-9][0-9]*)' if signed else '(0|[1-9][0-9]*)', value)
    result = int(value)
    assert str(result) == value and (-(2**63) <= result < 2**63 if signed else 0 <= result < 2**64)
    return result


def raw(value, size=None):
    assert type(value) is str and value.startswith('\\x')
    result = bytes.fromhex(value[2:])
    assert value == '\\x'+result.hex() and (size is None or len(result) == size)
    return result


def reference(value):
    assert set(value) == {'dataset_id','dictionary_kind','allocation_domain','ordinal'}
    assert re.fullmatch('[0-9a-f]{32}', value['dataset_id']) and int(value['dataset_id'],16)
    assert re.fullmatch('[0-9a-f]{32}', value['allocation_domain']) and int(value['allocation_domain'],16)
    ordinal = integer(value['ordinal']); assert ordinal > 0
    assert type(value['dictionary_kind']) is int and 1 <= value['dictionary_kind'] <= 9
    return [bytes.fromhex(value['dataset_id']),value['dictionary_kind'],bytes.fromhex(value['allocation_domain']),ordinal]


def references(state, initialized):
    dataset = initialized['private_scope']['dataset_id']
    domains = {row['dictionary_kind']:raw(row['allocation_domain'],16) for row in state['allocation_domains']}
    assert sorted(domains) == list(range(1,10)) and len(set(domains.values())) == 9
    assert all(row['dataset_id'] == '\\x'+dataset for row in state['allocation_domains'])
    def ref(kind, ordinal=1):
        return dict(dataset_id=dataset,dictionary_kind=kind,allocation_domain=domains[kind].hex(),ordinal=str(ordinal))
    expected = dict(endpoint_id=ref(1),observer_id=ref(2),route_id=ref(6),access_scope_id=ref(3),
        method_id=ref(5),clock_id=ref(2,2),visibility_scope_id=ref(4,3))
    assert expected['visibility_scope_id'] == initialized['private_scope']
    return expected


def context_bytes(refs):
    return b''.join(value[0]+value[1].to_bytes(2,'big')+value[2]+value[3].to_bytes(8,'big')
        for value in map(reference, refs.values()))


def source_selection(observations, state, initialized):
    assert len(observations) == 2 and [r['path'] for r in observations] == ['relay-endpoints','relay-enrollments']
    endpoint, enrollment = [row['body'] for row in observations]
    assert all(row['status'] == 202 and row['body']['created'] is True for row in observations)
    assert [row['key'] for row in observations] == ['91'*16,'92'*16]
    assert all(row['body']['command_key'] == row['key'] for row in observations)
    assert enrollment['enabled'] is True and enrollment['generation'] == '1'
    assert enrollment['endpoint_id'] == endpoint['endpoint_id']
    assert observations[0]['input'] == dict(url='ws://127.0.0.1:7447/installed-fixture',expected_config_revision='1',expected_policy_revision='1')
    assert observations[1]['input'] == dict(endpoint_id=endpoint['endpoint_id'],visibility=initialized['private_scope'],
        route={'class':'private','cidr':'127.0.0.1/32'},enabled=True,expected_generation=None,
        expected_config_revision='1',expected_policy_revision='1')
    selected = dict(format='bigbrotr-monitor-once-v1',endpoint_id=endpoint['endpoint_id'],
        access_scope_id=enrollment['access_scope_id'],enrollment_generation='1',route_id=enrollment['route_id'],visibility='private')
    for key in ('endpoint_id','access_scope_id','route_id'):
        assert re.fullmatch('[0-9a-f]{32}',selected[key]) and int(selected[key],16)
    current = only(state['relay_enrollments'])
    assert current['enabled'] is True and current['generation'] == 1
    assert all(current[key] == '\\x'+selected[key] for key in ('endpoint_id','access_scope_id','route_id'))
    assert all(not state[table] for table in EMPTY_TABLES) and not state['measurement_dispatches']
    assert len(state['producer_requests']) == len(state['producer_outcomes']) == 2
    return selected


def completed(report, state, initialized, selection):
    assert set(report) == {'format','state','result','profile','ready','attempt_id','source','context',
        'effective_authority_us','request_id','request_digest','measurement'}
    assert (report['format'],report['state'],report['result'],report['profile'],report['ready']) == (
        'bigbrotr-monitor-once/1','completed','admitted',1,False)
    assert re.fullmatch('[0-9a-f]{32}',report['attempt_id']) and int(report['attempt_id'],16)
    assert report['source'] == {key:selection[key] for key in ('endpoint_id','access_scope_id','enrollment_generation','route_id')}
    refs = references(state,initialized)
    assert report['context'] == refs
    assert report['request_id'] == dict(producer_slot_id=initialized['producer_slot_id'],producer_epoch='1',request_sequence='4')
    coordinate = report['measurement']
    assert set(coordinate) == {'dataset_id','shard_id','owner_epoch','sequence','payload_digest'}
    assert {k:v for k,v in coordinate.items() if k != 'payload_digest'} == dict(
        dataset_id=initialized['private_scope']['dataset_id'],shard_id=0,owner_epoch='1',sequence='10')
    assert re.fullmatch('[0-9a-f]{64}',coordinate['payload_digest'])
    assert all(not state[table] for table in EMPTY_TABLES)
    dispatch = only(state['measurement_dispatches'])
    assert dispatch['attempt_id'] == '\\x'+report['attempt_id'] and dispatch['profile_version'] == 1
    assert raw(dispatch['context_bytes'],294) == context_bytes(refs)
    assert 0 < dispatch['authority_ceiling_us'] == integer(report['effective_authority_us']) <= 5000000
    assert 0 < dispatch['dispatch_deadline_us']-dispatch['created_at_us'] <= dispatch['authority_ceiling_us']
    assert dispatch['config_revision'] == dispatch['policy_revision'] == 1
    assert all(dispatch[key] == '\\x'+selection[key] for key in ('endpoint_id','access_scope_id','route_id'))
    assert dispatch['enrollment_generation'] == 1
    owner = only(state['owner_epochs'],role='control',resource_id=dispatch['issuer_resource'],
        epoch=dispatch['issuer_epoch'],process_incarnation=dispatch['issuer_incarnation'])
    assert [dispatch['issuer_'+key] for key in ('resource','epoch','incarnation')] == [owner[key] for key in ('resource_id','epoch','process_incarnation')]
    assert dispatch['issuer_role'] == 'control' and dispatch['dataset_id'] == '\\x'+coordinate['dataset_id']
    requests = sorted(state['producer_requests'],key=lambda row:row['request_sequence'])
    outcomes = sorted(state['producer_outcomes'],key=lambda row:row['request_sequence'])
    assert [r['request_sequence'] for r in requests] == [1,2,3,4] == [r['request_sequence'] for r in outcomes]
    assert all(r['acquisition_job_id'] is None and r['acquisition_scope_id'] is None for r in requests)
    assert all(r['monitor_attempt_id'] is None and r['monitor_resolution'] is None for r in requests[:3])
    selected = requests[3]
    assert selected['dataset_id'] == outcomes[3]['dataset_id'] == dispatch['dataset_id']
    assert [selected['issuer_'+key] for key in ('resource','epoch','incarnation')] == [
        dispatch['issuer_'+key] for key in ('resource','epoch','incarnation')]
    assert [outcomes[3]['recorder_'+key] for key in ('resource','epoch','incarnation')] == [
        dispatch['issuer_'+key] for key in ('resource','epoch','incarnation')]
    assert selected['issuer_role'] == outcomes[3]['recorder_role'] == 'control'
    assert selected['monitor_attempt_id'] == dispatch['attempt_id'] and selected['monitor_resolution'] is False
    assert selected['relay_endpoint_id'] == dispatch['endpoint_id'] and selected['relay_access_scope_id'] == dispatch['access_scope_id']
    assert selected['relay_route_id'] == dispatch['route_id'] and selected['relay_enrollment_generation'] == 1
    assert raw(selected['request_digest'],32).hex() == report['request_digest']
    assert outcomes[3]['outcome_kind'] == 1 and outcomes[3]['policy_epoch'] == 1
    return refs


def moment(value, before, after):
    assert type(value) is dict and set(value) == {'observed_at','elapsed_us'}
    utc = integer(value['observed_at'],signed=True)
    elapsed = integer(value['elapsed_us'])
    assert before <= utc <= after
    return [utc,elapsed]


def scientific_payload(payload, attempt_id, authority, refs, wire, event, before, after):
    """Independent terminal A05 science, shared by typed inspection and retained input."""
    assert set(payload) == {'attempt_id','subject_kind','endpoint_id','observer_id','route_id','access_scope_id','method_id',
        'profile','clock_quality','state_kind','prior_unknown','observed','ended','termination','teardown'}
    assert payload['attempt_id'] == attempt_id and payload['subject_kind'] == 0
    assert all(payload[key] == refs[key] for key in ('endpoint_id','observer_id','route_id','access_scope_id','method_id'))
    assert payload['profile'] == PROFILE+[authority]
    assert all(type(value) is int for value in payload['profile'])
    assert payload['state_kind'] == 2 and payload['prior_unknown'] is None and payload['teardown'] == 1
    quality = payload['clock_quality']
    assert quality == dict(clock_id=refs['clock_id'],resolution_us=str(wire['clock_resolution_us']),uncertainty_us=None,anomaly_flags=0)
    observed = payload['observed']; assert set(observed) == {'execution','open','read','counters'}
    execution = observed['execution']
    assert set(execution) == {'destination','result','error','observed_at'}
    assert (execution['destination'],execution['result'],execution['error']) == (1,0,None)
    first = integer(execution['observed_at'],signed=True); assert before <= first <= after
    opened = observed['open']; assert set(opened) == {'tcp_connected','tls_completed','websocket_opened'}
    assert opened['tls_completed'] is None
    tcp = moment(opened['tcp_connected'],before,after); ws = moment(opened['websocket_opened'],before,after)
    read = observed['read']; assert set(read) == {'request_flushed','first_valid_event','eose','eose_prefix_verified','auth_observed','correlated_closed'}
    assert read['eose_prefix_verified'] is True and read['auth_observed'] is False and read['correlated_closed'] is False
    request = moment(read['request_flushed'],before,after)
    valid = read['first_valid_event']; assert set(valid) == {'event_id','received','verified'} and valid['event_id'] == event['id']
    delivered = moment(valid['received'],before,after); verified = moment(valid['verified'],before,after)
    eose = moment(read['eose'],before,after); ended = moment(payload['ended'],before,after)
    assert 0 <= tcp[1] <= ws[1] <= request[1] <= delivered[1] <= verified[1] <= ended[1]
    assert delivered[1] <= eose[1] <= ended[1]
    assert payload['termination'] == dict(kind=0,phase=None,failure=None,limit_code=None)
    assert observed['counters'] == {name:wire[name] for name in ('rx_physical_frames','rx_messages','event_candidates','rx_ws_bytes','rx_payload_bytes','tx_ws_bytes')}
    assert all(type(value) is int and 0 <= value < 2**64 for value in observed['counters'].values())
    assert observed['counters']['rx_physical_frames'] == observed['counters']['rx_messages'] == 3
    assert observed['counters']['event_candidates'] == 1
    context = [[0,reference(refs['endpoint_id'])]]+[reference(refs[key]) for key in ('observer_id','route_id','access_scope_id','method_id')]
    captured = [[1,[0],first],[tcp,None,ws],[request,[bytes.fromhex(event['id']),delivered,verified],eose,True,False,False],
        [observed['counters'][name] for name in ('rx_physical_frames','rx_messages','event_candidates','rx_ws_bytes','rx_payload_bytes','tx_ws_bytes')]]
    value = [bytes.fromhex(attempt_id),context,PROFILE+[authority],
        [reference(refs['clock_id']),wire['clock_resolution_us'],None,0],[2,[captured,ended,[0],1]]]
    return value


def scientific(document, report, refs, wire, event, before, after, cbor):
    assert set(document) == {'format','state','audience','catalog_id','frontier_revision','topology_generation','policy_revision','cut','fact'}
    assert document['format'] == 'bigbrotr-archive-measurement/1' and document['state'] == 'present'
    assert document['audience'] == 'trusted-local-operator' and document['policy_revision'] == '1'
    fact = document['fact']
    assert set(fact) == {'source_record_id','record_schema_version','record_kind','visibility_scope_id','payload_digest','recorded_at','payload'}
    assert fact['record_kind'] == 'measurement' and type(fact['record_schema_version']) is int and fact['record_schema_version'] == 1
    coordinate = report['measurement']
    assert fact['source_record_id'] == {k:v for k,v in coordinate.items() if k != 'payload_digest'}
    assert fact['payload_digest'] == coordinate['payload_digest'] and fact['visibility_scope_id'] == refs['visibility_scope_id']
    at = integer(fact['recorded_at'],signed=True); assert before <= at <= after
    value = scientific_payload(fact['payload'],report['attempt_id'],integer(report['effective_authority_us']),
        refs,wire,event,before,after)
    source = [bytes.fromhex(coordinate['dataset_id']),0,1,10]
    encoded = b'bigbrotr/fact/1\0'+cbor([source,'measurement',1,reference(refs['visibility_scope_id']),at,value])
    assert hashlib.sha256(encoded).hexdigest() == fact['payload_digest'], 'independently recomputed original A05 digest'
    return value, encoded


def dictionary_request(refs, slot, cbor):
    method = reference(refs['method_id']); visibility = reference(refs['visibility_scope_id'])
    observer = dict(clock=None,vantage=None,software=None,method_id=method,public_key=None,
        route_class='private',software_version=None)
    clock = dict(observer,clock='system-utc',route_class=None)
    definitions = [
        ('method_id','passive-ws-read',dict(input='nip01-passive-read/1',algorithm='guarded-ws-ordered-read',
            capability='scoped-passive-read-observation',output_unit='compound-attempt',
            failure_taxonomy='bigbrotr-passive-ws-read/1',algorithm_version=1,
            protocol_revision='a2494f4f81d46684e5814a9bf35e2b1df978f955')),
        ('endpoint_id','endpoint',dict(url='ws://127.0.0.1:7447/installed-fixture')),
        ('route_id','direct-route',{'class':'private','label':'direct-private'}),
        ('access_scope_id','access-scope',{'class':'public','label':None,'visibility_scope_id':visibility}),
        ('observer_id','observer',observer),('clock_id','observer',clock)]
    entries = [['dictionary',1,[reference(refs[key]),1,schema,1,fields]] for key,schema,fields in definitions]
    return b'bigbrotr/producer-request/1\0'+cbor([[bytes.fromhex(slot),1,3],'admit-evidence',1,[1,visibility,entries]])


def retained_inputs(state, initialized, refs, measurement, originals, cbor):
    """Four original reservations exist even when the terminal SQL copy does not."""
    assert len(originals) == 4
    slot = bytes.fromhex(initialized['producer_slot_id'])
    visibility = reference(refs['visibility_scope_id'])
    assert originals[2] == dictionary_request(refs,initialized['producer_slot_id'],cbor)
    expected = b'bigbrotr/producer-request/1\0'+cbor([[slot,1,4],'admit-evidence',1,[1,visibility,'measurement/1',measurement]])
    assert originals[3] == expected, 'original typed measurement input differs'
    requests = sorted(state['producer_requests'],key=lambda r:r['request_sequence'])
    assert [row['request_sequence'] for row in requests] == [1,2,3,4]
    assert all(row['manifest_sha256'] == requests[0]['manifest_sha256'] for row in requests)
    for request, original in zip(requests,originals):
        assert request['dataset_id'] == '\\x'+refs['endpoint_id']['dataset_id']
        assert all(type(request[field]) is int for field in ('request_sequence','producer_epoch','input_bytes','input_profile'))
        assert raw(request['request_digest'],32) == hashlib.sha256(original).digest()
        assert request['input_bytes'] == len(original) and request['input_profile'] == 1
        assert request['producer_slot_id'] == '\\x'+slot.hex() and request['producer_epoch'] == 1
    return requests


def _retained_receipts(state, initialized, refs, measurement, originals, cbor, decode, *, count):
    assert type(count) is int and count in (3,4)
    requests = retained_inputs(state,initialized,refs,measurement,originals,cbor)
    slot = bytes.fromhex(initialized['producer_slot_id']); dataset = bytes.fromhex(refs['endpoint_id']['dataset_id'])
    outcomes = sorted(state['producer_outcomes'],key=lambda r:r['request_sequence'])
    assert [row['request_sequence'] for row in outcomes] == list(range(1,count+1))
    references_by_source, receipts = {}, []
    ranges = [(1,2),(3,3),(4,9),(10,10)]
    for index,(request,outcome,original,bounds) in enumerate(zip(
            requests,outcomes,originals,ranges),1):
        digest = hashlib.sha256(original).digest()
        assert outcome['dataset_id'] == request['dataset_id']
        assert raw(request['request_digest'],32) == raw(outcome['request_digest'],32) == digest
        assert request['input_bytes'] == len(original) and request['input_profile'] == 1
        assert request['producer_slot_id'] == outcome['producer_slot_id'] == '\\x'+slot.hex()
        assert request['producer_epoch'] == outcome['producer_epoch'] == 1
        assert request['manifest_sha256'] == outcome['manifest_sha256']
        assert outcome['outcome_kind'] == 1 and outcome['policy_epoch'] == 1
        encoded = raw(outcome['outcome_bytes']); assert encoded.startswith(b'bigbrotr/receipt/1\0')
        assert hashlib.sha256(encoded).digest() == raw(outcome['outcome_sha256'],32)
        receipt = decode(encoded.split(b'\0',1)[1])
        assert len(receipt) == 6 and receipt[:4] == [1,[slot,1,index],digest,
            [dataset,0,1,bytes.fromhex(initialized['recovery_generation']),1]]
        assert len(receipt[4]) == bounds[1]-bounds[0]+1
        for number,fact in zip(range(bounds[0],bounds[1]+1),receipt[4]):
            assert len(fact) == 2 and fact[0] == [dataset,0,1,number] and len(fact[1]) == 32
            references_by_source[number] = fact
        dependencies = list(range(bounds[0],bounds[1]+1)) if index <= 2 else list(range(3,bounds[1]+1))
        assert receipt[5] == [references_by_source[n] for n in dependencies]
        receipts.append(receipt)
    return receipts


def retained_before_terminal(state, initialized, refs, measurement, originals, cbor, decode):
    """PG has exactly outcomes1..3; never invent a fourth row for the old oracle."""
    return _retained_receipts(state,initialized,refs,measurement,originals,cbor,decode,count=3)


def retained(state, initialized, report, refs, measurement, originals, cbor, decode):
    receipts = _retained_receipts(state,initialized,refs,measurement,originals,cbor,decode,count=4)
    references_by_source = {source[3]:[source,digest] for receipt in receipts for source,digest in receipt[4]}
    assert references_by_source[10][1].hex() == report['measurement']['payload_digest']
    return receipts


def publication(state, initialized, catalog_id, receipts, cbor, cut_digest):
    consumer = only(state['consumers'],consumer_role='archive')
    assert consumer['consumer_id'] == '\\x'+catalog_id
    assert consumer['current_revision'] == 4 and consumer['current_topology_generation'] == 0
    frontier = only(state['archive_frontier_snapshots'],frontier_revision=4)
    assert frontier['consumer_id'] == frontier['catalog_id'] == '\\x'+catalog_id
    assert frontier['dataset_id'] == '\\x'+initialized['private_scope']['dataset_id']
    assert frontier['snapshot_id'] == 5 and frontier['topology_generation'] == 0
    assert [frontier[name] for name in COUNT_COLUMNS] == COUNTS
    bound = only(state['consumer_frontiers'],consumer_id=consumer['consumer_id'],frontier_revision=4)
    assert (bound['shard_id'],bound['owner_epoch'],bound['last_complete_sequence']) == (0,1,10)
    attempts = sorted(state['archive_attempts'],key=lambda row:row['ordinal'])
    assert [row['ordinal'] for row in attempts] == [1,2,3,4]
    expected = []
    dataset = bytes.fromhex(initialized['private_scope']['dataset_id'])
    for ordinal,(attempt,receipt,(first,last)) in enumerate(zip(attempts,receipts,[(1,2),(3,3),(4,9),(10,10)]),1):
        archive = [dataset,1,0,[[0,1,first,last]],receipt[4]]
        digest = hashlib.sha256(b'bigbrotr/archive-batch/1\0'+cbor(archive)).hexdigest()
        assert attempt['batch_digest'] == '\\x'+digest and attempt['schema_version'] == 1
        assert attempt['expected_predecessor'] == ordinal-1 and attempt['topology_generation'] == 0
        assert attempt['dataset_id'] == '\\x'+dataset.hex() and attempt['catalog_id'] == '\\x'+catalog_id
        expected.append(dict(dataset_id=dataset.hex(),schema_version=1,topology_generation=0,batch_digest=digest,
            record_count=last-first+1,input_ranges=[dict(shard_id=0,owner_epoch=1,first_sequence=first,last_sequence=last)],
            attempt_id=raw(attempt['attempt_id'],16).hex(),publication_ordinal=ordinal))
    assert {row['attempt_id'] for row in state['archive_frontier_batches'] if row['frontier_revision'] == 4} == {
        '\\x'+row['attempt_id'] for row in expected}
    digest = cut_digest('\\x'+dataset.hex(),0,expected,[(1,10)],5)
    assert frontier['request_digest'] == '\\x'+digest
    return dict(format='bigbrotr-native-cut/2',snapshot_id=5,request_digest=digest,family_counts=COUNTS)


def inspection_pin(before, after, catalog_id):
    for name in ('snapshot_leases','archive_snapshot_pins','retention_claims'):
        assert not before[name] and len(after[name]) == 1
    pin, lease, claim = (after[name][0] for name in ('archive_snapshot_pins','snapshot_leases','retention_claims'))
    owner = only(after['owner_epochs'],role='control',resource_id=pin['issuer_resource'],epoch=pin['issuer_epoch'])
    old = only(before['owner_epochs'],role='control',resource_id=owner['resource_id'],epoch=owner['epoch']-1)
    assert [row for row in after['owner_epochs'] if row not in before['owner_epochs']] == [owner]
    assert all(row in after['owner_epochs'] for row in before['owner_epochs'])
    assert owner['resource_id'] == old['resource_id'] and owner['state'] == old['state'] == 'released'
    assert lease['lease_id'] == pin['lease_id'] == claim['lease_id']
    assert pin['consumer_id'] == lease['consumer_id'] == '\\x'+catalog_id
    assert pin['frontier_revision'] == lease['frontier_revision'] == 4
    assert pin['purpose'] == claim['purpose'] == 'reader' and lease['archive_pin_protocol'] == 1
    assert (pin['issuer_resource'],pin['issuer_epoch'],pin['issuer_incarnation']) == (
        owner['resource_id'],owner['epoch'],owner['process_incarnation'])
    assert lease['holder_kind'] == 'role_instance' and lease['holder_job_id'] is None
    assert lease['holder_role_instance_id'] == owner['role_instance_id'] and lease['holder_role'] == 'control'
    assert lease['holder_incarnation'] == owner['process_incarnation']
    assert lease['state'] == claim['state'] == 'released' and lease['lease_revision'] == 2
    assert lease['release_reason'] == claim['release_reason'] == 'reader_joined'
    assert owner['acquired_at_us'] <= lease['acquired_at_us'] <= claim['released_at_us'] <= lease['released_at_us'] <= owner['released_at_us']
    assert lease['updated_at_us'] == lease['released_at_us'] and lease['xmin'] == claim['xmin']
    assert (claim['dataset_id'],claim['topology_generation'],claim['shard_id'],claim['owner_epoch'],claim['first_sequence'],claim['last_sequence']) == (
        owner['dataset_id'],0,0,1,1,10)


def _checkpoint_values(state, initialized, receipts, *, through):
    """The two explicit initial prefixes share bytes, never observed head selection."""
    assert type(through) is int and through in (3,4)
    dataset = bytes.fromhex(initialized['private_scope']['dataset_id'])
    slot = bytes.fromhex(initialized['producer_slot_id'])
    count = 9 if through == 3 else 10
    assert len(receipts) == through and all(not state[name] for name in EMPTY_TABLES)
    expected = [reference for receipt in receipts for reference in receipt[4]]
    assert [r[0] for r in expected] == [[dataset,0,1,n] for n in range(1,count+1)]
    head = only(state['producer_checkpoints'])
    assert all(type(head[field]) is int for field in ('checkpoint_revision','complete_through_request',
        'progress_format','progress_jobs','dependency_count'))
    assert (head['checkpoint_revision'],head['complete_through_request'],head['progress_format'],
        head['progress_jobs'],head['dependency_count']) == (through,through,1,0,count)
    assert head['dataset_id'] == '\\x'+dataset.hex() and head['producer_slot_id'] == '\\x'+slot.hex()
    assert head['producer_epoch'] == 1
    manifest = raw(state['producer_requests'][0]['manifest_sha256'],32)
    assert raw(head['manifest_sha256'],32) == manifest
    rows = sorted(state['producer_checkpoint_dependencies'],key=lambda r:r['source_sequence'])
    assert len(rows) == count
    for row, (source, digest) in zip(rows,expected):
        assert (row['dataset_id'],row['producer_slot_id'],row['producer_epoch']) == ('\\x'+dataset.hex(),'\\x'+slot.hex(),1)
        assert (row['source_shard'],row['source_epoch'],row['source_sequence']) == tuple(source[1:])
        assert raw(row['payload_digest'],32) == digest
    # Version1 producer-progress bytes: fixed identity/head, zero jobs, sorted
    # full references. No product reducer, supplied digest or observed C chooses it.
    body = b'bigbrotr/producer-progress/1\0'+dataset+slot+(1).to_bytes(8,'big')+manifest
    body += b''.join(n.to_bytes(8,'big') for n in (through,through,0,count))
    for source, digest in expected:
        body += source[0]+b''.join(n.to_bytes(8,'big') for n in source[1:])+digest
    assert raw(head['checkpoint_digest'],32) == hashlib.sha256(body).digest()
    verdict = dict(generic_checkpoint=through,checkpoint_revision=through,checkpoint_dependencies=count,
        checkpoint_sha256=hashlib.sha256(body).hexdigest(),acquisition_progress=0)
    return verdict,head,rows[-1]


def _checkpoint(state, initialized, receipts, *, through):
    verdict,head,last = _checkpoint_values(state,initialized,receipts,through=through)
    outcome = only(state['producer_outcomes'],request_sequence=through)
    assert head['xmin'] == outcome['xmin'] == last['xmin'], 'outcome and checkpoint must commit together'
    for field in ('resource','epoch','incarnation'):
        assert head['issuer_'+field] == outcome['recorder_'+field]
    return verdict


def checkpoint_existing_outcome(state, initialized, receipts):
    """Seeded outcome4 predates the atomic head/dependency10 repair transaction."""
    verdict,head,last = _checkpoint_values(state,initialized,receipts,through=4)
    outcome = only(state['producer_outcomes'],request_sequence=4)
    assert head['xmin'] == last['xmin'] and head['xmin'] != outcome['xmin']
    return verdict


def checkpoint(state, initialized, receipts):
    """Literal generic C4 and exact ten-reference closure, with no Sync heads."""
    return _checkpoint(state,initialized,receipts,through=4)


def checkpoint_before_terminal(state, initialized, receipts):
    """Literal generic C3 and nine dependencies, atomically bound to outcome3."""
    return _checkpoint(state,initialized,receipts,through=3)


def native_rows(dump):
    assert set(dump) == {'event','rows','sequence'} and dump['event'] == 'dump-state'
    assert type(dump['sequence']) is int and dump['sequence'] > 0
    values = {}
    for row in dump['rows']:
        assert set(row) == {'cf','key','value'} and row['cf'] in ('default','facts','receipts')
        assert bytes.fromhex(row['key']).hex() == row['key'] and bytes.fromhex(row['value']).hex() == row['value']
        key = (row['cf'],bytes.fromhex(row['key']))
        assert key not in values
        values[key] = bytes.fromhex(row['value'])
    return values


def native_physical(dump, initialized, receipts, cbor, manifest_digest, *, phase):
    """Physical I4/facts/receipts; each caller separately proves its exact PG phase."""
    values = native_rows(dump)
    assert len(receipts) == 4
    dataset = bytes.fromhex(initialized['private_scope']['dataset_id'])
    assert [reference[0] for receipt in receipts for reference in receipt[4]] == [[dataset,0,1,n] for n in range(1,11)]
    slot = bytes.fromhex(initialized['producer_slot_id'])
    be = lambda n:n.to_bytes(8,'big')
    assert phase in ('admitted','reply-lost','closing-unacknowledged','recovered')
    acknowledged = 3 if phase in ('reply-lost','closing-unacknowledged') else 4
    closing = phase in ('closing-unacknowledged','recovered')
    head = bytes([2 if closing else 1,0x31])+be(4)+be(acknowledged)+be(0)
    assert values[('receipts',b'\x01\x31'+slot+be(1))] == head+(b'\x01' if closing else b'')
    assert len([key for cf,key in values if cf=='receipts']) == 5
    assert values[('default',b'\x01\x02'+dataset+bytes(4)+be(1))] == b'\x01\x02'+be(10)
    manifest = values[('default',b'\x01\x01')]
    assert hashlib.sha256(manifest).digest() == manifest_digest
    source_key = lambda n:dataset+bytes(4)+be(1)+be(n)
    fact_keys = {b'\x01\x20'+source_key(n) for n in range(1,11)}
    ranges = [(1,2),(3,3),(4,9),(10,10)]
    pending_keys = {b'\x01\x21'+source_key(first) for first,_ in ranges}
    # Both tagged domains live in Facts CF. Require the closed literal key set,
    # including complete pending metadata, rather than counting observed rows.
    assert {key for cf,key in values if cf=='facts'} == fact_keys | pending_keys
    for number,(receipt,(first,last)) in enumerate(zip(receipts,ranges),1):
        assert receipt[0] == 1 and receipt[1] == [slot,1,number]
        original = b'bigbrotr/receipt/1\0'+cbor(receipt)
        assert values[('receipts',b'\x01\x30'+slot+be(1)+be(number))] == original
        assert [source for source,_ in receipt[4]] == [[dataset,0,1,n] for n in range(first,last+1)]
        digest = hashlib.sha256(b'bigbrotr/admission-batch/1\0'+cbor([dataset,0,1,first,last,receipt[4]])).digest()
        assert values[('facts',b'\x01\x21'+source_key(first))] == b'\x01\x21'+be(last)+digest
        for source,digest in receipt[4]:
            assert source[:3] == [dataset,0,1]
            fact = values[('facts',b'\x01\x20'+dataset+bytes(4)+be(1)+be(source[3]))]
            assert fact[:34] == b'\x01\x20'+digest
            assert hashlib.sha256(fact[34:]).digest() == digest
    return dict(native_acknowledged=acknowledged,native_issued=4,native_retired=0,native_facts=10,native_pending_batches=4,native_receipts=4)


def native_ack(dump, state, initialized, receipts, cbor, *, phase='admitted'):
    """Old admitted/C4 cases still require every exact PG outcome before physics."""
    outcomes = sorted(state['producer_outcomes'],key=lambda r:r['request_sequence'])
    assert [row['request_sequence'] for row in outcomes] == [1,2,3,4] and len(receipts) == 4
    for outcome,receipt in zip(outcomes,receipts):
        assert raw(outcome['outcome_bytes']) == b'bigbrotr/receipt/1\0'+cbor(receipt)
    return native_physical(dump,initialized,receipts,cbor,
        raw(state['producer_requests'][0]['manifest_sha256'],32),phase=phase)
