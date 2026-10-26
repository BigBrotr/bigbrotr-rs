"""Finite epoch1→2 expectations for the maintained installed restart case.

The existing literal encoders own scientific bytes. Only identities and bounded
clock scalars parameterize them; neither Archive payloads nor SQL digests supply
expected facts. This module opens no engine and does not claim a native census.
"""
import hashlib
import json
from types import SimpleNamespace


ENDPOINT = 'ws://127.0.0.1:7447/installed-live-fixture'
SELECTOR_SHA256 = '64f3d2ad64cf6960aaa9908e8b85b21205ca89786cb2c21dacb3c4e8e4d1938f'
COUNTS = ('dictionary_count', 'payload_count', 'eligibility_count', 'receive_count', 'provenance_count')
PROGRESS_FIELDS = ('progress_request_id', 'progress_event_ordinal', 'progress_wire_sequence', 'progress_input_sha256')


def sha(value):
    return hashlib.sha256(value).hexdigest()


def exact(actual, expected, reason):
    # JSON integer1, booleantrue and float1.0 must not impersonate one another.
    assert json.dumps(actual, sort_keys=True, allow_nan=False) == json.dumps(expected, sort_keys=True, allow_nan=False), reason


def only(rows, **fields):
    row, = [row for row in rows if all(row[key] == value for key, value in fields.items())]
    return row


def raw(value):
    assert isinstance(value, str) and value.startswith('\\x')
    return bytes.fromhex(value[2:])


def be(value, width=8):
    assert type(value) is int and 0 <= value < 1 << (8 * width)
    return value.to_bytes(width, 'big')


def clock(value, lower, upper):
    assert type(value) is int and lower <= value <= upper, 'bounded clock scalar'
    return value


def originals(store, slot, epoch, count):
    paths = sorted((store / 'producer-inputs-v1').glob(f'{slot}-{epoch:016x}-*.cbor'))
    assert [path.name for path in paths] == [f'{slot}-{epoch:016x}-{n:016x}.cbor' for n in range(1, count + 1)]
    assert all(path.is_file() and not path.is_symlink() and 0 < path.stat().st_size <= 65536 for path in paths)
    return [path.read_bytes() for path in paths]


def row_at(census, table, epoch, sequence):
    assert census['format'] == 'bigbrotr-independent-installed-archive-read/1'
    return only(census['tables'][table], source_record_id=dict(
        dataset_id=census['tables']['events'][0]['source_record_id']['dataset_id'],
        shard_id=0, owner_epoch=epoch, sequence=sequence))


def request_binding(event, wire, ordinal):
    return dict(progress_request_id='\\x' + wire['request'][1].encode().hex(),
        progress_event_ordinal=ordinal, progress_wire_sequence=ordinal,
        progress_input_sha256='\\x' + sha(json.dumps(event, ensure_ascii=False, separators=(',', ':')).encode()))


def requests(state, epoch, manifest, inputs, receipts, owner, bindings, observations):
    """Exact current request/outcome columns, including typed wire attribution."""
    producer = only(state['producer_epochs'], producer_epoch=epoch)
    assert producer['outstanding_request_limit'] == 32
    assert producer['reserved_through_request'] == len(inputs) == len(receipts)
    common = dict(dataset_id=producer['dataset_id'], producer_slot_id=producer['producer_slot_id'],
        producer_epoch=epoch, manifest_sha256='\\x' + sha(manifest))
    actual_requests = sorted([r for r in state['producer_requests'] if r['producer_epoch'] == epoch], key=lambda r: r['request_sequence'])
    outcomes = sorted([r for r in state['producer_outcomes'] if r['producer_epoch'] == epoch], key=lambda r: r['request_sequence'])
    assert len(actual_requests) == len(outcomes) == len(inputs)
    columns = ('relay_endpoint_id', 'relay_access_scope_id', 'relay_enrollment_generation',
        'relay_route_id', 'acquisition_scope_id', 'acquisition_job_id')
    for n, (row, outcome, original, receipt) in enumerate(zip(actual_requests, outcomes, inputs, receipts), 1):
        binding = bindings.get(n)
        attribution = observations if binding else dict.fromkeys(columns)
        expected = dict(common, request_sequence=n, request_digest='\\x' + sha(original),
            input_bytes=len(original), input_profile=1, issuer_role='control', issuer_resource=owner['resource_id'],
            issuer_epoch=owner['epoch'], issuer_incarnation=owner['process_incarnation'], **attribution,
            **(binding if binding else dict.fromkeys(PROGRESS_FIELDS)))
        for key in ('progress_event_ordinal', 'progress_wire_sequence'):
            assert row[key] is None or type(row[key]) is int, 'exact progress integer'
        exact({k: v for k, v in row.items() if k not in ('xmin', 'reserved_at_us')}, expected, 'exact producer request')
        expected_outcome = dict(common, request_sequence=n, request_digest=expected['request_digest'],
            outcome_kind=1, outcome_bytes='\\x' + receipt.hex(), outcome_sha256='\\x' + sha(receipt), policy_epoch=1,
            recorder_role='control', recorder_resource=owner['resource_id'], recorder_epoch=owner['epoch'],
            recorder_incarnation=owner['process_incarnation'])
        exact({k: v for k, v in outcome.items() if k not in ('xmin', 'recorded_at_us')}, expected_outcome, 'exact producer outcome')
        assert owner['acquired_at_us'] <= row['reserved_at_us'] <= outcome['recorded_at_us'] < owner['lease_expires_at_us']


def attribution(state, job_key):
    job = only(state['jobs'], request_key='\\x' + job_key * 16)
    detail = only(state['acquisition_jobs'], job_id=job['job_id'])
    scope = only(state['acquisition_scopes'], scope_id=detail['scope_id'])
    enrollment = only(state['relay_enrollments'], endpoint_id=scope['endpoint_id'], access_scope_id=scope['access_scope_id'])
    return job, detail, dict(relay_endpoint_id=scope['endpoint_id'], relay_access_scope_id=scope['access_scope_id'],
        relay_enrollment_generation=1, relay_route_id=enrollment['route_id'], acquisition_scope_id=scope['scope_id'],
        acquisition_job_id=job['job_id'])


def prepared(state, commands, visibility):
    assert len(commands) == 3
    endpoint_command, enrollment_command, scope_command = commands
    for row, path, key in zip(commands, ('relay-endpoints', 'relay-enrollments', 'acquisition-scopes'), ('f1', 'f2', 'f3')):
        assert row['path'] == path and row['key'] == key * 16 and row['status'] == 202
        assert row['body']['created'] is True and row['body']['command_key'] == key * 16
        assert row['input']['expected_config_revision'] == row['input']['expected_policy_revision'] == '1'
    endpoint = only(state['relay_endpoints'], endpoint_id='\\x' + endpoint_command['body']['endpoint_id'])
    assert raw(endpoint['url_bytes']).decode() == endpoint_command['input']['url'] == ENDPOINT
    access = only(state['relay_access_contexts'], access_scope_id='\\x' + enrollment_command['body']['access_scope_id'])
    enrollment = only(state['relay_enrollments'], endpoint_id=endpoint['endpoint_id'], access_scope_id=access['access_scope_id'])
    route = only(state['network_routes'], route_id=enrollment['route_id'])
    assert route['route_version'] == 1 and route['route_bytes'] == '\\x0104207f000001'
    assert enrollment['generation'] == 1 and enrollment['enabled'] is True
    assert enrollment_command['input']['visibility'] == scope_command['input']['visibility'] == visibility
    scope = only(state['acquisition_scopes'], scope_id='\\x' + scope_command['body']['scope_id'])
    assert scope['endpoint_id'] == endpoint['endpoint_id'] and scope['access_scope_id'] == access['access_scope_id']
    assert scope['visibility_kind'] == 4 and scope['visibility_ordinal'] == 3
    assert scope['visibility_domain'] == '\\x' + visibility['allocation_domain']
    assert scope['dataset_id'] == '\\x' + visibility['dataset_id']
    selector = only(state['acquisition_selectors'], selector_id=scope['selector_id'])
    document = b'{"version":1,"filters":[{"kinds":[1]}],"history_since":0,"ephemeral":"retain"}'
    assert selector['document'] == '\\x' + document.hex() and selector['candidate_digest'] == '\\x' + sha(document)


def first_science(state, report, event, wires, census, store, completed):
    from installed_archive_oracle import expected
    from installed_progress_context_oracle import archive_request_progress, verify_archive_progress
    from installed_sync_oracle import decode
    job, _, source = attribution(state, 'b1')
    data = originals(store, report['producer_slot_id'], 1, 4)
    dictionary = clock(row_at(census, 'dictionaries', 1, 4)['recorded_at'], job['created_at_us'], wires[0]['connected_at_us'])
    received = decode(data[3].split(b'\0', 1)[1])[3][5][0]
    clock(received, wires[0]['sent_at_us'], wires[0]['closed_at_us'])
    recorded = clock(row_at(census, 'events', 1, 9)['recorded_at'], received, completed)
    science = expected(state, report, event, dictionary, received, recorded, wires[0]['clock_resolution_us'],
        native_options=dict(acknowledge=True))
    assert data == science['inputs'], 'initial original input bytes'
    for table, rows in science['tables'].items():
        exact(census['tables'][table], rows, 'initial independent scientific rows: ' + table)
    manifest = next(value for cf, key, value in science['native'] if cf == 'default' and key == b'\x01\x01')
    owner = only(state['owner_epochs'], role='control')
    requests(state, 1, manifest, science['inputs'], science['receipts'], owner,
        {4: archive_request_progress(event, wires)}, source)
    verify_archive_progress(state, wires, completed, event, science['native'], science['inputs'], science['receipts'], owner)
    science['manifest'] = manifest
    for batch in science['batches']:
        batch.update(epoch=1, topology=0)
    return science


def successor_wire(wires, old, new):
    import re
    assert 2 <= len(wires) <= 128
    nonces = set()
    previous = None
    for number, wire in enumerate(wires, 1):
        assert wire['number'] == number
        command = wire['request']
        assert len(command) == 3 and command[0] == 'REQ' and command[2] == dict(kinds=[1], limit=3)
        assert re.fullmatch('[0-9a-f]{48}', command[1]) and int(command[1][:32], 16)
        assert command[1][:32] not in nonces
        nonces.add(command[1][:32])
        assert wire['connected_at_us'] <= wire['request_at_us'] <= wire['closed_at_us']
        assert previous is None or previous <= wire['connected_at_us']
        assert 0 < wire['closed_at_us'] - wire['request_at_us'] < 3_000_000
        previous = wire['closed_at_us']
        assert type(wire['clock_resolution_us']) is int and 0 < wire['clock_resolution_us'] <= 1_000_000
        if number == 1:
            assert wire['frames'] == ['distinct', 'predecessor', 'distinct-resend', 'EOSE']
            assert wire['event_ids'] == [new['id'], old['id'], new['id']]
            assert wire['completion'] == 'request-and-websocket-close'
            assert wire['request_at_us'] <= wire['sent_at_us'] <= wire['closed_at_us']
        else:
            assert wire['frames'] == [] and wire['completion'] == 'quiet-eof'
    return wires[0]


def successor_science(before, after, report, first, old_event, new_event, wires, census, store, completed):
    from initial_succession_oracle import successor_manifest
    from installed_family_science import NetworkScience, decode
    from installed_archive_catch_up_model import archive_science
    from transition_replay import native_rows
    from live_replay import OLD_EVENT
    assert old_event == OLD_EVENT
    wire = successor_wire(wires, old_event, new_event)
    for table in ('producer_requests', 'producer_outcomes', 'producer_checkpoint_dependencies', 'acquisition_progress'):
        assert all(row in after[table] for row in before[table]), 'historical producer evidence changed: ' + table
    assert only(after['datasets'])['config_revision'] == only(after['datasets'])['policy_revision'] == 1
    assert only(after['datasets'])['topology_generation'] == 1
    intent = only(after['initial_ingest_successions'])
    domains = [only(after['allocation_domains'], owner_epoch=2, dictionary_kind=k)['allocation_domain'] for k in range(1, 10)]
    fresh_successor(before, intent, domains)
    manifest = successor_manifest(first['manifest'], raw(intent['target_incarnation']), [raw(d) for d in domains])
    assert raw(intent['previous_manifest']) == first['manifest'] and raw(intent['target_manifest']) == manifest
    assert intent['target_manifest_sha256'] == '\\x' + sha(manifest)
    source = only(before['source_streams'], owner_epoch=1)
    handoff = (b'\x01\x0a' + raw(intent['producer_slot_id']) + be(1) + be(4) + be(12)
        + raw(source['closure_digest']) + be(len(first['manifest']), 2) + first['manifest'] + be(len(manifest), 2) + manifest)
    assert raw(intent['handoff']) == handoff and intent['handoff_sha256'] == '\\x' + sha(handoff)
    ingest = only(after['owner_epochs'], role='ingest', epoch=2)
    assert ingest['process_incarnation'] == intent['target_incarnation']
    assert ingest['role_instance_id'] == intent['target_role_instance_id']
    assert report['process_incarnation'] == intent['target_incarnation'][2:]
    assert report['producer_slot_id'] == intent['producer_slot_id'][2:]
    model = SimpleNamespace(ds=only(after['datasets']), slot=intent['producer_slot_id'], legacy=intent,
        inputs={}, native=dict(event='dump-state', sequence=0, rows=[dict(cf=cf, key=key.hex(), value=value.hex()) for cf, key, value in first['native']]))
    plan = dict(epoch=2, topology=1, manifest=manifest, domains=domains)
    science = NetworkScience(model, plan, acknowledge=True, new_event=new_event, endpoint=ENDPOINT)
    data = originals(store, report['producer_slot_id'], 2, 3)
    job, detail, source = attribution(after, 'f4')
    dictionary = clock(row_at(census, 'dictionaries', 2, 1)['recorded_at'], job['created_at_us'], wire['connected_at_us'])
    received = decode(data[1].split(b'\0', 1)[1])[3][5][0]
    old_received = decode(data[2].split(b'\0', 1)[1])[3][5][0]
    resolution = wire['clock_resolution_us']
    clock(received, wire['sent_at_us'] - resolution, wire['closed_at_us'] + resolution)
    clock(old_received, received, wire['closed_at_us'] + resolution)
    recorded = clock(row_at(census, 'events', 2, 6)['recorded_at'], received, completed)
    old_recorded = clock(row_at(census, 'provenance_facts', 2, 10)['recorded_at'], old_received, completed)
    science.network(True, dictionary, received, recorded, resolution, (old_received, old_recorded))
    assert data == list(science.inputs.values()), 'successor original input bytes'
    wanted = archive_science(dict(rows=native_rows(science.values)), live_epoch=2)
    for table, rows in wanted['tables'].items():
        exact(census['tables'][table], rows, 'complete independent scientific rows: ' + table)
        assert [r for r in rows if r['source_record_id']['owner_epoch'] == 1] == first['tables'][table]
    owner = only(after['owner_epochs'], role='control', epoch=job['owner_epoch'])
    requests(after, 2, manifest, data, list(science.receipts.values()), owner,
        {2: request_binding(new_event, wire, 1), 3: request_binding(old_event, wire, 2)}, source)
    successor_progress(after, manifest, science, job, detail, owner, wire, new_event, completed)
    wanted.update(inputs=data, receipts=list(science.receipts.values()), manifest=manifest)
    return wanted


def fresh_successor(before, intent, domains):
    import re
    previous = set(re.findall(r'\\x([0-9a-f]{32})(?![0-9a-f])', json.dumps(before)))
    selected = [intent['target_incarnation'], intent['target_role_instance_id'], *domains]
    assert len(domains) == 9 and len(selected) == len(set(selected))
    for value in selected:
        assert re.fullmatch(r'\\x[0-9a-f]{32}', value) and int(value[2:], 16)
        assert value[2:] not in previous, 'successor reused an old identity'


def successor_progress(state, manifest, science, job, detail, owner, wire, event, completed):
    import installed_progress_replay as typed
    from installed_family_science import decode
    actual = only(state['acquisition_progress'], producer_epoch=2)
    head = only(state['producer_checkpoints'], producer_epoch=2)
    common = dict(dataset_id=job['dataset_id'], producer_slot_id=science.m.slot, producer_epoch=2)
    expected = dict(common, job_id=job['job_id'], scope_id=detail['scope_id'], progress_revision=3,
        request_id='\\x' + wire['request'][1].encode().hex(), event_ordinal=3, wire_sequence=3,
        input_sha256=request_binding(event, wire, 3)['progress_input_sha256'], classification=5, mutation_request=None,
        classified_events=3, successful_events=3, rejected_events=0, observed_at_us=actual['observed_at_us'],
        clock_resolution_us=wire['clock_resolution_us'], clock_anomaly_flags=4 if wire['clock_resolution_us'] > 1 else 0,
        xmin=actual['xmin'])
    expected['intent_digest'] = '\\x' + typed.certification_digest(event, science.scope, science.ref(2), science.ref(2),
        [science.ref(1), science.ref(3), science.ref(5)], expected, wire).hex()
    assert head['xmin'] == actual['xmin']
    # This fresh, bounded, nonwrapping fixture serializes the final proof after all three outcomes.
    assert int(typed.xid(head['xmin'])) > max(int(typed.xid(r['xmin'])) for r in state['producer_outcomes'] if r['producer_epoch'] == 2)
    clock(head['committed_at_us'], max(actual['observed_at_us'], wire['sent_at_us']), completed)
    assert head['committed_at_us'] < owner['lease_expires_at_us']
    dependencies = []
    for epoch, sequence in [(1, n) for n in (3, 4, 8, 9, 10, 11)] + [(2, n) for n in range(1, 11)]:
        key = ('facts', (b'\x01\x20' + raw(common['dataset_id']) + bytes(4) + be(epoch) + be(sequence)).hex())
        fact = bytes.fromhex(science.values[key])
        reference = [[raw(common['dataset_id']), 0, epoch, sequence], fact[2:34]]
        first = next(n for n, receipt in science.receipts.items() if reference in decode(receipt.split(b'\0', 1)[1])[5])
        dependencies.append(dict(common, source_shard=0, source_epoch=epoch, source_sequence=sequence,
            payload_digest='\\x' + fact[2:34].hex(), xmin=only(state['producer_outcomes'],
                producer_epoch=2, request_sequence=first)['xmin']))
    expected_head = dict(common, progress_format=1, manifest_sha256='\\x' + sha(manifest), checkpoint_revision=4,
        complete_through_request=3, progress_jobs=1, dependency_count=16, issuer_resource=owner['resource_id'],
        issuer_epoch=owner['epoch'], issuer_incarnation=owner['process_incarnation'], committed_at_us=head['committed_at_us'],
        xmin=head['xmin'], checkpoint_digest='\\x' + '00' * 32)
    expected_head['checkpoint_digest'] = '\\x' + typed.head_digest(expected_head, [expected], dependencies,
        lambda row: typed.native_reference(row, science.values)).hex()
    actual_dependencies = sorted([r for r in state['producer_checkpoint_dependencies'] if r['producer_epoch'] == 2],
        key=lambda row: (row['source_shard'], row['source_epoch'], row['source_sequence']))
    typed.exact_snapshot(head, [actual], actual_dependencies,
        expected_head, [expected], dependencies, science.values)


def publication(state, census, science, *, revision, snapshot, topology, cuts):
    from installed_archive_catch_up_oracle import cut_digest
    assert set(census) == {'format', 'tables'}
    assert set(census['tables']) == set(science['tables']) | {'archive_commits'}
    attempts = sorted(state['archive_attempts'], key=lambda row: row['ordinal'])
    assert len(attempts) == len(science['batches'])
    assert len({row['attempt_id'] for row in attempts}) == len(attempts)
    dataset = only(state['datasets'])['dataset_id']
    consumer = only(state['consumers'])
    assert consumer['current_revision'] == revision and consumer['current_topology_generation'] == topology
    receipts = []
    for n, (attempt, batch) in enumerate(zip(attempts, science['batches']), 1):
        assert attempt['ordinal'] == n and attempt['expected_predecessor'] == n - 1
        assert attempt['dataset_id'] == dataset and attempt['catalog_id'] == consumer['consumer_id']
        assert attempt['schema_version'] == 1 and attempt['topology_generation'] == batch['topology']
        assert attempt['batch_digest'] == '\\x' + batch['archive_digest']
        receipts.append(dict(dataset_id=dataset[2:].upper(), schema_version=1, topology_generation=batch['topology'],
            batch_digest=batch['archive_digest'].upper(), record_count=batch['last'] - batch['first'] + 1,
            input_ranges=[dict(shard_id=0, owner_epoch=batch['epoch'], first_sequence=batch['first'], last_sequence=batch['last'])],
            attempt_id=attempt['attempt_id'][2:].upper(), publication_ordinal=n))
    exact(census['tables'], {**science['tables'], 'archive_commits': receipts}, 'complete scientific census and commits')
    frontier = only(state['archive_frontier_snapshots'], frontier_revision=revision)
    assert frontier['snapshot_id'] == snapshot and frontier['topology_generation'] == topology
    assert [frontier[k] for k in COUNTS] == [len(rows) for rows in science['tables'].values()]
    assert frontier['request_digest'] == '\\x' + cut_digest(dataset, topology, receipts, cuts, snapshot)
    bounds = sorted([r for r in state['consumer_frontiers'] if r['frontier_revision'] == revision], key=lambda r: r['owner_epoch'])
    assert [(r['owner_epoch'], r['last_complete_sequence']) for r in bounds] == cuts
    assert all(r['shard_id'] == 0 and r['topology_generation'] == topology for r in bounds)
    assert {r['attempt_id'] for r in state['archive_frontier_batches'] if r['frontier_revision'] == revision} == {r['attempt_id'] for r in attempts}
    return receipts


def closure(manifest, slot, epoch, inputs, receipts, cut, handoff=None):
    prefix = b'bigbrotr/initial-source-closure/1\0' if epoch == 1 else b'bigbrotr/activated-initial-source-closure/1\0'
    data = prefix + (hashlib.sha256(handoff).digest() if handoff is not None else b'')
    data += hashlib.sha256(manifest).digest() + raw(slot) + be(epoch) + be(len(inputs))
    for n, (original, receipt) in enumerate(zip(inputs, receipts), 1):
        data += be(n) + hashlib.sha256(original).digest() + hashlib.sha256(receipt).digest()
    return sha(data + be(cut))


def retired(state, report, epoch, science, *, cut):
    producer = only(state['producer_epochs'], producer_epoch=epoch)
    handoff = raw(only(state['initial_ingest_successions'])['handoff']) if epoch == 2 else None
    digest = closure(science['manifest'], producer['producer_slot_id'], epoch, science['inputs'], science['receipts'], cut, handoff)
    source = only(state['source_streams'], owner_epoch=epoch)
    owner = only(state['owner_epochs'], role='ingest', epoch=epoch)
    assert producer['state'] == 'retired' and source['state'] == 'closed' and owner['state'] == 'released'
    assert producer['closure_digest'] == source['closure_digest'] == owner['exclusion_digest'] == '\\x' + digest
    assert source['last_complete_sequence'] == cut
    assert owner['exclusion_method'] == 'storage_fenced'
    domains = [row for row in state['allocation_domains'] if row['owner_epoch'] == epoch]
    assert len(domains) == 9 and all(row['state'] == 'closed' and row['closure_digest'] == '\\x' + digest for row in domains)
    assert report['format'] == 'bigbrotr-ingest-recovery/1' and report['ready'] is False
    assert report['producer_epoch'] == str(epoch) and report['native'] == 'closed' and report['owner_release'] == 'committed'
    assert report['state'] == ('initial-retired' if epoch == 1 else 'activated-initial-retired')
    assert report['source_closure'] == dict(last_complete_sequence=str(cut), sha256=digest)
    assert report['ingest_retirement'] == dict(allocation_domains='9', acquisition_gaps='1',
        owner_release='committed', native_acknowledged=str(len(science['inputs'])), native_retired='0')
    gap = only(state['acquisition_gaps'], producer_epoch=epoch)
    assert gap['reason'] == ('initial_producer_abandoned' if epoch == 1 else 'activated_initial_producer_abandoned')
    assert gap['last_complete_sequence'] == cut and gap['source_closure_digest'] == '\\x' + digest
    job = only(state['jobs'], job_id=gap['job_id'])
    assert job['state'] == 'cancelled' and job['job_revision'] == 4 and job['checkpoint_revision'] == 0
    assert job['reason_code'] == gap['reason'] and job['completed_at_us'] == gap['abandoned_at_us']


def bridge(state, initial_receipts):
    from installed_archive_catch_up_oracle import cut_digest
    dataset = only(state['datasets'])['dataset_id']
    assert sorted(r['frontier_revision'] for r in state['consumer_frontier_revisions']) == list(range(1, 7))
    row = only(state['archive_frontier_snapshots'], frontier_revision=5)
    assert row['topology_generation'] == 1 and row['snapshot_id'] == 8
    assert [row[key] for key in COUNTS] == [8, 1, 1, 1, 1]
    assert row['request_digest'] == '\\x' + cut_digest(dataset, 1, initial_receipts, [(1, 12), (2, 0)], 8)
    bounds = sorted([r for r in state['consumer_frontiers'] if r['frontier_revision'] == 5], key=lambda r: r['owner_epoch'])
    assert [(r['owner_epoch'], r['last_complete_sequence']) for r in bounds] == [(1, 12), (2, 0)]
    assert bounds[1]['attestation_kind'] is None and bounds[1]['attestation_digest'] is None
    assert {r['attempt_id'][2:].upper() for r in state['archive_frontier_batches'] if r['frontier_revision'] == 5} == {r['attempt_id'] for r in initial_receipts}


def catch_up_report(report, state, *, retry):
    frontier = only(state['archive_frontier_snapshots'], frontier_revision=6)
    expected = dict(format='bigbrotr-archive-catch-up/1', dataset_id=only(state['datasets'])['dataset_id'][2:],
        catalog_id=only(state['consumers'])['consumer_id'][2:], terminal_epoch=2,
        issued_attempts_before=7 if retry else 4, new_attempts=0 if retry else 3,
        frontier_revision=6, snapshot_id=8, operation_id=list(raw(frontier['operation_id'])),
        request_digest=frontier['request_digest'][2:], family_counts=[13, 2, 2, 2, 3],
        new_frontier_revisions=0 if retry else 2)
    exact(report, expected, 'exact current catch-up report')


def preserve_export(before, after):
    for table in ('export_jobs', 'export_execution_units', 'artifacts', 'artifact_files', 'scope_grants', 'scope_grant_changes'):
        assert after[table] == before[table], 'original export/authority changed: ' + table
    for table in ('jobs', 'job_admissions', 'job_input_bounds', 'archive_snapshot_pins', 'snapshot_leases', 'retention_claims'):
        for row in before[table]:
            if table != 'jobs' or row['kind'] == 'export':
                assert row in after[table], 'original export custody changed: ' + table
    for table in ('config_revisions', 'policy_revisions', 'schema_versions', 'principal_grants', 'local_credentials'):
        assert before[table] == after[table], 'accepted security/configuration changed: ' + table


def retry_unchanged(before, after):
    # Finite Control lease/audit lifetimes can advance; scientific publication and producer evidence cannot.
    mutable = {'owner_epochs', 'role_instances'}
    assert set(before) == set(after)
    for table in before.keys() - mutable:
        assert before[table] == after[table], 'exact catch-up retry mutated ' + table


def history_unchanged(before, after):
    mutable = {'owner_epochs', 'role_instances', 'browser_sessions', 'login_buckets', 'local_accounts',
        'management_audit', 'snapshot_leases', 'archive_snapshot_pins', 'retention_claims'}
    assert set(before) == set(after)
    for table in before.keys() - mutable:
        assert before[table] == after[table], 'ordinary history mutated ' + table
    for table in ('owner_epochs', 'role_instances', 'management_audit', 'archive_snapshot_pins', 'retention_claims'):
        assert all(row in after[table] for row in before[table]), 'ordinary history altered prior ' + table
    new_owners = [row for row in after['owner_epochs'] if row not in before['owner_epochs']]
    owner, = new_owners
    assert owner['role'] == 'control' and owner['state'] == 'released' and owner['released_at_us'] is not None
    assert all(row['revoked_at_us'] is not None for row in after['browser_sessions'])
    pins = [row for row in after['archive_snapshot_pins'] if row not in before['archive_snapshot_pins']]
    assert len(pins) == 2 and all(row['purpose'] == 'reader' and row['frontier_revision'] == 6 and row['snapshot_id'] == 8 for row in pins)
    for pin in pins:
        assert only(after['snapshot_leases'], lease_id=pin['lease_id'])['state'] == 'released'
