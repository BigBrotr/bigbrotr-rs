"""Independent installed publication SQL, retained headers and native receipt oracle."""
import hashlib

ARCHIVE_TABLES = {'archive_attempts', 'archive_owner_sessions', 'archive_frontier_snapshots',
    'archive_frontier_batches', 'consumers', 'consumer_frontier_revisions', 'consumer_frontiers'}
COUNTS = ('dictionary_count', 'payload_count', 'eligibility_count', 'receive_count', 'provenance_count')


def clean(row):
    return {k: v for k, v in row.items() if k != 'xmin'}


def current_owner_view(state, setup):
    """Validate the complete owner union before reusing the single-Control oracle."""
    old = setup['control']
    for table in ('owner_epochs', 'role_instances'):
        assert all(row in state[table] for row in old[table]), 'original setup owner changed'
    owners = [r for r in state['owner_epochs'] if r not in old['owner_epochs']]
    control, = [r for r in owners if r['role'] == 'control']
    ingest, = [r for r in owners if r['role'] == 'ingest']
    archive = [r for r in owners if r['role'] == 'archive']
    assert len(archive) <= 1 and len(owners) == 2 + len(archive)
    assert control['epoch'] == max(r['epoch'] for r in old['owner_epochs']) + 1
    assert control['resource_id'] == old['owner_epochs'][0]['resource_id']
    assert ingest['epoch'] == 1
    roles = [r for r in state['role_instances'] if r not in old['role_instances']]
    assert len(roles) == len(owners)
    assert {r['role_instance_id'] for r in roles} == {r['role_instance_id'] for r in owners}
    if archive:
        selected = archive[0]
        assert selected['resource_id'] == '\\x' + setup['resource']['catalog_id']
        assert selected['epoch'] == selected['archive_frontier_protocol'] == 1
        assert selected['process_incarnation'] == control['process_incarnation']
    result = dict(state)
    result['owner_epochs'] = [r for r in state['owner_epochs'] if r in (control, ingest)]
    result['role_instances'] = [r for r in roles if r['role_instance_id'] in (control['role_instance_id'], ingest['role_instance_id'])]
    return result


def cut_digest(dataset, topology, receipts, through, snapshot):
    be = lambda n, width=8: int(n).to_bytes(width, 'big')
    raw = lambda s: bytes.fromhex(s)
    body = b'bigbrotr/archive-cut-inspection/1\0' + raw(dataset) + be(topology)
    body += be(1) + be(0, 4) + be(1) + be(through) + be(len(receipts))
    for r in receipts:
        body += raw(r['dataset_id']) + be(r['schema_version'], 2) + be(r['topology_generation'])
        body += raw(r['batch_digest']) + be(r['record_count']) + raw(r['attempt_id']) + be(r['publication_ordinal'])
        body += be(1)
        bound, = r['input_ranges']
        body += be(bound['shard_id'], 4) + be(bound['owner_epoch']) + be(bound['first_sequence']) + be(bound['last_sequence'])
    return hashlib.sha256(body + b'\x01' + be(snapshot)).hexdigest()


def validate(state, catalog, setup, wanted):
    dataset = state['datasets'][0]['dataset_id']
    catalog_id = '\\x' + setup['resource']['catalog_id']
    view = current_owner_view(state, setup)
    control, = [r for r in view['owner_epochs'] if r['role'] == 'control']
    owner, = [r for r in state['owner_epochs'] if r['role'] == 'archive']
    assert clean(owner) == dict(resource_id=catalog_id, epoch=1, dataset_id=dataset, role='archive',
        role_instance_id=owner['role_instance_id'], process_incarnation=control['process_incarnation'],
        acquired_at_us=owner['acquired_at_us'], lease_expires_at_us=owner['lease_expires_at_us'],
        state='active', released_at_us=None, exclusion_method=None, exclusion_digest=None,
        control_fence_version=None, control_backend_pid=None, control_backend_start=None, archive_frontier_protocol=1)
    role, = [r for r in state['role_instances'] if r['role_instance_id'] == owner['role_instance_id']]
    assert clean(role) == dict(role_instance_id=owner['role_instance_id'], dataset_id=dataset, role='archive',
        created_at_us=role['created_at_us'], retired_at_us=None)
    assert control['acquired_at_us'] <= role['created_at_us'] <= owner['acquired_at_us'] < owner['lease_expires_at_us']
    stream, = state['topology_streams']
    topology = stream['topology_generation']
    assert stream['shard_id'] == 0 and stream['owner_epoch'] == 1
    attempts = sorted(state['archive_attempts'], key=lambda r: r['ordinal'])
    native = sorted(catalog['bigbrotr_archive.attempts'], key=lambda r: r['ordinal'])
    assert len(attempts) == len(native) == len(wanted['batches']) == 4
    assert len({r['coordinator_incarnation'] for r in attempts}) == 1
    assert len({r['attempt_id'] for r in attempts}) == len({r['worker_incarnation'] for r in attempts}) == 4
    receipts = []
    for number, (attempt, actual_native, batch) in enumerate(zip(attempts, native, wanted['batches']), 1):
        assert clean(attempt) == dict(attempt_id=attempt['attempt_id'], dataset_id=dataset, catalog_id=catalog_id,
            ordinal=number, expected_predecessor=number-1, schema_version=1, topology_generation=topology,
            batch_digest='\\x' + batch['archive_digest'], coordinator_incarnation=attempt['coordinator_incarnation'],
            worker_incarnation=attempt['worker_incarnation'], issuer_resource=control['resource_id'], issuer_epoch=control['epoch'],
            issuer_role='control', issuer_incarnation=control['process_incarnation'], issued_at_us=attempt['issued_at_us'],
            coordinator_binding_sha256='\\x' + setup['resource']['binding_sha256'], persistence_protocol=1)
        assert control['acquired_at_us'] <= attempt['issued_at_us'] < control['lease_expires_at_us']
        for field in ('attempt_id', 'coordinator_incarnation', 'worker_incarnation'):
            raw = bytes.fromhex(attempt[field][2:]); assert len(raw) == 16 and any(raw)
        assert attempt['coordinator_incarnation'] != attempt['worker_incarnation']
        assert clean(actual_native) == {**{k: attempt[k] for k in ('ordinal','attempt_id','coordinator_incarnation',
            'worker_incarnation','topology_generation','batch_digest')},
            'login_name': actual_native['login_name'], 'login_oid': actual_native['login_oid']}
        receipts.append(dict(dataset_id=dataset[2:].upper(), schema_version=1, topology_generation=topology,
            batch_digest=batch['archive_digest'].upper(), record_count=batch['last']-batch['first']+1,
            input_ranges=[dict(shard_id=0, owner_epoch=1, first_sequence=batch['first'], last_sequence=batch['last'])],
            attempt_id=attempt['attempt_id'][2:].upper(), publication_ordinal=number))
    native_owner, = catalog['bigbrotr_archive.owner']
    assert clean(native_owner) == dict(singleton=True, ordinal=4, enabled=False)
    assert sorted(r['snapshot_id'] for r in catalog['public.ducklake_snapshot']) == list(range(6))
    for table in ('bigbrotr_archive.installation', 'bigbrotr_archive.scientific_schema',
            'public.ducklake_schema', 'public.ducklake_table', 'public.ducklake_column'):
        assert catalog[table] == setup['catalog'][table], table
    attribution, = state['archive_owner_sessions']
    assert clean(attribution) == dict(dataset_id=dataset, catalog_id=catalog_id, owner_epoch=1, owner_role='archive',
        owner_incarnation=control['process_incarnation'], coordinator_binding_sha256='\\x' + setup['resource']['binding_sha256'],
        persistence_protocol=1, issuer_resource=control['resource_id'], issuer_epoch=control['epoch'],
        issuer_role='control', issuer_incarnation=control['process_incarnation'])
    revisions = sorted(state['consumer_frontier_revisions'], key=lambda r: r['frontier_revision'])
    snapshots = sorted(state['archive_frontier_snapshots'], key=lambda r: r['frontier_revision'])
    bounds = sorted(state['consumer_frontiers'], key=lambda r: r['frontier_revision'])
    assert 1 <= len(revisions) == len(snapshots) == len(bounds) <= 4
    consumer, = state['consumers']
    assert clean(consumer) == dict(consumer_id=catalog_id, dataset_id=dataset, consumer_role='archive',
        created_at_us=consumer['created_at_us'], current_revision=len(revisions), current_topology_generation=topology)
    prior_through = 0
    expected_batches = []
    for n, (revision, snapshot, bound) in enumerate(zip(revisions, snapshots, bounds), 1):
        through = bound['last_complete_sequence']
        assert through in (2, 3, 7, 12) and through > prior_through
        count = (2, 3, 7, 12).index(through) + 1
        assert clean(revision) == dict(consumer_id=catalog_id, frontier_revision=n, dataset_id=dataset,
            consumer_role='archive', topology_generation=topology, previous_revision=n-1 if n>1 else None,
            owner_resource_id=catalog_id, owner_epoch=1, owner_incarnation=control['process_incarnation'],
            created_at_us=revision['created_at_us'], sealed=True)
        counts = (8, 1, 1, 1, 1) if count == 4 else (through, 0, 0, 0, 0)
        assert clean(snapshot) == dict(operation_id=snapshot['operation_id'], consumer_id=catalog_id,
            frontier_revision=n, dataset_id=dataset, topology_generation=topology, catalog_id=catalog_id,
            owner_epoch=1, owner_incarnation=control['process_incarnation'], snapshot_id=count+1,
            request_digest='\\x' + cut_digest(dataset[2:], topology, receipts[:count], through, count+1),
            **dict(zip(COUNTS, counts)))
        operation = bytes.fromhex(snapshot['operation_id'][2:]); assert len(operation) == 16 and any(operation)
        assert clean(bound) == dict(consumer_id=catalog_id, frontier_revision=n, dataset_id=dataset,
            topology_generation=topology, shard_id=0, owner_epoch=1, last_complete_sequence=through,
            attestation_kind='archive-batch', attestation_digest='\\x' + wanted['batches'][count-1]['archive_digest'])
        expected_batches += [dict(consumer_id=catalog_id, frontier_revision=n, attempt_id=a['attempt_id']) for a in attempts[:count]]
        selected = [r for r in state['archive_frontier_batches'] if r['frontier_revision'] == n]
        assert {revision['xmin'], snapshot['xmin'], bound['xmin'], *[r['xmin'] for r in selected]} == {revision['xmin']}
        assert control['acquired_at_us'] <= consumer['created_at_us'] <= revision['created_at_us'] < control['lease_expires_at_us']
        prior_through = through
    assert prior_through == 12 and snapshots[-1]['snapshot_id'] == 5
    assert len({r['operation_id'] for r in snapshots}) == len(snapshots)
    canonical = lambda rows: sorted(rows, key=lambda r: json_key(r))
    assert canonical([clean(r) for r in state['archive_frontier_batches']]) == canonical(expected_batches)
    return receipts


def json_key(row):
    return row['frontier_revision'], row['attempt_id']


def retained_inputs(home, state, wanted):
    initialization = hashlib.sha256((home / 'data/.bigbrotr-initialization/intent.json').read_bytes()).digest()
    be = lambda n, width=8: int(n).to_bytes(width, 'big')
    retained = []
    for row, batch in zip(sorted(state['archive_attempts'], key=lambda r: r['ordinal']), wanted['batches']):
        raw = lambda key: bytes.fromhex(row[key][2:])
        body = (b'BBINPUT\x01' + initialization + raw('attempt_id') + raw('dataset_id') + raw('catalog_id')
            + raw('coordinator_incarnation') + raw('worker_incarnation') + be(row['ordinal'])
            + be(row['topology_generation']) + bytes.fromhex(batch['archive_digest']) + be(1, 4)
            + be(0, 4) + be(1) + be(batch['first']) + b'\x01\x21' + be(batch['last'])
            + bytes.fromhex(batch['admission_digest']))
        expected = body + hashlib.sha256(body).digest()
        path = home / 'state' / (row['attempt_id'][2:] + '.inputs')
        assert path.read_bytes() == expected and not path.is_symlink()
        assert path.stat().st_mode & 0o777 == 0o600 and path.stat().st_nlink == 1
        retained.append(dict(name=path.name, bytes=len(expected), sha256=hashlib.sha256(expected).hexdigest()))
    assert sorted(p.name for p in (home / 'state').glob('*.inputs')) == sorted(r['name'] for r in retained)
    return retained
