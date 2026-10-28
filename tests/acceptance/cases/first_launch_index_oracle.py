"""Independent closed one-event Index expectations, never product row mapping.

Scientific inputs come from the maintained literal signed/source oracle. SQL
observations supply only generated identity bindings, not expected row values.
Coordination encodings follow storage-services.md; all ten zero/nonzero outcomes
are checked. No engine, subprocess or file mutation occurs here.
"""
import hashlib
import json
import re
import uuid

TARGETS = ('dictionaries', 'events', 'event_eligibility_facts', 'event_receive_facts',
           'event_id_index', 'event_author_index', 'event_time_index', 'event_tag_index',
           'provenance_by_event', 'provenance_by_relay')
COUNTS = (8, 1, 1, 1, 1, 1, 1, 0, 1, 1)
# Largest single target in one literal source batch, not the cumulative census.
BATCH_MAX_ROWS = 4
CONFLICTING_CONTENT = 'independent physical readiness corruption'
RANGES = ((1, 2), (3, 3), (4, 7), (8, 12))
PLAN_COUNTS = ((2, 0, 0, 0, 0, 0, 0, 0, 0, 0),
               (1, 0, 0, 0, 0, 0, 0, 0, 0, 0),
               (4, 0, 0, 0, 0, 0, 0, 0, 0, 0),
               (1, 1, 1, 1, 1, 1, 1, 0, 1, 1))
INDEX_TABLES = ('projection_generations', 'index_owner_sessions', 'projection_plans',
                'projection_plan_chunks', 'projection_chunks', 'projection_chunk_bounds', 'index_frontier_plans')


def exact(actual, expected):
    assert json.dumps(actual, sort_keys=True, allow_nan=False) == json.dumps(expected, sort_keys=True, allow_nan=False)


def only(rows, **fields):
    row, = [r for r in rows if all(r[k] == v for k, v in fields.items())]
    return row


def raw(value, length=None):
    assert isinstance(value, str) and value.startswith('\\x')
    data = bytes.fromhex(value[2:])
    assert length is None or len(data) == length
    return data


def be(value, width=8):
    assert type(value) is int and 0 <= value < 1 << (8*width)
    return value.to_bytes(width, 'big')


def accepted_profile(state):
    dataset, config, policy = (only(state[t]) for t in ('datasets', 'config_revisions', 'policy_revisions'))
    exact([dataset['config_revision'], dataset['policy_revision'], config['revision'],
           policy['revision'], policy['config_revision']], [1]*5)
    exact(config['document'], policy['document'])
    assert config['document_digest'] == policy['document_digest']
    assert policy['document']['format'] == 'bigbrotr-product-policy/2'
    settings = policy['document']['settings']
    assert settings['indexer.enabled'] is True and settings['sync.provenance'] == 'local_first_witness'
    assert type(settings['indexer.batch_max_rows']) is int and settings['indexer.batch_max_rows'] == BATCH_MAX_ROWS


def live_roles(value, state, clock_before, clock_after, *, invalidated=False, revision=4, sequence=12):
    """Literal signed fixture frontier plus separately captured SQL identities."""
    assert type(revision) is type(sequence) is int and (revision, sequence) in ((2, 3), (4, 12))
    assert not invalidated or (revision, sequence) == (4, 12)
    dataset = only(state['datasets'])
    control = only(state['owner_epochs'], role='control', state='active')
    generation = only(state['projection_generations'])
    exact(sorted(value), sorted(('format', 'identity', 'observed_at_us', 'source', 'roles', 'capabilities', 'authority')))
    assert value['format'] == 'bigbrotr-role-status/1' and value['authority'] == 'observation-only'
    exact(value['identity'], dict(dataset_id=raw(dataset['dataset_id'],16).hex(),
        process_incarnation=raw(control['process_incarnation'],16).hex(),
        config_revision=str(dataset['config_revision']), policy_revision=str(dataset['policy_revision'])))
    assert re.fullmatch('-?(0|[1-9][0-9]*)', value['observed_at_us'])
    observed = int(value['observed_at_us'])
    assert -(1 << 63) <= observed < (1 << 63)
    assert type(clock_before) is int and type(clock_after) is int
    assert clock_before <= observed <= clock_after
    cut = dict(dataset_id=raw(dataset['dataset_id'],16).hex(), topology_generation='0',
        bounds=[dict(shard_id='0',owner_epoch='1',last_complete_sequence=str(sequence))])
    def timed(observation):
        stamp = observation['time']
        assert set(stamp) == {'observed_at_us','age_us'} and stamp['observed_at_us'] == str(observed)
        assert re.fullmatch('0|[1-9][0-9]*',stamp['age_us']) and int(stamp['age_us']) < 1 << 64
        age = int(stamp['age_us'])
        assert observation['freshness'] == ('fresh' if age < 30_000_000 else 'stale')
    def fresh(observation):
        assert observation['freshness'] == 'fresh' and observation['reason'] == 'current'
        timed(observation)
    assert set(value['source']) == {'cut','freshness','time','reason'}
    exact(value['source']['cut'],cut)
    fresh(value['source'])
    exact(value['roles'], [dict(role=role,
        state='degraded' if invalidated and role=='index' else 'ready' if role=='control' else 'idle',
        reason='remote_outcome_unknown' if invalidated and role=='index' else 'current')
        for role in ('control','ingest','sync','archive','index')])
    caps = value['capabilities']
    assert len(caps) == 4
    for cap,kind in zip(caps, ('signed_event_admission','finite_acquisition','archive_publication','index_publication')):
        assert set(cap) == {'capability','state','reason','freshness','time','published','generation','storage_incarnation'}
        assert cap['capability'] == kind
        if invalidated and kind == 'index_publication':
            # The second distinct row exceeds the declared one-row chunk.
            # Rejection precedes cursor EOF, so local join cannot establish
            # remote query cessation or return its retained engine credit.
            assert cap['state'] == 'unavailable' and cap['reason'] == 'remote_outcome_unknown'
            timed(cap)  # Retain the last full verified cut, never a zero/unknown replacement.
        else:
            assert cap['state'] == 'ready'
            fresh(cap)
        exact(cap['published'],dict(revision=str(revision),cut=cut) if kind.endswith('_publication') else None)
        assert cap['generation'] == (raw(generation['generation_id'],16).hex() if kind=='index_publication' else None)
        assert cap['storage_incarnation'] == (raw(generation['storage_incarnation'],16).hex() if kind=='index_publication' else None)


def content_conflict(before, after):
    """One independently introduced distinct row, not harmless exact duplication.

    The original all-family capture is separately checked against the signed
    source by publication(). Equal source/discriminator with different content
    must survive SELECT DISTINCT and invalidate complete typed verification.
    """
    exact(sorted(before), sorted(TARGETS))
    exact(sorted(after), sorted(TARGETS))
    exact([len(before[name]) for name in TARGETS], list(COUNTS))
    original, = before['events']
    assert original['content'] == 'import then network'
    conflicting = dict(original, content=CONFLICTING_CONTENT)
    # The two rows have equal ORDER BY keys; SQL need not order the tie.
    exact(sorted(after['events'], key=lambda row: row['content']),
          sorted([original, conflicting], key=lambda row: row['content']))
    for name in TARGETS:
        if name != 'events':
            exact(before[name], after[name])


def ordinary_selection(state):
    """Current complete Archive membership, independently fixed rev4/cut12/5."""
    dataset = only(state['datasets'])['dataset_id']
    consumer = only(state['consumers'], consumer_role='archive')
    assert consumer['dataset_id'] == dataset
    exact([consumer['current_revision'], consumer['current_topology_generation']], [4, 0])
    catalog = consumer['consumer_id']
    snapshot = only(state['archive_frontier_snapshots'], consumer_id=catalog, frontier_revision=4)
    exact([snapshot[k] for k in ('dataset_id', 'catalog_id', 'topology_generation', 'snapshot_id',
          'dictionary_count', 'payload_count', 'eligibility_count', 'receive_count', 'provenance_count')],
          [dataset, catalog, 0, 5, 8, 1, 1, 1, 1])
    bound = only(state['consumer_frontiers'], consumer_id=catalog, frontier_revision=4)
    exact([bound[k] for k in ('dataset_id', 'topology_generation', 'shard_id', 'owner_epoch', 'last_complete_sequence')],
          [dataset, 0, 0, 1, 12])
    members = [r for r in state['archive_frontier_batches'] if r['consumer_id'] == catalog and r['frontier_revision'] == 4]
    assert len(members) == len({r['attempt_id'] for r in members}) == 4
    attempts = sorted((only(state['archive_attempts'], attempt_id=r['attempt_id']) for r in members), key=lambda r:r['ordinal'])
    exact([r['ordinal'] for r in attempts], [1, 2, 3, 4])
    assert all(r['dataset_id'] == dataset and r['catalog_id'] == catalog for r in attempts)
    return attempts


def ordinary_roles(value, state, clock_before, clock_after, phase=None):
    ordinary_selection(state)
    dataset = only(state['datasets'])
    control = only(state['owner_epochs'], role='control', state='active')
    assert control['dataset_id'] == dataset['dataset_id']
    exact(sorted(value), sorted(('format', 'identity', 'observed_at_us', 'source', 'roles', 'capabilities', 'authority')))
    assert value['format'] == 'bigbrotr-role-status/1' and value['authority'] == 'observation-only'
    exact(value['identity'], dict(dataset_id=raw(dataset['dataset_id'],16).hex(),
        process_incarnation=raw(control['process_incarnation'],16).hex(), config_revision='1', policy_revision='1'))
    exact([dataset['config_revision'],dataset['policy_revision']], [1,1])
    assert re.fullmatch('-?(0|[1-9][0-9]*)', value['observed_at_us'])
    observed = int(value['observed_at_us'])
    assert -(1 << 63) <= observed < (1 << 63) and clock_before <= observed <= clock_after
    exact(value['source'], dict(cut=None,freshness='unknown',time=None,reason='not_selected'))
    exact(value['roles'], [dict(role='control',state='ready',reason='current')])
    caps = value['capabilities']
    assert len(caps) == 2
    published = dict(revision='4',cut=dict(dataset_id=raw(dataset['dataset_id'],16).hex(),
        topology_generation='0', bounds=[dict(shard_id='0',owner_epoch='1',last_complete_sequence='12')]))
    for cap,kind in zip(caps, ('private_history','signed_event_export')):
        exact(sorted(cap), sorted(('capability','state','reason','freshness','time','published','generation','storage_incarnation')))
        assert cap['capability'] == kind and cap['generation'] is cap['storage_incarnation'] is None
        age = None
        if cap['time'] is not None:
            exact(sorted(cap['time']), ['age_us','observed_at_us'])
            assert cap['time']['observed_at_us'] == str(observed)
            assert re.fullmatch('0|[1-9][0-9]*',cap['time']['age_us'])
            age = int(cap['time']['age_us'])
            assert age < 1 << 64
            assert cap['freshness'] == ('fresh' if age < 30_000_000 else 'stale')
            exact(cap['published'], published)
        else:
            assert cap['freshness'] == 'unknown' and cap['published'] is None
        if phase == 'setup':
            assert cap['state'] == 'building' and cap['reason'] == 'not_observed' and age is None
        elif phase in ('missing', 'narrow'):
            assert cap['state'] == 'unavailable' and cap['reason'] == 'dependency_unavailable' and age is not None
        elif phase in ('positive', 'restored'):
            assert age is not None
            if phase == 'restored' and kind == 'signed_event_export' and age >= 30_000_000:
                assert cap['state'] == 'stale' and cap['reason'] == 'verification_stale'
            else:
                assert age < 30_000_000 and cap['state'] == 'ready' and cap['reason'] == 'current'
        else:
            assert phase is None


def startup_reader_pending(before, after):
    """Only absent or active new reader observations may wait for completion."""
    new = {row['lease_id'] for row in after['snapshot_leases'] if row not in before['snapshot_leases']}
    assert len(new) <= 2, 'startup reader observation bound'
    for table in ('snapshot_leases','archive_snapshot_pins','retention_claims'):
        exact([r for r in after[table] if r['lease_id'] not in new],before[table])
        assert len([r for r in after[table] if r['lease_id'] in new]) == len(new), 'incomplete startup reader observation'
    if not new:
        return True
    owner = only(after['owner_epochs'],role='control',state='active')
    pending = False
    for key in new:
        lease = only(after['snapshot_leases'],lease_id=key)
        pin = only(after['archive_snapshot_pins'],lease_id=key)
        claim = only(after['retention_claims'],lease_id=key)
        assert lease['state'] in ('active','released') and claim['state'] == lease['state'], 'inconsistent startup reader state'
        if lease['state'] == 'active':
            exact([lease[k] for k in ('lease_revision','archive_pin_protocol','holder_kind','holder_role_instance_id','holder_role','holder_incarnation','released_at_us','release_reason')],
                  [1,1,'role_instance',owner['role_instance_id'],'control',owner['process_incarnation'],None,None])
            exact([pin[k] for k in ('purpose','issuer_resource','issuer_epoch','issuer_incarnation')],
                  ['reader',owner['resource_id'],owner['epoch'],owner['process_incarnation']])
            exact([claim[k] for k in ('purpose','released_at_us','release_reason')],['reader',None,None])
            pending = True
    return pending


def ordinary_startup(before, after, roles, clock_before, clock_after, worker):
    """Fresh operation caches remain empty after an actually completed full probe."""
    def require(condition, message):
        assert condition, message
    ordinary_roles(roles,after,clock_before,clock_after,'setup')
    catalog = only(after['consumers'],consumer_role='archive')['consumer_id']
    require(worker is None, 'startup native child remains')
    reader_ids = {row['lease_id'] for row in after['archive_snapshot_pins'] if row['purpose'] == 'reader'}
    require(not any(row['state'] == 'active' and row['lease_id'] in reader_ids
                    for row in after['snapshot_leases']), 'active reader remains at startup barrier')
    owner, = [row for row in after['owner_epochs'] if row['role'] == 'control' and row['state'] == 'active']
    previous = max((r for r in before['owner_epochs'] if r['role'] == 'control'),key=lambda r:r['epoch'])
    require(previous['state'] == 'released' and owner['epoch'] == previous['epoch']+1
            and owner['resource_id'] == previous['resource_id']
            and owner['released_at_us'] is None and owner['lease_expires_at_us'] > clock_after,
            'same current new Control authority at startup')
    new = {row['lease_id'] for row in after['snapshot_leases'] if row not in before['snapshot_leases']}
    require(1 <= len(new) <= 2, 'bounded completed startup probes')
    for table in ('snapshot_leases','archive_snapshot_pins','retention_claims'):
        require(len([r for r in after[table] if r['lease_id'] in new]) == len(new), 'one startup pin and complete claim')
        exact([r for r in after[table] if r['lease_id'] not in new],before[table])
    for key in new:
        lease, = [r for r in after['snapshot_leases'] if r['lease_id'] == key]
        pin, = [r for r in after['archive_snapshot_pins'] if r['lease_id'] == key]
        claim, = [r for r in after['retention_claims'] if r['lease_id'] == key]
        require(lease['state'] == claim['state'] == 'released' and lease['lease_revision'] == 2
                and lease['release_reason'] == claim['release_reason'] == 'reader_joined'
                and pin['purpose'] == claim['purpose'] == 'reader'
                and lease['holder_role_instance_id'] == owner['role_instance_id']
                and pin['issuer_epoch'] == owner['epoch'], 'startup probe actually released')
        exact([pin[k] for k in ('dataset_id','issuer_resource','issuer_incarnation','issuer_role','frontier_revision')],
                    [owner['dataset_id'],owner['resource_id'],owner['process_incarnation'],'control',4])
        exact([claim[k] for k in ('dataset_id','topology_generation','shard_id','owner_epoch','first_sequence','last_sequence')],
                    [owner['dataset_id'],0,0,1,1,12])
        require(lease['dataset_id'] == owner['dataset_id'] and lease['consumer_id'] == pin['consumer_id'] == catalog
                and lease['holder_kind'] == 'role_instance' and lease['holder_job_id'] is None
                and lease['holder_role'] == 'control' and lease['holder_incarnation'] == owner['process_incarnation']
                and lease['frontier_revision'] == 4 and lease['archive_pin_protocol'] == 1
                and owner['acquired_at_us'] <= lease['acquired_at_us'] <= claim['released_at_us'] <= lease['released_at_us'] <= clock_after
                and lease['updated_at_us'] == lease['released_at_us']
                and lease['xmin'] == claim['xmin'], 'startup pin identity and atomic joined release history')
    return owner


def ordinary_input(state, science, initialization):
    """Select by current membership, then reconstruct original input bytes."""
    row = ordinary_selection(state)[0]
    batch = science['batches'][0]
    exact([batch['first'],batch['last']], [1,2])
    assert row['batch_digest'] == '\\x' + batch['archive_digest']
    body = (b'BBINPUT\x01' + initialization + b''.join(raw(row[k],16) for k in
        ('attempt_id','dataset_id','catalog_id','coordinator_incarnation','worker_incarnation'))
        + be(1) + be(0) + bytes.fromhex(batch['archive_digest']) + be(1,4)
        + be(0,4) + be(1) + be(1) + b'\x01\x21' + be(2) + bytes.fromhex(batch['admission_digest']))
    return row, body + hashlib.sha256(body).digest()


def ordinary_unchanged(before, after):
    # Status may renew Control and the coalesced probe may add/release pins.
    # Every source/publication/job/auth row is otherwise unchanged in this barrier.
    exact(sorted(before), sorted(after))
    assert len(before) == 81
    for table in before.keys() - {'owner_epochs','snapshot_leases','archive_snapshot_pins','retention_claims'}:
        exact(before[table], after[table])


def ordinary_complete(phases, timing, journal):
    exact(phases, ['setup','positive','missing','narrow','restored'])
    assert journal['status'] == 'restored' and journal['after'] == journal['before']
    assert journal['events'] == ['prepared','withheld','restored']
    assert timing['mode'] == 'ordinary-readiness/1'
    exact([timing[k] for k in ('original_limit_seconds','export_limit_seconds','overall_limit_seconds')], [40,12,190])
    assert type(timing['original_active_ns']) is int and 0 < timing['original_active_ns'] <= 40_000_000_000
    exact([p['phase'] for p in timing['phases']], phases)
    for p in timing['phases']:
        assert p['limit_seconds'] == 30 and 0 < p['finished_ns'] - p['started_ns'] < 30_000_000_000


def unchanged_publication(before, after):
    """Physical invalidation cannot manufacture a new source/publication head."""
    for table in (*INDEX_TABLES, 'consumers', 'consumer_frontiers',
                  'consumer_frontier_revisions', 'datasets', 'config_revisions',
                  'policy_revisions', 'browser_sessions'):
        exact(before[table], after[table])


def retained(root):
    directory = root/'.bigbrotr-index-custody'
    paths = [directory/'identity.json', directory/'tables.json']
    for path in paths:
        assert not path.is_symlink() and path.is_file() and 0 < path.stat().st_size <= 4096
        assert path.stat().st_mode & 0o777 == 0o600 and path.stat().st_nlink == 1
    identity, tables = (json.loads(p.read_text()) for p in paths)
    assert identity['format'] == 'bigbrotr-local-index-custody/1'
    assert identity['root'] == '/var/lib/bigbrotr/index' and identity['provenance'] == 'local_first_witness'
    assert set(tables) == {'format', 'server_version', 'database', 'table_uuids'}
    assert tables['format'] == 'bigbrotr-index-tables/1' and tables['server_version'] == '26.8.35.10'
    assert tables['database'] == 'serving_fixture' and len(tables['table_uuids']) == len(set(tables['table_uuids'])) == 10
    assert all(str(uuid.UUID(t)) == t and uuid.UUID(t).int != 0 for t in tables['table_uuids'])
    return identity, tables, hashlib.sha256(paths[0].read_bytes()).digest()


def metadata_digest(binding, revision, before, after, source, chunks):
    data = b'bigbrotr/index-plan-metadata/1\0' + b''.join(binding[:6]) + bytes([binding[6]]) + be(revision)
    for topology, streams in (before, after):
        data += be(topology) + be(len(streams), 4)
        for shard, epoch, complete in streams:
            data += be(shard, 4) + be(epoch) + be(complete)
    admission, shard, epoch, first, last = source
    data += admission + be(shard, 4) + be(epoch) + be(first) + be(last)
    assert len(chunks) == 10
    for i, (count, chunk, rows) in enumerate(chunks):
        data += bytes([i]) + be(1, 2) + be(3, 4) + b'all' + chunk + be(count) + rows
    return hashlib.sha256(data).digest()


def engine_digest(table, plan, chunks):
    data = b'bigbrotr/index-engine-receipt/1\0' + table + plan
    for i, (count, _, rows) in enumerate(chunks):
        data += bytes([i]) + be(count) + rows
    return hashlib.sha256(data).digest()


def validate_metadata(state, root, *, complete=False, owner_count=1, publication_fault=None):
    """Check the entire observed Index owner/plan/head before any scoped view.

    Intermediate captures may contain one complete prepared plan. The final
    scientific comparison separately verifies every row commitment against the
    literal source; this structural check never certifies CH contents.
    """
    assert {r['role'] for r in state['owner_epochs']} <= {'control', 'ingest', 'archive', 'index'}
    assert {r['consumer_role'] for r in state['consumers']} <= {'archive', 'index'}
    assert len([r for r in state['consumers'] if r['consumer_role'] == 'archive']) <= 1
    assert publication_fault in (None,'before-forward','after-complete')
    assert type(owner_count) is int and owner_count in ((1,2,3) if publication_fault is None else (2,3,4))
    owners = sorted((r for r in state['owner_epochs'] if r['role'] == 'index'), key=lambda r:r['epoch'])
    if not owners:
        assert not complete and owner_count == 1 and all(not state[t] for t in INDEX_TABLES)
        assert not [r for r in state['consumers'] if r['consumer_role'] == 'index']
        return None
    exact([r['epoch'] for r in owners], list(range(1, owner_count+1)))
    owner = owners[0]  # Generation attribution remains the original creator.
    generation = only(state['projection_generations'])
    sessions = sorted(state['index_owner_sessions'], key=lambda r:r['owner_epoch'])
    exact([r['owner_epoch'] for r in sessions], list(range(1, owner_count+1)))
    session = sessions[0]
    identity, tables, custody = retained(root)
    dataset = only(state['datasets'])['dataset_id']
    for key, field in [('dataset', 'dataset_id'), ('resource', 'owner_resource_id'),
                       ('generation', 'generation_id'), ('storage_incarnation', 'storage_incarnation'),
                       ('table_identity_digest', 'table_identity_digest')]:
        assert generation[field] == '\\x'+identity[key]
        assert re.fullmatch('[0-9a-f]{'+('64' if key == 'table_identity_digest' else '32')+'}', identity[key])
    assert generation['dataset_id'] == dataset and generation['custody_binding_digest'] == '\\x'+custody.hex()
    assert generation['index_publication_protocol'] == owner['index_publication_protocol'] == 1
    assert generation['provenance'] == 'local_first_witness' and owner['epoch'] == generation['owner_epoch'] == session['owner_epoch'] == 1
    assert owner['resource_id'] == session['resource_id'] == generation['owner_resource_id']
    assert owner['process_incarnation'] == generation['owner_incarnation'] == session['owner_incarnation']
    assert session['generation_id'] == generation['generation_id'] and session['dataset_id'] == owner['dataset_id'] == dataset
    assert session['owner_role'] == generation['owner_role'] == 'index' and session['issuer_role'] == 'control'
    control = only(state['owner_epochs'], role='control', resource_id=session['issuer_resource'], epoch=session['issuer_epoch'])
    assert session['issuer_incarnation'] == control['process_incarnation'] == owner['process_incarnation']
    assert session['previous_epoch'] is session['recovery_digest'] is session['recovered_plan'] is session['recovered_pending_plan'] is None
    assert session['recovered_revision'] == 0
    for current in owners:
        role = only(state['role_instances'], role_instance_id=current['role_instance_id'])
        assert role['role'] == 'index' and role['dataset_id'] == dataset
        assert (role['retired_at_us'] is None) == (current['epoch'] == owner_count)
    consumer = only(state['consumers'], consumer_role='index')
    assert consumer['consumer_id'] == owner['resource_id'] and consumer['dataset_id'] == dataset
    plans = sorted(state['projection_plans'], key=lambda p:p['frontier_revision'])
    head = consumer['current_revision'] or 0
    assert type(head) is int and 0 <= head <= len(plans) <= min(4, head+1)
    assert consumer['current_topology_generation'] == (0 if head else None)
    if complete:
        assert head == len(plans) == 4
    if publication_fault is not None:
        assert (head,len(plans),complete) == ((2+int(publication_fault=='after-complete'),3,False)
                                             if owner_count==2 else (4,4,True))
    binding = tuple(bytes.fromhex(identity[n]) for n in ('dataset', 'resource', 'generation', 'storage_incarnation', 'table_identity_digest')) + (custody, 1)
    if owner_count > 1:
        assert complete or publication_fault is not None
        recovery_sessions(state, binding, owners, sessions, plans,publication_fault=publication_fault)
    assert len(state['projection_chunks']) == len(state['projection_plan_chunks']) == len(state['projection_chunk_bounds']) == 10*len(plans)
    assert len(state['index_frontier_plans']) == head
    revisions = [r for r in state['consumer_frontier_revisions'] if r['consumer_role'] == 'index']
    bounds = [r for r in state['consumer_frontiers'] if r['consumer_id'] == consumer['consumer_id']]
    assert len(revisions) == len(bounds) == head
    for number, plan in enumerate(plans, 1):
        first, last = RANGES[number-1]
        origins = (1,1,1,3) if publication_fault else (1,1,1,2)
        publishers = ((1,1,3,3) if publication_fault=='before-forward' else (1,1,2,3)) if publication_fault else (1,1,2,2)
        origin = owners[origins[number-1]-1] if owner_count > 1 else owner
        publisher = owners[publishers[number-1]-1] if owner_count > 1 and number <= head else owner
        for field, expected in dict(dataset_id=dataset, consumer_id=consumer['consumer_id'], generation_id=generation['generation_id'],
                frontier_revision=number, expected_revision=number-1, topology_generation=0, source_shard=0,
                source_owner_epoch=1, source_first=first, source_last=last, origin_epoch=origin['epoch'],
                origin_incarnation=origin['process_incarnation'], sealed=True).items():
            assert type(plan[field]) is type(expected) and plan[field] == expected, field
        members = [r for r in state['projection_plan_chunks'] if r['plan_digest'] == plan['plan_digest']]
        assert len(members) == 10 and {r['target'] for r in members} == set(TARGETS)
        chunks = []
        selected = []
        for target, count in zip(TARGETS, PLAN_COUNTS[number-1]):
            member = only(members, target=target)
            row = only(state['projection_chunks'], chunk_id=member['chunk_id'])
            assert member['generation_id'] == row['generation_id'] == generation['generation_id']
            assert member['partition_key'] == 'all' and member['rows_digest'] == member['chunk_id']
            for field, expected in dict(dataset_id=dataset, storage_incarnation=generation['storage_incarnation'],
                table_identity_digest=generation['table_identity_digest'], target=target, target_schema_version=1,
                topology_generation=0, chunk_ordinal=number, row_count=count, owner_resource_id=owner['resource_id'],
                owner_epoch=origin['epoch'], owner_role='index', owner_incarnation=origin['process_incarnation'], invalid_reason=None).items():
                assert type(row[field]) is type(expected) and row[field] == expected, field
            bound = only(state['projection_chunk_bounds'], chunk_id=member['chunk_id'])
            assert {k:v for k,v in bound.items() if k != 'xmin'} == dict(generation_id=generation['generation_id'],
                chunk_id=member['chunk_id'],dataset_id=dataset,topology_generation=0,shard_id=0,owner_epoch=1,last_complete_sequence=last)
            assert row['state'] == ('committed' if number <= head else 'prepared')
            chunks.append((count, raw(member['chunk_id'],32), raw(member['rows_digest'],32)))
            selected.append(row)
        digest = metadata_digest(binding, number-1, (0, [(0,1,first-1)]), (0, [(0,1,last)]),
            (raw(plan['source_batch_digest'],32),0,1,first,last), chunks)
        assert plan['plan_digest'] == '\\x'+digest.hex()
        if number <= head:
            published = only(state['index_frontier_plans'], frontier_revision=number)
            expected_engine = '\\x'+engine_digest(binding[4], digest, chunks).hex()
            assert published['plan_digest'] == plan['plan_digest'] and published['engine_receipt_digest'] == expected_engine
            assert published['consumer_id'] == consumer['consumer_id'] and published['generation_id'] == generation['generation_id']
            assert published['dataset_id'] == dataset and published['topology_generation'] == 0
            assert published['owner_epoch'] == publisher['epoch'] and published['owner_incarnation'] == publisher['process_incarnation']
            rev = only(revisions, frontier_revision=number)
            bound = only(bounds, frontier_revision=number)
            assert rev['sealed'] is True and rev['previous_revision'] == (number-1 if number > 1 else None)
            assert rev['consumer_id'] == rev['owner_resource_id'] == consumer['consumer_id']
            assert rev['dataset_id'] == bound['dataset_id'] == dataset and rev['topology_generation'] == bound['topology_generation'] == 0
            assert rev['owner_epoch'] == publisher['epoch'] and bound['owner_epoch'] == 1 and bound['shard_id'] == 0
            assert rev['owner_incarnation'] == publisher['process_incarnation'] and bound['last_complete_sequence'] == last
            assert bound['attestation_kind'] == 'projection-chunk' and bound['attestation_digest'] == plan['plan_digest']
            assert {r['engine_receipt_digest'] for r in selected} == {expected_engine}
            assert {r['committed_at_us'] for r in selected} == {published['committed_at_us']} == {rev['created_at_us']}
            assert {r['xmin'] for r in [published, rev, bound, *selected]} == {published['xmin']}
            if number == head:
                # Publication advances this row in the same transaction; lease
                # renewal touches owner_epochs, not the consumer head.
                assert consumer['xmin'] == published['xmin']
        else:
            assert all(r['engine_receipt_digest'] is None and r['committed_at_us'] is None for r in selected)
    return generation


def recovery_digest(binding, previous, plans, published, pending):
    """Independent fixed-width storage-services.md recovery commitment."""
    data = (b'bigbrotr/index-recovery-observation/1\0' + b''.join(binding[:6]) + bytes([binding[6]])
            + be(previous['epoch']) + raw(previous['process_incarnation'],16) + be(published))
    for number in (published, pending):
        data += b'\x01' + raw(plans[number-1]['plan_digest'],32) if number else b'\x00'
    for number in range(1, published+1+(pending is not None)):
        plan = plans[number-1]
        chunks = [(count, b'', raw(member['rows_digest'],32)) for count,member in zip(PLAN_COUNTS[number-1],
            [only(plan['members'], target=target) for target in TARGETS])]
        plan_digest = raw(plan['plan_digest'],32)
        data += bytes([int(number > published)]) + be(number-1) + plan_digest + engine_digest(binding[4],plan_digest,chunks)
    return hashlib.sha256(data).digest()


def recovery_sessions(state, binding, owners, sessions, plans, *, publication_fault=None):
    """Closed retained tuples, including the two actual publication fault branches."""
    attributed = [dict(p, members=[r for r in state['projection_plan_chunks'] if r['plan_digest']==p['plan_digest']]) for p in plans]
    for epoch, session in enumerate(sessions[1:], 2):
        previous, current = owners[epoch-2:epoch]
        published, pending = (2,3) if epoch == 2 else (4,None)
        if publication_fault is not None and epoch == 3:
            published,pending = (2,3) if publication_fault=='before-forward' else (3,None)
        expected = recovery_digest(binding, previous, attributed, published, pending)
        exact([session['previous_epoch'],session['recovered_revision']],[epoch-1,published])
        assert session['recovered_plan'] == plans[published-1]['plan_digest']
        assert session['recovered_pending_plan'] == (plans[2]['plan_digest'] if pending else None)
        assert session['recovery_digest'] == '\\x'+expected.hex()
        for key,value in dict(dataset_id=state['datasets'][0]['dataset_id'],resource_id=owners[0]['resource_id'],
                generation_id=state['projection_generations'][0]['generation_id'],owner_role='index',
                owner_incarnation=current['process_incarnation'],issuer_role='control').items():
            assert session[key] == value
        control = only(state['owner_epochs'],role='control',resource_id=session['issuer_resource'],epoch=session['issuer_epoch'])
        assert control['process_incarnation'] == session['issuer_incarnation'] == current['process_incarnation']
        assert current['resource_id'] == previous['resource_id'] and current['dataset_id'] == previous['dataset_id']
        assert current['index_publication_protocol'] == 1
        assert current['process_incarnation'] not in [r['process_incarnation'] for r in owners[:epoch-1]]
        assert previous['state'] == 'released' and previous['exclusion_method'] == 'storage_fenced'
        assert previous['exclusion_digest'] == session['recovery_digest'] and previous['released_at_us'] is not None
        role = only(state['role_instances'],role_instance_id=previous['role_instance_id'])
        assert role['retired_at_us'] is not None
        # The next rebind can update this owner/role, but the session and the
        # previous owner's terminal fence retain the original transfer xid.
        assert previous['xmin'] == role['xmin'] == session['xmin']
        if epoch == len(owners):
            assert current['state'] == 'active'
    assert len({r['role_instance_id'] for r in owners}) == len(owners)


def without_index_owner(state, root):
    validate_metadata(state, root)
    result = dict(state)
    removed = {r['role_instance_id'] for r in state['owner_epochs'] if r['role'] == 'index'}
    result['owner_epochs'] = [r for r in state['owner_epochs'] if r['role'] != 'index']
    result['role_instances'] = [r for r in state['role_instances'] if r['role_instance_id'] not in removed]
    return result


def digest(domain, value):
    from bootstrap_oracle import cbor
    return hashlib.sha256(b'bigbrotr/'+domain.encode()+b'/1\0'+cbor(value)).digest()


def expected_rows(science, generation, schema):
    from installed_sync_oracle import decode
    result = {name:[] for name in TARGETS}
    for batch in science['batches']:
        for entry in batch['facts']:
            preimage = bytes.fromhex(entry['preimage_hex'])
            assert preimage.startswith(b'bigbrotr/fact/1\0')
            source, family, version, visibility, recorded, payload = decode(preimage[16:])
            common = [generation,source,version,visibility,recorded,hashlib.sha256(preimage).digest(),0]
            def add(target, fields): result[target].append(common+fields)
            if family == 'dictionary':
                reference, normalization, name, version, fields = payload
                values = [fields.get('visibility_scope_id' if n == 'access_visibility_scope' else n) for n,_ in schema.DICTIONARY[5:]]
                add('dictionaries', [reference,normalization,name,version,digest('dictionary',payload),*values])
            elif family == 'event-payload':
                assert payload[4] == [], 'this frozen signed fixture has no tag postings'
                body = digest('body',payload[:6])
                add('events',payload+[body])
                for name in ('event_id_index','event_author_index','event_time_index'):
                    add(name,payload[:4]+[body])
            elif family == 'event-eligibility': add('event_eligibility_facts',payload)
            elif family == 'event-receive':
                event,at,observer,quality,corrects = payload
                add('event_receive_facts',[event,at,observer,*quality,corrects])
            else:
                assert family == 'provenance'
                event,endpoint,observer,access,at,quality,verification,corrects = payload
                for name in ('provenance_by_event','provenance_by_relay'):
                    add(name,[event,endpoint,observer,access,at,*quality,verification,corrects])
    assert tuple(map(len,result.values())) == COUNTS
    return result


def parts(value):
    depth,start,out = 0,0,[]
    for n,c in enumerate(value):
        if c == '(': depth += 1
        elif c == ')': depth -= 1
        elif c == ',' and depth == 0: out.append(value[start:n].strip()); start=n+1
    return out+[value[start:].strip()]


def expression(ty, value):
    if ty.startswith('Nullable('): return expression(ty[9:-1],value)
    if ty.startswith('FixedString('): return f'lower(hex({value}))'
    if ty.startswith('Tuple('):
        return 'tuple('+','.join(expression(t,f'tupleElement({value},{i})') for i,t in enumerate(parts(ty[6:-1]),1))+')'
    if ty.startswith('Array('): return f'arrayMap(v -> {expression(ty[6:-1], "v")}, {value})'
    assert ty in ('String','UInt16','UInt32','UInt64','Int64')
    return value


def select_rows(schema, database, target):
    assert database == 'serving_fixture' and target in TARGETS
    columns, = [columns for name,columns,_ in schema.FAMILIES if name == target]
    fields = ','.join(expression(ty,'`'+name+'`')+' AS `'+name+'`' for name,ty in schema.COMMON+columns)
    return f'SELECT {fields} FROM {database}.{target} ORDER BY source,discriminator'


def json_value(ty, value):
    if value is None: return [] if ty.startswith('Array(Tuple(') else None
    if ty.startswith('Nullable('): return json_value(ty[9:-1],value)
    if ty.startswith('FixedString('): return value.hex()
    if ty.startswith('Tuple('): return [json_value(t,v) for t,v in zip(parts(ty[6:-1]),value)]
    if ty.startswith('Array(Tuple('): return [json_value(ty[6:-1],value)]
    if ty.startswith('Array('): return [json_value(ty[6:-1],v) for v in value]
    return value


def physical_rows(actual, science, generation, schema, *, through=12, replayed_pending=False):
    """Raw physical multiplicity, including the declared immutable plan3 replay."""
    assert through in (7,12) and type(replayed_pending) is bool
    assert through == 12 or not replayed_pending
    rows = expected_rows(science,raw(generation,16),schema)
    assert set(actual) == set(TARGETS)
    for name,columns,_ in schema.FAMILIES:
        physical = [row for row in rows[name] if row[1][3] <= through for _ in range(
            2 if replayed_pending and 4 <= row[1][3] <= 7 else 1)]
        expected = [{key:json_value(ty,value) for (key,ty),value in zip(schema.COMMON+columns,row)} for row in physical]
        exact(actual[name],expected)
    return rows


def publication(live, after, actual, science, schema, root, *, owner_count=1, publication_fault=None):
    generation = validate_metadata(live, root, complete=True, owner_count=owner_count,publication_fault=publication_fault)
    validate_metadata(after, root, complete=True, owner_count=owner_count,publication_fault=publication_fault)
    for table in INDEX_TABLES:
        exact(live[table],after[table])
    identity,tables,_ = retained(root)
    h = hashlib.sha256(b'bigbrotr/index-table-manifest/1\0')
    def text(value): data=value.encode(); h.update(be(len(data),4)+data)
    for value in (schema.VERSION,tables['database'],'Atomic'): text(value)
    for t,identifier in zip(schema.inventory(),tables['table_uuids']):
        text(t['name']); h.update(uuid.UUID(identifier).bytes); text(t['engine_full']); h.update(be(len(t['columns']),2))
        for name,ty in t['columns']: text(name); text(ty)
    assert h.hexdigest() == identity['table_identity_digest']
    rows = physical_rows(actual,science,generation['generation_id'],schema,
                         replayed_pending=publication_fault=='before-forward')
    plans = sorted(live['projection_plans'], key=lambda r:r['frontier_revision'])
    for plan,batch in zip(plans,science['batches']):
        assert plan['source_batch_digest'] == '\\x'+batch['admission_digest']
        cut = [raw(generation['dataset_id'],16),0,[[0,1,batch['last']]]]
        for name in TARGETS:
            selected = [r for r in rows[name] if batch['first'] <= r[1][3] <= batch['last']]
            values = [[row[1],[name,row[6]],row] for row in selected]
            expected = digest('projection-chunk',[raw(generation['generation_id'],16),name,1,cut,0,values])
            member = only(live['projection_plan_chunks'],plan_digest=plan['plan_digest'],target=name)
            assert member['chunk_id'] == member['rows_digest'] == '\\x'+expected.hex()
    return dict(format='bigbrotr-installed-initial-index/1', source_cut=12, frontier_revision=4,
        admission_batches=4, committed_chunks=40, family_counts=dict(zip(TARGETS,COUNTS)),
        live_observation=True, independent_source_and_c_api=True, all_typed_fields_equal=True,
        zero_tag_outcome=True, batch_max_rows=BATCH_MAX_ROWS, metadata_rows=160,
        generation_id=generation['generation_id'][2:],table_identity_sha256=identity['table_identity_digest'],
        readiness='NOT_RUN', restart='NOT_RUN',column_ttl_negative='NOT_RUN')


def schema_metadata(schema,root,tables,columns):
    _,retained_tables,_ = retained(root)
    expected = schema.inventory()
    for row,ident in zip(expected,retained_tables['table_uuids']): row['uuid']=ident
    exact(tables,[dict(name=r['name'],uuid=r['uuid'],engine_full=r['engine_full']) for r in sorted(expected,key=lambda r:r['name'])])
    exact(columns,[dict(table=r['name'],name=name,type=ty,position=i+1,default_kind='',default_expression='')
        for r in sorted(expected,key=lambda r:r['name']) for i,(name,ty) in enumerate(r['columns'])])
