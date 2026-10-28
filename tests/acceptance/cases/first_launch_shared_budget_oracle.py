"""One actual pre-effect capacity wait, followed by the unchanged signed journey.

The diagnostic is an operator observation. Raw SQL and original source/science
oracles establish the separate ownership, pending-work and publication facts.
"""
import re

from cases import first_launch_index_oracle as index

FORMAT = 'bigbrotr-initial-capacity/1'
UNITS = {'data_bytes', 'items', 'native_bytes', 'control_bytes'}
IDENTITY = ('dataset_id', 'control_resource_id', 'control_epoch',
            'control_incarnation', 'control_role_instance_id')
COMMON = {'format', 'scope', *IDENTITY, 'transition', 'record_limit'}
FIELDS = COMMON | {'wait_id', 'role', 'timing', 'wait_started_us', 'observed_us',
    'wait_elapsed_us', 'budget_domain', 'budget_observation', 'requested',
    'charged', 'limits', 'retained_index_at_denial', 'retained_index_now',
    'local_joined_consumed_through', 'denied_index_locally_joined_and_consumed',
    'remote_query_unknown'}
THROUGH = (0, 2, 3, 7, 12)


def number(value):
    assert isinstance(value, str) and re.fullmatch(r'0|[1-9][0-9]*', value)
    result = int(value)
    assert result <= (1 << 64) - 1
    return result


def held(value):
    assert set(value) == {'incarnation', 'sequence', 'kind', 'state'}
    assert re.fullmatch('[0-9a-f]{32}', value['incarnation']) and int(value['incarnation'], 16)
    assert number(value['sequence']) > 0
    assert value['kind'] in ('publication', 'readiness-plan', 'readiness-finish')
    assert value['state'] == 'actual-retained-operation-owner'


def observation(value):
    assert set(value) == FIELDS and value['format'] == FORMAT
    assert value['scope'] == 'operator-observation-only' and value['record_limit'] == 128
    assert value['transition'] in ('deferred', 'admitted') and value['role'] in ('sync', 'archive')
    assert value['budget_domain'] in ('application', 'native')
    assert value['timing'] == 'monotonic-microseconds-from-capacity-observer-start'
    assert number(value['control_epoch']) > 0 and number(value['wait_id']) > 0
    for key in set(IDENTITY) - {'control_epoch'}:
        assert re.fullmatch('[0-9a-f]{32}', value[key]) and int(value[key], 16)
    for key in ('wait_started_us', 'observed_us', 'wait_elapsed_us'):
        assert type(value[key]) is int and 0 <= value[key] < 1 << 64
    # The three durations are independently truncated to microseconds.
    assert 0 <= value['observed_us'] - value['wait_started_us'] - value['wait_elapsed_us'] <= 1
    for name in ('requested', 'charged', 'limits'):
        assert set(value[name]) == UNITS
        assert all(type(n) is int and 0 <= n < 1 << 64 for n in value[name].values())
    assert any(value['requested'][key] for key in ('data_bytes', 'items', 'native_bytes'))
    assert all(value['requested'][key] <= value['limits'][key] for key in UNITS)
    number(value['local_joined_consumed_through'])
    assert type(value['denied_index_locally_joined_and_consumed']) is bool
    assert type(value['remote_query_unknown']) is bool


def denial(value, prefix):
    observation(value)
    assert value['transition'] == 'deferred' and value['wait_elapsed_us'] == 0
    assert value['budget_domain'] == 'application', 'selected proof must concern shared application credit'
    assert value['budget_observation'] == 'after-atomic-refusal-not-the-failing-snapshot'
    assert not value['denied_index_locally_joined_and_consumed'] and not value['remote_query_unknown']
    held(value['retained_index_at_denial'])
    index.exact(value['retained_index_now'], value['retained_index_at_denial'])
    assert value['retained_index_now']['incarnation'] == value['control_incarnation']
    assert number(value['local_joined_consumed_through']) < number(value['retained_index_now']['sequence'])
    owner = index.only(prefix['owner_epochs'], role='control', state='active')
    index.exact([value[key] for key in IDENTITY], [owner['dataset_id'][2:],
        owner['resource_id'][2:], str(owner['epoch']), owner['process_incarnation'][2:],
        owner['role_instance_id'][2:]])
    # The snapshot is explicitly post-refusal. It is not used to reconstruct an
    # atomic deficit or to assert that Index was the only charged owner.


def admitted(before, after):
    observation(after)
    assert after['transition'] == 'admitted'
    for key in (*IDENTITY, 'wait_id', 'role', 'wait_started_us', 'budget_domain',
                'requested', 'limits', 'retained_index_at_denial'):
        index.exact(after[key], before[key])
    # The fixture requires the positive post-reservation observation itself
    # inside the30s wait, although the product's check precedes reservation.
    assert after['observed_us'] > before['observed_us'] and 0 < after['wait_elapsed_us'] < 30_000_000
    assert after['budget_observation'] == 'after-whole-operation-reservation'
    assert after['denied_index_locally_joined_and_consumed'] and not after['remote_query_unknown']
    assert after['retained_index_now'] is None
    assert number(after['local_joined_consumed_through']) >= number(before['retained_index_at_denial']['sequence'])
    assert all(after['requested'][key] <= after['charged'][key] <= after['limits'][key] for key in UNITS)


# Literal mutation surface of this one four-batch, one-job initial fixture.
# Every other current-schema table and every old immutable row remains exact.
APPEND = {'acquisition_selectors', 'acquisition_scopes', 'acquisition_scope_commands',
    'jobs', 'acquisition_jobs', 'job_admissions', 'management_commands', 'management_audit',
    'producer_requests', 'producer_outcomes', 'producer_checkpoint_dependencies',
    'acquisition_progress', 'archive_attempts', 'archive_frontier_snapshots', 'archive_frontier_batches',
    'projection_plans', 'projection_plan_chunks', 'projection_chunks', 'projection_chunk_bounds',
    'index_frontier_plans', 'consumer_frontier_revisions', 'consumer_frontiers'}
MUTABLE = {'owner_epochs', 'producer_epochs', 'producer_checkpoints', 'consumers'}


def same_owner(prefix, current):
    assert len(prefix['owner_epochs']) == len(current['owner_epochs'])
    for old in prefix['owner_epochs']:
        new = index.only(current['owner_epochs'], resource_id=old['resource_id'], epoch=old['epoch'])
        changed = {'lease_expires_at_us', 'xmin'} if old['state'] == 'active' else set()
        index.exact({k:v for k,v in old.items() if k not in changed},
                    {k:v for k,v in new.items() if k not in changed})
        if changed:
            assert new['lease_expires_at_us'] >= old['lease_expires_at_us']


def attempt_owners(prefix, state):
    owner = index.only(state['owner_epochs'], role='control', state='active')
    archive = index.only(state['consumers'], consumer_role='archive')
    attempts = sorted(state['archive_attempts'], key=lambda row:row['ordinal'])
    assert [row['ordinal'] for row in attempts] == list(range(1, archive['current_revision']+1))
    assert len({row['attempt_id'] for row in attempts}) == len({row['worker_incarnation'] for row in attempts}) == len(attempts)
    for row in attempts:
        for key, value in dict(dataset_id=owner['dataset_id'],catalog_id=archive['consumer_id'],
            expected_predecessor=row['ordinal']-1, schema_version=1,topology_generation=0,persistence_protocol=1,
            issuer_role='control',issuer_resource=owner['resource_id'],issuer_epoch=owner['epoch'],
            issuer_incarnation=owner['process_incarnation'],coordinator_incarnation=owner['process_incarnation'],
            coordinator_binding_sha256=prefix['archive_attempts'][0]['coordinator_binding_sha256']).items():
            index.exact(row[key],value)
        assert owner['acquired_at_us'] <= row['issued_at_us'] < owner['lease_expires_at_us']
        assert any(index.raw(row['attempt_id'],16)) and any(index.raw(row['worker_incarnation'],16))
        assert row['worker_incarnation'] != row['coordinator_incarnation']


def pending(prefix, state, record, job_id):
    """Observe actual queued work; no fixed denial role or intermediate cut."""
    denial(record, prefix)
    assert set(state) == set(prefix) and len(state) == 82
    for table in set(prefix) - APPEND - MUTABLE:
        index.exact(state[table], prefix[table])
    for table in APPEND:
        assert all(row in state[table] for row in prefix[table]), 'old row changed: ' + table
    same_owner(prefix, state)
    job = index.only(state['jobs'], job_id='\\x'+job_id)
    assert len(state['jobs']) == len(state['acquisition_jobs']) == 1
    assert job['kind'] == 'acquisition'
    assert (job['state'], job['job_revision']) in (('accepted', 1), ('queued', 2))
    assert job['checkpoint_revision'] == 0 and job['completed_at_us'] is None
    detail = index.only(state['acquisition_jobs'], job_id=job['job_id'])
    assert detail['actor_id'] == job['principal_id']
    index.only(state['acquisition_scopes'], scope_id=detail['scope_id'])
    owner = index.only(state['owner_epochs'], role='control', state='active')
    assert job['owner_role'] == 'control'
    if job['state'] == 'queued':
        assert job['owner_epoch'] == owner['epoch'] and job['owner_incarnation'] == owner['process_incarnation']
        assert job['owner_resource_id'] == owner['resource_id']
    else:
        assert all(job[k] is None for k in ('owner_epoch', 'owner_incarnation', 'owner_resource_id'))
        assert not state['acquisition_progress']
    requests = sorted(state['producer_requests'], key=lambda r:r['request_sequence'])
    outcomes = sorted(state['producer_outcomes'], key=lambda r:r['request_sequence'])
    count = len(outcomes)
    assert 2 <= count <= 4 and len(requests) == count
    if job['state'] == 'accepted':
        assert count == 2
    index.exact([r['request_sequence'] for r in requests], list(range(1, count+1)))
    index.exact([r['request_sequence'] for r in outcomes], list(range(1, count+1)))
    for request, outcome in zip(requests, outcomes):
        assert request['request_digest'] == outcome['request_digest']
        assert request['producer_epoch'] == outcome['producer_epoch'] == 1
    producer = index.only(state['producer_epochs'])
    assert producer['state'] == 'open' and producer['reserved_through_request'] == count
    old = index.only(prefix['producer_epochs'])
    fields = {'reserved_through_request', 'xmin'}
    index.exact({k:v for k,v in old.items() if k not in fields}, {k:v for k,v in producer.items() if k not in fields})
    assert len(state['consumers']) == len(prefix['consumers']) == 2
    checkpoint = index.only(state['producer_checkpoints'])
    prior = index.only(prefix['producer_checkpoints'])
    for key in ('dataset_id', 'producer_slot_id', 'producer_epoch', 'progress_format',
                'manifest_sha256', 'issuer_resource', 'issuer_epoch', 'issuer_incarnation'):
        index.exact(checkpoint[key], prior[key])
    assert checkpoint['complete_through_request'] == count
    assert 2 <= checkpoint['checkpoint_revision'] <= 9
    for consumer in state['consumers']:
        prior = index.only(prefix['consumers'], consumer_id=consumer['consumer_id'])
        fields = {'current_revision', 'xmin'}
        index.exact({k:v for k,v in prior.items() if k not in fields}, {k:v for k,v in consumer.items() if k not in fields})
        assert 2 <= consumer['current_revision'] <= count
        bound = index.only(state['consumer_frontiers'], consumer_id=consumer['consumer_id'], frontier_revision=consumer['current_revision'])
        assert bound['last_complete_sequence'] == THROUGH[consumer['current_revision']]
    archive = index.only(state['consumers'], consumer_role='archive')
    attempt_owners(prefix,state)
    if record['role'] == 'archive':
        assert archive['current_revision'] < count, 'Archive denial has no complete pending batch'
    plans = sorted(state['projection_plans'], key=lambda r:r['frontier_revision'])
    consumer = index.only(state['consumers'], consumer_role='index')
    assert len(plans) in (consumer['current_revision'], consumer['current_revision']+1)
    for row in plans:
        assert row['sealed'] and row['frontier_revision'] <= count
    if record['retained_index_now']['kind'] == 'publication':
        plan = index.only(plans, frontier_revision=consumer['current_revision']+1)
        assert (plan['source_first'], plan['source_last']) == (THROUGH[consumer['current_revision']]+1, THROUGH[consumer['current_revision']+1])
        assert len([r for r in state['projection_chunks'] if r['state'] == 'prepared']) == 10
    a, i, p = archive['current_revision'], consumer['current_revision'], len(plans)
    counts = dict(acquisition_selectors=1, acquisition_scopes=1, acquisition_scope_commands=1,
        jobs=1, acquisition_jobs=1, job_admissions=1, producer_requests=count, producer_outcomes=count,
        archive_attempts=a, archive_frontier_snapshots=a, archive_frontier_batches=a*(a+1)//2,
        projection_plans=p, projection_plan_chunks=10*p, projection_chunks=10*p,
        projection_chunk_bounds=10*p, index_frontier_plans=i,
        consumer_frontier_revisions=a+i, consumer_frontiers=a+i)
    for table, expected in counts.items():
        assert len(state[table]) == expected, 'unexpected pending inventory: '+table
    for table in ('management_commands', 'management_audit'):
        assert len(state[table]) == len(prefix[table])+2
    assert len(prefix['producer_checkpoint_dependencies']) <= len(state['producer_checkpoint_dependencies']) <= THROUGH[count]
    assert len(state['acquisition_progress']) in (0,1)
    return dict(role=record['role'], job_state=job['state'], source_through=THROUGH[count], source_batches=count,
                archive_revision=archive['current_revision'], index_revision=consumer['current_revision'])


def preserved(denied, final):
    """The final science oracle independently validates all added rows/bytes."""
    assert set(denied) == set(final) and len(final) == 82
    for table in set(denied) - APPEND - MUTABLE:
        index.exact(denied[table], final[table])
    for table in APPEND - {'jobs', 'projection_chunks', 'acquisition_progress'}:
        assert all(row in final[table] for row in denied[table]), 'accepted row changed: ' + table
    for old in denied['projection_chunks']:
        new = index.only(final['projection_chunks'], chunk_id=old['chunk_id'])
        fields = {'state', 'committed_at_us', 'engine_receipt_digest', 'xmin'} if old['state'] == 'prepared' else set()
        index.exact({k:v for k,v in old.items() if k not in fields}, {k:v for k,v in new.items() if k not in fields})
        assert new['state'] == 'committed'
    same_owner(denied, final)
    attempt_owners(denied,final)
    for table, allowed in (
        ('producer_epochs', {'reserved_through_request', 'xmin'}),
        ('producer_checkpoints', {'checkpoint_digest', 'checkpoint_revision', 'committed_at_us',
            'complete_through_request', 'dependency_count', 'progress_jobs', 'xmin'}),
    ):
        old = index.only(denied[table]); new = index.only(final[table])
        index.exact({k:v for k,v in old.items() if k not in allowed}, {k:v for k,v in new.items() if k not in allowed})
    for old in denied['consumers']:
        new = index.only(final['consumers'], consumer_id=old['consumer_id'])
        allowed = {'current_revision', 'xmin'}
        index.exact({k:v for k,v in old.items() if k not in allowed}, {k:v for k,v in new.items() if k not in allowed})
        assert old['current_revision'] <= new['current_revision'] <= 4
    assert len(final['consumers']) == len(denied['consumers']) == 2
    old = index.only(denied['jobs']); new = index.only(final['jobs'])
    allowed = {'state', 'job_revision', 'owner_resource_id', 'owner_epoch', 'owner_incarnation', 'xmin'} if old['state'] == 'accepted' else set()
    index.exact({k:v for k,v in old.items() if k not in allowed}, {k:v for k,v in new.items() if k not in allowed})
    if allowed:
        assert new['xmin'] != old['xmin']
    assert new['state'] == 'queued' and new['job_revision'] == 2
    owner = index.only(final['owner_epochs'], role='control', state='active')
    assert [new[k] for k in ('owner_resource_id', 'owner_epoch', 'owner_incarnation')] == [owner[k] for k in ('resource_id', 'epoch', 'process_incarnation')]
