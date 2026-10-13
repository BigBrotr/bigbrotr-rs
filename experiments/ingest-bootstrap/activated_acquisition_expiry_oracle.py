"""Exact current-Control expiry after an observed final job-lock wait.

Frozen source2 science is independently pinned by the earlier source oracle.
Only a fresh Control claim and four real refreshes may change PostgreSQL here.
"""
import copy
import json

from activated_acquisition_oracle import seed_checked, migrated
from activated_recovery_faults_oracle import bounded_number
from initial_live_oracle import Capture, clock, encoded, digest
from initial_live_routes_oracle import backend_checked
from initial_succession_oracle import claim, renewed, same, only
from installed_empty_oracle import native as native_map, fresh_xmin, window
from installed_live_oracle import instant

MODE = 'successor-activated-acquisition-expiry'
CLAIMED_JOBS = """
    SELECT substring(j.job_id FROM 1 FOR 17),
           coalesce(j.acquisition_job_protocol=1 AND j.state='queued'
               AND j.checkpoint_revision=0 AND j.input_complete
               AND NOT EXISTS (SELECT 1 FROM control.acquisition_gaps g WHERE g.job_id=j.job_id), false)
      FROM control.jobs j
     WHERE j.dataset_id=$1::bytea AND j.owner_resource_id=$2::bytea
       AND j.owner_epoch=$3::text::numeric AND j.owner_incarnation=$4::bytea
       AND j.kind='acquisition'
     ORDER BY j.job_id LIMIT 65 FOR UPDATE"""


def checked_clock(value, stage, owner, envelope):
    at = value['server_us']
    clock(at, envelope)
    assert value == dict(event='activated-expiry-clock', stage=stage, server_us=at,
        lease_expires_at_us=owner['lease_expires_at_us'],
        remaining_us=owner['lease_expires_at_us'] - at, error=None)
    return at


def schedule_checked(schedule, expected, current, physical, refreshed, dispatch, returned, job, envelope):
    assert schedule['status'] == 'CAPTURED' and schedule['cleanup_errors'] == []
    assert 'schedule_error' not in schedule
    assert schedule['control'] == current and schedule['control_backend'] == physical
    same(schedule['before_holder'], expected)
    same(schedule['held'], expected)
    assert schedule['dispatch'] == dispatch
    gates = schedule['gates']
    assert len(gates) == 2
    for value, phase in zip(gates, ('waiting', 'released'), strict=True):
        assert value == dict(gate='before-abandonment', phase=phase, server_us=value['server_us'])
        clock(value['server_us'], envelope)
    assert refreshed['server_us'] <= gates[0]['server_us'] <= gates[1]['server_us'] <= dispatch['server_us']
    holder = schedule['holder']
    backend = holder['backend']
    assert backend['database'] == physical['database']
    assert type(backend['pid']) is int and backend['pid'] > 0
    assert (backend['pid'], instant(backend['backend_start'])) != (physical['pid'], instant(physical['backend_start']))
    assert envelope['before_us'] <= instant(backend['backend_start']) <= gates[0]['server_us']
    assert backend['user'] == 'bigbrotr_control' and backend['application'] == MODE + '_holder'
    assert backend['isolation'] == 'read committed' and backend['synchronous_commit'] == 'on'
    assert holder['identity'] == dict(pid=backend['pid'], database=backend['database'],
        user='bigbrotr_control', role='bigbrotr_control', superuser=False,
        session_replication_role='origin', isolation='read committed')
    settings = holder['settings']
    assert settings['sqlstate'] == '00000' and settings['rows'] == 1
    assert [json.loads(line) for line in settings['lines']] == [dict(
        statement_timeout='30s', lock_timeout='20s', idle_timeout='1min')]
    assert holder['begin'] == holder['rollback'] == dict(sqlstate='00000', rows=0, lines=[])
    statement = ("SELECT to_jsonb(j)||jsonb_build_object('xmin',xmin::text) "
        "FROM control.jobs j WHERE job_id=decode('" + job['job_id'][2:] + "','hex') FOR UPDATE;")
    assert holder['statement'] == statement
    assert holder['lock']['sqlstate'] == '00000' and holder['lock']['rows'] == 1
    assert [json.loads(line) for line in holder['lock']['lines']] == [job]

    def blocked(samples, timeout):
        assert samples
        last_seconds = -1
        for sample in samples:
            bounded_number(sample['sample_seconds'], timeout)
            assert sample['sample_seconds'] >= last_seconds
            last_seconds = sample['sample_seconds']
            clock(sample['observed_at_us'], envelope)
            for key, identity in (('waiter', physical), ('holder', backend)):
                observed = sample[key]
                assert observed['pid'] == identity['pid'] and observed['database'] == identity['database']
                assert instant(observed['backend_start']) == instant(identity['backend_start'])
                assert observed['user'] == 'bigbrotr_control'
            assert sample['holder']['application'] == MODE + '_holder'
            assert sample['holder']['query'] == statement and sample['holder']['state'] == 'idle in transaction'
            assert sample['holder']['backend_xid'].isdecimal()
        value = samples[-1]
        waiter = value['waiter']
        assert waiter['state'] == 'active' and waiter['wait_event_type'] == 'Lock'
        assert backend['pid'] in waiter['blockers']
        assert ' '.join(waiter['query'].split()) == ' '.join(CLAIMED_JOBS.split())
        locks = value['waiting_locks']
        assert locks and any(r['type'] == 'transactionid' and r['mode'] == 'ShareLock'
            and r['transaction'] == value['holder']['backend_xid'] and r['granted'] is False for r in locks)
        return value['observed_at_us']

    first = blocked(schedule['blocks'], 2)
    expired = blocked(schedule['expired_blocks'], 1)
    deadline = current['lease_expires_at_us']
    assert dispatch['server_us'] <= first < deadline
    observations = schedule['expiry_observations']
    assert observations
    previous = first
    for observation in observations:
        at = observation['server_us']
        clock(at, envelope)
        assert observation == dict(server_us=at, remaining_us=deadline - at)
        assert at >= previous
        previous = at
    assert deadline + 50_000 <= previous <= expired
    release_before, release_after = schedule['release_before_us'], schedule['release_after_us']
    clock(release_before, envelope)
    clock(release_after, envelope)
    assert expired <= release_before <= release_after
    # These two observations race after the same ROLLBACK: the probe can report
    # its return before the holder's independent post-rollback clock query.
    assert release_before <= returned['server_us']
    # Actual job wait must cross UTC expiry, not hit either 5s lock_timeout or
    # the 10s product operation deadline. Both clocks and returned taxonomy bind it.
    assert release_after - dispatch['server_us'] < 5_000_000
    assert returned['server_us'] - dispatch['server_us'] < 5_000_000
    closed = holder['closed']
    assert closed['backend'] == backend and type(closed['wrapper_exit']) is int
    assert closed['statement'] == ("SELECT count(*) FROM pg_stat_activity WHERE pid=" + str(backend['pid'])
        + " AND backend_start='" + backend['backend_start'] + "'::timestamptz"
        + " AND datname='" + backend['database'] + "';")
    assert closed['observation'] == '0'


def verify(events, original, native, inputs, response, *, seed, setup, server_window,
        backend, schedule, after, native_after, inputs_after, input_contents):
    window(server_window)
    assert seed['name'] == 'off'
    science = seed_checked(seed)
    same(original, migrated(seed, setup, server_window))
    assert native == native_after == seed['native']
    assert inputs == inputs_after == seed['inputs'] and input_contents == seed['contents']
    assert response['outcome'] == 'PROCESS_EXITED' and response['returncode'] == 0 and response['stderr'] == ''
    assert 'custody_error' not in response
    assert response['readers_joined'] is True and response['overflow'] is False
    bounded_number(response['seconds'], 120)
    assert [json.loads(line) for line in response['stdout'].splitlines()] == events
    cap = Capture(events, original, native_map(native), inputs)
    cap.sequence = 85
    assert cap.take('successor-control', 'expiry', 'claim-planned') == dict(event='successor-control',
        stage='expiry', phase='claim-planned', role='d5' * 16, incarnation='df' * 16, lease_ttl_us='20000000')
    same(original, cap.snapshot('expiry-before-claim'))
    claimed = cap.take('successor-control', 'expiry', 'claimed')
    current = cap.snapshot('expiry-after-claim')
    owner = claim(original, current, 3)
    assert owner['epoch'] == 11
    assert claimed == dict(event='successor-control', stage='expiry', phase='claimed',
        resource=owner['resource_id'][2:], epoch=11, role='d5' * 16, incarnation='df' * 16)
    role = only(current['role_instances'], role_instance_id=owner['role_instance_id'])
    for value in (owner['acquired_at_us'], role['created_at_us'], instant(owner['control_backend_start'])):
        clock(value, server_window)
    physical = cap.take('successor-backend', 'expiry')
    assert physical == dict(event='successor-backend', stage='expiry', **backend['exact']['expected'], present=True)
    same(current, cap.snapshot('expiry-loaded'))
    same(current, cap.snapshot('expiry-producer-closed'))

    def refresh(stage):
        nonlocal current, owner
        following = cap.snapshot(stage)
        updated = renewed(current, following, owner, server_window['after_us'])
        fresh_xmin(updated['xmin'], current)
        assert updated['lease_expires_at_us'] >= owner['lease_expires_at_us']
        clock(updated['lease_expires_at_us'] - 20_000_000, server_window)
        current, owner = following, updated

    for sequence in (1, 2):
        refresh('expiry-refreshed-' + str(sequence))
        outcome = only(seed['control']['producer_outcomes'], producer_epoch=2, request_sequence=sequence)
        assert cap.take('activated-expiry-request') == dict(event='activated-expiry-request', sequence=sequence,
            outcome='durable', receipt=outcome['outcome_bytes'][2:], rejection=None)
        same(current, cap.snapshot('expiry-resolved-' + str(sequence)))
    assert cap.take('activated-expiry-proof') == dict(event='activated-expiry-proof', resolved_requests=2,
        last_complete_sequence=5, sha256=science['digest'])
    refresh('expiry-source-refreshed')
    same(current, cap.snapshot('expiry-source-closed'))
    refresh('expiry-final-refreshed')
    refreshed = cap.take('activated-expiry-clock', 'refreshed')
    at = checked_clock(refreshed, 'refreshed', owner, server_window)
    assert owner['lease_expires_at_us'] - 20_000_000 <= at < owner['lease_expires_at_us']
    for phase in ('waiting', 'released'):
        assert cap.take('activated-expiry-gate', phase=phase) == dict(event='activated-expiry-gate',
            gate='before-abandonment', phase=phase)
    dispatch = cap.take('activated-expiry-clock', 'dispatch')
    checked_clock(dispatch, 'dispatch', owner, server_window)
    assert 1_000_000 <= dispatch['remaining_us'] <= 2_000_000
    assert cap.take('activated-expiry-result', 'abandonment') == dict(event='activated-expiry-result',
        stage='abandonment', sequence=None, jobs=None, error='Control(Unavailable)')
    returned = cap.take('activated-expiry-clock', 'returned')
    checked_clock(returned, 'returned', owner, server_window)
    assert returned['remaining_us'] <= 0
    same(current, cap.snapshot('expiry-after-abandonment'))
    assert cap.take('activated-expiry-reuse') == dict(event='activated-expiry-reuse', available=False, error='Unavailable')
    same(current, cap.snapshot('expiry-after-reuse'))
    same(current, cap.snapshot('expiry-final-before-control-end'))
    assert cap.take('activated-expiry-driver', 'control') == dict(event='activated-expiry-driver',
        stage='control', joined=True, failed=False, aborted=False, error=None)
    assert cap.take('successor-control', 'expiry', 'dropped-and-joined') == dict(event='successor-control',
        stage='expiry', phase='dropped-and-joined', resource=owner['resource_id'][2:], epoch=11)
    assert cap.take('activated-expiry-backend-absence') == dict(event='activated-expiry-backend-absence', error=None)
    assert cap.take('successor-backend', 'expiry') == dict(physical, present=False)
    same(current, cap.snapshot('expiry-final-after-control-end'))
    same(current, cap.snapshot('expiry-final'))
    assert cap.take('activated-expiry-driver', 'observer') == dict(event='activated-expiry-driver',
        stage='observer', joined=True, failed=False, aborted=False, error=None)
    assert cap.take('activated-expiry-complete') == dict(event='activated-expiry-complete', command=MODE,
        native='closed', control='dropped-and-joined', admission=False, expected_refusal=True)
    assert cap.position == len(events)
    same(current, after)
    backend_checked(backend, owner)
    bounded_number(backend['absence']['seconds'], 120)
    schedule_checked(schedule, current, owner, physical, refreshed, dispatch, returned, science['job'], server_window)
    return dict(status='PASS', snapshots=cap.snapshots, control_tables=72, native_rows=70, native_sequence=85,
        original_inputs=6, resolved_requests=2, source_cut=5, jobs_cancelled=0, gaps_added=0,
        expired_control_epoch=11, actual_blocked_query='CLAIMED_JOBS', refusal='Control(Unavailable)')


def negative_controls(**context):
    verify(**context)
    rejected = []

    def check(name, mutate):
        altered = copy.deepcopy(context)
        mutate(altered)
        altered['response']['stdout'] = ''.join(json.dumps(e) + '\n' for e in altered['events'])
        try:
            verify(**altered)
        except (AssertionError, KeyError, ValueError, TypeError, IndexError, StopIteration):
            rejected.append(name)
            return
        raise AssertionError('accepted expiry perturbation: ' + name)

    def sql(value, mutate):
        event = only(value['events'], event='successor-state', stage='expiry-after-abandonment')
        rows = {k:json.loads(v) for k, v in event['state']['control_tables']}
        mutate(rows)
        event['state']['control_tables'] = [[k, encoded(rows[k]).decode()] for k in sorted(rows)]
        event['state']['control_sha256'] = digest(encoded(event['state']['control_tables']))

    check('partial-job-cancel', lambda c: sql(c, lambda rows: only(rows['jobs'],
        job_id=seed_checked(c['seed'])['job']['job_id']).update(state='cancelled', job_revision=4)))
    check('prior-gap-rewritten', lambda c: sql(c, lambda rows: only(rows['acquisition_gaps'], producer_epoch=1).update(xmin='999999')))
    check('outcome-copy-reissued', lambda c: sql(c, lambda rows: only(rows['producer_outcomes'], producer_epoch=2, request_sequence=2).update(recorder_epoch=11)))
    check('native-write', lambda c: c['native_after'].update(sequence=86))
    check('lost-input', lambda c: c['input_contents'].pop(next(iter(c['input_contents']))))
    check('no-real-wait', lambda c: c['schedule']['blocks'][-1]['waiter'].update(blockers=[]))
    check('wrong-waited-query', lambda c: c['schedule']['blocks'][-1]['waiter'].update(query='SELECT 1'))
    check('wrong-holder-incarnation', lambda c: c['schedule']['expired_blocks'][-1]['holder'].update(backend_start='2000-01-01T00:00:00+00:00'))
    check('wait-ended-before-expiry', lambda c: c['schedule']['expired_blocks'][-1].update(observed_at_us=c['schedule']['control']['lease_expires_at_us'] - 1))
    check('released-before-expiry', lambda c: c['schedule'].update(release_before_us=c['schedule']['control']['lease_expires_at_us'] - 1))
    check('lock-timeout-envelope', lambda c: c['schedule'].update(release_after_us=c['schedule']['dispatch']['server_us'] + 5_000_000))
    check('holder-bypassed-guards', lambda c: c['schedule']['holder']['identity'].update(session_replication_role='replica'))
    check('holder-has-dml', lambda c: c['schedule']['holder'].update(statement='UPDATE control.jobs SET checkpoint_revision=1;'))
    check('unobserved-holder-exit', lambda c: c['schedule']['holder']['closed'].update(observation='1'))
    check('unjoined-driver', lambda c: only(c['events'], event='activated-expiry-driver', stage='control').update(joined=False))
    check('reusable-expired-session', lambda c: only(c['events'], event='activated-expiry-reuse').update(available=True, error=None))
    check('wrong-error-taxonomy', lambda c: only(c['events'], event='activated-expiry-result').update(error='Control(Backend)'))
    check('unknown-operation-timeout', lambda c: only(c['events'], event='activated-expiry-result').update(error='TimeoutOutcomeUnknown'))
    check('backend-not-absent', lambda c: c['backend']['exact'].update(present=True))
    check('catalog-disabled', lambda c: c['setup']['catalog_after']['triggers'][0].update(enabled='D'))
    check('reader-not-joined', lambda c: c['response'].update(readers_joined=False))
    check('wrapper-nonzero', lambda c: c['response'].update(returncode=1))
    check('missing-final-custody', lambda c: c['events'].pop())
    return dict(status='PASS', count=len(rejected), rejected=rejected)
