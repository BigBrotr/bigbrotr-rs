"""Exact current-Control expiry after an observed final retirement job-lock wait.

Frozen source2 science is independently pinned by the earlier source oracle.
Only a fresh Control claim and three real refreshes may change PostgreSQL here.
"""
import copy
import json
import re

from activated_retirement_oracle import seed_checked, migrated
from activated_recovery_faults_oracle import bounded_number
from initial_live_oracle import Capture, clock, encoded, digest
from initial_live_routes_oracle import backend_checked
from initial_succession_oracle import claim, renewed, same, only
from installed_empty_oracle import native as native_map, fresh_xmin, window
from installed_live_oracle import instant

MODE = 'successor-activated-retirement-expiry'
RETIREMENT_QUERY = 'SELECT control.require_activated_initial_retirement($1::bytea)'


def checked_clock(value, stage, owner, envelope):
    at = value['server_us']
    clock(at, envelope)
    assert value == dict(event='retirement-expiry-clock', stage=stage, server_us=at,
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
        assert value == dict(gate='before-retirement', phase=phase, server_us=value['server_us'])
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

    holder_xid = schedule['blocks'][-1]['holder']['backend_xid']
    assert holder_xid.isascii() and holder_xid.isdecimal() and int(holder_xid) > 0

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
            assert sample['holder']['backend_xid'] == holder_xid
        value = samples[-1]
        waiter = value['waiter']
        assert waiter['state'] == 'active' and waiter['wait_event_type'] == 'Lock'
        assert backend['pid'] in waiter['blockers']
        assert ' '.join(waiter['query'].split()) == ' '.join(RETIREMENT_QUERY.split())
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
    assert closed['statement'] == ("SET max_parallel_workers_per_gather=0; SELECT count(*) FROM pg_stat_activity WHERE pid=" + str(backend['pid'])
        + " AND backend_start='" + backend['backend_start'] + "'::timestamptz"
        + " AND datname='" + backend['database'] + "';")
    assert closed['observation'] == '0'


def diagnosis_checked(value, current, backend, envelope, released):
    assert value['backend'] == backend['exact']['expected'] and value['returncode'] == 0
    assert value['observed_sqlstate'] in (None, '23514')
    assert instant(value['since']) == envelope['before_us'] and instant(value['until']) == envelope['after_us']
    error, = value['errors']; context, = value['contexts']
    prefix = r'\[' + str(current['control_backend_pid']) + r'\] '
    assert re.search(prefix + r'ERROR:  (?:23514: )?activated retirement Control authority expired$', error)
    assert re.search(prefix + r'CONTEXT:  PL/pgSQL function control\.lock_activated_initial_retirement\(bytea\) line [1-9][0-9]* at RAISE', context)
    for line in (error, context):
        observed = instant(line.split()[0].replace('Z', '+00:00'))
        # Docker timestamps record log arrival, independently of the probe's
        # return clock. They must follow the holder's pre-release observation.
        assert released <= observed <= envelope['after_us']


def verify(events, original, native, inputs, response, *, seed, setup, server_window,
        backend, schedule, after, native_after, inputs_after, input_contents, diagnostic):
    window(server_window)
    assert seed['name'] == 'off'
    science = seed_checked(seed)
    same(original, migrated(seed, setup, server_window))
    assert native == native_after == seed['native']
    assert inputs == inputs_after == seed['inputs'] and input_contents == seed['contents']
    assert response['outcome'] == 'PROCESS_EXITED' and response['returncode'] == 0 and response['stderr'] == ''
    assert 'custody_error' not in response
    database = backend['exact']['expected']['database']
    assert response['command'] == ['docker', 'exec', response['command'][2], '/build/ingest_bootstrap_probe',
        MODE, database, '/run/secrets/application', '/lab/' + database + '.toml', '/lab/' + MODE + '-gate']
    assert isinstance(response['command'][2], str) and response['command'][2]
    assert response['readers_joined'] is True and response['overflow'] is False
    bounded_number(response['seconds'], 120)
    assert [json.loads(line) for line in response['stdout'].splitlines()] == events
    cap = Capture(events, original, native_map(native), inputs)
    cap.sequence = 85
    assert cap.take('successor-control', 'retirement-expiry', 'claim-planned') == dict(event='successor-control',
        stage='retirement-expiry', phase='claim-planned', role='d5' * 16, incarnation='df' * 16, lease_ttl_us='20000000')
    same(original, cap.snapshot('retirement-expiry-before-claim'))
    claimed = cap.take('successor-control', 'retirement-expiry', 'claimed')
    current = cap.snapshot('retirement-expiry-after-claim')
    owner = claim(original, current, 3)
    assert owner['epoch'] == 13
    assert claimed == dict(event='successor-control', stage='retirement-expiry', phase='claimed',
        resource=owner['resource_id'][2:], epoch=13, role='d5' * 16, incarnation='df' * 16)
    role = only(current['role_instances'], role_instance_id=owner['role_instance_id'])
    for value in (owner['acquired_at_us'], role['created_at_us'], instant(owner['control_backend_start'])):
        clock(value, server_window)
    physical = cap.take('successor-backend', 'retirement-expiry')
    assert physical == dict(event='successor-backend', stage='retirement-expiry', **backend['exact']['expected'], present=True)
    same(current, cap.snapshot('retirement-expiry-loaded'))
    same(current, cap.snapshot('retirement-expiry-producer-observed'))

    def refresh(stage):
        nonlocal current, owner
        following = cap.snapshot(stage)
        updated = renewed(current, following, owner, server_window['after_us'])
        fresh_xmin(updated['xmin'], current)
        assert updated['lease_expires_at_us'] >= owner['lease_expires_at_us']
        clock(updated['lease_expires_at_us'] - 20_000_000, server_window)
        current, owner = following, updated

    for sequence in (1, 2):
        refresh('retirement-expiry-refreshed-' + str(sequence))
        outcome = only(seed['control']['producer_outcomes'], producer_epoch=2, request_sequence=sequence)
        assert cap.take('retirement-expiry-request') == dict(event='retirement-expiry-request', sequence=sequence,
            outcome='durable', receipt=outcome['outcome_bytes'][2:], rejection=None)
        same(current, cap.snapshot('retirement-expiry-resolved-' + str(sequence)))
    assert cap.take('retirement-expiry-proof') == dict(event='retirement-expiry-proof', resolved_requests=2,
        last_complete_sequence=5, sha256=science['digest'])
    refresh('retirement-expiry-final-refreshed')
    refreshed = cap.take('retirement-expiry-clock', 'refreshed')
    at = checked_clock(refreshed, 'refreshed', owner, server_window)
    assert owner['lease_expires_at_us'] - 20_000_000 <= at < owner['lease_expires_at_us']
    for phase in ('waiting', 'released'):
        assert cap.take('retirement-expiry-gate', phase=phase) == dict(event='retirement-expiry-gate',
            gate='before-retirement', phase=phase)
    dispatch = cap.take('retirement-expiry-clock', 'dispatch')
    checked_clock(dispatch, 'dispatch', owner, server_window)
    assert 1_000_000 <= dispatch['remaining_us'] <= 2_000_000
    assert cap.take('retirement-expiry-result', 'retirement') == dict(event='retirement-expiry-result',
        stage='retirement', sequence=None, gaps=None, error='Control(Backend)')
    returned = cap.take('retirement-expiry-clock', 'returned')
    checked_clock(returned, 'returned', owner, server_window)
    assert returned['remaining_us'] <= 0
    same(current, cap.snapshot('retirement-expiry-after-retirement'))
    assert cap.take('retirement-expiry-reuse') == dict(event='retirement-expiry-reuse', available=False, error='Unavailable')
    same(current, cap.snapshot('retirement-expiry-after-reuse'))
    same(current, cap.snapshot('retirement-expiry-final-before-control-end'))
    assert cap.take('retirement-expiry-driver', 'control') == dict(event='retirement-expiry-driver',
        stage='control', joined=True, failed=False, aborted=False, error=None)
    assert cap.take('successor-control', 'retirement-expiry', 'dropped-and-joined') == dict(event='successor-control',
        stage='retirement-expiry', phase='dropped-and-joined', resource=owner['resource_id'][2:], epoch=13)
    assert cap.take('retirement-expiry-backend-absence') == dict(event='retirement-expiry-backend-absence', error=None)
    assert cap.take('successor-backend', 'retirement-expiry') == dict(physical, present=False)
    same(current, cap.snapshot('retirement-expiry-final-after-control-end'))
    same(current, cap.snapshot('retirement-expiry-final'))
    assert cap.take('retirement-expiry-driver', 'observer') == dict(event='retirement-expiry-driver',
        stage='observer', joined=True, failed=False, aborted=False, error=None)
    assert cap.take('retirement-expiry-complete') == dict(event='retirement-expiry-complete', command=MODE,
        native='closed', control='dropped-and-joined', admission=False, expected_refusal=True)
    assert cap.position == len(events)
    same(current, after)
    backend_checked(backend, owner)
    bounded_number(backend['absence']['seconds'], 120)
    gap = only(seed['control']['acquisition_gaps'], producer_epoch=2)
    job = only(seed['control']['jobs'], job_id=gap['job_id'])
    assert job['state'] == 'cancelled' and job['job_revision'] == 4
    schedule_checked(schedule, current, owner, physical, refreshed, dispatch, returned, job, server_window)
    diagnosis_checked(diagnostic, owner, backend, server_window, schedule['release_before_us'])
    return dict(status='PASS', snapshots=cap.snapshots, control_tables=72, native_rows=70, native_sequence=85,
        original_inputs=6, resolved_requests=2, source_cut=5, jobs_cancelled=0, gaps_added=0,
        expired_control_epoch=13, actual_blocked_query=RETIREMENT_QUERY, refusal='Control(Backend)')


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
        event = only(value['events'], event='successor-state', stage='retirement-expiry-after-retirement')
        rows = {k:json.loads(v) for k, v in event['state']['control_tables']}
        mutate(rows)
        event['state']['control_tables'] = [[k, encoded(rows[k]).decode()] for k in sorted(rows)]
        event['state']['control_sha256'] = digest(encoded(event['state']['control_tables']))

    check('partial-retirement', lambda c: sql(c, lambda rows: only(rows['producer_epochs'],
        producer_epoch=2).update(state='retired', retired_at_us=c['server_window']['after_us'])))
    check('completed-job-rewritten', lambda c: sql(c, lambda rows: only(rows['jobs'],
        job_id=only(rows['acquisition_gaps'], producer_epoch=2)['job_id']).update(job_revision=5)))
    check('prior-gap-rewritten', lambda c: sql(c, lambda rows: only(rows['acquisition_gaps'], producer_epoch=1).update(xmin='999999')))
    check('outcome-copy-reissued', lambda c: sql(c, lambda rows: only(rows['producer_outcomes'], producer_epoch=2, request_sequence=2).update(recorder_epoch=13)))
    check('native-write', lambda c: c['native_after'].update(sequence=86))
    check('lost-input', lambda c: c['input_contents'].pop(next(iter(c['input_contents']))))
    check('holder-transaction-switched', lambda c: c['schedule']['expired_blocks'][-1]['holder'].update(backend_xid='999999'))
    check('wrong-proof-digest', lambda c: only(c['events'], event='retirement-expiry-proof').update(sha256='00' * 32))
    check('source-receipt-mismatch', lambda c: only(c['events'], event='retirement-expiry-request', sequence=2).update(receipt='00'))
    check('no-real-wait', lambda c: c['schedule']['blocks'][-1]['waiter'].update(blockers=[]))
    check('wrong-waited-query', lambda c: c['schedule']['blocks'][-1]['waiter'].update(query='SELECT 1'))
    check('wrong-holder-incarnation', lambda c: c['schedule']['expired_blocks'][-1]['holder'].update(backend_start='2000-01-01T00:00:00+00:00'))
    check('wait-ended-before-expiry', lambda c: c['schedule']['expired_blocks'][-1].update(observed_at_us=c['schedule']['control']['lease_expires_at_us'] - 1))
    check('released-before-expiry', lambda c: c['schedule'].update(release_before_us=c['schedule']['control']['lease_expires_at_us'] - 1))
    check('lock-timeout-envelope', lambda c: c['schedule'].update(release_after_us=c['schedule']['dispatch']['server_us'] + 5_000_000))
    check('holder-bypassed-guards', lambda c: c['schedule']['holder']['identity'].update(session_replication_role='replica'))
    check('holder-has-dml', lambda c: c['schedule']['holder'].update(statement='UPDATE control.jobs SET checkpoint_revision=1;'))
    check('unobserved-holder-exit', lambda c: c['schedule']['holder']['closed'].update(observation='1'))
    check('unjoined-driver', lambda c: only(c['events'], event='retirement-expiry-driver', stage='control').update(joined=False))
    check('reusable-expired-session', lambda c: only(c['events'], event='retirement-expiry-reuse').update(available=True, error=None))
    check('wrong-error-taxonomy', lambda c: only(c['events'], event='retirement-expiry-result').update(error='Control(Unavailable)'))
    check('unknown-operation-timeout', lambda c: only(c['events'], event='retirement-expiry-result').update(error='TimeoutOutcomeUnknown'))
    check('backend-not-absent', lambda c: c['backend']['exact'].update(present=True))
    check('catalog-disabled', lambda c: c['setup']['catalog_after']['triggers'][0].update(enabled='D'))
    check('reader-not-joined', lambda c: c['response'].update(readers_joined=False))
    check('wrapper-nonzero', lambda c: c['response'].update(returncode=1))
    check('missing-final-custody', lambda c: c['events'].pop())
    check('wrong-diagnostic-function', lambda c: c['diagnostic'].update(contexts=[]))
    check('generic-backend-error', lambda c: c['diagnostic'].update(errors=[c['diagnostic']['errors'][0].replace('activated retirement', 'unrelated operation')]))
    check('wrong-database-config', lambda c: c['response']['command'].__setitem__(7, '/lab/other.toml'))
    check('source-rewritten', lambda c: sql(c, lambda rows: only(rows['source_streams'], owner_epoch=2).update(xmin='999999')))
    check('terminal-gap-rewritten', lambda c: sql(c, lambda rows: only(rows['acquisition_gaps'], producer_epoch=2).update(issuer_epoch=13)))
    return dict(status='PASS', count=len(rejected), rejected=rejected)
