#!/usr/bin/env python3
"""Independent semantic reader for finite successor retirement faults.

COMMIT uncertainty is not rollback. Literal wire observations and all 75 SQL
tables distinguish the two outcomes, while native/input/workflow bytes remain
unchanged. A separate raw caller binds successive processes and physical files.
"""
import copy
import json
import math
import re
import sys

sys.dont_write_bytecode = True
import retirement_replay as retirement
import transition_fault_replay as interrupted

history = retirement.history
encoded, sha, binary, be = history.encoded, history.sha, history.binary, history.be
only, same, loads, exact = history.only, history.same, history.loads, retirement.exact
CASES = ('before-forward', 'before-forward-fresh', 'after-complete',
         'after-complete-fresh', 'final-job-cancel', 'final-job-expiry')
CLONES = ('before', 'before', 'after', 'after', 'cancel', 'expiry')
PROXY_SHA256, COUNTERS = interrupted.PROXY_SHA256, interrupted.COUNTERS
NO_AUTHORITY = retirement.NO_AUTHORITY


def specification(case):
    index = CASES.index(case['name'])
    assert case['clone'] == CLONES[index] and case['origin'] == 'genuine-gap'
    assert case['operation'] == 'retire'
    assert case['predecessor'] == (CASES[index - 1] if index in (1, 3) else None)
    assert type(case['metadata_bytes']) is int and case['metadata_bytes'] == 262144
    assert re.fullmatch('[a-z0-9_]{1,63}', case['database'])
    assert case['config'] == '/lab/' + case['database'] + '.toml'
    return index


def wire_checked(fault, index, port):
    if index not in (0, 2):
        assert fault is None and port is None
        return
    mode = 'before-forward' if index == 0 else 'after-complete'
    assert set(fault) == {'point', 'mode', 'commit_number', 'proxy_port', 'wire'}
    assert fault['point'] == 'retirement' and fault['mode'] == mode
    assert type(fault['commit_number']) is int and fault['commit_number'] == 10
    assert type(port) is int and 1 <= port <= 65535 and port != 5432
    assert fault['proxy_port'] == port
    wire = fault['wire']
    assert wire['status'] == 'CAPTURED'
    for field in ('exit_code', 'wrapper_exit'):
        assert type(wire[field]) is int and wire[field] == 0
    for field in ('forced_wrapper_stop', 'stderr_nonempty', 'output_overflow'):
        assert wire[field] is False
    assert wire['proxy_sha256'] == PROXY_SHA256
    assert not any(k in wire for k in ('error', 'cleanup_error', 'metadata_error'))
    events = wire['events']
    assert events == [wire['ready'], wire['fault'], wire['receipt']]
    assert [e['event'] for e in events] == ['ready', 'fault', 'receipt']
    vector = [10, 9, 9, 9, 1] if index == 0 else [10, 10, 10, 9, 1]
    elapsed = 0
    for number, event in enumerate(events):
        assert event['format'] == 'bigbrotr-pg-fault-proxy/1' and event['mode'] == mode
        assert type(event['commit_number']) is int and event['commit_number'] == 10
        assert event['held'] is False and event['hold_action'] is None
        assert type(event['elapsed_ns']) is int and elapsed <= event['elapsed_ns'] <= 35_000_000_000
        elapsed = event['elapsed_ns']
        assert all(type(event[k]) is int for k in COUNTERS)
        assert [event[k] for k in COUNTERS] == (vector if number else [0] * 5)
        if number:
            assert event['reason'] == mode
    ready, _, receipt = events
    assert ready['proxy_sha256'] == PROXY_SHA256 and ready['listen_host'] == '127.0.0.1'
    assert ready['listen_port'] == port and ready['upstream_port'] == 5432
    assert type(ready['pid']) is int and ready['pid'] > 0
    assert ready['deadline_seconds'] == 30 and ready['max_frame_bytes'] == 1024**2
    assert ready['max_wire_bytes'] == 64 * 1024**2 and ready['max_messages'] == 20000
    assert set(receipt['frames']) == set(receipt['tags']) == {'frontend', 'backend'}
    for direction, tags in receipt['tags'].items():
        assert all(re.fullmatch('[0-9a-f]{2}', key) and type(n) is int and n > 0 for key, n in tags.items())
        assert type(receipt['frames'][direction]) is int
        assert receipt['frames'][direction] == sum(tags.values()) > 0
    assert sum(receipt['frames'].values()) <= ready['max_messages']
    assert receipt['tags']['frontend']['51'] >= 10
    assert receipt['tags']['backend']['43'] >= receipt['server_commit_complete']
    assert type(receipt['declared_wire_bytes']) is int and 0 < receipt['declared_wire_bytes'] <= ready['max_wire_bytes']
    raw = ''.join(json.dumps(e, sort_keys=True) + '\n' for e in events).encode()
    assert wire['stdout'].encode() == raw
    assert wire['stdout_sha256'] == sha(raw) and wire['stdout_bytes'] == len(raw)


class Model(interrupted.Model, retirement.Model):
    """Reuse exact active-Control fencing and the independent twelve-row reducer.

    The failed Control16 is still active in the original before image. Fresh17
    fences it and retires its role in the same transaction as its own claim.
    No artificially released baseline is supplied to either reducer.
    """


Transcript = retirement.Transcript


def clock(value, window):
    assert type(value) is int and window['before_us'] <= value <= window['after_us']
    return value


def wait_clock(t, mode, stage, current):
    value = t.retirement('wait-clock')
    at = clock(value['server_us'], t.model.window)
    exact(value, dict(event='lineage-retirement-wait-clock', mode=mode, stage=stage,
        server_us=at, control_deadline_us=current['lease_expires_at_us'],
        remaining_us=current['lease_expires_at_us'] - at, error=None))
    return value


def wait_gates(t, case, mode):
    gates = []
    for phase in ('ready', 'released'):
        event = t.retirement('wait-gate')
        exact(event, dict(event='lineage-retirement-wait-gate', mode=mode, phase=phase,
            prefix='/lab/' + case['name'] + '-gate', gate_timeout_ms=10000, poll_ms=5))
        gates.append(event)
    return gates


def diagnosis_checked(value, backend, window, released, *, compacted=False):
    assert type(compacted) is bool
    assert set(value) == {'backend', 'since', 'until', 'returncode', 'stdout', 'stderr',
        'errors', 'contexts', 'observed_sqlstate'}
    assert value['backend'] == backend and type(value['returncode']) is int and value['returncode'] == 0
    assert history.instant(value['since']) == window['before_us']
    assert history.instant(value['until']) == window['after_us']
    raw = value['stdout'] + value['stderr']
    assert len(raw.encode()) <= 256 * 1024
    prefix = r'\[' + str(backend['pid']) + r'\] '
    kind='activated' if compacted else 'lineage'
    pattern = prefix + r'ERROR:  (?:(?P<code>[0-9A-Z]{5}): )?'+kind+' retirement Control authority expired$'
    errors = [line for line in raw.splitlines() if re.search(pattern, line)]
    contexts = [line for line in raw.splitlines() if f'[{backend["pid"]}] CONTEXT:  ' in line
        and ('function control.lock_activated_initial_retirement(bytea)' if compacted else 'function control.require_ingest_successor(bytea,numeric,text)') in line]
    assert value['errors'] == errors and value['contexts'] == contexts
    error, = errors
    context, = contexts
    code = re.search(pattern, error).group('code')
    assert value['observed_sqlstate'] == code and code in (None, '23514')
    function=r'control\.lock_activated_initial_retirement\(bytea\)' if compacted else r'control\.require_ingest_successor\(bytea,numeric,text\)'
    assert re.search(prefix + r'CONTEXT:  PL/pgSQL function '+function+r' line [1-9][0-9]* at RAISE', context)
    for line in (error, context):
        # Docker log arrival can race with the probe return, but must follow the
        # holder's independent pre-release observation within this process run.
        assert released <= history.instant(line.split()[0]) <= window['after_us']


def schedule_checked(value, pg, current, backend, case, mode, refreshed, gates, dispatch, returned,
        cancelled, diagnostic, window, *, compacted=False):
    assert type(compacted) is bool
    fields = {'mode', 'status', 'gates', 'blocks', 'confirmed_blocks', 'expiry_observations',
        'cleanup_errors', 'holder', 'control_backend', 'control', 'refreshed', 'before_holder',
        'held', 'go_before_us', 'dispatch', 'release_before_us', 'release_after_us'}
    if mode == 'cancel':
        fields |= {'cancel_before_us', 'cancel', 'cancel_after_us'}
    assert set(value) == fields and value['mode'] == mode
    assert value['status'] == 'CAPTURED' and value['cleanup_errors'] == []
    assert value['control_backend'] == backend and value['control'] == current
    assert value['refreshed'] == refreshed and value['dispatch'] == dispatch
    same(pg, value['before_holder']); same(pg, value['held'])
    assert len(value['gates']) == 2
    observed = []
    for supplied, event in zip(value['gates'], gates, strict=True):
        exact(supplied, dict(event=event, observed_us=supplied['observed_us']))
        observed.append(clock(supplied['observed_us'], window))
    go = clock(value['go_before_us'], window)
    # The observer may sample dispatch before the host notices the released
    # gate; both must follow the host's creation of the .go file.
    assert refreshed['server_us'] <= observed[0] <= go <= observed[1]
    assert go <= dispatch['server_us'] and observed[1] - observed[0] < 10_000_000
    holder = value['holder']; physical = holder['backend']
    assert physical['database'] == backend['database'] and type(physical['pid']) is int and physical['pid'] > 0
    assert (physical['pid'], history.instant(physical['backend_start'])) != (backend['pid'], history.instant(backend['backend_start']))
    assert observed[0] <= history.instant(physical['backend_start']) <= go
    application = case['name'].replace('-', '_') + '_holder'
    assert physical['user'] == 'bigbrotr_control' and physical['application'] == application
    assert physical['isolation'] == 'read committed' and physical['synchronous_commit'] == 'on'
    exact(holder['identity'], dict(pid=physical['pid'], database=backend['database'], user='bigbrotr_control',
        role='bigbrotr_control', superuser=False, session_replication_role='origin', isolation='read committed'))
    assert holder['settings']['sqlstate'] == '00000' and holder['settings']['rows'] == 1
    assert [loads(s) for s in holder['settings']['lines']] == [dict(statement_timeout='30s', lock_timeout='20s', idle_timeout='1min')]
    assert holder['begin'] == holder['rollback'] == dict(sqlstate='00000', rows=0, lines=[])
    gap = only(pg['acquisition_gaps'], producer_epoch=2 if compacted else 3)
    job = only(pg['jobs'], job_id=gap['job_id'])
    assert job['request_key'] == '\\x' + 'a6' * 16 and job['state'] == 'cancelled'
    assert job['job_revision'] == 4 and job['checkpoint_revision'] == 0
    statement = ("SELECT to_jsonb(j)||jsonb_build_object('xmin',xmin::text) "
        "FROM control.jobs j WHERE job_id=decode('" + job['job_id'][2:] + "','hex') FOR UPDATE;")
    assert holder['statement'] == statement
    assert holder['lock']['sqlstate'] == '00000' and holder['lock']['rows'] == 1
    assert [loads(s) for s in holder['lock']['lines']] == [job]
    xid = value['blocks'][-1]['holder']['backend_xid']
    assert xid.isascii() and xid.isdecimal() and int(xid) > 0

    def blocked(samples, seconds):
        assert samples
        previous, elapsed = dispatch['server_us'], -1
        for sample in samples:
            duration = sample['sample_seconds']
            assert type(duration) in (int, float) and math.isfinite(duration) and elapsed <= duration <= seconds
            elapsed = duration
            at = clock(sample['observed_at_us'], window)
            assert at >= previous
            previous = at
            for key, identity in (('waiter', backend), ('holder', physical)):
                row = sample[key]
                assert row['pid'] == identity['pid'] and row['database'] == identity['database']
                assert history.instant(row['backend_start']) == history.instant(identity['backend_start'])
                assert row['user'] == 'bigbrotr_control'
            assert sample['holder']['application'] == application
            assert sample['holder']['query'] == statement and sample['holder']['state'] == 'idle in transaction'
            assert sample['holder']['backend_xid'] == xid
        last = samples[-1]; waiter = last['waiter']
        assert waiter['state'] == 'active' and waiter['wait_event_type'] == 'Lock' and physical['pid'] in waiter['blockers']
        assert ' '.join(waiter['query'].split()) == ('SELECT control.require_activated_initial_retirement($1::bytea)' if compacted else 'SELECT control.require_ingest_successor($1::bytea,$2::text::numeric,$3::text)')
        assert any(lock['type'] == 'transactionid' and lock['mode'] == 'ShareLock'
            and lock['transaction'] == xid and lock['granted'] is False for lock in last['waiting_locks'])
        return previous

    first = blocked(value['blocks'], 2)
    confirmed = blocked(value['confirmed_blocks'], 1)
    deadline = current['lease_expires_at_us']
    assert current['lease_expires_at_us'] - 20_000_000 <= refreshed['server_us'] <= dispatch['server_us'] <= first < deadline
    assert first <= confirmed
    if mode == 'cancel':
        assert value['expiry_observations'] == [] and diagnostic is None
        before = clock(value['cancel_before_us'], window); after = clock(value['cancel_after_us'], window)
        assert first <= before <= after <= confirmed and value['cancel'] == cancelled
        assert returned['server_us'] < deadline
    else:
        assert cancelled is None and 1_000_000 <= dispatch['remaining_us'] <= 2_000_000
        assert dispatch['server_us'] - refreshed['server_us'] <= 22_000_000
        previous = first
        assert value['expiry_observations']
        for observation in value['expiry_observations']:
            at = clock(observation['server_us'], window)
            exact(observation, dict(server_us=at, remaining_us=deadline - at))
            assert at >= previous
            previous = at
        assert deadline + 50_000 <= previous <= confirmed and returned['remaining_us'] <= 0
    release_before = clock(value['release_before_us'], window)
    release_after = clock(value['release_after_us'], window)
    assert confirmed <= release_before <= release_after and release_before <= returned['server_us']
    assert release_after - dispatch['server_us'] < 5_000_000 and returned['server_us'] - dispatch['server_us'] < 5_000_000
    closed = holder['closed']
    assert closed['backend'] == physical and type(closed['wrapper_exit']) is int
    assert closed['statement'] == ("SET max_parallel_workers_per_gather=0; SELECT count(*) FROM pg_stat_activity WHERE pid=" + str(physical['pid'])
        + " AND backend_start='" + physical['backend_start'] + "'::timestamptz AND datname='" + backend['database'] + "';")
    assert closed['observation'] == '0'
    if mode == 'expiry':
        diagnosis_checked(diagnostic, backend, window, release_before,compacted=compacted)


def prefix(t, m, known, case, index):
    exact(t.retirement('resources'), dict(event='lineage-retirement-resources', data_bytes=131072,
        native_bytes=0, max_jobs=64, source_closure=False, workflow=False, admission=False))
    resources = t.take('resources')
    for field in ('input_bytes', 'outcome_bytes', 'effect_bytes'):
        assert type(resources[field]) is int and 0 < resources[field] <= 256 * 1024**2
    exact(resources, dict(event='lineage-history-resources', metadata_bytes=262144,
        metadata_scratch_bytes=131072, metadata_entry_bytes=512, input_bytes=resources['input_bytes'],
        outcome_bytes=resources['outcome_bytes'], effect_bytes=resources['outcome_bytes'], native_bytes=128 * 1024**2,
        transport_bytes=4 * 1024**2, observation_bytes=32 * 1024**2, operation_timeout_ms=10000, frame_bytes=16 * 1024**2))
    assert sum(resources[k] for k in ('metadata_bytes', 'input_bytes', 'outcome_bytes', 'effect_bytes',
        'transport_bytes', 'observation_bytes')) + 4096 + 131072 <= 1024**3
    exact(t.take('resources', True), dict(event='lineage-transition-resources', operation='retire',
        mutation_data_bytes=4096, mutation_native_bytes=4096, lease_ttl_us='20000000', outstanding_request_limit=32))
    planned = t.take('control')
    assert set(planned) == {'event', 'phase', 'role', 'incarnation', 'lease_ttl_us'}
    assert planned['phase'] == 'claim-planned' and planned['lease_ttl_us'] == '20000000'
    t.snapshot('before-claim')
    if index in (0, 2):
        exact(t.take('transport', True), dict(event='lineage-transition-transport',
            control_port=case['proxy_port'], observer_port=5432, proxied=True))
    claimed, policy, backend = t.take('control'), t.take('policy'), t.take('backend')
    t.snapshot('after-claim', lambda pg: m.claim(pg, planned))
    assert m.control['epoch'] == (17 if index in (1, 3) else 16)
    exact(claimed, dict(event='lineage-history-control', phase='claimed', resource=m.control['resource_id'][2:],
        epoch=m.control['epoch'], role=planned['role'], incarnation=planned['incarnation']))
    exact(backend, dict(event='lineage-history-backend', pid=m.control['control_backend_pid'],
        backend_start=m.control['control_backend_start'], database=case['database'], control_epoch=m.control['epoch'], present=True))
    t.policy(policy, 'acquired', None, None)
    t.snapshot('before-retirement')
    t.snapshot('retirement-inspect-returned')
    old_at = retirement.terminal(m.pg, known, index == 3)
    exact(t.retirement('loaded'), dict(event='lineage-retirement-loaded', epoch=3, manifest=known['manifest'].hex(),
        producer_slot=known['slot'].hex(), issued_through=0, reserved_through=0,
        staged_input=None, barrier_confirmed=False, retired_at_us=old_at))
    t.snapshot('before-retirement-observe')
    t.snapshot('retirement-observe-returned')
    exact(t.retirement('observed'), dict(event='lineage-retirement-observed', epoch=3, confirmed=True, error=None))
    for epoch, count in enumerate(known['counts'], 1):
        for n in range(1, count + 1):
            t.refresh_retirement(epoch, n)
            t.snapshot(f'retirement-request-returned-{epoch}-{n}')
            exact(t.retirement('request'), dict(event='lineage-retirement-request', producer_slot=known['slot'].hex(),
                epoch=epoch, sequence=n, copy_mode='existing-retirement',
                reservation=only(m.pg['producer_requests'], producer_epoch=epoch, request_sequence=n),
                outcome=only(m.pg['producer_outcomes'], producer_epoch=epoch, request_sequence=n)))
        t.refresh_retirement(epoch, None)
        t.snapshot(f'retirement-epoch-returned-{epoch}')
        exact(t.retirement('epoch'), dict(event='lineage-retirement-epoch', summary=known['summaries'][epoch - 1]))
    t.refresh_retirement(None, None)
    t.snapshot('retirement-finish-returned')
    exact(t.retirement('proof'), dict(event='lineage-retirement-proof', verified_through=3,
        last_epoch=known['summaries'][-1], **NO_AUTHORITY))
    assert m.refreshes == 8
    return backend, old_at


def finish(t, m, backend, failed, lost):
    t.snapshot('after-retirement')
    exact(t.take('native'), dict(event='lineage-history-native', state='closed'))
    t.snapshot('before-control-end')
    driver = t.take('driver')
    assert type(driver['failed']) is bool
    # A real proxy drop may report a failed driver. Direct refusal/cancellation
    # is never allowed to turn missing driver custody into a completed process.
    assert lost or driver['failed'] is False
    exact(driver, dict(event='lineage-history-driver', driver='control', failed=driver['failed'], joined=True))
    exact(t.take('backend'), dict(backend, present=False))
    phase = 'dropped-and-joined' if failed else 'released-and-joined'
    exact(t.take('control'), dict(event='lineage-history-control', phase=phase,
        resource=m.control['resource_id'][2:], epoch=m.control['epoch']))
    t.snapshot('after-control-end', None if failed else m.release)
    t.snapshot('final')
    exact(t.take('driver'), dict(event='lineage-history-driver', driver='observer', failed=False, joined=True))
    exact(t.retirement('complete'), dict(event='lineage-retirement-complete', operation='retire', native='closed',
        control=phase, observer='joined', success=not failed, **dict(NO_AUTHORITY, retirement=not failed)))
    assert t.position == len(t.events), 'unclassified trailing telemetry'


def verify(events, before, native, inputs, case, server_window, *, wire=None, schedule=None, diagnostic=None):
    index = specification(case)
    wire_checked(wire, index, case['proxy_port'])
    if index not in (4, 5):
        assert schedule is diagnostic is None
    if index not in (1, 3):
        pin = retirement.PINS['genuine-gap']
        assert sha(encoded(history.normalized(before))) == pin['sql_rows']
        assert sha(encoded(native)) == pin['native_rows'] and sha(encoded(inputs)) == pin['input_rows']
    # The raw caller must additionally bind fresh before to the preceding full
    # after. This local discriminator never fabricates a release after loss.
    previous = max((r for r in before['owner_epochs'] if r['role'] == 'control'), key=lambda r: r['epoch'])
    assert previous['epoch'] == (16 if index in (1, 3) else 15)
    assert previous['state'] == ('active' if index in (1, 3) else 'released')
    known = retirement.science(before, native, inputs, 'genuine-gap', index == 3)
    assert known['counts'] == [4, 0, 0] and known['issued'] == 0
    m = Model(before, native, inputs, server_window)
    t = Transcript(events, m)
    backend, old_at = prefix(t, m, known, case, index)
    if index in (4, 5):
        mode = 'cancel' if index == 4 else 'expiry'
        refreshed = wait_clock(t, mode, 'refreshed', m.control)
        gates = wait_gates(t, case, mode)
    t.snapshot('before-retirement-commit')
    cancelled = None
    if index in (4, 5):
        dispatch = wait_clock(t, mode, 'dispatch', m.control)
        if index == 4:
            cancelled = t.retirement('wait-cancel')
            exact(cancelled, dict(event='lineage-retirement-wait-cancel', mode='cancel', phase='applied', cancelled=True))
    persisted = index in (1, 2, 3)
    t.snapshot('retirement-commit-returned', (lambda pg: m.retire(pg, known)) if persisted else None)
    error = 'CommitUnknown' if index in (0, 2) else 'Control(Unavailable)' if index == 4 else 'Control(Backend)' if index == 5 else None
    exact(t.retirement('committed'), dict(event='lineage-retirement-committed', epoch=3,
        gaps=None if error else 1, previous_retired_at_us=old_at, error=error))
    if index in (4, 5):
        returned = wait_clock(t, mode, 'returned', m.control)
        schedule_checked(schedule, m.pg, m.control, backend, case, mode, refreshed, gates, dispatch,
            returned, cancelled, diagnostic, server_window)
    observation = None if error else dict(target_epoch=3, issued_through=0, reserved_through=0,
        verified_through=3, last_epoch=known['summaries'][-1], gaps=1, **dict(NO_AUTHORITY, retirement=True))
    exact(t.retirement('result'), dict(event='lineage-retirement-result', operation='retire', observation=observation,
        stage='retire' if error else 'complete', epoch=3 if error else None, sequence=None, error=error))
    finish(t, m, backend, bool(error), index in (0, 2))
    assert t.snapshots == 28 and m.refreshes == 8
    retirement.terminal(m.pg, known, persisted)
    assert m.native == native and m.inputs == inputs
    return dict(status='PASS', case=case['name'], database=case['database'],
        snapshots=t.snapshots, tables=75, native_rows=55, sequence=68, inputs=4,
        control_epoch=m.control['epoch'], refreshes=8, failure=error,
        persistent_outcome='terminal' if persisted else 'ready', delivered_retirement=error is None,
        scope='Finite C10 loss or bounded final-job cancellation/UTC wait; no source/workflow/native mutation')


def negative_controls(events, before, native, inputs, case, server_window, *, wire=None, schedule=None, diagnostic=None):
    """Reject coherent mutations, including custody unknown and wrong wait cause."""
    original = dict(events=events, before=before, native=native, inputs=inputs, case=case,
        server_window=server_window, wire=wire, schedule=schedule, diagnostic=diagnostic)
    verify(**original)
    index = specification(case)
    rejected = []

    def event(context, name, **fields):
        return only(context['events'], event=name, **fields)

    def sql(context, mutation, stage=None):
        active = stage is None
        for value in context['events']:
            if value['event'] == 'lineage-history-state':
                active = active or value['stage'] == stage
                if active:
                    state = value['state']
                    pg = {name: loads(rows) for name, rows in state['control_tables']}
                    mutation(pg)
                    state['control_tables'] = [[name, encoded(rows).decode()] for name, rows in sorted(pg.items())]
                    state['control_sha256'] = sha(encoded(state['control_tables']))

    def reject(name, mutate):
        context = copy.deepcopy(original)
        mutate(context)
        try:
            verify(**context)
        except (AssertionError, KeyError, ValueError, TypeError, OverflowError, IndexError):
            rejected.append(name)
        else:
            raise AssertionError('accepted altered evidence: ' + name)

    reject('historical-xmin-rehashed', lambda c: sql(c,
        lambda pg: only(pg['producer_epochs'], producer_epoch=1).update(xmin='3999999999')))
    reject('source-xmin-rewritten', lambda c: sql(c,
        lambda pg: only(pg['source_streams'], owner_epoch=3).update(xmin='3999999999')))
    reject('source-cut-rewritten', lambda c: sql(c,
        lambda pg: only(pg['source_streams'], owner_epoch=3).update(last_complete_sequence=1)))
    reject('old-copy-rewritten', lambda c: sql(c,
        lambda pg: only(pg['producer_outcomes'], producer_epoch=1, request_sequence=4).update(xmin='3999999999')))
    reject('gap-rewritten', lambda c: sql(c,
        lambda pg: only(pg['acquisition_gaps'], producer_epoch=3).update(xmin='3999999999')))
    reject('claimed-job-rewritten', lambda c: sql(c,
        lambda pg: only(pg['jobs'], request_key='\\x' + 'a6' * 16).update(job_revision=3)))
    reject('unclaimed-job-rewritten', lambda c: sql(c,
        lambda pg: only(pg['jobs'], request_key='\\x' + 'a7' * 16).update(job_revision=2)))
    reject('session-revoked', lambda c: sql(c,
        lambda pg: only(pg['browser_sessions']).update(revoked_at_us=server_window['before_us'])))
    reject('input-rewritten', lambda c: event(c, 'lineage-history-state', stage='final')['state']['originals'][0][1].update(sha256='00' * 32))
    reject('native-sequence-rewritten', lambda c: event(c, 'lineage-history-state', stage='final')['state'].update(sequence=69))
    reject('control-driver-unjoined', lambda c: event(c, 'lineage-history-driver', driver='control').update(joined=False))
    reject('observer-driver-failed', lambda c: event(c, 'lineage-history-driver', driver='observer').update(failed=True))
    reject('backend-still-present', lambda c: event(c, 'lineage-history-backend', present=False).update(present=True))
    reject('native-not-closed', lambda c: event(c, 'lineage-history-native').update(state='unknown'))
    reject('missing-completion', lambda c: c['events'].pop())
    reject('invented-writer-authority', lambda c: event(c, 'lineage-retirement-complete').update(admission=True))
    reject('proof-confers-retirement', lambda c: event(c, 'lineage-retirement-proof').update(retirement=True))
    reject('copy-repair-invented', lambda c: event(c, 'lineage-retirement-request', epoch=1, sequence=1).update(copy_mode='current-positive'))
    reject('wrong-refusal-taxonomy', lambda c: event(c, 'lineage-retirement-result').update(error='TimeoutOutcomeUnknown'))
    reject('wrong-committed-gap-count', lambda c: event(c, 'lineage-retirement-committed').update(gaps=9))
    if index in (1, 2, 3):
        reject('terminal-receipt-issuer-rewritten', lambda c: sql(c,
            lambda pg: only(pg['ingest_terminal_transactions'], target_epoch=3).update(control_epoch=1), 'retirement-commit-returned'))
        reject('terminal-domain-partial', lambda c: sql(c,
            lambda pg: only(pg['allocation_domains'], owner_epoch=3, dictionary_kind=9).update(state='active'), 'retirement-commit-returned'))
    else:
        reject('partial-retirement-on-refusal', lambda c: sql(c,
            lambda pg: only(pg['producer_epochs'], producer_epoch=3).update(state='retired'), 'retirement-commit-returned'))
    if index in (1, 3):
        reject('old-control-fence-separate-xid', lambda c: sql(c,
            lambda pg: only(pg['owner_epochs'], role='control', epoch=16).update(xmin='3999999999'), 'after-claim'))
        role = only(before['owner_epochs'], role='control', epoch=16)['role_instance_id']
        reject('old-control-role-separate-xid', lambda c: sql(c,
            lambda pg: only(pg['role_instances'], role_instance_id=role).update(xmin='3999999999'), 'after-claim'))
    if index == 3:
        reject('retry-rewrites-receipt-to-new-control', lambda c: sql(c,
            lambda pg: only(pg['ingest_terminal_transactions'], target_epoch=3).update(control_epoch=17), 'retirement-commit-returned'))
        reject('retry-rewrites-target-time', lambda c: sql(c,
            lambda pg: only(pg['producer_epochs'], producer_epoch=3).update(retired_at_us=server_window['before_us']), 'retirement-commit-returned'))
    if index in (0, 2):
        reject('unknown-proxy-custody', lambda c: c['wire']['wire'].update(status='INCOMPLETE'))
        reject('proxy-forced-stop', lambda c: c['wire']['wire'].update(forced_wrapper_stop=True))
        reject('wire-unbound-raw', lambda c: c['wire']['wire'].update(stdout=''))
        def counter(c):
            w = c['wire']['wire']
            for e in w['events'][1:]:
                e['client_commit_complete'] = 10
            w.update(fault=w['events'][1], receipt=w['events'][2])
            raw = ''.join(json.dumps(e, sort_keys=True) + '\n' for e in w['events'])
            w.update(stdout=raw, stdout_bytes=len(raw.encode()), stdout_sha256=sha(raw.encode()))
        reject('wire-claims-complete-client-delivery-rehashed', counter)
        reject('loss-reported-as-success', lambda c: event(c, 'lineage-retirement-complete').update(success=True))
    else:
        reject('direct-control-driver-failed', lambda c: event(c, 'lineage-history-driver', driver='control').update(failed=True))
    if index in (4, 5):
        reject('unknown-holder-custody', lambda c: c['schedule']['holder']['closed'].update(observation='1'))
        reject('holder-rollback-missing', lambda c: c['schedule']['holder'].update(rollback=None))
        reject('schedule-incomplete', lambda c: c['schedule'].update(status='INCOMPLETE'))
        reject('waiter-wrong-pid', lambda c: c['schedule']['blocks'][-1]['waiter'].update(pid=1))
        reject('waiter-wrong-query', lambda c: c['schedule']['confirmed_blocks'][-1]['waiter'].update(query='SELECT 1'))
        reject('holder-wrong-transaction', lambda c: c['schedule']['confirmed_blocks'][-1]['holder'].update(backend_xid='3999999999'))
        reject('wait-not-blocked', lambda c: c['schedule']['confirmed_blocks'][-1]['waiter'].update(blockers=[]))
        reject('lease-refreshed-during-wait', lambda c: event(c, 'lineage-retirement-wait-clock', stage='returned').update(control_deadline_us=1))
        if index == 4:
            reject('cancel-not-applied', lambda c: event(c, 'lineage-retirement-wait-cancel').update(cancelled=False))
            reject('cancel-clock-after-expiry', lambda c: event(c, 'lineage-retirement-wait-clock', stage='returned').update(server_us=c['schedule']['control']['lease_expires_at_us']))
        else:
            reject('expiry-not-observed', lambda c: c['schedule'].update(expiry_observations=[]))
            reject('foreign-backend-diagnostic', lambda c: c['diagnostic']['backend'].update(pid=1))
            reject('fabricated-sqlstate', lambda c: c['diagnostic'].update(observed_sqlstate='57014'))
            reject('wrong-expiry-diagnostic', lambda c: c['diagnostic'].update(stdout='', stderr=''))
    return dict(status='PASS', count=len(rejected), rejected=rejected)
