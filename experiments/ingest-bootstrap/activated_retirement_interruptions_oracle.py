"""Independent installed retirement cancellation at the final retained-job wait.

The C6 hold separates prior positive replay from final dispatch. Full historical
SQL/native/input state remains exact; no installed session telemetry is invented.
"""
import copy
import json
import re

import activated_retirement_oracle as retirement
from activated_recovery_faults_oracle import (COUNTERS, PROXY_SHA256, backend_checked,
    bounded_number, config_checked, failed_control)
from activated_recovery_oracle import inside_window, sha
from initial_succession_oracle import only, same
from installed_empty_oracle import native, window
from installed_live_oracle import instant

MODE = 'installed-activated-retirement-job-signal'
CASES = (MODE,)
JOB = '42e09b5a41d12027920cb581d7941ec3'
LOCK = f"""SELECT json_build_object('job',to_jsonb(j)||jsonb_build_object('xmin',xmin::text),
    'transaction_id',pg_current_xact_id()::text,
    'locked_at_us',(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint)
FROM control.jobs j WHERE job_id=decode('{JOB}','hex') FOR UPDATE;"""
WAIT = 'SELECT control.require_activated_initial_retirement($1::bytea)'


def wire_checked(wire):
    assert wire['status'] == 'CAPTURED'
    assert type(wire['exit_code']) is type(wire['wrapper_exit']) is int
    assert wire['exit_code'] == wire['wrapper_exit'] == 0
    assert wire['forced_wrapper_stop'] is False and wire['stderr_nonempty'] is False
    assert wire['output_overflow'] is False and wire['readers_joined'] is True
    assert wire['proxy_sha256'] == PROXY_SHA256
    assert not any(k in wire for k in ('error', 'cleanup_error', 'metadata_error', 'close_error'))
    events = wire['events']
    assert events == [wire[k] for k in ('ready', 'held', 'released', 'receipt')]
    assert [e['event'] for e in events] == ['ready', 'held', 'released', 'receipt']
    expected = ([0] * 5, [6, 5, 5, 5, 1], [6, 5, 5, 5, 1], [6, 6, 6, 6, 1])
    elapsed = 0
    for index, event in enumerate(events):
        assert event['format'] == 'bigbrotr-pg-fault-proxy/1'
        assert event['mode'] == 'hold' and event['commit_number'] == 6
        assert event['held'] is (index == 1)
        assert event['hold_action'] == ('release' if index >= 2 else None)
        assert type(event['elapsed_ns']) is int and elapsed <= event['elapsed_ns'] <= 35_000_000_000
        elapsed = event['elapsed_ns']
        assert all(type(event[key]) is int for key in COUNTERS)
        assert [event[key] for key in COUNTERS] == expected[index], 'C6 hold/release, no retirement COMMIT7'
    ready, _, _, receipt = events
    assert ready['proxy_sha256'] == PROXY_SHA256 and ready['listen_host'] == '127.0.0.1'
    assert type(ready['listen_port']) is int and 1 <= ready['listen_port'] <= 65535
    assert ready['upstream_port'] == 5432 and ready['listen_port'] != 5432
    assert type(ready['pid']) is int and ready['pid'] > 0
    assert ready['deadline_seconds'] == 30 and ready['max_frame_bytes'] == 1024 ** 2
    assert ready['max_wire_bytes'] == 64 * 1024 ** 2 and ready['max_messages'] == 20000
    assert receipt['reason'] in ('frontend-eof', 'backend-eof')
    assert set(receipt['frames']) == set(receipt['tags']) == {'frontend', 'backend'}
    for direction, tags in receipt['tags'].items():
        assert all(re.fullmatch('[0-9a-f]{2}', tag) and type(n) is int and n > 0 for tag, n in tags.items())
        assert receipt['frames'][direction] == sum(tags.values()) > 0
    assert sum(receipt['frames'].values()) <= ready['max_messages']
    assert receipt['tags']['frontend']['51'] >= 6 and receipt['tags']['backend']['43'] >= 6
    assert type(receipt['declared_wire_bytes']) is int and 0 < receipt['declared_wire_bytes'] <= ready['max_wire_bytes']
    raw = ''.join(json.dumps(e, sort_keys=True) + '\n' for e in events).encode()
    assert wire['stdout'].encode() == raw and wire['stdout_sha256'] == sha(raw)
    assert wire['stdout_bytes'] == len(raw)
    return ready['listen_port']


def response_checked(response):
    assert response['outcome'] == 'PROCESS_EXITED' and type(response['returncode']) is int
    assert response['returncode'] == 4 and response['stdout'] == ''
    assert response['stderr'] == 'error: initial Ingest cancelled; original attempt may need recovery\n'
    assert not any(k in response for k in ('orchestration_error', 'terminal_error', 'clock_error',
        'output_limit_exceeded', 'holder_error'))
    bounded_number(response['seconds'], 120)
    terminal = response['terminal']
    assert terminal['exit_code'] == 4 and terminal['forced'] is False and terminal['reaped'] is True
    assert type(terminal['pid']) is int and terminal['pid'] > 0
    assert type(terminal['start_ticks']) is int and terminal['start_ticks'] > 0
    bounded_number(terminal['elapsed_seconds'], 50)
    assert terminal['prefix'] == '/lab/' + MODE
    assert terminal['config'] == '/lab/' + MODE + '-proxy.toml'
    assert terminal['command'] == ['/build/bigbrotr', 'ingest', 'recover', '--file', terminal['config'],
        '--retire-activated-initial']
    return terminal


def fixture_checked(fixture, before, current, clock, database, terminal, seed):
    assert fixture['kind'] == 'retirement-final-job-signal' and fixture['cleanup_errors'] == []
    assert not any(k in fixture for k in ('schedule_error', 'invoke_error', 'clock_error'))
    window(fixture['window'])
    assert fixture['window']['before_us'] <= clock['before_us'] <= clock['after_us'] <= fixture['window']['after_us']
    port = wire_checked(fixture['wire'])
    config_checked(fixture, MODE, database, seed, port)
    assert fixture['config']['proxy_path'] == terminal['config']
    holder = fixture['holder']
    assert set(holder) == {'pid', 'backend_start', 'database', 'user', 'application', 'isolation', 'synchronous_commit'}
    assert type(holder['pid']) is int and holder['pid'] > 0
    assert holder['database'] == database and holder['user'] == 'bigbrotr_control'
    assert holder['application'] == MODE + '_holder' and holder['isolation'] == 'read committed'
    assert holder['synchronous_commit'] == 'on'
    inside_window(instant(holder['backend_start']), fixture['window'])
    assert fixture['identity'] == dict(pid=holder['pid'], database=database, user='bigbrotr_control',
        role='bigbrotr_control', superuser=False, session_replication_role='origin', isolation='read committed')
    assert fixture['begin'] == dict(sqlstate='00000', rows=0, lines=[])
    assert fixture['lock_statement'] == LOCK
    answer = fixture['lock_result']
    assert answer['sqlstate'] == '00000' and answer['rows'] == 1
    raw, = answer['lines']
    locked = json.loads(raw)
    assert set(locked) == {'job', 'transaction_id', 'locked_at_us'}
    job = only(before['jobs'], job_id='\\x' + JOB)
    gap = only(before['acquisition_gaps'], producer_epoch=2)
    assert gap['job_id'] == job['job_id'] and gap['issuer_epoch'] == 11
    assert job['state'] == 'cancelled' and job['job_revision'] == 4 and job['owner_epoch'] == 11
    assert locked['job'] == job
    assert re.fullmatch('[1-9][0-9]*', locked['transaction_id']) and int(locked['transaction_id']) < 2**64
    observed = fixture['blocked']
    assert observed['pid'] == current['control_backend_pid'] and observed['backend_start'] == current['control_backend_start']
    assert observed['database'] == database and observed['user'] == 'bigbrotr_control'
    assert observed['control'] == current and observed['query'].strip() == WAIT
    assert observed['holder_pid'] == holder['pid'] != observed['pid']
    assert instant(observed['holder_start']) == instant(holder['backend_start']) and observed['holder_user'] == 'bigbrotr_control'
    assert observed['wait_event_type'] == 'Lock' and observed['wait_event'] == 'transactionid'
    assert observed['blockers'] == [holder['pid']]
    assert observed['waiting_locks'] == [dict(type='transactionid', mode='ShareLock', relation=None,
        transaction=str(int(locked['transaction_id']) % 2**32))]
    renewed = current['lease_expires_at_us'] - 20_000_000
    held, locked_at, release = fixture['held_observed_us'], locked['locked_at_us'], fixture['release_before_us']
    at, after_signal = observed['observed_at_us'], fixture['signal']['after_us']
    assert all(type(t) is int for t in (held, locked_at, release, at, after_signal))
    assert max(current['acquired_at_us'], renewed) <= held <= locked_at <= release
    assert release <= instant(observed['query_start']) <= at <= after_signal
    assert after_signal < renewed + 1_000_000, 'SIGTERM missed observed one-second envelope'
    assert after_signal <= clock['after_us'] < current['lease_expires_at_us']
    for t in (held, locked_at, release, at, after_signal):
        inside_window(t, clock)
    identity = {k: terminal[k] for k in ('pid', 'start_ticks', 'config', 'prefix', 'command')}
    assert fixture['signal'] == dict(name='SIGTERM', identity=identity, returned=True, after_us=after_signal)
    # Holder is itself an application backend and is explicitly excluded from
    # this census. Only the observed Control may linger before holder rollback.
    remaining = fixture['remaining_backends_before_unlock']
    assert len(remaining) in (0, 1)
    if remaining:
        assert remaining == [dict(pid=observed['pid'], backend_start=observed['backend_start'],
            database=database, user='bigbrotr_control', query=observed['query'])]
    assert fixture['rollback'] == dict(sqlstate='00000', rows=0, lines=[])
    ended = fixture['holder_end']
    assert ended['backend'] == holder and ended['absence'] == '0' and ended['joined'] is True
    assert type(ended['wrapper_exit']) is int and 'error' not in ended
    assert fixture['released'] is True


def verify(before, after, native_before, native_after, inputs_before, inputs_after, input_contents,
        response, mode, *, seed, setup, server_window, backend, fixture):
    assert mode == MODE and seed['name'] == 'off'
    window(server_window)
    retirement.seed_checked(seed)
    same(retirement.migrated(seed, setup, server_window), before)
    assert len(before) == len(after) == 72
    projected, current = failed_control(before, after, 'retirement', server_window)
    assert current['epoch'] == 13
    same(before, projected)
    assert native_before == native_after == seed['native']
    assert len(native(native_after)) == 70 and native_after['sequence'] == 85
    assert inputs_before == inputs_after == seed['inputs'] and input_contents == seed['contents']
    database = backend_checked(backend, current, server_window)
    terminal = response_checked(response)
    fixture_checked(fixture, before, current, server_window, database, terminal, seed)
    return dict(status='PASS', mode=mode, observed_states=2, control_tables=72, native_rows=70,
        native_sequence=85, retained_inputs=6, native_writes=0, target_writes=0,
        control_epoch=13, source_closed=True, producer_retired=False, signal='SIGTERM',
        final_commit_seen=False, timing='Observed UTC envelope after final refresh; no internal timer telemetry')


def negative_controls(**context):
    verify(**context)
    checks = []

    def reject(name, mutate):
        value = copy.deepcopy(context)
        mutate(value)
        try:
            verify(**value)
        except (AssertionError, KeyError, ValueError, TypeError, IndexError, StopIteration):
            checks.append(name)
            return
        raise AssertionError('accepted retirement interruption counterexample: ' + name)

    reject('native-write', lambda c: c['native_after'].update(sequence=86))
    reject('input-byte-changed', lambda c: c['input_contents'].update({next(iter(c['input_contents'])): '00'}))
    reject('source-rewritten', lambda c: only(c['after']['source_streams'], owner_epoch=2).update(xmin='999999'))
    reject('outcome-reattributed', lambda c: only(c['after']['producer_outcomes'], producer_epoch=2,
        request_sequence=2).update(recorder_epoch=13))
    reject('target-partial-retirement', lambda c: only(c['after']['producer_epochs'], producer_epoch=2).update(state='retired'))
    reject('gap-reattributed', lambda c: only(c['after']['acquisition_gaps'], producer_epoch=2).update(issuer_epoch=13))
    reject('job-rewritten', lambda c: only(c['after']['jobs'], job_id='\\x'+JOB).update(xmin='999999'))
    reject('catalog-bypassed', lambda c: c['setup']['catalog_after']['triggers'][0].update(enabled='D'))
    reject('wrong-command', lambda c: c['response']['terminal']['command'].__setitem__(-1, '--close-activated-source'))
    reject('deadline-is-not-signal', lambda c: c['response'].update(stderr='error: initial Ingest deadline; original attempt may need recovery\n'))
    reject('unjoined-child', lambda c: c['response']['terminal'].update(reaped=False))
    reject('unknown-custody', lambda c: c['response'].update(outcome='OUTCOME_UNKNOWN'))
    reject('backend-still-present', lambda c: c['backend']['exact'].update(present=True, stdout='t\n'))
    reject('wrong-holder', lambda c: c['fixture']['blocked'].update(holder_start='2000-01-01T00:00:00+00:00'))
    reject('earlier-phase', lambda c: c['fixture']['blocked'].update(query='SELECT epoch FROM control.owner_epochs FOR UPDATE'))
    reject('no-blocker', lambda c: c['fixture']['blocked'].update(blockers=[]))
    reject('wrong-transaction-lock', lambda c: c['fixture']['blocked']['waiting_locks'][0].update(transaction='1'))
    reject('wrong-signalled-process', lambda c: c['fixture']['signal']['identity'].update(start_ticks=1))
    reject('unreturned-signal', lambda c: c['fixture']['signal'].update(returned=False))
    reject('holder-unjoined', lambda c: c['fixture']['holder_end'].update(joined=False))
    reject('holder-backend-present', lambda c: c['fixture']['holder_end'].update(absence='1'))
    reject('origin-disabled', lambda c: c['fixture']['identity'].update(session_replication_role='replica'))
    reject('late-signal', lambda c: c['fixture']['signal'].update(after_us=c['fixture']['blocked']['control']['lease_expires_at_us']-19_000_000))
    reject('wrong-remaining-tuple', lambda c: c['fixture'].update(remaining_backends_before_unlock=[dict(
        pid=1, backend_start=c['fixture']['blocked']['backend_start'], database=c['backend']['absence']['database'],
        user='bigbrotr_control', query=c['fixture']['blocked']['query'])]))
    reject('proxy-readers-unjoined', lambda c: c['fixture']['wire'].update(readers_joined=False))
    reject('proxy-forced', lambda c: c['fixture']['wire'].update(forced_wrapper_stop=True))
    reject('proxy-missing-receipt', lambda c: c['fixture']['wire'].pop('receipt'))
    reject('holder-before-final-refresh', lambda c: c['fixture'].update(held_observed_us=c['server_window']['before_us']))

    def backdate(c):
        row = only(c['after']['owner_epochs'], role='control', epoch=13)
        row['lease_expires_at_us'] = c['fixture']['signal']['after_us'] + 18_999_999
        c['fixture']['blocked']['control'] = copy.deepcopy(row)

    reject('coherent-backdated-last-refresh', backdate)

    def rewire(c, mutate):
        wire = c['fixture']['wire']
        mutate(wire)
        wire['events'] = [wire[k] for k in ('ready', 'held', 'released', 'receipt')]
        wire['stdout'] = ''.join(json.dumps(e, sort_keys=True) + '\n' for e in wire['events'])
        wire['stdout_bytes'] = len(wire['stdout'].encode())
        wire['stdout_sha256'] = sha(wire['stdout'].encode())

    reject('coherent-final-C7', lambda c: rewire(c, lambda w: w['receipt'].update(
        commits_seen=7, commits_forwarded=7, server_commit_complete=7, client_commit_complete=7)))
    reject('coherent-earlier-gate', lambda c: rewire(c, lambda w: [w[k].update(commit_number=5)
        for k in ('ready', 'held', 'released', 'receipt')]))
    reject('coherent-C6-not-forwarded', lambda c: rewire(c, lambda w: w['receipt'].update(
        commits_forwarded=5, server_commit_complete=5, client_commit_complete=5)))
    return dict(status='PASS', count=len(checks), rejected=checks)
