"""Installed cancellation at the final, actually observed acquisition job wait.

The one-second UTC envelope is observed laboratory evidence, not an exported
Rust timer. No cancellation result is inferred from the child or socket alone.
"""
import copy
import json
import re
import tomllib

import activated_acquisition_oracle as acquisition
from activated_recovery_faults_oracle import backend_checked, bounded_number, failed_control
from activated_recovery_oracle import inside_window, sha
from initial_succession_oracle import only, same
from installed_empty_oracle import native, window
from installed_live_oracle import instant

MODE = 'installed-activated-acquisition-job-signal'
CASES = (MODE,)
JOB = '42e09b5a41d12027920cb581d7941ec3'
LOCK = f"""SELECT json_build_object('job',to_jsonb(j)||jsonb_build_object('xmin',xmin::text),
    'transaction_id',pg_current_xact_id()::text)
FROM control.jobs j WHERE job_id=decode('{JOB}','hex') FOR UPDATE;"""
CLAIMED = """SELECT substring(j.job_id FROM 1 FOR 17),
           coalesce(j.acquisition_job_protocol=1 AND j.state='queued'
               AND j.checkpoint_revision=0 AND j.input_complete
               AND NOT EXISTS (SELECT 1 FROM control.acquisition_gaps g WHERE g.job_id=j.job_id), false)
      FROM control.jobs j
     WHERE j.dataset_id=$1::bytea AND j.owner_resource_id=$2::bytea
       AND j.owner_epoch=$3::text::numeric AND j.owner_incarnation=$4::bytea
       AND j.kind='acquisition'
     ORDER BY j.job_id LIMIT 65 FOR UPDATE"""


def response_checked(response, mode, database):
    assert response['outcome'] == 'PROCESS_EXITED' and type(response['returncode']) is int
    assert response['returncode'] == 4 and response['stdout'] == ''
    assert response['stderr'] == 'error: initial Ingest cancelled; original attempt may need recovery\n'
    assert not any(k in response for k in ('orchestration_error', 'terminal_error', 'clock_error', 'output_limit_exceeded'))
    bounded_number(response['seconds'], 120)
    terminal = response['terminal']
    assert terminal['exit_code'] == 4 and terminal['forced'] is False and terminal['reaped'] is True
    assert type(terminal['pid']) is int and terminal['pid'] > 0
    assert type(terminal['start_ticks']) is int and terminal['start_ticks'] > 0
    bounded_number(terminal['elapsed_seconds'], 50)
    assert terminal['prefix'] == '/lab/' + mode and terminal['config'] == '/lab/' + database + '.toml'
    assert terminal['command'] == ['/build/bigbrotr', 'ingest', 'recover', '--file', terminal['config'],
        '--abandon-activated-acquisition']
    return terminal


def fixture_checked(fixture, before, current, clock, database, terminal, known):
    assert fixture['kind'] == 'owned-job-row-signal' and fixture['cleanup_errors'] == []
    assert not any(k in fixture for k in ('schedule_error', 'invoke_error', 'clock_error'))
    window(fixture['window'])
    assert fixture['window']['before_us'] <= clock['before_us'] <= clock['after_us'] <= fixture['window']['after_us']
    config = fixture['config']
    assert config['path'] == terminal['config'] and sha(config['text'].encode()) == config['sha256']
    parsed = tomllib.loads(config['text'])
    assert parsed['postgres']['control']['database'] == database
    assert parsed['postgres']['control']['endpoint'] == '127.0.0.1:5432'
    assert parsed['storage']['ingest_root'] == '/lab/' + MODE + '/store'
    assert parsed['topology']['dataset_id'] == before['datasets'][0]['dataset_id'][2:]
    assert parsed['topology']['instance_id'] == before['datasets'][0]['instance_id'][2:]
    for name, value in dict(lease_ttl='20s', renewal_timeout='1s', authorization_ttl='5s',
            graceful_shutdown_timeout='2s').items():
        assert parsed['runtime'][name] == value
    holder = fixture['holder']
    assert set(holder) == {'pid', 'backend_start', 'database', 'user', 'application', 'isolation', 'synchronous_commit'}
    assert type(holder['pid']) is int and holder['pid'] > 0
    assert holder['database'] == database and holder['user'] == 'postgres'
    assert holder['application'] == MODE + '_holder' and holder['isolation'] == 'read committed'
    assert holder['synchronous_commit'] == 'on'
    inside_window(instant(holder['backend_start']), fixture['window'])
    assert fixture['identity'] == dict(session_replication_role='origin', isolation='read committed', user='postgres', role='postgres')
    assert fixture['begin'] == dict(sqlstate='00000', rows=0, lines=[])
    assert fixture['lock_statement'] == LOCK
    answer = fixture['lock_result']
    assert answer['sqlstate'] == '00000' and answer['rows'] == 1
    raw, = answer['lines']
    locked = json.loads(raw)
    assert set(locked) == {'job', 'transaction_id'} and locked['job'] == known['job']
    assert re.fullmatch('[1-9][0-9]*', locked['transaction_id']) and int(locked['transaction_id']) < 2**64
    assert known['job']['job_id'] == '\\x' + JOB
    observed = fixture['blocked']
    assert observed['pid'] == current['control_backend_pid'] and observed['backend_start'] == current['control_backend_start']
    assert observed['database'] == database and observed['user'] == 'bigbrotr_control'
    assert observed['control'] == current and observed['query'].strip() == CLAIMED
    assert observed['holder_pid'] == holder['pid'] != observed['pid']
    assert instant(observed['holder_start']) == instant(holder['backend_start']) and observed['holder_user'] == 'postgres'
    assert observed['wait_event_type'] == 'Lock' and observed['wait_event'] == 'transactionid'
    assert observed['blockers'] == [holder['pid']]
    locks = observed['waiting_locks']
    assert locks == [dict(type='transactionid', mode='ShareLock', relation=None,
        transaction=str(int(locked['transaction_id']) % 2**32))]
    refreshed = current['lease_expires_at_us'] - 20_000_000
    started = instant(observed['query_start'])
    at = observed['observed_at_us']
    after_signal = fixture['signal']['after_us']
    assert type(at) is type(after_signal) is int
    assert max(current['acquired_at_us'], refreshed) <= started <= at <= after_signal
    assert after_signal < refreshed + 1_000_000, 'SIGTERM missed observed one-second envelope'
    assert after_signal <= clock['after_us'] < current['lease_expires_at_us']
    inside_window(at, clock)
    signal = fixture['signal']
    identity = {k: terminal[k] for k in ('pid', 'start_ticks', 'config', 'prefix', 'command')}
    assert signal == dict(name='SIGTERM', identity=identity, returned=True, after_us=after_signal)
    # A joined Rust driver does not promise synchronous server teardown. Before
    # unlocking, only this exact still-blocked tuple may remain; final absence
    # is separately required after holder rollback/join.
    remaining = fixture['remaining_backends_before_unlock']
    assert len(remaining) in (0, 1)
    if remaining:
        assert remaining == [dict(pid=observed['pid'], backend_start=observed['backend_start'],
            database=database, user='bigbrotr_control', query=observed['query'])]
    assert fixture['rollback'] == dict(sqlstate='00000', rows=0, lines=[])
    ended = fixture['holder_end']
    assert ended['backend'] == holder and ended['absence'] == '0' and ended['joined'] is True
    assert type(ended['wrapper_exit']) is int
    assert fixture['released'] is True


def verify(before, after, native_before, native_after, inputs_before, inputs_after, input_contents,
        response, mode, *, seed, setup, server_window, backend, fixture):
    assert mode == MODE and seed['name'] == 'off'
    window(server_window)
    known = acquisition.seed_checked(seed)
    same(acquisition.migrated(seed, setup, server_window), before)
    assert len(before) == len(after) == 72
    projected, current = failed_control(before, after, 'abandonment', server_window)
    assert current['epoch'] == 11
    same(before, projected)
    assert native_before == native_after == seed['native']
    assert len(native(native_after)) == 70 and native_after['sequence'] == 85
    assert inputs_before == inputs_after == seed['inputs'] and input_contents == seed['contents']
    database = backend_checked(backend, current, server_window)
    terminal = response_checked(response, mode, database)
    fixture_checked(fixture, before, current, server_window, database, terminal, known)
    return dict(status='PASS', mode=mode, observed_states=2, control_tables=72, native_rows=70,
        native_sequence=85, retained_inputs=6, native_writes=0, new_gaps=0, cancelled_jobs=0,
        control_epoch=11, source_closed=True, producer_retired=False, signal='SIGTERM',
        timing='observed UTC envelope after last committed refresh; no internal timer telemetry')


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
        raise AssertionError('accepted cancellation counterexample: ' + name)
    reject('native-write', lambda c: c['native_after'].update(sequence=86))
    reject('input-byte-changed', lambda c: c['input_contents'].update({next(iter(c['input_contents'])): '00'}))
    reject('source-rewritten', lambda c: only(c['after']['source_streams'], owner_epoch=2).update(xmin='999999'))
    reject('outcome-reattributed', lambda c: only(c['after']['producer_outcomes'], producer_epoch=2, request_sequence=2).update(recorder_epoch=11))
    reject('job-cancelled', lambda c: only(c['after']['jobs'], job_id='\\x'+JOB).update(state='cancelled', job_revision=4))
    reject('gap-inserted', lambda c: c['after']['acquisition_gaps'].append(dict(c['after']['acquisition_gaps'][0], producer_epoch=2)))
    reject('catalog-bypassed', lambda c: c['setup']['catalog_after']['triggers'][0].update(enabled='D'))
    reject('wrong-command', lambda c: c['response']['terminal']['command'].__setitem__(-1, '--close-activated-source'))
    reject('deadline-is-not-signal', lambda c: c['response'].update(stderr='error: initial Ingest deadline; original attempt may need recovery\n'))
    reject('unjoined-child', lambda c: c['response']['terminal'].update(reaped=False))
    reject('unknown-custody', lambda c: c['response'].update(outcome='OUTCOME_UNKNOWN'))
    reject('backend-still-present', lambda c: c['backend']['exact'].update(present=True, stdout='t\n'))
    reject('wrong-holder', lambda c: c['fixture']['blocked'].update(holder_start='2000-01-01T00:00:00+00:00'))
    reject('wrong-blocked-query', lambda c: c['fixture']['blocked'].update(query='SELECT * FROM control.datasets FOR UPDATE'))
    reject('no-blocker', lambda c: c['fixture']['blocked'].update(blockers=[]))
    reject('wrong-transaction-lock', lambda c: c['fixture']['blocked']['waiting_locks'][0].update(transaction='1'))
    reject('wrong-signalled-process', lambda c: c['fixture']['signal']['identity'].update(start_ticks=1))
    reject('unreturned-signal', lambda c: c['fixture']['signal'].update(returned=False))
    reject('wrong-locked-row', lambda c: c['fixture']['lock_result'].update(lines=[json.dumps(dict(
        json.loads(c['fixture']['lock_result']['lines'][0]), job=dict(acquisition.seed_checked(c['seed'])['job'], owner_epoch=3)))]))
    reject('holder-unjoined', lambda c: c['fixture']['holder_end'].update(joined=False))
    reject('holder-backend-present', lambda c: c['fixture']['holder_end'].update(absence='1'))
    reject('origin-disabled', lambda c: c['fixture']['identity'].update(session_replication_role='replica'))
    reject('late-signal', lambda c: c['fixture']['signal'].update(after_us=c['fixture']['blocked']['control']['lease_expires_at_us']-19_000_000))
    reject('wrong-remaining-tuple', lambda c: c['fixture'].update(remaining_backends_before_unlock=[dict(
        pid=1, backend_start=c['fixture']['blocked']['backend_start'], database=c['backend']['absence']['database'],
        user='bigbrotr_control', query=c['fixture']['blocked']['query'])]))
    def backdate(c):
        row = only(c['after']['owner_epochs'], role='control', epoch=11)
        row['lease_expires_at_us'] = c['fixture']['signal']['after_us'] + 18_999_999
        c['fixture']['blocked']['control'] = copy.deepcopy(row)
    reject('coherent-backdated-last-refresh', backdate)
    return dict(status='PASS', count=len(checks), rejected=checks)
