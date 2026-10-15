"""Real UTC expiry while the actual retirement query waits on one genuine job.

The fixture only locks an existing row. It does not alter authority, deadlines,
jobs or gaps. Known exits preserve captures; unknown custody forbids native open.
"""
import copy
from datetime import datetime, timezone
import re
import json
from pathlib import Path
import shutil
import time

import lab
import activated_retirement as retirement
import activated_recovery as recovery
from activated_retirement_expiry_oracle import MODE, verify, negative_controls
from concurrency_cases import Session
from initial_empty_locks import Probe, session_identity
from initial_gap import configuration
from initial_history import capture, inputs_at, observe_backend_absence, server_clock
from initial_live import exact_absence
from initial_succession import census, digest

CASES = (MODE,)


def closed_and_joined(response):
    """A docker-exec exit alone does not establish remote native exclusion."""
    if not (response['returncode'] == 0 and response.get('readers_joined') is True
            and response.get('overflow') is False and response['stderr'] == ''):
        return False
    try:
        events = [json.loads(line) for line in response['stdout'].splitlines()]
        assert all(isinstance(event, dict) for event in events)
        claimed, = [e for e in events if e.get('event') == 'successor-control'
            and e.get('stage') == 'retirement-expiry' and e.get('phase') == 'claimed']
        present, absent = [e for e in events if e.get('event') == 'successor-backend']
        assert present['stage'] == 'retirement-expiry' and present['present'] is True
        assert absent == dict(present, present=False)
        tail = events[-9:]
        assert len(tail) == 9
        for position, stage in ((0, 'retirement-expiry-final-before-control-end'),
                (5, 'retirement-expiry-final-after-control-end'), (6, 'retirement-expiry-final')):
            assert tail[position]['event'] == 'successor-state' and tail[position]['stage'] == stage
        for position, stage in ((1, 'control'), (7, 'observer')):
            assert tail[position] == dict(event='retirement-expiry-driver', stage=stage,
                joined=True, failed=False, aborted=False, error=None)
        assert tail[2] == dict(event='successor-control', stage='retirement-expiry', phase='dropped-and-joined',
            resource=claimed['resource'], epoch=claimed['epoch'])
        assert tail[3] == dict(event='retirement-expiry-backend-absence', error=None)
        assert tail[4] == absent
        assert tail[8] == dict(event='retirement-expiry-complete', command=MODE, native='closed',
            control='dropped-and-joined', admission=False, expected_refusal=True)
        return True
    except (AssertionError, KeyError, ValueError, TypeError, IndexError):
        return False


def wait_event(probe, event, *, stage=None, phase=None, timeout=10):
    deadline = time.monotonic() + timeout
    while True:
        probe.observe()
        found = [e for e in probe.parsed if e.get('event') == event
            and (stage is None or e.get('stage') == stage)
            and (phase is None or e.get('phase') == phase)]
        if found:
            assert len(found) == 1
            return found[0]
        assert probe.process.poll() is None and time.monotonic() < deadline, 'required probe event unavailable'
        with probe.condition:
            probe.condition.wait(.01)


def lock_statement(job):
    assert len(job) == 34 and job.startswith('\\x')
    bytes.fromhex(job[2:])
    return ("SELECT to_jsonb(j)||jsonb_build_object('xmin',xmin::text) "
        "FROM control.jobs j WHERE job_id=decode('" + job[2:] + "','hex') FOR UPDATE;")


def block_observation(pg, database, waiter, holder, timeout=2):
    """Observe both backend incarnations and actual blocking, never PID alone."""
    for backend in (waiter, holder):
        assert type(backend['pid']) is int and backend['pid'] > 0
        assert backend['database'] == database
        assert re.fullmatch(r'[0-9T:.+ Z-]+', backend['backend_start'])
    identity = "json_build_object('pid',pid,'backend_start',backend_start::text,'database',datname," \
        "'user',usename,'application',application_name,'query',query,'state',state," \
        "'wait_event_type',wait_event_type,'wait_event',wait_event,'blockers',pg_blocking_pids(pid)," \
        "'backend_xid',backend_xid::text)"
    statement = "SET max_parallel_workers_per_gather=0; SELECT json_build_object('observed_at_us',(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint," \
        f"'waiter',(SELECT {identity} FROM pg_stat_activity WHERE pid={waiter['pid']} AND datname=current_database())," \
        f"'holder',(SELECT {identity} FROM pg_stat_activity WHERE pid={holder['pid']} AND datname=current_database())," \
        "'waiting_locks',(SELECT coalesce(json_agg(json_build_object('type',locktype,'mode',mode," \
        "'transaction',transactionid::text,'relation',relation::regclass::text,'granted',granted)), '[]') " \
        f"FROM pg_locks WHERE pid={waiter['pid']} AND NOT granted));"
    started, samples = time.monotonic(), []
    while True:
        remaining = timeout - (time.monotonic() - started)
        assert remaining > 0, 'required exact Control-row blocking not observed'
        response = lab.docker('exec', '-i', pg['postgres'], 'psql', '-X', '-A', '-t', '-q',
            '--no-password', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', database,
            data=statement, timeout=remaining)
        value = json.loads(response.stdout)
        value['sample_seconds'] = time.monotonic() - started
        for key, expected in [('waiter', waiter), ('holder', holder)]:
            observed = value[key]
            assert observed is not None and observed['database'] == database
            # PostgreSQL renders timestamptz with a space; Rust's JSON constructor
            # renders the same value with T. Compare instants without PID reuse.
            assert observed['pid'] == expected['pid'] and datetime.fromisoformat(
                observed['backend_start']) == datetime.fromisoformat(expected['backend_start'])
        samples.append(value)
        if value['waiter']['wait_event_type'] == 'Lock' and holder['pid'] in value['waiter']['blockers']:
            assert value['waiting_locks'] and value['sample_seconds'] <= timeout
            return samples
        time.sleep(min(.02, max(0, remaining)))


def holder_excluded(schedule):
    holder = schedule.get('holder')
    if not isinstance(holder, dict):
        return False
    closed = holder.get('closed', {})
    return (type(closed.get('wrapper_exit')) is int
        and closed.get('backend') == holder.get('backend') and closed.get('observation') == '0')


def diagnostic(pg, expected, clock):
    since, until = [datetime.fromtimestamp(clock[k] / 1_000_000, timezone.utc).isoformat()
        for k in ('before_us', 'after_us')]
    answer = lab.docker('logs', '--timestamps', '--since', since, '--until', until,
        pg['postgres'], check=False, timeout=5)
    raw = answer.stdout + answer.stderr
    assert len(raw.encode()) <= 256 * 1024
    pattern = rf'\[{expected["pid"]}\] ERROR:  (?:(?P<code>[0-9A-Z]{{5}}): )?activated retirement Control authority expired$'
    errors = [line for line in raw.splitlines() if re.search(pattern, line)]
    contexts = [line for line in raw.splitlines() if f'[{expected["pid"]}] CONTEXT:  ' in line
        and 'function control.lock_activated_initial_retirement(bytea)' in line]
    return dict(backend=expected, since=since, until=until, returncode=answer.returncode,
        observed_sqlstate=re.search(pattern, errors[0]).group('code') if len(errors) == 1 else None,
        errors=errors, contexts=contexts)


def run_all(pg, client, output, rows, clone):
    spec = retirement.SOURCES['off']
    basis = output.parent.parent / spec['run'] / 'bindings'
    store = basis / spec['prefix'] / 'store'
    origin = None

    def state(label, database, directory):
        value = dict(control=rows(database))
        lab.write_json(output / (label + '-sql.json'), value['control'])
        value.update(inputs=inputs_at(directory / 'store'), contents=recovery.contents_at(directory / 'store'))
        lab.write_json(output / (label + '-inputs.json'), dict(inputs=value['inputs'], contents=value['contents']))
        try:
            answer, _ = capture(client, output, label + '-dump', 'dump-state', '/lab/' + directory.name + '/store')
            assert answer.returncode == 0 and not answer.stderr
            value['native'] = json.loads(answer.stdout)
            lab.write_json(output / (label + '-native.json'), value['native'])
        except Exception as error:
            lab.write_json(output / (label + '-state-incomplete.json'), dict(**value,
                native='UNAVAILABLE', status='INCOMPLETE', error=type(error).__name__))
            raise
        lab.write_json(output / (label + '-state.json'), value)
        return value

    def exclude(label, database, directory):
        try:
            return observe_backend_absence(pg, database, output, label)
        except BaseException as error:
            observation = dict(status='OUTCOME_UNKNOWN', native='NOT_RUN', error=type(error).__name__)
            lab.write_json(output / (label + '-external-unavailable.json'), observation)
            for name, read in [('sql', lambda: rows(database)), ('inputs', lambda: dict(
                    inputs=inputs_at(directory / 'store'), contents=recovery.contents_at(directory / 'store')))]:
                try:
                    lab.write_json(output / (label + '-uncertain-' + name + '.json'), read())
                except Exception as problem:
                    observation[name + '_capture_error'] = type(problem).__name__
            lab.write_json(output / (label + '-external-unavailable.json'), observation)
            raise

    def execute(database, config, directory, seed, setup):
        exclude(MODE + '-before', database, directory)
        before = state(MODE + '-before', database, directory)
        setup = dict(copy.deepcopy(setup), catalog_before=retirement.catalog(pg, database))
        schedule = dict(status='INCOMPLETE', gates=[], blocks=[], expired_blocks=[], expiry_observations=[],
            cleanup_errors=[], holder=None)
        sessions, probe = [], None
        clock = dict(before_us=server_clock(pg, database))
        response = dict(outcome='OUTCOME_UNKNOWN', returncode=None, stdout='', stderr='')
        gate = output / (MODE + '-gate')
        assert not gate.with_suffix('.go').exists()
        started = time.monotonic()

        def save():
            lab.write_json(output / (MODE + '-schedule.json'), schedule)
            lab.write_json(output / (MODE + '-clock.json'), clock)

        def owned():
            observed, = json.loads(lab.docker('inspect', pg['postgres']).stdout)
            assert observed['State']['Running'] and observed['Config']['Labels'][lab.LABEL] == pg['owner']

        try:
            holder = Session(pg, database, MODE + '_holder', sessions, role='bigbrotr_control')
            schedule['holder'] = dict(backend=holder.backend, identity=session_identity(holder))
            settings = holder.execute("SELECT json_build_object('statement_timeout',current_setting('statement_timeout'),"
                "'lock_timeout',current_setting('lock_timeout'),'idle_timeout',current_setting('idle_in_transaction_session_timeout'));")
            schedule['holder']['settings'] = settings
            save()
            response['command'] = ['docker', 'exec', client, '/build/ingest_bootstrap_probe', MODE,
                database, '/run/secrets/application', config, '/lab/' + gate.name]
            probe = Probe(response['command'])
            wait_event(probe, 'retirement-expiry-gate', phase='waiting')
            schedule['gates'].append(dict(gate='before-retirement', phase='waiting', server_us=server_clock(pg, database)))
            refreshed = wait_event(probe, 'retirement-expiry-clock', stage='refreshed')
            backend, = [e for e in probe.parsed if e['event'] == 'successor-backend' and e['present']]
            captured, = [e for e in probe.parsed if e['event'] == 'successor-state' and e['stage'] == 'retirement-expiry-final-refreshed']
            expected = {table: json.loads(value) for table, value in captured['state']['control_tables']}
            control, = [r for r in expected['owner_epochs'] if r['role'] == 'control' and r['state'] == 'active']
            schedule.update(control_backend=backend, control=control, before_holder=rows(database))
            save()
            assert schedule['before_holder'] == expected
            gap, = [g for g in expected['acquisition_gaps'] if g['producer_epoch'] == 2]
            job, = [j for j in expected['jobs'] if j['job_id'] == gap['job_id']]
            assert job['kind'] == 'acquisition' and job['state'] == 'cancelled' and job['job_revision'] == 4
            schedule['holder']['begin'] = holder.execute('BEGIN ISOLATION LEVEL READ COMMITTED;')
            statement = lock_statement(job['job_id'])
            schedule['holder']['statement'] = statement
            schedule['holder']['lock'] = holder.execute(statement, rows=1)
            schedule['held'] = rows(database)
            schedule['gates'].append(dict(gate='before-retirement', phase='released', server_us=server_clock(pg, database)))
            save()
            gate.with_suffix('.go').write_text('continue\n')
            dispatch = wait_event(probe, 'retirement-expiry-clock', stage='dispatch', timeout=24)
            schedule['dispatch'] = dispatch
            schedule['blocks'] = block_observation(pg, database, backend, holder.backend, timeout=2)
            save()
            deadline = time.monotonic() + 3
            while True:
                now = server_clock(pg, database)
                schedule['expiry_observations'].append(dict(server_us=now,
                    remaining_us=control['lease_expires_at_us'] - now))
                save()
                if now >= control['lease_expires_at_us'] + 50_000:
                    break
                assert time.monotonic() < deadline, 'real UTC expiry not observed inside lock margin'
                time.sleep(.02)
            schedule['expired_blocks'] = block_observation(pg, database, backend, holder.backend, timeout=1)
            schedule['release_before_us'] = server_clock(pg, database)
            save()
            schedule['holder']['rollback'] = holder.execute('ROLLBACK;')
            schedule['release_after_us'] = server_clock(pg, database)
            save()
            code = probe.wait(12)
            probe.observe()
            schedule['status'] = 'CAPTURED'
            assert code == 0 and not probe.stderr
        except BaseException as error:
            schedule['schedule_error'] = type(error).__name__
        finally:
            # End only this exact fixture backend. Its rollback/absence never
            # substitutes for the native-owning probe's actual process exit.
            for session in reversed(sessions):
                try:
                    session.close(owned)
                    if schedule['holder'] is not None:
                        physical = session.backend
                        query = ("SET max_parallel_workers_per_gather=0; SELECT count(*) FROM pg_stat_activity WHERE pid=" + str(physical['pid'])
                            + " AND backend_start='" + physical['backend_start'] + "'::timestamptz"
                            + " AND datname='" + database + "';")
                        observed_absence = lab.sql(pg, 'postgres', query)
                        schedule['holder']['closed'] = dict(wrapper_exit=session.process.returncode,
                            backend=session.backend, statement=query, observation=observed_absence)
                        assert observed_absence == '0'
                except BaseException as error:
                    schedule['cleanup_errors'].append(type(error).__name__)
            if probe is not None:
                code = probe.process.poll()
                if code is not None:
                    for thread in probe.threads:
                        thread.join(1)
                    response.update(outcome='PROCESS_EXITED', returncode=code,
                        stdout=bytes(probe.stdout).decode(errors='replace'), stderr=bytes(probe.stderr).decode(errors='replace'),
                        readers_joined=not any(t.is_alive() for t in probe.threads), overflow=probe.overflow)
                    if not closed_and_joined(response):
                        response.update(outcome='OUTCOME_UNKNOWN',
                            custody_error='Native close and both driver joins were not positively observed')
            response['seconds'] = time.monotonic() - started
            lab.write_json(output / (MODE + '-response.json'), response)
            if probe is not None:
                # A stopped docker-exec wrapper alone cannot authorize reopen.
                try:
                    probe.stop()
                except BaseException as error:
                    schedule['cleanup_errors'].append('probe-stop:' + type(error).__name__)
            try:
                clock['after_us'] = server_clock(pg, database)
            except Exception as error:
                clock['after_error'] = type(error).__name__
            save()
        if response['outcome'] != 'PROCESS_EXITED' or not holder_excluded(schedule):
            lab.write_json(output / (MODE + '-external-unavailable.json'), dict(status='OUTCOME_UNKNOWN',
                native='NOT_RUN', reason='Finite or holder custody unknown; outer owner must stop its client'))
            for name, read in [('sql', lambda: rows(database)), ('inputs', lambda: dict(
                    inputs=inputs_at(directory / 'store'), contents=recovery.contents_at(directory / 'store')))]:
                try:
                    lab.write_json(output / (MODE + '-uncertain-' + name + '.json'), read())
                except Exception as error:
                    schedule['cleanup_errors'].append(name + '-capture:' + type(error).__name__)
            save()
            raise AssertionError('retirement-expiry native custody unavailable')
        absent = exclude(MODE, database, directory)
        after = state(MODE + '-after', database, directory)
        setup['catalog_after'] = retirement.catalog(pg, database)
        lab.write_json(output / (MODE + '-external.json'), dict(**after, catalog=setup['catalog_after'], absence=absent))
        exact = exact_absence(pg, database, output, MODE, after['control'], before['control'])
        diagnosis = diagnostic(pg, exact['expected'], clock)
        lab.write_json(output / (MODE + '-diagnostic.json'), diagnosis)
        context = dict(events=[json.loads(line) for line in response['stdout'].splitlines()],
            original=before['control'], native=before['native'], inputs=before['inputs'], response=response,
            seed=seed, setup=setup, server_window=clock, backend=dict(exact=exact, absence=absent), schedule=schedule,
            after=after['control'], native_after=after['native'], inputs_after=after['inputs'], input_contents=after['contents'],
            diagnostic=diagnosis)
        lab.write_json(output / (MODE + '-verification.json'), context)
        checked, controls = verify(**context), negative_controls(**context)
        return dict(name=MODE, status='PASS', oracle=checked, negative_controls=controls,
            verification_sha256=digest(output / (MODE + '-verification.json')))

    try:
        frozen = {'result.json': spec['result'], spec['sql_file']: spec['sql'],
            spec['native_file']: spec['native'], spec['verification_file']: spec['verification']}
        assert all(digest(basis / n) == sha for n, sha in frozen.items())
        retained = json.loads((basis / 'result.json').read_text())
        assert all(retained[k] == spec[k] for k in ('source', 'owner', 'status'))
        assert not any(retained[k] for k in ('cleanup_errors', 'unrelated_changed', 'missing_owned'))
        containers = json.loads(lab.docker('inspect', *lab.docker('ps', '-aq').stdout.split()).stdout)
        old, = [c for c in containers if c['Id'] == retained['client']['id']]
        assert old['Config']['Labels'][retained['label']] == spec['owner']
        assert not old['State']['Running'] and not old['State']['Pid'] and not old['State']['OOMKilled']
        assert not any(c['State']['Running'] and any(str(basis) in m.get('Source', '') for m in c['Mounts']) for c in containers)
        absent = observe_backend_absence(pg, spec['database'], output, MODE + '-original')
        original = rows(spec['database'])
        assert original == json.loads((basis / spec['sql_file']).read_text())
        seed = dict(name='off', verification=json.loads((basis / spec['verification_file']).read_text()),
            control=original, native=json.loads((basis / spec['native_file']).read_text()),
            inputs=inputs_at(store), contents=recovery.contents_at(store), catalog=retirement.catalog(pg, spec['database']))
        origin = dict(seed=seed, files=census(store), frozen=frozen, backend_absence=absent,
            client_id=old['Id'], client_state=old['State'], label=retained['label'])
        lab.write_json(output / (MODE + '-original.json'), origin)
        database = clone('retirement-expiry', spec['database'])
        assert rows(database) == original
        directory = output / MODE
        directory.mkdir(mode=0o700)
        shutil.copytree(store, directory / 'store')
        assert state(MODE + '-seed', database, directory) == {k:seed[k] for k in ('control', 'native', 'inputs', 'contents')}
        config = configuration(pg, client, output, database, directory, original)
        setup = dict(migration_sha256=digest(Path(__file__).resolve().parents[2] /
            'migrations/postgres/0037_activated_initial_retirement.sql'),
            migration=retirement.upgrade(pg, client, output, database, config, MODE, rows))
        case = execute(database, config, directory, seed, setup)
    finally:
        if origin is not None:
            old, = json.loads(lab.docker('inspect', origin['client_id']).stdout)
            preserved = dict(control=rows(spec['database']) == seed['control'], files=census(store) == origin['files'],
                inputs=inputs_at(store) == seed['inputs'], contents=recovery.contents_at(store) == seed['contents'],
                catalog=retirement.catalog(pg, spec['database']) == seed['catalog'],
                client=old['State'] == origin['client_state'] and old['Config']['Labels'][origin['label']] == spec['owner'],
                artifacts=all(digest(basis / n) == sha for n, sha in frozen.items()))
            lab.write_json(output / (MODE + '-original-preserved.json'), preserved)
            assert all(preserved.values()), preserved
    return dict(name='activated-retirement-utc-expiry', status='PASS', cases=[case],
        finite_invocations=1, cli_invocations=0, source_preserved=preserved,
        scope='Finite 10s operation observes actual Control UTC expiry across final retirement job lock; no target retirement writes',
        not_run=['installed 1s supervision', 'remote execution cancellation', 'full M1'])
