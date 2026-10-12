"""Finite actual Control-row races against empty-attempt retirement.

The second writer uses ordinary guarded generic import SQL, not an installed
import executor. Original seeds, guards and immutable attribution stay intact.
"""
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import shutil
import time

import lab
from concurrency_cases import Session
from fault_cases import Process, safe_event
import initial_history as history
from initial_empty import upgrade
from initial_gap import configuration
from initial_gap_policy import GUARDS, require_guards
from initial_succession import census, digest
from initial_empty_locks_oracle import verify, negative_controls

CASES = ('empty-claim-commit', 'empty-claim-rollback',
    'empty-retirement-first', 'empty-control-expiry')
JOB, ACTOR, REQUEST, DIGEST = 'e1' * 16, 'e2' * 16, 'e3' * 16, 'e4' * 32


def literal(value):
    assert isinstance(value, str) and re.fullmatch(r'\\x[0-9a-f]{32}', value)
    return "decode('" + value[2:] + "','hex')"


def guards(pg, database):
    observed = json.loads(lab.sql(pg, database, 'SELECT ' + GUARDS))
    require_guards(observed)
    return observed


def claim_guard(pg, backend, window):
    """Retain only this exact backend's named veto, not unrelated server logs."""
    def instant(microseconds):
        return datetime.fromtimestamp(microseconds / 1_000_000, timezone.utc).isoformat()
    since, until = instant(window['before_us']), instant(window['after_us'])
    response = lab.docker('logs', '--timestamps', '--since', since, '--until', until,
        pg['postgres'], timeout=5)
    output = response.stdout + response.stderr
    assert len(output.encode()) <= 256 * 1024, 'server error observation exceeds bound'
    expected = 'empty attempt Control workflow claims remain unresolved'
    prefix = rf'\[{backend["pid"]}\] ERROR:  '
    pattern = prefix + r'(?:(?P<sqlstate>[0-9A-Z]{5}): )?' + re.escape(expected) + r'$'
    errors = [line for line in output.splitlines() if re.search(pattern, line)]
    contexts = [line for line in output.splitlines() if f'[{backend["pid"]}] CONTEXT:  ' in line
        and re.search(r'function (?:control\.)?require_initial_empty_claims\(control.initial_ingest_successions\)', line)]
    # Default server verbosity need not expose SQLSTATE. Never synthesize one
    # from a message; the observed message/function identifies the actual veto.
    observed = re.search(pattern, errors[0]).group('sqlstate') if len(errors) == 1 else None
    return dict(backend={k: backend[k] for k in ('pid', 'backend_start', 'database')},
        since=since, until=until, observed_sqlstate=observed, errors=errors, contexts=contexts)


def session_identity(session):
    result = session.execute("SELECT json_build_object('pid',pg_backend_pid(),"
        "'database',current_database(),'user',session_user,'role',current_user,"
        "'superuser',(SELECT rolsuper FROM pg_roles WHERE rolname=session_user),"
        "'session_replication_role',current_setting('session_replication_role'),"
        "'isolation',current_setting('transaction_isolation'));")
    value, = [json.loads(line) for line in result['lines']]
    assert value['pid'] == session.backend['pid'] and value['database'] == session.database
    assert value['user'] == value['role'] == 'bigbrotr_control'
    assert value['superuser'] is False and value['session_replication_role'] == 'origin'
    assert value['isolation'] == 'read committed'
    return value


def setup_import(pg, database, output, name, rows, owned):
    before = rows(database)
    assert not any(r['job_id'] == '\\x' + JOB for r in before['jobs'])
    sessions = []
    report = dict(before=before, job_id='\\x' + JOB, principal_id='\\x' + ACTOR,
        request_key='\\x' + REQUEST, request_digest='\\x' + DIGEST,
        guards_before=guards(pg, database), statement=[], transactions=[], status='INCOMPLETE')
    try:
        session = Session(pg, database, name + '_setup', sessions, role='bigbrotr_control')
        report['backend'] = session.backend
        report['identity'] = session_identity(session)
        report['server_window'] = dict(before_us=history.server_clock(pg, database))
        created = history.server_clock(pg, database)
        report['created_at_us'] = created
        statements = ['BEGIN ISOLATION LEVEL READ COMMITTED;',
            "INSERT INTO control.jobs(job_id,dataset_id,principal_id,request_key,request_digest,"
            "kind,config_revision,policy_revision,created_at_us) SELECT "
            f"decode('{JOB}','hex'),dataset_id,decode('{ACTOR}','hex'),decode('{REQUEST}','hex'),"
            f"decode('{DIGEST}','hex'),'import',config_revision,policy_revision,{created} "
            "FROM control.datasets RETURNING to_jsonb(jobs)||jsonb_build_object('xmin',xmin::text);",
            f"UPDATE control.jobs j SET input_complete=true WHERE job_id=decode('{JOB}','hex') "
            "RETURNING to_jsonb(j)||jsonb_build_object('xmin',xmin::text);",
            'COMMIT;']
        for statement in statements:
            report['statement'].append(statement)
            report['transactions'].append(session.execute(statement))
        report['server_window']['after_us'] = history.server_clock(pg, database)
        report['after'] = rows(database)
        report['guards_after'] = guards(pg, database)
        assert report['guards_after'] == report['guards_before']
        report['status'] = 'CAPTURED'
        return report
    finally:
        errors = []
        for session in reversed(sessions):
            try:
                session.close(owned)
            except BaseException as error:
                errors.append(type(error).__name__)
        report['cleanup_errors'] = errors
        lab.write_json(output / (name + '-setup.json'), report)
        assert not errors, errors


class Probe(Process):
    """Continuously drain bounded full captures; gates never rely on sleeps."""
    def __init__(self, arguments):
        super().__init__(arguments, limit=32 * 1024 * 1024)
        self.parsed = []
        self.parsed_offset = 0

    def observe(self):
        with self.condition:
            assert not self.overflow, 'probe output bound exceeded'
            while True:
                end = self.stdout.find(b'\n', self.parsed_offset)
                if end < 0:
                    return
                line = bytes(self.stdout[self.parsed_offset:end])
                self.parsed_offset = end + 1
                event = json.loads(line)
                assert isinstance(event, dict) and isinstance(event.get('event'), str)
                self.parsed.append(event)

    def gate(self, wanted, timeout=10):
        end = time.monotonic() + timeout
        while True:
            self.observe()
            matches = [e for e in self.parsed if e.get('event') == 'successor-empty-gate'
                and e.get('gate') == wanted and e.get('phase') == 'waiting']
            if matches:
                assert len(matches) == 1
                return matches[0]
            assert self.process.poll() is None and time.monotonic() < end, 'probe gate unobserved'
            with self.condition:
                self.condition.wait(.02)


def claim_statement(control, expiry):
    predicate = (f"resource_id={literal(control['resource_id'])} AND epoch={control['epoch']} "
        f"AND process_incarnation={literal(control['process_incarnation'])} AND role='control' AND state='active'")
    if expiry:
        return ('SELECT to_jsonb(o)||jsonb_build_object(\'xmin\',xmin::text) '
            f'FROM control.owner_epochs o WHERE {predicate} FOR SHARE;')
    return ("UPDATE control.jobs j SET state='queued',job_revision=2,"
        f"owner_resource_id={literal(control['resource_id'])},owner_epoch={control['epoch']},"
        f"owner_incarnation={literal(control['process_incarnation'])} "
        f"WHERE job_id=decode('{JOB}','hex') AND state='accepted' AND job_revision=1 "
        "RETURNING to_jsonb(j)||jsonb_build_object('xmin',xmin::text);")


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
    statement = "SELECT json_build_object('observed_at_us',(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint," \
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


def run(pg, client, output, rows, clone):
    basis = output.parent.parent / history.SEED / 'bindings'
    store = basis / 'successor-normal/store'
    assert all(digest(basis / name) == sha for name, sha in history.FROZEN.items())
    retained = json.loads((basis / 'result.json').read_text())
    assert retained['status'] == 'PASS_INITIAL_SUCCESSOR_PREPARATION'
    assert retained['source'] == history.SOURCE and retained['owner'] == history.OWNER
    assert not retained['cleanup_errors'] and not retained['unrelated_changed'] and not retained['missing_owned']
    containers = json.loads(lab.docker('inspect', *lab.docker('ps', '-aq').stdout.split()).stdout)
    previous, = [c for c in containers if c['Id'] == retained['client']['id']]
    assert previous['Config']['Labels'][retained['label']] == history.OWNER
    assert not previous['State']['Running'] and not previous['State']['Pid'] and not previous['State']['OOMKilled']
    assert not any(c['State']['Running'] and any(str(basis) in m.get('Source', '')
        for m in c['Mounts']) for c in containers)
    original = rows(history.DATABASE)
    assert original == json.loads((basis / 'successor-normal-after.json').read_text())
    baseline = json.loads((basis / 'successor-normal-after-native.json').read_text())
    files, inputs = census(store), history.inputs_at(store)
    assert len(original) == 71 and len(baseline['rows']) == 52 and len(inputs) == 4
    assert sum(v['bytes'] for v in files.values()) < 1024**2
    source_events = [json.loads(line) for line in json.loads(
        (basis / 'successor-normal-response.json').read_text())['stdout'].splitlines()]
    final, = [e for e in source_events if e['event'] == 'successor-state' and e['stage'] == 'final']
    assert final['rows'] == baseline['rows'] and dict(final['state']['originals']) == inputs
    assert {t: json.loads(v) for t, v in final['state']['control_tables']} == original
    assert final['state']['sequence'] == 62
    seed = dict(control=original, native=baseline, inputs=inputs, sequence=62)
    source_absence = history.observe_backend_absence(pg, history.DATABASE, output, 'empty-lock-source')
    lab.write_json(output / 'empty-lock-source.json', dict(**seed, files=files,
        frozen=history.FROZEN, backend_absence=source_absence))
    pg_id = json.loads(lab.docker('inspect', pg['postgres']).stdout)[0]['Id']

    def owned():
        item, = json.loads(lab.docker('inspect', pg_id).stdout)
        assert item['State']['Running'] and item['Config']['Labels'][lab.LABEL] == pg['owner']

    def dump(label, directory):
        response, _ = history.capture(client, output, label + '-dump', 'dump', '/lab/' + directory.name + '/store')
        assert response.returncode == 0 and not response.stderr
        (output / (label + '-native.json')).write_text(response.stdout)
        return json.loads(response.stdout)

    cases = []
    try:
        for mode in CASES:
            database = clone(mode, history.DATABASE)
            directory = output / mode
            directory.mkdir(mode=0o700)
            shutil.copytree(store, directory / 'store')
            assert dump(mode + '-seed', directory) == baseline
            assert history.inputs_at(directory / 'store') == inputs
            config = configuration(pg, client, output, database, directory, original)
            upgrade(pg, client, output, database, config, mode, rows)
            setup = setup_import(pg, database, output, mode, rows, owned)
            before = rows(database)
            assert before == setup['after']
            lab.write_json(output / (mode + '-before.json'), before)
            native = dump(mode + '-before', directory)
            present = history.inputs_at(directory / 'store')
            lab.write_json(output / (mode + '-inputs.json'), present)
            result = execute(pg, client, output, database, directory, config, mode,
                rows, owned, dump, seed, setup, before, native, present)
            cases.append(result)
            lab.write_json(output / 'empty-lock-cases.json', cases)
    finally:
        preserved = dict(control=rows(history.DATABASE) == original, files=census(store) == files,
            artifacts=all(digest(basis / name) == sha for name, sha in history.FROZEN.items()))
        lab.write_json(output / 'empty-lock-source-preserved.json', preserved)
        assert all(preserved.values()), preserved
    return dict(name='initial-empty-locks', status='PASS', cases=cases,
        source_lab=str(basis.parent), source_revision=history.SOURCE, source_owner=history.OWNER,
        source_preserved=preserved, source_artifacts=history.FROZEN,
        scope='guarded generic zero-request import claims versus actual empty retirement and Control UTC expiry',
        excluded='historical preparer8 claim, installed import execution, native I/O/power loss, activation and full M1')


def execute(pg, client, output, database, directory, config, mode, rows, owned, dump,
            seed, setup, before, native, inputs):
    sessions, probe, proxy = [], None, None
    schedule = dict(mode=mode, gates=[], blocks=[], expiry=None, claim_guard=None, status='INCOMPLETE')
    wire = dict(status='NOT_RUN')
    window = dict(before_us=history.server_clock(pg, database))
    started = time.monotonic()
    gate_path = output / (mode + '-gate')
    assert not gate_path.with_suffix('.go').exists() and not gate_path.with_suffix('.done').exists()

    def save():
        schedule['wire'] = wire
        lab.write_json(output / (mode + '-schedule.json'), schedule)
        lab.write_json(output / (mode + '-clock.json'), window)
        lab.write_json(output / (mode + '-wire.json'), wire)

    def gate(name, release=False):
        if release:
            path = gate_path.with_suffix('.go' if name == 'before-retirement' else '.done')
            with path.open('x') as stream:
                stream.write('continue\n')
        else:
            probe.gate(name)
        schedule['gates'].append(dict(gate=name, phase='released' if release else 'waiting',
            server_us=history.server_clock(pg, database)))
        save()

    try:
        claimant = Session(pg, database, mode + '_claimant', sessions, role='bigbrotr_control')
        schedule['claimant'] = claimant.backend
        schedule['claimant_identity'] = session_identity(claimant)
        extra = []
        if mode == 'empty-retirement-first':
            source = Path(__file__).resolve().parents[2] / 'experiments/control-migrations/fault_proxy.py'
            proxy = Process(['docker', 'exec', '-i', client, 'python3', '-B',
                '/source/experiments/control-migrations/fault_proxy.py', '--upstream-port', '5432',
                '--mode', 'hold', '--commit-number', '6', '--later-commits', '2',
                '--deadline-seconds', '30'], input_pipe=True)
            ready = proxy.event('ready', 5)
            wire.update(status='RUNNING', ready=ready, proxy_sha256=digest(source))
            assert ready['proxy_sha256'] == digest(source) and ready['listen_host'] == '127.0.0.1'
            assert ready['upstream_port'] == 5432 and ready['commit_number'] == 6
            extra = [str(ready['listen_port'])]
        probe = Probe(['docker', 'exec', client, '/build/ingest_bootstrap_probe', mode,
            database, '/run/secrets/application', config, '/lab/' + gate_path.name, *extra])
        gate('before-retirement')
        backend, = [e for e in probe.parsed if e['event'] == 'successor-backend']
        schedule['control_backend'] = backend
        state, = [e for e in probe.parsed if e['event'] == 'successor-state' and e['stage'] == 'empty-proof']
        proof = {table: json.loads(value) for table, value in state['state']['control_tables']}
        control, = [r for r in proof['owner_epochs'] if r['role'] == 'control' and r['state'] == 'active']
        schedule['control'] = control
        schedule['before_holder'] = rows(database)
        save()
        assert schedule['before_holder'] == proof and control['epoch'] == 10
        assert backend['database'] == database and backend['control_epoch'] == control['epoch']
        expiry = mode == 'empty-control-expiry'
        statement = claim_statement(control, expiry)
        schedule['claim_statement'] = statement
        if expiry:
            observation = dict(lease_expires_at_us=control['lease_expires_at_us'], pre_age_observations=[])
            schedule['expiry'] = observation
            until = time.monotonic() + 23
            while True:
                now = history.server_clock(pg, database)
                remaining = control['lease_expires_at_us'] - now
                observation['pre_age_observations'].append(dict(server_us=now, remaining_us=remaining))
                save()
                assert remaining > 0 and time.monotonic() < until, 'Control pre-age missed its live lease'
                if remaining <= 2_000_000:
                    break
                time.sleep(min(.25, (remaining - 2_000_000) / 1_000_000))
        claimant.execute('BEGIN ISOLATION LEVEL READ COMMITTED;')
        schedule['transaction_identity'] = session_identity(claimant)
        if mode == 'empty-retirement-first':
            gate('before-retirement', True)
            held = proxy.event('held', 5)
            wire['held'] = held
            assert held['commits_seen'] == 6 and held['commits_forwarded'] == 5
            schedule['held'] = rows(database)
            claimant.start(statement)
            schedule['blocks'] = block_observation(pg, database, claimant.backend, backend)
            save()
            proxy.send('release')
            wire['released'] = proxy.event('released', 2)
            schedule['claim_result'] = claimant.expect(claimant.finish(3), rows=1)
            gate('after-retirement')
            schedule['transaction_end_statement'] = 'COMMIT;'
            schedule['transaction_end'] = claimant.execute('COMMIT;')
            schedule['after_holder'] = rows(database)
            schedule['after_holder_phase'] = 'after-claim-before-postdispatch-release'
            save()
            gate('after-retirement', True)
        else:
            schedule['claim_result'] = claimant.execute(statement, rows=1)
            schedule['held'] = rows(database)
            save()
            gate('before-retirement', True)
            schedule['blocks'] = block_observation(pg, database, backend, claimant.backend)
            save()
            if expiry:
                observation['lock_started_us'] = schedule['blocks'][-1]['observed_at_us']
                until = time.monotonic() + 3
                while True:
                    now = history.server_clock(pg, database)
                    if now > control['lease_expires_at_us']:
                        observation['expired_server_us'] = now
                        break
                    assert time.monotonic() < until, 'Control expiry not observed during bounded lock wait'
                    time.sleep(.02)
                observation['released_server_us'] = history.server_clock(pg, database)
                save()
            ending = 'COMMIT;' if mode == 'empty-claim-commit' else 'ROLLBACK;'
            schedule['transaction_end_statement'] = ending
            schedule['transaction_end'] = claimant.execute(ending)
            save()
        code = probe.wait(12)
        probe.observe()
        if proxy:
            wire['receipt'] = proxy.event('receipt', 5)
            wire['exit_code'] = proxy.wait(3)
            wire['events'] = proxy.events
            receipt = wire['receipt']
            assert wire['exit_code'] == 0 and not proxy.stderr
            assert [e['event'] for e in proxy.events] == ['ready', 'held', 'released', 'receipt']
            assert receipt['reason'] in ('frontend-eof', 'backend-eof')
            assert receipt['commit_number'] == 6 and receipt['hold_action'] == 'release'
            assert [receipt[k] for k in ('commits_seen', 'commits_forwarded',
                'server_commit_complete', 'client_commit_complete', 'faults')] == [8, 8, 8, 8, 1]
            wire['status'] = 'PASS'
        assert code == 0 and not probe.stderr
        schedule['status'] = 'CAPTURED'
    except BaseException as error:
        schedule['failure'] = type(error).__name__
        if proxy:
            wire.update(status='INCOMPLETE', failure=type(error).__name__)
        raise
    finally:
        # Persist the full probe response before any external capture assertion.
        code = None if probe is None else probe.process.poll()
        if probe is not None and code is not None:
            # The wrapper has exited; joining readers cannot keep a remote
            # process alive. Preserve a complete response on failed assertions.
            for thread in probe.threads:
                thread.join(1)
        lab.write_json(output / (mode + '-response.json'), dict(returncode=code,
            stdout='' if probe is None else bytes(probe.stdout).decode(errors='replace'),
            stderr='' if probe is None else bytes(probe.stderr).decode(errors='replace'),
            seconds=time.monotonic() - started,
            outcome='UNKNOWN_PROCESS_OUTCOME' if code is None else 'PROCESS_EXITED'))
        try:
            if code is not None:
                window['after_us'] = history.server_clock(pg, database)
                after = rows(database)
                lab.write_json(output / (mode + '-after.json'), after)
                final_native = dump(mode + '-after', directory)
                final_inputs = history.inputs_at(directory / 'store')
                lab.write_json(output / (mode + '-after-inputs.json'), final_inputs)
                if 'after_holder' not in schedule:
                    schedule['after_holder'], schedule['after_holder_phase'] = after, 'after-process'
                if mode == 'empty-claim-commit' and 'control_backend' in schedule:
                    schedule['claim_guard'] = claim_guard(pg, schedule['control_backend'], window)
            else:
                lab.write_json(output / (mode + '-external-unavailable.json'), dict(
                    status='NOT_RUN_UNKNOWN_PROCESS_OUTCOME',
                    reason='Native directory may still be owned; outer owner-checked cleanup is required'))
        finally:
            errors = []
            if probe:
                try:
                    probe.stop()
                except BaseException as error:
                    errors.append('probe-' + type(error).__name__)
            if proxy:
                forced = proxy.process.poll() is None
                try:
                    proxy.stop()
                except BaseException as error:
                    errors.append('proxy-' + type(error).__name__)
                wire.update(forced_wrapper_stop=forced, wrapper_exit=proxy.process.returncode,
                    stderr_nonempty=bool(proxy.stderr))
                try:
                    wire['events'] = [safe_event(json.loads(line)) for line in proxy.stdout.splitlines()]
                except BaseException as error:
                    wire.update(status='INCOMPLETE', metadata_error=type(error).__name__)
                if forced or proxy.stderr:
                    wire['status'] = 'INCOMPLETE'
            for session in reversed(sessions):
                try:
                    session.close(owned)
                except BaseException as error:
                    errors.append(type(error).__name__)
            schedule['cleanup_errors'] = errors
            save()
            assert not errors, errors
    absence = history.observe_backend_absence(pg, database, output, mode)
    final_guards = guards(pg, database)
    lab.write_json(output / (mode + '-after-guards.json'), final_guards)
    context = dict(seed=seed, server_window=window, setup=setup, schedule=schedule)
    verification = dict(events=probe.parsed, original=before, native=native, inputs=inputs,
        mode=mode, **context)
    lab.write_json(output / (mode + '-verification.json'), verification)
    checked = verify(**verification)
    controls = negative_controls(**verification)
    final, = [e for e in probe.parsed if e.get('event') == 'successor-state' and e.get('stage') == 'empty-final']
    assert after == {table: json.loads(value) for table, value in final['state']['control_tables']}
    assert final_native == dict(native, rows=final['rows']) and final_inputs == inputs
    assert final_guards == setup['guards_after']
    assert mode != 'empty-retirement-first' or wire['status'] == 'PASS'
    return dict(name=mode, status='PASS', oracle=checked, negative_controls=controls,
        seconds=time.monotonic() - started, backend_absence=absence,
        guards_unchanged=True, commit_wire=wire, server_window=window,
        verification_sha256=digest(output / (mode + '-verification.json')))
