"""Real installed empty-attempt retirement, with retained external evidence.

The probe only observes native bytes or holds the actual native lock. Every
retirement/refusal is the installed CLI. Unknown custody never reopens a store.
"""
from datetime import datetime
import json
from pathlib import Path
import re
import shutil
import time

import lab
from concurrency_cases import Session
from fault_cases import Process
from installed import wait_for
import initial_history as history
from initial_empty import guarded_fault, upgrade
from initial_empty_locks import guards
from initial_gap import configuration
from initial_succession import census, digest
from installed_empty_oracle import verify, negative_controls

CASES = tuple('installed-empty-' + suffix for suffix in (
    'normal', 'native-first-failure', 'missing-input', 'missing-outcome',
    'old-schema', 'held', 'signal', 'deadline'))

# This finite wrapper owns/reaps the actual executable. It does not implement
# recovery or supply receipts. The outer owned container is the final fallback.
CLIENT = r'''import json
import os
from pathlib import Path
import subprocess
import sys
import time

assert len(sys.argv) == 4 and sys.argv[3] in ('default', 'retire-empty')
config, prefix = map(Path, sys.argv[1:3])
assert config.is_absolute() and prefix.is_absolute()
command = ['/build/bigbrotr', 'ingest', 'recover', '--file', str(config)]
if sys.argv[3] == 'retire-empty':
    command.append('--retire-empty-attempt')

def publish(suffix, value):
    target = Path(str(prefix) + suffix)
    partial = Path(str(target) + '.partial')
    assert not target.exists()
    with partial.open('x') as stream:
        stream.write(json.dumps(value) + '\n')
    os.replace(partial, target)

started = time.monotonic()
with Path(str(prefix) + '.stdout').open('xb') as out, Path(str(prefix) + '.stderr').open('xb') as err:
    child = subprocess.Popen(command, stdout=out, stderr=err, env={})
    forced = False
    identity = dict(pid=child.pid, config=str(config), prefix=str(prefix), command=command)
    try:
        identity['start_ticks'] = int(Path(f'/proc/{child.pid}/stat').read_text().split()[21])
        publish('.pid.json', identity)
        child.wait(timeout=45)
    except BaseException:
        forced = True
        child.kill()
        child.wait(timeout=5)
        raise
    finally:
        publish('.terminal.json', dict(identity, exit_code=child.returncode,
            elapsed_seconds=time.monotonic() - started, forced=forced,
            reaped=child.returncode is not None))
'''


def signal_child(client, identity):
    """One signal to the recorded executable incarnation, never name matching."""
    script = '''import os,signal,json
from pathlib import Path
r=json.loads(%r)
p=Path('/proc')/str(r['pid'])
assert int(p.joinpath('stat').read_text().split()[21]) == r['start_ticks']
assert p.joinpath('cmdline').read_bytes().split(b'\\0') == [v.encode() for v in r['command']] + [b'']
os.kill(r['pid'], signal.SIGTERM)
''' % json.dumps(identity)
    lab.docker('exec', client, 'python3', '-c', script, timeout=5)


def bounded_sql(pg, database, statement, timeout):
    return lab.docker('exec', '-i', pg['postgres'], 'psql', '-X', '-A', '-t', '-q',
        '--no-password', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', database,
        data=statement, timeout=timeout).stdout.strip()


def blocked_backend(pg, database, holder):
    """Observe the actual pre-claim relation wait, with both backend identities."""
    deadline = time.monotonic() + 1
    while True:
        remaining = deadline - time.monotonic()
        assert remaining > 0, 'installed pre-claim wait not observed'
        result = json.loads(bounded_sql(pg, database, f"""SELECT coalesce(json_agg(
            json_build_object('pid',a.pid,'backend_start',a.backend_start,'database',a.datname,
                'user',a.usename,'query',a.query,'wait_event_type',a.wait_event_type,
                'wait_event',a.wait_event,'blockers',pg_blocking_pids(a.pid),
                'holder_start',(SELECT backend_start FROM pg_stat_activity WHERE pid={holder['pid']}),
                'waiting_locks',(SELECT coalesce(json_agg(json_build_object('type',locktype,
                    'mode',mode,'relation',relation::regclass::text,'granted',granted)),'[]')
                    FROM pg_locks WHERE pid=a.pid AND NOT granted))),'[]')
            FROM pg_stat_activity a WHERE a.datname=current_database()
                AND a.usename='bigbrotr_control' AND a.wait_event_type='Lock'
                AND {holder['pid']}=ANY(pg_blocking_pids(a.pid))""", remaining))
        if result:
            observed, = result
            assert observed['database'] == database and 'control.datasets' in observed['query']
            assert observed['waiting_locks']
            assert datetime.fromisoformat(observed['holder_start']) == datetime.fromisoformat(holder['backend_start'])
            return observed
        time.sleep(min(.02, max(0, deadline - time.monotonic())))


def exact_absence(pg, database, expected):
    observations = []
    for backend in expected:
        assert type(backend['pid']) is int and backend['pid'] > 0
        assert backend['database'] == database and re.fullmatch(r'[a-z0-9_]+', database)
        start = backend['backend_start']
        datetime.fromisoformat(start)
        assert "'" not in start
        # Global absence was positively observed first; retain an independent
        # exact-tuple observation rather than treating a closed socket as proof.
        present = bounded_sql(pg, database, f"SELECT EXISTS(SELECT FROM pg_stat_activity "
            f"WHERE pid={backend['pid']} AND backend_start='{start}'::timestamptz "
            f"AND datname='{database}')", 5)
        assert present == 'f', 'original installed backend still present'
        observations.append(dict(backend, present=False))
    return observations


def native_ready(holder):
    with holder.condition:
        assert not holder.overflow and holder.process.poll() is None
        raw = bytes(holder.stdout)
        if not raw.endswith(b'\n'):
            return None
        event = json.loads(raw)
        assert event == {'event': 'native-held'} and not holder.stderr
        return event


def invoke(pg, client, output, database, config, directory, label, mode, owned, fixture):
    prefix = output / label
    holder = None
    sessions = []
    terminal = None
    started = time.monotonic()
    window = {}
    response = dict(returncode=None, stdout='', stderr='', outcome='OUTCOME_UNKNOWN')
    fixture['cleanup_errors'] = []

    def save():
        lab.write_json(output / (label + '-fixture.json'), fixture)
        lab.write_json(output / (label + '-clock.json'), window)

    try:
        if mode == 'installed-empty-held':
            source = json.loads((output / (label + '-before.json')).read_text())['datasets'][0]
            command = ['docker', 'exec', '-i', client, '/build/ingest_bootstrap_probe', 'history-hold',
                database, '/run/secrets/application', '/lab/' + directory.name + '/store',
                source['dataset_id'][2:], source['instance_id'][2:]]
            holder = Process(command, input_pipe=True)
            fixture.update(kind='native-held', command=command, forced=False, joined=False)
            fixture['ready'] = wait_for(lambda: native_ready(holder), 'actual-native-holder', 5)
        elif mode in ('installed-empty-signal', 'installed-empty-deadline'):
            locker = Session(pg, database, label + '_holder', sessions, role='postgres')
            statement = 'BEGIN; LOCK TABLE control.datasets IN ACCESS EXCLUSIVE MODE;'
            fixture.update(kind='sql-wait', holder=locker.backend, lock_statement=statement,
                lock_result=locker.execute(statement), signal=None, released=False)
        save()
        window['before_us'] = history.server_clock(pg, database)
        save()
        command_mode = 'default' if mode == 'installed-empty-default' else 'retire-empty'
        lab.docker('exec', '-d', client, 'python3', '/lab/installed-empty-client.py',
            config, '/lab/' + label, command_mode, timeout=5)
        identity_path = Path(str(prefix) + '.pid.json')
        wait_for(identity_path.exists, 'installed-empty-pid', 5)
        identity = json.loads(identity_path.read_text())
        measured = None
        if sessions:
            fixture['blocked'] = blocked_backend(pg, database, locker.backend)
            fixture['observed_at_us'] = history.server_clock(pg, database)
            measured = time.monotonic()
            save()
            if mode == 'installed-empty-signal':
                signal_child(client, identity)
                fixture['signal'] = 'SIGTERM'
                fixture['signal_identity'] = identity
                save()
        terminal_path = Path(str(prefix) + '.terminal.json')
        wait_for(terminal_path.exists, 'installed-empty-reaped', 8)
        terminal = json.loads(terminal_path.read_text())
        # The clock brackets the real CLI, not the later fixture unlock/capture.
        window['after_us'] = history.server_clock(pg, database)
        if measured is not None:
            fixture['seconds_after_observed_block'] = time.monotonic() - measured
            remaining = json.loads(bounded_sql(pg, database,
                "SELECT coalesce(json_agg(json_build_object('pid',pid,'backend_start',backend_start,"
                "'database',datname,'user',usename,'query',query) ORDER BY pid),'[]') "
                "FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control'", 5))
            fixture['remaining_backends_before_fixture_unlock'] = remaining
            fixture['application_backends_before_fixture_unlock'] = len(remaining)
        if holder:
            fixture['alive_during_cli'] = holder.process.poll() is None
        save()
    finally:
        try:
            if holder:
                try:
                    holder.send('release')
                    fixture['exit_code'] = holder.wait(5)
                    fixture['stdout'] = bytes(holder.stdout).decode()
                    fixture['stderr'] = bytes(holder.stderr).decode()
                    fixture['joined'] = True
                    assert fixture['exit_code'] == 0 and not fixture['stderr']
                except BaseException as error:
                    fixture['forced'] = True
                    fixture['cleanup_errors'].append(type(error).__name__)
                finally:
                    holder.stop()
            for session in reversed(sessions):
                try:
                    fixture['rollback'] = session.execute('ROLLBACK;')
                    fixture['released'] = True
                except BaseException as error:
                    fixture['cleanup_errors'].append(type(error).__name__)
                finally:
                    try:
                        session.close(owned)
                    except BaseException as error:
                        fixture['cleanup_errors'].append(type(error).__name__)
        finally:
            for stream in ('stdout', 'stderr'):
                path = Path(str(prefix) + '.' + stream)
                if path.exists():
                    with path.open('rb') as captured:
                        raw = captured.read(65537)
                    if len(raw) > 65536:
                        fixture['cleanup_errors'].append('OutputLimitExceeded')
                    response[stream] = raw[:65536].decode(errors='replace')
            response['seconds'] = time.monotonic() - started
            response['terminal'] = terminal
            if terminal and terminal['reaped'] and not terminal['forced'] and not fixture['cleanup_errors']:
                response.update(returncode=terminal['exit_code'], outcome='PROCESS_EXITED')
            lab.write_json(output / (label + '-response.json'), response)
            save()
            if response['outcome'] != 'PROCESS_EXITED':
                lab.write_json(output / (label + '-external-unavailable.json'), dict(
                    status='NOT_RUN_UNKNOWN_PROCESS_OUTCOME',
                    reason='Native custody is unresolved; outer owner cleanup must finish before reopening'))
    assert response['outcome'] == 'PROCESS_EXITED', response['outcome']
    return response, window


def run(pg, client, output, rows, clone):
    basis = output.parent.parent / history.SEED / 'bindings'
    store = basis / 'successor-normal/store'
    assert all(digest(basis / name) == value for name, value in history.FROZEN.items())
    retained = json.loads((basis / 'result.json').read_text())
    assert retained['status'] == 'PASS_INITIAL_SUCCESSOR_PREPARATION'
    assert retained['source'] == history.SOURCE and retained['owner'] == history.OWNER
    assert not retained['cleanup_errors'] and not retained['unrelated_changed'] and not retained['missing_owned']
    containers = json.loads(lab.docker('inspect', *lab.docker('ps', '-aq').stdout.split()).stdout)
    old, = [item for item in containers if item['Id'] == retained['client']['id']]
    assert old['Config']['Labels'][retained['label']] == history.OWNER
    assert not old['State']['Running'] and not old['State']['Pid'] and not old['State']['OOMKilled']
    assert not any(item['State']['Running'] and any(str(basis) in mount.get('Source', '')
        for mount in item['Mounts']) for item in containers)
    original = rows(history.DATABASE)
    assert original == json.loads((basis / 'successor-normal-after.json').read_text())
    baseline = json.loads((basis / 'successor-normal-after-native.json').read_text())
    files, inputs = census(store), history.inputs_at(store)
    assert len(original) == 71 and len(baseline['rows']) == 52 and len(inputs) == 4
    assert sum(value['bytes'] for value in files.values()) < 1024**2
    source_response = json.loads((basis / 'successor-normal-response.json').read_text())
    assert source_response['returncode'] == 0 and not source_response['stderr']
    events = [json.loads(line) for line in source_response['stdout'].splitlines()]
    final, = [event for event in events if event['event'] == 'successor-state' and event['stage'] == 'final']
    assert final['rows'] == baseline['rows'] and dict(final['state']['originals']) == inputs
    assert {table: json.loads(value) for table, value in final['state']['control_tables']} == original
    assert final['state']['sequence'] == 62
    seed = dict(control=original, native=baseline, inputs=inputs, sequence=62)
    absence = history.observe_backend_absence(pg, history.DATABASE, output, 'installed-empty-source')
    lab.write_json(output / 'installed-empty-source.json', dict(**seed, files=files,
        frozen=history.FROZEN, backend_absence=absence))
    helper = output / 'installed-empty-client.py'
    with helper.open('x') as stream:
        stream.write(CLIENT)
    pg_id = json.loads(lab.docker('inspect', pg['postgres']).stdout)[0]['Id']

    def owned():
        item, = json.loads(lab.docker('inspect', pg_id).stdout)
        assert item['State']['Running'] and item['Config']['Labels'][lab.LABEL] == pg['owner']

    def dump(label, directory):
        response, _ = history.capture(client, output, label + '-dump', 'dump-state',
            '/lab/' + directory.name + '/store')
        assert response.returncode == 0 and not response.stderr
        value = json.loads(response.stdout)
        assert set(value) == {'event', 'rows', 'sequence'} and value['event'] == 'dump-state'
        assert type(value['sequence']) is int
        lab.write_json(output / (label + '-native.json'), value)
        return value

    def execute(label, mode, database, config, directory, *, predecessor=None,
                fixture=None, removed_input=None):
        history.observe_backend_absence(pg, database, output, label + '-before')
        before, native = rows(database), dump(label + '-before', directory)
        present = history.inputs_at(directory / 'store')
        setup = dict(guards_before=guards(pg, database), removed_input=removed_input,
            fixture=dict(kind='none') if fixture is None else fixture)
        lab.write_json(output / (label + '-before.json'), before)
        lab.write_json(output / (label + '-inputs.json'), present)
        response, window = invoke(pg, client, output, database, config, directory,
            label, mode, owned, setup['fixture'])
        # Driver join is not a PostgreSQL observation. Observe absence before
        # either authoritative final SQL capture or reopening native storage.
        absence = history.observe_backend_absence(pg, database, output, label)
        after = rows(database)
        lab.write_json(output / (label + '-after.json'), after)
        added = [row for row in after['owner_epochs'] if row['role'] == 'control'
            and not any(old['resource_id'] == row['resource_id'] and old['epoch'] == row['epoch']
                for old in before['owner_epochs'])]
        expected = [dict(pid=row['control_backend_pid'], backend_start=row['control_backend_start'],
            database=database, control_epoch=row['epoch']) for row in added]
        if mode in ('installed-empty-signal', 'installed-empty-deadline'):
            blocked = setup['fixture']['blocked']
            # Preserve even an unexpected committed Control row before the
            # independent oracle rejects it. Do not discard native evidence.
            expected.append(dict(pid=blocked['pid'], backend_start=blocked['backend_start'],
                database=database, control_epoch=None))
        backend = dict(database=database, expected=expected,
            observations=exact_absence(pg, database, expected), absence=absence)
        lab.write_json(output / (label + '-backend.json'), backend)
        final_native = dump(label + '-after', directory)
        final_inputs = history.inputs_at(directory / 'store')
        lab.write_json(output / (label + '-after-inputs.json'), final_inputs)
        setup['guards_after'] = guards(pg, database)
        if mode == 'installed-empty-native-first-failure':
            setup['fixture']['hit'] = lab.sql(pg, database,
                'SELECT last_value::text||is_called::text FROM empty_retirement_fixture.source_hit')
        lab.write_json(output / (label + '-setup.json'), setup)
        context = dict(before=before, after=after, native_before=native, native_after=final_native,
            inputs_before=present, inputs_after=final_inputs, response=response, mode=mode,
            seed=seed, server_window=window, backend=backend, predecessor=predecessor, setup=setup)
        lab.write_json(output / (label + '-verification.json'), context)
        checked = verify(**context)
        perturbations = negative_controls(**context)
        return dict(name=label, mode=mode, status='PASS', oracle=checked,
            negative_controls=perturbations, seconds=response['seconds'],
            verification_sha256=digest(output / (label + '-verification.json'))), context

    cases = []
    try:
        for mode in CASES:
            database = clone(mode, history.DATABASE)
            directory = output / mode
            directory.mkdir(mode=0o700)
            shutil.copytree(store, directory / 'store')
            assert dump(mode + '-seed', directory) == dict(baseline, event='dump-state', sequence=62)
            assert history.inputs_at(directory / 'store') == inputs
            config = configuration(pg, client, output, database, directory, original)
            if mode != 'installed-empty-old-schema':
                upgrade(pg, client, output, database, config, mode, rows)
            guard_before = guards(pg, database)
            removed = None
            fixture = None
            if mode == 'installed-empty-missing-input':
                name, = [name for name in inputs if name.endswith('-0000000000000004.cbor')]
                removed = dict(name=name, **inputs[name])
                (directory / 'store/producer-inputs-v1' / name).rename(directory / 'removed-input.cbor')
            if mode == 'installed-empty-missing-outcome':
                statement = 'BEGIN; SET LOCAL session_replication_role=replica; DELETE FROM ' \
                    'control.producer_outcomes WHERE producer_epoch=1 AND request_sequence=3; COMMIT;'
                lab.sql(pg, database, statement)
                fixture = dict(kind='missing-outcome', statement=statement)
            if mode == 'installed-empty-native-first-failure':
                guarded_fault(pg, database, output, mode)
                fixture = dict(kind='native-first-sql-failure',
                    installed=json.loads((output / (mode + '-fault-installed.json')).read_text()))
            attempts = []
            if mode == 'installed-empty-normal':
                attempt, _ = execute(mode + '-default', 'installed-empty-default', database, config, directory)
                attempts.append(attempt)
            attempt, prior = execute(mode, mode, database, config, directory,
                fixture=fixture, removed_input=removed)
            attempts.append(attempt)
            if mode == 'installed-empty-native-first-failure':
                assert guarded_fault(pg, database, output, mode, remove=True) == guard_before
                fixture = dict(kind='fault-removed',
                    removed=json.loads((output / (mode + '-fault-removed.json')).read_text()))
                attempt, _ = execute(mode + '-recovered', 'installed-empty-fresh-completion',
                    database, config, directory, predecessor=prior, fixture=fixture)
                attempts.append(attempt)
            if mode == 'installed-empty-normal':
                attempt, _ = execute(mode + '-terminal', 'installed-empty-terminal-retry',
                    database, config, directory, predecessor=prior)
                attempts.append(attempt)
            if removed:
                retained_input = directory / 'removed-input.cbor'
                assert dict(bytes=retained_input.stat().st_size, sha256=digest(retained_input)) == inputs[name]
            guard_after = guards(pg, database)
            lab.write_json(output / (mode + '-after-guards.json'), guard_after)
            assert guard_before == guard_after
            cases.append(dict(name=mode, status='PASS', attempts=attempts,
                guard_catalog_unchanged=True, removed_input_retained=removed is not None))
            lab.write_json(output / 'installed-empty-cases.json', cases)
    finally:
        preserved = dict(control=rows(history.DATABASE) == original, files=census(store) == files,
            artifacts=all(digest(basis / name) == value for name, value in history.FROZEN.items()))
        lab.write_json(output / 'installed-empty-source-preserved.json', preserved)
        assert all(preserved.values()), preserved
    assert len(cases) == 8 and sum(len(case['attempts']) for case in cases) == 11
    return dict(name='installed-empty-retirement', status='PASS', cases=cases, cli_invocations=11,
        source_lab=str(basis.parent), source_revision=history.SOURCE, source_owner=history.OWNER,
        source_preserved=preserved, source_artifacts=history.FROZEN, helper_sha256=digest(helper),
        scope='installed explicit empty-attempt retirement and fresh recovery preserving all originals',
        not_run=['installed CLI COMMIT loss', 'blocked native syscall and power loss',
            'next activation and restart', 'installed import', 'full M1'])
