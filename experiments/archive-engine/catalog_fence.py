"""Bounded trusted native catalog experiment. Requires a fresh isolated PG lab.

Run inside the inventoried runner, without a Docker socket. The launch receipt
owns the matching PostgreSQL cluster_name and immutable source/bundle/probe.
No production fencing or Nostr acceptance is inferred from these canary cases.
"""
import argparse
import hashlib
import json
import os
import re
from pathlib import Path
import secrets
import select
import signal
import subprocess
import sys
import time

sys.dont_write_bytecode = True
sys.path.insert(0, '/source')
from tools.archive_bundle import build_environment, native_target, verify_bundle

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--case-set', choices=['baseline', 'startup', 'guard', 'guard-races', 'guard-privileges'], default='baseline')
parser.add_argument('--probe', type=Path, required=True)
parser.add_argument('--probe-sha256', required=True)
parser.add_argument('--bundle', type=Path, required=True)
parser.add_argument('--root', type=Path, required=True)
parser.add_argument('--admin-password-file', type=Path, required=True)
parser.add_argument('--cluster', required=True)
args = parser.parse_args()
assert args.cluster.startswith('bigbrotr-fence-') and args.cluster.removeprefix('bigbrotr-fence-').isalnum()
assert hashlib.sha256(args.probe.read_bytes()).hexdigest() == args.probe_sha256
verify_bundle(args.bundle, native_target())
root = args.root
root.mkdir(mode=0o700, exist_ok=False)
data = root / 'data'
data.mkdir(mode=0o700)
password = args.admin_password_file.read_text().strip()
assert password and all(c.isalnum() or c in '-_' for c in password)
passfile = root / 'admin.pgpass'
passfile.write_text(f'pg:5432:catalog_fence:postgres:{password}\n')
passfile.chmod(0o600)
pg_env = dict(os.environ, PGHOST='pg', PGPORT='5432', PGDATABASE='catalog_fence',
              PGUSER='postgres', PGPASSFILE=str(passfile), PGCONNECT_TIMEOUT='5',
              PGOPTIONS='-c statement_timeout=5000 -c lock_timeout=1000')
pg_env.pop('PGPASSWORD', None)
worker_env = {'LC_ALL': 'C', **build_environment(args.bundle)}
children = []
roles = []
relays = []
sessions = []
cases = []
report = {'status': 'RUNNING', 'probe_sha256': args.probe_sha256,
          'scope': 'Synthetic real Rust DuckLake/PG fencing investigation only',
          'case_set': args.case_set, 'cases': cases}


def save():
    (root / 'result.json').write_text(json.dumps(report, indent=2) + '\n')


def sql(query):
    result = subprocess.run(['psql', '-X', '--no-password', '-t', '-A', '-v', 'ON_ERROR_STOP=1'],
                            input=query, text=True, env=pg_env, capture_output=True, timeout=12)
    if result.returncode:
        # Never print a SQL string or driver diagnostic containing lab passwords.
        raise RuntimeError('PostgreSQL lab command refused')
    assert len(result.stdout) < 65536
    return result.stdout.strip()


def dsn_file(name, user, secret):
    path = root / (name + '.dsn')
    path.write_text(f'host=pg port=5432 dbname=catalog_fence user={user} password={secret} sslmode=disable connect_timeout=5')
    path.chmod(0o600)
    return path


def job_role():
    role = 'bb_fence_' + secrets.token_hex(10)
    secret = secrets.token_urlsafe(24)
    sql(f"CREATE ROLE {role} LOGIN PASSWORD '{secret}' IN ROLE bb_fence_writer;")
    roles.append(role)
    return role, dsn_file(role, role, secret)


def backends(role):
    return json.loads(sql("SELECT coalesce(json_agg(json_build_object('pid',pid,'start',backend_start::text,'state',state)), '[]') "
                          f"FROM pg_stat_activity WHERE usename='{role}'"))


class Probe:
    def __init__(self, mode, dsn, batch):
        temp = root / ('temp-' + secrets.token_hex(8))
        temp.mkdir(mode=0o700)
        self.process = subprocess.Popen([str(args.probe), mode, str(args.bundle), str(temp),
                                         str(data), str(dsn), str(batch)], env=worker_env,
                                        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.buffer = bytearray()
        children.append(self.process)

    def message(self):
        until = time.monotonic() + 15
        while b'\n' not in self.buffer:
            assert len(self.buffer) < 4096
            timeout = until - time.monotonic()
            if timeout <= 0 or not select.select([self.process.stdout], [], [], timeout)[0]:
                raise AssertionError('native barrier deadline')
            data = os.read(self.process.stdout.fileno(), 4096 - len(self.buffer))
            if not data:
                report['unexpected_child_exit'] = self.finish(False)
                raise AssertionError('native process exited before its barrier')
            self.buffer.extend(data)
        line, _, remaining = self.buffer.partition(b'\n')
        self.buffer = remaining
        return json.loads(line)

    def send(self, data):
        self.process.stdin.write(data)
        self.process.stdin.flush()

    def finish(self, success=True):
        self.process.stdin.close()
        self.process.stdin = None
        stdout, stderr = self.process.communicate(timeout=15)
        assert not stdout and not self.buffer
        assert len(stderr) < 4096
        assert (self.process.returncode == 0) == success
        if success:
            assert not stderr
        phase = None
        if stderr:
            match = re.fullmatch(rb'catalog fence experiment failed: ([a-z-]+)\n', stderr)
            assert match, 'unexpected native diagnostic (withheld)'
            phase = match[1].decode()
        return {'exit': self.process.returncode, 'phase': phase}


def read(batch, admin):
    probe = Probe('read', admin, batch)
    value = probe.message()
    probe.finish()
    assert value['stage'] == 'read'
    return value['counts']


def writer(dsn, batch, mode='write'):
    probe = Probe(mode, dsn, batch)
    assert probe.message() == {'stage': 'ready', 'counts': [0, 0, 0]}
    return probe


def commit(probe):
    probe.send(b'publish\n')
    assert probe.message() == {'stage': 'committed'}
    return probe.finish()


def baseline_cases(admin):
    report['active_case'] = 'unfenced-duplicate-negative-control'
    # Independent negative control: both read absent before either appends.
    _, a_dsn = job_role()
    _, b_dsn = job_role()
    a, b = writer(a_dsn, 1), writer(b_dsn, 1)
    commit(b)
    commit(a)
    assert read(1, admin) == [2, 2, 2]
    cases.append({'case': 'unfenced-duplicate-negative-control', 'counts': [2, 2, 2], 'status': 'PASS'})
    save()

    report['active_case'] = 'sealed-login-live-transaction'
    # Sealing login alone must not be mistaken for killing a live transaction.
    role, a_dsn = job_role()
    a = writer(a_dsn, 2)
    initial = backends(role)
    assert initial
    sql(f'ALTER ROLE {role} NOLOGIN;')
    denied = Probe('read', a_dsn, 2)
    refusal = denied.finish(False)
    assert refusal['phase'] == 'attach'
    assert backends(role), 'live sessions disappeared before the actual test'
    commit(a)
    assert read(2, admin) == [1, 1, 1]
    cases.append({'case': 'sealed-login-live-transaction', 'backends_before': initial,
                  'new_connection': refusal, 'counts': [1, 1, 1], 'status': 'PASS'})
    save()

    report['active_case'] = 'terminated-backends-paused-old-writer'
    # Positively terminate the recorded backend set while the old client is paused.
    role, a_dsn = job_role()
    a = writer(a_dsn, 3)
    a.process.send_signal(signal.SIGSTOP)
    until = time.monotonic() + 2
    while '\nState:\tT (stopped)\n' not in Path(f'/proc/{a.process.pid}/status').read_text():
        assert time.monotonic() < until
        time.sleep(.002)
    sql(f'ALTER ROLE {role} NOLOGIN;')
    initial = backends(role)
    assert initial
    for backend in initial:
        got = sql("SELECT pg_terminate_backend(pid,5000) FROM pg_stat_activity "
                  f"WHERE pid={backend['pid']} AND backend_start::text='{backend['start']}' AND usename='{role}'")
        assert got == 't'
    assert backends(role) == []
    _, b_dsn = job_role()
    b = writer(b_dsn, 3)
    commit(b)
    a.process.send_signal(signal.SIGCONT)
    a.send(b'publish\n')
    stale = a.finish(False)
    assert read(3, admin) == [1, 1, 1]
    assert backends(role) == []
    cases.append({'case': 'terminated-backends-paused-old-writer', 'backends': initial,
                  'stale_result': stale, 'counts': [1, 1, 1], 'status': 'PASS'})
    save()

    report['active_case'] = 'committed-acknowledgement-lost'
    role, a_dsn = job_role()
    a = writer(a_dsn, 4, 'lost-ack')
    sql(f'ALTER ROLE {role} NOLOGIN;')
    a.send(b'publish\n')
    assert a.message() == {'stage': 'before-ack'}
    a.process.kill()
    assert a.process.wait(timeout=5) == -signal.SIGKILL
    until = time.monotonic() + 5
    while backends(role):
        assert time.monotonic() < until
        time.sleep(.02)
    assert read(4, admin) == [1, 1, 1]
    cases.append({'case': 'committed-acknowledgement-lost', 'counts': [1, 1, 1], 'status': 'PASS'})


def startup_cases(admin):
    from catalog_startup_relay import StartupRelay

    for batch, kill in [(5, False), (6, True)]:
        name = "startup-reaped-client" if kill else "startup-live-native-negative-control"
        report["active_case"] = name
        role, path = job_role()
        relay = StartupRelay()
        relays.append(relay)
        dsn = path.read_text().replace("host=pg port=5432", f"host=127.0.0.1 port={relay.port}")
        path.write_text(dsn.replace("connect_timeout=5", "connect_timeout=15") +
                        " gssencmode=disable options='-c post_auth_delay=6'")
        a = Probe("write", path, batch)
        # Queue publication in the parent pipe before the native connection is
        # ready. Only the real native client may turn it into catalog SQL.
        a.send(b"publish\n")
        assert relay.proof_sent.wait(15), "actual SCRAM proof deadline"
        # Trigger the inspected post-auth gap. This wait is never a fence or an
        # assumption about completion: activity, native result and wire ordering
        # are measured independently below.
        time.sleep(.5)
        assert a.process.poll() is None and not relay.done.is_set()
        assert backends(role) == []
        sql(f"ALTER ROLE {role} NOLOGIN;")
        assert sql(f"SELECT rolcanlogin FROM pg_roles WHERE rolname='{role}'") == "f"
        assert backends(role) == []
        direct = root / (role + '-direct.dsn')
        direct.write_text(dsn.replace(f'host=127.0.0.1 port={relay.port}', 'host=pg port=5432'))
        direct.chmod(0o600)
        denied = Probe('read', direct, batch)
        refusal = denied.finish(False)
        assert refusal['phase'] == 'attach'
        if kill:
            killed_at = time.monotonic()
            a.process.kill()
            assert a.process.wait(timeout=5) == -signal.SIGKILL
            reaped_seconds = time.monotonic() - killed_at
            assert not a.process.stdout.read() and not a.process.stderr.read()
            a.process.stdin.close()
            a.process.stdout.close()
            a.process.stderr.close()
            # Every activity query gets a fresh psql connection/transaction.
            observed = backends(role)
            for backend in observed:
                assert sql("SELECT pg_terminate_backend(pid,5000) FROM pg_stat_activity "
                           f"WHERE pid={backend['pid']} AND backend_start::text='{backend['start']}' "
                           f"AND usename='{role}'") == 't'
            assert backends(role) == []
            # Replacement can commit before the old server finishes startup.
            _, replacement = job_role()
            commit(writer(replacement, batch))
            assert not relay.done.is_set(), 'fault window did not overlap replacement'
            outcome = {'exit': -signal.SIGKILL, 'reaped_seconds': reaped_seconds,
                       'backends_at_exclusion': observed}
        else:
            assert a.message() == {'stage': 'ready', 'counts': [0, 0, 0]}
            assert a.message() == {'stage': 'committed'}
            outcome = a.finish()
        wire = relay.finish()
        assert wire['queries_before_ready'] == 0
        if kill:
            assert wire['queries_after_ready'] == 0
            assert wire['client_eof_seconds'] < wire['ready_seconds']
        else:
            assert wire['queries_after_ready'] > 0
        assert backends(role) == []
        assert read(batch, admin) == [1, 1, 1]
        cases.append({'case': name, 'status': 'PASS', 'new_connection': refusal,
                      'old_client': outcome, 'wire': wire, 'counts': [1, 1, 1]})
        save()


def guard_cases(admin):
    # Candidate PG commit-boundary enforcement; no product selection is implied.
    sql(Path(__file__).with_name('catalog_commit_guard.sql').read_text())
    assert sql("SELECT count(*) FROM pg_trigger WHERE tgname='bigbrotr_writer_guard' AND tgenabled='O'") == '28'
    for batch, startup in [(7, False), (8, True)]:
        name = 'guard-invisible-live-client' if startup else 'guard-ready-live-client'
        report['active_case'] = name
        role, path = job_role()
        sql(f"UPDATE bb_fence_guard.owner SET login='{role}' WHERE singleton")
        relay = None
        if startup:
            from catalog_startup_relay import StartupRelay
            relay = StartupRelay()
            relays.append(relay)
            dsn = path.read_text().replace('host=pg port=5432', f'host=127.0.0.1 port={relay.port}')
            path.write_text(dsn.replace('connect_timeout=5', 'connect_timeout=15') +
                            " gssencmode=disable options='-c post_auth_delay=6'")
            a = Probe('write', path, batch)
            assert relay.proof_sent.wait(15)
            time.sleep(.5)  # Inject the gap; never used as exclusion evidence.
            assert backends(role) == [] and a.process.poll() is None
        else:
            a = writer(path, batch)
            assert backends(role) and a.process.poll() is None
        replacement_role, replacement = job_role()
        sql(f"UPDATE bb_fence_guard.owner SET login='{replacement_role}' WHERE singleton")
        # Keep the old login/client/backend alive: only the actual metadata guard
        # can prevent the old native transaction from committing after takeover.
        assert sql(f"SELECT rolcanlogin FROM pg_roles WHERE rolname='{role}'") == 't'
        commit(writer(replacement, batch))
        if startup:
            ready = a.message()
            assert ready == {'stage': 'ready', 'counts': [1, 1, 1]}
        a.send(b'publish\n')
        stale = a.finish(False)
        assert stale['phase'] == 'commit'
        wire = relay.finish() if relay else None
        if wire:
            assert wire['queries_before_ready'] == 0 and wire['queries_after_ready'] > 0
        assert read(batch, admin) == [1, 1, 1]
        assert backends(role) == []
        cases.append({'case': name, 'status': 'PASS', 'stale_result': stale,
                      'old_login_retained': True, 'old_client_killed': False,
                      'counts': [1, 1, 1], 'wire': wire})
        save()


def guard_races(admin):
    from catalog_pg_session import PgSession
    sql(Path(__file__).with_name('catalog_commit_guard.sql').read_text())
    sql("""CREATE TABLE bb_fence_guard.pause(login name);
    CREATE FUNCTION bb_fence_guard.pause_commit() RETURNS trigger LANGUAGE plpgsql
    SECURITY DEFINER SET search_path=pg_catalog AS $$ BEGIN
      IF EXISTS(SELECT 1 FROM bb_fence_guard.pause WHERE login=session_user) THEN
        PERFORM pg_advisory_xact_lock(73117, 19);
      END IF; RETURN NULL; END; $$;
    REVOKE ALL ON FUNCTION bb_fence_guard.pause_commit() FROM PUBLIC;
    CREATE TRIGGER zz_lab_pause BEFORE INSERT ON public.ducklake_snapshot
      FOR EACH STATEMENT EXECUTE FUNCTION bb_fence_guard.pause_commit();""")

    def session():
        env = dict(pg_env, PGOPTIONS='-c statement_timeout=20000 -c lock_timeout=15000')
        value = PgSession(env, children)
        sessions.append(value)
        return value

    def blocked(waiter, holder):
        until = time.monotonic() + 5
        while True:
            row = sql("SELECT count(*) FROM pg_stat_activity WHERE "
                      f"pid={waiter['pid']} AND backend_start::text='{waiter['start']}' "
                      f"AND {holder['pid']} = ANY(pg_blocking_pids(pid)) AND wait_event_type='Lock' "
                      f"AND EXISTS(SELECT 1 FROM pg_stat_activity h WHERE h.pid={holder['pid']} "
                      f"AND h.backend_start::text='{holder['start']}')")
            if row == '1':
                locks = json.loads(sql("SELECT json_agg(json_build_object('pid',pid,'type',locktype,"
                                       "'mode',mode,'granted',granted,'relation',relation::regclass::text)) "
                                       f"FROM pg_locks WHERE pid IN ({waiter['pid']},{holder['pid']})"))
                return {'waiter': waiter, 'holder': holder, 'locks': locks}
            assert time.monotonic() < until, 'expected lock schedule absent'
            time.sleep(.01)

    for batch, writer_first, rollback in [(9, True, False), (10, True, True),
                                           (11, False, False), (12, False, True)]:
        name = f"guard-race-{'writer' if writer_first else 'takeover'}-first-{'rollback' if rollback else 'commit'}"
        report['active_case'] = name
        role, path = job_role()
        successor_role, successor = job_role()
        sql(f"UPDATE bb_fence_guard.owner SET login='{role}' WHERE singleton;")
        if writer_first:
            # Visible even in the native client's already-open fixed snapshot.
            sql(f"INSERT INTO bb_fence_guard.pause VALUES ('{role}');")
        a = writer(path, batch)
        [backend] = backends(role)
        takeover = session()
        takeover.execute('BEGIN;')
        replacement = f"UPDATE bb_fence_guard.owner SET login='{successor_role}' WHERE singleton;"
        schedule = []
        if writer_first:
            barrier = session()
            barrier.execute('BEGIN; SELECT pg_advisory_xact_lock(73117,19);')
            a.send(b'publish\n')
            schedule.append(blocked(backend, barrier.backend))
            takeover.start(replacement)
            schedule.append(blocked(takeover.backend, backend))
            if rollback:
                a.process.kill()
                assert a.process.wait(timeout=5) == -signal.SIGKILL
                assert sql("SELECT pg_terminate_backend(pid,5000) FROM pg_stat_activity "
                           f"WHERE pid={backend['pid']} AND backend_start::text='{backend['start']}' "
                           f"AND usename='{role}'") == 't'
                assert backends(role) == []
                old_result = {'exit': -signal.SIGKILL, 'backend_terminated': True}
                barrier.execute('COMMIT;')
            else:
                barrier.execute('COMMIT;')
                assert a.message() == {'stage': 'committed'}
                old_result = a.finish()
            takeover.finish()
            takeover.execute('COMMIT;')
            expected_before = [0, 0, 0] if rollback else [1, 1, 1]
            observed_before = read(batch, admin)
            assert observed_before == expected_before
            if rollback:
                commit(writer(successor, batch))
        else:
            takeover.execute(replacement)
            a.send(b'publish\n')
            schedule.append(blocked(backend, takeover.backend))
            takeover.execute('ROLLBACK;' if rollback else 'COMMIT;')
            if rollback:
                assert a.message() == {'stage': 'committed'}
                old_result = a.finish()
            else:
                old_result = a.finish(False)
                assert old_result['phase'] == 'commit'
            expected_before = [1, 1, 1] if rollback else [0, 0, 0]
            observed_before = read(batch, admin)
            assert observed_before == expected_before
            if not rollback:
                commit(writer(successor, batch))
        assert read(batch, admin) == [1, 1, 1]
        assert backends(role) == []
        while sessions:
            sessions.pop().close(sql)
        sql('DELETE FROM bb_fence_guard.pause;')
        cases.append({'case': name, 'status': 'PASS', 'schedule': schedule,
                      'old_result': old_result, 'before_replay': observed_before,
                      'final_counts': [1, 1, 1]})
        save()


def guard_privileges(admin):
    from catalog_pg_session import PgSession
    sql(Path(__file__).with_name('catalog_commit_guard.sql').read_text())
    role, path = job_role()
    successor_role, successor = job_role()
    sql(f"UPDATE bb_fence_guard.owner SET login='{successor_role}' WHERE singleton;")
    secret = next(part[9:] for part in path.read_text().split() if part.startswith('password='))
    passfile = root / (role + '.pgpass')
    with os.fdopen(os.open(passfile, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'w') as stream:
        stream.write(f'pg:5432:catalog_fence:{role}:{secret}\n')
    session = PgSession(dict(pg_env, PGUSER=role, PGPASSFILE=str(passfile)), children)
    sessions.append(session)
    refusals = [
        ('write-owner', 'UPDATE bb_fence_guard.owner SET login=session_user;'),
        ('delete-owner', 'DELETE FROM bb_fence_guard.owner;'),
        ('call-guard', 'SELECT bb_fence_guard.require_owner();'),
        ('disable-trigger', 'ALTER TABLE public.ducklake_snapshot DISABLE TRIGGER bigbrotr_writer_guard;'),
        ('drop-trigger', 'DROP TRIGGER bigbrotr_writer_guard ON public.ducklake_snapshot;'),
        ('drop-table', 'DROP TABLE public.ducklake_snapshot;'),
        ('truncate-table', 'TRUNCATE public.ducklake_snapshot;'),
        ('create-table', 'CREATE TABLE public.unguarded(i integer);'),
        ('create-schema', 'CREATE SCHEMA unguarded;'),
        ('replication-role', 'SET session_replication_role=replica;'),
        ('session-authorization', 'SET SESSION AUTHORIZATION postgres;'),
        ('set-admin-role', 'SET ROLE postgres;'),
        ('set-current-writer-role', f'SET ROLE {successor_role};'),
        ('escalate-role', f'ALTER ROLE {role} SUPERUSER;'),
    ]
    for name, query in refusals:
        report['active_case'] = 'guard-privilege-' + name
        session.execute(query, '42501')
        cases.append({'case': report['active_case'], 'status': 'PASS', 'sqlstate': '42501'})
    names = json.loads(sql("SELECT json_agg(tablename ORDER BY tablename) FROM pg_tables WHERE schemaname='public'"))
    assert len(names) == 28
    for name in names:
        assert re.fullmatch('ducklake_[a-z_]+', name)
        # A zero-row DML still has to invoke a STATEMENT guard on every table.
        session.execute(f'DELETE FROM public.{name} WHERE false;', '55000')
        cases.append({'case': 'guard-coverage-' + name, 'status': 'PASS', 'sqlstate': '55000'})
    session.execute('SET ROLE bb_fence_writer;')
    assert session.execute('SELECT current_user,session_user;') == [f'bb_fence_writer|{role}']
    session.execute('DELETE FROM public.ducklake_metadata WHERE false;', '55000')
    session.execute('RESET ROLE;')
    session.execute('CREATE TEMP TABLE owner(singleton boolean, login name);')
    session.execute(f"INSERT INTO owner VALUES(true,'{role}');")
    session.execute('SET search_path=pg_temp,public,bb_fence_guard;')
    session.execute('DELETE FROM public.ducklake_metadata WHERE false;', '55000')
    cases.append({'case': 'guard-role-and-search-path-shadow', 'status': 'PASS', 'sqlstate': '55000'})
    # Native publication with the authorized role is a positive control after
    # every refusal; missing broad permissions must not explain the stale result.
    commit(writer(successor, 13))
    assert read(13, admin) == [1, 1, 1]
    assert sql("SELECT count(*) FROM pg_trigger WHERE tgname='bigbrotr_writer_guard' AND tgenabled='O'") == '28'
    assert sql("SELECT count(*) FROM pg_tables WHERE schemaname='public'") == '28'
    assert sql(f"SELECT login::text FROM bb_fence_guard.owner WHERE singleton") == successor_role
    cases.append({'case': 'guard-authorized-native-control-after-refusals', 'status': 'PASS', 'counts': [1, 1, 1]})
    session.close(sql)
    sessions.clear()
    save()


try:
    report['active_case'] = 'initialization'
    assert sql('SHOW cluster_name') == args.cluster, 'unowned PostgreSQL cluster'
    assert sql("SELECT count(*) FROM pg_tables WHERE schemaname='public'") == '0'
    report['postgres_version'] = sql('SHOW server_version')
    report['psql_version'] = subprocess.check_output(['psql', '--version'], text=True).strip()
    admin = dsn_file('admin', 'postgres', password)
    init = Probe('init', admin, 1)
    assert init.message() == {'stage': 'initialized', 'version': 'v1.5.5'}
    init.finish()
    sql('CREATE ROLE bb_fence_writer NOLOGIN; GRANT USAGE,CREATE ON SCHEMA public TO bb_fence_writer; '
        'GRANT ALL ON ALL TABLES IN SCHEMA public TO bb_fence_writer; '
        'GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO bb_fence_writer;')

    if args.case_set == 'baseline':
        baseline_cases(admin)
    elif args.case_set == 'startup':
        startup_cases(admin)
    elif args.case_set == 'guard':
        guard_cases(admin)
    elif args.case_set == 'guard-races':
        guard_races(admin)
    else:
        guard_privileges(admin)
    report['status'] = 'PASS'
except BaseException as error:
    report.update(status='FAIL', failure=type(error).__name__)
    raise
finally:
    for session in sessions:
        session.close(sql)
    for child in children:
        if child.poll() is None:
            child.kill()
        child.wait(timeout=5)
    for relay in relays:
        relay.thread.join(timeout=35)
        assert not relay.thread.is_alive()
    report['relay_observations'] = [dict(relay.result) for relay in relays]
    report['child_cleanup'] = 'all owned children reaped; relay threads joined'
    save()
print(json.dumps({'status': report['status'], 'cases': len(cases)}))
