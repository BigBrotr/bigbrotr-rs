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
cases = []
report = {'status': 'RUNNING', 'probe_sha256': args.probe_sha256,
          'scope': 'Synthetic real Rust DuckLake/PG fencing investigation only', 'cases': cases}


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
            assert data, 'native process exited before its barrier'
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
    report['status'] = 'PASS'
except BaseException as error:
    report.update(status='FAIL', failure=type(error).__name__)
    raise
finally:
    for child in children:
        if child.poll() is None:
            child.kill()
        child.wait(timeout=5)
    report['child_cleanup'] = 'all owned children reaped'
    save()
print(json.dumps({'status': report['status'], 'cases': len(cases)}))
