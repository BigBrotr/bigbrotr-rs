"""Isolated PG16.15 negative control for NOLOGIN plus activity enumeration.

Requires a fresh owned cluster. PostAuthDelay exposes the inspected gap after
login validation and before backend-status publication. A PASS means the unsafe
exclusion inference was disproved; this is not a product fencing implementation.
"""
import argparse
import json
import os
from pathlib import Path
import secrets
import subprocess
import time

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--root', type=Path, required=True)
parser.add_argument('--admin-password-file', type=Path, required=True)
parser.add_argument('--cluster', required=True)
args = parser.parse_args()
assert args.cluster.startswith('bigbrotr-fence-')
root = args.root
root.mkdir(mode=0o700, exist_ok=False)
client = '/usr/lib/postgresql/15/bin/psql'
command = [client, '-X', '--no-password', '-q', '-t', '-A', '-v', 'ON_ERROR_STOP=1']
password = args.admin_password_file.read_text().strip()
assert password and all(c in '0123456789abcdef' for c in password)


def environment(user, secret):
    path = root / (user + '.pgpass')
    with os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'w') as stream:
        stream.write(f'pg:5432:catalog_fence:{user}:{secret}\n')
    return {'LC_ALL': 'C', 'PGHOST': 'pg', 'PGPORT': '5432', 'PGDATABASE': 'catalog_fence',
            'PGUSER': user, 'PGPASSFILE': str(path), 'PGCONNECT_TIMEOUT': '15',
            'PGOPTIONS': '-c statement_timeout=5000 -c lock_timeout=1000'}


admin = environment('postgres', password)


def sql(query):
    result = subprocess.run(command, input=query, text=True, env=admin,
                            capture_output=True, timeout=10)
    if result.returncode:
        raise RuntimeError('administrative lab query refused')
    assert len(result.stdout) < 4096
    return result.stdout.strip()


report = {'status': 'RUNNING', 'scope': 'PG startup negative control; no product fence'}
child = None
try:
    assert sql('SHOW cluster_name') == args.cluster
    assert sql('SHOW server_version_num') == '160015'
    assert sql("SELECT count(*) FROM pg_tables WHERE schemaname='public'") == '0'
    role = 'bb_fence_' + secrets.token_hex(8)
    secret = secrets.token_hex(32)
    sql(f"CREATE ROLE {role} LOGIN PASSWORD '{secret}'; ALTER ROLE {role} SET post_auth_delay=6; "
        f'CREATE TABLE startup_commits(marker integer); GRANT USAGE ON SCHEMA public TO {role}; '
        f'GRANT INSERT ON startup_commits TO {role};')
    worker = environment(role, secret)
    started = time.monotonic()
    child = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE, text=True, env=worker)
    child.stdin.write('INSERT INTO startup_commits VALUES (73) RETURNING marker;\n')
    child.stdin.close()
    child.stdin = None
    # Delay injection is a counterexample trigger, never an exclusion timeout.
    time.sleep(1)
    assert child.poll() is None
    query = f"SELECT count(*) FROM pg_stat_activity WHERE usename='{role}'"
    assert sql(query) == '0'
    sql(f'ALTER ROLE {role} NOLOGIN;')
    assert sql(f"SELECT rolcanlogin FROM pg_roles WHERE rolname='{role}'") == 'f'
    assert sql(query) == '0'
    report['invisible_after_revocation'] = True
    fresh = subprocess.run(command, input='SELECT 1;\n', text=True, env=worker,
                           capture_output=True, timeout=10)
    assert fresh.returncode != 0 and not fresh.stdout
    report['new_connection_refused'] = True
    stdout, stderr = child.communicate(timeout=15)
    assert child.returncode == 0 and stdout.strip() == '73' and not stderr
    assert sql('SELECT count(*) FROM startup_commits WHERE marker=73') == '1'
    assert sql(query) == '0'
    report.update(status='PASS', counterexample=True, late_connection_exit=0,
                  committed_rows_after_empty_activity=1,
                  elapsed_seconds=time.monotonic() - started)
finally:
    if child is not None:
        if child.poll() is None:
            child.kill()
        child.wait(timeout=5)
    if report['status'] != 'PASS':
        report['status'] = 'FAIL'
    report['owned_child_reaped'] = True
    (root / 'result.json').write_text(json.dumps(report, indent=2) + '\n')
print(json.dumps(report))
