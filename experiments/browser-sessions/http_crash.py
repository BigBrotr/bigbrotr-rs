#!/usr/bin/env python3
"""Owned native lab only: immediate PG shutdown after the HTTP fixture marker."""
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import time

root = Path(sys.argv[1]).resolve()
assert re.fullmatch(r'/home/tux/bigbrotr-implementation-20260917/runs/browser-http-commits-[0-9a-f]{7}', str(root))
state = json.loads((root / 'resources.json').read_text())
assert state['root'] == str(root)
recovery_deadline = None

def cmd(*args):
    remaining = 15 if recovery_deadline is None else recovery_deadline - time.monotonic()
    assert remaining > 0, 'PG recovery observation deadline'
    return subprocess.check_output(args, text=True, timeout=min(15, remaining), stderr=subprocess.DEVNULL).strip()

def inspect(key):
    value = json.loads(cmd('docker', 'inspect', state[key]))[0]
    assert value['Id'] == state[key]
    assert value['Config']['Labels']['org.bigbrotr.archive-event'] == state['owner']
    assert not value['State']['OOMKilled']
    return value

def sql(query):
    return cmd('docker', 'exec', state['postgres_id'], 'psql', '-XAt', '-v', 'ON_ERROR_STOP=1',
               '-U', 'postgres', '-d', 'http_sessions', '-c', query)

def postmaster():
    value = cmd('docker', 'exec', state['postgres_id'], 'head', '-n', '1', '/var/lib/postgresql/data/postmaster.pid')
    assert value.isdecimal() and int(value) > 1
    assert cmd('docker', 'exec', state['postgres_id'], 'cat', '/proc/' + value + '/comm') == 'postgres'
    return value

fixture = root / 'out/http-fixture'
report = root / 'engine-crash.json'
assert not report.exists() and not (fixture / 'pg-crash-recovered').exists()
for key in ['container_id', 'postgres_id']:
    assert inspect(key)['State']['Running']
deadline = time.monotonic() + 180
while not (fixture / 'pg-crash-ready').exists():
    assert time.monotonic() < deadline, 'HTTP crash marker deadline'
    assert inspect('container_id')['State']['Running'], 'fixture terminated before crash marker'
    time.sleep(.1)
assert (fixture / 'pg-crash-ready').read_bytes() == b'ready\n'
assert sql("SELECT count(*) FROM pg_stat_activity WHERE usename='bigbrotr_control' AND datname=current_database()") == '1'
query = "SELECT json_agg(s ORDER BY session_id)::text FROM (SELECT s.*,xmin::text AS xmin FROM control.browser_sessions s) s"
before = sql(query)
assert sql('SELECT count(*) FROM control.browser_sessions WHERE revoked_at_us IS NULL') == '2'
old_pid = postmaster()
old_started = sql('SELECT pg_postmaster_start_time()::text')
started = time.monotonic_ns()
# PID is validated and passed as an argument, not interpolated into shell code.
cmd('docker', 'exec', '--user', 'postgres', state['postgres_id'], 'sh', '-c', 'kill -QUIT "$1"', 'sh', old_pid)
recovery_deadline = time.monotonic() + 15
while True:
    assert time.monotonic() < recovery_deadline, 'PG recovery deadline'
    assert inspect('postgres_id')['State']['Running'], 'owned PG wrapper stopped'
    try:
        ready = subprocess.run(['docker', 'exec', state['postgres_id'], 'pg_isready', '-h', '127.0.0.1', '-U', 'postgres'],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                               timeout=min(2, max(.001, recovery_deadline-time.monotonic())))
        if ready.returncode == 0:
            new_pid = postmaster()
            new_started = sql('SELECT pg_postmaster_start_time()::text')
            if new_pid != old_pid and new_started != old_started:
                break
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
        # Readiness and PID/SQL observations are not atomic across replacement.
        # Only transient engine/transport observations retry, never owner/ID/OOM.
        pass
    time.sleep(min(.05, max(0, recovery_deadline-time.monotonic())))

after = sql(query)
assert before == after
result = dict(status='PASS', source=state['source'], owner=state['owner'], container=state['postgres_id'],
              signal='SIGQUIT', before_pid=old_pid, after_pid=new_pid, before_start=old_started,
              after_start=new_started, recovery_observed_ms=(time.monotonic_ns()-started)//1_000_000,
              rows_sha256=hashlib.sha256(before.encode()).hexdigest(), live_sessions=2,
              scope='Actual immediate PostgreSQL shutdown/restart; retained rows/xmin unchanged. Not host power loss or filesystem fault.')
report.write_text(json.dumps(result, indent=2) + '\n')
(fixture / 'pg-crash-recovered').write_text('recovered\n')
print(json.dumps(result), flush=True)
