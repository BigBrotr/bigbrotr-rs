#!/usr/bin/env python3
"""Kill the owned PostgreSQL process and compare every retained fixture database."""
import json
from pathlib import Path
import sys
import time

root = Path(sys.argv[1])
runner_sha256 = sys.argv[2]
state = json.loads((root / 'resources.json').read_text())
sys.path.insert(0, str(Path(state['source']) / 'experiments/control-migrations'))
import lab

state = lab.load(root)  # Positive container/label ownership validation.
database_names = ['basic', 'expired', 'old', 'denied', 'exhausted', 'before', 'after', 'race', 'killed', 'upgrade']
databases = ['bb_owner_' + name for name in database_names]
actual = json.loads(lab.sql(state, 'postgres', "SELECT json_agg(datname ORDER BY datname) FROM pg_database WHERE datname LIKE 'bb_owner_%';"))
assert actual == sorted(databases)
assert int(lab.sql(state, 'postgres', "SELECT count(*) FROM pg_stat_activity WHERE usename IN ('bigbrotr_control','bigbrotr_migrator');")) == 0
report_path = root / 'engine-crash.json'
assert not report_path.exists()
before = {database: lab.schema_digest(state, database) for database in databases}
info = json.loads(lab.docker('inspect', state['postgres']).stdout)[0]
container_id = info['Id']
lab.docker('kill', '--signal', 'KILL', state['postgres'])
killed = json.loads(lab.docker('inspect', state['postgres']).stdout)[0]
assert killed['Id'] == container_id and not killed['State']['Running'] and killed['State']['ExitCode'] == 137 and not killed['State']['OOMKilled']
lab.docker('start', state['postgres'])
deadline = time.monotonic() + 30
while lab.docker('exec', state['postgres'], 'pg_isready', '-U', 'postgres', check=False).returncode:
    assert time.monotonic() < deadline
    time.sleep(.1)
after = {database: lab.schema_digest(state, database) for database in databases}
assert before == after
report = {'status': 'PASS', 'source': state['build']['build_info']['source']['revision'], 'binary_sha256': state['binary_sha256'], 'runner_sha256': runner_sha256,
          'container_id': container_id, 'killed_state': killed['State'], 'before': before, 'after': after,
          'scope': 'Actual PostgreSQL container/process SIGKILL and recovery with ten complete Control schema dumps unchanged. Not a host power loss or filesystem fsync fault.'}
report_path.write_text(json.dumps(report, indent=2) + '\n')
print(json.dumps({'status': 'PASS', 'databases': len(databases), 'engine_exit': killed['State']['ExitCode']}))
