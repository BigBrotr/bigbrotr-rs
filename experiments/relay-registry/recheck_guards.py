#!/usr/bin/env python3
"""Repeat only the corrected SQL fixture against the retained original database.

No rebuild, new database or overwritten original result. Only the identified
PostgreSQL container is restarted. Every fixture write rolls back; all three
original databases must retain their complete Control table contents.
"""
import datetime
import hashlib
import json
from pathlib import Path
import re
import shutil
import sys
import time

sys.dont_write_bytecode = True
SOURCE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SOURCE / 'experiments/control-migrations'))
import lab
sys.path.insert(0, str(SOURCE / 'experiments/network-transport'))
from inventory import projection

root = Path(sys.argv[1]).resolve(strict=True)
assert root == Path('/home/tux/bigbrotr-implementation-20260917/runs/relay-registry-03b0197')
revision = lab.command(['git', '-C', str(SOURCE), 'rev-parse', 'HEAD']).stdout.strip()
assert not lab.command(['git', '-C', str(SOURCE), 'status', '--porcelain', '--untracked-files=all']).stdout
pg = lab.load(root / 'pg')
original = json.loads((root / 'pg/registry-result.json').read_text())
assert original['source'] == '03b019794a7b79f9f8a0daaa6a1b6f902c783a3a'
assert original['status'] == 'FAIL' and original['failure'] == 'actual-sql-guards-and-rollback'
assert pg['state'] == 'stopped-retained'
output = root / ('guards-recheck-' + revision[:7])
output.mkdir(mode=0o700)


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def containers():
    return json.loads(lab.docker('inspect', *lab.docker('ps', '-aq').stdout.split()).stdout)


def tables(database):
    names = json.loads(lab.sql(pg, database,
        "SELECT json_agg(tablename ORDER BY tablename) FROM pg_tables WHERE schemaname='control'"))
    result = {}
    for name in names:
        assert re.fullmatch('[a-z_]+', name)
        raw = lab.sql(pg, database,
            f"SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]') FROM control.{name} t")
        result[name] = dict(rows=len(json.loads(raw)), sha256=hashlib.sha256(raw.encode()).hexdigest())
    return result


prior = containers()
target = next(c for c in prior if c['Name'] == '/' + pg['postgres'])
assert target['Config']['Labels'][lab.LABEL] == pg['owner']
assert target['State']['Status'] == 'exited' and target['State']['Pid'] == 0
assert not any(c['State']['Running'] and ('bigbrotr' in c['Name'] or
    any(Path(m['Source']).is_relative_to(root) for m in c['Mounts'])) for c in prior)
assert shutil.disk_usage(root).free > 2 * 1024**3
report = dict(status='RUNNING', source=revision, original_source=original['source'],
    owner=pg['owner'], postgres_id=target['Id'], started_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
    original_result_sha256=sha(root / 'pg/registry-result.json'),
    fixture_sha256=sha(SOURCE / 'experiments/relay-registry/guards.sql'),
    free_bytes_before=shutil.disk_usage(root).free)
lab.write_json(output / 'preflight.json', [projection(c) for c in prior])
lab.write_json(output / 'result.json', report)
try:
    lab.docker('start', target['Id'])
    deadline = time.monotonic() + 30
    while lab.docker('exec', target['Id'], 'pg_isready', '-U', 'postgres', check=False).returncode:
        assert time.monotonic() < deadline
        time.sleep(0.2)
    for case in ['basic', 'collision', 'expired']:
        expected = next(c['after'] for c in original['cases'] if c['name'] == 'independent-effects-' + case)
        assert tables('bb_registry_' + case) == expected
    query = "SELECT json_build_object('session',session_user,'role',current_user,'visible',backend_start IS NOT NULL) FROM pg_stat_activity WHERE pid=pg_backend_pid()"
    observations = []
    for user, sql in [('postgres', 'SET ROLE bigbrotr_control; ' + query), ('bigbrotr_control', query)]:
        result = lab.docker('exec', '-i', target['Id'], 'psql', '-X', '-A', '-t', '-q',
            '--no-password', '-U', user, '-d', 'bb_registry_basic', '-v', 'ON_ERROR_STOP=1', data=sql)
        observations.append(json.loads(result.stdout))
    report['backend_visibility'] = observations
    assert observations == [dict(session='postgres', role='bigbrotr_control', visible=False),
                            dict(session='bigbrotr_control', role='bigbrotr_control', visible=True)]
    result = lab.docker('exec', '-i', target['Id'], 'psql', '-X', '-A', '-t', '-q',
        '--no-password', '-U', 'bigbrotr_control', '-d', 'bb_registry_basic', '-v', 'ON_ERROR_STOP=1',
        data=(SOURCE / 'experiments/relay-registry/guards.sql').read_text(), check=False)
    (output / 'guards.stdout').write_text(result.stdout)
    (output / 'guards.stderr').write_text(result.stderr)
    report['exit_code'] = result.returncode
    report['summary'] = [json.loads(line) for line in result.stdout.splitlines() if line.startswith('{')]
    assert result.returncode == 0 and report['summary'] == [
        dict(status='PASS', expected_refusals=8, uncommitted_rows=6)]
    report['after'] = {}
    for case in ['basic', 'collision', 'expired']:
        expected = next(c['after'] for c in original['cases'] if c['name'] == 'independent-effects-' + case)
        report['after'][case] = tables('bb_registry_' + case)
        assert report['after'][case] == expected
    assert sha(root / 'pg/registry-result.json') == report['original_result_sha256']
    report['status'] = 'PASS_CORRECTED_SQL_FIXTURE_ONLY'
except BaseException as error:
    report.update(status='FAIL', failure=type(error).__name__ + ': ' + str(error))
    raise
finally:
    item = json.loads(lab.docker('inspect', target['Id']).stdout)[0]
    assert item['Config']['Labels'][lab.LABEL] == pg['owner']
    if item['State']['Running']:
        lab.docker('stop', '--time', '10', target['Id'])
    after = {c['Id']: c for c in containers()}
    report['terminal'] = projection(after[target['Id']])
    report['unrelated_changed'] = [c['Id'] for c in prior if c['Id'] != target['Id']
        and projection(c) != projection(after[c['Id']])]
    report['free_bytes_after'] = shutil.disk_usage(root).free
    report['finished_at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    terminal = report['terminal']['State']
    if report['unrelated_changed'] or terminal['Running'] or terminal['Pid'] or terminal['OOMKilled'] or terminal['ExitCode']:
        report['status'] = 'INCOMPLETE_LAB_AUDIT'
    lab.write_json(output / 'result.json', report)
print(json.dumps(dict(status=report['status'], output=str(output))))
assert report['status'] == 'PASS_CORRECTED_SQL_FIXTURE_ONLY'
