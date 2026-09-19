#!/usr/bin/env python3
"""Real PG provisioning oracle. Run only against the recorded isolated owned lab."""
import json
from pathlib import Path
import subprocess
import sys


def command(args, stdin=None):
    result = subprocess.run(args, input=stdin, text=True, capture_output=True, timeout=60)
    assert len(result.stdout) + len(result.stderr) < 2 * 1024 * 1024
    if result.returncode:
        raise RuntimeError((args, result.returncode, result.stdout, result.stderr))
    return result.stdout.strip()


run = Path(sys.argv[1]).resolve()
state = json.loads((run / 'resources.json').read_text())
assert Path(__file__).resolve().parents[2] == run / 'source'
assert command(['git', '-C', str(run / 'source'), 'rev-parse', 'HEAD']) == state['source']
assert not command(['git', '-C', str(run / 'source'), 'status', '--porcelain'])
for name in [state['container'], state['postgres']]:
    actual = json.loads(command(['docker', 'inspect', name]))[0]
    assert actual['Config']['Labels']['org.bigbrotr.publication'] == state['owner']
    assert actual['State']['Running']
ready = json.loads((run / 'out/native-tests/ready.json').read_text())
assert ready['source'] == state['source']
report = dict(source=state['source'], artifacts=ready, status='RUNNING', cases=[])
output = run / 'database-reservation.json'
output.open('x').close()


def save():
    output.write_text(json.dumps(report, indent=2) + '\n')


def sql(query, database='postgres'):
    return command(['docker', 'exec', '-i', state['postgres'], 'psql', '-XAt',
                    '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', database], query)


try:
    sql('CREATE ROLE "CatalogManager" LOGIN NOSUPERUSER CREATEROLE NOCREATEDB '
        'NOINHERIT NOREPLICATION NOBYPASSRLS; CREATE DATABASE bbreserve_one;')
    sql('REVOKE CREATE ON SCHEMA public FROM PUBLIC;', 'bbreserve_one')
    database_oid = int(sql("SELECT oid FROM pg_database WHERE datname='bbreserve_one'"))
    assert sql("SELECT count(*) FROM pg_depend WHERE refclassid='pg_namespace'::regclass "
               "AND refobjid='public'::regnamespace", 'bbreserve_one') == '0'
    result = json.loads(command(['docker', 'exec', state['container'],
                                 '/out/native-tests/catalog-initialization-probe',
                                 'bbreserve_one', 'first', '3']))
    names = [f'bb_init_database_{database_oid}', 'bb_init_' + '03' * 16]
    rows = json.loads(sql("SELECT COALESCE(json_agg(row_to_json(r)),'[]'::json) FROM "
                         "(SELECT oid,rolname,rolcanlogin,rolsuper,rolcreatedb,rolcreaterole,"
                         "rolinherit,rolreplication,rolbypassrls,rolconnlimit,"
                         "rolpassword IS NULL AS password_null FROM pg_authid WHERE rolname IN ('"
                         + "','".join(names) + "') ORDER BY rolname) r"))
    report['cases'].append(dict(name='atomic_fresh_reservation', result=result, roles=rows,
                                database_oid=database_oid))
    save()
    assert result['result']['status'] == 'reserved', result
    assert result['commit_started'] is True
    assert {r['rolname'] for r in rows} == set(names)
    assert all(not any(r[k] for k in ['rolsuper', 'rolcreatedb', 'rolcreaterole',
                                      'rolinherit', 'rolreplication', 'rolbypassrls']) for r in rows)
    report['status'] = 'PASS'
except Exception as error:
    report['status'] = 'FAIL'
    report['error'] = str(error)
    save()
    raise
save()
print(json.dumps(dict(status=report['status'], cases=len(report['cases']))), flush=True)
