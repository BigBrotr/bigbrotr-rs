#!/usr/bin/env python3
"""Actual reservation recovery; only the inventoried isolated PostgreSQL lab."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys

sys.dont_write_bytecode = True


def command(args, stdin=None):
    result = subprocess.run(args, input=stdin, text=True, capture_output=True, timeout=60)
    assert len(result.stdout) + len(result.stderr) < 2 * 1024 * 1024
    if result.returncode:
        raise RuntimeError((args, result.returncode, result.stdout, result.stderr))
    return result.stdout.strip()


run = Path(sys.argv[1]).resolve()
source = Path(__file__).resolve().parents[2]
state = json.loads((run / 'resources.json').read_text())
assert source == run / 'source'
assert command(['git', '-C', str(source), 'rev-parse', 'HEAD']) == state['source']
assert not command(['git', '-C', str(source), 'status', '--porcelain'])
for name in [state['container'], state['postgres']]:
    actual = json.loads(command(['docker', 'inspect', name]))[0]
    assert actual['Config']['Labels']['org.bigbrotr.publication'] == state['owner']
    assert actual['State']['Running']
ready = json.loads((run / 'out/native-tests/ready.json').read_text())
assert ready['source'] == state['source']
assert hashlib.sha256((run / 'out/native-tests/catalog-initialization-probe').read_bytes()).hexdigest() == ready['initializer']
report = dict(source=state['source'], artifacts=ready, status='RUNNING', cases=[])
output = run / 'initialization-recovery.json'
output.open('x').close()


def save():
    output.write_text(json.dumps(report, indent=2) + '\n')


def sql(query, database='postgres'):
    return command(['docker', 'exec', '-i', state['postgres'], 'psql', '-XAt',
                    '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', database], query)


def fresh(name):
    database = 'bbrecovery_' + name
    sql(f'CREATE DATABASE {database};')
    sql('REVOKE CREATE ON SCHEMA public FROM PUBLIC;', database)
    return database


def durable(database):
    # Full catalog/data dump plus complete global role/membership/comment digest.
    # Verifiers never leave the server. Role xmin distinguishes no-op ALTER ROLE.
    dump = command(['docker', 'exec', state['postgres'], 'pg_dump', '-U', 'postgres', '-d', database])
    dump = '\n'.join(line for line in dump.splitlines() if not line.startswith(('\\restrict ', '\\unrestrict ')))
    roles = sql("SELECT md5(json_build_object('roles',(SELECT json_agg((to_jsonb(r)-'rolpassword')||jsonb_build_object('credential',md5(rolpassword),'xmin',xmin::text) ORDER BY oid) FROM pg_authid r),'members',(SELECT json_agg(to_jsonb(m)||jsonb_build_object('xmin',xmin::text) ORDER BY roleid,member,grantor) FROM pg_auth_members m),'comments',(SELECT json_agg(to_jsonb(c) ORDER BY objoid,classoid) FROM pg_shdescription c))::text)")
    return dict(database_sha256=hashlib.sha256(dump.encode()).hexdigest(), global_roles_md5=roles)


def arguments(database, nonce, mode, expected=None, port=5432, suffix='owner'):
    args = ['docker', 'exec', state['container'], '/out/native-tests/catalog-initialization-probe',
            database, suffix, str(nonce), str(port), mode]
    if mode == 'retire':
        args += [str(expected[k]) for k in ['database_oid', 'reservation_oid', 'bootstrap_oid', 'manager_oid']]
    return args


def invoke(database, nonce, mode, expected=None, port=5432, suffix='owner'):
    return json.loads(command(arguments(database, nonce, mode, expected, port, suffix)))


def known(database, nonce):
    # Independent exact identity oracle, not the Rust result reused as expected.
    oid = int(sql("SELECT oid FROM pg_database WHERE datname='" + database + "'"))
    bootstrap = 'bb_init_' + f'{nonce:02x}' * 16
    reservation = f'bb_init_database_{oid}'
    values = sql("SELECT (SELECT oid FROM pg_roles WHERE rolname='" + reservation + "'),"
                 "(SELECT oid FROM pg_roles WHERE rolname='" + bootstrap + "'),"
                 "(SELECT oid FROM pg_roles WHERE rolname='CatalogManager')").split('|')
    return dict(database_oid=oid, reservation_oid=int(values[0]), bootstrap_oid=int(values[1]),
                manager_oid=int(values[2]), manager_login='CatalogManager', bootstrap_login=bootstrap,
                initialization_id=[nonce]*16)


def observe_expected(identity, retired=False):
    return dict(identity, status='observed', bootstrap_retired=retired)


def case(label, database, nonce, mode, expected_result, identity=None, changes=False, port=5432, suffix='owner'):
    before = durable(database)
    item = dict(name=label, status='INCOMPLETE', expected=expected_result, before=before)
    report['cases'].append(item); save()
    actual = invoke(database, nonce, mode, identity, port, suffix)
    after = durable(database)
    item.update(observed=actual, after=after)
    success = actual['result'] == expected_result
    if not changes: success = success and before == after
    success = success and actual['commit_started'] == (changes or expected_result.get('reason') == 'CommitUnknown')
    if expected_result.get('reason') != 'CommitUnknown': success = success and actual['driver_ok']
    item['status'] = 'PASS' if success else 'FAIL'; save()
    return success


try:
    password = (run / 'secrets/CatalogManager').read_text()
    assert len(password) == 64 and all(c in '0123456789abcdef' for c in password)
    sql('CREATE ROLE "CatalogManager" LOGIN NOSUPERUSER CREATEROLE NOCREATEDB '
        "NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD '" + password + "';")
    del password
    db = fresh('basic')
    reserved = invoke(db, 31, 'reserve')
    identity = known(db, 31)
    assert reserved['result'] == dict(identity, status='reserved')
    # Independent failures are all retained at the deliberate TDD baseline.
    outcomes = [case('observe-active', db, 31, 'observe', observe_expected(identity)),
                case('retire-exact', db, 31, 'retire', dict(status='retired', changed=True), identity, changes=True)]
    empty = fresh('absent')
    outcomes.append(case('fresh-custody-is-not-recovery', empty, 32, 'fresh-observe',
                         dict(status='refused', reason='InvalidInput')))
    outcomes.append(case('observe-absent-pristine', empty, 32, 'observe', dict(status='absent')))
    assert all(outcomes), 'core recovery assertions failed; later cases not run'
    assert sql("SELECT NOT rolcanlogin AND rolpassword IS NULL FROM pg_authid WHERE oid=" + str(identity['bootstrap_oid'])) == 't'
    assert case('observe-retired', db, 31, 'observe', observe_expected(identity, True))
    assert case('retire-idempotent-no-row-change', db, 31, 'retire', dict(status='retired', changed=False), identity)
    assert case('recovered-reserve-refused', db, 31, 'recover', dict(status='refused', reason='InvalidInput'))
    sql('CREATE TABLE public.unrelated(marker integer); INSERT INTO public.unrelated VALUES(73)', empty)
    assert case('absent-nonpristine-refused', empty, 32, 'observe', dict(status='refused', reason='ExistingData'))
    report['status'] = 'PASS'
except BaseException as error:
    report.update(status='FAIL', error=str(error)); save()
    raise
finally:
    save()
print(json.dumps(dict(status=report['status'], cases=len(report['cases']))), flush=True)
