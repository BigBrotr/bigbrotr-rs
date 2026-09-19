#!/usr/bin/env python3
"""Actual reservation recovery; only the inventoried isolated PostgreSQL lab."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time

sys.dont_write_bytecode = True


def command(args, stdin=None):
    result = subprocess.run(args, input=stdin, text=True, capture_output=True, timeout=60)
    assert len(result.stdout) + len(result.stderr) < 2 * 1024 * 1024
    if result.returncode:
        raise RuntimeError((args, result.returncode, result.stdout, result.stderr))
    return result.stdout.strip()


run = Path(sys.argv[1]).resolve()
source = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(source / 'experiments/control-migrations'))
from fault_cases import Process

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
    if mode in ('retire', 'retire-cancel', 'install'):
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


def unrelated_roles(bootstrap_oid):
    return sql(f"""SELECT md5(json_build_object(
        'roles',(SELECT json_agg(CASE WHEN oid={bootstrap_oid}
            THEN to_jsonb(r)-'rolpassword'-'rolcanlogin' ELSE
            (to_jsonb(r)-'rolpassword')||jsonb_build_object('credential',md5(rolpassword),'xmin',xmin::text)
            END ORDER BY oid) FROM pg_authid r),
        'members',(SELECT json_agg(to_jsonb(m)||jsonb_build_object('xmin',xmin::text) ORDER BY roleid,member,grantor) FROM pg_auth_members m),
        'comments',(SELECT json_agg(to_jsonb(c) ORDER BY objoid,classoid) FROM pg_shdescription c))::text)""")


def case(label, database, nonce, mode, expected_result, identity=None, changes=False, port=5432, suffix='owner'):
    before = durable(database)
    role_before = unrelated_roles(identity['bootstrap_oid']) if mode == 'retire' and changes else None
    item = dict(name=label, status='INCOMPLETE', expected=expected_result, before=before)
    report['cases'].append(item); save()
    actual = invoke(database, nonce, mode, identity, port, suffix)
    after = durable(database)
    item.update(observed=actual, after=after)
    success = actual['result'] == expected_result
    if not changes: success = success and before == after
    if role_before is not None:
        retired = sql('SELECT NOT rolcanlogin AND rolpassword IS NULL FROM pg_authid WHERE oid=' + str(identity['bootstrap_oid'])) == 't'
        isolated = role_before == unrelated_roles(identity['bootstrap_oid']) and before['database_sha256'] == after['database_sha256']
        item.update(exact_bootstrap_retired=retired, unrelated_role_attributes_and_database_unchanged=isolated)
        success = success and retired and isolated
    success = success and actual['commit_started'] == (changes or expected_result.get('reason') == 'CommitUnknown')
    if expected_result.get('reason') != 'CommitUnknown': success = success and actual['driver_ok']
    item['status'] = 'PASS' if success else 'FAIL'; save()
    return success


children = []
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
    # A known reservation must compare every returned OID, including the current
    # reservation object's OID which is deliberately not self-bound by comment v1.
    for field in ['database_oid', 'reservation_oid', 'bootstrap_oid', 'manager_oid']:
        wrong = dict(identity); wrong[field] += 1
        assert case('changed-' + field, db, 31, 'retire', dict(status='refused', reason='Conflict'), wrong)
    comment = sql(f"SELECT shobj_description({identity['reservation_oid']},'pg_authid')")
    reserved_name = f"bb_init_database_{identity['database_oid']}"
    for label, value, reason in [('missing-comment', 'NULL', 'Conflict'),
                                  ('malformed-comment', "'bigbrotr-catalog-initialization/1:wrong'", 'Conflict'),
                                  ('oversize-comment', "'" + 'x'*129 + "'", 'ResourceLimit')]:
        sql(f"COMMENT ON ROLE {reserved_name} IS {value}")
        assert case(label, db, 31, 'observe', dict(status='refused', reason=reason))
        assert case(label+'-retire', db, 31, 'retire', dict(status='refused', reason=reason), identity)
        sql(f"COMMENT ON ROLE {reserved_name} IS '{comment}'")
    for label, change, undo in [
        ('reservation-login', f'ALTER ROLE {reserved_name} LOGIN', f'ALTER ROLE {reserved_name} NOLOGIN'),
        ('bootstrap-inherit', f"ALTER ROLE {identity['bootstrap_login']} INHERIT", f"ALTER ROLE {identity['bootstrap_login']} NOINHERIT"),
        ('bootstrap-login-without-password', f"ALTER ROLE {identity['bootstrap_login']} LOGIN", f"ALTER ROLE {identity['bootstrap_login']} NOLOGIN"),
        ('bootstrap-member', f"GRANT {identity['bootstrap_login']} TO bigbrotr_control", f"REVOKE {identity['bootstrap_login']} FROM bigbrotr_control"),
        ('public-bypass', 'GRANT SET ON PARAMETER session_replication_role TO PUBLIC', 'REVOKE SET ON PARAMETER session_replication_role FROM PUBLIC')]:
        sql(change)
        try:
            assert case(label, db, 31, 'observe', dict(status='refused', reason='UnsafeRole'))
        finally: sql(undo)
    # Safe manager replacement has a different OID: the retained comment must refuse it.
    sql('ALTER ROLE "CatalogManager" RENAME TO "OriginalManager"; CREATE ROLE "CatalogManager" LOGIN CREATEROLE NOINHERIT')
    try:
        assert case('replaced-manager', db, 31, 'observe', dict(status='refused', reason='Conflict'))
    finally:
        sql('DROP ROLE "CatalogManager"; ALTER ROLE "OriginalManager" RENAME TO "CatalogManager"')
    bootstrap = identity['bootstrap_login']
    sql(f'ALTER ROLE {bootstrap} RENAME TO old_bootstrap; CREATE ROLE {bootstrap} NOLOGIN NOINHERIT')
    try:
        assert case('replaced-bootstrap', db, 31, 'observe', dict(status='refused', reason='Conflict'))
    finally:
        sql(f'DROP ROLE {bootstrap}; ALTER ROLE old_bootstrap RENAME TO {bootstrap}')
    sql(f"DROP ROLE {reserved_name}; CREATE ROLE {reserved_name} NOLOGIN NOINHERIT; COMMENT ON ROLE {reserved_name} IS '{comment}'")
    replaced = known(db, 31)
    assert replaced['reservation_oid'] != identity['reservation_oid']
    assert case('replaced-reservation-known-oid', db, 31, 'retire', dict(status='refused', reason='Conflict'), identity)
    assert case('first-observation-current-reservation-oid', db, 31, 'observe', observe_expected(replaced, True))
    for label, nonce in [('missing_reservation', 33), ('missing_bootstrap', 34)]:
        partial = fresh(label)
        assert invoke(partial, nonce, 'reserve')['result']['status'] == 'reserved'
        current = known(partial, nonce)
        if label == 'missing_reservation':
            sql(f"DROP ROLE bb_init_database_{current['database_oid']}")
        else:
            sql(f"DROP OWNED BY {current['bootstrap_login']}", partial)
            sql(f"DROP ROLE {current['bootstrap_login']}")
        assert case(label, partial, nonce, 'observe', dict(status='refused', reason='Conflict'))

    # Real native schema and privileged ownership transfer must not prevent later
    # observation: the bootstrap no longer owns the native tables at this point.
    native = fresh('installed')
    initialized = invoke(native, 35, 'initialize')
    current = known(native, 35)
    assert initialized['result'] == dict(current, status='reserved', native_initialized=True)
    assert case('native-retire', native, 35, 'retire', dict(status='retired', changed=True), current, changes=True)
    assert case('install-for-manager', native, 35, 'install', dict(status='installed', changed=True), current, changes=True)
    assert sql("SELECT count(*)=28 AND bool_and(relowner=(SELECT oid FROM pg_roles WHERE rolname='CatalogManager')) FROM pg_class WHERE relnamespace='public'::regnamespace AND relkind='r'", native) == 't'
    assert case('observe-after-manager-install', native, 35, 'observe', observe_expected(current, True))
    assert case('retire-after-manager-install-no-change', native, 35, 'retire', dict(status='retired', changed=False), current)

    for mode, counters, retired, nonce in [('before-forward', [1,0,0,0,1], False, 36), ('after-complete', [1,1,1,0,1], True, 37)]:
        lost = fresh('lost_before' if not retired else 'lost_after')
        assert invoke(lost, nonce, 'reserve')['result']['status'] == 'reserved'
        current = known(lost, nonce)
        before = durable(lost)
        proxy = Process(['docker','exec','-i',state['container'],'python3','-B',
            '/source/experiments/control-migrations/fault_proxy.py','--upstream-port','5432',
            '--mode',mode,'--deadline-seconds','60'], input_pipe=True)
        children.append(proxy); proxy_ready = proxy.event('ready', 10)
        assert proxy_ready['proxy_sha256'] == hashlib.sha256((source/'experiments/control-migrations/fault_proxy.py').read_bytes()).hexdigest()
        actual = invoke(lost, nonce, 'retire', current, proxy_ready['listen_port'])
        receipt = proxy.event('receipt', 10)
        assert proxy.wait(5) == 0 and not proxy.stderr
        assert actual['result'] == dict(status='refused', reason='CommitUnknown') and actual['commit_started']
        assert [receipt[k] for k in ['commits_seen','commits_forwarded','server_commit_complete','client_commit_complete','faults']] == counters
        if not retired: assert durable(lost) == before
        assert case('resolve-retirement-'+mode, lost, nonce, 'observe', observe_expected(current, retired))
        report['cases'][-1].update(ambiguous=actual, wire=receipt); save()
        assert case('repeat-retirement-'+mode, lost, nonce, 'retire', dict(status='retired', changed=not retired), current, changes=not retired)

    # A cancelled operation can have its COMMIT queued remotely. Destroy/join its
    # actual driver, reacquire real local custody, then wait on the authoritative
    # transaction lock before observing either durable result.
    for action, nonce in [('close', 38), ('release', 39)]:
        cancelled = fresh('cancel_' + action)
        assert invoke(cancelled, nonce, 'reserve')['result']['status'] == 'reserved'
        current = known(cancelled, nonce)
        journal = run/'out'/f'initialize_{cancelled}_owner/.bigbrotr-initialization/intent.json'
        journal_before = journal.read_bytes()
        proxy = Process(['docker','exec','-i',state['container'],'python3','-B',
            '/source/experiments/control-migrations/fault_proxy.py','--upstream-port','5432',
            '--mode','hold','--deadline-seconds','60'], input_pipe=True)
        children.append(proxy); proxy_ready = proxy.event('ready', 10)
        peer = Process(arguments(cancelled, nonce, 'retire-cancel', current, proxy_ready['listen_port']))
        children.append(peer); proxy.event('held', 10)
        assert peer.wait(10) == 0 and not peer.stderr
        cancellation = json.loads(peer.stdout)
        assert cancellation == dict(result=dict(status='cancelled'), commit_started=True, driver_ok=False)
        observer = Process(arguments(cancelled, nonce, 'observe')); children.append(observer)
        until = time.monotonic() + 3
        while True:
            waiters = json.loads(sql("SELECT COALESCE(json_agg(json_build_object('pid',a.pid,'blockers',pg_blocking_pids(a.pid))),'[]'::json) FROM pg_stat_activity a JOIN pg_locks l ON l.pid=a.pid WHERE a.datname='"+cancelled+"' AND a.application_name='bigbrotr-initialization-probe' AND NOT l.granted AND l.locktype='advisory' AND l.classid=1111651442 AND l.objid=1128354865"))
            if len(waiters) == 1: break
            assert time.monotonic() < until
            time.sleep(.02)
        assert waiters[0]['blockers']
        proxy.send(action)
        assert observer.wait(10) == 0 and not observer.stderr
        receipt = proxy.event('receipt', 10); exit_code = proxy.wait(5)
        assert exit_code in (0,3) and not proxy.stderr
        observation = json.loads(observer.stdout)
        assert observation == dict(result=observe_expected(current, action=='release'), commit_started=False, driver_ok=True)
        assert receipt['commits_seen']==1 and receipt['commits_forwarded']==(action=='release')
        assert journal.read_bytes() == journal_before
        report['cases'].append(dict(name='cancelled-driver-'+action, status='PASS', cancelled=cancellation,
            observed=observation, waiters=waiters, wire=receipt, proxy_exit=exit_code, journal_unchanged=True)); save()
    assert sql("SELECT count(*) FROM pg_stat_activity WHERE application_name='bigbrotr-initialization-probe' OR usename LIKE 'bb_init_%'") == '0'
    report['status'] = 'PASS'
except BaseException as error:
    report.update(status='FAIL', error=str(error)); save()
    raise
finally:
    cleanup = []
    for child in children:
        was_running = child.process.poll() is None
        child.stop()
        if was_running or child.overflow or any(t.is_alive() for t in child.threads): cleanup.append('unresolved-process')
    if cleanup: report.update(status='INCOMPLETE', cleanup_errors=cleanup, requires_runner_stop=True)
    save()
print(json.dumps(dict(status=report['status'], cases=len(report['cases']))), flush=True)
