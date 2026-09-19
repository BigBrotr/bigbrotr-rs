#!/usr/bin/env python3
"""Real PG provisioning oracle. Run only against the recorded isolated owned lab."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time

sys.dont_write_bytecode = True
SOURCE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SOURCE / 'experiments/control-migrations'))
from fault_cases import Process


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
for key, path in [('initializer','native-tests/catalog-initialization-probe'), ('worker','archive-worker/bigbrotr-archive-worker')]:
    assert hashlib.sha256((run / 'out' / path).read_bytes()).hexdigest() == ready[key]
report = dict(source=state['source'], artifacts=ready, status='RUNNING', cases=[])
output = run / 'database-reservation.json'
output.open('x').close()


def save():
    output.write_text(json.dumps(report, indent=2) + '\n')


def sql(query, database='postgres'):
    return command(['docker', 'exec', '-i', state['postgres'], 'psql', '-XAt',
                    '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', database], query)


def fresh(name):
    database = 'bbreserve_' + name
    sql(f'CREATE DATABASE {database};')
    sql('REVOKE CREATE ON SCHEMA public FROM PUBLIC;', database)
    return database


def roles():
    # Never return the actual verifier, only a whole-state fingerprint.
    return sql("SELECT md5(json_build_object('roles',(SELECT json_agg((to_jsonb(r)-'rolpassword')||jsonb_build_object('credential',md5(rolpassword),'xmin',xmin::text) ORDER BY oid) FROM pg_authid r),'members',(SELECT json_agg(to_jsonb(m)||jsonb_build_object('xmin',xmin::text) ORDER BY roleid,member,grantor) FROM pg_auth_members m),'comments',(SELECT json_agg(to_jsonb(c) ORDER BY objoid,classoid) FROM pg_shdescription c))::text)")


def dump(database):
    text = command(['docker','exec',state['postgres'],'pg_dump','-U','postgres','-d',database])
    return '\n'.join(line for line in text.splitlines() if not line.startswith(('\\restrict ', '\\unrestrict ')))


def arguments(database, suffix, nonce, port=5432, mode='reserve'):
    return ['docker','exec',state['container'],'/out/native-tests/catalog-initialization-probe',
            database, suffix, str(nonce), str(port), mode]


def invoke(database, suffix, nonce, expected, port=5432, mode='reserve'):
    before, before_roles = dump(database), roles()
    case = dict(name=database+'-'+suffix+'-'+mode, status='INCOMPLETE', expected=expected)
    report['cases'].append(case); save()
    result = json.loads(command(arguments(database,suffix,nonce,port,mode)))
    case['observed'] = result; save()
    assert result['result']['status'] == ('reserved' if expected=='reserved' else 'refused'), result
    if expected!='reserved': assert result['result']['reason']==expected, result
    assert result['commit_started'] == (expected in ('reserved','CommitUnknown'))
    if expected=='reserved':
        case['durable']=check_reservation(database,nonce,True,result)
    if expected not in ('reserved','CommitUnknown'):
        assert dump(database)==before and roles()==before_roles
        case['database_and_roles_unchanged']=True
    case['status']='PASS'; save()
    return result


def check_reservation(database, nonce, expected, observed=None):
    database_oid = int(sql("SELECT oid FROM pg_database WHERE datname='"+database+"'"))
    names = [f'bb_init_database_{database_oid}', 'bb_init_' + f'{nonce:02x}' * 16]
    rows = json.loads(sql("SELECT COALESCE(json_agg(row_to_json(r)),'[]'::json) FROM "
        "(SELECT oid::bigint AS oid,rolname,rolcanlogin,rolsuper,rolcreatedb,rolcreaterole,rolinherit,"
        "rolreplication,rolbypassrls,rolconnlimit,rolpassword IS NULL AS password_null,"
        "rolpassword LIKE 'SCRAM-SHA-256$4096:%' AS scram FROM pg_authid WHERE rolname IN ('"
        + "','".join(names) + "') ORDER BY rolname) r"))
    if not expected:
        assert rows==[]
        return dict(database_oid=database_oid,roles=[])
    assert {r['rolname'] for r in rows}==set(names)
    assert all(not any(r[k] for k in ['rolsuper','rolcreatedb','rolcreaterole','rolinherit',
                                     'rolreplication','rolbypassrls']) for r in rows)
    reservation = next(r for r in rows if r['rolname']==names[0])
    bootstrap = next(r for r in rows if r['rolname']==names[1])
    assert not reservation['rolcanlogin'] and reservation['password_null']
    assert bootstrap['rolcanlogin'] and bootstrap['scram'] and bootstrap['rolconnlimit']==8
    manager = int(sql("SELECT oid FROM pg_roles WHERE rolname='CatalogManager'"))
    comment = sql(f"SELECT shobj_description({reservation['oid']},'pg_authid')")
    assert comment==f"bigbrotr-catalog-initialization/1:{nonce:02x}"+f'{nonce:02x}'*15+f":{bootstrap['oid']}:{manager}"
    assert sql(f"SELECT count(*) FROM pg_auth_members WHERE roleid IN ({reservation['oid']},{bootstrap['oid']}) OR member IN ({reservation['oid']},{bootstrap['oid']})")=='0'
    assert sql(f"SELECT has_database_privilege({bootstrap['oid']},current_database(),'CONNECT') AND NOT has_database_privilege({bootstrap['oid']},current_database(),'CREATE') AND has_schema_privilege({bootstrap['oid']},'public','USAGE') AND has_schema_privilege({bootstrap['oid']},'public','CREATE')",database)=='t'
    if observed is not None:
        fields=observed['result'].copy()
        fields.pop('native_initialized',None)
        assert fields==dict(status='reserved',database_oid=database_oid,reservation_oid=reservation['oid'],
            bootstrap_login=names[1],bootstrap_oid=bootstrap['oid'],manager_oid=manager,
            manager_login='CatalogManager',initialization_id=[nonce]*16), fields
    return dict(database_oid=database_oid,roles=rows,comment=comment)


children=[]
try:
    password=(run/'secrets/CatalogManager').read_text()
    assert len(password)==64 and all(c in '0123456789abcdef' for c in password)
    sql('CREATE ROLE "CatalogManager" LOGIN NOSUPERUSER CREATEROLE NOCREATEDB '
        "NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD '"+password+"';")
    del password
    first = fresh('one')
    invoke(first,'first',3,'reserved')
    report['cases'][-1]['durable']=check_reservation(first,3,True); save()
    # Matching names/comment, a new root and a new nonce cannot bypass DB reservation.
    invoke(first,'same_nonce',3,'Conflict')
    invoke(first,'other_nonce',4,'Conflict')
    invoke(first,'first',3,'InvalidInput',mode='recover')
    other=fresh('other')
    invoke(other,'collision',3,'Conflict')
    invoke(other,'new_nonce',4,'reserved')
    report['cases'][-1]['durable']=check_reservation(other,4,True); save()
    native=fresh('native')
    result=invoke(native,'native',5,'reserved',mode='initialize')
    assert result['result']['native_initialized'] is True
    expected=['archive_commits','dictionaries','event_eligibility_facts','event_receive_facts','events','provenance_facts']
    # Literal schema1 catalog set, independently inspected through PostgreSQL.
    actual=sql("SELECT string_agg(table_name,',' ORDER BY table_name) FROM public.ducklake_table WHERE end_snapshot IS NULL",native)
    report['cases'][-1]['logical_tables']=actual; save()
    assert actual==','.join(expected), actual
    assert sql("SELECT string_agg(snapshot_id::text,',' ORDER BY snapshot_id) FROM public.ducklake_snapshot",native)=='0,1'
    assert sql('SELECT count(*) FROM public.ducklake_data_file',native)=='0'
    report['cases'][-1]['durable']=check_reservation(native,5,True); save()

    unprivileged=fresh('unprivileged')
    invoke(unprivileged,'manager',6,'UnsafeRole',mode='unprivileged')
    check_reservation(unprivileged,6,False)

    for label, query in [('table','CREATE TABLE public.unrelated(value integer)'),
                         ('schema','CREATE SCHEMA unrelated'),
                         ('extension','CREATE EXTENSION hstore')]:
        db=fresh(label); sql(query,db)
        invoke(db,'nonempty',6,'ExistingData')
        check_reservation(db,6,False)
    for label, grant, undo in [
        ('public_create','GRANT CREATE ON DATABASE {db} TO PUBLIC','REVOKE CREATE ON DATABASE {db} FROM PUBLIC'),
        ('parameter','GRANT SET ON PARAMETER session_replication_role TO PUBLIC','REVOKE SET ON PARAMETER session_replication_role FROM PUBLIC'),
        ('manager_super','ALTER ROLE "CatalogManager" SUPERUSER','ALTER ROLE "CatalogManager" NOSUPERUSER'),
        ('manager_inherit','ALTER ROLE "CatalogManager" INHERIT','ALTER ROLE "CatalogManager" NOINHERIT'),
        ('manager_member','GRANT "CatalogManager" TO bigbrotr_control','REVOKE "CatalogManager" FROM bigbrotr_control')]:
        db=fresh(label); sql(grant.format(db=db),db)
        try:
            invoke(db,'unsafe',7,'UnsafeRole')
            check_reservation(db,7,False)
        finally: sql(undo.format(db=db),db)

    for mode,counters,committed,nonce in [('before-forward',[1,0,0,0,1],False,8),('after-complete',[1,1,1,0,1],True,9)]:
        db=fresh('lost_before' if not committed else 'lost_after')
        before,before_roles=dump(db),roles()
        proxy=Process(['docker','exec','-i',state['container'],'python3','-B',
            '/source/experiments/control-migrations/fault_proxy.py','--upstream-port','5432',
            '--mode',mode,'--deadline-seconds','60'],input_pipe=True)
        children.append(proxy); ready_proxy=proxy.event('ready',10)
        assert ready_proxy['proxy_sha256']==hashlib.sha256((SOURCE/'experiments/control-migrations/fault_proxy.py').read_bytes()).hexdigest()
        invoke(db,'lost',nonce,'CommitUnknown',port=ready_proxy['listen_port'])
        receipt=proxy.event('receipt',10)
        assert proxy.wait(5)==0 and not proxy.stderr
        assert [receipt[k] for k in ['commits_seen','commits_forwarded','server_commit_complete','client_commit_complete','faults']]==counters
        report['cases'][-1].update(wire=receipt,durable=check_reservation(db,nonce,committed));save()
        if not committed: assert roles()==before_roles and dump(db)==before
        invoke(db,'lost',nonce,'InvalidInput',mode='recover')
        if committed: invoke(db,'fresh_root',nonce+20,'Conflict')

    # The proxy holds a fully prepared COMMIT. A second root must wait on the
    # actual database lock, then see either the first durable result or rollback.
    for scope,action,nonce in [('database','release',10),('database','close',12),
                              ('nonce','release',14),('nonce','close',16)]:
        db=fresh('race_'+scope+'_'+action)
        other_db=fresh('race_other_'+action) if scope=='nonce' else db
        other_before=dump(other_db)
        proxy=Process(['docker','exec','-i',state['container'],'python3','-B',
            '/source/experiments/control-migrations/fault_proxy.py','--upstream-port','5432',
            '--mode','hold','--deadline-seconds','60'],input_pipe=True)
        children.append(proxy); ready_proxy=proxy.event('ready',10)
        first_peer=Process(arguments(db,'first',nonce,ready_proxy['listen_port']));children.append(first_peer)
        proxy.event('held',10)
        assert check_reservation(db,nonce,False)['roles']==[]
        second_nonce=nonce if scope=='nonce' else nonce+1
        second=Process(arguments(other_db,'second',second_nonce));children.append(second)
        until=time.monotonic()+4
        while True:
            lock = "l.locktype='transactionid'" if scope=='nonce' else "l.locktype='advisory' AND l.classid=1111651442 AND l.objid=1128354865"
            waiters=json.loads(sql("SELECT COALESCE(json_agg(json_build_object('pid',a.pid,'blockers',pg_blocking_pids(a.pid),'locktype',l.locktype)),'[]'::json) FROM pg_stat_activity a JOIN pg_locks l ON l.pid=a.pid WHERE a.datname='"+other_db+"' AND a.application_name='bigbrotr-initialization-probe' AND NOT l.granted AND "+lock))
            if len(waiters)==1: break
            assert time.monotonic()<until
            time.sleep(.02)
        assert waiters[0]['blockers']
        proxy.send(action)
        assert first_peer.wait(30)==0 and second.wait(30)==0 and not first_peer.stderr and not second.stderr
        receipt=proxy.event('receipt',10);assert proxy.wait(5)==0 and not proxy.stderr
        a,b=json.loads(first_peer.stdout),json.loads(second.stdout)
        assert a['result']['status']==('reserved' if action=='release' else 'refused')
        assert b['result']['status']==('refused' if action=='release' else 'reserved')
        if action=='release': assert b['result']['reason']==('Backend' if scope=='nonce' else 'Conflict')
        else: assert a['result']['reason']=='CommitUnknown'
        winner=nonce if action=='release' else second_nonce
        winner_db=db if action=='release' else other_db
        if scope=='nonce':
            loser=other_db if action=='release' else db
            assert sql("SELECT count(*) FROM pg_roles WHERE rolname='bb_init_database_'||(SELECT oid::text FROM pg_database WHERE datname='"+loser+"')")=='0'
            if action=='release': assert dump(other_db)==other_before
        else:
            losing_nonce=second_nonce if action=='release' else nonce
            assert sql("SELECT count(*) FROM pg_roles WHERE rolname='bb_init_"+f'{losing_nonce:02x}'*16+"'")=='0'
        report['cases'].append(dict(name='concurrent-'+scope+'-'+action,status='PASS',observed=[a,b],wire=receipt,
            waiters=waiters,durable=check_reservation(winner_db,winner,True,a if action=='release' else b)));save()

    # Killing the actual coordinator excludes its local descriptors but cannot
    # retract COMMIT already received by a remote intermediary. Exercise both
    # durable outcomes after local custody is reacquired, without a new launch.
    for action,nonce in [('close',18),('release',19)]:
        db=fresh('killed_'+action)
        proxy=Process(['docker','exec','-i',state['container'],'python3','-B',
            '/source/experiments/control-migrations/fault_proxy.py','--upstream-port','5432',
            '--mode','hold','--deadline-seconds','60'],input_pipe=True)
        children.append(proxy); ready_proxy=proxy.event('ready',10)
        target=arguments(db,'killed',nonce,ready_proxy['listen_port'],'initialize')[3:]
        wrapper="import os,sys; print(os.getpid(),flush=True); os.execv(sys.argv[1],sys.argv[1:])"
        peer=Process(['docker','exec',state['container'],'python3','-c',wrapper,*target]);children.append(peer)
        proxy.event('held',10)
        until=time.monotonic()+5
        with peer.condition:
            while b'\n' not in peer.stdout:
                assert peer.process.poll() is None and not peer.overflow and time.monotonic()<until
                peer.condition.wait(min(.1,max(0,until-time.monotonic())))
            assert peer.stdout.endswith(b'\n') and peer.stdout.strip().isdigit()
            pid=int(peer.stdout.strip())
        journal=run/'out'/f'initialize_{db}_killed/.bigbrotr-initialization/intent.json'
        journal_before=journal.read_bytes()
        kill="""import json,os,pathlib,signal,sys
pid=int(sys.argv[1]); fd=os.pidfd_open(pid)
assert pathlib.Path(f'/proc/{pid}/exe').resolve()==pathlib.Path(sys.argv[2])
assert pathlib.Path(f'/proc/{pid}/cmdline').read_bytes().split(b'\\0')[:-1]==[v.encode() for v in sys.argv[2:]]
signal.pidfd_send_signal(fd,signal.SIGKILL); os.close(fd)
print(json.dumps({'pid':pid,'signal':'SIGKILL'}))"""
        killed=json.loads(command(['docker','exec',state['container'],'python3','-c',kill,str(pid),*target]))
        assert peer.wait(10)==137 and not peer.stderr
        assert command(['docker','exec',state['container'],'python3','-c',
            "import pathlib,sys; assert not pathlib.Path('/proc/'+sys.argv[1]).exists()",str(pid)])==''
        # This succeeds in reacquiring the actual lock while the remote PG
        # transaction remains open; reserve must still deny recovered custody.
        invoke(db,'killed',nonce,'InvalidInput',mode='recover')
        assert check_reservation(db,nonce,False)['roles']==[]
        assert sql("SELECT count(*) FROM pg_stat_activity WHERE datname='"+db+"' AND application_name='bigbrotr-initialization-probe' AND state='idle in transaction'")=='1'
        proxy.send(action)
        receipt=proxy.event('receipt',10)
        exit_code=proxy.wait(5)
        assert not proxy.stderr and exit_code in (0,3)
        assert receipt['commits_seen']==1 and receipt['commits_forwarded']==(action=='release')
        assert receipt['server_commit_complete'] in ([0,1] if action=='release' else [0])
        # With a dead frontend the proxy may observe EOF before the backend ACK.
        # The same real transaction lock resolves completion before SQL checks.
        sql('BEGIN; SELECT pg_advisory_xact_lock(1111651442,1128354865); COMMIT;',db)
        assert journal.read_bytes()==journal_before
        assert sql("SELECT count(*) FROM pg_depend WHERE refclassid='pg_namespace'::regclass AND refobjid='public'::regnamespace",db)=='0'
        report['cases'].append(dict(name='killed-before-'+action,status='PASS',coordinator=killed,
            coordinator_exit=137,proxy_exit=exit_code,wire=receipt,journal_unchanged=True,
            no_native_objects=True,durable=check_reservation(db,nonce,action=='release')));save()
    assert sql("SELECT count(*) FROM pg_stat_activity WHERE application_name='bigbrotr-initialization-probe' OR usename LIKE 'bb_init_%'")=='0'
    report['status']='PASS'
except BaseException as error:
    report['status']='FAIL'
    report['error']=str(error)
    save()
    raise
finally:
    cleanup=[]
    for child in children:
        was_running=child.process.poll() is None
        child.stop()
        if was_running or child.overflow or any(t.is_alive() for t in child.threads):cleanup.append('unresolved-process')
    if cleanup: report.update(status='INCOMPLETE',cleanup_errors=cleanup,requires_runner_stop=True)
    save()
print(json.dumps(dict(status=report['status'],cases=len(report['cases']))),flush=True)
