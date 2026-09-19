#!/usr/bin/env python3
"""Real installed initializer/recovery journeys in an exactly inventoried Linux lab."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

sys.dont_write_bytecode = True
run = Path(sys.argv[1]).resolve()
source = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(source / 'experiments/control-migrations'))
from fault_cases import Process


def command(args, stdin=None, timeout=60):
    result = subprocess.run(args, input=stdin, text=True, capture_output=True, timeout=timeout)
    assert len(result.stdout) + len(result.stderr) < 2 * 1024 * 1024
    assert result.returncode == 0, (args, result.returncode, 'command failed; payload withheld')
    return result.stdout.strip()


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
assert hashlib.sha256((run / 'out/native-tests/bigbrotr').read_bytes()).hexdigest() == ready['control']
report = dict(source=state['source'], artifacts=ready, status='RUNNING', cases=[])
output = run / 'initialization-cli.json'
output.open('x').close()
work = run / 'out/initialization-cli'
work.mkdir(mode=0o700)
children = []


def save():
    output.write_text(json.dumps(report, indent=2) + '\n')


def sql(query, database='postgres'):
    return command(['docker', 'exec', '-i', state['postgres'], 'psql', '-XAt',
                    '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', database], query)


def fresh(name):
    database = 'bbcli_' + name
    sql(f'CREATE DATABASE {database};')
    sql('REVOKE CREATE ON SCHEMA public FROM PUBLIC;', database)
    return database


def durable(database):
    dump = command(['docker', 'exec', state['postgres'], 'pg_dump', '-U', 'postgres', '-d', database])
    dump = '\n'.join(line for line in dump.splitlines() if not line.startswith(('\\restrict ', '\\unrestrict ')))
    roles = sql("SELECT md5(json_build_object('roles',(SELECT json_agg((to_jsonb(r)-'rolpassword')||jsonb_build_object('credential',md5(rolpassword),'xmin',xmin::text) ORDER BY oid) FROM pg_authid r),'members',(SELECT json_agg(to_jsonb(m)||jsonb_build_object('xmin',xmin::text) ORDER BY roleid,member,grantor) FROM pg_auth_members m),'comments',(SELECT json_agg(to_jsonb(c) ORDER BY objoid,classoid) FROM pg_shdescription c))::text)")
    return dict(database_sha256=hashlib.sha256(dump.encode()).hexdigest(), global_roles_md5=roles)


def config(database, label, *, root=None, port=5432, extra='', dataset='01'*16):
    if root is None:
        root = '/out/initialization-cli/' + label + '-data'
        (run / root.removeprefix('/')).mkdir(mode=0o700)
    scratch = work / (label + '-scratch')
    scratch.mkdir(mode=0o700)
    path = work / (label + '.toml')
    path.write_text(f'''[config]
schema_version=1
recipe="archive-only"
authority_mode="managed"
[topology]
local_roles=["control","discoverer","validator","monitor","sync","ingest","archive"]
instance_id="{'03'*16}"
dataset_id="{dataset}"
[archive]
catalog_id="{'02'*16}"
[storage]
archive_root="{root}"
scratch_root="/out/initialization-cli/{scratch.name}"
[postgres.catalog]
endpoint="127.0.0.1:{port}"
database="{database}"
user="CatalogManager"
password_ref="file:/out/initialization-cli/manager"
{extra}
''')
    return '/out/initialization-cli/' + path.name, root


def args(action, path):
    result = ['/out/native-tests/bigbrotr', 'archive', action, '--file', path]
    if action != 'inspect':
        result += ['--initializer-user', 'postgres', '--initializer-password-ref', 'file:/out/initialization-cli/initializer']
    if action == 'initialize': result += ['--worker-root', '/out/archive-worker']
    return result


def invoke(action, path):
    start = time.monotonic()
    actual = subprocess.run(['docker', 'exec', state['container'], *args(action, path)],
                            capture_output=True, timeout=160)
    assert len(actual.stdout) + len(actual.stderr) < 8192
    # CLI diagnostics are fixed reasons, but never retain raw unexpected errors.
    return dict(exit=actual.returncode, output=json.loads(actual.stdout) if actual.stdout else None,
                stderr_nonempty=bool(actual.stderr), seconds=time.monotonic()-start), actual.stderr


def case(name, operation):
    item = dict(name=name, status='INCOMPLETE')
    report['cases'].append(item); save()
    operation(item)
    item['status'] = 'PASS'; save()


def bootstrap(database):
    oid = sql("SELECT oid FROM pg_database WHERE datname='" + database + "'")
    value = sql(f"SELECT shobj_description(oid,'pg_authid') FROM pg_roles WHERE rolname='bb_init_database_{oid}'")
    if not value: return None
    prefix, nonce, boot_oid, manager_oid = value.split(':')
    assert prefix == 'bigbrotr-catalog-initialization/1'
    role = 'bb_init_' + nonce
    assert sql(f"SELECT oid FROM pg_roles WHERE rolname='{role}'") == boot_oid
    assert sql('SELECT oid FROM pg_roles WHERE rolname=\'CatalogManager\'') == manager_oid
    return role, int(boot_oid)


def installed(database):
    role, oid = bootstrap(database)
    assert sql(f"SELECT NOT rolcanlogin AND rolpassword IS NULL FROM pg_authid WHERE oid={oid}") == 't'
    assert sql(f"SELECT count(*) FROM pg_stat_activity WHERE usesysid={oid}") == '0'
    # Independent expected initial owner and exact transferred metadata objects.
    assert sql("SELECT ordinal::text||'|'||enabled::text FROM bigbrotr_archive.owner", database) == '0|false'
    assert sql("SELECT count(*) FROM bigbrotr_archive.installation", database) == '1'
    assert sql("SELECT count(*) FROM bigbrotr_archive.attempts", database) == '0'
    actual = sql("SELECT n.nspname||'|'||c.relname||'|'||r.rolname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_roles r ON r.oid=c.relowner WHERE n.nspname IN ('public','bigbrotr_archive') AND c.relkind='r' ORDER BY 1", database).splitlines()
    native = json.loads((source / 'contracts/archive/catalog-native-v1.json').read_text())['tables']
    expected = sorted(['public|' + name + '|CatalogManager' for name in native]
        + ['bigbrotr_archive|' + name + '|CatalogManager' for name in ['installation', 'owner', 'attempts', 'scientific_schema']])
    assert len(native) == 28 and actual == expected
    return dict(bootstrap_oid=oid, bootstrap_retired=True, native_tables=28, guard_tables=4, attempts=0)


def unchanged(name, database, action, path, exit_code):
    def execute(item):
        before = durable(database)
        observed, _ = invoke(action, path)
        item.update(observed=observed, before=before, after=durable(database))
        assert observed['exit'] == exit_code and item['after'] == before
    case(name, execute)


def managed_cli(action, path, label):
    # The wrapper owns and joins the actual CLI, and reports no raw diagnostics.
    script = '''import json,subprocess,sys
from pathlib import Path
p=subprocess.Popen(sys.argv[2:],stdout=subprocess.PIPE,stderr=subprocess.PIPE,env={})
Path(sys.argv[1]).write_text(str(p.pid))
out,err=p.communicate(timeout=160)
print(json.dumps(dict(exit=p.returncode,output=json.loads(out) if out else None,stderr_nonempty=bool(err))))
'''
    process = Process(['docker', 'exec', state['container'], 'python3', '-c', script,
        '/out/initialization-cli/' + label + '.pid', *args(action, path)])
    children.append(process)
    return process


def terminate_cli(label):
    script = '''import os,signal,sys
from pathlib import Path
pid=int(Path(sys.argv[1]).read_text())
assert Path('/proc/'+str(pid)+'/cmdline').read_bytes().split(b'\\0')[0]==b'/out/native-tests/bigbrotr'
os.kill(pid,signal.SIGTERM)
'''
    command(['docker', 'exec', state['container'], 'python3', '-c', script,
             '/out/initialization-cli/' + label + '.pid'])


def finished(process):
    assert process.wait(40) == 0
    return json.loads(process.stdout)


try:
    for target, secret in [('initializer', 'postgres'), ('manager', 'CatalogManager')]:
        path = work / target
        path.write_bytes((run / 'secrets' / secret).read_bytes()); path.chmod(0o600)
    password = (work / 'manager').read_text()
    assert len(password) == 64 and all(c in '0123456789abcdef' for c in password)
    sql('CREATE ROLE "CatalogManager" LOGIN NOSUPERUSER CREATEROLE NOCREATEDB '
        "NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD '" + password + "';")
    del password
    database = fresh('clean')
    path, root = config(database, 'clean', extra='[runtime.processes.application]\npayload_bytes="128MiB"')
    def clean(item):
        observed, _ = invoke('initialize', path)
        item['observed'] = observed; save()
        assert observed['exit'] == 0 and observed['output']['new_installation'] is True
        item['independent'] = installed(database)
    case('initialize-real-worker-with-128mib-parent-payload', clean)
    unchanged('fresh-manager-inspect', database, 'inspect', path, 0)
    unchanged('restart-recover-idempotent', database, 'recover', path, 0)
    unchanged('fresh-initialize-retry-refuses-no-row-change', database, 'initialize', path, 4)
    wrong, _ = config(database, 'wrong-dataset', root=root, dataset='04'*16)
    unchanged('wrong-dataset-recovery-refuses', database, 'recover', wrong, 4)

    def blocked_output(item):
        before = durable(database)
        script = '''import fcntl,json,os,subprocess,sys,time
r,w=os.pipe(); fcntl.fcntl(w,fcntl.F_SETFL,os.O_NONBLOCK)
try:
 while True: os.write(w,b'x'*4096)
except BlockingIOError: pass
fcntl.fcntl(w,fcntl.F_SETFL,0)
t=time.monotonic(); p=subprocess.Popen(sys.argv[1:],stdout=w,stderr=subprocess.PIPE,env={})
_,err=p.communicate(timeout=10)
print(json.dumps(dict(exit=p.returncode,seconds=time.monotonic()-t,stderr_nonempty=bool(err))))
os.close(w);os.close(r)
'''
        value = json.loads(command(['docker', 'exec', state['container'], 'python3', '-c', script, *args('recover', path)]))
        item.update(observed=value, before=before, after=durable(database))
        assert value['exit'] == 1 and value['seconds'] < 5 and before == item['after']
    case('actual-full-output-pipe-bounded-exit', blocked_output)

    for label, mode, nonce in [('stranded', 'reserve', 51), ('complete', 'initialize', 52), ('partial', 'initialize', 53)]:
        db = fresh(label)
        fixture = json.loads(command(['docker', 'exec', state['container'], '/out/native-tests/catalog-initialization-probe', db, 'owner', str(nonce), '5432', mode]))
        assert fixture['result']['status'] == 'reserved'
        fixture_root = '/out/initialize_' + db + '_owner'
        selected, _ = config(db, label, root=fixture_root)
        if label == 'partial': sql('CREATE TABLE unrelated(marker integer); INSERT INTO unrelated VALUES(73)', db)
        def recover_case(item, db=db, selected=selected, label=label):
            before = durable(db)
            observed, error = invoke('recover', selected)
            item.update(observed=observed, before=before, after=durable(db))
            assert observed['exit'] == (0 if label == 'complete' else 4)
            role, oid = bootstrap(db)
            assert sql(f"SELECT NOT rolcanlogin AND rolpassword IS NULL FROM pg_authid WHERE oid={oid}") == 't'
            if label == 'complete': item['independent'] = installed(db)
            else:
                assert before['database_sha256'] == item['after']['database_sha256']
                if label == 'stranded': assert b'pristine reservation is stranded' in error
        case('recover-' + label, recover_case)
        unchanged('recover-' + label + '-repeat', db, 'recover', selected, 0 if label == 'complete' else 4)

    # Actual first reservation COMMIT lost before/after PostgreSQL completion.
    for label, mode, expected in [('before', 'before-forward', False), ('after', 'after-complete', True)]:
        db = fresh(label)
        proxy = Process(['docker', 'exec', '-i', state['container'], 'python3', '-B',
            '/source/experiments/control-migrations/fault_proxy.py', '--upstream-port', '5432',
            '--mode', mode, '--deadline-seconds', '90'], input_pipe=True)
        children.append(proxy)
        event = proxy.event('ready', 10)
        selected, fault_root = config(db, label, port=event['listen_port'])
        direct, _ = config(db, label+'-direct', root=fault_root)
        def fault(item, db=db, selected=selected, direct=direct, proxy=proxy, expected=expected):
            observed, _ = invoke('initialize', selected)
            assert observed['exit'] == 4
            receipt = proxy.event('receipt', 10); assert proxy.wait(10) == 0
            assert receipt['commits_seen'] == 1 and receipt['commits_forwarded'] == int(expected)
            assert receipt['client_commit_complete'] == 0
            recovered, error = invoke('recover', direct)
            assert recovered['exit'] == 4
            assert (bootstrap(db) is not None) == expected
            if expected:
                _, oid = bootstrap(db)
                assert sql(f"SELECT NOT rolcanlogin AND rolpassword IS NULL FROM pg_authid WHERE oid={oid}") == 't'
            assert (b'pristine reservation is stranded' if expected else b'no current database reservation') in error
            assert sql("SELECT count(*) FROM pg_class WHERE relnamespace='public'::regnamespace AND relkind='r'", db) == '0'
            item.update(observed=observed, proxy=receipt, recovered=recovered,
                        reservation_present=expected, native_tables=0, durable=durable(db))
        case('lost-reservation-commit-' + label, fault)

    for action in ['close', 'release']:
        db = fresh('cancel_' + action)
        proxy = Process(['docker', 'exec', '-i', state['container'], 'python3', '-B',
            '/source/experiments/control-migrations/fault_proxy.py', '--upstream-port', '5432',
            '--mode', 'hold', '--deadline-seconds', '90'], input_pipe=True)
        children.append(proxy)
        event = proxy.event('ready', 10)
        label = 'cancel-' + action
        selected, retained_root = config(db, label, port=event['listen_port'])
        direct, _ = config(db, label+'-direct', root=retained_root)
        def cancelled_commit(item, db=db, label=label, selected=selected, direct=direct, proxy=proxy, action=action):
            client = managed_cli('initialize', selected, label)
            proxy.event('held', 15)
            start = time.monotonic(); terminate_cli(label)
            observed = finished(client)
            elapsed = time.monotonic()-start
            assert observed['exit'] == 143 and elapsed < 5
            recovery = managed_cli('recover', direct, label+'-recovery')
            until = time.monotonic()+3
            waiting = False
            while time.monotonic() < until:
                waiting = sql(f"SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname='{db}' AND wait_event_type='Lock' AND wait_event='advisory')") == 't'
                if waiting: break
                time.sleep(.05)
            assert waiting
            proxy.send(action)
            receipt = proxy.event('receipt', 10); assert proxy.wait(10) == 0
            resolved = finished(recovery)
            assert resolved['exit'] == 4
            expected = action == 'release'
            assert (bootstrap(db) is not None) == expected
            if expected:
                _, oid = bootstrap(db)
                assert sql(f"SELECT NOT rolcanlogin AND rolpassword IS NULL FROM pg_authid WHERE oid={oid}") == 't'
            assert sql("SELECT count(*) FROM pg_class WHERE relnamespace='public'::regnamespace AND relkind='r'", db) == '0'
            item.update(observed=observed, stop_seconds=elapsed, actual_recovery_lock_wait=True,
                        proxy=receipt, recovered=resolved, reservation_present=expected, native_tables=0)
        case('cancel-actual-driver-held-commit-' + action, cancelled_commit)

    db = fresh('native_cancel')
    selected, retained_root = config(db, 'native-cancel')
    def cancel_native(item):
        sql('ALTER SYSTEM SET post_auth_delay=6'); sql('SELECT pg_reload_conf()')
        try:
            client = managed_cli('initialize', selected, 'native-cancel')
            # Observe the actual native executable with an established libpq
            # socket before signaling its coordinator during delayed startup.
            script = '''import json,os,time
from pathlib import Path
until=time.monotonic()+15
while time.monotonic()<until:
 for p in Path('/proc').iterdir():
  if not p.name.isdigit(): continue
  try:
   if p.joinpath('cmdline').read_bytes().split(b'\\0')[0]!=b'/out/archive-worker/bigbrotr-archive-worker': continue
   sockets={os.readlink(fd) for fd in p.joinpath('fd').iterdir()}
   for line in Path('/proc/net/tcp').read_text().splitlines()[1:]:
    fields=line.split()
    if fields[2].endswith(':1538') and fields[3]=='01' and 'socket:['+fields[9]+']' in sockets:
     print(json.dumps(dict(pid=int(p.name),established=True)));raise SystemExit
  except (FileNotFoundError,PermissionError,ProcessLookupError): pass
 time.sleep(.02)
raise RuntimeError('native established socket not observed')
'''
            worker = json.loads(command(['docker', 'exec', state['container'], 'python3', '-c', script], timeout=20))
            start = time.monotonic(); terminate_cli('native-cancel')
            observed = finished(client); elapsed = time.monotonic()-start
            assert observed['exit'] == 143 and elapsed < 6
            command(['docker', 'exec', state['container'], 'python3', '-c',
                "from pathlib import Path;import sys;assert not Path('/proc/'+sys.argv[1]).exists()", str(worker['pid'])])
            item.update(observed=observed, worker=worker, stop_seconds=elapsed, native_process_reaped=True)
        finally:
            sql('ALTER SYSTEM RESET post_auth_delay'); sql('SELECT pg_reload_conf()')
        recovered, error = invoke('recover', selected)
        assert recovered['exit'] == 4 and b'pristine reservation is stranded' in error
        _, oid = bootstrap(db)
        assert sql(f"SELECT NOT rolcanlogin AND rolpassword IS NULL FROM pg_authid WHERE oid={oid}") == 't'
        assert sql("SELECT count(*) FROM pg_class WHERE relnamespace='public'::regnamespace AND relkind='r'", db) == '0'
        item.update(recovered=recovered, bootstrap_retired=True, native_tables=0)
    case('cancel-real-native-libpq-startup-and-recover', cancel_native)
    report['status'] = 'PASS'; save()
    print(json.dumps(dict(status='PASS', source=state['source'], cases=len(report['cases']))))
except BaseException as error:
    report.update(status='FAIL', failure_type=type(error).__name__); save()
    raise
finally:
    for child in children: child.stop()
