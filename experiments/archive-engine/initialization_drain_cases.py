#!/usr/bin/env python3
"""Actual archive recover with residual SQL after joined client destruction.

The fault producer is an explicit tokio-postgres fixture, not a native worker.
Only the separately inventoried, isolated native lab is accepted.
"""
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time

sys.dont_write_bytecode = True
source = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(source / 'experiments/control-migrations'))
from fault_cases import Process


def command(args, input=None, timeout=45):
    result = subprocess.run(args, input=input, text=True, capture_output=True, timeout=timeout)
    assert result.returncode == 0 and len(result.stdout) + len(result.stderr) < 2 * 1024 * 1024, 'command failed; payload withheld'
    return result.stdout.strip()


run = Path(sys.argv[1]).resolve()
state = json.loads((run / 'resources.json').read_text())
assert source in (run / 'source', run / 'harness-source')
assert command(['git', '-C', str(run / 'source'), 'rev-parse', 'HEAD']) == state['source']
assert not command(['git', '-C', str(source), 'status', '--porcelain', '--untracked-files=all'])
harness_revision = command(['git', '-C', str(source), 'rev-parse', 'HEAD'])
changed = command(['git', '-C', str(source), 'diff', '--name-only', state['source'], harness_revision]).splitlines()
assert all(name.startswith('docs/') or name == 'experiments/archive-engine/initialization_drain_cases.py' for name in changed)
harness = dict(source=harness_revision, tree=command(['git', '-C', str(source), 'rev-parse', 'HEAD^{tree}']),
    script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), changed_paths=changed,
    production_and_fixture_inputs_unchanged=True)
for name in [state['container'], state['postgres']]:
    actual = json.loads(command(['docker', 'inspect', name]))[0]
    assert actual['Config']['Labels']['org.bigbrotr.publication'] == state['owner'] and actual['State']['Running']
ready = json.loads((run / 'out/native-tests/ready.json').read_text())
assert ready['source'] == state['source']
for name, field in [('bigbrotr', 'control'), ('catalog-initialization-probe', 'initializer')]:
    assert hashlib.sha256((run / 'out/native-tests' / name).read_bytes()).hexdigest() == ready[field]
work = run / 'out/initialization-drain'
work.mkdir(mode=0o700)
output = run / 'initialization-drain.json'
output.open('x').close()
report = dict(source=state['source'], harness=harness, artifacts=ready, status='RUNNING', cases=[],
              fault_producer='custodied tokio-postgres fixture with actual queued SQL, driver aborted and joined')
children = []


def save():
    output.write_text(json.dumps(report, indent=2) + '\n')


def sql(query, database='postgres'):
    return command(['docker', 'exec', '-i', state['postgres'], 'psql', '-XAt',
                    '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', database], query)


def until(predicate, seconds=5):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(.025)
    raise AssertionError('expected state not observed before deadline')


def durable(database, root):
    dump = command(['docker', 'exec', state['postgres'], 'pg_dump', '-U', 'postgres', '-d', database])
    dump = '\n'.join(line for line in dump.splitlines() if not line.startswith(('\\restrict ', '\\unrestrict ')))
    return dict(database_sha256=hashlib.sha256(dump.encode()).hexdigest(),
                files={str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
                       for p in sorted(root.rglob('*')) if p.is_file()})


def active(oid):
    return json.loads(sql(f"SELECT COALESCE(json_agg(json_build_object('pid',pid,'start',backend_start::text,'database',datname,'state',state,'wait',wait_event) ORDER BY pid),'[]') FROM pg_stat_activity WHERE usesysid={oid}"))


def retired(oid):
    return sql(f'SELECT NOT rolcanlogin AND rolpassword IS NULL FROM pg_authid WHERE oid={oid}') == 't'


def cli(path, label):
    script = '''import json,os,subprocess,sys,time
from pathlib import Path
start=time.monotonic();p=subprocess.Popen(sys.argv[2:],stdout=subprocess.PIPE,stderr=subprocess.PIPE,env={})
record=dict(pid=p.pid,start=Path('/proc/'+str(p.pid)+'/stat').read_text().rsplit(')',1)[1].split()[19],argv=sys.argv[2:])
target=Path(sys.argv[1]);tmp=target.with_suffix('.tmp')
with tmp.open('x') as f: json.dump(record,f)
os.rename(tmp,target)
out,err=p.communicate(timeout=40)
assert len(out)+len(err)<8192
print(json.dumps(dict(exit=p.returncode,seconds=time.monotonic()-start,stdout_empty=not out,
 stranded=b'pristine reservation is stranded' in err,deadline=b'deadline exceeded' in err,interrupted=b'operation interrupted' in err)))
'''
    process = Process(['docker', 'exec', state['container'], 'python3', '-c', script,
        '/out/initialization-drain/' + label + '.pid', '/out/native-tests/bigbrotr',
        'archive', 'recover', '--file', path, '--initializer-user', 'postgres',
        '--initializer-password-ref', 'file:/out/initialization-drain/initializer'])
    children.append(process)
    return process


def inspect_or_signal_cli(label, number=0):
    script = '''import json,os,signal,sys
from pathlib import Path
record=json.loads(Path(sys.argv[1]).read_text());pid=record['pid'];fd=os.pidfd_open(pid)
try:
 proc=Path('/proc/'+str(pid))
 fields=proc.joinpath('stat').read_text().rsplit(')',1)[1].split()
 assert fields[19]==record['start'] and fields[0]!='Z'
 assert proc.joinpath('cmdline').read_bytes().rstrip(b'\\0').split(b'\\0')==[s.encode() for s in record['argv']]
 assert os.readlink(proc/'exe')=='/out/native-tests/bigbrotr'
 signal.pidfd_send_signal(fd,int(sys.argv[2]))
 print(json.dumps(dict(pid=pid,start=record['start'],state=fields[0],signal=int(sys.argv[2]))))
finally: os.close(fd)
'''
    return json.loads(command(['docker', 'exec', state['container'], 'python3', '-c', script,
        '/out/initialization-drain/' + label + '.pid', str(number)]))


def finished(process):
    assert process.wait(38) == 0
    return json.loads(process.stdout)


try:
    report['server_client_connection_check_interval'] = sql('SHOW client_connection_check_interval')
    assert report['server_client_connection_check_interval'] == '0'
    for file, role in [('initializer', 'postgres'), ('manager', 'CatalogManager')]:
        path = work / file
        path.write_bytes((run / 'secrets' / role).read_bytes()); path.chmod(0o600)
    password = (work / 'manager').read_text()
    assert len(password) == 64 and all(c in '0123456789abcdef' for c in password)
    sql('CREATE ROLE "CatalogManager" LOGIN NOSUPERUSER CREATEROLE NOCREATEDB '
        "NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD '" + password + "';")
    del password
    for index, label in enumerate(['same_database', 'other_database', 'timeout', 'sigterm', 'sigint'], 71):
        item = dict(name=label, status='INCOMPLETE'); report['cases'].append(item); save()
        database = 'bbdrain_' + label
        target = 'postgres' if label == 'other_database' else database
        sql(f'CREATE DATABASE {database};')
        sql('REVOKE CREATE ON SCHEMA public FROM PUBLIC;', database)
        blocker = Process(['docker', 'exec', '-i', state['postgres'], 'psql', '-XAt', '-v', 'ON_ERROR_STOP=1',
                           '-U', 'postgres', '-d', target], input_pipe=True)
        children.append(blocker)
        blocker.process.stdin.write(b"SET application_name='bigbrotr-drain-blocker'; SELECT pg_advisory_lock(1128419913,1);\n")
        blocker.process.stdin.flush()
        until(lambda: sql("SELECT EXISTS(SELECT 1 FROM pg_locks l JOIN pg_stat_activity a ON a.pid=l.pid WHERE a.application_name='bigbrotr-drain-blocker' AND l.locktype='advisory' AND l.granted)") == 't')
        fixture = json.loads(command(['docker', 'exec', state['container'], '/out/native-tests/catalog-initialization-probe',
            database, 'owner', str(index), '5432', 'drain-other-database' if target != database else 'drain']))
        assert fixture['driver_ok'] and fixture['commit_started']
        reservation = fixture['result']; residual = reservation['residual_backend']; oid = reservation['bootstrap_oid']
        assert residual['queued_sql_observed'] and residual['actual_driver_destroyed_and_joined']
        expected = [dict(pid=residual['pid'], start=residual['backend_start'], database=target, state='active', wait='advisory')]
        assert active(oid) == expected
        root = run / 'out' / ('initialize_' + database + '_owner')
        before = durable(database, root)
        assert set(before['files']) == {'.bigbrotr-initialization/custody.lock', '.bigbrotr-initialization/intent.json'}
        scratch = work / (label + '-scratch'); scratch.mkdir(mode=0o700)
        path = work / (label + '.toml')
        path.write_text(f'''[config]
schema_version=1
recipe="archive-only"
authority_mode="managed"
[topology]
local_roles=["control","discoverer","validator","monitor","sync","ingest","archive"]
instance_id="{'03'*16}"
dataset_id="{'01'*16}"
[archive]
catalog_id="{'02'*16}"
[storage]
archive_root="/out/{root.name}"
scratch_root="/out/initialization-drain/{scratch.name}"
[postgres.catalog]
endpoint="127.0.0.1:5432"
database="{database}"
user="CatalogManager"
password_ref="file:/out/initialization-drain/manager"
''')
        path = '/out/initialization-drain/' + path.name
        client = cli(path, label)
        until(lambda: retired(oid))
        # A skipped drain would have returned the pristine refusal by now.
        time.sleep(.3)
        assert client.process.poll() is None and active(oid) == expected
        remote_cli = inspect_or_signal_cli(label)
        # Observe the real consumer's fresh polling connection too. A delayed
        # docker-exec wrapper cannot stand in for the actual CLI or its drain.
        drain = json.loads(sql(f"SELECT COALESCE(json_agg(json_build_object('pid',pid,'start',backend_start::text,'state',state)),'[]') FROM pg_stat_activity WHERE datname='{database}' AND usename='postgres' AND query='SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE usesysid=$1::oid)'"))
        assert len(drain) == 1
        assert durable(database, root) == before
        item.update(fixture=fixture, blocked_backend=expected, before=before,
                    actual_recovery_retired_then_waited=True, remote_cli=remote_cli, drain_backend=drain)
        save()
        if label in ['timeout', 'sigterm', 'sigint']:
            if label != 'timeout':
                signal_start = time.monotonic()
                item['signal'] = inspect_or_signal_cli(label, 15 if label == 'sigterm' else 2)
            observed = finished(client)
            if label != 'timeout':
                item['signal_to_exit_seconds'] = time.monotonic() - signal_start
                assert item['signal_to_exit_seconds'] < 5
            assert observed['exit'] == {'timeout': 4, 'sigterm': 143, 'sigint': 130}[label]
            assert observed['deadline' if label == 'timeout' else 'interrupted'] and not observed['stranded']
            assert observed['stdout_empty']
            assert 29 <= observed['seconds'] < 36 if label == 'timeout' else observed['seconds'] < 5
            assert active(oid) == expected and retired(oid) and durable(database, root) == before
            item['unresolved'] = observed; save()
        blocker.process.stdin.write(b'\\q\n'); blocker.process.stdin.flush()
        assert blocker.wait(5) == 0
        until(lambda: not active(oid))
        resolved = finished(client) if label in ['same_database', 'other_database'] else finished(cli(path, label + '-retry'))
        assert resolved['exit'] == 4 and resolved['stranded'] and not resolved['deadline'] and resolved['stdout_empty']
        assert retired(oid) and durable(database, root) == before
        assert sql("SELECT count(*) FROM pg_class WHERE relnamespace='public'::regnamespace AND relkind='r'", database) == '0'
        item.update(resolved=resolved, after=durable(database, root), backend_gone=True,
                    bootstrap_retired=True, native_tables=0, status='PASS'); save()
    report['status'] = 'PASS'; save()
    print(json.dumps(dict(status='PASS', source=state['source'], cases=len(report['cases']))))
except BaseException as error:
    report.update(status='FAIL', failure_type=type(error).__name__); save()
    raise
finally:
    for child in children: child.stop()
