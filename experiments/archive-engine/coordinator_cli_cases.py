#!/usr/bin/env python3
"""Actual installed coordinator setup/startup in an inventoried native lab.

One case installs a real native catalog; refusal fixtures create only local
reservations. All Control configuration/activation uses the actual executable.
"""
import argparse
import hashlib
import json
import re
from pathlib import Path
import select
import subprocess
import sys
import time

sys.dont_write_bytecode = True
from signed_publication_cases import Process, command, execute, sha
from lab import change

SIGNAL = """import json,os,signal,sys
from pathlib import Path
expected=[v.encode() for v in sys.argv[2:]]+[b'']; found=[]
for p in Path('/proc').iterdir():
 if p.name.isdigit():
  try:
   if (p/'cmdline').read_bytes().split(b'\\0')==expected:found.append(p)
  except (FileNotFoundError,PermissionError,ProcessLookupError):pass
assert len(found)==1
p=found[0];start=(p/'stat').read_text().rsplit(')',1)[1].split()[19];fd=os.pidfd_open(int(p.name))
try:
 assert (p/'cmdline').read_bytes().split(b'\\0')==expected
 assert (p/'stat').read_text().rsplit(')',1)[1].split()[19]==start
 actual=(p/'exe').stat();wanted=Path(sys.argv[2]).stat()
 assert (actual.st_dev,actual.st_ino)==(wanted.st_dev,wanted.st_ino)
 signal.pidfd_send_signal(fd,getattr(signal,'SIG'+sys.argv[1]))
finally:os.close(fd)
print(json.dumps(dict(pid=int(p.name),start_ticks=start,pidfd=True,signal=sys.argv[1])))
"""


def run(root, namespace):
    source = Path(__file__).resolve().parents[2]
    resources = json.loads((root/'resources.json').read_text())
    assert source in (root/'source', root/'harness-source')
    assert command('git', '-C', str(root/'source'), 'rev-parse', 'HEAD') == resources['source']
    harness_revision = command('git', '-C', str(source), 'rev-parse', 'HEAD')
    changed = command('git', '-C', str(source), 'diff', '--name-only', resources['source'], harness_revision).splitlines()
    assert all(name.startswith('docs/') or name == 'experiments/archive-engine/coordinator_cli_cases.py' for name in changed)
    assert not namespace or re.fullmatch('[a-z]{1,12}', namespace)
    assert not command('git', '-C', str(source), 'status', '--porcelain', '--untracked-files=all')
    for key in ['container_id', 'postgres_id']:
        c = json.loads(command('docker', 'inspect', resources[key]))[0]
        assert c['Config']['Labels']['org.bigbrotr.publication'] == resources['owner']
        assert c['State']['Running'] and not c['State']['OOMKilled']
    ready = json.loads((root/'out/native-tests/ready.json').read_text())
    assert ready['source'] == resources['source']
    for label, name in [('control', 'bigbrotr'), ('coordinator_probe', 'archive-coordinator-probe'),
                        ('attempt_probe', 'archive-attempt-probe')]:
        assert sha(root/'out/native-tests'/name) == ready[label]
    directory_name = 'coordinator-cli'+('-'+namespace if namespace else '')
    directory = root/'out'/directory_name
    directory.mkdir(mode=0o700)
    report = dict(status='INCOMPLETE', source=resources['source'], tree=resources['tree'],
                  artifacts=ready, harness_sha256=sha(Path(__file__)),
                  harness=dict(source=harness_revision, changed_paths=changed, production_inputs_unchanged=True),
                  namespace=namespace, cases=[], commands=[], processes=[])
    report_path = root/('coordinator-cli-cases'+('-'+namespace if namespace else '')+'.json')
    assert not report_path.exists()
    processes = []
    runner, pg = resources['container_id'], resources['postgres_id']
    binary = '/out/native-tests/bigbrotr'

    def save():
        report_path.write_text(json.dumps(report, indent=2)+'\n')

    def passed(name, **details):
        report['cases'].append(dict(name=name, status='PASS', **details))
        save()

    def sql(db, statement):
        return command('docker', 'exec', '-i', pg, 'psql', '-XqAt', '-v', 'ON_ERROR_STOP=1',
                       '-U', 'postgres', '-d', db, input=statement)

    def cli(*args, code=0, contains=None):
        record = dict(args=args)
        report['commands'].append(record)
        try:
            execute(['docker', 'exec', runner, binary, *args], 150, record)
        finally:
            save()
        assert record['exit'] == code, record
        if contains:
            assert contains in record['stderr'] and not record['stdout']
        else:
            assert not record['stderr']
        return json.loads(record['stdout']) if record['stdout'] else None

    def prepare(label, target=15, native=False, state=True):
        database = 'bbcli_'+(namespace+'_' if namespace else '')+label
        home = directory/label
        home.mkdir(mode=0o700)
        for name in ['data', 'state', 'scratch']:
            (home/name).mkdir(mode=0o700)
        for role in ['postgres', 'bigbrotr_control', 'CatalogManager']:
            p = home/role
            p.write_bytes((root/'secrets'/role).read_bytes())
            p.chmod(0o600)
        container_home = '/out/'+directory_name+'/'+label
        text = (source/'config/recipes/archive-only.toml').read_text()
        for section, key, value in [
            ('administration', 'enabled', False), ('topology', 'dataset_id', '01'*16),
            ('topology', 'instance_id', '02'*16), ('archive', 'catalog_id', '5a'*16),
            ('postgres.control', 'endpoint', '127.0.0.1:5432'),
            ('postgres.control', 'database', database),
            ('postgres.control', 'password_ref', 'file:'+container_home+'/bigbrotr_control'),
            ('postgres.catalog', 'database', database+'_catalog'),
            ('postgres.catalog', 'user', 'CatalogManager'),
            ('postgres.catalog', 'password_ref', 'file:'+container_home+'/CatalogManager'),
            ('storage', 'archive_root', container_home+'/data'),
            ('storage', 'archive_state_root', container_home+'/state'),
            ('storage', 'scratch_root', container_home+'/scratch'),
        ]:
            text = change(text, section, key, value)
        config = home/'instance.toml'
        config.write_text(text)
        path = container_home+'/instance.toml'
        sql('postgres', f'CREATE DATABASE {database}')
        cli('migrate', 'apply', '--file', path, '--target', str(target), '--user', 'postgres',
            '--password-ref', 'file:'+container_home+'/postgres')
        sql(database, 'GRANT USAGE ON SCHEMA control TO bigbrotr_control; GRANT SELECT,INSERT,UPDATE ON ALL TABLES IN SCHEMA control TO bigbrotr_control; GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA control TO bigbrotr_control;')
        cli('config', 'initialize', '--file', path)
        cli('config', 'activate-initial', '--file', path, '--request-id', '44'*16, '--expected-revision', '1')
        if native:
            sql('postgres', f'CREATE DATABASE {database}_catalog')
            sql(database+'_catalog', f'REVOKE ALL ON DATABASE {database}_catalog FROM PUBLIC; REVOKE ALL ON SCHEMA public FROM PUBLIC')
            value = cli('archive', 'initialize', '--file', path, '--initializer-user', 'postgres',
                        '--initializer-password-ref', 'file:'+container_home+'/postgres', '--worker-root', '/out/archive-worker')
            assert value['new_installation'] and value['manager_inspection'] == 'verified' and value['ready'] is False
        else:
            assert command('docker', 'exec', runner, '/out/native-tests/archive-coordinator-probe',
                           'reserve', container_home+'/data') == ''
        if state:
            cli('archive', 'state-initialize', '--file', path)
        return database, home, path

    def rows(db):
        value = json.loads(sql(db, "SELECT json_build_object('bindings',(SELECT coalesce(json_agg(t),'[]') FROM (SELECT c.*,xmin::text AS xmin FROM control.archive_coordinators c)t),'attempts',(SELECT coalesce(json_agg(t ORDER BY ordinal),'[]') FROM (SELECT a.*,xmin::text AS xmin FROM control.archive_attempts a)t))"))
        value['persistence'] = json.loads(sql(db, "SELECT coalesce(json_agg(t),'[]') FROM (SELECT p.*,xmin::text AS xmin FROM control.archive_persistence p)t")) if sql(db, "SELECT to_regclass('control.archive_persistence') IS NOT NULL") == 't' else []
        return value

    def no_clients(db):
        deadline = time.monotonic()+6
        while sql(db, "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control'") != '0':
            assert time.monotonic() < deadline
            time.sleep(.01)

    def start(path, *prefix):
        args = [binary, *prefix, '--file', path]
        p = Process(['docker', 'exec', runner, *args])
        record = dict(args=args, exit=None, remote_exclusion='UNKNOWN')
        report['processes'].append(record)
        processes.append((p, record))
        save()
        return p, record

    def signal(item, name='TERM'):
        _, record = item
        record['signal'] = json.loads(command('docker', 'exec', runner, 'python3', '-c', SIGNAL, name, *record['args']))
        save()

    def terminal(item, code):
        p, record = item
        try:
            record['exit'] = p.wait(8)
        finally:
            record.update(stdout=p.stdout.decode(), stderr=p.stderr.decode())
            save()
        assert record['exit'] == code, record
        return record

    def idle(item):
        p, _ = item
        deadline = time.monotonic()+10
        while True:
            with p.condition:
                lines = bytes(p.stdout).split(b'\n')[:-1]
                if lines:
                    value = json.loads(lines[0])
                    assert value['state'] == 'idle' and value['archive_intents']['issued_through'] == '0'
                    assert value['listeners'] == value['native_stores'] == []
                    return value
                assert p.process.poll() is None and time.monotonic() < deadline
                p.condition.wait(.02)

    save()
    try:
        if sql('postgres', "SELECT count(*) FROM pg_roles WHERE rolname='CatalogManager'") == '0':
            manager_password = (root/'secrets/CatalogManager').read_text()
            assert len(manager_password) == 64 and all(c in '0123456789abcdef' for c in manager_password)
            sql('postgres', 'CREATE ROLE "CatalogManager" LOGIN NOSUPERUSER CREATEROLE NOCREATEDB NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD '+"'"+manager_password+"'")
            del manager_password
        assert sql('postgres', "SELECT rolcanlogin AND rolcreaterole AND NOT(rolsuper OR rolcreatedb OR rolinherit OR rolreplication OR rolbypassrls) FROM pg_roles WHERE rolname='CatalogManager'") == 't'
        db, home, path = prepare('installed', native=True, state=False)
        # Missing local custody refuses before reading either backend credential.
        (home/'CatalogManager').rename(home/'CatalogManager.unavailable')
        cli('archive', 'coordinator-initialize', '--file', path, code=4, contains='Incomplete')
        assert sql(db, 'SELECT count(*) FROM control.owner_epochs') == '0'
        assert rows(db) == dict(bindings=[], attempts=[], persistence=[])
        passed('missing-local-state-refuses-before-control-claim')
        cli('archive', 'state-initialize', '--file', path)
        local = (home/'state/coordinator.json').read_bytes()
        cli('serve', '--roles', 'control', '--file', path, code=3, contains='coordinator protocol refused: Prerequisite')
        no_clients(db)
        assert rows(db) == dict(bindings=[], attempts=[], persistence=[])
        assert sql(db, 'SELECT count(*) FROM control.owner_epochs') == '1'
        passed('ordinary-missing-sql-binding-preserves-empty-history')
        (home/'CatalogManager.unavailable').rename(home/'CatalogManager')
        for created in [True, False]:
            value = cli('archive', 'coordinator-initialize', '--file', path)
            assert value == dict(format='bigbrotr-coordinator-initialization/1', state='bound', created=created,
                                 dataset_id='01'*16, catalog_id='5a'*16, binding_sha256=hashlib.sha256(local).hexdigest(),
                                 owner_release='committed', catalog_inspection='verified', persistence_protocol=1, persistence_created=created,
                                 publication_authority='not-granted', ready=False)
            actual = rows(db)
            if created:
                original = actual
            assert actual == original
            assert actual['bindings'][0]['issuer_epoch'] == 2
        passed('explicit-binding-and-identical-retry', rows=original)
        assert len(original['persistence']) == 1
        persistence = original['persistence'][0]
        assert persistence['catalog_id'] == '\\x'+'5a'*16
        assert persistence['coordinator_binding_sha256'] == '\\x'+hashlib.sha256(local).hexdigest()
        assert persistence['protocol_version'] == persistence['baseline_snapshot'] == 1
        assert persistence['protection_class'] == 'local-synced'
        assert persistence['issuer_epoch'] == original['bindings'][0]['issuer_epoch']

        orphan_db, orphan_home, orphan_path = prepare('orphan', native=True)
        orphan = orphan_home/'data/unregistered.parquet'
        orphan.write_bytes(b'independent unregistered scientific file')
        value = cli('archive', 'coordinator-initialize', '--file', orphan_path)
        assert value['created'] is True and value['persistence_created'] is True
        assert orphan.read_bytes() == b'independent unregistered scientific file'
        assert sql(orphan_db+'_catalog', 'SELECT count(*) FROM public.ducklake_data_file') == '0'
        retained_orphan = rows(orphan_db)
        value = cli('archive', 'coordinator-initialize', '--file', orphan_path)
        assert value['created'] is False and value['persistence_created'] is False
        assert rows(orphan_db) == retained_orphan
        assert orphan.read_bytes() == b'independent unregistered scientific file'
        passed('unregistered-orphan-preserved-outside-membership', sha256=sha(orphan))

        for phase in ['before_verify', 'during_commit']:
            fault_db, _, fault_path = prepare('loss_'+phase, native=True)
            timing = 'BEFORE' if phase == 'before_verify' else 'AFTER'
            constraint = '' if phase == 'before_verify' else 'CONSTRAINT '
            deferred = '' if phase == 'before_verify' else 'DEFERRABLE INITIALLY DEFERRED '
            sql(fault_db, "CREATE FUNCTION control.test_persistence_barrier() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(902315); RETURN NEW; END $$; "
                +f"CREATE {constraint}TRIGGER test_persistence_barrier {timing} INSERT ON control.archive_persistence {deferred}FOR EACH ROW EXECUTE FUNCTION control.test_persistence_barrier();")
            blocker = subprocess.Popen(['docker','exec','-i',pg,'psql','-XqAt','-v','ON_ERROR_STOP=1','-U','postgres','-d',fault_db],
                                       stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            try:
                blocker.stdin.write("BEGIN; SELECT pg_advisory_xact_lock(902315); SELECT pg_backend_pid();\n")
                blocker.stdin.flush()
                assert select.select([blocker.stdout],[],[],5)[0]
                assert blocker.stdout.readline().strip() == ''
                holder = int(blocker.stdout.readline())
                item = start(fault_path, 'archive', 'coordinator-initialize')
                until = time.monotonic()+5
                while True:
                    blocked = json.loads(sql(fault_db, f"SELECT coalesce(json_agg(json_build_object('pid',pid,'backend_start',backend_start,'query',query,'blockers',pg_blocking_pids(pid))),'[]') FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control' AND wait_event_type='Lock' AND wait_event='advisory' AND {holder}=ANY(pg_blocking_pids(pid));"))
                    if blocked: break
                    assert time.monotonic() < until
                    time.sleep(.01)
                assert len(blocked) == 1
                assert blocked[0]['query'] == 'COMMIT' if phase == 'during_commit' else blocked[0]['query'].startswith('INSERT INTO control.archive_persistence')
                managers = json.loads(sql(fault_db+'_catalog', "SELECT coalesce(json_agg(json_build_object('pid',pid,'backend_start',backend_start,'state',state)),'[]') FROM pg_stat_activity WHERE datname=current_database() AND usename='CatalogManager';"))
                assert len(managers) == 1 and managers[0]['state'] == 'idle in transaction'
                manager = managers[0]
                assert sql(fault_db+'_catalog', f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid={manager['pid']} AND backend_start='{manager['backend_start']}'::timestamptz AND datname=current_database() AND usename='CatalogManager';") == 't'
            finally:
                blocker.stdin.write('ROLLBACK;\n')
                blocker.stdin.close()
                assert blocker.wait(timeout=5) == 0
            failure = terminal(item, 4)
            assert not failure['stdout']
            assert ('barrier termination unresolved' if phase == 'during_commit' else 'Unavailable') in failure['stderr']
            no_clients(fault_db)
            assert sql(fault_db+'_catalog', "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='CatalogManager'") == '0'
            before_retry = rows(fault_db)
            assert len(before_retry['bindings']) == 1 and before_retry['attempts'] == []
            assert len(before_retry['persistence']) == int(phase == 'during_commit')
            sql(fault_db, 'DROP TRIGGER test_persistence_barrier ON control.archive_persistence; DROP FUNCTION control.test_persistence_barrier()')
            retry = cli('archive', 'coordinator-initialize', '--file', fault_path)
            assert retry['created'] is False and retry['persistence_created'] is (phase == 'before_verify')
            after_retry = rows(fault_db)
            assert after_retry['bindings'] == before_retry['bindings'] and after_retry['attempts'] == []
            assert len(after_retry['persistence']) == 1
            if phase == 'during_commit': assert after_retry == before_retry
            for operation in ['UPDATE control.archive_persistence SET protocol_version=1',
                              'DELETE FROM control.archive_persistence', 'TRUNCATE control.archive_persistence CASCADE']:
                denied = subprocess.run(['docker','exec','-i',pg,'psql','-XqAt','-v','ON_ERROR_STOP=1','-U','postgres','-d',fault_db],
                                        input=operation, text=True, capture_output=True, timeout=5)
                report.setdefault('mutation_refusals', []).append(dict(phase=phase, operation=operation, exit=denied.returncode, stdout=denied.stdout, stderr=denied.stderr))
                save()
                assert denied.returncode != 0 and 'archive persistence participation is immutable' in denied.stderr
            assert rows(fault_db) == after_retry
            passed('manager-loss-'+phase+'-and-identical-retry', blocker=holder, blocked=blocked,
                   manager=manager, before_retry=before_retry, after_retry=after_retry)

        for name in ['start', 'restart']:
            item = start(path, 'serve', '--roles', 'control')
            value = idle(item)
            cli('archive', 'coordinator-initialize', '--file', path, code=4, contains='Occupied')
            signal(item)
            result = terminal(item, 0)
            assert not result['stderr'] and [json.loads(x)['state'] for x in result['stdout'].splitlines()] == ['idle', 'stopped']
            no_clients(db)
            assert rows(db) == original and (home/'state/coordinator.json').read_bytes() == local
            passed(name+'-requires-binding-and-preserves-history', owner_epoch=value['owner_epoch'])
        previous_epoch = int(sql(db, "SELECT max(epoch) FROM control.owner_epochs WHERE role='control'"))
        observation = {}
        execute(['docker', 'exec', runner, 'python3', '-B', '/source/experiments/control-runtime/blocked_output.py',
                 '--binary', binary, '--config', path, '--output', '/out/'+directory_name+'/full-output.json',
                 '--stream', 'stdout', '--command', 'coordinator-initialize'], 15, observation)
        assert observation['exit'] == 0 and not observation['stderr']
        full = json.loads(observation['stdout'])
        assert full['status'] == 'TERMINAL' and full['exit'] == 1 and full['elapsed_seconds'] < 8
        assert full['blocking'] and full['filled_bytes'] > 0 and full['unchanged'] and full['eof_after_child_exit']
        assert (directory/'full-output.other-output').read_bytes() == b''
        no_clients(db)
        assert rows(db) == original
        released = json.loads(sql(db, "SELECT row_to_json(t) FROM (SELECT epoch,state,released_at_us FROM control.owner_epochs WHERE role='control' ORDER BY epoch DESC LIMIT 1)t"))
        assert released['epoch'] == previous_epoch+1 and released['state'] == 'released' and released['released_at_us'] is not None
        passed('full-stdout-exits-with-committed-identical-binding', observation=full, released_owner=released)

        _, alternate_home, alternate_path = prepare('alternate')
        alternate_config = alternate_home/'instance.toml'
        alternate_config.write_text(change(alternate_config.read_text(), 'postgres.control', 'database', db))
        alternate_bytes = (alternate_home/'state/coordinator.json').read_bytes()
        assert alternate_bytes != local
        cli('archive', 'coordinator-initialize', '--file', alternate_path, code=3, contains='Conflict')
        cli('serve', '--roles', 'control', '--file', alternate_path, code=3, contains='Conflict')
        no_clients(db)
        assert rows(db) == original and (home/'state/coordinator.json').read_bytes() == local
        assert (alternate_home/'state/coordinator.json').read_bytes() == alternate_bytes
        passed('different-valid-local-binding-refused-without-repair')

        old_db, old_home, old_path = prepare('old', target=9)
        cli('archive', 'coordinator-initialize', '--file', old_path, code=3, contains='Prerequisite')
        cli('serve', '--roles', 'control', '--file', old_path, code=3, contains='Prerequisite')
        assert sql(old_db, 'SELECT count(*) FROM control.schema_versions') == '8'
        assert sql(old_db, 'SELECT count(*) FROM control.archive_attempts') == '0'
        passed('missing-migration-refused-without-runtime-ddl')
        seeded = json.loads(command('docker', 'exec', runner, '/out/native-tests/archive-attempt-probe', '--seed-runtime', old_db))
        assert seeded['rows'] == 130
        old_rows = json.loads(sql(old_db, 'SELECT json_agg(t ORDER BY ordinal) FROM control.archive_attempts t'))
        cli('migrate', 'apply', '--file', old_path, '--target', '10', '--user', 'postgres',
            '--password-ref', 'file:/out/'+directory_name+'/old/postgres')
        sql(old_db, 'GRANT SELECT,INSERT ON control.archive_coordinators TO bigbrotr_control; GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA control TO bigbrotr_control')
        retained = rows(old_db)
        assert not retained['bindings'] and len(retained['attempts']) == 130
        for before, after in zip(old_rows, retained['attempts']):
            assert {k:v for k,v in after.items() if k not in ['xmin','coordinator_binding_sha256']} == before
            assert after['coordinator_binding_sha256'] is None
        cli('archive', 'coordinator-initialize', '--file', old_path, code=3, contains='Conflict')
        cli('serve', '--roles', 'control', '--file', old_path, code=3, contains='Prerequisite')
        no_clients(old_db)
        assert rows(old_db) == retained
        passed('old130-attempt-history-preserved-and-not-adopted', rows=130, snapshot_sha256=hashlib.sha256(json.dumps(retained,sort_keys=True).encode()).hexdigest())
        cli('migrate', 'apply', '--file', old_path, '--target', '15', '--user', 'postgres',
            '--password-ref', 'file:/out/'+directory_name+'/old/postgres')
        sql(old_db, 'GRANT SELECT,INSERT ON control.archive_persistence TO bigbrotr_control; GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA control TO bigbrotr_control')
        after_persistence_upgrade = rows(old_db)
        assert after_persistence_upgrade['bindings'] == after_persistence_upgrade['persistence'] == []
        assert len(after_persistence_upgrade['attempts']) == len(retained['attempts']) == 130
        for before, after in zip(retained['attempts'], after_persistence_upgrade['attempts']):
            assert after['persistence_protocol'] is None
            assert {k:v for k,v in after.items() if k != 'persistence_protocol'} == before
        cli('archive', 'coordinator-initialize', '--file', old_path, code=3, contains='Conflict')
        assert rows(old_db) == after_persistence_upgrade
        passed('persistence-upgrade-preserves-old130-null-history-without-adoption')

        for name in ['INT', 'TERM']:
            cancelled_db, _, cancelled_path = prepare('cancel_'+name.lower(), native=True)
            blocker = subprocess.Popen(['docker', 'exec', '-i', pg, 'psql', '-XqAt', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', cancelled_db], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            try:
                blocker.stdin.write("BEGIN; LOCK TABLE control.archive_coordinators IN ACCESS EXCLUSIVE MODE; SELECT pg_backend_pid();\n")
                blocker.stdin.flush()
                assert select.select([blocker.stdout],[],[],5)[0]
                holder = int(blocker.stdout.readline())
                item = start(cancelled_path, 'archive', 'coordinator-initialize')
                deadline = time.monotonic()+2
                while True:
                    observed = json.loads(sql(cancelled_db, "SELECT coalesce(json_agg(json_build_object('pid',pid,'backend_start',backend_start,'blockers',pg_blocking_pids(pid))),'[]') FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control' AND wait_event_type='Lock' AND wait_event='relation'"))
                    if observed:
                        break
                    assert time.monotonic() < deadline
                    time.sleep(.01)
                assert len(observed) == 1 and holder in observed[0]['blockers']
                began = time.monotonic()
                signal(item, name)
                result = terminal(item, 4)
                elapsed = time.monotonic()-began
                assert elapsed < 5 and not result['stdout'] and 'Unavailable' in result['stderr']
                residual = json.loads(sql(cancelled_db, "SELECT json_build_object('pid',a.pid,'backend_start',a.backend_start,'fence',EXISTS(SELECT 1 FROM pg_locks l WHERE l.pid=a.pid AND l.locktype='advisory' AND l.classid=1111651442 AND l.objid=1129599564 AND l.granted)) FROM pg_stat_activity a WHERE a.pid="+str(observed[0]['pid'])))
                assert residual['backend_start'] == observed[0]['backend_start'] and residual['fence']
                epochs = sql(cancelled_db, 'SELECT json_agg(t ORDER BY epoch) FROM control.owner_epochs t')
                cli('archive', 'coordinator-initialize', '--file', cancelled_path, code=3, contains='Busy')
                assert sql(cancelled_db, 'SELECT json_agg(t ORDER BY epoch) FROM control.owner_epochs t') == epochs
            finally:
                blocker.stdin.write('ROLLBACK;\n')
                blocker.stdin.close()
                assert blocker.wait(timeout=5) == 0
            no_clients(cancelled_db)
            assert rows(cancelled_db) == dict(bindings=[], attempts=[], persistence=[])
            value = cli('archive', 'coordinator-initialize', '--file', cancelled_path)
            assert value['created'] is True
            passed('blocked-binding-'+name.lower()+'-preserves-fence-then-retries', elapsed_seconds=elapsed, blocker=holder, residual=residual)
        report['status'] = 'PASS'
    except BaseException as error:
        report.update(status='FAIL' if isinstance(error, AssertionError) else 'INCOMPLETE', failure=type(error).__name__)
        raise
    finally:
        for item in processes:
            p, record = item
            try:
                if p.process.poll() is None:
                    signal(item)
            except Exception as error:
                record['cleanup_signal_error'] = type(error).__name__
                report['status'] = 'INCOMPLETE'
            finally:
                try:
                    p.stop()
                except Exception as error:
                    record['cleanup_join_error'] = type(error).__name__
                    report['status'] = 'INCOMPLETE'
                record.update(exit=p.process.poll(), stdout=p.stdout.decode(errors='replace'), stderr=p.stderr.decode(errors='replace'))
                save()
        save()
    assert report['status'] == 'PASS'
    print(json.dumps(dict(status=report['status'], cases=len(report['cases']), source=report['source'])), flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    parser.add_argument('--namespace', default='')
    args = parser.parse_args()
    run(args.root.resolve(), args.namespace)
