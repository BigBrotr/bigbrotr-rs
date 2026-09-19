#!/usr/bin/env python3
"""Actual old/new clients and migration serialization in an owned disposable lab."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time

sys.dont_write_bytecode = True
SOURCE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SOURCE/'experiments/control-migrations'))
sys.path.insert(0, str(SOURCE/'experiments/ingest-bootstrap'))
from fault_cases import Process
from lab import change
from oracle import expected


def command(*args, input=None):
    p = subprocess.run(args, input=input, text=True, capture_output=True, timeout=40)
    assert p.returncode == 0, p.stderr
    return p.stdout.strip()


def send(p, text):
    p.process.stdin.write(text.encode()); p.process.stdin.flush()


def event(p, name, timeout=12):
    deadline = time.monotonic()+timeout
    with p.condition:
        while True:
            for line in bytes(p.stdout).split(b'\n')[:-1]:
                if line.startswith(b'{'):
                    value = json.loads(line)
                    if value.get('event') == name: return value
            assert p.process.poll() is None and not p.overflow, 'process ended before event '+name
            assert time.monotonic() < deadline, 'event deadline '+name
            p.condition.wait(.03)


def main(root):
    state = json.loads((root/'resources.json').read_text())
    assert state['root'] == str(root)
    harness = command('git', '-C', str(SOURCE), 'rev-parse', 'HEAD')
    assert not command('git', '-C', str(SOURCE), 'status', '--porcelain')
    assert command('git', '-C', str(root/'source'), 'rev-parse', 'HEAD') == state['source']
    assert not command('git', '-C', str(root/'source'), 'status', '--porcelain')
    assert (SOURCE/'migrations/postgres/0011_producer_requests.sql').read_bytes() == (root/'source/migrations/postgres/0011_producer_requests.sql').read_bytes()
    runner, pg = state['container_id'], state['postgres_id']
    for identity in [runner, pg]:
        c = json.loads(command('docker', 'inspect', identity))[0]
        assert c['Config']['Labels']['org.bigbrotr.publication'] == state['owner'] and c['State']['Running']
    concurrency = json.loads((root/'producer-concurrency.json').read_text())
    assert concurrency['status'] == 'PASS' and concurrency['source'] == state['source']
    ready = json.loads((root/'out/native-tests/ready.json').read_text())
    assert ready['source'] == state['source'] and ready['status'] == 'READY'
    old = json.loads((root/'out/compatibility-build/ready.json').read_text())
    assert old['status'] == 'READY' and old['source'] == 'a1da96a09ee73722d26003dedf793317b7bdf7e1'
    builder = json.loads((root/'compatibility-builder.json').read_text())
    c = json.loads(command('docker','inspect',builder['id']))[0]
    assert c['Config']['Labels']['org.bigbrotr.producer-compatibility'] == state['owner']
    assert not c['State']['Running'] and c['State']['ExitCode'] == 0 and not c['State']['OOMKilled']
    old_path = root/'out/compatibility-build/old-ingest-probe'
    assert hashlib.sha256(old_path.read_bytes()).hexdigest() == old['sha256']
    for name, key in [('bigbrotr','control'), ('ingest-bootstrap-probe','ingest_probe')]:
        assert hashlib.sha256((root/'out/native-tests'/name).read_bytes()).hexdigest() == ready[key]
    work = root/'out/compatibility'; work.mkdir(mode=0o700)
    credential = work/'migrator-secret'
    credential.write_bytes((root/'secrets/postgres').read_bytes()); credential.chmod(0o400)
    # The source of this finite supervisor is hash-bound to the clean harness.
    wrapper = SOURCE/'experiments/producer-requests/held_old_owner.py'
    (work/'held_old_owner.py').write_bytes(wrapper.read_bytes())
    report_path = root/'producer-compatibility.json'; assert not report_path.exists()
    report = dict(status='INCOMPLETE', source=state['source'], harness_revision=harness,
                  harness_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                  supervisor_sha256=hashlib.sha256(wrapper.read_bytes()).hexdigest(),
                  artifacts=ready, old_build=old, auxiliary=[builder], cases=[])
    def save(): report_path.write_text(json.dumps(report, indent=2)+'\n')
    def sql(db, text):
        return command('docker','exec','-i',pg,'psql','-XqAt','-v','ON_ERROR_STOP=1','-U','postgres','-d',db,input=text)
    new = ['docker','exec',runner,'/out/native-tests/ingest-bootstrap-probe']
    old_command = ['docker','exec',runner,'/out/compatibility-build/old-ingest-probe']
    main_command = ['docker','exec',runner,'/out/native-tests/bigbrotr']
    command(*main_command,'config','init','--recipe','archive-only','--output','/out/compatibility/template.toml')
    def config(db):
        text = change((work/'template.toml').read_text(),'postgres.control','database',db)
        text = change(text,'postgres.control','endpoint','127.0.0.1:5432')
        text = change(text,'postgres.control.tls','local_boundary','loopback')
        (work/(db+'.toml')).write_text(text)
        return '/out/compatibility/'+db+'.toml'
    def snapshot(db):
        names = json.loads(sql(db,"SELECT jsonb_agg(tablename ORDER BY tablename) FROM pg_tables WHERE schemaname='control'"))
        assert len(names) in (32,33)
        rows = {name:json.loads(sql(db,'SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),\'[]\'::jsonb) FROM control.'+name+' t')) for name in names}
        dump = command('docker','exec',pg,'pg_dump','-U','postgres','-d',db,'--schema=control','--no-owner','--no-privileges')
        dump = '\n'.join(line for line in dump.splitlines() if not line.startswith(('\\restrict ','\\unrestrict ')))
        return dict(rows=rows, schema_sha256=hashlib.sha256(dump.encode()).hexdigest())
    def await_rows(db, query):
        deadline = time.monotonic()+3
        while time.monotonic() < deadline:
            rows = json.loads(sql(db,"SELECT coalesce(jsonb_agg(to_jsonb(t)),'[]'::jsonb) FROM ("+query+") t"))
            if rows: return rows
            time.sleep(.02)
        raise AssertionError('actual SQL blocking/lock state not observed')
    def migration_lock(db, mode, granted):
        return await_rows(db,"SELECT a.pid,a.backend_start,l.mode,l.granted,pg_blocking_pids(a.pid) AS blockers FROM pg_locks l JOIN pg_stat_activity a USING(pid) WHERE a.datname=current_database() AND l.locktype='advisory' AND l.classid=1111651442 AND l.objid="+str(0x4d494752)+" AND l.mode='"+mode+"' AND l.granted="+str(granted).lower())
    def native(db, present):
        dumped = json.loads(command(*new,'dump','/out/compatibility/'+db+'/db'))
        wanted,_,_ = expected(bytes([1])*16,present)
        assert [(r['cf'],bytes.fromhex(r['key']),bytes.fromhex(r['value'])) for r in dumped['rows']] == wanted
        return dumped
    save()
    for mode in ['registration_first','migration_first','backend_loss','closed_history','new_on_old']:
        case = dict(name=mode,status='INCOMPLETE',processes={}); report['cases'].append(case); save()
        db = 'bbproducer_compat_'+mode
        sql('postgres','CREATE DATABASE '+db)
        command(*new,'setup-10',db,'/secrets/postgres','unused')
        migration = [*main_command,'migrate','apply','--file',config(db),'--user','postgres','--password-ref','file:/out/compatibility/migrator-secret','--target','11']
        home = '/out/compatibility/'+db+'/db'; (work/db).mkdir()
        clients = []
        def start(label,args,pipe=False):
            p = Process(args,input_pipe=pipe,limit=256*1024); clients.append((label,p)); return p
        def hold(table):
            p = start('holder',['docker','exec','-i',pg,'psql','-XqAt','-v','ON_ERROR_STOP=1','-U','postgres','-d',db],True)
            send(p,"SET application_name='compat_holder'; SET idle_in_transaction_session_timeout='10s'; BEGIN; LOCK TABLE control."+table+" IN SHARE MODE; SELECT json_build_object('event','held','pid',pg_backend_pid());\n")
            return p,event(p,'held')['pid']
        def success(p): assert p.wait(15) == 0 and not p.stderr
        def refused(p):
            assert p.wait(15) == 1
            assert not p.stdout and b'migration 11 refuses pre-ledger ingest history' in p.stderr
        try:
            if mode == 'registration_first':
                holder,pid = hold('producer_slots')
                legacy = start('old',old_command+['write',db,'/secrets/bigbrotr_control',home])
                case['registration_wait'] = await_rows(db,"SELECT pid,backend_start,pg_blocking_pids(pid) AS blockers FROM pg_stat_activity WHERE datname=current_database() AND application_name='bigbrotr-ingest-bootstrap-probe' AND "+str(pid)+"=ANY(pg_blocking_pids(pid))")
                case['old_shared'] = migration_lock(db,'ShareLock',True)
                migrator = start('migrate',migration)
                case['migration_wait'] = migration_lock(db,'ExclusiveLock',False)
                send(holder,'ROLLBACK;\n\\q\n'); success(holder)
                refused(migrator); success(legacy)
                case['native'] = native(db,True)
                before = snapshot(db)
                repeat = start('repeat',migration); refused(repeat)
                assert snapshot(db) == before
                case['unchanged_retry'] = before
            elif mode == 'migration_first':
                holder,pid = hold('schema_versions')
                migrator = start('migrate',migration)
                case['exclusive'] = migration_lock(db,'ExclusiveLock',True)
                case['ledger_wait'] = await_rows(db,"SELECT pid,backend_start,pg_blocking_pids(pid) AS blockers FROM pg_stat_activity WHERE datname=current_database() AND "+str(pid)+"=ANY(pg_blocking_pids(pid))")
                legacy = start('old',old_command+['write',db,'/secrets/bigbrotr_control',home])
                case['old_wait'] = migration_lock(db,'ShareLock',False)
                send(holder,'ROLLBACK;\n\\q\n'); success(holder); success(migrator)
                assert json.loads(migrator.stdout)['status'] == 'applied'
                assert legacy.wait(15) == 101 and b'IncompatibleDeployment' in legacy.stderr
                after = snapshot(db); case['after'] = after
                assert len(after['rows']['schema_versions']) == 10 and not after['rows']['owner_epochs']
                assert not (work/db/'db').exists() and not after['rows']['producer_requests']
            elif mode == 'backend_loss':
                legacy = start('supervisor',['docker','exec','-i',runner,'python3','-B','/out/compatibility/held_old_owner.py',old_command[-1],db,'/secrets/bigbrotr_control',home],True)
                case['old_held'] = event(legacy,'old-held')
                assert case['old_held']['executable_sha256'] == old['sha256']
                before = snapshot(db)
                holders = migration_lock(db,'ShareLock',True); assert len(holders) == 1
                migrator = start('migrate',migration)
                case['migration_wait'] = migration_lock(db,'ExclusiveLock',False)
                assert holders[0]['pid'] in case['migration_wait'][0]['blockers']
                holder = holders[0]; stamp = holder['backend_start'].replace("'","''")
                assert sql(db,"SELECT pg_terminate_backend(pid,1000) FROM pg_stat_activity WHERE datname=current_database() AND pid="+str(holder['pid'])+" AND backend_start='"+stamp+"'::timestamptz") == 't'
                case['terminated_backend'] = holder
                refused(migrator)
                send(legacy,'observe\n'); case['old_still_held'] = event(legacy,'old-still-held')
                assert {k:v for k,v in case['old_held'].items() if k!='event'} == {k:v for k,v in case['old_still_held'].items() if k!='event'}
                assert snapshot(db) == before
                case['unchanged_after_refusal'] = before
                send(legacy,'kill\n'); case['joined'] = event(legacy,'old-joined'); success(legacy)
                assert case['joined']['joined'] and case['joined']['exit'] == -9
                case['native'] = native(db,False)
            elif mode == 'closed_history':
                legacy = start('old',old_command+['write',db,'/secrets/bigbrotr_control',home]); success(legacy)
                case['native'] = native(db,True)
                # Synthetic SQL history only, after the real old process joined.
                # This fixture does not implement/qualify native closure or handoff.
                sql(db,"""BEGIN;
UPDATE control.producer_epochs SET state='closing';
UPDATE control.producer_epochs SET state='retired',retired_at_us=opened_at_us,closure_digest=decode(repeat('09',32),'hex');
UPDATE control.allocation_domains SET state='closed',closed_at_us=created_at_us,closure_digest=decode(repeat('09',32),'hex');
UPDATE control.source_streams SET state='closing';
UPDATE control.source_streams SET state='closed',closed_at_us=opened_at_us,last_complete_sequence=2,closure_digest=decode(repeat('09',32),'hex');
UPDATE control.owner_epochs SET state='draining' WHERE role='ingest';
UPDATE control.owner_epochs SET state='released',released_at_us=acquired_at_us,exclusion_method='joined',exclusion_digest=decode(repeat('09',32),'hex') WHERE role='ingest';
COMMIT;""")
                before = snapshot(db)
                assert {r['state'] for r in before['rows']['owner_epochs']} == {'released'}
                assert {r['state'] for r in before['rows']['producer_epochs']} == {'retired'}
                assert {r['state'] for r in before['rows']['source_streams']} == {'closed'}
                assert {r['state'] for r in before['rows']['allocation_domains']} == {'closed'}
                migrator = start('migrate',migration); refused(migrator)
                assert snapshot(db) == before and native(db,True) == case['native']
                case['unchanged_closed_history'] = before
            else:
                current = start('new',new+['write',db,'/secrets/bigbrotr_control',home])
                assert current.wait(15) == 101 and b'IncompatibleSchema' in current.stderr
                after = snapshot(db); case['after'] = after
                assert len(after['rows']['schema_versions']) == 9 and not (work/db/'db').exists()
                assert len(after['rows']['owner_epochs']) == 1 and after['rows']['owner_epochs'][0]['role'] == 'control'
                assert all(not after['rows'][t] for t in ['producer_slots','producer_epochs','source_streams','allocation_domains'])
        except BaseException as error:
            case.update(status='FAIL',failure=type(error).__name__+': '+str(error)); save(); raise
        finally:
            cleanup_errors = []
            for label,p in clients:
                try:
                    if p.process.poll() is None and label == 'holder': send(p,'ROLLBACK;\n\\q\n')
                    if p.process.poll() is None and label == 'supervisor': send(p,'kill\n')
                except (BrokenPipeError,OSError): pass
            for label,p in clients:
                try: p.wait(40)
                except Exception as error: cleanup_errors.append(type(error).__name__+': '+str(error))
                finally:
                    try: p.stop()
                    except Exception as error: cleanup_errors.append(type(error).__name__+': '+str(error))
                    case['processes'][label] = dict(exit=p.process.returncode,stdout=p.stdout.decode(),stderr=p.stderr.decode(),overflow=p.overflow)
            case['remaining_backends'] = int(sql(db,'SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()'))
            case['cleanup_errors'] = cleanup_errors; save()
            assert not cleanup_errors and case['remaining_backends'] == 0
        case['status'] = 'PASS'; save()
    report['status'] = 'PASS'; save()
    print(json.dumps(dict(status='PASS',cases=len(report['cases']))))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,required=True)
    main(parser.parse_args().root.resolve(strict=True))
