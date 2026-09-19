#!/usr/bin/env python3
"""Installed receipt consumer after independently checked signed publication.

Trusted isolated lab only. No production databases or unattended role mutations.
All negative controls preserve original input/catalog bytes and retained issuance.
"""
import argparse
import hashlib
import json
from pathlib import Path
import select
import subprocess
import time

from signed_publication_cases import Process, command, execute, sha, event, send
from signed_publication_oracle import MODES, restart_expectations, input_files
from coordinator_cli_cases import SIGNAL, change


WATCH = r"""import json,os,select,sys
from pathlib import Path
expected=sys.argv[1:]
def record(p):
 s=(p/'stat').read_text().rsplit(')',1)[1].split()
 return dict(pid=int(p.name),start=s[19],ppid=int(s[1]),exe=os.readlink(p/'exe'),argv=[v.decode() for v in (p/'cmdline').read_bytes().rstrip(b'\0').split(b'\0')])
peers=[]
for p in Path('/proc').iterdir():
 if not p.name.isdecimal():continue
 try:peers.append(record(p))
 except (FileNotFoundError,PermissionError,ProcessLookupError):pass
parents=[r for r in peers if r['argv']==expected and r['exe']==expected[0]]
assert len(parents)==1
parent=parents[0]
children=[r for r in peers if r['ppid']==parent['pid'] and r['exe']=='/out/archive-worker/bigbrotr-archive-worker' and 'inspect-receipt' in r['argv']]
assert len(children)==1
child=children[0];fds=[]
try:
 for r in [parent,child]:
  fd=os.pidfd_open(r['pid']);fds.append(fd)
  assert record(Path('/proc')/str(r['pid']))==r
 poll=select.poll();poll.register(fds[1],select.POLLIN)
 assert not poll.poll(0)
 print(json.dumps(dict(event='watch-ready',parent=parent,child=child)),flush=True)
 assert sys.stdin.readline()=='finish\n'
 assert poll.poll(0), 'native still live after CLI termination'
 try:current=record(Path('/proc')/str(child['pid']))
 except (FileNotFoundError,ProcessLookupError):current=None
 assert current is None or current['start']!=child['start'], 'native not reaped'
 print(json.dumps(dict(event='watch-terminal',native_exited=True,reaped=True)),flush=True)
finally:
 for fd in fds:os.close(fd)
"""


def run(root):
    source = Path(__file__).resolve().parents[2]
    resources = json.loads((root/'resources.json').read_text())
    assert source == root/'source'
    assert command('git', '-C', str(source), 'rev-parse', 'HEAD') == resources['source']
    assert not command('git', '-C', str(source), 'status', '--porcelain', '--untracked-files=all')
    runner, pg = resources['container_id'], resources['postgres_id']
    for identity in [runner, pg]:
        c = json.loads(command('docker', 'inspect', identity))[0]
        assert c['Config']['Labels']['org.bigbrotr.publication'] == resources['owner']
        assert c['State']['Running'] and not c['State']['OOMKilled']
    ready = json.loads((root/'out/native-tests/ready.json').read_text())
    assert ready['source'] == resources['source']
    assert sha(root/'out/native-tests/bigbrotr') == ready['control']
    signed = json.loads((root/'installed-signed-cases.json').read_text())
    assert signed['status'] == 'PASS' and signed['source'] == resources['source']
    report = dict(status='INCOMPLETE', source=resources['source'], tree=resources['tree'],
                  artifacts=ready, harness_sha256=sha(Path(__file__)), cases=[], commands=[], processes=[])
    path = root/'receipt-cli-cases.json'
    assert not path.exists()
    binary = '/out/native-tests/bigbrotr'
    processes, blockers, watchers = [], [], []

    def save():
        path.write_text(json.dumps(report, indent=2)+'\n')

    def passed(name, **details):
        report['cases'].append(dict(name=name, status='PASS', **details))
        save()

    def sql(db, text):
        return command('docker', 'exec', '-i', pg, 'psql', '-XqAt', '-v', 'ON_ERROR_STOP=1',
                       '-U', 'postgres', '-d', db, input=text)

    def rows(db):
        return json.loads(sql(db, "SELECT json_build_object('bindings',(SELECT json_agg(t) FROM (SELECT c.*,xmin::text AS xmin FROM control.archive_coordinators c)t),'attempts',(SELECT json_agg(t ORDER BY ordinal) FROM (SELECT a.*,xmin::text AS xmin FROM control.archive_attempts a)t))"))

    def cli(mode, attempt, code=0, contains=None, cap=128):
        args = [binary, 'archive', 'receipt-inspect', '--file', f'/out/signed_{mode}/receipt.toml',
                '--attempt-id', attempt, '--max-receipts', str(cap), '--worker-root', '/out/archive-worker']
        record = dict(args=args)
        report['commands'].append(record)
        try:
            execute(['docker', 'exec', runner, *args], 135, record)
        finally:
            save()
        assert record['exit'] == code, record
        if contains:
            assert contains in record['stderr'] and not record['stdout'], record
        else:
            assert not record['stderr'], record
        return json.loads(record['stdout']) if record['stdout'] else None

    def no_clients():
        deadline = time.monotonic()+8
        while sql('postgres', "SELECT count(*) FROM pg_stat_activity WHERE usename IN ('bigbrotr_control','CatalogManager','bigbrotr_receipt_reader')") != '0':
            assert time.monotonic() < deadline, 'actual application backend drain incomplete'
            time.sleep(.02)

    def lock(db, statement):
        p = subprocess.Popen(['docker','exec','-i',pg,'psql','-XqAt','-v','ON_ERROR_STOP=1','-U','postgres','-d',db],
                             stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        blockers.append(p)
        p.stdin.write('BEGIN; '+statement+'; SELECT pg_backend_pid();\n'); p.stdin.flush()
        assert select.select([p.stdout], [], [], 5)[0]
        pid = int(p.stdout.readline())
        return p, pid

    def unlock(p):
        p.stdin.write('ROLLBACK;\n'); p.stdin.close()
        assert p.wait(timeout=5) == 0 and p.stderr.read() == ''
        blockers.remove(p)

    def start():
        args = [binary,'archive','receipt-inspect','--file','/out/signed_normal/receipt.toml',
                '--attempt-id','b6'*16,'--worker-root','/out/archive-worker']
        p = Process(['docker','exec',runner,*args])
        record = dict(args=args, exit=None)
        report['processes'].append(record); processes.append((p,record)); save()
        return p,record

    def wait_lock(user, db, holder):
        deadline = time.monotonic()+10
        while True:
            value = json.loads(sql(db, "SELECT coalesce(json_agg(json_build_object('pid',pid,'backend_start',backend_start,'application_name',application_name,'blockers',pg_blocking_pids(pid))),'[]') FROM pg_stat_activity WHERE datname=current_database() AND usename='"+user+"' AND wait_event_type='Lock'"))
            if value:
                assert len(value) == 1 and holder in value[0]['blockers']
                return value[0]
            assert time.monotonic() < deadline, 'required actual lock wait absent'
            time.sleep(.02)

    def terminal(item, code, contains=None):
        p, record = item
        try:
            record['exit'] = p.wait(12)
        finally:
            record.update(stdout=p.stdout.decode(), stderr=p.stderr.decode()); save()
        assert record['exit'] == code, record
        if contains:
            assert contains in record['stderr'] and not record['stdout'], record
        else:
            assert not record['stderr'], record
        return record

    save()
    try:
        originals, catalog_dumps, physical = {}, {}, {}
        def dump(mode):
            text = command('docker','exec',pg,'pg_dump','-U','postgres','-d','bbpub_'+mode)
            return '\n'.join(line for line in text.splitlines() if not line.startswith(('\\restrict ', '\\unrestrict ')))+'\n'
        def physical_files(mode):
            home = root/f'out/signed_{mode}'
            return {str(p.relative_to(home)):dict(bytes=p.stat().st_size,sha256=sha(p))
                    for folder in ['kind=7','archive-state'] for p in sorted((home/folder).rglob('*')) if p.is_file()}

        for mode in MODES:
            home = root/f'out/signed_{mode}'
            chome = f'/out/signed_{mode}'
            for name, role in [('control','bigbrotr_control'), ('reader','bigbrotr_receipt_reader')]:
                f = home/name; f.write_bytes((root/'secrets'/role).read_bytes()); f.chmod(0o600)
            text = (source/'config/recipes/archive-only.toml').read_text()
            for section, key, value in [
                ('administration','enabled',False), ('topology','dataset_id','01'*16),
                ('topology','instance_id','02'*16), ('archive','catalog_id','5a'*16),
                ('storage','archive_root',chome+'/kind=7'), ('storage','archive_state_root',chome+'/archive-state'),
                ('storage','scratch_root',chome+'/initialize-scratch'),
                ('postgres.control','database','bbpub_control_'+mode), ('postgres.control','password_ref','file:'+chome+'/control'),
                ('postgres.catalog','database','bbpub_'+mode), ('postgres.catalog','user','CatalogManager'),
                ('postgres.catalog','password_ref','file:'+chome+'/manager'),
            ]:
                text = change(text,section,key,value)
            text += '\n[runtime]\nheartbeat_interval = "1s"\n'
            text = text.replace('[postgres.catalog]\n','[postgres.catalog]\nread_user="bigbrotr_receipt_reader"\nread_password_ref="file:'+chome+'/reader"\n')
            (home/'receipt.toml').write_text(text)
            catalog_dumps[mode] = dump(mode)
            (home/'receipt-catalog-before.sql').write_text(catalog_dumps[mode])
            physical[mode] = physical_files(mode)
            original = rows('bbpub_control_'+mode)
            originals[mode] = original
            files = input_files(home,mode)
            rocks = home/'rocks'; permission = rocks.stat().st_mode & 0o777; rocks.chmod(0)
            try:
                assert subprocess.run(['docker','exec',runner,'test','-r',chome+'/rocks'],capture_output=True).returncode == 1
                for wanted in restart_expectations(mode):
                    expected, status = wanted['expected'], wanted['receipt']
                    attempt = bytes(expected['attempt_id']).hex()
                    observed = cli(mode,attempt,code=4 if status == 'conflict' else 0,
                                   contains='ObservationUnknown' if status == 'conflict' else None)
                    if status != 'conflict':
                        assert observed == dict(format='bigbrotr-archive-receipt/1',state=status,
                            dataset_id='01'*16,catalog_id='5a'*16,attempt_id=attempt,
                            publication_ordinal=str(expected['publication_ordinal']),batch_digest=expected['batch_digest'],
                            max_receipts=128,catalog_inspection='verified',reader_privileges='select-only',
                            writer_exclusion='not-performed',replay_authority='not-granted',source_protection='not-granted',
                            ready=False,owner_release='committed',policy_revision='1')
                assert input_files(home,mode) == files and rows('bbpub_control_'+mode) == original
            finally:
                rocks.chmod(permission)
            passed('signed-retained-observations-'+mode,expected=restart_expectations(mode),rows=original,inputs=files)
        no_clients()
        cli('normal','11'*16,code=4,contains='retained Control attempt missing')
        home = root/'out/signed_normal'; inputs = home/'archive-state'/('b6'*16+'.inputs')
        saved = inputs.read_bytes()
        inputs.rename(inputs.with_suffix('.saved'))
        try: cli('normal','b6'*16,code=4,contains='inputs missing')
        finally: inputs.with_suffix('.saved').rename(inputs)
        try:
            inputs.write_bytes(saved[:-1]+bytes([saved[-1]^1]))
            cli('normal','b6'*16,code=4,contains='inputs unresolved')
        finally: inputs.write_bytes(saved)
        passed('missing-control-missing-input-corrupt-input-never-absent')
        # Whole-catalog cap: exact one-row catalog passes; two rows with cap1 fails.
        assert cli('before','be'*16,cap=1)['state'] == 'present'
        cli('normal','b6'*16,cap=1,code=4,contains='ObservationUnknown')
        cli('normal','b6'*16,cap=490,code=3,contains='ResourceLimit')
        assert cli('normal','b6'*16,cap=489)['state'] == 'present'
        config = home/'receipt.toml'; original_text = config.read_text()
        try:
            # Managed seed permits 1GiB, accepted policy remains512MiB. A cap of
            # 490 would fit the file profile but must still be refused.
            config.write_text(original_text.replace('[archive]\n','[archive]\nworker_memory_bytes = "1GiB"\n'))
            cli('normal','b6'*16,cap=490,code=3,contains='ResourceLimit')
        finally: config.write_text(original_text)
        passed('complete-scan-boundary-native-floor-and-managed-seed-drift')
        for grant, revoke in [
            ('GRANT INSERT ON public.ducklake_snapshot TO bigbrotr_receipt_reader','REVOKE INSERT ON public.ducklake_snapshot FROM bigbrotr_receipt_reader'),
            ('GRANT UPDATE(snapshot_id) ON public.ducklake_snapshot TO bigbrotr_receipt_reader','REVOKE UPDATE(snapshot_id) ON public.ducklake_snapshot FROM bigbrotr_receipt_reader'),
            ('GRANT TEMP ON DATABASE bbpub_normal TO bigbrotr_receipt_reader','REVOKE TEMP ON DATABASE bbpub_normal FROM bigbrotr_receipt_reader'),
            ('ALTER ROLE bigbrotr_receipt_reader CREATEROLE','ALTER ROLE bigbrotr_receipt_reader NOCREATEROLE'),
            ('REVOKE SELECT ON public.ducklake_snapshot FROM bigbrotr_receipt_reader','GRANT SELECT ON public.ducklake_snapshot TO bigbrotr_receipt_reader'),
        ]:
            sql('bbpub_normal',grant)
            try: cli('normal','b6'*16,code=4,contains='UnsafeRole')
            finally: sql('bbpub_normal',revoke)
        assert cli('normal','b6'*16)['state'] == 'present'
        passed('effective-reader-privilege-refusals-and-identical-retry')
        # Real native wait after manager inspection; supervise policy and renew
        # Control while PostgreSQL holds the reader's snapshot query.
        p, holder = lock('bbpub_normal','LOCK TABLE public.ducklake_data_file IN ACCESS EXCLUSIVE MODE')
        item = start(); observed = wait_lock('bigbrotr_receipt_reader','bbpub_normal',holder)
        assert observed['application_name'] == 'bigbrotr-archive-receipt'
        before = sql('bbpub_control_normal',"SELECT lease_expires_at_us FROM control.owner_epochs WHERE role='control' ORDER BY epoch DESC LIMIT 1")
        time.sleep(3)
        after = sql('bbpub_control_normal',"SELECT lease_expires_at_us FROM control.owner_epochs WHERE role='control' ORDER BY epoch DESC LIMIT 1")
        assert int(after) > int(before)
        unlock(p)
        assert json.loads(terminal(item,0)['stdout'])['state'] == 'present'
        no_clients(); passed('native-blocked-read-keeps-control-heartbeats',observed=observed,before=before,after=after)
        for reason in ['INT','TERM','policy','lease']:
            p, holder = lock('bbpub_normal','LOCK TABLE public.ducklake_data_file IN ACCESS EXCLUSIVE MODE')
            item = start(); observed = wait_lock('bigbrotr_receipt_reader','bbpub_normal',holder)
            watcher = Process(['docker','exec','-i',runner,'python3','-u','-c',WATCH,*item[1]['args']],input_pipe=True)
            watchers.append(watcher)
            item[1]['worker'] = event(watcher,'watch-ready')
            began = time.monotonic()
            if reason in ['INT','TERM']:
                item[1]['signal'] = json.loads(command('docker','exec',runner,'python3','-c',SIGNAL,reason,*item[1]['args']))
                contains = 'cancelled'
            elif reason == 'policy':
                # Controlled accepted-head change; this fixture is not a product
                # management command or audit qualification. Reuse the exact
                # valid document while advancing its authenticated policy epoch.
                sql('bbpub_control_normal',"BEGIN; INSERT INTO control.policy_revisions SELECT 2,dataset_id,1,config_revision,schema_version,document,document_digest,accepted_at_us+1,actor_id FROM control.policy_revisions WHERE revision=1; UPDATE control.datasets SET policy_revision=2; COMMIT;")
                contains = 'policy changed'
            else:
                blocked, control_holder = lock('bbpub_control_normal','LOCK TABLE control.config_revisions IN ACCESS EXCLUSIVE MODE')
                wait_lock('bigbrotr_control','bbpub_control_normal',control_holder)
                contains = 'Control backend operation failed'
            record = terminal(item,4,contains)
            elapsed = time.monotonic()-began
            assert elapsed < 9
            send(watcher,'finish\n')
            item[1]['worker_terminal'] = event(watcher,'watch-terminal')
            assert watcher.wait(5)==0 and not watcher.stderr
            assert item[1]['worker_terminal']==dict(event='watch-terminal',native_exited=True,reaped=True)
            save()
            if reason == 'lease': unlock(blocked)
            unlock(p); no_clients()
            passed('blocked-native-'+reason.lower()+'-joins-without-observation',observed=observed,elapsed_seconds=elapsed)
        for mode in MODES:
            after_dump = dump(mode)
            (root/f'out/signed_{mode}/receipt-catalog-after.sql').write_text(after_dump)
            assert after_dump == catalog_dumps[mode]
            assert physical_files(mode) == physical[mode]
            assert rows('bbpub_control_'+mode) == originals[mode]
            assert input_files(root/f'out/signed_{mode}',mode)
        passed('all-catalog-rows-and-retained-files-unchanged',
               catalog_sha256={m:hashlib.sha256(v.encode()).hexdigest() for m,v in catalog_dumps.items()}, physical_files=physical)
        assert inputs.read_bytes() == saved
        no_clients()
        report['status'] = 'PASS'
    except BaseException as error:
        report.update(status='FAIL' if isinstance(error,AssertionError) else 'INCOMPLETE',failure=type(error).__name__)
        raise
    finally:
        # First end only our lock transactions; then terminate/join only our
        # exact executable command lines. Never infer PG exclusion from docker-exec.
        for p in list(blockers):
            try: unlock(p)
            except Exception as error:
                report['status']='INCOMPLETE';report.setdefault('cleanup_errors',[]).append(type(error).__name__)
        for item in processes:
            p,record=item
            try:
                if p.process.poll() is None:
                    command('docker','exec',runner,'python3','-c',SIGNAL,'TERM',*record['args'])
            except Exception as error:
                record['cleanup_signal_error']=type(error).__name__;report['status']='INCOMPLETE'
            finally:
                try:p.stop()
                except Exception as error:
                    record['cleanup_join_error']=type(error).__name__;report['status']='INCOMPLETE'
                record.update(exit=p.process.poll(),stdout=p.stdout.decode(errors='replace'),stderr=p.stderr.decode(errors='replace'))
                save()
        for watcher in watchers:
            try:
                if watcher.process.poll() is None:
                    # EOF releases this observer's pidfds without touching peers.
                    watcher.process.stdin.close()
                    watcher.wait(5)
                watcher.stop()
            except Exception as error:
                report['status']='INCOMPLETE';report.setdefault('cleanup_errors',[]).append(type(error).__name__)
        save()
    assert report['status']=='PASS'
    print(json.dumps(dict(status='PASS',cases=len(report['cases']),source=resources['source'])),flush=True)


if __name__ == '__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root',type=Path)
    run(parser.parse_args().root.resolve())
