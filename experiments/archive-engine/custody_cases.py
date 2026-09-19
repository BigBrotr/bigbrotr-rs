#!/usr/bin/env python3
"""Owned exact-source Linux lab: local custody versus live native/PG lifetimes.

Kill one identity-checked PID, never the container/group. Retain incomplete results
and inspect server blockers before releasing a deliberately queued COMMIT.
"""
import argparse
import json
from pathlib import Path
import subprocess
import sys
import time

from signed_publication_cases import SOURCE, Process, command, event, send, sha

MODES = ('custody_worker', 'custody_cancel', 'custody_manager')
ROCKS_DIGEST = 'da535e9d1edcdce8bf09033bb1d2b48ee10941c34f47b0b3db228f6285128c61'
# Runs inside the owned PID namespace. pidfd prevents signaling a recycled PID.
PROCESS = r'''
import json,os,signal,sys
from pathlib import Path
pid=int(sys.argv[1]); action=sys.argv[2]; lock=Path(sys.argv[3])
def identity():
 p=Path('/proc')/str(pid)
 if not p.exists(): return None
 stat=(p/'stat').read_text().rsplit(')',1)[1].split()
 if stat[0]=='Z': return dict(pid=pid,state='Z',start=stat[19])
 target=lock.stat()
 held=[]
 for fd in (p/'fd').iterdir():
  try: m=fd.stat()
  except FileNotFoundError: continue
  if (m.st_dev,m.st_ino)==(target.st_dev,target.st_ino): held.append(int(fd.name))
 return dict(pid=pid,ppid=int(stat[1]),state=stat[0],start=stat[19],
  exe=os.readlink(p/'exe'),held=sorted(held),inode=target.st_ino,device=target.st_dev,
  children=[int(v) for v in (p/'task'/str(pid)/'children').read_text().split()])
before=identity()
if action=='kill':
 assert before and before['state']!='Z' and before['start']==sys.argv[4] and before['exe']==sys.argv[5]
 fd=os.pidfd_open(pid)
 after=identity()
 assert after and all(after[k]==before[k] for k in ('pid','start','exe'))
 signal.pidfd_send_signal(fd,signal.SIGKILL)
 os.close(fd)
print(json.dumps(before))
'''


def main(root):
    resources = json.loads((root/'resources.json').read_text())
    assert SOURCE == root/'source'
    assert command('git', '-C', str(SOURCE), 'rev-parse', 'HEAD') == resources['source']
    assert not command('git', '-C', str(SOURCE), 'status', '--porcelain', '--untracked-files=all')
    runner, pg = resources['container_id'], resources['postgres_id']
    for identity in (runner, pg):
        value = json.loads(command('docker', 'inspect', identity))[0]
        assert value['Config']['Labels']['org.bigbrotr.publication'] == resources['owner']
        assert value['State']['Running'] and not value['State']['OOMKilled']
    ready = json.loads((root/'out/native-tests/ready.json').read_text())
    assert ready['source'] == resources['source']
    for label, path in [('worker', root/'out/archive-worker/bigbrotr-archive-worker'),
                        ('publisher', root/'out/native-tests/archive-publication-probe')]:
        assert sha(path) == ready[label]
    result = dict(source=resources['source'], tree=resources['tree'], status='INCOMPLETE',
                  artifacts=ready, harness_sha256=sha(Path(__file__)), cases=[])
    evidence = root/'custody-cases.json'

    def save():
        evidence.write_text(json.dumps(result, indent=2)+'\n')

    def sql(db, query):
        return command('docker', 'exec', '-i', pg, 'psql', '-XAt', '-v', 'ON_ERROR_STOP=1',
                       '-U', 'postgres', '-d', db, input=query)

    binary = ['docker', 'exec', '--env', 'LD_LIBRARY_PATH=/out/archive-worker/lib',
              runner, '/out/native-tests/archive-publication-probe']
    save()
    for mode in MODES:
        case = dict(mode=mode, status='INCOMPLETE', processes=[])
        result['cases'].append(case)
        save()
        db = 'bbpub_'+mode
        sql('postgres', f'CREATE DATABASE {db}; CREATE DATABASE bbpub_control_{mode};')
        sql(db, 'GRANT USAGE,CREATE ON SCHEMA public TO "BootstrapMixed";')
        home = root/f'out/signed_{mode}'
        processes = []

        def start(args, pipe=False):
            process = Process(args, input_pipe=pipe, limit=128*1024)
            processes.append(process)
            return process

        def observe(expected):
            probe = start(binary+[mode, 'observe'])
            assert event(probe, 'custody-observed')['state'] == expected
            assert probe.wait(10) == 0 and not probe.stderr and not probe.overflow
            if expected == 'recovered':
                rows = [json.loads(line) for line in probe.stdout.splitlines()]
                retained = next(v for v in rows if v['event'] == 'retained-rocks')
                assert retained == dict(event='retained-rocks', rows=41, digest=ROCKS_DIGEST)

        def process_info(pid, action='inspect', previous=None):
            args = [str(pid), action, lock]
            if previous:
                args += [previous['start'], previous['exe']]
            return json.loads(command('docker', 'exec', runner, 'python3', '-c', PROCESS, *args))

        def gone(pid):
            until = time.monotonic()+5
            while time.monotonic() < until:
                info = process_info(pid)
                if info is None:
                    return
                time.sleep(.02)
            raise AssertionError('actual process not reaped')

        try:
            proxy = start(['docker', 'exec', '-i', runner, 'python3', '-B',
                           '/source/experiments/archive-engine/native_commit_proxy.py', '--mode',
                           'hold-commit' if mode == 'custody_worker' else 'hold-manager-commit'], True)
            hello = event(proxy, 'ready')
            assert hello['source_sha256'] == sha(SOURCE/'experiments/archive-engine/native_commit_proxy.py')
            parent = start(binary+[mode, str(hello['port'])])
            created = event(parent, 'custody-created', 45)
            lock = f'/out/signed_{mode}/lifetimes/{created["attempt"]}/custody.lock'
            journal = home/'lifetimes'/created['attempt']/'intent.json'
            case['journal_before_sha256'] = sha(journal)
            held = case['held'] = event(proxy, 'held')
            assert held['stage'] == ('commit' if mode == 'custody_worker' else 'manager-commit')
            case['parent_identity'] = process_info(created['parent_pid'])
            assert case['parent_identity']['exe'] == '/out/native-tests/archive-publication-probe'
            save()
            if mode == 'custody_worker':
                children = [process_info(pid) for pid in case['parent_identity']['children']]
                workers = [v for v in children if v and v.get('exe') == '/out/archive-worker/bigbrotr-archive-worker']
                assert len(workers) == 1 and workers[0]['held']
                worker = case['worker_before_parent_loss'] = workers[0]
                observe('occupied')
                process_info(created['parent_pid'], 'kill', case['parent_identity'])
                assert parent.wait(5) == 137
                gone(created['parent_pid'])
                # The existing worker binds PDEATHSIG=SIGKILL. Do not disable
                # that safeguard to manufacture a survivor. The separate native
                # test drops coordinator descriptors with its spawning thread
                # alive, proving the worker alone keeps custody across exec.
                gone(worker['pid'])
                case['worker_reaped_after_parent_loss'] = True
                # Proxy still holds the uncommitted server transaction, so this
                # local result is deliberately not catalog-outcome evidence.
                observe('recovered')
            else:
                connected = event(parent, 'manager-connected')
                assert connected['backend_pid'] == held['backend_pid']
                (home/'cancel-operation').touch(exist_ok=False)
                assert event(parent, 'cancelled-driver-retained')['backend_pid'] == held['backend_pid']
                observe('occupied')
                if mode == 'custody_manager':
                    process_info(created['parent_pid'], 'kill', case['parent_identity'])
                    assert parent.wait(5) == 137
                    gone(created['parent_pid'])
                else:
                    (home/'destroy-transport').touch(exist_ok=False)
                    event(parent, 'transport-destroyed-custody-recovered')
                    (home/'finish-custody').touch(exist_ok=False)
                    assert parent.wait(5) == 0 and not parent.stderr
                observe('recovered')
            save()
            # Actual product seal must wait for the surviving PG transaction;
            # a local journal lock is not a replacement for the owner-row lock.
            sealer = start(binary+[mode, 'seal'])
            assert event(sealer, 'custody-observed')['state'] == 'recovered'
            waiter = event(sealer, 'seal-begin')['backend_pid']
            until = time.monotonic()+3
            while time.monotonic() < until:
                rows = json.loads(sql(db, f"SELECT coalesce(json_agg(row_to_json(t)),'[]'::json) FROM (SELECT w.pid AS waiter,w.backend_start AS waiter_start,w.wait_event_type,w.wait_event,h.pid AS blocker,h.backend_start AS blocker_start,h.state AS blocker_state FROM pg_stat_activity w JOIN pg_stat_activity h ON h.pid=ANY(pg_blocking_pids(w.pid)) WHERE w.pid={waiter} AND h.pid={held['backend_pid']} AND w.datname=current_database() AND h.datname=current_database())t;"))
                if rows:
                    break
                time.sleep(.02)
            assert len(rows) == 1 and rows[0]['wait_event_type'] == 'Lock'
            case['catalog_blocking'] = rows[0]
            save()
            send(proxy, 'close\n' if mode == 'custody_worker' else 'release\n')
            assert event(sealer, 'seal-result')['disabled'] is True
            assert sealer.wait(10) == 0 and not sealer.stderr
            send(proxy, 'stop\n')
            assert proxy.wait(10) == 0 and not proxy.stderr
            terminal = next(v for v in map(json.loads, proxy.stdout.splitlines()) if v['event']=='terminal')
            assert terminal['unexpected'] == 0
            assert terminal['data_commits'] == 1
            assert terminal['server_complete'] == int(mode != 'custody_worker')
            assert terminal['faults'] == int(mode == 'custody_worker')
            login = 'bb_archive_'+created['attempt']
            final = json.loads(sql(db, f"SELECT json_build_object('owner',(SELECT row_to_json(o) FROM bigbrotr_archive.owner o),'attempt_count',(SELECT count(*) FROM bigbrotr_archive.attempts),'attempt',(SELECT encode(attempt_id,'hex') FROM bigbrotr_archive.attempts),'login',rolcanlogin,'password_null',rolpassword IS NULL,'files',(SELECT count(*) FROM public.ducklake_data_file),'snapshots',(SELECT count(*) FROM public.ducklake_snapshot),'backends',(SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid())) FROM pg_authid WHERE rolname='{login}';"))
            case['final_catalog'] = final
            assert final == dict(owner=dict(singleton=True,ordinal=1,enabled=False),
                                 attempt_count=1,attempt=created['attempt'],login=False,
                                 password_null=True,files=0,snapshots=2,backends=0)
            assert sha(journal) == case['journal_before_sha256']
            case['journal_after_sha256'] = sha(journal)
            case['physical_files'] = [dict(path=str(p.relative_to(home)),bytes=p.stat().st_size,sha256=sha(p))
                                      for p in sorted(home.rglob('*.parquet'))]
            assert len(case['physical_files']) == (2 if mode == 'custody_worker' else 0)
            case['status'] = 'PASS'
        except BaseException as error:
            case['failure'] = type(error).__name__
            raise
        finally:
            # Stopping docker-exec clients is not remote process exclusion.
            # Retain exact process identities/output for the terminal lab audit.
            cleanup_errors = []
            try:
                for process in processes:
                    try:
                        process.stop()
                    except BaseException as error:
                        cleanup_errors.append(type(error).__name__)
                    finally:
                        case['processes'].append(dict(exit=process.process.returncode,
                            stdout=process.stdout.decode(errors='replace'),stderr=process.stderr.decode(errors='replace'),
                            overflow=process.overflow))
                if cleanup_errors:
                    case.update(status='INCOMPLETE',cleanup_errors=cleanup_errors)
                    raise RuntimeError('custody wrapper cleanup incomplete; inspect owned remote processes')
            finally:
                save()
    result['status'] = 'PASS'
    save()
    print(json.dumps(dict(status='PASS',source=result['source'],cases=len(result['cases']))))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    main(parser.parse_args().root)
