#!/usr/bin/env python3
"""Exact-source native PG: delayed owner execution, live driver and lost driver.

The proxy retains the actual Bind/Execute/Sync request, then deliberately keeps
its backend alive after the client disappears. No SQL/authentication payloads
are recorded. This is a bounded lab fixture, not installed recovery.
"""
import argparse
import json
from pathlib import Path
import time

from signed_publication_cases import SOURCE, Process, command, event, send, sha
from signed_publication_oracle import corpus
from missing_import_refusals import run as refusal_cases

MODES = ('custody_prelock_live', 'custody_prelock_dead')
ROCKS_DIGEST = 'da535e9d1edcdce8bf09033bb1d2b48ee10941c34f47b0b3db228f6285128c61'


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
    evidence = root/'missing-import-cases.json'

    def save():
        evidence.write_text(json.dumps(result, indent=2)+'\n')

    def sql(db, query):
        return command('docker', 'exec', '-i', pg, 'psql', '-XAt', '-v', 'ON_ERROR_STOP=1',
                       '-U', 'postgres', '-d', db, input=query)

    binary = ['docker', 'exec', '--env', 'LD_LIBRARY_PATH=/out/archive-worker/lib',
              runner, '/out/native-tests/archive-publication-probe']
    save()
    for mode in MODES:
        live = mode.endswith('_live')
        case = dict(mode=mode, status='INCOMPLETE', failures=[], processes=[])
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

        def observe(action, expected, database=None):
            probe = start(binary+[mode, action]+([database] if database else []))
            observed = event(probe, 'custody-observed')
            assert observed['state'] == ('occupied' if expected == 'occupied' else 'recovered')
            if action == 'resolve':
                observed = event(probe, 'resolve-result')
                case.setdefault('resolutions', []).append(observed)
                if observed['state'] != expected:
                    case['failures'].append(dict(expected=expected, observed=observed))
                if observed['state'] == 'not-imported':
                    assert not observed['commit_started']
            assert probe.wait(10) == 0 and not probe.stderr and not probe.overflow
            return observed

        def snapshot():
            return json.loads(sql(db, f"SELECT json_build_object('owner',(SELECT row_to_json(o) FROM bigbrotr_archive.owner o),'attempts',(SELECT count(*) FROM bigbrotr_archive.attempts),'role',(SELECT json_build_object('login',rolcanlogin,'password_null',rolpassword IS NULL) FROM pg_authid WHERE rolname='{login}'),'files',(SELECT count(*) FROM public.ducklake_data_file),'snapshots',(SELECT count(*) FROM public.ducklake_snapshot));"))

        try:
            proxy = start(['docker', 'exec', '-i', runner, 'python3', '-B',
                           '/source/experiments/archive-engine/native_commit_proxy.py',
                           '--mode', 'hold-manager-owner'], True)
            hello = event(proxy, 'ready')
            assert hello['source_sha256'] == sha(SOURCE/'experiments/archive-engine/native_commit_proxy.py')
            parent = start(binary+[mode, str(hello['port'])])
            created = event(parent, 'custody-created', 45)
            event(parent, 'fresh-custody-refused')
            login = 'bb_archive_'+created['attempt']
            journal = home/'lifetimes'/created['attempt']/'intent.json'
            case['journal_sha256'] = sha(journal)
            held = case['held'] = event(proxy, 'held')
            assert held['stage'] == 'manager-owner' and held['owner_requests'] == 1
            assert held['owner_forwarded'] == held['data_queries'] == held['data_commits'] == 0
            case['before'] = snapshot()
            assert case['before'] == dict(owner=dict(singleton=True, ordinal=0, enabled=False),
                                          attempts=0, role=None, files=0, snapshots=2)
            observe('observe', 'occupied')
            if not live:
                (home/'cancel-operation').touch(exist_ok=False)
                assert not event(parent, 'prelock-driver-terminal')['committing']
                assert parent.wait(10) == 0 and not parent.stderr
                observe('resolve', 'not-imported')
                assert snapshot() == case['before']
            save()
            send(proxy, 'release\n')
            completed = case['owner_complete'] = event(proxy, 'owner-complete')
            assert completed['owner_forwarded'] == completed['owner_complete'] == 1
            if live:
                assert event(parent, 'prelock-driver-terminal')['committing']
                assert parent.wait(10) == 0 and not parent.stderr
                case['after_release'] = snapshot()
                assert case['after_release']['attempts'] == 1
                assert case['after_release']['owner'] == dict(singleton=True, ordinal=1, enabled=True)
                observe('resolve', 'sealed-changed')
                observe('resolve', 'sealed-unchanged')
            else:
                # The actual request executed after local recovery. Its backend
                # owns a row lock, but the destroyed driver sent no provisioning.
                case['late_backend'] = json.loads(sql(db, f"SELECT json_build_object('pid',pid,'state',state,'owner_query',query='SELECT ordinal::text,enabled FROM bigbrotr_archive.owner WHERE singleton FOR UPDATE','relation_row_share',EXISTS(SELECT 1 FROM pg_locks WHERE pid=a.pid AND relation='bigbrotr_archive.owner'::regclass AND mode='RowShareLock' AND granted)) FROM pg_stat_activity a WHERE pid={held['backend_pid']};"))
                assert case['late_backend'] == dict(pid=held['backend_pid'], state='idle in transaction', owner_query=True, relation_row_share=True)
                case['after_release'] = snapshot()
                assert case['after_release'] == case['before']
                resolver = start(binary+[mode, 'resolve'])
                waiter = event(resolver, 'resolve-begin')['backend_pid']
                until = time.monotonic()+3
                while time.monotonic() < until:
                    blocked = json.loads(sql(db, f"SELECT coalesce(json_agg(row_to_json(t)),'[]'::json) FROM (SELECT w.pid AS waiter,w.backend_start AS waiter_start,w.wait_event_type,w.wait_event,h.pid AS blocker,h.backend_start AS blocker_start,h.state AS blocker_state FROM pg_stat_activity w JOIN pg_stat_activity h ON h.pid=ANY(pg_blocking_pids(w.pid)) WHERE w.pid={waiter} AND h.pid={held['backend_pid']} AND w.datname=current_database() AND h.datname=current_database())t;"))
                    if blocked:
                        break
                    time.sleep(.01)
                assert len(blocked) == 1 and blocked[0]['wait_event_type'] == 'Lock'
                case['late_request_blocker'] = blocked[0]
                save()
            send(proxy, 'stop\n')
            assert proxy.wait(10) == 0 and not proxy.stderr
            terminal = event(proxy, 'terminal')
            assert terminal['unexpected'] == 0
            assert terminal['data_queries'] == terminal['data_commits'] == terminal['server_complete'] == int(live)
            if not live:
                resolved = event(resolver, 'resolve-result')
                assert resolved['state'] == 'not-imported' and not resolved['commit_started']
                assert resolver.wait(10) == 0 and not resolver.stderr and not resolver.overflow
                observe('resolve', 'not-imported')
                case['refusals'] = []
                refusal_cases(sql, observe, case['refusals'], save)
            case['after_stop'] = snapshot()
            if live:
                assert case['after_stop'] == dict(owner=dict(singleton=True, ordinal=1, enabled=False),
                                                  attempts=1, role=dict(login=False, password_null=True), files=0, snapshots=2)
            else:
                assert case['after_stop'] == case['before']
            until = time.monotonic()+3
            while sql(db, 'SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid();') != '0':
                assert time.monotonic() < until
                time.sleep(.01)
            rows = [json.loads(line) for line in parent.stdout.splitlines()]
            assert next(v['batches'] for v in rows if v['event'] == 'retained-inputs') == corpus()[0]
            assert rows[-1] == dict(event='custody-terminal', rocks_rows=41, rocks_digest=ROCKS_DIGEST)
            assert sha(journal) == case['journal_sha256'] and not list(home.rglob('*.parquet'))
            case['status'] = 'FAIL' if case['failures'] else 'PASS'
        finally:
            cleanup_errors = []
            try:
                for process in processes:
                    try:
                        process.stop()
                    except BaseException as error:
                        cleanup_errors.append(type(error).__name__)
                    finally:
                        case['processes'].append(dict(exit=process.process.returncode,
                            stdout=process.stdout.decode(errors='replace'), stderr=process.stderr.decode(errors='replace'),
                            overflow=process.overflow))
                if cleanup_errors:
                    case.update(status='INCOMPLETE', cleanup_errors=cleanup_errors)
                    raise RuntimeError('wrapper cleanup incomplete; inspect actual owned processes')
            finally:
                save()
    result['status'] = 'PASS' if all(v['status'] == 'PASS' for v in result['cases']) else 'FAIL'
    save()
    print(json.dumps(dict(status=result['status'], source=result['source'], cases=len(result['cases']))))
    return 0 if result['status'] == 'PASS' else 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    raise SystemExit(main(parser.parse_args().root))
