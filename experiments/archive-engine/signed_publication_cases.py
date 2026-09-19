#!/usr/bin/env python3
"""Trusted exact-Git Tux lab: actual signed publication and native COMMIT loss.

Requires the individually inventoried publication runner; no host ports or secret
output. Results are retained before assertions. Failed/unknown processes require
the existing owner-verified terminal lab audit; killing docker-exec is not exclusion.
"""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time

sys.dont_write_bytecode = True
SOURCE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SOURCE / 'experiments/control-migrations'))
from fault_cases import Incomplete, Process
from signed_publication_oracle import MODES, check, expected, input_files, restart_expectations


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def command(*args, input=None, timeout=30):
    r = subprocess.run(args, input=input, capture_output=True, text=True, timeout=timeout)
    assert r.returncode == 0, (args[0], r.returncode, r.stderr)
    return r.stdout.strip()


def execute(args, timeout, observation, interact=None):
    process = Process(args, limit=256 * 1024)
    observation.update(exit=None, remote_exclusion='UNKNOWN')
    deadline = time.monotonic() + timeout
    try:
        if interact:
            interact(process)
        observation['exit'] = process.wait(max(0, deadline - time.monotonic()))
    except BaseException as error:
        observation['failure'] = str(error) if isinstance(error, Incomplete) else type(error).__name__
        raise
    finally:
        process.stop()
        observation.update(stdout=process.stdout.decode(errors='replace'),
                           stderr=process.stderr.decode(errors='replace'), overflow=process.overflow)


def proxy_observation(proxy):
    # This proxy emits only allowlisted counters, never SQL or credentials.
    return dict(exit=proxy.process.poll(), stdout=proxy.stdout.decode(errors='replace'),
                stderr=proxy.stderr.decode(errors='replace'), overflow=proxy.overflow,
                remote_exclusion='UNKNOWN')


def event(process, wanted, timeout=15):
    deadline = time.monotonic() + timeout
    with process.condition:
        while True:
            complete = bytes(process.stdout).split(b'\n')[:-1]
            for line in complete:
                value = json.loads(line)
                if value.get('event') == wanted:
                    return value
            assert not process.overflow and process.process.poll() is None
            assert time.monotonic() < deadline, 'fixture event deadline'
            process.condition.wait(.05)


def send(process, text):
    process.process.stdin.write(text.encode())
    process.process.stdin.flush()


def main(root, installed=False):
    resources = json.loads((root / 'resources.json').read_text())
    revision = resources['source']
    assert SOURCE == root / 'source'
    assert command('git', '-C', str(SOURCE), 'rev-parse', 'HEAD') == revision
    assert not command('git', '-C', str(SOURCE), 'status', '--porcelain', '--untracked-files=all')
    runner, pg = resources['container_id'], resources['postgres_id']
    for identity in [runner, pg]:
        c = json.loads(command('docker', 'inspect', identity))[0]
        assert c['Config']['Labels']['org.bigbrotr.publication'] == resources['owner']
        assert c['State']['Running'] and not c['State']['OOMKilled']
    out = root / 'out/native-tests'
    ready = json.loads((out / 'ready.json').read_text())
    assert ready['source'] == revision
    for label, path in [('worker', root / 'out/archive-worker/bigbrotr-archive-worker'),
                        ('publisher', out / 'archive-publication-probe'), ('reader', out / 'archive-read-probe')]:
        assert sha(path) == ready[label]
    result = dict(source=revision, tree=resources['tree'], status='INCOMPLETE', artifacts=ready,
                  oracle_sha256=sha(SOURCE / 'experiments/archive-engine/signed_publication_oracle.py'),
                  harness_sha256=sha(Path(__file__)), cases=[])
    result['installation_mode'] = 'installed-manager' if installed else 'privileged-foundation'
    evidence = root / ('installed-signed-cases.json' if installed else 'signed-cases.json')
    assert not evidence.exists()
    if installed:
        assert sha(out / 'bigbrotr') == ready['control']

    def save():
        evidence.write_text(json.dumps(result, indent=2) + '\n')

    def sql(db, query):
        return command('docker', 'exec', '-i', pg, 'psql', '-XAt', '-v', 'ON_ERROR_STOP=1',
                       '-U', 'postgres', '-d', db, input=query)

    def public_access(db):
        return json.loads(sql(db, "SELECT json_build_object('database',(SELECT coalesce(json_agg(a.privilege_type ORDER BY a.privilege_type),'[]'::json) FROM pg_database d CROSS JOIN LATERAL aclexplode(COALESCE(d.datacl,acldefault('d',d.datdba))) a WHERE d.datname=current_database() AND a.grantee=0),'schema',(SELECT coalesce(json_agg(a.privilege_type ORDER BY a.privilege_type),'[]'::json) FROM pg_namespace n CROSS JOIN LATERAL aclexplode(COALESCE(n.nspacl,acldefault('n',n.nspowner))) a WHERE n.nspname='public' AND a.grantee=0))"))

    binary = ['docker', 'exec', '--env', 'LD_LIBRARY_PATH=/out/archive-worker/lib', runner]
    save()
    if installed:
        password = (root / 'secrets/CatalogManager').read_text()
        assert len(password) == 64 and all(c in '0123456789abcdef' for c in password)
        sql('postgres', 'CREATE ROLE "CatalogManager" LOGIN NOSUPERUSER CREATEROLE NOCREATEDB NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD ' + "'" + password + "';")
        del password
    for mode in MODES:
        case = dict(mode=mode, status='INCOMPLETE')
        result['cases'].append(case)
        save()
        db = 'bbpub_' + mode
        sql('postgres', f'CREATE DATABASE {db}; CREATE DATABASE bbpub_control_{mode};')
        if installed:
            sql(db, f'REVOKE ALL ON DATABASE {db} FROM PUBLIC; REVOKE ALL ON SCHEMA public FROM PUBLIC;')
        else:
            sql(db, 'GRANT USAGE,CREATE ON SCHEMA public TO "BootstrapMixed";')
        proxy = None
        holder = None

        def observe():
            data = root / f'out/signed_{mode}/kind=7'
            case['physical_files'] = [dict(path=str(p.relative_to(data)), bytes=p.stat().st_size, sha256=sha(p))
                                      for p in sorted(data.rglob('*.parquet'))]
            save()
            case['catalog_files'] = json.loads(sql(db, "SELECT coalesce(json_agg(row_to_json(t)),'[]'::json) FROM (SELECT d.data_file_id,d.path,d.path_is_relative,d.file_size_bytes,d.record_count,t.table_name,t.path AS table_path,s.path AS schema_path FROM ducklake_data_file d JOIN ducklake_table t ON t.table_id=d.table_id JOIN ducklake_schema s ON s.schema_id=t.schema_id ORDER BY t.table_name,d.data_file_id)t;"))
            case['active_writers'] = int(sql(db, "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND left(usename,11)='bb_archive_';"))
            if installed:
                home = root / f'out/signed_{mode}'
                case['public_access'] = public_access(db)
                case['installed_initializer'] = json.loads((home/'installed-initialization.json').read_text())
                case['runtime_manager'] = json.loads((home/'runtime-manager.json').read_text())
                case['roles'] = json.loads(sql(db, "SELECT json_build_object('manager_superuser',(SELECT rolsuper FROM pg_roles WHERE rolname='CatalogManager'),'manager_connect_delegate',has_database_privilege('CatalogManager',current_database(),'CONNECT WITH GRANT OPTION'),'manager_usage_delegate',has_schema_privilege('CatalogManager','public','USAGE WITH GRANT OPTION'),'native_and_guard_owned',(SELECT count(*)=32 AND bool_and(c.relowner='\"CatalogManager\"'::regrole) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname IN ('public','bigbrotr_archive') AND c.relkind='r'),'writers',(SELECT json_agg(json_build_object('login',a.login_name,'login_enabled',r.rolcanlogin,'password_removed',r.rolpassword IS NULL,'connect',has_database_privilege(r.oid,current_database(),'CONNECT'),'usage',has_schema_privilege(r.oid,'public','USAGE'),'connect_delegate',has_database_privilege(r.oid,current_database(),'CONNECT WITH GRANT OPTION'),'usage_delegate',has_schema_privilege(r.oid,'public','USAGE WITH GRANT OPTION')) ORDER BY a.ordinal) FROM bigbrotr_archive.attempts a JOIN pg_authid r ON r.oid=a.login_oid))"))
            save()

        try:
            port = 5432
            if mode != 'normal':
                proxy_mode = {'before': 'before-forward', 'after': 'after-complete',
                              'owner_first': 'hold-data', 'held_commit': 'hold-commit',
                              'held_rollback': 'hold-commit'}[mode]
                proxy = Process(['docker', 'exec', '-i', runner, 'python3', '-B',
                    '/source/experiments/archive-engine/native_commit_proxy.py', '--mode',
                    proxy_mode], input_pipe=True)
                hello = event(proxy, 'ready', 10)
                assert hello['event'] == 'ready'
                assert hello['source_sha256'] == sha(SOURCE / 'experiments/archive-engine/native_commit_proxy.py')
                port = hello['port']

            def blocking(waiter, blocker):
                assert type(waiter) is int and type(blocker) is int and waiter > 0 and blocker > 0
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline:
                    rows = json.loads(sql(db, f"SELECT coalesce(json_agg(row_to_json(r)),'[]'::json) FROM (SELECT w.pid AS waiter,w.backend_start AS waiter_start,w.usename AS waiter_user,w.wait_event_type,w.wait_event,h.pid AS blocker,h.backend_start AS blocker_start,h.usename AS blocker_user FROM pg_stat_activity w JOIN pg_stat_activity h ON h.pid=ANY(pg_blocking_pids(w.pid)) WHERE w.datname=current_database() AND h.datname=current_database() AND w.pid={waiter} AND h.pid={blocker} AND w.wait_event_type='Lock')r;"))
                    if rows:
                        assert len(rows) == 1
                        case.setdefault('blocking', []).append(rows[0])
                        save()
                        return rows[0]
                    time.sleep(.02)
                raise AssertionError('required PG lock wait absent')

            def race(parent):
                nonlocal holder
                held = event(proxy, 'held')
                assert held['stage'] == ('data' if mode == 'owner_first' else 'commit')
                case['held'] = held
                home = root / f'out/signed_{mode}'
                if mode == 'owner_first':
                    holder = Process(['docker', 'exec', '-i', pg, 'psql', '-XAtq', '-v',
                                      'ON_ERROR_STOP=1', '-U', 'postgres', '-d', db], input_pipe=True)
                    send(holder, "SET application_name='publication-race-holder'; BEGIN; LOCK TABLE bigbrotr_archive.scientific_schema IN ACCESS EXCLUSIVE MODE; SELECT json_build_object('event','holder-ready','backend_pid',pg_backend_pid());\n")
                    held_schema = event(holder, 'holder-ready')
                    case['holder'] = held_schema
                (home / 'activate').touch(exist_ok=False)
                activating = event(parent, 'activation-begin')
                case['activating'] = activating
                old_pid, manager_pid = held['backend_pid'], activating['backend_pid']
                n = 183 + 8 * MODES.index(mode)
                old_login = 'bb_archive_' + f'{n:02x}' * 16
                if mode == 'owner_first':
                    row = blocking(manager_pid, held_schema['backend_pid'])
                    assert row['waiter_user'] == ('CatalogManager' if installed else 'postgres') and row['blocker_user'] == 'postgres'
                    send(proxy, 'release\n')
                    row = blocking(old_pid, manager_pid)
                    assert row['waiter_user'] == old_login and row['blocker_user'] == ('CatalogManager' if installed else 'postgres')
                    send(holder, 'ROLLBACK;\n\\q\n')
                    assert holder.wait(5) == 0 and not holder.stderr
                else:
                    row = blocking(manager_pid, old_pid)
                    assert row['waiter_user'] == ('CatalogManager' if installed else 'postgres') and row['blocker_user'] == old_login
                    send(proxy, 'release\n' if mode == 'held_commit' else 'close\n')
                # Positive old child reap, successor fencing and exact historical
                # seal precede a separate native receipt/body observation. Only
                # that complete observation opens the fixture's next-launch gate.
                assert event(parent, 'old-excluded')['worker_active_permits'] == 0
                interim = case['before_successor'] = {}
                args = [] if mode == 'held_commit' else ['bootstrap']
                execute(binary + ['/out/native-tests/archive-read-probe', mode] + args, 45, interim)
                save()
                assert interim['exit'] == 0 and not interim['stderr']
                assert json.loads(interim['stdout']) == expected(mode, bootstrap_only=bool(args))
                state = json.loads(sql(db, "SELECT row_to_json(o) FROM bigbrotr_archive.owner o;"))
                assert state == dict(singleton=True, ordinal=4, enabled=True)
                case['successor_before_launch'] = state
                save()
                (home / 'readback-complete').touch(exist_ok=False)

            def delegation(parent):
                home = root / f'out/signed_{mode}'
                def state():
                    return json.loads(sql(db, "SELECT json_build_object('owner',(SELECT row_to_json(o) FROM bigbrotr_archive.owner o),'attempts',(SELECT coalesce(json_agg(row_to_json(a) ORDER BY ordinal),'[]'::json) FROM bigbrotr_archive.attempts a),'writers',(SELECT coalesce(json_agg(json_build_object('oid',oid,'name',rolname,'login',rolcanlogin,'password_present',rolpassword IS NOT NULL) ORDER BY rolname),'[]'::json) FROM pg_authid WHERE left(rolname,11)='bb_archive_'))"))
                def access():
                    return json.loads(sql(db, "SELECT json_build_object('connect',has_database_privilege('CatalogManager',current_database(),'CONNECT'),'connect_delegate',has_database_privilege('CatalogManager',current_database(),'CONNECT WITH GRANT OPTION'),'usage',has_schema_privilege('CatalogManager','public','USAGE'),'usage_delegate',has_schema_privilege('CatalogManager','public','USAGE WITH GRANT OPTION'))"))
                doc = (SOURCE/'docs/user/archive-initialization.md').read_text()
                upgrade = doc.split('```sql\n-- Runtime catalog manager delegation.\n',1)[1].split('```',1)[0]
                for privilege in ['connect','usage']:
                    event(parent, privilege+'-ready')
                    before = state()
                    assert before == dict(owner=dict(singleton=True,ordinal=0,enabled=False),attempts=[],writers=[])
                    assert access() == dict(connect=True,connect_delegate=True,usage=True,usage_delegate=True)
                    target = f'CONNECT ON DATABASE {db}' if privilege == 'connect' else 'USAGE ON SCHEMA public'
                    sql(db, f'REVOKE GRANT OPTION FOR {target} FROM "CatalogManager";')
                    rights = access()
                    wanted = dict(connect=True,connect_delegate=True,usage=True,usage_delegate=True)
                    wanted[privilege+'_delegate'] = False
                    assert rights == wanted
                    record = dict(privilege=privilege,before=before,revoked_access=rights,status='INCOMPLETE')
                    case.setdefault('delegation_refusals',[]).append(record)
                    save()
                    (home/(privilege+'-revoked')).touch(exist_ok=False)
                    record['refusal'] = event(parent, privilege+'-refused')
                    record['after'] = state()
                    assert record['after'] == before
                    assert record['refusal'] == dict(event=privilege+'-refused',error='UnsafeRole',commit_started=False)
                    # Execute the exact documented privileged ACL upgrade; do not
                    # rewrite applied SQL or repair roles through runtime access.
                    command('docker','exec','-i',pg,'psql','-XAt','-v','ON_ERROR_STOP=1',
                            '-v','catalog_database='+db,'-v','catalog_manager=CatalogManager',
                            '-U','postgres','-d',db,input=upgrade)
                    record['restored_access'] = access()
                    record['public_access'] = public_access(db)
                    assert record['restored_access'] == dict(connect=True,connect_delegate=True,usage=True,usage_delegate=True)
                    assert record['public_access'] == dict(database=[],schema=[])
                    assert state() == before
                    record['status'] = 'PASS'
                    save()
                    (home/(privilege+'-restored')).touch(exist_ok=False)

            case['parent'] = {}
            execute(binary + ['/out/native-tests/archive-publication-probe', mode, str(port)] + (['--installed'] if installed else []), 180,
                    case['parent'], race if MODES.index(mode) >= 3 else delegation if installed and mode == 'normal' else None)
            save()
            if proxy:
                proxy.process.stdin.write(b'stop\n')
                proxy.process.stdin.flush()
                case['proxy_exit'] = proxy.wait(10)
                case['proxy'] = [json.loads(line) for line in proxy.stdout.splitlines()]
                case['proxy_stderr'] = proxy.stderr.decode()
                save()
                assert case['proxy_exit'] == 0 and not case['proxy_stderr']
                terminal = case['proxy'][-1]
                assert terminal['event'] == 'terminal' and terminal['unexpected'] == 0
                # Pinned postgres defaults to REPEATABLE READ; a successor
                # update first aborts the old snapshot, then DuckLake retries
                # once and reaches the actual writer guard on a fresh snapshot.
                assert terminal['data_queries'] == (2 if mode == 'owner_first' else 1)
                assert terminal['data_commits'] == int(mode != 'owner_first')
                assert terminal['faults'] == int(mode in ('before', 'after', 'held_rollback'))
                assert terminal['client_complete'] == int(mode == 'held_commit')
                assert terminal['server_complete'] == int(mode in ('after', 'held_commit'))
                assert terminal['guard_rejections'] == int(mode == 'owner_first')
                assert terminal['serialization_rejections'] == int(mode == 'owner_first')
                assert terminal['rejection_sequence'] == (['serialization', 'guard'] if mode == 'owner_first' else [])
            # Observe durable references/bytes before trusting parent or reader
            # outcomes. In particular a failed body read must not hide file loss.
            observe()
            assert case['parent']['exit'] == 0 and not case['parent']['stderr'], 'actual parent failed'
            case['reader'] = {}
            execute(binary + ['/out/native-tests/archive-read-probe', mode], 45, case['reader'])
            save()
            assert case['reader']['exit'] == 0 and not case['reader']['stderr'], 'actual native readback failed'
            check(mode, [json.loads(line) for line in case['parent']['stdout'].splitlines()],
                  json.loads(case['reader']['stdout']), installed=installed)
            assert case['active_writers'] == 0
            if installed:
                home = root / f'out/signed_{mode}'
                case['retained_input_files'] = input_files(home, mode)
                restarted = case['input_restart'] = {}
                rocks = home / 'rocks'
                # Prove this fresh process can inspect exact catalog receipts
                # without opening or reconstructing any pending fact bodies.
                original_mode = rocks.stat().st_mode & 0o777
                rocks.chmod(0)
                try:
                    denied = subprocess.run(['docker','exec',runner,'test','-r',f'/out/signed_{mode}/rocks'], capture_output=True)
                    assert denied.returncode == 1
                    execute(binary + ['/out/native-tests/archive-publication-probe', mode, 'inputs'], 90, restarted)
                finally:
                    rocks.chmod(original_mode)
                    save()
                assert restarted['exit'] == 0 and not restarted['stderr']
                seen = [json.loads(line) for line in restarted['stdout'].splitlines()]
                # Final catalog may now contain a successor receipt with the
                # same semantic digest. Never call it the original attempt's receipt.
                assert seen == restart_expectations(mode)
                assert input_files(home, mode) == case['retained_input_files']
                assert case['public_access'] == dict(database=[],schema=[])
                assert case['installed_initializer']['new_installation'] is True
                assert case['runtime_manager'] == dict(current_user='CatalogManager', session_user='CatalogManager', superuser=False, createrole=True, inherit=False)
                rights = case['roles']
                assert rights['manager_superuser'] is False and rights['native_and_guard_owned'] is True
                assert rights['manager_connect_delegate'] is True and rights['manager_usage_delegate'] is True
                assert len(rights['writers']) == (3 if mode in ('before','after') else 4)
                assert all(w['connect'] and w['usage'] and not w['login_enabled'] and w['password_removed'] and not w['connect_delegate'] and not w['usage_delegate'] for w in rights['writers'])
            file_count = 14 if mode in ('owner_first', 'held_rollback') else 8
            assert len(case['physical_files']) == file_count
            physical = {row['path']: row for row in case['physical_files']}
            assert len(physical) == file_count
            assert len(case['catalog_files']) == (2 if mode == 'before' else 8)
            for file in case['catalog_files']:
                assert file['path_is_relative'] and file['schema_path'] == 'main/'
                assert file['table_path'] == file['table_name'] + '/'
                path = file['schema_path'] + file['table_path'] + file['path']
                assert path in physical and physical[path]['bytes'] == file['file_size_bytes']
                bootstrap_attempt = 'prepared-' + f'{182 + 8 * MODES.index(mode):02x}' * 16
                bootstrap_dictionary = file['table_name'] == 'dictionaries' and file['path'].split('/')[0] == bootstrap_attempt
                assert file['record_count'] == (5 if bootstrap_dictionary else 1)
            # Missing dictionaries and duplicate receipt checks must precede COPY;
            # Exactly the expected attempts exist, including retained old files.
            attempts = {Path(row['path']).parts[-2] for row in case['physical_files']}
            n = 180 + 8 * MODES.index(mode)
            prepared = (n + 2, n + 3, n + 4) if file_count == 14 else (n + 2, n + 3)
            assert attempts == {'prepared-' + f'{a:02x}' * 16 for a in prepared}
            case['status'] = 'PASS'
            save()
            print(json.dumps(dict(mode=mode, status='PASS', catalog_files=len(case['catalog_files']),
                                  physical_files=len(physical))), flush=True)
        except BaseException as error:
            case['status'] = ('INCOMPLETE' if isinstance(error, (Incomplete, subprocess.TimeoutExpired, KeyboardInterrupt))
                              else 'FAIL')
            case['failure'] = str(error) if isinstance(error, (Incomplete, AssertionError)) else type(error).__name__
            result['status'] = case['status']
            raise
        finally:
            if holder:
                holder.stop()
                case['holder_observation'] = proxy_observation(holder)
            if proxy:
                proxy.stop()
                case['proxy_observation'] = proxy_observation(proxy)
            if 'active_writers' not in case:
                # A failed ordering/readback assertion still needs observable
                # files/catalog state; an owned terminal audit must follow.
                try:
                    observe()
                except Exception as error:
                    case['observation_failure'] = type(error).__name__
            save()
    result['status'] = 'PASS'
    save()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    parser.add_argument('--installed', action='store_true')
    args = parser.parse_args()
    main(args.root.resolve(), args.installed)
