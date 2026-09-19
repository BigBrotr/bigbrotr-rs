#!/usr/bin/env python3
"""Real PostgreSQL atomic catalog ownership/ACL qualification; isolated exact-source lab only."""
import argparse
import hashlib
import json
from pathlib import Path
import sys
import time

sys.dont_write_bytecode = True
SOURCE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SOURCE / 'experiments/control-migrations'))
import lab
from fault_cases import Process


def main(root):
    state = json.loads((root / 'resources.json').read_text())
    assert SOURCE == root / 'source' and state['root'] == str(root) and state['state'] == 'running'
    assert lab.command(['git', '-C', str(SOURCE), 'rev-parse', 'HEAD']).stdout.strip() == state['source']
    assert not lab.command(['git', '-C', str(SOURCE), 'status', '--porcelain', '--untracked-files=all']).stdout
    ready = json.loads((root / 'out/native-tests/ready.json').read_text())
    assert ready['source'] == state['source']
    for key, path in [('installer', 'native-tests/catalog-install-probe'), ('probe', 'native-tests/catalog-schema-probe'),
                      ('guard', 'native-tests/scientific-guard-probe'),
                      ('worker', 'archive-worker/bigbrotr-archive-worker')]:
        assert hashlib.sha256((root / 'out' / path).read_bytes()).hexdigest() == ready[key]
    for key in ('container_id', 'postgres_id'):
        info = json.loads(lab.docker('inspect', state[key]).stdout)[0]
        assert info['Config']['Labels']['org.bigbrotr.publication'] == state['owner']
        assert info['State']['Running'] and not info['HostConfig']['PortBindings']
    network = json.loads(lab.docker('network', 'inspect', state['network_id']).stdout)[0]
    assert network['Internal'] and network['Labels']['org.bigbrotr.publication'] == state['owner']
    report = dict(source=state['source'], artifacts=ready, status='INCOMPLETE', cases=[])
    output = root / 'catalog-install.json'
    assert not output.exists()
    def save(): lab.write_json(output, report)
    def sql(db, text): return lab.sql(state, db, text)
    def command(*args): return lab.docker('exec', state['container_id'], *args)
    save()

    work = root / 'out/catalog-install'
    work.mkdir(mode=0o700)
    manager_password = (root / 'secrets/CatalogManager').read_text()
    bootstrap_password = (root / 'secrets/BootstrapMixed').read_text()
    assert len(manager_password) == 64 and all(c in '0123456789abcdef' for c in manager_password + bootstrap_password)
    sql('postgres', 'CREATE ROLE "CatalogManager" LOGIN NOSUPERUSER CREATEROLE NOCREATEDB NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD \'' + manager_password + '\'; CREATE ROLE bb_install_reader NOLOGIN;')
    del manager_password
    bootstrap_oid = int(sql('postgres', "SELECT oid FROM pg_roles WHERE rolname='BootstrapMixed';"))
    manager_oid = int(sql('postgres', "SELECT oid FROM pg_roles WHERE rolname='CatalogManager';"))

    def dump(database, data=False):
        args = ['exec', state['postgres_id'], 'pg_dump', '-U', 'postgres', '-d', database]
        if data: args += ['--data-only', '--schema=public']
        result = lab.docker(*args).stdout
        return '\n'.join(line for line in result.splitlines() if not line.startswith(('\\restrict ', '\\unrestrict ')))

    def role_state():
        return sql('postgres', "SELECT md5(json_build_object('roles',(SELECT json_agg((to_jsonb(r)-'rolpassword')||jsonb_build_object('credential',md5(rolpassword),'xmin',xmin::text) ORDER BY oid) FROM pg_authid r),'members',(SELECT json_agg(to_jsonb(m)||jsonb_build_object('xmin',xmin::text) ORDER BY roleid,member,grantor) FROM pg_auth_members m),'settings',(SELECT json_agg(ROW(setdatabase,setrole,md5(setconfig::text),xmin::text) ORDER BY setdatabase,setrole) FROM pg_db_role_setting))::text);")

    def fresh(suffix):
        database = 'bbcat_' + suffix
        sql('postgres', f'CREATE DATABASE {database};')
        # Laboratory-only fixed bootstrap fixture reused across separate new DBs;
        # this is not the production reservation/sole-holder credential protocol.
        sql('postgres', 'ALTER ROLE "BootstrapMixed" LOGIN PASSWORD \'' + bootstrap_password + '\';')
        sql(database, 'REVOKE CREATE ON SCHEMA public FROM PUBLIC; GRANT USAGE,CREATE ON SCHEMA public TO "BootstrapMixed";')
        (root / 'out' / ('schema_' + database)).mkdir(mode=0o700)
        initialized = command('/out/native-tests/catalog-schema-probe', database, 'initialize', 'success')
        assert json.loads(initialized.stdout) == dict(success=True, active_permits=0, outcome='Ok(())')
        sql('postgres', 'ALTER ROLE "BootstrapMixed" NOLOGIN PASSWORD NULL;')
        assert sql(database, "SELECT count(*) FROM pg_stat_activity WHERE usename='BootstrapMixed';") == '0'
        return database

    def invoke(name, database, expected, mode='install', **overrides):
        fixture = dict(database=database, mode=mode, expected=expected, bootstrap_oid=bootstrap_oid, manager_oid=manager_oid)
        fixture.update(overrides)
        path = work / (name + '.json'); path.write_text(json.dumps(fixture)+'\n')
        before, roles = dump(database), role_state()
        case = dict(name=name, status='INCOMPLETE', expected=expected, mode=mode)
        report['cases'].append(case); save()
        value = lab.docker('exec',state['container_id'],'/out/native-tests/catalog-install-probe','/out/catalog-install/'+path.name,check=False)
        case.update(exit_code=value.returncode,stdout_bytes=len(value.stdout.encode()),stderr_bytes=len(value.stderr.encode()),
                    stdout_sha256=hashlib.sha256(value.stdout.encode()).hexdigest(),stderr_sha256=hashlib.sha256(value.stderr.encode()).hexdigest()); save()
        assert len(value.stdout.encode())<=4096
        observed = json.loads(value.stdout)
        assert isinstance(observed,dict) and set(observed)=={'outcome','commit_started','session_superuser','active_permits'}
        assert observed['outcome'] in {'installed','existing','inspected','InvalidInput','Backend','IncompatibleEngine','IncompatibleLayout','WrongCatalog','UnsafeRole','ExistingData','Conflict','CommitUnknown','ResourceLimit','Entropy'}
        after, roles_after = dump(database), role_state()
        case.update(observed=observed, catalog_before_sha256=hashlib.sha256(before.encode()).hexdigest(),
                    catalog_after_sha256=hashlib.sha256(after.encode()).hexdigest(),
                    roles_before_fingerprint=roles, roles_after_fingerprint=roles_after); save()
        assert value.returncode==0 and not value.stderr
        assert observed == dict(outcome=expected, commit_started=expected in ('installed','CommitUnknown'), session_superuser=mode not in ('inspect-manager','install-manager'), active_permits=0)
        assert roles_after == roles
        if expected not in ('installed', 'CommitUnknown'):
            assert after == before
            case['catalog_unchanged'] = True
        until=time.monotonic()+5
        while True:
            remaining=int(sql(database, "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid();"))
            case['last_remaining_clients']=remaining; save()
            if remaining==0: break
            assert time.monotonic()<until
            time.sleep(.02)
        case.update(status='PASS', roles_unchanged=True, remaining_clients=0); save()
        return observed

    children = []
    try:
        good = fresh('owner_good')
        (work/'before.sql').write_text(dump(good)+'\n')
        native_before = dump(good, True)
        admin_before = sql(good, "SELECT datdba::text||':'||(SELECT nspowner::text FROM pg_namespace WHERE nspname='public') FROM pg_database WHERE datname=current_database();")
        invoke('manager-install', good, 'installed')
        assert dump(good, True) == native_before
        assert sql(good, "SELECT count(*)=32 AND bool_and(c.relowner=(SELECT oid FROM pg_roles WHERE rolname='CatalogManager')) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname IN ('public','bigbrotr_archive') AND c.relkind='r';") == 't'
        assert sql(good, "SELECT count(*)=2 AND bool_and(p.proowner=(SELECT oid FROM pg_roles WHERE rolname='CatalogManager')) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='bigbrotr_archive';") == 't'
        assert sql(good, "SELECT datdba::text||':'||(SELECT nspowner::text FROM pg_namespace WHERE nspname='public') FROM pg_database WHERE datname=current_database();") == admin_before
        report['ownership'] = dict(native_tables=28, guard_tables=4, functions=2, manager_superuser=False, database_and_public_owner_unchanged=True, native_data_unchanged=True)
        invoke('manager-inspection', good, 'inspected', 'inspect-manager')
        invoke('existing-manager', good, 'existing')
        invoke('runtime-manager-cannot-initialize', good, 'UnsafeRole', 'install-manager')
        sql(good, 'GRANT USAGE ON SCHEMA public,bigbrotr_archive TO bb_install_reader; GRANT SELECT ON ALL TABLES IN SCHEMA public TO bb_install_reader; GRANT SELECT ON bigbrotr_archive.installation,bigbrotr_archive.scientific_schema TO bb_install_reader;')
        invoke('named-reader-grants', good, 'inspected', 'inspect-manager')
        invoke('existing-with-reader', good, 'existing')
        (work/'after.sql').write_text(dump(good)+'\n')

        # Exact OID mismatch and unsafe role state refuse before any DDL.
        candidate = fresh('roles')
        invoke('wrong-manager-oid', candidate, 'UnsafeRole', manager_oid=manager_oid+1)
        invoke('wrong-bootstrap-oid', candidate, 'UnsafeRole', bootstrap_oid=bootstrap_oid+1)
        sql('postgres', 'ALTER ROLE "BootstrapMixed" LOGIN;')
        try: invoke('bootstrap-not-retired', candidate, 'UnsafeRole')
        finally: sql('postgres', 'ALTER ROLE "BootstrapMixed" NOLOGIN;')
        sql('postgres', 'ALTER ROLE "CatalogManager" SUPERUSER;')
        try: invoke('superuser-runtime-manager', candidate, 'UnsafeRole')
        finally: sql('postgres', 'ALTER ROLE "CatalogManager" NOSUPERUSER;')
        invoke('roles-after-refusals', candidate, 'installed')

        wrong = fresh('wrong_owner')
        installed = command('/out/native-tests/scientific-guard-probe', 'install', wrong, 'installed')
        assert json.loads(installed.stdout)['outcome'] == 'installed'
        invoke('existing-wrong-owner', wrong, 'UnsafeRole')

        for label, creation, undo in [
            ('table', 'GRANT SELECT,UPDATE ON TABLES TO bb_install_reader', 'REVOKE SELECT,UPDATE ON TABLES FROM bb_install_reader'),
            ('function', 'GRANT EXECUTE ON FUNCTIONS TO bb_install_reader', 'REVOKE EXECUTE ON FUNCTIONS FROM bb_install_reader'),
            ('schema', 'GRANT USAGE ON SCHEMAS TO bb_install_reader', 'REVOKE USAGE ON SCHEMAS FROM bb_install_reader'),
            ('public_table', 'GRANT UPDATE ON TABLES TO PUBLIC', 'REVOKE UPDATE ON TABLES FROM PUBLIC'),
            ('public_schema', 'GRANT CREATE ON SCHEMAS TO PUBLIC', 'REVOKE CREATE ON SCHEMAS FROM PUBLIC'),
            ('owner_select', 'REVOKE SELECT ON TABLES FROM postgres', 'GRANT SELECT ON TABLES TO postgres'),
        ]:
            database = fresh('default_' + label)
            sql(database, 'ALTER DEFAULT PRIVILEGES FOR ROLE postgres '+creation+';')
            invoke('default-'+label, database, 'UnsafeRole')
            assert sql(database, "SELECT count(*) FROM pg_namespace WHERE nspname='bigbrotr_archive';") == '0'
            sql(database, 'ALTER DEFAULT PRIVILEGES FOR ROLE postgres '+undo+';')
            invoke('default-'+label+'-known-rollback-retry', database, 'installed')

        for label, grant, revoke in [
            ('native_table', 'GRANT SELECT ON public.ducklake_metadata TO bb_install_reader', 'REVOKE SELECT ON public.ducklake_metadata FROM bb_install_reader'),
            ('native_column', 'GRANT SELECT(value) ON public.ducklake_metadata TO bb_install_reader', 'REVOKE SELECT(value) ON public.ducklake_metadata FROM bb_install_reader'),
        ]:
            database = fresh(label)
            sql(database, grant+';')
            invoke(label, database, 'UnsafeRole')
            sql(database, revoke+';')
            invoke(label+'-known-rollback-retry', database, 'installed')

        # Preserve the injected incompatible state on refusal. These are explicit
        # lab mutations, removed only by the fixture after its independent check.
        for label, grant, revoke, mode in [
            ('guard_table_write', 'GRANT UPDATE ON bigbrotr_archive.owner TO bb_install_reader', 'REVOKE UPDATE ON bigbrotr_archive.owner FROM bb_install_reader', 'inspect-manager'),
            ('guard_column_write', 'GRANT UPDATE(enabled) ON bigbrotr_archive.owner TO bb_install_reader', 'REVOKE UPDATE(enabled) ON bigbrotr_archive.owner FROM bb_install_reader', 'inspect-manager'),
            ('binding_grant_option', 'GRANT SELECT ON bigbrotr_archive.installation TO bb_install_reader WITH GRANT OPTION', 'REVOKE GRANT OPTION FOR SELECT ON bigbrotr_archive.installation FROM bb_install_reader', 'inspect-manager'),
            ('native_grant_option', 'GRANT SELECT ON public.ducklake_metadata TO bb_install_reader WITH GRANT OPTION', 'REVOKE GRANT OPTION FOR SELECT ON public.ducklake_metadata FROM bb_install_reader', 'inspect-manager'),
            ('native_reference', 'GRANT REFERENCES ON public.ducklake_metadata TO bb_install_reader', 'REVOKE REFERENCES ON public.ducklake_metadata FROM bb_install_reader', 'inspect-manager'),
            ('native_column_reference', 'GRANT REFERENCES(value) ON public.ducklake_metadata TO bb_install_reader', 'REVOKE REFERENCES(value) ON public.ducklake_metadata FROM bb_install_reader', 'inspect-manager'),
            ('public_function', 'GRANT EXECUTE ON FUNCTION bigbrotr_archive.require_writer() TO PUBLIC', 'REVOKE EXECUTE ON FUNCTION bigbrotr_archive.require_writer() FROM PUBLIC', 'inspect-manager'),
            ('public_binding', 'GRANT SELECT ON bigbrotr_archive.installation TO PUBLIC', 'REVOKE SELECT ON bigbrotr_archive.installation FROM PUBLIC', 'inspect-manager'),
            ('public_guard_usage', 'GRANT USAGE ON SCHEMA bigbrotr_archive TO PUBLIC', 'REVOKE USAGE ON SCHEMA bigbrotr_archive FROM PUBLIC', 'inspect-manager'),
            ('public_native', 'GRANT SELECT ON public.ducklake_metadata TO PUBLIC', 'REVOKE SELECT ON public.ducklake_metadata FROM PUBLIC', 'inspect-manager'),
            ('public_usage_delegation', 'GRANT USAGE ON SCHEMA public TO bb_install_reader WITH GRANT OPTION', 'REVOKE GRANT OPTION FOR USAGE ON SCHEMA public FROM bb_install_reader', 'inspect-manager'),
            ('guard_create', 'GRANT CREATE ON SCHEMA bigbrotr_archive TO bb_install_reader', 'REVOKE CREATE ON SCHEMA bigbrotr_archive FROM bb_install_reader', 'inspect-manager'),
            ('public_create', 'GRANT CREATE ON SCHEMA public TO bb_install_reader', 'REVOKE CREATE ON SCHEMA public FROM bb_install_reader', 'inspect-manager'),
            ('owner_missing_update', 'REVOKE UPDATE ON bigbrotr_archive.owner FROM "CatalogManager"', 'GRANT UPDATE ON bigbrotr_archive.owner TO "CatalogManager"', 'install'),
            ('member_inherit', 'GRANT "CatalogManager" TO bb_install_reader WITH INHERIT TRUE, SET FALSE, ADMIN FALSE', 'REVOKE "CatalogManager" FROM bb_install_reader', 'inspect-manager'),
            ('member_set', 'GRANT "CatalogManager" TO bb_install_reader WITH INHERIT FALSE, SET TRUE, ADMIN FALSE', 'REVOKE "CatalogManager" FROM bb_install_reader', 'inspect-manager'),
            ('member_admin', 'GRANT "CatalogManager" TO bb_install_reader WITH INHERIT FALSE, SET FALSE, ADMIN TRUE', 'REVOKE "CatalogManager" FROM bb_install_reader', 'inspect-manager'),
        ]:
            sql(good, grant+';')
            try: invoke(label, good, 'UnsafeRole', mode)
            finally: sql(good, revoke+';')
        invoke('manager-after-acl-refusals', good, 'inspected', 'inspect-manager')

        failure = fresh('atomic_failure')
        sql(failure, "CREATE SCHEMA lab_fault; CREATE FUNCTION lab_fault.fail() RETURNS event_trigger LANGUAGE plpgsql AS $$ BEGIN IF tg_tag='ALTER FUNCTION' THEN RAISE EXCEPTION 'owned fixture interruption'; END IF; END $$; CREATE EVENT TRIGGER lab_install_failure ON ddl_command_start EXECUTE FUNCTION lab_fault.fail();")
        invoke('failure-after-table-ownership-transfer', failure, 'Backend')
        assert sql(failure, "SELECT count(*) FROM pg_namespace WHERE nspname='bigbrotr_archive';") == '0'
        assert sql(failure, "SELECT bool_and(c.relowner=(SELECT oid FROM pg_roles WHERE rolname='BootstrapMixed')) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public';") == 't'
        sql(failure, 'DROP EVENT TRIGGER lab_install_failure; DROP SCHEMA lab_fault CASCADE;')
        invoke('atomic-failure-known-rollback-retry', failure, 'installed')
        for mode, counters in [('before-forward', [1,0,0,0,1]), ('after-complete', [1,1,1,0,1])]:
            database = fresh('lost_' + ('before' if mode=='before-forward' else 'after'))
            before = dump(database)
            proxy = Process(['docker','exec','-i',state['container_id'],'python3','-B',
                '/source/experiments/control-migrations/fault_proxy.py','--upstream-port','5432',
                '--mode',mode,'--deadline-seconds','60'], input_pipe=True)
            children.append(proxy)
            ready_proxy = proxy.event('ready',10)
            assert ready_proxy['proxy_sha256'] == hashlib.sha256((SOURCE/'experiments/control-migrations/fault_proxy.py').read_bytes()).hexdigest()
            invoke('lost-'+mode, database, 'CommitUnknown', port=ready_proxy['listen_port'])
            receipt = proxy.event('receipt',10)
            assert proxy.wait(5)==0 and not proxy.stderr
            assert [receipt[k] for k in ['commits_seen','commits_forwarded','server_commit_complete','client_commit_complete','faults']] == counters
            report['cases'][-1]['wire'] = receipt; save()
            assert sql(database, "SELECT to_regnamespace('bigbrotr_archive') IS NOT NULL;") == ('f' if mode=='before-forward' else 't')
            if mode=='before-forward': assert dump(database)==before
            invoke('lost-'+mode+'-fresh-resolution', database, 'installed' if mode=='before-forward' else 'existing')
            invoke('lost-'+mode+'-manager', database, 'inspected', 'inspect-manager')

        # Both actual installers must be waiting on the same real advisory lock
        # before release. Which process wins is deliberately not an oracle.
        database = fresh('concurrent')
        native_before = dump(database, True)
        holder = Process(['docker','exec','-i',state['postgres_id'],'psql','-XAtq',
            '-v','ON_ERROR_STOP=1','-U','postgres','-d',database],input_pipe=True)
        children.append(holder)
        holder.process.stdin.write(b"SET application_name='bigbrotr-install-lock-holder'; BEGIN; SELECT pg_advisory_xact_lock(1111651442,1128354865); SELECT 'holder-ready';\n")
        holder.process.stdin.flush()
        until=time.monotonic()+5
        while b'holder-ready\n' not in holder.stdout:
            assert holder.process.poll() is None and not holder.overflow and time.monotonic()<until
            time.sleep(.02)
        peers=[]
        for i in range(2):
            path=work/('concurrent-'+str(i)+'.json')
            path.write_text(json.dumps(dict(database=database,mode='install',expected='installed-or-existing',bootstrap_oid=bootstrap_oid,manager_oid=manager_oid))+'\n')
            peer=Process(['docker','exec',state['container_id'],'/out/native-tests/catalog-install-probe','/out/catalog-install/'+path.name])
            children.append(peer); peers.append(peer)
        until=time.monotonic()+4
        while True:
            waiters=json.loads(sql(database,"SELECT COALESCE(json_agg(json_build_object('pid',a.pid,'blocking_pids',pg_blocking_pids(a.pid),'classid',l.classid,'objid',l.objid) ORDER BY a.pid),'[]'::json) FROM pg_stat_activity a JOIN pg_locks l ON l.pid=a.pid WHERE a.datname=current_database() AND a.application_name='bigbrotr-catalog-recovery-probe' AND a.wait_event='advisory' AND l.locktype='advisory' AND NOT l.granted AND l.classid=1111651442 AND l.objid=1128354865;"))
            if len(waiters)==2: break
            assert time.monotonic()<until
            time.sleep(.02)
        holder_pid=int(sql(database,"SELECT pid FROM pg_stat_activity WHERE datname=current_database() AND application_name='bigbrotr-install-lock-holder';"))
        assert all(w['blocking_pids'] for w in waiters) and any(holder_pid in w['blocking_pids'] for w in waiters)
        holder.process.stdin.write(b'ROLLBACK;\n'); holder.process.stdin.close()
        assert holder.wait(5)==0 and not holder.stderr
        observations=[]
        for peer in peers:
            assert peer.wait(30)==0 and not peer.stderr
            value=json.loads(peer.stdout); observations.append(value)
            assert value['active_permits']==0 and value['session_superuser'] is True
            assert value['commit_started']==(value['outcome']=='installed')
        assert sorted(v['outcome'] for v in observations)==['existing','installed']
        assert dump(database,True)==native_before
        report['cases'].append(dict(name='concurrent-installers',status='PASS',both_waiters_observed=True,advisory_waiters=waiters,holder_pid=holder_pid,observed=observations,native_data_unchanged=True));save()
        invoke('concurrent-manager-inspection',database,'inspected','inspect-manager')
        report['status'] = 'PASS'
    except BaseException as error:
        report.update(status='FAIL', failure_class=type(error).__name__)
        raise
    finally:
        cleanup_errors=[]
        for child in children:
            try:
                was_running=child.process.poll() is None
                child.stop()
                if was_running or child.overflow or any(t.is_alive() for t in child.threads):
                    cleanup_errors.append('process-terminal-unresolved')
            except BaseException as error:
                cleanup_errors.append(type(error).__name__)
        if cleanup_errors:
            report.update(status='INCOMPLETE',cleanup_errors=cleanup_errors,requires_runner_stop=True)
        else: report['cleanup_errors']=[]
        save()
    assert report['status']=='PASS'
    print(json.dumps(dict(status=report['status'], cases=len(report['cases']))), flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    main(parser.parse_args().root.resolve(strict=True))
