#!/usr/bin/env python3
"""Real PostgreSQL ownership/ACL counterexamples; isolated exact-source lab only."""
import argparse
import hashlib
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True
SOURCE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SOURCE / 'experiments/control-migrations'))
import lab


def main(root):
    state = json.loads((root / 'resources.json').read_text())
    assert SOURCE == root / 'source' and state['root'] == str(root) and state['state'] == 'running'
    assert lab.command(['git', '-C', str(SOURCE), 'rev-parse', 'HEAD']).stdout.strip() == state['source']
    assert not lab.command(['git', '-C', str(SOURCE), 'status', '--porcelain', '--untracked-files=all']).stdout
    ready = json.loads((root / 'out/native-tests/ready.json').read_text())
    assert ready['source'] == state['source']
    for key, path in [('probe', 'native-tests/catalog-schema-probe'),
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
    # These are fresh lab roles. No credentials or changes to unrelated roles.
    sql('postgres', 'CREATE ROLE "CatalogManager" LOGIN NOSUPERUSER CREATEROLE NOCREATEDB NOINHERIT NOREPLICATION NOBYPASSRLS; CREATE ROLE bb_install_reader NOLOGIN;')
    try:
        for database, leak in [('bbcat_owner', False), ('bbcat_acl', True)]:
            sql('postgres', f'CREATE DATABASE {database};')
            sql(database, 'REVOKE CREATE ON SCHEMA public FROM PUBLIC; GRANT USAGE,CREATE ON SCHEMA public TO "BootstrapMixed";')
            (root / 'out' / ('schema_' + database)).mkdir(mode=0o700)
            initialized = command('/out/native-tests/catalog-schema-probe', database, 'initialize', 'success')
            assert json.loads(initialized.stdout) == dict(success=True, active_permits=0, outcome='Ok(())')
            before = sql(database, "SELECT datdba::text||':'||(SELECT nspowner::text FROM pg_namespace WHERE nspname='public') FROM pg_database WHERE datname=current_database();")
            if leak:
                sql(database, 'ALTER DEFAULT PRIVILEGES FOR ROLE postgres GRANT SELECT,UPDATE ON TABLES TO bb_install_reader;')
            installed = command('/out/native-tests/scientific-guard-probe', 'install', database, 'installed')
            assert json.loads(installed.stdout) == dict(outcome='installed', commit_started=True)
            manager_owns = sql(database, "SELECT nspowner=(SELECT oid FROM pg_roles WHERE rolname='CatalogManager') FROM pg_namespace WHERE nspname='bigbrotr_archive';")
            assert manager_owns == 'f', 'counterexample no longer reproduces: review the changed implementation'
            case = dict(name='initializer-default-acl' if leak else 'configured-manager-ownership',
                        expected_manager_ownership=True, actual_manager_ownership=False,
                        installed_outcome='installed', reproduced=True)
            if leak:
                # An ordinary SELECT-only catalog reader needs this schema USAGE.
                # Table defaults must not silently add guard mutation authority.
                sql(database, 'GRANT USAGE ON SCHEMA bigbrotr_archive TO bb_install_reader;')
                writable = sql(database, "SELECT has_table_privilege('bb_install_reader','bigbrotr_archive.owner','UPDATE');")
                assert writable == 't'
                # Exercise the actual non-owner role, then roll back its mutation.
                changed = sql(database, 'BEGIN; SET LOCAL ROLE bb_install_reader; UPDATE bigbrotr_archive.owner SET ordinal=1,enabled=true RETURNING ordinal::text; ROLLBACK;')
                assert changed == '1', changed
                assert sql(database, 'SELECT ordinal::text||\':\'||enabled::text FROM bigbrotr_archive.owner;') == '0:false'
                inspected = command('/out/native-tests/scientific-guard-probe', 'inspect', database, 'inspected')
                assert json.loads(inspected.stdout) == dict(outcome='inspected', commit_started=False)
                case.update(expected_reader_guard_write=False, actual_reader_guard_write=True,
                            non_owner_mutation_executed=True, mutation_rolled_back=True,
                            existing_inspection='inspected')
            assert sql(database, "SELECT datdba::text||':'||(SELECT nspowner::text FROM pg_namespace WHERE nspname='public') FROM pg_database WHERE datname=current_database();") == before
            case['database_and_public_owner_unchanged'] = True
            report['cases'].append(case); save()
        report['status'] = 'REPRODUCED'
    except BaseException as error:
        report.update(status='INCOMPLETE', failure_class=type(error).__name__)
        raise
    finally:
        # The sole modified default belongs to this newly created lab database.
        try:
            sql('bbcat_acl', 'ALTER DEFAULT PRIVILEGES FOR ROLE postgres REVOKE SELECT,UPDATE ON TABLES FROM bb_install_reader;')
            assert sql('bbcat_acl', "SELECT count(*) FROM pg_default_acl WHERE defaclrole=(SELECT oid FROM pg_roles WHERE rolname='postgres');") == '0'
            report['default_privileges_restored'] = True
        except BaseException as error:
            report.update(status='INCOMPLETE', cleanup_error=type(error).__name__)
        save()
    assert report['status'] == 'REPRODUCED'
    print(json.dumps(dict(status=report['status'], cases=len(report['cases']))), flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    main(parser.parse_args().root.resolve(strict=True))
