#!/usr/bin/env python3
"""Exact CLI migration and raw SQL lineage shapes on two retained-origin copies.

No native store is opened. This does not grant or qualify a successor writer.
The owned client and database copies are retained, including every failed run.
"""
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import sys
import time

sys.dont_write_bytecode = True
SOURCE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SOURCE / 'experiments/control-migrations'))
import lab
sys.path.insert(0, str(SOURCE / 'experiments/network-transport'))
from inventory import projection

ORIGINS = [
    dict(name='activated', kind=1, run='ingest-activated-retirement-engine-9bdc86a',
         database='bb_scope_bind_9bdc86a_installed_activated_retirement_off',
         snapshot='installed-activated-retirement-off-retry-after-sql.json',
         snapshot_sha256='7ac690057de370a00144ffd415bb1f504b05ff4b5599d6458b728820a98acbad',
         result_sha256='48506a152b393854f2f50cbedfedb71b66e53ed9578753d7e6b4df3bd76c8e09',
         status='PASS_INSTALLED_ACTIVATED_RETIREMENT_FIRST_WAVE'),
    dict(name='empty', kind=2, run='ingest-empty-installed-engine-788f971',
         database='bb_scope_bind_788f971_installed_empty_normal',
         snapshot='installed-empty-normal-terminal-after.json',
         snapshot_sha256='5cc9803df1c4f2093f56694ca08857bbdf62066c51f260a35637ce2208aa7579',
         result_sha256='ff1d2e111dc42ff4026c7e42985a7814dbf058eb0bd1d7ba92c5e269e7642d34',
         status='PASS_INSTALLED_EMPTY_RETIREMENT'),
]


def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def read(path):
    return json.loads(path.read_text())


def normalized(rows):
    return {name: sorted(values, key=lambda v: json.dumps(v, sort_keys=True))
            for name, values in rows.items()}


def containers():
    ids = lab.docker('ps', '-aq').stdout.split()
    return json.loads(lab.docker('inspect', *ids).stdout)


# Actual complete rows/xmin, including old unknown-to-the-fixture relations.
ROWS_SQL = """
SET max_parallel_workers_per_gather=0;
SET statement_timeout='15s';
CREATE FUNCTION pg_temp.capture() RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE relation text; part jsonb; answer jsonb:='{}';
BEGIN
    FOR relation IN SELECT tablename FROM pg_tables WHERE schemaname='control' ORDER BY tablename LOOP
        EXECUTE format('SELECT coalesce(jsonb_agg(v ORDER BY v::text COLLATE "C"),''[]'') FROM '
            '(SELECT to_jsonb(t)||jsonb_build_object(''xmin'',xmin::text) v FROM control.%I t) q',relation) INTO part;
        answer:=answer||jsonb_build_object(relation,part);
    END LOOP;
    RETURN answer;
END $$;
SELECT pg_temp.capture();
"""


def rows(pg, database):
    return json.loads(lab.sql(pg, database, ROWS_SQL))


def absence(pg, database):
    assert re.fullmatch(r'[a-z0-9_]+', database)
    return json.loads(lab.sql(pg, 'postgres', "SELECT coalesce(json_agg(row_to_json(t)),'[]') FROM "
        "(SELECT pid,backend_start,usename,state FROM pg_stat_activity WHERE datname='" + database + "')t;"))


def run(root, build):
    assert SOURCE == root / 'source'
    revision = lab.command(['git', '-C', str(SOURCE), 'rev-parse', 'HEAD']).stdout.strip()
    assert not lab.command(['git', '-C', str(SOURCE), 'status', '--porcelain', '--untracked-files=all']).stdout
    built = read(build / 'build-result.json')
    assert digest(build / 'bigbrotr') == built['binary_sha256']
    assert read(build.parent / 'qualification.json')['status'] in (
        'PASS_SUCCESSION_SCHEMA_COMPONENTS', 'PASS_SUCCESSION_REPLAY_SCHEMA_COMPONENTS')
    changes = lab.command(['git', '-C', str(SOURCE), 'diff', '--name-only', built['source'], revision]).stdout.splitlines()
    assert all(name.startswith(('docs/', 'experiments/')) for name in changes), changes
    # Compiled migration bytes and application/catalog inputs must still match.
    manifest = read(build.parent / 'build/source-manifest.json')
    assert all(digest(SOURCE / name) == wanted for name, wanted in manifest['files_sha256'].items()
               if name.startswith(('apps/', 'crates/', 'migrations/')) or name in ('Cargo.toml', 'Cargo.lock', 'rust-toolchain.toml'))
    pg_root = root.parent / 'relay-source-recheck-e06f580/pg'
    pg = lab.load(pg_root)
    assert pg['owner'] == '5e69b0f060b1607c25230d1dbd3e5608' and pg['state'] == 'stopped-retained'
    before = containers()
    borrowed, = [c for c in before if c['Name'].lstrip('/') == pg['postgres']]
    assert borrowed['Id'] == '7177550705c49b14237e20790af411af58f62f22d110cd0dfa1afddaf78861ec'
    assert borrowed['State']['Status'] == 'exited' and borrowed['State']['Pid'] == 0
    assert not any(c['State']['Running'] and 'bigbrotr' in c['Name'] for c in before)
    assert shutil.disk_usage(root).free > 2 * 1024**3
    output = root / 'sql'
    output.mkdir(mode=0o700)
    owner = secrets.token_hex(16)
    label = 'org.bigbrotr.succession-schema'
    client = 'bigbrotr-succession-schema-' + revision[:7]
    assert not any(c['Name'].lstrip('/') == client for c in before)
    report = dict(status='RUNNING', source=revision, build=built, owner=owner, label=label,
        postgres=borrowed['Id'], postgres_owner=pg['owner'], client=None, databases=[],
        started_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
        free_bytes_before=shutil.disk_usage(root).free, origins={}, cases=[],
        scope='CLI migration plus SQL-only lineage shapes; no native or writer authority',
        not_run=['Positive native whole-lineage replay and connected writer',
                 'SQL concurrent wait/expiry/COMMIT-loss schedules', 'Installed repeated succession',
                 '255-epoch PostgreSQL capacity and performance'])
    lab.write_json(output / 'preflight-containers.json', before)
    lab.write_json(output / 'result.json', report)
    originals = {}
    try:
        lab.docker('start', pg['postgres'])
        deadline = time.monotonic() + 30
        while lab.docker('exec', pg['postgres'], 'pg_isready', '-U', 'postgres', check=False).returncode:
            assert time.monotonic() < deadline
            time.sleep(.2)
        assert lab.sql(pg, 'postgres', 'SHOW server_version_num;') == '160015'
        report['database_names_before'] = lab.sql(pg, 'postgres', 'SELECT datname FROM pg_database ORDER BY datname;').splitlines()
        for origin in ORIGINS:
            basis = root.parent / origin['run'] / 'bindings'
            assert digest(basis / 'result.json') == origin['result_sha256']
            assert digest(basis / origin['snapshot']) == origin['snapshot_sha256']
            retained = read(basis / 'result.json')
            assert retained['status'] == origin['status']
            assert not any(retained[k] for k in ['cleanup_errors', 'missing_owned', 'unrelated_changed'])
            assert absence(pg, origin['database']) == []
            saved = rows(pg, origin['database'])
            assert normalized(saved) == normalized(read(basis / origin['snapshot']))
            originals[origin['name']] = dict(rows=saved, catalog=lab.schema_digest(pg, origin['database']))
            report['origins'][origin['name']] = dict(origin, **originals[origin['name']])
        previous_failure = root.parent / 'ingest-succession-sql-45b8462/sql/result.json'
        assert digest(previous_failure) == '37e54317fbb663930f0d244299a865cbc548c7c66f95ffee7d03ab23c52189d9'
        failed = read(previous_failure)
        assert failed['status'] == 'FAIL' and len(failed['databases']) == 1
        failed_database, = failed['databases']
        assert absence(pg, failed_database) == []
        failed_rows = rows(pg, failed_database)
        assert normalized(failed_rows) == normalized(originals['activated']['rows'])
        assert lab.schema_digest(pg, failed_database) == originals['activated']['catalog']
        lab.write_json(output / 'prior-failed-migration-state.json', failed_rows)
        report['prior_failed_migration'] = dict(database=failed_database,
            status='ROLLBACK_OBSERVED_BY_FULL_ROWS_AND_CATALOG', original_result_sha256=digest(previous_failure))
        # Keep the applied, failed full-width fixture copy unchanged. Reading its
        # old function distinguishes quotient rounding from byte-decoder errors.
        width_failure = root.parent / 'ingest-succession-sql-6350901/sql'
        assert digest(width_failure / 'result.json') == '463acb91f75a005018b328e2465dfea9cc337c42178564fe41fcc622828a8138'
        assert digest(width_failure / 'activated-final.json') == 'fb91778d1d4e70f1c6a5653dca9f4c420e4d013d31bd74da33e8d3c2c1e2a108'
        width_database, = read(width_failure / 'result.json')['databases']
        assert absence(pg, width_database) == []
        assert normalized(rows(pg, width_database)) == normalized(read(width_failure / 'activated-final.json'))
        report['prior_unsigned_failure'] = json.loads(lab.sql(pg, width_database, """
            SELECT json_build_object(
                'encoded',encode(control.ingest_lineage_u64(18446744073709551615),'hex'),
                'rounded_quotient',trunc(18446744073709551615::numeric/256)::text,
                'integer_quotient',div(18446744073709551615::numeric,256)::text,
                'decoded',control.ingest_lineage_uint(decode('8000000000000000','hex'),0,8)::text);
        """))
        assert normalized(rows(pg, width_database)) == normalized(read(width_failure / 'activated-final.json'))
        assert absence(pg, width_database) == []
        ident = lab.docker('create', '--init', '--name', client, '--label', label + '=' + owner,
            '--cpus', '2', '--memory', '1536m', '--memory-swap', '1536m', '--pids-limit', '128',
            '--read-only', '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges',
            '--user', f'{os.getuid()}:{os.getgid()}', '--network', pg['network'],
            '--tmpfs', '/tmp:rw,exec,nosuid,nodev,size=128m',
            '--mount', f'type=bind,src={output},dst=/lab',
            '--mount', f'type=bind,src={build},dst=/build,readonly',
            '--mount', f'type=bind,src={pg_root}/secrets,dst=/run/secrets,readonly',
            lab.CLIENT_IMAGE, 'python3', '-c', 'import time; time.sleep(1800)').stdout.strip()
        report['client'] = ident
        lab.write_json(output / 'result.json', report)
        lab.docker('start', ident)
        state = dict(pg, client=client)
        for origin in ORIGINS:
            name = origin['name']
            database = 'bb_succession_' + revision[:7] + '_' + name
            assert database not in report['database_names_before']
            lab.sql(pg, 'postgres', f'CREATE DATABASE {database} OWNER bigbrotr_migrator TEMPLATE {origin["database"]};')
            report['databases'].append(database)
            assert normalized(rows(pg, database)) == normalized(originals[name]['rows'])
            lab.write_json(output / (name + '-copy-before.json'), originals[name]['rows'])
            config = lab.config(output, state, database)
            case = dict(name=name, database=database, migrations=[], status='RUNNING')
            report['cases'].append(case)
            for action in ('plan', 'apply', 'apply'):
                answer = lab.cli(state, 'migrate', action, '--file', config, '--user', 'bigbrotr_migrator',
                    '--password-ref', 'file:/run/secrets/migrator', '--target', '38')
                response = dict(action=action, exit_code=answer.returncode, stdout=answer.stdout, stderr=answer.stderr)
                case['migrations'].append(response)
                lab.write_json(output / 'result.json', report)
                assert answer.returncode == 0 and not answer.stderr, (name, action)
            migrated = rows(pg, database)
            original = originals[name]['rows']
            assert len(migrated) == 75
            for relation, values in original.items():
                if relation == 'schema_versions':
                    assert all(row in migrated[relation] for row in values)
                else:
                    assert normalized({relation: values}) == normalized({relation: migrated[relation]})
            lab.write_json(output / (name + '-migrated.json'), migrated)
            # New relations are ungranted by migration; the explicit set is separate.
            assert lab.sql(pg, database, "SELECT has_table_privilege('bigbrotr_control','control.ingest_successions','SELECT');") == 'f'
            assert lab.sql(pg, database, "SELECT has_table_privilege('bigbrotr_control','control.ingest_terminal_transactions','INSERT');") == 'f'
            # Upgrade the seed31 role's existing0032 observation prerequisite
            # before exercising a legacy observer; these are not0038 grants.
            lab.sql(pg, database, 'GRANT SELECT ON control.initial_ingest_activations TO bigbrotr_control;')
            # Exercise the legacy epoch1 deferred role observer before adding
            # any0038 grants, with a complete rollback and external row check.
            legacy = lab.docker('exec', '-i', pg['postgres'], 'psql', '-X', '-A', '-t', '-q', '--no-password',
                '-U', 'bigbrotr_control', '-d', database, '-v', 'ON_ERROR_STOP=1', data="""
                BEGIN;
                UPDATE control.role_instances SET created_at_us=created_at_us WHERE role_instance_id IN
                    (SELECT role_instance_id FROM control.owner_epochs WHERE role='ingest' AND epoch=1);
                SET CONSTRAINTS ALL IMMEDIATE;
                ROLLBACK;
                SELECT 'LEGACY_ACL_ROLLBACK';
                """, timeout=15, check=False)
            case['legacy_grants'] = dict(exit_code=legacy.returncode, stdout=legacy.stdout, stderr=legacy.stderr)
            assert legacy.returncode == 0 and legacy.stdout.strip() == 'LEGACY_ACL_ROLLBACK' and not legacy.stderr
            assert normalized(rows(pg, database)) == normalized(migrated)
            lab.sql(pg, database, (SOURCE / 'experiments/ingest-succession/schema_grants.sql').read_text())
            assert lab.sql(pg, database, "SELECT has_table_privilege('bigbrotr_control','control.ingest_terminal_transactions','INSERT');") == 't'
            # Deliberately broader negative-case grants only on this disposable DB.
            negative_grants = ('GRANT DELETE ON control.role_instances TO bigbrotr_control; '
                'GRANT TRUNCATE ON control.allocation_domains,control.ingest_successions,'
                'control.ingest_activations,control.ingest_terminal_transactions TO bigbrotr_control;')
            lab.sql(pg, database, negative_grants)
            resource, = [x['owner_resource_id'][2:] for x in migrated['initial_ingest_successions']]
            answer = lab.docker('exec', '-i', pg['postgres'], 'psql', '-X', '-A', '-t', '-q', '--no-password',
                '-U', 'bigbrotr_control', '-d', database, '-v', 'ON_ERROR_STOP=1',
                '-v', 'resource_hex=' + resource, '-v', 'expected_genesis_kind=' + str(origin['kind']),
                data=(SOURCE / 'experiments/control-migrations/repeatable_succession_cases.sql').read_text(),
                timeout=180, check=False)
            (output / (name + '.stdout')).write_text(answer.stdout)
            (output / (name + '.stderr')).write_text(answer.stderr)
            case['fixture_exit'] = answer.returncode
            captured = rows(pg, database)
            lab.write_json(output / (name + '-external.json'), captured)
            case['backend_absence'] = absence(pg, database)
            lab.write_json(output / 'result.json', report)
            assert answer.returncode == 0 and case['backend_absence'] == [], name
            summaries = [json.loads(line) for line in answer.stdout.splitlines() if line.startswith('{')]
            summary, = summaries
            assert summary['kind'] == 'SQL_ONLY_NO_NATIVE_PROOF'
            assert normalized(summary['snapshots']['released']) == normalized(captured)
            assert len(summary['cases']) == 31 and len({c['name'] for c in summary['cases']}) == 31
            case.update(status='PASS_SQL_SHAPE', declared_cases=len(summary['cases']))
            lab.write_json(output / (name + '-summary.json'), summary)
        report['status'] = 'PASS_SQL_SHAPES_ONLY'
    except BaseException as error:
        report.update(status='FAIL', error=repr(error))
        raise
    finally:
        errors = []
        # Preserve clone state even if migration or the fixture refused early.
        # A failed CLI response alone does not establish transaction rollback.
        for case in report['cases']:
            try:
                lab.write_json(output / (case['name'] + '-final.json'), rows(pg, case['database']))
                case['final_backend_absence'] = absence(pg, case['database'])
                assert case['final_backend_absence'] == []
            except BaseException as error:
                errors.append(dict(copy=case['database'], error=repr(error)))
        for origin in ORIGINS:
            if origin['name'] not in originals:
                continue
            try:
                original = originals[origin['name']]
                assert normalized(rows(pg, origin['database'])) == normalized(original['rows'])
                assert lab.schema_digest(pg, origin['database']) == original['catalog']
                assert absence(pg, origin['database']) == []
                report['origins'][origin['name']]['preserved'] = True
            except BaseException as error:
                errors.append(dict(origin=origin['name'], error=repr(error)))
        try:
            names = lab.sql(pg, 'postgres', 'SELECT datname FROM pg_database ORDER BY datname;').splitlines()
            assert set(names) == set(report['database_names_before']) | set(report['databases'])
            report['database_names_after'] = names
        except BaseException as error:
            errors.append(dict(database_inventory=repr(error)))
        for ident, expected_label, expected_owner in [(report['client'], label, owner),
                (borrowed['Id'], lab.LABEL, pg['owner'])]:
            if ident is None:
                continue
            try:
                item, = json.loads(lab.docker('inspect', ident).stdout)
                assert item['Config']['Labels'][expected_label] == expected_owner
                if item['State']['Running']:
                    lab.docker('stop', '--time', '10', ident)
                item, = json.loads(lab.docker('inspect', ident).stdout)
                assert not item['State']['Running'] and not item['State']['Pid'] and not item['State']['OOMKilled']
                lab.write_json(output / ('postgres-terminal.json' if ident == borrowed['Id'] else 'client-terminal.json'), projection(item))
            except BaseException as error:
                errors.append(dict(container=ident, error=repr(error)))
        after = {c['Id']: projection(c) for c in containers()}
        logs = lab.docker('logs', '--since', report['started_at'], borrowed['Id'])
        (output / 'postgres.log').write_text(logs.stdout + logs.stderr)
        report['unrelated_changed'] = [c['Id'] for c in before if c['Id'] != borrowed['Id'] and after.get(c['Id']) != projection(c)]
        report['unexpected_containers'] = sorted(set(after) - {c['Id'] for c in before} - {report['client']})
        report.update(cleanup_errors=errors, finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
            free_bytes_after=shutil.disk_usage(root).free)
        if errors or report['unrelated_changed'] or report['unexpected_containers']:
            report['status'] = 'INCOMPLETE_CUSTODY'
        lab.write_json(output / 'result.json', report)
    assert report['status'] == 'PASS_SQL_SHAPES_ONLY'
    print(json.dumps(dict(status=report['status'], cases=[c['name'] for c in report['cases']], root=str(root))))


if __name__ == '__main__':
    assert len(sys.argv) == 3
    run(Path(sys.argv[1]).resolve(strict=True), Path(sys.argv[2]).resolve(strict=True))
