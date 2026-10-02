"""Installed retirement of the retained signed source, with independent SQL effects.

Only isolated clones receive synthetic incompatible history. The original signed
native oracle and complete retained PG/catalog/file census remain independent.
"""
import hashlib
import json
from pathlib import Path
import time

import lab
from acquisition_abandonment import assert_abandoned, duplicate_intent
from source_closure_faults import assert_effects


def assert_retired(before, after, digest, *, committed, released):
    changed = {'owner_epochs', 'role_instances', 'producer_epochs', 'allocation_domains'}
    assert before.keys() == after.keys()
    for table in before.keys() - changed:
        assert before[table] == after[table], table
    old_controls = [r for r in before['owner_epochs'] if r['role'] == 'control']
    current, = [r for r in after['owner_epochs'] if r['role'] == 'control' and r not in old_controls]
    assert len(after['owner_epochs']) == len(before['owner_epochs']) + 1
    assert len(after['role_instances']) == len(before['role_instances']) + 1
    assert current['epoch'] == max(r['epoch'] for r in old_controls) + 1
    assert current['state'] == ('released' if released else 'active')
    assert (current['released_at_us'] is not None) == released
    current_role, = [r for r in after['role_instances'] if r['role_instance_id'] == current['role_instance_id']]
    assert current_role['role'] == 'control'
    assert (current_role['retired_at_us'] is not None) == released
    old_ingest, = [r for r in before['owner_epochs'] if r['role'] == 'ingest']
    old_role, = [r for r in before['role_instances'] if r['role_instance_id'] == old_ingest['role_instance_id']]
    assert all(r in after['owner_epochs'] for r in before['owner_epochs'] if r != old_ingest)
    assert all(r in after['role_instances'] for r in before['role_instances'] if r != old_role)
    ingest, = [r for r in after['owner_epochs'] if r['role'] == 'ingest']
    role, = [r for r in after['role_instances'] if r['role_instance_id'] == old_role['role_instance_id']]
    previous, = before['producer_epochs']
    producer, = after['producer_epochs']
    if not committed or previous['state'] == 'retired':
        for table in ('producer_epochs', 'allocation_domains'):
            assert before[table] == after[table], (table, 'retry/refusal changed original rows')
        assert old_ingest == ingest and old_role == role
        return
    assert previous['state'] == 'closing'
    timestamp = producer['retired_at_us']
    end = current['released_at_us'] if released else current['lease_expires_at_us']
    assert current['acquired_at_us'] <= timestamp <= end
    assert before['source_streams'][0]['closed_at_us'] <= timestamp
    expected = dict(previous, state='retired', retired_at_us=timestamp,
        closure_digest='\\x' + digest, xmin=producer['xmin'])
    assert producer == expected
    assert ingest == dict(old_ingest, state='released', released_at_us=timestamp,
        exclusion_method='storage_fenced', exclusion_digest='\\x' + digest, xmin=ingest['xmin'])
    assert role == dict(old_role, retired_at_us=timestamp, xmin=role['xmin'])
    domains = {r['allocation_domain']: r for r in after['allocation_domains']}
    assert len(domains) == len(before['allocation_domains'])
    for old in before['allocation_domains']:
        new = domains[old['allocation_domain']]
        assert old['state'] == 'active'
        assert new == dict(old, state='closed', closed_at_us=timestamp,
            closure_digest='\\x' + digest, xmin=new['xmin'])


def run(pg, client, output, rows, prepare, check_native, cases, digest, initialized):
    def upgrade(database, config, name, target):
        before = rows(database)
        physical = lab.sql(pg, database, "SELECT jsonb_object_agg(relname,pg_relation_filenode(c.oid)) "
            "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
            "WHERE n.nspname='control' AND c.relkind='r'")
        response = lab.cli(dict(pg, client=client), 'migrate', 'apply', '--file', config,
            '--user', 'bigbrotr_migrator', '--password-ref', 'file:/run/secrets/migrator', '--target', str(target))
        lab.write_json(output / (name + '-migration.json'), dict(returncode=response.returncode,
            stdout=response.stdout, stderr=response.stderr))
        assert response.returncode == 0, response.stderr
        after = rows(database)
        assert len(after) == 70
        assert all(after[t] == before[t] for t in before if t != 'schema_versions')
        added = [r for r in after['schema_versions'] if r not in before['schema_versions']]
        names = {27: '0027_acquisition_abandonment.sql', 28: '0028_acquisition_gap_guard.sql',
            29: '0029_initial_retirement.sql'}
        expected = {n for n in names if n <= target and n not in {r['migration'] for r in before['schema_versions']}}
        assert len(added) == len(expected) and {r['migration'] for r in added} == expected
        for row in added:
            sql = Path(__file__).resolve().parents[2] / 'migrations/postgres' / names[row['migration']]
            assert row['checksum'] == '\\x' + hashlib.sha256(sql.read_bytes()).hexdigest()
        assert all(r in after['schema_versions'] for r in before['schema_versions'])
        repeat = lab.cli(dict(pg, client=client), 'migrate', 'apply', '--file', config,
            '--user', 'bigbrotr_migrator', '--password-ref', 'file:/run/secrets/migrator', '--target', str(target))
        assert repeat.returncode == 0 and rows(database) == after
        physical_after = lab.sql(pg, database, "SELECT jsonb_object_agg(relname,pg_relation_filenode(c.oid)) "
            "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
            "WHERE n.nspname='control' AND c.relkind='r'")
        original_files, new_files = json.loads(physical), json.loads(physical_after)
        assert all(new_files[t] == value for t, value in original_files.items())
        lab.write_json(output / (name + '-upgraded.json'), dict(before=before, after=after,
            exact_reapply=True, physical_before=original_files, physical_after=new_files))
        lab.sql(pg, database, 'GRANT SELECT,INSERT ON control.acquisition_gaps TO bigbrotr_control; '
            'GRANT EXECUTE ON FUNCTION control.guard_acquisition_gap(),control.require_acquisition_gap_complete() TO bigbrotr_control;')
        if target == 29:
            lab.sql(pg, database, 'GRANT EXECUTE ON FUNCTION '
                'control.require_initial_retirement(bytea,numeric,bytea) TO bigbrotr_control;')

    def ready(name, *, abandon=True, latest=True):
        database, config, directory = prepare(name)
        upgrade(database, config, name + '-28', 28)
        before = rows(database)
        mode = '--abandon-acquisition' if abandon else '--close-source'
        response = lab.cli(dict(pg, client=client), 'ingest', 'recover', '--file', config, mode)
        lab.write_json(output / (name + '-prepare.json'), dict(returncode=response.returncode,
            stdout=response.stdout, stderr=response.stderr))
        assert response.returncode == 0 and not response.stderr, response
        after = rows(database)
        if abandon:
            assert assert_abandoned(before, after, digest) == 1
        comparison = dict(before, jobs=after['jobs'], acquisition_gaps=after['acquisition_gaps'])
        assert_effects(comparison, after, True, digest, released=True)
        check_native(directory, name + '-prepared')
        if latest:
            upgrade(database, config, name + '-29', 29)
        return database, config, directory, before

    def invoke(database, config, directory, name, *, error=None):
        before = rows(database)
        lab.write_json(output / (name + '-before.json'), before)
        started = time.monotonic()
        response = lab.cli(dict(pg, client=client), 'ingest', 'recover', '--file', config, '--retire-initial')
        elapsed = time.monotonic() - started
        lab.write_json(output / (name + '-response.json'), dict(returncode=response.returncode,
            stdout=response.stdout, stderr=response.stderr, elapsed_seconds=elapsed))
        after = rows(database)
        lab.write_json(output / (name + '-after.json'), after)
        check_native(directory, name)
        if error:
            assert response.returncode == 4 and not response.stdout and error in response.stderr, response
        else:
            assert response.returncode == 0 and not response.stderr, response
            actual, = [json.loads(line) for line in response.stdout.splitlines()]
            assert actual == dict(format='bigbrotr-ingest-recovery/1', state='initial-retired',
                dataset_id=initialized['dataset_id'], owner_resource_id=initialized['owner_resource_id'],
                producer_slot_id=initialized['producer_slot_id'], producer_epoch='1', issued_through='4',
                reserved_through='4', resolved_requests='4', native='closed', owner_release='committed', ready=False,
                source_closure=dict(last_complete_sequence='12', sha256=digest),
                ingest_retirement=dict(allocation_domains=str(len(before['allocation_domains'])), acquisition_gaps='1',
                    owner_release='committed', native_acknowledged='0', native_retired='0'))
        assert_retired(before, after, digest, committed=not error, released=not error)
        assert lab.sql(pg, database, "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control'") == '0'
        cases.append(dict(name=name, status='PASS', control_tables=len(after), native_rows=49,
            original_requests=4, expected_refusal=error, seconds=elapsed))
        lab.write_json(output / 'signed-recovery-cases.json', cases)

    database, config, directory, _ = ready('retire_signed')
    invoke(database, config, directory, 'retire-signed')
    invoke(database, config, directory, 'retire-signed-retry')

    database, config, directory, _ = ready('retire_pending', abandon=False)
    invoke(database, config, directory, 'retire-pending', error='Backend')

    database, config, directory, _ = ready('retire_missing_copy')
    lab.sql(pg, database, 'BEGIN; SET LOCAL session_replication_role=replica; '
        'DELETE FROM control.producer_outcomes WHERE request_sequence=4; COMMIT;')
    invoke(database, config, directory, 'retire-missing-copy', error='Conflict')

    database, config, directory, _ = ready('retire_unbound_event')
    lab.sql(pg, database, 'BEGIN; SET LOCAL session_replication_role=replica; '
        'UPDATE control.producer_requests SET acquisition_job_id=NULL, acquisition_scope_id=NULL, '
        'relay_endpoint_id=NULL, relay_access_scope_id=NULL, relay_enrollment_generation=NULL, relay_route_id=NULL '
        'WHERE request_sequence=4; COMMIT;')
    invoke(database, config, directory, 'retire-unbound-event', error='Conflict')

    database, config, directory, _ = ready('retire_mixed')
    lab.sql(pg, database, "BEGIN; SET LOCAL session_replication_role=replica; "
        "UPDATE control.allocation_domains SET state='closed',closed_at_us=(SELECT closed_at_us FROM control.source_streams),"
        "closure_digest=(SELECT closure_digest FROM control.source_streams) "
        "WHERE dictionary_kind=(SELECT min(dictionary_kind) FROM control.allocation_domains); COMMIT;")
    invoke(database, config, directory, 'retire-mixed', error='InvalidMetadata')

    database, config, directory, _ = ready('retire_old_schema', latest=False)
    invoke(database, config, directory, 'retire-old-schema', error='IncompatibleDeployment')

    database, config, directory, _ = ready('retire_revoked')
    lab.sql(pg, database, 'BEGIN; SET LOCAL session_replication_role=replica; '
        'UPDATE control.local_accounts SET active=false; DELETE FROM control.principal_grants; COMMIT;')
    invoke(database, config, directory, 'retire-revoked')

    database, config, directory, original = ready('retire_no_request_job')
    pending = duplicate_intent(pg, database, original, True)
    before = rows(database)
    assert all(r['acquisition_job_id'] != pending for r in before['producer_requests'])
    assert next(r for r in before['jobs'] if r['job_id'] == pending)['state'] == 'queued'
    invoke(database, config, directory, 'retire-no-request-job', error='Backend')

    database, config, directory, _ = ready('retire_clock_regressed')
    # Model a wall clock now earlier than the already committed source closure.
    # Never alter the host clock or another experiment. Retirement must not
    # commit a timestamp its own completed-retry observation cannot accept.
    lab.sql(pg, database, 'BEGIN; SET LOCAL session_replication_role=replica; '
        'UPDATE control.source_streams SET closed_at_us='
        '(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint+60000000; COMMIT;')
    invoke(database, config, directory, 'retire-clock-regressed', error='InvalidMetadata')
