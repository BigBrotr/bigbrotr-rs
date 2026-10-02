"""Actual signed-store recovery and explicit whole-intent acquisition gaps.

Additional zero-request/advanced jobs are declared synthetic relational inputs,
not another qualification of HTTP acceptance. All product guards remain enabled
through migration and every recovery invocation.
"""
import hashlib
import json
from pathlib import Path
import time

import lab
from source_closure_faults import assert_effects

REASON = 'initial_producer_abandoned'


def assert_abandoned(before, after, digest):
    owner, = [r for r in after['owner_epochs'] if r not in before['owner_epochs']]
    slot, = before['producer_slots']
    old_tuple = (slot['registered_by_resource_id'], slot['registered_by_epoch'], slot['registered_by_incarnation'])
    current = (owner['resource_id'], owner['epoch'], owner['process_incarnation'])
    candidates = [r for r in before['jobs'] if r['kind'] == 'acquisition' and (
        r['owner_resource_id'], r['owner_epoch'], r['owner_incarnation']) == old_tuple]
    gaps = {r['job_id']: r for r in after['acquisition_gaps']}
    previous = {r['job_id']: r for r in before['acquisition_gaps']}
    assert len(gaps) == len(previous) + len(candidates)
    assert all(gaps[k] == v for k, v in previous.items()), 'gap retry changed retained row/xmin'
    source, = after['source_streams']
    for row in candidates:
        gap = gaps[row['job_id']]
        wanted = dict(job_id=row['job_id'], dataset_id=row['dataset_id'],
            producer_slot_id=slot['producer_slot_id'], producer_epoch=1,
            shard_id=source['shard_id'], source_owner_epoch=1, last_complete_sequence=12,
            source_closure_digest='\\x' + digest, previous_job_revision=row['job_revision'],
            previous_owner_resource=old_tuple[0], previous_owner_epoch=old_tuple[1],
            previous_owner_incarnation=old_tuple[2], issuer_resource=current[0],
            issuer_epoch=current[1], issuer_incarnation=current[2], owner_role='control',
            abandoned_at_us=gap['abandoned_at_us'], reason=REASON, xmin=gap['xmin'])
        assert gap == wanted
        end = owner['released_at_us'] if owner['released_at_us'] is not None else owner['lease_expires_at_us']
        assert max(source['closed_at_us'], owner['acquired_at_us']) <= gap['abandoned_at_us'] <= end
    jobs = {r['job_id']: r for r in after['jobs']}
    assert len(jobs) == len(before['jobs'])
    for row in before['jobs']:
        expected = dict(row)
        if row in candidates:
            expected.update(state='cancelled', job_revision=row['job_revision'] + 2,
                reason_code=REASON, completed_at_us=gaps[row['job_id']]['abandoned_at_us'],
                owner_resource_id=current[0], owner_epoch=current[1], owner_incarnation=current[2],
                xmin=jobs[row['job_id']]['xmin'])
        assert jobs[row['job_id']] == expected, row['job_id']
    return len(gaps)


def duplicate_intent(pg, database, state, claimed, *, serial=0):
    """Independent complete typed command/job copy, with no producer request."""
    original, = [r for r in state['jobs'] if r['kind'] == 'acquisition']
    detail, = state['acquisition_jobs']
    command, = [r for r in state['management_commands']
        if r['actor_id'] == detail['actor_id'] and r['request_key'] == detail['request_key']]
    audit, = [r for r in state['management_audit'] if r['audit_id'] == command['audit_id']]
    assert type(serial) is int and 0 <= serial < 65 and (claimed or serial == 0)
    ordinal = max(r['admission_ordinal'] for r in state['job_admissions']) + (1 if claimed else 2) + serial
    job, key, audit_id = ['\\x' + (int(byte * 16, 16) + serial).to_bytes(16, 'big').hex()
        for byte in (('d1','d2','d3') if claimed else ('e1','e2','e3'))]
    b = lambda value: bytes.fromhex(value[2:])
    preimage = b'bigbrotr/acquisition-job/1\0' + b(detail['dataset_id']) + b(detail['actor_id'])
    preimage += b(key) + b(detail['scope_id']) + detail['duration_us'].to_bytes(8, 'big')
    preimage += command['expected_config_revision'].to_bytes(8, 'big') + command['policy_revision'].to_bytes(8, 'big')
    digest = '\\x' + hashlib.sha256(preimage).hexdigest()
    job_row = dict(original, job_id=job, request_key=key, request_digest=digest)
    if not claimed:
        job_row.update(state='accepted', job_revision=1, owner_resource_id=None,
            owner_epoch=None, owner_incarnation=None)
    records = [
        ('management_audit', dict(audit, audit_id=audit_id, subject_id=key)),
        ('management_commands', dict(command, request_key=key, request_digest=digest, audit_id=audit_id)),
        ('jobs', job_row),
        ('acquisition_jobs', dict(detail, job_id=job, request_key=key)),
        ('job_admissions', dict(job_id=job, admission_ordinal=ordinal)),
    ]
    statements = []
    for table, row in records:
        row.pop('xmin', None)
        literal = json.dumps(row).replace("'", "''")
        statements.append(f"INSERT INTO control.{table} SELECT * FROM jsonb_populate_record(NULL::control.{table},'{literal}'::jsonb);")
    lab.sql(pg, database, 'BEGIN; SET LOCAL session_replication_role=replica; ' + '\n'.join(statements) + ' COMMIT;')
    return job


def run(pg, client, output, rows, prepare, check_native, cases, digest, initialized, *, wave='first'):
    assert wave in ('first', 'guards', 'limits', 'faults')
    def upgrade(database, config, name):
        before = rows(database)
        assert len(before) == 69 and 'acquisition_gaps' not in before
        target = '27' if wave == 'first' else '28'
        response = lab.cli(dict(pg, client=client), 'migrate', 'apply', '--file', config,
            '--user', 'bigbrotr_migrator', '--password-ref', 'file:/run/secrets/migrator', '--target', target)
        lab.write_json(output / (name + '-migration.json'), dict(returncode=response.returncode,
            stdout=response.stdout, stderr=response.stderr))
        assert response.returncode == 0, response.stderr
        after = rows(database)
        assert len(after) == 70 and after['acquisition_gaps'] == []
        assert all(after[t] == before[t] for t in before if t != 'schema_versions')
        added = [r for r in after['schema_versions'] if r not in before['schema_versions']]
        wanted = {27: '0027_acquisition_abandonment.sql'}
        if wave != 'first':
            wanted[28] = '0028_acquisition_gap_guard.sql'
        assert {r['migration'] for r in added} == set(wanted) and len(added) == len(wanted)
        for row in added:
            sql = Path(__file__).resolve().parents[2] / 'migrations/postgres' / wanted[row['migration']]
            assert row['checksum'] == '\\x' + hashlib.sha256(sql.read_bytes()).hexdigest()
        assert all(r in after['schema_versions'] for r in before['schema_versions'])
        repeat = lab.cli(dict(pg, client=client), 'migrate', 'apply', '--file', config,
            '--user', 'bigbrotr_migrator', '--password-ref', 'file:/run/secrets/migrator', '--target', target)
        assert repeat.returncode == 0 and rows(database) == after
        lab.write_json(output / (name + '-upgraded.json'), dict(before=before, after=after,
            exact_reapply=True))
        lab.sql(pg, database, 'GRANT SELECT,INSERT ON control.acquisition_gaps TO bigbrotr_control; '
            'GRANT EXECUTE ON FUNCTION control.guard_acquisition_gap(),control.require_acquisition_gap_complete() TO bigbrotr_control;')

    def invoke(database, config, directory, name, expected=1, error=None, reclaims_control=False):
        before = rows(database)
        lab.write_json(output / (name + '-before.json'), before)
        started = time.monotonic()
        response = lab.cli(dict(pg, client=client), 'ingest', 'recover', '--file', config, '--abandon-acquisition')
        elapsed = time.monotonic() - started
        lab.write_json(output / (name + '-response.json'), dict(returncode=response.returncode,
            stdout=response.stdout, stderr=response.stderr, elapsed_seconds=elapsed))
        after = rows(database)
        lab.write_json(output / (name + '-after.json'), after)
        check_native(directory, name)
        comparison = before
        if reclaims_control:
            from source_closure_faults import assert_reclaimed
            current = max((r for r in after['owner_epochs'] if r['role'] == 'control'), key=lambda r: r['epoch'])
            assert_reclaimed(before, after, current)
            # The separate oracle checked the exact prior-owner fence. Compare
            # every other effect against that observed monotone transition.
            comparison = dict(before,
                owner_epochs=[r for r in after['owner_epochs'] if r != current],
                role_instances=[r for r in after['role_instances'] if r['role_instance_id'] != current['role_instance_id']])
        if error:
            assert response.returncode == 4 and not response.stdout and error in response.stderr, response
            assert before.get('acquisition_gaps') == after.get('acquisition_gaps')
            assert before['jobs'] == after['jobs']
        else:
            assert response.returncode == 0 and not response.stderr, response
            result, = [json.loads(line) for line in response.stdout.splitlines()]
            assert result == dict(format='bigbrotr-ingest-recovery/1', state='acquisition-abandoned',
                dataset_id=initialized['dataset_id'], owner_resource_id=initialized['owner_resource_id'],
                producer_slot_id=initialized['producer_slot_id'], producer_epoch='1', issued_through='4',
                reserved_through='4', resolved_requests='4', native='closed', owner_release='committed', ready=False,
                source_closure=dict(last_complete_sequence='12', sha256=digest),
                acquisition_abandonment=dict(jobs=str(expected), reason=REASON, coverage='unknown'))
            assert assert_abandoned(comparison, after, digest) == expected
        # Check these two effects separately above, then reuse the complete
        # established Control/source/native oracle for every other table.
        isolated = dict(comparison, jobs=after['jobs'])
        if 'acquisition_gaps' in before:
            isolated['acquisition_gaps'] = after['acquisition_gaps']
        assert_effects(isolated, after, True, digest, released=not error)
        assert lab.sql(pg, database, "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control'") == '0'
        cases.append(dict(name=name, status='PASS', control_tables=len(after), native_rows=49,
            original_requests=4, expected_abandoned=None if error else expected,
            expected_refusal=error, seconds=elapsed))
        lab.write_json(output / 'signed-recovery-cases.json', cases)

    if wave == 'faults':
        from source_closure_faults import run as run_faults
        def prepare_abandonment(name):
            database, config, directory = prepare(name)
            upgrade(database, config, name)
            return database, config, directory
        run_faults(pg, client, output, rows, prepare_abandonment, check_native, invoke, cases, digest,
            abandon_acquisition=True)
        return

    if wave == 'limits':
        from acquisition_abandonment_limits import run as run_limits
        run_limits(pg, client, output, rows, prepare, upgrade, invoke, cases)
        return

    database, config, directory = prepare('abandon_signed')
    upgrade(database, config, directory.name)
    invoke(database, config, directory, 'abandon-signed')
    invoke(database, config, directory, 'abandon-signed-retry')

    if wave == 'guards':
        from acquisition_gap_guards import run as run_guards
        run_guards(pg, client, output, rows, prepare, check_native, upgrade, cases, digest)
        return

    database, config, directory = prepare('abandon_no_request')
    upgrade(database, config, directory.name)
    state = rows(database)
    claimed = duplicate_intent(pg, database, state, True)
    unclaimed = duplicate_intent(pg, database, state, False)
    before = rows(database)
    assert all(r['acquisition_job_id'] != claimed for r in before['producer_requests'])
    assert next(r for r in before['jobs'] if r['job_id'] == unclaimed)['state'] == 'accepted'
    invoke(database, config, directory, 'abandon-no-request', expected=2)

    database, config, directory = prepare('abandon_revoked')
    upgrade(database, config, directory.name)
    # Prepare a revoked historical actor. No authority or policy is changed
    # while the actual recovery is executing; all runtime triggers stay active.
    lab.sql(pg, database, 'BEGIN; SET LOCAL session_replication_role=replica; '
        'UPDATE control.local_accounts SET active=false; DELETE FROM control.principal_grants; COMMIT;')
    invoke(database, config, directory, 'abandon-revoked')

    database, config, directory = prepare('abandon_advanced')
    upgrade(database, config, directory.name)
    lab.sql(pg, database, "BEGIN; SET LOCAL session_replication_role=replica; "
        "UPDATE control.jobs SET checkpoint_revision=1 WHERE kind='acquisition'; COMMIT;")
    invoke(database, config, directory, 'abandon-advanced', error='InvalidMetadata')

    database, config, directory = prepare('abandon_old_schema')
    invoke(database, config, directory, 'abandon-old-schema', error='IncompatibleDeployment')
