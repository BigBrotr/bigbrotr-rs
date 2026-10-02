"""Real relational counterexamples after actual signed source closure.

Only the current Control backend/lease is synthetic, inside each transaction.
The privileged setup ends before application-role gap/job writes and every
negative rolls it back. This does not qualify native exclusion or owner adoption.
"""
import json

import lab
from source_closure_faults import assert_effects


SETUP = """
    BEGIN;
    SELECT pg_advisory_xact_lock(1111651442, 1129599564);
    SET LOCAL session_replication_role=replica;
    UPDATE control.role_instances SET retired_at_us=NULL
        WHERE role_instance_id=(SELECT role_instance_id FROM control.owner_epochs
            WHERE role='control' ORDER BY epoch DESC LIMIT 1);
    UPDATE control.owner_epochs SET state='active',released_at_us=NULL,
        exclusion_method=NULL,exclusion_digest=NULL,
        lease_expires_at_us=(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint+60000000,
        control_backend_pid=pg_backend_pid(),
        control_backend_start=(SELECT backend_start FROM pg_stat_activity WHERE pid=pg_backend_pid())
        WHERE role='control' AND epoch=(SELECT max(epoch) FROM control.owner_epochs WHERE role='control');
    SET LOCAL session_replication_role=origin;
    SET LOCAL ROLE bigbrotr_control;
"""

INSERT = """
    INSERT INTO control.acquisition_gaps
        (job_id,dataset_id,producer_slot_id,producer_epoch,shard_id,source_owner_epoch,
         last_complete_sequence,source_closure_digest,previous_job_revision,
         previous_owner_resource,previous_owner_epoch,previous_owner_incarnation,
         issuer_resource,issuer_epoch,issuer_incarnation,abandoned_at_us,reason)
    SELECT j.job_id,j.dataset_id,p.producer_slot_id,p.producer_epoch,s.shard_id,s.owner_epoch,
        s.last_complete_sequence,s.closure_digest,j.job_revision,
        j.owner_resource_id,j.owner_epoch,j.owner_incarnation,
        o.resource_id,o.epoch,o.process_incarnation,
        (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,'initial_producer_abandoned'
    FROM control.jobs j JOIN control.source_streams s USING(dataset_id)
    JOIN control.producer_epochs p ON p.dataset_id=s.dataset_id AND p.owner_resource_id=s.owner_resource_id
    JOIN control.owner_epochs o ON o.dataset_id=j.dataset_id AND o.role='control' AND o.state='active'
    WHERE j.kind='acquisition';
"""

REQUEST = """
    UPDATE control.jobs j SET state='cancel_requested',job_revision=j.job_revision+1,
        owner_resource_id=g.issuer_resource,owner_epoch=g.issuer_epoch,
        owner_incarnation=g.issuer_incarnation,reason_code=g.reason
    FROM control.acquisition_gaps g WHERE g.job_id=j.job_id;
"""

COMPLETE = """
    UPDATE control.jobs j SET state='cancelled',job_revision=j.job_revision+1,
        completed_at_us=g.abandoned_at_us
    FROM control.acquisition_gaps g WHERE g.job_id=j.job_id;
    SET CONSTRAINTS ALL IMMEDIATE;
    SELECT 'complete-cancellation-checked';
"""


def run(pg, client, output, rows, prepare, check_native, upgrade, cases, digest):
    database, config, directory = prepare('gap_relations')
    upgrade(database, config, directory.name)
    before = rows(database)
    response = lab.cli(dict(pg, client=client), 'ingest', 'recover', '--file', config, '--close-source')
    lab.write_json(output / 'gap-relations-source-response.json', dict(returncode=response.returncode,
        stdout=response.stdout, stderr=response.stderr))
    assert response.returncode == 0 and not response.stderr
    report, = [json.loads(line) for line in response.stdout.splitlines()]
    assert report['state'] == 'source-closed' and report['source_closure'] == dict(last_complete_sequence='12', sha256=digest)
    after = rows(database)
    assert_effects(before, after, True, digest, released=True)
    check_native(directory, 'gap-relations-source')
    # Broader fixture DML grants isolate immutable-history guards from privilege
    # refusal. Ordinary runtime provisioning remains unchanged.
    lab.sql(pg, database, 'GRANT UPDATE,DELETE,TRUNCATE ON control.acquisition_gaps TO bigbrotr_control; '
        'GRANT DELETE ON control.jobs TO bigbrotr_control;')
    assert rows(database) == after
    lab.write_json(output / 'gap-relations-before.json', after)
    atomicity = 'whole-intent gap and terminal job must commit atomically'
    history = 'immutable control history cannot be changed'
    partial = INSERT + REQUEST
    complete = partial + COMPLETE
    schedules = [
        ('gap-only-commit', INSERT, atomicity),
        ('gap-requested-commit', partial, atomicity),
        ('gap-premature-check', INSERT + 'SET CONSTRAINTS ALL IMMEDIATE;', atomicity),
        ('gap-terminal-update', complete + "UPDATE control.jobs SET job_revision=job_revision+1 WHERE kind='acquisition';", 'invalid finite job transition'),
        ('gap-terminal-delete', complete + "DELETE FROM control.jobs WHERE kind='acquisition';", 'job correctness obligations require explicit reclamation'),
        ('gap-history-update', complete + 'UPDATE control.acquisition_gaps SET reason=reason;', history),
        ('gap-history-delete', complete + 'DELETE FROM control.acquisition_gaps;', history),
        ('gap-history-truncate', complete + 'TRUNCATE control.acquisition_gaps;', history),
    ]
    def execute(name, statement):
        sql = SETUP + statement + '\nCOMMIT;'
        (output / (name + '.sql')).write_text(sql)
        answer = lab.docker('exec', '-i', pg['postgres'], 'psql', '-X', '-A', '-t', '-q',
            '--no-password', '-v', 'ON_ERROR_STOP=1', '-v', 'VERBOSITY=verbose',
            '-U', 'postgres', '-d', database, data=sql, check=False)
        lab.write_json(output / (name + '-response.json'), dict(returncode=answer.returncode,
            stdout=answer.stdout, stderr=answer.stderr))
        return answer
    for name, statement, reason in schedules:
        answer = execute(name, statement)
        assert answer.returncode == 3 and '23514: ' + reason in answer.stderr, answer
        assert ('complete-cancellation-checked' in answer.stdout) == statement.startswith(complete)
        current = rows(database)
        lab.write_json(output / (name + '-after.json'), current)
        assert current == after, name
        cases.append(dict(name=name, status='PASS', sqlstate='23514', reason=reason,
            all_control_rows_xmin_unchanged=True, synthetic_backend_setup_rolled_back=True))
    check_native(directory, 'gap-relations-after')
    # Isolated deliberate mutation after the positive-guard cases: the identical
    # first counterexample must now commit an incomplete gap. Retain this broken
    # clone explicitly; no product command is run against it afterwards.
    lab.sql(pg, database, 'DROP TRIGGER acquisition_gap_complete ON control.acquisition_gaps;')
    answer = execute('gap-missing-guard-negative', INSERT)
    assert answer.returncode == 0 and not answer.stderr, answer
    broken = rows(database)
    lab.write_json(output / 'gap-missing-guard-negative-after.json', broken)
    assert len(broken['acquisition_gaps']) == 1 and not after['acquisition_gaps']
    assert broken['jobs'] == after['jobs']
    assert next(r for r in broken['jobs'] if r['kind'] == 'acquisition')['state'] == 'queued'
    for table in after.keys() - {'acquisition_gaps', 'owner_epochs', 'role_instances'}:
        assert after[table] == broken[table], table
    cases.append(dict(name='gap-missing-guard-negative', status='EXPECTED_FAIL_CONSTRAINT_ABSENT',
        incomplete_gap_committed=True, retained_negative_database=database))
    lab.write_json(output / 'signed-recovery-cases.json', cases)
