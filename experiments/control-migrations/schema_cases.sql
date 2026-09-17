-- Isolated synthetic PostgreSQL 16.15 constraint fixture; see README.md.
-- The caller applies the migration first and supplies its independently hashed SQL.
\set ON_ERROR_STOP on
\pset pager off
\if :{?expected_migration_sha256}
\else
\echo 'Missing -v expected_migration_sha256=<sha256 of 0001_control.sql>'
\quit 2
\endif
SET statement_timeout = '10s';
SET lock_timeout = '2s';
SET idle_in_transaction_session_timeout = '30s';

CREATE TEMP TABLE fixture_results (
    case_name text PRIMARY KEY,
    expected_sqlstate text,
    observed_sqlstate text
);
CREATE FUNCTION pg_temp.id(n integer) RETURNS bytea LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT decode(lpad(to_hex(n), 32, '0'), 'hex')
$$;
CREATE FUNCTION pg_temp.digest(n integer) RETURNS bytea LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT decode(lpad(to_hex(n), 64, '0'), 'hex')
$$;
CREATE FUNCTION pg_temp.assert_true(label text, result boolean) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    IF result IS DISTINCT FROM true THEN
        RAISE EXCEPTION 'assertion failed: %', label;
    END IF;
    INSERT INTO pg_temp.fixture_results VALUES (label, NULL, NULL);
END;
$$;
-- A failed statement must leave every application row unchanged, including rows
-- inserted earlier within the same failing test subtransaction. The finite table
-- list is independent of the production planner or trigger implementations.
CREATE FUNCTION pg_temp.control_snapshot() RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE relation text; rows jsonb; result jsonb := '{}'::jsonb;
BEGIN
    FOREACH relation IN ARRAY ARRAY['schema_versions', 'datasets', 'role_instances',
        'owner_epochs', 'config_revisions', 'policy_revisions', 'topology_generations',
        'topology_streams', 'jobs', 'job_input_bounds'] LOOP
        EXECUTE format('SELECT coalesce(jsonb_agg(value ORDER BY value::text COLLATE "C"), ''[]''::jsonb) FROM (SELECT to_jsonb(t) AS value FROM control.%I t) captured', relation)
            INTO rows;
        result := result || jsonb_build_object(relation, rows);
    END LOOP;
    RETURN result;
END;
$$;
CREATE FUNCTION pg_temp.expect_error(label text, expected text, command text) RETURNS void LANGUAGE plpgsql AS $$
DECLARE before_rows jsonb; actual text;
BEGIN
    before_rows := pg_temp.control_snapshot();
    BEGIN
        EXECUTE command;
        -- Force deferred checks inside the exception subtransaction. A failure
        -- rolls back all its writes, rather than escaping into the next case.
        SET CONSTRAINTS ALL IMMEDIATE;
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS actual = RETURNED_SQLSTATE;
    END;
    IF actual IS DISTINCT FROM expected THEN
        RAISE EXCEPTION 'case % expected SQLSTATE %, received %', label, expected, coalesce(actual, 'no error');
    END IF;
    IF pg_temp.control_snapshot() IS DISTINCT FROM before_rows THEN
        RAISE EXCEPTION 'case % left changed control rows after failure', label;
    END IF;
    INSERT INTO pg_temp.fixture_results VALUES (label, expected, actual);
END;
$$;
-- Synthetic fixture constructors contain literal identities and independent data;
-- they do not call a production planner or calculate expected outcomes from DDL.
CREATE FUNCTION pg_temp.new_job(n integer, topology numeric, complete boolean) RETURNS void LANGUAGE sql AS $$
    INSERT INTO control.jobs (job_id, dataset_id, principal_id, request_key, request_digest,
        kind, config_revision, policy_revision, created_at_us, input_topology_generation, input_complete)
    VALUES (pg_temp.id(n), pg_temp.id(1), pg_temp.id(4), convert_to('fixture-job-' || n, 'UTF8'),
        pg_temp.digest(40), 'export', 1, 1, 100, topology, complete)
$$;
CREATE FUNCTION pg_temp.add_cut(n integer, closed_bound numeric DEFAULT 9) RETURNS void LANGUAGE sql AS $$
    INSERT INTO control.job_input_bounds VALUES
        (pg_temp.id(n), pg_temp.id(1), 0, 0, 1, 0),
        (pg_temp.id(n), pg_temp.id(1), 0, 7, 2, closed_bound),
        (pg_temp.id(n), pg_temp.id(1), 0, 4294967295, 18446744073709551615, 18446744073709551615)
$$;

SELECT pg_temp.assert_true('exact_pg_16_15', current_setting('server_version_num') = '160015');
SELECT pg_temp.assert_true('one_exact_independent_receipt',
    (SELECT count(*) = 1 FROM control.schema_versions)
    AND EXISTS (SELECT 1 FROM control.schema_versions WHERE engine = 'postgres-control'
        AND migration = 1 AND name = '0001_control.sql'
        AND checksum = decode(:'expected_migration_sha256', 'hex')
        AND reader_min = 1 AND reader_max = 1 AND writer_floor = 1));
SELECT pg_temp.assert_true('fresh_application_database',
    (SELECT bool_and(value = '[]'::jsonb)
        FROM jsonb_each(pg_temp.control_snapshot() - 'schema_versions')));

-- Scalar/domain behavior, including the server casts used by base-type bindings.
CREATE TEMP TABLE scalar_roundtrip (bound_id control.id16, bound_sequence control.u64);
PREPARE fixture_base_binding(bytea, text) AS
    INSERT INTO pg_temp.scalar_roundtrip VALUES ($1::bytea, ($2::text)::numeric);
EXECUTE fixture_base_binding(decode('00000000000000000000000000000001', 'hex'), '18446744073709551615');
SELECT pg_temp.assert_true('base_binding_preserves_u64_max',
    (SELECT count(*) = 1 AND bool_and(bound_id = pg_temp.id(1)
        AND bound_sequence::text = '18446744073709551615') FROM pg_temp.scalar_roundtrip));
SELECT pg_temp.assert_true('zero_is_valid_sequence_and_topology_domain', 0::control.u64 = 0);
SELECT pg_temp.assert_true('signed_observed_time_extremes',
    (-9223372036854775808)::bigint::text = '-9223372036854775808'
    AND 9223372036854775807::bigint::text = '9223372036854775807');
SELECT pg_temp.assert_true('numeric_scale_coerces_fraction_before_domain_check', 1.5::control.u64 = 2);
SELECT pg_temp.expect_error('u64_overflow', '23514', $q$SELECT 18446744073709551616::control.u64$q$);
SELECT pg_temp.expect_error('u64_negative', '23514', $q$SELECT (-1)::control.u64$q$);
SELECT pg_temp.expect_error('u64_nan', '23514', $q$SELECT 'NaN'::control.u64$q$);
SELECT pg_temp.expect_error('u32_overflow', '23514', $q$SELECT 4294967296::control.u32$q$);
SELECT pg_temp.expect_error('nil_identity', '23514', $q$SELECT decode(repeat('00',16),'hex')::control.id16$q$);
SELECT pg_temp.expect_error('short_identity', '23514', $q$SELECT decode('01','hex')::control.id16$q$);
SELECT pg_temp.expect_error('short_digest', '23514', $q$SELECT decode('01','hex')::control.digest32$q$);
SELECT pg_temp.expect_error('ledger_checksum_immutable', '23514', $q$UPDATE control.schema_versions SET checksum = pg_temp.digest(99)$q$);
SELECT pg_temp.expect_error('ledger_delete_refused', '23514', $q$DELETE FROM control.schema_versions$q$);
SELECT pg_temp.expect_error('initial_heads_cannot_be_prepopulated', '23514', $q$
    INSERT INTO control.datasets (dataset_id,instance_id,created_at_us,config_revision)
        VALUES(pg_temp.id(1),pg_temp.id(2),0,1)$q$);

BEGIN;
INSERT INTO control.datasets (dataset_id, instance_id, created_at_us) VALUES (pg_temp.id(1), pg_temp.id(2), -1);
INSERT INTO control.config_revisions VALUES
    (1, pg_temp.id(1), NULL, 1, 'managed', '{"fixture_policy":1}', pg_temp.digest(1), pg_temp.digest(2), 0, pg_temp.id(3));
INSERT INTO control.policy_revisions VALUES
    (1, pg_temp.id(1), NULL, 1, 1, '{"fixture_policy":1}', pg_temp.digest(3), 0, pg_temp.id(3));
INSERT INTO control.topology_generations VALUES (pg_temp.id(1), 0, NULL, 0, false);
INSERT INTO control.topology_streams VALUES
    (pg_temp.id(1), 0, 0, 1, NULL),
    (pg_temp.id(1), 0, 7, 2, 9),
    (pg_temp.id(1), 0, 4294967295, 18446744073709551615, 18446744073709551615);
UPDATE control.topology_generations SET sealed = true WHERE generation = 0;
UPDATE control.datasets SET config_revision = 1, policy_revision = 1, topology_generation = 0
    WHERE config_revision IS NULL AND policy_revision IS NULL AND topology_generation IS NULL;
INSERT INTO control.role_instances VALUES
    (pg_temp.id(10), pg_temp.id(1), 'control', 0, NULL),
    (pg_temp.id(11), pg_temp.id(1), 'ingest', 0, NULL);
-- The intentionally expired lease is not positive old-writer exclusion.
INSERT INTO control.owner_epochs (resource_id,epoch,dataset_id,role,role_instance_id,
    process_incarnation,acquired_at_us,lease_expires_at_us,state)
    VALUES (pg_temp.id(20),1,pg_temp.id(1),'control',pg_temp.id(10),pg_temp.id(30),0,-1,'active');
SELECT pg_temp.new_job(40, 0, false);
SELECT pg_temp.add_cut(40);
UPDATE control.jobs SET input_complete = true WHERE job_id = pg_temp.id(40);
SELECT pg_temp.new_job(41, NULL, true);
INSERT INTO control.topology_generations VALUES (pg_temp.id(1), 1, 0, 1, true);
SELECT pg_temp.new_job(42, 1, true);
COMMIT;
SELECT pg_temp.assert_true('committed_initial_identity_and_heads', EXISTS (
    SELECT 1 FROM control.datasets WHERE dataset_id = pg_temp.id(1) AND instance_id = pg_temp.id(2)
        AND config_revision = 1 AND policy_revision = 1 AND topology_generation = 0));
SELECT pg_temp.assert_true('committed_exact_cut_and_u64_bounds',
    (SELECT count(*) = 3 FROM control.job_input_bounds WHERE job_id = pg_temp.id(40))
    AND EXISTS (SELECT 1 FROM control.job_input_bounds WHERE job_id = pg_temp.id(40) AND shard_id = 0 AND last_complete_sequence = 0)
    AND EXISTS (SELECT 1 FROM control.job_input_bounds WHERE job_id = pg_temp.id(40) AND shard_id = 7 AND owner_epoch = 2 AND last_complete_sequence = 9)
    AND EXISTS (SELECT 1 FROM control.job_input_bounds WHERE job_id = pg_temp.id(40) AND shard_id = 4294967295
        AND owner_epoch::text = '18446744073709551615' AND last_complete_sequence::text = '18446744073709551615'));
SELECT pg_temp.assert_true('absent_cut_differs_from_sealed_empty_topology',
    EXISTS (SELECT 1 FROM control.jobs WHERE job_id = pg_temp.id(41) AND input_topology_generation IS NULL AND input_complete)
    AND EXISTS (SELECT 1 FROM control.jobs WHERE job_id = pg_temp.id(42) AND input_topology_generation = 1 AND input_complete)
    AND NOT EXISTS (SELECT 1 FROM control.job_input_bounds WHERE job_id IN (pg_temp.id(41),pg_temp.id(42))));

SELECT pg_temp.expect_error('only_one_local_dataset', '23505', $q$
    INSERT INTO control.datasets(dataset_id,instance_id,created_at_us) VALUES(pg_temp.id(91),pg_temp.id(92),0)$q$);
SELECT pg_temp.expect_error('dataset_identity_immutable', '23514', $q$UPDATE control.datasets SET instance_id = pg_temp.id(90)$q$);
SELECT pg_temp.expect_error('dataset_delete_refused', '23514', $q$DELETE FROM control.datasets$q$);
SELECT pg_temp.expect_error('head_skip_refused', '23514', $q$UPDATE control.datasets SET config_revision = 3$q$);
SELECT pg_temp.expect_error('head_backward_refused', '23514', $q$UPDATE control.datasets SET config_revision = 0$q$);
SELECT pg_temp.expect_error('policy_head_missing_revision_refused', '23503', $q$UPDATE control.datasets SET policy_revision = 2$q$);
SELECT pg_temp.expect_error('topology_head_skip_refused', '23514', $q$UPDATE control.datasets SET topology_generation = 2$q$);
SELECT pg_temp.expect_error('head_reset_refused', '23514', $q$UPDATE control.datasets SET config_revision = NULL$q$);
SELECT pg_temp.expect_error('head_missing_revision_refused', '23503', $q$UPDATE control.datasets SET config_revision = 2$q$);
SELECT pg_temp.expect_error('config_history_update_refused', '23514', $q$UPDATE control.config_revisions SET document = '{}'::jsonb$q$);
SELECT pg_temp.expect_error('config_history_delete_refused', '23514', $q$DELETE FROM control.config_revisions$q$);
SELECT pg_temp.expect_error('policy_history_update_refused', '23514', $q$UPDATE control.policy_revisions SET document = '{}'::jsonb$q$);
SELECT pg_temp.expect_error('policy_history_delete_refused', '23514', $q$DELETE FROM control.policy_revisions$q$);
SELECT pg_temp.expect_error('revision_foreign_dataset', '23503', $q$
    INSERT INTO control.config_revisions VALUES(2,pg_temp.id(99),1,1,'managed','{}',pg_temp.digest(1),pg_temp.digest(2),0,pg_temp.id(3))$q$);
SELECT pg_temp.expect_error('revision_gap_refused', '23514', $q$
    INSERT INTO control.config_revisions VALUES(3,pg_temp.id(1),1,1,'managed','{}',pg_temp.digest(1),pg_temp.digest(2),0,pg_temp.id(3))$q$);
SELECT pg_temp.expect_error('revision_document_requires_object', '23514', $q$
    INSERT INTO control.config_revisions VALUES(2,pg_temp.id(1),1,1,'managed','[]',pg_temp.digest(1),pg_temp.digest(2),0,pg_temp.id(3))$q$);
SELECT pg_temp.expect_error('revision_document_bounded', '23514', $q$
    INSERT INTO control.config_revisions VALUES(2,pg_temp.id(1),1,1,'managed',jsonb_build_object('x',repeat('a',10485761)),pg_temp.digest(1),pg_temp.digest(2),0,pg_temp.id(3))$q$);

-- Whole transaction abort, in addition to per-negative subtransaction rollback.
-- Exactly one expected ERROR is printed here; SQLSTATE is captured before ROLLBACK.
CREATE TEMP TABLE before_transaction AS SELECT pg_temp.control_snapshot() AS rows;
\echo 'Expected SQLSTATE 23514 in the following deliberate failed transaction'
\set ON_ERROR_STOP off
BEGIN;
INSERT INTO control.config_revisions VALUES(2,pg_temp.id(1),1,1,'managed','{}',pg_temp.digest(1),pg_temp.digest(2),0,pg_temp.id(3));
UPDATE control.datasets SET config_revision = 2 WHERE config_revision = 1;
SELECT (-1)::control.u64;
\set transaction_failure_state :SQLSTATE
ROLLBACK;
\set ON_ERROR_STOP on
SELECT pg_temp.assert_true('whole_transaction_expected_sqlstate', :'transaction_failure_state' = '23514');
SELECT pg_temp.assert_true('whole_transaction_rolls_back_revision_and_head',
    (SELECT rows = pg_temp.control_snapshot() FROM pg_temp.before_transaction));
-- A stale expected value changes zero rows, without implicitly advancing a head.
WITH changed AS (UPDATE control.datasets SET config_revision = 2 WHERE config_revision = 99 RETURNING *)
SELECT pg_temp.assert_true('stale_head_cas_updates_zero_rows', (SELECT count(*) = 0 FROM changed));

SELECT pg_temp.expect_error('topology_unsealed_commit_refused', '23514', $q$
    INSERT INTO control.topology_generations VALUES(pg_temp.id(1),2,1,2,false)$q$);
SELECT pg_temp.expect_error('topology_skip_refused', '23514', $q$
    INSERT INTO control.topology_generations VALUES(pg_temp.id(1),3,1,2,true)$q$);
SELECT pg_temp.expect_error('topology_missing_parent_refused', '23503', $q$
    INSERT INTO control.topology_generations VALUES(pg_temp.id(1),100,99,2,true)$q$);
SELECT pg_temp.expect_error('unsealed_topology_identity_cannot_change', '23514', $q$
    INSERT INTO control.topology_generations VALUES(pg_temp.id(1),2,1,2,false);
    UPDATE control.topology_generations SET created_at_us = 3,sealed = true WHERE generation = 2$q$);
SELECT pg_temp.expect_error('topology_reseal_refused', '23514', $q$UPDATE control.topology_generations SET sealed = true WHERE generation = 0$q$);
SELECT pg_temp.expect_error('topology_unseal_refused', '23514', $q$UPDATE control.topology_generations SET sealed = false WHERE generation = 0$q$);
SELECT pg_temp.expect_error('topology_delete_refused', '23514', $q$DELETE FROM control.topology_generations WHERE generation = 1$q$);
SELECT pg_temp.expect_error('topology_child_late_append_refused', '23514', $q$
    INSERT INTO control.topology_streams VALUES(pg_temp.id(1),0,8,1,NULL)$q$);
SELECT pg_temp.expect_error('topology_child_update_refused', '23514', $q$UPDATE control.topology_streams SET handoff_last_complete_sequence = 0 WHERE shard_id = 0$q$);
SELECT pg_temp.expect_error('topology_child_delete_refused', '23514', $q$DELETE FROM control.topology_streams WHERE shard_id = 0$q$);
SELECT pg_temp.expect_error('topology_duplicate_membership_refused', '23505', $q$
    INSERT INTO control.topology_generations VALUES(pg_temp.id(1),2,1,2,false);
    INSERT INTO control.topology_streams VALUES(pg_temp.id(1),2,8,1,NULL),(pg_temp.id(1),2,8,1,NULL)$q$);
SELECT pg_temp.expect_error('explicit_immediate_rejects_unsealed_topology', '23514', $q$
    INSERT INTO control.topology_generations VALUES(pg_temp.id(1),2,1,2,false); SET CONSTRAINTS ALL IMMEDIATE$q$);

SELECT pg_temp.expect_error('owner_initial_epoch_must_be_one', '23514', $q$
    INSERT INTO control.owner_epochs(resource_id,epoch,dataset_id,role,role_instance_id,process_incarnation,acquired_at_us,lease_expires_at_us,state)
    VALUES(pg_temp.id(90),2,pg_temp.id(1),'control',pg_temp.id(10),pg_temp.id(30),0,1,'active')$q$);
SELECT pg_temp.expect_error('expired_lease_does_not_authorize_successor', '23514', $q$
    INSERT INTO control.owner_epochs(resource_id,epoch,dataset_id,role,role_instance_id,process_incarnation,acquired_at_us,lease_expires_at_us,state)
    VALUES(pg_temp.id(20),2,pg_temp.id(1),'control',pg_temp.id(10),pg_temp.id(31),1,2,'active')$q$);
SELECT pg_temp.expect_error('owner_tuple_rewrite_refused', '23514', $q$UPDATE control.owner_epochs SET process_incarnation = pg_temp.id(99)$q$);
SELECT pg_temp.expect_error('owner_delete_refused', '23514', $q$DELETE FROM control.owner_epochs$q$);
SELECT pg_temp.expect_error('owner_release_requires_exclusion_record', '23514', $q$UPDATE control.owner_epochs SET state = 'released'$q$);
SELECT pg_temp.expect_error('owner_role_instance_must_match_dataset_and_role', '23503', $q$
    INSERT INTO control.owner_epochs(resource_id,epoch,dataset_id,role,role_instance_id,process_incarnation,acquired_at_us,lease_expires_at_us,state)
    VALUES(pg_temp.id(90),1,pg_temp.id(1),'ingest',pg_temp.id(10),pg_temp.id(30),0,1,'active')$q$);

SELECT pg_temp.expect_error('job_initial_running_refused', '23514', $q$
    INSERT INTO control.jobs(job_id,dataset_id,principal_id,request_key,request_digest,kind,config_revision,policy_revision,created_at_us,state,input_complete)
    VALUES(pg_temp.id(90),pg_temp.id(1),pg_temp.id(4),'x',pg_temp.digest(40),'export',1,1,100,'running',true)$q$);
SELECT pg_temp.expect_error('job_initial_revision_refused', '23514', $q$
    INSERT INTO control.jobs(job_id,dataset_id,principal_id,request_key,request_digest,kind,config_revision,policy_revision,created_at_us,job_revision,input_complete)
    VALUES(pg_temp.id(90),pg_temp.id(1),pg_temp.id(4),'x',pg_temp.digest(40),'export',1,1,100,2,true)$q$);
SELECT pg_temp.expect_error('job_initial_checkpoint_refused', '23514', $q$
    INSERT INTO control.jobs(job_id,dataset_id,principal_id,request_key,request_digest,kind,config_revision,policy_revision,created_at_us,checkpoint_revision,input_complete)
    VALUES(pg_temp.id(90),pg_temp.id(1),pg_temp.id(4),'x',pg_temp.digest(40),'export',1,1,100,1,true)$q$);
SELECT pg_temp.expect_error('job_duplicate_request_key_refused', '23505', $q$
    INSERT INTO control.jobs(job_id,dataset_id,principal_id,request_key,request_digest,kind,config_revision,policy_revision,created_at_us,input_complete)
    VALUES(pg_temp.id(90),pg_temp.id(1),pg_temp.id(4),convert_to('fixture-job-41','UTF8'),pg_temp.digest(99),'export',1,1,100,true)$q$);
SELECT pg_temp.expect_error('job_unsealed_input_refused', '23514', $q$SELECT pg_temp.new_job(90,NULL,false)$q$);
SELECT pg_temp.expect_error('job_missing_required_streams_refused', '23514', $q$SELECT pg_temp.new_job(90,0,true)$q$);
SELECT pg_temp.expect_error('job_extra_stream_refused', '23503', $q$
    SELECT pg_temp.new_job(90,0,false); SELECT pg_temp.add_cut(90);
    INSERT INTO control.job_input_bounds VALUES(pg_temp.id(90),pg_temp.id(1),0,8,1,0)$q$);
SELECT pg_temp.expect_error('job_foreign_dataset_bound_refused', '23503', $q$
    SELECT pg_temp.new_job(90,0,false);
    INSERT INTO control.job_input_bounds VALUES(pg_temp.id(90),pg_temp.id(99),0,0,1,0)$q$);
SELECT pg_temp.expect_error('job_foreign_topology_bound_refused', '23503', $q$
    SELECT pg_temp.new_job(90,0,false);
    INSERT INTO control.job_input_bounds VALUES(pg_temp.id(90),pg_temp.id(1),1,0,1,0)$q$);
SELECT pg_temp.expect_error('job_seal_cannot_change_other_state', '23514', $q$
    SELECT pg_temp.new_job(90,NULL,false);
    UPDATE control.jobs SET input_complete = true,reason_code = 'sneaked_state' WHERE job_id = pg_temp.id(90)$q$);
SELECT pg_temp.expect_error('job_duplicate_bound_refused', '23505', $q$
    SELECT pg_temp.new_job(90,0,false); SELECT pg_temp.add_cut(90);
    INSERT INTO control.job_input_bounds VALUES(pg_temp.id(90),pg_temp.id(1),0,0,1,0)$q$);
SELECT pg_temp.expect_error('job_above_closed_handoff_refused', '23514', $q$
    SELECT pg_temp.new_job(90,0,false); SELECT pg_temp.add_cut(90,10);
    UPDATE control.jobs SET input_complete = true WHERE job_id = pg_temp.id(90)$q$);
SELECT pg_temp.expect_error('job_bound_late_append_refused', '23514', $q$
    INSERT INTO control.job_input_bounds VALUES(pg_temp.id(40),pg_temp.id(1),0,8,1,0)$q$);
SELECT pg_temp.expect_error('job_bound_update_refused', '23514', $q$
    UPDATE control.job_input_bounds SET last_complete_sequence = 1 WHERE job_id = pg_temp.id(40) AND shard_id = 0$q$);
SELECT pg_temp.expect_error('job_bound_delete_refused', '23514', $q$DELETE FROM control.job_input_bounds WHERE job_id = pg_temp.id(40) AND shard_id = 0$q$);
SELECT pg_temp.expect_error('job_input_topology_rewrite_refused', '23514', $q$UPDATE control.jobs SET input_topology_generation = 1 WHERE job_id = pg_temp.id(40)$q$);
SELECT pg_temp.expect_error('job_request_digest_rewrite_refused', '23514', $q$UPDATE control.jobs SET request_digest = pg_temp.digest(99) WHERE job_id = pg_temp.id(40)$q$);
SELECT pg_temp.expect_error('job_request_key_rewrite_refused', '23514', $q$UPDATE control.jobs SET request_key = 'changed' WHERE job_id = pg_temp.id(40)$q$);
SELECT pg_temp.expect_error('job_config_rewrite_refused', '23514', $q$UPDATE control.jobs SET config_revision = 2 WHERE job_id = pg_temp.id(40)$q$);
SELECT pg_temp.expect_error('job_delete_refused', '23514', $q$DELETE FROM control.jobs WHERE job_id = pg_temp.id(40)$q$);
SELECT pg_temp.expect_error('job_update_without_owner_refused', '23514', $q$UPDATE control.jobs SET state = 'queued',job_revision = 2 WHERE job_id = pg_temp.id(40)$q$);

BEGIN;
UPDATE control.jobs SET state = 'queued',job_revision = 2,owner_resource_id = pg_temp.id(20),owner_epoch = 1,owner_incarnation = pg_temp.id(30) WHERE job_id = pg_temp.id(40);
UPDATE control.jobs SET state = 'running',job_revision = 3,checkpoint_revision = 1 WHERE job_id = pg_temp.id(40);
COMMIT;
SELECT pg_temp.assert_true('job_committed_running_checkpoint', EXISTS (
    SELECT 1 FROM control.jobs WHERE job_id = pg_temp.id(40) AND state = 'running' AND job_revision = 3 AND checkpoint_revision = 1));
SELECT pg_temp.expect_error('job_stale_revision_refused', '23514', $q$UPDATE control.jobs SET checkpoint_revision = 2 WHERE job_id = pg_temp.id(40)$q$);
SELECT pg_temp.expect_error('job_skipped_revision_refused', '23514', $q$UPDATE control.jobs SET job_revision = 5 WHERE job_id = pg_temp.id(40)$q$);
SELECT pg_temp.expect_error('job_checkpoint_regression_refused', '23514', $q$UPDATE control.jobs SET job_revision = 4,checkpoint_revision = 0 WHERE job_id = pg_temp.id(40)$q$);
SELECT pg_temp.expect_error('job_pause_requires_reason', '23514', $q$UPDATE control.jobs SET state = 'paused',job_revision = 4 WHERE job_id = pg_temp.id(40)$q$);
SELECT pg_temp.expect_error('job_success_requires_manifest', '23514', $q$UPDATE control.jobs SET state = 'succeeded',job_revision = 4,completed_at_us = 1000 WHERE job_id = pg_temp.id(40)$q$);
SELECT pg_temp.expect_error('job_success_requires_completion_time', '23514', $q$UPDATE control.jobs SET state = 'succeeded',job_revision = 4,result_manifest_digest = pg_temp.digest(50) WHERE job_id = pg_temp.id(40)$q$);
SELECT pg_temp.expect_error('job_wrong_incarnation_refused', '23514', $q$UPDATE control.jobs SET job_revision = 4,owner_incarnation = pg_temp.id(99) WHERE job_id = pg_temp.id(40)$q$);

UPDATE control.owner_epochs SET state = 'draining' WHERE resource_id = pg_temp.id(20) AND epoch = 1;
SELECT pg_temp.expect_error('draining_owner_cannot_mutate_job', '23514', $q$UPDATE control.jobs SET job_revision = 4 WHERE job_id = pg_temp.id(40)$q$);
SELECT pg_temp.expect_error('draining_owner_cannot_revive', '23514', $q$UPDATE control.owner_epochs SET state = 'active' WHERE epoch = 1$q$);
-- These are synthetic exclusion records; no real worker is running in this fixture.
UPDATE control.owner_epochs SET state = 'released',released_at_us = 2,exclusion_method = 'joined',exclusion_digest = pg_temp.digest(60)
    WHERE resource_id = pg_temp.id(20) AND epoch = 1;
SELECT pg_temp.expect_error('released_owner_cannot_mutate_job', '23514', $q$UPDATE control.jobs SET job_revision = 4 WHERE job_id = pg_temp.id(40)$q$);
SELECT pg_temp.expect_error('released_owner_cannot_revive', '23514', $q$
    UPDATE control.owner_epochs SET state = 'active',released_at_us = NULL,exclusion_method = NULL,exclusion_digest = NULL WHERE epoch = 1$q$);
SELECT pg_temp.expect_error('owner_successor_must_not_skip', '23514', $q$
    INSERT INTO control.owner_epochs(resource_id,epoch,dataset_id,role,role_instance_id,process_incarnation,acquired_at_us,lease_expires_at_us,state)
    VALUES(pg_temp.id(20),3,pg_temp.id(1),'control',pg_temp.id(10),pg_temp.id(31),2,3,'active')$q$);
SELECT pg_temp.expect_error('owner_successor_cannot_change_dataset', '23514', $q$
    INSERT INTO control.owner_epochs(resource_id,epoch,dataset_id,role,role_instance_id,process_incarnation,acquired_at_us,lease_expires_at_us,state)
    VALUES(pg_temp.id(20),2,pg_temp.id(99),'control',pg_temp.id(10),pg_temp.id(31),2,3,'active')$q$);
SELECT pg_temp.expect_error('owner_successor_cannot_change_role', '23514', $q$
    INSERT INTO control.owner_epochs(resource_id,epoch,dataset_id,role,role_instance_id,process_incarnation,acquired_at_us,lease_expires_at_us,state)
    VALUES(pg_temp.id(20),2,pg_temp.id(1),'ingest',pg_temp.id(11),pg_temp.id(31),2,3,'active')$q$);
INSERT INTO control.owner_epochs(resource_id,epoch,dataset_id,role,role_instance_id,process_incarnation,acquired_at_us,lease_expires_at_us,state)
    VALUES(pg_temp.id(20),2,pg_temp.id(1),'control',pg_temp.id(10),pg_temp.id(31),2,3,'active');
UPDATE control.jobs SET job_revision = 4,checkpoint_revision = 2,owner_epoch = 2,owner_incarnation = pg_temp.id(31) WHERE job_id = pg_temp.id(40);
SELECT pg_temp.expect_error('completion_cannot_replace_starting_cut', '23514', $q$
    UPDATE control.jobs SET state = 'succeeded',job_revision = 5,result_manifest_digest = pg_temp.digest(50),completed_at_us = 1000,
        input_topology_generation = 1 WHERE job_id = pg_temp.id(40)$q$);
UPDATE control.jobs SET state = 'succeeded',job_revision = 5,result_manifest_digest = pg_temp.digest(50),completed_at_us = 1000 WHERE job_id = pg_temp.id(40);
SELECT pg_temp.expect_error('terminal_job_cannot_resume', '23514', $q$UPDATE control.jobs SET state = 'queued',job_revision = 6,completed_at_us = NULL WHERE job_id = pg_temp.id(40)$q$);
SELECT pg_temp.assert_true('completion_keeps_starting_cut',
    EXISTS (SELECT 1 FROM control.jobs WHERE job_id = pg_temp.id(40) AND state = 'succeeded' AND input_topology_generation = 0)
    AND (SELECT count(*) = 3 FROM control.job_input_bounds WHERE job_id = pg_temp.id(40))
    AND EXISTS (SELECT 1 FROM control.job_input_bounds WHERE job_id = pg_temp.id(40) AND shard_id = 0 AND last_complete_sequence = 0));

UPDATE control.jobs SET state = 'queued',job_revision = 2,owner_resource_id = pg_temp.id(20),owner_epoch = 2,owner_incarnation = pg_temp.id(31) WHERE job_id = pg_temp.id(41);
UPDATE control.jobs SET state = 'paused',job_revision = 3,reason_code = 'operator_pause' WHERE job_id = pg_temp.id(41);
SELECT pg_temp.expect_error('paused_job_cannot_start_directly', '23514', $q$UPDATE control.jobs SET state = 'running',job_revision = 4 WHERE job_id = pg_temp.id(41)$q$);
UPDATE control.jobs SET state = 'queued',job_revision = 4 WHERE job_id = pg_temp.id(41);
UPDATE control.jobs SET state = 'running',job_revision = 5,checkpoint_revision = 1 WHERE job_id = pg_temp.id(41);
UPDATE control.jobs SET state = 'cancel_requested',job_revision = 6,reason_code = 'operator_cancel' WHERE job_id = pg_temp.id(41);
SELECT pg_temp.expect_error('cancel_requested_only_drains_to_cancelled', '23514', $q$UPDATE control.jobs SET state = 'running',job_revision = 7 WHERE job_id = pg_temp.id(41)$q$);
UPDATE control.jobs SET state = 'cancelled',job_revision = 7,completed_at_us = 1001 WHERE job_id = pg_temp.id(41);
SELECT pg_temp.assert_true('cancelled_job_retains_checkpoint_and_absent_cut', EXISTS (
    SELECT 1 FROM control.jobs WHERE job_id = pg_temp.id(41) AND state = 'cancelled' AND job_revision = 7
        AND checkpoint_revision = 1 AND input_topology_generation IS NULL));
SELECT pg_temp.assert_true('only_three_accepted_fixture_jobs_survive', (SELECT count(*) = 3 FROM control.jobs));
SELECT pg_temp.assert_true('only_exact_revision_history_survives',
    (SELECT count(*) = 1 FROM control.config_revisions) AND (SELECT count(*) = 1 FROM control.policy_revisions)
    AND (SELECT count(*) = 1 FROM control.schema_versions));

SELECT pg_temp.assert_true('committed_topologies_owners_and_empty_job',
    (SELECT count(*) = 2 AND bool_and(sealed) FROM control.topology_generations)
    AND (SELECT count(*) = 3 FROM control.topology_streams)
    AND (SELECT count(*) = 2 FROM control.owner_epochs)
    AND EXISTS (SELECT 1 FROM control.owner_epochs WHERE resource_id = pg_temp.id(20) AND epoch = 1 AND state = 'released' AND exclusion_digest = pg_temp.digest(60))
    AND EXISTS (SELECT 1 FROM control.owner_epochs WHERE resource_id = pg_temp.id(20) AND epoch = 2 AND state = 'active' AND process_incarnation = pg_temp.id(31))
    AND EXISTS (SELECT 1 FROM control.jobs WHERE job_id = pg_temp.id(42) AND state = 'accepted' AND job_revision = 1 AND input_topology_generation = 1));

SELECT pg_temp.assert_true('all_expected_cases_executed', (SELECT count(*) FROM pg_temp.fixture_results) = 103);

-- A successful run prints both individual identities and a compact machine-readable
-- summary. Source/engine/process identities belong in the caller's detached report.
SELECT case_name, expected_sqlstate, observed_sqlstate FROM pg_temp.fixture_results ORDER BY case_name;
SELECT jsonb_build_object('fixture','control-schema-0001','result','PASS',
    'executed',(SELECT count(*) FROM pg_temp.fixture_results),
    'failed',0,'skipped',0,'not_run',0,'server_version',current_setting('server_version'),
    'migration_sha256',:'expected_migration_sha256') AS fixture_result;
