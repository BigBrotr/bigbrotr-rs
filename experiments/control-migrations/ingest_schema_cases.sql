-- Independent synthetic PostgreSQL 16.15 cases for 0002_ingest.sql.
-- The caller applies migrations 0001 and 0002 with the real migration command,
-- then supplies independently calculated checksums for both source files.
\set ON_ERROR_STOP on
\pset pager off
\if :{?expected_migration_1_sha256}
\else
\echo 'Missing -v expected_migration_1_sha256=<sha256 of 0001_control.sql>'
\quit 2
\endif
\if :{?expected_migration_2_sha256}
\else
\echo 'Missing -v expected_migration_2_sha256=<sha256 of 0002_ingest.sql>'
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
-- The relation list is literal and independent of production trigger code. It
-- verifies that each expected failure rolls back every write in its subtransaction.
CREATE FUNCTION pg_temp.control_snapshot() RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE relation text; rows jsonb; result jsonb := '{}'::jsonb;
BEGIN
    FOREACH relation IN ARRAY ARRAY[
        'datasets', 'topology_generations', 'topology_streams', 'role_instances',
        'owner_epochs', 'config_revisions', 'policy_revisions', 'jobs', 'job_input_bounds',
        'allocation_domains', 'source_streams', 'producer_slots', 'producer_epochs',
        'producer_checkpoints', 'consumers', 'consumer_frontier_revisions',
        'consumer_frontiers', 'projection_generations', 'projection_chunks',
        'projection_chunk_bounds', 'snapshot_leases', 'retention_claims'
    ] LOOP
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

SELECT pg_temp.assert_true('exact_pg_16_15', current_setting('server_version_num') = '160015');
SELECT pg_temp.assert_true('two_exact_migration_receipts',
    (SELECT count(*) = 2 FROM control.schema_versions)
    AND EXISTS (SELECT 1 FROM control.schema_versions
        WHERE engine = 'postgres-control' AND migration = 1 AND name = '0001_control.sql'
          AND checksum = decode(:'expected_migration_1_sha256', 'hex'))
    AND EXISTS (SELECT 1 FROM control.schema_versions
        WHERE engine = 'postgres-control' AND migration = 2 AND name = '0002_ingest.sql'
          AND checksum = decode(:'expected_migration_2_sha256', 'hex')));
SELECT pg_temp.assert_true('fresh_ingest_relations',
    (SELECT bool_and(value = '[]'::jsonb) FROM jsonb_each(pg_temp.control_snapshot())));

-- Literal fixture authority. Expired lease timestamps are intentional: state and
-- positive exclusion, rather than wall time alone, govern writer authority.
BEGIN;
INSERT INTO control.datasets(dataset_id, instance_id, created_at_us)
    VALUES(pg_temp.id(1), pg_temp.id(2), 0);
INSERT INTO control.config_revisions VALUES
    (1,pg_temp.id(1),NULL,1,'managed','{}',pg_temp.digest(1),pg_temp.digest(2),0,pg_temp.id(3));
INSERT INTO control.policy_revisions VALUES
    (1,pg_temp.id(1),NULL,1,1,'{}',pg_temp.digest(3),0,pg_temp.id(3));
INSERT INTO control.role_instances VALUES
    (pg_temp.id(10),pg_temp.id(1),'control',0,NULL),
    (pg_temp.id(11),pg_temp.id(1),'ingest',0,NULL),
    (pg_temp.id(12),pg_temp.id(1),'discoverer',0,NULL),
    (pg_temp.id(13),pg_temp.id(1),'archive',0,NULL),
    (pg_temp.id(14),pg_temp.id(1),'index',0,NULL),
    (pg_temp.id(15),pg_temp.id(1),'read',0,1);
INSERT INTO control.owner_epochs(resource_id,epoch,dataset_id,role,role_instance_id,
    process_incarnation,acquired_at_us,lease_expires_at_us,state) VALUES
    (pg_temp.id(20),1,pg_temp.id(1),'control',pg_temp.id(10),pg_temp.id(21),0,-1,'active'),
    (pg_temp.id(30),1,pg_temp.id(1),'ingest',pg_temp.id(11),pg_temp.id(31),0,-1,'active'),
    (pg_temp.id(40),1,pg_temp.id(1),'archive',pg_temp.id(13),pg_temp.id(41),0,-1,'active'),
    (pg_temp.id(50),1,pg_temp.id(1),'index',pg_temp.id(14),pg_temp.id(51),0,-1,'active'),
    (pg_temp.id(52),1,pg_temp.id(1),'index',pg_temp.id(14),pg_temp.id(53),0,-1,'active');
INSERT INTO control.topology_generations VALUES(pg_temp.id(1),0,NULL,0,false);
INSERT INTO control.topology_streams VALUES
    (pg_temp.id(1),0,0,1,NULL),
    (pg_temp.id(1),0,7,1,NULL);
INSERT INTO control.source_streams(dataset_id,shard_id,owner_epoch,owner_resource_id,
    owner_incarnation,opened_at_us) VALUES
    (pg_temp.id(1),0,1,pg_temp.id(30),pg_temp.id(31),0),
    (pg_temp.id(1),7,1,pg_temp.id(30),pg_temp.id(31),0);
UPDATE control.topology_generations SET sealed = true WHERE generation = 0;
UPDATE control.datasets SET config_revision = 1, policy_revision = 1,
    topology_generation = 0 WHERE dataset_id = pg_temp.id(1);

INSERT INTO control.allocation_domains(dataset_id,dictionary_kind,allocation_domain,
    owner_resource_id,owner_epoch,owner_incarnation,created_at_us)
    VALUES(pg_temp.id(1),8,pg_temp.id(60),pg_temp.id(30),1,pg_temp.id(31),0);
INSERT INTO control.producer_slots(producer_slot_id,dataset_id,registered_by_resource_id,
    registered_by_epoch,registered_by_incarnation,created_at_us)
    VALUES(pg_temp.id(61),pg_temp.id(1),pg_temp.id(20),1,pg_temp.id(21),0);
INSERT INTO control.producer_epochs(producer_slot_id,producer_epoch,dataset_id,
    producer_role_instance_id,producer_role,producer_incarnation,owner_resource_id,
    owner_epoch,owner_incarnation,outstanding_request_limit,opened_at_us)
    VALUES(pg_temp.id(61),1,pg_temp.id(1),pg_temp.id(12),'discoverer',pg_temp.id(62),
        pg_temp.id(30),1,pg_temp.id(31),2,0);
INSERT INTO control.producer_checkpoints VALUES
    (pg_temp.id(61),1,pg_temp.id(1),1,0,pg_temp.digest(61),0);

INSERT INTO control.consumers VALUES
    (pg_temp.id(70),pg_temp.id(1),'archive',0,NULL,NULL),
    (pg_temp.id(71),pg_temp.id(1),'index',0,NULL,NULL);
INSERT INTO control.consumer_frontier_revisions(consumer_id,frontier_revision,dataset_id,
    consumer_role,topology_generation,previous_revision,owner_resource_id,owner_epoch,
    owner_incarnation,created_at_us)
    VALUES(pg_temp.id(70),1,pg_temp.id(1),'archive',0,NULL,pg_temp.id(40),1,pg_temp.id(41),0);
INSERT INTO control.consumer_frontiers VALUES
    (pg_temp.id(70),1,pg_temp.id(1),0,0,1,0,NULL,NULL),
    (pg_temp.id(70),1,pg_temp.id(1),0,7,1,5,'archive-batch',pg_temp.digest(71));
UPDATE control.consumer_frontier_revisions SET sealed = true
    WHERE consumer_id = pg_temp.id(70) AND frontier_revision = 1;
UPDATE control.consumers SET current_revision = 1,current_topology_generation = 0
    WHERE consumer_id = pg_temp.id(70);

INSERT INTO control.projection_generations VALUES
    (pg_temp.id(72),pg_temp.id(1),pg_temp.id(73),pg_temp.digest(72),
        pg_temp.id(50),1,'index',pg_temp.id(51),0);
INSERT INTO control.projection_chunks(generation_id,chunk_id,dataset_id,storage_incarnation,
    table_identity_digest,target,target_schema_version,topology_generation,chunk_ordinal,
    row_count,owner_resource_id,owner_epoch,owner_incarnation,prepared_at_us)
    VALUES(pg_temp.id(72),pg_temp.digest(73),pg_temp.id(1),pg_temp.id(73),pg_temp.digest(72),
        'events',1,0,0,0,pg_temp.id(50),1,pg_temp.id(51),0);
INSERT INTO control.projection_chunk_bounds VALUES
    (pg_temp.id(72),pg_temp.digest(73),pg_temp.id(1),0,0,1,0),
    (pg_temp.id(72),pg_temp.digest(73),pg_temp.id(1),0,7,1,5);
UPDATE control.projection_chunks SET state = 'committed',committed_at_us = 1,
    engine_receipt_digest = pg_temp.digest(74)
    WHERE generation_id = pg_temp.id(72) AND chunk_id = pg_temp.digest(73);

INSERT INTO control.snapshot_leases(lease_id,dataset_id,consumer_id,frontier_revision,
    topology_generation,holder_kind,holder_role_instance_id,holder_role,
    holder_incarnation,acquired_at_us,updated_at_us,expires_at_us)
    VALUES(pg_temp.id(80),pg_temp.id(1),pg_temp.id(70),1,0,'role_instance',
        pg_temp.id(13),'archive',pg_temp.id(81),0,0,200);
INSERT INTO control.retention_claims(claim_id,lease_id,dataset_id,topology_generation,
    shard_id,owner_epoch,first_sequence,last_sequence,purpose,created_at_us)
    VALUES(pg_temp.id(82),pg_temp.id(80),pg_temp.id(1),0,7,1,1,5,'backup',1);
COMMIT;

SELECT pg_temp.assert_true('registered_exact_source_and_allocation_authority',
    (SELECT count(*) = 2 FROM control.source_streams)
    AND EXISTS (SELECT 1 FROM control.allocation_domains WHERE dictionary_kind = 8
        AND state = 'active' AND allocation_domain = pg_temp.id(60)));
SELECT pg_temp.assert_true('zero_frontier_and_zero_row_chunk_are_explicit',
    EXISTS (SELECT 1 FROM control.consumer_frontiers WHERE consumer_id = pg_temp.id(70)
        AND shard_id = 0 AND last_complete_sequence = 0)
    AND EXISTS (SELECT 1 FROM control.projection_chunks WHERE generation_id = pg_temp.id(72)
        AND chunk_ordinal = 0 AND row_count = 0 AND state = 'committed'));
SELECT pg_temp.assert_true('expired_writer_lease_did_not_self_fence',
    EXISTS (SELECT 1 FROM control.producer_epochs WHERE producer_slot_id = pg_temp.id(61)
        AND state = 'open'));
SELECT pg_temp.assert_true('source_cut_lease_and_claim_are_distinct',
    EXISTS (SELECT 1 FROM control.snapshot_leases WHERE lease_id = pg_temp.id(80)
        AND state = 'active')
    AND EXISTS (SELECT 1 FROM control.retention_claims WHERE claim_id = pg_temp.id(82)
        AND state = 'active'));

-- Allocation authority is closed, exact and never a PG ordinal allocator.
SELECT pg_temp.expect_error('dictionary_kind_is_closed', '23514', $q$
    INSERT INTO control.allocation_domains(dataset_id,dictionary_kind,allocation_domain,
        owner_resource_id,owner_epoch,owner_incarnation,created_at_us)
    VALUES(pg_temp.id(1),10,pg_temp.id(63),pg_temp.id(30),1,pg_temp.id(31),0)$q$);
SELECT pg_temp.expect_error('allocation_domain_unique_across_kinds', '23505', $q$
    INSERT INTO control.allocation_domains(dataset_id,dictionary_kind,allocation_domain,
        owner_resource_id,owner_epoch,owner_incarnation,created_at_us)
    VALUES(pg_temp.id(1),7,pg_temp.id(60),pg_temp.id(30),1,pg_temp.id(31),0)$q$);
SELECT pg_temp.expect_error('one_active_allocation_domain_per_kind', '23505', $q$
    INSERT INTO control.allocation_domains(dataset_id,dictionary_kind,allocation_domain,
        owner_resource_id,owner_epoch,owner_incarnation,created_at_us)
    VALUES(pg_temp.id(1),8,pg_temp.id(63),pg_temp.id(30),1,pg_temp.id(31),0)$q$);
SELECT pg_temp.expect_error('allocation_identity_rewrite_refused', '23514', $q$
    UPDATE control.allocation_domains SET dictionary_kind = 7 WHERE allocation_domain = pg_temp.id(60)$q$);
SELECT pg_temp.expect_error('allocation_close_requires_receipt', '23514', $q$
    UPDATE control.allocation_domains SET state = 'closed' WHERE allocation_domain = pg_temp.id(60)$q$);
SELECT pg_temp.expect_error('allocation_delete_refused', '23514', $q$
    DELETE FROM control.allocation_domains WHERE allocation_domain = pg_temp.id(60)$q$);

-- Source registration and exact closed bounds cannot be inferred from a counter.
SELECT pg_temp.expect_error('source_without_topology_refused_at_commit', '23514', $q$
    INSERT INTO control.source_streams(dataset_id,shard_id,owner_epoch,owner_resource_id,
        owner_incarnation,opened_at_us)
    VALUES(pg_temp.id(1),99,1,pg_temp.id(30),pg_temp.id(31),0)$q$);
SELECT pg_temp.expect_error('source_cannot_begin_closed', '23514', $q$
    INSERT INTO control.source_streams(dataset_id,shard_id,owner_epoch,owner_resource_id,
        owner_incarnation,opened_at_us,state,closed_at_us,last_complete_sequence,closure_digest)
    VALUES(pg_temp.id(1),0,1,pg_temp.id(30),pg_temp.id(31),0,'closed',1,0,pg_temp.digest(1))$q$);
SELECT pg_temp.expect_error('source_cannot_skip_closing', '23514', $q$
    UPDATE control.source_streams SET state = 'closed',closed_at_us = 1,
        last_complete_sequence = 5,closure_digest = pg_temp.digest(1) WHERE shard_id = 7$q$);
SELECT pg_temp.expect_error('source_owner_tuple_immutable', '23514', $q$
    UPDATE control.source_streams SET owner_resource_id = pg_temp.id(20) WHERE shard_id = 7$q$);
SELECT pg_temp.expect_error('source_delete_refused', '23514', $q$
    DELETE FROM control.source_streams WHERE shard_id = 7$q$);
SELECT pg_temp.expect_error('owner_release_cannot_abandon_ingest_dependents', '23514', $q$
    UPDATE control.owner_epochs SET state = 'released',released_at_us = 1,
        exclusion_method = 'joined',exclusion_digest = pg_temp.digest(59)
        WHERE resource_id = pg_temp.id(30) AND epoch = 1$q$);

-- Producer registry/checkpoint state does not duplicate the Rocks owner ACK.
SELECT pg_temp.expect_error('producer_slot_identity_immutable', '23514', $q$
    UPDATE control.producer_slots SET created_at_us = 1 WHERE producer_slot_id = pg_temp.id(61)$q$);
SELECT pg_temp.expect_error('producer_slot_delete_refused', '23514', $q$
    DELETE FROM control.producer_slots WHERE producer_slot_id = pg_temp.id(61)$q$);
SELECT pg_temp.expect_error('producer_successor_requires_retired_predecessor', '23514', $q$
    INSERT INTO control.producer_epochs(producer_slot_id,producer_epoch,dataset_id,
        producer_role_instance_id,producer_role,producer_incarnation,owner_resource_id,
        owner_epoch,owner_incarnation,outstanding_request_limit,opened_at_us)
    VALUES(pg_temp.id(61),2,pg_temp.id(1),pg_temp.id(12),'discoverer',pg_temp.id(63),
        pg_temp.id(30),1,pg_temp.id(31),2,1)$q$);
SELECT pg_temp.expect_error('producer_epoch_cannot_skip_closing', '23514', $q$
    UPDATE control.producer_epochs SET state = 'retired',retired_at_us = 1,
        closure_digest = pg_temp.digest(62) WHERE producer_slot_id = pg_temp.id(61)$q$);
SELECT pg_temp.expect_error('producer_checkpoint_first_revision_is_one', '23514', $q$
    INSERT INTO control.producer_slots(producer_slot_id,dataset_id,registered_by_resource_id,
        registered_by_epoch,registered_by_incarnation,created_at_us)
        VALUES(pg_temp.id(64),pg_temp.id(1),pg_temp.id(20),1,pg_temp.id(21),0);
    INSERT INTO control.producer_epochs(producer_slot_id,producer_epoch,dataset_id,
        producer_role_instance_id,producer_role,producer_incarnation,owner_resource_id,
        owner_epoch,owner_incarnation,outstanding_request_limit,opened_at_us)
        VALUES(pg_temp.id(64),1,pg_temp.id(1),pg_temp.id(12),'discoverer',pg_temp.id(65),
            pg_temp.id(30),1,pg_temp.id(31),2,0);
    INSERT INTO control.producer_checkpoints VALUES
        (pg_temp.id(64),1,pg_temp.id(1),2,0,pg_temp.digest(1),0)$q$);
SELECT pg_temp.expect_error('producer_checkpoint_must_advance_bound', '23514', $q$
    UPDATE control.producer_checkpoints SET checkpoint_revision = 2,
        checkpoint_digest = pg_temp.digest(62) WHERE producer_slot_id = pg_temp.id(61)$q$);
UPDATE control.producer_checkpoints SET checkpoint_revision = 2,
    complete_through_request = 2,checkpoint_digest = pg_temp.digest(62),committed_at_us = 2
    WHERE producer_slot_id = pg_temp.id(61);
SELECT pg_temp.assert_true('producer_checkpoint_cas_advanced', EXISTS (
    SELECT 1 FROM control.producer_checkpoints WHERE producer_slot_id = pg_temp.id(61)
        AND checkpoint_revision = 2 AND complete_through_request = 2));

-- Frontier revisions are complete cuts with attestation per stream.
SELECT pg_temp.expect_error('frontier_unsealed_commit_refused', '23514', $q$
    INSERT INTO control.consumer_frontier_revisions(consumer_id,frontier_revision,dataset_id,
        consumer_role,topology_generation,previous_revision,owner_resource_id,owner_epoch,
        owner_incarnation,created_at_us)
    VALUES(pg_temp.id(70),2,pg_temp.id(1),'archive',0,1,pg_temp.id(40),1,pg_temp.id(41),1)$q$);
SELECT pg_temp.expect_error('frontier_missing_stream_refused', '23514', $q$
    INSERT INTO control.consumer_frontier_revisions(consumer_id,frontier_revision,dataset_id,
        consumer_role,topology_generation,previous_revision,owner_resource_id,owner_epoch,
        owner_incarnation,created_at_us)
    VALUES(pg_temp.id(70),2,pg_temp.id(1),'archive',0,1,pg_temp.id(40),1,pg_temp.id(41),1);
    INSERT INTO control.consumer_frontiers VALUES
        (pg_temp.id(70),2,pg_temp.id(1),0,0,1,0,NULL,NULL);
    UPDATE control.consumer_frontier_revisions SET sealed = true
        WHERE consumer_id = pg_temp.id(70) AND frontier_revision = 2$q$);
SELECT pg_temp.expect_error('frontier_regression_refused', '23514', $q$
    INSERT INTO control.consumer_frontier_revisions(consumer_id,frontier_revision,dataset_id,
        consumer_role,topology_generation,previous_revision,owner_resource_id,owner_epoch,
        owner_incarnation,created_at_us)
    VALUES(pg_temp.id(70),2,pg_temp.id(1),'archive',0,1,pg_temp.id(40),1,pg_temp.id(41),1);
    INSERT INTO control.consumer_frontiers VALUES
        (pg_temp.id(70),2,pg_temp.id(1),0,0,1,0,NULL,NULL),
        (pg_temp.id(70),2,pg_temp.id(1),0,7,1,4,'archive-batch',pg_temp.digest(76));
    UPDATE control.consumer_frontier_revisions SET sealed = true
        WHERE consumer_id = pg_temp.id(70) AND frontier_revision = 2$q$);
SELECT pg_temp.expect_error('frontier_attestation_kind_closed', '23514', $q$
    INSERT INTO control.consumer_frontier_revisions(consumer_id,frontier_revision,dataset_id,
        consumer_role,topology_generation,previous_revision,owner_resource_id,owner_epoch,
        owner_incarnation,created_at_us)
    VALUES(pg_temp.id(70),2,pg_temp.id(1),'archive',0,1,pg_temp.id(40),1,pg_temp.id(41),1);
    INSERT INTO control.consumer_frontiers VALUES
        (pg_temp.id(70),2,pg_temp.id(1),0,0,1,1,'counter-only',pg_temp.digest(75))$q$);
SELECT pg_temp.expect_error('zero_frontier_refuses_fabricated_receipt', '23514', $q$
    INSERT INTO control.consumer_frontier_revisions(consumer_id,frontier_revision,dataset_id,
        consumer_role,topology_generation,previous_revision,owner_resource_id,owner_epoch,
        owner_incarnation,created_at_us)
    VALUES(pg_temp.id(70),2,pg_temp.id(1),'archive',0,1,pg_temp.id(40),1,pg_temp.id(41),1);
    INSERT INTO control.consumer_frontiers VALUES
        (pg_temp.id(70),2,pg_temp.id(1),0,0,1,0,'archive-batch',pg_temp.digest(75))$q$);
SELECT pg_temp.expect_error('consumer_head_cannot_skip_revision', '23514', $q$
    UPDATE control.consumers SET current_revision = 3,current_topology_generation = 0
        WHERE consumer_id = pg_temp.id(70)$q$);

BEGIN;
INSERT INTO control.consumer_frontier_revisions(consumer_id,frontier_revision,dataset_id,
    consumer_role,topology_generation,previous_revision,owner_resource_id,owner_epoch,
    owner_incarnation,created_at_us)
VALUES(pg_temp.id(70),2,pg_temp.id(1),'archive',0,1,pg_temp.id(40),1,pg_temp.id(41),1);
INSERT INTO control.consumer_frontiers VALUES
    (pg_temp.id(70),2,pg_temp.id(1),0,0,1,0,NULL,NULL),
    (pg_temp.id(70),2,pg_temp.id(1),0,7,1,6,'archive-batch',pg_temp.digest(76));
UPDATE control.consumer_frontier_revisions SET sealed = true
    WHERE consumer_id = pg_temp.id(70) AND frontier_revision = 2;
UPDATE control.consumers SET current_revision = 2,current_topology_generation = 0
    WHERE consumer_id = pg_temp.id(70);
COMMIT;
SELECT pg_temp.assert_true('frontier_advances_atomically_with_zero_member', EXISTS (
    SELECT 1 FROM control.consumers WHERE consumer_id = pg_temp.id(70) AND current_revision = 2)
    AND (SELECT count(*) = 2 FROM control.consumer_frontiers
        WHERE consumer_id = pg_temp.id(70) AND frontier_revision = 2)
    AND EXISTS (SELECT 1 FROM control.consumer_frontiers
        WHERE consumer_id = pg_temp.id(70) AND frontier_revision = 2
            AND shard_id = 0 AND last_complete_sequence = 0));
SELECT pg_temp.expect_error('sealed_frontier_late_append_refused', '23514', $q$
    INSERT INTO control.consumer_frontiers VALUES
        (pg_temp.id(70),2,pg_temp.id(1),0,1,1,0,NULL,NULL)$q$);
SELECT pg_temp.expect_error('frontier_bound_update_refused', '23514', $q$
    UPDATE control.consumer_frontiers SET last_complete_sequence = 7
        WHERE consumer_id = pg_temp.id(70) AND frontier_revision = 2 AND shard_id = 7$q$);
SELECT pg_temp.expect_error('frontier_bound_delete_refused', '23514', $q$
    DELETE FROM control.consumer_frontiers
        WHERE consumer_id = pg_temp.id(70) AND frontier_revision = 2 AND shard_id = 7$q$);
BEGIN;
INSERT INTO control.consumer_frontier_revisions(consumer_id,frontier_revision,dataset_id,
    consumer_role,topology_generation,previous_revision,owner_resource_id,owner_epoch,
    owner_incarnation,created_at_us)
VALUES(pg_temp.id(70),3,pg_temp.id(1),'archive',0,2,pg_temp.id(40),1,pg_temp.id(41),2);
INSERT INTO control.consumer_frontiers VALUES
    (pg_temp.id(70),3,pg_temp.id(1),0,0,1,0,NULL,NULL),
    (pg_temp.id(70),3,pg_temp.id(1),0,7,1,6,'archive-batch',pg_temp.digest(78));
UPDATE control.consumer_frontier_revisions SET sealed = true
    WHERE consumer_id = pg_temp.id(70) AND frontier_revision = 3;
COMMIT;
SELECT pg_temp.expect_error('consumer_owner_cannot_abandon_unpublished_frontier', '23514', $q$
    UPDATE control.owner_epochs SET state = 'released',released_at_us = 2,
        exclusion_method = 'joined',exclusion_digest = pg_temp.digest(79)
        WHERE resource_id = pg_temp.id(40) AND epoch = 1$q$);
UPDATE control.owner_epochs SET state = 'draining'
    WHERE resource_id = pg_temp.id(40) AND epoch = 1;
UPDATE control.consumers SET current_revision = 3,current_topology_generation = 0
    WHERE consumer_id = pg_temp.id(70);
SELECT pg_temp.expect_error('draining_consumer_owner_cannot_start_frontier', '23514', $q$
    INSERT INTO control.consumer_frontier_revisions(consumer_id,frontier_revision,dataset_id,
        consumer_role,topology_generation,previous_revision,owner_resource_id,owner_epoch,
        owner_incarnation,created_at_us)
    VALUES(pg_temp.id(70),4,pg_temp.id(1),'archive',0,3,pg_temp.id(40),1,pg_temp.id(41),3);
    INSERT INTO control.consumer_frontiers VALUES
        (pg_temp.id(70),4,pg_temp.id(1),0,0,1,0,NULL,NULL),
        (pg_temp.id(70),4,pg_temp.id(1),0,7,1,6,'archive-batch',pg_temp.digest(81));
    UPDATE control.consumer_frontier_revisions SET sealed = true
        WHERE consumer_id = pg_temp.id(70) AND frontier_revision = 4$q$);
UPDATE control.owner_epochs SET state = 'released',released_at_us = 2,
    exclusion_method = 'joined',exclusion_digest = pg_temp.digest(79)
    WHERE resource_id = pg_temp.id(40) AND epoch = 1;
SELECT pg_temp.assert_true('draining_consumer_owner_publishes_before_release',
    EXISTS (SELECT 1 FROM control.consumers WHERE consumer_id = pg_temp.id(70)
        AND current_revision = 3)
    AND EXISTS (SELECT 1 FROM control.owner_epochs WHERE resource_id = pg_temp.id(40)
        AND epoch = 1 AND state = 'released'));

-- Projection chunk identity uses an open versioned target name and exact source cut.
SELECT pg_temp.expect_error('projection_target_spelling_bounded', '23514', $q$
    INSERT INTO control.projection_chunks(generation_id,chunk_id,dataset_id,storage_incarnation,
        table_identity_digest,target,target_schema_version,topology_generation,chunk_ordinal,
        row_count,owner_resource_id,owner_epoch,owner_incarnation,prepared_at_us)
    VALUES(pg_temp.id(72),pg_temp.digest(77),pg_temp.id(1),pg_temp.id(73),pg_temp.digest(72),
        'Event Rows',1,0,1,1,pg_temp.id(50),1,pg_temp.id(51),0)$q$);
SELECT pg_temp.expect_error('projection_chunk_duplicate_ordinal_refused', '23505', $q$
    INSERT INTO control.projection_chunks(generation_id,chunk_id,dataset_id,storage_incarnation,
        table_identity_digest,target,target_schema_version,topology_generation,chunk_ordinal,
        row_count,owner_resource_id,owner_epoch,owner_incarnation,prepared_at_us)
    VALUES(pg_temp.id(72),pg_temp.digest(77),pg_temp.id(1),pg_temp.id(73),pg_temp.digest(72),
        'events',1,0,0,1,pg_temp.id(50),1,pg_temp.id(51),0)$q$);
SELECT pg_temp.expect_error('projection_chunk_owner_bound_to_generation', '23503', $q$
    INSERT INTO control.projection_chunks(generation_id,chunk_id,dataset_id,storage_incarnation,
        table_identity_digest,target,target_schema_version,topology_generation,chunk_ordinal,
        row_count,owner_resource_id,owner_epoch,owner_incarnation,prepared_at_us)
    VALUES(pg_temp.id(72),pg_temp.digest(77),pg_temp.id(1),pg_temp.id(73),pg_temp.digest(72),
        'event_tags',1,0,1,1,pg_temp.id(52),1,pg_temp.id(53),0)$q$);
SELECT pg_temp.expect_error('projection_chunk_incomplete_cut_refused', '23514', $q$
    INSERT INTO control.projection_chunks(generation_id,chunk_id,dataset_id,storage_incarnation,
        table_identity_digest,target,target_schema_version,topology_generation,chunk_ordinal,
        row_count,owner_resource_id,owner_epoch,owner_incarnation,prepared_at_us)
    VALUES(pg_temp.id(72),pg_temp.digest(77),pg_temp.id(1),pg_temp.id(73),pg_temp.digest(72),
        'event_tags',1,0,1,1,pg_temp.id(50),1,pg_temp.id(51),0);
    INSERT INTO control.projection_chunk_bounds VALUES
        (pg_temp.id(72),pg_temp.digest(77),pg_temp.id(1),0,0,1,0);
    UPDATE control.projection_chunks SET state = 'committed',committed_at_us = 1,
        engine_receipt_digest = pg_temp.digest(78) WHERE chunk_id = pg_temp.digest(77)$q$);
SELECT pg_temp.expect_error('projection_chunk_identity_immutable', '23514', $q$
    UPDATE control.projection_chunks SET row_count = 1 WHERE chunk_id = pg_temp.digest(73)$q$);
SELECT pg_temp.expect_error('projection_chunk_delete_refused', '23514', $q$
    DELETE FROM control.projection_chunks WHERE chunk_id = pg_temp.digest(73)$q$);
SELECT pg_temp.expect_error('projection_bound_update_refused', '23514', $q$
    UPDATE control.projection_chunk_bounds SET last_complete_sequence = 1
        WHERE chunk_id = pg_temp.digest(73) AND shard_id = 0$q$);
SELECT pg_temp.expect_error('projection_generation_identity_immutable', '23514', $q$
    UPDATE control.projection_generations SET created_at_us = 1
        WHERE generation_id = pg_temp.id(72)$q$);

INSERT INTO control.projection_generations VALUES
    (pg_temp.id(74),pg_temp.id(1),pg_temp.id(75),pg_temp.digest(79),
        pg_temp.id(52),1,'index',pg_temp.id(53),0);
INSERT INTO control.projection_chunks(generation_id,chunk_id,dataset_id,storage_incarnation,
    table_identity_digest,target,target_schema_version,topology_generation,chunk_ordinal,
    row_count,owner_resource_id,owner_epoch,owner_incarnation,prepared_at_us)
VALUES(pg_temp.id(74),pg_temp.digest(79),pg_temp.id(1),pg_temp.id(75),pg_temp.digest(79),
    'event_tags',1,0,0,0,pg_temp.id(52),1,pg_temp.id(53),0);
UPDATE control.owner_epochs SET state = 'draining'
    WHERE resource_id = pg_temp.id(52) AND epoch = 1;
SELECT pg_temp.expect_error('draining_index_owner_cannot_start_chunk', '23514', $q$
    INSERT INTO control.projection_chunks(generation_id,chunk_id,dataset_id,storage_incarnation,
        table_identity_digest,target,target_schema_version,topology_generation,chunk_ordinal,
        row_count,owner_resource_id,owner_epoch,owner_incarnation,prepared_at_us)
    VALUES(pg_temp.id(74),pg_temp.digest(80),pg_temp.id(1),pg_temp.id(75),pg_temp.digest(79),
        'event_tags',1,0,1,0,pg_temp.id(52),1,pg_temp.id(53),1)$q$);
UPDATE control.projection_chunks SET state = 'invalid',invalid_reason = 'drained'
    WHERE generation_id = pg_temp.id(74) AND chunk_id = pg_temp.digest(79);
UPDATE control.owner_epochs SET state = 'released',released_at_us = 2,
    exclusion_method = 'joined',exclusion_digest = pg_temp.digest(80)
    WHERE resource_id = pg_temp.id(52) AND epoch = 1;
SELECT pg_temp.assert_true('draining_index_resolves_prepared_chunk_before_release',
    EXISTS (SELECT 1 FROM control.projection_chunks WHERE generation_id = pg_temp.id(74)
        AND chunk_id = pg_temp.digest(79) AND state = 'invalid')
    AND EXISTS (SELECT 1 FROM control.owner_epochs WHERE resource_id = pg_temp.id(52)
        AND epoch = 1 AND state = 'released'));

-- Lease expiry and retention release are separate explicit transitions.
SELECT pg_temp.expect_error('snapshot_holder_variant_is_exact', '23514', $q$
    INSERT INTO control.snapshot_leases(lease_id,dataset_id,consumer_id,frontier_revision,
        topology_generation,holder_kind,holder_job_id,holder_role_instance_id,holder_role,
        holder_incarnation,acquired_at_us,updated_at_us,expires_at_us)
    VALUES(pg_temp.id(83),pg_temp.id(1),pg_temp.id(70),2,0,'role_instance',pg_temp.id(20),
        pg_temp.id(13),'archive',pg_temp.id(81),0,0,10)$q$);
SELECT pg_temp.expect_error('snapshot_retired_role_holder_refused', '23514', $q$
    INSERT INTO control.snapshot_leases(lease_id,dataset_id,consumer_id,frontier_revision,
        topology_generation,holder_kind,holder_role_instance_id,holder_role,
        holder_incarnation,acquired_at_us,updated_at_us,expires_at_us)
    VALUES(pg_temp.id(83),pg_temp.id(1),pg_temp.id(70),2,0,'role_instance',
        pg_temp.id(15),'read',pg_temp.id(81),0,0,10)$q$);
SELECT pg_temp.expect_error('snapshot_lease_revision_cannot_skip', '23514', $q$
    UPDATE control.snapshot_leases SET lease_revision = 3,updated_at_us = 10,expires_at_us = 300
        WHERE lease_id = pg_temp.id(80)$q$);
SELECT pg_temp.expect_error('snapshot_renewal_must_extend_expiry', '23514', $q$
    UPDATE control.snapshot_leases SET lease_revision = 2,updated_at_us = 10,expires_at_us = 100
        WHERE lease_id = pg_temp.id(80)$q$);
SELECT pg_temp.expect_error('snapshot_release_requires_release_time', '23514', $q$
    UPDATE control.snapshot_leases SET lease_revision = 2,updated_at_us = 10,
        state = 'released',release_reason = 'completed'
        WHERE lease_id = pg_temp.id(80)$q$);
UPDATE control.snapshot_leases SET lease_revision = 2,updated_at_us = 10,expires_at_us = 300
    WHERE lease_id = pg_temp.id(80);
SELECT pg_temp.assert_true('snapshot_lease_renewed_by_cas', EXISTS (
    SELECT 1 FROM control.snapshot_leases WHERE lease_id = pg_temp.id(80)
        AND lease_revision = 2 AND expires_at_us = 300 AND state = 'active'));
SELECT pg_temp.expect_error('retention_claim_cannot_exceed_cut', '23514', $q$
    INSERT INTO control.retention_claims(claim_id,lease_id,dataset_id,topology_generation,
        shard_id,owner_epoch,first_sequence,last_sequence,purpose,created_at_us)
    VALUES(pg_temp.id(84),pg_temp.id(80),pg_temp.id(1),0,7,1,1,7,'backup',2)$q$);
SELECT pg_temp.expect_error('retention_claim_sequence_starts_at_one', '23514', $q$
    INSERT INTO control.retention_claims(claim_id,lease_id,dataset_id,topology_generation,
        shard_id,owner_epoch,first_sequence,last_sequence,purpose,created_at_us)
    VALUES(pg_temp.id(84),pg_temp.id(80),pg_temp.id(1),0,7,1,0,1,'backup',2)$q$);
SELECT pg_temp.expect_error('retention_claim_cannot_begin_released', '23514', $q$
    INSERT INTO control.retention_claims(claim_id,lease_id,dataset_id,topology_generation,
        shard_id,owner_epoch,first_sequence,last_sequence,purpose,created_at_us,
        state,released_at_us,release_reason)
    VALUES(pg_temp.id(84),pg_temp.id(80),pg_temp.id(1),0,7,1,1,5,'backup',2,
        'released',2,'completed')$q$);
SELECT pg_temp.expect_error('retention_claim_after_expiry_refused', '23514', $q$
    INSERT INTO control.retention_claims(claim_id,lease_id,dataset_id,topology_generation,
        shard_id,owner_epoch,first_sequence,last_sequence,purpose,created_at_us)
    VALUES(pg_temp.id(84),pg_temp.id(80),pg_temp.id(1),0,7,1,1,5,'export',300)$q$);
SELECT pg_temp.expect_error('retention_claim_identity_immutable', '23514', $q$
    UPDATE control.retention_claims SET last_sequence = 4 WHERE claim_id = pg_temp.id(82)$q$);
SELECT pg_temp.expect_error('retention_claim_delete_refused', '23514', $q$
    DELETE FROM control.retention_claims WHERE claim_id = pg_temp.id(82)$q$);

UPDATE control.snapshot_leases SET lease_revision = 3,updated_at_us = 400,
    state = 'released',released_at_us = 400,
    release_reason = 'completed' WHERE lease_id = pg_temp.id(80);
SELECT pg_temp.assert_true('lease_release_does_not_release_retention_claim',
    EXISTS (SELECT 1 FROM control.snapshot_leases WHERE lease_id = pg_temp.id(80)
        AND state = 'released')
    AND EXISTS (SELECT 1 FROM control.retention_claims WHERE claim_id = pg_temp.id(82)
        AND state = 'active'));
SELECT pg_temp.expect_error('released_lease_cannot_create_claim', '23514', $q$
    INSERT INTO control.retention_claims(claim_id,lease_id,dataset_id,topology_generation,
        shard_id,owner_epoch,first_sequence,last_sequence,purpose,created_at_us)
    VALUES(pg_temp.id(84),pg_temp.id(80),pg_temp.id(1),0,7,1,1,5,'export',21)$q$);
SELECT pg_temp.expect_error('snapshot_lease_delete_refused', '23514', $q$
    DELETE FROM control.snapshot_leases WHERE lease_id = pg_temp.id(80)$q$);
UPDATE control.retention_claims SET state = 'released',released_at_us = 401,
    release_reason = 'completed' WHERE claim_id = pg_temp.id(82);
SELECT pg_temp.assert_true('retention_claim_requires_explicit_release', EXISTS (
    SELECT 1 FROM control.retention_claims WHERE claim_id = pg_temp.id(82)
        AND state = 'released' AND release_reason = 'completed'));

-- Closed streams and producer epochs retain terminal evidence; lower claimed cuts fail.
SELECT pg_temp.expect_error('source_close_below_published_frontier_refused', '23514', $q$
    UPDATE control.source_streams SET state = 'closing' WHERE shard_id = 7;
    UPDATE control.source_streams SET state = 'closed',closed_at_us = 2,
        last_complete_sequence = 4,closure_digest = pg_temp.digest(80) WHERE shard_id = 7$q$);
UPDATE control.source_streams SET state = 'closing' WHERE shard_id = 7;
UPDATE control.source_streams SET state = 'closed',closed_at_us = 2,
    last_complete_sequence = 6,closure_digest = pg_temp.digest(80) WHERE shard_id = 7;
SELECT pg_temp.expect_error('new_topology_handoff_must_match_closed_source', '23514', $q$
    INSERT INTO control.topology_generations VALUES(pg_temp.id(1),1,0,3,false);
    INSERT INTO control.topology_streams VALUES
        (pg_temp.id(1),1,0,1,NULL),
        (pg_temp.id(1),1,7,1,5);
    UPDATE control.topology_generations SET sealed = true WHERE generation = 1$q$);
SELECT pg_temp.expect_error('closed_source_cannot_revive', '23514', $q$
    UPDATE control.source_streams SET state = 'open',closed_at_us = NULL,
        last_complete_sequence = NULL,closure_digest = NULL WHERE shard_id = 7$q$);

UPDATE control.producer_epochs SET state = 'closing'
    WHERE producer_slot_id = pg_temp.id(61) AND producer_epoch = 1;
UPDATE control.producer_epochs SET state = 'retired',retired_at_us = 3,
    closure_digest = pg_temp.digest(81)
    WHERE producer_slot_id = pg_temp.id(61) AND producer_epoch = 1;
SELECT pg_temp.expect_error('retired_producer_checkpoint_is_terminal', '23514', $q$
    UPDATE control.producer_checkpoints SET checkpoint_revision = 3,
        complete_through_request = 3,checkpoint_digest = pg_temp.digest(82)
        WHERE producer_slot_id = pg_temp.id(61)$q$);
INSERT INTO control.producer_epochs(producer_slot_id,producer_epoch,dataset_id,
    producer_role_instance_id,producer_role,producer_incarnation,owner_resource_id,
    owner_epoch,owner_incarnation,outstanding_request_limit,opened_at_us)
VALUES(pg_temp.id(61),2,pg_temp.id(1),pg_temp.id(12),'discoverer',pg_temp.id(63),
    pg_temp.id(30),1,pg_temp.id(31),2,4);
SELECT pg_temp.assert_true('producer_successor_follows_explicit_retirement', EXISTS (
    SELECT 1 FROM control.producer_epochs WHERE producer_slot_id = pg_temp.id(61)
        AND producer_epoch = 2 AND state = 'open'));

UPDATE control.allocation_domains SET state = 'closed',closed_at_us = 5,
    closure_digest = pg_temp.digest(83) WHERE allocation_domain = pg_temp.id(60);
SELECT pg_temp.expect_error('closed_allocation_domain_is_terminal', '23514', $q$
    UPDATE control.allocation_domains SET state = 'active',closed_at_us = NULL,
        closure_digest = NULL WHERE allocation_domain = pg_temp.id(60)$q$);

-- One deliberate multi-write failure proves the containing transaction restores
-- the prior checkpoint and leaves no partial claim.
CREATE TEMP TABLE before_whole_failure AS SELECT pg_temp.control_snapshot() AS rows;
\echo 'Expected SQLSTATE 23514 in the following deliberate failed transaction'
\set ON_ERROR_STOP off
BEGIN;
INSERT INTO control.producer_checkpoints VALUES
    (pg_temp.id(61),2,pg_temp.id(1),1,0,pg_temp.digest(90),6);
UPDATE control.producer_checkpoints SET checkpoint_revision = 2,
    checkpoint_digest = pg_temp.digest(91) WHERE producer_epoch = 2;
\set whole_failure_state :SQLSTATE
ROLLBACK;
\set ON_ERROR_STOP on
SELECT pg_temp.assert_true('whole_transaction_expected_sqlstate', :'whole_failure_state' = '23514');
SELECT pg_temp.assert_true('whole_transaction_rollback_is_exact',
    (SELECT rows = pg_temp.control_snapshot() FROM pg_temp.before_whole_failure));

SELECT pg_temp.assert_true('all_expected_cases_executed',
    (SELECT count(*) FROM pg_temp.fixture_results) = 73);
SELECT case_name, expected_sqlstate, observed_sqlstate
    FROM pg_temp.fixture_results ORDER BY case_name;
SELECT jsonb_build_object('fixture','control-schema-0002-ingest','result','PASS',
    'executed',(SELECT count(*) FROM pg_temp.fixture_results),
    'failed',0,'skipped',0,'not_run',0,
    'server_version',current_setting('server_version'),
    'migration_1_sha256',:'expected_migration_1_sha256',
    'migration_2_sha256',:'expected_migration_2_sha256') AS fixture_result;
