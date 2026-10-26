-- Independent synthetic SQL invariant cases, in a fresh owned PG16.15 database
-- migrated through0041 by the actual CLI. No physical CH proof is claimed here.
-- Run as the migration/fixture owner, never against application state. This
-- script uses no disabled trigger, session_replication_role, DDL bypass or fake
-- Rust proof constructor. Keep stdout/stderr and original failed transactions.
\set ON_ERROR_STOP on
\pset pager off
\if :{?expected_migration_41_sha256}
\else
\echo 'Missing -v expected_migration_41_sha256=<sha256 of0041_index_publication.sql>'
\quit 2
\endif
SET statement_timeout='10s';
SET lock_timeout='2s';
SET idle_in_transaction_session_timeout='30s';
CREATE TEMP TABLE results(case_name text PRIMARY KEY);
CREATE FUNCTION pg_temp.id(n integer) RETURNS bytea LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT decode(lpad(to_hex(n),32,'0'),'hex')
$$;
CREATE FUNCTION pg_temp.digest(n integer) RETURNS bytea LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT decode(lpad(to_hex(n),64,'0'),'hex')
$$;
CREATE FUNCTION pg_temp.assert_true(label text, result boolean) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    IF result IS DISTINCT FROM true THEN RAISE EXCEPTION 'assertion failed: %',label; END IF;
    INSERT INTO pg_temp.results VALUES(label);
END;
$$;
CREATE FUNCTION pg_temp.snapshot() RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE relation text; rows jsonb; result jsonb:='{}';
BEGIN
    FOREACH relation IN ARRAY ARRAY['datasets','role_instances','owner_epochs','topology_generations',
        'topology_streams','source_streams','consumers','consumer_frontier_revisions','consumer_frontiers',
        'projection_generations','projection_chunks','projection_chunk_bounds','index_owner_sessions',
        'projection_plans','projection_plan_chunks','index_frontier_plans'] LOOP
        EXECUTE format('SELECT coalesce(jsonb_agg(v ORDER BY v::text COLLATE "C"),''[]''::jsonb)
            FROM(SELECT to_jsonb(t) v FROM control.%I t)s',relation) INTO rows;
        result:=result||jsonb_build_object(relation,rows);
    END LOOP;
    RETURN result;
END;
$$;
CREATE FUNCTION pg_temp.expect_error(label text, expected text, command text) RETURNS void LANGUAGE plpgsql AS $$
DECLARE before_rows jsonb:=pg_temp.snapshot(); actual text;
BEGIN
    BEGIN
        EXECUTE command;
        SET CONSTRAINTS ALL IMMEDIATE;
    EXCEPTION WHEN OTHERS THEN GET STACKED DIAGNOSTICS actual=RETURNED_SQLSTATE;
    END;
    IF actual IS DISTINCT FROM expected THEN
        RAISE EXCEPTION 'case % expected %, received %',label,expected,coalesce(actual,'no error');
    END IF;
    IF pg_temp.snapshot() IS DISTINCT FROM before_rows THEN
        RAISE EXCEPTION 'refused case left changed rows: %',label;
    END IF;
    INSERT INTO pg_temp.results VALUES(label);
END;
$$;
SELECT pg_temp.assert_true('exact_pg_and_migration',current_setting('server_version_num')='160015'
    AND EXISTS(SELECT 1 FROM control.schema_versions WHERE migration=41
        AND name='0041_index_publication.sql' AND checksum=decode(:'expected_migration_41_sha256','hex')));
SELECT pg_temp.assert_true('fresh_database',NOT EXISTS(SELECT 1 FROM control.datasets));
SELECT pg_advisory_lock(1111651442,1129599564);
BEGIN;
INSERT INTO control.datasets(dataset_id,instance_id,created_at_us) VALUES(pg_temp.id(1),pg_temp.id(2),0);
INSERT INTO control.role_instances VALUES
    (pg_temp.id(10),pg_temp.id(1),'control',0,NULL),
    (pg_temp.id(11),pg_temp.id(1),'ingest',0,NULL),
    (pg_temp.id(12),pg_temp.id(1),'index',0,NULL);
INSERT INTO control.owner_epochs(resource_id,epoch,dataset_id,role,role_instance_id,process_incarnation,
    acquired_at_us,lease_expires_at_us,state,control_fence_version,control_backend_pid,control_backend_start)
    VALUES(pg_temp.id(20),1,pg_temp.id(1),'control',pg_temp.id(10),pg_temp.id(21),0,
        (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint+300000000,'active',1,pg_backend_pid(),
        (SELECT backend_start FROM pg_stat_activity WHERE pid=pg_backend_pid()));
INSERT INTO control.owner_epochs(resource_id,epoch,dataset_id,role,role_instance_id,process_incarnation,
    acquired_at_us,lease_expires_at_us,state,index_publication_protocol) VALUES
    (pg_temp.id(30),1,pg_temp.id(1),'ingest',pg_temp.id(11),pg_temp.id(31),0,
        (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint+300000000,'active',NULL),
    (pg_temp.id(50),1,pg_temp.id(1),'index',pg_temp.id(12),pg_temp.id(51),0,
        (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint+300000000,'active',1);
INSERT INTO control.topology_generations VALUES(pg_temp.id(1),0,NULL,0,false);
INSERT INTO control.topology_streams VALUES(pg_temp.id(1),0,0,1,NULL),(pg_temp.id(1),0,7,1,NULL);
INSERT INTO control.source_streams(dataset_id,shard_id,owner_epoch,owner_resource_id,owner_incarnation,opened_at_us)
    VALUES(pg_temp.id(1),0,1,pg_temp.id(30),pg_temp.id(31),0),
        (pg_temp.id(1),7,1,pg_temp.id(30),pg_temp.id(31),0);
UPDATE control.topology_generations SET sealed=true;
INSERT INTO control.consumers(consumer_id,dataset_id,consumer_role,created_at_us)
    VALUES(pg_temp.id(50),pg_temp.id(1),'index',0);
INSERT INTO control.projection_generations(generation_id,dataset_id,storage_incarnation,table_identity_digest,
    owner_resource_id,owner_epoch,owner_incarnation,created_at_us,index_publication_protocol,custody_binding_digest,provenance)
    VALUES(pg_temp.id(60),pg_temp.id(1),pg_temp.id(61),pg_temp.digest(62),pg_temp.id(50),1,pg_temp.id(51),0,1,pg_temp.digest(63),'off');
INSERT INTO control.index_owner_sessions(dataset_id,resource_id,owner_epoch,owner_incarnation,generation_id,
    issuer_resource,issuer_epoch,issuer_incarnation,recovered_revision)
    VALUES(pg_temp.id(1),pg_temp.id(50),1,pg_temp.id(51),pg_temp.id(60),pg_temp.id(20),1,pg_temp.id(21),0);
COMMIT;

SELECT pg_temp.expect_error('qualified_owner_cannot_omit_session','23514',$$
    INSERT INTO control.owner_epochs(resource_id,epoch,dataset_id,role,role_instance_id,process_incarnation,
        acquired_at_us,lease_expires_at_us,state,index_publication_protocol)
        VALUES(pg_temp.id(59),1,pg_temp.id(1),'index',pg_temp.id(12),pg_temp.id(58),0,
            (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint+300000000,'active',1)
$$);

CREATE FUNCTION pg_temp.prepare(revision integer,first_sequence integer,last_sequence integer,
    omitted text DEFAULT NULL,wrong_schema boolean DEFAULT false,bad_witness boolean DEFAULT false) RETURNS void LANGUAGE plpgsql AS $$
DECLARE target text; ordinal integer:=0; owner_number numeric; incarnation bytea;
BEGIN
    SELECT owner_epoch,owner_incarnation INTO STRICT owner_number,incarnation FROM control.index_owner_sessions
        ORDER BY owner_epoch DESC LIMIT 1;
    INSERT INTO control.projection_plans(generation_id,plan_digest,dataset_id,consumer_id,frontier_revision,
        expected_revision,topology_generation,source_shard,source_owner_epoch,source_first,source_last,
        source_batch_digest,origin_epoch,origin_incarnation,prepared_at_us)
        VALUES(pg_temp.id(60),pg_temp.digest(100+revision),pg_temp.id(1),pg_temp.id(50),revision,revision-1,
            0,7,1,first_sequence,last_sequence,pg_temp.digest(200+revision),owner_number,incarnation,0);
    FOREACH target IN ARRAY ARRAY['dictionaries','events','event_eligibility_facts','event_receive_facts',
        'event_id_index','event_author_index','event_time_index','event_tag_index','provenance_by_event','provenance_by_relay'] LOOP
        ordinal:=ordinal+1;
        IF target=omitted THEN CONTINUE; END IF;
        INSERT INTO control.projection_chunks(generation_id,chunk_id,dataset_id,storage_incarnation,
            table_identity_digest,target,target_schema_version,topology_generation,chunk_ordinal,row_count,
            owner_resource_id,owner_epoch,owner_incarnation,prepared_at_us)
            VALUES(pg_temp.id(60),pg_temp.digest(1000+revision*10+ordinal),pg_temp.id(1),pg_temp.id(61),pg_temp.digest(62),
                target,CASE WHEN wrong_schema THEN 2 ELSE 1 END,0,revision,CASE WHEN target='dictionaries' THEN 2 WHEN bad_witness AND target='provenance_by_event' THEN 1 ELSE 0 END,
                pg_temp.id(50),owner_number,incarnation,0);
        INSERT INTO control.projection_plan_chunks VALUES(pg_temp.id(60),pg_temp.digest(100+revision),target,
            pg_temp.digest(1000+revision*10+ordinal),'all',pg_temp.digest(2000+revision*10+ordinal));
        INSERT INTO control.projection_chunk_bounds VALUES
            (pg_temp.id(60),pg_temp.digest(1000+revision*10+ordinal),pg_temp.id(1),0,0,1,0),
            (pg_temp.id(60),pg_temp.digest(1000+revision*10+ordinal),pg_temp.id(1),0,7,1,last_sequence);
    END LOOP;
    UPDATE control.projection_plans SET sealed=true WHERE frontier_revision=revision;
END;
$$;
SELECT pg_temp.expect_error('missing_empty_family','23514',$$SELECT pg_temp.prepare(1,1,2,'provenance_by_relay')$$);
SELECT pg_temp.expect_error('wrong_schema','23514',$$SELECT pg_temp.prepare(1,1,2,NULL,true)$$);
SELECT pg_temp.expect_error('off_profile_forbids_witness_rows','23514',$$SELECT pg_temp.prepare(1,1,2,NULL,false,true)$$);
SELECT pg_temp.expect_error('source_hole','23514',$$SELECT pg_temp.prepare(1,2,3)$$);
BEGIN;
SELECT pg_temp.prepare(1,1,2);
COMMIT;
SELECT pg_temp.assert_true('prepared_all_ten_zero_outputs_retained',
    (SELECT count(*)=10 AND count(*) FILTER(WHERE row_count=0)=9 AND bool_and(state='prepared') FROM control.projection_chunks)
    AND (SELECT current_revision IS NULL FROM control.consumers WHERE consumer_id=pg_temp.id(50)));
SELECT pg_temp.expect_error('second_pending_plan','23514',$$SELECT pg_temp.prepare(2,3,4)$$);
SELECT pg_temp.expect_error('rewrite_sealed_source','23514',$$UPDATE control.projection_plans SET source_batch_digest=pg_temp.digest(999)$$);
SELECT pg_temp.expect_error('rewrite_membership','23514',$$UPDATE control.projection_plan_chunks SET partition_key='other'$$);
SELECT pg_temp.expect_error('rewrite_incarnation','23514',$$UPDATE control.projection_chunks SET storage_incarnation=pg_temp.id(99)$$);
SELECT pg_temp.expect_error('timeout_cannot_invalidate','23514',$$UPDATE control.projection_chunks SET state='invalid',invalid_reason='timeout'$$);
SELECT pg_temp.expect_error('partial_completion','23514',$$UPDATE control.projection_chunks SET state='committed',
    committed_at_us=1,engine_receipt_digest=pg_temp.digest(300) WHERE target='events'$$);
SELECT pg_temp.expect_error('arbitrary_attestation_is_not_a_complete_plan','P0002',$$
    INSERT INTO control.consumer_frontier_revisions(consumer_id,frontier_revision,dataset_id,consumer_role,
        topology_generation,previous_revision,owner_resource_id,owner_epoch,owner_incarnation,created_at_us)
        VALUES(pg_temp.id(50),1,pg_temp.id(1),'index',0,NULL,pg_temp.id(50),1,pg_temp.id(51),1);
    INSERT INTO control.consumer_frontiers VALUES(pg_temp.id(50),1,pg_temp.id(1),0,0,1,0,NULL,NULL),
        (pg_temp.id(50),1,pg_temp.id(1),0,7,1,2,'projection-chunk',pg_temp.digest(999));
    UPDATE control.consumer_frontier_revisions SET sealed=true;
    UPDATE control.consumers SET current_revision=1,current_topology_generation=0
$$);
SELECT pg_temp.expect_error('release_pending_without_successor','P0002',$$UPDATE control.owner_epochs SET state='released',
    released_at_us=1,exclusion_method='storage_fenced',exclusion_digest=pg_temp.digest(900) WHERE resource_id=pg_temp.id(50) AND epoch=1$$);

CREATE FUNCTION pg_temp.transfer(wrong_inventory boolean DEFAULT false) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
UPDATE control.owner_epochs SET state='released',released_at_us=1,exclusion_method='storage_fenced',
    exclusion_digest=pg_temp.digest(900) WHERE resource_id=pg_temp.id(50) AND epoch=1;
INSERT INTO control.role_instances VALUES(pg_temp.id(13),pg_temp.id(1),'index',1,NULL);
INSERT INTO control.owner_epochs(resource_id,epoch,dataset_id,role,role_instance_id,process_incarnation,
    acquired_at_us,lease_expires_at_us,state,index_publication_protocol)
    VALUES(pg_temp.id(50),2,pg_temp.id(1),'index',pg_temp.id(13),pg_temp.id(52),1,
        (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint+300000000,'active',1);
INSERT INTO control.index_owner_sessions(dataset_id,resource_id,owner_epoch,owner_incarnation,generation_id,
    issuer_resource,issuer_epoch,issuer_incarnation,previous_epoch,recovery_digest,recovered_revision,recovered_pending_plan)
    VALUES(pg_temp.id(1),pg_temp.id(50),2,pg_temp.id(52),pg_temp.id(60),pg_temp.id(20),1,pg_temp.id(21),1,
        pg_temp.digest(900),0,pg_temp.digest(CASE WHEN wrong_inventory THEN 999 ELSE 101 END));
END;
$$;
SELECT pg_temp.expect_error('recovery_cannot_omit_pending_plan','23514',$$SELECT pg_temp.transfer(true)$$);
CREATE TEMP TABLE original_prepared AS SELECT pg_temp.snapshot() AS original;
BEGIN;
SELECT pg_temp.transfer();
COMMIT;
SELECT pg_temp.assert_true('transfer_preserves_all_original_plan_bytes',
    (SELECT (pg_temp.snapshot()-ARRAY['owner_epochs','role_instances','index_owner_sessions'])
        = (original-ARRAY['owner_epochs','role_instances','index_owner_sessions']) FROM original_prepared));
SELECT pg_temp.expect_error('stale_owner_fenced','23514',$$SELECT control.require_index_owner(pg_temp.id(1),pg_temp.id(50),1,pg_temp.id(51))$$);
SELECT pg_temp.expect_error('expired_index_cannot_publish','23514',$$UPDATE control.owner_epochs SET lease_expires_at_us=0
    WHERE resource_id=pg_temp.id(50) AND epoch=2; SELECT control.require_index_owner(pg_temp.id(1),pg_temp.id(50),2,pg_temp.id(52))$$);
SELECT pg_temp.expect_error('expired_control_cannot_publish','23514',$$UPDATE control.owner_epochs SET lease_expires_at_us=0
    WHERE resource_id=pg_temp.id(20); SELECT control.require_index_owner(pg_temp.id(1),pg_temp.id(50),2,pg_temp.id(52))$$);
SELECT pg_advisory_unlock(1111651442,1129599564);
SELECT pg_temp.expect_error('actual_backend_lock_required','23514',$$SELECT control.require_index_owner(pg_temp.id(1),pg_temp.id(50),2,pg_temp.id(52))$$);
SELECT pg_advisory_lock(1111651442,1129599564);

CREATE FUNCTION pg_temp.publish(revision integer,skip_head boolean DEFAULT false,wrong_cut boolean DEFAULT false)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE owner_number numeric; incarnation bytea; last_sequence numeric;
BEGIN
    SELECT owner_epoch,owner_incarnation INTO STRICT owner_number,incarnation FROM control.index_owner_sessions ORDER BY owner_epoch DESC LIMIT 1;
    SELECT source_last INTO STRICT last_sequence FROM control.projection_plans WHERE frontier_revision=revision;
    UPDATE control.projection_chunks SET state='committed',committed_at_us=2,engine_receipt_digest=pg_temp.digest(300+revision)
        WHERE chunk_ordinal=revision;
    INSERT INTO control.consumer_frontier_revisions(consumer_id,frontier_revision,dataset_id,consumer_role,
        topology_generation,previous_revision,owner_resource_id,owner_epoch,owner_incarnation,created_at_us)
        VALUES(pg_temp.id(50),revision,pg_temp.id(1),'index',0,nullif(revision-1,0),pg_temp.id(50),owner_number,incarnation,2);
    INSERT INTO control.index_frontier_plans VALUES(pg_temp.id(50),revision,pg_temp.id(1),0,pg_temp.id(60),
        pg_temp.digest(100+revision),owner_number,incarnation,pg_temp.digest(300+revision),2);
    INSERT INTO control.consumer_frontiers VALUES
        (pg_temp.id(50),revision,pg_temp.id(1),0,0,1,0,NULL,NULL),
        (pg_temp.id(50),revision,pg_temp.id(1),0,7,1,last_sequence-CASE WHEN wrong_cut THEN 1 ELSE 0 END,'projection-chunk',pg_temp.digest(100+revision));
    UPDATE control.consumer_frontier_revisions SET sealed=true WHERE frontier_revision=revision;
    IF NOT skip_head THEN UPDATE control.consumers SET current_revision=revision,current_topology_generation=0; END IF;
END;
$$;
SELECT pg_temp.expect_error('head_and_completion_are_atomic','23514',$$SELECT pg_temp.publish(1,true)$$);
SELECT pg_temp.expect_error('frontier_cannot_shrink_the_complete_batch','23514',$$SELECT pg_temp.publish(1,false,true)$$);
BEGIN;
SELECT pg_temp.publish(1);
COMMIT;
SELECT pg_temp.assert_true('old_plan_published_by_current_owner',
    (SELECT count(*)=10 AND bool_and(owner_epoch=1 AND state='committed') FROM control.projection_chunks)
    AND (SELECT owner_epoch=2 AND plan_digest=pg_temp.digest(101) FROM control.index_frontier_plans)
    AND (SELECT current_revision=1 FROM control.consumers));
BEGIN;
SELECT pg_temp.prepare(2,3,4);
COMMIT;
SELECT pg_temp.assert_true('new_chunks_keep_new_origin',
    (SELECT count(*)=10 AND bool_and(owner_epoch=2 AND owner_incarnation=pg_temp.id(52))
        FROM control.projection_chunks WHERE chunk_ordinal=2)
    AND (SELECT owner_epoch=1 FROM control.projection_generations));
SELECT pg_temp.expect_error('source_close_cannot_erase_prepared_bound','23514',$$UPDATE control.source_streams SET state='closing' WHERE shard_id=7;
    UPDATE control.source_streams SET state='closed',
    closed_at_us=3,last_complete_sequence=3,closure_digest=pg_temp.digest(600) WHERE shard_id=7$$);
SELECT pg_temp.expect_error('publication_receipt_is_immutable','23514',$$UPDATE control.index_frontier_plans SET engine_receipt_digest=pg_temp.digest(999)$$);
SELECT pg_temp.expect_error('prepared_plan_cannot_be_deleted','23514',$$DELETE FROM control.projection_plans WHERE frontier_revision=2$$);

-- The original ownership oracle runs at schema2. Exercise its NULL-protocol
-- association at schema41 as well, with two independently live legacy owners.
-- Force deferred checks, then roll back these local rows to preserve the exact
-- qualified inventory consumed by index_publication_races.py. Capture only the
-- two completed assertions before rollback; any SQL error stops this script.
BEGIN;
INSERT INTO control.role_instances VALUES
    (pg_temp.id(140),pg_temp.id(1),'index',0,NULL),
    (pg_temp.id(141),pg_temp.id(1),'index',0,NULL);
INSERT INTO control.owner_epochs(resource_id,epoch,dataset_id,role,role_instance_id,process_incarnation,
    acquired_at_us,lease_expires_at_us,state,index_publication_protocol) VALUES
    (pg_temp.id(142),1,pg_temp.id(1),'index',pg_temp.id(140),pg_temp.id(143),0,
        (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint+300000000,'active',NULL),
    (pg_temp.id(144),1,pg_temp.id(1),'index',pg_temp.id(141),pg_temp.id(145),0,
        (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint+300000000,'active',NULL);
INSERT INTO control.projection_generations(generation_id,dataset_id,storage_incarnation,table_identity_digest,
    owner_resource_id,owner_epoch,owner_incarnation,created_at_us,index_publication_protocol,custody_binding_digest,provenance)
    VALUES(pg_temp.id(146),pg_temp.id(1),pg_temp.id(147),pg_temp.digest(148),
        pg_temp.id(142),1,pg_temp.id(143),0,NULL,NULL,NULL);
SELECT pg_temp.expect_error('legacy_wrong_generation_owner_at_41','23503',$$
    INSERT INTO control.projection_chunks(generation_id,chunk_id,dataset_id,storage_incarnation,
        table_identity_digest,target,target_schema_version,topology_generation,chunk_ordinal,row_count,
        owner_resource_id,owner_epoch,owner_incarnation,prepared_at_us)
        VALUES(pg_temp.id(146),pg_temp.digest(149),pg_temp.id(1),pg_temp.id(147),pg_temp.digest(148),
            'events',1,0,0,0,pg_temp.id(144),1,pg_temp.id(145),0)
$$);
INSERT INTO control.projection_chunks(generation_id,chunk_id,dataset_id,storage_incarnation,
    table_identity_digest,target,target_schema_version,topology_generation,chunk_ordinal,row_count,
    owner_resource_id,owner_epoch,owner_incarnation,prepared_at_us)
    VALUES(pg_temp.id(146),pg_temp.digest(149),pg_temp.id(1),pg_temp.id(147),pg_temp.digest(148),
        'events',1,0,0,0,pg_temp.id(142),1,pg_temp.id(143),0);
SET CONSTRAINTS ALL IMMEDIATE;
SELECT pg_temp.assert_true('legacy_same_generation_owner_at_41',
    (SELECT count(*)=1 AND bool_and(chunk_id=pg_temp.digest(149) AND state='prepared'
        AND owner_resource_id=pg_temp.id(142) AND owner_epoch=1 AND owner_role='index'
        AND owner_incarnation=pg_temp.id(143)) FROM control.projection_chunks WHERE generation_id=pg_temp.id(146))
    AND EXISTS(SELECT 1 FROM control.projection_generations WHERE generation_id=pg_temp.id(146)
        AND owner_resource_id=pg_temp.id(142) AND owner_epoch=1 AND owner_role='index'
        AND owner_incarnation=pg_temp.id(143) AND index_publication_protocol IS NULL));
SELECT count(*) FILTER(WHERE case_name='legacy_wrong_generation_owner_at_41')=1 AS legacy_wrong_owner_checked,
    count(*) FILTER(WHERE case_name='legacy_same_generation_owner_at_41')=1 AS legacy_same_owner_checked
    FROM pg_temp.results \gset
ROLLBACK;
SELECT pg_temp.assert_true('legacy_wrong_generation_owner_at_41', :'legacy_wrong_owner_checked'::boolean);
SELECT pg_temp.assert_true('legacy_same_generation_owner_at_41', :'legacy_same_owner_checked'::boolean);

SELECT jsonb_build_object('format','bigbrotr-index-publication-sql-cases/1','status','PASS',
    'case_count',count(*),'cases',jsonb_agg(case_name ORDER BY case_name),
    'physical_evidence','NOT_RUN','concurrent_after_wait_lease','NOT_RUN') FROM pg_temp.results;
SELECT pg_advisory_unlock(1111651442,1129599564);
