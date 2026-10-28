-- SQL coordination controls only, on a disposable clone of the maintained
-- provenance-on installed journey. The real seed has source cuts12/10, four
-- published epoch1 Index batches and a closed activated epoch2. Its retirement
-- is either genuinely pending or complete, selected by expected_terminal.
-- No native/CH proof, lifecycle rewind, trigger bypass or source mutation.
\set ON_ERROR_STOP on
\pset pager off
SET statement_timeout='10s';
SET lock_timeout='2s';
SET idle_in_transaction_session_timeout='30s';
SET max_parallel_workers_per_gather=0;
CREATE TEMP TABLE results(name text PRIMARY KEY, classification text, state text, message text);
CREATE TEMP TABLE snapshots(name text PRIMARY KEY, value jsonb);
CREATE TEMP TABLE context AS
SELECT g.dataset_id,g.owner_resource_id AS consumer,g.generation_id,s.owner_resource_id AS ingest,
    s.shard_id,s.last_complete_sequence AS old_through,s.handoff_sha256 AS succession_digest,
    c.current_revision AS old_revision, :'expected_terminal'::boolean AS terminal
FROM control.projection_generations g JOIN control.consumers c ON c.consumer_id=g.owner_resource_id
JOIN control.initial_ingest_successions s ON s.dataset_id=g.dataset_id;

CREATE FUNCTION pg_temp.snapshot() RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE names text[]; relation text; part jsonb; answer jsonb:='{}';
BEGIN
    SELECT array_agg(tablename ORDER BY tablename) INTO names FROM pg_tables WHERE schemaname='control';
    IF cardinality(names)<>82 THEN RAISE EXCEPTION 'exact current82-table inventory required'; END IF;
    FOREACH relation IN ARRAY names LOOP
        EXECUTE format('SELECT coalesce(jsonb_agg(v ORDER BY v::text COLLATE "C"),''[]'') FROM '
            '(SELECT to_jsonb(t)||jsonb_build_object(''xmin'',xmin::text) v FROM control.%I t) q',relation) INTO part;
        answer:=answer||jsonb_build_object(relation,part);
        IF octet_length(answer::text)>4194304 THEN RAISE EXCEPTION 'SQL snapshot4MiB ceiling'; END IF;
    END LOOP;
    RETURN answer;
END $$;
CREATE FUNCTION pg_temp.assert_true(label text, answer boolean) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    IF answer IS DISTINCT FROM true THEN RAISE EXCEPTION 'fixture assertion failed: %',label; END IF;
    INSERT INTO results VALUES(label,'positive SQL observation',NULL,NULL);
END $$;
CREATE FUNCTION pg_temp.expect_error(label text, command text, expected_message text,
    classification text, expected_constraint text DEFAULT NULL, expected_state text DEFAULT '23514') RETURNS void LANGUAGE plpgsql AS $$
DECLARE before_rows jsonb:=pg_temp.snapshot(); state text; message text; constraint_name text;
BEGIN
    BEGIN
        EXECUTE command;
        SET CONSTRAINTS ALL IMMEDIATE;
        RAISE EXCEPTION 'unexpected fixture success' USING ERRCODE='ZX999';
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS state=RETURNED_SQLSTATE,message=MESSAGE_TEXT,constraint_name=CONSTRAINT_NAME;
    END;
    IF state IS DISTINCT FROM expected_state OR (expected_message IS NOT NULL AND message IS DISTINCT FROM expected_message)
        OR (expected_constraint IS NOT NULL AND constraint_name IS DISTINCT FROM expected_constraint) THEN
        RAISE EXCEPTION 'case % unexpected refusal %/%/%',label,state,message,constraint_name;
    END IF;
    IF pg_temp.snapshot() IS DISTINCT FROM before_rows THEN RAISE EXCEPTION 'case % changed retained rows',label; END IF;
    INSERT INTO results VALUES(label,classification,state,message);
END $$;
CREATE FUNCTION pg_temp.now_us() RETURNS bigint LANGUAGE sql VOLATILE AS $$
    SELECT (extract(epoch FROM clock_timestamp())*1000000)::bigint
$$;
CREATE FUNCTION pg_temp.u64(value numeric) RETURNS bytea LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE answer text:=''; n numeric:=value;
BEGIN
    FOR i IN 1..8 LOOP answer:=lpad(to_hex(mod(n,256)::integer),2,'0')||answer; n:=div(n,256); END LOOP;
    IF n<>0 OR value<0 OR trunc(value)<>value THEN RAISE EXCEPTION 'fixture unsigned integer overflow'; END IF;
    RETURN decode(answer,'hex');
END $$;

SELECT pg_temp.assert_true('exact_current_seed',
    current_setting('server_version_num')='160015'
    AND (SELECT count(*)=1 FROM context)
    AND (SELECT old_revision=4 AND old_through=12 AND shard_id=0 FROM context)
    AND (SELECT count(*)=1 AND bool_and(index_publication_protocol=1 AND provenance='local_first_witness') FROM control.projection_generations)
    AND (SELECT count(*)=4 AND bool_and(sealed) FROM control.projection_plans)
    AND (SELECT count(*)=40 AND bool_and(state='committed') FROM control.projection_chunks)
    AND (SELECT count(*)=4 FROM control.index_frontier_plans)
    AND NOT EXISTS(SELECT 1 FROM control.index_frontier_handoffs)
    AND (SELECT count(*)=1 AND bool_and(owner_epoch=1) FROM control.index_owner_sessions)
    AND (SELECT count(*)=2 AND bool_and(state='closed') FROM control.source_streams)
    AND EXISTS(SELECT 1 FROM control.source_streams WHERE owner_epoch=2 AND last_complete_sequence=10)
    -- ACL coverage only: this migrator fixture does not invoke as the application.
    AND EXISTS(SELECT 1 FROM pg_proc p WHERE p.oid='control.require_index_handoff_metadata(bytea,numeric)'::regprocedure
        AND NOT p.prosecdef AND p.proowner<>'bigbrotr_control'::regrole
        AND has_function_privilege('bigbrotr_control',p.oid,'EXECUTE')
        AND NOT has_function_privilege('bigbrotr_control',p.oid,'EXECUTE WITH GRANT OPTION')
        AND NOT EXISTS(SELECT 1 FROM aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a WHERE a.grantee=0))
    AND NOT has_function_privilege('bigbrotr_control','control.guard_index_frontier_handoff()','EXECUTE')
    AND NOT has_function_privilege('bigbrotr_control','control.require_typed_index_frontier()','EXECUTE')
    AND EXISTS(SELECT 1 FROM control.schema_versions WHERE migration=41
        AND checksum=decode(:'expected_migration_41_sha256','hex')));
INSERT INTO snapshots VALUES('seed',pg_temp.snapshot());

-- A real fresh backend-bound Control claim in this clone. Index transfers below
-- are explicitly synthetic SQL records, never private application capabilities.
SELECT pg_advisory_lock(1111651442,1129599564);
BEGIN;
DO $$
DECLARE old control.owner_epochs%ROWTYPE; at_us bigint:=pg_temp.now_us();
        incarnation bytea:=uuid_send(gen_random_uuid()); role_id bytea:=uuid_send(gen_random_uuid());
BEGIN
    PERFORM control.retain_control_lock();
    SELECT * INTO STRICT old FROM control.owner_epochs WHERE role='control' ORDER BY epoch DESC LIMIT 1 FOR UPDATE;
    IF old.state<>'released' OR EXISTS(SELECT 1 FROM control.owner_epochs WHERE role='control' AND state='active') THEN
        RAISE EXCEPTION 'seed requires released prior Control';
    END IF;
    INSERT INTO control.role_instances VALUES(role_id,old.dataset_id,'control',at_us,NULL);
    INSERT INTO control.owner_epochs(resource_id,epoch,dataset_id,role,role_instance_id,process_incarnation,
        acquired_at_us,lease_expires_at_us,state,control_fence_version,control_backend_pid,control_backend_start)
        VALUES(old.resource_id,old.epoch+1,old.dataset_id,'control',role_id,incarnation,at_us,at_us+60000000,'active',1,
            pg_backend_pid(),(SELECT backend_start FROM pg_stat_activity WHERE pid=pg_backend_pid()));
END $$;
COMMIT;
SELECT pg_temp.assert_true('actual_terminal_classification',
    (SELECT (control.require_activated_initial_retirement(ingest) IS NOT NULL)=terminal FROM context));

CREATE FUNCTION pg_temp.rebind(wrong_handoff boolean DEFAULT false) RETURNS void LANGUAGE plpgsql AS $$
DECLARE old control.owner_epochs%ROWTYPE; c control.owner_epochs%ROWTYPE; x context%ROWTYPE;
    at_us bigint:=pg_temp.now_us(); role_id bytea:=uuid_send(gen_random_uuid()); head numeric;
    plan bytea; handoff bytea; observation bytea:=sha256(convert_to('SQL-only fixture observation','UTF8'));
BEGIN
    SELECT * INTO STRICT x FROM context;
    SELECT * INTO STRICT old FROM control.owner_epochs WHERE resource_id=x.consumer ORDER BY epoch DESC LIMIT 1 FOR UPDATE;
    SELECT * INTO STRICT c FROM control.owner_epochs WHERE role='control' AND state='active';
    SELECT current_revision INTO STRICT head FROM control.consumers WHERE consumer_id=x.consumer FOR UPDATE;
    SELECT plan_digest INTO plan FROM control.index_frontier_plans WHERE consumer_id=x.consumer AND frontier_revision=head;
    SELECT handoff_digest INTO handoff FROM control.index_frontier_handoffs WHERE consumer_id=x.consumer AND frontier_revision=head;
    IF old.state<>'released' THEN
        UPDATE control.owner_epochs SET state='released',released_at_us=at_us,exclusion_method='storage_fenced',exclusion_digest=observation
            WHERE resource_id=old.resource_id AND epoch=old.epoch;
    END IF;
    UPDATE control.role_instances SET retired_at_us=at_us WHERE role_instance_id=old.role_instance_id AND retired_at_us IS NULL;
    INSERT INTO control.role_instances VALUES(role_id,x.dataset_id,'index',at_us,NULL);
    INSERT INTO control.owner_epochs(resource_id,epoch,dataset_id,role,role_instance_id,process_incarnation,
        acquired_at_us,lease_expires_at_us,state,index_publication_protocol)
        VALUES(x.consumer,old.epoch+1,x.dataset_id,'index',role_id,c.process_incarnation,at_us,c.lease_expires_at_us,'active',1);
    INSERT INTO control.index_owner_sessions(dataset_id,resource_id,owner_epoch,owner_incarnation,generation_id,
        issuer_resource,issuer_epoch,issuer_incarnation,previous_epoch,recovery_digest,recovered_revision,recovered_plan,recovered_handoff)
        VALUES(x.dataset_id,x.consumer,old.epoch+1,c.process_incarnation,x.generation_id,c.resource_id,c.epoch,c.process_incarnation,
            old.epoch,observation,head,plan,CASE WHEN wrong_handoff THEN sha256('wrong'::bytea) ELSE handoff END);
END $$;
BEGIN;
SELECT pg_temp.rebind();
COMMIT;
INSERT INTO snapshots VALUES('before_bridge',pg_temp.snapshot());

-- Independent literal handoff hash recipe. Historical generation/publisher bytes
-- come from the untouched seed; no product encoder or observed bridge is used.
CREATE FUNCTION pg_temp.handoff_digest() RETURNS bytea LANGUAGE plpgsql AS $$
DECLARE x context%ROWTYPE; g control.projection_generations%ROWTYPE; prefix bytea; old_bound bytea;
BEGIN
    SELECT * INTO STRICT x FROM context;
    SELECT * INTO STRICT g FROM control.projection_generations WHERE generation_id=x.generation_id;
    prefix:=convert_to('bigbrotr/index-initial-topology-handoff/1','UTF8')||decode('00','hex')
        ||x.dataset_id||x.consumer||x.generation_id||g.storage_incarnation||x.ingest
        ||g.table_identity_digest||g.custody_binding_digest||decode('01','hex')
        ||pg_temp.u64(4)||x.succession_digest;
    old_bound:=substring(pg_temp.u64(x.shard_id) FROM 5)||pg_temp.u64(1)||pg_temp.u64(12);
    RETURN sha256(prefix||pg_temp.u64(0)||int4send(1)||old_bound||pg_temp.u64(1)||int4send(2)
        ||old_bound||substring(pg_temp.u64(x.shard_id) FROM 5)||pg_temp.u64(2)||pg_temp.u64(0));
END $$;

-- An unsealed competing plan intent is sufficient to exercise both insertion
-- orders. It is never claimed to be a valid completed/prepared native batch.
CREATE FUNCTION pg_temp.competing_plan() RETURNS void LANGUAGE plpgsql AS $$
DECLARE x context%ROWTYPE; s control.index_owner_sessions%ROWTYPE;
BEGIN
    SELECT * INTO STRICT x FROM context;
    SELECT * INTO STRICT s FROM control.index_owner_sessions WHERE resource_id=x.consumer ORDER BY owner_epoch DESC LIMIT 1;
    INSERT INTO control.projection_plans(generation_id,plan_digest,dataset_id,consumer_id,frontier_revision,expected_revision,
        topology_generation,source_shard,source_owner_epoch,source_first,source_last,source_batch_digest,origin_epoch,origin_incarnation,prepared_at_us)
        VALUES(x.generation_id,sha256('competing plan'::bytea),x.dataset_id,x.consumer,5,4,0,x.shard_id,1,13,13,
            sha256('synthetic source'::bytea),s.owner_epoch,s.owner_incarnation,pg_temp.now_us());
END $$;
CREATE FUNCTION pg_temp.bridge(mode text DEFAULT 'complete') RETURNS void LANGUAGE plpgsql AS $$
DECLARE x context%ROWTYPE; s control.index_owner_sessions%ROWTYPE; at_us bigint:=pg_temp.now_us(); expected numeric:=4;
BEGIN
    SELECT * INTO STRICT x FROM context;
    SELECT * INTO STRICT s FROM control.index_owner_sessions WHERE resource_id=x.consumer ORDER BY owner_epoch DESC LIMIT 1;
    IF mode='stale-owner' THEN SELECT * INTO STRICT s FROM control.index_owner_sessions WHERE resource_id=x.consumer AND owner_epoch=1; END IF;
    IF mode='stale-head' THEN expected:=3; END IF;
    IF mode='competing-before' THEN PERFORM pg_temp.competing_plan(); END IF;
    INSERT INTO control.consumer_frontier_revisions(consumer_id,frontier_revision,dataset_id,consumer_role,topology_generation,
        previous_revision,owner_resource_id,owner_epoch,owner_incarnation,created_at_us)
        VALUES(x.consumer,5,x.dataset_id,'index',1,expected,x.consumer,s.owner_epoch,s.owner_incarnation,at_us);
    IF mode<>'missing-receipt' THEN
      INSERT INTO control.index_frontier_handoffs(consumer_id,frontier_revision,expected_revision,dataset_id,topology_generation,
        generation_id,handoff_digest,ingest_resource,succession_digest,source_shard,predecessor_through,owner_epoch,owner_incarnation,committed_at_us)
        VALUES(x.consumer,5,4,x.dataset_id,1,x.generation_id,pg_temp.handoff_digest(),x.ingest,
            CASE WHEN mode='wrong-succession' THEN sha256('wrong'::bytea) ELSE x.succession_digest END,x.shard_id,12,
            s.owner_epoch,s.owner_incarnation,at_us);
    END IF;
    IF mode='fake-batch-overlap' THEN
        INSERT INTO control.index_frontier_plans SELECT consumer_id,5,dataset_id,topology_generation,generation_id,
            plan_digest,s.owner_epoch,s.owner_incarnation,engine_receipt_digest,committed_at_us
        FROM control.index_frontier_plans WHERE consumer_id=x.consumer AND frontier_revision=4;
    END IF;
    IF mode='competing-after' THEN PERFORM pg_temp.competing_plan(); END IF;
    IF mode<>'missing-old-bound' THEN
        INSERT INTO control.consumer_frontiers
            SELECT consumer_id,5,dataset_id,1,shard_id,owner_epoch,
                CASE WHEN mode='changed-old-cut' THEN 11 ELSE last_complete_sequence END,
                CASE WHEN mode='missing-attestation' THEN NULL ELSE attestation_kind END,
                CASE WHEN mode='missing-attestation' THEN NULL WHEN mode='changed-attestation' THEN sha256('wrong'::bytea) ELSE attestation_digest END
            FROM control.consumer_frontiers WHERE consumer_id=x.consumer AND frontier_revision=4;
    END IF;
    IF mode<>'missing-new-bound' THEN
        INSERT INTO control.consumer_frontiers VALUES(x.consumer,5,x.dataset_id,1,x.shard_id,2,0,NULL,NULL);
    END IF;
    UPDATE control.consumer_frontier_revisions SET sealed=true WHERE consumer_id=x.consumer AND frontier_revision=5;
    IF mode<>'missing-head' THEN UPDATE control.consumers SET current_revision=5,current_topology_generation=1 WHERE consumer_id=x.consumer; END IF;
    -- Select the new typed constraint explicitly for discriminating error order;
    -- the positive transaction subsequently forces every remaining constraint.
    SET CONSTRAINTS control.typed_index_frontier IMMEDIATE;
END $$;

\if :expected_terminal
SELECT pg_temp.expect_error('stale_owner',$$SELECT pg_temp.bridge('stale-owner')$$,
    'Control owner belongs to another backend or is inactive','earlier current Control authority guard for historical Index issuer');
SELECT pg_temp.expect_error('stale_head',$$SELECT pg_temp.bridge('stale-head')$$,
    'frontier revision must extend the currently published head','earlier frontier guard');
SELECT pg_temp.expect_error('competing_plan_before_handoff',$$SELECT pg_temp.bridge('competing-before')$$,
    'Index handoff requires its exact completed predecessor and live next revision','direct handoff insert guard; unsealed competing intent');
SELECT pg_temp.expect_error('competing_plan_after_handoff',$$SELECT pg_temp.bridge('competing-after')$$,
    'Index handoff must preserve the exact closed prefix and add only successor zero','direct typed handoff guard; unsealed competing intent');
SELECT pg_temp.expect_error('fake_batch_handoff_overlap',$$SELECT pg_temp.bridge('fake-batch-overlap')$$,
    'Index publication requires all exact completed targets','earlier complete batch receipt guard');
SELECT pg_temp.expect_error('missing_typed_receipt',$$SELECT pg_temp.bridge('missing-receipt')$$,
    'query returned no rows','typed revision requires exactly one receipt',NULL,'P0002');
SELECT pg_temp.expect_error('wrong_succession_digest',$$SELECT pg_temp.bridge('wrong-succession')$$,
    'Index handoff requires exact terminal initial succession and topology inventory','direct typed handoff guard');
SELECT pg_temp.expect_error('changed_predecessor_cut',$$SELECT pg_temp.bridge('changed-old-cut')$$,
    'Index handoff must preserve the exact closed prefix and add only successor zero','direct typed handoff guard');
SELECT pg_temp.expect_error('changed_predecessor_attestation',$$SELECT pg_temp.bridge('changed-attestation')$$,
    'Index handoff must preserve the exact closed prefix and add only successor zero','direct typed handoff guard');
SELECT pg_temp.expect_error('missing_predecessor_attestation',$$SELECT pg_temp.bridge('missing-attestation')$$,
    NULL,'earlier bound check', 'consumer_frontiers_check');
SELECT pg_temp.expect_error('missing_predecessor_bound',$$SELECT pg_temp.bridge('missing-old-bound')$$,
    'Index handoff must preserve the exact closed prefix and add only successor zero','direct typed handoff guard');
SELECT pg_temp.expect_error('missing_successor_bound',$$SELECT pg_temp.bridge('missing-new-bound')$$,
    'Index handoff must preserve the exact closed prefix and add only successor zero','direct typed handoff guard');
SELECT pg_temp.expect_error('missing_atomic_head',$$SELECT pg_temp.bridge('missing-head')$$,
    'Index handoff lacks atomic head publication','direct typed handoff guard');
BEGIN;
SELECT pg_temp.bridge();
SET CONSTRAINTS ALL IMMEDIATE;
COMMIT;
SELECT pg_temp.assert_true('positive_bridge_committed',
    (SELECT count(*)=1 AND bool_and(frontier_revision=5 AND expected_revision=4 AND owner_epoch=2 AND handoff_digest=pg_temp.handoff_digest()) FROM control.index_frontier_handoffs)
    AND (SELECT current_revision=5 AND current_topology_generation=1 FROM control.consumers WHERE consumer_role='index')
    AND NOT EXISTS(SELECT 1 FROM control.projection_plans WHERE frontier_revision=5)
    AND NOT EXISTS(SELECT 1 FROM control.index_frontier_plans WHERE frontier_revision=5));
INSERT INTO snapshots VALUES('published',pg_temp.snapshot());
SELECT pg_temp.assert_true('all_prior_rows_and_attestation_preserved',
    (SELECT (value-ARRAY['consumers','consumer_frontier_revisions','consumer_frontiers','index_frontier_handoffs'])
        = (pg_temp.snapshot()-ARRAY['consumers','consumer_frontier_revisions','consumer_frontiers','index_frontier_handoffs'])
        FROM snapshots WHERE name='before_bridge')
    AND (SELECT count(*)=1 FROM control.consumer_frontiers old JOIN control.consumer_frontiers new
        USING(consumer_id,shard_id,owner_epoch) WHERE old.consumer_id=(SELECT consumer FROM context)
            AND old.frontier_revision=4 AND new.frontier_revision=5 AND new.last_complete_sequence=old.last_complete_sequence
            AND new.attestation_kind=old.attestation_kind AND new.attestation_digest=old.attestation_digest)
    AND (SELECT (pg_temp.snapshot()->'consumer_frontier_revisions') @> (value->'consumer_frontier_revisions')
        AND jsonb_array_length(pg_temp.snapshot()->'consumer_frontier_revisions')=jsonb_array_length(value->'consumer_frontier_revisions')+1
        AND (pg_temp.snapshot()->'consumer_frontiers') @> (value->'consumer_frontiers')
        AND jsonb_array_length(pg_temp.snapshot()->'consumer_frontiers')=jsonb_array_length(value->'consumer_frontiers')+2
        FROM snapshots WHERE name='before_bridge')
    AND (SELECT bool_and(EXISTS(SELECT 1 FROM jsonb_array_elements(pg_temp.snapshot()->'consumers') observed
        WHERE CASE WHEN prior->>'consumer_role'='index'
            THEN observed-ARRAY['current_revision','current_topology_generation','xmin']=prior-ARRAY['current_revision','current_topology_generation','xmin']
            ELSE observed=prior END))
        FROM snapshots s CROSS JOIN LATERAL jsonb_array_elements(s.value->'consumers') prior WHERE s.name='before_bridge'));
SELECT pg_temp.expect_error('immutable_handoff_update',$$UPDATE control.index_frontier_handoffs SET committed_at_us=committed_at_us+1$$,
    'Index handoff receipts are immutable','handoff immutable guard');
SELECT pg_temp.expect_error('immutable_handoff_delete',$$DELETE FROM control.index_frontier_handoffs$$,
    'Index handoff receipts are immutable','handoff immutable guard');
SELECT pg_temp.expect_error('immutable_handoff_truncate',$$TRUNCATE control.index_frontier_handoffs$$,
    'Index handoff receipts are immutable','handoff immutable guard');
SELECT pg_temp.expect_error('wrong_handoff_recovery_capture',$$SELECT pg_temp.rebind(true)$$,
    'Index recovery inventory changed','session capture guard');
BEGIN;
SELECT pg_temp.rebind();
COMMIT;
SELECT pg_temp.assert_true('retry_captures_typed_handoff_head',
    EXISTS(SELECT 1 FROM control.index_owner_sessions WHERE owner_epoch=3 AND recovered_revision=5
        AND recovered_plan IS NULL AND recovered_handoff=pg_temp.handoff_digest() AND recovered_pending_plan IS NULL)
    AND (SELECT (value-ARRAY['owner_epochs','role_instances','index_owner_sessions'])
        = (pg_temp.snapshot()-ARRAY['owner_epochs','role_instances','index_owner_sessions']) FROM snapshots WHERE name='published'));
SELECT control.require_index_handoff_metadata((SELECT consumer FROM context),5);
SELECT pg_temp.assert_true('historical_handoff_replays_after_publisher_release',
    EXISTS(SELECT 1 FROM control.owner_epochs WHERE resource_id=(SELECT consumer FROM context) AND epoch=2 AND state='released')
    AND EXISTS(SELECT 1 FROM control.index_frontier_handoffs WHERE owner_epoch=2));
\else
SELECT pg_temp.expect_error('nonterminal_successor_refuses_bridge',$$SELECT pg_temp.bridge()$$,
    'Index handoff requires exact terminal initial succession and topology inventory','direct typed handoff guard; genuine closed unretired successor');
\endif

BEGIN;
DO $$
DECLARE c control.owner_epochs%ROWTYPE; at_us bigint:=pg_temp.now_us();
BEGIN
    SELECT * INTO STRICT c FROM control.owner_epochs WHERE role='control' AND state='active';
    PERFORM control.require_control_owner(c.dataset_id,c.resource_id,c.epoch,c.process_incarnation);
    UPDATE control.owner_epochs SET state='released',released_at_us=at_us,exclusion_method='storage_fenced',
        exclusion_digest=sha256(c.process_incarnation) WHERE resource_id=c.resource_id AND epoch=c.epoch;
    UPDATE control.role_instances SET retired_at_us=at_us WHERE role_instance_id=c.role_instance_id;
END $$;
COMMIT;
SELECT jsonb_build_object('format','bigbrotr-index-handoff-sql-cases/1','kind','SQL_ONLY_NO_NATIVE_PROOF',
    'terminal',(SELECT terminal FROM context),'case_count',count(*),'cases',jsonb_agg(to_jsonb(results) ORDER BY name),
    'final_rows_sha256',encode(sha256(convert_to(pg_temp.snapshot()::text,'UTF8')),'hex')) FROM results;
SELECT pg_advisory_unlock(1111651442,1129599564);
