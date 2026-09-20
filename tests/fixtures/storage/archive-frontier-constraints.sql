-- Synthetic relational cases only. Native truth comes from the separate signed
-- admission/publication/capture executable and independent full-row oracle.
\set ON_ERROR_STOP on
SET statement_timeout = '10s';
SET lock_timeout = '2s';
CREATE FUNCTION pg_temp.id(n integer) RETURNS bytea LANGUAGE sql IMMUTABLE AS $$
    SELECT decode(lpad(to_hex(n), 32, '0'), 'hex')
$$;
CREATE FUNCTION pg_temp.digest(n integer) RETURNS bytea LANGUAGE sql IMMUTABLE AS $$
    SELECT decode(lpad(to_hex(n), 64, '0'), 'hex')
$$;
CREATE FUNCTION pg_temp.now_us() RETURNS bigint LANGUAGE sql VOLATILE AS $$
    SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint
$$;
CREATE TEMP TABLE results(name text PRIMARY KEY, observed text);
CREATE FUNCTION pg_temp.state() RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE relation text; value jsonb; result jsonb := '{}'::jsonb;
BEGIN
    FOREACH relation IN ARRAY ARRAY['owner_epochs','role_instances','archive_owner_sessions',
        'consumers','consumer_frontier_revisions','consumer_frontiers',
        'archive_frontier_snapshots','archive_frontier_batches'] LOOP
        EXECUTE format('SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),''[]'') FROM control.%I t',relation) INTO value;
        result := result || jsonb_build_object(relation,value);
    END LOOP;
    RETURN result;
END;
$$;
CREATE FUNCTION pg_temp.refuses(label text, command text, wanted text DEFAULT '23514')
RETURNS void LANGUAGE plpgsql AS $$
DECLARE prior jsonb := pg_temp.state(); observed text;
BEGIN
    BEGIN
        EXECUTE command;
        SET CONSTRAINTS ALL IMMEDIATE;
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS observed = RETURNED_SQLSTATE;
    END;
    IF observed IS DISTINCT FROM wanted OR pg_temp.state() IS DISTINCT FROM prior THEN
        RAISE EXCEPTION '%: expected% with unchanged rows; observed%',label,wanted,coalesce(observed,'success');
    END IF;
    INSERT INTO results VALUES(label,observed);
END;
$$;

SELECT pg_advisory_lock(1111651442,1129599564);
BEGIN;
INSERT INTO control.datasets(dataset_id,instance_id,created_at_us) VALUES(pg_temp.id(1),pg_temp.id(2),0);
INSERT INTO control.topology_generations VALUES(pg_temp.id(1),0,NULL,0,false);
UPDATE control.topology_generations SET sealed=true;
INSERT INTO control.role_instances VALUES
    (pg_temp.id(3),pg_temp.id(1),'control',0,NULL),
    (pg_temp.id(4),pg_temp.id(1),'archive',0,NULL);
INSERT INTO control.owner_epochs(resource_id,epoch,dataset_id,role,role_instance_id,
    process_incarnation,acquired_at_us,lease_expires_at_us,state,
    control_fence_version,control_backend_pid,control_backend_start)
    VALUES(pg_temp.id(5),1,pg_temp.id(1),'control',pg_temp.id(3),pg_temp.id(6),
        pg_temp.now_us(),pg_temp.now_us()+60000000,'active',1,pg_backend_pid(),
        (SELECT backend_start FROM pg_stat_activity WHERE pid=pg_backend_pid()));
INSERT INTO control.archive_coordinators(dataset_id,catalog_id,binding_sha256,protocol_version,
    issuer_resource,issuer_epoch,issuer_incarnation,initialized_at_us)
    VALUES(pg_temp.id(1),pg_temp.id(9),pg_temp.digest(1),1,pg_temp.id(5),1,pg_temp.id(6),pg_temp.now_us());
INSERT INTO control.archive_persistence(dataset_id,catalog_id,coordinator_binding_sha256,
    protocol_version,protection_class,baseline_snapshot,issuer_resource,issuer_epoch,issuer_incarnation,initialized_at_us)
    VALUES(pg_temp.id(1),pg_temp.id(9),pg_temp.digest(1),1,'local-synced',1,pg_temp.id(5),1,pg_temp.id(6),pg_temp.now_us());
INSERT INTO control.owner_epochs(resource_id,epoch,dataset_id,role,role_instance_id,
    process_incarnation,acquired_at_us,lease_expires_at_us,state)
    VALUES(pg_temp.id(9),1,pg_temp.id(1),'archive',pg_temp.id(4),pg_temp.id(8),pg_temp.now_us(),pg_temp.now_us()+60000000,'active');
COMMIT;

SELECT pg_temp.refuses('old_protocol_cannot_be_relabelled',
    'UPDATE control.owner_epochs SET archive_frontier_protocol=1 WHERE role=''archive''');
SELECT pg_temp.refuses('old_owner_cannot_receive_qualified_attribution',$sql$
    INSERT INTO control.archive_owner_sessions(dataset_id,catalog_id,owner_epoch,owner_incarnation,
        coordinator_binding_sha256,persistence_protocol,issuer_resource,issuer_epoch,issuer_incarnation)
        VALUES(pg_temp.id(1),pg_temp.id(9),1,pg_temp.id(8),pg_temp.digest(1),1,pg_temp.id(5),1,pg_temp.id(6))
$sql$);
BEGIN;
UPDATE control.owner_epochs SET state='released',released_at_us=pg_temp.now_us(),
    exclusion_method='storage_fenced',exclusion_digest=pg_temp.digest(2) WHERE role='archive';
INSERT INTO control.owner_epochs(resource_id,epoch,dataset_id,role,role_instance_id,
    process_incarnation,acquired_at_us,lease_expires_at_us,state,archive_frontier_protocol)
    VALUES(pg_temp.id(9),2,pg_temp.id(1),'archive',pg_temp.id(4),pg_temp.id(8),pg_temp.now_us(),pg_temp.now_us()+60000000,'active',1);
INSERT INTO control.archive_owner_sessions(dataset_id,catalog_id,owner_epoch,owner_incarnation,
    coordinator_binding_sha256,persistence_protocol,issuer_resource,issuer_epoch,issuer_incarnation)
    VALUES(pg_temp.id(1),pg_temp.id(9),2,pg_temp.id(8),pg_temp.digest(1),1,pg_temp.id(5),1,pg_temp.id(6));
INSERT INTO control.consumers(consumer_id,dataset_id,consumer_role,created_at_us)
    VALUES(pg_temp.id(9),pg_temp.id(1),'archive',pg_temp.now_us());
COMMIT;

CREATE FUNCTION pg_temp.publish(revision integer, bind boolean DEFAULT true, head boolean DEFAULT true)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO control.consumer_frontier_revisions(consumer_id,frontier_revision,dataset_id,
        consumer_role,topology_generation,previous_revision,owner_resource_id,owner_epoch,owner_incarnation,created_at_us)
        VALUES(pg_temp.id(9),revision,pg_temp.id(1),'archive',0,NULLIF(revision-1,0),pg_temp.id(9),2,pg_temp.id(8),pg_temp.now_us());
    IF bind THEN
        INSERT INTO control.archive_frontier_snapshots VALUES(pg_temp.id(100+revision),pg_temp.id(9),revision,
            pg_temp.id(1),0,pg_temp.id(9),2,pg_temp.id(8),1,pg_temp.digest(revision),0,0,0,0,0);
    END IF;
    UPDATE control.consumer_frontier_revisions SET sealed=true WHERE frontier_revision=revision;
    IF head THEN
        UPDATE control.consumers SET current_revision=revision,current_topology_generation=0;
    END IF;
END;
$$;
SELECT pg_temp.refuses('empty_missing_snapshot','SELECT pg_temp.publish(1,false)');
SELECT pg_temp.refuses('bound_missing_atomic_head','SELECT pg_temp.publish(1,true,false)');
SELECT pg_temp.publish(1);
DO $$ BEGIN
    IF (SELECT count(*) FROM control.archive_frontier_snapshots)<>1
        OR (SELECT current_revision FROM control.consumers)<>1
        OR EXISTS(SELECT 1 FROM control.consumer_frontiers)
        OR EXISTS(SELECT 1 FROM control.archive_frontier_batches) THEN
        RAISE EXCEPTION 'empty native binding did not commit exact empty membership';
    END IF;
    INSERT INTO results VALUES('bound_empty_cut','committed');
END $$;
SELECT pg_temp.refuses('snapshot_update','UPDATE control.archive_frontier_snapshots SET snapshot_id=2');
SELECT pg_temp.refuses('snapshot_delete','DELETE FROM control.archive_frontier_snapshots');
SELECT pg_temp.refuses('snapshot_truncate','TRUNCATE control.archive_frontier_snapshots CASCADE');
SELECT pg_temp.refuses('sealed_input_append','INSERT INTO control.archive_frontier_batches VALUES(pg_temp.id(9),1,pg_temp.id(99))');
SELECT pg_temp.refuses('owner_attribution_update','UPDATE control.archive_owner_sessions SET issuer_incarnation=pg_temp.id(7)');
SELECT pg_temp.refuses('owner_attribution_delete','DELETE FROM control.archive_owner_sessions');
SELECT pg_temp.refuses('owner_attribution_truncate','TRUNCATE control.archive_owner_sessions CASCADE');

UPDATE control.owner_epochs SET lease_expires_at_us=pg_temp.now_us()-1 WHERE role='control';
SELECT pg_temp.refuses('expired_control_refuses_new_frontier','SELECT pg_temp.publish(2)');
UPDATE control.owner_epochs SET lease_expires_at_us=pg_temp.now_us()+60000000 WHERE role='control';
UPDATE control.owner_epochs SET lease_expires_at_us=pg_temp.now_us()-1 WHERE role='archive' AND state='active';
SELECT pg_temp.refuses('expired_archive_refuses_new_frontier','SELECT pg_temp.publish(2)');
UPDATE control.owner_epochs SET lease_expires_at_us=pg_temp.now_us()+60000000 WHERE role='archive' AND state='active';
UPDATE control.role_instances SET retired_at_us=pg_temp.now_us() WHERE role='archive';
SELECT pg_temp.refuses('retired_archive_role_refuses_new_frontier','SELECT pg_temp.publish(2)');

SELECT json_build_object('status','PASS','cases',(SELECT json_agg(t ORDER BY name) FROM results t),
    'durable',pg_temp.state());
