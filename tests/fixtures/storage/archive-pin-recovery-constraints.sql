-- Continue archive-frontier-constraints.sql in the SAME actual Control backend.
-- Empty native cut is synthetic relational evidence, not descriptor exclusion.
CREATE OR REPLACE FUNCTION pg_temp.state() RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE relation text; value jsonb; result jsonb := '{}'::jsonb;
BEGIN
    FOREACH relation IN ARRAY ARRAY['snapshot_leases','archive_snapshot_pins',
        'retention_claims','archive_pin_recoveries'] LOOP
        EXECUTE format('SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),''[]'') FROM (SELECT *,xmin::text AS xmin FROM control.%I) t',relation) INTO value;
        result := result || jsonb_build_object(relation,value);
    END LOOP;
    RETURN result;
END;
$$;
BEGIN;
INSERT INTO control.snapshot_leases(lease_id,dataset_id,consumer_id,frontier_revision,
    topology_generation,holder_kind,holder_role_instance_id,holder_role,holder_incarnation,
    acquired_at_us,updated_at_us,expires_at_us,archive_pin_protocol)
    SELECT pg_temp.id(n),pg_temp.id(1),pg_temp.id(9),1,0,'role_instance',pg_temp.id(3),
        'control',pg_temp.id(6),pg_temp.now_us(),pg_temp.now_us(),pg_temp.now_us()+10000000,1
    FROM generate_series(401,402) n;
INSERT INTO control.archive_snapshot_pins(lease_id,dataset_id,consumer_id,frontier_revision,
    issuer_resource,issuer_epoch,issuer_incarnation,purpose,requested_ttl_us)
    SELECT pg_temp.id(n),pg_temp.id(1),pg_temp.id(9),1,pg_temp.id(5),1,pg_temp.id(6),'reader',10000000
    FROM generate_series(401,402) n;
COMMIT;

CREATE FUNCTION pg_temp.recover(n integer, finish boolean DEFAULT true,
    binding integer DEFAULT 1, revision integer DEFAULT 1) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    PERFORM control.lock_archive_pins(pg_temp.id(1),pg_temp.id(9));
    INSERT INTO control.archive_pin_recoveries(lease_id,dataset_id,consumer_id,
        expected_lease_revision,coordinator_binding_sha256,exclusion_protocol,
        recoverer_resource,recoverer_epoch,recoverer_incarnation,recovered_at_us)
        SELECT pg_temp.id(n),pg_temp.id(1),pg_temp.id(9),revision,pg_temp.digest(binding),1,
            resource_id,epoch,process_incarnation,0 FROM control.owner_epochs
            WHERE role='control' AND state='active';
    IF finish THEN
        UPDATE control.snapshot_leases SET state='released',lease_revision=lease_revision+1,
            updated_at_us=pg_temp.now_us(),released_at_us=pg_temp.now_us(),release_reason='old_reader_excluded'
            WHERE lease_id=pg_temp.id(n);
    END IF;
END;
$$;
SELECT pg_temp.refuses('recovery_same_holder','SELECT pg_temp.recover(401)');
BEGIN;
UPDATE control.owner_epochs SET state='released',released_at_us=pg_temp.now_us(),
    exclusion_method='storage_fenced',exclusion_digest=pg_temp.digest(3) WHERE role='control';
INSERT INTO control.owner_epochs(resource_id,epoch,dataset_id,role,role_instance_id,
    process_incarnation,acquired_at_us,lease_expires_at_us,state,
    control_fence_version,control_backend_pid,control_backend_start)
    VALUES(pg_temp.id(5),2,pg_temp.id(1),'control',pg_temp.id(3),pg_temp.id(7),
        pg_temp.now_us(),pg_temp.now_us()+60000000,'active',1,pg_backend_pid(),
        (SELECT backend_start FROM pg_stat_activity WHERE pid=pg_backend_pid()));
COMMIT;
SELECT pg_temp.refuses('recovery_incomplete','SELECT pg_temp.recover(401,false)');
SELECT pg_temp.refuses('recovery_wrong_binding','SELECT pg_temp.recover(401,true,2)');
SELECT pg_temp.refuses('recovery_wrong_revision','SELECT pg_temp.recover(401,true,1,2)');
SELECT pg_temp.refuses('recovery_missing_lease','SELECT pg_temp.recover(499)','P0002');

CREATE TEMP TABLE rollback_expected AS SELECT pg_temp.state() AS value;
BEGIN;
SELECT pg_temp.recover(402);
SET CONSTRAINTS ALL IMMEDIATE;
ROLLBACK;
DO $$ BEGIN
    IF pg_temp.state() IS DISTINCT FROM (SELECT value FROM rollback_expected) THEN
        RAISE EXCEPTION 'rollback changed recovery/lease/claim history';
    END IF;
    INSERT INTO results VALUES('recovery_rollback','unchanged');
END $$;

-- Scheduling expiry is deliberately not authority to grant a new read. Actual
-- fenced backend may still close obligations after positive local exclusion.
UPDATE control.owner_epochs SET lease_expires_at_us=pg_temp.now_us()-1 WHERE role='control' AND state='active';
SELECT pg_temp.recover(401);
DO $$ DECLARE prior jsonb := (SELECT value FROM rollback_expected);
DECLARE current_state jsonb := pg_temp.state(); old_lease jsonb; new_lease jsonb;
DECLARE mutable text[] := ARRAY['lease_revision','state','updated_at_us','released_at_us','release_reason','xmin'];
BEGIN
    SELECT value INTO STRICT old_lease FROM jsonb_array_elements(prior->'snapshot_leases')
        WHERE value->>'lease_id'=pg_temp.id(401)::text;
    SELECT value INTO STRICT new_lease FROM jsonb_array_elements(current_state->'snapshot_leases')
        WHERE value->>'lease_id'=pg_temp.id(401)::text;
    IF current_state->'archive_snapshot_pins' IS DISTINCT FROM prior->'archive_snapshot_pins'
        OR current_state->'retention_claims' IS DISTINCT FROM prior->'retention_claims'
        OR (SELECT jsonb_agg(value ORDER BY value::text) FROM jsonb_array_elements(current_state->'snapshot_leases')
            WHERE value->>'lease_id'<>pg_temp.id(401)::text)
            IS DISTINCT FROM (SELECT jsonb_agg(value ORDER BY value::text) FROM jsonb_array_elements(prior->'snapshot_leases')
                WHERE value->>'lease_id'<>pg_temp.id(401)::text)
        OR old_lease-mutable IS DISTINCT FROM new_lease-mutable
        OR old_lease->>'xmin'=new_lease->>'xmin' THEN
        RAISE EXCEPTION 'recovery changed immutable or unrelated history';
    END IF;
    IF (SELECT count(*) FROM control.archive_pin_recoveries)<>1
        OR NOT EXISTS (SELECT 1 FROM control.snapshot_leases WHERE lease_id=pg_temp.id(401)
            AND state='released' AND lease_revision=2 AND release_reason='old_reader_excluded')
        OR EXISTS (SELECT 1 FROM control.retention_claims)
        OR NOT EXISTS (SELECT 1 FROM control.archive_pin_recoveries WHERE lease_id=pg_temp.id(401)
            AND recoverer_epoch=2 AND recoverer_incarnation=pg_temp.id(7) AND recovered_at_us>0)
        OR EXISTS (SELECT 1 FROM control.archive_snapshot_pins WHERE issuer_epoch<>1 OR issuer_incarnation<>pg_temp.id(6)) THEN
        RAISE EXCEPTION 'empty-cut recovery did not preserve original attribution';
    END IF;
    INSERT INTO results VALUES('recovery_empty_cut_after_scheduling_expiry','committed');
END $$;
SELECT pg_temp.refuses('recovery_update','UPDATE control.archive_pin_recoveries SET recovered_at_us=0');
SELECT pg_temp.refuses('recovery_delete','DELETE FROM control.archive_pin_recoveries');
SELECT pg_temp.refuses('recovery_truncate','TRUNCATE control.archive_pin_recoveries');
SELECT pg_temp.refuses('recovery_duplicate_release','SELECT pg_temp.recover(401)');
SELECT json_build_object('status','PASS','cases',(SELECT json_agg(t ORDER BY name) FROM results t),
    'durable',pg_temp.state());
