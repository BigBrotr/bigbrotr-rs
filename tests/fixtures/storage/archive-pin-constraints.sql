-- Continue archive-frontier-constraints.sql in its actual Control backend.
-- Synthetic relational evidence only: the original Archive holder is retired;
-- its immutable frontier remains a valid input to a NEW current-Control pin.
CREATE OR REPLACE FUNCTION pg_temp.state() RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE relation text; value jsonb; result jsonb := '{}'::jsonb;
BEGIN
    FOREACH relation IN ARRAY ARRAY['snapshot_leases','archive_snapshot_pins','retention_claims'] LOOP
        EXECUTE format('SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),''[]'') FROM (SELECT *,xmin::text AS xmin FROM control.%I) t',relation) INTO value;
        result := result || jsonb_build_object(relation,value);
    END LOOP;
    RETURN result;
END;
$$;
CREATE FUNCTION pg_temp.pin(n integer, incarnation integer DEFAULT 6,
    bind boolean DEFAULT true, frontier integer DEFAULT 1) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO control.snapshot_leases(lease_id,dataset_id,consumer_id,
        frontier_revision,topology_generation,holder_kind,holder_role_instance_id,
        holder_role,holder_incarnation,acquired_at_us,updated_at_us,expires_at_us,archive_pin_protocol)
        VALUES(pg_temp.id(n),pg_temp.id(1),pg_temp.id(9),1,0,'role_instance',
            pg_temp.id(3),'control',pg_temp.id(incarnation),pg_temp.now_us(),
            pg_temp.now_us(),pg_temp.now_us()+10000000,1);
    IF bind THEN
        INSERT INTO control.archive_snapshot_pins(lease_id,dataset_id,consumer_id,
            frontier_revision,issuer_resource,issuer_epoch,issuer_incarnation,purpose,requested_ttl_us)
            VALUES(pg_temp.id(n),pg_temp.id(1),pg_temp.id(9),frontier,
                pg_temp.id(5),1,pg_temp.id(6),'reader',10000000);
    END IF;
END;
$$;
SELECT pg_temp.refuses('pin_missing_binding','SELECT pg_temp.pin(211,6,false)');
SELECT pg_temp.refuses('pin_wrong_incarnation','SELECT pg_temp.pin(212,7)');
SELECT pg_temp.refuses('pin_wrong_frontier','SELECT pg_temp.pin(213,6,true,2)');
SELECT pg_temp.pin(214);
DO $$ BEGIN
    IF (SELECT count(*) FROM control.snapshot_leases)<>1
        OR (SELECT count(*) FROM control.archive_snapshot_pins)<>1
        OR EXISTS(SELECT 1 FROM control.retention_claims) THEN
        RAISE EXCEPTION 'empty cut must retain one native pin and no positive ranges';
    END IF;
    INSERT INTO results VALUES('pin_empty_cut_with_retired_publisher','committed');
END $$;
SELECT pg_temp.refuses('pin_binding_mutable','UPDATE control.archive_snapshot_pins SET purpose=''export''');
SELECT pg_temp.refuses('pin_binding_delete','DELETE FROM control.archive_snapshot_pins');
SELECT pg_temp.refuses('pin_binding_truncate','TRUNCATE control.archive_snapshot_pins CASCADE');
SELECT pg_temp.refuses('pin_lease_truncate','TRUNCATE control.snapshot_leases CASCADE');
SELECT pg_temp.refuses('pin_claim_truncate','TRUNCATE control.retention_claims');
SELECT pg_temp.refuses('pin_protocol_removed',
    'UPDATE control.snapshot_leases SET archive_pin_protocol=NULL,lease_revision=lease_revision+1,updated_at_us=pg_temp.now_us(),expires_at_us=expires_at_us+1');
SELECT pg_temp.refuses('pin_stale_revision',
    'UPDATE control.snapshot_leases SET updated_at_us=pg_temp.now_us(),expires_at_us=expires_at_us+1000000');
UPDATE control.snapshot_leases SET lease_revision=2,updated_at_us=pg_temp.now_us(),
    expires_at_us=pg_temp.now_us()+10000000 WHERE lease_id=pg_temp.id(214);
DO $$ BEGIN
    IF (SELECT lease_revision FROM control.snapshot_leases)<>2 THEN
        RAISE EXCEPTION 'live CAS renewal did not advance';
    END IF;
    INSERT INTO results VALUES('pin_live_renewal','committed');
END $$;
UPDATE control.snapshot_leases SET state='released',lease_revision=3,
    updated_at_us=pg_temp.now_us(),released_at_us=pg_temp.now_us(),release_reason='reader_joined';
SELECT pg_temp.refuses('pin_released_cannot_renew',
    'UPDATE control.snapshot_leases SET state=''active'',lease_revision=4,updated_at_us=pg_temp.now_us(),expires_at_us=expires_at_us+1000000,released_at_us=NULL,release_reason=NULL');

UPDATE control.owner_epochs SET lease_expires_at_us=pg_temp.now_us()-1 WHERE role='control';
SELECT pg_temp.refuses('pin_expired_control','SELECT pg_temp.pin(215)');
UPDATE control.owner_epochs SET lease_expires_at_us=pg_temp.now_us()+60000000 WHERE role='control';
SELECT json_build_object('status','PASS','cases',(SELECT json_agg(t ORDER BY name) FROM results t),
    'durable',pg_temp.state());
