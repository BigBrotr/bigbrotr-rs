-- Run after archive-frontier-constraints.sql in the SAME backend. Its current
-- Control is valid while the original Archive publisher is retired. A raw lease
-- without protocol attribution must never become a qualified native pin.
DO $$ DECLARE observed text;
BEGIN
    BEGIN
        INSERT INTO control.snapshot_leases(lease_id,dataset_id,consumer_id,
            frontier_revision,topology_generation,holder_kind,
            holder_role_instance_id,holder_role,holder_incarnation,
            acquired_at_us,updated_at_us,expires_at_us)
            VALUES(pg_temp.id(201),pg_temp.id(1),pg_temp.id(9),1,0,'role_instance',
                pg_temp.id(3),'control',pg_temp.id(6),
                pg_temp.now_us(),pg_temp.now_us(),pg_temp.now_us()+30000000);
        SET CONSTRAINTS ALL IMMEDIATE;
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS observed=RETURNED_SQLSTATE;
    END;
    IF observed IS DISTINCT FROM '23514' THEN
        RAISE EXCEPTION 'unqualified Archive lease accepted or wrong refusal: %',
            coalesce(observed,'success');
    END IF;
    IF EXISTS(SELECT 1 FROM control.snapshot_leases WHERE lease_id=pg_temp.id(201)) THEN
        RAISE EXCEPTION 'refused lease left durable state';
    END IF;
END $$;
SELECT 'PASS: unqualified Archive lease refused atomically';
