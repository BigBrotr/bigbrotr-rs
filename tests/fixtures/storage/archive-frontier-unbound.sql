-- An empty cut still needs native snapshot/persistence and commit authority.
-- Synthetic SQL-only negative control, not evidence of native observation.
\set ON_ERROR_STOP on
SET statement_timeout = '10s';
SET lock_timeout = '2s';
CREATE FUNCTION pg_temp.id(n integer) RETURNS bytea LANGUAGE sql IMMUTABLE AS $$
    SELECT decode(lpad(to_hex(n), 32, '0'), 'hex')
$$;
BEGIN;
INSERT INTO control.datasets(dataset_id, instance_id, created_at_us)
    VALUES (pg_temp.id(1), pg_temp.id(2), 0);
INSERT INTO control.topology_generations VALUES (pg_temp.id(1), 0, NULL, 0, false);
UPDATE control.topology_generations SET sealed = true;
INSERT INTO control.role_instances VALUES (pg_temp.id(3), pg_temp.id(1), 'archive', 0, NULL);
INSERT INTO control.owner_epochs(resource_id, epoch, dataset_id, role, role_instance_id,
    process_incarnation, acquired_at_us, lease_expires_at_us, state)
    VALUES (pg_temp.id(4), 1, pg_temp.id(1), 'archive', pg_temp.id(3), pg_temp.id(5), 0, 1, 'active');
INSERT INTO control.consumers(consumer_id, dataset_id, consumer_role, created_at_us)
    VALUES (pg_temp.id(4), pg_temp.id(1), 'archive', 0);
COMMIT;
DO $$
DECLARE observed text;
BEGIN
    BEGIN
        INSERT INTO control.consumer_frontier_revisions(consumer_id, frontier_revision,
            dataset_id, consumer_role, topology_generation, owner_resource_id,
            owner_epoch, owner_incarnation, created_at_us)
            VALUES (pg_temp.id(4), 1, pg_temp.id(1), 'archive', 0, pg_temp.id(4), 1, pg_temp.id(5), 0);
        UPDATE control.consumer_frontier_revisions SET sealed = true;
        UPDATE control.consumers SET current_revision = 1, current_topology_generation = 0;
        SET CONSTRAINTS ALL IMMEDIATE;
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS observed = RETURNED_SQLSTATE;
    END;
    IF observed IS DISTINCT FROM '23514' THEN
        RAISE EXCEPTION 'unbound empty archive frontier accepted: expected23514, observed%', coalesce(observed, 'success');
    END IF;
    IF EXISTS (SELECT 1 FROM control.consumer_frontier_revisions)
        OR EXISTS (SELECT 1 FROM control.consumers WHERE current_revision IS NOT NULL) THEN
        RAISE EXCEPTION 'refused frontier left durable publication state';
    END IF;
END;
$$;
SELECT 'PASS: unbound empty archive frontier refused atomically';
