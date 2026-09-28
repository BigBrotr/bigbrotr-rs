-- Isolated fixture. The harness deliberately gives this role UPDATE/DELETE/
-- TRUNCATE only here so invariant checks are exercised beyond privilege refusal.
-- Connect as bigbrotr_control. SET ROLE from a postgres session hides that
-- session's backend_start and cannot represent the product's actual login.
BEGIN;
CREATE TEMP TABLE fixture_counts (executed integer NOT NULL);
INSERT INTO fixture_counts VALUES (0);
CREATE FUNCTION pg_temp.must_fail(statement text, expected text) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE observed text;
BEGIN
    BEGIN
        EXECUTE statement;
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS observed = RETURNED_SQLSTATE;
    END;
    IF observed IS DISTINCT FROM expected THEN
        RAISE EXCEPTION 'expected SQLSTATE %, observed % for %', expected, observed, statement;
    END IF;
    UPDATE fixture_counts SET executed = executed + 1;
END $$;

SELECT pg_temp.must_fail('CREATE TABLE control.forbidden_registry_table (x integer)', '42501');
SELECT pg_temp.must_fail($q$
    INSERT INTO control.relay_endpoints (
        endpoint_id,dataset_id,normalization_version,url_bytes,registered_at_us,
        config_revision,policy_revision,owner_resource_id,owner_epoch,owner_incarnation)
    SELECT decode(repeat('d0',16),'hex'),dataset_id,1,url_bytes,1,
        config_revision,policy_revision,owner_resource_id,owner_epoch,owner_incarnation
    FROM control.relay_endpoints LIMIT 1
$q$, '23514');

SELECT pg_advisory_lock(1111651442,1129599564);
INSERT INTO control.role_instances(role_instance_id,dataset_id,role,created_at_us)
    SELECT decode(repeat('c1',16),'hex'),dataset_id,'control',1 FROM control.datasets;
INSERT INTO control.owner_epochs(
    resource_id,epoch,dataset_id,role,role_instance_id,process_incarnation,
    acquired_at_us,lease_expires_at_us,state,control_fence_version,
    control_backend_pid,control_backend_start)
SELECT resource_id,epoch+1,dataset_id,'control',decode(repeat('c1',16),'hex'),
    decode(repeat('c2',16),'hex'),1,
    (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint+30000000,
    'active',1,pg_backend_pid(),
    (SELECT backend_start FROM pg_stat_activity WHERE pid=pg_backend_pid())
FROM control.owner_epochs WHERE role='control' ORDER BY epoch DESC LIMIT 1;

SELECT pg_temp.must_fail($q$
    INSERT INTO control.relay_endpoints (
        endpoint_id,dataset_id,normalization_version,url_bytes,registered_at_us,
        config_revision,policy_revision,owner_resource_id,owner_epoch,owner_incarnation)
    SELECT decode(repeat('d1',16),'hex'),e.dataset_id,1,e.url_bytes,1,
        e.config_revision,e.policy_revision,o.resource_id,o.epoch,o.process_incarnation
    FROM control.relay_endpoints e JOIN control.owner_epochs o USING(dataset_id)
    WHERE o.role='control' AND o.state='active' LIMIT 1
$q$, '23505');
SELECT pg_temp.must_fail('UPDATE control.relay_endpoints SET registered_at_us=registered_at_us+1','23514');
SELECT pg_temp.must_fail('DELETE FROM control.relay_endpoints','23514');
SELECT pg_temp.must_fail('TRUNCATE control.relay_endpoints','23514');
SELECT pg_temp.must_fail($q$
    INSERT INTO control.relay_endpoints (
        endpoint_id,dataset_id,normalization_version,url_bytes,registered_at_us,
        config_revision,policy_revision,owner_resource_id,owner_epoch,owner_incarnation)
    SELECT decode(repeat('d2',16),'hex'),dataset_id,1,decode(repeat('61',8193),'hex'),1,
        1,1,resource_id,epoch,process_incarnation
    FROM control.owner_epochs WHERE role='control' AND state='active'
$q$, '23514');
SELECT pg_temp.must_fail($q$
    INSERT INTO control.relay_endpoints (
        endpoint_id,dataset_id,normalization_version,url_bytes,registered_at_us,
        config_revision,policy_revision,owner_resource_id,owner_epoch,owner_incarnation)
    SELECT decode(repeat('d3',16),'hex'),dataset_id,1,convert_to('wss://fixture.example/stale','UTF8'),1,
        2,1,resource_id,epoch,process_incarnation
    FROM control.owner_epochs WHERE role='control' AND state='active'
$q$, '23514');

INSERT INTO control.relay_endpoints (
    endpoint_id,dataset_id,normalization_version,url_bytes,registered_at_us,
    config_revision,policy_revision,owner_resource_id,owner_epoch,owner_incarnation)
SELECT decode(repeat('d4',16),'hex'),dataset_id,1,convert_to('wss://fixture.example/rollback','UTF8'),1,
    1,1,resource_id,epoch,process_incarnation
FROM control.owner_epochs WHERE role='control' AND state='active';
SELECT json_build_object('status','PASS','expected_refusals',executed,
    'uncommitted_rows',(SELECT count(*) FROM control.relay_endpoints)) FROM fixture_counts;
ROLLBACK;
