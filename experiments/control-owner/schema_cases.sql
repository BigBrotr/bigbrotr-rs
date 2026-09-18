-- Run only against the owned, populated lifecycle fixture; all changes roll back.
BEGIN;
SELECT pg_advisory_lock(1111651442,1129599564);
DO $$
DECLARE d bytea; r bytea; n integer := 0;
BEGIN
    SELECT dataset_id, resource_id INTO STRICT d,r FROM control.owner_epochs WHERE role='control' AND epoch=3 AND state='released';
    INSERT INTO control.role_instances(role_instance_id,dataset_id,role,created_at_us)
        VALUES (decode(repeat('71',16),'hex'),d,'control',1);
    BEGIN
        INSERT INTO control.owner_epochs(resource_id,epoch,dataset_id,role,role_instance_id,process_incarnation,acquired_at_us,lease_expires_at_us,state)
            VALUES (r,4,d,'control',decode(repeat('71',16),'hex'),decode(repeat('81',16),'hex'),1,2,'active');
        RAISE EXCEPTION 'accepted missing backend binding';
    EXCEPTION WHEN check_violation THEN n := n+1; END;
    BEGIN
        INSERT INTO control.owner_epochs(resource_id,epoch,dataset_id,role,role_instance_id,process_incarnation,acquired_at_us,lease_expires_at_us,state,control_fence_version,control_backend_pid,control_backend_start)
            VALUES (r,4,d,'control',decode(repeat('71',16),'hex'),decode(repeat('81',16),'hex'),1,2,'active',1,1,clock_timestamp());
        RAISE EXCEPTION 'accepted another backend';
    EXCEPTION WHEN check_violation THEN n := n+1; END;
    INSERT INTO control.owner_epochs(resource_id,epoch,dataset_id,role,role_instance_id,process_incarnation,acquired_at_us,lease_expires_at_us,state,control_fence_version,control_backend_pid,control_backend_start)
        VALUES (r,4,d,'control',decode(repeat('71',16),'hex'),decode(repeat('81',16),'hex'),1,2,'active',1,pg_backend_pid(),(SELECT backend_start FROM pg_stat_activity WHERE pid=pg_backend_pid()));
    PERFORM control.require_control_owner(d,r,4,decode(repeat('81',16),'hex'));
    BEGIN
        UPDATE control.owner_epochs SET control_backend_pid=1 WHERE resource_id=r AND epoch=4;
        RAISE EXCEPTION 'rewrote backend attribution';
    EXCEPTION WHEN check_violation THEN n := n+1; END;
    BEGIN
        INSERT INTO control.owner_epochs SELECT decode(repeat('72',16),'hex'),1,d,role,role_instance_id,process_incarnation,acquired_at_us,lease_expires_at_us,state,released_at_us,exclusion_method,exclusion_digest,control_fence_version,control_backend_pid,control_backend_start FROM control.owner_epochs WHERE resource_id=r AND epoch=4;
        RAISE EXCEPTION 'created another Control resource';
    EXCEPTION WHEN check_violation THEN n := n+1; END;
    BEGIN
        PERFORM control.require_control_owner(d,r,3,decode(repeat('2b',16),'hex'));
        RAISE EXCEPTION 'accepted a stale owner epoch';
    EXCEPTION WHEN check_violation THEN n := n+1; END;
    BEGIN
        UPDATE control.owner_epochs SET state='released',released_at_us=3,exclusion_method='joined',exclusion_digest=decode(repeat('00',32),'hex') WHERE resource_id=r AND epoch=4;
        RAISE EXCEPTION 'accepted an unrelated exclusion method';
    EXCEPTION WHEN check_violation THEN n := n+1; END;
    IF n <> 6 THEN RAISE EXCEPTION 'missing negative cases'; END IF;
    RAISE NOTICE 'six Control SQL negatives passed';
END;
$$;
ROLLBACK;
SELECT pg_advisory_unlock(1111651442,1129599564);
