-- Actual application login; an accepted/logged-out job in an isolated clone.
-- All positive and rejected mutations, including actual Control ownership,
-- roll back. The fixture does not infer native worker exclusion.
BEGIN;
SELECT pg_advisory_lock(1111651442,1129599564);
CREATE TEMP TABLE fixture_owner AS SELECT resource_id,epoch+1 AS epoch,dataset_id
    FROM control.owner_epochs WHERE role='control' ORDER BY epoch DESC LIMIT 1;
UPDATE control.owner_epochs SET state='released',
    released_at_us=(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,
    exclusion_method='storage_fenced',exclusion_digest=decode(repeat('a0',32),'hex')
    WHERE role='control' AND state<>'released';
UPDATE control.role_instances SET retired_at_us=(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint
    WHERE role='control' AND retired_at_us IS NULL;
INSERT INTO control.role_instances(role_instance_id,dataset_id,role,created_at_us)
    SELECT decode(repeat('a1',16),'hex'),dataset_id,'control',1 FROM fixture_owner;
INSERT INTO control.owner_epochs(resource_id,epoch,dataset_id,role,role_instance_id,
    process_incarnation,acquired_at_us,lease_expires_at_us,state,control_fence_version,
    control_backend_pid,control_backend_start)
SELECT resource_id,epoch,dataset_id,'control',decode(repeat('a1',16),'hex'),decode(repeat('a2',16),'hex'),1,
    (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint+30000000,
    'active',1,pg_backend_pid(),(SELECT backend_start FROM pg_stat_activity WHERE pid=pg_backend_pid())
FROM fixture_owner;

CREATE FUNCTION pg_temp.attempt(kind text, expected text) RETURNS void LANGUAGE plpgsql AS $$
DECLARE owner control.owner_epochs%ROWTYPE; job control.jobs%ROWTYPE; observed text;
BEGIN
    BEGIN
        SET CONSTRAINTS ALL DEFERRED;
        SELECT * INTO STRICT owner FROM control.owner_epochs WHERE role='control' AND state='active';
        SELECT * INTO STRICT job FROM control.jobs WHERE acquisition_job_protocol=1;
        PERFORM control.require_acquisition_job_authority(job.dataset_id,job.job_id);
        UPDATE control.jobs SET state='queued',job_revision=2,owner_resource_id=owner.resource_id,
            owner_epoch=owner.epoch,owner_incarnation=owner.process_incarnation WHERE job_id=job.job_id;
        SET CONSTRAINTS ALL IMMEDIATE;
        SET CONSTRAINTS ALL DEFERRED;
        IF kind='paused' THEN
            UPDATE control.jobs SET state='paused',job_revision=3,reason_code='fixture_paused'
                WHERE job_id=job.job_id;
            SET CONSTRAINTS ALL IMMEDIATE;
            SET CONSTRAINTS ALL DEFERRED;
        END IF;
        IF kind<>'same' THEN
            UPDATE control.owner_epochs SET state='released',
                released_at_us=(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,
                exclusion_method='storage_fenced',exclusion_digest=decode(repeat('a3',32),'hex')
                WHERE resource_id=owner.resource_id AND epoch=owner.epoch;
            UPDATE control.role_instances SET retired_at_us=(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint
                WHERE role_instance_id=owner.role_instance_id;
            INSERT INTO control.role_instances(role_instance_id,dataset_id,role,created_at_us)
                VALUES(decode(repeat('a4',16),'hex'),owner.dataset_id,'control',1);
            owner.epoch:=owner.epoch+1;
            owner.role_instance_id:=decode(repeat('a4',16),'hex');
            owner.process_incarnation:=decode(repeat('a5',16),'hex');
            INSERT INTO control.owner_epochs SELECT (owner).*;
            IF kind='helper' THEN
                PERFORM control.require_acquisition_job_authority(job.dataset_id,job.job_id);
            ELSE
                UPDATE control.jobs SET state='queued',job_revision=job_revision+1,reason_code=NULL,
                    owner_resource_id=owner.resource_id,owner_epoch=owner.epoch,owner_incarnation=owner.process_incarnation
                    WHERE job_id=job.job_id;
            END IF;
        END IF;
        SET CONSTRAINTS ALL IMMEDIATE;
        RAISE EXCEPTION 'fixture accepted all final constraints' USING ERRCODE='BZ001';
    EXCEPTION WHEN OTHERS THEN GET STACKED DIAGNOSTICS observed=RETURNED_SQLSTATE;
    END;
    IF observed IS DISTINCT FROM expected THEN
        RAISE EXCEPTION '% expected %, observed %',kind,expected,observed;
    END IF;
END $$;
SELECT pg_temp.attempt('same','BZ001');
SELECT pg_temp.attempt('helper','23514');
SELECT pg_temp.attempt('direct','23514');
SELECT pg_temp.attempt('paused','23514');
SELECT '{"status":"PASS","checks":4}'::json;
ROLLBACK;
