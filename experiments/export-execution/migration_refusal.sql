-- Before0501: construct a queued0813 job under actual backend exclusion.
-- The runner appends the migration; its refusal must roll back this whole transaction.
SELECT pg_advisory_lock(1111651442,1129599564);
BEGIN;
DO $$
DECLARE o control.owner_epochs%ROWTYPE; j control.jobs%ROWTYPE;
BEGIN
    SELECT * INTO STRICT o FROM control.owner_epochs WHERE role='control' ORDER BY epoch DESC LIMIT 1;
    IF o.state<>'released' THEN RAISE EXCEPTION 'fixture needs released Control'; END IF;
    INSERT INTO control.role_instances VALUES(decode(repeat('b2',16),'hex'),o.dataset_id,'control',1,NULL);
    INSERT INTO control.owner_epochs(resource_id,epoch,dataset_id,role,role_instance_id,process_incarnation,
        acquired_at_us,lease_expires_at_us,state,control_fence_version,control_backend_pid,control_backend_start)
        VALUES(o.resource_id,o.epoch+1,o.dataset_id,'control',decode(repeat('b2',16),'hex'),decode(repeat('b3',16),'hex'),
            (extract(epoch FROM clock_timestamp())*1000000)::bigint,
            (extract(epoch FROM clock_timestamp())*1000000)::bigint+120000000,'active',1,pg_backend_pid(),
            (SELECT backend_start FROM pg_stat_activity WHERE pid=pg_backend_pid()));
    SELECT * INTO STRICT j FROM control.jobs WHERE export_job_protocol=1;
    UPDATE control.jobs SET state='queued',job_revision=job_revision+1,owner_resource_id=o.resource_id,
        owner_epoch=o.epoch+1,owner_incarnation=decode(repeat('b3',16),'hex') WHERE job_id=j.job_id;
END $$;
