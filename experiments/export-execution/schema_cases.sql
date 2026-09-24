-- Isolated PostgreSQL fixture after real native publication/export acceptance.
-- A fresh actual Control backend holds exclusion; every trial rolls back. This
-- tests metadata constraints, not file synchronization or native authorization.
BEGIN;
SELECT pg_advisory_lock(1111651442,1129599564);
CREATE TEMP TABLE export_trial_results(name text PRIMARY KEY, outcome text NOT NULL);
DO $$
DECLARE previous control.owner_epochs%ROWTYPE; j control.jobs%ROWTYPE;
    unit_id bytea:=decode(repeat('a1',16),'hex'); mode text; phase text;
    code text; message text; before_job jsonb;
BEGIN
    SELECT * INTO STRICT previous FROM control.owner_epochs WHERE role='control'
        ORDER BY epoch DESC LIMIT 1;
    IF previous.state<>'released' THEN RAISE EXCEPTION 'fixture needs positively released original owner'; END IF;
    INSERT INTO control.role_instances VALUES(decode(repeat('a2',16),'hex'),previous.dataset_id,'control',1,NULL);
    INSERT INTO control.owner_epochs(resource_id,epoch,dataset_id,role,role_instance_id,process_incarnation,
        acquired_at_us,lease_expires_at_us,state,control_fence_version,control_backend_pid,control_backend_start)
        VALUES(previous.resource_id,previous.epoch+1,previous.dataset_id,'control',decode(repeat('a2',16),'hex'),
            decode(repeat('a3',16),'hex'),(extract(epoch FROM clock_timestamp())*1000000)::bigint,
            (extract(epoch FROM clock_timestamp())*1000000)::bigint+120000000,'active',1,pg_backend_pid(),
            (SELECT backend_start FROM pg_stat_activity WHERE pid=pg_backend_pid()));
    SELECT * INTO STRICT j FROM control.jobs WHERE export_job_protocol=1;
    before_job:=to_jsonb(j);
    FOREACH mode IN ARRAY ARRAY['claim','run_before_claim_commit','immediate_constraint_bypass','missing_unit','rewrite_unit'] LOOP
        BEGIN
            phase:='queue';
            UPDATE control.jobs SET state='queued',job_revision=job_revision+1,
                owner_resource_id=previous.resource_id,owner_epoch=previous.epoch+1,
                owner_incarnation=decode(repeat('a3',16),'hex') WHERE job_id=j.job_id;
            IF mode<>'missing_unit' THEN
                phase:='unit';
                INSERT INTO control.export_execution_units(job_id,artifact_id,max_output_bytes,max_temporary_bytes)
                    VALUES(j.job_id,unit_id,1048576,2097152);
            END IF;
            IF mode='immediate_constraint_bypass' THEN SET CONSTRAINTS ALL IMMEDIATE; END IF;
            IF mode NOT IN ('claim','rewrite_unit') THEN
                phase:='run';
                UPDATE control.jobs SET state='running',job_revision=job_revision+1 WHERE job_id=j.job_id;
            END IF;
            IF mode='rewrite_unit' THEN
                phase:='rewrite'; UPDATE control.export_execution_units SET artifact_id=decode(repeat('a4',16),'hex') WHERE job_id=j.job_id;
            END IF;
            phase:='constraints'; SET CONSTRAINTS ALL IMMEDIATE;
            phase:='positive-rollback'; RAISE EXCEPTION 'complete reservation rollback' USING ERRCODE='BB099';
        EXCEPTION WHEN OTHERS THEN GET STACKED DIAGNOSTICS code=RETURNED_SQLSTATE,message=MESSAGE_TEXT;
        END;
        IF mode='claim' THEN
            IF (phase,code,message) IS DISTINCT FROM ('positive-rollback','BB099','complete reservation rollback') THEN
                RAISE EXCEPTION 'positive claim failed: % % %',phase,code,message;
            END IF;
        ELSIF code<>'23514' OR phase NOT IN ('rewrite','checkpoint','success','run','constraints') THEN
            RAISE EXCEPTION 'negative % failed elsewhere: % % %',mode,phase,code,message;
        END IF;
        IF (SELECT to_jsonb(t) FROM control.jobs t WHERE job_id=j.job_id) IS DISTINCT FROM before_job
            OR EXISTS(SELECT 1 FROM control.export_execution_units) OR EXISTS(SELECT 1 FROM control.artifacts)
            OR EXISTS(SELECT 1 FROM control.artifact_files) THEN RAISE EXCEPTION 'trial changed durable identity'; END IF;
        INSERT INTO export_trial_results VALUES(mode,code);
    END LOOP;
END $$;
TABLE export_trial_results;
ROLLBACK;
SELECT pg_advisory_unlock(1111651442,1129599564);
