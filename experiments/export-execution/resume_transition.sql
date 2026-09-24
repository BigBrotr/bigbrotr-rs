-- Real retained native seed, committed0501 reservation, no output writer.
-- Probe the new resume requirement independently of the future Rust handler.
SELECT pg_advisory_lock(1111651442,1129599564);
BEGIN;
DO $$
DECLARE old_owner control.owner_epochs%ROWTYPE; selected control.jobs%ROWTYPE;
BEGIN
    SELECT * INTO STRICT old_owner FROM control.owner_epochs WHERE role='control'
        ORDER BY epoch DESC LIMIT 1;
    IF old_owner.state<>'released' THEN RAISE EXCEPTION 'fixture requires released Control'; END IF;
    INSERT INTO control.role_instances VALUES(decode(repeat('d2',16),'hex'),old_owner.dataset_id,'control',1,NULL);
    INSERT INTO control.owner_epochs(resource_id,epoch,dataset_id,role,role_instance_id,process_incarnation,
        acquired_at_us,lease_expires_at_us,state,control_fence_version,control_backend_pid,control_backend_start)
        VALUES(old_owner.resource_id,old_owner.epoch+1,old_owner.dataset_id,'control',decode(repeat('d2',16),'hex'),
            decode(repeat('d3',16),'hex'),(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,
            (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint+120000000,'active',1,pg_backend_pid(),
            (SELECT backend_start FROM pg_stat_activity WHERE pid=pg_backend_pid()));
    SELECT * INTO STRICT selected FROM control.jobs WHERE export_job_protocol=1;
    IF selected.state<>'queued' OR selected.job_revision<>2
        OR NOT EXISTS (SELECT 1 FROM control.export_execution_units WHERE job_id=selected.job_id
            AND reservation_transaction<>pg_current_xact_id()) THEN
        RAISE EXCEPTION 'fixture requires its original committed unit';
    END IF;
    PERFORM control.require_export_execution_authority(selected.job_id);
    UPDATE control.jobs SET state='running',job_revision=job_revision+1,
        owner_resource_id=old_owner.resource_id,owner_epoch=old_owner.epoch+1,
        owner_incarnation=decode(repeat('d3',16),'hex') WHERE job_id=selected.job_id;
END $$;
COMMIT;
BEGIN;
UPDATE control.jobs SET state='paused',job_revision=job_revision+1,reason_code='execution_refused'
    WHERE export_job_protocol=1 AND state='running';
COMMIT;
BEGIN;
CREATE TEMP TABLE resume_transition_result(code text, phase text, message text, unchanged boolean);
DO $$
DECLARE selected control.jobs%ROWTYPE; before_job jsonb; code text; message text; phase text;
BEGIN
    SELECT * INTO STRICT selected FROM control.jobs WHERE export_job_protocol=1;
    IF selected.state<>'paused' OR selected.job_revision<>4 THEN RAISE EXCEPTION 'fixture failed to park'; END IF;
    before_job:=to_jsonb(selected);
    BEGIN
        PERFORM control.require_export_execution_authority(selected.job_id);
        phase:='update';
        UPDATE control.jobs SET state='queued',job_revision=job_revision+1,reason_code=NULL WHERE job_id=selected.job_id;
        phase:='constraints'; SET CONSTRAINTS ALL IMMEDIATE;
        phase:='unreceipted-resume-accepted';
        RAISE EXCEPTION 'resume passed constraints without a command receipt' USING ERRCODE='BB099';
    EXCEPTION WHEN OTHERS THEN GET STACKED DIAGNOSTICS code=RETURNED_SQLSTATE,message=MESSAGE_TEXT;
    END;
    INSERT INTO resume_transition_result VALUES(code,phase,message,
        (SELECT to_jsonb(j) FROM control.jobs j WHERE job_id=selected.job_id)=before_job);
END $$;
SELECT row_to_json(r) FROM resume_transition_result r;
UPDATE control.owner_epochs SET state='released',released_at_us=(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,
    exclusion_method='storage_fenced',exclusion_digest=decode(repeat('de',32),'hex') WHERE role='control' AND state='active';
COMMIT;
SELECT pg_advisory_unlock(1111651442,1129599564);
