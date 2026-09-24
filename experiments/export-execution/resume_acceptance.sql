-- Command values/digest are independently derived in Python from the retained
-- seed. Each failed trial rolls back rows; the last positive receipt commits.
SELECT pg_advisory_lock(1111651442,1129599564);
BEGIN;
CREATE TEMP TABLE resume_fixture AS SELECT :'digest'::text AS digest;
CREATE TEMP TABLE resume_results(name text, phase text, code text, message text);
DO $$
DECLARE owner control.owner_epochs%ROWTYPE;
BEGIN
    SELECT * INTO STRICT owner FROM control.owner_epochs WHERE role='control' ORDER BY epoch DESC LIMIT 1;
    IF owner.state<>'released' THEN RAISE EXCEPTION 'fixture requires released owner'; END IF;
    INSERT INTO control.role_instances VALUES(decode(repeat('e2',16),'hex'),owner.dataset_id,'control',1,NULL);
    INSERT INTO control.owner_epochs(resource_id,epoch,dataset_id,role,role_instance_id,process_incarnation,
        acquired_at_us,lease_expires_at_us,state,control_fence_version,control_backend_pid,control_backend_start)
        VALUES(owner.resource_id,owner.epoch+1,owner.dataset_id,'control',decode(repeat('e2',16),'hex'),
            decode(repeat('e3',16),'hex'),(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,
            (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint+120000000,'active',1,pg_backend_pid(),
            (SELECT backend_start FROM pg_stat_activity WHERE pid=pg_backend_pid()));
END $$;
COMMIT;
CREATE FUNCTION pg_temp.try_resume(mode text) RETURNS void LANGUAGE plpgsql AS $$
DECLARE j control.jobs%ROWTYPE; e control.export_jobs%ROWTYPE; a control.local_accounts%ROWTYPE;
    owner control.owner_epochs%ROWTYPE; d control.datasets%ROWTYPE; time_us bigint;
    key bytea:=decode(repeat('e4',16),'hex'); audit bytea:=decode(repeat('e5',16),'hex');
BEGIN
    SELECT * INTO STRICT owner FROM control.owner_epochs WHERE role='control' AND state='active';
    SELECT * INTO STRICT e FROM control.export_jobs;
    PERFORM control.require_control_owner(e.dataset_id,owner.resource_id,owner.epoch,owner.process_incarnation);
    SELECT * INTO STRICT d FROM control.datasets WHERE dataset_id=e.dataset_id FOR SHARE;
    SELECT * INTO STRICT a FROM control.local_accounts WHERE dataset_id=e.dataset_id AND principal_id=e.actor_id FOR SHARE;
    PERFORM 1 FROM control.browser_sessions WHERE session_id=e.session_id FOR SHARE;
    PERFORM control.require_export_execution_authority(e.job_id);
    SELECT * INTO STRICT j FROM control.jobs WHERE job_id=e.job_id;
    IF j.state<>'paused' OR j.job_revision<>4 THEN RAISE EXCEPTION 'fixture requires pause4'; END IF;
    time_us:=(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint;
    IF mode<>'missing_audit' THEN
        INSERT INTO control.management_audit(audit_id,dataset_id,actor_id,action,outcome,created_at_us,config_revision,subject_id,policy_revision)
            VALUES(audit,j.dataset_id,j.principal_id,'job.export.resume','accepted',time_us,d.config_revision,key,d.policy_revision);
    END IF;
    IF mode<>'missing_command' THEN
        INSERT INTO control.management_commands(actor_id,request_key,dataset_id,action,request_digest,expected_config_revision,policy_revision,audit_id,accepted_at_us)
            VALUES(j.principal_id,key,j.dataset_id,'job.export.resume',
                CASE WHEN mode='wrong_digest' THEN decode(repeat('ff',32),'hex') ELSE decode((SELECT digest FROM resume_fixture),'hex') END,
                d.config_revision,d.policy_revision,audit,time_us);
    END IF;
    IF mode<>'missing_detail' THEN
        INSERT INTO control.export_resume_commands(actor_id,request_key,dataset_id,job_id,expected_job_revision,session_id,
            actor_credential_revision,actor_authentication_revision,actor_grant_revision)
            VALUES(j.principal_id,key,j.dataset_id,j.job_id,CASE WHEN mode='stale_revision' THEN 3 ELSE 4 END,e.session_id,
                a.credential_revision,a.authentication_revision,a.grant_revision);
    END IF;
    IF mode<>'missing_transition' THEN
        UPDATE control.jobs SET state='queued',job_revision=job_revision+1,
            reason_code=CASE WHEN mode='retained_reason' THEN reason_code ELSE NULL END,
            owner_resource_id=owner.resource_id,owner_epoch=owner.epoch,owner_incarnation=owner.process_incarnation
            WHERE job_id=j.job_id;
    END IF;
    IF mode='orphan_audit' THEN
        INSERT INTO control.management_audit(audit_id,dataset_id,actor_id,action,outcome,created_at_us,config_revision,subject_id,policy_revision)
            VALUES(decode(repeat('e6',16),'hex'),j.dataset_id,j.principal_id,'job.export.resume','accepted',time_us,d.config_revision,key,d.policy_revision);
    END IF;
    SET CONSTRAINTS ALL IMMEDIATE;
    IF mode='early_execution' THEN
        UPDATE control.jobs SET state='running',job_revision=job_revision+1 WHERE job_id=j.job_id;
    ELSIF mode='rewrite_receipt' THEN
        UPDATE control.export_resume_commands SET expected_job_revision=5 WHERE job_id=j.job_id;
    END IF;
END $$;
BEGIN;
DO $$
DECLARE mode text; phase text; code text; message text; before_job jsonb;
    before_commands bigint; before_audit bigint;
BEGIN
    SELECT to_jsonb(j) INTO STRICT before_job FROM control.jobs j WHERE export_job_protocol=1;
    SELECT count(*) INTO before_commands FROM control.management_commands;
    SELECT count(*) INTO before_audit FROM control.management_audit;
    FOREACH mode IN ARRAY ARRAY['positive','missing_audit','missing_command','missing_detail','missing_transition',
        'wrong_digest','stale_revision','retained_reason','orphan_audit','early_execution','rewrite_receipt'] LOOP
        BEGIN
            phase:='command'; PERFORM pg_temp.try_resume(mode);
            phase:='accepted-rollback'; RAISE EXCEPTION 'accepted resume rollback' USING ERRCODE='BB099';
        EXCEPTION WHEN OTHERS THEN GET STACKED DIAGNOSTICS code=RETURNED_SQLSTATE,message=MESSAGE_TEXT;
        END;
        IF mode='positive' THEN
            IF (phase,code,message) IS DISTINCT FROM ('accepted-rollback','BB099','accepted resume rollback') THEN
                RAISE EXCEPTION 'positive resume failed: % % %',phase,code,message;
            END IF;
        ELSIF code<>(CASE WHEN mode IN ('missing_audit','missing_command') THEN '23503' ELSE '23514' END) THEN
            RAISE EXCEPTION 'negative resume % failed elsewhere: % % %',mode,phase,code,message;
        END IF;
        IF (SELECT to_jsonb(j) FROM control.jobs j WHERE export_job_protocol=1) IS DISTINCT FROM before_job
            OR EXISTS(SELECT 1 FROM control.export_resume_commands)
            OR (SELECT count(*) FROM control.management_commands)<>before_commands
            OR (SELECT count(*) FROM control.management_audit)<>before_audit THEN
            RAISE EXCEPTION 'resume trial changed rows after rollback';
        END IF;
        INSERT INTO resume_results VALUES(mode,phase,code,message);
    END LOOP;
END $$;
SELECT jsonb_agg(r ORDER BY name) FROM resume_results r;
COMMIT;
BEGIN;
SELECT pg_temp.try_resume('positive');
COMMIT;
-- The committed receipt does not obstruct execution in a subsequent transaction.
BEGIN;
DO $$ DECLARE j control.jobs%ROWTYPE; BEGIN
    SELECT * INTO STRICT j FROM control.jobs WHERE export_job_protocol=1;
    IF j.state<>'queued' OR j.job_revision<>5 OR j.reason_code IS NOT NULL THEN RAISE EXCEPTION 'resume commit absent'; END IF;
    PERFORM control.require_export_execution_authority(j.job_id);
    UPDATE control.jobs SET state='running',job_revision=6 WHERE job_id=j.job_id;
END $$;
COMMIT;
BEGIN;
UPDATE control.jobs SET state='paused',job_revision=7,reason_code='execution_refused' WHERE export_job_protocol=1;
COMMIT;
SELECT json_build_object('state',j.state,'revision',j.job_revision::text,'accepted_revision',(r.expected_job_revision+1)::text,
    'resume_commands',(SELECT count(*) FROM control.export_resume_commands),'units',(SELECT count(*) FROM control.export_execution_units),
    'artifacts',(SELECT count(*) FROM control.artifacts),'pin_active',l.state='active')
    FROM control.jobs j JOIN control.export_resume_commands r USING(job_id) JOIN control.export_jobs e USING(job_id)
    JOIN control.snapshot_leases l ON l.lease_id=e.pin_lease_id;
BEGIN;
CREATE TEMP TABLE resume_before_cleanup AS SELECT to_jsonb(r) AS receipt FROM control.export_resume_commands r;
DO $$ DECLARE e control.export_jobs%ROWTYPE; d control.datasets%ROWTYPE; now_us bigint; BEGIN
    SELECT * INTO STRICT e FROM control.export_jobs;
    SELECT * INTO STRICT d FROM control.datasets WHERE dataset_id=e.dataset_id FOR SHARE;
    PERFORM 1 FROM control.local_accounts WHERE dataset_id=e.dataset_id AND principal_id=e.actor_id FOR UPDATE;
    now_us:=(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint;
    UPDATE control.browser_sessions SET revoked_at_us=now_us WHERE session_id=e.session_id;
    INSERT INTO control.management_audit(audit_id,dataset_id,actor_id,action,outcome,created_at_us,config_revision,subject_id,policy_revision)
        VALUES(decode(repeat('e7',16),'hex'),e.dataset_id,e.actor_id,'browser.session.revoke','accepted',now_us,
            d.config_revision,e.session_id,d.policy_revision);
    DELETE FROM control.browser_sessions WHERE session_id=e.session_id AND revoked_at_us IS NOT NULL;
END $$;
COMMIT;
SELECT json_build_object('cleanup_session_removed',NOT EXISTS(SELECT 1 FROM control.browser_sessions s JOIN control.export_resume_commands r USING(session_id)),
    'resume_receipt_unchanged',(SELECT to_jsonb(r) FROM control.export_resume_commands r)=(SELECT receipt FROM resume_before_cleanup));
BEGIN;
UPDATE control.owner_epochs SET state='released' ,released_at_us=(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,
    exclusion_method='storage_fenced',exclusion_digest=decode(repeat('ee',32),'hex') WHERE role='control' AND state='active';
COMMIT;
SELECT pg_advisory_unlock(1111651442,1129599564);
