-- Run as the application login in a fresh clone of the actual accepted case.
-- These raw timestamps are independent of the Rust adapter's server-time query.
BEGIN;
CREATE TEMP TABLE fixture_owner AS SELECT resource_id,epoch+1 AS epoch,dataset_id
    FROM control.owner_epochs WHERE role='control' ORDER BY epoch DESC LIMIT 1;
SELECT pg_advisory_lock(1111651442,1129599564);
INSERT INTO control.role_instances(role_instance_id,dataset_id,role,created_at_us)
    SELECT decode(repeat('a1',16),'hex'),dataset_id,'control',1 FROM fixture_owner;
INSERT INTO control.owner_epochs(resource_id,epoch,dataset_id,role,role_instance_id,
    process_incarnation,acquired_at_us,lease_expires_at_us,state,control_fence_version,
    control_backend_pid,control_backend_start)
SELECT resource_id,epoch,dataset_id,'control',decode(repeat('a1',16),'hex'),decode(repeat('a2',16),'hex'),1,
    (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint+30000000,
    'active',1,pg_backend_pid(),(SELECT backend_start FROM pg_stat_activity WHERE pid=pg_backend_pid())
FROM fixture_owner;

CREATE FUNCTION pg_temp.attempt(offset_us bigint, expected text) RETURNS void LANGUAGE plpgsql AS $$
DECLARE command control.management_commands%ROWTYPE; detail control.acquisition_jobs%ROWTYPE;
    job control.jobs%ROWTYPE; audit control.management_audit%ROWTYPE; observed text;
BEGIN
    BEGIN
        SET CONSTRAINTS ALL DEFERRED;
        SELECT * INTO STRICT detail FROM control.acquisition_jobs;
        SELECT * INTO STRICT job FROM control.jobs WHERE job_id=detail.job_id;
        SELECT * INTO STRICT command FROM control.management_commands
            WHERE actor_id=detail.actor_id AND request_key=detail.request_key;
        SELECT * INTO STRICT audit FROM control.management_audit WHERE audit_id=command.audit_id;
        detail.job_id:=decode(repeat('b1',16),'hex');
        detail.request_key:=decode(repeat('b2',16),'hex');
        command.request_key:=detail.request_key;
        command.audit_id:=decode(repeat('b3',16),'hex');
        command.accepted_at_us:=(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint+offset_us;
        command.request_digest:=sha256(convert_to('bigbrotr/acquisition-job/1','UTF8')||decode('00','hex')
            ||detail.dataset_id||detail.actor_id||detail.request_key||detail.scope_id
            ||int8send(120000000)||int8send(1)||int8send(1));
        detail.deadline_at_us:=command.accepted_at_us+detail.duration_us;
        job.job_id:=detail.job_id; job.request_key:=detail.request_key;
        job.request_digest:=command.request_digest; job.created_at_us:=command.accepted_at_us;
        job.input_complete:=false;
        audit.audit_id:=command.audit_id; audit.subject_id:=detail.request_key;
        audit.created_at_us:=command.accepted_at_us;
        INSERT INTO control.management_audit SELECT (audit).*;
        INSERT INTO control.management_commands SELECT (command).*;
        INSERT INTO control.jobs SELECT (job).*;
        INSERT INTO control.acquisition_jobs SELECT (detail).*;
        UPDATE control.jobs SET input_complete=true WHERE job_id=detail.job_id;
        SET CONSTRAINTS ALL IMMEDIATE;
        -- Roll back even the positive control's rows; distinguish it from any
        -- engine rejection. Outer owner state is also rolled back at the end.
        RAISE EXCEPTION 'fixture accepted all final constraints' USING ERRCODE='BZ001';
    EXCEPTION WHEN OTHERS THEN GET STACKED DIAGNOSTICS observed=RETURNED_SQLSTATE;
    END;
    IF observed IS DISTINCT FROM expected THEN
        RAISE EXCEPTION 'timestamp %, expected %, observed %',offset_us,expected,observed;
    END IF;
END $$;
SELECT pg_temp.attempt(0,'BZ001');
SELECT pg_temp.attempt(31536000000000,'BB001');
SELECT '{"status":"PASS","valid_current_time":true,"future_time_refused":true}'::json;
ROLLBACK;
