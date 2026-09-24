-- Resume receipts apply only to future transitions. Preserve all historical
-- acceptance/execution rows; never infer a historical resume command.
LOCK TABLE control.jobs,control.export_jobs,control.export_execution_units IN SHARE ROW EXCLUSIVE MODE;
DO $$ BEGIN
    IF current_setting('transaction_isolation')<>'read committed' THEN
        RAISE EXCEPTION 'export resume migration requires READ COMMITTED' USING ERRCODE='25001';
    END IF;
    IF EXISTS (SELECT 1 FROM control.jobs j WHERE j.export_job_protocol=1 AND j.state='paused'
        AND (j.checkpoint_revision<>0 OR j.result_manifest_digest IS NOT NULL
            OR NOT EXISTS (SELECT 1 FROM control.export_execution_units u WHERE u.job_id=j.job_id)
            OR NOT EXISTS (SELECT 1 FROM control.export_jobs e JOIN control.archive_snapshot_pins p
                ON p.lease_id=e.pin_lease_id JOIN control.snapshot_leases l USING(lease_id)
                WHERE e.job_id=j.job_id AND p.export_job_id=j.job_id AND p.purpose='export'
                    AND p.consumer_id=e.catalog_id AND p.frontier_revision=e.frontier_revision AND l.state='active'))) THEN
        RAISE EXCEPTION '0814 refuses paused exports without their committed retained unit; explicit recovery required' USING ERRCODE='55000';
    END IF;
END $$;

ALTER TABLE control.management_commands DROP CONSTRAINT management_command_shape;
ALTER TABLE control.management_commands ADD CONSTRAINT management_command_shape CHECK (
    (action='policy.activate-initial' AND authority_mode IS NOT NULL
        AND authority_mode IN ('managed','declarative') AND document_digest IS NOT NULL AND policy_revision=1)
    OR (action IN ('scope-grant.change','job.export.accept','job.export.resume') AND authority_mode IS NULL
        AND document_digest IS NULL AND policy_revision>0)
);
ALTER TABLE control.management_audit DROP CONSTRAINT management_audit_shape;
ALTER TABLE control.management_audit ADD CONSTRAINT management_audit_shape CHECK (
    (policy_revision IS NULL AND (
        (action='dataset.initialize' AND outcome='accepted' AND config_revision IS NOT NULL AND config_revision=1 AND subject_id IS NULL AND reason IS NULL)
        OR (action IN ('owner-token.issue','owner-token.revoke','owner.enroll') AND outcome='accepted' AND config_revision IS NULL AND subject_id IS NOT NULL AND reason IS NULL AND requester_database_role IS NOT NULL AND requester_os_uid IS NOT NULL)
        OR (action='owner.enroll' AND outcome='rejected' AND config_revision IS NULL AND subject_id IS NOT NULL AND reason IS NOT NULL AND reason IN ('invalid-token','expired','exhausted','terminal') AND requester_database_role IS NOT NULL AND requester_os_uid IS NOT NULL)
        OR (action='policy.activate-initial' AND outcome='accepted' AND config_revision IS NOT NULL AND config_revision>0 AND subject_id IS NOT NULL AND reason IS NULL AND requester_database_role IS NOT NULL AND requester_os_uid IS NOT NULL)
    ))
    OR (action IN ('browser.session.issue','browser.session.revoke','browser.session.reauthenticate','scope-grant.change','job.export.accept','job.export.resume') AND outcome='accepted'
        AND config_revision IS NOT NULL AND config_revision>0 AND policy_revision IS NOT NULL AND policy_revision>0
        AND subject_id IS NOT NULL AND reason IS NULL AND requester_database_role IS NULL AND requester_os_uid IS NULL)
);

CREATE TABLE control.export_resume_commands (
    actor_id control.id16 NOT NULL,
    request_key control.id16 NOT NULL,
    dataset_id control.id16 NOT NULL,
    job_id control.id16 NOT NULL REFERENCES control.export_execution_units(job_id),
    expected_job_revision control.u64 NOT NULL CHECK (expected_job_revision BETWEEN 1 AND 18446744073709551614),
    -- Attribution outlives disposable sessions; command-time guards validate it.
    session_id control.id16 NOT NULL,
    actor_credential_revision control.u64 NOT NULL CHECK (actor_credential_revision>0),
    actor_authentication_revision control.u64 NOT NULL CHECK (actor_authentication_revision>0),
    actor_grant_revision control.u64 NOT NULL CHECK (actor_grant_revision>0),
    -- Internal atomicity only; never a portable receipt or source identity.
    resume_transaction xid8 NOT NULL DEFAULT pg_current_xact_id(),
    PRIMARY KEY (actor_id,request_key),
    UNIQUE (job_id,expected_job_revision),
    FOREIGN KEY (actor_id,request_key) REFERENCES control.management_commands(actor_id,request_key),
    FOREIGN KEY (dataset_id,actor_id) REFERENCES control.local_accounts(dataset_id,principal_id)
);
CREATE FUNCTION control.export_resume_digest(detail control.export_resume_commands,
    command control.management_commands) RETURNS bytea
LANGUAGE sql IMMUTABLE STRICT SET search_path=pg_catalog AS $$
    SELECT sha256(convert_to('bigbrotr/export-resume/1','UTF8') || decode('00','hex')
        || detail.dataset_id || detail.actor_id || detail.request_key || detail.job_id
        || control.command_u64_bytes(detail.expected_job_revision)
        || control.command_u64_bytes(command.expected_config_revision)
        || control.command_u64_bytes(command.policy_revision))
$$;

CREATE FUNCTION control.guard_export_resume_detail() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE job control.jobs%ROWTYPE;
BEGIN
    -- Typed callers preacquire Control, dataset, account and session before
    -- consumer/job/source. A trigger cannot repair inverted acquisition order.
    PERFORM 1 FROM control.local_accounts WHERE dataset_id=NEW.dataset_id AND principal_id=NEW.actor_id FOR SHARE;
    PERFORM 1 FROM control.browser_sessions WHERE dataset_id=NEW.dataset_id AND principal_id=NEW.actor_id
        AND session_id=NEW.session_id FOR SHARE;
    IF NOT FOUND THEN RAISE EXCEPTION 'resume requires its authenticated session' USING ERRCODE='23514'; END IF;
    PERFORM control.require_export_execution_authority(NEW.job_id);
    SELECT * INTO STRICT job FROM control.jobs WHERE job_id=NEW.job_id;
    IF job.export_job_protocol IS DISTINCT FROM 1 OR job.state<>'paused' OR job.checkpoint_revision<>0
        OR job.result_manifest_digest IS NOT NULL OR NOT job.input_complete
        OR (job.dataset_id,job.principal_id,job.job_revision) IS DISTINCT FROM
            (NEW.dataset_id,NEW.actor_id,NEW.expected_job_revision)
        OR NOT EXISTS (SELECT 1 FROM control.export_execution_units u WHERE u.job_id=job.job_id
            AND u.reservation_transaction<>pg_current_xact_id()) THEN
        RAISE EXCEPTION 'resume requires the original actor and paused committed unit revision' USING ERRCODE='23514';
    END IF;
    NEW.resume_transaction:=pg_current_xact_id();
    RETURN NEW;
END $$;
CREATE TRIGGER export_resume_detail_guard BEFORE INSERT ON control.export_resume_commands
    FOR EACH ROW EXECUTE FUNCTION control.guard_export_resume_detail();

CREATE FUNCTION control.guard_export_resume_transition() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
    IF OLD.state='paused' AND NEW.state='queued' THEN
        IF NEW.reason_code IS NOT NULL OR NOT EXISTS (SELECT 1 FROM control.export_resume_commands r
            WHERE r.job_id=OLD.job_id AND r.dataset_id=OLD.dataset_id AND r.actor_id=OLD.principal_id
                AND r.expected_job_revision=OLD.job_revision AND r.resume_transaction=pg_current_xact_id()) THEN
            RAISE EXCEPTION 'paused export resume requires its atomic command receipt' USING ERRCODE='23514';
        END IF;
    ELSIF EXISTS (SELECT 1 FROM control.export_resume_commands r WHERE r.job_id=OLD.job_id
        AND r.resume_transaction=pg_current_xact_id()) THEN
        -- Remains effective after SET CONSTRAINTS IMMEDIATE: a receipt cannot
        -- be flushed early and followed by execution or another pause in this tx.
        RAISE EXCEPTION 'export resume must commit before another transition' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER export_resume_transition_guard BEFORE UPDATE ON control.jobs
    FOR EACH ROW WHEN (NEW.export_job_protocol=1) EXECUTE FUNCTION control.guard_export_resume_transition();

CREATE FUNCTION control.require_export_resume() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE detail control.export_resume_commands%ROWTYPE; command control.management_commands%ROWTYPE;
    job control.jobs%ROWTYPE; original control.export_jobs%ROWTYPE; time_us bigint;
BEGIN
    IF TG_TABLE_NAME='jobs' THEN
        SELECT * INTO detail FROM control.export_resume_commands WHERE job_id=NEW.job_id
            AND expected_job_revision=OLD.job_revision AND resume_transaction=pg_current_xact_id();
    ELSIF TG_TABLE_NAME='management_audit' THEN
        SELECT * INTO detail FROM control.export_resume_commands WHERE actor_id=NEW.actor_id AND request_key=NEW.subject_id;
    ELSE
        SELECT * INTO detail FROM control.export_resume_commands WHERE actor_id=NEW.actor_id AND request_key=NEW.request_key;
    END IF;
    IF detail.job_id IS NULL OR detail.resume_transaction<>pg_current_xact_id() THEN
        RAISE EXCEPTION 'export resume requires atomic detail' USING ERRCODE='23514';
    END IF;
    SELECT * INTO STRICT command FROM control.management_commands WHERE actor_id=detail.actor_id AND request_key=detail.request_key;
    SELECT * INTO STRICT job FROM control.jobs WHERE job_id=detail.job_id;
    SELECT * INTO STRICT original FROM control.export_jobs WHERE job_id=detail.job_id;
    IF command.action<>'job.export.resume' OR command.dataset_id<>detail.dataset_id
        OR command.request_digest IS DISTINCT FROM control.export_resume_digest(detail,command)
        OR detail.request_key=original.request_key OR job.state<>'queued' OR job.job_revision<>detail.expected_job_revision+1
        OR job.reason_code IS NOT NULL OR job.checkpoint_revision<>0 OR job.result_manifest_digest IS NOT NULL
        OR (job.dataset_id,job.principal_id) IS DISTINCT FROM (detail.dataset_id,detail.actor_id)
        OR NOT EXISTS (SELECT 1 FROM control.datasets d WHERE d.dataset_id=detail.dataset_id
            AND d.config_revision=command.expected_config_revision AND d.policy_revision=command.policy_revision)
        OR NOT EXISTS (SELECT 1 FROM control.management_audit a WHERE a.audit_id=command.audit_id
            AND (a.dataset_id,a.actor_id,a.action,a.outcome,a.subject_id,a.config_revision,a.policy_revision,a.created_at_us)
                IS NOT DISTINCT FROM (detail.dataset_id,detail.actor_id,command.action,'accepted'::text,
                    detail.request_key,command.expected_config_revision,command.policy_revision,command.accepted_at_us)) THEN
        RAISE EXCEPTION 'export resume requires exact atomic command audit and queued revision' USING ERRCODE='23514';
    END IF;
    IF TG_TABLE_NAME='management_audit' THEN
        IF NEW.audit_id IS DISTINCT FROM command.audit_id THEN
            RAISE EXCEPTION 'export resume audit is not its command receipt' USING ERRCODE='23514';
        END IF;
    END IF;
    PERFORM control.require_export_execution_authority(detail.job_id);
    -- The original export pin protects retained input beyond scheduling expiry.
    -- Never require its old issuer to still be the active execution owner.
    IF NOT EXISTS (SELECT 1 FROM control.archive_snapshot_pins p JOIN control.snapshot_leases l USING(lease_id)
        WHERE p.lease_id=original.pin_lease_id AND p.export_job_id=detail.job_id AND p.purpose='export'
            AND p.consumer_id=original.catalog_id AND p.frontier_revision=original.frontier_revision AND l.state='active') THEN
        RAISE EXCEPTION 'export resume requires its original retained input' USING ERRCODE='23514';
    END IF;
    time_us:=(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint;
    IF NOT EXISTS (SELECT 1 FROM control.local_accounts a JOIN control.browser_sessions s
        ON s.dataset_id=a.dataset_id AND s.principal_id=a.principal_id
        WHERE a.dataset_id=detail.dataset_id AND a.principal_id=detail.actor_id AND a.active
            AND a.credential_revision=detail.actor_credential_revision AND a.authentication_revision=detail.actor_authentication_revision
            AND a.grant_revision=detail.actor_grant_revision AND s.session_id=detail.session_id AND s.revoked_at_us IS NULL
            AND s.credential_revision=a.credential_revision AND s.authentication_revision=a.authentication_revision
            AND time_us>=s.last_seen_at_us AND time_us<LEAST(s.expires_at_us,s.idle_expires_at_us)) THEN
        RAISE EXCEPTION 'export resume session authority expired or changed before commit' USING ERRCODE='BB001';
    END IF;
    RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER export_resume_command_complete AFTER INSERT ON control.management_commands
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.action='job.export.resume') EXECUTE FUNCTION control.require_export_resume();
CREATE CONSTRAINT TRIGGER export_resume_audit_complete AFTER INSERT ON control.management_audit
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.action='job.export.resume') EXECUTE FUNCTION control.require_export_resume();
CREATE CONSTRAINT TRIGGER export_resume_detail_complete AFTER INSERT ON control.export_resume_commands
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION control.require_export_resume();
CREATE CONSTRAINT TRIGGER zzzz_export_resume_complete AFTER UPDATE ON control.jobs
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.export_job_protocol=1 AND OLD.state='paused' AND NEW.state='queued')
    EXECUTE FUNCTION control.require_export_resume();
CREATE TRIGGER immutable_export_resume_commands BEFORE UPDATE OR DELETE ON control.export_resume_commands
    FOR EACH ROW EXECUTE FUNCTION control.reject_history_mutation();
CREATE TRIGGER export_resume_commands_truncate_guard BEFORE TRUNCATE ON control.export_resume_commands
    FOR EACH STATEMENT EXECUTE FUNCTION control.reject_history_mutation();
REVOKE ALL ON control.export_resume_commands FROM PUBLIC;
REVOKE ALL ON FUNCTION control.export_resume_digest(control.export_resume_commands,control.management_commands),
    control.guard_export_resume_detail(),control.guard_export_resume_transition(),control.require_export_resume() FROM PUBLIC;
