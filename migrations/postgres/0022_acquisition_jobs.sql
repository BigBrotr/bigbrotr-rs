-- Additive finite acquisition acceptance. No historical job adoption or rewrite.
ALTER TABLE control.jobs ADD COLUMN acquisition_job_protocol smallint
    CHECK (acquisition_job_protocol IS NULL OR (kind='acquisition' AND acquisition_job_protocol=1));

ALTER TABLE control.management_commands DROP CONSTRAINT management_command_shape;
ALTER TABLE control.management_commands ADD CONSTRAINT management_command_shape CHECK (
    (action='policy.activate-initial' AND authority_mode IS NOT NULL
        AND authority_mode IN ('managed','declarative') AND document_digest IS NOT NULL AND policy_revision=1)
    OR (action IN ('scope-grant.change','job.export.accept','job.export.resume','relay.enrollment.change','job.acquisition.accept')
        AND authority_mode IS NULL AND document_digest IS NULL AND policy_revision>0)
);
ALTER TABLE control.management_audit DROP CONSTRAINT management_audit_shape;
ALTER TABLE control.management_audit ADD CONSTRAINT management_audit_shape CHECK (
    (policy_revision IS NULL AND (
        (action='dataset.initialize' AND outcome='accepted'
            AND config_revision=1 AND config_revision IS NOT NULL
            AND subject_id IS NULL AND reason IS NULL)
        OR (action IN ('owner-token.issue','owner-token.revoke','owner.enroll',
                      'cursor-keyring.register','cursor-key.enroll',
                      'cursor-key.activate','cursor-key.revoke')
            AND outcome='accepted' AND config_revision IS NULL
            AND subject_id IS NOT NULL AND reason IS NULL
            AND requester_database_role IS NOT NULL AND requester_os_uid IS NOT NULL)
        OR (action='owner.enroll' AND outcome='rejected' AND config_revision IS NULL
            AND subject_id IS NOT NULL AND reason IS NOT NULL
            AND reason IN ('invalid-token','expired','exhausted','terminal')
            AND requester_database_role IS NOT NULL AND requester_os_uid IS NOT NULL)
        OR (action='policy.activate-initial' AND outcome='accepted'
            AND config_revision IS NOT NULL AND config_revision>0
            AND subject_id IS NOT NULL AND reason IS NULL
            AND requester_database_role IS NOT NULL AND requester_os_uid IS NOT NULL)
    ))
    OR (action IN ('browser.session.issue','browser.session.revoke',
                  'browser.session.reauthenticate','scope-grant.change',
                  'job.export.accept','job.export.resume','relay.enrollment.change','job.acquisition.accept')
        AND outcome='accepted' AND config_revision IS NOT NULL AND config_revision>0
        AND policy_revision IS NOT NULL AND policy_revision>0
        AND subject_id IS NOT NULL AND reason IS NULL
        AND requester_database_role IS NULL AND requester_os_uid IS NULL)
);

CREATE TABLE control.acquisition_jobs (
    job_id control.id16 PRIMARY KEY REFERENCES control.jobs(job_id),
    dataset_id control.id16 NOT NULL,
    actor_id control.id16 NOT NULL,
    request_key control.id16 NOT NULL,
    scope_id control.id16 NOT NULL,
    duration_us bigint NOT NULL CHECK (duration_us BETWEEN 1 AND 86400000000),
    deadline_at_us bigint NOT NULL,
    session_id control.id16 NOT NULL,
    actor_credential_revision control.u64 NOT NULL CHECK (actor_credential_revision>0),
    actor_authentication_revision control.u64 NOT NULL CHECK (actor_authentication_revision>0),
    actor_grant_revision control.u64 NOT NULL CHECK (actor_grant_revision>0),
    UNIQUE (actor_id,request_key),
    FOREIGN KEY (actor_id,request_key)
        REFERENCES control.management_commands(actor_id,request_key),
    FOREIGN KEY (dataset_id,actor_id)
        REFERENCES control.local_accounts(dataset_id,principal_id),
    FOREIGN KEY (dataset_id,scope_id)
        REFERENCES control.acquisition_scopes(dataset_id,scope_id)
);
CREATE TRIGGER immutable_acquisition_jobs BEFORE UPDATE OR DELETE ON control.acquisition_jobs
    FOR EACH ROW EXECUTE FUNCTION control.reject_history_mutation();
CREATE TRIGGER acquisition_jobs_no_truncate BEFORE TRUNCATE ON control.acquisition_jobs
    FOR EACH STATEMENT EXECUTE FUNCTION control.reject_history_mutation();

CREATE FUNCTION control.acquisition_command_digest(detail control.acquisition_jobs,
    command control.management_commands) RETURNS bytea
LANGUAGE sql IMMUTABLE STRICT SET search_path=pg_catalog AS $$
    SELECT sha256(convert_to('bigbrotr/acquisition-job/1','UTF8') || decode('00','hex')
        || detail.dataset_id || detail.actor_id || detail.request_key || detail.scope_id
        || int8send(detail.duration_us)
        || control.command_u64_bytes(command.expected_config_revision)
        || control.command_u64_bytes(command.policy_revision))
$$;

CREATE FUNCTION control.guard_acquisition_job() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE owner control.owner_epochs%ROWTYPE;
BEGIN
    IF TG_OP='DELETE' THEN
        RAISE EXCEPTION 'job correctness obligations require explicit reclamation' USING ERRCODE='23514';
    END IF;
    IF TG_OP='UPDATE' AND NEW.acquisition_job_protocol IS DISTINCT FROM OLD.acquisition_job_protocol THEN
        RAISE EXCEPTION 'acquisition job protocol is immutable' USING ERRCODE='23514';
    END IF;
    IF NEW.acquisition_job_protocol IS NULL THEN RETURN NEW; END IF;
    IF current_setting('transaction_isolation')<>'read committed' THEN
        RAISE EXCEPTION 'acquisition jobs require READ COMMITTED' USING ERRCODE='25001';
    END IF;
    SELECT * INTO STRICT owner FROM control.owner_epochs
        WHERE dataset_id=NEW.dataset_id AND role='control' AND state='active';
    PERFORM control.require_control_owner(owner.dataset_id,owner.resource_id,owner.epoch,owner.process_incarnation);
    IF owner.lease_expires_at_us<=(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint THEN
        RAISE EXCEPTION 'acquisition mutation requires live actual Control owner' USING ERRCODE='23514';
    END IF;
    PERFORM 1 FROM control.datasets WHERE dataset_id=NEW.dataset_id FOR UPDATE;
    IF TG_OP='INSERT' THEN
        IF NEW.input_complete OR NEW.input_topology_generation IS NOT NULL THEN
            RAISE EXCEPTION 'new acquisition starts with unsealed typed intent and no archive cut' USING ERRCODE='23514';
        END IF;
        PERFORM 1 FROM control.local_accounts
            WHERE dataset_id=NEW.dataset_id AND principal_id=NEW.principal_id FOR SHARE;
    ELSIF NOT (NOT OLD.input_complete AND NEW.input_complete) THEN
        IF (NEW.owner_resource_id,NEW.owner_epoch,NEW.owner_incarnation)
            IS DISTINCT FROM (owner.resource_id,owner.epoch,owner.process_incarnation) THEN
            RAISE EXCEPTION 'acquisition transition names another Control owner' USING ERRCODE='23514';
        END IF;
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER acquisition_job_guard BEFORE INSERT OR UPDATE OR DELETE ON control.jobs
    FOR EACH ROW EXECUTE FUNCTION control.guard_acquisition_job();

-- Reuse the final actual-Control fence, which depends only on NEW.dataset_id.
-- No export pin/input checks participate in this acquisition trigger.
CREATE CONSTRAINT TRIGGER zz_acquisition_job_owner_complete AFTER INSERT OR UPDATE ON control.jobs
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.acquisition_job_protocol=1)
    EXECUTE FUNCTION control.require_export_transition_owner();

CREATE FUNCTION control.guard_acquisition_job_detail() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE job control.jobs%ROWTYPE;
BEGIN
    -- Adapter and job guard acquire dataset/account before this session/source.
    PERFORM 1 FROM control.browser_sessions WHERE dataset_id=NEW.dataset_id
        AND principal_id=NEW.actor_id AND session_id=NEW.session_id FOR SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'acquisition requires its authenticated session' USING ERRCODE='23514';
    END IF;
    SELECT * INTO STRICT job FROM control.jobs WHERE job_id=NEW.job_id FOR UPDATE;
    IF job.acquisition_job_protocol IS DISTINCT FROM 1 OR job.input_complete OR job.state<>'accepted'
        OR job.job_revision<>1 OR job.checkpoint_revision<>0 OR job.owner_resource_id IS NOT NULL
        OR (job.dataset_id,job.principal_id,job.request_key)
            IS DISTINCT FROM (NEW.dataset_id,NEW.actor_id,NEW.request_key) THEN
        RAISE EXCEPTION 'acquisition detail cannot adopt historical jobs' USING ERRCODE='23514';
    END IF;
    PERFORM 1 FROM control.relay_enrollments e JOIN control.acquisition_scopes s
        ON s.dataset_id=e.dataset_id AND s.endpoint_id=e.endpoint_id AND s.access_scope_id=e.access_scope_id
        WHERE s.dataset_id=NEW.dataset_id AND s.scope_id=NEW.scope_id FOR SHARE OF e;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'acquisition scope has no enrollment' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER acquisition_job_detail_guard BEFORE INSERT ON control.acquisition_jobs
    FOR EACH ROW EXECUTE FUNCTION control.guard_acquisition_job_detail();

CREATE FUNCTION control.require_acquisition_acceptance() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE detail control.acquisition_jobs%ROWTYPE; command control.management_commands%ROWTYPE;
    job control.jobs%ROWTYPE; wanted_job bytea; now_us bigint;
BEGIN
    IF TG_TABLE_NAME='management_commands' THEN
        SELECT job_id INTO wanted_job FROM control.acquisition_jobs WHERE actor_id=NEW.actor_id AND request_key=NEW.request_key;
    ELSIF TG_TABLE_NAME='management_audit' THEN
        SELECT job_id INTO wanted_job FROM control.acquisition_jobs WHERE actor_id=NEW.actor_id AND request_key=NEW.subject_id;
    ELSE wanted_job:=NEW.job_id; END IF;
    SELECT * INTO detail FROM control.acquisition_jobs WHERE job_id=wanted_job;
    IF detail.job_id IS NULL THEN
        RAISE EXCEPTION 'acquisition acceptance requires atomic detail' USING ERRCODE='23514';
    END IF;
    SELECT * INTO STRICT job FROM control.jobs WHERE job_id=wanted_job;
    SELECT * INTO STRICT command FROM control.management_commands
        WHERE actor_id=detail.actor_id AND request_key=detail.request_key;
    IF command.action<>'job.acquisition.accept' OR command.dataset_id<>detail.dataset_id
        OR command.request_digest IS DISTINCT FROM control.acquisition_command_digest(detail,command)
        OR job.acquisition_job_protocol IS DISTINCT FROM 1 OR NOT job.input_complete OR job.state<>'accepted'
        OR job.job_revision<>1 OR job.checkpoint_revision<>0 OR job.owner_resource_id IS NOT NULL
        OR job.input_topology_generation IS NOT NULL
        OR (job.dataset_id,job.principal_id,job.request_key,job.request_digest,job.config_revision,job.policy_revision,job.created_at_us)
            IS DISTINCT FROM (detail.dataset_id,detail.actor_id,detail.request_key,command.request_digest,command.expected_config_revision,command.policy_revision,command.accepted_at_us)
        OR detail.deadline_at_us::numeric<>command.accepted_at_us::numeric+detail.duration_us
        OR NOT EXISTS (SELECT 1 FROM control.management_audit a WHERE a.audit_id=command.audit_id
            AND (a.dataset_id,a.actor_id,a.action,a.outcome,a.subject_id,a.config_revision,a.policy_revision,a.created_at_us)
                IS NOT DISTINCT FROM (detail.dataset_id,detail.actor_id,command.action,'accepted'::text,detail.request_key,command.expected_config_revision,command.policy_revision,command.accepted_at_us)) THEN
        RAISE EXCEPTION 'acquisition requires exact atomic command, audit and sealed typed job' USING ERRCODE='23514';
    END IF;
    IF TG_TABLE_NAME='management_audit' THEN
        IF NEW.audit_id IS DISTINCT FROM command.audit_id THEN
            RAISE EXCEPTION 'acquisition audit is not its command receipt' USING ERRCODE='23514';
        END IF;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM control.datasets d
        JOIN control.config_revisions c ON c.dataset_id=d.dataset_id AND c.revision=d.config_revision
        JOIN control.policy_revisions p ON p.dataset_id=d.dataset_id AND p.revision=d.policy_revision
        JOIN control.acquisition_scopes s ON s.dataset_id=d.dataset_id AND s.scope_id=detail.scope_id
        JOIN control.relay_enrollments e ON e.dataset_id=s.dataset_id AND e.endpoint_id=s.endpoint_id AND e.access_scope_id=s.access_scope_id
        WHERE d.dataset_id=detail.dataset_id AND d.config_revision=command.expected_config_revision
            AND d.policy_revision=command.policy_revision AND s.policy_revision=d.policy_revision AND e.enabled
            AND c.document->>'format'='bigbrotr-product-policy/3' AND p.document->>'format'='bigbrotr-product-policy/3'
            AND c.document->'settings'->'sync.enabled'='true'::jsonb AND p.document->'settings'->'sync.enabled'='true'::jsonb)
        OR NOT EXISTS (SELECT 1 FROM control.principal_grants WHERE dataset_id=detail.dataset_id
            AND principal_id=detail.actor_id AND action='roles.control' AND resource_kind='dataset' AND resource_id=detail.dataset_id) THEN
        RAISE EXCEPTION 'acquisition requires current configured scope, enrollment and role permission' USING ERRCODE='23514';
    END IF;
    -- Durable attribution outlives the session. This is only acceptance-time
    -- authentication; later execution must not depend on a logged-out browser.
    now_us:=(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint;
    IF command.accepted_at_us>now_us OR now_us>=detail.deadline_at_us OR NOT EXISTS (
        SELECT 1 FROM control.local_accounts a JOIN control.browser_sessions s
            ON s.dataset_id=a.dataset_id AND s.principal_id=a.principal_id
        WHERE a.dataset_id=detail.dataset_id AND a.principal_id=detail.actor_id AND a.active
            AND a.credential_revision=detail.actor_credential_revision
            AND a.authentication_revision=detail.actor_authentication_revision
            AND a.grant_revision=detail.actor_grant_revision
            AND s.session_id=detail.session_id AND s.revoked_at_us IS NULL
            AND s.credential_revision=a.credential_revision AND s.authentication_revision=a.authentication_revision
            AND now_us>=s.last_seen_at_us AND now_us<LEAST(s.expires_at_us,s.idle_expires_at_us)) THEN
        RAISE EXCEPTION 'acquisition acceptance authority expired before commit' USING ERRCODE='BB001';
    END IF;
    RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER acquisition_command_complete AFTER INSERT ON control.management_commands
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.action='job.acquisition.accept')
    EXECUTE FUNCTION control.require_acquisition_acceptance();
CREATE CONSTRAINT TRIGGER acquisition_audit_complete AFTER INSERT ON control.management_audit
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.action='job.acquisition.accept')
    EXECUTE FUNCTION control.require_acquisition_acceptance();
CREATE CONSTRAINT TRIGGER acquisition_job_complete AFTER INSERT ON control.jobs
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.acquisition_job_protocol=1)
    EXECUTE FUNCTION control.require_acquisition_acceptance();
CREATE CONSTRAINT TRIGGER acquisition_job_detail_complete AFTER INSERT ON control.acquisition_jobs
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION control.require_acquisition_acceptance();

REVOKE ALL ON control.acquisition_jobs FROM PUBLIC;
REVOKE ALL ON FUNCTION control.acquisition_command_digest(control.acquisition_jobs,control.management_commands),
    control.guard_acquisition_job(),control.guard_acquisition_job_detail(),control.require_acquisition_acceptance() FROM PUBLIC;
