-- Explicit first export-job participation. Existing jobs/pins remain unchanged
-- and cannot be relabelled into this protocol. No historical adoption/backfill.
ALTER TABLE control.jobs ADD COLUMN export_job_protocol smallint
    CHECK (export_job_protocol IS NULL OR (kind='export' AND export_job_protocol=1));
ALTER TABLE control.archive_snapshot_pins ADD COLUMN export_job_id control.id16
    UNIQUE REFERENCES control.jobs(job_id);

ALTER TABLE control.management_commands DROP CONSTRAINT management_command_shape;
ALTER TABLE control.management_commands ADD CONSTRAINT management_command_shape CHECK (
    (action='policy.activate-initial' AND authority_mode IS NOT NULL
        AND authority_mode IN ('managed','declarative') AND document_digest IS NOT NULL AND policy_revision=1)
    OR (action IN ('scope-grant.change','job.export.accept') AND authority_mode IS NULL
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
    OR (action IN ('browser.session.issue','browser.session.revoke','browser.session.reauthenticate','scope-grant.change','job.export.accept') AND outcome='accepted'
        AND config_revision IS NOT NULL AND config_revision>0 AND policy_revision IS NOT NULL AND policy_revision>0
        AND subject_id IS NOT NULL AND reason IS NULL AND requester_database_role IS NULL AND requester_os_uid IS NULL)
);

CREATE TABLE control.export_jobs (
    job_id control.id16 PRIMARY KEY REFERENCES control.jobs(job_id),
    dataset_id control.id16 NOT NULL,
    actor_id control.id16 NOT NULL,
    request_key control.id16 NOT NULL,
    format text NOT NULL CHECK (format='signed-events-jsonl'),
    visibility text NOT NULL CHECK (visibility='private'),
    dictionary_kind smallint NOT NULL CHECK (dictionary_kind=4),
    allocation_domain control.id16 NOT NULL,
    ordinal control.u64 NOT NULL CHECK (ordinal>0),
    event_id control.digest32 NOT NULL,
    page_size smallint NOT NULL CHECK (page_size BETWEEN 1 AND 1000),
    catalog_id control.id16 NOT NULL,
    frontier_revision control.u64 NOT NULL CHECK (frontier_revision>0),
    destination_id control.id16 NOT NULL,
    destination_binding_sha256 control.digest32 NOT NULL,
    pin_lease_id control.id16 NOT NULL UNIQUE REFERENCES control.archive_snapshot_pins(lease_id),
    allocation_pin_id control.id16 NOT NULL REFERENCES control.archive_snapshot_pins(lease_id),
    session_id control.id16 NOT NULL,
    actor_credential_revision control.u64 NOT NULL CHECK (actor_credential_revision>0),
    actor_authentication_revision control.u64 NOT NULL CHECK (actor_authentication_revision>0),
    actor_grant_revision control.u64 NOT NULL CHECK (actor_grant_revision>0),
    UNIQUE (actor_id,request_key),
    FOREIGN KEY (actor_id,request_key) REFERENCES control.management_commands(actor_id,request_key),
    FOREIGN KEY (dataset_id,actor_id) REFERENCES control.local_accounts(dataset_id,principal_id),
    FOREIGN KEY (catalog_id,frontier_revision) REFERENCES control.archive_frontier_snapshots(consumer_id,frontier_revision),
    CHECK (pin_lease_id<>allocation_pin_id)
);
CREATE TRIGGER immutable_export_jobs BEFORE UPDATE OR DELETE ON control.export_jobs
    FOR EACH ROW EXECUTE FUNCTION control.reject_history_mutation();
CREATE TRIGGER export_jobs_truncate_guard BEFORE TRUNCATE ON control.export_jobs
    FOR EACH STATEMENT EXECUTE FUNCTION control.reject_history_mutation();

CREATE FUNCTION control.export_command_digest(detail control.export_jobs,
    command control.management_commands) RETURNS bytea
LANGUAGE sql IMMUTABLE STRICT SET search_path=pg_catalog AS $$
    SELECT sha256(convert_to('bigbrotr/export-event/1','UTF8') || decode('00','hex')
        || detail.dataset_id || detail.actor_id || detail.request_key || detail.dataset_id
        || decode('0004','hex') || detail.allocation_domain || control.command_u64_bytes(detail.ordinal)
        || detail.event_id || int2send(detail.page_size) || detail.catalog_id
        || control.command_u64_bytes(detail.frontier_revision) || detail.destination_id
        || detail.destination_binding_sha256 || decode('0101','hex')
        || control.command_u64_bytes(command.expected_config_revision)
        || control.command_u64_bytes(command.policy_revision))
$$;

CREATE FUNCTION control.guard_export_job() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE owner control.owner_epochs%ROWTYPE;
BEGIN
    IF TG_OP='DELETE' THEN
        RAISE EXCEPTION 'job correctness obligations require explicit reclamation' USING ERRCODE='23514';
    END IF;
    IF TG_OP='UPDATE' AND NEW.export_job_protocol IS DISTINCT FROM OLD.export_job_protocol THEN
        RAISE EXCEPTION 'export job protocol is immutable' USING ERRCODE='23514';
    END IF;
    IF NEW.export_job_protocol IS NULL THEN RETURN NEW; END IF;
    IF current_setting('transaction_isolation')<>'read committed' THEN
        RAISE EXCEPTION 'export jobs require READ COMMITTED' USING ERRCODE='25001';
    END IF;
    SELECT * INTO STRICT owner FROM control.owner_epochs
        WHERE dataset_id=NEW.dataset_id AND role='control' AND state='active';
    PERFORM control.require_control_owner(owner.dataset_id,owner.resource_id,owner.epoch,owner.process_incarnation);
    IF owner.lease_expires_at_us<=(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint THEN
        RAISE EXCEPTION 'export mutation requires live actual Control owner' USING ERRCODE='23514';
    END IF;
    IF TG_OP='INSERT' THEN
        IF NEW.input_complete OR NEW.input_topology_generation IS NULL THEN
            RAISE EXCEPTION 'new qualified export starts with its unsealed source cut' USING ERRCODE='23514';
        END IF;
        -- Serialize policy/account changes before the consumer and pin locks.
        -- The adapter has already locked its authenticated browser session.
        PERFORM 1 FROM control.datasets WHERE dataset_id=NEW.dataset_id FOR SHARE;
        PERFORM 1 FROM control.local_accounts
            WHERE dataset_id=NEW.dataset_id AND principal_id=NEW.principal_id FOR SHARE;
    ELSIF NOT (NOT OLD.input_complete AND NEW.input_complete) THEN
        IF (NEW.owner_resource_id,NEW.owner_epoch,NEW.owner_incarnation)
            IS DISTINCT FROM (owner.resource_id,owner.epoch,owner.process_incarnation) THEN
            RAISE EXCEPTION 'export transition names another Control owner' USING ERRCODE='23514';
        END IF;
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER export_job_guard BEFORE INSERT OR UPDATE OR DELETE ON control.jobs
    FOR EACH ROW EXECUTE FUNCTION control.guard_export_job();

-- Final fencing is separate from initial acceptance: a paused/expired-pin job
-- must still be cancellable. Order after job_input_complete, whose source-row
-- locks can wait beyond the BEFORE guard's authority observation.
CREATE FUNCTION control.require_export_transition_owner() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE owner control.owner_epochs%ROWTYPE;
BEGIN
    SELECT * INTO STRICT owner FROM control.owner_epochs
        WHERE dataset_id=NEW.dataset_id AND role='control' AND state='active';
    PERFORM control.require_control_owner(owner.dataset_id,owner.resource_id,owner.epoch,owner.process_incarnation);
    IF owner.lease_expires_at_us<=(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint THEN
        RAISE EXCEPTION 'export transition authority expired before commit' USING ERRCODE='BB001';
    END IF;
    RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER zz_export_job_owner_complete AFTER INSERT OR UPDATE ON control.jobs
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.export_job_protocol=1)
    EXECUTE FUNCTION control.require_export_transition_owner();

CREATE FUNCTION control.guard_export_pin_link() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
    -- Existing0017 rejects all attribution updates/deletes. New non-export pins
    -- cannot acquire job linkage; new export pins cannot precede their new job.
    IF (NEW.purpose='export' AND (NEW.export_job_id IS NULL OR NOT EXISTS (
        SELECT 1 FROM control.jobs j WHERE j.job_id=NEW.export_job_id AND j.dataset_id=NEW.dataset_id
            AND j.export_job_protocol=1 AND NOT j.input_complete AND j.state='accepted'
            AND j.job_revision=1 AND j.checkpoint_revision=0 AND j.owner_resource_id IS NULL)))
        OR (NEW.purpose<>'export' AND NEW.export_job_id IS NOT NULL) THEN
        RAISE EXCEPTION 'export pin must belong to a new unsealed qualified job' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER export_pin_link_guard BEFORE INSERT ON control.archive_snapshot_pins
    FOR EACH ROW EXECUTE FUNCTION control.guard_export_pin_link();

CREATE FUNCTION control.guard_export_detail() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE job control.jobs%ROWTYPE; pin control.archive_snapshot_pins%ROWTYPE;
BEGIN
    -- Typed callers preacquire account/session before creating the job/pin.
    -- Retain the exact session row as well: deferred SELECT alone cannot
    -- serialize revocation from an independently executing SQL writer.
    PERFORM 1 FROM control.browser_sessions WHERE dataset_id=NEW.dataset_id
        AND principal_id=NEW.actor_id AND session_id=NEW.session_id FOR SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'export requires its authenticated session' USING ERRCODE='23514';
    END IF;
    PERFORM control.lock_archive_pins(NEW.dataset_id,NEW.catalog_id);
    SELECT * INTO STRICT job FROM control.jobs WHERE job_id=NEW.job_id FOR UPDATE;
    SELECT * INTO STRICT pin FROM control.archive_snapshot_pins WHERE lease_id=NEW.pin_lease_id;
    IF job.export_job_protocol IS DISTINCT FROM 1 OR job.input_complete OR job.state<>'accepted'
        OR job.job_revision<>1 OR job.checkpoint_revision<>0 OR job.owner_resource_id IS NOT NULL
        OR (job.dataset_id,job.principal_id,job.request_key) IS DISTINCT FROM (NEW.dataset_id,NEW.actor_id,NEW.request_key)
        OR (pin.export_job_id,pin.dataset_id,pin.consumer_id,pin.frontier_revision,pin.purpose)
            IS DISTINCT FROM (NEW.job_id,NEW.dataset_id,NEW.catalog_id,NEW.frontier_revision,'export'::text) THEN
        RAISE EXCEPTION 'export detail cannot adopt historical jobs or pins' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER export_detail_guard BEFORE INSERT ON control.export_jobs
    FOR EACH ROW EXECUTE FUNCTION control.guard_export_detail();

CREATE FUNCTION control.require_export_acceptance() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE detail control.export_jobs%ROWTYPE; command control.management_commands%ROWTYPE;
    job control.jobs%ROWTYPE; pin control.archive_snapshot_pins%ROWTYPE;
    lease control.snapshot_leases%ROWTYPE; frontier control.archive_frontier_snapshots%ROWTYPE;
    wanted_job bytea; now_us bigint;
BEGIN
    IF TG_TABLE_NAME='management_commands' THEN
        SELECT job_id INTO wanted_job FROM control.export_jobs WHERE actor_id=NEW.actor_id AND request_key=NEW.request_key;
    ELSIF TG_TABLE_NAME='management_audit' THEN
        SELECT job_id INTO wanted_job FROM control.export_jobs WHERE actor_id=NEW.actor_id AND request_key=NEW.subject_id;
    ELSIF TG_TABLE_NAME='archive_snapshot_pins' THEN wanted_job:=NEW.export_job_id;
    ELSE wanted_job:=NEW.job_id; END IF;
    SELECT * INTO detail FROM control.export_jobs WHERE job_id=wanted_job;
    IF detail.job_id IS NULL THEN
        RAISE EXCEPTION 'export acceptance requires atomic detail' USING ERRCODE='23514';
    END IF;
    SELECT * INTO STRICT job FROM control.jobs WHERE job_id=wanted_job;
    SELECT * INTO STRICT command FROM control.management_commands WHERE actor_id=detail.actor_id AND request_key=detail.request_key;
    SELECT * INTO STRICT pin FROM control.archive_snapshot_pins WHERE lease_id=detail.pin_lease_id;
    SELECT * INTO STRICT lease FROM control.snapshot_leases WHERE lease_id=detail.pin_lease_id;
    SELECT * INTO STRICT frontier FROM control.archive_frontier_snapshots
        WHERE consumer_id=detail.catalog_id AND frontier_revision=detail.frontier_revision;
    IF command.action<>'job.export.accept' OR command.dataset_id<>detail.dataset_id
        OR command.request_digest IS DISTINCT FROM control.export_command_digest(detail,command)
        OR job.export_job_protocol IS DISTINCT FROM 1 OR NOT job.input_complete OR job.state<>'accepted'
        OR job.job_revision<>1 OR job.checkpoint_revision<>0 OR job.owner_resource_id IS NOT NULL
        OR (job.dataset_id,job.principal_id,job.request_key,job.request_digest,job.config_revision,job.policy_revision,job.created_at_us,job.input_topology_generation)
            IS DISTINCT FROM (detail.dataset_id,detail.actor_id,detail.request_key,command.request_digest,command.expected_config_revision,command.policy_revision,command.accepted_at_us,frontier.topology_generation)
        OR (pin.export_job_id,pin.dataset_id,pin.consumer_id,pin.frontier_revision,pin.purpose)
            IS DISTINCT FROM (job.job_id,detail.dataset_id,detail.catalog_id,detail.frontier_revision,'export'::text)
        OR lease.state<>'active' OR lease.lease_revision<>1 OR lease.archive_pin_protocol IS DISTINCT FROM 1
        OR NOT EXISTS (SELECT 1 FROM control.datasets d WHERE d.dataset_id=detail.dataset_id
            AND d.config_revision=command.expected_config_revision AND d.policy_revision=command.policy_revision)
        OR NOT EXISTS (SELECT 1 FROM control.management_audit a WHERE a.audit_id=command.audit_id
            AND (a.dataset_id,a.actor_id,a.action,a.outcome,a.subject_id,a.config_revision,a.policy_revision,a.created_at_us)
                IS NOT DISTINCT FROM (detail.dataset_id,detail.actor_id,command.action,'accepted'::text,detail.request_key,command.expected_config_revision,command.policy_revision,command.accepted_at_us)) THEN
        RAISE EXCEPTION 'export acceptance requires exact atomic command, audit, job and new pin' USING ERRCODE='23514';
    END IF;
    IF TG_TABLE_NAME='management_audit' THEN
        IF NEW.audit_id IS DISTINCT FROM command.audit_id THEN
            RAISE EXCEPTION 'export audit is not its command receipt' USING ERRCODE='23514';
        END IF;
    ELSIF TG_TABLE_NAME='archive_snapshot_pins' THEN
        IF NEW.lease_id IS DISTINCT FROM detail.pin_lease_id THEN
            RAISE EXCEPTION 'export pin is not its job retention owner' USING ERRCODE='23514';
        END IF;
    END IF;
    -- Equality in both directions includes zero prefixes and prohibits a job
    -- from declaring a weaker/stronger or different cut than its retained input.
    IF EXISTS ((SELECT shard_id,owner_epoch,last_complete_sequence FROM control.job_input_bounds WHERE job_id=wanted_job)
        EXCEPT (SELECT shard_id,owner_epoch,last_complete_sequence FROM control.consumer_frontiers
            WHERE consumer_id=detail.catalog_id AND frontier_revision=detail.frontier_revision))
        OR EXISTS ((SELECT shard_id,owner_epoch,last_complete_sequence FROM control.consumer_frontiers
            WHERE consumer_id=detail.catalog_id AND frontier_revision=detail.frontier_revision)
        EXCEPT (SELECT shard_id,owner_epoch,last_complete_sequence FROM control.job_input_bounds WHERE job_id=wanted_job)) THEN
        RAISE EXCEPTION 'export job input must equal its complete retained frontier' USING ERRCODE='23514';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM control.local_accounts a JOIN control.browser_sessions s
        ON s.dataset_id=a.dataset_id AND s.principal_id=a.principal_id
        WHERE a.dataset_id=detail.dataset_id AND a.principal_id=detail.actor_id AND a.active
            AND a.credential_revision=detail.actor_credential_revision AND a.authentication_revision=detail.actor_authentication_revision
            AND a.grant_revision=detail.actor_grant_revision AND s.session_id=detail.session_id AND s.revoked_at_us IS NULL
            AND s.credential_revision=a.credential_revision AND s.authentication_revision=a.authentication_revision)
        OR NOT EXISTS (SELECT 1 FROM control.principal_grants WHERE dataset_id=detail.dataset_id AND principal_id=detail.actor_id
            AND action='data.read' AND resource_kind='dataset' AND resource_id=detail.dataset_id)
        OR EXISTS (SELECT 1 FROM (VALUES ('history.read'),('export'),('private-export')) wanted(action)
            WHERE NOT EXISTS (SELECT 1 FROM control.scope_grants g WHERE g.dataset_id=detail.dataset_id AND g.principal_id=detail.actor_id
                AND g.action=wanted.action AND g.dictionary_kind=4 AND g.allocation_domain=detail.allocation_domain AND g.ordinal=detail.ordinal AND g.enabled)) THEN
        RAISE EXCEPTION 'export acceptance requires current explicit input and export grants' USING ERRCODE='23514';
    END IF;
    IF NOT control.scope_pin_live(detail.dataset_id,detail.catalog_id,detail.frontier_revision,detail.allocation_pin_id) THEN
        RAISE EXCEPTION 'export allocation authority expired before commit' USING ERRCODE='BB001';
    END IF;
    PERFORM control.require_archive_pin_holder(lease,true);
    -- Both holder and allocation helpers can wait. Sample session time only
    -- after those locks, never using the timestamp from before a slow response.
    now_us:=(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint;
    IF NOT EXISTS (SELECT 1 FROM control.browser_sessions s WHERE s.session_id=detail.session_id
            AND s.revoked_at_us IS NULL AND s.dataset_id=detail.dataset_id AND s.principal_id=detail.actor_id
            AND s.credential_revision=detail.actor_credential_revision AND s.authentication_revision=detail.actor_authentication_revision
            AND now_us>=s.last_seen_at_us AND now_us<LEAST(s.expires_at_us,s.idle_expires_at_us)) THEN
        RAISE EXCEPTION 'export acceptance authority expired before commit' USING ERRCODE='BB001';
    END IF;
    RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER export_command_complete AFTER INSERT ON control.management_commands
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.action='job.export.accept') EXECUTE FUNCTION control.require_export_acceptance();
CREATE CONSTRAINT TRIGGER export_audit_complete AFTER INSERT ON control.management_audit
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.action='job.export.accept') EXECUTE FUNCTION control.require_export_acceptance();
CREATE CONSTRAINT TRIGGER export_job_complete AFTER INSERT ON control.jobs
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.export_job_protocol=1) EXECUTE FUNCTION control.require_export_acceptance();
CREATE CONSTRAINT TRIGGER export_detail_complete AFTER INSERT ON control.export_jobs
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION control.require_export_acceptance();
CREATE CONSTRAINT TRIGGER export_pin_complete AFTER INSERT ON control.archive_snapshot_pins
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.export_job_id IS NOT NULL) EXECUTE FUNCTION control.require_export_acceptance();

CREATE FUNCTION control.guard_export_retention() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE job control.jobs%ROWTYPE; pin control.archive_snapshot_pins%ROWTYPE;
BEGIN
    SELECT * INTO pin FROM control.archive_snapshot_pins WHERE lease_id=NEW.lease_id;
    IF pin.export_job_id IS NULL THEN RETURN NEW; END IF;
    PERFORM control.lock_archive_pins(pin.dataset_id,pin.consumer_id);
    -- Actual callers must acquire this before the UPDATE locks the lease/claim.
    -- This trigger cannot repair an already inverted row-lock acquisition order.
    SELECT * INTO STRICT job FROM control.jobs WHERE job_id=pin.export_job_id FOR UPDATE;
    IF NEW.state<>'active' AND job.state NOT IN ('succeeded','partial','failed','cancelled','skipped') THEN
        RAISE EXCEPTION 'nonterminal export retains its lease and every claim' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER export_lease_retention BEFORE UPDATE ON control.snapshot_leases
    FOR EACH ROW EXECUTE FUNCTION control.guard_export_retention();
CREATE TRIGGER export_claim_retention BEFORE UPDATE ON control.retention_claims
    FOR EACH ROW EXECUTE FUNCTION control.guard_export_retention();

REVOKE ALL ON control.export_jobs FROM PUBLIC;
REVOKE ALL ON FUNCTION control.export_command_digest(control.export_jobs,control.management_commands),
    control.guard_export_job(),control.guard_export_pin_link(),control.guard_export_detail(),
    control.require_export_acceptance(),control.guard_export_retention(),control.require_export_transition_owner() FROM PUBLIC;
