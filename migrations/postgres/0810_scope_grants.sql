-- Extend the one command namespace. Legacy initial activation keeps its exact
-- shape and uniqueness; neither existing grants nor proofs are reinterpreted.
ALTER TABLE control.management_commands DROP CONSTRAINT management_commands_action_check;
ALTER TABLE control.management_commands DROP CONSTRAINT management_commands_authority_mode_check;
ALTER TABLE control.management_commands DROP CONSTRAINT management_commands_policy_revision_check;
ALTER TABLE control.management_commands DROP CONSTRAINT management_commands_dataset_id_action_key;
ALTER TABLE control.management_commands ALTER COLUMN authority_mode DROP NOT NULL;
ALTER TABLE control.management_commands ALTER COLUMN document_digest DROP NOT NULL;
ALTER TABLE control.management_commands ADD CONSTRAINT management_command_shape CHECK (
    (action = 'policy.activate-initial' AND authority_mode IS NOT NULL
        AND authority_mode IN ('managed','declarative') AND document_digest IS NOT NULL AND policy_revision = 1)
    OR (action = 'scope-grant.change' AND authority_mode IS NULL AND document_digest IS NULL AND policy_revision > 0)
);
CREATE UNIQUE INDEX one_initial_activation_command ON control.management_commands(dataset_id)
    WHERE action = 'policy.activate-initial';

ALTER TABLE control.management_audit DROP CONSTRAINT management_audit_shape;
ALTER TABLE control.management_audit ADD CONSTRAINT management_audit_shape CHECK (
    (policy_revision IS NULL AND (
        (action = 'dataset.initialize' AND outcome = 'accepted' AND config_revision IS NOT NULL AND config_revision = 1 AND subject_id IS NULL AND reason IS NULL)
        OR (action IN ('owner-token.issue', 'owner-token.revoke', 'owner.enroll') AND outcome = 'accepted' AND config_revision IS NULL AND subject_id IS NOT NULL AND reason IS NULL AND requester_database_role IS NOT NULL AND requester_os_uid IS NOT NULL)
        OR (action = 'owner.enroll' AND outcome = 'rejected' AND config_revision IS NULL AND subject_id IS NOT NULL AND reason IS NOT NULL AND reason IN ('invalid-token', 'expired', 'exhausted', 'terminal') AND requester_database_role IS NOT NULL AND requester_os_uid IS NOT NULL)
        OR (action = 'policy.activate-initial' AND outcome = 'accepted' AND config_revision IS NOT NULL AND config_revision > 0 AND subject_id IS NOT NULL AND reason IS NULL AND requester_database_role IS NOT NULL AND requester_os_uid IS NOT NULL)
    ))
    OR (action IN ('browser.session.issue', 'browser.session.revoke', 'browser.session.reauthenticate', 'scope-grant.change') AND outcome = 'accepted'
        AND config_revision IS NOT NULL AND config_revision > 0
        AND policy_revision IS NOT NULL AND policy_revision > 0
        AND subject_id IS NOT NULL AND reason IS NULL
        AND requester_database_role IS NULL AND requester_os_uid IS NULL)
);

CREATE TABLE control.scope_grant_changes (
    actor_id control.id16 NOT NULL,
    request_key control.id16 NOT NULL,
    dataset_id control.id16 NOT NULL,
    principal_id control.id16 NOT NULL,
    action text NOT NULL CHECK (action = 'history.read'),
    dictionary_kind smallint NOT NULL CHECK (dictionary_kind = 4),
    allocation_domain control.id16 NOT NULL,
    ordinal control.u64 NOT NULL CHECK (ordinal > 0),
    enabled boolean NOT NULL,
    scope_generation control.u64 NOT NULL CHECK (scope_generation > 0),
    expected_grant_revision control.u64 NOT NULL CHECK (expected_grant_revision > 0),
    resulting_grant_revision control.u64 NOT NULL CHECK (resulting_grant_revision = expected_grant_revision + 1),
    proof_id control.id16 NOT NULL UNIQUE,
    session_id control.id16 NOT NULL,
    actor_credential_revision control.u64 NOT NULL CHECK (actor_credential_revision > 0),
    actor_authentication_revision control.u64 NOT NULL CHECK (actor_authentication_revision > 0),
    actor_grant_revision control.u64 NOT NULL CHECK (actor_grant_revision > 0),
    proof_expires_at_us bigint NOT NULL,
    catalog_id control.id16,
    frontier_revision control.u64,
    pin_lease_id control.id16 REFERENCES control.archive_snapshot_pins(lease_id),
    PRIMARY KEY (actor_id, request_key),
    UNIQUE (dataset_id, principal_id, resulting_grant_revision),
    UNIQUE (dataset_id, principal_id, action, dictionary_kind, allocation_domain, ordinal, scope_generation),
    FOREIGN KEY (actor_id, request_key) REFERENCES control.management_commands(actor_id, request_key),
    FOREIGN KEY (dataset_id, principal_id) REFERENCES control.local_accounts(dataset_id, principal_id),
    FOREIGN KEY (catalog_id, frontier_revision) REFERENCES control.archive_frontier_snapshots(consumer_id, frontier_revision),
    CHECK ((enabled AND catalog_id IS NOT NULL AND frontier_revision IS NOT NULL AND frontier_revision > 0 AND pin_lease_id IS NOT NULL)
        OR (NOT enabled AND catalog_id IS NULL AND frontier_revision IS NULL AND pin_lease_id IS NULL))
);
CREATE TRIGGER immutable_scope_grant_changes BEFORE UPDATE OR DELETE ON control.scope_grant_changes
    FOR EACH ROW EXECUTE FUNCTION control.reject_history_mutation();

-- A disabled row is a permanent generation tombstone, not an absent allocation.
CREATE TABLE control.scope_grants (
    dataset_id control.id16 NOT NULL,
    principal_id control.id16 NOT NULL,
    action text NOT NULL CHECK (action = 'history.read'),
    dictionary_kind smallint NOT NULL CHECK (dictionary_kind = 4),
    allocation_domain control.id16 NOT NULL,
    ordinal control.u64 NOT NULL CHECK (ordinal > 0),
    enabled boolean NOT NULL,
    generation control.u64 NOT NULL CHECK (generation > 0),
    actor_id control.id16 NOT NULL,
    request_key control.id16 NOT NULL,
    PRIMARY KEY (dataset_id, principal_id, action, dictionary_kind, allocation_domain, ordinal),
    FOREIGN KEY (actor_id, request_key) REFERENCES control.scope_grant_changes(actor_id, request_key),
    FOREIGN KEY (dataset_id, principal_id) REFERENCES control.local_accounts(dataset_id, principal_id)
);

-- Fixed-width portable u64, including values above PostgreSQL bigint's maximum.
CREATE FUNCTION control.command_u64_bytes(n control.u64) RETURNS bytea
LANGUAGE sql IMMUTABLE STRICT SET search_path = pg_catalog AS $$
    SELECT decode(lpad(to_hex(div(n,4294967296)::bigint),8,'0') ||
        lpad(to_hex(mod(n,4294967296)::bigint),8,'0'),'hex')
$$;

-- Must run inside the accepting Control transaction and after the native
-- catalog barrier. This checks retention/holder validity, not native file bytes.
CREATE FUNCTION control.scope_pin_live(wanted_dataset bytea, wanted_catalog bytea,
    wanted_frontier numeric, wanted_lease bytea) RETURNS boolean
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE lease control.snapshot_leases%ROWTYPE; pin control.archive_snapshot_pins%ROWTYPE;
    frontier control.archive_frontier_snapshots%ROWTYPE; holder control.owner_epochs%ROWTYPE; now_us bigint;
BEGIN
    PERFORM control.lock_archive_pins(wanted_dataset,wanted_catalog);
    SELECT * INTO lease FROM control.snapshot_leases WHERE lease_id = wanted_lease FOR UPDATE;
    SELECT * INTO pin FROM control.archive_snapshot_pins WHERE lease_id = wanted_lease;
    SELECT * INTO frontier FROM control.archive_frontier_snapshots
        WHERE consumer_id = wanted_catalog AND frontier_revision = wanted_frontier;
    IF lease.lease_id IS NULL OR pin.lease_id IS NULL OR frontier.operation_id IS NULL
        OR lease.state <> 'active' OR lease.archive_pin_protocol IS DISTINCT FROM 1
        OR (lease.dataset_id,lease.consumer_id,lease.frontier_revision,lease.topology_generation)
            IS DISTINCT FROM (wanted_dataset,wanted_catalog,wanted_frontier,frontier.topology_generation)
        OR (pin.dataset_id,pin.consumer_id,pin.frontier_revision,pin.purpose)
            IS DISTINCT FROM (wanted_dataset,wanted_catalog,wanted_frontier,'reader'::text)
        OR NOT EXISTS (SELECT 1 FROM control.consumer_frontier_revisions r JOIN control.topology_generations t
            ON t.dataset_id=r.dataset_id AND t.generation=r.topology_generation
            WHERE r.consumer_id=wanted_catalog AND r.frontier_revision=wanted_frontier AND r.sealed AND t.sealed)
        OR EXISTS (SELECT 1 FROM control.topology_streams t WHERE t.dataset_id=wanted_dataset
            AND t.topology_generation=frontier.topology_generation AND NOT EXISTS (
                SELECT 1 FROM control.consumer_frontiers b WHERE b.consumer_id=wanted_catalog AND b.frontier_revision=wanted_frontier
                    AND b.shard_id=t.shard_id AND b.owner_epoch=t.owner_epoch))
        OR EXISTS (SELECT 1 FROM control.consumer_frontiers b WHERE b.consumer_id=wanted_catalog AND b.frontier_revision=wanted_frontier
            AND b.last_complete_sequence>0 AND NOT EXISTS (SELECT 1 FROM control.retention_claims c
                WHERE c.lease_id=wanted_lease AND c.dataset_id=wanted_dataset AND c.topology_generation=frontier.topology_generation
                    AND c.shard_id=b.shard_id AND c.owner_epoch=b.owner_epoch AND c.first_sequence=1
                    AND c.last_sequence=b.last_complete_sequence AND c.purpose='reader' AND c.state='active')) THEN
        RETURN false;
    END IF;
    SELECT * INTO holder FROM control.owner_epochs WHERE dataset_id=wanted_dataset AND resource_id=pin.issuer_resource
        AND epoch=pin.issuer_epoch AND process_incarnation=pin.issuer_incarnation FOR SHARE;
    IF holder.resource_id IS NULL OR holder.state<>'active'
        OR holder.control_backend_pid IS DISTINCT FROM pg_backend_pid()
        OR holder.role_instance_id IS DISTINCT FROM lease.holder_role_instance_id
        OR holder.process_incarnation IS DISTINCT FROM lease.holder_incarnation THEN RETURN false; END IF;
    -- false still demands the actual current backend/holder, but does not mistake
    -- scheduling expiry for exclusion or raise on an ordinary expired read.
    PERFORM control.require_archive_pin_holder(lease,false);
    now_us := (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint;
    RETURN lease.expires_at_us > now_us AND lease.acquired_at_us <= now_us
        AND EXISTS (SELECT 1 FROM control.owner_epochs o WHERE o.resource_id=pin.issuer_resource
            AND o.epoch=pin.issuer_epoch AND o.process_incarnation=pin.issuer_incarnation
            AND o.state='active' AND o.lease_expires_at_us >= lease.expires_at_us AND o.lease_expires_at_us > now_us);
END $$;

CREATE FUNCTION control.guard_scope_grant_change() RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE command control.management_commands%ROWTYPE; proof control.reauthentication_proofs%ROWTYPE;
    actor control.local_accounts%ROWTYPE; target control.local_accounts%ROWTYPE;
    session control.browser_sessions%ROWTYPE; previous control.scope_grants%ROWTYPE;
    dataset control.datasets%ROWTYPE; owner control.owner_epochs%ROWTYPE; digest bytea; now_us bigint;
BEGIN
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'scope grants require read committed' USING ERRCODE='23514';
    END IF;
    SELECT * INTO STRICT owner FROM control.owner_epochs WHERE dataset_id=NEW.dataset_id AND role='control' AND state='active';
    PERFORM control.require_control_owner(owner.dataset_id,owner.resource_id,owner.epoch,owner.process_incarnation);
    SELECT * INTO STRICT dataset FROM control.datasets WHERE dataset_id=NEW.dataset_id FOR UPDATE;
    -- Callers take this same order before issuing any account/session/proof DML.
    PERFORM 1 FROM control.local_accounts WHERE dataset_id=NEW.dataset_id
        AND principal_id IN (NEW.actor_id,NEW.principal_id) ORDER BY principal_id FOR UPDATE;
    SELECT * INTO STRICT actor FROM control.local_accounts WHERE dataset_id=NEW.dataset_id AND principal_id=NEW.actor_id;
    SELECT * INTO STRICT target FROM control.local_accounts WHERE dataset_id=NEW.dataset_id AND principal_id=NEW.principal_id;
    SELECT * INTO session FROM control.browser_sessions WHERE session_id=NEW.session_id FOR UPDATE;
    SELECT * INTO proof FROM control.reauthentication_proofs WHERE proof_id=NEW.proof_id FOR UPDATE;
    SELECT * INTO STRICT command FROM control.management_commands WHERE actor_id=NEW.actor_id AND request_key=NEW.request_key;
    SELECT * INTO previous FROM control.scope_grants WHERE dataset_id=NEW.dataset_id AND principal_id=NEW.principal_id
        AND action=NEW.action AND dictionary_kind=NEW.dictionary_kind AND allocation_domain=NEW.allocation_domain AND ordinal=NEW.ordinal;
    digest := sha256(convert_to('bigbrotr/scope-grant-change/1','UTF8') || decode('00','hex') || NEW.dataset_id
        || NEW.actor_id || NEW.request_key || NEW.principal_id || NEW.dataset_id || decode('0004','hex')
        || NEW.allocation_domain || control.command_u64_bytes(NEW.ordinal) || decode('01','hex')
        || CASE WHEN NEW.enabled THEN decode('01','hex') ELSE decode('00','hex') END
        || control.command_u64_bytes(command.expected_config_revision) || control.command_u64_bytes(command.policy_revision)
        || control.command_u64_bytes(NEW.expected_grant_revision)
        || CASE WHEN NEW.enabled THEN decode('01','hex') || NEW.catalog_id || control.command_u64_bytes(NEW.frontier_revision)
            ELSE decode('00','hex') END);
    now_us := (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint;
    IF command.action <> 'scope-grant.change' OR command.dataset_id <> NEW.dataset_id OR command.request_digest IS DISTINCT FROM digest
        OR (command.expected_config_revision,command.policy_revision) IS DISTINCT FROM (dataset.config_revision,dataset.policy_revision)
        OR NOT actor.active OR NOT target.active OR target.grant_revision <> NEW.expected_grant_revision
        OR (NEW.actor_credential_revision,NEW.actor_authentication_revision,NEW.actor_grant_revision)
            IS DISTINCT FROM (actor.credential_revision,actor.authentication_revision,actor.grant_revision)
        OR proof.proof_id IS NULL OR session.session_id IS NULL
        OR (proof.dataset_id,proof.actor_id,proof.session_id,proof.request_key,proof.request_digest,proof.config_revision,proof.policy_revision,
            proof.credential_revision,proof.authentication_revision,proof.grant_revision,proof.expires_at_us)
            IS DISTINCT FROM (NEW.dataset_id,NEW.actor_id,NEW.session_id,NEW.request_key,digest,command.expected_config_revision,command.policy_revision,
                NEW.actor_credential_revision,NEW.actor_authentication_revision,NEW.actor_grant_revision,NEW.proof_expires_at_us)
        OR (session.dataset_id,session.principal_id,session.credential_revision,session.authentication_revision)
            IS DISTINCT FROM (NEW.dataset_id,NEW.actor_id,actor.credential_revision,actor.authentication_revision)
        OR session.revoked_at_us IS NOT NULL OR now_us < session.last_seen_at_us
        OR command.accepted_at_us > now_us OR command.accepted_at_us < proof.issued_at_us
        OR NOT EXISTS (SELECT 1 FROM control.principal_grants WHERE dataset_id=NEW.dataset_id AND principal_id=NEW.actor_id
            AND action='principals.manage' AND resource_kind='dataset' AND resource_id=NEW.dataset_id)
        OR (previous.generation IS NULL AND (NOT NEW.enabled OR NEW.scope_generation<>1))
        OR (previous.generation IS NOT NULL AND (previous.enabled=NEW.enabled OR NEW.scope_generation<>previous.generation+1)) THEN
        RAISE EXCEPTION 'scope grant command does not match current authority and exact transition' USING ERRCODE='23514';
    END IF;
    IF now_us >= LEAST(proof.expires_at_us,session.expires_at_us,session.idle_expires_at_us) THEN RETURN NULL; END IF;
    IF NEW.enabled AND NOT control.scope_pin_live(NEW.dataset_id,NEW.catalog_id,NEW.frontier_revision,NEW.pin_lease_id) THEN RETURN NULL; END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER scope_grant_change_guard BEFORE INSERT ON control.scope_grant_changes
    FOR EACH ROW EXECUTE FUNCTION control.guard_scope_grant_change();

CREATE FUNCTION control.guard_scope_grant() RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    IF TG_OP = 'DELETE' OR TG_OP = 'TRUNCATE' THEN
        RAISE EXCEPTION 'scope grant generation tombstones are retained' USING ERRCODE='23514';
    END IF;
    IF (TG_OP='INSERT' AND (NEW.generation<>1 OR NOT NEW.enabled))
        OR (TG_OP='UPDATE' AND ((NEW.dataset_id,NEW.principal_id,NEW.action,NEW.dictionary_kind,NEW.allocation_domain,NEW.ordinal)
            IS DISTINCT FROM (OLD.dataset_id,OLD.principal_id,OLD.action,OLD.dictionary_kind,OLD.allocation_domain,OLD.ordinal)
            OR NEW.generation<>OLD.generation+1 OR NEW.enabled=OLD.enabled))
        OR NOT EXISTS (SELECT 1 FROM control.scope_grant_changes d WHERE d.actor_id=NEW.actor_id AND d.request_key=NEW.request_key
            AND (d.dataset_id,d.principal_id,d.action,d.dictionary_kind,d.allocation_domain,d.ordinal,d.enabled,d.scope_generation)
                IS NOT DISTINCT FROM (NEW.dataset_id,NEW.principal_id,NEW.action,NEW.dictionary_kind,NEW.allocation_domain,NEW.ordinal,NEW.enabled,NEW.generation)) THEN
        RAISE EXCEPTION 'scope grant requires its exact new accepted command' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER scope_grant_guard BEFORE INSERT OR UPDATE OR DELETE ON control.scope_grants
    FOR EACH ROW EXECUTE FUNCTION control.guard_scope_grant();
CREATE TRIGGER scope_grant_truncate_guard BEFORE TRUNCATE ON control.scope_grants
    FOR EACH STATEMENT EXECUTE FUNCTION control.guard_scope_grant();

CREATE FUNCTION control.require_scope_grant_effects() RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE command control.management_commands%ROWTYPE; detail control.scope_grant_changes%ROWTYPE;
    actor_id bytea; request_key bytea; owner control.owner_epochs%ROWTYPE;
BEGIN
    IF TG_TABLE_NAME='management_audit' THEN actor_id:=NEW.actor_id; request_key:=NEW.subject_id;
    ELSE actor_id:=NEW.actor_id; request_key:=NEW.request_key; END IF;
    SELECT * INTO command FROM control.management_commands c WHERE c.actor_id=require_scope_grant_effects.actor_id AND c.request_key=require_scope_grant_effects.request_key;
    SELECT * INTO detail FROM control.scope_grant_changes d WHERE d.actor_id=require_scope_grant_effects.actor_id AND d.request_key=require_scope_grant_effects.request_key;
    IF command.actor_id IS NULL OR detail.actor_id IS NULL OR command.action<>'scope-grant.change'
        OR NOT EXISTS (SELECT 1 FROM control.datasets ds WHERE ds.dataset_id=command.dataset_id
            AND ds.config_revision=command.expected_config_revision AND ds.policy_revision=command.policy_revision)
        OR NOT EXISTS (SELECT 1 FROM control.local_accounts a JOIN control.browser_sessions s
            ON s.dataset_id=a.dataset_id AND s.principal_id=a.principal_id
            WHERE a.dataset_id=detail.dataset_id AND a.principal_id=detail.actor_id AND a.active
                AND a.credential_revision=detail.actor_credential_revision AND a.authentication_revision=detail.actor_authentication_revision
                AND a.grant_revision=detail.actor_grant_revision + CASE WHEN detail.actor_id=detail.principal_id THEN 1 ELSE 0 END
                AND s.session_id=detail.session_id AND s.revoked_at_us IS NULL
                AND s.credential_revision=a.credential_revision AND s.authentication_revision=a.authentication_revision
)
        OR NOT EXISTS (SELECT 1 FROM control.principal_grants WHERE dataset_id=detail.dataset_id AND principal_id=detail.actor_id
            AND action='principals.manage' AND resource_kind='dataset' AND resource_id=detail.dataset_id)
        OR NOT EXISTS (SELECT 1 FROM control.management_audit a WHERE a.audit_id=command.audit_id AND a.dataset_id=command.dataset_id
            AND a.actor_id=command.actor_id AND a.subject_id=command.request_key AND a.action=command.action AND a.outcome='accepted'
            AND a.config_revision=command.expected_config_revision AND a.policy_revision=command.policy_revision AND a.created_at_us=command.accepted_at_us)
        OR EXISTS (SELECT 1 FROM control.reauthentication_proofs p WHERE p.proof_id=detail.proof_id)
        OR NOT EXISTS (SELECT 1 FROM control.local_accounts a WHERE a.dataset_id=detail.dataset_id AND a.principal_id=detail.principal_id
            AND a.active AND a.grant_revision=detail.resulting_grant_revision)
        OR NOT EXISTS (SELECT 1 FROM control.scope_grants g WHERE g.actor_id=detail.actor_id AND g.request_key=detail.request_key
            AND (g.dataset_id,g.principal_id,g.action,g.dictionary_kind,g.allocation_domain,g.ordinal,g.enabled,g.generation)
                IS NOT DISTINCT FROM (detail.dataset_id,detail.principal_id,detail.action,detail.dictionary_kind,detail.allocation_domain,detail.ordinal,detail.enabled,detail.scope_generation)) THEN
        RAISE EXCEPTION 'scope grant requires atomic command, exact audit, revision, grant and proof consumption' USING ERRCODE='23514';
    END IF;
    IF TG_TABLE_NAME='management_audit' THEN
        IF NEW.audit_id IS DISTINCT FROM command.audit_id THEN
            RAISE EXCEPTION 'scope grant audit is not the command receipt' USING ERRCODE='23514';
        END IF;
    END IF;
    SELECT * INTO STRICT owner FROM control.owner_epochs WHERE dataset_id=detail.dataset_id AND role='control' AND state='active';
    PERFORM control.require_control_owner(owner.dataset_id,owner.resource_id,owner.epoch,owner.process_incarnation);
    IF (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint >= detail.proof_expires_at_us
        OR NOT EXISTS (SELECT 1 FROM control.browser_sessions s WHERE s.session_id=detail.session_id
            AND (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint >= s.last_seen_at_us
            AND (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint < LEAST(s.expires_at_us,s.idle_expires_at_us))
        OR (detail.enabled AND NOT control.scope_pin_live(detail.dataset_id,detail.catalog_id,detail.frontier_revision,detail.pin_lease_id)) THEN
        -- An explicit server rejection of COMMIT establishes rollback, unlike
        -- a lost transport response. The adapter distinguishes this expiry code.
        RAISE EXCEPTION 'scope grant authority expired before commit' USING ERRCODE='BB001';
    END IF;
    RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER scope_command_complete AFTER INSERT ON control.management_commands
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.action='scope-grant.change') EXECUTE FUNCTION control.require_scope_grant_effects();
CREATE CONSTRAINT TRIGGER scope_audit_complete AFTER INSERT ON control.management_audit
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.action='scope-grant.change') EXECUTE FUNCTION control.require_scope_grant_effects();
CREATE CONSTRAINT TRIGGER scope_detail_complete AFTER INSERT ON control.scope_grant_changes
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION control.require_scope_grant_effects();
CREATE CONSTRAINT TRIGGER scope_current_complete AFTER INSERT OR UPDATE ON control.scope_grants
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION control.require_scope_grant_effects();

-- A separate DELETE guard permits exact command consumption. Keep809's INSERT
-- expiry suppression and other transitions byte-for-byte in their owning code.
CREATE FUNCTION control.guard_reauthentication_consumption() RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM control.scope_grant_changes d JOIN control.management_commands c USING(actor_id,request_key)
        WHERE d.proof_id=OLD.proof_id AND d.session_id=OLD.session_id AND d.dataset_id=OLD.dataset_id AND d.actor_id=OLD.actor_id
            AND d.request_key=OLD.request_key AND c.request_digest=OLD.request_digest) AND
        (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint < OLD.expires_at_us THEN
        RAISE EXCEPTION 'live reauthentication proof requires exact command consumption' USING ERRCODE='23514';
    END IF;
    RETURN OLD;
END $$;
DROP TRIGGER reauthentication_proof_transition ON control.reauthentication_proofs;
CREATE TRIGGER reauthentication_proof_transition BEFORE INSERT OR UPDATE ON control.reauthentication_proofs
    FOR EACH ROW EXECUTE FUNCTION control.guard_reauthentication_proof();
CREATE TRIGGER reauthentication_proof_consumption BEFORE DELETE ON control.reauthentication_proofs
    FOR EACH ROW EXECUTE FUNCTION control.guard_reauthentication_consumption();
REVOKE ALL ON control.scope_grants,control.scope_grant_changes FROM PUBLIC;
REVOKE ALL ON FUNCTION control.command_u64_bytes(control.u64),control.scope_pin_live(bytea,bytea,numeric,bytea),
    control.guard_scope_grant_change(),control.guard_scope_grant(),control.require_scope_grant_effects(),
    control.guard_reauthentication_consumption() FROM PUBLIC;

CREATE TRIGGER scope_change_truncate_guard BEFORE TRUNCATE ON control.scope_grant_changes
    FOR EACH STATEMENT EXECUTE FUNCTION control.reject_history_mutation();
CREATE TRIGGER management_command_truncate_guard BEFORE TRUNCATE ON control.management_commands
    FOR EACH STATEMENT EXECUTE FUNCTION control.reject_history_mutation();
CREATE TRIGGER reauthentication_truncate_guard BEFORE TRUNCATE ON control.reauthentication_proofs
    FOR EACH STATEMENT EXECUTE FUNCTION control.reject_history_mutation();
