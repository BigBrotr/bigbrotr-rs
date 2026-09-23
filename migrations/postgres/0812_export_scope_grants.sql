-- Extend explicit data actions without changing any accepted command bytes,
-- current grants, generations or initial administrator permissions. The811
-- deferred six-effect/expiry check remains installed without replacement.
ALTER TABLE control.scope_grant_changes DROP CONSTRAINT scope_grant_changes_action_check;
ALTER TABLE control.scope_grant_changes ADD CONSTRAINT scope_grant_changes_action_check
    CHECK (action IN ('history.read','export','private-export'));
ALTER TABLE control.scope_grants DROP CONSTRAINT scope_grants_action_check;
ALTER TABLE control.scope_grants ADD CONSTRAINT scope_grants_action_check
    CHECK (action IN ('history.read','export','private-export'));

-- The action byte belongs to the exact reauthenticated command. An export
-- permission cannot reuse the fingerprint of a history or private-export grant.
CREATE OR REPLACE FUNCTION control.guard_scope_grant_change() RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
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
        || NEW.allocation_domain || control.command_u64_bytes(NEW.ordinal)
        || CASE NEW.action WHEN 'history.read' THEN decode('01','hex')
            WHEN 'export' THEN decode('02','hex') WHEN 'private-export' THEN decode('03','hex') END
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
