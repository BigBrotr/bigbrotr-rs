-- Additive authenticated acceptance over existing immutable collection intent.
-- Existing selectors/scopes keep original attribution and require no backfill.
ALTER TABLE control.management_commands DROP CONSTRAINT management_command_shape;
ALTER TABLE control.management_commands ADD CONSTRAINT management_command_shape CHECK (
    (action='policy.activate-initial' AND authority_mode IS NOT NULL
        AND authority_mode IN ('managed','declarative') AND document_digest IS NOT NULL AND policy_revision=1)
    OR (action IN ('scope-grant.change','job.export.accept','job.export.resume','relay.enrollment.change','job.acquisition.accept','relay.endpoint.register','acquisition.scope.register')
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
                  'job.export.accept','job.export.resume','relay.enrollment.change','job.acquisition.accept','relay.endpoint.register','acquisition.scope.register')
        AND outcome='accepted' AND config_revision IS NOT NULL AND config_revision>0
        AND policy_revision IS NOT NULL AND policy_revision>0
        AND subject_id IS NOT NULL AND reason IS NULL
        AND requester_database_role IS NULL AND requester_os_uid IS NULL)
);

CREATE TABLE control.acquisition_scope_commands (
    actor_id control.id16 NOT NULL,
    request_key control.id16 NOT NULL,
    dataset_id control.id16 NOT NULL,
    scope_id control.id16 NOT NULL,
    session_id control.id16 NOT NULL,
    actor_credential_revision control.u64 NOT NULL CHECK (actor_credential_revision > 0),
    actor_authentication_revision control.u64 NOT NULL CHECK (actor_authentication_revision > 0),
    actor_grant_revision control.u64 NOT NULL CHECK (actor_grant_revision > 0),
    owner_resource_id control.id16 NOT NULL,
    owner_epoch control.u64 NOT NULL,
    owner_incarnation control.id16 NOT NULL,
    owner_role text NOT NULL DEFAULT 'control' CHECK (owner_role = 'control'),
    PRIMARY KEY (actor_id, request_key),
    FOREIGN KEY (actor_id, request_key)
        REFERENCES control.management_commands(actor_id, request_key),
    FOREIGN KEY (dataset_id, actor_id)
        REFERENCES control.local_accounts(dataset_id, principal_id),
    FOREIGN KEY (dataset_id, scope_id)
        REFERENCES control.acquisition_scopes(dataset_id, scope_id),
    FOREIGN KEY (dataset_id, owner_resource_id, owner_epoch, owner_role, owner_incarnation)
        REFERENCES control.owner_epochs(dataset_id, resource_id, epoch, role, process_incarnation)
);
CREATE TRIGGER acquisition_scope_commands_immutable
    BEFORE UPDATE OR DELETE ON control.acquisition_scope_commands
    FOR EACH ROW EXECUTE FUNCTION control.reject_history_mutation();
CREATE TRIGGER acquisition_scope_commands_no_truncate
    BEFORE TRUNCATE ON control.acquisition_scope_commands
    FOR EACH STATEMENT EXECUTE FUNCTION control.reject_history_mutation();

-- Complete original selector bytes define intent; generated IDs and candidate
-- hashes do not. The referenced immutable relations make this function STABLE.
CREATE FUNCTION control.acquisition_scope_digest(
    detail control.acquisition_scope_commands, command control.management_commands
) RETURNS bytea
LANGUAGE sql STABLE STRICT SET search_path = pg_catalog AS $$
    SELECT sha256(convert_to('bigbrotr/acquisition-scope-registration/1', 'UTF8')
        || decode('00', 'hex') || detail.dataset_id || detail.actor_id || detail.request_key
        || scope.endpoint_id || scope.access_scope_id || scope.dataset_id
        || int2send(scope.visibility_kind) || scope.visibility_domain
        || control.command_u64_bytes(scope.visibility_ordinal)
        || control.command_u64_bytes(command.expected_config_revision)
        || control.command_u64_bytes(command.policy_revision)
        || int4send(octet_length(selector.document)) || selector.document)
    FROM control.acquisition_scopes scope JOIN control.acquisition_selectors selector
        ON selector.dataset_id = scope.dataset_id AND selector.selector_id = scope.selector_id
    WHERE scope.dataset_id = detail.dataset_id AND scope.scope_id = detail.scope_id
$$;

CREATE FUNCTION control.guard_acquisition_scope_command() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'scope command requires READ COMMITTED' USING ERRCODE = '25001';
    END IF;
    -- Same lock order as the adapter. Session attribution has no durable FK:
    -- expired session cleanup must not delete or invalidate historical receipts.
    PERFORM control.require_control_owner(
        NEW.dataset_id, NEW.owner_resource_id, NEW.owner_epoch, NEW.owner_incarnation);
    PERFORM 1 FROM control.datasets WHERE dataset_id = NEW.dataset_id FOR UPDATE;
    PERFORM 1 FROM control.local_accounts
        WHERE dataset_id = NEW.dataset_id AND principal_id = NEW.actor_id FOR SHARE;
    PERFORM 1 FROM control.browser_sessions
        WHERE dataset_id = NEW.dataset_id AND principal_id = NEW.actor_id
            AND session_id = NEW.session_id FOR SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'scope command requires its authenticated session' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER acquisition_scope_command_guard BEFORE INSERT ON control.acquisition_scope_commands
    FOR EACH ROW EXECUTE FUNCTION control.guard_acquisition_scope_command();

CREATE FUNCTION control.require_acquisition_scope_command() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    detail control.acquisition_scope_commands%ROWTYPE;
    command control.management_commands%ROWTYPE;
    time_us bigint;
BEGIN
    IF TG_TABLE_NAME = 'management_audit' THEN
        SELECT * INTO detail FROM control.acquisition_scope_commands
            WHERE actor_id = NEW.actor_id AND request_key = NEW.subject_id;
    ELSE
        SELECT * INTO detail FROM control.acquisition_scope_commands
            WHERE actor_id = NEW.actor_id AND request_key = NEW.request_key;
    END IF;
    IF detail.scope_id IS NULL THEN
        RAISE EXCEPTION 'scope command requires atomic registration detail' USING ERRCODE = '23514';
    END IF;
    SELECT * INTO STRICT command FROM control.management_commands
        WHERE actor_id = detail.actor_id AND request_key = detail.request_key;
    IF command.action <> 'acquisition.scope.register' OR command.dataset_id <> detail.dataset_id
        OR command.request_digest IS DISTINCT FROM control.acquisition_scope_digest(detail, command)
        OR NOT EXISTS (
            SELECT 1 FROM control.acquisition_scopes WHERE dataset_id = detail.dataset_id
                AND scope_id = detail.scope_id AND policy_revision = command.policy_revision
        )
        OR NOT EXISTS (
            SELECT 1 FROM control.datasets WHERE dataset_id = detail.dataset_id
                AND config_revision = command.expected_config_revision
                AND policy_revision = command.policy_revision
        ) OR NOT EXISTS (
            SELECT 1 FROM control.management_audit a WHERE a.audit_id = command.audit_id
                AND (a.dataset_id, a.actor_id, a.action, a.outcome, a.subject_id,
                     a.config_revision, a.policy_revision, a.created_at_us)
                    IS NOT DISTINCT FROM
                    (detail.dataset_id, detail.actor_id, command.action, 'accepted'::text,
                     detail.request_key, command.expected_config_revision,
                     command.policy_revision, command.accepted_at_us)
        ) THEN
        RAISE EXCEPTION 'scope registration requires exact command, audit and policy heads'
            USING ERRCODE = '23514';
    END IF;
    IF TG_TABLE_NAME = 'management_audit' THEN
        IF NEW.audit_id IS DISTINCT FROM command.audit_id THEN
            RAISE EXCEPTION 'scope registration audit is not its command receipt' USING ERRCODE = '23514';
        END IF;
    END IF;
    PERFORM control.require_control_owner(
        detail.dataset_id, detail.owner_resource_id, detail.owner_epoch, detail.owner_incarnation);
    time_us := (EXTRACT(EPOCH FROM clock_timestamp()) * 1000000)::bigint;
    -- This also authenticates a new command that references an existing scope;
    -- the historical scope insertion fence cannot cover this later acceptance.
    IF command.accepted_at_us > time_us OR NOT EXISTS (
        SELECT 1 FROM control.local_accounts a JOIN control.browser_sessions s
            ON s.dataset_id = a.dataset_id AND s.principal_id = a.principal_id
        WHERE a.dataset_id = detail.dataset_id AND a.principal_id = detail.actor_id AND a.active
            AND a.credential_revision = detail.actor_credential_revision
            AND a.authentication_revision = detail.actor_authentication_revision
            AND a.grant_revision = detail.actor_grant_revision
            AND s.session_id = detail.session_id AND s.revoked_at_us IS NULL
            AND s.credential_revision = a.credential_revision
            AND s.authentication_revision = a.authentication_revision
            AND time_us >= s.last_seen_at_us AND time_us < LEAST(s.expires_at_us, s.idle_expires_at_us)
    ) OR NOT EXISTS (
        SELECT 1 FROM control.principal_grants WHERE dataset_id = detail.dataset_id
            AND principal_id = detail.actor_id AND action = 'roles.control'
            AND resource_kind = 'dataset' AND resource_id = detail.dataset_id
    ) OR NOT EXISTS (
        SELECT 1 FROM control.owner_epochs WHERE resource_id = detail.owner_resource_id
            AND epoch = detail.owner_epoch AND lease_expires_at_us > time_us
    ) THEN
        RAISE EXCEPTION 'scope registration authentication or owner expired before commit'
            USING ERRCODE = 'BB001';
    END IF;
    RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER acquisition_scope_receipt_complete AFTER INSERT ON control.management_commands
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.action = 'acquisition.scope.register')
    EXECUTE FUNCTION control.require_acquisition_scope_command();
CREATE CONSTRAINT TRIGGER acquisition_scope_audit_complete AFTER INSERT ON control.management_audit
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.action = 'acquisition.scope.register')
    EXECUTE FUNCTION control.require_acquisition_scope_command();
CREATE CONSTRAINT TRIGGER acquisition_scope_detail_complete AFTER INSERT ON control.acquisition_scope_commands
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
    EXECUTE FUNCTION control.require_acquisition_scope_command();

REVOKE ALL ON control.acquisition_scope_commands FROM PUBLIC;
REVOKE ALL ON FUNCTION
    control.acquisition_scope_digest(control.acquisition_scope_commands, control.management_commands),
    control.guard_acquisition_scope_command(), control.require_acquisition_scope_command() FROM PUBLIC;
