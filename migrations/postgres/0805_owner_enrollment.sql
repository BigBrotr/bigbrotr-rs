-- Extend the existing attributable installation identity; never promote it.
ALTER TABLE control.principals DROP CONSTRAINT principals_kind_check;
ALTER TABLE control.principals ADD CHECK (kind IN ('installation', 'local'));
ALTER TABLE control.principals DROP CONSTRAINT principals_dataset_id_kind_key;
CREATE UNIQUE INDEX one_installation_principal ON control.principals(dataset_id)
    WHERE kind = 'installation';

CREATE TABLE control.local_accounts (
    principal_id control.id16 PRIMARY KEY,
    dataset_id control.id16 NOT NULL,
    username text NOT NULL CHECK (octet_length(username) BETWEEN 1 AND 64 AND username COLLATE "C" ~ '^[a-z0-9._-]+$'),
    initial_owner boolean NOT NULL,
    active boolean NOT NULL,
    credential_revision control.u64 NOT NULL CHECK (credential_revision > 0),
    grant_revision control.u64 NOT NULL CHECK (grant_revision > 0),
    FOREIGN KEY (dataset_id, principal_id) REFERENCES control.principals(dataset_id, principal_id),
    UNIQUE (dataset_id, principal_id),
    UNIQUE (dataset_id, username)
);
CREATE UNIQUE INDEX one_initial_owner ON control.local_accounts(dataset_id) WHERE initial_owner;
CREATE FUNCTION control.require_local_principal() RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM control.principals WHERE dataset_id = NEW.dataset_id AND principal_id = NEW.principal_id AND kind = 'local') THEN
        RAISE EXCEPTION 'local account requires a local principal' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER local_principal BEFORE INSERT OR UPDATE ON control.local_accounts
    FOR EACH ROW EXECUTE FUNCTION control.require_local_principal();

CREATE TABLE control.local_credentials (
    principal_id control.id16 PRIMARY KEY,
    dataset_id control.id16 NOT NULL,
    credential_revision control.u64 NOT NULL CHECK (credential_revision > 0),
    format text NOT NULL CHECK (format = 'argon2id-v1'),
    argon_version integer NOT NULL CHECK (argon_version = 19),
    memory_kib integer NOT NULL CHECK (memory_kib = 65536),
    passes integer NOT NULL CHECK (passes = 3),
    lanes integer NOT NULL CHECK (lanes = 1),
    salt bytea NOT NULL CHECK (octet_length(salt) = 16),
    password_hash control.digest32 NOT NULL,
    FOREIGN KEY (dataset_id, principal_id) REFERENCES control.local_accounts(dataset_id, principal_id)
);
CREATE TABLE control.principal_grants (
    dataset_id control.id16 NOT NULL,
    principal_id control.id16 NOT NULL,
    action text NOT NULL CHECK (action IN ('data.read', 'status.read', 'roles.control', 'budgets.change', 'policy.change', 'principals.manage', 'audit.read')),
    resource_kind text NOT NULL CHECK (resource_kind = 'dataset'),
    resource_id control.id16 NOT NULL CHECK (resource_id = dataset_id),
    PRIMARY KEY (principal_id, action, resource_kind, resource_id),
    FOREIGN KEY (dataset_id, principal_id) REFERENCES control.local_accounts(dataset_id, principal_id)
);

CREATE TABLE control.owner_challenges (
    challenge_id control.id16 PRIMARY KEY,
    dataset_id control.id16 NOT NULL REFERENCES control.datasets(dataset_id),
    username text NOT NULL CHECK (octet_length(username) BETWEEN 1 AND 64 AND username COLLATE "C" ~ '^[a-z0-9._-]+$'),
    intended_principal control.id16 NOT NULL,
    token_digest control.digest32 NOT NULL,
    issued_at_us bigint NOT NULL,
    expires_at_us bigint NOT NULL CHECK (expires_at_us::numeric - issued_at_us = 600000000),
    state text NOT NULL CHECK (state IN ('pending', 'revoked', 'consumed')),
    failed_attempts integer NOT NULL CHECK (failed_attempts BETWEEN 0 AND 32),
    consumed_principal control.id16,
    consumed_at_us bigint,
    FOREIGN KEY (dataset_id, consumed_principal) REFERENCES control.local_accounts(dataset_id, principal_id),
    CHECK ((state = 'consumed' AND consumed_principal IS NOT NULL AND consumed_principal = intended_principal AND consumed_at_us IS NOT NULL AND consumed_at_us >= issued_at_us AND consumed_at_us < expires_at_us)
        OR (state <> 'consumed' AND consumed_principal IS NULL AND consumed_at_us IS NULL))
);
CREATE UNIQUE INDEX one_pending_owner_challenge ON control.owner_challenges(dataset_id) WHERE state = 'pending';
CREATE FUNCTION control.guard_owner_challenge() RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'owner challenge history is retained' USING ERRCODE = '23514';
    ELSIF TG_OP = 'INSERT' THEN
        IF NEW.state <> 'pending' OR NEW.failed_attempts <> 0 THEN
            RAISE EXCEPTION 'owner challenge must begin pending' USING ERRCODE = '23514';
        END IF;
    ELSE
        IF OLD.state <> 'pending' OR
            (NEW.challenge_id, NEW.dataset_id, NEW.username, NEW.intended_principal, NEW.token_digest, NEW.issued_at_us, NEW.expires_at_us)
            IS DISTINCT FROM
            (OLD.challenge_id, OLD.dataset_id, OLD.username, OLD.intended_principal, OLD.token_digest, OLD.issued_at_us, OLD.expires_at_us)
            OR NOT ((NEW.state = 'pending' AND NEW.failed_attempts = OLD.failed_attempts + 1)
                OR (NEW.state IN ('revoked', 'consumed') AND NEW.failed_attempts = OLD.failed_attempts))
            OR (NEW.state = 'consumed' AND (OLD.failed_attempts >= 32 OR NEW.consumed_at_us >= NEW.expires_at_us)) THEN
            RAISE EXCEPTION 'invalid owner challenge transition' USING ERRCODE = '23514';
        END IF;
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER owner_challenge_transition BEFORE INSERT OR UPDATE OR DELETE ON control.owner_challenges
    FOR EACH ROW EXECUTE FUNCTION control.guard_owner_challenge();

ALTER TABLE control.management_audit DROP CONSTRAINT management_audit_action_check;
ALTER TABLE control.management_audit DROP CONSTRAINT management_audit_outcome_check;
ALTER TABLE control.management_audit DROP CONSTRAINT management_audit_config_revision_check;
ALTER TABLE control.management_audit DROP CONSTRAINT management_audit_dataset_id_action_key;
ALTER TABLE control.management_audit ALTER COLUMN config_revision DROP NOT NULL;
ALTER TABLE control.management_audit ADD COLUMN subject_id control.id16;
ALTER TABLE control.management_audit ADD COLUMN reason text;
ALTER TABLE control.management_audit ADD COLUMN requester_database_role text;
ALTER TABLE control.management_audit ADD COLUMN requester_os_uid control.u32;
ALTER TABLE control.management_audit ADD CHECK (
    (action = 'dataset.initialize' AND outcome = 'accepted' AND config_revision IS NOT NULL AND config_revision = 1 AND subject_id IS NULL AND reason IS NULL)
    OR (action IN ('owner-token.issue', 'owner-token.revoke', 'owner.enroll') AND outcome = 'accepted' AND config_revision IS NULL AND subject_id IS NOT NULL AND reason IS NULL AND requester_database_role IS NOT NULL AND requester_os_uid IS NOT NULL)
    OR (action = 'owner.enroll' AND outcome = 'rejected' AND config_revision IS NULL AND subject_id IS NOT NULL AND reason IS NOT NULL AND reason IN ('invalid-token', 'expired', 'exhausted', 'terminal') AND requester_database_role IS NOT NULL AND requester_os_uid IS NOT NULL)
);
CREATE UNIQUE INDEX one_initialization_audit ON control.management_audit(dataset_id) WHERE action = 'dataset.initialize';
CREATE UNIQUE INDEX one_owner_transition_audit ON control.management_audit(dataset_id, subject_id, action)
    WHERE action IN ('owner-token.issue', 'owner-token.revoke', 'owner.enroll') AND outcome = 'accepted';
CREATE INDEX owner_audit_subject ON control.management_audit(dataset_id, subject_id, created_at_us);
REVOKE ALL ON control.local_accounts, control.local_credentials, control.principal_grants, control.owner_challenges FROM PUBLIC;
REVOKE ALL ON FUNCTION control.require_local_principal(), control.guard_owner_challenge() FROM PUBLIC;
