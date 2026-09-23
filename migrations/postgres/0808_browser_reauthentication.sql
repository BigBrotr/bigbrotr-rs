-- Short-lived, same-session command proofs. These identifiers are not bearer
-- credentials. Password verification belongs to the bounded Rust hash worker.
-- No existing session, command, policy document or grant is reinterpreted.
CREATE TABLE control.reauthentication_proofs (
    proof_id control.id16 PRIMARY KEY,
    dataset_id control.id16 NOT NULL,
    actor_id control.id16 NOT NULL,
    session_id control.id16 NOT NULL,
    request_key control.id16 NOT NULL,
    request_digest control.digest32 NOT NULL,
    config_revision control.u64 NOT NULL CHECK (config_revision > 0),
    policy_revision control.u64 NOT NULL CHECK (policy_revision > 0),
    credential_revision control.u64 NOT NULL CHECK (credential_revision > 0),
    authentication_revision control.u64 NOT NULL CHECK (authentication_revision > 0),
    grant_revision control.u64 NOT NULL CHECK (grant_revision > 0),
    issued_at_us bigint NOT NULL,
    expires_at_us bigint NOT NULL,
    UNIQUE (session_id, request_key),
    FOREIGN KEY (dataset_id, actor_id) REFERENCES control.local_accounts(dataset_id, principal_id),
    FOREIGN KEY (dataset_id, policy_revision, config_revision)
        REFERENCES control.policy_revisions(dataset_id, revision, config_revision),
    CHECK (expires_at_us::numeric - issued_at_us BETWEEN 1 AND 300000000)
);
CREATE INDEX reauthentication_proofs_account ON control.reauthentication_proofs(dataset_id, actor_id);

ALTER TABLE control.management_audit DROP CONSTRAINT management_audit_shape;
ALTER TABLE control.management_audit ADD CONSTRAINT management_audit_shape CHECK (
    (policy_revision IS NULL AND (
        (action = 'dataset.initialize' AND outcome = 'accepted' AND config_revision IS NOT NULL AND config_revision = 1 AND subject_id IS NULL AND reason IS NULL)
        OR (action IN ('owner-token.issue', 'owner-token.revoke', 'owner.enroll') AND outcome = 'accepted' AND config_revision IS NULL AND subject_id IS NOT NULL AND reason IS NULL AND requester_database_role IS NOT NULL AND requester_os_uid IS NOT NULL)
        OR (action = 'owner.enroll' AND outcome = 'rejected' AND config_revision IS NULL AND subject_id IS NOT NULL AND reason IS NOT NULL AND reason IN ('invalid-token', 'expired', 'exhausted', 'terminal') AND requester_database_role IS NOT NULL AND requester_os_uid IS NOT NULL)
        OR (action = 'policy.activate-initial' AND outcome = 'accepted' AND config_revision IS NOT NULL AND config_revision > 0 AND subject_id IS NOT NULL AND reason IS NULL AND requester_database_role IS NOT NULL AND requester_os_uid IS NOT NULL)
    ))
    OR (action IN ('browser.session.issue', 'browser.session.revoke', 'browser.session.reauthenticate') AND outcome = 'accepted'
        AND config_revision IS NOT NULL AND config_revision > 0
        AND policy_revision IS NOT NULL AND policy_revision > 0
        AND subject_id IS NOT NULL AND reason IS NULL
        AND requester_database_role IS NULL AND requester_os_uid IS NULL)
);
CREATE UNIQUE INDEX one_reauthentication_audit ON control.management_audit(dataset_id, subject_id)
    WHERE action = 'browser.session.reauthenticate';

CREATE FUNCTION control.guard_reauthentication_proof() RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE account control.local_accounts%ROWTYPE; session control.browser_sessions%ROWTYPE;
    dataset control.datasets%ROWTYPE; now_us bigint;
BEGIN
    IF TG_OP = 'UPDATE' THEN
        RAISE EXCEPTION 'reauthentication proof is immutable' USING ERRCODE = '23514';
    ELSIF TG_OP = 'DELETE' THEN
        -- Callers lock the account before cleanup, matching issuance. A later
        -- grant migration adds explicit atomic consumption, never a bearer DELETE.
        now_us := (EXTRACT(EPOCH FROM clock_timestamp()) * 1000000)::bigint;
        IF now_us < OLD.expires_at_us THEN
            RAISE EXCEPTION 'live reauthentication proof requires command consumption' USING ERRCODE = '23514';
        END IF;
        RETURN OLD;
    END IF;
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'reauthentication requires read committed' USING ERRCODE = '23514';
    END IF;
    SELECT * INTO STRICT dataset FROM control.datasets WHERE dataset_id = NEW.dataset_id FOR UPDATE;
    SELECT * INTO STRICT account FROM control.local_accounts
        WHERE dataset_id = NEW.dataset_id AND principal_id = NEW.actor_id FOR UPDATE;
    SELECT * INTO session FROM control.browser_sessions
        WHERE dataset_id = NEW.dataset_id AND principal_id = NEW.actor_id AND session_id = NEW.session_id FOR UPDATE;
    now_us := (EXTRACT(EPOCH FROM clock_timestamp()) * 1000000)::bigint;
    IF session.session_id IS NULL OR NOT account.active OR session.revoked_at_us IS NOT NULL
        OR (NEW.config_revision, NEW.policy_revision) IS DISTINCT FROM (dataset.config_revision, dataset.policy_revision)
        OR (NEW.credential_revision, NEW.authentication_revision, NEW.grant_revision)
            IS DISTINCT FROM (account.credential_revision, account.authentication_revision, account.grant_revision)
        OR (NEW.credential_revision, NEW.authentication_revision)
            IS DISTINCT FROM (session.credential_revision, session.authentication_revision)
        OR NEW.issued_at_us > now_us OR NEW.issued_at_us < session.last_seen_at_us
        OR NEW.expires_at_us <= now_us OR NEW.expires_at_us > LEAST(session.expires_at_us, session.idle_expires_at_us)
        OR NOT EXISTS (SELECT 1 FROM control.principal_grants WHERE dataset_id = NEW.dataset_id
            AND principal_id = NEW.actor_id AND action = 'principals.manage'
            AND resource_kind = 'dataset' AND resource_id = NEW.dataset_id)
        OR (SELECT count(*) FROM control.reauthentication_proofs
            WHERE dataset_id = NEW.dataset_id AND actor_id = NEW.actor_id) >= 32 THEN
        RAISE EXCEPTION 'invalid or exhausted reauthentication proof' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER reauthentication_proof_transition BEFORE INSERT OR UPDATE OR DELETE ON control.reauthentication_proofs
    FOR EACH ROW EXECUTE FUNCTION control.guard_reauthentication_proof();

-- Compare policy explicitly; the older management-command audit FK does not
-- include this column. A valid audit for another policy cannot cover issuance.
CREATE FUNCTION control.require_reauthentication_audit() RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE proof control.reauthentication_proofs%ROWTYPE;
BEGIN
    IF TG_TABLE_NAME = 'reauthentication_proofs' THEN
        proof := NEW;
    ELSE
        SELECT * INTO proof FROM control.reauthentication_proofs
            WHERE proof_id = NEW.subject_id AND dataset_id = NEW.dataset_id;
        IF proof.proof_id IS NULL THEN
            RAISE EXCEPTION 'reauthentication audit lacks issuance' USING ERRCODE = '23514';
        END IF;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM control.management_audit a WHERE a.dataset_id = proof.dataset_id
        AND a.actor_id = proof.actor_id AND a.subject_id = proof.proof_id
        AND a.action = 'browser.session.reauthenticate' AND a.outcome = 'accepted'
        AND a.config_revision = proof.config_revision AND a.policy_revision = proof.policy_revision
        AND a.created_at_us = proof.issued_at_us) THEN
        RAISE EXCEPTION 'reauthentication issuance lacks exact audit' USING ERRCODE = '23514';
    END IF;
    RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER reauthentication_proof_audit AFTER INSERT ON control.reauthentication_proofs
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION control.require_reauthentication_audit();
CREATE CONSTRAINT TRIGGER reauthentication_audit_proof AFTER INSERT ON control.management_audit
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.action = 'browser.session.reauthenticate')
    EXECUTE FUNCTION control.require_reauthentication_audit();
-- No FK to removable browser credentials. Proof expiry never deletes its audit.
REVOKE ALL ON control.reauthentication_proofs FROM PUBLIC;
REVOKE ALL ON FUNCTION control.guard_reauthentication_proof(), control.require_reauthentication_audit() FROM PUBLIC;
