-- Additive browser identity. Existing policy documents and initial grants keep
-- their exact meaning. No account is enrolled and no data action is granted.
ALTER TABLE control.local_accounts ADD COLUMN authentication_revision control.u64 NOT NULL DEFAULT 1
    CHECK (authentication_revision > 0);

CREATE TABLE control.browser_sessions (
    session_id control.id16 PRIMARY KEY,
    dataset_id control.id16 NOT NULL,
    principal_id control.id16 NOT NULL,
    credential_revision control.u64 NOT NULL CHECK (credential_revision > 0),
    authentication_revision control.u64 NOT NULL CHECK (authentication_revision > 0),
    token_digest control.digest32 NOT NULL UNIQUE,
    csrf_digest control.digest32 NOT NULL,
    issued_at_us bigint NOT NULL,
    expires_at_us bigint NOT NULL,
    idle_window_us bigint NOT NULL CHECK (idle_window_us > 0),
    last_seen_at_us bigint NOT NULL,
    idle_expires_at_us bigint NOT NULL,
    revoked_at_us bigint,
    FOREIGN KEY (dataset_id, principal_id) REFERENCES control.local_accounts(dataset_id, principal_id),
    CHECK (expires_at_us::numeric - issued_at_us >= idle_window_us),
    CHECK (last_seen_at_us >= issued_at_us AND last_seen_at_us < expires_at_us),
    CHECK (idle_expires_at_us = LEAST(expires_at_us::numeric, last_seen_at_us::numeric + idle_window_us)),
    CHECK (revoked_at_us IS NULL OR revoked_at_us >= issued_at_us)
);
CREATE INDEX browser_sessions_account ON control.browser_sessions(dataset_id, principal_id);

-- A fixed number of hashed buckets bounds attacker-controlled persistent keys.
-- Collisions can deny attempts, never grant additional password guesses. No raw
-- username, peer address, password or token is retained in this counter table.
CREATE TABLE control.login_buckets (
    dataset_id control.id16 NOT NULL REFERENCES control.datasets(dataset_id),
    kind text NOT NULL CHECK (kind IN ('account', 'peer')),
    bucket smallint NOT NULL CHECK (bucket BETWEEN 0 AND 255),
    window_started_at_us bigint NOT NULL,
    attempts integer NOT NULL CHECK (attempts BETWEEN 1 AND 20),
    PRIMARY KEY (dataset_id, kind, bucket),
    CHECK (kind <> 'account' OR attempts <= 5)
);

CREATE FUNCTION control.guard_browser_session() RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE account control.local_accounts%ROWTYPE; now_us bigint;
BEGIN
    now_us := (EXTRACT(EPOCH FROM clock_timestamp()) * 1000000)::bigint;
    IF TG_OP = 'INSERT' THEN
        IF current_setting('transaction_isolation') <> 'read committed' THEN
            RAISE EXCEPTION 'browser session issuance requires read committed' USING ERRCODE = '23514';
        END IF;
        SELECT * INTO STRICT account FROM control.local_accounts
            WHERE dataset_id = NEW.dataset_id AND principal_id = NEW.principal_id FOR UPDATE;
        now_us := (EXTRACT(EPOCH FROM clock_timestamp()) * 1000000)::bigint;
        IF NOT account.active OR account.credential_revision <> NEW.credential_revision
            OR account.authentication_revision <> NEW.authentication_revision
            OR NEW.revoked_at_us IS NOT NULL OR NEW.last_seen_at_us <> NEW.issued_at_us
            OR NEW.issued_at_us > now_us OR NEW.idle_expires_at_us <= now_us
            OR (SELECT count(*) FROM control.browser_sessions WHERE dataset_id = NEW.dataset_id
                AND principal_id = NEW.principal_id) >= 8 THEN
            RAISE EXCEPTION 'invalid or exhausted browser session issuance' USING ERRCODE = '23514';
        END IF;
    ELSIF TG_OP = 'DELETE' THEN
        IF OLD.revoked_at_us IS NULL AND now_us < OLD.idle_expires_at_us AND now_us < OLD.expires_at_us THEN
            RAISE EXCEPTION 'active browser session cannot be forgotten' USING ERRCODE = '23514';
        END IF;
        RETURN OLD;
    ELSE
        IF (NEW.session_id, NEW.dataset_id, NEW.principal_id, NEW.credential_revision,
            NEW.authentication_revision, NEW.token_digest, NEW.csrf_digest,
            NEW.issued_at_us, NEW.expires_at_us, NEW.idle_window_us)
            IS DISTINCT FROM
            (OLD.session_id, OLD.dataset_id, OLD.principal_id, OLD.credential_revision,
            OLD.authentication_revision, OLD.token_digest, OLD.csrf_digest,
            OLD.issued_at_us, OLD.expires_at_us, OLD.idle_window_us)
            OR OLD.revoked_at_us IS NOT NULL
            OR (NEW.revoked_at_us IS NOT NULL AND
                (NEW.last_seen_at_us, NEW.idle_expires_at_us) IS DISTINCT FROM
                (OLD.last_seen_at_us, OLD.idle_expires_at_us))
            OR (NEW.revoked_at_us IS NULL AND
                (NEW.last_seen_at_us < OLD.last_seen_at_us OR NEW.last_seen_at_us > now_us)) THEN
            RAISE EXCEPTION 'invalid browser session transition' USING ERRCODE = '23514';
        END IF;
        IF NEW.revoked_at_us IS NULL AND (now_us >= OLD.idle_expires_at_us OR now_us >= OLD.expires_at_us) THEN
            -- Expiry can cross after the caller's last clock read. Suppress this
            -- touch (UPDATE affects zero rows), without aborting the transaction
            -- and mistaking an ordinary denial for a broken Control backend.
            RETURN NULL;
        END IF;
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER browser_session_transition BEFORE INSERT OR UPDATE OR DELETE ON control.browser_sessions
    FOR EACH ROW EXECUTE FUNCTION control.guard_browser_session();

-- Grant edits do not log out unrelated actions. Credential/active transitions
-- advance a separate generation, including deactivate -> reactivate during hash.
CREATE FUNCTION control.guard_account_authentication() RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    IF NEW.authentication_revision <> OLD.authentication_revision
        OR NEW.credential_revision < OLD.credential_revision OR NEW.grant_revision < OLD.grant_revision
        OR (NEW.dataset_id, NEW.principal_id, NEW.username, NEW.initial_owner)
            IS DISTINCT FROM (OLD.dataset_id, OLD.principal_id, OLD.username, OLD.initial_owner) THEN
        RAISE EXCEPTION 'invalid account identity transition' USING ERRCODE = '23514';
    END IF;
    IF (NEW.active, NEW.credential_revision) IS DISTINCT FROM (OLD.active, OLD.credential_revision) THEN
        NEW.authentication_revision := OLD.authentication_revision + 1;
        UPDATE control.browser_sessions SET revoked_at_us = GREATEST(issued_at_us,
            (EXTRACT(EPOCH FROM clock_timestamp()) * 1000000)::bigint)
            WHERE dataset_id = OLD.dataset_id AND principal_id = OLD.principal_id AND revoked_at_us IS NULL;
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER account_authentication_transition BEFORE UPDATE ON control.local_accounts
    FOR EACH ROW EXECUTE FUNCTION control.guard_account_authentication();

CREATE FUNCTION control.guard_credential_revision() RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    IF TG_OP = 'UPDATE' AND (NEW.dataset_id, NEW.principal_id) IS DISTINCT FROM (OLD.dataset_id, OLD.principal_id) THEN
        RAISE EXCEPTION 'credential identity is immutable' USING ERRCODE = '23514';
    END IF;
    IF TG_OP = 'UPDATE' AND NEW IS DISTINCT FROM OLD AND NEW.credential_revision <= OLD.credential_revision THEN
        RAISE EXCEPTION 'credential replacement requires a new revision' USING ERRCODE = '23514';
    END IF;
    PERFORM 1 FROM control.local_accounts WHERE dataset_id = NEW.dataset_id AND principal_id = NEW.principal_id
        AND credential_revision = NEW.credential_revision FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'credential revision must match account' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER credential_revision_transition BEFORE INSERT OR UPDATE ON control.local_credentials
    FOR EACH ROW EXECUTE FUNCTION control.guard_credential_revision();
REVOKE ALL ON control.browser_sessions, control.login_buckets FROM PUBLIC;
REVOKE ALL ON FUNCTION control.guard_browser_session(), control.guard_account_authentication(), control.guard_credential_revision() FROM PUBLIC;
