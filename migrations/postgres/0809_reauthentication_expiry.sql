-- Additive correction after an executed PG16.15 expiry-at-INSERT counterexample.
-- Preserve applied0808 bytes and existing proof/audit rows.
CREATE OR REPLACE FUNCTION control.guard_reauthentication_proof() RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
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
        OR NEW.expires_at_us > LEAST(session.expires_at_us, session.idle_expires_at_us)
        OR NOT EXISTS (SELECT 1 FROM control.principal_grants WHERE dataset_id = NEW.dataset_id
            AND principal_id = NEW.actor_id AND action = 'principals.manage'
            AND resource_kind = 'dataset' AND resource_id = NEW.dataset_id)
        OR (SELECT count(*) FROM control.reauthentication_proofs
            WHERE dataset_id = NEW.dataset_id AND actor_id = NEW.actor_id) >= 32 THEN
        RAISE EXCEPTION 'invalid or exhausted reauthentication proof' USING ERRCODE = '23514';
    END IF;
    IF NEW.expires_at_us <= now_us THEN
        -- Expiry can cross during the INSERT, after the adapter's clock read.
        -- Suppress only this row; the adapter observes zero affected rows and
        -- returns ordinary denial without audit or poisoning its owner.
        RETURN NULL;
    END IF;
    RETURN NEW;
END $$;
