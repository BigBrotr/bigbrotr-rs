-- Actor visibility and time/key bounds precede the bounded operational scan.
CREATE INDEX jobs_actor_listing
    ON control.jobs(dataset_id,principal_id,created_at_us DESC,job_id DESC)
    WHERE kind='export' AND export_job_protocol=1;

-- Preserve0604's exact transition/projection semantics. Only a changed retention
-- deadline takes the new path; INSERT and every other UPDATE keep the old guard.
DROP TRIGGER cursor_lifecycle_projection_guard ON control.cursor_key_lifecycle;
CREATE TRIGGER cursor_lifecycle_projection_guard BEFORE INSERT ON control.cursor_key_lifecycle
    FOR EACH ROW EXECUTE FUNCTION control.guard_cursor_lifecycle_projection();
CREATE TRIGGER cursor_lifecycle_transition_guard BEFORE UPDATE ON control.cursor_key_lifecycle
    FOR EACH ROW WHEN (NEW.decrypt_until_us IS NOT DISTINCT FROM OLD.decrypt_until_us)
    EXECUTE FUNCTION control.guard_cursor_lifecycle_projection();

CREATE FUNCTION control.require_cursor_retention_owner(wanted_dataset bytea) RETURNS void
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE owner control.owner_epochs%ROWTYPE;
BEGIN
    IF current_setting('transaction_isolation')<>'read committed' THEN
        RAISE EXCEPTION 'cursor retention requires READ COMMITTED' USING ERRCODE='25001';
    END IF;
    SELECT * INTO STRICT owner FROM control.owner_epochs
        WHERE dataset_id=wanted_dataset AND role='control' AND state='active';
    PERFORM control.require_control_owner(owner.dataset_id,owner.resource_id,owner.epoch,owner.process_incarnation);
    IF owner.lease_expires_at_us<=(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint THEN
        RAISE EXCEPTION 'cursor retention requires live Control' USING ERRCODE='BB001';
    END IF;
END $$;

CREATE FUNCTION control.guard_cursor_retention() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE active_key bytea;
BEGIN
    PERFORM control.require_cursor_retention_owner(OLD.dataset_id);
    -- The page adapter already holds Control then dataset/account/session.
    -- Retain dataset exclusion for direct SQL as well, before reading the head.
    PERFORM 1 FROM control.datasets WHERE dataset_id=OLD.dataset_id FOR SHARE;
    SELECT resulting_active_key_id INTO active_key FROM control.cursor_key_transitions
        WHERE dataset_id=OLD.dataset_id ORDER BY revision DESC LIMIT 1;
    IF (NEW.dataset_id,NEW.key_id,NEW.first_activation_revision,NEW.revoked_revision)
            IS DISTINCT FROM (OLD.dataset_id,OLD.key_id,OLD.first_activation_revision,OLD.revoked_revision)
        OR NEW.first_activation_revision IS NULL OR NEW.revoked_revision IS NOT NULL
        OR NEW.key_id IS DISTINCT FROM active_key
        OR NEW.decrypt_until_us IS NULL
        OR (OLD.decrypt_until_us IS NOT NULL AND NEW.decrypt_until_us<=OLD.decrypt_until_us) THEN
        RAISE EXCEPTION 'cursor retention permits only active-key monotone extension' USING ERRCODE='23514';
    END IF;
    -- No current-TTL ceiling: a continued token can retain its original longer
    -- expiry after TTL was reduced. The issuing adapter validates the absolute
    -- token deadline and adds the maximum30s allowance before this update.
    RETURN NEW;
END $$;
CREATE TRIGGER cursor_lifecycle_retention_guard BEFORE UPDATE ON control.cursor_key_lifecycle
    FOR EACH ROW WHEN (NEW.decrypt_until_us IS DISTINCT FROM OLD.decrypt_until_us)
    EXECUTE FUNCTION control.guard_cursor_retention();

CREATE FUNCTION control.require_cursor_retention_commit() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
    -- Recheck after waits and held COMMIT. A lease observed at UPDATE time alone
    -- cannot authorize a subsequently expired owner to publish a new token.
    PERFORM control.require_cursor_retention_owner(NEW.dataset_id);
    RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER cursor_retention_complete AFTER UPDATE ON control.cursor_key_lifecycle
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
    WHEN (NEW.decrypt_until_us IS DISTINCT FROM OLD.decrypt_until_us)
    EXECUTE FUNCTION control.require_cursor_retention_commit();

REVOKE ALL ON FUNCTION control.require_cursor_retention_owner(bytea),
    control.guard_cursor_retention(),control.require_cursor_retention_commit() FROM PUBLIC;
