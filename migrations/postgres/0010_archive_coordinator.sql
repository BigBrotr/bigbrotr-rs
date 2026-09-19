-- Preserve pre-protocol attempts for inspection, but never adopt their custody.
-- An explicit binding starts only at an empty dataset history under Control's
-- actual session/transaction fence. Later issuance names that immutable binding.
CREATE TABLE control.archive_coordinators (
    dataset_id control.id16 PRIMARY KEY REFERENCES control.datasets(dataset_id),
    catalog_id control.id16 NOT NULL,
    binding_sha256 control.digest32 NOT NULL,
    protocol_version smallint NOT NULL CHECK (protocol_version = 1),
    issuer_resource control.id16 NOT NULL,
    issuer_epoch control.u64 NOT NULL,
    issuer_role text NOT NULL DEFAULT 'control' CHECK (issuer_role = 'control'),
    issuer_incarnation control.id16 NOT NULL,
    initialized_at_us bigint NOT NULL,
    FOREIGN KEY (dataset_id, issuer_resource, issuer_epoch, issuer_role, issuer_incarnation)
        REFERENCES control.owner_epochs(dataset_id, resource_id, epoch, role, process_incarnation)
);

CREATE FUNCTION control.guard_archive_coordinator() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'archive coordinator binding is immutable' USING ERRCODE = '23514';
    END IF;
    PERFORM control.require_control_owner(NEW.dataset_id, NEW.issuer_resource,
        NEW.issuer_epoch, NEW.issuer_incarnation);
    PERFORM 1 FROM control.datasets WHERE dataset_id = NEW.dataset_id FOR UPDATE;
    IF EXISTS (SELECT 1 FROM control.archive_attempts WHERE dataset_id = NEW.dataset_id) THEN
        RAISE EXCEPTION 'pre-protocol archive history requires explicit recovery'
            USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER archive_coordinator_guard BEFORE INSERT OR UPDATE OR DELETE
    ON control.archive_coordinators FOR EACH ROW EXECUTE FUNCTION control.guard_archive_coordinator();
CREATE TRIGGER archive_coordinator_truncate_guard BEFORE TRUNCATE
    ON control.archive_coordinators FOR EACH STATEMENT EXECUTE FUNCTION control.guard_archive_coordinator();

-- NULL remains only on historical rows. No checksum or historical request is
-- rewritten; readers may still inspect them, but protocol initialization refuses.
ALTER TABLE control.archive_attempts ADD COLUMN coordinator_binding_sha256 control.digest32;
CREATE FUNCTION control.require_archive_coordinator() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
BEGIN
    -- The existing alphabetically earlier archive_attempt_guard has already
    -- retained the actual Control fence and locked this dataset for issuance.
    IF NEW.coordinator_binding_sha256 IS NULL OR NOT EXISTS (
        SELECT 1 FROM control.archive_coordinators
        WHERE dataset_id = NEW.dataset_id AND catalog_id = NEW.catalog_id
            AND protocol_version = 1 AND binding_sha256 = NEW.coordinator_binding_sha256
    ) THEN
        RAISE EXCEPTION 'archive issuance requires its initialized coordinator binding'
            USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER archive_coordinator_attempt_guard BEFORE INSERT ON control.archive_attempts
    FOR EACH ROW EXECUTE FUNCTION control.require_archive_coordinator();
REVOKE ALL ON FUNCTION control.guard_archive_coordinator() FROM PUBLIC;
REVOKE ALL ON FUNCTION control.require_archive_coordinator() FROM PUBLIC;
