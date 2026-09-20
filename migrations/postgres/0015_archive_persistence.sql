-- Qualified local file persistence starts at a separately verified virgin
-- catalog. Keep older attempts unchanged and inspectable; no retrospective
-- claim can be inferred from installing this migration or a newer binary.
CREATE TABLE control.archive_persistence (
    dataset_id control.id16 PRIMARY KEY REFERENCES control.archive_coordinators(dataset_id),
    catalog_id control.id16 NOT NULL,
    coordinator_binding_sha256 control.digest32 NOT NULL,
    protocol_version smallint NOT NULL CHECK (protocol_version = 1),
    protection_class text NOT NULL CHECK (protection_class = 'local-synced'),
    baseline_snapshot bigint NOT NULL CHECK (baseline_snapshot = 1),
    issuer_resource control.id16 NOT NULL,
    issuer_epoch control.u64 NOT NULL,
    issuer_role text NOT NULL DEFAULT 'control' CHECK (issuer_role = 'control'),
    issuer_incarnation control.id16 NOT NULL,
    initialized_at_us bigint NOT NULL,
    UNIQUE (dataset_id, catalog_id, coordinator_binding_sha256, protocol_version),
    FOREIGN KEY (dataset_id, issuer_resource, issuer_epoch, issuer_role, issuer_incarnation)
        REFERENCES control.owner_epochs(dataset_id, resource_id, epoch, role, process_incarnation)
);

CREATE FUNCTION control.guard_archive_persistence() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'archive persistence participation is immutable' USING ERRCODE = '23514';
    END IF;
    PERFORM control.require_control_owner(NEW.dataset_id, NEW.issuer_resource,
        NEW.issuer_epoch, NEW.issuer_incarnation);
    -- Same order as intent issuance, and a new READ COMMITTED statement after
    -- the lock. Failed/ambiguous attempts also make history non-virgin.
    PERFORM 1 FROM control.datasets WHERE dataset_id = NEW.dataset_id FOR UPDATE;
    IF EXISTS (SELECT 1 FROM control.archive_attempts WHERE dataset_id = NEW.dataset_id)
        OR NOT EXISTS (SELECT 1 FROM control.archive_coordinators
            WHERE dataset_id = NEW.dataset_id AND catalog_id = NEW.catalog_id
                AND protocol_version = 1 AND binding_sha256 = NEW.coordinator_binding_sha256) THEN
        RAISE EXCEPTION 'archive persistence requires matching empty coordinator history'
            USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER archive_persistence_guard BEFORE INSERT OR UPDATE OR DELETE
    ON control.archive_persistence FOR EACH ROW EXECUTE FUNCTION control.guard_archive_persistence();
CREATE TRIGGER archive_persistence_truncate_guard BEFORE TRUNCATE
    ON control.archive_persistence FOR EACH STATEMENT EXECUTE FUNCTION control.guard_archive_persistence();

ALTER TABLE control.archive_attempts ADD COLUMN persistence_protocol smallint
    CHECK (persistence_protocol = 1);
ALTER TABLE control.archive_attempts ADD FOREIGN KEY
    (dataset_id, catalog_id, coordinator_binding_sha256, persistence_protocol)
    REFERENCES control.archive_persistence
        (dataset_id, catalog_id, coordinator_binding_sha256, protocol_version);

CREATE FUNCTION control.require_archive_persistence() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
BEGIN
    -- The alphabetically earlier archive_attempt_guard already holds the Control
    -- fence and dataset lock. Old NULL history is retained; new NULL is refused.
    IF NEW.persistence_protocol IS NULL OR NOT EXISTS (
        SELECT 1 FROM control.archive_persistence
        WHERE dataset_id = NEW.dataset_id AND catalog_id = NEW.catalog_id
            AND coordinator_binding_sha256 = NEW.coordinator_binding_sha256
            AND protocol_version = NEW.persistence_protocol
    ) THEN
        RAISE EXCEPTION 'archive issuance requires qualified persistence participation'
            USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER archive_persistence_attempt_guard BEFORE INSERT ON control.archive_attempts
    FOR EACH ROW EXECUTE FUNCTION control.require_archive_persistence();
REVOKE ALL ON control.archive_persistence FROM PUBLIC;
REVOKE ALL ON FUNCTION control.guard_archive_persistence() FROM PUBLIC;
REVOKE ALL ON FUNCTION control.require_archive_persistence() FROM PUBLIC;
