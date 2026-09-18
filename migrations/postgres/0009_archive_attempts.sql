-- Archive publication intent issuance, not a catalog commit receipt or a lease.
-- Existing archive owners predate this protocol and cannot be adopted implicitly.
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM control.owner_epochs WHERE role = 'archive') THEN
        RAISE EXCEPTION 'unqualified archive owner history requires explicit recovery'
            USING ERRCODE = '55000';
    END IF;
END $$;

CREATE TABLE control.archive_attempts (
    attempt_id control.id16 PRIMARY KEY,
    dataset_id control.id16 NOT NULL REFERENCES control.datasets(dataset_id),
    catalog_id control.id16 NOT NULL,
    ordinal control.u64 NOT NULL CHECK (ordinal > 0),
    expected_predecessor control.u64 NOT NULL,
    schema_version smallint NOT NULL CHECK (schema_version = 1),
    topology_generation control.u64 NOT NULL,
    batch_digest control.digest32 NOT NULL,
    coordinator_incarnation control.id16 NOT NULL,
    worker_incarnation control.id16 NOT NULL UNIQUE,
    issuer_resource control.id16 NOT NULL,
    issuer_epoch control.u64 NOT NULL,
    issuer_role text NOT NULL DEFAULT 'control' CHECK (issuer_role = 'control'),
    issuer_incarnation control.id16 NOT NULL,
    issued_at_us bigint NOT NULL,
    UNIQUE (dataset_id, ordinal),
    CHECK (ordinal = expected_predecessor + 1),
    CHECK (coordinator_incarnation <> worker_incarnation),
    CHECK (catalog_id <> issuer_resource),
    FOREIGN KEY (dataset_id, topology_generation)
        REFERENCES control.topology_generations(dataset_id, generation),
    FOREIGN KEY (dataset_id, issuer_resource, issuer_epoch, issuer_role, issuer_incarnation)
        REFERENCES control.owner_epochs(dataset_id, resource_id, epoch, role, process_incarnation)
);

CREATE FUNCTION control.guard_archive_attempt() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
DECLARE prior control.archive_attempts%ROWTYPE;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'archive attempt history is immutable' USING ERRCODE = '23514';
    END IF;
    PERFORM control.require_control_owner(NEW.dataset_id, NEW.issuer_resource,
        NEW.issuer_epoch, NEW.issuer_incarnation);
    -- The fenced Control backend serializes issuance; the dataset lock also
    -- makes the predecessor check explicit at the retained application boundary.
    PERFORM 1 FROM control.datasets WHERE dataset_id = NEW.dataset_id FOR UPDATE;
    SELECT * INTO prior FROM control.archive_attempts
        WHERE dataset_id = NEW.dataset_id ORDER BY ordinal DESC LIMIT 1;
    IF (NOT FOUND AND NEW.expected_predecessor <> 0)
        OR (FOUND AND (NEW.expected_predecessor <> prior.ordinal
            OR NEW.catalog_id <> prior.catalog_id)) THEN
        RAISE EXCEPTION 'archive attempt predecessor or catalog mismatch' USING ERRCODE = '23514';
    END IF;
    PERFORM 1 FROM control.topology_generations
        WHERE dataset_id = NEW.dataset_id AND generation = NEW.topology_generation AND sealed;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'archive attempt requires a retained sealed topology' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER archive_attempt_guard BEFORE INSERT OR UPDATE OR DELETE
    ON control.archive_attempts FOR EACH ROW EXECUTE FUNCTION control.guard_archive_attempt();
CREATE TRIGGER archive_attempt_truncate_guard BEFORE TRUNCATE
    ON control.archive_attempts FOR EACH STATEMENT EXECUTE FUNCTION control.guard_archive_attempt();
REVOKE ALL ON FUNCTION control.guard_archive_attempt() FROM PUBLIC;
