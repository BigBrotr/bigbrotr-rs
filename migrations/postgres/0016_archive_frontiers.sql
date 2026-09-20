-- Native snapshot bindings extend the existing consumer/pin ledgers. Historical
-- unbound rows remain diagnostic history and receive no retrospective authority.
ALTER TABLE control.owner_epochs ADD COLUMN archive_frontier_protocol smallint
    CHECK (archive_frontier_protocol IS NULL
        OR (archive_frontier_protocol = 1 AND role = 'archive'));
CREATE FUNCTION control.guard_archive_owner_protocol() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
BEGIN
    IF NEW.archive_frontier_protocol IS DISTINCT FROM OLD.archive_frontier_protocol THEN
        RAISE EXCEPTION 'archive owner protocol cannot be adopted retrospectively' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER archive_owner_protocol_guard BEFORE UPDATE ON control.owner_epochs
    FOR EACH ROW EXECUTE FUNCTION control.guard_archive_owner_protocol();

CREATE TABLE control.archive_owner_sessions (
    dataset_id control.id16 NOT NULL,
    catalog_id control.id16 NOT NULL,
    owner_epoch control.u64 NOT NULL,
    owner_role text NOT NULL DEFAULT 'archive' CHECK (owner_role = 'archive'),
    owner_incarnation control.id16 NOT NULL,
    coordinator_binding_sha256 control.digest32 NOT NULL,
    persistence_protocol smallint NOT NULL CHECK (persistence_protocol = 1),
    issuer_resource control.id16 NOT NULL,
    issuer_epoch control.u64 NOT NULL,
    issuer_role text NOT NULL DEFAULT 'control' CHECK (issuer_role = 'control'),
    issuer_incarnation control.id16 NOT NULL,
    PRIMARY KEY (catalog_id, owner_epoch),
    UNIQUE (dataset_id, catalog_id, owner_epoch, owner_incarnation),
    FOREIGN KEY (dataset_id, catalog_id, owner_epoch, owner_role, owner_incarnation)
        REFERENCES control.owner_epochs(dataset_id, resource_id, epoch, role, process_incarnation),
    FOREIGN KEY (dataset_id, catalog_id, coordinator_binding_sha256, persistence_protocol)
        REFERENCES control.archive_persistence(dataset_id, catalog_id, coordinator_binding_sha256, protocol_version),
    FOREIGN KEY (dataset_id, issuer_resource, issuer_epoch, issuer_role, issuer_incarnation)
        REFERENCES control.owner_epochs(dataset_id, resource_id, epoch, role, process_incarnation)
);

CREATE FUNCTION control.guard_archive_owner_session() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'archive owner attribution is immutable' USING ERRCODE = '23514';
    END IF;
    PERFORM control.require_control_owner(NEW.dataset_id, NEW.issuer_resource,
        NEW.issuer_epoch, NEW.issuer_incarnation);
    PERFORM 1 FROM control.owner_epochs WHERE dataset_id = NEW.dataset_id
        AND resource_id = NEW.catalog_id AND epoch = NEW.owner_epoch
        AND process_incarnation = NEW.owner_incarnation AND role = 'archive' AND state = 'active'
        AND archive_frontier_protocol = 1
        FOR SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'archive attribution requires its exact active owner' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER archive_owner_session_guard BEFORE INSERT OR UPDATE OR DELETE
    ON control.archive_owner_sessions FOR EACH ROW EXECUTE FUNCTION control.guard_archive_owner_session();
CREATE TRIGGER archive_owner_session_truncate_guard BEFORE TRUNCATE
    ON control.archive_owner_sessions FOR EACH STATEMENT EXECUTE FUNCTION control.guard_archive_owner_session();

-- Called again after all blocking work, including by deferred validation.
CREATE FUNCTION control.require_archive_frontier_owner(wanted_dataset bytea,
    wanted_catalog bytea, wanted_epoch numeric, wanted_incarnation bytea)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog, control AS $$
DECLARE attribution control.archive_owner_sessions%ROWTYPE;
BEGIN
    SELECT * INTO attribution FROM control.archive_owner_sessions
        WHERE dataset_id = wanted_dataset AND catalog_id = wanted_catalog
            AND owner_epoch = wanted_epoch AND owner_incarnation = wanted_incarnation;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Archive owner lacks qualified attribution' USING ERRCODE = '23514';
    END IF;
    PERFORM control.require_control_owner(wanted_dataset, attribution.issuer_resource,
        attribution.issuer_epoch, attribution.issuer_incarnation);
    PERFORM 1 FROM control.owner_epochs o JOIN control.role_instances r USING (role_instance_id)
        WHERE o.resource_id = wanted_catalog AND o.epoch = wanted_epoch AND o.state = 'active'
            AND o.archive_frontier_protocol = 1 AND o.process_incarnation = wanted_incarnation
            AND r.retired_at_us IS NULL FOR SHARE OF o, r;
    IF NOT FOUND OR NOT EXISTS (SELECT 1 FROM control.owner_epochs
        WHERE resource_id = wanted_catalog AND epoch = wanted_epoch
            AND lease_expires_at_us > (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint)
        OR NOT EXISTS (SELECT 1 FROM control.owner_epochs
        WHERE resource_id = attribution.issuer_resource AND epoch = attribution.issuer_epoch
            AND lease_expires_at_us > (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint) THEN
        RAISE EXCEPTION 'Archive frontier requires unexpired actual owners' USING ERRCODE = '23514';
    END IF;
END;
$$;

CREATE TABLE control.archive_frontier_snapshots (
    operation_id control.id16 NOT NULL UNIQUE,
    consumer_id control.id16 NOT NULL,
    frontier_revision control.u64 NOT NULL,
    dataset_id control.id16 NOT NULL,
    topology_generation control.u64 NOT NULL,
    catalog_id control.id16 NOT NULL,
    owner_epoch control.u64 NOT NULL,
    owner_incarnation control.id16 NOT NULL,
    snapshot_id bigint NOT NULL CHECK (snapshot_id > 0),
    request_digest control.digest32 NOT NULL,
    dictionary_count control.u64 NOT NULL,
    payload_count control.u64 NOT NULL,
    eligibility_count control.u64 NOT NULL,
    receive_count control.u64 NOT NULL,
    provenance_count control.u64 NOT NULL,
    PRIMARY KEY (consumer_id, frontier_revision),
    CHECK (consumer_id = catalog_id),
    FOREIGN KEY (consumer_id, dataset_id, frontier_revision, topology_generation)
        REFERENCES control.consumer_frontier_revisions(consumer_id, dataset_id, frontier_revision, topology_generation),
    FOREIGN KEY (dataset_id, catalog_id, owner_epoch, owner_incarnation)
        REFERENCES control.archive_owner_sessions(dataset_id, catalog_id, owner_epoch, owner_incarnation)
);

CREATE FUNCTION control.guard_archive_frontier_snapshot() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'archive snapshot binding is immutable' USING ERRCODE = '23514';
    END IF;
    PERFORM control.require_archive_frontier_owner(NEW.dataset_id, NEW.catalog_id, NEW.owner_epoch, NEW.owner_incarnation);
    PERFORM 1 FROM control.consumer_frontier_revisions
        WHERE consumer_id = NEW.consumer_id AND frontier_revision = NEW.frontier_revision
            AND NOT sealed AND consumer_role = 'archive' AND owner_resource_id = NEW.catalog_id
            AND owner_epoch = NEW.owner_epoch AND owner_incarnation = NEW.owner_incarnation
        FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'native binding requires its exact assembling Archive frontier' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER archive_frontier_snapshot_guard BEFORE INSERT OR UPDATE OR DELETE
    ON control.archive_frontier_snapshots FOR EACH ROW EXECUTE FUNCTION control.guard_archive_frontier_snapshot();
CREATE TRIGGER archive_frontier_snapshot_truncate_guard BEFORE TRUNCATE
    ON control.archive_frontier_snapshots FOR EACH STATEMENT EXECUTE FUNCTION control.guard_archive_frontier_snapshot();

-- Exact selected intent identities allow recovery of the original complete local
-- input headers. This is not a second admission or native receipt authority.
CREATE TABLE control.archive_frontier_batches (
    consumer_id control.id16 NOT NULL,
    frontier_revision control.u64 NOT NULL,
    attempt_id control.id16 NOT NULL REFERENCES control.archive_attempts(attempt_id),
    PRIMARY KEY (consumer_id, frontier_revision, attempt_id),
    FOREIGN KEY (consumer_id, frontier_revision)
        REFERENCES control.archive_frontier_snapshots(consumer_id, frontier_revision)
);
CREATE FUNCTION control.guard_archive_frontier_batch() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'archive frontier inputs are immutable' USING ERRCODE = '23514';
    END IF;
    PERFORM 1 FROM control.consumer_frontier_revisions
        WHERE consumer_id = NEW.consumer_id AND frontier_revision = NEW.frontier_revision AND NOT sealed
        FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'sealed frontier cannot accept another input' USING ERRCODE = '23514';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM control.archive_frontier_snapshots f
        JOIN control.archive_owner_sessions o ON o.catalog_id = f.catalog_id AND o.owner_epoch = f.owner_epoch
        JOIN control.archive_attempts a ON a.attempt_id = NEW.attempt_id
        WHERE f.consumer_id = NEW.consumer_id AND f.frontier_revision = NEW.frontier_revision
            AND a.dataset_id = f.dataset_id AND a.catalog_id = f.catalog_id
            AND a.topology_generation <= f.topology_generation
            AND a.persistence_protocol = 1 AND a.coordinator_binding_sha256 = o.coordinator_binding_sha256) THEN
        RAISE EXCEPTION 'frontier input lacks matching persistence participation' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER archive_frontier_batch_guard BEFORE INSERT OR UPDATE OR DELETE
    ON control.archive_frontier_batches FOR EACH ROW EXECUTE FUNCTION control.guard_archive_frontier_batch();
CREATE TRIGGER archive_frontier_batch_truncate_guard BEFORE TRUNCATE
    ON control.archive_frontier_batches FOR EACH STATEMENT EXECUTE FUNCTION control.guard_archive_frontier_batch();

CREATE FUNCTION control.require_archive_frontier_snapshot() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
DECLARE attribution control.archive_owner_sessions%ROWTYPE;
BEGIN
    IF NEW.consumer_role <> 'archive' THEN RETURN NULL; END IF;
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'archive frontier publication requires READ COMMITTED' USING ERRCODE = '25001';
    END IF;
    SELECT o.* INTO attribution FROM control.archive_frontier_snapshots f
        JOIN control.archive_owner_sessions o ON o.catalog_id = f.catalog_id AND o.owner_epoch = f.owner_epoch
        WHERE f.consumer_id = NEW.consumer_id AND f.frontier_revision = NEW.frontier_revision
            AND f.catalog_id = NEW.owner_resource_id AND f.owner_epoch = NEW.owner_epoch
            AND f.owner_incarnation = NEW.owner_incarnation;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Archive frontier requires its native snapshot binding' USING ERRCODE = '23514';
    END IF;
    PERFORM control.require_archive_frontier_owner(NEW.dataset_id, NEW.owner_resource_id, NEW.owner_epoch, NEW.owner_incarnation);
    IF NOT EXISTS (SELECT 1 FROM control.consumers WHERE consumer_id = NEW.consumer_id
        AND current_revision >= NEW.frontier_revision) THEN
        RAISE EXCEPTION 'Archive frontier must publish its head atomically' USING ERRCODE = '23514';
    END IF;
    IF EXISTS (SELECT 1 FROM control.consumer_frontiers b
        WHERE b.consumer_id = NEW.consumer_id AND b.frontier_revision = NEW.frontier_revision
            AND b.last_complete_sequence > 0 AND NOT EXISTS (
                SELECT 1 FROM control.archive_frontier_batches i
                JOIN control.archive_attempts a USING (attempt_id)
                WHERE i.consumer_id = b.consumer_id AND i.frontier_revision = b.frontier_revision
                    AND a.batch_digest = b.attestation_digest)) THEN
        RAISE EXCEPTION 'Archive frontier attestation is absent from its retained inputs' USING ERRCODE = '23514';
    END IF;
    RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER archive_frontier_snapshot_complete AFTER INSERT OR UPDATE
    ON control.consumer_frontier_revisions DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION control.require_archive_frontier_snapshot();

-- An old sealed-but-unpublished revision must not bypass the new constraint by
-- changing only the consumer head after migration. Do not backfill old bindings.
CREATE FUNCTION control.guard_archive_consumer_head() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
DECLARE attribution control.archive_owner_sessions%ROWTYPE;
BEGIN
    IF NEW.consumer_role <> 'archive' THEN RETURN NEW; END IF;
    SELECT o.* INTO attribution FROM control.archive_frontier_snapshots f
        JOIN control.archive_owner_sessions o ON o.catalog_id = f.catalog_id AND o.owner_epoch = f.owner_epoch
        JOIN control.consumer_frontier_revisions r ON r.consumer_id = f.consumer_id AND r.frontier_revision = f.frontier_revision
        WHERE f.consumer_id = NEW.consumer_id AND f.frontier_revision = NEW.current_revision
            AND r.owner_resource_id = o.catalog_id AND r.owner_epoch = o.owner_epoch
            AND r.owner_incarnation = o.owner_incarnation;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Archive head requires its qualified native binding' USING ERRCODE = '23514';
    END IF;
    PERFORM control.require_archive_frontier_owner(NEW.dataset_id, attribution.catalog_id, attribution.owner_epoch, attribution.owner_incarnation);
    RETURN NEW;
END;
$$;
CREATE TRIGGER archive_consumer_head_guard BEFORE UPDATE ON control.consumers
    FOR EACH ROW EXECUTE FUNCTION control.guard_archive_consumer_head();

REVOKE ALL ON control.archive_owner_sessions, control.archive_frontier_snapshots,
    control.archive_frontier_batches FROM PUBLIC;
REVOKE ALL ON FUNCTION control.guard_archive_owner_session(),
    control.guard_archive_frontier_snapshot(), control.guard_archive_frontier_batch(),
    control.require_archive_frontier_snapshot(), control.guard_archive_consumer_head(),
    control.guard_archive_owner_protocol(),
    control.require_archive_frontier_owner(bytea, bytea, numeric, bytea) FROM PUBLIC;
