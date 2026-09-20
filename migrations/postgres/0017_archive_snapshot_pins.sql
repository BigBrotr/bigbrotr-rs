-- Additive qualification of the common lease ledger. Old rows and their claims
-- remain retention obligations; NULL is never retrospectively adopted.
ALTER TABLE control.snapshot_leases ADD COLUMN archive_pin_protocol smallint
    CHECK (archive_pin_protocol IS NULL OR archive_pin_protocol = 1);
CREATE INDEX snapshot_leases_active_catalog ON control.snapshot_leases(consumer_id, lease_id)
    WHERE state = 'active';

-- A stable row exists before any pin. Maintenance must take this same lock and
-- then scan obligations in a NEW READ COMMITTED statement, including legacy and
-- expired-but-unreleased rows. A lock on existing leases alone misses insertion.
CREATE FUNCTION control.lock_archive_pins(wanted_dataset bytea, wanted_catalog bytea)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog, control AS $$
BEGIN
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'archive pins require READ COMMITTED' USING ERRCODE = '25001';
    END IF;
    PERFORM 1 FROM control.consumers WHERE dataset_id = wanted_dataset
        AND consumer_id = wanted_catalog AND consumer_role = 'archive' FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'archive pin requires its registered consumer' USING ERRCODE = '23514';
    END IF;
END;
$$;

CREATE TABLE control.archive_snapshot_pins (
    lease_id control.id16 PRIMARY KEY REFERENCES control.snapshot_leases(lease_id),
    dataset_id control.id16 NOT NULL,
    consumer_id control.id16 NOT NULL,
    frontier_revision control.u64 NOT NULL,
    issuer_resource control.id16 NOT NULL,
    issuer_epoch control.u64 NOT NULL,
    issuer_role text NOT NULL DEFAULT 'control' CHECK (issuer_role = 'control'),
    issuer_incarnation control.id16 NOT NULL,
    purpose text NOT NULL CHECK (purpose IN ('reader','export','backup','derived','publication','maintenance')),
    requested_ttl_us bigint NOT NULL CHECK (requested_ttl_us BETWEEN 1 AND 120000000),
    FOREIGN KEY (consumer_id, frontier_revision)
        REFERENCES control.archive_frontier_snapshots(consumer_id, frontier_revision),
    FOREIGN KEY (dataset_id, issuer_resource, issuer_epoch, issuer_role, issuer_incarnation)
        REFERENCES control.owner_epochs(dataset_id, resource_id, epoch, role, process_incarnation)
);

CREATE FUNCTION control.require_archive_pin_holder(wanted_lease control.snapshot_leases,
    require_live boolean) RETURNS void LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
DECLARE holder control.owner_epochs%ROWTYPE;
DECLARE now_us bigint;
BEGIN
    IF wanted_lease.holder_kind <> 'role_instance' OR wanted_lease.holder_role <> 'control' THEN
        RAISE EXCEPTION 'native pin protocol1 requires its actual Control holder' USING ERRCODE = '23514';
    END IF;
    SELECT * INTO holder FROM control.owner_epochs WHERE dataset_id = wanted_lease.dataset_id
        AND role = 'control' AND role_instance_id = wanted_lease.holder_role_instance_id
        AND process_incarnation = wanted_lease.holder_incarnation AND state = 'active' FOR SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'native pin holder is not current' USING ERRCODE = '23514';
    END IF;
    PERFORM control.require_control_owner(holder.dataset_id, holder.resource_id,
        holder.epoch, holder.process_incarnation);
    -- Sample after every owner/role lock. Caller timestamps cannot renew expiry.
    now_us := (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint;
    IF require_live AND (holder.lease_expires_at_us <= now_us
        OR wanted_lease.expires_at_us <= now_us
        OR wanted_lease.expires_at_us > holder.lease_expires_at_us) THEN
        RAISE EXCEPTION 'native pin requires unexpired actual holder and lease' USING ERRCODE = '23514';
    END IF;
END;
$$;

CREATE FUNCTION control.guard_archive_snapshot_lease() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
DECLARE now_us bigint;
BEGIN
    IF TG_OP = 'TRUNCATE' THEN
        RAISE EXCEPTION 'snapshot lease history is retained' USING ERRCODE = '23514';
    END IF;
    IF TG_OP = 'UPDATE' AND NEW.archive_pin_protocol IS DISTINCT FROM OLD.archive_pin_protocol THEN
        RAISE EXCEPTION 'native pin protocol is immutable' USING ERRCODE = '23514';
    END IF;
    IF TG_OP = 'INSERT' AND EXISTS (SELECT 1 FROM control.consumers
        WHERE consumer_id = NEW.consumer_id AND consumer_role = 'archive')
        AND NEW.archive_pin_protocol IS DISTINCT FROM 1 THEN
        RAISE EXCEPTION 'new Archive lease requires explicit native pin participation' USING ERRCODE = '23514';
    END IF;
    IF NEW.archive_pin_protocol IS NULL THEN RETURN NEW; END IF;
    PERFORM control.lock_archive_pins(NEW.dataset_id, NEW.consumer_id);
    PERFORM control.require_archive_pin_holder(NEW, NEW.state = 'active');
    now_us := (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint;
    IF NEW.state = 'active' AND (NEW.expires_at_us <= now_us
        OR NEW.expires_at_us - now_us > 120000000
        OR (TG_OP = 'UPDATE' AND OLD.expires_at_us <= now_us)) THEN
        RAISE EXCEPTION 'native pin expiry cannot be extended after a gap' USING ERRCODE = '23514';
    END IF;
    -- These timestamps are PostgreSQL observations, not caller authority. The
    -- existing CAS guard still validates immutable identity and revision order.
    IF TG_OP = 'INSERT' THEN NEW.acquired_at_us := now_us; END IF;
    NEW.updated_at_us := now_us;
    IF NEW.state = 'released' THEN NEW.released_at_us := now_us; END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER archive_snapshot_lease_guard BEFORE INSERT OR UPDATE ON control.snapshot_leases
    FOR EACH ROW EXECUTE FUNCTION control.guard_archive_snapshot_lease();
CREATE TRIGGER archive_snapshot_lease_truncate_guard BEFORE TRUNCATE ON control.snapshot_leases
    FOR EACH STATEMENT EXECUTE FUNCTION control.guard_archive_snapshot_lease();

CREATE FUNCTION control.guard_archive_snapshot_pin() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
DECLARE lease control.snapshot_leases%ROWTYPE;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'native snapshot pin attribution is immutable' USING ERRCODE = '23514';
    END IF;
    SELECT * INTO STRICT lease FROM control.snapshot_leases WHERE lease_id = NEW.lease_id;
    PERFORM control.lock_archive_pins(lease.dataset_id, lease.consumer_id);
    SELECT * INTO STRICT lease FROM control.snapshot_leases WHERE lease_id = NEW.lease_id FOR UPDATE;
    IF lease.archive_pin_protocol IS DISTINCT FROM 1 OR lease.lease_revision <> 1
        OR lease.state <> 'active' OR lease.dataset_id <> NEW.dataset_id
        OR lease.consumer_id <> NEW.consumer_id OR lease.frontier_revision <> NEW.frontier_revision
        OR NOT EXISTS (SELECT 1 FROM control.archive_frontier_snapshots
            WHERE consumer_id = NEW.consumer_id AND frontier_revision = NEW.frontier_revision
                AND dataset_id = lease.dataset_id AND topology_generation = lease.topology_generation)
        OR NOT EXISTS (SELECT 1 FROM control.owner_epochs WHERE dataset_id = NEW.dataset_id
            AND resource_id = NEW.issuer_resource AND epoch = NEW.issuer_epoch
            AND role = 'control' AND process_incarnation = NEW.issuer_incarnation
            AND role_instance_id = lease.holder_role_instance_id
            AND process_incarnation = lease.holder_incarnation) THEN
        RAISE EXCEPTION 'native pin must bind its exact new lease, frontier and holder' USING ERRCODE = '23514';
    END IF;
    PERFORM control.require_archive_pin_holder(lease, true);
    RETURN NEW;
END;
$$;
CREATE TRIGGER archive_snapshot_pin_guard BEFORE INSERT OR UPDATE OR DELETE ON control.archive_snapshot_pins
    FOR EACH ROW EXECUTE FUNCTION control.guard_archive_snapshot_pin();
CREATE TRIGGER archive_snapshot_pin_truncate_guard BEFORE TRUNCATE ON control.archive_snapshot_pins
    FOR EACH STATEMENT EXECUTE FUNCTION control.guard_archive_snapshot_pin();

CREATE FUNCTION control.guard_archive_pin_claim() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
DECLARE lease control.snapshot_leases%ROWTYPE;
DECLARE pin control.archive_snapshot_pins%ROWTYPE;
BEGIN
    IF TG_OP = 'TRUNCATE' THEN
        RAISE EXCEPTION 'retention claim history is retained' USING ERRCODE = '23514';
    END IF;
    SELECT * INTO STRICT lease FROM control.snapshot_leases WHERE lease_id = NEW.lease_id;
    IF lease.archive_pin_protocol IS NULL THEN RETURN NEW; END IF;
    PERFORM control.lock_archive_pins(lease.dataset_id, lease.consumer_id);
    SELECT * INTO STRICT lease FROM control.snapshot_leases WHERE lease_id = NEW.lease_id FOR UPDATE;
    PERFORM control.require_archive_pin_holder(lease, TG_OP = 'INSERT');
    SELECT * INTO STRICT pin FROM control.archive_snapshot_pins WHERE lease_id = NEW.lease_id;
    IF TG_OP = 'INSERT' THEN
        IF NEW.purpose <> pin.purpose OR NEW.first_sequence <> 1
            OR NOT EXISTS (SELECT 1 FROM control.consumer_frontiers
                WHERE consumer_id = lease.consumer_id AND frontier_revision = lease.frontier_revision
                    AND shard_id = NEW.shard_id AND owner_epoch = NEW.owner_epoch
                    AND last_complete_sequence = NEW.last_sequence AND last_complete_sequence > 0) THEN
            RAISE EXCEPTION 'native pin claim must retain its exact positive prefix' USING ERRCODE = '23514';
        END IF;
        NEW.created_at_us := (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint;
    ELSE
        NEW.released_at_us := (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint;
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER archive_pin_claim_guard BEFORE INSERT OR UPDATE ON control.retention_claims
    FOR EACH ROW EXECUTE FUNCTION control.guard_archive_pin_claim();
CREATE TRIGGER archive_pin_claim_truncate_guard BEFORE TRUNCATE ON control.retention_claims
    FOR EACH STATEMENT EXECUTE FUNCTION control.guard_archive_pin_claim();

CREATE FUNCTION control.require_complete_archive_pin() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
DECLARE lease control.snapshot_leases%ROWTYPE;
DECLARE pin control.archive_snapshot_pins%ROWTYPE;
DECLARE now_us bigint;
BEGIN
    SELECT * INTO STRICT lease FROM control.snapshot_leases WHERE lease_id = NEW.lease_id;
    IF lease.archive_pin_protocol IS NULL THEN RETURN NULL; END IF;
    SELECT * INTO pin FROM control.archive_snapshot_pins WHERE lease_id = lease.lease_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'qualified lease requires atomic native snapshot binding' USING ERRCODE = '23514';
    END IF;
    PERFORM control.require_archive_pin_holder(lease, lease.state = 'active');
    now_us := (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint;
    IF lease.state = 'active' THEN
        IF lease.expires_at_us <= now_us OR lease.expires_at_us - now_us > pin.requested_ttl_us THEN
            RAISE EXCEPTION 'native pin exceeds its declared lifetime' USING ERRCODE = '23514';
        END IF;
        IF TG_TABLE_NAME = 'snapshot_leases' AND TG_OP = 'UPDATE' THEN
            IF OLD.state = 'active' AND NEW.state = 'active' AND OLD.expires_at_us <= now_us THEN
                RAISE EXCEPTION 'native renewal commit crossed the old expiry' USING ERRCODE = '23514';
            END IF;
        END IF;
        IF EXISTS (SELECT 1 FROM control.consumer_frontiers b
            WHERE b.consumer_id = lease.consumer_id AND b.frontier_revision = lease.frontier_revision
                AND b.last_complete_sequence > 0 AND NOT EXISTS (
                    SELECT 1 FROM control.retention_claims c WHERE c.lease_id = lease.lease_id
                        AND c.shard_id = b.shard_id AND c.owner_epoch = b.owner_epoch
                        AND c.first_sequence = 1 AND c.last_sequence = b.last_complete_sequence
                        AND c.purpose = pin.purpose AND c.state = 'active')) THEN
            RAISE EXCEPTION 'native pin requires every positive source prefix atomically' USING ERRCODE = '23514';
        END IF;
    ELSIF EXISTS (SELECT 1 FROM control.retention_claims WHERE lease_id = lease.lease_id AND state = 'active') THEN
        RAISE EXCEPTION 'native lease release must release its source claims atomically' USING ERRCODE = '23514';
    END IF;
    RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER archive_pin_complete AFTER INSERT OR UPDATE ON control.snapshot_leases
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION control.require_complete_archive_pin();
CREATE CONSTRAINT TRIGGER archive_pin_claim_complete AFTER INSERT OR UPDATE ON control.retention_claims
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION control.require_complete_archive_pin();

REVOKE ALL ON control.archive_snapshot_pins FROM PUBLIC;
REVOKE ALL ON FUNCTION control.lock_archive_pins(bytea, bytea),
    control.require_archive_pin_holder(control.snapshot_leases, boolean),
    control.guard_archive_snapshot_lease(), control.guard_archive_snapshot_pin(),
    control.guard_archive_pin_claim(), control.require_complete_archive_pin() FROM PUBLIC;
