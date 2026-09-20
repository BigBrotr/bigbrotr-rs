-- Explicit recovery after fresh local descriptor exclusion. No lease, binding or
-- claim is adopted or rewritten by migration; expiry alone authorizes nothing.
CREATE TABLE control.archive_pin_recoveries (
    lease_id control.id16 PRIMARY KEY REFERENCES control.archive_snapshot_pins(lease_id),
    dataset_id control.id16 NOT NULL,
    consumer_id control.id16 NOT NULL,
    expected_lease_revision control.u64 NOT NULL CHECK (expected_lease_revision > 0),
    coordinator_binding_sha256 control.digest32 NOT NULL,
    exclusion_protocol smallint NOT NULL CHECK (exclusion_protocol = 1),
    recoverer_resource control.id16 NOT NULL,
    recoverer_epoch control.u64 NOT NULL,
    recoverer_role text NOT NULL DEFAULT 'control' CHECK (recoverer_role = 'control'),
    recoverer_incarnation control.id16 NOT NULL,
    recovered_at_us bigint NOT NULL,
    FOREIGN KEY (dataset_id,recoverer_resource,recoverer_epoch,recoverer_role,recoverer_incarnation)
        REFERENCES control.owner_epochs(dataset_id,resource_id,epoch,role,process_incarnation)
);

CREATE FUNCTION control.require_archive_pin_recovery(wanted_lease control.snapshot_leases)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog, control AS $$
DECLARE recovery control.archive_pin_recoveries%ROWTYPE;
BEGIN
    SELECT * INTO STRICT recovery FROM control.archive_pin_recoveries
        WHERE lease_id = wanted_lease.lease_id;
    IF wanted_lease.archive_pin_protocol IS DISTINCT FROM 1
        OR wanted_lease.dataset_id <> recovery.dataset_id
        OR wanted_lease.consumer_id <> recovery.consumer_id
        OR (wanted_lease.state = 'active' AND wanted_lease.lease_revision <> recovery.expected_lease_revision)
        OR (wanted_lease.state = 'released' AND wanted_lease.lease_revision <> recovery.expected_lease_revision + 1)
        OR NOT EXISTS (SELECT 1 FROM control.archive_snapshot_pins pin
            JOIN control.owner_epochs original ON original.resource_id = pin.issuer_resource
                AND original.epoch = pin.issuer_epoch
            JOIN control.archive_persistence persistence ON persistence.dataset_id = pin.dataset_id
                AND persistence.catalog_id = pin.consumer_id
            WHERE pin.lease_id = wanted_lease.lease_id AND original.state = 'released'
                AND persistence.coordinator_binding_sha256 = recovery.coordinator_binding_sha256
                AND ROW(pin.issuer_resource,pin.issuer_epoch,pin.issuer_incarnation)
                    IS DISTINCT FROM ROW(recovery.recoverer_resource,recovery.recoverer_epoch,recovery.recoverer_incarnation)) THEN
        RAISE EXCEPTION 'pin recovery requires its exact excluded original holder and local binding' USING ERRCODE = '23514';
    END IF;
    PERFORM control.require_control_owner(recovery.dataset_id,recovery.recoverer_resource,
        recovery.recoverer_epoch,recovery.recoverer_incarnation);
END;
$$;

CREATE FUNCTION control.guard_archive_pin_recovery() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
DECLARE lease control.snapshot_leases%ROWTYPE;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'archive pin recovery attribution is immutable' USING ERRCODE = '23514';
    END IF;
    PERFORM control.lock_archive_pins(NEW.dataset_id, NEW.consumer_id);
    SELECT * INTO STRICT lease FROM control.snapshot_leases WHERE lease_id = NEW.lease_id FOR UPDATE;
    IF lease.archive_pin_protocol IS DISTINCT FROM 1 OR lease.state <> 'active'
        OR lease.lease_revision <> NEW.expected_lease_revision
        OR lease.dataset_id <> NEW.dataset_id OR lease.consumer_id <> NEW.consumer_id THEN
        RAISE EXCEPTION 'pin recovery requires its exact active qualified lease' USING ERRCODE = '23514';
    END IF;
    PERFORM control.require_control_owner(NEW.dataset_id,NEW.recoverer_resource,
        NEW.recoverer_epoch,NEW.recoverer_incarnation);
    NEW.recovered_at_us := (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint;
    RETURN NEW;
END;
$$;
CREATE TRIGGER archive_pin_recovery_guard BEFORE INSERT OR UPDATE OR DELETE ON control.archive_pin_recoveries
    FOR EACH ROW EXECUTE FUNCTION control.guard_archive_pin_recovery();
CREATE TRIGGER archive_pin_recovery_truncate_guard BEFORE TRUNCATE ON control.archive_pin_recoveries
    FOR EACH STATEMENT EXECUTE FUNCTION control.guard_archive_pin_recovery();

CREATE FUNCTION control.complete_archive_pin_recovery() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
DECLARE lease control.snapshot_leases%ROWTYPE;
BEGIN
    SELECT * INTO STRICT lease FROM control.snapshot_leases WHERE lease_id = NEW.lease_id;
    PERFORM control.require_archive_pin_recovery(lease);
    IF lease.state <> 'released' OR lease.release_reason <> 'old_reader_excluded'
        OR lease.released_at_us < NEW.recovered_at_us
        OR EXISTS (SELECT 1 FROM control.retention_claims WHERE lease_id = NEW.lease_id
            AND (state <> 'released' OR release_reason <> 'old_reader_excluded'
                OR released_at_us < NEW.recovered_at_us)) THEN
        RAISE EXCEPTION 'pin recovery and every retained claim must release atomically' USING ERRCODE = '23514';
    END IF;
    RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER archive_pin_recovery_complete AFTER INSERT ON control.archive_pin_recoveries
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION control.complete_archive_pin_recovery();

-- Original-holder semantics remain unchanged. The new branch applies solely to
-- explicit recovery release, never acquisition, renewal or another live grant.
CREATE OR REPLACE FUNCTION control.require_archive_pin_holder(wanted_lease control.snapshot_leases,
    require_live boolean) RETURNS void LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
DECLARE holder control.owner_epochs%ROWTYPE;
DECLARE now_us bigint;
BEGIN
    IF NOT require_live AND EXISTS (SELECT 1 FROM control.archive_pin_recoveries
        WHERE lease_id = wanted_lease.lease_id) THEN
        PERFORM control.require_archive_pin_recovery(wanted_lease);
        RETURN;
    END IF;
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
    now_us := (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint;
    IF require_live AND (holder.lease_expires_at_us <= now_us
        OR wanted_lease.expires_at_us <= now_us
        OR wanted_lease.expires_at_us > holder.lease_expires_at_us) THEN
        RAISE EXCEPTION 'native pin requires unexpired actual holder and lease' USING ERRCODE = '23514';
    END IF;
END;
$$;
REVOKE ALL ON control.archive_pin_recoveries FROM PUBLIC;
REVOKE ALL ON FUNCTION control.require_archive_pin_recovery(control.snapshot_leases),
    control.guard_archive_pin_recovery(),control.complete_archive_pin_recovery() FROM PUBLIC;
