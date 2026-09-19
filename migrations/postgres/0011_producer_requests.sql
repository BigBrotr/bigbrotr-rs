-- A scheduling lease or PG backend loss does not exclude a native writer.
-- This first ledger version is deliberately fresh-only, including retired history.
-- The migrator's exclusive advisory lock serializes old adapter registration;
-- relation locks also protect the preflight from direct concurrent registry DML.
LOCK TABLE control.owner_epochs, control.producer_slots, control.source_streams,
    control.allocation_domains IN SHARE MODE;
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM control.producer_slots)
        OR EXISTS (SELECT 1 FROM control.source_streams)
        OR EXISTS (SELECT 1 FROM control.allocation_domains)
        OR EXISTS (SELECT 1 FROM control.owner_epochs WHERE role = 'ingest') THEN
        RAISE EXCEPTION 'pre-ledger ingest history requires explicit recovery; no adoption'
            USING ERRCODE = '55000';
    END IF;
END $$;

ALTER TABLE control.producer_epochs ADD COLUMN reserved_through_request control.u64
    NOT NULL DEFAULT 0;

-- Event bodies remain in the synchronized co-located spool. These immutable
-- metadata rows are reservations, never native receipts, outcomes or ACKs.
CREATE TABLE control.producer_requests (
    producer_slot_id control.id16 NOT NULL,
    producer_epoch control.u64 NOT NULL CHECK (producer_epoch > 0),
    request_sequence control.u64 NOT NULL CHECK (request_sequence > 0),
    dataset_id control.id16 NOT NULL,
    request_digest control.digest32 NOT NULL,
    input_bytes integer NOT NULL CHECK (input_bytes BETWEEN 1 AND 16777216),
    input_profile smallint NOT NULL CHECK (input_profile = 1),
    manifest_sha256 control.digest32 NOT NULL,
    issuer_resource control.id16 NOT NULL,
    issuer_epoch control.u64 NOT NULL CHECK (issuer_epoch > 0),
    issuer_role text NOT NULL DEFAULT 'control' CHECK (issuer_role = 'control'),
    issuer_incarnation control.id16 NOT NULL,
    reserved_at_us bigint NOT NULL,
    PRIMARY KEY (producer_slot_id, producer_epoch, request_sequence),
    FOREIGN KEY (producer_slot_id, producer_epoch, dataset_id)
        REFERENCES control.producer_epochs(producer_slot_id, producer_epoch, dataset_id),
    FOREIGN KEY (dataset_id, issuer_resource, issuer_epoch, issuer_role, issuer_incarnation)
        REFERENCES control.owner_epochs(dataset_id, resource_id, epoch, role, process_incarnation)
);

CREATE OR REPLACE FUNCTION control.guard_producer_epoch() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
DECLARE prior control.producer_epochs%ROWTYPE;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'producer-epoch history is retained' USING ERRCODE = '23514';
    END IF;
    IF TG_OP = 'INSERT' AND NEW.reserved_through_request <> 0 THEN
        RAISE EXCEPTION 'new producer epoch starts with zero reserved requests' USING ERRCODE = '23514';
    END IF;
    IF TG_OP = 'UPDATE' THEN
        IF NEW.reserved_through_request <> OLD.reserved_through_request THEN
            -- Only the AFTER INSERT reservation trigger has a new retained row.
            -- Compare the complete row except this one monotone counter.
            IF (to_jsonb(NEW) - 'reserved_through_request') IS DISTINCT FROM
                   (to_jsonb(OLD) - 'reserved_through_request')
                OR OLD.state <> 'open'
                OR NEW.reserved_through_request <> OLD.reserved_through_request + 1
                OR NOT EXISTS (SELECT 1 FROM control.producer_requests
                    WHERE producer_slot_id = NEW.producer_slot_id
                        AND producer_epoch = NEW.producer_epoch
                        AND request_sequence = NEW.reserved_through_request) THEN
                RAISE EXCEPTION 'reservation counter requires its exact new retained input row'
                    USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
        END IF;
        -- Until typed outcome closure is implemented, even a caller-supplied
        -- checkpoint digest cannot abandon any reserved request.
        IF NEW.state = 'retired' AND OLD.reserved_through_request > 0 THEN
            RAISE EXCEPTION 'reserved producer requests require explicit outcome closure'
                USING ERRCODE = '55000';
        END IF;
    END IF;
    IF TG_OP = 'INSERT' THEN
        PERFORM 1 FROM control.producer_slots WHERE producer_slot_id = NEW.producer_slot_id FOR UPDATE;
        PERFORM 1 FROM control.role_instances
            WHERE role_instance_id = NEW.producer_role_instance_id
                AND dataset_id = NEW.dataset_id AND role = NEW.producer_role
                AND retired_at_us IS NULL FOR SHARE;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'producer epoch requires a live registered producer role'
                USING ERRCODE = '23514';
        END IF;
        SELECT * INTO prior FROM control.producer_epochs
            WHERE producer_slot_id = NEW.producer_slot_id
            ORDER BY producer_epoch DESC LIMIT 1;
        IF NEW.state <> 'open'
            OR (NOT FOUND AND NEW.producer_epoch <> 1)
            OR (FOUND AND (prior.state <> 'retired'
                OR NEW.producer_epoch <> prior.producer_epoch + 1
                OR NEW.dataset_id <> prior.dataset_id)) THEN
            RAISE EXCEPTION 'producer epoch requires the next ordinal and retired predecessor'
                USING ERRCODE = '23514';
        END IF;
    ELSE
        IF ROW(OLD.producer_slot_id, OLD.producer_epoch, OLD.dataset_id,
                OLD.producer_role_instance_id, OLD.producer_role, OLD.producer_incarnation,
                OLD.owner_resource_id, OLD.owner_epoch, OLD.owner_role,
                OLD.owner_incarnation, OLD.outstanding_request_limit, OLD.opened_at_us)
            IS DISTINCT FROM
               ROW(NEW.producer_slot_id, NEW.producer_epoch, NEW.dataset_id,
                NEW.producer_role_instance_id, NEW.producer_role, NEW.producer_incarnation,
                NEW.owner_resource_id, NEW.owner_epoch, NEW.owner_role,
                NEW.owner_incarnation, NEW.outstanding_request_limit, NEW.opened_at_us)
            OR OLD.state = 'retired'
            OR NOT ((OLD.state = 'open' AND NEW.state = 'closing')
                OR (OLD.state = 'closing' AND NEW.state = 'retired')) THEN
            RAISE EXCEPTION 'invalid producer-epoch transition' USING ERRCODE = '23514';
        END IF;
    END IF;
    PERFORM 1 FROM control.owner_epochs
        WHERE dataset_id = NEW.dataset_id AND resource_id = NEW.owner_resource_id
            AND epoch = NEW.owner_epoch AND role = NEW.owner_role
            AND process_incarnation = NEW.owner_incarnation
            AND state IN ('active', 'draining') FOR SHARE;
    IF NOT FOUND OR (TG_OP = 'INSERT' AND NOT EXISTS (
        SELECT 1 FROM control.owner_epochs
        WHERE dataset_id = NEW.dataset_id AND resource_id = NEW.owner_resource_id
            AND epoch = NEW.owner_epoch AND role = NEW.owner_role
            AND process_incarnation = NEW.owner_incarnation AND state = 'active')) THEN
        RAISE EXCEPTION 'producer-epoch mutation requires its exact live ingest owner'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE FUNCTION control.guard_producer_request() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
DECLARE producer control.producer_epochs%ROWTYPE;
        retained_bytes bigint;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'producer request reservations are retained and immutable'
            USING ERRCODE = '23514';
    END IF;
    PERFORM control.require_control_owner(NEW.dataset_id, NEW.issuer_resource,
        NEW.issuer_epoch, NEW.issuer_incarnation);
    -- Match the registry lock order: Control fence, Ingest owner, producer.
    PERFORM 1 FROM control.owner_epochs o JOIN control.producer_epochs p
        ON p.dataset_id = o.dataset_id AND p.owner_resource_id = o.resource_id
            AND p.owner_epoch = o.epoch AND p.owner_incarnation = o.process_incarnation
        WHERE p.producer_slot_id = NEW.producer_slot_id AND p.producer_epoch = NEW.producer_epoch
            AND p.dataset_id = NEW.dataset_id AND o.role = 'ingest' AND o.state = 'active'
        FOR SHARE OF o;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'reservation requires its live ingest owner' USING ERRCODE = '23514';
    END IF;
    SELECT * INTO STRICT producer FROM control.producer_epochs
        WHERE producer_slot_id = NEW.producer_slot_id AND producer_epoch = NEW.producer_epoch
            AND dataset_id = NEW.dataset_id FOR UPDATE;
    IF producer.state <> 'open'
        OR NEW.request_sequence <> producer.reserved_through_request + 1
        OR NEW.request_sequence > LEAST(producer.outstanding_request_limit, 64) THEN
        RAISE EXCEPTION 'reservation requires the next request within its retained window'
            USING ERRCODE = '23514';
    END IF;
    SELECT coalesce(sum(input_bytes), 0) INTO retained_bytes FROM control.producer_requests
        WHERE producer_slot_id = NEW.producer_slot_id AND producer_epoch = NEW.producer_epoch;
    IF retained_bytes + NEW.input_bytes > 33554432 OR EXISTS (
        SELECT 1 FROM control.producer_requests
        WHERE producer_slot_id = NEW.producer_slot_id AND producer_epoch = NEW.producer_epoch
            AND manifest_sha256 <> NEW.manifest_sha256
    ) THEN
        RAISE EXCEPTION 'reservation exceeds retained input bounds or changes native context'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER producer_request_guard BEFORE INSERT OR UPDATE OR DELETE
    ON control.producer_requests FOR EACH ROW EXECUTE FUNCTION control.guard_producer_request();
CREATE TRIGGER producer_request_truncate_guard BEFORE TRUNCATE
    ON control.producer_requests FOR EACH STATEMENT EXECUTE FUNCTION control.guard_producer_request();

CREATE FUNCTION control.advance_producer_reservation() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
BEGIN
    UPDATE control.producer_epochs SET reserved_through_request = NEW.request_sequence
        WHERE producer_slot_id = NEW.producer_slot_id AND producer_epoch = NEW.producer_epoch;
    RETURN NULL;
END;
$$;
CREATE TRIGGER producer_request_advance AFTER INSERT ON control.producer_requests
    FOR EACH ROW EXECUTE FUNCTION control.advance_producer_reservation();
REVOKE ALL ON control.producer_requests FROM PUBLIC;
REVOKE ALL ON FUNCTION control.guard_producer_request() FROM PUBLIC;
REVOKE ALL ON FUNCTION control.advance_producer_reservation() FROM PUBLIC;
