-- Reserve completion space before native dispatch. Existing inputs are never
-- trimmed, adopted as outcomes or silently made undrainable by this extension.
LOCK TABLE control.producer_epochs, control.producer_requests IN SHARE MODE;
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM control.producer_requests
        GROUP BY producer_slot_id, producer_epoch
        HAVING sum(input_bytes::bigint + 65536) > 33554432) THEN
        RAISE EXCEPTION 'retained producer inputs lack outcome headroom; explicit recovery required'
            USING ERRCODE = '55000';
    END IF;
END $$;

CREATE FUNCTION control.reserve_producer_outcome_capacity() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
DECLARE retained_bytes bigint;
BEGIN
    -- Runs after producer_request_guard (trigger name order); independently lock
    -- the same serialization row before the fresh bounded aggregate as well.
    PERFORM 1 FROM control.producer_epochs
        WHERE producer_slot_id = NEW.producer_slot_id AND producer_epoch = NEW.producer_epoch
        FOR UPDATE;
    SELECT coalesce(sum(input_bytes::bigint + 65536), 0) INTO retained_bytes
        FROM control.producer_requests
        WHERE producer_slot_id = NEW.producer_slot_id AND producer_epoch = NEW.producer_epoch;
    IF retained_bytes + NEW.input_bytes + 65536 > 33554432 THEN
        RAISE EXCEPTION 'producer reservation must include outcome completion capacity'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER producer_request_outcome_capacity BEFORE INSERT ON control.producer_requests
    FOR EACH ROW EXECUTE FUNCTION control.reserve_producer_outcome_capacity();

-- Complete canonical operational receipts/rejections, never scientific bodies.
-- The connected native owner attests the bytes; SQL is not native commit authority.
CREATE TABLE control.producer_outcomes (
    producer_slot_id control.id16 NOT NULL,
    producer_epoch control.u64 NOT NULL CHECK (producer_epoch > 0),
    request_sequence control.u64 NOT NULL CHECK (request_sequence > 0),
    dataset_id control.id16 NOT NULL,
    request_digest control.digest32 NOT NULL,
    manifest_sha256 control.digest32 NOT NULL,
    outcome_kind smallint NOT NULL CHECK (outcome_kind IN (1, 2, 3)),
    policy_epoch control.u64 NOT NULL,
    outcome_bytes bytea NOT NULL CHECK (octet_length(outcome_bytes) BETWEEN 1 AND 65536),
    outcome_sha256 control.digest32 NOT NULL CHECK (outcome_sha256 = sha256(outcome_bytes)),
    recorder_resource control.id16 NOT NULL,
    recorder_epoch control.u64 NOT NULL CHECK (recorder_epoch > 0),
    recorder_role text NOT NULL DEFAULT 'control' CHECK (recorder_role = 'control'),
    recorder_incarnation control.id16 NOT NULL,
    recorded_at_us bigint NOT NULL,
    PRIMARY KEY (producer_slot_id, producer_epoch, request_sequence),
    FOREIGN KEY (producer_slot_id, producer_epoch, request_sequence)
        REFERENCES control.producer_requests(producer_slot_id, producer_epoch, request_sequence),
    FOREIGN KEY (dataset_id, recorder_resource, recorder_epoch, recorder_role, recorder_incarnation)
        REFERENCES control.owner_epochs(dataset_id, resource_id, epoch, role, process_incarnation)
);

CREATE FUNCTION control.guard_producer_outcome() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
DECLARE producer control.producer_epochs%ROWTYPE;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'producer outcomes are retained and immutable' USING ERRCODE = '23514';
    END IF;
    PERFORM control.require_control_owner(NEW.dataset_id, NEW.recorder_resource,
        NEW.recorder_epoch, NEW.recorder_incarnation);
    -- Same lock order as reservation/connected registry observation.
    PERFORM 1 FROM control.owner_epochs o JOIN control.producer_epochs p
        ON p.dataset_id = o.dataset_id AND p.owner_resource_id = o.resource_id
            AND p.owner_epoch = o.epoch AND p.owner_incarnation = o.process_incarnation
        WHERE p.producer_slot_id = NEW.producer_slot_id AND p.producer_epoch = NEW.producer_epoch
            AND p.dataset_id = NEW.dataset_id AND o.role = 'ingest'
            AND o.state IN ('active', 'draining') FOR SHARE OF o;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'outcome copy requires its live ingest owner' USING ERRCODE = '23514';
    END IF;
    SELECT * INTO STRICT producer FROM control.producer_epochs
        WHERE producer_slot_id = NEW.producer_slot_id AND producer_epoch = NEW.producer_epoch
            AND dataset_id = NEW.dataset_id FOR UPDATE;
    IF producer.state = 'retired' OR NOT EXISTS (
        SELECT 1 FROM control.producer_requests
        WHERE producer_slot_id = NEW.producer_slot_id AND producer_epoch = NEW.producer_epoch
            AND request_sequence = NEW.request_sequence AND dataset_id = NEW.dataset_id
            AND request_digest = NEW.request_digest AND manifest_sha256 = NEW.manifest_sha256
    ) THEN
        RAISE EXCEPTION 'outcome copy requires its exact retained reservation'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER producer_outcome_guard BEFORE INSERT OR UPDATE OR DELETE ON control.producer_outcomes
    FOR EACH ROW EXECUTE FUNCTION control.guard_producer_outcome();
CREATE TRIGGER producer_outcome_truncate_guard BEFORE TRUNCATE ON control.producer_outcomes
    FOR EACH STATEMENT EXECUTE FUNCTION control.guard_producer_outcome();
REVOKE ALL ON control.producer_outcomes FROM PUBLIC;
REVOKE ALL ON FUNCTION control.guard_producer_outcome() FROM PUBLIC;
REVOKE ALL ON FUNCTION control.reserve_producer_outcome_capacity() FROM PUBLIC;
