-- Initial bounded import handoff: event-only JSONL batches, not checkpoints.
-- Old reservations remain unbound. They may have native effects even without a
-- PG outcome; this migration does not retrospectively attach them to positions.
CREATE TABLE control.producer_input_bindings (
    job_id control.id16 NOT NULL REFERENCES control.jobs(job_id),
    dataset_id control.id16 NOT NULL REFERENCES control.datasets(dataset_id),
    source_namespace control.digest32 NOT NULL,
    shard_id control.u32 NOT NULL,
    source_bytes control.u64 NOT NULL CHECK (source_bytes > 0),
    source_sha256 control.digest32 NOT NULL,
    parser_profile smallint NOT NULL CHECK (parser_profile = 1), -- nostr-jsonl/1
    batch_first_record control.u64 NOT NULL CHECK (batch_first_record > 0),
    batch_records smallint NOT NULL CHECK (batch_records BETWEEN 1 AND 64),
    obligation_index smallint NOT NULL CHECK (obligation_index BETWEEN 0 AND 63),
    obligation_kind smallint NOT NULL CHECK (obligation_kind = 1), -- event Admit
    logical_record control.u64 NOT NULL CHECK (logical_record > 0),
    byte_start control.u64 NOT NULL,
    byte_end control.u64 NOT NULL,
    record_sha256 control.digest32 NOT NULL,
    producer_slot_id control.id16 NOT NULL,
    producer_epoch control.u64 NOT NULL CHECK (producer_epoch > 0),
    request_sequence control.u64 NOT NULL CHECK (request_sequence > 0),
    request_digest control.digest32 NOT NULL,
    input_bytes integer NOT NULL CHECK (input_bytes BETWEEN 1 AND 16777216),
    input_profile smallint NOT NULL CHECK (input_profile = 1),
    manifest_sha256 control.digest32 NOT NULL,
    issuer_resource control.id16 NOT NULL,
    issuer_epoch control.u64 NOT NULL CHECK (issuer_epoch > 0),
    issuer_incarnation control.id16 NOT NULL,
    PRIMARY KEY (job_id, shard_id, logical_record),
    UNIQUE (producer_slot_id, producer_epoch, request_sequence),
    FOREIGN KEY (producer_slot_id, producer_epoch, request_sequence)
        REFERENCES control.producer_requests(producer_slot_id, producer_epoch, request_sequence)
        DEFERRABLE INITIALLY DEFERRED,
    CHECK (logical_record = batch_first_record + obligation_index),
    CHECK (obligation_index < batch_records),
    CHECK (byte_start < byte_end AND byte_end <= source_bytes),
    CHECK (byte_start >= logical_record - 1),
    CHECK (logical_record <> 1 OR byte_start = 0)
);
CREATE INDEX input_bindings_batch ON control.producer_input_bindings
    (job_id, shard_id, batch_first_record, obligation_index);

CREATE FUNCTION control.guard_input_binding() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
DECLARE job control.jobs%ROWTYPE;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'input bindings are immutable retained recovery obligations'
            USING ERRCODE = '23514';
    END IF;
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'input bindings require READ COMMITTED' USING ERRCODE = '25001';
    END IF;
    PERFORM control.require_control_owner(NEW.dataset_id, NEW.issuer_resource,
        NEW.issuer_epoch, NEW.issuer_incarnation);
    -- Control fence, job, Ingest owner, producer: shared with the typed bridge.
    -- Job serialization bounds all metadata and source/position comparisons below.
    SELECT * INTO STRICT job FROM control.jobs WHERE job_id = NEW.job_id FOR UPDATE;
    IF job.dataset_id <> NEW.dataset_id OR job.kind <> 'import' OR job.state <> 'running'
        OR NOT job.input_complete OR job.owner_resource_id IS DISTINCT FROM NEW.issuer_resource
        OR job.owner_epoch IS DISTINCT FROM NEW.issuer_epoch
        OR job.owner_incarnation IS DISTINCT FROM NEW.issuer_incarnation THEN
        RAISE EXCEPTION 'input binding requires its exact running job owner'
            USING ERRCODE = '23514';
    END IF;
    PERFORM 1 FROM control.owner_epochs o JOIN control.producer_epochs p
        ON p.dataset_id = o.dataset_id AND p.owner_resource_id = o.resource_id
            AND p.owner_epoch = o.epoch AND p.owner_incarnation = o.process_incarnation
        WHERE p.producer_slot_id = NEW.producer_slot_id AND p.producer_epoch = NEW.producer_epoch
            AND p.dataset_id = NEW.dataset_id AND o.role = 'ingest' AND o.state = 'active'
        FOR SHARE OF o;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'input binding requires its live ingest owner' USING ERRCODE = '23514';
    END IF;
    PERFORM 1 FROM control.producer_epochs
        WHERE producer_slot_id = NEW.producer_slot_id AND producer_epoch = NEW.producer_epoch
            AND dataset_id = NEW.dataset_id AND state = 'open' FOR UPDATE;
    IF NOT FOUND OR EXISTS (SELECT 1 FROM control.producer_requests
        WHERE producer_slot_id = NEW.producer_slot_id AND producer_epoch = NEW.producer_epoch
            AND request_sequence = NEW.request_sequence) THEN
        RAISE EXCEPTION 'input binding cannot attach an already reserved request'
            USING ERRCODE = '23514';
    END IF;
    IF (SELECT count(*) FROM control.producer_input_bindings WHERE job_id = NEW.job_id) >= 64 THEN
        RAISE EXCEPTION 'initial import binding ledger is full; explicit closure required'
            USING ERRCODE = '23514';
    END IF;
    IF EXISTS (SELECT 1 FROM control.producer_input_bindings b WHERE b.job_id = NEW.job_id
        AND (b.source_namespace <> NEW.source_namespace
            OR (b.shard_id = NEW.shard_id AND
                ROW(b.source_bytes, b.source_sha256, b.parser_profile)
                IS DISTINCT FROM ROW(NEW.source_bytes, NEW.source_sha256, NEW.parser_profile)))) THEN
        RAISE EXCEPTION 'job source namespace and shard identity cannot change'
            USING ERRCODE = '23514';
    END IF;
    IF EXISTS (SELECT 1 FROM control.producer_input_bindings b
        WHERE b.job_id = NEW.job_id AND b.shard_id = NEW.shard_id
            AND ((b.logical_record < NEW.logical_record AND b.byte_end > NEW.byte_start)
                OR (b.logical_record > NEW.logical_record AND b.byte_start < NEW.byte_end)
                OR (b.logical_record = NEW.logical_record - 1 AND b.byte_end <> NEW.byte_start)
                OR (b.logical_record = NEW.logical_record + 1 AND b.byte_start <> NEW.byte_end))) THEN
        RAISE EXCEPTION 'logical record order contradicts source byte order' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER input_binding_guard BEFORE INSERT OR UPDATE OR DELETE ON control.producer_input_bindings
    FOR EACH ROW EXECUTE FUNCTION control.guard_input_binding();
CREATE TRIGGER input_binding_truncate_guard BEFORE TRUNCATE ON control.producer_input_bindings
    FOR EACH STATEMENT EXECUTE FUNCTION control.guard_input_binding();

CREATE FUNCTION control.require_complete_input_batch() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
BEGIN
    -- At most 64 rows per job, including all batches, shards and producer epochs.
    -- A complete batch and every original reservation become visible together.
    IF (SELECT count(*) FROM control.producer_input_bindings b
        WHERE b.job_id = NEW.job_id AND b.shard_id = NEW.shard_id
            AND b.batch_first_record = NEW.batch_first_record) <> NEW.batch_records
        OR EXISTS (SELECT 1 FROM control.producer_input_bindings b
            WHERE b.job_id = NEW.job_id AND b.shard_id = NEW.shard_id
                AND b.batch_first_record = NEW.batch_first_record
                AND ROW(b.batch_records, b.producer_slot_id, b.producer_epoch, b.request_sequence - b.obligation_index,
                    b.manifest_sha256, b.issuer_resource, b.issuer_epoch, b.issuer_incarnation)
                    IS DISTINCT FROM ROW(NEW.batch_records, NEW.producer_slot_id, NEW.producer_epoch, NEW.request_sequence - NEW.obligation_index,
                    NEW.manifest_sha256, NEW.issuer_resource, NEW.issuer_epoch, NEW.issuer_incarnation))
        OR EXISTS (SELECT 1 FROM control.producer_input_bindings b
            JOIN control.producer_input_bindings previous ON previous.job_id = b.job_id
                AND previous.shard_id = b.shard_id AND previous.logical_record = b.logical_record - 1
            WHERE b.job_id = NEW.job_id AND b.shard_id = NEW.shard_id
                AND b.batch_first_record = NEW.batch_first_record AND b.obligation_index > 0
                AND previous.byte_end <> b.byte_start)
        OR EXISTS (SELECT 1 FROM control.producer_input_bindings b
            LEFT JOIN control.producer_requests r USING (producer_slot_id, producer_epoch, request_sequence)
            WHERE b.job_id = NEW.job_id AND b.shard_id = NEW.shard_id
                AND b.batch_first_record = NEW.batch_first_record
                AND (r.request_sequence IS NULL OR
                    ROW(b.dataset_id, b.request_digest, b.input_bytes, b.input_profile, b.manifest_sha256,
                        b.issuer_resource, b.issuer_epoch, b.issuer_incarnation)
                    IS DISTINCT FROM ROW(r.dataset_id, r.request_digest, r.input_bytes, r.input_profile, r.manifest_sha256,
                        r.issuer_resource, r.issuer_epoch, r.issuer_incarnation))) THEN
        RAISE EXCEPTION 'incomplete or inconsistent input batch/reservation binding'
            USING ERRCODE = '23514';
    END IF;
    RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER input_batch_complete AFTER INSERT ON control.producer_input_bindings
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION control.require_complete_input_batch();

REVOKE ALL ON control.producer_input_bindings FROM PUBLIC;
REVOKE ALL ON FUNCTION control.guard_input_binding() FROM PUBLIC;
REVOKE ALL ON FUNCTION control.require_complete_input_batch() FROM PUBLIC;
