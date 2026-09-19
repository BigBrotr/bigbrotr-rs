-- Complete event-only logical records, distinct from producer mutation ACKs.
-- Old outcome rows are retained without inferred native closure or backfill.
LOCK TABLE control.jobs IN SHARE MODE;
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM control.jobs WHERE kind = 'import' AND checkpoint_revision <> 0) THEN
        RAISE EXCEPTION 'old import checkpoint lacks typed closure; explicit recovery required'
            USING ERRCODE = '55000';
    END IF;
END $$;

CREATE TABLE control.import_record_closures (
    job_id control.id16 NOT NULL,
    dataset_id control.id16 NOT NULL REFERENCES control.datasets(dataset_id),
    shard_id control.u32 NOT NULL,
    logical_record control.u64 NOT NULL CHECK (logical_record > 0),
    producer_slot_id control.id16 NOT NULL,
    producer_epoch control.u64 NOT NULL CHECK (producer_epoch > 0),
    request_sequence control.u64 NOT NULL CHECK (request_sequence > 0),
    outcome_sha256 control.digest32 NOT NULL,
    recorder_resource control.id16 NOT NULL,
    recorder_epoch control.u64 NOT NULL CHECK (recorder_epoch > 0),
    recorder_incarnation control.id16 NOT NULL,
    recorded_at_us bigint NOT NULL,
    PRIMARY KEY (job_id, shard_id, logical_record),
    UNIQUE (producer_slot_id, producer_epoch, request_sequence),
    FOREIGN KEY (job_id, shard_id, logical_record)
        REFERENCES control.producer_input_bindings(job_id, shard_id, logical_record),
    FOREIGN KEY (producer_slot_id, producer_epoch, request_sequence)
        REFERENCES control.producer_outcomes(producer_slot_id, producer_epoch, request_sequence)
);

-- Absence is prefix zero. Both the original source identity/byte boundary and
-- complete receipt dependencies remain reachable through immutable membership.
CREATE TABLE control.import_checkpoints (
    job_id control.id16 NOT NULL,
    shard_id control.u32 NOT NULL,
    revision control.u64 NOT NULL CHECK (revision > 0),
    complete_through_record control.u64 NOT NULL CHECK (complete_through_record > 0),
    byte_offset control.u64 NOT NULL CHECK (byte_offset > 0),
    PRIMARY KEY (job_id, shard_id),
    FOREIGN KEY (job_id, shard_id, complete_through_record)
        REFERENCES control.import_record_closures(job_id, shard_id, logical_record)
);

CREATE FUNCTION control.guard_import_record_closure() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
DECLARE job control.jobs%ROWTYPE;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'import closure membership is retained and immutable' USING ERRCODE = '23514';
    END IF;
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'import closure requires READ COMMITTED' USING ERRCODE = '25001';
    END IF;
    PERFORM control.require_control_owner(NEW.dataset_id, NEW.recorder_resource,
        NEW.recorder_epoch, NEW.recorder_incarnation);
    SELECT * INTO STRICT job FROM control.jobs WHERE job_id = NEW.job_id FOR UPDATE;
    IF job.dataset_id <> NEW.dataset_id OR job.kind <> 'import' OR job.state <> 'running'
        OR NOT job.input_complete OR job.owner_resource_id IS DISTINCT FROM NEW.recorder_resource
        OR job.owner_epoch IS DISTINCT FROM NEW.recorder_epoch
        OR job.owner_incarnation IS DISTINCT FROM NEW.recorder_incarnation THEN
        RAISE EXCEPTION 'import closure requires its exact running job owner' USING ERRCODE = '23514';
    END IF;
    PERFORM 1 FROM control.owner_epochs o JOIN control.producer_epochs p
        ON p.dataset_id = o.dataset_id AND p.owner_resource_id = o.resource_id
            AND p.owner_epoch = o.epoch AND p.owner_incarnation = o.process_incarnation
        WHERE p.producer_slot_id = NEW.producer_slot_id AND p.producer_epoch = NEW.producer_epoch
            AND p.dataset_id = NEW.dataset_id AND o.role = 'ingest'
            AND o.state IN ('active', 'draining') FOR SHARE OF o;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'import closure requires its live ingest owner' USING ERRCODE = '23514';
    END IF;
    PERFORM 1 FROM control.producer_epochs WHERE producer_slot_id = NEW.producer_slot_id
        AND producer_epoch = NEW.producer_epoch AND dataset_id = NEW.dataset_id
        AND state <> 'retired' FOR UPDATE;
    IF NOT FOUND OR NOT EXISTS (
        SELECT 1 FROM control.producer_input_bindings b
        JOIN control.producer_outcomes o USING (producer_slot_id, producer_epoch, request_sequence)
        WHERE b.job_id = NEW.job_id AND b.shard_id = NEW.shard_id AND b.logical_record = NEW.logical_record
            AND b.dataset_id = NEW.dataset_id AND b.parser_profile = 1 AND b.obligation_kind = 1
            AND b.producer_slot_id = NEW.producer_slot_id AND b.producer_epoch = NEW.producer_epoch
            AND b.request_sequence = NEW.request_sequence AND o.dataset_id = b.dataset_id
            AND o.request_digest = b.request_digest AND o.manifest_sha256 = b.manifest_sha256
            AND o.outcome_kind IN (1, 2) AND o.outcome_sha256 = NEW.outcome_sha256
    ) THEN
        RAISE EXCEPTION 'import closure requires its original successful receipt binding' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER import_closure_guard BEFORE INSERT OR UPDATE OR DELETE ON control.import_record_closures
    FOR EACH ROW EXECUTE FUNCTION control.guard_import_record_closure();
CREATE TRIGGER import_closure_truncate_guard BEFORE TRUNCATE ON control.import_record_closures
    FOR EACH STATEMENT EXECUTE FUNCTION control.guard_import_record_closure();

CREATE FUNCTION control.guard_import_checkpoint() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
DECLARE job control.jobs%ROWTYPE;
BEGIN
    IF TG_OP IN ('DELETE', 'TRUNCATE') THEN
        RAISE EXCEPTION 'import checkpoints retain their dependency closure' USING ERRCODE = '23514';
    END IF;
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'import checkpoint requires READ COMMITTED' USING ERRCODE = '25001';
    END IF;
    SELECT * INTO STRICT job FROM control.jobs WHERE job_id = NEW.job_id;
    PERFORM control.require_control_owner(job.dataset_id, job.owner_resource_id,
        job.owner_epoch, job.owner_incarnation);
    SELECT * INTO STRICT job FROM control.jobs WHERE job_id = NEW.job_id FOR UPDATE;
    IF job.kind <> 'import' OR job.state <> 'running' OR NOT job.input_complete
        OR (TG_OP = 'INSERT' AND NEW.revision <> 1)
        OR (TG_OP = 'UPDATE' AND
            (ROW(NEW.job_id, NEW.shard_id) IS DISTINCT FROM ROW(OLD.job_id, OLD.shard_id)
                OR NEW.revision <> OLD.revision + 1
                OR NEW.complete_through_record <= OLD.complete_through_record
                OR NEW.byte_offset <= OLD.byte_offset))
        OR (SELECT count(*) FROM control.import_record_closures
            WHERE job_id = NEW.job_id AND shard_id = NEW.shard_id
                AND logical_record <= NEW.complete_through_record) <> NEW.complete_through_record
        OR NOT EXISTS (SELECT 1 FROM control.producer_input_bindings
            WHERE job_id = NEW.job_id AND shard_id = NEW.shard_id
                AND logical_record = NEW.complete_through_record AND byte_end = NEW.byte_offset) THEN
        RAISE EXCEPTION 'import checkpoint requires its complete next logical prefix' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER import_checkpoint_guard BEFORE INSERT OR UPDATE OR DELETE ON control.import_checkpoints
    FOR EACH ROW EXECUTE FUNCTION control.guard_import_checkpoint();
CREATE TRIGGER import_checkpoint_truncate_guard BEFORE TRUNCATE ON control.import_checkpoints
    FOR EACH STATEMENT EXECUTE FUNCTION control.guard_import_checkpoint();

CREATE FUNCTION control.advance_import_checkpoint() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
DECLARE position record; complete_record numeric := 0; complete_byte numeric := 0;
BEGIN
    -- The job lock spans all shards; at most 64 retained bindings exist per job.
    -- VOLATILE AFTER visibility includes this closure and every row in its statement.
    FOR position IN SELECT b.logical_record, b.byte_end FROM control.import_record_closures c
        JOIN control.producer_input_bindings b USING (job_id, shard_id, logical_record)
        WHERE c.job_id = NEW.job_id AND c.shard_id = NEW.shard_id ORDER BY c.logical_record LOOP
        EXIT WHEN position.logical_record <> complete_record + 1;
        complete_record := position.logical_record;
        complete_byte := position.byte_end;
    END LOOP;
    IF complete_record > 0 THEN
        INSERT INTO control.import_checkpoints(job_id, shard_id, revision, complete_through_record, byte_offset)
            VALUES (NEW.job_id, NEW.shard_id, 1, complete_record, complete_byte)
            ON CONFLICT (job_id, shard_id) DO UPDATE SET revision = control.import_checkpoints.revision + 1,
                complete_through_record = EXCLUDED.complete_through_record, byte_offset = EXCLUDED.byte_offset
            WHERE control.import_checkpoints.complete_through_record < EXCLUDED.complete_through_record;
    END IF;
    RETURN NULL;
END;
$$;
CREATE TRIGGER import_closure_advance AFTER INSERT ON control.import_record_closures
    FOR EACH ROW EXECUTE FUNCTION control.advance_import_checkpoint();

CREATE FUNCTION control.advance_import_job_revision() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
BEGIN
    UPDATE control.jobs SET job_revision = job_revision + 1, checkpoint_revision = checkpoint_revision + 1
        WHERE job_id = NEW.job_id;
    RETURN NULL;
END;
$$;
CREATE TRIGGER import_checkpoint_job_revision AFTER INSERT OR UPDATE ON control.import_checkpoints
    FOR EACH ROW EXECUTE FUNCTION control.advance_import_job_revision();

CREATE FUNCTION control.guard_import_job_revision() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
BEGIN
    IF NEW.kind = 'import' AND NEW.checkpoint_revision <> OLD.checkpoint_revision THEN
        IF NEW.checkpoint_revision <> OLD.checkpoint_revision + 1
            OR NEW.checkpoint_revision <> (SELECT coalesce(sum(revision), 0)
                FROM control.import_checkpoints WHERE job_id = NEW.job_id) THEN
            RAISE EXCEPTION 'import job checkpoint revision requires its advancing shard head'
                USING ERRCODE = '23514';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER import_job_checkpoint_guard BEFORE UPDATE ON control.jobs
    FOR EACH ROW EXECUTE FUNCTION control.guard_import_job_revision();

REVOKE ALL ON control.import_record_closures, control.import_checkpoints FROM PUBLIC;
REVOKE ALL ON FUNCTION control.guard_import_record_closure(), control.guard_import_checkpoint(),
    control.advance_import_checkpoint(), control.advance_import_job_revision(),
    control.guard_import_job_revision() FROM PUBLIC;
