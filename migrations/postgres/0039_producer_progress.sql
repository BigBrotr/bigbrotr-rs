-- Typed cumulative responsibility. No native ACK, credit, reclamation or job
-- completion is an effect of this migration. Legacy opaque heads remain
-- readable as legacy; they cannot be upgraded by supplying a new digest.
ALTER TABLE control.producer_checkpoints
    ADD COLUMN progress_format smallint CHECK (progress_format = 1),
    ADD COLUMN manifest_sha256 control.digest32,
    ADD COLUMN progress_jobs integer,
    ADD COLUMN dependency_count integer,
    ADD COLUMN issuer_resource control.id16,
    ADD COLUMN issuer_epoch control.u64,
    ADD COLUMN issuer_incarnation control.id16,
    ADD CONSTRAINT producer_progress_shape CHECK (
        (progress_format IS NULL AND num_nonnulls(manifest_sha256, progress_jobs,
            dependency_count, issuer_resource, issuer_epoch, issuer_incarnation) = 0)
        OR (progress_format IS NOT DISTINCT FROM 1 AND num_nonnulls(manifest_sha256, progress_jobs,
            dependency_count, issuer_resource, issuer_epoch, issuer_incarnation) = 6
            AND progress_jobs BETWEEN 0 AND 64
            AND dependency_count BETWEEN 0 AND 16384 AND issuer_epoch > 0)
    );

-- One local prefix per job/producer, not a relay cursor or per-delivery ledger.
-- Ordinals enumerate only retained correlated EVENTs in one actual REQ.
CREATE TABLE control.acquisition_progress (
    dataset_id control.id16 NOT NULL,
    producer_slot_id control.id16 NOT NULL,
    producer_epoch control.u64 NOT NULL CHECK (producer_epoch > 0),
    job_id control.id16 NOT NULL,
    scope_id control.id16 NOT NULL,
    progress_revision control.u64 NOT NULL CHECK (progress_revision > 0),
    request_id bytea NOT NULL CHECK (octet_length(request_id) = 48
        AND encode(request_id, 'escape') ~ '^[0-9a-f]{48}$'),
    event_ordinal smallint NOT NULL CHECK (event_ordinal BETWEEN 1 AND 3),
    wire_sequence control.u64 NOT NULL CHECK (wire_sequence > 0),
    input_sha256 control.digest32 NOT NULL,
    intent_digest control.digest32 NOT NULL,
    classification smallint NOT NULL CHECK (classification BETWEEN 2 AND 6),
    mutation_request control.u64,
    classified_events control.u64 NOT NULL CHECK (classified_events > 0),
    successful_events control.u64 NOT NULL,
    rejected_events control.u64 NOT NULL,
    observed_at_us bigint NOT NULL,
    clock_resolution_us control.u64 NOT NULL CHECK (clock_resolution_us > 0),
    clock_anomaly_flags control.u32 NOT NULL CHECK (clock_anomaly_flags IN (0,1,4,5)),
    CHECK ((clock_anomaly_flags::bigint & 4 <> 0) = (clock_resolution_us > 1)),
    PRIMARY KEY (producer_slot_id, producer_epoch, job_id),
    FOREIGN KEY (producer_slot_id, producer_epoch, dataset_id)
        REFERENCES control.producer_epochs(producer_slot_id, producer_epoch, dataset_id),
    FOREIGN KEY (dataset_id, job_id, scope_id)
        REFERENCES control.acquisition_jobs(dataset_id, job_id, scope_id),
    CHECK (classified_events = successful_events + rejected_events),
    CHECK (progress_revision = classified_events),
    CHECK ((classification IN (2,3,4) AND mutation_request IS NOT NULL AND mutation_request > 0)
        OR (classification IN (5,6) AND mutation_request IS NULL))
);

-- A cumulative deduplicated closure: complete identities/digests, never one
-- proof row for every repeated delivery. All detail remains immutable/no-GC.
CREATE TABLE control.producer_checkpoint_dependencies (
    dataset_id control.id16 NOT NULL,
    producer_slot_id control.id16 NOT NULL,
    producer_epoch control.u64 NOT NULL CHECK (producer_epoch > 0),
    source_shard control.u32 NOT NULL,
    source_epoch control.u64 NOT NULL CHECK (source_epoch > 0),
    source_sequence control.u64 NOT NULL CHECK (source_sequence > 0),
    payload_digest control.digest32 NOT NULL,
    PRIMARY KEY (producer_slot_id, producer_epoch, source_shard, source_epoch, source_sequence),
    FOREIGN KEY (producer_slot_id, producer_epoch, dataset_id)
        REFERENCES control.producer_epochs(producer_slot_id, producer_epoch, dataset_id)
);
CREATE TRIGGER producer_checkpoint_dependencies_immutable BEFORE UPDATE OR DELETE
    ON control.producer_checkpoint_dependencies FOR EACH ROW
    EXECUTE FUNCTION control.reject_history_mutation();
CREATE TRIGGER producer_checkpoint_dependencies_no_truncate BEFORE TRUNCATE
    ON control.producer_checkpoint_dependencies FOR EACH STATEMENT
    EXECUTE FUNCTION control.reject_history_mutation();
CREATE TRIGGER acquisition_progress_no_truncate BEFORE TRUNCATE
    ON control.acquisition_progress FOR EACH STATEMENT
    EXECUTE FUNCTION control.reject_history_mutation();

ALTER TABLE control.producer_requests
    ADD COLUMN progress_request_id bytea,
    ADD COLUMN progress_event_ordinal smallint,
    ADD COLUMN progress_wire_sequence control.u64,
    ADD COLUMN progress_input_sha256 control.digest32,
    ADD CONSTRAINT producer_request_progress_shape CHECK (
        num_nonnulls(progress_request_id, progress_event_ordinal,
            progress_wire_sequence, progress_input_sha256) = 0
        OR (num_nonnulls(progress_request_id, progress_event_ordinal,
            progress_wire_sequence, progress_input_sha256) = 4
            AND acquisition_job_id IS NOT NULL
            AND octet_length(progress_request_id) = 48
            AND encode(progress_request_id, 'escape') ~ '^[0-9a-f]{48}$'
            AND progress_event_ordinal BETWEEN 1 AND 3 AND progress_wire_sequence > 0)
    );

CREATE FUNCTION control.guard_acquisition_progress() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, control AS $$
DECLARE producer control.producer_epochs%ROWTYPE; source control.acquisition_scopes%ROWTYPE;
        jobs integer; dependencies integer;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'classified acquisition progress is retained' USING ERRCODE='23514';
    END IF;
    -- Preserve Control/dataset/actor/job before producer lock order.
    SELECT * INTO STRICT source FROM control.acquisition_scopes
        WHERE dataset_id=NEW.dataset_id AND scope_id=NEW.scope_id;
    PERFORM control.require_acquisition_execution(NEW.dataset_id,NEW.job_id,NEW.scope_id,
        source.endpoint_id,source.access_scope_id);
    SELECT * INTO STRICT producer FROM control.producer_epochs
        WHERE dataset_id=NEW.dataset_id AND producer_slot_id=NEW.producer_slot_id
            AND producer_epoch=NEW.producer_epoch FOR UPDATE;
    IF producer.state <> 'open' THEN
        RAISE EXCEPTION 'new classified progress requires the open producer' USING ERRCODE='23514';
    END IF;
    IF TG_OP = 'INSERT' THEN
        IF NEW.progress_revision <> 1 OR NEW.event_ordinal <> 1
            OR NEW.successful_events <> CASE WHEN NEW.classification IN (2,3,5) THEN 1 ELSE 0 END
            OR NEW.rejected_events <> CASE WHEN NEW.classification IN (4,6) THEN 1 ELSE 0 END THEN
            RAISE EXCEPTION 'classified progress starts at the first retained event' USING ERRCODE='23514';
        END IF;
    ELSE
        IF ROW(NEW.dataset_id,NEW.producer_slot_id,NEW.producer_epoch,NEW.job_id,NEW.scope_id)
            IS DISTINCT FROM ROW(OLD.dataset_id,OLD.producer_slot_id,OLD.producer_epoch,OLD.job_id,OLD.scope_id)
            OR NEW.progress_revision <> OLD.progress_revision + 1
            OR NEW.classified_events <> OLD.classified_events + 1
            OR NEW.successful_events <> OLD.successful_events + CASE WHEN NEW.classification IN (2,3,5) THEN 1 ELSE 0 END
            OR NEW.rejected_events <> OLD.rejected_events + CASE WHEN NEW.classification IN (4,6) THEN 1 ELSE 0 END
            OR (NEW.request_id=OLD.request_id AND
                (NEW.event_ordinal<>OLD.event_ordinal+1 OR NEW.wire_sequence<=OLD.wire_sequence))
            OR (NEW.request_id<>OLD.request_id AND NEW.event_ordinal<>1) THEN
            RAISE EXCEPTION 'classified progress cannot skip or replace a prefix' USING ERRCODE='23514';
        END IF;
    END IF;
    SELECT count(*) INTO jobs FROM (SELECT 1 FROM control.acquisition_progress
        WHERE producer_slot_id=NEW.producer_slot_id AND producer_epoch=NEW.producer_epoch LIMIT 65) b;
    SELECT count(*) INTO dependencies FROM (SELECT 1 FROM control.producer_checkpoint_dependencies
        WHERE producer_slot_id=NEW.producer_slot_id AND producer_epoch=NEW.producer_epoch LIMIT 16385) b;
    PERFORM control.require_producer_progress_capacity(NEW.producer_slot_id,NEW.producer_epoch,
        0,jobs+CASE WHEN TG_OP='INSERT' THEN 1 ELSE 0 END,dependencies);
    IF NEW.mutation_request IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM control.producer_requests r JOIN control.producer_outcomes o
            USING (producer_slot_id, producer_epoch, request_sequence)
        WHERE r.dataset_id=NEW.dataset_id AND r.producer_slot_id=NEW.producer_slot_id
            AND r.producer_epoch=NEW.producer_epoch AND r.request_sequence=NEW.mutation_request
            AND r.acquisition_job_id=NEW.job_id AND r.acquisition_scope_id=NEW.scope_id
            AND r.progress_request_id=NEW.request_id
            AND r.progress_event_ordinal=NEW.event_ordinal
            AND r.progress_wire_sequence=NEW.wire_sequence
            AND r.progress_input_sha256=NEW.input_sha256
            AND o.request_digest=NEW.intent_digest
            AND o.outcome_kind=NEW.classification-1
    ) THEN
        RAISE EXCEPTION 'classified mutation requires its original reservation and native outcome'
            USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER acquisition_progress_guard BEFORE INSERT OR UPDATE OR DELETE
    ON control.acquisition_progress FOR EACH ROW
    EXECUTE FUNCTION control.guard_acquisition_progress();

-- This capacity is retained across all history. Native ACK will not release it.
-- Values bound typed logical payload/SQL metadata, not PG physical pages or RSS.
CREATE FUNCTION control.require_producer_progress_capacity(
    wanted_slot bytea, wanted_epoch numeric, additional_input bigint,
    wanted_jobs integer, wanted_dependencies integer
) RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE retained bigint;
BEGIN
    IF wanted_jobs NOT BETWEEN 0 AND 64 OR wanted_dependencies NOT BETWEEN 0 AND 16384
        OR additional_input < 0 THEN
        RAISE EXCEPTION 'producer progress exceeds its finite retained profile' USING ERRCODE='23514';
    END IF;
    SELECT coalesce(sum(input_bytes::bigint+65536),0) INTO retained
        FROM control.producer_requests
        WHERE producer_slot_id=wanted_slot AND producer_epoch=wanted_epoch;
    IF retained + additional_input + 1024 + 1024::bigint*wanted_jobs
            + 256::bigint*wanted_dependencies > 33554432 THEN
        RAISE EXCEPTION 'producer progress and input/completion exceed retained capacity' USING ERRCODE='23514';
    END IF;
END $$;

CREATE OR REPLACE FUNCTION control.guard_producer_checkpoint() RETURNS trigger LANGUAGE plpgsql
SET search_path=pg_catalog,control AS $$
DECLARE producer control.producer_epochs%ROWTYPE;
        jobs integer; dependencies integer;
BEGIN
    IF TG_OP='DELETE' THEN
        RAISE EXCEPTION 'producer checkpoints are retained through epoch retirement' USING ERRCODE='23514';
    END IF;
    SELECT * INTO STRICT producer FROM control.producer_epochs
        WHERE dataset_id=NEW.dataset_id AND producer_slot_id=NEW.producer_slot_id
            AND producer_epoch=NEW.producer_epoch FOR UPDATE;
    IF producer.state='retired' THEN
        RAISE EXCEPTION 'retired producer cannot advance progress' USING ERRCODE='23514';
    END IF;
    IF TG_OP='INSERT' THEN
        IF NEW.checkpoint_revision<>1 THEN
            RAISE EXCEPTION 'first checkpoint revision is one' USING ERRCODE='23514';
        END IF;
    ELSIF ROW(OLD.producer_slot_id,OLD.producer_epoch,OLD.dataset_id,OLD.progress_format,OLD.manifest_sha256)
        IS DISTINCT FROM ROW(NEW.producer_slot_id,NEW.producer_epoch,NEW.dataset_id,NEW.progress_format,NEW.manifest_sha256)
        OR NEW.checkpoint_revision<>OLD.checkpoint_revision+1
        OR NEW.complete_through_request<OLD.complete_through_request
        OR NEW.committed_at_us<OLD.committed_at_us
        OR (NEW.progress_format IS NULL AND NEW.complete_through_request=OLD.complete_through_request) THEN
        RAISE EXCEPTION 'checkpoint CAS cannot replace context or regress cumulative closure'
            USING ERRCODE='23514';
    END IF;
    IF NEW.progress_format IS NULL THEN RETURN NEW; END IF;
    PERFORM control.require_control_owner(NEW.dataset_id,NEW.issuer_resource,
        NEW.issuer_epoch,NEW.issuer_incarnation);
    IF NEW.complete_through_request>producer.reserved_through_request OR NEW.complete_through_request>64 THEN
        RAISE EXCEPTION 'checkpoint exceeds bounded reserved requests' USING ERRCODE='23514';
    END IF;
    IF EXISTS (
        SELECT 1 FROM generate_series(1,NEW.complete_through_request::bigint) n
        LEFT JOIN control.producer_outcomes o ON o.producer_slot_id=NEW.producer_slot_id
            AND o.producer_epoch=NEW.producer_epoch AND o.request_sequence=n
        WHERE o.request_sequence IS NULL OR o.manifest_sha256<>NEW.manifest_sha256
    ) THEN
        RAISE EXCEPTION 'checkpoint requires a complete bounded outcome prefix' USING ERRCODE='23514';
    END IF;
    SELECT count(*) INTO jobs FROM control.acquisition_progress
        WHERE producer_slot_id=NEW.producer_slot_id AND producer_epoch=NEW.producer_epoch;
    SELECT count(*) INTO dependencies FROM control.producer_checkpoint_dependencies
        WHERE producer_slot_id=NEW.producer_slot_id AND producer_epoch=NEW.producer_epoch;
    IF (jobs,dependencies) IS DISTINCT FROM (NEW.progress_jobs,NEW.dependency_count) THEN
        RAISE EXCEPTION 'checkpoint requires exact retained progress and closure counts' USING ERRCODE='23514';
    END IF;
    PERFORM control.require_producer_progress_capacity(NEW.producer_slot_id,NEW.producer_epoch,0,jobs,dependencies);
    IF NEW.checkpoint_digest<>control.producer_progress_digest(NEW) THEN
        RAISE EXCEPTION 'checkpoint digest must bind the complete canonical retained head' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;

-- Fixed-width canonical head member, matching the adapter's independent
-- big-endian encoder. Actual received time/quality are retained; SQL physical
-- values and native counters are not inferred. The enclosing digest also binds dataset/producer/context.
CREATE FUNCTION control.acquisition_progress_bytes(p control.acquisition_progress)
RETURNS bytea LANGUAGE sql IMMUTABLE STRICT SET search_path=pg_catalog AS $$
    SELECT p.job_id || p.scope_id || control.command_u64_bytes(p.progress_revision)
        || p.request_id || int2send(p.event_ordinal)
        || control.command_u64_bytes(p.wire_sequence) || p.input_sha256 || p.intent_digest
        || int2send(p.classification) || control.command_u64_bytes(coalesce(p.mutation_request,0))
        || control.command_u64_bytes(p.classified_events)
        || control.command_u64_bytes(p.successful_events)
        || control.command_u64_bytes(p.rejected_events)
        || int8send(p.observed_at_us) || control.command_u64_bytes(p.clock_resolution_us)
        || substring(control.command_u64_bytes(p.clock_anomaly_flags) FROM 5 FOR 4)
$$;

-- Called under the producer lock. Count sentinels precede the bounded aggregate;
-- at most 64*216 + 16384*72 bytes enter the server-side canonical buffer.
-- This server work is separate from the adapter's 2 MiB operation reservation.
CREATE FUNCTION control.producer_progress_digest(h control.producer_checkpoints)
RETURNS bytea LANGUAGE plpgsql STABLE STRICT SET search_path=pg_catalog AS $$
DECLARE jobs integer; dependencies integer; job_bytes bytea; dependency_bytes bytea;
BEGIN
    SELECT count(*) INTO jobs FROM (SELECT 1 FROM control.acquisition_progress
        WHERE producer_slot_id=h.producer_slot_id AND producer_epoch=h.producer_epoch LIMIT 65) b;
    SELECT count(*) INTO dependencies FROM (SELECT 1 FROM control.producer_checkpoint_dependencies
        WHERE producer_slot_id=h.producer_slot_id AND producer_epoch=h.producer_epoch LIMIT 16385) b;
    IF h.progress_format IS DISTINCT FROM 1 OR jobs>64 OR dependencies>16384
        OR (jobs,dependencies) IS DISTINCT FROM (h.progress_jobs,h.dependency_count) THEN
        RAISE EXCEPTION 'typed head requires its exact bounded retained children' USING ERRCODE='23514';
    END IF;
    SELECT coalesce(string_agg(control.acquisition_progress_bytes(p),''::bytea ORDER BY job_id),''::bytea)
        INTO job_bytes FROM control.acquisition_progress p
        WHERE producer_slot_id=h.producer_slot_id AND producer_epoch=h.producer_epoch;
    SELECT coalesce(string_agg(dataset_id || control.command_u64_bytes(source_shard)
        || control.command_u64_bytes(source_epoch) || control.command_u64_bytes(source_sequence)
        || payload_digest,''::bytea ORDER BY source_shard,source_epoch,source_sequence),''::bytea)
        INTO dependency_bytes FROM control.producer_checkpoint_dependencies
        WHERE producer_slot_id=h.producer_slot_id AND producer_epoch=h.producer_epoch;
    RETURN sha256(convert_to('bigbrotr/producer-progress/1','UTF8') || decode('00','hex')
        || h.dataset_id || h.producer_slot_id || control.command_u64_bytes(h.producer_epoch)
        || h.manifest_sha256 || control.command_u64_bytes(h.checkpoint_revision)
        || control.command_u64_bytes(h.complete_through_request)
        || control.command_u64_bytes(jobs) || control.command_u64_bytes(dependencies)
        || job_bytes || dependency_bytes);
END $$;

-- Historical SQL consistency only. This neither checks native Facts nor
-- establishes ACK, remote coverage, job completion or reusable window credit.
-- No-head legacy history is retained; opaque legacy heads are not auto-adopted.
CREATE FUNCTION control.require_retained_producer_progress(wanted_slot bytea,wanted_epoch numeric)
RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE producer control.producer_epochs%ROWTYPE; h control.producer_checkpoints%ROWTYPE;
BEGIN
    SELECT * INTO STRICT producer FROM control.producer_epochs
        WHERE producer_slot_id=wanted_slot AND producer_epoch=wanted_epoch FOR UPDATE;
    SELECT * INTO h FROM control.producer_checkpoints
        WHERE producer_slot_id=wanted_slot AND producer_epoch=wanted_epoch;
    IF NOT FOUND THEN
        IF EXISTS(SELECT 1 FROM control.acquisition_progress
                WHERE producer_slot_id=wanted_slot AND producer_epoch=wanted_epoch)
            OR EXISTS(SELECT 1 FROM control.producer_checkpoint_dependencies
                WHERE producer_slot_id=wanted_slot AND producer_epoch=wanted_epoch) THEN
            RAISE EXCEPTION 'orphan producer progress is not a checkpoint' USING ERRCODE='23514';
        END IF;
        RETURN;
    END IF;
    IF h.progress_format IS DISTINCT FROM 1 OR h.dataset_id<>producer.dataset_id
        OR h.complete_through_request>producer.reserved_through_request
        OR producer.reserved_through_request>64 OR h.complete_through_request>64
        OR h.checkpoint_digest<>control.producer_progress_digest(h) THEN
        RAISE EXCEPTION 'retained producer progress is not a validated typed head' USING ERRCODE='23514';
    END IF;
    -- Historical attribution, not current authority. Owner identity and backend
    -- attribution are immutable under owner_epoch_guard/control_backend_guard.
    -- Do not acquire parent locks after the producer lock, require a live old
    -- backend/lease, or reject the later retirement of its role instance.
    IF NOT EXISTS (
        SELECT 1 FROM control.owner_epochs o JOIN control.role_instances r
            ON r.role_instance_id=o.role_instance_id
                AND r.dataset_id=o.dataset_id AND r.role=o.role
        WHERE o.dataset_id=h.dataset_id AND o.resource_id=h.issuer_resource
            AND o.epoch=h.issuer_epoch AND o.process_incarnation=h.issuer_incarnation
            AND o.role='control' AND o.control_fence_version=1
            AND o.control_backend_pid>0 AND o.control_backend_start IS NOT NULL
    ) THEN
        RAISE EXCEPTION 'retained producer progress lost its historical Control attribution'
            USING ERRCODE='23514';
    END IF;
    IF EXISTS(SELECT 1 FROM generate_series(1,h.complete_through_request::bigint) n
        LEFT JOIN control.producer_requests r ON r.producer_slot_id=wanted_slot
            AND r.producer_epoch=wanted_epoch AND r.request_sequence=n
        LEFT JOIN control.producer_outcomes o ON o.producer_slot_id=wanted_slot
            AND o.producer_epoch=wanted_epoch AND o.request_sequence=n
        WHERE r.request_sequence IS NULL OR o.request_sequence IS NULL
            OR r.dataset_id<>h.dataset_id OR o.dataset_id<>h.dataset_id
            OR r.manifest_sha256<>h.manifest_sha256 OR o.manifest_sha256<>h.manifest_sha256
            OR r.request_digest<>o.request_digest)
        OR EXISTS(SELECT 1 FROM control.acquisition_progress p
            WHERE p.producer_slot_id=wanted_slot AND p.producer_epoch=wanted_epoch
                AND (p.dataset_id<>h.dataset_id OR p.mutation_request>h.complete_through_request))
        OR EXISTS(SELECT 1 FROM control.producer_checkpoint_dependencies d
            WHERE d.producer_slot_id=wanted_slot AND d.producer_epoch=wanted_epoch
                AND d.dataset_id<>h.dataset_id) THEN
        RAISE EXCEPTION 'retained progress lost its original contiguous outcome closure' USING ERRCODE='23514';
    END IF;
    PERFORM control.require_producer_progress_capacity(wanted_slot,wanted_epoch,
        0,h.progress_jobs,h.dependency_count);
END $$;

CREATE FUNCTION control.guard_producer_checkpoint_dependency() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE producer control.producer_epochs%ROWTYPE; jobs integer; dependencies integer;
BEGIN
    SELECT * INTO STRICT producer FROM control.producer_epochs
        WHERE producer_slot_id=NEW.producer_slot_id AND producer_epoch=NEW.producer_epoch FOR UPDATE;
    IF producer.state<>'open' OR producer.dataset_id<>NEW.dataset_id THEN
        RAISE EXCEPTION 'new checkpoint closure requires its open producer' USING ERRCODE='23514';
    END IF;
    SELECT count(*) INTO jobs FROM (SELECT 1 FROM control.acquisition_progress
        WHERE producer_slot_id=NEW.producer_slot_id AND producer_epoch=NEW.producer_epoch LIMIT 65) b;
    SELECT count(*) INTO dependencies FROM (SELECT 1 FROM control.producer_checkpoint_dependencies
        WHERE producer_slot_id=NEW.producer_slot_id AND producer_epoch=NEW.producer_epoch LIMIT 16385) b;
    PERFORM control.require_producer_progress_capacity(NEW.producer_slot_id,NEW.producer_epoch,
        0,jobs,dependencies+1);
    RETURN NEW;
END $$;
CREATE TRIGGER producer_checkpoint_dependency_guard BEFORE INSERT
    ON control.producer_checkpoint_dependencies FOR EACH ROW
    EXECUTE FUNCTION control.guard_producer_checkpoint_dependency();

CREATE FUNCTION control.require_producer_progress_children() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE head control.producer_checkpoints%ROWTYPE; jobs bigint; dependencies bigint;
BEGIN
    SELECT * INTO STRICT head FROM control.producer_checkpoints
        WHERE dataset_id=NEW.dataset_id AND producer_slot_id=NEW.producer_slot_id
            AND producer_epoch=NEW.producer_epoch;
    SELECT count(*) INTO jobs FROM control.acquisition_progress
        WHERE producer_slot_id=NEW.producer_slot_id AND producer_epoch=NEW.producer_epoch;
    SELECT count(*) INTO dependencies FROM control.producer_checkpoint_dependencies
        WHERE producer_slot_id=NEW.producer_slot_id AND producer_epoch=NEW.producer_epoch;
    IF head.progress_format IS DISTINCT FROM 1
        OR jobs<>head.progress_jobs OR dependencies<>head.dependency_count THEN
        RAISE EXCEPTION 'progress children require the complete typed cumulative head' USING ERRCODE='23514';
    END IF;
    PERFORM control.require_retained_producer_progress(NEW.producer_slot_id,NEW.producer_epoch);
    PERFORM control.require_control_owner(head.dataset_id,head.issuer_resource,
        head.issuer_epoch,head.issuer_incarnation);
    PERFORM control.require_producer_progress_capacity(NEW.producer_slot_id,NEW.producer_epoch,
        0,jobs::integer,dependencies::integer);
    RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER zzz_acquisition_progress_complete AFTER INSERT OR UPDATE
    ON control.acquisition_progress DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
    EXECUTE FUNCTION control.require_producer_progress_children();
CREATE CONSTRAINT TRIGGER zzz_producer_checkpoint_dependencies_complete AFTER INSERT
    ON control.producer_checkpoint_dependencies DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
    EXECUTE FUNCTION control.require_producer_progress_children();

CREATE OR REPLACE FUNCTION control.reserve_producer_outcome_capacity() RETURNS trigger LANGUAGE plpgsql
SET search_path=pg_catalog,control AS $$
DECLARE retained_bytes bigint; metadata bigint;
BEGIN
    PERFORM 1 FROM control.producer_epochs WHERE producer_slot_id=NEW.producer_slot_id
        AND producer_epoch=NEW.producer_epoch FOR UPDATE;
    SELECT coalesce(sum(input_bytes::bigint+65536),0) INTO retained_bytes
        FROM control.producer_requests WHERE producer_slot_id=NEW.producer_slot_id
            AND producer_epoch=NEW.producer_epoch;
    SELECT 1024+1024::bigint*progress_jobs+256::bigint*dependency_count INTO metadata
        FROM control.producer_checkpoints WHERE producer_slot_id=NEW.producer_slot_id
            AND producer_epoch=NEW.producer_epoch AND progress_format=1;
    IF retained_bytes+NEW.input_bytes+65536+coalesce(metadata,0)>33554432 THEN
        RAISE EXCEPTION 'producer reservation must include retained progress and outcome capacity'
            USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;

REVOKE ALL ON control.acquisition_progress, control.producer_checkpoint_dependencies FROM PUBLIC;
REVOKE ALL ON FUNCTION control.guard_acquisition_progress(),
    control.require_producer_progress_capacity(bytea,numeric,bigint,integer,integer),
    control.acquisition_progress_bytes(control.acquisition_progress),
    control.require_producer_progress_children(),
    control.producer_progress_digest(control.producer_checkpoints),
    control.require_retained_producer_progress(bytea,numeric),
    control.guard_producer_checkpoint_dependency() FROM PUBLIC;

-- Append to unpublished0039 after the structural retained-progress helper.
-- Historical validators only; no target-unactivated veto is removed.

CREATE OR REPLACE FUNCTION control.require_initial_retirement(
    wanted_slot bytea, wanted_epoch numeric, wanted_digest bytea
) RETURNS bigint LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE producer control.producer_epochs%ROWTYPE;
        slot control.producer_slots%ROWTYPE;
        owner control.owner_epochs%ROWTYPE;
        source control.source_streams%ROWTYPE;
        gap_count bigint;
BEGIN
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'initial retirement requires READ COMMITTED' USING ERRCODE = '25001';
    END IF;
    SELECT * INTO STRICT producer FROM control.producer_epochs
        WHERE producer_slot_id = wanted_slot AND producer_epoch = wanted_epoch;
    SELECT * INTO STRICT owner FROM control.owner_epochs
        WHERE dataset_id = producer.dataset_id AND role = 'control' AND state = 'active';
    PERFORM control.require_control_owner(owner.dataset_id, owner.resource_id,
        owner.epoch, owner.process_incarnation);
    -- The adapter takes these before producer UPDATE. The trigger rechecks under
    -- that custody; direct writers must use the same Control/parent lock order.
    PERFORM 1 FROM control.datasets WHERE dataset_id = producer.dataset_id FOR UPDATE;
    PERFORM 1 FROM control.owner_epochs
        WHERE resource_id = producer.owner_resource_id AND epoch = producer.owner_epoch FOR SHARE;
    PERFORM 1 FROM control.source_streams
        WHERE dataset_id = producer.dataset_id AND owner_resource_id = producer.owner_resource_id
            AND owner_epoch = producer.owner_epoch FOR SHARE;
    SELECT * INTO STRICT producer FROM control.producer_epochs
        WHERE producer_slot_id = wanted_slot AND producer_epoch = wanted_epoch FOR UPDATE;
    IF producer.producer_epoch <> 1 OR producer.owner_epoch <> 1
        OR producer.producer_role <> 'ingest' OR producer.state NOT IN ('closing', 'retired')
        OR producer.reserved_through_request > 64
        OR (SELECT count(*) FROM control.producer_epochs
            WHERE dataset_id = producer.dataset_id AND owner_resource_id = producer.owner_resource_id
                AND owner_epoch = producer.owner_epoch) <> 1
        OR EXISTS (SELECT 1 FROM control.producer_input_bindings
            WHERE producer_slot_id = wanted_slot AND producer_epoch = wanted_epoch) THEN
        RAISE EXCEPTION 'unsupported initial producer retirement history' USING ERRCODE = '23514';
    END IF;
    PERFORM control.require_retained_producer_progress(wanted_slot, wanted_epoch);
    SELECT * INTO STRICT slot FROM control.producer_slots WHERE producer_slot_id = wanted_slot;
    SELECT * INTO STRICT source FROM control.source_streams
        WHERE dataset_id = producer.dataset_id AND owner_resource_id = producer.owner_resource_id
            AND owner_epoch = producer.owner_epoch;
    IF source.owner_incarnation <> producer.owner_incarnation OR source.state <> 'closed'
        OR wanted_digest IS DISTINCT FROM source.closure_digest
        OR NOT EXISTS (SELECT 1 FROM control.owner_epochs
            WHERE resource_id = slot.registered_by_resource_id AND epoch = slot.registered_by_epoch
                AND process_incarnation = slot.registered_by_incarnation
                AND state = 'released' AND exclusion_method = 'storage_fenced') THEN
        RAISE EXCEPTION 'retirement requires the closed source and fenced original Control'
            USING ERRCODE = '23514';
    END IF;
    IF (SELECT count(*) FROM control.producer_requests
        WHERE producer_slot_id = wanted_slot AND producer_epoch = wanted_epoch)
            <> producer.reserved_through_request
        OR EXISTS (
            SELECT 1 FROM control.producer_requests r
            LEFT JOIN control.producer_outcomes o USING (producer_slot_id, producer_epoch, request_sequence)
            WHERE r.producer_slot_id = wanted_slot AND r.producer_epoch = wanted_epoch
                AND (o.request_sequence IS NULL OR r.request_sequence > producer.reserved_through_request
                    OR r.dataset_id <> producer.dataset_id OR o.dataset_id <> r.dataset_id
                    OR o.request_digest <> r.request_digest OR o.manifest_sha256 <> r.manifest_sha256
                    OR (r.issuer_resource, r.issuer_epoch, r.issuer_incarnation) IS DISTINCT FROM
                        (slot.registered_by_resource_id, slot.registered_by_epoch, slot.registered_by_incarnation)
                    OR (r.relay_endpoint_id IS NOT NULL AND r.acquisition_job_id IS NULL)
                    OR (r.acquisition_job_id IS NOT NULL AND NOT EXISTS (
                        SELECT 1 FROM control.acquisition_gaps g
                        WHERE g.job_id = r.acquisition_job_id AND g.producer_slot_id = wanted_slot
                            AND g.producer_epoch = wanted_epoch)))) THEN
        RAISE EXCEPTION 'retirement requires every original outcome and supported workflow'
            USING ERRCODE = '23514';
    END IF;
    -- Include zero-request claims. A missing request binding is not evidence
    -- that the original executor had no acquisition/import obligation.
    IF EXISTS (SELECT 1 FROM control.jobs
        WHERE dataset_id = producer.dataset_id AND kind = 'acquisition'
            AND (owner_resource_id, owner_epoch, owner_incarnation) =
                (slot.registered_by_resource_id, slot.registered_by_epoch, slot.registered_by_incarnation))
        OR EXISTS (SELECT 1 FROM control.jobs
        WHERE dataset_id = producer.dataset_id AND kind = 'import'
            AND (owner_resource_id, owner_epoch, owner_incarnation) =
                (slot.registered_by_resource_id, slot.registered_by_epoch, slot.registered_by_incarnation)) THEN
        RAISE EXCEPTION 'original producer workflow claims remain unresolved' USING ERRCODE = '23514';
    END IF;
    SELECT count(*) INTO gap_count FROM (
        SELECT 1 FROM control.acquisition_gaps
        WHERE producer_slot_id = wanted_slot AND producer_epoch = wanted_epoch LIMIT 65
    ) AS bounded;
    IF gap_count > 64 OR EXISTS (
        SELECT 1 FROM control.acquisition_gaps g JOIN control.jobs j USING (job_id)
        WHERE g.producer_slot_id = wanted_slot AND g.producer_epoch = wanted_epoch
            AND (g.dataset_id <> producer.dataset_id OR g.shard_id <> source.shard_id
                OR g.source_owner_epoch <> source.owner_epoch
                OR g.last_complete_sequence <> source.last_complete_sequence
                OR g.source_closure_digest <> source.closure_digest
                OR (g.previous_owner_resource, g.previous_owner_epoch, g.previous_owner_incarnation)
                    IS DISTINCT FROM (slot.registered_by_resource_id, slot.registered_by_epoch,
                        slot.registered_by_incarnation)
                OR j.dataset_id <> g.dataset_id OR j.acquisition_job_protocol IS DISTINCT FROM 1
                OR j.state <> 'cancelled' OR j.job_revision <> g.previous_job_revision + 2
                OR j.checkpoint_revision <> 0 OR j.result_manifest_digest IS NOT NULL
                OR j.reason_code IS DISTINCT FROM g.reason
                OR j.completed_at_us IS DISTINCT FROM g.abandoned_at_us
                OR (j.owner_resource_id, j.owner_epoch, j.owner_incarnation)
                    IS DISTINCT FROM (g.issuer_resource, g.issuer_epoch, g.issuer_incarnation))) THEN
        RAISE EXCEPTION 'retirement requires complete retained acquisition gaps' USING ERRCODE = '23514';
    END IF;
    IF owner.lease_expires_at_us <= (EXTRACT(EPOCH FROM clock_timestamp()) * 1000000)::bigint THEN
        RAISE EXCEPTION 'retirement Control authority expired' USING ERRCODE = '23514';
    END IF;
    RETURN gap_count;
END $$;

CREATE OR REPLACE FUNCTION control.lock_activated_initial_retirement(wanted_resource bytea)
RETURNS control.initial_ingest_successions LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE s control.initial_ingest_successions%ROWTYPE;
        a control.initial_ingest_activations%ROWTYPE;
        current_owner control.owner_epochs%ROWTYPE;
        old_owner control.owner_epochs%ROWTYPE;
        old_producer control.producer_epochs%ROWTYPE;
BEGIN
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'activated recovery requires READ COMMITTED' USING ERRCODE = '25001';
    END IF;
    SELECT * INTO STRICT s FROM control.initial_ingest_successions
        WHERE owner_resource_id = wanted_resource;
    SELECT * INTO STRICT a FROM control.initial_ingest_activations WHERE owner_resource_id = wanted_resource;
    IF octet_length(s.handoff) > 610 OR s.previous_topology <> 0 OR s.target_topology <> 1
        OR a.activation_transaction = pg_current_xact_id()
        OR ROW(a.dataset_id,a.target_epoch,a.target_incarnation,a.target_role_instance_id,
            a.producer_slot_id,a.producer_epoch,a.previous_manifest_sha256,a.target_manifest_sha256,a.handoff_sha256,
            a.prepared_by_resource,a.prepared_by_epoch,a.prepared_by_incarnation,
            a.activated_by_resource,a.activated_by_epoch,a.activated_by_incarnation,
            a.config_revision,a.policy_revision,a.config_digest,a.policy_digest)
        IS DISTINCT FROM ROW(s.dataset_id,s.target_epoch,s.target_incarnation,s.target_role_instance_id,
            s.producer_slot_id,s.producer_epoch,s.previous_manifest_sha256,s.target_manifest_sha256,s.handoff_sha256,
            s.prepared_by_resource,s.prepared_by_epoch,s.prepared_by_incarnation,
            s.prepared_by_resource,s.prepared_by_epoch,s.prepared_by_incarnation,
            s.config_revision,s.policy_revision,s.config_digest,s.policy_digest)
        OR a.activated_at_us < s.created_at_us THEN
        RAISE EXCEPTION 'activated recovery requires exact committed preparation and activation' USING ERRCODE = '23514';
    END IF;
    SELECT * INTO STRICT current_owner FROM control.owner_epochs
        WHERE dataset_id = s.dataset_id AND role = 'control' AND state = 'active';
    PERFORM control.require_control_owner(s.dataset_id, current_owner.resource_id,
        current_owner.epoch, current_owner.process_incarnation);
    -- Generic import claims take SHARE on this row, without dataset custody.
    -- The typed caller takes this exclusive parent before its dataset lock too.
    SELECT * INTO STRICT current_owner FROM control.owner_epochs
        WHERE resource_id = current_owner.resource_id AND epoch = current_owner.epoch
            AND state = 'active' FOR UPDATE;
    PERFORM 1 FROM control.datasets WHERE dataset_id = s.dataset_id FOR UPDATE;
    IF current_owner.resource_id <> s.prepared_by_resource
        OR current_owner.epoch <= s.prepared_by_epoch
        OR NOT EXISTS (SELECT 1 FROM control.owner_epochs o JOIN control.role_instances r
            ON r.role_instance_id = o.role_instance_id
            WHERE o.dataset_id = s.dataset_id AND o.resource_id = s.prepared_by_resource
                AND o.epoch = s.prepared_by_epoch AND o.process_incarnation = s.prepared_by_incarnation
                AND o.role = 'control' AND r.dataset_id = s.dataset_id AND r.role = 'control'
                AND o.control_fence_version = 1
                AND o.epoch < current_owner.epoch AND o.state = 'released'
                AND o.exclusion_method = 'storage_fenced' AND o.exclusion_digest IS NOT NULL
                AND r.retired_at_us IS NOT NULL)
        OR NOT EXISTS (SELECT 1 FROM control.datasets d JOIN control.config_revisions c
            ON c.dataset_id = d.dataset_id AND c.revision = d.config_revision
            JOIN control.policy_revisions p ON p.dataset_id = d.dataset_id
                AND p.revision = d.policy_revision AND p.config_revision = c.revision
            WHERE d.dataset_id = s.dataset_id AND d.config_revision >= s.config_revision
                AND d.policy_revision >= s.policy_revision AND d.topology_generation = s.target_topology)
        OR NOT EXISTS (SELECT 1 FROM control.config_revisions c JOIN control.policy_revisions p
            ON p.dataset_id = c.dataset_id AND p.config_revision = c.revision
            WHERE c.dataset_id = s.dataset_id AND c.revision = s.config_revision
                AND c.document_digest = s.config_digest AND p.revision = s.policy_revision
                AND p.document_digest = s.policy_digest) THEN
        RAISE EXCEPTION 'activated recovery requires exact preparation and current Control policy'
            USING ERRCODE = '23514';
    END IF;

    -- Old Ingest scheduling expiry is irrelevant. Lock the historical registry, followed
    -- by the target parents. All domain work is bounded by the nine known kinds.
    SELECT * INTO STRICT old_owner FROM control.owner_epochs
        WHERE resource_id = s.owner_resource_id AND epoch = 1 FOR UPDATE;
    PERFORM 1 FROM control.role_instances WHERE role_instance_id = old_owner.role_instance_id FOR UPDATE;
    PERFORM 1 FROM control.allocation_domains
        WHERE dataset_id = s.dataset_id AND owner_resource_id = s.owner_resource_id AND owner_epoch = 1
        ORDER BY dictionary_kind, allocation_domain LIMIT 10 FOR UPDATE;
    IF (SELECT count(*) FROM (SELECT 1 FROM control.allocation_domains
        WHERE dataset_id = s.dataset_id AND owner_resource_id = s.owner_resource_id AND owner_epoch = 1
        LIMIT 10) AS bounded) <> 9 THEN
        RAISE EXCEPTION 'initial predecessor domain set is incomplete or exceeds its bound' USING ERRCODE = '23514';
    END IF;
    PERFORM 1 FROM control.source_streams
        WHERE dataset_id = s.dataset_id AND shard_id = s.shard_id AND owner_epoch = 1 FOR UPDATE;
    SELECT * INTO STRICT old_producer FROM control.producer_epochs
        WHERE producer_slot_id = s.producer_slot_id AND producer_epoch = 1 FOR UPDATE;
    PERFORM control.require_initial_retirement(s.producer_slot_id, 1, s.closure_digest);
    IF old_owner.dataset_id <> s.dataset_id OR old_owner.role <> 'ingest'
        OR old_owner.state <> 'released' OR old_owner.exclusion_method <> 'storage_fenced'
        OR old_owner.exclusion_digest IS DISTINCT FROM s.closure_digest
        OR old_owner.released_at_us IS DISTINCT FROM old_producer.retired_at_us
        OR old_producer.state <> 'retired' OR old_producer.owner_resource_id <> s.owner_resource_id
        OR old_producer.owner_epoch <> 1 OR old_producer.owner_incarnation <> old_owner.process_incarnation
        OR old_producer.closure_digest IS DISTINCT FROM s.closure_digest
        OR old_producer.reserved_through_request <> s.issued_through
        OR old_owner.process_incarnation = s.target_incarnation
        OR old_owner.role_instance_id = s.target_role_instance_id
        OR s.created_at_us < old_producer.retired_at_us
        OR NOT EXISTS (SELECT 1 FROM control.role_instances
            WHERE role_instance_id = old_owner.role_instance_id AND dataset_id = s.dataset_id
                AND role = 'ingest' AND retired_at_us = old_producer.retired_at_us)
        OR EXISTS (SELECT 1 FROM control.producer_requests
            WHERE producer_slot_id = s.producer_slot_id AND producer_epoch = 1
                AND manifest_sha256 <> s.previous_manifest_sha256)
        OR EXISTS (SELECT 1 FROM control.allocation_domains
            WHERE dataset_id = s.dataset_id AND owner_resource_id = s.owner_resource_id AND owner_epoch = 1
                AND (state <> 'closed' OR closure_digest IS DISTINCT FROM s.closure_digest
                    OR closed_at_us IS DISTINCT FROM old_producer.retired_at_us))
        OR NOT EXISTS (SELECT 1 FROM control.source_streams
            WHERE dataset_id = s.dataset_id AND shard_id = s.shard_id AND owner_epoch = 1
                AND owner_resource_id = s.owner_resource_id AND owner_incarnation = old_owner.process_incarnation
                AND state = 'closed' AND closure_digest = s.closure_digest
                AND last_complete_sequence = s.last_complete_sequence
                AND closed_at_us <= old_producer.retired_at_us) THEN
        RAISE EXCEPTION 'activated recovery requires the exact retired initial predecessor'
            USING ERRCODE = '23514';
    END IF;

    PERFORM 1 FROM control.owner_epochs WHERE resource_id = s.owner_resource_id AND epoch = 2 FOR UPDATE;
    PERFORM 1 FROM control.role_instances WHERE role_instance_id = s.target_role_instance_id FOR UPDATE;
    PERFORM 1 FROM control.allocation_domains
        WHERE dataset_id = s.dataset_id AND owner_resource_id = s.owner_resource_id AND owner_epoch = 2
        ORDER BY dictionary_kind, allocation_domain LIMIT 10 FOR UPDATE;
    IF (SELECT count(*) FROM (SELECT 1 FROM control.allocation_domains
        WHERE dataset_id = s.dataset_id AND owner_resource_id = s.owner_resource_id AND owner_epoch = 2
        LIMIT 10) AS bounded) <> 9 THEN
        RAISE EXCEPTION 'activated recovery target domain set is incomplete or exceeds its bound' USING ERRCODE = '23514';
    END IF;
    PERFORM 1 FROM control.source_streams
        WHERE dataset_id = s.dataset_id AND shard_id = s.shard_id AND owner_epoch = 2 FOR UPDATE;
    PERFORM 1 FROM control.producer_epochs
        WHERE producer_slot_id = s.producer_slot_id AND producer_epoch = 2 FOR UPDATE;
    IF NOT EXISTS (SELECT 1 FROM control.owner_epochs o JOIN control.role_instances r
            ON r.role_instance_id = o.role_instance_id
            WHERE o.resource_id = s.owner_resource_id AND o.epoch = 2 AND o.dataset_id = s.dataset_id
                AND o.role = 'ingest' AND o.process_incarnation = s.target_incarnation
                AND o.role_instance_id = s.target_role_instance_id AND o.acquired_at_us = s.created_at_us
                AND o.state IN ('active','draining','released')
                AND r.dataset_id = s.dataset_id AND r.role = 'ingest' AND r.created_at_us = s.created_at_us
                )
        OR NOT EXISTS (SELECT 1 FROM control.producer_epochs
            WHERE producer_slot_id = s.producer_slot_id AND producer_epoch = 2 AND dataset_id = s.dataset_id
                AND producer_role_instance_id = s.target_role_instance_id AND producer_role = 'ingest'
                AND producer_incarnation = s.target_incarnation AND owner_resource_id = s.owner_resource_id
                AND owner_epoch = 2 AND owner_role = 'ingest' AND owner_incarnation = s.target_incarnation
                AND state IN ('closing', 'retired') AND opened_at_us = s.created_at_us
                AND outstanding_request_limit = s.outstanding_request_limit
                AND reserved_through_request <= s.outstanding_request_limit)
        OR NOT EXISTS (SELECT 1 FROM control.source_streams
            WHERE dataset_id = s.dataset_id AND shard_id = s.shard_id AND owner_epoch = 2
                AND owner_resource_id = s.owner_resource_id AND owner_role = 'ingest'
                AND owner_incarnation = s.target_incarnation AND state = 'closed' AND opened_at_us = s.created_at_us)
        OR (SELECT count(*) FROM (SELECT 1 FROM control.producer_epochs WHERE dataset_id = s.dataset_id
            AND owner_resource_id = s.owner_resource_id AND owner_epoch = 2 LIMIT 2) AS bounded) <> 1
        OR (SELECT count(*) FROM (SELECT 1 FROM control.source_streams WHERE dataset_id = s.dataset_id
            AND owner_resource_id = s.owner_resource_id AND owner_epoch = 2 LIMIT 2) AS bounded) <> 1
        OR (SELECT array_agg(dictionary_kind ORDER BY dictionary_kind) FROM control.allocation_domains
            WHERE dataset_id = s.dataset_id AND owner_resource_id = s.owner_resource_id AND owner_epoch = 1)
            IS DISTINCT FROM (SELECT array_agg(dictionary_kind ORDER BY dictionary_kind) FROM control.allocation_domains
                WHERE dataset_id = s.dataset_id AND owner_resource_id = s.owner_resource_id AND owner_epoch = 2)
        OR EXISTS (SELECT 1 FROM control.allocation_domains
            WHERE dataset_id = s.dataset_id AND owner_resource_id = s.owner_resource_id AND owner_epoch = 2
                AND (owner_role <> 'ingest' OR owner_incarnation <> s.target_incarnation
                    OR created_at_us <> s.created_at_us)) THEN
        RAISE EXCEPTION 'activated recovery target registry identity differs' USING ERRCODE = '23514';
    END IF;
    -- The typed caller also compares all eighteen domain IDs with both decoded
    -- manifests. SQL metadata never substitutes for the native closing proof.
    -- Positive unique sequences with count=max=reserved form the exact retained
    -- prefix, including zero. The typed source owner separately requires native
    -- issued=reserved; SQL metadata cannot establish an unissued native tail.
    IF NOT EXISTS (SELECT 1 FROM control.producer_epochs p CROSS JOIN LATERAL (
            SELECT count(*) AS retained, coalesce(max(r.request_sequence), 0) AS last_sequence,
                coalesce(bool_and(r.dataset_id = s.dataset_id AND r.manifest_sha256 = s.target_manifest_sha256
                    AND r.issuer_resource = a.activated_by_resource AND r.issuer_epoch = a.activated_by_epoch
                    AND r.issuer_incarnation = a.activated_by_incarnation), true) AS exact_identity
            FROM control.producer_requests r WHERE r.producer_slot_id = s.producer_slot_id AND r.producer_epoch = 2
        ) requests WHERE p.producer_slot_id = s.producer_slot_id AND p.producer_epoch = 2
            AND requests.retained = p.reserved_through_request
            AND requests.last_sequence = p.reserved_through_request AND requests.exact_identity)
        OR NOT EXISTS (SELECT 1 FROM control.topology_generations
            WHERE dataset_id = s.dataset_id AND generation = s.target_topology
                AND previous_generation = s.previous_topology AND sealed AND created_at_us = s.created_at_us)
        OR (SELECT count(*) FROM (SELECT 1 FROM control.topology_streams
            WHERE dataset_id = s.dataset_id AND topology_generation = s.target_topology LIMIT 3) AS bounded) <> 2
        OR NOT EXISTS (SELECT 1 FROM control.topology_streams
            WHERE dataset_id = s.dataset_id AND topology_generation = s.target_topology
                AND shard_id = s.shard_id AND owner_epoch = 1
                AND handoff_last_complete_sequence = s.last_complete_sequence)
        OR NOT EXISTS (SELECT 1 FROM control.topology_streams
            WHERE dataset_id = s.dataset_id AND topology_generation = s.target_topology
                AND shard_id = s.shard_id AND owner_epoch = 2 AND handoff_last_complete_sequence IS NULL) THEN
        RAISE EXCEPTION 'activated recovery requires original target reservations and exact sealed topology'
            USING ERRCODE = '23514';
    END IF;
    -- Local classified progress does not complete or abandon workflow claims.
    -- Retirement still requires exact copies and no unresolved import obligation.
    PERFORM control.require_retained_producer_progress(s.producer_slot_id, 2);
    IF EXISTS (SELECT 1 FROM control.producer_input_bindings
            WHERE producer_slot_id=s.producer_slot_id AND producer_epoch=2)
        OR EXISTS (SELECT 1 FROM control.producer_requests r
            LEFT JOIN control.producer_outcomes o USING(producer_slot_id,producer_epoch,request_sequence)
            WHERE r.producer_slot_id=s.producer_slot_id AND r.producer_epoch=2
                AND (o.request_sequence IS NULL OR o.dataset_id<>r.dataset_id
                    OR o.request_digest<>r.request_digest OR o.manifest_sha256<>r.manifest_sha256
                    OR (r.relay_endpoint_id IS NOT NULL AND r.acquisition_job_id IS NULL)
                    OR (r.acquisition_job_id IS NOT NULL AND NOT EXISTS (
                        SELECT 1 FROM control.acquisition_gaps g WHERE g.job_id=r.acquisition_job_id
                            AND g.producer_slot_id=s.producer_slot_id AND g.producer_epoch=2)))) THEN
        RAISE EXCEPTION 'activated retirement requires exact copies and supported workflow obligations'
            USING ERRCODE='23514';
    END IF;
    -- An owned claim must already have been abandoned by the separate operation.
    -- Both partial owner indexes avoid scanning unrelated job histories. Actual
    -- job custody follows the registry/source/producer parents, in job-ID order.
    PERFORM job_id FROM control.jobs
        WHERE dataset_id=s.dataset_id AND owner_resource_id=a.activated_by_resource
            AND owner_epoch=a.activated_by_epoch AND owner_incarnation=a.activated_by_incarnation
            AND kind='acquisition' ORDER BY job_id LIMIT 1 FOR UPDATE;
    IF FOUND THEN
        RAISE EXCEPTION 'activated producer workflow claims remain unresolved' USING ERRCODE='23514';
    END IF;
    PERFORM job_id FROM control.jobs
        WHERE dataset_id=s.dataset_id AND owner_resource_id=a.activated_by_resource
            AND owner_epoch=a.activated_by_epoch AND owner_incarnation=a.activated_by_incarnation
            AND kind='import' ORDER BY job_id LIMIT 1 FOR UPDATE;
    IF FOUND THEN
        RAISE EXCEPTION 'activated producer import claims remain unresolved' USING ERRCODE='23514';
    END IF;
    IF (SELECT count(*) FROM (SELECT 1 FROM control.acquisition_gaps
        WHERE producer_slot_id=s.producer_slot_id AND producer_epoch=2 LIMIT 65) bounded)>64 THEN
        RAISE EXCEPTION 'activated retirement acquisition gap bound exceeded' USING ERRCODE='23514';
    END IF;
    PERFORM j.job_id FROM control.acquisition_gaps g JOIN control.jobs j USING(job_id)
        WHERE g.producer_slot_id=s.producer_slot_id AND g.producer_epoch=2
        ORDER BY j.job_id LIMIT 65 FOR UPDATE OF j;
    -- These immutable gaps have historical issuers. Never rerun their insertion
    -- guard or require those issuers to remain live in this fresh transaction.
    IF EXISTS (SELECT 1 FROM control.acquisition_gaps g LEFT JOIN control.jobs j USING(job_id)
        LEFT JOIN control.source_streams stream ON stream.dataset_id=g.dataset_id
            AND stream.shard_id=g.shard_id AND stream.owner_epoch=g.source_owner_epoch
        WHERE g.producer_slot_id=s.producer_slot_id AND g.producer_epoch=2
            AND NOT coalesce(g.dataset_id=s.dataset_id AND g.shard_id=s.shard_id AND g.source_owner_epoch=2
                AND g.reason='activated_initial_producer_abandoned' AND g.owner_role='control'
                AND g.last_complete_sequence=stream.last_complete_sequence AND g.source_closure_digest=stream.closure_digest
                AND g.previous_owner_resource=a.activated_by_resource AND g.previous_owner_epoch=a.activated_by_epoch
                AND g.previous_owner_incarnation=a.activated_by_incarnation AND g.issuer_resource=a.activated_by_resource
                AND g.issuer_epoch>a.activated_by_epoch AND g.issuer_epoch<=current_owner.epoch
                AND g.abandoned_at_us>=stream.closed_at_us AND g.abandoned_at_us>=j.created_at_us
                AND g.abandoned_at_us<=(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint
                AND j.dataset_id=g.dataset_id AND j.kind='acquisition' AND j.acquisition_job_protocol=1
                AND j.input_complete AND j.state='cancelled' AND j.job_revision=g.previous_job_revision+2
                AND j.checkpoint_revision=0 AND j.result_manifest_digest IS NULL
                AND j.reason_code=g.reason AND j.completed_at_us=g.abandoned_at_us
                AND (j.owner_resource_id,j.owner_epoch,j.owner_incarnation)
                    IS NOT DISTINCT FROM (g.issuer_resource,g.issuer_epoch,g.issuer_incarnation), false)) THEN
        RAISE EXCEPTION 'activated retirement requires complete retained acquisition gaps' USING ERRCODE='23514';
    END IF;
    -- Parent/job locks can wait: sample current actual authority again afterwards.
    PERFORM control.require_control_owner(s.dataset_id, current_owner.resource_id,
        current_owner.epoch, current_owner.process_incarnation);
    IF current_owner.lease_expires_at_us <= (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint THEN
        RAISE EXCEPTION 'activated retirement Control authority expired' USING ERRCODE='23514';
    END IF;
    RETURN s;
END $$;

CREATE OR REPLACE FUNCTION control.require_ingest_lineage_workflows(wanted_resource bytea,wanted_epoch numeric,current_epoch numeric)
RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE p control.producer_epochs%ROWTYPE; stream control.source_streams%ROWTYPE;
        original record; wanted_reason text; gap_count bigint;
BEGIN
    SELECT * INTO STRICT p FROM control.producer_epochs WHERE owner_resource_id=wanted_resource AND owner_epoch=wanted_epoch;
    SELECT * INTO STRICT stream FROM control.source_streams WHERE owner_resource_id=wanted_resource AND owner_epoch=wanted_epoch;
    SELECT * INTO STRICT original FROM control.ingest_lineage_activation(wanted_resource,wanted_epoch);
    wanted_reason:=CASE WHEN wanted_epoch=1 THEN 'initial_producer_abandoned'
        WHEN wanted_epoch=2 THEN 'activated_initial_producer_abandoned' ELSE 'ingest_successor_abandoned' END;
    PERFORM control.require_retained_producer_progress(p.producer_slot_id, wanted_epoch);
    IF EXISTS(SELECT 1 FROM control.producer_input_bindings WHERE producer_slot_id=p.producer_slot_id AND producer_epoch=wanted_epoch)
        OR EXISTS(SELECT 1 FROM control.producer_requests r
            WHERE r.producer_slot_id=p.producer_slot_id AND r.producer_epoch=wanted_epoch
                AND ((r.relay_endpoint_id IS NOT NULL AND r.acquisition_job_id IS NULL)
                    OR (r.acquisition_job_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM control.acquisition_gaps g
                        WHERE g.job_id=r.acquisition_job_id AND g.producer_slot_id=p.producer_slot_id AND g.producer_epoch=wanted_epoch)))) THEN
        RAISE EXCEPTION 'lineage retirement has unsupported workflow obligations' USING ERRCODE='23514';
    END IF;
    PERFORM job_id FROM control.jobs WHERE dataset_id=p.dataset_id AND kind='acquisition'
        AND owner_resource_id=original.resource AND owner_epoch=original.epoch AND owner_incarnation=original.incarnation
        ORDER BY job_id LIMIT 1 FOR UPDATE;
    IF FOUND THEN RAISE EXCEPTION 'lineage acquisition claims remain unresolved' USING ERRCODE='23514'; END IF;
    PERFORM job_id FROM control.jobs WHERE dataset_id=p.dataset_id AND kind='import'
        AND owner_resource_id=original.resource AND owner_epoch=original.epoch AND owner_incarnation=original.incarnation
        ORDER BY job_id LIMIT 1 FOR UPDATE;
    IF FOUND THEN RAISE EXCEPTION 'lineage import claims remain unresolved' USING ERRCODE='23514'; END IF;
    SELECT count(*) INTO gap_count FROM (SELECT 1 FROM control.acquisition_gaps
        WHERE producer_slot_id=p.producer_slot_id AND producer_epoch=wanted_epoch LIMIT 65) bounded;
    IF gap_count>64 THEN RAISE EXCEPTION 'lineage gap bound exceeded' USING ERRCODE='23514'; END IF;
    PERFORM j.job_id FROM control.acquisition_gaps g JOIN control.jobs j USING(job_id)
        WHERE g.producer_slot_id=p.producer_slot_id AND g.producer_epoch=wanted_epoch
        ORDER BY j.job_id LIMIT 65 FOR UPDATE OF j;
    IF EXISTS(SELECT 1 FROM control.acquisition_gaps g LEFT JOIN control.jobs j USING(job_id)
        WHERE g.producer_slot_id=p.producer_slot_id AND g.producer_epoch=wanted_epoch
            AND NOT coalesce(g.dataset_id=p.dataset_id AND g.shard_id=stream.shard_id AND g.source_owner_epoch=wanted_epoch
                AND g.reason=wanted_reason AND g.owner_role='control' AND g.last_complete_sequence=stream.last_complete_sequence
                AND g.source_closure_digest=stream.closure_digest
                AND ROW(g.previous_owner_resource,g.previous_owner_epoch,g.previous_owner_incarnation)
                    IS NOT DISTINCT FROM ROW(original.resource,original.epoch,original.incarnation)
                AND g.issuer_resource=original.resource AND g.issuer_epoch>original.epoch AND g.issuer_epoch<=current_epoch
                AND g.abandoned_at_us>=stream.closed_at_us AND g.abandoned_at_us>=j.created_at_us
                AND g.abandoned_at_us<=(extract(epoch FROM clock_timestamp())*1000000)::bigint
                AND j.dataset_id=g.dataset_id AND j.kind='acquisition' AND j.acquisition_job_protocol=1 AND j.input_complete
                AND j.state='cancelled' AND j.job_revision=g.previous_job_revision+2 AND j.checkpoint_revision=0
                AND j.result_manifest_digest IS NULL AND j.reason_code=g.reason AND j.completed_at_us=g.abandoned_at_us
                AND ROW(j.owner_resource_id,j.owner_epoch,j.owner_incarnation)
                    IS NOT DISTINCT FROM ROW(g.issuer_resource,g.issuer_epoch,g.issuer_incarnation),false)) THEN
        RAISE EXCEPTION 'lineage retirement requires exact retained gaps' USING ERRCODE='23514';
    END IF;
END $$;
