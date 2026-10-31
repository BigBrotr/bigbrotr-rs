-- Initial OPEN-prefix two-slot state. Native synced retirement and authenticated
-- input unlink precede finalization; SQL cannot establish either local effect.
-- Existing v3/successor heads retain their old profile. Request65 remains gated
-- by the unchanged absolute intake ceiling pending physical-lane qualification.
ALTER TABLE control.producer_checkpoints
    DROP CONSTRAINT producer_checkpoints_progress_format_check,
    DROP CONSTRAINT producer_progress_shape,
    ADD COLUMN prefix_profile boolean NOT NULL DEFAULT false,
    ADD COLUMN prefix_active_bytes bytea,
    ADD COLUMN prefix_active_sha256 bytea,
    ADD COLUMN prefix_active_generation numeric(20,0),
    ADD COLUMN prefix_active_retired numeric(20,0),
    ADD COLUMN prefix_active_source numeric(20,0),
    ADD COLUMN prefix_prepared_bytes bytea,
    ADD COLUMN prefix_prepared_sha256 bytea,
    ADD COLUMN prefix_prepared_generation numeric(20,0),
    ADD COLUMN prefix_prepared_retired numeric(20,0),
    ADD COLUMN prefix_prepared_source numeric(20,0),
    ADD CONSTRAINT producer_progress_shape CHECK (
        (progress_format IS NULL AND num_nonnulls(manifest_sha256,progress_jobs,
            dependency_count,issuer_resource,issuer_epoch,issuer_incarnation)=0 AND NOT prefix_profile)
        OR (progress_format IN (1,2) AND num_nonnulls(manifest_sha256,progress_jobs,
            dependency_count,issuer_resource,issuer_epoch,issuer_incarnation)=6
            AND progress_jobs BETWEEN 0 AND 64 AND dependency_count BETWEEN 0 AND 16384
            AND issuer_epoch>0 AND (progress_format=1 OR prefix_profile))),
    ADD CONSTRAINT producer_prefix_slots CHECK (
        (NOT prefix_profile AND num_nonnulls(prefix_active_bytes,prefix_active_sha256,
            prefix_active_generation,prefix_active_retired,prefix_active_source,
            prefix_prepared_bytes,prefix_prepared_sha256,prefix_prepared_generation,
            prefix_prepared_retired,prefix_prepared_source)=0)
        OR (prefix_profile AND producer_epoch=1 AND progress_format IS NOT NULL
            AND ((prefix_active_bytes IS NULL AND num_nonnulls(prefix_active_sha256,
                prefix_active_generation,prefix_active_retired,prefix_active_source)=0 AND progress_format=1)
              OR (num_nonnulls(prefix_active_bytes,prefix_active_sha256,prefix_active_generation,
                prefix_active_retired,prefix_active_source)=5 AND progress_format=2
                AND octet_length(prefix_active_bytes) BETWEEN 325 AND 16384
                AND octet_length(prefix_active_sha256)=32 AND prefix_active_sha256=sha256(prefix_active_bytes)
                AND prefix_active_generation BETWEEN 1 AND 18446744073709551615
                AND prefix_active_retired BETWEEN 1 AND 18446744073709551615
                AND prefix_active_source BETWEEN 0 AND 18446744073709551615))
            AND ((prefix_prepared_bytes IS NULL AND num_nonnulls(prefix_prepared_sha256,
                prefix_prepared_generation,prefix_prepared_retired,prefix_prepared_source)=0)
              OR (num_nonnulls(prefix_prepared_bytes,prefix_prepared_sha256,prefix_prepared_generation,
                prefix_prepared_retired,prefix_prepared_source)=5
                AND octet_length(prefix_prepared_bytes) BETWEEN 325 AND 16384
                AND octet_length(prefix_prepared_sha256)=32 AND prefix_prepared_sha256=sha256(prefix_prepared_bytes)
                AND prefix_prepared_generation BETWEEN 1 AND 18446744073709551615
                AND prefix_prepared_retired BETWEEN 1 AND 18446744073709551615
                AND prefix_prepared_source BETWEEN 0 AND 18446744073709551615))));

-- Zero-based canonical offsets, including values above signed bigint::MAX.
CREATE FUNCTION control.producer_prefix_u64(bytes bytea, at_offset integer)
RETURNS numeric LANGUAGE plpgsql IMMUTABLE STRICT SET search_path=pg_catalog AS $$
DECLARE value numeric:=0; i integer;
BEGIN
    IF at_offset<0 OR at_offset>octet_length(bytes)-8 THEN
        RAISE EXCEPTION 'truncated prefix u64' USING ERRCODE='23514';
    END IF;
    FOR i IN at_offset..at_offset+7 LOOP value:=value*256+get_byte(bytes,i); END LOOP;
    RETURN value;
END $$;

-- Complete canonical job bytes must equal the captured current heads at prepare.
-- An active slot is immutable; later current heads may monotonically advance.
CREATE FUNCTION control.require_producer_prefix_base(h control.producer_checkpoints,b bytea)
RETURNS void LANGUAGE plpgsql STABLE STRICT SET search_path=pg_catalog AS $$
DECLARE jobs integer; generation numeric; retired numeric; source_cut numeric; batches numeric;
BEGIN
    IF NOT h.prefix_profile OR h.producer_epoch<>1 OR octet_length(b) NOT BETWEEN 325 AND 16384
        OR substring(b FROM 1 FOR 27)<>convert_to('bigbrotr/producer-prefix/1','UTF8')||decode('00','hex')
        OR substring(b FROM 28 FOR 72)<>h.dataset_id||h.producer_slot_id
            ||control.command_u64_bytes(1)||h.manifest_sha256 THEN
        RAISE EXCEPTION 'prefix base context or fixed format differs' USING ERRCODE='23514';
    END IF;
    generation:=control.producer_prefix_u64(b,99);
    retired:=control.producer_prefix_u64(b,139);
    source_cut:=control.producer_prefix_u64(b,147);
    batches:=control.producer_prefix_u64(b,203);
    IF control.producer_prefix_u64(b,317)>64 THEN
        RAISE EXCEPTION 'prefix base job limit' USING ERRCODE='23514';
    END IF;
    jobs:=control.producer_prefix_u64(b,317)::integer;
    IF octet_length(b)<>325+216*jobs OR generation<1 OR retired<generation
        OR (generation=1)<>(substring(b FROM 108 FOR 32)=decode(repeat('00',32),'hex'))
        OR substring(b FROM 156 FOR 16)=decode(repeat('00',16),'hex')
        OR control.producer_prefix_u64(b,171)=0
        OR substring(b FROM 180 FOR 16)=decode(repeat('00',16),'hex')
        OR control.producer_prefix_u64(b,195)=0 OR control.producer_prefix_u64(b,277)=0
        OR substring(b FROM 276 FOR 2)<>int2send((CASE WHEN generation=1 THEN 1 ELSE 2 END)::smallint)
        OR (source_cut=0)<>(batches=0) OR batches>source_cut OR batches>retired
        OR (source_cut=0 AND substring(b FROM 212 FOR 32)<>sha256(
            convert_to('bigbrotr/producer-prefix-source/1','UTF8')||decode('00','hex')||substring(b FROM 28 FOR 72))) THEN
        RAISE EXCEPTION 'prefix base has invalid canonical coordinates' USING ERRCODE='23514';
    END IF;
END $$;

-- Only the migration owner inside the exact atomic slot promotion can delete
-- detail. No GUC, caller flag, portable base or prepared-only row is sufficient.
CREATE FUNCTION control.producer_prefix_deletion(wanted_slot bytea,wanted_epoch numeric,
    wanted_dataset bytea,wanted_request numeric,wanted_source numeric)
RETURNS boolean LANGUAGE sql STABLE SET search_path=pg_catalog AS $$
    SELECT EXISTS(SELECT 1 FROM control.producer_checkpoints h
        JOIN pg_catalog.pg_class c ON c.oid='control.producer_checkpoints'::regclass
        WHERE h.producer_slot_id=wanted_slot AND h.producer_epoch=wanted_epoch
            AND h.dataset_id=wanted_dataset AND h.prefix_profile
            AND current_user=pg_catalog.pg_get_userbyid(c.relowner)
            AND h.prefix_active_bytes=h.prefix_prepared_bytes
            AND h.prefix_active_sha256=h.prefix_prepared_sha256
            AND h.prefix_active_generation=h.prefix_prepared_generation
            AND h.prefix_active_retired=h.prefix_prepared_retired
            AND h.prefix_active_source=h.prefix_prepared_source
            AND ((wanted_request IS NOT NULL AND wanted_source IS NULL
                    AND wanted_request>0 AND wanted_request<=h.prefix_active_retired)
                OR (wanted_request IS NULL AND wanted_source IS NOT NULL
                    AND wanted_source>0 AND wanted_source<=h.prefix_active_source)))
$$;

CREATE OR REPLACE FUNCTION control.producer_progress_digest(h control.producer_checkpoints)
RETURNS bytea LANGUAGE plpgsql STABLE STRICT SET search_path=pg_catalog AS $$
DECLARE jobs integer; dependencies integer; job_bytes bytea; dependency_bytes bytea;
BEGIN
    SELECT count(*) INTO jobs FROM (SELECT 1 FROM control.acquisition_progress
        WHERE producer_slot_id=h.producer_slot_id AND producer_epoch=h.producer_epoch LIMIT 65) b;
    SELECT count(*) INTO dependencies FROM (SELECT 1 FROM control.producer_checkpoint_dependencies
        WHERE producer_slot_id=h.producer_slot_id AND producer_epoch=h.producer_epoch LIMIT 16385) b;
    IF (h.progress_format IS DISTINCT FROM 1 AND h.progress_format IS DISTINCT FROM 2) OR jobs>64 OR dependencies>16384
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
    RETURN sha256(convert_to(CASE WHEN h.progress_format=2 THEN 'bigbrotr/producer-progress/2'
        ELSE 'bigbrotr/producer-progress/1' END,'UTF8') || decode('00','hex')
        || h.dataset_id || h.producer_slot_id || control.command_u64_bytes(h.producer_epoch)
        || h.manifest_sha256 || CASE WHEN h.progress_format=2 THEN
            control.command_u64_bytes(h.prefix_active_generation)||h.prefix_active_sha256 ELSE ''::bytea END
        || control.command_u64_bytes(h.checkpoint_revision)
        || control.command_u64_bytes(h.complete_through_request)
        || control.command_u64_bytes(jobs) || control.command_u64_bytes(dependencies)
        || job_bytes || dependency_bytes);
END $$;

CREATE OR REPLACE FUNCTION control.require_producer_progress_capacity(
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
    IF retained + additional_input + (CASE WHEN EXISTS(SELECT 1 FROM control.producer_checkpoints
            WHERE producer_slot_id=wanted_slot AND producer_epoch=wanted_epoch AND prefix_profile)
            OR (wanted_epoch=1 AND NOT EXISTS(SELECT 1 FROM control.producer_checkpoints
                WHERE producer_slot_id=wanted_slot AND producer_epoch=wanted_epoch))
            THEN 32768 ELSE 0 END) + 1024 + 1024::bigint*wanted_jobs
            + 256::bigint*wanted_dependencies > 33554432 THEN
        RAISE EXCEPTION 'producer progress and input/completion exceed retained capacity' USING ERRCODE='23514';
    END IF;
END $$;

CREATE OR REPLACE FUNCTION control.require_retained_producer_progress(wanted_slot bytea,wanted_epoch numeric)
RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE producer control.producer_epochs%ROWTYPE; h control.producer_checkpoints%ROWTYPE;
        retired numeric; tail numeric;
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
    retired:=coalesce(h.prefix_active_retired,0);
    tail:=h.complete_through_request-retired;
    IF h.prefix_active_bytes IS NOT NULL THEN
        PERFORM control.require_producer_prefix_base(h,h.prefix_active_bytes);
    END IF;
    IF h.prefix_prepared_bytes IS NOT NULL THEN
        PERFORM control.require_producer_prefix_base(h,h.prefix_prepared_bytes);
    END IF;
    IF (h.progress_format IS DISTINCT FROM 1 AND h.progress_format IS DISTINCT FROM 2) OR h.dataset_id<>producer.dataset_id
        OR h.complete_through_request>producer.reserved_through_request
        OR tail NOT BETWEEN 0 AND 64 OR producer.reserved_through_request-retired NOT BETWEEN 0 AND 64
        OR (NOT h.prefix_profile AND producer.reserved_through_request>64)
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
    IF EXISTS(SELECT 1 FROM control.producer_requests
            WHERE producer_slot_id=wanted_slot AND producer_epoch=wanted_epoch
                AND request_sequence<=retired)
        OR EXISTS(SELECT 1 FROM control.producer_outcomes
            WHERE producer_slot_id=wanted_slot AND producer_epoch=wanted_epoch
                AND request_sequence<=retired)
        OR EXISTS(SELECT 1 FROM generate_series(1,tail::bigint) part(delta)
        CROSS JOIN LATERAL (SELECT retired+part.delta AS request_number) absolute
        LEFT JOIN control.producer_requests r ON r.producer_slot_id=wanted_slot
            AND r.producer_epoch=wanted_epoch AND r.request_sequence=absolute.request_number
        LEFT JOIN control.producer_outcomes o ON o.producer_slot_id=wanted_slot
            AND o.producer_epoch=wanted_epoch AND o.request_sequence=absolute.request_number
        WHERE r.request_sequence IS NULL OR o.request_sequence IS NULL
            OR r.dataset_id<>h.dataset_id OR o.dataset_id<>h.dataset_id
            OR r.manifest_sha256<>h.manifest_sha256 OR o.manifest_sha256<>h.manifest_sha256
            OR r.request_digest<>o.request_digest)
        OR EXISTS(SELECT 1 FROM control.acquisition_progress p
            WHERE p.producer_slot_id=wanted_slot AND p.producer_epoch=wanted_epoch
                AND (p.dataset_id<>h.dataset_id OR p.mutation_request>h.complete_through_request))
        OR EXISTS(SELECT 1 FROM control.producer_checkpoint_dependencies d
            WHERE d.producer_slot_id=wanted_slot AND d.producer_epoch=wanted_epoch
                AND (d.dataset_id<>h.dataset_id OR (h.prefix_profile AND
                    (d.source_epoch<>1 OR d.source_sequence<=coalesce(h.prefix_active_source,0))))) THEN
        RAISE EXCEPTION 'retained progress lost its original contiguous outcome closure' USING ERRCODE='23514';
    END IF;
    PERFORM control.require_producer_progress_capacity(wanted_slot,wanted_epoch,
        0,h.progress_jobs,h.dependency_count);
END $$;

CREATE OR REPLACE FUNCTION control.require_producer_progress_children() RETURNS trigger
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
    IF (head.progress_format IS DISTINCT FROM 1 AND head.progress_format IS DISTINCT FROM 2)
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

CREATE OR REPLACE FUNCTION control.reserve_producer_outcome_capacity() RETURNS trigger LANGUAGE plpgsql
SET search_path=pg_catalog,control AS $$
DECLARE retained_bytes bigint; metadata bigint;
BEGIN
    PERFORM 1 FROM control.producer_epochs WHERE producer_slot_id=NEW.producer_slot_id
        AND producer_epoch=NEW.producer_epoch FOR UPDATE;
    SELECT coalesce(sum(input_bytes::bigint+65536),0) INTO retained_bytes
        FROM control.producer_requests WHERE producer_slot_id=NEW.producer_slot_id
            AND producer_epoch=NEW.producer_epoch;
    SELECT 1024+1024::bigint*progress_jobs+256::bigint*dependency_count
            +CASE WHEN prefix_profile THEN 32768 ELSE 0 END INTO metadata
        FROM control.producer_checkpoints WHERE producer_slot_id=NEW.producer_slot_id
            AND producer_epoch=NEW.producer_epoch AND progress_format IN (1,2);
    IF retained_bytes+NEW.input_bytes+65536+coalesce(metadata,CASE WHEN NEW.producer_epoch=1 THEN 32768 ELSE 0 END)>33554432 THEN
        RAISE EXCEPTION 'producer reservation must include retained progress and outcome capacity'
            USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;

CREATE OR REPLACE FUNCTION control.guard_producer_checkpoint() RETURNS trigger LANGUAGE plpgsql
SET search_path=pg_catalog,control AS $$
DECLARE producer control.producer_epochs%ROWTYPE;
        jobs integer; dependencies integer; retired numeric; table_owner name;
BEGIN
    IF TG_OP='DELETE' THEN
        RAISE EXCEPTION 'producer checkpoints are retained through epoch retirement' USING ERRCODE='23514';
    END IF;
    SELECT * INTO STRICT producer FROM control.producer_epochs
        WHERE dataset_id=NEW.dataset_id AND producer_slot_id=NEW.producer_slot_id
            AND producer_epoch=NEW.producer_epoch FOR UPDATE;
    SELECT pg_catalog.pg_get_userbyid(relowner) INTO STRICT table_owner
        FROM pg_catalog.pg_class WHERE oid='control.producer_checkpoints'::regclass;
    IF TG_OP='UPDATE' AND OLD.prefix_prepared_bytes IS NOT NULL THEN
        -- First and last updates of the owner-only atomic finalizer. Ordinary
        -- checkpoint writers cannot promote a slot or clear an unresolved one.
        IF current_user<>table_owner THEN
            RAISE EXCEPTION 'prepared prefix requires exact finalization' USING ERRCODE='23514';
        END IF;
        IF NEW.prefix_active_bytes=OLD.prefix_prepared_bytes
            AND NEW.prefix_active_sha256=OLD.prefix_prepared_sha256
            AND NEW.prefix_active_generation=OLD.prefix_prepared_generation
            AND NEW.prefix_active_retired=OLD.prefix_prepared_retired
            AND NEW.prefix_active_source=OLD.prefix_prepared_source
            AND NEW.progress_format=2
            AND (to_jsonb(NEW)-ARRAY['prefix_active_bytes','prefix_active_sha256',
                'prefix_active_generation','prefix_active_retired','prefix_active_source','progress_format'])
                = (to_jsonb(OLD)-ARRAY['prefix_active_bytes','prefix_active_sha256',
                'prefix_active_generation','prefix_active_retired','prefix_active_source','progress_format']) THEN
            RETURN NEW;
        END IF;
        IF OLD.prefix_active_bytes=OLD.prefix_prepared_bytes
            AND NEW.prefix_prepared_bytes IS NULL
            AND (to_jsonb(NEW)-ARRAY['prefix_prepared_bytes','prefix_prepared_sha256',
                'prefix_prepared_generation','prefix_prepared_retired','prefix_prepared_source',
                'dependency_count','checkpoint_digest'])
                = (to_jsonb(OLD)-ARRAY['prefix_prepared_bytes','prefix_prepared_sha256',
                'prefix_prepared_generation','prefix_prepared_retired','prefix_prepared_source',
                'dependency_count','checkpoint_digest'])
            AND NEW.checkpoint_digest=control.producer_progress_digest(NEW) THEN
            RETURN NEW;
        END IF;
        RAISE EXCEPTION 'prefix finalization cannot change captured progress' USING ERRCODE='23514';
    END IF;
    IF TG_OP='UPDATE' AND NEW.prefix_prepared_bytes IS NOT NULL THEN
        IF NOT OLD.prefix_profile OR OLD.prefix_prepared_bytes IS NOT NULL
            OR (to_jsonb(NEW)-ARRAY['prefix_prepared_bytes','prefix_prepared_sha256',
                'prefix_prepared_generation','prefix_prepared_retired','prefix_prepared_source'])
                IS DISTINCT FROM (to_jsonb(OLD)-ARRAY['prefix_prepared_bytes','prefix_prepared_sha256',
                'prefix_prepared_generation','prefix_prepared_retired','prefix_prepared_source']) THEN
            RAISE EXCEPTION 'prepare preserves the exact captured head' USING ERRCODE='23514';
        END IF;
        PERFORM control.validate_prepared_producer_prefix(OLD,NEW.prefix_prepared_bytes,
            NEW.prefix_prepared_sha256);
        IF ROW(NEW.prefix_prepared_generation,NEW.prefix_prepared_retired,NEW.prefix_prepared_source)
            IS DISTINCT FROM ROW(control.producer_prefix_u64(NEW.prefix_prepared_bytes,99),
                control.producer_prefix_u64(NEW.prefix_prepared_bytes,139),
                control.producer_prefix_u64(NEW.prefix_prepared_bytes,147)) THEN
            RAISE EXCEPTION 'prepared slot typed coordinates differ' USING ERRCODE='23514';
        END IF;
        RETURN NEW;
    END IF;
    IF TG_OP='INSERT' THEN
        IF NEW.prefix_active_bytes IS NOT NULL OR NEW.prefix_prepared_bytes IS NOT NULL
            OR (NEW.prefix_profile AND (NEW.producer_epoch<>1 OR NEW.progress_format IS DISTINCT FROM 1)) THEN
            RAISE EXCEPTION 'first progress cannot import a base' USING ERRCODE='23514';
        END IF;
    ELSIF ROW(NEW.prefix_profile,NEW.prefix_active_bytes,NEW.prefix_active_sha256,
            NEW.prefix_active_generation,NEW.prefix_active_retired,NEW.prefix_active_source)
        IS DISTINCT FROM ROW(OLD.prefix_profile,OLD.prefix_active_bytes,OLD.prefix_active_sha256,
            OLD.prefix_active_generation,OLD.prefix_active_retired,OLD.prefix_active_source) THEN
        RAISE EXCEPTION 'ordinary progress cannot replace prefix authority' USING ERRCODE='23514';
    END IF;
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
    retired:=coalesce(NEW.prefix_active_retired,0);
    IF NEW.complete_through_request>producer.reserved_through_request
        OR NEW.complete_through_request-retired NOT BETWEEN 0 AND 64
        OR (NOT NEW.prefix_profile AND NEW.complete_through_request>64) THEN
        RAISE EXCEPTION 'checkpoint exceeds bounded reserved requests' USING ERRCODE='23514';
    END IF;
    IF EXISTS (
        SELECT 1 FROM generate_series(1,(NEW.complete_through_request-retired)::bigint) part(delta)
        CROSS JOIN LATERAL (SELECT retired+part.delta AS request_number) absolute
        LEFT JOIN control.producer_outcomes o ON o.producer_slot_id=NEW.producer_slot_id
            AND o.producer_epoch=NEW.producer_epoch AND o.request_sequence=absolute.request_number
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

CREATE OR REPLACE FUNCTION control.guard_producer_request() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
DECLARE producer control.producer_epochs%ROWTYPE;
        retained_bytes bigint;
        progress control.producer_checkpoints%ROWTYPE;
        complete_through numeric := 0;
BEGIN
    IF TG_OP='DELETE' AND control.producer_prefix_deletion(OLD.producer_slot_id,
            OLD.producer_epoch,OLD.dataset_id,OLD.request_sequence,NULL) THEN RETURN OLD; END IF;
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
        OR NEW.request_sequence > 64 THEN
        RAISE EXCEPTION 'reservation requires the next request within the retained history bound'
            USING ERRCODE = '23514';
    END IF;
    -- This producer lock also serializes progress publication. Only a validated
    -- typed head contributes coordination C. Opaque legacy history keeps C=0;
    -- the native owner independently requires observed A before reusing credit.
    SELECT * INTO progress FROM control.producer_checkpoints
        WHERE producer_slot_id = NEW.producer_slot_id AND producer_epoch = NEW.producer_epoch;
    IF NOT FOUND THEN
        PERFORM control.require_retained_producer_progress(NEW.producer_slot_id, NEW.producer_epoch);
    ELSIF progress.progress_format IN (1,2) THEN
        PERFORM control.require_retained_producer_progress(NEW.producer_slot_id, NEW.producer_epoch);
        IF progress.manifest_sha256 <> NEW.manifest_sha256 THEN
            RAISE EXCEPTION 'reservation differs from its typed progress native context'
                USING ERRCODE = '23514';
        END IF;
        complete_through := progress.complete_through_request;
    END IF;
    IF progress.prefix_prepared_bytes IS NOT NULL
        OR NEW.request_sequence<=coalesce(progress.prefix_active_retired,0)
        OR NEW.request_sequence-coalesce(progress.prefix_active_retired,0)>64
        OR NEW.request_sequence > complete_through + producer.outstanding_request_limit THEN
        RAISE EXCEPTION 'reservation exceeds its classified coordination window'
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

CREATE OR REPLACE FUNCTION control.guard_producer_outcome() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
DECLARE producer control.producer_epochs%ROWTYPE;
BEGIN
    IF TG_OP='DELETE' AND control.producer_prefix_deletion(OLD.producer_slot_id,
            OLD.producer_epoch,OLD.dataset_id,OLD.request_sequence,NULL) THEN RETURN OLD; END IF;
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

-- PostgreSQL validates immutable originals and the captured head independently
-- of the adapter. Complete native science/reference closure remains mandatory.
CREATE FUNCTION control.validate_prepared_producer_prefix(h control.producer_checkpoints,
    b bytea,wanted_sha bytea)
RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE p control.producer_epochs%ROWTYPE; row_record record; old_cut numeric;
        next_request numeric; rows_seen integer:=0; requests bytea; jobs bytea;
        old_source numeric; new_source numeric; generation numeric;
BEGIN
    PERFORM control.require_producer_prefix_base(h,b);
    IF wanted_sha IS DISTINCT FROM sha256(b) OR h.prefix_prepared_bytes IS NOT NULL
        OR h.complete_through_request<>control.producer_prefix_u64(b,139)
        OR h.checkpoint_revision<>control.producer_prefix_u64(b,277)
        OR int2send(h.progress_format)<>substring(b FROM 276 FOR 2)
        OR h.checkpoint_digest<>substring(b FROM 286 FOR 32) THEN
        RAISE EXCEPTION 'prepare differs from the exact current checkpoint' USING ERRCODE='23514';
    END IF;
    PERFORM control.require_control_owner(h.dataset_id,substring(b FROM 156 FOR 16),
        control.producer_prefix_u64(b,171),substring(b FROM 180 FOR 16));
    IF NOT EXISTS(SELECT 1 FROM control.datasets WHERE dataset_id=h.dataset_id
        AND policy_revision=control.producer_prefix_u64(b,195)) THEN
        RAISE EXCEPTION 'prepare policy differs from current accepted policy' USING ERRCODE='23514';
    END IF;
    SELECT * INTO STRICT p FROM control.producer_epochs
        WHERE producer_slot_id=h.producer_slot_id AND producer_epoch=h.producer_epoch FOR UPDATE;
    IF p.state NOT IN ('open','closing') OR p.owner_epoch<>1 OR p.producer_role<>'ingest'
        OR p.reserved_through_request<>h.complete_through_request
        OR EXISTS(SELECT 1 FROM control.producer_epochs
            WHERE producer_slot_id=h.producer_slot_id AND producer_epoch<>1)
        OR EXISTS(SELECT 1 FROM control.initial_ingest_successions WHERE owner_resource_id=p.owner_resource_id)
        OR EXISTS(SELECT 1 FROM control.initial_ingest_activations WHERE owner_resource_id=p.owner_resource_id)
        OR EXISTS(SELECT 1 FROM control.ingest_successions WHERE owner_resource_id=p.owner_resource_id)
        OR EXISTS(SELECT 1 FROM control.ingest_activations WHERE owner_resource_id=p.owner_resource_id)
        OR EXISTS(SELECT 1 FROM control.producer_input_bindings
            WHERE producer_slot_id=h.producer_slot_id AND producer_epoch=1) THEN
        RAISE EXCEPTION 'prefix prepare requires the complete connected initial owner' USING ERRCODE='23514';
    END IF;
    PERFORM control.require_retained_producer_progress(h.producer_slot_id,1);
    old_cut:=coalesce(h.prefix_active_retired,0);
    old_source:=coalesce(h.prefix_active_source,0);
    new_source:=control.producer_prefix_u64(b,147);
    generation:=control.producer_prefix_u64(b,99);
    IF h.complete_through_request-old_cut NOT BETWEEN 1 AND 64
        OR generation<>coalesce(h.prefix_active_generation,0)+1
        OR substring(b FROM 108 FOR 32)<>coalesce(h.prefix_active_sha256,decode(repeat('00',32),'hex'))
        OR new_source<old_source
        OR (h.prefix_active_bytes IS NOT NULL AND
            (control.producer_prefix_u64(b,203)<control.producer_prefix_u64(h.prefix_active_bytes,203)
                OR (new_source=old_source AND substring(b FROM 204 FOR 40)
                    <>substring(h.prefix_active_bytes FROM 204 FOR 40)))) THEN
        RAISE EXCEPTION 'prefix base is not the exact monotone successor' USING ERRCODE='23514';
    END IF;
    SELECT coalesce(string_agg(control.acquisition_progress_bytes(a),''::bytea ORDER BY job_id),''::bytea)
        INTO jobs FROM control.acquisition_progress a
        WHERE producer_slot_id=h.producer_slot_id AND producer_epoch=1;
    IF octet_length(jobs)<>216*h.progress_jobs
        OR control.producer_prefix_u64(b,317)<>h.progress_jobs
        OR substring(b FROM 326)<>jobs THEN
        RAISE EXCEPTION 'base must capture every exact current job head' USING ERRCODE='23514';
    END IF;
    requests:=coalesce(substring(h.prefix_active_bytes FROM 244 FOR 32),sha256(
        convert_to('bigbrotr/producer-prefix-requests/1','UTF8')||decode('00','hex')
            ||substring(b FROM 28 FOR 72)));
    next_request:=old_cut;
    FOR row_record IN
        SELECT r.*,o.outcome_bytes,o.outcome_sha256,o.outcome_kind,
            o.request_digest AS copied_digest,o.manifest_sha256 AS copied_manifest,
            o.dataset_id AS copied_dataset
        FROM control.producer_requests r LEFT JOIN control.producer_outcomes o
            USING(producer_slot_id,producer_epoch,request_sequence)
        WHERE r.producer_slot_id=h.producer_slot_id AND r.producer_epoch=1
        ORDER BY r.request_sequence LIMIT 65
    LOOP
        next_request:=next_request+1;
        rows_seen:=rows_seen+1;
        IF rows_seen>64 OR row_record.request_sequence<>next_request
            OR next_request>h.complete_through_request
            OR row_record.dataset_id<>h.dataset_id OR row_record.manifest_sha256<>h.manifest_sha256
            OR row_record.outcome_bytes IS NULL
            OR row_record.copied_dataset IS DISTINCT FROM row_record.dataset_id
            OR row_record.copied_digest IS DISTINCT FROM row_record.request_digest
            OR row_record.copied_manifest IS DISTINCT FROM row_record.manifest_sha256
            OR row_record.outcome_sha256 IS DISTINCT FROM sha256(row_record.outcome_bytes) THEN
            RAISE EXCEPTION 'prefix prepare requires all exact original outcomes' USING ERRCODE='23514';
        END IF;
        requests:=sha256(convert_to('bigbrotr/producer-prefix-request/1','UTF8')||decode('00','hex')
            ||requests||control.command_u64_bytes(next_request)
            ||row_record.request_digest||row_record.outcome_sha256);
    END LOOP;
    IF next_request<>h.complete_through_request OR requests<>substring(b FROM 244 FOR 32) THEN
        RAISE EXCEPTION 'prefix request chain differs from complete retained originals' USING ERRCODE='23514';
    END IF;
END $$;

CREATE FUNCTION control.prepare_producer_prefix(wanted_slot bytea,b bytea,wanted_sha bytea)
RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE h control.producer_checkpoints%ROWTYPE;
BEGIN
    IF current_setting('transaction_isolation')<>'read committed' THEN
        RAISE EXCEPTION 'prefix prepare requires READ COMMITTED' USING ERRCODE='25001';
    END IF;
    PERFORM 1 FROM control.producer_epochs
        WHERE producer_slot_id=wanted_slot AND producer_epoch=1 FOR UPDATE;
    SELECT * INTO STRICT h FROM control.producer_checkpoints
        WHERE producer_slot_id=wanted_slot AND producer_epoch=1 FOR UPDATE;
    PERFORM control.validate_prepared_producer_prefix(h,b,wanted_sha);
    UPDATE control.producer_checkpoints SET prefix_prepared_bytes=b,prefix_prepared_sha256=wanted_sha,
        prefix_prepared_generation=control.producer_prefix_u64(b,99),
        prefix_prepared_retired=control.producer_prefix_u64(b,139),
        prefix_prepared_source=control.producer_prefix_u64(b,147)
        WHERE producer_slot_id=wanted_slot AND producer_epoch=1
            AND checkpoint_revision=h.checkpoint_revision AND progress_format=h.progress_format
            AND checkpoint_digest=h.checkpoint_digest AND prefix_prepared_bytes IS NULL
            AND prefix_active_sha256 IS NOT DISTINCT FROM h.prefix_active_sha256
            AND prefix_active_generation IS NOT DISTINCT FROM h.prefix_active_generation;
    IF NOT FOUND THEN RAISE EXCEPTION 'prefix prepare CAS changed' USING ERRCODE='23514'; END IF;
END $$;

CREATE FUNCTION control.guard_producer_prefix_dependency() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
    IF TG_OP='DELETE' AND control.producer_prefix_deletion(OLD.producer_slot_id,
        OLD.producer_epoch,OLD.dataset_id,NULL,OLD.source_sequence)
        AND OLD.source_epoch=1
        AND EXISTS(SELECT 1 FROM control.producer_epochs p JOIN control.source_streams s
            ON s.dataset_id=p.dataset_id AND s.owner_resource_id=p.owner_resource_id
                AND s.owner_epoch=p.owner_epoch
            WHERE p.producer_slot_id=OLD.producer_slot_id AND p.producer_epoch=OLD.producer_epoch
                AND s.shard_id=OLD.source_shard) THEN RETURN OLD; END IF;
    RAISE EXCEPTION 'checkpoint dependency is immutable outside exact prefix finalization'
        USING ERRCODE='23514';
END $$;
DROP TRIGGER producer_checkpoint_dependencies_immutable ON control.producer_checkpoint_dependencies;
CREATE TRIGGER producer_checkpoint_dependencies_immutable BEFORE UPDATE OR DELETE
    ON control.producer_checkpoint_dependencies FOR EACH ROW
    EXECUTE FUNCTION control.guard_producer_prefix_dependency();

-- The narrow application role has no DELETE authority. Its typed connected
-- finalizer calls this only with a fresh private post-unlink native witness.
-- SECURITY DEFINER supplies table-owner privilege, never current Control identity.
CREATE FUNCTION control.finalize_producer_prefix(wanted_slot bytea,wanted_sha bytea,
    wanted_requests numeric,current_resource bytea,current_epoch numeric,current_incarnation bytea)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE h control.producer_checkpoints%ROWTYPE; promoted control.producer_checkpoints%ROWTYPE;
        b bytea; old_cut numeric; deleted bigint; dependencies bigint;
BEGIN
    IF current_setting('transaction_isolation')<>'read committed' THEN
        RAISE EXCEPTION 'prefix finalization requires READ COMMITTED' USING ERRCODE='25001';
    END IF;
    -- Read identity before taking the current Control lock; recheck under the
    -- same producer serialization used by all progress/prepare writers.
    SELECT * INTO STRICT h FROM control.producer_checkpoints
        WHERE producer_slot_id=wanted_slot AND producer_epoch=1;
    PERFORM control.require_control_owner(h.dataset_id,current_resource,current_epoch,current_incarnation);
    PERFORM 1 FROM control.producer_epochs
        WHERE producer_slot_id=wanted_slot AND producer_epoch=1 FOR UPDATE;
    SELECT * INTO STRICT h FROM control.producer_checkpoints
        WHERE producer_slot_id=wanted_slot AND producer_epoch=1 FOR UPDATE;
    b:=h.prefix_prepared_bytes;
    old_cut:=coalesce(h.prefix_active_retired,0);
    IF b IS NULL OR wanted_sha IS DISTINCT FROM h.prefix_prepared_sha256
        OR wanted_sha IS DISTINCT FROM sha256(b)
        OR h.prefix_prepared_retired-old_cut NOT BETWEEN 1 AND 64
        OR wanted_requests IS DISTINCT FROM h.prefix_prepared_retired-old_cut
        OR h.checkpoint_revision<>control.producer_prefix_u64(b,277)
        OR h.checkpoint_digest<>substring(b FROM 286 FOR 32)
        OR int2send(h.progress_format)<>substring(b FROM 276 FOR 2) THEN
        RAISE EXCEPTION 'finalization differs from the exact prepared head' USING ERRCODE='23514';
    END IF;
    -- Recheck the complete historical originals, jobs and request chain. The
    -- candidate's old issuer need not still be live after fenced recovery.
    PERFORM control.require_retained_producer_progress(wanted_slot,1);
    IF (SELECT count(*) FROM control.producer_requests
            WHERE producer_slot_id=wanted_slot AND producer_epoch=1)<>wanted_requests
        OR (SELECT count(*) FROM control.producer_outcomes
            WHERE producer_slot_id=wanted_slot AND producer_epoch=1)<>wanted_requests
        OR EXISTS(SELECT 1 FROM control.producer_input_bindings
            WHERE producer_slot_id=wanted_slot AND producer_epoch=1) THEN
        RAISE EXCEPTION 'finalization detail count or obligations changed' USING ERRCODE='23514';
    END IF;
    SELECT count(*) INTO dependencies FROM control.producer_checkpoint_dependencies d
        JOIN control.producer_epochs p USING(producer_slot_id,producer_epoch,dataset_id)
        JOIN control.source_streams s ON s.dataset_id=p.dataset_id
            AND s.owner_resource_id=p.owner_resource_id AND s.owner_epoch=p.owner_epoch
        WHERE d.producer_slot_id=wanted_slot AND d.producer_epoch=1
            AND d.source_shard=s.shard_id AND d.source_epoch=1
            AND d.source_sequence<=h.prefix_prepared_source;
    UPDATE control.producer_checkpoints SET progress_format=2,
        prefix_active_bytes=prefix_prepared_bytes,prefix_active_sha256=prefix_prepared_sha256,
        prefix_active_generation=prefix_prepared_generation,prefix_active_retired=prefix_prepared_retired,
        prefix_active_source=prefix_prepared_source WHERE producer_slot_id=wanted_slot AND producer_epoch=1;
    DELETE FROM control.producer_checkpoint_dependencies d
        USING control.producer_epochs p,control.source_streams s
        WHERE p.producer_slot_id=d.producer_slot_id AND p.producer_epoch=d.producer_epoch
            AND s.dataset_id=p.dataset_id AND s.owner_resource_id=p.owner_resource_id AND s.owner_epoch=p.owner_epoch
            AND d.producer_slot_id=wanted_slot AND d.producer_epoch=1
            AND d.source_shard=s.shard_id AND d.source_epoch=1
            AND d.source_sequence<=h.prefix_prepared_source;
    GET DIAGNOSTICS deleted=ROW_COUNT;
    IF deleted<>dependencies THEN RAISE EXCEPTION 'prefix dependency deletion changed' USING ERRCODE='23514'; END IF;
    DELETE FROM control.producer_outcomes WHERE producer_slot_id=wanted_slot AND producer_epoch=1
        AND request_sequence>old_cut AND request_sequence<=h.prefix_prepared_retired;
    GET DIAGNOSTICS deleted=ROW_COUNT;
    IF deleted<>wanted_requests THEN RAISE EXCEPTION 'prefix outcome deletion changed' USING ERRCODE='23514'; END IF;
    DELETE FROM control.producer_requests WHERE producer_slot_id=wanted_slot AND producer_epoch=1
        AND request_sequence>old_cut AND request_sequence<=h.prefix_prepared_retired;
    GET DIAGNOSTICS deleted=ROW_COUNT;
    IF deleted<>wanted_requests THEN RAISE EXCEPTION 'prefix request deletion changed' USING ERRCODE='23514'; END IF;
    SELECT * INTO STRICT promoted FROM control.producer_checkpoints
        WHERE producer_slot_id=wanted_slot AND producer_epoch=1;
    promoted.dependency_count:=h.dependency_count-dependencies;
    promoted.checkpoint_digest:=control.producer_progress_digest(promoted);
    UPDATE control.producer_checkpoints SET dependency_count=promoted.dependency_count,
        checkpoint_digest=promoted.checkpoint_digest,prefix_prepared_bytes=NULL,prefix_prepared_sha256=NULL,
        prefix_prepared_generation=NULL,prefix_prepared_retired=NULL,prefix_prepared_source=NULL
        WHERE producer_slot_id=wanted_slot AND producer_epoch=1
            AND prefix_active_sha256=wanted_sha AND prefix_prepared_sha256=wanted_sha
            AND checkpoint_revision=h.checkpoint_revision;
    IF NOT FOUND THEN RAISE EXCEPTION 'prefix promotion CAS changed' USING ERRCODE='23514'; END IF;
    PERFORM control.require_retained_producer_progress(wanted_slot,1);
    PERFORM control.require_control_owner(h.dataset_id,current_resource,current_epoch,current_incarnation);
END $$;

REVOKE ALL ON FUNCTION control.producer_prefix_u64(bytea,integer),
    control.require_producer_prefix_base(control.producer_checkpoints,bytea),
    control.producer_prefix_deletion(bytea,numeric,bytea,numeric,numeric),
    control.validate_prepared_producer_prefix(control.producer_checkpoints,bytea,bytea),
    control.prepare_producer_prefix(bytea,bytea,bytea),
    control.finalize_producer_prefix(bytea,bytea,numeric,bytea,numeric,bytea),
    control.guard_producer_prefix_dependency() FROM PUBLIC;
