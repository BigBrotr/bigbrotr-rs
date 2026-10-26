-- Complete initial Index plans. Origin attribution is immutable; publication
-- authority belongs to the current positively fenced Index/Control session.
-- SQL receipts coordinate publication; they do not certify physical CH survival.
ALTER TABLE control.owner_epochs ADD COLUMN index_publication_protocol smallint
    CHECK (index_publication_protocol IS NULL OR
        (index_publication_protocol = 1 AND role = 'index'));
ALTER TABLE control.projection_generations
    ADD COLUMN index_publication_protocol smallint,
    ADD COLUMN custody_binding_digest control.digest32,
    ADD COLUMN provenance text,
    ADD CONSTRAINT index_generation_profile CHECK (
        (index_publication_protocol IS NULL AND custody_binding_digest IS NULL AND provenance IS NULL)
        OR (index_publication_protocol IS NOT NULL AND index_publication_protocol = 1
            AND custody_binding_digest IS NOT NULL AND provenance IS NOT NULL
            AND provenance IN ('local_first_witness', 'off'))),
    ADD CONSTRAINT index_generation_identity UNIQUE
        (generation_id, dataset_id, storage_incarnation, table_identity_digest),
    ADD CONSTRAINT index_generation_resource UNIQUE (generation_id, dataset_id, owner_resource_id);

-- The old FK accidentally made a generation's origin its permanent authority.
-- Retain every original attribution and the separate chunk owner FK; new chunks
-- after recovery carry the new origin, within the same immutable installation.
-- The NULL-protocol guard below retains the original generation/creator association.
DO $$ DECLARE relation_constraint name; BEGIN
    SELECT conname INTO STRICT relation_constraint FROM pg_constraint
        WHERE conrelid = 'control.projection_chunks'::regclass
            AND confrelid = 'control.projection_generations'::regclass AND contype = 'f';
    EXECUTE format('ALTER TABLE control.projection_chunks DROP CONSTRAINT %I', relation_constraint);
END $$;
ALTER TABLE control.projection_chunks ADD CONSTRAINT index_chunk_generation
    FOREIGN KEY (generation_id, dataset_id, storage_incarnation, table_identity_digest)
    REFERENCES control.projection_generations
        (generation_id, dataset_id, storage_incarnation, table_identity_digest);

CREATE TABLE control.index_owner_sessions (
    dataset_id control.id16 NOT NULL,
    resource_id control.id16 NOT NULL,
    owner_epoch control.u64 NOT NULL CHECK (owner_epoch > 0),
    owner_role text NOT NULL DEFAULT 'index' CHECK (owner_role = 'index'),
    owner_incarnation control.id16 NOT NULL,
    generation_id control.id16 NOT NULL,
    issuer_resource control.id16 NOT NULL,
    issuer_epoch control.u64 NOT NULL,
    issuer_role text NOT NULL DEFAULT 'control' CHECK (issuer_role = 'control'),
    issuer_incarnation control.id16 NOT NULL,
    previous_epoch control.u64,
    recovery_digest control.digest32,
    recovered_revision control.u64 NOT NULL,
    recovered_plan control.digest32,
    recovered_pending_plan control.digest32,
    PRIMARY KEY (resource_id, owner_epoch),
    UNIQUE (dataset_id, resource_id, owner_epoch, owner_incarnation, generation_id),
    FOREIGN KEY (dataset_id, resource_id, owner_epoch, owner_role, owner_incarnation)
        REFERENCES control.owner_epochs(dataset_id, resource_id, epoch, role, process_incarnation),
    FOREIGN KEY (generation_id, dataset_id, resource_id)
        REFERENCES control.projection_generations(generation_id, dataset_id, owner_resource_id),
    FOREIGN KEY (dataset_id, issuer_resource, issuer_epoch, issuer_role, issuer_incarnation)
        REFERENCES control.owner_epochs(dataset_id, resource_id, epoch, role, process_incarnation),
    FOREIGN KEY (resource_id, previous_epoch) REFERENCES control.index_owner_sessions(resource_id, owner_epoch),
    CHECK ((owner_epoch = 1 AND previous_epoch IS NULL AND recovery_digest IS NULL
            AND recovered_revision = 0 AND recovered_plan IS NULL AND recovered_pending_plan IS NULL)
        OR (owner_epoch > 1 AND previous_epoch IS NOT NULL
            AND previous_epoch = owner_epoch - 1 AND recovery_digest IS NOT NULL)),
    CHECK ((recovered_revision = 0) = (recovered_plan IS NULL))
);

CREATE FUNCTION control.require_index_owner(wanted_dataset bytea, wanted_resource bytea,
    wanted_epoch numeric, wanted_incarnation bytea) RETURNS void LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
DECLARE attribution control.index_owner_sessions%ROWTYPE;
BEGIN
    SELECT * INTO attribution FROM control.index_owner_sessions
        WHERE dataset_id = wanted_dataset AND resource_id = wanted_resource
            AND owner_epoch = wanted_epoch AND owner_incarnation = wanted_incarnation;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Index owner lacks qualified attribution' USING ERRCODE = '23514';
    END IF;
    PERFORM control.require_control_owner(wanted_dataset, attribution.issuer_resource,
        attribution.issuer_epoch, attribution.issuer_incarnation);
    PERFORM 1 FROM control.owner_epochs o JOIN control.role_instances r USING (role_instance_id)
        WHERE o.dataset_id = wanted_dataset AND o.resource_id = wanted_resource
            AND o.epoch = wanted_epoch AND o.process_incarnation = wanted_incarnation
            AND o.role = 'index' AND o.index_publication_protocol = 1 AND o.state = 'active'
            AND r.retired_at_us IS NULL FOR SHARE OF o, r;
    -- Fresh clocks AFTER all blocking locks; neither expired owner is revived.
    IF NOT FOUND OR NOT EXISTS (SELECT 1 FROM control.owner_epochs
        WHERE resource_id = wanted_resource AND epoch = wanted_epoch
            AND lease_expires_at_us > (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint)
        OR NOT EXISTS (SELECT 1 FROM control.owner_epochs
        WHERE resource_id = attribution.issuer_resource AND epoch = attribution.issuer_epoch
            AND lease_expires_at_us > (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint) THEN
        RAISE EXCEPTION 'Index publication requires unexpired actual owners' USING ERRCODE = '23514';
    END IF;
END;
$$;

CREATE FUNCTION control.require_current_index_owner(wanted_generation bytea)
RETURNS control.index_owner_sessions LANGUAGE plpgsql SET search_path = pg_catalog, control AS $$
DECLARE attribution control.index_owner_sessions%ROWTYPE;
BEGIN
    SELECT s.* INTO STRICT attribution FROM control.index_owner_sessions s
        JOIN control.owner_epochs o ON o.resource_id = s.resource_id AND o.epoch = s.owner_epoch
        WHERE s.generation_id = wanted_generation AND o.state = 'active';
    PERFORM control.require_index_owner(attribution.dataset_id, attribution.resource_id,
        attribution.owner_epoch, attribution.owner_incarnation);
    RETURN attribution;
END;
$$;

CREATE TABLE control.projection_plans (
    generation_id control.id16 NOT NULL,
    plan_digest control.digest32 NOT NULL,
    dataset_id control.id16 NOT NULL,
    consumer_id control.id16 NOT NULL,
    frontier_revision control.u64 NOT NULL CHECK (frontier_revision > 0),
    expected_revision control.u64 NOT NULL,
    topology_generation control.u64 NOT NULL,
    source_shard control.u32 NOT NULL,
    source_owner_epoch control.u64 NOT NULL CHECK (source_owner_epoch > 0),
    source_first control.u64 NOT NULL CHECK (source_first > 0),
    source_last control.u64 NOT NULL CHECK (source_last >= source_first),
    source_batch_digest control.digest32 NOT NULL,
    origin_epoch control.u64 NOT NULL,
    origin_incarnation control.id16 NOT NULL,
    prepared_at_us bigint NOT NULL,
    sealed boolean NOT NULL DEFAULT false,
    PRIMARY KEY (generation_id, plan_digest),
    UNIQUE (generation_id, frontier_revision),
    UNIQUE (consumer_id, frontier_revision),
    UNIQUE (generation_id, plan_digest, dataset_id, topology_generation),
    FOREIGN KEY (generation_id, dataset_id, consumer_id)
        REFERENCES control.projection_generations(generation_id, dataset_id, owner_resource_id),
    FOREIGN KEY (dataset_id, consumer_id, origin_epoch, origin_incarnation, generation_id)
        REFERENCES control.index_owner_sessions(dataset_id, resource_id, owner_epoch, owner_incarnation, generation_id),
    FOREIGN KEY (dataset_id, topology_generation, source_shard, source_owner_epoch)
        REFERENCES control.topology_streams(dataset_id, topology_generation, shard_id, owner_epoch),
    CHECK (frontier_revision = expected_revision + 1)
);

CREATE TABLE control.projection_plan_chunks (
    generation_id control.id16 NOT NULL,
    plan_digest control.digest32 NOT NULL,
    target text COLLATE "C" NOT NULL CHECK (target IN ('dictionaries', 'events',
        'event_eligibility_facts', 'event_receive_facts', 'event_id_index', 'event_author_index',
        'event_time_index', 'event_tag_index', 'provenance_by_event', 'provenance_by_relay')),
    chunk_id control.digest32 NOT NULL,
    partition_key text COLLATE "C" NOT NULL CHECK (partition_key = 'all'),
    rows_digest control.digest32 NOT NULL,
    PRIMARY KEY (generation_id, plan_digest, target),
    UNIQUE (generation_id, chunk_id),
    FOREIGN KEY (generation_id, plan_digest) REFERENCES control.projection_plans(generation_id, plan_digest),
    FOREIGN KEY (generation_id, chunk_id) REFERENCES control.projection_chunks(generation_id, chunk_id)
);

CREATE TABLE control.index_frontier_plans (
    consumer_id control.id16 NOT NULL,
    frontier_revision control.u64 NOT NULL,
    dataset_id control.id16 NOT NULL,
    topology_generation control.u64 NOT NULL,
    generation_id control.id16 NOT NULL,
    plan_digest control.digest32 NOT NULL,
    owner_epoch control.u64 NOT NULL,
    owner_incarnation control.id16 NOT NULL,
    engine_receipt_digest control.digest32 NOT NULL,
    committed_at_us bigint NOT NULL,
    PRIMARY KEY (consumer_id, frontier_revision),
    UNIQUE (generation_id, plan_digest),
    FOREIGN KEY (consumer_id, dataset_id, frontier_revision, topology_generation)
        REFERENCES control.consumer_frontier_revisions(consumer_id, dataset_id, frontier_revision, topology_generation),
    FOREIGN KEY (generation_id, plan_digest, dataset_id, topology_generation)
        REFERENCES control.projection_plans(generation_id, plan_digest, dataset_id, topology_generation),
    FOREIGN KEY (dataset_id, consumer_id, owner_epoch, owner_incarnation, generation_id)
        REFERENCES control.index_owner_sessions(dataset_id, resource_id, owner_epoch, owner_incarnation, generation_id)
);

CREATE FUNCTION control.guard_index_owner_session() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
DECLARE previous control.index_owner_sessions%ROWTYPE; current_revision numeric;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'Index owner attribution is immutable' USING ERRCODE = '23514';
    END IF;
    PERFORM control.require_control_owner(NEW.dataset_id, NEW.issuer_resource,
        NEW.issuer_epoch, NEW.issuer_incarnation);
    PERFORM 1 FROM control.projection_generations WHERE generation_id = NEW.generation_id
        AND index_publication_protocol = 1 AND owner_resource_id = NEW.resource_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Index requires the qualified generation' USING ERRCODE = '23514';
    END IF;
    PERFORM 1 FROM control.owner_epochs WHERE resource_id = NEW.resource_id AND epoch = NEW.owner_epoch
        AND index_publication_protocol = 1 AND state = 'active' FOR SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Index requires its exact live owner' USING ERRCODE = '23514';
    END IF;
    SELECT coalesce(c.current_revision, 0) INTO STRICT current_revision FROM control.consumers c
        WHERE c.consumer_id = NEW.resource_id AND c.dataset_id = NEW.dataset_id AND c.consumer_role = 'index' FOR UPDATE;
    IF NEW.recovered_revision <> current_revision
        OR NEW.recovered_plan IS DISTINCT FROM (SELECT plan_digest FROM control.index_frontier_plans
            WHERE consumer_id = NEW.resource_id AND frontier_revision = current_revision)
        OR NEW.recovered_pending_plan IS DISTINCT FROM (SELECT plan_digest FROM control.projection_plans p
            WHERE p.generation_id = NEW.generation_id AND p.frontier_revision = current_revision + 1)
    THEN
        RAISE EXCEPTION 'Index recovery inventory changed' USING ERRCODE = '23514';
    END IF;
    IF NEW.previous_epoch IS NOT NULL THEN
        SELECT * INTO STRICT previous FROM control.index_owner_sessions
            WHERE resource_id = NEW.resource_id AND owner_epoch = NEW.previous_epoch;
        IF previous.generation_id <> NEW.generation_id OR NOT EXISTS (
            SELECT 1 FROM control.owner_epochs WHERE resource_id = NEW.resource_id
                AND epoch = NEW.previous_epoch AND state = 'released') THEN
            RAISE EXCEPTION 'Index recovery requires positive exclusion of the exact prior owner' USING ERRCODE = '23514';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER index_owner_session_guard BEFORE INSERT OR UPDATE OR DELETE ON control.index_owner_sessions
    FOR EACH ROW EXECUTE FUNCTION control.guard_index_owner_session();
CREATE TRIGGER index_owner_session_truncate_guard BEFORE TRUNCATE ON control.index_owner_sessions
    FOR EACH STATEMENT EXECUTE FUNCTION control.guard_index_owner_session();

CREATE FUNCTION control.require_index_session_live() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
BEGIN
    PERFORM control.require_index_owner(NEW.dataset_id, NEW.resource_id, NEW.owner_epoch, NEW.owner_incarnation);
    RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER index_session_live AFTER INSERT ON control.index_owner_sessions
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION control.require_index_session_live();

CREATE FUNCTION control.require_index_generation_session() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
BEGIN
    IF NEW.index_publication_protocol = 1 THEN
        PERFORM control.require_index_owner(NEW.dataset_id, NEW.owner_resource_id,
            NEW.owner_epoch, NEW.owner_incarnation);
    END IF;
    RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER index_generation_session AFTER INSERT ON control.projection_generations
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION control.require_index_generation_session();

CREATE FUNCTION control.require_index_owner_session() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
BEGIN
    IF NEW.index_publication_protocol = 1 THEN
        PERFORM control.require_index_owner(NEW.dataset_id, NEW.resource_id, NEW.epoch, NEW.process_incarnation);
    END IF;
    RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER index_owner_session_complete AFTER INSERT ON control.owner_epochs
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION control.require_index_owner_session();

CREATE FUNCTION control.guard_projection_plan() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
DECLARE attribution control.index_owner_sessions%ROWTYPE; head numeric;
BEGIN
    IF TG_OP = 'DELETE' OR TG_OP = 'TRUNCATE' THEN
        RAISE EXCEPTION 'Index plans are retained' USING ERRCODE = '23514';
    END IF;
    attribution := control.require_current_index_owner(NEW.generation_id);
    IF TG_OP = 'INSERT' THEN
        SELECT coalesce(current_revision, 0) INTO STRICT head FROM control.consumers
            WHERE consumer_id = NEW.consumer_id AND consumer_role = 'index' FOR UPDATE;
        IF NEW.sealed OR NEW.origin_epoch <> attribution.owner_epoch
            OR NEW.origin_incarnation <> attribution.owner_incarnation
            OR NEW.expected_revision <> head THEN
            RAISE EXCEPTION 'Index plan requires its current owner and next contiguous head' USING ERRCODE = '23514';
        END IF;
    ELSIF OLD.sealed OR NOT NEW.sealed OR
        (to_jsonb(OLD) - 'sealed') IS DISTINCT FROM (to_jsonb(NEW) - 'sealed') THEN
        RAISE EXCEPTION 'Index plans seal once without metadata changes' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER projection_plan_guard BEFORE INSERT OR UPDATE OR DELETE ON control.projection_plans
    FOR EACH ROW EXECUTE FUNCTION control.guard_projection_plan();
CREATE TRIGGER projection_plan_truncate_guard BEFORE TRUNCATE ON control.projection_plans
    FOR EACH STATEMENT EXECUTE FUNCTION control.guard_projection_plan();

CREATE FUNCTION control.guard_projection_plan_chunk() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
DECLARE plan control.projection_plans%ROWTYPE;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'Index plan membership is immutable' USING ERRCODE = '23514';
    END IF;
    SELECT * INTO STRICT plan FROM control.projection_plans
        WHERE generation_id = NEW.generation_id AND plan_digest = NEW.plan_digest FOR UPDATE;
    PERFORM control.require_current_index_owner(NEW.generation_id);
    IF plan.sealed OR NOT EXISTS (SELECT 1 FROM control.projection_chunks c
        WHERE c.generation_id = NEW.generation_id AND c.chunk_id = NEW.chunk_id
            AND c.target = NEW.target AND c.target_schema_version = 1
            AND c.chunk_ordinal = plan.frontier_revision AND c.topology_generation = plan.topology_generation
            AND c.owner_resource_id = plan.consumer_id AND c.owner_epoch = plan.origin_epoch
            AND c.owner_incarnation = plan.origin_incarnation AND c.state = 'prepared') THEN
        RAISE EXCEPTION 'Index membership requires its exact assembling chunk' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER projection_plan_chunk_guard BEFORE INSERT OR UPDATE OR DELETE ON control.projection_plan_chunks
    FOR EACH ROW EXECUTE FUNCTION control.guard_projection_plan_chunk();
CREATE TRIGGER projection_plan_chunk_truncate_guard BEFORE TRUNCATE ON control.projection_plan_chunks
    FOR EACH STATEMENT EXECUTE FUNCTION control.guard_projection_plan_chunk();

CREATE FUNCTION control.require_complete_index_plan(wanted_generation bytea, wanted_plan bytea)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog, control AS $$
DECLARE plan control.projection_plans%ROWTYPE; stream_count integer;
BEGIN
    SELECT * INTO STRICT plan FROM control.projection_plans
        WHERE generation_id = wanted_generation AND plan_digest = wanted_plan;
    IF NOT plan.sealed OR (SELECT count(*) FROM control.projection_plan_chunks
        WHERE generation_id = wanted_generation AND plan_digest = wanted_plan) <> 10
        OR NOT EXISTS (SELECT 1 FROM control.topology_generations WHERE dataset_id = plan.dataset_id
            AND generation = plan.topology_generation AND sealed) THEN
        RAISE EXCEPTION 'Index plan requires a sealed topology and all ten explicit targets' USING ERRCODE = '23514';
    END IF;
    SELECT count(*) INTO stream_count FROM control.topology_streams
        WHERE dataset_id = plan.dataset_id AND topology_generation = plan.topology_generation;
    IF stream_count NOT BETWEEN 1 AND 1024 THEN
        RAISE EXCEPTION 'Index topology exceeds the bounded profile' USING ERRCODE = '23514';
    END IF;
    IF plan.expected_revision > 0 AND NOT EXISTS (SELECT 1 FROM control.consumer_frontier_revisions
        WHERE consumer_id = plan.consumer_id AND frontier_revision = plan.expected_revision
            AND sealed AND topology_generation = plan.topology_generation) THEN
        RAISE EXCEPTION 'Index plan requires the same retained topology' USING ERRCODE = '23514';
    END IF;
    IF plan.source_first <> 1 + coalesce((SELECT last_complete_sequence FROM control.consumer_frontiers
        WHERE consumer_id = plan.consumer_id AND frontier_revision = plan.expected_revision
            AND shard_id = plan.source_shard AND owner_epoch = plan.source_owner_epoch), 0) THEN
        RAISE EXCEPTION 'Index source batch skips or repeats the contiguous prefix' USING ERRCODE = '23514';
    END IF;
    PERFORM 1 FROM control.source_streams s JOIN control.topology_streams t
        USING(dataset_id, shard_id, owner_epoch) WHERE t.dataset_id = plan.dataset_id
            AND t.topology_generation = plan.topology_generation FOR SHARE OF s;
    IF EXISTS (SELECT 1 FROM control.projection_plan_chunks m JOIN control.projection_chunks c
        USING(generation_id, chunk_id) JOIN control.projection_generations g USING(generation_id)
        WHERE m.generation_id = wanted_generation AND m.plan_digest = wanted_plan AND
        (c.target <> m.target OR c.target_schema_version <> 1 OR c.chunk_ordinal <> plan.frontier_revision
            OR c.state = 'invalid' OR c.topology_generation <> plan.topology_generation
            OR (g.provenance = 'off' AND m.target IN ('provenance_by_event','provenance_by_relay') AND c.row_count <> 0)
            OR (SELECT count(*) FROM control.projection_chunk_bounds b
                WHERE b.generation_id = c.generation_id AND b.chunk_id = c.chunk_id) <> stream_count))
        OR EXISTS (SELECT 1 FROM control.projection_plan_chunks m JOIN control.projection_chunk_bounds b
            USING(generation_id, chunk_id) JOIN control.source_streams s
            USING(dataset_id, shard_id, owner_epoch)
            WHERE m.generation_id = wanted_generation AND m.plan_digest = wanted_plan AND
            (b.last_complete_sequence <> CASE WHEN b.shard_id = plan.source_shard
                AND b.owner_epoch = plan.source_owner_epoch THEN plan.source_last ELSE coalesce(
                    (SELECT last_complete_sequence FROM control.consumer_frontiers f
                     WHERE f.consumer_id = plan.consumer_id AND f.frontier_revision = plan.expected_revision
                         AND f.shard_id = b.shard_id AND f.owner_epoch = b.owner_epoch), 0) END
                OR (s.state = 'closed' AND b.last_complete_sequence > s.last_complete_sequence))) THEN
        RAISE EXCEPTION 'Index plan has incompatible rows or incomplete contiguous source bounds' USING ERRCODE = '23514';
    END IF;
END;
$$;

CREATE FUNCTION control.check_complete_index_plan() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
BEGIN
    PERFORM control.require_complete_index_plan(NEW.generation_id, NEW.plan_digest);
    PERFORM control.require_current_index_owner(NEW.generation_id);
    RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER complete_index_plan AFTER INSERT OR UPDATE ON control.projection_plans
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION control.check_complete_index_plan();

CREATE FUNCTION control.guard_index_frontier_plan() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'Index publication receipts are immutable' USING ERRCODE = '23514';
    END IF;
    PERFORM control.require_index_owner(NEW.dataset_id, NEW.consumer_id, NEW.owner_epoch, NEW.owner_incarnation);
    PERFORM control.require_complete_index_plan(NEW.generation_id, NEW.plan_digest);
    IF NOT EXISTS (SELECT 1 FROM control.projection_plans p
        WHERE p.generation_id = NEW.generation_id AND p.plan_digest = NEW.plan_digest
            AND p.consumer_id = NEW.consumer_id AND p.frontier_revision = NEW.frontier_revision)
        OR NOT EXISTS (SELECT 1 FROM control.consumer_frontier_revisions r
        WHERE r.consumer_id = NEW.consumer_id AND r.frontier_revision = NEW.frontier_revision
            AND NOT r.sealed AND r.consumer_role = 'index' AND r.owner_resource_id = NEW.consumer_id
            AND r.owner_epoch = NEW.owner_epoch AND r.owner_incarnation = NEW.owner_incarnation)
        OR EXISTS (SELECT 1 FROM control.projection_plan_chunks m JOIN control.projection_chunks c
            USING(generation_id, chunk_id) WHERE m.generation_id = NEW.generation_id
                AND m.plan_digest = NEW.plan_digest AND (c.state <> 'committed'
                    OR c.engine_receipt_digest <> NEW.engine_receipt_digest
                    OR c.committed_at_us <> NEW.committed_at_us)) THEN
        RAISE EXCEPTION 'Index publication requires all exact completed targets' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER index_frontier_plan_guard BEFORE INSERT OR UPDATE OR DELETE ON control.index_frontier_plans
    FOR EACH ROW EXECUTE FUNCTION control.guard_index_frontier_plan();
CREATE TRIGGER index_frontier_plan_truncate_guard BEFORE TRUNCATE ON control.index_frontier_plans
    FOR EACH STATEMENT EXECUTE FUNCTION control.guard_index_frontier_plan();

CREATE FUNCTION control.require_typed_index_frontier() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
DECLARE receipt control.index_frontier_plans%ROWTYPE;
BEGIN
    IF NEW.consumer_role <> 'index' THEN RETURN NULL; END IF;
    SELECT * INTO STRICT receipt FROM control.index_frontier_plans
        WHERE consumer_id = NEW.consumer_id AND frontier_revision = NEW.frontier_revision;
    PERFORM control.require_index_owner(receipt.dataset_id, receipt.consumer_id,
        receipt.owner_epoch, receipt.owner_incarnation);
    IF NOT EXISTS (SELECT 1 FROM control.consumers WHERE consumer_id = NEW.consumer_id
        AND current_revision = NEW.frontier_revision) OR EXISTS (
        SELECT 1 FROM control.consumer_frontiers f
        WHERE f.consumer_id = NEW.consumer_id AND f.frontier_revision = NEW.frontier_revision
            AND (f.last_complete_sequence IS DISTINCT FROM (SELECT b.last_complete_sequence
                FROM control.projection_plan_chunks m JOIN control.projection_chunk_bounds b
                USING(generation_id, chunk_id) WHERE m.generation_id = receipt.generation_id
                    AND m.plan_digest = receipt.plan_digest AND m.target = 'dictionaries'
                    AND b.shard_id = f.shard_id AND b.owner_epoch = f.owner_epoch)
                OR (f.last_complete_sequence > 0 AND f.attestation_digest <> receipt.plan_digest))) THEN
        RAISE EXCEPTION 'Index frontier lacks its exact complete plan and atomic head' USING ERRCODE = '23514';
    END IF;
    RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER typed_index_frontier AFTER INSERT OR UPDATE ON control.consumer_frontier_revisions
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION control.require_typed_index_frontier();

-- Preserve the original transition rules; qualified generations use the current
-- session, not the immutable creating owner, for every subsequent mutation.
CREATE OR REPLACE FUNCTION control.guard_projection_chunk() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
DECLARE attribution control.index_owner_sessions%ROWTYPE; protocol smallint;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'projection chunk history is retained' USING ERRCODE = '23514';
    END IF;
    IF TG_OP = 'INSERT' THEN
        IF NEW.state <> 'prepared' THEN
            RAISE EXCEPTION 'a projection chunk begins prepared' USING ERRCODE = '23514';
        END IF;
    ELSIF OLD.state <> 'prepared' OR NEW.state NOT IN ('committed', 'invalid')
        OR (to_jsonb(OLD) - ARRAY['state','committed_at_us','engine_receipt_digest','invalid_reason'])
            IS DISTINCT FROM (to_jsonb(NEW) - ARRAY['state','committed_at_us','engine_receipt_digest','invalid_reason']) THEN
        RAISE EXCEPTION 'invalid projection-chunk transition' USING ERRCODE = '23514';
    END IF;
    SELECT index_publication_protocol INTO STRICT protocol FROM control.projection_generations
        WHERE generation_id = NEW.generation_id;
    IF protocol = 1 THEN
        attribution := control.require_current_index_owner(NEW.generation_id);
        IF NEW.state = 'invalid' OR (TG_OP = 'INSERT' AND
            ROW(NEW.owner_resource_id, NEW.owner_epoch, NEW.owner_incarnation) IS DISTINCT FROM
            ROW(attribution.resource_id, attribution.owner_epoch, attribution.owner_incarnation)) THEN
            RAISE EXCEPTION 'qualified Index chunks preserve plans and exact creator attribution' USING ERRCODE = '23514';
        END IF;
    ELSE
        PERFORM 1 FROM control.owner_epochs WHERE dataset_id = NEW.dataset_id
            AND resource_id = NEW.owner_resource_id AND epoch = NEW.owner_epoch AND role = 'index'
            AND process_incarnation = NEW.owner_incarnation AND state IN ('active','draining') FOR SHARE;
        IF NOT FOUND OR (TG_OP = 'INSERT' AND NOT EXISTS (SELECT 1 FROM control.owner_epochs
            WHERE resource_id = NEW.owner_resource_id AND epoch = NEW.owner_epoch AND state = 'active')) THEN
            RAISE EXCEPTION 'projection-chunk mutation requires its exact live Index owner' USING ERRCODE = '23514';
        END IF;
        -- Only qualified sessions can change creator attribution within a
        -- generation. Preserve the owner tuple formerly enforced by the FK.
        IF NOT EXISTS (SELECT 1 FROM control.projection_generations g
            WHERE g.generation_id = NEW.generation_id
                AND ROW(g.owner_resource_id, g.owner_epoch, g.owner_role, g.owner_incarnation)
                    = ROW(NEW.owner_resource_id, NEW.owner_epoch, NEW.owner_role, NEW.owner_incarnation)) THEN
            RAISE EXCEPTION 'unqualified projection chunk owner must match its generation origin'
                USING ERRCODE = '23503';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION control.require_index_chunk_plan() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
DECLARE membership control.projection_plan_chunks%ROWTYPE;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM control.projection_generations
        WHERE generation_id = NEW.generation_id AND index_publication_protocol = 1) THEN RETURN NULL; END IF;
    SELECT * INTO STRICT membership FROM control.projection_plan_chunks
        WHERE generation_id = NEW.generation_id AND chunk_id = NEW.chunk_id;
    PERFORM control.require_complete_index_plan(membership.generation_id, membership.plan_digest);
    IF NEW.state = 'committed' AND NOT EXISTS (SELECT 1 FROM control.index_frontier_plans
        WHERE generation_id = NEW.generation_id AND plan_digest = membership.plan_digest) THEN
        RAISE EXCEPTION 'Index target completion and frontier publish atomically' USING ERRCODE = '23514';
    END IF;
    PERFORM control.require_current_index_owner(NEW.generation_id);
    RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER index_chunk_plan AFTER INSERT OR UPDATE ON control.projection_chunks
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION control.require_index_chunk_plan();

-- Preparation already binds a complete committed native batch. A concurrent
-- closer cannot make that retained batch impossible to publish. The prepare
-- validator locks these source rows before reading their closure bounds.
CREATE FUNCTION control.guard_index_prepared_source_closure() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
BEGIN
    IF NEW.state = 'closed' AND EXISTS (
        SELECT 1 FROM control.projection_chunk_bounds b
        JOIN control.projection_chunks c USING(generation_id,chunk_id)
        JOIN control.projection_generations g USING(generation_id)
        WHERE b.dataset_id = NEW.dataset_id AND b.shard_id = NEW.shard_id
            AND b.owner_epoch = NEW.owner_epoch AND g.index_publication_protocol = 1
            AND c.state = 'prepared' AND b.last_complete_sequence > NEW.last_complete_sequence) THEN
        RAISE EXCEPTION 'source closure is below a retained complete Index plan' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER index_prepared_source_closure BEFORE UPDATE ON control.source_streams
    FOR EACH ROW EXECUTE FUNCTION control.guard_index_prepared_source_closure();

CREATE FUNCTION control.guard_index_protocol() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
BEGIN
    IF NEW.index_publication_protocol IS DISTINCT FROM OLD.index_publication_protocol THEN
        RAISE EXCEPTION 'Index protocol cannot be adopted retrospectively' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER index_protocol_guard BEFORE UPDATE ON control.owner_epochs
    FOR EACH ROW EXECUTE FUNCTION control.guard_index_protocol();

CREATE FUNCTION control.require_index_owner_transfer() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
DECLARE attribution control.index_owner_sessions%ROWTYPE; successor control.index_owner_sessions%ROWTYPE;
BEGIN
    IF NEW.role <> 'index' OR OLD.state = 'released' OR NEW.state <> 'released' THEN RETURN NULL; END IF;
    SELECT * INTO attribution FROM control.index_owner_sessions
        WHERE resource_id = NEW.resource_id AND owner_epoch = NEW.epoch;
    IF FOUND AND EXISTS (SELECT 1 FROM control.projection_chunks
        WHERE generation_id = attribution.generation_id AND state = 'prepared') THEN
        SELECT * INTO STRICT successor FROM control.index_owner_sessions
            WHERE resource_id = NEW.resource_id AND owner_epoch = NEW.epoch + 1
                AND generation_id = attribution.generation_id AND previous_epoch = NEW.epoch
                AND recovery_digest = NEW.exclusion_digest;
        PERFORM control.require_index_owner(successor.dataset_id, successor.resource_id,
            successor.owner_epoch, successor.owner_incarnation);
    ELSIF NOT FOUND AND EXISTS (SELECT 1 FROM control.projection_chunks
        WHERE owner_resource_id = NEW.resource_id AND owner_epoch = NEW.epoch AND state = 'prepared') THEN
        RAISE EXCEPTION 'unqualified Index release requires every prepared chunk to resolve' USING ERRCODE = '23514';
    END IF;
    RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER index_owner_transfer AFTER UPDATE ON control.owner_epochs
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION control.require_index_owner_transfer();

-- Keep all non-Index release guards unchanged. Index prepared work is checked by
-- the deferred atomic-transfer guard above, after the successor session exists.
CREATE OR REPLACE FUNCTION control.guard_owner_ingest_dependents() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
BEGIN
    IF OLD.state <> 'released' AND NEW.state = 'released' THEN
        IF NEW.role = 'ingest' AND (
            EXISTS (SELECT 1 FROM control.allocation_domains
                WHERE dataset_id = NEW.dataset_id AND owner_resource_id = NEW.resource_id
                    AND owner_epoch = NEW.epoch AND owner_incarnation = NEW.process_incarnation AND state = 'active')
            OR EXISTS (SELECT 1 FROM control.source_streams
                WHERE dataset_id = NEW.dataset_id AND owner_resource_id = NEW.resource_id
                    AND owner_epoch = NEW.epoch AND owner_incarnation = NEW.process_incarnation AND state <> 'closed')
            OR EXISTS (SELECT 1 FROM control.producer_epochs
                WHERE dataset_id = NEW.dataset_id AND owner_resource_id = NEW.resource_id
                    AND owner_epoch = NEW.epoch AND owner_incarnation = NEW.process_incarnation AND state <> 'retired')) THEN
            RAISE EXCEPTION 'ingest owner release requires closed domains, streams and producer epochs' USING ERRCODE = '23514';
        END IF;
        IF NEW.role IN ('archive', 'index') AND EXISTS (
            SELECT 1 FROM control.consumer_frontier_revisions AS revision
            JOIN control.consumers AS consumer USING (consumer_id)
            WHERE revision.dataset_id = NEW.dataset_id AND revision.owner_resource_id = NEW.resource_id
                AND revision.owner_epoch = NEW.epoch AND revision.owner_incarnation = NEW.process_incarnation
                AND (consumer.current_revision IS NULL OR revision.frontier_revision > consumer.current_revision)) THEN
            RAISE EXCEPTION 'consumer owner release requires publication of its next sealed frontier' USING ERRCODE = '23514';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

REVOKE ALL ON control.index_owner_sessions, control.projection_plans,
    control.projection_plan_chunks, control.index_frontier_plans FROM PUBLIC;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA control FROM PUBLIC;
