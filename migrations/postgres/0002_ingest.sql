-- BigBrotr ingest coordination, schema compatibility 1.
--
-- PostgreSQL records registered authority, causal checkpoints, exact source cuts
-- and external-engine attestations. It is not a second admission or projection
-- store: a row here does not prove that a RocksDB batch or ClickHouse insert
-- committed. The application verifies those receipts at their owning boundary.

-- Migration 0001 had no source registry, so a populated topology cannot be
-- attributed or backfilled safely. Refuse instead of installing only prospective
-- triggers over silently incompatible existing rows.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM control.topology_streams) THEN
        RAISE EXCEPTION '0002 requires explicit verified source registration for every existing topology stream'
            USING ERRCODE = '55000';
    END IF;
END;
$$;

CREATE TABLE control.allocation_domains (
    dataset_id control.id16 NOT NULL REFERENCES control.datasets(dataset_id),
    dictionary_kind smallint NOT NULL CHECK (dictionary_kind BETWEEN 1 AND 9),
    allocation_domain control.id16 NOT NULL,
    owner_resource_id control.id16 NOT NULL,
    owner_epoch control.u64 NOT NULL CHECK (owner_epoch > 0),
    owner_role text NOT NULL DEFAULT 'ingest' CHECK (owner_role = 'ingest'),
    owner_incarnation control.id16 NOT NULL,
    created_at_us bigint NOT NULL,
    state text NOT NULL DEFAULT 'active' CHECK (state IN ('active', 'closed')),
    closed_at_us bigint,
    closure_digest control.digest32,
    PRIMARY KEY (dataset_id, dictionary_kind, allocation_domain),
    UNIQUE (dataset_id, allocation_domain),
    FOREIGN KEY (dataset_id, owner_resource_id, owner_epoch, owner_role, owner_incarnation)
        REFERENCES control.owner_epochs(dataset_id, resource_id, epoch, role, process_incarnation),
    CHECK ((state = 'closed' AND closed_at_us IS NOT NULL AND closure_digest IS NOT NULL)
        OR (state = 'active' AND closed_at_us IS NULL AND closure_digest IS NULL)),
    CHECK (closed_at_us IS NULL OR closed_at_us >= created_at_us)
);
CREATE UNIQUE INDEX allocation_domains_one_active_per_kind
    ON control.allocation_domains(dataset_id, dictionary_kind, owner_resource_id)
    WHERE state = 'active';

CREATE FUNCTION control.guard_allocation_domain() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'allocation-domain history is retained' USING ERRCODE = '23514';
    END IF;
    IF TG_OP = 'INSERT' THEN
        IF NEW.state <> 'active' THEN
            RAISE EXCEPTION 'an allocation domain begins active' USING ERRCODE = '23514';
        END IF;
    ELSE
        IF ROW(OLD.dataset_id, OLD.dictionary_kind, OLD.allocation_domain,
                OLD.owner_resource_id, OLD.owner_epoch, OLD.owner_role,
                OLD.owner_incarnation, OLD.created_at_us)
            IS DISTINCT FROM
               ROW(NEW.dataset_id, NEW.dictionary_kind, NEW.allocation_domain,
                NEW.owner_resource_id, NEW.owner_epoch, NEW.owner_role,
                NEW.owner_incarnation, NEW.created_at_us)
            OR OLD.state <> 'active' OR NEW.state <> 'closed' THEN
            RAISE EXCEPTION 'allocation-domain identity and closed history are immutable'
                USING ERRCODE = '23514';
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
        RAISE EXCEPTION 'allocation-domain mutation requires its exact live ingest owner'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER allocation_domain_guard BEFORE INSERT OR UPDATE OR DELETE
    ON control.allocation_domains FOR EACH ROW EXECUTE FUNCTION control.guard_allocation_domain();

CREATE TABLE control.source_streams (
    dataset_id control.id16 NOT NULL REFERENCES control.datasets(dataset_id),
    shard_id control.u32 NOT NULL,
    owner_epoch control.u64 NOT NULL CHECK (owner_epoch > 0),
    owner_resource_id control.id16 NOT NULL,
    owner_role text NOT NULL DEFAULT 'ingest' CHECK (owner_role = 'ingest'),
    owner_incarnation control.id16 NOT NULL,
    opened_at_us bigint NOT NULL,
    state text NOT NULL DEFAULT 'open' CHECK (state IN ('open', 'closing', 'closed')),
    closed_at_us bigint,
    last_complete_sequence control.u64,
    closure_digest control.digest32,
    PRIMARY KEY (dataset_id, shard_id, owner_epoch),
    FOREIGN KEY (dataset_id, owner_resource_id, owner_epoch, owner_role, owner_incarnation)
        REFERENCES control.owner_epochs(dataset_id, resource_id, epoch, role, process_incarnation),
    CHECK ((state = 'closed' AND closed_at_us IS NOT NULL
                AND last_complete_sequence IS NOT NULL AND closure_digest IS NOT NULL)
        OR (state <> 'closed' AND closed_at_us IS NULL
                AND last_complete_sequence IS NULL AND closure_digest IS NULL)),
    CHECK (closed_at_us IS NULL OR closed_at_us >= opened_at_us)
);
CREATE UNIQUE INDEX source_streams_one_unclosed_per_shard
    ON control.source_streams(dataset_id, shard_id) WHERE state <> 'closed';

CREATE FUNCTION control.guard_source_stream() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
DECLARE prior control.source_streams%ROWTYPE;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'source-stream history is retained' USING ERRCODE = '23514';
    END IF;
    IF TG_OP = 'INSERT' THEN
        IF NEW.state <> 'open' THEN
            RAISE EXCEPTION 'a source stream begins open' USING ERRCODE = '23514';
        END IF;
        SELECT * INTO prior FROM control.source_streams
            WHERE dataset_id = NEW.dataset_id AND shard_id = NEW.shard_id
            ORDER BY owner_epoch DESC LIMIT 1 FOR UPDATE;
        IF (NOT FOUND AND NEW.owner_epoch <> 1)
            OR (FOUND AND (prior.state <> 'closed'
                OR NEW.owner_epoch <> prior.owner_epoch + 1)) THEN
            RAISE EXCEPTION 'source stream requires the next epoch and closed predecessor'
                USING ERRCODE = '23514';
        END IF;
    ELSE
        IF ROW(OLD.dataset_id, OLD.shard_id, OLD.owner_epoch, OLD.owner_resource_id,
                OLD.owner_role, OLD.owner_incarnation, OLD.opened_at_us)
            IS DISTINCT FROM
               ROW(NEW.dataset_id, NEW.shard_id, NEW.owner_epoch, NEW.owner_resource_id,
                NEW.owner_role, NEW.owner_incarnation, NEW.opened_at_us)
            OR OLD.state = 'closed'
            OR NOT ((OLD.state = 'open' AND NEW.state = 'closing')
                OR (OLD.state = 'closing' AND NEW.state = 'closed')) THEN
            RAISE EXCEPTION 'invalid source-stream transition' USING ERRCODE = '23514';
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
        RAISE EXCEPTION 'source-stream mutation requires its exact live ingest owner'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER source_stream_guard BEFORE INSERT OR UPDATE OR DELETE
    ON control.source_streams FOR EACH ROW EXECUTE FUNCTION control.guard_source_stream();

CREATE FUNCTION control.require_source_stream_membership() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
DECLARE source control.source_streams%ROWTYPE;
BEGIN
    IF TG_TABLE_NAME = 'source_streams' THEN
        IF NOT EXISTS (SELECT 1 FROM control.topology_streams
            WHERE dataset_id = NEW.dataset_id AND shard_id = NEW.shard_id
                AND owner_epoch = NEW.owner_epoch) THEN
            RAISE EXCEPTION 'source stream is absent from every registered topology'
                USING ERRCODE = '23514';
        END IF;
        IF NEW.state = 'closed' AND EXISTS (SELECT 1 FROM control.topology_streams
            WHERE dataset_id = NEW.dataset_id AND shard_id = NEW.shard_id
                AND owner_epoch = NEW.owner_epoch
                AND handoff_last_complete_sequence IS NOT NULL
                AND handoff_last_complete_sequence <> NEW.last_complete_sequence) THEN
            RAISE EXCEPTION 'source closure disagrees with a topology handoff boundary'
                USING ERRCODE = '23514';
        END IF;
    ELSE
        SELECT * INTO source FROM control.source_streams
            WHERE dataset_id = NEW.dataset_id AND shard_id = NEW.shard_id
                AND owner_epoch = NEW.owner_epoch FOR SHARE;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'topology member lacks a registered source stream'
                USING ERRCODE = '23514';
        END IF;
        IF (source.state = 'closed' AND
                (NEW.handoff_last_complete_sequence IS NULL
                    OR NEW.handoff_last_complete_sequence <> source.last_complete_sequence))
            OR (source.state <> 'closed'
                AND NEW.handoff_last_complete_sequence IS NOT NULL) THEN
            RAISE EXCEPTION 'topology handoff must exactly match closed source state'
                USING ERRCODE = '23514';
        END IF;
    END IF;
    RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER source_stream_has_topology AFTER INSERT OR UPDATE
    ON control.source_streams DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION control.require_source_stream_membership();
CREATE CONSTRAINT TRIGGER topology_has_source_stream AFTER INSERT
    ON control.topology_streams DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION control.require_source_stream_membership();

CREATE TABLE control.producer_slots (
    producer_slot_id control.id16 PRIMARY KEY,
    dataset_id control.id16 NOT NULL REFERENCES control.datasets(dataset_id),
    registered_by_resource_id control.id16 NOT NULL,
    registered_by_epoch control.u64 NOT NULL CHECK (registered_by_epoch > 0),
    registered_by_role text NOT NULL DEFAULT 'control' CHECK (registered_by_role = 'control'),
    registered_by_incarnation control.id16 NOT NULL,
    created_at_us bigint NOT NULL,
    UNIQUE (producer_slot_id, dataset_id),
    FOREIGN KEY (dataset_id, registered_by_resource_id, registered_by_epoch,
            registered_by_role, registered_by_incarnation)
        REFERENCES control.owner_epochs(dataset_id, resource_id, epoch, role, process_incarnation)
);

CREATE FUNCTION control.guard_producer_slot() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'producer-slot identity is retained and immutable' USING ERRCODE = '23514';
    END IF;
    PERFORM 1 FROM control.owner_epochs
        WHERE dataset_id = NEW.dataset_id AND resource_id = NEW.registered_by_resource_id
            AND epoch = NEW.registered_by_epoch AND role = NEW.registered_by_role
            AND process_incarnation = NEW.registered_by_incarnation AND state = 'active'
        FOR SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'producer-slot registration requires the exact active Control owner'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER producer_slot_guard BEFORE INSERT OR UPDATE OR DELETE
    ON control.producer_slots FOR EACH ROW EXECUTE FUNCTION control.guard_producer_slot();

CREATE TABLE control.producer_epochs (
    producer_slot_id control.id16 NOT NULL,
    producer_epoch control.u64 NOT NULL CHECK (producer_epoch > 0),
    dataset_id control.id16 NOT NULL,
    producer_role_instance_id control.id16 NOT NULL,
    producer_role text NOT NULL CHECK (producer_role IN
        ('discoverer', 'validator', 'monitor', 'sync', 'ingest')),
    producer_incarnation control.id16 NOT NULL,
    owner_resource_id control.id16 NOT NULL,
    owner_epoch control.u64 NOT NULL CHECK (owner_epoch > 0),
    owner_role text NOT NULL DEFAULT 'ingest' CHECK (owner_role = 'ingest'),
    owner_incarnation control.id16 NOT NULL,
    outstanding_request_limit control.u32 NOT NULL CHECK (outstanding_request_limit > 0),
    opened_at_us bigint NOT NULL,
    state text NOT NULL DEFAULT 'open' CHECK (state IN ('open', 'closing', 'retired')),
    retired_at_us bigint,
    closure_digest control.digest32,
    PRIMARY KEY (producer_slot_id, producer_epoch),
    UNIQUE (producer_slot_id, producer_epoch, dataset_id),
    FOREIGN KEY (producer_slot_id, dataset_id)
        REFERENCES control.producer_slots(producer_slot_id, dataset_id),
    FOREIGN KEY (producer_role_instance_id, dataset_id, producer_role)
        REFERENCES control.role_instances(role_instance_id, dataset_id, role),
    FOREIGN KEY (dataset_id, owner_resource_id, owner_epoch, owner_role, owner_incarnation)
        REFERENCES control.owner_epochs(dataset_id, resource_id, epoch, role, process_incarnation),
    CHECK ((state = 'retired' AND retired_at_us IS NOT NULL AND closure_digest IS NOT NULL)
        OR (state <> 'retired' AND retired_at_us IS NULL AND closure_digest IS NULL)),
    CHECK (retired_at_us IS NULL OR retired_at_us >= opened_at_us)
);

CREATE FUNCTION control.guard_producer_epoch() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
DECLARE prior control.producer_epochs%ROWTYPE;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'producer-epoch history is retained' USING ERRCODE = '23514';
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
CREATE TRIGGER producer_epoch_guard BEFORE INSERT OR UPDATE OR DELETE
    ON control.producer_epochs FOR EACH ROW EXECUTE FUNCTION control.guard_producer_epoch();

-- This is the producer's durable contiguous checkpoint. The ingest owner's ACK
-- and receipt-GC watermark remains in its synchronized RocksDB batch.
CREATE TABLE control.producer_checkpoints (
    producer_slot_id control.id16 NOT NULL,
    producer_epoch control.u64 NOT NULL CHECK (producer_epoch > 0),
    dataset_id control.id16 NOT NULL,
    checkpoint_revision control.u64 NOT NULL CHECK (checkpoint_revision > 0),
    complete_through_request control.u64 NOT NULL,
    checkpoint_digest control.digest32 NOT NULL,
    committed_at_us bigint NOT NULL,
    PRIMARY KEY (producer_slot_id, producer_epoch),
    FOREIGN KEY (producer_slot_id, producer_epoch, dataset_id)
        REFERENCES control.producer_epochs(producer_slot_id, producer_epoch, dataset_id)
);

CREATE FUNCTION control.guard_producer_checkpoint() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
DECLARE epoch_state text;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'producer checkpoints are retained through epoch retirement'
            USING ERRCODE = '23514';
    END IF;
    SELECT state INTO STRICT epoch_state FROM control.producer_epochs
        WHERE producer_slot_id = NEW.producer_slot_id AND producer_epoch = NEW.producer_epoch
        FOR UPDATE;
    IF epoch_state = 'retired' THEN
        RAISE EXCEPTION 'a retired producer epoch cannot advance its checkpoint'
            USING ERRCODE = '23514';
    END IF;
    IF TG_OP = 'INSERT' THEN
        IF NEW.checkpoint_revision <> 1 THEN
            RAISE EXCEPTION 'the first producer checkpoint revision is one'
                USING ERRCODE = '23514';
        END IF;
    ELSIF ROW(OLD.producer_slot_id, OLD.producer_epoch, OLD.dataset_id)
            IS DISTINCT FROM ROW(NEW.producer_slot_id, NEW.producer_epoch, NEW.dataset_id)
        OR NEW.checkpoint_revision <> OLD.checkpoint_revision + 1
        OR NEW.complete_through_request <= OLD.complete_through_request
        OR NEW.committed_at_us < OLD.committed_at_us THEN
        RAISE EXCEPTION 'producer checkpoint CAS must advance its contiguous request bound'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER producer_checkpoint_guard BEFORE INSERT OR UPDATE OR DELETE
    ON control.producer_checkpoints FOR EACH ROW EXECUTE FUNCTION control.guard_producer_checkpoint();

CREATE TABLE control.consumers (
    consumer_id control.id16 PRIMARY KEY,
    dataset_id control.id16 NOT NULL REFERENCES control.datasets(dataset_id),
    consumer_role text NOT NULL CHECK (consumer_role IN ('archive', 'index')),
    created_at_us bigint NOT NULL,
    current_revision control.u64 CHECK (current_revision > 0),
    current_topology_generation control.u64,
    UNIQUE (consumer_id, dataset_id, consumer_role),
    CHECK ((current_revision IS NULL) = (current_topology_generation IS NULL))
);

CREATE TABLE control.consumer_frontier_revisions (
    consumer_id control.id16 NOT NULL,
    frontier_revision control.u64 NOT NULL CHECK (frontier_revision > 0),
    dataset_id control.id16 NOT NULL,
    consumer_role text NOT NULL,
    topology_generation control.u64 NOT NULL,
    previous_revision control.u64,
    owner_resource_id control.id16 NOT NULL,
    owner_epoch control.u64 NOT NULL CHECK (owner_epoch > 0),
    owner_incarnation control.id16 NOT NULL,
    created_at_us bigint NOT NULL,
    sealed boolean NOT NULL DEFAULT false,
    PRIMARY KEY (consumer_id, frontier_revision),
    UNIQUE (consumer_id, dataset_id, frontier_revision, topology_generation),
    FOREIGN KEY (consumer_id, dataset_id, consumer_role)
        REFERENCES control.consumers(consumer_id, dataset_id, consumer_role),
    FOREIGN KEY (dataset_id, topology_generation)
        REFERENCES control.topology_generations(dataset_id, generation),
    FOREIGN KEY (consumer_id, previous_revision)
        REFERENCES control.consumer_frontier_revisions(consumer_id, frontier_revision),
    FOREIGN KEY (dataset_id, owner_resource_id, owner_epoch, consumer_role, owner_incarnation)
        REFERENCES control.owner_epochs(dataset_id, resource_id, epoch, role, process_incarnation),
    CHECK ((previous_revision IS NULL AND frontier_revision = 1)
        OR (previous_revision IS NOT NULL AND frontier_revision = previous_revision + 1))
);

CREATE TABLE control.consumer_frontiers (
    consumer_id control.id16 NOT NULL,
    frontier_revision control.u64 NOT NULL CHECK (frontier_revision > 0),
    dataset_id control.id16 NOT NULL,
    topology_generation control.u64 NOT NULL,
    shard_id control.u32 NOT NULL,
    owner_epoch control.u64 NOT NULL CHECK (owner_epoch > 0),
    last_complete_sequence control.u64 NOT NULL,
    attestation_kind text CHECK (attestation_kind IN ('archive-batch', 'projection-chunk')),
    attestation_digest control.digest32,
    PRIMARY KEY (consumer_id, frontier_revision, shard_id, owner_epoch),
    FOREIGN KEY (consumer_id, dataset_id, frontier_revision, topology_generation)
        REFERENCES control.consumer_frontier_revisions(
            consumer_id, dataset_id, frontier_revision, topology_generation),
    FOREIGN KEY (dataset_id, topology_generation, shard_id, owner_epoch)
        REFERENCES control.topology_streams(dataset_id, topology_generation, shard_id, owner_epoch),
    FOREIGN KEY (dataset_id, shard_id, owner_epoch)
        REFERENCES control.source_streams(dataset_id, shard_id, owner_epoch),
    CHECK ((last_complete_sequence = 0
                AND attestation_kind IS NULL AND attestation_digest IS NULL)
        OR (last_complete_sequence > 0
                AND attestation_kind IS NOT NULL AND attestation_digest IS NOT NULL))
);

ALTER TABLE control.consumers ADD FOREIGN KEY
    (consumer_id, dataset_id, current_revision, current_topology_generation)
    REFERENCES control.consumer_frontier_revisions(
        consumer_id, dataset_id, frontier_revision, topology_generation)
    DEFERRABLE INITIALLY DEFERRED;

CREATE FUNCTION control.guard_consumer_creation() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
BEGIN
    IF NEW.current_revision IS NOT NULL THEN
        RAISE EXCEPTION 'a consumer starts without a published frontier'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER consumer_creation_guard BEFORE INSERT ON control.consumers
    FOR EACH ROW EXECUTE FUNCTION control.guard_consumer_creation();

CREATE FUNCTION control.guard_consumer_update() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
DECLARE revision control.consumer_frontier_revisions%ROWTYPE;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'consumer frontier history requires explicit reclamation'
            USING ERRCODE = '23514';
    END IF;
    IF ROW(OLD.consumer_id, OLD.dataset_id, OLD.consumer_role, OLD.created_at_us)
        IS DISTINCT FROM ROW(NEW.consumer_id, NEW.dataset_id, NEW.consumer_role, NEW.created_at_us)
        OR NEW.current_revision IS NULL
        OR NEW.current_revision <> coalesce(OLD.current_revision + 1, 1) THEN
        RAISE EXCEPTION 'consumer head must publish the next immutable frontier'
            USING ERRCODE = '23514';
    END IF;
    SELECT * INTO STRICT revision FROM control.consumer_frontier_revisions
        WHERE consumer_id = NEW.consumer_id AND frontier_revision = NEW.current_revision
        FOR SHARE;
    IF NOT revision.sealed OR revision.dataset_id <> NEW.dataset_id
        OR revision.consumer_role <> NEW.consumer_role
        OR revision.topology_generation <> NEW.current_topology_generation THEN
        RAISE EXCEPTION 'consumer head requires its exact sealed frontier revision'
            USING ERRCODE = '23514';
    END IF;
    PERFORM 1 FROM control.owner_epochs
        WHERE dataset_id = revision.dataset_id AND resource_id = revision.owner_resource_id
            AND epoch = revision.owner_epoch AND role = revision.consumer_role
            AND process_incarnation = revision.owner_incarnation
            AND state IN ('active', 'draining')
        FOR SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'frontier publication requires the exact live consumer owner'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER consumer_update_guard BEFORE UPDATE OR DELETE ON control.consumers
    FOR EACH ROW EXECUTE FUNCTION control.guard_consumer_update();

CREATE FUNCTION control.guard_frontier_revision() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
DECLARE prior control.consumer_frontier_revisions%ROWTYPE;
DECLARE current_head control.u64;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'consumer frontier revisions are retained' USING ERRCODE = '23514';
    END IF;
    IF TG_OP = 'INSERT' THEN
        IF NEW.sealed THEN
            RAISE EXCEPTION 'a frontier is assembled before it is sealed'
                USING ERRCODE = '23514';
        END IF;
        SELECT current_revision INTO STRICT current_head FROM control.consumers
            WHERE consumer_id = NEW.consumer_id FOR UPDATE;
        IF current_head IS DISTINCT FROM NEW.previous_revision THEN
            RAISE EXCEPTION 'frontier revision must extend the currently published head'
                USING ERRCODE = '23514';
        END IF;
        IF NEW.previous_revision IS NOT NULL THEN
            SELECT * INTO STRICT prior FROM control.consumer_frontier_revisions
                WHERE consumer_id = NEW.consumer_id
                    AND frontier_revision = NEW.previous_revision FOR SHARE;
            IF NOT prior.sealed OR prior.dataset_id <> NEW.dataset_id
                OR prior.consumer_role <> NEW.consumer_role THEN
                RAISE EXCEPTION 'frontier predecessor must be sealed in the same namespace'
                    USING ERRCODE = '23514';
            END IF;
        END IF;
    ELSE
        IF OLD.sealed OR NOT NEW.sealed
            OR ROW(OLD.consumer_id, OLD.frontier_revision, OLD.dataset_id,
                OLD.consumer_role, OLD.topology_generation, OLD.previous_revision,
                OLD.owner_resource_id, OLD.owner_epoch, OLD.owner_incarnation,
                OLD.created_at_us)
              IS DISTINCT FROM
               ROW(NEW.consumer_id, NEW.frontier_revision, NEW.dataset_id,
                NEW.consumer_role, NEW.topology_generation, NEW.previous_revision,
                NEW.owner_resource_id, NEW.owner_epoch, NEW.owner_incarnation,
                NEW.created_at_us) THEN
            RAISE EXCEPTION 'only initial frontier sealing is allowed'
                USING ERRCODE = '23514';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER frontier_revision_guard BEFORE INSERT OR UPDATE OR DELETE
    ON control.consumer_frontier_revisions
    FOR EACH ROW EXECUTE FUNCTION control.guard_frontier_revision();

CREATE FUNCTION control.guard_frontier_bound() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
DECLARE locked boolean;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'published frontier bounds are immutable' USING ERRCODE = '23514';
    END IF;
    SELECT sealed INTO STRICT locked FROM control.consumer_frontier_revisions
        WHERE consumer_id = NEW.consumer_id AND frontier_revision = NEW.frontier_revision
        FOR UPDATE;
    IF locked THEN
        RAISE EXCEPTION 'sealed frontier cannot accept another bound' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER frontier_bound_guard BEFORE INSERT OR UPDATE OR DELETE
    ON control.consumer_frontiers FOR EACH ROW EXECUTE FUNCTION control.guard_frontier_bound();

CREATE FUNCTION control.require_complete_frontier() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
DECLARE prior control.consumer_frontier_revisions%ROWTYPE;
DECLARE current_revision control.consumer_frontier_revisions%ROWTYPE;
BEGIN
    SELECT * INTO STRICT current_revision FROM control.consumer_frontier_revisions
        WHERE consumer_id = NEW.consumer_id AND frontier_revision = NEW.frontier_revision;
    IF NOT current_revision.sealed THEN
        RAISE EXCEPTION 'an unsealed consumer frontier cannot commit' USING ERRCODE = '23514';
    END IF;
    IF EXISTS (SELECT 1 FROM control.topology_streams AS stream
        WHERE stream.dataset_id = current_revision.dataset_id
            AND stream.topology_generation = current_revision.topology_generation
            AND NOT EXISTS (SELECT 1 FROM control.consumer_frontiers AS bound
                WHERE bound.consumer_id = current_revision.consumer_id
                    AND bound.frontier_revision = current_revision.frontier_revision
                    AND bound.shard_id = stream.shard_id
                    AND bound.owner_epoch = stream.owner_epoch)) THEN
        RAISE EXCEPTION 'sealed consumer frontier omits a topology stream'
            USING ERRCODE = '23514';
    END IF;
    IF EXISTS (SELECT 1 FROM control.consumer_frontiers AS bound
        WHERE bound.consumer_id = current_revision.consumer_id
            AND bound.frontier_revision = current_revision.frontier_revision
            AND bound.last_complete_sequence > 0
            AND bound.attestation_kind <> CASE current_revision.consumer_role
                WHEN 'archive' THEN 'archive-batch' ELSE 'projection-chunk' END) THEN
        RAISE EXCEPTION 'frontier attestation kind is incompatible with its consumer role'
            USING ERRCODE = '23514';
    END IF;
    -- Serialize source closure against publication. Otherwise each transaction
    -- could validate against the other's not-yet-visible state.
    PERFORM 1 FROM control.source_streams AS stream
        JOIN control.consumer_frontiers AS bound
          ON bound.dataset_id = stream.dataset_id AND bound.shard_id = stream.shard_id
         AND bound.owner_epoch = stream.owner_epoch
        WHERE bound.consumer_id = current_revision.consumer_id
            AND bound.frontier_revision = current_revision.frontier_revision
        FOR SHARE OF stream;
    IF EXISTS (SELECT 1 FROM control.consumer_frontiers AS bound
        JOIN control.source_streams AS stream
          ON stream.dataset_id = bound.dataset_id AND stream.shard_id = bound.shard_id
         AND stream.owner_epoch = bound.owner_epoch
        WHERE bound.consumer_id = current_revision.consumer_id
            AND bound.frontier_revision = current_revision.frontier_revision
            AND stream.state = 'closed'
            AND bound.last_complete_sequence > stream.last_complete_sequence) THEN
        RAISE EXCEPTION 'consumer frontier exceeds a closed source stream'
            USING ERRCODE = '23514';
    END IF;
    PERFORM 1 FROM control.owner_epochs
        WHERE dataset_id = current_revision.dataset_id
            AND resource_id = current_revision.owner_resource_id
            AND epoch = current_revision.owner_epoch
            AND role = current_revision.consumer_role
            AND process_incarnation = current_revision.owner_incarnation AND state = 'active'
        FOR SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'frontier sealing requires the exact active consumer owner'
            USING ERRCODE = '23514';
    END IF;
    IF current_revision.previous_revision IS NOT NULL THEN
        SELECT * INTO STRICT prior FROM control.consumer_frontier_revisions
            WHERE consumer_id = current_revision.consumer_id
                AND frontier_revision = current_revision.previous_revision;
        IF current_revision.topology_generation = prior.topology_generation THEN
            IF EXISTS (SELECT 1 FROM control.consumer_frontiers AS old_bound
                JOIN control.consumer_frontiers AS new_bound
                  ON new_bound.consumer_id = current_revision.consumer_id
                 AND new_bound.frontier_revision = current_revision.frontier_revision
                 AND new_bound.shard_id = old_bound.shard_id
                 AND new_bound.owner_epoch = old_bound.owner_epoch
                WHERE old_bound.consumer_id = current_revision.consumer_id
                  AND old_bound.frontier_revision = prior.frontier_revision
                  AND new_bound.last_complete_sequence < old_bound.last_complete_sequence) THEN
                RAISE EXCEPTION 'consumer frontier cannot regress' USING ERRCODE = '23514';
            END IF;
        ELSE
            IF NOT EXISTS (SELECT 1 FROM control.topology_generations
                WHERE dataset_id = current_revision.dataset_id
                    AND generation = current_revision.topology_generation
                    AND previous_generation = prior.topology_generation AND sealed) THEN
                RAISE EXCEPTION 'frontier topology must follow its exact predecessor'
                    USING ERRCODE = '23514';
            END IF;
            IF EXISTS (SELECT 1 FROM control.consumer_frontiers AS old_bound
                JOIN control.topology_streams AS old_stream
                  ON old_stream.dataset_id = prior.dataset_id
                 AND old_stream.topology_generation = prior.topology_generation
                 AND old_stream.shard_id = old_bound.shard_id
                 AND old_stream.owner_epoch = old_bound.owner_epoch
                LEFT JOIN control.consumer_frontiers AS new_bound
                  ON new_bound.consumer_id = current_revision.consumer_id
                 AND new_bound.frontier_revision = current_revision.frontier_revision
                 AND new_bound.shard_id = old_bound.shard_id
                 AND new_bound.owner_epoch = old_bound.owner_epoch
                WHERE old_bound.consumer_id = current_revision.consumer_id
                  AND old_bound.frontier_revision = prior.frontier_revision
                  AND ((new_bound.consumer_id IS NULL
                        AND (old_stream.handoff_last_complete_sequence IS NULL
                            OR old_bound.last_complete_sequence
                                <> old_stream.handoff_last_complete_sequence))
                    OR (new_bound.consumer_id IS NOT NULL
                        AND new_bound.last_complete_sequence
                            < old_bound.last_complete_sequence))) THEN
                RAISE EXCEPTION 'topology handoff loses an incomplete or regressed stream'
                    USING ERRCODE = '23514';
            END IF;
        END IF;
    END IF;
    RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER consumer_frontier_complete AFTER INSERT OR UPDATE
    ON control.consumer_frontier_revisions DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION control.require_complete_frontier();

-- The generation registry fixes namespace/incarnation identity only. It does not
-- activate a complete serving generation.
CREATE TABLE control.projection_generations (
    generation_id control.id16 PRIMARY KEY,
    dataset_id control.id16 NOT NULL REFERENCES control.datasets(dataset_id),
    storage_incarnation control.id16 NOT NULL,
    table_identity_digest control.digest32 NOT NULL,
    owner_resource_id control.id16 NOT NULL,
    owner_epoch control.u64 NOT NULL CHECK (owner_epoch > 0),
    owner_role text NOT NULL DEFAULT 'index' CHECK (owner_role = 'index'),
    owner_incarnation control.id16 NOT NULL,
    created_at_us bigint NOT NULL,
    UNIQUE (generation_id, dataset_id, storage_incarnation, table_identity_digest,
        owner_resource_id, owner_epoch, owner_role, owner_incarnation),
    FOREIGN KEY (dataset_id, owner_resource_id, owner_epoch, owner_role, owner_incarnation)
        REFERENCES control.owner_epochs(dataset_id, resource_id, epoch, role, process_incarnation)
);

CREATE FUNCTION control.guard_projection_generation() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'projection generation identity is immutable' USING ERRCODE = '23514';
    END IF;
    PERFORM 1 FROM control.owner_epochs
        WHERE dataset_id = NEW.dataset_id AND resource_id = NEW.owner_resource_id
            AND epoch = NEW.owner_epoch AND role = NEW.owner_role
            AND process_incarnation = NEW.owner_incarnation AND state = 'active'
        FOR SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'projection generation requires the exact active Index owner'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER projection_generation_guard BEFORE INSERT OR UPDATE OR DELETE
    ON control.projection_generations
    FOR EACH ROW EXECUTE FUNCTION control.guard_projection_generation();

CREATE TABLE control.projection_chunks (
    generation_id control.id16 NOT NULL,
    chunk_id control.digest32 NOT NULL,
    dataset_id control.id16 NOT NULL,
    storage_incarnation control.id16 NOT NULL,
    table_identity_digest control.digest32 NOT NULL,
    target text COLLATE "C" NOT NULL CHECK (octet_length(target) BETWEEN 1 AND 128
        AND target ~ '^[a-z][a-z0-9_.-]*$'),
    target_schema_version integer NOT NULL CHECK (target_schema_version BETWEEN 1 AND 65535),
    topology_generation control.u64 NOT NULL,
    chunk_ordinal control.u64 NOT NULL,
    row_count control.u64 NOT NULL,
    owner_resource_id control.id16 NOT NULL,
    owner_epoch control.u64 NOT NULL CHECK (owner_epoch > 0),
    owner_role text NOT NULL DEFAULT 'index' CHECK (owner_role = 'index'),
    owner_incarnation control.id16 NOT NULL,
    prepared_at_us bigint NOT NULL,
    state text NOT NULL DEFAULT 'prepared' CHECK (state IN ('prepared', 'committed', 'invalid')),
    committed_at_us bigint,
    engine_receipt_digest control.digest32,
    invalid_reason text CHECK (octet_length(invalid_reason) BETWEEN 1 AND 64
        AND invalid_reason ~ '^[a-z][a-z0-9_]*$'),
    PRIMARY KEY (generation_id, chunk_id),
    UNIQUE (generation_id, target, target_schema_version, chunk_ordinal),
    UNIQUE (generation_id, chunk_id, dataset_id, topology_generation),
    FOREIGN KEY (generation_id, dataset_id, storage_incarnation, table_identity_digest,
            owner_resource_id, owner_epoch, owner_role, owner_incarnation)
        REFERENCES control.projection_generations(
            generation_id, dataset_id, storage_incarnation, table_identity_digest,
            owner_resource_id, owner_epoch, owner_role, owner_incarnation),
    FOREIGN KEY (dataset_id, topology_generation)
        REFERENCES control.topology_generations(dataset_id, generation),
    FOREIGN KEY (dataset_id, owner_resource_id, owner_epoch, owner_role, owner_incarnation)
        REFERENCES control.owner_epochs(dataset_id, resource_id, epoch, role, process_incarnation),
    CHECK ((state = 'prepared' AND committed_at_us IS NULL
                AND engine_receipt_digest IS NULL AND invalid_reason IS NULL)
        OR (state = 'committed' AND committed_at_us IS NOT NULL
                AND engine_receipt_digest IS NOT NULL AND invalid_reason IS NULL)
        OR (state = 'invalid' AND committed_at_us IS NULL
                AND engine_receipt_digest IS NULL AND invalid_reason IS NOT NULL)),
    CHECK (committed_at_us IS NULL OR committed_at_us >= prepared_at_us)
);

CREATE TABLE control.projection_chunk_bounds (
    generation_id control.id16 NOT NULL,
    chunk_id control.digest32 NOT NULL,
    dataset_id control.id16 NOT NULL,
    topology_generation control.u64 NOT NULL,
    shard_id control.u32 NOT NULL,
    owner_epoch control.u64 NOT NULL CHECK (owner_epoch > 0),
    last_complete_sequence control.u64 NOT NULL,
    PRIMARY KEY (generation_id, chunk_id, shard_id, owner_epoch),
    FOREIGN KEY (generation_id, chunk_id, dataset_id, topology_generation)
        REFERENCES control.projection_chunks(
            generation_id, chunk_id, dataset_id, topology_generation),
    FOREIGN KEY (dataset_id, topology_generation, shard_id, owner_epoch)
        REFERENCES control.topology_streams(dataset_id, topology_generation, shard_id, owner_epoch),
    FOREIGN KEY (dataset_id, shard_id, owner_epoch)
        REFERENCES control.source_streams(dataset_id, shard_id, owner_epoch)
);

CREATE FUNCTION control.guard_projection_chunk() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'projection chunk history is retained' USING ERRCODE = '23514';
    END IF;
    IF TG_OP = 'INSERT' THEN
        IF NEW.state <> 'prepared' THEN
            RAISE EXCEPTION 'a projection chunk begins prepared' USING ERRCODE = '23514';
        END IF;
    ELSE
        IF OLD.state <> 'prepared' OR NEW.state NOT IN ('committed', 'invalid')
            OR ROW(OLD.generation_id, OLD.chunk_id, OLD.dataset_id,
                OLD.storage_incarnation, OLD.table_identity_digest, OLD.target,
                OLD.target_schema_version, OLD.topology_generation, OLD.chunk_ordinal,
                OLD.row_count, OLD.owner_resource_id, OLD.owner_epoch, OLD.owner_role,
                OLD.owner_incarnation, OLD.prepared_at_us)
              IS DISTINCT FROM
               ROW(NEW.generation_id, NEW.chunk_id, NEW.dataset_id,
                NEW.storage_incarnation, NEW.table_identity_digest, NEW.target,
                NEW.target_schema_version, NEW.topology_generation, NEW.chunk_ordinal,
                NEW.row_count, NEW.owner_resource_id, NEW.owner_epoch, NEW.owner_role,
                NEW.owner_incarnation, NEW.prepared_at_us) THEN
            RAISE EXCEPTION 'invalid projection-chunk transition' USING ERRCODE = '23514';
        END IF;
    END IF;
    PERFORM 1 FROM control.owner_epochs
        WHERE dataset_id = NEW.dataset_id AND resource_id = NEW.owner_resource_id
            AND epoch = NEW.owner_epoch AND role = NEW.owner_role
            AND process_incarnation = NEW.owner_incarnation
            AND state IN ('active', 'draining')
        FOR SHARE;
    IF NOT FOUND OR (TG_OP = 'INSERT' AND NOT EXISTS (
        SELECT 1 FROM control.owner_epochs
        WHERE dataset_id = NEW.dataset_id AND resource_id = NEW.owner_resource_id
            AND epoch = NEW.owner_epoch AND role = NEW.owner_role
            AND process_incarnation = NEW.owner_incarnation AND state = 'active')) THEN
        RAISE EXCEPTION 'projection-chunk mutation requires its exact live Index owner'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER projection_chunk_guard BEFORE INSERT OR UPDATE OR DELETE
    ON control.projection_chunks FOR EACH ROW EXECUTE FUNCTION control.guard_projection_chunk();

CREATE FUNCTION control.guard_projection_chunk_bound() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
DECLARE chunk_state text;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'projection chunk source cut is immutable' USING ERRCODE = '23514';
    END IF;
    SELECT state INTO STRICT chunk_state FROM control.projection_chunks
        WHERE generation_id = NEW.generation_id AND chunk_id = NEW.chunk_id FOR UPDATE;
    IF chunk_state <> 'prepared' THEN
        RAISE EXCEPTION 'only a prepared chunk can accept source bounds'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER projection_chunk_bound_guard BEFORE INSERT OR UPDATE OR DELETE
    ON control.projection_chunk_bounds
    FOR EACH ROW EXECUTE FUNCTION control.guard_projection_chunk_bound();

CREATE FUNCTION control.require_complete_projection_chunk() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
BEGIN
    IF NEW.state = 'committed' AND EXISTS (SELECT 1 FROM control.topology_streams AS stream
        WHERE stream.dataset_id = NEW.dataset_id
            AND stream.topology_generation = NEW.topology_generation
            AND NOT EXISTS (SELECT 1 FROM control.projection_chunk_bounds AS bound
                WHERE bound.generation_id = NEW.generation_id AND bound.chunk_id = NEW.chunk_id
                    AND bound.shard_id = stream.shard_id
                    AND bound.owner_epoch = stream.owner_epoch)) THEN
        RAISE EXCEPTION 'committed projection chunk omits a source-cut stream'
            USING ERRCODE = '23514';
    END IF;
    IF NEW.state = 'committed' THEN
        PERFORM 1 FROM control.source_streams AS stream
            JOIN control.projection_chunk_bounds AS bound
              ON bound.dataset_id = stream.dataset_id AND bound.shard_id = stream.shard_id
             AND bound.owner_epoch = stream.owner_epoch
            WHERE bound.generation_id = NEW.generation_id AND bound.chunk_id = NEW.chunk_id
            FOR SHARE OF stream;
    END IF;
    IF NEW.state = 'committed' AND EXISTS (
        SELECT 1 FROM control.projection_chunk_bounds AS bound
        JOIN control.source_streams AS stream
          ON stream.dataset_id = bound.dataset_id AND stream.shard_id = bound.shard_id
         AND stream.owner_epoch = bound.owner_epoch
        WHERE bound.generation_id = NEW.generation_id AND bound.chunk_id = NEW.chunk_id
            AND stream.state = 'closed'
            AND bound.last_complete_sequence > stream.last_complete_sequence) THEN
        RAISE EXCEPTION 'projection chunk exceeds a closed source stream'
            USING ERRCODE = '23514';
    END IF;
    RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER projection_chunk_complete AFTER INSERT OR UPDATE
    ON control.projection_chunks DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION control.require_complete_projection_chunk();

-- These relations are declared after source_streams, so the cross-boundary check
-- is installed here rather than relying on a forward relation reference.
CREATE FUNCTION control.guard_source_closure_bounds() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
BEGIN
    IF NEW.state = 'closed' AND (
        EXISTS (SELECT 1 FROM control.consumer_frontiers
            WHERE dataset_id = NEW.dataset_id AND shard_id = NEW.shard_id
                AND owner_epoch = NEW.owner_epoch
                AND last_complete_sequence > NEW.last_complete_sequence)
        OR EXISTS (SELECT 1 FROM control.projection_chunk_bounds AS bound
            JOIN control.projection_chunks AS chunk
              ON chunk.generation_id = bound.generation_id AND chunk.chunk_id = bound.chunk_id
            WHERE bound.dataset_id = NEW.dataset_id AND bound.shard_id = NEW.shard_id
                AND bound.owner_epoch = NEW.owner_epoch AND chunk.state = 'committed'
                AND bound.last_complete_sequence > NEW.last_complete_sequence)) THEN
        RAISE EXCEPTION 'source closure is below an already recorded source cut'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER source_closure_bounds_guard BEFORE UPDATE ON control.source_streams
    FOR EACH ROW EXECUTE FUNCTION control.guard_source_closure_bounds();

-- The first concrete lease variant pins one sealed, exact source cut. Later
-- artifact and cursor variants add FK-backed typed relations in their migrations.
CREATE TABLE control.snapshot_leases (
    lease_id control.id16 PRIMARY KEY,
    dataset_id control.id16 NOT NULL REFERENCES control.datasets(dataset_id),
    consumer_id control.id16 NOT NULL,
    frontier_revision control.u64 NOT NULL CHECK (frontier_revision > 0),
    topology_generation control.u64 NOT NULL,
    holder_kind text NOT NULL CHECK (holder_kind IN ('job', 'role_instance')),
    holder_job_id control.id16 REFERENCES control.jobs(job_id),
    holder_role_instance_id control.id16,
    holder_role text,
    holder_incarnation control.id16,
    lease_revision control.u64 NOT NULL DEFAULT 1 CHECK (lease_revision > 0),
    acquired_at_us bigint NOT NULL,
    updated_at_us bigint NOT NULL,
    expires_at_us bigint NOT NULL,
    state text NOT NULL DEFAULT 'active' CHECK (state IN ('active', 'released')),
    released_at_us bigint,
    release_reason text CHECK (octet_length(release_reason) BETWEEN 1 AND 64
        AND release_reason ~ '^[a-z][a-z0-9_]*$'),
    FOREIGN KEY (consumer_id, dataset_id, frontier_revision, topology_generation)
        REFERENCES control.consumer_frontier_revisions(
            consumer_id, dataset_id, frontier_revision, topology_generation),
    FOREIGN KEY (holder_role_instance_id, dataset_id, holder_role)
        REFERENCES control.role_instances(role_instance_id, dataset_id, role),
    CHECK (updated_at_us >= acquired_at_us
        AND (state = 'released' OR expires_at_us > updated_at_us)),
    CHECK ((holder_kind = 'job' AND holder_job_id IS NOT NULL
                AND holder_role_instance_id IS NULL AND holder_role IS NULL
                AND holder_incarnation IS NULL)
        OR (holder_kind = 'role_instance' AND holder_job_id IS NULL
                AND holder_role_instance_id IS NOT NULL AND holder_role IS NOT NULL
                AND holder_incarnation IS NOT NULL)),
    CHECK ((state = 'released' AND released_at_us IS NOT NULL
                AND released_at_us = updated_at_us AND release_reason IS NOT NULL)
        OR (state = 'active' AND released_at_us IS NULL AND release_reason IS NULL))
);

CREATE FUNCTION control.guard_snapshot_lease() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
DECLARE frontier_sealed boolean;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'snapshot-lease history is retained' USING ERRCODE = '23514';
    END IF;
    IF TG_OP = 'INSERT' THEN
        IF NEW.state <> 'active' OR NEW.lease_revision <> 1 THEN
            RAISE EXCEPTION 'a snapshot lease begins active at revision one'
                USING ERRCODE = '23514';
        END IF;
    ELSE
        IF ROW(OLD.lease_id, OLD.dataset_id, OLD.consumer_id, OLD.frontier_revision,
                OLD.topology_generation, OLD.holder_kind, OLD.holder_job_id,
                OLD.holder_role_instance_id, OLD.holder_role, OLD.holder_incarnation,
                OLD.acquired_at_us)
              IS DISTINCT FROM
               ROW(NEW.lease_id, NEW.dataset_id, NEW.consumer_id, NEW.frontier_revision,
                NEW.topology_generation, NEW.holder_kind, NEW.holder_job_id,
                NEW.holder_role_instance_id, NEW.holder_role, NEW.holder_incarnation,
                NEW.acquired_at_us)
            OR OLD.state <> 'active' OR NEW.lease_revision <> OLD.lease_revision + 1
            OR NEW.updated_at_us <= OLD.updated_at_us
            OR NOT ((NEW.state = 'active' AND NEW.updated_at_us < OLD.expires_at_us
                    AND NEW.expires_at_us > OLD.expires_at_us)
                OR (NEW.state = 'released' AND NEW.expires_at_us = OLD.expires_at_us)) THEN
            RAISE EXCEPTION 'invalid snapshot-lease CAS transition' USING ERRCODE = '23514';
        END IF;
    END IF;
    SELECT sealed INTO STRICT frontier_sealed FROM control.consumer_frontier_revisions
        WHERE consumer_id = NEW.consumer_id AND frontier_revision = NEW.frontier_revision
        FOR SHARE;
    IF NOT frontier_sealed THEN
        RAISE EXCEPTION 'snapshot lease requires a sealed source cut'
            USING ERRCODE = '23514';
    END IF;
    IF NEW.state = 'active' THEN
        IF NEW.holder_kind = 'job' AND NOT EXISTS (SELECT 1 FROM control.jobs
            WHERE job_id = NEW.holder_job_id AND dataset_id = NEW.dataset_id
                AND state NOT IN ('succeeded', 'partial', 'failed', 'cancelled', 'skipped')
            FOR SHARE) THEN
            RAISE EXCEPTION 'snapshot lease requires a live job in the same dataset'
                USING ERRCODE = '23514';
        END IF;
        IF NEW.holder_kind = 'role_instance' AND NOT EXISTS (
            SELECT 1 FROM control.role_instances
            WHERE role_instance_id = NEW.holder_role_instance_id
                AND dataset_id = NEW.dataset_id AND role = NEW.holder_role
                AND retired_at_us IS NULL FOR SHARE) THEN
            RAISE EXCEPTION 'snapshot lease requires a live registered role instance'
                USING ERRCODE = '23514';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER snapshot_lease_guard BEFORE INSERT OR UPDATE OR DELETE
    ON control.snapshot_leases FOR EACH ROW EXECUTE FUNCTION control.guard_snapshot_lease();

CREATE TABLE control.retention_claims (
    claim_id control.id16 PRIMARY KEY,
    lease_id control.id16 NOT NULL REFERENCES control.snapshot_leases(lease_id),
    dataset_id control.id16 NOT NULL,
    topology_generation control.u64 NOT NULL,
    shard_id control.u32 NOT NULL,
    owner_epoch control.u64 NOT NULL CHECK (owner_epoch > 0),
    first_sequence control.u64 NOT NULL CHECK (first_sequence > 0),
    last_sequence control.u64 NOT NULL CHECK (last_sequence >= first_sequence),
    purpose text NOT NULL CHECK (purpose IN
        ('reader', 'export', 'backup', 'derived', 'publication', 'maintenance')),
    created_at_us bigint NOT NULL,
    state text NOT NULL DEFAULT 'active' CHECK (state IN ('active', 'released')),
    released_at_us bigint,
    release_reason text CHECK (octet_length(release_reason) BETWEEN 1 AND 64
        AND release_reason ~ '^[a-z][a-z0-9_]*$'),
    UNIQUE (lease_id, dataset_id, topology_generation, shard_id, owner_epoch,
        first_sequence, last_sequence, purpose),
    FOREIGN KEY (dataset_id, topology_generation, shard_id, owner_epoch)
        REFERENCES control.topology_streams(dataset_id, topology_generation, shard_id, owner_epoch),
    FOREIGN KEY (dataset_id, shard_id, owner_epoch)
        REFERENCES control.source_streams(dataset_id, shard_id, owner_epoch),
    CHECK ((state = 'released' AND released_at_us IS NOT NULL AND release_reason IS NOT NULL)
        OR (state = 'active' AND released_at_us IS NULL AND release_reason IS NULL)),
    CHECK (released_at_us IS NULL OR released_at_us >= created_at_us)
);

CREATE FUNCTION control.guard_retention_claim() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
DECLARE lease control.snapshot_leases%ROWTYPE;
DECLARE retained_through control.u64;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'retention-claim history is retained' USING ERRCODE = '23514';
    END IF;
    IF TG_OP = 'INSERT' THEN
        IF NEW.state <> 'active' THEN
            RAISE EXCEPTION 'a retention claim begins active' USING ERRCODE = '23514';
        END IF;
        SELECT * INTO STRICT lease FROM control.snapshot_leases
            WHERE lease_id = NEW.lease_id FOR UPDATE;
        IF lease.dataset_id <> NEW.dataset_id
            OR lease.topology_generation <> NEW.topology_generation
            OR lease.state <> 'active' OR NEW.created_at_us < lease.acquired_at_us
            OR NEW.created_at_us >= lease.expires_at_us THEN
            RAISE EXCEPTION 'retention claim requires its active unexpired source-cut lease'
                USING ERRCODE = '23514';
        END IF;
        SELECT last_complete_sequence INTO STRICT retained_through
            FROM control.consumer_frontiers
            WHERE consumer_id = lease.consumer_id
                AND frontier_revision = lease.frontier_revision
                AND shard_id = NEW.shard_id AND owner_epoch = NEW.owner_epoch;
        IF NEW.last_sequence > retained_through THEN
            RAISE EXCEPTION 'retention claim exceeds its leased source cut'
                USING ERRCODE = '23514';
        END IF;
    ELSE
        IF ROW(OLD.claim_id, OLD.lease_id, OLD.dataset_id, OLD.topology_generation,
                OLD.shard_id, OLD.owner_epoch, OLD.first_sequence, OLD.last_sequence,
                OLD.purpose, OLD.created_at_us)
              IS DISTINCT FROM
               ROW(NEW.claim_id, NEW.lease_id, NEW.dataset_id, NEW.topology_generation,
                NEW.shard_id, NEW.owner_epoch, NEW.first_sequence, NEW.last_sequence,
                NEW.purpose, NEW.created_at_us)
            OR OLD.state <> 'active' OR NEW.state <> 'released' THEN
            RAISE EXCEPTION 'only explicit retention-claim release is allowed'
                USING ERRCODE = '23514';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER retention_claim_guard BEFORE INSERT OR UPDATE OR DELETE
    ON control.retention_claims FOR EACH ROW EXECUTE FUNCTION control.guard_retention_claim();

-- An owner lease and even a recorded exclusion cannot abandon unresolved local
-- authority rows. The owner drains them under the existing draining state first.
CREATE FUNCTION control.guard_owner_ingest_dependents() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
BEGIN
    IF OLD.state <> 'released' AND NEW.state = 'released' THEN
        IF NEW.role = 'ingest' AND (
            EXISTS (SELECT 1 FROM control.allocation_domains
                WHERE dataset_id = NEW.dataset_id AND owner_resource_id = NEW.resource_id
                    AND owner_epoch = NEW.epoch AND owner_incarnation = NEW.process_incarnation
                    AND state = 'active')
            OR EXISTS (SELECT 1 FROM control.source_streams
                WHERE dataset_id = NEW.dataset_id AND owner_resource_id = NEW.resource_id
                    AND owner_epoch = NEW.epoch AND owner_incarnation = NEW.process_incarnation
                    AND state <> 'closed')
            OR EXISTS (SELECT 1 FROM control.producer_epochs
                WHERE dataset_id = NEW.dataset_id AND owner_resource_id = NEW.resource_id
                    AND owner_epoch = NEW.epoch AND owner_incarnation = NEW.process_incarnation
                    AND state <> 'retired')) THEN
            RAISE EXCEPTION 'ingest owner release requires closed domains, streams and producer epochs'
                USING ERRCODE = '23514';
        END IF;
        IF NEW.role = 'index' AND EXISTS (SELECT 1 FROM control.projection_chunks
            WHERE dataset_id = NEW.dataset_id AND owner_resource_id = NEW.resource_id
                AND owner_epoch = NEW.epoch AND owner_incarnation = NEW.process_incarnation
                AND state = 'prepared') THEN
            RAISE EXCEPTION 'Index owner release requires every prepared chunk to resolve'
                USING ERRCODE = '23514';
        END IF;
        IF NEW.role IN ('archive', 'index') AND EXISTS (
            SELECT 1 FROM control.consumer_frontier_revisions AS revision
            JOIN control.consumers AS consumer USING (consumer_id)
            WHERE revision.dataset_id = NEW.dataset_id
                AND revision.owner_resource_id = NEW.resource_id
                AND revision.owner_epoch = NEW.epoch
                AND revision.owner_incarnation = NEW.process_incarnation
                AND (consumer.current_revision IS NULL
                    OR revision.frontier_revision > consumer.current_revision)) THEN
            RAISE EXCEPTION 'consumer owner release requires publication of its next sealed frontier'
                USING ERRCODE = '23514';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER owner_ingest_dependents_guard BEFORE UPDATE ON control.owner_epochs
    FOR EACH ROW EXECUTE FUNCTION control.guard_owner_ingest_dependents();

REVOKE ALL ON ALL TABLES IN SCHEMA control FROM PUBLIC;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA control FROM PUBLIC;
