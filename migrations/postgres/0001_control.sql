-- BigBrotr control foundation, schema compatibility 1.
-- The dedicated migrator owns the transaction and advisory transaction lock.
-- Execute this file and its exact checksum receipt in ONE transaction. No service
-- startup, inspection, or unreachable/unknown database may bootstrap implicitly.
CREATE SCHEMA control;
REVOKE ALL ON SCHEMA control FROM PUBLIC;

CREATE DOMAIN control.id16 AS bytea
    CHECK (octet_length(VALUE) = 16 AND VALUE <> decode('00000000000000000000000000000000', 'hex'));
CREATE DOMAIN control.digest32 AS bytea CHECK (octet_length(VALUE) = 32);
-- NUMERIC scale coercion occurs before CHECK. The typed adapter must reject
-- noninteger input before a cast; these domains preserve, not parse, u64 values.
CREATE DOMAIN control.u64 AS numeric(20, 0)
    CHECK (VALUE BETWEEN 0 AND 18446744073709551615);
CREATE DOMAIN control.u32 AS bigint CHECK (VALUE BETWEEN 0 AND 4294967295);

CREATE TABLE control.schema_versions (
    engine text NOT NULL CHECK (engine = 'postgres-control'),
    migration control.u32 NOT NULL CHECK (migration > 0),
    name text NOT NULL CHECK (octet_length(name) BETWEEN 1 AND 128),
    checksum control.digest32 NOT NULL,
    reader_min control.u32 NOT NULL CHECK (reader_min > 0),
    reader_max control.u32 NOT NULL CHECK (reader_max >= reader_min),
    writer_floor control.u32 NOT NULL CHECK (writer_floor BETWEEN reader_min AND reader_max),
    applied_at_us bigint NOT NULL,
    applied_by text NOT NULL CHECK (octet_length(applied_by) BETWEEN 1 AND 128),
    PRIMARY KEY (engine, migration)
);

-- This application database owns one instance/dataset. Imported source datasets
-- retain their separate A03 lineage; they do not become another local authority.
CREATE TABLE control.datasets (
    dataset_id control.id16 PRIMARY KEY,
    instance_id control.id16 NOT NULL UNIQUE,
    singleton boolean NOT NULL DEFAULT true UNIQUE CHECK (singleton),
    created_at_us bigint NOT NULL,
    topology_generation control.u64,
    config_revision control.u64 CHECK (config_revision > 0),
    policy_revision control.u64 CHECK (policy_revision > 0)
);
CREATE TABLE control.topology_generations (
    dataset_id control.id16 NOT NULL REFERENCES control.datasets(dataset_id),
    generation control.u64 NOT NULL,
    previous_generation control.u64,
    created_at_us bigint NOT NULL,
    sealed boolean NOT NULL DEFAULT false,
    PRIMARY KEY (dataset_id, generation),
    FOREIGN KEY (dataset_id, previous_generation)
        REFERENCES control.topology_generations(dataset_id, generation),
    CHECK ((previous_generation IS NULL AND generation = 0)
        OR (previous_generation IS NOT NULL AND generation = previous_generation + 1))
);
CREATE TABLE control.topology_streams (
    dataset_id control.id16 NOT NULL,
    topology_generation control.u64 NOT NULL,
    shard_id control.u32 NOT NULL,
    owner_epoch control.u64 NOT NULL CHECK (owner_epoch > 0),
    handoff_last_complete_sequence control.u64,
    PRIMARY KEY (dataset_id, topology_generation, shard_id, owner_epoch),
    FOREIGN KEY (dataset_id, topology_generation)
        REFERENCES control.topology_generations(dataset_id, generation)
);
ALTER TABLE control.datasets ADD FOREIGN KEY (dataset_id, topology_generation)
    REFERENCES control.topology_generations(dataset_id, generation) DEFERRABLE INITIALLY DEFERRED;

CREATE TABLE control.role_instances (
    role_instance_id control.id16 PRIMARY KEY,
    dataset_id control.id16 NOT NULL REFERENCES control.datasets(dataset_id),
    role text NOT NULL CHECK (role IN ('control', 'discoverer', 'validator', 'monitor', 'sync',
        'ingest', 'archive', 'index', 'deriver', 'ranker', 'read')),
    created_at_us bigint NOT NULL,
    retired_at_us bigint,
    UNIQUE (role_instance_id, dataset_id, role)
);
-- WriterRole is a logical responsibility, not a PostgreSQL login/GRANT role.
-- A row and its lease are coordination metadata, never positive old-writer
-- exclusion. The owner adapter must prove that at its real commit boundary.
CREATE TABLE control.owner_epochs (
    resource_id control.id16 NOT NULL,
    epoch control.u64 NOT NULL CHECK (epoch > 0),
    dataset_id control.id16 NOT NULL REFERENCES control.datasets(dataset_id),
    role text NOT NULL CHECK (role IN ('control', 'ingest', 'archive', 'index')),
    role_instance_id control.id16 NOT NULL,
    process_incarnation control.id16 NOT NULL,
    acquired_at_us bigint NOT NULL,
    lease_expires_at_us bigint NOT NULL,
    state text NOT NULL CHECK (state IN ('active', 'draining', 'released')),
    released_at_us bigint,
    exclusion_method text CHECK (exclusion_method IN ('joined', 'process_excluded', 'storage_fenced')),
    exclusion_digest control.digest32,
    PRIMARY KEY (resource_id, epoch),
    UNIQUE (dataset_id, resource_id, epoch, role, process_incarnation),
    FOREIGN KEY (role_instance_id, dataset_id, role)
        REFERENCES control.role_instances(role_instance_id, dataset_id, role),
    CHECK ((state = 'released' AND released_at_us IS NOT NULL AND exclusion_method IS NOT NULL AND exclusion_digest IS NOT NULL)
        OR (state <> 'released' AND released_at_us IS NULL AND exclusion_method IS NULL AND exclusion_digest IS NULL))
);
CREATE UNIQUE INDEX owner_epochs_one_unreleased ON control.owner_epochs(resource_id)
    WHERE state <> 'released';

-- Only validated policy documents belong here: no deployment roots, credential
-- values, arbitrary input payloads, or competing generic checkpoint documents.
-- Control validates the closed Rust schema before parameterized insertion.
CREATE TABLE control.config_revisions (
    revision control.u64 PRIMARY KEY CHECK (revision > 0),
    dataset_id control.id16 NOT NULL REFERENCES control.datasets(dataset_id),
    previous_revision control.u64 REFERENCES control.config_revisions(revision),
    schema_version integer NOT NULL CHECK (schema_version BETWEEN 1 AND 65535),
    authority_mode text NOT NULL CHECK (authority_mode IN ('managed', 'declarative')),
    document jsonb NOT NULL CHECK (jsonb_typeof(document) = 'object' AND octet_length(document::text) <= 10485760),
    document_digest control.digest32 NOT NULL,
    accepted_source_digest control.digest32 NOT NULL,
    accepted_at_us bigint NOT NULL,
    actor_id control.id16 NOT NULL,
    UNIQUE (dataset_id, revision),
    CHECK ((previous_revision IS NULL AND revision = 1)
        OR (previous_revision IS NOT NULL AND revision = previous_revision + 1))
);
CREATE TABLE control.policy_revisions (
    revision control.u64 PRIMARY KEY CHECK (revision > 0),
    dataset_id control.id16 NOT NULL REFERENCES control.datasets(dataset_id),
    previous_revision control.u64 REFERENCES control.policy_revisions(revision),
    config_revision control.u64 NOT NULL,
    schema_version integer NOT NULL CHECK (schema_version BETWEEN 1 AND 65535),
    document jsonb NOT NULL CHECK (jsonb_typeof(document) = 'object' AND octet_length(document::text) <= 10485760),
    document_digest control.digest32 NOT NULL,
    accepted_at_us bigint NOT NULL,
    actor_id control.id16 NOT NULL,
    UNIQUE (dataset_id, revision),
    FOREIGN KEY (dataset_id, config_revision) REFERENCES control.config_revisions(dataset_id, revision),
    CHECK ((previous_revision IS NULL AND revision = 1)
        OR (previous_revision IS NOT NULL AND revision = previous_revision + 1))
);
ALTER TABLE control.datasets ADD FOREIGN KEY (dataset_id, config_revision)
    REFERENCES control.config_revisions(dataset_id, revision) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE control.datasets ADD FOREIGN KEY (dataset_id, policy_revision)
    REFERENCES control.policy_revisions(dataset_id, revision) DEFERRABLE INITIALLY DEFERRED;

-- Shared finite-job identity; actual job-specific recovery units and pin owners
-- are added with their consumers. This does not authorize executing any job.
CREATE TABLE control.jobs (
    job_id control.id16 PRIMARY KEY,
    dataset_id control.id16 NOT NULL REFERENCES control.datasets(dataset_id),
    principal_id control.id16 NOT NULL,
    request_key bytea NOT NULL CHECK (octet_length(request_key) BETWEEN 1 AND 128),
    request_digest control.digest32 NOT NULL,
    kind text NOT NULL CHECK (kind IN ('migration', 'import', 'export', 'backup', 'restore_verification',
        'scrub', 'repair', 'acquisition', 'derivation', 'ranking')),
    config_revision control.u64 NOT NULL,
    policy_revision control.u64 NOT NULL,
    created_at_us bigint NOT NULL,
    state text NOT NULL DEFAULT 'accepted' CHECK (state IN ('accepted', 'queued', 'running', 'paused',
        'cancel_requested', 'succeeded', 'partial', 'failed', 'cancelled', 'skipped')),
    job_revision control.u64 NOT NULL DEFAULT 1 CHECK (job_revision > 0),
    checkpoint_revision control.u64 NOT NULL DEFAULT 0,
    reason_code text CHECK (octet_length(reason_code) BETWEEN 1 AND 64 AND reason_code ~ '^[a-z][a-z0-9_]*$'),
    result_manifest_digest control.digest32,
    owner_resource_id control.id16,
    owner_epoch control.u64 CHECK (owner_epoch > 0),
    owner_role text NOT NULL DEFAULT 'control' CHECK (owner_role = 'control'),
    owner_incarnation control.id16,
    completed_at_us bigint,
    input_topology_generation control.u64,
    input_complete boolean NOT NULL DEFAULT false,
    UNIQUE (principal_id, kind, request_key),
    UNIQUE (job_id, dataset_id, input_topology_generation),
    FOREIGN KEY (dataset_id, config_revision) REFERENCES control.config_revisions(dataset_id, revision),
    FOREIGN KEY (dataset_id, policy_revision) REFERENCES control.policy_revisions(dataset_id, revision),
    FOREIGN KEY (dataset_id, input_topology_generation) REFERENCES control.topology_generations(dataset_id, generation),
    FOREIGN KEY (dataset_id, owner_resource_id, owner_epoch, owner_role, owner_incarnation)
        REFERENCES control.owner_epochs(dataset_id, resource_id, epoch, role, process_incarnation),
    CHECK ((owner_resource_id IS NULL AND owner_epoch IS NULL AND owner_incarnation IS NULL)
        OR (owner_resource_id IS NOT NULL AND owner_epoch IS NOT NULL AND owner_incarnation IS NOT NULL)),
    CHECK ((state IN ('succeeded', 'partial', 'failed', 'cancelled', 'skipped')) = (completed_at_us IS NOT NULL)),
    CHECK (state <> 'succeeded' OR result_manifest_digest IS NOT NULL),
    CHECK (state NOT IN ('paused', 'cancel_requested', 'partial', 'failed', 'cancelled', 'skipped') OR reason_code IS NOT NULL)
);
CREATE INDEX jobs_listing ON control.jobs(created_at_us DESC, job_id DESC);
CREATE TABLE control.job_input_bounds (
    job_id control.id16 NOT NULL,
    dataset_id control.id16 NOT NULL,
    topology_generation control.u64 NOT NULL,
    shard_id control.u32 NOT NULL,
    owner_epoch control.u64 NOT NULL CHECK (owner_epoch > 0),
    last_complete_sequence control.u64 NOT NULL,
    PRIMARY KEY (job_id, shard_id, owner_epoch),
    FOREIGN KEY (job_id, dataset_id, topology_generation)
        REFERENCES control.jobs(job_id, dataset_id, input_topology_generation),
    FOREIGN KEY (dataset_id, topology_generation, shard_id, owner_epoch)
        REFERENCES control.topology_streams(dataset_id, topology_generation, shard_id, owner_epoch)
);

CREATE FUNCTION control.reject_history_mutation() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
BEGIN
    RAISE EXCEPTION 'immutable control history cannot be changed' USING ERRCODE = '23514';
END;
$$;
CREATE TRIGGER immutable_schema_versions BEFORE UPDATE OR DELETE ON control.schema_versions
    FOR EACH ROW EXECUTE FUNCTION control.reject_history_mutation();
CREATE TRIGGER immutable_config_revisions BEFORE UPDATE OR DELETE ON control.config_revisions
    FOR EACH ROW EXECUTE FUNCTION control.reject_history_mutation();
CREATE TRIGGER immutable_policy_revisions BEFORE UPDATE OR DELETE ON control.policy_revisions
    FOR EACH ROW EXECUTE FUNCTION control.reject_history_mutation();

CREATE FUNCTION control.guard_dataset_heads() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'deployment identity cannot be removed' USING ERRCODE = '23514';
    END IF;
    IF TG_OP = 'INSERT' THEN
        IF NEW.config_revision IS NOT NULL OR NEW.policy_revision IS NOT NULL OR NEW.topology_generation IS NOT NULL THEN
            RAISE EXCEPTION 'new deployment starts without authority heads' USING ERRCODE = '23514';
        END IF;
        RETURN NEW;
    END IF;
    IF ROW(OLD.dataset_id, OLD.instance_id, OLD.singleton, OLD.created_at_us)
        IS DISTINCT FROM ROW(NEW.dataset_id, NEW.instance_id, NEW.singleton, NEW.created_at_us) THEN
        RAISE EXCEPTION 'deployment identity is immutable' USING ERRCODE = '23514';
    END IF;
    IF (NEW.config_revision IS DISTINCT FROM OLD.config_revision AND
            (NEW.config_revision IS NULL OR NEW.config_revision <> coalesce(OLD.config_revision + 1, 1)))
        OR (NEW.policy_revision IS DISTINCT FROM OLD.policy_revision AND
            (NEW.policy_revision IS NULL OR NEW.policy_revision <> coalesce(OLD.policy_revision + 1, 1)))
        OR (NEW.topology_generation IS DISTINCT FROM OLD.topology_generation AND
            (NEW.topology_generation IS NULL OR NEW.topology_generation <> coalesce(OLD.topology_generation + 1, 0))) THEN
        RAISE EXCEPTION 'authority heads advance one revision at a time' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER dataset_head_guard BEFORE INSERT OR UPDATE OR DELETE ON control.datasets
    FOR EACH ROW EXECUTE FUNCTION control.guard_dataset_heads();
CREATE FUNCTION control.guard_owner_epoch() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
DECLARE previous control.owner_epochs%ROWTYPE;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'owner epoch history is retained' USING ERRCODE = '23514';
    END IF;
    IF TG_OP = 'INSERT' THEN
        SELECT * INTO previous FROM control.owner_epochs WHERE resource_id = NEW.resource_id ORDER BY epoch DESC LIMIT 1 FOR UPDATE;
        IF NEW.state <> 'active' OR (NOT FOUND AND NEW.epoch <> 1)
            OR (FOUND AND (previous.state <> 'released' OR NEW.epoch <> previous.epoch + 1
                OR NEW.dataset_id <> previous.dataset_id OR NEW.role <> previous.role)) THEN
            RAISE EXCEPTION 'new owner epoch requires the next ordinal and resolved prior exclusion' USING ERRCODE = '23514';
        END IF;
    ELSE
        IF ROW(OLD.resource_id, OLD.epoch, OLD.dataset_id, OLD.role, OLD.role_instance_id, OLD.process_incarnation, OLD.acquired_at_us)
            IS DISTINCT FROM ROW(NEW.resource_id, NEW.epoch, NEW.dataset_id, NEW.role, NEW.role_instance_id, NEW.process_incarnation, NEW.acquired_at_us)
            OR OLD.state = 'released' OR (OLD.state = 'draining' AND NEW.state = 'active') THEN
            RAISE EXCEPTION 'owner identity cannot be rewritten or revived' USING ERRCODE = '23514';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER owner_epoch_guard BEFORE INSERT OR UPDATE OR DELETE ON control.owner_epochs
    FOR EACH ROW EXECUTE FUNCTION control.guard_owner_epoch();

-- Topology membership is constructed and sealed in one transaction. Locking its
-- parent serializes a late child insertion against the seal; FK alone cannot.
CREATE FUNCTION control.guard_topology_stream() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
DECLARE locked boolean;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'topology membership is immutable' USING ERRCODE = '23514';
    END IF;
    SELECT sealed INTO STRICT locked FROM control.topology_generations
        WHERE dataset_id = NEW.dataset_id AND generation = NEW.topology_generation FOR UPDATE;
    IF locked THEN
        RAISE EXCEPTION 'topology membership is sealed' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER topology_stream_guard BEFORE INSERT OR UPDATE OR DELETE ON control.topology_streams
    FOR EACH ROW EXECUTE FUNCTION control.guard_topology_stream();
CREATE FUNCTION control.guard_topology_generation() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'topology history is retained' USING ERRCODE = '23514';
    END IF;
    IF OLD.sealed OR NOT NEW.sealed OR ROW(OLD.dataset_id, OLD.generation, OLD.previous_generation, OLD.created_at_us)
        IS DISTINCT FROM ROW(NEW.dataset_id, NEW.generation, NEW.previous_generation, NEW.created_at_us) THEN
        RAISE EXCEPTION 'only initial topology sealing is allowed' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER topology_generation_guard BEFORE UPDATE OR DELETE ON control.topology_generations
    FOR EACH ROW EXECUTE FUNCTION control.guard_topology_generation();
CREATE FUNCTION control.require_sealed_topology() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
BEGIN
    IF NOT (SELECT sealed FROM control.topology_generations WHERE dataset_id = NEW.dataset_id AND generation = NEW.generation) THEN
        RAISE EXCEPTION 'unsealed topology cannot commit' USING ERRCODE = '23514';
    END IF;
    RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER topology_complete AFTER INSERT OR UPDATE ON control.topology_generations
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION control.require_sealed_topology();

CREATE FUNCTION control.guard_job_input() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
DECLARE locked boolean;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'initial job cut cannot be changed' USING ERRCODE = '23514';
    END IF;
    SELECT input_complete INTO STRICT locked FROM control.jobs WHERE job_id = NEW.job_id FOR UPDATE;
    IF locked THEN
        RAISE EXCEPTION 'initial job cut is sealed' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER job_input_guard BEFORE INSERT OR UPDATE OR DELETE ON control.job_input_bounds
    FOR EACH ROW EXECUTE FUNCTION control.guard_job_input();
CREATE FUNCTION control.guard_job_creation() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
BEGIN
    IF NEW.state <> 'accepted' OR NEW.job_revision <> 1 OR NEW.checkpoint_revision <> 0
        OR NEW.result_manifest_digest IS NOT NULL OR NEW.completed_at_us IS NOT NULL
        OR NEW.reason_code IS NOT NULL OR NEW.owner_resource_id IS NOT NULL THEN
        RAISE EXCEPTION 'new job must begin as unowned accepted work' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER job_creation_guard BEFORE INSERT ON control.jobs
    FOR EACH ROW EXECUTE FUNCTION control.guard_job_creation();
CREATE FUNCTION control.guard_job_update() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'job correctness obligations require explicit reclamation' USING ERRCODE = '23514';
    END IF;
    IF ROW(OLD.job_id, OLD.dataset_id, OLD.principal_id, OLD.kind, OLD.request_key, OLD.request_digest,
        OLD.config_revision, OLD.policy_revision, OLD.created_at_us, OLD.input_topology_generation)
        IS DISTINCT FROM ROW(NEW.job_id, NEW.dataset_id, NEW.principal_id, NEW.kind, NEW.request_key, NEW.request_digest,
        NEW.config_revision, NEW.policy_revision, NEW.created_at_us, NEW.input_topology_generation) THEN
        RAISE EXCEPTION 'accepted job identity and initial inputs are immutable' USING ERRCODE = '23514';
    END IF;
    IF NOT OLD.input_complete AND NEW.input_complete AND OLD.state = 'accepted' AND NEW.state = 'accepted'
        AND NEW.job_revision = OLD.job_revision AND NEW.checkpoint_revision = OLD.checkpoint_revision
        AND ROW(NEW.reason_code, NEW.result_manifest_digest, NEW.completed_at_us, NEW.owner_resource_id, NEW.owner_epoch, NEW.owner_incarnation)
            IS NOT DISTINCT FROM ROW(OLD.reason_code, OLD.result_manifest_digest, OLD.completed_at_us, OLD.owner_resource_id, OLD.owner_epoch, OLD.owner_incarnation) THEN
        RETURN NEW;
    END IF;
    IF NOT OLD.input_complete OR NOT NEW.input_complete OR NEW.job_revision <> OLD.job_revision + 1
        OR NEW.checkpoint_revision < OLD.checkpoint_revision THEN
        RAISE EXCEPTION 'job update requires sealed input and the next revision' USING ERRCODE = '23514';
    END IF;
    IF OLD.state IN ('succeeded', 'partial', 'failed', 'cancelled', 'skipped') OR NOT (
        NEW.state = OLD.state
        OR (OLD.state = 'accepted' AND NEW.state IN ('queued', 'cancel_requested'))
        OR (OLD.state = 'queued' AND NEW.state IN ('running', 'paused', 'cancel_requested', 'skipped'))
        OR (OLD.state = 'running' AND NEW.state IN ('paused', 'cancel_requested', 'succeeded', 'partial', 'failed', 'skipped'))
        OR (OLD.state = 'paused' AND NEW.state IN ('queued', 'cancel_requested'))
        OR (OLD.state = 'cancel_requested' AND NEW.state = 'cancelled')
    ) THEN
        RAISE EXCEPTION 'invalid finite job transition' USING ERRCODE = '23514';
    END IF;
    PERFORM 1 FROM control.owner_epochs WHERE dataset_id = NEW.dataset_id AND resource_id = NEW.owner_resource_id
        AND epoch = NEW.owner_epoch AND role = 'control' AND process_incarnation = NEW.owner_incarnation AND state = 'active'
        FOR SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'job mutation requires the active Control owner tuple' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER job_update_guard BEFORE UPDATE OR DELETE ON control.jobs
    FOR EACH ROW EXECUTE FUNCTION control.guard_job_update();
CREATE FUNCTION control.require_complete_job_input() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
DECLARE job control.jobs%ROWTYPE;
BEGIN
    SELECT * INTO STRICT job FROM control.jobs WHERE job_id = NEW.job_id;
    IF NOT job.input_complete THEN
        RAISE EXCEPTION 'incomplete initial job input cannot commit' USING ERRCODE = '23514';
    END IF;
    IF job.input_topology_generation IS NOT NULL AND EXISTS (
        SELECT 1 FROM control.topology_streams AS stream
        WHERE stream.dataset_id = job.dataset_id AND stream.topology_generation = job.input_topology_generation
            AND NOT EXISTS (SELECT 1 FROM control.job_input_bounds AS bound WHERE bound.job_id = job.job_id
                AND bound.shard_id = stream.shard_id AND bound.owner_epoch = stream.owner_epoch)
    ) THEN
        RAISE EXCEPTION 'initial job cut omits a required topology stream' USING ERRCODE = '23514';
    END IF;
    IF EXISTS (
        SELECT 1 FROM control.job_input_bounds AS bound JOIN control.topology_streams AS stream
            USING (dataset_id, topology_generation, shard_id, owner_epoch)
        WHERE bound.job_id = job.job_id AND stream.handoff_last_complete_sequence IS NOT NULL
            AND bound.last_complete_sequence > stream.handoff_last_complete_sequence
    ) THEN
        RAISE EXCEPTION 'initial job cut exceeds a closed stream handoff' USING ERRCODE = '23514';
    END IF;
    RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER job_input_complete AFTER INSERT OR UPDATE ON control.jobs
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION control.require_complete_job_input();

REVOKE ALL ON ALL TABLES IN SCHEMA control FROM PUBLIC;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA control FROM PUBLIC;
