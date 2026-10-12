-- Prepared metadata only. No activation receipt or successor admission exists.
-- The native handoff is dispatched only after this intent's confirmed COMMIT.
-- Every old manifest, input, outcome, domain and topology remains retained.
LOCK TABLE control.owner_epochs, control.producer_epochs IN SHARE MODE;
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM control.owner_epochs WHERE role = 'ingest' AND epoch <> 1)
        OR EXISTS (SELECT 1 FROM control.producer_epochs WHERE producer_epoch <> 1 OR owner_epoch <> 1) THEN
        RAISE EXCEPTION 'pre-protocol successor history requires explicit recovery; no adoption'
            USING ERRCODE = '55000';
    END IF;
END $$;
CREATE TABLE control.initial_ingest_successions (
    dataset_id control.id16 NOT NULL,
    owner_resource_id control.id16 PRIMARY KEY,
    target_epoch control.u64 NOT NULL CHECK (target_epoch = 2),
    owner_role text NOT NULL DEFAULT 'ingest' CHECK (owner_role = 'ingest'),
    target_incarnation control.id16 NOT NULL,
    target_role_instance_id control.id16 NOT NULL,
    producer_slot_id control.id16 NOT NULL UNIQUE,
    producer_epoch control.u64 NOT NULL CHECK (producer_epoch = 2),
    shard_id control.u32 NOT NULL,
    previous_topology control.u64 NOT NULL,
    target_topology control.u64 NOT NULL CHECK (target_topology = previous_topology + 1),
    issued_through control.u64 NOT NULL CHECK (issued_through <= 64),
    last_complete_sequence control.u64 NOT NULL,
    closure_digest control.digest32 NOT NULL,
    previous_manifest bytea NOT NULL CHECK (octet_length(previous_manifest) BETWEEN 104 AND 266),
    target_manifest bytea NOT NULL CHECK (octet_length(target_manifest) BETWEEN 104 AND 266),
    previous_manifest_sha256 control.digest32 NOT NULL CHECK (previous_manifest_sha256 = sha256(previous_manifest)),
    target_manifest_sha256 control.digest32 NOT NULL CHECK (target_manifest_sha256 = sha256(target_manifest)),
    handoff bytea NOT NULL CHECK (octet_length(handoff) BETWEEN 1 AND 1024),
    handoff_sha256 control.digest32 NOT NULL CHECK (handoff_sha256 = sha256(handoff)),
    config_revision control.u64 NOT NULL,
    policy_revision control.u64 NOT NULL,
    config_digest control.digest32 NOT NULL,
    policy_digest control.digest32 NOT NULL,
    outstanding_request_limit control.u32 NOT NULL CHECK (outstanding_request_limit BETWEEN 1 AND 64),
    created_at_us bigint NOT NULL,
    lease_expires_at_us bigint NOT NULL CHECK (lease_expires_at_us > created_at_us),
    prepared_by_resource control.id16 NOT NULL,
    prepared_by_epoch control.u64 NOT NULL,
    prepared_by_role text NOT NULL DEFAULT 'control' CHECK (prepared_by_role = 'control'),
    prepared_by_incarnation control.id16 NOT NULL,
    FOREIGN KEY (dataset_id, owner_resource_id, target_epoch, owner_role, target_incarnation)
        REFERENCES control.owner_epochs(dataset_id, resource_id, epoch, role, process_incarnation),
    FOREIGN KEY (target_role_instance_id, dataset_id, owner_role)
        REFERENCES control.role_instances(role_instance_id, dataset_id, role),
    FOREIGN KEY (producer_slot_id, producer_epoch, dataset_id)
        REFERENCES control.producer_epochs(producer_slot_id, producer_epoch, dataset_id),
    FOREIGN KEY (dataset_id, shard_id, target_epoch)
        REFERENCES control.source_streams(dataset_id, shard_id, owner_epoch),
    FOREIGN KEY (dataset_id, target_topology)
        REFERENCES control.topology_generations(dataset_id, generation),
    FOREIGN KEY (dataset_id, previous_topology)
        REFERENCES control.topology_generations(dataset_id, generation),
    FOREIGN KEY (dataset_id, config_revision) REFERENCES control.config_revisions(dataset_id, revision),
    FOREIGN KEY (dataset_id, policy_revision) REFERENCES control.policy_revisions(dataset_id, revision),
    FOREIGN KEY (dataset_id, prepared_by_resource, prepared_by_epoch, prepared_by_role, prepared_by_incarnation)
        REFERENCES control.owner_epochs(dataset_id, resource_id, epoch, role, process_incarnation)
);

-- Also veto an attempted successor registered without a preparation row. This
-- capability fence must be replaced by a real activation protocol, not removed
-- merely because a native manifest or prepared row can be observed.
CREATE FUNCTION control.veto_unactivated_producer() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog AS $$
BEGIN
    IF NEW.producer_epoch <> 1 OR EXISTS (
        SELECT 1 FROM control.producer_epochs WHERE producer_slot_id = NEW.producer_slot_id
            AND producer_epoch = NEW.producer_epoch AND owner_epoch <> 1) THEN
        RAISE EXCEPTION 'successor admission requires an explicit activation protocol'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER producer_request_activation_guard BEFORE INSERT ON control.producer_requests
    FOR EACH ROW EXECUTE FUNCTION control.veto_unactivated_producer();
CREATE TRIGGER input_binding_activation_guard BEFORE INSERT ON control.producer_input_bindings
    FOR EACH ROW EXECUTE FUNCTION control.veto_unactivated_producer();

-- The caller holds the actual current Control fence and the dataset UPDATE lock
-- before historical Ingest/domain/source/producer custody. This function checks
-- positive retained metadata; it never reports a native outcome or grants use.
CREATE FUNCTION control.require_prepared_initial_successor(wanted_resource bytea)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE s control.initial_ingest_successions%ROWTYPE;
        old_owner control.owner_epochs%ROWTYPE;
        old_producer control.producer_epochs%ROWTYPE;
        current_owner control.owner_epochs%ROWTYPE;
BEGIN
    SELECT * INTO STRICT s FROM control.initial_ingest_successions
        WHERE owner_resource_id = wanted_resource;
    SELECT * INTO STRICT current_owner FROM control.owner_epochs
        WHERE dataset_id = s.dataset_id AND role = 'control' AND state = 'active';
    PERFORM control.require_control_owner(s.dataset_id, current_owner.resource_id,
        current_owner.epoch, current_owner.process_incarnation);
    PERFORM 1 FROM control.datasets WHERE dataset_id = s.dataset_id FOR UPDATE;
    SELECT * INTO STRICT old_owner FROM control.owner_epochs
        WHERE resource_id = s.owner_resource_id AND epoch = 1 FOR SHARE;
    SELECT * INTO STRICT old_producer FROM control.producer_epochs
        WHERE producer_slot_id = s.producer_slot_id AND producer_epoch = 1 FOR UPDATE;
    PERFORM control.require_initial_retirement(s.producer_slot_id, 1, s.closure_digest);
    IF old_owner.dataset_id <> s.dataset_id OR old_owner.role <> 'ingest'
        OR old_owner.state <> 'released' OR old_owner.exclusion_method <> 'storage_fenced'
        OR old_owner.exclusion_digest <> s.closure_digest
        OR old_owner.released_at_us IS DISTINCT FROM old_producer.retired_at_us
        OR old_producer.state <> 'retired' OR old_producer.owner_resource_id <> s.owner_resource_id
        OR old_producer.owner_epoch <> 1 OR old_producer.owner_incarnation <> old_owner.process_incarnation
        OR old_producer.closure_digest <> s.closure_digest
        OR old_producer.reserved_through_request <> s.issued_through
        OR old_owner.process_incarnation = s.target_incarnation
        OR old_owner.role_instance_id = s.target_role_instance_id
        OR s.created_at_us < old_producer.retired_at_us
        OR NOT EXISTS (SELECT 1 FROM control.role_instances
            WHERE role_instance_id = old_owner.role_instance_id
                AND retired_at_us = old_producer.retired_at_us)
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
        RAISE EXCEPTION 'successor requires the exact fully retired initial predecessor'
            USING ERRCODE = '23514';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM control.owner_epochs o JOIN control.role_instances r
            ON r.role_instance_id = o.role_instance_id
            WHERE o.resource_id = s.owner_resource_id AND o.epoch = 2 AND o.dataset_id = s.dataset_id
                AND o.role = 'ingest' AND o.process_incarnation = s.target_incarnation
                AND o.role_instance_id = s.target_role_instance_id AND o.state = 'active'
                AND o.acquired_at_us = s.created_at_us AND o.lease_expires_at_us = s.lease_expires_at_us
                AND r.retired_at_us IS NULL AND r.created_at_us = s.created_at_us)
        OR NOT EXISTS (SELECT 1 FROM control.producer_epochs
            WHERE producer_slot_id = s.producer_slot_id AND producer_epoch = 2 AND dataset_id = s.dataset_id
                AND producer_role_instance_id = s.target_role_instance_id AND producer_role = 'ingest'
                AND producer_incarnation = s.target_incarnation AND owner_resource_id = s.owner_resource_id
                AND owner_epoch = 2 AND owner_incarnation = s.target_incarnation
                AND state = 'open' AND reserved_through_request = 0 AND opened_at_us = s.created_at_us
                AND outstanding_request_limit = s.outstanding_request_limit)
        OR NOT EXISTS (SELECT 1 FROM control.source_streams
            WHERE dataset_id = s.dataset_id AND shard_id = s.shard_id AND owner_epoch = 2
                AND owner_resource_id = s.owner_resource_id AND owner_incarnation = s.target_incarnation
                AND state = 'open' AND opened_at_us = s.created_at_us)
        OR (SELECT count(*) FROM control.producer_epochs
            WHERE dataset_id = s.dataset_id AND owner_resource_id = s.owner_resource_id
                AND owner_epoch = 2) <> 1
        OR (SELECT count(*) FROM control.source_streams
            WHERE dataset_id = s.dataset_id AND owner_resource_id = s.owner_resource_id
                AND owner_epoch = 2) <> 1
        OR EXISTS (SELECT 1 FROM control.producer_requests
            WHERE producer_slot_id = s.producer_slot_id AND producer_epoch = 2)
        OR EXISTS (SELECT 1 FROM control.producer_checkpoints
            WHERE producer_slot_id = s.producer_slot_id AND producer_epoch = 2)
        OR EXISTS (SELECT 1 FROM control.producer_input_bindings
            WHERE producer_slot_id = s.producer_slot_id AND producer_epoch = 2)
        OR EXISTS (SELECT 1 FROM control.allocation_domains
            WHERE dataset_id = s.dataset_id AND owner_resource_id = s.owner_resource_id AND owner_epoch = 2
                AND (state <> 'active' OR owner_incarnation <> s.target_incarnation
                    OR created_at_us <> s.created_at_us)) THEN
        RAISE EXCEPTION 'prepared successor must retain its exact empty registered shape'
            USING ERRCODE = '23514';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM control.datasets
            WHERE dataset_id = s.dataset_id AND topology_generation = s.target_topology)
        OR NOT EXISTS (SELECT 1 FROM control.topology_generations
            WHERE dataset_id = s.dataset_id AND generation = s.target_topology
                AND previous_generation = s.previous_topology AND sealed AND created_at_us = s.created_at_us)
        OR (SELECT count(*) FROM control.topology_streams
            WHERE dataset_id = s.dataset_id AND topology_generation = s.target_topology) <> 2
        OR NOT EXISTS (SELECT 1 FROM control.topology_streams
            WHERE dataset_id = s.dataset_id AND topology_generation = s.target_topology
                AND shard_id = s.shard_id AND owner_epoch = 1
                AND handoff_last_complete_sequence = s.last_complete_sequence)
        OR NOT EXISTS (SELECT 1 FROM control.topology_streams
            WHERE dataset_id = s.dataset_id AND topology_generation = s.target_topology
                AND shard_id = s.shard_id AND owner_epoch = 2
                AND handoff_last_complete_sequence IS NULL)
        OR NOT EXISTS (SELECT 1 FROM control.config_revisions c JOIN control.policy_revisions p
            ON p.dataset_id = c.dataset_id AND p.config_revision = c.revision
            WHERE c.dataset_id = s.dataset_id AND c.revision = s.config_revision
                AND c.document_digest = s.config_digest AND p.revision = s.policy_revision
                AND p.document_digest = s.policy_digest) THEN
        RAISE EXCEPTION 'prepared successor topology or policy binding differs'
            USING ERRCODE = '23514';
    END IF;
    IF current_owner.lease_expires_at_us <= (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint THEN
        RAISE EXCEPTION 'succession Control authority expired' USING ERRCODE = '23514';
    END IF;
END $$;

CREATE FUNCTION control.guard_initial_ingest_succession() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog AS $$
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'initial succession intents are retained and immutable' USING ERRCODE = '23514';
    END IF;
    PERFORM control.require_control_owner(NEW.dataset_id, NEW.prepared_by_resource,
        NEW.prepared_by_epoch, NEW.prepared_by_incarnation);
    IF NOT EXISTS (SELECT 1 FROM control.datasets WHERE dataset_id = NEW.dataset_id
        AND config_revision = NEW.config_revision AND policy_revision = NEW.policy_revision) THEN
        RAISE EXCEPTION 'succession preparation requires current accepted policy' USING ERRCODE = '23514';
    END IF;
    PERFORM control.require_prepared_initial_successor(NEW.owner_resource_id);
    RETURN NULL;
END $$;
CREATE TRIGGER initial_ingest_succession_guard AFTER INSERT OR UPDATE OR DELETE
    ON control.initial_ingest_successions FOR EACH ROW EXECUTE FUNCTION control.guard_initial_ingest_succession();
CREATE TRIGGER initial_ingest_succession_truncate_guard BEFORE TRUNCATE
    ON control.initial_ingest_successions FOR EACH STATEMENT EXECUTE FUNCTION control.guard_initial_ingest_succession();
REVOKE ALL ON control.initial_ingest_successions FROM PUBLIC;
REVOKE ALL ON FUNCTION control.veto_unactivated_producer() FROM PUBLIC;
REVOKE ALL ON FUNCTION control.require_prepared_initial_successor(bytea) FROM PUBLIC;
REVOKE ALL ON FUNCTION control.guard_initial_ingest_succession() FROM PUBLIC;
