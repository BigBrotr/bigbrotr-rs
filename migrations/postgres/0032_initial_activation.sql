-- A live writer is a continuous local capability, never reconstructed from this
-- immutable receipt. The receipt distinguishes active-zero from prepared-empty.
CREATE TABLE control.initial_ingest_activations (
    dataset_id control.id16 NOT NULL,
    owner_resource_id control.id16 PRIMARY KEY REFERENCES control.initial_ingest_successions(owner_resource_id),
    target_epoch control.u64 NOT NULL CHECK (target_epoch = 2),
    target_incarnation control.id16 NOT NULL,
    target_role_instance_id control.id16 NOT NULL,
    producer_slot_id control.id16 NOT NULL UNIQUE,
    producer_epoch control.u64 NOT NULL CHECK (producer_epoch = 2),
    previous_manifest_sha256 control.digest32 NOT NULL,
    target_manifest_sha256 control.digest32 NOT NULL,
    handoff_sha256 control.digest32 NOT NULL,
    prepared_by_resource control.id16 NOT NULL,
    prepared_by_epoch control.u64 NOT NULL,
    prepared_by_incarnation control.id16 NOT NULL,
    activated_by_resource control.id16 NOT NULL,
    activated_by_epoch control.u64 NOT NULL,
    activated_by_incarnation control.id16 NOT NULL,
    config_revision control.u64 NOT NULL,
    policy_revision control.u64 NOT NULL,
    config_digest control.digest32 NOT NULL,
    policy_digest control.digest32 NOT NULL,
    activated_at_us bigint NOT NULL,
    activation_transaction xid8 NOT NULL DEFAULT pg_current_xact_id(),
    CHECK (ROW(prepared_by_resource, prepared_by_epoch, prepared_by_incarnation)
        = ROW(activated_by_resource, activated_by_epoch, activated_by_incarnation)),
    FOREIGN KEY (producer_slot_id, producer_epoch, dataset_id)
        REFERENCES control.producer_epochs(producer_slot_id, producer_epoch, dataset_id),
    FOREIGN KEY (dataset_id, config_revision) REFERENCES control.config_revisions(dataset_id, revision),
    FOREIGN KEY (dataset_id, policy_revision) REFERENCES control.policy_revisions(dataset_id, revision)
);

CREATE OR REPLACE FUNCTION control.require_prepared_initial_successor(wanted_resource bytea)
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
    IF EXISTS (SELECT 1 FROM control.initial_ingest_activations
        WHERE owner_resource_id = wanted_resource) THEN
        RAISE EXCEPTION 'activated initial writer is not an unactivated preparation'
            USING ERRCODE = '23514';
    END IF;
END $$;

CREATE OR REPLACE FUNCTION control.lock_initial_empty_retirement(wanted_resource bytea, wanted_digest bytea)
RETURNS control.initial_ingest_successions LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE s control.initial_ingest_successions%ROWTYPE;
        current_owner control.owner_epochs%ROWTYPE;
        old_owner control.owner_epochs%ROWTYPE;
        old_producer control.producer_epochs%ROWTYPE;
BEGIN
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'empty retirement requires READ COMMITTED' USING ERRCODE = '25001';
    END IF;
    SELECT * INTO STRICT s FROM control.initial_ingest_successions
        WHERE owner_resource_id = wanted_resource;
    IF octet_length(s.handoff) > 610
        OR wanted_digest IS DISTINCT FROM control.initial_empty_retirement_digest(s) THEN
        RAISE EXCEPTION 'empty retirement digest differs from retained preparation' USING ERRCODE = '23514';
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
        OR current_owner.epoch < s.prepared_by_epoch
        OR NOT EXISTS (SELECT 1 FROM control.owner_epochs o JOIN control.role_instances r
            ON r.role_instance_id = o.role_instance_id
            WHERE o.dataset_id = s.dataset_id AND o.resource_id = s.prepared_by_resource
                AND o.epoch = s.prepared_by_epoch AND o.process_incarnation = s.prepared_by_incarnation
                AND o.role = 'control' AND r.dataset_id = s.dataset_id AND r.role = 'control'
                AND o.control_fence_version = 1
                AND ((o.epoch = current_owner.epoch AND o.state = 'active'
                        AND o.process_incarnation = current_owner.process_incarnation
                        AND r.retired_at_us IS NULL)
                    OR (o.epoch < current_owner.epoch AND o.state = 'released'
                        AND o.exclusion_method = 'storage_fenced' AND r.retired_at_us IS NOT NULL)))
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
        RAISE EXCEPTION 'empty retirement requires exact preparation and current Control policy'
            USING ERRCODE = '23514';
    END IF;

    -- Same parent order as the original retirement registry adapter, followed
    -- by the target parents. All domain work is bounded by the nine known kinds.
    SELECT * INTO STRICT old_owner FROM control.owner_epochs
        WHERE resource_id = s.owner_resource_id AND epoch = 1 FOR UPDATE;
    PERFORM 1 FROM control.role_instances WHERE role_instance_id = old_owner.role_instance_id FOR UPDATE;
    PERFORM 1 FROM control.allocation_domains
        WHERE dataset_id = s.dataset_id AND owner_resource_id = s.owner_resource_id AND owner_epoch = 1
        ORDER BY dictionary_kind, allocation_domain LIMIT 10 FOR UPDATE;
    IF (SELECT count(*) FROM (SELECT 1 FROM control.allocation_domains
        WHERE dataset_id = s.dataset_id AND owner_resource_id = s.owner_resource_id AND owner_epoch = 1
        LIMIT 10) AS bounded) > 9 THEN
        RAISE EXCEPTION 'initial predecessor domain set exceeds its finite bound' USING ERRCODE = '23514';
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
        RAISE EXCEPTION 'empty retirement requires the exact retired initial predecessor'
            USING ERRCODE = '23514';
    END IF;

    PERFORM 1 FROM control.owner_epochs WHERE resource_id = s.owner_resource_id AND epoch = 2 FOR UPDATE;
    PERFORM 1 FROM control.role_instances WHERE role_instance_id = s.target_role_instance_id FOR UPDATE;
    PERFORM 1 FROM control.allocation_domains
        WHERE dataset_id = s.dataset_id AND owner_resource_id = s.owner_resource_id AND owner_epoch = 2
        ORDER BY dictionary_kind, allocation_domain LIMIT 10 FOR UPDATE;
    IF (SELECT count(*) FROM (SELECT 1 FROM control.allocation_domains
        WHERE dataset_id = s.dataset_id AND owner_resource_id = s.owner_resource_id AND owner_epoch = 2
        LIMIT 10) AS bounded) > 9 THEN
        RAISE EXCEPTION 'empty retirement target domain set exceeds its finite bound' USING ERRCODE = '23514';
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
                AND o.lease_expires_at_us = s.lease_expires_at_us
                AND r.dataset_id = s.dataset_id AND r.role = 'ingest' AND r.created_at_us = s.created_at_us)
        OR NOT EXISTS (SELECT 1 FROM control.producer_epochs
            WHERE producer_slot_id = s.producer_slot_id AND producer_epoch = 2 AND dataset_id = s.dataset_id
                AND producer_role_instance_id = s.target_role_instance_id AND producer_role = 'ingest'
                AND producer_incarnation = s.target_incarnation AND owner_resource_id = s.owner_resource_id
                AND owner_epoch = 2 AND owner_role = 'ingest' AND owner_incarnation = s.target_incarnation
                AND reserved_through_request = 0 AND opened_at_us = s.created_at_us
                AND outstanding_request_limit = s.outstanding_request_limit)
        OR NOT EXISTS (SELECT 1 FROM control.source_streams
            WHERE dataset_id = s.dataset_id AND shard_id = s.shard_id AND owner_epoch = 2
                AND owner_resource_id = s.owner_resource_id AND owner_role = 'ingest'
                AND owner_incarnation = s.target_incarnation AND opened_at_us = s.created_at_us)
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
        RAISE EXCEPTION 'empty retirement target registry identity differs' USING ERRCODE = '23514';
    END IF;
    -- The typed caller also compares the complete bounded domain set with the
    -- decoded handoff. A count or kind census alone is not a native identity proof.
    IF EXISTS (SELECT 1 FROM control.producer_requests WHERE producer_slot_id = s.producer_slot_id AND producer_epoch = 2)
        OR EXISTS (SELECT 1 FROM control.producer_outcomes WHERE producer_slot_id = s.producer_slot_id AND producer_epoch = 2)
        OR EXISTS (SELECT 1 FROM control.producer_checkpoints WHERE producer_slot_id = s.producer_slot_id AND producer_epoch = 2)
        OR EXISTS (SELECT 1 FROM control.producer_input_bindings WHERE producer_slot_id = s.producer_slot_id AND producer_epoch = 2)
        OR EXISTS (SELECT 1 FROM control.acquisition_gaps WHERE producer_slot_id = s.producer_slot_id AND producer_epoch = 2)
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
        RAISE EXCEPTION 'empty retirement requires absent target detail and exact sealed topology'
            USING ERRCODE = '23514';
    END IF;
    IF current_owner.lease_expires_at_us <= (EXTRACT(EPOCH FROM clock_timestamp()) * 1000000)::bigint THEN
        RAISE EXCEPTION 'empty retirement Control authority expired' USING ERRCODE = '23514';
    END IF;
    IF EXISTS (SELECT 1 FROM control.initial_ingest_activations
        WHERE owner_resource_id = wanted_resource) THEN
        RAISE EXCEPTION 'activated initial writer is not an unactivated preparation'
            USING ERRCODE = '23514';
    END IF;
    RETURN s;
END $$;

-- This function validates SQL authority and scheduling, never native custody.
-- The typed owner additionally compares all target allocation-domain identities.
CREATE FUNCTION control.require_active_initial_writer(wanted_resource bytea)
RETURNS bigint LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE a control.initial_ingest_activations%ROWTYPE;
        s control.initial_ingest_successions%ROWTYPE;
        c control.owner_epochs%ROWTYPE;
        i control.owner_epochs%ROWTYPE;
        now_us bigint;
BEGIN
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'initial activation requires READ COMMITTED' USING ERRCODE = '25001';
    END IF;
    SELECT * INTO STRICT a FROM control.initial_ingest_activations WHERE owner_resource_id=wanted_resource;
    SELECT * INTO STRICT s FROM control.initial_ingest_successions WHERE owner_resource_id=wanted_resource;
    PERFORM control.require_control_owner(a.dataset_id,a.activated_by_resource,a.activated_by_epoch,a.activated_by_incarnation);
    PERFORM 1 FROM control.datasets WHERE dataset_id=a.dataset_id FOR UPDATE;
    SELECT * INTO STRICT c FROM control.owner_epochs
        WHERE resource_id=a.activated_by_resource AND epoch=a.activated_by_epoch FOR SHARE;
    SELECT * INTO STRICT i FROM control.owner_epochs
        WHERE resource_id=a.owner_resource_id AND epoch=2 FOR SHARE;
    PERFORM 1 FROM control.role_instances WHERE role_instance_id=a.target_role_instance_id FOR SHARE;
    PERFORM 1 FROM control.allocation_domains WHERE dataset_id=a.dataset_id
        AND owner_resource_id=a.owner_resource_id AND owner_epoch=2
        ORDER BY dictionary_kind,allocation_domain LIMIT 10 FOR SHARE;
    PERFORM 1 FROM control.source_streams WHERE dataset_id=a.dataset_id AND shard_id=s.shard_id AND owner_epoch=2 FOR SHARE;
    PERFORM 1 FROM control.producer_epochs WHERE producer_slot_id=a.producer_slot_id AND producer_epoch=2 FOR UPDATE;
    IF ROW(a.dataset_id,a.target_epoch,a.target_incarnation,a.target_role_instance_id,
            a.producer_slot_id,a.producer_epoch,a.previous_manifest_sha256,a.target_manifest_sha256,a.handoff_sha256,
            a.prepared_by_resource,a.prepared_by_epoch,a.prepared_by_incarnation,
            a.config_revision,a.policy_revision,a.config_digest,a.policy_digest)
        IS DISTINCT FROM ROW(s.dataset_id,s.target_epoch,s.target_incarnation,s.target_role_instance_id,
            s.producer_slot_id,s.producer_epoch,s.previous_manifest_sha256,s.target_manifest_sha256,s.handoff_sha256,
            s.prepared_by_resource,s.prepared_by_epoch,s.prepared_by_incarnation,
            s.config_revision,s.policy_revision,s.config_digest,s.policy_digest)
        OR a.activated_at_us < s.created_at_us
        OR NOT EXISTS (SELECT 1 FROM control.datasets WHERE dataset_id=a.dataset_id
            AND config_revision=a.config_revision AND policy_revision=a.policy_revision AND topology_generation=s.target_topology)
        OR NOT EXISTS (SELECT 1 FROM control.config_revisions cr JOIN control.policy_revisions pr
            ON pr.dataset_id=cr.dataset_id AND pr.config_revision=cr.revision
            WHERE cr.dataset_id=a.dataset_id AND cr.revision=a.config_revision AND cr.document_digest=a.config_digest
                AND pr.revision=a.policy_revision AND pr.document_digest=a.policy_digest)
        OR i.dataset_id<>a.dataset_id OR i.role<>'ingest' OR i.state<>'active'
        OR i.process_incarnation<>a.target_incarnation OR i.role_instance_id<>a.target_role_instance_id
        OR i.acquired_at_us<>s.created_at_us
        OR NOT EXISTS (SELECT 1 FROM control.role_instances WHERE role_instance_id=a.target_role_instance_id
            AND dataset_id=a.dataset_id AND role='ingest' AND created_at_us=s.created_at_us AND retired_at_us IS NULL)
        OR NOT EXISTS (SELECT 1 FROM control.producer_epochs WHERE producer_slot_id=a.producer_slot_id
            AND producer_epoch=2 AND dataset_id=a.dataset_id AND producer_role_instance_id=a.target_role_instance_id
            AND producer_role='ingest' AND producer_incarnation=a.target_incarnation
            AND owner_resource_id=a.owner_resource_id AND owner_epoch=2 AND owner_role='ingest'
            AND owner_incarnation=a.target_incarnation AND state='open' AND opened_at_us=s.created_at_us
            AND outstanding_request_limit=s.outstanding_request_limit)
        OR NOT EXISTS (SELECT 1 FROM control.source_streams WHERE dataset_id=a.dataset_id
            AND shard_id=s.shard_id AND owner_epoch=2 AND owner_resource_id=a.owner_resource_id
            AND owner_role='ingest' AND owner_incarnation=a.target_incarnation AND state='open' AND opened_at_us=s.created_at_us)
        OR (SELECT count(*) FROM (SELECT 1 FROM control.producer_epochs WHERE dataset_id=a.dataset_id
            AND owner_resource_id=a.owner_resource_id AND owner_epoch=2 LIMIT 2) AS bounded)<>1
        OR (SELECT count(*) FROM (SELECT 1 FROM control.source_streams WHERE dataset_id=a.dataset_id
            AND owner_resource_id=a.owner_resource_id AND owner_epoch=2 LIMIT 2) AS bounded)<>1
        OR (SELECT count(*) FROM (SELECT 1 FROM control.allocation_domains WHERE dataset_id=a.dataset_id
            AND owner_resource_id=a.owner_resource_id AND owner_epoch=2 LIMIT 10) AS bounded)>9
        OR EXISTS (SELECT 1 FROM control.allocation_domains WHERE dataset_id=a.dataset_id
            AND owner_resource_id=a.owner_resource_id AND owner_epoch=2
            AND (state<>'active' OR owner_role<>'ingest' OR owner_incarnation<>a.target_incarnation OR created_at_us<>s.created_at_us))
        OR NOT EXISTS (SELECT 1 FROM control.topology_generations WHERE dataset_id=a.dataset_id
            AND generation=s.target_topology AND previous_generation=s.previous_topology AND sealed AND created_at_us=s.created_at_us)
        OR (SELECT count(*) FROM (SELECT 1 FROM control.topology_streams WHERE dataset_id=a.dataset_id
            AND topology_generation=s.target_topology LIMIT 3) AS bounded)<>2
        OR NOT EXISTS (SELECT 1 FROM control.topology_streams WHERE dataset_id=a.dataset_id AND topology_generation=s.target_topology
            AND shard_id=s.shard_id AND owner_epoch=1 AND handoff_last_complete_sequence=s.last_complete_sequence)
        OR NOT EXISTS (SELECT 1 FROM control.topology_streams WHERE dataset_id=a.dataset_id AND topology_generation=s.target_topology
            AND shard_id=s.shard_id AND owner_epoch=2 AND handoff_last_complete_sequence IS NULL) THEN
        RAISE EXCEPTION 'active initial writer metadata or accepted policy differs' USING ERRCODE='23514';
    END IF;
    now_us := (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint;
    IF c.lease_expires_at_us<=now_us OR i.lease_expires_at_us<=now_us THEN
        RAISE EXCEPTION 'active initial writer scheduling authority expired' USING ERRCODE='23514';
    END IF;
    RETURN LEAST(c.lease_expires_at_us,i.lease_expires_at_us)-now_us;
END $$;

CREATE FUNCTION control.guard_initial_ingest_activation() RETURNS trigger LANGUAGE plpgsql
SET search_path=pg_catalog AS $$
DECLARE s control.initial_ingest_successions%ROWTYPE;
        c control.owner_epochs%ROWTYPE;
        ingest_deadline bigint;
BEGIN
    IF TG_OP<>'INSERT' THEN
        RAISE EXCEPTION 'initial activation receipts are retained and immutable' USING ERRCODE='23514';
    END IF;
    SELECT * INTO STRICT s FROM control.initial_ingest_successions WHERE owner_resource_id=NEW.owner_resource_id;
    -- Serialize activation against same-Control work before dataset/parent locks.
    SELECT * INTO STRICT c FROM control.owner_epochs
        WHERE resource_id=NEW.activated_by_resource AND epoch=NEW.activated_by_epoch FOR UPDATE;
    PERFORM control.require_control_owner(NEW.dataset_id,NEW.activated_by_resource,NEW.activated_by_epoch,NEW.activated_by_incarnation);
    PERFORM 1 FROM control.datasets WHERE dataset_id=NEW.dataset_id FOR UPDATE;
    PERFORM control.require_prepared_initial_successor(NEW.owner_resource_id);
    IF ROW(NEW.dataset_id,NEW.target_epoch,NEW.target_incarnation,NEW.target_role_instance_id,
            NEW.producer_slot_id,NEW.producer_epoch,NEW.previous_manifest_sha256,NEW.target_manifest_sha256,NEW.handoff_sha256,
            NEW.prepared_by_resource,NEW.prepared_by_epoch,NEW.prepared_by_incarnation,
            NEW.activated_by_resource,NEW.activated_by_epoch,NEW.activated_by_incarnation,
            NEW.config_revision,NEW.policy_revision,NEW.config_digest,NEW.policy_digest)
        IS DISTINCT FROM ROW(s.dataset_id,s.target_epoch,s.target_incarnation,s.target_role_instance_id,
            s.producer_slot_id,s.producer_epoch,s.previous_manifest_sha256,s.target_manifest_sha256,s.handoff_sha256,
            s.prepared_by_resource,s.prepared_by_epoch,s.prepared_by_incarnation,
            s.prepared_by_resource,s.prepared_by_epoch,s.prepared_by_incarnation,
            s.config_revision,s.policy_revision,s.config_digest,s.policy_digest)
        OR NOT EXISTS (SELECT 1 FROM control.datasets WHERE dataset_id=s.dataset_id
            AND config_revision=s.config_revision AND policy_revision=s.policy_revision) THEN
        RAISE EXCEPTION 'activation requires the exact current preparer and preparation policy' USING ERRCODE='23514';
    END IF;
    SELECT lease_expires_at_us INTO STRICT ingest_deadline FROM control.owner_epochs
        WHERE resource_id=s.owner_resource_id AND epoch=2 FOR SHARE;
    NEW.activation_transaction := pg_current_xact_id();
    NEW.activated_at_us := (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint;
    IF NEW.activated_at_us<s.created_at_us OR c.lease_expires_at_us<=NEW.activated_at_us
        OR ingest_deadline<=NEW.activated_at_us THEN
        RAISE EXCEPTION 'activation cannot revive expired scheduling authority' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER initial_ingest_activation_guard BEFORE INSERT OR UPDATE OR DELETE
    ON control.initial_ingest_activations FOR EACH ROW EXECUTE FUNCTION control.guard_initial_ingest_activation();
CREATE TRIGGER initial_ingest_activation_truncate_guard BEFORE TRUNCATE
    ON control.initial_ingest_activations FOR EACH STATEMENT EXECUTE FUNCTION control.guard_initial_ingest_activation();
CREATE FUNCTION control.require_initial_activation_commit() RETURNS trigger LANGUAGE plpgsql
SET search_path=pg_catalog AS $$
BEGIN
    PERFORM control.require_active_initial_writer(NEW.owner_resource_id);
    -- Activation is committed before the first request, even in a raw SQL
    -- transaction. This deferred check runs only for the activation INSERT;
    -- ordinary later requests use the separate active validator.
    IF NOT EXISTS (SELECT 1 FROM control.producer_epochs WHERE producer_slot_id=NEW.producer_slot_id
            AND producer_epoch=2 AND reserved_through_request=0)
        OR EXISTS (SELECT 1 FROM control.producer_requests WHERE producer_slot_id=NEW.producer_slot_id AND producer_epoch=2)
        OR EXISTS (SELECT 1 FROM control.producer_outcomes WHERE producer_slot_id=NEW.producer_slot_id AND producer_epoch=2)
        OR EXISTS (SELECT 1 FROM control.producer_input_bindings WHERE producer_slot_id=NEW.producer_slot_id AND producer_epoch=2)
        OR EXISTS (SELECT 1 FROM control.producer_checkpoints WHERE producer_slot_id=NEW.producer_slot_id AND producer_epoch=2)
        OR EXISTS (SELECT 1 FROM control.acquisition_gaps WHERE producer_slot_id=NEW.producer_slot_id AND producer_epoch=2) THEN
        RAISE EXCEPTION 'activation must commit before the first target request or obligation' USING ERRCODE='23514';
    END IF;
    RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER initial_activation_complete AFTER INSERT ON control.initial_ingest_activations
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION control.require_initial_activation_commit();

-- This guard precedes the original request/job guards. Do not take the producer
-- UPDATE lock ahead of the input-binding job lock. Dataset custody serializes
-- target changes; the existing guards take the remaining job/producer locks.
CREATE OR REPLACE FUNCTION control.veto_unactivated_producer() RETURNS trigger LANGUAGE plpgsql
SET search_path=pg_catalog AS $$
DECLARE a control.initial_ingest_activations%ROWTYPE;
        p control.producer_epochs%ROWTYPE;
        c control.owner_epochs%ROWTYPE;
        i control.owner_epochs%ROWTYPE;
        now_us bigint;
BEGIN
    SELECT * INTO STRICT p FROM control.producer_epochs
        WHERE producer_slot_id=NEW.producer_slot_id AND producer_epoch=NEW.producer_epoch;
    IF NEW.producer_epoch=1 AND p.owner_epoch=1 THEN RETURN NEW; END IF;
    IF NEW.producer_epoch<>2 OR p.owner_epoch<>2 THEN
        RAISE EXCEPTION 'producer epoch has no selected activation protocol' USING ERRCODE='23514';
    END IF;
    IF current_setting('transaction_isolation')<>'read committed' THEN
        RAISE EXCEPTION 'activated requests require READ COMMITTED' USING ERRCODE='25001';
    END IF;
    SELECT * INTO STRICT a FROM control.initial_ingest_activations WHERE producer_slot_id=NEW.producer_slot_id;
    IF a.activation_transaction=pg_current_xact_id() THEN
        RAISE EXCEPTION 'target admission requires a previously committed activation' USING ERRCODE='23514';
    END IF;
    PERFORM control.require_control_owner(NEW.dataset_id,NEW.issuer_resource,NEW.issuer_epoch,NEW.issuer_incarnation);
    PERFORM 1 FROM control.datasets WHERE dataset_id=NEW.dataset_id FOR UPDATE;
    SELECT * INTO STRICT c FROM control.owner_epochs WHERE resource_id=NEW.issuer_resource AND epoch=NEW.issuer_epoch FOR SHARE;
    SELECT * INTO STRICT i FROM control.owner_epochs WHERE resource_id=p.owner_resource_id AND epoch=2 FOR SHARE;
    IF ROW(NEW.dataset_id,NEW.manifest_sha256,NEW.issuer_resource,NEW.issuer_epoch,NEW.issuer_incarnation)
        IS DISTINCT FROM ROW(a.dataset_id,a.target_manifest_sha256,a.activated_by_resource,a.activated_by_epoch,a.activated_by_incarnation)
        OR p.dataset_id<>a.dataset_id OR p.owner_resource_id<>a.owner_resource_id
        OR p.owner_incarnation<>a.target_incarnation OR p.producer_incarnation<>a.target_incarnation
        OR p.producer_role_instance_id<>a.target_role_instance_id OR p.state<>'open'
        OR i.state<>'active' OR i.process_incarnation<>a.target_incarnation OR i.role_instance_id<>a.target_role_instance_id
        OR NOT EXISTS (SELECT 1 FROM control.datasets WHERE dataset_id=a.dataset_id
            AND config_revision=a.config_revision AND policy_revision=a.policy_revision) THEN
        RAISE EXCEPTION 'request requires its activated target manifest and actual activating Control' USING ERRCODE='23514';
    END IF;
    now_us := (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint;
    IF c.lease_expires_at_us<=now_us OR i.lease_expires_at_us<=now_us THEN
        RAISE EXCEPTION 'activated request scheduling authority expired' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
REVOKE ALL ON control.initial_ingest_activations FROM PUBLIC;
REVOKE ALL ON FUNCTION control.require_active_initial_writer(bytea) FROM PUBLIC;
REVOKE ALL ON FUNCTION control.guard_initial_ingest_activation() FROM PUBLIC;
REVOKE ALL ON FUNCTION control.require_initial_activation_commit() FROM PUBLIC;

-- A zero checkpoint has no request FK and could be inserted after SET
-- CONSTRAINTS has fired the activation check. Outcomes/input bindings require
-- a request; acquisition gaps remain restricted to epoch one by their schema.
-- This independent target obligation must also follow a committed receipt.
CREATE FUNCTION control.guard_initial_activation_checkpoint() RETURNS trigger LANGUAGE plpgsql
SET search_path=pg_catalog AS $$
BEGIN
    IF NEW.producer_epoch=2 AND EXISTS (SELECT 1 FROM control.initial_ingest_activations
        WHERE producer_slot_id=NEW.producer_slot_id AND activation_transaction=pg_current_xact_id()) THEN
        RAISE EXCEPTION 'target checkpoint requires a previously committed activation' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER initial_activation_checkpoint_guard BEFORE INSERT OR UPDATE ON control.producer_checkpoints
    FOR EACH ROW EXECUTE FUNCTION control.guard_initial_activation_checkpoint();
REVOKE ALL ON FUNCTION control.guard_initial_activation_checkpoint() FROM PUBLIC;

-- SET CONSTRAINTS can force the receipt's deferred check early. Preserve its
-- active-zero shape and the mutable Control/policy envelope for the remainder
-- of that same transaction too. Later transactions reauthorize independently.
-- This is a mutation fence, not a promise about arbitrary client delays after
-- forcing constraint checks: the typed owner also retains its local deadline
-- across COMMIT and validates current authority again before writer delivery.
CREATE FUNCTION control.guard_initial_activation_target() RETURNS trigger LANGUAGE plpgsql
SET search_path=pg_catalog AS $$
DECLARE resource bytea;
BEGIN
    IF TG_TABLE_NAME='datasets' THEN
        IF ROW(OLD.config_revision,OLD.policy_revision,OLD.topology_generation)
            IS NOT DISTINCT FROM ROW(NEW.config_revision,NEW.policy_revision,NEW.topology_generation) THEN
            RETURN NEW;
        END IF;
        SELECT owner_resource_id INTO resource FROM control.initial_ingest_activations
            WHERE dataset_id=OLD.dataset_id AND activation_transaction=pg_current_xact_id();
    ELSIF TG_TABLE_NAME='role_instances' THEN
        SELECT a.owner_resource_id INTO resource FROM control.initial_ingest_activations a
            WHERE a.activation_transaction=pg_current_xact_id()
                AND (a.target_role_instance_id=OLD.role_instance_id OR EXISTS (
                    SELECT 1 FROM control.owner_epochs c WHERE c.resource_id=a.activated_by_resource
                        AND c.epoch=a.activated_by_epoch AND c.process_incarnation=a.activated_by_incarnation
                        AND c.role_instance_id=OLD.role_instance_id));
    ELSIF TG_TABLE_NAME='owner_epochs' THEN
        IF OLD.role='control' THEN
            SELECT owner_resource_id INTO resource FROM control.initial_ingest_activations
                WHERE activated_by_resource=OLD.resource_id AND activated_by_epoch=OLD.epoch
                    AND activated_by_incarnation=OLD.process_incarnation
                    AND activation_transaction=pg_current_xact_id();
        ELSIF OLD.role='ingest' AND OLD.epoch=2 THEN
            resource := OLD.resource_id;
        ELSE
            RETURN NEW;
        END IF;
    ELSE
        IF OLD.owner_epoch<>2 THEN RETURN NEW; END IF;
        resource := OLD.owner_resource_id;
    END IF;
    IF EXISTS (SELECT 1 FROM control.initial_ingest_activations WHERE owner_resource_id=resource
        AND activation_transaction=pg_current_xact_id()) THEN
        RAISE EXCEPTION 'activation authority and target must remain unchanged through its commit' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER initial_activation_target_guard BEFORE UPDATE ON control.datasets
    FOR EACH ROW EXECUTE FUNCTION control.guard_initial_activation_target();
CREATE TRIGGER initial_activation_target_guard BEFORE UPDATE ON control.owner_epochs
    FOR EACH ROW EXECUTE FUNCTION control.guard_initial_activation_target();
CREATE TRIGGER initial_activation_target_guard BEFORE UPDATE ON control.role_instances
    FOR EACH ROW EXECUTE FUNCTION control.guard_initial_activation_target();
CREATE TRIGGER initial_activation_target_guard BEFORE UPDATE ON control.allocation_domains
    FOR EACH ROW EXECUTE FUNCTION control.guard_initial_activation_target();
CREATE TRIGGER initial_activation_target_guard BEFORE UPDATE ON control.source_streams
    FOR EACH ROW EXECUTE FUNCTION control.guard_initial_activation_target();
CREATE TRIGGER initial_activation_target_guard BEFORE UPDATE ON control.producer_epochs
    FOR EACH ROW EXECUTE FUNCTION control.guard_initial_activation_target();
REVOKE ALL ON FUNCTION control.guard_initial_activation_target() FROM PUBLIC;
