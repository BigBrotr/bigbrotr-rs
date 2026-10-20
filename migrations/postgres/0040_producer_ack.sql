-- Separate the bounded retained history (64 requests / 32 MiB) from the
-- reusable outstanding window. Validated PostgreSQL C coordinates reservations;
-- only a confirmed checkpoint followed by a synced native ACK establishes A.
-- This migration neither performs native ACK nor deletes retained originals.
-- Keep every earlier migration immutable and preserve historical closure rules.


CREATE OR REPLACE FUNCTION control.guard_producer_request() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
DECLARE producer control.producer_epochs%ROWTYPE;
        retained_bytes bigint;
        progress control.producer_checkpoints%ROWTYPE;
        complete_through numeric := 0;
BEGIN
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
    ELSIF progress.progress_format = 1 THEN
        PERFORM control.require_retained_producer_progress(NEW.producer_slot_id, NEW.producer_epoch);
        IF progress.manifest_sha256 <> NEW.manifest_sha256 THEN
            RAISE EXCEPTION 'reservation differs from its typed progress native context'
                USING ERRCODE = '23514';
        END IF;
        complete_through := progress.complete_through_request;
    END IF;
    IF NEW.request_sequence > complete_through + producer.outstanding_request_limit THEN
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

-- Retained reservations can exceed the active window after native ACK.
CREATE OR REPLACE FUNCTION control.require_activated_initial_recovery(wanted_resource bytea)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $$
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
                AND o.state = 'active'
                AND r.dataset_id = s.dataset_id AND r.role = 'ingest' AND r.created_at_us = s.created_at_us
                AND r.retired_at_us IS NULL)
        OR NOT EXISTS (SELECT 1 FROM control.producer_epochs
            WHERE producer_slot_id = s.producer_slot_id AND producer_epoch = 2 AND dataset_id = s.dataset_id
                AND producer_role_instance_id = s.target_role_instance_id AND producer_role = 'ingest'
                AND producer_incarnation = s.target_incarnation AND owner_resource_id = s.owner_resource_id
                AND owner_epoch = 2 AND owner_role = 'ingest' AND owner_incarnation = s.target_incarnation
                AND state IN ('open', 'closing') AND opened_at_us = s.created_at_us
                AND outstanding_request_limit = s.outstanding_request_limit
                AND reserved_through_request <= 64)
        OR NOT EXISTS (SELECT 1 FROM control.source_streams
            WHERE dataset_id = s.dataset_id AND shard_id = s.shard_id AND owner_epoch = 2
                AND owner_resource_id = s.owner_resource_id AND owner_role = 'ingest'
                AND owner_incarnation = s.target_incarnation AND state = 'open' AND opened_at_us = s.created_at_us)
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
                AND (state <> 'active' OR owner_role <> 'ingest' OR owner_incarnation <> s.target_incarnation
                    OR created_at_us <> s.created_at_us)) THEN
        RAISE EXCEPTION 'activated recovery target registry identity differs' USING ERRCODE = '23514';
    END IF;
    -- The typed caller also compares all eighteen domain IDs with both decoded
    -- manifests. SQL metadata never substitutes for the native closing proof.
    -- Positive unique sequences with count=max=reserved form the exact retained
    -- prefix, including zero. Native issued may be lower: never infer its tail.
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
    IF current_owner.lease_expires_at_us <= (EXTRACT(EPOCH FROM clock_timestamp()) * 1000000)::bigint THEN
        RAISE EXCEPTION 'activated recovery Control authority expired' USING ERRCODE = '23514';
    END IF;
    RETURN;
END $$;

-- Retained reservations can exceed the active window after native ACK.
CREATE OR REPLACE FUNCTION control.require_activated_initial_source(wanted_resource bytea)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $$
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
                AND o.state IN ('active','draining')
                AND r.dataset_id = s.dataset_id AND r.role = 'ingest' AND r.created_at_us = s.created_at_us
                AND r.retired_at_us IS NULL)
        OR NOT EXISTS (SELECT 1 FROM control.producer_epochs
            WHERE producer_slot_id = s.producer_slot_id AND producer_epoch = 2 AND dataset_id = s.dataset_id
                AND producer_role_instance_id = s.target_role_instance_id AND producer_role = 'ingest'
                AND producer_incarnation = s.target_incarnation AND owner_resource_id = s.owner_resource_id
                AND owner_epoch = 2 AND owner_role = 'ingest' AND owner_incarnation = s.target_incarnation
                AND state IN ('open', 'closing') AND opened_at_us = s.created_at_us
                AND outstanding_request_limit = s.outstanding_request_limit
                AND reserved_through_request <= 64)
        OR NOT EXISTS (SELECT 1 FROM control.source_streams
            WHERE dataset_id = s.dataset_id AND shard_id = s.shard_id AND owner_epoch = 2
                AND owner_resource_id = s.owner_resource_id AND owner_role = 'ingest'
                AND owner_incarnation = s.target_incarnation AND state IN ('open','closed') AND opened_at_us = s.created_at_us)
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
                AND (state <> 'active' OR owner_role <> 'ingest' OR owner_incarnation <> s.target_incarnation
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
    -- A terminal source is an observation of a complete copied prefix. It is
    -- never permission to repair a missing outcome or adopt an intermediate
    -- source=closing shape. Native bytes and the closure digest are checked by
    -- the typed source witness, not inferred from these coordination rows.
    IF EXISTS (SELECT 1 FROM control.source_streams
        WHERE dataset_id=s.dataset_id AND shard_id=s.shard_id AND owner_epoch=2 AND state='closed') THEN
        IF NOT EXISTS (SELECT 1 FROM control.producer_epochs
                WHERE producer_slot_id=s.producer_slot_id AND producer_epoch=2 AND state='closing')
            OR EXISTS (SELECT 1 FROM control.producer_requests r
                LEFT JOIN control.producer_outcomes o USING(producer_slot_id,producer_epoch,request_sequence)
                WHERE r.producer_slot_id=s.producer_slot_id AND r.producer_epoch=2
                    AND (o.request_sequence IS NULL OR o.dataset_id<>r.dataset_id
                        OR o.request_digest<>r.request_digest OR o.manifest_sha256<>r.manifest_sha256)) THEN
            RAISE EXCEPTION 'activated source closure requires its closing producer and complete outcome copies'
                USING ERRCODE='23514';
        END IF;
    END IF;
    IF current_owner.lease_expires_at_us <= (EXTRACT(EPOCH FROM clock_timestamp()) * 1000000)::bigint THEN
        RAISE EXCEPTION 'activated recovery Control authority expired' USING ERRCODE = '23514';
    END IF;
    RETURN;
END $$;

-- Retained reservations can exceed the active window after native ACK.
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
                AND reserved_through_request <= 64)
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

-- Retained reservations can exceed the active window after native ACK.
CREATE OR REPLACE FUNCTION control.lock_ingest_lineage(wanted_resource bytea,wanted_epoch numeric)
RETURNS control.owner_epochs LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE legacy control.initial_ingest_successions%ROWTYPE; s control.ingest_successions%ROWTYPE;
        c control.owner_epochs%ROWTYPE; o control.owner_epochs%ROWTYPE; p control.producer_epochs%ROWTYPE;
        stream control.source_streams%ROWTYPE; role_row control.role_instances%ROWTYPE;
        old_p control.producer_epochs%ROWTYPE; old_stream control.source_streams%ROWTYPE;
        original record; m record; first_manifest record; previous_manifest record;
        a control.ingest_activations%ROWTYPE; old_a control.initial_ingest_activations%ROWTYPE;
        manifest bytea; previous bytea; incoming bytea; expected bytea; digest bytea;
        activated boolean; e integer; k integer; offset_generation numeric; empty_prefix integer; created bigint;
        expected_role bytea; prepared_resource bytea; prepared_epoch numeric; prepared_incarnation bytea;
        request_limit numeric; request_count bigint; maximum numeric;
BEGIN
    IF current_setting('transaction_isolation')<>'read committed' THEN
        RAISE EXCEPTION 'lineage requires READ COMMITTED' USING ERRCODE='25001';
    END IF;
    IF wanted_epoch NOT BETWEEN 2 AND 255 OR trunc(wanted_epoch)<>wanted_epoch
        OR wanted_epoch*(wanted_epoch+1)/2>32768 THEN
        RAISE EXCEPTION 'lineage metadata or membership profile exhausted' USING ERRCODE='23514';
    END IF;
    SELECT * INTO STRICT legacy FROM control.initial_ingest_successions WHERE owner_resource_id=wanted_resource;
    SELECT * INTO STRICT c FROM control.owner_epochs
        WHERE dataset_id=legacy.dataset_id AND role='control' AND state='active' FOR UPDATE;
    PERFORM control.require_control_owner(c.dataset_id,c.resource_id,c.epoch,c.process_incarnation);
    PERFORM 1 FROM control.datasets WHERE dataset_id=legacy.dataset_id FOR UPDATE;
    IF wanted_epoch=2 THEN
        -- The same flat checker can inspect terminal legacy genesis before any
        -- new preparation. These are coordinates from its retained0030 anchor,
        -- not a fabricated new receipt or a reconstructed writer capability.
        s.dataset_id:=legacy.dataset_id; s.producer_slot_id:=legacy.producer_slot_id;
        s.shard_id:=legacy.shard_id; s.config_revision:=legacy.config_revision; s.policy_revision:=legacy.policy_revision;
    ELSE
        SELECT * INTO STRICT s FROM control.ingest_successions WHERE owner_resource_id=wanted_resource AND target_epoch=wanted_epoch;
    END IF;
    SELECT * INTO STRICT first_manifest FROM control.ingest_lineage_manifest(legacy.previous_manifest);
    offset_generation:=first_manifest.topology;
    empty_prefix:=CASE WHEN offset_generation=0 THEN 0 ELSE 1 END;
    IF wanted_epoch-1+offset_generation>18446744073709551615
        OR first_manifest.epoch<>1 OR first_manifest.dataset_id<>legacy.dataset_id
        OR first_manifest.resource<>wanted_resource OR first_manifest.shard<>legacy.shard_id
        OR legacy.previous_topology<>first_manifest.topology OR legacy.target_topology<>first_manifest.topology+1
        OR s.dataset_id<>legacy.dataset_id OR s.producer_slot_id<>legacy.producer_slot_id OR s.shard_id<>legacy.shard_id
        OR NOT EXISTS(SELECT 1 FROM control.datasets d JOIN control.config_revisions cr
            ON cr.dataset_id=d.dataset_id AND cr.revision=d.config_revision
            JOIN control.policy_revisions pr ON pr.dataset_id=d.dataset_id AND pr.revision=d.policy_revision AND pr.config_revision=cr.revision
            WHERE d.dataset_id=s.dataset_id AND d.topology_generation=wanted_epoch-1+offset_generation
                AND d.config_revision>=s.config_revision AND d.policy_revision>=s.policy_revision) THEN
        RAISE EXCEPTION 'lineage identity or current policy/head differs' USING ERRCODE='23514';
    END IF;
    -- Sentinel queries bound unsupported profiles before ordered history work.
    IF (SELECT count(*) FROM(SELECT 1 FROM control.ingest_successions WHERE owner_resource_id=wanted_resource LIMIT 254)b)<>wanted_epoch-2
        OR (SELECT count(*) FROM(SELECT 1 FROM control.owner_epochs WHERE dataset_id=s.dataset_id AND role='ingest' LIMIT 256)b)<>wanted_epoch
        OR (SELECT count(*) FROM(SELECT 1 FROM control.producer_epochs WHERE dataset_id=s.dataset_id LIMIT 256)b)<>wanted_epoch
        OR (SELECT count(*) FROM(SELECT 1 FROM control.producer_slots WHERE dataset_id=s.dataset_id LIMIT 2)b)<>1
        OR (SELECT count(*) FROM(SELECT 1 FROM control.source_streams WHERE dataset_id=s.dataset_id LIMIT 256)b)<>wanted_epoch
        OR (SELECT count(*) FROM(SELECT 1 FROM control.allocation_domains WHERE dataset_id=s.dataset_id LIMIT 2296)b)<>wanted_epoch*9
        OR (SELECT count(DISTINCT allocation_domain) FROM control.allocation_domains WHERE dataset_id=s.dataset_id)<>wanted_epoch*9
        OR (SELECT count(*) FROM(SELECT 1 FROM control.topology_generations WHERE dataset_id=s.dataset_id LIMIT 257)b)<>wanted_epoch+empty_prefix
        OR (SELECT count(*) FROM(SELECT 1 FROM control.topology_streams WHERE dataset_id=s.dataset_id LIMIT 32769)b)<>wanted_epoch*(wanted_epoch+1)/2
        OR (SELECT count(DISTINCT process_incarnation) FROM control.owner_epochs
            WHERE dataset_id=s.dataset_id AND role='ingest')<>wanted_epoch
        OR (SELECT count(DISTINCT role_instance_id) FROM control.owner_epochs
            WHERE dataset_id=s.dataset_id AND role='ingest')<>wanted_epoch THEN
        RAISE EXCEPTION 'lineage has missing, foreign or excessive retained parents' USING ERRCODE='23514';
    END IF;
    -- The optional empty predecessor may have any u64 generation. Its presence,
    -- not its numerical value, consumes one inspected generation and no members.
    IF empty_prefix=1 AND (NOT EXISTS(SELECT 1 FROM control.topology_generations
            WHERE dataset_id=s.dataset_id AND generation=offset_generation-1 AND previous_generation IS NULL AND sealed)
        OR EXISTS(SELECT 1 FROM control.topology_streams WHERE dataset_id=s.dataset_id AND topology_generation=offset_generation-1)) THEN
        RAISE EXCEPTION 'lineage empty genesis prefix differs' USING ERRCODE='23514';
    END IF;
    -- One flat walk. It never calls a historical head validator recursively.
    FOR e IN 1..wanted_epoch::integer LOOP
        manifest:=control.ingest_lineage_manifest_at(wanted_resource,e);
        SELECT * INTO STRICT m FROM control.ingest_lineage_manifest(manifest);
        SELECT * INTO STRICT o FROM control.owner_epochs WHERE resource_id=wanted_resource AND epoch=e FOR UPDATE;
        SELECT * INTO STRICT role_row FROM control.role_instances WHERE role_instance_id=o.role_instance_id FOR UPDATE;
        PERFORM 1 FROM control.allocation_domains WHERE dataset_id=s.dataset_id AND owner_resource_id=wanted_resource AND owner_epoch=e
            ORDER BY dictionary_kind,allocation_domain LIMIT 10 FOR UPDATE;
        SELECT * INTO STRICT stream FROM control.source_streams WHERE dataset_id=s.dataset_id AND shard_id=s.shard_id AND owner_epoch=e FOR UPDATE;
        SELECT * INTO STRICT p FROM control.producer_epochs WHERE producer_slot_id=s.producer_slot_id AND producer_epoch=e FOR UPDATE;
        IF ROW(m.dataset_id,m.resource,m.shard,m.epoch,m.topology,m.recovery)
            IS DISTINCT FROM ROW(s.dataset_id::bytea,wanted_resource,s.shard_id::numeric,e::numeric,(e-1+offset_generation)::numeric,first_manifest.recovery)
            OR ROW(o.dataset_id,o.role,o.process_incarnation,role_row.dataset_id,role_row.role)
                IS DISTINCT FROM ROW(s.dataset_id,'ingest'::text,m.incarnation,s.dataset_id,'ingest'::text)
            OR ROW(p.dataset_id,p.producer_role_instance_id,p.producer_role,p.producer_incarnation,p.owner_resource_id,p.owner_epoch,p.owner_role,p.owner_incarnation)
                IS DISTINCT FROM ROW(s.dataset_id,o.role_instance_id,'ingest'::text,m.incarnation,wanted_resource,e::numeric,'ingest'::text,m.incarnation)
            OR ROW(stream.owner_resource_id,stream.owner_role,stream.owner_incarnation)
                IS DISTINCT FROM ROW(wanted_resource,'ingest'::text,m.incarnation)
            OR p.reserved_through_request>64 OR p.outstanding_request_limit>64
            OR (SELECT count(*) FROM control.allocation_domains WHERE dataset_id=s.dataset_id AND owner_resource_id=wanted_resource AND owner_epoch=e)<>9 THEN
            RAISE EXCEPTION 'lineage registry/manifest identity differs' USING ERRCODE='23514';
        END IF;
        IF e>=3 AND EXISTS(SELECT 1 FROM control.owner_epochs WHERE dataset_id=s.dataset_id
            AND process_incarnation=m.incarnation AND (resource_id<>wanted_resource OR epoch<>e)) THEN
            RAISE EXCEPTION 'lineage target incarnation was already registered' USING ERRCODE='23514';
        END IF;
        FOR k IN 1..9 LOOP
            IF NOT EXISTS(SELECT 1 FROM control.allocation_domains WHERE dataset_id=s.dataset_id AND dictionary_kind=k
                AND allocation_domain=substring(manifest FROM 107+(k-1)*18 FOR 16)
                AND owner_resource_id=wanted_resource AND owner_epoch=e AND owner_role='ingest' AND owner_incarnation=m.incarnation) THEN
                RAISE EXCEPTION 'lineage manifest allocation namespace differs' USING ERRCODE='23514';
            END IF;
        END LOOP;
        IF NOT EXISTS(SELECT 1 FROM control.policy_revisions WHERE dataset_id=s.dataset_id AND revision=m.policy) THEN
            RAISE EXCEPTION 'lineage manifest policy is absent' USING ERRCODE='23514';
        END IF;
        IF e=1 THEN
            created:=o.acquired_at_us; expected_role:=o.role_instance_id; request_limit:=p.outstanding_request_limit;
        ELSE
            incoming:=control.ingest_lineage_handoff_at(wanted_resource,e);
            IF e=2 THEN
                created:=legacy.created_at_us; expected_role:=legacy.target_role_instance_id; request_limit:=legacy.outstanding_request_limit;
                prepared_resource:=legacy.prepared_by_resource; prepared_epoch:=legacy.prepared_by_epoch; prepared_incarnation:=legacy.prepared_by_incarnation;
                expected:=decode('010a','hex')||s.producer_slot_id||control.ingest_lineage_u64(1)
                    ||control.ingest_lineage_u64(legacy.issued_through)||control.ingest_lineage_u64(legacy.last_complete_sequence)
                    ||legacy.closure_digest||decode('010a','hex')||legacy.previous_manifest||decode('010a','hex')||legacy.target_manifest;
                IF incoming<>expected OR legacy.previous_manifest<>previous OR legacy.target_manifest<>manifest
                    OR legacy.issued_through<>old_p.reserved_through_request OR legacy.last_complete_sequence<>old_stream.last_complete_sequence
                    OR legacy.closure_digest<>old_stream.closure_digest OR legacy.target_incarnation<>m.incarnation
                    OR sha256(incoming)<>legacy.handoff_sha256 OR sha256(previous)<>legacy.previous_manifest_sha256
                    OR sha256(manifest)<>legacy.target_manifest_sha256 OR legacy.policy_revision<>m.policy THEN
                    RAISE EXCEPTION 'lineage legacy anchor differs' USING ERRCODE='23514';
                END IF;
            ELSE
                SELECT * INTO STRICT s FROM control.ingest_successions WHERE owner_resource_id=wanted_resource AND target_epoch=e;
                created:=s.created_at_us; expected_role:=s.target_role_instance_id; request_limit:=s.outstanding_request_limit;
                prepared_resource:=s.prepared_by_resource; prepared_epoch:=s.prepared_by_epoch; prepared_incarnation:=s.prepared_by_incarnation;
                expected:=decode('020a','hex')||decode(lpad(to_hex(s.terminal_kind::integer),2,'0'),'hex')||s.producer_slot_id
                    ||control.ingest_lineage_u64(e-1)||control.ingest_lineage_u64(s.issued_through)
                    ||control.ingest_lineage_u64(s.last_complete_sequence)||s.closure_digest||s.parent_handoff_sha256
                    ||decode('010a','hex')||s.previous_manifest||decode('010a','hex')||s.target_manifest;
                IF incoming<>expected OR s.previous_manifest<>previous OR s.target_manifest<>manifest
                    OR s.dataset_id<>legacy.dataset_id OR s.producer_slot_id<>legacy.producer_slot_id OR s.shard_id<>legacy.shard_id
                    OR s.previous_epoch<>e-1 OR s.previous_topology<>e-2+offset_generation OR s.target_topology<>m.topology
                    OR s.target_incarnation<>m.incarnation OR s.policy_revision<>m.policy
                    OR s.parent_handoff_sha256<>sha256(control.ingest_lineage_handoff_at(wanted_resource,e-1))
                    OR s.issued_through<>old_p.reserved_through_request OR s.last_complete_sequence<>old_stream.last_complete_sequence
                    OR s.closure_digest<>old_stream.closure_digest
                    OR s.terminal_kind<>(CASE WHEN EXISTS(SELECT 1 FROM control.ingest_lineage_activation(wanted_resource,e-1)) THEN 1 ELSE 2 END) THEN
                    RAISE EXCEPTION 'lineage successor receipt or parent hash differs' USING ERRCODE='23514';
                END IF;
            END IF;
            IF created<old_p.retired_at_us THEN
                RAISE EXCEPTION 'lineage prepared identities or creation times differ' USING ERRCODE='23514';
            END IF;
            IF NOT EXISTS(SELECT 1 FROM control.config_revisions cr JOIN control.policy_revisions pr
                    ON pr.dataset_id=cr.dataset_id AND pr.config_revision=cr.revision
                    WHERE cr.dataset_id=s.dataset_id AND cr.revision=CASE WHEN e=2 THEN legacy.config_revision ELSE s.config_revision END
                        AND cr.document_digest=CASE WHEN e=2 THEN legacy.config_digest ELSE s.config_digest END
                        AND pr.revision=m.policy AND pr.document_digest=CASE WHEN e=2 THEN legacy.policy_digest ELSE s.policy_digest END) THEN
                RAISE EXCEPTION 'lineage immutable preparation policy differs' USING ERRCODE='23514';
            END IF;
            IF prepared_resource<>c.resource_id OR prepared_epoch>c.epoch
                OR NOT EXISTS(SELECT 1 FROM control.owner_epochs old JOIN control.role_instances r USING(role_instance_id)
                    WHERE old.dataset_id=s.dataset_id AND old.resource_id=prepared_resource AND old.epoch=prepared_epoch
                        AND old.process_incarnation=prepared_incarnation AND old.role='control' AND old.control_fence_version=1
                        AND r.dataset_id=s.dataset_id AND r.role='control'
                        AND ((old.epoch=c.epoch AND old.state='active' AND old.process_incarnation=c.process_incarnation AND r.retired_at_us IS NULL)
                            OR (old.epoch<c.epoch AND old.state='released' AND old.exclusion_method='storage_fenced'
                                AND old.exclusion_digest IS NOT NULL AND r.retired_at_us IS NOT NULL))) THEN
                RAISE EXCEPTION 'lineage historical preparing Control differs' USING ERRCODE='23514';
            END IF;
        END IF;
        IF o.role_instance_id<>expected_role OR o.acquired_at_us<>created
            OR role_row.created_at_us<>created OR p.opened_at_us<>created OR stream.opened_at_us<>created
            OR p.outstanding_request_limit<>request_limit
            OR EXISTS(SELECT 1 FROM control.allocation_domains WHERE dataset_id=s.dataset_id AND owner_resource_id=wanted_resource
                AND owner_epoch=e AND created_at_us<>created)
            OR (e=1 AND NOT EXISTS(SELECT 1 FROM control.producer_slots slot
                WHERE slot.producer_slot_id=s.producer_slot_id AND slot.dataset_id=s.dataset_id AND slot.created_at_us=created
                    AND slot.registered_by_role='control' AND slot.registered_by_resource_id=c.resource_id)) THEN
            RAISE EXCEPTION 'lineage prepared identities or creation times differ' USING ERRCODE='23514';
        END IF;
        SELECT * INTO original FROM control.ingest_lineage_activation(wanted_resource,e);
        activated:=FOUND;
        IF NOT activated AND o.lease_expires_at_us IS DISTINCT FROM
            (CASE WHEN e=2 THEN legacy.lease_expires_at_us ELSE s.lease_expires_at_us END) THEN
            RAISE EXCEPTION 'empty lineage scheduling metadata differs from preparation' USING ERRCODE='23514';
        END IF;
        IF e=2 AND activated THEN
            SELECT * INTO STRICT old_a FROM control.initial_ingest_activations WHERE owner_resource_id=wanted_resource;
            IF ROW(old_a.dataset_id,old_a.target_epoch,old_a.target_incarnation,old_a.target_role_instance_id,
                    old_a.producer_slot_id,old_a.producer_epoch,old_a.previous_manifest_sha256,old_a.target_manifest_sha256,old_a.handoff_sha256,
                    old_a.prepared_by_resource,old_a.prepared_by_epoch,old_a.prepared_by_incarnation,
                    old_a.activated_by_resource,old_a.activated_by_epoch,old_a.activated_by_incarnation,
                    old_a.config_revision,old_a.policy_revision,old_a.config_digest,old_a.policy_digest)
                IS DISTINCT FROM ROW(legacy.dataset_id,legacy.target_epoch,legacy.target_incarnation,legacy.target_role_instance_id,
                    legacy.producer_slot_id,legacy.producer_epoch,legacy.previous_manifest_sha256,legacy.target_manifest_sha256,legacy.handoff_sha256,
                    legacy.prepared_by_resource,legacy.prepared_by_epoch,legacy.prepared_by_incarnation,
                    legacy.prepared_by_resource,legacy.prepared_by_epoch,legacy.prepared_by_incarnation,
                    legacy.config_revision,legacy.policy_revision,legacy.config_digest,legacy.policy_digest)
                OR old_a.activated_at_us<created OR old_a.activation_transaction=pg_current_xact_id() THEN
                RAISE EXCEPTION 'lineage legacy activation differs' USING ERRCODE='23514';
            END IF;
        ELSIF e>=3 AND activated THEN
            SELECT * INTO STRICT a FROM control.ingest_activations WHERE owner_resource_id=wanted_resource AND target_epoch=e;
            IF ROW(a.dataset_id,a.target_incarnation,a.target_role_instance_id,a.producer_slot_id,a.producer_epoch,
                    a.target_manifest_sha256,a.handoff_sha256,a.activated_by_resource,a.activated_by_epoch,a.activated_by_incarnation)
                IS DISTINCT FROM ROW(s.dataset_id,s.target_incarnation,s.target_role_instance_id,s.producer_slot_id,s.producer_epoch,
                    s.target_manifest_sha256,s.handoff_sha256,s.prepared_by_resource,s.prepared_by_epoch,s.prepared_by_incarnation)
                OR a.activated_at_us<created OR (e<wanted_epoch AND a.activation_transaction=pg_current_xact_id()) THEN
                RAISE EXCEPTION 'lineage activation differs' USING ERRCODE='23514';
            END IF;
        END IF;
        SELECT count(*),coalesce(max(request_sequence),0) INTO request_count,maximum FROM
            (SELECT request_sequence FROM control.producer_requests WHERE producer_slot_id=p.producer_slot_id AND producer_epoch=e LIMIT 65)b;
        IF request_count<>p.reserved_through_request OR maximum<>p.reserved_through_request
            OR (NOT activated AND request_count<>0)
            OR EXISTS(SELECT 1 FROM control.producer_requests r WHERE r.producer_slot_id=p.producer_slot_id AND r.producer_epoch=e
                AND (r.dataset_id<>p.dataset_id OR r.manifest_sha256<>sha256(manifest)
                    OR ROW(r.issuer_resource,r.issuer_epoch,r.issuer_incarnation)
                        IS DISTINCT FROM ROW(original.resource,original.epoch,original.incarnation))) THEN
            RAISE EXCEPTION 'lineage reservation prefix or original issuer differs' USING ERRCODE='23514';
        END IF;
        IF NOT activated AND (EXISTS(SELECT 1 FROM control.producer_outcomes WHERE producer_slot_id=p.producer_slot_id AND producer_epoch=e)
            OR EXISTS(SELECT 1 FROM control.producer_input_bindings WHERE producer_slot_id=p.producer_slot_id AND producer_epoch=e)
            OR EXISTS(SELECT 1 FROM control.producer_checkpoints WHERE producer_slot_id=p.producer_slot_id AND producer_epoch=e)
            OR EXISTS(SELECT 1 FROM control.acquisition_gaps WHERE producer_slot_id=p.producer_slot_id AND producer_epoch=e)) THEN
            RAISE EXCEPTION 'unactivated lineage has target detail' USING ERRCODE='23514';
        END IF;
        IF stream.state='closed' THEN
            digest:=control.ingest_lineage_closure_digest(wanted_resource,e,NOT activated);
            IF stream.closure_digest<>digest OR (p.reserved_through_request=0 AND stream.last_complete_sequence<>0) THEN
                RAISE EXCEPTION 'lineage closed-source commitment differs' USING ERRCODE='23514';
            END IF;
        END IF;
        IF e<wanted_epoch OR wanted_epoch=2 THEN
            IF p.state<>'retired' OR stream.state<>'closed' OR o.state<>'released'
                OR o.exclusion_method<>'storage_fenced' OR o.exclusion_digest IS DISTINCT FROM digest
                OR p.closure_digest IS DISTINCT FROM digest OR p.retired_at_us<stream.closed_at_us
                OR role_row.retired_at_us IS DISTINCT FROM p.retired_at_us OR o.released_at_us IS DISTINCT FROM p.retired_at_us
                OR EXISTS(SELECT 1 FROM control.allocation_domains WHERE dataset_id=s.dataset_id AND owner_resource_id=wanted_resource
                    AND owner_epoch=e AND (state<>'closed' OR closure_digest IS DISTINCT FROM digest OR closed_at_us IS DISTINCT FROM p.retired_at_us))
                OR (NOT activated AND (stream.closed_at_us IS DISTINCT FROM p.retired_at_us OR stream.last_complete_sequence<>0))
                OR (e>=3 AND NOT EXISTS(SELECT 1 FROM control.ingest_terminal_transactions WHERE owner_resource_id=wanted_resource AND target_epoch=e)) THEN
                RAISE EXCEPTION 'lineage ancestor is not exactly terminal' USING ERRCODE='23514';
            END IF;
            IF activated THEN
                PERFORM control.require_ingest_lineage_workflows(wanted_resource,e,c.epoch);
                IF NOT EXISTS(SELECT 1 FROM control.owner_epochs old JOIN control.role_instances r USING(role_instance_id)
                    WHERE old.dataset_id=s.dataset_id AND old.resource_id=original.resource AND old.epoch=original.epoch
                        AND old.process_incarnation=original.incarnation AND old.role='control' AND old.control_fence_version=1
                        AND old.state='released' AND old.exclusion_method='storage_fenced' AND old.exclusion_digest IS NOT NULL
                        AND r.dataset_id=s.dataset_id AND r.role='control' AND r.retired_at_us IS NOT NULL AND old.epoch<c.epoch) THEN
                    RAISE EXCEPTION 'lineage original Control is not positively excluded' USING ERRCODE='23514';
                END IF;
            END IF;
        END IF;
        IF NOT EXISTS(SELECT 1 FROM control.topology_generations WHERE dataset_id=s.dataset_id AND generation=m.topology AND sealed
            AND previous_generation IS NOT DISTINCT FROM CASE WHEN m.topology=0 THEN NULL::numeric ELSE m.topology-1 END
            AND created_at_us=created)
            OR (SELECT count(*) FROM control.topology_streams WHERE dataset_id=s.dataset_id AND topology_generation=m.topology)<>e
            OR EXISTS(SELECT 1 FROM control.topology_streams member LEFT JOIN control.source_streams source
                ON source.dataset_id=member.dataset_id AND source.shard_id=member.shard_id AND source.owner_epoch=member.owner_epoch
                WHERE member.dataset_id=s.dataset_id AND member.topology_generation=m.topology
                    AND (member.shard_id<>s.shard_id OR member.owner_epoch>e OR source.owner_resource_id IS DISTINCT FROM wanted_resource
                        OR member.handoff_last_complete_sequence IS DISTINCT FROM
                            CASE WHEN member.owner_epoch=e THEN NULL::numeric ELSE source.last_complete_sequence END)) THEN
            RAISE EXCEPTION 'lineage sealed topology membership differs' USING ERRCODE='23514';
        END IF;
        previous:=manifest; old_p:=p; old_stream:=stream;
    END LOOP;
    PERFORM control.require_control_owner(c.dataset_id,c.resource_id,c.epoch,c.process_incarnation);
    IF c.lease_expires_at_us<=(extract(epoch FROM clock_timestamp())*1000000)::bigint THEN
        RAISE EXCEPTION 'lineage current Control authority expired' USING ERRCODE='23514';
    END IF;
    RETURN c;
END $$;

-- CREATE OR REPLACE preserves existing ownership and grants; no new privilege
-- is introduced by this protocol.
REVOKE ALL ON FUNCTION control.guard_producer_request(),
    control.require_activated_initial_recovery(bytea),
    control.require_activated_initial_source(bytea),
    control.lock_activated_initial_retirement(bytea),
    control.lock_ingest_lineage(bytea,numeric) FROM PUBLIC;
