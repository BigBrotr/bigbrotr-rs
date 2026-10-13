-- Recover positive outcomes from an already activated epoch2. No source
-- closure, retirement, checkpoint, acknowledgement or writer adoption.
-- Native exclusion/barrier and each positive outcome remain caller proofs.
CREATE FUNCTION control.require_activated_initial_recovery(wanted_resource bytea)
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
                AND r.retired_at_us = o.released_at_us)
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
                AND reserved_through_request <= s.outstanding_request_limit)
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

CREATE OR REPLACE FUNCTION control.require_complete_initial_empty_retirement() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE resource bytea;
        s control.initial_ingest_successions%ROWTYPE;
BEGIN
    IF TG_TABLE_NAME = 'role_instances' THEN
        -- Epoch1 retirement keeps its existing grants and behavior. Do not ask
        -- an ordinary role retirement to read the later succession relation.
        SELECT resource_id INTO resource FROM control.owner_epochs
            WHERE role_instance_id = NEW.role_instance_id AND role = 'ingest' AND epoch = 2;
        IF NOT FOUND THEN RETURN NULL; END IF;
    ELSIF TG_TABLE_NAME = 'owner_epochs' THEN
        resource := NEW.resource_id;
    ELSE
        resource := NEW.owner_resource_id;
    END IF;
    -- Only the exact activated producer barrier dispatches to the recovery
    -- classifier. Source/domain/owner/role changes and retirement still pass
    -- through the unactivated empty protocol and therefore refuse activation.
    IF TG_TABLE_NAME = 'producer_epochs' THEN
        IF OLD.producer_epoch = 2 AND OLD.owner_epoch = 2 AND OLD.state = 'open'
            AND NEW.producer_epoch = 2 AND NEW.owner_epoch = 2 AND NEW.state = 'closing'
            AND EXISTS (SELECT 1 FROM control.initial_ingest_activations WHERE owner_resource_id = resource) THEN
            PERFORM control.require_activated_initial_recovery(resource);
            IF NOT EXISTS (SELECT 1 FROM control.producer_epochs WHERE producer_slot_id = NEW.producer_slot_id
                AND producer_epoch = 2 AND state = 'closing') THEN
                RAISE EXCEPTION 'activated recovery requires its retained closing barrier' USING ERRCODE = '23514';
            END IF;
            RETURN NULL;
        END IF;
    END IF;
    SELECT * INTO STRICT s FROM control.initial_ingest_successions WHERE owner_resource_id = resource;
    IF control.require_empty_initial_retirement(resource, control.initial_empty_retirement_digest(s)) IS NULL THEN
        RAISE EXCEPTION 'empty attempt lifecycle changes require complete terminal retirement'
            USING ERRCODE = '23514';
    END IF;
    -- Unlike later terminal observation, this is the actual changing transaction.
    -- A claim written after the producer transition in this transaction refuses.
    PERFORM control.require_initial_empty_claims(s);
    RETURN NULL;
END $$;

REVOKE ALL ON FUNCTION control.require_activated_initial_recovery(bytea) FROM PUBLIC;
