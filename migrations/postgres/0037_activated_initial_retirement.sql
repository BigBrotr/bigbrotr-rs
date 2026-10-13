-- Existing-copy-only activated producer retirement. Source closure and workflow
-- abandonment are separate committed prerequisites; native state never changes.
-- The shared lock validator accepts staging identities, not a durable lifecycle.
CREATE FUNCTION control.lock_activated_initial_retirement(wanted_resource bytea)
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
    -- Retirement never repairs missing copies or accepts import obligations,
    -- including zero checkpoints and workflow claims with no producer request.
    IF EXISTS (SELECT 1 FROM control.producer_checkpoints
            WHERE producer_slot_id=s.producer_slot_id AND producer_epoch=2)
        OR EXISTS (SELECT 1 FROM control.producer_input_bindings
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

-- NULL is exactly ready; a timestamp is exactly completed. No intermediate
-- mixture is accepted by the typed loader, including after early constraints.
CREATE FUNCTION control.require_activated_initial_retirement(wanted_resource bytea)
RETURNS bigint LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE s control.initial_ingest_successions%ROWTYPE;
        producer control.producer_epochs%ROWTYPE;
        owner control.owner_epochs%ROWTYPE;
        stream control.source_streams%ROWTYPE;
        role_retired bigint;
BEGIN
    s := control.lock_activated_initial_retirement(wanted_resource);
    SELECT * INTO STRICT producer FROM control.producer_epochs WHERE producer_slot_id=s.producer_slot_id AND producer_epoch=2;
    SELECT * INTO STRICT owner FROM control.owner_epochs WHERE resource_id=wanted_resource AND epoch=2;
    SELECT * INTO STRICT stream FROM control.source_streams WHERE dataset_id=s.dataset_id AND shard_id=s.shard_id AND owner_epoch=2;
    SELECT retired_at_us INTO STRICT role_retired FROM control.role_instances WHERE role_instance_id=s.target_role_instance_id;
    IF producer.state='closing' AND owner.state IN ('active','draining') AND role_retired IS NULL
        AND NOT EXISTS (SELECT 1 FROM control.allocation_domains
            WHERE dataset_id=s.dataset_id AND owner_resource_id=wanted_resource AND owner_epoch=2 AND state<>'active') THEN
        RETURN NULL;
    END IF;
    IF producer.state='retired' AND producer.retired_at_us>=stream.closed_at_us
        AND producer.closure_digest=stream.closure_digest AND owner.state='released'
        AND owner.exclusion_method='storage_fenced' AND owner.exclusion_digest=stream.closure_digest
        AND owner.released_at_us=producer.retired_at_us AND role_retired=producer.retired_at_us
        AND NOT EXISTS (SELECT 1 FROM control.allocation_domains
            WHERE dataset_id=s.dataset_id AND owner_resource_id=wanted_resource AND owner_epoch=2
                AND (state<>'closed' OR closure_digest IS DISTINCT FROM stream.closure_digest
                    OR closed_at_us IS DISTINCT FROM producer.retired_at_us)) THEN
        RETURN producer.retired_at_us;
    END IF;
    RAISE EXCEPTION 'activated retirement requires exactly ready or complete terminal metadata' USING ERRCODE='23514';
END $$;

CREATE FUNCTION control.require_activated_initial_retirement_transition(wanted_slot bytea, wanted_digest bytea, wanted_at bigint)
RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE s control.initial_ingest_successions%ROWTYPE;
BEGIN
    SELECT * INTO STRICT s FROM control.initial_ingest_successions WHERE producer_slot_id=wanted_slot;
    -- BEFORE UPDATE sees the old closing producer and all other ready rows.
    IF control.require_activated_initial_retirement(s.owner_resource_id) IS NOT NULL
        OR wanted_at IS NULL OR wanted_at>(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint
        OR NOT EXISTS (SELECT 1 FROM control.source_streams WHERE dataset_id=s.dataset_id
            AND shard_id=s.shard_id AND owner_epoch=2 AND state='closed'
            AND closure_digest=wanted_digest AND closed_at_us<=wanted_at) THEN
        RAISE EXCEPTION 'activated retirement requires exact atomic transition staging' USING ERRCODE='23514';
    END IF;
END $$;

CREATE OR REPLACE FUNCTION control.guard_producer_epoch() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
DECLARE prior control.producer_epochs%ROWTYPE;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'producer-epoch history is retained' USING ERRCODE = '23514';
    END IF;
    IF TG_OP = 'INSERT' AND NEW.reserved_through_request <> 0 THEN
        RAISE EXCEPTION 'new producer epoch starts with zero reserved requests' USING ERRCODE = '23514';
    END IF;
    IF TG_OP = 'UPDATE' THEN
        IF NEW.reserved_through_request <> OLD.reserved_through_request THEN
            -- Only the AFTER INSERT reservation trigger has a new retained row.
            -- Compare the complete row except this one monotone counter.
            IF (to_jsonb(NEW) - 'reserved_through_request') IS DISTINCT FROM
                   (to_jsonb(OLD) - 'reserved_through_request')
                OR OLD.state <> 'open'
                OR NEW.reserved_through_request <> OLD.reserved_through_request + 1
                OR NOT EXISTS (SELECT 1 FROM control.producer_requests
                    WHERE producer_slot_id = NEW.producer_slot_id
                        AND producer_epoch = NEW.producer_epoch
                        AND request_sequence = NEW.reserved_through_request) THEN
                RAISE EXCEPTION 'reservation counter requires its exact new retained input row'
                    USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
        END IF;
        -- Preserve epoch1 and unactivated empty staging. Only a retained
        -- activation selects the separate epoch2 retirement protocol.
        IF NEW.state = 'retired' THEN
            IF NEW.producer_epoch = 2 AND NEW.owner_epoch = 2 THEN
                IF EXISTS (SELECT 1 FROM control.initial_ingest_activations WHERE producer_slot_id=NEW.producer_slot_id) THEN
                    PERFORM control.require_activated_initial_retirement_transition(
                        NEW.producer_slot_id, NEW.closure_digest, NEW.retired_at_us);
                ELSE
                    PERFORM control.require_initial_empty_transition(
                        NEW.producer_slot_id, NEW.closure_digest, NEW.retired_at_us);
                END IF;
            ELSE
                PERFORM control.require_initial_retirement(
                    NEW.producer_slot_id, NEW.producer_epoch, NEW.closure_digest);
            END IF;
        END IF;
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
    -- Only terminal lifecycle changes select the retirement classifier. Earlier
    -- native barrier/source publication keep their exact validators and grants.
    IF EXISTS (SELECT 1 FROM control.initial_ingest_activations WHERE owner_resource_id=resource) THEN
        IF (TG_TABLE_NAME='producer_epochs' AND to_jsonb(NEW)->>'state'='retired')
            OR (TG_TABLE_NAME='allocation_domains' AND to_jsonb(NEW)->>'state'='closed')
            OR (TG_TABLE_NAME='owner_epochs' AND to_jsonb(NEW)->>'state'='released')
            OR (TG_TABLE_NAME='role_instances' AND (to_jsonb(NEW)->>'retired_at_us') IS NOT NULL) THEN
            IF control.require_activated_initial_retirement(resource) IS NULL THEN
                RAISE EXCEPTION 'activated lifecycle changes require complete terminal retirement' USING ERRCODE='23514';
            END IF;
            RETURN NULL;
        END IF;
    END IF;
    -- Two enumerated transitions only. Preserve the old function/grants for
    -- ordinary active/open producer barriers; the source mode authorizes its
    -- extra draining or closed shapes through the distinct complete validator.
    IF TG_TABLE_NAME = 'producer_epochs' THEN
        IF OLD.producer_epoch = 2 AND OLD.owner_epoch = 2 AND OLD.state = 'open'
            AND NEW.producer_epoch = 2 AND NEW.owner_epoch = 2 AND NEW.state = 'closing'
            AND EXISTS (SELECT 1 FROM control.initial_ingest_activations WHERE owner_resource_id = resource) THEN
            IF EXISTS (SELECT 1 FROM control.owner_epochs o JOIN control.source_streams stream
                ON stream.dataset_id=o.dataset_id AND stream.owner_resource_id=o.resource_id AND stream.owner_epoch=o.epoch
                WHERE o.resource_id=resource AND o.epoch=2 AND o.state='active' AND stream.state='open') THEN
                PERFORM control.require_activated_initial_recovery(resource);
            ELSE
                PERFORM control.require_activated_initial_source(resource);
            END IF;
            IF NOT EXISTS (SELECT 1 FROM control.producer_epochs WHERE producer_slot_id = NEW.producer_slot_id
                AND producer_epoch = 2 AND state = 'closing') THEN
                RAISE EXCEPTION 'activated recovery requires its retained closing barrier' USING ERRCODE = '23514';
            END IF;
            RETURN NULL;
        END IF;
    ELSIF TG_TABLE_NAME = 'source_streams' THEN
        IF OLD.owner_epoch=2 AND NEW.owner_epoch=2
            AND ((OLD.state='open' AND NEW.state='closing') OR (OLD.state='closing' AND NEW.state='closed'))
            AND EXISTS (SELECT 1 FROM control.initial_ingest_activations WHERE owner_resource_id=resource) THEN
            PERFORM control.require_activated_initial_source(resource);
            IF NOT EXISTS (SELECT 1 FROM control.source_streams
                WHERE dataset_id=NEW.dataset_id AND shard_id=NEW.shard_id AND owner_epoch=2 AND state='closed') THEN
                RAISE EXCEPTION 'activated source transitions require complete source closure' USING ERRCODE='23514';
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

-- The old deferred role trigger watches only retired_at_us. Protect every
-- column even after SET CONSTRAINTS has already validated the terminal shape.
-- Ordinary Control/epoch1 paths do not read the activation/recovery relations.
CREATE FUNCTION control.guard_activated_retirement_role() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
    IF TG_OP='TRUNCATE' THEN
        IF EXISTS (SELECT 1 FROM control.initial_ingest_activations) THEN
            RAISE EXCEPTION 'activated target role history cannot be truncated' USING ERRCODE='23514';
        END IF;
        RETURN NULL;
    END IF;
    IF OLD.role<>'ingest' THEN
        IF TG_OP='DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM control.owner_epochs
        WHERE role_instance_id=OLD.role_instance_id AND role='ingest' AND epoch=2) THEN
        IF TG_OP='DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM control.initial_ingest_activations WHERE target_role_instance_id=OLD.role_instance_id) THEN
        IF TG_OP='DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;
    END IF;
    IF TG_OP='DELETE' THEN
        RAISE EXCEPTION 'activated target role history cannot be deleted' USING ERRCODE='23514';
    END IF;
    IF OLD.retired_at_us IS NOT NULL OR NEW.retired_at_us IS NULL
        OR (to_jsonb(NEW)-'retired_at_us') IS DISTINCT FROM (to_jsonb(OLD)-'retired_at_us') THEN
        RAISE EXCEPTION 'activated target role permits only exact retirement' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER activated_retirement_role_guard BEFORE UPDATE OR DELETE ON control.role_instances
    FOR EACH ROW EXECUTE FUNCTION control.guard_activated_retirement_role();
CREATE TRIGGER activated_retirement_role_truncate_guard BEFORE TRUNCATE ON control.role_instances
    FOR EACH STATEMENT EXECUTE FUNCTION control.guard_activated_retirement_role();

REVOKE ALL ON FUNCTION control.lock_activated_initial_retirement(bytea),
    control.require_activated_initial_retirement(bytea),
    control.require_activated_initial_retirement_transition(bytea,bytea,bigint),
    control.guard_activated_retirement_role() FROM PUBLIC;
