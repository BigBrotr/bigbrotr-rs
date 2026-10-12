-- Retire only the exact unactivated epoch2 attempt. Native exclusion and the
-- positive predecessor proof remain the caller's responsibility. No activation,
-- new successor, acknowledgement or reclamation is introduced here.
LOCK TABLE control.owner_epochs, control.role_instances, control.allocation_domains,
    control.source_streams, control.producer_epochs IN SHARE MODE;
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM control.owner_epochs WHERE role = 'ingest' AND epoch = 2 AND state <> 'active')
        OR EXISTS (SELECT 1 FROM control.producer_epochs WHERE producer_epoch = 2 AND state <> 'open')
        OR EXISTS (SELECT 1 FROM control.source_streams WHERE owner_epoch = 2 AND state <> 'open')
        OR EXISTS (SELECT 1 FROM control.allocation_domains WHERE owner_epoch = 2 AND state <> 'active')
        OR EXISTS (SELECT 1 FROM control.initial_ingest_successions s JOIN control.role_instances r
            ON r.role_instance_id = s.target_role_instance_id WHERE r.retired_at_us IS NOT NULL) THEN
        RAISE EXCEPTION 'pre-protocol empty retirement history requires explicit recovery; no adoption'
            USING ERRCODE = '55000';
    END IF;
END $$;

-- This checksum is an expectation over retained metadata, not native authority.
CREATE FUNCTION control.initial_empty_retirement_digest(s control.initial_ingest_successions)
RETURNS bytea LANGUAGE sql IMMUTABLE STRICT SET search_path = pg_catalog AS $$
    SELECT sha256(convert_to('bigbrotr/initial-empty-attempt-closure/1', 'UTF8') || decode('00', 'hex')
        || sha256(s.handoff) || sha256(s.target_manifest) || s.producer_slot_id
        || int8send(2::bigint) || int8send(0::bigint) || int8send(0::bigint))
$$;

-- Shared custody/identity checks deliberately do not accept a lifecycle shape.
-- The public classifier below accepts only completely prepared or terminal rows;
-- the producer trigger separately recognizes its transaction-internal staging.
CREATE FUNCTION control.lock_initial_empty_retirement(wanted_resource bytea, wanted_digest bytea)
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
    RETURN s;
END $$;

CREATE FUNCTION control.require_initial_empty_claims(s control.initial_ingest_successions)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE current_epoch numeric;
BEGIN
    -- Caller retains exclusive current Control custody. Existing partial indexes
    -- bound these probes to the preparing/subsequent Control claim window.
    SELECT epoch INTO STRICT current_epoch FROM control.owner_epochs
        WHERE dataset_id = s.dataset_id AND resource_id = s.prepared_by_resource
            AND role = 'control' AND state = 'active';
    IF EXISTS (SELECT 1 FROM control.jobs WHERE dataset_id = s.dataset_id AND kind = 'acquisition'
            AND owner_resource_id = s.prepared_by_resource
            AND owner_epoch BETWEEN s.prepared_by_epoch AND current_epoch)
        OR EXISTS (SELECT 1 FROM control.jobs WHERE dataset_id = s.dataset_id AND kind = 'import'
            AND owner_resource_id = s.prepared_by_resource
            AND owner_epoch BETWEEN s.prepared_by_epoch AND current_epoch) THEN
        RAISE EXCEPTION 'empty attempt Control workflow claims remain unresolved' USING ERRCODE = '23514';
    END IF;
END $$;

-- NULL = exactly PREPARED; a timestamp = exactly TERMINAL. No partial lifecycle
-- state is a recoverable SQL preparation, including a producer merely closing.
CREATE FUNCTION control.require_empty_initial_retirement(wanted_resource bytea, wanted_digest bytea)
RETURNS bigint LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE s control.initial_ingest_successions%ROWTYPE;
        target_owner control.owner_epochs%ROWTYPE;
        producer control.producer_epochs%ROWTYPE;
        source control.source_streams%ROWTYPE;
        role_retired bigint;
BEGIN
    s := control.lock_initial_empty_retirement(wanted_resource, wanted_digest);
    SELECT * INTO STRICT target_owner FROM control.owner_epochs WHERE resource_id = wanted_resource AND epoch = 2;
    SELECT * INTO STRICT producer FROM control.producer_epochs WHERE producer_slot_id = s.producer_slot_id AND producer_epoch = 2;
    SELECT * INTO STRICT source FROM control.source_streams WHERE dataset_id = s.dataset_id AND shard_id = s.shard_id AND owner_epoch = 2;
    SELECT retired_at_us INTO STRICT role_retired FROM control.role_instances WHERE role_instance_id = s.target_role_instance_id;
    IF target_owner.state = 'active' AND role_retired IS NULL
        AND producer.state = 'open' AND source.state = 'open'
        AND NOT EXISTS (SELECT 1 FROM control.allocation_domains
            WHERE dataset_id = s.dataset_id AND owner_resource_id = wanted_resource AND owner_epoch = 2 AND state <> 'active') THEN
        PERFORM control.require_initial_empty_claims(s);
        RETURN NULL;
    END IF;
    IF producer.state = 'retired' AND producer.retired_at_us >= s.created_at_us
        AND producer.closure_digest = wanted_digest
        AND target_owner.state = 'released' AND target_owner.exclusion_method = 'storage_fenced'
        AND target_owner.exclusion_digest = wanted_digest AND target_owner.released_at_us = producer.retired_at_us
        AND role_retired = producer.retired_at_us AND source.state = 'closed'
        AND source.last_complete_sequence = 0 AND source.closure_digest = wanted_digest
        AND source.closed_at_us = producer.retired_at_us
        AND NOT EXISTS (SELECT 1 FROM control.allocation_domains
            WHERE dataset_id = s.dataset_id AND owner_resource_id = wanted_resource AND owner_epoch = 2
                AND (state <> 'closed' OR closure_digest IS DISTINCT FROM wanted_digest
                    OR closed_at_us IS DISTINCT FROM producer.retired_at_us)) THEN
        -- Later unrelated Control claims do not rewrite the historical result.
        RETURN producer.retired_at_us;
    END IF;
    RAISE EXCEPTION 'empty retirement requires exactly prepared or complete terminal metadata'
        USING ERRCODE = '23514';
END $$;

-- BEFORE producer retirement sees its old closing row. This staging check is
-- deliberately separate from the durable classifier and never returns a plan.
CREATE FUNCTION control.require_initial_empty_transition(wanted_slot bytea, wanted_digest bytea, wanted_at bigint)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE s control.initial_ingest_successions%ROWTYPE;
        resource bytea;
BEGIN
    SELECT owner_resource_id INTO STRICT resource FROM control.initial_ingest_successions WHERE producer_slot_id = wanted_slot;
    s := control.lock_initial_empty_retirement(resource, wanted_digest);
    PERFORM control.require_initial_empty_claims(s);
    IF wanted_at IS NULL OR wanted_at < s.created_at_us
        OR wanted_at > (EXTRACT(EPOCH FROM clock_timestamp()) * 1000000)::bigint
        OR NOT EXISTS (SELECT 1 FROM control.owner_epochs o JOIN control.role_instances r
            ON r.role_instance_id = o.role_instance_id
            WHERE o.resource_id = resource AND o.epoch = 2 AND o.state = 'active' AND r.retired_at_us IS NULL)
        OR NOT EXISTS (SELECT 1 FROM control.producer_epochs
            WHERE producer_slot_id = wanted_slot AND producer_epoch = 2 AND state = 'closing')
        OR NOT EXISTS (SELECT 1 FROM control.source_streams
            WHERE dataset_id = s.dataset_id AND shard_id = s.shard_id AND owner_epoch = 2
                AND state = 'closed' AND last_complete_sequence = 0 AND closure_digest = wanted_digest
                AND closed_at_us = wanted_at)
        OR EXISTS (SELECT 1 FROM control.allocation_domains
            WHERE dataset_id = s.dataset_id AND owner_resource_id = resource AND owner_epoch = 2 AND state <> 'active') THEN
        RAISE EXCEPTION 'empty retirement requires its exact atomic transition staging'
            USING ERRCODE = '23514';
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
        -- Keep0029's complete initial behavior; only exact epoch2 takes the new
        -- empty-attempt staging guard. Unsupported epochs still refuse in0029.
        IF NEW.state = 'retired' THEN
            IF NEW.producer_epoch = 2 AND NEW.owner_epoch = 2 THEN
                PERFORM control.require_initial_empty_transition(
                    NEW.producer_slot_id, NEW.closure_digest, NEW.retired_at_us);
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

-- Every target lifecycle change must commit with the whole terminal result.
-- Re-read final rows: queued NEW records can describe earlier staging writes.
CREATE FUNCTION control.require_complete_initial_empty_retirement() RETURNS trigger
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
CREATE CONSTRAINT TRIGGER initial_empty_source_complete AFTER UPDATE ON control.source_streams
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
    WHEN (NEW.owner_epoch = 2 AND NEW.state IS DISTINCT FROM OLD.state)
    EXECUTE FUNCTION control.require_complete_initial_empty_retirement();
CREATE CONSTRAINT TRIGGER initial_empty_producer_complete AFTER UPDATE ON control.producer_epochs
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
    WHEN (NEW.producer_epoch = 2 AND NEW.owner_epoch = 2 AND NEW.state IS DISTINCT FROM OLD.state)
    EXECUTE FUNCTION control.require_complete_initial_empty_retirement();
CREATE CONSTRAINT TRIGGER initial_empty_domain_complete AFTER UPDATE ON control.allocation_domains
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
    WHEN (NEW.owner_epoch = 2 AND NEW.state IS DISTINCT FROM OLD.state)
    EXECUTE FUNCTION control.require_complete_initial_empty_retirement();
CREATE CONSTRAINT TRIGGER initial_empty_owner_complete AFTER UPDATE ON control.owner_epochs
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
    WHEN (NEW.role = 'ingest' AND NEW.epoch = 2 AND NEW.state IS DISTINCT FROM OLD.state)
    EXECUTE FUNCTION control.require_complete_initial_empty_retirement();
CREATE CONSTRAINT TRIGGER initial_empty_role_complete AFTER UPDATE ON control.role_instances
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
    WHEN (NEW.role = 'ingest' AND NEW.retired_at_us IS DISTINCT FROM OLD.retired_at_us)
    EXECUTE FUNCTION control.require_complete_initial_empty_retirement();

REVOKE ALL ON FUNCTION control.initial_empty_retirement_digest(control.initial_ingest_successions),
    control.lock_initial_empty_retirement(bytea, bytea),
    control.require_initial_empty_claims(control.initial_ingest_successions),
    control.require_empty_initial_retirement(bytea, bytea),
    control.require_initial_empty_transition(bytea, bytea, bigint),
    control.require_complete_initial_empty_retirement() FROM PUBLIC;
