-- Activated initial acquisition records the same whole-intent uncertainty as
-- epoch1, attributed to its activating Control rather than the slot registrar.
-- Preserve applied migrations, all old gaps and the 0028 ordinary-job grants.
ALTER TABLE control.acquisition_gaps
    DROP CONSTRAINT acquisition_gaps_producer_epoch_check,
    DROP CONSTRAINT acquisition_gaps_source_owner_epoch_check,
    DROP CONSTRAINT acquisition_gaps_reason_check,
    ADD CONSTRAINT acquisition_gap_epoch_reason CHECK (
        (producer_epoch = 1 AND source_owner_epoch = 1 AND reason = 'initial_producer_abandoned')
        OR (producer_epoch = 2 AND source_owner_epoch = 2 AND reason = 'activated_initial_producer_abandoned')
    );

CREATE OR REPLACE FUNCTION control.guard_acquisition_gap() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE job control.jobs%ROWTYPE; activation control.initial_ingest_activations%ROWTYPE;
BEGIN
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'acquisition abandonment requires READ COMMITTED' USING ERRCODE = '25001';
    END IF;
    PERFORM control.require_control_owner(NEW.dataset_id, NEW.issuer_resource,
        NEW.issuer_epoch, NEW.issuer_incarnation);
    -- The connected adapter takes this before registry/source/producer locks.
    PERFORM 1 FROM control.datasets WHERE dataset_id = NEW.dataset_id FOR UPDATE;
    IF NEW.producer_epoch = 2 THEN
        SELECT * INTO STRICT activation FROM control.initial_ingest_activations
            WHERE producer_slot_id = NEW.producer_slot_id;
        PERFORM control.require_activated_initial_source(activation.owner_resource_id);
        IF (NEW.dataset_id, NEW.source_owner_epoch, NEW.reason,
            NEW.previous_owner_resource, NEW.previous_owner_epoch, NEW.previous_owner_incarnation)
            IS DISTINCT FROM (activation.dataset_id, 2::numeric, 'activated_initial_producer_abandoned'::text,
                activation.activated_by_resource, activation.activated_by_epoch, activation.activated_by_incarnation) THEN
            RAISE EXCEPTION 'activated acquisition gap requires its activating Control'
                USING ERRCODE = '23514';
        END IF;
    END IF;
    PERFORM 1 FROM control.owner_epochs
        WHERE dataset_id = NEW.dataset_id AND resource_id = NEW.previous_owner_resource
            AND epoch = NEW.previous_owner_epoch AND role = 'control'
            AND process_incarnation = NEW.previous_owner_incarnation
            AND state = 'released' AND exclusion_method = 'storage_fenced' FOR SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'old acquisition Control must be positively fenced' USING ERRCODE = '23514';
    END IF;
    PERFORM 1 FROM control.source_streams s
        JOIN control.producer_epochs p ON p.dataset_id = s.dataset_id
            AND p.owner_resource_id = s.owner_resource_id AND p.owner_epoch = s.owner_epoch
            AND p.owner_incarnation = s.owner_incarnation
        JOIN control.producer_slots slot ON slot.producer_slot_id = p.producer_slot_id
        WHERE s.dataset_id = NEW.dataset_id AND s.shard_id = NEW.shard_id
            AND s.owner_epoch = NEW.source_owner_epoch AND s.state = 'closed'
            AND s.last_complete_sequence = NEW.last_complete_sequence
            AND s.closure_digest = NEW.source_closure_digest
            AND s.closed_at_us <= NEW.abandoned_at_us
            AND p.producer_slot_id = NEW.producer_slot_id AND p.producer_epoch = NEW.producer_epoch
            AND p.state = 'closing' AND p.reserved_through_request <= 64
            AND (NEW.producer_epoch = 2 OR
                (slot.registered_by_resource_id, slot.registered_by_epoch, slot.registered_by_incarnation)
                    = (NEW.previous_owner_resource, NEW.previous_owner_epoch, NEW.previous_owner_incarnation))
            AND NOT EXISTS (
                SELECT 1 FROM control.producer_requests r
                LEFT JOIN control.producer_outcomes o USING (producer_slot_id, producer_epoch, request_sequence)
                WHERE r.producer_slot_id = p.producer_slot_id AND r.producer_epoch = p.producer_epoch
                    AND o.request_sequence IS NULL)
        FOR SHARE OF s, p;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'acquisition gap requires its closed resolved initial source' USING ERRCODE = '23514';
    END IF;
    SELECT * INTO STRICT job FROM control.jobs WHERE job_id = NEW.job_id FOR UPDATE;
    IF job.dataset_id <> NEW.dataset_id OR job.acquisition_job_protocol IS DISTINCT FROM 1
        OR job.state <> 'queued' OR job.checkpoint_revision <> 0 OR NOT job.input_complete
        OR job.job_revision <> NEW.previous_job_revision
        OR (job.owner_resource_id, job.owner_epoch, job.owner_incarnation)
            IS DISTINCT FROM (NEW.previous_owner_resource, NEW.previous_owner_epoch, NEW.previous_owner_incarnation)
        OR NEW.abandoned_at_us < job.created_at_us
        OR NEW.abandoned_at_us > (EXTRACT(EPOCH FROM clock_timestamp()) * 1000000)::bigint THEN
        RAISE EXCEPTION 'acquisition gap must describe its original queued claim' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END $$;
-- The existing trigger fires only for an already owned acquisition job. Its
-- unchanged-owner path reads no gap or activated-source relation. Do not restore
-- the redundant deferred jobs UPDATE trigger removed by migration 0028.
CREATE OR REPLACE FUNCTION control.guard_acquisition_claim_owner() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    IF (NEW.owner_resource_id, NEW.owner_epoch, NEW.owner_incarnation)
        IS DISTINCT FROM (OLD.owner_resource_id, OLD.owner_epoch, OLD.owner_incarnation) THEN
        IF OLD.state <> 'queued' OR NEW.state <> 'cancel_requested'
            OR OLD.checkpoint_revision <> 0 OR NEW.checkpoint_revision <> 0
            OR NEW.job_revision <> OLD.job_revision + 1
            OR NEW.reason_code NOT IN ('initial_producer_abandoned', 'activated_initial_producer_abandoned')
            OR NEW.reason_code IS NULL
            OR NOT EXISTS (SELECT 1 FROM control.acquisition_gaps g
                WHERE g.job_id = OLD.job_id AND g.dataset_id = OLD.dataset_id
                    AND g.reason = NEW.reason_code
                    AND ((g.producer_epoch = 1 AND g.source_owner_epoch = 1 AND g.reason = 'initial_producer_abandoned')
                        OR (g.producer_epoch = 2 AND g.source_owner_epoch = 2 AND g.reason = 'activated_initial_producer_abandoned'))
                    AND g.previous_job_revision = OLD.job_revision
                    AND (g.previous_owner_resource, g.previous_owner_epoch, g.previous_owner_incarnation)
                        = (OLD.owner_resource_id, OLD.owner_epoch, OLD.owner_incarnation)
                    AND (g.issuer_resource, g.issuer_epoch, g.issuer_incarnation)
                        = (NEW.owner_resource_id, NEW.owner_epoch, NEW.owner_incarnation)) THEN
            RAISE EXCEPTION 'acquisition owner replacement requires explicit custody recovery'
                USING ERRCODE = '23514';
        END IF;
    END IF;
    RETURN NEW;
END $$;

-- Existing function identities retain their explicit grants under OR REPLACE.
-- The epoch2 path additionally requires the existing source-recovery privileges;
-- no ordinary acceptance/claim acquires those privileges through a trigger.
