-- Initial retirement keeps all native/SQL receipts and workflow evidence.
-- No historical row is adopted, no ACK is synthesized and no detail is reclaimed.
-- The caller retains native exclusion and its independently resolved source.
-- Acquisition already has this owner lookup in0027. Keep the import refusal
-- independent of the number of unrelated retained export/analytics jobs.
CREATE INDEX import_jobs_owner ON control.jobs(owner_resource_id, owner_epoch, job_id)
    WHERE kind = 'import' AND owner_resource_id IS NOT NULL;

CREATE FUNCTION control.require_initial_retirement(
    wanted_slot bytea, wanted_epoch numeric, wanted_digest bytea
) RETURNS bigint LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE producer control.producer_epochs%ROWTYPE;
        slot control.producer_slots%ROWTYPE;
        owner control.owner_epochs%ROWTYPE;
        source control.source_streams%ROWTYPE;
        gap_count bigint;
BEGIN
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'initial retirement requires READ COMMITTED' USING ERRCODE = '25001';
    END IF;
    SELECT * INTO STRICT producer FROM control.producer_epochs
        WHERE producer_slot_id = wanted_slot AND producer_epoch = wanted_epoch;
    SELECT * INTO STRICT owner FROM control.owner_epochs
        WHERE dataset_id = producer.dataset_id AND role = 'control' AND state = 'active';
    PERFORM control.require_control_owner(owner.dataset_id, owner.resource_id,
        owner.epoch, owner.process_incarnation);
    -- The adapter takes these before producer UPDATE. The trigger rechecks under
    -- that custody; direct writers must use the same Control/parent lock order.
    PERFORM 1 FROM control.datasets WHERE dataset_id = producer.dataset_id FOR UPDATE;
    PERFORM 1 FROM control.owner_epochs
        WHERE resource_id = producer.owner_resource_id AND epoch = producer.owner_epoch FOR SHARE;
    PERFORM 1 FROM control.source_streams
        WHERE dataset_id = producer.dataset_id AND owner_resource_id = producer.owner_resource_id
            AND owner_epoch = producer.owner_epoch FOR SHARE;
    SELECT * INTO STRICT producer FROM control.producer_epochs
        WHERE producer_slot_id = wanted_slot AND producer_epoch = wanted_epoch FOR UPDATE;
    IF producer.producer_epoch <> 1 OR producer.owner_epoch <> 1
        OR producer.producer_role <> 'ingest' OR producer.state NOT IN ('closing', 'retired')
        OR producer.reserved_through_request > 64
        OR (SELECT count(*) FROM control.producer_epochs
            WHERE dataset_id = producer.dataset_id AND owner_resource_id = producer.owner_resource_id
                AND owner_epoch = producer.owner_epoch) <> 1
        OR EXISTS (SELECT 1 FROM control.producer_checkpoints
            WHERE producer_slot_id = wanted_slot AND producer_epoch = wanted_epoch)
        OR EXISTS (SELECT 1 FROM control.producer_input_bindings
            WHERE producer_slot_id = wanted_slot AND producer_epoch = wanted_epoch) THEN
        RAISE EXCEPTION 'unsupported initial producer retirement history' USING ERRCODE = '23514';
    END IF;
    SELECT * INTO STRICT slot FROM control.producer_slots WHERE producer_slot_id = wanted_slot;
    SELECT * INTO STRICT source FROM control.source_streams
        WHERE dataset_id = producer.dataset_id AND owner_resource_id = producer.owner_resource_id
            AND owner_epoch = producer.owner_epoch;
    IF source.owner_incarnation <> producer.owner_incarnation OR source.state <> 'closed'
        OR wanted_digest IS DISTINCT FROM source.closure_digest
        OR NOT EXISTS (SELECT 1 FROM control.owner_epochs
            WHERE resource_id = slot.registered_by_resource_id AND epoch = slot.registered_by_epoch
                AND process_incarnation = slot.registered_by_incarnation
                AND state = 'released' AND exclusion_method = 'storage_fenced') THEN
        RAISE EXCEPTION 'retirement requires the closed source and fenced original Control'
            USING ERRCODE = '23514';
    END IF;
    IF (SELECT count(*) FROM control.producer_requests
        WHERE producer_slot_id = wanted_slot AND producer_epoch = wanted_epoch)
            <> producer.reserved_through_request
        OR EXISTS (
            SELECT 1 FROM control.producer_requests r
            LEFT JOIN control.producer_outcomes o USING (producer_slot_id, producer_epoch, request_sequence)
            WHERE r.producer_slot_id = wanted_slot AND r.producer_epoch = wanted_epoch
                AND (o.request_sequence IS NULL OR r.request_sequence > producer.reserved_through_request
                    OR r.dataset_id <> producer.dataset_id OR o.dataset_id <> r.dataset_id
                    OR o.request_digest <> r.request_digest OR o.manifest_sha256 <> r.manifest_sha256
                    OR (r.issuer_resource, r.issuer_epoch, r.issuer_incarnation) IS DISTINCT FROM
                        (slot.registered_by_resource_id, slot.registered_by_epoch, slot.registered_by_incarnation)
                    OR (r.relay_endpoint_id IS NOT NULL AND r.acquisition_job_id IS NULL)
                    OR (r.acquisition_job_id IS NOT NULL AND NOT EXISTS (
                        SELECT 1 FROM control.acquisition_gaps g
                        WHERE g.job_id = r.acquisition_job_id AND g.producer_slot_id = wanted_slot
                            AND g.producer_epoch = wanted_epoch)))) THEN
        RAISE EXCEPTION 'retirement requires every original outcome and supported workflow'
            USING ERRCODE = '23514';
    END IF;
    -- Include zero-request claims. A missing request binding is not evidence
    -- that the original executor had no acquisition/import obligation.
    IF EXISTS (SELECT 1 FROM control.jobs
        WHERE dataset_id = producer.dataset_id AND kind = 'acquisition'
            AND (owner_resource_id, owner_epoch, owner_incarnation) =
                (slot.registered_by_resource_id, slot.registered_by_epoch, slot.registered_by_incarnation))
        OR EXISTS (SELECT 1 FROM control.jobs
        WHERE dataset_id = producer.dataset_id AND kind = 'import'
            AND (owner_resource_id, owner_epoch, owner_incarnation) =
                (slot.registered_by_resource_id, slot.registered_by_epoch, slot.registered_by_incarnation)) THEN
        RAISE EXCEPTION 'original producer workflow claims remain unresolved' USING ERRCODE = '23514';
    END IF;
    SELECT count(*) INTO gap_count FROM (
        SELECT 1 FROM control.acquisition_gaps
        WHERE producer_slot_id = wanted_slot AND producer_epoch = wanted_epoch LIMIT 65
    ) AS bounded;
    IF gap_count > 64 OR EXISTS (
        SELECT 1 FROM control.acquisition_gaps g JOIN control.jobs j USING (job_id)
        WHERE g.producer_slot_id = wanted_slot AND g.producer_epoch = wanted_epoch
            AND (g.dataset_id <> producer.dataset_id OR g.shard_id <> source.shard_id
                OR g.source_owner_epoch <> source.owner_epoch
                OR g.last_complete_sequence <> source.last_complete_sequence
                OR g.source_closure_digest <> source.closure_digest
                OR (g.previous_owner_resource, g.previous_owner_epoch, g.previous_owner_incarnation)
                    IS DISTINCT FROM (slot.registered_by_resource_id, slot.registered_by_epoch,
                        slot.registered_by_incarnation)
                OR j.dataset_id <> g.dataset_id OR j.acquisition_job_protocol IS DISTINCT FROM 1
                OR j.state <> 'cancelled' OR j.job_revision <> g.previous_job_revision + 2
                OR j.checkpoint_revision <> 0 OR j.result_manifest_digest IS NOT NULL
                OR j.reason_code IS DISTINCT FROM g.reason
                OR j.completed_at_us IS DISTINCT FROM g.abandoned_at_us
                OR (j.owner_resource_id, j.owner_epoch, j.owner_incarnation)
                    IS DISTINCT FROM (g.issuer_resource, g.issuer_epoch, g.issuer_incarnation))) THEN
        RAISE EXCEPTION 'retirement requires complete retained acquisition gaps' USING ERRCODE = '23514';
    END IF;
    IF owner.lease_expires_at_us <= (EXTRACT(EPOCH FROM clock_timestamp()) * 1000000)::bigint THEN
        RAISE EXCEPTION 'retirement Control authority expired' USING ERRCODE = '23514';
    END IF;
    RETURN gap_count;
END $$;
REVOKE ALL ON FUNCTION control.require_initial_retirement(bytea, numeric, bytea) FROM PUBLIC;

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
        -- Empty producers can still own zero-request workflow intents.
        IF NEW.state = 'retired' THEN
            PERFORM control.require_initial_retirement(
                NEW.producer_slot_id, NEW.producer_epoch, NEW.closure_digest);
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
