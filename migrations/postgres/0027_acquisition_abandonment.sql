-- Explicit whole-intent uncertainty for the finite initial local executor.
-- No old job is adopted at migration time, and no scientific row is removed.
CREATE TABLE control.acquisition_gaps (
    job_id control.id16 PRIMARY KEY REFERENCES control.acquisition_jobs(job_id),
    dataset_id control.id16 NOT NULL,
    producer_slot_id control.id16 NOT NULL,
    producer_epoch control.u64 NOT NULL CHECK (producer_epoch = 1),
    shard_id control.u32 NOT NULL,
    source_owner_epoch control.u64 NOT NULL CHECK (source_owner_epoch = 1),
    last_complete_sequence control.u64 NOT NULL,
    source_closure_digest control.digest32 NOT NULL,
    previous_job_revision control.u64 NOT NULL CHECK (previous_job_revision > 0),
    previous_owner_resource control.id16 NOT NULL,
    previous_owner_epoch control.u64 NOT NULL CHECK (previous_owner_epoch > 0),
    previous_owner_incarnation control.id16 NOT NULL,
    issuer_resource control.id16 NOT NULL,
    issuer_epoch control.u64 NOT NULL CHECK (issuer_epoch > 0),
    issuer_incarnation control.id16 NOT NULL,
    owner_role text NOT NULL DEFAULT 'control' CHECK (owner_role = 'control'),
    abandoned_at_us bigint NOT NULL,
    reason text NOT NULL CHECK (reason = 'initial_producer_abandoned'),
    FOREIGN KEY (producer_slot_id, producer_epoch, dataset_id)
        REFERENCES control.producer_epochs(producer_slot_id, producer_epoch, dataset_id),
    FOREIGN KEY (dataset_id, shard_id, source_owner_epoch)
        REFERENCES control.source_streams(dataset_id, shard_id, owner_epoch),
    FOREIGN KEY (dataset_id, previous_owner_resource, previous_owner_epoch, owner_role, previous_owner_incarnation)
        REFERENCES control.owner_epochs(dataset_id, resource_id, epoch, role, process_incarnation),
    FOREIGN KEY (dataset_id, issuer_resource, issuer_epoch, owner_role, issuer_incarnation)
        REFERENCES control.owner_epochs(dataset_id, resource_id, epoch, role, process_incarnation)
);
CREATE INDEX acquisition_gaps_producer ON control.acquisition_gaps(producer_slot_id, producer_epoch, job_id);
CREATE INDEX acquisition_jobs_owner ON control.jobs(owner_resource_id, owner_epoch, job_id)
    WHERE kind = 'acquisition';
CREATE TRIGGER acquisition_gaps_immutable BEFORE UPDATE OR DELETE ON control.acquisition_gaps
    FOR EACH ROW EXECUTE FUNCTION control.reject_history_mutation();
CREATE TRIGGER acquisition_gaps_no_truncate BEFORE TRUNCATE ON control.acquisition_gaps
    FOR EACH STATEMENT EXECUTE FUNCTION control.reject_history_mutation();

CREATE FUNCTION control.guard_acquisition_gap() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE job control.jobs%ROWTYPE;
BEGIN
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'acquisition abandonment requires READ COMMITTED' USING ERRCODE = '25001';
    END IF;
    PERFORM control.require_control_owner(NEW.dataset_id, NEW.issuer_resource,
        NEW.issuer_epoch, NEW.issuer_incarnation);
    -- The connected adapter takes this before registry/source/producer locks.
    PERFORM 1 FROM control.datasets WHERE dataset_id = NEW.dataset_id FOR UPDATE;
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
            AND (slot.registered_by_resource_id, slot.registered_by_epoch, slot.registered_by_incarnation)
                = (NEW.previous_owner_resource, NEW.previous_owner_epoch, NEW.previous_owner_incarnation)
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
CREATE TRIGGER acquisition_gap_guard BEFORE INSERT ON control.acquisition_gaps
    FOR EACH ROW EXECUTE FUNCTION control.guard_acquisition_gap();

-- Ordinary owner changes still refuse. This exception transfers only the
-- terminal write; the deferred gap check forbids committing adopted execution.
CREATE OR REPLACE FUNCTION control.guard_acquisition_claim_owner() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    IF (NEW.owner_resource_id, NEW.owner_epoch, NEW.owner_incarnation)
        IS DISTINCT FROM (OLD.owner_resource_id, OLD.owner_epoch, OLD.owner_incarnation) THEN
        IF OLD.state <> 'queued' OR NEW.state <> 'cancel_requested'
            OR OLD.checkpoint_revision <> 0 OR NEW.checkpoint_revision <> 0
            OR NEW.job_revision <> OLD.job_revision + 1
            OR NEW.reason_code IS DISTINCT FROM 'initial_producer_abandoned'
            OR NOT EXISTS (SELECT 1 FROM control.acquisition_gaps g
                WHERE g.job_id = OLD.job_id AND g.dataset_id = OLD.dataset_id
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

CREATE FUNCTION control.require_acquisition_gap_complete() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE gap control.acquisition_gaps%ROWTYPE; job control.jobs%ROWTYPE;
BEGIN
    SELECT * INTO gap FROM control.acquisition_gaps WHERE job_id = NEW.job_id;
    IF NOT FOUND THEN RETURN NULL; END IF;
    SELECT * INTO STRICT job FROM control.jobs WHERE job_id = gap.job_id;
    IF job.dataset_id <> gap.dataset_id OR job.acquisition_job_protocol IS DISTINCT FROM 1
        OR job.state <> 'cancelled' OR job.job_revision <> gap.previous_job_revision + 2
        OR job.checkpoint_revision <> 0 OR job.result_manifest_digest IS NOT NULL
        OR job.reason_code IS DISTINCT FROM gap.reason
        OR job.completed_at_us IS DISTINCT FROM gap.abandoned_at_us
        OR (job.owner_resource_id, job.owner_epoch, job.owner_incarnation)
            IS DISTINCT FROM (gap.issuer_resource, gap.issuer_epoch, gap.issuer_incarnation) THEN
        RAISE EXCEPTION 'whole-intent gap and terminal job must commit atomically' USING ERRCODE = '23514';
    END IF;
    PERFORM control.require_control_owner(gap.dataset_id, gap.issuer_resource,
        gap.issuer_epoch, gap.issuer_incarnation);
    IF NOT EXISTS (SELECT 1 FROM control.owner_epochs
        WHERE resource_id = gap.issuer_resource AND epoch = gap.issuer_epoch
            AND lease_expires_at_us > (EXTRACT(EPOCH FROM clock_timestamp()) * 1000000)::bigint) THEN
        RAISE EXCEPTION 'acquisition abandonment authority expired before commit' USING ERRCODE = '23514';
    END IF;
    RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER acquisition_gap_complete AFTER INSERT ON control.acquisition_gaps
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION control.require_acquisition_gap_complete();
CREATE CONSTRAINT TRIGGER acquisition_abandonment_complete AFTER UPDATE ON control.jobs
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.acquisition_job_protocol = 1)
    EXECUTE FUNCTION control.require_acquisition_gap_complete();

REVOKE ALL ON control.acquisition_gaps FROM PUBLIC;
REVOKE ALL ON FUNCTION control.guard_acquisition_gap(),
    control.require_acquisition_gap_complete() FROM PUBLIC;
