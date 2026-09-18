-- MON-009 forward repair. Earlier SQL and checksummed receipts stay immutable.
-- Prevent writers from racing the incompatible-history audit or new guards.
LOCK TABLE control.jobs, control.job_input_bounds, control.source_streams
    IN SHARE ROW EXCLUSIVE MODE;
DO $$
BEGIN
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'job-bound migration requires READ COMMITTED' USING ERRCODE = '25001';
    END IF;
    IF EXISTS (
        SELECT 1 FROM control.job_input_bounds AS bound
        JOIN control.source_streams AS source USING (dataset_id, shard_id, owner_epoch)
        WHERE source.state = 'closed' AND bound.last_complete_sequence > source.last_complete_sequence
    ) THEN
        RAISE EXCEPTION '0008 refuses job cuts beyond authoritative source closure; explicit reconciliation required'
            USING ERRCODE = '55000';
    END IF;
END;
$$;

-- Reverse lookup at closure must not scan every job's cut. This is an access
-- path over existing authority, not a second watermark or retained obligation.
CREATE INDEX job_bounds_by_source ON control.job_input_bounds
    (dataset_id, shard_id, owner_epoch, last_complete_sequence);

CREATE OR REPLACE FUNCTION control.require_complete_job_input() RETURNS trigger LANGUAGE plpgsql
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
    -- Locks out closure until this complete cut commits or rolls back. Immutable
    -- topology order gives every multi-stream job the same acquisition order.
    -- The subsequent query has a fresh READ COMMITTED snapshot after any wait.
    IF job.input_topology_generation IS NOT NULL THEN
        IF current_setting('transaction_isolation') <> 'read committed' THEN
            RAISE EXCEPTION 'source job cuts require READ COMMITTED'
                USING ERRCODE = '25001';
        END IF;
        PERFORM 1 FROM control.source_streams AS source
            JOIN control.job_input_bounds AS bound
                USING (dataset_id, shard_id, owner_epoch)
            WHERE bound.job_id = job.job_id
            ORDER BY source.dataset_id, source.shard_id, source.owner_epoch
            FOR SHARE OF source;
        IF EXISTS (
            SELECT 1 FROM control.job_input_bounds AS bound
            JOIN control.source_streams AS source USING (dataset_id, shard_id, owner_epoch)
            WHERE bound.job_id = job.job_id AND source.state = 'closed'
                AND bound.last_complete_sequence > source.last_complete_sequence
        ) THEN
            RAISE EXCEPTION 'initial job cut exceeds authoritative source closure'
                USING ERRCODE = '23514';
        END IF;
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

CREATE OR REPLACE FUNCTION control.guard_source_closure_bounds() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
BEGIN
    -- UPDATE has locked the source row before this BEFORE trigger. Job seals
    -- hold SHARE on the same row through COMMIT; this volatile function's query
    -- must then see their newly committed bounds, not an older fixed snapshot.
    IF NEW.state = 'closed' AND current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'source closure requires READ COMMITTED'
            USING ERRCODE = '25001';
    END IF;
    IF NEW.state = 'closed' AND (
        EXISTS (SELECT 1 FROM control.consumer_frontiers
            WHERE dataset_id = NEW.dataset_id AND shard_id = NEW.shard_id
                AND owner_epoch = NEW.owner_epoch
                AND last_complete_sequence > NEW.last_complete_sequence)
        OR EXISTS (SELECT 1 FROM control.projection_chunk_bounds AS bound
            JOIN control.projection_chunks AS chunk
              ON chunk.generation_id = bound.generation_id AND chunk.chunk_id = bound.chunk_id
            WHERE bound.dataset_id = NEW.dataset_id AND bound.shard_id = NEW.shard_id
                AND bound.owner_epoch = NEW.owner_epoch AND chunk.state = 'committed'
                AND bound.last_complete_sequence > NEW.last_complete_sequence)
        OR EXISTS (SELECT 1 FROM control.job_input_bounds
            WHERE dataset_id = NEW.dataset_id AND shard_id = NEW.shard_id
                AND owner_epoch = NEW.owner_epoch
                AND last_complete_sequence > NEW.last_complete_sequence)) THEN
        RAISE EXCEPTION 'source closure is below an already recorded source cut'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
