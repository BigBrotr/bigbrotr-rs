-- Attribute new relay work to its finite job without adopting historical rows.
ALTER TABLE control.acquisition_jobs ADD CONSTRAINT acquisition_job_scope
    UNIQUE (dataset_id, job_id, scope_id);
ALTER TABLE control.producer_requests
    ADD COLUMN acquisition_job_id bytea,
    ADD CONSTRAINT producer_acquisition_job_identity CHECK (
        acquisition_job_id IS NULL OR (
            octet_length(acquisition_job_id) = 16
            AND acquisition_job_id <> decode(repeat('00', 16), 'hex')
            AND acquisition_scope_id IS NOT NULL AND relay_endpoint_id IS NOT NULL
        )
    ),
    ADD CONSTRAINT producer_acquisition_job_scope FOREIGN KEY
        (dataset_id, acquisition_job_id, acquisition_scope_id)
        REFERENCES control.acquisition_jobs(dataset_id, job_id, scope_id);

-- Call before source, Ingest and producer locks. The inherited helper takes
-- Control -> dataset -> account/grant -> enrollment -> job, and checks the
-- original deadline after waits. This adds exact claimed execution attribution.
CREATE FUNCTION control.require_acquisition_execution(
    wanted_dataset bytea, wanted_job bytea, wanted_scope bytea,
    wanted_endpoint bytea, wanted_access bytea
) RETURNS bigint LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE remaining bigint;
BEGIN
    PERFORM control.require_acquisition_job_authority(wanted_dataset, wanted_job);
    SELECT a.deadline_at_us - (EXTRACT(EPOCH FROM clock_timestamp()) * 1000000)::bigint
        INTO remaining
        FROM control.acquisition_jobs a
        JOIN control.jobs j ON j.job_id = a.job_id
        JOIN control.acquisition_scopes s
            ON s.dataset_id = a.dataset_id AND s.scope_id = a.scope_id
        WHERE a.dataset_id = wanted_dataset AND a.job_id = wanted_job
            AND a.scope_id = wanted_scope
            AND s.endpoint_id = wanted_endpoint AND s.access_scope_id = wanted_access
            AND j.state IN ('queued', 'running') AND j.owner_resource_id IS NOT NULL;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'acquisition execution requires the exact claimed job and source'
            USING ERRCODE = '23514';
    END IF;
    IF remaining <= 0 THEN
        RAISE EXCEPTION 'acquisition execution exhausted its original duration'
            USING ERRCODE = 'BB001';
    END IF;
    RETURN remaining;
END $$;

CREATE FUNCTION control.guard_producer_acquisition_job() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    -- Bootstrap/import retain their original protocol and lock order. Existing
    -- source rows with NULL jobs remain immutable historical recovery inputs.
    IF NEW.relay_endpoint_id IS NULL THEN RETURN NEW; END IF;
    IF NEW.acquisition_job_id IS NULL OR NEW.acquisition_scope_id IS NULL THEN
        RAISE EXCEPTION 'new relay reservation requires its finite acquisition job'
            USING ERRCODE = '23514';
    END IF;
    PERFORM control.require_control_owner(NEW.dataset_id, NEW.issuer_resource,
        NEW.issuer_epoch, NEW.issuer_incarnation);
    PERFORM control.require_acquisition_execution(NEW.dataset_id, NEW.acquisition_job_id,
        NEW.acquisition_scope_id, NEW.relay_endpoint_id, NEW.relay_access_scope_id);
    RETURN NEW;
END $$;
-- Alphabetically before producer_request_00_relay_source: actor/grant locks
-- cannot be acquired for the first time after the source/producer locks.
CREATE TRIGGER producer_request_00_acquisition_job BEFORE INSERT ON control.producer_requests
    FOR EACH ROW EXECUTE FUNCTION control.guard_producer_acquisition_job();
CREATE CONSTRAINT TRIGGER zzz_producer_acquisition_job_complete
    AFTER INSERT ON control.producer_requests DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW WHEN (NEW.acquisition_job_id IS NOT NULL)
    EXECUTE FUNCTION control.guard_producer_acquisition_job();

REVOKE ALL ON FUNCTION
    control.require_acquisition_execution(bytea, bytea, bytea, bytea, bytea),
    control.guard_producer_acquisition_job() FROM PUBLIC;
