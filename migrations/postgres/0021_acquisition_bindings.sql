-- Retain collection attribution independently of native/scientific event bytes.
-- Existing NULL reservations stay unbound; no historical request is adopted.
ALTER TABLE control.acquisition_scopes ADD CONSTRAINT acquisition_scope_source
    UNIQUE (dataset_id, scope_id, endpoint_id, access_scope_id);

ALTER TABLE control.producer_requests
    ADD COLUMN acquisition_scope_id bytea,
    ADD CONSTRAINT producer_acquisition_identity CHECK (
        acquisition_scope_id IS NULL OR (
            octet_length(acquisition_scope_id) = 16
            AND acquisition_scope_id <> decode(repeat('00', 16), 'hex')
            AND relay_endpoint_id IS NOT NULL
        )
    ),
    ADD CONSTRAINT producer_acquisition_source FOREIGN KEY
        (dataset_id, acquisition_scope_id, relay_endpoint_id, relay_access_scope_id)
        REFERENCES control.acquisition_scopes(dataset_id, scope_id, endpoint_id, access_scope_id);

-- Control is already fenced. Dataset serialization precedes domain/Ingest/
-- producer locks; reentrant dataset acquisition after0020 takes no new lock.
-- This validates retained relational intent, not signed membership or permission.
CREATE FUNCTION control.require_acquisition_scope(
    selected_dataset bytea, selected_scope bytea,
    selected_endpoint bytea, selected_access bytea
) RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog, control AS $$
BEGIN
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'acquisition scope checks require READ COMMITTED' USING ERRCODE = '25001';
    END IF;
    PERFORM 1 FROM control.datasets WHERE dataset_id = selected_dataset FOR UPDATE;
    -- Separate statement sees policy advancement committed while the lock waited.
    IF NOT EXISTS (
        SELECT 1 FROM control.acquisition_scopes s
        JOIN control.datasets d ON d.dataset_id = s.dataset_id
        WHERE s.dataset_id = selected_dataset AND s.scope_id = selected_scope
            AND s.endpoint_id = selected_endpoint AND s.access_scope_id = selected_access
            AND s.policy_revision = d.policy_revision
    ) THEN
        RAISE EXCEPTION 'acquisition scope is not current for this source' USING ERRCODE = '23514';
    END IF;
END $$;

CREATE FUNCTION control.guard_producer_acquisition_scope() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, control AS $$
BEGIN
    -- Imports retain their original lock order and have no collection binding.
    IF NEW.acquisition_scope_id IS NULL THEN RETURN NEW; END IF;
    IF num_nonnulls(NEW.relay_endpoint_id, NEW.relay_access_scope_id,
            NEW.relay_enrollment_generation, NEW.relay_route_id) <> 4 THEN
        RAISE EXCEPTION 'producer acquisition scope requires a complete relay source'
            USING ERRCODE = '23514';
    END IF;
    PERFORM control.require_acquisition_scope(NEW.dataset_id, NEW.acquisition_scope_id,
        NEW.relay_endpoint_id, NEW.relay_access_scope_id);
    RETURN NEW;
END $$;

--0020 already fences Control/source before this trigger; producer_guard follows.
CREATE TRIGGER producer_request_01_acquisition_scope BEFORE INSERT ON control.producer_requests
    FOR EACH ROW EXECUTE FUNCTION control.guard_producer_acquisition_scope();

REVOKE ALL ON FUNCTION control.require_acquisition_scope(bytea, bytea, bytea, bytea),
    control.guard_producer_acquisition_scope() FROM PUBLIC;
