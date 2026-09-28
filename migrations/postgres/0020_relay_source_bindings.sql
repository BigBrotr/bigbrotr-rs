-- Operational recovery context, including when scientific provenance is off.
-- Existing reservations remain unbound; neither native inputs nor receipts are
-- rewritten or retrospectively attributed to an enrollment.
ALTER TABLE control.producer_requests
    ADD COLUMN relay_endpoint_id control.id16,
    ADD COLUMN relay_access_scope_id control.id16,
    ADD COLUMN relay_enrollment_generation control.u64,
    ADD COLUMN relay_route_id control.id16,
    ADD CONSTRAINT producer_relay_source_complete CHECK (
        num_nonnulls(relay_endpoint_id, relay_access_scope_id,
            relay_enrollment_generation, relay_route_id) IN (0, 4)
        AND (relay_enrollment_generation IS NULL OR relay_enrollment_generation > 0)
    ),
    ADD CONSTRAINT producer_relay_generation FOREIGN KEY
        (dataset_id, relay_endpoint_id, relay_access_scope_id, relay_enrollment_generation)
        REFERENCES control.relay_enrollment_changes(dataset_id, endpoint_id, access_scope_id, generation),
    ADD CONSTRAINT producer_relay_route FOREIGN KEY
        (dataset_id, relay_endpoint_id, relay_route_id)
        REFERENCES control.network_routes(dataset_id, endpoint_id, route_id);

-- The caller must already hold its Control fence. This is relational source
-- validation only: bans, committed visibility and socket custody are not SQL
-- facts. Dataset then enrollment locks also serialize with management changes.
CREATE FUNCTION control.require_relay_source(
    selected_dataset bytea, selected_endpoint bytea, selected_access bytea,
    selected_generation numeric, selected_route bytea
) RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog, control AS $$
BEGIN
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'relay source checks require READ COMMITTED' USING ERRCODE = '25001';
    END IF;
    PERFORM 1 FROM control.datasets WHERE dataset_id = selected_dataset FOR UPDATE;
    PERFORM 1 FROM control.relay_enrollments
        WHERE dataset_id = selected_dataset AND endpoint_id = selected_endpoint
            AND access_scope_id = selected_access FOR SHARE;
    -- Separate post-wait statement; a route FK alone cannot prove which route
    -- belonged to the immutable generation (or that generation was enabled).
    IF NOT EXISTS (
        SELECT 1 FROM control.relay_enrollments e
        JOIN control.relay_enrollment_changes h
            ON h.dataset_id = e.dataset_id AND h.endpoint_id = e.endpoint_id
                AND h.access_scope_id = e.access_scope_id AND h.generation = e.generation
        WHERE e.dataset_id = selected_dataset AND e.endpoint_id = selected_endpoint
            AND e.access_scope_id = selected_access AND e.generation = selected_generation
            AND e.route_id = selected_route AND e.enabled
            AND h.route_id = selected_route AND h.enabled
    ) THEN
        RAISE EXCEPTION 'relay source enrollment is not current and enabled'
            USING ERRCODE = '23514';
    END IF;
END $$;

CREATE FUNCTION control.guard_producer_relay_source() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, control AS $$
DECLARE members integer;
BEGIN
    members := num_nonnulls(NEW.relay_endpoint_id, NEW.relay_access_scope_id,
        NEW.relay_enrollment_generation, NEW.relay_route_id);
    IF members = 0 THEN RETURN NEW; END IF;
    IF members <> 4 OR NEW.relay_enrollment_generation <= 0 THEN
        RAISE EXCEPTION 'producer relay source must be complete' USING ERRCODE = '23514';
    END IF;
    PERFORM control.require_control_owner(NEW.dataset_id, NEW.issuer_resource,
        NEW.issuer_epoch, NEW.issuer_incarnation);
    PERFORM control.require_relay_source(NEW.dataset_id, NEW.relay_endpoint_id,
        NEW.relay_access_scope_id, NEW.relay_enrollment_generation, NEW.relay_route_id);
    RETURN NEW;
END $$;
-- Before the existing Control/Ingest/producer guard. Unbound imports retain
-- their existing Control/job/Ingest/producer order without a late dataset lock.
CREATE TRIGGER producer_request_00_relay_source BEFORE INSERT ON control.producer_requests
    FOR EACH ROW EXECUTE FUNCTION control.guard_producer_relay_source();

CREATE FUNCTION control.exclude_producer_relay_import() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, control AS $$
BEGIN
    -- After producer_request_guard has locked the producer. Observe a concurrent
    -- import binding only after that wait; its trigger takes the same lock and
    -- refuses attaching an already reserved request in the opposite order.
    IF NEW.relay_endpoint_id IS NOT NULL AND EXISTS (
        SELECT 1 FROM control.producer_input_bindings
        WHERE producer_slot_id = NEW.producer_slot_id AND producer_epoch = NEW.producer_epoch
            AND request_sequence = NEW.request_sequence
    ) THEN
        RAISE EXCEPTION 'producer request cannot bind both relay and import sources'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER producer_request_source_exclusion BEFORE INSERT ON control.producer_requests
    FOR EACH ROW EXECUTE FUNCTION control.exclude_producer_relay_import();

REVOKE ALL ON FUNCTION control.require_relay_source(bytea, bytea, bytea, numeric, bytea),
    control.guard_producer_relay_source(), control.exclude_producer_relay_import() FROM PUBLIC;
