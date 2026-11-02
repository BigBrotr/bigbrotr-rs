-- First passive Monitor: immutable bounded intent, never an execution witness.
-- No acquisition job, owner takeover, automatic re-probe or display-TTL deletion.
CREATE FUNCTION control.measurement_context_valid(context bytea, dataset bytea)
RETURNS boolean LANGUAGE plpgsql IMMUTABLE STRICT SET search_path=pg_catalog AS $$
DECLARE n integer; kinds integer[]:=ARRAY[1,2,6,3,5,2,4];
BEGIN
    IF octet_length(context)<>294 THEN RETURN false; END IF;
    FOR n IN 0..6 LOOP
        IF substring(context FROM n*42+1 FOR 16)<>dataset
            OR get_byte(context,n*42+16)<>0 OR get_byte(context,n*42+17)<>kinds[n+1]
            OR substring(context FROM n*42+19 FOR 16)=decode(repeat('00',16),'hex')
            OR substring(context FROM n*42+35 FOR 8)=decode(repeat('00',8),'hex')
        THEN RETURN false; END IF;
    END LOOP;
    RETURN true;
END $$;
CREATE TABLE control.measurement_dispatches (
    dataset_id control.id16 NOT NULL REFERENCES control.datasets(dataset_id),
    attempt_id control.id16 NOT NULL,
    endpoint_id control.id16 NOT NULL,
    access_scope_id control.id16 NOT NULL,
    enrollment_generation control.u64 NOT NULL CHECK(enrollment_generation>0),
    route_id control.id16 NOT NULL,
    context_bytes bytea NOT NULL CHECK(control.measurement_context_valid(context_bytes,dataset_id)),
    profile_version smallint NOT NULL DEFAULT 1 CHECK(profile_version=1),
    authority_ceiling_us bigint NOT NULL CHECK(authority_ceiling_us BETWEEN 1 AND 5000000),
    config_revision control.u64 NOT NULL,
    policy_revision control.u64 NOT NULL,
    issuer_resource control.id16 NOT NULL,
    issuer_epoch control.u64 NOT NULL,
    issuer_incarnation control.id16 NOT NULL,
    issuer_role text NOT NULL DEFAULT 'control' CHECK(issuer_role='control'),
    created_at_us bigint NOT NULL,
    dispatch_deadline_us bigint NOT NULL,
    PRIMARY KEY(dataset_id,attempt_id),
    CHECK(dispatch_deadline_us>created_at_us AND dispatch_deadline_us-created_at_us<=authority_ceiling_us),
    FOREIGN KEY(dataset_id,endpoint_id,access_scope_id,enrollment_generation)
        REFERENCES control.relay_enrollment_changes(dataset_id,endpoint_id,access_scope_id,generation),
    FOREIGN KEY(dataset_id,endpoint_id,route_id) REFERENCES control.network_routes(dataset_id,endpoint_id,route_id),
    FOREIGN KEY(dataset_id,config_revision) REFERENCES control.config_revisions(dataset_id,revision),
    FOREIGN KEY(dataset_id,policy_revision) REFERENCES control.policy_revisions(dataset_id,revision),
    FOREIGN KEY(dataset_id,issuer_resource,issuer_epoch,issuer_role,issuer_incarnation)
        REFERENCES control.owner_epochs(dataset_id,resource_id,epoch,role,process_incarnation)
);
CREATE FUNCTION control.guard_measurement_dispatch() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE now_us bigint;
BEGIN
    IF TG_OP<>'INSERT' THEN
        RAISE EXCEPTION 'measurement dispatch intent is immutable and retained' USING ERRCODE='23514';
    END IF;
    IF NOT control.measurement_context_valid(NEW.context_bytes,NEW.dataset_id) OR NEW.profile_version<>1 OR NEW.authority_ceiling_us NOT BETWEEN 1 AND 5000000 THEN
        RAISE EXCEPTION 'invalid closed Monitor context or profile' USING ERRCODE='23514';
    END IF;
    PERFORM control.require_control_owner(NEW.dataset_id,NEW.issuer_resource,NEW.issuer_epoch,NEW.issuer_incarnation);
    PERFORM control.require_relay_source(NEW.dataset_id,NEW.endpoint_id,NEW.access_scope_id,NEW.enrollment_generation,NEW.route_id);
    now_us:=(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint;
    IF now_us<NEW.created_at_us OR now_us>=NEW.dispatch_deadline_us
        OR NOT EXISTS(SELECT 1 FROM control.owner_epochs WHERE resource_id=NEW.issuer_resource
            AND epoch=NEW.issuer_epoch AND lease_expires_at_us>now_us)
        OR NOT EXISTS(SELECT 1 FROM control.datasets d
            JOIN control.config_revisions c ON c.dataset_id=d.dataset_id AND c.revision=d.config_revision
            JOIN control.policy_revisions p ON p.dataset_id=d.dataset_id AND p.revision=d.policy_revision
            WHERE d.dataset_id=NEW.dataset_id AND d.config_revision=NEW.config_revision
                AND d.policy_revision=NEW.policy_revision
                AND c.document->>'format'='bigbrotr-product-policy/3'
                AND p.document->>'format'='bigbrotr-product-policy/3'
                AND c.document->'settings'->'monitor.enabled'='true'::jsonb
                AND p.document->'settings'->'monitor.enabled'='true'::jsonb)
        OR NOT EXISTS(SELECT 1 FROM control.relay_access_contexts a
            WHERE a.dataset_id=NEW.dataset_id AND a.access_scope_id=NEW.access_scope_id
                AND a.access_class='public' AND a.dictionary_kind=4
                AND substring(NEW.context_bytes FROM 271 FOR 16)=a.allocation_domain
                AND substring(NEW.context_bytes FROM 287 FOR 8)=control.command_u64_bytes(a.ordinal))
    THEN RAISE EXCEPTION 'measurement dispatch requires current source and policy authority' USING ERRCODE='BB001'; END IF;
    -- A different random attempt ID, route, generation or reallocated dictionary
    -- cannot bypass this source's unresolved obligation.
    IF EXISTS(SELECT 1 FROM control.measurement_dispatches d
        WHERE d.dataset_id=NEW.dataset_id AND d.endpoint_id=NEW.endpoint_id AND d.access_scope_id=NEW.access_scope_id
            AND NOT EXISTS(SELECT 1 FROM control.measurement_dispatch_refusals f WHERE f.dataset_id=d.dataset_id AND f.attempt_id=d.attempt_id)
            AND NOT EXISTS(SELECT 1 FROM control.producer_requests r JOIN control.producer_outcomes o
                ON o.dataset_id=r.dataset_id AND o.producer_slot_id=r.producer_slot_id AND o.producer_epoch=r.producer_epoch AND o.request_sequence=r.request_sequence
                WHERE r.dataset_id=d.dataset_id AND r.monitor_attempt_id=d.attempt_id AND NOT r.monitor_resolution AND o.outcome_kind IN(1,2))) THEN
        RAISE EXCEPTION 'source has an unresolved measurement dispatch' USING ERRCODE='BB001';
    END IF;
    -- Initial bounded profile. Refuse further intents; never delete an unresolved
    -- obligation to return capacity. Later checked retirement owns reclamation.
    IF (SELECT count(*) FROM control.measurement_dispatches WHERE dataset_id=NEW.dataset_id)>=64 THEN
        RAISE EXCEPTION 'retained measurement dispatch limit' USING ERRCODE='54000';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER measurement_dispatch_guard BEFORE INSERT OR UPDATE OR DELETE ON control.measurement_dispatches
    FOR EACH ROW EXECUTE FUNCTION control.guard_measurement_dispatch();

-- A private local proof is consumed by the trusted adapter. These operational
-- rows never claim actual start or manufacture a scientific unknown outcome.
CREATE TABLE control.measurement_dispatch_refusals (
    dataset_id control.id16 NOT NULL,
    attempt_id control.id16 NOT NULL,
    reason smallint NOT NULL CHECK(reason IN(1,2,3,4)),
    issuer_resource control.id16 NOT NULL,
    issuer_epoch control.u64 NOT NULL,
    issuer_incarnation control.id16 NOT NULL,
    issuer_role text NOT NULL DEFAULT 'control' CHECK(issuer_role='control'),
    recorded_at_us bigint NOT NULL,
    PRIMARY KEY(dataset_id,attempt_id),
    FOREIGN KEY(dataset_id,attempt_id) REFERENCES control.measurement_dispatches(dataset_id,attempt_id),
    FOREIGN KEY(dataset_id,issuer_resource,issuer_epoch,issuer_role,issuer_incarnation)
        REFERENCES control.owner_epochs(dataset_id,resource_id,epoch,role,process_incarnation)
);
CREATE FUNCTION control.guard_measurement_refusal() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
    IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'measurement refusal is immutable and retained' USING ERRCODE='23514'; END IF;
    PERFORM control.require_control_owner(NEW.dataset_id,NEW.issuer_resource,NEW.issuer_epoch,NEW.issuer_incarnation);
    PERFORM 1 FROM control.datasets WHERE dataset_id=NEW.dataset_id FOR UPDATE;
    PERFORM 1 FROM control.measurement_dispatches WHERE dataset_id=NEW.dataset_id AND attempt_id=NEW.attempt_id FOR SHARE;
    IF NOT FOUND OR EXISTS(SELECT 1 FROM control.producer_requests WHERE dataset_id=NEW.dataset_id AND monitor_attempt_id=NEW.attempt_id) THEN
        RAISE EXCEPTION 'known no-I/O closure cannot replace evidence reservation' USING ERRCODE='23514';
    END IF;
    IF NOT EXISTS(SELECT 1 FROM control.owner_epochs WHERE resource_id=NEW.issuer_resource AND epoch=NEW.issuer_epoch
        AND lease_expires_at_us>(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint) THEN
        RAISE EXCEPTION 'measurement refusal requires current Control authority' USING ERRCODE='BB001';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER measurement_refusal_guard BEFORE INSERT OR UPDATE OR DELETE ON control.measurement_dispatch_refusals
    FOR EACH ROW EXECUTE FUNCTION control.guard_measurement_refusal();
CREATE CONSTRAINT TRIGGER zzz_measurement_refusal_complete AFTER INSERT ON control.measurement_dispatch_refusals
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION control.guard_measurement_refusal();

ALTER TABLE control.producer_requests
    ADD COLUMN monitor_attempt_id control.id16,
    ADD COLUMN monitor_resolution boolean,
    ADD CONSTRAINT producer_monitor_shape CHECK (
        (monitor_attempt_id IS NULL AND monitor_resolution IS NULL)
        OR (monitor_attempt_id IS NOT NULL AND monitor_resolution IS NOT NULL
            AND relay_endpoint_id IS NOT NULL AND acquisition_job_id IS NULL AND acquisition_scope_id IS NULL)),
    ADD CONSTRAINT producer_monitor_dispatch FOREIGN KEY(dataset_id,monitor_attempt_id)
        REFERENCES control.measurement_dispatches(dataset_id,attempt_id);
-- A retry cannot allocate a replacement request for the same initial fact or
-- resolution. Native admission owns unknown->terminal reference validation.
CREATE UNIQUE INDEX producer_monitor_evidence_identity
    ON control.producer_requests(dataset_id,monitor_attempt_id,monitor_resolution)
    WHERE monitor_attempt_id IS NOT NULL;

-- Historical context lock only. Current Control is checked by every caller.
-- Neither old execution deadline, current enrollment nor Monitor enablement
-- governs the obligation to resolve the original evidence request.
CREATE FUNCTION control.require_measurement_evidence(wanted_dataset bytea,wanted_attempt bytea,
    wanted_endpoint bytea,wanted_access bytea,wanted_generation numeric,wanted_route bytea)
RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
    IF current_setting('transaction_isolation')<>'read committed' THEN
        RAISE EXCEPTION 'measurement evidence requires READ COMMITTED' USING ERRCODE='25001';
    END IF;
    PERFORM 1 FROM control.datasets WHERE dataset_id=wanted_dataset FOR UPDATE;
    PERFORM 1 FROM control.measurement_dispatches WHERE dataset_id=wanted_dataset AND attempt_id=wanted_attempt FOR SHARE;
    IF NOT EXISTS(SELECT 1 FROM control.measurement_dispatches WHERE dataset_id=wanted_dataset AND attempt_id=wanted_attempt
        AND endpoint_id=wanted_endpoint AND access_scope_id=wanted_access AND enrollment_generation=wanted_generation AND route_id=wanted_route
        AND NOT EXISTS(SELECT 1 FROM control.measurement_dispatch_refusals f WHERE f.dataset_id=wanted_dataset AND f.attempt_id=wanted_attempt))
    THEN RAISE EXCEPTION 'measurement evidence requires exact original dispatch' USING ERRCODE='23514'; END IF;
END $$;

CREATE OR REPLACE FUNCTION control.guard_producer_acquisition_job() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
    IF NEW.relay_endpoint_id IS NULL THEN
        IF NEW.monitor_attempt_id IS NOT NULL THEN RAISE EXCEPTION 'monitor source missing' USING ERRCODE='23514'; END IF;
        RETURN NEW;
    END IF;
    PERFORM control.require_control_owner(NEW.dataset_id,NEW.issuer_resource,NEW.issuer_epoch,NEW.issuer_incarnation);
    IF NEW.monitor_attempt_id IS NOT NULL THEN
        IF NEW.acquisition_job_id IS NOT NULL OR NEW.acquisition_scope_id IS NOT NULL OR NEW.monitor_resolution IS NULL THEN
            RAISE EXCEPTION 'relay execution bindings are exclusive' USING ERRCODE='23514';
        END IF;
        PERFORM control.require_measurement_evidence(NEW.dataset_id,NEW.monitor_attempt_id,NEW.relay_endpoint_id,
            NEW.relay_access_scope_id,NEW.relay_enrollment_generation,NEW.relay_route_id);
    ELSE
        IF NEW.acquisition_job_id IS NULL OR NEW.acquisition_scope_id IS NULL THEN
            RAISE EXCEPTION 'new relay reservation requires its finite acquisition job' USING ERRCODE='23514';
        END IF;
        PERFORM control.require_acquisition_execution(NEW.dataset_id,NEW.acquisition_job_id,
            NEW.acquisition_scope_id,NEW.relay_endpoint_id,NEW.relay_access_scope_id);
    END IF;
    RETURN NEW;
END $$;
CREATE OR REPLACE FUNCTION control.guard_producer_relay_source() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE members integer;
BEGIN
    members:=num_nonnulls(NEW.relay_endpoint_id,NEW.relay_access_scope_id,NEW.relay_enrollment_generation,NEW.relay_route_id);
    IF members=0 THEN RETURN NEW; END IF;
    IF members<>4 OR NEW.relay_enrollment_generation<=0 THEN
        RAISE EXCEPTION 'producer relay source must be complete' USING ERRCODE='23514';
    END IF;
    PERFORM control.require_control_owner(NEW.dataset_id,NEW.issuer_resource,NEW.issuer_epoch,NEW.issuer_incarnation);
    IF NEW.monitor_attempt_id IS NOT NULL THEN
        PERFORM control.require_measurement_evidence(NEW.dataset_id,NEW.monitor_attempt_id,NEW.relay_endpoint_id,
            NEW.relay_access_scope_id,NEW.relay_enrollment_generation,NEW.relay_route_id);
    ELSE
        PERFORM control.require_relay_source(NEW.dataset_id,NEW.relay_endpoint_id,
            NEW.relay_access_scope_id,NEW.relay_enrollment_generation,NEW.relay_route_id);
    END IF;
    RETURN NEW;
END $$;
-- Match the old final acquisition recheck for new Monitor reservations.
CREATE CONSTRAINT TRIGGER zzz_producer_monitor_complete AFTER INSERT ON control.producer_requests
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN(NEW.monitor_attempt_id IS NOT NULL)
    EXECUTE FUNCTION control.guard_producer_acquisition_job();
REVOKE ALL ON control.measurement_dispatches,control.measurement_dispatch_refusals FROM PUBLIC;
REVOKE ALL ON FUNCTION control.measurement_context_valid(bytea,bytea),control.guard_measurement_dispatch(),
    control.require_measurement_evidence(bytea,bytea,bytea,bytea,numeric,bytea),control.guard_measurement_refusal() FROM PUBLIC;
