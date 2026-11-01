-- Bounded linear request obligations and terminal evidence. Neither a plan nor
-- an ordinary EOSE is complete history. Native proof remains the typed owner's.
CREATE TABLE control.acquisition_quanta (
    quantum_id control.id16 PRIMARY KEY,
    quantum_profile smallint NOT NULL DEFAULT 1 CHECK(quantum_profile=1),
    quantum_number control.u64 NOT NULL CHECK(quantum_number>0),
    dataset_id control.id16 NOT NULL,
    job_id control.id16 NOT NULL,
    scope_id control.id16 NOT NULL,
    enrollment_generation control.u64 NOT NULL CHECK(enrollment_generation>0),
    route_id control.id16 NOT NULL,
    producer_slot_id control.id16 NOT NULL,
    producer_epoch control.u64 NOT NULL CHECK(producer_epoch>0),
    manifest_sha256 control.digest32 NOT NULL,
    issuer_resource control.id16 NOT NULL,
    issuer_epoch control.u64 NOT NULL CHECK(issuer_epoch>0),
    issuer_role text NOT NULL DEFAULT 'control' CHECK(issuer_role='control'),
    issuer_incarnation control.id16 NOT NULL,
    interval_lower control.u64 NOT NULL,
    interval_upper control.u64 NOT NULL,
    horizon control.u64 NOT NULL,
    request_predicate bytea NOT NULL CHECK(octet_length(request_predicate) BETWEEN 1 AND 2097152),
    predicate_sha256 control.digest32 NOT NULL CHECK(predicate_sha256=sha256(request_predicate)),
    maximum_dependencies integer NOT NULL CHECK(maximum_dependencies BETWEEN 0 AND 768),
    checkpoint_before control.u64 NOT NULL,
    classified_before control.u64 NOT NULL,
    charged_bytes bigint NOT NULL CHECK(charged_bytes BETWEEN 1024 AND 33554432),
    created_at_us bigint NOT NULL,
    terminal_kind text CHECK(terminal_kind IN ('eose','closed','deadline','cancelled','frame_cap',
        'event_cap','transport','decode','disconnected')),
    terminal_frame bytea CHECK(octet_length(terminal_frame)<=2097152),
    request_id bytea CHECK(octet_length(request_id)=48 AND encode(request_id,'escape')~'^[0-9a-f]{48}$'),
    terminal_sequence control.u64,
    terminal_received_at_us bigint,
    teardown_complete boolean,
    retained_events smallint CHECK(retained_events BETWEEN 0 AND 3),
    classified_prefix smallint CHECK(classified_prefix BETWEEN 0 AND 3),
    effect_records bytea CHECK(octet_length(effect_records)%93=0 AND octet_length(effect_records)<=279),
    dependencies bytea CHECK(octet_length(dependencies)%72=0 AND octet_length(dependencies)<=55296),
    checkpoint_after control.u64,
    checkpoint_sha256 control.digest32,
    result_class text CHECK(result_class IN ('interrupted','linear_observed','relay_reported_exhaustion')),
    gap_reason text CHECK(gap_reason IN ('closed','deadline','cancelled','frame_cap','event_cap',
        'transport','decode','disconnected','unclassified_suffix','teardown','contradictory_hints','auth','ordering')),
    continuation_upper control.u64,
    completed_at_us bigint,
    CHECK(interval_lower<interval_upper AND interval_upper<=horizon),
    CHECK((terminal_kind IS NULL)=(completed_at_us IS NULL)),
    CHECK((terminal_kind IS NULL AND num_nonnulls(terminal_frame,request_id,terminal_sequence,
        terminal_received_at_us,teardown_complete,retained_events,classified_prefix,effect_records,
        dependencies,checkpoint_after,checkpoint_sha256,result_class,gap_reason,continuation_upper)=0)
        OR (terminal_kind IS NOT NULL AND terminal_frame IS NOT NULL
            AND terminal_received_at_us IS NOT NULL AND teardown_complete IS NOT NULL
            AND retained_events IS NOT NULL AND classified_prefix IS NOT NULL
            AND classified_prefix<=retained_events AND effect_records IS NOT NULL
            AND dependencies IS NOT NULL AND checkpoint_after IS NOT NULL
            AND result_class IS NOT NULL AND continuation_upper IS NOT NULL
            AND continuation_upper>interval_lower AND continuation_upper<=interval_upper
            AND (result_class='interrupted')=(gap_reason IS NOT NULL)
            AND (terminal_kind NOT IN ('eose','closed') OR (request_id IS NOT NULL AND terminal_sequence>0))
            AND octet_length(dependencies)/72<=maximum_dependencies)),
    UNIQUE(job_id,quantum_number),
    FOREIGN KEY(job_id) REFERENCES control.acquisition_jobs(job_id),
    FOREIGN KEY(dataset_id,scope_id) REFERENCES control.acquisition_scopes(dataset_id,scope_id),
    FOREIGN KEY(producer_slot_id,producer_epoch,dataset_id)
        REFERENCES control.producer_epochs(producer_slot_id,producer_epoch,dataset_id),
    FOREIGN KEY(dataset_id,issuer_resource,issuer_epoch,issuer_role,issuer_incarnation)
        REFERENCES control.owner_epochs(dataset_id,resource_id,epoch,role,process_incarnation)
);
CREATE INDEX acquisition_quanta_producer ON control.acquisition_quanta(producer_slot_id,producer_epoch);
CREATE INDEX acquisition_quanta_job ON control.acquisition_quanta(job_id,quantum_number DESC);
CREATE UNIQUE INDEX acquisition_quantum_pending ON control.acquisition_quanta(job_id)
    WHERE terminal_kind IS NULL;

CREATE FUNCTION control.quantum_retained_charge(wanted_slot bytea,wanted_epoch numeric)
RETURNS bigint LANGUAGE sql STABLE STRICT SET search_path=pg_catalog AS $$
    SELECT coalesce(sum(charged_bytes),0)::bigint FROM control.acquisition_quanta
        WHERE producer_slot_id=wanted_slot AND producer_epoch=wanted_epoch
$$;

CREATE OR REPLACE FUNCTION control.require_producer_progress_capacity(
    wanted_slot bytea,wanted_epoch numeric,additional_input bigint,
    wanted_jobs integer,wanted_dependencies integer
) RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE retained bigint;
BEGIN
    IF wanted_jobs NOT BETWEEN 0 AND 64 OR wanted_dependencies NOT BETWEEN 0 AND 16384
        OR additional_input<0 THEN
        RAISE EXCEPTION 'producer progress exceeds its finite retained profile' USING ERRCODE='23514';
    END IF;
    SELECT coalesce(sum(input_bytes::bigint+65536),0) INTO retained FROM control.producer_requests
        WHERE producer_slot_id=wanted_slot AND producer_epoch=wanted_epoch;
    retained:=retained+control.quantum_retained_charge(wanted_slot,wanted_epoch);
    IF retained+additional_input+(CASE WHEN EXISTS(SELECT 1 FROM control.producer_checkpoints
        WHERE producer_slot_id=wanted_slot AND producer_epoch=wanted_epoch AND prefix_profile)
        OR (wanted_epoch=1 AND NOT EXISTS(SELECT 1 FROM control.producer_checkpoints
            WHERE producer_slot_id=wanted_slot AND producer_epoch=wanted_epoch)) THEN 32768 ELSE 0 END)
        +1024+1024::bigint*wanted_jobs+256::bigint*wanted_dependencies>33554432 THEN
        RAISE EXCEPTION 'producer progress and input/completion exceed retained capacity' USING ERRCODE='23514';
    END IF;
END $$;

-- Reservation INSERTs also fund NEW input/completion through the same envelope.
-- The pre-insert history guard cannot account for an as-yet absent reservation.
CREATE OR REPLACE FUNCTION control.reserve_producer_outcome_capacity() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE head control.producer_checkpoints%ROWTYPE;
BEGIN
    PERFORM 1 FROM control.producer_epochs WHERE producer_slot_id=NEW.producer_slot_id
        AND producer_epoch=NEW.producer_epoch FOR UPDATE;
    SELECT * INTO head FROM control.producer_checkpoints WHERE producer_slot_id=NEW.producer_slot_id
        AND producer_epoch=NEW.producer_epoch;
    PERFORM control.require_producer_progress_capacity(NEW.producer_slot_id,NEW.producer_epoch,
        NEW.input_bytes::bigint+65536,coalesce(head.progress_jobs,0),coalesce(head.dependency_count,0));
    RETURN NEW;
END $$;

CREATE FUNCTION control.guard_acquisition_quantum() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE scope control.acquisition_scopes%ROWTYPE; producer control.producer_epochs%ROWTYPE;
        h control.producer_checkpoints%ROWTYPE; previous_charge bigint:=0;
BEGIN
    IF TG_OP='DELETE' THEN
        RAISE EXCEPTION 'linear quantum obligations require explicit reclamation' USING ERRCODE='23514';
    END IF;
    PERFORM control.require_control_owner(NEW.dataset_id,NEW.issuer_resource,NEW.issuer_epoch,NEW.issuer_incarnation);
    SELECT * INTO STRICT scope FROM control.acquisition_scopes
        WHERE dataset_id=NEW.dataset_id AND scope_id=NEW.scope_id;
    PERFORM control.require_acquisition_execution(NEW.dataset_id,NEW.job_id,NEW.scope_id,scope.endpoint_id,scope.access_scope_id);
    PERFORM control.require_relay_source(NEW.dataset_id,scope.endpoint_id,scope.access_scope_id,
        NEW.enrollment_generation,NEW.route_id);
    SELECT * INTO STRICT producer FROM control.producer_epochs WHERE producer_slot_id=NEW.producer_slot_id
        AND producer_epoch=NEW.producer_epoch AND dataset_id=NEW.dataset_id FOR UPDATE;
    SELECT * INTO h FROM control.producer_checkpoints WHERE producer_slot_id=NEW.producer_slot_id
        AND producer_epoch=NEW.producer_epoch;
    IF producer.state<>'open' OR producer.reserved_through_request<>coalesce(h.complete_through_request,0)
        OR (h.manifest_sha256 IS NOT NULL AND h.manifest_sha256<>NEW.manifest_sha256)
        OR NOT EXISTS(SELECT 1 FROM control.jobs WHERE job_id=NEW.job_id
            AND NEW.horizon<=floor(created_at_us::numeric/1000000)
            AND owner_resource_id=NEW.issuer_resource AND owner_epoch=NEW.issuer_epoch
            AND owner_incarnation=NEW.issuer_incarnation) THEN
        RAISE EXCEPTION 'linear quantum requires its exact current complete producer and job' USING ERRCODE='23514';
    END IF;
    IF TG_WHEN='AFTER' THEN
        IF NEW.terminal_kind IS NOT NULL AND (NEW.checkpoint_after<>coalesce(h.checkpoint_revision,0)
            OR NEW.checkpoint_sha256 IS DISTINCT FROM h.checkpoint_digest) THEN
            RAISE EXCEPTION 'linear quantum checkpoint changed before commit' USING ERRCODE='23514';
        END IF;
        RETURN NEW;
    END IF;
    IF TG_OP='INSERT' THEN
        IF NEW.quantum_number<>(SELECT coalesce(max(q.quantum_number),0)+1
                FROM control.acquisition_quanta q WHERE q.job_id=NEW.job_id)
            OR NEW.classified_before<>coalesce((SELECT classified_events FROM control.acquisition_progress
                WHERE producer_slot_id=NEW.producer_slot_id AND producer_epoch=NEW.producer_epoch AND job_id=NEW.job_id),0)
            OR NEW.terminal_kind IS NOT NULL OR NEW.checkpoint_before<>coalesce(h.checkpoint_revision,0)
            OR NEW.charged_bytes<>1024+octet_length(NEW.request_predicate)+2097152
                +3*1024+256::bigint*NEW.maximum_dependencies THEN
            RAISE EXCEPTION 'linear plan requires prefunded immutable completion' USING ERRCODE='23514';
        END IF;
    ELSE
        previous_charge:=OLD.charged_bytes;
        IF OLD.terminal_kind IS NOT NULL OR NEW.terminal_kind IS NULL
            OR (to_jsonb(NEW)-ARRAY['charged_bytes','terminal_kind','terminal_frame','request_id','terminal_sequence',
                'terminal_received_at_us','teardown_complete','retained_events','classified_prefix','effect_records',
                'dependencies','checkpoint_after','checkpoint_sha256','result_class','gap_reason','continuation_upper','completed_at_us'])
            IS DISTINCT FROM (to_jsonb(OLD)-ARRAY['charged_bytes','terminal_kind','terminal_frame','request_id','terminal_sequence',
                'terminal_received_at_us','teardown_complete','retained_events','classified_prefix','effect_records',
                'dependencies','checkpoint_after','checkpoint_sha256','result_class','gap_reason','continuation_upper','completed_at_us'])
            OR NEW.checkpoint_after<>coalesce(h.checkpoint_revision,0)
            OR NEW.checkpoint_sha256 IS DISTINCT FROM h.checkpoint_digest
            OR NEW.charged_bytes<>1024+octet_length(NEW.request_predicate)+octet_length(NEW.terminal_frame)
                +octet_length(NEW.effect_records)+256::bigint*(octet_length(NEW.dependencies)/72)
            OR NEW.charged_bytes>OLD.charged_bytes THEN
            RAISE EXCEPTION 'linear terminal differs from its exact retained plan or checkpoint' USING ERRCODE='23514';
        END IF;
    END IF;
    PERFORM control.require_producer_progress_capacity(NEW.producer_slot_id,NEW.producer_epoch,
        greatest(NEW.charged_bytes-previous_charge,0),coalesce(h.progress_jobs,0),coalesce(h.dependency_count,0));
    RETURN NEW;
END $$;
CREATE TRIGGER acquisition_quantum_guard BEFORE INSERT OR UPDATE OR DELETE ON control.acquisition_quanta
    FOR EACH ROW EXECUTE FUNCTION control.guard_acquisition_quantum();
CREATE CONSTRAINT TRIGGER zzz_acquisition_quantum_owner AFTER INSERT OR UPDATE ON control.acquisition_quanta
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION control.guard_acquisition_quantum();
CREATE TRIGGER acquisition_quanta_no_truncate BEFORE TRUNCATE ON control.acquisition_quanta
    FOR EACH STATEMENT EXECUTE FUNCTION control.reject_history_mutation();
REVOKE ALL ON control.acquisition_quanta FROM PUBLIC;
REVOKE ALL ON FUNCTION control.quantum_retained_charge(bytea,numeric),control.guard_acquisition_quantum() FROM PUBLIC;
