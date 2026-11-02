-- An explicit sealed workflow handoff, never execution permission or a cursor.
-- Native authentication remains the typed exclusive owner's responsibility.
CREATE TABLE control.acquisition_continuations (
    continuation_id control.id16 PRIMARY KEY,
    dataset_id control.id16 NOT NULL, job_id control.id16 NOT NULL REFERENCES control.acquisition_jobs(job_id),
    previous_job_revision control.u64 NOT NULL CHECK(previous_job_revision>=2),
    previous_continuation control.id16 REFERENCES control.acquisition_continuations(continuation_id),
    previous_owner_resource control.id16 NOT NULL, previous_owner_epoch control.u64 NOT NULL,
    previous_owner_incarnation control.id16 NOT NULL,
    scope_id control.id16 NOT NULL, enrollment_generation control.u64 NOT NULL CHECK(enrollment_generation>0),
    route_id control.id16 NOT NULL, producer_slot_id control.id16 NOT NULL,
    producer_epoch control.u64 NOT NULL CHECK(producer_epoch BETWEEN 1 AND 254),
    manifest_sha256 control.digest32 NOT NULL, shard_id control.u32 NOT NULL,
    source_sequence control.u64 NOT NULL, source_digest control.digest32 NOT NULL,
    checkpoint_revision control.u64 NOT NULL, complete_through_request control.u64 NOT NULL CHECK(complete_through_request<=64),
    checkpoint_sha256 control.digest32, last_quantum_number control.u64 NOT NULL CHECK(last_quantum_number<=385),
    sealing_resource control.id16 NOT NULL, sealing_epoch control.u64 NOT NULL, sealing_incarnation control.id16 NOT NULL,
    sealed_at_us bigint NOT NULL,
    target_epoch control.u64, target_manifest_sha256 control.digest32, activation_sha256 control.digest32,
    adopting_resource control.id16, adopting_epoch control.u64, adopting_incarnation control.id16,
    adopted_job_revision control.u64, adopted_at_us bigint, adoption_transaction xid8,
    CHECK((checkpoint_revision=0)=(checkpoint_sha256 IS NULL)),
    CHECK(num_nonnulls(target_epoch,target_manifest_sha256,activation_sha256,adopting_resource,adopting_epoch,
        adopting_incarnation,adopted_job_revision,adopted_at_us,adoption_transaction) IN (0,9)),
    CHECK(target_epoch IS NULL OR (target_epoch>producer_epoch AND target_epoch<=255
        AND adopted_job_revision=previous_job_revision+1 AND adopted_at_us>=sealed_at_us)),
    UNIQUE(job_id,previous_job_revision), UNIQUE(job_id,producer_slot_id,producer_epoch),
    FOREIGN KEY(dataset_id,scope_id) REFERENCES control.acquisition_scopes(dataset_id,scope_id),
    FOREIGN KEY(producer_slot_id,producer_epoch,dataset_id) REFERENCES control.producer_epochs(producer_slot_id,producer_epoch,dataset_id)
);
CREATE UNIQUE INDEX acquisition_continuation_pending ON control.acquisition_continuations(job_id) WHERE adopted_at_us IS NULL;
CREATE INDEX acquisition_continuation_producer ON control.acquisition_continuations(producer_slot_id,producer_epoch);

-- This shares the logical32MiB envelope; seals never refund original inputs.
CREATE OR REPLACE FUNCTION control.quantum_retained_charge(wanted_slot bytea,wanted_epoch numeric)
RETURNS bigint LANGUAGE sql STABLE STRICT SET search_path=pg_catalog AS $$
    WITH RECURSIVE retained AS (
        SELECT c.* FROM control.acquisition_continuations c WHERE c.producer_slot_id=wanted_slot
            AND (c.producer_epoch=wanted_epoch OR c.target_epoch=wanted_epoch)
        UNION SELECT p.* FROM control.acquisition_continuations p JOIN retained c ON p.continuation_id=c.previous_continuation
    ) SELECT coalesce((SELECT sum(charged_bytes) FROM control.acquisition_quanta
        WHERE producer_slot_id=wanted_slot AND producer_epoch=wanted_epoch),0)::bigint
        +1024*(SELECT count(*) FROM retained)
$$;

-- Same current actor/policy/source/deadline checks as ordinary execution; only
-- this private handoff consumer may inspect an excluded previous owner.
CREATE FUNCTION control.require_acquisition_continuation_authority(wanted_dataset bytea, wanted_job bytea)
RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE detail control.acquisition_jobs%ROWTYPE; job control.jobs%ROWTYPE;
    owner control.owner_epochs%ROWTYPE; now_us bigint;
BEGIN
    IF current_setting('transaction_isolation')<>'read committed' THEN
        RAISE EXCEPTION 'acquisition claims require READ COMMITTED' USING ERRCODE='25001';
    END IF;
    SELECT * INTO detail FROM control.acquisition_jobs
        WHERE dataset_id=wanted_dataset AND job_id=wanted_job;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'missing typed acquisition job' USING ERRCODE='23514';
    END IF;
    SELECT * INTO STRICT owner FROM control.owner_epochs
        WHERE dataset_id=wanted_dataset AND role='control' AND state='active';
    PERFORM control.require_control_owner(owner.dataset_id,owner.resource_id,owner.epoch,owner.process_incarnation);
    PERFORM 1 FROM control.datasets WHERE dataset_id=wanted_dataset FOR UPDATE;
    PERFORM 1 FROM control.local_accounts
        WHERE dataset_id=wanted_dataset AND principal_id=detail.actor_id FOR SHARE;
    PERFORM 1 FROM control.principal_grants
        WHERE dataset_id=wanted_dataset AND principal_id=detail.actor_id
            AND action='roles.control' AND resource_kind='dataset' AND resource_id=wanted_dataset FOR SHARE;
    PERFORM 1 FROM control.relay_enrollments e JOIN control.acquisition_scopes s
        ON s.dataset_id=e.dataset_id AND s.endpoint_id=e.endpoint_id AND s.access_scope_id=e.access_scope_id
        WHERE s.dataset_id=wanted_dataset AND s.scope_id=detail.scope_id FOR SHARE OF e;
    SELECT * INTO STRICT job FROM control.jobs WHERE job_id=wanted_job FOR UPDATE;
    IF job.acquisition_job_protocol IS DISTINCT FROM 1 OR NOT job.input_complete
        OR job.state<>'queued'
        OR (job.dataset_id,job.principal_id,job.request_key)
            IS DISTINCT FROM (detail.dataset_id,detail.actor_id,detail.request_key) THEN
        RAISE EXCEPTION 'ineligible acquisition job' USING ERRCODE='23514';
    END IF;
    now_us:=(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint;
    IF now_us<job.created_at_us OR now_us>=detail.deadline_at_us OR now_us>=owner.lease_expires_at_us
        OR NOT EXISTS (SELECT 1 FROM control.local_accounts
            WHERE dataset_id=wanted_dataset AND principal_id=detail.actor_id AND active)
        OR NOT EXISTS (SELECT 1 FROM control.principal_grants
            WHERE dataset_id=wanted_dataset AND principal_id=detail.actor_id
                AND action='roles.control' AND resource_kind='dataset' AND resource_id=wanted_dataset)
        OR NOT EXISTS (SELECT 1 FROM control.datasets d
            JOIN control.config_revisions c ON c.dataset_id=d.dataset_id AND c.revision=d.config_revision
            JOIN control.policy_revisions p ON p.dataset_id=d.dataset_id AND p.revision=d.policy_revision
            JOIN control.acquisition_scopes s ON s.dataset_id=d.dataset_id AND s.scope_id=detail.scope_id
            JOIN control.relay_enrollments e ON e.dataset_id=s.dataset_id
                AND e.endpoint_id=s.endpoint_id AND e.access_scope_id=s.access_scope_id
            WHERE d.dataset_id=wanted_dataset AND d.config_revision=job.config_revision
                AND d.policy_revision=job.policy_revision AND s.policy_revision=d.policy_revision AND e.enabled
                AND c.document->>'format'='bigbrotr-product-policy/3'
                AND p.document->>'format'='bigbrotr-product-policy/3'
                AND c.document->'settings'->'sync.enabled'='true'::jsonb
                AND p.document->'settings'->'sync.enabled'='true'::jsonb) THEN
        RAISE EXCEPTION 'acquisition job requires current authority and original remaining duration'
            USING ERRCODE='BB001';
    END IF;
END $$;

-- Called beneath the existing dataset/producer custody. Preserve the closed
-- profile's64 workflow identities even after an adoption CAS moves the job.
-- Each branch is bounded before UNION; each contains distinct JobIds, so65
-- from any branch already proves refusal. At most195 IDs reach deduplication.
CREATE FUNCTION control.require_acquisition_workflow_capacity(wanted_slot bytea,wanted_epoch numeric)
RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE p control.producer_epochs%ROWTYPE; original record; retained integer;
BEGIN
    SELECT * INTO STRICT p FROM control.producer_epochs WHERE producer_slot_id=wanted_slot AND producer_epoch=wanted_epoch;
    IF wanted_epoch=1 THEN
        SELECT registered_by_resource_id AS resource,registered_by_epoch AS epoch,registered_by_incarnation AS incarnation
            INTO STRICT original FROM control.producer_slots WHERE producer_slot_id=wanted_slot;
    ELSE
        SELECT * INTO STRICT original FROM control.ingest_lineage_activation(p.owner_resource_id,wanted_epoch);
    END IF;
    WITH claims AS (
        SELECT job_id FROM control.jobs WHERE dataset_id=p.dataset_id AND kind='acquisition'
            AND (owner_resource_id,owner_epoch,owner_incarnation)=(original.resource,original.epoch,original.incarnation)
            ORDER BY job_id LIMIT 65
    ), gaps AS (
        SELECT job_id FROM control.acquisition_gaps WHERE producer_slot_id=wanted_slot AND producer_epoch=wanted_epoch
            ORDER BY job_id LIMIT 65
    ), seals AS (
        SELECT job_id FROM control.acquisition_continuations WHERE producer_slot_id=wanted_slot AND producer_epoch=wanted_epoch
            ORDER BY job_id LIMIT 65
    ), identities AS (SELECT job_id FROM claims UNION SELECT job_id FROM gaps UNION SELECT job_id FROM seals)
    SELECT count(*) INTO retained FROM (SELECT job_id FROM identities LIMIT 65) bounded;
    IF retained>64 THEN
        RAISE EXCEPTION 'closed producer acquisition workflow identities exceed64' USING ERRCODE='23514';
    END IF;
END $$;

CREATE FUNCTION control.acquisition_continuation_covers(wanted_job bytea,wanted_slot bytea,wanted_epoch numeric)
RETURNS boolean LANGUAGE sql STABLE STRICT SET search_path=pg_catalog AS $$
    SELECT EXISTS(SELECT 1 FROM control.acquisition_continuations c
        JOIN control.producer_epochs p USING(producer_slot_id,producer_epoch,dataset_id)
        JOIN control.source_streams s ON s.dataset_id=c.dataset_id AND s.shard_id=c.shard_id AND s.owner_epoch=c.producer_epoch
        LEFT JOIN control.producer_checkpoints h ON h.producer_slot_id=c.producer_slot_id AND h.producer_epoch=c.producer_epoch AND h.dataset_id=c.dataset_id
        WHERE c.job_id=wanted_job AND c.producer_slot_id=wanted_slot AND c.producer_epoch=wanted_epoch
            AND p.state IN ('closing','retired') AND p.reserved_through_request=c.complete_through_request
            AND s.state='closed' AND s.owner_resource_id=p.owner_resource_id
            AND s.owner_incarnation=p.owner_incarnation AND s.last_complete_sequence=c.source_sequence
            AND s.closure_digest=c.source_digest AND coalesce(h.checkpoint_revision,0)=c.checkpoint_revision
            AND h.checkpoint_digest IS NOT DISTINCT FROM c.checkpoint_sha256
            AND (h.manifest_sha256 IS NULL OR h.manifest_sha256=c.manifest_sha256)
            AND coalesce(h.complete_through_request,0)=c.complete_through_request
            AND NOT EXISTS(SELECT 1 FROM control.acquisition_quanta q WHERE q.job_id=c.job_id
                AND q.producer_slot_id=c.producer_slot_id AND q.producer_epoch=c.producer_epoch
                AND (q.quantum_number>c.last_quantum_number OR q.manifest_sha256<>c.manifest_sha256
                    OR q.scope_id<>c.scope_id OR q.enrollment_generation<>c.enrollment_generation OR q.route_id<>c.route_id
                    OR (q.issuer_resource,q.issuer_epoch,q.issuer_incarnation) IS DISTINCT FROM
                        (c.previous_owner_resource,c.previous_owner_epoch,c.previous_owner_incarnation)
                    OR (q.entry_kind='request' AND (q.terminal_kind IS NULL OR NOT q.teardown_complete OR q.disposition NOT IN ('overlap','window_closed','saturated_second')))))
            AND NOT EXISTS(SELECT 1 FROM control.producer_requests r
                WHERE r.producer_slot_id=c.producer_slot_id AND r.producer_epoch=c.producer_epoch AND r.acquisition_job_id=c.job_id
                    AND NOT EXISTS(SELECT 1 FROM control.acquisition_quanta q
                        JOIN control.producer_outcomes o ON o.producer_slot_id=r.producer_slot_id
                            AND o.producer_epoch=r.producer_epoch AND o.request_sequence=r.request_sequence
                        CROSS JOIN LATERAL generate_series(0,octet_length(q.effect_records)/93-1) e(i)
                        WHERE q.job_id=c.job_id AND q.producer_slot_id=c.producer_slot_id AND q.producer_epoch=c.producer_epoch
                            AND q.quantum_number<=c.last_quantum_number AND q.terminal_kind IS NOT NULL AND q.teardown_complete
                            AND control.ingest_lineage_uint(q.effect_records,93*e.i+44,8)=r.request_sequence
                            AND control.ingest_lineage_uint(q.effect_records,93*e.i+42,2) BETWEEN 2 AND 4
                            AND q.dataset_id=r.dataset_id AND q.scope_id=r.acquisition_scope_id
                            AND q.request_id=r.progress_request_id
                            AND control.ingest_lineage_uint(q.effect_records,93*e.i,2)=r.progress_event_ordinal
                            AND control.ingest_lineage_uint(q.effect_records,93*e.i+2,8)=r.progress_wire_sequence
                            AND substring(q.effect_records FROM 93*e.i+11 FOR 32)=r.progress_input_sha256
                            AND o.dataset_id=r.dataset_id AND o.request_digest=r.request_digest
                            AND o.manifest_sha256=r.manifest_sha256
                            AND o.outcome_kind=control.ingest_lineage_uint(q.effect_records,93*e.i+42,2)-1)))
$$;

CREATE FUNCTION control.require_acquisition_continuation(c control.acquisition_continuations)
RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE j control.jobs%ROWTYPE; prior control.acquisition_continuations%ROWTYPE;
        p control.producer_epochs%ROWTYPE; a record; target record;
BEGIN
    PERFORM control.require_acquisition_continuation_authority(c.dataset_id,c.job_id);
    PERFORM control.require_acquisition_workflow_capacity(c.producer_slot_id,c.producer_epoch);
    SELECT * INTO STRICT j FROM control.jobs WHERE job_id=c.job_id FOR UPDATE;
    SELECT * INTO STRICT p FROM control.producer_epochs WHERE producer_slot_id=c.producer_slot_id AND producer_epoch=c.producer_epoch FOR UPDATE;
    IF j.checkpoint_revision<>0 OR j.reason_code IS NOT NULL OR j.completed_at_us IS NOT NULL
        OR j.result_manifest_digest IS NOT NULL OR j.dataset_id<>c.dataset_id
        OR NOT EXISTS(SELECT 1 FROM control.acquisition_jobs WHERE job_id=c.job_id AND scope_id=c.scope_id)
        OR NOT EXISTS(SELECT 1 FROM control.owner_epochs WHERE dataset_id=c.dataset_id
            AND resource_id=c.previous_owner_resource AND epoch=c.previous_owner_epoch
            AND process_incarnation=c.previous_owner_incarnation AND role='control'
            AND state='released' AND exclusion_method='storage_fenced')
        OR c.sealed_at_us<(SELECT closed_at_us FROM control.source_streams
            WHERE dataset_id=c.dataset_id AND shard_id=c.shard_id AND owner_epoch=c.producer_epoch)
        OR c.sealed_at_us>(extract(epoch FROM clock_timestamp())*1000000)::bigint THEN
        RAISE EXCEPTION 'continuation requires its exact excluded previous job and closed source' USING ERRCODE='23514';
    END IF;
    IF c.producer_epoch=1 THEN
        SELECT registered_by_resource_id AS resource,registered_by_epoch AS epoch,registered_by_incarnation AS incarnation
            INTO STRICT a FROM control.producer_slots WHERE producer_slot_id=c.producer_slot_id;
    ELSE
        SELECT * INTO STRICT a FROM control.ingest_lineage_activation(p.owner_resource_id,c.producer_epoch);
        IF sha256(control.ingest_lineage_manifest_at(p.owner_resource_id,c.producer_epoch))<>c.manifest_sha256 THEN
            RAISE EXCEPTION 'continuation manifest differs from retained activation' USING ERRCODE='23514';
        END IF;
    END IF;
    IF (a.resource,a.epoch,a.incarnation) IS DISTINCT FROM
        (c.previous_owner_resource,c.previous_owner_epoch,c.previous_owner_incarnation) THEN
        RAISE EXCEPTION 'continuation previous issuer differs from its native lifetime' USING ERRCODE='23514';
    END IF;
    IF c.previous_continuation IS NULL THEN
        IF c.previous_job_revision<>2 OR EXISTS(SELECT 1 FROM control.acquisition_continuations old
            WHERE old.job_id=c.job_id AND old.continuation_id<>c.continuation_id AND old.previous_job_revision<c.previous_job_revision) THEN
            RAISE EXCEPTION 'continuation must start at the original exact queued revision' USING ERRCODE='23514';
        END IF;
    ELSE
        SELECT * INTO STRICT prior FROM control.acquisition_continuations WHERE continuation_id=c.previous_continuation;
        IF prior.job_id<>c.job_id OR prior.dataset_id<>c.dataset_id OR prior.adopted_at_us IS NULL
            OR prior.adopted_job_revision<>c.previous_job_revision OR prior.target_epoch<>c.producer_epoch
            OR prior.producer_slot_id<>c.producer_slot_id OR prior.target_manifest_sha256<>c.manifest_sha256
            OR prior.last_quantum_number>c.last_quantum_number
            OR (prior.adopting_resource,prior.adopting_epoch,prior.adopting_incarnation) IS DISTINCT FROM
                (c.previous_owner_resource,c.previous_owner_epoch,c.previous_owner_incarnation)
            OR (prior.scope_id,prior.enrollment_generation,prior.route_id) IS DISTINCT FROM
                (c.scope_id,c.enrollment_generation,c.route_id) THEN
            RAISE EXCEPTION 'continuation chain differs from the exact adopted segment' USING ERRCODE='23514';
        END IF;
    END IF;
    IF c.adopted_at_us IS NULL THEN
        IF j.job_revision<>c.previous_job_revision OR (j.owner_resource_id,j.owner_epoch,j.owner_incarnation) IS DISTINCT FROM
                (c.previous_owner_resource,c.previous_owner_epoch,c.previous_owner_incarnation) THEN
            RAISE EXCEPTION 'sealed continuation cannot change its excluded queued job' USING ERRCODE='23514';
        END IF;
    ELSE
        PERFORM control.require_control_owner(c.dataset_id,c.adopting_resource,c.adopting_epoch,c.adopting_incarnation);
        IF j.job_revision<>c.adopted_job_revision OR (j.owner_resource_id,j.owner_epoch,j.owner_incarnation) IS DISTINCT FROM
                (c.adopting_resource,c.adopting_epoch,c.adopting_incarnation)
            OR c.adoption_transaction<>pg_current_xact_id() THEN
            RAISE EXCEPTION 'adoption and job owner CAS must commit together' USING ERRCODE='23514';
        END IF;
        SELECT * INTO STRICT target FROM control.ingest_lineage_activation(p.owner_resource_id,c.target_epoch);
        IF (target.resource,target.epoch,target.incarnation) IS DISTINCT FROM
                (c.adopting_resource,c.adopting_epoch,c.adopting_incarnation)
            OR sha256(control.ingest_lineage_manifest_at(p.owner_resource_id,c.target_epoch))<>c.target_manifest_sha256
            OR sha256(control.ingest_lineage_manifest_at(p.owner_resource_id,c.producer_epoch))<>c.manifest_sha256
            OR sha256(control.ingest_lineage_handoff_at(p.owner_resource_id,c.target_epoch))<>c.activation_sha256
            OR NOT EXISTS(SELECT 1 FROM control.producer_epochs WHERE producer_slot_id=c.producer_slot_id
                AND producer_epoch=c.target_epoch AND state='open') THEN
            RAISE EXCEPTION 'adoption requires the exact positively activated descendant' USING ERRCODE='23514';
        END IF;
    END IF;
    PERFORM control.require_relay_source(c.dataset_id,
        (SELECT endpoint_id FROM control.acquisition_scopes WHERE dataset_id=c.dataset_id AND scope_id=c.scope_id),
        (SELECT access_scope_id FROM control.acquisition_scopes WHERE dataset_id=c.dataset_id AND scope_id=c.scope_id),
        c.enrollment_generation,c.route_id);
END $$;

CREATE FUNCTION control.guard_acquisition_continuation() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
    IF TG_OP='DELETE' OR (TG_OP='UPDATE' AND (OLD.adopted_at_us IS NOT NULL OR NEW.adopted_at_us IS NULL
        OR (to_jsonb(NEW)-ARRAY['target_epoch','target_manifest_sha256','activation_sha256','adopting_resource','adopting_epoch',
            'adopting_incarnation','adopted_job_revision','adopted_at_us','adoption_transaction']) IS DISTINCT FROM
           (to_jsonb(OLD)-ARRAY['target_epoch','target_manifest_sha256','activation_sha256','adopting_resource','adopting_epoch',
            'adopting_incarnation','adopted_job_revision','adopted_at_us','adoption_transaction']))) THEN
        RAISE EXCEPTION 'continuation seal is immutable and adoption is write once' USING ERRCODE='23514';
    END IF;
    -- Job CAS follows this UPDATE, and is checked by both deferred triggers.
    IF TG_OP='INSERT' THEN
        PERFORM control.require_control_owner(NEW.dataset_id,NEW.sealing_resource,NEW.sealing_epoch,NEW.sealing_incarnation);
        IF NEW.adopted_at_us IS NOT NULL OR NEW.last_quantum_number<>(SELECT coalesce(max(quantum_number),0)
            FROM control.acquisition_quanta WHERE job_id=NEW.job_id) THEN
            RAISE EXCEPTION 'seal must retain the complete existing settled cut' USING ERRCODE='23514';
        END IF;
        PERFORM control.require_acquisition_continuation(NEW);
    ELSE
        PERFORM control.require_control_owner(NEW.dataset_id,NEW.adopting_resource,NEW.adopting_epoch,NEW.adopting_incarnation);
        PERFORM control.require_acquisition_continuation_authority(NEW.dataset_id,NEW.job_id);
    END IF;
    RETURN NEW;
END $$;
CREATE FUNCTION control.require_acquisition_continuation_complete() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE c control.acquisition_continuations%ROWTYPE;
BEGIN
    SELECT * INTO STRICT c FROM control.acquisition_continuations WHERE continuation_id=NEW.continuation_id;
    PERFORM control.require_acquisition_continuation(c);
    IF NOT control.acquisition_continuation_covers(c.job_id,c.producer_slot_id,c.producer_epoch) THEN
        RAISE EXCEPTION 'continuation does not cover its complete original workflow and checkpoint' USING ERRCODE='23514';
    END IF;
    PERFORM control.require_producer_progress_capacity(c.producer_slot_id,c.producer_epoch,0,
        coalesce((SELECT progress_jobs FROM control.producer_checkpoints WHERE producer_slot_id=c.producer_slot_id AND producer_epoch=c.producer_epoch),0),
        coalesce((SELECT dependency_count FROM control.producer_checkpoints WHERE producer_slot_id=c.producer_slot_id AND producer_epoch=c.producer_epoch),0));
    IF c.target_epoch IS NOT NULL THEN
        PERFORM control.require_acquisition_workflow_capacity(c.producer_slot_id,c.target_epoch);
        PERFORM control.require_producer_progress_capacity(c.producer_slot_id,c.target_epoch,0,
            coalesce((SELECT progress_jobs FROM control.producer_checkpoints WHERE producer_slot_id=c.producer_slot_id AND producer_epoch=c.target_epoch),0),
            coalesce((SELECT dependency_count FROM control.producer_checkpoints WHERE producer_slot_id=c.producer_slot_id AND producer_epoch=c.target_epoch),0));
    END IF;
    RETURN NULL;
END $$;
CREATE TRIGGER acquisition_continuation_guard BEFORE INSERT OR UPDATE OR DELETE ON control.acquisition_continuations
    FOR EACH ROW EXECUTE FUNCTION control.guard_acquisition_continuation();
CREATE CONSTRAINT TRIGGER zzz_acquisition_continuation_complete AFTER INSERT OR UPDATE ON control.acquisition_continuations
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION control.require_acquisition_continuation_complete();
CREATE TRIGGER acquisition_continuations_no_truncate BEFORE TRUNCATE ON control.acquisition_continuations
    FOR EACH STATEMENT EXECUTE FUNCTION control.reject_history_mutation();

-- A finalized exact adoption is the only additional same-owner claim shape.
CREATE FUNCTION control.acquisition_adopted_claim(wanted_job bytea,wanted_revision numeric)
RETURNS boolean LANGUAGE sql STABLE STRICT SET search_path=pg_catalog AS $$
    SELECT EXISTS(SELECT 1 FROM control.acquisition_continuations c JOIN control.jobs j USING(job_id)
        WHERE c.job_id=wanted_job AND c.adopted_job_revision=wanted_revision AND j.job_revision=wanted_revision
            AND (c.adopting_resource,c.adopting_epoch,c.adopting_incarnation)=
                (j.owner_resource_id,j.owner_epoch,j.owner_incarnation) AND c.adopted_at_us IS NOT NULL)
$$;

CREATE FUNCTION control.acquisition_quantum_historical_context(
    wanted_job bytea,wanted_number numeric,wanted_slot bytea,wanted_epoch numeric,wanted_manifest bytea,
    old_slot bytea,old_epoch numeric,old_manifest bytea,old_resource bytea,old_control_epoch numeric,old_incarnation bytea)
RETURNS boolean LANGUAGE sql STABLE STRICT SET search_path=pg_catalog AS $$
    WITH RECURSIVE chain AS (
        SELECT c.* FROM control.acquisition_continuations c JOIN control.jobs j USING(job_id)
        WHERE c.job_id=wanted_job AND c.adopted_job_revision=j.job_revision
            AND c.target_epoch=wanted_epoch AND c.producer_slot_id=wanted_slot AND c.target_manifest_sha256=wanted_manifest
            AND (c.adopting_resource,c.adopting_epoch,c.adopting_incarnation)=
                (j.owner_resource_id,j.owner_epoch,j.owner_incarnation)
        UNION ALL SELECT p.* FROM control.acquisition_continuations p JOIN chain c ON c.previous_continuation=p.continuation_id
    ) SELECT EXISTS(SELECT 1 FROM chain c WHERE c.producer_slot_id=old_slot AND c.producer_epoch=old_epoch
        AND c.manifest_sha256=old_manifest AND (c.previous_owner_resource,c.previous_owner_epoch,c.previous_owner_incarnation)=
            (old_resource,old_control_epoch,old_incarnation)
        AND wanted_number<=c.last_quantum_number AND wanted_number>coalesce((SELECT last_quantum_number
            FROM control.acquisition_continuations WHERE continuation_id=c.previous_continuation),0))
$$;

-- Preserve the complete existing retirement proofs, adding only the sealed
-- continuation alternative for the exact job and original producer segment.
CREATE OR REPLACE FUNCTION control.guard_acquisition_claim_owner() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    IF (NEW.owner_resource_id, NEW.owner_epoch, NEW.owner_incarnation)
        IS DISTINCT FROM (OLD.owner_resource_id, OLD.owner_epoch, OLD.owner_incarnation) THEN
        IF OLD.state='queued' AND NEW.state='queued' AND OLD.checkpoint_revision=0 AND NEW.checkpoint_revision=0
            AND NEW.job_revision=OLD.job_revision+1 AND EXISTS(
                SELECT 1 FROM control.acquisition_continuations c WHERE c.job_id=OLD.job_id
                    AND c.previous_job_revision=OLD.job_revision AND c.adopted_job_revision=NEW.job_revision
                    AND c.adoption_transaction=pg_current_xact_id()
                    AND (c.previous_owner_resource,c.previous_owner_epoch,c.previous_owner_incarnation)=
                        (OLD.owner_resource_id,OLD.owner_epoch,OLD.owner_incarnation)
                    AND (c.adopting_resource,c.adopting_epoch,c.adopting_incarnation)=
                        (NEW.owner_resource_id,NEW.owner_epoch,NEW.owner_incarnation)) THEN
            RETURN NEW;
        END IF;
        IF OLD.state <> 'queued' OR NEW.state <> 'cancel_requested'
            OR OLD.checkpoint_revision <> 0 OR NEW.checkpoint_revision <> 0
            OR NEW.job_revision <> OLD.job_revision + 1
            OR NEW.reason_code NOT IN ('initial_producer_abandoned', 'activated_initial_producer_abandoned', 'ingest_successor_abandoned')
            OR NEW.reason_code IS NULL
            OR NOT EXISTS (SELECT 1 FROM control.acquisition_gaps g
                WHERE g.job_id = OLD.job_id AND g.dataset_id = OLD.dataset_id
                    AND g.reason = NEW.reason_code
                    AND ((g.producer_epoch = 1 AND g.source_owner_epoch = 1 AND g.reason = 'initial_producer_abandoned')
                        OR (g.producer_epoch = 2 AND g.source_owner_epoch = 2 AND g.reason = 'activated_initial_producer_abandoned')
                        OR (g.producer_epoch BETWEEN 3 AND 255 AND g.source_owner_epoch=g.producer_epoch AND g.reason='ingest_successor_abandoned'))
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

CREATE OR REPLACE FUNCTION control.require_initial_retirement(
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
        OR EXISTS (SELECT 1 FROM control.producer_input_bindings
            WHERE producer_slot_id = wanted_slot AND producer_epoch = wanted_epoch) THEN
        RAISE EXCEPTION 'unsupported initial producer retirement history' USING ERRCODE = '23514';
    END IF;
    PERFORM control.require_retained_producer_progress(wanted_slot, wanted_epoch);
    PERFORM control.require_acquisition_workflow_capacity(wanted_slot,wanted_epoch);
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
                    OR (r.acquisition_job_id IS NOT NULL AND NOT control.acquisition_continuation_covers(r.acquisition_job_id,wanted_slot,wanted_epoch) AND NOT EXISTS (
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
            AND NOT control.acquisition_continuation_covers(job_id,wanted_slot,wanted_epoch)
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

CREATE OR REPLACE FUNCTION control.lock_activated_initial_retirement(wanted_resource bytea)
RETURNS control.initial_ingest_successions LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE s control.initial_ingest_successions%ROWTYPE;
        a control.initial_ingest_activations%ROWTYPE;
        current_owner control.owner_epochs%ROWTYPE;
        old_owner control.owner_epochs%ROWTYPE;
        old_producer control.producer_epochs%ROWTYPE;
BEGIN
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'activated recovery requires READ COMMITTED' USING ERRCODE = '25001';
    END IF;
    SELECT * INTO STRICT s FROM control.initial_ingest_successions
        WHERE owner_resource_id = wanted_resource;
    SELECT * INTO STRICT a FROM control.initial_ingest_activations WHERE owner_resource_id = wanted_resource;
    IF octet_length(s.handoff) > 610 OR s.previous_topology <> 0 OR s.target_topology <> 1
        OR a.activation_transaction = pg_current_xact_id()
        OR ROW(a.dataset_id,a.target_epoch,a.target_incarnation,a.target_role_instance_id,
            a.producer_slot_id,a.producer_epoch,a.previous_manifest_sha256,a.target_manifest_sha256,a.handoff_sha256,
            a.prepared_by_resource,a.prepared_by_epoch,a.prepared_by_incarnation,
            a.activated_by_resource,a.activated_by_epoch,a.activated_by_incarnation,
            a.config_revision,a.policy_revision,a.config_digest,a.policy_digest)
        IS DISTINCT FROM ROW(s.dataset_id,s.target_epoch,s.target_incarnation,s.target_role_instance_id,
            s.producer_slot_id,s.producer_epoch,s.previous_manifest_sha256,s.target_manifest_sha256,s.handoff_sha256,
            s.prepared_by_resource,s.prepared_by_epoch,s.prepared_by_incarnation,
            s.prepared_by_resource,s.prepared_by_epoch,s.prepared_by_incarnation,
            s.config_revision,s.policy_revision,s.config_digest,s.policy_digest)
        OR a.activated_at_us < s.created_at_us THEN
        RAISE EXCEPTION 'activated recovery requires exact committed preparation and activation' USING ERRCODE = '23514';
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
        OR current_owner.epoch <= s.prepared_by_epoch
        OR NOT EXISTS (SELECT 1 FROM control.owner_epochs o JOIN control.role_instances r
            ON r.role_instance_id = o.role_instance_id
            WHERE o.dataset_id = s.dataset_id AND o.resource_id = s.prepared_by_resource
                AND o.epoch = s.prepared_by_epoch AND o.process_incarnation = s.prepared_by_incarnation
                AND o.role = 'control' AND r.dataset_id = s.dataset_id AND r.role = 'control'
                AND o.control_fence_version = 1
                AND o.epoch < current_owner.epoch AND o.state = 'released'
                AND o.exclusion_method = 'storage_fenced' AND o.exclusion_digest IS NOT NULL
                AND r.retired_at_us IS NOT NULL)
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
        RAISE EXCEPTION 'activated recovery requires exact preparation and current Control policy'
            USING ERRCODE = '23514';
    END IF;

    -- Old Ingest scheduling expiry is irrelevant. Lock the historical registry, followed
    -- by the target parents. All domain work is bounded by the nine known kinds.
    SELECT * INTO STRICT old_owner FROM control.owner_epochs
        WHERE resource_id = s.owner_resource_id AND epoch = 1 FOR UPDATE;
    PERFORM 1 FROM control.role_instances WHERE role_instance_id = old_owner.role_instance_id FOR UPDATE;
    PERFORM 1 FROM control.allocation_domains
        WHERE dataset_id = s.dataset_id AND owner_resource_id = s.owner_resource_id AND owner_epoch = 1
        ORDER BY dictionary_kind, allocation_domain LIMIT 10 FOR UPDATE;
    IF (SELECT count(*) FROM (SELECT 1 FROM control.allocation_domains
        WHERE dataset_id = s.dataset_id AND owner_resource_id = s.owner_resource_id AND owner_epoch = 1
        LIMIT 10) AS bounded) <> 9 THEN
        RAISE EXCEPTION 'initial predecessor domain set is incomplete or exceeds its bound' USING ERRCODE = '23514';
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
        RAISE EXCEPTION 'activated recovery requires the exact retired initial predecessor'
            USING ERRCODE = '23514';
    END IF;

    PERFORM 1 FROM control.owner_epochs WHERE resource_id = s.owner_resource_id AND epoch = 2 FOR UPDATE;
    PERFORM 1 FROM control.role_instances WHERE role_instance_id = s.target_role_instance_id FOR UPDATE;
    PERFORM 1 FROM control.allocation_domains
        WHERE dataset_id = s.dataset_id AND owner_resource_id = s.owner_resource_id AND owner_epoch = 2
        ORDER BY dictionary_kind, allocation_domain LIMIT 10 FOR UPDATE;
    IF (SELECT count(*) FROM (SELECT 1 FROM control.allocation_domains
        WHERE dataset_id = s.dataset_id AND owner_resource_id = s.owner_resource_id AND owner_epoch = 2
        LIMIT 10) AS bounded) <> 9 THEN
        RAISE EXCEPTION 'activated recovery target domain set is incomplete or exceeds its bound' USING ERRCODE = '23514';
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
                AND o.state IN ('active','draining','released')
                AND r.dataset_id = s.dataset_id AND r.role = 'ingest' AND r.created_at_us = s.created_at_us
                )
        OR NOT EXISTS (SELECT 1 FROM control.producer_epochs
            WHERE producer_slot_id = s.producer_slot_id AND producer_epoch = 2 AND dataset_id = s.dataset_id
                AND producer_role_instance_id = s.target_role_instance_id AND producer_role = 'ingest'
                AND producer_incarnation = s.target_incarnation AND owner_resource_id = s.owner_resource_id
                AND owner_epoch = 2 AND owner_role = 'ingest' AND owner_incarnation = s.target_incarnation
                AND state IN ('closing', 'retired') AND opened_at_us = s.created_at_us
                AND outstanding_request_limit = s.outstanding_request_limit
                AND reserved_through_request <= 64)
        OR NOT EXISTS (SELECT 1 FROM control.source_streams
            WHERE dataset_id = s.dataset_id AND shard_id = s.shard_id AND owner_epoch = 2
                AND owner_resource_id = s.owner_resource_id AND owner_role = 'ingest'
                AND owner_incarnation = s.target_incarnation AND state = 'closed' AND opened_at_us = s.created_at_us)
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
        RAISE EXCEPTION 'activated recovery target registry identity differs' USING ERRCODE = '23514';
    END IF;
    -- The typed caller also compares all eighteen domain IDs with both decoded
    -- manifests. SQL metadata never substitutes for the native closing proof.
    -- Positive unique sequences with count=max=reserved form the exact retained
    -- prefix, including zero. The typed source owner separately requires native
    -- issued=reserved; SQL metadata cannot establish an unissued native tail.
    IF NOT EXISTS (SELECT 1 FROM control.producer_epochs p CROSS JOIN LATERAL (
            SELECT count(*) AS retained, coalesce(max(r.request_sequence), 0) AS last_sequence,
                coalesce(bool_and(r.dataset_id = s.dataset_id AND r.manifest_sha256 = s.target_manifest_sha256
                    AND r.issuer_resource = a.activated_by_resource AND r.issuer_epoch = a.activated_by_epoch
                    AND r.issuer_incarnation = a.activated_by_incarnation), true) AS exact_identity
            FROM control.producer_requests r WHERE r.producer_slot_id = s.producer_slot_id AND r.producer_epoch = 2
        ) requests WHERE p.producer_slot_id = s.producer_slot_id AND p.producer_epoch = 2
            AND requests.retained = p.reserved_through_request
            AND requests.last_sequence = p.reserved_through_request AND requests.exact_identity)
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
        RAISE EXCEPTION 'activated recovery requires original target reservations and exact sealed topology'
            USING ERRCODE = '23514';
    END IF;
    -- Local classified progress does not complete or abandon workflow claims.
    -- Retirement still requires exact copies and no unresolved import obligation.
    PERFORM control.require_retained_producer_progress(s.producer_slot_id, 2);
    PERFORM control.require_acquisition_workflow_capacity(s.producer_slot_id,2);
    IF EXISTS (SELECT 1 FROM control.producer_input_bindings
            WHERE producer_slot_id=s.producer_slot_id AND producer_epoch=2)
        OR EXISTS (SELECT 1 FROM control.producer_requests r
            LEFT JOIN control.producer_outcomes o USING(producer_slot_id,producer_epoch,request_sequence)
            WHERE r.producer_slot_id=s.producer_slot_id AND r.producer_epoch=2
                AND (o.request_sequence IS NULL OR o.dataset_id<>r.dataset_id
                    OR o.request_digest<>r.request_digest OR o.manifest_sha256<>r.manifest_sha256
                    OR (r.relay_endpoint_id IS NOT NULL AND r.acquisition_job_id IS NULL)
                    OR (r.acquisition_job_id IS NOT NULL AND NOT control.acquisition_continuation_covers(r.acquisition_job_id,s.producer_slot_id,2) AND NOT EXISTS (
                        SELECT 1 FROM control.acquisition_gaps g WHERE g.job_id=r.acquisition_job_id
                            AND g.producer_slot_id=s.producer_slot_id AND g.producer_epoch=2)))) THEN
        RAISE EXCEPTION 'activated retirement requires exact copies and supported workflow obligations'
            USING ERRCODE='23514';
    END IF;
    -- An owned claim must already have been abandoned by the separate operation.
    -- Both partial owner indexes avoid scanning unrelated job histories. Actual
    -- job custody follows the registry/source/producer parents, in job-ID order.
    PERFORM job_id FROM control.jobs
        WHERE dataset_id=s.dataset_id AND owner_resource_id=a.activated_by_resource
            AND owner_epoch=a.activated_by_epoch AND owner_incarnation=a.activated_by_incarnation
            AND kind='acquisition' AND NOT control.acquisition_continuation_covers(job_id,s.producer_slot_id,2) ORDER BY job_id LIMIT 1 FOR UPDATE;
    IF FOUND THEN
        RAISE EXCEPTION 'activated producer workflow claims remain unresolved' USING ERRCODE='23514';
    END IF;
    PERFORM job_id FROM control.jobs
        WHERE dataset_id=s.dataset_id AND owner_resource_id=a.activated_by_resource
            AND owner_epoch=a.activated_by_epoch AND owner_incarnation=a.activated_by_incarnation
            AND kind='import' ORDER BY job_id LIMIT 1 FOR UPDATE;
    IF FOUND THEN
        RAISE EXCEPTION 'activated producer import claims remain unresolved' USING ERRCODE='23514';
    END IF;
    IF (SELECT count(*) FROM (SELECT 1 FROM control.acquisition_gaps
        WHERE producer_slot_id=s.producer_slot_id AND producer_epoch=2 LIMIT 65) bounded)>64 THEN
        RAISE EXCEPTION 'activated retirement acquisition gap bound exceeded' USING ERRCODE='23514';
    END IF;
    PERFORM j.job_id FROM control.acquisition_gaps g JOIN control.jobs j USING(job_id)
        WHERE g.producer_slot_id=s.producer_slot_id AND g.producer_epoch=2
        ORDER BY j.job_id LIMIT 65 FOR UPDATE OF j;
    -- These immutable gaps have historical issuers. Never rerun their insertion
    -- guard or require those issuers to remain live in this fresh transaction.
    IF EXISTS (SELECT 1 FROM control.acquisition_gaps g LEFT JOIN control.jobs j USING(job_id)
        LEFT JOIN control.source_streams stream ON stream.dataset_id=g.dataset_id
            AND stream.shard_id=g.shard_id AND stream.owner_epoch=g.source_owner_epoch
        WHERE g.producer_slot_id=s.producer_slot_id AND g.producer_epoch=2
            AND NOT coalesce(g.dataset_id=s.dataset_id AND g.shard_id=s.shard_id AND g.source_owner_epoch=2
                AND g.reason='activated_initial_producer_abandoned' AND g.owner_role='control'
                AND g.last_complete_sequence=stream.last_complete_sequence AND g.source_closure_digest=stream.closure_digest
                AND g.previous_owner_resource=a.activated_by_resource AND g.previous_owner_epoch=a.activated_by_epoch
                AND g.previous_owner_incarnation=a.activated_by_incarnation AND g.issuer_resource=a.activated_by_resource
                AND g.issuer_epoch>a.activated_by_epoch AND g.issuer_epoch<=current_owner.epoch
                AND g.abandoned_at_us>=stream.closed_at_us AND g.abandoned_at_us>=j.created_at_us
                AND g.abandoned_at_us<=(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint
                AND j.dataset_id=g.dataset_id AND j.kind='acquisition' AND j.acquisition_job_protocol=1
                AND j.input_complete AND j.state='cancelled' AND j.job_revision=g.previous_job_revision+2
                AND j.checkpoint_revision=0 AND j.result_manifest_digest IS NULL
                AND j.reason_code=g.reason AND j.completed_at_us=g.abandoned_at_us
                AND (j.owner_resource_id,j.owner_epoch,j.owner_incarnation)
                    IS NOT DISTINCT FROM (g.issuer_resource,g.issuer_epoch,g.issuer_incarnation), false)) THEN
        RAISE EXCEPTION 'activated retirement requires complete retained acquisition gaps' USING ERRCODE='23514';
    END IF;
    -- Parent/job locks can wait: sample current actual authority again afterwards.
    PERFORM control.require_control_owner(s.dataset_id, current_owner.resource_id,
        current_owner.epoch, current_owner.process_incarnation);
    IF current_owner.lease_expires_at_us <= (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint THEN
        RAISE EXCEPTION 'activated retirement Control authority expired' USING ERRCODE='23514';
    END IF;
    RETURN s;
END $$;

CREATE OR REPLACE FUNCTION control.require_ingest_lineage_workflows(wanted_resource bytea,wanted_epoch numeric,current_epoch numeric)
RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE p control.producer_epochs%ROWTYPE; stream control.source_streams%ROWTYPE;
        original record; wanted_reason text; gap_count bigint;
BEGIN
    SELECT * INTO STRICT p FROM control.producer_epochs WHERE owner_resource_id=wanted_resource AND owner_epoch=wanted_epoch;
    SELECT * INTO STRICT stream FROM control.source_streams WHERE owner_resource_id=wanted_resource AND owner_epoch=wanted_epoch;
    SELECT * INTO STRICT original FROM control.ingest_lineage_activation(wanted_resource,wanted_epoch);
    wanted_reason:=CASE WHEN wanted_epoch=1 THEN 'initial_producer_abandoned'
        WHEN wanted_epoch=2 THEN 'activated_initial_producer_abandoned' ELSE 'ingest_successor_abandoned' END;
    PERFORM control.require_retained_producer_progress(p.producer_slot_id, wanted_epoch);
    PERFORM control.require_acquisition_workflow_capacity(p.producer_slot_id,wanted_epoch);
    IF EXISTS(SELECT 1 FROM control.producer_input_bindings WHERE producer_slot_id=p.producer_slot_id AND producer_epoch=wanted_epoch)
        OR EXISTS(SELECT 1 FROM control.producer_requests r
            WHERE r.producer_slot_id=p.producer_slot_id AND r.producer_epoch=wanted_epoch
                AND ((r.relay_endpoint_id IS NOT NULL AND r.acquisition_job_id IS NULL)
                    OR (r.acquisition_job_id IS NOT NULL AND NOT control.acquisition_continuation_covers(r.acquisition_job_id,p.producer_slot_id,wanted_epoch) AND NOT EXISTS(SELECT 1 FROM control.acquisition_gaps g
                        WHERE g.job_id=r.acquisition_job_id AND g.producer_slot_id=p.producer_slot_id AND g.producer_epoch=wanted_epoch)))) THEN
        RAISE EXCEPTION 'lineage retirement has unsupported workflow obligations' USING ERRCODE='23514';
    END IF;
    PERFORM job_id FROM control.jobs WHERE dataset_id=p.dataset_id AND kind='acquisition'
        AND NOT control.acquisition_continuation_covers(job_id,p.producer_slot_id,wanted_epoch)
        AND owner_resource_id=original.resource AND owner_epoch=original.epoch AND owner_incarnation=original.incarnation
        ORDER BY job_id LIMIT 1 FOR UPDATE;
    IF FOUND THEN RAISE EXCEPTION 'lineage acquisition claims remain unresolved' USING ERRCODE='23514'; END IF;
    PERFORM job_id FROM control.jobs WHERE dataset_id=p.dataset_id AND kind='import'
        AND owner_resource_id=original.resource AND owner_epoch=original.epoch AND owner_incarnation=original.incarnation
        ORDER BY job_id LIMIT 1 FOR UPDATE;
    IF FOUND THEN RAISE EXCEPTION 'lineage import claims remain unresolved' USING ERRCODE='23514'; END IF;
    SELECT count(*) INTO gap_count FROM (SELECT 1 FROM control.acquisition_gaps
        WHERE producer_slot_id=p.producer_slot_id AND producer_epoch=wanted_epoch LIMIT 65) bounded;
    IF gap_count>64 THEN RAISE EXCEPTION 'lineage gap bound exceeded' USING ERRCODE='23514'; END IF;
    PERFORM j.job_id FROM control.acquisition_gaps g JOIN control.jobs j USING(job_id)
        WHERE g.producer_slot_id=p.producer_slot_id AND g.producer_epoch=wanted_epoch
        ORDER BY j.job_id LIMIT 65 FOR UPDATE OF j;
    IF EXISTS(SELECT 1 FROM control.acquisition_gaps g LEFT JOIN control.jobs j USING(job_id)
        WHERE g.producer_slot_id=p.producer_slot_id AND g.producer_epoch=wanted_epoch
            AND NOT coalesce(g.dataset_id=p.dataset_id AND g.shard_id=stream.shard_id AND g.source_owner_epoch=wanted_epoch
                AND g.reason=wanted_reason AND g.owner_role='control' AND g.last_complete_sequence=stream.last_complete_sequence
                AND g.source_closure_digest=stream.closure_digest
                AND ROW(g.previous_owner_resource,g.previous_owner_epoch,g.previous_owner_incarnation)
                    IS NOT DISTINCT FROM ROW(original.resource,original.epoch,original.incarnation)
                AND g.issuer_resource=original.resource AND g.issuer_epoch>original.epoch AND g.issuer_epoch<=current_epoch
                AND g.abandoned_at_us>=stream.closed_at_us AND g.abandoned_at_us>=j.created_at_us
                AND g.abandoned_at_us<=(extract(epoch FROM clock_timestamp())*1000000)::bigint
                AND j.dataset_id=g.dataset_id AND j.kind='acquisition' AND j.acquisition_job_protocol=1 AND j.input_complete
                AND j.state='cancelled' AND j.job_revision=g.previous_job_revision+2 AND j.checkpoint_revision=0
                AND j.result_manifest_digest IS NULL AND j.reason_code=g.reason AND j.completed_at_us=g.abandoned_at_us
                AND ROW(j.owner_resource_id,j.owner_epoch,j.owner_incarnation)
                    IS NOT DISTINCT FROM ROW(g.issuer_resource,g.issuer_epoch,g.issuer_incarnation),false)) THEN
        RAISE EXCEPTION 'lineage retirement requires exact retained gaps' USING ERRCODE='23514';
    END IF;
END $$;

REVOKE ALL ON control.acquisition_continuations FROM PUBLIC;
REVOKE ALL ON FUNCTION control.require_acquisition_continuation_authority(bytea,bytea),
    control.require_acquisition_workflow_capacity(bytea,numeric),
    control.acquisition_continuation_covers(bytea,bytea,numeric),
    control.require_acquisition_continuation(control.acquisition_continuations),
    control.guard_acquisition_continuation(),control.require_acquisition_continuation_complete(),
    control.acquisition_adopted_claim(bytea,numeric),
    control.acquisition_quantum_historical_context(bytea,numeric,bytea,numeric,bytea,bytea,numeric,bytea,bytea,numeric,bytea) FROM PUBLIC;
