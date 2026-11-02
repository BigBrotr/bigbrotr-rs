-- Source fixture; connected execution remains NOT_RUN until separately recorded.
-- Install only in the isolated Monitor qualification DB
-- before its first real dispatch. Recursion skips this hook alone; all product
-- constraints and the actual Control login/backend remain enabled.
CREATE SCHEMA monitor_fixture;
CREATE TABLE monitor_fixture.results(name text PRIMARY KEY, sqlstate text NOT NULL);
CREATE FUNCTION monitor_fixture.dispatch_checks() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE candidate control.measurement_dispatches; mode text; code text; expected text; count_before bigint;
BEGIN
    IF pg_trigger_depth()>1 OR EXISTS(SELECT 1 FROM monitor_fixture.results WHERE name='valid-before') THEN RETURN NEW; END IF;
    IF current_user<>'bigbrotr_control' OR session_user<>'bigbrotr_control' THEN
        RAISE EXCEPTION 'monitor fixture requires actual Control login';
    END IF;
    FOREACH mode IN ARRAY ARRAY['valid-before','foreign-context','wrong-kind','zero-domain','missing-byte',
        'profile','ceiling','owner','expired','pending-other-id','closed-permits-new','refusal-immutable','immutable-owner','immutable-context','delete-intent','valid-after'] LOOP
        candidate:=NEW; expected:='23514';
        SELECT count(*) INTO count_before FROM control.measurement_dispatches;
        IF mode LIKE 'valid-%' OR mode='closed-permits-new' THEN expected:='ZX001';
        ELSIF mode='pending-other-id' THEN expected:='BB001';
        ELSIF mode='foreign-context' THEN candidate.context_bytes:=set_byte(candidate.context_bytes,0,128);
        ELSIF mode='wrong-kind' THEN candidate.context_bytes:=set_byte(candidate.context_bytes,17,8);
        ELSIF mode='zero-domain' THEN candidate.context_bytes:=overlay(candidate.context_bytes placing decode(repeat('00',16),'hex') from 19 for 16);
        ELSIF mode='missing-byte' THEN candidate.context_bytes:=substring(candidate.context_bytes from 1 for 293);
        ELSIF mode='profile' THEN candidate.profile_version:=2;
        ELSIF mode='ceiling' THEN candidate.authority_ceiling_us:=5000001;
        ELSIF mode='owner' THEN candidate.issuer_epoch:=candidate.issuer_epoch+1;
        ELSIF mode='expired' THEN candidate.created_at_us:=0;candidate.dispatch_deadline_us:=1;expected:='BB001';
        END IF;
        BEGIN
            INSERT INTO control.measurement_dispatches SELECT (candidate).*;
            IF mode IN ('closed-permits-new','refusal-immutable') THEN
                INSERT INTO control.measurement_dispatch_refusals(dataset_id,attempt_id,reason,issuer_resource,issuer_epoch,issuer_incarnation,recorded_at_us)
                    VALUES(candidate.dataset_id,candidate.attempt_id,1,candidate.issuer_resource,candidate.issuer_epoch,candidate.issuer_incarnation,
                        (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint);
            END IF;
            IF mode IN ('pending-other-id','closed-permits-new') THEN
                candidate.attempt_id:=set_byte(candidate.attempt_id,0,get_byte(candidate.attempt_id,0)#128);
                INSERT INTO control.measurement_dispatches SELECT (candidate).*;
            ELSIF mode='refusal-immutable' THEN
                UPDATE control.measurement_dispatch_refusals SET reason=2 WHERE dataset_id=NEW.dataset_id AND attempt_id=NEW.attempt_id;
            ELSIF mode='immutable-owner' THEN
                UPDATE control.measurement_dispatches SET issuer_epoch=issuer_epoch+1 WHERE dataset_id=NEW.dataset_id AND attempt_id=NEW.attempt_id;
            ELSIF mode='immutable-context' THEN
                UPDATE control.measurement_dispatches SET context_bytes=set_byte(context_bytes,0,128) WHERE dataset_id=NEW.dataset_id AND attempt_id=NEW.attempt_id;
            ELSIF mode='delete-intent' THEN
                DELETE FROM control.measurement_dispatches WHERE dataset_id=NEW.dataset_id AND attempt_id=NEW.attempt_id;
            END IF;
            RAISE EXCEPTION 'positive fixture rollback' USING ERRCODE='ZX001';
        EXCEPTION WHEN OTHERS THEN GET STACKED DIAGNOSTICS code=RETURNED_SQLSTATE; END;
        IF code<>expected OR (SELECT count(*) FROM control.measurement_dispatches)<>count_before THEN
            RAISE EXCEPTION 'monitor dispatch case %: observed %, expected %, retained state changed=%',mode,code,expected,(SELECT count(*) FROM control.measurement_dispatches)<>count_before;
        END IF;
        INSERT INTO monitor_fixture.results VALUES(mode,code);
    END LOOP;
    RETURN NEW;
END $$;
CREATE TRIGGER zz_monitor_fixture BEFORE INSERT ON control.measurement_dispatches
    FOR EACH ROW EXECUTE FUNCTION monitor_fixture.dispatch_checks();
GRANT USAGE ON SCHEMA monitor_fixture TO bigbrotr_control;
GRANT SELECT,INSERT ON monitor_fixture.results TO bigbrotr_control;
GRANT EXECUTE ON FUNCTION monitor_fixture.dispatch_checks() TO bigbrotr_control;
-- This isolated fixture alone may reach the retained-row deletion refusal.
GRANT DELETE ON control.measurement_dispatches TO bigbrotr_control;

CREATE FUNCTION monitor_fixture.evidence_checks() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE candidate control.producer_requests; mode text; code text; expected text; count_before bigint;
BEGIN
    IF pg_trigger_depth()>1 OR EXISTS(SELECT 1 FROM monitor_fixture.results WHERE name='evidence-before') THEN RETURN NEW; END IF;
    IF current_user<>'bigbrotr_control' OR session_user<>'bigbrotr_control' THEN
        RAISE EXCEPTION 'monitor fixture requires actual Control login';
    END IF;
    FOREACH mode IN ARRAY ARRAY['evidence-before','missing-dispatch','unknown-dispatch','mixed-acquisition',
        'changed-generation','changed-route','missing-resolution','closed-evidence-refused','evidence-noio-exclusion','evidence-after'] LOOP
        candidate:=NEW;expected:='23514';
        SELECT count(*) INTO count_before FROM control.producer_requests;
        IF mode IN ('evidence-before','evidence-after') THEN expected:='ZX001';
        ELSIF mode='missing-dispatch' THEN candidate.monitor_attempt_id:=NULL;candidate.monitor_resolution:=NULL;
        ELSIF mode='unknown-dispatch' THEN candidate.monitor_attempt_id:=decode(repeat('fc',16),'hex');
        ELSIF mode='mixed-acquisition' THEN candidate.acquisition_job_id:=decode(repeat('fd',16),'hex');
        ELSIF mode='changed-generation' THEN candidate.relay_enrollment_generation:=candidate.relay_enrollment_generation+1;
        ELSIF mode='changed-route' THEN candidate.relay_route_id:=decode(repeat('fe',16),'hex');
        ELSIF mode='missing-resolution' THEN candidate.monitor_resolution:=NULL;
        END IF;
        BEGIN
            IF mode='closed-evidence-refused' THEN
                INSERT INTO control.measurement_dispatch_refusals(dataset_id,attempt_id,reason,issuer_resource,issuer_epoch,issuer_incarnation,recorded_at_us)
                    VALUES(candidate.dataset_id,candidate.monitor_attempt_id,1,candidate.issuer_resource,candidate.issuer_epoch,candidate.issuer_incarnation,
                        (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint);
            END IF;
            INSERT INTO control.producer_requests SELECT (candidate).*;
            IF mode='evidence-noio-exclusion' THEN
                INSERT INTO control.measurement_dispatch_refusals(dataset_id,attempt_id,reason,issuer_resource,issuer_epoch,issuer_incarnation,recorded_at_us)
                    VALUES(candidate.dataset_id,candidate.monitor_attempt_id,1,candidate.issuer_resource,candidate.issuer_epoch,candidate.issuer_incarnation,
                        (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint);
            END IF;
            RAISE EXCEPTION 'positive fixture rollback' USING ERRCODE='ZX001';
        EXCEPTION WHEN OTHERS THEN GET STACKED DIAGNOSTICS code=RETURNED_SQLSTATE; END;
        IF code<>expected OR (SELECT count(*) FROM control.producer_requests)<>count_before THEN
            RAISE EXCEPTION 'monitor evidence case %: observed %, expected %',mode,code,expected;
        END IF;
        INSERT INTO monitor_fixture.results VALUES(mode,code);
    END LOOP;
    RETURN NEW;
END $$;
CREATE TRIGGER zz_monitor_evidence_fixture BEFORE INSERT ON control.producer_requests
    FOR EACH ROW WHEN(NEW.monitor_attempt_id IS NOT NULL)
    EXECUTE FUNCTION monitor_fixture.evidence_checks();
GRANT EXECUTE ON FUNCTION monitor_fixture.evidence_checks() TO bigbrotr_control;

-- Connected closing-tail/capacity and deferred-authority controls. The harness
-- supplies independently encoded C4 bytes and retained charges before invoking
-- the actual resolver. These rows are fixture inputs, never product authority.
CREATE TABLE monitor_fixture.expected (
    singleton boolean PRIMARY KEY CHECK(singleton), dataset_id bytea NOT NULL,
    producer_slot_id bytea NOT NULL, c4_digest bytea NOT NULL CHECK(octet_length(c4_digest)=32),
    retained_bytes bigint NOT NULL CHECK(retained_bytes>=0)
);
CREATE TABLE monitor_fixture.late_fault (
    singleton boolean PRIMARY KEY CHECK(singleton), mode text,
    invocations integer NOT NULL DEFAULT 0
);
INSERT INTO monitor_fixture.late_fault(singleton) VALUES(true);
CREATE TABLE monitor_fixture.observations(name text PRIMARY KEY, document jsonb NOT NULL);

-- Include xmins and authentic authority rows in every rollback comparison.
CREATE FUNCTION monitor_fixture.snapshot() RETURNS jsonb LANGUAGE sql SET search_path=pg_catalog AS $$
    SELECT jsonb_build_object(
        'requests',(SELECT jsonb_agg(to_jsonb(t)||jsonb_build_object('xmin',xmin::text) ORDER BY request_sequence) FROM control.producer_requests t),
        'outcomes',(SELECT jsonb_agg(to_jsonb(t)||jsonb_build_object('xmin',xmin::text) ORDER BY request_sequence) FROM control.producer_outcomes t),
        'heads',(SELECT jsonb_agg(to_jsonb(t)||jsonb_build_object('xmin',xmin::text) ORDER BY producer_slot_id) FROM control.producer_checkpoints t),
        'dependencies',(SELECT jsonb_agg(to_jsonb(t)||jsonb_build_object('xmin',xmin::text) ORDER BY source_sequence) FROM control.producer_checkpoint_dependencies t),
        'producers',(SELECT jsonb_agg(to_jsonb(t)||jsonb_build_object('xmin',xmin::text) ORDER BY producer_slot_id) FROM control.producer_epochs t),
        'owners',(SELECT jsonb_agg(to_jsonb(t)||jsonb_build_object('xmin',xmin::text) ORDER BY resource_id,epoch) FROM control.owner_epochs t),
        'roles',(SELECT jsonb_agg(to_jsonb(t)||jsonb_build_object('xmin',xmin::text) ORDER BY role_instance_id) FROM control.role_instances t),
        'dispatches',(SELECT jsonb_agg(to_jsonb(t)||jsonb_build_object('xmin',xmin::text) ORDER BY attempt_id) FROM control.measurement_dispatches t))
$$;

-- Explicit post-immediate fault injection, not a valid product transition.
-- Catalog ordering is checked before use. Every product trigger stays enabled.
CREATE FUNCTION monitor_fixture.late_issuer() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE mode text; historical control.owner_epochs%ROWTYPE;
BEGIN
    SELECT f.mode INTO mode FROM monitor_fixture.late_fault f WHERE singleton;
    IF mode IS NULL THEN RETURN NEW; END IF;
    IF pg_trigger_depth()<>2 OR OLD.complete_through_request<>3
        OR NEW.complete_through_request<>4 OR NEW.checkpoint_revision<>4 THEN
        RAISE EXCEPTION 'late issuer fixture reached an unexpected operation';
    END IF;
    UPDATE monitor_fixture.late_fault SET invocations=invocations+1 WHERE singleton;
    IF mode='deferred-historical' THEN
        SELECT * INTO STRICT historical FROM control.owner_epochs
            WHERE dataset_id=NEW.dataset_id AND role='control' AND state='released'
                AND epoch<NEW.issuer_epoch ORDER BY epoch LIMIT 1;
        NEW.issuer_resource:=historical.resource_id;
        NEW.issuer_epoch:=historical.epoch;
        NEW.issuer_incarnation:=historical.process_incarnation;
    ELSIF mode<>'deferred-current' THEN RAISE EXCEPTION 'unrecognized late issuer mode';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER zzzz_monitor_late_issuer BEFORE UPDATE ON control.producer_checkpoints
    FOR EACH ROW EXECUTE FUNCTION monitor_fixture.late_issuer();

CREATE FUNCTION monitor_fixture.closing_checks() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE candidate control.producer_checkpoint_dependencies; e monitor_fixture.expected%ROWTYPE;
    head control.producer_checkpoints%ROWTYPE; current_owner control.owner_epochs%ROWTYPE;
    historical control.owner_epochs%ROWTYPE; saved jsonb; trial text; code text; expected text;
    message text; context text; updated integer; late_count integer; retained_ok boolean;
    stored_ok boolean; jobs integer; deps integer; charge bigint; additional bigint;
BEGIN
    -- Only this fixture recurses; no product constraint or authority is skipped.
    IF pg_trigger_depth()>1 OR NOT EXISTS(SELECT 1 FROM control.producer_epochs
        WHERE producer_slot_id=NEW.producer_slot_id AND producer_epoch=NEW.producer_epoch AND state='closing')
        OR EXISTS(SELECT 1 FROM monitor_fixture.results WHERE name='closing-valid-before') THEN RETURN NEW; END IF;
    IF current_user<>'bigbrotr_control' OR session_user<>'bigbrotr_control'
        OR current_setting('session_replication_role')<>'origin'
        OR EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid IN(
            'control.producer_checkpoints'::regclass,'control.producer_checkpoint_dependencies'::regclass,
            'control.producer_requests'::regclass,'control.producer_outcomes'::regclass,
            'control.measurement_dispatches'::regclass,'control.owner_epochs'::regclass,'control.role_instances'::regclass)
            AND tgenabled<>'O')
        OR EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid='control.producer_checkpoints'::regclass
            AND NOT tgisinternal AND (tgtype & 1)=1 AND (tgtype & 2)=2 AND (tgtype & 16)=16
            AND tgname<>'zzzz_monitor_late_issuer' AND tgname>='zzzz_monitor_late_issuer') THEN
        RAISE EXCEPTION 'closing fixture requires enabled product guards and final invoker hook order';
    END IF;
    INSERT INTO monitor_fixture.observations VALUES('catalog',(SELECT jsonb_agg(jsonb_build_object(
        'table',c.relname,'trigger',t.tgname,'enabled',t.tgenabled,
        'security_definer',p.prosecdef,'type',t.tgtype,'deferrable',t.tgdeferrable,'initially_deferred',t.tginitdeferred)
        ORDER BY c.relname,t.tgname) FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid
        JOIN pg_proc p ON p.oid=t.tgfoid WHERE t.tgrelid IN(
            'control.producer_checkpoints'::regclass,'control.producer_checkpoint_dependencies'::regclass) AND NOT t.tgisinternal));
    SELECT * INTO STRICT e FROM monitor_fixture.expected WHERE singleton;
    SELECT * INTO STRICT head FROM control.producer_checkpoints
        WHERE producer_slot_id=NEW.producer_slot_id AND producer_epoch=NEW.producer_epoch;
    SELECT * INTO STRICT current_owner FROM control.owner_epochs
        WHERE dataset_id=NEW.dataset_id AND role='control' AND state='active' AND control_backend_pid=pg_backend_pid();
    SELECT * INTO STRICT historical FROM control.owner_epochs
        WHERE dataset_id=NEW.dataset_id AND role='control' AND state='released'
            AND epoch<current_owner.epoch ORDER BY epoch LIMIT 1;
    PERFORM control.require_control_owner(NEW.dataset_id,current_owner.resource_id,current_owner.epoch,current_owner.process_incarnation);
    IF (NEW.dataset_id,NEW.producer_slot_id,NEW.producer_epoch,NEW.source_shard,NEW.source_epoch,NEW.source_sequence)
            IS DISTINCT FROM (e.dataset_id,e.producer_slot_id,1::numeric,0::bigint,1::numeric,10::numeric)
        OR head.checkpoint_revision<>3 OR head.complete_through_request<>3 OR head.dependency_count<>9
        OR NOT head.prefix_profile OR head.prefix_active_bytes IS NOT NULL OR head.prefix_prepared_bytes IS NOT NULL
        OR (SELECT count(*) FROM control.producer_checkpoint_dependencies)<>9
        OR (SELECT count(*) FROM control.producer_outcomes)<>4 THEN
        RAISE EXCEPTION 'closing fixture requires exact original C3 plus outcome4 before dependency10';
    END IF;
    saved:=monitor_fixture.snapshot();
    FOREACH trial IN ARRAY ARRAY['closing-valid-before','closing-dataset','closing-slot',
        'closing-epoch','closing-zero-source','closing-zero-sequence','closing-valid-after'] LOOP
        candidate:=NEW; expected:='23514';
        IF trial LIKE 'closing-valid-%' THEN expected:='ZX001';
        ELSIF trial='closing-dataset' THEN candidate.dataset_id:=set_byte(candidate.dataset_id,0,get_byte(candidate.dataset_id,0)#128);
        ELSIF trial='closing-slot' THEN candidate.producer_slot_id:=decode(repeat('fc',16),'hex');expected:='P0002';
        ELSIF trial='closing-epoch' THEN candidate.producer_epoch:=2;expected:='P0002';
        ELSIF trial='closing-zero-source' THEN candidate.source_epoch:=0;
        ELSIF trial='closing-zero-sequence' THEN candidate.source_sequence:=0;
        END IF;
        BEGIN
            INSERT INTO control.producer_checkpoint_dependencies SELECT (candidate).*;
            -- Immediate acceptance only. Outer head is still C3; do not force
            -- deferred closure while the nested dependency is temporarily10.
            RAISE EXCEPTION 'positive fixture rollback' USING ERRCODE='ZX001';
        EXCEPTION WHEN OTHERS THEN GET STACKED DIAGNOSTICS code=RETURNED_SQLSTATE; END;
        IF code<>expected OR monitor_fixture.snapshot()<>saved THEN
            RAISE EXCEPTION 'closing dependency case %: observed %, expected %, or retained state changed',trial,code,expected;
        END IF;
        INSERT INTO monitor_fixture.results VALUES(trial,code);
    END LOOP;

    -- Independent literal boundaries: count has ample bytes; byte pair keeps
    -- the actual jobs/dependencies fixed. No claim of installed exhaustion.
    SELECT count(*) INTO jobs FROM control.acquisition_progress WHERE producer_slot_id=NEW.producer_slot_id AND producer_epoch=1;
    SELECT count(*) INTO deps FROM control.producer_checkpoint_dependencies WHERE producer_slot_id=NEW.producer_slot_id AND producer_epoch=1;
    SELECT coalesce(sum(input_bytes::bigint+65536),0)+control.quantum_retained_charge(NEW.producer_slot_id,1)
        INTO charge FROM control.producer_requests WHERE producer_slot_id=NEW.producer_slot_id AND producer_epoch=1;
    IF charge<>e.retained_bytes OR charge+32768+1024+1024::bigint*jobs+256::bigint*16384>=33554432 THEN
        RAISE EXCEPTION 'independent capacity charge or dependency byte headroom differs';
    END IF;
    FOREACH trial IN ARRAY ARRAY['capacity-deps-16384','capacity-deps-16385','capacity-bytes-exact','capacity-bytes-plus-one'] LOOP
        expected:='ZX001'; additional:=0;
        IF trial LIKE 'capacity-deps-%' THEN
            IF trial='capacity-deps-16384' THEN deps:=16384; ELSE deps:=16385;expected:='23514'; END IF;
        ELSE
            SELECT count(*) INTO deps FROM control.producer_checkpoint_dependencies WHERE producer_slot_id=NEW.producer_slot_id AND producer_epoch=1;
            additional:=33554432-(e.retained_bytes+32768+1024+1024::bigint*jobs+256::bigint*deps);
            IF trial='capacity-bytes-plus-one' THEN additional:=additional+1;expected:='23514'; END IF;
        END IF;
        BEGIN
            PERFORM control.require_producer_progress_capacity(NEW.producer_slot_id,1,additional,jobs,deps);
            RAISE EXCEPTION 'positive fixture rollback' USING ERRCODE='ZX001';
        EXCEPTION WHEN OTHERS THEN GET STACKED DIAGNOSTICS code=RETURNED_SQLSTATE; END;
        IF code<>expected OR monitor_fixture.snapshot()<>saved THEN
            RAISE EXCEPTION 'capacity case %: observed %, expected %, or retained state changed',trial,code,expected;
        END IF;
        INSERT INTO monitor_fixture.results VALUES(trial,code);
    END LOOP;

    FOREACH trial IN ARRAY ARRAY['deferred-historical','deferred-current'] LOOP
        UPDATE monitor_fixture.late_fault f SET mode=trial,invocations=0 WHERE singleton;
        updated:=0;late_count:=0;retained_ok:=false;stored_ok:=false;
        BEGIN
            INSERT INTO control.producer_checkpoint_dependencies SELECT (NEW).*;
            UPDATE control.producer_checkpoints SET checkpoint_revision=4,complete_through_request=4,
                dependency_count=10,checkpoint_digest=e.c4_digest,
                issuer_resource=current_owner.resource_id,issuer_epoch=current_owner.epoch,
                issuer_incarnation=current_owner.process_incarnation,
                committed_at_us=(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint
                WHERE producer_slot_id=NEW.producer_slot_id AND producer_epoch=1;
            GET DIAGNOSTICS updated=ROW_COUNT;
            SELECT invocations INTO late_count FROM monitor_fixture.late_fault WHERE singleton;
            SELECT checkpoint_revision=4 AND complete_through_request=4 AND dependency_count=10
                AND checkpoint_digest=e.c4_digest AND progress_format=1 AND prefix_profile
                AND prefix_active_bytes IS NULL AND prefix_prepared_bytes IS NULL
                AND (issuer_resource,issuer_epoch,issuer_incarnation)=
                    (CASE WHEN trial='deferred-historical' THEN historical.resource_id ELSE current_owner.resource_id END,
                     CASE WHEN trial='deferred-historical' THEN historical.epoch ELSE current_owner.epoch END,
                     CASE WHEN trial='deferred-historical' THEN historical.process_incarnation ELSE current_owner.process_incarnation END)
                AND (SELECT count(*) FROM control.producer_checkpoint_dependencies)=10
                AND (SELECT count(*) FROM control.producer_outcomes)=4
                INTO stored_ok FROM control.producer_checkpoints WHERE producer_slot_id=NEW.producer_slot_id AND producer_epoch=1;
            PERFORM control.require_retained_producer_progress(NEW.producer_slot_id,1);
            PERFORM control.require_control_owner(NEW.dataset_id,current_owner.resource_id,current_owner.epoch,current_owner.process_incarnation);
            retained_ok:=true;
            IF updated IS DISTINCT FROM 1 OR late_count IS DISTINCT FROM 1 OR stored_ok IS DISTINCT FROM true THEN RAISE EXCEPTION 'late issuer trial did not reach complete candidate'; END IF;
            -- The nested candidate is now complete C4, never partial C3+10.
            SET CONSTRAINTS ALL IMMEDIATE;
            RAISE EXCEPTION 'positive fixture rollback' USING ERRCODE='ZX001';
        EXCEPTION WHEN OTHERS THEN GET STACKED DIAGNOSTICS code=RETURNED_SQLSTATE,message=MESSAGE_TEXT,context=PG_EXCEPTION_CONTEXT; END;
        SET CONSTRAINTS ALL DEFERRED;
        IF updated IS DISTINCT FROM 1 OR late_count IS DISTINCT FROM 1
            OR retained_ok IS DISTINCT FROM true OR stored_ok IS DISTINCT FROM true
            OR (trial='deferred-current' AND code<>'ZX001')
            OR (trial='deferred-historical' AND (code<>'23514'
                OR message<>'Control owner belongs to another backend or is inactive'
                OR position('require_producer_progress_children' in context)=0
                OR position('require_control_owner' in context)=0))
            OR monitor_fixture.snapshot()<>saved THEN
            RAISE EXCEPTION 'deferred case %: did not reach its exact owning guard (% / %)',trial,code,message;
        END IF;
        INSERT INTO monitor_fixture.results VALUES(trial,code);
        INSERT INTO monitor_fixture.observations VALUES(trial,jsonb_build_object(
            'updated',updated,'late_hook_invocations',late_count,'stored_complete_c4',stored_ok,
            'retained_validated',retained_ok,'sqlstate',code,'message',message,
            'exception_context',context,'runtime_user',current_user,'session_user',session_user,'backend_pid',pg_backend_pid()));
    END LOOP;
    UPDATE monitor_fixture.late_fault SET mode=NULL,invocations=0 WHERE singleton;
    -- Each trial rolls back; explicitly restored DEFERRED applies to the outer commit.
    IF monitor_fixture.snapshot()<>saved THEN RAISE EXCEPTION 'fixture changed retained state'; END IF;
    INSERT INTO monitor_fixture.observations VALUES('closing',jsonb_build_object(
        'runtime_user',current_user,'session_user',session_user,'backend_pid',pg_backend_pid(),
        'retained_bytes',charge,'jobs',jobs,'dependencies',9,'c4_digest',encode(e.c4_digest,'hex')));
    RETURN NEW;
END $$;
CREATE TRIGGER zz_monitor_closing_fixture BEFORE INSERT ON control.producer_checkpoint_dependencies
    FOR EACH ROW EXECUTE FUNCTION monitor_fixture.closing_checks();
GRANT SELECT ON monitor_fixture.expected TO bigbrotr_control;
GRANT SELECT,UPDATE ON monitor_fixture.late_fault TO bigbrotr_control;
GRANT SELECT,INSERT ON monitor_fixture.observations TO bigbrotr_control;
GRANT EXECUTE ON FUNCTION monitor_fixture.snapshot(),monitor_fixture.late_issuer(),monitor_fixture.closing_checks() TO bigbrotr_control;
