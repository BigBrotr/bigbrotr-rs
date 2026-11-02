-- NOT_RUN source fixture. Install only in the isolated Monitor qualification DB
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
            SET CONSTRAINTS ALL IMMEDIATE;
            RAISE EXCEPTION 'positive fixture rollback' USING ERRCODE='ZX001';
        EXCEPTION WHEN OTHERS THEN GET STACKED DIAGNOSTICS code=RETURNED_SQLSTATE; END;
        IF code<>expected OR (SELECT count(*) FROM control.producer_requests)<>count_before THEN
            RAISE EXCEPTION 'monitor evidence case %: observed %, expected %',mode,code,expected;
        END IF;
        INSERT INTO monitor_fixture.results VALUES(mode,code);
    END LOOP;
    SET CONSTRAINTS ALL DEFERRED;
    RETURN NEW;
END $$;
CREATE TRIGGER zz_monitor_evidence_fixture BEFORE INSERT ON control.producer_requests
    FOR EACH ROW WHEN(NEW.monitor_attempt_id IS NOT NULL)
    EXECUTE FUNCTION monitor_fixture.evidence_checks();
GRANT EXECUTE ON FUNCTION monitor_fixture.evidence_checks() TO bigbrotr_control;
