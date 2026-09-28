-- Clone-only hook in the actual application backend. Production guards stay on;
-- each nested attempt rolls back all Control rows and xmin before the next one.
CREATE SCHEMA binding_fixture;
CREATE TABLE binding_fixture.results (
    position integer PRIMARY KEY,
    name text NOT NULL UNIQUE,
    sqlstate text NOT NULL,
    message text NOT NULL,
    backend_pid integer NOT NULL,
    unchanged boolean NOT NULL
);
CREATE FUNCTION binding_fixture.snapshot() RETURNS jsonb
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE name text; value jsonb; result jsonb := '{}';
BEGIN
    FOR name IN SELECT tablename FROM pg_tables WHERE schemaname='control' ORDER BY tablename LOOP
        EXECUTE format('SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),''[]'')
            FROM (SELECT *,xmin::text AS fixture_xmin FROM control.%I) t',name) INTO value;
        result := result || jsonb_build_object(name,value);
    END LOOP;
    RETURN result;
END $$;

CREATE FUNCTION binding_fixture.attempt(parent control.producer_requests, mode text) RETURNS void
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE candidate control.producer_requests := parent; other_scope bytea;
BEGIN
    SELECT scope_id INTO STRICT other_scope FROM control.acquisition_scopes
        WHERE scope_id<>parent.acquisition_scope_id;
    IF mode IN ('scope-without-source','valid-unbound') THEN
        candidate.relay_endpoint_id := NULL;
        candidate.relay_access_scope_id := NULL;
        candidate.relay_enrollment_generation := NULL;
        candidate.relay_route_id := NULL;
    ELSIF mode='missing-endpoint' THEN candidate.relay_endpoint_id := NULL;
    ELSIF mode='missing-access' THEN candidate.relay_access_scope_id := NULL;
    ELSIF mode='missing-generation' THEN candidate.relay_enrollment_generation := NULL;
    ELSIF mode='missing-route' THEN candidate.relay_route_id := NULL;
    ELSIF mode='zero-scope' THEN candidate.acquisition_scope_id := decode(repeat('00',16),'hex');
    ELSIF mode='short-scope' THEN candidate.acquisition_scope_id := decode('01','hex');
    ELSIF mode='absent-scope' THEN candidate.acquisition_scope_id := decode(repeat('ef',16),'hex');
    ELSIF mode='valid-overlapping' THEN candidate.acquisition_scope_id := other_scope;
    END IF;
    IF mode IN ('valid-unbound','valid-source-only') THEN candidate.acquisition_scope_id := NULL; END IF;
    IF mode LIKE 'helper-%' THEN
        PERFORM control.require_acquisition_scope(
            CASE mode WHEN 'helper-null-dataset' THEN NULL
                WHEN 'helper-other-dataset' THEN decode(repeat('ef',16),'hex') ELSE parent.dataset_id END,
            CASE WHEN mode='helper-null-scope' THEN NULL ELSE parent.acquisition_scope_id END,
            CASE mode WHEN 'helper-null-endpoint' THEN NULL
                WHEN 'helper-other-endpoint' THEN decode(repeat('ef',16),'hex') ELSE parent.relay_endpoint_id END,
            CASE mode WHEN 'helper-null-access' THEN NULL
                WHEN 'helper-other-access' THEN decode(repeat('ef',16),'hex') ELSE parent.relay_access_scope_id END);
        RETURN;
    END IF;
    INSERT INTO control.producer_requests SELECT (candidate).*;
    IF mode IN ('change-scope','erase-scope') THEN
        UPDATE control.producer_requests SET acquisition_scope_id=
            CASE WHEN mode='erase-scope' THEN NULL ELSE other_scope END
            WHERE producer_slot_id=parent.producer_slot_id AND producer_epoch=parent.producer_epoch
                AND request_sequence=parent.request_sequence;
    ELSIF mode='delete-scope' THEN
        DELETE FROM control.producer_requests WHERE producer_slot_id=parent.producer_slot_id
            AND producer_epoch=parent.producer_epoch AND request_sequence=parent.request_sequence;
    END IF;
    SET CONSTRAINTS ALL IMMEDIATE;
END $$;

CREATE FUNCTION binding_fixture.check_relations() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE before_state jsonb; expected record; code text; message text;
BEGIN
    IF pg_trigger_depth()>1 THEN RETURN NEW; END IF;
    IF session_user<>'bigbrotr_control' OR current_user<>'bigbrotr_control' THEN
        RAISE EXCEPTION 'binding fixture requires actual Control login';
    END IF;
    FOR expected IN SELECT * FROM (VALUES
        (1,'valid-bound-before','ZX001','fixture valid rollback'),
        (2,'scope-without-source','23514','producer acquisition scope requires a complete relay source'),
        (3,'missing-endpoint','23514','producer relay source must be complete'),
        (4,'missing-access','23514','producer relay source must be complete'),
        (5,'missing-generation','23514','producer relay source must be complete'),
        (6,'missing-route','23514','producer relay source must be complete'),
        (7,'zero-scope','23514','acquisition scope is not current for this source'),
        (8,'short-scope','23514','acquisition scope is not current for this source'),
        (9,'absent-scope','23514','acquisition scope is not current for this source'),
        (10,'helper-other-dataset','23514','acquisition scope is not current for this source'),
        (11,'helper-other-endpoint','23514','acquisition scope is not current for this source'),
        (12,'helper-other-access','23514','acquisition scope is not current for this source'),
        (13,'helper-null-dataset','23514','acquisition scope is not current for this source'),
        (14,'helper-null-scope','23514','acquisition scope is not current for this source'),
        (15,'helper-null-endpoint','23514','acquisition scope is not current for this source'),
        (16,'helper-null-access','23514','acquisition scope is not current for this source'),
        (17,'valid-overlapping','ZX001','fixture valid rollback'),
        (18,'valid-source-only','ZX001','fixture valid rollback'),
        (19,'valid-unbound','ZX001','fixture valid rollback'),
        (20,'change-scope','23514','producer request reservations are retained and immutable'),
        (21,'erase-scope','23514','producer request reservations are retained and immutable'),
        (22,'delete-scope','23514','producer request reservations are retained and immutable'),
        (23,'valid-bound-after','ZX001','fixture valid rollback')
    ) cases(position,name,code,message) ORDER BY position LOOP
        SET CONSTRAINTS ALL DEFERRED;
        before_state := binding_fixture.snapshot();
        BEGIN
            PERFORM binding_fixture.attempt(NEW,expected.name);
            RAISE EXCEPTION 'fixture valid rollback' USING ERRCODE='ZX001';
        EXCEPTION WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS code=RETURNED_SQLSTATE,message=MESSAGE_TEXT;
        END;
        IF code<>expected.code OR message<>expected.message OR before_state<>binding_fixture.snapshot() THEN
            RAISE EXCEPTION 'binding fixture %: observed % / %, expected % / %, unchanged=%',
                expected.name,code,message,expected.code,expected.message,before_state=binding_fixture.snapshot();
        END IF;
        INSERT INTO binding_fixture.results VALUES(expected.position,expected.name,code,message,pg_backend_pid(),true);
    END LOOP;
    SET CONSTRAINTS ALL DEFERRED;
    RETURN NEW;
END $$;
CREATE TRIGGER zz_binding_fixture BEFORE INSERT ON control.producer_requests
    FOR EACH ROW WHEN (NEW.acquisition_scope_id IS NOT NULL)
    EXECUTE FUNCTION binding_fixture.check_relations();
GRANT USAGE ON SCHEMA binding_fixture TO bigbrotr_control;
GRANT INSERT ON binding_fixture.results TO bigbrotr_control;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA binding_fixture TO bigbrotr_control;
-- Reach the retained-history guards instead of stopping at clone-only ACLs.
GRANT DELETE ON control.producer_requests TO bigbrotr_control;
