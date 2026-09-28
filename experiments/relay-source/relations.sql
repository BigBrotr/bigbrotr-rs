-- Isolated test hook: actual Control login/backend, original reservation and
-- every production constraint stay in force. Each nested attempt rolls back.
CREATE SCHEMA source_fixture;
CREATE TABLE source_fixture.results (
    position integer PRIMARY KEY,
    name text NOT NULL UNIQUE,
    sqlstate text NOT NULL,
    message text NOT NULL,
    backend_pid integer NOT NULL,
    unchanged boolean NOT NULL
);
CREATE FUNCTION source_fixture.snapshot() RETURNS jsonb
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

CREATE FUNCTION source_fixture.bind_import(parent control.producer_requests) RETURNS void
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE job bytea := decode(repeat('90',16),'hex');
BEGIN
    INSERT INTO control.jobs(job_id,dataset_id,principal_id,request_key,request_digest,
        kind,config_revision,policy_revision,created_at_us,input_complete)
        SELECT job,parent.dataset_id,principal_id,job,decode(repeat('91',32),'hex'),
            'import',1,1,0,true FROM control.principals
        WHERE dataset_id=parent.dataset_id AND kind='installation';
    UPDATE control.jobs SET state='queued',job_revision=2,
        owner_resource_id=parent.issuer_resource,owner_epoch=parent.issuer_epoch,
        owner_incarnation=parent.issuer_incarnation WHERE job_id=job;
    UPDATE control.jobs SET state='running',job_revision=3 WHERE job_id=job;
    INSERT INTO control.producer_input_bindings(job_id,dataset_id,source_namespace,shard_id,
        source_bytes,source_sha256,parser_profile,batch_first_record,batch_records,obligation_index,
        obligation_kind,logical_record,byte_start,byte_end,record_sha256,producer_slot_id,producer_epoch,
        request_sequence,request_digest,input_bytes,input_profile,manifest_sha256,
        issuer_resource,issuer_epoch,issuer_incarnation)
        VALUES(job,parent.dataset_id,decode(repeat('92',32),'hex'),0,100,
            decode(repeat('93',32),'hex'),1,1,1,0,1,1,0,100,decode(repeat('94',32),'hex'),
            parent.producer_slot_id,parent.producer_epoch,parent.request_sequence,
            parent.request_digest,parent.input_bytes,parent.input_profile,parent.manifest_sha256,
            parent.issuer_resource,parent.issuer_epoch,parent.issuer_incarnation);
END $$;

CREATE FUNCTION source_fixture.attempt(parent control.producer_requests, mode text) RETURNS void
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE candidate control.producer_requests := parent;
BEGIN
    IF mode IN ('valid-unbound','valid-import') THEN
        candidate.relay_endpoint_id := NULL;
        candidate.relay_access_scope_id := NULL;
        candidate.relay_enrollment_generation := NULL;
        candidate.relay_route_id := NULL;
    ELSIF mode='missing-endpoint' THEN candidate.relay_endpoint_id := NULL;
    ELSIF mode='missing-access' THEN candidate.relay_access_scope_id := NULL;
    ELSIF mode='missing-generation' THEN candidate.relay_enrollment_generation := NULL;
    ELSIF mode='missing-route' THEN candidate.relay_route_id := NULL;
    ELSIF mode='zero-generation' THEN candidate.relay_enrollment_generation := 0;
    ELSIF mode='future-generation' THEN candidate.relay_enrollment_generation := 2;
    ELSIF mode='other-endpoint' THEN
        SELECT endpoint_id INTO STRICT candidate.relay_endpoint_id FROM control.relay_endpoints
            WHERE endpoint_id<>parent.relay_endpoint_id;
    ELSIF mode='other-route' THEN
        SELECT route_id INTO STRICT candidate.relay_route_id FROM control.network_routes
            WHERE endpoint_id<>parent.relay_endpoint_id;
    END IF;
    IF mode IN ('import-then-bound','valid-import') THEN
        PERFORM source_fixture.bind_import(parent);
    END IF;
    INSERT INTO control.producer_requests SELECT (candidate).*;
    IF mode='bound-then-import' THEN
        PERFORM source_fixture.bind_import(parent);
    ELSIF mode='change-source' THEN
        UPDATE control.producer_requests SET relay_enrollment_generation=2
            WHERE producer_slot_id=parent.producer_slot_id AND request_sequence=parent.request_sequence;
    ELSIF mode='erase-source' THEN
        UPDATE control.producer_requests SET relay_endpoint_id=NULL,relay_access_scope_id=NULL,
            relay_enrollment_generation=NULL,relay_route_id=NULL
            WHERE producer_slot_id=parent.producer_slot_id AND request_sequence=parent.request_sequence;
    ELSIF mode='delete-source' THEN
        DELETE FROM control.producer_requests
            WHERE producer_slot_id=parent.producer_slot_id AND request_sequence=parent.request_sequence;
    END IF;
    SET CONSTRAINTS ALL IMMEDIATE;
END $$;

CREATE FUNCTION source_fixture.check_relations() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE before_state jsonb; expected record; code text; message text;
BEGIN
    IF pg_trigger_depth()>1 THEN RETURN NEW; END IF;
    IF session_user<>'bigbrotr_control' OR current_user<>'bigbrotr_control' THEN
        RAISE EXCEPTION 'source fixture requires actual Control login';
    END IF;
    FOR expected IN SELECT * FROM (VALUES
        (1,'valid-bound-before','ZX001','fixture valid rollback'),
        (2,'missing-endpoint','23514','producer relay source must be complete'),
        (3,'missing-access','23514','producer relay source must be complete'),
        (4,'missing-generation','23514','producer relay source must be complete'),
        (5,'missing-route','23514','producer relay source must be complete'),
        (6,'zero-generation','23514','producer relay source must be complete'),
        (7,'future-generation','23514','relay source enrollment is not current and enabled'),
        (8,'other-endpoint','23514','relay source enrollment is not current and enabled'),
        (9,'other-route','23514','relay source enrollment is not current and enabled'),
        (10,'import-then-bound','23514','producer request cannot bind both relay and import sources'),
        (11,'bound-then-import','23514','input binding cannot attach an already reserved request'),
        (12,'change-source','23514','producer request reservations are retained and immutable'),
        (13,'erase-source','23514','producer request reservations are retained and immutable'),
        (14,'delete-source','23514','producer request reservations are retained and immutable'),
        (15,'valid-unbound','ZX001','fixture valid rollback'),
        (16,'valid-import','ZX001','fixture valid rollback'),
        (17,'valid-bound-after','ZX001','fixture valid rollback')
    ) cases(position,name,code,message) ORDER BY position LOOP
        SET CONSTRAINTS ALL DEFERRED;
        before_state := source_fixture.snapshot();
        BEGIN
            PERFORM source_fixture.attempt(NEW,expected.name);
            RAISE EXCEPTION 'fixture valid rollback' USING ERRCODE='ZX001';
        EXCEPTION WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS code=RETURNED_SQLSTATE,message=MESSAGE_TEXT;
        END;
        IF code<>expected.code OR message<>expected.message OR before_state<>source_fixture.snapshot() THEN
            RAISE EXCEPTION 'source fixture %: observed % / %, expected % / %, unchanged=%',
                expected.name,code,message,expected.code,expected.message,before_state=source_fixture.snapshot();
        END IF;
        INSERT INTO source_fixture.results VALUES(expected.position,expected.name,code,message,pg_backend_pid(),true);
    END LOOP;
    SET CONSTRAINTS ALL DEFERRED;
    RETURN NEW;
END $$;
-- Last BEFORE hook: the real parent already passed every reservation guard;
-- recursive attempts still run all those guards but skip only this fixture.
CREATE TRIGGER zz_source_fixture BEFORE INSERT ON control.producer_requests
    FOR EACH ROW WHEN (NEW.relay_endpoint_id IS NOT NULL)
    EXECUTE FUNCTION source_fixture.check_relations();
GRANT USAGE ON SCHEMA source_fixture TO bigbrotr_control;
GRANT INSERT ON source_fixture.results TO bigbrotr_control;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA source_fixture TO bigbrotr_control;
