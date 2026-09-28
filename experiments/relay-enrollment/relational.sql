-- Test-only hook, installed solely in an isolated clone. Every attempted write
-- runs as the actual Control login, inside its valid owner/session transaction.
-- Python supplies independently encoded command digests, never the SQL reducer.
CREATE SCHEMA enrollment_fixture;
CREATE TABLE enrollment_fixture.results (
    position integer PRIMARY KEY,
    name text NOT NULL UNIQUE,
    sqlstate text NOT NULL,
    message text NOT NULL,
    backend_pid integer NOT NULL,
    owner_epoch numeric NOT NULL,
    unchanged boolean NOT NULL
);

CREATE FUNCTION enrollment_fixture.snapshot() RETURNS jsonb
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE name text; value jsonb; result jsonb := '{}';
BEGIN
    FOR name IN SELECT tablename FROM pg_tables WHERE schemaname='control' ORDER BY tablename LOOP
        EXECUTE format('SELECT jsonb_build_object(''rows'',count(*),''sha256'',
            encode(sha256(convert_to(coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),''[]'')::text,''UTF8'')),''hex''))
            FROM control.%I t',name) INTO value;
        result := result || jsonb_build_object(name,value);
    END LOOP;
    RETURN result;
END $$;

CREATE FUNCTION enrollment_fixture.attempt(parent control.relay_enrollments, mode text) RETURNS void
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE detail control.relay_enrollment_changes%ROWTYPE;
    command control.management_commands%ROWTYPE;
    audit control.management_audit%ROWTYPE;
    access control.relay_access_contexts%ROWTYPE;
    route control.network_routes%ROWTYPE;
    state control.relay_enrollments%ROWTYPE;
BEGIN
    SELECT * INTO STRICT detail FROM control.relay_enrollment_changes
        WHERE actor_id=parent.actor_id AND request_key=parent.request_key;
    SELECT * INTO STRICT command FROM control.management_commands
        WHERE actor_id=parent.actor_id AND request_key=parent.request_key;
    SELECT * INTO STRICT audit FROM control.management_audit WHERE audit_id=command.audit_id;
    SELECT * INTO STRICT access FROM control.relay_access_contexts WHERE access_scope_id=parent.access_scope_id;
    SELECT * INTO STRICT route FROM control.network_routes WHERE route_id=parent.route_id;
    state := parent;
    command.request_key := decode(repeat('88',16),'hex');
    command.audit_id := decode(repeat('89',16),'hex');
    command.request_digest := decode('@fresh_digest@','hex');
    audit.audit_id := command.audit_id;
    audit.subject_id := command.request_key;
    detail.request_key := command.request_key;
    access.actor_id := command.actor_id;
    access.request_key := command.request_key;
    access.access_scope_id := decode(repeat('90',16),'hex');
    access.ordinal := 101;
    detail.access_scope_id := access.access_scope_id;
    state.access_scope_id := access.access_scope_id;
    state.request_key := command.request_key;

    IF mode='early-second-transition' THEN
        -- The parent really passes every pending constraint before the second
        -- transition. Deferring new events cannot erase its transaction identity.
        SET CONSTRAINTS ALL IMMEDIATE;
        SET CONSTRAINTS ALL DEFERRED;
        command.request_digest := decode('@withdraw_digest@','hex');
        detail.access_scope_id := parent.access_scope_id;
        detail.expected_generation := 1;
        detail.generation := 2;
        detail.enabled := false;
    END IF;
    IF mode='wrong-digest' THEN command.request_digest := decode(repeat('00',32),'hex'); END IF;
    IF mode='wrong-audit-time' THEN audit.created_at_us := audit.created_at_us+1; END IF;
    INSERT INTO control.management_audit SELECT (audit).*;
    INSERT INTO control.management_commands SELECT (command).*;
    IF mode<>'early-second-transition' THEN
        INSERT INTO control.relay_access_contexts SELECT (access).*;
    END IF;
    INSERT INTO control.relay_enrollment_changes SELECT (detail).*;
    IF mode='wrong-generation' THEN state.generation := 2; END IF;
    IF mode='wrong-route' THEN
        SELECT route_id INTO STRICT state.route_id FROM control.network_routes
            WHERE dataset_id=parent.dataset_id AND endpoint_id=parent.endpoint_id
                AND route_bytes=decode('00','hex');
    END IF;
    IF mode<>'missing-current' THEN INSERT INTO control.relay_enrollments SELECT (state).*; END IF;
    IF mode='extra-access' THEN
        access.access_scope_id := decode(repeat('92',16),'hex');
        access.ordinal := 102;
        INSERT INTO control.relay_access_contexts SELECT (access).*;
    ELSIF mode='extra-route' THEN
        route.route_id := decode(repeat('93',16),'hex');
        route.route_bytes := decode('0104180a020400','hex');
        route.actor_id := command.actor_id;
        route.request_key := command.request_key;
        INSERT INTO control.network_routes SELECT (route).*;
    ELSIF mode='extra-audit' THEN
        audit.audit_id := decode(repeat('94',16),'hex');
        INSERT INTO control.management_audit SELECT (audit).*;
    ELSIF mode='mutate-access' THEN
        UPDATE control.relay_access_contexts SET ordinal=103 WHERE access_scope_id=access.access_scope_id;
    ELSIF mode='mutate-route' THEN
        UPDATE control.network_routes SET route_bytes=decode('0104180a020400','hex') WHERE route_id=parent.route_id;
    ELSIF mode='old-receipt' THEN
        UPDATE control.relay_enrollments e SET route_id=d.route_id,enabled=d.enabled,
            generation=d.generation,actor_id=d.actor_id,request_key=d.request_key
            FROM control.relay_enrollment_changes d
            WHERE d.actor_id=parent.actor_id AND d.request_key=decode(repeat('01',16),'hex')
                AND e.dataset_id=d.dataset_id AND e.endpoint_id=d.endpoint_id
                AND e.access_scope_id=d.access_scope_id;
        IF NOT FOUND THEN RAISE EXCEPTION 'fixture old receipt did not select its current row'; END IF;
    END IF;
    SET CONSTRAINTS ALL IMMEDIATE;
END $$;

CREATE FUNCTION enrollment_fixture.check_relations() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE before_state jsonb; after_state jsonb; expected record;
    observed_code text; observed_message text; current_owner numeric;
BEGIN
    IF session_user<>'bigbrotr_control' OR current_user<>'bigbrotr_control' THEN
        RAISE EXCEPTION 'fixture must exercise the actual Control login';
    END IF;
    -- Positive parent closure establishes that no stale owner, absent session or
    -- unrelated missing dependency can account for a later expected refusal.
    SET CONSTRAINTS ALL IMMEDIATE;
    SELECT owner_epoch INTO STRICT current_owner FROM control.relay_enrollment_changes
        WHERE actor_id=NEW.actor_id AND request_key=NEW.request_key;
    FOR expected IN SELECT * FROM (VALUES
        (1,'valid-before','ZX001','fixture valid rollback'),
        (2,'missing-current','23514','enrollment requires exact atomic command audit and state'),
        (3,'wrong-generation','23514','enrollment requires its exact atomic command'),
        (4,'wrong-route','23514','enrollment requires its exact atomic command'),
        (5,'wrong-digest','23514','enrollment requires exact atomic command audit and state'),
        (6,'wrong-audit-time','23514','enrollment requires exact atomic command audit and state'),
        (7,'extra-access','23514','access identity requires its creating enrollment'),
        (8,'extra-route','23514','route identity requires its creating enrollment'),
        (9,'extra-audit','23514','enrollment audit is not its command receipt'),
        (10,'mutate-access','23514','immutable control history cannot be changed'),
        (11,'mutate-route','23514','immutable control history cannot be changed'),
        (12,'old-receipt','23514','enrollment requires its exact atomic command'),
        (13,'early-second-transition','23514','enrollment must commit before another transition'),
        (14,'valid-after','ZX001','fixture valid rollback')
    ) cases(position,name,code,message) ORDER BY position LOOP
        SET CONSTRAINTS ALL DEFERRED;
        before_state := enrollment_fixture.snapshot();
        BEGIN
            PERFORM enrollment_fixture.attempt(NEW,expected.name);
            -- Roll back even the fully valid positive controls; no artificial
            -- accepted command is left in the real application's receipt set.
            RAISE EXCEPTION 'fixture valid rollback' USING ERRCODE='ZX001';
        EXCEPTION WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS observed_code=RETURNED_SQLSTATE,observed_message=MESSAGE_TEXT;
        END;
        after_state := enrollment_fixture.snapshot();
        IF observed_code<>expected.code OR observed_message<>expected.message OR before_state<>after_state THEN
            RAISE EXCEPTION 'fixture %: observed % / %, expected % / %, unchanged=%',
                expected.name,observed_code,observed_message,expected.code,expected.message,before_state=after_state;
        END IF;
        INSERT INTO enrollment_fixture.results VALUES(expected.position,expected.name,observed_code,
            observed_message,pg_backend_pid(),current_owner,true);
    END LOOP;
    -- Return the caller to its ordinary initially deferred COMMIT behavior.
    SET CONSTRAINTS ALL DEFERRED;
    RETURN NULL;
END $$;
CREATE TRIGGER zz_enrollment_fixture AFTER INSERT ON control.relay_enrollments
    FOR EACH ROW WHEN (NEW.request_key=decode(repeat('31',16),'hex'))
    EXECUTE FUNCTION enrollment_fixture.check_relations();
GRANT USAGE ON SCHEMA enrollment_fixture TO bigbrotr_control;
GRANT INSERT ON enrollment_fixture.results TO bigbrotr_control;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA enrollment_fixture TO bigbrotr_control;
GRANT UPDATE ON control.relay_access_contexts,control.network_routes TO bigbrotr_control;
