-- Clone-only hook. The real authenticated Control transaction must first pass
-- every parent constraint. Speculative commands are rolled back individually.
CREATE SCHEMA registration_fixture;
CREATE TABLE registration_fixture.results (
    position integer PRIMARY KEY,
    name text NOT NULL UNIQUE,
    sqlstate text NOT NULL,
    message text NOT NULL,
    constraint_name text NOT NULL,
    backend_pid integer NOT NULL,
    owner_epoch numeric NOT NULL,
    unchanged boolean NOT NULL
);

CREATE FUNCTION registration_fixture.snapshot() RETURNS jsonb
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE name text; value jsonb; result jsonb := '{}';
BEGIN
    FOR name IN SELECT tablename FROM pg_tables WHERE schemaname='control' ORDER BY tablename LOOP
        EXECUTE format('SELECT jsonb_build_object(''rows'',count(*),''sha256'',
            encode(sha256(convert_to(coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),''[]'')::text,''UTF8'')),''hex''))
            FROM (SELECT *,xmin::text AS xmin FROM control.%I) t',name) INTO value;
        result := result || jsonb_build_object(name,value);
    END LOOP;
    IF (SELECT count(*) FROM jsonb_object_keys(result)) <> 69 THEN
        RAISE EXCEPTION 'fixture requires all 69 Control relations';
    END IF;
    RETURN result;
END $$;

CREATE FUNCTION registration_fixture.attempt(parent control.acquisition_scope_commands, mode text)
RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE detail control.acquisition_scope_commands%ROWTYPE;
    command control.management_commands%ROWTYPE;
    audit control.management_audit%ROWTYPE;
    alternate control.acquisition_scopes%ROWTYPE;
BEGIN
    detail := parent;
    SELECT * INTO STRICT command FROM control.management_commands
        WHERE actor_id=parent.actor_id AND request_key=parent.request_key;
    SELECT * INTO STRICT audit FROM control.management_audit WHERE audit_id=command.audit_id;
    command.request_key := decode(repeat('88',16),'hex');
    command.audit_id := decode(repeat('89',16),'hex');
    command.request_digest := decode('@fresh_digest@','hex');
    audit.audit_id := command.audit_id;
    audit.subject_id := command.request_key;
    detail.request_key := command.request_key;

    IF mode='wrong-scope' OR mode='valid-new-scope' OR mode LIKE 'new-scope-%' THEN
        -- The retained parent has only one scope. Create a real, valid alternate
        -- in this subtransaction; a missing-row/FK error cannot stand in for a
        -- digest refusal. Every speculative identity is rolled back by the hook.
        INSERT INTO control.acquisition_selectors (
            selector_id,dataset_id,profile_version,document,registered_at_us,
            config_revision,policy_revision,owner_resource_id,owner_epoch,owner_incarnation
        ) VALUES (
            decode(repeat('93',16),'hex'),parent.dataset_id,1,decode('@alternate_document@','hex'),
            command.accepted_at_us,command.expected_config_revision,command.policy_revision,
            parent.owner_resource_id,parent.owner_epoch,parent.owner_incarnation
        );
        SELECT * INTO STRICT alternate FROM control.acquisition_scopes
            WHERE dataset_id=parent.dataset_id AND scope_id=parent.scope_id;
        alternate.scope_id := decode(repeat('94',16),'hex');
        alternate.selector_id := decode(repeat('93',16),'hex');
        alternate.registered_at_us := command.accepted_at_us;
        alternate.owner_resource_id := parent.owner_resource_id;
        alternate.owner_epoch := parent.owner_epoch;
        alternate.owner_incarnation := parent.owner_incarnation;
        INSERT INTO control.acquisition_scopes SELECT (alternate).*;
        detail.scope_id := alternate.scope_id;
        IF mode<>'wrong-scope' THEN
            command.request_digest := decode('@alternate_digest@','hex');
        END IF;
    END IF;
    IF mode='wrong-audit-time' THEN
        audit.created_at_us := audit.created_at_us+1;
    ELSIF mode='old-owner' THEN
        SELECT owner_resource_id,owner_epoch,owner_incarnation
            INTO STRICT detail.owner_resource_id,detail.owner_epoch,detail.owner_incarnation
            FROM control.acquisition_scopes WHERE scope_id=parent.scope_id;
    ELSIF mode='absent-session' THEN
        detail.session_id := decode(repeat('90',16),'hex');
    ELSIF mode IN ('revoked-session','new-scope-revoked-session') THEN
        UPDATE control.browser_sessions SET revoked_at_us=GREATEST(issued_at_us,
            (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint)
            WHERE session_id=parent.session_id;
        IF NOT FOUND THEN RAISE EXCEPTION 'fixture requires current session'; END IF;
    ELSIF mode='credential-revision' THEN
        detail.actor_credential_revision := detail.actor_credential_revision+1;
    ELSIF mode='authentication-revision' THEN
        detail.actor_authentication_revision := detail.actor_authentication_revision+1;
    ELSIF mode='grant-revision' THEN
        detail.actor_grant_revision := detail.actor_grant_revision+1;
    ELSIF mode='missing-grant' THEN
        DELETE FROM control.principal_grants WHERE dataset_id=parent.dataset_id
            AND principal_id=parent.actor_id AND action='roles.control';
        IF NOT FOUND THEN RAISE EXCEPTION 'fixture requires current grant'; END IF;
    END IF;
    IF mode<>'missing-audit' THEN INSERT INTO control.management_audit SELECT (audit).*; END IF;
    IF mode<>'missing-command' THEN INSERT INTO control.management_commands SELECT (command).*; END IF;
    IF mode NOT IN ('missing-detail','new-scope-missing-detail') THEN INSERT INTO control.acquisition_scope_commands SELECT (detail).*; END IF;
    IF mode='extra-audit' THEN
        audit.audit_id := decode(repeat('91',16),'hex');
        INSERT INTO control.management_audit SELECT (audit).*;
    ELSIF mode='mutate-detail' THEN
        UPDATE control.acquisition_scope_commands SET session_id=decode(repeat('92',16),'hex')
            WHERE actor_id=parent.actor_id AND request_key=command.request_key;
    ELSIF mode='delete-detail' THEN
        DELETE FROM control.acquisition_scope_commands
            WHERE actor_id=parent.actor_id AND request_key=command.request_key;
    END IF;
    SET CONSTRAINTS ALL IMMEDIATE;
END $$;

CREATE FUNCTION registration_fixture.check_relations() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE before_state jsonb; expected record; unchanged boolean;
    observed_code text; observed_message text; observed_constraint text; observed_table text;
BEGIN
    IF session_user<>'bigbrotr_control' OR current_user<>'bigbrotr_control' THEN
        RAISE EXCEPTION 'fixture must exercise the actual Control login';
    END IF;
    SET CONSTRAINTS ALL IMMEDIATE;
    FOR expected IN SELECT * FROM (VALUES
        (1,'valid-before','ZX001','fixture valid rollback',NULL,NULL),
        (2,'missing-detail','23514','scope command requires atomic registration detail',NULL,NULL),
        (3,'missing-command','23503',NULL,'acquisition_scope_commands','management_commands'),
        (4,'missing-audit','23503',NULL,'management_commands','management_audit'),
        (5,'wrong-scope','23514','scope registration requires exact command, audit and policy heads',NULL,NULL),
        (6,'wrong-audit-time','23514','scope registration requires exact command, audit and policy heads',NULL,NULL),
        (7,'extra-audit','23514','scope registration audit is not its command receipt',NULL,NULL),
        (8,'old-owner','23514','Control owner belongs to another backend or is inactive',NULL,NULL),
        (9,'absent-session','23514','scope command requires its authenticated session',NULL,NULL),
        (10,'revoked-session','BB001','scope registration authentication or owner expired before commit',NULL,NULL),
        (11,'credential-revision','BB001','scope registration authentication or owner expired before commit',NULL,NULL),
        (12,'authentication-revision','BB001','scope registration authentication or owner expired before commit',NULL,NULL),
        (13,'grant-revision','BB001','scope registration authentication or owner expired before commit',NULL,NULL),
        (14,'mutate-detail','23514','immutable control history cannot be changed',NULL,NULL),
        (15,'delete-detail','23514','immutable control history cannot be changed',NULL,NULL),
        (16,'valid-new-scope','ZX001','fixture valid rollback',NULL,NULL),
        (17,'new-scope-missing-detail','23514','scope command requires atomic registration detail',NULL,NULL),
        (18,'new-scope-revoked-session','BB001','scope registration authentication or owner expired before commit',NULL,NULL),
        (19,'missing-grant','BB001','scope registration authentication or owner expired before commit',NULL,NULL),
        (20,'valid-after','ZX001','fixture valid rollback',NULL,NULL)
    ) cases(position,name,code,message,relation,foreign_relation) ORDER BY position LOOP
        SET CONSTRAINTS ALL DEFERRED;
        before_state := registration_fixture.snapshot();
        BEGIN
            PERFORM registration_fixture.attempt(NEW,expected.name);
            RAISE EXCEPTION 'fixture valid rollback' USING ERRCODE='ZX001';
        EXCEPTION WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS observed_code=RETURNED_SQLSTATE,observed_message=MESSAGE_TEXT,
                observed_constraint=CONSTRAINT_NAME,observed_table=TABLE_NAME;
        END;
        unchanged := before_state=registration_fixture.snapshot();
        IF observed_code<>expected.code OR NOT unchanged
            OR (expected.message IS NOT NULL AND observed_message<>expected.message)
            OR (expected.relation IS NOT NULL AND (observed_table<>expected.relation OR NOT EXISTS (
                SELECT 1 FROM pg_constraint WHERE conname=observed_constraint AND contype='f'
                    AND conrelid=('control.'||expected.relation)::regclass
                    AND confrelid=('control.'||expected.foreign_relation)::regclass
            ))) THEN
            RAISE EXCEPTION 'fixture %: observed % / %, expected % / %, unchanged=%',
                expected.name,observed_code,observed_message,expected.code,expected.message,unchanged;
        END IF;
        INSERT INTO registration_fixture.results VALUES(expected.position,expected.name,observed_code,
            observed_message,observed_constraint,pg_backend_pid(),NEW.owner_epoch,unchanged);
    END LOOP;
    SET CONSTRAINTS ALL DEFERRED;
    RETURN NULL;
END $$;
CREATE TRIGGER zz_registration_fixture AFTER INSERT ON control.acquisition_scope_commands
    FOR EACH ROW WHEN (NEW.request_key=decode(repeat('31',16),'hex'))
    EXECUTE FUNCTION registration_fixture.check_relations();
GRANT USAGE ON SCHEMA registration_fixture TO bigbrotr_control;
GRANT INSERT ON registration_fixture.results TO bigbrotr_control;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA registration_fixture TO bigbrotr_control;
GRANT UPDATE,DELETE,TRUNCATE ON control.acquisition_scope_commands TO bigbrotr_control;
-- Only the isolated fault hook gets direct grant deletion. This is a same-
-- transaction final-validation negative, not a supported concurrent grant writer.
GRANT DELETE ON control.principal_grants TO bigbrotr_control;
