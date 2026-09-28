-- Append after0003. No existing migration or historical receipt is rewritten.
-- This first consumer records public/no-auth intent and one direct route only.
ALTER TABLE control.management_commands DROP CONSTRAINT management_command_shape;
ALTER TABLE control.management_commands ADD CONSTRAINT management_command_shape CHECK (
    (action='policy.activate-initial' AND authority_mode IS NOT NULL
        AND authority_mode IN ('managed','declarative') AND document_digest IS NOT NULL AND policy_revision=1)
    OR (action IN ('scope-grant.change','job.export.accept','job.export.resume','relay.enrollment.change')
        AND authority_mode IS NULL AND document_digest IS NULL AND policy_revision>0)
);
ALTER TABLE control.management_audit DROP CONSTRAINT management_audit_shape;
ALTER TABLE control.management_audit ADD CONSTRAINT management_audit_shape CHECK (
    (policy_revision IS NULL AND (
        (action='dataset.initialize' AND outcome='accepted'
            AND config_revision=1 AND config_revision IS NOT NULL
            AND subject_id IS NULL AND reason IS NULL)
        OR (action IN ('owner-token.issue','owner-token.revoke','owner.enroll',
                      'cursor-keyring.register','cursor-key.enroll',
                      'cursor-key.activate','cursor-key.revoke')
            AND outcome='accepted' AND config_revision IS NULL
            AND subject_id IS NOT NULL AND reason IS NULL
            AND requester_database_role IS NOT NULL AND requester_os_uid IS NOT NULL)
        OR (action='owner.enroll' AND outcome='rejected' AND config_revision IS NULL
            AND subject_id IS NOT NULL AND reason IS NOT NULL
            AND reason IN ('invalid-token','expired','exhausted','terminal')
            AND requester_database_role IS NOT NULL AND requester_os_uid IS NOT NULL)
        OR (action='policy.activate-initial' AND outcome='accepted'
            AND config_revision IS NOT NULL AND config_revision>0
            AND subject_id IS NOT NULL AND reason IS NULL
            AND requester_database_role IS NOT NULL AND requester_os_uid IS NOT NULL)
    ))
    OR (action IN ('browser.session.issue','browser.session.revoke',
                  'browser.session.reauthenticate','scope-grant.change',
                  'job.export.accept','job.export.resume','relay.enrollment.change')
        AND outcome='accepted' AND config_revision IS NOT NULL AND config_revision>0
        AND policy_revision IS NOT NULL AND policy_revision>0
        AND subject_id IS NOT NULL AND reason IS NULL
        AND requester_database_role IS NULL AND requester_os_uid IS NULL)
);

CREATE FUNCTION control.relay_route_valid(value bytea) RETURNS boolean
LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE SET search_path=pg_catalog AS $$
DECLARE family integer; bits integer; width integer; i integer; keep integer;
BEGIN
    IF value=decode('00','hex') THEN RETURN true; END IF;
    IF octet_length(value) NOT IN (7,19) OR get_byte(value,0)<>1 THEN RETURN false; END IF;
    family:=get_byte(value,1); bits:=get_byte(value,2);
    IF family=4 THEN width:=4; ELSIF family=6 THEN width:=16; ELSE RETURN false; END IF;
    IF octet_length(value)<>width+3 OR bits>width*8 THEN RETURN false; END IF;
    IF family=6 AND substring(value FROM 4 FOR 12)=decode('00000000000000000000ffff','hex') THEN
        RETURN false;
    END IF;
    FOR i IN 0..width-1 LOOP
        keep:=greatest(0,least(8,bits-i*8));
        IF (get_byte(value,i+3) & ((1 << (8-keep))-1))<>0 THEN RETURN false; END IF;
    END LOOP;
    RETURN true;
END $$;

CREATE TABLE control.relay_access_contexts (
    access_scope_id control.id16 PRIMARY KEY,
    dataset_id control.id16 NOT NULL REFERENCES control.datasets(dataset_id),
    access_class text NOT NULL DEFAULT 'public' CHECK (access_class='public'),
    dictionary_kind smallint NOT NULL DEFAULT 4 CHECK (dictionary_kind=4),
    allocation_domain control.id16 NOT NULL,
    ordinal control.u64 NOT NULL CHECK (ordinal>0),
    actor_id control.id16 NOT NULL,
    request_key control.id16 NOT NULL,
    UNIQUE (dataset_id,access_scope_id),
    UNIQUE (dataset_id,dictionary_kind,allocation_domain,ordinal),
    FOREIGN KEY (actor_id,request_key) REFERENCES control.management_commands(actor_id,request_key)
);
CREATE TABLE control.network_routes (
    route_id control.id16 PRIMARY KEY,
    dataset_id control.id16 NOT NULL,
    endpoint_id control.id16 NOT NULL,
    route_version smallint NOT NULL DEFAULT 1 CHECK (route_version=1),
    route_bytes bytea NOT NULL CHECK (control.relay_route_valid(route_bytes)),
    actor_id control.id16 NOT NULL,
    request_key control.id16 NOT NULL,
    UNIQUE (dataset_id,endpoint_id,route_id),
    UNIQUE (dataset_id,endpoint_id,route_version,route_bytes),
    FOREIGN KEY (dataset_id,endpoint_id) REFERENCES control.relay_endpoints(dataset_id,endpoint_id),
    FOREIGN KEY (actor_id,request_key) REFERENCES control.management_commands(actor_id,request_key)
);
CREATE TABLE control.relay_enrollment_changes (
    actor_id control.id16 NOT NULL,
    request_key control.id16 NOT NULL,
    dataset_id control.id16 NOT NULL,
    endpoint_id control.id16 NOT NULL,
    access_scope_id control.id16 NOT NULL,
    route_id control.id16 NOT NULL,
    enabled boolean NOT NULL,
    expected_generation control.u64 CHECK (expected_generation BETWEEN 1 AND 18446744073709551614),
    generation control.u64 NOT NULL CHECK (generation=coalesce(expected_generation,0)+1),
    session_id control.id16 NOT NULL,
    actor_credential_revision control.u64 NOT NULL CHECK (actor_credential_revision>0),
    actor_authentication_revision control.u64 NOT NULL CHECK (actor_authentication_revision>0),
    actor_grant_revision control.u64 NOT NULL CHECK (actor_grant_revision>0),
    owner_resource_id control.id16 NOT NULL,
    owner_epoch control.u64 NOT NULL,
    owner_incarnation control.id16 NOT NULL,
    owner_role text NOT NULL DEFAULT 'control' CHECK (owner_role='control'),
    acceptance_transaction xid8 NOT NULL DEFAULT pg_current_xact_id(),
    PRIMARY KEY (actor_id,request_key),
    UNIQUE (dataset_id,endpoint_id,access_scope_id,generation),
    FOREIGN KEY (actor_id,request_key) REFERENCES control.management_commands(actor_id,request_key),
    FOREIGN KEY (dataset_id,actor_id) REFERENCES control.local_accounts(dataset_id,principal_id),
    FOREIGN KEY (dataset_id,access_scope_id) REFERENCES control.relay_access_contexts(dataset_id,access_scope_id),
    FOREIGN KEY (dataset_id,endpoint_id,route_id) REFERENCES control.network_routes(dataset_id,endpoint_id,route_id),
    FOREIGN KEY (dataset_id,owner_resource_id,owner_epoch,owner_role,owner_incarnation)
        REFERENCES control.owner_epochs(dataset_id,resource_id,epoch,role,process_incarnation),
    CHECK (enabled OR expected_generation IS NOT NULL)
);
CREATE TABLE control.relay_enrollments (
    dataset_id control.id16 NOT NULL,
    endpoint_id control.id16 NOT NULL,
    access_scope_id control.id16 NOT NULL,
    route_id control.id16 NOT NULL,
    enabled boolean NOT NULL,
    generation control.u64 NOT NULL CHECK (generation>0),
    actor_id control.id16 NOT NULL,
    request_key control.id16 NOT NULL,
    PRIMARY KEY (dataset_id,endpoint_id,access_scope_id),
    FOREIGN KEY (dataset_id,access_scope_id) REFERENCES control.relay_access_contexts(dataset_id,access_scope_id),
    FOREIGN KEY (dataset_id,endpoint_id,route_id) REFERENCES control.network_routes(dataset_id,endpoint_id,route_id),
    FOREIGN KEY (actor_id,request_key) REFERENCES control.relay_enrollment_changes(actor_id,request_key)
);

-- Bytes, not text conversion: all function inputs have immutable byte semantics.
CREATE FUNCTION control.relay_enrollment_digest(detail control.relay_enrollment_changes,
    command control.management_commands) RETURNS bytea
LANGUAGE sql STABLE STRICT SET search_path=pg_catalog AS $$
    SELECT sha256(decode('62696762726f74722f72656c61792d656e726f6c6c6d656e742d6368616e67652f3100','hex')
        || detail.dataset_id || detail.actor_id || detail.request_key || detail.endpoint_id
        || access.dataset_id || decode('0004','hex') || access.allocation_domain
        || control.command_u64_bytes(access.ordinal)
        || CASE WHEN detail.enabled THEN decode('01','hex') ELSE decode('00','hex') END
        || control.command_u64_bytes(coalesce(detail.expected_generation,0)::control.u64)
        || control.command_u64_bytes(command.expected_config_revision)
        || control.command_u64_bytes(command.policy_revision) || route.route_bytes)
    FROM control.relay_access_contexts access JOIN control.network_routes route
        ON route.dataset_id=access.dataset_id
    WHERE access.dataset_id=detail.dataset_id AND access.access_scope_id=detail.access_scope_id
        AND route.endpoint_id=detail.endpoint_id AND route.route_id=detail.route_id
$$;

CREATE FUNCTION control.guard_relay_enrollment_detail() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE previous control.relay_enrollments%ROWTYPE;
BEGIN
    IF current_setting('transaction_isolation')<>'read committed' THEN
        RAISE EXCEPTION 'enrollment requires READ COMMITTED' USING ERRCODE='25001';
    END IF;
    -- Caller lock order: actual Control, dataset, actor account, session, domain.
    PERFORM control.require_control_owner(NEW.dataset_id,NEW.owner_resource_id,NEW.owner_epoch,NEW.owner_incarnation);
    PERFORM 1 FROM control.datasets WHERE dataset_id=NEW.dataset_id FOR UPDATE;
    PERFORM 1 FROM control.local_accounts WHERE dataset_id=NEW.dataset_id AND principal_id=NEW.actor_id FOR SHARE;
    PERFORM 1 FROM control.browser_sessions WHERE dataset_id=NEW.dataset_id
        AND principal_id=NEW.actor_id AND session_id=NEW.session_id FOR SHARE;
    IF NOT FOUND THEN RAISE EXCEPTION 'enrollment requires its session' USING ERRCODE='23514'; END IF;
    SELECT * INTO previous FROM control.relay_enrollments WHERE dataset_id=NEW.dataset_id
        AND endpoint_id=NEW.endpoint_id AND access_scope_id=NEW.access_scope_id FOR UPDATE;
    IF (FOUND AND (previous.generation IS DISTINCT FROM NEW.expected_generation
            OR (previous.enabled=NEW.enabled AND previous.route_id=NEW.route_id)
            OR (NOT NEW.enabled AND previous.route_id<>NEW.route_id)))
        OR (NOT FOUND AND (NEW.expected_generation IS NOT NULL OR NOT NEW.enabled)) THEN
        RAISE EXCEPTION 'enrollment requires a changed desired state and exact generation' USING ERRCODE='23514';
    END IF;
    -- One accepted transition per key in a transaction, even after SET CONSTRAINTS.
    IF EXISTS (SELECT 1 FROM control.relay_enrollment_changes WHERE dataset_id=NEW.dataset_id
        AND endpoint_id=NEW.endpoint_id AND access_scope_id=NEW.access_scope_id
        AND acceptance_transaction=pg_current_xact_id()) THEN
        RAISE EXCEPTION 'enrollment must commit before another transition' USING ERRCODE='23514';
    END IF;
    NEW.acceptance_transaction:=pg_current_xact_id();
    RETURN NEW;
END $$;
CREATE TRIGGER relay_enrollment_detail_guard BEFORE INSERT ON control.relay_enrollment_changes
    FOR EACH ROW EXECUTE FUNCTION control.guard_relay_enrollment_detail();

CREATE FUNCTION control.guard_relay_enrollment_transition() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE detail control.relay_enrollment_changes%ROWTYPE;
BEGIN
    SELECT * INTO STRICT detail FROM control.relay_enrollment_changes
        WHERE actor_id=NEW.actor_id AND request_key=NEW.request_key;
    IF detail.acceptance_transaction<>pg_current_xact_id()
        OR (NEW.dataset_id,NEW.endpoint_id,NEW.access_scope_id,NEW.route_id,NEW.enabled,NEW.generation)
            IS DISTINCT FROM (detail.dataset_id,detail.endpoint_id,detail.access_scope_id,detail.route_id,detail.enabled,detail.generation) THEN
        RAISE EXCEPTION 'enrollment requires its exact atomic command' USING ERRCODE='23514';
    END IF;
    IF TG_OP='UPDATE' THEN
        IF (NEW.dataset_id,NEW.endpoint_id,NEW.access_scope_id) IS DISTINCT FROM
                (OLD.dataset_id,OLD.endpoint_id,OLD.access_scope_id)
            OR OLD.generation IS DISTINCT FROM detail.expected_generation THEN
            RAISE EXCEPTION 'enrollment identity or generation changed' USING ERRCODE='23514';
        END IF;
    ELSIF detail.expected_generation IS NOT NULL OR NEW.generation<>1 THEN
        RAISE EXCEPTION 'new enrollment requires initial generation' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER relay_enrollment_transition_guard BEFORE INSERT OR UPDATE ON control.relay_enrollments
    FOR EACH ROW EXECUTE FUNCTION control.guard_relay_enrollment_transition();

CREATE FUNCTION control.require_relay_enrollment() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE detail control.relay_enrollment_changes%ROWTYPE; command control.management_commands%ROWTYPE;
    time_us bigint;
BEGIN
    IF TG_TABLE_NAME='management_audit' THEN
        SELECT * INTO detail FROM control.relay_enrollment_changes WHERE actor_id=NEW.actor_id AND request_key=NEW.subject_id;
    ELSE
        SELECT * INTO detail FROM control.relay_enrollment_changes WHERE actor_id=NEW.actor_id AND request_key=NEW.request_key;
    END IF;
    IF detail.endpoint_id IS NULL OR detail.acceptance_transaction<>pg_current_xact_id() THEN
        RAISE EXCEPTION 'enrollment requires atomic detail' USING ERRCODE='23514';
    END IF;
    SELECT * INTO STRICT command FROM control.management_commands WHERE actor_id=detail.actor_id AND request_key=detail.request_key;
    IF command.action<>'relay.enrollment.change' OR command.dataset_id<>detail.dataset_id
        OR command.request_digest IS DISTINCT FROM control.relay_enrollment_digest(detail,command)
        OR NOT EXISTS (SELECT 1 FROM control.datasets WHERE dataset_id=detail.dataset_id
            AND config_revision=command.expected_config_revision AND policy_revision=command.policy_revision)
        OR NOT EXISTS (SELECT 1 FROM control.relay_enrollments e WHERE
            (e.dataset_id,e.endpoint_id,e.access_scope_id,e.route_id,e.enabled,e.generation,e.actor_id,e.request_key)
            IS NOT DISTINCT FROM (detail.dataset_id,detail.endpoint_id,detail.access_scope_id,detail.route_id,
                detail.enabled,detail.generation,detail.actor_id,detail.request_key))
        OR NOT EXISTS (SELECT 1 FROM control.management_audit a WHERE a.audit_id=command.audit_id
            AND (a.dataset_id,a.actor_id,a.action,a.outcome,a.subject_id,a.config_revision,a.policy_revision,a.created_at_us)
            IS NOT DISTINCT FROM (detail.dataset_id,detail.actor_id,command.action,'accepted'::text,
                detail.request_key,command.expected_config_revision,command.policy_revision,command.accepted_at_us)) THEN
        RAISE EXCEPTION 'enrollment requires exact atomic command audit and state' USING ERRCODE='23514';
    END IF;
    IF TG_TABLE_NAME='management_audit' THEN
        IF NEW.audit_id IS DISTINCT FROM command.audit_id THEN
            RAISE EXCEPTION 'enrollment audit is not its command receipt' USING ERRCODE='23514';
        END IF;
    ELSIF TG_TABLE_NAME='relay_access_contexts' THEN
        IF (NEW.dataset_id,NEW.access_scope_id) IS DISTINCT FROM (detail.dataset_id,detail.access_scope_id) THEN
            RAISE EXCEPTION 'access identity requires its creating enrollment' USING ERRCODE='23514';
        END IF;
    ELSIF TG_TABLE_NAME='network_routes' THEN
        IF (NEW.dataset_id,NEW.endpoint_id,NEW.route_id) IS DISTINCT FROM (detail.dataset_id,detail.endpoint_id,detail.route_id) THEN
            RAISE EXCEPTION 'route identity requires its creating enrollment' USING ERRCODE='23514';
        END IF;
    END IF;
    PERFORM control.require_control_owner(detail.dataset_id,detail.owner_resource_id,detail.owner_epoch,detail.owner_incarnation);
    time_us:=(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint;
    IF NOT EXISTS (SELECT 1 FROM control.local_accounts a JOIN control.browser_sessions s
        ON s.dataset_id=a.dataset_id AND s.principal_id=a.principal_id
        WHERE a.dataset_id=detail.dataset_id AND a.principal_id=detail.actor_id AND a.active
            AND a.credential_revision=detail.actor_credential_revision
            AND a.authentication_revision=detail.actor_authentication_revision AND a.grant_revision=detail.actor_grant_revision
            AND s.session_id=detail.session_id AND s.revoked_at_us IS NULL
            AND s.credential_revision=a.credential_revision AND s.authentication_revision=a.authentication_revision
            AND time_us>=s.last_seen_at_us AND time_us<LEAST(s.expires_at_us,s.idle_expires_at_us))
        OR NOT EXISTS (SELECT 1 FROM control.principal_grants WHERE dataset_id=detail.dataset_id
            AND principal_id=detail.actor_id AND action='policy.change' AND resource_kind='dataset' AND resource_id=dataset_id)
        OR NOT EXISTS (SELECT 1 FROM control.owner_epochs WHERE resource_id=detail.owner_resource_id
            AND epoch=detail.owner_epoch AND lease_expires_at_us>time_us) THEN
        RAISE EXCEPTION 'enrollment session or owner authority expired or changed before commit' USING ERRCODE='BB001';
    END IF;
    RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER relay_enrollment_command_complete AFTER INSERT ON control.management_commands
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.action='relay.enrollment.change')
    EXECUTE FUNCTION control.require_relay_enrollment();
CREATE CONSTRAINT TRIGGER relay_enrollment_audit_complete AFTER INSERT ON control.management_audit
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.action='relay.enrollment.change')
    EXECUTE FUNCTION control.require_relay_enrollment();
CREATE CONSTRAINT TRIGGER relay_enrollment_complete AFTER INSERT OR UPDATE ON control.relay_enrollments
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION control.require_relay_enrollment();
CREATE TRIGGER relay_enrollment_delete_guard BEFORE DELETE ON control.relay_enrollments
    FOR EACH ROW EXECUTE FUNCTION control.reject_history_mutation();
CREATE TRIGGER relay_enrollment_no_truncate BEFORE TRUNCATE ON control.relay_enrollments
    FOR EACH STATEMENT EXECUTE FUNCTION control.reject_history_mutation();
CREATE CONSTRAINT TRIGGER relay_access_contexts_complete AFTER INSERT ON control.relay_access_contexts
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION control.require_relay_enrollment();
CREATE TRIGGER relay_access_contexts_immutable BEFORE UPDATE OR DELETE ON control.relay_access_contexts
    FOR EACH ROW EXECUTE FUNCTION control.reject_history_mutation();
CREATE TRIGGER relay_access_contexts_no_truncate BEFORE TRUNCATE ON control.relay_access_contexts
    FOR EACH STATEMENT EXECUTE FUNCTION control.reject_history_mutation();
CREATE CONSTRAINT TRIGGER network_routes_complete AFTER INSERT ON control.network_routes
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION control.require_relay_enrollment();
CREATE TRIGGER network_routes_immutable BEFORE UPDATE OR DELETE ON control.network_routes
    FOR EACH ROW EXECUTE FUNCTION control.reject_history_mutation();
CREATE TRIGGER network_routes_no_truncate BEFORE TRUNCATE ON control.network_routes
    FOR EACH STATEMENT EXECUTE FUNCTION control.reject_history_mutation();
CREATE CONSTRAINT TRIGGER relay_enrollment_changes_complete AFTER INSERT ON control.relay_enrollment_changes
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION control.require_relay_enrollment();
CREATE TRIGGER relay_enrollment_changes_immutable BEFORE UPDATE OR DELETE ON control.relay_enrollment_changes
    FOR EACH ROW EXECUTE FUNCTION control.reject_history_mutation();
CREATE TRIGGER relay_enrollment_changes_no_truncate BEFORE TRUNCATE ON control.relay_enrollment_changes
    FOR EACH STATEMENT EXECUTE FUNCTION control.reject_history_mutation();
REVOKE ALL ON control.relay_access_contexts,control.network_routes,
    control.relay_enrollment_changes,control.relay_enrollments FROM PUBLIC;
REVOKE ALL ON FUNCTION control.relay_route_valid(bytea),
    control.relay_enrollment_digest(control.relay_enrollment_changes,control.management_commands),
    control.guard_relay_enrollment_detail(),control.guard_relay_enrollment_transition(),
    control.require_relay_enrollment() FROM PUBLIC;
