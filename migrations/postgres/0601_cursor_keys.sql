-- Initial key registration is permanent compatibility metadata, never issuance.
-- This fresh relation adopts no prior keys and changes no historical row values.
ALTER TABLE control.management_audit DROP CONSTRAINT management_audit_shape;
ALTER TABLE control.management_audit ADD CONSTRAINT management_audit_shape CHECK (
    (policy_revision IS NULL AND (
        (action='dataset.initialize' AND outcome='accepted' AND config_revision IS NOT NULL AND config_revision=1 AND subject_id IS NULL AND reason IS NULL)
        OR (action IN ('owner-token.issue','owner-token.revoke','owner.enroll','cursor-keyring.register') AND outcome='accepted' AND config_revision IS NULL AND subject_id IS NOT NULL AND reason IS NULL AND requester_database_role IS NOT NULL AND requester_os_uid IS NOT NULL)
        OR (action='owner.enroll' AND outcome='rejected' AND config_revision IS NULL AND subject_id IS NOT NULL AND reason IS NOT NULL AND reason IN ('invalid-token','expired','exhausted','terminal') AND requester_database_role IS NOT NULL AND requester_os_uid IS NOT NULL)
        OR (action='policy.activate-initial' AND outcome='accepted' AND config_revision IS NOT NULL AND config_revision>0 AND subject_id IS NOT NULL AND reason IS NULL AND requester_database_role IS NOT NULL AND requester_os_uid IS NOT NULL)
    ))
    OR (action IN ('browser.session.issue','browser.session.revoke','browser.session.reauthenticate','scope-grant.change','job.export.accept','job.export.resume') AND outcome='accepted'
        AND config_revision IS NOT NULL AND config_revision>0 AND policy_revision IS NOT NULL AND policy_revision>0
        AND subject_id IS NOT NULL AND reason IS NULL AND requester_database_role IS NULL AND requester_os_uid IS NULL)
);
CREATE TABLE control.cursor_keyrings (
    dataset_id control.id16 PRIMARY KEY REFERENCES control.datasets(dataset_id),
    keyring_id control.id16 NOT NULL UNIQUE,
    file_format integer NOT NULL CHECK (file_format=1),
    initial_file_digest control.digest32 NOT NULL,
    key_count smallint NOT NULL CHECK (key_count BETWEEN 1 AND 16),
    registered_at_us bigint NOT NULL,
    actor_id control.id16 NOT NULL,
    audit_id control.id16 NOT NULL UNIQUE REFERENCES control.management_audit(audit_id),
    FOREIGN KEY (dataset_id,actor_id) REFERENCES control.principals(dataset_id,principal_id),
    UNIQUE (dataset_id,keyring_id)
);
CREATE TABLE control.cursor_keys (
    dataset_id control.id16 NOT NULL,
    keyring_id control.id16 NOT NULL,
    key_id control.id16 NOT NULL,
    registration_position smallint NOT NULL CHECK (registration_position BETWEEN 1 AND 16),
    material_digest control.digest32 NOT NULL,
    PRIMARY KEY (dataset_id,key_id),
    UNIQUE (dataset_id,registration_position),
    FOREIGN KEY (dataset_id,keyring_id) REFERENCES control.cursor_keyrings(dataset_id,keyring_id)
);
CREATE UNIQUE INDEX one_cursor_registration_audit ON control.management_audit(dataset_id)
    WHERE action='cursor-keyring.register';

CREATE FUNCTION control.guard_cursor_registration() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
    IF current_setting('transaction_isolation')<>'read committed' THEN
        RAISE EXCEPTION 'cursor registration requires READ COMMITTED' USING ERRCODE='25001';
    END IF;
    -- Shared with typed Control work. Acquire before reading initial metadata;
    -- a concurrent registration/retry observes the preceding committed result.
    PERFORM 1 FROM control.datasets WHERE dataset_id=NEW.dataset_id FOR UPDATE;
    IF NOT FOUND OR NOT EXISTS (SELECT 1 FROM control.principals p
        WHERE p.dataset_id=NEW.dataset_id AND p.principal_id=NEW.actor_id
            AND p.kind='installation' AND p.database_role=SESSION_USER) THEN
        RAISE EXCEPTION 'cursor registration requires attributable installation credentials' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER cursor_registration_guard BEFORE INSERT ON control.cursor_keyrings
    FOR EACH ROW EXECUTE FUNCTION control.guard_cursor_registration();

CREATE FUNCTION control.require_cursor_registration() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE registered control.cursor_keyrings%ROWTYPE;
BEGIN
    SELECT * INTO STRICT registered FROM control.cursor_keyrings WHERE dataset_id=NEW.dataset_id;
    IF NOT EXISTS (SELECT 1 FROM control.management_audit a
        WHERE a.audit_id=registered.audit_id
            AND (a.dataset_id,a.actor_id,a.action,a.outcome,a.subject_id,a.created_at_us)
                IS NOT DISTINCT FROM (registered.dataset_id,registered.actor_id,
                    'cursor-keyring.register'::text,'accepted'::text,registered.keyring_id,registered.registered_at_us)
            AND a.config_revision IS NULL AND a.policy_revision IS NULL
            AND a.requester_database_role=SESSION_USER AND a.requester_os_uid IS NOT NULL)
        OR (SELECT count(*) FROM control.cursor_keys WHERE dataset_id=registered.dataset_id)<>registered.key_count
        OR EXISTS (SELECT 1 FROM control.cursor_keys WHERE dataset_id=registered.dataset_id
            AND registration_position>registered.key_count) THEN
        RAISE EXCEPTION 'cursor registration requires every ordered key and exact audit' USING ERRCODE='23514';
    END IF;
    IF TG_TABLE_NAME='management_audit' THEN
        IF NEW.audit_id IS DISTINCT FROM registered.audit_id THEN
            RAISE EXCEPTION 'cursor audit requires its registration' USING ERRCODE='23514';
        END IF;
    END IF;
    RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER cursor_keyring_complete AFTER INSERT ON control.cursor_keyrings
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION control.require_cursor_registration();
CREATE CONSTRAINT TRIGGER cursor_key_complete AFTER INSERT ON control.cursor_keys
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION control.require_cursor_registration();
CREATE CONSTRAINT TRIGGER cursor_keyring_audit_complete AFTER INSERT ON control.management_audit
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.action='cursor-keyring.register')
    EXECUTE FUNCTION control.require_cursor_registration();
CREATE TRIGGER immutable_cursor_keyring BEFORE UPDATE OR DELETE ON control.cursor_keyrings
    FOR EACH ROW EXECUTE FUNCTION control.reject_history_mutation();
CREATE TRIGGER immutable_cursor_key BEFORE UPDATE OR DELETE ON control.cursor_keys
    FOR EACH ROW EXECUTE FUNCTION control.reject_history_mutation();
CREATE TRIGGER cursor_keyring_truncate_guard BEFORE TRUNCATE ON control.cursor_keyrings
    FOR EACH STATEMENT EXECUTE FUNCTION control.reject_history_mutation();
CREATE TRIGGER cursor_key_truncate_guard BEFORE TRUNCATE ON control.cursor_keys
    FOR EACH STATEMENT EXECUTE FUNCTION control.reject_history_mutation();
REVOKE ALL ON control.cursor_keyrings,control.cursor_keys FROM PUBLIC;
REVOKE ALL ON FUNCTION control.guard_cursor_registration(),control.require_cursor_registration() FROM PUBLIC;
