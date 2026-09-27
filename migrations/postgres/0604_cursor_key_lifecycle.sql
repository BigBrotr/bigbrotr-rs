-- Maintenance-window lifecycle. Registration/enrollment and their audits remain
-- immutable. No token issuer or worker-distribution proof is introduced here.
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
                  'job.export.accept','job.export.resume')
        AND outcome='accepted' AND config_revision IS NOT NULL AND config_revision>0
        AND policy_revision IS NOT NULL AND policy_revision>0
        AND subject_id IS NOT NULL AND reason IS NULL
        AND requester_database_role IS NULL AND requester_os_uid IS NULL)
);

CREATE TABLE control.cursor_key_transitions (
    dataset_id control.id16 NOT NULL,
    request_id control.id16 NOT NULL,
    keyring_id control.id16 NOT NULL,
    actor_id control.id16 NOT NULL,
    action text NOT NULL CHECK (action IN ('activate','revoke')),
    key_id control.id16 NOT NULL,
    expected_revision control.u64 NOT NULL,
    revision control.u64 NOT NULL CHECK (revision>0),
    resulting_active_key_id control.id16,
    request_digest control.digest32 NOT NULL,
    accepted_at_us bigint NOT NULL,
    audit_id control.id16 NOT NULL UNIQUE REFERENCES control.management_audit(audit_id),
    PRIMARY KEY (dataset_id,request_id),
    UNIQUE (dataset_id,revision),
    FOREIGN KEY (dataset_id,keyring_id) REFERENCES control.cursor_keyrings(dataset_id,keyring_id),
    FOREIGN KEY (dataset_id,actor_id) REFERENCES control.principals(dataset_id,principal_id),
    FOREIGN KEY (dataset_id,key_id) REFERENCES control.cursor_key_materials(dataset_id,key_id),
    FOREIGN KEY (dataset_id,resulting_active_key_id) REFERENCES control.cursor_key_materials(dataset_id,key_id)
);
CREATE UNIQUE INDEX one_cursor_key_revocation
    ON control.cursor_key_transitions(dataset_id,key_id) WHERE action='revoke';
CREATE INDEX first_cursor_key_activation
    ON control.cursor_key_transitions(dataset_id,key_id,revision) WHERE action='activate';
CREATE TABLE control.cursor_key_lifecycle (
    dataset_id control.id16 NOT NULL,
    key_id control.id16 NOT NULL,
    first_activation_revision control.u64,
    revoked_revision control.u64,
    decrypt_until_us bigint,
    PRIMARY KEY (dataset_id,key_id),
    FOREIGN KEY (dataset_id,key_id) REFERENCES control.cursor_key_materials(dataset_id,key_id),
    FOREIGN KEY (dataset_id,first_activation_revision) REFERENCES control.cursor_key_transitions(dataset_id,revision),
    FOREIGN KEY (dataset_id,revoked_revision) REFERENCES control.cursor_key_transitions(dataset_id,revision),
    CHECK (first_activation_revision IS NOT NULL OR revoked_revision IS NOT NULL),
    CHECK (decrypt_until_us IS NULL OR first_activation_revision IS NOT NULL)
);
CREATE INDEX cursor_required_decrypt_keys
    ON control.cursor_key_lifecycle(dataset_id,decrypt_until_us,key_id)
    WHERE revoked_revision IS NULL AND decrypt_until_us IS NOT NULL;

CREATE FUNCTION control.lock_cursor_maintenance(wanted_dataset bytea) RETURNS void
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
    IF current_setting('transaction_isolation')<>'read committed' THEN
        RAISE EXCEPTION 'cursor lifecycle requires READ COMMITTED' USING ERRCODE='25001';
    END IF;
    PERFORM pg_advisory_xact_lock_shared(1111651442,1296648018);
    IF NOT pg_try_advisory_xact_lock(1111651442,1129599564) THEN
        RAISE EXCEPTION 'cursor lifecycle requires stopped Control' USING ERRCODE='55000';
    END IF;
    PERFORM 1 FROM control.datasets WHERE dataset_id=wanted_dataset FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'cursor lifecycle requires its dataset' USING ERRCODE='23514';
    END IF;
    -- Fresh statement after exclusion and row locking. Expiry is not exit proof.
    IF EXISTS (SELECT 1 FROM control.owner_epochs WHERE dataset_id=wanted_dataset
        AND role='control' AND state<>'released') THEN
        RAISE EXCEPTION 'cursor lifecycle requires released Control history' USING ERRCODE='55000';
    END IF;
END $$;

CREATE FUNCTION control.guard_cursor_transition() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE
    previous control.cursor_key_transitions%ROWTYPE;
    expected_digest bytea;
BEGIN
    PERFORM control.lock_cursor_maintenance(NEW.dataset_id);
    IF NEW.revision IS NOT NULL OR NEW.resulting_active_key_id IS NOT NULL THEN
        RAISE EXCEPTION 'cursor lifecycle result is allocated by Control' USING ERRCODE='23514';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM control.principals p JOIN control.datasets d USING(dataset_id)
        WHERE p.dataset_id=NEW.dataset_id AND p.principal_id=NEW.actor_id
            AND p.kind='installation' AND p.database_role=SESSION_USER
            AND d.config_revision IS NOT NULL) THEN
        RAISE EXCEPTION 'cursor lifecycle requires installation attribution' USING ERRCODE='23514';
    END IF;
    SELECT * INTO previous FROM control.cursor_key_transitions
        WHERE dataset_id=NEW.dataset_id ORDER BY revision DESC LIMIT 1;
    IF NEW.expected_revision IS DISTINCT FROM coalesce(previous.revision,0) THEN
        RAISE EXCEPTION 'cursor lifecycle predecessor changed' USING ERRCODE='23514';
    END IF;
    -- The immutable index also sees preceding rows in a multirow INSERT, before
    -- their AFTER triggers have populated the mutable projection.
    IF EXISTS (SELECT 1 FROM control.cursor_key_transitions
        WHERE dataset_id=NEW.dataset_id AND key_id=NEW.key_id AND action='revoke') THEN
        RAISE EXCEPTION 'cursor revocation is irreversible' USING ERRCODE='23514';
    END IF;
    NEW.revision:=NEW.expected_revision+1;
    IF NEW.action='activate' THEN
        IF NEW.key_id IS NOT DISTINCT FROM previous.resulting_active_key_id THEN
            RAISE EXCEPTION 'cursor key is already active' USING ERRCODE='23514';
        END IF;
        NEW.resulting_active_key_id:=NEW.key_id;
    ELSE
        NEW.resulting_active_key_id:=CASE WHEN previous.resulting_active_key_id=NEW.key_id
            THEN NULL ELSE previous.resulting_active_key_id END;
    END IF;
    expected_digest:=sha256(convert_to('bigbrotr/cursor-key-lifecycle/1','UTF8') || decode('00','hex')
        || NEW.dataset_id || NEW.keyring_id || NEW.actor_id || NEW.request_id
        || CASE WHEN NEW.action='activate' THEN decode('01','hex') ELSE decode('02','hex') END
        || NEW.key_id || control.command_u64_bytes(NEW.expected_revision));
    IF NEW.request_digest IS DISTINCT FROM expected_digest THEN
        RAISE EXCEPTION 'cursor lifecycle request digest differs' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER cursor_transition_guard BEFORE INSERT ON control.cursor_key_transitions
    FOR EACH ROW EXECUTE FUNCTION control.guard_cursor_transition();

CREATE FUNCTION control.guard_cursor_lifecycle_projection() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE current_transition control.cursor_key_transitions%ROWTYPE;
BEGIN
    -- Only the receipt's AFTER trigger produces these effects. The exact receipt
    -- checks below also prevent recycling historical transitions via other triggers.
    IF pg_trigger_depth()<>2 THEN
        RAISE EXCEPTION 'cursor projection requires its transition' USING ERRCODE='23514';
    END IF;
    SELECT * INTO STRICT current_transition FROM control.cursor_key_transitions
        WHERE dataset_id=NEW.dataset_id
            AND revision=coalesce(NEW.revoked_revision,NEW.first_activation_revision);
    IF NEW.key_id IS DISTINCT FROM current_transition.key_id THEN
        RAISE EXCEPTION 'cursor projection names another key' USING ERRCODE='23514';
    END IF;
    IF TG_OP='INSERT' THEN
        IF NEW.decrypt_until_us IS NOT NULL
            OR NEW.first_activation_revision IS DISTINCT FROM
                (SELECT revision FROM control.cursor_key_transitions
                 WHERE dataset_id=NEW.dataset_id AND key_id=NEW.key_id AND action='activate'
                 ORDER BY revision LIMIT 1)
            OR NEW.first_activation_revision IS DISTINCT FROM
                CASE WHEN current_transition.action='activate' THEN current_transition.revision ELSE NULL END
            OR NEW.revoked_revision IS DISTINCT FROM
                CASE WHEN current_transition.action='revoke' THEN current_transition.revision ELSE NULL END THEN
            RAISE EXCEPTION 'cursor projection differs from first transition' USING ERRCODE='23514';
        END IF;
    ELSE
        IF (NEW.dataset_id,NEW.key_id,NEW.first_activation_revision,NEW.decrypt_until_us)
                IS DISTINCT FROM (OLD.dataset_id,OLD.key_id,OLD.first_activation_revision,OLD.decrypt_until_us)
            OR OLD.revoked_revision IS NOT NULL
            OR current_transition.action<>'revoke'
            OR NEW.revoked_revision IS DISTINCT FROM current_transition.revision THEN
            RAISE EXCEPTION 'cursor projection permits only irreversible revocation' USING ERRCODE='23514';
        END IF;
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER cursor_lifecycle_projection_guard BEFORE INSERT OR UPDATE ON control.cursor_key_lifecycle
    FOR EACH ROW EXECUTE FUNCTION control.guard_cursor_lifecycle_projection();

CREATE FUNCTION control.capture_cursor_transition() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
    IF TG_RELID<>'control.cursor_key_transitions'::regclass OR TG_OP<>'INSERT' THEN
        RAISE EXCEPTION 'cursor capture requires its actual receipt relation' USING ERRCODE='23514';
    END IF;
    IF NEW.action='activate' THEN
        -- Reactivation of a decrypt-only key preserves its first activation and
        -- retention deadline. A revoked key was already refused before INSERT.
        IF NOT EXISTS (SELECT 1 FROM control.cursor_key_lifecycle
            WHERE dataset_id=NEW.dataset_id AND key_id=NEW.key_id) THEN
            INSERT INTO control.cursor_key_lifecycle(dataset_id,key_id,first_activation_revision)
                VALUES(NEW.dataset_id,NEW.key_id,NEW.revision);
        END IF;
    ELSE
        UPDATE control.cursor_key_lifecycle SET revoked_revision=NEW.revision
            WHERE dataset_id=NEW.dataset_id AND key_id=NEW.key_id;
        IF NOT FOUND THEN
            INSERT INTO control.cursor_key_lifecycle(dataset_id,key_id,revoked_revision)
                VALUES(NEW.dataset_id,NEW.key_id,NEW.revision);
        END IF;
    END IF;
    RETURN NULL;
END $$;
-- Run projection effects before immediate constraint checks as well as deferred
-- ones. Audit checks bind immutable receipts, never an earlier mutable state.
CREATE TRIGGER capture_cursor_transition AFTER INSERT ON control.cursor_key_transitions
    FOR EACH ROW EXECUTE FUNCTION control.capture_cursor_transition();

CREATE FUNCTION control.require_cursor_transition_audit() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE transition control.cursor_key_transitions%ROWTYPE;
BEGIN
    IF TG_TABLE_NAME='management_audit' THEN
        SELECT * INTO STRICT transition FROM control.cursor_key_transitions WHERE audit_id=NEW.audit_id;
    ELSE
        transition:=NEW;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM control.management_audit a
        WHERE a.audit_id=transition.audit_id
            AND (a.dataset_id,a.actor_id,a.action,a.outcome,a.subject_id,a.created_at_us)
                IS NOT DISTINCT FROM (transition.dataset_id,transition.actor_id,
                    'cursor-key.' || transition.action,'accepted'::text,transition.key_id,transition.accepted_at_us)
            AND a.config_revision IS NULL AND a.policy_revision IS NULL
            AND a.requester_database_role=SESSION_USER AND a.requester_os_uid IS NOT NULL) THEN
        RAISE EXCEPTION 'cursor lifecycle requires its exact audit' USING ERRCODE='23514';
    END IF;
    RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER cursor_transition_complete AFTER INSERT ON control.cursor_key_transitions
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION control.require_cursor_transition_audit();
CREATE CONSTRAINT TRIGGER cursor_transition_audit_complete AFTER INSERT ON control.management_audit
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
    WHEN (NEW.action IN ('cursor-key.activate','cursor-key.revoke'))
    EXECUTE FUNCTION control.require_cursor_transition_audit();
CREATE TRIGGER immutable_cursor_transition BEFORE UPDATE OR DELETE ON control.cursor_key_transitions
    FOR EACH ROW EXECUTE FUNCTION control.reject_history_mutation();
CREATE TRIGGER cursor_transition_truncate_guard BEFORE TRUNCATE ON control.cursor_key_transitions
    FOR EACH STATEMENT EXECUTE FUNCTION control.reject_history_mutation();
CREATE TRIGGER cursor_lifecycle_delete_guard BEFORE DELETE ON control.cursor_key_lifecycle
    FOR EACH ROW EXECUTE FUNCTION control.reject_history_mutation();
CREATE TRIGGER cursor_lifecycle_truncate_guard BEFORE TRUNCATE ON control.cursor_key_lifecycle
    FOR EACH STATEMENT EXECUTE FUNCTION control.reject_history_mutation();

REVOKE ALL ON control.cursor_key_transitions,control.cursor_key_lifecycle FROM PUBLIC;
REVOKE ALL ON FUNCTION control.lock_cursor_maintenance(bytea),control.guard_cursor_transition(),
    control.guard_cursor_lifecycle_projection(),control.capture_cursor_transition(),
    control.require_cursor_transition_audit() FROM PUBLIC;
