-- Preserve the original registration snapshot and all its receipts. This
-- registry is the permanent key namespace for subsequent lifecycle consumers.
-- Table locks also exclude trusted direct SQL during the initial backfill.
LOCK TABLE control.datasets,control.cursor_keyrings,control.cursor_keys,control.management_audit IN SHARE ROW EXCLUSIVE MODE;
ALTER TABLE control.management_audit DROP CONSTRAINT management_audit_shape;
ALTER TABLE control.management_audit ADD CONSTRAINT management_audit_shape CHECK (
    (policy_revision IS NULL AND (
        (action='dataset.initialize' AND outcome='accepted' AND config_revision IS NOT NULL AND config_revision=1 AND subject_id IS NULL AND reason IS NULL)
        OR (action IN ('owner-token.issue','owner-token.revoke','owner.enroll','cursor-keyring.register','cursor-key.enroll') AND outcome='accepted' AND config_revision IS NULL AND subject_id IS NOT NULL AND reason IS NULL AND requester_database_role IS NOT NULL AND requester_os_uid IS NOT NULL)
        OR (action='owner.enroll' AND outcome='rejected' AND config_revision IS NULL AND subject_id IS NOT NULL AND reason IS NOT NULL AND reason IN ('invalid-token','expired','exhausted','terminal') AND requester_database_role IS NOT NULL AND requester_os_uid IS NOT NULL)
        OR (action='policy.activate-initial' AND outcome='accepted' AND config_revision IS NOT NULL AND config_revision>0 AND subject_id IS NOT NULL AND reason IS NULL AND requester_database_role IS NOT NULL AND requester_os_uid IS NOT NULL)
    ))
    OR (action IN ('browser.session.issue','browser.session.revoke','browser.session.reauthenticate','scope-grant.change','job.export.accept','job.export.resume') AND outcome='accepted'
        AND config_revision IS NOT NULL AND config_revision>0 AND policy_revision IS NOT NULL AND policy_revision>0
        AND subject_id IS NOT NULL AND reason IS NULL AND requester_database_role IS NULL AND requester_os_uid IS NULL)
);

CREATE TABLE control.cursor_key_materials (
    dataset_id control.id16 NOT NULL,
    keyring_id control.id16 NOT NULL,
    key_id control.id16 NOT NULL,
    material_digest control.digest32 NOT NULL,
    origin text NOT NULL CHECK (origin IN ('initial','additional')),
    enrolled_at_us bigint NOT NULL,
    actor_id control.id16 NOT NULL,
    audit_id control.id16 NOT NULL REFERENCES control.management_audit(audit_id),
    PRIMARY KEY (dataset_id,key_id),
    FOREIGN KEY (dataset_id,keyring_id) REFERENCES control.cursor_keyrings(dataset_id,keyring_id),
    FOREIGN KEY (dataset_id,actor_id) REFERENCES control.principals(dataset_id,principal_id)
);
CREATE UNIQUE INDEX one_additional_key_audit ON control.cursor_key_materials(audit_id)
    WHERE origin='additional';
CREATE UNIQUE INDEX one_cursor_enrollment_audit ON control.management_audit(dataset_id,subject_id)
    WHERE action='cursor-key.enroll';

-- The existing immutable snapshot is the authoritative initial provenance.
-- One registration audit may describe several initial keys; do not invent audits.
INSERT INTO control.cursor_key_materials(dataset_id,keyring_id,key_id,material_digest,origin,enrolled_at_us,actor_id,audit_id)
SELECT k.dataset_id,k.keyring_id,k.key_id,k.material_digest,'initial',r.registered_at_us,r.actor_id,r.audit_id
FROM control.cursor_keys k JOIN control.cursor_keyrings r USING(dataset_id,keyring_id);

CREATE FUNCTION control.guard_cursor_key_material() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
    IF current_setting('transaction_isolation')<>'read committed' THEN
        RAISE EXCEPTION 'cursor enrollment requires READ COMMITTED' USING ERRCODE='25001';
    END IF;
    PERFORM 1 FROM control.datasets WHERE dataset_id=NEW.dataset_id FOR UPDATE;
    IF NOT FOUND OR NOT EXISTS (SELECT 1 FROM control.principals p
        WHERE p.dataset_id=NEW.dataset_id AND p.principal_id=NEW.actor_id
            AND p.kind='installation' AND p.database_role=SESSION_USER) THEN
        RAISE EXCEPTION 'cursor enrollment requires attributable installation credentials' USING ERRCODE='23514';
    END IF;
    IF NEW.origin='initial' THEN
        IF NOT EXISTS (SELECT 1 FROM control.cursor_keys k
            JOIN control.cursor_keyrings r USING(dataset_id,keyring_id)
            WHERE (k.dataset_id,k.keyring_id,k.key_id,k.material_digest,r.registered_at_us,r.actor_id,r.audit_id)
                IS NOT DISTINCT FROM (NEW.dataset_id,NEW.keyring_id,NEW.key_id,NEW.material_digest,NEW.enrolled_at_us,NEW.actor_id,NEW.audit_id)) THEN
            RAISE EXCEPTION 'initial cursor binding requires exact registration provenance' USING ERRCODE='23514';
        END IF;
    ELSIF EXISTS (SELECT 1 FROM control.cursor_keys WHERE dataset_id=NEW.dataset_id AND key_id=NEW.key_id) THEN
        RAISE EXCEPTION 'initial cursor key cannot be enrolled as additional' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER cursor_key_material_guard BEFORE INSERT ON control.cursor_key_materials
    FOR EACH ROW EXECUTE FUNCTION control.guard_cursor_key_material();

CREATE FUNCTION control.capture_initial_cursor_material() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
    INSERT INTO control.cursor_key_materials(dataset_id,keyring_id,key_id,material_digest,origin,enrolled_at_us,actor_id,audit_id)
    SELECT NEW.dataset_id,NEW.keyring_id,NEW.key_id,NEW.material_digest,'initial',r.registered_at_us,r.actor_id,r.audit_id
    FROM control.cursor_keyrings r WHERE r.dataset_id=NEW.dataset_id AND r.keyring_id=NEW.keyring_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'initial cursor material requires its ring' USING ERRCODE='23514';
    END IF;
    RETURN NULL;
END $$;
CREATE TRIGGER capture_initial_cursor_material AFTER INSERT ON control.cursor_keys
    FOR EACH ROW EXECUTE FUNCTION control.capture_initial_cursor_material();

CREATE FUNCTION control.require_cursor_enrollment() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE enrolled control.cursor_key_materials%ROWTYPE;
BEGIN
    IF TG_TABLE_NAME='management_audit' THEN
        SELECT * INTO STRICT enrolled FROM control.cursor_key_materials
            WHERE dataset_id=NEW.dataset_id AND key_id=NEW.subject_id AND origin='additional';
    ELSE
        enrolled:=NEW;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM control.management_audit a
        WHERE a.audit_id=enrolled.audit_id
            AND (a.dataset_id,a.actor_id,a.action,a.outcome,a.subject_id,a.created_at_us)
                IS NOT DISTINCT FROM (enrolled.dataset_id,enrolled.actor_id,
                    'cursor-key.enroll'::text,'accepted'::text,enrolled.key_id,enrolled.enrolled_at_us)
            AND a.config_revision IS NULL AND a.policy_revision IS NULL
            AND a.requester_database_role=SESSION_USER AND a.requester_os_uid IS NOT NULL) THEN
        RAISE EXCEPTION 'additional cursor enrollment requires its exact audit' USING ERRCODE='23514';
    END IF;
    IF TG_TABLE_NAME='management_audit' AND NEW.audit_id IS DISTINCT FROM enrolled.audit_id THEN
        RAISE EXCEPTION 'cursor enrollment audit requires its key' USING ERRCODE='23514';
    END IF;
    RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER cursor_enrollment_complete AFTER INSERT ON control.cursor_key_materials
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.origin='additional')
    EXECUTE FUNCTION control.require_cursor_enrollment();
CREATE CONSTRAINT TRIGGER cursor_enrollment_audit_complete AFTER INSERT ON control.management_audit
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.action='cursor-key.enroll')
    EXECUTE FUNCTION control.require_cursor_enrollment();
CREATE TRIGGER immutable_cursor_key_material BEFORE UPDATE OR DELETE ON control.cursor_key_materials
    FOR EACH ROW EXECUTE FUNCTION control.reject_history_mutation();
CREATE TRIGGER cursor_key_material_truncate_guard BEFORE TRUNCATE ON control.cursor_key_materials
    FOR EACH STATEMENT EXECUTE FUNCTION control.reject_history_mutation();
REVOKE ALL ON control.cursor_key_materials FROM PUBLIC;
REVOKE ALL ON FUNCTION control.guard_cursor_key_material(),control.capture_initial_cursor_material(),control.require_cursor_enrollment() FROM PUBLIC;
