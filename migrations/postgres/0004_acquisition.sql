-- Append after0020. Collection intent is private operational data, not a grant,
-- archive dictionary or evidence of completed acquisition. Existing rows stay put.
CREATE FUNCTION control.acquisition_selector_candidate(document bytea) RETURNS bytea
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE SET search_path = pg_catalog
AS $$ SELECT sha256(document) $$;

CREATE TABLE control.acquisition_selectors (
    selector_id control.id16 PRIMARY KEY,
    dataset_id control.id16 NOT NULL REFERENCES control.datasets(dataset_id),
    profile_version smallint NOT NULL CHECK (profile_version = 1),
    document bytea NOT NULL CHECK (octet_length(document) BETWEEN 1 AND 2097152),
    candidate_digest bytea GENERATED ALWAYS AS
        (control.acquisition_selector_candidate(document)) STORED,
    registered_at_us bigint NOT NULL,
    config_revision control.u64 NOT NULL CHECK (config_revision > 0),
    policy_revision control.u64 NOT NULL CHECK (policy_revision > 0),
    owner_resource_id control.id16 NOT NULL,
    owner_epoch control.u64 NOT NULL,
    owner_incarnation control.id16 NOT NULL,
    owner_role text NOT NULL DEFAULT 'control' CHECK (owner_role = 'control'),
    UNIQUE (dataset_id, selector_id),
    FOREIGN KEY (dataset_id, config_revision)
        REFERENCES control.config_revisions(dataset_id, revision),
    FOREIGN KEY (dataset_id, policy_revision)
        REFERENCES control.policy_revisions(dataset_id, revision),
    FOREIGN KEY (dataset_id, owner_resource_id, owner_epoch, owner_role, owner_incarnation)
        REFERENCES control.owner_epochs(dataset_id, resource_id, epoch, role, process_incarnation)
);
CREATE INDEX acquisition_selector_candidates ON control.acquisition_selectors
    (dataset_id, profile_version, candidate_digest);

-- Make the full existing access-to-visibility binding available to a composite
-- FK. The prior unique dataset/access key already prevents duplicate rows.
ALTER TABLE control.relay_access_contexts ADD CONSTRAINT relay_access_visibility
    UNIQUE (dataset_id, access_scope_id, dictionary_kind, allocation_domain, ordinal);

CREATE TABLE control.acquisition_scopes (
    scope_id control.id16 PRIMARY KEY,
    dataset_id control.id16 NOT NULL,
    endpoint_id control.id16 NOT NULL,
    selector_id control.id16 NOT NULL,
    access_scope_id control.id16 NOT NULL,
    visibility_kind smallint NOT NULL DEFAULT 4 CHECK (visibility_kind = 4),
    visibility_domain control.id16 NOT NULL,
    visibility_ordinal control.u64 NOT NULL CHECK (visibility_ordinal > 0),
    policy_revision control.u64 NOT NULL CHECK (policy_revision > 0),
    config_revision control.u64 NOT NULL CHECK (config_revision > 0),
    registered_at_us bigint NOT NULL,
    owner_resource_id control.id16 NOT NULL,
    owner_epoch control.u64 NOT NULL,
    owner_incarnation control.id16 NOT NULL,
    owner_role text NOT NULL DEFAULT 'control' CHECK (owner_role = 'control'),
    UNIQUE (dataset_id, scope_id),
    UNIQUE (dataset_id, endpoint_id, selector_id, access_scope_id,
        visibility_kind, visibility_domain, visibility_ordinal, policy_revision),
    FOREIGN KEY (dataset_id, selector_id)
        REFERENCES control.acquisition_selectors(dataset_id, selector_id),
    FOREIGN KEY (dataset_id, endpoint_id, access_scope_id)
        REFERENCES control.relay_enrollments(dataset_id, endpoint_id, access_scope_id),
    FOREIGN KEY (dataset_id, access_scope_id, visibility_kind, visibility_domain, visibility_ordinal)
        REFERENCES control.relay_access_contexts(dataset_id, access_scope_id, dictionary_kind, allocation_domain, ordinal),
    FOREIGN KEY (dataset_id, config_revision)
        REFERENCES control.config_revisions(dataset_id, revision),
    FOREIGN KEY (dataset_id, policy_revision)
        REFERENCES control.policy_revisions(dataset_id, revision),
    FOREIGN KEY (dataset_id, owner_resource_id, owner_epoch, owner_role, owner_incarnation)
        REFERENCES control.owner_epochs(dataset_id, resource_id, epoch, role, process_incarnation)
);

CREATE FUNCTION control.guard_acquisition_registration() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    head control.datasets%ROWTYPE;
    candidate bytea;
    bucket_size integer;
BEGIN
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'acquisition registration requires READ COMMITTED' USING ERRCODE = '25001';
    END IF;
    PERFORM control.require_control_owner(
        NEW.dataset_id, NEW.owner_resource_id, NEW.owner_epoch, NEW.owner_incarnation);
    SELECT * INTO STRICT head FROM control.datasets
        WHERE dataset_id = NEW.dataset_id FOR UPDATE;
    IF (NEW.config_revision, NEW.policy_revision)
            IS DISTINCT FROM (head.config_revision, head.policy_revision) THEN
        RAISE EXCEPTION 'acquisition registration requires current policy' USING ERRCODE = '23514';
    END IF;
    IF TG_TABLE_NAME = 'acquisition_selectors' THEN
        -- The generated column is not populated until AFTER this trigger.
        candidate := control.acquisition_selector_candidate(NEW.document);
        SELECT count(*) INTO bucket_size FROM (
            SELECT selector_id FROM control.acquisition_selectors
            WHERE dataset_id = NEW.dataset_id AND profile_version = NEW.profile_version
                AND candidate_digest = candidate LIMIT 65
        ) AS bounded_bucket;
        IF bucket_size >= 64 THEN
            RAISE EXCEPTION 'selector candidate bucket capacity exhausted' USING ERRCODE = '54000';
        END IF;
        IF EXISTS (SELECT 1 FROM control.acquisition_selectors
            WHERE dataset_id = NEW.dataset_id AND profile_version = NEW.profile_version
                AND candidate_digest = candidate AND document = NEW.document) THEN
            RAISE EXCEPTION 'selector is already registered' USING ERRCODE = '23505';
        END IF;
    END IF;
    RETURN NEW;
END $$;

CREATE TRIGGER acquisition_selector_guard BEFORE INSERT ON control.acquisition_selectors
    FOR EACH ROW EXECUTE FUNCTION control.guard_acquisition_registration();
CREATE TRIGGER acquisition_scope_guard BEFORE INSERT ON control.acquisition_scopes
    FOR EACH ROW EXECUTE FUNCTION control.guard_acquisition_registration();

-- Reuse the existing final registration fence: its only requirements are the
-- same dataset/config/policy and Control attribution columns in each new row.
CREATE CONSTRAINT TRIGGER acquisition_selector_complete AFTER INSERT ON control.acquisition_selectors
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
    EXECUTE FUNCTION control.require_relay_registration_commit();
CREATE CONSTRAINT TRIGGER acquisition_scope_complete AFTER INSERT ON control.acquisition_scopes
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
    EXECUTE FUNCTION control.require_relay_registration_commit();

CREATE TRIGGER acquisition_selector_immutable BEFORE UPDATE OR DELETE ON control.acquisition_selectors
    FOR EACH ROW EXECUTE FUNCTION control.reject_history_mutation();
CREATE TRIGGER acquisition_selector_no_truncate BEFORE TRUNCATE ON control.acquisition_selectors
    FOR EACH STATEMENT EXECUTE FUNCTION control.reject_history_mutation();
CREATE TRIGGER acquisition_scope_immutable BEFORE UPDATE OR DELETE ON control.acquisition_scopes
    FOR EACH ROW EXECUTE FUNCTION control.reject_history_mutation();
CREATE TRIGGER acquisition_scope_no_truncate BEFORE TRUNCATE ON control.acquisition_scopes
    FOR EACH STATEMENT EXECUTE FUNCTION control.reject_history_mutation();

REVOKE ALL ON control.acquisition_selectors, control.acquisition_scopes FROM PUBLIC;
REVOKE ALL ON FUNCTION control.acquisition_selector_candidate(bytea),
    control.guard_acquisition_registration() FROM PUBLIC;
