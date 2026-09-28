-- First operational relay identity consumer. Appended after0816, not inserted
-- into the already applied prefix. Registration grants no enrollment or dialing.
-- Canonical UTF-8 bytes avoid collation-dependent identity and oversized B-tree
-- keys. The Rust Endpoint normalizer owns canonicality; SQL owns exact equality.
CREATE FUNCTION control.relay_endpoint_candidate(url_bytes bytea) RETURNS bytea
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE SET search_path = pg_catalog
AS $$ SELECT sha256(url_bytes) $$;

CREATE TABLE control.relay_endpoints (
    endpoint_id control.id16 PRIMARY KEY,
    dataset_id control.id16 NOT NULL REFERENCES control.datasets(dataset_id),
    normalization_version smallint NOT NULL CHECK (normalization_version = 1),
    url_bytes bytea NOT NULL CHECK (octet_length(url_bytes) BETWEEN 1 AND 8192),
    candidate_digest bytea GENERATED ALWAYS AS
        (control.relay_endpoint_candidate(url_bytes)) STORED,
    registered_at_us bigint NOT NULL,
    config_revision control.u64 NOT NULL,
    policy_revision control.u64 NOT NULL,
    owner_resource_id control.id16 NOT NULL,
    owner_epoch control.u64 NOT NULL,
    owner_incarnation control.id16 NOT NULL,
    owner_role text NOT NULL DEFAULT 'control' CHECK (owner_role = 'control'),
    UNIQUE (dataset_id, endpoint_id),
    FOREIGN KEY (dataset_id, config_revision)
        REFERENCES control.config_revisions(dataset_id, revision),
    FOREIGN KEY (dataset_id, policy_revision)
        REFERENCES control.policy_revisions(dataset_id, revision),
    FOREIGN KEY (dataset_id, owner_resource_id, owner_epoch, owner_role, owner_incarnation)
        REFERENCES control.owner_epochs(dataset_id, resource_id, epoch, role, process_incarnation)
);
CREATE INDEX relay_endpoint_candidates ON control.relay_endpoints
    (dataset_id, normalization_version, candidate_digest);

CREATE FUNCTION control.guard_relay_endpoint() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    head control.datasets%ROWTYPE;
    candidate bytea;
    bucket_size integer;
BEGIN
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'relay registration requires READ COMMITTED' USING ERRCODE = '25001';
    END IF;
    PERFORM control.require_control_owner(
        NEW.dataset_id, NEW.owner_resource_id, NEW.owner_epoch, NEW.owner_incarnation);
    SELECT * INTO STRICT head FROM control.datasets
        WHERE dataset_id = NEW.dataset_id FOR UPDATE;
    IF (NEW.config_revision, NEW.policy_revision)
            IS DISTINCT FROM (head.config_revision, head.policy_revision) THEN
        RAISE EXCEPTION 'relay registration requires current policy' USING ERRCODE = '23514';
    END IF;
    -- Generated columns are computed AFTER BEFORE triggers. Recompute the same
    -- candidate here; never treat its equality as endpoint equality.
    candidate := control.relay_endpoint_candidate(NEW.url_bytes);
    SELECT count(*) INTO bucket_size FROM (
        SELECT endpoint_id FROM control.relay_endpoints
        WHERE dataset_id = NEW.dataset_id AND normalization_version = NEW.normalization_version
            AND candidate_digest = candidate LIMIT 65
    ) AS bounded_bucket;
    IF bucket_size >= 64 THEN
        RAISE EXCEPTION 'relay candidate bucket capacity exhausted' USING ERRCODE = '54000';
    END IF;
    IF EXISTS (SELECT 1 FROM control.relay_endpoints
        WHERE dataset_id = NEW.dataset_id AND normalization_version = NEW.normalization_version
            AND candidate_digest = candidate AND url_bytes = NEW.url_bytes) THEN
        RAISE EXCEPTION 'relay endpoint is already registered' USING ERRCODE = '23505';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER relay_endpoint_guard BEFORE INSERT ON control.relay_endpoints
    FOR EACH ROW EXECUTE FUNCTION control.guard_relay_endpoint();
CREATE TRIGGER relay_endpoint_immutable BEFORE UPDATE OR DELETE ON control.relay_endpoints
    FOR EACH ROW EXECUTE FUNCTION control.reject_history_mutation();
CREATE TRIGGER relay_endpoint_no_truncate BEFORE TRUNCATE ON control.relay_endpoints
    FOR EACH STATEMENT EXECUTE FUNCTION control.reject_history_mutation();

CREATE FUNCTION control.require_relay_registration_commit() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    PERFORM control.require_control_owner(
        NEW.dataset_id, NEW.owner_resource_id, NEW.owner_epoch, NEW.owner_incarnation);
    IF NOT EXISTS (SELECT 1 FROM control.datasets
        WHERE dataset_id = NEW.dataset_id AND config_revision = NEW.config_revision
            AND policy_revision = NEW.policy_revision)
        OR NOT EXISTS (SELECT 1 FROM control.owner_epochs
            WHERE resource_id = NEW.owner_resource_id AND epoch = NEW.owner_epoch
                AND lease_expires_at_us > (EXTRACT(EPOCH FROM clock_timestamp()) * 1000000)::bigint) THEN
        RAISE EXCEPTION 'relay registration authority expired before commit' USING ERRCODE = 'BB001';
    END IF;
    RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER relay_registration_complete AFTER INSERT ON control.relay_endpoints
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
    EXECUTE FUNCTION control.require_relay_registration_commit();

REVOKE ALL ON control.relay_endpoints FROM PUBLIC;
REVOKE ALL ON FUNCTION control.relay_endpoint_candidate(bytea),
    control.guard_relay_endpoint(), control.require_relay_registration_commit() FROM PUBLIC;
