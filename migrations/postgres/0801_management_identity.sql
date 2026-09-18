-- First attributable deployment initializer. Human enrollment/credentials and
-- action grants arrive with their real consumers in later immutable migrations.
CREATE TABLE control.principals (
    principal_id control.id16 PRIMARY KEY,
    dataset_id control.id16 NOT NULL REFERENCES control.datasets(dataset_id),
    kind text NOT NULL CHECK (kind = 'installation'),
    created_at_us bigint NOT NULL,
    database_role text NOT NULL CHECK (octet_length(database_role) BETWEEN 1 AND 128),
    local_os_uid control.u32 NOT NULL,
    UNIQUE (dataset_id, principal_id),
    UNIQUE (dataset_id, kind)
);
-- An installation actor is permanent attribution only. It has no password,
-- browser identity, session or permission. No historical actor is backfilled.
CREATE TRIGGER immutable_installation_principal BEFORE UPDATE OR DELETE ON control.principals
    FOR EACH ROW EXECUTE FUNCTION control.reject_history_mutation();

CREATE TABLE control.management_audit (
    audit_id control.id16 PRIMARY KEY,
    dataset_id control.id16 NOT NULL,
    actor_id control.id16 NOT NULL,
    action text NOT NULL CHECK (action = 'dataset.initialize'),
    outcome text NOT NULL CHECK (outcome = 'accepted'),
    created_at_us bigint NOT NULL,
    config_revision control.u64 NOT NULL CHECK (config_revision = 1),
    FOREIGN KEY (dataset_id, actor_id) REFERENCES control.principals(dataset_id, principal_id),
    FOREIGN KEY (dataset_id, config_revision) REFERENCES control.config_revisions(dataset_id, revision),
    UNIQUE (dataset_id, action)
);
CREATE TRIGGER immutable_initialization_audit BEFORE UPDATE OR DELETE ON control.management_audit
    FOR EACH ROW EXECUTE FUNCTION control.reject_history_mutation();
-- This receipt remains a bootstrap/replay obligation after ordinary audit TTL.
-- It is not deleted by generic operational-history maintenance.
REVOKE ALL ON control.principals, control.management_audit FROM PUBLIC;
