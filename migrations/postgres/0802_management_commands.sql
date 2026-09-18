-- First real management command: protected local, zero-participant activation.
-- Appended after 0805; numeric reservations are not application chronology.
ALTER TABLE control.management_audit DROP CONSTRAINT management_audit_check;
ALTER TABLE control.management_audit ADD CONSTRAINT management_audit_shape CHECK (
    (action = 'dataset.initialize' AND outcome = 'accepted' AND config_revision IS NOT NULL AND config_revision = 1 AND subject_id IS NULL AND reason IS NULL)
    OR (action IN ('owner-token.issue', 'owner-token.revoke', 'owner.enroll') AND outcome = 'accepted' AND config_revision IS NULL AND subject_id IS NOT NULL AND reason IS NULL AND requester_database_role IS NOT NULL AND requester_os_uid IS NOT NULL)
    OR (action = 'owner.enroll' AND outcome = 'rejected' AND config_revision IS NULL AND subject_id IS NOT NULL AND reason IS NOT NULL AND reason IN ('invalid-token', 'expired', 'exhausted', 'terminal') AND requester_database_role IS NOT NULL AND requester_os_uid IS NOT NULL)
    OR (action = 'policy.activate-initial' AND outcome = 'accepted' AND config_revision IS NOT NULL AND config_revision > 0 AND subject_id IS NOT NULL AND reason IS NULL AND requester_database_role IS NOT NULL AND requester_os_uid IS NOT NULL)
);
ALTER TABLE control.management_audit ADD UNIQUE (audit_id, dataset_id, actor_id, action, config_revision, subject_id);
ALTER TABLE control.policy_revisions ADD UNIQUE (dataset_id, revision, config_revision);

CREATE TABLE control.management_commands (
    actor_id control.id16 NOT NULL,
    request_key control.id16 NOT NULL,
    dataset_id control.id16 NOT NULL,
    action text NOT NULL CHECK (action = 'policy.activate-initial'),
    request_digest control.digest32 NOT NULL,
    expected_config_revision control.u64 NOT NULL CHECK (expected_config_revision > 0),
    authority_mode text NOT NULL CHECK (authority_mode IN ('managed', 'declarative')),
    document_digest control.digest32 NOT NULL,
    policy_revision control.u64 NOT NULL CHECK (policy_revision = 1),
    audit_id control.id16 NOT NULL UNIQUE,
    accepted_at_us bigint NOT NULL,
    PRIMARY KEY (actor_id, request_key),
    UNIQUE (dataset_id, action),
    FOREIGN KEY (dataset_id, actor_id) REFERENCES control.principals(dataset_id, principal_id),
    FOREIGN KEY (dataset_id, policy_revision, expected_config_revision)
        REFERENCES control.policy_revisions(dataset_id, revision, config_revision),
    FOREIGN KEY (audit_id, dataset_id, actor_id, action, expected_config_revision, request_key)
        REFERENCES control.management_audit(audit_id, dataset_id, actor_id, action, config_revision, subject_id)
);
-- An initial activation is permanent recovery history, not ordinary audit TTL.
CREATE TRIGGER immutable_management_commands BEFORE UPDATE OR DELETE ON control.management_commands
    FOR EACH ROW EXECUTE FUNCTION control.reject_history_mutation();
REVOKE ALL ON control.management_commands FROM PUBLIC;
