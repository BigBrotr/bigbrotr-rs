-- Attribute newly committed session mutations. Existing sessions and historical
-- revocations are not backfilled; accepting a retry is not a new logout.
ALTER TABLE control.management_audit ADD COLUMN policy_revision control.u64;
ALTER TABLE control.management_audit ADD FOREIGN KEY (dataset_id, policy_revision, config_revision)
    REFERENCES control.policy_revisions(dataset_id, revision, config_revision);
ALTER TABLE control.management_audit DROP CONSTRAINT management_audit_shape;
ALTER TABLE control.management_audit ADD CONSTRAINT management_audit_shape CHECK (
    (policy_revision IS NULL AND (
        (action = 'dataset.initialize' AND outcome = 'accepted' AND config_revision IS NOT NULL AND config_revision = 1 AND subject_id IS NULL AND reason IS NULL)
        OR (action IN ('owner-token.issue', 'owner-token.revoke', 'owner.enroll') AND outcome = 'accepted' AND config_revision IS NULL AND subject_id IS NOT NULL AND reason IS NULL AND requester_database_role IS NOT NULL AND requester_os_uid IS NOT NULL)
        OR (action = 'owner.enroll' AND outcome = 'rejected' AND config_revision IS NULL AND subject_id IS NOT NULL AND reason IS NOT NULL AND reason IN ('invalid-token', 'expired', 'exhausted', 'terminal') AND requester_database_role IS NOT NULL AND requester_os_uid IS NOT NULL)
        OR (action = 'policy.activate-initial' AND outcome = 'accepted' AND config_revision IS NOT NULL AND config_revision > 0 AND subject_id IS NOT NULL AND reason IS NULL AND requester_database_role IS NOT NULL AND requester_os_uid IS NOT NULL)
    ))
    OR (action IN ('browser.session.issue', 'browser.session.revoke') AND outcome = 'accepted'
        AND config_revision IS NOT NULL AND config_revision > 0
        AND policy_revision IS NOT NULL AND policy_revision > 0
        AND subject_id IS NOT NULL AND reason IS NULL
        AND requester_database_role IS NULL AND requester_os_uid IS NULL)
);
CREATE UNIQUE INDEX one_browser_session_mutation_audit
    ON control.management_audit(dataset_id, subject_id, action)
    WHERE action IN ('browser.session.issue', 'browser.session.revoke') AND outcome = 'accepted';
-- subject_id is the non-secret session identity, deliberately not a foreign key
-- to the short-lived credential row. Session cleanup must retain attribution.
-- The existing append-only trigger and private ACL remain in force. Ordinary
-- audit retention needs its explicit maintenance consumer; no implicit TTL/GC.
