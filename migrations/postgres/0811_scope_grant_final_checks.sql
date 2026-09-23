-- Additive correction after the real0810 COMMIT rejected a local-variable
-- qualification as a table reference. Keep applied810 bytes and all rows.
-- Distinct local names avoid column ambiguity without an implicit block label.
CREATE OR REPLACE FUNCTION control.require_scope_grant_effects() RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE command control.management_commands%ROWTYPE; detail control.scope_grant_changes%ROWTYPE;
    wanted_actor bytea; wanted_key bytea; owner control.owner_epochs%ROWTYPE;
BEGIN
    IF TG_TABLE_NAME='management_audit' THEN wanted_actor:=NEW.actor_id; wanted_key:=NEW.subject_id;
    ELSE wanted_actor:=NEW.actor_id; wanted_key:=NEW.request_key; END IF;
    SELECT * INTO command FROM control.management_commands c WHERE c.actor_id=wanted_actor AND c.request_key=wanted_key;
    SELECT * INTO detail FROM control.scope_grant_changes d WHERE d.actor_id=wanted_actor AND d.request_key=wanted_key;
    IF command.actor_id IS NULL OR detail.actor_id IS NULL OR command.action<>'scope-grant.change'
        OR NOT EXISTS (SELECT 1 FROM control.datasets ds WHERE ds.dataset_id=command.dataset_id
            AND ds.config_revision=command.expected_config_revision AND ds.policy_revision=command.policy_revision)
        OR NOT EXISTS (SELECT 1 FROM control.local_accounts a JOIN control.browser_sessions s
            ON s.dataset_id=a.dataset_id AND s.principal_id=a.principal_id
            WHERE a.dataset_id=detail.dataset_id AND a.principal_id=detail.actor_id AND a.active
                AND a.credential_revision=detail.actor_credential_revision AND a.authentication_revision=detail.actor_authentication_revision
                AND a.grant_revision=detail.actor_grant_revision + CASE WHEN detail.actor_id=detail.principal_id THEN 1 ELSE 0 END
                AND s.session_id=detail.session_id AND s.revoked_at_us IS NULL
                AND s.credential_revision=a.credential_revision AND s.authentication_revision=a.authentication_revision
)
        OR NOT EXISTS (SELECT 1 FROM control.principal_grants WHERE dataset_id=detail.dataset_id AND principal_id=detail.actor_id
            AND action='principals.manage' AND resource_kind='dataset' AND resource_id=detail.dataset_id)
        OR NOT EXISTS (SELECT 1 FROM control.management_audit a WHERE a.audit_id=command.audit_id AND a.dataset_id=command.dataset_id
            AND a.actor_id=command.actor_id AND a.subject_id=command.request_key AND a.action=command.action AND a.outcome='accepted'
            AND a.config_revision=command.expected_config_revision AND a.policy_revision=command.policy_revision AND a.created_at_us=command.accepted_at_us)
        OR EXISTS (SELECT 1 FROM control.reauthentication_proofs p WHERE p.proof_id=detail.proof_id)
        OR NOT EXISTS (SELECT 1 FROM control.local_accounts a WHERE a.dataset_id=detail.dataset_id AND a.principal_id=detail.principal_id
            AND a.active AND a.grant_revision=detail.resulting_grant_revision)
        OR NOT EXISTS (SELECT 1 FROM control.scope_grants g WHERE g.actor_id=detail.actor_id AND g.request_key=detail.request_key
            AND (g.dataset_id,g.principal_id,g.action,g.dictionary_kind,g.allocation_domain,g.ordinal,g.enabled,g.generation)
                IS NOT DISTINCT FROM (detail.dataset_id,detail.principal_id,detail.action,detail.dictionary_kind,detail.allocation_domain,detail.ordinal,detail.enabled,detail.scope_generation)) THEN
        RAISE EXCEPTION 'scope grant requires atomic command, exact audit, revision, grant and proof consumption' USING ERRCODE='23514';
    END IF;
    IF TG_TABLE_NAME='management_audit' THEN
        IF NEW.audit_id IS DISTINCT FROM command.audit_id THEN
            RAISE EXCEPTION 'scope grant audit is not the command receipt' USING ERRCODE='23514';
        END IF;
    END IF;
    SELECT * INTO STRICT owner FROM control.owner_epochs WHERE dataset_id=detail.dataset_id AND role='control' AND state='active';
    PERFORM control.require_control_owner(owner.dataset_id,owner.resource_id,owner.epoch,owner.process_incarnation);
    IF (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint >= detail.proof_expires_at_us
        OR NOT EXISTS (SELECT 1 FROM control.browser_sessions s WHERE s.session_id=detail.session_id
            AND (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint >= s.last_seen_at_us
            AND (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint < LEAST(s.expires_at_us,s.idle_expires_at_us))
        OR (detail.enabled AND NOT control.scope_pin_live(detail.dataset_id,detail.catalog_id,detail.frontier_revision,detail.pin_lease_id)) THEN
        -- An explicit server rejection of COMMIT establishes rollback, unlike
        -- a lost transport response. The adapter distinguishes this expiry code.
        RAISE EXCEPTION 'scope grant authority expired before commit' USING ERRCODE='BB001';
    END IF;
    RETURN NULL;
END $$;
