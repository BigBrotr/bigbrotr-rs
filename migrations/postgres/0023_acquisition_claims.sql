-- Additive queued-claim authority; no historical job adoption or new ledger.
LOCK TABLE control.jobs, control.acquisition_jobs IN SHARE ROW EXCLUSIVE MODE;
DO $$ BEGIN
    IF current_setting('transaction_isolation')<>'read committed' THEN
        RAISE EXCEPTION 'acquisition claim migration requires READ COMMITTED' USING ERRCODE='25001';
    END IF;
    IF EXISTS (SELECT 1 FROM control.jobs j WHERE j.acquisition_job_protocol=1 AND (
        j.state<>'accepted' OR j.job_revision<>1 OR j.checkpoint_revision<>0
        OR NOT j.input_complete OR j.input_topology_generation IS NOT NULL
        OR j.owner_resource_id IS NOT NULL OR j.result_manifest_digest IS NOT NULL
        OR j.reason_code IS NOT NULL OR j.completed_at_us IS NOT NULL
        OR NOT EXISTS (SELECT 1 FROM control.acquisition_jobs a WHERE a.job_id=j.job_id
            AND a.dataset_id=j.dataset_id AND a.actor_id=j.principal_id AND a.request_key=j.request_key))) THEN
        RAISE EXCEPTION '0023 refuses advanced or inconsistent acquisition history; explicit recovery required'
            USING ERRCODE='55000';
    END IF;
END $$;

-- Call before job UPDATE. A row trigger cannot repair a caller's inverted lock
-- order. No browser session or old credential revision authorizes queued work.
CREATE FUNCTION control.require_acquisition_job_authority(wanted_dataset bytea, wanted_job bytea)
RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE detail control.acquisition_jobs%ROWTYPE; job control.jobs%ROWTYPE;
    owner control.owner_epochs%ROWTYPE; now_us bigint;
BEGIN
    IF current_setting('transaction_isolation')<>'read committed' THEN
        RAISE EXCEPTION 'acquisition claims require READ COMMITTED' USING ERRCODE='25001';
    END IF;
    SELECT * INTO detail FROM control.acquisition_jobs
        WHERE dataset_id=wanted_dataset AND job_id=wanted_job;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'missing typed acquisition job' USING ERRCODE='23514';
    END IF;
    SELECT * INTO STRICT owner FROM control.owner_epochs
        WHERE dataset_id=wanted_dataset AND role='control' AND state='active';
    PERFORM control.require_control_owner(owner.dataset_id,owner.resource_id,owner.epoch,owner.process_incarnation);
    PERFORM 1 FROM control.datasets WHERE dataset_id=wanted_dataset FOR UPDATE;
    PERFORM 1 FROM control.local_accounts
        WHERE dataset_id=wanted_dataset AND principal_id=detail.actor_id FOR SHARE;
    PERFORM 1 FROM control.principal_grants
        WHERE dataset_id=wanted_dataset AND principal_id=detail.actor_id
            AND action='roles.control' AND resource_kind='dataset' AND resource_id=wanted_dataset FOR SHARE;
    PERFORM 1 FROM control.relay_enrollments e JOIN control.acquisition_scopes s
        ON s.dataset_id=e.dataset_id AND s.endpoint_id=e.endpoint_id AND s.access_scope_id=e.access_scope_id
        WHERE s.dataset_id=wanted_dataset AND s.scope_id=detail.scope_id FOR SHARE OF e;
    SELECT * INTO STRICT job FROM control.jobs WHERE job_id=wanted_job FOR UPDATE;
    IF job.acquisition_job_protocol IS DISTINCT FROM 1 OR NOT job.input_complete
        OR job.state NOT IN ('accepted','queued','running')
        OR (job.state IN ('queued','running') AND
            (job.owner_resource_id,job.owner_epoch,job.owner_incarnation)
                IS DISTINCT FROM (owner.resource_id,owner.epoch,owner.process_incarnation))
        OR (job.dataset_id,job.principal_id,job.request_key)
            IS DISTINCT FROM (detail.dataset_id,detail.actor_id,detail.request_key) THEN
        RAISE EXCEPTION 'ineligible acquisition job' USING ERRCODE='23514';
    END IF;
    now_us:=(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint;
    IF now_us<job.created_at_us OR now_us>=detail.deadline_at_us OR now_us>=owner.lease_expires_at_us
        OR NOT EXISTS (SELECT 1 FROM control.local_accounts
            WHERE dataset_id=wanted_dataset AND principal_id=detail.actor_id AND active)
        OR NOT EXISTS (SELECT 1 FROM control.principal_grants
            WHERE dataset_id=wanted_dataset AND principal_id=detail.actor_id
                AND action='roles.control' AND resource_kind='dataset' AND resource_id=wanted_dataset)
        OR NOT EXISTS (SELECT 1 FROM control.datasets d
            JOIN control.config_revisions c ON c.dataset_id=d.dataset_id AND c.revision=d.config_revision
            JOIN control.policy_revisions p ON p.dataset_id=d.dataset_id AND p.revision=d.policy_revision
            JOIN control.acquisition_scopes s ON s.dataset_id=d.dataset_id AND s.scope_id=detail.scope_id
            JOIN control.relay_enrollments e ON e.dataset_id=s.dataset_id
                AND e.endpoint_id=s.endpoint_id AND e.access_scope_id=s.access_scope_id
            WHERE d.dataset_id=wanted_dataset AND d.config_revision=job.config_revision
                AND d.policy_revision=job.policy_revision AND s.policy_revision=d.policy_revision AND e.enabled
                AND c.document->>'format'='bigbrotr-product-policy/2'
                AND p.document->>'format'='bigbrotr-product-policy/2'
                AND c.document->'settings'->'sync.enabled'='true'::jsonb
                AND p.document->'settings'->'sync.enabled'='true'::jsonb) THEN
        RAISE EXCEPTION 'acquisition job requires current authority and original remaining duration'
            USING ERRCODE='BB001';
    END IF;
END $$;

-- A new Control epoch alone excludes PostgreSQL writes, not an old native
-- worker. Preserve that unresolved custody until an explicit recovery protocol
-- provides positive exclusion; neither API retries nor direct SQL may adopt it.
CREATE FUNCTION control.guard_acquisition_claim_owner() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
    IF (NEW.owner_resource_id,NEW.owner_epoch,NEW.owner_incarnation)
        IS DISTINCT FROM (OLD.owner_resource_id,OLD.owner_epoch,OLD.owner_incarnation) THEN
        RAISE EXCEPTION 'acquisition owner replacement requires explicit custody recovery'
            USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER acquisition_claim_owner_guard BEFORE UPDATE ON control.jobs
    FOR EACH ROW WHEN (OLD.acquisition_job_protocol=1 AND OLD.owner_resource_id IS NOT NULL)
    EXECUTE FUNCTION control.guard_acquisition_claim_owner();

CREATE FUNCTION control.require_acquisition_claim_complete() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
    PERFORM control.require_acquisition_job_authority(NEW.dataset_id,NEW.job_id);
    RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER zzz_acquisition_claim_complete AFTER UPDATE ON control.jobs
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
    WHEN (NEW.acquisition_job_protocol=1 AND NEW.state IN ('queued','running'))
    EXECUTE FUNCTION control.require_acquisition_claim_complete();

REVOKE ALL ON FUNCTION control.require_acquisition_job_authority(bytea,bytea),
    control.guard_acquisition_claim_owner(),control.require_acquisition_claim_complete() FROM PUBLIC;
