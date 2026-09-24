-- First common artifact consumer: one bounded private signed export unit.
-- Existing accepted0813 jobs have no execution yet; never infer this from an
-- absent file or fabricate a reservation for already advanced legacy work.
LOCK TABLE control.jobs, control.export_jobs IN SHARE ROW EXCLUSIVE MODE;
DO $$ BEGIN
    IF current_setting('transaction_isolation')<>'read committed' THEN
        RAISE EXCEPTION 'artifact migration requires READ COMMITTED' USING ERRCODE='25001';
    END IF;
    IF EXISTS (SELECT 1 FROM control.jobs j WHERE j.export_job_protocol=1 AND (
        j.state<>'accepted' OR j.job_revision<>1 OR j.checkpoint_revision<>0
        OR NOT j.input_complete OR j.owner_resource_id IS NOT NULL
        OR j.result_manifest_digest IS NOT NULL OR j.reason_code IS NOT NULL OR j.completed_at_us IS NOT NULL
        OR NOT EXISTS (SELECT 1 FROM control.export_jobs e JOIN control.archive_snapshot_pins p
            ON p.lease_id=e.pin_lease_id JOIN control.snapshot_leases l USING(lease_id)
            WHERE e.job_id=j.job_id AND e.dataset_id=j.dataset_id AND e.actor_id=j.principal_id
                AND p.export_job_id=j.job_id AND p.purpose='export' AND l.state='active'
                AND p.consumer_id=e.catalog_id AND p.frontier_revision=e.frontier_revision))) THEN
        RAISE EXCEPTION '0501 refuses advanced or inconsistent export history; explicit recovery required' USING ERRCODE='55000';
    END IF;
END $$;

-- Reservation is immutable and survives owner replacement. It is neither a
-- second job-state ledger nor a completed artifact. Expiry never reclaims it.
CREATE TABLE control.export_execution_units (
    job_id control.id16 PRIMARY KEY REFERENCES control.export_jobs(job_id),
    artifact_id control.id16 NOT NULL UNIQUE,
    -- Internal same-transaction guard, never a portable job/recovery identity.
    reservation_transaction xid8 NOT NULL DEFAULT pg_current_xact_id(),
    max_output_bytes control.u64 NOT NULL CHECK (max_output_bytes>0),
    max_temporary_bytes control.u64 NOT NULL CHECK (max_temporary_bytes>=2*max_output_bytes),
    reserved_at_us bigint NOT NULL DEFAULT ((EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint)
);

-- Only completed artifacts enter the common inventory. Publication/user catalog
-- state is a later0804 consumer; inventory presence grants no discovery/download.
CREATE TABLE control.artifacts (
    artifact_id control.id16 PRIMARY KEY,
    job_id control.id16 NOT NULL UNIQUE REFERENCES control.jobs(job_id),
    dataset_id control.id16 NOT NULL REFERENCES control.datasets(dataset_id),
    principal_id control.id16 NOT NULL,
    destination_id control.id16 NOT NULL,
    destination_binding_sha256 control.digest32 NOT NULL,
    manifest_format text NOT NULL CHECK (manifest_format='bigbrotr-signed-events-export/1'),
    manifest_bytes control.u64 NOT NULL CHECK (manifest_bytes BETWEEN 1 AND 262144),
    manifest_sha256 control.digest32 NOT NULL,
    file_count integer NOT NULL CHECK (file_count BETWEEN 1 AND 1024),
    total_bytes control.u64 NOT NULL CHECK (total_bytes>=manifest_bytes),
    verification_pin_id control.id16 NOT NULL REFERENCES control.archive_snapshot_pins(lease_id),
    completed_at_us bigint NOT NULL,
    FOREIGN KEY (dataset_id,principal_id) REFERENCES control.local_accounts(dataset_id,principal_id)
);
CREATE INDEX artifacts_listing ON control.artifacts(completed_at_us DESC,artifact_id DESC);
CREATE TABLE control.artifact_files (
    artifact_id control.id16 NOT NULL REFERENCES control.artifacts(artifact_id),
    file_id text NOT NULL CHECK (file_id ~ '^[a-z][a-z0-9_-]{0,63}$'),
    relative_path text NOT NULL CHECK (octet_length(relative_path) BETWEEN 1 AND 256
        AND relative_path ~ '^[a-zA-Z0-9][a-zA-Z0-9._/-]*$'
        AND relative_path !~ '(^|/)[.][.]?(/|$)' AND relative_path !~ '//|/$'
        AND lower(relative_path)<>'manifest.json'),
    format text NOT NULL CHECK (format='nostr-jsonl/1'),
    byte_count control.u64 NOT NULL,
    row_count control.u64 NOT NULL,
    sha256 control.digest32 NOT NULL,
    PRIMARY KEY (artifact_id,file_id)
);
CREATE UNIQUE INDEX artifact_file_paths ON control.artifact_files(artifact_id,lower(relative_path));

-- Callers preacquire these locks before any job UPDATE: a row trigger cannot
-- repair an inverted acquisition. No browser session participates in execution.
CREATE FUNCTION control.require_export_execution_authority(wanted_job bytea) RETURNS void
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE detail control.export_jobs%ROWTYPE; owner control.owner_epochs%ROWTYPE;
BEGIN
    IF current_setting('transaction_isolation')<>'read committed' THEN
        RAISE EXCEPTION 'export execution requires READ COMMITTED' USING ERRCODE='25001';
    END IF;
    SELECT * INTO STRICT detail FROM control.export_jobs WHERE job_id=wanted_job;
    SELECT * INTO STRICT owner FROM control.owner_epochs
        WHERE dataset_id=detail.dataset_id AND role='control' AND state='active';
    PERFORM control.require_control_owner(owner.dataset_id,owner.resource_id,owner.epoch,owner.process_incarnation);
    PERFORM 1 FROM control.datasets WHERE dataset_id=detail.dataset_id FOR SHARE;
    PERFORM 1 FROM control.local_accounts WHERE dataset_id=detail.dataset_id AND principal_id=detail.actor_id FOR SHARE;
    PERFORM 1 FROM control.principal_grants WHERE dataset_id=detail.dataset_id AND principal_id=detail.actor_id
        AND action='data.read' AND resource_kind='dataset' AND resource_id=detail.dataset_id FOR SHARE;
    PERFORM control.lock_archive_pins(detail.dataset_id,detail.catalog_id);
    PERFORM 1 FROM control.jobs WHERE job_id=wanted_job FOR UPDATE;
    -- Match the existing source-bound commit guard's lock order. A slow wait
    -- must finish before the final clock/grant observation, on every caller.
    PERFORM 1 FROM control.source_streams s JOIN control.job_input_bounds b USING(dataset_id,shard_id,owner_epoch)
        WHERE b.job_id=wanted_job ORDER BY s.dataset_id,s.shard_id,s.owner_epoch FOR SHARE OF s;
    IF NOT EXISTS (SELECT 1 FROM control.local_accounts WHERE dataset_id=detail.dataset_id
            AND principal_id=detail.actor_id AND active)
        OR NOT EXISTS (SELECT 1 FROM control.principal_grants WHERE dataset_id=detail.dataset_id
            AND principal_id=detail.actor_id AND action='data.read' AND resource_kind='dataset' AND resource_id=detail.dataset_id)
        OR EXISTS (SELECT 1 FROM (VALUES ('history.read'),('export'),('private-export')) wanted(action)
            WHERE NOT EXISTS (SELECT 1 FROM control.scope_grants g WHERE g.dataset_id=detail.dataset_id
                AND g.principal_id=detail.actor_id AND g.action=wanted.action AND g.dictionary_kind=4
                AND g.allocation_domain=detail.allocation_domain AND g.ordinal=detail.ordinal AND g.enabled)) THEN
        RAISE EXCEPTION 'export execution requires current input and export grants' USING ERRCODE='BB001';
    END IF;
    IF owner.lease_expires_at_us<=(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint THEN
        RAISE EXCEPTION 'export execution authority expired' USING ERRCODE='BB001';
    END IF;
END $$;

CREATE FUNCTION control.guard_export_unit() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE job control.jobs%ROWTYPE; detail control.export_jobs%ROWTYPE;
BEGIN
    PERFORM control.require_export_execution_authority(NEW.job_id);
    SELECT * INTO STRICT job FROM control.jobs WHERE job_id=NEW.job_id;
    SELECT * INTO STRICT detail FROM control.export_jobs WHERE job_id=NEW.job_id;
    IF job.export_job_protocol IS DISTINCT FROM 1 OR job.state<>'queued' OR NOT job.input_complete
        OR job.checkpoint_revision<>0 OR job.result_manifest_digest IS NOT NULL
        OR NOT EXISTS (SELECT 1 FROM control.owner_epochs o WHERE o.dataset_id=job.dataset_id AND o.role='control'
            AND o.state='active' AND (o.resource_id,o.epoch,o.process_incarnation)
                IS NOT DISTINCT FROM (job.owner_resource_id,job.owner_epoch,job.owner_incarnation))
        OR NOT EXISTS (SELECT 1 FROM control.archive_snapshot_pins p JOIN control.snapshot_leases l USING(lease_id)
            WHERE p.lease_id=detail.pin_lease_id AND p.export_job_id=job.job_id AND p.purpose='export'
                AND p.consumer_id=detail.catalog_id AND p.frontier_revision=detail.frontier_revision AND l.state='active') THEN
        RAISE EXCEPTION 'export reservation requires a queued qualified retained job' USING ERRCODE='23514';
    END IF;
    -- Lease scheduling expiry is deliberately absent: retained inputs are still
    -- protected, while actual reading requires a separate fresh reader pin.
    NEW.reservation_transaction:=pg_current_xact_id();
    NEW.reserved_at_us:=(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint;
    RETURN NEW;
END $$;
CREATE TRIGGER export_unit_guard BEFORE INSERT ON control.export_execution_units
    FOR EACH ROW EXECUTE FUNCTION control.guard_export_unit();

CREATE FUNCTION control.guard_artifact_insert() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE job control.jobs%ROWTYPE; wanted_job bytea;
BEGIN
    IF TG_TABLE_NAME='artifact_files' THEN
        SELECT job_id INTO STRICT wanted_job FROM control.artifacts WHERE artifact_id=NEW.artifact_id FOR UPDATE;
    ELSE wanted_job:=NEW.job_id; END IF;
    PERFORM control.require_export_execution_authority(wanted_job);
    SELECT * INTO STRICT job FROM control.jobs WHERE job_id=wanted_job;
    IF job.export_job_protocol IS DISTINCT FROM 1 OR job.state<>'running' OR job.checkpoint_revision<>0
        OR NOT EXISTS (SELECT 1 FROM control.export_execution_units WHERE job_id=wanted_job AND artifact_id=NEW.artifact_id) THEN
        RAISE EXCEPTION 'artifact requires its running reserved export unit' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER artifact_insert_guard BEFORE INSERT ON control.artifacts
    FOR EACH ROW EXECUTE FUNCTION control.guard_artifact_insert();
CREATE TRIGGER artifact_file_insert_guard BEFORE INSERT ON control.artifact_files
    FOR EACH ROW EXECUTE FUNCTION control.guard_artifact_insert();

-- One result contract checked from every side. Jobs cannot forge a checkpoint,
-- nor can inventory insertion become visible without exactly that completion.
CREATE FUNCTION control.require_export_result() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE job control.jobs%ROWTYPE; detail control.export_jobs%ROWTYPE;
    unit control.export_execution_units%ROWTYPE; artifact control.artifacts%ROWTYPE;
    wanted_job bytea; files bigint; bytes numeric;
BEGIN
    IF TG_TABLE_NAME='artifact_files' THEN
        SELECT job_id INTO STRICT wanted_job FROM control.artifacts WHERE artifact_id=NEW.artifact_id;
    ELSE wanted_job:=NEW.job_id; END IF;
    SELECT * INTO STRICT job FROM control.jobs WHERE job_id=wanted_job;
    IF job.export_job_protocol IS DISTINCT FROM 1 THEN RETURN NULL; END IF;
    SELECT * INTO STRICT detail FROM control.export_jobs WHERE job_id=wanted_job;
    SELECT * INTO unit FROM control.export_execution_units WHERE job_id=wanted_job;
    SELECT * INTO artifact FROM control.artifacts WHERE job_id=wanted_job;
    IF TG_TABLE_NAME='export_execution_units' AND job.state<>'queued' THEN
        RAISE EXCEPTION 'new export reservation must commit before execution' USING ERRCODE='23514';
    END IF;
    IF job.state IN ('running','succeeded') AND unit.reservation_transaction=pg_current_xact_id() THEN
        RAISE EXCEPTION 'export reservation must commit before execution' USING ERRCODE='23514';
    END IF;
    IF job.state='running' AND unit.job_id IS NULL THEN
        RAISE EXCEPTION 'running export requires its committed reservation' USING ERRCODE='23514';
    END IF;
    IF job.state='succeeded' THEN
        IF unit.job_id IS NULL OR artifact.artifact_id IS NULL OR job.checkpoint_revision<>1
            OR artifact.artifact_id<>unit.artifact_id OR job.result_manifest_digest IS DISTINCT FROM artifact.manifest_sha256
            OR (artifact.dataset_id,artifact.principal_id,artifact.destination_id,artifact.destination_binding_sha256,artifact.completed_at_us)
                IS DISTINCT FROM (job.dataset_id,job.principal_id,detail.destination_id,detail.destination_binding_sha256,job.completed_at_us)
            OR artifact.file_count<>1 OR artifact.total_bytes>unit.max_output_bytes THEN
            RAISE EXCEPTION 'export completion requires exact atomic inventory and checkpoint' USING ERRCODE='23514';
        END IF;
        SELECT count(*),coalesce(sum(byte_count),0) INTO files,bytes FROM control.artifact_files WHERE artifact_id=artifact.artifact_id;
        IF files<>artifact.file_count OR bytes+artifact.manifest_bytes<>artifact.total_bytes
            OR NOT EXISTS (SELECT 1 FROM control.artifact_files f WHERE f.artifact_id=artifact.artifact_id
                AND f.file_id='events' AND f.relative_path='events.jsonl' AND f.format='nostr-jsonl/1'
                AND f.row_count BETWEEN 0 AND 1 AND ((f.row_count=0)=(f.byte_count=0))
                AND (f.row_count<>0 OR f.sha256=sha256(''::bytea))) THEN
            RAISE EXCEPTION 'export inventory requires its exact complete signed file' USING ERRCODE='23514';
        END IF;
        PERFORM control.require_export_execution_authority(wanted_job);
        IF NOT control.scope_pin_live(detail.dataset_id,detail.catalog_id,detail.frontier_revision,artifact.verification_pin_id) THEN
            RAISE EXCEPTION 'export completion requires a fresh live reader on its original cut' USING ERRCODE='BB001';
        END IF;
    ELSIF job.checkpoint_revision<>0 OR job.result_manifest_digest IS NOT NULL OR artifact.artifact_id IS NOT NULL THEN
        RAISE EXCEPTION 'unfinished exact export cannot advertise a completed unit' USING ERRCODE='23514';
    ELSIF job.state IN ('queued','running') THEN
        PERFORM control.require_export_execution_authority(wanted_job);
    END IF;
    RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER zzz_export_result_complete AFTER INSERT OR UPDATE ON control.jobs
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.export_job_protocol=1)
    EXECUTE FUNCTION control.require_export_result();
CREATE CONSTRAINT TRIGGER export_unit_complete AFTER INSERT ON control.export_execution_units
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION control.require_export_result();
CREATE CONSTRAINT TRIGGER artifact_complete AFTER INSERT ON control.artifacts
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION control.require_export_result();
CREATE CONSTRAINT TRIGGER artifact_file_complete AFTER INSERT ON control.artifact_files
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION control.require_export_result();

CREATE TRIGGER immutable_export_units BEFORE UPDATE OR DELETE ON control.export_execution_units
    FOR EACH ROW EXECUTE FUNCTION control.reject_history_mutation();
CREATE TRIGGER export_units_truncate_guard BEFORE TRUNCATE ON control.export_execution_units
    FOR EACH STATEMENT EXECUTE FUNCTION control.reject_history_mutation();
CREATE TRIGGER immutable_artifacts BEFORE UPDATE OR DELETE ON control.artifacts
    FOR EACH ROW EXECUTE FUNCTION control.reject_history_mutation();
CREATE TRIGGER artifacts_truncate_guard BEFORE TRUNCATE ON control.artifacts
    FOR EACH STATEMENT EXECUTE FUNCTION control.reject_history_mutation();
CREATE TRIGGER immutable_artifact_files BEFORE UPDATE OR DELETE ON control.artifact_files
    FOR EACH ROW EXECUTE FUNCTION control.reject_history_mutation();
CREATE TRIGGER artifact_files_truncate_guard BEFORE TRUNCATE ON control.artifact_files
    FOR EACH STATEMENT EXECUTE FUNCTION control.reject_history_mutation();
REVOKE ALL ON control.export_execution_units,control.artifacts,control.artifact_files FROM PUBLIC;
REVOKE ALL ON FUNCTION control.require_export_execution_authority(bytea),control.guard_export_unit(),
    control.guard_artifact_insert(),control.require_export_result() FROM PUBLIC;
