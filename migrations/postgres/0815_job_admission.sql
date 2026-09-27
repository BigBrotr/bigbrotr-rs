-- A listing cut is committed admission order, not the job's wall-clock time.
-- One Control database has one immutable local dataset (datasets.singleton).
-- The mapping therefore derives its dataset through its immutable job FK;
-- neither another dataset key nor a mutable counter duplicates that authority.
LOCK TABLE control.datasets,control.jobs IN SHARE ROW EXCLUSIVE MODE;

CREATE TABLE control.job_admissions (
    job_id control.id16 PRIMARY KEY REFERENCES control.jobs(job_id),
    admission_ordinal control.u64 NOT NULL UNIQUE CHECK (admission_ordinal>0)
);

-- Existing jobs all precede any future cut. This deterministic assignment does
-- not infer their original commit order or rewrite their rows/receipts/xmin.
INSERT INTO control.job_admissions(job_id,admission_ordinal)
SELECT job_id,row_number() OVER (ORDER BY created_at_us,job_id) FROM control.jobs;

CREATE FUNCTION control.lock_job_admission() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
    IF current_setting('transaction_isolation')<>'read committed' THEN
        RAISE EXCEPTION 'job admission requires READ COMMITTED' USING ERRCODE='25001';
    END IF;
    -- BEFORE INSERT: take the serialization lock before the job's FK acquires
    -- KEY SHARE, avoiding a concurrent pair of later lock upgrades. Qualified
    -- exports first execute export_job_guard, which checks their Control owner.
    PERFORM 1 FROM control.datasets WHERE dataset_id=NEW.dataset_id FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'job admission requires its local dataset' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER job_admission_lock BEFORE INSERT ON control.jobs
    FOR EACH ROW EXECUTE FUNCTION control.lock_job_admission();

CREATE FUNCTION control.assign_job_admission() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE previous numeric;
BEGIN
    IF current_setting('transaction_isolation')<>'read committed' THEN
        RAISE EXCEPTION 'job admission requires READ COMMITTED' USING ERRCODE='25001';
    END IF;
    IF NEW.admission_ordinal IS NOT NULL THEN
        RAISE EXCEPTION 'job admission order is allocated by Control' USING ERRCODE='23514';
    END IF;
    PERFORM 1 FROM control.datasets d JOIN control.jobs j USING(dataset_id)
        WHERE j.job_id=NEW.job_id FOR UPDATE OF d;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'job admission requires its existing job' USING ERRCODE='23514';
    END IF;
    -- Separate VOLATILE statement after the lock: see the preceding commit,
    -- including when this transaction began before it. Unique ordinal index
    -- supplies a bounded backward lookup; no scan or nontransactional sequence.
    SELECT admission_ordinal INTO previous FROM control.job_admissions
        ORDER BY admission_ordinal DESC LIMIT 1;
    NEW.admission_ordinal:=coalesce(previous,0)+1;
    RETURN NEW;
END $$;
CREATE TRIGGER job_admission_assign BEFORE INSERT ON control.job_admissions
    FOR EACH ROW EXECUTE FUNCTION control.assign_job_admission();

CREATE FUNCTION control.capture_job_admission() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
    INSERT INTO control.job_admissions(job_id) VALUES(NEW.job_id);
    RETURN NULL;
END $$;
CREATE TRIGGER capture_job_admission AFTER INSERT ON control.jobs
    FOR EACH ROW EXECUTE FUNCTION control.capture_job_admission();

CREATE FUNCTION control.require_job_admission() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM control.job_admissions WHERE job_id=NEW.job_id) THEN
        RAISE EXCEPTION 'accepted job requires its admission order' USING ERRCODE='23514';
    END IF;
    RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER job_admission_complete AFTER INSERT ON control.jobs
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION control.require_job_admission();
CREATE TRIGGER immutable_job_admission BEFORE UPDATE OR DELETE ON control.job_admissions
    FOR EACH ROW EXECUTE FUNCTION control.reject_history_mutation();
CREATE TRIGGER job_admission_truncate_guard BEFORE TRUNCATE ON control.job_admissions
    FOR EACH STATEMENT EXECUTE FUNCTION control.reject_history_mutation();

REVOKE ALL ON control.job_admissions FROM PUBLIC;
REVOKE ALL ON FUNCTION control.lock_job_admission(),control.assign_job_admission(),
    control.capture_job_admission(),control.require_job_admission() FROM PUBLIC;
