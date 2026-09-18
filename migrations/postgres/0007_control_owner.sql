-- PostgreSQL Control commit fencing, protocol 1. Never infer participation from
-- historical coordination rows written before this protocol existed.
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM control.owner_epochs WHERE role = 'control') THEN
        RAISE EXCEPTION 'unqualified Control owner history requires explicit recovery' USING ERRCODE = '23514';
    END IF;
END $$;
ALTER TABLE control.owner_epochs
    ADD COLUMN control_fence_version smallint,
    ADD COLUMN control_backend_pid integer,
    ADD COLUMN control_backend_start timestamptz,
    ADD CONSTRAINT control_backend_shape CHECK (
        (role = 'control' AND control_fence_version IS NOT NULL AND control_fence_version = 1
            AND control_backend_pid IS NOT NULL AND control_backend_pid > 0 AND control_backend_start IS NOT NULL)
        OR (role <> 'control' AND control_fence_version IS NULL
            AND control_backend_pid IS NULL AND control_backend_start IS NULL));
CREATE UNIQUE INDEX control_one_unreleased ON control.owner_epochs(dataset_id)
    WHERE role = 'control' AND state <> 'released';

-- The session owner must already hold the exclusive lock. Add a transaction
-- hold so even an explicit session unlock cannot expose an in-flight COMMIT.
CREATE FUNCTION control.retain_control_lock() RETURNS void LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_locks
        WHERE locktype = 'advisory' AND database = (SELECT oid FROM pg_catalog.pg_database WHERE datname = current_database())
            AND classid = 1111651442::oid AND objid = 1129599564::oid AND objsubid = 2
            AND pid = pg_backend_pid() AND mode = 'ExclusiveLock' AND granted)
        OR NOT pg_try_advisory_xact_lock(1111651442, 1129599564) THEN
        RAISE EXCEPTION 'Control backend does not own commit exclusion' USING ERRCODE = '23514';
    END IF;
END;
$$;
CREATE FUNCTION control.require_control_owner(wanted_dataset bytea, wanted_resource bytea,
    wanted_epoch numeric, wanted_incarnation bytea) RETURNS void LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
BEGIN
    PERFORM control.retain_control_lock();
    PERFORM 1 FROM control.owner_epochs o JOIN control.role_instances r USING (role_instance_id)
        WHERE o.dataset_id = wanted_dataset AND o.resource_id = wanted_resource
            AND o.epoch = wanted_epoch AND o.process_incarnation = wanted_incarnation
            AND o.role = 'control' AND o.state = 'active' AND o.control_fence_version = 1
            AND o.control_backend_pid = pg_backend_pid()
            AND o.control_backend_start = (SELECT backend_start FROM pg_catalog.pg_stat_activity WHERE pid = pg_backend_pid())
            AND r.retired_at_us IS NULL FOR SHARE OF o, r;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Control owner belongs to another backend or is inactive' USING ERRCODE = '23514';
    END IF;
END;
$$;
CREATE FUNCTION control.guard_control_backend() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN RETURN OLD; END IF; -- existing history guard refuses
    IF NEW.role <> 'control' THEN RETURN NEW; END IF;
    PERFORM control.retain_control_lock();
    IF TG_OP = 'INSERT' THEN
        IF NEW.control_fence_version <> 1 OR NEW.control_backend_pid <> pg_backend_pid()
            OR NEW.control_backend_start IS DISTINCT FROM
                (SELECT backend_start FROM pg_catalog.pg_stat_activity WHERE pid = pg_backend_pid())
            OR EXISTS (SELECT 1 FROM control.owner_epochs WHERE dataset_id = NEW.dataset_id
                AND role = 'control' AND resource_id <> NEW.resource_id)
            OR NOT EXISTS (SELECT 1 FROM control.role_instances WHERE role_instance_id = NEW.role_instance_id
                AND role = 'control' AND retired_at_us IS NULL) THEN
            RAISE EXCEPTION 'Control claim requires its actual backend and stable resource' USING ERRCODE = '23514';
        END IF;
    ELSE
        IF ROW(NEW.control_fence_version, NEW.control_backend_pid, NEW.control_backend_start)
            IS DISTINCT FROM ROW(OLD.control_fence_version, OLD.control_backend_pid, OLD.control_backend_start) THEN
            RAISE EXCEPTION 'Control backend attribution is immutable' USING ERRCODE = '23514';
        END IF;
        IF NEW.state = 'released' THEN
            IF NEW.exclusion_method <> 'storage_fenced' THEN
                RAISE EXCEPTION 'Control release requires commit exclusion evidence' USING ERRCODE = '23514';
            END IF;
        ELSE
            PERFORM control.require_control_owner(OLD.dataset_id, OLD.resource_id, OLD.epoch, OLD.process_incarnation);
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER control_backend_guard BEFORE INSERT OR UPDATE OR DELETE ON control.owner_epochs
    FOR EACH ROW EXECUTE FUNCTION control.guard_control_backend();
REVOKE ALL ON FUNCTION control.retain_control_lock() FROM PUBLIC;
REVOKE ALL ON FUNCTION control.require_control_owner(bytea, bytea, numeric, bytea) FROM PUBLIC;
REVOKE ALL ON FUNCTION control.guard_control_backend() FROM PUBLIC;
