-- Application-owned guard only. Native DuckLake values/columns stay untouched.
CREATE SCHEMA bigbrotr_archive;
REVOKE ALL ON SCHEMA bigbrotr_archive FROM PUBLIC;
CREATE TABLE bigbrotr_archive.installation (
    singleton boolean PRIMARY KEY CHECK (singleton),
    dataset_id bytea NOT NULL CHECK (octet_length(dataset_id)=16 AND dataset_id<>decode(repeat('00',16),'hex')),
    catalog_id bytea NOT NULL CHECK (octet_length(catalog_id)=16 AND catalog_id<>decode(repeat('00',16),'hex')),
    data_root text NOT NULL CHECK (octet_length(data_root) BETWEEN 2 AND 4096),
    protocol smallint NOT NULL CHECK (protocol=1),
    sql_checksum bytea NOT NULL CHECK (octet_length(sql_checksum)=32),
    layout_checksum bytea NOT NULL CHECK (octet_length(layout_checksum)=32)
);
CREATE TABLE bigbrotr_archive.attempts (
    ordinal numeric(20,0) PRIMARY KEY CHECK (ordinal BETWEEN 1 AND 18446744073709551615),
    attempt_id bytea NOT NULL UNIQUE CHECK (octet_length(attempt_id)=16 AND attempt_id<>decode(repeat('00',16),'hex')),
    coordinator_incarnation bytea NOT NULL CHECK (octet_length(coordinator_incarnation)=16 AND coordinator_incarnation<>decode(repeat('00',16),'hex')),
    worker_incarnation bytea NOT NULL UNIQUE CHECK (octet_length(worker_incarnation)=16 AND worker_incarnation<>decode(repeat('00',16),'hex') AND worker_incarnation<>coordinator_incarnation),
    topology_generation numeric(20,0) NOT NULL CHECK (topology_generation BETWEEN 0 AND 18446744073709551615),
    batch_digest bytea NOT NULL CHECK (octet_length(batch_digest)=32),
    login_name name NOT NULL UNIQUE,
    login_oid oid NOT NULL UNIQUE
);
CREATE TABLE bigbrotr_archive.owner (
    singleton boolean PRIMARY KEY CHECK (singleton),
    ordinal numeric(20,0) NOT NULL CHECK (ordinal BETWEEN 0 AND 18446744073709551615),
    enabled boolean NOT NULL,
    CHECK (ordinal>0 OR NOT enabled)
);
INSERT INTO bigbrotr_archive.owner VALUES (true,0,false);
CREATE FUNCTION bigbrotr_archive.require_writer() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $writer$
DECLARE wanted numeric; allowed boolean; login name; role_id oid;
BEGIN
    -- Lock survives through the real native metadata COMMIT/ROLLBACK. A fixed
    -- older snapshot must lock its unchanged tuple or fail serialization.
    SELECT ordinal,enabled INTO STRICT wanted,allowed
        FROM bigbrotr_archive.owner WHERE singleton FOR SHARE;
    IF NOT allowed THEN
        RAISE EXCEPTION 'archive catalog writer excluded' USING ERRCODE='55000';
    END IF;
    SELECT login_name,login_oid INTO STRICT login,role_id
        FROM bigbrotr_archive.attempts WHERE ordinal=wanted;
    IF login<>session_user OR role_id<>session_user::regrole::oid THEN
        RAISE EXCEPTION 'archive catalog writer excluded' USING ERRCODE='55000';
    END IF;
    RETURN NULL;
END;
$writer$;
CREATE FUNCTION bigbrotr_archive.retain_history() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $history$
BEGIN
    RAISE EXCEPTION 'archive guard history is immutable' USING ERRCODE='23514';
END;
$history$;
CREATE TRIGGER retain_installation BEFORE UPDATE OR DELETE OR TRUNCATE ON bigbrotr_archive.installation
    FOR EACH STATEMENT EXECUTE FUNCTION bigbrotr_archive.retain_history();
CREATE TRIGGER retain_attempts BEFORE UPDATE OR DELETE OR TRUNCATE ON bigbrotr_archive.attempts
    FOR EACH STATEMENT EXECUTE FUNCTION bigbrotr_archive.retain_history();
REVOKE ALL ON ALL TABLES IN SCHEMA bigbrotr_archive FROM PUBLIC;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA bigbrotr_archive FROM PUBLIC;
