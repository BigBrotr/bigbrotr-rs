-- Installed atomically with 0001 for a verified empty scientific schema1.
-- This is not an upgrade/adoption path for an existing native-only guard.
CREATE TABLE bigbrotr_archive.scientific_schema (
    singleton boolean PRIMARY KEY REFERENCES bigbrotr_archive.installation(singleton) CHECK (singleton),
    format_version smallint NOT NULL CHECK (format_version=1),
    declaration_checksum bytea NOT NULL CHECK (octet_length(declaration_checksum)=32),
    native_layout_checksum bytea NOT NULL CHECK (octet_length(native_layout_checksum)=32),
    sql_checksum bytea NOT NULL CHECK (octet_length(sql_checksum)=32),
    initialization_snapshot bigint NOT NULL CHECK (initialization_snapshot=1)
);
CREATE TRIGGER retain_scientific_schema BEFORE UPDATE OR DELETE OR TRUNCATE ON bigbrotr_archive.scientific_schema
    FOR EACH STATEMENT EXECUTE FUNCTION bigbrotr_archive.retain_history();
REVOKE ALL ON bigbrotr_archive.scientific_schema FROM PUBLIC;
