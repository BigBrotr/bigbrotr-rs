//! Private native staging schema. Public archive migration/identity is separate.
use crate::engine::Engine;

pub(super) const TABLES: [&str; 5] = [
    "dictionaries",
    "events",
    "event_eligibility_facts",
    "event_receive_facts",
    "provenance_facts",
];
const COMMON: &str = "source_record_id STRUCT(dataset_id BLOB, shard_id UINTEGER, owner_epoch UBIGINT, sequence UBIGINT) NOT NULL,
record_schema_version USMALLINT NOT NULL, record_kind VARCHAR NOT NULL,
visibility_scope_id STRUCT(dataset_id BLOB, dictionary_kind USMALLINT, allocation_domain BLOB, ordinal UBIGINT) NOT NULL,
payload_digest BLOB NOT NULL, recorded_at BIGINT NOT NULL";
const REFERENCE: &str =
    "STRUCT(dataset_id BLOB, dictionary_kind USMALLINT, allocation_domain BLOB, ordinal UBIGINT)";
const CLOCK: &str = "STRUCT(clock_id STRUCT(dataset_id BLOB, dictionary_kind USMALLINT, allocation_domain BLOB, ordinal UBIGINT), resolution_us UBIGINT, uncertainty_us UBIGINT, anomaly_flags UINTEGER)";
const SOURCE: &str =
    "STRUCT(dataset_id BLOB, shard_id UINTEGER, owner_epoch UBIGINT, sequence UBIGINT)";

pub(super) fn create(engine: &Engine) -> duckdb::Result<()> {
    engine.connection.execute_batch("CREATE SCHEMA stage")?;
    let fields = [
        format!("reference {REFERENCE} NOT NULL, normalization_version USMALLINT NOT NULL, descriptor_schema VARCHAR NOT NULL, descriptor_version USMALLINT NOT NULL,
descriptor_value STRUCT(event_id BLOB, class VARCHAR, url VARCHAR, clock VARCHAR, vantage VARCHAR, software VARCHAR,
method_id {REFERENCE}, public_key BLOB, route_class VARCHAR, software_version VARCHAR, label VARCHAR, visibility_scope_id {REFERENCE},
input VARCHAR, algorithm VARCHAR, capability VARCHAR, output_unit VARCHAR, failure_taxonomy VARCHAR, algorithm_version USMALLINT,
protocol_revision VARCHAR, format VARCHAR, format_version USMALLINT, source_dataset_id BLOB, source_artifact_digest BLOB) NOT NULL"),
        format!("event_id BLOB NOT NULL, pubkey BLOB NOT NULL, created_at UBIGINT NOT NULL, kind USMALLINT NOT NULL, tags VARCHAR[][] NOT NULL, content VARCHAR NOT NULL, event_ordinal {REFERENCE} NOT NULL"),
        format!("event_id BLOB NOT NULL, body_digest BLOB NOT NULL, sig BLOB NOT NULL, first_eligible_at BIGINT NOT NULL, origin VARCHAR NOT NULL, import_lineage {REFERENCE}"),
        format!("event_id BLOB NOT NULL, received_at BIGINT NOT NULL, observer_id {REFERENCE} NOT NULL, clock_quality {CLOCK} NOT NULL, corrects {SOURCE}"),
        format!("event_id BLOB NOT NULL, endpoint_id {REFERENCE} NOT NULL, observer_id {REFERENCE} NOT NULL, access_scope_id {REFERENCE} NOT NULL, received_at BIGINT NOT NULL, clock_quality {CLOCK} NOT NULL, verification_method_id {REFERENCE} NOT NULL, corrects {SOURCE}"),
    ];
    for (table, fields) in TABLES.iter().zip(fields) {
        // Every identifier and type is a closed implementation constant.
        engine
            .connection
            .execute_batch(&format!("CREATE TABLE stage.{table} ({COMMON}, {fields})"))?;
    }
    Ok(())
}
