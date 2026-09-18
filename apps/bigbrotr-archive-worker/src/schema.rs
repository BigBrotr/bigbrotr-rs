//! Closed scientific declarations shared by private staging and explicit native
//! initialization. This module never adopts, migrates or authorizes a catalog.
use duckdb::Connection;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::sync::LazyLock;

const DECLARATION: &str = include_str!("../../../contracts/archive/v1/schema.json");
pub(crate) const TABLES: [&str; 5] = [
    "dictionaries",
    "events",
    "event_eligibility_facts",
    "event_receive_facts",
    "provenance_facts",
];
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Schema {
    format: String,
    tables: Vec<Table>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Table {
    name: String,
    columns: String,
}
static SCHEMA: LazyLock<Schema> = LazyLock::new(|| {
    let schema: Schema = serde_json::from_str(DECLARATION).expect("compiled archive schema");
    assert_eq!(schema.format, "bigbrotr-archive-schema/1");
    assert_eq!(schema.tables.len(), 6);
    for (actual, expected) in schema
        .tables
        .iter()
        .map(|t| t.name.as_str())
        .zip(TABLES.into_iter().chain(["archive_commits"]))
    {
        assert_eq!(actual, expected);
    }
    schema
});

pub(crate) fn create_stage(connection: &Connection) -> duckdb::Result<()> {
    connection.execute_batch("CREATE SCHEMA stage")?;
    for table in &SCHEMA.tables {
        connection.execute_batch(&format!(
            "CREATE TABLE stage.{} ({})",
            table.name, table.columns
        ))?;
    }
    Ok(())
}

/// Caller must hold exclusive initialization authority over a fresh catalog.
/// All statements use compiled identifiers/types; no arbitrary SQL input exists.
/// Engine destruction rolls back a failed transaction. A failed COMMIT remains
/// ambiguous to the parent even when a rollback/close subsequently succeeds.
pub(crate) fn initialize(connection: &Connection) -> duckdb::Result<()> {
    connection.execute_batch("BEGIN TRANSACTION")?;
    for table in &SCHEMA.tables {
        connection.execute_batch(&format!(
            "CREATE TABLE archive.main.{} ({})",
            table.name, table.columns
        ))?;
    }
    use std::fmt::Write;
    let mut hash = String::with_capacity(64);
    for byte in Sha256::digest(DECLARATION.as_bytes()) {
        write!(hash, "{byte:02x}").expect("String write");
    }
    connection.execute_batch(&format!("CALL ducklake_set_commit_message('archive', 'BigBrotr', 'initialize archive schema 1', extra_info => 'sha256:{hash}'); COMMIT"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{Engine, EngineLimits};
    use std::path::Path;
    fn open(temp: &Path, catalog: &Path, root: &Path) -> Engine {
        let engine = Engine::open(
            &temp.canonicalize().unwrap(),
            EngineLimits {
                memory_bytes: 134217728,
                temp_bytes: 67108864,
                threads: 1,
            },
        )
        .unwrap();
        engine
            .load_bundle(Path::new(
                &std::env::var_os("BIGBROTR_ARCHIVE_TEST_BUNDLE").expect("pinned bundle"),
            ))
            .unwrap();
        engine.connection.execute_batch(&format!("ATTACH 'ducklake:{}' AS archive (DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0, AUTOMATIC_MIGRATION false)", catalog.display(), root.display())).unwrap();
        engine
    }
    #[test]
    fn six_tables_and_one_schema_transaction_survive_a_fresh_native_reopen() {
        let home = tempfile::tempdir().unwrap();
        let temp = tempfile::tempdir().unwrap();
        let catalog = home.path().join("catalog.ducklake");
        let root = home.path().join("objects");
        let engine = open(temp.path(), &catalog, &root);
        initialize(&engine.connection).unwrap();
        drop(engine);
        let temp = tempfile::tempdir().unwrap();
        let engine = open(temp.path(), &catalog, &root);
        let actual: String = engine.connection.query_row("SELECT string_agg(table_name, ',' ORDER BY table_name) FROM information_schema.tables WHERE table_catalog='archive'", [], |r|r.get(0)).unwrap();
        assert_eq!(
            actual,
            "archive_commits,dictionaries,event_eligibility_facts,event_receive_facts,events,provenance_facts"
        );
        let snapshots: (u64,u64,u64) = engine.connection.query_row("SELECT count(*), max(snapshot_id), max(schema_version) FROM ducklake_snapshots('archive')", [], |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).unwrap();
        assert_eq!(snapshots, (2, 1, 1));
        let total: u64 = engine
            .connection
            .query_row(
                "SELECT sum(file_count) FROM ducklake_table_info('archive')",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(total, 0);
        let receipt_columns: String = engine.connection.query_row("SELECT string_agg(column_name, ',' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_catalog='archive' AND table_name='archive_commits'", [], |r|r.get(0)).unwrap();
        assert_eq!(
            receipt_columns,
            "dataset_id,schema_version,topology_generation,batch_digest,record_count,input_ranges,attempt_id,publication_ordinal"
        );
        let times: (String,String) = engine.connection.query_row("SELECT data_type,is_nullable FROM information_schema.columns WHERE table_catalog='archive' AND table_name='events' AND column_name='created_at'",[],|r|Ok((r.get(0)?,r.get(1)?))).unwrap();
        assert_eq!(times, ("UBIGINT".into(), "NO".into()));
    }
    #[test]
    fn late_ddl_failure_rolls_back_the_whole_scientific_schema() {
        let home = tempfile::tempdir().unwrap();
        let temp = tempfile::tempdir().unwrap();
        let catalog = home.path().join("catalog.ducklake");
        let root = home.path().join("objects");
        let engine = open(temp.path(), &catalog, &root);
        engine
            .connection
            .execute_batch("CREATE TABLE archive.main.archive_commits (canary UBIGINT)")
            .unwrap();
        assert!(initialize(&engine.connection).is_err());
        drop(engine);
        let temp = tempfile::tempdir().unwrap();
        let engine = open(temp.path(), &catalog, &root);
        let actual: String = engine.connection.query_row("SELECT string_agg(table_name, ',' ORDER BY table_name) FROM information_schema.tables WHERE table_catalog='archive'", [], |r|r.get(0)).unwrap();
        assert_eq!(actual, "archive_commits");
        let count: u64 = engine
            .connection
            .query_row(
                "SELECT count(*) FROM ducklake_snapshots('archive')",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 2);
    }
}
