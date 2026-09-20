//! Version1 logical schema inspection. Native DML can encode logical DDL, so the
//! PG object hash and an immutable version marker cannot replace these checks.
//! Install holds every native table lock; installed callers hold owner FOR UPDATE.
use super::{CatalogIdentity, GuardError, backend, inspection::require};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, sync::LazyLock};
use tokio_postgres::Transaction;

pub(super) const SQL: &str =
    include_str!("../../../../../../migrations/catalog/0002_scientific_schema.sql");
const DECLARATION: &str = include_str!("../../../../../../contracts/archive/v1/schema.json");
const LAYOUT: &str = include_str!("../../../../../../contracts/archive/v1/native-layout.json");
type Column = (i64, String, String, Option<i64>, bool);
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Layout {
    format: String,
    schema_sha256: String,
    tables: BTreeMap<String, Vec<Column>>,
}
static EXPECTED: LazyLock<Layout> = LazyLock::new(|| {
    let result: Layout = serde_json::from_str(LAYOUT).expect("compiled scientific layout");
    assert_eq!(result.format, "bigbrotr-archive-native-layout/1");
    assert_eq!(
        result.schema_sha256,
        bigbrotr_model::Digest256::from_bytes(Sha256::digest(DECLARATION).into()).to_string()
    );
    assert_eq!(result.tables.len(), 6);
    assert_eq!(result.tables.values().map(Vec::len).sum::<usize>(), 203);
    result
});
fn hashes() -> [[u8; 32]; 3] {
    [DECLARATION, LAYOUT, SQL].map(|s| Sha256::digest(s).into())
}
pub(super) async fn bind(tx: &Transaction<'_>) -> Result<(), GuardError> {
    let [declaration, layout, sql] = hashes();
    tx.execute("INSERT INTO bigbrotr_archive.scientific_schema VALUES(true,1,$1::bytea,$2::bytea,$3::bytea,1)", &[&&declaration[..],&&layout[..],&&sql[..]]).await.map_err(backend)?;
    Ok(())
}
pub(super) async fn installed(
    tx: &Transaction<'_>,
    identity: &CatalogIdentity,
) -> Result<(), GuardError> {
    let present: bool = tx
        .query_one(
            "SELECT to_regclass('bigbrotr_archive.scientific_schema') IS NOT NULL",
            &[],
        )
        .await
        .map_err(backend)?
        .try_get(0)
        .map_err(backend)?;
    if present != identity.scientific {
        return Err(GuardError::IncompatibleLayout);
    }
    if !present {
        return Ok(());
    }
    let [declaration, layout, sql] = hashes();
    require(tx, "SELECT count(*)=1 AND count(*) FILTER(WHERE (singleton AND format_version=1 AND declaration_checksum=$1::bytea AND native_layout_checksum=$2::bytea AND sql_checksum=$3::bytea AND initialization_snapshot=1) IS TRUE)=1 FROM bigbrotr_archive.scientific_schema", &[&&declaration[..],&&layout[..],&&sql[..]], GuardError::IncompatibleLayout).await?;
    live(tx).await
}

async fn live(tx: &Transaction<'_>) -> Result<(), GuardError> {
    // Only declared placement/version and zero-inlining settings are selected.
    // A new persistent option requires explicit compatibility qualification.
    require(tx, "SELECT count(*)<=12 AND count(*)=count(DISTINCT ROW(key,scope,scope_id)) AND count(*) FILTER(WHERE (key IS NOT NULL AND value IS NOT NULL AND ((key IN ('version','data_path','encrypted') AND scope IS NULL AND scope_id IS NULL) OR (key='created_by' AND value='DuckDB d8cdaa33fd' AND scope IS NULL AND scope_id IS NULL) OR (key='data_inlining_row_limit' AND value='0' AND ((scope IS NULL AND scope_id IS NULL) OR (scope='schema' AND scope_id=0) OR (scope='table' AND scope_id BETWEEN 1 AND 6))))) IS TRUE)=count(*) AND count(*) FILTER(WHERE key='created_by')=1 FROM (SELECT * FROM public.ducklake_metadata LIMIT 13)m", &[], GuardError::IncompatibleLayout).await?;

    require(tx, "SELECT count(*)=1 AND count(*) FILTER(WHERE (schema_id=0 AND schema_uuid<>'00000000-0000-0000-0000-000000000000'::uuid AND begin_snapshot=0 AND end_snapshot IS NULL AND schema_name='main' AND path='main/' AND path_is_relative) IS TRUE)=1 FROM (SELECT * FROM public.ducklake_schema LIMIT 2)s", &[], GuardError::IncompatibleLayout).await?;
    let tables = tx.query("SELECT left(table_name,64),table_id,(table_id BETWEEN 1 AND 6 AND table_uuid<>'00000000-0000-0000-0000-000000000000'::uuid AND begin_snapshot=1 AND end_snapshot IS NULL AND schema_id=0 AND path=table_name||'/' AND path_is_relative) IS TRUE FROM public.ducklake_table LIMIT 7", &[]).await.map_err(backend)?;
    if tables.len() != 6 {
        return Err(GuardError::IncompatibleLayout);
    }
    let mut actual: BTreeMap<String, Vec<Column>> = BTreeMap::new();
    let mut ids = BTreeMap::new();
    for row in tables {
        let name: String = row
            .try_get::<_, Option<String>>(0)
            .map_err(backend)?
            .ok_or(GuardError::IncompatibleLayout)?;
        if !row.try_get::<_, bool>(2).map_err(backend)? || !EXPECTED.tables.contains_key(&name) {
            return Err(GuardError::IncompatibleLayout);
        }
        let id: i64 = row.try_get(1).map_err(backend)?;
        if ids.insert(id, name.clone()).is_some() || actual.insert(name, Vec::new()).is_some() {
            return Err(GuardError::IncompatibleLayout);
        }
    }
    require(
        tx,
        "SELECT count(DISTINCT table_uuid)=6 FROM public.ducklake_table",
        &[],
        GuardError::IncompatibleLayout,
    )
    .await?;
    // At most204 bounded rows; unknown/oversized strings never cross the wire in
    // full. SQL NULL must fail every required predicate, not disappear in bool_and.
    let columns = tx.query("SELECT table_id,column_id,left(column_name,64),left(column_type,32),parent_column,nulls_allowed,(begin_snapshot=1 AND end_snapshot IS NULL AND column_order=column_id AND initial_default IS NULL AND default_value='NULL' AND default_value_type=CASE WHEN column_type IN ('struct','list') THEN '' ELSE 'literal' END AND default_value_dialect='duckdb' AND octet_length(column_name)<=64 AND octet_length(column_type)<=32) IS TRUE FROM public.ducklake_column ORDER BY table_id,column_id LIMIT 204", &[]).await.map_err(backend)?;
    if columns.len() != 203 {
        return Err(GuardError::IncompatibleLayout);
    }
    for row in columns {
        if !row.try_get::<_, bool>(6).map_err(backend)? {
            return Err(GuardError::IncompatibleLayout);
        }
        let id: i64 = row
            .try_get::<_, Option<i64>>(0)
            .map_err(backend)?
            .ok_or(GuardError::IncompatibleLayout)?;
        let name = ids.get(&id).ok_or(GuardError::IncompatibleLayout)?;
        let column = (
            row.try_get::<_, Option<i64>>(1)
                .map_err(backend)?
                .ok_or(GuardError::IncompatibleLayout)?,
            row.try_get::<_, Option<String>>(2)
                .map_err(backend)?
                .ok_or(GuardError::IncompatibleLayout)?,
            row.try_get::<_, Option<String>>(3)
                .map_err(backend)?
                .ok_or(GuardError::IncompatibleLayout)?,
            row.try_get::<_, Option<i64>>(4).map_err(backend)?,
            row.try_get::<_, Option<bool>>(5)
                .map_err(backend)?
                .ok_or(GuardError::IncompatibleLayout)?,
        );
        actual.get_mut(name).expect("checked table").push(column);
    }
    if actual != EXPECTED.tables {
        return Err(GuardError::IncompatibleLayout);
    }
    require(tx,"SELECT count(*)=6 AND count(DISTINCT table_id)=6 AND count(*) FILTER(WHERE (table_id BETWEEN 1 AND 6 AND begin_snapshot=1 AND schema_version=1) IS TRUE)=6 FROM (SELECT * FROM public.ducklake_schema_versions LIMIT 7)s", &[], GuardError::IncompatibleLayout).await?;
    require(tx,"SELECT EXISTS(SELECT 1 FROM (SELECT * FROM public.ducklake_snapshot ORDER BY snapshot_id DESC LIMIT 1)s WHERE (schema_version=1 AND next_catalog_id=7 AND next_file_id>=0 AND snapshot_id>=1 AND snapshot_time IS NOT NULL) IS TRUE)", &[], GuardError::IncompatibleLayout).await?;
    // These metadata classes can alter interpretation; version1 declares none.
    // Data/files/statistics and snapshot history are deliberately not scanned.
    for table in [
        "ducklake_column_tag",
        "ducklake_tag",
        "ducklake_view",
        "ducklake_macro",
        "ducklake_macro_impl",
        "ducklake_macro_parameters",
        "ducklake_partition_info",
        "ducklake_partition_column",
        "ducklake_sort_info",
        "ducklake_sort_expression",
    ] {
        require(
            tx,
            &format!("SELECT NOT EXISTS(SELECT 1 FROM public.{table})"),
            &[],
            GuardError::IncompatibleLayout,
        )
        .await?;
    }
    Ok(())
}

pub(super) async fn fresh(
    tx: &Transaction<'_>,
    identity: &CatalogIdentity,
) -> Result<(), GuardError> {
    virgin(tx, identity).await?;
    require(tx,"SELECT NOT EXISTS(SELECT 1 FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public') AND NOT EXISTS(SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public')",&[],GuardError::IncompatibleLayout).await
}

/// Exact initial metadata, also reusable after the writer guard is installed.
pub(super) async fn virgin(
    tx: &Transaction<'_>,
    identity: &CatalogIdentity,
) -> Result<(), GuardError> {
    super::inspection::metadata(tx, identity).await?;
    live(tx).await?;
    for table in super::inspection::native(tx).await? {
        let predicate = match table.as_str() {
            "ducklake_schema"
            | "ducklake_table"
            | "ducklake_column"
            | "ducklake_schema_versions" => continue,
            "ducklake_snapshot" => {
                "count(*)=2 AND count(*) FILTER(WHERE (snapshot_id=0 AND schema_version=0 AND next_catalog_id=1 AND next_file_id=0 AND snapshot_time IS NOT NULL) IS TRUE)=1 AND count(*) FILTER(WHERE (snapshot_id=1 AND schema_version=1 AND next_catalog_id=7 AND next_file_id=0 AND snapshot_time IS NOT NULL) IS TRUE)=1"
            }
            "ducklake_snapshot_changes" => continue,
            "ducklake_metadata" => {
                "count(*)=4 AND count(*) FILTER(WHERE key='created_by' AND value='DuckDB d8cdaa33fd' AND scope IS NULL AND scope_id IS NULL)=1"
            }
            _ => "count(*)=0",
        };
        require(
            tx,
            &format!("SELECT {predicate} FROM public.{table}"),
            &[],
            GuardError::ExistingData,
        )
        .await?;
    }
    // Compare the native unordered CREATE set, plus the two complete messages.
    let names: Vec<String> = EXPECTED
        .tables
        .keys()
        .map(|s| format!("created_table:\"main\".\"{s}\""))
        .collect();
    let extra = format!("sha256:{}", EXPECTED.schema_sha256);
    require(tx,"SELECT count(*)=2 AND count(*) FILTER(WHERE (snapshot_id=0 AND changes_made='created_schema:\"main\"' AND author IS NULL AND commit_message IS NULL AND commit_extra_info IS NULL) IS TRUE)=1 AND count(*) FILTER(WHERE (snapshot_id=1 AND ARRAY(SELECT x FROM unnest(string_to_array(changes_made,','))x ORDER BY x COLLATE \"C\")=$1::text[] AND author='BigBrotr' AND commit_message='initialize archive schema 1' AND commit_extra_info=$2::text) IS TRUE)=1 FROM public.ducklake_snapshot_changes", &[&names,&extra],GuardError::ExistingData).await?;
    Ok(())
}
