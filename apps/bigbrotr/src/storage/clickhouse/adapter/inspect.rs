//! Inspect actual engine identity and the closed physical schema. Query metadata
//! is bounded and typed; matching names alone never adopts a foreign table.
use super::{PhysicalError, operation::QueryOwner};
use crate::storage::{
    clickhouse::schema::{self, IndexTables},
    postgres::IndexTarget,
};
use clickhouse::{Row, RowOwned, RowRead, query::Query};
use serde::Deserialize;

pub(super) async fn collect<R: RowOwned + RowRead>(
    owner: &mut QueryOwner,
    query: Query,
    limit: usize,
) -> Result<Vec<R>, PhysicalError> {
    let id = owner.begin()?;
    let mut cursor = query
        // Metadata cardinality is independent of the accepted insert batch
        // ceiling. Every caller supplies its closed 1/10/175-row bound; using
        // the data ceiling here could create tables and then reject their own
        // 160-column inventory for a valid smaller source batch.
        .with_setting("max_result_rows", limit.to_string())
        .with_setting("query_id", &id)
        .fetch::<R>()
        .map_err(|error| owner.failed(&id, error))?;
    let mut rows = Vec::new();
    while let Some(row) = cursor
        .next()
        .await
        .map_err(|error| owner.failed(&id, error))?
    {
        if rows.len() >= limit {
            return Err(PhysicalError::Limit);
        }
        rows.try_reserve_exact(1)
            .map_err(|_| PhysicalError::Allocation)?;
        rows.push(row);
    }
    owner.completed(&id)?;
    Ok(rows)
}

#[derive(Row, Deserialize)]
struct ServerRow {
    version: String,
    user: String,
}
#[derive(Row, Deserialize)]
struct DatabaseRow {
    engine: String,
}
#[derive(Row, Deserialize)]
struct TableRow {
    name: String,
    uuid: String,
    engine: String,
    engine_full: String,
    partition_key: String,
    sorting_key: String,
    primary_key: String,
    sampling_key: String,
    storage_policy: String,
    dependent_databases: u64,
    dependent_tables: u64,
}
#[derive(Row, Deserialize)]
struct ColumnRow {
    table: String,
    name: String,
    r#type: String,
    position: u64,
    default_kind: String,
    default_expression: String,
}
#[derive(Row, Deserialize)]
struct DiskRow {
    name: String,
    r#type: String,
}
#[derive(Row, Deserialize)]
struct PolicyRow {
    local_only: u8,
}
#[derive(Row, Deserialize)]
struct LimitRow {
    value: String,
    min: Option<String>,
    max: Option<String>,
}
#[derive(Row, Deserialize)]
struct CountRow {
    n: u64,
}
#[derive(Row, Deserialize)]
struct DefinitionRow {
    matches: u8,
}

pub(super) async fn engine(
    owner: &mut QueryOwner,
    tables: &IndexTables,
    expected_user: &str,
    writer: bool,
) -> Result<(), PhysicalError> {
    let query = owner
        .client
        .query("SELECT version() AS version, currentUser() AS user");
    let rows = collect::<ServerRow>(owner, query, 1).await?;
    if rows.len() != 1 || rows[0].version != schema::SERVER_VERSION || rows[0].user != expected_user
    {
        return Err(PhysicalError::IncompatibleEngine);
    }
    let query = owner
        .client
        .query("SELECT engine FROM system.databases WHERE name={database:String}")
        .param("database", tables.database());
    let rows = collect::<DatabaseRow>(owner, query, 1).await?;
    if rows.len() != 1 || rows[0].engine != "Atomic" {
        return Err(PhysicalError::Schema);
    }
    let query = owner
        .client
        .query("SELECT name, type FROM system.disks WHERE name='default'");
    let rows = collect::<DiskRow>(owner, query, 1).await?;
    if rows.len() != 1 || rows[0].name != "default" || rows[0].r#type != "Local" {
        return Err(PhysicalError::IncompatibleEngine);
    }
    let query = owner.client.query("SELECT toUInt8(length(disks)=1 AND disks[1]='default') AS local_only FROM system.storage_policies WHERE policy_name='default'");
    let rows = collect::<PolicyRow>(owner, query, 1).await?;
    if rows.len() != 1 || rows[0].local_only != 1 {
        return Err(PhysicalError::IncompatibleEngine);
    }
    if writer {
        let query = owner.client.query("SELECT value, min, max FROM system.settings WHERE name='max_concurrent_queries_for_user'");
        let rows = collect::<LimitRow>(owner, query, 1).await?;
        // Fixed account constraint, not merely a per-request setting that a new
        // process or late request can override. The control identity is separate.
        if rows.len() != 1
            || rows[0].value != "1"
            || rows[0].min.as_deref() != Some("1")
            || rows[0].max.as_deref() != Some("1")
        {
            return Err(PhysicalError::IncompatibleEngine);
        }
    }
    Ok(())
}

pub(super) async fn empty_database(
    owner: &mut QueryOwner,
    tables: &IndexTables,
) -> Result<(), PhysicalError> {
    let query = owner
        .client
        .query("SELECT count() AS n FROM system.tables WHERE database={database:String}")
        .param("database", tables.database());
    let rows = collect::<CountRow>(owner, query, 1).await?;
    if rows.len() != 1 || rows[0].n != 0 {
        return Err(PhysicalError::Schema);
    }
    Ok(())
}

pub(super) async fn installed(
    owner: &mut QueryOwner,
    tables: &IndexTables,
    require_empty: bool,
) -> Result<(), PhysicalError> {
    let query = owner.client.query("SELECT name,toString(uuid) AS uuid,engine,engine_full,partition_key,sorting_key,primary_key,sampling_key,storage_policy,length(dependencies_database) AS dependent_databases,length(dependencies_table) AS dependent_tables FROM system.tables WHERE database={database:String} ORDER BY name").param("database", tables.database());
    let rows = collect::<TableRow>(owner, query, 10).await?;
    if rows.len() != 10 {
        return Err(PhysicalError::Schema);
    }
    for target in IndexTarget::ALL {
        let row = rows
            .iter()
            .find(|row| row.name == target.as_str())
            .ok_or(PhysicalError::Schema)?;
        let key = schema::sort_key(target);
        if row.uuid != schema::uuid_text(tables.uuid(target))
            || row.engine != "MergeTree"
            || row.engine_full != schema::engine_expression(target)
            || !row.partition_key.is_empty()
            || row.sorting_key != key
            || row.primary_key != key
            || !row.sampling_key.is_empty()
            || row.storage_policy != "default"
            || row.dependent_databases != 0
            || row.dependent_tables != 0
        {
            return Err(PhysicalError::Schema);
        }
        // engine_full covers only the storage AST. Per-column TTL, codecs,
        // constraints and skip indexes live elsewhere in CREATE. Canonicalize
        // both complete definitions with the same selected server parser; the
        // expected input is our closed declaration, never observed metadata.
        let query = owner.client.query("SELECT toUInt8(formatQuerySingleLine(create_table_query)=formatQuerySingleLine({expected:String})) AS matches FROM system.tables WHERE database={database:String} AND name={table:String}")
            .param("expected", tables.create_table(target))
            .param("database", tables.database())
            .param("table", target.as_str())
            .with_setting("show_table_uuid_in_table_create_query_if_not_nil", "1");
        let definitions = collect::<DefinitionRow>(owner, query, 1).await?;
        if definitions.len() != 1 || definitions[0].matches != 1 {
            return Err(PhysicalError::Schema);
        }
    }
    let query = owner.client.query("SELECT table,name,type,position,default_kind,default_expression FROM system.columns WHERE database={database:String} ORDER BY table,position").param("database", tables.database());
    let rows = collect::<ColumnRow>(owner, query, 175).await?;
    let expected_count: usize = IndexTarget::ALL
        .into_iter()
        .map(|target| schema::columns(target).count())
        .sum();
    if rows.len() != expected_count {
        return Err(PhysicalError::Schema);
    }
    for target in IndexTarget::ALL {
        let actual: Vec<_> = rows
            .iter()
            .filter(|row| row.table == target.as_str())
            .collect();
        let expected = schema::columns(target);
        if actual.len() != schema::columns(target).count() {
            return Err(PhysicalError::Schema);
        }
        for (position, (row, column)) in actual.into_iter().zip(expected).enumerate() {
            if row.name != column.name
                || row.r#type != column.ty
                || row.position != position as u64 + 1
                || !row.default_kind.is_empty()
                || !row.default_expression.is_empty()
            {
                return Err(PhysicalError::Schema);
            }
        }
    }
    let query = owner
        .client
        .query("SELECT count() AS n FROM system.projections WHERE database={database:String}")
        .param("database", tables.database());
    let rows = collect::<CountRow>(owner, query, 1).await?;
    if rows.len() != 1 || rows[0].n != 0 {
        return Err(PhysicalError::Schema);
    }
    if require_empty {
        for target in IndexTarget::ALL {
            let query = owner.client.query(&format!(
                "SELECT count() AS n FROM {}",
                tables.qualified(target)
            ));
            let rows = collect::<CountRow>(owner, query, 1).await?;
            if rows.len() != 1 || rows[0].n != 0 {
                return Err(PhysicalError::Schema);
            }
        }
    }
    Ok(())
}
