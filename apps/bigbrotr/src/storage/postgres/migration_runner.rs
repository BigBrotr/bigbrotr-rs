//! Dedicated PostgreSQL control migrations; no application pool or implicit init.
use super::migrations::{self, AppliedMigration, MigrationPlan};
use std::{cell::Cell, fmt};
use tokio_postgres::{Client, IsolationLevel, Transaction};

// Database-local, transaction-scoped namespace. Every migrator and inspection
// uses this pair before reading the ledger; release proves that earlier work in
// the same migration transaction cannot later commit behind the observer.
const LOCK_NAMESPACE: i32 = 0x4242_7472;
const LOCK_MIGRATIONS: i32 = 0x4d49_4752;

#[derive(Debug)]
pub enum RunError {
    Backend,
    UnrecognizedSchema,
    InvalidLedger,
    IncompatibleEngine,
    PlaintextRoute,
    Plan(migrations::MigrationError),
    CommitUnknown,
}
impl fmt::Display for RunError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Backend => f.write_str("control migration backend unavailable or refused the operation"),
            Self::UnrecognizedSchema => f.write_str("existing control namespace lacks a recognized migration ledger"),
            Self::InvalidLedger => f.write_str("control migration ledger has invalid bounded metadata"),
            Self::IncompatibleEngine => f.write_str("control migrations require PostgreSQL 16.15 with fsync and full_page_writes enabled"),
            Self::PlaintextRoute => f.write_str("resolved PostgreSQL plaintext route is outside its declared local boundary or exceeds the address limit"),
            Self::Plan(error) => error.fmt(f),
            Self::CommitUnknown => f.write_str("migration COMMIT outcome unknown; resolve the same catalog target under migration serialization before retry"),
        }
    }
}
impl std::error::Error for RunError {}

fn backend(_: tokio_postgres::Error) -> RunError {
    RunError::Backend
}

#[derive(Debug)]
pub struct RunResult {
    pub plan: MigrationPlan,
    pub applied: usize,
}

/// The caller bounds the whole operation and owns the connection driver.
/// `commit_started` is set before polling COMMIT and survives caller timeout.
pub async fn run(
    client: &mut Client,
    target: u32,
    apply: bool,
    commit_started: &Cell<bool>,
) -> Result<RunResult, RunError> {
    let tx = client
        .build_transaction()
        .isolation_level(IsolationLevel::ReadCommitted)
        .read_only(!apply)
        .start()
        .await
        .map_err(backend)?;
    // All SQL is selected internally. Override search path, durability and these
    // deadlines locally; other settings remain trusted provisioning inputs.
    tx.batch_execute("SET LOCAL search_path = pg_catalog; SET LOCAL statement_timeout = '30s'; SET LOCAL lock_timeout = '5s'; SET LOCAL idle_in_transaction_session_timeout = '10s'; SET LOCAL synchronous_commit = on")
        .await.map_err(backend)?;
    let engine = tx.query_one(
        "SELECT pg_catalog.current_setting('server_version_num')::integer, pg_catalog.current_setting('fsync') = 'on', pg_catalog.current_setting('full_page_writes') = 'on'",
        &[],
    ).await.map_err(backend)?;
    if engine.try_get::<_, i32>(0).map_err(backend)? != 160015
        || !engine.try_get::<_, bool>(1).map_err(backend)?
        || !engine.try_get::<_, bool>(2).map_err(backend)?
    {
        return Err(RunError::IncompatibleEngine);
    }
    let lock = if apply {
        "SELECT pg_catalog.pg_advisory_xact_lock($1, $2)"
    } else {
        "SELECT pg_catalog.pg_advisory_xact_lock_shared($1, $2)"
    };
    tx.query_one(lock, &[&LOCK_NAMESPACE, &LOCK_MIGRATIONS])
        .await
        .map_err(backend)?;

    let applied = read_ledger(&tx).await?;
    let plan = migrations::plan(&applied, target).map_err(RunError::Plan)?;
    if !apply || plan.pending.is_empty() {
        tx.rollback().await.map_err(backend)?;
        return Ok(RunResult { plan, applied: 0 });
    }

    for migration in plan.pending {
        tx.batch_execute(migration.sql).await.map_err(backend)?;
        let checksum = migration.checksum();
        tx.execute(
            "INSERT INTO control.schema_versions (engine, migration, name, checksum, reader_min, reader_max, writer_floor, applied_at_us, applied_by) VALUES ($1::text, $2::bigint, $3::text, $4::bytea, $5::bigint, $6::bigint, $7::bigint, (EXTRACT(EPOCH FROM pg_catalog.clock_timestamp()) * 1000000)::bigint, CURRENT_USER)",
            &[&migrations::ENGINE, &i64::from(migration.version), &migration.name, &&checksum[..], &i64::from(migration.reader_min), &i64::from(migration.reader_max), &i64::from(migration.writer_floor)],
        ).await.map_err(backend)?;
    }
    commit_started.set(true);
    // Driver errors and dropped COMMIT futures cannot establish rollback. The
    // atomic ledger and the next transaction's lock resolve this exact target.
    tx.commit().await.map_err(|_| RunError::CommitUnknown)?;
    let count = plan.pending.len();
    Ok(RunResult {
        plan,
        applied: count,
    })
}

async fn read_ledger(tx: &Transaction<'_>) -> Result<Vec<AppliedMigration>, RunError> {
    let namespace: bool = tx
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = 'control')",
            &[],
        )
        .await
        .map_err(backend)?
        .try_get(0)
        .map_err(backend)?;
    if !namespace {
        return Ok(Vec::new());
    }
    let ordinary_table: bool = tx.query_one(
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'control' AND c.relname = 'schema_versions' AND c.relkind = 'r' AND c.relpersistence = 'p')",
        &[],
    ).await.map_err(backend)?.try_get(0).map_err(backend)?;
    if !ordinary_table {
        return Err(RunError::UnrecognizedSchema);
    }
    let limit =
        i64::try_from(migrations::catalog().len()).map_err(|_| RunError::InvalidLedger)? + 1;
    // Return bounded strings/bytes even if an operator damaged constraints.
    // The extra row discriminates excess/foreign history without an unbounded
    // result allocation. Engine identity is checked, not silently filtered away.
    let rows = tx.query(
        "SELECT CASE WHEN pg_catalog.octet_length(engine) BETWEEN 1 AND 128 THEN engine ELSE NULL END, migration::bigint, CASE WHEN pg_catalog.octet_length(name) BETWEEN 1 AND 128 THEN name ELSE NULL END, CASE WHEN pg_catalog.octet_length(checksum) = 32 THEN checksum::bytea ELSE NULL END, reader_min::bigint, reader_max::bigint, writer_floor::bigint FROM control.schema_versions LIMIT $1",
        &[&limit],
    ).await.map_err(backend)?;
    if rows.is_empty() || rows.len() > migrations::catalog().len() {
        return Err(RunError::InvalidLedger);
    }
    let mut result = Vec::with_capacity(rows.len());
    for row in rows {
        let engine: Option<String> = row.try_get(0).map_err(backend)?;
        let name: Option<String> = row.try_get(2).map_err(backend)?;
        if engine.as_deref() != Some(migrations::ENGINE) {
            return Err(RunError::InvalidLedger);
        }
        let name = name.ok_or(RunError::InvalidLedger)?;
        let checksum: Option<Vec<u8>> = row.try_get(3).map_err(backend)?;
        let checksum = checksum
            .and_then(|bytes| bytes.try_into().ok())
            .ok_or(RunError::InvalidLedger)?;
        let number = |index| -> Result<u32, RunError> {
            let value: i64 = row.try_get(index).map_err(backend)?;
            u32::try_from(value)
                .ok()
                .filter(|n| *n > 0)
                .ok_or(RunError::InvalidLedger)
        };
        result.push(AppliedMigration {
            version: number(1)?,
            name,
            checksum,
            reader_min: number(4)?,
            reader_max: number(5)?,
            writer_floor: number(6)?,
        });
    }
    // Never sort an arbitrarily damaged database table. Only this bounded,
    // validated result is ordered; physical MVCC scan cost is not a hard cap.
    result.sort_unstable_by_key(|row| row.version);
    Ok(result)
}
