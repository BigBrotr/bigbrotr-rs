//! Versioned PostgreSQL commit guard around native DuckLake metadata. These
//! privileged finite operations never expose the connection to a native worker.
//! The caller retains its driver and operation budget through join/cancellation.
use super::archive_attempt::ArchiveAttemptReceipt;
use bigbrotr_model::{DatasetId, OwnerResourceId};
use sha2::{Digest, Sha256};
use std::{cell::Cell, fmt};
use tokio_postgres::{Client, IsolationLevel, Transaction};
mod inspection;
const SQL: &str = include_str!("../../../../../migrations/catalog/0001_writer_guard.sql");
const WRITER_BODY: &str = "writer";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GuardError {
    InvalidInput,
    Backend,
    IncompatibleEngine,
    IncompatibleLayout,
    WrongCatalog,
    UnsafeRole,
    ExistingData,
    Conflict,
    CommitUnknown,
    ResourceLimit,
}
impl fmt::Display for GuardError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "archive catalog guard refused: {self:?}")
    }
}
impl std::error::Error for GuardError {}
fn backend(_: tokio_postgres::Error) -> GuardError {
    GuardError::Backend
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogIdentity {
    dataset: DatasetId,
    catalog: OwnerResourceId,
    root: String,
}
impl CatalogIdentity {
    /// Local placement only. S3 selection requires its separate qualified adapter;
    /// no fallback or filesystem probing occurs in this PostgreSQL value type.
    pub fn local(
        dataset: DatasetId,
        catalog: OwnerResourceId,
        root: &str,
    ) -> Result<Self, GuardError> {
        if root.len() < 2
            || root.len() > 4096
            || !root.starts_with('/')
            || !root.ends_with('/')
            || root.contains(['\0', '\n', '\r'])
            || root.contains("//")
            || root.split('/').any(|part| matches!(part, "." | ".."))
        {
            return Err(GuardError::InvalidInput);
        }
        Ok(Self {
            dataset,
            catalog,
            root: root.into(),
        })
    }
    pub const fn dataset(&self) -> DatasetId {
        self.dataset
    }
    pub const fn catalog(&self) -> OwnerResourceId {
        self.catalog
    }
    pub fn root(&self) -> &str {
        &self.root
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Activation {
    Activated,
    Observed { current: bool, enabled: bool },
}

fn body(delimiter: &str) -> &str {
    let delimiter = format!("${delimiter}$");
    SQL.split(&delimiter)
        .nth(1)
        .expect("compiled guard function delimiter")
}
fn checksum() -> [u8; 32] {
    Sha256::digest(SQL.as_bytes()).into()
}
fn identifier(value: &str) -> Result<String, GuardError> {
    if value.is_empty() || value.len() > 63 || value.contains('\0') {
        return Err(GuardError::InvalidInput);
    }
    Ok(format!("\"{}\"", value.replace('"', "\"\"")))
}
async fn begin(client: &mut Client, exclusive: bool) -> Result<Transaction<'_>, GuardError> {
    let tx = client
        .build_transaction()
        .isolation_level(IsolationLevel::ReadCommitted)
        .start()
        .await
        .map_err(backend)?;
    tx.batch_execute("SET LOCAL search_path=pg_catalog; SET LOCAL statement_timeout='20s'; SET LOCAL lock_timeout='5s'; SET LOCAL idle_in_transaction_session_timeout='10s'; SET LOCAL synchronous_commit=on").await.map_err(backend)?;
    let valid:bool=tx.query_one("SELECT current_setting('server_version_num')::integer=160015 AND current_setting('fsync')='on' AND current_setting('full_page_writes')='on'",&[]).await.map_err(backend)?.try_get(0).map_err(backend)?;
    if !valid {
        return Err(GuardError::IncompatibleEngine);
    }
    let lock = if exclusive {
        "SELECT pg_advisory_xact_lock(1111651442,1128354865)"
    } else {
        "SELECT pg_advisory_xact_lock_shared(1111651442,1128354865)"
    };
    tx.query_one(lock, &[]).await.map_err(backend)?;
    Ok(tx)
}

/// Explicit fresh installation after the isolated native bootstrap is joined.
/// The bootstrap login must have created this empty pinned native catalog without
/// role membership or administrative privilege. Transfer ownership and revoke
/// its DDL/DML in the same transaction as all statement guards. No native metadata
/// values/columns are rewritten. A pre-existing guarded catalog is inspected only.
/// Caller reserves at least64MiB for bounded inspection responses and owns timeout.
pub async fn install(
    client: &mut Client,
    identity: &CatalogIdentity,
    bootstrap_login: &str,
    commit_started: &Cell<bool>,
) -> Result<bool, GuardError> {
    let bootstrap = identifier(bootstrap_login)?;
    let tx = begin(client, true).await?;
    let exists: bool = tx
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname='bigbrotr_archive')",
            &[],
        )
        .await
        .map_err(backend)?
        .try_get(0)
        .map_err(backend)?;
    if exists {
        inspection::installed(&tx, identity).await?;
        tx.rollback().await.map_err(backend)?;
        return Ok(false);
    }
    let tables = inspection::native(&tx).await?;
    tx.batch_execute(&format!(
        "LOCK TABLE {} IN ACCESS EXCLUSIVE MODE",
        tables
            .iter()
            .map(|t| format!("public.{t}"))
            .collect::<Vec<_>>()
            .join(",")
    ))
    .await
    .map_err(backend)?;
    inspection::fresh(&tx, identity).await?;
    inspection::bootstrap(&tx, bootstrap_login).await?;
    tx.batch_execute(SQL).await.map_err(backend)?;
    let mut edits = format!(
        "REVOKE ALL ON SCHEMA public FROM {bootstrap}; REVOKE CREATE ON SCHEMA public FROM PUBLIC; REVOKE ALL ON ALL TABLES IN SCHEMA public FROM PUBLIC;"
    );
    for table in tables {
        edits.push_str(&format!("ALTER TABLE public.{table} OWNER TO CURRENT_USER; REVOKE ALL ON public.{table} FROM {bootstrap}; CREATE TRIGGER bigbrotr_writer_guard BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON public.{table} FOR EACH STATEMENT EXECUTE FUNCTION bigbrotr_archive.require_writer();"));
    }
    tx.batch_execute(&edits).await.map_err(backend)?;
    inspection::revoked(&tx, bootstrap_login).await?;
    let layout = inspection::layout(&tx).await?;
    tx.execute("INSERT INTO bigbrotr_archive.installation VALUES(true,$1::bytea,$2::bytea,$3::text,1,$4::bytea,$5::bytea)", &[&&identity.dataset.as_bytes()[..],&&identity.catalog.as_bytes()[..],&identity.root,&&checksum()[..],&&layout[..]]).await.map_err(backend)?;
    inspection::installed(&tx, identity).await?;
    commit_started.set(true);
    tx.commit().await.map_err(|_| GuardError::CommitUnknown)?;
    Ok(true)
}

/// Read-only verification, including actual trigger/function/schema state. This
/// is not a receipt lookup and grants no native write authority.
pub async fn inspect(client: &mut Client, identity: &CatalogIdentity) -> Result<(), GuardError> {
    let tx = begin(client, false).await?;
    inspection::installed(&tx, identity).await?;
    tx.rollback().await.map_err(backend)
}

/// Each attempt requires a separately provisioned login with this immutable name.
/// Provisioning/secret custody is a separate boundary; never reuse or re-enable a
/// credential lifetime on an observation/retry. The manager may grant only the
/// actual native table DML required here; the worker never receives manager access.
pub fn writer_login(attempt: &ArchiveAttemptReceipt) -> String {
    let mut name = String::from("bb_archive_");
    use std::fmt::Write;
    for byte in attempt.request().attempt_id() {
        write!(name, "{byte:02x}").expect("String write");
    }
    name
}

/// Import an actually retained Control intent. Monotonic catalog activation is
/// serialized against real native commits by the same owner-row lock. A matching
/// historical/current observation never issues fresh credentials or a launch.
/// A lost COMMIT resolves through this same immutable request in a new connection.
pub async fn activate(
    client: &mut Client,
    identity: &CatalogIdentity,
    receipt: &ArchiveAttemptReceipt,
    commit_started: &Cell<bool>,
) -> Result<Activation, GuardError> {
    let request = receipt.request();
    if request.dataset() != identity.dataset || request.catalog() != identity.catalog {
        return Err(GuardError::WrongCatalog);
    }
    let login = writer_login(receipt);
    let quoted = identifier(&login)?;
    let tx = begin(client, false).await?;
    let state = tx
        .query_one(
            "SELECT ordinal::text,enabled FROM bigbrotr_archive.owner WHERE singleton FOR UPDATE",
            &[],
        )
        .await
        .map_err(backend)?;
    inspection::installed(&tx, identity).await?;
    let current = state
        .try_get::<_, &str>(0)
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .ok_or(GuardError::IncompatibleLayout)?;
    let enabled: bool = state.try_get(1).map_err(backend)?;
    let ordinal = request.ordinal().to_string();
    let topology = request.topology_generation().get().to_string();
    let row=tx.query_opt("SELECT attempt_id=$2::bytea AND coordinator_incarnation=$3::bytea AND worker_incarnation=$4::bytea AND topology_generation=$5::text::numeric AND batch_digest=$6::bytea AND login_name=$7::name AND login_oid=(SELECT oid FROM pg_roles WHERE rolname=$7::name) FROM bigbrotr_archive.attempts WHERE ordinal=$1::text::numeric", &[&ordinal,&&request.attempt_id()[..],&&request.coordinator().as_bytes()[..],&&request.worker().as_bytes()[..],&topology,&&request.batch_digest().as_bytes()[..],&login]).await.map_err(backend)?;
    if let Some(row) = row {
        if row.try_get::<_, Option<bool>>(0).map_err(backend)? != Some(true) {
            return Err(GuardError::Conflict);
        }
        tx.rollback().await.map_err(backend)?;
        return Ok(Activation::Observed {
            current: current == request.ordinal(),
            enabled: current == request.ordinal() && enabled,
        });
    }
    if request.ordinal() <= current {
        return Err(GuardError::Conflict);
    }
    inspection::writer(&tx, &login).await?;
    tx.execute("INSERT INTO bigbrotr_archive.attempts SELECT $1::text::numeric,$2::bytea,$3::bytea,$4::bytea,$5::text::numeric,$6::bytea,$7::name,oid FROM pg_roles WHERE rolname=$7::name", &[&ordinal,&&request.attempt_id()[..],&&request.coordinator().as_bytes()[..],&&request.worker().as_bytes()[..],&topology,&&request.batch_digest().as_bytes()[..],&login]).await.map_err(backend)?;
    let tables = inspection::native(&tx).await?;
    let mut grants = format!("GRANT USAGE ON SCHEMA public TO {quoted};");
    for table in tables {
        grants.push_str(&format!(
            "GRANT SELECT,INSERT,UPDATE,DELETE ON public.{table} TO {quoted};"
        ));
    }
    tx.batch_execute(&grants).await.map_err(backend)?;
    let changed=tx.execute("UPDATE bigbrotr_archive.owner SET ordinal=$1::text::numeric,enabled=true WHERE singleton AND ordinal<$1::text::numeric", &[&ordinal]).await.map_err(backend)?;
    if changed != 1 {
        return Err(GuardError::Conflict);
    }
    commit_started.set(true);
    tx.commit().await.map_err(|_| GuardError::CommitUnknown)?;
    Ok(Activation::Activated)
}
