//! Versioned PostgreSQL commit guard around native DuckLake metadata. These
//! privileged finite operations never expose the connection to a native worker.
//! The caller retains its driver and operation budget through join/cancellation.
use super::archive_attempt::{ArchiveAttemptReceipt, ArchiveAttemptRequest};
use bigbrotr_model::{DatasetId, OwnerResourceId};
use sha2::{Digest, Sha256};
use std::{cell::Cell, fmt};
use tokio_postgres::{Client, IsolationLevel, Transaction};
use zeroize::Zeroizing;
mod credentials;
mod inspection;
mod installation;
mod privileges;
mod scientific;
pub use installation::{InstallationRoles, install_for_manager};
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
    Entropy,
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
    scientific: bool,
}
impl CatalogIdentity {
    /// Scientific schema1 on local placement. S3 selection requires its separate qualified adapter;
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
            // These legal Unix names become patterns in native registration.
            || root.contains(['*', '?', '['])
            || root.contains("//")
            || root.split('/').any(|part| matches!(part, "." | ".."))
        {
            return Err(GuardError::InvalidInput);
        }
        Ok(Self {
            dataset,
            catalog,
            root: root.into(),
            scientific: true,
        })
    }
    /// Explicit native-only foundation fixture. This cannot inspect or activate
    /// a scientific installation, and is not ordinary publication readiness.
    pub fn native_only_local(
        dataset: DatasetId,
        catalog: OwnerResourceId,
        root: &str,
    ) -> Result<Self, GuardError> {
        let mut value = Self::local(dataset, catalog, root)?;
        value.scientific = false;
        Ok(value)
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Resolution {
    NotImported,
    /// Whether this call changed an enabled current owner to disabled. False
    /// also includes an already disabled current owner, not only historical ones.
    Sealed {
        disabled_current: bool,
    },
}

/// One newly committed credential lifetime. Observation/recovery never constructs
/// this value. It is move-only and has no serialization or plaintext getter.
/// The consuming callback must transfer it only to its one privately supervised
/// native child; this value alone does not prove process launch or termination.
pub struct FreshWriter {
    receipt: ArchiveAttemptReceipt,
    identity: CatalogIdentity,
    password: Zeroizing<String>,
}
impl fmt::Debug for FreshWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("FreshWriter([redacted])")
    }
}
impl FreshWriter {
    pub fn request(&self) -> &ArchiveAttemptRequest {
        self.receipt.request()
    }
    pub fn identity(&self) -> &CatalogIdentity {
        &self.identity
    }
    /// Consume the local handoff once. The callback owns any downstream copies
    /// and must retain their grant/lifecycle; there is no process-wide memory
    /// erasure promise. Never put these values in argv, environment or logs.
    pub fn with_login<T>(self, handoff: impl FnOnce(&str, &str) -> T) -> T {
        handoff(&writer_login(&self.receipt), &self.password)
    }
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

/// Explicit fresh installation after the isolated native initializer is joined.
/// The bootstrap login must have created this pinned, empty scientific catalog without
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
    install_inner(client, identity, bootstrap_login, None, commit_started).await
}

async fn install_inner(
    client: &mut Client,
    identity: &CatalogIdentity,
    bootstrap_login: &str,
    roles: Option<InstallationRoles<'_>>,
    commit_started: &Cell<bool>,
) -> Result<bool, GuardError> {
    let bootstrap = identifier(bootstrap_login)?;
    let tx = begin(client, true).await?;
    let owner = if let Some(roles) = roles {
        installation::validate(&tx, roles).await?
    } else {
        "CURRENT_USER".to_owned()
    };
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
        lock_inspection(&tx).await?;
        inspection::installed(&tx, identity).await?;
        if let Some(roles) = roles {
            installation::expected_owner(&tx, roles).await?;
            inspection::revoked(&tx, roles.bootstrap_login).await?;
        }
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
    if identity.scientific {
        scientific::fresh(&tx, identity).await?;
    } else {
        inspection::fresh(&tx, identity).await?;
    }
    inspection::bootstrap(&tx, bootstrap_login).await?;
    privileges::fresh(&tx).await?;
    tx.batch_execute(SQL).await.map_err(backend)?;
    if identity.scientific {
        tx.batch_execute(scientific::SQL).await.map_err(backend)?;
    }
    let mut edits = format!(
        "REVOKE ALL ON SCHEMA public FROM {bootstrap}; REVOKE CREATE ON SCHEMA public FROM PUBLIC; REVOKE ALL ON ALL TABLES IN SCHEMA public FROM PUBLIC;"
    );
    for table in tables {
        edits.push_str(&format!("ALTER TABLE public.{table} OWNER TO {owner}; REVOKE ALL ON public.{table} FROM {bootstrap}; CREATE TRIGGER bigbrotr_writer_guard BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON public.{table} FOR EACH STATEMENT EXECUTE FUNCTION bigbrotr_archive.require_writer();"));
    }
    tx.batch_execute(&edits).await.map_err(backend)?;
    if let Some(roles) = roles {
        installation::transfer_guard(&tx, roles).await?;
        installation::expected_owner(&tx, roles).await?;
    }
    inspection::revoked(&tx, bootstrap_login).await?;
    // Foreign-key checks use the table owner's rights, even for a superuser
    // initializer. Refuse missing manager privileges before inserting bindings.
    privileges::installed(&tx).await?;
    let layout = inspection::layout(&tx).await?;
    tx.execute("INSERT INTO bigbrotr_archive.installation VALUES(true,$1::bytea,$2::bytea,$3::text,1,$4::bytea,$5::bytea)", &[&&identity.dataset.as_bytes()[..],&&identity.catalog.as_bytes()[..],&identity.root,&&checksum()[..],&&layout[..]]).await.map_err(backend)?;
    if identity.scientific {
        scientific::bind(&tx).await?;
    }
    inspection::installed(&tx, identity).await?;
    commit_started.set(true);
    tx.commit().await.map_err(|_| GuardError::CommitUnknown)?;
    Ok(true)
}

async fn lock_inspection(tx: &Transaction<'_>) -> Result<(), GuardError> {
    // Native DML already obtains a table lock before its trigger locks this row.
    // Do not request SHARE table locks after this: that would reverse lock order.
    // Shape is not trusted yet. Transfer no owner values and at most two fixed
    // scalars; query_one still refuses a malformed non-singleton owner table.
    tx.query_one(
        "SELECT 1 FROM bigbrotr_archive.owner WHERE singleton LIMIT 2 FOR UPDATE",
        &[],
    )
    .await
    .map_err(backend)?;
    Ok(())
}

/// Verification without durable mutation, including actual logical/native shape.
/// The owner lock briefly excludes native commits for a coherent inspection. This
/// is not a receipt lookup and grants no native write authority.
pub async fn inspect(client: &mut Client, identity: &CatalogIdentity) -> Result<(), GuardError> {
    let tx = begin(client, false).await?;
    lock_inspection(&tx).await?;
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
    activate_inner(client, identity, receipt, None, commit_started).await
}

/// Atomically create a new restricted login and activate its retained intent.
/// Plaintext remains local; CREATE ROLE receives a precomputed SCRAM verifier.
/// Existing roles/attempts are refused, never adopted, reset or re-enabled.
/// Unknown commit/cancellation returns no credential. Resolve under a fresh
/// connection; an already committed lifetime requires a new retained intent.
pub async fn provision_and_activate(
    client: &mut Client,
    identity: &CatalogIdentity,
    receipt: &ArchiveAttemptReceipt,
    commit_started: &Cell<bool>,
) -> Result<FreshWriter, GuardError> {
    let material = credentials::Material::generate()?;
    match activate_inner(client, identity, receipt, Some(&material), commit_started).await? {
        Activation::Activated => Ok(FreshWriter {
            receipt: receipt.clone(),
            identity: identity.clone(),
            password: material.password,
        }),
        Activation::Observed { .. } => Err(GuardError::Conflict),
    }
}

async fn matches_attempt(
    tx: &Transaction<'_>,
    request: &ArchiveAttemptRequest,
    login: &str,
) -> Result<Option<bool>, GuardError> {
    let ordinal = request.ordinal().to_string();
    let topology = request.topology_generation().get().to_string();
    let row=tx.query_opt("SELECT attempt_id=$2::bytea AND coordinator_incarnation=$3::bytea AND worker_incarnation=$4::bytea AND topology_generation=$5::text::numeric AND batch_digest=$6::bytea AND login_name=$7::name AND login_oid=(SELECT oid FROM pg_roles WHERE rolname=$7::name) FROM bigbrotr_archive.attempts WHERE ordinal=$1::text::numeric", &[&ordinal,&&request.attempt_id()[..],&&request.coordinator().as_bytes()[..],&&request.worker().as_bytes()[..],&topology,&&request.batch_digest().as_bytes()[..],&login]).await.map_err(backend)?;
    row.map(|row| Ok(row.try_get::<_, Option<bool>>(0).map_err(backend)? == Some(true)))
        .transpose()
}

async fn activate_inner(
    client: &mut Client,
    identity: &CatalogIdentity,
    receipt: &ArchiveAttemptReceipt,
    material: Option<&credentials::Material>,
    commit_started: &Cell<bool>,
) -> Result<Activation, GuardError> {
    let request = receipt.request();
    if request.dataset() != identity.dataset || request.catalog() != identity.catalog {
        return Err(GuardError::WrongCatalog);
    }
    let login = writer_login(receipt);
    let quoted = identifier(&login)?;
    let tx = begin(client, false).await?;
    lock_inspection(&tx).await?;
    inspection::installed(&tx, identity).await?;
    let state = tx
        .query_one(
            "SELECT ordinal::text,enabled FROM bigbrotr_archive.owner WHERE singleton",
            &[],
        )
        .await
        .map_err(backend)?;
    let current = state
        .try_get::<_, &str>(0)
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .ok_or(GuardError::IncompatibleLayout)?;
    let enabled: bool = state.try_get(1).map_err(backend)?;
    let ordinal = request.ordinal().to_string();
    let topology = request.topology_generation().get().to_string();
    if let Some(matches) = matches_attempt(&tx, request, &login).await? {
        if !matches || material.is_some() {
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
    if let Some(material) = material {
        let exists: bool = tx
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1::name)",
                &[&login],
            )
            .await
            .map_err(backend)?
            .try_get(0)
            .map_err(backend)?;
        if exists {
            return Err(GuardError::Conflict);
        }
        // PG16 otherwise permits creator-selected inherited/SET memberships.
        // Only its implicit non-inheriting ADMIN link to the manager is allowed.
        tx.batch_execute("SET LOCAL createrole_self_grant=''")
            .await
            .map_err(backend)?;
        tx.batch_execute(&format!("CREATE ROLE {quoted} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS CONNECTION LIMIT 8 PASSWORD '{}'", material.verifier.as_str())).await.map_err(backend)?;
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

/// Seal only the exact imported role lifetime, and disable it if still current.
/// The owner lock serializes this with native commits. NOLOGIN/password removal
/// alone is not backend exclusion, worker death, receipt resolution or cleanup.
pub async fn seal(
    client: &mut Client,
    identity: &CatalogIdentity,
    receipt: &ArchiveAttemptReceipt,
    commit_started: &Cell<bool>,
) -> Result<bool, GuardError> {
    match resolve_inner(client, identity, receipt, false, commit_started).await? {
        Resolution::Sealed { disabled_current } => Ok(disabled_current),
        Resolution::NotImported => Err(GuardError::Conflict),
    }
}

async fn resolve_inner(
    client: &mut Client,
    identity: &CatalogIdentity,
    receipt: &ArchiveAttemptReceipt,
    allow_missing: bool,
    commit_started: &Cell<bool>,
) -> Result<Resolution, GuardError> {
    let request = receipt.request();
    if request.dataset() != identity.dataset || request.catalog() != identity.catalog {
        return Err(GuardError::WrongCatalog);
    }
    let login = writer_login(receipt);
    let tx = begin(client, false).await?;
    lock_inspection(&tx).await?;
    inspection::installed(&tx, identity).await?;
    match matches_attempt(&tx, request, &login).await? {
        Some(true) => {}
        Some(false) => return Err(GuardError::Conflict),
        None if allow_missing => {
            // An ordinal miss alone cannot distinguish absence from a conflicting
            // imported lifetime or an orphan credential. Each identity predicate
            // uses an existing unique key; no retained payload crosses the wire.
            inspection::require(
                &tx,
                "SELECT NOT EXISTS(SELECT 1 FROM bigbrotr_archive.attempts WHERE attempt_id=$1::bytea OR worker_incarnation=$2::bytea OR login_name=$3::name) AND NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$3::name)",
                &[&&request.attempt_id()[..], &&request.worker().as_bytes()[..], &login],
                GuardError::Conflict,
            ).await?;
            // The actual provisioning driver was excluded before this owner
            // lock. Already submitted mutations therefore finish before our
            // inspection; a delayed owner query cannot enqueue later mutations
            // through a destroyed sequential driver. No tombstone or new grant.
            tx.rollback().await.map_err(backend)?;
            return Ok(Resolution::NotImported);
        }
        None => return Err(GuardError::Conflict),
    }
    inspection::writer(&tx, &login).await?;
    tx.batch_execute(&format!(
        "ALTER ROLE {} NOLOGIN PASSWORD NULL",
        identifier(&login)?
    ))
    .await
    .map_err(backend)?;
    let disabled = tx.execute("UPDATE bigbrotr_archive.owner SET enabled=false WHERE singleton AND ordinal=$1::text::numeric AND enabled", &[&request.ordinal().to_string()]).await.map_err(backend)?;
    commit_started.set(true);
    tx.commit().await.map_err(|_| GuardError::CommitUnknown)?;
    Ok(Resolution::Sealed {
        disabled_current: disabled == 1,
    })
}

/// Resolve one retained attempt only after matching local custody is recovered.
/// The dedicated manager driver must also retain that custody through transport
/// destruction. This is not receipt absence, file protection or process reap.
pub async fn resolve_after_local_custody(
    client: &mut Client,
    identity: &CatalogIdentity,
    receipt: &ArchiveAttemptReceipt,
    custody: &crate::storage::archive::LocalAttempt,
    commit_started: &Cell<bool>,
) -> Result<Resolution, GuardError> {
    if !custody.recovered_for(receipt.request(), identity) {
        return Err(GuardError::InvalidInput);
    }
    resolve_inner(client, identity, receipt, true, commit_started).await
}
