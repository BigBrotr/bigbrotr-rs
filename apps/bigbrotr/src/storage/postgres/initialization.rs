//! Explicit empty-dataset acceptance. Receipts are data, never runtime authority.
use super::{migration_runner, migrations};
use crate::runtime::policy_document::PolicyDocumentVersion;
use bigbrotr_model::{DatasetId, PrincipalId};
use sha2::{Digest, Sha256};
use std::{cell::Cell, fmt};
use tokio_postgres::{Client, IsolationLevel, Row};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InitializeError {
    InvalidInput,
    Backend,
    IncompatibleEngine,
    IncompatibleSchema,
    MigrationPrivileges,
    WrongDeployment,
    ExistingState,
    EntropyUnavailable,
    CommitUnknown,
}
impl fmt::Display for InitializeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CommitUnknown => f.write_str("Control initialization COMMIT outcome unknown; retry the same dataset and instance under initialization serialization"),
            _ => write!(f, "Control initialization refused: {self:?}"),
        }
    }
}
impl std::error::Error for InitializeError {}
fn backend(_: tokio_postgres::Error) -> InitializeError {
    InitializeError::Backend
}

/// Transport from the closed configuration validator, not an authorization
/// capability. The caller validates all normalized settings against its roots.
/// No arbitrary HTTP request may call this deployment-only operation.
pub struct InitialConfiguration<'a> {
    pub dataset: DatasetId,
    pub instance: DatasetId,
    pub authority_mode: &'a str,
    pub document: &'a str,
    pub document_digest: [u8; 32],
    pub source_digest: [u8; 32],
}
pub struct InitializationReceipt {
    pub created: bool,
    pub actor: PrincipalId,
    pub audit_id: [u8; 16],
    pub document_digest: [u8; 32],
    pub source_digest: [u8; 32],
    pub policy_version: PolicyDocumentVersion,
}

/// The finite CLI owns the protected application connection and a reservation
/// through driver termination. `commit_started` survives timeout/cancellation;
/// only a new transaction holding the same lock can resolve an unknown COMMIT.
/// The validator checks the complete current normalized document and canonical
/// digest against the actual roots, for both new input and the retained receipt.
pub async fn initialize(
    client: &mut Client,
    input: InitialConfiguration<'_>,
    mut validate: impl FnMut(&str, [u8; 32]) -> Result<(), InitializeError>,
    commit_started: &Cell<bool>,
) -> Result<InitializationReceipt, InitializeError> {
    let policy_version = PolicyDocumentVersion::from_document(input.document)
        .map_err(|_| InitializeError::InvalidInput)?;
    if !matches!(input.authority_mode, "managed" | "declarative")
        || input.document.is_empty()
        || input.document.len() > 10 * 1024 * 1024
        || <[u8; 32]>::from(
            Sha256::new()
                .chain_update(policy_version.digest_domain())
                .chain_update(input.document.as_bytes())
                .finalize(),
        ) != input.document_digest
    {
        return Err(InitializeError::InvalidInput);
    }
    validate(input.document, input.document_digest).map_err(|_| InitializeError::InvalidInput)?;
    let tx = application_transaction(client, 801).await?;
    tx.query_one(
        "SELECT pg_catalog.pg_advisory_xact_lock($1, $2)",
        &[&0x4242_7472_i32, &0x494e_4954_i32],
    )
    .await
    .map_err(backend)?;
    // Read Committed starts this snapshot AFTER the preceding initializer ends.
    // Read the original receipt, never reinterpret current config as a new seed.
    const ORIGINAL_RECEIPT: &str = "
        SELECT substring(d.dataset_id FROM 1 FOR 17),
               substring(d.instance_id FROM 1 FOR 17), d.config_revision IS NOT NULL,
               substring(c.actor_id FROM 1 FOR 17), substring(a.audit_id FROM 1 FOR 17),
               substring(c.document_digest FROM 1 FOR 33),
               substring(c.accepted_source_digest FROM 1 FOR 33),
               p.kind = 'installation' AND a.actor_id = c.actor_id
               AND a.created_at_us = c.accepted_at_us AND p.created_at_us = d.created_at_us
               AND c.accepted_at_us = d.created_at_us,
               left(c.authority_mode,12), c.schema_version, left(c.document->>'format',64),
               CASE WHEN octet_length(c.document::text)<=10485760 THEN c.document::text END
          FROM control.datasets d
          LEFT JOIN control.config_revisions c ON c.dataset_id = d.dataset_id AND c.revision = 1
          LEFT JOIN control.principals p ON p.dataset_id = d.dataset_id AND p.principal_id = c.actor_id
          LEFT JOIN control.management_audit a ON a.dataset_id = d.dataset_id
               AND a.action = 'dataset.initialize' AND a.outcome = 'accepted'
               AND a.config_revision = c.revision
         LIMIT 2";
    let rows = tx.query(ORIGINAL_RECEIPT, &[]).await.map_err(backend)?;
    if let [row] = rows.as_slice() {
        if fixed::<16>(row, 0)? != *input.dataset.as_bytes()
            || fixed::<16>(row, 1)? != *input.instance.as_bytes()
        {
            return Err(InitializeError::WrongDeployment);
        }
        if !row
            .try_get::<_, bool>(2)
            .map_err(|_| InitializeError::ExistingState)?
            || !row
                .try_get::<_, bool>(7)
                .map_err(|_| InitializeError::ExistingState)?
        {
            return Err(InitializeError::ExistingState);
        }
        // A mode mismatch requires its explicit reconciliation, not bootstrap.
        if row
            .try_get::<_, &str>(8)
            .map_err(|_| InitializeError::ExistingState)?
            != input.authority_mode
        {
            return Err(InitializeError::ExistingState);
        }
        let receipt = InitializationReceipt {
            created: false,
            actor: PrincipalId::try_from(fixed::<16>(row, 3)?)
                .map_err(|_| InitializeError::ExistingState)?,
            audit_id: fixed(row, 4)?,
            document_digest: fixed(row, 5)?,
            source_digest: fixed(row, 6)?,
            policy_version: PolicyDocumentVersion::from_metadata(
                row.try_get(9).map_err(|_| InitializeError::ExistingState)?,
                row.try_get(10)
                    .map_err(|_| InitializeError::ExistingState)?,
            )
            .map_err(|_| InitializeError::ExistingState)?,
        };
        // Header/digest metadata alone does not establish the original complete
        // current policy. In particular, retry must not certify a missing field
        // or changed body merely because its old digest is still retained.
        validate(
            row.try_get::<_, &str>(11)
                .map_err(|_| InitializeError::ExistingState)?,
            receipt.document_digest,
        )
        .map_err(|_| InitializeError::ExistingState)?;
        tx.rollback().await.map_err(backend)?;
        return Ok(receipt);
    }
    if !rows.is_empty() {
        return Err(InitializeError::ExistingState);
    }
    let actor_bytes = fresh_id()?;
    let actor =
        PrincipalId::try_from(actor_bytes).map_err(|_| InitializeError::EntropyUnavailable)?;
    let audit_id = fresh_id()?;
    let now: i64 = tx
        .query_one(
            "SELECT (EXTRACT(EPOCH FROM pg_catalog.clock_timestamp()) * 1000000)::bigint",
            &[],
        )
        .await
        .map_err(backend)?
        .try_get(0)
        .map_err(backend)?;
    let dataset = &input.dataset.as_bytes()[..];
    tx.execute(
        "INSERT INTO control.datasets(dataset_id, instance_id, created_at_us) VALUES ($1::bytea, $2::bytea, $3::bigint)",
        &[&dataset, &&input.instance.as_bytes()[..], &now],
    )
    .await
    .map_err(backend)?;
    tx.execute("INSERT INTO control.principals(principal_id, dataset_id, kind, created_at_us, database_role, local_os_uid) VALUES ($1::bytea, $2::bytea, 'installation', $3::bigint, SESSION_USER, $4::bigint)",
        &[&&actor_bytes[..], &dataset, &now, &i64::from(rustix::process::geteuid().as_raw())]).await.map_err(backend)?;
    const INSERT_CONFIGURATION: &str = "
        INSERT INTO control.config_revisions
            (revision, dataset_id, schema_version, authority_mode, document,
             document_digest, accepted_source_digest, accepted_at_us, actor_id)
        VALUES (1, $1::bytea, $2::integer, $3::text, $4::text::jsonb,
                $5::bytea, $6::bytea, $7::bigint, $8::bytea)";
    tx.execute(
        INSERT_CONFIGURATION,
        &[
            &dataset,
            &policy_version.schema_version(),
            &input.authority_mode,
            &input.document,
            &&input.document_digest[..],
            &&input.source_digest[..],
            &now,
            &&actor_bytes[..],
        ],
    )
    .await
    .map_err(backend)?;
    tx.execute("INSERT INTO control.management_audit(audit_id, dataset_id, actor_id, action, outcome, created_at_us, config_revision) VALUES ($1::bytea, $2::bytea, $3::bytea, 'dataset.initialize', 'accepted', $4::bigint, 1)",
        &[&&audit_id[..], &dataset, &&actor_bytes[..], &now]).await.map_err(backend)?;
    let changed = tx.execute("UPDATE control.datasets SET config_revision = 1 WHERE dataset_id = $1::bytea AND config_revision IS NULL AND policy_revision IS NULL AND topology_generation IS NULL", &[&dataset])
        .await.map_err(backend)?;
    if changed != 1 {
        return Err(InitializeError::ExistingState);
    }
    commit_started.set(true);
    tx.commit()
        .await
        .map_err(|_| InitializeError::CommitUnknown)?;
    Ok(InitializationReceipt {
        created: true,
        actor,
        audit_id,
        document_digest: input.document_digest,
        source_digest: input.source_digest,
        policy_version,
    })
}
fn fixed<const N: usize>(row: &Row, index: usize) -> Result<[u8; N], InitializeError> {
    row.try_get::<_, &[u8]>(index)
        .map_err(|_| InitializeError::ExistingState)?
        .try_into()
        .map_err(|_| InitializeError::ExistingState)
}
fn fresh_id() -> Result<[u8; 16], InitializeError> {
    let mut bytes = [0; 16];
    getrandom::fill(&mut bytes).map_err(|_| InitializeError::EntropyUnavailable)?;
    if bytes == [0; 16] {
        return Err(InitializeError::EntropyUnavailable);
    }
    Ok(bytes)
}

/// Engine/schema/least-privilege guard, not principal authentication or a grant.
pub(super) async fn application_transaction(
    client: &mut Client,
    required: u32,
) -> Result<tokio_postgres::Transaction<'_>, InitializeError> {
    let tx = client
        .build_transaction()
        .isolation_level(IsolationLevel::ReadCommitted)
        .start()
        .await
        .map_err(backend)?;
    tx.batch_execute("SET LOCAL search_path = pg_catalog; SET LOCAL statement_timeout = '20s'; SET LOCAL lock_timeout = '5s'; SET LOCAL idle_in_transaction_session_timeout = '10s'; SET LOCAL synchronous_commit = on")
        .await.map_err(backend)?;
    tx.query_one(
        "SELECT pg_catalog.pg_advisory_xact_lock_shared($1, $2)",
        &[&0x4242_7472_i32, &0x4d49_4752_i32],
    )
    .await
    .map_err(backend)?;
    let row = tx.query_one("SELECT pg_catalog.current_setting('server_version_num')::integer, pg_catalog.current_setting('fsync') = 'on', pg_catalog.current_setting('full_page_writes') = 'on'", &[])
        .await.map_err(backend)?;
    if row.try_get::<_, i32>(0).map_err(backend)? != 160015
        || !row.try_get::<_, bool>(1).map_err(backend)?
        || !row.try_get::<_, bool>(2).map_err(backend)?
    {
        return Err(InitializeError::IncompatibleEngine);
    }
    let applied = migration_runner::read_ledger(&tx)
        .await
        .map_err(|_| InitializeError::IncompatibleSchema)?;
    migrations::require_applied(&applied, required)
        .map_err(|_| InitializeError::IncompatibleSchema)?;
    // Provision a narrow application role. Schema ownership or inherited owner
    // privileges would let this command bypass the invariants it relies upon.
    let row = tx.query_one("SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = CURRENT_USER AND rolsuper) OR pg_catalog.has_schema_privilege(CURRENT_USER, 'control', 'CREATE') OR EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = 'control' AND pg_catalog.pg_has_role(CURRENT_USER, nspowner, 'USAGE')) OR EXISTS (SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'control' AND c.relkind IN ('r', 'p') AND pg_catalog.pg_has_role(CURRENT_USER, c.relowner, 'USAGE'))", &[])
        .await.map_err(backend)?;
    if row.try_get::<_, bool>(0).map_err(backend)? {
        return Err(InitializeError::MigrationPrivileges);
    }
    Ok(tx)
}
