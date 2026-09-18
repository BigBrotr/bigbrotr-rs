//! Bounded observation of accepted configuration. Returned data grants no authority.
use super::{migration_runner, migrations};
use bigbrotr_model::DatasetId;
use std::fmt;
use tokio_postgres::{Client, IsolationLevel, Row};

const DOCUMENT_LIMIT: i32 = 10 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InspectError {
    Backend,
    IncompatibleEngine,
    IncompatibleSchema,
    WrongDeployment,
    InvalidMetadata,
}
impl fmt::Display for InspectError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Control configuration inspection failed: {self:?}")
    }
}
impl std::error::Error for InspectError {}
fn backend(_: tokio_postgres::Error) -> InspectError {
    InspectError::Backend
}

/// Transport data only. The closed configuration schema must still validate the
/// document and digest against deployment roots before any effective use.
pub struct AcceptedConfiguration {
    pub revision: u64,
    pub security_revision: Option<u64>,
    pub authority_mode: String,
    pub document: String,
    pub document_digest: [u8; 32],
    pub source_digest: [u8; 32],
}
pub enum ConfigurationObservation {
    NoDataset,
    NoConfiguration,
    Accepted(AcceptedConfiguration),
}

/// Caller owns one connection/driver, a finite deadline and a document/parse
/// reservation. This read never creates a dataset or infers absence from errors.
pub async fn inspect(
    client: &mut Client,
    dataset: DatasetId,
    instance: [u8; 16],
) -> Result<ConfigurationObservation, InspectError> {
    let tx = client
        .build_transaction()
        .isolation_level(IsolationLevel::ReadCommitted)
        .read_only(true)
        .start()
        .await
        .map_err(backend)?;
    tx.batch_execute("SET LOCAL search_path = pg_catalog; SET LOCAL statement_timeout = '20s'; SET LOCAL lock_timeout = '5s'; SET LOCAL idle_in_transaction_session_timeout = '10s'").await.map_err(backend)?;
    // Read Committed acquires the subsequent ledger snapshot after the migrator
    // releases its transaction lock. The accepted document/heads use ONE SELECT.
    tx.query_one(
        "SELECT pg_catalog.pg_advisory_xact_lock_shared($1, $2)",
        &[&0x4242_7472_i32, &0x4d49_4752_i32],
    )
    .await
    .map_err(backend)?;
    let engine = tx
        .query_one(
            "SELECT pg_catalog.current_setting('server_version_num')::integer",
            &[],
        )
        .await
        .map_err(backend)?;
    if engine.try_get::<_, i32>(0).map_err(backend)? != 160015 {
        return Err(InspectError::IncompatibleEngine);
    }
    let ledger = migration_runner::read_ledger(&tx)
        .await
        .map_err(|_| InspectError::IncompatibleSchema)?;
    let plan = migrations::plan(&ledger, 2).map_err(|_| InspectError::IncompatibleSchema)?;
    if !plan.pending.is_empty() {
        return Err(InspectError::IncompatibleSchema);
    }
    let rows = tx.query("SELECT substring(d.dataset_id FROM 1 FOR 17), substring(d.instance_id FROM 1 FOR 17), left(d.config_revision::text,21), left(d.policy_revision::text,21), c.schema_version, left(c.authority_mode,12), CASE WHEN octet_length(c.document::text) <= $1 THEN c.document::text ELSE NULL END, substring(c.document_digest FROM 1 FOR 33), substring(c.accepted_source_digest FROM 1 FOR 33) FROM control.datasets d LEFT JOIN control.config_revisions c ON c.dataset_id = d.dataset_id AND c.revision = d.config_revision LIMIT 2", &[&DOCUMENT_LIMIT]).await.map_err(backend)?;
    let result = match rows.as_slice() {
        [] => ConfigurationObservation::NoDataset,
        [row] => {
            if fixed(row, 0)? != *dataset.as_bytes() || fixed::<16>(row, 1)? != instance {
                return Err(InspectError::WrongDeployment);
            }
            let security_revision = revision(row, 3)?;
            match revision(row, 2)? {
                None if security_revision.is_none() => ConfigurationObservation::NoConfiguration,
                None => return Err(InspectError::InvalidMetadata),
                Some(revision) => {
                    if row
                        .try_get::<_, i32>(4)
                        .map_err(|_| InspectError::InvalidMetadata)?
                        != 1
                    {
                        return Err(InspectError::IncompatibleSchema);
                    }
                    let mode: String = row.try_get(5).map_err(|_| InspectError::InvalidMetadata)?;
                    if !matches!(mode.as_str(), "managed" | "declarative") {
                        return Err(InspectError::InvalidMetadata);
                    }
                    ConfigurationObservation::Accepted(AcceptedConfiguration {
                        revision,
                        security_revision,
                        authority_mode: mode,
                        document: row.try_get(6).map_err(|_| InspectError::InvalidMetadata)?,
                        document_digest: fixed(row, 7)?,
                        source_digest: fixed(row, 8)?,
                    })
                }
            }
        }
        _ => return Err(InspectError::InvalidMetadata),
    };
    tx.rollback().await.map_err(backend)?;
    Ok(result)
}
fn fixed<const N: usize>(row: &Row, column: usize) -> Result<[u8; N], InspectError> {
    row.try_get::<_, &[u8]>(column)
        .map_err(|_| InspectError::InvalidMetadata)?
        .try_into()
        .map_err(|_| InspectError::InvalidMetadata)
}
fn revision(row: &Row, column: usize) -> Result<Option<u64>, InspectError> {
    let value: Option<&str> = row
        .try_get(column)
        .map_err(|_| InspectError::InvalidMetadata)?;
    value
        .map(|text| {
            text.parse::<u64>()
                .ok()
                .filter(|n| *n > 0 && n.to_string() == text)
                .ok_or(InspectError::InvalidMetadata)
        })
        .transpose()
}
