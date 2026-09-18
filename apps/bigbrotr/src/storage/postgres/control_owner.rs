//! Dedicated PostgreSQL Control owner. This fences PG commits, not native writers.
//!
//! The transport owner retains its driver and resource reservation until joined.
//! Acquisition consumes the connection even on failure/cancellation; reconnecting
//! always requires a fresh claim. No method exposes or lends the raw client.
use super::{
    ingest_registry,
    initialization::{InitializeError, application_transaction},
};
use bigbrotr_model::{DatasetId, OwnerEpoch, OwnerResourceId, ProcessIncarnation};
use sha2::{Digest, Sha256};
use std::{cell::Cell, fmt, time::Duration};
use tokio_postgres::{Client, Row, Transaction};

#[path = "initial_batch.rs"]
mod initial_batch;
pub use initial_batch::{
    InitialBatchError, InitialBatchRequest, InitialEventError, InitialEventRequest,
    InitialOperationError,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnerError {
    InvalidInput,
    Backend,
    IncompatibleDeployment,
    Busy,
    InvalidPolicy,
    InvalidMetadata,
    Exhausted,
    EntropyUnavailable,
    Unavailable,
    CommitUnknown,
}
impl fmt::Display for OwnerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Control backend operation failed: {self:?}")
    }
}
impl std::error::Error for OwnerError {}
fn backend(_: tokio_postgres::Error) -> OwnerError {
    OwnerError::Backend
}
fn transaction_error(error: InitializeError) -> OwnerError {
    match error {
        InitializeError::Backend => OwnerError::Backend,
        _ => OwnerError::IncompatibleDeployment,
    }
}

pub struct ClaimRequest {
    pub dataset: DatasetId,
    pub instance: DatasetId,
    pub authority_mode: String,
    pub role_instance_id: [u8; 16],
    pub incarnation: ProcessIncarnation,
    pub lease_ttl: Duration,
}
impl ClaimRequest {
    fn lease_us(&self) -> Result<i64, OwnerError> {
        if self.role_instance_id == [0; 16]
            || !matches!(self.authority_mode.as_str(), "managed" | "declarative")
        {
            return Err(OwnerError::InvalidInput);
        }
        i64::try_from(self.lease_ttl.as_micros())
            .ok()
            .filter(|n| *n > 0)
            .ok_or(OwnerError::InvalidInput)
    }
}
/// Observed revisions only; the runtime starts and bounds its own refresh lease.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PolicySnapshot {
    pub config_revision: u64,
    pub policy_revision: u64,
    pub config_digest: [u8; 32],
    pub policy_digest: [u8; 32],
}
pub struct ControlSession {
    client: Client,
    request: ClaimRequest,
    identity: ingest_registry::ControlOwner,
    available: bool,
}
impl ControlSession {
    /// The validator checks BOTH complete stored documents against actual roots.
    /// Deadline/driver ownership stays with the transport supervisor. A cancelled
    /// acquisition can have committed; close/join before a fresh incarnation.
    pub async fn acquire(
        mut client: Client,
        request: ClaimRequest,
        mut validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
        commit_started: &Cell<bool>,
    ) -> Result<(Self, PolicySnapshot), OwnerError> {
        let lease_us = request.lease_us()?;
        let tx = application_transaction(&mut client, 7)
            .await
            .map_err(transaction_error)?;
        let locked: bool = tx
            .query_one(
                "SELECT pg_catalog.pg_try_advisory_lock($1, $2)",
                &[&0x4242_7472_i32, &0x4354_524c_i32],
            )
            .await
            .map_err(backend)?
            .try_get(0)
            .map_err(backend)?;
        if !locked {
            return Err(OwnerError::Busy);
        }
        tx.query_one("SELECT control.retain_control_lock()", &[])
            .await
            .map_err(backend)?;
        let policy = snapshot(&tx, &request, &mut validate).await?;
        let previous = tx.query_opt("SELECT substring(resource_id FROM 1 FOR 17), epoch::text, substring(process_incarnation FROM 1 FOR 17), substring(role_instance_id FROM 1 FOR 17), state = 'released' FROM control.owner_epochs WHERE dataset_id = $1::bytea AND role = 'control' ORDER BY control.owner_epochs.epoch DESC LIMIT 1 FOR UPDATE", &[&&request.dataset.as_bytes()[..]]).await.map_err(backend)?;
        let (resource, epoch) = if let Some(row) = previous {
            let resource = fixed::<16>(&row, 0)?;
            let previous_epoch = number(&row, 1)?;
            if !row.try_get::<_, bool>(4).map_err(backend)? {
                release_epoch(
                    &tx,
                    request.dataset,
                    resource,
                    previous_epoch,
                    fixed(&row, 2)?,
                    request.incarnation,
                )
                .await?;
                tx.execute("UPDATE control.role_instances SET retired_at_us = (EXTRACT(EPOCH FROM pg_catalog.clock_timestamp()) * 1000000)::bigint WHERE role_instance_id = $1::bytea AND retired_at_us IS NULL", &[&&fixed::<16>(&row, 3)?[..]]).await.map_err(backend)?;
            }
            (
                resource,
                previous_epoch.checked_add(1).ok_or(OwnerError::Exhausted)?,
            )
        } else {
            (fresh_id()?, 1)
        };
        tx.execute("INSERT INTO control.role_instances(role_instance_id, dataset_id, role, created_at_us) VALUES ($1::bytea, $2::bytea, 'control', (EXTRACT(EPOCH FROM pg_catalog.clock_timestamp()) * 1000000)::bigint)", &[&&request.role_instance_id[..], &&request.dataset.as_bytes()[..]]).await.map_err(backend)?;
        tx.execute("INSERT INTO control.owner_epochs(resource_id, epoch, dataset_id, role, role_instance_id, process_incarnation, acquired_at_us, lease_expires_at_us, state, control_fence_version, control_backend_pid, control_backend_start) SELECT $1::bytea, $2::text::numeric, $3::bytea, 'control', $4::bytea, $5::bytea, now_us, now_us + $6::bigint, 'active', 1, pg_catalog.pg_backend_pid(), (SELECT backend_start FROM pg_catalog.pg_stat_activity WHERE pid = pg_catalog.pg_backend_pid()) FROM (SELECT (EXTRACT(EPOCH FROM pg_catalog.clock_timestamp()) * 1000000)::bigint AS now_us) t", &[&&resource[..], &epoch.to_string(), &&request.dataset.as_bytes()[..], &&request.role_instance_id[..], &&request.incarnation.as_bytes()[..], &lease_us]).await.map_err(backend)?;
        let identity = ingest_registry::ControlOwner {
            resource_id: OwnerResourceId::try_from(resource)
                .map_err(|_| OwnerError::InvalidMetadata)?,
            epoch: OwnerEpoch::new(epoch).map_err(|_| OwnerError::InvalidMetadata)?,
            role_instance_id: request.role_instance_id,
            process_incarnation: request.incarnation,
        };
        commit_started.set(true);
        tx.commit().await.map_err(|_| OwnerError::CommitUnknown)?;
        Ok((
            Self {
                client,
                request,
                identity,
                available: true,
            },
            policy,
        ))
    }
    /// Attribution only: possession of this Copy value never grants access.
    pub const fn identity(&self) -> ingest_registry::ControlOwner {
        self.identity
    }

    pub async fn refresh(
        &mut self,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<PolicySnapshot, OwnerError> {
        self.read_policy(validate, true).await
    }

    /// Validate current policy under the owner fence without renewing the
    /// scheduling lease. Authorization polling has its own shorter cadence.
    pub async fn poll_policy(
        &mut self,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<PolicySnapshot, OwnerError> {
        self.read_policy(validate, false).await
    }

    async fn read_policy(
        &mut self,
        mut validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
        renew: bool,
    ) -> Result<PolicySnapshot, OwnerError> {
        self.begin_operation()?;
        let tx = application_transaction(&mut self.client, 7)
            .await
            .map_err(transaction_error)?;
        require(&tx, self.request.dataset, self.identity).await?;
        let policy = snapshot(&tx, &self.request, &mut validate).await?;
        if renew {
            let changed = tx.execute("UPDATE control.owner_epochs SET lease_expires_at_us = (EXTRACT(EPOCH FROM pg_catalog.clock_timestamp()) * 1000000)::bigint + $4::bigint WHERE resource_id = $1::bytea AND epoch = $2::text::numeric AND process_incarnation = $3::bytea AND state = 'active'", &[&&self.identity.resource_id.as_bytes()[..], &self.identity.epoch.get().to_string(), &&self.identity.process_incarnation.as_bytes()[..], &self.request.lease_us()?]).await.map_err(backend)?;
            if changed != 1 {
                return Err(OwnerError::Unavailable);
            }
        }
        tx.commit().await.map_err(|_| OwnerError::CommitUnknown)?;
        self.available = true;
        Ok(policy)
    }
    pub async fn register_ingest(
        &mut self,
        attempt: &mut ingest_registry::RegistrationAttempt,
    ) -> Result<ingest_registry::RegistrationObservation, ingest_registry::RegistryError> {
        if attempt.specification().dataset_id != self.request.dataset
            || attempt.specification().control != self.identity
        {
            return Err(ingest_registry::RegistryError::ControlMismatch);
        }
        self.begin_operation()
            .map_err(|_| ingest_registry::RegistryError::ControlMismatch)?;
        let result = attempt.register(&mut self.client).await;
        // Any unsuccessful/cancelled operation poisons this connection. Resolution
        // uses a fresh read connection; a successor must acquire its own claim.
        if result.is_ok() {
            self.available = true;
        }
        result
    }
    /// Issue or resolve the same immutable publication intent. The caller owns
    /// the operation deadline/driver; cancellation poisons this session. A lost
    /// COMMIT is resolved by the same request on a freshly acquired Control owner.
    pub async fn register_archive_attempt(
        &mut self,
        request: &super::archive_attempt::ArchiveAttemptRequest,
        commit_started: &Cell<bool>,
    ) -> Result<
        super::archive_attempt::ArchiveAttemptReceipt,
        super::archive_attempt::ArchiveAttemptError,
    > {
        use super::archive_attempt::{self, ArchiveAttemptError};
        if request.dataset() != self.request.dataset
            || request.catalog() == self.identity.resource_id
        {
            return Err(ArchiveAttemptError::InvalidInput);
        }
        self.begin_operation()
            .map_err(|_| ArchiveAttemptError::Unavailable)?;
        let tx = application_transaction(&mut self.client, 9)
            .await
            .map_err(|_| ArchiveAttemptError::Prerequisite)?;
        require(&tx, self.request.dataset, self.identity)
            .await
            .map_err(|_| ArchiveAttemptError::Unavailable)?;
        let receipt = archive_attempt::register(tx, request, self.identity, commit_started).await?;
        self.available = true;
        Ok(receipt)
    }

    pub async fn release(mut self) -> Result<(), OwnerError> {
        self.begin_operation()?;
        let tx = application_transaction(&mut self.client, 7)
            .await
            .map_err(transaction_error)?;
        require(&tx, self.request.dataset, self.identity).await?;
        release_epoch(
            &tx,
            self.request.dataset,
            *self.identity.resource_id.as_bytes(),
            self.identity.epoch.get(),
            *self.request.incarnation.as_bytes(),
            self.request.incarnation,
        )
        .await?;
        tx.execute("UPDATE control.role_instances SET retired_at_us = (EXTRACT(EPOCH FROM pg_catalog.clock_timestamp()) * 1000000)::bigint WHERE role_instance_id = $1::bytea AND retired_at_us IS NULL", &[&&self.request.role_instance_id[..]]).await.map_err(backend)?;
        tx.commit().await.map_err(|_| OwnerError::CommitUnknown)?;
        // Drop closes the client; session exclusion survives until driver exit.
        Ok(())
    }
    fn begin_operation(&mut self) -> Result<(), OwnerError> {
        if !self.available {
            return Err(OwnerError::Unavailable);
        }
        self.available = false;
        Ok(())
    }
}
async fn require(
    tx: &Transaction<'_>,
    dataset: DatasetId,
    owner: ingest_registry::ControlOwner,
) -> Result<(), OwnerError> {
    tx.query_one(
        "SELECT control.require_control_owner($1::bytea, $2::bytea, $3::text::numeric, $4::bytea)",
        &[
            &&dataset.as_bytes()[..],
            &&owner.resource_id.as_bytes()[..],
            &owner.epoch.get().to_string(),
            &&owner.process_incarnation.as_bytes()[..],
        ],
    )
    .await
    .map_err(backend)?;
    Ok(())
}
async fn snapshot(
    tx: &Transaction<'_>,
    request: &ClaimRequest,
    validate: &mut impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
) -> Result<PolicySnapshot, OwnerError> {
    // Lock before the subsequent snapshot; policy changes cannot race validation
    // and the owner transaction. These are bounded documents, never default-fill.
    let rows = tx.query("SELECT substring(dataset_id FROM 1 FOR 17), substring(instance_id FROM 1 FOR 17) FROM control.datasets LIMIT 2 FOR UPDATE", &[]).await.map_err(backend)?;
    let [row] = rows.as_slice() else {
        return Err(OwnerError::IncompatibleDeployment);
    };
    if fixed::<16>(row, 0)? != *request.dataset.as_bytes()
        || fixed::<16>(row, 1)? != *request.instance.as_bytes()
    {
        return Err(OwnerError::IncompatibleDeployment);
    }
    let row = tx.query_opt("SELECT d.config_revision::text, d.policy_revision::text, substring(c.document_digest FROM 1 FOR 33), substring(p.document_digest FROM 1 FOR 33), CASE WHEN octet_length(c.document::text) <= 10485760 THEN c.document::text END, CASE WHEN octet_length(p.document::text) <= 10485760 THEN p.document::text END, c.schema_version = 1 AND p.schema_version = 1 AND c.authority_mode = $2::text AND p.config_revision = c.revision FROM control.datasets d JOIN control.config_revisions c ON c.dataset_id = d.dataset_id AND c.revision = d.config_revision JOIN control.policy_revisions p ON p.dataset_id = d.dataset_id AND p.revision = d.policy_revision WHERE d.dataset_id = $1::bytea", &[&&request.dataset.as_bytes()[..], &request.authority_mode]).await.map_err(backend)?.ok_or(OwnerError::InvalidPolicy)?;
    if !row.try_get::<_, bool>(6).map_err(backend)? {
        return Err(OwnerError::InvalidPolicy);
    }
    let result = PolicySnapshot {
        config_revision: number(&row, 0)?,
        policy_revision: number(&row, 1)?,
        config_digest: fixed(&row, 2)?,
        policy_digest: fixed(&row, 3)?,
    };
    validate(
        row.try_get(4).map_err(|_| OwnerError::InvalidPolicy)?,
        result.config_digest,
    )?;
    validate(
        row.try_get(5).map_err(|_| OwnerError::InvalidPolicy)?,
        result.policy_digest,
    )?;
    Ok(result)
}
async fn release_epoch(
    tx: &Transaction<'_>,
    dataset: DatasetId,
    resource: [u8; 16],
    epoch: u64,
    previous_incarnation: [u8; 16],
    successor: ProcessIncarnation,
) -> Result<(), OwnerError> {
    let digest: [u8; 32] = Sha256::new()
        .chain_update(b"bigbrotr/control-exclusion/1\0")
        .chain_update(dataset.as_bytes())
        .chain_update(resource)
        .chain_update(epoch.to_be_bytes())
        .chain_update(previous_incarnation)
        .chain_update(successor.as_bytes())
        .finalize()
        .into();
    let changed = tx.execute("UPDATE control.owner_epochs SET state = 'released', released_at_us = (EXTRACT(EPOCH FROM pg_catalog.clock_timestamp()) * 1000000)::bigint, exclusion_method = 'storage_fenced', exclusion_digest = $4::bytea WHERE resource_id = $1::bytea AND epoch = $2::text::numeric AND process_incarnation = $3::bytea AND state <> 'released'", &[&&resource[..], &epoch.to_string(), &&previous_incarnation[..], &&digest[..]]).await.map_err(backend)?;
    if changed != 1 {
        return Err(OwnerError::Unavailable);
    }
    Ok(())
}
fn fixed<const N: usize>(row: &Row, index: usize) -> Result<[u8; N], OwnerError> {
    row.try_get::<_, &[u8]>(index)
        .map_err(|_| OwnerError::InvalidMetadata)?
        .try_into()
        .map_err(|_| OwnerError::InvalidMetadata)
}
fn number(row: &Row, index: usize) -> Result<u64, OwnerError> {
    row.try_get::<_, &str>(index)
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|n| *n > 0)
        .ok_or(OwnerError::InvalidMetadata)
}
fn fresh_id() -> Result<[u8; 16], OwnerError> {
    let mut bytes = [0; 16];
    getrandom::fill(&mut bytes).map_err(|_| OwnerError::EntropyUnavailable)?;
    if bytes == [0; 16] {
        return Err(OwnerError::EntropyUnavailable);
    }
    Ok(bytes)
}
