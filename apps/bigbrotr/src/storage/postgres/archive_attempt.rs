//! Retained publication intent issuance. Neither a grant row nor its observation
//! proves catalog exclusion, retained input validity, publication or protection.
use super::ingest_registry::ControlOwner;
use bigbrotr_model::{
    ArchiveBatch, DatasetId, Digest256, OwnerResourceId, ProcessIncarnation, TopologyGeneration,
};
use std::{cell::Cell, fmt};
use tokio_postgres::{Row, Transaction};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveAttemptError {
    InvalidInput,
    InvalidMetadata,
    Exhausted,
    Unavailable,
    Prerequisite,
    Conflict,
    Backend,
    CommitUnknown,
}
impl fmt::Display for ArchiveAttemptError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "archive attempt registration refused: {self:?}")
    }
}
impl std::error::Error for ArchiveAttemptError {}
fn backend(_: tokio_postgres::Error) -> ArchiveAttemptError {
    ArchiveAttemptError::Backend
}

/// Immutable intended input and credential/process lifetime, prepared before any
/// credential is exposed. The caller retains complete source batches separately.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveAttemptRequest {
    attempt_id: [u8; 16],
    dataset: DatasetId,
    catalog: OwnerResourceId,
    coordinator: ProcessIncarnation,
    worker: ProcessIncarnation,
    predecessor: u64,
    ordinal: u64,
    topology: TopologyGeneration,
    batch_digest: Digest256,
}
impl ArchiveAttemptRequest {
    pub fn new(
        attempt_id: [u8; 16],
        catalog: OwnerResourceId,
        coordinator: ProcessIncarnation,
        worker: ProcessIncarnation,
        expected_predecessor: u64,
        batch: &ArchiveBatch<'_>,
    ) -> Result<Self, ArchiveAttemptError> {
        if attempt_id == [0; 16] || coordinator == worker || batch.schema_version() != 1 {
            return Err(ArchiveAttemptError::InvalidInput);
        }
        let ordinal = expected_predecessor
            .checked_add(1)
            .ok_or(ArchiveAttemptError::Exhausted)?;
        Ok(Self {
            attempt_id,
            catalog,
            coordinator,
            worker,
            predecessor: expected_predecessor,
            ordinal,
            dataset: batch.dataset_id(),
            topology: batch.topology_generation(),
            batch_digest: batch.digest(),
        })
    }
    pub const fn attempt_id(&self) -> &[u8; 16] {
        &self.attempt_id
    }
    pub const fn dataset(&self) -> DatasetId {
        self.dataset
    }
    pub const fn catalog(&self) -> OwnerResourceId {
        self.catalog
    }
    pub const fn coordinator(&self) -> ProcessIncarnation {
        self.coordinator
    }
    pub const fn worker(&self) -> ProcessIncarnation {
        self.worker
    }
    pub const fn ordinal(&self) -> u64 {
        self.ordinal
    }
    pub const fn topology_generation(&self) -> TopologyGeneration {
        self.topology
    }
    pub const fn batch_digest(&self) -> Digest256 {
        self.batch_digest
    }
}

/// A historical observation, never a bearer capability. A newer issued intent
/// does not by itself prove that an earlier catalog writer has been fenced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveAttemptReceipt {
    request: ArchiveAttemptRequest,
    pub ordinal: u64,
    pub latest_issued: bool,
    pub created: bool,
}

impl ArchiveAttemptReceipt {
    /// Committed intended input, not a catalog write capability. Only the Control
    /// adapter constructs this receipt; catalog activation still has its own CAS.
    pub fn request(&self) -> &ArchiveAttemptRequest {
        &self.request
    }
}

/// One bounded immutable observation in the caller's fenced transaction. It
/// does not invoke registration or create a missing intent. Latest is observed
/// in the same statement as the row and remains a historical fact, not a grant.
pub(super) async fn read(
    tx: Transaction<'_>,
    dataset: DatasetId,
    catalog: OwnerResourceId,
    attempt_id: [u8; 16],
) -> Result<Option<ArchiveAttemptReceipt>, ArchiveAttemptError> {
    let row = tx.query_opt("SELECT substring(a.dataset_id FROM 1 FOR 17),substring(a.catalog_id FROM 1 FOR 17),CASE WHEN length(a.expected_predecessor::text)<=20 THEN a.expected_predecessor::text END,CASE WHEN length(a.ordinal::text)<=20 THEN a.ordinal::text END,a.schema_version=1,CASE WHEN length(a.topology_generation::text)<=20 THEN a.topology_generation::text END,substring(a.batch_digest FROM 1 FOR 33),substring(a.coordinator_incarnation FROM 1 FOR 17),substring(a.worker_incarnation FROM 1 FOR 17),NOT EXISTS(SELECT 1 FROM control.archive_attempts newer WHERE newer.dataset_id=a.dataset_id AND newer.ordinal>a.ordinal) FROM control.archive_attempts a WHERE a.attempt_id=$1::bytea LIMIT 2", &[&&attempt_id[..]]).await.map_err(backend)?;
    let receipt = row
        .map(|row| {
            if fixed::<16>(&row, 0)? != *dataset.as_bytes()
                || fixed::<16>(&row, 1)? != *catalog.as_bytes()
            {
                return Err(ArchiveAttemptError::Conflict);
            }
            let predecessor = number(&row, 2)?;
            let ordinal = number(&row, 3)?;
            let coordinator = ProcessIncarnation::try_from(fixed::<16>(&row, 7)?)
                .map_err(|_| ArchiveAttemptError::InvalidMetadata)?;
            let worker = ProcessIncarnation::try_from(fixed::<16>(&row, 8)?)
                .map_err(|_| ArchiveAttemptError::InvalidMetadata)?;
            if ordinal == 0
                || predecessor.checked_add(1) != Some(ordinal)
                || coordinator == worker
                || !row
                    .try_get::<_, bool>(4)
                    .map_err(|_| ArchiveAttemptError::InvalidMetadata)?
            {
                return Err(ArchiveAttemptError::InvalidMetadata);
            }
            Ok(ArchiveAttemptReceipt {
                request: ArchiveAttemptRequest {
                    attempt_id,
                    dataset,
                    catalog,
                    coordinator,
                    worker,
                    predecessor,
                    ordinal,
                    topology: TopologyGeneration::new(number(&row, 5)?),
                    batch_digest: Digest256::from_bytes(fixed(&row, 6)?),
                },
                ordinal,
                latest_issued: row
                    .try_get(9)
                    .map_err(|_| ArchiveAttemptError::InvalidMetadata)?,
                created: false,
            })
        })
        .transpose()?;
    tx.rollback().await.map_err(backend)?;
    Ok(receipt)
}

fn fixed<const N: usize>(row: &Row, index: usize) -> Result<[u8; N], ArchiveAttemptError> {
    row.try_get::<_, &[u8]>(index)
        .map_err(|_| ArchiveAttemptError::InvalidMetadata)?
        .try_into()
        .map_err(|_| ArchiveAttemptError::InvalidMetadata)
}
fn number(row: &Row, index: usize) -> Result<u64, ArchiveAttemptError> {
    row.try_get::<_, &str>(index)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or(ArchiveAttemptError::InvalidMetadata)
}

pub(super) async fn register(
    tx: Transaction<'_>,
    request: &ArchiveAttemptRequest,
    issuer: ControlOwner,
    commit_started: &Cell<bool>,
) -> Result<ArchiveAttemptReceipt, ArchiveAttemptError> {
    let dataset = &request.dataset.as_bytes()[..];
    let attempt = &request.attempt_id[..];
    let catalog = &request.catalog.as_bytes()[..];
    let coordinator = &request.coordinator.as_bytes()[..];
    let worker = &request.worker.as_bytes()[..];
    let predecessor = request.predecessor.to_string();
    let ordinal = request.ordinal.to_string();
    let topology = request.topology.get().to_string();
    let digest = &request.batch_digest.as_bytes()[..];
    // Equality is evaluated in PostgreSQL; no unbounded stored value is returned.
    // Retries compare every request field, retaining the original issuer even
    // when the resolving Control session has a later epoch.
    let row = tx.query_opt("SELECT dataset_id=$2::bytea AND catalog_id=$3::bytea AND expected_predecessor=$4::text::numeric AND ordinal=$5::text::numeric AND schema_version=1 AND topology_generation=$6::text::numeric AND batch_digest=$7::bytea AND coordinator_incarnation=$8::bytea AND worker_incarnation=$9::bytea FROM control.archive_attempts WHERE attempt_id=$1::bytea", &[&attempt,&dataset,&catalog,&predecessor,&ordinal,&topology,&digest,&coordinator,&worker]).await.map_err(backend)?;
    if let Some(row) = row {
        if !row.try_get::<_, bool>(0).map_err(backend)? {
            return Err(ArchiveAttemptError::Conflict);
        }
        let latest = tx.query_one("SELECT NOT EXISTS (SELECT 1 FROM control.archive_attempts WHERE dataset_id=$1::bytea AND ordinal>$2::text::numeric)", &[&dataset,&ordinal]).await.map_err(backend)?.try_get(0).map_err(backend)?;
        tx.rollback().await.map_err(backend)?;
        return Ok(ArchiveAttemptReceipt {
            request: request.clone(),
            ordinal: request.ordinal,
            latest_issued: latest,
            created: false,
        });
    }
    let matches: bool = tx.query_one("SELECT COALESCE((SELECT ordinal=$2::text::numeric AND catalog_id=$3::bytea FROM control.archive_attempts WHERE dataset_id=$1::bytea ORDER BY ordinal DESC LIMIT 1), $2::text::numeric=0)", &[&dataset,&predecessor,&catalog]).await.map_err(backend)?.try_get(0).map_err(backend)?;
    if !matches {
        return Err(ArchiveAttemptError::Conflict);
    }
    tx.execute("INSERT INTO control.archive_attempts(attempt_id,dataset_id,catalog_id,ordinal,expected_predecessor,schema_version,topology_generation,batch_digest,coordinator_incarnation,worker_incarnation,issuer_resource,issuer_epoch,issuer_incarnation,issued_at_us) VALUES ($1::bytea,$2::bytea,$3::bytea,$4::text::numeric,$5::text::numeric,1,$6::text::numeric,$7::bytea,$8::bytea,$9::bytea,$10::bytea,$11::text::numeric,$12::bytea,(EXTRACT(EPOCH FROM pg_catalog.clock_timestamp())*1000000)::bigint)", &[&attempt,&dataset,&catalog,&ordinal,&predecessor,&topology,&digest,&coordinator,&worker,&&issuer.resource_id.as_bytes()[..],&issuer.epoch.get().to_string(),&&issuer.process_incarnation.as_bytes()[..]]).await.map_err(backend)?;
    commit_started.set(true);
    tx.commit()
        .await
        .map_err(|_| ArchiveAttemptError::CommitUnknown)?;
    Ok(ArchiveAttemptReceipt {
        request: request.clone(),
        ordinal: request.ordinal,
        latest_issued: true,
        created: true,
    })
}
