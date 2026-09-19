//! Immutable reservation metadata. No row here is a native admission receipt.
use super::ingest_registry::ControlOwner;
use crate::storage::rocks::store::StagedProducerRequest;
use bigbrotr_model::{DatasetId, Digest256, ProducerRequestId};
use tokio_postgres::Transaction;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReservationError {
    Backend,
    Conflict,
    InvalidMetadata,
    /// PostgreSQL may have committed; native dispatch has not begun.
    CommitUnknown,
}
impl std::fmt::Display for ReservationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "producer request reservation unavailable: {self:?}")
    }
}
impl std::error::Error for ReservationError {}
fn backend(_: tokio_postgres::Error) -> ReservationError {
    ReservationError::Backend
}

/// Bounded observation for recovery by a current Control owner. It does not
/// reconstruct the former process, authorize dispatch or establish an outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerReservation {
    pub request: ProducerRequestId,
    pub digest: Digest256,
    pub input_bytes: u32,
    pub manifest_sha256: [u8; 32],
}

pub(super) async fn read(
    tx: &Transaction<'_>,
    dataset: DatasetId,
    request: ProducerRequestId,
) -> Result<Option<ProducerReservation>, ReservationError> {
    let row = tx.query_opt("SELECT substring(request_digest FROM 1 FOR 33), input_bytes, substring(manifest_sha256 FROM 1 FOR 33), input_profile FROM control.producer_requests WHERE dataset_id = $1::bytea AND producer_slot_id = $2::bytea AND producer_epoch = $3::text::numeric AND request_sequence = $4::text::numeric",
        &[&&dataset.as_bytes()[..], &&request.producer_slot_id.as_bytes()[..], &request.producer_epoch.get().to_string(), &request.request_sequence.get().to_string()]).await.map_err(backend)?;
    row.map(|row| {
        let fixed = |index| -> Result<[u8; 32], ReservationError> {
            row.try_get::<_, &[u8]>(index)
                .map_err(backend)?
                .try_into()
                .map_err(|_| ReservationError::InvalidMetadata)
        };
        let input_bytes: i32 = row.try_get(1).map_err(backend)?;
        if !(1..=16 * 1024 * 1024).contains(&input_bytes)
            || row.try_get::<_, i16>(3).map_err(backend)? != 1
        {
            return Err(ReservationError::InvalidMetadata);
        }
        Ok(ProducerReservation {
            request,
            digest: Digest256::from_bytes(fixed(0)?),
            input_bytes: input_bytes as u32,
            manifest_sha256: fixed(2)?,
        })
    })
    .transpose()
}

pub(super) async fn reserve(
    tx: &Transaction<'_>,
    staged: &StagedProducerRequest,
    issuer: ControlOwner,
) -> Result<(), ReservationError> {
    // The bridge already holds the exact owner/registry locks. Upgrade producer
    // serialization before a separate READ COMMITTED observation of prior rows.
    tx.query_one("SELECT reserved_through_request::text FROM control.producer_epochs WHERE dataset_id = $1::bytea AND producer_slot_id = $2::bytea AND producer_epoch = $3::text::numeric FOR UPDATE",
        &[&&staged.dataset.as_bytes()[..], &&staged.request.producer_slot_id.as_bytes()[..], &staged.request.producer_epoch.get().to_string()]).await.map_err(backend)?;
    if let Some(row) = read(tx, staged.dataset, staged.request).await? {
        if row.digest != staged.digest
            || row.input_bytes != staged.input_bytes
            || row.manifest_sha256 != staged.manifest_sha256
        {
            return Err(ReservationError::Conflict);
        }
        return Ok(());
    }
    tx.execute("INSERT INTO control.producer_requests(producer_slot_id, producer_epoch, request_sequence, dataset_id, request_digest, input_bytes, input_profile, manifest_sha256, issuer_resource, issuer_epoch, issuer_incarnation, reserved_at_us) VALUES ($1::bytea, $2::text::numeric, $3::text::numeric, $4::bytea, $5::bytea, $6::integer, 1, $7::bytea, $8::bytea, $9::text::numeric, $10::bytea, (EXTRACT(EPOCH FROM pg_catalog.clock_timestamp()) * 1000000)::bigint)",
        &[&&staged.request.producer_slot_id.as_bytes()[..], &staged.request.producer_epoch.get().to_string(), &staged.request.request_sequence.get().to_string(), &&staged.dataset.as_bytes()[..], &&staged.digest.as_bytes()[..], &(staged.input_bytes as i32), &&staged.manifest_sha256[..], &&issuer.resource_id.as_bytes()[..], &issuer.epoch.get().to_string(), &&issuer.process_incarnation.as_bytes()[..]]).await.map_err(backend)?;
    Ok(())
}
