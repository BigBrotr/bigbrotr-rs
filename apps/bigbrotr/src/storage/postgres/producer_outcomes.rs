//! Complete native outcomes copied under the reservation's finite retained grant.
use super::ingest_registry::ControlOwner;
use crate::storage::rocks::store::{
    MAX_OUTCOME_BYTES, NativeOutcomeCopy, NativeOutcomeKind, StagedProducerRequest,
};
use bigbrotr_model::{DatasetId, Digest256, PolicyEpoch, ProducerRequestId};
use sha2::{Digest, Sha256};
use tokio_postgres::Transaction;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutcomeError {
    Backend,
    Conflict,
    InvalidMetadata,
    /// Native completion is established; the PG copy may have committed.
    CommitUnknown,
}
impl std::fmt::Display for OutcomeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "producer outcome copy unavailable: {self:?}")
    }
}
impl std::error::Error for OutcomeError {}
fn backend(_: tokio_postgres::Error) -> OutcomeError {
    OutcomeError::Backend
}

/// Bounded diagnostics for a fresh Control owner, not an event progress basis.
/// Complete canonical bytes stay in PG; native replay still resolves admission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerOutcomeObservation {
    pub request: ProducerRequestId,
    pub request_digest: Digest256,
    pub manifest_sha256: [u8; 32],
    pub kind: NativeOutcomeKind,
    pub policy_epoch: PolicyEpoch,
    pub outcome_sha256: [u8; 32],
    pub outcome_bytes: u32,
}

pub(super) async fn read(
    tx: &Transaction<'_>,
    dataset: DatasetId,
    request: ProducerRequestId,
) -> Result<Option<ProducerOutcomeObservation>, OutcomeError> {
    let row = tx.query_opt("SELECT substring(request_digest FROM 1 FOR 33), substring(manifest_sha256 FROM 1 FOR 33), outcome_kind, left(policy_epoch::text, 21), substring(outcome_sha256 FROM 1 FOR 33), octet_length(outcome_bytes) FROM control.producer_outcomes WHERE dataset_id = $1::bytea AND producer_slot_id = $2::bytea AND producer_epoch = $3::text::numeric AND request_sequence = $4::text::numeric",
        &[&&dataset.as_bytes()[..], &&request.producer_slot_id.as_bytes()[..], &request.producer_epoch.get().to_string(), &request.request_sequence.get().to_string()]).await.map_err(backend)?;
    row.map(|row| {
        let fixed = |index| -> Result<[u8; 32], OutcomeError> {
            row.try_get::<_, &[u8]>(index)
                .map_err(backend)?
                .try_into()
                .map_err(|_| OutcomeError::InvalidMetadata)
        };
        let kind = match row.try_get::<_, i16>(2).map_err(backend)? {
            1 => NativeOutcomeKind::NewFacts,
            2 => NativeOutcomeKind::AlreadyDurable,
            3 => NativeOutcomeKind::Rejected,
            _ => return Err(OutcomeError::InvalidMetadata),
        };
        let count: i32 = row.try_get(5).map_err(backend)?;
        if !(1..=MAX_OUTCOME_BYTES as i32).contains(&count) {
            return Err(OutcomeError::InvalidMetadata);
        }
        let policy = row.try_get::<_, &str>(3).map_err(backend)?;
        if policy.is_empty() || !policy.bytes().all(|b| b.is_ascii_digit()) {
            return Err(OutcomeError::InvalidMetadata);
        }
        Ok(ProducerOutcomeObservation {
            request,
            request_digest: Digest256::from_bytes(fixed(0)?),
            manifest_sha256: fixed(1)?,
            kind,
            policy_epoch: PolicyEpoch::new(
                policy.parse().map_err(|_| OutcomeError::InvalidMetadata)?,
            ),
            outcome_sha256: fixed(4)?,
            outcome_bytes: count as u32,
        })
    })
    .transpose()
}

pub(super) async fn record(
    tx: &Transaction<'_>,
    staged: &StagedProducerRequest,
    outcome: &NativeOutcomeCopy,
    recorder: ControlOwner,
) -> Result<(), OutcomeError> {
    if outcome.request() != staged.request
        || outcome.dataset() != staged.dataset
        || outcome.digest() != staged.digest
        || outcome.manifest_sha256() != staged.manifest_sha256
        || outcome.bytes().is_empty()
        || outcome.bytes().len() > MAX_OUTCOME_BYTES
    {
        return Err(OutcomeError::Conflict);
    }
    let request = staged.request;
    let epoch = request.producer_epoch.get().to_string();
    let sequence = request.request_sequence.get().to_string();
    let policy = outcome.policy_epoch().get().to_string();
    let kind = outcome.kind() as i16;
    let sha256: [u8; 32] = Sha256::digest(outcome.bytes()).into();
    // Upgrade the already-held registry lock before observing a competing copy.
    tx.query_one("SELECT reserved_through_request::text FROM control.producer_epochs WHERE dataset_id = $1::bytea AND producer_slot_id = $2::bytea AND producer_epoch = $3::text::numeric FOR UPDATE",
        &[&&staged.dataset.as_bytes()[..], &&request.producer_slot_id.as_bytes()[..], &epoch]).await.map_err(backend)?;
    let prior = tx.query_opt("SELECT dataset_id = $4::bytea AND request_digest = $5::bytea AND manifest_sha256 = $6::bytea AND outcome_kind = $7::smallint AND policy_epoch = $8::text::numeric AND outcome_bytes = $9::bytea AND outcome_sha256 = $10::bytea FROM control.producer_outcomes WHERE producer_slot_id = $1::bytea AND producer_epoch = $2::text::numeric AND request_sequence = $3::text::numeric",
        &[&&request.producer_slot_id.as_bytes()[..], &epoch, &sequence, &&staged.dataset.as_bytes()[..], &&staged.digest.as_bytes()[..], &&staged.manifest_sha256[..], &kind, &policy, &outcome.bytes(), &&sha256[..]]).await.map_err(backend)?;
    if let Some(row) = prior {
        return if row.try_get::<_, bool>(0).map_err(backend)? {
            Ok(())
        } else {
            Err(OutcomeError::Conflict)
        };
    }
    tx.execute("INSERT INTO control.producer_outcomes(producer_slot_id, producer_epoch, request_sequence, dataset_id, request_digest, manifest_sha256, outcome_kind, policy_epoch, outcome_bytes, outcome_sha256, recorder_resource, recorder_epoch, recorder_incarnation, recorded_at_us) VALUES ($1::bytea, $2::text::numeric, $3::text::numeric, $4::bytea, $5::bytea, $6::bytea, $7::smallint, $8::text::numeric, $9::bytea, $10::bytea, $11::bytea, $12::text::numeric, $13::bytea, (EXTRACT(EPOCH FROM pg_catalog.clock_timestamp()) * 1000000)::bigint)",
        &[&&request.producer_slot_id.as_bytes()[..], &epoch, &sequence, &&staged.dataset.as_bytes()[..], &&staged.digest.as_bytes()[..], &&staged.manifest_sha256[..], &kind, &policy, &outcome.bytes(), &&sha256[..], &&recorder.resource_id.as_bytes()[..], &recorder.epoch.get().to_string(), &&recorder.process_incarnation.as_bytes()[..]]).await.map_err(backend)?;
    Ok(())
}
