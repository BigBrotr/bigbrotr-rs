//! Logical record closure from the current native owner, never PG diagnostics.
use super::ingest_registry::ControlOwner;
use crate::storage::rocks::store::{NativeOutcomeCopy, NativeOutcomeKind};
use sha2::{Digest, Sha256};
use tokio_postgres::Transaction;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImportCheckpointError {
    Backend,
    Conflict,
}
impl std::fmt::Display for ImportCheckpointError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "import checkpoint unavailable: {self:?}")
    }
}
impl std::error::Error for ImportCheckpointError {}
fn backend(_: tokio_postgres::Error) -> ImportCheckpointError {
    ImportCheckpointError::Backend
}

/// Called only after the complete outcome copy in the same fenced transaction.
/// The witness cannot be made by decoding a receipt or reading its PG metadata.
/// SQL advances the contiguous head atomically with new membership; it never
/// substitutes a request sequence or dependency maximum for the logical prefix.
pub(super) async fn close_record(
    tx: &Transaction<'_>,
    outcome: &NativeOutcomeCopy,
    recorder: ControlOwner,
) -> Result<(), ImportCheckpointError> {
    if outcome.kind() == NativeOutcomeKind::Rejected {
        // Rejection classification/whole-record closure has a different shape.
        return Ok(());
    }
    let request = outcome.request();
    let epoch = request.producer_epoch.get().to_string();
    let sequence = request.request_sequence.get().to_string();
    let dataset = outcome.dataset();
    let digest = outcome.digest();
    let manifest = outcome.manifest_sha256();
    let sha256: [u8; 32] = Sha256::digest(outcome.bytes()).into();
    let parameters: &[&(dyn tokio_postgres::types::ToSql + Sync)] = &[
        &&request.producer_slot_id.as_bytes()[..],
        &epoch,
        &sequence,
        &&dataset.as_bytes()[..],
        &&digest.as_bytes()[..],
        &&manifest[..],
        &&sha256[..],
    ];
    // The caller already locked Control, the bound job, Ingest and producer.
    // Return only booleans; complete receipt bytes stay in the outcome ledger.
    let row = tx.query_opt("SELECT b.dataset_id = $4::bytea AND b.request_digest = $5::bytea AND b.manifest_sha256 = $6::bytea AND b.parser_profile = 1 AND b.obligation_kind = 1, c.outcome_sha256 = $7::bytea AND c.dataset_id = b.dataset_id AND c.job_id = b.job_id AND c.shard_id = b.shard_id AND c.logical_record = b.logical_record FROM control.producer_input_bindings b LEFT JOIN control.import_record_closures c USING (producer_slot_id, producer_epoch, request_sequence) WHERE b.producer_slot_id = $1::bytea AND b.producer_epoch = $2::text::numeric AND b.request_sequence = $3::text::numeric", parameters).await.map_err(backend)?;
    let Some(row) = row else {
        // Separately specified unbound initial consumers produce no progress.
        return Ok(());
    };
    if !row.try_get::<_, bool>(0).map_err(backend)? {
        return Err(ImportCheckpointError::Conflict);
    }
    if let Some(matches) = row.try_get::<_, Option<bool>>(1).map_err(backend)? {
        return if matches {
            Ok(())
        } else {
            Err(ImportCheckpointError::Conflict)
        };
    }
    let inserted = tx.execute("INSERT INTO control.import_record_closures(job_id, dataset_id, shard_id, logical_record, producer_slot_id, producer_epoch, request_sequence, outcome_sha256, recorder_resource, recorder_epoch, recorder_incarnation, recorded_at_us) SELECT job_id, dataset_id, shard_id, logical_record, producer_slot_id, producer_epoch, request_sequence, $4::bytea, $5::bytea, $6::text::numeric, $7::bytea, (EXTRACT(EPOCH FROM pg_catalog.clock_timestamp()) * 1000000)::bigint FROM control.producer_input_bindings WHERE producer_slot_id = $1::bytea AND producer_epoch = $2::text::numeric AND request_sequence = $3::text::numeric", &[parameters[0], &epoch, &sequence, &&sha256[..], &&recorder.resource_id.as_bytes()[..], &recorder.epoch.get().to_string(), &&recorder.process_incarnation.as_bytes()[..]]).await.map_err(backend)?;
    if inserted != 1 {
        return Err(ImportCheckpointError::Conflict);
    }
    Ok(())
}
