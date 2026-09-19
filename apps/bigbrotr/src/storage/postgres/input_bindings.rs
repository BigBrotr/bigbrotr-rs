//! Immutable import positions bound to original staged mutation requests.
//! These values describe work; neither construction nor a PG row grants authority.
use crate::acquisition::import::RecordPosition;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ImportJobBinding {
    pub id: [u8; 16],
    pub request_digest: [u8; 32],
    pub expected_revision: u64,
    pub config_revision: u64,
    pub policy_revision: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputBindingError {
    InvalidInput,
    Backend,
    Conflict,
    /// The complete PG binding/reservation transaction may have committed.
    CommitUnknown,
}
impl std::fmt::Display for InputBindingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "import input binding unavailable: {self:?}")
    }
}
impl std::error::Error for InputBindingError {}

/// Initial event-only JSONL batch: one strict Admit obligation per record.
/// Claims, certification, rejection classification and logical completion are
/// separate later consumers. In particular, this cannot silently omit a sidecar.
#[derive(Debug)]
pub struct ImportBatch<'a> {
    pub(crate) job: ImportJobBinding,
    pub(crate) namespace: [u8; 32],
    pub(crate) shard: u32,
    pub(crate) positions: &'a [RecordPosition],
}
impl<'a> ImportBatch<'a> {
    pub fn new(
        job: ImportJobBinding,
        namespace: [u8; 32],
        shard: u32,
        positions: &'a [RecordPosition],
    ) -> Result<Self, InputBindingError> {
        if job.id == [0; 16]
            || job.expected_revision == 0
            || job.config_revision == 0
            || job.policy_revision == 0
            || positions.is_empty()
            || positions.len() > 64
        {
            return Err(InputBindingError::InvalidInput);
        }
        let first = positions[0];
        for (index, position) in positions.iter().enumerate() {
            if position.source != first.source
                || position.line == 0
                || first.line.checked_add(index as u64) != Some(position.line)
                || position.byte_start < position.line.saturating_sub(1)
                || position.byte_start >= position.byte_end
                || position.byte_end > position.source.bytes
                || (index > 0 && positions[index - 1].byte_end != position.byte_start)
                || (position.line == 1 && position.byte_start != 0)
            {
                return Err(InputBindingError::InvalidInput);
            }
        }
        Ok(Self {
            job,
            namespace,
            shard,
            positions,
        })
    }
    pub fn record_count(&self) -> usize {
        self.positions.len()
    }
    pub fn first_record(&self) -> u64 {
        self.positions[0].line
    }
}

use super::ingest_registry::ControlOwner;
use crate::storage::rocks::store::StagedProducerRequest;
use bigbrotr_model::{DatasetId, ProducerRequestId};
use tokio_postgres::Transaction;
fn backend(_: tokio_postgres::Error) -> InputBindingError {
    InputBindingError::Backend
}

pub(super) async fn lock_job(
    tx: &Transaction<'_>,
    dataset: DatasetId,
    owner: ControlOwner,
    job: ImportJobBinding,
) -> Result<(), InputBindingError> {
    let row = tx.query_opt("SELECT dataset_id = $2::bytea AND kind = 'import' AND state = 'running' AND input_complete AND request_digest = $3::bytea AND job_revision = $4::text::numeric AND config_revision = $5::text::numeric AND policy_revision = $6::text::numeric AND owner_resource_id = $7::bytea AND owner_epoch = $8::text::numeric AND owner_incarnation = $9::bytea FROM control.jobs WHERE job_id = $1::bytea FOR UPDATE",
        &[&&job.id[..], &&dataset.as_bytes()[..], &&job.request_digest[..], &job.expected_revision.to_string(), &job.config_revision.to_string(), &job.policy_revision.to_string(), &&owner.resource_id.as_bytes()[..], &owner.epoch.get().to_string(), &&owner.process_incarnation.as_bytes()[..]]).await.map_err(backend)?;
    if row.and_then(|r| r.try_get::<_, Option<bool>>(0).ok().flatten()) != Some(true) {
        return Err(InputBindingError::Conflict);
    }
    Ok(())
}

/// A retained binding restricts dispatch to its current running job. This is
/// rechecked in both reservation and native transactions, before producer locks.
/// Absence is allowed for the separately specified non-import initial consumers;
/// it never lets the batch adapter retrospectively attach an old reservation.
pub(super) async fn lock_request_job(
    tx: &Transaction<'_>,
    dataset: DatasetId,
    owner: ControlOwner,
    request: ProducerRequestId,
    config: u64,
    policy: u64,
) -> Result<(), InputBindingError> {
    let row = tx.query_opt("SELECT j.dataset_id = $4::bytea AND j.kind = 'import' AND j.state = 'running' AND j.input_complete AND j.config_revision = $5::text::numeric AND j.policy_revision = $6::text::numeric AND j.owner_resource_id = $7::bytea AND j.owner_epoch = $8::text::numeric AND j.owner_incarnation = $9::bytea FROM control.producer_input_bindings b JOIN control.jobs j USING (job_id) WHERE b.producer_slot_id = $1::bytea AND b.producer_epoch = $2::text::numeric AND b.request_sequence = $3::text::numeric FOR UPDATE OF j",
        &[&&request.producer_slot_id.as_bytes()[..], &request.producer_epoch.get().to_string(), &request.request_sequence.get().to_string(), &&dataset.as_bytes()[..], &config.to_string(), &policy.to_string(), &&owner.resource_id.as_bytes()[..], &owner.epoch.get().to_string(), &&owner.process_incarnation.as_bytes()[..]]).await.map_err(backend)?;
    if let Some(row) = row
        && row.try_get::<_, Option<bool>>(0).map_err(backend)? != Some(true)
    {
        return Err(InputBindingError::Conflict);
    }
    Ok(())
}

/// Inserts the entire list before reservations. The deferred SQL closure/FKs
/// require every exact reservation in this transaction. Existing lists are
/// compared completely; missing or changed bindings cannot be patched in place.
pub(super) async fn record_batch(
    tx: &Transaction<'_>,
    batch: &ImportBatch<'_>,
    staged: &[StagedProducerRequest],
    issuer: ControlOwner,
) -> Result<(), InputBindingError> {
    if batch.positions.len() != staged.len() {
        return Err(InputBindingError::InvalidInput);
    }
    let count = staged.len() as i16;
    let first = batch.first_record().to_string();
    let shard = i64::from(batch.shard);
    let prior: i64 = tx.query_one("SELECT count(*) FROM control.producer_input_bindings WHERE job_id = $1::bytea AND shard_id = $2::bigint AND batch_first_record = $3::text::numeric", &[&&batch.job.id[..], &shard, &first]).await.map_err(backend)?.try_get(0).map_err(backend)?;
    if prior != 0 && prior != i64::from(count) {
        return Err(InputBindingError::Conflict);
    }
    for (index, (position, request)) in batch.positions.iter().zip(staged).enumerate() {
        let ordinal = index as i16;
        let source_bytes = position.source.bytes.to_string();
        let line = position.line.to_string();
        let start = position.byte_start.to_string();
        let end = position.byte_end.to_string();
        let epoch = request.request.producer_epoch.get().to_string();
        let sequence = request.request.request_sequence.get().to_string();
        let bytes = request.input_bytes as i32;
        let owner_epoch = issuer.epoch.get().to_string();
        let parameters: &[&(dyn tokio_postgres::types::ToSql + Sync)] = &[
            &&batch.job.id[..],
            &&request.dataset.as_bytes()[..],
            &&batch.namespace[..],
            &shard,
            &source_bytes,
            &&position.source.sha256[..],
            &first,
            &count,
            &ordinal,
            &line,
            &start,
            &end,
            &&position.sha256[..],
            &&request.request.producer_slot_id.as_bytes()[..],
            &epoch,
            &sequence,
            &&request.digest.as_bytes()[..],
            &bytes,
            &&request.manifest_sha256[..],
            &&issuer.resource_id.as_bytes()[..],
            &owner_epoch,
            &&issuer.process_incarnation.as_bytes()[..],
        ];
        if prior > 0 {
            let row = tx.query_opt("SELECT dataset_id = $2::bytea AND source_namespace = $3::bytea AND source_bytes = $5::text::numeric AND source_sha256 = $6::bytea AND parser_profile = 1 AND batch_records = $8::smallint AND obligation_index = $9::smallint AND obligation_kind = 1 AND logical_record = $10::text::numeric AND byte_start = $11::text::numeric AND byte_end = $12::text::numeric AND record_sha256 = $13::bytea AND producer_slot_id = $14::bytea AND producer_epoch = $15::text::numeric AND request_sequence = $16::text::numeric AND request_digest = $17::bytea AND input_bytes = $18::integer AND input_profile = 1 AND manifest_sha256 = $19::bytea AND issuer_resource = $20::bytea AND issuer_epoch = $21::text::numeric AND issuer_incarnation = $22::bytea FROM control.producer_input_bindings WHERE job_id = $1::bytea AND shard_id = $4::bigint AND batch_first_record = $7::text::numeric AND obligation_index = $9::smallint", parameters).await.map_err(backend)?;
            if row.and_then(|r| r.try_get::<_, bool>(0).ok()) != Some(true) {
                return Err(InputBindingError::Conflict);
            }
        } else {
            tx.execute("INSERT INTO control.producer_input_bindings(job_id, dataset_id, source_namespace, shard_id, source_bytes, source_sha256, parser_profile, batch_first_record, batch_records, obligation_index, obligation_kind, logical_record, byte_start, byte_end, record_sha256, producer_slot_id, producer_epoch, request_sequence, request_digest, input_bytes, input_profile, manifest_sha256, issuer_resource, issuer_epoch, issuer_incarnation) VALUES ($1::bytea, $2::bytea, $3::bytea, $4::bigint, $5::text::numeric, $6::bytea, 1, $7::text::numeric, $8::smallint, $9::smallint, 1, $10::text::numeric, $11::text::numeric, $12::text::numeric, $13::bytea, $14::bytea, $15::text::numeric, $16::text::numeric, $17::bytea, $18::integer, 1, $19::bytea, $20::bytea, $21::text::numeric, $22::bytea)", parameters).await.map_err(backend)?;
        }
    }
    Ok(())
}
