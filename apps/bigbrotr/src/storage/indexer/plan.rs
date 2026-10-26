//! Plan one complete retained batch before any remote Index effect.
//!
//! Only one fact and its tag references are decoded at a time. Counting and
//! hashing read the same immutable owned bytes; no serving rows are collected.
use super::super::{
    clickhouse::rows::{ChunkHasher, FactRows, PARTITION, RowsError, SCHEMA_VERSION},
    postgres::{
        INDEX_TARGET_COUNT, IndexBinding, IndexChunk, IndexError, IndexPlan, IndexProvenance,
        IndexTarget, MAX_INDEX_STREAMS,
    },
    rocks::store::{PendingBatch, PendingReadLimits},
};
use crate::runtime::OwnedPermit;
use bigbrotr_model::{
    AdmissionBatch, CompleteThrough, Fact, FactRef, SourceCut, StreamBound,
    codec::{CodecError, FactDecodeLimits},
};
use std::{
    mem::size_of,
    sync::atomic::{AtomicBool, Ordering},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanError {
    InvalidLimits,
    InsufficientReservation,
    Cancelled,
    InvalidSource,
    SourceLimit,
    Allocation,
    Decode(CodecError),
    Rows(RowsError),
    Metadata(IndexError),
}
impl std::fmt::Display for PlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "initial Index planning failed: {self:?}")
    }
}
impl std::error::Error for PlanError {}

/// Not a publication capability. The source's existing grant and this planner's
/// decoder/metadata grant remain owned through dispatch and replay serialization.
/// Fields drop before their grants; no caller can detach the data reservation.
pub struct PlannedBatch {
    plan: IndexPlan,
    source: PendingBatch,
    wire_bytes: [u64; INDEX_TARGET_COUNT],
    fact_limits: FactDecodeLimits,
    _permit: OwnedPermit,
}
impl PlannedBatch {
    pub fn plan(&self) -> &IndexPlan {
        &self.plan
    }
    pub fn source(&self) -> &PendingBatch {
        &self.source
    }
    pub const fn wire_bytes(&self) -> &[u64; INDEX_TARGET_COUNT] {
        &self.wire_bytes
    }
    pub const fn fact_limits(&self) -> FactDecodeLimits {
        self.fact_limits
    }
}

/// Additional requested allocation, excluding the separately owned encoded
/// PendingBatch, native engine, network/driver and decoded readback reservations.
/// Allocator overhead and process RSS require their enclosing process envelope.
pub fn required_data_bytes(limits: PendingReadLimits) -> Result<u64, PlanError> {
    limits
        .required_data_bytes()
        .map_err(|_| PlanError::InvalidLimits)?;
    let tags = FactRows::required_tag_reference_bytes(limits.fact).map_err(PlanError::Rows)?;
    limits
        .max_records
        .checked_mul(size_of::<FactRef>())
        .and_then(|n| n.checked_add(limits.fact.max_allocation_bytes))
        .and_then(|n| n.checked_add(tags))
        .and_then(|n| n.checked_add(size_of::<Fact>() + size_of::<PlannedBatch>()))
        .and_then(|n| n.checked_add(2 * MAX_INDEX_STREAMS * size_of::<StreamBound>()))
        .and_then(|n| {
            n.checked_add(
                INDEX_TARGET_COUNT
                    * (size_of::<ChunkHasher<'_>>() + size_of::<IndexChunk>() + PARTITION.len()),
            )
        })
        .and_then(|n| u64::try_from(n).ok())
        .ok_or(PlanError::InvalidLimits)
}

fn check_cancel(cancelled: &AtomicBool) -> Result<(), PlanError> {
    if cancelled.load(Ordering::Acquire) {
        Err(PlanError::Cancelled)
    } else {
        Ok(())
    }
}

fn advanced_cut(before: &SourceCut, batch: &AdmissionBatch) -> Result<SourceCut, PlanError> {
    if before.dataset_id() != batch.first().dataset_id
        || before.bounds().is_empty()
        || before.bounds().len() > MAX_INDEX_STREAMS
    {
        return Err(PlanError::InvalidSource);
    }
    let position = before
        .bounds()
        .binary_search_by_key(&batch.first().stream(), |bound| bound.stream)
        .map_err(|_| PlanError::InvalidSource)?;
    if before.bounds()[position]
        .last_complete_sequence
        .get()
        .checked_add(1)
        != Some(batch.first().sequence.get())
    {
        return Err(PlanError::InvalidSource);
    }
    let mut bounds = Vec::new();
    bounds
        .try_reserve_exact(before.bounds().len())
        .map_err(|_| PlanError::Allocation)?;
    bounds.extend_from_slice(before.bounds());
    bounds[position].last_complete_sequence = CompleteThrough::new(batch.last().sequence.get());
    SourceCut::new(before.dataset_id(), before.topology_generation(), bounds)
        .map_err(|_| PlanError::InvalidSource)
}

/// Derive all ten outcomes, including zero rows. A gap, foreign stream, corrupt
/// preimage or disabled-provenance witness refuses the complete batch. No remote
/// effect, frontier movement or reclamation occurs here.
pub fn plan_batch(
    binding: IndexBinding,
    expected_revision: u64,
    before: SourceCut,
    source: PendingBatch,
    limits: PendingReadLimits,
    permit: OwnedPermit,
    cancelled: &AtomicBool,
) -> Result<PlannedBatch, PlanError> {
    let required = required_data_bytes(limits)?;
    if permit.request().units().data_bytes() < required || permit.request().units().items() == 0 {
        return Err(PlanError::InsufficientReservation);
    }
    let (plan, wire_bytes) = plan_source(
        binding,
        expected_revision,
        before,
        source.batch(),
        source.records(),
        limits,
        cancelled,
    )?;
    Ok(PlannedBatch {
        plan,
        source,
        wire_bytes,
        fact_limits: limits.fact,
        _permit: permit,
    })
}

// Private pure calculation; only plan_batch transfers actual retained source
// custody to dispatch. Unit fixtures do not manufacture a PendingBatch or proof.
fn plan_source(
    binding: IndexBinding,
    expected_revision: u64,
    before: SourceCut,
    batch: &AdmissionBatch,
    records: &[Vec<u8>],
    limits: PendingReadLimits,
    cancelled: &AtomicBool,
) -> Result<(IndexPlan, [u64; INDEX_TARGET_COUNT]), PlanError> {
    check_cancel(cancelled)?;
    if batch.first().dataset_id != binding.dataset() || records.len() != batch.records().len() {
        return Err(PlanError::InvalidSource);
    }
    if records.len() > limits.max_records
        || records
            .iter()
            .try_fold(0usize, |n, bytes| n.checked_add(bytes.len()))
            .is_none_or(|n| n > limits.max_encoded_bytes)
    {
        return Err(PlanError::SourceLimit);
    }
    let after = advanced_cut(&before, batch)?;
    let decode = |bytes: &[u8], reference: &FactRef| -> Result<Fact, PlanError> {
        check_cancel(cancelled)?;
        let fact = Fact::decode_preimage(bytes, reference.payload_digest, limits.fact)
            .map_err(PlanError::Decode)?;
        if fact.source_record_id() != reference.source_record_id {
            return Err(PlanError::InvalidSource);
        }
        Ok(fact)
    };
    let provenance = binding.provenance() == IndexProvenance::LocalFirstWitness;
    let mut counts = [0u64; INDEX_TARGET_COUNT];
    for (bytes, reference) in records.iter().zip(batch.records()) {
        let fact = decode(bytes, reference)?;
        let rows = FactRows::new(binding.generation(), &fact, provenance, limits.fact)
            .map_err(PlanError::Rows)?;
        for (count, added) in counts.iter_mut().zip(rows.counts()) {
            *count = count.checked_add(added).ok_or(PlanError::InvalidLimits)?;
        }
    }
    let mut hashers = std::array::from_fn(|i| {
        ChunkHasher::new(binding.generation(), IndexTarget::ALL[i], &after, counts[i])
    });
    for (bytes, reference) in records.iter().zip(batch.records()) {
        let fact = decode(bytes, reference)?;
        FactRows::new(binding.generation(), &fact, provenance, limits.fact)
            .map_err(PlanError::Rows)?
            .hash_into(&mut hashers)
            .map_err(PlanError::Rows)?;
    }
    let mut wire_bytes = [0; INDEX_TARGET_COUNT];
    let mut chunks = Vec::new();
    chunks
        .try_reserve_exact(INDEX_TARGET_COUNT)
        .map_err(|_| PlanError::Allocation)?;
    for (i, hasher) in hashers.into_iter().enumerate() {
        let summary = hasher.finish().map_err(PlanError::Rows)?;
        wire_bytes[i] = summary.row_binary_bytes;
        // projection-chunk/1 already binds generation, target, schema, complete
        // cut, ordinal and every row. It is also this profile's chunk identity.
        chunks.push(
            IndexChunk::new(
                summary.target,
                SCHEMA_VERSION,
                PARTITION.to_owned(),
                summary.rows_digest,
                summary.row_count,
                summary.rows_digest,
            )
            .map_err(PlanError::Metadata)?,
        );
    }
    check_cancel(cancelled)?;
    let plan = IndexPlan::new(
        binding,
        expected_revision,
        before,
        after,
        batch.clone(),
        chunks.try_into().map_err(|_| PlanError::InvalidSource)?,
    )
    .map_err(PlanError::Metadata)?;
    Ok((plan, wire_bytes))
}

#[cfg(test)]
mod tests;
