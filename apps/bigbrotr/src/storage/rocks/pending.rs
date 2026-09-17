//! Bounded, snapshot-consistent pending feed; not a public read or admission grant.
use super::IngestStore;
use crate::runtime::OwnedPermit;
use crate::storage::rocks::keys::IngestKey;
use bigbrotr_model::{
    AdmissionBatch, Digest256, Fact, FactRef, Sequence, SourceRecordId,
    codec::{CodecError, FactDecodeLimits},
};
use rocksdb::ReadOptions;
use std::{
    fmt,
    mem::size_of,
    sync::atomic::{AtomicBool, Ordering},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PendingError {
    InvalidLimits,
    InsufficientReservation,
    ForeignDataset,
    Malformed,
    Limit,
    MissingFact,
    CorruptFact,
    IncompatibleSchema,
    IncompleteBatch,
    EngineRead,
    Allocation,
    Cancelled,
}
impl fmt::Display for PendingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "pending source read failed: {self:?}")
    }
}
impl std::error::Error for PendingError {}

/// Compact boundary retained independently of producer request-detail GC.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingBatchHeader {
    first: SourceRecordId,
    last: SourceRecordId,
    digest: Digest256,
}
impl PendingBatchHeader {
    pub fn from_batch(batch: &AdmissionBatch) -> Self {
        Self {
            first: batch.first(),
            last: batch.last(),
            digest: batch.digest(),
        }
    }
    pub const fn first(self) -> SourceRecordId {
        self.first
    }
    pub const fn last(self) -> SourceRecordId {
        self.last
    }
    pub const fn digest(self) -> Digest256 {
        self.digest
    }
    pub fn encode(self) -> [u8; 42] {
        let mut bytes = [0; 42];
        bytes[..2].copy_from_slice(&[1, 0x21]);
        bytes[2..10].copy_from_slice(&self.last.sequence.get().to_be_bytes());
        bytes[10..].copy_from_slice(self.digest.as_bytes());
        bytes
    }
    pub fn decode(first: SourceRecordId, bytes: &[u8]) -> Result<Self, PendingError> {
        if bytes.len() != 42 || bytes[..2] != [1, 0x21] {
            return Err(PendingError::Malformed);
        }
        let last = u64::from_be_bytes(
            bytes[2..10]
                .try_into()
                .map_err(|_| PendingError::Malformed)?,
        );
        if last < first.sequence.get() {
            return Err(PendingError::Malformed);
        }
        Ok(Self {
            first,
            last: SourceRecordId {
                sequence: Sequence::new(last).map_err(|_| PendingError::Malformed)?,
                ..first
            },
            digest: Digest256::from_bytes(
                bytes[10..]
                    .try_into()
                    .map_err(|_| PendingError::Malformed)?,
            ),
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PendingReadLimits {
    pub max_records: usize,
    pub max_encoded_bytes: usize,
    pub fact: FactDecodeLimits,
}
impl PendingReadLimits {
    /// Requested application allocation floor: all returned encoded bytes and
    /// vector elements, plus one transient decoded fact. Native block/pin cost
    /// stays inside the separately retained engine allowance; neither is RSS.
    pub fn required_data_bytes(self) -> Result<u64, PendingError> {
        if self.max_records == 0
            || self.max_encoded_bytes == 0
            || self.fact.max_encoded_bytes == 0
            || self.fact.max_allocation_bytes == 0
            || self.fact.max_tag_nodes == 0
            || self.fact.max_string_bytes == 0
        {
            return Err(PendingError::InvalidLimits);
        }
        self.max_records
            .checked_mul(size_of::<Vec<u8>>() + size_of::<FactRef>())
            .and_then(|n| n.checked_add(self.max_encoded_bytes))
            .and_then(|n| n.checked_add(self.fact.max_allocation_bytes))
            .and_then(|n| n.checked_add(size_of::<Fact>()))
            .and_then(|n| u64::try_from(n).ok())
            .ok_or(PendingError::InvalidLimits)
    }
}

/// Encoded complete facts retain their data permit through the downstream handoff.
/// Native pins and the temporary decoded fact never escape the read call.
pub struct PendingBatch {
    batch: AdmissionBatch,
    records: Vec<Vec<u8>>,
    _permit: OwnedPermit,
}
impl PendingBatch {
    pub fn batch(&self) -> &AdmissionBatch {
        &self.batch
    }
    pub fn records(&self) -> &[Vec<u8>] {
        &self.records
    }
}

impl IngestStore {
    /// Read one retained complete batch at its exact first source identity.
    /// None means only that the local pending marker is absent (possibly already
    /// archived/reclaimed), never that an Admit did not commit. This internal
    /// feed includes private envelopes; callers must enforce their own grants.
    /// Cancellation is checked between bounded records, not inside native I/O.
    pub fn read_pending_batch(
        &self,
        first: SourceRecordId,
        limits: PendingReadLimits,
        permit: OwnedPermit,
        cancelled: &AtomicBool,
    ) -> Result<Option<PendingBatch>, PendingError> {
        let required = limits.required_data_bytes()?;
        if permit.request().units().data_bytes() < required || permit.request().units().items() == 0
        {
            return Err(PendingError::InsufficientReservation);
        }
        check_cancel(cancelled)?;
        if first.dataset_id != self.manifest.identity().dataset_id {
            return Err(PendingError::ForeignDataset);
        }
        let facts = self.db.cf_handle("facts").ok_or(PendingError::EngineRead)?;
        let snapshot = self.db.snapshot();
        let key = IngestKey::PendingBatch { first }
            .encode()
            .map_err(|_| PendingError::Malformed)?;
        let header = {
            let Some(value) = snapshot
                .get_pinned_cf_opt(facts, key.as_bytes(), read_options())
                .map_err(|_| PendingError::EngineRead)?
            else {
                return Ok(None);
            };
            PendingBatchHeader::decode(first, &value)?
        };
        let count = header
            .last
            .sequence
            .get()
            .checked_sub(first.sequence.get())
            .and_then(|n| n.checked_add(1))
            .and_then(|n| usize::try_from(n).ok())
            .filter(|n| *n <= limits.max_records)
            .ok_or(PendingError::Limit)?;
        let stream = IngestKey::SourceStream {
            dataset_id: first.dataset_id,
            shard_id: first.shard_id,
            owner_epoch: first.owner_epoch,
        }
        .encode()
        .map_err(|_| PendingError::Malformed)?;
        {
            let value = snapshot
                .get_pinned_opt(stream.as_bytes(), read_options())
                .map_err(|_| PendingError::EngineRead)?
                .ok_or(PendingError::IncompleteBatch)?;
            if value.len() != 10 || value[..2] != [1, 2] {
                return Err(PendingError::Malformed);
            }
            let through =
                u64::from_be_bytes(value[2..].try_into().map_err(|_| PendingError::Malformed)?);
            if through < header.last.sequence.get() {
                return Err(PendingError::IncompleteBatch);
            }
        }
        let mut records = Vec::new();
        let mut references = Vec::new();
        records
            .try_reserve_exact(count)
            .map_err(|_| PendingError::Allocation)?;
        references
            .try_reserve_exact(count)
            .map_err(|_| PendingError::Allocation)?;
        let mut remaining = limits.max_encoded_bytes;
        for offset in 0..count {
            check_cancel(cancelled)?;
            let sequence = first
                .sequence
                .get()
                .checked_add(offset as u64)
                .ok_or(PendingError::Limit)?;
            let source_record_id = SourceRecordId {
                sequence: Sequence::new(sequence).map_err(|_| PendingError::Malformed)?,
                ..first
            };
            let key = IngestKey::Fact { source_record_id }
                .encode()
                .map_err(|_| PendingError::Malformed)?;
            let pinned = snapshot
                .get_pinned_cf_opt(facts, key.as_bytes(), read_options())
                .map_err(|_| PendingError::EngineRead)?
                .ok_or(PendingError::MissingFact)?;
            if pinned.len() < 34 || pinned[..2] != [1, 0x20] {
                return Err(PendingError::CorruptFact);
            }
            let bytes = &pinned[34..];
            remaining = remaining
                .checked_sub(bytes.len())
                .ok_or(PendingError::Limit)?;
            if bytes.len() > limits.fact.max_encoded_bytes {
                return Err(PendingError::Limit);
            }
            let payload_digest = Digest256::from_bytes(
                pinned[2..34]
                    .try_into()
                    .map_err(|_| PendingError::CorruptFact)?,
            );
            let fact =
                Fact::decode_preimage(bytes, payload_digest, limits.fact).map_err(|error| {
                    match error {
                        CodecError::InputLimit
                        | CodecError::AllocationLimit
                        | CodecError::TagLimit
                        | CodecError::StringLimit => PendingError::Limit,
                        CodecError::AllocationFailed => PendingError::Allocation,
                        CodecError::IncompatibleSchema => PendingError::IncompatibleSchema,
                        _ => PendingError::CorruptFact,
                    }
                })?;
            if fact.source_record_id() != source_record_id {
                return Err(PendingError::CorruptFact);
            }
            drop(fact);
            let mut encoded = Vec::new();
            encoded
                .try_reserve_exact(bytes.len())
                .map_err(|_| PendingError::Allocation)?;
            encoded.extend_from_slice(bytes);
            records.push(encoded);
            references.push(FactRef {
                source_record_id,
                payload_digest,
            });
        }
        check_cancel(cancelled)?;
        let batch = AdmissionBatch::new(references).map_err(|_| PendingError::IncompleteBatch)?;
        if batch.last() != header.last || batch.digest() != header.digest {
            return Err(PendingError::IncompleteBatch);
        }
        Ok(Some(PendingBatch {
            batch,
            records,
            _permit: permit,
        }))
    }
}

fn read_options() -> ReadOptions {
    let mut options = ReadOptions::default();
    options.set_verify_checksums(true);
    options.fill_cache(false);
    options
}
fn check_cancel(cancelled: &AtomicBool) -> Result<(), PendingError> {
    if cancelled.load(Ordering::Relaxed) {
        Err(PendingError::Cancelled)
    } else {
        Ok(())
    }
}
