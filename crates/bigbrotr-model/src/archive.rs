//! Borrowed complete source membership for the archive fingerprint. Construction
//! validates structure, not retained admission receipts, topology or authority.
use crate::{AdmissionBatch, DatasetId, FactRef, Sequence, StreamId, TopologyGeneration};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveBatchError {
    Empty,
    ForeignDataset,
    UnorderedBatches,
    RecordCountOverflow,
}
impl fmt::Display for ArchiveBatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Empty => "archive batch must contain complete admission batches",
            Self::ForeignDataset => "archive batch contains a foreign dataset",
            Self::UnorderedBatches => "archive batches must be ordered and disjoint",
            Self::RecordCountOverflow => "archive record count exceeds u64",
        })
    }
}
impl std::error::Error for ArchiveBatchError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArchiveRange {
    pub stream: StreamId,
    pub first: Sequence,
    pub last: Sequence,
}

/// Borrows already bounded batches without copying references or fact bodies.
/// Callers resolve each complete admission receipt and the retained topology
/// before granting publication. A digest is never evidence of a catalog commit.
#[derive(Debug)]
pub struct ArchiveBatch<'a> {
    batches: &'a [AdmissionBatch],
    dataset_id: DatasetId,
    schema_version: u16,
    topology_generation: TopologyGeneration,
    record_count: u64,
    range_count: u64,
}
impl<'a> ArchiveBatch<'a> {
    pub fn new(
        schema_version: u16,
        topology_generation: TopologyGeneration,
        batches: &'a [AdmissionBatch],
    ) -> Result<Self, ArchiveBatchError> {
        let first = batches.first().ok_or(ArchiveBatchError::Empty)?.first();
        let mut previous = None;
        let mut record_count = 0u64;
        let mut range_count = 0;
        for batch in batches {
            let begin = batch.first();
            if begin.dataset_id != first.dataset_id {
                return Err(ArchiveBatchError::ForeignDataset);
            }
            if previous.is_some_and(|last| last >= begin) {
                return Err(ArchiveBatchError::UnorderedBatches);
            }
            if !previous.is_some_and(|last: crate::SourceRecordId| {
                last.stream() == begin.stream()
                    && last.sequence.get().checked_add(1) == Some(begin.sequence.get())
            }) {
                range_count += 1;
            }
            record_count = u64::try_from(batch.records().len())
                .ok()
                .and_then(|count| record_count.checked_add(count))
                .ok_or(ArchiveBatchError::RecordCountOverflow)?;
            previous = Some(batch.last());
        }
        Ok(Self {
            batches,
            dataset_id: first.dataset_id,
            schema_version,
            topology_generation,
            record_count,
            range_count,
        })
    }
    pub const fn dataset_id(&self) -> DatasetId {
        self.dataset_id
    }
    pub const fn schema_version(&self) -> u16 {
        self.schema_version
    }
    pub const fn topology_generation(&self) -> TopologyGeneration {
        self.topology_generation
    }
    pub const fn record_count(&self) -> u64 {
        self.record_count
    }
    pub const fn range_count(&self) -> u64 {
        self.range_count
    }
    pub fn records(&self) -> impl Iterator<Item = &FactRef> {
        self.batches.iter().flat_map(AdmissionBatch::records)
    }
    pub fn ranges(&self) -> impl Iterator<Item = ArchiveRange> + '_ {
        let mut remaining = self.batches;
        std::iter::from_fn(move || {
            let (batch, rest) = remaining.split_first()?;
            remaining = rest;
            let mut range = ArchiveRange {
                stream: batch.first().stream(),
                first: batch.first().sequence,
                last: batch.last().sequence,
            };
            // Maximal ranges remove admission grouping from the semantic digest.
            // Never wrap a maximum sequence into the next stream or into zero.
            while let Some((next, rest)) = remaining.split_first() {
                if next.first().stream() != range.stream
                    || range.last.get().checked_add(1) != Some(next.first().sequence.get())
                {
                    break;
                }
                range.last = next.last().sequence;
                remaining = rest;
            }
            Some(range)
        })
    }
}
