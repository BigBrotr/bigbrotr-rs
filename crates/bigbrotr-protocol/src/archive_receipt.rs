//! Private expected receipt identity, not publication/replay/protection authority.
use bigbrotr_model::{ArchiveBatch, DatasetId, Digest256, TopologyGeneration};
use serde::{Deserialize, Serialize};

pub mod group;

pub const MAX_INPUT_BYTES: usize = 32768;
/// Bounds owned range allocation separately from the smaller encoded-input cap.
pub const MAX_RANGES: usize = 1024;
/// Private normalized row bound, shared with the parent's allocation admission.
pub const MAX_ROW_BYTES: usize = 256 * 1024;
/// Maximum native vector size for the pinned DuckDB receipt stream.
pub const MAX_CHUNK_ROWS: usize = 2048;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReceiptRange {
    pub shard_id: u32,
    pub owner_epoch: u64,
    pub first_sequence: u64,
    pub last_sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReceiptExpectation {
    pub dataset_id: DatasetId,
    pub schema_version: u16,
    pub topology_generation: TopologyGeneration,
    pub batch_digest: Digest256,
    pub record_count: u64,
    pub input_ranges: Vec<ReceiptRange>,
    pub attempt_id: [u8; 16],
    pub publication_ordinal: u64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpectationError {
    InvalidIdentity,
    ResourceLimit,
}
impl ReceiptExpectation {
    /// Caller supplies the original retained attempt, never an invented retry ID.
    /// This constructs expected bytes; it does not inspect a committed receipt.
    pub fn from_archive(
        batch: &ArchiveBatch<'_>,
        attempt_id: [u8; 16],
        publication_ordinal: u64,
    ) -> Result<Self, ExpectationError> {
        if batch.range_count() > MAX_RANGES as u64 {
            return Err(ExpectationError::ResourceLimit);
        }
        let result = Self {
            dataset_id: batch.dataset_id(),
            schema_version: batch.schema_version(),
            topology_generation: batch.topology_generation(),
            batch_digest: batch.digest(),
            record_count: batch.record_count(),
            input_ranges: batch
                .ranges()
                .map(|r| ReceiptRange {
                    shard_id: r.stream.shard_id,
                    owner_epoch: r.stream.owner_epoch.get(),
                    first_sequence: r.first.get(),
                    last_sequence: r.last.get(),
                })
                .collect(),
            attempt_id,
            publication_ordinal,
        };
        result.validate()?;
        Ok(result)
    }
    pub fn validate(&self) -> Result<(), ExpectationError> {
        if self.input_ranges.len() > MAX_RANGES {
            return Err(ExpectationError::ResourceLimit);
        }
        if self.schema_version != 1
            || self.record_count == 0
            || self.input_ranges.is_empty()
            || self.attempt_id == [0; 16]
            || self.publication_ordinal == 0
        {
            return Err(ExpectationError::InvalidIdentity);
        }
        let mut previous: Option<ReceiptRange> = None;
        let mut count = 0u64;
        for &range in &self.input_ranges {
            if range.owner_epoch == 0
                || range.first_sequence == 0
                || range.last_sequence < range.first_sequence
            {
                return Err(ExpectationError::InvalidIdentity);
            }
            if let Some(prior) = previous {
                let stream = (range.shard_id, range.owner_epoch);
                let prior_stream = (prior.shard_id, prior.owner_epoch);
                if stream < prior_stream
                    || (stream == prior_stream
                        && (range.first_sequence <= prior.last_sequence
                            || prior.last_sequence.checked_add(1) == Some(range.first_sequence)))
                {
                    return Err(ExpectationError::InvalidIdentity);
                }
            }
            count = count
                .checked_add(range.last_sequence - range.first_sequence + 1)
                .ok_or(ExpectationError::InvalidIdentity)?;
            previous = Some(range);
        }
        if count != self.record_count {
            return Err(ExpectationError::InvalidIdentity);
        }
        Ok(())
    }
}
