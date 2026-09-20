//! Private exact-reference historical read, not a scope grant or source-cut proof.
use bigbrotr_model::{DatasetId, DictionaryKind, EventId, FactRef, QualifiedRef, SignedEvent};
use serde::{Deserialize, Serialize};
pub const MAX_EVENT_ROW_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_EVENT_RESPONSE_BYTES: usize = MAX_EVENT_ROW_BYTES + 1024;
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventRead {
    pub dataset: DatasetId,
    pub event_id: EventId,
    pub scope: QualifiedRef,
    pub payload: FactRef,
    pub eligibility: FactRef,
    /// Exact retained native snapshot, when selected by the caller's pin.
    /// None preserves the private fresh-view observer; this field is not a pin.
    #[serde(default)]
    pub snapshot_id: Option<u64>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventReadError {
    InvalidInput,
    Missing,
    Mismatch,
    ResourceLimit,
    Native,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventObservation {
    pub snapshot_id: u64,
    pub event: SignedEvent,
}

impl EventRead {
    pub fn validate(&self, max_bytes: usize) -> Result<(), EventReadError> {
        if max_bytes == 0 || max_bytes > MAX_EVENT_ROW_BYTES {
            return Err(EventReadError::ResourceLimit);
        }
        if self
            .scope
            .require(self.dataset, DictionaryKind::VisibilityScope)
            .is_err()
            || self.payload.source_record_id.dataset_id != self.dataset
            || self.eligibility.source_record_id.dataset_id != self.dataset
            || self.payload.source_record_id == self.eligibility.source_record_id
            // Native SNAPSHOT_VERSION is BIGINT, unlike unsigned source cuts.
            || self.snapshot_id.is_some_and(|id| id > i64::MAX as u64)
        {
            return Err(EventReadError::InvalidInput);
        }
        Ok(())
    }
}

impl std::fmt::Display for EventReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "private archive event read failed: {self:?}")
    }
}
impl std::error::Error for EventReadError {}
