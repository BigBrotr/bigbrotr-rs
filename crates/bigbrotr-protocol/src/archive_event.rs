//! Private historical reference/ID reads, not scope grants or source-cut proofs.
use bigbrotr_model::{
    DatasetId, DictionaryKind, Digest256, EventId, FactRef, QualifiedRef, SignedEvent,
    SourceRecordId, TopologyGeneration,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
pub const QUERY_FORMAT: &str = "bigbrotr-native-event-query/1";
/// Private paired-worker exit for a known finite read refusal. It carries no
/// observation; the parent requires positive reap and empty stdout.
pub const RESOURCE_LIMIT_EXIT_CODE: u8 = 75;
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
/// Private lookup within one already authorized scope and retained source cut.
/// These caller-declared values are not authorization or a certified frontier.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventQuery {
    pub dataset: DatasetId,
    pub event_id: EventId,
    pub scope: QualifiedRef,
    pub topology_generation: TopologyGeneration,
    pub bounds: Vec<crate::archive_cut::CutBound>,
    pub snapshot_id: u64,
}

impl EventQuery {
    /// Correlates a completed observation, including absence, to every selection
    /// field. It is neither principal authority nor a certified source cut.
    pub fn digest(&self) -> Digest256 {
        let mut hash = Sha256::new();
        hash.update(b"bigbrotr/archive-event-query/1\0");
        hash.update(self.dataset.as_bytes());
        hash.update(self.event_id.as_bytes());
        hash.update(self.scope.dataset_id.as_bytes());
        hash.update((self.scope.dictionary_kind as u16).to_be_bytes());
        hash.update(self.scope.allocation_domain.as_bytes());
        hash.update(self.scope.ordinal.get().to_be_bytes());
        hash.update(self.topology_generation.get().to_be_bytes());
        hash.update(self.snapshot_id.to_be_bytes());
        hash.update((self.bounds.len() as u64).to_be_bytes());
        for bound in &self.bounds {
            hash.update(bound.shard_id.to_be_bytes());
            hash.update(bound.owner_epoch.to_be_bytes());
            hash.update(bound.complete_through.to_be_bytes());
        }
        Digest256::from_bytes(hash.finalize().into())
    }
    pub fn validate(&self, max_bytes: usize) -> Result<(), EventReadError> {
        if max_bytes == 0
            || max_bytes > MAX_EVENT_ROW_BYTES
            || self.bounds.len() > crate::archive_receipt::MAX_RANGES
        {
            return Err(EventReadError::ResourceLimit);
        }
        if self
            .scope
            .require(self.dataset, DictionaryKind::VisibilityScope)
            .is_err()
            || self.snapshot_id > i64::MAX as u64
            || self.bounds.iter().any(|b| b.owner_epoch == 0)
            || self
                .bounds
                .windows(2)
                .any(|b| b[0].stream() >= b[1].stream())
        {
            return Err(EventReadError::InvalidInput);
        }
        Ok(())
    }

    /// A missing stream and an explicit zero prefix both exclude every record.
    /// Call validate first: the private wire requires strictly ordered bounds.
    pub fn contains(&self, source: SourceRecordId) -> bool {
        source.dataset_id == self.dataset
            && self
                .bounds
                .binary_search_by_key(&(source.shard_id, source.owner_epoch.get()), |b| b.stream())
                .is_ok_and(|i| source.sequence.get() <= self.bounds[i].complete_through)
    }
}

/// Private selection evidence; never expose these internal references as public
/// event attribution. The parent checks both against its retained cut again.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventMatch {
    pub snapshot_id: u64,
    pub event: SignedEvent,
    pub payload: FactRef,
    pub eligibility: FactRef,
}
/// Emitted only after the native read transaction successfully completes.
/// Absent means no eligibility for this exact selection; a missing payload after
/// selecting eligibility is an error, never a successful empty observation.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventQueryObservation {
    pub format: String,
    pub snapshot_id: u64,
    pub request_digest: Digest256,
    pub result: EventSelection,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum EventSelection {
    Absent {},
    Present { matched: Box<EventMatch> },
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive_cut::CutBound;
    use bigbrotr_model::{Ordinal, OwnerEpoch, Sequence};

    fn query() -> EventQuery {
        let dataset = [1; 16].try_into().unwrap();
        EventQuery {
            dataset,
            event_id: EventId::from_bytes([2; 32]),
            scope: QualifiedRef {
                dataset_id: dataset,
                dictionary_kind: DictionaryKind::VisibilityScope,
                allocation_domain: [3; 16].try_into().unwrap(),
                ordinal: Ordinal::new(1).unwrap(),
            },
            topology_generation: TopologyGeneration::new(u64::MAX),
            bounds: vec![
                CutBound {
                    shard_id: 0,
                    owner_epoch: 1,
                    complete_through: 0,
                },
                CutBound {
                    shard_id: u32::MAX,
                    owner_epoch: u64::MAX,
                    complete_through: u64::MAX,
                },
            ],
            snapshot_id: i64::MAX as u64,
        }
    }

    #[test]
    fn event_query_cut_is_ordered_bounded_and_cannot_implicitly_inherit_a_stream() {
        let read = query();
        assert_eq!(read.validate(MAX_EVENT_ROW_BYTES), Ok(()));
        let source = SourceRecordId {
            dataset_id: read.dataset,
            shard_id: u32::MAX,
            owner_epoch: OwnerEpoch::new(u64::MAX).unwrap(),
            sequence: Sequence::new(u64::MAX).unwrap(),
        };
        assert!(read.contains(source));
        assert!(!read.contains(SourceRecordId {
            owner_epoch: OwnerEpoch::new(1).unwrap(),
            ..source
        }));
        assert!(!read.contains(SourceRecordId {
            dataset_id: [4; 16].try_into().unwrap(),
            ..source
        }));
        assert!(!read.contains(SourceRecordId {
            shard_id: 0,
            owner_epoch: OwnerEpoch::new(1).unwrap(),
            sequence: Sequence::new(1).unwrap(),
            ..source
        }));
        for variant in [
            "order",
            "duplicate",
            "epoch",
            "snapshot",
            "scope-kind",
            "scope-dataset",
        ] {
            let mut invalid = read.clone();
            match variant {
                "order" => invalid.bounds.reverse(),
                "duplicate" => invalid.bounds.push(invalid.bounds[1]),
                "epoch" => invalid.bounds[0].owner_epoch = 0,
                "snapshot" => invalid.snapshot_id = i64::MAX as u64 + 1,
                "scope-kind" => invalid.scope.dictionary_kind = DictionaryKind::Event,
                _ => invalid.scope.dataset_id = [4; 16].try_into().unwrap(),
            }
            assert_eq!(
                invalid.validate(4096),
                Err(EventReadError::InvalidInput),
                "{variant}"
            );
        }
        for maximum in [0, MAX_EVENT_ROW_BYTES + 1] {
            assert_eq!(read.validate(maximum), Err(EventReadError::ResourceLimit));
        }
        let mut large = read.clone();
        large.bounds = (0..=crate::archive_receipt::MAX_RANGES)
            .map(|shard| CutBound {
                shard_id: shard as u32,
                owner_epoch: 1,
                complete_through: 0,
            })
            .collect();
        assert_eq!(large.validate(4096), Err(EventReadError::ResourceLimit));
        large.bounds.pop();
        assert_eq!(large.validate(4096), Ok(()));
        large.bounds.clear();
        assert_eq!(large.validate(4096), Ok(()));
        assert!(!large.contains(source));
    }

    #[test]
    fn event_query_wire_requires_the_cut_and_exact_snapshot_and_is_closed() {
        let value = serde_json::to_value(query()).unwrap();
        assert_eq!(
            serde_json::from_value::<EventQuery>(value.clone()).unwrap(),
            query()
        );
        for missing in [
            "dataset",
            "event_id",
            "scope",
            "topology_generation",
            "bounds",
            "snapshot_id",
        ] {
            let mut invalid = value.clone();
            invalid.as_object_mut().unwrap().remove(missing);
            assert!(
                serde_json::from_value::<EventQuery>(invalid).is_err(),
                "{missing}"
            );
        }
        let mut invalid = value;
        invalid["principal"] = serde_json::json!("owner");
        assert!(serde_json::from_value::<EventQuery>(invalid).is_err());
    }
}
