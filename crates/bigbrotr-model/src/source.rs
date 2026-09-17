use crate::{
    AllocationDomainId, CompleteThrough, DatasetId, Digest256, Ordinal, OwnerEpoch, Sequence,
    TopologyGeneration,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceError {
    UnknownDictionaryKind,
    WrongDictionaryKind,
    ForeignDataset,
    IncompatibleTopology,
    UnorderedStreams,
    UnknownStream,
    InvalidBatch,
}
impl fmt::Display for SourceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::UnknownDictionaryKind => "unassigned dictionary kind",
            Self::WrongDictionaryKind => "reference has the wrong dictionary kind",
            Self::ForeignDataset => "incompatible dataset namespace",
            Self::IncompatibleTopology => "cut does not name exactly the required topology streams",
            Self::UnorderedStreams => "source streams must be strictly ordered without duplicates",
            Self::UnknownStream => "source stream is absent from this cut",
            Self::InvalidBatch => {
                "admission batch must be nonempty and contiguous in one source stream"
            }
        })
    }
}
impl std::error::Error for SourceError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u16)]
pub enum DictionaryKind {
    Endpoint = 1,
    Observer = 2,
    AccessScope = 3,
    VisibilityScope = 4,
    Method = 5,
    Route = 6,
    HttpResource = 7,
    Event = 8,
    DatasetLineage = 9,
}
impl TryFrom<u16> for DictionaryKind {
    type Error = SourceError;
    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Endpoint),
            2 => Ok(Self::Observer),
            3 => Ok(Self::AccessScope),
            4 => Ok(Self::VisibilityScope),
            5 => Ok(Self::Method),
            6 => Ok(Self::Route),
            7 => Ok(Self::HttpResource),
            8 => Ok(Self::Event),
            9 => Ok(Self::DatasetLineage),
            _ => Err(SourceError::UnknownDictionaryKind),
        }
    }
}
impl Serialize for DictionaryKind {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u16(*self as u16)
    }
}
impl<'de> Deserialize<'de> for DictionaryKind {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::try_from(u16::deserialize(deserializer)?).map_err(de::Error::custom)
    }
}

/// A qualified name is not proof of allocation or authority. Owners verify the
/// exact durable mapping and callers' permitted scopes before using it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QualifiedRef {
    pub dataset_id: DatasetId,
    pub dictionary_kind: DictionaryKind,
    pub allocation_domain: AllocationDomainId,
    pub ordinal: Ordinal,
}
impl QualifiedRef {
    pub fn require(self, dataset: DatasetId, kind: DictionaryKind) -> Result<(), SourceError> {
        if self.dataset_id != dataset {
            return Err(SourceError::ForeignDataset);
        }
        if self.dictionary_kind != kind {
            return Err(SourceError::WrongDictionaryKind);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamId {
    pub shard_id: u32,
    pub owner_epoch: OwnerEpoch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceRecordId {
    pub dataset_id: DatasetId,
    pub shard_id: u32,
    pub owner_epoch: OwnerEpoch,
    pub sequence: Sequence,
}
impl SourceRecordId {
    pub const fn stream(self) -> StreamId {
        StreamId {
            shard_id: self.shard_id,
            owner_epoch: self.owner_epoch,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct StreamBound {
    pub stream: StreamId,
    pub last_complete_sequence: CompleteThrough,
}

/// Valid structural cut, not a storage certificate. The authority must also
/// resolve its retained topology, complete batch boundaries and handoff closure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SourceCut {
    dataset_id: DatasetId,
    topology_generation: TopologyGeneration,
    bounds: Vec<StreamBound>,
}
impl SourceCut {
    pub fn new(
        dataset_id: DatasetId,
        topology_generation: TopologyGeneration,
        bounds: Vec<StreamBound>,
    ) -> Result<Self, SourceError> {
        if bounds
            .windows(2)
            .any(|pair| pair[0].stream >= pair[1].stream)
        {
            return Err(SourceError::UnorderedStreams);
        }
        Ok(Self {
            dataset_id,
            topology_generation,
            bounds,
        })
    }
    pub const fn dataset_id(&self) -> DatasetId {
        self.dataset_id
    }
    pub const fn topology_generation(&self) -> TopologyGeneration {
        self.topology_generation
    }
    pub fn bounds(&self) -> &[StreamBound] {
        &self.bounds
    }

    pub fn validate_topology(
        &self,
        dataset: DatasetId,
        generation: TopologyGeneration,
        required_streams: &[StreamId],
    ) -> Result<(), SourceError> {
        if self.dataset_id != dataset {
            return Err(SourceError::ForeignDataset);
        }
        if self.topology_generation != generation
            || !self
                .bounds
                .iter()
                .map(|b| b.stream)
                .eq(required_streams.iter().copied())
        {
            return Err(SourceError::IncompatibleTopology);
        }
        Ok(())
    }
    pub fn contains(&self, id: SourceRecordId) -> Result<bool, SourceError> {
        if self.dataset_id != id.dataset_id {
            return Err(SourceError::ForeignDataset);
        }
        let position = self
            .bounds
            .binary_search_by_key(&id.stream(), |bound| bound.stream)
            .map_err(|_| SourceError::UnknownStream)?;
        Ok(id.sequence.get() <= self.bounds[position].last_complete_sequence.get())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FactRef {
    pub source_record_id: SourceRecordId,
    pub payload_digest: Digest256,
}

/// Ordered semantic membership; construction does not claim that a batch was
/// committed. The storage owner binds this value into its durable receipt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionBatch {
    records: Vec<FactRef>,
}
impl AdmissionBatch {
    pub fn new(records: Vec<FactRef>) -> Result<Self, SourceError> {
        let first = records
            .first()
            .ok_or(SourceError::InvalidBatch)?
            .source_record_id;
        for (offset, record) in records.iter().enumerate() {
            let sequence = u64::try_from(offset)
                .ok()
                .and_then(|n| first.sequence.get().checked_add(n));
            if record.source_record_id.dataset_id != first.dataset_id
                || record.source_record_id.stream() != first.stream()
                || Some(record.source_record_id.sequence.get()) != sequence
            {
                return Err(SourceError::InvalidBatch);
            }
        }
        Ok(Self { records })
    }
    pub fn records(&self) -> &[FactRef] {
        &self.records
    }
    pub fn first(&self) -> SourceRecordId {
        self.records[0].source_record_id
    }
    pub fn last(&self) -> SourceRecordId {
        self.records[self.records.len() - 1].source_record_id
    }
}
