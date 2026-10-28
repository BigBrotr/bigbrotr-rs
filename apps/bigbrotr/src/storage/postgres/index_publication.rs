//! Complete initial Index plans and PG publication coordination.
//!
//! These metadata values grant no native access. A plan is prepared before any
//! ClickHouse dispatch; only a verifier-owned physical proof can publish it.
//! An unknown insert leaves the entire batch prepared for identical replay.
use super::*;
use bigbrotr_model::{
    AdmissionBatch, Digest256, ProjectionGenerationId, SourceCut, SourceRecordId,
};

#[path = "index_publication/adapter.rs"]
mod adapter;
#[path = "index_publication/handoff.rs"]
mod handoff;
#[path = "index_publication/proofs.rs"]
mod proofs;
pub use handoff::{IndexHandoff, IndexHandoffReceipt, RetainedIndexRevision};
#[cfg(test)]
#[path = "index_publication/tests.rs"]
mod tests;

pub const INDEX_TARGET_COUNT: usize = 10;
pub const MAX_INDEX_STREAMS: usize = 1024;
pub const INDEX_SCHEMA_VERSION: u16 = 1;
pub const INDEX_PARTITION: &str = "all";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexError {
    InvalidInput,
    Conflict,
    Backend,
    IncompatibleDeployment,
    Unavailable,
    Exhausted,
    CommitUnknown,
}
impl fmt::Display for IndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Index publication failed: {self:?}")
    }
}
impl std::error::Error for IndexError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexProvenance {
    LocalFirstWitness,
    Off,
}
impl IndexProvenance {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LocalFirstWitness => "local_first_witness",
            Self::Off => "off",
        }
    }
}

/// Canonical initial physical targets. Every plan includes all ten, even empty.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum IndexTarget {
    Dictionaries,
    Events,
    EventEligibilityFacts,
    EventReceiveFacts,
    EventIdIndex,
    EventAuthorIndex,
    EventTimeIndex,
    EventTagIndex,
    ProvenanceByEvent,
    ProvenanceByRelay,
}
impl IndexTarget {
    pub const ALL: [Self; INDEX_TARGET_COUNT] = [
        Self::Dictionaries,
        Self::Events,
        Self::EventEligibilityFacts,
        Self::EventReceiveFacts,
        Self::EventIdIndex,
        Self::EventAuthorIndex,
        Self::EventTimeIndex,
        Self::EventTagIndex,
        Self::ProvenanceByEvent,
        Self::ProvenanceByRelay,
    ];
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Dictionaries => "dictionaries",
            Self::Events => "events",
            Self::EventEligibilityFacts => "event_eligibility_facts",
            Self::EventReceiveFacts => "event_receive_facts",
            Self::EventIdIndex => "event_id_index",
            Self::EventAuthorIndex => "event_author_index",
            Self::EventTimeIndex => "event_time_index",
            Self::EventTagIndex => "event_tag_index",
            Self::ProvenanceByEvent => "provenance_by_event",
            Self::ProvenanceByRelay => "provenance_by_relay",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexBinding {
    dataset: DatasetId,
    resource: OwnerResourceId,
    generation: ProjectionGenerationId,
    storage_incarnation: ProcessIncarnation,
    table_identity_digest: Digest256,
    custody_binding_digest: Digest256,
    provenance: IndexProvenance,
}
impl IndexBinding {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        dataset: DatasetId,
        resource: OwnerResourceId,
        generation: ProjectionGenerationId,
        storage_incarnation: ProcessIncarnation,
        table_identity_digest: Digest256,
        custody_binding_digest: Digest256,
        provenance: IndexProvenance,
    ) -> Result<Self, IndexError> {
        if table_identity_digest.as_bytes() == &[0; 32]
            || custody_binding_digest.as_bytes() == &[0; 32]
        {
            return Err(IndexError::InvalidInput);
        }
        Ok(Self {
            dataset,
            resource,
            generation,
            storage_incarnation,
            table_identity_digest,
            custody_binding_digest,
            provenance,
        })
    }
    pub const fn dataset(self) -> DatasetId {
        self.dataset
    }
    pub const fn resource(self) -> OwnerResourceId {
        self.resource
    }
    pub const fn generation(self) -> ProjectionGenerationId {
        self.generation
    }
    pub const fn storage_incarnation(self) -> ProcessIncarnation {
        self.storage_incarnation
    }
    pub const fn table_identity_digest(self) -> Digest256 {
        self.table_identity_digest
    }
    pub const fn custody_binding_digest(self) -> Digest256 {
        self.custody_binding_digest
    }
    pub const fn provenance(self) -> IndexProvenance {
        self.provenance
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexChunk {
    target: IndexTarget,
    schema_version: u16,
    partition: String,
    chunk_id: Digest256,
    row_count: u64,
    rows_digest: Digest256,
}
impl IndexChunk {
    pub fn new(
        target: IndexTarget,
        schema_version: u16,
        partition: String,
        chunk_id: Digest256,
        row_count: u64,
        rows_digest: Digest256,
    ) -> Result<Self, IndexError> {
        if schema_version != INDEX_SCHEMA_VERSION
            || partition != INDEX_PARTITION
            || chunk_id.as_bytes() == &[0; 32]
            || rows_digest.as_bytes() == &[0; 32]
        {
            return Err(IndexError::InvalidInput);
        }
        Ok(Self {
            target,
            schema_version,
            partition,
            chunk_id,
            row_count,
            rows_digest,
        })
    }
    pub const fn target(&self) -> IndexTarget {
        self.target
    }
    pub const fn schema_version(&self) -> u16 {
        self.schema_version
    }
    pub fn partition(&self) -> &str {
        &self.partition
    }
    pub const fn chunk_id(&self) -> Digest256 {
        self.chunk_id
    }
    pub const fn row_count(&self) -> u64 {
        self.row_count
    }
    pub const fn rows_digest(&self) -> Digest256 {
        self.rows_digest
    }
}

/// Fixed complete batch identity, never a claim of native commitment by itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexSourceBatch {
    first: SourceRecordId,
    last: SourceRecordId,
    digest: Digest256,
}
impl IndexSourceBatch {
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexPlan {
    binding: IndexBinding,
    expected_revision: u64,
    before: SourceCut,
    after: SourceCut,
    source: IndexSourceBatch,
    chunks: [IndexChunk; INDEX_TARGET_COUNT],
    digest: Digest256,
}
impl IndexPlan {
    /// Structural validation only. Preparation also requires the actual retained
    /// PendingBatch. This first adapter advances one whole batch in one retained
    /// topology; it does not manufacture a topology handoff from scalar maxima.
    pub fn new(
        binding: IndexBinding,
        expected_revision: u64,
        before: SourceCut,
        after: SourceCut,
        source: AdmissionBatch,
        chunks: [IndexChunk; INDEX_TARGET_COUNT],
    ) -> Result<Self, IndexError> {
        Self::from_metadata(
            binding,
            expected_revision,
            before,
            after,
            IndexSourceBatch::from_batch(&source),
            chunks,
        )
    }
    fn from_metadata(
        binding: IndexBinding,
        expected_revision: u64,
        before: SourceCut,
        after: SourceCut,
        source: IndexSourceBatch,
        chunks: [IndexChunk; INDEX_TARGET_COUNT],
    ) -> Result<Self, IndexError> {
        if source.first().stream() != source.last().stream()
            || source.first().dataset_id != source.last().dataset_id
            || source.first().sequence.get() > source.last().sequence.get()
            || expected_revision == u64::MAX
            || before.dataset_id() != binding.dataset
            || after.dataset_id() != binding.dataset
            || source.first().dataset_id != binding.dataset
            || before.topology_generation() != after.topology_generation()
            || before.bounds().is_empty()
            || before.bounds().len() > MAX_INDEX_STREAMS
            || before.bounds().len() != after.bounds().len()
            || !chunks.iter().map(IndexChunk::target).eq(IndexTarget::ALL)
            || (binding.provenance == IndexProvenance::Off
                && chunks[8..].iter().any(|chunk| chunk.row_count != 0))
        {
            return Err(IndexError::InvalidInput);
        }
        let mut advanced = false;
        for (old, new) in before.bounds().iter().zip(after.bounds()) {
            if old.stream != new.stream {
                return Err(IndexError::InvalidInput);
            }
            if old.stream == source.first().stream() {
                if old.last_complete_sequence.get().checked_add(1)
                    != Some(source.first().sequence.get())
                    || new.last_complete_sequence.get() != source.last().sequence.get()
                {
                    return Err(IndexError::InvalidInput);
                }
                advanced = true;
            } else if old != new {
                return Err(IndexError::InvalidInput);
            }
            if expected_revision == 0 && old.last_complete_sequence.get() != 0 {
                return Err(IndexError::InvalidInput);
            }
        }
        if !advanced
            || chunks.iter().enumerate().any(|(i, chunk)| {
                chunks[..i]
                    .iter()
                    .any(|previous| previous.chunk_id == chunk.chunk_id)
            })
        {
            return Err(IndexError::InvalidInput);
        }
        let mut value = Self {
            binding,
            expected_revision,
            before,
            after,
            source,
            chunks,
            digest: Digest256::from_bytes([0; 32]),
        };
        value.digest = value.metadata_digest();
        Ok(value)
    }
    pub const fn binding(&self) -> &IndexBinding {
        &self.binding
    }
    pub const fn expected_revision(&self) -> u64 {
        self.expected_revision
    }
    pub fn before(&self) -> &SourceCut {
        &self.before
    }
    pub fn after(&self) -> &SourceCut {
        &self.after
    }
    pub const fn source(&self) -> IndexSourceBatch {
        self.source
    }
    pub fn chunks(&self) -> &[IndexChunk; INDEX_TARGET_COUNT] {
        &self.chunks
    }
    pub const fn digest(&self) -> Digest256 {
        self.digest
    }

    // Versioned PG metadata encoding, independent of CH row encoding. Every
    // variable field is length-prefixed; cuts are ordered by SourceCut itself.
    fn metadata_digest(&self) -> Digest256 {
        let mut hash = Sha256::new();
        hash.update(b"bigbrotr/index-plan-metadata/1\0");
        hash.update(self.binding.dataset.as_bytes());
        hash.update(self.binding.resource.as_bytes());
        hash.update(self.binding.generation.as_bytes());
        hash.update(self.binding.storage_incarnation.as_bytes());
        hash.update(self.binding.table_identity_digest.as_bytes());
        hash.update(self.binding.custody_binding_digest.as_bytes());
        hash.update([match self.binding.provenance {
            IndexProvenance::LocalFirstWitness => 1,
            IndexProvenance::Off => 0,
        }]);
        hash.update(self.expected_revision.to_be_bytes());
        for cut in [&self.before, &self.after] {
            hash.update(cut.topology_generation().get().to_be_bytes());
            hash.update((cut.bounds().len() as u32).to_be_bytes());
            for bound in cut.bounds() {
                hash.update(bound.stream.shard_id.to_be_bytes());
                hash.update(bound.stream.owner_epoch.get().to_be_bytes());
                hash.update(bound.last_complete_sequence.get().to_be_bytes());
            }
        }
        hash.update(self.source.digest().as_bytes());
        hash.update(self.source.first().shard_id.to_be_bytes());
        hash.update(self.source.first().owner_epoch.get().to_be_bytes());
        hash.update(self.source.first().sequence.get().to_be_bytes());
        hash.update(self.source.last().sequence.get().to_be_bytes());
        for chunk in &self.chunks {
            hash.update([chunk.target as u8]);
            hash.update(chunk.schema_version.to_be_bytes());
            hash.update((chunk.partition.len() as u32).to_be_bytes());
            hash.update(chunk.partition.as_bytes());
            hash.update(chunk.chunk_id.as_bytes());
            hash.update(chunk.row_count.to_be_bytes());
            hash.update(chunk.rows_digest.as_bytes());
        }
        Digest256::from_bytes(hash.finalize().into())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexOwnerIdentity {
    pub resource: OwnerResourceId,
    pub epoch: OwnerEpoch,
    pub incarnation: ProcessIncarnation,
}

/// Exact finite recovery binding. The immutable publication chain fixes the
/// complete prefix; its physical verification belongs to VerifiedIndexRecovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexRecoveryState {
    pub published_revision: u64,
    pub published_plan: Option<Digest256>,
    pub published_handoff: Option<Digest256>,
    pub pending_plan: Option<Digest256>,
}
impl IndexRecoveryState {
    pub fn valid_head(self) -> bool {
        matches!(
            (
                self.published_revision,
                self.published_plan,
                self.published_handoff,
            ),
            (0, None, None) | (1.., Some(_), None) | (1.., None, Some(_))
        )
    }
}

/// Coordination identity, not a native capability. Constructors stay private.
#[derive(Debug)]
pub struct IndexSession {
    binding: IndexBinding,
    owner: IndexOwnerIdentity,
    issuer: ingest_registry::ControlOwner,
}
impl IndexSession {
    pub const fn binding(&self) -> &IndexBinding {
        &self.binding
    }
    pub const fn owner(&self) -> IndexOwnerIdentity {
        self.owner
    }
}

#[derive(Debug, Clone)]
pub struct PreparedIndexPlan {
    plan: IndexPlan,
}
impl PreparedIndexPlan {
    pub fn plan(&self) -> &IndexPlan {
        &self.plan
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexPublicationReceipt {
    pub binding: IndexBinding,
    pub plan_digest: Digest256,
    pub revision: u64,
    pub cut: SourceCut,
    pub family_counts: [u64; INDEX_TARGET_COUNT],
    pub publisher: IndexOwnerIdentity,
    pub engine_receipt_digest: Digest256,
    pub committed_at_us: i64,
    pub created: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetainedIndexPlan {
    pub plan: IndexPlan,
    pub publication: Option<IndexPublicationReceipt>,
}
