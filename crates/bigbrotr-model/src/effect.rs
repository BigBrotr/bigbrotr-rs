//! Structural retained-effect values. Only the storage owner can establish that
//! their named facts are committed, sufficient, protected and currently usable.
use crate::{
    AdmissionBatch, DatasetId, Digest256, FactRef, PolicyEpoch, ProducerRequestId,
    RecoveryGenerationId, TopologyGeneration,
};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EffectError {
    EmptyDependencies,
    UnorderedDependencies,
    ForeignDataset,
    MissingBatchDependency,
    ConflictingDependency,
    FutureDependency,
}
impl fmt::Display for EffectError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::EmptyDependencies => "a retained effect requires source dependencies",
            Self::UnorderedDependencies => "dependencies must be strictly ordered by source ID",
            Self::ForeignDataset => "effect context and dependencies require the same dataset",
            Self::MissingBatchDependency => "new batch record is absent from dependency closure",
            Self::ConflictingDependency => "dependency digest differs from the new batch record",
            Self::FutureDependency => "dependency follows the new batch in its own source stream",
        })
    }
}
impl std::error::Error for EffectError {}

/// No constructor sorts or removes duplicates: conflicting digests must not be
/// hidden by normalization. Input allocation/work is bounded by the caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceDependencies {
    dataset_id: DatasetId,
    records: Vec<FactRef>,
}
impl SourceDependencies {
    pub fn new(dataset_id: DatasetId, records: Vec<FactRef>) -> Result<Self, EffectError> {
        if records.is_empty() {
            return Err(EffectError::EmptyDependencies);
        }
        if records
            .iter()
            .any(|r| r.source_record_id.dataset_id != dataset_id)
        {
            return Err(EffectError::ForeignDataset);
        }
        if records
            .windows(2)
            .any(|r| r[0].source_record_id >= r[1].source_record_id)
        {
            return Err(EffectError::UnorderedDependencies);
        }
        Ok(Self {
            dataset_id,
            records,
        })
    }
    pub const fn dataset_id(&self) -> DatasetId {
        self.dataset_id
    }
    pub fn records(&self) -> &[FactRef] {
        &self.records
    }

    fn covers(&self, batch: &AdmissionBatch) -> Result<(), EffectError> {
        let last = batch.last();
        if self.records.iter().any(|r| {
            r.source_record_id.stream() == last.stream()
                && r.source_record_id.sequence > last.sequence
        }) {
            return Err(EffectError::FutureDependency);
        }
        // Both inputs are ordered. Consume once without cloning or an auxiliary
        // set; the closure may include older facts in other streams/scopes.
        let mut dependencies = self.records.iter().peekable();
        for record in batch.records() {
            while dependencies
                .peek()
                .is_some_and(|r| r.source_record_id < record.source_record_id)
            {
                dependencies.next();
            }
            match dependencies.next() {
                Some(found) if found.source_record_id == record.source_record_id => {
                    if found.payload_digest != record.payload_digest {
                        return Err(EffectError::ConflictingDependency);
                    }
                }
                _ => return Err(EffectError::MissingBatchDependency),
            }
        }
        Ok(())
    }
}

/// Supported operational spelling. Selecting this value is not qualification of
/// any host/filesystem; off-host classes require their own implemented contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProtectionClass {
    #[serde(rename = "local-synced")]
    LocalSynced,
}
impl ProtectionClass {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LocalSynced => "local-synced",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProtectionContext {
    pub dataset_id: DatasetId,
    pub topology_generation: TopologyGeneration,
    pub protection_class: ProtectionClass,
    pub recovery_generation: RecoveryGenerationId,
    pub policy_epoch: PolicyEpoch,
}
impl ProtectionContext {
    fn validate(self, dependencies: &SourceDependencies) -> Result<(), EffectError> {
        if self.dataset_id != dependencies.dataset_id {
            return Err(EffectError::ForeignDataset);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExistingProof {
    context: ProtectionContext,
    intent_digest: Digest256,
    dependencies: SourceDependencies,
}
impl ExistingProof {
    pub fn new(
        context: ProtectionContext,
        intent_digest: Digest256,
        dependencies: SourceDependencies,
    ) -> Result<Self, EffectError> {
        context.validate(&dependencies)?;
        Ok(Self {
            context,
            intent_digest,
            dependencies,
        })
    }
    pub const fn protection_context(&self) -> &ProtectionContext {
        &self.context
    }
    pub const fn intent_digest(&self) -> Digest256 {
        self.intent_digest
    }
    pub const fn dependencies(&self) -> &SourceDependencies {
        &self.dependencies
    }
}

/// A structural mutation outcome, never a verified constructor for durability.
/// Even AlreadyDurable binds this mutation request; certification has no request.
/// The optional batch is Some exactly for NewFacts and binds endpoints/membership
/// through AdmissionBatch::digest without retaining a second mutable digest copy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Receipt {
    request_id: ProducerRequestId,
    request_digest: Digest256,
    context: ProtectionContext,
    batch: Option<AdmissionBatch>,
    dependencies: SourceDependencies,
}
impl Receipt {
    pub fn new_facts(
        request_id: ProducerRequestId,
        request_digest: Digest256,
        context: ProtectionContext,
        batch: AdmissionBatch,
        dependencies: SourceDependencies,
    ) -> Result<Self, EffectError> {
        context.validate(&dependencies)?;
        if batch.first().dataset_id != context.dataset_id {
            return Err(EffectError::ForeignDataset);
        }
        dependencies.covers(&batch)?;
        Ok(Self {
            request_id,
            request_digest,
            context,
            batch: Some(batch),
            dependencies,
        })
    }
    pub fn already_durable(
        request_id: ProducerRequestId,
        request_digest: Digest256,
        context: ProtectionContext,
        dependencies: SourceDependencies,
    ) -> Result<Self, EffectError> {
        context.validate(&dependencies)?;
        Ok(Self {
            request_id,
            request_digest,
            context,
            batch: None,
            dependencies,
        })
    }
    pub const fn request_id(&self) -> ProducerRequestId {
        self.request_id
    }
    pub const fn request_digest(&self) -> Digest256 {
        self.request_digest
    }
    pub const fn protection_context(&self) -> &ProtectionContext {
        &self.context
    }
    pub const fn batch(&self) -> Option<&AdmissionBatch> {
        self.batch.as_ref()
    }
    pub const fn dependencies(&self) -> &SourceDependencies {
        &self.dependencies
    }
}

/// Retained acquisition progress basis, not a public artifact or new fact family.
/// Producers must resolve an uncertain Admit before attempting certification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventEffectBasis {
    Receipt(Receipt),
    ExistingProof(ExistingProof),
}
impl EventEffectBasis {
    pub const fn request_id(&self) -> Option<ProducerRequestId> {
        match self {
            Self::Receipt(r) => Some(r.request_id()),
            Self::ExistingProof(_) => None,
        }
    }
    pub const fn dependencies(&self) -> &SourceDependencies {
        match self {
            Self::Receipt(r) => r.dependencies(),
            Self::ExistingProof(p) => p.dependencies(),
        }
    }
    pub const fn protection_context(&self) -> &ProtectionContext {
        match self {
            Self::Receipt(r) => r.protection_context(),
            Self::ExistingProof(p) => p.protection_context(),
        }
    }
}
