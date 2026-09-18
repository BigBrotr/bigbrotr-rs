//! Lossless signed fields. Deserialization does not establish cryptographic validity.
pub mod admission;
pub mod codec;
pub mod dictionary;
pub mod effect;
pub mod event;
pub mod evidence;
pub mod evidence_intent;
pub mod fact;
mod fingerprint;
pub mod identity;
pub mod receipt_codec;
pub mod scalar;
pub mod source;

pub use admission::{EventIntent, LocalObservation, ProducerRequestId, WitnessContext};
pub use dictionary::{
    AccessClass, AccessScopeDescriptor, ClockSource, DatasetLineageDescriptor, DescriptorLabel,
    DictionaryDescriptor, DictionaryEntry, EndpointDescriptor, ObserverDescriptor, RouteClass,
};
pub use effect::{
    EventEffectBasis, ExistingProof, ProtectionClass, ProtectionContext, Receipt,
    SourceDependencies,
};
pub use event::{EventBody, NostrTime, SignedEvent};
pub use evidence::ClockQuality;
pub use evidence_intent::{EvidenceIntent, EvidenceIntentError, EvidenceIntentLimits};
pub use fact::{
    EventEligibility, EventOrigin, EventPayload, EventReceive, Fact, FactPayload, Provenance,
};
pub use identity::{
    AllocationDomainId, DatasetId, Digest256, EventId, OwnerResourceId, ProcessIncarnation,
    ProducerSlotId, ProjectionGenerationId, PublicKey, RecoveryGenerationId, Signature,
};
pub use scalar::{
    CompleteThrough, DurationUs, ObservedTime, Ordinal, OwnerEpoch, PolicyEpoch, ProducerEpoch,
    RequestSequence, Sequence, TopologyGeneration,
};
pub use source::{
    AdmissionBatch, DictionaryKind, FactRef, QualifiedRef, SourceCut, SourceRecordId, StreamBound,
    StreamId,
};
