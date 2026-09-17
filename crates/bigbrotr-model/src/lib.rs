//! Lossless signed fields. Deserialization does not establish cryptographic validity.
pub mod event;
pub mod evidence;
mod fingerprint;
pub mod identity;
pub mod scalar;
pub mod source;

pub use event::{NostrTime, SignedEvent};
pub use evidence::ClockQuality;
pub use identity::{
    AllocationDomainId, DatasetId, Digest256, EventId, ProducerSlotId, ProjectionGenerationId,
    PublicKey, Signature,
};
pub use scalar::{
    CompleteThrough, DurationUs, ObservedTime, Ordinal, OwnerEpoch, ProducerEpoch, RequestSequence,
    Sequence, TopologyGeneration,
};
pub use source::{
    AdmissionBatch, DictionaryKind, FactRef, QualifiedRef, SourceCut, SourceRecordId, StreamBound,
    StreamId,
};
