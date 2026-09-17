//! Lossless signed fields. Deserialization does not establish cryptographic validity.
pub mod admission;
pub mod codec;
pub mod event;
pub mod evidence;
pub mod fact;
mod fingerprint;
pub mod identity;
pub mod scalar;
pub mod source;

pub use admission::{EventIntent, LocalObservation, ProducerRequestId, WitnessContext};
pub use event::{EventBody, NostrTime, SignedEvent};
pub use evidence::ClockQuality;
pub use fact::{
    EventEligibility, EventOrigin, EventPayload, EventReceive, Fact, FactPayload, Provenance,
};
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
