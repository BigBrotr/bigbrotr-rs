//! Lossless signed fields. Deserialization does not establish cryptographic validity.
pub mod acquisition;
pub mod admission;
pub mod archive;
pub mod codec;
pub mod dictionary;
pub mod effect;
pub mod event;
pub mod evidence;
pub mod evidence_intent;
pub mod fact;
mod fingerprint;
pub mod identity;
pub mod measurement;
pub mod measurement_intent;
pub mod read;
pub mod receipt_codec;
pub mod scalar;
pub mod source;

pub use admission::{EventIntent, LocalObservation, ProducerRequestId, WitnessContext};
pub use archive::{ArchiveBatch, ArchiveRange};
pub use dictionary::{
    AccessClass, AccessScopeDescriptor, ClockSource, DatasetLineageDescriptor, DescriptorLabel,
    DictionaryDescriptor, DictionaryEntry, DirectRouteClass, DirectRouteDescriptor,
    EndpointDescriptor, ObserverDescriptor, RouteClass,
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
    AccessScopeId, AcquisitionScopeId, AllocationDomainId, ArtifactId, CommandKey, DatasetId,
    DestinationId, Digest256, EndpointId, EventId, JobId, MeasurementAttemptId, OwnerResourceId,
    PrincipalId, ProcessIncarnation, ProducerSlotId, ProjectionGenerationId, PublicKey,
    ReauthenticationId, RecoveryGenerationId, RouteId, SelectorId, Signature,
};
pub use measurement::{
    ConnectCallResult, ConnectDestination, ConnectErrorClass, FirstValidEvent, Measurement,
    MeasurementContext, MeasurementCounters, MeasurementFailure, MeasurementLimit,
    MeasurementMoment, MeasurementPhase, MeasurementState, MeasurementSubject,
    MeasurementTermination, ObservedAttempt, ObservedExecution, OpenObservation,
    PassiveReadProfile, ReadObservation, TeardownObservation, TerminalMeasurement,
};
pub use measurement_intent::MeasurementEvidenceIntent;
pub use scalar::{
    CompleteThrough, DurationUs, ObservedTime, Ordinal, OwnerEpoch, PolicyEpoch, ProducerEpoch,
    RequestSequence, Sequence, TopologyGeneration,
};
pub use source::{
    AdmissionBatch, DictionaryKind, FactRef, QualifiedRef, SourceCut, SourceRecordId, StreamBound,
    StreamId,
};
