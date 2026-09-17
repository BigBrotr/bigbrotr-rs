//! Immutable, structurally validated core facts. Constructors establish neither
//! cryptographic validity nor allocation, policy, correction or commit authority.
use crate::{
    ClockQuality, DatasetId, DictionaryKind, Digest256, EventBody, EventId, FactRef, ObservedTime,
    QualifiedRef, Signature, SourceRecordId, source::SourceError,
};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FactError {
    Reference(SourceError),
    InvalidCorrectionOrder,
    ProvenanceContext,
}
impl fmt::Display for FactError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Reference(error) => error.fmt(f),
            Self::InvalidCorrectionOrder => {
                f.write_str("a correction must follow its target in the same source stream")
            }
            Self::ProvenanceContext => {
                f.write_str("network witness context must match the current provenance policy")
            }
        }
    }
}
impl std::error::Error for FactError {}
impl From<SourceError> for FactError {
    fn from(value: SourceError) -> Self {
        Self::Reference(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventOrigin {
    Network,
    Import(QualifiedRef),
}
impl EventOrigin {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Network => "network",
            Self::Import(_) => "import",
        }
    }
    pub const fn import_lineage(self) -> Option<QualifiedRef> {
        match self {
            Self::Network => None,
            Self::Import(lineage) => Some(lineage),
        }
    }
    pub(crate) fn validate(self, dataset: DatasetId) -> Result<(), FactError> {
        if let Some(lineage) = self.import_lineage() {
            lineage.require(dataset, DictionaryKind::DatasetLineage)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventPayload {
    pub body: EventBody,
    pub event_ordinal: QualifiedRef,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventEligibility {
    pub event_id: EventId,
    pub body_digest: Digest256,
    pub sig: Signature,
    pub first_eligible_at: ObservedTime,
    pub origin: EventOrigin,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventReceive {
    pub event_id: EventId,
    pub received_at: ObservedTime,
    pub observer_id: QualifiedRef,
    pub clock_quality: ClockQuality,
    pub corrects: Option<SourceRecordId>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Provenance {
    pub event_id: EventId,
    pub endpoint_id: QualifiedRef,
    pub observer_id: QualifiedRef,
    pub access_scope_id: QualifiedRef,
    pub received_at: ObservedTime,
    pub clock_quality: ClockQuality,
    pub verification_method_id: QualifiedRef,
    pub corrects: Option<SourceRecordId>,
}

/// Only implemented families are constructible; unknown schemas are never
/// represented as an untyped JSON payload or silently coerced to another kind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FactPayload {
    EventPayload(EventPayload),
    EventEligibility(EventEligibility),
    EventReceive(EventReceive),
    Provenance(Provenance),
}
impl FactPayload {
    pub const fn record_kind(&self) -> &'static str {
        match self {
            Self::EventPayload(_) => "event-payload",
            Self::EventEligibility(_) => "event-eligibility",
            Self::EventReceive(_) => "event-receive",
            Self::Provenance(_) => "provenance",
        }
    }
    pub const fn event_id(&self) -> EventId {
        match self {
            Self::EventPayload(value) => value.body.event_id,
            Self::EventEligibility(value) => value.event_id,
            Self::EventReceive(value) => value.event_id,
            Self::Provenance(value) => value.event_id,
        }
    }
    pub const fn corrects(&self) -> Option<SourceRecordId> {
        match self {
            Self::EventReceive(value) => value.corrects,
            Self::Provenance(value) => value.corrects,
            _ => None,
        }
    }
    // Each position carries its expected dictionary kind; retaining the pair
    // prevents a same-dataset endpoint being accepted as a clock or observer.
    fn references(&self) -> [Option<(QualifiedRef, DictionaryKind)>; 5] {
        use DictionaryKind as K;
        match self {
            Self::EventPayload(value) => [
                Some((value.event_ordinal, K::Event)),
                None,
                None,
                None,
                None,
            ],
            Self::EventEligibility(value) => [
                value
                    .origin
                    .import_lineage()
                    .map(|r| (r, K::DatasetLineage)),
                None,
                None,
                None,
                None,
            ],
            Self::EventReceive(value) => [
                Some((value.observer_id, K::Observer)),
                Some((value.clock_quality.clock_id(), K::Observer)),
                None,
                None,
                None,
            ],
            Self::Provenance(value) => [
                Some((value.endpoint_id, K::Endpoint)),
                Some((value.observer_id, K::Observer)),
                Some((value.access_scope_id, K::AccessScope)),
                Some((value.clock_quality.clock_id(), K::Observer)),
                Some((value.verification_method_id, K::Method)),
            ],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fact {
    source_record_id: SourceRecordId,
    visibility_scope_id: QualifiedRef,
    recorded_at: ObservedTime,
    payload: FactPayload,
}
impl Fact {
    pub fn new(
        source_record_id: SourceRecordId,
        visibility_scope_id: QualifiedRef,
        recorded_at: ObservedTime,
        payload: FactPayload,
    ) -> Result<Self, FactError> {
        let dataset = source_record_id.dataset_id;
        visibility_scope_id.require(dataset, DictionaryKind::VisibilityScope)?;
        for (reference, kind) in payload.references().into_iter().flatten() {
            reference.require(dataset, kind)?;
        }
        if let Some(corrects) = payload.corrects() {
            if corrects.dataset_id != dataset {
                return Err(SourceError::ForeignDataset.into());
            }
            if corrects.stream() == source_record_id.stream()
                && corrects.sequence >= source_record_id.sequence
            {
                return Err(FactError::InvalidCorrectionOrder);
            }
        }
        Ok(Self {
            source_record_id,
            visibility_scope_id,
            recorded_at,
            payload,
        })
    }
    pub const fn source_record_id(&self) -> SourceRecordId {
        self.source_record_id
    }
    pub const fn visibility_scope_id(&self) -> QualifiedRef {
        self.visibility_scope_id
    }
    pub const fn recorded_at(&self) -> ObservedTime {
        self.recorded_at
    }
    pub const fn payload(&self) -> &FactPayload {
        &self.payload
    }
    pub const fn record_kind(&self) -> &'static str {
        self.payload.record_kind()
    }
    pub const fn record_schema_version(&self) -> u16 {
        1
    }
    pub const fn corrects(&self) -> Option<SourceRecordId> {
        self.payload.corrects()
    }
    /// Field occurrences, including visibility; repeated references are retained.
    /// A dependency-set owner must deduplicate before persisting its closure.
    pub fn dictionary_refs(&self) -> impl Iterator<Item = QualifiedRef> {
        std::iter::once(self.visibility_scope_id).chain(
            self.payload
                .references()
                .into_iter()
                .flatten()
                .map(|(reference, _)| reference),
        )
    }
    pub fn fact_ref(&self) -> FactRef {
        FactRef {
            source_record_id: self.source_record_id,
            payload_digest: self.payload_digest(),
        }
    }
}
