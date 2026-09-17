//! Bound producer intent. Transport authority, verification and durable outcomes
//! are deliberately not supplied by constructing one of these values.
use crate::{
    ClockQuality, DatasetId, DictionaryKind, EventOrigin, ObservedTime, ProducerEpoch,
    ProducerSlotId, QualifiedRef, RequestSequence, SignedEvent, fact::FactError,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProducerRequestId {
    pub producer_slot_id: ProducerSlotId,
    pub producer_epoch: ProducerEpoch,
    pub request_sequence: RequestSequence,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WitnessContext {
    pub endpoint_id: QualifiedRef,
    pub access_scope_id: QualifiedRef,
    pub verification_method_id: QualifiedRef,
}
impl WitnessContext {
    fn validate(self, dataset: DatasetId) -> Result<(), FactError> {
        self.endpoint_id
            .require(dataset, DictionaryKind::Endpoint)?;
        self.access_scope_id
            .require(dataset, DictionaryKind::AccessScope)?;
        self.verification_method_id
            .require(dataset, DictionaryKind::Method)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalObservation {
    pub received_at: ObservedTime,
    pub observer_id: QualifiedRef,
    pub clock_quality: ClockQuality,
    pub witness: Option<WitnessContext>,
}
impl LocalObservation {
    fn validate(self, dataset: DatasetId) -> Result<(), FactError> {
        self.observer_id
            .require(dataset, DictionaryKind::Observer)?;
        self.clock_quality
            .clock_id()
            .require(dataset, DictionaryKind::Observer)?;
        if let Some(witness) = self.witness {
            witness.validate(dataset)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(
    clippy::large_enum_variant,
    reason = "network is the common path; its fixed observation stays inline to avoid an allocation per delivery"
)]
enum Arrival {
    Network(LocalObservation),
    Import(QualifiedRef),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventIntent {
    admission_policy_version: u16,
    visibility_scope_id: QualifiedRef,
    event: SignedEvent,
    arrival: Arrival,
}
impl EventIntent {
    pub fn network(
        admission_policy_version: u16,
        visibility_scope_id: QualifiedRef,
        event: SignedEvent,
        observation: LocalObservation,
    ) -> Result<Self, FactError> {
        Self::new(
            admission_policy_version,
            visibility_scope_id,
            event,
            Arrival::Network(observation),
        )
    }
    pub fn import(
        admission_policy_version: u16,
        visibility_scope_id: QualifiedRef,
        event: SignedEvent,
        lineage: QualifiedRef,
    ) -> Result<Self, FactError> {
        Self::new(
            admission_policy_version,
            visibility_scope_id,
            event,
            Arrival::Import(lineage),
        )
    }
    fn new(
        admission_policy_version: u16,
        visibility_scope_id: QualifiedRef,
        event: SignedEvent,
        arrival: Arrival,
    ) -> Result<Self, FactError> {
        let dataset = visibility_scope_id.dataset_id;
        visibility_scope_id.require(dataset, DictionaryKind::VisibilityScope)?;
        match arrival {
            Arrival::Network(observation) => observation.validate(dataset)?,
            Arrival::Import(lineage) => EventOrigin::Import(lineage).validate(dataset)?,
        }
        Ok(Self {
            admission_policy_version,
            visibility_scope_id,
            event,
            arrival,
        })
    }
    /// Re-evaluate against current policy at admission; a matching context alone
    /// does not authorize its endpoint, source or visibility scope.
    pub fn validate_provenance(&self, enabled: bool) -> Result<(), FactError> {
        if let Arrival::Network(observation) = self.arrival
            && observation.witness.is_some() != enabled
        {
            return Err(FactError::ProvenanceContext);
        }
        Ok(())
    }
    pub const fn admission_policy_version(&self) -> u16 {
        self.admission_policy_version
    }
    pub const fn visibility_scope_id(&self) -> QualifiedRef {
        self.visibility_scope_id
    }
    pub const fn event(&self) -> &SignedEvent {
        &self.event
    }
    pub const fn origin(&self) -> EventOrigin {
        match self.arrival {
            Arrival::Network(_) => EventOrigin::Network,
            Arrival::Import(lineage) => EventOrigin::Import(lineage),
        }
    }
    pub const fn observation(&self) -> Option<&LocalObservation> {
        match &self.arrival {
            Arrival::Network(observation) => Some(observation),
            Arrival::Import(_) => None,
        }
    }
}
