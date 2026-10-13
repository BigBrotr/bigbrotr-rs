//! Existing effects in one serialized committed view, without mutation identity.
use super::admission::{
    ReadBudget, input_allowance, input_capacity, matching_payload, verify_event,
};
use super::{AdmissionError, AdmissionLimits, IngestStore, RejectionReason};
use crate::{runtime::OwnedPermit, storage::rocks::keys::IngestKey};
use bigbrotr_model::*;
use bigbrotr_protocol::VerifiedEvent;
use std::mem::size_of;

pub struct PreparedCertification {
    digest: Digest256,
    scope: QualifiedRef,
    origin: EventOrigin,
    observation: Option<LocalObservation>,
    event: Result<VerifiedEvent, RejectionReason>,
    allocation_bytes: usize,
    verification_limit_bytes: usize,
    _permit: OwnedPermit,
}
impl PreparedCertification {
    pub fn new(
        intent: EventIntent,
        max_event_bytes: usize,
        permit: OwnedPermit,
    ) -> Result<Self, AdmissionError> {
        let left = input_allowance(&intent, max_event_bytes, &permit, size_of::<Self>())?;
        let allocation_bytes = input_capacity(&permit) - left;
        let digest = intent.certification_digest();
        let scope = intent.visibility_scope_id();
        let origin = intent.origin();
        let observation = intent.observation().copied();
        let event = verify_event(intent.into_event(), max_event_bytes)?;
        Ok(Self {
            digest,
            scope,
            origin,
            observation,
            event,
            allocation_bytes,
            verification_limit_bytes: max_event_bytes,
            _permit: permit,
        })
    }
    pub fn intent_digest(&self) -> Digest256 {
        self.digest
    }
    pub fn verification_limit_bytes(&self) -> usize {
        self.verification_limit_bytes
    }
    pub fn verified_event(&self) -> Option<&VerifiedEvent> {
        self.event.as_ref().ok()
    }
    pub fn visibility_scope_id(&self) -> QualifiedRef {
        self.scope
    }
    pub fn origin(&self) -> EventOrigin {
        self.origin
    }
    pub fn observation(&self) -> Option<&LocalObservation> {
        self.observation.as_ref()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct CertificationLimits {
    pub max_fact_bytes: usize,
    pub max_dependencies: usize,
}
impl Default for CertificationLimits {
    fn default() -> Self {
        Self {
            max_fact_bytes: 2 * 1024 * 1024,
            max_dependencies: 64,
        }
    }
}
impl CertificationLimits {
    pub(super) fn reads(self) -> Result<AdmissionLimits, AdmissionError> {
        if self.max_fact_bytes == 0
            || self.max_fact_bytes > 2 * 1024 * 1024
            || !(8..=256).contains(&self.max_dependencies)
        {
            return Err(AdmissionError::InvalidLimits);
        }
        // Only the shared fact/lookup bounds are consumed, never producer state.
        Ok(AdmissionLimits {
            max_batch_bytes: self.max_fact_bytes,
            max_dependencies: self.max_dependencies,
            max_outstanding_requests: 1,
        })
    }
    pub fn required_data_bytes(self) -> Result<u64, AdmissionError> {
        self.reads()?;
        Ok((8 * self.max_fact_bytes
            + self.max_dependencies * (size_of::<FactRef>() + size_of::<QualifiedRef>())
            + 4 * size_of::<Fact>()
            + size_of::<ExistingProof>()) as u64)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum CertificationOutcome {
    Existing(ExistingProof),
    NeedsAdmission,
    Rejected(RejectionReason),
}
pub struct StoredCertification {
    outcome: CertificationOutcome,
    _permit: OwnedPermit,
}
impl StoredCertification {
    pub fn outcome(&self) -> &CertificationOutcome {
        &self.outcome
    }
}

impl IngestStore {
    /// Exclusive access serializes this complete view against every native writer.
    /// Native handles never escape the owner; background maintenance cannot add
    /// logical facts. Local pins are dropped before the retained result permit.
    pub(in crate::storage) fn certify_event(
        &mut self,
        input: PreparedCertification,
        provenance: bool,
        limits: CertificationLimits,
        permit: OwnedPermit,
        mut authority: impl FnMut() -> Result<(), AdmissionError>,
    ) -> Result<StoredCertification, AdmissionError> {
        if permit.request().units().data_bytes() < limits.required_data_bytes()?
            || permit.request().units().items() == 0
        {
            return Err(AdmissionError::InsufficientReservation);
        }
        let limits = limits.reads()?;
        if input.allocation_bytes > limits.facts().max_allocation_bytes {
            return Err(AdmissionError::Limit);
        }
        authority()?;
        if self.write_uncertain {
            return Err(AdmissionError::CommitUnknown);
        }
        let identity = self.manifest.identity();
        self.require_admission_owner(None)?;
        if input.scope.dataset_id != identity.dataset_id
            || input
                .observation
                .is_some_and(|o| o.witness.is_some() != provenance)
        {
            return Err(AdmissionError::InvalidInput);
        }
        let outcome = match &input.event {
            Err(reason) => CertificationOutcome::Rejected(*reason),
            Ok(event) => self.existing_event_effects(&input, event, limits)?,
        };
        authority()?;
        Ok(StoredCertification {
            outcome,
            _permit: permit,
        })
    }

    fn existing_event_effects(
        &self,
        input: &PreparedCertification,
        event: &VerifiedEvent,
        limits: AdmissionLimits,
    ) -> Result<CertificationOutcome, AdmissionError> {
        let identity = *self.manifest.identity();
        let dataset_id = identity.dataset_id;
        let event_id = event.event().id;
        let mut reads = ReadBudget::new(limits);
        let mut dependencies = Vec::new();
        dependencies
            .try_reserve_exact(limits.max_dependencies)
            .map_err(|_| AdmissionError::Allocation)?;
        let observation = input.observation;
        let witness = observation.and_then(|o| o.witness);
        let lineage = match input.origin {
            EventOrigin::Import(r) => Some(r),
            EventOrigin::Network => None,
        };
        // Input attribution and selected historical facts can need different
        // lineage/observer/clock descriptors. Both sets belong in the proof.
        for reference in [
            Some(input.scope),
            lineage,
            observation.map(|o| o.observer_id),
            observation.map(|o| o.clock_quality.clock_id()),
            witness.map(|w| w.endpoint_id),
            witness.map(|w| w.access_scope_id),
            witness.map(|w| w.verification_method_id),
        ]
        .into_iter()
        .flatten()
        {
            let Some(entry) = self.lookup_dictionary_dependency(
                reference,
                limits,
                &mut reads,
                &mut dependencies,
            )?
            else {
                return Ok(CertificationOutcome::NeedsAdmission);
            };
            if reference.dictionary_kind == DictionaryKind::AccessScope
                && !matches!(entry.descriptor(), DictionaryDescriptor::AccessScope(v) if v.visibility_scope_id == input.scope)
            {
                return Err(AdmissionError::InvalidInput);
            }
            if reference.dictionary_kind == DictionaryKind::Method
                && entry.descriptor() != &DictionaryDescriptor::EventVerification
            {
                return Err(AdmissionError::InvalidInput);
            }
        }
        let Some(fact) = self.indexed_fact(
            IngestKey::EventPayload {
                dataset_id,
                event_id,
            },
            limits,
            &mut reads,
        )?
        else {
            return Ok(CertificationOutcome::NeedsAdmission);
        };
        let event_ordinal = matching_payload(&fact, event)?;
        self.retain_admission_fact(&fact, limits, &mut reads, &mut dependencies)?;
        let mapping =
            self.dictionary_dependency(event_ordinal, limits, &mut reads, &mut dependencies)?;
        if mapping.descriptor() != &DictionaryDescriptor::Event(event_id) {
            return Err(AdmissionError::StateMismatch);
        }
        drop(fact);
        let Some(fact) = self.indexed_fact(
            IngestKey::EventEligibility {
                dataset_id,
                event_id,
                visibility_scope: input.scope,
            },
            limits,
            &mut reads,
        )?
        else {
            return Ok(CertificationOutcome::NeedsAdmission);
        };
        if fact.visibility_scope_id() != input.scope
            || !matches!(fact.payload(), FactPayload::EventEligibility(v) if v.event_id == event_id && v.body_digest == event.body_digest())
        {
            return Err(AdmissionError::StateMismatch);
        }
        self.retain_admission_fact(&fact, limits, &mut reads, &mut dependencies)?;
        drop(fact);
        if let Some(observation) = observation {
            let Some(fact) = self.indexed_fact(
                IngestKey::EventReceive {
                    dataset_id,
                    event_id,
                    visibility_scope: input.scope,
                },
                limits,
                &mut reads,
            )?
            else {
                return Ok(CertificationOutcome::NeedsAdmission);
            };
            let FactPayload::EventReceive(value) = fact.payload() else {
                return Err(AdmissionError::StateMismatch);
            };
            if value.event_id != event_id || fact.visibility_scope_id() != input.scope {
                return Err(AdmissionError::StateMismatch);
            }
            if observation.received_at < value.received_at {
                return Ok(CertificationOutcome::NeedsAdmission);
            }
            self.retain_admission_fact(&fact, limits, &mut reads, &mut dependencies)?;
            drop(fact);
            if let Some(witness) = observation.witness {
                let Some(fact) = self.indexed_fact(
                    IngestKey::Provenance {
                        dataset_id,
                        event_id,
                        endpoint_id: witness.endpoint_id,
                        observer_id: observation.observer_id,
                        access_scope_id: witness.access_scope_id,
                        visibility_scope_id: input.scope,
                    },
                    limits,
                    &mut reads,
                )?
                else {
                    return Ok(CertificationOutcome::NeedsAdmission);
                };
                let FactPayload::Provenance(value) = fact.payload() else {
                    return Err(AdmissionError::StateMismatch);
                };
                if value.event_id != event_id
                    || fact.visibility_scope_id() != input.scope
                    || value.endpoint_id != witness.endpoint_id
                    || value.observer_id != observation.observer_id
                    || value.access_scope_id != witness.access_scope_id
                {
                    return Err(AdmissionError::StateMismatch);
                }
                if observation.received_at < value.received_at {
                    return Ok(CertificationOutcome::NeedsAdmission);
                }
                self.retain_admission_fact(&fact, limits, &mut reads, &mut dependencies)?;
            }
        }
        dependencies.sort_unstable_by_key(|r| r.source_record_id);
        let dependencies = SourceDependencies::new(dataset_id, dependencies)
            .map_err(|_| AdmissionError::StateMismatch)?;
        let proof = ExistingProof::new(
            ProtectionContext {
                dataset_id,
                topology_generation: identity.topology_generation,
                protection_class: ProtectionClass::LocalSynced,
                recovery_generation: identity.recovery_generation,
                policy_epoch: identity.policy_epoch,
            },
            input.digest,
            dependencies,
        )
        .map_err(|_| AdmissionError::StateMismatch)?;
        Ok(CertificationOutcome::Existing(proof))
    }
}
