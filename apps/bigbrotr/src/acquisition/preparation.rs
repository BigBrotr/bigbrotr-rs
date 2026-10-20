//! Bind one actual retained EVENT to its decoded intent before verification.
//! This is neither current SQL authority nor a successful admission witness.
use super::{
    observation::StoredObservation,
    relay_source::{RelaySourceBinding, StoredRelaySource},
    scope::{AcquisitionEventContext, StoredAcquisitionScope},
    session::RetainedRelayEvent,
};
use crate::runtime::OwnedPermit;
use bigbrotr_model::{AcquisitionScopeId, Digest256, EventIntent, JobId};
use bigbrotr_protocol::decode_event_json;
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapturedInputError {
    Budget,
    Decode(bigbrotr_protocol::EventError),
    Scope(super::scope::ScopeEventError),
    Observation(super::observation::ObservationError),
    Intent,
}
impl std::fmt::Display for CapturedInputError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "retained EVENT preparation unavailable: {self:?}")
    }
}
impl std::error::Error for CapturedInputError {}

/// No public constructor, deserializer, mutation or independently supplied raw
/// digest. Invalid signatures still retain this complete preparation binding.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CapturedBinding {
    pub request: [u8; 48],
    pub wire_sequence: u64,
    pub input_sha256: [u8; 32],
    pub job: JobId,
    pub scope: AcquisitionScopeId,
    pub source: RelaySourceBinding,
    pub certification_digest: Digest256,
    pub observed_at_us: i64,
    pub clock_resolution_us: u64,
    pub clock_anomaly_flags: u32,
}

pub struct CapturedEventInput {
    intent: EventIntent,
    binding: CapturedBinding,
    permit: OwnedPermit,
}
impl CapturedEventInput {
    pub fn prepare(
        captured: &RetainedRelayEvent<'_>,
        job: JobId,
        scope: &StoredAcquisitionScope,
        source: &StoredRelaySource,
        attribution: &StoredObservation,
        permit: OwnedPermit,
    ) -> Result<Self, CapturedInputError> {
        if permit.request().units().items() == 0
            || permit.request().units().data_bytes()
                < captured.limits().parse_bytes as u64 + std::mem::size_of::<Self>() as u64
        {
            return Err(CapturedInputError::Budget);
        }
        let event = decode_event_json(captured.json(), captured.limits())
            .map_err(CapturedInputError::Decode)?;
        let context = AcquisitionEventContext::for_job(job, scope, captured.request().filters())
            .map_err(CapturedInputError::Scope)?;
        context
            .check(source.binding(), scope.scope().visibility(), &event)
            .map_err(CapturedInputError::Scope)?;
        if attribution.visibility() != scope.scope().visibility() {
            return Err(CapturedInputError::Intent);
        }
        let observation = attribution
            .received(source.endpoint(), source.route(), captured.received())
            .map_err(CapturedInputError::Observation)?;
        let intent = EventIntent::network(1, scope.scope().visibility(), event, observation)
            .map_err(|_| CapturedInputError::Intent)?;
        let binding = CapturedBinding {
            request: captured
                .request()
                .id()
                .as_str()
                .as_bytes()
                .try_into()
                .map_err(|_| CapturedInputError::Intent)?,
            wire_sequence: captured.sequence(),
            input_sha256: Sha256::digest(captured.json()).into(),
            job,
            scope: scope.identity(),
            source: source.binding(),
            certification_digest: intent.certification_digest(),
            observed_at_us: observation.received_at.get(),
            clock_resolution_us: observation.clock_quality.resolution_us().get(),
            clock_anomaly_flags: observation.clock_quality.anomaly_flags(),
        };
        Ok(Self {
            intent,
            binding,
            permit,
        })
    }

    pub(crate) fn into_parts(self) -> (EventIntent, CapturedBinding, OwnedPermit) {
        (self.intent, self.binding, self.permit)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        runtime::{ProcessEnvelope, ResourceBudget, ResourceLimits, ResourceRequest, WaiterLimits},
        storage::rocks::store::{AdmissionError, PreparedCertification, PreparedEvent},
    };
    use bigbrotr_model::{
        ClockQuality, DictionaryKind, DurationUs, LocalObservation, ObservedTime, Ordinal,
        ProducerEpoch, ProducerRequestId, QualifiedRef, RequestSequence, SignedEvent,
    };

    #[test]
    fn a_mismatched_intent_cannot_cross_either_opaque_preparation_handoff() {
        let corpus: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../experiments/identity-contract/signed-fixture.json"
        ))
        .unwrap();
        let a: SignedEvent = serde_json::from_value(corpus["events"]["E_private"].clone()).unwrap();
        let b: SignedEvent = serde_json::from_value(corpus["events"]["E_public"].clone()).unwrap();
        let budget = ResourceBudget::new(
            ProcessEnvelope::new(
                64 * 1024 * 1024,
                0,
                ResourceLimits::new(48 * 1024 * 1024, 8, 0, 0),
                WaiterLimits::new(0, 0),
            )
            .unwrap(),
        );
        let grant = || {
            budget
                .try_acquire(ResourceRequest::data(10 * 1024 * 1024, 1, 0).unwrap())
                .unwrap()
        };
        let reference = |kind| QualifiedRef {
            dataset_id: [1; 16].try_into().unwrap(),
            dictionary_kind: kind,
            allocation_domain: [2; 16].try_into().unwrap(),
            ordinal: Ordinal::new(1).unwrap(),
        };
        let visibility = reference(DictionaryKind::VisibilityScope);
        let observer = reference(DictionaryKind::Observer);
        let intent = |event| {
            EventIntent::network(
                1,
                visibility,
                event,
                LocalObservation {
                    received_at: ObservedTime::new(17),
                    observer_id: observer,
                    clock_quality: ClockQuality::new(observer, DurationUs::new(1), None, 0)
                        .unwrap(),
                    witness: None,
                },
            )
            .unwrap()
        };
        let original = intent(a.clone());
        let binding = CapturedBinding {
            request: [b'a'; 48],
            wire_sequence: 7,
            input_sha256: [3; 32],
            job: [4; 16].try_into().unwrap(),
            scope: [5; 16].try_into().unwrap(),
            source: RelaySourceBinding {
                endpoint_id: [6; 16].try_into().unwrap(),
                access_scope_id: [7; 16].try_into().unwrap(),
                enrollment_generation: 1.try_into().unwrap(),
                route_id: [8; 16].try_into().unwrap(),
            },
            certification_digest: original.certification_digest(),
            observed_at_us: 17,
            clock_resolution_us: 1,
            clock_anomaly_flags: 0,
        };
        let request = ProducerRequestId {
            producer_slot_id: [9; 16].try_into().unwrap(),
            producer_epoch: ProducerEpoch::new(1).unwrap(),
            request_sequence: RequestSequence::new(1).unwrap(),
        };
        let mut invalid = a;
        invalid.sig = "00".repeat(64).parse().unwrap();
        // Deliberate internal corruption of an otherwise unconstructible handoff.
        // Both a different valid event and a rejected signature must keep their
        // own full semantic identity; neither can classify the original frame.
        for event in [b, invalid] {
            let changed = intent(event);
            assert_ne!(changed.certification_digest(), binding.certification_digest);
            let handoff = || CapturedEventInput {
                intent: changed.clone(),
                binding,
                permit: grant(),
            };
            assert!(matches!(
                PreparedCertification::from_captured(handoff(), 256 * 1024),
                Err(AdmissionError::InvalidInput)
            ));
            assert!(matches!(
                PreparedEvent::from_captured(request, handoff(), 256 * 1024),
                Err(AdmissionError::InvalidInput)
            ));
        }
    }
}
