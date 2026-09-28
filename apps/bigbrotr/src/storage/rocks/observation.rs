//! One serialized read of the committed attribution needed by a local receiver.
use super::{AdmissionError, CertificationLimits, IngestStore, admission::ReadBudget};
use crate::{
    acquisition::observation::{ObservationBindings, StoredObservation},
    runtime::OwnedPermit,
};
use bigbrotr_model::{AccessClass, ClockSource, DictionaryDescriptor, DictionaryKind};

impl IngestStore {
    /// The first profile requires every descriptor dependency in the exact target
    /// visibility. Supporting shared dictionaries across visibility boundaries
    /// needs explicit grants; descriptor equality alone cannot supply them.
    pub(in crate::storage) fn read_observation(
        &mut self,
        input: ObservationBindings<'_>,
        limits: CertificationLimits,
        permit: OwnedPermit,
        mut authority: impl FnMut() -> Result<(), AdmissionError>,
    ) -> Result<Option<StoredObservation>, AdmissionError> {
        if permit.request().units().data_bytes() < limits.required_data_bytes()?
            || permit.request().units().items() == 0
        {
            return Err(AdmissionError::InsufficientReservation);
        }
        let limits = limits.reads()?;
        authority()?;
        if self.write_uncertain {
            return Err(AdmissionError::CommitUnknown);
        }
        let dataset = self.manifest.identity().dataset_id;
        let witness = input.witness;
        for (reference, kind) in [
            (Some(input.visibility), DictionaryKind::VisibilityScope),
            (Some(input.observer.reference()), DictionaryKind::Observer),
            (Some(input.clock.reference()), DictionaryKind::Observer),
            (witness.map(|w| w.endpoint_id), DictionaryKind::Endpoint),
            (
                witness.map(|w| w.access_scope_id),
                DictionaryKind::AccessScope,
            ),
            (
                witness.map(|w| w.verification_method_id),
                DictionaryKind::Method,
            ),
        ] {
            if let Some(reference) = reference {
                reference
                    .require(dataset, kind)
                    .map_err(|_| AdmissionError::InvalidInput)?;
            }
        }
        let DictionaryDescriptor::Observer(observer) = input.observer.descriptor() else {
            return Err(AdmissionError::InvalidInput);
        };
        let DictionaryDescriptor::Observer(clock) = input.clock.descriptor() else {
            return Err(AdmissionError::InvalidInput);
        };
        if clock.clock != Some(ClockSource::SystemUtc)
            || observer
                .clock
                .is_some_and(|source| source != ClockSource::SystemUtc)
        {
            return Err(AdmissionError::InvalidInput);
        }
        let mut reads = ReadBudget::new(limits);
        let mut dependencies = Vec::new();
        dependencies
            .try_reserve_exact(limits.max_dependencies)
            .map_err(|_| AdmissionError::Allocation)?;
        let result = (|| {
            let mut lookup = |reference| {
                self.lookup_dictionary_dependency(reference, limits, &mut reads, &mut dependencies)
            };
            if lookup(input.visibility)?.is_none() {
                return Ok(None);
            }
            for expected in [input.observer, input.clock] {
                let Some(actual) = lookup(expected.reference())? else {
                    return Ok(None);
                };
                actual
                    .require_same_mapping(expected)
                    .map_err(|_| AdmissionError::InvalidInput)?;
            }
            let endpoint = if let Some(witness) = witness {
                let Some(endpoint) = lookup(witness.endpoint_id)? else {
                    return Ok(None);
                };
                let DictionaryDescriptor::Endpoint(endpoint) = endpoint.descriptor() else {
                    return Err(AdmissionError::StateMismatch);
                };
                let Some(access) = lookup(witness.access_scope_id)? else {
                    return Ok(None);
                };
                if !matches!(access.descriptor(), DictionaryDescriptor::AccessScope(access)
                    if access.class == AccessClass::Public && access.label.is_none()
                        && access.visibility_scope_id == input.visibility)
                {
                    return Err(AdmissionError::InvalidInput);
                }
                let Some(method) = lookup(witness.verification_method_id)? else {
                    return Ok(None);
                };
                if method.descriptor() != &DictionaryDescriptor::EventVerification {
                    return Err(AdmissionError::InvalidInput);
                }
                Some((witness, endpoint.clone()))
            } else {
                None
            };
            // The shared lookup verifies values and complete transitive closure.
            // Read its exact facts again under the same budget/view to check the
            // envelopes too; returning a mapping alone loses that visibility.
            for dependency in &dependencies {
                if self
                    .admission_fact(*dependency, limits, &mut reads)?
                    .visibility_scope_id()
                    != input.visibility
                {
                    return Err(AdmissionError::InvalidInput);
                }
            }
            Ok(Some(endpoint))
        })();
        drop(dependencies);
        authority()?;
        Ok(result?.map(|witness| StoredObservation {
            visibility: input.visibility,
            observer: input.observer.reference(),
            clock: input.clock.reference(),
            observer_route: observer.route_class,
            clock_route: clock.route_class,
            witness,
            permit,
        }))
    }
}
