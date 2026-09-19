//! Validate a historical receipt's own closure, without consulting newer minima.
use super::admission::{ReadBudget, matching_signed_payload};
use super::{AdmissionError, AdmissionLimits, IngestStore};
use bigbrotr_model::*;

pub(super) struct ReplayEventIntent<'a> {
    pub event: &'a SignedEvent,
    pub scope: QualifiedRef,
    pub origin: EventOrigin,
    pub observation: Option<LocalObservation>,
}

impl IngestStore {
    pub(super) fn require_receipt_effects(
        &self,
        receipt: &Receipt,
        input: ReplayEventIntent<'_>,
        limits: AdmissionLimits,
        reads: &mut ReadBudget,
    ) -> Result<(), AdmissionError> {
        let mut required = Vec::new();
        let mut dictionaries = Vec::new();
        for values in [&mut required, &mut dictionaries] {
            values
                .try_reserve_exact(limits.max_dependencies)
                .map_err(|_| AdmissionError::Allocation)?;
        }
        let local = input.observation;
        let witness = local.and_then(|o| o.witness);
        for reference in [
            Some(input.scope),
            input.origin.import_lineage(),
            local.map(|o| o.observer_id),
            local.map(|o| o.clock_quality.clock_id()),
            witness.map(|w| w.endpoint_id),
            witness.map(|w| w.access_scope_id),
            witness.map(|w| w.verification_method_id),
        ]
        .into_iter()
        .flatten()
        {
            add_reference(&mut required, reference, limits.max_dependencies)?;
        }
        let (mut payload, mut event_mapping) = (None, None);
        let mut eligible = false;
        let mut received = local.is_none();
        let mut witnessed = witness.is_none();
        let body_digest = input.event.body_digest();
        // Decode one fact at a time. Only fixed-size dictionary references escape
        // this iteration; all I/O and metadata counts remain in the same budget.
        for dependency in receipt.dependencies().records() {
            let fact = self.admission_fact(*dependency, limits, reads)?;
            for reference in fact.dictionary_refs() {
                add_reference(&mut required, reference, limits.max_dependencies)?;
            }
            match fact.payload() {
                FactPayload::Dictionary(entry) => {
                    add_reference(
                        &mut dictionaries,
                        entry.reference(),
                        limits.max_dependencies,
                    )?;
                    if entry.descriptor() == &DictionaryDescriptor::Event(input.event.id)
                        && event_mapping.replace(entry.reference()).is_some()
                    {
                        return Err(AdmissionError::StateMismatch);
                    }
                    if witness.is_some_and(|w| w.access_scope_id == entry.reference())
                        && !matches!(entry.descriptor(), DictionaryDescriptor::AccessScope(v) if v.visibility_scope_id == input.scope)
                    {
                        return Err(AdmissionError::StateMismatch);
                    }
                    if witness.is_some_and(|w| w.verification_method_id == entry.reference())
                        && entry.descriptor() != &DictionaryDescriptor::EventVerification
                    {
                        return Err(AdmissionError::StateMismatch);
                    }
                }
                FactPayload::EventPayload(_) => {
                    let reference = matching_signed_payload(&fact, input.event)?;
                    if payload.replace(reference).is_some() {
                        return Err(AdmissionError::StateMismatch);
                    }
                }
                FactPayload::EventEligibility(v) => {
                    eligible |= fact.visibility_scope_id() == input.scope
                        && v.event_id == input.event.id
                        && v.body_digest == body_digest;
                }
                FactPayload::EventReceive(v) => {
                    received |= local.is_some_and(|o| v.received_at <= o.received_at)
                        && fact.visibility_scope_id() == input.scope
                        && v.event_id == input.event.id;
                }
                FactPayload::Provenance(v) => {
                    witnessed |= local.zip(witness).is_some_and(|(o, w)| {
                        v.received_at <= o.received_at
                            && v.observer_id == o.observer_id
                            && v.endpoint_id == w.endpoint_id
                            && v.access_scope_id == w.access_scope_id
                    }) && fact.visibility_scope_id() == input.scope
                        && v.event_id == input.event.id;
                }
            }
        }
        if payload.is_none()
            || payload != event_mapping
            || !eligible
            || !received
            || !witnessed
            || required.iter().any(|r| !dictionaries.contains(r))
        {
            return Err(AdmissionError::StateMismatch);
        }
        Ok(())
    }
}

fn add_reference(
    values: &mut Vec<QualifiedRef>,
    value: QualifiedRef,
    maximum: usize,
) -> Result<(), AdmissionError> {
    if !values.contains(&value) {
        if values.len() >= maximum {
            return Err(AdmissionError::Limit);
        }
        values.push(value);
    }
    Ok(())
}
