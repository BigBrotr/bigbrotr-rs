//! Read-only preparation of fixed dictionary references for the initial writer.
use super::{
    AdmissionError, AdmissionLimits, IngestStore, admission::ReadBudget, bootstrap::read_options,
};
use crate::{
    runtime::OwnedPermit,
    storage::rocks::keys::{ColumnFamily, IngestKey, decode_key},
};
use bigbrotr_model::*;
use sha2::{Digest, Sha256};
use std::{
    mem::size_of,
    sync::atomic::{AtomicBool, Ordering},
};

/// One borrowed native view. References are provisional until the fixed intent
/// passes the existing reservation/dispatch path; this value grants no authority.
pub struct DictionaryPreparation<'a> {
    store: &'a IngestStore,
    cancelled: &'a AtomicBool,
    visibility: QualifiedRef,
    limits: AdmissionLimits,
    entries: Vec<DictionaryEntry>,
    through: [Option<u64>; 9],
    retained_heap_bytes: usize,
    reads: ReadBudget,
    // Retained values always drop before the grant, including failed preparation.
    permit: OwnedPermit,
}

pub struct PreparedDictionaries {
    intent: EvidenceIntent,
    _permit: OwnedPermit,
}
impl PreparedDictionaries {
    pub fn intent(&self) -> &EvidenceIntent {
        &self.intent
    }
}
impl AdmissionLimits {
    /// Retained descriptors, candidate encoding, native value copies/decoding,
    /// closure scratch and the final intent's validation table overlap. Native
    /// iterator/cache memory remains in the store's existing native reservation.
    pub fn required_dictionary_preparation_data_bytes(self) -> Result<u64, AdmissionError> {
        self.required_data_bytes()?;
        Ok((8 * self.max_batch_bytes
            + self.max_dependencies
                * (size_of::<DictionaryEntry>()
                    + 2 * size_of::<FactRef>()
                    + size_of::<QualifiedRef>())
            + 2 * size_of::<Fact>()
            + size_of::<DictionaryPreparation<'_>>()) as u64)
    }
}
impl IngestStore {
    pub fn prepare_dictionaries<'a>(
        &'a self,
        visibility: QualifiedRef,
        limits: AdmissionLimits,
        permit: OwnedPermit,
        cancelled: &'a AtomicBool,
    ) -> Result<DictionaryPreparation<'a>, AdmissionError> {
        if permit.request().units().data_bytes()
            < limits.required_dictionary_preparation_data_bytes()?
            || permit.request().units().items() == 0
        {
            return Err(AdmissionError::InsufficientReservation);
        }
        check_cancelled(cancelled)?;
        if self.write_uncertain {
            return Err(AdmissionError::CommitUnknown);
        }
        self.require_admission_owner(None)?;
        visibility
            .require(
                self.manifest.identity().dataset_id,
                DictionaryKind::VisibilityScope,
            )
            .map_err(|_| AdmissionError::InvalidInput)?;
        let mut preparation = DictionaryPreparation {
            store: self,
            cancelled,
            visibility,
            limits,
            entries: Vec::new(),
            through: [None; 9],
            retained_heap_bytes: 0,
            reads: ReadBudget::dictionaries(limits),
            permit,
        };
        preparation
            .entries
            .try_reserve_exact(limits.max_dependencies)
            .map_err(|_| AdmissionError::Allocation)?;
        // Vec may reserve more than requested. Refuse before retaining values if
        // its actual element capacity exceeds the reserved preparation allowance.
        if preparation.entries.capacity() > limits.max_dependencies {
            return Err(AdmissionError::InsufficientReservation);
        }
        let fact = self
            .indexed_fact(
                IngestKey::Dictionary {
                    reference: visibility,
                },
                limits,
                &mut preparation.reads,
            )?
            .ok_or(AdmissionError::InvalidInput)?;
        let FactPayload::Dictionary(entry) = fact.payload() else {
            return Err(AdmissionError::StateMismatch);
        };
        if entry.reference() != visibility {
            return Err(AdmissionError::StateMismatch);
        }
        if fact.visibility_scope_id() != visibility {
            return Err(AdmissionError::InvalidInput);
        }
        self.require_dictionary_indices(entry, limits, &mut preparation.reads)?;
        preparation.require_closure(&fact)?;
        check_cancelled(cancelled)?;
        Ok(preparation)
    }
}
impl DictionaryPreparation<'_> {
    /// Uses full values and the exact envelope, never the hash alone. No durable
    /// or provisional counter advances until every check for this entry passes.
    pub fn intern(
        &mut self,
        descriptor: DictionaryDescriptor,
    ) -> Result<QualifiedRef, AdmissionError> {
        check_cancelled(self.cancelled)?;
        self.reads.charge(0)?;
        let heap = descriptor.heap_allocation_bytes();
        if heap > self.limits.max_batch_bytes {
            return Err(AdmissionError::Limit);
        }
        let kind = descriptor.kind();
        if matches!(
            kind,
            DictionaryKind::VisibilityScope | DictionaryKind::Event
        ) {
            return Err(AdmissionError::InvalidInput);
        }
        if let Some(prior) = self
            .entries
            .iter()
            .find(|entry| entry.descriptor() == &descriptor)
        {
            return Ok(prior.reference());
        }
        if heap
            > self
                .limits
                .max_batch_bytes
                .saturating_sub(self.retained_heap_bytes)
        {
            return Err(AdmissionError::Limit);
        }
        if self.entries.len() == self.limits.max_dependencies {
            return Err(AdmissionError::Limit);
        }
        let domain = self
            .store
            .manifest
            .allocation_namespaces()
            .domain(kind)
            .ok_or(AdmissionError::InvalidInput)?;
        let mut reference = QualifiedRef {
            dataset_id: self.visibility.dataset_id,
            dictionary_kind: kind,
            allocation_domain: domain,
            ordinal: Ordinal::new(1).expect("nonzero probe ordinal"),
        };
        // The temporary reference supplies namespace validation and encoding; it
        // is not allocated and is replaced with the selected qualified identity.
        let probe = DictionaryEntry::new(reference, descriptor)
            .map_err(|_| AdmissionError::InvalidInput)?;
        if let DictionaryDescriptor::Endpoint(endpoint) = probe.descriptor() {
            let normalized = bigbrotr_protocol::endpoint::Endpoint::parse(endpoint.url())
                .map_err(|_| AdmissionError::InvalidInput)?;
            if normalized.as_str() != endpoint.url() {
                return Err(AdmissionError::InvalidInput);
            }
        }
        let encoded = probe
            .encode_descriptor(self.limits.max_batch_bytes)
            .map_err(|_| AdmissionError::Limit)?;
        let existing = self.find_candidate(&probe, &encoded)?;
        drop(encoded);
        let slot = kind as usize - 1;
        let next = if let Some(existing) = existing {
            reference = existing;
            None
        } else {
            let high = match self.through[slot] {
                Some(high) => high,
                None => self
                    .store
                    .dictionary_counter(allocation_key(reference), &mut self.reads)?
                    .unwrap_or(0),
            };
            let ordinal = high.checked_add(1).ok_or(AdmissionError::Limit)?;
            reference.ordinal = Ordinal::new(ordinal).map_err(|_| AdmissionError::Limit)?;
            if self
                .store
                .admission_value(IngestKey::Dictionary { reference }, 70, &mut self.reads)?
                .is_some()
            {
                return Err(AdmissionError::StateMismatch);
            }
            Some(ordinal)
        };
        check_cancelled(self.cancelled)?;
        // Moving the validated descriptor preserves its already charged capacity.
        let entry = DictionaryEntry::new(reference, probe.into_parts().1)
            .map_err(|_| AdmissionError::InvalidInput)?;
        self.entries.push(entry);
        self.retained_heap_bytes += heap;
        if let Some(next) = next {
            self.through[slot] = Some(next);
        }
        Ok(reference)
    }

    /// Seal the caller-ordered unique values and retain their grant. Request,
    /// window, timestamp and full new dependency closure are checked by staging;
    /// an ambiguous retry must keep this intent rather than repeat interning.
    pub fn finish(self) -> Result<PreparedDictionaries, AdmissionError> {
        check_cancelled(self.cancelled)?;
        let intent = EvidenceIntent::new(
            1,
            self.visibility,
            self.entries,
            EvidenceIntentLimits {
                max_entries: self.limits.max_dependencies,
                max_encoded_intent_bytes: self.limits.max_batch_bytes,
                max_validation_scratch_bytes: self.limits.max_dependencies
                    * size_of::<QualifiedRef>(),
            },
        )
        .map_err(|error| match error {
            EvidenceIntentError::AllocationFailed => AdmissionError::Allocation,
            EvidenceIntentError::EmptyEntries => AdmissionError::InvalidInput,
            _ => AdmissionError::Limit,
        })?;
        Ok(PreparedDictionaries {
            intent,
            _permit: self.permit,
        })
    }

    fn find_candidate(
        &mut self,
        probe: &DictionaryEntry,
        descriptor: &[u8],
    ) -> Result<Option<QualifiedRef>, AdmissionError> {
        let reference = probe.reference();
        let encoded = IngestKey::DictionaryReverseCandidate {
            dataset_id: reference.dataset_id,
            dictionary_kind: reference.dictionary_kind,
            descriptor_digest: Digest256::from_bytes(Sha256::digest(descriptor).into()),
            reference,
        }
        .encode()
        .map_err(|_| AdmissionError::InvalidInput)?;
        let prefix = &encoded.as_bytes()[..52];
        let store = self.store;
        let cf = store
            .db
            .cf_handle("default")
            .ok_or(AdmissionError::EngineRead)?;
        let mut iterator = store.db.raw_iterator_cf_opt(cf, read_options());
        iterator.seek(prefix);
        let mut selected = None;
        while let Some(key) = iterator.key().filter(|key| key.starts_with(prefix)) {
            check_cancelled(self.cancelled)?;
            let value = iterator.value().ok_or(AdmissionError::EngineRead)?;
            self.reads.charge(
                key.len()
                    .checked_add(value.len())
                    .ok_or(AdmissionError::Limit)?,
            )?;
            let IngestKey::DictionaryReverseCandidate {
                reference: candidate,
                ..
            } = decode_key(ColumnFamily::Default, key)
                .map_err(|_| AdmissionError::StateMismatch)?
            else {
                return Err(AdmissionError::StateMismatch);
            };
            if value.len() < 2 || value[..2] != [1, 5] {
                return Err(AdmissionError::StateMismatch);
            }
            if value[2..] == *descriptor
                && candidate.allocation_domain == reference.allocation_domain
            {
                let fact = self
                    .store
                    .indexed_fact(
                        IngestKey::Dictionary {
                            reference: candidate,
                        },
                        self.limits,
                        &mut self.reads,
                    )?
                    .ok_or(AdmissionError::StateMismatch)?;
                let FactPayload::Dictionary(entry) = fact.payload() else {
                    return Err(AdmissionError::StateMismatch);
                };
                if entry.reference() != candidate || entry.descriptor() != probe.descriptor() {
                    return Err(AdmissionError::StateMismatch);
                }
                self.store
                    .require_dictionary_indices(entry, self.limits, &mut self.reads)?;
                if fact.visibility_scope_id() == self.visibility {
                    self.require_closure(&fact)?;
                    selected = Some(candidate);
                    break;
                }
            }
            iterator.next();
        }
        iterator.status().map_err(|_| AdmissionError::EngineRead)?;
        Ok(selected)
    }

    fn require_closure(&mut self, fact: &Fact) -> Result<(), AdmissionError> {
        let mut dependencies = Vec::new();
        dependencies
            .try_reserve_exact(self.limits.max_dependencies)
            .map_err(|_| AdmissionError::Allocation)?;
        self.store
            .retain_admission_fact(fact, self.limits, &mut self.reads, &mut dependencies)?;
        for dependency in dependencies {
            check_cancelled(self.cancelled)?;
            if self
                .store
                .admission_fact(dependency, self.limits, &mut self.reads)?
                .visibility_scope_id()
                != self.visibility
            {
                return Err(AdmissionError::InvalidInput);
            }
        }
        Ok(())
    }
}
fn allocation_key(reference: QualifiedRef) -> IngestKey {
    IngestKey::AllocationStream {
        dataset_id: reference.dataset_id,
        dictionary_kind: reference.dictionary_kind,
        allocation_domain: reference.allocation_domain,
    }
}
fn check_cancelled(cancelled: &AtomicBool) -> Result<(), AdmissionError> {
    if cancelled.load(Ordering::Relaxed) {
        Err(AdmissionError::AuthorityUnavailable)
    } else {
        Ok(())
    }
}
