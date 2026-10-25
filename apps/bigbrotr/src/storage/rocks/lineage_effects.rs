//! Whole-frontier semantic census with constant retained state. Historical
//! receipt validation alone deliberately does not consult newer mutable minima.
use super::lineage::Census;
use super::receipt_effects::ReplayEventIntent;
use super::{AdmissionError, AdmissionLimits, IngestRecovery, RetainedLineage};
use super::{admission::ReadBudget, bootstrap::read_options};
use crate::{
    runtime::OwnedPermit,
    storage::rocks::keys::{ColumnFamily, IngestKey, decode_key},
};
use bigbrotr_model::{
    DictionaryDescriptor, Digest256, Fact, FactPayload, FactRef, ObservedTime, Receipt,
    SourceRecordId,
};
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicBool, Ordering};

impl super::IngestStore {
    /// The legacy replay establishes the original requested effects from its
    /// retained dependency closure. The new frontier additionally attributes
    /// EVERY emitted member to this input before extending the contiguous cut.
    pub(super) fn require_lineage_event_batch(
        &self,
        receipt: &Receipt,
        input: ReplayEventIntent<'_>,
        limits: AdmissionLimits,
    ) -> Result<(), AdmissionError> {
        let Some(batch) = receipt.batch() else {
            return Ok(());
        };
        let mut reads = ReadBudget::new(limits);
        let (mut rank, mut recorded_at, mut mapping, mut payload) = (0, None, None, None);
        for reference in batch.records() {
            let fact = self.admission_fact(*reference, limits, &mut reads)?;
            if fact.visibility_scope_id() != input.scope
                || recorded_at.is_some_and(|time| time != fact.recorded_at())
                || fact.corrects().is_some_and(|prior| {
                    !receipt
                        .dependencies()
                        .records()
                        .iter()
                        .any(|dependency| dependency.source_record_id == prior)
                })
            {
                return Err(AdmissionError::StateMismatch);
            }
            recorded_at = Some(fact.recorded_at());
            let next_rank = match fact.payload() {
                FactPayload::Dictionary(entry)
                    if entry.descriptor() == &DictionaryDescriptor::Event(input.event.id) =>
                {
                    mapping = Some(entry.reference());
                    1
                }
                FactPayload::EventPayload(_) => {
                    payload = Some(super::admission::matching_signed_payload(
                        &fact,
                        input.event,
                    )?);
                    2
                }
                FactPayload::EventEligibility(value)
                    if value.event_id == input.event.id
                        && value.body_digest == input.event.body_digest()
                        && value.sig == input.event.sig
                        && value.first_eligible_at == fact.recorded_at()
                        && value.origin == input.origin =>
                {
                    3
                }
                FactPayload::EventReceive(value)
                    if input.observation.is_some_and(|observed| {
                        value.event_id == input.event.id
                            && value.received_at == observed.received_at
                            && value.observer_id == observed.observer_id
                            && value.clock_quality == observed.clock_quality
                    }) =>
                {
                    4
                }
                FactPayload::Provenance(value)
                    if input.observation.is_some_and(|observed| {
                        observed.witness.is_some_and(|witness| {
                            value.event_id == input.event.id
                                && value.received_at == observed.received_at
                                && value.clock_quality == observed.clock_quality
                                && value.observer_id == observed.observer_id
                                && value.endpoint_id == witness.endpoint_id
                                && value.access_scope_id == witness.access_scope_id
                                && value.verification_method_id == witness.verification_method_id
                        })
                    }) =>
                {
                    5
                }
                _ => return Err(AdmissionError::StateMismatch),
            };
            if next_rank <= rank {
                return Err(AdmissionError::StateMismatch);
            }
            rank = next_rank;
        }
        if mapping != payload {
            return Err(AdmissionError::StateMismatch);
        }
        Ok(())
    }
}

impl RetainedLineage {
    pub(super) fn verify_effect_census(
        &self,
        recovery: &IngestRecovery,
        through: u64,
        limits: AdmissionLimits,
        permit: OwnedPermit,
        cancelled: &AtomicBool,
    ) -> Result<(), AdmissionError> {
        self.census()
            .verify_effect_census(recovery, through, limits, &permit, cancelled)
    }
}

impl Census<'_> {
    pub(super) fn verify_effect_census(
        &self,
        recovery: &IngestRecovery,
        through: u64,
        limits: AdmissionLimits,
        permit: &OwnedPermit,
        cancelled: &AtomicBool,
    ) -> Result<(), AdmissionError> {
        self.verify_store_effect_census(&recovery.store, through, limits, permit, cancelled)
    }
    pub(super) fn verify_store_effect_census(
        &self,
        store: &super::IngestStore,
        through: u64,
        limits: AdmissionLimits,
        permit: &OwnedPermit,
        cancelled: &AtomicBool,
    ) -> Result<(), AdmissionError> {
        if permit.request().units().items() == 0
            || permit.request().units().data_bytes()
                < limits.required_dictionary_replay_data_bytes()?
        {
            return Err(AdmissionError::InsufficientReservation);
        }
        let correction_request_limit = self.correction_request_limit()?;
        // Every index must name the exact semantic subject of its pointed fact.
        // Every reverse candidate must be the exact descriptor for that mapping.
        // The structural census counts candidates per allocation domain. Exact
        // descriptor equality permits only one key per reference; together
        // these checks prove one candidate per allocated ref, without a map.
        let cf = store
            .db
            .cf_handle("default")
            .ok_or(AdmissionError::EngineRead)?;
        let mut cursor = store.db.raw_iterator_cf_opt(cf, read_options());
        cursor.seek_to_first();
        while cursor.valid() {
            check(cancelled)?;
            let key = decode_key(
                ColumnFamily::Default,
                cursor.key().ok_or(AdmissionError::StateMismatch)?,
            )
            .map_err(|_| AdmissionError::StateMismatch)?;
            let mut reads = ReadBudget::dictionaries(limits);
            match key {
                IngestKey::DictionaryReverseCandidate {
                    reference,
                    descriptor_digest,
                    ..
                } => {
                    let fact = store
                        .indexed_fact(IngestKey::Dictionary { reference }, limits, &mut reads)?
                        .ok_or(AdmissionError::StateMismatch)?;
                    self.require_verified_fact(&fact, through)?;
                    let FactPayload::Dictionary(entry) = fact.payload() else {
                        return Err(AdmissionError::StateMismatch);
                    };
                    if entry.reference() != reference {
                        return Err(AdmissionError::StateMismatch);
                    }
                    let expected = entry
                        .encode_descriptor(limits.max_batch_bytes)
                        .map_err(|_| AdmissionError::Limit)?;
                    let bytes = cursor.value().ok_or(AdmissionError::StateMismatch)?;
                    if bytes.len() < 2
                        || bytes[..2] != [1, 5]
                        || bytes[2..] != expected
                        || <[u8; 32]>::from(Sha256::digest(&expected))
                            != *descriptor_digest.as_bytes()
                    {
                        return Err(AdmissionError::StateMismatch);
                    }
                }
                IngestKey::Dictionary { .. }
                | IngestKey::EventPayload { .. }
                | IngestKey::EventEligibility { .. }
                | IngestKey::EventReceive { .. }
                | IngestKey::Provenance { .. } => {
                    let fact = store
                        .indexed_fact(key, limits, &mut reads)?
                        .ok_or(AdmissionError::StateMismatch)?;
                    self.require_verified_fact(&fact, through)?;
                    if fact_index(&fact) != key {
                        return Err(AdmissionError::StateMismatch);
                    }
                }
                _ => {}
            }
            cursor.next();
        }
        cursor.status().map_err(|_| AdmissionError::EngineRead)?;
        drop(cursor);

        // Each immutable subject is unique. For mutable receive/provenance
        // subjects, the selected fact must attain the minimum across ALL facts,
        // including epochs older than the immediate predecessor. No map grows.
        let cf = store
            .db
            .cf_handle("facts")
            .ok_or(AdmissionError::EngineRead)?;
        let mut cursor = store.db.raw_iterator_cf_opt(cf, read_options());
        cursor.seek([1, 0x20]);
        while cursor.valid() {
            check(cancelled)?;
            let key = decode_key(
                ColumnFamily::Facts,
                cursor.key().ok_or(AdmissionError::StateMismatch)?,
            )
            .map_err(|_| AdmissionError::StateMismatch)?;
            let IngestKey::Fact { source_record_id } = key else {
                break;
            };
            let fact = decode_fact(
                source_record_id,
                cursor.value().ok_or(AdmissionError::StateMismatch)?,
                limits,
            )?;
            self.require_verified_fact(&fact, through)?;
            let index = fact_index(&fact);
            let reference = fact.fact_ref();
            let time = minimum(&fact);
            let corrects = fact.corrects();
            // Only fixed coordinates survive. The ordinary replay grant must
            // never cover two independently maximum-sized decoded payloads.
            drop(fact);
            let mut reads = ReadBudget::dictionaries(limits);
            let selected = store
                .indexed_fact(index, limits, &mut reads)?
                .ok_or(AdmissionError::StateMismatch)?;
            self.require_verified_fact(&selected, through)?;
            if fact_index(&selected) != index {
                return Err(AdmissionError::StateMismatch);
            }
            match (time, minimum(&selected)) {
                (Some(time), Some(selected_time)) if selected_time < time => {}
                (time, selected_time)
                    if time == selected_time && reference == selected.fact_ref() => {}
                _ => return Err(AdmissionError::StateMismatch),
            }
            let selected_reference = selected.fact_ref();
            drop(selected);
            if time.is_some() {
                // A second disconnected first-observation cannot be justified
                // merely because another fact has a lower current timestamp.
                require_correction_ancestor(
                    store,
                    selected_reference,
                    reference,
                    index,
                    limits,
                    &mut reads,
                    cancelled,
                    correction_request_limit,
                )?;
            }
            if let Some(corrects) = corrects {
                if (corrects.owner_epoch, corrects.sequence)
                    >= (source_record_id.owner_epoch, source_record_id.sequence)
                {
                    return Err(AdmissionError::StateMismatch);
                }
                let bytes = store
                    .admission_value(
                        IngestKey::Fact {
                            source_record_id: corrects,
                        },
                        limits.max_batch_bytes + 34,
                        &mut reads,
                    )?
                    .ok_or(AdmissionError::StateMismatch)?;
                let prior = decode_fact(corrects, &bytes, limits)?;
                self.require_verified_fact(&prior, through)?;
                if fact_index(&prior) != index
                    || !minimum(&prior)
                        .zip(time)
                        .is_some_and(|(old, new)| new < old)
                {
                    return Err(AdmissionError::StateMismatch);
                }
            }
            cursor.next();
        }
        cursor.status().map_err(|_| AdmissionError::EngineRead)?;
        check(cancelled)
    }

    fn require_verified_fact(&self, fact: &Fact, through: u64) -> Result<(), AdmissionError> {
        let source = fact.source_record_id();
        let entry = self
            .entry(source.owner_epoch.get())
            .ok_or(AdmissionError::StateMismatch)?;
        let id = entry.manifest().identity();
        if source.owner_epoch.get() > through
            || source.dataset_id != id.dataset_id
            || source.shard_id != id.shard_id
            || source.sequence.get() > entry.source_through()
        {
            return Err(AdmissionError::StateMismatch);
        }
        Ok(())
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "exact retained request bound and unchanged per-call read/cancellation budgets"
)]
fn require_correction_ancestor(
    store: &super::IngestStore,
    mut selected: FactRef,
    target: FactRef,
    index: IngestKey,
    limits: AdmissionLimits,
    reads: &mut ReadBudget,
    cancelled: &AtomicBool,
    request_limit: u64,
) -> Result<(), AdmissionError> {
    // Positive Event batch checking admits at most one member for this minimum
    // per request. A compacted initial epoch contributes retained science beyond
    // the physical spool: the checked logical bound is at most N+64<=128.
    // Existing ReadBudget bytes/read charges and cancellation remain unchanged;
    // this traversal allocates no set, and checked source order excludes cycles.
    for _ in 0..request_limit {
        check(cancelled)?;
        if selected == target {
            return Ok(());
        }
        let fact = store.admission_fact(selected, limits, reads)?;
        let Some(prior) = fact.corrects() else {
            return Err(AdmissionError::StateMismatch);
        };
        if fact_index(&fact) != index
            || (prior.owner_epoch, prior.sequence)
                >= (
                    selected.source_record_id.owner_epoch,
                    selected.source_record_id.sequence,
                )
        {
            return Err(AdmissionError::StateMismatch);
        }
        drop(fact);
        let bytes = store
            .admission_value(
                IngestKey::Fact {
                    source_record_id: prior,
                },
                limits.max_batch_bytes + 34,
                reads,
            )?
            .ok_or(AdmissionError::StateMismatch)?;
        let fact = decode_fact(prior, &bytes, limits)?;
        if fact_index(&fact) != index {
            return Err(AdmissionError::StateMismatch);
        }
        selected = fact.fact_ref();
    }
    Err(AdmissionError::Limit)
}

fn decode_fact(
    source: SourceRecordId,
    bytes: &[u8],
    limits: AdmissionLimits,
) -> Result<Fact, AdmissionError> {
    if bytes.len() < 34 || bytes[..2] != [1, 0x20] {
        return Err(AdmissionError::StateMismatch);
    }
    let digest = Digest256::from_bytes(bytes[2..34].try_into().expect("fixed digest"));
    let fact = Fact::decode_preimage(&bytes[34..], digest, limits.facts())
        .map_err(super::BootstrapError::from)?;
    if fact.source_record_id() != source {
        return Err(AdmissionError::StateMismatch);
    }
    Ok(fact)
}

fn fact_index(fact: &Fact) -> IngestKey {
    let dataset_id = fact.source_record_id().dataset_id;
    let visibility_scope = fact.visibility_scope_id();
    match fact.payload() {
        FactPayload::Dictionary(entry) => IngestKey::Dictionary {
            reference: entry.reference(),
        },
        FactPayload::EventPayload(payload) => IngestKey::EventPayload {
            dataset_id,
            event_id: payload.body.event_id,
        },
        FactPayload::EventEligibility(value) => IngestKey::EventEligibility {
            dataset_id,
            event_id: value.event_id,
            visibility_scope,
        },
        FactPayload::EventReceive(value) => IngestKey::EventReceive {
            dataset_id,
            event_id: value.event_id,
            visibility_scope,
        },
        FactPayload::Provenance(value) => IngestKey::Provenance {
            dataset_id,
            event_id: value.event_id,
            endpoint_id: value.endpoint_id,
            observer_id: value.observer_id,
            access_scope_id: value.access_scope_id,
            visibility_scope_id: visibility_scope,
        },
    }
}
fn minimum(fact: &Fact) -> Option<ObservedTime> {
    match fact.payload() {
        FactPayload::EventReceive(value) => Some(value.received_at),
        FactPayload::Provenance(value) => Some(value.received_at),
        _ => None,
    }
}
fn check(cancelled: &AtomicBool) -> Result<(), AdmissionError> {
    if cancelled.load(Ordering::Relaxed) {
        Err(AdmissionError::AuthorityUnavailable)
    } else {
        Ok(())
    }
}
