//! One immutable compound attempt and optional immutable resolution. Inventory
//! indexes accelerate science lookup; the shared producer ledger owns retries.
use super::{
    AdmissionError, AdmissionLimits, BootstrapError, BootstrapLimits, IngestStore,
    PendingBatchHeader, StoredReceipt,
    admission::{ReadBudget, add_dependency},
    bootstrap::{Prepared, frame},
    evidence_append::{EvidenceHead, EvidenceInput},
    producer_recovery::ReplayContext,
};
use crate::{
    runtime::OwnedPermit,
    storage::rocks::{keys::IngestKey, manifest::IngestManifest},
};
use bigbrotr_model::*;
use std::{mem::size_of, sync::atomic::AtomicBool};

pub(super) fn measurement_index(dataset_id: DatasetId, value: &Measurement) -> IngestKey {
    let attempt_id = value.attempt_id();
    if value.prior_unknown().is_some() {
        IngestKey::MeasurementResolution {
            dataset_id,
            attempt_id,
        }
    } else {
        IngestKey::MeasurementAttempt {
            dataset_id,
            attempt_id,
        }
    }
}

impl IngestStore {
    pub(in crate::storage) fn measurement_pending_reservation(
        &self,
        request: ProducerRequestId,
        intent: &MeasurementEvidenceIntent,
        recorded_at: ObservedTime,
        limits: AdmissionLimits,
        permit: &OwnedPermit,
    ) -> Result<super::PendingReservationRequest, AdmissionError> {
        self.evidence_pending_reservation(
            request,
            EvidenceInput::Measurement(intent),
            recorded_at,
            limits,
            permit,
        )
    }
    #[allow(
        clippy::too_many_arguments,
        reason = "shared staging with explicit successor cancellation"
    )]
    pub(in crate::storage) fn stage_measurement_append(
        &mut self,
        request: ProducerRequestId,
        intent: &MeasurementEvidenceIntent,
        recorded_at: ObservedTime,
        limits: AdmissionLimits,
        permit: &OwnedPermit,
        cancelled: Option<&AtomicBool>,
    ) -> Result<super::StagedProducerRequest, AdmissionError> {
        self.stage_evidence_append_in(
            request,
            EvidenceInput::Measurement(intent),
            recorded_at,
            limits,
            permit,
            cancelled,
        )
    }
    #[allow(
        clippy::too_many_arguments,
        reason = "exact input, owned grant and live authority through synced commit"
    )]
    pub(in crate::storage) fn append_measurement(
        &mut self,
        request: ProducerRequestId,
        intent: &MeasurementEvidenceIntent,
        recorded_at: ObservedTime,
        limits: AdmissionLimits,
        permit: OwnedPermit,
        cancelled: Option<&AtomicBool>,
        authority: impl FnMut() -> Result<(), AdmissionError>,
    ) -> Result<StoredReceipt, AdmissionError> {
        self.append_evidence_in(
            request,
            EvidenceInput::Measurement(intent),
            recorded_at,
            limits,
            permit,
            cancelled,
            authority,
        )
    }

    pub(super) fn prepare_measurement_operation(
        &self,
        request: ProducerRequestId,
        intent: &MeasurementEvidenceIntent,
        recorded_at: ObservedTime,
        limits: AdmissionLimits,
    ) -> Result<(Option<Prepared>, Receipt), AdmissionError> {
        let state = match self.prepare_evidence_head(
            request,
            EvidenceInput::Measurement(intent),
            limits,
        )? {
            EvidenceHead::Replayed(receipt) => return Ok((None, receipt)),
            EvidenceHead::Fresh(state) => state,
        };
        let mut reads = ReadBudget::dictionaries(limits);
        let mut dependencies = fact_refs(limits)?;
        let mut writes = Prepared::new(BootstrapLimits {
            max_mappings: limits.max_dependencies,
            max_batch_bytes: limits.max_batch_bytes,
        })?;
        let value = intent.measurement();
        let key = measurement_index(self.manifest.identity().dataset_id, value);
        let prior = self.measurement_prior(value, limits, &mut reads)?;
        let existing = self.indexed_measurement(key, limits, &mut reads)?;
        let mut batch = None;
        if let Some(fact) = existing {
            require_same_measurement(&fact, intent)?;
            if let Some(prior) = &prior {
                fact.validate_measurement_resolution(prior)
                    .map_err(|_| AdmissionError::StateMismatch)?;
            }
            self.measurement_closure(&fact, limits, &mut reads, &mut dependencies)?;
        } else {
            if value.prior_unknown().is_none()
                && self
                    .indexed_measurement(
                        IngestKey::MeasurementResolution {
                            dataset_id: self.manifest.identity().dataset_id,
                            attempt_id: value.attempt_id(),
                        },
                        limits,
                        &mut reads,
                    )?
                    .is_some()
            {
                return Err(AdmissionError::StateMismatch);
            }
            let last = self
                .admission_source_boundary(limits, &mut reads)?
                .checked_add(1)
                .ok_or(AdmissionError::Limit)?;
            let identity = self.manifest.identity();
            let source_record_id = SourceRecordId {
                dataset_id: identity.dataset_id,
                shard_id: identity.shard_id,
                owner_epoch: identity.owner_epoch,
                sequence: Sequence::new(last).map_err(|_| AdmissionError::Limit)?,
            };
            if self
                .admission_value(
                    IngestKey::Fact { source_record_id },
                    limits.max_batch_bytes,
                    &mut reads,
                )?
                .is_some()
            {
                return Err(AdmissionError::StateMismatch);
            }
            let fact = Fact::new(
                source_record_id,
                intent.visibility_scope_id(),
                recorded_at,
                FactPayload::Measurement(value.clone()),
            )
            .map_err(|_| AdmissionError::InvalidInput)?;
            if let Some(prior) = &prior {
                fact.validate_measurement_resolution(prior)
                    .map_err(|_| AdmissionError::InvalidInput)?;
            }
            self.measurement_closure(&fact, limits, &mut reads, &mut dependencies)?;
            let reference = fact.fact_ref();
            let bytes = fact
                .encode_preimage(limits.facts().max_encoded_bytes)
                .map_err(|_| AdmissionError::Limit)?;
            writes.put(
                IngestKey::Fact { source_record_id },
                frame(&[1, 0x20], &[reference.payload_digest.as_bytes(), &bytes])?,
            )?;
            let source = IngestKey::Fact { source_record_id }
                .encode()
                .map_err(|_| AdmissionError::InvalidInput)?;
            writes.put(
                key,
                frame(
                    &key.domain().prefix(),
                    &[&source.as_bytes()[2..], reference.payload_digest.as_bytes()],
                )?,
            )?;
            let mut records = Vec::new();
            records
                .try_reserve_exact(1)
                .map_err(|_| AdmissionError::Allocation)?;
            records.push(reference);
            let emitted =
                AdmissionBatch::new(records).map_err(|_| AdmissionError::StateMismatch)?;
            writes.put(
                source_key(&self.manifest),
                frame(&[1, 2], &[&last.to_be_bytes()])?,
            )?;
            writes.put(
                IngestKey::PendingBatch {
                    first: emitted.first(),
                },
                PendingBatchHeader::from_batch(&emitted).encode().to_vec(),
            )?;
            batch = Some(emitted);
        }
        if let Some(prior) = &prior {
            self.measurement_closure(prior, limits, &mut reads, &mut dependencies)?;
        }
        self.require_measurement_visibility(
            intent.visibility_scope_id(),
            &dependencies,
            batch.as_ref(),
            limits,
            &mut reads,
        )?;
        dependencies.sort_unstable_by_key(|reference| reference.source_record_id);
        let dependencies =
            SourceDependencies::new(self.manifest.identity().dataset_id, dependencies)
                .map_err(|_| AdmissionError::StateMismatch)?;
        let receipt = match batch {
            Some(batch) => Receipt::new_facts(
                request,
                intent.request_digest(request),
                protection(&self.manifest),
                batch,
                dependencies,
            ),
            None => Receipt::already_durable(
                request,
                intent.request_digest(request),
                protection(&self.manifest),
                dependencies,
            ),
        }
        .map_err(|_| AdmissionError::StateMismatch)?;
        writes.put(
            IngestKey::Request {
                request_id: request,
            },
            receipt
                .encode_persistent(limits.max_batch_bytes)
                .map_err(|_| AdmissionError::Limit)?,
        )?;
        writes.put(
            IngestKey::ProducerState {
                producer_slot_id: request.producer_slot_id,
                producer_epoch: request.producer_epoch,
            },
            self.producer_state_bytes(
                request.producer_slot_id,
                request.producer_epoch,
                super::producer_closing::ProducerState {
                    issued: request.request_sequence.get(),
                    ..state
                },
            )?,
        )?;
        Ok((Some(writes), receipt))
    }

    pub(super) fn validate_measurement_append_context(
        &self,
        request: ProducerRequestId,
        intent: &MeasurementEvidenceIntent,
        limits: AdmissionLimits,
        manifest: &IngestManifest,
        lineage: Option<&super::RetainedLineage>,
    ) -> Result<(), AdmissionError> {
        self.validate_evidence_context(
            request,
            intent.admission_policy_version(),
            intent.visibility_scope_id(),
            limits,
            manifest,
            lineage,
        )?;
        if intent.encoded_intent_bytes() > limits.max_batch_bytes {
            return Err(AdmissionError::Limit);
        }
        // This adapter owns one shard and its verified predecessor lineage. The
        // portable model supports other causal cuts; it does not grant this owner
        // a cross-shard lookup or a new source authority.
        if intent.measurement().prior_unknown().is_some_and(|prior| {
            prior.source_record_id.shard_id != manifest.identity().shard_id
                || prior.source_record_id.owner_epoch > manifest.identity().owner_epoch
        }) {
            return Err(AdmissionError::InvalidInput);
        }
        Ok(())
    }

    pub(super) fn replay_measurement_append_in(
        &self,
        request: ProducerRequestId,
        intent: &MeasurementEvidenceIntent,
        limits: AdmissionLimits,
        context: ReplayContext<'_>,
        lineage: Option<&super::RetainedLineage>,
    ) -> Result<Option<Receipt>, AdmissionError> {
        self.require_pending_accounting()
            .map_err(BootstrapError::from)?;
        if &self.manifest != context.current_manifest {
            return Err(AdmissionError::StateMismatch);
        }
        self.validate_measurement_append_context(
            request,
            intent,
            limits,
            context.manifest,
            lineage,
        )?;
        let mut reads = ReadBudget::dictionaries(limits);
        let Some(bytes) = self.admission_value(
            IngestKey::Request {
                request_id: request,
            },
            limits.max_batch_bytes,
            &mut reads,
        )?
        else {
            return Ok(None);
        };
        let receipt = Receipt::decode_persistent(&bytes, receipt_limits(limits))
            .map_err(BootstrapError::from)?;
        if receipt.request_id() != request
            || receipt.protection_context() != &protection(context.manifest)
        {
            return Err(AdmissionError::StateMismatch);
        }
        if receipt.request_digest() != intent.request_digest(request) {
            return Err(AdmissionError::RequestConflict);
        }
        let key = measurement_index(context.manifest.identity().dataset_id, intent.measurement());
        let fact = self
            .indexed_measurement(key, limits, &mut reads)?
            .ok_or(AdmissionError::StateMismatch)?;
        require_same_measurement(&fact, intent).map_err(|_| AdmissionError::StateMismatch)?;
        let prior = self
            .measurement_prior(intent.measurement(), limits, &mut reads)
            .map_err(|error| match error {
                AdmissionError::EvidenceConflict => AdmissionError::StateMismatch,
                other => other,
            })?;
        if let Some(prior) = &prior {
            fact.validate_measurement_resolution(prior)
                .map_err(|_| AdmissionError::StateMismatch)?;
        }
        let mut dependencies = fact_refs(limits)?;
        self.measurement_closure(&fact, limits, &mut reads, &mut dependencies)?;
        if let Some(prior) = &prior {
            self.measurement_closure(prior, limits, &mut reads, &mut dependencies)?;
        }
        self.require_measurement_visibility(
            intent.visibility_scope_id(),
            &dependencies,
            None,
            limits,
            &mut reads,
        )?;
        dependencies.sort_unstable_by_key(|reference| reference.source_record_id);
        if dependencies != receipt.dependencies().records() {
            return Err(AdmissionError::StateMismatch);
        }
        if let Some(batch) = receipt.batch() {
            let source = fact.source_record_id();
            if batch.records() != [fact.fact_ref()]
                || source.shard_id != context.manifest.identity().shard_id
                || source.owner_epoch != context.manifest.identity().owner_epoch
            {
                return Err(AdmissionError::StateMismatch);
            }
            let pending = self
                .admission_value(
                    IngestKey::PendingBatch {
                        first: batch.first(),
                    },
                    128,
                    &mut reads,
                )?
                .ok_or(AdmissionError::StateMismatch)?;
            if pending != PendingBatchHeader::from_batch(batch).encode()
                || self
                    .dictionary_counter(source_key(context.manifest), &mut reads)?
                    .is_none_or(|high| high < source.sequence.get())
            {
                return Err(AdmissionError::StateMismatch);
            }
        }
        Ok(Some(receipt))
    }

    fn indexed_measurement(
        &self,
        key: IngestKey,
        limits: AdmissionLimits,
        reads: &mut ReadBudget,
    ) -> Result<Option<Fact>, AdmissionError> {
        let Some(fact) = self.indexed_fact(key, limits, reads)? else {
            return Ok(None);
        };
        let FactPayload::Measurement(value) = fact.payload() else {
            return Err(AdmissionError::StateMismatch);
        };
        if measurement_index(fact.source_record_id().dataset_id, value) != key
            || fact.source_record_id().shard_id != self.manifest.identity().shard_id
        {
            return Err(AdmissionError::StateMismatch);
        }
        Ok(Some(fact))
    }
    fn measurement_prior(
        &self,
        value: &Measurement,
        limits: AdmissionLimits,
        reads: &mut ReadBudget,
    ) -> Result<Option<Fact>, AdmissionError> {
        let Some(reference) = value.prior_unknown() else {
            return Ok(None);
        };
        let key = IngestKey::MeasurementAttempt {
            dataset_id: self.manifest.identity().dataset_id,
            attempt_id: value.attempt_id(),
        };
        let prior = self
            .indexed_measurement(key, limits, reads)?
            .ok_or(AdmissionError::EvidenceConflict)?;
        if prior.fact_ref() != reference
            || !matches!(prior.payload(), FactPayload::Measurement(v) if matches!(v.state(), bigbrotr_model::measurement::MeasurementState::ExecutionUnknown | bigbrotr_model::measurement::MeasurementState::CompletionUnknown(_)))
        {
            return Err(AdmissionError::EvidenceConflict);
        }
        Ok(Some(prior))
    }
    fn measurement_closure(
        &self,
        fact: &Fact,
        limits: AdmissionLimits,
        reads: &mut ReadBudget,
        dependencies: &mut Vec<FactRef>,
    ) -> Result<(), AdmissionError> {
        let FactPayload::Measurement(value) = fact.payload() else {
            return Err(AdmissionError::StateMismatch);
        };
        add_dependency(dependencies, fact.fact_ref(), limits)?;
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(7)
            .map_err(|_| AdmissionError::Allocation)?;
        for reference in fact.dictionary_refs() {
            if entries
                .iter()
                .any(|entry: &DictionaryEntry| entry.reference() == reference)
            {
                continue;
            }
            let entry = self.dictionary_dependency(reference, limits, reads, dependencies)?;
            if matches!(entry.descriptor(), DictionaryDescriptor::AccessScope(access) if access.visibility_scope_id != fact.visibility_scope_id())
            {
                return Err(AdmissionError::InvalidInput);
            }
            entries.push(entry);
        }
        value
            .validate_dictionary_dependencies(|reference| {
                entries.iter().find(|entry| entry.reference() == reference)
            })
            .map_err(|_| AdmissionError::InvalidInput)
    }
    fn require_measurement_visibility(
        &self,
        scope: QualifiedRef,
        dependencies: &[FactRef],
        fresh: Option<&AdmissionBatch>,
        limits: AdmissionLimits,
        reads: &mut ReadBudget,
    ) -> Result<(), AdmissionError> {
        for reference in dependencies {
            if fresh.is_some_and(|batch| batch.records().contains(reference)) {
                continue;
            }
            if self
                .admission_fact(*reference, limits, reads)?
                .visibility_scope_id()
                != scope
            {
                return Err(AdmissionError::InvalidInput);
            }
        }
        Ok(())
    }
}
fn require_same_measurement(
    fact: &Fact,
    intent: &MeasurementEvidenceIntent,
) -> Result<(), AdmissionError> {
    if fact.visibility_scope_id() != intent.visibility_scope_id()
        || !matches!(fact.payload(), FactPayload::Measurement(value) if value == intent.measurement())
    {
        return Err(AdmissionError::EvidenceConflict);
    }
    Ok(())
}
fn fact_refs(limits: AdmissionLimits) -> Result<Vec<FactRef>, AdmissionError> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(limits.max_dependencies)
        .map_err(|_| AdmissionError::Allocation)?;
    Ok(values)
}
fn source_key(manifest: &IngestManifest) -> IngestKey {
    let identity = manifest.identity();
    IngestKey::SourceStream {
        dataset_id: identity.dataset_id,
        shard_id: identity.shard_id,
        owner_epoch: identity.owner_epoch,
    }
}
fn protection(manifest: &IngestManifest) -> ProtectionContext {
    let identity = manifest.identity();
    ProtectionContext {
        dataset_id: identity.dataset_id,
        topology_generation: identity.topology_generation,
        protection_class: ProtectionClass::LocalSynced,
        recovery_generation: identity.recovery_generation,
        policy_epoch: identity.policy_epoch,
    }
}
fn receipt_limits(limits: AdmissionLimits) -> bigbrotr_model::receipt_codec::ReceiptDecodeLimits {
    bigbrotr_model::receipt_codec::ReceiptDecodeLimits {
        max_encoded_bytes: limits.max_batch_bytes,
        max_allocation_bytes: 2 * limits.max_dependencies * size_of::<FactRef>(),
        max_batch_records: 1,
        max_dependencies: limits.max_dependencies,
    }
}
