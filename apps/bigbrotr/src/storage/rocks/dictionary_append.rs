//! Fixed-reference dictionary admission after bootstrap. Preparation is not a write.
use super::producer_recovery::ReplayContext;
use super::{
    AdmissionError, AdmissionLimits, BootstrapError, BootstrapLimits, IngestStore,
    PendingBatchHeader, StoredReceipt,
    admission::{ReadBudget, add_dependency},
    bootstrap::{Prepared, frame},
};
use crate::storage::rocks::manifest::IngestManifest;
use crate::{runtime::OwnedPermit, storage::rocks::keys::IngestKey};
use bigbrotr_model::*;
use sha2::{Digest, Sha256};
use std::mem::size_of;

impl AdmissionLimits {
    /// Appends share event bounds, but may emit more than five facts. In addition
    /// to event scratch, retain up to four closure/member vectors during replay,
    /// the larger staged entry table, and overlapping dictionary/input decoding.
    pub fn required_dictionary_data_bytes(self) -> Result<u64, AdmissionError> {
        Ok(self.required_dictionary_replay_data_bytes()?
            + (4 * self.max_batch_bytes + Prepared::entry_allocation_bytes(self.max_dependencies))
                as u64)
    }
    /// Read-only replay retains the original receipt and reconstructs closure and
    /// members. Event decoding scratch already covers the transient native reads;
    /// no staged write table or newly encoded input overlaps this operation.
    pub(super) fn required_dictionary_replay_data_bytes(self) -> Result<u64, AdmissionError> {
        Ok(self.required_data_bytes()? + (3 * self.max_dependencies * size_of::<FactRef>()) as u64)
    }
}

fn dictionary_receipts(
    limits: AdmissionLimits,
) -> bigbrotr_model::receipt_codec::ReceiptDecodeLimits {
    bigbrotr_model::receipt_codec::ReceiptDecodeLimits {
        max_encoded_bytes: limits.max_batch_bytes,
        max_allocation_bytes: 2 * limits.max_dependencies * size_of::<FactRef>(),
        max_batch_records: limits.max_dependencies,
        max_dependencies: limits.max_dependencies,
    }
}

impl IngestStore {
    pub(in crate::storage) fn stage_dictionary_append(
        &mut self,
        request: ProducerRequestId,
        intent: &EvidenceIntent,
        recorded_at: ObservedTime,
        limits: AdmissionLimits,
        permit: &OwnedPermit,
    ) -> Result<super::StagedProducerRequest, AdmissionError> {
        require_permit(permit, limits)?;
        // Validate all allocation and closure choices before the immutable PG
        // reservation, including the actual timestamp's variable-width encoding.
        // This read-only preview emits no fact. The caller retains this exclusive
        // owner and the same recorded_at through both PG phases.
        drop(self.prepare_dictionary_operation(request, intent, recorded_at, limits)?);
        let bytes = encode_input(request, intent, limits)?;
        self.stage_producer_request(request, intent.request_digest(request), &bytes)
            .map_err(AdmissionError::RequestInput)
    }

    /// The connected owner fixes all references before reserving this request.
    /// Changed choices on an ambiguous retry are a conflict, never reallocation.
    #[allow(
        clippy::too_many_arguments,
        reason = "distinct authority and owned operation grant"
    )]
    pub(in crate::storage) fn append_dictionaries(
        &mut self,
        request: ProducerRequestId,
        intent: &EvidenceIntent,
        recorded_at: ObservedTime,
        limits: AdmissionLimits,
        permit: OwnedPermit,
        mut authority: impl FnMut() -> Result<(), AdmissionError>,
    ) -> Result<StoredReceipt, AdmissionError> {
        require_permit(&permit, limits)?;
        authority()?;
        let (writes, receipt) =
            self.prepare_dictionary_operation(request, intent, recorded_at, limits)?;
        let copy = super::NativeOutcomeCopy::receipt(&receipt, &self.manifest)?;
        authority()?;
        if let Some(writes) = writes {
            let input = encode_input(request, intent, limits)?;
            self.retain_request_input(request, &input)
                .map_err(AdmissionError::RequestInput)?;
            drop(input);
            self.commit_prepared(writes, || {
                authority().map_err(|_| BootstrapError::AuthorityUnavailable)
            })?;
        }
        Ok(StoredReceipt::new(receipt, copy, permit))
    }

    fn prepare_dictionary_operation(
        &self,
        request: ProducerRequestId,
        intent: &EvidenceIntent,
        recorded_at: ObservedTime,
        limits: AdmissionLimits,
    ) -> Result<(Option<Prepared>, Receipt), AdmissionError> {
        self.require_admission_owner(Some(request))?;
        self.validate_dictionary_append(request, intent, limits, &self.manifest)?;
        let state = self
            .producer_state(request.producer_slot_id, request.producer_epoch)?
            .ok_or(AdmissionError::StateMismatch)?;
        if state.closing {
            return Err(AdmissionError::ProducerClosing);
        }
        let sequence = request.request_sequence.get();
        if sequence <= state.retired {
            return Err(AdmissionError::RetiredRequest);
        }
        if let Some(receipt) = self.replay_dictionary_append(request, intent, limits)? {
            if sequence > state.issued {
                return Err(AdmissionError::StateMismatch);
            }
            return Ok((None, receipt));
        }
        if sequence <= state.issued {
            return Err(AdmissionError::StateMismatch);
        }
        if state.issued.checked_add(1) != Some(sequence) {
            return Err(AdmissionError::RequestGap);
        }
        if state.issued - state.acknowledged >= limits.max_outstanding_requests {
            return Err(AdmissionError::WindowFull);
        }
        let mut reads = ReadBudget::dictionaries(limits);
        let mut writes = Prepared::new(BootstrapLimits {
            max_mappings: limits.max_dependencies,
            max_batch_bytes: limits.max_batch_bytes,
        })?;
        let mut dependencies = fact_refs(limits)?;
        let mut records = fact_refs(limits)?;
        let mut through = [None; 9];
        let source_key = dictionary_source_key(&self.manifest);
        let mut last = self.admission_source_boundary(limits, &mut reads)?;
        for (index, entry) in intent.dictionaries().iter().enumerate() {
            let reference = entry.reference();
            if let Some(fact) =
                self.indexed_fact(IngestKey::Dictionary { reference }, limits, &mut reads)?
            {
                require_mapping(&fact, entry, intent.visibility_scope_id())?;
                self.require_dictionary_indices(entry, limits, &mut reads)?;
                self.retain_admission_fact(&fact, limits, &mut reads, &mut dependencies)?;
                continue;
            }
            let counter = &mut through[reference.dictionary_kind as usize - 1];
            let high = match *counter {
                Some(value) => value,
                None => self
                    .dictionary_counter(allocation_key(reference), &mut reads)?
                    .unwrap_or(0),
            };
            let next = high.checked_add(1).ok_or(AdmissionError::Limit)?;
            if reference.ordinal.get() != next {
                return Err(AdmissionError::InvalidInput);
            }
            *counter = Some(next);
            last = last.checked_add(1).ok_or(AdmissionError::Limit)?;
            let source_record_id = self.dictionary_source(last)?;
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
                FactPayload::Dictionary(entry.clone()),
            )
            .map_err(|_| AdmissionError::InvalidInput)?;
            for reference in fact.dictionary_refs() {
                // Self-scoped visibility and earlier definitions are supplied by
                // this same atomic batch; later definitions cannot fill a hole.
                if reference != entry.reference()
                    && !intent.dictionaries()[..index]
                        .iter()
                        .any(|prior| prior.reference() == reference)
                {
                    self.dictionary_dependency(reference, limits, &mut reads, &mut dependencies)?;
                }
            }
            let fact_ref = fact.fact_ref();
            let bytes = fact
                .encode_preimage(limits.facts().max_encoded_bytes)
                .map_err(|_| AdmissionError::Limit)?;
            writes.put(
                IngestKey::Fact { source_record_id },
                frame(&[1, 0x20], &[fact_ref.payload_digest.as_bytes(), &bytes])?,
            )?;
            let source = IngestKey::Fact { source_record_id }
                .encode()
                .map_err(|_| AdmissionError::InvalidInput)?;
            writes.put(
                IngestKey::Dictionary { reference },
                frame(
                    &[1, 4],
                    &[&source.as_bytes()[2..], fact_ref.payload_digest.as_bytes()],
                )?,
            )?;
            let (reverse, descriptor) = reverse_entry(entry, limits)?;
            if self
                .admission_value(reverse, limits.max_batch_bytes, &mut reads)?
                .is_some()
            {
                return Err(AdmissionError::StateMismatch);
            }
            writes.put(reverse, frame(&[1, 5], &[&descriptor])?)?;
            add_dependency(&mut dependencies, fact_ref, limits)?;
            records.push(fact_ref);
        }
        // Existing transitive dependencies must stay within the exact target
        // visibility. New members above already have that envelope.
        for reference in &dependencies {
            if !records.contains(reference)
                && self
                    .admission_fact(*reference, limits, &mut reads)?
                    .visibility_scope_id()
                    != intent.visibility_scope_id()
            {
                return Err(AdmissionError::InvalidInput);
            }
        }
        for entry in intent.dictionaries() {
            let reference = entry.reference();
            if let Some(high) = through[reference.dictionary_kind as usize - 1].take() {
                writes.put(
                    allocation_key(reference),
                    frame(&[1, 3], &[&high.to_be_bytes()])?,
                )?;
            }
        }
        dependencies.sort_unstable_by_key(|r| r.source_record_id);
        let dependencies =
            SourceDependencies::new(self.manifest.identity().dataset_id, dependencies)
                .map_err(|_| AdmissionError::StateMismatch)?;
        let context = dictionary_context(&self.manifest);
        let receipt = if records.is_empty() {
            Receipt::already_durable(
                request,
                intent.request_digest(request),
                context,
                dependencies,
            )
        } else {
            let batch = AdmissionBatch::new(records).map_err(|_| AdmissionError::StateMismatch)?;
            writes.put(source_key, frame(&[1, 2], &[&last.to_be_bytes()])?)?;
            writes.put(
                IngestKey::PendingBatch {
                    first: batch.first(),
                },
                PendingBatchHeader::from_batch(&batch).encode().to_vec(),
            )?;
            Receipt::new_facts(
                request,
                intent.request_digest(request),
                context,
                batch,
                dependencies,
            )
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
            frame(
                &[1, 0x31],
                &[
                    &sequence.to_be_bytes(),
                    &state.acknowledged.to_be_bytes(),
                    &state.retired.to_be_bytes(),
                ],
            )?,
        )?;
        Ok((Some(writes), receipt))
    }

    /// Positive historical resolution, including while the producer is closing.
    /// No input staging, fresh allocation or current-policy reinterpretation.
    pub(super) fn replay_dictionary_append(
        &self,
        request: ProducerRequestId,
        intent: &EvidenceIntent,
        limits: AdmissionLimits,
    ) -> Result<Option<Receipt>, AdmissionError> {
        self.replay_dictionary_append_in(
            request,
            intent,
            limits,
            ReplayContext {
                manifest: &self.manifest,
                current_manifest: &self.manifest,
            },
            None,
        )
    }

    pub(super) fn replay_dictionary_append_in(
        &self,
        request: ProducerRequestId,
        intent: &EvidenceIntent,
        limits: AdmissionLimits,
        context: ReplayContext<'_>,
        lineage: Option<&super::RetainedLineage>,
    ) -> Result<Option<Receipt>, AdmissionError> {
        if &self.manifest != context.current_manifest {
            return Err(AdmissionError::StateMismatch);
        }
        self.validate_dictionary_append_context(
            request,
            intent,
            limits,
            context.manifest,
            lineage,
        )?;
        let mut reads = ReadBudget::dictionaries(limits);
        let Some(value) = self.admission_value(
            IngestKey::Request {
                request_id: request,
            },
            limits.max_batch_bytes,
            &mut reads,
        )?
        else {
            return Ok(None);
        };
        let receipt = Receipt::decode_persistent(&value, dictionary_receipts(limits))
            .map_err(BootstrapError::from)?;
        if receipt.request_id() != request
            || receipt.protection_context() != &dictionary_context(context.manifest)
        {
            return Err(AdmissionError::StateMismatch);
        }
        if receipt.request_digest() != intent.request_digest(request) {
            return Err(AdmissionError::RequestConflict);
        }
        let mut dependencies = fact_refs(limits)?;
        let mut members = fact_refs(limits)?;
        let mut time = None;
        for entry in intent.dictionaries() {
            let fact = self
                .indexed_fact(
                    IngestKey::Dictionary {
                        reference: entry.reference(),
                    },
                    limits,
                    &mut reads,
                )?
                .ok_or(AdmissionError::StateMismatch)?;
            require_mapping(&fact, entry, intent.visibility_scope_id())?;
            self.require_dictionary_indices(entry, limits, &mut reads)?;
            self.retain_admission_fact(&fact, limits, &mut reads, &mut dependencies)?;
            if let Some(batch) = receipt.batch() {
                let id = fact.source_record_id();
                let first = batch.first();
                let same_stream = id.dataset_id == first.dataset_id
                    && id.shard_id == first.shard_id
                    && id.owner_epoch == first.owner_epoch;
                if same_stream && id.sequence >= first.sequence {
                    if id.sequence > batch.last().sequence
                        || time.is_some_and(|t| t != fact.recorded_at())
                    {
                        return Err(AdmissionError::StateMismatch);
                    }
                    time = Some(fact.recorded_at());
                    members.push(fact.fact_ref());
                }
            }
        }
        for reference in &dependencies {
            if self
                .admission_fact(*reference, limits, &mut reads)?
                .visibility_scope_id()
                != intent.visibility_scope_id()
            {
                return Err(AdmissionError::StateMismatch);
            }
        }
        dependencies.sort_unstable_by_key(|r| r.source_record_id);
        if dependencies != receipt.dependencies().records() {
            return Err(AdmissionError::StateMismatch);
        }
        if let Some(batch) = receipt.batch() {
            if members != batch.records() {
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
            if pending != PendingBatchHeader::from_batch(batch).encode() {
                return Err(AdmissionError::StateMismatch);
            }
            if self
                .dictionary_counter(dictionary_source_key(context.manifest), &mut reads)?
                .is_none_or(|high| high < batch.last().sequence.get())
            {
                return Err(AdmissionError::StateMismatch);
            }
        }
        Ok(Some(receipt))
    }

    fn validate_dictionary_append(
        &self,
        request: ProducerRequestId,
        intent: &EvidenceIntent,
        limits: AdmissionLimits,
        manifest: &IngestManifest,
    ) -> Result<(), AdmissionError> {
        self.validate_dictionary_append_context(request, intent, limits, manifest, None)
    }

    fn validate_dictionary_append_context(
        &self,
        request: ProducerRequestId,
        intent: &EvidenceIntent,
        limits: AdmissionLimits,
        manifest: &IngestManifest,
        lineage: Option<&super::RetainedLineage>,
    ) -> Result<(), AdmissionError> {
        limits.required_dictionary_data_bytes()?;
        if self.write_uncertain {
            return Err(AdmissionError::CommitUnknown);
        }
        let identity = manifest.identity();
        let supported = match identity.owner_epoch.get() {
            1 => request.producer_epoch.get() == 1 && request.request_sequence.get() > 1,
            2 => manifest.key_schema() == 2 && request.producer_epoch.get() == 2,
            epoch => {
                manifest.key_schema() == 3
                    && request.producer_epoch.get() == epoch
                    && lineage.is_some_and(|lineage| {
                        lineage.native_head().manifest() == &self.manifest
                            && lineage
                                .entry(epoch)
                                .is_some_and(|entry| entry.manifest() == manifest)
                    })
            }
        };
        if !supported
            || intent.admission_policy_version() != 1
            || intent.visibility_scope_id().dataset_id != identity.dataset_id
        {
            return Err(AdmissionError::InvalidInput);
        }
        if intent.dictionaries().len() > limits.max_dependencies
            || intent.encoded_intent_bytes() > limits.max_batch_bytes
        {
            return Err(AdmissionError::Limit);
        }
        for entry in intent.dictionaries() {
            let reference = entry.reference();
            if reference.dictionary_kind == DictionaryKind::Event
                || manifest
                    .allocation_namespaces()
                    .domain(reference.dictionary_kind)
                    != Some(reference.allocation_domain)
            {
                return Err(AdmissionError::InvalidInput);
            }
            if let DictionaryDescriptor::Endpoint(endpoint) = entry.descriptor() {
                let normalized = bigbrotr_protocol::endpoint::Endpoint::parse(endpoint.url())
                    .map_err(|_| AdmissionError::InvalidInput)?;
                if normalized.as_str() != endpoint.url() {
                    return Err(AdmissionError::InvalidInput);
                }
            }
        }
        Ok(())
    }

    pub(super) fn require_dictionary_indices(
        &self,
        entry: &DictionaryEntry,
        limits: AdmissionLimits,
        reads: &mut ReadBudget,
    ) -> Result<(), AdmissionError> {
        let (reverse, descriptor) = reverse_entry(entry, limits)?;
        let value = self
            .admission_value(reverse, limits.max_batch_bytes, reads)?
            .ok_or(AdmissionError::StateMismatch)?;
        if value.len() < 2 || value[..2] != [1, 5] || value[2..] != descriptor {
            return Err(AdmissionError::StateMismatch);
        }
        if self
            .dictionary_counter(allocation_key(entry.reference()), reads)?
            .is_none_or(|high| high < entry.reference().ordinal.get())
        {
            return Err(AdmissionError::StateMismatch);
        }
        Ok(())
    }
    pub(super) fn dictionary_counter(
        &self,
        key: IngestKey,
        reads: &mut ReadBudget,
    ) -> Result<Option<u64>, AdmissionError> {
        self.admission_value(key, 10, reads)?
            .map(|value| {
                if value.len() != 10 || value[..2] != key.domain().prefix() {
                    return Err(AdmissionError::StateMismatch);
                }
                let high = u64::from_be_bytes(value[2..].try_into().expect("checked length"));
                if high == 0 {
                    return Err(AdmissionError::StateMismatch);
                }
                Ok(high)
            })
            .transpose()
    }
    fn dictionary_source(&self, sequence: u64) -> Result<SourceRecordId, AdmissionError> {
        let identity = self.manifest.identity();
        Ok(SourceRecordId {
            dataset_id: identity.dataset_id,
            shard_id: identity.shard_id,
            owner_epoch: identity.owner_epoch,
            sequence: Sequence::new(sequence).map_err(|_| AdmissionError::Limit)?,
        })
    }
}

fn require_permit(permit: &OwnedPermit, limits: AdmissionLimits) -> Result<(), AdmissionError> {
    if permit.request().units().data_bytes() < limits.required_dictionary_data_bytes()?
        || permit.request().units().native_bytes() < limits.required_native_bytes()?
        || permit.request().units().items() == 0
    {
        return Err(AdmissionError::InsufficientReservation);
    }
    Ok(())
}
fn fact_refs(limits: AdmissionLimits) -> Result<Vec<FactRef>, AdmissionError> {
    let mut result = Vec::new();
    result
        .try_reserve_exact(limits.max_dependencies)
        .map_err(|_| AdmissionError::Allocation)?;
    Ok(result)
}
fn allocation_key(reference: QualifiedRef) -> IngestKey {
    IngestKey::AllocationStream {
        dataset_id: reference.dataset_id,
        dictionary_kind: reference.dictionary_kind,
        allocation_domain: reference.allocation_domain,
    }
}
fn reverse_entry(
    entry: &DictionaryEntry,
    limits: AdmissionLimits,
) -> Result<(IngestKey, Vec<u8>), AdmissionError> {
    let descriptor = entry
        .encode_descriptor(limits.max_batch_bytes)
        .map_err(|_| AdmissionError::Limit)?;
    let reference = entry.reference();
    Ok((
        IngestKey::DictionaryReverseCandidate {
            dataset_id: reference.dataset_id,
            dictionary_kind: reference.dictionary_kind,
            descriptor_digest: Digest256::from_bytes(Sha256::digest(&descriptor).into()),
            reference,
        },
        descriptor,
    ))
}
fn require_mapping(
    fact: &Fact,
    entry: &DictionaryEntry,
    scope: QualifiedRef,
) -> Result<(), AdmissionError> {
    if fact.visibility_scope_id() != scope
        || !matches!(fact.payload(), FactPayload::Dictionary(found) if found == entry)
    {
        return Err(AdmissionError::StateMismatch);
    }
    Ok(())
}
fn encode_input(
    request: ProducerRequestId,
    intent: &EvidenceIntent,
    limits: AdmissionLimits,
) -> Result<Vec<u8>, AdmissionError> {
    intent
        .encode_request_preimage(request, limits.max_batch_bytes)
        .map_err(|error| match error {
            EvidenceIntentError::AllocationFailed => AdmissionError::Allocation,
            _ => AdmissionError::Limit,
        })
}

fn dictionary_source_key(manifest: &IngestManifest) -> IngestKey {
    let identity = manifest.identity();
    IngestKey::SourceStream {
        dataset_id: identity.dataset_id,
        shard_id: identity.shard_id,
        owner_epoch: identity.owner_epoch,
    }
}

fn dictionary_context(manifest: &IngestManifest) -> ProtectionContext {
    let identity = manifest.identity();
    ProtectionContext {
        dataset_id: identity.dataset_id,
        topology_generation: identity.topology_generation,
        protection_class: ProtectionClass::LocalSynced,
        recovery_generation: identity.recovery_generation,
        policy_epoch: identity.policy_epoch,
    }
}
