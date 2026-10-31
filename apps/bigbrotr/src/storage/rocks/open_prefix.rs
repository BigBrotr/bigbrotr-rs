//! Initial base-plus-tail proof under the actual continuously excluded owner.
//! These readers do not authorize a write, ACK, unlink or SQL finalization.
use super::{
    AdmissionError, AdmissionLimits, AdmissionOutcome, BootstrapError, IngestStore,
    NativeOutcomeCopy, PendingBatchHeader, PendingReadLimits, ProducerRecoveryLimits,
    admission::{ReadBudget, verify_event},
    producer_closing::ProducerState,
    producer_recovery::ReplayContext,
    request_inputs::ClosedInputInventory,
};
use crate::{
    runtime::{OwnedPermit, ResourceBudget, ResourceRequest},
    storage::producer_prefix::{self, Base, BaseRef, Context},
};
use bigbrotr_model::{
    Digest256, Fact, ProducerEpoch, ProducerRequestId, ProducerSlotId, Sequence, SourceRecordId,
    codec::{FactDecodeLimits, request::MutationIntent},
};
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicBool, Ordering};

#[path = "open_prefix_retirement.rs"]
mod retirement;
pub(in crate::storage) use retirement::{PrefixRetirement, UnlinkedPrefix};

pub(in crate::storage) struct NativePrefixRead<'a> {
    store: &'a IngestStore,
    slot: ProducerSlotId,
    state: ProducerState,
    root: (u64, u64),
    sequence: u64,
    inventory: ClosedInputInventory,
    prior: Option<&'a Base>,
    requests: u64,
    request_chain: [u8; 32],
    current_source: u64,
    source: u64,
    batches: u64,
    science: [u8; 32],
    _scratch: &'a OwnedPermit,
}

/// Native-owned positive original, retaining its borrowed effect allocation.
pub(in crate::storage) struct PrefixOriginal<'a> {
    pub copy: NativeOutcomeCopy,
    pub input_bytes: u32,
    pub event: bool,
    source_after: u64,
    root: (u64, u64),
    sequence: u64,
    custody: std::sync::Arc<()>,
    _effects: &'a OwnedPermit,
}

pub(in crate::storage) fn pending_limits() -> PendingReadLimits {
    PendingReadLimits {
        max_records: 64,
        max_encoded_bytes: 1024 * 1024,
        fact: FactDecodeLimits {
            max_encoded_bytes: 1024 * 1024,
            max_allocation_bytes: 4 * 1024 * 1024,
            max_tag_nodes: 1024 * 1024,
            max_string_bytes: 1024 * 1024,
        },
    }
}

impl IngestStore {
    /// Actual native watermarks for the private fresh SQL-resolution read. This
    /// observation supplies neither a retirement plan nor reusable capacity.
    pub(in crate::storage) fn prefix_observation(
        &self,
        slot: ProducerSlotId,
    ) -> Result<(u64, u64, u64, Option<producer_prefix::NativeFloor>), AdmissionError> {
        if self.write_uncertain {
            return Err(AdmissionError::CommitUnknown);
        }
        let epoch = ProducerEpoch::new(1).expect("initial prefix");
        let state = self
            .producer_state(slot, epoch)?
            .ok_or(AdmissionError::StateMismatch)?;
        self.require_retained_state(slot, epoch, state)?;
        Ok((
            state.issued,
            state.acknowledged,
            state.retired,
            self.open_prefix_floor(slot, epoch)?,
        ))
    }
    pub(in crate::storage) fn prefix_context(&self, slot: ProducerSlotId) -> Context {
        Context {
            dataset: *self.manifest.identity().dataset_id.as_bytes(),
            slot: *slot.as_bytes(),
            epoch: self.manifest.identity().owner_epoch.get(),
            manifest: Sha256::digest(self.manifest.encode().as_bytes()).into(),
        }
    }
    pub(in crate::storage) fn prefix_read<'a>(
        &'a self,
        slot: ProducerSlotId,
        prior: Option<&'a Base>,
        scratch: &'a OwnedPermit,
        effects: &OwnedPermit,
        budget: &ResourceBudget,
        cancelled: &AtomicBool,
        mut authority: impl FnMut() -> Result<(), AdmissionError>,
    ) -> Result<NativePrefixRead<'a>, AdmissionError> {
        authority()?;
        check(cancelled)?;
        if scratch.request().units().items() == 0
            || scratch.request().units().data_bytes() < 128 * 1024
        {
            return Err(AdmissionError::InsufficientReservation);
        }
        if self.write_uncertain {
            return Err(AdmissionError::CommitUnknown);
        }
        let root = self.handoff_root_identity()?;
        let sequence = self.db.latest_sequence_number();
        let epoch = ProducerEpoch::new(1).expect("initial epoch");
        let state = self
            .producer_state(slot, epoch)?
            .ok_or(AdmissionError::StateMismatch)?;
        self.require_retained_state(slot, epoch, state)?;
        let context = self.prefix_context(slot);
        match (self.open_prefix_floor(slot, epoch)?, prior) {
            (None, None) if state.retired == 0 => {}
            (Some(floor), Some(base)) if base.context == context => {
                floor
                    .require_base(base)
                    .map_err(|_| AdmissionError::StateMismatch)?;
            }
            _ => return Err(AdmissionError::StateMismatch),
        }
        let inventory = self.prefix_input_inventory(
            slot,
            state.retired,
            state.issued,
            state.retired,
            cancelled,
        )?;
        let source = self.inspect_open_prefix_census(
            slot,
            AdmissionLimits {
                max_dependencies: 256,
                ..AdmissionLimits::default()
            },
            effects,
            cancelled,
        )?;
        let mut science = context.source_seed();
        let mut batches = 0_u64;
        let mut next = 1_u64;
        if let Some(base) = prior {
            if base.source > source
                || (base.source == 0 && (base.batches != 0 || base.science != science))
            {
                return Err(AdmissionError::StateMismatch);
            }
        }
        let mut prior_seen = prior.is_none_or(|base| base.source == 0);
        while next <= source {
            authority()?;
            check(cancelled)?;
            let limits = pending_limits();
            let permit = budget
                .try_acquire(
                    ResourceRequest::data(
                        limits
                            .required_data_bytes()
                            .map_err(|_| AdmissionError::InvalidLimits)?,
                        1,
                        0,
                    )
                    .map_err(|_| AdmissionError::InvalidLimits)?,
                )
                .map_err(|_| AdmissionError::InsufficientReservation)?;
            let first = SourceRecordId {
                dataset_id: self.manifest.identity().dataset_id,
                shard_id: self.manifest.identity().shard_id,
                owner_epoch: self.manifest.identity().owner_epoch,
                sequence: Sequence::new(next).map_err(|_| AdmissionError::Limit)?,
            };
            let pending = self
                .read_pending_batch(first, limits, permit, cancelled)
                .map_err(|_| AdmissionError::StateMismatch)?
                .ok_or(AdmissionError::StateMismatch)?;
            let header = PendingBatchHeader::from_batch(pending.batch());
            if header.last().sequence.get() > source {
                return Err(AdmissionError::StateMismatch);
            }
            // Independently check every Fact's reference closure, including
            // scientific records no longer mentioned by progress dependencies.
            let admission = AdmissionLimits {
                max_dependencies: 256,
                ..AdmissionLimits::default()
            };
            for (bytes, reference) in pending.records().iter().zip(pending.batch().records()) {
                authority()?;
                check(cancelled)?;
                let fact = Fact::decode_preimage(bytes, reference.payload_digest, limits.fact)
                    .map_err(BootstrapError::from)?;
                let mut dependencies = Vec::new();
                dependencies
                    .try_reserve_exact(admission.max_dependencies)
                    .map_err(|_| AdmissionError::Allocation)?;
                let mut reads = ReadBudget::dictionaries(admission);
                for reference in fact.dictionary_refs() {
                    self.dictionary_dependency(
                        reference,
                        admission,
                        &mut reads,
                        &mut dependencies,
                    )?;
                }
                if dependencies.iter().any(|reference| {
                    let id = reference.source_record_id;
                    id.dataset_id != first.dataset_id
                        || id.shard_id != first.shard_id
                        || id.owner_epoch != first.owner_epoch
                        || id.sequence.get() > source
                }) {
                    return Err(AdmissionError::StateMismatch);
                }
            }
            science = producer_prefix::next_batch(
                science,
                next,
                header.last().sequence.get(),
                *header.digest().as_bytes(),
            );
            batches = batches.checked_add(1).ok_or(AdmissionError::Limit)?;
            if let Some(base) = prior.filter(|base| base.source == header.last().sequence.get()) {
                if base.batches != batches || base.science != science {
                    return Err(AdmissionError::StateMismatch);
                }
                prior_seen = true;
            }
            next = header
                .last()
                .sequence
                .get()
                .checked_add(1)
                .ok_or(AdmissionError::Limit)?;
        }
        if !prior_seen {
            return Err(AdmissionError::StateMismatch);
        }
        let read = NativePrefixRead {
            store: self,
            slot,
            state,
            root,
            sequence,
            inventory,
            prior,
            requests: state.retired,
            request_chain: prior.map_or_else(|| context.request_seed(), |base| base.requests),
            current_source: prior.map_or(0, |base| base.source),
            source,
            batches,
            science,
            _scratch: scratch,
        };
        read.validate(cancelled)?;
        authority()?;
        Ok(read)
    }
}

impl NativePrefixRead<'_> {
    /// Freeze only the exact complete native view. PostgreSQL must still bind
    /// the captured progress head and confirm its prepared slot before effects.
    pub(in crate::storage) fn retirement(
        self,
        base: Base,
        metadata: OwnedPermit,
        cancelled: &AtomicBool,
    ) -> Result<PrefixRetirement, AdmissionError> {
        PrefixRetirement::capture(self, base, metadata, cancelled)
    }
    pub fn context(&self) -> Context {
        self.store.prefix_context(self.slot)
    }
    pub fn issued(&self) -> u64 {
        self.state.issued
    }
    pub fn acknowledged(&self) -> u64 {
        self.state.acknowledged
    }
    pub fn retired(&self) -> u64 {
        self.state.retired
    }
    pub fn closing(&self) -> bool {
        self.state.closing
    }
    pub fn source(&self) -> u64 {
        self.source
    }
    pub fn batches(&self) -> u64 {
        self.batches
    }
    pub fn science(&self) -> [u8; 32] {
        self.science
    }
    pub fn request_chain(&self) -> Result<[u8; 32], AdmissionError> {
        if self.requests != self.state.issued || self.current_source != self.source {
            return Err(AdmissionError::StateMismatch);
        }
        Ok(self.request_chain)
    }
    pub fn prior_reference(&self) -> Result<Option<BaseRef>, AdmissionError> {
        self.prior
            .map(|base| base.reference().map_err(|_| AdmissionError::StateMismatch))
            .transpose()
    }
    pub fn validate(&self, cancelled: &AtomicBool) -> Result<(), AdmissionError> {
        check(cancelled)?;
        if self.store.write_uncertain {
            return Err(AdmissionError::CommitUnknown);
        }
        if self.store.handoff_root_identity()? != self.root
            || self.store.db.latest_sequence_number() != self.sequence
        {
            return Err(AdmissionError::StateMismatch);
        }
        self.store
            .require_closed_input_inventory(&self.inventory, cancelled)?;
        if self.store.handoff_root_identity()? != self.root
            || self.store.db.latest_sequence_number() != self.sequence
        {
            return Err(AdmissionError::StateMismatch);
        }
        check(cancelled)
    }
    pub fn resolve<'a>(
        &self,
        request: ProducerRequestId,
        digest: Digest256,
        limits: ProducerRecoveryLimits,
        input: OwnedPermit,
        effects: &'a OwnedPermit,
        cancelled: &AtomicBool,
        mut authority: impl FnMut() -> Result<(), AdmissionError>,
    ) -> Result<PrefixOriginal<'a>, AdmissionError> {
        authority()?;
        self.validate(cancelled)?;
        if request.producer_slot_id != self.slot
            || request.producer_epoch.get() != 1
            || request.request_sequence.get() <= self.state.retired
            || request.request_sequence.get() > self.state.issued
            || effects.request().units().items() == 0
            || effects.request().units().data_bytes() < limits.required_data_bytes()?
        {
            return Err(AdmissionError::InvalidInput);
        }
        let original = self
            .store
            .read_request_input(request, digest, limits.input, input)
            .map_err(AdmissionError::RequestInput)?;
        let input_bytes = original.encoded_bytes();
        let (_, intent, _input) = original.into_parts();
        let event = matches!(&intent, MutationIntent::Event(_));
        let context = ReplayContext {
            manifest: &self.store.manifest,
            current_manifest: &self.store.manifest,
        };
        let (outcome, copy) = match intent {
            MutationIntent::Evidence(intent) => {
                let receipt = if request.request_sequence.get() == 1 {
                    self.store
                        .replay_bootstrap(request, &intent, limits.bootstrap)?
                } else {
                    self.store
                        .replay_dictionary_append(request, &intent, limits.event)?
                }
                .ok_or(AdmissionError::StateMismatch)?;
                let copy = NativeOutcomeCopy::receipt(&receipt, &self.store.manifest)?;
                (AdmissionOutcome::Durable(receipt), copy)
            }
            MutationIntent::Event(intent) => {
                let scope = intent.visibility_scope_id();
                let origin = intent.origin();
                let observation = intent.observation().copied();
                let verified = verify_event(intent.into_event(), 4 * 1024 * 1024)?;
                let observed = self
                    .store
                    .replay_event_in(
                        request,
                        digest,
                        verified.as_ref().ok().map(|event| {
                            super::receipt_effects::ReplayEventIntent {
                                event: event.event(),
                                scope,
                                origin,
                                observation,
                            }
                        }),
                        None,
                        limits.event,
                        &mut ReadBudget::new(limits.event),
                        context,
                    )?
                    .ok_or(AdmissionError::StateMismatch)?;
                if let AdmissionOutcome::Durable(receipt) = &observed.0 {
                    let event = verified
                        .as_ref()
                        .map_err(|_| AdmissionError::StateMismatch)?;
                    self.store.require_lineage_event_batch(
                        receipt,
                        super::receipt_effects::ReplayEventIntent {
                            event: event.event(),
                            scope,
                            origin,
                            observation,
                        },
                        limits.event,
                    )?;
                }
                observed
            }
        };
        let mut source_after = self.current_source;
        if let AdmissionOutcome::Durable(receipt) = &outcome {
            let id = self.store.manifest.identity();
            if receipt.protection_context().topology_generation != id.topology_generation
                || receipt.protection_context().recovery_generation != id.recovery_generation
            {
                return Err(AdmissionError::StateMismatch);
            }
            if let Some(batch) = receipt.batch() {
                let first = batch.first();
                if first.dataset_id != id.dataset_id
                    || first.shard_id != id.shard_id
                    || first.owner_epoch != id.owner_epoch
                    || self.current_source.checked_add(1) != Some(first.sequence.get())
                {
                    return Err(AdmissionError::StateMismatch);
                }
                source_after = batch.last().sequence.get();
            }
            if receipt.dependencies().records().iter().any(|reference| {
                let source = reference.source_record_id;
                source.dataset_id != id.dataset_id
                    || source.shard_id != id.shard_id
                    || source.owner_epoch != id.owner_epoch
                    || source.sequence.get() > source_after
            }) {
                return Err(AdmissionError::StateMismatch);
            }
        }
        self.validate(cancelled)?;
        authority()?;
        Ok(PrefixOriginal {
            copy,
            input_bytes,
            event,
            source_after,
            root: self.root,
            sequence: self.sequence,
            custody: std::sync::Arc::clone(&self.store._reservation.custody),
            _effects: effects,
        })
    }
    /// The connected validator compares the exact locked PG row before feeding
    /// its native original here. A refused row never advances this accumulator.
    pub fn record(&mut self, original: &PrefixOriginal<'_>) -> Result<(), AdmissionError> {
        let request = original.copy.request();
        if request.producer_slot_id != self.slot
            || request.producer_epoch.get() != 1
            || original.root != self.root
            || original.sequence != self.sequence
            || !std::sync::Arc::ptr_eq(&original.custody, &self.store._reservation.custody)
            || self.requests.checked_add(1) != Some(request.request_sequence.get())
            || request.request_sequence.get() > self.state.issued
            || original.source_after > self.source
            || original.source_after < self.current_source
        {
            return Err(AdmissionError::StateMismatch);
        }
        self.request_chain = producer_prefix::next_request(
            self.request_chain,
            request.request_sequence.get(),
            *original.copy.digest().as_bytes(),
            Sha256::digest(original.copy.bytes()).into(),
        );
        self.requests = request.request_sequence.get();
        self.current_source = original.source_after;
        Ok(())
    }
}

fn check(cancelled: &AtomicBool) -> Result<(), AdmissionError> {
    if cancelled.load(Ordering::Relaxed) {
        Err(AdmissionError::AuthorityUnavailable)
    } else {
        Ok(())
    }
}
