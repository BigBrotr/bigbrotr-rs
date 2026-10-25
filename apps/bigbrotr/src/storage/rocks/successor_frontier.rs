//! Privately promoted positive prefix and exact locally admitted progress.
//! This is not a refreshed RetainedLineage and has no portable constructor.
use super::super::{AdmissionError, IngestStore, SuccessorHandoff};
use super::*;
use bigbrotr_model::{ProducerRequestId, SourceRecordId};

pub(in crate::storage::rocks::store) struct AdmittedFrontier {
    // The original allocation is moved, never copied or grown for the new head.
    entries: Vec<RetainedLineageEntry>,
    manifest: IngestManifest,
    compacted: Option<ClosedProducerFloor>,
    incoming: [u8; 32],
    slot: ProducerSlotId,
    root: (u64, u64),
    sequence: u64,
    inputs: [u8; 32],
    issued: u64,
    acknowledged: u64,
    staged: u64,
    source: u64,
    poisoned: AtomicBool,
    _permit: OwnedPermit,
}
const _: () = assert!(size_of::<AdmittedFrontier>() + 16 * 1024 < LINEAGE_SCRATCH_BYTES as usize);

impl RetainedLineage {
    pub(in crate::storage::rocks::store) fn into_admitted_frontier(
        mut self,
        handoff: &SuccessorHandoff,
        sequence: u64,
    ) -> AdmittedFrontier {
        // The caller has just validated this exact local plan and completed +4
        // state. No fallible work, allocation, await or native write follows.
        let fields = handoff.record().fields();
        let head = self.entries.last_mut().expect("checked nonempty lineage");
        head.terminal_kind = Some(match fields.terminal_kind {
            PredecessorTerminalKind::Activated => RetainedTerminalKind::Activated,
            PredecessorTerminalKind::UnactivatedEmpty => RetainedTerminalKind::UnactivatedEmpty,
        });
        head.terminal_digest = fields.closure_digest;
        AdmittedFrontier {
            entries: self.entries,
            manifest: fields.next_manifest,
            compacted: self.compacted,
            incoming: Sha256::digest(handoff.record().encode().as_bytes()).into(),
            slot: self.slot,
            root: self.root,
            sequence,
            inputs: self.inputs,
            issued: 0,
            acknowledged: 0,
            staged: 0,
            source: 0,
            poisoned: AtomicBool::new(false),
            _permit: self._permit,
        }
    }
}

impl AdmittedFrontier {
    pub(in crate::storage::rocks::store) fn has_compacted_initial(&self) -> bool {
        self.compacted.is_some()
    }
    #[cfg(test)]
    pub(in crate::storage::rocks::store) fn entries_identity(
        &self,
    ) -> (*const RetainedLineageEntry, usize) {
        (self.entries.as_ptr(), self.entries.capacity())
    }
    pub(in crate::storage::rocks::store) fn manifest(&self) -> &IngestManifest {
        &self.manifest
    }
    pub(in crate::storage::rocks::store) fn poison(&self) {
        self.poisoned.store(true, Ordering::Relaxed);
    }
    pub(in crate::storage::rocks::store) fn require_identity(
        &self,
        store: &IngestStore,
        request: Option<ProducerRequestId>,
    ) -> Result<(), AdmissionError> {
        let result = (|| {
            if self.poisoned.load(Ordering::Relaxed) {
                return Err(AdmissionError::AuthorityUnavailable);
            }
            if store.write_uncertain {
                return Err(AdmissionError::CommitUnknown);
            }
            if store.manifest != self.manifest
                || store.handoff_root_identity()? != self.root
                || store.db.latest_sequence_number() != self.sequence
            {
                return Err(AdmissionError::StateMismatch);
            }
            if request.is_some_and(|id| {
                id.producer_slot_id != self.slot
                    || id.producer_epoch.get() != self.manifest.identity().owner_epoch.get()
            }) {
                return Err(AdmissionError::InvalidInput);
            }
            let producer = store
                .producer_state(
                    self.slot,
                    ProducerEpoch::new(self.manifest.identity().owner_epoch.get())
                        .expect("checked epoch"),
                )?
                .ok_or(AdmissionError::StateMismatch)?;
            if producer
                != (ProducerState {
                    issued: self.issued,
                    acknowledged: self.acknowledged,
                    retired: 0,
                    closing: false,
                })
            {
                return Err(AdmissionError::StateMismatch);
            }
            let key = source_key(&self.manifest)
                .encode()
                .map_err(|_| AdmissionError::InvalidInput)?;
            let value = store
                .db
                .get_pinned_opt(key.as_bytes(), &read_options())
                .map_err(|_| AdmissionError::EngineRead)?
                .ok_or(AdmissionError::StateMismatch)?;
            if value.len() != 10 || value[..2] != [1, 2] || value[2..] != self.source.to_be_bytes()
            {
                return Err(AdmissionError::StateMismatch);
            }
            Ok(())
        })();
        if result.is_err() {
            self.poison();
        }
        result
    }
    pub(in crate::storage::rocks::store) fn require_fact(
        &self,
        store: &IngestStore,
        id: SourceRecordId,
    ) -> Result<(), AdmissionError> {
        self.require_identity(store, None)?;
        let current = self.manifest.identity();
        let cut = if id.owner_epoch == current.owner_epoch {
            Some(self.source)
        } else {
            id.owner_epoch
                .get()
                .checked_sub(1)
                .and_then(|epoch| usize::try_from(epoch).ok())
                .and_then(|index| self.entries.get(index))
                .filter(|entry| entry.terminal_kind.is_some())
                .map(|entry| entry.source_through)
        };
        if id.dataset_id != current.dataset_id
            || id.shard_id != current.shard_id
            || cut.is_none_or(|cut| id.sequence.get() > cut)
        {
            self.poison();
            return Err(AdmissionError::StateMismatch);
        }
        Ok(())
    }
    fn accepts(&self, id: ProducerRequestId, staged: u64) -> bool {
        if id.producer_slot_id != self.slot {
            return false;
        }
        if id.producer_epoch.get() == self.manifest.identity().owner_epoch.get() {
            return id.request_sequence.get() <= staged;
        }
        if self.compacted.is_some() && id.producer_epoch.get() == 1 {
            return false;
        }
        id.producer_epoch
            .get()
            .checked_sub(1)
            .and_then(|epoch| usize::try_from(epoch).ok())
            .and_then(|index| self.entries.get(index))
            .is_some_and(|entry| id.request_sequence.get() <= entry.issued)
    }
    fn input_count(&self, staged: u64) -> Result<u64, AdmissionError> {
        self.entries
            .iter()
            .filter(|entry| {
                self.compacted.is_none() || entry.manifest.identity().owner_epoch.get() != 1
            })
            .try_fold(staged, |total, entry| total.checked_add(entry.issued))
            .filter(|total| *total <= 64)
            .ok_or(AdmissionError::Limit)
    }
    fn input_set(
        &self,
        staged: u64,
    ) -> Result<super::super::request_inputs::LineageInputSet, AdmissionError> {
        let required = self.input_count(staged)?;
        Ok(match self.compacted.as_ref() {
            Some(floor) => super::super::request_inputs::LineageInputSet::with_compacted_initial(
                required,
                self.slot,
                floor.fields().issued_through,
            )?,
            None => required.into(),
        })
    }
    fn current_entry(&self, store: &IngestStore) -> Result<RetainedLineageEntry, BootstrapError> {
        entry_coordinates_in(
            store,
            self.manifest,
            self.incoming,
            None,
            ProducerState {
                issued: self.issued,
                acknowledged: self.acknowledged,
                retired: 0,
                closing: false,
            },
        )
    }
    fn census<'a>(&'a self, current: &'a RetainedLineageEntry) -> Census<'a> {
        Census {
            entries: CensusEntries {
                retained: &self.entries,
                current: Some(current),
            },
            slot: self.slot,
            compacted: self.compacted.as_ref(),
        }
    }
    pub(in crate::storage::rocks::store) fn validate(
        &self,
        store: &IngestStore,
        cancelled: &AtomicBool,
    ) -> Result<(), AdmissionError> {
        let result = (|| {
            check_cancelled(cancelled)?;
            self.require_identity(store, None)?;
            let stamp = store.lineage_input_census(
                self.input_set(self.staged)?,
                |id| self.accepts(id, self.staged),
                cancelled,
            )?;
            if stamp != self.inputs {
                return Err(AdmissionError::StateMismatch);
            }
            if self.compacted.is_some() {
                let current = self.current_entry(store)?;
                self.census(&current)
                    .inspect_store_census(store, cancelled)?;
            }
            self.require_identity(store, None)?;
            check_cancelled(cancelled)?;
            Ok(())
        })();
        if result.is_err() {
            self.poison();
        }
        result
    }
    pub(in crate::storage::rocks::store) fn staging(
        &self,
        store: &IngestStore,
        request: ProducerRequestId,
        cancelled: &AtomicBool,
    ) -> Result<bool, AdmissionError> {
        self.validate(store, cancelled)?;
        self.require_identity(store, Some(request))?;
        let n = request.request_sequence.get();
        if n > self.staged {
            if self.staged != self.issued || self.issued.checked_add(1) != Some(n) || n > 64 {
                return Err(AdmissionError::RequestGap);
            }
            self.input_count(n)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
    pub(in crate::storage::rocks::store) fn staged_stamp(
        &self,
        store: &IngestStore,
        request: ProducerRequestId,
        bytes: &[u8],
        added: bool,
        cancelled: &AtomicBool,
    ) -> Result<[u8; 32], AdmissionError> {
        self.require_identity(store, Some(request))?;
        let staged = if added {
            request.request_sequence.get()
        } else {
            self.staged
        };
        let addition = added.then(|| (request, Sha256::digest(bytes).into(), bytes.len() as u64));
        let (all, prior) = store.lineage_input_delta(
            self.input_set(staged)?,
            |id| self.accepts(id, staged),
            addition,
            cancelled,
        )?;
        if prior != self.inputs {
            return Err(AdmissionError::StateMismatch);
        }
        self.require_identity(store, Some(request))?;
        Ok(all)
    }
    pub(in crate::storage::rocks::store) fn record_staging(
        &mut self,
        request: ProducerRequestId,
        stamp: [u8; 32],
    ) {
        self.staged = self.staged.max(request.request_sequence.get());
        self.inputs = stamp;
    }
    pub(in crate::storage::rocks::store) fn prepare_commit(
        &self,
        store: &IngestStore,
        request: ProducerRequestId,
        source: Option<u64>,
        puts: usize,
        cancelled: &AtomicBool,
    ) -> Result<(u64, u64), AdmissionError> {
        self.validate(store, cancelled)?;
        self.require_identity(store, Some(request))?;
        if request.request_sequence.get() != self.staged
            || self.issued.checked_add(1) != Some(self.staged)
            || source.is_some_and(|source| source <= self.source)
        {
            return Err(AdmissionError::StateMismatch);
        }
        let sequence = self
            .sequence
            .checked_add(u64::try_from(puts).map_err(|_| AdmissionError::Limit)?)
            .ok_or(AdmissionError::Limit)?;
        Ok((sequence, source.unwrap_or(self.source)))
    }
    pub(in crate::storage::rocks::store) fn committed(&mut self, sequence: u64, source: u64) {
        self.issued = self.staged;
        self.sequence = sequence;
        self.source = source;
    }

    pub(in crate::storage::rocks::store) fn require_acknowledgement(
        &self,
        store: &IngestStore,
        acknowledged: u64,
        sequence: u64,
    ) -> Result<(), AdmissionError> {
        self.require_identity(store, None)?;
        if acknowledged <= self.acknowledged
            || acknowledged > self.issued
            || self.sequence.checked_add(1) != Some(sequence)
        {
            self.poison();
            return Err(AdmissionError::StateMismatch);
        }
        Ok(())
    }

    pub(in crate::storage::rocks::store) fn acknowledged(
        &mut self,
        acknowledged: u64,
        sequence: u64,
    ) {
        self.acknowledged = acknowledged;
        self.sequence = sequence;
    }
}

/// A private read view over the continuously held live owner. This carries no
/// IngestRecovery reference/conversion and cannot mint a historical handoff.
#[derive(Clone, Copy)]
pub(in crate::storage) struct CompactedLiveSuccessor<'a> {
    frontier: &'a AdmittedFrontier,
    store: &'a IngestStore,
    cancelled: &'a AtomicBool,
}
impl<'a> CompactedLiveSuccessor<'a> {
    pub(in crate::storage::rocks::store) fn new(
        store: &'a IngestStore,
        limits: super::super::AdmissionLimits,
        effects: &OwnedPermit,
        cancelled: &'a AtomicBool,
    ) -> Result<Self, AdmissionError> {
        let frontier = store
            .live_successor
            .as_ref()
            .ok_or(AdmissionError::StateMismatch)?;
        let view = Self {
            frontier,
            store,
            cancelled,
        };
        let result = (|| {
            view.validate(cancelled)?;
            if frontier.compacted.is_none() {
                return Err(AdmissionError::StateMismatch);
            }
            let current = frontier.current_entry(store)?;
            frontier.census(&current).verify_store_effect_census(
                store,
                frontier.manifest.identity().owner_epoch.get(),
                limits,
                effects,
                cancelled,
            )?;
            view.validate(cancelled)
        })();
        if result.is_err() {
            frontier.poison();
        }
        result?;
        Ok(view)
    }
    pub(in crate::storage) fn manifest(&self) -> &IngestManifest {
        &self.frontier.entries[0].manifest
    }
    pub(in crate::storage) fn floor(&self) -> &ClosedProducerFloor {
        self.frontier.compacted.as_ref().expect("sealed live floor")
    }
    pub(in crate::storage) fn producer_slot_id(&self) -> ProducerSlotId {
        self.frontier.slot
    }
    pub(in crate::storage) fn progress_scratch(&self) -> &OwnedPermit {
        &self.frontier._permit
    }
    pub(in crate::storage::rocks::store) fn store(&self) -> &'a IngestStore {
        self.store
    }
    pub(in crate::storage) fn validate(
        &self,
        cancelled: &AtomicBool,
    ) -> Result<(), AdmissionError> {
        if self.cancelled.load(Ordering::Relaxed) {
            self.frontier.poison();
            return Err(AdmissionError::AuthorityUnavailable);
        }
        self.frontier.validate(self.store, cancelled)?;
        if self.cancelled.load(Ordering::Relaxed) || cancelled.load(Ordering::Relaxed) {
            self.frontier.poison();
            return Err(AdmissionError::AuthorityUnavailable);
        }
        Ok(())
    }
    pub(in crate::storage::rocks::store) fn require_input(
        &self,
        checked: (ProducerRequestId, [u8; 32], u64),
        cancelled: &AtomicBool,
    ) -> Result<(), AdmissionError> {
        let result = (|| {
            self.validate(cancelled)?;
            let stamp = self.store.require_compacted_lineage_input(
                self.frontier.input_set(self.frontier.staged)?,
                |id| self.frontier.accepts(id, self.frontier.staged),
                checked,
                cancelled,
            )?;
            if stamp != self.frontier.inputs {
                return Err(AdmissionError::StateMismatch);
            }
            self.validate(cancelled)
        })();
        if result.is_err() {
            self.frontier.poison();
        }
        result
    }
    pub(in crate::storage) fn progress_read(
        &self,
    ) -> Result<super::super::ProgressRead<'_>, AdmissionError> {
        self.validate(self.cancelled)?;
        self.store.progress_read(self.progress_scratch())
    }
    pub(in crate::storage) fn resolution(
        &self,
        cancelled: &AtomicBool,
    ) -> Result<CompactedPrefixResolution<'a>, AdmissionError> {
        CompactedPrefixResolution::from_live(*self, cancelled)
    }
}
