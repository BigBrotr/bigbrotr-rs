//! Privately promoted positive prefix and exact locally admitted progress.
//! This is not a refreshed RetainedLineage and has no portable constructor.
use super::super::{AdmissionError, IngestStore, SuccessorHandoff};
use super::*;
use bigbrotr_model::{ProducerRequestId, SourceRecordId};

pub(in crate::storage::rocks::store) struct AdmittedFrontier {
    // The original allocation is moved, never copied or grown for the new head.
    entries: Vec<RetainedLineageEntry>,
    manifest: IngestManifest,
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
            .try_fold(staged, |total, entry| total.checked_add(entry.issued))
            .filter(|total| *total <= 64)
            .ok_or(AdmissionError::Limit)
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
                self.input_count(self.staged)?,
                |id| self.accepts(id, self.staged),
                cancelled,
            )?;
            if stamp != self.inputs {
                return Err(AdmissionError::StateMismatch);
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
            self.input_count(staged)?,
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
