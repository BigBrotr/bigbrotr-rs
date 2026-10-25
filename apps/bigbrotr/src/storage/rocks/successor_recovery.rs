//! Fresh activated-family recovery, never adoption of a former writer.
//! The one structural allocation remains private even after positive replay.
use super::super::{
    AdmissionError, AdmissionLimits, ProducerRecoveryError, ProducerRecoveryLimits,
    ResolvedProducerRequest, SuccessorStagedInput,
    lineage_replay::{self, LineageEpochSummary, LineageResolution},
    source_closure::Prefix,
};
use super::*;
use bigbrotr_model::{Digest256, RequestSequence, codec::request::RequestDecodeLimits};

pub(in crate::storage) struct NativeSuccessorRecovery {
    lineage: RetainedLineage,
    tail: Option<SuccessorStagedInput>,
    closing_permit: Option<OwnedPermit>,
    failed: bool,
}

/// Borrows the one recovery allocation and its exact tail-aware input census.
/// This is not an ordinary lineage ancestor, a writer or a handoff witness.
#[derive(Clone, Copy)]
pub(in crate::storage) struct CompactedRecoveryPredecessor<'a> {
    owner: &'a NativeSuccessorRecovery,
    recovery: &'a IngestRecovery,
    cancelled: &'a AtomicBool,
}

pub(in crate::storage) struct SuccessorRecoveryResolution<'a> {
    owner: &'a NativeSuccessorRecovery,
    recovery: &'a IngestRecovery,
    cancelled: &'a AtomicBool,
    cursor: LineageResolution<'a>,
    poisoned: bool,
}

/// Full original-input/effect validation and the proposed prefix precede any PG
/// copy insertion. The exclusive borrow prevents parallel/cross-cursor advance.
/// Dropping after SQL failure/unknown outcome permanently poisons this cursor.
pub(in crate::storage) struct PendingSuccessorRequest<'step, 'owner> {
    cursor: &'step mut SuccessorRecoveryResolution<'owner>,
    resolved: ResolvedProducerRequest,
    prefix: Option<Prefix>,
    accepted: bool,
}

/// Recovery-only completion, not a source closure or a predecessor capability.
pub(in crate::storage) struct ResolvedSuccessorRecovery<'a> {
    owner: &'a NativeSuccessorRecovery,
    recovery: &'a IngestRecovery,
    cancelled: &'a AtomicBool,
    last: LineageEpochSummary,
}

// One Vec retains its separately checked capacity and permit. Native fixed
// state occupies at most half the existing 128KiB scratch; the connected owner
// separately checks its PG headers/tuples against the other half. Summing both
// sequential census/input scans is conservative. Decoded input/outcome payloads
// retain their separate ordinary grants, never this metadata reservation.
const _: () = assert!(
    size_of::<NativeSuccessorRecovery>()
        + size_of::<SuccessorRecoveryResolution<'_>>()
        + size_of::<PendingSuccessorRequest<'_, '_>>()
        + size_of::<ResolvedSuccessorRecovery<'_>>()
        + size_of::<CompactedRecoveryPredecessor<'_>>()
        + size_of::<InitialHandoffRecord>()
        + 2 * size_of::<SuccessionRecord>()
        + super::census::COUNTER_BYTES
        + 16 * 1024
        < LINEAGE_SCRATCH_BYTES as usize / 2
);

impl NativeSuccessorRecovery {
    pub(in crate::storage) fn inspect(
        recovery: &IngestRecovery,
        permit: OwnedPermit,
        cancelled: &AtomicBool,
    ) -> Result<Self, BootstrapError> {
        Self::inspect_in(recovery, permit, cancelled, false)
    }
    pub(in crate::storage) fn inspect_with_compacted_initial(
        recovery: &IngestRecovery,
        permit: OwnedPermit,
        cancelled: &AtomicBool,
    ) -> Result<Self, BootstrapError> {
        Self::inspect_in(recovery, permit, cancelled, true)
    }
    fn inspect_in(
        recovery: &IngestRecovery,
        permit: OwnedPermit,
        cancelled: &AtomicBool,
        compacted: bool,
    ) -> Result<Self, BootstrapError> {
        if recovery.manifest().identity().owner_epoch.get() < 3 {
            return Err(BootstrapError::InvalidIntent);
        }
        let lineage = if compacted {
            RetainedLineage::inspect_structure_in(recovery, None, permit, cancelled, true)?
        } else {
            RetainedLineage::inspect_structure(recovery, None, permit, cancelled)?
        };
        if compacted && !lineage.has_compacted_initial() {
            return Err(BootstrapError::StateMismatch);
        }
        let mut owner = Self {
            lineage,
            tail: None,
            closing_permit: None,
            failed: false,
        };
        let (stamp, tail) = owner.input_census(recovery, cancelled)?;
        owner.lineage.inputs = stamp;
        owner.tail = tail;
        owner.validate(recovery, cancelled)?;
        Ok(owner)
    }
    pub(in crate::storage) fn entry(&self, epoch: u64) -> Option<&RetainedLineageEntry> {
        self.lineage.entry(epoch)
    }
    pub(in crate::storage) fn head(&self) -> &RetainedLineageEntry {
        self.lineage.native_head()
    }
    pub(in crate::storage) fn producer_slot_id(&self) -> ProducerSlotId {
        self.lineage.slot
    }
    pub(in crate::storage) fn issued_through(&self) -> u64 {
        self.head().issued_through()
    }
    pub(in crate::storage) fn is_closing(&self) -> bool {
        self.head().is_closing()
    }
    pub(in crate::storage) fn staged_input(&self) -> Option<SuccessorStagedInput> {
        self.tail
    }
    pub(in crate::storage) fn compacted_requests(&self) -> u64 {
        self.lineage
            .compacted_floor()
            .map_or(0, |floor| floor.fields().issued_through)
    }
    pub(in crate::storage) fn compacted_predecessor<'a>(
        &'a self,
        recovery: &'a IngestRecovery,
        limits: AdmissionLimits,
        effects: &OwnedPermit,
        cancelled: &'a AtomicBool,
    ) -> Result<CompactedRecoveryPredecessor<'a>, AdmissionError> {
        if self.compacted_requests() == 0 {
            return Err(AdmissionError::StateMismatch);
        }
        if effects.request().units().items() == 0
            || effects.request().units().data_bytes()
                < limits.required_dictionary_replay_data_bytes()?
        {
            return Err(AdmissionError::InsufficientReservation);
        }
        self.validate(recovery, cancelled)?;
        self.lineage.census().verify_effect_census(
            recovery,
            self.head().manifest.identity().owner_epoch.get(),
            limits,
            effects,
            cancelled,
        )?;
        self.validate(recovery, cancelled)?;
        Ok(CompactedRecoveryPredecessor {
            owner: self,
            recovery,
            cancelled,
        })
    }
    // No PG/public escape to ordinary lineage resolution or handoff proof.
    pub(in crate::storage::rocks::store) fn lineage(&self) -> &RetainedLineage {
        &self.lineage
    }
    fn input_census(
        &self,
        recovery: &IngestRecovery,
        cancelled: &AtomicBool,
    ) -> Result<([u8; 32], Option<SuccessorStagedInput>), BootstrapError> {
        recovery.store.successor_recovery_inputs(
            self.lineage.input_set()?,
            |id| self.lineage.accepts_input(id),
            self.tail_request()?,
            cancelled,
        )
    }
    fn tail_request(&self) -> Result<ProducerRequestId, BootstrapError> {
        Ok(ProducerRequestId {
            producer_slot_id: self.producer_slot_id(),
            producer_epoch: ProducerEpoch::new(self.head().manifest.identity().owner_epoch.get())
                .map_err(|_| BootstrapError::InvalidIntent)?,
            request_sequence: RequestSequence::new(self.issued_through() + 1)
                .map_err(|_| BootstrapError::InvalidIntent)?,
        })
    }
    pub(in crate::storage) fn validate(
        &self,
        recovery: &IngestRecovery,
        cancelled: &AtomicBool,
    ) -> Result<(), BootstrapError> {
        check_cancelled(cancelled)?;
        if self.failed {
            return Err(BootstrapError::AuthorityUnavailable);
        }
        if recovery.store.write_uncertain {
            return Err(BootstrapError::CommitUnknown);
        }
        let identity = || {
            recovery.manifest() == self.head().manifest()
                && recovery.store.db.latest_sequence_number() == self.lineage.sequence
        };
        if !identity() || recovery.store.handoff_root_identity()? != self.lineage.root {
            return Err(BootstrapError::StateMismatch);
        }
        let (stamp, tail) = self.input_census(recovery, cancelled)?;
        if stamp != self.lineage.inputs
            || tail != self.tail
            || !identity()
            || recovery.store.handoff_root_identity()? != self.lineage.root
        {
            return Err(BootstrapError::StateMismatch);
        }
        check_cancelled(cancelled)
    }
    /// Optional input bytes are decoded under an ordinary input grant. Even a
    /// canonical unreserved preimage carries no manifest/protection authority.
    pub(in crate::storage) fn validate_tail(
        &self,
        recovery: &IngestRecovery,
        limits: RequestDecodeLimits,
        permit: OwnedPermit,
        cancelled: &AtomicBool,
    ) -> Result<(), ProducerRecoveryError> {
        self.validate(recovery, cancelled)
            .map_err(AdmissionError::from)?;
        if let Some(tail) = self.tail {
            let retained = recovery
                .store
                .read_request_input(tail.request, tail.digest, limits, permit)
                .map_err(ProducerRecoveryError::Input)?;
            if retained.encoded_bytes() != tail.input_bytes {
                return Err(ProducerRecoveryError::Unresolved);
            }
            drop(retained);
        }
        self.validate(recovery, cancelled)
            .map_err(AdmissionError::from)?;
        Ok(())
    }
    /// The owner and its original allocation remain reachable on every error.
    /// Unknown dispatch poisons this owner; a fresh process must inspect reality.
    pub(in crate::storage) fn close_producer(
        &mut self,
        recovery: &mut IngestRecovery,
        permit: OwnedPermit,
        cancelled: &AtomicBool,
        mut authority: impl FnMut() -> Result<(), BootstrapError>,
    ) -> Result<(), BootstrapError> {
        if permit.request().units().data_bytes() < 4096
            || permit.request().units().native_bytes() < 4096
            || permit.request().units().items() == 0
        {
            return Err(BootstrapError::InsufficientReservation);
        }
        // A repeated observation needs no second retained grant.
        if self.closing_permit.is_none() {
            self.closing_permit = Some(permit);
        }
        let result = (|| {
            self.validate(recovery, cancelled)?;
            authority()?;
            let old = ProducerState {
                issued: self.issued_through(),
                acknowledged: self.head().acknowledged_through(),
                retired: 0,
                closing: self.is_closing(),
            };
            let sequence = self
                .lineage
                .sequence
                .checked_add(u64::from(!old.closing))
                .ok_or(BootstrapError::Limit)?;
            let state = recovery.store.close_producer_marker(
                self.producer_slot_id(),
                ProducerEpoch::new(self.head().manifest.identity().owner_epoch.get())
                    .map_err(|_| BootstrapError::InvalidIntent)?,
                old,
                || {
                    check_cancelled(cancelled)?;
                    authority()
                },
            )?;
            if !state.closing
                || state.issued != old.issued
                || state.acknowledged != old.acknowledged
                || state.retired != old.retired
            {
                return Err(BootstrapError::StateMismatch);
            }
            // This private owner alone consumes the exact permitted +1/+0
            // transition. Ordinary RetainedLineage::validate is not refreshed.
            self.lineage
                .entries
                .last_mut()
                .expect("inspected head")
                .closing = true;
            self.lineage.sequence = sequence;
            self.lineage.inspect_census(recovery, cancelled)?;
            self.validate(recovery, cancelled)?;
            authority()?;
            check_cancelled(cancelled)
        })();
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    /// Source-closed replay observes an existing barrier. It cannot normalize
    /// OPEN native state, change the sequence or construct a predecessor proof.
    /// After grant admission, the same allocation and closing grant remain
    /// retained on every error.
    pub(in crate::storage) fn observe_closed(
        &mut self,
        recovery: &IngestRecovery,
        permit: OwnedPermit,
        cancelled: &AtomicBool,
        mut authority: impl FnMut() -> Result<(), BootstrapError>,
    ) -> Result<(), BootstrapError> {
        if permit.request().units().data_bytes() < 4096
            || permit.request().units().native_bytes() < 4096
            || permit.request().units().items() == 0
        {
            return Err(BootstrapError::InsufficientReservation);
        }
        if self.closing_permit.is_none() {
            self.closing_permit = Some(permit);
        }
        let result = (|| {
            self.validate(recovery, cancelled)?;
            authority()?;
            if !self.is_closing() {
                return Err(BootstrapError::StateMismatch);
            }
            self.lineage.inspect_census(recovery, cancelled)?;
            self.validate(recovery, cancelled)?;
            authority()?;
            check_cancelled(cancelled)
        })();
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    pub(in crate::storage) fn resolution<'a>(
        &'a self,
        recovery: &'a IngestRecovery,
        cancelled: &'a AtomicBool,
    ) -> Result<SuccessorRecoveryResolution<'a>, BootstrapError> {
        self.validate(recovery, cancelled)?;
        if self.closing_permit.is_none() || !self.is_closing() {
            return Err(BootstrapError::StateMismatch);
        }
        Ok(SuccessorRecoveryResolution {
            owner: self,
            recovery,
            cancelled,
            cursor: LineageResolution::for_recovery(self, recovery, cancelled)?,
            poisoned: false,
        })
    }
}

impl<'a> SuccessorRecoveryResolution<'a> {
    pub(in crate::storage) fn needs_compacted_initial(&self) -> bool {
        self.cursor.needs_compacted_initial()
    }
    pub(in crate::storage) fn accept_compacted_initial(
        &mut self,
        proof: &super::super::VerifiedCompactedPrefix<'_>,
        authority: impl FnMut() -> Result<(), AdmissionError>,
    ) -> Result<super::super::CompactedInitialSummary, AdmissionError> {
        let result = self
            .validate()
            .map_err(AdmissionError::from)
            .and_then(|()| self.cursor.accept_compacted_initial(proof, authority));
        if result.is_err() {
            self.poison();
        }
        result
    }
    pub(in crate::storage) fn current_epoch(&self) -> Option<u64> {
        self.cursor.current_epoch()
    }
    pub(in crate::storage) fn next_request(&self) -> Option<ProducerRequestId> {
        self.cursor.next_request()
    }
    pub(in crate::storage) fn poison(&mut self) {
        self.poisoned = true;
    }
    fn validate(&self) -> Result<(), BootstrapError> {
        if self.poisoned {
            return Err(BootstrapError::AuthorityUnavailable);
        }
        self.owner.validate(self.recovery, self.cancelled)
    }
    #[allow(
        clippy::too_many_arguments,
        reason = "distinct input/outcome grants and current authority"
    )]
    pub(in crate::storage) fn preview_next<'step>(
        &'step mut self,
        digest: Digest256,
        limits: ProducerRecoveryLimits,
        input_permit: OwnedPermit,
        outcome_permit: OwnedPermit,
        mut authority: impl FnMut() -> Result<(), AdmissionError>,
    ) -> Result<PendingSuccessorRequest<'step, 'a>, ProducerRecoveryError> {
        self.validate().map_err(AdmissionError::from)?;
        let result = (|| {
            let resolved = self.cursor.resolve_next(
                digest,
                limits,
                input_permit,
                outcome_permit,
                &mut authority,
            )?;
            let prefix = self
                .cursor
                .preview(&resolved)
                .map_err(AdmissionError::from)?;
            authority()?;
            self.validate().map_err(AdmissionError::from)?;
            Ok((resolved, prefix))
        })();
        let (resolved, prefix) = match result {
            Ok(value) => value,
            Err(error) => {
                self.poison();
                return Err(error);
            }
        };
        Ok(PendingSuccessorRequest {
            cursor: self,
            resolved,
            prefix: Some(prefix),
            accepted: false,
        })
    }
    pub(in crate::storage) fn finish_epoch(
        &mut self,
    ) -> Result<LineageEpochSummary, BootstrapError> {
        self.validate()?;
        let result = self.cursor.finish_epoch();
        if result.is_err() {
            self.poison();
        }
        result
    }
    pub(in crate::storage) fn finish(
        self,
        limits: AdmissionLimits,
        permit: OwnedPermit,
    ) -> Result<ResolvedSuccessorRecovery<'a>, ProducerRecoveryError> {
        self.validate().map_err(AdmissionError::from)?;
        let last = self.cursor.finish_checked(limits, permit)?;
        // The complete issued prefix can be copied independently, but retained
        // staging is unresolved. PG separately checks its reserved suffix.
        if self.owner.tail.is_some() {
            return Err(ProducerRecoveryError::Unresolved);
        }
        Ok(ResolvedSuccessorRecovery {
            owner: self.owner,
            recovery: self.recovery,
            cancelled: self.cancelled,
            last,
        })
    }
}

impl<'a> CompactedRecoveryPredecessor<'a> {
    pub(in crate::storage) fn manifest(&self) -> &IngestManifest {
        self.owner
            .lineage
            .entry(1)
            .expect("inspected initial ancestor")
            .manifest()
    }
    pub(in crate::storage) fn floor(&self) -> &ClosedProducerFloor {
        self.owner
            .lineage
            .compacted_floor()
            .expect("sealed recovery basis")
    }
    pub(in crate::storage) fn producer_slot_id(&self) -> ProducerSlotId {
        self.owner.producer_slot_id()
    }
    pub(in crate::storage) fn progress_scratch(&self) -> &OwnedPermit {
        self.owner.lineage.progress_scratch()
    }
    pub(in crate::storage::rocks::store) fn matches_owner(
        &self,
        owner: &NativeSuccessorRecovery,
    ) -> bool {
        std::ptr::eq(self.owner, owner)
    }
    pub(in crate::storage) fn validate(
        &self,
        recovery: &IngestRecovery,
        cancelled: &AtomicBool,
    ) -> Result<(), AdmissionError> {
        check_cancelled(self.cancelled)?;
        if !std::ptr::eq(self.recovery, recovery) {
            return Err(AdmissionError::StateMismatch);
        }
        self.owner.validate(recovery, cancelled)?;
        check_cancelled(self.cancelled).map_err(Into::into)
    }
    pub(in crate::storage::rocks::store) fn require_input(
        &self,
        recovery: &IngestRecovery,
        checked: (ProducerRequestId, [u8; 32], u64),
        cancelled: &AtomicBool,
    ) -> Result<(), AdmissionError> {
        self.validate(recovery, cancelled)?;
        let observed = recovery.store.require_compacted_recovery_input(
            self.owner.lineage.input_set()?,
            |id| self.owner.lineage.accepts_input(id),
            self.owner.tail_request()?,
            checked,
            cancelled,
        )?;
        if observed != (self.owner.lineage.inputs, self.owner.tail) {
            return Err(AdmissionError::StateMismatch);
        }
        self.validate(recovery, cancelled)
    }
    pub(in crate::storage) fn resolution(
        &self,
        cancelled: &AtomicBool,
    ) -> Result<super::super::CompactedPrefixResolution<'a>, AdmissionError> {
        super::super::CompactedPrefixResolution::from_recovery(*self, self.recovery, cancelled)
    }
}
impl PendingSuccessorRequest<'_, '_> {
    pub(in crate::storage) fn resolved(&self) -> &ResolvedProducerRequest {
        &self.resolved
    }
    pub(in crate::storage) fn request(&self) -> ProducerRequestId {
        self.resolved.native_outcome().request()
    }
    /// The connected caller invokes this only after a known SQL result. All
    /// fallible validation precedes the infallible fixed-prefix transfer.
    pub(in crate::storage) fn accept(
        mut self,
        mut authority: impl FnMut() -> Result<(), BootstrapError>,
    ) -> Result<(), BootstrapError> {
        self.cursor.validate()?;
        self.resolved.require_native_context(self.cursor.recovery)?;
        authority()?;
        check_cancelled(self.cursor.cancelled)?;
        self.cursor
            .cursor
            .accept_preview(self.prefix.take().expect("sealed pending prefix"));
        self.accepted = true;
        Ok(())
    }
}
impl Drop for PendingSuccessorRequest<'_, '_> {
    fn drop(&mut self) {
        if !self.accepted {
            self.cursor.poison();
        }
    }
}
impl ResolvedSuccessorRecovery<'_> {
    pub(in crate::storage::rocks::store) fn acknowledgement_owner(
        &self,
        recovery: &IngestRecovery,
        cancelled: &AtomicBool,
    ) -> Result<&NativeSuccessorRecovery, BootstrapError> {
        if !std::ptr::eq(self.recovery, recovery) {
            return Err(BootstrapError::StateMismatch);
        }
        self.validate(cancelled)?;
        Ok(self.owner)
    }
    pub(in crate::storage) fn recovery_acknowledgement(
        &self,
        cancelled: &AtomicBool,
    ) -> Result<super::super::RecoveryAcknowledgement, BootstrapError> {
        self.validate(cancelled)?;
        if self.owner.compacted_requests() != 0 {
            return super::super::RecoveryAcknowledgement::from_compacted_successor_replay(
                self.recovery,
                self,
                cancelled,
            );
        }
        super::super::RecoveryAcknowledgement::from_replay(
            self.recovery,
            *self.owner.head().manifest(),
            self.owner.producer_slot_id(),
            self.owner.issued_through(),
        )
    }
    pub(in crate::storage) fn validate(
        &self,
        cancelled: &AtomicBool,
    ) -> Result<(), BootstrapError> {
        if self.owner.compacted_requests() != 0 {
            check_cancelled(self.cancelled)?;
        }
        self.owner.validate(self.recovery, cancelled)?;
        if self.owner.compacted_requests() != 0 {
            check_cancelled(self.cancelled)?;
        }
        Ok(())
    }
    pub(in crate::storage) fn matches_owner(&self, owner: &NativeSuccessorRecovery) -> bool {
        std::ptr::eq(self.owner, owner)
    }
    pub(in crate::storage) fn verified_through(&self) -> u64 {
        self.last.epoch
    }
    pub(in crate::storage) fn last_epoch(&self) -> LineageEpochSummary {
        self.last
    }
    pub(in crate::storage) fn matches_recorded(
        &self,
        epoch: u64,
        records: impl IntoIterator<Item = (u64, [u8; 32], [u8; 32])>,
    ) -> bool {
        lineage_replay::matches_recorded(&self.owner.lineage, self.last, epoch, records)
    }
}
