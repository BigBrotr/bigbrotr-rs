//! Explicit compacted predecessor proof for the unactivated empty epoch2.
//! It grants no original replay, writer, ACK or reclamation capability.
use super::*;
use crate::storage::rocks::{
    keys::IngestKey,
    producer_floor::ClosedProducerFloor,
    store::{
        AdmissionError, AdmissionLimits, CompactedInitialAncestor, CompactedPrefixResolution,
        RetainedLineage, VerifiedCompactedPrefix,
    },
    succession::LINEAGE_SCRATCH_BYTES,
};
use std::sync::atomic::{AtomicBool, Ordering};

pub const COMPACTED_EMPTY_DATA_BYTES: u64 = LINEAGE_SCRATCH_BYTES;

/// Borrows the single retained lineage allocation and its metadata grant.
/// A checked zero target is still not a positive predecessor proof.
pub(in crate::storage) struct CompactedEmptyInitialAttempt<'a> {
    attempt: EmptyInitialAttempt<'a>,
    lineage: &'a RetainedLineage,
    cancelled: &'a AtomicBool,
}
impl<'a> CompactedEmptyInitialAttempt<'a> {
    pub(in crate::storage) fn historical(&self) -> &HistoricalInitial<'a> {
        self.attempt.historical()
    }
    pub(in crate::storage) fn is_closing(&self) -> bool {
        self.attempt.is_closing()
    }
    fn validate(&self, cancelled: &AtomicBool) -> Result<(), BootstrapError> {
        check_cancelled(self.cancelled)?;
        check_cancelled(cancelled)?;
        self.lineage.require_initial_empty_shape()?;
        self.lineage
            .validate(self.historical().recovery(), cancelled)?;
        self.historical().validate()?;
        if !self.lineage.has_compacted_initial()
            || self.lineage.native_head().is_closing() != self.is_closing()
            || self.lineage.producer_slot_id() != self.historical().producer_slot_id()
            || self.lineage.entry(1).map(|entry| entry.manifest())
                != Some(self.historical().manifest())
        {
            return Err(BootstrapError::StateMismatch);
        }
        check_cancelled(self.cancelled)?;
        check_cancelled(cancelled)
    }
    pub(in crate::storage) fn compacted_initial_ancestor(
        &self,
        limits: AdmissionLimits,
        effects: &OwnedPermit,
        cancelled: &AtomicBool,
    ) -> Result<CompactedInitialAncestor<'a>, AdmissionError> {
        self.validate(cancelled)?;
        let ancestor = self
            .lineage
            .compacted_initial_ancestor_with_permit(
                self.historical().recovery(),
                limits,
                effects,
                cancelled,
            )?
            .ok_or(AdmissionError::StateMismatch)?;
        self.validate(cancelled)?;
        Ok(ancestor)
    }
    /// Only this exact two-entry canonical origin can mint the fixed plan.
    /// A plan-derived cursor cannot refresh the original sequence/input stamp.
    /// The connected owner retains the lineage grant after this borrow ends.
    pub(in crate::storage) fn reconstruct_retirement(
        &self,
        source: &VerifiedCompactedPrefix<'_>,
        cancelled: &AtomicBool,
    ) -> Result<CompactedEmptyInitialRetirement, AdmissionError> {
        self.validate(cancelled)?;
        source.validate(self.historical().recovery(), cancelled)?;
        let floor = *source.floor();
        let fields = floor.fields();
        let history = self.historical();
        if !source.matches_lineage(self.lineage)
            || !source.matches_recovery(history.recovery())
            || source.manifest() != history.manifest()
            || source.producer_slot_id() != history.producer_slot_id()
            || fields.issued_through != history.issued_through()
            || fields.source_through != history.last_complete_sequence()
            || fields.closure_digest != history.closure_digest()
        {
            return Err(AdmissionError::StateMismatch);
        }
        self.validate(cancelled)?;
        Ok(CompactedEmptyInitialRetirement {
            empty: EmptyInitialRetirement::from_history(history),
            floor,
            inputs: self.lineage.input_stamp(),
        })
    }
}

/// Fixed local witness, with no portable constructor or ordinary-plan conversion.
/// Its connected owner retains the metadata grant through final SQL completion.
#[derive(Clone, Debug)]
pub struct CompactedEmptyInitialRetirement {
    empty: EmptyInitialRetirement,
    floor: ClosedProducerFloor,
    inputs: [u8; 32],
}
const _: () = assert!(
    2 * std::mem::size_of::<CompactedEmptyInitialRetirement>()
        + std::mem::size_of::<EncodedHandoff>()
        < 8192
);
impl CompactedEmptyInitialRetirement {
    pub fn previous_manifest(&self) -> &IngestManifest {
        self.empty.previous_manifest()
    }
    pub fn target_manifest(&self) -> &IngestManifest {
        self.empty.target_manifest()
    }
    pub fn producer_slot_id(&self) -> ProducerSlotId {
        self.empty.producer_slot_id()
    }
    pub fn issued_through(&self) -> u64 {
        self.floor.fields().issued_through
    }
    pub fn last_complete_sequence(&self) -> u64 {
        self.floor.fields().source_through
    }
    pub fn original_closure_digest(&self) -> [u8; 32] {
        self.floor.fields().closure_digest
    }
    pub fn closure_digest(&self) -> [u8; 32] {
        self.empty.closure_digest()
    }
    pub fn handoff(&self) -> EncodedHandoff {
        self.empty.handoff()
    }
    pub fn initially_closing(&self) -> bool {
        self.empty.initially_closing()
    }
    pub fn predecessor<'a>(
        &'a self,
        recovery: &'a IngestRecovery,
        scratch: &'a OwnedPermit,
        cancelled: &'a AtomicBool,
    ) -> Result<CompactedEmptyInitialPredecessor<'a>, BootstrapError> {
        recovery.validate_compacted_empty_initial_retirement(self, scratch, cancelled)?;
        Ok(CompactedEmptyInitialPredecessor {
            retirement: self,
            recovery,
            scratch,
            cancelled,
        })
    }
}

/// Bounded canonical recheck of a sealed plan, including its exact +1 close.
/// This view can never be a new handoff or retirement construction origin.
#[derive(Clone, Copy)]
pub struct CompactedEmptyInitialPredecessor<'a> {
    retirement: &'a CompactedEmptyInitialRetirement,
    recovery: &'a IngestRecovery,
    scratch: &'a OwnedPermit,
    cancelled: &'a AtomicBool,
}
impl<'a> CompactedEmptyInitialPredecessor<'a> {
    pub fn manifest(&self) -> &IngestManifest {
        self.retirement.previous_manifest()
    }
    pub fn floor(&self) -> &ClosedProducerFloor {
        &self.retirement.floor
    }
    pub fn producer_slot_id(&self) -> ProducerSlotId {
        self.retirement.producer_slot_id()
    }
    pub(in crate::storage::rocks::store) fn input_stamp(&self) -> [u8; 32] {
        self.retirement.inputs
    }
    pub fn validate(
        &self,
        recovery: &IngestRecovery,
        cancelled: &AtomicBool,
    ) -> Result<(), BootstrapError> {
        check_cancelled(self.cancelled)?;
        if !std::ptr::eq(recovery, self.recovery) {
            return Err(BootstrapError::StateMismatch);
        }
        recovery.validate_compacted_empty_initial_retirement(
            self.retirement,
            self.scratch,
            cancelled,
        )?;
        check_cancelled(self.cancelled)
    }
    pub fn resolution(
        &self,
        cancelled: &AtomicBool,
    ) -> Result<CompactedPrefixResolution<'a>, AdmissionError> {
        CompactedPrefixResolution::from_empty_retirement(*self, self.recovery, cancelled)
    }
}

pub struct ObservedCompactedEmptyInitialRetirement {
    retirement: CompactedEmptyInitialRetirement,
    _closing: ObservedEmptyInitialRetirement,
}
impl ObservedCompactedEmptyInitialRetirement {
    pub fn retirement(&self) -> &CompactedEmptyInitialRetirement {
        &self.retirement
    }
}

impl IngestRecovery {
    pub(in crate::storage) fn compacted_empty_initial_attempt<'a>(
        &'a self,
        handoff: &[u8],
        lineage: &'a RetainedLineage,
        cancelled: &'a AtomicBool,
    ) -> Result<CompactedEmptyInitialAttempt<'a>, BootstrapError> {
        let context = CompactedEmptyInitialAttempt {
            attempt: self.empty_initial_attempt(handoff)?,
            lineage,
            cancelled,
        };
        context.validate(cancelled)?;
        Ok(context)
    }
    /// Same fixed root/floor/input stamp before native dispatch and after SQL
    /// COMMIT. Only an Open-derived plan admits exactly +1 with exact Closing0.
    /// Returns whether the currently validated target is Closing0.
    pub(in crate::storage) fn validate_compacted_empty_initial_retirement(
        &self,
        retirement: &CompactedEmptyInitialRetirement,
        scratch: &OwnedPermit,
        cancelled: &AtomicBool,
    ) -> Result<bool, BootstrapError> {
        require_scratch(scratch)?;
        check_cancelled(cancelled)?;
        let target = self.empty_initial_retirement_state(&retirement.empty)?;
        self.store.handoff_value(
            IngestKey::ProducerState {
                producer_slot_id: retirement.producer_slot_id(),
                producer_epoch: ProducerEpoch::new(1).expect("initial ancestor"),
            },
            Some(&retirement.floor.encode()),
        )?;
        if self.store.compacted_input_census(
            retirement.producer_slot_id(),
            retirement.issued_through(),
            cancelled,
        )? != retirement.inputs
        {
            return Err(BootstrapError::StateMismatch);
        }
        if self.empty_initial_retirement_state(&retirement.empty)? != target {
            return Err(BootstrapError::StateMismatch);
        }
        check_cancelled(cancelled)?;
        Ok(target == EmptyTargetState::Closing)
    }
    pub(in crate::storage) fn close_compacted_empty_initial_attempt(
        &mut self,
        retirement: &CompactedEmptyInitialRetirement,
        permit: OwnedPermit,
        cancelled: &AtomicBool,
        mut authority: impl FnMut() -> Result<(), BootstrapError>,
    ) -> Result<ObservedCompactedEmptyInitialRetirement, BootstrapError> {
        require_scratch(&permit)?;
        if permit.request().units().native_bytes() < 4096 {
            return Err(BootstrapError::InsufficientReservation);
        }
        let mut authority = || {
            check_cancelled(cancelled)?;
            authority()?;
            check_cancelled(cancelled)
        };
        authority()?;
        self.validate_compacted_empty_initial_retirement(retirement, &permit, cancelled)?;
        let closing =
            self.close_empty_initial_attempt(&retirement.empty, permit, &mut authority)?;
        self.validate_compacted_empty_initial_retirement(retirement, &closing._permit, cancelled)?;
        authority()?;
        Ok(ObservedCompactedEmptyInitialRetirement {
            retirement: retirement.clone(),
            _closing: closing,
        })
    }
}
fn require_scratch(permit: &OwnedPermit) -> Result<(), BootstrapError> {
    if permit.request().units().data_bytes() < COMPACTED_EMPTY_DATA_BYTES
        || permit.request().units().items() == 0
    {
        return Err(BootstrapError::InsufficientReservation);
    }
    Ok(())
}
fn check_cancelled(cancelled: &AtomicBool) -> Result<(), BootstrapError> {
    if cancelled.load(Ordering::Relaxed) {
        Err(BootstrapError::AuthorityUnavailable)
    } else {
        Ok(())
    }
}
