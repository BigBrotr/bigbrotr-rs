//! Borrowed ancestor checks across the exact permitted later transition delta.
//! Neither a portable record nor this view can mint a new positive lineage.
use super::*;
use crate::storage::rocks::store::{
    AdmissionError, AdmissionLimits, EmptySuccessorRetirement, SuccessorHandoff,
};

#[derive(Clone, Copy)]
enum Transition<'a> {
    Handoff(&'a SuccessorHandoff),
    Empty(&'a EmptySuccessorRetirement),
}

#[derive(Clone, Copy)]
pub(in crate::storage) struct CompactedSuccessorPredecessor<'a> {
    lineage: &'a RetainedLineage,
    recovery: &'a IngestRecovery,
    transition: Transition<'a>,
    cancelled: &'a AtomicBool,
}
impl<'a> CompactedSuccessorPredecessor<'a> {
    pub(in crate::storage) fn handoff(
        lineage: &'a RetainedLineage,
        recovery: &'a IngestRecovery,
        handoff: &'a SuccessorHandoff,
        limits: AdmissionLimits,
        effects: &OwnedPermit,
        cancelled: &'a AtomicBool,
    ) -> Result<Self, AdmissionError> {
        Self::new(
            lineage,
            recovery,
            Transition::Handoff(handoff),
            limits,
            effects,
            cancelled,
        )
    }

    pub(in crate::storage) fn empty(
        lineage: &'a RetainedLineage,
        recovery: &'a IngestRecovery,
        plan: &'a EmptySuccessorRetirement,
        limits: AdmissionLimits,
        effects: &OwnedPermit,
        cancelled: &'a AtomicBool,
    ) -> Result<Self, AdmissionError> {
        Self::new(
            lineage,
            recovery,
            Transition::Empty(plan),
            limits,
            effects,
            cancelled,
        )
    }

    fn new(
        lineage: &'a RetainedLineage,
        recovery: &'a IngestRecovery,
        transition: Transition<'a>,
        limits: AdmissionLimits,
        effects: &OwnedPermit,
        cancelled: &'a AtomicBool,
    ) -> Result<Self, AdmissionError> {
        let view = Self {
            lineage,
            recovery,
            transition,
            cancelled,
        };
        view.validate(recovery, cancelled)?;
        // The permitted writes affect only metadata. All original science is
        // checked under the old census; the new target has no facts or inputs.
        lineage.census().verify_effect_census(
            recovery,
            lineage
                .native_head()
                .manifest()
                .identity()
                .owner_epoch
                .get(),
            limits,
            effects,
            cancelled,
        )?;
        view.validate(recovery, cancelled)?;
        Ok(view)
    }

    pub(in crate::storage) fn manifest(&self) -> &IngestManifest {
        self.lineage.entry(1).expect("checked ancestor").manifest()
    }
    pub(in crate::storage) fn floor(&self) -> &ClosedProducerFloor {
        self.lineage
            .compacted
            .as_ref()
            .expect("checked compacted basis")
    }
    pub(in crate::storage) fn producer_slot_id(&self) -> ProducerSlotId {
        self.lineage.slot
    }
    pub(in crate::storage) fn validate(
        &self,
        recovery: &IngestRecovery,
        cancelled: &AtomicBool,
    ) -> Result<(), AdmissionError> {
        check_cancelled(self.cancelled)?;
        match self.transition {
            Transition::Handoff(plan) => {
                recovery.validate_compacted_successor_handoff(self.lineage, plan, cancelled)?
            }
            Transition::Empty(plan) => recovery.validate_compacted_empty_successor_retirement(
                self.lineage,
                plan,
                cancelled,
            )?,
        }
        check_cancelled(self.cancelled)?;
        Ok(())
    }
    pub(in crate::storage::rocks::store) fn require_input(
        &self,
        recovery: &IngestRecovery,
        checked: (ProducerRequestId, [u8; 32], u64),
        cancelled: &AtomicBool,
    ) -> Result<(), AdmissionError> {
        self.validate(recovery, cancelled)?;
        let stamp = recovery.store.require_compacted_lineage_input(
            self.lineage.input_set()?,
            |id| self.lineage.accepts_input(id),
            checked,
            cancelled,
        )?;
        if stamp != self.lineage.inputs {
            return Err(AdmissionError::StateMismatch);
        }
        self.validate(recovery, cancelled)
    }
    pub(in crate::storage) fn resolution(
        &self,
        cancelled: &AtomicBool,
    ) -> Result<CompactedPrefixResolution<'a>, AdmissionError> {
        CompactedPrefixResolution::from_successor(*self, self.recovery, cancelled)
    }
}
