//! Explicit compacted predecessor plans. Native consistency remains separate
//! from PostgreSQL retirement, preparation and activation authority.
use super::*;
use crate::storage::rocks::{
    producer_floor::ClosedProducerFloor,
    store::{CompactedPrefixResolution, VerifiedCompactedPrefix},
    succession::LINEAGE_SCRATCH_BYTES,
};
use std::sync::atomic::{AtomicBool, Ordering};

/// Reuses the caller's serial metadata/input scan grant; no second allocation.
pub const COMPACTED_HANDOFF_DATA_BYTES: u64 = LINEAGE_SCRATCH_BYTES;
const _: () = assert!(
    2 * std::mem::size_of::<InitialHandoff>() + std::mem::size_of::<EncodedHandoff>() < 8192
);

/// A sealed plan's exact predecessor under the actual native owner and retained
/// scratch grant. It can stream canonical rows, never supply OriginalReplay.
#[derive(Clone, Copy)]
pub struct CompactedInitialPredecessor<'a> {
    handoff: &'a InitialHandoff,
    recovery: &'a IngestRecovery,
    scratch: &'a OwnedPermit,
}
impl<'a> CompactedInitialPredecessor<'a> {
    pub fn manifest(&self) -> &IngestManifest {
        self.handoff.previous_manifest()
    }
    pub fn floor(&self) -> &ClosedProducerFloor {
        match &self.handoff.basis {
            InitialPredecessorBasis::Compacted { floor, .. } => floor,
            InitialPredecessorBasis::Original => unreachable!("sealed compacted view"),
        }
    }
    pub fn producer_slot_id(&self) -> ProducerSlotId {
        self.handoff.producer_slot_id()
    }
    pub fn validate(
        &self,
        recovery: &IngestRecovery,
        cancelled: &AtomicBool,
    ) -> Result<(), BootstrapError> {
        if !std::ptr::eq(self.recovery, recovery) {
            return Err(BootstrapError::StateMismatch);
        }
        recovery.validate_compacted_initial_handoff(self.handoff, self.scratch, cancelled)
    }
    pub fn resolution(
        &self,
        cancelled: &AtomicBool,
    ) -> Result<CompactedPrefixResolution<'a>, AdmissionError> {
        CompactedPrefixResolution::from_handoff(*self, self.recovery, cancelled)
    }
    pub(in crate::storage::rocks::store) fn input_stamp(&self) -> [u8; 32] {
        match self.handoff.basis {
            InitialPredecessorBasis::Compacted { inputs, .. } => inputs,
            InitialPredecessorBasis::Original => unreachable!("sealed compacted view"),
        }
    }
}

impl InitialHandoff {
    /// Only a complete native canonical-outcome proof at the current initial
    /// floor can mint this basis. SQL authority is still a separate obligation.
    pub fn from_compacted(
        source: &VerifiedCompactedPrefix<'_>,
        next: IngestManifest,
        cancelled: &AtomicBool,
    ) -> Result<Self, AdmissionError> {
        let (recovery, inputs) = source.handoff_context(cancelled)?;
        let previous = *source.manifest();
        if recovery.manifest() != &previous {
            return Err(AdmissionError::StateMismatch);
        }
        let epoch = ProducerEpoch::new(1).expect("initial floor");
        validate_transition(previous, next, epoch)?;
        let floor = *source.floor();
        Ok(Self {
            record: InitialHandoffRecord {
                previous,
                next,
                slot: source.producer_slot_id(),
                epoch,
                issued: floor.fields().issued_through,
                source_through: floor.fields().source_through,
                closure_digest: floor.fields().closure_digest,
            },
            native_sequence: recovery.store.db.latest_sequence_number(),
            native_root: recovery.store.handoff_root_identity()?,
            basis: InitialPredecessorBasis::Compacted { floor, inputs },
        })
    }

    /// The explicit basis discriminator returns a checked borrowed cursor owner,
    /// not raw floor metadata that could be promoted into a native proof.
    pub fn compacted_predecessor<'a>(
        &'a self,
        recovery: &'a IngestRecovery,
        scratch: &'a OwnedPermit,
        cancelled: &AtomicBool,
    ) -> Result<Option<CompactedInitialPredecessor<'a>>, BootstrapError> {
        if matches!(self.basis, InitialPredecessorBasis::Original) {
            return Ok(None);
        }
        recovery.validate_compacted_initial_handoff(self, scratch, cancelled)?;
        Ok(Some(CompactedInitialPredecessor {
            handoff: self,
            recovery,
            scratch,
        }))
    }
}

impl HistoricalInitial<'_> {
    /// Rebind only an exact empty Prepared2 target after a complete two-entry
    /// scientific census and ordered compacted initial canonical validation.
    pub(in crate::storage) fn reconstruct_compacted_handoff(
        &self,
        source: &VerifiedCompactedPrefix<'_>,
        cancelled: &AtomicBool,
    ) -> Result<InitialHandoff, AdmissionError> {
        check_cancelled(cancelled)?;
        self.validate()?;
        let (recovery, inputs) = source.handoff_context(cancelled)?;
        let floor = *source.floor();
        let fields = floor.fields();
        if self.target != EmptyTargetState::Open
            || !std::ptr::eq(recovery, self.recovery)
            || source.manifest() != self.manifest()
            || source.producer_slot_id() != self.record.slot
            || fields.issued_through != self.record.issued
            || fields.source_through != self.record.source_through
            || fields.closure_digest != self.record.closure_digest
        {
            return Err(AdmissionError::StateMismatch);
        }
        let handoff = InitialHandoff {
            record: self.record,
            native_sequence: self
                .sequence
                .checked_sub(4)
                .ok_or(AdmissionError::StateMismatch)?,
            native_root: self.root,
            basis: InitialPredecessorBasis::Compacted { floor, inputs },
        };
        // The verified source retains the already checked input/census grant.
        // This exact empty-target snapshot supplies the only permitted +4 state.
        self.recovery.validate_initial_handoff_snapshot(&handoff)?;
        Ok(handoff)
    }
}

impl IngestRecovery {
    pub(in crate::storage) fn validate_compacted_initial_handoff(
        &self,
        handoff: &InitialHandoff,
        scratch: &OwnedPermit,
        cancelled: &AtomicBool,
    ) -> Result<(), BootstrapError> {
        require_scratch(scratch)?;
        check_cancelled(cancelled)?;
        let InitialPredecessorBasis::Compacted { floor, inputs } = &handoff.basis else {
            return Err(BootstrapError::StateMismatch);
        };
        self.validate_initial_handoff_snapshot(handoff)?;
        self.store.handoff_value(
            IngestKey::ProducerState {
                producer_slot_id: handoff.producer_slot_id(),
                producer_epoch: handoff.producer_epoch(),
            },
            Some(&floor.encode()),
        )?;
        if self.store.compacted_input_census(
            handoff.producer_slot_id(),
            floor.fields().issued_through,
            cancelled,
        )? != *inputs
        {
            return Err(BootstrapError::StateMismatch);
        }
        self.validate_initial_handoff_snapshot(handoff)?;
        check_cancelled(cancelled)
    }

    pub(in crate::storage) fn apply_compacted_initial_handoff(
        &mut self,
        handoff: &InitialHandoff,
        permit: OwnedPermit,
        cancelled: &AtomicBool,
        mut authority: impl FnMut() -> Result<(), BootstrapError>,
    ) -> Result<ObservedHandoff, BootstrapError> {
        require_scratch(&permit)?;
        if permit.request().units().native_bytes() < 8192 {
            return Err(BootstrapError::InsufficientReservation);
        }
        let mut authority = || {
            check_cancelled(cancelled)?;
            authority()
        };
        authority()?;
        self.validate_compacted_initial_handoff(handoff, &permit, cancelled)?;
        let observed = self.apply_checked_initial_handoff(handoff, permit, &mut authority)?;
        self.validate_compacted_initial_handoff(handoff, &observed._permit, cancelled)?;
        authority()?;
        Ok(observed)
    }
}
fn require_scratch(permit: &OwnedPermit) -> Result<(), BootstrapError> {
    if permit.request().units().data_bytes() < COMPACTED_HANDOFF_DATA_BYTES
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
