//! One borrowed compacted basis for the continuously retained initial successor.
//! Each observation authenticates the entire native state and exact input set;
//! no floor bytes or failed original replay can select this capability.
use super::*;
use crate::storage::rocks::store::{
    ActiveInitialWriter, AdmissionError, AdmissionLimits, ProgressRead,
};

#[derive(Clone, Copy)]
pub(in crate::storage) struct CompactedLiveInitial<'a> {
    writer: &'a ActiveInitialWriter,
    scratch: &'a OwnedPermit,
    sequence: u64,
    inputs: [u8; 32],
    issued: u64,
}
impl ActiveInitialWriter {
    pub(in crate::storage) fn compacted_initial_view<'a>(
        &'a self,
        limits: AdmissionLimits,
        scratch: &'a OwnedPermit,
        effects: &OwnedPermit,
        cancelled: &AtomicBool,
    ) -> Result<Option<CompactedLiveInitial<'a>>, AdmissionError> {
        let Some(floor) = self.handoff().compacted_floor() else {
            return Ok(None);
        };
        if scratch.request().units().items() == 0
            || scratch.request().units().data_bytes() < LINEAGE_SCRATCH_BYTES
        {
            return Err(AdmissionError::InsufficientReservation);
        }
        self.handoff().require_live_source(self.store())?;
        let sequence = self.store().db.latest_sequence_number();
        let fields = floor.fields();
        let slot = self.producer_slot_id();
        let predecessor = entry_coordinates_in(
            self.store(),
            *self.handoff().previous_manifest(),
            [0; 32],
            Some((
                RetainedTerminalKind::Initial,
                fields.issued_through,
                fields.source_through,
                fields.closure_digest,
            )),
            ProducerState {
                issued: fields.issued_through,
                acknowledged: fields.issued_through,
                retired: fields.issued_through,
                closing: true,
            },
        )?;
        let state = self
            .store()
            .producer_state(slot, ProducerEpoch::new(2).expect("live initial successor"))?
            .ok_or(AdmissionError::StateMismatch)?;
        if state.closing || state.retired != 0 || state.issued > 64 {
            return Err(AdmissionError::StateMismatch);
        }
        let current = entry_coordinates_in(
            self.store(),
            *self.manifest(),
            Sha256::digest(self.handoff().encode().as_bytes()).into(),
            None,
            state,
        )?;
        let entries = [predecessor, current];
        let census = Census {
            entries: entries.as_slice().into(),
            slot,
            compacted: Some(floor),
        };
        census.inspect_store_census(self.store(), cancelled)?;
        census.verify_store_effect_census(self.store(), 2, limits, effects, cancelled)?;
        let mut view = CompactedLiveInitial {
            writer: self,
            scratch,
            sequence,
            inputs: [0; 32],
            issued: state.issued,
        };
        view.inputs = view.input_census(cancelled)?;
        view.validate(cancelled)?;
        Ok(Some(view))
    }
}
impl<'a> CompactedLiveInitial<'a> {
    pub(in crate::storage) fn store(&self) -> &'a IngestStore {
        self.writer.store()
    }
    pub(in crate::storage) fn manifest(&self) -> &'a IngestManifest {
        self.writer.handoff().previous_manifest()
    }
    pub(in crate::storage) fn floor(&self) -> &'a ClosedProducerFloor {
        self.writer
            .handoff()
            .compacted_floor()
            .expect("sealed compacted view")
    }
    pub(in crate::storage) fn producer_slot_id(&self) -> ProducerSlotId {
        self.writer.producer_slot_id()
    }
    pub(in crate::storage) fn progress_scratch(&self) -> &'a OwnedPermit {
        self.scratch
    }
    pub(in crate::storage) fn progress_read(&self) -> Result<ProgressRead<'a>, AdmissionError> {
        self.store().progress_read(self.scratch)
    }
    fn input_set(&self) -> Result<super::super::request_inputs::LineageInputSet, AdmissionError> {
        Ok(
            super::super::request_inputs::LineageInputSet::with_compacted_initial(
                self.issued,
                self.producer_slot_id(),
                self.floor().fields().issued_through,
            )?,
        )
    }
    fn accepts(&self, id: ProducerRequestId) -> bool {
        id.producer_slot_id == self.producer_slot_id()
            && id.producer_epoch.get() == 2
            && id.request_sequence.get() <= self.issued
    }
    fn input_census(&self, cancelled: &AtomicBool) -> Result<[u8; 32], AdmissionError> {
        Ok(self.store().lineage_input_census(
            self.input_set()?,
            |id| self.accepts(id),
            cancelled,
        )?)
    }
    pub(in crate::storage) fn validate(
        &self,
        cancelled: &AtomicBool,
    ) -> Result<(), AdmissionError> {
        check_cancelled(cancelled)?;
        self.writer.handoff().require_live_source(self.store())?;
        if self.store().db.latest_sequence_number() != self.sequence
            || self.input_census(cancelled)? != self.inputs
            || self.store().db.latest_sequence_number() != self.sequence
        {
            return Err(AdmissionError::StateMismatch);
        }
        Ok(())
    }
    pub(in crate::storage) fn require_input(
        &self,
        checked: (ProducerRequestId, [u8; 32], u64),
        cancelled: &AtomicBool,
    ) -> Result<(), AdmissionError> {
        self.validate(cancelled)?;
        if self.store().require_compacted_lineage_input(
            self.input_set()?,
            |id| self.accepts(id),
            checked,
            cancelled,
        )? != self.inputs
        {
            return Err(AdmissionError::StateMismatch);
        }
        self.validate(cancelled)
    }
    pub(in crate::storage) fn resolution(
        &self,
        cancelled: &AtomicBool,
    ) -> Result<CompactedPrefixResolution<'a>, AdmissionError> {
        CompactedPrefixResolution::from_live_initial(*self, cancelled)
    }
}
