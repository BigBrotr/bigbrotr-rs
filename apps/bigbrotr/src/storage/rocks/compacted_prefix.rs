//! Read-only native observation of one compacted initial producer. No PG
//! retirement, checkpoint authority, writer or reclamation capability is minted.
use super::*;
use crate::storage::rocks::store::{AdmissionError, AdmissionLimits};

#[path = "compacted_outcomes.rs"]
mod outcomes;
pub use outcomes::{
    CompactedOutcomeRow, CompactedPrefixResolution, VerifiedCompactedPrefix,
    compacted_outcome_data_bytes,
};

pub const COMPACTED_PREFIX_METADATA_BYTES: u64 = LINEAGE_SCRATCH_BYTES + LINEAGE_ENTRY_BYTES;

pub struct RetainedCompactedPrefix {
    entry: RetainedLineageEntry,
    floor: ClosedProducerFloor,
    slot: ProducerSlotId,
    root: (u64, u64),
    sequence: u64,
    inputs: [u8; 32],
    _permit: OwnedPermit,
}
const _: () = assert!(size_of::<RetainedCompactedPrefix>() < LINEAGE_SCRATCH_BYTES as usize / 4);

impl IngestRecovery {
    /// Select the initial closed-floor reader without granting any authority.
    /// Ordinary legacy decoding/recovery still refuses the new version.
    pub fn closed_producer_floor(
        &self,
        slot: ProducerSlotId,
    ) -> Result<Option<ClosedProducerFloor>, BootstrapError> {
        if self.store.write_uncertain {
            return Err(BootstrapError::CommitUnknown);
        }
        if self.manifest().key_schema() != 1 || self.manifest().identity().owner_epoch.get() != 1 {
            return Err(BootstrapError::StateMismatch);
        }
        let key = IngestKey::ProducerState {
            producer_slot_id: slot,
            producer_epoch: ProducerEpoch::new(1).expect("initial epoch"),
        }
        .encode()
        .map_err(|_| BootstrapError::InvalidIntent)?;
        let cf = self
            .store
            .db
            .cf_handle("receipts")
            .ok_or(BootstrapError::EngineRead)?;
        let bytes = self
            .store
            .db
            .get_pinned_cf_opt(cf, key.as_bytes(), &read_options())
            .map_err(|_| BootstrapError::EngineRead)?;
        match bytes.as_deref() {
            None => Ok(None),
            Some(bytes) if bytes.starts_with(&[3, 0x31]) => ClosedProducerFloor::decode(bytes)
                .map(Some)
                .map_err(|_| BootstrapError::StateMismatch),
            Some(bytes) => {
                ProducerState::decode(bytes)?;
                Ok(None)
            }
        }
    }
}

impl RetainedCompactedPrefix {
    pub fn inspect_initial(
        recovery: &IngestRecovery,
        slot: ProducerSlotId,
        limits: AdmissionLimits,
        metadata: OwnedPermit,
        effects: OwnedPermit,
        cancelled: &AtomicBool,
    ) -> Result<Self, AdmissionError> {
        if metadata.request().units().items() == 0
            || metadata.request().units().data_bytes() < COMPACTED_PREFIX_METADATA_BYTES
        {
            return Err(AdmissionError::InsufficientReservation);
        }
        check_cancelled(cancelled)?;
        if recovery.store.write_uncertain {
            return Err(AdmissionError::CommitUnknown);
        }
        let manifest = *recovery.manifest();
        if manifest.key_schema() != 1 || manifest.identity().owner_epoch.get() != 1 {
            return Err(AdmissionError::StateMismatch);
        }
        let root = recovery.store.handoff_root_identity()?;
        let sequence = recovery.store.db.latest_sequence_number();
        let bytes = value(
            recovery,
            IngestKey::ProducerState {
                producer_slot_id: slot,
                producer_epoch: ProducerEpoch::new(1).expect("initial epoch"),
            },
        )?;
        let floor =
            ClosedProducerFloor::decode(&bytes).map_err(|_| AdmissionError::StateMismatch)?;
        drop(bytes);
        let fields = floor.fields();
        if fields.manifest_sha256 != <[u8; 32]>::from(Sha256::digest(manifest.encode().as_bytes()))
        {
            return Err(AdmissionError::StateMismatch);
        }
        require_fresh(&[], &manifest)?;
        let entry = entry_coordinates(
            recovery,
            manifest,
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
        let mut observed = Self {
            entry,
            floor,
            slot,
            root,
            sequence,
            inputs: [0; 32],
            _permit: metadata,
        };
        observed.census().inspect_census(recovery, cancelled)?;
        observed
            .census()
            .verify_effect_census(recovery, 1, limits, effects, cancelled)?;
        observed.inputs = observed.input_census(recovery, cancelled)?;
        observed.validate(recovery, cancelled)?;
        Ok(observed)
    }
    pub fn manifest(&self) -> &IngestManifest {
        &self.entry.manifest
    }
    pub fn producer_slot_id(&self) -> ProducerSlotId {
        self.slot
    }
    pub fn floor(&self) -> &ClosedProducerFloor {
        &self.floor
    }
    pub fn validate(
        &self,
        recovery: &IngestRecovery,
        cancelled: &AtomicBool,
    ) -> Result<(), AdmissionError> {
        check_cancelled(cancelled)?;
        if recovery.store.write_uncertain {
            return Err(AdmissionError::CommitUnknown);
        }
        if recovery.manifest() != self.manifest()
            || recovery.store.handoff_root_identity()? != self.root
            || recovery.store.db.latest_sequence_number() != self.sequence
            || self.input_census(recovery, cancelled)? != self.inputs
            || recovery.store.handoff_root_identity()? != self.root
            || recovery.store.db.latest_sequence_number() != self.sequence
        {
            return Err(AdmissionError::StateMismatch);
        }
        check_cancelled(cancelled)?;
        Ok(())
    }
    fn census(&self) -> Census<'_> {
        Census {
            entries: std::slice::from_ref(&self.entry),
            slot: self.slot,
            compacted: Some(&self.floor),
        }
    }
    fn input_census(
        &self,
        recovery: &IngestRecovery,
        cancelled: &AtomicBool,
    ) -> Result<[u8; 32], BootstrapError> {
        recovery.store.compacted_input_census(
            self.slot,
            self.floor().fields().issued_through,
            cancelled,
        )
    }
}
