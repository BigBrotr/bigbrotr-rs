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

/// Full native consistency of a compacted initial ancestor, borrowing the
/// single retained lineage allocation and actual excluded native owner. Neither
/// this view nor its canonical-outcome result enables positive original replay.
pub struct CompactedInitialAncestor<'a> {
    lineage: &'a RetainedLineage,
    recovery: &'a IngestRecovery,
}

// One private borrowed interpretation shared by the canonical outcome reducer.
// The strict initial owner retains its own entry/grant; an ancestor borrows the
// existing lineage entry and grant, never a second frontier allocation.
#[derive(Clone, Copy)]
enum PrefixOwner<'a> {
    Initial(&'a RetainedCompactedPrefix),
    Ancestor(&'a RetainedLineage),
}
// The optional floor and all simultaneous borrowed cursor headers use the
// existing metadata scratch, separately from the one capacity-charged Vec.
const _: () = assert!(
    size_of::<RetainedLineage>()
        + size_of::<CompactedInitialAncestor<'_>>()
        + size_of::<CompactedPrefixResolution<'_>>()
        + size_of::<VerifiedCompactedPrefix<'_>>()
        < LINEAGE_SCRATCH_BYTES as usize / 4
);
impl PrefixOwner<'_> {
    fn manifest(&self) -> &IngestManifest {
        match self {
            Self::Initial(prefix) => prefix.manifest(),
            Self::Ancestor(lineage) => lineage.entry(1).expect("inspected ancestor").manifest(),
        }
    }
    fn floor(&self) -> &ClosedProducerFloor {
        match self {
            Self::Initial(prefix) => prefix.floor(),
            Self::Ancestor(lineage) => lineage.compacted.as_ref().expect("sealed floor view"),
        }
    }
    fn slot(&self) -> ProducerSlotId {
        match self {
            Self::Initial(prefix) => prefix.slot,
            Self::Ancestor(lineage) => lineage.slot,
        }
    }
    fn validate(
        &self,
        recovery: &IngestRecovery,
        cancelled: &AtomicBool,
    ) -> Result<(), AdmissionError> {
        match self {
            Self::Initial(prefix) => prefix.validate(recovery, cancelled),
            Self::Ancestor(lineage) => lineage.validate(recovery, cancelled).map_err(Into::into),
        }
    }
    fn require_input(
        &self,
        recovery: &IngestRecovery,
        checked: (ProducerRequestId, [u8; 32], u64),
        cancelled: &AtomicBool,
    ) -> Result<(), AdmissionError> {
        let (actual, expected) = match self {
            Self::Initial(prefix) => (
                recovery.store.require_compacted_input(
                    prefix.floor.fields().issued_through,
                    checked.0,
                    checked.1,
                    checked.2,
                    cancelled,
                )?,
                prefix.inputs,
            ),
            Self::Ancestor(lineage) => (
                recovery.store.require_compacted_lineage_input(
                    lineage.input_set()?,
                    |id| lineage.accepts_input(id),
                    checked,
                    cancelled,
                )?,
                lineage.inputs,
            ),
        };
        if actual != expected {
            return Err(AdmissionError::StateMismatch);
        }
        Ok(())
    }
}

impl RetainedLineage {
    /// Validate every retained scientific/index/pending stream before exposing
    /// the ancestor reducer. Later original requests are still unproved; the
    /// ordinary positive cursor deliberately refuses this structural object.
    pub fn compacted_initial_ancestor<'a>(
        &'a self,
        recovery: &'a IngestRecovery,
        limits: AdmissionLimits,
        effects: OwnedPermit,
        cancelled: &AtomicBool,
    ) -> Result<Option<CompactedInitialAncestor<'a>>, AdmissionError> {
        self.compacted_initial_ancestor_with_permit(recovery, limits, &effects, cancelled)
    }

    /// Connected canonical replay reuses the same effect grant serially after
    /// this census. Neither the view nor its cursor retains decoded payloads.
    pub(in crate::storage) fn compacted_initial_ancestor_with_permit<'a>(
        &'a self,
        recovery: &'a IngestRecovery,
        limits: AdmissionLimits,
        effects: &OwnedPermit,
        cancelled: &AtomicBool,
    ) -> Result<Option<CompactedInitialAncestor<'a>>, AdmissionError> {
        if self.compacted.is_none() {
            return Ok(None);
        }
        if effects.request().units().items() == 0
            || effects.request().units().data_bytes()
                < limits.required_dictionary_replay_data_bytes()?
        {
            return Err(AdmissionError::InsufficientReservation);
        }
        self.validate(recovery, cancelled)?;
        self.census().verify_effect_census(
            recovery,
            self.native_head().manifest().identity().owner_epoch.get(),
            limits,
            effects,
            cancelled,
        )?;
        self.validate(recovery, cancelled)?;
        Ok(Some(CompactedInitialAncestor {
            lineage: self,
            recovery,
        }))
    }
}

impl<'a> CompactedInitialAncestor<'a> {
    pub(in crate::storage) fn required_effect_data_bytes(
        limits: AdmissionLimits,
    ) -> Result<u64, AdmissionError> {
        limits.required_dictionary_replay_data_bytes()
    }
    pub fn manifest(&self) -> &IngestManifest {
        self.lineage
            .entry(1)
            .expect("inspected ancestor")
            .manifest()
    }
    pub fn floor(&self) -> &ClosedProducerFloor {
        self.lineage.compacted.as_ref().expect("sealed floor view")
    }
    pub fn producer_slot_id(&self) -> ProducerSlotId {
        self.lineage.slot
    }
    pub fn validate(
        &self,
        recovery: &IngestRecovery,
        cancelled: &AtomicBool,
    ) -> Result<(), AdmissionError> {
        PrefixOwner::Ancestor(self.lineage).validate(recovery, cancelled)
    }
    pub fn resolution(
        &self,
        cancelled: &AtomicBool,
    ) -> Result<CompactedPrefixResolution<'a>, AdmissionError> {
        CompactedPrefixResolution::new(
            PrefixOwner::Ancestor(self.lineage),
            self.recovery,
            cancelled,
        )
    }
}

/// Only the exact first handoff can select the original manifest/count/cut/D.
/// All other producer markers keep the generic decoder's strict R=0 boundary.
pub(super) fn initial_entry(
    recovery: &IngestRecovery,
    initial: &InitialHandoffRecord,
    allowed: bool,
) -> Result<Option<(RetainedLineageEntry, ClosedProducerFloor)>, BootstrapError> {
    if !allowed {
        return Ok(None);
    }
    let manifest = *initial.previous_manifest();
    let bytes = value(
        recovery,
        IngestKey::ProducerState {
            producer_slot_id: initial.producer_slot_id(),
            producer_epoch: ProducerEpoch::new(1).expect("initial ancestor"),
        },
    )?;
    if !bytes.starts_with(&[3, 0x31]) {
        ProducerState::decode(&bytes)?;
        return Ok(None);
    }
    let floor = ClosedProducerFloor::decode(&bytes).map_err(|_| BootstrapError::StateMismatch)?;
    let fields = floor.fields();
    if manifest.key_schema() != 1
        || manifest.identity().owner_epoch.get() != 1
        || fields.manifest_sha256 != <[u8; 32]>::from(Sha256::digest(manifest.encode().as_bytes()))
        || fields.issued_through != initial.issued_through()
        || fields.source_through != initial.last_complete_sequence()
        || fields.closure_digest != initial.closure_digest()
    {
        return Err(BootstrapError::StateMismatch);
    }
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
    Ok(Some((entry, floor)))
}

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
            .verify_effect_census(recovery, 1, limits, &effects, cancelled)?;
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
