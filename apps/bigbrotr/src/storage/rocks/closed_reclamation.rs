//! One reusable closed-producer reclamation kernel. Only connected, confirmed
//! terminal factories may authorize it; portable floors and ACKs cannot do so.
use super::*;
use crate::storage::rocks::{
    store::request_inputs::ClosedInputInventory,
    store::{AdmissionError, AdmissionLimits, ResolvedSource},
};

pub(in crate::storage) struct ClosedProducerReclamation {
    manifest: IngestManifest,
    slot: ProducerSlotId,
    epoch: ProducerEpoch,
    floor: ClosedProducerFloor,
    custody: std::sync::Arc<()>,
    root: (u64, u64),
    sequence: u64,
    compacted: bool,
    inventory: ClosedInputInventory,
    _metadata: OwnedPermit,
}
impl ClosedProducerReclamation {
    pub(in crate::storage) const DATA_BYTES: u64 = 128 * 1024;
    pub(in crate::storage) const NATIVE_BYTES: u64 = 16 * 1024;

    /// Narrow presently available authority: genuine original initial replay.
    /// This supplies mechanics, never PostgreSQL terminal authority.
    pub(in crate::storage) fn initial_original(
        source: &ResolvedSource<'_>,
        floor: ClosedProducerFloor,
        limits: AdmissionLimits,
        metadata: OwnedPermit,
        effects: &OwnedPermit,
        cancelled: &AtomicBool,
    ) -> Result<Self, AdmissionError> {
        source.validate_initial(cancelled)?;
        let recovery = source.recovery();
        let slot = source.producer().producer_slot_id();
        let fields = floor.fields();
        if source.resolved_requests() != fields.issued_through
            || source.acknowledged_through() != fields.issued_through
            || source.last_complete_sequence() != fields.source_through
            || source.digest() != fields.closure_digest
            || fields.manifest_sha256
                != <[u8; 32]>::from(Sha256::digest(recovery.manifest().encode().as_bytes()))
        {
            return Err(AdmissionError::StateMismatch);
        }
        let producer = recovery
            .store
            .producer_state(slot, source.producer().producer_epoch())?
            .ok_or(AdmissionError::StateMismatch)?;
        if !producer.closing || producer.retired != 0 || producer.issued != producer.acknowledged {
            return Err(AdmissionError::StateMismatch);
        }
        let entry = entry_coordinates(
            recovery,
            *recovery.manifest(),
            [0; 32],
            Some((
                RetainedTerminalKind::Initial,
                fields.issued_through,
                fields.source_through,
                fields.closure_digest,
            )),
            producer,
        )?;
        let census = Census {
            entries: std::slice::from_ref(&entry).into(),
            slot,
            compacted: None,
        };
        census.inspect_census(recovery, cancelled)?;
        census.verify_effect_census(recovery, 1, limits, effects, cancelled)?;
        let plan = Self::capture(recovery, slot, floor, false, metadata, cancelled)?;
        source.validate_initial(cancelled)?;
        Ok(plan)
    }
    /// The canonical compacted stream authenticates every present residual;
    /// a fresh retry captures only that exact authenticated residual inventory.
    pub(in crate::storage) fn initial_compacted(
        recovery: &IngestRecovery,
        proof: &VerifiedCompactedPrefix<'_>,
        metadata: OwnedPermit,
        cancelled: &AtomicBool,
    ) -> Result<Self, AdmissionError> {
        proof.validate(recovery, cancelled)?;
        if recovery.manifest() != proof.manifest()
            || recovery.manifest().key_schema() != 1
            || recovery.manifest().identity().owner_epoch.get() != 1
        {
            return Err(AdmissionError::StateMismatch);
        }
        let plan = Self::capture(
            recovery,
            proof.producer_slot_id(),
            *proof.floor(),
            true,
            metadata,
            cancelled,
        )?;
        proof.validate(recovery, cancelled)?;
        Ok(plan)
    }
    fn capture(
        recovery: &IngestRecovery,
        slot: ProducerSlotId,
        floor: ClosedProducerFloor,
        compacted: bool,
        metadata: OwnedPermit,
        cancelled: &AtomicBool,
    ) -> Result<Self, AdmissionError> {
        if metadata.request().units().data_bytes() < Self::DATA_BYTES
            || metadata.request().units().native_bytes() < Self::NATIVE_BYTES
            || metadata.request().units().items() == 0
        {
            return Err(AdmissionError::InsufficientReservation);
        }
        let root = recovery.store.handoff_root_identity()?;
        let sequence = recovery.store.db.latest_sequence_number();
        let inventory = recovery.store.closed_input_inventory(
            slot,
            floor.fields().issued_through,
            !compacted,
            cancelled,
        )?;
        let plan = Self {
            manifest: *recovery.manifest(),
            slot,
            epoch: ProducerEpoch::new(1).expect("initial constructor"),
            floor,
            custody: std::sync::Arc::clone(&recovery.store._reservation.custody),
            root,
            sequence,
            compacted,
            inventory,
            _metadata: metadata,
        };
        plan.validate(recovery, cancelled)?;
        Ok(plan)
    }
    pub(in crate::storage) fn manifest(&self) -> &IngestManifest {
        &self.manifest
    }
    pub(in crate::storage) fn floor(&self) -> &ClosedProducerFloor {
        &self.floor
    }
    pub(in crate::storage) fn producer_slot_id(&self) -> ProducerSlotId {
        self.slot
    }
    pub(in crate::storage) fn resolution<'a>(
        &'a self,
        recovery: &'a IngestRecovery,
        cancelled: &AtomicBool,
    ) -> Result<CompactedPrefixResolution<'a>, AdmissionError> {
        CompactedPrefixResolution::from_reclamation(self, recovery, cancelled)
    }
    pub(super) fn require_input(
        &self,
        recovery: &IngestRecovery,
        checked: (ProducerRequestId, [u8; 32], u64),
        cancelled: &AtomicBool,
    ) -> Result<(), AdmissionError> {
        self.validate(recovery, cancelled)?;
        recovery.store.require_compacted_input(
            self.floor.fields().issued_through,
            checked.0,
            checked.1,
            checked.2,
            cancelled,
        )?;
        self.validate(recovery, cancelled)
    }
    pub(in crate::storage) fn validate(
        &self,
        recovery: &IngestRecovery,
        cancelled: &AtomicBool,
    ) -> Result<(), AdmissionError> {
        self.require_store(&recovery.store, cancelled)
    }
    fn require_store(
        &self,
        store: &IngestStore,
        cancelled: &AtomicBool,
    ) -> Result<(), AdmissionError> {
        check_cancelled(cancelled)?;
        if store.write_uncertain {
            return Err(AdmissionError::CommitUnknown);
        }
        if store.manifest() != &self.manifest
            || !std::sync::Arc::ptr_eq(&store._reservation.custody, &self.custody)
            || store.handoff_root_identity()? != self.root
            || store.db.latest_sequence_number() != self.sequence
            || store.live_initial.is_some()
            || store.live_successor.is_some()
        {
            return Err(AdmissionError::StateMismatch);
        }
        store.require_closed_input_inventory(&self.inventory, cancelled)?;
        if store.handoff_root_identity()? != self.root
            || store.db.latest_sequence_number() != self.sequence
        {
            return Err(AdmissionError::StateMismatch);
        }
        check_cancelled(cancelled)?;
        Ok(())
    }
    /// Consumed only after a positively observed PostgreSQL COMMIT. Native
    /// success is observed before any unlink, and exactly our batch's sequence
    /// delta is admitted. An exact floor retry performs no native write.
    fn apply(
        mut self,
        recovery: &mut IngestRecovery,
        cancelled: &AtomicBool,
        mut authority: impl FnMut() -> Result<(), AdmissionError>,
    ) -> Result<ClosedReclamationReceipt, AdmissionError> {
        authority()?;
        self.validate(recovery, cancelled)?;
        let store = &mut recovery.store;
        let marker = IngestKey::ProducerState {
            producer_slot_id: self.slot,
            producer_epoch: self.epoch,
        };
        if self.compacted {
            store.handoff_value(marker, Some(&self.floor.encode()))?;
        } else {
            let state = store
                .producer_state(self.slot, self.epoch)?
                .ok_or(AdmissionError::StateMismatch)?;
            let n = self.floor.fields().issued_through;
            if !state.closing || state.issued != n || state.acknowledged != n || state.retired != 0
            {
                return Err(AdmissionError::StateMismatch);
            }
            let expected = self
                .sequence
                .checked_add(n + 1)
                .ok_or(AdmissionError::Limit)?;
            let cf = store
                .db
                .cf_handle("receipts")
                .ok_or(AdmissionError::EngineRead)?;
            let mut batch = rocksdb::WriteBatch::with_capacity_bytes(Self::NATIVE_BYTES as usize);
            batch.put_cf(
                cf,
                marker
                    .encode()
                    .map_err(|_| AdmissionError::InvalidInput)?
                    .as_bytes(),
                self.floor.encode(),
            );
            for sequence in 1..=n {
                let request_id = bigbrotr_model::ProducerRequestId {
                    producer_slot_id: self.slot,
                    producer_epoch: self.epoch,
                    request_sequence: bigbrotr_model::RequestSequence::new(sequence)
                        .map_err(|_| AdmissionError::InvalidInput)?,
                };
                let key = IngestKey::Request { request_id }
                    .encode()
                    .map_err(|_| AdmissionError::InvalidInput)?;
                if store
                    .db
                    .get_pinned_cf_opt(cf, key.as_bytes(), &read_options())
                    .map_err(|_| AdmissionError::EngineRead)?
                    .is_none()
                {
                    return Err(AdmissionError::StateMismatch);
                }
                batch.delete_cf(cf, key.as_bytes());
            }
            if batch.size_in_bytes() > Self::NATIVE_BYTES as usize {
                return Err(AdmissionError::Limit);
            }
            self.require_store(store, cancelled)?;
            authority()?;
            let mut options = rocksdb::WriteOptions::default();
            options.set_sync(true);
            options.disable_wal(false);
            options.set_ignore_missing_column_families(false);
            store.write_uncertain = true;
            store
                .db
                .write_opt(batch, &options)
                .map_err(|_| AdmissionError::CommitUnknown)?;
            if store.db.latest_sequence_number() != expected {
                return Err(AdmissionError::CommitUnknown);
            }
            store.write_uncertain = false;
            self.sequence = expected;
            self.compacted = true;
            self.require_store(store, cancelled)?;
            store.handoff_value(marker, Some(&self.floor.encode()))?;
        }
        authority()?;
        let before =
            self.inventory
                .range_count(self.slot, self.epoch, self.floor.fields().issued_through);
        let removed = self.inventory.remove_range(
            store,
            self.slot,
            self.epoch,
            self.floor.fields().issued_through,
            cancelled,
            || authority().map_err(|_| BootstrapError::AuthorityUnavailable),
        )?;
        if removed != before {
            return Err(AdmissionError::StateMismatch);
        }
        self.require_store(store, cancelled)?;
        authority()?;
        Ok(ClosedReclamationReceipt {
            requests: self.floor.fields().issued_through,
            removed,
            native_sequence: self.sequence,
        })
    }
}
/// Reporting only; no writer, proof or deletion capability follows from this.
pub struct ClosedReclamationReceipt {
    requests: u64,
    removed: u64,
    native_sequence: u64,
}
impl ClosedReclamationReceipt {
    pub fn retired_requests(&self) -> u64 {
        self.requests
    }
    pub fn removed_input_files(&self) -> u64 {
        self.removed
    }
    pub fn native_sequence(&self) -> u64 {
        self.native_sequence
    }
}

impl IngestRecovery {
    pub(in crate::storage) fn reclaim_closed_producer(
        &mut self,
        confirmed: crate::storage::postgres::control_owner::ConfirmedNativeReclamation,
        cancelled: &AtomicBool,
        authority: impl FnMut() -> Result<(), AdmissionError>,
    ) -> Result<ClosedReclamationReceipt, AdmissionError> {
        confirmed.into_plan().apply(self, cancelled, authority)
    }
}
#[cfg(test)]
impl ClosedProducerReclamation {
    pub(in crate::storage::rocks::store) fn test_apply(
        self,
        recovery: &mut IngestRecovery,
        cancelled: &AtomicBool,
        authority: impl FnMut() -> Result<(), AdmissionError>,
    ) -> Result<ClosedReclamationReceipt, AdmissionError> {
        self.apply(recovery, cancelled, authority)
    }
}
