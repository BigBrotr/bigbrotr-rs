//! Exact native retirement and authenticated input delta. Neither a portable
//! base nor this plan grants effects; only a confirmed SQL prepare can apply it.
use super::*;
use crate::storage::{postgres::control_owner::ConfirmedPrefixPrepare, rocks::keys::IngestKey};
use bigbrotr_model::{ProducerEpoch, RequestSequence};

pub(in crate::storage) struct PrefixRetirement {
    base: Base,
    reference: BaseRef,
    old_retired: u64,
    pending_bytes: u64,
    closing: bool,
    root: (u64, u64),
    sequence: u64,
    custody: std::sync::Arc<()>,
    inventory: ClosedInputInventory,
    _metadata: OwnedPermit,
}

/// Private post-unlink witness. It retains the exact inventory, native sequence
/// and physical owner until SQL promotion/deletion has positively committed.
pub(in crate::storage) struct UnlinkedPrefix(PrefixRetirement);

impl PrefixRetirement {
    pub(in crate::storage) const DATA_BYTES: u64 = 128 * 1024;
    pub(in crate::storage) const NATIVE_BYTES: u64 = 16 * 1024;

    pub(super) fn capture(
        read: NativePrefixRead<'_>,
        base: Base,
        metadata: OwnedPermit,
        cancelled: &AtomicBool,
    ) -> Result<Self, AdmissionError> {
        if metadata.request().units().items() == 0
            || metadata.request().units().data_bytes() < Self::DATA_BYTES
            || metadata.request().units().native_bytes() < Self::NATIVE_BYTES
        {
            return Err(AdmissionError::InsufficientReservation);
        }
        read.validate(cancelled)?;
        base.require_successor(read.prior)
            .map_err(|_| AdmissionError::StateMismatch)?;
        if base.context != read.context()
            || read.state.issued != read.state.acknowledged
            || read.state.retired >= read.state.issued
            || base.retired != read.state.issued
            || base.source != read.source
            || base.batches != read.batches
            || base.science != read.science
            || base.requests != read.request_chain()?
        {
            return Err(AdmissionError::StateMismatch);
        }
        let reference = base
            .reference()
            .map_err(|_| AdmissionError::StateMismatch)?;
        Ok(Self {
            base,
            reference,
            old_retired: read.state.retired,
            pending_bytes: read.pending_bytes(),
            closing: read.state.closing,
            root: read.root,
            sequence: read.sequence,
            custody: std::sync::Arc::clone(&read.store._reservation.custody),
            inventory: read.inventory,
            _metadata: metadata,
        })
    }
    pub(in crate::storage) fn base(&self) -> &Base {
        &self.base
    }
    pub(in crate::storage) fn reference(&self) -> BaseRef {
        self.reference
    }
    pub(in crate::storage) fn old_retired(&self) -> u64 {
        self.old_retired
    }
    pub(in crate::storage) fn pending_bytes(&self) -> u64 {
        self.pending_bytes
    }

    fn floor(&self) -> producer_prefix::NativeFloor {
        producer_prefix::NativeFloor {
            issued: self.base.retired,
            acknowledged: self.base.retired,
            retired: self.base.retired,
            closing: self.closing,
            base: self.reference,
        }
    }
    pub(in crate::storage) fn validate(
        &self,
        store: &IngestStore,
        cancelled: &AtomicBool,
    ) -> Result<(), AdmissionError> {
        check(cancelled)?;
        if store.write_uncertain {
            return Err(AdmissionError::CommitUnknown);
        }
        if store.prefix_context(self.slot()?) != self.base.context
            || store.handoff_root_identity()? != self.root
            || store.db.latest_sequence_number() != self.sequence
            || !std::sync::Arc::ptr_eq(&store._reservation.custody, &self.custody)
        {
            return Err(AdmissionError::StateMismatch);
        }
        store.require_closed_input_inventory(&self.inventory, cancelled)?;
        if store.handoff_root_identity()? != self.root
            || store.db.latest_sequence_number() != self.sequence
        {
            return Err(AdmissionError::StateMismatch);
        }
        check(cancelled)
    }
    fn slot(&self) -> Result<ProducerSlotId, AdmissionError> {
        self.base
            .context
            .slot
            .try_into()
            .map_err(|_| AdmissionError::StateMismatch)
    }
    fn apply(
        mut self,
        store: &mut IngestStore,
        cancelled: &AtomicBool,
        mut authority: impl FnMut() -> Result<(), AdmissionError>,
    ) -> Result<UnlinkedPrefix, AdmissionError> {
        authority()?;
        self.validate(store, cancelled)?;
        let slot = self.slot()?;
        let epoch = ProducerEpoch::new(1).expect("initial prefix");
        let state = store
            .producer_state(slot, epoch)?
            .ok_or(AdmissionError::StateMismatch)?;
        if state.issued != self.base.retired
            || state.acknowledged != self.base.retired
            || state.retired != self.old_retired
            || state.closing != self.closing
        {
            return Err(AdmissionError::StateMismatch);
        }
        let count = self
            .base
            .retired
            .checked_sub(self.old_retired)
            .filter(|n| (1..=64).contains(n))
            .ok_or(AdmissionError::Limit)?;
        let expected = self
            .sequence
            .checked_add(count)
            .and_then(|n| n.checked_add(1))
            .ok_or(AdmissionError::Limit)?;
        let cf = store
            .db
            .cf_handle("receipts")
            .ok_or(AdmissionError::EngineRead)?;
        let mut batch = rocksdb::WriteBatch::with_capacity_bytes(Self::NATIVE_BYTES as usize);
        let marker = IngestKey::ProducerState {
            producer_slot_id: slot,
            producer_epoch: epoch,
        };
        let encoded = self
            .floor()
            .encode()
            .map_err(|_| AdmissionError::StateMismatch)?;
        batch.put_cf(
            cf,
            marker
                .encode()
                .map_err(|_| AdmissionError::InvalidInput)?
                .as_bytes(),
            encoded,
        );
        for offset in 1..=count {
            check(cancelled)?;
            let request_id = ProducerRequestId {
                producer_slot_id: slot,
                producer_epoch: epoch,
                request_sequence: RequestSequence::new(
                    self.old_retired
                        .checked_add(offset)
                        .ok_or(AdmissionError::Limit)?,
                )
                .map_err(|_| AdmissionError::Limit)?,
            };
            let key = IngestKey::Request { request_id }
                .encode()
                .map_err(|_| AdmissionError::InvalidInput)?;
            if store
                .db
                .get_pinned_cf_opt(cf, key.as_bytes(), &super::super::bootstrap::read_options())
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
        self.validate(store, cancelled)?;
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
        self.validate(store, cancelled)?;
        store.handoff_value(marker, Some(&encoded))?;
        authority()?;
        self.unlink(store, cancelled, authority)
    }
    fn unlink(
        mut self,
        store: &IngestStore,
        cancelled: &AtomicBool,
        mut authority: impl FnMut() -> Result<(), AdmissionError>,
    ) -> Result<UnlinkedPrefix, AdmissionError> {
        let slot = self.slot()?;
        let epoch = ProducerEpoch::new(1).expect("initial prefix");
        self.validate(store, cancelled)?;
        let count = self.inventory.range_count(slot, epoch, self.base.retired);
        let removed = self.inventory.remove_range(
            store,
            slot,
            epoch,
            self.base.retired,
            cancelled,
            || authority().map_err(|_| BootstrapError::AuthorityUnavailable),
        )?;
        if removed != count || self.inventory.range_count(slot, epoch, self.base.retired) != 0 {
            return Err(AdmissionError::StateMismatch);
        }
        self.validate(store, cancelled)?;
        authority()?;
        Ok(UnlinkedPrefix(self))
    }
}
impl UnlinkedPrefix {
    pub(in crate::storage) fn base(&self) -> &Base {
        &self.0.base
    }
    pub(in crate::storage) fn reference(&self) -> BaseRef {
        self.0.reference
    }
    pub(in crate::storage) fn old_retired(&self) -> u64 {
        self.0.old_retired
    }
    pub(in crate::storage) fn validate(
        &self,
        store: &IngestStore,
        cancelled: &AtomicBool,
    ) -> Result<(), AdmissionError> {
        self.0.validate(store, cancelled)?;
        let slot = self.0.slot()?;
        let floor = store
            .open_prefix_floor(slot, ProducerEpoch::new(1).expect("initial prefix"))?
            .ok_or(AdmissionError::StateMismatch)?;
        if floor != self.0.floor()
            || self.0.inventory.range_count(
                slot,
                ProducerEpoch::new(1).expect("initial prefix"),
                self.0.base.retired,
            ) != 0
        {
            return Err(AdmissionError::StateMismatch);
        }
        Ok(())
    }
}
impl IngestStore {
    pub(in crate::storage) fn retire_open_prefix(
        &mut self,
        confirmed: ConfirmedPrefixPrepare,
        cancelled: &AtomicBool,
        authority: impl FnMut() -> Result<(), AdmissionError>,
    ) -> Result<UnlinkedPrefix, AdmissionError> {
        confirmed.into_plan().apply(self, cancelled, authority)
    }
}
