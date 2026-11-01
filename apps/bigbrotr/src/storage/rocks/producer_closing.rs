//! Explicit per-producer barrier migration; never a scientific outcome or ACK.
use super::bootstrap::{Prepared, read_options};
use super::{BootstrapError, BootstrapLimits, IngestRecovery, IngestStore};
use crate::runtime::OwnedPermit;
use crate::storage::rocks::{keys::IngestKey, manifest::IngestManifest};
use bigbrotr_model::{ProducerEpoch, ProducerRequestId, ProducerSlotId, RequestSequence};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct ProducerState {
    pub issued: u64,
    pub acknowledged: u64,
    pub retired: u64,
    pub closing: bool,
}
impl ProducerState {
    pub(super) fn decode(bytes: &[u8]) -> Result<Self, BootstrapError> {
        if bytes.starts_with(&[4, 0x31]) {
            let floor = crate::storage::producer_prefix::NativeFloor::decode(bytes)
                .map_err(|_| BootstrapError::StateMismatch)?;
            return Ok(Self {
                issued: floor.issued,
                acknowledged: floor.acknowledged,
                retired: floor.retired,
                closing: floor.closing,
            });
        }
        let closing = match bytes {
            [1, 0x31, ..] if bytes.len() == 26 => false,
            [2, 0x31, ..] if bytes.len() == 27 && bytes[26] == 1 => true,
            _ => return Err(BootstrapError::StateMismatch),
        };
        let number = |at| {
            u64::from_be_bytes(
                bytes[at..at + 8]
                    .try_into()
                    .expect("fixed validated length"),
            )
        };
        let value = Self {
            issued: number(2),
            acknowledged: number(10),
            retired: number(18),
            closing,
        };
        if value.retired > value.acknowledged || value.acknowledged > value.issued {
            return Err(BootstrapError::StateMismatch);
        }
        Ok(value)
    }
    fn closing_bytes(self) -> [u8; 27] {
        let mut bytes = [0; 27];
        bytes[..2].copy_from_slice(&[2, 0x31]);
        bytes[2..10].copy_from_slice(&self.issued.to_be_bytes());
        bytes[10..18].copy_from_slice(&self.acknowledged.to_be_bytes());
        bytes[18..26].copy_from_slice(&self.retired.to_be_bytes());
        bytes[26] = 1;
        bytes
    }
}

pub struct ProducerClosing {
    slot: ProducerSlotId,
    epoch: ProducerEpoch,
    manifest: IngestManifest,
    state: ProducerState,
    _permit: OwnedPermit,
}
impl ProducerClosing {
    pub fn issued_through(&self) -> u64 {
        self.state.issued
    }
    pub fn acknowledged_through(&self) -> u64 {
        self.state.acknowledged
    }
    pub fn producer_slot_id(&self) -> ProducerSlotId {
        self.slot
    }
    pub fn producer_epoch(&self) -> ProducerEpoch {
        self.epoch
    }
    pub fn manifest(&self) -> &IngestManifest {
        &self.manifest
    }
}
impl IngestRecovery {
    /// Read an existing closing barrier for completed-retirement observation.
    /// Unlike closing, this cannot create metadata for an absent/open producer.
    pub(in crate::storage) fn observe_closed_producer(
        &self,
        slot: ProducerSlotId,
        epoch: ProducerEpoch,
        permit: OwnedPermit,
    ) -> Result<ProducerClosing, BootstrapError> {
        self.observe_closed_producer_in(slot, epoch, permit, self.manifest())
    }

    pub(super) fn observe_closed_producer_in(
        &self,
        slot: ProducerSlotId,
        epoch: ProducerEpoch,
        permit: OwnedPermit,
        manifest: &IngestManifest,
    ) -> Result<ProducerClosing, BootstrapError> {
        if permit.request().units().data_bytes() < 4096
            || permit.request().units().native_bytes() < 4096
            || permit.request().units().items() == 0
        {
            return Err(BootstrapError::InsufficientReservation);
        }
        if self.store.native_unavailable() {
            return Err(BootstrapError::CommitUnknown);
        }
        if manifest.key_schema() != 1
            || epoch.get() != 1
            || manifest.identity().owner_epoch.get() != 1
        {
            return Err(BootstrapError::InvalidIntent);
        }
        let state = self
            .store
            .producer_state(slot, epoch)?
            .filter(|state| state.closing)
            .ok_or(BootstrapError::StateMismatch)?;
        self.store.require_producer_boundary(slot, epoch, state)?;
        Ok(ProducerClosing {
            slot,
            epoch,
            manifest: *manifest,
            state,
            _permit: permit,
        })
    }

    pub(in crate::storage) fn close_producer(
        &mut self,
        slot: ProducerSlotId,
        epoch: ProducerEpoch,
        permit: OwnedPermit,
        mut authority: impl FnMut() -> Result<(), BootstrapError>,
    ) -> Result<ProducerClosing, BootstrapError> {
        // One fixed value, bounded native batch and retained result. This is an
        // allocation floor, not a process RSS or native-I/O latency guarantee.
        if permit.request().units().data_bytes() < 4096
            || permit.request().units().native_bytes() < 4096
            || permit.request().units().items() == 0
        {
            return Err(BootstrapError::InsufficientReservation);
        }
        authority()?;
        if self.store.native_unavailable() {
            return Err(BootstrapError::CommitUnknown);
        }
        if self.store.manifest.key_schema() != 1
            || epoch.get() != 1
            || self.store.manifest.identity().owner_epoch.get() != 1
        {
            return Err(BootstrapError::InvalidIntent);
        }
        let state = self.store.producer_state(slot, epoch)?.unwrap_or_default();
        self.store.require_producer_boundary(slot, epoch, state)?;
        let state = self
            .store
            .close_producer_marker(slot, epoch, state, &mut authority)?;
        authority()?;
        Ok(ProducerClosing {
            slot,
            epoch,
            manifest: self.store.manifest,
            state,
            _permit: permit,
        })
    }
}

impl IngestStore {
    /// Observe v4 data only in its selected initial context. The connected PG
    /// adapter must independently authenticate the corresponding base.
    pub(in crate::storage) fn open_prefix_floor(
        &self,
        slot: ProducerSlotId,
        epoch: ProducerEpoch,
    ) -> Result<Option<crate::storage::producer_prefix::NativeFloor>, BootstrapError> {
        let key = IngestKey::ProducerState {
            producer_slot_id: slot,
            producer_epoch: epoch,
        }
        .encode()
        .map_err(|_| BootstrapError::InvalidIntent)?;
        let cf = self
            .db
            .cf_handle("receipts")
            .ok_or(BootstrapError::EngineRead)?;
        let bytes = self
            .db
            .get_pinned_cf_opt(cf, key.as_bytes(), &read_options())
            .map_err(|_| BootstrapError::EngineRead)?;
        match bytes {
            Some(bytes) if bytes.starts_with(&[4, 0x31]) => {
                if epoch.get() != 1
                    || self.manifest.key_schema() != 1
                    || self.manifest.identity().owner_epoch.get() != 1
                {
                    return Err(BootstrapError::StateMismatch);
                }
                crate::storage::producer_prefix::NativeFloor::decode(&bytes)
                    .map(Some)
                    .map_err(|_| BootstrapError::StateMismatch)
            }
            _ => Ok(None),
        }
    }

    /// Every ordinary state write preserves an installed prefix reference.
    /// This cannot install or replace one; only the confirmed prefix kernel can.
    pub(super) fn producer_state_bytes(
        &self,
        slot: ProducerSlotId,
        epoch: ProducerEpoch,
        state: ProducerState,
    ) -> Result<Vec<u8>, BootstrapError> {
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(67)
            .map_err(|_| BootstrapError::Allocation)?;
        if let Some(mut floor) = self.open_prefix_floor(slot, epoch)? {
            if state.retired != floor.retired
                || state.issued < floor.issued
                || state.acknowledged < floor.acknowledged
                || (floor.closing && !state.closing)
            {
                return Err(BootstrapError::StateMismatch);
            }
            floor.issued = state.issued;
            floor.acknowledged = state.acknowledged;
            floor.closing = state.closing;
            bytes.extend_from_slice(&floor.encode().map_err(|_| BootstrapError::StateMismatch)?);
        } else {
            if state.retired != 0 {
                return Err(BootstrapError::StateMismatch);
            }
            if state.closing {
                bytes.extend_from_slice(&state.closing_bytes());
            } else {
                bytes.extend_from_slice(&[1, 0x31]);
                bytes.extend_from_slice(&state.issued.to_be_bytes());
                bytes.extend_from_slice(&state.acknowledged.to_be_bytes());
                bytes.extend_from_slice(&state.retired.to_be_bytes());
            }
        }
        Ok(bytes)
    }

    pub(super) fn require_retained_state(
        &self,
        slot: ProducerSlotId,
        epoch: ProducerEpoch,
        state: ProducerState,
    ) -> Result<(), BootstrapError> {
        if state
            .issued
            .checked_sub(state.retired)
            .is_none_or(|tail| tail > super::RETAINED_PRODUCER_REQUEST_LIMIT)
        {
            return Err(BootstrapError::StateMismatch);
        }
        match self.open_prefix_floor(slot, epoch)? {
            Some(floor)
                if floor.issued == state.issued
                    && floor.acknowledged == state.acknowledged
                    && floor.retired == state.retired
                    && floor.closing == state.closing =>
            {
                Ok(())
            }
            None if state.retired == 0 => Ok(()),
            _ => Err(BootstrapError::StateMismatch),
        }
    }

    /// The caller checks its selected lifetime, existing state and retained
    /// grant. This shared kernel only installs the same one-way native marker.
    pub(super) fn close_producer_marker(
        &mut self,
        slot: ProducerSlotId,
        epoch: ProducerEpoch,
        mut state: ProducerState,
        mut authority: impl FnMut() -> Result<(), BootstrapError>,
    ) -> Result<ProducerState, BootstrapError> {
        if !state.closing {
            let mut batch = Prepared::new(BootstrapLimits {
                max_mappings: 1,
                max_batch_bytes: 1024,
            })?;
            let mut closing = state;
            closing.closing = true;
            let bytes = self.producer_state_bytes(slot, epoch, closing)?;
            batch.put(
                IngestKey::ProducerState {
                    producer_slot_id: slot,
                    producer_epoch: epoch,
                },
                bytes,
            )?;
            // The same exclusive mutable owner serializes this with admission.
            // A prior prepared input has no right to cross this durable barrier.
            self.commit_prepared(batch, &mut authority)?;
            state.closing = true;
        }
        Ok(state)
    }
    pub(super) fn producer_state(
        &self,
        slot: ProducerSlotId,
        epoch: ProducerEpoch,
    ) -> Result<Option<ProducerState>, BootstrapError> {
        let key = IngestKey::ProducerState {
            producer_slot_id: slot,
            producer_epoch: epoch,
        }
        .encode()
        .map_err(|_| BootstrapError::InvalidIntent)?;
        let cf = self
            .db
            .cf_handle("receipts")
            .ok_or(BootstrapError::EngineRead)?;
        self.db
            .get_pinned_cf_opt(cf, key.as_bytes(), &read_options())
            .map_err(|_| BootstrapError::EngineRead)?
            .map(|bytes| ProducerState::decode(&bytes))
            .transpose()
    }
    pub(super) fn require_open_producer(
        &self,
        request: ProducerRequestId,
    ) -> Result<(), BootstrapError> {
        if self
            .producer_state(request.producer_slot_id, request.producer_epoch)?
            .is_some_and(|state| state.closing)
        {
            return Err(BootstrapError::ProducerClosing);
        }
        Ok(())
    }
    pub(super) fn require_producer_boundary(
        &self,
        slot: ProducerSlotId,
        epoch: ProducerEpoch,
        state: ProducerState,
    ) -> Result<(), BootstrapError> {
        // Structural no-GC bound only. Connected recovery separately validates
        // ACK <= typed checkpoint and then replays every retained original.
        if state.acknowledged > state.issued {
            return Err(BootstrapError::StateMismatch);
        }
        self.require_retained_state(slot, epoch, state)?;
        let key = |n| {
            IngestKey::Request {
                request_id: ProducerRequestId {
                    producer_slot_id: slot,
                    producer_epoch: epoch,
                    request_sequence: RequestSequence::new(n)
                        .expect("nonzero fixed or checked issued"),
                },
            }
            .encode()
            .map_err(|_| BootstrapError::InvalidIntent)
        };
        let maximum = key(u64::MAX)?;
        let prefix = &maximum.as_bytes()[..26];
        let cf = self
            .db
            .cf_handle("receipts")
            .ok_or(BootstrapError::EngineRead)?;
        let mut iterator = self.db.raw_iterator_cf_opt(cf, read_options());
        if state.issued == state.retired {
            iterator.seek(prefix);
        } else {
            iterator.seek_for_prev(maximum.as_bytes());
        }
        iterator.status().map_err(|_| BootstrapError::EngineRead)?;
        let last = iterator.key().filter(|key| key.starts_with(prefix));
        if state.issued == state.retired {
            if last.is_some() {
                return Err(BootstrapError::StateMismatch);
            }
        } else if last != Some(key(state.issued)?.as_bytes()) {
            return Err(BootstrapError::StateMismatch);
        }
        // This bounded boundary check is not complete receipt/dependency recovery.
        // Closing cannot acknowledge, retire or complete a missing interior slot.
        Ok(())
    }
}
