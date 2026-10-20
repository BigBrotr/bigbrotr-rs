//! Integrity readback of an already-authoritative retained closure.
//! No decoded Fact, writable owner, pin or admission authority escapes.
use super::bootstrap::read_options;
use super::{AdmissionError, BootstrapError, IngestRecovery};
use crate::{runtime::OwnedPermit, storage::rocks::keys::IngestKey};
use bigbrotr_model::{Fact, FactRef, ProducerEpoch, ProducerSlotId};
use sha2::{Digest, Sha256};

const MAX_FACT_BYTES: usize = 2 * 1024 * 1024;
const HASH_CHUNK_BYTES: usize = 64 * 1024;

/// Borrows the actual excluded recovery owner and the caller's existing scratch
/// grant for the entire SQL stream. Native buffers stay charged to the store's
/// separate retained reservation. This is not a hard native allocation limit.
pub(in crate::storage) struct ProgressRead<'a> {
    recovery: &'a IngestRecovery,
    root: (u64, u64),
    sequence: u64,
    _scratch: &'a OwnedPermit,
}
impl IngestRecovery {
    pub(in crate::storage) fn progress_read<'a>(
        &'a self,
        scratch: &'a OwnedPermit,
    ) -> Result<ProgressRead<'a>, AdmissionError> {
        // Fixed reader/hash state and one encoded key, not a decoded Fact or
        // dependency vector. The encompassing reducer accounts its own overlap.
        if scratch.request().units().data_bytes() < 4096 || scratch.request().units().items() == 0 {
            return Err(AdmissionError::InsufficientReservation);
        }
        let view = ProgressRead {
            recovery: self,
            root: self.store.handoff_root_identity()?,
            sequence: self.store.db.latest_sequence_number(),
            _scratch: scratch,
        };
        view.validate()?;
        Ok(view)
    }
}

impl ProgressRead<'_> {
    /// A native ACK requires covering retained typed progress. No head/legacy
    /// progress has C=0 and therefore still requires A=0. No detail is reclaimed.
    pub(in crate::storage) fn require_frontier(
        &self,
        slot: ProducerSlotId,
        epoch: ProducerEpoch,
        complete: u64,
        reserved: u64,
    ) -> Result<(), AdmissionError> {
        self.validate()?;
        let store = &self.recovery.store;
        let state = store.producer_state(slot, epoch)?.unwrap_or_default();
        if state.acknowledged > complete
            || complete > state.issued
            || state.issued > reserved
            || reserved > super::RETAINED_PRODUCER_REQUEST_LIMIT
        {
            return Err(AdmissionError::StateMismatch);
        }
        store.require_producer_boundary(slot, epoch, state)?;
        self.validate()
    }

    pub(in crate::storage) fn validate(&self) -> Result<(), AdmissionError> {
        let store = &self.recovery.store;
        if store.write_uncertain {
            return Err(AdmissionError::CommitUnknown);
        }
        if store.handoff_root_identity()? != self.root
            || store.db.latest_sequence_number() != self.sequence
        {
            return Err(AdmissionError::StateMismatch);
        }
        Ok(())
    }

    /// The caller supplies its original cancellation/policy-deadline check; it
    /// must not renew that deadline between references or hash chunks. A native
    /// read itself remains non-preemptible under the existing owner watchdog.
    pub(in crate::storage) fn require_fact(
        &self,
        reference: FactRef,
        mut authority: impl FnMut() -> Result<(), AdmissionError>,
    ) -> Result<(), AdmissionError> {
        authority()?;
        self.validate()?;
        if reference.source_record_id.dataset_id != self.recovery.manifest().identity().dataset_id {
            return Err(AdmissionError::InvalidInput);
        }
        let key = IngestKey::Fact {
            source_record_id: reference.source_record_id,
        }
        .encode()
        .map_err(|_| AdmissionError::InvalidInput)?;
        {
            let store = &self.recovery.store;
            let cf = store
                .db
                .cf_handle(key.column_family().name())
                .ok_or(AdmissionError::EngineRead)?;
            let value = store
                .db
                .get_pinned_cf_opt(cf, key.as_bytes(), &read_options())
                .map_err(|_| AdmissionError::EngineRead)?
                .ok_or(AdmissionError::StateMismatch)?;
            // PinnableSlice may already have copied native bytes before this
            // check. It bounds hashing, not an allocation preceding the check.
            if value.len() > MAX_FACT_BYTES + 34 {
                return Err(AdmissionError::Limit);
            }
            if value.len() < 34
                || value[..2] != [1, 0x20]
                || value[2..34] != reference.payload_digest.as_bytes()[..]
            {
                return Err(AdmissionError::StateMismatch);
            }
            let preimage = &value[34..];
            let mut hash = Sha256::new();
            for chunk in preimage.chunks(HASH_CHUNK_BYTES) {
                authority()?;
                hash.update(chunk);
            }
            let actual: [u8; 32] = hash.finalize().into();
            if actual != *reference.payload_digest.as_bytes()
                || Fact::retained_source_record_id(preimage, MAX_FACT_BYTES)
                    .map_err(BootstrapError::from)?
                    != reference.source_record_id
            {
                return Err(AdmissionError::StateMismatch);
            }
        } // Release the sole native pin before returning to the SQL row await.
        authority()?;
        self.validate()
    }
}
