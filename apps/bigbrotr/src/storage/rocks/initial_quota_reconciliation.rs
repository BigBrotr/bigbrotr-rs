//! Positive OPEN-initial quota reconciliation. This never closes a producer,
//! writes native data, supplies an absence proof, or adopts a Control identity.
use super::{
    AdmissionError, BootstrapError, IngestStore, NativeOutcomeCopy, PendingAccounting,
    PendingQuotaPolicy, ProducerRecoveryError, ProducerRecoveryLimits,
    bootstrap::read_options,
    producer_closing::ProducerState,
    producer_recovery::{RecoveryContext, ReplayContext},
};
use crate::{
    runtime::OwnedPermit,
    storage::rocks::{
        keys::{ColumnFamily, IngestKey, KeyDomain, decode_key},
        manifest::IngestManifest,
    },
};
use bigbrotr_model::{Digest256, ProducerRequestId, ProducerSlotId, SourceRecordId};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

pub const INITIAL_QUOTA_RECONCILIATION_DATA_BYTES: u64 = 64 * 1024;

struct Stamp {
    root: (u64, u64),
    sequence: u64,
    custody: Arc<()>,
    manifest: IngestManifest,
    accounting: PendingAccounting,
}
impl Stamp {
    fn require(
        &self,
        store: &IngestStore,
        cancelled: &AtomicBool,
    ) -> Result<(), ProducerRecoveryError> {
        check(store, cancelled)?;
        store
            .require_pending_accounting()
            .map_err(BootstrapError::from)
            .map_err(native)?;
        if !Arc::ptr_eq(&self.custody, &store._reservation.custody)
            || store.handoff_root_identity().map_err(native)? != self.root
            || store.db.latest_sequence_number() != self.sequence
            || store.manifest != self.manifest
            || store.accounting != self.accounting
        {
            return Err(AdmissionError::StateMismatch.into());
        }
        check(store, cancelled)
    }
}

/// One complete finite native request/input head, retained across the parent's
/// current-authority SQL reads. Portable bytes or a receipt cannot create it.
pub(in crate::storage) struct InitialQuotaReconciliation {
    stamp: Stamp,
    slot: ProducerSlotId,
    state: ProducerState,
    inputs: [u8; 32],
    recorded: u64,
    recorded_source: u64,
    _scratch: OwnedPermit,
}
const _: () = assert!(
    std::mem::size_of::<InitialQuotaReconciliation>() + 16 * 1024
        < INITIAL_QUOTA_RECONCILIATION_DATA_BYTES as usize
);
impl InitialQuotaReconciliation {
    pub(in crate::storage) fn issued_through(&self) -> u64 {
        self.state.issued
    }

    /// Called only after the connected parent compares this exact original to
    /// both retained PG rows. Return the same two grants for the next ordinal.
    pub(in crate::storage) fn record(
        &mut self,
        original: InitialQuotaOriginal,
    ) -> Result<(OwnedPermit, OwnedPermit), ProducerRecoveryError> {
        let request = original.copy.request();
        if !Arc::ptr_eq(&original.custody, &self.stamp.custody)
            || original.root != self.stamp.root
            || original.sequence != self.stamp.sequence
            || request.producer_slot_id != self.slot
            || request.producer_epoch.get() != 1
            || self.recorded.checked_add(1) != Some(request.request_sequence.get())
            || request.request_sequence.get() > self.state.issued
        {
            return Err(AdmissionError::StateMismatch.into());
        }
        if let Some((first, last)) = original.new_batch {
            let identity = self.stamp.manifest.identity();
            if first.dataset_id != identity.dataset_id
                || first.shard_id != identity.shard_id
                || first.owner_epoch != identity.owner_epoch
                || last.dataset_id != first.dataset_id
                || last.shard_id != first.shard_id
                || last.owner_epoch != first.owner_epoch
                || self.recorded_source.checked_add(1) != Some(first.sequence.get())
                || last.sequence < first.sequence
                || last.sequence.get() > self.stamp.accounting.source_through()
            {
                return Err(AdmissionError::StateMismatch.into());
            }
            self.recorded_source = last.sequence.get();
        }
        self.recorded = request.request_sequence.get();
        Ok((original.input_permit, original.outcome_permit))
    }
}

/// The same semantic replay validators used by closing recovery produce this
/// private native copy. Both operation grants remain owned through the PG await.
pub(in crate::storage) struct InitialQuotaOriginal {
    copy: NativeOutcomeCopy,
    input_bytes: u32,
    new_batch: Option<(SourceRecordId, SourceRecordId)>,
    root: (u64, u64),
    sequence: u64,
    custody: Arc<()>,
    input_permit: OwnedPermit,
    outcome_permit: OwnedPermit,
}
impl InitialQuotaOriginal {
    pub(in crate::storage) fn native_outcome(&self) -> &NativeOutcomeCopy {
        &self.copy
    }
    pub(in crate::storage) fn input_bytes(&self) -> u32 {
        self.input_bytes
    }
}

fn native(error: BootstrapError) -> ProducerRecoveryError {
    AdmissionError::from(error).into()
}
fn check(store: &IngestStore, cancelled: &AtomicBool) -> Result<(), ProducerRecoveryError> {
    if cancelled.load(Ordering::Acquire) {
        return Err(AdmissionError::AuthorityUnavailable.into());
    }
    if store.native_unavailable() {
        return Err(AdmissionError::CommitUnknown.into());
    }
    Ok(())
}
impl IngestStore {
    pub(in crate::storage) fn begin_initial_quota_reconciliation(
        &self,
        slot: ProducerSlotId,
        scratch: OwnedPermit,
        cancelled: &AtomicBool,
    ) -> Result<InitialQuotaReconciliation, ProducerRecoveryError> {
        if scratch.request().units().items() == 0
            || scratch.request().units().data_bytes() < INITIAL_QUOTA_RECONCILIATION_DATA_BYTES
        {
            return Err(AdmissionError::InsufficientReservation.into());
        }
        check(self, cancelled)?;
        let state = self.initial_quota_open_state(slot, cancelled)?;
        let accounting = super::pending_accounting::census(&self.db, &self.manifest)
            .map_err(BootstrapError::from)
            .map_err(native)?;
        if accounting != self.accounting {
            return Err(AdmissionError::StateMismatch.into());
        }
        let root = self.handoff_root_identity().map_err(native)?;
        let sequence = self.db.latest_sequence_number();
        let inputs = self.initial_quota_inputs(slot, state.issued, cancelled)?;
        let head = InitialQuotaReconciliation {
            stamp: Stamp {
                root,
                sequence,
                custody: Arc::clone(&self._reservation.custody),
                manifest: self.manifest,
                accounting,
            },
            slot,
            state,
            inputs,
            recorded: 0,
            recorded_source: 0,
            _scratch: scratch,
        };
        head.stamp.require(self, cancelled)?;
        Ok(head)
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "one original and two retained reusable operation grants"
    )]
    pub(in crate::storage) fn read_initial_quota_original(
        &self,
        head: &InitialQuotaReconciliation,
        request: ProducerRequestId,
        digest: Digest256,
        limits: ProducerRecoveryLimits,
        input_permit: OwnedPermit,
        outcome_permit: OwnedPermit,
        cancelled: &AtomicBool,
    ) -> Result<InitialQuotaOriginal, ProducerRecoveryError> {
        if outcome_permit.request().units().items() == 0
            || outcome_permit.request().units().data_bytes() < limits.required_data_bytes()?
        {
            return Err(AdmissionError::InsufficientReservation.into());
        }
        head.stamp.require(self, cancelled)?;
        if request.producer_slot_id != head.slot
            || request.producer_epoch.get() != 1
            || head.recorded.checked_add(1) != Some(request.request_sequence.get())
            || request.request_sequence.get() > head.state.issued
        {
            return Err(AdmissionError::InvalidInput.into());
        }
        let retained = self
            .read_request_input(request, digest, limits.input, input_permit)
            .map_err(ProducerRecoveryError::Input)?;
        let input_bytes = retained.encoded_bytes();
        let (_, intent, input_permit) = retained.into_parts();
        check(self, cancelled)?;
        let context = ReplayContext {
            manifest: &head.stamp.manifest,
            current_manifest: &head.stamp.manifest,
        };
        let (outcome, copy) = self.replay_original_intent(
            request,
            digest,
            limits,
            intent,
            context,
            &RecoveryContext::Initial(context),
            &outcome_permit,
        )?;
        head.stamp.require(self, cancelled)?;
        let new_batch = match &outcome {
            super::AdmissionOutcome::Durable(receipt) => {
                receipt.batch().map(|batch| (batch.first(), batch.last()))
            }
            super::AdmissionOutcome::Rejected(_) => None,
        };
        Ok(InitialQuotaOriginal {
            copy,
            input_bytes,
            new_batch,
            root: head.stamp.root,
            sequence: head.stamp.sequence,
            custody: Arc::clone(&self._reservation.custody),
            input_permit,
            outcome_permit,
        })
    }

    /// The parent calls this only after exact PG sets/outcomes and confirmed
    /// read-transaction rollback/current authority. All native checks are fresh;
    /// success changes only the logical RAM binding and its latched interval.
    pub(in crate::storage) fn finish_initial_quota_reconciliation(
        &mut self,
        head: InitialQuotaReconciliation,
        policy: PendingQuotaPolicy,
        cancelled: &AtomicBool,
    ) -> Result<(), ProducerRecoveryError> {
        head.stamp.require(self, cancelled)?;
        if head.recorded != head.state.issued
            || head.recorded_source != head.stamp.accounting.source_through()
            || self.initial_quota_open_state(head.slot, cancelled)? != head.state
            || self.initial_quota_inputs(head.slot, head.state.issued, cancelled)? != head.inputs
        {
            return Err(AdmissionError::StateMismatch.into());
        }
        head.stamp.require(self, cancelled)?;
        self.bind_reconciled_pending_quota(policy)
            .map_err(BootstrapError::PendingQuota)
            .map_err(native)?;
        check(self, cancelled)
    }

    fn initial_quota_inputs(
        &self,
        slot: ProducerSlotId,
        issued: u64,
        cancelled: &AtomicBool,
    ) -> Result<[u8; 32], ProducerRecoveryError> {
        self.lineage_input_census(
            issued,
            |request| {
                request.producer_slot_id == slot
                    && request.producer_epoch.get() == 1
                    && request.request_sequence.get() <= issued
            },
            cancelled,
        )
        .map_err(native)
    }

    /// Entire receipts-family census: exactly one OPEN/retired0 epoch1 producer
    /// and every original native outcome1..issued. No filtered foreign tail can
    /// hide behind the selected producer. Zero is only the exact fresh layout.
    fn initial_quota_open_state(
        &self,
        slot: ProducerSlotId,
        cancelled: &AtomicBool,
    ) -> Result<ProducerState, ProducerRecoveryError> {
        check(self, cancelled)?;
        if self.manifest.key_schema() != 1 || self.manifest.identity().owner_epoch.get() != 1 {
            return Err(AdmissionError::InvalidInput.into());
        }
        self.require_pending_accounting()
            .map_err(BootstrapError::from)
            .map_err(native)?;
        let cf = self
            .db
            .cf_handle("receipts")
            .ok_or(AdmissionError::EngineRead)?;
        let mut rows = self.db.raw_iterator_cf_opt(cf, read_options());
        rows.seek_to_first();
        let mut count = 0u64;
        let mut state = None;
        while rows.valid() {
            check(self, cancelled)?;
            let key = rows.key().ok_or(AdmissionError::StateMismatch)?;
            let value = rows.value().ok_or(AdmissionError::StateMismatch)?;
            match decode_key(ColumnFamily::Receipts, key)
                .map_err(|_| AdmissionError::StateMismatch)?
            {
                IngestKey::Request { request_id }
                    if request_id.producer_slot_id == slot
                        && request_id.producer_epoch.get() == 1
                        && count.checked_add(1) == Some(request_id.request_sequence.get())
                        && count < super::RETAINED_PRODUCER_REQUEST_LIMIT
                        && !value.is_empty()
                        && value.len() <= super::MAX_OUTCOME_BYTES =>
                {
                    count += 1;
                }
                IngestKey::ProducerState {
                    producer_slot_id,
                    producer_epoch,
                } if producer_slot_id == slot
                    && producer_epoch.get() == 1
                    && state.is_none()
                    && value.len() == 26
                    && value[..2] == [1, 0x31] =>
                {
                    let found = ProducerState::decode(value).map_err(native)?;
                    if found.closing
                        || found.retired != 0
                        || found.issued == 0
                        || found.issued > super::RETAINED_PRODUCER_REQUEST_LIMIT
                    {
                        return Err(AdmissionError::StateMismatch.into());
                    }
                    state = Some(found);
                }
                _ => return Err(AdmissionError::StateMismatch.into()),
            }
            rows.next();
        }
        rows.status().map_err(|_| AdmissionError::EngineRead)?;
        match state {
            Some(state) if state.issued == count => Ok(state),
            None if count == 0 => {
                self.require_initial_quota_empty()?;
                Ok(ProducerState::default())
            }
            _ => Err(AdmissionError::StateMismatch.into()),
        }
    }
    fn require_initial_quota_empty(&self) -> Result<(), ProducerRecoveryError> {
        if self.accounting.pending_bytes() != 0 || self.accounting.source_through() != 0 {
            return Err(AdmissionError::StateMismatch.into());
        }
        let identity = self.manifest.identity();
        let source = IngestKey::SourceStream {
            dataset_id: identity.dataset_id,
            shard_id: identity.shard_id,
            owner_epoch: identity.owner_epoch,
        }
        .encode()
        .map_err(|_| AdmissionError::StateMismatch)?;
        for (name, wanted) in [("default", 3usize), ("facts", 0), ("receipts", 0)] {
            let cf = self.db.cf_handle(name).ok_or(AdmissionError::EngineRead)?;
            let mut rows = self.db.raw_iterator_cf_opt(cf, read_options());
            rows.seek_to_first();
            let mut count = 0;
            while rows.valid() {
                count += 1;
                if count > wanted
                    || (name == "default"
                        && rows.key().is_none_or(|key| {
                            key != KeyDomain::Manifest.prefix().as_slice()
                                && key != KeyDomain::PendingAccounting.prefix().as_slice()
                                && key != source.as_bytes()
                        }))
                {
                    return Err(AdmissionError::StateMismatch.into());
                }
                rows.next();
            }
            rows.status().map_err(|_| AdmissionError::EngineRead)?;
            if count != wanted {
                return Err(AdmissionError::StateMismatch.into());
            }
        }
        Ok(())
    }
}
