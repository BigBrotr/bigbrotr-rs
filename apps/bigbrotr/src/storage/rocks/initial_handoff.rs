//! Initial metadata succession under continuous native custody; never admission.
use super::bootstrap::{Prepared, frame, read_options};
use super::{
    AdmissionError, ProducerClosing, ProducerRecoveryError, ProducerRecoveryLimits,
    ResolvedProducerRequest, SourceResolution,
};
use super::{BootstrapError, BootstrapLimits, IngestRecovery, IngestStore, ResolvedSource};
use crate::runtime::OwnedPermit;
use crate::storage::rocks::{
    keys::IngestKey,
    manifest::{IngestManifest, MAX_MANIFEST_BYTES},
};
use bigbrotr_model::{
    DictionaryKind, Digest256, Ordinal, ProducerEpoch, ProducerRequestId, ProducerSlotId,
    QualifiedRef, Sequence, SourceRecordId,
};

use std::os::unix::fs::MetadataExt;

const MAX_HANDOFF_BYTES: usize = 78 + 2 * MAX_MANIFEST_BYTES;

/// A positive, bounded initial source proof and exact proposed target. This is
/// data, not authority. No external decoder can fabricate its native witness.
#[derive(Clone, Debug)]
pub struct InitialHandoff {
    record: InitialHandoffRecord,
    native_sequence: u64,
    native_root: (u64, u64),
}

/// Bounded portable metadata only. Its decoder cannot create a positive local
/// witness or authorize application; that still requires every original input.
#[derive(Clone, Copy, Debug)]
pub(in crate::storage) struct InitialHandoffRecord {
    previous: IngestManifest,
    next: IngestManifest,
    slot: ProducerSlotId,
    epoch: ProducerEpoch,
    issued: u64,
    source_through: u64,
    closure_digest: [u8; 32],
}
impl InitialHandoff {
    pub fn from_source(
        source: &ResolvedSource<'_>,
        next: IngestManifest,
    ) -> Result<Self, BootstrapError> {
        let previous = *source.producer().manifest();
        if source.recovery().manifest() != &previous {
            return Err(BootstrapError::InvalidIntent);
        }
        validate_transition(previous, next, source.producer().producer_epoch())?;
        Ok(Self {
            record: InitialHandoffRecord {
                previous,
                next,
                slot: source.producer().producer_slot_id(),
                epoch: source.producer().producer_epoch(),
                issued: source.resolved_requests(),
                source_through: source.last_complete_sequence(),
                closure_digest: source.digest(),
            },
            native_sequence: source.recovery().store.db.latest_sequence_number(),
            native_root: source.recovery().store.handoff_root_identity()?,
        })
    }
    pub fn previous_manifest(&self) -> &IngestManifest {
        &self.record.previous
    }
    pub fn next_manifest(&self) -> &IngestManifest {
        &self.record.next
    }
    pub fn producer_slot_id(&self) -> ProducerSlotId {
        self.record.slot
    }
    pub fn producer_epoch(&self) -> ProducerEpoch {
        self.record.epoch
    }
    pub fn issued_through(&self) -> u64 {
        self.record.issued
    }
    pub fn last_complete_sequence(&self) -> u64 {
        self.record.source_through
    }
    pub fn closure_digest(&self) -> [u8; 32] {
        self.record.closure_digest
    }

    pub fn encode(&self) -> EncodedHandoff {
        self.record.encode()
    }
}

fn validate_transition(
    previous: IngestManifest,
    next: IngestManifest,
    epoch: ProducerEpoch,
) -> Result<(), BootstrapError> {
    let old = previous.identity();
    let new = next.identity();
    if previous.key_schema() != 1
        || next.key_schema() != 2
        || old.owner_epoch.get() != 1
        || epoch.get() != 1
        || old.owner_epoch.get().checked_add(1) != Some(new.owner_epoch.get())
        || old.topology_generation.get().checked_add(1) != Some(new.topology_generation.get())
        || old.dataset_id != new.dataset_id
        || old.owner_resource_id != new.owner_resource_id
        || old.shard_id != new.shard_id
        || old.recovery_generation != new.recovery_generation
        || old.process_incarnation == new.process_incarnation
    {
        return Err(BootstrapError::InvalidIntent);
    }
    for number in 1..=9 {
        let kind = DictionaryKind::try_from(number).expect("assigned bounded kind");
        let old_domain = previous.allocation_namespaces().domain(kind);
        let next_domain = next.allocation_namespaces().domain(kind);
        if old_domain.is_some() != next_domain.is_some() {
            return Err(BootstrapError::InvalidIntent);
        }
        if let Some(domain) = next_domain {
            for other in 1..=9 {
                let other_kind = DictionaryKind::try_from(other).expect("assigned bounded kind");
                if previous.allocation_namespaces().domain(other_kind) == Some(domain)
                    || (other != number
                        && next.allocation_namespaces().domain(other_kind) == Some(domain))
                {
                    return Err(BootstrapError::InvalidIntent);
                }
            }
        }
    }
    Ok(())
}

impl InitialHandoffRecord {
    pub(in crate::storage) fn decode(bytes: &[u8]) -> Result<Self, BootstrapError> {
        if !(78 + 2 * 104..=MAX_HANDOFF_BYTES).contains(&bytes.len()) {
            return Err(BootstrapError::StateMismatch);
        }
        let mut rest = bytes;
        fn take<const N: usize>(rest: &mut &[u8]) -> Result<[u8; N], BootstrapError> {
            let bytes = rest.get(..N).ok_or(BootstrapError::StateMismatch)?;
            let fixed = bytes
                .try_into()
                .map_err(|_| BootstrapError::StateMismatch)?;
            *rest = &rest[N..];
            Ok(fixed)
        }
        if take::<2>(&mut rest)? != [1, 10] {
            return Err(BootstrapError::StateMismatch);
        }
        let slot = take::<16>(&mut rest)?
            .try_into()
            .map_err(|_| BootstrapError::StateMismatch)?;
        let epoch = ProducerEpoch::new(u64::from_be_bytes(take(&mut rest)?))
            .map_err(|_| BootstrapError::StateMismatch)?;
        let issued = u64::from_be_bytes(take(&mut rest)?);
        let source_through = u64::from_be_bytes(take(&mut rest)?);
        let closure_digest = take(&mut rest)?;
        let mut manifest = || {
            let len = usize::from(u16::from_be_bytes(take(&mut rest)?));
            let bytes = rest.get(..len).ok_or(BootstrapError::StateMismatch)?;
            let decoded =
                IngestManifest::decode(bytes).map_err(|_| BootstrapError::StateMismatch)?;
            rest = &rest[len..];
            Ok::<_, BootstrapError>(decoded)
        };
        let previous = manifest()?;
        let next = manifest()?;
        if !rest.is_empty() || issued > 64 {
            return Err(BootstrapError::StateMismatch);
        }
        validate_transition(previous, next, epoch)?;
        Ok(Self {
            previous,
            next,
            slot,
            epoch,
            issued,
            source_through,
            closure_digest,
        })
    }
    pub(in crate::storage) fn previous_manifest(&self) -> &IngestManifest {
        &self.previous
    }
    pub(in crate::storage) fn next_manifest(&self) -> &IngestManifest {
        &self.next
    }
    pub(in crate::storage) fn producer_slot_id(&self) -> ProducerSlotId {
        self.slot
    }
    pub(in crate::storage) fn issued_through(&self) -> u64 {
        self.issued
    }
    pub(in crate::storage) fn last_complete_sequence(&self) -> u64 {
        self.source_through
    }
    pub(in crate::storage) fn closure_digest(&self) -> [u8; 32] {
        self.closure_digest
    }
    fn key(&self) -> IngestKey {
        let id = self.previous.identity();
        IngestKey::OwnerHandoff {
            dataset_id: id.dataset_id,
            owner_resource_id: id.owner_resource_id,
            predecessor_epoch: id.owner_epoch,
        }
    }
    fn source_key(&self) -> IngestKey {
        let id = self.next.identity();
        IngestKey::SourceStream {
            dataset_id: id.dataset_id,
            shard_id: id.shard_id,
            owner_epoch: id.owner_epoch,
        }
    }
    fn producer_key(&self) -> IngestKey {
        IngestKey::ProducerState {
            producer_slot_id: self.slot,
            producer_epoch: ProducerEpoch::new(2).expect("initial successor"),
        }
    }

    fn encode(&self) -> EncodedHandoff {
        let mut output = EncodedHandoff {
            bytes: [0; MAX_HANDOFF_BYTES],
            len: 0,
        };
        output.push(&[1, 10]);
        output.push(self.slot.as_bytes());
        output.push(&self.epoch.get().to_be_bytes());
        output.push(&self.issued.to_be_bytes());
        output.push(&self.source_through.to_be_bytes());
        output.push(&self.closure_digest);
        for manifest in [&self.previous, &self.next] {
            let encoded = manifest.encode();
            output.push(&(encoded.as_bytes().len() as u16).to_be_bytes());
            output.push(encoded.as_bytes());
        }
        output
    }
}

/// A checked historical read view, not a reconstructed positive proof or a
/// writer. Its borrow retains the actual epoch-two native owner throughout
/// original-input/outcome validation. Portable bytes cannot supply local guards.
#[derive(Clone, Copy)]
pub(in crate::storage) struct HistoricalInitial<'a> {
    recovery: &'a IngestRecovery,
    record: InitialHandoffRecord,
    sequence: u64,
    root: (u64, u64),
}

impl IngestRecovery {
    pub(in crate::storage) fn historical_initial(
        &self,
        handoff: &[u8],
    ) -> Result<HistoricalInitial<'_>, BootstrapError> {
        let view = HistoricalInitial {
            recovery: self,
            record: InitialHandoffRecord::decode(handoff)?,
            sequence: self.store.db.latest_sequence_number(),
            root: self.store.handoff_root_identity()?,
        };
        view.validate()?;
        Ok(view)
    }
}

impl<'a> HistoricalInitial<'a> {
    pub(in crate::storage) fn manifest(&self) -> &IngestManifest {
        &self.record.previous
    }
    pub(in crate::storage) fn target_manifest(&self) -> &IngestManifest {
        &self.record.next
    }
    pub(in crate::storage) fn producer_slot_id(&self) -> ProducerSlotId {
        self.record.slot
    }
    pub(in crate::storage) fn issued_through(&self) -> u64 {
        self.record.issued
    }
    pub(in crate::storage) fn last_complete_sequence(&self) -> u64 {
        self.record.source_through
    }
    pub(in crate::storage) fn closure_digest(&self) -> [u8; 32] {
        self.record.closure_digest
    }
    pub(in crate::storage) fn recovery(&self) -> &'a IngestRecovery {
        self.recovery
    }

    pub(in crate::storage) fn observe_closed_producer(
        &self,
        permit: OwnedPermit,
    ) -> Result<ProducerClosing, BootstrapError> {
        self.validate()?;
        let closed = self.recovery.observe_closed_producer_in(
            self.record.slot,
            self.record.epoch,
            permit,
            self.manifest(),
        )?;
        if closed.issued_through() != self.record.issued {
            return Err(BootstrapError::StateMismatch);
        }
        Ok(closed)
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "separate retained input and outcome grants"
    )]
    pub(in crate::storage) fn resolve_original_request(
        &self,
        request: ProducerRequestId,
        digest: Digest256,
        limits: ProducerRecoveryLimits,
        input_permit: OwnedPermit,
        outcome_permit: OwnedPermit,
        authority: impl FnMut() -> Result<(), AdmissionError>,
    ) -> Result<ResolvedProducerRequest, ProducerRecoveryError> {
        self.validate().map_err(AdmissionError::from)?;
        if request.producer_slot_id != self.record.slot
            || request.producer_epoch != self.record.epoch
            || request.request_sequence.get() > self.record.issued
        {
            return Err(AdmissionError::InvalidInput.into());
        }
        self.recovery.resolve_original_request_in(
            request,
            digest,
            limits,
            input_permit,
            outcome_permit,
            authority,
            super::producer_recovery::ReplayContext {
                manifest: self.manifest(),
                current_manifest: self.target_manifest(),
            },
        )
    }

    pub(in crate::storage) fn source_resolution(
        &self,
        closing: ProducerClosing,
        reserved_through: u64,
    ) -> Result<SourceResolution<'a>, BootstrapError> {
        self.validate()?;
        if closing.producer_slot_id() != self.record.slot
            || closing.producer_epoch() != self.record.epoch
            || reserved_through != self.record.issued
        {
            return Err(BootstrapError::StateMismatch);
        }
        closing.source_resolution_in(self.recovery, reserved_through, Some(*self))
    }

    /// Only the complete original positive proof can rebind a portable receipt
    /// to this actual store. No operation here mutates native or SQL state.
    pub(in crate::storage) fn reconstruct_handoff(
        &self,
        source: &ResolvedSource<'_>,
    ) -> Result<InitialHandoff, BootstrapError> {
        self.validate()?;
        if !std::ptr::eq(source.recovery(), self.recovery)
            || source.producer().manifest() != self.manifest()
            || source.producer().producer_slot_id() != self.record.slot
            || source.producer().producer_epoch() != self.record.epoch
            || source.resolved_requests() != self.record.issued
            || source.last_complete_sequence() != self.last_complete_sequence()
            || source.digest() != self.closure_digest()
            || !source.matches_historical(self)
        {
            return Err(BootstrapError::StateMismatch);
        }
        let handoff = InitialHandoff {
            record: self.record,
            native_sequence: self
                .sequence
                .checked_sub(4)
                .ok_or(BootstrapError::StateMismatch)?,
            native_root: self.root,
        };
        self.recovery.validate_initial_handoff(&handoff)?;
        Ok(handoff)
    }

    pub(super) fn same_view(&self, other: &HistoricalInitial<'_>) -> bool {
        std::ptr::eq(self.recovery, other.recovery)
            && self.sequence == other.sequence
            && self.root == other.root
            && self.record.encode().as_bytes() == other.record.encode().as_bytes()
    }

    pub(super) fn validate(&self) -> Result<(), BootstrapError> {
        let store = &self.recovery.store;
        if store.write_uncertain {
            return Err(BootstrapError::CommitUnknown);
        }
        if store.manifest != self.record.next
            || store.db.latest_sequence_number() != self.sequence
            || store.handoff_root_identity()? != self.root
        {
            return Err(BootstrapError::StateMismatch);
        }
        store.require_handoff_state(&self.record, true)
    }
}

pub struct EncodedHandoff {
    bytes: [u8; MAX_HANDOFF_BYTES],
    len: usize,
}
impl EncodedHandoff {
    fn push(&mut self, bytes: &[u8]) {
        self.bytes[self.len..self.len + bytes.len()].copy_from_slice(bytes);
        self.len += bytes.len();
    }
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..self.len]
    }
}

/// An observation only. No conversion to a native writer or producer grant.
pub struct ObservedHandoff {
    handoff: InitialHandoff,
    _permit: OwnedPermit,
}
impl ObservedHandoff {
    pub fn handoff(&self) -> &InitialHandoff {
        &self.handoff
    }
}

impl IngestRecovery {
    pub(in crate::storage) fn apply_initial_handoff(
        &mut self,
        handoff: &InitialHandoff,
        permit: OwnedPermit,
        mut authority: impl FnMut() -> Result<(), BootstrapError>,
    ) -> Result<ObservedHandoff, BootstrapError> {
        if permit.request().units().data_bytes() < 8192
            || permit.request().units().native_bytes() < 8192
            || permit.request().units().items() == 0
        {
            return Err(BootstrapError::InsufficientReservation);
        }
        authority()?;
        self.validate_initial_handoff(handoff)?;
        let completed = self.manifest() == &handoff.record.next;
        let encoded = handoff.encode();
        let source_zero = [1, 2, 0, 0, 0, 0, 0, 0, 0, 0];
        let mut producer_zero = [0; 26];
        producer_zero[..2].copy_from_slice(&[1, 49]);
        if !completed {
            let mut batch = Prepared::new(BootstrapLimits {
                max_mappings: 1,
                max_batch_bytes: 2048,
            })?;
            batch.put(handoff.record.key(), frame(&[], &[encoded.as_bytes()])?)?;
            batch.put(
                IngestKey::Manifest,
                frame(&[], &[handoff.record.next.encode().as_bytes()])?,
            )?;
            batch.put(handoff.record.source_key(), frame(&[], &[&source_zero])?)?;
            batch.put(
                handoff.record.producer_key(),
                frame(&[], &[&producer_zero])?,
            )?;
            self.store.commit_prepared(batch, &mut authority)?;
            self.store.manifest = handoff.record.next;
        }
        authority()?;
        Ok(ObservedHandoff {
            handoff: handoff.clone(),
            _permit: permit,
        })
    }
    /// Read-only local preflight before coordination or native handoff dispatch.
    pub(in crate::storage) fn validate_initial_handoff(
        &self,
        handoff: &InitialHandoff,
    ) -> Result<(), BootstrapError> {
        if self.store.write_uncertain {
            return Err(BootstrapError::CommitUnknown);
        }
        let completed = self.manifest() == &handoff.record.next;
        let sequence = if completed {
            handoff
                .native_sequence
                .checked_add(4)
                .ok_or(BootstrapError::StateMismatch)?
        } else {
            handoff.native_sequence
        };
        if self.store.handoff_root_identity()? != handoff.native_root
            || (!completed && self.manifest() != &handoff.record.previous)
            || self.store.db.latest_sequence_number() != sequence
        {
            return Err(BootstrapError::StateMismatch);
        }
        self.store.require_handoff_state(&handoff.record, completed)
    }

    /// Portable intent metadata may inspect the old state but cannot dispatch
    /// its transition. Application still requires a complete positive proof.
    pub(in crate::storage) fn validate_pending_initial_handoff(
        &self,
        record: &InitialHandoffRecord,
    ) -> Result<(), BootstrapError> {
        if self.store.write_uncertain {
            return Err(BootstrapError::CommitUnknown);
        }
        if self.manifest() != record.previous_manifest() {
            return Err(BootstrapError::StateMismatch);
        }
        self.store.require_handoff_state(record, false)
    }
}
impl IngestStore {
    fn require_handoff_state(
        &self,
        record: &InitialHandoffRecord,
        completed: bool,
    ) -> Result<(), BootstrapError> {
        let manifest = if completed {
            record.next
        } else {
            record.previous
        };
        self.handoff_value(IngestKey::Manifest, Some(manifest.encode().as_bytes()))?;
        let encoded = record.encode();
        let source_zero = [1, 2, 0, 0, 0, 0, 0, 0, 0, 0];
        let mut producer_zero = [0; 26];
        producer_zero[..2].copy_from_slice(&[1, 49]);
        for (key, bytes) in [
            (record.key(), encoded.as_bytes()),
            (record.source_key(), &source_zero),
            (record.producer_key(), &producer_zero),
        ] {
            self.handoff_value(key, if completed { Some(bytes) } else { None })?;
        }
        self.require_empty_handoff_target(record)?;
        self.require_no_epoch_inputs(
            record.slot,
            ProducerEpoch::new(2).expect("initial successor"),
        )
        .map_err(BootstrapError::RequestInput)
    }
    pub(super) fn handoff_root_identity(&self) -> Result<(u64, u64), BootstrapError> {
        let metadata =
            std::fs::metadata(&self._reservation.path.0).map_err(|_| BootstrapError::EngineRead)?;
        Ok((metadata.dev(), metadata.ino()))
    }
    fn handoff_value(&self, key: IngestKey, expected: Option<&[u8]>) -> Result<(), BootstrapError> {
        let key = key.encode().map_err(|_| BootstrapError::InvalidIntent)?;
        let cf = self
            .db
            .cf_handle(key.column_family().name())
            .ok_or(BootstrapError::EngineRead)?;
        let value = self
            .db
            .get_pinned_cf_opt(cf, key.as_bytes(), &read_options())
            .map_err(|_| BootstrapError::EngineRead)?;
        if value.as_deref() != expected {
            return Err(BootstrapError::StateMismatch);
        }
        Ok(())
    }
    fn handoff_empty_prefix(&self, key: IngestKey, suffix: usize) -> Result<(), BootstrapError> {
        let key = key.encode().map_err(|_| BootstrapError::InvalidIntent)?;
        let prefix = &key.as_bytes()[..key.as_bytes().len() - suffix];
        let cf = self
            .db
            .cf_handle(key.column_family().name())
            .ok_or(BootstrapError::EngineRead)?;
        let mut cursor = self.db.raw_iterator_cf_opt(cf, read_options());
        cursor.seek(prefix);
        cursor.status().map_err(|_| BootstrapError::EngineRead)?;
        if cursor.key().is_some_and(|key| key.starts_with(prefix)) {
            return Err(BootstrapError::StateMismatch);
        }
        Ok(())
    }
    fn require_empty_handoff_target(
        &self,
        handoff: &InitialHandoffRecord,
    ) -> Result<(), BootstrapError> {
        let id = handoff.next.identity();
        let first = SourceRecordId {
            dataset_id: id.dataset_id,
            shard_id: id.shard_id,
            owner_epoch: id.owner_epoch,
            sequence: Sequence::new(1).expect("nonzero"),
        };
        self.handoff_empty_prefix(
            IngestKey::Fact {
                source_record_id: first,
            },
            8,
        )?;
        self.handoff_empty_prefix(IngestKey::PendingBatch { first }, 8)?;
        self.handoff_empty_prefix(
            IngestKey::Request {
                request_id: bigbrotr_model::ProducerRequestId {
                    producer_slot_id: handoff.slot,
                    producer_epoch: ProducerEpoch::new(2).expect("initial successor"),
                    request_sequence: bigbrotr_model::RequestSequence::new(1).expect("nonzero"),
                },
            },
            8,
        )?;
        for number in 1..=9 {
            let kind = DictionaryKind::try_from(number).expect("assigned bounded kind");
            if let Some(domain) = handoff.next.allocation_namespaces().domain(kind) {
                self.handoff_value(
                    IngestKey::AllocationStream {
                        dataset_id: id.dataset_id,
                        dictionary_kind: kind,
                        allocation_domain: domain,
                    },
                    None,
                )?;
                self.handoff_empty_prefix(
                    IngestKey::Dictionary {
                        reference: QualifiedRef {
                            dataset_id: id.dataset_id,
                            dictionary_kind: kind,
                            allocation_domain: domain,
                            ordinal: Ordinal::new(1).expect("nonzero"),
                        },
                    },
                    8,
                )?;
            }
        }
        Ok(())
    }
}
