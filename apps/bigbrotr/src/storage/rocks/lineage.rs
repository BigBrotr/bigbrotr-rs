//! Retained lineage inspection. Structural history is never a positive frontier.
use super::bootstrap::read_options;
use super::producer_closing::ProducerState;
use super::{BootstrapError, IngestRecovery, InitialHandoffRecord};
use crate::runtime::OwnedPermit;
use crate::storage::rocks::{
    keys::{ColumnFamily, IngestKey, decode_key},
    manifest::IngestManifest,
    succession::{
        LINEAGE_ENTRY_BYTES, LINEAGE_SCRATCH_BYTES, PredecessorTerminalKind, SuccessionProfile,
        SuccessionRecord,
    },
};
use bigbrotr_model::{
    DictionaryKind, OwnerEpoch, ProducerEpoch, ProducerRequestId, ProducerSlotId,
};
use sha2::{Digest, Sha256};
use std::{
    mem::size_of,
    sync::atomic::{AtomicBool, Ordering},
};

#[path = "successor_frontier.rs"]
mod frontier;
pub(super) use frontier::AdmittedFrontier;

#[path = "lineage_census.rs"]
mod census;

/// A retained claim from an outgoing handoff, not a verified terminal result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetainedTerminalKind {
    Initial,
    Activated,
    UnactivatedEmpty,
}

/// One manifest and fixed coordinates. No raw receipt or request vector lives
/// here. The head has no outgoing terminal claim, even if its marker is CLOSING.
pub struct RetainedLineageEntry {
    manifest: IngestManifest,
    issued: u64,
    source_through: u64,
    incoming_sha256: [u8; 32],
    terminal_digest: [u8; 32],
    terminal_kind: Option<RetainedTerminalKind>,
    closing: bool,
    source_present: bool,
    allocations: [u64; 9],
}
const _: () = assert!(size_of::<RetainedLineageEntry>() <= LINEAGE_ENTRY_BYTES as usize);

impl RetainedLineageEntry {
    pub fn manifest(&self) -> &IngestManifest {
        &self.manifest
    }
    pub fn issued_through(&self) -> u64 {
        self.issued
    }
    pub fn source_through(&self) -> u64 {
        self.source_through
    }
    pub fn incoming_handoff_sha256(&self) -> [u8; 32] {
        self.incoming_sha256
    }
    pub fn terminal_claim(&self) -> Option<(RetainedTerminalKind, [u8; 32])> {
        self.terminal_kind.map(|kind| (kind, self.terminal_digest))
    }
    pub fn is_closing(&self) -> bool {
        self.closing
    }
}

/// Checked retained shape and content-stability guards under a metadata grant.
/// It does not decode original requests, validate scientific effects, establish
/// SQL lifecycle/activation, or expose a writer or a handoff conversion.
pub struct RetainedLineage {
    entries: Vec<RetainedLineageEntry>,
    slot: ProducerSlotId,
    root: (u64, u64),
    sequence: u64,
    inputs: [u8; 32],
    profile: SuccessionProfile,
    _permit: OwnedPermit,
}

/// Private snapshot identity. This is not a positive frontier by itself.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct LineageStamp {
    root: (u64, u64),
    pub(super) sequence: u64,
    inputs: [u8; 32],
}

impl RetainedLineage {
    /// A pending PG target counts for resource admission only. Its existence,
    /// absence of activation and exact recorded identity remain PG obligations.
    pub fn inspect(
        recovery: &IngestRecovery,
        pending_target: Option<u64>,
        permit: OwnedPermit,
        cancelled: &AtomicBool,
    ) -> Result<Self, BootstrapError> {
        let head = recovery.manifest();
        let epoch = head.identity().owner_epoch.get();
        let profile = SuccessionProfile::for_recovery(epoch, pending_target)
            .map_err(|_| BootstrapError::InsufficientReservation)?;
        if permit.request().units().data_bytes() < profile.required_metadata_bytes()
            || permit.request().units().items() == 0
        {
            return Err(BootstrapError::InsufficientReservation);
        }
        check_cancelled(cancelled)?;
        if recovery.store.write_uncertain {
            return Err(BootstrapError::CommitUnknown);
        }
        if head.key_schema() != if epoch == 2 { 2 } else { 3 } {
            return Err(BootstrapError::StateMismatch);
        }
        let root = recovery.store.handoff_root_identity()?;
        let sequence = recovery.store.db.latest_sequence_number();
        let count = usize::try_from(epoch).map_err(|_| BootstrapError::Limit)?;
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(count)
            .map_err(|_| BootstrapError::Allocation)?;
        // Capacity, including any allocator rounding, is charged before use.
        // The Vec/header and decoder temporaries use the fixed scratch share.
        if entries
            .capacity()
            .checked_mul(size_of::<RetainedLineageEntry>())
            .is_none_or(|bytes| bytes as u64 > LINEAGE_ENTRY_BYTES * profile.registered_epochs())
            || size_of::<Self>() > LINEAGE_SCRATCH_BYTES as usize / 4
        {
            return Err(BootstrapError::InsufficientReservation);
        }
        let initial_key = handoff_key(head, 1)?;
        let initial_bytes = value(recovery, initial_key)?;
        let initial = InitialHandoffRecord::decode(&initial_bytes)?;
        let slot = initial.producer_slot_id();
        let mut previous = *initial.next_manifest();
        let mut incoming: [u8; 32] = Sha256::digest(&initial_bytes).into();
        require_fresh(&[], initial.previous_manifest())?;
        entries.push(entry(
            recovery,
            *initial.previous_manifest(),
            [0; 32],
            Some((
                RetainedTerminalKind::Initial,
                initial.issued_through(),
                initial.last_complete_sequence(),
                initial.closure_digest(),
            )),
            slot,
        )?);
        drop(initial_bytes);
        for predecessor in 2..epoch {
            check_cancelled(cancelled)?;
            let key = handoff_key(head, predecessor)?;
            let bytes = value(recovery, key)?;
            let record =
                SuccessionRecord::decode(&bytes).map_err(|_| BootstrapError::StateMismatch)?;
            let fields = record.fields();
            if record.key() != key
                || fields.producer_slot_id != slot
                || fields.previous_manifest != previous
                || fields.incoming_handoff_sha256 != incoming
            {
                return Err(BootstrapError::StateMismatch);
            }
            let kind = match fields.terminal_kind {
                PredecessorTerminalKind::Activated => RetainedTerminalKind::Activated,
                PredecessorTerminalKind::UnactivatedEmpty => RetainedTerminalKind::UnactivatedEmpty,
            };
            require_fresh(&entries, &previous)?;
            entries.push(entry(
                recovery,
                previous,
                incoming,
                Some((
                    kind,
                    fields.issued_through,
                    fields.last_complete_sequence,
                    fields.closure_digest,
                )),
                slot,
            )?);
            previous = fields.next_manifest;
            incoming = Sha256::digest(&bytes).into();
        }
        if previous != *head {
            return Err(BootstrapError::StateMismatch);
        }
        require_fresh(&entries, head)?;
        entries.push(entry(recovery, *head, incoming, None, slot)?);
        let mut lineage = Self {
            entries,
            slot,
            root,
            sequence,
            inputs: [0; 32],
            profile,
            _permit: permit,
        };
        lineage.inspect_census(recovery, cancelled)?;
        lineage.inputs = lineage.input_census(recovery, cancelled)?;
        lineage.validate(recovery, cancelled)?;
        Ok(lineage)
    }

    pub fn entries(&self) -> &[RetainedLineageEntry] {
        &self.entries
    }
    pub fn entry(&self, epoch: u64) -> Option<&RetainedLineageEntry> {
        self.at(epoch)
    }
    pub fn native_head(&self) -> &RetainedLineageEntry {
        self.entries
            .last()
            .expect("inspection requires genesis and head")
    }
    pub fn pending_target(&self) -> Option<u64> {
        (self.profile.registered_epochs() > self.entries.len() as u64)
            .then_some(self.profile.registered_epochs())
    }
    pub fn producer_slot_id(&self) -> ProducerSlotId {
        self.slot
    }
    pub fn profile(&self) -> SuccessionProfile {
        self.profile
    }

    pub(super) fn input_stamp(&self) -> [u8; 32] {
        self.inputs
    }

    pub(super) fn transition_stamp(&self) -> LineageStamp {
        LineageStamp {
            root: self.root,
            sequence: self.sequence,
            inputs: self.inputs,
        }
    }

    /// The retained grant must already cover recovery of the proposed attempt.
    /// This does not invent an observed pending PostgreSQL preparation.
    pub(super) fn require_preparation_capacity(&self) -> Result<(), BootstrapError> {
        let future = SuccessionProfile::for_preparation(
            self.native_head().manifest().identity().owner_epoch.get(),
        )
        .map_err(|_| BootstrapError::InsufficientReservation)?;
        if self._permit.request().units().data_bytes() < future.required_metadata_bytes() {
            return Err(BootstrapError::InsufficientReservation);
        }
        Ok(())
    }

    pub(super) fn require_fresh_successor(
        &self,
        next: &IngestManifest,
    ) -> Result<(), BootstrapError> {
        require_fresh(&self.entries, next)
    }

    /// A sealed transition supplies its one exact permitted manifest/sequence.
    /// Ordinary validate remains strict: this cannot refresh a stale lineage or
    /// publish another positive frontier after a native write.
    pub(super) fn validate_transition_snapshot(
        &self,
        recovery: &IngestRecovery,
        stamp: LineageStamp,
        manifest: &IngestManifest,
        sequence: u64,
        cancelled: &AtomicBool,
    ) -> Result<(), BootstrapError> {
        check_cancelled(cancelled)?;
        if recovery.store.write_uncertain {
            return Err(BootstrapError::CommitUnknown);
        }
        if self.transition_stamp() != stamp
            || recovery.manifest() != manifest
            || recovery.store.handoff_root_identity()? != stamp.root
            || recovery.store.db.latest_sequence_number() != sequence
            || self.input_census(recovery, cancelled)? != stamp.inputs
            || recovery.store.handoff_root_identity()? != stamp.root
            || recovery.store.db.latest_sequence_number() != sequence
        {
            return Err(BootstrapError::StateMismatch);
        }
        check_cancelled(cancelled)
    }

    /// Sequence equality cannot witness sidecar stability. Re-read the entire
    /// finite spool, including contents, before publishing this observation.
    pub fn validate(
        &self,
        recovery: &IngestRecovery,
        cancelled: &AtomicBool,
    ) -> Result<(), BootstrapError> {
        check_cancelled(cancelled)?;
        if recovery.store.write_uncertain {
            return Err(BootstrapError::CommitUnknown);
        }
        if self.entries.last().map(|e| &e.manifest) != Some(recovery.manifest())
            || recovery.store.handoff_root_identity()? != self.root
            || recovery.store.db.latest_sequence_number() != self.sequence
            || self.input_census(recovery, cancelled)? != self.inputs
            || recovery.store.handoff_root_identity()? != self.root
            || recovery.store.db.latest_sequence_number() != self.sequence
        {
            return Err(BootstrapError::StateMismatch);
        }
        check_cancelled(cancelled)
    }

    fn input_census(
        &self,
        recovery: &IngestRecovery,
        cancelled: &AtomicBool,
    ) -> Result<[u8; 32], BootstrapError> {
        let expected = self
            .entries
            .iter()
            .try_fold(0_u64, |total, entry| total.checked_add(entry.issued))
            .filter(|total| *total <= 64)
            .ok_or(BootstrapError::Limit)?;
        recovery.store.lineage_input_census(
            expected,
            |id: ProducerRequestId| {
                id.producer_slot_id == self.slot
                    && self
                        .at(id.producer_epoch.get())
                        .is_some_and(|entry| id.request_sequence.get() <= entry.issued)
            },
            cancelled,
        )
    }
    fn at(&self, epoch: u64) -> Option<&RetainedLineageEntry> {
        usize::try_from(epoch.checked_sub(1)?)
            .ok()
            .and_then(|index| self.entries.get(index))
    }
}

fn entry(
    recovery: &IngestRecovery,
    manifest: IngestManifest,
    incoming_sha256: [u8; 32],
    terminal: Option<(RetainedTerminalKind, u64, u64, [u8; 32])>,
    slot: ProducerSlotId,
) -> Result<RetainedLineageEntry, BootstrapError> {
    let epoch = manifest.identity().owner_epoch.get();
    let producer = recovery
        .store
        .producer_state(
            slot,
            ProducerEpoch::new(epoch).map_err(|_| BootstrapError::InvalidIntent)?,
        )?
        .ok_or(BootstrapError::StateMismatch)?;
    if producer.issued > 64 || producer.acknowledged != 0 || producer.retired != 0 {
        return Err(BootstrapError::StateMismatch);
    }
    let source = counter(recovery, source_key(&manifest))?;
    // Legacy genesis could close at zero after no-fact requests, without ever
    // creating a counter. Exact originals still need their later positive replay.
    if source.is_none()
        && !(epoch == 1 && producer.closing && terminal.is_some_and(|(_, _, cut, _)| cut == 0))
    {
        return Err(BootstrapError::StateMismatch);
    }
    let source_through = source.unwrap_or(0);
    if (producer.issued == 0 && source_through != 0)
        || terminal.is_some_and(|(kind, issued, cut, _)| {
            !producer.closing
                || issued != producer.issued
                || cut != source_through
                || (kind == RetainedTerminalKind::UnactivatedEmpty && (issued != 0 || cut != 0))
        })
    {
        return Err(BootstrapError::StateMismatch);
    }
    let mut allocations = [0; 9];
    for number in 1..=9 {
        let kind = DictionaryKind::try_from(number).expect("assigned kind");
        if let Some(domain) = manifest.allocation_namespaces().domain(kind) {
            let value = counter(
                recovery,
                IngestKey::AllocationStream {
                    dataset_id: manifest.identity().dataset_id,
                    dictionary_kind: kind,
                    allocation_domain: domain,
                },
            )?;
            if value.is_some_and(|value| value == 0 || value > source_through) {
                return Err(BootstrapError::StateMismatch);
            }
            allocations[number as usize - 1] = value.unwrap_or(0);
        }
    }
    Ok(RetainedLineageEntry {
        manifest,
        issued: producer.issued,
        source_through,
        incoming_sha256,
        terminal_digest: terminal.map_or([0; 32], |(_, _, _, digest)| digest),
        terminal_kind: terminal.map(|(kind, _, _, _)| kind),
        closing: producer.closing,
        source_present: source.is_some(),
        allocations,
    })
}

fn require_fresh(
    entries: &[RetainedLineageEntry],
    next: &IngestManifest,
) -> Result<(), BootstrapError> {
    // Domains are identities across kinds too, including within genesis.
    for number in 1..=9 {
        if let Some(domain) = next
            .allocation_namespaces()
            .domain(DictionaryKind::try_from(number).expect("assigned kind"))
        {
            for other in 1..number {
                if next
                    .allocation_namespaces()
                    .domain(DictionaryKind::try_from(other).expect("assigned kind"))
                    == Some(domain)
                {
                    return Err(BootstrapError::StateMismatch);
                }
            }
        }
    }
    for old in entries {
        if old.manifest.identity().process_incarnation == next.identity().process_incarnation {
            return Err(BootstrapError::StateMismatch);
        }
        for number in 1..=9 {
            let kind = DictionaryKind::try_from(number).expect("assigned kind");
            if let Some(domain) = next.allocation_namespaces().domain(kind) {
                for old_number in 1..=9 {
                    if old
                        .manifest
                        .allocation_namespaces()
                        .domain(DictionaryKind::try_from(old_number).expect("assigned kind"))
                        == Some(domain)
                    {
                        return Err(BootstrapError::StateMismatch);
                    }
                }
            }
        }
    }
    Ok(())
}

fn handoff_key(head: &IngestManifest, epoch: u64) -> Result<IngestKey, BootstrapError> {
    Ok(IngestKey::OwnerHandoff {
        dataset_id: head.identity().dataset_id,
        owner_resource_id: head.identity().owner_resource_id,
        predecessor_epoch: OwnerEpoch::new(epoch).map_err(|_| BootstrapError::InvalidIntent)?,
    })
}
fn source_key(manifest: &IngestManifest) -> IngestKey {
    let id = manifest.identity();
    IngestKey::SourceStream {
        dataset_id: id.dataset_id,
        shard_id: id.shard_id,
        owner_epoch: id.owner_epoch,
    }
}
fn value(
    recovery: &IngestRecovery,
    key: IngestKey,
) -> Result<rocksdb::DBPinnableSlice<'_>, BootstrapError> {
    let key = key.encode().map_err(|_| BootstrapError::InvalidIntent)?;
    recovery
        .store
        .db
        .get_pinned_cf_opt(
            recovery
                .store
                .db
                .cf_handle(key.column_family().name())
                .ok_or(BootstrapError::EngineRead)?,
            key.as_bytes(),
            &read_options(),
        )
        .map_err(|_| BootstrapError::EngineRead)?
        .ok_or(BootstrapError::StateMismatch)
}
fn counter(recovery: &IngestRecovery, key: IngestKey) -> Result<Option<u64>, BootstrapError> {
    let encoded = key.encode().map_err(|_| BootstrapError::InvalidIntent)?;
    let bytes = recovery
        .store
        .db
        .get_pinned_opt(encoded.as_bytes(), &read_options())
        .map_err(|_| BootstrapError::EngineRead)?;
    bytes
        .map(|bytes| {
            if bytes.len() != 10 || bytes[..2] != key.domain().prefix() {
                return Err(BootstrapError::StateMismatch);
            }
            Ok(u64::from_be_bytes(
                bytes[2..].try_into().expect("fixed counter"),
            ))
        })
        .transpose()
}
fn check_cancelled(cancelled: &AtomicBool) -> Result<(), BootstrapError> {
    if cancelled.load(Ordering::Relaxed) {
        Err(BootstrapError::AuthorityUnavailable)
    } else {
        Ok(())
    }
}
