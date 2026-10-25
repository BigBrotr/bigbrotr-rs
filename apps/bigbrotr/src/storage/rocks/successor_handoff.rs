//! Positive native succession plans. PostgreSQL preparation and activation are
//! separate obligations; neither portable bytes nor gap recovery mint a writer.
use super::bootstrap::{Prepared, frame, read_options};
use super::lineage::LineageStamp;
use super::{
    BootstrapError, BootstrapLimits, IngestRecovery, ResolvedLineage, RetainedLineage,
    RetainedTerminalKind,
};
use crate::runtime::{OwnedPermit, ownership::fresh_process_incarnation};
use crate::storage::rocks::{
    keys::IngestKey,
    manifest::{AllocationNamespaces, IngestManifest},
    succession::{
        EncodedSuccession, PredecessorTerminalKind, SuccessionFields, SuccessionProfile,
        SuccessionRecord,
    },
};
use bigbrotr_model::{
    AllocationDomainId, DictionaryKind, OwnerEpoch, PolicyEpoch, ProcessIncarnation, ProducerEpoch,
    ProducerSlotId, TopologyGeneration,
};
use std::sync::atomic::{AtomicBool, Ordering};

pub(super) const EMPTY_SOURCE: [u8; 10] = [1, 2, 0, 0, 0, 0, 0, 0, 0, 0];
pub(super) const OPEN_PRODUCER: [u8; 26] = {
    let mut bytes = [0; 26];
    bytes[0] = 1;
    bytes[1] = 0x31;
    bytes
};
pub(super) const CLOSING_PRODUCER: [u8; 27] = {
    let mut bytes = [0; 27];
    bytes[0] = 2;
    bytes[1] = 0x31;
    bytes[26] = 1;
    bytes
};

/// Fresh local identity and continuous exclusive custody. No identity arguments,
/// historical conversion, Clone, Deserialize or public writer conversion exists.
pub struct SuccessorCandidate {
    recovery: IngestRecovery,
    previous: IngestManifest,
    incarnation: ProcessIncarnation,
    role: [u8; 16],
    namespaces: AllocationNamespaces,
}
impl SuccessorCandidate {
    pub fn new(recovery: IngestRecovery) -> Result<Self, BootstrapError> {
        let previous = *recovery.manifest();
        let epoch = previous.identity().owner_epoch.get();
        if recovery.store.write_uncertain {
            return Err(BootstrapError::CommitUnknown);
        }
        if epoch < 2 || previous.key_schema() != if epoch == 2 { 2 } else { 3 } {
            return Err(BootstrapError::InvalidIntent);
        }
        SuccessionProfile::for_preparation(epoch)
            .map_err(|_| BootstrapError::InsufficientReservation)?;
        let fresh =
            || fresh_process_incarnation().map_err(|_| BootstrapError::AuthorityUnavailable);
        let incarnation = fresh()?;
        let role = *fresh()?.as_bytes();
        if incarnation == previous.identity().process_incarnation || role == *incarnation.as_bytes()
        {
            return Err(BootstrapError::InvalidIntent);
        }
        let mut entries = [(
            DictionaryKind::Event,
            AllocationDomainId::try_from([1; 16]).expect("nonzero"),
        ); 9];
        let mut count = 0;
        for number in 1..=9 {
            let kind = DictionaryKind::try_from(number).expect("assigned kind");
            if previous.allocation_namespaces().domain(kind).is_some() {
                let domain = AllocationDomainId::try_from(*fresh()?.as_bytes())
                    .map_err(|_| BootstrapError::InvalidIntent)?;
                if entries[..count].iter().any(|(_, old)| *old == domain) {
                    return Err(BootstrapError::InvalidIntent);
                }
                entries[count] = (kind, domain);
                count += 1;
            }
        }
        let namespaces = AllocationNamespaces::from_entries(&entries[..count])
            .map_err(|_| BootstrapError::InvalidIntent)?;
        Ok(Self {
            recovery,
            previous,
            incarnation,
            role,
            namespaces,
        })
    }

    pub fn recovery(&self) -> &IngestRecovery {
        &self.recovery
    }
    pub fn process_incarnation(&self) -> ProcessIncarnation {
        self.incarnation
    }
    pub fn role_instance_id(&self) -> [u8; 16] {
        self.role
    }
    pub fn allocation_namespaces(&self) -> &AllocationNamespaces {
        &self.namespaces
    }

    pub fn handoff(
        &self,
        proof: &ResolvedLineage<'_>,
        policy: PolicyEpoch,
        cancelled: &AtomicBool,
    ) -> Result<SuccessorHandoff, BootstrapError> {
        self.handoff_in(proof, policy, cancelled, SuccessorBasis::Original)
    }

    pub fn handoff_compacted(
        &self,
        proof: &ResolvedLineage<'_>,
        policy: PolicyEpoch,
        cancelled: &AtomicBool,
    ) -> Result<SuccessorHandoff, BootstrapError> {
        self.handoff_in(proof, policy, cancelled, SuccessorBasis::Compacted)
    }

    fn handoff_in(
        &self,
        proof: &ResolvedLineage<'_>,
        policy: PolicyEpoch,
        cancelled: &AtomicBool,
        basis: SuccessorBasis,
    ) -> Result<SuccessorHandoff, BootstrapError> {
        if !std::ptr::eq(proof.recovery(), &self.recovery)
            || self.recovery.manifest() != &self.previous
            || proof.lineage().pending_target().is_some()
        {
            return Err(BootstrapError::StateMismatch);
        }
        let mut identity = *self.previous.identity();
        identity.owner_epoch = OwnerEpoch::new(
            identity
                .owner_epoch
                .get()
                .checked_add(1)
                .ok_or(BootstrapError::Limit)?,
        )
        .map_err(|_| BootstrapError::InvalidIntent)?;
        identity.topology_generation = TopologyGeneration::new(
            identity
                .topology_generation
                .get()
                .checked_add(1)
                .ok_or(BootstrapError::Limit)?,
        );
        identity.process_incarnation = self.incarnation;
        identity.policy_epoch = policy;
        let next = IngestManifest::for_repeatable_successor(identity, self.namespaces);
        let summary = proof.last_epoch();
        let kind = terminal_kind(summary.terminal_kind)?;
        let record = SuccessionRecord::new(SuccessionFields {
            terminal_kind: kind,
            producer_slot_id: proof.lineage().producer_slot_id(),
            predecessor_epoch: ProducerEpoch::new(summary.epoch)
                .map_err(|_| BootstrapError::InvalidIntent)?,
            issued_through: summary.resolved_requests,
            last_complete_sequence: summary.last_complete_sequence,
            closure_digest: summary.digest,
            incoming_handoff_sha256: proof.lineage().native_head().incoming_handoff_sha256(),
            previous_manifest: self.previous,
            next_manifest: next,
        })
        .map_err(|_| BootstrapError::InvalidIntent)?;
        SuccessorHandoff::from_proof(proof, record, Some(self.role), cancelled, basis)
    }

    /// Read-only local binding. A reconstructed gap plan never matches a local
    /// candidate, even if its portable identity is otherwise identical.
    pub fn require_handoff(
        &self,
        lineage: &RetainedLineage,
        handoff: &SuccessorHandoff,
        cancelled: &AtomicBool,
    ) -> Result<(), BootstrapError> {
        self.require_handoff_in(lineage, handoff, cancelled, SuccessorBasis::Original)
    }

    pub fn require_compacted_handoff(
        &self,
        lineage: &RetainedLineage,
        handoff: &SuccessorHandoff,
        cancelled: &AtomicBool,
    ) -> Result<(), BootstrapError> {
        self.require_handoff_in(lineage, handoff, cancelled, SuccessorBasis::Compacted)
    }

    fn require_handoff_in(
        &self,
        lineage: &RetainedLineage,
        handoff: &SuccessorHandoff,
        cancelled: &AtomicBool,
        basis: SuccessorBasis,
    ) -> Result<(), BootstrapError> {
        let fields = handoff.record.fields();
        if handoff.candidate_role != Some(self.role)
            || fields.previous_manifest != self.previous
            || fields.next_manifest.identity().process_incarnation != self.incarnation
            || fields.next_manifest.allocation_namespaces() != &self.namespaces
        {
            return Err(BootstrapError::StateMismatch);
        }
        self.recovery
            .validate_successor_handoff_in(lineage, handoff, cancelled, basis)
    }

    pub(in crate::storage) fn require_prepared_handoff(
        &self,
        lineage: &RetainedLineage,
        handoff: &SuccessorHandoff,
        cancelled: &AtomicBool,
    ) -> Result<(), BootstrapError> {
        self.require_handoff(lineage, handoff, cancelled)?;
        if self.recovery.manifest() != &handoff.record.fields().next_manifest {
            return Err(BootstrapError::StateMismatch);
        }
        Ok(())
    }

    pub(in crate::storage) fn require_prepared_compacted_handoff(
        &self,
        lineage: &RetainedLineage,
        handoff: &SuccessorHandoff,
        cancelled: &AtomicBool,
    ) -> Result<(), BootstrapError> {
        self.require_compacted_handoff(lineage, handoff, cancelled)?;
        if self.recovery.manifest() != &handoff.record.fields().next_manifest {
            return Err(BootstrapError::StateMismatch);
        }
        Ok(())
    }

    // Infallible field transfer, called only after the envelope's final checked
    // prepared gate. No decoder or recovery path can call a public conversion.
    pub(super) fn into_writer_parts(self) -> (super::IngestStore, [u8; 16]) {
        (self.recovery.store, self.role)
    }

    pub(in crate::storage) fn apply_handoff(
        &mut self,
        lineage: &RetainedLineage,
        handoff: &SuccessorHandoff,
        permit: OwnedPermit,
        cancelled: &AtomicBool,
        authority: impl FnMut() -> Result<(), BootstrapError>,
    ) -> Result<ObservedSuccessorHandoff, BootstrapError> {
        self.require_handoff(lineage, handoff, cancelled)?;
        self.recovery
            .apply_successor_handoff(lineage, handoff, permit, cancelled, authority)
    }

    pub(in crate::storage) fn apply_compacted_handoff(
        &mut self,
        lineage: &RetainedLineage,
        handoff: &SuccessorHandoff,
        permit: OwnedPermit,
        cancelled: &AtomicBool,
        authority: impl FnMut() -> Result<(), BootstrapError>,
    ) -> Result<ObservedSuccessorHandoff, BootstrapError> {
        self.require_compacted_handoff(lineage, handoff, cancelled)?;
        self.recovery
            .apply_compacted_successor_handoff(lineage, handoff, permit, cancelled, authority)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum SuccessorBasis {
    Original,
    Compacted,
}
impl SuccessorBasis {
    pub(super) fn require(self, lineage: &RetainedLineage) -> Result<(), BootstrapError> {
        if lineage.has_compacted_initial() != (self == Self::Compacted) {
            return Err(BootstrapError::StateMismatch);
        }
        Ok(())
    }
}

/// Owned positive proof for one exact native transition. The separately retained
/// lineage owns its original metadata grant; this fixed plan owns no second Vec.
pub struct SuccessorHandoff {
    record: SuccessionRecord,
    stamp: LineageStamp,
    candidate_role: Option<[u8; 16]>,
    basis: SuccessorBasis,
}
impl SuccessorHandoff {
    pub fn record(&self) -> &SuccessionRecord {
        &self.record
    }
    pub fn encode(&self) -> EncodedSuccession {
        self.record.encode()
    }

    /// Rebind the exact PG-recorded intent to a positively replayed native-old
    /// history. This constructor cannot produce a locally minted candidate.
    pub fn reconstruct_gap(
        proof: &ResolvedLineage<'_>,
        record: SuccessionRecord,
        cancelled: &AtomicBool,
    ) -> Result<Self, BootstrapError> {
        Self::reconstruct_gap_in(proof, record, cancelled, SuccessorBasis::Original)
    }

    pub fn reconstruct_compacted_gap(
        proof: &ResolvedLineage<'_>,
        record: SuccessionRecord,
        cancelled: &AtomicBool,
    ) -> Result<Self, BootstrapError> {
        Self::reconstruct_gap_in(proof, record, cancelled, SuccessorBasis::Compacted)
    }

    fn reconstruct_gap_in(
        proof: &ResolvedLineage<'_>,
        record: SuccessionRecord,
        cancelled: &AtomicBool,
        basis: SuccessorBasis,
    ) -> Result<Self, BootstrapError> {
        if proof.lineage().pending_target()
            != Some(record.fields().next_manifest.identity().owner_epoch.get())
        {
            return Err(BootstrapError::StateMismatch);
        }
        Self::from_proof(proof, record, None, cancelled, basis)
    }

    fn from_proof(
        proof: &ResolvedLineage<'_>,
        record: SuccessionRecord,
        candidate_role: Option<[u8; 16]>,
        cancelled: &AtomicBool,
        basis: SuccessorBasis,
    ) -> Result<Self, BootstrapError> {
        basis.require(proof.lineage())?;
        proof.validate(cancelled)?;
        let lineage = proof.lineage();
        if proof.verified_through()
            != lineage
                .native_head()
                .manifest()
                .identity()
                .owner_epoch
                .get()
        {
            return Err(BootstrapError::StateMismatch);
        }
        require_predecessor(proof, &record)?;
        lineage.require_preparation_capacity()?;
        lineage.require_fresh_successor(&record.fields().next_manifest)?;
        let plan = Self {
            record,
            stamp: lineage.transition_stamp(),
            candidate_role,
            basis,
        };
        proof
            .recovery()
            .validate_successor_handoff_in(lineage, &plan, cancelled, basis)?;
        Ok(plan)
    }
}

pub struct ObservedSuccessorHandoff {
    record: SuccessionRecord,
    _permit: OwnedPermit,
}
impl ObservedSuccessorHandoff {
    pub fn record(&self) -> &SuccessionRecord {
        &self.record
    }
}

impl IngestRecovery {
    pub(in crate::storage) fn validate_successor_handoff(
        &self,
        lineage: &RetainedLineage,
        handoff: &SuccessorHandoff,
        cancelled: &AtomicBool,
    ) -> Result<(), BootstrapError> {
        self.validate_successor_handoff_in(lineage, handoff, cancelled, SuccessorBasis::Original)
    }

    pub(in crate::storage) fn validate_compacted_successor_handoff(
        &self,
        lineage: &RetainedLineage,
        handoff: &SuccessorHandoff,
        cancelled: &AtomicBool,
    ) -> Result<(), BootstrapError> {
        self.validate_successor_handoff_in(lineage, handoff, cancelled, SuccessorBasis::Compacted)
    }

    fn validate_successor_handoff_in(
        &self,
        lineage: &RetainedLineage,
        handoff: &SuccessorHandoff,
        cancelled: &AtomicBool,
        basis: SuccessorBasis,
    ) -> Result<(), BootstrapError> {
        basis.require(lineage)?;
        if handoff.basis != basis {
            return Err(BootstrapError::StateMismatch);
        }
        let f = handoff.record.fields();
        if lineage.native_head().manifest() != &f.previous_manifest
            || lineage.producer_slot_id() != f.producer_slot_id
        {
            return Err(BootstrapError::StateMismatch);
        }
        lineage.require_preparation_capacity()?;
        let completed = self.manifest() == &f.next_manifest;
        let sequence = if completed {
            handoff
                .stamp
                .sequence
                .checked_add(4)
                .ok_or(BootstrapError::StateMismatch)?
        } else {
            handoff.stamp.sequence
        };
        let expected = if completed {
            &f.next_manifest
        } else {
            &f.previous_manifest
        };
        lineage.validate_transition_snapshot(self, handoff.stamp, expected, sequence, cancelled)?;
        require_value(
            self,
            IngestKey::Manifest,
            Some(expected.encode().as_bytes()),
        )?;
        let encoded = handoff.encode();
        for (key, bytes) in [
            (handoff.record.key(), encoded.as_bytes()),
            (source_key(&f.next_manifest), &EMPTY_SOURCE[..]),
            (
                producer_key(&f.next_manifest, f.producer_slot_id),
                &OPEN_PRODUCER[..],
            ),
        ] {
            require_value(self, key, completed.then_some(bytes))?;
        }
        // The old complete census plus the exact sequence delta and four
        // necessary target keys excludes additional native writes. Sidecars
        // remain independently checked because they do not advance that delta.
        lineage.validate_transition_snapshot(self, handoff.stamp, expected, sequence, cancelled)
    }

    /// Called only by the connected adapter after confirmed PG preparation.
    /// A synchronized native error is unknown; no caller may replace custody
    /// until the actual native owner and its connected driver have ended.
    pub(in crate::storage) fn apply_successor_handoff(
        &mut self,
        lineage: &RetainedLineage,
        handoff: &SuccessorHandoff,
        permit: OwnedPermit,
        cancelled: &AtomicBool,
        authority: impl FnMut() -> Result<(), BootstrapError>,
    ) -> Result<ObservedSuccessorHandoff, BootstrapError> {
        self.apply_successor_handoff_in(
            lineage,
            handoff,
            permit,
            cancelled,
            authority,
            SuccessorBasis::Original,
        )
    }

    pub(in crate::storage) fn apply_compacted_successor_handoff(
        &mut self,
        lineage: &RetainedLineage,
        handoff: &SuccessorHandoff,
        permit: OwnedPermit,
        cancelled: &AtomicBool,
        authority: impl FnMut() -> Result<(), BootstrapError>,
    ) -> Result<ObservedSuccessorHandoff, BootstrapError> {
        self.apply_successor_handoff_in(
            lineage,
            handoff,
            permit,
            cancelled,
            authority,
            SuccessorBasis::Compacted,
        )
    }

    fn apply_successor_handoff_in(
        &mut self,
        lineage: &RetainedLineage,
        handoff: &SuccessorHandoff,
        permit: OwnedPermit,
        cancelled: &AtomicBool,
        mut authority: impl FnMut() -> Result<(), BootstrapError>,
        basis: SuccessorBasis,
    ) -> Result<ObservedSuccessorHandoff, BootstrapError> {
        require_permit(&permit, 8192)?;
        authority()?;
        self.validate_successor_handoff_in(lineage, handoff, cancelled, basis)?;
        let f = handoff.record.fields();
        if self.manifest() != &f.next_manifest {
            // Refuse sequence overflow before entering the native dispatch.
            handoff
                .stamp
                .sequence
                .checked_add(4)
                .ok_or(BootstrapError::Limit)?;
            let mut batch = Prepared::new(BootstrapLimits {
                max_mappings: 1,
                max_batch_bytes: 2048,
            })?;
            batch.put(
                handoff.record.key(),
                frame(&[], &[handoff.encode().as_bytes()])?,
            )?;
            batch.put(
                IngestKey::Manifest,
                frame(&[], &[f.next_manifest.encode().as_bytes()])?,
            )?;
            batch.put(source_key(&f.next_manifest), frame(&[], &[&EMPTY_SOURCE])?)?;
            batch.put(
                producer_key(&f.next_manifest, f.producer_slot_id),
                frame(&[], &[&OPEN_PRODUCER])?,
            )?;
            self.store.commit_prepared(batch, || {
                require_current_authority(cancelled, &mut authority)
            })?;
            self.store.manifest = f.next_manifest;
        }
        authority()?;
        self.validate_successor_handoff_in(lineage, handoff, cancelled, basis)?;
        Ok(ObservedSuccessorHandoff {
            record: handoff.record,
            _permit: permit,
        })
    }
}

pub(super) fn terminal_kind(
    kind: RetainedTerminalKind,
) -> Result<PredecessorTerminalKind, BootstrapError> {
    match kind {
        RetainedTerminalKind::Activated => Ok(PredecessorTerminalKind::Activated),
        RetainedTerminalKind::UnactivatedEmpty => Ok(PredecessorTerminalKind::UnactivatedEmpty),
        RetainedTerminalKind::Initial => Err(BootstrapError::InvalidIntent),
    }
}
pub(super) fn require_predecessor(
    proof: &ResolvedLineage<'_>,
    record: &SuccessionRecord,
) -> Result<(), BootstrapError> {
    let f = record.fields();
    let summary = proof.last_epoch();
    let entry = proof
        .lineage()
        .entry(summary.epoch)
        .ok_or(BootstrapError::StateMismatch)?;
    if f.predecessor_epoch.get() != summary.epoch
        || f.previous_manifest != *entry.manifest()
        || f.producer_slot_id != proof.lineage().producer_slot_id()
        || f.terminal_kind != terminal_kind(summary.terminal_kind)?
        || f.issued_through != summary.resolved_requests
        || f.last_complete_sequence != summary.last_complete_sequence
        || f.closure_digest != summary.digest
        || f.incoming_handoff_sha256 != entry.incoming_handoff_sha256()
    {
        return Err(BootstrapError::StateMismatch);
    }
    Ok(())
}
pub(super) fn source_key(manifest: &IngestManifest) -> IngestKey {
    let id = manifest.identity();
    IngestKey::SourceStream {
        dataset_id: id.dataset_id,
        shard_id: id.shard_id,
        owner_epoch: id.owner_epoch,
    }
}
pub(super) fn producer_key(manifest: &IngestManifest, slot: ProducerSlotId) -> IngestKey {
    IngestKey::ProducerState {
        producer_slot_id: slot,
        producer_epoch: ProducerEpoch::new(manifest.identity().owner_epoch.get())
            .expect("manifest epoch"),
    }
}
pub(super) fn require_value(
    recovery: &IngestRecovery,
    key: IngestKey,
    expected: Option<&[u8]>,
) -> Result<(), BootstrapError> {
    let key = key.encode().map_err(|_| BootstrapError::InvalidIntent)?;
    let value = recovery
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
        .map_err(|_| BootstrapError::EngineRead)?;
    if value.as_deref() != expected {
        return Err(BootstrapError::StateMismatch);
    }
    Ok(())
}
pub(super) fn require_permit(permit: &OwnedPermit, bytes: u64) -> Result<(), BootstrapError> {
    if permit.request().units().data_bytes() < bytes
        || permit.request().units().native_bytes() < bytes
        || permit.request().units().items() == 0
    {
        return Err(BootstrapError::InsufficientReservation);
    }
    Ok(())
}
pub(super) fn require_current_authority(
    cancelled: &AtomicBool,
    authority: &mut impl FnMut() -> Result<(), BootstrapError>,
) -> Result<(), BootstrapError> {
    if cancelled.load(Ordering::Relaxed) {
        return Err(BootstrapError::AuthorityUnavailable);
    }
    authority()?;
    if cancelled.load(Ordering::Relaxed) {
        return Err(BootstrapError::AuthorityUnavailable);
    }
    Ok(())
}
