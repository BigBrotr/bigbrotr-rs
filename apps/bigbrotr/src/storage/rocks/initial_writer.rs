//! Local custody for one live initial successor. Portable history is not a writer.
use super::{
    AdmissionError, BootstrapError, IngestRecovery, IngestStore, InitialHandoff, PendingBatch,
    PendingError, PendingReadLimits, ResolvedSource, SourceHead, VerifiedCompactedPrefix,
};
use crate::runtime::OwnedPermit;
use crate::{
    runtime::ownership::fresh_process_incarnation,
    storage::rocks::manifest::{AllocationNamespaces, IngestManifest},
};
use bigbrotr_model::{
    AllocationDomainId, DictionaryKind, OwnerEpoch, PolicyEpoch, ProcessIncarnation,
    ProducerRequestId, ProducerSlotId, SourceRecordId, TopologyGeneration,
};
use std::sync::atomic::AtomicBool;

/// Owns the exclusive native handle and identities minted in this process.
/// There is no caller-supplied identity, Clone, Deserialize or writer conversion.
pub struct InitialWriterCandidate {
    recovery: IngestRecovery,
    previous: IngestManifest,
    incarnation: ProcessIncarnation,
    role: [u8; 16],
    namespaces: AllocationNamespaces,
}

impl InitialWriterCandidate {
    /// Retained epoch2 state is never adopted. The old epoch1 producer still
    /// requires an explicit original or compacted proof and relational retirement.
    pub fn new(recovery: IngestRecovery) -> Result<Self, BootstrapError> {
        let previous = *recovery.manifest();
        if recovery.store.write_uncertain {
            return Err(BootstrapError::CommitUnknown);
        }
        if previous.key_schema() != 1 || previous.identity().owner_epoch.get() != 1 {
            return Err(BootstrapError::InvalidIntent);
        }
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
            let kind = DictionaryKind::try_from(number).expect("assigned dictionary kind");
            if previous.allocation_namespaces().domain(kind).is_some() {
                let domain = AllocationDomainId::try_from(*fresh()?.as_bytes())
                    .map_err(|_| BootstrapError::InvalidIntent)?;
                if entries[..count].iter().any(|(_, prior)| *prior == domain)
                    || (1..=9).any(|number| {
                        previous.allocation_namespaces().domain(
                            DictionaryKind::try_from(number).expect("assigned dictionary kind"),
                        ) == Some(domain)
                    })
                {
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

    /// Only this owner's positive source witness may select its exact target.
    /// The connected preparation independently checks policy and historical rows.
    pub fn handoff(
        &self,
        source: &ResolvedSource<'_>,
        policy: PolicyEpoch,
    ) -> Result<InitialHandoff, BootstrapError> {
        if !std::ptr::eq(source.recovery(), &self.recovery)
            || self.recovery.manifest() != &self.previous
        {
            return Err(BootstrapError::StateMismatch);
        }
        InitialHandoff::from_source(source, self.target_manifest(policy)?)
    }

    /// The compacted proof must borrow this actual candidate's excluded owner.
    /// It cannot select caller-supplied incarnation or allocation namespaces.
    pub fn handoff_compacted(
        &self,
        source: &VerifiedCompactedPrefix<'_>,
        policy: PolicyEpoch,
        cancelled: &AtomicBool,
    ) -> Result<InitialHandoff, AdmissionError> {
        if !source.matches_recovery(&self.recovery) || self.recovery.manifest() != &self.previous {
            return Err(AdmissionError::StateMismatch);
        }
        InitialHandoff::from_compacted(source, self.target_manifest(policy)?, cancelled)
    }

    fn target_manifest(&self, policy: PolicyEpoch) -> Result<IngestManifest, BootstrapError> {
        let mut identity = *self.previous.identity();
        identity.owner_epoch = OwnerEpoch::new(2).expect("initial successor");
        identity.topology_generation = TopologyGeneration::new(
            identity
                .topology_generation
                .get()
                .checked_add(1)
                .ok_or(BootstrapError::Limit)?,
        );
        identity.process_incarnation = self.incarnation;
        identity.policy_epoch = policy;
        Ok(IngestManifest::for_successor(identity, self.namespaces))
    }

    pub(in crate::storage) fn recovery_mut(&mut self) -> &mut IngestRecovery {
        &mut self.recovery
    }

    pub(in crate::storage) fn require_handoff(
        &self,
        handoff: &InitialHandoff,
    ) -> Result<(), BootstrapError> {
        self.require_target(handoff)?;
        self.recovery.validate_initial_handoff(handoff)
    }

    fn require_target(&self, handoff: &InitialHandoff) -> Result<(), BootstrapError> {
        if handoff.previous_manifest() != &self.previous
            || handoff.next_manifest().identity().process_incarnation != self.incarnation
            || handoff.next_manifest().allocation_namespaces() != &self.namespaces
        {
            return Err(BootstrapError::StateMismatch);
        }
        Ok(())
    }

    pub(in crate::storage) fn require_compacted_handoff(
        &self,
        handoff: &InitialHandoff,
        scratch: &OwnedPermit,
        cancelled: &AtomicBool,
    ) -> Result<(), BootstrapError> {
        self.require_target(handoff)?;
        self.recovery
            .validate_compacted_initial_handoff(handoff, scratch, cancelled)
    }

    pub(in crate::storage) fn require_prepared_compacted_handoff(
        &self,
        handoff: &InitialHandoff,
        scratch: &OwnedPermit,
        cancelled: &AtomicBool,
    ) -> Result<(), BootstrapError> {
        self.require_compacted_handoff(handoff, scratch, cancelled)?;
        if self.recovery.manifest() != handoff.next_manifest() {
            return Err(BootstrapError::StateMismatch);
        }
        Ok(())
    }

    pub(in crate::storage) fn require_prepared_handoff(
        &self,
        handoff: &InitialHandoff,
    ) -> Result<(), BootstrapError> {
        self.require_handoff(handoff)?;
        if self.recovery.manifest() != handoff.next_manifest() {
            return Err(BootstrapError::StateMismatch);
        }
        Ok(())
    }

    /// The connected adapter calls this only after confirmed activation COMMIT
    /// and final authority checks. Neither an observation nor a decoder calls it.
    pub(in crate::storage) fn activate(
        self,
        handoff: InitialHandoff,
    ) -> Result<ActiveInitialWriter, BootstrapError> {
        self.require_prepared_handoff(&handoff)?;
        Ok(self.activate_checked(handoff))
    }

    /// Same local custody transition, explicitly after compacted PG checks and
    /// confirmed activation COMMIT. Recovery/live frontier remain separate.
    pub(in crate::storage) fn activate_compacted(
        self,
        handoff: InitialHandoff,
        scratch: &OwnedPermit,
        cancelled: &AtomicBool,
    ) -> Result<ActiveInitialWriter, BootstrapError> {
        self.require_prepared_compacted_handoff(&handoff, scratch, cancelled)?;
        Ok(self.activate_checked(handoff))
    }

    fn activate_checked(self, handoff: InitialHandoff) -> ActiveInitialWriter {
        let mut store = self.recovery.store;
        store.live_initial = Some(LiveInitialBinding {
            manifest: *handoff.next_manifest(),
            slot: handoff.producer_slot_id(),
        });
        ActiveInitialWriter {
            store,
            handoff,
            role: self.role,
        }
    }
}

/// Native custody, still requiring the connected adapter's current authority
/// for every operation. No public method exposes or detaches the mutable store.
pub struct ActiveInitialWriter {
    store: IngestStore,
    handoff: InitialHandoff,
    role: [u8; 16],
}
impl ActiveInitialWriter {
    pub fn producer_credit(&self) -> Result<super::ProducerCredit, AdmissionError> {
        self.store.producer_credit(self.producer_slot_id())
    }
    pub fn manifest(&self) -> &IngestManifest {
        self.store.manifest()
    }
    pub fn handoff(&self) -> &InitialHandoff {
        &self.handoff
    }
    pub fn role_instance_id(&self) -> [u8; 16] {
        self.role
    }
    pub fn producer_slot_id(&self) -> ProducerSlotId {
        self.handoff.producer_slot_id()
    }
    pub(in crate::storage) fn store(&self) -> &IngestStore {
        &self.store
    }
    pub(in crate::storage) fn store_mut(&mut self) -> &mut IngestStore {
        &mut self.store
    }
}

pub(super) struct LiveInitialBinding {
    manifest: IngestManifest,
    slot: ProducerSlotId,
}

impl IngestStore {
    /// The initial kernel retains its original epoch1 behavior. An epoch2 read
    /// or mutation kernel requires the capability retained in this actual owner.
    pub(super) fn require_admission_owner(
        &self,
        request: Option<ProducerRequestId>,
    ) -> Result<(), AdmissionError> {
        let epoch = self.manifest.identity().owner_epoch.get();
        if self.manifest.key_schema() == 1
            && epoch == 1
            && request.is_none_or(|request| request.producer_epoch.get() == 1)
        {
            return Ok(());
        }
        if epoch == 2
            && self.live_initial.as_ref().is_some_and(|live| {
                live.manifest == self.manifest
                    && request.is_none_or(|request| {
                        request.producer_epoch.get() == 2 && request.producer_slot_id == live.slot
                    })
            })
        {
            return Ok(());
        }
        if let Some(frontier) = &self.live_successor {
            return frontier.require_identity(self, request);
        }
        Err(AdmissionError::InvalidInput)
    }
}

/// Native read mechanics, constructible only by the continuously retained owner.
/// The PostgreSQL live wrapper supplies the connected authority for effects.
pub(in crate::storage) struct InitialIndexSource<'a> {
    writer: &'a ActiveInitialWriter,
}
impl ActiveInitialWriter {
    pub(in crate::storage) fn index_source(
        &self,
    ) -> Result<InitialIndexSource<'_>, AdmissionError> {
        self.handoff().require_live_source(self.store())?;
        self.producer_credit()?;
        Ok(InitialIndexSource { writer: self })
    }
}
impl InitialIndexSource<'_> {
    pub fn manifest(&self) -> &IngestManifest {
        self.writer.manifest()
    }
    pub fn handoff(&self) -> &InitialHandoff {
        self.writer.handoff()
    }
    pub fn observe(&self, cancelled: &AtomicBool) -> Result<SourceHead, AdmissionError> {
        self.handoff().require_live_source(self.writer.store())?;
        self.writer
            .store()
            .observe_source_head(self.manifest(), cancelled)
            .map_err(|_| AdmissionError::StateMismatch)
    }
    /// Counts actual requests, independently of fact counts and the bridge's
    /// extra publication revision. The consumer enforces its own finite limit.
    pub fn retained_requests(&self) -> Result<u64, AdmissionError> {
        self.handoff().require_live_source(self.writer.store())?;
        self.handoff()
            .issued_through()
            .checked_add(self.writer.producer_credit()?.issued_through())
            .ok_or(AdmissionError::Limit)
    }
    pub fn read_pending_batch(
        &self,
        first: SourceRecordId,
        limits: PendingReadLimits,
        permit: OwnedPermit,
        cancelled: &AtomicBool,
    ) -> Result<Option<PendingBatch>, PendingError> {
        if permit.request().units().data_bytes() < limits.required_data_bytes()?
            || permit.request().units().items() == 0
        {
            return Err(PendingError::InsufficientReservation);
        }
        if cancelled.load(std::sync::atomic::Ordering::Acquire) {
            return Err(PendingError::Cancelled);
        }
        let head = self
            .observe(cancelled)
            .map_err(|_| PendingError::IncompleteBatch)?;
        let id = head.identity();
        if first.dataset_id != id.dataset_id
            || first.shard_id != id.shard_id
            || first.owner_epoch != id.owner_epoch
        {
            return Err(PendingError::IncompleteBatch);
        }
        if head.through().get().checked_add(1) == Some(first.sequence.get()) {
            return Ok(None);
        }
        if first.sequence.get() > head.through().get() {
            return Err(PendingError::IncompleteBatch);
        }
        let pending = self
            .writer
            .store()
            .read_pending_batch(first, limits, permit, cancelled)?;
        let after = self
            .observe(cancelled)
            .map_err(|_| PendingError::IncompleteBatch)?;
        if after.manifest() != head.manifest()
            || after.through() < head.through()
            || pending
                .as_ref()
                .is_some_and(|batch| batch.batch().last().sequence.get() > head.through().get())
        {
            return Err(PendingError::IncompleteBatch);
        }
        Ok(pending)
    }
}
