//! Local custody for one live initial successor. Portable history is not a writer.
use super::{
    AdmissionError, BootstrapError, IngestRecovery, IngestStore, InitialHandoff, ResolvedSource,
};
use crate::{
    runtime::ownership::fresh_process_incarnation,
    storage::rocks::manifest::{AllocationNamespaces, IngestManifest},
};
use bigbrotr_model::{
    AllocationDomainId, DictionaryKind, OwnerEpoch, PolicyEpoch, ProcessIncarnation,
    ProducerRequestId, ProducerSlotId, TopologyGeneration,
};

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
    /// requires complete positive replay and relational retirement validation.
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
        InitialHandoff::from_source(
            source,
            IngestManifest::for_successor(identity, self.namespaces),
        )
    }

    pub(in crate::storage) fn recovery_mut(&mut self) -> &mut IngestRecovery {
        &mut self.recovery
    }

    pub(in crate::storage) fn require_handoff(
        &self,
        handoff: &InitialHandoff,
    ) -> Result<(), BootstrapError> {
        if handoff.previous_manifest() != &self.previous
            || handoff.next_manifest().identity().process_incarnation != self.incarnation
            || handoff.next_manifest().allocation_namespaces() != &self.namespaces
        {
            return Err(BootstrapError::StateMismatch);
        }
        self.recovery.validate_initial_handoff(handoff)
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
        let mut store = self.recovery.store;
        store.live_initial = Some(LiveInitialBinding {
            manifest: *handoff.next_manifest(),
            slot: handoff.producer_slot_id(),
        });
        Ok(ActiveInitialWriter {
            store,
            handoff,
            role: self.role,
        })
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
