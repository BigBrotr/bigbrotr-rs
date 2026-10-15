//! Private lifecycle dispatch. Portable historical data is never a live owner.
use super::*;
use bigbrotr::storage::{
    postgres::control_owner::*,
    rocks::store::{
        AdmissionError, AdmissionLimits, DictionaryPreparation, InitialWriterCandidate,
        StoredCertification, StoredOutcome, StoredReceipt, StoredVisibility,
    },
};
use bigbrotr::{acquisition::observation::StoredObservation, runtime::OwnedPermit};
use bigbrotr_model::{ProducerSlotId, QualifiedRef};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Profile {
    Initial,
    Successor,
}
impl Profile {
    pub(super) fn minimum(self, sync: bool) -> u64 {
        match (self, sync) {
            (Self::Initial, false) | (Self::Successor, true) => 2,
            (Self::Initial, true) => 4,
            (Self::Successor, false) => 1,
        }
    }
}

/// Private runtime selection, not a storage authority. Only the fresh initial
/// store needs its real registration; both live capabilities own their binding.
#[derive(Clone, Copy)]
pub(super) enum RuntimeContext<'a> {
    Initial(&'a InitialRegistration),
    Live,
}
impl<'a> RuntimeContext<'a> {
    pub(super) fn initial(self) -> std::result::Result<&'a InitialRegistration, OwnerError> {
        match self {
            Self::Initial(spec) => Ok(spec),
            Self::Live => Err(OwnerError::InvalidInput),
        }
    }
    pub(super) fn require(self, native: &NativeOwner) -> std::result::Result<(), OwnerError> {
        match (self, native) {
            (Self::Initial(_), NativeOwner::Initial(_))
            | (Self::Live, NativeOwner::Live(_) | NativeOwner::Successor(_)) => Ok(()),
            _ => Err(OwnerError::InvalidInput),
        }
    }
}

pub(super) enum NativeOwner {
    Initial(Box<IngestStore>),
    Candidate(Box<InitialWriterCandidate>),
    Live(Box<LiveInitialWriter>),
    Successor(Box<LiveSuccessorWriter>),
}
impl NativeOwner {
    pub(super) fn initial(&self) -> Result<&IngestStore> {
        match self {
            Self::Initial(store) => Ok(store),
            _ => Err((4, "initial-only consumer cannot use a successor".into())),
        }
    }
    pub(super) fn initial_mut(&mut self) -> Result<&mut IngestStore> {
        match self {
            Self::Initial(store) => Ok(store),
            _ => Err((4, "initial-only consumer cannot use a successor".into())),
        }
    }
    pub(super) fn profile(&self) -> Result<Profile> {
        match self {
            Self::Initial(_) => Ok(Profile::Initial),
            Self::Live(_) | Self::Successor(_) => Ok(Profile::Successor),
            Self::Candidate(_) => Err((4, "unactivated candidate cannot dispatch".into())),
        }
    }
    pub(super) fn dataset(&self) -> Result<DatasetId> {
        match self {
            Self::Initial(store) => Ok(store.manifest().identity().dataset_id),
            Self::Live(writer) => Ok(writer.manifest().identity().dataset_id),
            Self::Successor(writer) => Ok(writer.manifest().identity().dataset_id),
            Self::Candidate(_) => Err((4, "candidate has no dispatch identity".into())),
        }
    }
    pub(super) fn policy(&self, context: RuntimeContext<'_>) -> Result<PolicySnapshot> {
        context.require(self).map_err(owner_error)?;
        match self {
            Self::Initial(_) => {
                let spec = context.initial().map_err(owner_error)?;
                Ok(PolicySnapshot {
                    config_revision: spec.config_revision,
                    policy_revision: spec.policy_revision,
                    config_digest: *spec.config_digest.as_bytes(),
                    policy_digest: *spec.policy_digest.as_bytes(),
                })
            }
            Self::Live(writer) => Ok(writer.policy_snapshot()),
            Self::Successor(writer) => Ok(writer.policy_snapshot()),
            Self::Candidate(_) => Err((4, "unactivated candidate has no dispatch policy".into())),
        }
    }
    pub(super) fn limit(&self, context: RuntimeContext<'_>) -> Result<u32> {
        context.require(self).map_err(owner_error)?;
        match self {
            Self::Initial(_) => Ok(context
                .initial()
                .map_err(owner_error)?
                .outstanding_request_limit),
            Self::Live(writer) => Ok(writer.outstanding_request_limit()),
            Self::Successor(writer) => Ok(writer.outstanding_request_limit()),
            Self::Candidate(_) => Err((4, "candidate has no dispatch window".into())),
        }
    }
    pub(super) fn slot(&self, context: RuntimeContext<'_>) -> Result<ProducerSlotId> {
        context.require(self).map_err(owner_error)?;
        match self {
            Self::Initial(_) => Ok(context.initial().map_err(owner_error)?.producer_slot_id),
            Self::Live(writer) => Ok(writer.producer_slot_id()),
            Self::Successor(writer) => Ok(writer.producer_slot_id()),
            Self::Candidate(_) => Err((4, "candidate has no dispatch slot".into())),
        }
    }
    pub(super) fn epoch(&self) -> Result<u64> {
        match self {
            Self::Initial(store) => Ok(store.manifest().identity().owner_epoch.get()),
            Self::Live(writer) => Ok(writer.manifest().identity().owner_epoch.get()),
            Self::Successor(writer) => Ok(writer.manifest().identity().owner_epoch.get()),
            Self::Candidate(_) => Err((4, "candidate has no dispatch epoch".into())),
        }
    }
    pub(super) fn prepare_dictionaries<'a>(
        &'a self,
        visibility: QualifiedRef,
        limits: AdmissionLimits,
        permit: OwnedPermit,
        cancelled: &'a AtomicBool,
    ) -> std::result::Result<DictionaryPreparation<'a>, AdmissionError> {
        match self {
            Self::Initial(store) => {
                store.prepare_dictionaries(visibility, limits, permit, cancelled)
            }
            Self::Live(writer) => {
                writer.prepare_dictionaries(visibility, limits, permit, cancelled)
            }
            Self::Successor(writer) => {
                writer.prepare_dictionaries(visibility, limits, permit, cancelled)
            }
            Self::Candidate(_) => Err(AdmissionError::InvalidInput),
        }
    }
    pub(super) async fn visibility(
        &mut self,
        owner: &mut ControlSession,
        context: RuntimeContext<'_>,
        input: LiveSuccessorVisibilityRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> std::result::Result<(), OwnerError>,
    ) -> std::result::Result<Option<StoredVisibility>, InitialEventError> {
        context.require(self)?;
        match self {
            Self::Initial(store) => {
                owner
                    .read_initial_visibility(
                        store,
                        InitialVisibilityRequest {
                            registration: context.initial()?,
                            ingest_root: input.ingest_root,
                            reference: input.reference,
                            limits: input.limits,
                            policy_ttl: input.policy_ttl,
                            cancelled: input.cancelled,
                        },
                        permit,
                        validate,
                    )
                    .await
            }
            Self::Live(writer) => {
                owner
                    .read_live_initial_visibility(writer, input, permit, validate)
                    .await
            }
            Self::Successor(writer) => {
                owner
                    .read_live_successor_visibility(writer, input, permit, validate)
                    .await
            }
            Self::Candidate(_) => Err(OwnerError::InvalidInput.into()),
        }
    }
    pub(super) async fn observation(
        &mut self,
        owner: &mut ControlSession,
        context: RuntimeContext<'_>,
        input: LiveSuccessorObservationRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> std::result::Result<(), OwnerError>,
    ) -> std::result::Result<Option<StoredObservation>, InitialEventError> {
        context.require(self)?;
        match self {
            Self::Initial(store) => {
                owner
                    .read_initial_observation(
                        store,
                        InitialObservationRequest {
                            registration: context.initial()?,
                            ingest_root: input.ingest_root,
                            bindings: input.bindings,
                            limits: input.limits,
                            policy_ttl: input.policy_ttl,
                            cancelled: input.cancelled,
                        },
                        permit,
                        validate,
                    )
                    .await
            }
            Self::Live(writer) => {
                owner
                    .read_live_initial_observation(writer, input, permit, validate)
                    .await
            }
            Self::Successor(writer) => {
                owner
                    .read_live_successor_observation(writer, input, permit, validate)
                    .await
            }
            Self::Candidate(_) => Err(OwnerError::InvalidInput.into()),
        }
    }
    pub(super) async fn certify(
        &mut self,
        owner: &mut ControlSession,
        context: RuntimeContext<'_>,
        input: LiveSuccessorCertificationRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(
            &str,
            [u8; 32],
            &bigbrotr::storage::rocks::store::PreparedCertification,
        ) -> std::result::Result<(), OwnerError>,
    ) -> std::result::Result<StoredCertification, InitialEventError> {
        context.require(self)?;
        match self {
            Self::Initial(store) => {
                owner
                    .certify_initial_event(
                        store,
                        InitialCertificationRequest {
                            registration: context.initial()?,
                            ingest_root: input.ingest_root,
                            event: input.event,
                            relay_source: input.relay_source,
                            acquisition: input.acquisition,
                            provenance_enabled: input.provenance_enabled,
                            limits: input.limits,
                            policy_ttl: input.policy_ttl,
                            cancelled: input.cancelled,
                        },
                        permit,
                        validate,
                    )
                    .await
            }
            Self::Live(writer) => {
                owner
                    .certify_live_initial_event(writer, input, permit, validate)
                    .await
            }
            Self::Successor(writer) => {
                owner
                    .certify_live_successor_event(writer, input, permit, validate)
                    .await
            }
            Self::Candidate(_) => Err(OwnerError::InvalidInput.into()),
        }
    }
    pub(super) async fn append(
        &mut self,
        owner: &mut ControlSession,
        context: RuntimeContext<'_>,
        input: LiveSuccessorDictionaryAppendRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> std::result::Result<(), OwnerError>,
    ) -> std::result::Result<StoredReceipt, InitialEventError> {
        context.require(self)?;
        match self {
            Self::Initial(store) => {
                owner
                    .append_initial_dictionaries(
                        store,
                        InitialDictionaryAppendRequest {
                            registration: context.initial()?,
                            ingest_root: input.ingest_root,
                            request_sequence: input.request_sequence,
                            intent: input.intent,
                            recorded_at: input.recorded_at,
                            limits: input.limits,
                            policy_ttl: input.policy_ttl,
                            cancelled: input.cancelled,
                        },
                        permit,
                        validate,
                    )
                    .await
            }
            Self::Live(writer) => {
                owner
                    .append_live_initial_dictionaries(writer, input, permit, validate)
                    .await
            }
            Self::Successor(writer) => {
                owner
                    .append_live_successor_dictionaries(writer, input, permit, validate)
                    .await
            }
            Self::Candidate(_) => Err(OwnerError::InvalidInput.into()),
        }
    }
    pub(super) async fn admit(
        &mut self,
        owner: &mut ControlSession,
        context: RuntimeContext<'_>,
        input: LiveSuccessorEventRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(
            &str,
            [u8; 32],
            &bigbrotr::storage::rocks::store::PreparedEvent,
        ) -> std::result::Result<(), OwnerError>,
    ) -> std::result::Result<StoredOutcome, InitialEventError> {
        context.require(self)?;
        match self {
            Self::Initial(store) => {
                owner
                    .admit_initial_event(
                        store,
                        InitialEventRequest {
                            registration: context.initial()?,
                            ingest_root: input.ingest_root,
                            event: input.event,
                            relay_source: input.relay_source,
                            acquisition: input.acquisition,
                            provenance_enabled: input.provenance_enabled,
                            recorded_at: input.recorded_at,
                            limits: input.limits,
                            policy_ttl: input.policy_ttl,
                            cancelled: input.cancelled,
                        },
                        permit,
                        validate,
                    )
                    .await
            }
            Self::Live(writer) => {
                owner
                    .admit_live_initial_event(writer, input, permit, validate)
                    .await
            }
            Self::Successor(writer) => {
                owner
                    .admit_live_successor_event(writer, input, permit, validate)
                    .await
            }
            Self::Candidate(_) => Err(OwnerError::InvalidInput.into()),
        }
    }
}
