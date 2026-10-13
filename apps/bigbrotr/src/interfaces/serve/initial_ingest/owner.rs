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
use bigbrotr_model::QualifiedRef;

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

pub(super) enum NativeOwner {
    Initial(Box<IngestStore>),
    Candidate(Box<InitialWriterCandidate>),
    Live(Box<LiveInitialWriter>),
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
            Self::Live(_) => Ok(Profile::Successor),
            Self::Candidate(_) => Err((4, "unactivated candidate cannot dispatch".into())),
        }
    }
    pub(super) fn policy(&self, spec: &InitialRegistration) -> Result<PolicySnapshot> {
        match self {
            Self::Initial(_) => Ok(PolicySnapshot {
                config_revision: spec.config_revision,
                policy_revision: spec.policy_revision,
                config_digest: *spec.config_digest.as_bytes(),
                policy_digest: *spec.policy_digest.as_bytes(),
            }),
            Self::Live(writer) => Ok(writer.policy_snapshot()),
            Self::Candidate(_) => Err((4, "unactivated candidate has no dispatch policy".into())),
        }
    }
    pub(super) fn limit(&self, spec: &InitialRegistration) -> u32 {
        match self {
            Self::Initial(_) => spec.outstanding_request_limit,
            Self::Live(writer) => writer.outstanding_request_limit(),
            Self::Candidate(_) => 0,
        }
    }
    pub(super) fn epoch(&self) -> u64 {
        match self {
            Self::Initial(_) => 1,
            Self::Live(_) => 2,
            Self::Candidate(_) => 0,
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
            Self::Candidate(_) => Err(AdmissionError::InvalidInput),
        }
    }
    pub(super) async fn visibility(
        &mut self,
        owner: &mut ControlSession,
        input: InitialVisibilityRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> std::result::Result<(), OwnerError>,
    ) -> std::result::Result<Option<StoredVisibility>, InitialEventError> {
        match self {
            Self::Initial(store) => {
                owner
                    .read_initial_visibility(store, input, permit, validate)
                    .await
            }
            Self::Live(writer) => {
                owner
                    .read_live_initial_visibility(
                        writer,
                        LiveInitialVisibilityRequest {
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
            Self::Candidate(_) => Err(OwnerError::InvalidInput.into()),
        }
    }
    pub(super) async fn observation(
        &mut self,
        owner: &mut ControlSession,
        input: InitialObservationRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> std::result::Result<(), OwnerError>,
    ) -> std::result::Result<Option<StoredObservation>, InitialEventError> {
        match self {
            Self::Initial(store) => {
                owner
                    .read_initial_observation(store, input, permit, validate)
                    .await
            }
            Self::Live(writer) => {
                owner
                    .read_live_initial_observation(
                        writer,
                        LiveInitialObservationRequest {
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
            Self::Candidate(_) => Err(OwnerError::InvalidInput.into()),
        }
    }
    pub(super) async fn certify(
        &mut self,
        owner: &mut ControlSession,
        input: InitialCertificationRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(
            &str,
            [u8; 32],
            &bigbrotr::storage::rocks::store::PreparedCertification,
        ) -> std::result::Result<(), OwnerError>,
    ) -> std::result::Result<StoredCertification, InitialEventError> {
        match self {
            Self::Initial(store) => {
                owner
                    .certify_initial_event(store, input, permit, validate)
                    .await
            }
            Self::Live(writer) => {
                owner
                    .certify_live_initial_event(
                        writer,
                        LiveInitialCertificationRequest {
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
            Self::Candidate(_) => Err(OwnerError::InvalidInput.into()),
        }
    }
    pub(super) async fn append(
        &mut self,
        owner: &mut ControlSession,
        input: InitialDictionaryAppendRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> std::result::Result<(), OwnerError>,
    ) -> std::result::Result<StoredReceipt, InitialEventError> {
        match self {
            Self::Initial(store) => {
                owner
                    .append_initial_dictionaries(store, input, permit, validate)
                    .await
            }
            Self::Live(writer) => {
                owner
                    .append_live_initial_dictionaries(
                        writer,
                        LiveInitialDictionaryAppendRequest {
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
            Self::Candidate(_) => Err(OwnerError::InvalidInput.into()),
        }
    }
    pub(super) async fn admit(
        &mut self,
        owner: &mut ControlSession,
        input: InitialEventRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(
            &str,
            [u8; 32],
            &bigbrotr::storage::rocks::store::PreparedEvent,
        ) -> std::result::Result<(), OwnerError>,
    ) -> std::result::Result<StoredOutcome, InitialEventError> {
        match self {
            Self::Initial(store) => {
                owner
                    .admit_initial_event(store, input, permit, validate)
                    .await
            }
            Self::Live(writer) => {
                owner
                    .admit_live_initial_event(
                        writer,
                        LiveInitialEventRequest {
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
            Self::Candidate(_) => Err(OwnerError::InvalidInput.into()),
        }
    }
}
