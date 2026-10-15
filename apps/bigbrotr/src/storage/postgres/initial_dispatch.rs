//! Private native dispatch. Successor work always crosses its admitted-frontier
//! wrapper; a raw IngestStore reference is retained only for the legacy paths.
use super::*;
use crate::storage::rocks::{
    manifest::IngestManifest,
    store::{ActiveSuccessorWriter, StagedProducerRequest},
};

pub(super) enum NativeOwner<'a> {
    Store(&'a mut IngestStore),
    Successor(&'a mut ActiveSuccessorWriter),
}
impl NativeOwner<'_> {
    pub(super) fn manifest(&self) -> &IngestManifest {
        match self {
            Self::Store(s) => s.manifest(),
            Self::Successor(s) => s.manifest(),
        }
    }
    pub(super) fn matches_configured_root(&self, root: &std::path::Path) -> bool {
        match self {
            Self::Store(s) => s.matches_configured_root(root),
            Self::Successor(s) => s.matches_configured_root(root),
        }
    }
    pub(super) fn stage_event(
        &mut self,
        input: &PreparedEvent,
        limits: AdmissionLimits,
        permit: &OwnedPermit,
        cancelled: &AtomicBool,
    ) -> Result<StagedProducerRequest, AdmissionError> {
        match self {
            Self::Store(s) => input.stage(s, limits, permit),
            Self::Successor(s) => s.stage_event(input, limits, permit, cancelled),
        }
    }
    pub(super) fn stage_dictionary_append(
        &mut self,
        request: ProducerRequestId,
        input: &LiveInitialDictionaryAppendRequest<'_>,
        permit: &OwnedPermit,
    ) -> Result<StagedProducerRequest, AdmissionError> {
        match self {
            Self::Store(s) => s.stage_dictionary_append(
                request,
                input.intent,
                input.recorded_at,
                input.limits,
                permit,
            ),
            Self::Successor(s) => s.stage_dictionary_append(
                request,
                input.intent,
                input.recorded_at,
                input.limits,
                permit,
                input.cancelled,
            ),
        }
    }
    pub(super) fn append_dictionaries(
        &mut self,
        request: ProducerRequestId,
        input: &LiveInitialDictionaryAppendRequest<'_>,
        permit: OwnedPermit,
        authority: impl FnMut() -> Result<(), AdmissionError>,
    ) -> Result<StoredReceipt, AdmissionError> {
        match self {
            Self::Store(s) => s.append_dictionaries(
                request,
                input.intent,
                input.recorded_at,
                input.limits,
                permit,
                authority,
            ),
            Self::Successor(s) => s.append_dictionaries(
                request,
                input.intent,
                input.recorded_at,
                input.limits,
                permit,
                input.cancelled,
                authority,
            ),
        }
    }
    pub(super) fn admit_event(
        &mut self,
        input: LiveInitialEventRequest<'_>,
        permit: OwnedPermit,
        authority: impl FnMut() -> Result<(), AdmissionError>,
    ) -> Result<StoredOutcome, AdmissionError> {
        match self {
            Self::Store(s) => s.admit_event(
                input.event,
                input.recorded_at,
                input.provenance_enabled,
                input.limits,
                permit,
                authority,
            ),
            Self::Successor(s) => s.admit_event(
                input.event,
                input.recorded_at,
                input.provenance_enabled,
                input.limits,
                permit,
                input.cancelled,
                authority,
            ),
        }
    }
    pub(super) fn certify_event(
        &mut self,
        input: LiveInitialCertificationRequest<'_>,
        permit: OwnedPermit,
        authority: impl FnMut() -> Result<(), AdmissionError>,
    ) -> Result<StoredCertification, AdmissionError> {
        match self {
            Self::Store(s) => s.certify_event(
                input.event,
                input.provenance_enabled,
                input.limits,
                permit,
                authority,
            ),
            Self::Successor(s) => s.certify_event(
                input.event,
                input.provenance_enabled,
                input.limits,
                permit,
                input.cancelled,
                authority,
            ),
        }
    }
    pub(super) fn read_visibility(
        &mut self,
        input: &LiveInitialVisibilityRequest<'_>,
        permit: OwnedPermit,
        authority: impl FnMut() -> Result<(), AdmissionError>,
    ) -> Result<Option<StoredVisibility>, AdmissionError> {
        match self {
            Self::Store(s) => s.read_visibility(input.reference, input.limits, permit, authority),
            Self::Successor(s) => s.read_visibility(
                input.reference,
                input.limits,
                permit,
                input.cancelled,
                authority,
            ),
        }
    }
    pub(super) fn read_observation(
        &mut self,
        input: LiveInitialObservationRequest<'_>,
        permit: OwnedPermit,
        authority: impl FnMut() -> Result<(), AdmissionError>,
    ) -> Result<Option<StoredObservation>, AdmissionError> {
        match self {
            Self::Store(s) => s.read_observation(input.bindings, input.limits, permit, authority),
            Self::Successor(s) => s.read_observation(
                input.bindings,
                input.limits,
                permit,
                input.cancelled,
                authority,
            ),
        }
    }
}
