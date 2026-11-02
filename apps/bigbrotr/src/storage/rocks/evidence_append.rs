//! Shared serialized staging/commit path for the two concrete AdmitEvidence inputs.
use super::{
    AdmissionError, AdmissionLimits, BootstrapError, IngestStore, StoredReceipt,
    bootstrap::Prepared, producer_closing::ProducerState, producer_recovery::ReplayContext,
};
use crate::{runtime::OwnedPermit, storage::rocks::manifest::IngestManifest};
use bigbrotr_model::*;
use std::sync::atomic::AtomicBool;

#[derive(Clone, Copy)]
pub(super) enum EvidenceInput<'a> {
    Dictionaries(&'a EvidenceIntent),
    Measurement(&'a MeasurementEvidenceIntent),
}
pub(super) enum EvidenceHead {
    Replayed(Receipt),
    Fresh(ProducerState),
}
impl EvidenceInput<'_> {
    pub(super) fn digest(self, request: ProducerRequestId) -> Digest256 {
        match self {
            Self::Dictionaries(value) => value.request_digest(request),
            Self::Measurement(value) => value.request_digest(request),
        }
    }
    fn encode(self, request: ProducerRequestId, limit: usize) -> Result<Vec<u8>, AdmissionError> {
        match self {
            Self::Dictionaries(value) => value.encode_request_preimage(request, limit),
            Self::Measurement(value) => value.encode_request_preimage(request, limit),
        }
        .map_err(|error| match error {
            EvidenceIntentError::AllocationFailed => AdmissionError::Allocation,
            _ => AdmissionError::Limit,
        })
    }
    fn prepare(
        self,
        store: &IngestStore,
        request: ProducerRequestId,
        recorded_at: ObservedTime,
        limits: AdmissionLimits,
    ) -> Result<(Option<Prepared>, Receipt), AdmissionError> {
        match self {
            Self::Dictionaries(value) => {
                store.prepare_dictionary_operation(request, value, recorded_at, limits)
            }
            Self::Measurement(value) => {
                store.prepare_measurement_operation(request, value, recorded_at, limits)
            }
        }
    }
    fn validate_and_replay(
        self,
        store: &IngestStore,
        request: ProducerRequestId,
        limits: AdmissionLimits,
    ) -> Result<Option<Receipt>, AdmissionError> {
        let context = ReplayContext {
            manifest: &store.manifest,
            current_manifest: &store.manifest,
        };
        match self {
            Self::Dictionaries(value) => store.replay_dictionary_append(request, value, limits),
            Self::Measurement(value) => {
                store.replay_measurement_append_in(request, value, limits, context, None)
            }
        }
    }
    fn validate(
        self,
        store: &IngestStore,
        request: ProducerRequestId,
        limits: AdmissionLimits,
    ) -> Result<(), AdmissionError> {
        match self {
            Self::Dictionaries(value) => store.validate_dictionary_append_context(
                request,
                value,
                limits,
                &store.manifest,
                None,
            ),
            Self::Measurement(value) => store.validate_measurement_append_context(
                request,
                value,
                limits,
                &store.manifest,
                None,
            ),
        }
    }
}

impl IngestStore {
    pub(super) fn evidence_pending_reservation(
        &self,
        request: ProducerRequestId,
        input: EvidenceInput<'_>,
        recorded_at: ObservedTime,
        limits: AdmissionLimits,
        permit: &OwnedPermit,
    ) -> Result<super::PendingReservationRequest, AdmissionError> {
        require_permit(permit, limits)?;
        let (writes, _) = input.prepare(self, request, recorded_at, limits)?;
        let required_growth = if let Some(mut writes) = writes {
            self.seal_pending_accounting(&mut writes)?;
            self.prepared_pending_delta(&writes)?
        } else {
            0
        };
        self.pending_input_reservation(
            request,
            input.digest(request),
            limits.max_batch_bytes as u64,
            required_growth,
        )
        .map_err(Into::into)
    }
    #[allow(
        clippy::too_many_arguments,
        reason = "same staging kernel with explicit successor cancellation"
    )]
    pub(super) fn stage_evidence_append_in(
        &mut self,
        request: ProducerRequestId,
        input: EvidenceInput<'_>,
        recorded_at: ObservedTime,
        limits: AdmissionLimits,
        permit: &OwnedPermit,
        cancelled: Option<&AtomicBool>,
    ) -> Result<super::StagedProducerRequest, AdmissionError> {
        self.require_successor_operation(cancelled)?;
        require_permit(permit, limits)?;
        self.require_request_capacity(request, limits.max_outstanding_requests)?;
        let reservation =
            self.evidence_pending_reservation(request, input, recorded_at, limits, permit)?;
        self.reserve_pending_requests(&[reservation])
            .map_err(AdmissionError::PendingQuota)?;
        let bytes = input.encode(request, limits.max_batch_bytes)?;
        self.stage_admission_input(request, input.digest(request), &bytes, cancelled)
    }
    #[allow(
        clippy::too_many_arguments,
        reason = "serialized commit retains authority and owned operation grant"
    )]
    pub(super) fn append_evidence_in(
        &mut self,
        request: ProducerRequestId,
        input: EvidenceInput<'_>,
        recorded_at: ObservedTime,
        limits: AdmissionLimits,
        permit: OwnedPermit,
        cancelled: Option<&AtomicBool>,
        mut authority: impl FnMut() -> Result<(), AdmissionError>,
    ) -> Result<StoredReceipt, AdmissionError> {
        self.require_successor_operation(cancelled)?;
        require_permit(&permit, limits)?;
        authority()?;
        let (writes, receipt) = input.prepare(self, request, recorded_at, limits)?;
        let copy = super::NativeOutcomeCopy::receipt(&receipt, &self.manifest)?;
        authority()?;
        if let Some(writes) = writes {
            let delta = self.prepared_pending_delta(&writes)?;
            let reservation = self.pending_input_reservation(
                request,
                input.digest(request),
                limits.max_batch_bytes as u64,
                delta,
            )?;
            self.reserve_pending_requests(&[reservation])
                .map_err(AdmissionError::PendingQuota)?;
            let bytes = input.encode(request, limits.max_batch_bytes)?;
            self.retain_admission_input(request, &bytes, cancelled)?;
            drop(bytes);
            self.commit_admission(
                writes,
                request,
                receipt.batch().map(|batch| batch.last().sequence.get()),
                cancelled,
                || authority().map_err(|_| BootstrapError::AuthorityUnavailable),
            )?;
        } else {
            self.settle_pending_replay(request, input.digest(request))
                .map_err(AdmissionError::PendingQuota)?;
        }
        if self.native_unavailable() {
            return Err(AdmissionError::CommitUnknown);
        }
        Ok(StoredReceipt::new(receipt, copy, permit))
    }

    /// Original-request replay precedes any lookup of current scientific indexes.
    /// A new request still advances the producer even when science is already durable.
    pub(super) fn prepare_evidence_head(
        &self,
        request: ProducerRequestId,
        input: EvidenceInput<'_>,
        limits: AdmissionLimits,
    ) -> Result<EvidenceHead, AdmissionError> {
        self.require_admission_owner(Some(request))?;
        self.require_unretired_request(request, &self.manifest)?;
        input.validate(self, request, limits)?;
        let state = self
            .producer_state(request.producer_slot_id, request.producer_epoch)?
            .ok_or(AdmissionError::StateMismatch)?;
        if state.closing {
            return Err(AdmissionError::ProducerClosing);
        }
        let sequence = request.request_sequence.get();
        self.require_retained_state(request.producer_slot_id, request.producer_epoch, state)?;
        if sequence > super::RETAINED_PRODUCER_REQUEST_LIMIT
            || sequence
                .checked_sub(state.retired)
                .is_none_or(|tail| tail > super::RETAINED_PRODUCER_REQUEST_LIMIT)
        {
            return Err(AdmissionError::Limit);
        }
        if sequence <= state.retired {
            return Err(AdmissionError::RetiredRequest);
        }
        if let Some(receipt) = input.validate_and_replay(self, request, limits)? {
            if sequence > state.issued {
                return Err(AdmissionError::StateMismatch);
            }
            return Ok(EvidenceHead::Replayed(receipt));
        }
        if sequence <= state.issued {
            return Err(AdmissionError::StateMismatch);
        }
        if state.issued.checked_add(1) != Some(sequence) {
            return Err(AdmissionError::RequestGap);
        }
        if state.issued - state.acknowledged >= limits.max_outstanding_requests {
            return Err(AdmissionError::WindowFull);
        }
        Ok(EvidenceHead::Fresh(state))
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "exact original metadata and historical lineage interpretation"
    )]
    pub(super) fn validate_evidence_context(
        &self,
        request: ProducerRequestId,
        policy: u16,
        scope: QualifiedRef,
        limits: AdmissionLimits,
        manifest: &IngestManifest,
        lineage: Option<&super::RetainedLineage>,
    ) -> Result<(), AdmissionError> {
        limits.required_dictionary_data_bytes()?;
        if self.native_unavailable() {
            return Err(AdmissionError::CommitUnknown);
        }
        let identity = manifest.identity();
        let supported = match identity.owner_epoch.get() {
            1 => request.producer_epoch.get() == 1 && request.request_sequence.get() > 1,
            2 => manifest.key_schema() == 2 && request.producer_epoch.get() == 2,
            epoch => {
                manifest.key_schema() == 3
                    && request.producer_epoch.get() == epoch
                    && (lineage.is_some_and(|lineage| {
                        lineage.native_head().manifest() == &self.manifest
                            && lineage
                                .entry(epoch)
                                .is_some_and(|entry| entry.manifest() == manifest)
                    }) || (manifest == &self.manifest
                        && self
                            .live_successor
                            .as_ref()
                            .is_some_and(|frontier| frontier.manifest() == manifest)))
            }
        };
        if !supported || policy != 1 || scope.dataset_id != identity.dataset_id {
            return Err(AdmissionError::InvalidInput);
        }
        Ok(())
    }
}
fn require_permit(permit: &OwnedPermit, limits: AdmissionLimits) -> Result<(), AdmissionError> {
    if permit.request().units().data_bytes() < limits.required_dictionary_data_bytes()?
        || permit.request().units().native_bytes() < limits.required_native_bytes()?
        || permit.request().units().items() == 0
    {
        return Err(AdmissionError::InsufficientReservation);
    }
    Ok(())
}
