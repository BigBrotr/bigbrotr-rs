//! One-way promotion after connected activation, never receipt-based adoption.
use super::{
    AdmissionError, BootstrapError, IngestRecovery, IngestStore, RetainedLineage,
    SuccessorCandidate, SuccessorHandoff,
};
use super::{
    AdmissionLimits, CertificationLimits, DictionaryPreparation, PreparedCertification,
    PreparedEvent, StagedProducerRequest, StoredCertification, StoredOutcome, StoredReceipt,
    StoredVisibility,
};
use crate::storage::rocks::{manifest::IngestManifest, succession::SuccessionRecord};
use crate::{
    acquisition::observation::{ObservationBindings, StoredObservation},
    runtime::OwnedPermit,
};
use bigbrotr_model::*;
use std::sync::atomic::AtomicBool;

/// Internal transfer envelope. PG owns its fixed intent and one-shot operation
/// state alongside this native-first value. Failed promotion retains every owned
/// field, including the original Vec and its permit, until caller teardown.
pub(in crate::storage) struct NativeSuccessorActivation {
    parts: Option<(SuccessorCandidate, RetainedLineage, SuccessorHandoff)>,
    attempted: bool,
}
impl NativeSuccessorActivation {
    pub(in crate::storage) fn new(
        candidate: SuccessorCandidate,
        lineage: RetainedLineage,
        handoff: SuccessorHandoff,
    ) -> Self {
        Self {
            parts: Some((candidate, lineage, handoff)),
            attempted: false,
        }
    }
    pub(in crate::storage) fn recovery(&self) -> Option<&IngestRecovery> {
        self.parts
            .as_ref()
            .map(|(candidate, _, _)| candidate.recovery())
    }
    pub(in crate::storage) fn require_prepared(
        &self,
        cancelled: &AtomicBool,
    ) -> Result<(), BootstrapError> {
        let (candidate, lineage, handoff) = self
            .parts
            .as_ref()
            .ok_or(BootstrapError::AuthorityUnavailable)?;
        candidate.require_prepared_handoff(lineage, handoff, cancelled)
    }
    /// Called only after the distinct activation COMMIT and the final actual
    /// Control/policy/deadline/driver checks. The final callback follows bounded
    /// native validation; the field transfer after it has no await/allocation.
    pub(in crate::storage) fn activate(
        &mut self,
        cancelled: &AtomicBool,
        mut authority: impl FnMut() -> Result<(), BootstrapError>,
    ) -> Result<ActiveSuccessorWriter, BootstrapError> {
        if self.attempted {
            return Err(BootstrapError::AuthorityUnavailable);
        }
        self.attempted = true;
        self.require_prepared(cancelled)?;
        authority()?;
        if cancelled.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(BootstrapError::AuthorityUnavailable);
        }
        let (candidate, lineage, handoff) = self.parts.take().expect("checked retained candidate");
        let record = *handoff.record();
        let (mut store, role) = candidate.into_writer_parts();
        store.live_successor =
            Some(lineage.into_admitted_frontier(&handoff, store.db.latest_sequence_number()));
        Ok(ActiveSuccessorWriter {
            store,
            record,
            role,
        })
    }
}

/// Actual exclusive native custody plus a positively checked historical prefix.
/// Mutable storage and the frontier never escape publicly. SQL authority remains
/// a separate obligation before every operation and after every await.
pub struct ActiveSuccessorWriter {
    store: IngestStore,
    record: SuccessionRecord,
    role: [u8; 16],
}
// Includes overlapping transfer headers and the input census scratch; entries
// remain in the original separately capacity-checked allocation.
const _: () = assert!(
    std::mem::size_of::<NativeSuccessorActivation>()
        + std::mem::size_of::<ActiveSuccessorWriter>()
        + std::mem::size_of::<super::lineage::AdmittedFrontier>()
        + 16 * 1024
        < crate::storage::rocks::succession::LINEAGE_SCRATCH_BYTES as usize
);
impl ActiveSuccessorWriter {
    pub fn manifest(&self) -> &IngestManifest {
        self.store.manifest()
    }
    pub fn record(&self) -> &SuccessionRecord {
        &self.record
    }
    pub fn role_instance_id(&self) -> [u8; 16] {
        self.role
    }
    pub fn producer_slot_id(&self) -> ProducerSlotId {
        self.record.fields().producer_slot_id
    }
    pub(in crate::storage) fn validate(
        &self,
        cancelled: &AtomicBool,
    ) -> Result<(), AdmissionError> {
        self.store.require_successor_operation(Some(cancelled))
    }
    pub(in crate::storage) fn matches_configured_root(&self, path: &std::path::Path) -> bool {
        self.store.matches_configured_root(path)
    }
    pub fn prepare_dictionaries<'a>(
        &'a self,
        visibility: QualifiedRef,
        limits: AdmissionLimits,
        permit: OwnedPermit,
        cancelled: &'a AtomicBool,
    ) -> Result<DictionaryPreparation<'a>, AdmissionError> {
        self.store
            .prepare_dictionaries(visibility, limits, permit, cancelled)
    }
    pub(in crate::storage) fn stage_event(
        &mut self,
        input: &PreparedEvent,
        limits: AdmissionLimits,
        permit: &OwnedPermit,
        cancelled: &AtomicBool,
    ) -> Result<StagedProducerRequest, AdmissionError> {
        input.stage_successor(&mut self.store, limits, permit, cancelled)
    }
    #[allow(
        clippy::too_many_arguments,
        reason = "explicit operation grant, cancellation and SQL authority"
    )]
    pub(in crate::storage) fn admit_event(
        &mut self,
        input: PreparedEvent,
        recorded_at: ObservedTime,
        provenance: bool,
        limits: AdmissionLimits,
        permit: OwnedPermit,
        cancelled: &AtomicBool,
        authority: impl FnMut() -> Result<(), AdmissionError>,
    ) -> Result<StoredOutcome, AdmissionError> {
        let result = self.store.admit_successor_event(
            input,
            recorded_at,
            provenance,
            limits,
            permit,
            cancelled,
            authority,
        );
        let validation = self.validate(cancelled);
        let result = result?;
        validation?;
        Ok(result)
    }
    #[allow(
        clippy::too_many_arguments,
        reason = "exact staged input and cancellation"
    )]
    pub(in crate::storage) fn stage_dictionary_append(
        &mut self,
        request: ProducerRequestId,
        intent: &EvidenceIntent,
        recorded_at: ObservedTime,
        limits: AdmissionLimits,
        permit: &OwnedPermit,
        cancelled: &AtomicBool,
    ) -> Result<StagedProducerRequest, AdmissionError> {
        self.store.stage_successor_dictionaries(
            request,
            intent,
            recorded_at,
            limits,
            permit,
            cancelled,
        )
    }
    #[allow(
        clippy::too_many_arguments,
        reason = "explicit operation grant, cancellation and SQL authority"
    )]
    pub(in crate::storage) fn append_dictionaries(
        &mut self,
        request: ProducerRequestId,
        intent: &EvidenceIntent,
        recorded_at: ObservedTime,
        limits: AdmissionLimits,
        permit: OwnedPermit,
        cancelled: &AtomicBool,
        authority: impl FnMut() -> Result<(), AdmissionError>,
    ) -> Result<StoredReceipt, AdmissionError> {
        let result = self.store.append_successor_dictionaries(
            request,
            intent,
            recorded_at,
            limits,
            permit,
            cancelled,
            authority,
        );
        let validation = self.validate(cancelled);
        let result = result?;
        validation?;
        Ok(result)
    }
    #[allow(
        clippy::too_many_arguments,
        reason = "read authority and retained input/result grants"
    )]
    pub(in crate::storage) fn certify_event(
        &mut self,
        input: PreparedCertification,
        provenance: bool,
        limits: CertificationLimits,
        permit: OwnedPermit,
        cancelled: &AtomicBool,
        authority: impl FnMut() -> Result<(), AdmissionError>,
    ) -> Result<StoredCertification, AdmissionError> {
        self.validate(cancelled)?;
        let result = self
            .store
            .certify_event(input, provenance, limits, permit, authority);
        let validation = self.validate(cancelled);
        let result = result?;
        validation?;
        Ok(result)
    }
    pub(in crate::storage) fn read_visibility(
        &mut self,
        reference: QualifiedRef,
        limits: CertificationLimits,
        permit: OwnedPermit,
        cancelled: &AtomicBool,
        authority: impl FnMut() -> Result<(), AdmissionError>,
    ) -> Result<Option<StoredVisibility>, AdmissionError> {
        self.validate(cancelled)?;
        let result = self
            .store
            .read_visibility(reference, limits, permit, authority);
        let validation = self.validate(cancelled);
        let result = result?;
        validation?;
        Ok(result)
    }
    pub(in crate::storage) fn read_observation(
        &mut self,
        input: ObservationBindings<'_>,
        limits: CertificationLimits,
        permit: OwnedPermit,
        cancelled: &AtomicBool,
        authority: impl FnMut() -> Result<(), AdmissionError>,
    ) -> Result<Option<StoredObservation>, AdmissionError> {
        self.validate(cancelled)?;
        let result = self
            .store
            .read_observation(input, limits, permit, authority);
        let validation = self.validate(cancelled);
        let result = result?;
        validation?;
        Ok(result)
    }
    #[cfg(test)]
    pub(super) fn store(&self) -> &IngestStore {
        &self.store
    }
}

impl IngestStore {
    /// Shared kernels keep their old entrypoints. A later writer must select the
    /// cancellation-aware branch; an ordinary reopened store cannot select it.
    pub(super) fn require_successor_operation(
        &self,
        cancelled: Option<&AtomicBool>,
    ) -> Result<(), AdmissionError> {
        match (&self.live_successor, cancelled) {
            (Some(frontier), Some(cancelled)) => frontier.validate(self, cancelled),
            (Some(_), None) | (None, Some(_)) => Err(AdmissionError::InvalidInput),
            (None, None) => Ok(()),
        }
    }
    pub(super) fn poison_successor(&self) {
        if let Some(frontier) = &self.live_successor {
            frontier.poison();
        }
    }
    pub(super) fn retain_admission_input(
        &mut self,
        request: bigbrotr_model::ProducerRequestId,
        bytes: &[u8],
        cancelled: Option<&AtomicBool>,
    ) -> Result<(), AdmissionError> {
        let Some(frontier) = &self.live_successor else {
            if cancelled.is_some() {
                return Err(AdmissionError::InvalidInput);
            }
            return self
                .retain_request_input(request, bytes)
                .map_err(AdmissionError::RequestInput);
        };
        let cancelled = cancelled.ok_or(AdmissionError::InvalidInput)?;
        let added = frontier.staging(self, request, cancelled)?;
        let result = (|| {
            self.retain_request_input(request, bytes)
                .map_err(AdmissionError::RequestInput)?;
            let stamp = self
                .live_successor
                .as_ref()
                .expect("continuous writer")
                .staged_stamp(self, request, bytes, added, cancelled)?;
            self.live_successor
                .as_mut()
                .expect("continuous writer")
                .record_staging(request, stamp);
            self.require_successor_operation(Some(cancelled))
        })();
        if result.is_err() {
            self.poison_successor();
        }
        result
    }

    pub(super) fn commit_admission(
        &mut self,
        batch: super::bootstrap::Prepared,
        request: bigbrotr_model::ProducerRequestId,
        source: Option<u64>,
        cancelled: Option<&AtomicBool>,
        authority: impl FnMut() -> Result<(), BootstrapError>,
    ) -> Result<(), AdmissionError> {
        let Some(frontier) = &self.live_successor else {
            if cancelled.is_some() {
                return Err(AdmissionError::InvalidInput);
            }
            return self.commit_prepared(batch, authority).map_err(Into::into);
        };
        let cancelled = cancelled.ok_or(AdmissionError::InvalidInput)?;
        let next = frontier.prepare_commit(self, request, source, batch.put_count(), cancelled)?;
        let result = (|| {
            self.commit_prepared(batch, authority)?;
            self.live_successor
                .as_mut()
                .expect("continuous writer")
                .committed(next.0, next.1);
            self.require_successor_operation(Some(cancelled))
        })();
        if result.is_err() {
            self.poison_successor();
        }
        result
    }
}
