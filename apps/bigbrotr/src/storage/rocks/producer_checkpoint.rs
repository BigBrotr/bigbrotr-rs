//! Opaque current-owner evidence for a downstream producer checkpoint.
//! No constructor accepts a decoded proof, PG outcome row or caller digest as
//! native authority. This module does not write ACK or reclaim request detail.
use super::{
    AdmissionError, AdmissionLimits, AdmissionOutcome, BootstrapError, BootstrapLimits,
    CertificationOutcome, IngestRecovery, IngestStore, NativeOutcomeCopy, NativeOutcomeKind,
    PreparedEvent, ResolvedProducerRequest, StoredCertification, StoredOutcome, StoredReceipt,
};
use crate::runtime::OwnedPermit;
use crate::storage::rocks::keys::IngestKey;
use bigbrotr_model::{
    Digest256, EvidenceIntent, FactRef, MeasurementEvidenceIntent, ObservedTime, ProducerRequestId,
};
use sha2::{Digest, Sha256};
use std::sync::atomic::AtomicBool;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct CheckpointStamp {
    root: (u64, u64),
    sequence: u64,
    manifest: [u8; 32],
}
impl CheckpointStamp {
    pub(super) fn capture(store: &IngestStore) -> Result<Self, AdmissionError> {
        if store.native_unavailable() {
            return Err(AdmissionError::CommitUnknown);
        }
        store.require_admission_owner(None)?;
        let stamp = Self {
            root: store.handoff_root_identity()?,
            sequence: store.db.latest_sequence_number(),
            manifest: Sha256::digest(store.manifest.encode().as_bytes()).into(),
        };
        if store.native_unavailable() {
            return Err(AdmissionError::CommitUnknown);
        }
        Ok(stamp)
    }

    pub(super) fn require(self, store: &IngestStore) -> Result<(), AdmissionError> {
        if Self::capture(store)? != self {
            return Err(AdmissionError::StateMismatch);
        }
        Ok(())
    }
}

/// Captured only by the opted-in serialized certification kernel. Retaining a
/// structural ExistingProof cannot reconstruct this same-owner observation.
#[derive(Clone, Copy)]
pub(super) struct CertificationCheckpoint {
    pub(super) stamp: CheckpointStamp,
    pub(super) intent: Digest256,
    pub(super) captured: Option<crate::acquisition::preparation::CapturedBinding>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i16)]
pub(in crate::storage) enum CheckpointKind {
    InternalEvidence = 1,
    EventNewFacts = 2,
    EventAlreadyDurable = 3,
    EventRejected = 4,
    ExistingProof = 5,
    CertificationRejected = 6,
}

/// The borrowed original result keeps its native handoff permit alive. The
/// connected adapter owns current SQL authority and validates this stamp again
/// before committing progress. It cannot clone or deserialize this capability.
pub(in crate::storage) struct NativeCheckpointBasis<'a> {
    stamp: CheckpointStamp,
    kind: CheckpointKind,
    intent: Digest256,
    request: Option<&'a NativeOutcomeCopy>,
    dependencies: &'a [FactRef],
    captured: Option<crate::acquisition::preparation::CapturedBinding>,
}
impl NativeCheckpointBasis<'_> {
    fn require_live_observation(&self) -> Result<(), AdmissionError> {
        // Historical replay retains exact outcome bytes without the serialized
        // live operation's checkpoint stamp. Equal coordinates cannot promote
        // that replay into live checkpoint or intermediate ACK authority.
        if self
            .request
            .is_some_and(|copy| copy.checkpoint() != Some(self.stamp))
        {
            return Err(AdmissionError::InvalidInput);
        }
        Ok(())
    }

    pub(in crate::storage) fn acknowledgement_target(
        &self,
        slot: bigbrotr_model::ProducerSlotId,
        epoch: bigbrotr_model::ProducerEpoch,
        complete: u64,
    ) -> Result<super::producer_ack::AckTarget, AdmissionError> {
        self.require_live_observation()?;
        let request = self.request.ok_or(AdmissionError::InvalidInput)?.request();
        if request.producer_slot_id != slot
            || request.producer_epoch != epoch
            || request.request_sequence.get() > complete
        {
            return Err(AdmissionError::InvalidInput);
        }
        Ok(super::producer_ack::AckTarget::new(
            self.stamp, slot, epoch, complete,
        ))
    }
    pub(in crate::storage) fn kind(&self) -> CheckpointKind {
        self.kind
    }
    pub(in crate::storage) fn intent_digest(&self) -> Digest256 {
        self.intent
    }
    pub(in crate::storage) fn request(&self) -> Option<ProducerRequestId> {
        self.request.map(NativeOutcomeCopy::request)
    }
    pub(in crate::storage) fn dependencies(&self) -> &[FactRef] {
        self.dependencies
    }
    pub(in crate::storage) fn manifest_sha256(&self) -> [u8; 32] {
        self.stamp.manifest
    }
    pub(in crate::storage) fn captured(
        &self,
    ) -> Option<crate::acquisition::preparation::CapturedBinding> {
        self.captured
    }
}

/// Historical checkpoint evidence borrows both exclusive recovery custody and
/// the actual original replay. No live admission stamp or intermediate ACK is
/// issued; the complete replay prefix still owns final ACK confirmation.
pub(in crate::storage) struct HistoricalMeasurementCheckpoint<'a> {
    recovery: &'a IngestRecovery,
    resolved: &'a ResolvedProducerRequest,
    basis: NativeCheckpointBasis<'a>,
}
impl HistoricalMeasurementCheckpoint<'_> {
    pub(in crate::storage) fn require(&self) -> Result<(), AdmissionError> {
        self.resolved
            .require_native_context(self.recovery)
            .map_err(AdmissionError::from)?;
        let manifest: [u8; 32] =
            Sha256::digest(self.recovery.manifest().encode().as_bytes()).into();
        if manifest != self.basis.stamp.manifest
            || self.recovery.store.handoff_root_identity()? != self.basis.stamp.root
            || self.recovery.store.db.latest_sequence_number() != self.basis.stamp.sequence
        {
            return Err(AdmissionError::StateMismatch);
        }
        Ok(())
    }
    pub(in crate::storage) fn basis(&self) -> &NativeCheckpointBasis<'_> {
        &self.basis
    }
    pub(in crate::storage) fn original(&self) -> &ResolvedProducerRequest {
        self.resolved
    }
}
impl IngestRecovery {
    pub(in crate::storage) fn measurement_checkpoint<'a>(
        &'a self,
        resolved: &'a ResolvedProducerRequest,
    ) -> Result<HistoricalMeasurementCheckpoint<'a>, AdmissionError> {
        resolved
            .require_native_context(self)
            .map_err(AdmissionError::from)?;
        let copy = resolved.native_outcome();
        let request = copy.request();
        let manifest: [u8; 32] = Sha256::digest(self.manifest().encode().as_bytes()).into();
        if resolved.measurement().is_none()
            || self.manifest().key_schema() != 1
            || self.manifest().identity().owner_epoch.get() != 1
            || request.producer_epoch.get() != 1
            || copy.manifest_sha256() != manifest
            || copy.dataset() != self.manifest().identity().dataset_id
            || resolved.input_bytes() == 0
        {
            return Err(AdmissionError::InvalidInput);
        }
        self.store
            .require_unretired_request(request, self.manifest())?;
        let state = self
            .store
            .producer_state(request.producer_slot_id, request.producer_epoch)?
            .ok_or(AdmissionError::StateMismatch)?;
        if !state.closing || state.retired != 0 || request.request_sequence.get() > state.issued {
            return Err(AdmissionError::StateMismatch);
        }
        let AdmissionOutcome::Durable(receipt) = resolved.outcome() else {
            return Err(AdmissionError::InvalidInput);
        };
        let value = HistoricalMeasurementCheckpoint {
            recovery: self,
            resolved,
            basis: NativeCheckpointBasis {
                stamp: CheckpointStamp {
                    root: self.store.handoff_root_identity()?,
                    sequence: self.store.db.latest_sequence_number(),
                    manifest,
                },
                kind: CheckpointKind::InternalEvidence,
                intent: copy.digest(),
                request: Some(copy),
                dependencies: receipt.dependencies().records(),
                captured: None,
            },
        };
        value.require()?;
        Ok(value)
    }
}

impl IngestStore {
    /// These wrappers alone bind a mutation result to the actual serialized
    /// operation. Equal receipt bytes supplied from another root cannot acquire
    /// this stamp; the ordinary operations/readers do not mint it.
    #[allow(
        clippy::too_many_arguments,
        reason = "unchanged native operation inputs and explicit successor cancellation"
    )]
    pub(in crate::storage) fn admit_checkpoint_event(
        &mut self,
        input: PreparedEvent,
        recorded_at: ObservedTime,
        provenance: bool,
        limits: AdmissionLimits,
        permit: OwnedPermit,
        cancelled: Option<&AtomicBool>,
        mut authority: impl FnMut() -> Result<(), AdmissionError>,
    ) -> Result<StoredOutcome, AdmissionError> {
        let captured = input.captured();
        let result = match cancelled {
            Some(cancelled) => self.admit_successor_event(
                input,
                recorded_at,
                provenance,
                limits,
                permit,
                cancelled,
                &mut authority,
            ),
            None => self.admit_event(
                input,
                recorded_at,
                provenance,
                limits,
                permit,
                &mut authority,
            ),
        }?;
        let stamp = CheckpointStamp::capture(self)?;
        authority()?;
        if self.native_unavailable() {
            return Err(AdmissionError::CommitUnknown);
        }
        Ok(result.bind_checkpoint(stamp, captured))
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "unchanged native operation inputs and explicit successor cancellation"
    )]
    pub(in crate::storage) fn append_checkpoint_dictionaries(
        &mut self,
        request: ProducerRequestId,
        intent: &EvidenceIntent,
        recorded_at: ObservedTime,
        limits: AdmissionLimits,
        permit: OwnedPermit,
        cancelled: Option<&AtomicBool>,
        mut authority: impl FnMut() -> Result<(), AdmissionError>,
    ) -> Result<StoredReceipt, AdmissionError> {
        let result = match cancelled {
            Some(cancelled) => self.append_successor_dictionaries(
                request,
                intent,
                recorded_at,
                limits,
                permit,
                cancelled,
                &mut authority,
            ),
            None => self.append_dictionaries(
                request,
                intent,
                recorded_at,
                limits,
                permit,
                &mut authority,
            ),
        }?;
        let stamp = CheckpointStamp::capture(self)?;
        authority()?;
        if self.native_unavailable() {
            return Err(AdmissionError::CommitUnknown);
        }
        Ok(result.bind_checkpoint(stamp))
    }

    /// Only the actual serialized measurement append/replay can mint this
    /// current-root checkpoint witness. Ordinary receipt reads cannot ACK.
    #[allow(
        clippy::too_many_arguments,
        reason = "exact measurement input, retained operation grant and authority"
    )]
    pub(in crate::storage) fn append_checkpoint_measurement(
        &mut self,
        request: ProducerRequestId,
        intent: &MeasurementEvidenceIntent,
        recorded_at: ObservedTime,
        limits: AdmissionLimits,
        permit: OwnedPermit,
        mut authority: impl FnMut() -> Result<(), AdmissionError>,
    ) -> Result<StoredReceipt, AdmissionError> {
        let result = self.append_measurement(
            request,
            intent,
            recorded_at,
            limits,
            permit,
            None,
            &mut authority,
        )?;
        let stamp = CheckpointStamp::capture(self)?;
        authority()?;
        if self.native_unavailable() {
            return Err(AdmissionError::CommitUnknown);
        }
        Ok(result.bind_checkpoint(stamp))
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "unchanged initial native operation inputs"
    )]
    pub(in crate::storage) fn bootstrap_checkpoint_dictionaries(
        &mut self,
        request: ProducerRequestId,
        intent: &EvidenceIntent,
        recorded_at: ObservedTime,
        limits: BootstrapLimits,
        permit: OwnedPermit,
        mut authority: impl FnMut() -> Result<(), BootstrapError>,
    ) -> Result<StoredReceipt, BootstrapError> {
        let result = self.bootstrap_dictionaries(
            request,
            intent,
            recorded_at,
            limits,
            permit,
            &mut authority,
        )?;
        let stamp = CheckpointStamp::capture(self).map_err(|error| match error {
            AdmissionError::CommitUnknown => BootstrapError::CommitUnknown,
            _ => BootstrapError::StateMismatch,
        })?;
        authority()?;
        if self.native_unavailable() {
            return Err(BootstrapError::CommitUnknown);
        }
        Ok(result.bind_checkpoint(stamp))
    }

    pub(in crate::storage) fn checkpoint_certification<'a>(
        &self,
        result: &'a StoredCertification,
    ) -> Result<Option<NativeCheckpointBasis<'a>>, AdmissionError> {
        let observation = result.checkpoint().ok_or(AdmissionError::InvalidInput)?;
        observation.stamp.require(self)?;
        let (kind, dependencies) = match result.outcome() {
            CertificationOutcome::Existing(proof) => {
                if proof.intent_digest() != observation.intent {
                    return Err(AdmissionError::StateMismatch);
                }
                (
                    CheckpointKind::ExistingProof,
                    proof.dependencies().records(),
                )
            }
            CertificationOutcome::Rejected(_) => (CheckpointKind::CertificationRejected, &[][..]),
            CertificationOutcome::NeedsAdmission => return Ok(None),
        };
        Ok(Some(NativeCheckpointBasis {
            stamp: observation.stamp,
            kind,
            intent: observation.intent,
            request: None,
            dependencies,
            captured: observation.captured,
        }))
    }

    pub(in crate::storage) fn checkpoint_event<'a>(
        &self,
        result: &'a StoredOutcome,
    ) -> Result<NativeCheckpointBasis<'a>, AdmissionError> {
        let copy = result.native_outcome();
        let stamp = self.checkpoint_outcome(copy)?;
        let (kind, dependencies) = match result.outcome() {
            AdmissionOutcome::Durable(receipt) => (
                if receipt.batch().is_some() {
                    CheckpointKind::EventNewFacts
                } else {
                    CheckpointKind::EventAlreadyDurable
                },
                receipt.dependencies().records(),
            ),
            AdmissionOutcome::Rejected(_) => (CheckpointKind::EventRejected, &[][..]),
        };
        Ok(NativeCheckpointBasis {
            stamp,
            kind,
            intent: copy.digest(),
            request: Some(copy),
            dependencies,
            captured: copy.captured(),
        })
    }

    pub(in crate::storage) fn checkpoint_evidence<'a>(
        &self,
        result: &'a StoredReceipt,
    ) -> Result<NativeCheckpointBasis<'a>, AdmissionError> {
        // read_receipt deliberately returns no native handoff witness.
        let copy = result
            .native_outcome()
            .ok_or(AdmissionError::InvalidInput)?;
        let stamp = self.checkpoint_outcome(copy)?;
        if copy.kind() == NativeOutcomeKind::Rejected {
            return Err(AdmissionError::StateMismatch);
        }
        Ok(NativeCheckpointBasis {
            stamp,
            kind: CheckpointKind::InternalEvidence,
            intent: copy.digest(),
            request: Some(copy),
            dependencies: result.receipt().dependencies().records(),
            captured: None,
        })
    }

    pub(in crate::storage) fn require_checkpoint_basis(
        &self,
        basis: &NativeCheckpointBasis<'_>,
    ) -> Result<(), AdmissionError> {
        basis.require_live_observation()?;
        basis.stamp.require(self)
    }

    fn checkpoint_outcome(
        &self,
        outcome: &NativeOutcomeCopy,
    ) -> Result<CheckpointStamp, AdmissionError> {
        let stamp = outcome.checkpoint().ok_or(AdmissionError::InvalidInput)?;
        stamp.require(self)?;
        let request = outcome.request();
        if outcome.manifest_sha256() != stamp.manifest
            || outcome.dataset() != self.manifest.identity().dataset_id
            || request.producer_epoch.get() != self.manifest.identity().owner_epoch.get()
        {
            return Err(AdmissionError::InvalidInput);
        }
        self.require_admission_owner(Some(request))?;
        let key = IngestKey::Request {
            request_id: request,
        }
        .encode()
        .map_err(|_| AdmissionError::InvalidInput)?;
        let cf = self
            .db
            .cf_handle("receipts")
            .ok_or(AdmissionError::EngineRead)?;
        let retained = self
            .db
            .get_pinned_cf_opt(cf, key.as_bytes(), &super::bootstrap::read_options())
            .map_err(|_| AdmissionError::EngineRead)?
            .ok_or(AdmissionError::StateMismatch)?;
        if retained.len() > super::MAX_OUTCOME_BYTES || retained.as_ref() != outcome.bytes() {
            return Err(AdmissionError::StateMismatch);
        }
        drop(retained);
        stamp.require(self)?;
        Ok(stamp)
    }
}
