//! Opaque current-owner evidence for a downstream producer checkpoint.
//! No constructor accepts a decoded proof, PG outcome row or caller digest as
//! native authority. This module does not write ACK or reclaim request detail.
use super::{
    AdmissionError, AdmissionLimits, AdmissionOutcome, BootstrapError, BootstrapLimits,
    CertificationOutcome, IngestStore, NativeOutcomeCopy, NativeOutcomeKind, PreparedEvent,
    StoredCertification, StoredOutcome, StoredReceipt,
};
use crate::runtime::OwnedPermit;
use crate::storage::rocks::keys::IngestKey;
use bigbrotr_model::{Digest256, EvidenceIntent, FactRef, ObservedTime, ProducerRequestId};
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
        if store.write_uncertain {
            return Err(AdmissionError::CommitUnknown);
        }
        store.require_admission_owner(None)?;
        Ok(Self {
            root: store.handoff_root_identity()?,
            sequence: store.db.latest_sequence_number(),
            manifest: Sha256::digest(store.manifest.encode().as_bytes()).into(),
        })
    }

    fn require(self, store: &IngestStore) -> Result<(), AdmissionError> {
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
