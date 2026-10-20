//! Synced cumulative ACK without reclamation. PG confirmation is a capability,
//! never a caller-supplied watermark. Every original and receipt remains intact.
use super::bootstrap::Prepared;
use super::producer_checkpoint::CheckpointStamp;
use super::{AdmissionError, BootstrapError, BootstrapLimits, IngestRecovery, IngestStore};
use crate::runtime::OwnedPermit;
use crate::storage::postgres::control_owner::ConfirmedAcknowledgement;
use crate::storage::rocks::keys::IngestKey;
use bigbrotr_model::{ProducerEpoch, ProducerSlotId};
use sha2::{Digest, Sha256};

enum AckStamp {
    Live(CheckpointStamp),
    Recovery {
        root: (u64, u64),
        sequence: u64,
        manifest: [u8; 32],
    },
}
impl AckStamp {
    fn require(&self, store: &IngestStore) -> Result<(), AdmissionError> {
        match self {
            Self::Live(stamp) => stamp.require(store),
            Self::Recovery {
                root,
                sequence,
                manifest,
            } => {
                if store.write_uncertain {
                    return Err(AdmissionError::CommitUnknown);
                }
                if store.handoff_root_identity()? != *root
                    || store.db.latest_sequence_number() != *sequence
                    || <[u8; 32]>::from(Sha256::digest(store.manifest.encode().as_bytes()))
                        != *manifest
                    || store.live_initial.is_some()
                    || store.live_successor.is_some()
                {
                    return Err(AdmissionError::StateMismatch);
                }
                Ok(())
            }
        }
    }
}

/// Consuming handoff from complete positive replay. It contains no borrowed
/// native handle, so completion can regain exclusive mutable custody. This is
/// not a checkpoint or a writer; current PG closure must still be validated.
pub struct RecoveryAcknowledgement {
    stamp: AckStamp,
    manifest: crate::storage::rocks::manifest::IngestManifest,
    slot: ProducerSlotId,
    issued: u64,
    acknowledged: u64,
}

/// Positive original-prefix reducer for ACK completion only. It does not expose
/// a source-publication/retirement conversion to ordinary recovery callers.
pub struct RecoveryAcknowledgementResolution<'a>(AckResolution<'a>);
#[allow(
    clippy::large_enum_variant,
    reason = "fixed bounded prefix state stays under the retained closing grant; no extra heap allocation"
)]
enum AckResolution<'a> {
    Initial(super::SourceResolution<'a>),
    Activated(super::ActivatedSourceResolution<'a>),
}
const _: () = assert!(size_of::<RecoveryAcknowledgementResolution<'_>>() < 4096);
impl<'a> RecoveryAcknowledgementResolution<'a> {
    pub(in crate::storage) fn initial(source: super::SourceResolution<'a>) -> Self {
        Self(AckResolution::Initial(source))
    }
    pub(in crate::storage) fn activated(source: super::ActivatedSourceResolution<'a>) -> Self {
        Self(AckResolution::Activated(source))
    }
    pub fn record(
        &mut self,
        original: &super::ResolvedProducerRequest,
    ) -> Result<(), BootstrapError> {
        match &mut self.0 {
            AckResolution::Initial(source) => source.record(original),
            AckResolution::Activated(source) => source.record(original),
        }
    }
    pub fn finish(self) -> Result<RecoveryAcknowledgement, BootstrapError> {
        match self.0 {
            AckResolution::Initial(source) => source.finish()?.into_recovery_acknowledgement(),
            AckResolution::Activated(source) => source.finish()?.into_recovery_acknowledgement(),
        }
    }
}
impl RecoveryAcknowledgement {
    /// Retained completion token/manifest/lease plus the streamed PG validator.
    pub const REQUIRED_DATA_BYTES: u64 = 8192;
    /// One fixed synced producer-state WriteBatch; no decoded scientific data.
    pub const REQUIRED_NATIVE_BYTES: u64 = 4096;
    pub(in crate::storage::rocks::store) fn from_replay(
        recovery: &IngestRecovery,
        manifest: crate::storage::rocks::manifest::IngestManifest,
        slot: ProducerSlotId,
        issued: u64,
    ) -> Result<Self, BootstrapError> {
        let store = &recovery.store;
        if store.write_uncertain {
            return Err(BootstrapError::CommitUnknown);
        }
        let epoch = ProducerEpoch::new(manifest.identity().owner_epoch.get())
            .map_err(|_| BootstrapError::InvalidIntent)?;
        let state = store
            .producer_state(slot, epoch)?
            .ok_or(BootstrapError::StateMismatch)?;
        if !state.closing
            || state.issued != issued
            || state.retired != 0
            || issued > RETAINED_PRODUCER_REQUEST_LIMIT
        {
            return Err(BootstrapError::StateMismatch);
        }
        Ok(Self {
            stamp: AckStamp::Recovery {
                root: store.handoff_root_identity()?,
                sequence: store.db.latest_sequence_number(),
                manifest: Sha256::digest(store.manifest.encode().as_bytes()).into(),
            },
            manifest,
            slot,
            issued,
            acknowledged: state.acknowledged,
        })
    }
    pub(in crate::storage) fn manifest(&self) -> &crate::storage::rocks::manifest::IngestManifest {
        &self.manifest
    }
    pub(in crate::storage) fn slot(&self) -> ProducerSlotId {
        self.slot
    }
    pub(in crate::storage) fn issued(&self) -> u64 {
        self.issued
    }
    pub(in crate::storage) fn acknowledged(&self) -> u64 {
        self.acknowledged
    }
    pub(in crate::storage) fn validate(
        &self,
        recovery: &IngestRecovery,
    ) -> Result<(), AdmissionError> {
        self.stamp.require(&recovery.store)
    }
    pub(in crate::storage) fn target(self, complete: u64) -> Result<AckTarget, AdmissionError> {
        if complete > self.issued {
            return Err(AdmissionError::StateMismatch);
        }
        Ok(AckTarget {
            stamp: self.stamp,
            slot: self.slot,
            epoch: ProducerEpoch::new(self.manifest.identity().owner_epoch.get())
                .map_err(|_| AdmissionError::InvalidInput)?,
            complete,
            closed: true,
        })
    }
}

/// Finite first no-GC slice, independent of the accepted outstanding window.
pub const RETAINED_PRODUCER_REQUEST_LIMIT: u64 = 64;

/// Current native observation for scheduling. It is not SQL authority, a
/// checkpoint, or permission to reclaim any retained input/outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProducerCredit {
    issued: u64,
    acknowledged: u64,
}
impl ProducerCredit {
    pub fn issued_through(self) -> u64 {
        self.issued
    }
    pub fn acknowledged_through(self) -> u64 {
        self.acknowledged
    }
    pub fn outstanding_requests(self) -> u64 {
        self.issued - self.acknowledged
    }
}

/// The native completion stamp is captured before the checkpoint COMMIT. Only
/// the connected PG owner can turn this pending target into confirmation.
pub(in crate::storage) struct AckTarget {
    stamp: AckStamp,
    slot: ProducerSlotId,
    epoch: ProducerEpoch,
    complete: u64,
    closed: bool,
}
impl AckTarget {
    pub(super) fn new(
        stamp: CheckpointStamp,
        slot: ProducerSlotId,
        epoch: ProducerEpoch,
        complete: u64,
    ) -> Self {
        Self {
            stamp: AckStamp::Live(stamp),
            slot,
            epoch,
            complete,
            closed: false,
        }
    }
}

impl IngestRecovery {
    pub(in crate::storage) fn acknowledge_checkpoint(
        &mut self,
        confirmed: ConfirmedAcknowledgement,
        permit: &OwnedPermit,
        authority: impl FnMut() -> Result<(), AdmissionError>,
    ) -> Result<(), AdmissionError> {
        self.store
            .acknowledge_checkpoint(confirmed, permit, authority)
    }
}

impl IngestStore {
    pub(super) fn require_request_capacity(
        &self,
        request: bigbrotr_model::ProducerRequestId,
        maximum: u64,
    ) -> Result<(), AdmissionError> {
        self.require_admission_owner(Some(request))?;
        let credit = self.producer_credit(request.producer_slot_id)?;
        let sequence = request.request_sequence.get();
        if sequence > RETAINED_PRODUCER_REQUEST_LIMIT {
            return Err(AdmissionError::Limit);
        }
        // Staging may reserve a contiguous import batch before native issue.
        // Its furthest request must fit actual native credit; final admission
        // still requires exactly I+1. A PG checkpoint alone grants no credit.
        if sequence > credit.issued && (maximum == 0 || sequence - credit.acknowledged > maximum) {
            return Err(AdmissionError::Limit);
        }
        Ok(())
    }
    pub fn producer_credit(&self, slot: ProducerSlotId) -> Result<ProducerCredit, AdmissionError> {
        let epoch = ProducerEpoch::new(self.manifest.identity().owner_epoch.get())
            .map_err(|_| AdmissionError::InvalidInput)?;
        // Bind the supplied initial slot to the selected owner on live paths.
        self.require_admission_owner(Some(bigbrotr_model::ProducerRequestId {
            producer_slot_id: slot,
            producer_epoch: epoch,
            request_sequence: bigbrotr_model::RequestSequence::new(1).expect("nonzero"),
        }))?;
        if self.write_uncertain {
            return Err(AdmissionError::CommitUnknown);
        }
        let state = match self.producer_state(slot, epoch)? {
            Some(state) => state,
            None if epoch.get() == 1 => super::producer_closing::ProducerState::default(),
            None => return Err(AdmissionError::StateMismatch),
        };
        if state.closing || state.retired != 0 || state.issued > RETAINED_PRODUCER_REQUEST_LIMIT {
            return Err(AdmissionError::StateMismatch);
        }
        Ok(ProducerCredit {
            issued: state.issued,
            acknowledged: state.acknowledged,
        })
    }

    pub(in crate::storage) fn acknowledge_checkpoint(
        &mut self,
        confirmed: ConfirmedAcknowledgement,
        permit: &OwnedPermit,
        authority: impl FnMut() -> Result<(), AdmissionError>,
    ) -> Result<(), AdmissionError> {
        self.acknowledge_target(confirmed.into_target(), permit, authority)
    }

    // The production entry consumes PG confirmation; this private kernel also
    // admits unit tests of native mechanics without claiming SQL confirmation.
    fn acknowledge_target(
        &mut self,
        target: AckTarget,
        permit: &OwnedPermit,
        mut authority: impl FnMut() -> Result<(), AdmissionError>,
    ) -> Result<(), AdmissionError> {
        if permit.request().units().data_bytes() < 4096
            || permit.request().units().native_bytes() < 4096
            || permit.request().units().items() == 0
        {
            return Err(AdmissionError::InsufficientReservation);
        }
        authority()?;
        target.stamp.require(self)?;
        let mut state = self
            .producer_state(target.slot, target.epoch)?
            .ok_or(AdmissionError::StateMismatch)?;
        if state.closing != target.closed
            || state.retired != 0
            || target.complete < state.acknowledged
            || target.complete > state.issued
            || state.issued > RETAINED_PRODUCER_REQUEST_LIMIT
        {
            return Err(AdmissionError::StateMismatch);
        }
        if target.complete == state.acknowledged {
            return authority();
        }
        let sequence = self
            .db
            .latest_sequence_number()
            .checked_add(1)
            .ok_or(AdmissionError::Limit)?;
        if let Some(frontier) = &self.live_successor {
            frontier.require_acknowledgement(self, target.complete, sequence)?;
        }
        state.acknowledged = target.complete;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(27)
            .map_err(|_| AdmissionError::Allocation)?;
        bytes.extend_from_slice(&[if state.closing { 2 } else { 1 }, 0x31]);
        bytes.extend_from_slice(&state.issued.to_be_bytes());
        bytes.extend_from_slice(&state.acknowledged.to_be_bytes());
        bytes.extend_from_slice(&state.retired.to_be_bytes());
        if state.closing {
            bytes.push(1);
        }
        let mut batch = Prepared::new(BootstrapLimits {
            max_mappings: 1,
            max_batch_bytes: 1024,
        })?;
        batch.put(
            IngestKey::ProducerState {
                producer_slot_id: target.slot,
                producer_epoch: target.epoch,
            },
            bytes,
        )?;
        let result = self.commit_prepared(batch, || {
            authority().map_err(|_| BootstrapError::AuthorityUnavailable)
        });
        if result.is_err() {
            if let Some(frontier) = &self.live_successor {
                frontier.poison();
            }
            result?;
        }
        self.require_acknowledgement_sequence(sequence)?;
        if let Some(frontier) = &mut self.live_successor {
            frontier.acknowledged(target.complete, sequence);
        }
        authority()
    }
    fn require_acknowledgement_sequence(&mut self, expected: u64) -> Result<(), AdmissionError> {
        if self.db.latest_sequence_number() != expected {
            self.write_uncertain = true;
            if let Some(frontier) = &self.live_successor {
                frontier.poison();
            }
            return Err(AdmissionError::StateMismatch);
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn test_acknowledge_target(
        &mut self,
        target: AckTarget,
        permit: &OwnedPermit,
        authority: impl FnMut() -> Result<(), AdmissionError>,
    ) -> Result<(), AdmissionError> {
        self.acknowledge_target(target, permit, authority)
    }

    #[cfg(test)]
    pub(super) fn test_acknowledgement_sequence(
        &mut self,
        expected: u64,
    ) -> Result<(), AdmissionError> {
        self.require_acknowledgement_sequence(expected)
    }
}
