//! Complete a lost native ACK only after full original replay and a fresh
//! authenticated read of the retained cumulative checkpoint/dependency closure.
use super::*;
use crate::{
    runtime::OwnedPermit,
    storage::rocks::store::{
        AdmissionError, AdmissionLimits, IngestRecovery, RecoveryAcknowledgement,
        compacted_outcome_data_bytes,
    },
};
use bigbrotr_model::{ProducerEpoch, ProducerSlotId};

pub type ProducerAcknowledgementError = ProducerOperationError<AdmissionError>;

/// Positively observed final native watermark. No source-completion, workflow,
/// admission or reclamation capability follows from this reporting value.
pub struct ProducerAcknowledgementReceipt {
    slot: ProducerSlotId,
    epoch: ProducerEpoch,
    issued: u64,
    acknowledged: u64,
}
impl ProducerAcknowledgementReceipt {
    pub fn producer_slot_id(&self) -> ProducerSlotId {
        self.slot
    }
    pub fn producer_epoch(&self) -> ProducerEpoch {
        self.epoch
    }
    pub fn issued_through(&self) -> u64 {
        self.issued
    }
    pub fn acknowledged_through(&self) -> u64 {
        self.acknowledged
    }
}

// The inner streaming reducer retains its own 4KiB floor. This additional
// bound includes the owned handoff, copied manifest, policy tracker, receipt,
// pending/confirmed target and fixed outer SQL parameter/header scratch.
const _: () = assert!(
    4096 + size_of::<RecoveryAcknowledgement>()
        + size_of::<crate::storage::rocks::manifest::IngestManifest>()
        + size_of::<crate::runtime::policy_authority::PolicyLeaseTracker>()
        + size_of::<ProducerAcknowledgementReceipt>()
        + 2 * size_of::<crate::storage::rocks::store::AckTarget>()
        + 1024
        < RecoveryAcknowledgement::REQUIRED_DATA_BYTES as usize
);

impl ControlSession {
    pub async fn acknowledge_recovered_producer(
        &mut self,
        recovery: &mut IngestRecovery,
        replay: RecoveryAcknowledgement,
        input: InitialHistoryRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<ProducerAcknowledgementReceipt, ProducerAcknowledgementError> {
        if replay.compacted_requests() != 0 {
            return Err(OwnerError::InvalidInput.into());
        }
        self.acknowledge_recovered_producer_in(recovery, replay, input, permit, None, validate)
            .await
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "explicit compacted replay with distinct metadata/native and decoded-effect grants"
    )]
    pub async fn acknowledge_recovered_compacted_producer(
        &mut self,
        recovery: &mut IngestRecovery,
        replay: RecoveryAcknowledgement,
        input: InitialHistoryRequest<'_>,
        permit: OwnedPermit,
        limits: AdmissionLimits,
        outcomes: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<ProducerAcknowledgementReceipt, ProducerAcknowledgementError> {
        if replay.compacted_requests() == 0 {
            return Err(OwnerError::InvalidInput.into());
        }
        if outcomes.request().units().items() == 0
            || outcomes.request().units().data_bytes()
                < compacted_outcome_data_bytes(limits)
                    .map_err(ProducerAcknowledgementError::Native)?
        {
            return Err(ProducerAcknowledgementError::Native(
                AdmissionError::InsufficientReservation,
            ));
        }
        self.acknowledge_recovered_producer_in(
            recovery,
            replay,
            input,
            permit,
            Some((limits, outcomes)),
            validate,
        )
        .await
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "shared ACK transaction with an explicit compacted proof phase"
    )]
    async fn acknowledge_recovered_producer_in(
        &mut self,
        recovery: &mut IngestRecovery,
        replay: RecoveryAcknowledgement,
        input: InitialHistoryRequest<'_>,
        permit: OwnedPermit,
        compacted: Option<(AdmissionLimits, OwnedPermit)>,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<ProducerAcknowledgementReceipt, ProducerAcknowledgementError> {
        let required = if compacted.is_some() {
            RecoveryAcknowledgement::COMPACTED_REQUIRED_DATA_BYTES
        } else {
            RecoveryAcknowledgement::REQUIRED_DATA_BYTES
        };
        if permit.request().units().data_bytes() < required
            || permit.request().units().native_bytes()
                < RecoveryAcknowledgement::REQUIRED_NATIVE_BYTES
            || permit.request().units().items() == 0
        {
            return Err(ProducerAcknowledgementError::Native(
                AdmissionError::InsufficientReservation,
            ));
        }
        if !recovery.matches_configured_root(input.ingest_root)
            || recovery.manifest().identity().dataset_id != self.request.dataset
        {
            return Err(OwnerError::InvalidInput.into());
        }
        validate_replay(&replay, recovery, input.cancelled, compacted.is_some())?;
        let manifest = *replay.manifest();
        let slot = replay.slot();
        let issued = replay.issued();
        let acknowledged = replay.acknowledged();
        let id = manifest.identity();
        let producer_epoch =
            ProducerEpoch::new(id.owner_epoch.get()).map_err(|_| OwnerError::InvalidInput)?;
        let instance = *self.request.instance.as_bytes();
        let (tx, mut lease, policy) = self
            .begin_history_authority(
                recovery,
                &input,
                if compacted.is_some() { 34 } else { 14 },
                validate,
            )
            .await?;
        if let Some((limits, outcomes)) = &compacted {
            let ancestor = replay
                .compacted_initial_predecessor(recovery, &permit, input.cancelled)
                .map_err(ProducerAcknowledgementError::Native)?
                .ok_or(OwnerError::InvalidInput)?;
            let previous =
                ingest_registry::reconstruct_historical(&tx, instance, ancestor.manifest())
                    .await
                    .map_err(ProducerAcknowledgementError::Registry)?;
            if previous.producer_slot_id != slot {
                return Err(OwnerError::InvalidMetadata.into());
            }
            // Establish the historical parent/source locks before the typed
            // reader takes producer UPDATE, preserving retirement lock order.
            let floor = ancestor.floor().fields();
            initial_compaction::require_retired_predecessor(
                &tx,
                &previous,
                floor.issued_through,
                floor.source_through,
                floor.closure_digest,
            )
            .await
            .map_err(history_error)?;
            producer_progress::recovery::require_compacted_acknowledgement(
                &tx,
                recovery,
                &ancestor,
                &permit,
                input.cancelled,
                || producer_progress::recovery::authority(&mut lease, policy, input.cancelled),
            )
            .await?;
            let verified = producer_compaction::resolve_initial_outcomes(
                &tx,
                ancestor
                    .resolution(input.cancelled)
                    .map_err(ProducerAcknowledgementError::Native)?,
                *limits,
                outcomes,
                &mut lease,
                policy,
                input.cancelled,
            )
            .await?;
            verified
                .validate(recovery, input.cancelled)
                .map_err(ProducerAcknowledgementError::Native)?;
        }
        // Current Control/dataset precede this historical Ingest parent SHARE
        // and the progress reader's producer UPDATE. Old scheduling expiry is
        // not recovery authority; the consumed replay supplies local exclusion.
        tx.query_one(
            "SELECT p.producer_slot_id FROM control.owner_epochs o
            JOIN control.producer_epochs p ON p.dataset_id=o.dataset_id
              AND p.owner_resource_id=o.resource_id AND p.owner_epoch=o.epoch
              AND p.owner_incarnation=o.process_incarnation
            WHERE o.dataset_id=$1::bytea AND o.resource_id=$2::bytea
              AND o.epoch=$3::text::numeric AND o.process_incarnation=$4::bytea
              AND o.role='ingest' AND p.producer_slot_id=$5::bytea
              AND p.producer_epoch=$3::text::numeric AND p.state IN ('closing','retired')
            FOR SHARE OF o",
            &[
                &&id.dataset_id.as_bytes()[..],
                &&id.owner_resource_id.as_bytes()[..],
                &id.owner_epoch.get().to_string(),
                &&id.process_incarnation.as_bytes()[..],
                &&slot.as_bytes()[..],
            ],
        )
        .await
        .map_err(backend)?;
        let complete =
            producer_progress::recovery::require(&tx, recovery, &manifest, slot, &permit, || {
                producer_progress::recovery::authority(&mut lease, policy, input.cancelled)
            })
            .await?;
        validate_replay(&replay, recovery, input.cancelled, compacted.is_some())?;
        initial_history::check_delivery(&mut lease, policy, input.cancelled)?;
        let enabled: bool = tx
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM control.schema_versions WHERE migration=40)",
                &[],
            )
            .await
            .map_err(backend)?
            .try_get(0)
            .map_err(backend)?;
        if !enabled {
            if acknowledged != 0 {
                return Err(OwnerError::InvalidMetadata.into());
            }
            initial_history::finish(tx, &mut lease, policy, input.cancelled).await?;
            validate_replay(&replay, recovery, input.cancelled, compacted.is_some())?;
            if self.client.is_closed() {
                return Err(ProducerAcknowledgementError::DeliveryUnknown);
            }
            initial_history::check_delivery(&mut lease, policy, input.cancelled)?;
            self.available = true;
            return Ok(ProducerAcknowledgementReceipt {
                slot,
                epoch: producer_epoch,
                issued,
                acknowledged,
            });
        }
        validate_replay(&replay, recovery, input.cancelled, compacted.is_some())?;
        initial_history::check_delivery(&mut lease, policy, input.cancelled)?;
        let pending = if compacted.is_some() {
            producer_progress::PendingAcknowledgement::recovered_compacted(replay, complete)
        } else {
            producer_progress::PendingAcknowledgement::recovered(replay, complete)
        }?;
        let confirmed = pending
            .commit(tx)
            .await
            .map_err(|_| ProducerAcknowledgementError::CommitUnknown)?;
        if self.client.is_closed() {
            return Err(ProducerAcknowledgementError::DeliveryUnknown);
        }
        initial_history::check_delivery(&mut lease, policy, input.cancelled)?;
        let authority =
            || producer_progress::recovery::authority(&mut lease, policy, input.cancelled);
        if compacted.is_some() {
            recovery.acknowledge_compacted_checkpoint(
                confirmed,
                &permit,
                input.cancelled,
                authority,
            )
        } else {
            recovery.acknowledge_checkpoint(confirmed, &permit, authority)
        }
        .map_err(ProducerAcknowledgementError::Native)?;
        if self.client.is_closed() {
            return Err(ProducerAcknowledgementError::DeliveryUnknown);
        }
        self.available = true;
        Ok(ProducerAcknowledgementReceipt {
            slot,
            epoch: producer_epoch,
            issued,
            acknowledged: complete,
        })
    }
}

fn validate_replay(
    replay: &RecoveryAcknowledgement,
    recovery: &IngestRecovery,
    cancelled: &std::sync::atomic::AtomicBool,
    compacted: bool,
) -> Result<(), ProducerAcknowledgementError> {
    if compacted {
        replay.validate_compacted(recovery, cancelled)
    } else {
        replay.validate(recovery)
    }
    .map_err(ProducerAcknowledgementError::Native)
}

fn history_error(error: InitialSuccessionError) -> ProducerAcknowledgementError {
    match error {
        ProducerOperationError::Control(e) => ProducerOperationError::Control(e),
        ProducerOperationError::Registry(e) => ProducerOperationError::Registry(e),
        ProducerOperationError::Native(e) => ProducerOperationError::Native(e.into()),
        ProducerOperationError::Reservation(e) => ProducerOperationError::Reservation(e),
        ProducerOperationError::Outcome(e) => ProducerOperationError::Outcome(e),
        ProducerOperationError::Progress(e) => ProducerOperationError::Progress(e),
        ProducerOperationError::CommitUnknown => ProducerOperationError::CommitUnknown,
        ProducerOperationError::DeliveryUnknown => ProducerOperationError::DeliveryUnknown,
    }
}
