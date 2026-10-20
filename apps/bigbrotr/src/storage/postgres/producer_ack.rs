//! Complete a lost native ACK only after full original replay and a fresh
//! authenticated read of the retained cumulative checkpoint/dependency closure.
use super::*;
use crate::{
    runtime::OwnedPermit,
    storage::rocks::store::{AdmissionError, IngestRecovery, RecoveryAcknowledgement},
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
        if permit.request().units().data_bytes() < RecoveryAcknowledgement::REQUIRED_DATA_BYTES
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
        replay
            .validate(recovery)
            .map_err(ProducerAcknowledgementError::Native)?;
        let manifest = *replay.manifest();
        let slot = replay.slot();
        let issued = replay.issued();
        let acknowledged = replay.acknowledged();
        let id = manifest.identity();
        let producer_epoch =
            ProducerEpoch::new(id.owner_epoch.get()).map_err(|_| OwnerError::InvalidInput)?;
        let (tx, mut lease, policy) = self
            .begin_history_authority(recovery, &input, 14, validate)
            .await?;
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
        replay
            .validate(recovery)
            .map_err(ProducerAcknowledgementError::Native)?;
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
            replay
                .validate(recovery)
                .map_err(ProducerAcknowledgementError::Native)?;
            if self.client.is_closed() {
                return Err(ProducerAcknowledgementError::DeliveryUnknown);
            }
            self.available = true;
            return Ok(ProducerAcknowledgementReceipt {
                slot,
                epoch: producer_epoch,
                issued,
                acknowledged,
            });
        }
        replay
            .validate(recovery)
            .map_err(ProducerAcknowledgementError::Native)?;
        initial_history::check_delivery(&mut lease, policy, input.cancelled)?;
        let pending = producer_progress::PendingAcknowledgement::recovered(replay, complete)?;
        let confirmed = pending
            .commit(tx)
            .await
            .map_err(|_| ProducerAcknowledgementError::CommitUnknown)?;
        if self.client.is_closed() {
            return Err(ProducerAcknowledgementError::DeliveryUnknown);
        }
        initial_history::check_delivery(&mut lease, policy, input.cancelled)?;
        recovery
            .acknowledge_checkpoint(confirmed, &permit, || {
                producer_progress::recovery::authority(&mut lease, policy, input.cancelled)
            })
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
