//! Positively confirmed authority for the shared native reclamation kernel.
//! The sole constructor currently accepts a completely retired initial lifetime
//! without any successor intent. No SQL proof ledger or floor format is added.
use super::*;
use crate::{
    runtime::{OwnedPermit, policy_authority::PolicyLeaseTracker},
    storage::rocks::{
        producer_floor::{ClosedProducerFloor, ClosedProducerFloorFields},
        store::{
            AdmissionError, AdmissionLimits, ClosedProducerReclamation, ClosedReclamationReceipt,
            IngestRecovery, ResolvedSource,
        },
    },
};
use bigbrotr_model::PolicyEpoch;
use std::sync::atomic::AtomicBool;

pub type ProducerReclamationError = ProducerOperationError<AdmissionError>;
pub struct ConfirmedClosedReclamation {
    native: ConfirmedNativeReclamation,
    lease: PolicyLeaseTracker,
    policy: PolicyEpoch,
    control: ingest_registry::ControlOwner,
}
// Only the terminal PostgreSQL confirmation factories below can mint this
// native dispatch capability. Pending native plans cannot enter production GC.
pub(in crate::storage) struct ConfirmedNativeReclamation {
    plan: ClosedProducerReclamation,
}
impl ConfirmedNativeReclamation {
    pub(in crate::storage) fn into_plan(self) -> ClosedProducerReclamation {
        self.plan
    }
}
impl ConfirmedClosedReclamation {
    pub const REQUIRED_DATA_BYTES: u64 = ClosedProducerReclamation::DATA_BYTES;
    pub const REQUIRED_NATIVE_BYTES: u64 = ClosedProducerReclamation::NATIVE_BYTES;
}
impl CompactedInitialRetirement<'_> {
    /// Consume the freshly confirmed terminal reader, retaining its original
    /// deadline and exact canonical/native/input proof. A fresh retry must run
    /// the reader again; a portable floor cannot call this constructor.
    pub fn confirm_reclamation(
        self,
        recovery: &IngestRecovery,
        metadata: OwnedPermit,
        cancelled: &AtomicBool,
    ) -> Result<ConfirmedClosedReclamation, ProducerReclamationError> {
        let plan = ClosedProducerReclamation::initial_compacted(
            recovery,
            &self.native,
            metadata,
            cancelled,
        )
        .map_err(ProducerReclamationError::Native)?;
        let mut lease = self.lease;
        initial_history::check_delivery(&mut lease, self.policy, cancelled)?;
        Ok(ConfirmedClosedReclamation {
            native: ConfirmedNativeReclamation { plan },
            lease,
            policy: self.policy,
            control: self.control,
        })
    }
}
impl ControlSession {
    #[allow(
        clippy::too_many_arguments,
        reason = "complete replay, typed closure, separate retained grants and original authority deadline"
    )]
    pub async fn confirm_initial_producer_reclamation(
        &mut self,
        source: &ResolvedSource<'_>,
        input: ProducerCloseRequest<'_>,
        limits: AdmissionLimits,
        metadata: OwnedPermit,
        effects: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<ConfirmedClosedReclamation, ProducerReclamationError> {
        let recovery = source.recovery();
        source
            .validate_initial(input.cancelled)
            .map_err(|e| ProducerReclamationError::Native(e.into()))?;
        let spec = input.registration;
        if source.producer().producer_slot_id() != spec.producer_slot_id
            || source.resolved_requests() == 0
            || source.resolved_requests() > 64
            || source.acknowledged_through() != source.resolved_requests()
        {
            return Err(OwnerError::InvalidInput.into());
        }
        let control = self.identity;
        let (tx, mut lease, policy) = self
            .begin_producer_recovery::<AdmissionError>(
                recovery,
                input.ingest_root,
                spec,
                input.policy_ttl,
                ingest_registry::RegistryLocks::Retirement,
                validate,
            )
            .await?;
        source_closure::require_recorded_outcomes(&tx, source, spec)
            .await
            .map_err(reclamation_close_error)?;
        let complete = producer_progress::recovery::require(
            &tx,
            recovery,
            recovery.manifest(),
            spec.producer_slot_id,
            &metadata,
            || producer_progress::recovery::authority(&mut lease, policy, input.cancelled),
        )
        .await?;
        let slot = &spec.producer_slot_id.as_bytes().as_slice();
        let resource = &spec.ingest_resource_id.as_bytes().as_slice();
        if complete != source.resolved_requests()
            || !tx
                .query_one(producer_compaction::NO_SUCCESSOR, &[resource, slot])
                .await
                .map_err(backend)?
                .try_get::<_, bool>(0)
                .map_err(backend)?
        {
            return Err(OwnerError::InvalidMetadata.into());
        }
        let requests = tx.query("SELECT left(request_sequence::text,21),acquisition_job_id IS NOT NULL
            FROM control.producer_requests WHERE producer_slot_id=$1::bytea AND producer_epoch=1 ORDER BY request_sequence LIMIT 65", &[slot]).await.map_err(backend)?;
        if requests.len() as u64 != source.resolved_requests() {
            return Err(OwnerError::InvalidMetadata.into());
        }
        for (offset, row) in requests.iter().enumerate() {
            let sequence = ingest_registry::decimal(row.try_get(0).map_err(backend)?)
                .map_err(ProducerReclamationError::Registry)?;
            if sequence != offset as u64 + 1
                || source.event_request_mask() & (1_u64 << offset) != 0
                    && !row.try_get::<_, bool>(1).map_err(backend)?
            {
                return Err(OwnerError::InvalidMetadata.into());
            }
        }
        drop(requests);
        let digest = source.digest();
        tx.query_one(
            "SELECT control.require_initial_retirement($1::bytea,1,$2::bytea)",
            &[slot, &&digest[..]],
        )
        .await
        .map_err(backend)?;
        let state = tx
            .query_one(
                producer_retirement::RETIREMENT_STATE,
                &[
                    slot,
                    resource,
                    &spec.ingest_role_instance_id.as_slice(),
                    &spec.dataset_id.as_bytes().as_slice(),
                    &&digest[..],
                    &source.resolved_requests().to_string(),
                    &i64::from(spec.shard_id),
                    &source.last_complete_sequence().to_string(),
                ],
            )
            .await
            .map_err(backend)?;
        if !state.try_get::<_, bool>(1).map_err(backend)? {
            return Err(OwnerError::InvalidMetadata.into());
        }
        drop(state);
        // require() above verified the complete typed head/dependency digest.
        // This exact locked head supplies H to the existing v3 floor.
        let head = tx.query_one("SELECT left(checkpoint_revision::text,21),substring(checkpoint_digest FROM 1 FOR 33)
            FROM control.producer_checkpoints WHERE producer_slot_id=$1::bytea AND producer_epoch=1 AND progress_format=1
                AND complete_through_request=$2::text::numeric", &[slot, &complete.to_string()]).await.map_err(backend)?;
        let revision = ingest_registry::decimal(head.try_get(0).map_err(backend)?)
            .map_err(ProducerReclamationError::Registry)?;
        let floor = ClosedProducerFloor::new(ClosedProducerFloorFields {
            issued_through: source.resolved_requests(),
            manifest_sha256: Sha256::digest(recovery.manifest().encode().as_bytes()).into(),
            source_through: source.last_complete_sequence(),
            closure_digest: digest,
            checkpoint_revision: revision,
            checkpoint_digest: fixed::<32>(&head, 1)?,
        })
        .map_err(|_| OwnerError::InvalidMetadata)?;
        drop(head);
        let plan = ClosedProducerReclamation::initial_original(
            source,
            floor,
            limits,
            metadata,
            &effects,
            input.cancelled,
        )
        .map_err(ProducerReclamationError::Native)?;
        let canonical = producer_compaction::resolve_initial_outcomes(
            &tx,
            plan.resolution(recovery, input.cancelled)
                .map_err(ProducerReclamationError::Native)?,
            limits,
            &effects,
            &mut lease,
            policy,
            input.cancelled,
        )
        .await?;
        canonical
            .validate(recovery, input.cancelled)
            .map_err(ProducerReclamationError::Native)?;
        initial_history::check_delivery(&mut lease, policy, input.cancelled)?;
        producer_retirement::finish::<AdmissionError>(tx, &mut lease, policy, input.cancelled)
            .await?;
        if self.client.is_closed() {
            return Err(ProducerReclamationError::DeliveryUnknown);
        }
        plan.validate(recovery, input.cancelled)
            .map_err(ProducerReclamationError::Native)?;
        initial_history::check_delivery(&mut lease, policy, input.cancelled)?;
        self.available = true;
        Ok(ConfirmedClosedReclamation {
            native: ConfirmedNativeReclamation { plan },
            lease,
            policy,
            control,
        })
    }
    /// Consume exact confirmation under the same current Control deadline.
    /// This performs one bounded synced native batch, then authenticated unlink
    /// and directory fsync. Any uncertain stage returns no success report.
    pub fn reclaim_closed_producer(
        &mut self,
        recovery: &mut IngestRecovery,
        confirmed: ConfirmedClosedReclamation,
        cancelled: &AtomicBool,
    ) -> Result<ClosedReclamationReceipt, ProducerReclamationError> {
        if confirmed.control != self.identity || self.client.is_closed() {
            return Err(ProducerReclamationError::DeliveryUnknown);
        }
        self.begin_operation()?;
        let ConfirmedClosedReclamation {
            native,
            mut lease,
            policy,
            ..
        } = confirmed;
        let result = recovery
            .reclaim_closed_producer(native, cancelled, || {
                producer_progress::recovery::authority(&mut lease, policy, cancelled)
            })
            .map_err(ProducerReclamationError::Native)?;
        if self.client.is_closed() {
            return Err(ProducerReclamationError::DeliveryUnknown);
        }
        initial_history::check_delivery(&mut lease, policy, cancelled)?;
        self.available = true;
        Ok(result)
    }
}
fn reclamation_close_error(error: ProducerCloseError) -> ProducerReclamationError {
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
