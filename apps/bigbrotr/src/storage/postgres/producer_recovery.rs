//! Copy a positive original under fresh Control; complete only the next retained
//! Monitor checkpoint. Native ACK remains the separate full-prefix operation.
use super::super::{producer_outcomes, producer_requests};
use super::*;
use crate::{
    runtime::OwnedPermit,
    storage::rocks::store::{
        AdmissionError, IngestRecovery, ProducerRecoveryError, ProducerRecoveryLimits,
        ResolvedProducerRequest,
    },
};
use bigbrotr_model::ProducerRequestId;
use std::{
    path::Path,
    sync::atomic::{AtomicBool, Ordering},
};

pub type ProducerResolveError = ProducerOperationError<ProducerRecoveryError>;

/// Trusted factory target and original request identity. The expected digest and
/// byte length come from its locked immutable reservation, never a new request.
pub struct ProducerResolveRequest<'a> {
    pub ingest_root: &'a Path,
    pub registration: &'a ingest_registry::InitialRegistration,
    pub request: ProducerRequestId,
    pub limits: ProducerRecoveryLimits,
    pub policy_ttl: Duration,
    pub cancelled: &'a AtomicBool,
}

impl ControlSession {
    /// Requires explicit producer closing first. Only the validated native
    /// result can supply the copied witness; a PG row alone cannot. Keep this
    /// bounded native owner worker and the connection driver retained and joined.
    /// Failures after begin leave this session unavailable; reconnect/reclaim
    /// current Control and resolve the same request, never a replacement ID.
    /// The borrowed progress grant is separate from retained input/outcome
    /// grants. Historical completion writes no native ACK or acquisition state.
    pub async fn resolve_initial_producer_request(
        &mut self,
        recovery: &IngestRecovery,
        input: ProducerResolveRequest<'_>,
        input_permit: OwnedPermit,
        outcome_permit: OwnedPermit,
        progress: &ProgressGrant,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<ResolvedProducerRequest, ProducerResolveError> {
        if input.cancelled.load(Ordering::Relaxed) {
            return Err(ProducerResolveError::Native(
                AdmissionError::AuthorityUnavailable.into(),
            ));
        }
        if input.request.producer_slot_id != input.registration.producer_slot_id
            || input.request.producer_epoch.get() != 1
        {
            return Err(OwnerError::InvalidInput.into());
        }
        let recorder = self.identity;
        let (tx, mut lease, epoch) = self
            .begin_producer_recovery(
                recovery,
                input.ingest_root,
                input.registration,
                input.policy_ttl,
                ingest_registry::RegistryLocks::Producer,
                validate,
            )
            .await?;
        // The common preflight already holds producer UPDATE. This SQL state is
        // a scheduling veto only; resolve_original_request also checks native v2.
        let closing: bool = tx.query_one(
            "SELECT state='closing' FROM control.producer_epochs WHERE producer_slot_id=$1::bytea AND producer_epoch=1",
            &[&&input.request.producer_slot_id.as_bytes()[..]],
        ).await.map_err(backend)?.try_get(0).map_err(backend)?;
        if !closing {
            return Err(ProducerResolveError::Registry(
                ingest_registry::RegistryError::InvalidMetadata,
            ));
        }
        producer_progress::recovery::require(
            &tx,
            recovery,
            recovery.manifest(),
            input.request.producer_slot_id,
            &input_permit,
            || producer_progress::recovery::authority(&mut lease, epoch, input.cancelled),
        )
        .await?;
        let reservation =
            producer_requests::read(&tx, input.registration.dataset_id, input.request)
                .await
                .map_err(ProducerResolveError::Reservation)?
                .ok_or(ProducerResolveError::Native(
                    ProducerRecoveryError::Unresolved,
                ))?;
        let manifest: [u8; 32] = Sha256::digest(recovery.manifest().encode().as_bytes()).into();
        if reservation.manifest_sha256 != manifest {
            return Err(ProducerResolveError::Reservation(
                producer_requests::ReservationError::Conflict,
            ));
        }
        let resolved = recovery
            .resolve_original_request(
                input.request,
                reservation.digest,
                input.limits,
                input_permit,
                outcome_permit,
                || {
                    if input.cancelled.load(Ordering::Relaxed) {
                        return Err(AdmissionError::AuthorityUnavailable);
                    }
                    lease
                        .check_delivery(epoch)
                        .map_err(|_| AdmissionError::AuthorityUnavailable)
                },
            )
            .map_err(ProducerResolveError::Native)?;
        let context = producer_progress::Context {
            dataset: input.registration.dataset_id,
            slot: input.request.producer_slot_id,
            epoch: 1,
            shard: input.registration.shard_id,
            manifest,
        };
        let witness =
            if producer_progress::supports_measurement_recovery(&tx, context, &resolved).await? {
                Some(
                    recovery
                        .measurement_checkpoint(&resolved)
                        .map_err(|error| ProducerResolveError::Native(error.into()))?,
                )
            } else {
                None
            };
        let mut progress_state = match &witness {
            Some(witness) => Some(
                producer_progress::prepare_measurement_recovery(&tx, context, witness, progress)
                    .await?,
            ),
            None => None,
        };
        producer_outcomes::record_recovered(&tx, &reservation, &resolved, recorder)
            .await
            .map_err(ProducerResolveError::Outcome)?;
        if let (Some(witness), Some(state)) = (&witness, &mut progress_state) {
            producer_progress::record(&tx, context, state, None, witness.basis(), recorder).await?;
            witness
                .require()
                .map_err(|error| ProducerResolveError::Native(error.into()))?;
        }
        if input.cancelled.load(Ordering::Relaxed)
            || recovery.native_unavailable()
            || lease.check_delivery(epoch).is_err()
        {
            return Err(ProducerResolveError::DeliveryUnknown);
        }
        tx.commit()
            .await
            .map_err(|_| ProducerResolveError::CommitUnknown)?;
        if input.cancelled.load(Ordering::Relaxed)
            || recovery.native_unavailable()
            || self.client.is_closed()
            || witness
                .as_ref()
                .is_some_and(|witness| witness.require().is_err())
            || lease.check_delivery(epoch).is_err()
        {
            return Err(ProducerResolveError::DeliveryUnknown);
        }
        self.available = true;
        Ok(resolved)
    }
}
