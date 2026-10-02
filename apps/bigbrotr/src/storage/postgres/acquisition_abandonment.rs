//! Terminal recovery of the initial executor's whole acquisition intents.
use super::*;
use crate::{
    runtime::OwnedPermit,
    storage::rocks::store::{BootstrapError, ResolvedSource},
};
use std::sync::atomic::Ordering;

impl ControlSession {
    /// The initial executor borrows this native owner for its entire socket
    /// callback. Its exclusive resolved-source witness and current Control fence
    /// exclude old execution; neither a scheduling expiry nor a copied cut does.
    pub async fn abandon_initial_acquisition(
        &mut self,
        source: &ResolvedSource<'_>,
        input: ProducerCloseRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<u64, ProducerCloseError> {
        if permit.request().units().data_bytes() < 128 * 1024
            || permit.request().units().items() == 0
        {
            return Err(ProducerCloseError::Native(
                BootstrapError::InsufficientReservation,
            ));
        }
        let _reservation = permit;
        let spec = input.registration;
        if input.cancelled.load(Ordering::Relaxed) {
            return Err(ProducerCloseError::Native(
                BootstrapError::AuthorityUnavailable,
            ));
        }
        if source.producer().producer_slot_id() != spec.producer_slot_id
            || source.producer().producer_epoch().get() != 1
            || source.resolved_requests() > 64
        {
            return Err(OwnerError::InvalidInput.into());
        }
        let issuer = self.identity;
        let (tx, mut lease, epoch) = self
            .begin_producer_recovery(
                source.recovery(),
                input.ingest_root,
                spec,
                input.policy_ttl,
                ingest_registry::RegistryLocks::Workflow,
                validate,
            )
            .await?;
        source_closure::require_resolved_prefix(&tx, source, spec).await?;
        let dataset = &spec.dataset_id.as_bytes().as_slice();
        let slot = &spec.producer_slot_id.as_bytes().as_slice();
        let shard = i64::from(spec.shard_id);
        let bound = source.last_complete_sequence().to_string();
        let digest = source.digest();
        let old_resource = &spec.control.resource_id.as_bytes().as_slice();
        let old_epoch = spec.control.epoch.get().to_string();
        let old_incarnation = &spec.control.process_incarnation.as_bytes().as_slice();
        let closed: bool = tx
            .query_one(
                "SELECT state='closed' AND last_complete_sequence=$3::text::numeric
                    AND closure_digest=$4::bytea
               FROM control.source_streams
              WHERE dataset_id=$1::bytea AND shard_id=$2::bigint AND owner_epoch=1",
                &[dataset, &shard, &bound, &&digest[..]],
            )
            .await
            .map_err(backend)?
            .try_get(0)
            .map_err(backend)?;
        if !closed {
            return Err(OwnerError::InvalidMetadata.into());
        }

        // Retained gaps are checked without rewriting their original issuer or
        // timestamp. The predicate includes the complete terminal job relation.
        let prior = tx
            .query(
                RETAINED_GAPS,
                &[
                    slot,
                    dataset,
                    &shard,
                    &bound,
                    &&digest[..],
                    old_resource,
                    &old_epoch,
                    old_incarnation,
                ],
            )
            .await
            .map_err(backend)?;
        if prior.len() > 64 {
            return Err(ProducerCloseError::Native(
                BootstrapError::InsufficientReservation,
            ));
        }
        for row in &prior {
            if !row.try_get::<_, bool>(0).map_err(backend)? {
                return Err(OwnerError::InvalidMetadata.into());
            }
        }
        let completed = prior.len();
        drop(prior);
        let jobs = tx
            .query(
                CLAIMED_JOBS,
                &[dataset, old_resource, &old_epoch, old_incarnation],
            )
            .await
            .map_err(backend)?;
        if completed + jobs.len() > 64 {
            return Err(ProducerCloseError::Native(
                BootstrapError::InsufficientReservation,
            ));
        }
        let total = (completed + jobs.len()) as u64;
        // Refuse the complete batch before the first gap INSERT when any job is
        // advanced or unqualified. Jobs without mutation requests are included.
        for row in &jobs {
            if !row.try_get::<_, bool>(1).map_err(backend)? {
                return Err(OwnerError::InvalidMetadata.into());
            }
        }
        for row in jobs {
            if input.cancelled.load(Ordering::Relaxed) || lease.check_delivery(epoch).is_err() {
                return Err(ProducerCloseError::DeliveryUnknown);
            }
            let job: &[u8] = row.try_get(0).map_err(backend)?;
            if job.len() != 16 {
                return Err(OwnerError::InvalidMetadata.into());
            }
            let inserted = tx
                .execute(
                    INSERT_GAP,
                    &[
                        &job,
                        dataset,
                        slot,
                        &shard,
                        &bound,
                        &&digest[..],
                        &&issuer.resource_id.as_bytes()[..],
                        &issuer.epoch.get().to_string(),
                        &&issuer.process_incarnation.as_bytes()[..],
                    ],
                )
                .await
                .map_err(backend)?;
            let requested = tx
                .execute(REQUEST_CANCELLATION, &[&job])
                .await
                .map_err(backend)?;
            let cancelled = tx
                .execute(COMPLETE_CANCELLATION, &[&job])
                .await
                .map_err(backend)?;
            if (inserted, requested, cancelled) != (1, 1, 1) {
                return Err(OwnerError::InvalidMetadata.into());
            }
        }
        if input.cancelled.load(Ordering::Relaxed) || lease.check_delivery(epoch).is_err() {
            return Err(ProducerCloseError::DeliveryUnknown);
        }
        tx.commit()
            .await
            .map_err(|_| ProducerCloseError::CommitUnknown)?;
        if input.cancelled.load(Ordering::Relaxed)
            || self.client.is_closed()
            || lease.check_delivery(epoch).is_err()
        {
            return Err(ProducerCloseError::DeliveryUnknown);
        }
        self.available = true;
        Ok(total)
    }
}

const RETAINED_GAPS: &str = "
    SELECT g.dataset_id=$2::bytea AND g.shard_id=$3::bigint AND g.source_owner_epoch=1
        AND g.last_complete_sequence=$4::text::numeric AND g.source_closure_digest=$5::bytea
        AND g.previous_owner_resource=$6::bytea AND g.previous_owner_epoch=$7::text::numeric
        AND g.previous_owner_incarnation=$8::bytea
        AND j.dataset_id=g.dataset_id AND j.acquisition_job_protocol=1
        AND j.state='cancelled' AND j.job_revision=g.previous_job_revision+2
        AND j.checkpoint_revision=0 AND j.result_manifest_digest IS NULL
        AND j.reason_code=g.reason AND j.completed_at_us=g.abandoned_at_us
        AND (j.owner_resource_id,j.owner_epoch,j.owner_incarnation)
            = (g.issuer_resource,g.issuer_epoch,g.issuer_incarnation)
      FROM control.acquisition_gaps g JOIN control.jobs j USING(job_id)
     WHERE g.producer_slot_id=$1::bytea AND g.producer_epoch=1
     ORDER BY g.job_id LIMIT 65";

const CLAIMED_JOBS: &str = "
    SELECT substring(job_id FROM 1 FOR 17),
           coalesce(acquisition_job_protocol=1 AND state='queued'
               AND checkpoint_revision=0 AND input_complete, false)
      FROM control.jobs
     WHERE dataset_id=$1::bytea AND owner_resource_id=$2::bytea
       AND owner_epoch=$3::text::numeric AND owner_incarnation=$4::bytea
       AND kind='acquisition'
     ORDER BY job_id LIMIT 65 FOR UPDATE";

const INSERT_GAP: &str = "
    INSERT INTO control.acquisition_gaps
        (job_id,dataset_id,producer_slot_id,producer_epoch,shard_id,source_owner_epoch,
         last_complete_sequence,source_closure_digest,previous_job_revision,
         previous_owner_resource,previous_owner_epoch,previous_owner_incarnation,
         issuer_resource,issuer_epoch,issuer_incarnation,abandoned_at_us,reason)
    SELECT job_id,$2::bytea,$3::bytea,1,$4::bigint,1,$5::text::numeric,$6::bytea,job_revision,
           owner_resource_id,owner_epoch,owner_incarnation,$7::bytea,$8::text::numeric,$9::bytea,
           (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,'initial_producer_abandoned'
      FROM control.jobs WHERE job_id=$1::bytea";

const REQUEST_CANCELLATION: &str = "
    UPDATE control.jobs j SET state='cancel_requested',job_revision=j.job_revision+1,
           reason_code=g.reason,owner_resource_id=g.issuer_resource,
           owner_epoch=g.issuer_epoch,owner_incarnation=g.issuer_incarnation
      FROM control.acquisition_gaps g WHERE j.job_id=$1::bytea AND g.job_id=j.job_id";

const COMPLETE_CANCELLATION: &str = "
    UPDATE control.jobs j SET state='cancelled',job_revision=j.job_revision+1,
           completed_at_us=g.abandoned_at_us
      FROM control.acquisition_gaps g WHERE j.job_id=$1::bytea AND g.job_id=j.job_id";
