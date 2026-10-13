//! Whole-intent uncertainty for the activated local executor's queued claims.
//! The borrowed native source proof excludes execution; SQL metadata does not.
use super::*;
use crate::{
    runtime::OwnedPermit,
    storage::rocks::store::{BootstrapError, ResolvedActivatedSource},
};

impl ControlSession {
    /// Preserve the activating Control's accepted intents without adopting its
    /// execution. Completed gaps retain their original issuer and timestamp.
    pub async fn abandon_activated_initial_acquisition(
        &mut self,
        closed: &ClosedActivatedInitialSource,
        source: &ResolvedActivatedSource<'_>,
        input: InitialHistoryRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<u64, SourceCloseError> {
        if permit.request().units().data_bytes() < 128 * 1024
            || permit.request().units().items() == 0
        {
            return Err(SourceCloseError::Native(
                BootstrapError::InsufficientReservation,
            ));
        }
        let _permit = permit;
        closed.require_source(source)?;
        activated_recovery::require_deployment(&closed.0.metadata, self)?;
        let issuer = self.identity;
        let (tx, mut lease, epoch) = self
            .begin_initial_history_locked(source.recovery(), &input, 2, 36, validate)
            .await?;
        let state =
            activated_recovery::require_metadata(&tx, &closed.0.metadata, issuer, epoch).await?;
        if !state.source_closed || !state.producer_closing {
            return Err(OwnerError::InvalidMetadata.into());
        }
        source.validate().map_err(SourceCloseError::Native)?;
        activated_source::require_recorded(&tx, source).await?;
        let id = source.manifest().identity();
        let dataset = &id.dataset_id.as_bytes().as_slice();
        let slot_id = source.producer_slot_id();
        let slot = slot_id.as_bytes().as_slice();
        let shard = i64::from(id.shard_id);
        let bound = source.last_complete_sequence().to_string();
        let digest = source.digest();
        // The complete source classifier already matched this immutable receipt
        // to the retained handoff. Its Control tuple owns the queued jobs; the
        // original producer-slot registrar and Ingest incarnation do not.
        let activation = tx
            .query_opt(
                "SELECT substring(a.activated_by_resource FROM 1 FOR 17),
                    left(a.activated_by_epoch::text,21),substring(a.activated_by_incarnation FROM 1 FOR 17)
                 FROM control.initial_ingest_activations a JOIN control.source_streams s
                    ON s.dataset_id=a.dataset_id AND s.owner_resource_id=a.owner_resource_id AND s.owner_epoch=2
                 WHERE a.producer_slot_id=$1::bytea AND a.dataset_id=$2::bytea
                    AND s.shard_id=$3::bigint AND s.state='closed'
                    AND s.last_complete_sequence=$4::text::numeric AND s.closure_digest=$5::bytea",
                &[&slot, dataset, &shard, &bound, &&digest[..]],
            )
            .await
            .map_err(backend)?
            .ok_or(OwnerError::InvalidMetadata)?;
        let old_resource = fixed::<16>(&activation, 0)?;
        let old_epoch = number(&activation, 1)?.to_string();
        let old_incarnation = fixed::<16>(&activation, 2)?;
        drop(activation);
        let prior = tx
            .query(
                RETAINED_GAPS,
                &[
                    &slot,
                    dataset,
                    &shard,
                    &bound,
                    &&digest[..],
                    &&old_resource[..],
                    &old_epoch,
                    &&old_incarnation[..],
                    &issuer.epoch.get().to_string(),
                ],
            )
            .await
            .map_err(backend)?;
        if prior.len() > 64 {
            return Err(SourceCloseError::Native(
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
                &[
                    dataset,
                    &&old_resource[..],
                    &old_epoch,
                    &&old_incarnation[..],
                ],
            )
            .await
            .map_err(backend)?;
        if completed + jobs.len() > 64 {
            return Err(SourceCloseError::Native(
                BootstrapError::InsufficientReservation,
            ));
        }
        let total = (completed + jobs.len()) as u64;
        // Job UPDATE custody can wait. Validate the entire selected batch and
        // recheck the actual current authority before any new terminal effect.
        for row in &jobs {
            fixed::<16>(row, 0)?;
            if !row.try_get::<_, bool>(1).map_err(backend)? {
                return Err(OwnerError::InvalidMetadata.into());
            }
        }
        source.validate().map_err(SourceCloseError::Native)?;
        require_current(&tx, id.dataset_id, issuer).await?;
        initial_history::check_delivery(&mut lease, epoch, input.cancelled)?;
        for row in jobs {
            initial_history::check_delivery(&mut lease, epoch, input.cancelled)?;
            let job = fixed::<16>(&row, 0)?;
            let inserted = tx
                .execute(
                    INSERT_GAP,
                    &[
                        &&job[..],
                        dataset,
                        &slot,
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
                .execute(acquisition_abandonment::REQUEST_CANCELLATION, &[&&job[..]])
                .await
                .map_err(backend)?;
            let cancelled = tx
                .execute(acquisition_abandonment::COMPLETE_CANCELLATION, &[&&job[..]])
                .await
                .map_err(backend)?;
            if (inserted, requested, cancelled) != (1, 1, 1) {
                return Err(OwnerError::InvalidMetadata.into());
            }
        }
        source.validate().map_err(SourceCloseError::Native)?;
        require_current(&tx, id.dataset_id, issuer).await?;
        initial_history::check_delivery(&mut lease, epoch, input.cancelled)?;
        tx.commit()
            .await
            .map_err(|_| SourceCloseError::CommitUnknown)?;
        initial_history::check_delivery(&mut lease, epoch, input.cancelled)?;
        if self.client.is_closed() {
            return Err(SourceCloseError::DeliveryUnknown);
        }
        self.available = true;
        Ok(total)
    }
}

async fn require_current(
    tx: &Transaction<'_>,
    dataset: DatasetId,
    current: ingest_registry::ControlOwner,
) -> Result<(), OwnerError> {
    require(tx, dataset, current).await?;
    let live: bool = tx
        .query_one(
            "SELECT lease_expires_at_us>(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint
             FROM control.owner_epochs WHERE resource_id=$1::bytea AND epoch=$2::text::numeric",
            &[
                &&current.resource_id.as_bytes()[..],
                &current.epoch.get().to_string(),
            ],
        )
        .await
        .map_err(backend)?
        .try_get(0)
        .map_err(backend)?;
    if !live {
        return Err(OwnerError::Unavailable);
    }
    Ok(())
}

// Terminal rows are immutable. A historical gap issuer need not still be live:
// current recovery authority is checked separately, and retry rewrites nothing.
const RETAINED_GAPS: &str = "
    SELECT coalesce(g.dataset_id=$2::bytea AND g.shard_id=$3::bigint AND g.source_owner_epoch=2
        AND g.reason='activated_initial_producer_abandoned' AND g.owner_role='control'
        AND g.last_complete_sequence=$4::text::numeric AND g.source_closure_digest=$5::bytea
        AND g.previous_owner_resource=$6::bytea AND g.previous_owner_epoch=$7::text::numeric
        AND g.previous_owner_incarnation=$8::bytea AND g.issuer_resource=$6::bytea
        AND g.issuer_epoch>$7::text::numeric AND g.issuer_epoch<=$9::text::numeric
        AND g.abandoned_at_us>=s.closed_at_us AND g.abandoned_at_us>=j.created_at_us
        AND g.abandoned_at_us<=(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint
        AND j.dataset_id=g.dataset_id AND j.kind='acquisition' AND j.acquisition_job_protocol=1
        AND j.input_complete AND j.state='cancelled' AND j.job_revision=g.previous_job_revision+2
        AND j.checkpoint_revision=0 AND j.result_manifest_digest IS NULL
        AND j.reason_code=g.reason AND j.completed_at_us=g.abandoned_at_us
        AND (j.owner_resource_id,j.owner_epoch,j.owner_incarnation)
            IS NOT DISTINCT FROM (g.issuer_resource,g.issuer_epoch,g.issuer_incarnation), false)
      FROM control.acquisition_gaps g LEFT JOIN control.jobs j USING(job_id)
      LEFT JOIN control.source_streams s ON s.dataset_id=g.dataset_id
          AND s.shard_id=g.shard_id AND s.owner_epoch=g.source_owner_epoch
     WHERE g.producer_slot_id=$1::bytea AND g.producer_epoch=2
     ORDER BY g.job_id LIMIT 65";

const CLAIMED_JOBS: &str = "
    SELECT substring(j.job_id FROM 1 FOR 17),
           coalesce(j.acquisition_job_protocol=1 AND j.state='queued'
               AND j.checkpoint_revision=0 AND j.input_complete
               AND NOT EXISTS (SELECT 1 FROM control.acquisition_gaps g WHERE g.job_id=j.job_id), false)
      FROM control.jobs j
     WHERE j.dataset_id=$1::bytea AND j.owner_resource_id=$2::bytea
       AND j.owner_epoch=$3::text::numeric AND j.owner_incarnation=$4::bytea
       AND j.kind='acquisition'
     ORDER BY j.job_id LIMIT 65 FOR UPDATE";

const INSERT_GAP: &str = "
    INSERT INTO control.acquisition_gaps
        (job_id,dataset_id,producer_slot_id,producer_epoch,shard_id,source_owner_epoch,
         last_complete_sequence,source_closure_digest,previous_job_revision,
         previous_owner_resource,previous_owner_epoch,previous_owner_incarnation,
         issuer_resource,issuer_epoch,issuer_incarnation,abandoned_at_us,reason)
    SELECT job_id,$2::bytea,$3::bytea,2,$4::bigint,2,$5::text::numeric,$6::bytea,job_revision,
           owner_resource_id,owner_epoch,owner_incarnation,$7::bytea,$8::text::numeric,$9::bytea,
           (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,'activated_initial_producer_abandoned'
      FROM control.jobs WHERE job_id=$1::bytea";
