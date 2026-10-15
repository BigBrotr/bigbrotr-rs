//! Whole-intent acquisition uncertainty for a positively resolved, already
//! closed successor source. This transfers only terminal SQL writes; it neither
//! adopts old execution nor resolves import or retirement obligations.
use super::*;

const JOB_DATA_BYTES: u64 = 128 * 1024;
const MAX_JOBS: usize = 64;

impl ControlSession {
    /// Consume a fresh source-purpose proof. Publishing source closure spends
    /// the same owner, so callers must inspect/replay again before abandonment.
    /// Exact retries preserve every retained gap and its historical issuer.
    pub async fn abandon_activated_successor_acquisition(
        &mut self,
        source: ResolvedActivatedSuccessorSource<'_>,
        input: InitialHistoryRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<u64, ProducerResolveError> {
        source.owner.publication.start()?;
        if permit.request().units().data_bytes() < JOB_DATA_BYTES
            || permit.request().units().items() == 0
        {
            return Err(native_error(BootstrapError::InsufficientReservation));
        }
        // This separate grant retains at most65 fixed PG row projections. The
        // prior-gap result is dropped before job selection; no history Vec or
        // native allocation is copied or transferred into this operation.
        let _permit = permit;
        let owner = &source.owner.inner;
        let proof = &source.inner;
        require_closed_source(&owner.head, &proof.last_epoch())?;
        if !proof.matches_recovery(owner) || !owner.is_closed() {
            return Err(OwnerError::InvalidInput.into());
        }
        let issuer = self.identity;
        let (tx, mut lease, policy) = self
            .begin_checked_successor_recovery(proof.recovery, owner, &input, validate)
            .await?;
        proof.validate(input.cancelled).map_err(native_error)?;
        require_head_summary(&proof.last_epoch(), owner)?;
        require_complete(
            owner.issued_through(),
            owner.reserved_through(),
            owner.staged_input().is_some(),
        )?;
        require_recorded_recovery(
            &tx,
            proof.recovery,
            owner,
            &proof.native,
            &mut lease,
            policy,
            input.cancelled,
        )
        .await?;

        let id = owner.head.manifest.identity();
        let dataset = &id.dataset_id.as_bytes().as_slice();
        let slot_id = owner.producer_slot_id();
        let slot = slot_id.as_bytes().as_slice();
        let epoch = owner.epoch().to_string();
        let shard = i64::from(id.shard_id);
        let summary = proof.last_epoch();
        let bound = summary.last_complete_sequence.to_string();
        let digest = summary.digest;
        // begin_checked compared the complete immutable activation to intent,
        // including this actual activating Control tuple. It is neither the
        // producer-slot registrar nor the target Ingest process incarnation.
        let activator = owner.intent.preparer;
        let old_resource = activator.resource_id.as_bytes().as_slice();
        let old_epoch = activator.epoch.get().to_string();
        let old_incarnation = activator.process_incarnation.as_bytes().as_slice();
        let issuer_epoch = issuer.epoch.get().to_string();
        let prior = tx
            .query(
                RETAINED_GAPS,
                &[
                    &slot,
                    &epoch,
                    dataset,
                    &shard,
                    &bound,
                    &&digest[..],
                    &old_resource,
                    &old_epoch,
                    &old_incarnation,
                    &issuer_epoch,
                ],
            )
            .await
            .map_err(backend)?;
        checked_total(prior.len(), 0)?;
        for row in &prior {
            if !row.try_get::<_, bool>(0).map_err(backend)? {
                return Err(OwnerError::InvalidMetadata.into());
            }
        }
        let completed = prior.len();
        drop(prior);
        // All activator-owned acquisitions are selected, even those with zero
        // producer requests. Accepted unclaimed and other-lifetime jobs stay out.
        let jobs = tx
            .query(
                CLAIMED_JOBS,
                &[dataset, &old_resource, &old_epoch, &old_incarnation],
            )
            .await
            .map_err(backend)?;
        let total = checked_total(completed, jobs.len())?;
        for row in &jobs {
            fixed::<16>(row, 0)?;
            if !row.try_get::<_, bool>(1).map_err(backend)? {
                return Err(OwnerError::InvalidMetadata.into());
            }
        }
        // Ordered job UPDATE custody can wait. Check the entire batch before
        // the first INSERT, then sample actual current UTC after those waits.
        proof.validate(input.cancelled).map_err(native_error)?;
        rows::require_live(&tx, issuer, None).await?;
        initial_history::check_delivery(&mut lease, policy, input.cancelled)?;
        for row in jobs {
            initial_history::check_delivery(&mut lease, policy, input.cancelled)?;
            let job = fixed::<16>(&row, 0)?;
            let inserted = tx
                .execute(
                    INSERT_GAP,
                    &[
                        &&job[..],
                        dataset,
                        &slot,
                        &epoch,
                        &shard,
                        &bound,
                        &&digest[..],
                        &&issuer.resource_id.as_bytes()[..],
                        &issuer_epoch,
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
        // No mutation of the source/activation/producer/native context is
        // allowed. SQL gap guards recheck issuer authority; this final check
        // also covers the no-op retry, whose old issuer need no longer be live.
        proof.validate(input.cancelled).map_err(native_error)?;
        rows::require_live(&tx, issuer, None).await?;
        initial_history::check_delivery(&mut lease, policy, input.cancelled)?;
        tx.commit()
            .await
            .map_err(|_| ProducerResolveError::CommitUnknown)?;
        proof.validate(input.cancelled).map_err(native_error)?;
        initial_history::check_delivery(&mut lease, policy, input.cancelled)?;
        if self.client.is_closed() {
            return Err(ProducerResolveError::DeliveryUnknown);
        }
        self.available = true;
        Ok(total)
    }
}

fn require_closed_source(head: &Epoch, summary: &LineageEpochSummary) -> Result<(), OwnerError> {
    if !(3..=255).contains(&head.epoch())
        || !head.activated
        || head.producer != ProducerState::Closing
        || head.source != SourceState::Closed
        || summary.epoch != head.epoch()
        || summary.terminal_kind != RetainedTerminalKind::Activated
        || summary.resolved_requests != head.reserved
        || head.source_cut != Some(summary.last_complete_sequence)
        || head.source_digest != Some(summary.digest)
    {
        return Err(OwnerError::InvalidMetadata);
    }
    Ok(())
}
fn checked_total(prior: usize, selected: usize) -> Result<u64, ProducerResolveError> {
    prior
        .checked_add(selected)
        .filter(|count| *count <= MAX_JOBS)
        .map(|count| count as u64)
        .ok_or_else(|| native_error(BootstrapError::InsufficientReservation))
}

// Indexed bounded immutable history. Historical issuer liveness is deliberately
// absent: the new actual issuer is checked independently after every lock wait.
const RETAINED_GAPS: &str = "
    SELECT coalesce(g.dataset_id=$3::bytea AND g.shard_id=$4::bigint AND g.source_owner_epoch=$2::text::numeric
        AND g.reason='ingest_successor_abandoned' AND g.owner_role='control'
        AND g.last_complete_sequence=$5::text::numeric AND g.source_closure_digest=$6::bytea
        AND g.previous_owner_resource=$7::bytea AND g.previous_owner_epoch=$8::text::numeric
        AND g.previous_owner_incarnation=$9::bytea AND g.issuer_resource=$7::bytea
        AND g.issuer_epoch>$8::text::numeric AND g.issuer_epoch<=$10::text::numeric
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
     WHERE g.producer_slot_id=$1::bytea AND g.producer_epoch=$2::text::numeric
     ORDER BY g.job_id LIMIT 65";

const CLAIMED_JOBS: &str = "
    SELECT substring(j.job_id FROM 1 FOR 17),
           coalesce(j.acquisition_job_protocol=1 AND j.state='queued'
               AND j.checkpoint_revision=0 AND j.input_complete AND j.result_manifest_digest IS NULL
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
    SELECT job_id,$2::bytea,$3::bytea,$4::text::numeric,$5::bigint,$4::text::numeric,$6::text::numeric,$7::bytea,job_revision,
           owner_resource_id,owner_epoch,owner_incarnation,$8::bytea,$9::text::numeric,$10::bytea,
           (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,'ingest_successor_abandoned'
      FROM control.jobs WHERE job_id=$1::bytea";

#[cfg(test)]
#[path = "successor_acquisition_tests.rs"]
mod tests;
