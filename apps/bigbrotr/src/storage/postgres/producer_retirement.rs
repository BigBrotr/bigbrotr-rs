//! Initial retirement after explicit source closure and workflow abandonment.
//! Native state remains closing with all input/receipt detail retained.
use super::super::{producer_outcomes::OutcomeError, producer_requests};
use super::*;
use crate::{
    runtime::{OwnedPermit, policy_authority::PolicyLeaseTracker},
    storage::rocks::store::{
        AdmissionError, BootstrapError, IngestRecovery, ProducerRecoveryError,
        ResolvedProducerRequest, ResolvedSource,
    },
};
use bigbrotr_model::{PolicyEpoch, ProducerEpoch};
use std::sync::atomic::{AtomicBool, Ordering};

impl ControlSession {
    /// Observes a pre-existing native barrier. No native or historical PG row
    /// is repaired here; the same path is valid after the final retirement COMMIT.
    pub async fn observe_initial_retirement(
        &mut self,
        recovery: &IngestRecovery,
        input: ProducerCloseRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<InitialProducerClosing, ProducerCloseError> {
        let spec = input.registration;
        let (tx, mut lease, epoch) = self
            .begin_producer_recovery(
                recovery,
                input.ingest_root,
                spec,
                input.policy_ttl,
                ingest_registry::RegistryLocks::Retirement,
                validate,
            )
            .await?;
        let row = tx
            .query_one(
                "SELECT state IN ('closing','retired'), reserved_through_request::text
             FROM control.producer_epochs WHERE producer_slot_id=$1::bytea AND producer_epoch=1",
                &[&&spec.producer_slot_id.as_bytes()[..]],
            )
            .await
            .map_err(backend)?;
        if !row.try_get::<_, bool>(0).map_err(backend)? {
            return Err(OwnerError::InvalidMetadata.into());
        }
        let reserved = ingest_registry::decimal(row.try_get(1).map_err(backend)?)
            .map_err(ProducerCloseError::Registry)?;
        if reserved > 64 {
            return Err(ProducerCloseError::Native(
                BootstrapError::InsufficientReservation,
            ));
        }
        check_delivery(&mut lease, epoch, input.cancelled)?;
        producer_progress::recovery::require(
            &tx,
            recovery,
            recovery.manifest(),
            spec.producer_slot_id,
            &permit,
            || producer_progress::recovery::authority(&mut lease, epoch, input.cancelled),
        )
        .await?;
        let native = recovery
            .observe_closed_producer(
                spec.producer_slot_id,
                ProducerEpoch::new(1).expect("initial epoch"),
                permit,
            )
            .map_err(ProducerCloseError::Native)?;
        if native.issued_through() != reserved {
            return Err(OwnerError::InvalidMetadata.into());
        }
        finish(tx, &mut lease, epoch, input.cancelled).await?;
        if self.client.is_closed() {
            return Err(ProducerCloseError::DeliveryUnknown);
        }
        self.available = true;
        Ok(InitialProducerClosing {
            native,
            reserved_through: reserved,
        })
    }

    /// Revalidate the original native result and an already existing exact PG
    /// copy. Missing copies refuse even before retirement; this never inserts.
    pub async fn inspect_initial_retirement_request(
        &mut self,
        recovery: &IngestRecovery,
        input: ProducerResolveRequest<'_>,
        input_permit: OwnedPermit,
        outcome_permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<ResolvedProducerRequest, ProducerResolveError> {
        if input.request.producer_slot_id != input.registration.producer_slot_id
            || input.request.producer_epoch.get() != 1
        {
            return Err(OwnerError::InvalidInput.into());
        }
        let (tx, mut lease, epoch) = self
            .begin_producer_recovery(
                recovery,
                input.ingest_root,
                input.registration,
                input.policy_ttl,
                ingest_registry::RegistryLocks::Retirement,
                validate,
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
            return Err(OwnerError::InvalidMetadata.into());
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
        if resolved.input_bytes() != reservation.input_bytes {
            return Err(ProducerResolveError::Outcome(OutcomeError::Conflict));
        }
        require_retirement_copy(&tx, &resolved, input.request, manifest).await?;
        finish(tx, &mut lease, epoch, input.cancelled).await?;
        if self.client.is_closed() {
            return Err(ProducerResolveError::DeliveryUnknown);
        }
        self.available = true;
        Ok(resolved)
    }

    /// One SQL transaction retires the initial producer/domains and releases
    /// Ingest. An exact completed result is observed without rewriting history.
    pub async fn retire_initial_producer(
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
        if source.producer().producer_slot_id() != spec.producer_slot_id
            || source.producer().producer_epoch().get() != 1
            || source.resolved_requests() > 64
        {
            return Err(OwnerError::InvalidInput.into());
        }
        let (tx, mut lease, epoch) = self
            .begin_producer_recovery(
                source.recovery(),
                input.ingest_root,
                spec,
                input.policy_ttl,
                ingest_registry::RegistryLocks::Retirement,
                validate,
            )
            .await?;
        source_closure::require_recorded_outcomes(&tx, source, spec).await?;
        producer_progress::recovery::require(
            &tx,
            source.recovery(),
            source.producer().manifest(),
            spec.producer_slot_id,
            &_reservation,
            || producer_progress::recovery::authority(&mut lease, epoch, input.cancelled),
        )
        .await?;
        let slot = &spec.producer_slot_id.as_bytes().as_slice();
        let dataset = &spec.dataset_id.as_bytes().as_slice();
        let resource = &spec.ingest_resource_id.as_bytes().as_slice();
        let role = &spec.ingest_role_instance_id.as_slice();
        let digest = source.digest();
        // Enforce the native Event classification at the final effect boundary,
        // including a witness built through ordinary positive recovery.
        // Order the numeric column, not the text projection's output name.
        let requests = tx
            .query(
                "SELECT r.request_sequence::text, r.acquisition_job_id IS NOT NULL
             FROM control.producer_requests r WHERE r.producer_slot_id=$1::bytea
                 AND r.producer_epoch=1 ORDER BY r.request_sequence LIMIT 65",
                &[slot],
            )
            .await
            .map_err(backend)?;
        if requests.len() as u64 != source.resolved_requests() {
            return Err(OwnerError::InvalidMetadata.into());
        }
        for (offset, row) in requests.iter().enumerate() {
            let sequence = ingest_registry::decimal(row.try_get(0).map_err(backend)?)
                .map_err(ProducerCloseError::Registry)?;
            if sequence != offset as u64 + 1
                || (source.event_request_mask() & (1_u64 << offset) != 0
                    && !row.try_get::<_, bool>(1).map_err(backend)?)
            {
                return Err(OwnerError::InvalidMetadata.into());
            }
        }
        drop(requests);
        let gaps: i64 = tx
            .query_one(
                "SELECT control.require_initial_retirement($1::bytea,1,$2::bytea)",
                &[slot, &&digest[..]],
            )
            .await
            .map_err(backend)?
            .try_get(0)
            .map_err(backend)?;
        let row = tx
            .query_one(
                RETIREMENT_STATE,
                &[
                    slot,
                    resource,
                    role,
                    dataset,
                    &&digest[..],
                    &source.resolved_requests().to_string(),
                    &i64::from(spec.shard_id),
                    &source.last_complete_sequence().to_string(),
                ],
            )
            .await
            .map_err(backend)?;
        let prepared: bool = row.try_get(0).map_err(backend)?;
        let complete: bool = row.try_get(1).map_err(backend)?;
        if !prepared && !complete {
            return Err(OwnerError::InvalidMetadata.into());
        }
        if prepared {
            let clock = tx
                .query_one(
                    "SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,
                            closed_at_us FROM control.source_streams
                     WHERE dataset_id=$1::bytea AND shard_id=$2::bigint AND owner_epoch=1",
                    &[dataset, &i64::from(spec.shard_id)],
                )
                .await
                .map_err(backend)?;
            let now: i64 = clock.try_get(0).map_err(backend)?;
            let closed_at: i64 = clock.try_get(1).map_err(backend)?;
            // Wall clocks can regress between independent operations. Never
            // commit a terminal shape that our completed retry would refuse.
            if now < closed_at {
                return Err(OwnerError::InvalidMetadata.into());
            }
            let changed = tx
                .execute(
                    "UPDATE control.producer_epochs SET state='retired',retired_at_us=$2::bigint,
                    closure_digest=$3::bytea WHERE producer_slot_id=$1::bytea AND producer_epoch=1",
                    &[slot, &now, &&digest[..]],
                )
                .await
                .map_err(backend)?;
            let domains = tx
                .execute(
                    "UPDATE control.allocation_domains SET state='closed',closed_at_us=$3::bigint,
                    closure_digest=$4::bytea WHERE dataset_id=$1::bytea
                    AND owner_resource_id=$2::bytea AND owner_epoch=1",
                    &[dataset, resource, &now, &&digest[..]],
                )
                .await
                .map_err(backend)?;
            let owner = tx
                .execute(
                    "UPDATE control.owner_epochs SET state='released',released_at_us=$2::bigint,
                    exclusion_method='storage_fenced',exclusion_digest=$3::bytea
                    WHERE resource_id=$1::bytea AND epoch=1",
                    &[resource, &now, &&digest[..]],
                )
                .await
                .map_err(backend)?;
            let retired_role = tx
                .execute(
                    "UPDATE control.role_instances SET retired_at_us=$2::bigint
                    WHERE role_instance_id=$1::bytea",
                    &[role, &now],
                )
                .await
                .map_err(backend)?;
            if (changed, domains, owner, retired_role)
                != (1, spec.allocation_domains.len() as u64, 1, 1)
            {
                return Err(OwnerError::InvalidMetadata.into());
            }
        }
        finish(tx, &mut lease, epoch, input.cancelled).await?;
        if self.client.is_closed() {
            return Err(ProducerCloseError::DeliveryUnknown);
        }
        self.available = true;
        u64::try_from(gaps).map_err(|_| OwnerError::InvalidMetadata.into())
    }
}

/// Shared exact immutable PG copy comparison for current and historical native
/// contexts. The caller supplies the already validated original manifest digest.
pub(super) async fn require_retirement_copy(
    tx: &Transaction<'_>,
    resolved: &ResolvedProducerRequest,
    request: bigbrotr_model::ProducerRequestId,
    manifest: [u8; 32],
) -> Result<(), ProducerResolveError> {
    let copy = resolved.native_outcome();
    let digest: [u8; 32] = Sha256::digest(copy.bytes()).into();
    const EXISTING_COPY: &str = "
        SELECT o.dataset_id=$4::bytea AND o.request_digest=$5::bytea
               AND o.manifest_sha256=$6::bytea AND o.outcome_kind=$7::smallint
               AND o.policy_epoch=$8::text::numeric AND o.outcome_bytes=$9::bytea
               AND o.outcome_sha256=$10::bytea,
               r.acquisition_job_id IS NOT NULL
          FROM control.producer_outcomes o JOIN control.producer_requests r
            USING (producer_slot_id,producer_epoch,request_sequence)
         WHERE o.producer_slot_id=$1::bytea AND o.producer_epoch=$2::text::numeric
               AND o.request_sequence=$3::text::numeric";
    let row = tx
        .query_opt(
            EXISTING_COPY,
            &[
                &&request.producer_slot_id.as_bytes()[..],
                &request.producer_epoch.get().to_string(),
                &request.request_sequence.get().to_string(),
                &&copy.dataset().as_bytes()[..],
                &&copy.digest().as_bytes()[..],
                &&manifest[..],
                &(copy.kind() as i16),
                &copy.policy_epoch().get().to_string(),
                &copy.bytes(),
                &&digest[..],
            ],
        )
        .await
        .map_err(backend)?
        .ok_or(ProducerResolveError::Outcome(OutcomeError::Conflict))?;
    // Old Event inputs can be entirely unbound, unlike dictionary requests.
    // Their native classification, not nullable SQL fields, selects this veto.
    if !row.try_get::<_, bool>(0).map_err(backend)?
        || (resolved.is_event() && !row.try_get::<_, bool>(1).map_err(backend)?)
    {
        return Err(ProducerResolveError::Outcome(OutcomeError::Conflict));
    }
    Ok(())
}

fn check_delivery<E>(
    lease: &mut PolicyLeaseTracker,
    epoch: PolicyEpoch,
    cancelled: &AtomicBool,
) -> Result<(), ProducerOperationError<E>> {
    if cancelled.load(Ordering::Relaxed) || lease.check_delivery(epoch).is_err() {
        Err(ProducerOperationError::DeliveryUnknown)
    } else {
        Ok(())
    }
}

pub(super) async fn finish<E>(
    tx: Transaction<'_>,
    lease: &mut PolicyLeaseTracker,
    epoch: PolicyEpoch,
    cancelled: &AtomicBool,
) -> Result<(), ProducerOperationError<E>> {
    check_delivery(lease, epoch, cancelled)?;
    tx.commit()
        .await
        .map_err(|_| ProducerOperationError::CommitUnknown)?;
    check_delivery(lease, epoch, cancelled)
}

// Registry identity/set comparison already locked every row. Source and request
// cuts are compared here as well as in the native outcome witness. No mixed state
// is repaired opportunistically; the operation writes either the entire initial
// transition or nothing, and a retry preserves original timestamps and attribution.
pub(super) const RETIREMENT_STATE: &str = "
    SELECT (p.state='closing' AND p.reserved_through_request=$6::text::numeric
               AND i.state IN ('active','draining') AND r.retired_at_us IS NULL
               AND s.state='closed' AND s.closure_digest=$5::bytea
               AND s.last_complete_sequence=$8::text::numeric
               AND NOT EXISTS (SELECT 1 FROM control.allocation_domains
                   WHERE dataset_id=$4::bytea AND owner_resource_id=$2::bytea AND state<>'active')) IS TRUE,
           (p.state='retired' AND p.reserved_through_request=$6::text::numeric
               AND p.closure_digest=$5::bytea AND i.state='released'
               AND i.exclusion_method='storage_fenced' AND i.exclusion_digest=$5::bytea
               AND i.released_at_us=p.retired_at_us AND r.retired_at_us=p.retired_at_us
               AND s.state='closed' AND s.closure_digest=$5::bytea
               AND s.last_complete_sequence=$8::text::numeric
               AND s.closed_at_us<=p.retired_at_us
               AND NOT EXISTS (SELECT 1 FROM control.allocation_domains
                   WHERE dataset_id=$4::bytea AND owner_resource_id=$2::bytea
                       AND (state<>'closed' OR closure_digest IS DISTINCT FROM p.closure_digest
                           OR closed_at_us IS DISTINCT FROM p.retired_at_us))) IS TRUE
      FROM control.producer_epochs p
      JOIN control.owner_epochs i ON i.resource_id=$2::bytea AND i.epoch=1
      JOIN control.role_instances r ON r.role_instance_id=$3::bytea
      JOIN control.source_streams s ON s.dataset_id=$4::bytea AND s.shard_id=$7::bigint AND s.owner_epoch=1
     WHERE p.producer_slot_id=$1::bytea AND p.producer_epoch=1";
