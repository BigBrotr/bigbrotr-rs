//! Explicit same-Control reconciliation of a complete OPEN initial producer.
//! Missing work stays unresolved; this path neither closes nor repairs outcomes.
use super::*;
use crate::storage::rocks::store::{PendingQuotaStatus, ProducerRecoveryLimits};

pub struct InitialPendingQuotaRequest<'a> {
    pub ingest_root: &'a std::path::Path,
    pub registration: &'a ingest_registry::InitialRegistration,
    pub limits: ProducerRecoveryLimits,
    pub policy_ttl: Duration,
    pub cancelled: &'a AtomicBool,
}

impl ControlSession {
    /// Restore only logical readiness after every existing original/native/PG
    /// result agrees. All grants remain retained through their last read/await.
    /// This supplies no filesystem credit, negative proof or permission to
    /// resume an unresolved original through a different identity.
    pub async fn reconcile_initial_pending_quota(
        &mut self,
        store: &mut IngestStore,
        input: InitialPendingQuotaRequest<'_>,
        scratch: OwnedPermit,
        mut input_grant: OwnedPermit,
        mut outcome_grant: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<PendingQuotaStatus, InitialEventError> {
        if input.cancelled.load(Ordering::Relaxed) {
            return Err(InitialEventError::Native(
                AdmissionError::AuthorityUnavailable,
            ));
        }
        let spec = input.registration;
        let policy = NativeAuthority::Initial(spec).quota_policy()?;
        let mut head = store
            .begin_initial_quota_reconciliation(spec.producer_slot_id, scratch, input.cancelled)
            .map_err(InitialEventError::Recovery)?;
        let issued = head.issued_through();
        let manifest: [u8; 32] = Sha256::digest(store.manifest().encode().as_bytes()).into();
        let epoch = store.manifest().identity().policy_epoch;
        let (tx, mut lease) = self
            .begin_initial_native_operation(
                &NativeOwner::Store(&mut *store),
                InitialIngestContext {
                    ingest_root: input.ingest_root,
                    authority: NativeAuthority::Initial(spec),
                    cancelled: input.cancelled,
                    policy_ttl: input.policy_ttl,
                    request: None,
                    source: None,
                    network_event: false,
                    job: None,
                    progress: false,
                },
                validate,
            )
            .await?;
        let producer = tx
            .query_one(
                "SELECT state='open',left(reserved_through_request::text,21)
             FROM control.producer_epochs WHERE dataset_id=$1::bytea
               AND producer_slot_id=$2::bytea AND producer_epoch=1 FOR UPDATE",
                &[
                    &&spec.dataset_id.as_bytes()[..],
                    &&spec.producer_slot_id.as_bytes()[..],
                ],
            )
            .await
            .map_err(backend)?;
        if !producer.try_get::<_, bool>(0).map_err(backend)?
            || ingest_registry::decimal(producer.try_get(1).map_err(backend)?)
                .map_err(|_| OwnerError::InvalidMetadata)?
                != issued
        {
            return Err(OwnerError::InvalidMetadata.into());
        }
        drop(producer);
        // The native proof is bounded by64. Inspect complete PG sets as well,
        // including extras: a watermark alone cannot hide an unresolved tail.
        // Dataset is a checked row identity, never a filter that hides corrupt
        // foreign-dataset rows belonging to this same producer slot/epoch.
        for table in ["producer_requests", "producer_outcomes"] {
            let rows = tx
                .query(
                    &format!(
                        "SELECT dataset_id=$1::bytea,left(request_sequence::text,21) FROM control.{table}
                    WHERE producer_slot_id=$2::bytea
                    AND producer_epoch=1 ORDER BY request_sequence LIMIT 65"
                    ),
                    &[
                        &&spec.dataset_id.as_bytes()[..],
                        &&spec.producer_slot_id.as_bytes()[..],
                    ],
                )
                .await
                .map_err(backend)?;
            if rows.len() != issued as usize {
                return Err(OwnerError::InvalidMetadata.into());
            }
            for (index, row) in rows.iter().enumerate() {
                if !row.try_get::<_, bool>(0).map_err(backend)?
                    || ingest_registry::decimal(row.try_get(1).map_err(backend)?)
                        .map_err(|_| OwnerError::InvalidMetadata)?
                        != index as u64 + 1
                {
                    return Err(OwnerError::InvalidMetadata.into());
                }
            }
        }
        for sequence in 1..=issued {
            if input.cancelled.load(Ordering::Relaxed) || lease.check_delivery(epoch).is_err() {
                return Err(OwnerError::Unavailable.into());
            }
            let request = ProducerRequestId {
                producer_slot_id: spec.producer_slot_id,
                producer_epoch: ProducerEpoch::new(1).map_err(|_| OwnerError::InvalidInput)?,
                request_sequence: RequestSequence::new(sequence)
                    .map_err(|_| OwnerError::InvalidInput)?,
            };
            let reservation = producer_requests::read(&tx, spec.dataset_id, request)
                .await
                .map_err(InitialEventError::Reservation)?
                .ok_or(OwnerError::InvalidMetadata)?;
            if reservation.manifest_sha256 != manifest {
                return Err(OwnerError::InvalidMetadata.into());
            }
            let original = store
                .read_initial_quota_original(
                    &head,
                    request,
                    reservation.digest,
                    input.limits,
                    input_grant,
                    outcome_grant,
                    input.cancelled,
                )
                .map_err(InitialEventError::Recovery)?;
            producer_outcomes::require_quota_original(&tx, &reservation, &original)
                .await
                .map_err(InitialEventError::Outcome)?;
            (input_grant, outcome_grant) =
                head.record(original).map_err(InitialEventError::Recovery)?;
        }
        drop((input_grant, outcome_grant));
        if input.cancelled.load(Ordering::Relaxed)
            || store.native_unavailable()
            || lease.check_delivery(epoch).is_err()
        {
            return Err(OwnerError::Unavailable.into());
        }
        // No application rows were written. A known rollback must precede the
        // local readiness transition; uncertain cleanup grants no new intake.
        tx.rollback()
            .await
            .map_err(|_| InitialEventError::DeliveryUnknown)?;
        if self.client.is_closed()
            || input.cancelled.load(Ordering::Relaxed)
            || lease.check_delivery(epoch).is_err()
        {
            return Err(InitialEventError::DeliveryUnknown);
        }
        store
            .finish_initial_quota_reconciliation(head, policy, input.cancelled)
            .map_err(InitialEventError::Recovery)?;
        if self.client.is_closed()
            || input.cancelled.load(Ordering::Relaxed)
            || store.native_unavailable()
            || lease.check_delivery(epoch).is_err()
        {
            return Err(InitialEventError::DeliveryUnknown);
        }
        self.available = true;
        Ok(store.pending_quota_status())
    }
}
