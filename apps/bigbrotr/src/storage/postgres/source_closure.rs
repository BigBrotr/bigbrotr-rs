//! Publish a positive native source bound without retiring its producer or owner.
use super::*;
use crate::runtime::OwnedPermit;
use crate::storage::rocks::store::{BootstrapError, ResolvedSource};
use std::sync::atomic::Ordering;

pub type SourceCloseError = ProducerOperationError<BootstrapError>;

impl ControlSession {
    pub async fn close_initial_source(
        &mut self,
        source: &ResolvedSource<'_>,
        input: ProducerCloseRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<(), SourceCloseError> {
        // At most 64 fixed digest rows plus PG row/parameter staging. No original
        // input, outcome bytes or whole-source payload is copied into this query.
        if permit.request().units().data_bytes() < 64 * 1024
            || permit.request().units().items() == 0
        {
            return Err(SourceCloseError::Native(
                BootstrapError::InsufficientReservation,
            ));
        }
        let _reservation = permit;
        if input.cancelled.load(Ordering::Relaxed) {
            return Err(SourceCloseError::Native(
                BootstrapError::AuthorityUnavailable,
            ));
        }
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
                ingest_registry::RegistryLocks::Source,
                validate,
            )
            .await?;
        require_resolved_prefix(&tx, source, spec).await?;
        let dataset = &spec.dataset_id.as_bytes().as_slice();
        let shard = i64::from(spec.shard_id);
        // UPDATE custody was acquired by the registry before producer UPDATE;
        // existing job/frontier guards serialize on this same source row.
        let row = tx
            .query_one(
                "
            SELECT state, last_complete_sequence::text, closure_digest
              FROM control.source_streams
             WHERE dataset_id = $1::bytea AND shard_id = $2::bigint AND owner_epoch = 1",
                &[dataset, &shard],
            )
            .await
            .map_err(backend)?;
        match row.try_get::<_, &str>(0).map_err(backend)? {
            "closed" => {
                let bound = row
                    .try_get::<_, Option<&str>>(1)
                    .map_err(backend)?
                    .map(ingest_registry::decimal)
                    .transpose()
                    .map_err(SourceCloseError::Registry)?;
                let digest = row.try_get::<_, Option<&[u8]>>(2).map_err(backend)?;
                if bound != Some(source.last_complete_sequence())
                    || digest != Some(&source.digest()[..])
                {
                    return Err(OwnerError::InvalidMetadata.into());
                }
            }
            state @ ("open" | "closing") => {
                if state == "open" {
                    tx.execute(
                        "UPDATE control.source_streams SET state = 'closing'
                        WHERE dataset_id = $1::bytea AND shard_id = $2::bigint AND owner_epoch = 1",
                        &[dataset, &shard],
                    )
                    .await
                    .map_err(backend)?;
                }
                const CLOSE_SOURCE: &str = "
                    UPDATE control.source_streams
                       SET state = 'closed',
                           closed_at_us = (EXTRACT(EPOCH FROM clock_timestamp()) * 1000000)::bigint,
                           last_complete_sequence = $3::text::numeric,
                           closure_digest = $4::bytea
                     WHERE dataset_id = $1::bytea AND shard_id = $2::bigint AND owner_epoch = 1";
                let bound = source.last_complete_sequence().to_string();
                let digest = source.digest();
                tx.execute(CLOSE_SOURCE, &[dataset, &shard, &bound, &&digest[..]])
                    .await
                    .map_err(backend)?;
            }
            _ => return Err(OwnerError::InvalidMetadata.into()),
        }
        if input.cancelled.load(Ordering::Relaxed) || lease.check_delivery(epoch).is_err() {
            return Err(SourceCloseError::DeliveryUnknown);
        }
        tx.commit()
            .await
            .map_err(|_| SourceCloseError::CommitUnknown)?;
        if input.cancelled.load(Ordering::Relaxed)
            || self.client.is_closed()
            || lease.check_delivery(epoch).is_err()
        {
            return Err(SourceCloseError::DeliveryUnknown);
        }
        self.available = true;
        Ok(())
    }
}

/// Recheck the complete SQL outcome prefix against the retained native witness.
pub(super) async fn require_resolved_prefix(
    tx: &Transaction<'_>,
    source: &ResolvedSource<'_>,
    spec: &ingest_registry::InitialRegistration,
) -> Result<(), SourceCloseError> {
    let slot = &spec.producer_slot_id.as_bytes().as_slice();
    let resolved = source.resolved_requests().to_string();
    const CLOSED_PRODUCER: &str = "
        SELECT state = 'closing' AND reserved_through_request = $2::text::numeric
          FROM control.producer_epochs
         WHERE producer_slot_id = $1::bytea AND producer_epoch = 1";
    let row = tx
        .query_one(CLOSED_PRODUCER, &[slot, &resolved])
        .await
        .map_err(backend)?;
    if !row.try_get::<_, bool>(0).map_err(backend)? {
        return Err(OwnerError::InvalidMetadata.into());
    }
    let manifest: [u8; 32] =
        Sha256::digest(source.recovery().manifest().encode().as_bytes()).into();
    const OUTCOMES: &str = "
        SELECT left(r.request_sequence::text,21),
               substring(r.request_digest FROM 1 FOR 33),
               substring(o.outcome_sha256 FROM 1 FOR 33)
          FROM control.producer_requests r
          JOIN control.producer_outcomes o USING (producer_slot_id, producer_epoch, request_sequence)
         WHERE r.producer_slot_id = $1::bytea AND r.producer_epoch = 1
           AND r.dataset_id = $2::bytea AND o.dataset_id = r.dataset_id
           AND r.manifest_sha256 = $3::bytea AND o.manifest_sha256 = r.manifest_sha256
           AND o.request_digest = r.request_digest
         ORDER BY r.request_sequence LIMIT 65";
    let rows = tx
        .query(
            OUTCOMES,
            &[slot, &&spec.dataset_id.as_bytes()[..], &&manifest[..]],
        )
        .await
        .map_err(backend)?;
    if rows.len() as u64 != source.resolved_requests() {
        return Err(OwnerError::InvalidMetadata.into());
    }
    let mut recorded = Vec::new();
    recorded
        .try_reserve_exact(rows.len())
        .map_err(|_| SourceCloseError::Native(BootstrapError::Allocation))?;
    for row in rows {
        let sequence = ingest_registry::decimal(row.try_get(0).map_err(backend)?)
            .map_err(SourceCloseError::Registry)?;
        let fixed = |column: usize| -> Result<[u8; 32], OwnerError> {
            row.try_get::<_, &[u8]>(column)
                .map_err(backend)?
                .try_into()
                .map_err(|_| OwnerError::InvalidMetadata)
        };
        recorded.push((sequence, fixed(1)?, fixed(2)?));
    }
    if !source.matches_recorded(recorded) {
        return Err(OwnerError::InvalidMetadata.into());
    }
    Ok(())
}
