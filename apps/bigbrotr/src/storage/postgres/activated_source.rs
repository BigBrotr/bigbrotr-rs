//! Publish a complete activated source prefix without retiring any lifetime.
//! Only these opaque wrappers select the source-purpose registry classifier.
use super::*;
use crate::{
    runtime::OwnedPermit,
    storage::rocks::store::{
        ActivatedSourceResolution, BootstrapError, IngestRecovery, ResolvedActivatedSource,
        ResolvedProducerRequest,
    },
};
use bigbrotr_model::ProducerSlotId;

pub struct ActivatedInitialSourceRecovery(ActivatedInitialRecovery);
pub struct ClosedActivatedInitialSource(ClosedActivatedInitialProducer);

impl ActivatedInitialSourceRecovery {
    pub fn producer_slot_id(&self) -> ProducerSlotId {
        self.0.producer_slot_id()
    }
    pub fn issued_through(&self) -> u64 {
        self.0.issued_through()
    }
    pub fn reserved_through(&self) -> u64 {
        self.0.reserved_through()
    }
}
impl ClosedActivatedInitialSource {
    pub fn producer_slot_id(&self) -> ProducerSlotId {
        self.0.producer_slot_id()
    }
    pub fn issued_through(&self) -> u64 {
        self.0.issued_through()
    }
    pub fn reserved_through(&self) -> u64 {
        self.0.reserved_through()
    }
    /// The accumulator borrows this exact closed context and its retained grant.
    /// It cannot be combined with a different native owner or registry wrapper.
    pub fn source_resolution<'a>(
        &'a self,
        recovery: &'a IngestRecovery,
    ) -> Result<ActivatedSourceResolution<'a>, BootstrapError> {
        self.0
            .native
            .source_resolution(recovery, self.reserved_through())
    }
}

impl ControlSession {
    pub async fn inspect_activated_initial_source(
        &mut self,
        recovery: &IngestRecovery,
        input: InitialHistoryRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<ActivatedInitialSourceRecovery, InitialSuccessionError> {
        self.inspect_activated_producer(
            recovery,
            input,
            permit,
            activated_recovery::Purpose::Source,
            validate,
        )
        .await
        .map(ActivatedInitialSourceRecovery)
    }

    pub async fn close_activated_initial_source_producer(
        &mut self,
        recovery: &mut IngestRecovery,
        inspected: ActivatedInitialSourceRecovery,
        input: InitialHistoryRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<ClosedActivatedInitialSource, InitialSuccessionError> {
        self.close_activated_initial_producer(recovery, inspected.0, input, permit, validate)
            .await
            .map(ClosedActivatedInitialSource)
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "distinct closed context and per-original input/outcome grants"
    )]
    pub async fn resolve_activated_initial_source_request(
        &mut self,
        recovery: &IngestRecovery,
        closed: &ClosedActivatedInitialSource,
        input: ActivatedInitialResolveRequest<'_>,
        input_permit: OwnedPermit,
        outcome_permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<ResolvedProducerRequest, ProducerResolveError> {
        self.resolve_activated_initial_request(
            recovery,
            &closed.0,
            input,
            input_permit,
            outcome_permit,
            validate,
        )
        .await
    }

    /// Compare every recorded digest with the native proof under current Control
    /// and source UPDATE custody. No native write, terminal owner transition or
    /// acquisition abandonment is performed by source publication.
    pub async fn close_activated_initial_source(
        &mut self,
        closed: &ClosedActivatedInitialSource,
        source: &ResolvedActivatedSource<'_>,
        input: InitialHistoryRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<(), SourceCloseError> {
        if permit.request().units().data_bytes() < 64 * 1024
            || permit.request().units().items() == 0
        {
            return Err(SourceCloseError::Native(
                BootstrapError::InsufficientReservation,
            ));
        }
        let _permit = permit;
        if !source.matches_closed(&closed.0.native)
            || source.producer_slot_id() != closed.producer_slot_id()
            || source.resolved_requests() != closed.reserved_through()
            || source.resolved_requests() != closed.issued_through()
        {
            return Err(OwnerError::InvalidInput.into());
        }
        source.validate().map_err(SourceCloseError::Native)?;
        activated_recovery::require_deployment(&closed.0.metadata, self)?;
        let current = self.identity;
        let (tx, mut lease, epoch) = self
            .begin_initial_history_locked(source.recovery(), &input, 2, 35, validate)
            .await?;
        let state =
            activated_recovery::require_metadata(&tx, &closed.0.metadata, current, epoch).await?;
        if !state.producer_closing {
            return Err(OwnerError::InvalidMetadata.into());
        }
        source.validate().map_err(SourceCloseError::Native)?;
        require_recorded(&tx, source).await?;
        let id = source.manifest().identity();
        let dataset = &id.dataset_id.as_bytes().as_slice();
        let shard = i64::from(id.shard_id);
        let digest = source.digest();
        let bound = source.last_complete_sequence().to_string();
        if state.source_closed {
            let exact: bool = tx.query_one(
                "SELECT last_complete_sequence=$3::text::numeric AND closure_digest=$4::bytea
                 FROM control.source_streams WHERE dataset_id=$1::bytea AND shard_id=$2::bigint AND owner_epoch=2",
                &[dataset, &shard, &bound, &&digest[..]],
            ).await.map_err(backend)?.try_get(0).map_err(backend)?;
            if !exact {
                return Err(OwnerError::InvalidMetadata.into());
            }
        } else {
            let closing = tx.execute(
                "UPDATE control.source_streams SET state='closing'
                 WHERE dataset_id=$1::bytea AND shard_id=$2::bigint AND owner_epoch=2 AND state='open'",
                &[dataset, &shard],
            ).await.map_err(backend)?;
            let completed = tx.execute(
                "UPDATE control.source_streams SET state='closed',
                    closed_at_us=(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,
                    last_complete_sequence=$3::text::numeric,closure_digest=$4::bytea
                 WHERE dataset_id=$1::bytea AND shard_id=$2::bigint AND owner_epoch=2 AND state='closing'",
                &[dataset, &shard, &bound, &&digest[..]],
            ).await.map_err(backend)?;
            if (closing, completed) != (1, 1) {
                return Err(OwnerError::InvalidMetadata.into());
            }
        }
        source.validate().map_err(SourceCloseError::Native)?;
        initial_history::check_delivery(&mut lease, epoch, input.cancelled)?;
        tx.commit()
            .await
            .map_err(|_| SourceCloseError::CommitUnknown)?;
        initial_history::check_delivery(&mut lease, epoch, input.cancelled)?;
        if self.client.is_closed() {
            return Err(SourceCloseError::DeliveryUnknown);
        }
        self.available = true;
        Ok(())
    }
}

async fn require_recorded(
    tx: &Transaction<'_>,
    source: &ResolvedActivatedSource<'_>,
) -> Result<(), SourceCloseError> {
    let manifest: [u8; 32] = Sha256::digest(source.manifest().encode().as_bytes()).into();
    let rows = tx
        .query(
            "SELECT left(r.request_sequence::text,21),substring(r.request_digest FROM 1 FOR 33),
                substring(o.outcome_sha256 FROM 1 FOR 33)
         FROM control.producer_requests r JOIN control.producer_outcomes o
             USING(producer_slot_id,producer_epoch,request_sequence)
         WHERE r.producer_slot_id=$1::bytea AND r.producer_epoch=2 AND r.dataset_id=$2::bytea
             AND o.dataset_id=r.dataset_id AND r.manifest_sha256=$3::bytea
             AND o.manifest_sha256=r.manifest_sha256 AND o.request_digest=r.request_digest
         ORDER BY r.request_sequence LIMIT 65",
            &[
                &&source.producer_slot_id().as_bytes()[..],
                &&source.manifest().identity().dataset_id.as_bytes()[..],
                &&manifest[..],
            ],
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
        recorded.push((sequence, fixed(&row, 1)?, fixed(&row, 2)?));
    }
    if !source.matches_recorded(recorded) {
        return Err(OwnerError::InvalidMetadata.into());
    }
    Ok(())
}
