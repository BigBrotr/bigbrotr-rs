//! Retire only an already closed activated source with completed workflows.
//! Every original and native byte is retained; this never closes a native OPEN
//! producer, repairs an outcome, abandons a job or constructs another writer.
use super::*;
use crate::{
    runtime::OwnedPermit,
    storage::rocks::store::{
        ActivatedSourceResolution, BootstrapError, IngestRecovery, ResolvedActivatedSource,
        ResolvedProducerRequest,
    },
};
use bigbrotr_model::ProducerSlotId;

pub struct ActivatedInitialRetirement(ActivatedInitialRecovery);
pub struct ClosedActivatedInitialRetirement(ClosedActivatedInitialProducer);

impl ActivatedInitialRetirement {
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
impl ClosedActivatedInitialRetirement {
    pub fn predecessor_issued_through(&self) -> u64 {
        self.0.predecessor_issued_through()
    }
    pub fn acknowledged_through(&self) -> u64 {
        self.0.acknowledged_through()
    }
    pub fn producer_slot_id(&self) -> ProducerSlotId {
        self.0.producer_slot_id()
    }
    pub fn issued_through(&self) -> u64 {
        self.0.issued_through()
    }
    pub fn reserved_through(&self) -> u64 {
        self.0.reserved_through()
    }
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
    #[allow(
        clippy::too_many_arguments,
        reason = "closed context and per-original grants"
    )]
    pub async fn inspect_activated_initial_retirement_predecessor_request(
        &mut self,
        recovery: &IngestRecovery,
        closed: &ClosedActivatedInitialRetirement,
        input: ActivatedInitialResolveRequest<'_>,
        input_permit: OwnedPermit,
        outcome_permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<ResolvedProducerRequest, ProducerResolveError> {
        self.resolve_activated_initial_predecessor_request(
            recovery,
            &closed.0,
            input,
            input_permit,
            outcome_permit,
            validate,
        )
        .await
    }
    pub async fn inspect_activated_initial_retirement(
        &mut self,
        recovery: &IngestRecovery,
        input: InitialHistoryRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<ActivatedInitialRetirement, InitialSuccessionError> {
        self.inspect_activated_producer(
            recovery,
            input,
            permit,
            activated_recovery::Purpose::Retirement,
            validate,
        )
        .await
        .map(ActivatedInitialRetirement)
    }

    /// Observe, never create, the native CLOSING barrier. The same fixed-size
    /// proof and its grant remain borrowed by the complete source witness.
    pub async fn observe_activated_initial_retirement(
        &mut self,
        recovery: &IngestRecovery,
        inspected: ActivatedInitialRetirement,
        input: InitialHistoryRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<ClosedActivatedInitialRetirement, InitialSuccessionError> {
        self.observe_activated_producer(recovery, inspected.0, input, permit, validate)
            .await
            .map(ClosedActivatedInitialRetirement)
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "distinct closed context and bounded original grants"
    )]
    pub async fn inspect_activated_initial_retirement_request(
        &mut self,
        recovery: &IngestRecovery,
        closed: &ClosedActivatedInitialRetirement,
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

    /// The twelve target rows transition together; source2, original issuers,
    /// outcome recorders and completed workflow history remain unchanged.
    pub async fn retire_activated_initial_producer(
        &mut self,
        closed: &ClosedActivatedInitialRetirement,
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
        if !source.matches_closed(&closed.0.native)
            || source.producer_slot_id() != closed.producer_slot_id()
            || source.resolved_requests() != closed.reserved_through()
            || source.resolved_requests() != closed.issued_through()
            || source.resolved_requests() > 64
        {
            return Err(OwnerError::InvalidInput.into());
        }
        source.validate().map_err(SourceCloseError::Native)?;
        activated_recovery::require_deployment(&closed.0.metadata, self)?;
        let current = self.identity;
        let (tx, mut lease, epoch) = self
            .begin_initial_history_locked(source.recovery(), &input, 2, 37, validate)
            .await?;
        let state =
            activated_recovery::require_metadata(&tx, &closed.0.metadata, current, epoch).await?;
        if !state.source_closed || !state.producer_closing {
            return Err(OwnerError::InvalidMetadata.into());
        }
        source.validate().map_err(SourceCloseError::Native)?;
        activated_source::require_recorded(&tx, source).await?;
        let id = source.manifest().identity();
        let slot_id = source.producer_slot_id();
        let slot = &slot_id.as_bytes().as_slice();
        let dataset = &id.dataset_id.as_bytes().as_slice();
        let resource = &id.owner_resource_id.as_bytes().as_slice();
        let role = &closed.0.metadata.target_role().as_slice();
        let digest = source.digest();
        let requests = tx.query(
            "SELECT left(r.request_sequence::text,21), r.acquisition_job_id IS NOT NULL
                AND EXISTS (SELECT 1 FROM control.acquisition_gaps g WHERE g.job_id=r.acquisition_job_id
                    AND g.producer_slot_id=r.producer_slot_id AND g.producer_epoch=2)
             FROM control.producer_requests r WHERE r.producer_slot_id=$1::bytea AND r.producer_epoch=2
             ORDER BY r.request_sequence LIMIT 65", &[slot],
        ).await.map_err(backend)?;
        if requests.len() as u64 != source.resolved_requests() {
            return Err(OwnerError::InvalidMetadata.into());
        }
        for (offset, row) in requests.iter().enumerate() {
            if number(row, 0)? != offset as u64 + 1
                || (source.event_request_mask() & (1_u64 << offset) != 0
                    && !row.try_get::<_, bool>(1).map_err(backend)?)
            {
                return Err(OwnerError::InvalidMetadata.into());
            }
        }
        drop(requests);
        let row = tx.query_one(
            "SELECT p.retired_at_us, s.last_complete_sequence=$4::text::numeric AND s.closure_digest=$5::bytea,
                s.closed_at_us, (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,
                (SELECT count(*) FROM control.acquisition_gaps WHERE producer_slot_id=p.producer_slot_id AND producer_epoch=2)
             FROM control.producer_epochs p JOIN control.source_streams s
                ON s.dataset_id=p.dataset_id AND s.owner_resource_id=p.owner_resource_id AND s.owner_epoch=p.owner_epoch
             WHERE p.producer_slot_id=$1::bytea AND p.producer_epoch=2 AND s.dataset_id=$2::bytea AND s.shard_id=$3::bigint",
            &[slot,dataset,&i64::from(id.shard_id),&source.last_complete_sequence().to_string(),&&digest[..]],
        ).await.map_err(backend)?;
        if !row.try_get::<_, bool>(1).map_err(backend)? {
            return Err(OwnerError::InvalidMetadata.into());
        }
        let retired: Option<i64> = row.try_get(0).map_err(backend)?;
        let source_closed: i64 = row.try_get(2).map_err(backend)?;
        let now: i64 = row.try_get(3).map_err(backend)?;
        let gaps: i64 = row.try_get(4).map_err(backend)?;
        let gaps = u64::try_from(gaps)
            .ok()
            .filter(|n| *n <= 64)
            .ok_or(OwnerError::InvalidMetadata)?;
        drop(row);
        // The classifier took ordered job UPDATE custody. Do not treat the
        // authority sampled before a lock wait as current scheduling authority.
        activated_acquisition::require_current(&tx, id.dataset_id, current).await?;
        initial_history::check_delivery(&mut lease, epoch, input.cancelled)?;
        if retired.is_none() {
            if now < source_closed {
                return Err(OwnerError::InvalidMetadata.into());
            }
            let producer = tx.execute(
                "UPDATE control.producer_epochs SET state='retired',retired_at_us=$2::bigint,closure_digest=$3::bytea
                 WHERE producer_slot_id=$1::bytea AND producer_epoch=2 AND state='closing'",
                &[slot,&now,&&digest[..]],
            ).await.map_err(backend)?;
            let domains = tx.execute(
                "UPDATE control.allocation_domains SET state='closed',closed_at_us=$3::bigint,closure_digest=$4::bytea
                 WHERE dataset_id=$1::bytea AND owner_resource_id=$2::bytea AND owner_epoch=2 AND state='active'",
                &[dataset,resource,&now,&&digest[..]],
            ).await.map_err(backend)?;
            let owner = tx.execute(
                "UPDATE control.owner_epochs SET state='released',released_at_us=$2::bigint,
                    exclusion_method='storage_fenced',exclusion_digest=$3::bytea WHERE resource_id=$1::bytea AND epoch=2",
                &[resource,&now,&&digest[..]],
            ).await.map_err(backend)?;
            let retired_role = tx.execute(
                "UPDATE control.role_instances SET retired_at_us=$2::bigint WHERE role_instance_id=$1::bytea",
                &[role,&now],
            ).await.map_err(backend)?;
            if (producer, domains, owner, retired_role) != (1, 9, 1, 1) {
                return Err(OwnerError::InvalidMetadata.into());
            }
        }
        activated_recovery::require_metadata(&tx, &closed.0.metadata, current, epoch).await?;
        source.validate().map_err(SourceCloseError::Native)?;
        activated_acquisition::require_current(&tx, id.dataset_id, current).await?;
        initial_history::check_delivery(&mut lease, epoch, input.cancelled)?;
        tx.commit()
            .await
            .map_err(|_| SourceCloseError::CommitUnknown)?;
        initial_history::check_delivery(&mut lease, epoch, input.cancelled)?;
        if self.client.is_closed() {
            return Err(SourceCloseError::DeliveryUnknown);
        }
        self.available = true;
        Ok(gaps)
    }
}
