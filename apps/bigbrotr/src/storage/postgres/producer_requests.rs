//! Immutable reservation metadata. No row here is a native admission receipt.
use super::ingest_registry::ControlOwner;
use super::producer_sources::{self, RequestPhase, SourceBindingError};
use crate::acquisition::relay_source::RelaySourceBinding;
use crate::storage::rocks::store::StagedProducerRequest;
use bigbrotr_model::{AcquisitionScopeId, DatasetId, Digest256, JobId, ProducerRequestId};
use tokio_postgres::Transaction;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReservationError {
    Backend,
    Conflict,
    InvalidMetadata,
    Source(SourceBindingError),
    /// PostgreSQL may have committed; native dispatch has not begun.
    CommitUnknown,
}
impl std::fmt::Display for ReservationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "producer request reservation unavailable: {self:?}")
    }
}
impl std::error::Error for ReservationError {}
fn backend(_: tokio_postgres::Error) -> ReservationError {
    ReservationError::Backend
}

/// Bounded observation for recovery by a current Control owner. It does not
/// reconstruct the former process, authorize dispatch or establish an outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerReservation {
    pub request: ProducerRequestId,
    pub digest: Digest256,
    pub input_bytes: u32,
    pub manifest_sha256: [u8; 32],
}

pub(super) async fn read(
    tx: &Transaction<'_>,
    dataset: DatasetId,
    request: ProducerRequestId,
) -> Result<Option<ProducerReservation>, ReservationError> {
    const READ: &str = "
        SELECT substring(request_digest FROM 1 FOR 33), input_bytes,
            substring(manifest_sha256 FROM 1 FOR 33), input_profile
        FROM control.producer_requests
        WHERE dataset_id=$1::bytea AND producer_slot_id=$2::bytea
            AND producer_epoch=$3::text::numeric AND request_sequence=$4::text::numeric";
    let row = tx
        .query_opt(
            READ,
            &[
                &&dataset.as_bytes()[..],
                &&request.producer_slot_id.as_bytes()[..],
                &request.producer_epoch.get().to_string(),
                &request.request_sequence.get().to_string(),
            ],
        )
        .await
        .map_err(backend)?;
    row.map(|row| {
        let fixed = |index| -> Result<[u8; 32], ReservationError> {
            row.try_get::<_, &[u8]>(index)
                .map_err(backend)?
                .try_into()
                .map_err(|_| ReservationError::InvalidMetadata)
        };
        let input_bytes: i32 = row.try_get(1).map_err(backend)?;
        if !(1..=16 * 1024 * 1024).contains(&input_bytes)
            || row.try_get::<_, i16>(3).map_err(backend)? != 1
        {
            return Err(ReservationError::InvalidMetadata);
        }
        Ok(ProducerReservation {
            request,
            digest: Digest256::from_bytes(fixed(0)?),
            input_bytes: input_bytes as u32,
            manifest_sha256: fixed(2)?,
        })
    })
    .transpose()
}

pub(super) async fn reserve(
    tx: &Transaction<'_>,
    staged: &StagedProducerRequest,
    issuer: ControlOwner,
    source: Option<RelaySourceBinding>,
    scope: Option<AcquisitionScopeId>,
    job: Option<JobId>,
) -> Result<(), ReservationError> {
    reserve_in(tx, staged, issuer, source, scope, job, None).await
}

#[allow(
    clippy::too_many_arguments,
    reason = "original immutable source and actual retained-event position"
)]
pub(super) async fn reserve_progress(
    tx: &Transaction<'_>,
    staged: &StagedProducerRequest,
    issuer: ControlOwner,
    source: Option<RelaySourceBinding>,
    scope: Option<AcquisitionScopeId>,
    job: Option<JobId>,
    position: super::control_owner::producer_progress::Position,
) -> Result<(), ReservationError> {
    if source.is_none() || scope.is_none() || job.is_none() {
        return Err(ReservationError::InvalidMetadata);
    }
    reserve_in(tx, staged, issuer, source, scope, job, Some(position)).await
}

#[allow(
    clippy::too_many_arguments,
    reason = "one reservation kernel preserves legacy and typed progress inputs"
)]
async fn reserve_in(
    tx: &Transaction<'_>,
    staged: &StagedProducerRequest,
    issuer: ControlOwner,
    source: Option<RelaySourceBinding>,
    scope: Option<AcquisitionScopeId>,
    job: Option<JobId>,
    position: Option<super::control_owner::producer_progress::Position>,
) -> Result<(), ReservationError> {
    // The bridge already holds the exact owner/registry locks. Upgrade producer
    // serialization before a separate READ COMMITTED observation of prior rows.
    const LOCK_PRODUCER: &str = "
        SELECT reserved_through_request::text FROM control.producer_epochs
        WHERE dataset_id=$1::bytea AND producer_slot_id=$2::bytea
            AND producer_epoch=$3::text::numeric FOR UPDATE";
    tx.query_one(
        LOCK_PRODUCER,
        &[
            &&staged.dataset.as_bytes()[..],
            &&staged.request.producer_slot_id.as_bytes()[..],
            &staged.request.producer_epoch.get().to_string(),
        ],
    )
    .await
    .map_err(backend)?;
    producer_sources::require_binding(
        tx,
        staged.dataset,
        staged.request,
        source,
        scope,
        job,
        RequestPhase::Reservation,
    )
    .await
    .map_err(ReservationError::Source)?;
    if let Some(row) = read(tx, staged.dataset, staged.request).await? {
        if row.digest != staged.digest
            || row.input_bytes != staged.input_bytes
            || row.manifest_sha256 != staged.manifest_sha256
        {
            return Err(ReservationError::Conflict);
        }
        if let Some(position) = position {
            let matches: bool = tx.query_one("SELECT progress_request_id=$4::bytea AND progress_event_ordinal=$5::smallint AND progress_wire_sequence=$6::text::numeric AND progress_input_sha256=$7::bytea FROM control.producer_requests WHERE producer_slot_id=$1::bytea AND producer_epoch=$2::text::numeric AND request_sequence=$3::text::numeric", &[&&staged.request.producer_slot_id.as_bytes()[..], &staged.request.producer_epoch.get().to_string(), &staged.request.request_sequence.get().to_string(), &&position.request[..], &(position.ordinal as i16), &position.wire_sequence.to_string(), &&position.input_digest[..]]).await.map_err(backend)?.try_get::<_,Option<bool>>(0).map_err(backend)?.unwrap_or(false);
            if !matches {
                return Err(ReservationError::Conflict);
            }
        }
        return Ok(());
    }
    if let Some(source) = source {
        const INSERT: &str = "
            INSERT INTO control.producer_requests(producer_slot_id, producer_epoch,
                request_sequence, dataset_id, request_digest, input_bytes, input_profile,
                manifest_sha256, issuer_resource, issuer_epoch, issuer_incarnation, reserved_at_us,
                relay_endpoint_id, relay_access_scope_id, relay_enrollment_generation, relay_route_id)
            VALUES ($1::bytea, $2::text::numeric, $3::text::numeric, $4::bytea, $5::bytea,
                $6::integer, 1, $7::bytea, $8::bytea, $9::text::numeric, $10::bytea,
                (EXTRACT(EPOCH FROM pg_catalog.clock_timestamp()) * 1000000)::bigint,
                $11::bytea, $12::bytea, $13::text::numeric, $14::bytea)";
        const INSERT_SCOPED: &str = "
            INSERT INTO control.producer_requests(producer_slot_id, producer_epoch,
                request_sequence, dataset_id, request_digest, input_bytes, input_profile,
                manifest_sha256, issuer_resource, issuer_epoch, issuer_incarnation, reserved_at_us,
                relay_endpoint_id, relay_access_scope_id, relay_enrollment_generation, relay_route_id,
                acquisition_scope_id)
            VALUES ($1::bytea, $2::text::numeric, $3::text::numeric, $4::bytea, $5::bytea,
                $6::integer, 1, $7::bytea, $8::bytea, $9::text::numeric, $10::bytea,
                (EXTRACT(EPOCH FROM pg_catalog.clock_timestamp()) * 1000000)::bigint,
                $11::bytea, $12::bytea, $13::text::numeric, $14::bytea, $15::bytea)";
        const INSERT_JOB: &str = "
            INSERT INTO control.producer_requests(producer_slot_id, producer_epoch,
                request_sequence, dataset_id, request_digest, input_bytes, input_profile,
                manifest_sha256, issuer_resource, issuer_epoch, issuer_incarnation, reserved_at_us,
                relay_endpoint_id, relay_access_scope_id, relay_enrollment_generation, relay_route_id,
                acquisition_scope_id, acquisition_job_id)
            VALUES ($1::bytea, $2::text::numeric, $3::text::numeric, $4::bytea, $5::bytea,
                $6::integer, 1, $7::bytea, $8::bytea, $9::text::numeric, $10::bytea,
                (EXTRACT(EPOCH FROM pg_catalog.clock_timestamp()) * 1000000)::bigint,
                $11::bytea, $12::bytea, $13::text::numeric, $14::bytea, $15::bytea, $16::bytea)";
        const INSERT_JOB_PROGRESS: &str = "
            INSERT INTO control.producer_requests(producer_slot_id, producer_epoch,
                request_sequence, dataset_id, request_digest, input_bytes, input_profile,
                manifest_sha256, issuer_resource, issuer_epoch, issuer_incarnation, reserved_at_us,
                relay_endpoint_id, relay_access_scope_id, relay_enrollment_generation, relay_route_id,
                acquisition_scope_id, acquisition_job_id, progress_request_id, progress_event_ordinal,
                progress_wire_sequence, progress_input_sha256)
            VALUES ($1::bytea,$2::text::numeric,$3::text::numeric,$4::bytea,$5::bytea,
                $6::integer,1,$7::bytea,$8::bytea,$9::text::numeric,$10::bytea,
                (EXTRACT(EPOCH FROM pg_catalog.clock_timestamp())*1000000)::bigint,
                $11::bytea,$12::bytea,$13::text::numeric,$14::bytea,$15::bytea,$16::bytea,
                $17::bytea,$18::smallint,$19::text::numeric,$20::bytea)";
        let epoch = staged.request.producer_epoch.get().to_string();
        let sequence = staged.request.request_sequence.get().to_string();
        let owner_epoch = issuer.epoch.get().to_string();
        let generation = source.enrollment_generation.get().to_string();
        let scope_bytes = scope.as_ref().map(|s| s.as_bytes().as_slice());
        let job_bytes = job.as_ref().map(|j| j.as_bytes().as_slice());
        let progress_request = position.as_ref().map(|p| p.request.as_slice());
        let progress_ordinal = position.map(|p| p.ordinal as i16);
        let progress_sequence = position.map(|p| p.wire_sequence.to_string());
        let progress_digest = position.as_ref().map(|p| p.input_digest.as_slice());
        let params: &[&(dyn tokio_postgres::types::ToSql + Sync)] = &[
            &&staged.request.producer_slot_id.as_bytes()[..],
            &epoch,
            &sequence,
            &&staged.dataset.as_bytes()[..],
            &&staged.digest.as_bytes()[..],
            &(staged.input_bytes as i32),
            &&staged.manifest_sha256[..],
            &&issuer.resource_id.as_bytes()[..],
            &owner_epoch,
            &&issuer.process_incarnation.as_bytes()[..],
            &&source.endpoint_id.as_bytes()[..],
            &&source.access_scope_id.as_bytes()[..],
            &generation,
            &&source.route_id.as_bytes()[..],
            &scope_bytes,
            &job_bytes,
            &progress_request,
            &progress_ordinal,
            &progress_sequence,
            &progress_digest,
        ];
        let (query, params) = if position.is_some() {
            (INSERT_JOB_PROGRESS, params)
        } else if job.is_some() {
            (INSERT_JOB, &params[..16])
        } else if scope.is_some() {
            (INSERT_SCOPED, &params[..15])
        } else {
            (INSERT, &params[..14])
        };
        tx.execute(query, params).await.map_err(backend)?;
        return Ok(());
    }
    // Keep the earlier unbound schema usable; bound INSERT has an explicit0020
    // prerequisite above. Both forms share the same immutable reservation guard.
    const INSERT_UNBOUND: &str = "
        INSERT INTO control.producer_requests(producer_slot_id, producer_epoch,
            request_sequence, dataset_id, request_digest, input_bytes, input_profile,
            manifest_sha256, issuer_resource, issuer_epoch, issuer_incarnation, reserved_at_us)
        VALUES ($1::bytea, $2::text::numeric, $3::text::numeric, $4::bytea, $5::bytea,
            $6::integer, 1, $7::bytea, $8::bytea, $9::text::numeric, $10::bytea,
            (EXTRACT(EPOCH FROM pg_catalog.clock_timestamp()) * 1000000)::bigint)";
    tx.execute(
        INSERT_UNBOUND,
        &[
            &&staged.request.producer_slot_id.as_bytes()[..],
            &staged.request.producer_epoch.get().to_string(),
            &staged.request.request_sequence.get().to_string(),
            &&staged.dataset.as_bytes()[..],
            &&staged.digest.as_bytes()[..],
            &(staged.input_bytes as i32),
            &&staged.manifest_sha256[..],
            &&issuer.resource_id.as_bytes()[..],
            &issuer.epoch.get().to_string(),
            &&issuer.process_incarnation.as_bytes()[..],
        ],
    )
    .await
    .map_err(backend)?;
    Ok(())
}
