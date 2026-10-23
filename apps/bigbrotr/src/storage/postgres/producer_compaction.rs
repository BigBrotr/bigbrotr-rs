//! Authenticate an existing closed native floor against retained PostgreSQL
//! history. This reader writes no checkpoint, ACK, floor or reclaimed detail.
use super::*;
use crate::{
    runtime::{OwnedPermit, policy_authority::PolicyLeaseTracker},
    storage::rocks::store::{
        AdmissionError, AdmissionLimits, CompactedOutcomeRow, CompactedPrefixResolution,
        IngestRecovery, MAX_OUTCOME_BYTES, NativeOutcomeKind, RetainedCompactedPrefix,
        VerifiedCompactedPrefix, compacted_outcome_data_bytes,
    },
};
use bigbrotr_model::{
    Digest256, PolicyEpoch, ProducerEpoch, ProducerRequestId, ProducerSlotId, RequestSequence,
};
use futures_util::TryStreamExt;
use std::sync::atomic::AtomicBool;
use tokio_postgres::types::ToSql;

pub type ProducerCompactionError = ProducerOperationError<AdmissionError>;

/// The native prefix and its retained grants outlive this confirmed PG read.
/// This is historical integrity evidence, not current Control authority or a
/// capability to delete detail, admit work, create an outcome or issue ACK.
pub struct CompactedInitialRetirement<'a> {
    native: VerifiedCompactedPrefix<'a>,
    gaps: u64,
    _metadata: OwnedPermit,
}
impl CompactedInitialRetirement<'_> {
    /// One bounded SQL outcome row, fixed parameters, typed-head hash scratch
    /// and the confirmed wrapper. Native decoded effects have a separate grant.
    pub const REQUIRED_DATA_BYTES: u64 = 128 * 1024;

    pub fn producer_slot_id(&self) -> ProducerSlotId {
        self.native.producer_slot_id()
    }
    pub fn issued_through(&self) -> u64 {
        self.native.floor().fields().issued_through
    }
    pub fn acknowledged_through(&self) -> u64 {
        self.issued_through()
    }
    pub fn retired_through(&self) -> u64 {
        self.issued_through()
    }
    pub fn last_complete_sequence(&self) -> u64 {
        self.native.floor().fields().source_through
    }
    pub fn digest(&self) -> [u8; 32] {
        self.native.floor().fields().closure_digest
    }
    pub fn acquisition_gaps(&self) -> u64 {
        self.gaps
    }
    pub fn validate(
        &self,
        recovery: &IngestRecovery,
        cancelled: &AtomicBool,
    ) -> Result<(), AdmissionError> {
        self.native.validate(recovery, cancelled)
    }
}

impl ControlSession {
    /// Observe an already compacted initial retirement. Only the explicit
    /// native v3 inspector can supply `prefix`; decoded SQL bytes never create
    /// original-request witnesses. Every read uses the existing retirement lock
    /// order and the original policy deadline, through the final COMMIT.
    #[allow(
        clippy::too_many_arguments,
        reason = "native observation, historical request and separate retained SQL/effect grants"
    )]
    pub async fn inspect_initial_compacted_retirement<'a>(
        &mut self,
        recovery: &'a IngestRecovery,
        prefix: &'a RetainedCompactedPrefix,
        input: ProducerCloseRequest<'_>,
        limits: AdmissionLimits,
        metadata_permit: OwnedPermit,
        outcome_permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<CompactedInitialRetirement<'a>, ProducerCompactionError> {
        if metadata_permit.request().units().data_bytes()
            < CompactedInitialRetirement::REQUIRED_DATA_BYTES
            || metadata_permit.request().units().items() == 0
            || outcome_permit.request().units().data_bytes()
                < compacted_outcome_data_bytes(limits).map_err(ProducerCompactionError::Native)?
            || outcome_permit.request().units().items() == 0
        {
            return Err(ProducerCompactionError::Native(
                AdmissionError::InsufficientReservation,
            ));
        }
        prefix
            .validate(recovery, input.cancelled)
            .map_err(ProducerCompactionError::Native)?;
        let spec = input.registration;
        let floor = prefix.floor().fields();
        if prefix.producer_slot_id() != spec.producer_slot_id
            || prefix.manifest() != recovery.manifest()
            || prefix.manifest().identity().owner_epoch.get() != 1
        {
            return Err(OwnerError::InvalidInput.into());
        }
        let (tx, mut lease, policy) = self
            .begin_producer_recovery::<AdmissionError>(
                recovery,
                input.ingest_root,
                spec,
                input.policy_ttl,
                ingest_registry::RegistryLocks::Retirement,
                validate,
            )
            .await?;
        // This entry requires both typed progress and native ACK schemas. The
        // authenticated migration ledger was read under the migration lock by
        // begin_producer_recovery; the ordinary R=0 reader remains unchanged.
        producer_progress::recovery::require_compacted(
            &tx,
            recovery,
            prefix,
            &metadata_permit,
            input.cancelled,
            || producer_progress::recovery::authority(&mut lease, policy, input.cancelled),
        )
        .await?;
        let slot = &spec.producer_slot_id.as_bytes().as_slice();
        let resource = &spec.ingest_resource_id.as_bytes().as_slice();
        let dataset = &spec.dataset_id.as_bytes().as_slice();
        let no_successor: bool = tx
            .query_one(NO_SUCCESSOR, &[resource, slot])
            .await
            .map_err(backend)?
            .try_get(0)
            .map_err(backend)?;
        if !no_successor {
            return Err(OwnerError::InvalidMetadata.into());
        }
        // Reuse the complete original SQL validator, including zero-request
        // workflow claims, import vetoes, exact gaps and historical attribution.
        let gaps: i64 = tx
            .query_one(
                "SELECT control.require_initial_retirement($1::bytea,1,$2::bytea)",
                &[slot, &&floor.closure_digest[..]],
            )
            .await
            .map_err(backend)?
            .try_get(0)
            .map_err(backend)?;
        let gaps = u64::try_from(gaps).map_err(|_| OwnerError::InvalidMetadata)?;
        let state = tx
            .query_one(
                producer_retirement::RETIREMENT_STATE,
                &[
                    slot,
                    resource,
                    &spec.ingest_role_instance_id.as_slice(),
                    dataset,
                    &&floor.closure_digest[..],
                    &floor.issued_through.to_string(),
                    &i64::from(spec.shard_id),
                    &floor.source_through.to_string(),
                ],
            )
            .await
            .map_err(backend)?;
        if !state.try_get::<_, bool>(1).map_err(backend)? {
            return Err(OwnerError::InvalidMetadata.into());
        }
        drop(state);
        initial_history::check_delivery::<AdmissionError>(&mut lease, policy, input.cancelled)?;
        let resolution = prefix
            .resolution(recovery, input.cancelled)
            .map_err(ProducerCompactionError::Native)?;
        let native = resolve_initial_outcomes(
            &tx,
            resolution,
            limits,
            &outcome_permit,
            &mut lease,
            policy,
            input.cancelled,
        )
        .await?;
        producer_retirement::finish::<AdmissionError>(tx, &mut lease, policy, input.cancelled)
            .await?;
        if self.client.is_closed() {
            return Err(ProducerCompactionError::DeliveryUnknown);
        }
        native
            .validate(recovery, input.cancelled)
            .map_err(ProducerCompactionError::Native)?;
        initial_history::check_delivery::<AdmissionError>(&mut lease, policy, input.cancelled)?;
        self.available = true;
        Ok(CompactedInitialRetirement {
            native,
            gaps,
            _metadata: metadata_permit,
        })
    }
}

// Shared canonical outcome/native consistency phase. The caller retains SQL
// metadata custody and validates the typed checkpoint and historical terminal
// context under its own locks first. This result is not a confirmed PG proof;
// the owning operation must complete its transaction and delivery checks.
#[allow(
    clippy::too_many_arguments,
    reason = "locked transaction, native observation, bounded effect grant and original authority deadline"
)]
pub(super) async fn resolve_initial_outcomes<'a>(
    tx: &Transaction<'_>,
    mut resolution: CompactedPrefixResolution<'a>,
    limits: AdmissionLimits,
    outcome_permit: &OwnedPermit,
    lease: &mut PolicyLeaseTracker,
    policy: PolicyEpoch,
    cancelled: &AtomicBool,
) -> Result<VerifiedCompactedPrefix<'a>, ProducerCompactionError> {
    let floor = *resolution.floor().fields();
    let slot_id = resolution.producer_slot_id();
    let slot = &slot_id.as_bytes().as_slice();
    let dataset_id = resolution.manifest().identity().dataset_id;
    let dataset = &dataset_id.as_bytes().as_slice();
    initial_history::check_delivery::<AdmissionError>(lease, policy, cancelled)?;
    {
        let params: &[&(dyn ToSql + Sync)] = &[slot, dataset];
        let stream = tx
            .query_raw(OUTCOMES, params.iter().copied())
            .await
            .map_err(backend)?;
        futures_util::pin_mut!(stream);
        while let Some(row) = stream.try_next().await.map_err(backend)? {
            initial_history::check_delivery::<AdmissionError>(lease, policy, cancelled)?;
            let sequence = ingest_registry::decimal(row.try_get(0).map_err(backend)?)
                .map_err(ProducerCompactionError::Registry)?;
            let input_bytes: i32 = row.try_get(3).map_err(backend)?;
            let bytes: &[u8] = row.try_get(6).map_err(backend)?;
            if !(1..=16 * 1024 * 1024).contains(&input_bytes)
                || bytes.is_empty()
                || bytes.len() > MAX_OUTCOME_BYTES
                || !row.try_get::<_, bool>(8).map_err(backend)?
                || fixed::<32>(&row, 2)? != floor.manifest_sha256
                || <[u8; 32]>::from(Sha256::digest(bytes)) != fixed::<32>(&row, 7)?
            {
                return Err(OwnerError::InvalidMetadata.into());
            }
            let kind = match row.try_get::<_, i16>(4).map_err(backend)? {
                1 => NativeOutcomeKind::NewFacts,
                2 => NativeOutcomeKind::AlreadyDurable,
                3 => NativeOutcomeKind::Rejected,
                _ => return Err(OwnerError::InvalidMetadata.into()),
            };
            let historical_policy = ingest_registry::decimal(row.try_get(5).map_err(backend)?)
                .map_err(ProducerCompactionError::Registry)?;
            resolution
                .record(
                    CompactedOutcomeRow {
                        request: ProducerRequestId {
                            producer_slot_id: slot_id,
                            producer_epoch: ProducerEpoch::new(1).expect("initial epoch"),
                            request_sequence: RequestSequence::new(sequence)
                                .map_err(|_| OwnerError::InvalidMetadata)?,
                        },
                        digest: Digest256::from_bytes(fixed::<32>(&row, 1)?),
                        kind,
                        policy_epoch: PolicyEpoch::new(historical_policy),
                        bytes,
                        input_bytes: input_bytes as u32,
                    },
                    limits,
                    outcome_permit,
                    cancelled,
                    || producer_progress::recovery::authority(lease, policy, cancelled),
                )
                .map_err(ProducerCompactionError::Native)?;
        }
    }
    resolution
        .finish(cancelled)
        .map_err(ProducerCompactionError::Native)
}

// Current Control/dataset custody prevents concurrent successor preparation.
// This first reader accepts only the original initial lifetime, even if a
// prepared successor has not yet changed the native singleton.
const NO_SUCCESSOR: &str = "
    SELECT NOT EXISTS(SELECT 1 FROM control.initial_ingest_successions
               WHERE owner_resource_id=$1::bytea)
       AND NOT EXISTS(SELECT 1 FROM control.initial_ingest_activations
               WHERE owner_resource_id=$1::bytea)
       AND NOT EXISTS(SELECT 1 FROM control.ingest_successions
               WHERE owner_resource_id=$1::bytea)
       AND NOT EXISTS(SELECT 1 FROM control.ingest_activations
               WHERE owner_resource_id=$1::bytea)
       AND NOT EXISTS(SELECT 1 FROM control.owner_epochs
               WHERE resource_id=$1::bytea AND epoch<>1)
       AND NOT EXISTS(SELECT 1 FROM control.producer_epochs
               WHERE producer_slot_id=$2::bytea AND producer_epoch<>1)";

// The producer UPDATE lock precedes this bounded stream. Every immutable
// outcome is fed to the native ordered reducer; no copied row mints a receipt.
const OUTCOMES: &str = "
    SELECT left(r.request_sequence::text,21),
           substring(r.request_digest FROM 1 FOR 33),
           substring(r.manifest_sha256 FROM 1 FOR 33),r.input_bytes,
           o.outcome_kind,left(o.policy_epoch::text,21),
           substring(o.outcome_bytes FROM 1 FOR 65537),
           substring(o.outcome_sha256 FROM 1 FOR 33),
           r.dataset_id=$2::bytea AND o.dataset_id=r.dataset_id
               AND o.request_digest=r.request_digest
               AND o.manifest_sha256=r.manifest_sha256 AND r.input_profile=1
    FROM control.producer_requests r
    JOIN control.producer_outcomes o USING(producer_slot_id,producer_epoch,request_sequence)
    WHERE r.producer_slot_id=$1::bytea AND r.producer_epoch=1
    ORDER BY r.request_sequence LIMIT 65";
