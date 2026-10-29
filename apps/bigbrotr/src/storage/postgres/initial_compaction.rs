//! Explicit compacted predecessor checks for the existing initial handoff.
//! Portable intent bytes remain unchanged and never supply native custody.
use super::*;
use crate::{
    runtime::{OwnedPermit, policy_authority::PolicyLeaseTracker},
    storage::rocks::{
        manifest::IngestManifest,
        store::{
            ActiveInitialWriter, AdmissionError, AdmissionLimits, BootstrapError,
            COMPACTED_HANDOFF_DATA_BYTES, IngestRecovery, InitialHandoff, InitialWriterCandidate,
            ObservedHandoff, RetainedCompactedPrefix, RetainedLineage,
            compacted_outcome_data_bytes,
        },
    },
};
use bigbrotr_model::PolicyEpoch;
use std::sync::atomic::AtomicBool;
use tokio_postgres::Transaction;

pub(super) struct Check {
    limits: AdmissionLimits,
    outcomes: OwnedPermit,
}

impl Check {
    pub(super) fn new(
        limits: AdmissionLimits,
        outcomes: OwnedPermit,
    ) -> Result<Self, InitialSuccessionError> {
        if outcomes.request().units().items() == 0
            || outcomes.request().units().data_bytes()
                < compacted_outcome_data_bytes(limits).map_err(native_error)?
        {
            return Err(native_error(AdmissionError::InsufficientReservation));
        }
        Ok(Self { limits, outcomes })
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "locked transaction, native plan, separate grants and original authority deadline"
    )]
    pub(super) async fn require(
        &self,
        tx: &Transaction<'_>,
        recovery: &IngestRecovery,
        handoff: &InitialHandoff,
        registration: &ingest_registry::InitialRegistration,
        metadata: &OwnedPermit,
        lease: &mut PolicyLeaseTracker,
        policy: PolicyEpoch,
        cancelled: &AtomicBool,
    ) -> Result<(), InitialSuccessionError> {
        let predecessor = handoff
            .compacted_predecessor(recovery, metadata, cancelled)
            .map_err(InitialSuccessionError::Native)?
            .ok_or(InitialSuccessionError::Native(
                BootstrapError::StateMismatch,
            ))?;
        producer_progress::recovery::require_compacted_handoff(
            tx,
            recovery,
            &predecessor,
            metadata,
            cancelled,
            || producer_progress::recovery::authority(lease, policy, cancelled),
        )
        .await?;
        require_terminal(tx, registration, handoff).await?;
        let resolution = predecessor.resolution(cancelled).map_err(native_error)?;
        let verified = producer_compaction::resolve_initial_outcomes(
            tx,
            resolution,
            self.limits,
            &self.outcomes,
            lease,
            policy,
            cancelled,
        )
        .await
        .map_err(compaction_error)?;
        verified
            .validate(recovery, cancelled)
            .map_err(native_error)?;
        initial_history::check_delivery(lease, policy, cancelled)
    }
}

impl Check {
    pub(super) async fn require_live_initial(
        &self,
        tx: &Transaction<'_>,
        writer: &LiveInitialWriter,
        lease: &mut PolicyLeaseTracker,
        policy: PolicyEpoch,
        cancelled: &AtomicBool,
    ) -> Result<(), InitialSuccessionError> {
        let view = writer
            .native
            .compacted_initial_view(self.limits, &writer._permit, &self.outcomes, cancelled)
            .map_err(native_error)?
            .ok_or(OwnerError::InvalidInput)?;
        let fields = view.floor().fields();
        require_retired_predecessor(
            tx,
            writer.previous_registration(),
            fields.issued_through,
            fields.source_through,
            fields.closure_digest,
        )
        .await?;
        producer_progress::recovery::require_compacted_live_initial(tx, &view, cancelled, || {
            producer_progress::recovery::authority(lease, policy, cancelled)
        })
        .await?;
        let proof = producer_compaction::resolve_initial_outcomes(
            tx,
            view.resolution(cancelled).map_err(native_error)?,
            self.limits,
            &self.outcomes,
            lease,
            policy,
            cancelled,
        )
        .await
        .map_err(compaction_error)?;
        proof.validate_live(cancelled).map_err(native_error)?;
        initial_history::check_delivery(lease, policy, cancelled)
    }
}

pub(super) fn validate(
    check: Option<&Check>,
    recovery: &IngestRecovery,
    handoff: &InitialHandoff,
    metadata: &OwnedPermit,
    cancelled: &AtomicBool,
) -> Result<(), BootstrapError> {
    if check.is_some() {
        recovery.validate_compacted_initial_handoff(handoff, metadata, cancelled)
    } else {
        recovery.validate_initial_handoff(handoff)
    }
}

pub(super) fn apply(
    check: Option<&Check>,
    recovery: &mut IngestRecovery,
    handoff: &InitialHandoff,
    permit: OwnedPermit,
    cancelled: &AtomicBool,
    authority: impl FnMut() -> Result<(), BootstrapError>,
) -> Result<ObservedHandoff, BootstrapError> {
    if check.is_some() {
        recovery.apply_compacted_initial_handoff(handoff, permit, cancelled, authority)
    } else {
        recovery.apply_initial_handoff(handoff, permit, authority)
    }
}

pub(super) fn require_candidate(
    check: Option<&Check>,
    candidate: &InitialWriterCandidate,
    handoff: &InitialHandoff,
    metadata: &OwnedPermit,
    cancelled: &AtomicBool,
) -> Result<(), BootstrapError> {
    if check.is_some() {
        candidate.require_prepared_compacted_handoff(handoff, metadata, cancelled)
    } else {
        candidate.require_prepared_handoff(handoff)
    }
}

pub(super) fn activate(
    check: Option<&Check>,
    candidate: InitialWriterCandidate,
    handoff: InitialHandoff,
    metadata: &OwnedPermit,
    cancelled: &AtomicBool,
) -> Result<ActiveInitialWriter, BootstrapError> {
    if check.is_some() {
        candidate.activate_compacted(handoff, metadata, cancelled)
    } else {
        candidate.activate(handoff)
    }
}

fn native_error(error: AdmissionError) -> InitialSuccessionError {
    InitialSuccessionError::Native(BootstrapError::Compacted(error))
}

pub(super) fn compaction_error(error: ProducerCompactionError) -> InitialSuccessionError {
    match error {
        ProducerOperationError::Control(error) => ProducerOperationError::Control(error),
        ProducerOperationError::Registry(error) => ProducerOperationError::Registry(error),
        ProducerOperationError::Native(error) => native_error(error),
        ProducerOperationError::Reservation(error) => ProducerOperationError::Reservation(error),
        ProducerOperationError::Outcome(error) => ProducerOperationError::Outcome(error),
        ProducerOperationError::Progress(error) => ProducerOperationError::Progress(error),
        ProducerOperationError::CommitUnknown => ProducerOperationError::CommitUnknown,
        ProducerOperationError::DeliveryUnknown => ProducerOperationError::DeliveryUnknown,
    }
}

async fn require_terminal(
    tx: &Transaction<'_>,
    registration: &ingest_registry::InitialRegistration,
    handoff: &InitialHandoff,
) -> Result<(), InitialSuccessionError> {
    require_retired_predecessor(
        tx,
        registration,
        handoff.issued_through(),
        handoff.last_complete_sequence(),
        handoff.closure_digest(),
    )
    .await
}

pub(super) async fn require_retired_predecessor(
    tx: &Transaction<'_>,
    registration: &ingest_registry::InitialRegistration,
    issued: u64,
    source_through: u64,
    digest: [u8; 32],
) -> Result<(), InitialSuccessionError> {
    let slot = &registration.producer_slot_id.as_bytes().as_slice();
    tx.query_one(
        "SELECT control.require_initial_retirement($1::bytea,1,$2::bytea)",
        &[slot, &&digest[..]],
    )
    .await
    .map_err(backend)?;
    let row = tx
        .query_one(
            RETIRED_PREDECESSOR,
            &[
                slot,
                &registration.ingest_resource_id.as_bytes().as_slice(),
                &registration.ingest_role_instance_id.as_slice(),
                &registration.dataset_id.as_bytes().as_slice(),
                &&digest[..],
                &issued.to_string(),
                &i64::from(registration.shard_id),
                &source_through.to_string(),
            ],
        )
        .await
        .map_err(backend)?;
    if !row.try_get::<_, bool>(0).map_err(backend)? {
        return Err(OwnerError::InvalidMetadata.into());
    }
    Ok(())
}

// The current-initial reader deliberately checks all domains and forbids a
// successor. This historical predicate selects epoch1 while the caller locks
// and validates the separate prepared/activated epoch2 shape.
const RETIRED_PREDECESSOR: &str = "
    SELECT (p.state='retired' AND p.reserved_through_request=$6::text::numeric
               AND p.closure_digest=$5::bytea AND i.state='released'
               AND i.exclusion_method='storage_fenced' AND i.exclusion_digest=$5::bytea
               AND i.released_at_us=p.retired_at_us AND r.retired_at_us=p.retired_at_us
               AND s.state='closed' AND s.closure_digest=$5::bytea
               AND s.last_complete_sequence=$8::text::numeric
               AND s.closed_at_us<=p.retired_at_us
               AND NOT EXISTS (SELECT 1 FROM control.allocation_domains
                   WHERE dataset_id=$4::bytea AND owner_resource_id=$2::bytea AND owner_epoch=1
                       AND (state<>'closed' OR closure_digest IS DISTINCT FROM p.closure_digest
                           OR closed_at_us IS DISTINCT FROM p.retired_at_us))) IS TRUE
      FROM control.producer_epochs p
      JOIN control.owner_epochs i ON i.resource_id=$2::bytea AND i.epoch=1
      JOIN control.role_instances r ON r.role_instance_id=$3::bytea
      JOIN control.source_streams s ON s.dataset_id=$4::bytea AND s.shard_id=$7::bigint AND s.owner_epoch=1
     WHERE p.producer_slot_id=$1::bytea AND p.producer_epoch=1";

impl CompactedInitialRetirement<'_> {
    /// A confirmed current-initial read may construct data for a future
    /// preparation. Preparation and activation still recheck their own custody.
    pub fn handoff(
        &self,
        next: IngestManifest,
        cancelled: &AtomicBool,
    ) -> Result<InitialHandoff, AdmissionError> {
        InitialHandoff::from_compacted(self.native(), next, cancelled)
    }

    pub fn handoff_candidate(
        &self,
        candidate: &InitialWriterCandidate,
        policy: PolicyEpoch,
        cancelled: &AtomicBool,
    ) -> Result<InitialHandoff, AdmissionError> {
        candidate.handoff_compacted(self.native(), policy, cancelled)
    }
}

enum HistoricalNative<'a> {
    Initial(&'a RetainedCompactedPrefix),
    Prepared(&'a RetainedLineage),
}

impl ControlSession {
    /// Authenticate a retained preparation while its actual native owner is
    /// still epoch1. This never inserts intent or adopts its attempted writer.
    #[allow(
        clippy::too_many_arguments,
        reason = "native observation and separate SQL/outcome grants"
    )]
    pub async fn inspect_prepared_compacted_initial_gap(
        &mut self,
        recovery: &IngestRecovery,
        prefix: &RetainedCompactedPrefix,
        input: InitialHistoryRequest<'_>,
        limits: AdmissionLimits,
        metadata: OwnedPermit,
        outcomes: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<InitialGapPlan, InitialSuccessionError> {
        self.inspect_prepared_compacted_initial_in(
            recovery,
            HistoricalNative::Initial(prefix),
            input,
            limits,
            metadata,
            outcomes,
            validate,
        )
        .await
    }

    /// Reconstruct only an empty, unactivated native epoch2 preparation.
    /// The returned owned plan can verify an exact retry; it is not a candidate.
    #[allow(
        clippy::too_many_arguments,
        reason = "borrowed lineage and separate SQL/outcome grants"
    )]
    pub async fn inspect_prepared_compacted_initial_history(
        &mut self,
        recovery: &IngestRecovery,
        lineage: &RetainedLineage,
        input: InitialHistoryRequest<'_>,
        limits: AdmissionLimits,
        metadata: OwnedPermit,
        outcomes: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<InitialGapPlan, InitialSuccessionError> {
        self.inspect_prepared_compacted_initial_in(
            recovery,
            HistoricalNative::Prepared(lineage),
            input,
            limits,
            metadata,
            outcomes,
            validate,
        )
        .await
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "same historical protocol with two sealed native contexts"
    )]
    async fn inspect_prepared_compacted_initial_in(
        &mut self,
        recovery: &IngestRecovery,
        native: HistoricalNative<'_>,
        input: InitialHistoryRequest<'_>,
        limits: AdmissionLimits,
        metadata: OwnedPermit,
        outcomes: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<InitialGapPlan, InitialSuccessionError> {
        if metadata.request().units().items() == 0
            || metadata.request().units().data_bytes() < COMPACTED_HANDOFF_DATA_BYTES
        {
            return Err(InitialSuccessionError::Native(
                BootstrapError::InsufficientReservation,
            ));
        }
        let check = Check::new(limits, outcomes)?;
        let current = self.identity;
        let instance = *self.request.instance.as_bytes();
        let owner_epoch = match &native {
            HistoricalNative::Initial(_) => 1,
            HistoricalNative::Prepared(_) => 2,
        };
        let (tx, mut lease, policy) = self
            .begin_initial_history(recovery, &input, owner_epoch, validate)
            .await?;
        let intent = initial_history::read_intent(&tx, recovery).await?;
        let registration = ingest_registry::reconstruct_historical(
            &tx,
            instance,
            intent.record.previous_manifest(),
        )
        .await
        .map_err(InitialSuccessionError::Registry)?;
        initial_history::require_intent(&tx, &registration, &intent, current, policy).await?;
        let handoff = match native {
            HistoricalNative::Initial(prefix) => {
                recovery
                    .validate_pending_initial_handoff(&intent.record)
                    .map_err(InitialSuccessionError::Native)?;
                producer_progress::recovery::require_compacted(
                    &tx,
                    recovery,
                    prefix,
                    &metadata,
                    input.cancelled,
                    || producer_progress::recovery::authority(&mut lease, policy, input.cancelled),
                )
                .await?;
                let resolution = prefix
                    .resolution(recovery, input.cancelled)
                    .map_err(native_error)?;
                let verified = producer_compaction::resolve_initial_outcomes(
                    &tx,
                    resolution,
                    limits,
                    &check.outcomes,
                    &mut lease,
                    policy,
                    input.cancelled,
                )
                .await
                .map_err(compaction_error)?;
                InitialHandoff::from_compacted(
                    &verified,
                    *intent.record.next_manifest(),
                    input.cancelled,
                )
                .map_err(native_error)?
            }
            HistoricalNative::Prepared(lineage) => {
                let historical = recovery
                    .historical_initial(&intent.handoff)
                    .map_err(InitialSuccessionError::Native)?;
                let ancestor = lineage
                    .compacted_initial_ancestor_with_permit(
                        recovery,
                        limits,
                        &check.outcomes,
                        input.cancelled,
                    )
                    .map_err(native_error)?
                    .ok_or(InitialSuccessionError::Native(
                        BootstrapError::StateMismatch,
                    ))?;
                producer_progress::recovery::require_compacted_ancestor(
                    &tx,
                    recovery,
                    &ancestor,
                    &metadata,
                    input.cancelled,
                    || producer_progress::recovery::authority(&mut lease, policy, input.cancelled),
                )
                .await?;
                let resolution = ancestor.resolution(input.cancelled).map_err(native_error)?;
                let verified = producer_compaction::resolve_initial_outcomes(
                    &tx,
                    resolution,
                    limits,
                    &check.outcomes,
                    &mut lease,
                    policy,
                    input.cancelled,
                )
                .await
                .map_err(compaction_error)?;
                historical
                    .reconstruct_compacted_handoff(&verified, input.cancelled)
                    .map_err(native_error)?
            }
        };
        if handoff.encode().as_bytes() != intent.handoff.as_slice()
            || handoff.producer_slot_id() != registration.producer_slot_id
        {
            return Err(InitialSuccessionError::Native(
                BootstrapError::StateMismatch,
            ));
        }
        require_terminal(&tx, &registration, &handoff).await?;
        initial_history::finish(tx, &mut lease, policy, input.cancelled).await?;
        if self.client.is_closed() {
            return Err(InitialSuccessionError::DeliveryUnknown);
        }
        recovery
            .validate_compacted_initial_handoff(&handoff, &metadata, input.cancelled)
            .map_err(InitialSuccessionError::Native)?;
        initial_history::check_delivery(&mut lease, policy, input.cancelled)?;
        self.available = true;
        Ok(InitialGapPlan {
            registration,
            intent,
            handoff,
            _permit: metadata,
        })
    }
}
