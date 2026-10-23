//! Complete only an already durable initial preparation after process loss.
//! Historical metadata and a reconstructed plan never activate its incarnation.
use super::*;
use crate::{
    runtime::OwnedPermit,
    storage::rocks::store::{
        AdmissionLimits, BootstrapError, IngestRecovery, InitialHandoff, ObservedHandoff,
        ResolvedSource,
    },
};
use std::sync::atomic::Ordering;

/// Existing PostgreSQL preparation with the actual predecessor still native.
/// Its borrow retains exclusion while ordinary positive retirement reads prove
/// the original source. The retained metadata is not a dispatch capability.
pub struct PreparedInitialGap<'a> {
    recovery: &'a IngestRecovery,
    registration: ingest_registry::InitialRegistration,
    intent: initial_history::Intent,
    permit: OwnedPermit,
}
impl PreparedInitialGap<'_> {
    pub fn registration(&self) -> &ingest_registry::InitialRegistration {
        &self.registration
    }
    pub fn issued_through(&self) -> u64 {
        self.intent.record.issued_through()
    }

    /// Consume checked metadata only after a complete positive source proof.
    /// Drop the source borrow before requesting mutable native completion. The
    /// owned plan keeps the exact directory/sequence guards and metadata grant;
    /// dropping that borrow does not make intervening writes acceptable.
    pub fn reconstruct_handoff(
        self,
        source: &ResolvedSource<'_>,
    ) -> Result<InitialGapPlan, BootstrapError> {
        if !std::ptr::eq(source.recovery(), self.recovery) {
            return Err(BootstrapError::StateMismatch);
        }
        let handoff = InitialHandoff::from_source(source, *self.intent.record.next_manifest())?;
        if handoff.encode().as_bytes() != self.intent.handoff.as_slice() {
            return Err(BootstrapError::StateMismatch);
        }
        self.recovery.validate_initial_handoff(&handoff)?;
        Ok(InitialGapPlan {
            registration: self.registration,
            intent: self.intent,
            handoff,
            _permit: self.permit,
        })
    }
}

/// Exact existing intent plus a positive local proof; never admission authority.
/// The metadata grant stays owned across native dispatch and any exact retry.
pub struct InitialGapPlan {
    pub(super) registration: ingest_registry::InitialRegistration,
    pub(super) intent: initial_history::Intent,
    pub(super) handoff: InitialHandoff,
    pub(super) _permit: OwnedPermit,
}
impl InitialGapPlan {
    pub fn handoff(&self) -> &InitialHandoff {
        &self.handoff
    }
}

impl ControlSession {
    /// Read an exact existing preparation while native remains at epoch one.
    /// Missing intent and completed/mixed native states refuse without repair.
    /// Completed epoch two has its separate historical-read adapter.
    pub async fn inspect_prepared_initial_gap<'a>(
        &mut self,
        recovery: &'a IngestRecovery,
        input: InitialHistoryRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<PreparedInitialGap<'a>, InitialSuccessionError> {
        if permit.request().units().data_bytes() < 128 * 1024
            || permit.request().units().items() == 0
        {
            return Err(InitialSuccessionError::Native(
                BootstrapError::InsufficientReservation,
            ));
        }
        let instance = *self.request.instance.as_bytes();
        let current = self.identity;
        let (tx, mut lease, epoch) = self
            .begin_initial_history(recovery, &input, 1, validate)
            .await?;
        let intent = initial_history::read_intent(&tx, recovery).await?;
        recovery
            .validate_pending_initial_handoff(&intent.record)
            .map_err(InitialSuccessionError::Native)?;
        let registration = ingest_registry::reconstruct_historical(
            &tx,
            instance,
            intent.record.previous_manifest(),
        )
        .await
        .map_err(InitialSuccessionError::Registry)?;
        initial_history::require_intent(&tx, &registration, &intent, current, epoch).await?;
        initial_history::finish(tx, &mut lease, epoch, input.cancelled).await?;
        if self.client.is_closed() {
            return Err(InitialSuccessionError::DeliveryUnknown);
        }
        self.available = true;
        Ok(PreparedInitialGap {
            recovery,
            registration,
            intent,
            permit,
        })
    }

    /// Complete only the immutable preparation already observed and proven.
    /// No PostgreSQL row is inserted or rewritten. Current Control and accepted
    /// policy custody remain held across the native batch and confirmed rollback.
    /// A retained exact plan may observe its completed batch without rewriting.
    ///
    /// Any failure after dispatch can leave the synchronized transition durable.
    /// Keep/join the bounded native worker and connection driver; cancellation
    /// cannot interrupt FFI or permit another owner. Reopen and positively inspect
    /// the actual state under fresh Control, never infer absence or activation.
    pub async fn complete_prepared_initial_gap(
        &mut self,
        recovery: &mut IngestRecovery,
        plan: &InitialGapPlan,
        input: InitialHistoryRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<ObservedHandoff, InitialSuccessionError> {
        self.complete_prepared_initial_gap_in(recovery, plan, input, permit, None, validate)
            .await
    }

    /// Complete only an existing intent with a separately sealed compacted
    /// predecessor. Exact completed retries retain the same zero-write path.
    #[allow(
        clippy::too_many_arguments,
        reason = "native plan and separate metadata/outcome grants"
    )]
    pub async fn complete_prepared_compacted_initial_gap(
        &mut self,
        recovery: &mut IngestRecovery,
        plan: &InitialGapPlan,
        input: InitialHistoryRequest<'_>,
        limits: AdmissionLimits,
        metadata: OwnedPermit,
        outcomes: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<ObservedHandoff, InitialSuccessionError> {
        let check = initial_compaction::Check::new(limits, outcomes)?;
        self.complete_prepared_initial_gap_in(
            recovery,
            plan,
            input,
            metadata,
            Some(check),
            validate,
        )
        .await
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "shared existing-intent protocol with explicit native grants"
    )]
    async fn complete_prepared_initial_gap_in(
        &mut self,
        recovery: &mut IngestRecovery,
        plan: &InitialGapPlan,
        input: InitialHistoryRequest<'_>,
        permit: OwnedPermit,
        compacted: Option<initial_compaction::Check>,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<ObservedHandoff, InitialSuccessionError> {
        if input.cancelled.load(Ordering::Relaxed) {
            return Err(InitialSuccessionError::Native(
                BootstrapError::AuthorityUnavailable,
            ));
        }
        if permit.request().units().data_bytes() < 8192
            || permit.request().units().native_bytes() < 8192
            || permit.request().units().items() == 0
        {
            return Err(InitialSuccessionError::Native(
                BootstrapError::InsufficientReservation,
            ));
        }
        if !recovery.matches_configured_root(input.ingest_root)
            || plan.registration.dataset_id != self.request.dataset
            || plan.registration.instance_id != *self.request.instance.as_bytes()
        {
            return Err(OwnerError::InvalidInput.into());
        }
        initial_compaction::validate(
            compacted.as_ref(),
            recovery,
            &plan.handoff,
            &permit,
            input.cancelled,
        )
        .map_err(InitialSuccessionError::Native)?;
        let current = self.identity;
        let owner_epoch = recovery.manifest().identity().owner_epoch.get();
        let (tx, mut lease, epoch) = self
            .begin_initial_history(recovery, &input, owner_epoch, validate)
            .await?;
        initial_history::require_intent(&tx, &plan.registration, &plan.intent, current, epoch)
            .await?;
        if let Some(check) = &compacted {
            check
                .require(
                    &tx,
                    recovery,
                    &plan.handoff,
                    &plan.registration,
                    &permit,
                    &mut lease,
                    epoch,
                    input.cancelled,
                )
                .await?;
        }
        initial_history::check_delivery(&mut lease, epoch, input.cancelled)?;
        let observed = initial_compaction::apply(
            compacted.as_ref(),
            recovery,
            &plan.handoff,
            permit,
            input.cancelled,
            || {
                if input.cancelled.load(Ordering::Relaxed) {
                    return Err(BootstrapError::AuthorityUnavailable);
                }
                lease
                    .check_delivery(epoch)
                    .map_err(|_| BootstrapError::AuthorityUnavailable)
            },
        )
        .map_err(InitialSuccessionError::Native)?;
        initial_history::finish(tx, &mut lease, epoch, input.cancelled).await?;
        if self.client.is_closed() {
            return Err(InitialSuccessionError::DeliveryUnknown);
        }
        self.available = true;
        Ok(observed)
    }
}
