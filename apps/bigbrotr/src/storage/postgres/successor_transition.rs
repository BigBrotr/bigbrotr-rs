//! Preparation and unactivated recovery for the epoch-indexed family. None of
//! these plans can adopt a recorded incarnation. The private activation child
//! consumes only a continuously retained, successfully prepared candidate.
use super::*;
use crate::{
    runtime::{OwnedPermit, policy_authority::PolicyLeaseTracker},
    storage::rocks::{
        store::{
            AdmissionError, BootstrapError, EmptySuccessorRetirement, IngestRecovery,
            ObservedSuccessorHandoff, RetainedTerminalKind, SuccessorCandidate, SuccessorHandoff,
        },
        succession::{LINEAGE_SCRATCH_BYTES, SuccessionRecord},
    },
};
use bigbrotr_model::PolicyEpoch;
use std::{mem::size_of, sync::atomic::Ordering};

#[path = "successor_transition_rows.rs"]
mod rows;
use rows::Intent;
#[path = "successor_activation.rs"]
mod activation;
pub(super) use activation::LiveSuccessorBinding;
pub use activation::{LiveSuccessorWriter, SuccessorActivation};
#[path = "successor_recovery.rs"]
mod recovery;
pub use recovery::{
    ActivatedSuccessorRecovery, ActivatedSuccessorResolution, ActivatedSuccessorRetirement,
    ActivatedSuccessorRetirementResolution, ActivatedSuccessorSourceRecovery,
    ActivatedSuccessorSourceResolution, ResolvedActivatedSuccessor,
    ResolvedActivatedSuccessorRetirement, ResolvedActivatedSuccessorSource, SuccessorRecoveryInput,
};

/// Scheduling is sampled once, under actual Control and dataset custody. A
/// delayed plan may expire; preparing it never silently resets either deadline.
pub struct SuccessorPreparationRequest<'a> {
    pub history: InitialHistoryRequest<'a>,
    pub lease_duration: Duration,
    pub outstanding_request_limit: u32,
}

/// Fixed positive plan plus exact accepted SQL coordinates. No history vector,
/// mutable recovery handle or writer capability is duplicated here.
pub struct SuccessorPreparationPlan {
    intent: Intent,
    native: SuccessorHandoff,
}
impl SuccessorPreparationPlan {
    pub fn record(&self) -> &SuccessionRecord {
        &self.intent.record
    }
    pub fn policy(&self) -> PolicySnapshot {
        self.intent.policy
    }
    pub fn created_at_us(&self) -> i64 {
        self.intent.created
    }
    pub fn lease_expires_at_us(&self) -> i64 {
        self.intent.deadline
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PreparationState {
    Fresh,
    Spent,
    Prepared,
}
impl PreparationState {
    fn start(&mut self) -> Result<(), OwnerError> {
        if *self != Self::Fresh {
            return Err(OwnerError::Unavailable);
        }
        *self = Self::Spent;
        Ok(())
    }
}

/// Caller-retained custody across every error, including unknown prepare COMMIT.
/// Construct the native candidate BEFORE acquiring Control: its constructor may
/// fail and drop recovery. Assemble this owner after the positive witness has
/// produced a plan and its borrows have ended. Assembly itself grants nothing.
/// Keep this native-first field alive until the supervisor closes native, then
/// releases/drops Control and joins the actual driver. There is no extraction.
pub struct SuccessorPreparation {
    candidate: SuccessorCandidate,
    history: SuccessorHistory,
    plan: SuccessorPreparationPlan,
    state: PreparationState,
    observed: Option<ObservedSuccessorHandoff>,
}
impl SuccessorPreparation {
    pub fn new(
        candidate: SuccessorCandidate,
        history: SuccessorHistory,
        plan: SuccessorPreparationPlan,
    ) -> Self {
        Self {
            candidate,
            history,
            plan,
            state: PreparationState::Fresh,
            observed: None,
        }
    }
    pub fn recovery(&self) -> &IngestRecovery {
        self.candidate.recovery()
    }
    pub fn record(&self) -> &SuccessionRecord {
        self.plan.record()
    }
    /// Confirmed preparation/native completion only, never activation.
    pub fn is_prepared(&self) -> bool {
        self.state == PreparationState::Prepared
            && self
                .observed
                .as_ref()
                .is_some_and(|observed| observed.record() == self.plan.record())
    }
}

pub struct SuccessorGapPlan {
    intent: Intent,
    native: SuccessorHandoff,
    control: ingest_registry::ControlOwner,
}
impl SuccessorGapPlan {
    pub fn record(&self) -> &SuccessionRecord {
        &self.intent.record
    }
}
pub struct SuccessorEmptyPlan {
    intent: Intent,
    native: EmptySuccessorRetirement,
    control: ingest_registry::ControlOwner,
}
impl SuccessorEmptyPlan {
    pub fn record(&self) -> &SuccessionRecord {
        &self.intent.record
    }
    pub fn closure_digest(&self) -> [u8; 32] {
        self.native.closure_digest()
    }
}

// All fixed plans, one bounded SQL projection and native input hashing scratch
// share the retained lineage's 128KiB scratch. Native transition grants are
// additional 8KiB/4KiB data+native grants; replay grants have already been freed.
const _: () = assert!(
    size_of::<SuccessorPreparation>()
        + size_of::<SuccessorGapPlan>()
        + size_of::<SuccessorEmptyPlan>()
        + 16 * 1024
        < LINEAGE_SCRATCH_BYTES as usize
);

impl ControlSession {
    pub async fn plan_successor_preparation(
        &mut self,
        candidate: &SuccessorCandidate,
        proof: &VerifiedSuccessorHistory<'_>,
        input: SuccessorPreparationRequest<'_>,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<SuccessorPreparationPlan, ProducerResolveError> {
        self.check_successor_plan_cancellation(input.history.cancelled)?;
        let duration = lease_micros(input.lease_duration, input.outstanding_request_limit)?;
        let history = proof.history();
        if history.state() != SuccessorHistoryState::Terminal
            || proof.verified_through() != history.native_epoch()
        {
            return Err(OwnerError::InvalidInput.into());
        }
        let current = self.identity;
        let (tx, mut lease, epoch) = self
            .begin_checked_successor_history(
                history,
                candidate.recovery(),
                &input.history,
                validate,
            )
            .await?;
        proof
            .validate(input.history.cancelled)
            .map_err(native_error)?;
        let policy = rows::current_policy(&tx, candidate.recovery()).await?;
        let native = candidate
            .handoff(proof.resolved_lineage(), epoch, input.history.cancelled)
            .map_err(native_error)?;
        let created = rows::clock(&tx).await?;
        let intent = Intent {
            record: *native.record(),
            role: candidate.role_instance_id(),
            policy,
            created,
            deadline: created
                .checked_add(duration)
                .ok_or(OwnerError::InvalidInput)?,
            limit: input.outstanding_request_limit,
            preparer: current,
        };
        rows::require_fresh(&tx, &intent).await?;
        candidate
            .require_handoff(history.lineage(), &native, input.history.cancelled)
            .map_err(native_error)?;
        successor_history::finish_read(tx, &mut lease, epoch, input.history.cancelled, current)
            .await?;
        if self.client.is_closed() {
            return Err(ProducerResolveError::DeliveryUnknown);
        }
        self.available = true;
        Ok(SuccessorPreparationPlan { intent, native })
    }

    /// One attempt only. Every failure leaves the caller's owner intact and
    /// spent. Only confirmed SQL COMMIT may precede the four native puts.
    pub async fn prepare_successor(
        &mut self,
        owner: &mut SuccessorPreparation,
        input: InitialHistoryRequest<'_>,
        permit: OwnedPermit,
        mut validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<(), ProducerResolveError> {
        let result: Result<ObservedSuccessorHandoff, ProducerResolveError> = async {
            owner.state.start()?;
            require_permit(&permit, 8192)?;
            let intent = &owner.plan.intent;
            require_control(owner.history.control(), intent.preparer, self.identity)?;
            if owner.history.state() != SuccessorHistoryState::Terminal {
                return Err(OwnerError::InvalidInput.into());
            }
            owner
                .candidate
                .require_handoff(owner.history.lineage(), &owner.plan.native, input.cancelled)
                .map_err(native_error)?;
            let current = self.identity;
            let (tx, mut lease, epoch) = self
                .begin_checked_successor_history(
                    &owner.history,
                    owner.candidate.recovery(),
                    &input,
                    &mut validate,
                )
                .await?;
            rows::require_policy(&tx, owner.candidate.recovery(), intent.policy).await?;
            rows::require_fresh(&tx, intent).await?;
            rows::insert(&tx, intent).await?;
            rows::require_intent(&tx, intent).await?;
            rows::require_target(&tx, intent, "prepared").await?;
            rows::require_live(&tx, current, Some(intent)).await?;
            owner
                .candidate
                .require_handoff(owner.history.lineage(), &owner.plan.native, input.cancelled)
                .map_err(native_error)?;
            initial_history::check_delivery(&mut lease, epoch, input.cancelled)?;
            tx.commit()
                .await
                .map_err(|_| ProducerResolveError::CommitUnknown)?;
            initial_history::check_delivery(&mut lease, epoch, input.cancelled)?;

            // Do not begin another operation or refresh the monotonic lease.
            // Reacquire the same actual Control before dataset/lineage locks.
            let tx = application_transaction(&mut self.client, 38)
                .await
                .map_err(transaction_error)?;
            rows::lock_control(&tx, current).await?;
            require(
                &tx,
                intent.record.fields().next_manifest.identity().dataset_id,
                current,
            )
            .await?;
            if snapshot(&tx, &self.request, &mut validate).await? != intent.policy {
                return Err(OwnerError::InvalidPolicy.into());
            }
            rows::require_intent(&tx, intent).await?;
            rows::require_target(&tx, intent, "prepared").await?;
            rows::require_live(&tx, current, Some(intent)).await?;
            initial_history::check_delivery(&mut lease, epoch, input.cancelled)?;
            let observed = owner
                .candidate
                .apply_handoff(
                    owner.history.lineage(),
                    &owner.plan.native,
                    permit,
                    input.cancelled,
                    || native_authority(&mut lease, epoch, input.cancelled),
                )
                .map_err(native_error)?;
            rows::require_target(&tx, intent, "prepared").await?;
            rows::require_live(&tx, current, Some(intent)).await?;
            owner
                .candidate
                .require_handoff(owner.history.lineage(), &owner.plan.native, input.cancelled)
                .map_err(native_error)?;
            successor_history::finish_read(tx, &mut lease, epoch, input.cancelled, current).await?;
            if self.client.is_closed() {
                return Err(ProducerResolveError::DeliveryUnknown);
            }
            Ok(observed)
        }
        .await;
        match result {
            Ok(observed) => {
                owner.observed = Some(observed);
                owner.state = PreparationState::Prepared;
                self.available = true;
                Ok(())
            }
            Err(error) => {
                owner.state = PreparationState::Spent;
                self.available = false;
                Err(error)
            }
        }
    }

    pub async fn plan_prepared_successor_gap(
        &mut self,
        recovery: &IngestRecovery,
        proof: &VerifiedSuccessorHistory<'_>,
        input: InitialHistoryRequest<'_>,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<SuccessorGapPlan, ProducerResolveError> {
        self.check_successor_plan_cancellation(input.cancelled)?;
        if proof.history().state() != SuccessorHistoryState::PendingNative {
            return Err(OwnerError::InvalidInput.into());
        }
        let current = self.identity;
        let (tx, mut lease, epoch) = self
            .begin_checked_successor_history(proof.history(), recovery, &input, validate)
            .await?;
        proof.validate(input.cancelled).map_err(native_error)?;
        let intent = rows::read(&tx, recovery, proof.history().registered_epoch()).await?;
        let native = SuccessorHandoff::reconstruct_gap(
            proof.resolved_lineage(),
            intent.record,
            input.cancelled,
        )
        .map_err(native_error)?;
        recovery
            .validate_successor_handoff(proof.history().lineage(), &native, input.cancelled)
            .map_err(native_error)?;
        successor_history::finish_read(tx, &mut lease, epoch, input.cancelled, current).await?;
        if self.client.is_closed() {
            return Err(ProducerResolveError::DeliveryUnknown);
        }
        self.available = true;
        Ok(SuccessorGapPlan {
            intent,
            native,
            control: current,
        })
    }

    /// Existing-only native completion: no SQL INSERT, no local candidate and no
    /// adoption even when the former preparation's Control is still current.
    pub async fn complete_prepared_successor_gap(
        &mut self,
        recovery: &mut IngestRecovery,
        history: &SuccessorHistory,
        plan: &SuccessorGapPlan,
        input: InitialHistoryRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<ObservedSuccessorHandoff, ProducerResolveError> {
        let result: Result<ObservedSuccessorHandoff, ProducerResolveError> = async {
            require_permit(&permit, 8192)?;
            require_control(history.control(), plan.control, self.identity)?;
            if history.state() != SuccessorHistoryState::PendingNative
                || history.registered_epoch() != plan.intent.epoch()
            {
                return Err(OwnerError::InvalidInput.into());
            }
            recovery
                .validate_successor_handoff(history.lineage(), &plan.native, input.cancelled)
                .map_err(native_error)?;
            let current = self.identity;
            let (tx, mut lease, epoch) = self
                .begin_successor_history(recovery, &input, validate)
                .await?;
            rows::require_intent(&tx, &plan.intent).await?;
            rows::require_target(&tx, &plan.intent, "prepared").await?;
            rows::require_live(&tx, current, None).await?;
            let observed = recovery
                .apply_successor_handoff(
                    history.lineage(),
                    &plan.native,
                    permit,
                    input.cancelled,
                    || native_authority(&mut lease, epoch, input.cancelled),
                )
                .map_err(native_error)?;
            rows::require_target(&tx, &plan.intent, "prepared").await?;
            recovery
                .validate_successor_handoff(history.lineage(), &plan.native, input.cancelled)
                .map_err(native_error)?;
            successor_history::finish_read(tx, &mut lease, epoch, input.cancelled, current).await?;
            if self.client.is_closed() {
                return Err(ProducerResolveError::DeliveryUnknown);
            }
            Ok(observed)
        }
        .await;
        self.available = result.is_ok();
        result
    }

    pub async fn plan_empty_successor_retirement(
        &mut self,
        recovery: &IngestRecovery,
        proof: &VerifiedSuccessorHistory<'_>,
        input: InitialHistoryRequest<'_>,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<SuccessorEmptyPlan, ProducerResolveError> {
        self.check_successor_plan_cancellation(input.cancelled)?;
        if !matches!(
            proof.history().state(),
            SuccessorHistoryState::PreparedOpen | SuccessorHistoryState::PreparedClosing
        ) {
            return Err(OwnerError::InvalidInput.into());
        }
        let current = self.identity;
        let (tx, mut lease, epoch) = self
            .begin_checked_successor_history(proof.history(), recovery, &input, validate)
            .await?;
        proof.validate(input.cancelled).map_err(native_error)?;
        let intent = rows::read(&tx, recovery, proof.history().registered_epoch()).await?;
        let native =
            EmptySuccessorRetirement::from_lineage(proof.resolved_lineage(), input.cancelled)
                .map_err(native_error)?;
        if native.record() != &intent.record {
            return Err(OwnerError::InvalidMetadata.into());
        }
        rows::require_empty_claims(&tx, &intent).await?;
        recovery
            .validate_empty_successor_retirement(
                proof.history().lineage(),
                &native,
                input.cancelled,
            )
            .map_err(native_error)?;
        successor_history::finish_read(tx, &mut lease, epoch, input.cancelled, current).await?;
        if self.client.is_closed() {
            return Err(ProducerResolveError::DeliveryUnknown);
        }
        self.available = true;
        Ok(SuccessorEmptyPlan {
            intent,
            native,
            control: current,
        })
    }

    /// Native close precedes the complete SQL retirement. Fresh recovery rebuilds
    /// the original positive prefix after any unknown/delivery failure.
    pub async fn retire_empty_successor(
        &mut self,
        recovery: &mut IngestRecovery,
        history: &SuccessorHistory,
        plan: &SuccessorEmptyPlan,
        input: InitialHistoryRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<i64, ProducerResolveError> {
        let result: Result<i64, ProducerResolveError> = async {
            require_permit(&permit, 4096)?;
            require_control(history.control(), plan.control, self.identity)?;
            if !matches!(
                history.state(),
                SuccessorHistoryState::PreparedOpen | SuccessorHistoryState::PreparedClosing
            ) || history.registered_epoch() != plan.intent.epoch()
            {
                return Err(OwnerError::InvalidInput.into());
            }
            recovery
                .validate_empty_successor_retirement(
                    history.lineage(),
                    &plan.native,
                    input.cancelled,
                )
                .map_err(native_error)?;
            let current = self.identity;
            let (tx, mut lease, epoch) = self
                .begin_successor_history(recovery, &input, validate)
                .await?;
            rows::require_intent(&tx, &plan.intent).await?;
            if rows::require_target(&tx, &plan.intent, "empty")
                .await?
                .is_some()
            {
                return Err(OwnerError::InvalidMetadata.into());
            }
            rows::require_empty_claims(&tx, &plan.intent).await?;
            let _closing = recovery
                .close_empty_successor(
                    history.lineage(),
                    &plan.native,
                    permit,
                    input.cancelled,
                    || native_authority(&mut lease, epoch, input.cancelled),
                )
                .map_err(native_error)?;
            rows::require_live(&tx, current, None).await?;
            let time = rows::retire_empty(&tx, &plan.intent, plan.native.closure_digest()).await?;
            if rows::require_target(&tx, &plan.intent, "empty").await? != Some(time) {
                return Err(OwnerError::InvalidMetadata.into());
            }
            recovery
                .validate_empty_successor_retirement(
                    history.lineage(),
                    &plan.native,
                    input.cancelled,
                )
                .map_err(native_error)?;
            rows::require_live(&tx, current, None).await?;
            initial_history::check_delivery(&mut lease, epoch, input.cancelled)?;
            tx.commit()
                .await
                .map_err(|_| ProducerResolveError::CommitUnknown)?;
            initial_history::check_delivery(&mut lease, epoch, input.cancelled)?;
            if self.client.is_closed() {
                return Err(ProducerResolveError::DeliveryUnknown);
            }
            Ok(time)
        }
        .await;
        self.available = result.is_ok();
        result
    }

    /// Terminal retries replay THROUGH the empty head, whereas an unfinished
    /// attempt replays only its predecessor. Do not downgrade this full proof or
    /// reapply today's workflow veto to an immutable historical terminal result.
    pub async fn observe_retired_empty_successor(
        &mut self,
        recovery: &IngestRecovery,
        proof: &VerifiedSuccessorHistory<'_>,
        input: InitialHistoryRequest<'_>,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<i64, ProducerResolveError> {
        let history = proof.history();
        self.check_successor_plan_cancellation(input.cancelled)?;
        let summary = proof.last_epoch();
        if history.state() != SuccessorHistoryState::Terminal
            || history.native_epoch() < 3
            || proof.verified_through() != history.native_epoch()
            || summary.terminal_kind != RetainedTerminalKind::UnactivatedEmpty
            || summary.resolved_requests != 0
            || summary.last_complete_sequence != 0
        {
            return Err(OwnerError::InvalidInput.into());
        }
        let current = self.identity;
        let (tx, mut lease, epoch) = self
            .begin_checked_successor_history(history, recovery, &input, validate)
            .await?;
        proof.validate(input.cancelled).map_err(native_error)?;
        let intent = rows::read(&tx, recovery, history.registered_epoch()).await?;
        let time = rows::require_target(&tx, &intent, "empty")
            .await?
            .ok_or(OwnerError::InvalidMetadata)?;
        rows::require_empty_digest(&tx, &intent, summary.digest).await?;
        successor_history::finish_read(tx, &mut lease, epoch, input.cancelled, current).await?;
        if self.client.is_closed() {
            return Err(ProducerResolveError::DeliveryUnknown);
        }
        self.available = true;
        Ok(time)
    }

    fn check_successor_plan_cancellation(
        &mut self,
        cancelled: &std::sync::atomic::AtomicBool,
    ) -> Result<(), OwnerError> {
        if cancelled.load(Ordering::Relaxed) {
            self.available = false;
            return Err(OwnerError::Unavailable);
        }
        Ok(())
    }
}

fn native_error(error: BootstrapError) -> ProducerResolveError {
    ProducerResolveError::Native(AdmissionError::from(error).into())
}
fn require_control(
    history: ingest_registry::ControlOwner,
    plan: ingest_registry::ControlOwner,
    current: ingest_registry::ControlOwner,
) -> Result<(), OwnerError> {
    if history != current || plan != current {
        return Err(OwnerError::InvalidInput);
    }
    Ok(())
}
fn require_permit(permit: &OwnedPermit, bytes: u64) -> Result<(), ProducerResolveError> {
    if permit.request().units().data_bytes() < bytes
        || permit.request().units().native_bytes() < bytes
        || permit.request().units().items() == 0
    {
        return Err(native_error(BootstrapError::InsufficientReservation));
    }
    Ok(())
}
fn lease_micros(duration: Duration, limit: u32) -> Result<i64, OwnerError> {
    let micros = i64::try_from(duration.as_micros()).map_err(|_| OwnerError::InvalidInput)?;
    if micros <= 0 || !(1..=64).contains(&limit) {
        return Err(OwnerError::InvalidInput);
    }
    Ok(micros)
}
fn native_authority(
    lease: &mut PolicyLeaseTracker,
    epoch: PolicyEpoch,
    cancelled: &std::sync::atomic::AtomicBool,
) -> Result<(), BootstrapError> {
    if cancelled.load(Ordering::Relaxed) || lease.check_delivery(epoch).is_err() {
        return Err(BootstrapError::AuthorityUnavailable);
    }
    Ok(())
}

#[cfg(test)]
#[path = "successor_transition_tests.rs"]
mod tests;
