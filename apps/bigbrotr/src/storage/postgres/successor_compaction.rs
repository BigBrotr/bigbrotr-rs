//! One retained grant for compacted ancestor rechecks at later proof use.
//! The existing flat classifier still owns terminal/target/workflow locking.
use super::*;
use crate::storage::rocks::store::{
    AdmissionLimits, CompactedSuccessorPredecessor, RetainedLineage, compacted_outcome_data_bytes,
};
use std::sync::atomic::AtomicBool;

pub(in crate::storage::postgres::control_owner) struct Check {
    limits: AdmissionLimits,
    effects: OwnedPermit,
}
impl Check {
    pub(super) fn new(
        limits: AdmissionLimits,
        effects: OwnedPermit,
    ) -> Result<Self, ProducerResolveError> {
        if effects.request().units().items() == 0
            || effects.request().units().data_bytes()
                < compacted_outcome_data_bytes(limits).map_err(effect_error)?
        {
            return Err(effect_error(AdmissionError::InsufficientReservation));
        }
        Ok(Self { limits, effects })
    }

    pub(super) fn validate_activation(
        &self,
        owner: &crate::storage::rocks::store::NativeSuccessorActivation,
        cancelled: &AtomicBool,
    ) -> Result<(), ProducerResolveError> {
        owner
            .compacted_predecessor(self.limits, &self.effects, cancelled)
            .map_err(effect_error)?;
        Ok(())
    }
    pub(super) async fn require_activation(
        &self,
        tx: &Transaction<'_>,
        owner: &crate::storage::rocks::store::NativeSuccessorActivation,
        lease: &mut PolicyLeaseTracker,
        policy: PolicyEpoch,
        cancelled: &AtomicBool,
    ) -> Result<(), ProducerResolveError> {
        let view = owner
            .compacted_predecessor(self.limits, &self.effects, cancelled)
            .map_err(effect_error)?;
        self.require(
            tx,
            owner.recovery().ok_or(OwnerError::Unavailable)?,
            owner.lineage().ok_or(OwnerError::Unavailable)?,
            &view,
            lease,
            policy,
            cancelled,
        )
        .await
    }
    pub(in crate::storage::postgres::control_owner) fn validate_live(
        &self,
        native: &crate::storage::rocks::store::ActiveSuccessorWriter,
        cancelled: &AtomicBool,
    ) -> Result<(), ProducerResolveError> {
        native
            .compacted_view(self.limits, &self.effects, cancelled)
            .map_err(effect_error)?;
        Ok(())
    }
    pub(in crate::storage::postgres::control_owner) async fn require_live(
        &self,
        tx: &Transaction<'_>,
        native: &crate::storage::rocks::store::ActiveSuccessorWriter,
        lease: &mut PolicyLeaseTracker,
        policy: PolicyEpoch,
        cancelled: &AtomicBool,
    ) -> Result<(), ProducerResolveError> {
        let view = native
            .compacted_view(self.limits, &self.effects, cancelled)
            .map_err(effect_error)?;
        producer_progress::recovery::require_compacted_live(tx, &view, cancelled, || {
            producer_progress::recovery::authority(lease, policy, cancelled)
        })
        .await?;
        let verified = producer_compaction::resolve_initial_outcomes(
            tx,
            view.resolution(cancelled).map_err(effect_error)?,
            self.limits,
            &self.effects,
            lease,
            policy,
            cancelled,
        )
        .await
        .map_err(super::super::successor_history::compacted_error)?;
        verified.validate_live(cancelled).map_err(effect_error)?;
        initial_history::check_delivery(lease, policy, cancelled)
    }

    pub(super) fn handoff<'a>(
        &self,
        recovery: &'a IngestRecovery,
        lineage: &'a RetainedLineage,
        plan: &'a SuccessorHandoff,
        cancelled: &'a AtomicBool,
    ) -> Result<CompactedSuccessorPredecessor<'a>, ProducerResolveError> {
        CompactedSuccessorPredecessor::handoff(
            lineage,
            recovery,
            plan,
            self.limits,
            &self.effects,
            cancelled,
        )
        .map_err(effect_error)
    }
    pub(super) fn empty<'a>(
        &self,
        recovery: &'a IngestRecovery,
        lineage: &'a RetainedLineage,
        plan: &'a EmptySuccessorRetirement,
        cancelled: &'a AtomicBool,
    ) -> Result<CompactedSuccessorPredecessor<'a>, ProducerResolveError> {
        CompactedSuccessorPredecessor::empty(
            lineage,
            recovery,
            plan,
            self.limits,
            &self.effects,
            cancelled,
        )
        .map_err(effect_error)
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "locked transaction, sealed view, lineage scratch and unchanged authority deadline"
    )]
    pub(super) async fn require(
        &self,
        tx: &Transaction<'_>,
        recovery: &IngestRecovery,
        lineage: &RetainedLineage,
        view: &CompactedSuccessorPredecessor<'_>,
        lease: &mut PolicyLeaseTracker,
        policy: PolicyEpoch,
        cancelled: &AtomicBool,
    ) -> Result<(), ProducerResolveError> {
        producer_progress::recovery::require_compacted_successor(
            tx,
            recovery,
            view,
            lineage.progress_scratch(),
            cancelled,
            || producer_progress::recovery::authority(lease, policy, cancelled),
        )
        .await?;
        let verified = producer_compaction::resolve_initial_outcomes(
            tx,
            view.resolution(cancelled).map_err(effect_error)?,
            self.limits,
            &self.effects,
            lease,
            policy,
            cancelled,
        )
        .await
        .map_err(super::super::successor_history::compacted_error)?;
        verified
            .validate(recovery, cancelled)
            .map_err(effect_error)?;
        initial_history::check_delivery(lease, policy, cancelled)
    }

    pub(super) fn validate_history(
        &self,
        recovery: &IngestRecovery,
        history: &SuccessorHistory,
        cancelled: &AtomicBool,
    ) -> Result<(), ProducerResolveError> {
        history
            .lineage()
            .compacted_initial_ancestor_with_permit(recovery, self.limits, &self.effects, cancelled)
            .map_err(effect_error)?
            .ok_or(OwnerError::InvalidInput)?;
        Ok(())
    }

    pub(super) async fn require_history(
        &self,
        tx: &Transaction<'_>,
        recovery: &IngestRecovery,
        history: &SuccessorHistory,
        lease: &mut PolicyLeaseTracker,
        policy: PolicyEpoch,
        cancelled: &AtomicBool,
    ) -> Result<(), ProducerResolveError> {
        let ancestor = history
            .lineage()
            .compacted_initial_ancestor_with_permit(recovery, self.limits, &self.effects, cancelled)
            .map_err(effect_error)?
            .ok_or(OwnerError::InvalidInput)?;
        producer_progress::recovery::require_compacted_ancestor(
            tx,
            recovery,
            &ancestor,
            history.lineage().progress_scratch(),
            cancelled,
            || producer_progress::recovery::authority(lease, policy, cancelled),
        )
        .await?;
        let verified = producer_compaction::resolve_initial_outcomes(
            tx,
            ancestor.resolution(cancelled).map_err(effect_error)?,
            self.limits,
            &self.effects,
            lease,
            policy,
            cancelled,
        )
        .await
        .map_err(super::super::successor_history::compacted_error)?;
        verified
            .validate(recovery, cancelled)
            .map_err(effect_error)?;
        initial_history::check_delivery(lease, policy, cancelled)
    }
}

pub(super) fn require_basis(
    check: Option<&Check>,
    lineage: &RetainedLineage,
    compacted: bool,
) -> Result<(), ProducerResolveError> {
    require_basis_presence(check.is_some(), lineage.has_compacted_initial(), compacted)
}

pub(super) fn require_basis_presence(
    check: bool,
    retained: bool,
    compacted: bool,
) -> Result<(), ProducerResolveError> {
    if check != compacted || retained != compacted {
        return Err(OwnerError::InvalidInput.into());
    }
    Ok(())
}

fn effect_error(error: AdmissionError) -> ProducerResolveError {
    ProducerResolveError::Native(error.into())
}

pub(super) fn require_candidate(
    check: Option<&Check>,
    candidate: &SuccessorCandidate,
    history: &SuccessorHistory,
    plan: &SuccessorHandoff,
    cancelled: &AtomicBool,
) -> Result<(), BootstrapError> {
    if check.is_some() {
        candidate.require_compacted_handoff(history.lineage(), plan, cancelled)
    } else {
        candidate.require_handoff(history.lineage(), plan, cancelled)
    }
}
pub(super) fn validate_handoff(
    check: Option<&Check>,
    recovery: &IngestRecovery,
    history: &SuccessorHistory,
    plan: &SuccessorHandoff,
    cancelled: &AtomicBool,
) -> Result<(), BootstrapError> {
    if check.is_some() {
        recovery.validate_compacted_successor_handoff(history.lineage(), plan, cancelled)
    } else {
        recovery.validate_successor_handoff(history.lineage(), plan, cancelled)
    }
}
pub(super) fn validate_empty(
    check: Option<&Check>,
    recovery: &IngestRecovery,
    history: &SuccessorHistory,
    plan: &EmptySuccessorRetirement,
    cancelled: &AtomicBool,
) -> Result<(), BootstrapError> {
    if check.is_some() {
        recovery.validate_compacted_empty_successor_retirement(history.lineage(), plan, cancelled)
    } else {
        recovery.validate_empty_successor_retirement(history.lineage(), plan, cancelled)
    }
}
