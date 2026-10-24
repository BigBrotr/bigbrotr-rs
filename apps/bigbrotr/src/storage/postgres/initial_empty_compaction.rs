//! Empty epoch2 retirement with an explicitly authenticated compacted ancestor.
//! Neither portable intent bytes nor an original-only proof select this path.
use super::*;
use crate::storage::rocks::store::{
    AdmissionError, AdmissionLimits, CompactedEmptyInitialRetirement, RetainedLineage,
    compacted_outcome_data_bytes,
};

/// Fixed native proof and original SQL identity, with both serial read grants.
/// This plan cannot activate the target or supply acknowledgement authority.
pub struct CompactedInitialEmptyRetirementPlan {
    retirement: CompactedEmptyInitialRetirement,
    registration: ingest_registry::InitialRegistration,
    intent: initial_history::Intent,
    metadata: OwnedPermit,
    outcomes: OwnedPermit,
    limits: AdmissionLimits,
}
impl CompactedInitialEmptyRetirementPlan {
    pub fn retirement(&self) -> &CompactedEmptyInitialRetirement {
        &self.retirement
    }
    pub fn allocation_domains(&self) -> usize {
        self.registration.allocation_domains.len()
    }
}

impl ControlSession {
    /// Read the exact unactivated empty target and its canonical ancestor. The
    /// lineage carries native custody; no failed original proof is retried here.
    #[allow(
        clippy::too_many_arguments,
        reason = "explicit native lineage, historical authority and separate metadata/effect grants"
    )]
    pub async fn inspect_compacted_empty_initial_attempt(
        &mut self,
        recovery: &IngestRecovery,
        lineage: RetainedLineage,
        input: InitialHistoryRequest<'_>,
        limits: AdmissionLimits,
        outcomes: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<CompactedInitialEmptyRetirementPlan, InitialSuccessionError> {
        let metadata = lineage.progress_scratch();
        require_metadata_grant(metadata)?;
        if outcomes.request().units().items() == 0
            || outcomes.request().units().data_bytes()
                < compacted_outcome_data_bytes(limits).map_err(native_error)?
        {
            return Err(native_error(AdmissionError::InsufficientReservation));
        }
        let current = self.identity;
        let instance = *self.request.instance.as_bytes();
        let (tx, mut lease, policy) = self
            .begin_empty_initial_history(recovery, &input, validate)
            .await?;
        let intent = initial_history::read_intent(&tx, recovery).await?;
        let native = recovery
            .compacted_empty_initial_attempt(&intent.handoff, &lineage, input.cancelled)
            .map_err(InitialSuccessionError::Native)?;
        let registration = ingest_registry::reconstruct_historical(
            &tx,
            instance,
            intent.record.previous_manifest(),
        )
        .await
        .map_err(InitialSuccessionError::Registry)?;
        // Retired parent locks precede the typed-head producer UPDATE lock.
        initial_history::require_intent_identity(&tx, &registration, &intent, current, policy)
            .await?;
        let terminal =
            require_target(&tx, &registration, &intent, expected_digest(&intent)).await?;
        if terminal.is_some() && !native.is_closing() {
            return Err(OwnerError::InvalidMetadata.into());
        }
        require_ancestor_terminal(&tx, &registration, &intent).await?;
        let ancestor = native
            .compacted_initial_ancestor(limits, &outcomes, input.cancelled)
            .map_err(native_error)?;
        // This typed floor branch requires the authenticated 39/40 schemas.
        producer_progress::recovery::require_compacted_ancestor(
            &tx,
            recovery,
            &ancestor,
            metadata,
            input.cancelled,
            || producer_progress::recovery::authority(&mut lease, policy, input.cancelled),
        )
        .await?;
        let resolution = ancestor.resolution(input.cancelled).map_err(native_error)?;
        let verified = producer_compaction::resolve_initial_outcomes(
            &tx,
            resolution,
            limits,
            &outcomes,
            &mut lease,
            policy,
            input.cancelled,
        )
        .await
        .map_err(initial_compaction::compaction_error)?;
        let retirement = native
            .reconstruct_retirement(&verified, input.cancelled)
            .map_err(native_error)?;
        if retirement.handoff().as_bytes() != intent.handoff
            || retirement.closure_digest() != expected_digest(&intent)
        {
            return Err(OwnerError::InvalidMetadata.into());
        }
        initial_history::finish(tx, &mut lease, policy, input.cancelled).await?;
        recovery
            .validate_compacted_empty_initial_retirement(&retirement, metadata, input.cancelled)
            .map_err(InitialSuccessionError::Native)?;
        initial_history::check_delivery(&mut lease, policy, input.cancelled)?;
        if self.client.is_closed() {
            return Err(InitialSuccessionError::DeliveryUnknown);
        }
        self.available = true;
        Ok(CompactedInitialEmptyRetirementPlan {
            retirement,
            registration,
            intent,
            metadata: lineage.into_metadata_permit(),
            outcomes,
            limits,
        })
    }

    /// Recheck canonical ancestor state in this final transaction before the
    /// one native closing write and the existing atomic empty SQL transition.
    pub async fn retire_compacted_empty_initial_attempt(
        &mut self,
        recovery: &mut IngestRecovery,
        plan: &CompactedInitialEmptyRetirementPlan,
        input: InitialHistoryRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<i64, InitialSuccessionError> {
        require_metadata_grant(&permit)?;
        if permit.request().units().native_bytes() < 4096 {
            return Err(InitialSuccessionError::Native(
                BootstrapError::InsufficientReservation,
            ));
        }
        if input.cancelled.load(Ordering::Relaxed) {
            self.available = false;
            return Err(InitialSuccessionError::Native(
                BootstrapError::AuthorityUnavailable,
            ));
        }
        if !recovery.matches_configured_root(input.ingest_root)
            || plan.registration.dataset_id != self.request.dataset
            || plan.registration.instance_id != *self.request.instance.as_bytes()
        {
            return Err(OwnerError::InvalidInput.into());
        }
        let native_closing = recovery
            .validate_compacted_empty_initial_retirement(
                &plan.retirement,
                &plan.metadata,
                input.cancelled,
            )
            .map_err(InitialSuccessionError::Native)?;
        let current = self.identity;
        let (tx, mut lease, policy) = self
            .begin_empty_initial_history(recovery, &input, validate)
            .await?;
        initial_history::require_intent_identity(
            &tx,
            &plan.registration,
            &plan.intent,
            current,
            policy,
        )
        .await?;
        let digest = plan.retirement.closure_digest();
        let terminal = require_target(&tx, &plan.registration, &plan.intent, digest).await?;
        if terminal.is_some() && !native_closing {
            return Err(OwnerError::InvalidMetadata.into());
        }
        require_ancestor_terminal(&tx, &plan.registration, &plan.intent).await?;
        {
            let ancestor = plan
                .retirement
                .predecessor(recovery, &plan.metadata, input.cancelled)
                .map_err(InitialSuccessionError::Native)?;
            producer_progress::recovery::require_compacted_empty_retirement(
                &tx,
                recovery,
                &ancestor,
                &plan.metadata,
                input.cancelled,
                || producer_progress::recovery::authority(&mut lease, policy, input.cancelled),
            )
            .await?;
            let resolution = ancestor.resolution(input.cancelled).map_err(native_error)?;
            let verified = producer_compaction::resolve_initial_outcomes(
                &tx,
                resolution,
                plan.limits,
                &plan.outcomes,
                &mut lease,
                policy,
                input.cancelled,
            )
            .await
            .map_err(initial_compaction::compaction_error)?;
            verified
                .validate(recovery, input.cancelled)
                .map_err(native_error)?;
        }
        initial_history::check_delivery(&mut lease, policy, input.cancelled)?;
        let _closing = recovery
            .close_compacted_empty_initial_attempt(
                &plan.retirement,
                permit,
                input.cancelled,
                || {
                    producer_progress::recovery::authority(&mut lease, policy, input.cancelled)
                        .map_err(|_| BootstrapError::AuthorityUnavailable)
                },
            )
            .map_err(InitialSuccessionError::Native)?;
        let retired_at = match terminal {
            Some(time) => time,
            None => {
                let time = retire_target(&tx, &plan.registration, &plan.intent, digest).await?;
                if require_target(&tx, &plan.registration, &plan.intent, digest).await?
                    != Some(time)
                {
                    return Err(OwnerError::InvalidMetadata.into());
                }
                time
            }
        };
        initial_history::check_delivery(&mut lease, policy, input.cancelled)?;
        tx.commit()
            .await
            .map_err(|_| InitialSuccessionError::CommitUnknown)?;
        // A committed result still cannot be delivered from changed native or
        // input state. Keep the actual native owner and dispatch grant alive.
        recovery
            .validate_compacted_empty_initial_retirement(
                &plan.retirement,
                &plan.metadata,
                input.cancelled,
            )
            .map_err(InitialSuccessionError::Native)?;
        initial_history::check_delivery(&mut lease, policy, input.cancelled)?;
        if self.client.is_closed() {
            return Err(InitialSuccessionError::DeliveryUnknown);
        }
        self.available = true;
        Ok(retired_at)
    }
}

fn native_error(error: AdmissionError) -> InitialSuccessionError {
    InitialSuccessionError::Native(BootstrapError::Compacted(error))
}

async fn require_ancestor_terminal(
    tx: &Transaction<'_>,
    registration: &ingest_registry::InitialRegistration,
    intent: &initial_history::Intent,
) -> Result<(), InitialSuccessionError> {
    initial_compaction::require_retired_predecessor(
        tx,
        registration,
        intent.record.issued_through(),
        intent.record.last_complete_sequence(),
        intent.record.closure_digest(),
    )
    .await
}
