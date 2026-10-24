//! Explicit compacted ancestor checks for activated epoch2 protocols.
//! Each caller retains its distinct pending source or ACK capability.
use super::*;
use crate::{
    runtime::policy_authority::PolicyLeaseTracker,
    storage::rocks::store::{
        ActivatedSourceResolution, AdmissionLimits, RecoveryAcknowledgementResolution,
        ResolvedActivatedSource, compacted_outcome_data_bytes,
    },
};
use bigbrotr_model::PolicyEpoch;
use tokio_postgres::Transaction;

pub(in crate::storage::postgres::control_owner) struct Inspection {
    limits: AdmissionLimits,
    outcomes: OwnedPermit,
}

fn effect_error(error: AdmissionError) -> InitialSuccessionError {
    InitialSuccessionError::Native(BootstrapError::Compacted(error))
}

fn require_effects(
    limits: AdmissionLimits,
    permit: &OwnedPermit,
) -> Result<(), InitialSuccessionError> {
    if permit.request().units().items() == 0
        || permit.request().units().data_bytes()
            < compacted_outcome_data_bytes(limits).map_err(effect_error)?
    {
        return Err(effect_error(AdmissionError::InsufficientReservation));
    }
    Ok(())
}

pub(super) fn validate(
    native: &ActivatedInitial,
    recovery: &IngestRecovery,
    cancelled: &AtomicBool,
) -> Result<(), InitialSuccessionError> {
    if native.has_compacted_initial() {
        native.validate_compacted(recovery, cancelled)
    } else {
        native.validate(recovery)
    }
    .map_err(InitialSuccessionError::Native)
}

pub(in crate::storage::postgres::control_owner) enum Pending<'a, 'b> {
    Acknowledgement(Option<&'a mut RecoveryAcknowledgementResolution<'b>>),
    Source(&'a mut ActivatedSourceResolution<'b>),
}

pub(in crate::storage::postgres::control_owner) fn validate_source(
    inspection: Option<&Inspection>,
    source: &ResolvedActivatedSource<'_>,
    cancelled: &AtomicBool,
) -> Result<(), SourceCloseError> {
    if inspection.is_some() && source.has_compacted_initial() {
        source.validate_compacted(cancelled)
    } else {
        source.validate()
    }
    .map_err(SourceCloseError::Native)
}

impl Inspection {
    pub(in crate::storage::postgres::control_owner) fn new(
        limits: AdmissionLimits,
        outcomes: OwnedPermit,
    ) -> Result<Self, InitialSuccessionError> {
        require_effects(limits, &outcomes)?;
        Ok(Self { limits, outcomes })
    }

    /// Rebind the retained ancestor in the final source/terminal transaction.
    /// Earlier confirmed replay does not authenticate a later SQL snapshot.
    #[allow(
        clippy::too_many_arguments,
        reason = "one locked source transaction and its bounded native authority"
    )]
    pub(in crate::storage::postgres::control_owner) async fn require_source(
        &self,
        tx: &Transaction<'_>,
        closed: &ClosedActivatedInitialProducer,
        source: &ResolvedActivatedSource<'_>,
        lease: &mut PolicyLeaseTracker,
        policy: PolicyEpoch,
        cancelled: &AtomicBool,
    ) -> Result<(), SourceCloseError> {
        if !source.has_compacted_initial() {
            return Ok(());
        }
        let recovery = source.recovery();
        let ancestor = source
            .compacted_initial_ancestor(self.limits, &self.outcomes, cancelled)
            .map_err(effect_error)?;
        producer_progress::recovery::require_compacted_ancestor(
            tx,
            recovery,
            &ancestor,
            closed
                .native
                .compacted_progress_scratch()
                .map_err(SourceCloseError::Native)?,
            cancelled,
            || producer_progress::recovery::authority(lease, policy, cancelled),
        )
        .await?;
        let verified = producer_compaction::resolve_initial_outcomes(
            tx,
            ancestor.resolution(cancelled).map_err(effect_error)?,
            self.limits,
            &self.outcomes,
            lease,
            policy,
            cancelled,
        )
        .await
        .map_err(initial_compaction::compaction_error)?;
        let floor = verified.floor().fields();
        initial_compaction::require_retired_predecessor(
            tx,
            &closed.metadata.previous,
            floor.issued_through,
            floor.source_through,
            floor.closure_digest,
        )
        .await?;
        verified
            .validate(recovery, cancelled)
            .map_err(effect_error)?;
        initial_history::check_delivery(lease, policy, cancelled)
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "one locked transaction and its native, grant and authority context"
    )]
    pub(super) async fn require(
        &self,
        tx: &Transaction<'_>,
        recovery: &IngestRecovery,
        native: &ActivatedInitial,
        previous: &ingest_registry::InitialRegistration,
        intent: &initial_history::Intent,
        lease: &mut PolicyLeaseTracker,
        policy: PolicyEpoch,
        cancelled: &AtomicBool,
    ) -> Result<(), InitialSuccessionError> {
        let ancestor = native
            .compacted_initial_ancestor(recovery, self.limits, &self.outcomes, cancelled)
            .map_err(effect_error)?;
        producer_progress::recovery::require_compacted_ancestor(
            tx,
            recovery,
            &ancestor,
            native
                .compacted_progress_scratch()
                .map_err(InitialSuccessionError::Native)?,
            cancelled,
            || producer_progress::recovery::authority(lease, policy, cancelled),
        )
        .await?;
        initial_compaction::require_retired_predecessor(
            tx,
            previous,
            intent.record.issued_through(),
            intent.record.last_complete_sequence(),
            intent.record.closure_digest(),
        )
        .await
    }
}

impl ControlSession {
    /// Opt in to the sealed initial floor while retaining the original branch.
    /// Source and retirement factories never call this positive-only entry.
    #[allow(
        clippy::too_many_arguments,
        reason = "explicit metadata and serial decoded-effect grants"
    )]
    pub async fn inspect_activated_initial_producer_with_compacted(
        &mut self,
        recovery: &IngestRecovery,
        input: InitialHistoryRequest<'_>,
        metadata: OwnedPermit,
        limits: AdmissionLimits,
        outcomes: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<ActivatedInitialRecovery, InitialSuccessionError> {
        require_effects(limits, &outcomes)?;
        self.inspect_activated_producer_in(
            recovery,
            input,
            metadata,
            Purpose::Positive,
            Some(Inspection { limits, outcomes }),
            validate,
        )
        .await
    }

    /// Prove the canonical ancestor again at the post-closing native sequence.
    /// Only a confirmed read can unlock target recording on the ACK reducer.
    #[allow(
        clippy::too_many_arguments,
        reason = "borrowed closing/ACK proofs and a separate bounded effect grant"
    )]
    pub async fn resolve_activated_compacted_predecessor(
        &mut self,
        recovery: &IngestRecovery,
        closed: &ClosedActivatedInitialProducer,
        input: InitialHistoryRequest<'_>,
        limits: AdmissionLimits,
        outcomes: OwnedPermit,
        acknowledgement: Option<&mut RecoveryAcknowledgementResolution<'_>>,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<u64, InitialSuccessionError> {
        self.resolve_activated_compacted_predecessor_in(
            recovery,
            closed,
            input,
            limits,
            outcomes,
            Pending::Acknowledgement(acknowledgement),
            validate,
        )
        .await
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "private protocol-specific pending proof and independent effect grant"
    )]
    pub(in crate::storage::postgres::control_owner) async fn resolve_activated_compacted_predecessor_in(
        &mut self,
        recovery: &IngestRecovery,
        closed: &ClosedActivatedInitialProducer,
        input: InitialHistoryRequest<'_>,
        limits: AdmissionLimits,
        outcomes: OwnedPermit,
        pending: Pending<'_, '_>,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<u64, InitialSuccessionError> {
        if !closed.has_compacted_initial()
            || matches!(&pending, Pending::Acknowledgement(_))
                != matches!(closed.metadata.purpose, Purpose::Positive)
        {
            return Err(OwnerError::InvalidInput.into());
        }
        require_effects(limits, &outcomes)?;
        closed
            .validate(recovery, input.cancelled)
            .map_err(InitialSuccessionError::Native)?;
        require_deployment(&closed.metadata, self)?;
        let current = self.identity;
        let (tx, mut lease, policy) = self
            .begin_initial_history_locked(
                recovery,
                &input,
                2,
                closed.metadata.purpose.schema(),
                validate,
            )
            .await?;
        let state = require_metadata(&tx, &closed.metadata, current, policy).await?;
        if !state.producer_closing {
            return Err(OwnerError::InvalidMetadata.into());
        }
        let ancestor = closed
            .native
            .compacted_initial_ancestor(recovery, limits, &outcomes, input.cancelled)
            .map_err(effect_error)?;
        producer_progress::recovery::require_compacted_ancestor(
            &tx,
            recovery,
            &ancestor,
            closed
                .native
                .compacted_progress_scratch()
                .map_err(InitialSuccessionError::Native)?,
            input.cancelled,
            || producer_progress::recovery::authority(&mut lease, policy, input.cancelled),
        )
        .await?;
        let verified = producer_compaction::resolve_initial_outcomes(
            &tx,
            ancestor.resolution(input.cancelled).map_err(effect_error)?,
            limits,
            &outcomes,
            &mut lease,
            policy,
            input.cancelled,
        )
        .await
        .map_err(initial_compaction::compaction_error)?;
        let floor = verified.floor().fields();
        initial_compaction::require_retired_predecessor(
            &tx,
            &closed.metadata.previous,
            floor.issued_through,
            floor.source_through,
            floor.closure_digest,
        )
        .await?;
        let count = floor.issued_through;
        initial_history::finish(tx, &mut lease, policy, input.cancelled).await?;
        if self.client.is_closed() {
            return Err(InitialSuccessionError::DeliveryUnknown);
        }
        verified
            .validate(recovery, input.cancelled)
            .map_err(effect_error)?;
        initial_history::check_delivery(&mut lease, policy, input.cancelled)?;
        let authority = || {
            initial_history::check_delivery::<BootstrapError>(&mut lease, policy, input.cancelled)
                .map_err(|_| BootstrapError::AuthorityUnavailable)
        };
        match pending {
            Pending::Acknowledgement(Some(acknowledgement)) => {
                acknowledgement.confirm_compacted_initial(&verified, authority)
            }
            Pending::Source(source) => source.confirm_compacted_initial(&verified, authority),
            Pending::Acknowledgement(None) => Ok(()),
        }
        .map_err(InitialSuccessionError::Native)?;
        self.available = true;
        Ok(count)
    }
}
