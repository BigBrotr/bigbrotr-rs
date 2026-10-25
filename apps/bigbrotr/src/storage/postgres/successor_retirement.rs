//! Existing-only positive retirement of an activated repeatable successor.
//! Native custody stays with the caller. This distinct owner cannot repair,
//! close a source, abandon work or recreate a writer from retained metadata.
use super::*;
use crate::storage::rocks::store::{
    NativeSuccessorRetirement, ResolvedSuccessorRetirement, RetainedRequestInput,
    SuccessorRetirementResolution,
};

#[path = "successor_retirement_rows.rs"]
mod retirement_rows;
use retirement_rows::Context;

pub struct ActivatedSuccessorRetirement {
    native: NativeSuccessorRetirement,
    context: Context,
    state: ClosingState,
    compacted: Option<successor_compaction::Check>,
}
pub struct ActivatedSuccessorRetirementResolution<'a> {
    context: &'a Context,
    recovery: &'a IngestRecovery,
    cancelled: &'a AtomicBool,
    native: SuccessorRetirementResolution<'a>,
    compacted: Option<&'a successor_compaction::Check>,
}
/// One non-cloneable result of the one-shot retirement replay. It is consumed
/// by the terminal operation, including on error or unknown commit; the owner
/// and exclusive native handle remain externally retained until teardown.
pub struct ResolvedActivatedSuccessorRetirement<'a> {
    context: &'a Context,
    recovery: &'a IngestRecovery,
    native: ResolvedSuccessorRetirement<'a>,
    compacted: Option<&'a successor_compaction::Check>,
}

// Native owns the same compact Vec and half of the 128KiB scratch. Account for
// this separate PG half, both tuple buffers and simultaneous fixed projections.
// Input/outcome, transport and the final bounded job projection have own grants.
const _: () = assert!(
    size_of::<ActivatedSuccessorRetirement>() - size_of::<NativeSuccessorRetirement>()
        + size_of::<ActivatedSuccessorRetirementResolution<'_>>()
        - size_of::<SuccessorRetirementResolution<'_>>()
        + size_of::<ResolvedActivatedSuccessorRetirement<'_>>()
        - size_of::<ResolvedSuccessorRetirement<'_>>()
        + 2 * size_of::<Context>()
        + 2 * size_of::<Epoch>()
        + 2 * size_of::<[(u64, [u8; 32], [u8; 32]); 64]>()
        + 16 * 1024
        < LINEAGE_SCRATCH_BYTES as usize / 2
);

impl ActivatedSuccessorRetirement {
    pub fn epoch(&self) -> u64 {
        self.context.head.epoch()
    }
    pub fn producer_slot_id(&self) -> ProducerSlotId {
        self.native.producer_slot_id()
    }
    pub fn issued_through(&self) -> u64 {
        self.native.issued_through()
    }
    pub fn compacted_requests(&self) -> u64 {
        self.native.compacted_requests()
    }
    pub fn reserved_through(&self) -> u64 {
        self.context.head.reserved
    }
    pub fn retired_at_us(&self) -> Option<i64> {
        self.context.terminal.retired_at
    }
    pub fn is_closed(&self) -> bool {
        self.state == ClosingState::Closed
    }
    /// Staged input is not accepted by the retirement factory, even if unreserved.
    pub fn staged_input(&self) -> Option<SuccessorRecoveryInput> {
        None
    }
    pub fn resolution<'a>(
        &'a mut self,
        recovery: &'a IngestRecovery,
        cancelled: &'a AtomicBool,
    ) -> Result<ActivatedSuccessorRetirementResolution<'a>, BootstrapError> {
        if !self.is_closed() {
            return Err(BootstrapError::AuthorityUnavailable);
        }
        // The native owner spends its replay phase before any fallible work.
        // Split the fixed immutable PG context from its one native allocation.
        Ok(ActivatedSuccessorRetirementResolution {
            context: &self.context,
            recovery,
            cancelled,
            native: self.native.resolution(recovery, cancelled)?,
            compacted: self.compacted.as_ref(),
        })
    }
}
impl ActivatedSuccessorRetirementResolution<'_> {
    pub fn needs_compacted_initial(&self) -> bool {
        self.native.needs_compacted_initial()
    }
    pub fn current_epoch(&self) -> Option<u64> {
        self.native.current_epoch()
    }
    pub fn next_request(&self) -> Option<ProducerRequestId> {
        self.native.next_request()
    }
}
impl ResolvedActivatedSuccessorRetirement<'_> {
    pub fn recovery_acknowledgement(
        &self,
        cancelled: &AtomicBool,
    ) -> Result<crate::storage::rocks::store::RecoveryAcknowledgement, BootstrapError> {
        self.validate(cancelled)?;
        self.native.recovery_acknowledgement(cancelled)
    }
    pub fn into_recovery_acknowledgement(
        self,
        cancelled: &AtomicBool,
    ) -> Result<crate::storage::rocks::store::RecoveryAcknowledgement, BootstrapError> {
        self.recovery_acknowledgement(cancelled)
    }
    pub fn verified_through(&self) -> u64 {
        self.native.verified_through()
    }
    pub fn last_epoch(&self) -> LineageEpochSummary {
        self.native.last_epoch()
    }
    pub fn validate(&self, cancelled: &AtomicBool) -> Result<(), BootstrapError> {
        self.native.validate(cancelled)
    }
}

impl ControlSession {
    pub async fn inspect_activated_successor_retirement(
        &mut self,
        recovery: &IngestRecovery,
        input: SuccessorReplayRequest<'_>,
        metadata_permit: OwnedPermit,
        input_permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<ActivatedSuccessorRetirement, ProducerResolveError> {
        self.inspect_activated_successor_retirement_in(
            recovery,
            input,
            metadata_permit,
            input_permit,
            None,
            validate,
        )
        .await
    }

    /// Require a compacted initial floor for existing-only retirement. This
    /// neither creates a native barrier nor repairs copies or workflow state.
    #[allow(
        clippy::too_many_arguments,
        reason = "distinct metadata, input and retained canonical grants"
    )]
    pub async fn inspect_activated_successor_retirement_with_compacted(
        &mut self,
        recovery: &IngestRecovery,
        input: SuccessorReplayRequest<'_>,
        metadata_permit: OwnedPermit,
        input_permit: OwnedPermit,
        effects: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<ActivatedSuccessorRetirement, ProducerResolveError> {
        let check = successor_compaction::Check::new(input.limits.event, effects)?;
        self.inspect_activated_successor_retirement_in(
            recovery,
            input,
            metadata_permit,
            input_permit,
            Some(check),
            validate,
        )
        .await
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "private fixed-basis factory with separately retained grants"
    )]
    async fn inspect_activated_successor_retirement_in(
        &mut self,
        recovery: &IngestRecovery,
        input: SuccessorReplayRequest<'_>,
        metadata_permit: OwnedPermit,
        input_permit: OwnedPermit,
        compacted: Option<successor_compaction::Check>,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<ActivatedSuccessorRetirement, ProducerResolveError> {
        // Admit the same bounded original decoder as the subsequent replay.
        // No staged original is decoded or normalized during this inspection.
        let required = RetainedRequestInput::required_data_bytes(input.limits.input)
            .map_err(|error| ProducerResolveError::Native(ProducerRecoveryError::Input(error)))?;
        if input_permit.request().units().data_bytes() < required
            || input_permit.request().units().items() == 0
        {
            return Err(native_error(BootstrapError::InsufficientReservation));
        }
        let _input_permit = input_permit;
        let current = self.identity;
        let operation = history_request(&input);
        let (tx, mut lease, policy) = self
            .begin_activated_successor_recovery(recovery, &operation, validate)
            .await?;
        let epoch = recovery.manifest().identity().owner_epoch.get();
        let intent = rows::read(&tx, recovery, epoch).await?;
        let retired = retirement_rows::require_context(&tx, &intent, current).await?;
        let activation = projection::activation(&tx, &intent).await?;
        let head = history_rows::read_epoch(&tx, recovery, epoch).await?;
        let terminal = retirement_rows::terminal(&tx, &intent, current, retired).await?;
        let native = if compacted.is_some() {
            NativeSuccessorRetirement::inspect_with_compacted_initial(
                recovery,
                metadata_permit,
                input.cancelled,
            )
        } else {
            NativeSuccessorRetirement::inspect(recovery, metadata_permit, input.cancelled)
        }
        .map_err(native_error)?;
        successor_compaction::require_basis_presence(
            compacted.is_some(),
            native.compacted_requests() != 0,
            compacted.is_some(),
        )?;
        let context = Context {
            intent,
            activation,
            head,
            terminal,
            control: current,
        };
        retirement_rows::require_native(&tx, recovery, &context, &native, input.cancelled).await?;
        if let Some(check) = &compacted {
            check
                .resolve_retirement(&tx, recovery, &native, &mut lease, policy, input.cancelled)
                .await?;
        }
        // No input decoder is live during this fixed streaming read. Its
        // retained staging allowance is separate from the native lineage Vec.
        for epoch in 1..=context.head.epoch() {
            if epoch == 1 && compacted.is_some() {
                continue;
            }
            let entry = native.entry(epoch).ok_or(OwnerError::InvalidMetadata)?;
            producer_progress::recovery::require(
                &tx,
                recovery,
                entry.manifest(),
                native.producer_slot_id(),
                &_input_permit,
                || producer_progress::recovery::authority(&mut lease, policy, input.cancelled),
            )
            .await?;
        }
        successor_history::finish_read(tx, &mut lease, policy, input.cancelled, current).await?;
        if let Some(check) = &compacted {
            check.validate_retirement(recovery, &native, input.cancelled)?;
        }
        native
            .validate(recovery, input.cancelled)
            .map_err(native_error)?;
        initial_history::check_delivery(&mut lease, policy, input.cancelled)?;
        if self.client.is_closed() {
            return Err(ProducerResolveError::DeliveryUnknown);
        }
        self.available = true;
        Ok(ActivatedSuccessorRetirement {
            native,
            context,
            state: ClosingState::Inspected,
            compacted,
        })
    }

    /// Observe an already CLOSING native marker, never create a barrier. A
    /// failed observation remains spent while the caller retains both owners.
    pub async fn observe_activated_successor_retirement(
        &mut self,
        recovery: &IngestRecovery,
        owner: &mut ActivatedSuccessorRetirement,
        input: InitialHistoryRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<(), ProducerResolveError> {
        owner.state.start()?;
        let current = self.identity;
        let (tx, mut lease, policy) = self
            .begin_checked_successor_retirement(
                recovery,
                &owner.context,
                &owner.native,
                owner.compacted.as_ref(),
                &input,
                validate,
            )
            .await?;
        owner
            .native
            .observe_closed(recovery, permit, input.cancelled, || {
                native_authority(&mut lease, policy, input.cancelled)
            })
            .map_err(native_error)?;
        successor_history::finish_read(tx, &mut lease, policy, input.cancelled, current).await?;
        owner
            .native
            .validate(recovery, input.cancelled)
            .map_err(native_error)?;
        initial_history::check_delivery(&mut lease, policy, input.cancelled)?;
        if self.client.is_closed() {
            return Err(ProducerResolveError::DeliveryUnknown);
        }
        owner.state = ClosingState::Closed;
        self.available = true;
        Ok(())
    }

    /// Confirm the canonical floor under this exact retirement owner. There is
    /// no reconstructed initial Event mask and no ordinary replay of its inputs.
    pub async fn inspect_activated_successor_retirement_compacted_initial(
        &mut self,
        resolution: &mut ActivatedSuccessorRetirementResolution<'_>,
        input: InitialHistoryRequest<'_>,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<crate::storage::rocks::store::CompactedInitialSummary, ProducerResolveError> {
        let result = async {
            if !resolution.native.needs_compacted_initial() {
                return Err(OwnerError::InvalidInput.into());
            }
            let current = self.identity;
            let (tx, mut lease, policy) = self
                .begin_checked_successor_retirement(
                    resolution.recovery,
                    resolution.context,
                    resolution.native.owner(),
                    resolution.compacted,
                    &input,
                    validate,
                )
                .await?;
            let check = resolution.compacted.ok_or(OwnerError::InvalidInput)?;
            let proof = check
                .resolve_retirement(
                    &tx,
                    resolution.recovery,
                    resolution.native.owner(),
                    &mut lease,
                    policy,
                    resolution.cancelled,
                )
                .await?;
            successor_history::finish_read(tx, &mut lease, policy, input.cancelled, current)
                .await?;
            resolution
                .native
                .accept_compacted_initial(&proof, || {
                    if self.client.is_closed() {
                        return Err(AdmissionError::AuthorityUnavailable);
                    }
                    producer_progress::recovery::authority(&mut lease, policy, input.cancelled)
                })
                .map_err(|error| ProducerResolveError::Native(error.into()))
        }
        .await;
        if result.is_err() {
            resolution.native.poison();
        }
        self.available = result.is_ok();
        result
    }

    /// Both current and ancestral originals require an existing exact copy.
    /// Native Event classification includes rejected and already durable input;
    /// nullable SQL fields cannot silently turn that request into Evidence.
    pub async fn inspect_next_activated_successor_retirement_request(
        &mut self,
        resolution: &mut ActivatedSuccessorRetirementResolution<'_>,
        input: SuccessorReplayRequest<'_>,
        input_permit: OwnedPermit,
        outcome_permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<ProducerRequestId, ProducerResolveError> {
        let result = async {
            let request = resolution.next_request().ok_or(OwnerError::InvalidInput)?;
            let current = self.identity;
            let operation = history_request(&input);
            let (tx, mut lease, policy) = self
                .begin_checked_successor_retirement(
                    resolution.recovery,
                    resolution.context,
                    resolution.native.owner(),
                    resolution.compacted,
                    &operation,
                    validate,
                )
                .await?;
            let original = producer_requests::read(
                &tx,
                resolution.context.head.manifest.identity().dataset_id,
                request,
            )
            .await
            .map_err(ProducerResolveError::Reservation)?
            .ok_or(ProducerResolveError::Native(
                ProducerRecoveryError::Unresolved,
            ))?;
            let entry = resolution
                .native
                .owner()
                .entry(request.producer_epoch.get())
                .ok_or(OwnerError::InvalidMetadata)?;
            let manifest: [u8; 32] = Sha256::digest(entry.manifest().encode().as_bytes()).into();
            if original.manifest_sha256 != manifest {
                return Err(OwnerError::InvalidMetadata.into());
            }
            let pending = resolution
                .native
                .preview_next(
                    original.digest,
                    input.limits,
                    input_permit,
                    outcome_permit,
                    || {
                        native_authority(&mut lease, policy, input.cancelled)
                            .map_err(AdmissionError::from)
                    },
                )
                .map_err(ProducerResolveError::Native)?;
            if pending.request() != request
                || pending.resolved().input_bytes() != original.input_bytes
            {
                return Err(ProducerResolveError::Outcome(
                    producer_outcomes::OutcomeError::Conflict,
                ));
            }
            producer_retirement::require_retirement_copy(
                &tx,
                pending.resolved(),
                request,
                manifest,
            )
            .await?;
            successor_history::finish_read(tx, &mut lease, policy, input.cancelled, current)
                .await?;
            pending
                .accept(|| {
                    if self.client.is_closed() {
                        return Err(BootstrapError::AuthorityUnavailable);
                    }
                    native_authority(&mut lease, policy, input.cancelled)
                })
                .map_err(native_error)?;
            Ok(request)
        }
        .await;
        if result.is_err() {
            resolution.native.poison();
        }
        self.available = result.is_ok();
        result
    }

    pub async fn finish_activated_successor_retirement_epoch(
        &mut self,
        resolution: &mut ActivatedSuccessorRetirementResolution<'_>,
        input: InitialHistoryRequest<'_>,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<LineageEpochSummary, ProducerResolveError> {
        let result = async {
            let epoch = resolution.current_epoch().ok_or(OwnerError::InvalidInput)?;
            if resolution.next_request().is_some() {
                return Err(OwnerError::InvalidInput.into());
            }
            let current = self.identity;
            let (tx, mut lease, policy) = self
                .begin_checked_successor_retirement(
                    resolution.recovery,
                    resolution.context,
                    resolution.native.owner(),
                    resolution.compacted,
                    &input,
                    validate,
                )
                .await?;
            let row = history_rows::read_epoch(&tx, resolution.recovery, epoch).await?;
            let summary = resolution.native.finish_epoch().map_err(native_error)?;
            if epoch < resolution.context.head.epoch() {
                row.require_summary(&summary)?;
            } else {
                retirement_rows::require_summary(
                    &summary,
                    resolution.context,
                    resolution.native.owner(),
                )?;
            }
            successor_history::finish_read(tx, &mut lease, policy, input.cancelled, current)
                .await?;
            resolution
                .native
                .owner()
                .validate(resolution.recovery, resolution.cancelled)
                .map_err(native_error)?;
            initial_history::check_delivery(&mut lease, policy, input.cancelled)?;
            if self.client.is_closed() {
                return Err(ProducerResolveError::DeliveryUnknown);
            }
            Ok(summary)
        }
        .await;
        if result.is_err() {
            resolution.native.poison();
        }
        self.available = result.is_ok();
        result
    }

    pub async fn finish_activated_successor_retirement<'a>(
        &mut self,
        resolution: ActivatedSuccessorRetirementResolution<'a>,
        input: InitialHistoryRequest<'_>,
        limits: AdmissionLimits,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<ResolvedActivatedSuccessorRetirement<'a>, ProducerResolveError> {
        let current = self.identity;
        let (tx, mut lease, policy) = self
            .begin_checked_successor_retirement(
                resolution.recovery,
                resolution.context,
                resolution.native.owner(),
                resolution.compacted,
                &input,
                validate,
            )
            .await?;
        let native = resolution
            .native
            .finish(limits, permit)
            .map_err(ProducerResolveError::Native)?;
        retirement_rows::require_summary(&native.last_epoch(), resolution.context, native.owner())?;
        require_recorded(
            &tx,
            resolution.recovery,
            &native,
            &mut lease,
            policy,
            input.cancelled,
        )
        .await?;
        native
            .validate(resolution.cancelled)
            .map_err(native_error)?;
        successor_history::finish_read(tx, &mut lease, policy, input.cancelled, current).await?;
        native
            .validate(resolution.cancelled)
            .map_err(native_error)?;
        initial_history::check_delivery(&mut lease, policy, input.cancelled)?;
        if self.client.is_closed() {
            return Err(ProducerResolveError::DeliveryUnknown);
        }
        self.available = true;
        Ok(ResolvedActivatedSuccessorRetirement {
            context: resolution.context,
            recovery: resolution.recovery,
            native,
            compacted: resolution.compacted,
        })
    }

    /// Consume the only result before any fallible work. One transaction changes
    /// the twelve target rows and their trigger-generated transaction receipt;
    /// exact terminal retry changes none of them or the source's row version.
    pub async fn retire_activated_successor_producer(
        &mut self,
        proof: ResolvedActivatedSuccessorRetirement<'_>,
        input: InitialHistoryRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<u64, ProducerResolveError> {
        self.retire_activated_successor_producer_in(proof, input, permit, false, validate)
            .await
    }

    /// Consume only a compacted retirement proof. The ordinary consumer stays
    /// strict; neither path can repair prerequisites or rearm the spent owner.
    pub async fn retire_activated_successor_producer_with_compacted(
        &mut self,
        proof: ResolvedActivatedSuccessorRetirement<'_>,
        input: InitialHistoryRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<u64, ProducerResolveError> {
        self.retire_activated_successor_producer_in(proof, input, permit, true, validate)
            .await
    }

    async fn retire_activated_successor_producer_in(
        &mut self,
        proof: ResolvedActivatedSuccessorRetirement<'_>,
        input: InitialHistoryRequest<'_>,
        permit: OwnedPermit,
        expected_compacted: bool,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<u64, ProducerResolveError> {
        // Taking this non-Clone proof spends the only completed replay even on
        // a wrong-basis or grant refusal. Its native owner already spent its
        // cursor phase and remains in caller custody; there is no fallback.
        successor_compaction::require_basis_presence(
            proof.compacted.is_some(),
            proof.native.owner().compacted_requests() != 0,
            expected_compacted,
        )?;
        if permit.request().units().data_bytes() < 128 * 1024
            || permit.request().units().items() == 0
        {
            return Err(native_error(BootstrapError::InsufficientReservation));
        }
        let _permit = permit;
        let context = proof.context;
        let current = self.identity;
        let (tx, mut lease, policy) = self
            .begin_checked_successor_retirement(
                proof.recovery,
                context,
                proof.native.owner(),
                proof.compacted,
                &input,
                validate,
            )
            .await?;
        proof.validate(input.cancelled).map_err(native_error)?;
        retirement_rows::require_summary(&proof.last_epoch(), context, proof.native.owner())?;
        require_recorded(
            &tx,
            proof.recovery,
            &proof.native,
            &mut lease,
            policy,
            input.cancelled,
        )
        .await?;
        let gaps = retirement_rows::require_event_jobs(&tx, context, &proof.last_epoch()).await?;
        // The classifier already holds all ordered job custody. Sampling UTC
        // and checking the monotonic lease here accounts for those actual waits.
        rows::require_live(&tx, current, None).await?;
        initial_history::check_delivery(&mut lease, policy, input.cancelled)?;
        proof.validate(input.cancelled).map_err(native_error)?;
        let retired = match context.terminal.retired_at {
            Some(at) => at,
            None => {
                let now = rows::clock(&tx).await?;
                if now < context.terminal.source_closed_at {
                    return Err(OwnerError::InvalidMetadata.into());
                }
                retirement_rows::retire(&tx, context, now).await?;
                now
            }
        };
        let observed = retirement_rows::require_context(&tx, &context.intent, current).await?;
        if observed != Some(retired)
            || projection::activation(&tx, &context.intent).await? != context.activation
        {
            return Err(OwnerError::InvalidMetadata.into());
        }
        let after = history_rows::read_epoch(&tx, proof.recovery, context.head.epoch()).await?;
        retirement_rows::require_terminal_head(&context.head, &after)?;
        let terminal = retirement_rows::terminal(&tx, &context.intent, current, observed).await?;
        retirement_rows::require_terminal_effect(&tx, &context.terminal, &terminal, current)
            .await?;
        proof.validate(input.cancelled).map_err(native_error)?;
        rows::require_live(&tx, current, None).await?;
        initial_history::check_delivery(&mut lease, policy, input.cancelled)?;
        tx.commit()
            .await
            .map_err(|_| ProducerResolveError::CommitUnknown)?;
        proof.validate(input.cancelled).map_err(native_error)?;
        initial_history::check_delivery(&mut lease, policy, input.cancelled)?;
        if self.client.is_closed() {
            return Err(ProducerResolveError::DeliveryUnknown);
        }
        self.available = true;
        Ok(gaps)
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "fixed retirement context, retained Check and transaction authority"
    )]
    async fn begin_checked_successor_retirement<'a>(
        &'a mut self,
        recovery: &IngestRecovery,
        context: &Context,
        native: &NativeSuccessorRetirement,
        compacted: Option<&successor_compaction::Check>,
        input: &InitialHistoryRequest<'_>,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<(Transaction<'a>, PolicyLeaseTracker, PolicyEpoch), ProducerResolveError> {
        if context.control != self.identity {
            return Err(OwnerError::InvalidInput.into());
        }
        let (tx, mut lease, policy) = self
            .begin_activated_successor_recovery(recovery, input, validate)
            .await?;
        let retired =
            retirement_rows::require_context(&tx, &context.intent, context.control).await?;
        if projection::activation(&tx, &context.intent).await? != context.activation
            || retirement_rows::terminal(&tx, &context.intent, context.control, retired).await?
                != context.terminal
        {
            return Err(OwnerError::InvalidMetadata.into());
        }
        retirement_rows::require_native(&tx, recovery, context, native, input.cancelled).await?;
        successor_compaction::require_basis_presence(
            compacted.is_some(),
            native.compacted_requests() != 0,
            compacted.is_some(),
        )?;
        if let Some(check) = compacted {
            // The classifier already holds Control/dataset, all lineage parents
            // and ordered job locks. H/D stay protected through this transaction.
            check
                .resolve_retirement(&tx, recovery, native, &mut lease, policy, input.cancelled)
                .await?;
        }
        Ok((tx, lease, policy))
    }
}

async fn require_recorded(
    tx: &Transaction<'_>,
    recovery: &IngestRecovery,
    native: &ResolvedSuccessorRetirement<'_>,
    lease: &mut PolicyLeaseTracker,
    policy: PolicyEpoch,
    cancelled: &AtomicBool,
) -> Result<(), ProducerResolveError> {
    if !native.matches_owner(native.owner())
        || native.verified_through()
            != native
                .owner()
                .head()
                .manifest()
                .identity()
                .owner_epoch
                .get()
    {
        return Err(OwnerError::InvalidMetadata.into());
    }
    for epoch in 1..=native.verified_through() {
        initial_history::check_delivery(lease, policy, cancelled)?;
        let entry = native
            .owner()
            .entry(epoch)
            .ok_or(OwnerError::InvalidMetadata)?;
        let count =
            usize::try_from(entry.issued_through()).map_err(|_| OwnerError::InvalidMetadata)?;
        let records = history_rows::recorded_prefix(
            tx,
            recovery,
            native.owner().producer_slot_id(),
            epoch,
            count,
        )
        .await?;
        if !native.matches_recorded(epoch, records[..count].iter().copied()) {
            return Err(OwnerError::InvalidMetadata.into());
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "successor_retirement_tests.rs"]
mod tests;
