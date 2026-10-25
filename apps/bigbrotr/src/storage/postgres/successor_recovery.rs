//! Activated-family positive recovery under a replacement Control. The native
//! handle stays with the caller; this owner never adopts a recorded incarnation.
use super::compacted as successor_compaction;
use super::*;
use crate::storage::{
    postgres::{producer_outcomes, producer_requests},
    rocks::store::{
        AdmissionLimits, LineageEpochSummary, NativeSuccessorRecovery, ProducerRecoveryError,
        ResolvedSuccessorRecovery, RetainedTerminalKind, SuccessorRecoveryResolution,
    },
};
use bigbrotr_model::{Digest256, ProducerRequestId, ProducerSlotId};
use std::sync::atomic::AtomicBool;
use successor_history::rows::{self as history_rows, Epoch, ProducerState, SourceState};

#[path = "successor_recovery_rows.rs"]
mod projection;
#[path = "successor_retirement.rs"]
mod retirement;
#[path = "successor_source.rs"]
mod source;
pub use retirement::{
    ActivatedSuccessorRetirement, ActivatedSuccessorRetirementResolution,
    ResolvedActivatedSuccessorRetirement,
};
pub use source::{
    ActivatedSuccessorSourceRecovery, ActivatedSuccessorSourceResolution,
    ResolvedActivatedSuccessorSource,
};

// Private purpose, selected only by distinct connected factories. No caller can
// convert a source owner into the public source-open recovery capability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Purpose {
    Positive,
    Source,
}
impl Purpose {
    fn classifier(self) -> &'static str {
        match self {
            Self::Positive => "positive",
            Self::Source => "source",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClosingState {
    Inspected,
    Spent,
    Closed,
}
impl ClosingState {
    fn start(&mut self) -> Result<(), OwnerError> {
        if *self != Self::Inspected {
            return Err(OwnerError::Unavailable);
        }
        *self = Self::Spent;
        Ok(())
    }
}

/// An optional retained head input, not an outcome or authority to execute it.
/// An unreserved preimage has no manifest/protection attribution. Even when the
/// immutable reservation agrees, this input remains unresolved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SuccessorRecoveryInput {
    pub request: ProducerRequestId,
    pub digest: Digest256,
    pub input_bytes: u32,
    pub reserved: bool,
}

/// One native lineage allocation plus fixed SQL observations inside its scratch
/// share. Retain this owner AND the exclusive native handle on every error. A
/// failed/unknown close cannot be retried or turned into an ordered resolution
/// through this owner; a fresh excluded Control must inspect durable reality.
pub struct ActivatedSuccessorRecovery {
    native: NativeSuccessorRecovery,
    intent: Intent,
    activation: projection::Activation,
    head: Epoch,
    control: ingest_registry::ControlOwner,
    state: ClosingState,
    purpose: Purpose,
    compacted: Option<successor_compaction::Check>,
}

/// Ordered whole-history replay. Only this connected owner can accept a native
/// pending step after its known SQL result; callers cannot skip or inject steps.
pub struct ActivatedSuccessorResolution<'a> {
    owner: &'a ActivatedSuccessorRecovery,
    recovery: &'a IngestRecovery,
    cancelled: &'a AtomicBool,
    native: SuccessorRecoveryResolution<'a>,
}

/// Complete issued/reserved recovery and exact SQL copies. This borrowed result
/// deliberately exposes no lineage, source closure, retirement or writer proof.
pub struct ResolvedActivatedSuccessor<'a> {
    recovery: &'a IngestRecovery,
    owner: &'a ActivatedSuccessorRecovery,
    native: ResolvedSuccessorRecovery<'a>,
}

// Native recovery bounds its headers/census to one half of the original 128KiB
// scratch; this is the other half. Subtract embedded native headers, whose own
// assertion accounts for them and the pending Prefix. Include both return/caller
// tuple buffers and simultaneous fixed projections; no second history Vec exists.
// Input/outcome decoding and transport/policy documents keep separate grants.
const _: () = assert!(
    size_of::<ActivatedSuccessorRecovery>() - size_of::<NativeSuccessorRecovery>()
        + size_of::<ActivatedSuccessorResolution<'_>>()
        - size_of::<SuccessorRecoveryResolution<'_>>()
        + size_of::<ResolvedActivatedSuccessor<'_>>()
        - size_of::<ResolvedSuccessorRecovery<'_>>()
        + 2 * size_of::<Intent>()
        + 2 * size_of::<Epoch>()
        + 2 * size_of::<projection::Activation>()
        + 2 * size_of::<[(u64, [u8; 32], [u8; 32]); 64]>()
        + 1024 // distinct source wrappers and one post-publication projection
        + 16 * 1024
        < LINEAGE_SCRATCH_BYTES as usize / 2
);

impl ActivatedSuccessorRecovery {
    pub fn epoch(&self) -> u64 {
        self.intent.epoch()
    }
    pub fn producer_slot_id(&self) -> ProducerSlotId {
        self.native.producer_slot_id()
    }
    pub fn issued_through(&self) -> u64 {
        self.native.issued_through()
    }
    pub fn reserved_through(&self) -> u64 {
        self.head.reserved
    }
    pub fn is_closed(&self) -> bool {
        self.state == ClosingState::Closed
    }
    pub fn staged_input(&self) -> Option<SuccessorRecoveryInput> {
        self.native
            .staged_input()
            .map(|tail| SuccessorRecoveryInput {
                request: tail.request,
                digest: tail.digest,
                input_bytes: tail.input_bytes,
                reserved: tail.request.request_sequence.get() <= self.head.reserved,
            })
    }
    pub fn compacted_requests(&self) -> u64 {
        self.native.compacted_requests()
    }
    pub fn resolution<'a>(
        &'a self,
        recovery: &'a IngestRecovery,
        cancelled: &'a AtomicBool,
    ) -> Result<ActivatedSuccessorResolution<'a>, BootstrapError> {
        if !self.is_closed() {
            return Err(BootstrapError::AuthorityUnavailable);
        }
        let native = self.native.resolution(recovery, cancelled)?;
        Ok(ActivatedSuccessorResolution {
            owner: self,
            recovery,
            cancelled,
            native,
        })
    }
}
impl ActivatedSuccessorResolution<'_> {
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
impl ResolvedActivatedSuccessor<'_> {
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
    pub fn matches_recovery(&self, owner: &ActivatedSuccessorRecovery) -> bool {
        std::ptr::eq(self.owner, owner) && self.native.matches_owner(&owner.native)
    }
    pub fn validate(&self, cancelled: &AtomicBool) -> Result<(), BootstrapError> {
        self.native.validate(cancelled)
    }
}

impl ControlSession {
    /// The separate input grant decodes only a possible staged head input. Its
    /// presence is retained explicitly, and cannot be filtered into completeness.
    pub async fn inspect_activated_successor_producer(
        &mut self,
        recovery: &IngestRecovery,
        input: SuccessorReplayRequest<'_>,
        metadata_permit: OwnedPermit,
        input_permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<ActivatedSuccessorRecovery, ProducerResolveError> {
        self.inspect_successor_recovery(
            recovery,
            input,
            metadata_permit,
            input_permit,
            Purpose::Positive,
            None,
            validate,
        )
        .await
    }

    /// Require the retained compacted basis for positive recovery only. Ordinary
    /// state is refused; original factories and retirement stay strict.
    #[allow(
        clippy::too_many_arguments,
        reason = "distinct native metadata, original input and retained canonical effect grants"
    )]
    pub async fn inspect_activated_successor_producer_with_compacted(
        &mut self,
        recovery: &IngestRecovery,
        input: SuccessorReplayRequest<'_>,
        metadata_permit: OwnedPermit,
        input_permit: OwnedPermit,
        effects: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<ActivatedSuccessorRecovery, ProducerResolveError> {
        let check = successor_compaction::Check::new(input.limits.event, effects)?;
        self.inspect_successor_recovery(
            recovery,
            input,
            metadata_permit,
            input_permit,
            Purpose::Positive,
            Some(check),
            validate,
        )
        .await
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "private purpose and optional retained compacted proof-use grant"
    )]
    async fn inspect_successor_recovery(
        &mut self,
        recovery: &IngestRecovery,
        input: SuccessorReplayRequest<'_>,
        metadata_permit: OwnedPermit,
        input_permit: OwnedPermit,
        purpose: Purpose,
        compacted: Option<successor_compaction::Check>,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<ActivatedSuccessorRecovery, ProducerResolveError> {
        let current = self.identity;
        let operation = history_request(&input);
        let (tx, mut lease, policy) = self
            .begin_activated_successor_recovery(recovery, &operation, validate)
            .await?;
        let epoch = recovery.manifest().identity().owner_epoch.get();
        let intent = rows::read(&tx, recovery, epoch).await?;
        // Locks current Control, dataset and ordered parents before inspecting
        // native. Historical scheduling expiry is deliberately irrelevant.
        projection::require_context(&tx, &intent, current, purpose).await?;
        let activation = projection::activation(&tx, &intent).await?;
        let head = history_rows::read_epoch(&tx, recovery, epoch).await?;
        let native = if compacted.is_some() {
            NativeSuccessorRecovery::inspect_with_compacted_initial(
                recovery,
                metadata_permit,
                input.cancelled,
            )
        } else {
            NativeSuccessorRecovery::inspect(recovery, metadata_permit, input.cancelled)
        }
        .map_err(native_error)?;
        require_recovery_basis(compacted.is_some(), native.compacted_requests() != 0)?;
        let owner = ActivatedSuccessorRecovery {
            native,
            intent,
            activation,
            head,
            control: current,
            state: ClosingState::Inspected,
            purpose,
            compacted,
        };
        projection::require_native(&tx, recovery, &owner, input.cancelled).await?;
        if let Some(check) = &owner.compacted {
            check
                .resolve_recovery(
                    &tx,
                    recovery,
                    &owner.native,
                    &mut lease,
                    policy,
                    input.cancelled,
                )
                .await?;
        }
        // The original-input grant has not begun decoding. Its existing 64KiB
        // staging allowance covers this sequential 4KiB closure reader.
        for epoch in 1..=owner.epoch() {
            if epoch == 1 && owner.compacted.is_some() {
                continue;
            }
            let entry = owner
                .native
                .entry(epoch)
                .ok_or(OwnerError::InvalidMetadata)?;
            producer_progress::recovery::require(
                &tx,
                recovery,
                entry.manifest(),
                owner.producer_slot_id(),
                &input_permit,
                || producer_progress::recovery::authority(&mut lease, policy, input.cancelled),
            )
            .await?;
        }
        owner
            .native
            .validate_tail(recovery, input.limits.input, input_permit, input.cancelled)
            .map_err(ProducerResolveError::Native)?;
        projection::require_tail(&tx, &owner).await?;
        successor_history::finish_read(tx, &mut lease, policy, input.cancelled, current).await?;
        owner
            .native
            .validate(recovery, input.cancelled)
            .map_err(native_error)?;
        if let Some(check) = &owner.compacted {
            check.validate_recovery(recovery, &owner.native, input.cancelled)?;
        }
        initial_history::check_delivery(&mut lease, policy, input.cancelled)?;
        if self.client.is_closed() {
            return Err(ProducerResolveError::DeliveryUnknown);
        }
        self.available = true;
        Ok(owner)
    }

    /// Native CLOSING precedes SQL OPEN->CLOSING. A native-closed/SQL-open gap is
    /// recoverable; SQL-closed/native-open is refused by inspection. On every
    /// error the caller keeps both owners, including any durable native barrier.
    pub async fn close_activated_successor_producer(
        &mut self,
        recovery: &mut IngestRecovery,
        owner: &mut ActivatedSuccessorRecovery,
        input: InitialHistoryRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<(), ProducerResolveError> {
        owner.state.start()?;
        let current = self.identity;
        let (tx, mut lease, policy) = self
            .begin_checked_successor_recovery(recovery, owner, &input, validate)
            .await?;
        if owner.head.source == SourceState::Closed {
            owner
                .native
                .observe_closed(recovery, permit, input.cancelled, || {
                    native_authority(&mut lease, policy, input.cancelled)
                })
                .map_err(native_error)?;
            successor_history::finish_read(tx, &mut lease, policy, input.cancelled, current)
                .await?;
        } else {
            owner
                .native
                .close_producer(recovery, permit, input.cancelled, || {
                    native_authority(&mut lease, policy, input.cancelled)
                })
                .map_err(native_error)?;
            let changed = tx
                .execute(
                    "UPDATE control.producer_epochs SET state='closing'
            WHERE dataset_id=$1::bytea AND producer_slot_id=$2::bytea
                AND producer_epoch=$3::text::numeric AND state='open'",
                    &[
                        &&recovery.manifest().identity().dataset_id.as_bytes()[..],
                        &&owner.producer_slot_id().as_bytes()[..],
                        &owner.epoch().to_string(),
                    ],
                )
                .await
                .map_err(backend)?;
            if changed != u64::from(owner.head.producer == ProducerState::Open) {
                return Err(OwnerError::InvalidMetadata.into());
            }
            projection::require_context(&tx, &owner.intent, current, owner.purpose).await?;
            if let Some(check) = &owner.compacted {
                check
                    .resolve_recovery(
                        &tx,
                        recovery,
                        &owner.native,
                        &mut lease,
                        policy,
                        input.cancelled,
                    )
                    .await?;
            }
            owner
                .native
                .validate(recovery, input.cancelled)
                .map_err(native_error)?;
            rows::require_live(&tx, current, None).await?;
            initial_history::check_delivery(&mut lease, policy, input.cancelled)?;
            tx.commit()
                .await
                .map_err(|_| ProducerResolveError::CommitUnknown)?;
        }
        // Never reset the monotonic deadline after COMMIT or native validation.
        if let Some(check) = &owner.compacted {
            check.validate_recovery(recovery, &owner.native, input.cancelled)?;
        }
        owner
            .native
            .validate(recovery, input.cancelled)
            .map_err(native_error)?;
        initial_history::check_delivery(&mut lease, policy, input.cancelled)?;
        if self.client.is_closed() {
            return Err(ProducerResolveError::DeliveryUnknown);
        }
        owner.head.producer = ProducerState::Closing;
        owner.state = ClosingState::Closed;
        self.available = true;
        Ok(())
    }

    /// Confirm the compacted initial step under the same recovery owner. The
    /// cursor advances only after a known read completion and final authority.
    pub async fn inspect_activated_successor_compacted_initial(
        &mut self,
        resolution: &mut ActivatedSuccessorResolution<'_>,
        input: InitialHistoryRequest<'_>,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<crate::storage::rocks::store::CompactedInitialSummary, ProducerResolveError> {
        let result = async {
            if !resolution.native.needs_compacted_initial() {
                return Err(OwnerError::InvalidInput.into());
            }
            let current = self.identity;
            let (tx, mut lease, policy) = self
                .begin_checked_successor_recovery(
                    resolution.recovery,
                    resolution.owner,
                    &input,
                    validate,
                )
                .await?;
            let check = resolution
                .owner
                .compacted
                .as_ref()
                .ok_or(OwnerError::InvalidInput)?;
            let proof = check
                .resolve_recovery(
                    &tx,
                    resolution.recovery,
                    &resolution.owner.native,
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

    /// Ancestors require their existing terminal copies. Only the current head
    /// can repair a missing copy, without imposing a later Event workflow veto.
    /// Prefix/dependency acceptance is previewed BEFORE any outcome INSERT.
    pub async fn resolve_next_activated_successor_request(
        &mut self,
        resolution: &mut ActivatedSuccessorResolution<'_>,
        input: SuccessorReplayRequest<'_>,
        input_permit: OwnedPermit,
        outcome_permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<ProducerRequestId, ProducerResolveError> {
        let result = async {
            let request = resolution.next_request().ok_or(OwnerError::InvalidInput)?;
            let owner = resolution.owner;
            let current = self.identity;
            let operation = history_request(&input);
            let (tx, mut lease, policy) = self
                .begin_checked_successor_recovery(resolution.recovery, owner, &operation, validate)
                .await?;
            let original =
                producer_requests::read(&tx, owner.head.manifest.identity().dataset_id, request)
                    .await
                    .map_err(ProducerResolveError::Reservation)?
                    .ok_or(ProducerResolveError::Native(
                        ProducerRecoveryError::Unresolved,
                    ))?;
            let entry = owner
                .native
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
            if request.producer_epoch.get() < owner.epoch() {
                producer_retirement::require_retirement_copy(
                    &tx,
                    pending.resolved(),
                    request,
                    manifest,
                )
                .await?;
                successor_history::finish_read(tx, &mut lease, policy, input.cancelled, current)
                    .await?;
            } else if owner.head.source == SourceState::Closed {
                // Source closure is not workflow retirement. Exact existing
                // Event copies remain admissible here, including unbound ones.
                producer_outcomes::require_recovered(&tx, &original, pending.resolved())
                    .await
                    .map_err(ProducerResolveError::Outcome)?;
                successor_history::finish_read(tx, &mut lease, policy, input.cancelled, current)
                    .await?;
            } else {
                producer_outcomes::record_recovered(&tx, &original, pending.resolved(), current)
                    .await
                    .map_err(ProducerResolveError::Outcome)?;
                owner
                    .native
                    .validate(resolution.recovery, input.cancelled)
                    .map_err(native_error)?;
                rows::require_live(&tx, current, None).await?;
                initial_history::check_delivery(&mut lease, policy, input.cancelled)?;
                tx.commit()
                    .await
                    .map_err(|_| ProducerResolveError::CommitUnknown)?;
            }
            // Dropping pending on error/unknown poisons the native cursor. The
            // final callback follows native validation and precedes its sole
            // infallible prefix move, so no rejected SQL result advances replay.
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

    pub async fn finish_activated_successor_epoch(
        &mut self,
        resolution: &mut ActivatedSuccessorResolution<'_>,
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
                .begin_checked_successor_recovery(
                    resolution.recovery,
                    resolution.owner,
                    &input,
                    validate,
                )
                .await?;
            let row = history_rows::read_epoch(&tx, resolution.recovery, epoch).await?;
            let summary = resolution.native.finish_epoch().map_err(native_error)?;
            if epoch < resolution.owner.epoch() {
                row.require_summary(&summary)?;
            } else {
                require_head_summary(&summary, resolution.owner)?;
            }
            successor_history::finish_read(tx, &mut lease, policy, input.cancelled, current)
                .await?;
            resolution
                .owner
                .native
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

    /// A tail or unresolved reservation suffix prevents this complete result,
    /// even though earlier positively checked copies may already be committed.
    pub async fn finish_activated_successor_recovery<'a>(
        &mut self,
        resolution: ActivatedSuccessorResolution<'a>,
        input: InitialHistoryRequest<'_>,
        limits: AdmissionLimits,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<ResolvedActivatedSuccessor<'a>, ProducerResolveError> {
        let current = self.identity;
        let (tx, mut lease, policy) = self
            .begin_checked_successor_recovery(
                resolution.recovery,
                resolution.owner,
                &input,
                validate,
            )
            .await?;
        require_complete(
            resolution.owner.issued_through(),
            resolution.owner.reserved_through(),
            resolution.owner.staged_input().is_some(),
        )?;
        let native = resolution
            .native
            .finish(limits, permit)
            .map_err(ProducerResolveError::Native)?;
        require_recorded_recovery(
            &tx,
            resolution.recovery,
            resolution.owner,
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
        Ok(ResolvedActivatedSuccessor {
            recovery: resolution.recovery,
            owner: resolution.owner,
            native,
        })
    }

    async fn begin_activated_successor_recovery<'a>(
        &'a mut self,
        recovery: &IngestRecovery,
        input: &InitialHistoryRequest<'_>,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<(Transaction<'a>, PolicyLeaseTracker, PolicyEpoch), ProducerResolveError> {
        if input.cancelled.load(Ordering::Relaxed) {
            self.available = false;
            return Err(OwnerError::Unavailable.into());
        }
        let id = recovery.manifest().identity();
        if !recovery.matches_configured_root(input.ingest_root)
            || id.dataset_id != self.request.dataset
            || recovery.manifest().key_schema() != 3
            || !(3..=255).contains(&id.owner_epoch.get())
        {
            return Err(OwnerError::InvalidInput.into());
        }
        self.begin_history_authority(recovery, input, 38, validate)
            .await
    }
    async fn begin_checked_successor_recovery<'a>(
        &'a mut self,
        recovery: &IngestRecovery,
        owner: &ActivatedSuccessorRecovery,
        input: &InitialHistoryRequest<'_>,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<(Transaction<'a>, PolicyLeaseTracker, PolicyEpoch), ProducerResolveError> {
        if owner.control != self.identity {
            return Err(OwnerError::InvalidInput.into());
        }
        let (tx, mut lease, policy) = self
            .begin_activated_successor_recovery(recovery, input, validate)
            .await?;
        projection::require_context(&tx, &owner.intent, owner.control, owner.purpose).await?;
        if projection::activation(&tx, &owner.intent).await? != owner.activation
            || history_rows::read_epoch(&tx, recovery, owner.epoch()).await? != owner.head
        {
            return Err(OwnerError::InvalidMetadata.into());
        }
        projection::require_native(&tx, recovery, owner, input.cancelled).await?;
        projection::require_tail(&tx, owner).await?;
        require_recovery_basis(owner.compacted.is_some(), owner.compacted_requests() != 0)?;
        if let Some(check) = &owner.compacted {
            check
                .resolve_recovery(
                    &tx,
                    recovery,
                    &owner.native,
                    &mut lease,
                    policy,
                    input.cancelled,
                )
                .await?;
        }
        Ok((tx, lease, policy))
    }
}

fn history_request<'a>(input: &SuccessorReplayRequest<'a>) -> InitialHistoryRequest<'a> {
    InitialHistoryRequest {
        ingest_root: input.ingest_root,
        policy_ttl: input.policy_ttl,
        cancelled: input.cancelled,
    }
}
fn require_recovery_basis(check: bool, retained: bool) -> Result<(), ProducerResolveError> {
    successor_compaction::require_basis_presence(check, retained, check)
}
fn native_error(error: BootstrapError) -> ProducerResolveError {
    ProducerResolveError::Native(AdmissionError::from(error).into())
}
fn native_authority(
    lease: &mut PolicyLeaseTracker,
    policy: PolicyEpoch,
    cancelled: &AtomicBool,
) -> Result<(), BootstrapError> {
    if cancelled.load(Ordering::Relaxed) {
        return Err(BootstrapError::AuthorityUnavailable);
    }
    lease
        .check_delivery(policy)
        .map_err(|_| BootstrapError::AuthorityUnavailable)
}
fn require_complete(issued: u64, reserved: u64, tail: bool) -> Result<(), ProducerResolveError> {
    if issued != reserved || tail {
        return Err(ProducerResolveError::Native(
            ProducerRecoveryError::Unresolved,
        ));
    }
    Ok(())
}
fn require_head_summary(
    summary: &LineageEpochSummary,
    owner: &ActivatedSuccessorRecovery,
) -> Result<(), OwnerError> {
    projection::require_summary(
        summary,
        &owner.head,
        owner.issued_through(),
        owner.native.head().source_through(),
    )
}

async fn require_recorded_recovery(
    tx: &Transaction<'_>,
    recovery: &IngestRecovery,
    owner: &ActivatedSuccessorRecovery,
    native: &ResolvedSuccessorRecovery<'_>,
    lease: &mut PolicyLeaseTracker,
    policy: PolicyEpoch,
    cancelled: &AtomicBool,
) -> Result<(), ProducerResolveError> {
    for epoch in 1..=native.verified_through() {
        initial_history::check_delivery(lease, policy, cancelled)?;
        let entry = owner
            .native
            .entry(epoch)
            .ok_or(OwnerError::InvalidMetadata)?;
        let count =
            usize::try_from(entry.issued_through()).map_err(|_| OwnerError::InvalidMetadata)?;
        let records =
            history_rows::recorded_prefix(tx, recovery, owner.producer_slot_id(), epoch, count)
                .await?;
        if !native.matches_recorded(epoch, records[..count].iter().copied()) {
            return Err(OwnerError::InvalidMetadata.into());
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "successor_recovery_tests.rs"]
mod tests;
