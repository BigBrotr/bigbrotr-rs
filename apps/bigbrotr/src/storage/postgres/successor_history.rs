//! Read-only positive history for the repeatable family. A checked predecessor
//! is not a writer, a candidate, a closing barrier, or a portable authority.
use super::super::producer_requests;
use super::*;
use crate::{
    runtime::{OwnedPermit, policy_authority::PolicyLeaseTracker},
    storage::rocks::{
        store::{
            AdmissionError, AdmissionLimits, BootstrapError, IngestRecovery, LineageEpochSummary,
            LineageResolution, ProducerRecoveryError, ProducerRecoveryLimits, ResolvedLineage,
            RetainedLineage, RetainedTerminalKind,
        },
        succession::{LINEAGE_SCRATCH_BYTES, PredecessorTerminalKind},
    },
};
use bigbrotr_model::{PolicyEpoch, ProducerRequestId, ProducerSlotId};
use std::{
    mem::size_of,
    path::Path,
    sync::atomic::{AtomicBool, Ordering},
};

#[path = "successor_history_rows.rs"]
pub(super) mod rows;
use rows::{Epoch, ProducerState, SourceState};

/// Classification of the observed pair of SQL/native heads, never authority to
/// complete a gap, retire an attempt or adopt its incarnation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SuccessorHistoryState {
    Terminal,
    PendingNative,
    PreparedOpen,
    PreparedClosing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Selection {
    state: SuccessorHistoryState,
    through: u64,
    head_kind: Option<PredecessorTerminalKind>,
}

/// Owns the single compact lineage allocation and its metadata grant. The fixed
/// SQL head and the transient projections below use that grant's 128KiB scratch;
/// no second history vector or metadata grant is retained.
pub struct SuccessorHistory {
    lineage: RetainedLineage,
    head: Epoch,
    selection: Selection,
    control: ingest_registry::ControlOwner,
}

/// Only this ordered owner can feed PG-checked originals into the native
/// accumulator. It exposes neither an arbitrary record() nor a mutable cursor.
pub struct SuccessorHistoryResolution<'a> {
    history: &'a SuccessorHistory,
    recovery: &'a IngestRecovery,
    cancelled: &'a AtomicBool,
    native: LineageResolution<'a>,
}

/// Positive native history and exact existing SQL copies under one checked head.
/// Its current Control observation is not a renewable lease or writer grant.
pub struct VerifiedSuccessorHistory<'a> {
    history: &'a SuccessorHistory,
    native: ResolvedLineage<'a>,
}

/// The next identity and original digest come from the sealed ordered context,
/// not this caller payload. Input/outcome grants remain distinct from metadata.
pub struct SuccessorReplayRequest<'a> {
    pub ingest_root: &'a Path,
    pub limits: ProducerRecoveryLimits,
    pub policy_ttl: Duration,
    pub cancelled: &'a AtomicBool,
}

// Fixed PG context, cursor and both callee/caller 64-tuple buffers fit scratch.
// Native sidecar inspection and SQL projection decoding run serially. Transport
// buffers/policy documents retain the existing separate Control/driver grant;
// semantic input/effect decoding has its own admission grant.
const _: () = assert!(size_of::<SuccessorHistory>() < LINEAGE_SCRATCH_BYTES as usize / 8);
const _: () =
    assert!(size_of::<SuccessorHistoryResolution<'_>>() < LINEAGE_SCRATCH_BYTES as usize / 8);
const _: () = assert!(
    size_of::<SuccessorHistory>()
        + size_of::<SuccessorHistoryResolution<'_>>()
        + 2 * size_of::<[(u64, [u8; 32], [u8; 32]); 64]>()
        + 8 * 1024 // native sidecar hashing scratch, used serially with SQL rows
        < LINEAGE_SCRATCH_BYTES as usize
);

impl SuccessorHistory {
    /// Private custody transfer: no second allocation and no new authority.
    pub(super) fn into_lineage(self) -> RetainedLineage {
        self.lineage
    }
    pub(super) fn control(&self) -> ingest_registry::ControlOwner {
        self.control
    }
    pub fn state(&self) -> SuccessorHistoryState {
        self.selection.state
    }
    pub fn registered_epoch(&self) -> u64 {
        self.head.epoch()
    }
    pub fn native_epoch(&self) -> u64 {
        self.lineage
            .native_head()
            .manifest()
            .identity()
            .owner_epoch
            .get()
    }
    pub fn replay_through(&self) -> u64 {
        self.selection.through
    }
    pub fn producer_slot_id(&self) -> ProducerSlotId {
        self.lineage.producer_slot_id()
    }
    /// Structural observation only. Even a fully checked census is not positive
    /// request/effect evidence until resolution and the final census complete.
    pub fn lineage(&self) -> &RetainedLineage {
        &self.lineage
    }
    pub fn resolution<'a>(
        &'a self,
        recovery: &'a IngestRecovery,
        cancelled: &'a AtomicBool,
    ) -> Result<SuccessorHistoryResolution<'a>, BootstrapError> {
        let native = self.lineage.resolution(
            recovery,
            self.selection.through,
            self.selection.head_kind,
            cancelled,
        )?;
        Ok(SuccessorHistoryResolution {
            history: self,
            recovery,
            cancelled,
            native,
        })
    }
}
impl SuccessorHistoryResolution<'_> {
    pub fn current_epoch(&self) -> Option<u64> {
        self.native.current_epoch()
    }
    pub fn next_request(&self) -> Option<ProducerRequestId> {
        self.native.next_request()
    }
}
impl VerifiedSuccessorHistory<'_> {
    pub fn verified_through(&self) -> u64 {
        self.native.verified_through()
    }
    pub fn last_epoch(&self) -> LineageEpochSummary {
        self.native.last_epoch()
    }
    pub fn history(&self) -> &SuccessorHistory {
        self.history
    }
    /// Positive native evidence borrowed from the same history, for a later
    /// checked transition plan. It grants no current SQL authority by itself.
    pub fn resolved_lineage(&self) -> &ResolvedLineage<'_> {
        &self.native
    }
    pub fn matches_history(&self, history: &SuccessorHistory) -> bool {
        std::ptr::eq(self.history, history) && self.native.matches_lineage(&history.lineage)
    }
    pub fn validate(&self, cancelled: &AtomicBool) -> Result<(), BootstrapError> {
        self.native.validate(cancelled)
    }
}

impl ControlSession {
    /// Inspect terminal genesis2, a terminal new-family head, or an exact empty
    /// prepared head/native gap. Activated nonterminal heads are intentionally
    /// outside this predecessor-only reader. All transactions finish by rollback.
    pub async fn inspect_successor_history(
        &mut self,
        recovery: &IngestRecovery,
        input: InitialHistoryRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<SuccessorHistory, ProducerResolveError> {
        let current = self.identity;
        let (tx, mut lease, policy) = self
            .begin_successor_history(recovery, &input, validate)
            .await?;
        let registered = rows::registered_epoch(&tx, recovery).await?;
        let head = rows::read_epoch(&tx, recovery, registered).await?;
        // Shape classification cannot manufacture a native entry for a pending
        // SQL preparation. Its extra epoch counts only for metadata admission.
        let native_epoch = recovery.manifest().identity().owner_epoch.get();
        let pending = pending_target(native_epoch, registered)?;
        rows::require_head(&tx, recovery, &head).await?;
        let lineage = RetainedLineage::inspect(recovery, pending, permit, input.cancelled)
            .map_err(native_error)?;
        let entry = lineage.native_head();
        let native_row = if pending.is_some() {
            rows::read_epoch(&tx, recovery, native_epoch).await?
        } else {
            head
        };
        let selection = select_history(
            native_epoch,
            entry.issued_through(),
            entry.source_through(),
            entry.is_closing(),
            &head,
            native_row.activated,
        )?;
        let history = SuccessorHistory {
            lineage,
            head,
            selection,
            control: current,
        };
        rows::require_native(&tx, recovery, &history, input.cancelled).await?;
        for entry in history.lineage.entries() {
            producer_progress::recovery::require(
                &tx,
                recovery,
                entry.manifest(),
                history.producer_slot_id(),
                history.lineage.progress_scratch(),
                || producer_progress::recovery::authority(&mut lease, policy, input.cancelled),
            )
            .await?;
        }
        history
            .lineage
            .validate(recovery, input.cancelled)
            .map_err(native_error)?;
        finish_read(tx, &mut lease, policy, input.cancelled, current).await?;
        if self.client.is_closed() {
            return Err(ProducerResolveError::DeliveryUnknown);
        }
        self.available = true;
        Ok(history)
    }

    /// Resolve and compare exactly the next original. Existing recorder/issuer
    /// attribution is never rewritten; missing copies refuse, including zero
    /// source-cut Event outcomes. Only a known completed read advances the cursor.
    pub async fn inspect_next_successor_request(
        &mut self,
        resolution: &mut SuccessorHistoryResolution<'_>,
        input: SuccessorReplayRequest<'_>,
        input_permit: OwnedPermit,
        outcome_permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<ProducerRequestId, ProducerResolveError> {
        let request = resolution.next_request().ok_or(OwnerError::InvalidInput)?;
        let operation = InitialHistoryRequest {
            ingest_root: input.ingest_root,
            policy_ttl: input.policy_ttl,
            cancelled: input.cancelled,
        };
        let current = self.identity;
        let (tx, mut lease, policy) = self
            .begin_checked_successor_history(
                resolution.history,
                resolution.recovery,
                &operation,
                validate,
            )
            .await?;
        let original = producer_requests::read(&tx, self_dataset(resolution.history), request)
            .await
            .map_err(ProducerResolveError::Reservation)?
            .ok_or(ProducerResolveError::Native(
                ProducerRecoveryError::Unresolved,
            ))?;
        let entry = resolution
            .history
            .lineage
            .entry(request.producer_epoch.get())
            .ok_or(OwnerError::InvalidMetadata)?;
        let manifest: [u8; 32] = Sha256::digest(entry.manifest().encode().as_bytes()).into();
        if original.manifest_sha256 != manifest {
            return Err(OwnerError::InvalidMetadata.into());
        }
        let resolved = resolution
            .native
            .resolve_next(
                original.digest,
                input.limits,
                input_permit,
                outcome_permit,
                || {
                    if input.cancelled.load(Ordering::Relaxed) {
                        return Err(AdmissionError::AuthorityUnavailable);
                    }
                    lease
                        .check_delivery(policy)
                        .map_err(|_| AdmissionError::AuthorityUnavailable)
                },
            )
            .map_err(ProducerResolveError::Native)?;
        if resolved.input_bytes() != original.input_bytes {
            return Err(ProducerResolveError::Outcome(
                super::super::producer_outcomes::OutcomeError::Conflict,
            ));
        }
        producer_retirement::require_retirement_copy(&tx, &resolved, request, manifest).await?;
        finish_read(tx, &mut lease, policy, input.cancelled, current).await?;
        if self.client.is_closed() {
            return Err(ProducerResolveError::DeliveryUnknown);
        }
        resolution.native.record(&resolved).map_err(native_error)?;
        // Recording revalidates the retained native/input stamp after rollback.
        // Its read work must not deliver an advanced cursor after local expiry.
        initial_history::check_delivery(&mut lease, policy, input.cancelled)?;
        self.available = true;
        Ok(request)
    }

    /// Check one terminal epoch, including empty epochs, before the ordered
    /// accumulator moves to the next. The summary remains observation data.
    pub async fn finish_successor_history_epoch(
        &mut self,
        resolution: &mut SuccessorHistoryResolution<'_>,
        input: InitialHistoryRequest<'_>,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<LineageEpochSummary, ProducerResolveError> {
        let epoch = resolution.current_epoch().ok_or(OwnerError::InvalidInput)?;
        if resolution.next_request().is_some() {
            return Err(OwnerError::InvalidInput.into());
        }
        let current = self.identity;
        let (tx, mut lease, policy) = self
            .begin_checked_successor_history(
                resolution.history,
                resolution.recovery,
                &input,
                validate,
            )
            .await?;
        let row = rows::read_epoch(&tx, resolution.recovery, epoch).await?;
        let summary = resolution.native.finish_epoch().map_err(native_error)?;
        row.require_summary(&summary)?;
        finish_read(tx, &mut lease, policy, input.cancelled, current).await?;
        if self.client.is_closed() {
            return Err(ProducerResolveError::DeliveryUnknown);
        }
        self.available = true;
        Ok(summary)
    }

    /// The independent semantic native census keeps its ordinary replay/effect
    /// grant. SQL digest projections reuse only metadata scratch, one epoch at
    /// a time. This operation performs no native or SQL mutation.
    pub async fn finish_successor_history<'a>(
        &mut self,
        resolution: SuccessorHistoryResolution<'a>,
        input: InitialHistoryRequest<'_>,
        limits: AdmissionLimits,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<VerifiedSuccessorHistory<'a>, ProducerResolveError> {
        if resolution.current_epoch().is_some() {
            return Err(OwnerError::InvalidInput.into());
        }
        let current = self.identity;
        let (tx, mut lease, policy) = self
            .begin_checked_successor_history(
                resolution.history,
                resolution.recovery,
                &input,
                validate,
            )
            .await?;
        let native = resolution
            .native
            .finish(limits, permit)
            .map_err(|error| ProducerResolveError::Native(error.into()))?;
        for epoch in 1..=native.verified_through() {
            initial_history::check_delivery(&mut lease, policy, input.cancelled)?;
            rows::require_recorded(&tx, resolution.recovery, &native, epoch).await?;
        }
        native
            .validate(resolution.cancelled)
            .map_err(native_error)?;
        finish_read(tx, &mut lease, policy, input.cancelled, current).await?;
        if self.client.is_closed() {
            return Err(ProducerResolveError::DeliveryUnknown);
        }
        self.available = true;
        Ok(VerifiedSuccessorHistory {
            history: resolution.history,
            native,
        })
    }

    pub(super) async fn begin_successor_history<'a>(
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
            || !matches!(
                (id.owner_epoch.get(), recovery.manifest().key_schema()),
                (2, 2) | (3..=255, 3)
            )
        {
            return Err(OwnerError::InvalidInput.into());
        }
        self.begin_history_authority(recovery, input, 38, validate)
            .await
    }

    pub(super) async fn begin_checked_successor_history<'a>(
        &'a mut self,
        history: &SuccessorHistory,
        recovery: &IngestRecovery,
        input: &InitialHistoryRequest<'_>,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<(Transaction<'a>, PolicyLeaseTracker, PolicyEpoch), ProducerResolveError> {
        if history.control != self.identity {
            return Err(OwnerError::InvalidInput.into());
        }
        let (tx, lease, policy) = self
            .begin_successor_history(recovery, input, validate)
            .await?;
        history
            .lineage
            .validate(recovery, input.cancelled)
            .map_err(native_error)?;
        let head =
            rows::read_epoch(&tx, recovery, rows::registered_epoch(&tx, recovery).await?).await?;
        if head != history.head {
            return Err(OwnerError::InvalidMetadata.into());
        }
        rows::require_head(&tx, recovery, &head).await?;
        rows::require_native(&tx, recovery, history, input.cancelled).await?;
        Ok((tx, lease, policy))
    }
}

fn native_error(error: BootstrapError) -> ProducerResolveError {
    ProducerResolveError::Native(AdmissionError::from(error).into())
}
fn self_dataset(history: &SuccessorHistory) -> bigbrotr_model::DatasetId {
    history.head.manifest.identity().dataset_id
}

pub(super) async fn finish_read(
    tx: Transaction<'_>,
    lease: &mut PolicyLeaseTracker,
    policy: PolicyEpoch,
    cancelled: &AtomicBool,
    current: ingest_registry::ControlOwner,
) -> Result<(), ProducerResolveError> {
    // The flat checker samples UTC after registry/workflow lock waits. Sample
    // again after replay/copy queries, before releasing the actual authority.
    let row = tx.query_one("SELECT control.require_control_owner(dataset_id,resource_id,epoch,process_incarnation),
        lease_expires_at_us>(extract(epoch FROM clock_timestamp())*1000000)::bigint
        FROM control.owner_epochs WHERE resource_id=$1::bytea AND epoch=$2::text::numeric
            AND process_incarnation=$3::bytea", &[
        &&current.resource_id.as_bytes()[..], &current.epoch.get().to_string(),
        &&current.process_incarnation.as_bytes()[..],
    ]).await.map_err(backend)?;
    if !row.try_get::<_, bool>(1).map_err(backend)? {
        return Err(OwnerError::Unavailable.into());
    }
    drop(row);
    initial_history::finish(tx, lease, policy, cancelled).await
}

fn pending_target(native: u64, registered: u64) -> Result<Option<u64>, OwnerError> {
    if !(2..=255).contains(&native) || !(2..=255).contains(&registered) {
        return Err(OwnerError::InvalidMetadata);
    }
    if registered == native {
        Ok(None)
    } else if native.checked_add(1) == Some(registered) {
        Ok(Some(registered))
    } else {
        Err(OwnerError::InvalidMetadata)
    }
}

fn select_history(
    native: u64,
    issued: u64,
    cut: u64,
    closing: bool,
    head: &Epoch,
    native_activated: bool,
) -> Result<Selection, OwnerError> {
    let pending = pending_target(native, head.epoch())?;
    let kind = if native_activated {
        PredecessorTerminalKind::Activated
    } else {
        PredecessorTerminalKind::UnactivatedEmpty
    };
    if pending.is_some() {
        if !head.prepared() || !closing {
            return Err(OwnerError::InvalidMetadata);
        }
        return Ok(Selection {
            state: SuccessorHistoryState::PendingNative,
            through: native,
            head_kind: Some(kind),
        });
    }
    if head.producer == ProducerState::Retired && head.source == SourceState::Closed && closing {
        return Ok(Selection {
            state: SuccessorHistoryState::Terminal,
            through: native,
            head_kind: Some(kind),
        });
    }
    if native >= 3 && head.prepared() && issued == 0 && cut == 0 {
        return Ok(Selection {
            state: if closing {
                SuccessorHistoryState::PreparedClosing
            } else {
                SuccessorHistoryState::PreparedOpen
            },
            through: native - 1,
            head_kind: None,
        });
    }
    Err(OwnerError::InvalidMetadata)
}

#[cfg(test)]
#[path = "successor_history_tests.rs"]
mod tests;
