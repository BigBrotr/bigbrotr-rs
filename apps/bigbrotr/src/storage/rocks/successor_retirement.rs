//! Retirement-purpose replay only. Existing native CLOSING is a prerequisite;
//! SQL source closure, retained copies and workflows remain connected checks.
//! No conversion exposes recovery, source publication or predecessor authority.
use super::super::{
    AdmissionError, AdmissionLimits, ProducerRecoveryError, ProducerRecoveryLimits,
    ResolvedProducerRequest, lineage_replay::LineageEpochSummary,
};
use super::recovery::{
    PendingSuccessorRequest, ResolvedSuccessorRecovery, SuccessorRecoveryResolution,
};
use super::*;
use bigbrotr_model::Digest256;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Inspected,
    Observed,
    Spent,
}

/// Owns the same single retained lineage allocation and metadata/closing grants
/// as recovery. The inner capability never escapes this retirement purpose.
pub(in crate::storage) struct NativeSuccessorRetirement {
    inner: NativeSuccessorRecovery,
    phase: Phase,
}

pub(in crate::storage) struct SuccessorRetirementResolution<'a> {
    owner: &'a NativeSuccessorRetirement,
    inner: SuccessorRecoveryResolution<'a>,
}

/// Preview positively validates the original and complete proposed prefix before
/// the connected caller checks its existing SQL copy. Drop poisons the cursor.
pub(in crate::storage) struct PendingSuccessorRetirementRequest<'step, 'owner> {
    inner: PendingSuccessorRequest<'step, 'owner>,
}

/// A borrowed, full-history retirement proof. Its last summary preserves the
/// native-derived Event mask, including Rejected and AlreadyDurable requests.
/// It does not assert that PostgreSQL source/workflow prerequisites have passed.
pub(in crate::storage) struct ResolvedSuccessorRetirement<'a> {
    owner: &'a NativeSuccessorRetirement,
    inner: ResolvedSuccessorRecovery<'a>,
}

// Count the actual wrapper headers and all common native scratch, rather than
// reserving a second lineage or omitting the inner cursor/proof. The connected
// projection retains the other half of the existing fixed 128KiB scratch.
const _: () = assert!(
    size_of::<NativeSuccessorRetirement>()
        + size_of::<SuccessorRetirementResolution<'_>>()
        + size_of::<PendingSuccessorRetirementRequest<'_, '_>>()
        + size_of::<ResolvedSuccessorRetirement<'_>>()
        + size_of::<InitialHandoffRecord>()
        + 2 * size_of::<SuccessionRecord>()
        + super::census::COUNTER_BYTES
        + 16 * 1024
        < LINEAGE_SCRATCH_BYTES as usize / 2
);

impl NativeSuccessorRetirement {
    pub(in crate::storage) fn inspect(
        recovery: &IngestRecovery,
        permit: OwnedPermit,
        cancelled: &AtomicBool,
    ) -> Result<Self, BootstrapError> {
        let inner = NativeSuccessorRecovery::inspect(recovery, permit, cancelled)?;
        // Retirement may not complete an absent barrier or unresolved staging.
        // Ordinary recovery retains its broader classifier unchanged.
        if !inner.is_closing() || inner.staged_input().is_some() {
            return Err(BootstrapError::StateMismatch);
        }
        Ok(Self {
            inner,
            phase: Phase::Inspected,
        })
    }
    pub(in crate::storage) fn entry(&self, epoch: u64) -> Option<&RetainedLineageEntry> {
        self.inner.entry(epoch)
    }
    pub(in crate::storage) fn head(&self) -> &RetainedLineageEntry {
        self.inner.head()
    }
    pub(in crate::storage) fn producer_slot_id(&self) -> ProducerSlotId {
        self.inner.producer_slot_id()
    }
    pub(in crate::storage) fn issued_through(&self) -> u64 {
        self.inner.issued_through()
    }
    pub(in crate::storage) fn validate(
        &self,
        recovery: &IngestRecovery,
        cancelled: &AtomicBool,
    ) -> Result<(), BootstrapError> {
        self.inner.validate(recovery, cancelled)
    }
    /// Spend before any fallible observation. Failure keeps the original owner
    /// and admitted grants reachable but cannot be retried through this owner.
    /// There is deliberately no close_producer or mutable native-store method.
    pub(in crate::storage) fn observe_closed(
        &mut self,
        recovery: &IngestRecovery,
        permit: OwnedPermit,
        cancelled: &AtomicBool,
        authority: impl FnMut() -> Result<(), BootstrapError>,
    ) -> Result<(), BootstrapError> {
        if self.phase != Phase::Inspected {
            return Err(BootstrapError::AuthorityUnavailable);
        }
        self.phase = Phase::Spent;
        self.inner
            .observe_closed(recovery, permit, cancelled, authority)?;
        self.phase = Phase::Observed;
        Ok(())
    }
    /// One ordered replay per owner, including errors and dropped cursors. A
    /// fresh process must re-inspect after an unknown final SQL outcome; this
    /// borrowed proof cannot rearm or become a positive/source capability.
    pub(in crate::storage) fn resolution<'a>(
        &'a mut self,
        recovery: &'a IngestRecovery,
        cancelled: &'a AtomicBool,
    ) -> Result<SuccessorRetirementResolution<'a>, BootstrapError> {
        if self.phase != Phase::Observed {
            return Err(BootstrapError::AuthorityUnavailable);
        }
        self.phase = Phase::Spent;
        let inner = self.inner.resolution(recovery, cancelled)?;
        Ok(SuccessorRetirementResolution { owner: self, inner })
    }
}

impl<'a> SuccessorRetirementResolution<'a> {
    pub(in crate::storage) fn owner(&self) -> &NativeSuccessorRetirement {
        self.owner
    }
    pub(in crate::storage) fn current_epoch(&self) -> Option<u64> {
        self.inner.current_epoch()
    }
    pub(in crate::storage) fn next_request(&self) -> Option<ProducerRequestId> {
        self.inner.next_request()
    }
    pub(in crate::storage) fn poison(&mut self) {
        self.inner.poison();
    }
    #[allow(
        clippy::too_many_arguments,
        reason = "distinct input/outcome grants and current authority"
    )]
    pub(in crate::storage) fn preview_next<'step>(
        &'step mut self,
        digest: Digest256,
        limits: ProducerRecoveryLimits,
        input_permit: OwnedPermit,
        outcome_permit: OwnedPermit,
        authority: impl FnMut() -> Result<(), AdmissionError>,
    ) -> Result<PendingSuccessorRetirementRequest<'step, 'a>, ProducerRecoveryError> {
        Ok(PendingSuccessorRetirementRequest {
            inner: self.inner.preview_next(
                digest,
                limits,
                input_permit,
                outcome_permit,
                authority,
            )?,
        })
    }
    pub(in crate::storage) fn finish_epoch(
        &mut self,
    ) -> Result<LineageEpochSummary, BootstrapError> {
        self.inner.finish_epoch()
    }
    pub(in crate::storage) fn finish(
        self,
        limits: AdmissionLimits,
        permit: OwnedPermit,
    ) -> Result<ResolvedSuccessorRetirement<'a>, ProducerRecoveryError> {
        Ok(ResolvedSuccessorRetirement {
            owner: self.owner,
            inner: self.inner.finish(limits, permit)?,
        })
    }
}
impl PendingSuccessorRetirementRequest<'_, '_> {
    pub(in crate::storage) fn resolved(&self) -> &ResolvedProducerRequest {
        self.inner.resolved()
    }
    pub(in crate::storage) fn request(&self) -> ProducerRequestId {
        self.inner.request()
    }
    /// Only a positively checked already-existing SQL copy permits acceptance.
    /// The native layer cannot establish that SQL premise itself.
    pub(in crate::storage) fn accept(
        self,
        authority: impl FnMut() -> Result<(), BootstrapError>,
    ) -> Result<(), BootstrapError> {
        self.inner.accept(authority)
    }
}
impl ResolvedSuccessorRetirement<'_> {
    pub(in crate::storage) fn owner(&self) -> &NativeSuccessorRetirement {
        self.owner
    }
    pub(in crate::storage) fn validate(
        &self,
        cancelled: &AtomicBool,
    ) -> Result<(), BootstrapError> {
        self.inner.validate(cancelled)
    }
    pub(in crate::storage) fn matches_owner(&self, owner: &NativeSuccessorRetirement) -> bool {
        std::ptr::eq(self.owner, owner) && self.inner.matches_owner(&owner.inner)
    }
    pub(in crate::storage) fn verified_through(&self) -> u64 {
        self.inner.verified_through()
    }
    pub(in crate::storage) fn last_epoch(&self) -> LineageEpochSummary {
        self.inner.last_epoch()
    }
    pub(in crate::storage) fn matches_recorded(
        &self,
        epoch: u64,
        records: impl IntoIterator<Item = (u64, [u8; 32], [u8; 32])>,
    ) -> bool {
        self.inner.matches_recorded(epoch, records)
    }
}
