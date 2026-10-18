//! Ordered positive history under one retained lineage grant. The result is a
//! native frontier, never proof of SQL activation, retirement or writer custody.
use super::lineage::{NativeSuccessorRecovery, RetainedLineage, RetainedTerminalKind};
use super::producer_recovery::{RecoveryContext, ResolvedProducerRequest};
use super::source_closure::Prefix;
use super::{
    AdmissionError, BootstrapError, IngestRecovery, PendingBatch, PendingError, PendingReadLimits,
    ProducerRecoveryError, ProducerRecoveryLimits,
};
use crate::{runtime::OwnedPermit, storage::rocks::succession::PredecessorTerminalKind};
use bigbrotr_model::{
    Digest256, ProducerEpoch, ProducerRequestId, RequestSequence, SourceRecordId,
};
use sha2::{Digest, Sha256};
use std::{fmt, sync::atomic::AtomicBool};

/// A historical pending read never turns missing data into an Archive result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LineagePendingError {
    Proof(BootstrapError),
    Read(PendingError),
    OutsideVerifiedCut,
    RequiredBatchMissing,
}
impl fmt::Display for LineagePendingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "verified lineage pending read failed: {self:?}")
    }
}
impl std::error::Error for LineagePendingError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LineageEpochSummary {
    pub epoch: u64,
    pub terminal_kind: RetainedTerminalKind,
    pub resolved_requests: u64,
    pub last_complete_sequence: u64,
    pub digest: [u8; 32],
    pub event_request_mask: u64,
}

pub struct LineageResolution<'a> {
    lineage: &'a RetainedLineage,
    recovery: &'a IngestRecovery,
    cancelled: &'a AtomicBool,
    through: u64,
    head_kind: Option<RetainedTerminalKind>,
    epoch: u64,
    prefix: Prefix,
    completed: Option<LineageEpochSummary>,
    recovery_owner: Option<&'a NativeSuccessorRecovery>,
}

pub struct ResolvedLineage<'a> {
    lineage: &'a RetainedLineage,
    recovery: &'a IngestRecovery,
    last: LineageEpochSummary,
}

impl RetainedLineage {
    /// The PG owner supplies the head classification from exact activation or
    /// its proved absence. This choice computes bytes, not SQL authority. An
    /// outgoing ancestor's retained kind is independently checked by replay.
    pub fn resolution<'a>(
        &'a self,
        recovery: &'a IngestRecovery,
        through_epoch: u64,
        head_kind: Option<PredecessorTerminalKind>,
        cancelled: &'a AtomicBool,
    ) -> Result<LineageResolution<'a>, BootstrapError> {
        self.resolution_in(recovery, through_epoch, head_kind, cancelled, None)
    }

    fn resolution_in<'a>(
        &'a self,
        recovery: &'a IngestRecovery,
        through_epoch: u64,
        head_kind: Option<PredecessorTerminalKind>,
        cancelled: &'a AtomicBool,
        recovery_owner: Option<&'a NativeSuccessorRecovery>,
    ) -> Result<LineageResolution<'a>, BootstrapError> {
        if let Some(owner) = recovery_owner {
            owner.validate(recovery, cancelled)?;
        } else {
            self.validate(recovery, cancelled)?;
        }
        let head = self.native_head();
        let epoch = head.manifest().identity().owner_epoch.get();
        if through_epoch == 0
            || through_epoch > epoch
            || (through_epoch == epoch && (head_kind.is_none() || !head.is_closing()))
            || (through_epoch < epoch
                && (through_epoch + 1 != epoch
                    || head_kind.is_some()
                    || head.issued_through() != 0
                    || head.source_through() != 0))
            || (head_kind == Some(PredecessorTerminalKind::UnactivatedEmpty)
                && (head.issued_through() != 0 || head.source_through() != 0))
        {
            return Err(BootstrapError::StateMismatch);
        }
        let first = self.entry(1).expect("inspected genesis");
        Ok(LineageResolution {
            lineage: self,
            recovery,
            cancelled,
            through: through_epoch,
            head_kind: head_kind.map(|kind| match kind {
                PredecessorTerminalKind::Activated => RetainedTerminalKind::Activated,
                PredecessorTerminalKind::UnactivatedEmpty => RetainedTerminalKind::UnactivatedEmpty,
            }),
            epoch: 1,
            prefix: Prefix::lineage(
                *first.manifest(),
                self.producer_slot_id(),
                first.issued_through(),
                digest_start(self, 1, RetainedTerminalKind::Initial)?,
            ),
            completed: None,
            recovery_owner,
        })
    }
}

impl<'a> LineageResolution<'a> {
    pub(super) fn for_recovery(
        owner: &'a NativeSuccessorRecovery,
        recovery: &'a IngestRecovery,
        cancelled: &'a AtomicBool,
    ) -> Result<Self, BootstrapError> {
        let lineage = owner.lineage();
        lineage.resolution_in(
            recovery,
            lineage
                .native_head()
                .manifest()
                .identity()
                .owner_epoch
                .get(),
            Some(PredecessorTerminalKind::Activated),
            cancelled,
            Some(owner),
        )
    }
    pub(super) fn validate(&self) -> Result<(), BootstrapError> {
        match self.recovery_owner {
            Some(owner) => owner.validate(self.recovery, self.cancelled),
            None => self.lineage.validate(self.recovery, self.cancelled),
        }
    }

    pub fn current_epoch(&self) -> Option<u64> {
        (self.epoch <= self.through).then_some(self.epoch)
    }
    pub fn completed_epoch(&self) -> Option<LineageEpochSummary> {
        self.completed
    }
    pub fn next_request(&self) -> Option<ProducerRequestId> {
        let entry = self.lineage.entry(self.current_epoch()?)?;
        let next = self.prefix.requests().checked_add(1)?;
        (next <= entry.issued_through()).then(|| ProducerRequestId {
            producer_slot_id: self.lineage.producer_slot_id(),
            producer_epoch: ProducerEpoch::new(self.epoch).expect("inspected epoch"),
            request_sequence: RequestSequence::new(next).expect("positive next request"),
        })
    }

    /// Resolving does not advance the prefix. The connected owner can compare
    /// the exact existing PG copy before recording this native observation.
    pub fn resolve_next(
        &self,
        digest: Digest256,
        limits: ProducerRecoveryLimits,
        input_permit: OwnedPermit,
        outcome_permit: OwnedPermit,
        authority: impl FnMut() -> Result<(), AdmissionError>,
    ) -> Result<ResolvedProducerRequest, ProducerRecoveryError> {
        let request = self
            .next_request()
            .ok_or(ProducerRecoveryError::Unresolved)?;
        self.recovery.resolve_original_request_in(
            request,
            digest,
            limits,
            input_permit,
            outcome_permit,
            authority,
            match self.recovery_owner {
                Some(owner) => RecoveryContext::Successor {
                    owner,
                    epoch: self.epoch,
                    cancelled: self.cancelled,
                },
                None => RecoveryContext::Lineage {
                    lineage: self.lineage,
                    epoch: self.epoch,
                    cancelled: self.cancelled,
                },
            },
        )
    }

    pub fn record(&mut self, resolved: &ResolvedProducerRequest) -> Result<(), BootstrapError> {
        let prefix = self.preview(resolved)?;
        self.prefix = prefix;
        Ok(())
    }

    pub(super) fn preview(
        &self,
        resolved: &ResolvedProducerRequest,
    ) -> Result<Prefix, BootstrapError> {
        self.validate()?;
        resolved.require_native_context(self.recovery)?;
        resolved.require_lineage(self.lineage)?;
        if self.current_epoch().is_none() {
            return Err(BootstrapError::StateMismatch);
        }
        let epoch = self.epoch;
        let mut prefix = self.prefix.clone();
        prefix.record_with_frontier(resolved, |source| {
            source.owner_epoch.get() < epoch
                && self
                    .lineage
                    .entry(source.owner_epoch.get())
                    .is_some_and(|entry| source.sequence.get() <= entry.source_through())
        })?;
        Ok(prefix)
    }

    pub(super) fn accept_preview(&mut self, prefix: Prefix) {
        self.prefix = prefix;
    }

    pub fn finish_epoch(&mut self) -> Result<LineageEpochSummary, BootstrapError> {
        self.validate()?;
        let epoch = self.current_epoch().ok_or(BootstrapError::StateMismatch)?;
        let entry = self
            .lineage
            .entry(epoch)
            .ok_or(BootstrapError::StateMismatch)?;
        let (kind, expected) = match entry.terminal_claim() {
            Some((kind, digest)) => (kind, Some(digest)),
            None => (self.head_kind.ok_or(BootstrapError::StateMismatch)?, None),
        };
        let digest = self.prefix.completed_digest();
        if !entry.is_closing()
            || self.prefix.requests() != entry.issued_through()
            || self.prefix.source_through() != entry.source_through()
            || expected.is_some_and(|expected| digest != expected)
        {
            return Err(BootstrapError::StateMismatch);
        }
        let summary = LineageEpochSummary {
            epoch,
            terminal_kind: kind,
            resolved_requests: self.prefix.requests(),
            last_complete_sequence: self.prefix.source_through(),
            digest,
            event_request_mask: self.prefix.event_requests(),
        };
        // Form the next state before advancing; every refused operation leaves
        // the current frontier and its exact next request unchanged.
        let next = epoch + 1;
        let prefix = if next <= self.through {
            let entry = self
                .lineage
                .entry(next)
                .ok_or(BootstrapError::StateMismatch)?;
            let kind = entry
                .terminal_claim()
                .map(|v| v.0)
                .or(self.head_kind)
                .ok_or(BootstrapError::StateMismatch)?;
            Some(Prefix::lineage(
                *entry.manifest(),
                self.lineage.producer_slot_id(),
                entry.issued_through(),
                digest_start(self.lineage, next, kind)?,
            ))
        } else {
            None
        };
        self.epoch = next;
        if let Some(prefix) = prefix {
            self.prefix = prefix;
        }
        self.completed = Some(summary);
        Ok(summary)
    }

    pub fn finish(
        self,
        limits: super::AdmissionLimits,
        permit: OwnedPermit,
    ) -> Result<ResolvedLineage<'a>, AdmissionError> {
        // Recovery has a separate result even when every current request was
        // positively resolved. It cannot construct a predecessor/handoff proof.
        if self.recovery_owner.is_some() {
            return Err(AdmissionError::InvalidInput);
        }
        let lineage = self.lineage;
        let recovery = self.recovery;
        let last = self.finish_checked(limits, permit)?;
        Ok(ResolvedLineage {
            lineage,
            recovery,
            last,
        })
    }

    pub(super) fn finish_checked(
        self,
        limits: super::AdmissionLimits,
        permit: OwnedPermit,
    ) -> Result<LineageEpochSummary, AdmissionError> {
        if self.current_epoch().is_some() {
            return Err(AdmissionError::StateMismatch);
        }
        self.validate()?;
        self.lineage.verify_effect_census(
            self.recovery,
            self.through,
            limits,
            permit,
            self.cancelled,
        )?;
        self.validate()?;
        self.completed.ok_or(AdmissionError::StateMismatch)
    }
}

impl ResolvedLineage<'_> {
    pub(super) fn recovery(&self) -> &IngestRecovery {
        self.recovery
    }
    pub fn verified_through(&self) -> u64 {
        self.last.epoch
    }
    pub fn last_epoch(&self) -> LineageEpochSummary {
        self.last
    }
    pub fn lineage(&self) -> &RetainedLineage {
        self.lineage
    }
    pub fn matches_lineage(&self, lineage: &RetainedLineage) -> bool {
        std::ptr::eq(self.lineage, lineage)
    }
    pub fn validate(&self, cancelled: &AtomicBool) -> Result<(), BootstrapError> {
        self.lineage.validate(self.recovery, cancelled)
    }

    /// Read one complete original batch under the full native-head proof.
    /// SQL terminal state, current Control and Archive authority remain caller
    /// obligations. The returned bytes retain their grant but confer no writer,
    /// publication or acknowledgement capability. The caller keeps this proof
    /// and its native owner alive and revalidates before downstream effects.
    pub fn read_complete_pending_batch(
        &self,
        first: SourceRecordId,
        limits: PendingReadLimits,
        permit: OwnedPermit,
        cancelled: &AtomicBool,
    ) -> Result<PendingBatch, LineagePendingError> {
        // Admit the complete pending allocation before even rereading sidecars.
        // The snapshot reader independently enforces the same floor.
        let required = limits
            .required_data_bytes()
            .map_err(LineagePendingError::Read)?;
        if permit.request().units().data_bytes() < required || permit.request().units().items() == 0
        {
            return Err(LineagePendingError::Read(
                PendingError::InsufficientReservation,
            ));
        }
        if self.verified_through()
            != self
                .lineage
                .native_head()
                .manifest()
                .identity()
                .owner_epoch
                .get()
            || self.lineage.pending_target().is_some()
        {
            return Err(LineagePendingError::Proof(BootstrapError::StateMismatch));
        }
        self.validate(cancelled)
            .map_err(LineagePendingError::Proof)?;
        if first.dataset_id != self.recovery.manifest().identity().dataset_id {
            return Err(LineagePendingError::Read(PendingError::ForeignDataset));
        }
        let entry = self
            .lineage
            .entry(first.owner_epoch.get())
            .filter(|entry| {
                first.shard_id == entry.manifest().identity().shard_id
                    && first.owner_epoch.get() <= self.verified_through()
                    && first.sequence.get() <= entry.source_through()
            })
            .ok_or(LineagePendingError::OutsideVerifiedCut)?;
        let result = self
            .recovery
            .store
            .read_pending_batch(first, limits, permit, cancelled)
            .map_err(LineagePendingError::Read)
            .and_then(|batch| batch.ok_or(LineagePendingError::RequiredBatchMissing))
            .and_then(|batch| {
                let last = batch.batch().last();
                if batch.batch().first() != first
                    || last.dataset_id != first.dataset_id
                    || last.shard_id != first.shard_id
                    || last.owner_epoch != first.owner_epoch
                    || last.sequence.get() > entry.source_through()
                {
                    Err(LineagePendingError::OutsideVerifiedCut)
                } else {
                    Ok(batch)
                }
            });
        // Always check after the snapshot read, including its failures. Preserve
        // its original error if validation also fails; neither outcome advances
        // a cursor or delivers a partial batch.
        let validated = self.validate(cancelled).map_err(LineagePendingError::Proof);
        let batch = result?;
        validated?;
        Ok(batch)
    }

    pub fn matches_recorded(
        &self,
        epoch: u64,
        records: impl IntoIterator<Item = (u64, [u8; 32], [u8; 32])>,
    ) -> bool {
        matches_recorded(self.lineage, self.last, epoch, records)
    }
}

pub(super) fn matches_recorded(
    lineage: &RetainedLineage,
    last: LineageEpochSummary,
    epoch: u64,
    records: impl IntoIterator<Item = (u64, [u8; 32], [u8; 32])>,
) -> bool {
    if epoch == 0 || epoch > last.epoch {
        return false;
    }
    let Some(entry) = lineage.entry(epoch) else {
        return false;
    };
    let (kind, expected) = if epoch == last.epoch {
        (last.terminal_kind, last.digest)
    } else {
        let Some(pair) = entry.terminal_claim() else {
            return false;
        };
        pair
    };
    let Ok(mut digest) = digest_start(lineage, epoch, kind) else {
        return false;
    };
    let mut count = 0_u64;
    for (sequence, input, outcome) in records {
        if count.checked_add(1) != Some(sequence) || sequence > entry.issued_through() {
            return false;
        }
        digest.update(sequence.to_be_bytes());
        digest.update(input);
        digest.update(outcome);
        count = sequence;
    }
    digest.update(entry.source_through().to_be_bytes());
    count == entry.issued_through() && <[u8; 32]>::from(digest.finalize()) == expected
}

fn digest_start(
    lineage: &RetainedLineage,
    epoch: u64,
    kind: RetainedTerminalKind,
) -> Result<Sha256, BootstrapError> {
    let entry = lineage.entry(epoch).ok_or(BootstrapError::StateMismatch)?;
    if (epoch == 1) != (kind == RetainedTerminalKind::Initial)
        || (kind == RetainedTerminalKind::UnactivatedEmpty
            && (entry.issued_through() != 0 || entry.source_through() != 0))
    {
        return Err(BootstrapError::StateMismatch);
    }
    let domain: &[u8] = match (epoch, kind) {
        (1, RetainedTerminalKind::Initial) => b"bigbrotr/initial-source-closure/1\0",
        (2, RetainedTerminalKind::Activated) => b"bigbrotr/activated-initial-source-closure/1\0",
        (2, RetainedTerminalKind::UnactivatedEmpty) => {
            b"bigbrotr/initial-empty-attempt-closure/1\0"
        }
        (_, RetainedTerminalKind::Activated) => b"bigbrotr/ingest-successor-source-closure/1\0",
        (_, RetainedTerminalKind::UnactivatedEmpty) => {
            b"bigbrotr/ingest-successor-empty-closure/1\0"
        }
        _ => return Err(BootstrapError::StateMismatch),
    };
    let mut digest = Sha256::new();
    digest.update(domain);
    if epoch != 1 {
        digest.update(entry.incoming_handoff_sha256());
    }
    digest.update(Sha256::digest(entry.manifest().encode().as_bytes()));
    digest.update(lineage.producer_slot_id().as_bytes());
    digest.update(epoch.to_be_bytes());
    digest.update(entry.issued_through().to_be_bytes());
    Ok(digest)
}
