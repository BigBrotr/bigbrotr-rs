//! Current Archive metadata under a fully terminal positive Ingest history.
//! Neither SQL rows nor the reconstructed cut grant catalog presence, exclusion,
//! publication, a read pin or permission to replay an absent attempt.
use super::{
    ArchiveFrontierReceipt, ControlSession, InitialHistoryRequest, OwnerError,
    ProducerResolveError, SuccessorHistoryState, VerifiedSuccessorHistory, initial_history,
    successor_history, successor_transition::SuccessorCompactionCheck,
};
use crate::{
    runtime::OwnedPermit,
    storage::{
        archive::{LocalCoordinator, RetainedInputs},
        postgres::archive_attempt::{ArchiveAttemptRequest, ArchiveAttemptScan},
        rocks::store::{AdmissionLimits, BootstrapError, IngestRecovery, PendingBatchHeader},
    },
};
use bigbrotr_model::{
    DatasetId, Digest256, OwnerResourceId, ProcessIncarnation, TopologyGeneration,
};
use bigbrotr_protocol::{
    archive_cut::{CutBound, CutRead, MAX_BATCHES},
    archive_receipt::{MAX_RANGES, ReceiptExpectation, ReceiptRange},
};
use std::{fmt, mem::size_of, sync::atomic::Ordering};
use tokio_postgres::{Row, Transaction};

const MAX_ATTEMPTS: u64 = 128;
const METADATA_BYTES: u64 = 256 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetainedArchiveError {
    InvalidInput,
    InvalidMetadata,
    ResourceLimit,
    Prerequisite,
    Backend,
    History(ProducerResolveError),
    Native(BootstrapError),
    Unavailable,
}
impl fmt::Display for RetainedArchiveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "retained Archive frontier unavailable: {self:?}")
    }
}
impl std::error::Error for RetainedArchiveError {}
impl From<ProducerResolveError> for RetainedArchiveError {
    fn from(value: ProducerResolveError) -> Self {
        Self::History(value)
    }
}
use RetainedArchiveError as Error;
fn backend(_: tokio_postgres::Error) -> Error {
    Error::Backend
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Bound {
    value: CutBound,
    attestation: Option<Digest256>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SelectedAttempt {
    id: [u8; 16],
    ordinal: u64,
    topology: TopologyGeneration,
    digest: Digest256,
    coordinator: ProcessIncarnation,
    worker: ProcessIncarnation,
}
impl SelectedAttempt {
    fn matches(&self, request: &ArchiveAttemptRequest) -> bool {
        self.id == *request.attempt_id()
            && self.ordinal == request.ordinal()
            && self.topology == request.topology_generation()
            && self.digest == request.batch_digest()
            && self.coordinator == request.coordinator()
            && self.worker == request.worker()
    }
}
struct Current {
    receipt: ArchiveFrontierReceipt,
    topology: TopologyGeneration,
    bounds: Vec<Bound>,
    attempts: Vec<SelectedAttempt>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Reserved,
    Loaded,
    Bound,
}

struct Observation {
    dataset: DatasetId,
    catalog: OwnerResourceId,
    binding: Digest256,
    issued: u64,
    current: Option<Current>,
}

/// Bounded SQL observation anchored in a complete positive terminal history.
/// Input handles still have to be bound; receipt/file presence remains separate.
/// The caller keeps the actual native/coordinator and database driver custody.
pub struct RetainedArchiveFrontier {
    dataset: DatasetId,
    catalog: OwnerResourceId,
    binding: Digest256,
    issued: u64,
    current: Option<Current>,
    permit: OwnedPermit,
    phase: Phase,
    compacted: Option<SuccessorCompactionCheck>,
    bound: Option<BoundRetainedArchiveFrontier>,
}

/// Historical cut reconstructed from original retained headers. This owns its
/// observation allocation, but is not a CapturedCut or any mutation authority.
pub struct BoundRetainedArchiveFrontier {
    receipt: Option<ArchiveFrontierReceipt>,
    read: Option<CutRead>,
    issued: u64,
    // In-place binding borrows the outer frontier owner which keeps its permit.
    // The historical consuming conversion instead transfers that same permit.
    _permit: Option<OwnedPermit>,
}

/// Actual retained coordinator and completed, current-Control inventory. Neither
/// borrowed value certifies a catalog result or permission to replay an attempt.
pub struct RetainedArchiveRequest<'a> {
    pub coordinator: &'a LocalCoordinator,
    pub attempts: &'a ArchiveAttemptScan,
}
impl RetainedArchiveFrontier {
    pub const fn required_data_bytes() -> u64 {
        METADATA_BYTES
    }
    /// Resource reservation only. Install this owner in native-first custody
    /// before connecting; no query or observation has succeeded at this point.
    pub fn reserve_compacted(
        coordinator: &LocalCoordinator,
        limits: AdmissionLimits,
        effects: OwnedPermit,
        metadata: OwnedPermit,
    ) -> Result<Self, Error> {
        require_capacity(0, &metadata)?;
        let check = SuccessorCompactionCheck::new(limits, effects)?;
        Ok(Self {
            dataset: coordinator.identity().dataset(),
            catalog: coordinator.identity().catalog(),
            binding: coordinator.binding_digest(),
            issued: 0,
            current: None,
            permit: metadata,
            phase: Phase::Reserved,
            compacted: Some(check),
            bound: None,
        })
    }

    fn observed(observation: Observation, permit: OwnedPermit) -> Self {
        Self {
            dataset: observation.dataset,
            catalog: observation.catalog,
            binding: observation.binding,
            issued: observation.issued,
            current: observation.current,
            permit,
            phase: Phase::Loaded,
            compacted: None,
            bound: None,
        }
    }

    fn require_phase(&self, phase: Phase) -> Result<(), Error> {
        if self.phase == phase {
            Ok(())
        } else {
            Err(Error::InvalidInput)
        }
    }

    pub fn current_receipt(&self) -> Result<Option<ArchiveFrontierReceipt>, Error> {
        self.require_phase(Phase::Loaded)?;
        Ok(self.current.as_ref().map(|value| value.receipt))
    }
    pub fn issued_attempts(&self) -> Result<u64, Error> {
        self.require_phase(Phase::Loaded)?;
        Ok(self.issued)
    }
    pub fn selected_attempt_ids(&self) -> Result<impl Iterator<Item = [u8; 16]> + '_, Error> {
        self.require_phase(Phase::Loaded)?;
        Ok(self
            .current
            .iter()
            .flat_map(|value| value.attempts.iter().map(|attempt| attempt.id)))
    }

    /// Binding does not consume this resource keeper, even on allocation or
    /// input failure. Only a fully validated result advances the private phase.
    pub fn bind_current_inputs_in_place(
        &mut self,
        inputs: &[&RetainedInputs],
    ) -> Result<(), Error> {
        self.require_phase(Phase::Loaded)?;
        let read = self.bind(inputs.iter().map(|input| Input {
            request: input.request(),
            headers: input.headers(),
            expectation: input.expectation(),
            binding: input.coordinator_binding(),
        }))?;
        let bound = BoundRetainedArchiveFrontier {
            receipt: self.current_receipt()?,
            read,
            issued: self.issued,
            _permit: None,
        };
        // No fallible operation follows. Current projection is no longer needed;
        // the same outer permit covers the bound cut for its entire lifetime.
        self.current = None;
        self.bound = Some(bound);
        self.phase = Phase::Bound;
        Ok(())
    }

    pub fn bound_current_inputs(&self) -> Result<&BoundRetainedArchiveFrontier, Error> {
        self.require_phase(Phase::Bound)?;
        self.bound.as_ref().ok_or(Error::InvalidInput)
    }

    pub fn has_compacted_history(&self) -> bool {
        self.compacted.is_some()
    }

    pub(super) fn compacted_check(&self) -> Result<&SuccessorCompactionCheck, Error> {
        self.require_phase(Phase::Bound)?;
        self.compacted.as_ref().ok_or(Error::InvalidInput)
    }

    pub(super) fn require_compacted_context(
        &self,
        proof: &VerifiedSuccessorHistory<'_>,
        recovery: &IngestRecovery,
        catalog: OwnerResourceId,
        binding: Digest256,
    ) -> Result<&SuccessorCompactionCheck, Error> {
        require_terminal_basis(proof, true)?;
        self.require_binding(recovery.manifest().identity().dataset_id, catalog, binding)?;
        self.compacted_check()
    }

    fn require_binding(
        &self,
        dataset: DatasetId,
        catalog: OwnerResourceId,
        binding: Digest256,
    ) -> Result<(), Error> {
        if self.dataset != dataset || self.catalog != catalog || self.binding != binding {
            Err(Error::InvalidInput)
        } else {
            Ok(())
        }
    }

    pub(super) fn validate_compacted(
        &self,
        proof: &VerifiedSuccessorHistory<'_>,
        recovery: &IngestRecovery,
        cancelled: &std::sync::atomic::AtomicBool,
    ) -> Result<(), Error> {
        self.compacted_check()?
            .validate_history(recovery, proof.history(), cancelled)?;
        proof.validate(cancelled).map_err(Error::Native)
    }
    /// References may include the complete retained inventory, including failed
    /// attempts. No nonclone custody/permit owner is copied or consumed here.
    /// Every selected identity must occur exactly once; extra selected/foreign
    /// or duplicate inventory identities refuse rather than silently matching.
    pub fn bind_current_inputs(
        self,
        inputs: &[&RetainedInputs],
    ) -> Result<BoundRetainedArchiveFrontier, Error> {
        self.require_phase(Phase::Loaded)?;
        // Compacted custody must remain in its outer owner on every Err path.
        if self.compacted.is_some() {
            return Err(Error::InvalidInput);
        }
        if inputs.len() > MAX_ATTEMPTS as usize {
            return Err(Error::ResourceLimit);
        }
        let read = self.bind(inputs.iter().map(|input| Input {
            request: input.request(),
            headers: input.headers(),
            expectation: input.expectation(),
            binding: input.coordinator_binding(),
        }))?;
        Ok(BoundRetainedArchiveFrontier {
            receipt: self.current_receipt()?,
            read,
            issued: self.issued,
            _permit: Some(self.permit),
        })
    }

    fn bind<'a>(
        &self,
        inputs: impl Iterator<Item = Input<'a>> + Clone,
    ) -> Result<Option<CutRead>, Error> {
        self.require_phase(Phase::Loaded)?;
        bind_current(
            self.dataset,
            self.catalog,
            self.binding,
            self.issued,
            self.current.as_ref(),
            inputs,
        )
    }
}
/// Shared structural reconstruction only. The caller supplies its separately
/// checked metadata basis; this does not establish source or native authority.
fn bind_current<'a>(
    dataset: DatasetId,
    catalog: OwnerResourceId,
    binding: Digest256,
    issued: u64,
    current: Option<&Current>,
    inputs: impl Iterator<Item = Input<'a>> + Clone,
) -> Result<Option<CutRead>, Error> {
    let count = inputs.clone().take(MAX_ATTEMPTS as usize + 1).count();
    if count > MAX_ATTEMPTS as usize {
        return Err(Error::ResourceLimit);
    }
    for (index, input) in inputs.clone().enumerate() {
        let request = input.request;
        if request.dataset() != dataset
            || request.catalog() != catalog
            || input.binding != binding
            || request.ordinal() == 0
            || request.ordinal() > issued
            || inputs.clone().take(index).any(|old| {
                old.request.attempt_id() == request.attempt_id()
                    || old.request.ordinal() == request.ordinal()
            })
        {
            return Err(Error::InvalidMetadata);
        }
    }
    let Some(current) = current else {
        return Ok(None);
    };
    let mut receipts = Vec::new();
    receipts
        .try_reserve_exact(current.attempts.len())
        .map_err(|_| Error::ResourceLimit)?;
    let mut ranges = 0_usize;
    for selected in &current.attempts {
        let input = inputs
            .clone()
            .find(|input| *input.request.attempt_id() == selected.id)
            .ok_or(Error::InvalidMetadata)?;
        if !selected.matches(input.request) {
            return Err(Error::InvalidMetadata);
        }
        ranges = ranges
            .checked_add(input.expectation.input_ranges.len())
            .filter(|count| *count <= MAX_RANGES)
            .ok_or(Error::ResourceLimit)?;
        require_headers(&input)?;
        receipts.push(input.expectation.clone());
    }
    let mut bounds = Vec::new();
    bounds
        .try_reserve_exact(current.bounds.len())
        .map_err(|_| Error::ResourceLimit)?;
    bounds.extend(current.bounds.iter().map(|bound| bound.value));
    let read = CutRead {
        dataset_id: dataset,
        topology_generation: current.topology,
        bounds,
        receipts,
        snapshot_id: Some(current.receipt.snapshot_id),
    };
    let count = read
        .validate(usize::MAX)
        .map_err(|_| Error::InvalidMetadata)?;
    if read.digest() != current.receipt.request_digest
        || current
            .receipt
            .family_counts
            .iter()
            .try_fold(0_u64, |n, value| n.checked_add(*value))
            != Some(count)
    {
        return Err(Error::InvalidMetadata);
    }
    for bound in &current.bounds {
        let digest = read
            .receipts
            .iter()
            .find(|receipt| {
                receipt.input_ranges.iter().any(|range| {
                    (range.shard_id, range.owner_epoch, range.last_sequence)
                        == (
                            bound.value.shard_id,
                            bound.value.owner_epoch,
                            bound.value.complete_through,
                        )
                })
            })
            .map(|receipt| receipt.batch_digest);
        if digest != bound.attestation {
            return Err(Error::InvalidMetadata);
        }
    }
    Ok(Some(read))
}

impl BoundRetainedArchiveFrontier {
    pub fn current_receipt(&self) -> Option<ArchiveFrontierReceipt> {
        self.receipt
    }
    pub fn current_cut(&self) -> Option<&CutRead> {
        self.read.as_ref()
    }
    pub fn issued_attempts(&self) -> u64 {
        self.issued
    }
    /// Capacity observation only; later issuance must recheck current history.
    pub fn admits_new_attempts(&self, planned: u64) -> bool {
        self.issued
            .checked_add(planned)
            .is_some_and(|n| n <= MAX_ATTEMPTS)
    }
}

#[derive(Clone, Copy)]
struct Input<'a> {
    request: &'a ArchiveAttemptRequest,
    headers: &'a [PendingBatchHeader],
    expectation: &'a ReceiptExpectation,
    binding: Digest256,
}
fn require_headers(input: &Input<'_>) -> Result<(), Error> {
    let r = input.request;
    let expected = input.expectation;
    if expected.dataset_id != r.dataset()
        || expected.schema_version != 1
        || expected.topology_generation != r.topology_generation()
        || expected.batch_digest != r.batch_digest()
        || expected.attempt_id != *r.attempt_id()
        || expected.publication_ordinal != r.ordinal()
        || input.headers.is_empty()
        || input.headers.len() > MAX_RANGES
    {
        return Err(Error::InvalidMetadata);
    }
    let mut count = 0_u64;
    let mut previous = None;
    let mut accumulated: Option<ReceiptRange> = None;
    let mut ranges = expected.input_ranges.iter();
    for header in input.headers {
        let first = header.first();
        let last = header.last();
        if first.dataset_id != r.dataset()
            || last.dataset_id != first.dataset_id
            || last.shard_id != first.shard_id
            || last.owner_epoch != first.owner_epoch
            || previous.is_some_and(|prior| prior >= first)
        {
            return Err(Error::InvalidMetadata);
        }
        count = last
            .sequence
            .get()
            .checked_sub(first.sequence.get())
            .and_then(|n| n.checked_add(1))
            .and_then(|n| count.checked_add(n))
            .ok_or(Error::InvalidMetadata)?;
        previous = Some(last);
        if let Some(range) = accumulated.as_mut().filter(|range| {
            range.shard_id == first.shard_id
                && range.owner_epoch == first.owner_epoch.get()
                && range.last_sequence.checked_add(1) == Some(first.sequence.get())
        }) {
            range.last_sequence = last.sequence.get();
        } else {
            if accumulated
                .as_ref()
                .is_some_and(|range| ranges.next() != Some(range))
            {
                return Err(Error::InvalidMetadata);
            }
            accumulated = Some(ReceiptRange {
                shard_id: first.shard_id,
                owner_epoch: first.owner_epoch.get(),
                first_sequence: first.sequence.get(),
                last_sequence: last.sequence.get(),
            });
        }
    }
    if accumulated.as_ref() != ranges.next()
        || ranges.next().is_some()
        || count != expected.record_count
    {
        return Err(Error::InvalidMetadata);
    }
    Ok(())
}

impl ControlSession {
    /// Load into an already retained resource owner. Cancellation or any loader
    /// error cannot consume its canonical Check or turn Reserved into absence.
    #[allow(
        clippy::too_many_arguments,
        reason = "existing history, custody, authority and bounded metadata"
    )]
    pub async fn inspect_compacted_retained_archive_frontier(
        &mut self,
        frontier: &mut RetainedArchiveFrontier,
        proof: &VerifiedSuccessorHistory<'_>,
        recovery: &IngestRecovery,
        retained: RetainedArchiveRequest<'_>,
        input: InitialHistoryRequest<'_>,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<(), Error> {
        frontier.require_phase(Phase::Reserved)?;
        let check = frontier.compacted.as_ref().ok_or(Error::InvalidInput)?;
        require_terminal_basis(proof, true)?;
        let coordinator = retained.coordinator;
        let scan = retained.attempts;
        if coordinator.identity().dataset() != self.request.dataset
            || coordinator.identity().catalog() == self.identity.resource_id
            || frontier.dataset != coordinator.identity().dataset()
            || frontier.catalog != coordinator.identity().catalog()
            || frontier.binding != coordinator.binding_digest()
            || !scan.belongs_to(self.request.dataset, self.identity)
            || !scan.is_complete()
        {
            return Err(Error::InvalidInput);
        }
        require_capacity(scan.high_water(), &frontier.permit)?;
        proof.validate(input.cancelled).map_err(Error::Native)?;
        check.validate_history(recovery, proof.history(), input.cancelled)?;
        let current = self.identity;
        let (tx, mut lease, policy) = self
            .begin_checked_successor_history(proof.history(), recovery, &input, validate)
            .await?;
        check
            .require_history(
                &tx,
                recovery,
                proof.history(),
                &mut lease,
                policy,
                input.cancelled,
            )
            .await?;
        let observation = load(&tx, proof, coordinator, scan.high_water()).await?;
        proof.validate(input.cancelled).map_err(Error::Native)?;
        check.validate_history(recovery, proof.history(), input.cancelled)?;
        successor_history::finish_read(tx, &mut lease, policy, input.cancelled, current).await?;
        proof.validate(input.cancelled).map_err(Error::Native)?;
        check.validate_history(recovery, proof.history(), input.cancelled)?;
        initial_history::check_delivery(&mut lease, policy, input.cancelled)?;
        if self.client.is_closed() {
            return Err(Error::Unavailable);
        }
        // All fallible work finished. The original keeper and allocation stay.
        frontier.issued = observation.issued;
        frontier.current = observation.current;
        frontier.phase = Phase::Loaded;
        self.available = true;
        Ok(())
    }

    /// Short same-transaction SQL H/D check at a downstream proof-use boundary.
    /// No transaction is carried across a native worker or catalog await.
    pub async fn recheck_compacted_archive_history(
        &mut self,
        frontier: &RetainedArchiveFrontier,
        proof: &VerifiedSuccessorHistory<'_>,
        recovery: &IngestRecovery,
        input: InitialHistoryRequest<'_>,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<(), Error> {
        let check = frontier.compacted_check()?;
        require_terminal_basis(proof, true)?;
        if frontier.dataset != self.request.dataset
            || frontier.dataset != recovery.manifest().identity().dataset_id
        {
            return Err(Error::InvalidInput);
        }
        let current = self.identity;
        let (tx, mut lease, policy) = self
            .begin_checked_successor_history(proof.history(), recovery, &input, validate)
            .await?;
        check
            .require_history(
                &tx,
                recovery,
                proof.history(),
                &mut lease,
                policy,
                input.cancelled,
            )
            .await?;
        proof.validate(input.cancelled).map_err(Error::Native)?;
        successor_history::finish_read(tx, &mut lease, policy, input.cancelled, current).await?;
        check.validate_history(recovery, proof.history(), input.cancelled)?;
        proof.validate(input.cancelled).map_err(Error::Native)?;
        initial_history::check_delivery(&mut lease, policy, input.cancelled)?;
        if self.client.is_closed() {
            return Err(Error::Unavailable);
        }
        self.available = true;
        Ok(())
    }

    /// Inspect only: no new attempt, native operation, catalog seal or frontier.
    /// A completed ordinary inventory cursor is required, not a caller counter.
    /// Every returned collection and all temporary decoding own this permit.
    pub async fn inspect_retained_archive_frontier(
        &mut self,
        proof: &VerifiedSuccessorHistory<'_>,
        recovery: &IngestRecovery,
        retained: RetainedArchiveRequest<'_>,
        input: InitialHistoryRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<RetainedArchiveFrontier, Error> {
        let coordinator = retained.coordinator;
        let scan = retained.attempts;
        if input.cancelled.load(Ordering::Relaxed) {
            self.available = false;
            return Err(Error::Unavailable);
        }
        require_terminal(
            proof.history().state(),
            proof.history().native_epoch(),
            proof.history().registered_epoch(),
            proof.verified_through(),
            proof.compacted_initial_requests(),
        )?;
        let identity = coordinator.identity();
        if identity.dataset() != self.request.dataset
            || identity.catalog() == self.identity.resource_id
            || !scan.belongs_to(self.request.dataset, self.identity)
            || !scan.is_complete()
        {
            return Err(Error::InvalidInput);
        }
        require_capacity(scan.high_water(), &permit)?;
        proof.validate(input.cancelled).map_err(Error::Native)?;
        proof
            .history()
            .lineage()
            .validate(recovery, input.cancelled)
            .map_err(Error::Native)?;
        let current_control = self.identity;
        let (tx, mut lease, policy) = self
            .begin_checked_successor_history(proof.history(), recovery, &input, validate)
            .await?;
        let observation = load(&tx, proof, coordinator, scan.high_water()).await?;
        proof.validate(input.cancelled).map_err(Error::Native)?;
        proof
            .history()
            .lineage()
            .validate(recovery, input.cancelled)
            .map_err(Error::Native)?;
        successor_history::finish_read(tx, &mut lease, policy, input.cancelled, current_control)
            .await?;
        proof.validate(input.cancelled).map_err(Error::Native)?;
        proof
            .history()
            .lineage()
            .validate(recovery, input.cancelled)
            .map_err(Error::Native)?;
        initial_history::check_delivery(&mut lease, policy, input.cancelled)
            .map_err(Error::History)?;
        if self.client.is_closed() {
            return Err(Error::Unavailable);
        }
        self.available = true;
        Ok(RetainedArchiveFrontier::observed(observation, permit))
    }
}

pub(super) fn require_terminal_basis(
    proof: &VerifiedSuccessorHistory<'_>,
    compacted: bool,
) -> Result<(), Error> {
    let history = proof.history();
    require_terminal_in(
        history.state(),
        history.native_epoch(),
        history.registered_epoch(),
        proof.verified_through(),
        proof.compacted_initial_requests(),
        compacted,
    )
}

fn require_terminal(
    state: SuccessorHistoryState,
    native: u64,
    registered: u64,
    through: u64,
    compacted_initial: u64,
) -> Result<(), Error> {
    require_terminal_in(state, native, registered, through, compacted_initial, false)
}

fn require_terminal_in(
    state: SuccessorHistoryState,
    native: u64,
    registered: u64,
    through: u64,
    compacted_initial: u64,
    compacted: bool,
) -> Result<(), Error> {
    if (compacted_initial != 0) != compacted
        || compacted && (!(1..=64).contains(&compacted_initial) || native < 3)
        || state != SuccessorHistoryState::Terminal
        || native != registered
        || native != through
        || !(2..=255).contains(&native)
    {
        Err(Error::InvalidInput)
    } else {
        Ok(())
    }
}

fn require_capacity(issued: u64, permit: &OwnedPermit) -> Result<(), Error> {
    if issued > MAX_ATTEMPTS
        || permit.request().units().data_bytes() < METADATA_BYTES
        || permit.request().units().items() < 1
    {
        Err(Error::ResourceLimit)
    } else {
        Ok(())
    }
}

async fn load(
    tx: &Transaction<'_>,
    proof: &VerifiedSuccessorHistory<'_>,
    coordinator: &LocalCoordinator,
    issued: u64,
) -> Result<Observation, Error> {
    let dataset = coordinator.identity().dataset();
    let catalog = coordinator.identity().catalog();
    let binding = coordinator.binding_digest();
    let d = &dataset.as_bytes()[..];
    let c = &catalog.as_bytes()[..];
    let b = &binding.as_bytes()[..];
    let valid: bool = tx
        .query_one(PREREQUISITES, &[&d, &c, &b, &issued.to_string()])
        .await
        .map_err(backend)?
        .try_get(0)
        .map_err(backend)?;
    if !valid {
        return Err(Error::Prerequisite);
    }
    let row = tx.query_opt(CURRENT, &[&d, &c]).await.map_err(backend)?;
    let Some(row) = row else {
        let contradictory: bool = tx
            .query_one(ABSENT_CONTRADICTIONS, &[&d, &c])
            .await
            .map_err(backend)?
            .try_get(0)
            .map_err(backend)?;
        require_absent(contradictory)?;
        return Ok(Observation {
            dataset,
            catalog,
            binding,
            issued,
            current: None,
        });
    };
    let (receipt, topology) = decode_current(&row, catalog)?;
    let revision = receipt.revision;
    drop(row);
    let lineage = proof.history().lineage();
    let epochs = lineage
        .entries()
        .iter()
        .find(|entry| entry.manifest().identity().topology_generation == topology)
        .map(|entry| entry.manifest().identity().owner_epoch.get());
    let first_topology = lineage
        .entry(1)
        .ok_or(Error::InvalidMetadata)?
        .manifest()
        .identity()
        .topology_generation
        .get();
    let epochs = match epochs {
        Some(n) => n,
        None if first_topology.checked_sub(1) == Some(topology.get()) => 0,
        None => return Err(Error::InvalidMetadata),
    };
    let counts = tx
        .query_one(
            COUNTS,
            &[&d, &c, &revision.to_string(), &topology.get().to_string()],
        )
        .await
        .map_err(backend)?;
    let bounds_count = usize::try_from(counts.try_get::<_, i64>(0).map_err(backend)?)
        .map_err(|_| Error::InvalidMetadata)?;
    let attempts_count = usize::try_from(counts.try_get::<_, i64>(1).map_err(backend)?)
        .map_err(|_| Error::InvalidMetadata)?;
    if bounds_count > MAX_RANGES || attempts_count > MAX_BATCHES {
        return Err(Error::ResourceLimit);
    }
    let streams_count = usize::try_from(counts.try_get::<_, i64>(3).map_err(backend)?)
        .map_err(|_| Error::InvalidMetadata)?;
    if bounds_count as u64 != epochs
        || streams_count != bounds_count
        || !counts.try_get::<_, bool>(2).map_err(backend)?
    {
        return Err(Error::InvalidMetadata);
    }
    drop(counts);
    let mut bounds = Vec::new();
    bounds
        .try_reserve_exact(bounds_count)
        .map_err(|_| Error::ResourceLimit)?;
    for index in 0..bounds_count {
        let row = tx
            .query_one(
                BOUND,
                &[
                    &d,
                    &c,
                    &revision.to_string(),
                    &topology.get().to_string(),
                    &(index as i64),
                ],
            )
            .await
            .map_err(backend)?;
        let entry = lineage
            .entry(index as u64 + 1)
            .ok_or(Error::InvalidMetadata)?;
        let value = CutBound {
            shard_id: u32::try_from(row.try_get::<_, i64>(0).map_err(backend)?)
                .map_err(|_| Error::InvalidMetadata)?,
            owner_epoch: positive(&row, 1)?,
            complete_through: number(&row, 2)?,
        };
        let attestation = row
            .try_get::<_, Option<&[u8]>>(3)
            .map_err(backend)?
            .map(|bytes| {
                bytes
                    .try_into()
                    .map(Digest256::from_bytes)
                    .map_err(|_| Error::InvalidMetadata)
            })
            .transpose()?;
        let valid: bool = row.try_get(4).map_err(backend)?;
        if !valid
            || value.stream() != (entry.manifest().identity().shard_id, index as u64 + 1)
            || value.complete_through > entry.source_through()
            || (value.complete_through == 0) != attestation.is_none()
        {
            return Err(Error::InvalidMetadata);
        }
        bounds.push(Bound { value, attestation });
    }
    let mut attempts = Vec::new();
    attempts
        .try_reserve_exact(attempts_count)
        .map_err(|_| Error::ResourceLimit)?;
    for index in 0..attempts_count {
        let row = tx
            .query_one(
                ATTEMPT,
                &[
                    &d,
                    &c,
                    &revision.to_string(),
                    &topology.get().to_string(),
                    &b,
                    &(index as i64),
                ],
            )
            .await
            .map_err(backend)?;
        if row.try_get::<_, Option<bool>>(0).map_err(backend)? != Some(true) {
            return Err(Error::InvalidMetadata);
        }
        let attempt = SelectedAttempt {
            id: fixed(&row, 1)?,
            ordinal: positive(&row, 2)?,
            topology: TopologyGeneration::new(number(&row, 3)?),
            digest: Digest256::from_bytes(fixed(&row, 4)?),
            coordinator: ProcessIncarnation::try_from(fixed::<16>(&row, 5)?)
                .map_err(|_| Error::InvalidMetadata)?,
            worker: ProcessIncarnation::try_from(fixed::<16>(&row, 6)?)
                .map_err(|_| Error::InvalidMetadata)?,
        };
        if attempt.id == [0; 16]
            || attempt.ordinal > issued
            || attempts
                .last()
                .is_some_and(|previous: &SelectedAttempt| previous.ordinal >= attempt.ordinal)
        {
            return Err(Error::InvalidMetadata);
        }
        attempts.push(attempt);
    }
    Ok(Observation {
        dataset,
        catalog,
        binding,
        issued,
        current: Some(Current {
            receipt,
            topology,
            bounds,
            attempts,
        }),
    })
}

fn decode_current(
    row: &Row,
    catalog: OwnerResourceId,
) -> Result<(ArchiveFrontierReceipt, TopologyGeneration), Error> {
    if row.try_get::<_, Option<bool>>(0).map_err(backend)? != Some(true) {
        return Err(Error::InvalidMetadata);
    }
    let revision = positive(row, 1)?;
    let topology = TopologyGeneration::new(number(row, 2)?);
    let receipt = ArchiveFrontierReceipt {
        operation_id: fixed(row, 3)?,
        consumer_id: catalog,
        revision,
        snapshot_id: u64::try_from(row.try_get::<_, i64>(4).map_err(backend)?)
            .ok()
            .filter(|n| *n > 0)
            .ok_or(Error::InvalidMetadata)?,
        request_digest: Digest256::from_bytes(fixed(row, 5)?),
        family_counts: [
            number(row, 6)?,
            number(row, 7)?,
            number(row, 8)?,
            number(row, 9)?,
            number(row, 10)?,
        ],
        created: false,
    };
    if receipt.operation_id == [0; 16] {
        return Err(Error::InvalidMetadata);
    }
    Ok((receipt, topology))
}

fn require_absent(contradictory: bool) -> Result<(), Error> {
    if contradictory {
        Err(Error::InvalidMetadata)
    } else {
        Ok(())
    }
}
fn number(row: &Row, index: usize) -> Result<u64, Error> {
    let text: &str = row.try_get(index).map_err(|_| Error::InvalidMetadata)?;
    super::super::ingest_registry::decimal(text).map_err(|_| Error::InvalidMetadata)
}
fn positive(row: &Row, index: usize) -> Result<u64, Error> {
    number(row, index).and_then(|n| {
        if n == 0 {
            Err(Error::InvalidMetadata)
        } else {
            Ok(n)
        }
    })
}
fn fixed<const N: usize>(row: &Row, index: usize) -> Result<[u8; N], Error> {
    row.try_get::<_, &[u8]>(index)
        .map_err(|_| Error::InvalidMetadata)?
        .try_into()
        .map_err(|_| Error::InvalidMetadata)
}

// Every SQL projection has fixed bytes/booleans or at most21 decimal characters.
// Collections are counted with a sentinel before allocation, then read one row
// at a time. Existing Control transport/policy documents keep their own grant.
const PREREQUISITES: &str = "SELECT
    EXISTS(SELECT 1 FROM control.archive_coordinators WHERE dataset_id=$1::bytea AND catalog_id=$2::bytea AND binding_sha256=$3::bytea AND protocol_version=1)
    AND EXISTS(SELECT 1 FROM control.archive_persistence WHERE dataset_id=$1::bytea AND catalog_id=$2::bytea AND coordinator_binding_sha256=$3::bytea AND protocol_version=1 AND protection_class='local-synced' AND baseline_snapshot=1)
    AND (SELECT count(*) FROM (SELECT 1 FROM control.archive_attempts WHERE dataset_id=$1::bytea LIMIT 129)b)=$4::text::numeric
    AND NOT EXISTS(SELECT 1 FROM control.archive_attempts WHERE dataset_id=$1::bytea AND NOT coalesce(catalog_id=$2::bytea AND ordinal BETWEEN 1 AND $4::text::numeric AND expected_predecessor=ordinal-1 AND schema_version=1 AND persistence_protocol=1 AND coordinator_binding_sha256=$3::bytea,false))
    AND NOT EXISTS(SELECT 1 FROM control.consumers WHERE dataset_id=$1::bytea AND consumer_role='archive' AND consumer_id<>$2::bytea)";
const CURRENT: &str = "SELECT
    c.dataset_id=$1::bytea AND c.consumer_role='archive' AND r.dataset_id=c.dataset_id AND r.consumer_role='archive' AND r.sealed
        AND r.topology_generation=c.current_topology_generation AND r.owner_resource_id=c.consumer_id
        AND f.dataset_id=c.dataset_id AND f.consumer_id=f.catalog_id AND f.topology_generation=r.topology_generation
        AND f.owner_epoch=r.owner_epoch AND f.owner_incarnation=r.owner_incarnation
        AND o.dataset_id=c.dataset_id AND o.role='archive' AND o.process_incarnation=f.owner_incarnation AND o.archive_frontier_protocol=1
        AND ri.dataset_id=c.dataset_id AND ri.role='archive'
        AND a.dataset_id=c.dataset_id AND a.owner_incarnation=f.owner_incarnation AND a.persistence_protocol=1
        AND a.coordinator_binding_sha256=p.coordinator_binding_sha256 AND a.issuer_incarnation=o.process_incarnation
        AND i.dataset_id=c.dataset_id AND i.role='control' AND i.control_fence_version=1
        AND ((r.frontier_revision=1 AND r.previous_revision IS NULL)
            OR (r.previous_revision=r.frontier_revision-1 AND EXISTS(SELECT 1 FROM control.consumer_frontier_revisions prior
                WHERE prior.consumer_id=r.consumer_id AND prior.frontier_revision=r.previous_revision
                AND prior.dataset_id=r.dataset_id AND prior.consumer_role='archive' AND prior.sealed)))
        AND NOT EXISTS(SELECT 1 FROM control.consumer_frontier_revisions later WHERE later.consumer_id=c.consumer_id AND later.frontier_revision>c.current_revision),
    left(c.current_revision::text,21),left(c.current_topology_generation::text,21),substring(f.operation_id FROM 1 FOR 17),f.snapshot_id,
    substring(f.request_digest FROM 1 FOR 33),left(f.dictionary_count::text,21),left(f.payload_count::text,21),left(f.eligibility_count::text,21),left(f.receive_count::text,21),left(f.provenance_count::text,21)
    FROM control.consumers c
    LEFT JOIN control.consumer_frontier_revisions r ON r.consumer_id=c.consumer_id AND r.frontier_revision=c.current_revision
    LEFT JOIN control.archive_frontier_snapshots f ON f.consumer_id=c.consumer_id AND f.frontier_revision=c.current_revision
    LEFT JOIN control.owner_epochs o ON o.resource_id=f.catalog_id AND o.epoch=f.owner_epoch
    LEFT JOIN control.role_instances ri ON ri.role_instance_id=o.role_instance_id
    LEFT JOIN control.archive_owner_sessions a ON a.catalog_id=f.catalog_id AND a.owner_epoch=f.owner_epoch
    LEFT JOIN control.archive_persistence p ON p.dataset_id=c.dataset_id AND p.catalog_id=f.catalog_id
    LEFT JOIN control.owner_epochs i ON i.resource_id=a.issuer_resource AND i.epoch=a.issuer_epoch AND i.process_incarnation=a.issuer_incarnation
    WHERE c.consumer_id=$2::bytea";
const ABSENT_CONTRADICTIONS: &str = "SELECT
    EXISTS(SELECT 1 FROM control.consumer_frontier_revisions WHERE consumer_id=$2::bytea OR (dataset_id=$1::bytea AND consumer_role='archive'))
    OR EXISTS(SELECT 1 FROM control.consumer_frontiers WHERE consumer_id=$2::bytea)
    OR EXISTS(SELECT 1 FROM control.archive_frontier_snapshots WHERE consumer_id=$2::bytea OR catalog_id=$2::bytea OR dataset_id=$1::bytea)
    OR EXISTS(SELECT 1 FROM control.archive_frontier_batches WHERE consumer_id=$2::bytea)
    OR EXISTS(SELECT 1 FROM control.archive_owner_sessions WHERE catalog_id=$2::bytea OR dataset_id=$1::bytea)
    OR EXISTS(SELECT 1 FROM control.owner_epochs WHERE resource_id=$2::bytea OR (dataset_id=$1::bytea AND role='archive'))";
const COUNTS: &str = "SELECT
    (SELECT count(*) FROM (SELECT 1 FROM control.consumer_frontiers WHERE consumer_id=$2::bytea AND frontier_revision=$3::text::numeric LIMIT 1025)b),
    (SELECT count(*) FROM (SELECT 1 FROM control.archive_frontier_batches WHERE consumer_id=$2::bytea AND frontier_revision=$3::text::numeric LIMIT 129)b),
    EXISTS(SELECT 1 FROM control.topology_generations WHERE dataset_id=$1::bytea AND generation=$4::text::numeric AND sealed),
    (SELECT count(*) FROM (SELECT 1 FROM control.topology_streams WHERE dataset_id=$1::bytea AND topology_generation=$4::text::numeric LIMIT 1025)b)";
const BOUND: &str = "SELECT shard_id::bigint,left(owner_epoch::text,21),left(last_complete_sequence::text,21),substring(attestation_digest FROM 1 FOR 33),
    dataset_id=$1::bytea AND topology_generation=$4::text::numeric AND
    ((last_complete_sequence=0 AND attestation_kind IS NULL AND attestation_digest IS NULL)
        OR (last_complete_sequence>0 AND attestation_kind='archive-batch' AND attestation_digest IS NOT NULL))
    FROM control.consumer_frontiers WHERE consumer_id=$2::bytea AND frontier_revision=$3::text::numeric
    ORDER BY shard_id,owner_epoch LIMIT 1 OFFSET $5::bigint";
const ATTEMPT: &str = "SELECT a.dataset_id=$1::bytea AND a.catalog_id=$2::bytea AND a.schema_version=1
    AND a.persistence_protocol=1 AND a.coordinator_binding_sha256=$5::bytea AND a.topology_generation<=$4::text::numeric
    AND a.expected_predecessor=a.ordinal-1 AND a.coordinator_incarnation<>a.worker_incarnation,
    substring(b.attempt_id FROM 1 FOR 17),left(a.ordinal::text,21),left(a.topology_generation::text,21),
    substring(a.batch_digest FROM 1 FOR 33),substring(a.coordinator_incarnation FROM 1 FOR 17),substring(a.worker_incarnation FROM 1 FOR 17)
    FROM control.archive_frontier_batches b LEFT JOIN control.archive_attempts a USING(attempt_id)
    WHERE b.consumer_id=$2::bytea AND b.frontier_revision=$3::text::numeric
    ORDER BY a.ordinal NULLS FIRST,b.attempt_id LIMIT 1 OFFSET $6::bigint";

// Worst simultaneous owned metadata: original bound/attempt vectors, reconstructed
// cut with all receipt/range vectors, and validation's borrowed range sort. The
// remaining64KiB covers fixed headers, one bounded SQL projection and serial
// native stamp scratch. Original input owners and Control transport are separate.
const _: () = assert!(
    MAX_RANGES
        * (size_of::<Bound>()
            + size_of::<CutBound>()
            + size_of::<ReceiptRange>()
            + size_of::<&ReceiptRange>())
        + MAX_BATCHES * (size_of::<SelectedAttempt>() + size_of::<ReceiptExpectation>())
        + size_of::<RetainedArchiveFrontier>()
        + size_of::<BoundRetainedArchiveFrontier>()
        + 64 * 1024
        < METADATA_BYTES as usize
);

#[cfg(test)]
#[path = "archive_retained_frontier_tests.rs"]
mod tests;

#[path = "archive_read_expectations.rs"]
mod read_expectations;
pub use read_expectations::{ArchiveReadExpectations, BoundArchiveReadExpectations};
