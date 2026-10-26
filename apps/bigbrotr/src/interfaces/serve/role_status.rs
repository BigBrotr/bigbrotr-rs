//! Ordinary observations combine complete current Archive verification with a
//! completed exact-ID read/export. This cache grants no authority or retention.
use crate::interfaces::management;
use bigbrotr::{
    management::{
        roles::{
            CapabilityKind, CapabilityObservation, CapabilityState, Freshness,
            MAX_SNAPSHOT_STREAM_BOUNDS, ObservationTime, PublishedCut, RoleKind, RoleObservation,
            RoleSnapshot, RoleState, SnapshotIdentity, SourceObservation, StatusReason,
        },
        sessions::SessionError,
    },
    runtime::{
        OwnedPermit, ResourceBudget, ResourceRequest,
        policy_authority::{AuthorityClock, AuthorityTick, SuspendAwareClock},
    },
    storage::postgres::{
        control_owner::{ArchiveReadSelection, PolicySnapshot},
        ingest_registry::ControlOwner,
    },
};
use bigbrotr_model::{
    CompleteThrough, DatasetId, OwnerEpoch, OwnerResourceId, SourceCut, StreamBound, StreamId,
    TopologyGeneration,
};
use bigbrotr_protocol::{archive_cut::CutBound, archive_event::EventQuery};
use std::time::Duration;

const FRESH_FOR: Duration = Duration::from_secs(30);
// Three retained cuts, three candidates, latest current selection and one
// replacement allocation, each at most1024 bounds. No receipt/fact bodies live here.
// The request's64KiB grant separately owns any response snapshot clone.
const CACHE_BYTES: u64 = 256 * 1024;
const _: () = assert!(
    8 * MAX_SNAPSHOT_STREAM_BOUNDS * std::mem::size_of::<StreamBound>()
        + std::mem::size_of::<Observations>()
        + 16 * 1024
        <= CACHE_BYTES as usize
);
#[derive(Clone, Copy)]
pub(super) enum ReadCapability {
    History,
    Export,
}
struct Selection {
    catalog: OwnerResourceId,
    snapshot_id: u64,
    published: PublishedCut,
}
impl Selection {
    fn new(
        dataset: DatasetId,
        catalog: OwnerResourceId,
        revision: u64,
        snapshot_id: u64,
        topology: TopologyGeneration,
        input: &[CutBound],
    ) -> Result<Self, SessionError> {
        if snapshot_id == 0 || snapshot_id > i64::MAX as u64 {
            return Err(SessionError::InvalidMetadata);
        }
        if input.len() > MAX_SNAPSHOT_STREAM_BOUNDS {
            return Err(SessionError::Capacity);
        }
        let mut bounds = Vec::new();
        bounds
            .try_reserve_exact(input.len())
            .map_err(|_| SessionError::Capacity)?;
        for b in input {
            bounds.push(StreamBound {
                stream: StreamId {
                    shard_id: b.shard_id,
                    owner_epoch: OwnerEpoch::new(b.owner_epoch)
                        .map_err(|_| SessionError::InvalidMetadata)?,
                },
                last_complete_sequence: CompleteThrough::new(b.complete_through),
            });
        }
        Ok(Self {
            catalog,
            snapshot_id,
            published: PublishedCut {
                revision: revision
                    .to_string()
                    .try_into()
                    .map_err(|_| SessionError::InvalidMetadata)?,
                cut: SourceCut::new(dataset, topology, bounds)
                    .map_err(|_| SessionError::InvalidMetadata)?,
            },
        })
    }
    fn from_current(
        dataset: DatasetId,
        selected: &ArchiveReadSelection,
    ) -> Result<Self, SessionError> {
        if selected.dataset_id != dataset {
            return Err(SessionError::InvalidMetadata);
        }
        Self::new(
            dataset,
            selected.catalog_id,
            selected.frontier_revision,
            selected.snapshot_id,
            selected.topology_generation,
            &selected.bounds,
        )
    }
    fn matches(&self, other: &Self) -> bool {
        self.catalog == other.catalog
            && self.snapshot_id == other.snapshot_id
            && self.published.revision == other.published.revision
            && self.published.cut == other.published.cut
    }
}
struct Evidence {
    selection: Selection,
    started: AuthorityTick,
}
struct Attempt {
    started: AuthorityTick,
    candidate: Option<Selection>,
}
struct Slot {
    selected: bool,
    last: Option<Evidence>,
    attempt: Option<Attempt>,
    failure: Option<StatusReason>,
}
struct ProbeView<'a> {
    slot: &'a Slot,
    changed: bool,
    age: Option<Duration>,
}
impl Slot {
    fn new(selected: bool) -> Self {
        Self {
            selected,
            last: None,
            attempt: None,
            failure: None,
        }
    }
    fn observation(
        &self,
        capability: CapabilityKind,
        operation_age: Option<Duration>,
        probe: ProbeView<'_>,
        observed_at_us: i64,
    ) -> Result<CapabilityObservation, SessionError> {
        let matches = self
            .last
            .as_ref()
            .zip(probe.slot.last.as_ref())
            .is_some_and(|(operation, full)| operation.selection.matches(&full.selection));
        // Only a matching full observation contributes to the shown historical
        // operation cut. Ready uses the oldest of both required observations.
        let age = if matches {
            operation_age.zip(probe.age).map(|(a, b)| a.max(b))
        } else {
            operation_age
        };
        let (state, reason) = if !self.selected {
            (CapabilityState::Disabled, StatusReason::NotSelected)
        } else if let Some(reason) = self.failure.or(probe.slot.failure) {
            (CapabilityState::Unavailable, reason)
        } else if self.attempt.is_some() || probe.slot.attempt.is_some() {
            (
                CapabilityState::Building,
                StatusReason::VerificationInFlight,
            )
        } else if self.last.is_none() {
            (CapabilityState::Building, StatusReason::NotObserved)
        } else if !probe.changed && matches && age.is_some_and(|age| age < FRESH_FOR) {
            (CapabilityState::Ready, StatusReason::Current)
        } else {
            (CapabilityState::Stale, StatusReason::VerificationStale)
        };
        let freshness = match age {
            Some(age) if age < FRESH_FOR => Freshness::Fresh,
            Some(_) => Freshness::Stale,
            None => Freshness::Unknown,
        };
        // This UTC coordinate is the authenticated PG report timestamp. Only
        // the monotonic age measures how old the retained evidence is.
        let time = self
            .last
            .as_ref()
            .zip(age)
            .map(|(_, age)| ObservationTime::new(observed_at_us, age))
            .transpose()?;
        Ok(CapabilityObservation {
            capability,
            state,
            reason,
            freshness,
            time,
            published: self.last.as_ref().map(|e| PublishedCut {
                revision: e.selection.published.revision,
                cut: e.selection.published.cut.clone(),
            }),
            generation: None,
            storage_incarnation: None,
        })
    }
    fn begin(&mut self) {
        if !self.selected || self.attempt.is_some() {
            return;
        }
        // A repeated start cannot extend an in-flight observation's age. A
        // later retry cannot clear a previous failed observation.
        match SuspendAwareClock.read() {
            Ok(started) => self.begin_at(started),
            Err(_) => self.failure = Some(StatusReason::AuthorityUnavailable),
        }
    }
    fn begin_at(&mut self, started: AuthorityTick) {
        if self.selected && self.attempt.is_none() {
            self.attempt = Some(Attempt {
                started,
                candidate: None,
            });
        }
    }
    fn stage(&mut self, candidate: Result<Selection, SessionError>) {
        let Some(attempt) = self.attempt.as_mut() else {
            return;
        };
        attempt.candidate = None;
        match candidate {
            Ok(value) => attempt.candidate = Some(value),
            Err(SessionError::Capacity) => self.failure = Some(StatusReason::ResourceUnavailable),
            Err(_) => self.failure = Some(StatusReason::DependencyUnavailable),
        }
    }
    fn finish(&mut self, terminal: bool, failure: StatusReason) -> bool {
        let Some(attempt) = self.attempt.take() else {
            return false;
        };
        match attempt.candidate.filter(|_| terminal) {
            Some(selection) => {
                self.last = Some(Evidence {
                    selection,
                    started: attempt.started,
                });
                self.failure = None;
                true
            }
            None => {
                self.failure.get_or_insert(failure);
                false
            }
        }
    }
}

pub(super) struct Observations {
    dataset: DatasetId,
    history: Slot,
    export: Slot,
    probe: Slot,
    current: Option<Selection>,
    changed: bool,
    _permit: OwnedPermit,
}
impl Observations {
    pub(super) fn new(
        budget: &ResourceBudget,
        dataset: DatasetId,
        history: bool,
        export: bool,
    ) -> Result<Self, SessionError> {
        let permit = budget
            .try_acquire(ResourceRequest::data(CACHE_BYTES, 1, 0).expect("fixed observation cache"))
            .map_err(|_| SessionError::Capacity)?;
        Ok(Self {
            dataset,
            history: Slot::new(history),
            export: Slot::new(export),
            probe: Slot::new(history || export),
            current: None,
            changed: false,
            _permit: permit,
        })
    }
    fn slot(&mut self, kind: ReadCapability) -> &mut Slot {
        match kind {
            ReadCapability::History => &mut self.history,
            ReadCapability::Export => &mut self.export,
        }
    }
    pub(super) fn begin(&mut self, kind: ReadCapability) {
        self.slot(kind).begin();
    }
    /// Export records this clock sample before destination/scratch checks, but
    /// starts the cache attempt only after known positive job authorization.
    pub(super) fn begin_at(&mut self, kind: ReadCapability, started: AuthorityTick) {
        self.slot(kind).begin_at(started);
    }
    /// A known job authority/CAS denial is neither a successful physical probe
    /// nor a physical dependency failure. Keep all prior observations unchanged.
    pub(super) fn discard_attempt(&mut self, kind: ReadCapability) {
        self.slot(kind).attempt = None;
    }

    /// Stage only the query actually returned by the native reader and checked
    /// against its selected snapshot; final cleanup/join must still succeed.
    pub(super) fn observed(
        &mut self,
        kind: ReadCapability,
        query: &EventQuery,
        catalog: OwnerResourceId,
        revision: u64,
    ) {
        let dataset = self.dataset;
        let slot = self.slot(kind);
        if slot.attempt.is_none() {
            return;
        }
        let candidate = if query.dataset != dataset {
            Err(SessionError::InvalidMetadata)
        } else {
            Selection::new(
                dataset,
                catalog,
                revision,
                query.snapshot_id,
                query.topology_generation,
                &query.bounds,
            )
        };
        slot.stage(candidate);
    }
    /// A known unsuccessful attempt invalidates usability but keeps the old
    /// observed cut/time. Unknown outcomes fail-stop the owning supervisor.
    pub(super) fn finish(&mut self, kind: ReadCapability, terminal: bool) {
        self.slot(kind)
            .finish(terminal, StatusReason::ReadPathUnavailable);
    }

    /// Called before the probe's first I/O. HTTP only observes this state; it
    /// cannot start or advance a physical probe. The caller coalesces attempts.
    pub(super) fn begin_probe(&mut self) {
        self.probe.begin();
    }

    /// Only an independently loaded CURRENT selection belongs here, never a
    /// pinned export's historical selection. A changed head invalidates the old
    /// proof without claiming a physical failure. Returning to the old head
    /// cannot revive it; only a successful full probe clears `changed`.
    pub(super) fn note_current(&mut self, selected: &ArchiveReadSelection) {
        if !self.probe.selected {
            return;
        }
        match Selection::from_current(self.dataset, selected) {
            Ok(current) => {
                if self
                    .probe
                    .last
                    .as_ref()
                    .is_some_and(|full| !full.selection.matches(&current))
                {
                    self.changed = true;
                }
                self.current = Some(current);
            }
            Err(error) => {
                // Invalid current metadata cannot leave the old selection
                // eligible to accept a candidate already in flight.
                self.current = None;
                self.probe.failure = Some(if matches!(error, SessionError::Capacity) {
                    StatusReason::ResourceUnavailable
                } else {
                    StatusReason::DependencyUnavailable
                });
            }
        }
    }

    /// Inert candidate only. The caller has verified every selected receipt and
    /// physical row, then compared this exact selection to the current PG head.
    /// Cleanup and actual driver/worker joins still precede finish_probe(true).
    pub(super) fn observed_probe(&mut self, selected: &ArchiveReadSelection) {
        if self.probe.attempt.is_some() {
            self.probe
                .stage(Selection::from_current(self.dataset, selected));
        }
    }

    /// A known changed selection/auth refusal is not a physical failure. Call
    /// only after cleanup/join; preserve both the last proof and any failure.
    pub(super) fn discard_probe(&mut self) {
        self.probe.attempt = None;
    }

    pub(super) fn finish_probe(&mut self, terminal: bool) {
        if terminal
            && let Some(candidate) = self
                .probe
                .attempt
                .as_ref()
                .and_then(|attempt| attempt.candidate.as_ref())
            && self
                .current
                .as_ref()
                .is_none_or(|current| !candidate.matches(current))
        {
            // Even an otherwise successful old capture cannot certify the
            // independently observed current selection.
            self.changed = true;
            self.discard_probe();
            return;
        }
        if self
            .probe
            .finish(terminal, StatusReason::DependencyUnavailable)
        {
            self.changed = false;
        }
    }
    fn snapshot(
        &self,
        identity: SnapshotIdentity,
        observed_at_us: i64,
        now: AuthorityTick,
    ) -> Result<RoleSnapshot, SessionError> {
        if identity.dataset_id != self.dataset {
            return Err(SessionError::PolicyChanged);
        }
        // Validate the complete retained aggregate BEFORE cloning even one cut.
        let total = self
            .history
            .last
            .as_ref()
            .map_or(0, |e| e.selection.published.cut.bounds().len())
            .checked_add(
                self.export
                    .last
                    .as_ref()
                    .map_or(0, |e| e.selection.published.cut.bounds().len()),
            )
            .ok_or(SessionError::Capacity)?;
        if total > MAX_SNAPSHOT_STREAM_BOUNDS {
            return Err(SessionError::Capacity);
        }
        let age = |slot: &Slot| {
            slot.last
                .as_ref()
                .map(|e| {
                    now.elapsed_since(e.started)
                        .map_err(|_| SessionError::Unavailable)
                })
                .transpose()
        };
        let history_age = age(&self.history)?;
        let export_age = age(&self.export)?;
        let probe_age = age(&self.probe)?;
        let probe = || ProbeView {
            slot: &self.probe,
            changed: self.changed,
            age: probe_age,
        };
        let mut capabilities = Vec::with_capacity(2);
        if self.history.selected {
            capabilities.push(self.history.observation(
                CapabilityKind::PrivateHistory,
                history_age,
                probe(),
                observed_at_us,
            )?);
        }
        if self.export.selected {
            capabilities.push(self.export.observation(
                CapabilityKind::SignedEventExport,
                export_age,
                probe(),
                observed_at_us,
            )?);
        }
        RoleSnapshot::new(
            identity,
            observed_at_us,
            SourceObservation {
                cut: None,
                freshness: Freshness::Unknown,
                time: None,
                reason: StatusReason::NotSelected,
            },
            vec![RoleObservation {
                role: RoleKind::Control,
                state: RoleState::Ready,
                reason: StatusReason::Current,
            }],
            capabilities,
        )
    }
}

pub(super) fn complete(
    work: management::RoleStatusWork,
    owner: ControlOwner,
    dataset: DatasetId,
    policy: PolicySnapshot,
    observations: Option<&Observations>,
) -> management::Response {
    let snapshot = (|| {
        let identity = SnapshotIdentity {
            dataset_id: dataset,
            process_incarnation: owner.process_incarnation,
            config_revision: policy
                .config_revision
                .to_string()
                .try_into()
                .map_err(|_| SessionError::InvalidMetadata)?,
            policy_revision: policy
                .policy_revision
                .to_string()
                .try_into()
                .map_err(|_| SessionError::InvalidMetadata)?,
        };
        let now = SuspendAwareClock
            .read()
            .map_err(|_| SessionError::Unavailable)?;
        observations.ok_or(SessionError::Unavailable)?.snapshot(
            identity,
            work.observed_at_us(),
            now,
        )
    })();
    match snapshot {
        Ok(snapshot) => work.complete(snapshot),
        Err(error) => work.refuse(error),
    }
}

#[cfg(test)]
#[path = "role_status_tests.rs"]
mod tests;
