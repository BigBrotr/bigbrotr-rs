//! Ordinary Control observations at the existing exact-ID reader/export scope.
//! A successful selected read is not a complete current Archive readiness proof.
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
    storage::postgres::{control_owner::PolicySnapshot, ingest_registry::ControlOwner},
};
use bigbrotr_model::{CompleteThrough, DatasetId, OwnerEpoch, SourceCut, StreamBound, StreamId};
use bigbrotr_protocol::archive_event::EventQuery;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const FRESH_FOR: Duration = Duration::from_secs(30);
// Two retained cuts plus two in-flight replacements, each at most1024 bounds.
// The request's64KiB grant separately owns any response snapshot clone.
const CACHE_BYTES: u64 = 128 * 1024;
#[derive(Clone, Copy)]
pub(super) enum ReadCapability {
    History,
    Export,
}
struct Evidence {
    published: PublishedCut,
    started: AuthorityTick,
    observed_at_us: i64,
}
struct Attempt {
    started: AuthorityTick,
    observed_at_us: i64,
    candidate: Option<PublishedCut>,
}
struct Slot {
    selected: bool,
    last: Option<Evidence>,
    attempt: Option<Attempt>,
    failure: Option<StatusReason>,
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
        age: Option<Duration>,
    ) -> Result<CapabilityObservation, SessionError> {
        let (state, reason) = if !self.selected {
            (CapabilityState::Disabled, StatusReason::NotSelected)
        } else if let Some(reason) = self.failure {
            (CapabilityState::Unavailable, reason)
        } else if self.attempt.is_some() {
            (
                CapabilityState::Building,
                StatusReason::VerificationInFlight,
            )
        } else if self.last.is_some() {
            // Full current-selection/physical readiness is deliberately not
            // established by one exact-ID operation, even inside its30s TTL.
            (CapabilityState::Stale, StatusReason::VerificationStale)
        } else {
            (CapabilityState::Building, StatusReason::NotObserved)
        };
        let freshness = match age {
            Some(age) if age < FRESH_FOR => Freshness::Fresh,
            Some(_) => Freshness::Stale,
            None => Freshness::Unknown,
        };
        let time = self
            .last
            .as_ref()
            .zip(age)
            .map(|(e, age)| ObservationTime::new(e.observed_at_us, age))
            .transpose()?;
        Ok(CapabilityObservation {
            capability,
            state,
            reason,
            freshness,
            time,
            published: self.last.as_ref().map(|e| PublishedCut {
                revision: e.published.revision,
                cut: e.published.cut.clone(),
            }),
            generation: None,
            storage_incarnation: None,
        })
    }
}

pub(super) struct Observations {
    dataset: DatasetId,
    history: Slot,
    export: Slot,
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
            _permit: permit,
        })
    }
    fn slot(&mut self, kind: ReadCapability) -> &mut Slot {
        match kind {
            ReadCapability::History => &mut self.history,
            ReadCapability::Export => &mut self.export,
        }
    }
    pub(super) fn begin(&mut self, kind: ReadCapability, observed_at_us: Option<i64>) {
        let slot = self.slot(kind);
        if !slot.selected {
            return;
        }
        // Beginning a retry never clears a previous failed observation.
        slot.attempt = None;
        match SuspendAwareClock
            .read()
            .ok()
            .zip(observed_at_us.or_else(wall_time))
        {
            Some((started, observed_at_us)) => {
                slot.attempt = Some(Attempt {
                    started,
                    observed_at_us,
                    candidate: None,
                })
            }
            None => slot.failure = Some(StatusReason::AuthorityUnavailable),
        }
    }
    /// Stage only the query actually returned by the native reader and checked
    /// against its selected snapshot; final cleanup/join must still succeed.
    pub(super) fn observed(&mut self, kind: ReadCapability, query: &EventQuery, revision: u64) {
        let dataset = self.dataset;
        let slot = self.slot(kind);
        let Some(attempt) = slot.attempt.as_mut() else {
            return;
        };
        attempt.candidate = None;
        let candidate = (|| {
            if query.dataset != dataset {
                return Err(SessionError::InvalidMetadata);
            }
            if query.bounds.len() > MAX_SNAPSHOT_STREAM_BOUNDS {
                return Err(SessionError::Capacity);
            }
            let mut bounds = Vec::new();
            bounds
                .try_reserve_exact(query.bounds.len())
                .map_err(|_| SessionError::Capacity)?;
            for b in &query.bounds {
                bounds.push(StreamBound {
                    stream: StreamId {
                        shard_id: b.shard_id,
                        owner_epoch: OwnerEpoch::new(b.owner_epoch)
                            .map_err(|_| SessionError::InvalidMetadata)?,
                    },
                    last_complete_sequence: CompleteThrough::new(b.complete_through),
                });
            }
            Ok(PublishedCut {
                revision: revision
                    .to_string()
                    .try_into()
                    .map_err(|_| SessionError::InvalidMetadata)?,
                cut: SourceCut::new(dataset, query.topology_generation, bounds)
                    .map_err(|_| SessionError::InvalidMetadata)?,
            })
        })();
        match candidate {
            Ok(value) => attempt.candidate = Some(value),
            Err(SessionError::Capacity) => slot.failure = Some(StatusReason::ResourceUnavailable),
            Err(_) => slot.failure = Some(StatusReason::DependencyUnavailable),
        }
    }
    /// A known unsuccessful attempt invalidates usability but keeps the old
    /// observed cut/time. Unknown outcomes fail-stop the owning supervisor.
    pub(super) fn finish(&mut self, kind: ReadCapability, terminal: bool) {
        let slot = self.slot(kind);
        let Some(attempt) = slot.attempt.take() else {
            return;
        };
        match attempt.candidate.filter(|_| terminal) {
            Some(published) => {
                slot.last = Some(Evidence {
                    published,
                    started: attempt.started,
                    observed_at_us: attempt.observed_at_us,
                });
                slot.failure = None;
            }
            None => {
                slot.failure
                    .get_or_insert(StatusReason::ReadPathUnavailable);
            }
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
            .map_or(0, |e| e.published.cut.bounds().len())
            .checked_add(
                self.export
                    .last
                    .as_ref()
                    .map_or(0, |e| e.published.cut.bounds().len()),
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
        let mut capabilities = Vec::with_capacity(2);
        if self.history.selected {
            capabilities.push(
                self.history
                    .observation(CapabilityKind::PrivateHistory, history_age)?,
            );
        }
        if self.export.selected {
            capabilities.push(
                self.export
                    .observation(CapabilityKind::SignedEventExport, export_age)?,
            );
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
fn wall_time() -> Option<i64> {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(value) => i64::try_from(value.as_micros()).ok(),
        Err(error) => i64::try_from(error.duration().as_micros())
            .ok()?
            .checked_neg(),
    }
}

#[cfg(test)]
#[path = "role_status_tests.rs"]
mod tests;
