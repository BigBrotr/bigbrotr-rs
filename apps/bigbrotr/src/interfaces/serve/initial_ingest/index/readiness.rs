//! Initial-profile physical observations, not serving or recovery authority.
//! A coalesced sweep reads the captured complete PG lineage without replay,
//! adoption or publication. One joined plan per turn bounds local work; the
//! whole sweep must finish within FRESHNESS from its oldest read. History size
//! still affects engine work. Expiry is unavailable, not a scalable-read claim.
use super::*;
use bigbrotr::{
    runtime::policy_authority::{AuthorityClock, AuthorityTick, SuspendAwareClock},
    storage::{
        clickhouse::adapter::{
            IndexRecoveryVerifier, RecoveryResult, required_recovery_metadata_bytes,
        },
        postgres::{IndexBinding, IndexOwnerIdentity, IndexRecoveryState},
        rocks::store::SourceHead,
    },
};
use bigbrotr_model::{ProcessIncarnation, ProjectionGenerationId};

pub const FRESHNESS: Duration = Duration::from_secs(30);
pub(super) const INTERVAL: Duration = Duration::from_secs(15);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Failure {
    Clock,
    Source,
    Changed,
    Expired,
    Dependency,
    Cancelled,
    Capacity,
}

/// Inert, fixed-size cache only. Its consumer separately checks authority,
/// elapsed time, actual current source and published cut before advertising ready.
#[derive(Debug, Clone, Copy)]
pub struct VerifiedObservation {
    pub source: SourceHead,
    pub revision: u64,
    pub generation: ProjectionGenerationId,
    pub storage_incarnation: ProcessIncarnation,
    pub started: AuthorityTick,
}
#[derive(Debug, Clone, Copy)]
pub struct Observation {
    pub verified: Option<VerifiedObservation>,
    pub failure: Option<Failure>,
    pub building: bool,
    pub active: bool,
    pub parked: bool,
    pub unresolved: bool,
    pub freshness: Duration,
}

struct Capture {
    source: SourceHead,
    binding: IndexBinding,
    owner: IndexOwnerIdentity,
    state: IndexRecoveryState,
    profile: Profile,
    started: AuthorityTick,
}

#[derive(Default)]
pub(super) struct Readiness {
    verifier: Option<IndexRecoveryVerifier>,
    capture: Option<Capture>,
    verified: Option<VerifiedObservation>,
    failure: Option<Failure>,
    last_attempt: Option<AuthorityTick>,
    last_tick: Option<AuthorityTick>,
    cancelled: Cell<bool>,
}
impl Readiness {
    pub(super) fn observation(&self, active: bool, parked: bool, unresolved: bool) -> Observation {
        let failure = if self.cancelled.get() {
            Some(Failure::Cancelled)
        } else {
            self.failure
        };
        Observation {
            verified: self.verified,
            failure,
            building: !parked
                && (self.capture.is_some() || (self.verified.is_none() && failure.is_none())),
            active,
            parked,
            unresolved,
            freshness: FRESHNESS,
        }
    }
    pub(super) fn cancel(&self) {
        self.cancelled.set(true);
    }
    pub(super) fn fail(&mut self, failure: Failure) {
        // Preserve the last observation as historical data, but never let a
        // later schema-only success refresh it or erase this failed observation.
        self.failure = Some(failure);
    }
    pub(super) fn sample(&mut self) -> Result<AuthorityTick> {
        let result = SuspendAwareClock.read().and_then(|now| {
            if let Some(last) = self.last_tick {
                now.elapsed_since(last)?;
            }
            Ok(now)
        });
        match result {
            Ok(now) => {
                self.last_tick = Some(now);
                Ok(now)
            }
            Err(_) => {
                self.fail(Failure::Clock);
                Err(failed("readiness clock unavailable or regressed"))
            }
        }
    }
    pub(super) fn running(&self) -> bool {
        self.capture.is_some()
    }
    pub(super) fn abandon(&mut self) {
        self.verifier = None;
        self.capture = None;
        // Do not reset last_attempt: new publication/input cannot force repeated
        // 1..N scans. Only the independent cooldown permits another sweep.
    }
    pub(super) fn expired(&self, now: AuthorityTick) -> bool {
        self.capture.as_ref().is_some_and(|capture| {
            now.elapsed_since(capture.started)
                .map_or(true, |elapsed| !within_window(elapsed))
        })
    }
    fn due(&self, now: AuthorityTick) -> bool {
        self.last_attempt
            .is_none_or(|last| now.elapsed_since(last).is_ok_and(probe_due))
    }
    fn remaining(&self, now: AuthorityTick) -> Result<Duration> {
        let started = self
            .capture
            .as_ref()
            .ok_or_else(|| failed("readiness capture absent"))?
            .started;
        let elapsed = now
            .elapsed_since(started)
            .map_err(|_| failed("readiness clock regressed"))?;
        if !within_window(elapsed) {
            return Err(failed("readiness sweep expired"));
        }
        Ok(FRESHNESS - elapsed)
    }
}

fn within_window(elapsed: Duration) -> bool {
    elapsed < FRESHNESS
}
fn probe_due(elapsed: Duration) -> bool {
    elapsed >= INTERVAL
}

fn source_matches(manifest: &IngestManifest, through: CompleteThrough, cut: &SourceCut) -> bool {
    let identity = manifest.identity();
    let [bound] = cut.bounds() else {
        return false;
    };
    manifest.key_schema() == 1
        && identity.owner_epoch.get() == 1
        && cut.dataset_id() == identity.dataset_id
        && cut.topology_generation() == identity.topology_generation
        && bound.stream
            == (StreamId {
                shard_id: identity.shard_id,
                owner_epoch: identity.owner_epoch,
            })
        && bound.last_complete_sequence == through
}

fn capture_matches(
    owner: IndexOwnerIdentity,
    state: IndexRecoveryState,
    expected_owner: IndexOwnerIdentity,
    revision: u64,
) -> bool {
    owner == expected_owner
        && state.published_revision == revision
        && state.pending_plan.is_none()
        && (revision == 0) == state.published_plan.is_none()
}

impl Index {
    /// Only clocks/native metadata here; HTTP never calls this or any CH owner.
    /// None means ordinary source work has priority (or observation failed).
    pub(super) fn probe_source(
        &mut self,
        store: &IngestStore,
        now: AuthorityTick,
        cancelled: &AtomicBool,
    ) -> Option<SourceHead> {
        if self.session.is_none() || self.pending.is_some() {
            return None;
        }
        let head = match store.observe_source_head(store.manifest(), cancelled) {
            Ok(head) => head,
            Err(_) => {
                self.readiness.fail(Failure::Source);
                self.readiness.abandon();
                return None;
            }
        };
        let caught_up = self
            .cut
            .as_ref()
            .is_some_and(|cut| source_matches(head.manifest(), head.through(), cut));
        if !caught_up {
            self.readiness.abandon();
            return None;
        }
        if self.readiness.running() || self.readiness.due(now) {
            Some(head)
        } else {
            None
        }
    }

    /// Called only after the ordinary renew/deadline checks and full local/engine
    /// reservation. No source/planner buffer is held concurrently with a sweep.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn probe(
        &mut self,
        control: &mut ControlSession,
        source: SourceHead,
        local: OwnedPermit,
        engine: OwnedPermit,
        deadline: Instant,
        cancelled: &AtomicBool,
    ) -> Result<Tick> {
        live_window(deadline, cancelled)?;
        let now = self.readiness.sample()?;
        if !self.readiness.running() {
            let Some(metadata) =
                admitted(&self.application, required_recovery_metadata_bytes(), 0)?
            else {
                self.readiness.fail(Failure::Capacity);
                return Ok(Tick::WaitingForResources);
            };
            self.readiness.last_attempt = Some(now);
            let state = self.state.as_ref().expect("installed Index state");
            let verifier = IndexRecoveryVerifier::begin(
                self.writer.as_ref().expect("installed adapter").clone(),
                control,
                state.custody(),
                metadata,
            )
            .await
            .map_err(|_| {
                self.readiness.fail(Failure::Dependency);
                failed("readiness PG capture refused")
            })?;
            live_window(deadline, cancelled)?;
            if !capture_matches(
                verifier.captured_owner(),
                verifier.captured_state(),
                self.session.as_ref().unwrap().owner(),
                self.revision,
            ) {
                self.readiness.fail(Failure::Changed);
                self.parked = true;
                return Ok(Tick::Parked);
            }
            self.readiness.capture = Some(Capture {
                source,
                binding: *state.binding(),
                owner: verifier.captured_owner(),
                state: verifier.captured_state(),
                profile: self.accepted.unwrap(),
                started: now,
            });
            self.readiness.verifier = Some(verifier);
        }
        let now = self.readiness.sample()?;
        if self.readiness.expired(now) {
            self.readiness.fail(Failure::Expired);
            self.readiness.abandon();
            return Ok(Tick::Idle);
        }
        let operation_deadline = Instant::now()
            .checked_add(self.readiness.remaining(now)?)
            .ok_or_else(|| failed("readiness deadline overflow"))?;
        let operation = self.operation()?;
        let state = self.state.as_ref().expect("installed state");
        let verifier = self.readiness.verifier.as_ref().expect("captured verifier");
        live_window(deadline, cancelled)?;
        let active = if verifier.is_complete() {
            let cut_matches = match verifier.verified_cut() {
                Some(cut) => source_matches(source.manifest(), source.through(), cut),
                None => self.revision == 0 && source.through().get() == 0,
            };
            if !cut_matches {
                self.readiness.fail(Failure::Changed);
                self.readiness.abandon();
                self.parked = true;
                return Ok(Tick::Parked);
            }
            let verifier = self.readiness.verifier.take().unwrap();
            verifier
                .finish(
                    control,
                    state.custody(),
                    local,
                    engine,
                    operation,
                    operation_deadline,
                )
                .await
                .map(Active::ProbeFinish)
        } else {
            self.readiness
                .verifier
                .as_mut()
                .unwrap()
                .next(
                    control,
                    state.custody(),
                    None,
                    local,
                    engine,
                    operation,
                    operation_deadline,
                )
                .await
                .map(Active::ProbePlan)
        };
        match active {
            Ok(active) => {
                self.active = Some(active);
                Ok(Tick::Busy)
            }
            Err(PhysicalError::Control) => {
                self.readiness.fail(Failure::Dependency);
                Err(failed("readiness PG read failed"))
            }
            Err(error) => {
                self.probe_failed(error);
                Ok(if self.parked {
                    Tick::Parked
                } else {
                    Tick::Idle
                })
            }
        }
    }

    fn probe_failed(&mut self, error: PhysicalError) {
        self.readiness.fail(Failure::Dependency);
        self.readiness.abandon();
        if error != PhysicalError::Unavailable || self.slot.unresolved_query().is_some() {
            self.parked = true;
        }
    }

    pub(super) fn complete_probe_plan(
        &mut self,
        joined: JoinedIndexOperation<std::result::Result<VerifiedIndexPlan, PhysicalError>>,
    ) -> Result<Tick> {
        let now = self.readiness.sample()?;
        if self.readiness.expired(now) || self.readiness.cancelled.get() {
            self.readiness.fail(if self.readiness.cancelled.get() {
                Failure::Cancelled
            } else {
                Failure::Expired
            });
            self.readiness.abandon();
            return Ok(Tick::Idle);
        }
        let (outcome, permit) = joined.into_parts();
        match outcome {
            IndexOperationOutcome::Completed(Ok(proof)) => {
                let accepted = self
                    .readiness
                    .verifier
                    .as_mut()
                    .ok_or_else(|| failed("readiness verifier absent"))?
                    .accept(proof);
                if let Err(error) = accepted {
                    self.probe_failed(error);
                }
            }
            IndexOperationOutcome::Completed(Err(error)) => self.probe_failed(error),
            _ => self.probe_failed(PhysicalError::RemoteUnresolved),
        }
        drop(permit); // Keep result allocation/custody charged through accept.
        Ok(if self.parked {
            Tick::Parked
        } else {
            Tick::Busy
        })
    }

    pub(super) async fn complete_probe_finish(
        &mut self,
        control: &mut ControlSession,
        store: &IngestStore,
        joined: JoinedIndexOperation<std::result::Result<RecoveryResult, PhysicalError>>,
        deadline: Instant,
        cancelled: &AtomicBool,
    ) -> Result<Tick> {
        let result = match joined.outcome() {
            IndexOperationOutcome::Completed(Ok(result)) => result,
            IndexOperationOutcome::Completed(Err(error)) => {
                self.probe_failed(*error);
                return Ok(if self.parked {
                    Tick::Parked
                } else {
                    Tick::Idle
                });
            }
            _ => {
                self.probe_failed(PhysicalError::RemoteUnresolved);
                return Ok(Tick::Parked);
            }
        };
        let now = self.readiness.sample()?;
        if self.readiness.expired(now) || self.readiness.cancelled.get() {
            self.readiness.fail(if self.readiness.cancelled.get() {
                Failure::Cancelled
            } else {
                Failure::Expired
            });
            self.readiness.abandon();
            return Ok(Tick::Idle);
        }
        // The joined result/real permit and its custody remain borrowed across
        // the final actual fenced SQL reads. No adoption/publication follows.
        self.renew(control, &Cell::new(false)).await?;
        live_window(deadline, cancelled)?;
        let state = self.state.as_ref().expect("installed state");
        state
            .custody()
            .check_live()
            .map_err(|_| failed("readiness custody changed"))?;
        let current = control
            .read_index_recovery_state(state.binding())
            .await
            .map_err(|_| failed("readiness final PG state unavailable"))?;
        live_window(deadline, cancelled)?;
        let source = match store.observe_source_head(store.manifest(), cancelled) {
            Ok(source) => source,
            Err(_) => {
                self.readiness.fail(Failure::Source);
                self.readiness.abandon();
                return Ok(Tick::Idle);
            }
        };
        let now = self.readiness.sample()?;
        live_window(deadline, cancelled)?;
        if self.readiness.expired(now) {
            self.readiness.fail(Failure::Expired);
            self.readiness.abandon();
            return Ok(Tick::Idle);
        }
        let capture = self
            .readiness
            .capture
            .as_ref()
            .ok_or_else(|| failed("readiness capture absent"))?;
        let proof = result.proof();
        if result.pending().is_some()
            || proof.binding() != &capture.binding
            || proof.previous_owner() != capture.owner
            || proof.state() != capture.state
            || state.binding() != &capture.binding
            || current != (capture.owner, capture.state)
            || self.session.as_ref().unwrap().owner() != capture.owner
            || self.revision != capture.state.published_revision
            || self.pending.is_some()
            || self.accepted != Some(capture.profile)
        {
            self.readiness.fail(Failure::Changed);
            self.readiness.abandon();
            self.parked = true;
            return Ok(Tick::Parked);
        }
        if source != capture.source
            || !self
                .cut
                .as_ref()
                .is_some_and(|cut| source_matches(source.manifest(), source.through(), cut))
        {
            // Ordinary admission may advance while the final read is in flight.
            // Its new data gets the next turn; this sweep cannot certify it.
            self.readiness.abandon();
            return Ok(Tick::Busy);
        }
        self.readiness.verified = Some(VerifiedObservation {
            source,
            revision: capture.state.published_revision,
            generation: capture.binding.generation(),
            storage_incarnation: capture.binding.storage_incarnation(),
            started: capture.started,
        });
        self.readiness.failure = None;
        self.readiness.abandon();
        drop(joined);
        Ok(Tick::Idle)
    }
}

#[cfg(test)]
#[path = "readiness/tests.rs"]
mod tests;
