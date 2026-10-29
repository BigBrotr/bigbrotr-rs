//! Initial-only Index consumer. Physical work is pollable and independently
//! owned; no overall readiness, producer ACK or restart authority is implied.
pub(in crate::interfaces::serve) mod catch_up;
mod live;
mod prepare;
pub(super) mod readiness;
pub(super) mod source;
use super::{Result, required};
use crate::config::Resolved;
use bigbrotr::{
    runtime::{AcquireError, OwnedPermit, ResourceBudget, ResourceLimits, ResourceRequest},
    storage::{
        archive::LocalCoordinator,
        clickhouse::{
            adapter::{
                IndexAdapter, IndexEngineSlot, IndexOperation, IndexOperationId,
                IndexOperationOutcome, JoinedIndexOperation, PhysicalError, PhysicalLimits,
                RecoveryResult, required_recovery_metadata_bytes,
            },
            schema::IndexTables,
            verified::{VerifiedIndexPlan, VerifiedInstallation},
        },
        indexer::{
            custody::IndexDriverCustody,
            plan::{self, PlannedBatch},
            state::IndexState,
        },
        postgres::{
            IndexProvenance, IndexSession, PreparedIndexPlan,
            control_owner::{ControlSession, OwnerError},
            ingest_registry::{ControlOwner, InitialRegistration},
        },
        rocks::{
            manifest::{AllocationNamespaces, IngestManifest, ManifestIdentity},
            store::{IngestStore, PendingReadLimits},
        },
    },
};
use bigbrotr_model::{
    CompleteThrough, OwnerEpoch, Sequence, SourceCut, SourceRecordId, StreamBound, StreamId,
    codec::FactDecodeLimits,
};
use prepare::PreparedIndex;
use std::{
    cell::Cell,
    num::{NonZeroU64, NonZeroUsize},
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

const MIB: u64 = 1024 * 1024;
const METADATA_BYTES: u64 = MIB;
const MAX_SOURCE_RECORDS: u64 = 64;
const MAX_SOURCE_BYTES: u64 = MIB;
const MAX_WIRE_BYTES: u64 = 16 * MIB;
const MAX_ROWS: u64 = 20_000;
const WIRE_OVERHEAD: u64 = 256 * 1024 + 64 * 1024;
const QUERY_TIMEOUT: Duration = Duration::from_secs(5);
const OPERATION_TIMEOUT: Duration = Duration::from_secs(30);

// Retained preparation, fixed actor metadata, one source, one planner and one
// physical executor/result. The remote query uses its independent engine budget.
pub(super) const ITEMS: u64 = prepare::ITEMS + 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Profile {
    delay: Duration,
    source_records: usize,
    source_bytes: usize,
    rows: u64,
    wire_bytes: u64,
    query_memory: u64,
    provenance: IndexProvenance,
    // Compare the actual accepted ceilings too, even when both exceed our
    // smaller closed initial profile. Two documents must agree before effects.
    configured: [u64; 3],
}
impl Profile {
    fn source(self) -> PendingReadLimits {
        PendingReadLimits {
            max_records: self.source_records,
            max_encoded_bytes: self.source_bytes,
            fact: FactDecodeLimits::default(),
        }
    }
    fn physical(self) -> Result<PhysicalLimits> {
        PhysicalLimits::new(
            NonZeroUsize::new(MAX_WIRE_BYTES as usize).unwrap(),
            self.wire_bytes,
            self.rows,
            self.query_memory,
            QUERY_TIMEOUT,
        )
        .map_err(|_| failed("physical bounds invalid"))
    }
    fn required_application(self) -> Result<ResourceLimits> {
        let physical = self.physical()?;
        let source = self.source();
        // A read-only sweep replaces the source/planner pair; it never overlaps
        // new publication. Reserve whichever complete operation needs more.
        let work = source
            .required_data_bytes()
            .ok()
            .and_then(|n| n.checked_add(plan::required_data_bytes(source).ok()?))
            .ok_or_else(|| failed("combined Index reservation overflow"))?
            .max(required_recovery_metadata_bytes());
        let bytes = prepare::PREPARATION_BYTES
            .checked_add(METADATA_BYTES)
            .and_then(|n| n.checked_add(physical.required_local_data_bytes().ok()?))
            .and_then(|n| n.checked_add(work))
            .ok_or_else(|| failed("combined Index reservation overflow"))?;
        Ok(ResourceLimits::new(
            bytes,
            ITEMS,
            physical.required_local_native_bytes(),
            0,
        ))
    }

    /// Called once, before accepting this actor's first policy. Preparation and
    /// metadata are already charged, alongside Control and the other startup
    /// owners. Prove that one entire operation can coexist with those owners;
    /// comparing this actor alone with the envelope would miss permanent pressure.
    fn require_startup_capacity(self, application: &ResourceBudget) -> Result<()> {
        let needed = self.required_application()?;
        let request = ResourceRequest::data(
            needed.data_bytes() - prepare::PREPARATION_BYTES - METADATA_BYTES,
            needed.items() - prepare::ITEMS - 1,
            needed.native_bytes(),
        )
        .map_err(|_| failed("combined startup reservation invalid"))?;
        let complete = application
            .try_acquire(request)
            .map_err(|_| failed("Index cannot coexist with the retained startup owners"))?;
        drop(complete);
        Ok(())
    }
}

fn profile(root: &Resolved) -> std::result::Result<Profile, OwnerError> {
    let delay = Duration::from_micros(root.number("indexer.flush_interval"));
    let rows = root.number("indexer.batch_max_rows").min(MAX_ROWS);
    let bytes = root.number("indexer.batch_max_bytes").min(MAX_WIRE_BYTES);
    let memory = root.number("indexer.query_memory");
    let provenance = match root.text("sync.provenance") {
        Some("local_first_witness") => IndexProvenance::LocalFirstWitness,
        Some("off") => IndexProvenance::Off,
        _ => return Err(OwnerError::InvalidPolicy),
    };
    if !root.local("index")
        || !root.flag("indexer.enabled")
        || delay.is_zero()
        || rows == 0
        || bytes <= WIRE_OVERHEAD
        || memory == 0
        || root.number("indexer.insert_workers") == 0
    {
        return Err(OwnerError::InvalidPolicy);
    }
    Ok(Profile {
        delay,
        // One atomic source batch can feed several target families. The
        // accepted row ceiling bounds each physical insert, not the total
        // number of facts needed to plan that batch without splitting it.
        source_records: MAX_SOURCE_RECORDS as usize,
        source_bytes: bytes.min(MAX_SOURCE_BYTES) as usize,
        rows,
        wire_bytes: bytes,
        query_memory: memory,
        provenance,
        configured: [
            root.number("indexer.batch_max_rows"),
            root.number("indexer.batch_max_bytes"),
            root.number("indexer.insert_workers"),
        ],
    })
}

enum Active {
    Install(IndexOperation<VerifiedInstallation>),
    Write(IndexOperation<VerifiedIndexPlan>),
    ProbePlan(IndexOperation<VerifiedIndexPlan>),
    ProbeFinish(IndexOperation<RecoveryResult>),
}
enum Completed {
    Install(JoinedIndexOperation<std::result::Result<VerifiedInstallation, PhysicalError>>),
    Write(JoinedIndexOperation<std::result::Result<VerifiedIndexPlan, PhysicalError>>),
    ProbePlan(JoinedIndexOperation<std::result::Result<VerifiedIndexPlan, PhysicalError>>),
    // One fixed-size recovery proof under the retained metadata allowance;
    // keep the less frequent result from enlarging every actor completion.
    ProbeFinish(Box<JoinedIndexOperation<std::result::Result<RecoveryResult, PhysicalError>>>),
}
impl Active {
    fn identity(&self) -> IndexOperationId {
        match self {
            Self::Install(v) => v.identity(),
            Self::Write(v) => v.identity(),
            Self::ProbePlan(v) => v.identity(),
            Self::ProbeFinish(v) => v.identity(),
        }
    }
    fn poll(&mut self) -> Result<Option<Completed>> {
        match self {
            Self::Install(operation) => operation.try_join().map(|v| v.map(Completed::Install)),
            Self::Write(operation) => operation.try_join().map(|v| v.map(Completed::Write)),
            Self::ProbePlan(operation) => operation.try_join().map(|v| v.map(Completed::ProbePlan)),
            Self::ProbeFinish(operation) => operation
                .try_join()
                .map(|v| v.map(|joined| Completed::ProbeFinish(Box::new(joined)))),
        }
        .map_err(|_| failed("physical executor join failed"))
    }
    fn cancel(&self) {
        match self {
            Self::Install(v) => v.request_cancel(),
            Self::Write(v) => v.request_cancel(),
            Self::ProbePlan(v) => v.request_cancel(),
            Self::ProbeFinish(v) => v.request_cancel(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Tick {
    Busy,
    Idle,
    Installed,
    Published,
    WaitingForResources,
    RetryRetainedPlan,
    Parked,
}

pub(super) struct Index {
    // Declaration order is a shutdown guarantee: actual executor Drop/join
    // precedes every state, adapter, credential and resource owner below.
    active: Option<Active>,
    completed: Option<(IndexOperationId, Completed)>,
    readiness: readiness::Readiness,
    pending: Option<PreparedIndexPlan>,
    session: Option<IndexSession>,
    state: Option<IndexState>,
    writer: Option<IndexAdapter>,
    initializer: Option<IndexAdapter>,
    prepared: PreparedIndex,
    slot: Arc<IndexEngineSlot>,
    application: ResourceBudget,
    accepted: Option<Profile>,
    source_identity: Option<ManifestIdentity>,
    control: Option<ControlOwner>,
    cut: Option<SourceCut>,
    revision: u64,
    next_operation: u64,
    local_joined_consumed_through: u64,
    installation_started: bool,
    parked: bool,
    // Last: even the retained cut/plan metadata must drop before its allowance.
    _metadata: OwnedPermit,
}

fn failed(reason: &'static str) -> (u8, String) {
    (
        4,
        format!("initial Index refused; preserve retained state: {reason}"),
    )
}
fn admitted(budget: &ResourceBudget, bytes: u64, native: u64) -> Result<Option<OwnedPermit>> {
    match budget.try_acquire(
        ResourceRequest::data(bytes, 1, native).map_err(|_| failed("resource request invalid"))?,
    ) {
        Ok(grant) => Ok(Some(grant)),
        Err(AcquireError::Unavailable) => Ok(None),
        Err(_) => Err(failed("work cannot fit the declared process envelope")),
    }
}

impl Index {
    pub(super) fn prepare(
        root: &Resolved,
        application: &ResourceBudget,
        initialize: bool,
    ) -> Result<Self> {
        let metadata = admitted(application, METADATA_BYTES, 0)?
            .ok_or_else(|| failed("metadata reservation unavailable"))?;
        let prepared = prepare::prepare(root, application, initialize)?;
        Ok(Self {
            active: None,
            completed: None,
            readiness: readiness::Readiness::default(),
            pending: None,
            session: None,
            state: None,
            writer: None,
            initializer: None,
            prepared,
            slot: Arc::new(IndexEngineSlot::new()),
            application: application.clone(),
            _metadata: metadata,
            accepted: None,
            source_identity: None,
            control: None,
            cut: None,
            revision: 0,
            next_operation: 1,
            local_joined_consumed_through: 0,
            installation_started: false,
            parked: false,
        })
    }
    pub(super) fn accept(&mut self, effective: &Resolved) -> std::result::Result<(), OwnerError> {
        let next = profile(effective)?;
        if self.accepted.is_some_and(|old| old != next)
            || self
                .state
                .as_ref()
                .is_some_and(|state| state.binding().provenance() != next.provenance)
            || next.query_memory > self.prepared.engine().snapshot().limits().data_bytes()
        {
            return Err(OwnerError::InvalidPolicy);
        }
        // Refresh may overlap an actual Index operation. Its existing grants
        // must not be charged twice; unchanged accepted policy is frozen above.
        if self.accepted.is_none() {
            next.require_startup_capacity(&self.application)
                .map_err(|_| OwnerError::InvalidPolicy)?;
        }
        self.accepted = Some(next);
        Ok(())
    }
    pub(super) fn delay(&self) -> Duration {
        self.accepted
            .map_or(Duration::from_secs(1), |p| p.delay)
            .min(readiness::INTERVAL)
    }
    pub(super) fn phase(&self) -> &'static str {
        if self.parked {
            "parked"
        } else if self.active.is_some() {
            "physical-operation"
        } else if self.state.is_none() {
            "uninitialized"
        } else if self.session.is_none() {
            "installation"
        } else if self.pending.is_some() {
            "retained-plan"
        } else {
            "idle"
        }
    }
    pub(super) fn published_revision(&self) -> u64 {
        self.revision
    }
    pub(super) fn frontier(&self) -> Option<&SourceCut> {
        (self.revision != 0).then_some(self.cut.as_ref()).flatten()
    }
    pub(super) fn request_cancel(&self) {
        self.readiness.cancel();
        if let Some(active) = &self.active {
            active.cancel();
        }
    }
    pub(super) fn capacity_observation(&self) -> super::capacity::IndexObservation {
        let retained = self
            .active
            .as_ref()
            .map(|active| super::capacity::RetainedIndex {
                incarnation: active.identity().incarnation(),
                sequence: active.identity().sequence().get(),
                kind: match active {
                    Active::Install(_) => "installation",
                    Active::Write(_) => "publication",
                    Active::ProbePlan(_) => "readiness-plan",
                    Active::ProbeFinish(_) => "readiness-finish",
                },
            });
        super::capacity::IndexObservation {
            retained,
            local_joined_consumed_through: self.local_joined_consumed_through,
            remote_unknown: self.slot.unresolved_query().is_some(),
        }
    }
    /// Called only after an actual whole-work reservation failed. The oldest
    /// deferred role therefore retries released physical credit before any
    /// between-plan metadata is discarded. Never alter a live or unknown owner.
    pub(super) fn yield_idle_readiness_for_capacity(&mut self, refusal: super::admission::Refusal) {
        self.readiness.yield_for_capacity(
            refusal.domain == "application",
            self.active.is_some() || self.completed.is_some(),
            self.parked || self.slot.unresolved_query().is_some(),
        );
    }
    pub(super) fn readiness_observation(&self) -> readiness::Observation {
        self.readiness.observation(
            self.active.is_some(),
            self.parked,
            self.slot.unresolved_query().is_some(),
        )
    }

    /// Paths are deployment inputs only. All scientific policy comes from the
    /// effective accepted profile captured above, never this root's seed.
    pub(super) fn initialize(
        &mut self,
        root: &Resolved,
        store: &IngestStore,
        spec: &InitialRegistration,
        driver: &IndexDriverCustody,
    ) -> Result<()> {
        let profile = self
            .accepted
            .ok_or_else(|| failed("accepted policy missing"))?;
        if self.state.is_some()
            || self.installation_started
            || self.prepared.initializer().is_none()
        {
            return Err(failed(
                "fresh installation requires its explicit initializer",
            ));
        }
        let identity = *store.manifest().identity();
        validate_source(store.manifest(), spec)?;
        if self.prepared.state_root() != Path::new(required(root, "storage.index_root")?)
            || self.prepared.database() != required(root, "clickhouse.database")?
        {
            return Err(failed("deployment roots changed"));
        }
        for key in root
            .selected_storage_roots()
            .filter(|key| *key != "storage.index_root")
        {
            let other = Path::new(required(root, key)?)
                .canonicalize()
                .map_err(|_| failed("selected store placement unavailable"))?;
            LocalCoordinator::require_disjoint(self.prepared.state_root(), &other)
                .map_err(|_| failed("Index state overlaps a selected writable store"))?;
        }
        let fresh = || {
            super::super::fresh_process_incarnation()
                .map_err(|_| failed("fresh identity unavailable"))
        };
        let mut uuids = [[0; 16]; 10];
        for id in &mut uuids {
            *id = *fresh()?.as_bytes();
        }
        let tables = IndexTables::new(self.prepared.database().to_owned(), uuids)
            .map_err(|_| failed("table manifest invalid"))?;
        let state = IndexState::create(
            self.prepared.state_root(),
            spec.dataset_id,
            (*fresh()?.as_bytes())
                .try_into()
                .map_err(|_| failed("resource identity invalid"))?,
            (*fresh()?.as_bytes())
                .try_into()
                .map_err(|_| failed("generation identity invalid"))?,
            fresh()?,
            tables,
            profile.provenance,
        )
        .map_err(|_| failed("fresh Index custody refused"))?;
        driver
            .attach(state.custody())
            .map_err(|_| failed("Control driver custody attachment refused"))?;
        let tables = Arc::new(state.tables().clone());
        let limits = profile.physical()?;
        self.writer = Some(IndexAdapter::new(
            Arc::clone(&tables),
            Arc::clone(self.prepared.writer()),
            limits,
            Arc::clone(&self.slot),
        ));
        self.initializer = Some(IndexAdapter::new(
            tables,
            Arc::clone(self.prepared.initializer().expect("checked initializer")),
            limits,
            Arc::clone(&self.slot),
        ));
        self.cut = Some(zero_cut(identity)?);
        self.source_identity = Some(identity);
        self.control = Some(spec.control);
        self.state = Some(state);
        Ok(())
    }

    /// Root refreshes Control/Ingest first, including during independent Archive
    /// waits. The actual PG renewal never revives an expired Index owner.
    pub(super) async fn renew(
        &mut self,
        owner: &mut ControlSession,
        commit_started: &Cell<bool>,
    ) -> Result<()> {
        if self
            .control
            .is_some_and(|expected| expected != owner.identity())
        {
            return Err(failed("Control identity changed"));
        }
        if let Some(session) = &self.session {
            owner
                .renew_index(
                    session,
                    self.state.as_ref().expect("session state").custody(),
                    commit_started,
                )
                .await
                .map_err(|_| failed("Index renewal failed"))?;
        }
        Ok(())
    }

    /// Caller checks its actual policy lease and bounds this serial PG work by
    /// deadline. This method never waits for CH: it polls the real OS join and
    /// otherwise returns, leaving the parent free to drive Control and Archive.
    pub(super) async fn tick(
        &mut self,
        owner: &mut ControlSession,
        store: source::Source<'_>,
        deadline: Instant,
        cancelled: &AtomicBool,
        allow_dispatch: bool,
    ) -> Result<Tick> {
        if cancelled.load(Ordering::Acquire) {
            self.readiness.cancel();
        }
        live_window(deadline, cancelled)?;
        if self.parked {
            return Ok(Tick::Parked);
        }
        if self.state.is_none()
            || self.source_identity.as_ref() != Some(store.manifest().identity())
        {
            return Err(failed("initial source/state identity missing or changed"));
        }
        let now = self.readiness.sample()?;
        if self.readiness.expired(now) {
            self.readiness.fail(readiness::Failure::Expired);
            if let Some(active) = &self.active {
                // Request cancellation, then retain every owner until actual
                // join. A surviving remote query parks the existing slot.
                active.cancel();
            } else {
                self.readiness.abandon();
            }
        }
        if let Some(active) = &mut self.active {
            let identity = active.identity();
            let Some(completed) = active.poll()? else {
                return Ok(Tick::Busy);
            };
            self.active = None;
            self.completed = Some((identity, completed));
        }
        if let Some((operation, completed)) = self.completed.take() {
            let tick = self
                .complete(owner, &store, completed, deadline, cancelled)
                .await?;
            // complete() has consumed the joined result and its local permit.
            // This marker says nothing about a surviving named remote query.
            if self.control.map(|owner| owner.process_incarnation) != Some(operation.incarnation())
            {
                return Err(failed(
                    "completed operation belongs to another Control incarnation",
                ));
            }
            self.local_joined_consumed_through = operation.sequence().get();
            return Ok(tick);
        }
        if !allow_dispatch {
            // Keep the verifier until the deferred role actually retries its
            // complete reservation. Returning local work credit may suffice.
            return Ok(Tick::WaitingForResources);
        }
        // An existing session is positively renewed before every new dispatch.
        // Root also invokes renew after its periodic Control/Ingest refresh.
        self.renew(owner, &Cell::new(false)).await?;
        live_window(deadline, cancelled)?;
        let now = self.readiness.sample()?;
        // An issued operation was polled/joined above before applying the finite
        // history boundary. Sync admission remains independently owned.
        if !store.within_history_limit()? {
            self.readiness.fail(readiness::Failure::Capacity);
            self.readiness.abandon();
            return Ok(Tick::Idle);
        }
        if let Some(session) = &self.session {
            store
                .require_current(owner, session, self.state.as_ref().unwrap(), cancelled)
                .await?;
            live_window(deadline, cancelled)?;
        }
        let probe_source = self.probe_source(&store, now, cancelled);
        live_window(deadline, cancelled)?;
        let profile = self.accepted.expect("initialized profile");
        let limits = profile.physical()?;
        let Some(local) = admitted(
            &self.application,
            limits
                .required_local_data_bytes()
                .map_err(|_| failed("local bounds overflow"))?,
            limits.required_local_native_bytes(),
        )?
        else {
            if probe_source.is_some() {
                self.readiness.fail(readiness::Failure::Capacity);
            }
            return Ok(Tick::WaitingForResources);
        };
        let Some(engine) = admitted(
            self.prepared.engine(),
            limits.required_engine_data_bytes(),
            0,
        )?
        else {
            if probe_source.is_some() {
                self.readiness.fail(readiness::Failure::Capacity);
            }
            return Ok(Tick::WaitingForResources);
        };
        if let Some(source) = probe_source {
            return self
                .probe(owner, source, local, engine, deadline, cancelled)
                .await;
        }
        if self.session.is_none() {
            if self.installation_started {
                self.parked = true;
                return Ok(Tick::Parked);
            }
            let operation = self.operation()?;
            self.installation_started = true;
            match self
                .initializer
                .as_ref()
                .expect("initialized adapter")
                .install_fresh(
                    self.state.as_ref().unwrap().custody(),
                    local,
                    engine,
                    operation,
                    Instant::now() + OPERATION_TIMEOUT,
                ) {
                Ok(active) => {
                    self.active = Some(Active::Install(active));
                    Ok(Tick::Busy)
                }
                Err(_) => {
                    self.parked = true;
                    Ok(Tick::Parked)
                }
            }
        } else {
            let source_limits = profile.source();
            let Some(source_grant) = admitted(
                &self.application,
                source_limits
                    .required_data_bytes()
                    .map_err(|_| failed("source bounds invalid"))?,
                0,
            )?
            else {
                return Ok(Tick::WaitingForResources);
            };
            let Some(planner_grant) = admitted(
                &self.application,
                plan::required_data_bytes(source_limits)
                    .map_err(|_| failed("planner bounds invalid"))?,
                0,
            )?
            else {
                return Ok(Tick::WaitingForResources);
            };
            let before = self.cut.as_ref().expect("initial cut");
            let first = store.next(before)?;
            let Some(pending) = store.read(first, source_limits, source_grant, cancelled)? else {
                if self.pending.is_some() {
                    return Err(failed("prepared plan lost its retained input"));
                }
                return Ok(Tick::Idle);
            };
            let planned = plan::plan_batch(
                *self.state.as_ref().unwrap().binding(),
                self.revision,
                before.clone(),
                pending,
                source_limits,
                planner_grant,
                cancelled,
            )
            .map_err(|_| failed("source planning refused"))?;
            check_plan_bounds(&planned, profile)?;
            if let Some(expected) = &self.pending {
                if expected.plan() != planned.plan() {
                    return Err(failed("retry changed its immutable prepared plan"));
                }
            } else {
                live_window(deadline, cancelled)?;
                let prepared = store
                    .prepare(
                        owner,
                        self.session.as_ref().unwrap(),
                        self.state.as_ref().unwrap(),
                        cancelled,
                        &planned,
                    )
                    .await?;
                self.pending = Some(prepared);
            }
            live_window(deadline, cancelled)?;
            let operation = self.operation()?;
            let result = self.writer.as_ref().unwrap().write_and_verify(
                self.state.as_ref().unwrap().custody(),
                self.pending.as_ref().unwrap(),
                planned,
                local,
                engine,
                operation,
                Instant::now() + OPERATION_TIMEOUT,
            );
            match result {
                Ok(active) => {
                    self.active = Some(Active::Write(active));
                    Ok(Tick::Busy)
                }
                Err(PhysicalError::Unavailable) if self.slot.unresolved_query().is_none() => {
                    self.readiness.fail(readiness::Failure::Dependency);
                    Ok(Tick::RetryRetainedPlan)
                }
                Err(_) => {
                    self.parked = true;
                    Ok(Tick::Parked)
                }
            }
        }
    }

    fn operation(&mut self) -> Result<IndexOperationId> {
        let sequence = NonZeroU64::new(self.next_operation)
            .ok_or_else(|| failed("operation identity exhausted"))?;
        self.next_operation = self
            .next_operation
            .checked_add(1)
            .ok_or_else(|| failed("operation identity exhausted"))?;
        Ok(IndexOperationId::new(
            self.control
                .ok_or_else(|| failed("Control identity missing"))?
                .process_incarnation,
            sequence,
        ))
    }

    async fn complete(
        &mut self,
        owner: &mut ControlSession,
        store: &source::Source<'_>,
        completed: Completed,
        deadline: Instant,
        cancelled: &AtomicBool,
    ) -> Result<Tick> {
        live_window(deadline, cancelled)?;
        if self.control != Some(owner.identity()) {
            return Err(failed("Control identity changed"));
        }
        if self.slot.unresolved_query().is_some() {
            self.parked = true;
            self.readiness.fail(readiness::Failure::Dependency);
            self.readiness.abandon();
            return Ok(Tick::Parked);
        }
        if !store.within_history_limit()? {
            self.readiness.fail(readiness::Failure::Capacity);
        }
        let state = self.state.as_ref().expect("active state");
        match completed {
            Completed::ProbePlan(joined) => self.complete_probe_plan(joined),
            Completed::ProbeFinish(joined) => {
                self.complete_probe_finish(owner, store, *joined, deadline, cancelled)
                    .await
            }
            Completed::Install(joined) => {
                // Borrow from the joined owner through the entire SQL future.
                // Its own field order retains custody/credit through result
                // destruction on success, error and cancellation alike.
                let result = match joined.outcome() {
                    IndexOperationOutcome::Completed(Ok(proof)) => {
                        let session = owner
                            .claim_initial_index(
                                state.binding(),
                                state.custody(),
                                proof,
                                &Cell::new(false),
                            )
                            .await
                            .map_err(|_| failed("initial PG claim failed"))?;
                        self.session = Some(session);
                        Tick::Installed
                    }
                    _ => {
                        self.parked = true;
                        Tick::Parked
                    }
                };
                drop(joined);
                Ok(result)
            }
            Completed::Write(joined) => {
                let result = match joined.outcome() {
                    IndexOperationOutcome::Completed(Ok(proof)) => {
                        let prepared = self
                            .pending
                            .as_ref()
                            .ok_or_else(|| failed("physical result has no prepared plan"))?;
                        let receipt = store
                            .publish(
                                owner,
                                self.session.as_ref().unwrap(),
                                state,
                                cancelled,
                                prepared,
                                proof,
                            )
                            .await?;
                        if receipt.binding != *state.binding()
                            || receipt.plan_digest != prepared.plan().digest()
                            || receipt.revision
                                != self
                                    .revision
                                    .checked_add(1)
                                    .ok_or_else(|| failed("frontier revision exhausted"))?
                            || receipt.cut != *prepared.plan().after()
                            || receipt.family_counts
                                != std::array::from_fn(|i| prepared.plan().chunks()[i].row_count())
                            || receipt.publisher != self.session.as_ref().unwrap().owner()
                        {
                            return Err(failed(
                                "publication receipt changed the expected frontier",
                            ));
                        }
                        self.revision = receipt.revision;
                        self.cut = Some(receipt.cut);
                        self.pending = None;
                        Tick::Published
                    }
                    IndexOperationOutcome::Completed(Err(PhysicalError::Unavailable))
                        if self.slot.unresolved_query().is_none() =>
                    {
                        self.readiness.fail(readiness::Failure::Dependency);
                        Tick::RetryRetainedPlan
                    }
                    _ => {
                        self.parked = true;
                        Tick::Parked
                    }
                };
                drop(joined);
                Ok(result)
            }
        }
    }
}

fn live_window(deadline: Instant, cancelled: &AtomicBool) -> Result<()> {
    if cancelled.load(Ordering::Acquire) || Instant::now() >= deadline {
        Err(failed("dispatch cancelled or expired"))
    } else {
        Ok(())
    }
}
fn validate_source(manifest: &IngestManifest, spec: &InitialRegistration) -> Result<()> {
    let identity = manifest.identity();
    if manifest.key_schema() != 1
        || identity.dataset_id != spec.dataset_id
        || identity.owner_resource_id != spec.ingest_resource_id
        || identity.shard_id != spec.shard_id
        || identity.owner_epoch.get() != 1
        || identity.process_incarnation != spec.process_incarnation
        || identity.policy_epoch.get() != spec.policy_revision
        || identity.topology_generation
            != spec
                .target_topology()
                .map_err(|_| failed("source topology invalid"))?
        || manifest.allocation_namespaces()
            != &AllocationNamespaces::from_entries(&spec.allocation_domains)
                .map_err(|_| failed("source allocation domains invalid"))?
    {
        return Err(failed("not the actual initial source lifetime"));
    }
    Ok(())
}
fn zero_cut(identity: ManifestIdentity) -> Result<SourceCut> {
    if identity.owner_epoch.get() != 1 {
        return Err(failed("not an initial source epoch"));
    }
    SourceCut::new(
        identity.dataset_id,
        identity.topology_generation,
        vec![StreamBound {
            stream: StreamId {
                shard_id: identity.shard_id,
                owner_epoch: OwnerEpoch::new(1).unwrap(),
            },
            last_complete_sequence: CompleteThrough::new(0),
        }],
    )
    .map_err(|_| failed("initial source cut invalid"))
}
fn next_source(identity: ManifestIdentity, cut: &SourceCut) -> Result<SourceRecordId> {
    let [bound] = cut.bounds() else {
        return Err(failed("initial cut is not one complete source stream"));
    };
    if cut.dataset_id() != identity.dataset_id
        || cut.topology_generation() != identity.topology_generation
        || bound.stream
            != (StreamId {
                shard_id: identity.shard_id,
                owner_epoch: identity.owner_epoch,
            })
    {
        return Err(failed("source cut changed identity"));
    }
    Ok(SourceRecordId {
        dataset_id: identity.dataset_id,
        shard_id: identity.shard_id,
        owner_epoch: identity.owner_epoch,
        sequence: Sequence::new(
            bound
                .last_complete_sequence
                .get()
                .checked_add(1)
                .ok_or_else(|| failed("source sequence exhausted"))?,
        )
        .map_err(|_| failed("source sequence exhausted"))?,
    })
}
fn check_plan_bounds(planned: &PlannedBatch, profile: Profile) -> Result<()> {
    let counts = std::array::from_fn(|i| planned.plan().chunks()[i].row_count());
    check_family_bounds(&counts, planned.wire_bytes(), profile)
}
fn check_family_bounds(counts: &[u64; 10], wire: &[u64; 10], profile: Profile) -> Result<()> {
    for (&count, bytes) in counts.iter().zip(wire) {
        if count > profile.rows
            || (count == 0 && *bytes != 0)
            || bytes
                .checked_add(WIRE_OVERHEAD)
                .is_none_or(|n| n > profile.wire_bytes)
        {
            return Err(failed(
                "whole batch exceeds the accepted initial Index profile",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "index/tests.rs"]
mod tests;
