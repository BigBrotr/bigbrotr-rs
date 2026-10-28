//! The live native owner consumes the existing private command queue serially.
//! HTTP and password tasks run on the parent runtime; neither borrows native state.
use super::owner::RuntimeContext;
use super::*;
use bigbrotr::runtime::policy_authority::{LeaseError, PolicyLeaseTracker, PolicyLeaseTtl};
use bigbrotr::storage::postgres::control_owner::PolicySnapshot;
use std::time::Duration;

pub(super) struct CommandOwner<'a> {
    pub(super) owner: &'a mut ControlSession,
    context: RuntimeContext<'a>,
    dataset: DatasetId,
    policy: PolicySnapshot,
    profile: Profile,
    pub(super) root: &'a Resolved,
    progress: &'a watch::Sender<Progress>,
    pub(super) cancellation: &'a Cancellation,
    lease: PolicyLeaseTracker,
    administration: Option<bool>,
    public_routes: bool,
    sync: Option<&'a mut sync::Sync>,
    index: Option<&'a mut index::Index>,
    archive_selected: bool,
    source_observation: readiness::Source,
    archive_observation: readiness::ArchiveStatus,
    // One already-selected request retains its existing grant while awaiting
    // renewal or the end of independent Archive work. Never a second queue.
    pending: Option<management::Work>,
    pending_renewed: bool,
}

fn authority(error: LeaseError) -> (u8, String) {
    (4, error.to_string())
}

const DISPATCH_MARGIN: Duration = Duration::from_millis(100);

#[derive(Debug, PartialEq, Eq)]
enum Admission {
    Ready,
    Renew,
    Defer,
}

fn admission(now: Instant, due: Instant, deadline: Option<Instant>) -> Admission {
    if deadline.is_some_and(|end| end.saturating_duration_since(now) < DISPATCH_MARGIN) {
        Admission::Defer
    } else if due.saturating_duration_since(now) < DISPATCH_MARGIN {
        Admission::Renew
    } else {
        Admission::Ready
    }
}

async fn renewal_due(due: Instant) {
    // A newly created Sleep can remain Pending until the timer driver advances.
    // Retained commands are already Ready: near renewal, return Ready directly
    // so the biased renewal arm wins without moving the original due time.
    if admission(Instant::now(), due, None) != Admission::Renew {
        tokio::time::sleep_until(due).await;
    }
}

fn renewed_due(
    now: Instant,
    remaining: Duration,
    heartbeat: Duration,
    elapsed: Duration,
) -> Result<Instant> {
    let period = (remaining / 2).min(heartbeat.saturating_sub(elapsed));
    if period <= DISPATCH_MARGIN {
        return Err((4, "initial Ingest renewal left no margin beyond the 100ms dispatch window; preserve retained state".into()));
    }
    Ok(now + period)
}

/// Called only between completed operations. The finite Sync round keeps its
/// original input and grants while this future obtains fresh authority. A
/// started SQL/native operation never calls this helper to extend its deadline.
async fn before_sync_operation(
    due: Instant,
    wire_phase: bool,
    cancellation: &Cancellation,
    renew: impl Future<Output = Result<Instant>>,
) -> Result<Option<Instant>> {
    if cancellation.requested.load(Ordering::Acquire) {
        return Ok(None);
    }
    let due = if wire_phase || admission(Instant::now(), due, None) == Admission::Renew {
        renew.await?
    } else {
        due
    };
    if cancellation.requested.load(Ordering::Acquire) {
        return Ok(None);
    }
    if admission(Instant::now(), due, None) != Admission::Ready {
        return Err((
            4,
            "initial Sync renewal left no dispatch margin; preserve retained input".into(),
        ));
    }
    Ok(Some(due))
}

pub(super) async fn sync_operation<T>(
    trace: sync_diagnostic::Trace<'_>,
    deadline: Instant,
    cancelled: &AtomicBool,
    stage: sync_diagnostic::Stage,
    frame: Option<u8>,
    request: Option<u64>,
    future: impl Future<Output = T>,
) -> Result<T> {
    trace.enter(stage, frame, request);
    if cancelled.load(Ordering::Acquire) {
        return Err((
            4,
            "initial Sync cancelled before dispatch; preserve retained input".into(),
        ));
    }
    // A fresh timer may not yet have observed expiry. Never first-poll an
    // effect under an already expired phase, and never renew after polling.
    if Instant::now() >= deadline {
        return Err((4, "initial Sync deadline; preserve original outcome".into()));
    }
    tokio::time::timeout_at(deadline, trace.wait(stage, frame, request, future))
        .await
        .map_err(|_| (4, "initial Sync deadline; preserve original outcome".into()))
}

fn retained_command(
    pending: &mut Option<management::Work>,
    due: Instant,
    cancellation: &Cancellation,
) -> Option<management::Work> {
    if !cancellation.requested.load(Ordering::Acquire)
        && admission(Instant::now(), due, None) == Admission::Ready
    {
        pending.take()
    } else {
        None
    }
}

// Taking a retained item happens only when this future is actually polled in
// the management slot. No await separates take from Ready; losing biased arms
// leave it in its owner. Management::next is itself cancellation-safe.
async fn next_command(
    pending: &mut Option<management::Work>,
    management: &mut Option<management::Management>,
) -> management::Work {
    match pending.take() {
        Some(work) => work,
        None => super::super::next_management(management).await,
    }
}

async fn bounded_capped<T>(
    maximum: Duration,
    deadline: Option<Instant>,
    cancellation: &Cancellation,
    work: impl Future<Output = Result<T>>,
) -> Result<T> {
    if let Some(deadline) = deadline {
        // A Sleep can remain Pending until its timer driver catches up. Check
        // actual authority before the nested future gets its first poll.
        check_external_deadline(deadline, cancellation)?;
        tokio::select! {
            biased;
            _ = cancellation.wait() => Err((4, "initial Ingest cancelled; original attempt may need recovery".into())),
            _ = tokio::time::sleep_until(deadline) => Err((4, "initial Archive deadline; retain original outcome".into())),
            result = bounded(maximum, cancellation, work) => result,
        }
    } else {
        bounded(maximum, cancellation, work).await
    }
}

fn check_external_deadline(deadline: Instant, cancellation: &Cancellation) -> Result<()> {
    if cancellation.requested.load(Ordering::Acquire) {
        return Err((
            4,
            "initial Ingest cancelled; original attempt may need recovery".into(),
        ));
    }
    if Instant::now() >= deadline {
        return Err((
            4,
            "initial Archive deadline; retain original outcome".into(),
        ));
    }
    Ok(())
}

impl<'a> CommandOwner<'a> {
    /// Archive remains an initial-only consumer. Return the original context's
    /// reference, without borrowing the command owner across its later waits.
    pub(super) fn initial_registration(&self) -> Result<&'a InitialRegistration> {
        self.context.initial().map_err(owner_error)
    }

    pub(super) fn operation_window(&mut self, deadline: Instant) -> Result<Duration> {
        let maximum = self
            .maximum()?
            .min(deadline.saturating_duration_since(Instant::now()));
        phase(self.progress, maximum)?;
        Ok(maximum)
    }
    pub(super) fn maximum(&mut self) -> Result<std::time::Duration> {
        Ok(duration(self.root, "renewal_timeout")
            .min(self.lease.remaining_validity().map_err(authority)? / 2))
    }

    pub(super) async fn sync_window(
        &mut self,
        native: &NativeOwner,
        selected_sync: &mut sync::Sync,
        management: &mut Option<management::Management>,
        due: &mut Instant,
        wire_phase: bool,
    ) -> Result<Option<sync_diagnostic::Window>> {
        let cancellation = self.cancellation;
        let next = before_sync_operation(*due, wire_phase, cancellation, async {
            let started = self
                .renew_capped(Some(native), None, Some(selected_sync))
                .await?;
            self.next_renewal(started)
        })
        .await?;
        let Some(next) = next else { return Ok(None) };
        *due = next;
        if let Some(management) = management.as_ref() {
            management.renew_delivery(
                self.lease
                    .delivery_window(self.lease.known_epoch())
                    .map_err(authority)?,
            )?;
        }
        let selected = Instant::now();
        let renewal_timeout = duration(self.root, "renewal_timeout");
        let authority_remaining = self.lease.remaining_validity().map_err(authority)?;
        let authority_checked = Instant::now();
        let due_remaining = due.saturating_duration_since(authority_checked);
        let maximum = renewal_timeout
            .min(authority_remaining / 2)
            .min(due_remaining);
        if maximum < DISPATCH_MARGIN {
            return Err((
                4,
                "initial Sync authority left no dispatch margin; preserve retained input".into(),
            ));
        }
        let deadline = authority_checked + maximum;
        phase_capped(self.progress, maximum, Some(deadline))?;
        Ok(Some(sync_diagnostic::Window {
            selected,
            authority_checked,
            renewal_timeout,
            authority_remaining,
            due_remaining,
            maximum,
            renewal_due: *due,
            parent_deadline: self.progress.borrow().deadline,
            dispatch_deadline: deadline,
            outer_deadline: deadline,
        }))
    }

    fn next_renewal(&mut self, started: Instant) -> Result<Instant> {
        // Slow valid responses already consumed authority and heartbeat time.
        renewed_due(
            Instant::now(),
            self.lease.remaining_validity().map_err(authority)?,
            duration(self.root, "heartbeat_interval"),
            started.elapsed(),
        )
    }

    async fn renew(&mut self, deadline: Instant) -> Result<Instant> {
        // Archive callbacks remain strictly initial-only. They never borrow a
        // live writer or turn historical registration into current authority.
        if self.profile != Profile::Initial {
            return Err((4, "initial Archive cannot renew a successor".into()));
        }
        self.renew_capped(None, Some(deadline), None).await
    }

    async fn renew_for(&mut self, native: Option<&NativeOwner>) -> Result<Instant> {
        self.renew_capped(native, None, None).await
    }

    async fn renew_capped(
        &mut self,
        native: Option<&NativeOwner>,
        deadline: Option<Instant>,
        mut selected_sync: Option<&mut sync::Sync>,
    ) -> Result<Instant> {
        // Covers both biased timer paths, even if no management slot is reached
        // between renewals. A selected item may wait through one real renewal;
        // a second one fails closed instead of indefinitely deferring that item.
        if self.pending_renewed {
            return Err((4, "initial Ingest retained command could not be admitted after renewal; preserve retained state".into()));
        }
        if self.profile == Profile::Successor
            && !matches!(
                native,
                Some(NativeOwner::Live(_) | NativeOwner::Successor(_))
            )
        {
            return Err((4, "successor renewal requires its retained writer".into()));
        }
        let ttl = duration(self.root, "authorization_ttl");
        let maximum = if self.administration.is_some() {
            duration(self.root, "renewal_timeout")
                .min(self.lease.remaining_validity().map_err(authority)?)
        } else {
            duration(self.root, "renewal_timeout").min(ttl)
        };
        phase_capped(self.progress, maximum, deadline)?;
        let ticket = self.lease.begin_refresh().map_err(authority)?;
        let started = Instant::now();
        let mut administration = true;
        let mut public_routes = true;
        let snapshot = bounded_capped(maximum, deadline, self.cancellation, async {
            let snapshot = self
                .owner
                .refresh(|doc, digest| {
                    let restored = super::super::accepted_policy(self.root, doc, digest)?;
                    accepted_owner_profile(&restored, self.profile)?;
                    if let Some(sync) = selected_sync.as_deref_mut().or(self.sync.as_deref_mut()) {
                        sync.accept(&restored)?;
                    }
                    if self.archive_selected {
                        archive::validate(&restored).map_err(|_| OwnerError::InvalidPolicy)?;
                    }
                    if let Some(index) = self.index.as_deref_mut() {
                        index.accept(&restored)?;
                    }
                    administration &= restored.flag("administration.enabled");
                    public_routes &= restored.flag("network.public.enabled");
                    Ok(())
                })
                .await
                .map_err(owner_error)?;
            if snapshot != self.policy {
                return Err((4, "accepted policy changed; initial Ingest stopped".into()));
            }
            match native {
                Some(NativeOwner::Live(writer)) => {
                    self.owner
                        .renew_live_initial_ingest(
                            writer,
                            ttl,
                            |doc, digest| validate_owner(self.root, self.profile, doc, digest),
                            &Cell::new(false),
                        )
                        .await
                }
                Some(NativeOwner::Successor(writer)) => {
                    self.owner
                        .renew_live_successor_ingest(
                            writer,
                            ttl,
                            &self.cancellation.requested,
                            |doc, digest| validate_owner(self.root, self.profile, doc, digest),
                            &Cell::new(false),
                        )
                        .await
                }
                Some(NativeOwner::Candidate(_)) => {
                    return Err((4, "candidate cannot renew".into()));
                }
                Some(NativeOwner::Initial(_)) | None => {
                    self.owner
                        .renew_initial_ingest(
                            self.context.initial().map_err(owner_error)?,
                            ttl,
                            |doc, digest| validate(self.root, doc, digest),
                            &Cell::new(false),
                        )
                        .await
                }
            }
            .map_err(owner_error)?;
            if let Some(index) = self.index.as_deref_mut() {
                index.renew(self.owner, &Cell::new(false)).await?;
            }
            Ok(snapshot)
        })
        .await?;
        if self.administration.is_some_and(|old| old != administration) {
            return Err((
                4,
                "accepted listener profile changed; initial Ingest stopped".into(),
            ));
        }
        // Include both actual SQL responses in the original request-start window.
        self.lease
            .complete_refresh(
                &ticket,
                super::super::response(
                    self.dataset,
                    self.owner.identity().process_incarnation,
                    snapshot,
                    ttl,
                ),
            )
            .map_err(authority)?;
        self.administration = Some(administration);
        self.public_routes = public_routes;
        self.pending_renewed = self.pending.is_some();
        Ok(started)
    }

    fn idle_phase(&self, due: Instant) -> Result<()> {
        phase(
            self.progress,
            due.saturating_duration_since(Instant::now()) + duration(self.root, "renewal_timeout"),
        )
    }

    async fn command(
        &mut self,
        work: management::Work,
        management: &mut Option<management::Management>,
        due: Instant,
        deadline: Option<Instant>,
    ) -> Result<()> {
        assert!(self.pending.is_none(), "selected command already retained");
        self.pending = Some(work);
        // The select may have slept since its earlier admission check. No
        // command transaction starts until this fresh decision permits it.
        let now = Instant::now();
        match admission(now, due, deadline) {
            Admission::Defer => return Ok(()),
            Admission::Renew => return Ok(()),
            Admission::Ready => {}
        }
        let maximum = self
            .maximum()?
            .min(due.saturating_duration_since(Instant::now()));
        phase_capped(self.progress, maximum, deadline)?;
        let work = self.pending.take().expect("selected command retained");
        self.pending_renewed = false;
        let execution = bounded_capped(
            maximum,
            deadline,
            self.cancellation,
            management.as_mut().expect("command source").execute(
                work,
                self.owner,
                &mut self.lease,
                maximum,
                false,
                None,
            ),
        )
        .await?;
        let response = match execution {
            management::Execution::Complete(response) => response,
            management::Execution::RoleStatus(work) => {
                // Bind to the actual owner and accepted policy, independently
                // of the authorization metadata that complete() compares.
                let snapshot = (|| {
                    let identity = bigbrotr::management::roles::SnapshotIdentity {
                        dataset_id: self.dataset,
                        process_incarnation: self.owner.identity().process_incarnation,
                        config_revision: self.policy.config_revision.to_string().try_into()?,
                        policy_revision: self.policy.policy_revision.to_string().try_into()?,
                    };
                    readiness::snapshot(
                        identity,
                        work.observed_at_us(),
                        self.source_observation,
                        self.sync.is_some(),
                        self.archive_selected.then_some(self.archive_observation),
                        self.index.as_deref(),
                    )
                })();
                Some(match snapshot {
                    Ok(snapshot) => work.complete(snapshot),
                    Err(error) => work.refuse(error),
                })
            }
            _ => return Err((4, "unexpected operation on initial session surface".into())),
        };
        if !self.cancellation.requested.load(Ordering::Acquire) {
            self.lease
                .check_delivery(self.lease.known_epoch())
                .map_err(authority)?;
            if let Some(deadline) = deadline {
                phase_capped(
                    self.progress,
                    due.saturating_duration_since(Instant::now())
                        + duration(self.root, "renewal_timeout"),
                    Some(deadline),
                )?;
            } else {
                self.idle_phase(due)?;
            }
            if let Some(response) = response {
                response.deliver();
            }
        }
        Ok(())
    }

    /// Only independent catalog/native work belongs here. Original Control and
    /// Ingest renewal stays serialized, with its original due time. A ready
    /// result wins HTTP traffic; no renewal extends the external deadline.
    pub(super) async fn external<T>(
        &mut self,
        due: &mut Instant,
        management: &mut Option<management::Management>,
        deadline: Instant,
        work: impl Future<Output = Result<T>>,
    ) -> Result<T> {
        tokio::pin!(work);
        loop {
            check_external_deadline(deadline, self.cancellation)?;
            self.lease
                .check_delivery(self.lease.known_epoch())
                .map_err(authority)?;
            phase_capped(
                self.progress,
                (*due + duration(self.root, "renewal_timeout"))
                    .saturating_duration_since(Instant::now()),
                Some(deadline),
            )?;
            // Do not dequeue more HTTP near this independent operation's end.
            // A race-selected item stays retained if the postselect check defers.
            let commands = admission(Instant::now(), *due, Some(deadline)) != Admission::Defer;
            tokio::select! {
                biased;
                _ = self.cancellation.wait() => return Err((4, "initial Archive cancelled; retain original outcome".into())),
                _ = tokio::time::sleep_until(deadline) => return Err((4, "initial Archive deadline; retain original outcome".into())),
                _ = renewal_due(*due) => {
                    let started = self.renew(deadline).await?;
                    *due = self.next_renewal(started)?;
                    if let Some(management) = management.as_ref() {
                        management.renew_delivery(self.lease.delivery_window(self.lease.known_epoch()).map_err(authority)?)?;
                    }
                },
                result = &mut work => return result,
                work = next_command(&mut self.pending, management), if commands => {
                    self.command(work, management, *due, Some(deadline)).await?;
                },
            }
        }
    }

    /// A joined independent operation cannot turn late cancellation or expired
    /// Control authority into permission to continue the combined lifetime.
    pub(super) fn check_external_completion(&mut self, deadline: Instant) -> Result<()> {
        check_external_deadline(deadline, self.cancellation)?;
        self.lease
            .check_delivery(self.lease.known_epoch())
            .map_err(authority)
    }

    /// Dropping a blocking handle is not termination. Every failed renewal,
    /// command, signal or deadline still cancels and collects the same worker.
    pub(super) async fn blocking<T: Send + 'static>(
        &mut self,
        due: &mut Instant,
        management: &mut Option<management::Management>,
        deadline: Instant,
        work: impl FnOnce(&AtomicBool) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        self.lease
            .check_delivery(self.lease.known_epoch())
            .map_err(authority)?;
        if self.cancellation.requested.load(Ordering::Acquire) || Instant::now() >= deadline {
            return Err((4, "initial Archive admission cancelled or expired".into()));
        }
        let cancel = Arc::new(AtomicBool::new(false));
        let worker_cancel = cancel.clone();
        let mut worker = tokio::task::spawn_blocking(move || work(&worker_cancel));
        let outcome = self
            .external(due, management, deadline, async { Ok((&mut worker).await) })
            .await;
        cancel.store(true, Ordering::Release);
        match outcome {
            Ok(terminal) => terminal.map_err(|_| {
                (
                    4,
                    "initial Archive worker panicked; preserve retained state".into(),
                )
            })?,
            Err(error) => {
                if let Some(management) = management.as_ref() {
                    management.stop_admission();
                }
                if tokio::time::timeout(
                    duration(self.root, "graceful_shutdown_timeout"),
                    &mut worker,
                )
                .await
                .is_err()
                {
                    // The independent parent cannot release Control underneath
                    // an unjoined thread/native call. No cleanup is claimed.
                    std::process::exit(4);
                }
                Err(error)
            }
        }
    }
}

pub(super) async fn maintain(
    custody: &mut Custody,
    spec: &InitialRegistration,
    root: &Resolved,
    budget: &ResourceBudget,
    progress: &watch::Sender<Progress>,
    cancellation: &Cancellation,
    report: Value,
) -> Result<()> {
    let context = match custody.store.as_ref() {
        Some(NativeOwner::Initial(_)) => RuntimeContext::Initial(spec),
        Some(NativeOwner::Live(_)) => RuntimeContext::Live,
        _ => {
            return Err((
                4,
                "legacy lifecycle requires its original native owner".into(),
            ));
        }
    };
    maintain_context(
        custody,
        context,
        root,
        budget,
        progress,
        cancellation,
        report,
    )
    .await
}

pub(super) async fn maintain_successor(
    custody: &mut Custody,
    root: &Resolved,
    budget: &ResourceBudget,
    progress: &watch::Sender<Progress>,
    cancellation: &Cancellation,
    report: Value,
) -> Result<()> {
    if !matches!(custody.store.as_ref(), Some(NativeOwner::Successor(_))) {
        return Err((
            4,
            "successor lifecycle requires its continuously retained writer".into(),
        ));
    }
    maintain_context(
        custody,
        RuntimeContext::Live,
        root,
        budget,
        progress,
        cancellation,
        report,
    )
    .await
}

async fn maintain_context(
    custody: &mut Custody,
    context: RuntimeContext<'_>,
    root: &Resolved,
    budget: &ResourceBudget,
    progress: &watch::Sender<Progress>,
    cancellation: &Cancellation,
    mut report: Value,
) -> Result<()> {
    let Custody {
        index,
        store,
        sync,
        archive,
        owner,
    } = custody;
    let store = store.as_mut().expect("initialized native owner");
    context.require(store).map_err(owner_error)?;
    let profile = store.profile()?;
    let policy = store.policy(context)?;
    let dataset = store.dataset()?;
    if profile == Profile::Successor && archive.is_some() {
        return Err((3, "successor Archive is not available".into()));
    }
    if profile != Profile::Initial && index.is_some() {
        return Err((3, "Index requires its initial native lifetime".into()));
    }
    let lease = PolicyLeaseTracker::new(
        dataset,
        owner.identity().process_incarnation,
        PolicyEpoch::new(policy.policy_revision),
        PolicyLeaseTtl::new(duration(root, "authorization_ttl")).map_err(authority)?,
    )
    .map_err(authority)?;
    let mut state = CommandOwner {
        owner,
        context,
        dataset,
        policy,
        profile,
        root,
        progress,
        cancellation,
        lease,
        administration: None,
        public_routes: false,
        sync: sync.as_mut(),
        index: index.as_mut(),
        archive_selected: archive.is_some(),
        source_observation: readiness::Source::default(),
        archive_observation: readiness::ArchiveStatus::default(),
        pending: None,
        pending_renewed: false,
    };
    let started = state.renew_for(Some(store)).await?;
    state
        .source_observation
        .refresh(store, &cancellation.requested);
    // Measure the actual renewal, before independent startup work consumes time.
    // An overdue due time after startup is still handled by the renewal arm.
    let mut due = state.next_renewal(started)?;
    if state.sync.is_some() {
        let maximum = state.maximum()?;
        phase(progress, maximum)?;
        bounded(maximum, cancellation, async {
            state
                .owner
                .require_acquisition_execution()
                .await
                .map_err(|e| {
                    (
                        4,
                        format!("initial acquisition execution prerequisites unavailable: {e:?}"),
                    )
                })
        })
        .await?;
    }
    let mut management = if state.administration == Some(true) {
        let maximum = state.maximum()?;
        phase(progress, maximum)?;
        bounded(maximum, cancellation, async {
            state.owner.require_browser_sessions().await.map_err(|e| {
                (
                    4,
                    format!("initial session prerequisites unavailable: {e:?}"),
                )
            })?;
            state.owner.require_relay_enrollment().await.map_err(|e| {
                (
                    4,
                    format!("initial relay enrollment prerequisites unavailable: {e:?}"),
                )
            })?;
            state
                .owner
                .require_relay_registration()
                .await
                .map_err(|e| {
                    (
                        4,
                        format!("initial relay registration prerequisites unavailable: {e:?}"),
                    )
                })?;
            state.owner.require_acquisition_scope().await.map_err(|e| {
                (
                    4,
                    format!("initial acquisition scope prerequisites unavailable: {e:?}"),
                )
            })?;
            state.owner.require_acquisition_jobs().await.map_err(|e| {
                (
                    4,
                    format!("initial acquisition job prerequisites unavailable: {e:?}"),
                )
            })
        })
        .await?;
        phase(progress, maximum)?;
        let window = state
            .lease
            .delivery_window(state.lease.known_epoch())
            .map_err(authority)?;
        Some(
            management::Management::start(
                root,
                budget.clone(),
                window,
                None,
                management::Profile::Initial {
                    public_routes: state.public_routes,
                },
            )
            .await?,
        )
    } else {
        None
    };

    // No early return after binding bypasses actual HTTP/password termination.
    let outcome = async {
        progress.send_modify(|value| {
            value.shutdown = management
                .as_ref()
                .map(management::Management::shutdown_handle);
        });
        // Covers cancellation before publication; the parent checks the newest
        // handle after setting this flag to cover publication before cancellation.
        if cancellation.requested.load(Ordering::Acquire) {
            if let Some(management) = &management {
                management.stop_admission();
            }
            return Ok(());
        }
        report["listeners"] = if management.is_some() {
            json!([{
                "kind": "private-session-http",
                "host": root.text("administration.listen.host"),
                "port": root.number("administration.listen.port"),
                "profile": "initial-source-commands"
            }])
        } else {
            json!([])
        };
        let mut dispatch_due = Instant::now() + sync::INTERVAL;
        let mut index_due = Instant::now();
        let mut publish_due = Instant::now() + archive.as_ref().map_or(Duration::ZERO, archive::Archive::delay);
        let mut deferred = super::admission::Deferred::default();
        let mut capacity = super::capacity::Trace::new(dataset, state.owner.identity(), progress);
        state.idle_phase(due)?;
        progress.send_modify(|value| value.initialized = Some(report));
        loop {
            state
                .lease
                .check_delivery(state.lease.known_epoch())
                .map_err(authority)?;
            // A command already selected near a previous deadline owns its
            // queue slot. Consume it after renewal before beginning a Sync
            // round that may itself cross more than one safe-point renewal.
            if let Some(work) = retained_command(&mut state.pending, due, cancellation) {
                state.command(work, &mut management, due, None).await?;
                continue;
            }
            tokio::select! {
                biased;
                _ = cancellation.wait() => return Ok(()),
                _ = renewal_due(due) => {
                    let started = state.renew_for(Some(store)).await?;
                    due = state.next_renewal(started)?;
                    state.idle_phase(due)?;
                    if let Some(management) = &management {
                        let window = state.lease
                            .delivery_window(state.lease.known_epoch())
                            .map_err(authority)?;
                        management.renew_delivery(window)?;
                    }
                },
                _ = tokio::time::sleep_until(publish_due), if archive.as_ref().is_some_and(|archive| !archive.parked())
                    && deferred.allows(super::admission::Role::Archive) => {
                    // The timer may become ready only after select slept near
                    // renewal. No native/Sync effect precedes this recheck.
                    if admission(Instant::now(), due, None) == Admission::Renew {
                        continue;
                    }
                    let archive = archive.as_mut().expect("selected Archive");
                    deferred.check(Instant::now())?;
                    let reservation = match archive.reserve(root, budget)? {
                        super::admission::Admission::Granted(reservation) => reservation,
                        super::admission::Admission::Deferred(refusal) => {
                            let now = Instant::now();
                            if deferred.waiting(super::admission::Role::Archive, now) {
                                capacity.denied(super::admission::Role::Archive, refusal,
                                    state.index.as_deref().map(index::Index::capacity_observation).unwrap_or_default(), now)?;
                            }
                            // A failed due timer must not remain immediately
                            // selectable ahead of the Index join which frees it.
                            publish_due = now + sync::INTERVAL;
                            continue;
                        }
                    };
                    capacity.admitted(super::admission::Role::Archive, budget.snapshot(), Some(archive.native_snapshot()),
                        state.index.as_deref().map(index::Index::capacity_observation).unwrap_or_default(), Instant::now());
                    deferred.admitted(super::admission::Role::Archive);
                    let maximum = state.maximum()?.min(due.saturating_duration_since(Instant::now()));
                    phase(progress, maximum)?;
                    // A catalog failure can become known to its driver while
                    // joined native cleanup still services private commands.
                    // Old evidence must not remain Ready during that operation.
                    state.archive_observation.verifying = true;
                    let published = archive.publish(store.initial()?, reservation, &mut state, &mut due,
                        &mut management, &cancellation.requested).await?;
                    state.archive_observation = readiness::ArchiveStatus {
                        publication: archive.observation(),
                        verifying: false,
                        connection_refused: archive.parked(),
                    };
                    // Drain available complete batches without another formation
                    // delay, but allow management/renewal between each pass.
                    publish_due = Instant::now() + if published { sync::INTERVAL } else { archive.delay() };
                    state.idle_phase(due)?;
                },
                _ = tokio::time::sleep_until(dispatch_due), if state.sync.is_some()
                    && deferred.allows(super::admission::Role::Sync) => {
                    // The timer may become ready only after select slept near
                    // renewal. No native/Sync effect precedes this recheck.
                    if admission(Instant::now(), due, None) == Admission::Renew {
                        continue;
                    }
                    deferred.check(Instant::now())?;
                    let reservation = match super::admission::reserve(budget, sync::requirements()?, "application")? {
                        super::admission::Admission::Granted(reservation) => reservation,
                        super::admission::Admission::Deferred(refusal) => {
                            let now = Instant::now();
                            if deferred.waiting(super::admission::Role::Sync, now) {
                                capacity.denied(super::admission::Role::Sync, refusal,
                                    state.index.as_deref().map(index::Index::capacity_observation).unwrap_or_default(), now)?;
                            }
                            dispatch_due = now + sync::INTERVAL;
                            continue;
                        }
                    };
                    capacity.admitted(super::admission::Role::Sync, budget.snapshot(), None,
                        state.index.as_deref().map(index::Index::capacity_observation).unwrap_or_default(), Instant::now());
                    deferred.admitted(super::admission::Role::Sync);
                    // The round retains its bounded frames on this stack. Taking
                    // only this dispatcher reference lets safe points reuse the
                    // same renewal owner, including Sync's policy validator.
                    let selected_sync = state.sync.take().expect("selected Sync");
                    let trace = sync_diagnostic::Trace::new(progress);
                    let result = selected_sync.dispatch(sync::Dispatch {
                        commands: &mut state, writer: store, context, root, budget: reservation.budget(),
                        cancel: cancellation, management: &mut management, due: &mut due,
                        deadline: Instant::now(), trace,
                    }, &reservation).await;
                    state.sync = Some(selected_sync);
                    trace.finish(result)?;
                    state.source_observation.refresh(store, &cancellation.requested);
                    // A timed gap lets HTTP run even under continuous work;
                    // the priority timer prevents continuous HTTP starving Sync.
                    dispatch_due = Instant::now() + sync::INTERVAL;
                    state.idle_phase(due)?;
                },
                _ = tokio::time::sleep_until(index_due), if state.index.is_some() => {
                    if admission(Instant::now(), due, None) == Admission::Renew {
                        continue;
                    }
                    let maximum = state.maximum()?.min(due.saturating_duration_since(Instant::now()));
                    phase(progress, maximum)?;
                    let deadline = Instant::now() + maximum;
                    let index = state.index.as_deref_mut().expect("selected Index");
                    // Polling does not wait for ClickHouse. Serial PG work keeps
                    // the same deadline and is drained on a requested stop, as
                    // for Sync, before the parent joins the actual executor.
                    let tick = tokio::time::timeout_at(deadline, index.tick(
                        state.owner, store.initial()?, deadline.into_std(), &cancellation.requested,
                        !deferred.any(),
                    )).await.map_err(|_| (4, "initial Index deadline; preserve original outcome".into()))??;
                    index_due = Instant::now() + match tick {
                        index::Tick::Busy | index::Tick::Installed | index::Tick::Published => sync::INTERVAL,
                        _ => index.delay(),
                    };
                    state.idle_phase(due)?;
                },
                work = next_command(&mut state.pending, &mut management) => {
                    // The native owner remains serial and supervised. Nested
                    // Archive work uses this fixed cached observation and its
                    // age; it cannot borrow an Ingest handle concurrently.
                    if admission(Instant::now(), due, None) == Admission::Ready {
                        phase(progress, state.maximum()?.min(due.saturating_duration_since(Instant::now())))?;
                        state.source_observation.refresh(store, &cancellation.requested);
                    }
                    state.command(work, &mut management, due, None).await?;
                },
            }
        }
    }
    .await;
    if let Some(index) = state.index.as_deref() {
        index.request_cancel();
    }
    let grace = duration(root, "graceful_shutdown_timeout");
    // A late phase must not revive the watchdog, but still owes actual cleanup.
    let phase_result = phase(progress, grace);
    if let Some(management) = &mut management {
        management.stop(grace).await?;
    }
    outcome?;
    phase_result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn sync_safe_point_retains_prepared_input_and_grant_until_reauthorization() {
        use bigbrotr::storage::rocks::store::PreparedEvent;
        use bigbrotr_model::{
            ClockQuality, DurationUs, EventIntent, LocalObservation, ObservedTime, Ordinal,
            ProducerEpoch, ProducerRequestId, QualifiedRef, RequestSequence,
        };
        let root = crate::config::load(
            &Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/recipes/archive-only.toml"),
        )
        .unwrap();
        let budget = super::super::budget(&root, true, false).unwrap();
        let corpus: Value = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../experiments/identity-contract/signed-fixture.json"
        )))
        .unwrap();
        let signed: bigbrotr_model::SignedEvent =
            serde_json::from_value(corpus["events"]["E_public"].clone()).unwrap();
        let reference = |kind| QualifiedRef {
            dataset_id: [1; 16].try_into().unwrap(),
            dictionary_kind: kind,
            allocation_domain: [2; 16].try_into().unwrap(),
            ordinal: Ordinal::new(1).unwrap(),
        };
        let observer = reference(DictionaryKind::Observer);
        let observation = LocalObservation {
            received_at: ObservedTime::new(17),
            observer_id: observer,
            clock_quality: ClockQuality::new(observer, DurationUs::new(1), None, 0).unwrap(),
            witness: None,
        };
        let request = ProducerRequestId {
            producer_slot_id: [3; 16].try_into().unwrap(),
            producer_epoch: ProducerEpoch::new(1).unwrap(),
            request_sequence: RequestSequence::new(4).unwrap(),
        };
        let prepared = Box::pin(
            PreparedEvent::new(
                request,
                EventIntent::network(
                    1,
                    reference(DictionaryKind::VisibilityScope),
                    signed.clone(),
                    observation,
                )
                .unwrap(),
                256 * 1024,
                grant(&budget, 10 * MIB, 0).unwrap(),
            )
            .unwrap(),
        );
        let original = std::ptr::from_ref(&*prepared);
        let cancellation = Cancellation::new();
        let effects = Cell::new(0);
        let (release, wait) = tokio::sync::oneshot::channel();
        let mut resumed = std::pin::pin!(async {
            let next = before_sync_operation(
                Instant::now() + Duration::from_millis(38),
                false,
                &cancellation,
                async {
                    wait.await.unwrap();
                    Ok(Instant::now() + Duration::from_secs(5))
                },
            )
            .await?;
            assert!(next.is_some());
            effects.set(effects.get() + 1);
            Ok::<_, (u8, String)>(prepared)
        });
        assert!(
            resumed
                .as_mut()
                .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
                .is_pending()
        );
        assert_eq!(effects.get(), 0);
        assert_eq!(budget.snapshot().active_permits(), 1);
        release.send(()).unwrap();
        let prepared = resumed.await.unwrap();
        assert_eq!(effects.get(), 1);
        assert_eq!(std::ptr::from_ref(&*prepared), original);
        assert_eq!(prepared.request_id(), request);
        assert_eq!(prepared.observation(), Some(&observation));
        assert_eq!(prepared.verified_event().unwrap().event(), &signed);
        assert_eq!(budget.snapshot().active_permits(), 1);
        drop(prepared);
        assert_eq!(budget.snapshot().active_permits(), 0);
    }

    #[tokio::test]
    async fn sync_wire_phase_requires_renewal_even_with_remaining_time() {
        let cancellation = Cancellation::new();
        let due = Instant::now() + Duration::from_secs(5);
        let renewals = Cell::new(0);
        let unchanged = before_sync_operation(due, false, &cancellation, async {
            renewals.set(renewals.get() + 1);
            Ok(due)
        })
        .await
        .unwrap();
        assert_eq!(unchanged, Some(due));
        assert_eq!(renewals.get(), 0);
        let next = before_sync_operation(due, true, &cancellation, async {
            renewals.set(renewals.get() + 1);
            Ok(due)
        })
        .await
        .unwrap();
        assert_eq!(next, Some(due));
        assert_eq!(renewals.get(), 1);
    }

    #[tokio::test]
    async fn sync_safe_point_refuses_failed_revalidation_and_cancellation() {
        let cancellation = Cancellation::new();
        for reason in ["policy changed", "owner changed", "renewal commit unknown"] {
            let error = before_sync_operation(Instant::now(), false, &cancellation, async {
                Err((4, reason.into()))
            })
            .await
            .unwrap_err();
            assert_eq!(error, (4, reason.into()));
        }
        let next = before_sync_operation(Instant::now(), false, &cancellation, async {
            cancellation.request();
            Ok(Instant::now() + Duration::from_secs(5))
        })
        .await
        .unwrap();
        assert_eq!(next, None);
        let next = before_sync_operation(Instant::now(), true, &cancellation, async {
            panic!("cancelled work renewed")
        })
        .await
        .unwrap();
        assert_eq!(next, None);
    }
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn renewal_admission_does_not_wait_for_a_timer_tick() {
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        for due in [Instant::now() + Duration::from_millis(50), Instant::now()] {
            let mut renewal = std::pin::pin!(renewal_due(due));
            assert!(renewal.as_mut().poll(&mut context).is_ready());
        }
        let mut renewal = std::pin::pin!(renewal_due(Instant::now() + Duration::from_secs(60)));
        assert!(renewal.as_mut().poll(&mut context).is_pending());
    }

    #[test]
    fn admission_is_rechecked_after_a_select_wait() {
        let start = Instant::now();
        let due = start + Duration::from_millis(150);
        assert_eq!(admission(start, due, None), Admission::Ready);
        // HTTP and either Sync/Archive timer can become ready only now: the
        // earlier floor was true, but a 5ms operation cannot fit this horizon.
        let selected = start + Duration::from_millis(149);
        assert_eq!(admission(selected, due, None), Admission::Renew);
        let renewed = renewed_due(
            selected,
            Duration::from_secs(4),
            Duration::from_secs(1),
            Duration::from_millis(5),
        )
        .unwrap();
        assert_eq!(admission(selected, renewed, None), Admission::Ready);
    }

    #[test]
    fn a_short_archive_horizon_defers_instead_of_borrowing_new_budget() {
        let start = Instant::now();
        let due = start + Duration::from_secs(1);
        let end = start + Duration::from_millis(99);
        assert_eq!(admission(start, due, Some(end)), Admission::Defer);
        assert_eq!(admission(end, due, Some(end)), Admission::Defer);
        // A different operation after the original Archive future returned can
        // consume the retained request, under the actual current owner window.
        assert_eq!(admission(end, due, None), Admission::Ready);
    }

    #[test]
    fn slow_valid_renewals_cannot_create_an_unbounded_renewal_only_loop() {
        let now = Instant::now();
        let error = renewed_due(
            now,
            Duration::from_secs(5),
            Duration::from_millis(200),
            Duration::from_millis(250),
        )
        .unwrap_err();
        assert_eq!(error.0, 4);
        assert!(error.1.contains("100ms dispatch window"));
        assert!(
            renewed_due(
                now,
                Duration::from_millis(199),
                Duration::from_secs(1),
                Duration::ZERO
            )
            .is_err()
        );
        assert!(
            renewed_due(
                now,
                Duration::from_secs(5),
                Duration::from_millis(200),
                Duration::from_millis(100)
            )
            .is_err()
        );
        assert_eq!(
            renewed_due(
                now,
                Duration::from_secs(5),
                Duration::from_millis(200),
                Duration::from_millis(99)
            )
            .unwrap(),
            now + DISPATCH_MARGIN + Duration::from_millis(1)
        );
    }

    #[tokio::test]
    async fn external_deadline_and_cancellation_precede_first_nested_poll() {
        let cancellation = Cancellation::new();
        let polled = Cell::new(false);
        // Do not rely on the timer driver having observed this fresh expiry.
        let expired = Instant::now();
        let outcome = bounded_capped(
            Duration::from_secs(5),
            Some(expired),
            &cancellation,
            async {
                polled.set(true);
                Ok(())
            },
        )
        .await;
        assert_eq!(
            outcome.unwrap_err().1,
            "initial Archive deadline; retain original outcome"
        );
        assert!(!polled.get());
        cancellation.request();
        let outcome = bounded_capped(
            Duration::from_secs(5),
            Some(expired),
            &cancellation,
            async {
                polled.set(true);
                Ok(())
            },
        )
        .await;
        assert!(
            outcome
                .unwrap_err()
                .1
                .starts_with("initial Ingest cancelled")
        );
        assert!(!polled.get());
    }

    #[test]
    fn nested_phase_publication_keeps_the_absolute_archive_ceiling() {
        let old = Instant::now() + Duration::from_secs(60);
        let (progress, _observed) = watch::channel(Progress {
            deadline: old,
            phase: None,
            initialized: None,
            shutdown: None,
            sync: None,
            capacity: None,
        });
        let end = Instant::now() + Duration::from_secs(1);
        phase_capped(&progress, Duration::from_secs(5), Some(end)).unwrap();
        assert_eq!(progress.borrow().deadline, end);
        // A subsequent phase must keep the same Instant, not renew its budget.
        phase_capped(&progress, Duration::from_secs(20), Some(end)).unwrap();
        assert_eq!(progress.borrow().deadline, end);
    }

    #[tokio::test]
    async fn a_selected_http_request_retains_its_permit_until_consumed_or_dropped() {
        use bigbrotr::runtime::policy_authority::LeaseResponse;
        let mut root = crate::config::load(
            &Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/recipes/archive-only.toml"),
        )
        .unwrap();
        let reserved = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = reserved.local_addr().unwrap();
        root.values
            .insert("administration.listen.port".into(), json!(address.port()));
        let budget = super::super::budget(&root, false, false).unwrap();
        let dataset = "01010101010101010101010101010101".parse().unwrap();
        let incarnation = "02020202020202020202020202020202".parse().unwrap();
        let epoch = PolicyEpoch::new(1);
        let ttl = Duration::from_secs(5);
        let mut lease = PolicyLeaseTracker::new(
            dataset,
            incarnation,
            epoch,
            PolicyLeaseTtl::new(ttl).unwrap(),
        )
        .unwrap();
        let ticket = lease.begin_refresh().unwrap();
        lease
            .complete_refresh(
                &ticket,
                LeaseResponse {
                    dataset,
                    process_incarnation: incarnation,
                    policy_epoch: epoch,
                    ttl,
                },
            )
            .unwrap();
        drop(reserved);
        let mut management = Some(
            management::Management::start(
                &root,
                budget.clone(),
                lease.delivery_window(epoch).unwrap(),
                None,
                management::Profile::Initial {
                    public_routes: true,
                },
            )
            .await
            .unwrap(),
        );
        let client = tokio::spawn(async move {
            let mut socket = tokio::net::TcpStream::connect(address).await.unwrap();
            socket.write_all(format!("GET /management/v1/me HTTP/1.1\r\nHost: {address}\r\nCookie: BigBrotrLocalSession={}\r\n\r\n", "11".repeat(32)).as_bytes()).await.unwrap();
            let mut bytes = Vec::new();
            socket.read_to_end(&mut bytes).await.unwrap();
            bytes
        });
        let work = tokio::time::timeout(
            Duration::from_secs(1),
            super::super::super::next_management(&mut management),
        )
        .await
        .unwrap();
        let mut pending = Some(work);
        let retained = budget.snapshot().active_permits();
        assert!(retained >= 2);
        let due = Instant::now() + Duration::from_millis(50);
        tokio::select! {
            biased;
            _ = renewal_due(due) => {},
            _ = next_command(&mut pending, &mut management) => panic!("retained command overtook due renewal"),
        }
        assert!(pending.is_some());
        assert_eq!(budget.snapshot().active_permits(), retained);
        let cancellation = Cancellation::new();
        let renewed = Cell::new(false);
        assert!(retained_command(&mut pending, due, &cancellation).is_none());
        for cancelled in [false, true] {
            if cancelled {
                cancellation.request();
            }
            let outcome = bounded_capped(
                Duration::from_secs(5),
                Some(Instant::now()),
                &cancellation,
                async {
                    renewal_due(due).await;
                    renewed.set(true);
                    Ok(())
                },
            )
            .await;
            assert!(outcome.is_err());
            assert!(!renewed.get());
            assert!(pending.is_some());
            assert_eq!(budget.snapshot().active_permits(), retained);
        }
        assert!(
            retained_command(
                &mut pending,
                Instant::now() + Duration::from_secs(5),
                &cancellation,
            )
            .is_none()
        );
        tokio::select! {
            biased;
            _ = std::future::ready(()) => {},
            _ = next_command(&mut pending, &mut management) => panic!("management overtook ready timer/result"),
        }
        assert!(pending.is_some());
        assert_eq!(budget.snapshot().active_permits(), retained);
        tokio::select! {
            biased;
            _ = std::future::ready(()) => {},
            _ = next_command(&mut pending, &mut management), if false => panic!("disabled management slot ran"),
        }
        assert!(pending.is_some());
        // After one successful renewal the selected item precedes another
        // Sync round. It keeps the same grant and cannot be dequeued twice.
        let due = Instant::now() + Duration::from_secs(5);
        let cancellation = Cancellation::new();
        let work = retained_command(&mut pending, due, &cancellation).unwrap();
        assert!(retained_command(&mut pending, due, &cancellation).is_none());
        assert!(pending.is_none());
        assert_eq!(budget.snapshot().active_permits(), retained);
        pending = Some(work);
        management
            .as_mut()
            .unwrap()
            .stop(Duration::from_secs(1))
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(1), client)
                .await
                .unwrap()
                .unwrap()
                .is_empty()
        );
        // Socket termination alone never releases the selected request's grant.
        assert_eq!(budget.snapshot().active_permits(), 1);
        drop(pending);
        assert_eq!(budget.snapshot().active_permits(), 0);
    }
}
