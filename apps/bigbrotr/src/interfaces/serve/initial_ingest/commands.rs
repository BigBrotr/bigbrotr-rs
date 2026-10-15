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
    archive_selected: bool,
}

fn authority(error: LeaseError) -> (u8, String) {
    (4, error.to_string())
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

    fn next_renewal(&mut self, started: Instant) -> Result<Instant> {
        // Slow valid responses already consumed authority and heartbeat time.
        let period = (self.lease.remaining_validity().map_err(authority)? / 2)
            .min(duration(self.root, "heartbeat_interval").saturating_sub(started.elapsed()));
        Ok(Instant::now() + period)
    }

    async fn renew(&mut self) -> Result<Instant> {
        // Archive callbacks remain strictly initial-only. They never borrow a
        // live writer or turn historical registration into current authority.
        if self.profile != Profile::Initial {
            return Err((4, "initial Archive cannot renew a successor".into()));
        }
        self.renew_for(None).await
    }

    async fn renew_for(&mut self, native: Option<&NativeOwner>) -> Result<Instant> {
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
        phase(self.progress, maximum)?;
        let ticket = self.lease.begin_refresh().map_err(authority)?;
        let started = Instant::now();
        let mut administration = true;
        let mut public_routes = true;
        let snapshot = bounded(maximum, self.cancellation, async {
            let snapshot = self
                .owner
                .refresh(|doc, digest| {
                    let restored = super::super::accepted_policy(self.root, doc, digest)?;
                    accepted_owner_profile(&restored, self.profile)?;
                    if let Some(sync) = self.sync.as_deref_mut() {
                        sync.accept(&restored)?;
                    }
                    if self.archive_selected {
                        archive::validate(&restored).map_err(|_| OwnerError::InvalidPolicy)?;
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
    ) -> Result<()> {
        let maximum = self
            .maximum()?
            .min(due.saturating_duration_since(Instant::now()));
        phase(self.progress, maximum)?;
        let execution = bounded(
            maximum,
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
        let management::Execution::Complete(response) = execution else {
            return Err((4, "unexpected operation on initial session surface".into()));
        };
        if !self.cancellation.requested.load(Ordering::Acquire) {
            self.lease
                .check_delivery(self.lease.known_epoch())
                .map_err(authority)?;
            self.idle_phase(due)?;
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
            self.lease
                .check_delivery(self.lease.known_epoch())
                .map_err(authority)?;
            phase(
                self.progress,
                (deadline.min(*due + duration(self.root, "renewal_timeout")))
                    .saturating_duration_since(Instant::now()),
            )?;
            tokio::select! {
                biased;
                _ = self.cancellation.wait() => return Err((4, "initial Archive cancelled; retain original outcome".into())),
                _ = tokio::time::sleep_until(deadline) => return Err((4, "initial Archive deadline; retain original outcome".into())),
                _ = tokio::time::sleep_until(*due) => {
                    let started = self.renew().await?;
                    *due = self.next_renewal(started)?;
                    if let Some(management) = management.as_ref() {
                        management.renew_delivery(self.lease.delivery_window(self.lease.known_epoch()).map_err(authority)?)?;
                    }
                },
                result = &mut work => return result,
                work = super::super::next_management(management) => self.command(work, management, (*due).min(deadline)).await?,
            }
        }
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
        archive_selected: archive.is_some(),
    };
    let started = state.renew_for(Some(store)).await?;
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
        let mut due = state.next_renewal(started)?;
        let mut dispatch_due = Instant::now() + sync::INTERVAL;
        let mut publish_due = Instant::now() + archive.as_ref().map_or(Duration::ZERO, archive::Archive::delay);
        state.idle_phase(due)?;
        progress.send_modify(|value| value.initialized = Some(report));
        loop {
            state
                .lease
                .check_delivery(state.lease.known_epoch())
                .map_err(authority)?;
            // Avoid starting a new bounded operation with only a timer tick
            // left. This advances renewal; it never extends the old authority.
            if due.saturating_duration_since(Instant::now()) < Duration::from_millis(100) {
                due = Instant::now();
            }
            tokio::select! {
                biased;
                _ = cancellation.wait() => return Ok(()),
                _ = tokio::time::sleep_until(due) => {
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
                _ = tokio::time::sleep_until(publish_due), if archive.is_some() => {
                    let maximum = state.maximum()?.min(due.saturating_duration_since(Instant::now()));
                    phase(progress, maximum)?;
                    let archive = archive.as_mut().expect("selected Archive");
                    let published = archive.publish(store.initial()?, budget, &mut state, &mut due,
                        &mut management, &cancellation.requested).await?;
                    // Drain available complete batches without another formation
                    // delay, but allow management/renewal between each pass.
                    publish_due = Instant::now() + if published { sync::INTERVAL } else { archive.delay() };
                    state.idle_phase(due)?;
                },
                _ = tokio::time::sleep_until(dispatch_due), if state.sync.is_some() => {
                    let maximum = state.maximum()?.min(due.saturating_duration_since(Instant::now()));
                    phase(progress, maximum)?;
                    // A signal asks this serial consumer to finish its current
                    // SQL rollback/commit. Cancelling the whole future here
                    // would poison an otherwise healthy quiet source owner.
                    // The hard deadline and independent parent watchdog remain.
                    tokio::time::timeout(maximum,
                        state.sync.as_deref_mut().expect("selected Sync").dispatch(sync::Dispatch {
                            owner: state.owner, writer: store, context, root, budget, cancel: cancellation,
                            deadline: Instant::now() + maximum,
                        }),
                    ).await.map_err(|_| (4, "initial Sync deadline; preserve original outcome".into()))??;
                    // A timed gap lets HTTP run even under continuous work;
                    // the priority timer prevents continuous HTTP starving Sync.
                    dispatch_due = Instant::now() + sync::INTERVAL;
                    state.idle_phase(due)?;
                },
                work = super::super::next_management(&mut management) => {
                    state.command(work, &mut management, due).await?;
                },
            }
        }
    }
    .await;
    let grace = duration(root, "graceful_shutdown_timeout");
    // A late phase must not revive the watchdog, but still owes actual cleanup.
    let phase_result = phase(progress, grace);
    if let Some(management) = &mut management {
        management.stop(grace).await?;
    }
    outcome?;
    phase_result
}
