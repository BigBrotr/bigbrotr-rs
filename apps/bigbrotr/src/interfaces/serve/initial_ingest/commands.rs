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
        self.renew_capped(None, Some(deadline)).await
    }

    async fn renew_for(&mut self, native: Option<&NativeOwner>) -> Result<Instant> {
        self.renew_capped(native, None).await
    }

    async fn renew_capped(
        &mut self,
        native: Option<&NativeOwner>,
        deadline: Option<Instant>,
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
        let management::Execution::Complete(response) = execution else {
            return Err((4, "unexpected operation on initial session surface".into()));
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
        pending: None,
        pending_renewed: false,
    };
    let started = state.renew_for(Some(store)).await?;
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
        let mut publish_due = Instant::now() + archive.as_ref().map_or(Duration::ZERO, archive::Archive::delay);
        state.idle_phase(due)?;
        progress.send_modify(|value| value.initialized = Some(report));
        loop {
            state
                .lease
                .check_delivery(state.lease.known_epoch())
                .map_err(authority)?;
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
                _ = tokio::time::sleep_until(publish_due), if archive.is_some() => {
                    // The timer may become ready only after select slept near
                    // renewal. No native/Sync effect precedes this recheck.
                    if admission(Instant::now(), due, None) == Admission::Renew {
                        continue;
                    }
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
                    // The timer may become ready only after select slept near
                    // renewal. No native/Sync effect precedes this recheck.
                    if admission(Instant::now(), due, None) == Admission::Renew {
                        continue;
                    }
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
                work = next_command(&mut state.pending, &mut management) => {
                    state.command(work, &mut management, due, None).await?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn renewal_admission_does_not_wait_for_a_timer_tick() {
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        for due in [
            Instant::now() + Duration::from_millis(50),
            Instant::now(),
        ] {
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
        let work = next_command(&mut pending, &mut management).await;
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
