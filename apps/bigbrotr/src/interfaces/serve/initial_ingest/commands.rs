//! The live native owner consumes the existing private command queue serially.
//! HTTP and password tasks run on the parent runtime; neither borrows native state.
use super::*;
use bigbrotr::runtime::policy_authority::{LeaseError, PolicyLeaseTracker, PolicyLeaseTtl};

struct CommandOwner<'a> {
    owner: &'a mut ControlSession,
    spec: &'a InitialRegistration,
    root: &'a Resolved,
    progress: &'a watch::Sender<Progress>,
    cancellation: &'a Cancellation,
    lease: PolicyLeaseTracker,
    administration: Option<bool>,
}

fn authority(error: LeaseError) -> (u8, String) {
    (4, error.to_string())
}

impl CommandOwner<'_> {
    fn maximum(&mut self) -> Result<std::time::Duration> {
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
        let snapshot = bounded(maximum, self.cancellation, async {
            let snapshot = self
                .owner
                .refresh(|doc, digest| {
                    let restored = super::super::accepted_policy(self.root, doc, digest)?;
                    selected(&restored).map_err(|_| OwnerError::InvalidPolicy)?;
                    administration &= restored.flag("administration.enabled");
                    Ok(())
                })
                .await
                .map_err(owner_error)?;
            if snapshot.config_revision != self.spec.config_revision
                || snapshot.policy_revision != self.spec.policy_revision
                || snapshot.config_digest != *self.spec.config_digest.as_bytes()
                || snapshot.policy_digest != *self.spec.policy_digest.as_bytes()
            {
                return Err((4, "accepted policy changed; initial Ingest stopped".into()));
            }
            self.owner
                .renew_initial_ingest(
                    self.spec,
                    ttl,
                    |doc, digest| validate(self.root, doc, digest),
                    &Cell::new(false),
                )
                .await
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
                    self.spec.dataset_id,
                    self.owner.identity().process_incarnation,
                    snapshot,
                    ttl,
                ),
            )
            .map_err(authority)?;
        self.administration = Some(administration);
        Ok(started)
    }

    fn idle_phase(&self, due: Instant) -> Result<()> {
        phase(
            self.progress,
            due.saturating_duration_since(Instant::now()) + duration(self.root, "renewal_timeout"),
        )
    }
}

pub(super) async fn maintain(
    owner: &mut ControlSession,
    spec: &InitialRegistration,
    root: &Resolved,
    budget: &ResourceBudget,
    progress: &watch::Sender<Progress>,
    cancellation: &Cancellation,
    mut report: Value,
) -> Result<()> {
    let lease = PolicyLeaseTracker::new(
        spec.dataset_id,
        owner.identity().process_incarnation,
        PolicyEpoch::new(spec.policy_revision),
        PolicyLeaseTtl::new(duration(root, "authorization_ttl")).map_err(authority)?,
    )
    .map_err(authority)?;
    let mut state = CommandOwner {
        owner,
        spec,
        root,
        progress,
        cancellation,
        lease,
        administration: None,
    };
    let started = state.renew().await?;
    let mut management = if state.administration == Some(true) {
        let maximum = state.maximum()?;
        phase(progress, maximum)?;
        bounded(maximum, cancellation, async {
            state.owner.require_browser_sessions().await.map_err(|e| {
                (
                    4,
                    format!("initial session prerequisites unavailable: {e:?}"),
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
                management::Profile::Sessions,
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
                "profile": "initial-sessions-only"
            }])
        } else {
            json!([])
        };
        let mut due = state.next_renewal(started)?;
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
                _ = tokio::time::sleep_until(due) => {
                    let started = state.renew().await?;
                    due = state.next_renewal(started)?;
                    state.idle_phase(due)?;
                    if let Some(management) = &management {
                        let window = state.lease
                            .delivery_window(state.lease.known_epoch())
                            .map_err(authority)?;
                        management.renew_delivery(window)?;
                    }
                },
                work = super::super::next_management(&mut management) => {
                    let maximum = state.maximum()?;
                    phase(progress, maximum)?;
                    let execution = bounded(
                        maximum,
                        cancellation,
                        management.as_mut().expect("command source").execute(
                            work, state.owner, &mut state.lease, maximum, false, None,
                        ),
                    ).await?;
                    let management::Execution::Complete(response) = execution else {
                        return Err((4, "unexpected operation on initial session surface".into()));
                    };
                    if cancellation.requested.load(Ordering::Acquire) {
                        return Ok(());
                    }
                    state.lease
                        .check_delivery(state.lease.known_epoch())
                        .map_err(authority)?;
                    // Keep due unchanged: an always-ready queue cannot defer renewal.
                    state.idle_phase(due)?;
                    if let Some(response) = response {
                        response.deliver();
                    }
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
