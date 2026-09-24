//! One authenticated allocation read under the ordinary Control supervisor.
use super::{Result, Stop, duration, refresh_authority};
use crate::{
    config::Resolved,
    interfaces::{archive_preflight as preflight, management, postgres_command},
};
use bigbrotr::{
    management::sessions::SessionError,
    runtime::{OwnedPermit, ResourceBudget, ResourceRequest, policy_authority::PolicyLeaseTracker},
    secrets::{SecretBytes, SecretFile},
    storage::{
        archive::{
            CatalogReaderCredentials, LocalCoordinator, NativeClientIdentity, Preparation,
            PreparationError, ProcessError, ReaderCustody, ReceiptError,
        },
        postgres::{
            catalog_guard::{self, CatalogCapture},
            connection_settings::PreparedConnection,
            control_owner::{
                ArchivePinError, ArchivePinRequest, ControlSession, PolicySnapshot,
                ScopeGrantAllocation,
            },
            transport,
        },
    },
};
use bigbrotr_model::DatasetId;
use bigbrotr_protocol::archive_visibility::{MAX_FACT_BYTES, VisibilityRead};
use std::{
    cell::Cell,
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::time::Instant;

/// This borrow keeps all authority on the Control thread. Catalog futures may
/// borrow the non-Sync coordinator; only ReaderCustody moves to blocking work.
pub(super) struct Supervisor<'a> {
    pub owner: &'a mut ControlSession,
    pub lease: &'a mut PolicyLeaseTracker,
    pub renewed: &'a mut Instant,
    pub refresh_due: &'a mut Instant,
    pub dataset: DatasetId,
    pub snapshot: PolicySnapshot,
    pub root: &'a Resolved,
    pub stop: &'a mut Stop,
    pub management: &'a mut management::Management,
    pub stopping: bool,
}
pub(super) enum OnShutdown {
    Cancel,
    /// Collect the same cleanup future; a signal cannot imply its completion.
    Finish,
}
impl Supervisor<'_> {
    pub(super) fn maximum(&mut self) -> Result<Duration> {
        Ok(duration(self.root, "renewal_timeout").min(
            self.lease
                .remaining_validity()
                .map_err(|e| (4, e.to_string()))?
                / 2,
        ))
    }

    /// The same absolute deadline survives every ready phase. Renewal wins ready
    /// work and never extends this finite allocation operation's own deadline.
    /// Only catalog/native work belongs here: dropping an in-flight Control
    /// operation poisons that session and must remain a fatal outcome.
    pub(super) async fn external<T>(
        &mut self,
        deadline: Instant,
        future: impl Future<Output = T>,
        on_shutdown: OnShutdown,
    ) -> Result<Option<T>> {
        self.external_monitored(deadline, future, on_shutdown, || {})
            .await
    }

    async fn external_monitored<T>(
        &mut self,
        deadline: Instant,
        future: impl Future<Output = T>,
        on_shutdown: OnShutdown,
        monitor: impl Fn(),
    ) -> Result<Option<T>> {
        if self.stopping && matches!(on_shutdown, OnShutdown::Cancel) {
            return Ok(None);
        }
        tokio::pin!(future);
        loop {
            self.lease
                .check_delivery(self.lease.known_epoch())
                .map_err(|e| (4, e.to_string()))?;
            tokio::select! {
                biased;
                _ = self.stop.requested(), if !self.stopping => {
                    self.management.stop_admission();
                    self.stopping = true;
                    if matches!(on_shutdown, OnShutdown::Cancel) {
                        return Ok(None);
                    }
                },
                _ = tokio::time::sleep_until(deadline) => return Ok(None),
                _ = tokio::time::sleep_until(*self.refresh_due) => {
                    let current = refresh_authority(self.owner, self.lease, self.renewed,
                        self.dataset, self.root, self.stop).await?;
                    if current != self.snapshot {
                        return Err((4, "accepted policy changed during native grant".into()));
                    }
                    if !self.stopping {
                        self.management.renew_delivery(self.lease.delivery_window(self.lease.known_epoch())
                            .map_err(|e| (4,e.to_string()))?)?;
                    }
                    *self.refresh_due = Instant::now()
                        + (self.lease.remaining_validity().map_err(|e| (4,e.to_string()))? / 2)
                        .min(duration(self.root,"heartbeat_interval").saturating_sub(self.renewed.elapsed()));
                },
                result = &mut future => {
                    // Return ownership before checking authority again: a ready
                    // JoinHandle must never be polled twice after expiry.
                    return Ok(Some(result));
                },
                work = self.management.next(), if !self.stopping && matches!(on_shutdown, OnShutdown::Cancel) => {
                    // The ready native result wins the queue above, so HTTP
                    // traffic cannot postpone collecting a completed worker.
                    let maximum = self.maximum()?.min(deadline.saturating_duration_since(Instant::now()));
                    let execution = tokio::select! {
                        biased;
                        _ = self.stop.requested() => return Err((4,"private operation cancelled during native work; Control outcome unresolved".into())),
                        result = monitored(self.management.execute(work,self.owner,maximum,false,None), &monitor) => result?,
                    };
                    let response = match execution {
                        management::Execution::Complete(response) => response,
                        // One native consumer owns the capture barrier/budget.
                        // Historical retries and revocation can use Control,
                        // but new archive work must retry after this consumer.
                        management::Execution::History(work) => Some(work.refuse(SessionError::Capacity)),
                        management::Execution::Archive(work) => Some(work.refuse(SessionError::Capacity)),
                    };
                    self.lease.check_delivery(self.lease.known_epoch()).map_err(|e| (4,e.to_string()))?;
                    if let Some(response) = response { response.deliver(); }
                },
            }
        }
    }

    /// Dropping a JoinHandle never joins spawn_blocking. Even on signal, expiry
    /// or lost Control, inspect its final result before allowing pin cleanup.
    pub(super) async fn blocking<T: Send + 'static>(
        &mut self,
        deadline: Instant,
        job: impl FnOnce(&AtomicBool) -> std::result::Result<T, WorkerError> + Send + 'static,
    ) -> Result<std::result::Result<T, SessionError>> {
        self.blocking_while(deadline, || true, job).await
    }

    pub(super) async fn blocking_while<T: Send + 'static>(
        &mut self,
        deadline: Instant,
        permitted: impl Fn() -> bool,
        job: impl FnOnce(&AtomicBool) -> std::result::Result<T, WorkerError> + Send + 'static,
    ) -> Result<std::result::Result<T, SessionError>> {
        self.lease
            .check_delivery(self.lease.known_epoch())
            .map_err(|e| (4, e.to_string()))?;
        if self.stopping || Instant::now() >= deadline || !permitted() {
            return Ok(Err(SessionError::ArchiveUnavailable));
        }
        let cancel = Arc::new(AtomicBool::new(false));
        let worker_cancel = cancel.clone();
        let mut worker = tokio::task::spawn_blocking(move || job(&worker_cancel));
        let observation = self
            .external_monitored(
                deadline,
                async {
                    loop {
                        if !permitted() {
                            return None;
                        }
                        tokio::select! {
                            biased;
                            _ = tokio::time::sleep(Duration::from_millis(25)) => {},
                            terminal = &mut worker => return Some(terminal),
                        }
                    }
                },
                OnShutdown::Cancel,
                || {
                    // Management SQL still has to be collected, but cannot
                    // suspend disconnect/action-expiry cancellation of a worker.
                    if !permitted() || Instant::now() >= deadline {
                        cancel.store(true, Ordering::Release);
                    }
                },
            )
            .await;
        cancel.store(true, Ordering::Release);
        let (terminal, prior) = match observation {
            Ok(Some(Some(terminal))) => (terminal, Ok(true)),
            other => {
                if other.is_err() {
                    self.management.stop_admission();
                }
                let terminal = tokio::time::timeout(
                    duration(self.root, "graceful_shutdown_timeout"),
                    &mut worker,
                )
                .await
                .map_err(|_| {
                    (
                        4,
                        "native grant worker not joined; retain pin and terminate process".into(),
                    )
                })?;
                (terminal, other.map(|_| false))
            }
        };
        let terminal = terminal.map_err(|_| {
            (
                4,
                "native grant worker panicked; retain pin and terminate process".into(),
            )
        })?;
        if matches!(terminal, Err(WorkerError::Unjoined)) {
            return Err((
                4,
                "native child termination unknown; retain pin and scratch".into(),
            ));
        }
        let admitted = prior?;
        self.lease
            .check_delivery(self.lease.known_epoch())
            .map_err(|e| (4, e.to_string()))?;
        if !admitted {
            return Ok(Err(SessionError::ArchiveUnavailable));
        }
        Ok(terminal.map_err(|error| match error {
            WorkerError::Refused(error) => error,
            WorkerError::Unjoined => unreachable!("handled before cleanup"),
        }))
    }
}

pub(super) enum WorkerError {
    Refused(SessionError),
    Unjoined,
}
pub(super) fn unavailable<T>(_: T) -> WorkerError {
    WorkerError::Refused(SessionError::ArchiveUnavailable)
}
pub(super) struct Prepared {
    pub(super) manager: PreparedConnection,
    pub(super) manager_grant: OwnedPermit,
    pub(super) reader: ReaderInput,
}
pub(super) struct ReaderInput {
    pub(super) password: SecretBytes,
    pub(super) tls: Option<NativeClientIdentity>,
    _permit: OwnedPermit,
}
pub(super) fn required<'a>(
    config: &'a Resolved,
    key: &str,
) -> std::result::Result<&'a str, WorkerError> {
    config.text(key).ok_or_else(|| unavailable(()))
}
pub(super) fn prepare(
    config: &Resolved,
    budget: &ResourceBudget,
) -> std::result::Result<Prepared, WorkerError> {
    let permit = budget
        .try_acquire(ResourceRequest::data(1024 * 1024, 1, 0).unwrap())
        .map_err(|_| WorkerError::Refused(SessionError::Capacity))?;
    let (manager, manager_grant) = postgres_command::prepare_catalog_as(
        config,
        required(config, "postgres.catalog.user")?,
        required(config, "postgres.catalog.password_ref")?,
        budget,
    )
    .map_err(unavailable)?;
    let password = SecretFile::parse(required(config, "postgres.catalog.read_password_ref")?)
        .and_then(|file| file.read(4096))
        .map_err(unavailable)?;
    std::str::from_utf8(password.as_bytes()).map_err(unavailable)?;
    let tls = preflight::native_tls(config, budget).map_err(unavailable)?;
    Ok(Prepared {
        manager,
        manager_grant,
        reader: ReaderInput {
            password,
            tls,
            _permit: permit,
        },
    })
}

#[allow(clippy::too_many_arguments)]
fn observe(
    config: Arc<Resolved>,
    budget: ResourceBudget,
    reader: ReaderInput,
    route: transport::PinnedRoute,
    custody: ReaderCustody,
    expected: VisibilityRead,
    deadline: Instant,
    cancel: &AtomicBool,
) -> std::result::Result<bigbrotr::storage::archive::ArchivedVisibility, WorkerError> {
    let route = preflight::NativeRoute::new(&config, &route, reader.tls).map_err(unavailable)?;
    let mut limits = preflight::limits(&config).map_err(unavailable)?;
    limits.max_records = 2;
    limits.max_encoded_bytes = MAX_FACT_BYTES;
    let (installation, scratch, _) = preflight::worker(
        &config,
        custody.identity(),
        None,
        "grant-allocation",
        limits,
        deadline.into_std(),
        cancel,
    )
    .map_err(unavailable)?;
    let result = Preparation {
        installation: &installation,
        temp_root: &scratch,
        limits,
        deadline: deadline.into_std(),
        cancelled: cancel,
    }
    .read_visibility(
        expected,
        custody,
        &route.borrowed(),
        CatalogReaderCredentials {
            login: required(&config, "postgres.catalog.read_user")?,
            password: std::str::from_utf8(reader.password.as_bytes()).map_err(unavailable)?,
        },
        &budget,
    );
    if matches!(
        result,
        Err(ReceiptError::TerminationUnknown
            | ReceiptError::Preparation(PreparationError::Process(
                ProcessError::TerminationUnknown
            )))
    ) {
        return Err(WorkerError::Unjoined);
    }
    // This directory was created exclusively for read-only native scratch. The
    // actual child is joined on every known result; no archive data lives here.
    std::fs::remove_dir_all(&scratch).map_err(unavailable)?;
    result.map_err(|error| match error {
        ReceiptError::ResourceLimit => WorkerError::Refused(SessionError::Capacity),
        _ => unavailable(error),
    })
}

pub(super) fn pin_error(error: ArchivePinError) -> (u8, String) {
    (
        4,
        format!("native grant Control operation failed: {error:?}; pin outcome requires recovery"),
    )
}

pub(super) async fn execute(
    work: management::AllocationWork,
    config: Arc<Resolved>,
    budget: ResourceBudget,
    coordinator: &LocalCoordinator,
    authority: &mut Supervisor<'_>,
) -> Result<management::Response> {
    let deadline = work.deadline();
    let Some(allocation) = work.allocation() else {
        return Ok(work.refuse(SessionError::InvalidInput));
    };
    if work.abandoned() || allocation.catalog_id != coordinator.identity().catalog() {
        return Ok(work.refuse(SessionError::ArchiveUnavailable));
    }
    let maximum = authority.maximum()?;
    let selected = tokio::select! {
        biased;
        _ = authority.stop.requested() => return Err((4,"native grant selection cancelled".into())),
        result = tokio::time::timeout(maximum, authority.owner.read_archive_selection(allocation.catalog_id,allocation.frontier_revision.get())) =>
            result.map_err(|_| (4,"native grant selection deadline; Control unavailable".into()))?,
    };
    let selected = match selected {
        Err(ArchivePinError::ResourceLimit) => return Ok(work.refuse(SessionError::Capacity)),
        other => other.map_err(pin_error)?,
    };
    let Some(selected) = selected else {
        return Ok(work.refuse(SessionError::ArchiveUnavailable));
    };
    // Filesystem calls can block indefinitely. They run under renewal/signal
    // supervision before any catalog transaction or temporary pin exists.
    let destination = match work.destination() {
        Some(destination) => match authority
            .blocking_while(
                deadline,
                || !work.abandoned(),
                move |_| destination.check_for_acceptance().map_err(unavailable),
            )
            .await?
        {
            Ok(checked) => Some(checked),
            Err(error) => return Ok(work.refuse(error)),
        },
        None => None,
    };
    let preparation_config = config.clone();
    let preparation_budget = budget.clone();
    let prepared = match authority
        .blocking(deadline, move |_| {
            prepare(&preparation_config, &preparation_budget)
        })
        .await?
    {
        Ok(value) => value,
        Err(error) => return Ok(work.refuse(error)),
    };
    let connection = authority
        .external(
            deadline,
            transport::connect_pinned(prepared.manager, None),
            OnShutdown::Cancel,
        )
        .await?;
    let Some(Ok((mut manager, connection, route))) = connection else {
        return Ok(work.refuse(SessionError::ArchiveUnavailable));
    };
    let driver = tokio::spawn(coordinator.retain_connection(connection).map_err(|_| {
        (
            4,
            "cannot retain native grant catalog driver custody".into(),
        )
    })?);
    // All exits after driver creation reach its join with the manager grant held.
    let outcome = async {
        let reader = config.text("postgres.catalog.read_user").expect("selected reader");
        if !matches!(authority.external(deadline, catalog_guard::inspect_reader(&mut manager, coordinator.identity(),reader), OnShutdown::Cancel).await?,Some(Ok(()))) {
            return Ok(work.refuse(SessionError::ArchiveUnavailable));
        }
        let capture_duration = deadline.saturating_duration_since(Instant::now()).min(Duration::from_secs(120));
        let capture = match authority.external(deadline, CatalogCapture::at_snapshot(&mut manager,coordinator,selected.snapshot_id,capture_duration), OnShutdown::Cancel).await? {
            Some(Ok(value)) => value,
            _ => return Ok(work.refuse(SessionError::ArchiveUnavailable)),
        };
        let lease_id = bigbrotr::runtime::ownership::fresh_process_incarnation()
            .map_err(|_| (4,"native grant pin identity unavailable".into()))?;
        let maximum = authority.maximum()?;
        let request = ArchivePinRequest { lease_id:*lease_id.as_bytes(),frontier_revision:selected.frontier_revision,ttl:capture_duration };
        let commit = Cell::new(false);
        let pin = tokio::select! {
            biased;
            _ = authority.stop.requested() => return Err((4,"native grant pin admission cancelled; outcome unresolved".into())),
            result = tokio::time::timeout(maximum, authority.owner.register_archive_pin(&capture,&request, &commit)) => result.map_err(|_| (4,"native grant pin admission deadline; outcome unresolved".into()))?.map_err(pin_error)?,
        };
        let expected = VisibilityRead { dataset:selected.dataset_id, scope:work.scope(),
            topology_generation:selected.topology_generation,bounds:selected.bounds,snapshot_id:selected.snapshot_id };
        let custody = coordinator.reader_custody().map_err(|_| (4,"native grant reader custody refused; retain pin".into()))?;
        let native = authority.blocking(deadline, move |cancel| observe(config,budget,prepared.reader,route,custody,expected,deadline,cancel)).await?;
        let response = match native {
            Ok(visibility) => {
                let allocation = ScopeGrantAllocation { capture:&capture, visibility:&visibility, pin:&pin };
                let maximum = authority.maximum()?;
                let response = tokio::select! {
                    biased;
                    _ = authority.stop.requested() => return Err((4,"native grant acceptance cancelled; outcome unresolved".into())),
                    result = work.accept(authority.owner,maximum,&allocation,destination.as_ref()) => result?,
                };
                drop(visibility);
                response
            }
            Err(error) => work.refuse(error),
        };
        // No release on unknown worker/Control outcome. Expiry never releases a
        // durable pin; only this positively joined holder can end the obligation.
        authority.lease.check_delivery(authority.lease.known_epoch()).map_err(|e| (4,e.to_string()))?;
        tokio::time::timeout(authority.maximum()?,authority.owner.release_archive_pin(&pin,&Cell::new(false))).await
            .map_err(|_| (4,"native grant pin release outcome unresolved".into()))?.map_err(pin_error)?;
        // The request deadline cannot cancel cleanup after a known child join.
        // This separate finite cleanup allowance permits no new native work or
        // acceptance. Driver destruction below still retains local custody.
        let cleanup_deadline = Instant::now() + duration(authority.root,"graceful_shutdown_timeout");
        if !matches!(authority.external(cleanup_deadline, capture.finish(), OnShutdown::Finish).await?, Some(Ok(()))) {
            return Err((4,"native grant catalog barrier completion unresolved".into()));
        }
        Ok(response)
    }.await;
    if outcome.is_err() {
        authority.management.stop_admission();
    }
    drop(manager);
    postgres_command::join(driver).await;
    drop(prepared.manager_grant);
    outcome
}

/// Keep a native cancellation observer live while collecting one Control call.
async fn monitored<T>(future: impl Future<Output = T>, monitor: &impl Fn()) -> T {
    tokio::pin!(future);
    loop {
        monitor();
        tokio::select! {
            biased;
            result = &mut future => return result,
            _ = tokio::time::sleep(Duration::from_millis(25)) => {},
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_cancellation_remains_observed_while_control_waits() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        runtime.block_on(async {
            let allowed = Cell::new(true);
            let cancelled = Cell::new(false);
            let control = async {
                allowed.set(false);
                // A pending real timer gives the monitor multiple opportunities;
                // collection continues to the original Control result.
                tokio::time::sleep(Duration::from_millis(100)).await;
                assert!(
                    cancelled.get(),
                    "native cancellation observer was suspended"
                );
                42
            };
            let result = monitored(control, &|| {
                if !allowed.get() {
                    cancelled.set(true);
                }
            })
            .await;
            assert_eq!(result, 42);
        });
    }
}
