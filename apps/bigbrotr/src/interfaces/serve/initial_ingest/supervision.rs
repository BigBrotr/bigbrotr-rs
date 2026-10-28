//! Retain the one native owner through actual completion or explicit fail-stop.
use super::{Cancellation, Progress, Result};
use serde_json::Value;
use std::{future::Future, time::Duration};
use tokio::{
    sync::watch,
    task::{JoinError, JoinHandle},
    time::Instant,
};

// Declare after the pinned acquisition future, so cancellation, an early return
// or unwinding closes native before that future is destroyed. This controls
// caller-driven destruction only: an adapter error or transport failure may
// already have closed the client. The supervisor independently retains the
// actual driver and its grant until this worker has completed native teardown.
pub(super) struct ClaimExit<'a, T>(pub(super) Option<&'a mut Option<T>>);
impl<T> Drop for ClaimExit<'_, T> {
    fn drop(&mut self) {
        if let Some(native) = self.0.take() {
            drop(native.take());
        }
    }
}

pub(super) enum Collected<T> {
    Joined {
        terminal: std::result::Result<Result<T>, JoinError>,
        interrupted: bool,
    },
    Unjoined,
}

pub(super) async fn collect<T>(
    worker: &mut JoinHandle<Result<T>>,
    observed: &mut watch::Receiver<Progress>,
    cancellation: &Cancellation,
    stop: impl Future<Output = ()>,
    grace: Duration,
    mut emit: impl FnMut(Value),
) -> Collected<T> {
    tokio::pin!(stop);
    let mut reported = false;
    let mut deadline_diagnostic = None;
    let (terminal, interrupted) = loop {
        let current = observed.borrow_and_update().clone();
        if !reported && let Some(report) = current.initialized {
            emit(report);
            reported = true;
        }
        let reason = tokio::select! {
            biased;
            _ = &mut stop => Some("stop"),
            _ = tokio::time::sleep_until(current.deadline) => {
                // A new phase can be published after our snapshot. Its already
                // ready watch notification must not lose to this cached timer.
                let latest = observed.borrow();
                if Instant::now() >= latest.deadline {
                    // Freeze before cancellation/grace lets cleanup advance
                    // the caller. This is a Copy snapshot, not a report clone.
                    deadline_diagnostic = super::sync_diagnostic::capture(&latest);
                    Some("owner deadline")
                } else {
                    None
                }
            },
            result = &mut *worker => break (result, false),
            _ = std::future::ready(()), if current.capacity.as_ref().is_some_and(|mailbox| mailbox.has_pending()) => {
                current.capacity.as_ref().unwrap().drain(&mut emit);
                None
            },
            result = observed.changed() => {
                // A closed progress sender can precede JoinHandle readiness.
                // Await that same worker; never spin or treat it as joined.
                if result.is_err() { Some("owner ended") } else { None }
            },
        };
        if let Some(reason) = reason {
            cancellation.request();
            // Use the latest publication after setting cancellation. The worker
            // publishes first, then checks that same atomic flag before dispatch.
            if let Some(shutdown) = &observed.borrow().shutdown {
                shutdown.stop_admission();
            }
            if let Some(snapshot) = deadline_diagnostic.take() {
                // Safety actions precede formatting/output. The retained
                // snapshot still describes the original deadline observation.
                emit(snapshot.report("parent-watchdog"));
            }
            match tokio::time::timeout(grace, &mut *worker).await {
                Ok(terminal) => break (terminal, reason != "stop" && reason != "owner ended"),
                Err(_) => return Collected::Unjoined,
            }
        }
    };
    if let Some(shutdown) = &observed.borrow().shutdown {
        shutdown.stop_admission();
    }
    // Also cover a terminal worker winning over watch delivery, including the
    // actual cancellation-grace join. Safety and join precede formatting.
    let capacity = observed.borrow().capacity.clone();
    if let Some(mailbox) = capacity {
        mailbox.drain(&mut emit);
    }

    Collected::Joined {
        terminal,
        interrupted,
    }
}

/// Shared shell for owners that must open existing native state before reading
/// protected connection files. The handle never leaves its blocking worker.
pub(super) type DriverCustody = tokio::sync::oneshot::Sender<(
    crate::interfaces::postgres_command::DriverHandle,
    bigbrotr::runtime::OwnedPermit,
)>;
pub(super) fn run_owned<F, Work>(
    root: crate::config::Resolved,
    budget: bigbrotr::runtime::ResourceBudget,
    output: &mut crate::interfaces::lifecycle_output::LifecycleOutput,
    label: &'static str,
    graceful_stop: bool,
    blocking_threads: usize,
    work: F,
) -> Result<()>
where
    F: FnOnce(
            crate::config::Resolved,
            bigbrotr::runtime::ResourceBudget,
            watch::Sender<Progress>,
            std::sync::Arc<Cancellation>,
            DriverCustody,
        ) -> Work
        + Send
        + 'static,
    Work: Future<Output = Result<Value>>,
{
    use super::{Stop, duration, postgres_command};
    use std::sync::{Arc, atomic::Ordering};
    let timeout = duration(&root, "renewal_timeout");
    let grace = duration(&root, "graceful_shutdown_timeout");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(blocking_threads)
        .build()
        .map_err(|_| (1, format!("{label} runtime unavailable")))?;
    let result = runtime.block_on(async {
        let mut stop = Stop::new()?;
        let cancellation = Arc::new(Cancellation::new());
        let (progress, mut observed) = watch::channel(Progress {
            deadline: Instant::now() + timeout, phase: None, initialized: None, shutdown: None, sync: None, capacity: None,
        });
        let (send_driver, receive_driver) = tokio::sync::oneshot::channel();
        let worker_cancel = cancellation.clone();
        // Keep progress alive through collection, including the join boundary.
        let worker_progress = progress.clone();
        let handle = tokio::runtime::Handle::current();
        let mut worker = tokio::task::spawn_blocking(move || {
            handle.block_on(work(root, budget, worker_progress, worker_cancel, send_driver))
        });
        let (terminal, interrupted) = match collect(&mut worker, &mut observed,
            &cancellation, stop.requested(), grace, |report| output.emit(report, false)).await {
            Collected::Joined { terminal, interrupted } => (terminal, interrupted),
            Collected::Unjoined => {
                output.emit(format!("error: {label} worker not joined; preserve native and Control outcomes; terminating process"), true);
                std::process::exit(4);
            }
        };
        if let Ok((driver, grant)) = receive_driver.await {
            postgres_command::join(driver).await;
            drop(grant);
        }
        let report = terminal.map_err(|_| (4, format!("{label} worker panicked; preserve retained state")))??;
        if interrupted || (!graceful_stop && cancellation.requested.load(Ordering::Acquire)) {
            return Err((4, format!("{label} cancelled or expired; preserve retained state")));
        }
        output.emit(report, false);
        Ok(())
    });
    runtime.shutdown_timeout(postgres_command::DRIVER_DRAIN);
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    #[tokio::test]
    async fn parent_watchdog_freezes_sync_stage_before_grace() {
        use super::super::sync_diagnostic::{Stage, Trace, Window};
        let now = Instant::now();
        let expired = now - Duration::from_secs(1);
        let (progress, mut observed) = watch::channel(Progress {
            deadline: expired,
            phase: None,
            initialized: None,
            shutdown: None,
            sync: None,
            capacity: None,
        });
        let trace = Trace::new(&progress);
        trace.begin(Window {
            selected: expired,
            authority_checked: expired,
            renewal_timeout: Duration::from_secs(1),
            authority_remaining: Duration::from_secs(4),
            due_remaining: Duration::from_secs(1),
            maximum: Duration::from_secs(1),
            renewal_due: now,
            parent_deadline: expired,
            dispatch_deadline: now,
            outer_deadline: now,
        });
        trace.enter(Stage::Admission, Some(0), Some(4));
        let cancel = Arc::new(Cancellation::new());
        let child_cancel = cancel.clone();
        let child_progress = progress.clone();
        let mut worker = tokio::spawn(async move {
            child_cancel.wait().await;
            let trace = Trace::new(&child_progress);
            trace.returned();
            trace.delivered_admission(4);
            trace.enter(Stage::Certification, Some(2), None);
            Ok(json!({"state":"closed"}))
        });
        let mut reports = Vec::new();
        let result = collect(
            &mut worker,
            &mut observed,
            &cancel,
            std::future::pending(),
            Duration::from_secs(1),
            |report| {
                assert!(cancel.requested.load(Ordering::Acquire));
                reports.push(report);
            },
        )
        .await;
        assert!(matches!(
            result,
            Collected::Joined {
                terminal: Ok(Ok(_)),
                interrupted: true
            }
        ));
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0]["reason"], "parent-watchdog");
        assert_eq!(reports[0]["stage"], "admission");
        assert_eq!(reports[0]["request_sequence"], 4);
        assert_eq!(reports[0]["returned_us"], Value::Null);
        assert_eq!(reports[0]["last_delivered_admission_sequence"], Value::Null);
        let later = super::super::sync_diagnostic::capture(&progress.borrow())
            .unwrap()
            .report("test");
        assert_eq!(later["stage"], "certification");
        assert_eq!(later["last_delivered_admission_sequence"], 4);
        assert_eq!(progress.borrow().deadline, expired);
    }

    #[tokio::test]
    async fn a_stopped_preparer_returns_owned_state_only_after_join() {
        struct Prepared(Arc<AtomicBool>);
        impl Drop for Prepared {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }
        let cancelled = Arc::new(Cancellation::new());
        let released = Arc::new(AtomicBool::new(false));
        let child_cancel = cancelled.clone();
        let child_released = released.clone();
        let mut worker = tokio::task::spawn_blocking(move || {
            let owned = Prepared(child_released);
            while !child_cancel.requested.load(Ordering::Acquire) {
                std::thread::yield_now();
            }
            Ok(owned)
        });
        let (_progress, mut observed) = watch::channel(Progress {
            deadline: Instant::now() + Duration::from_secs(60),
            phase: None,
            initialized: None,
            shutdown: None,
            sync: None,
            capacity: None,
        });
        let result = collect(
            &mut worker,
            &mut observed,
            &cancelled,
            async {},
            Duration::from_secs(1),
            |_| panic!("preparation published readiness"),
        )
        .await;
        let Collected::Joined {
            terminal: Ok(Ok(prepared)),
            interrupted: false,
        } = result
        else {
            panic!("preparer did not join");
        };
        assert!(worker.is_finished());
        assert!(cancelled.requested.load(Ordering::Acquire));
        assert!(!released.load(Ordering::Acquire));
        drop(prepared);
        assert!(released.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn published_phase_wins_an_expired_cached_deadline() {
        let cancel = Cancellation::new();
        let (progress, mut observed) = watch::channel(Progress {
            deadline: Instant::now() - Duration::from_secs(1),
            phase: None,
            initialized: Some(json!({"state":"initialized"})),
            shutdown: None,
            sync: None,
            capacity: None,
        });
        let mut worker = tokio::spawn(async { Ok(json!({"state":"closed"})) });
        tokio::task::yield_now().await;
        // Advance after the supervisor snapshots the old phase, but before its
        // biased select. Both the old timer and new progress are then ready.
        let result = collect(
            &mut worker,
            &mut observed,
            &cancel,
            std::future::pending(),
            Duration::from_secs(1),
            |_| progress.send_modify(|p| p.deadline = Instant::now() + Duration::from_secs(60)),
        )
        .await;
        assert!(matches!(
            result,
            Collected::Joined {
                terminal: Ok(Ok(_)),
                interrupted: false
            }
        ));
        assert!(!cancel.requested.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn stop_collects_the_blocking_worker_after_its_cleanup() {
        let cancel = Arc::new(Cancellation::new());
        let closed = Arc::new(AtomicBool::new(false));
        let child_cancel = cancel.clone();
        let child_closed = closed.clone();
        let mut worker = tokio::task::spawn_blocking(move || {
            while !child_cancel.requested.load(Ordering::Acquire) {
                std::thread::yield_now();
            }
            std::thread::sleep(Duration::from_millis(25));
            child_closed.store(true, Ordering::Release);
            Ok(json!({"state":"closed"}))
        });
        let (_progress, mut observed) = watch::channel(Progress {
            deadline: Instant::now() + Duration::from_secs(60),
            phase: None,
            initialized: None,
            shutdown: None,
            sync: None,
            capacity: None,
        });
        let result = collect(
            &mut worker,
            &mut observed,
            &cancel,
            async {},
            Duration::from_secs(1),
            |_| {},
        )
        .await;
        assert!(matches!(
            result,
            Collected::Joined {
                terminal: Ok(Ok(_)),
                interrupted: false
            }
        ));
        assert!(closed.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn grace_expiry_never_reports_or_aborts_a_blocked_worker() {
        let cancel = Cancellation::new();
        let (release, wait) = std::sync::mpsc::sync_channel(1);
        let mut worker = tokio::task::spawn_blocking(move || {
            wait.recv().unwrap();
            Ok(json!({"state":"closed"}))
        });
        let (_progress, mut observed) = watch::channel(Progress {
            deadline: Instant::now() + Duration::from_secs(60),
            phase: None,
            initialized: None,
            shutdown: None,
            sync: None,
            capacity: None,
        });
        let result = collect(
            &mut worker,
            &mut observed,
            &cancel,
            async {},
            Duration::from_millis(10),
            |_| {},
        )
        .await;
        assert!(matches!(result, Collected::Unjoined));
        assert!(!worker.is_finished());
        assert!(cancel.requested.load(Ordering::Acquire));
        // The fixture can unblock its own worker. The CLI instead fail-stops;
        // neither drops its driver while this result remains unresolved.
        release.send(()).unwrap();
        worker.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn stop_revokes_a_listener_published_after_the_supervisor_snapshot() {
        use crate::interfaces::management::{Management, Profile};
        use bigbrotr::runtime::policy_authority::{
            LeaseResponse, PolicyLeaseTracker, PolicyLeaseTtl,
        };
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let mut root = crate::config::load(
            &std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../config/recipes/archive-only.toml"),
        )
        .unwrap();
        let reserved = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = reserved.local_addr().unwrap();
        root.values
            .insert("administration.listen.port".into(), json!(address.port()));
        let budget = super::super::budget(&root, false, false).unwrap();
        let dataset = "01010101010101010101010101010101".parse().unwrap();
        let incarnation = "02020202020202020202020202020202".parse().unwrap();
        let epoch = bigbrotr_model::PolicyEpoch::new(1);
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
        let mut management = Management::start(
            &root,
            budget.clone(),
            lease.delivery_window(epoch).unwrap(),
            None,
            Profile::Initial {
                public_routes: true,
            },
        )
        .await
        .unwrap();
        let shutdown = management.shutdown_handle();
        let client = tokio::spawn(async move {
            let mut socket = tokio::net::TcpStream::connect(address).await.unwrap();
            socket.write_all(format!(
                "GET /management/v1/me HTTP/1.1\r\nHost: {address}\r\nCookie: BigBrotrLocalSession={}\r\n\r\n",
                "11".repeat(32),
            ).as_bytes()).await.unwrap();
            let mut bytes = Vec::new();
            socket.read_to_end(&mut bytes).await.unwrap();
            bytes
        });
        // The real queued request is a barrier: HTTP owns a live connection,
        // while the dispatcher retains the request and its memory reservation.
        let work = tokio::time::timeout(Duration::from_secs(1), management.next())
            .await
            .unwrap();
        let cancel = Cancellation::new();
        let (release, wait) = std::sync::mpsc::sync_channel(1);
        let mut worker = tokio::task::spawn_blocking(move || {
            wait.recv().unwrap();
            Ok(json!({"state":"closed"}))
        });
        let (progress, mut observed) = watch::channel(Progress {
            deadline: Instant::now() + Duration::from_secs(60),
            phase: None,
            initialized: Some(json!({"state":"initialized"})),
            shutdown: None,
            sync: None,
            capacity: None,
        });
        let result = collect(
            &mut worker,
            &mut observed,
            &cancel,
            async {},
            Duration::from_millis(10),
            // collect already copied Progress; this publication must still be
            // seen when the immediately ready stop wins its biased select.
            |_| progress.send_modify(|p| p.shutdown = Some(shutdown.clone())),
        )
        .await;
        let still_running = !worker.is_finished();
        let closed = tokio::time::timeout(Duration::from_secs(1), client).await;
        release.send(()).unwrap();
        worker.await.unwrap().unwrap();
        management.stop(Duration::from_secs(1)).await.unwrap();
        assert!(matches!(result, Collected::Unjoined));
        assert!(still_running);
        assert!(cancel.requested.load(Ordering::Acquire));
        assert!(closed.unwrap().unwrap().is_empty());
        assert_eq!(budget.snapshot().active_permits(), 1);
        drop(work);
        assert_eq!(budget.snapshot().active_permits(), 0);
    }
}
