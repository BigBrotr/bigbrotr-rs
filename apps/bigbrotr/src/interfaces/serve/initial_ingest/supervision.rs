//! Retain the one native owner through actual completion or explicit fail-stop.
use super::{Cancellation, Progress, Result};
use serde_json::Value;
use std::{future::Future, time::Duration};
use tokio::{
    sync::watch,
    task::{JoinError, JoinHandle},
    time::Instant,
};

pub(super) enum Collected {
    Joined {
        terminal: std::result::Result<Result<Value>, JoinError>,
        interrupted: bool,
    },
    Unjoined,
}

pub(super) async fn collect(
    worker: &mut JoinHandle<Result<Value>>,
    observed: &mut watch::Receiver<Progress>,
    cancellation: &Cancellation,
    stop: impl Future<Output = ()>,
    grace: Duration,
    mut emit: impl FnMut(Value),
) -> Collected {
    tokio::pin!(stop);
    let mut reported = false;
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
                (Instant::now() >= observed.borrow().deadline).then_some("owner deadline")
            },
            result = &mut *worker => break (result, false),
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
            match tokio::time::timeout(grace, &mut *worker).await {
                Ok(terminal) => break (terminal, reason != "stop" && reason != "owner ended"),
                Err(_) => return Collected::Unjoined,
            }
        }
    };
    if let Some(shutdown) = &observed.borrow().shutdown {
        shutdown.stop_admission();
    }

    Collected::Joined {
        terminal,
        interrupted,
    }
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
    async fn published_phase_wins_an_expired_cached_deadline() {
        let cancel = Cancellation::new();
        let (progress, mut observed) = watch::channel(Progress {
            deadline: Instant::now() - Duration::from_secs(1),
            initialized: Some(json!({"state":"initialized"})),
            shutdown: None,
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
            initialized: None,
            shutdown: None,
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
            initialized: None,
            shutdown: None,
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
        let budget = super::super::budget(&root, false).unwrap();
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
            initialized: Some(json!({"state":"initialized"})),
            shutdown: None,
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
