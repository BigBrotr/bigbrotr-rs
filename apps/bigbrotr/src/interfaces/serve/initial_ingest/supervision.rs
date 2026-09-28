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
            match tokio::time::timeout(grace, &mut *worker).await {
                Ok(terminal) => break (terminal, reason != "stop" && reason != "owner ended"),
                Err(_) => return Collected::Unjoined,
            }
        }
    };

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
}
