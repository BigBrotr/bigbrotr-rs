//! One owned ClickHouse operation on a fully joined, dedicated executor.
//!
//! The pinned client and Hyper spawn tasks not owned by the request future.
//! Dropping that future therefore starts shutdown; it does not finish it. This
//! lane drops the complete runtime (including its blocking pool) and joins its
//! OS thread before returning an outcome. No timeout/background runtime shutdown
//! or detached join substitutes for cessation.
//!
//! The generic API stays crate-private. Its adapter must construct clients inside
//! the factory and return only inert owned data/proofs: never a client, cursor,
//! runtime handle, future or closure capable of launching more transport work.
//! A deadline/cancel/panic can leave a named remote query or immutable insert
//! running. Remote cancellation and complete-plan verification are separate.
//!
//! `join`, `try_join` after readiness, and Drop may block, including on DNS or a
//! thread-local destructor. A timer on that same calling thread cannot guarantee
//! bounded shutdown. Integration must retain an independent supervisor (the
//! initial Ingest parent supervises its blocking Control/native worker) or remain
//! blocked while holding custody and capacity. This component never claims a
//! process kill or remote cancellation on its own.
use crate::{runtime::OwnedPermit, storage::indexer::custody::HeldIndexCustody};
use std::{
    fs::File,
    future::Future,
    io,
    sync::Arc,
    thread::{self, JoinHandle},
    time::Instant,
};
use tokio::{runtime::Builder, sync::watch};

const THREAD_STACK_BYTES: usize = 2 * 1024 * 1024;
/// One executor thread and at most one blocking/DNS thread. Client buffers,
/// runtime/socket overhead and owned input/output bytes need additional credit;
/// this floor is not an RSS or complete-operation memory estimate.
pub(crate) const INDEX_LANE_STACK_RESERVATION: u64 = 2 * THREAD_STACK_BYTES as u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LaneError {
    InvalidGrant,
    Custody,
    ThreadUnavailable,
    AlreadyJoined,
}

/// Only observable after complete local executor destruction and OS-thread join.
/// Completed(T) preserves the operation's own typed error/result interpretation;
/// none of the other variants establish absence of a remote durable effect.
#[derive(Debug)]
pub(crate) enum LaneOutcome<T> {
    Completed(T),
    Cancelled,
    Deadline,
    RuntimeUnavailable,
    Panicked,
}

struct Retained {
    custody: File,
    permit: OwnedPermit,
}

/// No queue and no reusable ambient runtime: one admitted operation/whole batch.
/// Ownership remains here across cancellation and while the executor tears down.
pub(crate) struct OwnedIndexLane<T: Send + 'static> {
    worker: Option<JoinHandle<LaneOutcome<T>>>,
    cancel: watch::Sender<bool>,
    retained: Option<Arc<Retained>>,
}

/// Output allocations keep their actual permit after the executor is gone.
/// Field order deliberately drops the outcome before releasing its reservation.
pub(crate) struct JoinedIndexLane<T> {
    outcome: LaneOutcome<T>,
    retained: Arc<Retained>,
}
impl<T> JoinedIndexLane<T> {
    pub(crate) fn outcome(&self) -> &LaneOutcome<T> {
        &self.outcome
    }

    /// Transfer the real reservation with the inert result. The adapter retains
    /// it until all result buffers are freed or covered by another admitted
    /// reservation. Physical proof types retain their own actual custody clone
    /// while they cross later SQL boundaries; a binding digest is not custody.
    pub(crate) fn into_parts(self) -> (LaneOutcome<T>, OwnedPermit) {
        let Self { outcome, retained } = self;
        let Retained { custody, permit } = Arc::try_unwrap(retained)
            .unwrap_or_else(|_| panic!("joined Index lane still has an internal retention owner"));
        drop(custody);
        (outcome, permit)
    }
}

impl<T: Send + 'static> OwnedIndexLane<T> {
    pub(crate) fn start<F, Work>(
        custody: &HeldIndexCustody,
        permit: OwnedPermit,
        deadline: Instant,
        factory: F,
    ) -> Result<Self, LaneError>
    where
        F: FnOnce() -> Work + Send + 'static,
        Work: Future<Output = T> + 'static,
    {
        check_grant(&permit)?;
        let retained = custody.retain_lane().map_err(|_| LaneError::Custody)?;
        Self::start_retained(retained, permit, deadline, factory, |job| {
            thread::Builder::new()
                .name("bigbrotr-index-lane".into())
                .stack_size(THREAD_STACK_BYTES)
                .spawn(job)
        })
    }

    // Private fault-injection seam. Tests retain an actual flock description,
    // never manufacture HeldIndexCustody or a physical publication proof.
    fn start_retained<F, Work, Spawn>(
        custody: File,
        permit: OwnedPermit,
        deadline: Instant,
        factory: F,
        spawn: Spawn,
    ) -> Result<Self, LaneError>
    where
        F: FnOnce() -> Work + Send + 'static,
        Work: Future<Output = T> + 'static,
        Spawn: FnOnce(
            Box<dyn FnOnce() -> LaneOutcome<T> + Send>,
        ) -> io::Result<JoinHandle<LaneOutcome<T>>>,
    {
        check_grant(&permit)?;
        let retained = Arc::new(Retained { custody, permit });
        let worker_retention = Arc::clone(&retained);
        let (cancel, receiver) = watch::channel(false);
        let worker = spawn(Box::new(move || {
            run(worker_retention, receiver, deadline, factory)
        }))
        .map_err(|_| LaneError::ThreadUnavailable)?;
        Ok(Self {
            worker: Some(worker),
            cancel,
            retained: Some(retained),
        })
    }

    /// Request local future cancellation. Neither receipt nor capacity is
    /// returned here: hidden tasks and blocking work can still be alive.
    pub(crate) fn request_cancel(&self) {
        self.cancel.send_replace(true);
    }

    /// None means the thread has not reached join readiness. Readiness itself
    /// is never a cessation proof: the subsequent actual join can still wait
    /// for thread-local destructors. Do not use this as a bounded wait promise.
    pub(crate) fn try_join(&mut self) -> Result<Option<JoinedIndexLane<T>>, LaneError> {
        if !self
            .worker
            .as_ref()
            .ok_or(LaneError::AlreadyJoined)?
            .is_finished()
        {
            return Ok(None);
        }
        self.finish().map(Some)
    }

    /// Block until the actual dedicated thread is joined. External supervision
    /// is required when the process must enforce a finite shutdown grace.
    pub(crate) fn join(mut self) -> Result<JoinedIndexLane<T>, LaneError> {
        self.finish()
    }

    fn finish(&mut self) -> Result<JoinedIndexLane<T>, LaneError> {
        let worker = self.worker.take().ok_or(LaneError::AlreadyJoined)?;
        let outcome = worker.join().unwrap_or_else(|_| LaneOutcome::Panicked);
        Ok(JoinedIndexLane {
            outcome,
            retained: self
                .retained
                .take()
                .expect("unjoined lane retains custody and capacity"),
        })
    }
}

impl<T: Send + 'static> Drop for OwnedIndexLane<T> {
    fn drop(&mut self) {
        self.request_cancel();
        if let Some(worker) = self.worker.take() {
            // The parent retention remains a field until this actual join and
            // any discarded result/panic payload have been destroyed.
            let _ = worker.join();
        }
    }
}

fn check_grant(permit: &OwnedPermit) -> Result<(), LaneError> {
    let units = permit.request().units();
    if units.items() == 0
        || units.native_bytes() < INDEX_LANE_STACK_RESERVATION
        || units.control_bytes() != 0
    {
        return Err(LaneError::InvalidGrant);
    }
    Ok(())
}

fn run<F, Work, T>(
    retained: Arc<Retained>,
    mut cancel: watch::Receiver<bool>,
    deadline: Instant,
    factory: F,
) -> LaneOutcome<T>
where
    F: FnOnce() -> Work,
    Work: Future<Output = T>,
{
    // Declare retention before Runtime: panic unwinding drops Runtime first.
    let retention = retained;
    let runtime = match Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .thread_name("bigbrotr-index-blocking")
        .thread_stack_size(THREAD_STACK_BYTES)
        .build()
    {
        Ok(runtime) => runtime,
        Err(_) => return LaneOutcome::RuntimeUnavailable,
    };
    let outcome = runtime.block_on(async move {
        if *cancel.borrow() { return LaneOutcome::Cancelled; }
        if Instant::now() >= deadline { return LaneOutcome::Deadline; }
        // Creating/polling the client future happens inside this runtime; its
        // hidden Tokio/Hyper tasks cannot adopt the caller's executor.
        let work = factory();
        tokio::pin!(work);
        tokio::select! {
            biased;
            _ = cancel.changed() => LaneOutcome::Cancelled,
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => LaneOutcome::Deadline,
            result = &mut work => LaneOutcome::Completed(result),
        }
    });
    // Complete Drop waits for hidden async tasks and the bounded blocking pool.
    // It may wait indefinitely; never replace this with shutdown_timeout.
    drop(runtime);
    drop(retention);
    outcome
}

#[cfg(test)]
#[path = "lane/tests.rs"]
mod tests;
