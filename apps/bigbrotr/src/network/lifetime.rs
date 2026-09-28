use super::TransportError;
use crate::runtime::policy_authority::ActionWindow;
use std::{
    future::Future,
    io,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    sync::watch,
    time::{Instant, MissedTickBehavior},
};

struct State {
    window: ActionWindow,
    deadline: Instant,
    failed: Option<TransportError>,
}

/// Dropping this unique handle cancels its finite attempt, including pending IO.
/// It cannot renew an action or cancel a later replacement connection.
pub struct CancelAttempt {
    state: Arc<Mutex<State>>,
    _signal: watch::Sender<()>,
}
impl Drop for CancelAttempt {
    fn drop(&mut self) {
        if let Ok(mut state) = self.state.lock() {
            state.failed.get_or_insert(TransportError::Cancelled);
        }
        // The final sender drop wakes the enclosing operation's receiver.
    }
}

#[derive(Clone)]
pub(super) struct Gate {
    state: Arc<Mutex<State>>,
    stopped: watch::Receiver<()>,
}
impl Gate {
    pub fn new(window: ActionWindow, deadline: Instant) -> (Self, CancelAttempt) {
        let state = Arc::new(Mutex::new(State {
            window,
            deadline,
            failed: None,
        }));
        let (signal, stopped) = watch::channel(());
        (
            Self {
                state: state.clone(),
                stopped,
            },
            CancelAttempt {
                state,
                _signal: signal,
            },
        )
    }

    pub fn check(&self) -> Result<(), TransportError> {
        let mut state = self.state.lock().map_err(|_| TransportError::Authority)?;
        if let Some(error) = state.failed {
            return Err(error);
        }
        let result = if Instant::now() >= state.deadline {
            Err(TransportError::Deadline)
        } else {
            state.window.check().map_err(|_| TransportError::Authority)
        };
        if let Err(error) = result {
            state.failed = Some(error);
        }
        result
    }

    pub fn check_io(&self) -> io::Result<()> {
        self.check().map_err(|_| {
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                "acquisition operation no longer permitted",
            )
        })
    }

    /// Owns, polls and drops all work in this future. No spawned watcher/driver
    /// survives cancellation. The periodic sample covers idle IO after host
    /// suspension; actual IO checks the suspend-aware window on every poll.
    pub async fn supervise<T>(
        &self,
        work: impl Future<Output = Result<T, TransportError>>,
    ) -> Result<T, TransportError> {
        let mut stopped = self.stopped.clone();
        let mut clock = tokio::time::interval(Duration::from_millis(50));
        clock.set_missed_tick_behavior(MissedTickBehavior::Skip);
        tokio::pin!(work);
        // select! may resume directly in this branch after Pending, without
        // running the loop's check. Recheck before every poll so a late DNS
        // reply cannot initiate another exchange under an expired action.
        let checked = std::future::poll_fn(|cx| {
            self.check()?;
            work.as_mut().poll(cx)
        });
        tokio::pin!(checked);
        loop {
            self.check()?;
            tokio::select! {
                biased;
                _ = stopped.changed() => { self.check()?; return Err(TransportError::Cancelled); }
                _ = clock.tick() => {}
                result = &mut checked => { self.check()?; return result; }
            }
        }
    }
}
