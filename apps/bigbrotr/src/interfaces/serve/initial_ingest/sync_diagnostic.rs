//! Bounded caller progress, never authority or evidence of a database commit.
//! Stage updates are silent: no timers, notifications, allocation or I/O.
use super::Progress;
use serde_json::{Value, json};
use std::{future::Future, time::Duration};
use tokio::{sync::watch, time::Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Stage {
    Selected,
    Nominate,
    Claim,
    Scope,
    Source,
    Visibility,
    ObservationPreparation,
    ObservationLookup,
    ObservationAppend,
    ObservationRelookup,
    RelayCollection,
    EventPreparation,
    Certification,
    AdmissionPreparation,
    Admission,
}
impl Stage {
    fn label(self) -> &'static str {
        match self {
            Self::Selected => "selected",
            Self::Nominate => "nominate",
            Self::Claim => "claim",
            Self::Scope => "scope",
            Self::Source => "source",
            Self::Visibility => "visibility",
            Self::ObservationPreparation => "observation-preparation",
            Self::ObservationLookup => "observation-lookup",
            Self::ObservationAppend => "observation-append",
            Self::ObservationRelookup => "observation-relookup",
            Self::RelayCollection => "relay-collection",
            Self::EventPreparation => "event-preparation",
            Self::Certification => "certification",
            Self::AdmissionPreparation => "admission-preparation",
            Self::Admission => "admission",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Window {
    /// Start of this operation's safe-point window. A later operation may use
    /// renewed authority; an already polled operation never replaces its window.
    pub selected: Instant,
    /// Taken immediately after the existing suspend-aware lease check returns;
    /// this is not a conversion of that authority clock into Tokio's clock.
    pub authority_checked: Instant,
    pub renewal_timeout: Duration,
    pub authority_remaining: Duration,
    pub due_remaining: Duration,
    pub maximum: Duration,
    pub renewal_due: Instant,
    pub parent_deadline: Instant,
    pub dispatch_deadline: Instant,
    pub outer_deadline: Instant,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Record {
    window: Window,
    active: bool,
    stage: Stage,
    entered: Instant,
    returned: Option<Instant>,
    /// Zero-based position in this dispatch's at-most-three retained frames.
    frame: Option<u8>,
    request: Option<u64>,
    /// Actual delivered Durable event outcome, retained across dispatches of
    /// this same owning lifetime. It is not reconstructed from SQL or a cursor.
    last_delivered_admission: Option<u64>,
}

#[derive(Clone, Copy)]
pub(super) struct Snapshot {
    record: Record,
    captured: Instant,
}
impl Snapshot {
    /// Formatting occurs only on failure, after the fixed-size capture. No
    /// endpoint, event, principal, credential or absolute source path is stored.
    pub(super) fn report(self, reason: &'static str) -> Value {
        let r = self.record;
        let w = r.window;
        let offset = |instant| offset_us(w.selected, instant);
        json!({
            "format": "bigbrotr-initial-sync-diagnostic/1",
            "reason": reason,
            "scope": "caller-progress-not-commit-outcome",
            "timing": "monotonic-offset-microseconds-saturating",
            "captured_us": offset(self.captured),
            "authority_checked_us": offset(w.authority_checked),
            "renewal_timeout_us": micros(w.renewal_timeout),
            "authority_remaining_us": micros(w.authority_remaining),
            "due_remaining_us": micros(w.due_remaining),
            "maximum_us": micros(w.maximum),
            "renewal_due_us": offset(w.renewal_due),
            "parent_deadline_us": offset(w.parent_deadline),
            "dispatch_deadline_us": offset(w.dispatch_deadline),
            "outer_deadline_us": offset(w.outer_deadline),
            "stage": r.stage.label(),
            "entered_us": offset(r.entered),
            "returned_us": r.returned.map(offset),
            "frame_ordinal": r.frame,
            "request_sequence": r.request,
            "last_delivered_admission_sequence": r.last_delivered_admission,
        })
    }
}

fn micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}
fn offset_us(origin: Instant, value: Instant) -> i64 {
    if value >= origin {
        i64::try_from(micros(value.duration_since(origin))).unwrap_or(i64::MAX)
    } else {
        -i64::try_from(micros(origin.duration_since(value))).unwrap_or(i64::MAX)
    }
}

#[derive(Clone, Copy)]
pub(super) struct Trace<'a>(&'a watch::Sender<Progress>);
impl<'a> Trace<'a> {
    pub(super) fn new(progress: &'a watch::Sender<Progress>) -> Self {
        Self(progress)
    }
    pub(super) fn begin(self, window: Window) {
        self.0.send_if_modified(|progress| {
            let last_delivered_admission = progress.sync.and_then(|r| r.last_delivered_admission);
            progress.sync = Some(Record {
                window,
                active: true,
                stage: Stage::Selected,
                entered: window.selected,
                returned: None,
                frame: None,
                request: None,
                last_delivered_admission,
            });
            // Tokio watch explicitly supports silent modifications. Readers
            // sample this at an existing failure boundary, not via changed().
            false
        });
    }
    fn update(self, change: impl FnOnce(&mut Record)) {
        self.0.send_if_modified(|progress| {
            if let Some(record) = &mut progress.sync {
                change(record);
            }
            false
        });
    }
    pub(super) fn enter(self, stage: Stage, frame: Option<u8>, request: Option<u64>) {
        let now = Instant::now();
        self.update(|r| {
            r.stage = stage;
            r.entered = now;
            r.returned = None;
            r.frame = frame;
            r.request = request;
        });
    }
    pub(super) fn returned(self) {
        let now = Instant::now();
        self.update(|r| r.returned = Some(now));
    }
    pub(super) fn delivered_admission(self, request: u64) {
        self.update(|r| r.last_delivered_admission = Some(request));
    }
    /// Renewal has its own parent phase. Do not attribute its failure to the
    /// previously completed Sync operation; preserve the delivered watermark.
    pub(super) fn pause(self) {
        self.update(|r| r.active = false);
    }
    pub(super) fn run<T>(
        self,
        stage: Stage,
        frame: Option<u8>,
        request: Option<u64>,
        operation: impl FnOnce() -> T,
    ) -> T {
        self.enter(stage, frame, request);
        let result = operation();
        self.returned();
        result
    }
    pub(super) async fn wait<T>(
        self,
        stage: Stage,
        frame: Option<u8>,
        request: Option<u64>,
        future: impl Future<Output = T>,
    ) -> T {
        self.enter(stage, frame, request);
        let result = future.await;
        // A returned Err is still a returned caller operation, not a pending
        // await. Dropping this future never executes this completion marker.
        self.returned();
        result
    }
    pub(super) fn finish(self, result: super::Result<()>) -> super::Result<()> {
        let snapshot = capture(&self.0.borrow());
        self.update(|r| r.active = false);
        result.map_err(|(code, text)| {
            let text = match snapshot {
                Some(snapshot) => format!(
                    "{text}; sync_dispatch={}",
                    snapshot.report("operation-error")
                ),
                None => text,
            };
            (code, text)
        })
    }
}

/// Copy under the caller's existing watch borrow. The watchdog must do this
/// before cancellation/grace can let the owner advance to another stage.
pub(super) fn capture(progress: &Progress) -> Option<Snapshot> {
    progress.sync.filter(|r| r.active).map(|record| Snapshot {
        record,
        captured: Instant::now(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        pin::pin,
        sync::atomic::AtomicBool,
        task::{Context, Poll, Waker},
    };

    #[tokio::test]
    async fn an_expired_operation_never_polls_a_new_effect() {
        let (progress, _, _) = fixture();
        let polled = std::cell::Cell::new(false);
        let trace = Trace::new(&progress);
        let result = super::super::commands::sync_operation(
            trace,
            Instant::now(),
            &AtomicBool::new(false),
            Stage::Admission,
            Some(0),
            Some(4),
            async {
                polled.set(true);
            },
        )
        .await;
        assert!(result.is_err());
        assert!(!polled.get());
        let record = capture(&progress.borrow()).unwrap().record;
        assert_eq!(record.stage, Stage::Admission);
        assert_eq!(record.request, Some(4));
        assert_eq!(record.returned, None);
        assert_eq!(record.last_delivered_admission, None);
        let result = super::super::commands::sync_operation(
            trace,
            Instant::now() + Duration::from_secs(5),
            &AtomicBool::new(true),
            Stage::Admission,
            Some(0),
            Some(4),
            async {
                polled.set(true);
            },
        )
        .await;
        assert!(
            result
                .unwrap_err()
                .1
                .starts_with("initial Sync cancelled before dispatch")
        );
        assert!(!polled.get());
    }

    #[tokio::test]
    async fn an_in_flight_admission_keeps_its_deadline_and_unknown_outcome() {
        let (progress, _, _) = fixture();
        let trace = Trace::new(&progress);
        let polls = std::cell::Cell::new(0);
        let deadline = Instant::now() + Duration::from_millis(25);
        let result = super::super::commands::sync_operation(
            trace,
            deadline,
            &AtomicBool::new(false),
            Stage::Admission,
            Some(0),
            Some(4),
            async {
                polls.set(polls.get() + 1);
                std::future::pending::<()>().await;
            },
        )
        .await;
        assert!(result.is_err());
        assert_eq!(polls.get(), 1);
        let error = trace.finish(result).unwrap_err();
        assert!(
            error
                .1
                .starts_with("initial Sync deadline; preserve original outcome")
        );
        let report: Value =
            serde_json::from_str(error.1.split_once("sync_dispatch=").unwrap().1).unwrap();
        assert_eq!(report["request_sequence"], 4);
        assert_eq!(report["returned_us"], Value::Null);
        assert_eq!(report["last_delivered_admission_sequence"], Value::Null);
    }

    #[test]
    fn renewal_does_not_relabel_prior_progress_as_a_new_operation() {
        let (progress, _, mut window) = fixture();
        let trace = Trace::new(&progress);
        trace.enter(Stage::Admission, Some(0), Some(4));
        trace.returned();
        trace.delivered_admission(4);
        trace.pause();
        assert!(capture(&progress.borrow()).is_none());
        window.selected = Instant::now();
        trace.begin(window);
        trace.enter(Stage::Certification, Some(2), None);
        let record = capture(&progress.borrow()).unwrap().record;
        assert_eq!(record.stage, Stage::Certification);
        assert_eq!(record.request, None);
        assert_eq!(record.returned, None);
        assert_eq!(record.last_delivered_admission, Some(4));
    }

    pub(super) fn fixture() -> (watch::Sender<Progress>, watch::Receiver<Progress>, Window) {
        let start = Instant::now();
        let window = Window {
            selected: start,
            authority_checked: start,
            renewal_timeout: Duration::from_secs(1),
            authority_remaining: Duration::from_secs(4),
            due_remaining: Duration::from_millis(700),
            maximum: Duration::from_millis(700),
            renewal_due: start + Duration::from_millis(700),
            parent_deadline: start + Duration::from_millis(710),
            dispatch_deadline: start + Duration::from_millis(720),
            outer_deadline: start + Duration::from_millis(730),
        };
        let (sender, receiver) = watch::channel(Progress {
            deadline: window.parent_deadline,
            phase: None,
            initialized: None,
            shutdown: None,
            sync: None,
        });
        Trace::new(&sender).begin(window);
        (sender, receiver, window)
    }

    #[test]
    fn pending_admission_and_following_certification_are_distinct() {
        let (progress, _, _) = fixture();
        let trace = Trace::new(&progress);
        let mut cx = Context::from_waker(Waker::noop());
        {
            let mut pending = pin!(trace.wait(
                Stage::Admission,
                Some(0),
                Some(4),
                std::future::pending::<()>(),
            ));
            assert!(pending.as_mut().poll(&mut cx).is_pending());
            let record = capture(&progress.borrow()).unwrap().record;
            assert_eq!(record.stage, Stage::Admission);
            assert_eq!(record.request, Some(4));
            assert_eq!(record.returned, None);
            assert_eq!(record.last_delivered_admission, None);
        }
        let mut returned =
            pin!(trace.wait(Stage::Admission, Some(0), Some(4), std::future::ready(()),));
        assert!(returned.as_mut().poll(&mut cx).is_ready());
        trace.delivered_admission(4);
        let mut duplicate = pin!(trace.wait(
            Stage::Certification,
            Some(2),
            None,
            std::future::pending::<()>(),
        ));
        assert!(duplicate.as_mut().poll(&mut cx).is_pending());
        let report = capture(&progress.borrow()).unwrap().report("test");
        assert_eq!(report["stage"], "certification");
        assert_eq!(report["frame_ordinal"], 2);
        assert_eq!(report["request_sequence"], Value::Null);
        assert_eq!(report["returned_us"], Value::Null);
        assert_eq!(report["last_delivered_admission_sequence"], 4);
    }

    #[test]
    fn dropping_a_pending_stage_never_marks_it_returned() {
        let (progress, _, _) = fixture();
        let trace = Trace::new(&progress);
        {
            let mut future = pin!(trace.wait(
                Stage::Admission,
                Some(0),
                Some(4),
                std::future::pending::<()>(),
            ));
            assert!(
                future
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
        }
        let before = capture(&progress.borrow()).unwrap().record;
        let error = trace
            .finish(Err((4, "original timeout".into())))
            .unwrap_err();
        assert_eq!(error.0, 4);
        assert!(error.1.starts_with("original timeout; sync_dispatch="));
        let report: Value =
            serde_json::from_str(error.1.split_once("sync_dispatch=").unwrap().1).unwrap();
        assert_eq!(report["stage"], "admission");
        assert_eq!(report["returned_us"], Value::Null);
        assert_eq!(report["request_sequence"], 4);
        assert_eq!(before.returned, None);
        assert_eq!(before.last_delivered_admission, None);
    }

    #[test]
    fn silent_updates_preserve_deadlines_and_do_not_notify() {
        let (progress, mut observed, window) = fixture();
        let trace = Trace::new(&progress);
        let sequence = 4_u64;
        assert!(!observed.has_changed().unwrap());
        observed.borrow_and_update();
        for _ in 0..1000 {
            trace.enter(Stage::Admission, Some(0), Some(sequence));
            trace.returned();
        }
        assert!(!observed.has_changed().unwrap());
        let value = observed.borrow();
        assert_eq!(value.deadline, window.parent_deadline);
        assert!(value.phase.is_none());
        assert_eq!(value.sync.unwrap().window, window);
        assert_eq!(value.sync.unwrap().request, Some(sequence));
        assert!(!std::mem::needs_drop::<Record>());
        assert!(!std::mem::needs_drop::<Snapshot>());
        assert!(std::mem::size_of::<Option<Record>>() <= 320);
    }

    #[test]
    fn returned_error_is_distinct_from_pending() {
        let (progress, _, _) = fixture();
        let mut future = pin!(Trace::new(&progress).wait(
            Stage::Claim,
            None,
            None,
            std::future::ready(Err::<(), _>("refused")),
        ));
        assert_eq!(
            future
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(Err("refused"))
        );
        assert!(
            capture(&progress.borrow())
                .unwrap()
                .record
                .returned
                .is_some()
        );
    }

    #[test]
    fn completed_dispatch_does_not_leak_into_later_watchdogs() {
        let (progress, _, window) = fixture();
        let trace = Trace::new(&progress);
        trace.enter(Stage::Admission, Some(0), Some(4));
        trace.returned();
        trace.delivered_admission(4);
        assert!(trace.finish(Ok(())).is_ok());
        assert!(capture(&progress.borrow()).is_none());
        trace.begin(window);
        let next = capture(&progress.borrow()).unwrap().record;
        assert_eq!(next.stage, Stage::Selected);
        assert_eq!(next.request, None);
        assert_eq!(next.returned, None);
        assert_eq!(next.last_delivered_admission, Some(4));
    }
}
