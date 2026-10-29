use super::*;
use std::{cell::Cell, rc::Rc};

#[derive(Clone)]
pub(crate) struct ManualClock(pub(crate) Rc<Cell<Result<AuthorityTick, LeaseError>>>);
impl sealed::Sealed for ManualClock {}
impl AuthorityClock for ManualClock {
    fn read(&self) -> Result<AuthorityTick, LeaseError> {
        self.0.get()
    }
}
impl std::ops::Add<Duration> for AuthorityTick {
    type Output = Self;
    fn add(self, value: Duration) -> Self {
        Self(self.0.checked_add(value).unwrap())
    }
}
macro_rules! at {
    ($tracker:expr, $now:expr, $method:ident($($argument:expr),*)) => {{
        let tracker = &mut $tracker;
        tracker.clock.0.set(Ok($now));
        tracker.$method($($argument),*)
    }};
}
fn dataset(n: u8) -> DatasetId {
    DatasetId::try_from([n; 16]).unwrap()
}
fn process(n: u8) -> ProcessIncarnation {
    ProcessIncarnation::try_from([n; 16]).unwrap()
}
fn epoch(n: u64) -> PolicyEpoch {
    PolicyEpoch::new(n)
}
fn seconds(n: u64) -> Duration {
    Duration::from_secs(n)
}
fn tracker(at: AuthorityTick) -> PolicyLeaseTracker<ManualClock> {
    PolicyLeaseTracker::with_clock(
        dataset(1),
        process(1),
        epoch(1),
        PolicyLeaseTtl::new(seconds(5)).unwrap(),
        ManualClock(Rc::new(Cell::new(Ok(at)))),
    )
    .unwrap()
}
fn response(n: u64) -> LeaseResponse {
    LeaseResponse {
        dataset: dataset(1),
        process_incarnation: process(1),
        policy_epoch: epoch(n),
        ttl: seconds(5),
    }
}
fn installed(at: AuthorityTick) -> PolicyLeaseTracker<ManualClock> {
    let mut tracker = tracker(at);
    let refresh = tracker.begin_refresh().unwrap();
    tracker.complete_refresh(&refresh, response(1)).unwrap();
    tracker
}

pub(crate) fn installed_with_clock() -> (PolicyLeaseTracker<ManualClock>, ManualClock) {
    let owner = installed(AuthorityTick(seconds(100)));
    let clock = owner.clock.clone();
    (owner, clock)
}

#[test]
fn action_reply_cannot_add_its_wait_or_subsequent_control_renewals_to_its_lifetime() {
    let start = AuthorityTick(seconds(100));
    let mut owner = installed(start);
    let pending = owner.begin_action(epoch(1)).unwrap();
    owner.clock.0.set(Ok(start + seconds(2)));
    let mut action = pending.complete(seconds(3)).unwrap();
    let refresh = owner.begin_refresh().unwrap();
    owner.complete_refresh(&refresh, response(1)).unwrap();
    owner.clock.0.set(Ok(start + seconds(3)));
    owner.check_delivery(epoch(1)).unwrap();
    assert_eq!(action.check(), Err(LeaseError::Expired));

    let pending = owner.begin_action(epoch(1)).unwrap();
    owner.clock.0.set(Ok(start + seconds(4)));
    assert!(matches!(
        pending.complete(seconds(1)),
        Err(LeaseError::Expired)
    ));
    let pending = owner.begin_action(epoch(1)).unwrap();
    assert!(matches!(
        pending.complete(Duration::ZERO),
        Err(LeaseError::Expired)
    ));
    let pending = owner.begin_action(epoch(1)).unwrap();
    let mut action = pending.complete(seconds(100)).unwrap();
    owner.clock.0.set(Ok(start + seconds(7)));
    assert_eq!(action.check(), Err(LeaseError::Expired));
}

#[test]
fn delivery_window_rechecks_real_clock_without_extending_the_installed_lease() {
    let start = AuthorityTick(seconds(100));
    let mut owner = installed(start);
    owner.clock.0.set(Ok(start + seconds(3)));
    let mut delivery = owner.delivery_window(epoch(1)).unwrap();
    assert_eq!(delivery.epoch(), epoch(1));
    assert_eq!(delivery.check(), Ok(()));
    owner.clock.0.set(Ok(start + seconds(5)));
    assert_eq!(delivery.check(), Err(LeaseError::Expired));
    owner.clock.0.set(Ok(start + seconds(4)));
    assert_eq!(delivery.check(), Err(LeaseError::Expired));
    let mut owner = installed(start);
    let mut delivery = owner.delivery_window(epoch(1)).unwrap();
    owner.clock.0.set(Ok(start + seconds(2)));
    assert_eq!(delivery.check(), Ok(()));
    owner.clock.0.set(Ok(start + seconds(1)));
    assert_eq!(delivery.check(), Err(LeaseError::ClockRegression));
    assert!(owner.delivery_window(epoch(2)).is_err());
}

#[test]
fn delivery_window_renewal_preserves_samples_taken_while_control_was_waiting() {
    let start = AuthorityTick(seconds(100));
    let mut owner = installed(start);
    let mut delivery = owner.delivery_window(epoch(1)).unwrap();
    owner.clock.0.set(Ok(start + Duration::from_millis(4000)));
    let ticket = owner.begin_refresh().unwrap();
    owner.clock.0.set(Ok(start + Duration::from_millis(4200)));
    delivery.check().unwrap();
    owner.clock.0.set(Ok(start + Duration::from_millis(4100)));
    owner.complete_refresh(&ticket, response(1)).unwrap();
    let replacement = owner.delivery_window(epoch(1)).unwrap();
    assert_eq!(
        delivery.renew(replacement),
        Err(LeaseError::ClockRegression)
    );
    owner.clock.0.set(Ok(start + Duration::from_millis(4300)));
    assert_eq!(delivery.check(), Err(LeaseError::ClockRegression));

    let mut owner = installed(start);
    let mut delivery = owner.delivery_window(epoch(1)).unwrap();
    owner.clock.0.set(Ok(start + seconds(3)));
    let ticket = owner.begin_refresh().unwrap();
    owner.complete_refresh(&ticket, response(1)).unwrap();
    delivery
        .renew(owner.delivery_window(epoch(1)).unwrap())
        .unwrap();
    owner.clock.0.set(Ok(start + seconds(6)));
    delivery.check().unwrap();
    owner.clock.0.set(Ok(start + seconds(8)));
    assert_eq!(delivery.check(), Err(LeaseError::Expired));
}

#[test]
fn absent_policy_refuses_and_each_delivery_or_retry_rechecks_expiry() {
    let now = AuthorityTick(seconds(100));
    assert_eq!(
        at!(tracker(now), now, check_delivery(epoch(1))),
        Err(LeaseError::Unavailable)
    );
    let mut tracker = installed(now);
    assert_eq!(
        at!(tracker, now + seconds(4), check_delivery(epoch(1))),
        Ok(())
    );
    assert_eq!(
        at!(tracker, now + seconds(5), check_delivery(epoch(1))),
        Err(LeaseError::Expired)
    );
    assert_eq!(
        at!(tracker, now + seconds(60), check_delivery(epoch(1))),
        Err(LeaseError::Expired)
    );
}

#[test]
fn delayed_reply_consumes_request_start_ttl_including_exact_expiry() {
    let now = AuthorityTick(seconds(100));
    for delay in [5, 6, 60] {
        let mut tracker = tracker(now);
        let refresh = at!(tracker, now, begin_refresh()).unwrap();
        assert_eq!(
            at!(
                tracker,
                now + seconds(delay),
                complete_refresh(&refresh, response(1))
            ),
            Err(LeaseError::Expired)
        );
        assert_eq!(
            at!(tracker, now + seconds(delay), check_delivery(epoch(1))),
            Err(LeaseError::Unavailable)
        );
    }
    let mut tracker = tracker(now);
    let refresh = at!(tracker, now, begin_refresh()).unwrap();
    at!(
        tracker,
        now + seconds(4),
        complete_refresh(&refresh, response(1))
    )
    .unwrap();
    assert_eq!(
        at!(tracker, now + seconds(4), check_delivery(epoch(1))),
        Ok(())
    );
    assert_eq!(
        at!(tracker, now + seconds(5), check_delivery(epoch(1))),
        Err(LeaseError::Expired)
    );
}

#[test]
fn remaining_validity_accounts_for_each_response_without_extending_expiry() {
    let start = AuthorityTick(seconds(100));
    let mut tracker = tracker(start);
    assert_eq!(tracker.remaining_validity(), Err(LeaseError::Unavailable));
    let acquired = tracker.begin_refresh().unwrap();
    at!(
        tracker,
        start + seconds(3),
        complete_refresh(&acquired, response(1))
    )
    .unwrap();
    assert_eq!(tracker.remaining_validity(), Ok(seconds(2)));
    let poll = at!(tracker, start + seconds(4), begin_refresh()).unwrap();
    at!(
        tracker,
        start + seconds(7),
        complete_refresh(&poll, response(1))
    )
    .unwrap();
    assert_eq!(tracker.remaining_validity(), Ok(seconds(2)));
    assert_eq!(
        at!(tracker, start + seconds(9), remaining_validity()),
        Err(LeaseError::Expired)
    );
    assert_eq!(
        at!(tracker, start + seconds(30), remaining_validity()),
        Err(LeaseError::Expired)
    );
}

#[test]
fn foreign_identity_and_oversized_lease_never_install() {
    let now = AuthorityTick(seconds(100));
    let mut foreign_dataset = response(1);
    foreign_dataset.dataset = dataset(2);
    let mut foreign_process = response(1);
    foreign_process.process_incarnation = process(2);
    let mut too_long = response(1);
    too_long.ttl = seconds(60);
    let mut zero = response(1);
    zero.ttl = Duration::ZERO;
    for (response, reason) in [
        (foreign_dataset, LeaseError::WrongDataset),
        (foreign_process, LeaseError::WrongIncarnation),
        (too_long, LeaseError::InvalidTtl),
        (zero, LeaseError::InvalidTtl),
    ] {
        let mut tracker = tracker(now);
        let refresh = at!(tracker, now, begin_refresh()).unwrap();
        assert_eq!(
            at!(tracker, now, complete_refresh(&refresh, response)),
            Err(reason)
        );
        assert_eq!(
            at!(tracker, now, check_delivery(epoch(1))),
            Err(LeaseError::Unavailable)
        );
    }
}

#[test]
fn same_process_trackers_and_replayed_responses_cannot_cross_install() {
    let now = AuthorityTick(seconds(100));
    let mut first = tracker(now);
    let mut second = tracker(now);
    let a = at!(first, now, begin_refresh()).unwrap();
    let b = at!(second, now, begin_refresh()).unwrap();
    assert_eq!(a.request_sequence(), b.request_sequence());
    assert_eq!(a.dataset(), dataset(1));
    assert_eq!(a.process_incarnation(), process(1));
    assert_eq!(
        at!(second, now, complete_refresh(&a, response(1))),
        Err(LeaseError::WrongRefresh)
    );
    at!(second, now, complete_refresh(&b, response(1))).unwrap();
    assert_eq!(
        at!(second, now, complete_refresh(&b, response(1))),
        Err(LeaseError::WrongRefresh)
    );
    at!(first, now, complete_refresh(&a, response(1))).unwrap();
    let c = at!(first, now, begin_refresh()).unwrap();
    assert_eq!(
        at!(first, now, complete_refresh(&a, response(1))),
        Err(LeaseError::WrongRefresh)
    );
    at!(first, now, complete_refresh(&c, response(1))).unwrap();
}

#[test]
fn a_restriction_invalidates_old_work_and_late_old_refreshes() {
    let now = AuthorityTick(seconds(100));
    let mut tracker = installed(now);
    let refresh = at!(tracker, now + seconds(1), begin_refresh()).unwrap();
    at!(
        tracker,
        now + seconds(2),
        observe_epoch(dataset(1), epoch(2))
    )
    .unwrap();
    assert_eq!(tracker.known_epoch(), epoch(2));
    assert_eq!(
        at!(tracker, now + seconds(2), check_delivery(epoch(1))),
        Err(LeaseError::StalePolicy)
    );
    assert_eq!(
        at!(
            tracker,
            now + seconds(3),
            complete_refresh(&refresh, response(1))
        ),
        Err(LeaseError::StalePolicy)
    );
    assert_eq!(
        at!(tracker, now + seconds(3), check_delivery(epoch(2))),
        Err(LeaseError::Unavailable)
    );
    let next = at!(tracker, now + seconds(3), begin_refresh()).unwrap();
    at!(
        tracker,
        now + seconds(3),
        complete_refresh(&next, response(2))
    )
    .unwrap();
    at!(
        tracker,
        now + seconds(3),
        observe_epoch(dataset(1), epoch(1))
    )
    .unwrap();
    assert_eq!(
        at!(tracker, now + seconds(3), check_delivery(epoch(1))),
        Err(LeaseError::StalePolicy)
    );
    assert_eq!(
        at!(tracker, now + seconds(3), check_delivery(epoch(2))),
        Ok(())
    );
}

#[test]
fn timed_out_refresh_retains_one_slot_until_terminal_and_cannot_extend_old_lease() {
    let now = AuthorityTick(seconds(100));
    let mut tracker = installed(now);
    let refresh = at!(tracker, now + seconds(1), begin_refresh()).unwrap();
    at!(tracker, now + seconds(2), refresh_timed_out(&refresh)).unwrap();
    for _ in 0..100 {
        assert!(matches!(
            at!(tracker, now + seconds(2), begin_refresh()),
            Err(LeaseError::RefreshInFlight)
        ));
    }
    assert_eq!(
        at!(tracker, now + seconds(4), check_delivery(epoch(1))),
        Ok(())
    );
    assert_eq!(
        at!(tracker, now + seconds(5), check_delivery(epoch(1))),
        Err(LeaseError::Expired)
    );
    assert_eq!(
        at!(
            tracker,
            now + seconds(5),
            complete_refresh(&refresh, response(1))
        ),
        Err(LeaseError::RefreshTimedOut)
    );
    let next = at!(tracker, now + seconds(5), begin_refresh()).unwrap();
    at!(tracker, now + seconds(5), refresh_failed(&next)).unwrap();
    assert_eq!(
        at!(tracker, now + seconds(5), check_delivery(epoch(1))),
        Err(LeaseError::Expired)
    );
}

#[test]
fn rejected_newer_response_still_invalidates_known_older_authority() {
    let now = AuthorityTick(seconds(100));
    let mut tracker = installed(now);
    let refresh = at!(tracker, now + seconds(1), begin_refresh()).unwrap();
    at!(tracker, now + seconds(2), refresh_timed_out(&refresh)).unwrap();
    assert_eq!(
        at!(
            tracker,
            now + seconds(3),
            complete_refresh(&refresh, response(2))
        ),
        Err(LeaseError::RefreshTimedOut)
    );
    assert_eq!(tracker.known_epoch(), epoch(2));
    assert_eq!(
        at!(tracker, now + seconds(3), check_delivery(epoch(1))),
        Err(LeaseError::StalePolicy)
    );
    assert_eq!(
        at!(tracker, now + seconds(3), check_delivery(epoch(2))),
        Err(LeaseError::Unavailable)
    );
}

#[test]
fn monotonic_clock_regression_permanently_invalidates_the_tracker() {
    let now = AuthorityTick(seconds(100));
    let mut tracker = installed(now);
    at!(tracker, now + seconds(3), check_delivery(epoch(1))).unwrap();
    assert_eq!(
        at!(tracker, now + seconds(2), check_delivery(epoch(1))),
        Err(LeaseError::ClockRegression)
    );
    assert!(matches!(
        at!(tracker, now + seconds(4), begin_refresh()),
        Err(LeaseError::ClockRegression)
    ));
    assert_eq!(
        at!(tracker, now + seconds(4), check_delivery(epoch(1))),
        Err(LeaseError::ClockRegression)
    );
}

#[test]
fn clock_failure_and_counter_exhaustion_fail_closed_without_wraparound() {
    let now = AuthorityTick(seconds(100));
    let mut failed = installed(now);
    failed.clock.0.set(Err(LeaseError::ClockUnavailable));
    assert_eq!(
        failed.check_delivery(epoch(1)),
        Err(LeaseError::ClockUnavailable)
    );
    assert_eq!(
        at!(failed, now + seconds(1), check_delivery(epoch(1))),
        Err(LeaseError::ClockUnavailable)
    );
    let mut exhausted = installed(now);
    exhausted.last_sequence = u64::MAX;
    assert!(matches!(
        exhausted.begin_refresh(),
        Err(LeaseError::Exhausted)
    ));
    assert_eq!(
        exhausted.check_delivery(epoch(1)),
        Err(LeaseError::Unavailable)
    );
    assert_eq!(exhausted.last_sequence, u64::MAX);
    let mut near_limit = tracker(AuthorityTick(Duration::MAX));
    let ticket = near_limit.begin_refresh().unwrap();
    assert_eq!(
        near_limit.complete_refresh(&ticket, response(1)),
        Err(LeaseError::ClockRange)
    );
    assert_eq!(
        near_limit.check_delivery(epoch(1)),
        Err(LeaseError::Unavailable)
    );
}

#[test]
fn shorter_configured_ttl_and_foreign_epoch_notifications_keep_their_boundaries() {
    let now = AuthorityTick(seconds(100));
    let mut tracker = tracker(now);
    tracker.maximum_ttl = PolicyLeaseTtl::new(seconds(1)).unwrap();
    let refresh = tracker.begin_refresh().unwrap();
    assert_eq!(
        tracker.complete_refresh(&refresh, response(1)),
        Err(LeaseError::InvalidTtl)
    );
    let refresh = tracker.begin_refresh().unwrap();
    let mut reply = response(1);
    reply.ttl = seconds(1);
    tracker.complete_refresh(&refresh, reply).unwrap();
    assert_eq!(
        tracker.observe_epoch(dataset(2), epoch(u64::MAX)),
        Err(LeaseError::WrongDataset)
    );
    assert_eq!(tracker.known_epoch(), epoch(1));
    assert_eq!(tracker.check_delivery(epoch(1)), Ok(()));
    assert_eq!(
        at!(tracker, now + seconds(1), check_delivery(epoch(1))),
        Err(LeaseError::Expired)
    );
}

#[test]
fn simulated_suspend_and_wall_steps_distinguish_unsafe_clock_choices() {
    use std::time::SystemTime;
    let start = AuthorityTick(seconds(100));
    let mut tracker = installed(start);
    let wall_start = SystemTime::UNIX_EPOCH + seconds(1000);
    let wall_after_backward_step = SystemTime::UNIX_EPOCH + seconds(900);
    let awake_only_elapsed = Duration::ZERO;
    // Negative controls: both wrong clock choices would still permit delivery.
    assert!(wall_after_backward_step < wall_start + seconds(5));
    assert!(awake_only_elapsed < seconds(5));
    // The injected provider models 30 seconds of suspend-inclusive elapsed time.
    assert_eq!(
        at!(tracker, start + seconds(30), check_delivery(epoch(1))),
        Err(LeaseError::Expired)
    );
    let wall_after_forward_step = wall_start + seconds(3600);
    assert!(wall_after_forward_step > wall_start + seconds(5));
    let mut current = installed(start);
    assert_eq!(
        at!(current, start + seconds(1), check_delivery(epoch(1))),
        Ok(())
    );
}

#[test]
fn actual_suspend_aware_provider_drives_expiry_without_supplied_timestamps() {
    let mut tracker = PolicyLeaseTracker::new(
        dataset(1),
        process(1),
        epoch(1),
        PolicyLeaseTtl::new(seconds(5)).unwrap(),
    )
    .unwrap();
    let refresh = tracker.begin_refresh().unwrap();
    let mut reply = response(1);
    reply.ttl = seconds(1);
    tracker.complete_refresh(&refresh, reply).unwrap();
    assert_eq!(tracker.check_delivery(epoch(1)), Ok(()));
    let limit = std::time::Instant::now() + seconds(3);
    loop {
        match tracker.check_delivery(epoch(1)) {
            Err(LeaseError::Expired) => break,
            Ok(()) => assert!(
                std::time::Instant::now() < limit,
                "actual authority clock failed to expire within bounded observation"
            ),
            result => panic!("unexpected actual clock result: {result:?}"),
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}
