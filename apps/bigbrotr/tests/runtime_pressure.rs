#[path = "../src/runtime/mod.rs"]
mod runtime;

use runtime::{
    AcquireError, BudgetSnapshot, DrainError, EnvelopeError, OwnedPermit, ProcessEnvelope,
    RequestError, ResourceBudget, ResourceLimits, ResourceRequest, WaiterLimits,
};
use std::{
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

#[test]
fn process_envelope_checks_memory_arithmetic_before_admission() {
    let resources = ResourceLimits::new(512, 8, 256, 128);
    let waiters = WaiterLimits::new(2, 1);
    let envelope = ProcessEnvelope::new(1_024, 128, resources, waiters).unwrap();
    assert_eq!(envelope.memory_max_bytes(), 1_024);
    assert_eq!(envelope.baseline_bytes(), 128);
    assert_eq!(envelope.resources(), resources);
    assert_eq!(envelope.waiters(), waiters);
    assert_eq!(resources.data_bytes(), 512);
    assert_eq!(resources.items(), 8);
    assert_eq!(resources.native_bytes(), 256);
    assert_eq!(resources.control_bytes(), 128);
    assert_eq!(waiters.data(), 2);
    assert_eq!(waiters.control(), 1);

    assert_eq!(
        ProcessEnvelope::new(u64::MAX, u64::MAX, ResourceLimits::new(1, 1, 0, 0), waiters),
        Err(EnvelopeError::MemoryArithmeticOverflow)
    );
    assert_eq!(
        ProcessEnvelope::new(1_023, 128, resources, waiters),
        Err(EnvelopeError::MemoryLimitExceeded {
            required_bytes: 1_024,
            memory_max_bytes: 1_023,
        })
    );
}

#[test]
fn requests_reject_empty_units_and_capacity_that_can_never_fit() {
    assert_eq!(ResourceRequest::data(0, 0, 0), Err(RequestError::Empty));
    assert_eq!(ResourceRequest::control(0), Err(RequestError::Empty));

    let budget = budget(WaiterLimits::new(1, 1));
    assert_eq!(
        budget
            .try_acquire(ResourceRequest::data(513, 1, 0).unwrap())
            .unwrap_err(),
        AcquireError::RequestExceedsLimit
    );
    assert_eq!(
        budget.drain_until(Instant::now() + Duration::from_millis(20)),
        Err(DrainError::NotClosed)
    );
}

fn budget(waiters: WaiterLimits) -> ResourceBudget {
    ResourceBudget::new(
        ProcessEnvelope::new(1_024, 128, ResourceLimits::new(512, 4, 256, 128), waiters).unwrap(),
    )
}

#[test]
fn owned_permits_release_on_drop_and_control_progresses_when_data_is_saturated() {
    let budget = budget(WaiterLimits::new(2, 2));
    let data_request = ResourceRequest::data(512, 4, 256).unwrap();
    let data: OwnedPermit = budget.try_acquire(data_request).unwrap();
    assert_eq!(data.request(), data_request);
    assert_eq!(
        budget
            .try_acquire(ResourceRequest::data(1, 1, 0).unwrap())
            .unwrap_err(),
        AcquireError::Unavailable
    );

    let control_request = ResourceRequest::control(128).unwrap();
    let control = budget.try_acquire(control_request).unwrap();
    assert_eq!(control.request(), control_request);
    let saturated: BudgetSnapshot = budget.snapshot();
    assert_eq!(saturated.used(), ResourceLimits::new(512, 4, 256, 128));
    assert_eq!(saturated.active_permits(), 2);

    drop(data);
    assert_eq!(budget.snapshot().used(), ResourceLimits::new(0, 0, 0, 128));
    drop(control);
    assert_eq!(budget.snapshot().used(), ResourceLimits::new(0, 0, 0, 0));
}

#[test]
fn caller_timeout_does_not_release_a_permit_still_owned_by_work() {
    let budget = budget(WaiterLimits::new(1, 1));
    let permit = budget
        .try_acquire(ResourceRequest::data(256, 1, 64).unwrap())
        .unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (finish_tx, finish_rx) = mpsc::channel();
    let worker = thread::spawn(move || {
        let _permit = permit;
        started_tx.send(()).unwrap();
        finish_rx.recv().unwrap();
    });
    started_rx.recv().unwrap();

    let (_reply_tx, reply_rx) = mpsc::channel::<()>();
    assert_eq!(
        reply_rx.recv_timeout(Duration::from_millis(20)),
        Err(mpsc::RecvTimeoutError::Timeout)
    );
    assert_eq!(budget.snapshot().active_permits(), 1);
    assert_eq!(budget.snapshot().used(), ResourceLimits::new(256, 1, 64, 0));

    finish_tx.send(()).unwrap();
    worker.join().unwrap();
    assert_eq!(budget.snapshot().active_permits(), 0);
}

#[test]
fn reduction_below_live_use_blocks_new_data_but_preserves_control_reserve() {
    let budget = budget(WaiterLimits::new(1, 1));
    let data = budget
        .try_acquire(ResourceRequest::data(400, 2, 128).unwrap())
        .unwrap();
    budget
        .set_limits(ResourceLimits::new(256, 1, 64, 128))
        .unwrap();
    let snapshot = budget.snapshot();
    assert!(snapshot.data_over_limit());
    assert_eq!(snapshot.limits(), ResourceLimits::new(256, 1, 64, 128));
    assert_eq!(
        budget
            .try_acquire(ResourceRequest::data(1, 1, 0).unwrap())
            .unwrap_err(),
        AcquireError::Unavailable
    );

    let control = budget
        .try_acquire(ResourceRequest::control(32).unwrap())
        .unwrap();
    drop(control);
    drop(data);
    assert!(!budget.snapshot().data_over_limit());
    budget
        .try_acquire(ResourceRequest::data(256, 1, 64).unwrap())
        .unwrap();
}

#[test]
fn live_limit_change_cannot_exceed_the_checked_process_envelope() {
    let budget = budget(WaiterLimits::new(1, 1));
    assert_eq!(
        budget.set_limits(ResourceLimits::new(769, 4, 0, 128)),
        Err(EnvelopeError::MemoryLimitExceeded {
            required_bytes: 1_025,
            memory_max_bytes: 1_024,
        })
    );
    assert_eq!(
        budget.snapshot().limits(),
        ResourceLimits::new(512, 4, 256, 128)
    );
}

fn wait_until_data_waiters(budget: &ResourceBudget, expected: usize) {
    let deadline = Instant::now() + Duration::from_secs(1);
    while budget.snapshot().waiting_data() != expected {
        assert!(Instant::now() < deadline, "waiter did not enter the queue");
        thread::yield_now();
    }
}

#[test]
fn queued_data_work_does_not_block_the_control_lane() {
    let budget = budget(WaiterLimits::new(1, 1));
    let held = budget
        .try_acquire(ResourceRequest::data(512, 4, 256).unwrap())
        .unwrap();
    let waiting_budget = budget.clone();
    let waiter = thread::spawn(move || {
        waiting_budget.acquire_until(
            ResourceRequest::data(1, 1, 0).unwrap(),
            Instant::now() + Duration::from_secs(5),
        )
    });
    wait_until_data_waiters(&budget, 1);

    let control = budget
        .try_acquire(ResourceRequest::control(128).unwrap())
        .unwrap();
    assert_eq!(budget.snapshot().waiting_data(), 1);
    drop(control);
    budget.close();
    assert_eq!(waiter.join().unwrap().unwrap_err(), AcquireError::Closed);
    drop(held);
}

#[test]
fn pending_waiters_are_bounded_independently_of_item_capacity() {
    let budget = budget(WaiterLimits::new(1, 1));
    let held = budget
        .try_acquire(ResourceRequest::data(512, 4, 256).unwrap())
        .unwrap();
    let held_control = budget
        .try_acquire(ResourceRequest::control(128).unwrap())
        .unwrap();
    let waiting_budget = budget.clone();
    let data_waiter = thread::spawn(move || {
        waiting_budget.acquire_until(
            ResourceRequest::data(1, 1, 0).unwrap(),
            Instant::now() + Duration::from_secs(5),
        )
    });
    let waiting_budget = budget.clone();
    let control_waiter = thread::spawn(move || {
        waiting_budget.acquire_until(
            ResourceRequest::control(1).unwrap(),
            Instant::now() + Duration::from_secs(5),
        )
    });
    wait_until_data_waiters(&budget, 1);
    let deadline = Instant::now() + Duration::from_secs(1);
    while budget.snapshot().waiting_control() != 1 {
        assert!(
            Instant::now() < deadline,
            "control waiter did not enter the queue"
        );
        thread::yield_now();
    }

    assert_eq!(
        budget
            .acquire_until(
                ResourceRequest::data(1, 1, 0).unwrap(),
                Instant::now() + Duration::from_secs(1),
            )
            .unwrap_err(),
        AcquireError::WaitQueueFull
    );
    assert_eq!(
        budget
            .acquire_until(
                ResourceRequest::control(1).unwrap(),
                Instant::now() + Duration::from_secs(1),
            )
            .unwrap_err(),
        AcquireError::WaitQueueFull
    );
    budget.close();
    assert_eq!(
        data_waiter.join().unwrap().unwrap_err(),
        AcquireError::Closed
    );
    assert_eq!(
        control_waiter.join().unwrap().unwrap_err(),
        AcquireError::Closed
    );
    drop(held);
    drop(held_control);
}

#[test]
fn waiter_deadline_removes_its_queue_entry() {
    let budget = budget(WaiterLimits::new(1, 1));
    let held = budget
        .try_acquire(ResourceRequest::data(512, 4, 256).unwrap())
        .unwrap();
    assert_eq!(
        budget
            .acquire_until(
                ResourceRequest::data(1, 1, 0).unwrap(),
                Instant::now() + Duration::from_millis(20),
            )
            .unwrap_err(),
        AcquireError::DeadlineExceeded
    );
    assert_eq!(budget.snapshot().waiting_data(), 0);
    drop(held);
}

#[test]
fn close_wakes_waiters_and_drain_waits_for_owned_permits() {
    let budget = budget(WaiterLimits::new(1, 1));
    let held = budget
        .try_acquire(ResourceRequest::data(512, 4, 256).unwrap())
        .unwrap();
    let waiting_budget = budget.clone();
    let waiter = thread::spawn(move || {
        waiting_budget.acquire_until(
            ResourceRequest::data(1, 1, 0).unwrap(),
            Instant::now() + Duration::from_secs(5),
        )
    });
    wait_until_data_waiters(&budget, 1);
    budget.close();
    assert_eq!(waiter.join().unwrap().unwrap_err(), AcquireError::Closed);
    assert!(budget.snapshot().closed());
    assert_eq!(budget.snapshot().waiting_data(), 0);
    assert_eq!(
        budget.drain_until(Instant::now() + Duration::from_millis(20)),
        Err(DrainError::DeadlineExceeded)
    );

    drop(held);
    assert_eq!(
        budget.drain_until(Instant::now() + Duration::from_secs(1)),
        Ok(())
    );
    assert_eq!(budget.snapshot().active_permits(), 0);
}
