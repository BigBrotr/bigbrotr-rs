use super::*;
use crate::runtime::policy_authority::{LeaseResponse, installed_with_clock};
use bigbrotr_model::{DatasetId, PolicyEpoch, ProcessIncarnation};
use std::{sync::atomic::Ordering, time::Duration};

#[test]
fn repeated_live_checks_keep_the_first_owner_window_after_policy_refresh() {
    let (mut lease, clock) = installed_with_clock();
    let start = clock.read().unwrap();
    let cancelled = AtomicBool::new(false);
    let mut live = LiveIndexRequest::new(&mut lease, PolicyEpoch::new(1), &cancelled);
    let pending = live.lease.begin_action(live.epoch).unwrap();
    clock.0.set(Ok(start + Duration::from_secs(2)));
    live.action = Some(pending.complete(Duration::from_secs(3)).unwrap());
    live.check().unwrap();

    let refresh = live.lease.begin_refresh().unwrap();
    live.lease
        .complete_refresh(
            &refresh,
            LeaseResponse {
                dataset: DatasetId::try_from([1; 16]).unwrap(),
                process_incarnation: ProcessIncarnation::try_from([1; 16]).unwrap(),
                policy_epoch: PolicyEpoch::new(1),
                ttl: Duration::from_secs(5),
            },
        )
        .unwrap();
    live.check().unwrap();
    clock.0.set(Ok(start + Duration::from_secs(3)));
    live.lease.check_delivery(live.epoch).unwrap();
    assert_eq!(live.check(), Err(IndexError::Unavailable));
}

#[test]
fn live_delivery_intersects_the_existing_policy_and_owner_deadlines() {
    for (remaining, expires) in [(1, 1), (100, 5)] {
        let (mut lease, clock) = installed_with_clock();
        let start = clock.read().unwrap();
        let cancelled = AtomicBool::new(false);
        let mut live = LiveIndexRequest::new(&mut lease, PolicyEpoch::new(1), &cancelled);
        let pending = live.lease.begin_action(live.epoch).unwrap();
        live.action = Some(pending.complete(Duration::from_secs(remaining)).unwrap());
        clock.0.set(Ok(
            start + (Duration::from_secs(expires) - Duration::from_nanos(1))
        ));
        live.check().unwrap();
        clock.0.set(Ok(start + Duration::from_secs(expires)));
        assert_eq!(live.check(), Err(IndexError::Unavailable));
    }
}

#[test]
fn live_delivery_refuses_expiry_cancellation_and_stale_policy_after_acknowledgement() {
    // Exercise the operation-owned carrier at its pre-transaction and post-ack
    // boundaries. SQL/COMMIT ordering remains the installed regression's scope.
    for failure in [
        "owner-expired",
        "policy-expired",
        "cancelled",
        "policy-changed",
    ] {
        let (mut lease, clock) = installed_with_clock();
        let start = clock.read().unwrap();
        let cancelled = AtomicBool::new(false);
        let mut live = LiveIndexRequest::new(&mut lease, PolicyEpoch::new(1), &cancelled);
        let pending = live.lease.begin_action(live.epoch).unwrap();
        let remaining = if failure == "owner-expired" { 2 } else { 100 };
        live.action = Some(pending.complete(Duration::from_secs(remaining)).unwrap());
        live.check().unwrap();
        live.check().unwrap();
        match failure {
            "owner-expired" => clock.0.set(Ok(start + Duration::from_secs(2))),
            "policy-expired" => clock.0.set(Ok(start + Duration::from_secs(5))),
            "cancelled" => cancelled.store(true, Ordering::Release),
            "policy-changed" => live
                .lease
                .observe_epoch(DatasetId::try_from([1; 16]).unwrap(), PolicyEpoch::new(2))
                .unwrap(),
            _ => unreachable!(),
        }
        assert_eq!(live.check(), Err(IndexError::Unavailable), "{failure}");
    }
}
