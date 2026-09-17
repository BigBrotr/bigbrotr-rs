//! P01-T06 local schedules and OS providers, not storage/process-exclusion acceptance.
#[path = "../src/runtime/ownership.rs"]
mod ownership;
#[path = "../src/runtime/policy_authority.rs"]
mod policy_authority;

use bigbrotr_model::{DatasetId, OwnerEpoch, PolicyEpoch, ProcessIncarnation};
use ownership::{OwnershipTiming, TimingError, WriterIdentity, WriterMismatch, WriterRole};
use policy_authority::{LeaseError, PolicyLeaseTtl};
use std::time::Duration;

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
#[test]
fn writer_comparison_checks_each_identity_dimension_without_granting_ownership() {
    let writer = WriterIdentity::new(
        dataset(1),
        WriterRole::Ingest,
        OwnerEpoch::new(1).unwrap(),
        process(1),
    );
    assert_eq!(writer.require_same(writer), Ok(()));
    for (other, expected) in [
        (
            WriterIdentity::new(
                dataset(2),
                WriterRole::Ingest,
                OwnerEpoch::new(1).unwrap(),
                process(1),
            ),
            WriterMismatch::Dataset,
        ),
        (
            WriterIdentity::new(
                dataset(1),
                WriterRole::Archive,
                OwnerEpoch::new(1).unwrap(),
                process(1),
            ),
            WriterMismatch::Role,
        ),
        (
            WriterIdentity::new(
                dataset(1),
                WriterRole::Ingest,
                OwnerEpoch::new(2).unwrap(),
                process(1),
            ),
            WriterMismatch::Epoch,
        ),
        (
            WriterIdentity::new(
                dataset(1),
                WriterRole::Ingest,
                OwnerEpoch::new(1).unwrap(),
                process(2),
            ),
            WriterMismatch::Incarnation,
        ),
    ] {
        assert_eq!(writer.require_same(other), Err(expected));
    }
    assert!(OwnerEpoch::new(u64::MAX).unwrap().checked_add(1).is_err());
    assert!(epoch(u64::MAX).checked_add(1).is_err());
}

#[test]
fn ownership_timing_has_a_strict_checked_safety_window() {
    let timing = OwnershipTiming::new(
        seconds(60),
        seconds(20),
        seconds(5),
        seconds(20),
        seconds(30),
    )
    .unwrap();
    assert_eq!(timing.lease_ttl(), seconds(60));
    assert_eq!(timing.heartbeat_interval(), seconds(20));
    assert_eq!(timing.renewal_timeout(), seconds(5));
    assert_eq!(timing.safety_margin(), seconds(20));
    assert_eq!(timing.graceful_shutdown_timeout(), seconds(30));
    assert_eq!(
        OwnershipTiming::new(
            seconds(60),
            seconds(35),
            seconds(5),
            seconds(20),
            seconds(30)
        ),
        Err(TimingError::UnsafeRenewalWindow)
    );
    assert_eq!(
        OwnershipTiming::new(
            seconds(60),
            Duration::MAX,
            seconds(5),
            seconds(20),
            seconds(30)
        ),
        Err(TimingError::Overflow)
    );
    assert_eq!(
        OwnershipTiming::new(
            seconds(60),
            seconds(20),
            seconds(5),
            seconds(61),
            seconds(30)
        ),
        Err(TimingError::UnsafeRenewalWindow)
    );
    assert_eq!(
        OwnershipTiming::new(
            seconds(60),
            seconds(20),
            seconds(5),
            seconds(20),
            Duration::ZERO
        ),
        Err(TimingError::ZeroDuration)
    );
    assert_eq!(
        PolicyLeaseTtl::new(seconds(60)),
        Err(LeaseError::InvalidTtl)
    );
    assert_eq!(
        PolicyLeaseTtl::new(Duration::ZERO),
        Err(LeaseError::InvalidTtl)
    );
}
