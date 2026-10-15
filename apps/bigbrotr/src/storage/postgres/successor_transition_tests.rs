//! Component admission/state checks only. Actual transaction, native dispatch,
//! COMMIT loss, UTC waits and supervisor teardown require connected fixtures.
use super::*;
use crate::runtime::{
    ProcessEnvelope, ResourceBudget, ResourceLimits, ResourceRequest, WaiterLimits,
};
use bigbrotr_model::{OwnerEpoch, OwnerResourceId, ProcessIncarnation};

fn control() -> ingest_registry::ControlOwner {
    ingest_registry::ControlOwner {
        resource_id: OwnerResourceId::try_from([1; 16]).unwrap(),
        epoch: OwnerEpoch::new(9).unwrap(),
        role_instance_id: [2; 16],
        process_incarnation: ProcessIncarnation::try_from([3; 16]).unwrap(),
    }
}

#[test]
fn successor_preparation_is_one_shot_even_after_a_confirmed_observation() {
    let mut state = PreparationState::Fresh;
    state.start().unwrap();
    // A begun attempt is spent before any fallible I/O; an error cannot rearm it.
    assert_eq!(state, PreparationState::Spent);
    assert_eq!(state.start(), Err(OwnerError::Unavailable));
    let mut confirmed = PreparationState::Prepared;
    assert_eq!(confirmed.start(), Err(OwnerError::Unavailable));
}

#[test]
fn successor_preparation_checks_scheduling_overflow_and_independent_request_window() {
    assert_eq!(lease_micros(Duration::from_micros(1), 1).unwrap(), 1);
    assert_eq!(
        lease_micros(Duration::from_secs(20), 64).unwrap(),
        20_000_000
    );
    assert!(lease_micros(Duration::ZERO, 1).is_err());
    assert!(lease_micros(Duration::from_nanos(999), 1).is_err());
    assert!(lease_micros(Duration::from_secs(u64::MAX), 1).is_err());
    for limit in [0, 65, u32::MAX] {
        assert!(lease_micros(Duration::from_secs(20), limit).is_err());
    }
}

#[test]
fn successor_plan_binding_requires_every_actual_control_coordinate() {
    let current = control();
    require_control(current, current, current).unwrap();
    let alternatives = [
        ingest_registry::ControlOwner {
            resource_id: OwnerResourceId::try_from([4; 16]).unwrap(),
            ..current
        },
        ingest_registry::ControlOwner {
            epoch: OwnerEpoch::new(10).unwrap(),
            ..current
        },
        ingest_registry::ControlOwner {
            role_instance_id: [5; 16],
            ..current
        },
        ingest_registry::ControlOwner {
            process_incarnation: ProcessIncarnation::try_from([6; 16]).unwrap(),
            ..current
        },
    ];
    for other in alternatives {
        assert!(require_control(other, current, current).is_err());
        assert!(require_control(current, other, current).is_err());
        assert!(require_control(current, current, other).is_err());
    }
}

#[test]
fn successor_native_transition_reserves_data_native_and_item_together() {
    let budget = ResourceBudget::new(
        ProcessEnvelope::new(
            64 * 1024,
            0,
            ResourceLimits::new(32 * 1024, 4, 32 * 1024, 0),
            WaiterLimits::new(0, 0),
        )
        .unwrap(),
    );
    for minimum in [4096, 8192] {
        for (data, items, native, accepted) in [
            (minimum, 1, minimum, true),
            (minimum - 1, 1, minimum, false),
            (minimum, 1, minimum - 1, false),
            (minimum, 0, minimum, false),
        ] {
            let permit = budget
                .try_acquire(ResourceRequest::data(data, items, native).unwrap())
                .unwrap();
            assert_eq!(require_permit(&permit, minimum).is_ok(), accepted);
        }
    }
}
