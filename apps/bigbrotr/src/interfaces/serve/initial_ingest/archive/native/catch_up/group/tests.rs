use super::*;
use bigbrotr::runtime::{ProcessEnvelope, ResourceBudget, ResourceRequest, WaiterLimits};
use catalog_guard::Resolution::{NotImported, Sealed};

#[tokio::test]
async fn grouped_lifetimes_require_every_known_completion_before_any_observation() {
    let deadline = Instant::now() + Duration::from_secs(30);
    for fault in [None, Some(0), Some(1), Some(2)] {
        let cancel = AtomicBool::new(false);
        let mut resolutions = Vec::with_capacity(3);
        let mut calls = Vec::new();
        let original = error("original unknown catalog completion");
        let result = resolve_members(
            &mut resolutions,
            3,
            deadline,
            &cancel,
            async |index, actual, flag| {
                assert_eq!(actual, deadline);
                assert!(std::ptr::eq(flag, &cancel));
                calls.push(index);
                if Some(index) == fault {
                    return Err(original.clone());
                }
                Ok(if index == 0 {
                    NotImported
                } else {
                    Sealed {
                        disabled_current: index == 2,
                    }
                })
            },
        )
        .await;
        let mut statuses = [false; 3];
        if let Some(index) = fault {
            assert_eq!(result.unwrap_err(), original);
            assert_eq!(calls, (0..=index).collect::<Vec<_>>());
            assert!(
                observe_resolved(
                    &resolutions,
                    &mut statuses,
                    deadline,
                    &cancel,
                    |_, _| panic!("unknown seal reached observation"),
                    |_, _, _| panic!("membership")
                )
                .is_err()
            );
            assert_eq!(statuses, [false; 3]);
        } else {
            result.unwrap();
            assert_eq!(calls, [0, 1, 2]);
            observe_resolved(
                &resolutions,
                &mut statuses,
                deadline,
                &cancel,
                |_, _| Ok(Some(2)),
                |_, _, _| Ok(()),
            )
            .unwrap();
            assert_eq!(statuses, [false, false, true]);
        }
    }
    for pre_cancel in [false, true] {
        let cancel = AtomicBool::new(pre_cancel);
        let mut resolutions = Vec::with_capacity(3);
        let mut calls = 0;
        assert!(
            resolve_members(
                &mut resolutions,
                3,
                deadline,
                &cancel,
                async |_, _, flag| {
                    calls += 1;
                    flag.store(true, Ordering::Release);
                    Ok(NotImported)
                }
            )
            .await
            .is_err()
        );
        assert_eq!(calls, usize::from(!pre_cancel));
        assert!(resolutions.is_empty());
    }
}

#[test]
fn grouped_observation_requires_all_resolutions_and_each_selected_members_own_seal() {
    let deadline = Instant::now() + Duration::from_secs(30);
    let cancel = AtomicBool::new(false);
    for selected in [None, Some(0), Some(1), Some(2)] {
        let mut statuses = [false; 3];
        let mut resolutions = [NotImported; 3];
        if let Some(index) = selected {
            resolutions[index] = Sealed {
                disabled_current: true,
            };
        }
        let membership_calls = Cell::new(0);
        observe_resolved(
            &resolutions,
            &mut statuses,
            deadline,
            &cancel,
            |actual, flag| {
                assert_eq!(actual, deadline);
                assert!(std::ptr::eq(flag, &cancel));
                Ok(selected)
            },
            |index, actual, flag| {
                assert_eq!(Some(index), selected);
                assert_eq!(actual, deadline);
                assert!(std::ptr::eq(flag, &cancel));
                membership_calls.set(membership_calls.get() + 1);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(
            statuses,
            std::array::from_fn(|index| Some(index) == selected)
        );
        assert_eq!(membership_calls.get(), usize::from(selected.is_some()));
    }
    for selected in [0, 1, 2, 3] {
        let mut statuses = [false; 3];
        assert!(
            observe_resolved(
                &[NotImported; 3],
                &mut statuses,
                deadline,
                &cancel,
                |_, _| Ok(Some(selected)),
                |_, _, _| panic!("unsealed member reached membership")
            )
            .is_err()
        );
        assert_eq!(statuses, [false; 3]);
    }
    let mut statuses = [false; 3];
    assert!(
        observe_resolved(
            &[NotImported; 2],
            &mut statuses,
            deadline,
            &cancel,
            |_, _| panic!("incomplete resolution vector reached scan"),
            |_, _, _| panic!("membership")
        )
        .is_err()
    );
}

#[test]
fn grouped_observation_preserves_errors_cancel_expiry_and_atomic_status_delivery() {
    let deadline = Instant::now() + Duration::from_secs(30);
    for stage in [
        "scan-error",
        "membership-error",
        "cancel-after-scan",
        "cancel-after-membership",
        "pre-cancel",
        "pre-expired",
        "expire-after-scan",
        "expire-after-membership",
    ] {
        let cancel = AtomicBool::new(stage == "pre-cancel");
        let deadline = if stage == "pre-expired" {
            Instant::now()
        } else {
            deadline
        };
        let mut statuses = [false; 3];
        let scans = Cell::new(0);
        let memberships = Cell::new(0);
        let failure = error("original grouped stage failure");
        // The short deadlines belong to the test; production's 30s value is unchanged.
        let deadline = if stage.starts_with("expire-after") {
            Instant::now() + Duration::from_millis(20)
        } else {
            deadline
        };
        let result = observe_resolved(
            &[
                NotImported,
                Sealed {
                    disabled_current: false,
                },
                NotImported,
            ],
            &mut statuses,
            deadline,
            &cancel,
            |actual, flag| {
                assert_eq!(actual, deadline);
                assert!(std::ptr::eq(flag, &cancel));
                scans.set(scans.get() + 1);
                if stage == "scan-error" {
                    return Err(failure.clone());
                }
                if stage == "cancel-after-scan" {
                    flag.store(true, Ordering::Release);
                }
                if stage == "expire-after-scan" {
                    std::thread::sleep(Duration::from_millis(30));
                }
                Ok(Some(1))
            },
            |index, actual, flag| {
                assert_eq!(index, 1);
                assert_eq!(actual, deadline);
                assert!(std::ptr::eq(flag, &cancel));
                memberships.set(memberships.get() + 1);
                if stage == "membership-error" {
                    return Err(failure.clone());
                }
                if stage == "cancel-after-membership" {
                    flag.store(true, Ordering::Release);
                }
                if stage == "expire-after-membership" {
                    std::thread::sleep(Duration::from_millis(30));
                }
                Ok(())
            },
        );
        assert!(result.is_err(), "{stage}");
        assert_eq!(statuses, [false; 3], "{stage}");
        assert_eq!(
            scans.get(),
            usize::from(!stage.starts_with("pre-")),
            "{stage}"
        );
        assert_eq!(
            memberships.get(),
            usize::from(matches!(
                stage,
                "membership-error" | "cancel-after-membership" | "expire-after-membership"
            )),
            "{stage}"
        );
        if stage.ends_with("error") {
            assert_eq!(result.unwrap_err(), failure);
        }
        if stage.starts_with("pre-") {
            assert_eq!(scans.get(), 0);
        }
        if matches!(
            stage,
            "scan-error" | "cancel-after-scan" | "expire-after-scan"
        ) {
            assert_eq!(memberships.get(), 0);
        }
    }
}

#[test]
fn grouped_reader_admission_keeps_one_serial_lane_and_returns_failed_reservations() {
    let budget = |native, control| {
        ResourceBudget::new(
            ProcessEnvelope::new(
                native + control,
                0,
                ResourceLimits::new(0, 1, native, control),
                WaiterLimits::new(0, 0),
            )
            .unwrap(),
        )
    };
    let limits = PreparationLimits {
        memory_bytes: 128 << 20,
        native_reserve_bytes: 384 << 20,
        temp_bytes: 64 << 20,
        threads: 1,
        max_records: 128,
        max_encoded_bytes: 1 << 20,
    };
    let app = budget(0, 128 << 10);
    let native = budget(512 << 20, 0);
    let budgets = WorkerBudgets {
        application: &app,
        native: &native,
    };
    let occupied = app
        .try_acquire(ResourceRequest::control(1).unwrap())
        .unwrap();
    assert!(reserve_readers(budgets, limits).is_err());
    assert_eq!(native.snapshot().active_permits(), 0);
    drop(occupied);
    let (application, child) = reserve_readers(budgets, limits).unwrap();
    for _ in 0..2 {
        let native_grant = child
            .budget()
            .try_acquire(ResourceRequest::data(0, 1, 512 << 20).unwrap())
            .unwrap();
        let control = application
            .budget()
            .try_acquire(ResourceRequest::control(128 << 10).unwrap())
            .unwrap();
        assert!(
            child
                .budget()
                .try_acquire(ResourceRequest::data(0, 1, 1).unwrap())
                .is_err()
        );
        assert_eq!(native.snapshot().used().native_bytes(), 512 << 20);
        drop((native_grant, control));
        assert_eq!(native.snapshot().used().native_bytes(), 512 << 20);
    }
    drop((application, child));
    assert_eq!(app.snapshot().active_permits(), 0);
    assert_eq!(native.snapshot().active_permits(), 0);
    // Unknown native termination can outlive the reservation owner. Its actual
    // escaped process permits must keep precisely their parent charges alive.
    let (application, child) = reserve_readers(budgets, limits).unwrap();
    let escaped_native = child
        .budget()
        .try_acquire(ResourceRequest::data(0, 1, 512 << 20).unwrap())
        .unwrap();
    let escaped_control = application
        .budget()
        .try_acquire(ResourceRequest::control(128 << 10).unwrap())
        .unwrap();
    drop((application, child));
    assert_eq!(native.snapshot().used().native_bytes(), 512 << 20);
    assert_eq!(app.snapshot().used().control_bytes(), 128 << 10);
    drop((escaped_native, escaped_control));
    assert_eq!(app.snapshot().active_permits(), 0);
    assert_eq!(native.snapshot().active_permits(), 0);
    assert!(metadata_bytes(0, 8).is_err());
    assert!(metadata_bytes(129, 8).is_err());
    assert!(metadata_bytes(128, usize::MAX).is_err());
}
