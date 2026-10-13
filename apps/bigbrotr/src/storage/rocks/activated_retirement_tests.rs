//! Read-only native retirement observations. The fixture's activation authority
//! is synthetic; these tests do not qualify SQL retirement or installed custody.
use super::*;

#[test]
fn activated_retirement_observes_only_existing_closing_and_preserves_event_classification() {
    for (mode, count, cut, mask) in [
        ("zero", 0, 0, 0),
        ("rejected", 1, 0, 1),
        ("durable", 1, 0, 1),
        ("new-facts", 2, 5, 0b10),
    ] {
        let fixture = closed_fixture();
        let mut writer = active(&fixture);
        if mode == "new-facts" {
            assert_append(&fixture, &mut writer, 1);
            signed_new(&fixture, &mut writer, 2);
        } else if mode != "zero" {
            let outcome = writer
                .store_mut()
                .admit_event(
                    event(&fixture, 1, mode == "rejected"),
                    ObservedTime::new(77),
                    false,
                    AdmissionLimits::default(),
                    grant(&fixture.budget, 40 * MIB, 10 * MIB),
                    || Ok(()),
                )
                .unwrap();
            assert!(match outcome.outcome() {
                AdmissionOutcome::Rejected(RejectionReason::Signature) => mode == "rejected",
                AdmissionOutcome::Durable(receipt) =>
                    mode == "durable" && receipt.batch().is_none(),
                _ => false,
            });
        }
        drop(writer);
        let mut recovery = fixture.reopen();
        let before = capture(&fixture, &recovery.store);
        assert!(matches!(
            inspect(&recovery).observe_closed_producer(
                &recovery,
                grant(&fixture.budget, 4096, 4096),
                || Ok(())
            ),
            Err(BootstrapError::StateMismatch)
        ));
        unchanged(&fixture, &recovery.store, &before);
        // The prior protocol performs the barrier. The new observation is made
        // only after dropping that proof and reopening the actual closed store.
        drop(close(&fixture, &mut recovery));
        assert_eq!(
            value(&recovery.store, producer_key(2)),
            closing_marker(count)
        );
        assert_eq!(recovery.store.db.latest_sequence_number(), before.1 + 1);
        drop(recovery);
        let recovery = fixture.reopen();
        let before = capture(&fixture, &recovery.store);
        let view = inspect(&recovery);
        let closed = view
            .observe_closed_producer(&recovery, grant(&fixture.budget, 4096, 4096), || Ok(()))
            .unwrap();
        let another = view
            .observe_closed_producer(&recovery, grant(&fixture.budget, 4096, 4096), || Ok(()))
            .unwrap();
        let mut prefix = closed.source_resolution(&recovery, count).unwrap();
        for sequence in 1..=count {
            prefix
                .record(&resolve(&fixture, &recovery, &closed, sequence).unwrap())
                .unwrap();
        }
        let source = prefix.finish().unwrap();
        assert_eq!(source.resolved_requests(), count);
        assert_eq!(source.last_complete_sequence(), cut);
        assert_eq!(source.event_request_mask(), mask);
        assert_eq!(
            source.digest(),
            expected_closure(&fixture, &recovery, count, cut).0
        );
        assert_eq!(source.validate(), Ok(()));
        assert!(source.matches_closed(&closed));
        assert!(!source.matches_closed(&another));
        unchanged(&fixture, &recovery.store, &before);
        originals(&fixture, &recovery.store);
    }
}

#[test]
fn activated_retirement_observation_checks_grants_and_authority_without_writes() {
    let fixture = closed_fixture();
    drop(active(&fixture));
    let mut recovery = fixture.reopen();
    drop(close(&fixture, &mut recovery));
    let view = inspect(&recovery);
    let before = capture(&fixture, &recovery.store);
    let baseline = fixture.budget.snapshot();
    for fault in ["data", "native", "items", "authority1", "authority2"] {
        let permit = fixture
            .budget
            .try_acquire(
                ResourceRequest::data(
                    4096 - u64::from(fault == "data"),
                    u64::from(fault != "items"),
                    4096 - u64::from(fault == "native"),
                )
                .unwrap(),
            )
            .unwrap();
        let mut calls = 0;
        let result = view.observe_closed_producer(&recovery, permit, || {
            calls += 1;
            if fault == format!("authority{calls}") {
                Err(BootstrapError::AuthorityUnavailable)
            } else {
                Ok(())
            }
        });
        assert!(match result {
            Err(BootstrapError::AuthorityUnavailable) => fault.starts_with("authority"),
            Err(BootstrapError::InsufficientReservation) => !fault.starts_with("authority"),
            _ => false,
        });
        assert_eq!(
            calls,
            match fault {
                "authority1" => 1,
                "authority2" => 2,
                _ => 0,
            }
        );
        unchanged(&fixture, &recovery.store, &before);
        assert_eq!(fixture.budget.snapshot(), baseline);
        let closed = view
            .observe_closed_producer(&recovery, grant(&fixture.budget, 4096, 4096), || Ok(()))
            .unwrap();
        let source = closed
            .source_resolution(&recovery, 0)
            .unwrap()
            .finish()
            .unwrap();
        assert_eq!(source.event_request_mask(), 0);
        assert_eq!(
            source.digest(),
            expected_closure(&fixture, &recovery, 0, 0).0
        );
        let held = fixture.budget.snapshot();
        assert_eq!(
            held.used().data_bytes(),
            baseline.used().data_bytes() + 4096
        );
        assert_eq!(
            held.used().native_bytes(),
            baseline.used().native_bytes() + 4096
        );
        assert_eq!(held.used().items(), baseline.used().items() + 1);
        assert_eq!(held.active_permits(), baseline.active_permits() + 1);
        unchanged(&fixture, &recovery.store, &before);
        drop(source);
        drop(closed);
        assert_eq!(fixture.budget.snapshot(), baseline);
    }
}

#[test]
fn activated_retirement_observation_binds_root_sequence_and_final_authority_check() {
    let fixture = closed_fixture();
    let mut writer = active(&fixture);
    assert_append(&fixture, &mut writer, 1);
    drop(writer);
    let mut recovery = fixture.reopen();
    drop(close(&fixture, &mut recovery));
    drop(recovery);
    let clone = tempfile::TempDir::new().unwrap();
    let path = clone.path().join("db");
    copy_directory(&fixture.path(), &path);
    let recovery = fixture.reopen();
    let foreign_budget = ResourceBudget::new(
        ProcessEnvelope::new(
            400 * MIB,
            0,
            ResourceLimits::new(128 * MIB, 16, 256 * MIB, 16 * MIB),
            WaiterLimits::new(1, 1),
        )
        .unwrap(),
    );
    let foreign = IngestRecovery::inspect(
        &path,
        fixture.previous.identity().dataset_id,
        fixture.native,
        grant(&foreign_budget, 0, 128 * MIB),
    )
    .unwrap();
    let before = capture(&fixture, &recovery.store);
    assert_eq!(kv(&foreign.store), before.0);
    assert_eq!(foreign.store.db.latest_sequence_number(), before.1);
    assert_eq!(inputs(&path), before.2);
    let view = inspect(&recovery);
    assert!(matches!(
        view.observe_closed_producer(&foreign, grant(&fixture.budget, 4096, 4096), || Ok(())),
        Err(BootstrapError::StateMismatch)
    ));
    unchanged(&fixture, &recovery.store, &before);
    let handoff = value(&recovery.store, handoff_key());
    let mut calls = 0;
    // Privileged fixture interference at the final callback changes only the
    // sequence. Returning Ok authority must not publish a stale closing proof.
    assert!(matches!(
        view.observe_closed_producer(&recovery, grant(&fixture.budget, 4096, 4096), || {
            calls += 1;
            if calls == 2 {
                put(&recovery.store, handoff_key(), &handoff);
            }
            Ok(())
        }),
        Err(BootstrapError::StateMismatch)
    ));
    assert_eq!(calls, 2);
    assert_eq!(kv(&recovery.store), before.0);
    assert_eq!(recovery.store.db.latest_sequence_number(), before.1 + 1);
    assert_eq!(inputs(&fixture.path()), before.2);
    let after = capture(&fixture, &recovery.store);
    assert!(matches!(
        view.observe_closed_producer(&recovery, grant(&fixture.budget, 4096, 4096), || Ok(())),
        Err(BootstrapError::StateMismatch)
    ));
    let fresh = inspect(&recovery)
        .observe_closed_producer(&recovery, grant(&fixture.budget, 4096, 4096), || Ok(()))
        .unwrap();
    let mut prefix = fresh.source_resolution(&recovery, 1).unwrap();
    prefix
        .record(&resolve(&fixture, &recovery, &fresh, 1).unwrap())
        .unwrap();
    let source = prefix.finish().unwrap();
    assert_eq!(source.event_request_mask(), 0);
    assert_eq!(
        source.digest(),
        expected_closure(&fixture, &recovery, 1, 1).0
    );
    unchanged(&fixture, &recovery.store, &after);
    originals(&fixture, &recovery.store);
    assert_eq!(kv(&foreign.store), before.0);
    assert_eq!(foreign.store.db.latest_sequence_number(), before.1);
    assert_eq!(inputs(&path), before.2);
}
