//! Real retained native state and literal expected source framing. Connected
//! authority, SQL terminal effects and delivery qualification belong to root.
use super::super::activated_source_tests::expected_closure;
use super::*;
use crate::storage::rocks::store::{ActivatedSourceResolution, ResolvedActivatedSource};

fn confirmed_prefix<'a>(
    f: &Fixture,
    r: &'a IngestRecovery,
    closed: &'a ActivatedProducerClosing,
    cancelled: &'a AtomicBool,
) -> ActivatedSourceResolution<'a> {
    let mut cursor = closed
        .compacted_source_resolution(r, closed.issued_through(), cancelled)
        .unwrap();
    let effects = grant(&f.budget, 40 * MIB, 0);
    let ancestor = closed
        .compacted_initial_ancestor(r, AdmissionLimits::default(), &effects, cancelled)
        .unwrap();
    let proof = canonical(f, ancestor.resolution(cancelled).unwrap());
    cursor.confirm_compacted_initial(&proof, || Ok(())).unwrap();
    cursor
}

pub(super) fn completed_source<'a>(
    f: &Fixture,
    r: &'a IngestRecovery,
    closed: &'a ActivatedProducerClosing,
    cancelled: &'a AtomicBool,
) -> ResolvedActivatedSource<'a> {
    let mut cursor = confirmed_prefix(f, r, closed, cancelled);
    for n in 1..=closed.issued_through() {
        cursor
            .record(&target_request(f, r, closed, n).unwrap())
            .unwrap();
    }
    cursor.finish().unwrap()
}

fn stage_tail(f: &Fixture, r: &IngestRecovery, n: u64) {
    let path = f.path().join("producer-inputs-v1").join(input_name(2, n));
    std::fs::write(
        &path,
        expected(
            r.manifest()
                .allocation_namespaces()
                .domain(DictionaryKind::VisibilityScope)
                .unwrap()
                .as_bytes(),
            n,
        )
        .input,
    )
    .unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
}

#[test]
fn compacted_activated_source_preserves_genuine_digest_event_mask_and_counts() {
    for (mode, count, cut, mask, residual) in [
        ("zero", 0, 0, 0, 4),
        ("dictionary", 1, 1, 0, 2),
        ("mixed", 4, 5, 0b1110, 0),
        ("old-durable", 1, 0, 1, 0),
        ("rejected", 1, 0, 1, 0),
    ] {
        let f = closed_fixture();
        let mut writer = active(&f);
        if matches!(mode, "dictionary" | "mixed") {
            assert_append(&f, &mut writer, 1);
        }
        if mode == "mixed" {
            signed_new(&f, &mut writer, 2);
            let duplicate = PreparedEvent::new(
                next_request(3),
                intent("B_note", 1, 10_000_000),
                256 * 1024,
                grant(&f.budget, MIB, 0),
            )
            .unwrap();
            let result = writer
                .store_mut()
                .admit_event(
                    duplicate,
                    ObservedTime::new(77),
                    false,
                    AdmissionLimits::default(),
                    grant(&f.budget, 40 * MIB, 10 * MIB),
                    || Ok(()),
                )
                .unwrap();
            assert!(
                matches!(result.outcome(), AdmissionOutcome::Durable(receipt) if receipt.batch().is_none())
            );
            drop(result);
        }
        if matches!(mode, "mixed" | "old-durable" | "rejected") {
            let n = if mode == "mixed" { 4 } else { 1 };
            let result = writer
                .store_mut()
                .admit_event(
                    event(&f, n, mode != "old-durable"),
                    ObservedTime::new(77),
                    false,
                    AdmissionLimits::default(),
                    grant(&f.budget, 40 * MIB, 10 * MIB),
                    || Ok(()),
                )
                .unwrap();
            assert!(match result.outcome() {
                AdmissionOutcome::Rejected(RejectionReason::Signature) => mode != "old-durable",
                AdmissionOutcome::Durable(receipt) =>
                    mode == "old-durable" && receipt.batch().is_none(),
                _ => false,
            });
        }
        drop(writer);
        let mut r = f.reopen();
        compact(&f, &r, residual);
        let closed = compacted_close(&f, &mut r, count);
        let before = capture(&f, &r.store);
        let (digest, records) = expected_closure(&f, &r, count, cut);
        let source = completed_source(&f, &r, &closed, &READY);
        assert!(source.has_compacted_initial());
        assert_eq!(
            (source.compacted_requests(), source.resolved_requests()),
            (4, count)
        );
        assert_eq!(
            (source.last_complete_sequence(), source.event_request_mask()),
            (cut, mask)
        );
        assert_eq!(source.digest(), digest);
        assert!(source.matches_recorded(records.clone()));
        assert!(source.matches_closed(&closed));
        assert!(source.validate().is_err());
        source.validate_compacted(&READY).unwrap();
        assert!(closed.source_resolution(&r, count).is_err());
        if count != 0 {
            for fault in ["missing", "duplicate", "input", "outcome"] {
                let mut changed = records.clone();
                match fault {
                    "missing" => {
                        changed.pop();
                    }
                    "duplicate" => changed.push(records[0]),
                    "input" => changed[0].1[0] ^= 1,
                    "outcome" => changed[0].2[0] ^= 1,
                    _ => unreachable!(),
                }
                assert!(!source.matches_recorded(changed), "{fault}");
            }
        }
        let ack = source.into_recovery_acknowledgement().unwrap();
        assert_eq!((ack.compacted_requests(), ack.issued()), (4, count));
        assert!(ack.validate(&r).is_err());
        ack.validate_compacted(&r, &READY).unwrap();
        unchanged(&f, &r.store, &before);
    }
}

#[test]
fn compacted_activated_source_confirmation_and_order_refusals_never_advance() {
    let f = closed_fixture();
    let mut writer = active(&f);
    assert_append(&f, &mut writer, 1);
    signed_new(&f, &mut writer, 2);
    drop(writer);
    let mut r = f.reopen();
    compact(&f, &r, 0);
    let closed = compacted_close(&f, &mut r, 2);
    let another = compacted_close(&f, &mut r, 2);
    let before = capture(&f, &r.store);
    let cancelled = AtomicBool::new(false);
    let mut cursor = closed
        .compacted_source_resolution(&r, 2, &cancelled)
        .unwrap();
    assert!(cursor.has_compacted_initial() && cursor.needs_compacted_initial());
    assert!(
        closed
            .compacted_source_resolution(&r, 2, &READY)
            .unwrap()
            .finish()
            .is_err()
    );
    {
        let one = target_request(&f, &r, &closed, 1).unwrap();
        assert_eq!(cursor.record(&one), Err(BootstrapError::StateMismatch));
    }
    let effects = grant(&f.budget, 40 * MIB, 0);
    let wrong = another
        .compacted_initial_ancestor(&r, AdmissionLimits::default(), &effects, &READY)
        .unwrap();
    assert!(
        cursor
            .confirm_compacted_initial(&canonical(&f, wrong.resolution(&READY).unwrap()), || Ok(()))
            .is_err()
    );
    assert!(cursor.needs_compacted_initial());
    let ancestor = closed
        .compacted_initial_ancestor(&r, AdmissionLimits::default(), &effects, &READY)
        .unwrap();
    let proof = canonical(&f, ancestor.resolution(&READY).unwrap());
    cancelled.store(true, std::sync::atomic::Ordering::Relaxed);
    assert!(cursor.confirm_compacted_initial(&proof, || Ok(())).is_err());
    assert!(cursor.needs_compacted_initial());
    cancelled.store(false, std::sync::atomic::Ordering::Relaxed);
    assert_eq!(
        cursor.confirm_compacted_initial(&proof, || Err(BootstrapError::AuthorityUnavailable)),
        Err(BootstrapError::AuthorityUnavailable)
    );
    assert!(cursor.needs_compacted_initial());
    cursor.confirm_compacted_initial(&proof, || Ok(())).unwrap();
    assert!(!cursor.needs_compacted_initial());
    assert!(cursor.confirm_compacted_initial(&proof, || Ok(())).is_err());
    assert!(confirmed_prefix(&f, &r, &closed, &READY).finish().is_err());
    let one = target_request(&f, &r, &closed, 1).unwrap();
    let two = target_request(&f, &r, &closed, 2).unwrap();
    assert_eq!(cursor.record(&two), Err(BootstrapError::StateMismatch));
    cursor.record(&one).unwrap();
    assert_eq!(cursor.record(&one), Err(BootstrapError::StateMismatch));
    cursor.record(&two).unwrap();
    let source = cursor.finish().unwrap();
    assert_eq!(source.digest(), expected_closure(&f, &r, 2, 5).0);
    assert!(!source.matches_closed(&another));
    unchanged(&f, &r.store, &before);
}

#[test]
fn compacted_activated_retirement_observes_complete_closing_without_native_writes() {
    for fault in [
        "open",
        "small-data",
        "small-native",
        "cancel",
        "authority",
        "final-authority",
        "reserved",
        "valid",
    ] {
        let f = closed_fixture();
        let mut writer = active(&f);
        assert_append(&f, &mut writer, 1);
        drop(writer);
        let mut r = f.reopen();
        compact(&f, &r, 2);
        let reserved = if fault == "reserved" { 2 } else { 1 };
        if fault != "open" {
            drop(compacted_close(&f, &mut r, reserved));
        }
        let view = compacted_view(&f, &r, reserved);
        let before = capture(&f, &r.store);
        let mut calls = 0;
        let result = view.observe_compacted_closed_producer(
            &r,
            grant(
                &f.budget,
                if fault == "small-data" { 4095 } else { 4096 },
                if fault == "small-native" { 4095 } else { 4096 },
            ),
            &AtomicBool::new(fault == "cancel"),
            || {
                calls += 1;
                if fault == "authority" || (fault == "final-authority" && calls == 2) {
                    Err(BootstrapError::AuthorityUnavailable)
                } else {
                    Ok(())
                }
            },
        );
        if fault == "valid" {
            let closed = result.unwrap();
            assert!(closed.validate(&r).is_err());
            let source = completed_source(&f, &r, &closed, &READY);
            assert_eq!(source.digest(), expected_closure(&f, &r, 1, 1).0);
        } else {
            assert!(result.is_err(), "{fault}");
        }
        unchanged(&f, &r.store, &before);
    }
}

#[test]
fn compacted_activated_source_cut_and_retirement_root_are_exact() {
    {
        let f = closed_fixture();
        let mut writer = active(&f);
        assert_append(&f, &mut writer, 1);
        signed_new(&f, &mut writer, 2);
        let next = QualifiedRef {
            ordinal: Ordinal::new(2).unwrap(),
            ..scope(&writer)
        };
        drop(
            writer
                .store_mut()
                .append_dictionaries(
                    next_request(3),
                    &dictionary(next, "tail"),
                    ObservedTime::new(77),
                    AdmissionLimits::default(),
                    append_permit(&f),
                    || Ok(()),
                )
                .unwrap(),
        );
        // A genuine extra source batch remains beyond the claimed two-request
        // prefix. Its input is merely the allowed unreserved staged tail.
        delete(
            writer.store(),
            IngestKey::Request {
                request_id: next_request(3),
            },
        );
        put(writer.store(), producer_key(2), &marker(2));
        drop(writer);
        let mut r = f.reopen();
        compact(&f, &r, 0);
        let closed = compacted_close(&f, &mut r, 2);
        let before = capture(&f, &r.store);
        let mut cursor = confirmed_prefix(&f, &r, &closed, &READY);
        for n in 1..=2 {
            cursor
                .record(&target_request(&f, &r, &closed, n).unwrap())
                .unwrap();
        }
        assert!(matches!(
            cursor.finish(),
            Err(BootstrapError::StateMismatch)
        ));
        unchanged(&f, &r.store, &before);
    }
    let f = closed_fixture();
    let mut writer = active(&f);
    assert_append(&f, &mut writer, 1);
    drop(writer);
    let mut r = f.reopen();
    compact(&f, &r, 0);
    drop(compacted_close(&f, &mut r, 1));
    drop(r);
    let clone = tempfile::TempDir::new().unwrap();
    let path = clone.path().join("db");
    copy_directory(&f.path(), &path);
    let r = f.reopen();
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
        f.previous.identity().dataset_id,
        f.native,
        grant(&foreign_budget, 0, 128 * MIB),
    )
    .unwrap();
    assert_eq!(kv(&r.store), kv(&foreign.store));
    assert_eq!(
        r.store.db.latest_sequence_number(),
        foreign.store.db.latest_sequence_number()
    );
    let before = capture(&f, &r.store);
    assert!(
        compacted_view(&f, &r, 1)
            .observe_compacted_closed_producer(
                &foreign,
                grant(&f.budget, 4096, 4096),
                &READY,
                || Ok(()),
            )
            .is_err()
    );
    let closed = compacted_view(&f, &r, 1)
        .observe_compacted_closed_producer(&r, grant(&f.budget, 4096, 4096), &READY, || Ok(()))
        .unwrap();
    assert!(
        closed
            .compacted_source_resolution(&foreign, 1, &READY)
            .is_err()
    );
    let source = completed_source(&f, &r, &closed, &READY);
    assert_eq!(source.digest(), expected_closure(&f, &r, 1, 1).0);
    assert!(
        source
            .compacted_initial_ancestor(AdmissionLimits::default(), &grant(&f.budget, 1, 0), &READY)
            .is_err()
    );
    unchanged(&f, &r.store, &before);
}

#[test]
fn compacted_activated_source_rechecks_sidecars_sequence_cancel_and_science() {
    for fault in [
        "sequence",
        "required-input",
        "residual",
        "cancel",
        "pending",
        "index",
    ] {
        let f = closed_fixture();
        let mut writer = active(&f);
        assert_append(&f, &mut writer, 1);
        drop(writer);
        let mut r = f.reopen();
        compact(&f, &r, 2);
        let closed = compacted_close(&f, &mut r, 1);
        let cancelled = AtomicBool::new(false);
        let source = completed_source(&f, &r, &closed, &cancelled);
        match fault {
            "sequence" => put(&r.store, handoff_key(), &value(&r.store, handoff_key())),
            "required-input" => {
                std::fs::remove_file(f.path().join("producer-inputs-v1").join(input_name(2, 1)))
                    .unwrap()
            }
            "residual" => {
                let path = f.path().join("producer-inputs-v1").join(input_name(1, 1));
                let mut bytes = std::fs::read(&path).unwrap();
                bytes[0] ^= 1;
                std::fs::write(path, bytes).unwrap();
            }
            "cancel" => cancelled.store(true, std::sync::atomic::Ordering::Relaxed),
            "pending" => delete(
                &r.store,
                IngestKey::PendingBatch {
                    first: fact_id(2, 1),
                },
            ),
            "index" => {
                let key = kv(&r.store)
                    .into_iter()
                    .find(|row| row.0 == "default" && row.1.starts_with(&[1, 5]))
                    .unwrap()
                    .1;
                r.store.db.delete(key).unwrap();
            }
            _ => unreachable!(),
        }
        let before = capture(&f, &r.store);
        assert!(source.validate_compacted(&READY).is_err(), "{fault}");
        assert!(source.recovery_acknowledgement().is_err(), "{fault}");
        assert!(
            source
                .compacted_initial_ancestor(
                    AdmissionLimits::default(),
                    &grant(&f.budget, 40 * MIB, 0),
                    &READY
                )
                .is_err(),
            "{fault}"
        );
        unchanged(&f, &r.store, &before);
    }
}

#[test]
fn compacted_activated_source_preserves_optional_tail_but_refuses_reserved_gaps() {
    for reserved in [1, 2] {
        let f = closed_fixture();
        let mut writer = active(&f);
        assert_append(&f, &mut writer, 1);
        drop(writer);
        let mut r = f.reopen();
        compact(&f, &r, 0);
        stage_tail(&f, &r, 2);
        let closed = compacted_close(&f, &mut r, reserved);
        let before = capture(&f, &r.store);
        if reserved == 2 {
            assert!(closed.compacted_source_resolution(&r, 2, &READY).is_err());
            assert!(closed.compacted_source_resolution(&r, 1, &READY).is_err());
        } else {
            let source = completed_source(&f, &r, &closed, &READY);
            assert_eq!(
                (source.resolved_requests(), source.last_complete_sequence()),
                (1, 1)
            );
            let ack = source.into_recovery_acknowledgement().unwrap();
            assert_eq!(ack.compacted_requests(), 4);
            drop(closed);
            r.store
                .test_acknowledge_compacted_target(
                    ack.compacted_target(1).unwrap(),
                    &grant(
                        &f.budget,
                        RecoveryAcknowledgement::COMPACTED_REQUIRED_DATA_BYTES,
                        4096,
                    ),
                    &READY,
                    || Ok(()),
                )
                .unwrap();
            assert_eq!(r.store.db.latest_sequence_number(), before.1 + 1);
            assert_eq!(inputs(&f.path()), before.2);
            let target = producer_key(2).encode().unwrap();
            assert_eq!(
                kv(&r.store)
                    .into_iter()
                    .filter(|row| row.1 != target.as_bytes())
                    .collect::<Vec<_>>(),
                before
                    .0
                    .into_iter()
                    .filter(|row| row.1 != target.as_bytes())
                    .collect::<Vec<_>>()
            );
            continue;
        }
        unchanged(&f, &r.store, &before);
    }
}

#[test]
fn compacted_activated_source_ack_handoff_retains_final_input_seal() {
    let f = closed_fixture();
    let mut writer = active(&f);
    assert_append(&f, &mut writer, 1);
    drop(writer);
    let mut r = f.reopen();
    compact(&f, &r, 0);
    stage_tail(&f, &r, 2);
    let closed = compacted_close(&f, &mut r, 1);
    let source = completed_source(&f, &r, &closed, &READY);
    let ordinary = source.recovery_acknowledgement().unwrap();
    assert!(ordinary.target(1).is_err());
    let target = source
        .into_recovery_acknowledgement()
        .unwrap()
        .compacted_target(1)
        .unwrap();
    drop(closed);
    let path = f.path().join("producer-inputs-v1").join(input_name(2, 2));
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[0] ^= 1;
    std::fs::write(path, bytes).unwrap();
    let before = capture(&f, &r.store);
    assert!(
        r.store
            .test_acknowledge_compacted_target(
                target,
                &grant(
                    &f.budget,
                    RecoveryAcknowledgement::COMPACTED_REQUIRED_DATA_BYTES,
                    4096
                ),
                &READY,
                || Ok(()),
            )
            .is_err()
    );
    unchanged(&f, &r.store, &before);
}
