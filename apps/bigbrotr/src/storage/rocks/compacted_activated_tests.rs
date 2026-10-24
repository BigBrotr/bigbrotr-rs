//! Existing real native fixture, literal synthetic floor and independently
//! retained original bytes. Native authority is synthetic; no SQL claim.
use super::*;
use crate::storage::rocks::{
    store::{
        COMPACTED_HANDOFF_DATA_BYTES, COMPACTED_PREFIX_METADATA_BYTES, CompactedOutcomeRow,
        CompactedPrefixResolution, NativeOutcomeKind, RecoveryAcknowledgement,
        RetainedCompactedPrefix, VerifiedCompactedPrefix,
    },
    succession::SuccessionProfile,
};
use std::sync::atomic::AtomicBool;

static READY: AtomicBool = AtomicBool::new(false);

#[path = "compacted_activated_terminal_tests.rs"]
mod terminal_tests;

fn metadata(f: &Fixture) -> OwnedPermit {
    grant(
        &f.budget,
        SuccessionProfile::for_recovery(2, None)
            .unwrap()
            .required_metadata_bytes(),
        0,
    )
}
fn floor(f: &Fixture) -> [u8; 139] {
    let mut bytes = [0; 139];
    bytes[..2].copy_from_slice(&[3, 0x31]);
    for at in [2, 10, 18] {
        bytes[at..at + 8].copy_from_slice(&4_u64.to_be_bytes());
    }
    bytes[26] = 1;
    bytes[27..59].copy_from_slice(&Sha256::digest(f.previous.encode().as_bytes()));
    bytes[59..67].copy_from_slice(&9_u64.to_be_bytes());
    bytes[67..99].copy_from_slice(&f.closure);
    bytes[99..107].copy_from_slice(&5_u64.to_be_bytes());
    bytes[107..].fill(0x45);
    bytes
}
fn compact(f: &Fixture, r: &IngestRecovery, residual: u64) {
    let cf = r.store.db.cf_handle("receipts").unwrap();
    let mut batch = rocksdb::WriteBatch::default();
    batch.put_cf(cf, producer_key(1).encode().unwrap().as_bytes(), floor(f));
    for n in 1..=4 {
        batch.delete_cf(
            cf,
            IngestKey::Request {
                request_id: request(7, n),
            }
            .encode()
            .unwrap()
            .as_bytes(),
        );
    }
    r.store.db.write(batch).unwrap();
    for n in residual + 1..=4 {
        std::fs::remove_file(f.path().join("producer-inputs-v1").join(input_name(1, n))).unwrap();
    }
}
fn canonical<'a>(
    f: &Fixture,
    mut cursor: CompactedPrefixResolution<'a>,
) -> VerifiedCompactedPrefix<'a> {
    let effects = grant(&f.budget, 40 * MIB, 0);
    for (i, original) in f.originals.iter().enumerate() {
        cursor
            .record(
                CompactedOutcomeRow {
                    request: request(7, i as u64 + 1),
                    digest: input_digest(original),
                    kind: if i == 3 {
                        NativeOutcomeKind::AlreadyDurable
                    } else {
                        NativeOutcomeKind::NewFacts
                    },
                    policy_epoch: PolicyEpoch::new(1),
                    bytes: &original.outcome,
                    input_bytes: original.input.len() as u32,
                },
                AdmissionLimits::default(),
                &effects,
                &READY,
                || Ok(()),
            )
            .unwrap();
    }
    cursor.finish(&READY).unwrap()
}
fn compacted_view(f: &Fixture, r: &IngestRecovery, reserved: u64) -> ActivatedInitial {
    ActivatedInitial::inspect_compacted(r, record(r), reserved, metadata(f), &READY).unwrap()
}
fn compacted_close(f: &Fixture, r: &mut IngestRecovery, reserved: u64) -> ActivatedProducerClosing {
    compacted_view(f, r, reserved)
        .close_compacted_producer(r, grant(&f.budget, 4096, 4096), &READY, || Ok(()))
        .unwrap()
}
fn target_request(
    f: &Fixture,
    r: &IngestRecovery,
    closed: &ActivatedProducerClosing,
    n: u64,
) -> Result<ResolvedProducerRequest, ProducerRecoveryError> {
    let bytes = std::fs::read(f.path().join("producer-inputs-v1").join(input_name(2, n))).unwrap();
    closed.resolve_compacted_original_request(
        r,
        next_request(n),
        Digest256::from_bytes(Sha256::digest(bytes).into()),
        recovery_limits(),
        grant(&f.budget, 3 * MIB, 0),
        grant(&f.budget, 40 * MIB, 0),
        &READY,
        || Ok(()),
    )
}
fn completed(
    f: &Fixture,
    r: &IngestRecovery,
    closed: &ActivatedProducerClosing,
) -> RecoveryAcknowledgement {
    let mut cursor = closed
        .compacted_acknowledgement_resolution(r, closed.issued_through(), &READY)
        .unwrap();
    let effects = grant(&f.budget, 40 * MIB, 0);
    let ancestor = closed
        .compacted_initial_ancestor(r, AdmissionLimits::default(), &effects, &READY)
        .unwrap();
    let proof = canonical(f, ancestor.resolution(&READY).unwrap());
    cursor.confirm_compacted_initial(&proof, || Ok(())).unwrap();
    for n in 1..=closed.issued_through() {
        cursor
            .record(&target_request(f, r, closed, n).unwrap())
            .unwrap();
    }
    cursor.finish().unwrap()
}

#[test]
fn compacted_activated_recovery_separates_ancestor_and_target_and_ack_preserves_all_bytes() {
    for (count, residual) in [(0, 4), (1, 2), (2, 0)] {
        let f = closed_fixture();
        let mut writer = active(&f);
        if count > 0 {
            assert_append(&f, &mut writer, 1);
        }
        if count > 1 {
            signed_new(&f, &mut writer, 2);
        }
        drop(writer);
        let mut r = f.reopen();
        compact(&f, &r, residual);
        assert_eq!(
            ActivatedInitial::compacted_initial_selected(&r, record(&r)),
            Ok(true)
        );
        assert!(ActivatedInitial::inspect(&r, record(&r)).is_err());
        let before = capture(&f, &r.store);
        let view = compacted_view(&f, &r, count);
        assert!(view.validate(&r).is_err());
        assert!(
            view.close_producer(&mut r, grant(&f.budget, 4096, 4096), || Ok(()))
                .is_err()
        );
        assert!(
            view.observe_closed_producer(&r, grant(&f.budget, 4096, 4096), || Ok(()))
                .is_err()
        );
        let closed = view
            .close_compacted_producer(&mut r, grant(&f.budget, 4096, 4096), &READY, || Ok(()))
            .unwrap();
        assert_eq!(r.store.db.latest_sequence_number(), before.1 + 1);
        assert!(closed.source_resolution(&r, count).is_err());
        assert!(closed.validate(&r).is_err());
        let stable = capture(&f, &r.store);
        let ack = completed(&f, &r, &closed);
        assert_eq!(ack.compacted_requests(), 4);
        assert_eq!(ack.issued(), count);
        assert!(ack.validate(&r).is_err());
        let scratch = grant(
            &f.budget,
            RecoveryAcknowledgement::COMPACTED_REQUIRED_DATA_BYTES,
            4096,
        );
        let final_view = ack
            .compacted_initial_predecessor(&r, &scratch, &READY)
            .unwrap()
            .unwrap();
        let final_proof = canonical(&f, final_view.resolution(&READY).unwrap());
        assert_eq!(final_proof.floor().encode(), floor(&f));
        assert!(InitialHandoff::from_compacted(&final_proof, f.next, &READY).is_err());
        unchanged(&f, &r.store, &stable);
        drop(closed);
        let target = ack.compacted_target(count).unwrap();
        r.store
            .test_acknowledge_compacted_target(target, &scratch, &READY, || Ok(()))
            .unwrap();
        let mut rows = stable.0;
        let key = producer_key(2).encode().unwrap();
        rows.iter_mut()
            .find(|row| row.0 == "receipts" && row.1 == key.as_bytes())
            .unwrap()
            .2[10..18]
            .copy_from_slice(&count.to_be_bytes());
        assert_eq!(kv(&r.store), rows);
        assert_eq!(
            r.store.db.latest_sequence_number(),
            stable.1 + u64::from(count > 0)
        );
        assert_eq!(inputs(&f.path()), stable.2);
    }
}

#[test]
fn compacted_activated_pending_requires_exact_ancestor_owner_and_never_partially_advances() {
    let f = closed_fixture();
    let mut writer = active(&f);
    assert_append(&f, &mut writer, 1);
    drop(writer);
    let mut r = f.reopen();
    compact(&f, &r, 0);
    let closed = compacted_close(&f, &mut r, 1);
    let other = compacted_close(&f, &mut r, 1);
    let original = target_request(&f, &r, &closed, 1).unwrap();
    let cancelled = AtomicBool::new(false);
    let mut cursor = closed
        .compacted_acknowledgement_resolution(&r, 1, &cancelled)
        .unwrap();
    assert!(cursor.needs_compacted_initial());
    assert!(cursor.record(&original).is_err());
    assert!(
        closed
            .compacted_acknowledgement_resolution(&r, 1, &READY)
            .unwrap()
            .finish()
            .is_err()
    );
    let effects = grant(&f.budget, 40 * MIB, 0);
    let wrong = other
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
    cursor.record(&original).unwrap();
    assert!(cursor.record(&original).is_err());
    assert_eq!(cursor.finish().unwrap().issued(), 1);
}

#[test]
fn compacted_activated_census_refuses_incompatible_floor_missing_science_and_required_originals() {
    for fault in [
        "floor-manifest",
        "floor-cut",
        "floor-digest",
        "handoff",
        "later-floor",
        "pending1",
        "pending2",
        "reverse",
        "fact",
        "target-input",
        "target-input-with-tail",
        "residual-corrupt",
        "third-marker",
    ] {
        let f = closed_fixture();
        let mut writer = active(&f);
        assert_append(&f, &mut writer, 1);
        drop(writer);
        let r = f.reopen();
        compact(&f, &r, 1);
        let retained_record = record(&r);
        match fault {
            "floor-manifest" | "floor-cut" | "floor-digest" => {
                let mut bytes = floor(&f);
                bytes[match fault {
                    "floor-manifest" => 27,
                    "floor-cut" => 66,
                    _ => 67,
                }] ^= 1;
                put(&r.store, producer_key(1), &bytes);
            }
            "handoff" => {
                let mut bytes = value(&r.store, handoff_key());
                bytes[45] ^= 1;
                put(&r.store, handoff_key(), &bytes);
            }
            "later-floor" => put(&r.store, producer_key(2), &floor(&f)),
            "pending1" | "pending2" => delete(
                &r.store,
                IngestKey::PendingBatch {
                    first: fact_id(if fault == "pending1" { 1 } else { 2 }, 1),
                },
            ),
            "reverse" => {
                let row = kv(&r.store)
                    .into_iter()
                    .find(|row| row.0 == "default" && row.1.starts_with(&[1, 5]))
                    .unwrap();
                r.store.db.delete(&row.1).unwrap();
            }
            "fact" => {
                let key = IngestKey::Fact {
                    source_record_id: fact_id(1, 1),
                };
                let mut bytes = value(&r.store, key);
                *bytes.last_mut().unwrap() ^= 1;
                put(&r.store, key, &bytes);
            }
            "target-input" | "target-input-with-tail" => {
                std::fs::remove_file(f.path().join("producer-inputs-v1").join(input_name(2, 1)))
                    .unwrap();
                if fault == "target-input-with-tail" {
                    let path = f.path().join("producer-inputs-v1").join(input_name(2, 2));
                    std::fs::write(&path, &f.originals[0].input).unwrap();
                    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
                }
            }
            "residual-corrupt" => std::fs::write(
                f.path().join("producer-inputs-v1").join(input_name(1, 1)),
                b"changed-original",
            )
            .unwrap(),
            "third-marker" => put(&r.store, producer_key(3), &marker(0)),
            _ => unreachable!(),
        }
        let before = capture(&f, &r.store);
        let result =
            ActivatedInitial::inspect_compacted(&r, retained_record, 1, metadata(&f), &READY);
        if fault == "residual-corrupt" {
            let view = result.unwrap();
            let effects = grant(&f.budget, 40 * MIB, 0);
            let ancestor = view
                .compacted_initial_ancestor(&r, AdmissionLimits::default(), &effects, &READY)
                .unwrap();
            let original = &f.originals[0];
            assert!(
                ancestor
                    .resolution(&READY)
                    .unwrap()
                    .record(
                        CompactedOutcomeRow {
                            request: request(7, 1),
                            digest: input_digest(original),
                            kind: NativeOutcomeKind::NewFacts,
                            policy_epoch: PolicyEpoch::new(1),
                            bytes: &original.outcome,
                            input_bytes: original.input.len() as u32
                        },
                        AdmissionLimits::default(),
                        &effects,
                        &READY,
                        || Ok(())
                    )
                    .is_err()
            );
        } else if let Ok(view) = result {
            assert!(
                view.compacted_initial_ancestor(
                    &r,
                    AdmissionLimits::default(),
                    &grant(&f.budget, 40 * MIB, 0),
                    &READY
                )
                .is_err(),
                "{fault}"
            );
        }
        unchanged(&f, &r.store, &before);
    }
}

#[test]
fn compacted_activated_stamps_refuse_stale_sequence_sidecars_cancellation_and_small_grants() {
    for fault in [
        "sequence",
        "sidecar",
        "cancel",
        "metadata",
        "effect",
        "closing-data",
        "postwrite-authority",
    ] {
        let f = closed_fixture();
        drop(active(&f));
        let mut r = f.reopen();
        compact(&f, &r, 0);
        let before = capture(&f, &r.store);
        if fault == "metadata" {
            assert!(matches!(
                ActivatedInitial::inspect_compacted(
                    &r,
                    record(&r),
                    0,
                    grant(
                        &f.budget,
                        SuccessionProfile::for_recovery(2, None)
                            .unwrap()
                            .required_metadata_bytes()
                            - 1,
                        0
                    ),
                    &READY
                ),
                Err(BootstrapError::InsufficientReservation)
            ));
        } else {
            let view = compacted_view(&f, &r, 0);
            if fault == "sequence" {
                let bytes = value(&r.store, handoff_key());
                put(&r.store, handoff_key(), &bytes);
                assert!(view.validate_compacted(&r, &READY).is_err());
                continue;
            }
            if fault == "sidecar" {
                std::fs::write(
                    f.path().join("producer-inputs-v1").join(input_name(1, 1)),
                    &f.originals[0].input,
                )
                .unwrap();
                assert!(view.validate_compacted(&r, &READY).is_err());
                continue;
            }
            if fault == "effect" {
                assert!(
                    view.compacted_initial_ancestor(
                        &r,
                        AdmissionLimits::default(),
                        &grant(&f.budget, 1, 0),
                        &READY
                    )
                    .is_err()
                );
            } else {
                let mut calls = 0;
                let result = view.close_compacted_producer(
                    &mut r,
                    grant(
                        &f.budget,
                        if fault == "closing-data" { 4095 } else { 4096 },
                        4096,
                    ),
                    &AtomicBool::new(fault == "cancel"),
                    || {
                        calls += 1;
                        if fault == "postwrite-authority" && calls == 3 {
                            Err(BootstrapError::AuthorityUnavailable)
                        } else {
                            Ok(())
                        }
                    },
                );
                assert!(result.is_err(), "{fault}");
                if fault == "postwrite-authority" {
                    assert_eq!(r.store.db.latest_sequence_number(), before.1 + 1);
                    assert_eq!(value(&r.store, producer_key(2)), closing_marker(0));
                    continue;
                }
            }
        }
        unchanged(&f, &r.store, &before);
    }
}

#[test]
fn compacted_activated_unreserved_tail_is_preserved_but_reserved_tail_cannot_ack() {
    for (reserved, present) in [(1, true), (3, true), (3, false)] {
        let f = closed_fixture();
        let mut writer = active(&f);
        assert_append(&f, &mut writer, 1);
        drop(writer);
        let mut r = f.reopen();
        compact(&f, &r, 0);
        for n in 2..=if !present {
            1
        } else if reserved == 1 {
            2
        } else {
            3
        } {
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
        let closed = compacted_close(&f, &mut r, reserved);
        let before = capture(&f, &r.store);
        if reserved > 1 {
            assert!(matches!(
                closed.resolve_compacted_original_request(
                    &r,
                    next_request(2),
                    Digest256::from_bytes([5; 32]),
                    recovery_limits(),
                    grant(&f.budget, 3 * MIB, 0),
                    grant(&f.budget, 40 * MIB, 0),
                    &READY,
                    || Ok(())
                ),
                Err(ProducerRecoveryError::Unresolved)
            ));
            assert!(
                closed
                    .compacted_acknowledgement_resolution(&r, reserved, &READY)
                    .is_err()
            );
        } else {
            let ack = completed(&f, &r, &closed);
            let permit = grant(
                &f.budget,
                RecoveryAcknowledgement::COMPACTED_REQUIRED_DATA_BYTES,
                4096,
            );
            ack.validate_compacted(&r, &READY).unwrap();
            r.store
                .test_acknowledge_compacted_target(
                    ack.compacted_target(1).unwrap(),
                    &permit,
                    &READY,
                    || Ok(()),
                )
                .unwrap();
            assert_eq!(inputs(&f.path()), before.2);
            assert_eq!(r.store.db.latest_sequence_number(), before.1 + 1);
        }
    }
}

#[test]
fn compacted_activated_ack_target_rechecks_optional_tail_after_token_creation() {
    for fault in [
        "changed",
        "cancel",
        "small-grant",
        "ordinary",
        "postwrite-tail",
    ] {
        let f = closed_fixture();
        let mut writer = active(&f);
        assert_append(&f, &mut writer, 1);
        drop(writer);
        let mut r = f.reopen();
        compact(&f, &r, 0);
        let closed = compacted_close(&f, &mut r, 1);
        let ack = completed(&f, &r, &closed);
        drop(closed);
        let target = ack.compacted_target(1).unwrap();
        if fault == "changed" {
            let path = f.path().join("producer-inputs-v1").join(input_name(2, 2));
            std::fs::write(&path, b"new-tail").unwrap();
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        let before = capture(&f, &r.store);
        let permit = grant(
            &f.budget,
            RecoveryAcknowledgement::COMPACTED_REQUIRED_DATA_BYTES
                - u64::from(fault == "small-grant"),
            4096,
        );
        let result = if fault == "ordinary" {
            r.store.test_acknowledge_target(target, &permit, || Ok(()))
        } else {
            let mut calls = 0;
            r.store.test_acknowledge_compacted_target(
                target,
                &permit,
                &AtomicBool::new(fault == "cancel"),
                || {
                    calls += 1;
                    if fault == "postwrite-tail" && calls == 3 {
                        let path = f.path().join("producer-inputs-v1").join(input_name(2, 2));
                        std::fs::write(&path, b"staged-during-final-delivery").unwrap();
                        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
                            .unwrap();
                    }
                    Ok(())
                },
            )
        };
        assert!(result.is_err(), "{fault}");
        if fault == "postwrite-tail" {
            assert_eq!(r.store.db.latest_sequence_number(), before.1 + 1);
            let mut wanted = closing_marker(1);
            wanted[10..18].copy_from_slice(&1_u64.to_be_bytes());
            assert_eq!(value(&r.store, producer_key(2)), wanted);
            continue;
        }
        unchanged(&f, &r.store, &before);
    }
}

#[test]
fn compacted_activated_context_binds_actual_root_and_cannot_mint_prepared_handoff() {
    let f = closed_fixture();
    drop(active(&f));
    let r = f.reopen();
    compact(&f, &r, 0);
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
    let mut foreign = IngestRecovery::inspect(
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
    let view = compacted_view(&f, &r, 0);
    assert!(view.validate_compacted(&foreign, &READY).is_err());
    let effects = grant(&f.budget, 40 * MIB, 0);
    let ancestor = view
        .compacted_initial_ancestor(&r, AdmissionLimits::default(), &effects, &READY)
        .unwrap();
    let proof = canonical(&f, ancestor.resolution(&READY).unwrap());
    let handoff = value(&r.store, handoff_key());
    let prepared = r.historical_initial(&handoff).unwrap();
    assert!(
        prepared
            .reconstruct_compacted_handoff(&proof, &READY)
            .is_err()
    );
    let before = kv(&foreign.store);
    let sequence = foreign.store.db.latest_sequence_number();
    assert!(
        view.close_compacted_producer(
            &mut foreign,
            grant(&f.budget, 4096, 4096),
            &READY,
            || Ok(())
        )
        .is_err()
    );
    assert_eq!(kv(&foreign.store), before);
    assert_eq!(foreign.store.db.latest_sequence_number(), sequence);
}

#[test]
fn compacted_activated_optional_range_keeps_the_physical_file_ceiling() {
    let f = closed_fixture();
    drop(active(&f));
    let r = f.reopen();
    compact(&f, &r, 0);
    for n in 1..=64 {
        let path = f.path().join("producer-inputs-v1").join(input_name(2, n));
        std::fs::write(&path, &f.originals[0].input).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    let before = capture(&f, &r.store);
    drop(compacted_view(&f, &r, 64));
    unchanged(&f, &r.store, &before);
    let path = f.path().join("producer-inputs-v1").join(input_name(1, 1));
    std::fs::write(&path, &f.originals[0].input).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
    let before = capture(&f, &r.store);
    assert!(matches!(
        ActivatedInitial::inspect_compacted(&r, record(&r), 64, metadata(&f), &READY),
        Err(BootstrapError::Limit)
    ));
    unchanged(&f, &r.store, &before);
}

#[test]
fn compacted_activated_logical_requests_can_exceed_physical_spool_limit() {
    let f = closed_fixture();
    let r = f.reopen();
    compact(&f, &r, 0);
    let mut candidate = InitialWriterCandidate::new(r).unwrap();
    let handoff = {
        let prefix = RetainedCompactedPrefix::inspect_initial(
            candidate.recovery(),
            request(7, 1).producer_slot_id,
            AdmissionLimits::default(),
            grant(&f.budget, COMPACTED_PREFIX_METADATA_BYTES, 0),
            grant(&f.budget, 40 * MIB, 0),
            &READY,
        )
        .unwrap();
        candidate
            .handoff_compacted(
                &canonical(&f, prefix.resolution(candidate.recovery(), &READY).unwrap()),
                PolicyEpoch::new(1),
                &READY,
            )
            .unwrap()
    };
    drop(
        candidate
            .recovery_mut()
            .apply_compacted_initial_handoff(
                &handoff,
                grant(&f.budget, COMPACTED_HANDOFF_DATA_BYTES, 8192),
                &READY,
                || Ok(()),
            )
            .unwrap(),
    );
    let mut writer = candidate
        .activate_compacted(
            handoff,
            &grant(&f.budget, COMPACTED_HANDOFF_DATA_BYTES, 8192),
            &READY,
        )
        .unwrap();
    for n in 1..=61 {
        let outcome = writer
            .store_mut()
            .admit_event(
                event(&f, n, true),
                ObservedTime::new(77),
                false,
                AdmissionLimits::default(),
                grant(&f.budget, 40 * MIB, 10 * MIB),
                || Ok(()),
            )
            .unwrap();
        assert!(matches!(
            outcome.outcome(),
            AdmissionOutcome::Rejected(RejectionReason::Signature)
        ));
    }
    drop(writer);
    let mut r = f.reopen();
    let closed = compacted_close(&f, &mut r, 61);
    let before = capture(&f, &r.store);
    let ack = completed(&f, &r, &closed);
    assert_eq!(ack.issued(), 61);
    assert_eq!(ack.compacted_requests(), 4);
    assert_eq!(inputs(&f.path()).len(), 61);
    let source = terminal_tests::completed_source(&f, &r, &closed, &READY);
    assert_eq!(
        (source.compacted_requests(), source.resolved_requests()),
        (4, 61)
    );
    assert_eq!(source.event_request_mask(), (1_u64 << 61) - 1);
    assert_eq!(source.last_complete_sequence(), 0);
    assert_eq!(
        source.digest(),
        super::activated_source_tests::expected_closure(&f, &r, 61, 0).0
    );
    unchanged(&f, &r.store, &before);
}
