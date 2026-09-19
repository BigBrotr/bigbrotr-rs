//! A real signed request prepared before the native closing barrier cannot write.
use super::*;
use crate::storage::rocks::keys::IngestKey;

#[test]
fn native_closing_preserves_effects_and_refuses_a_previously_verified_request() {
    let (root, mut store, budget, manifest, native) = setup(false);
    drop(admit(&mut store, &budget, 1).unwrap());
    let pending = PreparedEvent::new(
        request(5, 2),
        intent("E_private", 2, 9_000_000),
        256 * 1024,
        grant(&budget, MIB, 0),
    )
    .unwrap();
    let original = kv(&store);
    drop(store);
    let mut recovery = IngestRecovery::open(
        &root.path().join("db"),
        manifest,
        native,
        grant(&budget, 0, 128 * MIB),
    )
    .unwrap();
    let closed = recovery
        .close_producer(
            request(5, 1).producer_slot_id,
            ProducerEpoch::new(1).unwrap(),
            grant(&budget, 4096, 4096),
            || Ok(()),
        )
        .unwrap();
    assert_eq!(closed.issued_through(), 1);
    assert_eq!(closed.producer_slot_id(), request(5, 1).producer_slot_id);
    assert_eq!(closed.producer_epoch(), ProducerEpoch::new(1).unwrap());
    assert_eq!(closed.manifest(), &manifest);
    drop(closed);
    let mut expected = original;
    let key = IngestKey::ProducerState {
        producer_slot_id: request(5, 1).producer_slot_id,
        producer_epoch: ProducerEpoch::new(1).unwrap(),
    }
    .encode()
    .unwrap();
    let mut closing = vec![2, 0x31];
    closing.extend_from_slice(&1u64.to_be_bytes());
    closing.extend_from_slice(&[0; 16]);
    closing.push(1);
    let row = expected
        .iter_mut()
        .find(|(cf, k, _)| cf == "receipts" && k == key.as_bytes())
        .unwrap();
    row.2 = closing;
    assert_eq!(kv(&recovery.store), expected);
    let sequence = recovery.store.db.latest_sequence_number();
    drop(
        recovery
            .close_producer(
                request(5, 1).producer_slot_id,
                ProducerEpoch::new(1).unwrap(),
                grant(&budget, 4096, 4096),
                || Ok(()),
            )
            .unwrap(),
    );
    assert_eq!(recovery.store.db.latest_sequence_number(), sequence);
    assert!(matches!(
        pending.stage(
            &mut recovery.store,
            AdmissionLimits::default(),
            &grant(&budget, 40 * MIB, 10 * MIB)
        ),
        Err(AdmissionError::ProducerClosing)
    ));
    // Test-only access deliberately attacks the lower writer. Production callers
    // cannot extract this store from the recovery handle or dispatch admission.
    assert!(matches!(
        recovery.store.admit_event(
            pending,
            ObservedTime::new(100),
            false,
            AdmissionLimits::default(),
            grant(&budget, 40 * MIB, 10 * MIB),
            || Ok(())
        ),
        Err(AdmissionError::ProducerClosing)
    ));
    assert_eq!(kv(&recovery.store), expected);
    assert_eq!(recovery.store.db.latest_sequence_number(), sequence);
    assert_eq!(
        std::fs::read_dir(root.path().join("db/producer-inputs-v1"))
            .unwrap()
            .count(),
        2
    );
    drop(recovery);
    let mut recovery = IngestRecovery::open(
        &root.path().join("db"),
        manifest,
        native,
        grant(&budget, 0, 128 * MIB),
    )
    .unwrap();
    drop(
        recovery
            .close_producer(
                request(5, 1).producer_slot_id,
                ProducerEpoch::new(1).unwrap(),
                grant(&budget, 4096, 4096),
                || Ok(()),
            )
            .unwrap(),
    );
    assert_eq!(recovery.store.db.latest_sequence_number(), sequence);
    assert_eq!(kv(&recovery.store), expected);
}

#[test]
fn empty_producer_closing_preserves_zero_and_refuses_first_bootstrap() {
    let (root, store, budget, manifest, native) = setup(false);
    drop(store);
    let path = root.path().join("empty");
    let store =
        IngestStore::initialize(&path, manifest, native, grant(&budget, 0, 128 * MIB)).unwrap();
    let before = kv(&store);
    drop(store);
    let mut recovery =
        IngestRecovery::open(&path, manifest, native, grant(&budget, 0, 128 * MIB)).unwrap();
    let closed = recovery
        .close_producer(
            request(7, 1).producer_slot_id,
            ProducerEpoch::new(1).unwrap(),
            grant(&budget, 4096, 4096),
            || Ok(()),
        )
        .unwrap();
    assert_eq!(closed.issued_through(), 0);
    drop(closed);
    let bootstrap = EvidenceIntent::new(
        1,
        reference(DictionaryKind::VisibilityScope, 1),
        entries(false),
        EvidenceIntentLimits {
            max_entries: 16,
            max_encoded_intent_bytes: 8192,
            max_validation_scratch_bytes: 4096,
        },
    )
    .unwrap();
    let limits = BootstrapLimits {
        max_mappings: 16,
        max_batch_bytes: MIB as usize,
    };
    assert!(matches!(
        recovery.store.stage_initial_dictionaries(
            request(7, 1),
            &bootstrap,
            limits,
            &grant(&budget, 8 * MIB, 4 * MIB)
        ),
        Err(BootstrapError::ProducerClosing)
    ));
    assert!(matches!(
        recovery.store.bootstrap_dictionaries(
            request(7, 1),
            &bootstrap,
            ObservedTime::new(0),
            limits,
            grant(&budget, 8 * MIB, 4 * MIB),
            || Ok(())
        ),
        Err(BootstrapError::ProducerClosing)
    ));
    let rows = kv(&recovery.store);
    assert_eq!(rows.len(), before.len() + 1);
    assert!(before.iter().all(|row| rows.contains(row)));
    let mut expected = vec![2, 0x31];
    expected.extend_from_slice(&[0; 24]);
    expected.push(1);
    assert_eq!(
        rows.iter().find(|(cf, _, _)| cf == "receipts").unwrap().2,
        expected
    );
    assert!(!path.join("producer-inputs-v1").exists());
}

#[test]
fn lost_closing_authority_preserves_the_actual_before_or_after_commit_state() {
    for at in [1, 2, 3] {
        let (root, mut store, budget, manifest, native) = setup(false);
        drop(admit(&mut store, &budget, 1).unwrap());
        let before = kv(&store);
        let before_sequence = store.db.latest_sequence_number();
        drop(store);
        let mut recovery = IngestRecovery::open(
            &root.path().join("db"),
            manifest,
            native,
            grant(&budget, 0, 128 * MIB),
        )
        .unwrap();
        let mut calls = 0;
        assert!(matches!(
            recovery.close_producer(
                request(5, 1).producer_slot_id,
                ProducerEpoch::new(1).unwrap(),
                grant(&budget, 4096, 4096),
                || {
                    calls += 1;
                    if calls == at {
                        Err(BootstrapError::AuthorityUnavailable)
                    } else {
                        Ok(())
                    }
                }
            ),
            Err(BootstrapError::AuthorityUnavailable)
        ));
        assert_eq!(calls, at);
        if at < 3 {
            assert_eq!(kv(&recovery.store), before);
            assert_eq!(recovery.store.db.latest_sequence_number(), before_sequence);
        } else {
            assert!(matches!(
                admit(&mut recovery.store, &budget, 2),
                Err(AdmissionError::ProducerClosing)
            ));
            assert_eq!(
                recovery.store.db.latest_sequence_number(),
                before_sequence + 1
            );
            drop(
                recovery
                    .close_producer(
                        request(5, 1).producer_slot_id,
                        ProducerEpoch::new(1).unwrap(),
                        grant(&budget, 4096, 4096),
                        || Ok(()),
                    )
                    .unwrap(),
            );
            assert_eq!(
                recovery.store.db.latest_sequence_number(),
                before_sequence + 1
            );
        }
    }
}

#[test]
fn absent_or_wrong_producer_state_cannot_close_over_retained_request_detail() {
    for missing in [true, false] {
        let (root, mut store, budget, manifest, native) = setup(false);
        drop(admit(&mut store, &budget, 1).unwrap());
        let key = IngestKey::ProducerState {
            producer_slot_id: request(5, 1).producer_slot_id,
            producer_epoch: ProducerEpoch::new(1).unwrap(),
        }
        .encode()
        .unwrap();
        let cf = store.db.cf_handle("receipts").unwrap();
        if missing {
            store.db.delete_cf(cf, key.as_bytes()).unwrap();
        } else {
            let mut bytes = vec![1, 0x31];
            bytes.extend_from_slice(&[0; 24]);
            store.db.put_cf(cf, key.as_bytes(), bytes).unwrap();
        }
        let before = kv(&store);
        let sequence = store.db.latest_sequence_number();
        drop(store);
        let mut recovery = IngestRecovery::open(
            &root.path().join("db"),
            manifest,
            native,
            grant(&budget, 0, 128 * MIB),
        )
        .unwrap();
        assert!(matches!(
            recovery.close_producer(
                request(5, 1).producer_slot_id,
                ProducerEpoch::new(1).unwrap(),
                grant(&budget, 4096, 4096),
                || Ok(())
            ),
            Err(BootstrapError::StateMismatch)
        ));
        assert_eq!(kv(&recovery.store), before);
        assert_eq!(recovery.store.db.latest_sequence_number(), sequence);
    }
}

#[test]
fn closing_format_has_exact_version_length_and_full_unsigned_counters() {
    use super::super::producer_closing::ProducerState;
    let closing = [
        2, 0x31, 1, 2, 3, 4, 5, 6, 7, 8, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 1, 1,
    ];
    let value = ProducerState::decode(&closing).unwrap();
    assert_eq!(
        (
            value.issued,
            value.acknowledged,
            value.retired,
            value.closing
        ),
        (0x0102_0304_0506_0708, 2, 1, true)
    );
    for n in 0..closing.len() {
        assert!(ProducerState::decode(&closing[..n]).is_err());
    }
    let mut extra = closing.to_vec();
    extra.push(0);
    assert!(ProducerState::decode(&extra).is_err());
    for (at, byte) in [(0, 3), (1, 0x30), (26, 0), (26, 2), (25, 3)] {
        let mut altered = closing;
        altered[at] = byte;
        assert!(ProducerState::decode(&altered).is_err());
    }
    let mut maximum = [255; 27];
    maximum[..2].copy_from_slice(&[2, 0x31]);
    maximum[26] = 1;
    let value = ProducerState::decode(&maximum).unwrap();
    assert_eq!(
        (value.issued, value.acknowledged, value.retired),
        (u64::MAX, u64::MAX, u64::MAX)
    );
}

#[test]
fn closing_refuses_missing_or_excess_boundary_and_unsupported_ack_without_writes() {
    for case in ["missing-last", "excess-last", "acknowledged", "retired"] {
        let (root, mut store, budget, manifest, native) = setup(false);
        drop(admit(&mut store, &budget, 1).unwrap());
        let cf = store.db.cf_handle("receipts").unwrap();
        let key = |n| {
            IngestKey::Request {
                request_id: request(5, n),
            }
            .encode()
            .unwrap()
        };
        if case == "missing-last" {
            store.db.delete_cf(cf, key(1).as_bytes()).unwrap();
        } else if case == "excess-last" {
            let bytes = store.db.get_cf(cf, key(1).as_bytes()).unwrap().unwrap();
            store.db.put_cf(cf, key(2).as_bytes(), bytes).unwrap();
        } else {
            let key = IngestKey::ProducerState {
                producer_slot_id: request(5, 1).producer_slot_id,
                producer_epoch: ProducerEpoch::new(1).unwrap(),
            }
            .encode()
            .unwrap();
            let mut value = vec![1, 0x31];
            for n in [1u64, 1, u64::from(case == "retired")] {
                value.extend_from_slice(&n.to_be_bytes());
            }
            store.db.put_cf(cf, key.as_bytes(), value).unwrap();
        }
        let before = kv(&store);
        let sequence = store.db.latest_sequence_number();
        drop(store);
        let mut recovery = IngestRecovery::open(
            &root.path().join("db"),
            manifest,
            native,
            grant(&budget, 0, 128 * MIB),
        )
        .unwrap();
        assert!(
            matches!(
                recovery.close_producer(
                    request(5, 1).producer_slot_id,
                    ProducerEpoch::new(1).unwrap(),
                    grant(&budget, 4096, 4096),
                    || Ok(())
                ),
                Err(BootstrapError::StateMismatch)
            ),
            "{case}"
        );
        assert_eq!(kv(&recovery.store), before, "{case}");
        assert_eq!(
            recovery.store.db.latest_sequence_number(),
            sequence,
            "{case}"
        );
    }
}

#[test]
fn adjacent_producer_receipts_do_not_close_the_target_at_the_wrong_prefix() {
    let (root, mut store, budget, manifest, native) = setup(false);
    drop(admit(&mut store, &budget, 1).unwrap());
    // The native store has producers5 and7. Closing absent6 must use zero even
    // though a lexicographically preceding producer has a retained request.
    drop(store);
    let mut recovery = IngestRecovery::open(
        &root.path().join("db"),
        manifest,
        native,
        grant(&budget, 0, 128 * MIB),
    )
    .unwrap();
    let zero = recovery
        .close_producer(
            request(6, 1).producer_slot_id,
            ProducerEpoch::new(1).unwrap(),
            grant(&budget, 4096, 4096),
            || Ok(()),
        )
        .unwrap();
    assert_eq!(zero.issued_through(), 0);
    drop(zero);
    // Conversely, neighboring valid detail cannot substitute the target's
    // missing highest request. Delete only5/1; producer7 remains intact.
    let key = IngestKey::Request {
        request_id: request(5, 1),
    }
    .encode()
    .unwrap();
    recovery
        .store
        .db
        .delete_cf(
            recovery.store.db.cf_handle("receipts").unwrap(),
            key.as_bytes(),
        )
        .unwrap();
    let before = kv(&recovery.store);
    let sequence = recovery.store.db.latest_sequence_number();
    assert!(matches!(
        recovery.close_producer(
            request(5, 1).producer_slot_id,
            ProducerEpoch::new(1).unwrap(),
            grant(&budget, 4096, 4096),
            || Ok(())
        ),
        Err(BootstrapError::StateMismatch)
    ));
    assert_eq!(kv(&recovery.store), before);
    assert_eq!(recovery.store.db.latest_sequence_number(), sequence);
}

#[test]
fn insufficient_grant_and_uncertain_native_handle_cannot_install_closing() {
    let (root, store, budget, manifest, native) = setup(false);
    // Real path reservation/native LOCK excludes recovery while admission owns
    // the directory. Failed open must not consume or release the first owner.
    assert!(matches!(
        IngestRecovery::open(
            &root.path().join("db"),
            manifest,
            native,
            grant(&budget, 0, 128 * MIB)
        ),
        Err(super::super::StoreError::AlreadyOpen)
    ));
    drop(store);
    let mut recovery = IngestRecovery::open(
        &root.path().join("db"),
        manifest,
        native,
        grant(&budget, 0, 128 * MIB),
    )
    .unwrap();
    let before = kv(&recovery.store);
    let sequence = recovery.store.db.latest_sequence_number();
    for (data, native) in [(4095, 4096), (4096, 4095)] {
        assert!(matches!(
            recovery.close_producer(
                request(7, 1).producer_slot_id,
                ProducerEpoch::new(1).unwrap(),
                grant(&budget, data, native),
                || panic!("unreserved operation dispatched")
            ),
            Err(BootstrapError::InsufficientReservation)
        ));
    }
    recovery.store.write_uncertain = true;
    assert!(matches!(
        recovery.close_producer(
            request(7, 1).producer_slot_id,
            ProducerEpoch::new(1).unwrap(),
            grant(&budget, 4096, 4096),
            || Ok(())
        ),
        Err(BootstrapError::CommitUnknown)
    ));
    assert_eq!(kv(&recovery.store), before);
    assert_eq!(recovery.store.db.latest_sequence_number(), sequence);
}
