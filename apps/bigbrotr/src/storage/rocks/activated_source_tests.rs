//! Full target prefixes from actual native replay. The existing fixture supplies
//! synthetic activation authority only; SQL/installed qualification is separate.
use super::*;

#[path = "activated_retirement_tests.rs"]
mod activated_retirement_tests;

type Digests = Vec<(u64, [u8; 32], [u8; 32])>;

/// Independent literal framing from retained preimages/outcomes, with an explicit
/// expected request count and source cut. No production closure encoder is used.
fn expected_closure(
    fixture: &Fixture,
    recovery: &IngestRecovery,
    count: u64,
    cut: u64,
) -> ([u8; 32], Digests) {
    let mut bytes = b"bigbrotr/activated-initial-source-closure/1\0".to_vec();
    bytes.extend(Sha256::digest(value(&recovery.store, handoff_key())));
    bytes.extend(Sha256::digest(value(&recovery.store, IngestKey::Manifest)));
    bytes.extend([7; 16]);
    bytes.extend(2_u64.to_be_bytes());
    bytes.extend(count.to_be_bytes());
    let mut records = Vec::new();
    for sequence in 1..=count {
        let input: [u8; 32] = Sha256::digest(
            std::fs::read(
                fixture
                    .path()
                    .join("producer-inputs-v1")
                    .join(input_name(2, sequence)),
            )
            .unwrap(),
        )
        .into();
        let outcome: [u8; 32] = Sha256::digest(value(
            &recovery.store,
            IngestKey::Request {
                request_id: next_request(sequence),
            },
        ))
        .into();
        bytes.extend(sequence.to_be_bytes());
        bytes.extend(input);
        bytes.extend(outcome);
        records.push((sequence, input, outcome));
    }
    bytes.extend(cut.to_be_bytes());
    (Sha256::digest(bytes).into(), records)
}

// Reuse exact native replay for the mandatory epoch1 prefix. Its expected
// complete digest/cut comes from the retained handoff, not these test inputs.
fn full_prefix<'a>(
    fixture: &Fixture,
    recovery: &'a IngestRecovery,
    closed: &'a ActivatedProducerClosing,
    count: u64,
) -> crate::storage::rocks::store::ActivatedSourceResolution<'a> {
    let mut prefix = closed.source_resolution(recovery, count).unwrap();
    for sequence in 1..=closed.predecessor_issued_through() {
        let bytes = std::fs::read(
            fixture
                .path()
                .join("producer-inputs-v1")
                .join(input_name(1, sequence)),
        )
        .unwrap();
        let resolved = closed
            .resolve_predecessor_request(
                recovery,
                request(7, sequence),
                Digest256::from_bytes(Sha256::digest(bytes).into()),
                recovery_limits(),
                grant(&fixture.budget, 3 * MIB, 0),
                grant(&fixture.budget, 40 * MIB, 0),
                || Ok(()),
            )
            .unwrap();
        prefix.record(&resolved).unwrap();
    }
    prefix
}

#[test]
fn activated_ack_handoff_requires_all_predecessor_originals_before_target() {
    let fixture = closed_fixture();
    let mut writer = active(&fixture);
    assert_append(&fixture, &mut writer, 1);
    // Privileged test setup: historical nonzero ACK alone cannot attest its
    // original input prefix. All semantic rows and receipts stay retained.
    let mut previous = value(writer.store(), producer_key(1));
    previous[10..18].copy_from_slice(&4_u64.to_be_bytes());
    put(writer.store(), producer_key(1), &previous);
    drop(writer);
    let mut recovery = fixture.reopen();
    let closed = close(&fixture, &mut recovery);
    let result = resolve(&fixture, &recovery, &closed, 1).unwrap();
    let mut target_only = closed.source_resolution(&recovery, 1).unwrap();
    assert_eq!(
        target_only.record(&result),
        Err(BootstrapError::StateMismatch)
    );
    assert!(target_only.finish().is_err());
    let mut prefix = full_prefix(&fixture, &recovery, &closed, 1);
    prefix.record(&result).unwrap();
    let source = prefix.finish().unwrap();
    let token = source.recovery_acknowledgement().unwrap();
    assert_eq!((token.issued(), token.acknowledged()), (1, 0));
    token.validate(&recovery).unwrap();
    let stale = source.recovery_acknowledgement().unwrap();
    drop(source);
    drop(result);
    drop(closed);
    let before = capture(&fixture, &recovery.store);
    // Native-only mechanics after an explicitly synthetic PG confirmation.
    recovery
        .store
        .test_acknowledge_target(
            token.target(1).unwrap(),
            &grant(&fixture.budget, 4096, 4096),
            || Ok(()),
        )
        .unwrap();
    assert_eq!(recovery.store.db.latest_sequence_number(), before.1 + 1);
    assert_eq!(inputs(&fixture.path()), before.2);
    let target_key = producer_key(2).encode().unwrap();
    for row in before.0.iter().filter(|row| row.1 != target_key.as_bytes()) {
        assert!(kv(&recovery.store).contains(row));
    }
    assert!(stale.validate(&recovery).is_err());
    let closed = close(&fixture, &mut recovery);
    assert_eq!(closed.acknowledged_through(), 1);
    let mut fresh = full_prefix(&fixture, &recovery, &closed, 1);
    fresh
        .record(&resolve(&fixture, &recovery, &closed, 1).unwrap())
        .unwrap();
    assert_eq!(fresh.finish().unwrap().acknowledged_through(), 1);
    let original = &fixture.originals[0];
    std::fs::remove_file(
        fixture
            .path()
            .join("producer-inputs-v1")
            .join(input_name(1, 1)),
    )
    .unwrap();
    assert!(
        closed
            .resolve_predecessor_request(
                &recovery,
                request(7, 1),
                input_digest(original),
                recovery_limits(),
                grant(&fixture.budget, 3 * MIB, 0),
                grant(&fixture.budget, 40 * MIB, 0),
                || Ok(())
            )
            .is_err()
    );
    assert!(
        closed
            .source_resolution(&recovery, 1)
            .unwrap()
            .finish()
            .is_err()
    );
}

#[test]
fn activated_source_closure_covers_the_complete_two_stream_prefix_and_exact_digest() {
    let fixture = closed_fixture();
    let mut writer = active(&fixture);
    assert_append(&fixture, &mut writer, 1);
    signed_new(&fixture, &mut writer, 2);
    let duplicate = PreparedEvent::new(
        next_request(3),
        intent("B_note", 1, 10_000_000),
        256 * 1024,
        grant(&fixture.budget, MIB, 0),
    )
    .unwrap();
    let outcome = writer
        .store_mut()
        .admit_event(
            duplicate,
            ObservedTime::new(77),
            false,
            AdmissionLimits::default(),
            grant(&fixture.budget, 40 * MIB, 10 * MIB),
            || Ok(()),
        )
        .unwrap();
    assert!(
        matches!(outcome.outcome(), AdmissionOutcome::Durable(receipt) if receipt.batch().is_none())
    );
    drop(outcome);
    drop(writer);
    let mut recovery = fixture.reopen();
    let closed = close(&fixture, &mut recovery);
    let before = capture(&fixture, &recovery.store);
    let (expected, records) = expected_closure(&fixture, &recovery, 3, 5);
    let mut prefix = full_prefix(&fixture, &recovery, &closed, 3);
    for sequence in 1..=3 {
        let resolved = resolve(&fixture, &recovery, &closed, sequence).unwrap();
        prefix.record(&resolved).unwrap();
    }
    let source = prefix.finish().unwrap();
    assert_eq!(source.resolved_requests(), 3);
    assert_eq!(source.last_complete_sequence(), 5);
    assert_eq!(source.digest(), expected);
    // Dictionary request1 is not an Event; NewFacts2 and AlreadyDurable3 are.
    assert_eq!(source.event_request_mask(), 0b110);
    assert_eq!(source.validate(), Ok(()));
    assert!(source.matches_closed(&closed));
    assert!(std::ptr::eq(source.recovery(), &recovery));
    assert_eq!(source.manifest(), recovery.manifest());
    assert_eq!(source.producer_slot_id(), next_request(1).producer_slot_id);
    assert!(source.matches_recorded(records.clone()));
    for fault in ["missing", "duplicate", "order", "input", "outcome"] {
        let mut changed = records.clone();
        match fault {
            "missing" => {
                changed.pop();
            }
            "duplicate" => changed.push(records[2]),
            "order" => changed.swap(0, 1),
            "input" => changed[0].1[0] ^= 1,
            "outcome" => changed[1].2[0] ^= 1,
            _ => unreachable!(),
        }
        assert!(!source.matches_recorded(changed), "{fault}");
    }
    unchanged(&fixture, &recovery.store, &before);
    originals(&fixture, &recovery.store);
    drop(source);
    drop(closed);
    drop(recovery);
    let mut recovery = fixture.reopen();
    let closed = close(&fixture, &mut recovery);
    let mut prefix = full_prefix(&fixture, &recovery, &closed, 3);
    for sequence in 1..=3 {
        prefix
            .record(&resolve(&fixture, &recovery, &closed, sequence).unwrap())
            .unwrap();
    }
    assert_eq!(prefix.finish().unwrap().digest(), expected);
    unchanged(&fixture, &recovery.store, &before);
}

#[test]
fn activated_source_closure_distinguishes_zero_rejected_and_old_durable_prefixes() {
    for (mode, append) in [
        ("zero", false),
        ("rejected", false),
        ("durable", false),
        ("rejected", true),
        ("durable", true),
    ] {
        let fixture = closed_fixture();
        let mut writer = active(&fixture);
        if mode != "zero" {
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
            drop(outcome);
        }
        if append {
            assert_append(&fixture, &mut writer, 2);
        }
        drop(writer);
        let mut recovery = fixture.reopen();
        let closed = close(&fixture, &mut recovery);
        let count = u64::from(mode != "zero") + u64::from(append);
        let cut = u64::from(append);
        let before = capture(&fixture, &recovery.store);
        let (expected, records) = expected_closure(&fixture, &recovery, count, cut);
        let mut prefix = full_prefix(&fixture, &recovery, &closed, count);
        for sequence in 1..=count {
            prefix
                .record(&resolve(&fixture, &recovery, &closed, sequence).unwrap())
                .unwrap();
        }
        let source = prefix.finish().unwrap();
        assert_eq!(source.last_complete_sequence(), cut);
        assert_eq!(source.resolved_requests(), count);
        assert_eq!(source.digest(), expected);
        assert_eq!(source.event_request_mask(), u64::from(mode != "zero"));
        assert!(source.matches_recorded(records));
        if count != 0 {
            assert_ne!(
                source.digest(),
                expected_closure(&fixture, &recovery, 0, cut).0
            );
        }
        unchanged(&fixture, &recovery.store, &before);
        originals(&fixture, &recovery.store);
    }
}

#[test]
fn activated_source_closure_refuses_omissions_duplicates_order_and_reserved_tails() {
    let fixture = closed_fixture();
    let mut writer = active(&fixture);
    assert_append(&fixture, &mut writer, 1);
    signed_new(&fixture, &mut writer, 2);
    drop(writer);
    let mut recovery = fixture.reopen();
    let closed = close(&fixture, &mut recovery);
    let before = capture(&fixture, &recovery.store);
    for reserved in [0, 1, 3] {
        assert!(matches!(
            closed.source_resolution(&recovery, reserved),
            Err(BootstrapError::StateMismatch)
        ));
    }
    assert!(matches!(
        closed.source_resolution(&recovery, 2).unwrap().finish(),
        Err(BootstrapError::StateMismatch)
    ));
    // Finish both ancestor scans before retaining the two heavy target results.
    let mut incomplete = full_prefix(&fixture, &recovery, &closed, 2);
    let mut prefix = full_prefix(&fixture, &recovery, &closed, 2);
    let one = resolve(&fixture, &recovery, &closed, 1).unwrap();
    let two = resolve(&fixture, &recovery, &closed, 2).unwrap();
    incomplete.record(&one).unwrap();
    assert!(matches!(
        incomplete.finish(),
        Err(BootstrapError::StateMismatch)
    ));
    assert_eq!(prefix.record(&two), Err(BootstrapError::StateMismatch));
    prefix.record(&one).unwrap();
    assert_eq!(prefix.record(&one), Err(BootstrapError::StateMismatch));
    prefix.record(&two).unwrap();
    assert_eq!(prefix.record(&two), Err(BootstrapError::StateMismatch));
    let source = prefix.finish().unwrap();
    assert_eq!(
        source.digest(),
        expected_closure(&fixture, &recovery, 2, 5).0
    );
    assert_eq!(source.event_request_mask(), 0b10);
    unchanged(&fixture, &recovery.store, &before);
    drop(source);
    drop(one);
    drop(two);
    drop(closed);
    drop(recovery);
    drop(fixture);

    // An existing boundary marker is not a complete input prefix. Refuse the
    // explicit selected source-proof bound before attempting any request read.
    let fixture = closed_fixture();
    let writer = active(&fixture);
    put(writer.store(), producer_key(2), &marker(63));
    put(
        writer.store(),
        IngestKey::Request {
            request_id: next_request(63),
        },
        b"unvalidated-tail",
    );
    drop(writer);
    let mut recovery = fixture.reopen();
    let closed = close(&fixture, &mut recovery);
    let before = capture(&fixture, &recovery.store);
    assert!(matches!(
        closed.source_resolution(&recovery, 63),
        Err(BootstrapError::InsufficientReservation)
    ));
    unchanged(&fixture, &recovery.store, &before);
}

#[test]
fn activated_source_closure_binds_root_sequence_and_the_exact_closed_owner() {
    let fixture = closed_fixture();
    let mut writer = active(&fixture);
    assert_append(&fixture, &mut writer, 1);
    drop(writer);
    let clone = tempfile::TempDir::new().unwrap();
    let path = clone.path().join("db");
    copy_directory(&fixture.path(), &path);
    for directory in [path.clone(), path.join("producer-inputs-v1")] {
        assert_eq!(
            std::fs::metadata(directory).unwrap().permissions().mode() & 0o777,
            0o700
        );
    }
    assert_eq!(inputs(&path), inputs(&fixture.path()));
    let mut recovery = fixture.reopen();
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
        fixture.previous.identity().dataset_id,
        fixture.native,
        grant(&foreign_budget, 0, 128 * MIB),
    )
    .unwrap();
    let closed = close(&fixture, &mut recovery);
    let another = close(&fixture, &mut recovery);
    let foreign_closed = close(&fixture, &mut foreign);
    assert_eq!(kv(&recovery.store), kv(&foreign.store));
    assert_eq!(
        recovery.store.db.latest_sequence_number(),
        foreign.store.db.latest_sequence_number()
    );
    assert!(matches!(
        closed.source_resolution(&foreign, 1),
        Err(BootstrapError::StateMismatch)
    ));
    let result = resolve(&fixture, &recovery, &closed, 1).unwrap();
    let mut foreign_prefix = full_prefix(&fixture, &foreign, &foreign_closed, 1);
    assert_eq!(
        foreign_prefix.record(&result),
        Err(BootstrapError::StateMismatch)
    );
    foreign_prefix
        .record(&resolve(&fixture, &foreign, &foreign_closed, 1).unwrap())
        .unwrap();
    let foreign_source = foreign_prefix.finish().unwrap();
    let mut prefix = full_prefix(&fixture, &recovery, &closed, 1);
    prefix.record(&result).unwrap();
    let source = prefix.finish().unwrap();
    assert!(source.matches_closed(&closed));
    assert!(!source.matches_closed(&another));
    assert!(!source.matches_closed(&foreign_closed));
    assert_eq!(source.digest(), foreign_source.digest());
    let mut stale = full_prefix(&fixture, &recovery, &closed, 1);
    stale.record(&result).unwrap();
    // A privileged identical-value rewrite changes only the native sequence.
    let bytes = value(&recovery.store, handoff_key());
    put(&recovery.store, handoff_key(), &bytes);
    let before = capture(&fixture, &recovery.store);
    assert_eq!(source.validate(), Err(BootstrapError::StateMismatch));
    assert_eq!(stale.record(&result), Err(BootstrapError::StateMismatch));
    assert!(matches!(stale.finish(), Err(BootstrapError::StateMismatch)));
    unchanged(&fixture, &recovery.store, &before);
    drop(source);
    drop(result);
    drop(another);
    drop(closed);
    let closed = close(&fixture, &mut recovery);
    let mut fresh = full_prefix(&fixture, &recovery, &closed, 1);
    fresh
        .record(&resolve(&fixture, &recovery, &closed, 1).unwrap())
        .unwrap();
    assert_eq!(fresh.finish().unwrap().digest(), foreign_source.digest());
    unchanged(&fixture, &recovery.store, &before);
}

#[test]
fn activated_source_closure_refuses_a_retained_source_tail_beyond_the_request_prefix() {
    let fixture = closed_fixture();
    let mut writer = active(&fixture);
    assert_append(&fixture, &mut writer, 1);
    signed_new(&fixture, &mut writer, 2);
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
                append_permit(&fixture),
                || Ok(()),
            )
            .unwrap(),
    );
    // Keep the genuine third batch/fact/input but truncate only the producer
    // detail/counter. Per-request positive replay alone cannot detect this tail.
    delete(
        writer.store(),
        IngestKey::Request {
            request_id: next_request(3),
        },
    );
    put(writer.store(), producer_key(2), &marker(2));
    drop(writer);
    let mut recovery = fixture.reopen();
    let closed = close(&fixture, &mut recovery);
    let before = capture(&fixture, &recovery.store);
    let mut prefix = full_prefix(&fixture, &recovery, &closed, 2);
    for sequence in 1..=2 {
        prefix
            .record(&resolve(&fixture, &recovery, &closed, sequence).unwrap())
            .unwrap();
    }
    assert!(matches!(
        prefix.finish(),
        Err(BootstrapError::StateMismatch)
    ));
    unchanged(&fixture, &recovery.store, &before);
}

#[test]
fn activated_source_closure_refuses_future_dependencies_and_noncontiguous_new_batches() {
    for new_facts in [false, true] {
        let fixture = closed_fixture();
        let mut writer = active(&fixture);
        assert_append(&fixture, &mut writer, 1);
        signed_new(&fixture, &mut writer, 2);
        drop(writer);
        let mut recovery = fixture.reopen();
        // Deliberately rewrite request1 to claim the actual signed request2
        // effects. The ordinary positive resolver sees genuine retained facts;
        // only the ordered prefix proves they cannot belong to this first step.
        let original = Receipt::decode_persistent(
            &value(
                &recovery.store,
                IngestKey::Request {
                    request_id: next_request(2),
                },
            ),
            bigbrotr_model::receipt_codec::ReceiptDecodeLimits::default(),
        )
        .unwrap();
        let input = intent("B_note", 1, 10_000_000)
            .encode_request_preimage(next_request(1), 256 * 1024)
            .unwrap();
        let digest = Digest256::from_bytes(Sha256::digest(&input).into());
        let receipt = if new_facts {
            Receipt::new_facts(
                next_request(1),
                digest,
                *original.protection_context(),
                original.batch().unwrap().clone(),
                original.dependencies().clone(),
            )
            .unwrap()
        } else {
            Receipt::already_durable(
                next_request(1),
                digest,
                *original.protection_context(),
                original.dependencies().clone(),
            )
            .unwrap()
        };
        put(
            &recovery.store,
            IngestKey::Request {
                request_id: next_request(1),
            },
            &receipt.encode_persistent(256 * 1024).unwrap(),
        );
        std::fs::write(
            fixture
                .path()
                .join("producer-inputs-v1")
                .join(input_name(2, 1)),
            input,
        )
        .unwrap();
        let closed = close(&fixture, &mut recovery);
        let before = capture(&fixture, &recovery.store);
        let result = resolve(&fixture, &recovery, &closed, 1).unwrap();
        let mut prefix = full_prefix(&fixture, &recovery, &closed, 2);
        assert_eq!(prefix.record(&result), Err(BootstrapError::StateMismatch));
        assert!(matches!(
            prefix.finish(),
            Err(BootstrapError::StateMismatch)
        ));
        unchanged(&fixture, &recovery.store, &before);
    }
}
