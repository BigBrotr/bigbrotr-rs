//! Real native close/reopen fixtures. Activation is synthetic here; PostgreSQL
//! receipt/fence and installed process qualification are separate obligations.
use super::*;
use crate::storage::rocks::store::{
    ActivatedInitial, ActivatedProducerClosing, InitialHandoffRecord,
};

#[path = "activated_source_tests.rs"]
mod activated_source_tests;

fn record(recovery: &IngestRecovery) -> InitialHandoffRecord {
    InitialHandoffRecord::decode(&value(&recovery.store, handoff_key())).unwrap()
}
fn inspect(recovery: &IngestRecovery) -> ActivatedInitial {
    ActivatedInitial::inspect(recovery, record(recovery)).unwrap()
}
fn close(fixture: &Fixture, recovery: &mut IngestRecovery) -> ActivatedProducerClosing {
    inspect(recovery)
        .close_producer(recovery, grant(&fixture.budget, 4096, 4096), || Ok(()))
        .unwrap()
}
fn resolve(
    fixture: &Fixture,
    recovery: &IngestRecovery,
    closed: &ActivatedProducerClosing,
    sequence: u64,
) -> Result<ResolvedProducerRequest, ProducerRecoveryError> {
    let bytes = std::fs::read(
        fixture
            .path()
            .join("producer-inputs-v1")
            .join(input_name(2, sequence)),
    )
    .unwrap();
    closed.resolve_original_request(
        recovery,
        next_request(sequence),
        Digest256::from_bytes(Sha256::digest(bytes).into()),
        recovery_limits(),
        grant(&fixture.budget, 3 * MIB, 0),
        grant(&fixture.budget, 40 * MIB, 0),
        || Ok(()),
    )
}
fn closing_marker(issued: u64) -> Vec<u8> {
    [
        vec![2, 49],
        issued.to_be_bytes().to_vec(),
        vec![0; 16],
        vec![1],
    ]
    .concat()
}
fn producer_key(epoch: u64) -> IngestKey {
    IngestKey::ProducerState {
        producer_slot_id: request(7, 1).producer_slot_id,
        producer_epoch: ProducerEpoch::new(epoch).unwrap(),
    }
}
fn signed_new(fixture: &Fixture, writer: &mut ActiveInitialWriter, sequence: u64) {
    let prepared = PreparedEvent::new(
        next_request(sequence),
        intent("B_note", 1, 10_000_000),
        256 * 1024,
        grant(&fixture.budget, MIB, 0),
    )
    .unwrap();
    let outcome = writer
        .store_mut()
        .admit_event(
            prepared,
            ObservedTime::new(77),
            false,
            AdmissionLimits::default(),
            grant(&fixture.budget, 40 * MIB, 10 * MIB),
            || Ok(()),
        )
        .unwrap();
    let AdmissionOutcome::Durable(receipt) = outcome.outcome() else {
        panic!("signed new event")
    };
    assert_eq!(receipt.batch().unwrap().first(), fact_id(2, 2));
    assert_eq!(receipt.batch().unwrap().last(), fact_id(2, 5));
    // Frozen bootstrap order is public scope, private scope, Observer1,
    // lineage. This public network intent uses Observer1 for both observer
    // and clock, so its predecessor dependencies are source1 and source3;
    // private scope at source2 is unrelated to this event.
    assert_eq!(
        receipt
            .dependencies()
            .records()
            .iter()
            .map(|r| (
                r.source_record_id.owner_epoch.get(),
                r.source_record_id.sequence.get()
            ))
            .collect::<Vec<_>>(),
        [(1, 1), (1, 3), (2, 2), (2, 3), (2, 4), (2, 5)]
    );
}

#[test]
fn activated_recovery_replays_dictionary_one_and_signed_two_with_exact_original_bytes() {
    let fixture = closed_fixture();
    let mut writer = active(&fixture);
    assert_append(&fixture, &mut writer, 1);
    signed_new(&fixture, &mut writer, 2);
    let expected_dictionary = expected(scope(&writer).allocation_domain.as_bytes(), 1);
    let before = capture(&fixture, writer.store());
    let receipts: Vec<_> = (1..=2)
        .map(|sequence| {
            value(
                writer.store(),
                IngestKey::Request {
                    request_id: next_request(sequence),
                },
            )
        })
        .collect();
    assert_eq!(receipts[0], expected_dictionary.receipt);
    drop(writer);
    let mut recovery = fixture.reopen();
    let view = inspect(&recovery);
    assert_eq!(view.previous_manifest(), &fixture.previous);
    assert_eq!(view.manifest(), recovery.manifest());
    assert_eq!(view.issued_through(), 2);
    assert_eq!(
        value(&recovery.store, source_key(2)),
        [vec![1, 2], 5_u64.to_be_bytes().to_vec()].concat()
    );
    assert!(!view.is_closing());
    let closed = view
        .close_producer(&mut recovery, grant(&fixture.budget, 4096, 4096), || Ok(()))
        .unwrap();
    let mut expected_rows = before.0.clone();
    let key = [vec![1, 49], vec![7; 16], 2_u64.to_be_bytes().to_vec()].concat();
    expected_rows
        .iter_mut()
        .find(|r| r.0 == "receipts" && r.1 == key)
        .unwrap()
        .2 = closing_marker(2);
    assert_eq!(kv(&recovery.store), expected_rows);
    assert_eq!(recovery.store.db.latest_sequence_number(), before.1 + 1);
    assert_eq!(inputs(&fixture.path()), before.2);
    assert_eq!(view.validate(&recovery), Err(BootstrapError::StateMismatch));
    assert_eq!(closed.validate(&recovery), Ok(()));
    let stable = capture(&fixture, &recovery.store);
    for sequence in [1, 2, 1] {
        let result = resolve(&fixture, &recovery, &closed, sequence).unwrap();
        assert_eq!(
            result.native_outcome().bytes(),
            receipts[sequence as usize - 1]
        );
        assert_eq!(
            result.native_outcome().manifest_sha256(),
            <[u8; 32]>::from(Sha256::digest(recovery.manifest().encode().as_bytes()))
        );
        assert_eq!(result.is_event(), sequence == 2);
        unchanged(&fixture, &recovery.store, &stable);
    }
    drop(closed);
    drop(recovery);
    let mut recovery = fixture.reopen();
    assert!(inspect(&recovery).is_closing());
    let closed = close(&fixture, &mut recovery);
    assert_eq!(closed.issued_through(), 2);
    assert_eq!(closed.manifest(), recovery.manifest());
    assert_eq!(closed.producer_slot_id(), next_request(1).producer_slot_id);
    drop(resolve(&fixture, &recovery, &closed, 2).unwrap());
    unchanged(&fixture, &recovery.store, &stable);
    originals(&fixture, &recovery.store);
}

#[test]
fn activated_recovery_accepts_explicit_zero_and_positive_issued_without_source_facts() {
    for mode in ["zero", "rejected", "already-durable"] {
        let fixture = closed_fixture();
        let mut writer = active(&fixture);
        let original = if mode == "zero" {
            None
        } else {
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
                    mode == "already-durable" && receipt.batch().is_none(),
                _ => false,
            });
            drop(outcome);
            Some(value(
                writer.store(),
                IngestKey::Request {
                    request_id: next_request(1),
                },
            ))
        };
        drop(writer);
        let mut recovery = fixture.reopen();
        let closed = close(&fixture, &mut recovery);
        assert_eq!(closed.issued_through(), u64::from(mode != "zero"));
        assert_eq!(
            value(&recovery.store, source_key(2)),
            [vec![1, 2], vec![0; 8]].concat()
        );
        let before = capture(&fixture, &recovery.store);
        if let Some(original) = original {
            let result = resolve(&fixture, &recovery, &closed, 1).unwrap();
            assert_eq!(result.native_outcome().bytes(), original);
        }
        let missing = closed.resolve_original_request(
            &recovery,
            next_request(2),
            Digest256::from_bytes([5; 32]),
            recovery_limits(),
            grant(&fixture.budget, 3 * MIB, 0),
            grant(&fixture.budget, 40 * MIB, 0),
            || Ok(()),
        );
        assert!(matches!(missing, Err(ProducerRecoveryError::Unresolved)));
        unchanged(&fixture, &recovery.store, &before);
        assert!(matches!(
            recovery.resolve_original_request(
                next_request(1),
                Digest256::from_bytes([5; 32]),
                recovery_limits(),
                grant(&fixture.budget, 3 * MIB, 0),
                grant(&fixture.budget, 40 * MIB, 0),
                || Ok(())
            ),
            Err(ProducerRecoveryError::Native(AdmissionError::InvalidInput))
        ));
        assert!(matches!(
            recovery.observe_closed_producer(
                next_request(1).producer_slot_id,
                ProducerEpoch::new(2).unwrap(),
                grant(&fixture.budget, 4096, 4096)
            ),
            Err(BootstrapError::InvalidIntent)
        ));
        drop(closed);
        drop(recovery);
        assert!(matches!(
            InitialWriterCandidate::new(fixture.reopen()),
            Err(BootstrapError::InvalidIntent)
        ));
    }
}

#[test]
fn activated_context_refuses_missing_markers_handoff_foreign_streams_and_false_zero() {
    for fault in [
        "target-marker",
        "old-marker",
        "old-open",
        "target-ack",
        "source-missing",
        "source-zero-fact",
        "source-zero-pending",
        "old-source-cut",
        "handoff",
        "third-producer",
        "third-source",
        "foreign-fact",
        "foreign-pending",
        "allocation-zero",
        "unknown-allocation",
    ] {
        let fixture = closed_fixture();
        let writer = active(&fixture);
        let target = *writer.manifest();
        drop(writer);
        let recovery = fixture.reopen();
        let retained = record(&recovery);
        match fault {
            "target-marker" => delete(&recovery.store, producer_key(2)),
            "old-marker" => delete(&recovery.store, producer_key(1)),
            "old-open" => put(&recovery.store, producer_key(1), &marker(4)),
            "target-ack" => {
                let mut bytes = marker(1);
                bytes[10..18].copy_from_slice(&1_u64.to_be_bytes());
                put(&recovery.store, producer_key(2), &bytes);
            }
            "source-missing" => delete(&recovery.store, source_key(2)),
            "source-zero-fact" => put(
                &recovery.store,
                IngestKey::Fact {
                    source_record_id: fact_id(2, 1),
                },
                b"stray",
            ),
            "source-zero-pending" => put(
                &recovery.store,
                IngestKey::PendingBatch {
                    first: fact_id(2, 1),
                },
                b"stray",
            ),
            "old-source-cut" => put(
                &recovery.store,
                source_key(1),
                &[vec![1, 2], 8_u64.to_be_bytes().to_vec()].concat(),
            ),
            "handoff" => {
                let mut bytes = value(&recovery.store, handoff_key());
                bytes[45] ^= 1;
                put(&recovery.store, handoff_key(), &bytes);
            }
            "third-producer" => put(&recovery.store, producer_key(3), &marker(0)),
            "third-source" => put(
                &recovery.store,
                source_key(3),
                &[vec![1, 2], vec![0; 8]].concat(),
            ),
            "foreign-fact" => put(
                &recovery.store,
                IngestKey::Fact {
                    source_record_id: fact_id(3, 1),
                },
                b"foreign",
            ),
            "foreign-pending" => put(
                &recovery.store,
                IngestKey::PendingBatch {
                    first: fact_id(3, 1),
                },
                b"foreign",
            ),
            "allocation-zero" | "unknown-allocation" => put(
                &recovery.store,
                IngestKey::AllocationStream {
                    dataset_id: target.identity().dataset_id,
                    dictionary_kind: DictionaryKind::VisibilityScope,
                    allocation_domain: if fault == "allocation-zero" {
                        target
                            .allocation_namespaces()
                            .domain(DictionaryKind::VisibilityScope)
                            .unwrap()
                    } else {
                        [99; 16].try_into().unwrap()
                    },
                },
                &[vec![1, 3], vec![0; 8]].concat(),
            ),
            _ => unreachable!(),
        }
        let before = capture(&fixture, &recovery.store);
        assert!(
            matches!(
                ActivatedInitial::inspect(&recovery, retained),
                Err(BootstrapError::StateMismatch)
            ),
            "{fault}"
        );
        unchanged(&fixture, &recovery.store, &before);
    }
}

#[test]
fn activated_replay_requires_old_dependency_bytes_original_input_and_dictionary_indices() {
    for fault in [
        "old-fact-missing",
        "old-fact-corrupt",
        "input-missing",
        "input-corrupt",
        "dictionary-reverse",
    ] {
        let fixture = closed_fixture();
        let mut writer = active(&fixture);
        assert_append(&fixture, &mut writer, 1);
        signed_new(&fixture, &mut writer, 2);
        let old_fact = IngestKey::Fact {
            source_record_id: fact_id(1, 1),
        };
        let original = value(writer.store(), old_fact);
        let input_path = fixture
            .path()
            .join("producer-inputs-v1")
            .join(input_name(2, 2));
        let original_input = std::fs::read(&input_path).unwrap();
        let input_digest = Digest256::from_bytes(Sha256::digest(&original_input).into());
        let dictionary_expected = expected(scope(&writer).allocation_domain.as_bytes(), 1);
        let reverse = dictionary_expected
            .writes
            .iter()
            .find(|r| r.1.starts_with(&[1, 5]))
            .unwrap()
            .clone();
        drop(writer);
        let mut recovery = fixture.reopen();
        match fault {
            "old-fact-missing" => delete(&recovery.store, old_fact),
            "old-fact-corrupt" => {
                let mut bytes = original.clone();
                *bytes.last_mut().unwrap() ^= 1;
                put(&recovery.store, old_fact, &bytes);
            }
            "input-missing" => std::fs::remove_file(&input_path).unwrap(),
            "input-corrupt" => {
                let mut bytes = original_input.clone();
                *bytes.last_mut().unwrap() ^= 1;
                std::fs::write(&input_path, bytes).unwrap();
            }
            "dictionary-reverse" => recovery
                .store
                .db
                .delete_cf(recovery.store.db.cf_handle("default").unwrap(), &reverse.1)
                .unwrap(),
            _ => unreachable!(),
        }
        let closed = close(&fixture, &mut recovery);
        let before = capture(&fixture, &recovery.store);
        let result = if fault == "dictionary-reverse" {
            resolve(&fixture, &recovery, &closed, 1)
        } else {
            closed.resolve_original_request(
                &recovery,
                next_request(2),
                input_digest,
                recovery_limits(),
                grant(&fixture.budget, 3 * MIB, 0),
                grant(&fixture.budget, 40 * MIB, 0),
                || Ok(()),
            )
        };
        assert!(result.is_err(), "{fault}");
        unchanged(&fixture, &recovery.store, &before);
        drop(result);
        drop(closed);
        match fault {
            "old-fact-missing" | "old-fact-corrupt" => put(&recovery.store, old_fact, &original),
            "input-missing" | "input-corrupt" => {
                std::fs::write(&input_path, &original_input).unwrap();
                std::fs::set_permissions(&input_path, std::fs::Permissions::from_mode(0o600))
                    .unwrap();
            }
            "dictionary-reverse" => recovery
                .store
                .db
                .put_cf(
                    recovery.store.db.cf_handle("default").unwrap(),
                    &reverse.1,
                    &reverse.2,
                )
                .unwrap(),
            _ => unreachable!(),
        }
        let closed = close(&fixture, &mut recovery);
        drop(
            resolve(
                &fixture,
                &recovery,
                &closed,
                if fault == "dictionary-reverse" { 1 } else { 2 },
            )
            .unwrap(),
        );
        originals(&fixture, &recovery.store);
    }
}

#[test]
fn activated_closing_checks_grants_authority_and_preserves_postwrite_barrier() {
    for fault in [
        "data",
        "native",
        "items",
        "authority1",
        "authority2",
        "authority3",
    ] {
        let fixture = closed_fixture();
        drop(active(&fixture));
        let mut recovery = fixture.reopen();
        let view = inspect(&recovery);
        let before = capture(&fixture, &recovery.store);
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
        let result = view.close_producer(&mut recovery, permit, || {
            calls += 1;
            if fault == format!("authority{calls}") {
                Err(BootstrapError::AuthorityUnavailable)
            } else {
                Ok(())
            }
        });
        assert!(
            matches!(
                result,
                Err(BootstrapError::AuthorityUnavailable | BootstrapError::InsufficientReservation)
            ),
            "{fault}"
        );
        if fault == "authority3" {
            assert_eq!(value(&recovery.store, producer_key(2)), closing_marker(0));
            assert_eq!(recovery.store.db.latest_sequence_number(), before.1 + 1);
            assert_eq!(inputs(&fixture.path()), before.2);
        } else {
            unchanged(&fixture, &recovery.store, &before);
        }
        if !fault.starts_with("authority") {
            assert_eq!(calls, 0);
        }
        drop(result);
        let closed = close(&fixture, &mut recovery);
        assert_eq!(closed.issued_through(), 0);
        assert_eq!(value(&recovery.store, producer_key(2)), closing_marker(0));
    }
}

fn copy_directory(source: &Path, target: &Path) {
    std::fs::create_dir(target).unwrap();
    // Native input reads require private directories, including the store root.
    // Recreating them with the process umask is not an equivalent fixture copy.
    std::fs::set_permissions(target, std::fs::metadata(source).unwrap().permissions()).unwrap();
    for entry in std::fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let to = target.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_directory(&entry.path(), &to);
        } else {
            std::fs::copy(entry.path(), to).unwrap();
        }
    }
}

#[test]
fn activated_proofs_bind_actual_root_and_sequence_even_with_identical_logical_bytes() {
    let fixture = closed_fixture();
    let mut writer = active(&fixture);
    assert_append(&fixture, &mut writer, 1);
    drop(writer);
    let clone = tempfile::TempDir::new().unwrap();
    let path = clone.path().join("db");
    copy_directory(&fixture.path(), &path);
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
    assert_eq!(kv(&recovery.store), kv(&foreign.store));
    assert_eq!(
        recovery.store.db.latest_sequence_number(),
        foreign.store.db.latest_sequence_number()
    );
    let view = inspect(&recovery);
    assert!(matches!(
        view.close_producer(&mut foreign, grant(&fixture.budget, 4096, 4096), || Ok(())),
        Err(BootstrapError::StateMismatch)
    ));
    let closed = view
        .close_producer(&mut recovery, grant(&fixture.budget, 4096, 4096), || Ok(()))
        .unwrap();
    let foreign_closed = close(&fixture, &mut foreign);
    assert_eq!(kv(&recovery.store), kv(&foreign.store));
    assert_eq!(
        recovery.store.db.latest_sequence_number(),
        foreign.store.db.latest_sequence_number()
    );
    assert_eq!(
        closed.validate(&foreign),
        Err(BootstrapError::StateMismatch)
    );
    let bytes = value(&recovery.store, handoff_key());
    put(&recovery.store, handoff_key(), &bytes);
    let before = capture(&fixture, &recovery.store);
    assert_eq!(
        closed.validate(&recovery),
        Err(BootstrapError::StateMismatch)
    );
    assert!(matches!(
        resolve(&fixture, &recovery, &closed, 1),
        Err(ProducerRecoveryError::Native(AdmissionError::StateMismatch))
    ));
    unchanged(&fixture, &recovery.store, &before);
    let closed = close(&fixture, &mut recovery);
    drop(resolve(&fixture, &recovery, &closed, 1).unwrap());
    unchanged(&fixture, &recovery.store, &before);
    assert_eq!(foreign_closed.validate(&foreign), Ok(()));

    let input = expected(
        recovery
            .manifest()
            .allocation_namespaces()
            .domain(DictionaryKind::VisibilityScope)
            .unwrap()
            .as_bytes(),
        1,
    )
    .input;
    for fault in [
        "input-data",
        "outcome-data",
        "outcome-items",
        "authority1",
        "authority2",
        "authority3",
    ] {
        let input_permit = grant(
            &fixture.budget,
            if fault == "input-data" { 0 } else { 3 * MIB },
            0,
        );
        let outcome_permit = fixture
            .budget
            .try_acquire(
                ResourceRequest::data(
                    if fault == "outcome-data" { 0 } else { 40 * MIB },
                    u64::from(fault != "outcome-items"),
                    0,
                )
                .unwrap(),
            )
            .unwrap();
        let mut calls = 0;
        let result = closed.resolve_original_request(
            &recovery,
            next_request(1),
            Digest256::from_bytes(Sha256::digest(&input).into()),
            recovery_limits(),
            input_permit,
            outcome_permit,
            || {
                calls += 1;
                if fault == format!("authority{calls}") {
                    Err(AdmissionError::AuthorityUnavailable)
                } else {
                    Ok(())
                }
            },
        );
        assert!(
            match result {
                Err(ProducerRecoveryError::Input(RequestInputError::InsufficientReservation)) =>
                    fault == "input-data",
                Err(ProducerRecoveryError::Native(AdmissionError::InsufficientReservation)) =>
                    fault.starts_with("outcome"),
                Err(ProducerRecoveryError::Native(AdmissionError::AuthorityUnavailable)) =>
                    fault.starts_with("authority"),
                _ => false,
            },
            "{fault}"
        );
        unchanged(&fixture, &recovery.store, &before);
    }
    drop(resolve(&fixture, &recovery, &closed, 1).unwrap());
    unchanged(&fixture, &recovery.store, &before);
}

#[test]
fn activated_dependency_bounds_are_exact_and_new_batches_cannot_belong_to_predecessor() {
    let fixture = closed_fixture();
    let mut writer = active(&fixture);
    assert_append(&fixture, &mut writer, 1);
    drop(writer);
    let recovery = fixture.reopen();
    let view = inspect(&recovery);
    let context = ProtectionContext {
        dataset_id: recovery.manifest().identity().dataset_id,
        topology_generation: recovery.manifest().identity().topology_generation,
        protection_class: ProtectionClass::LocalSynced,
        recovery_generation: recovery.manifest().identity().recovery_generation,
        policy_epoch: PolicyEpoch::new(1),
    };
    // Exercise only the structural bound predicate here. Actual fact/digest and
    // input closure is independently exercised by the signed replay tests above.
    for (epoch, sequence, shard, accepted) in [
        (1, 9, 0, true),
        (1, 10, 0, false),
        (2, 1, 0, true),
        (2, 2, 0, false),
        (3, 1, 0, false),
        (1, 1, 1, false),
    ] {
        let reference = FactRef {
            source_record_id: SourceRecordId {
                shard_id: shard,
                ..fact_id(epoch, sequence)
            },
            payload_digest: Digest256::from_bytes([8; 32]),
        };
        let dependencies = SourceDependencies::new(context.dataset_id, vec![reference]).unwrap();
        let receipt = Receipt::already_durable(
            next_request(1),
            Digest256::from_bytes([7; 32]),
            context,
            dependencies.clone(),
        )
        .unwrap();
        assert_eq!(view.require_receipt_bounds(&receipt).is_ok(), accepted);
        let batch = AdmissionBatch::new(vec![reference]).unwrap();
        let receipt = Receipt::new_facts(
            next_request(1),
            Digest256::from_bytes([7; 32]),
            context,
            batch,
            dependencies,
        )
        .unwrap();
        assert_eq!(
            view.require_receipt_bounds(&receipt).is_ok(),
            accepted && epoch == 2
        );
    }
}
