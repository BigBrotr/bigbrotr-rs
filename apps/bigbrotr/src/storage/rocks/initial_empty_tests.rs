//! Empty-attempt component fixtures; no PG, process replacement or activation.
//! Register as a child of initial_reconstruction_tests to share its independently
//! checked signed bootstrap/dictionary/event/duplicate fixture and full census.
use super::*;

fn literal_manifests() -> (Vec<u8>, Vec<u8>) {
    let mut previous = vec![1, 1, 0, 1, 0, 1, 0, 1, 0, 1];
    previous.extend_from_slice(&[1; 16]);
    previous.extend_from_slice(&1_u64.to_be_bytes());
    previous.extend_from_slice(&[3; 16]);
    previous.extend_from_slice(&0_u32.to_be_bytes());
    previous.extend_from_slice(&1_u64.to_be_bytes());
    previous.extend_from_slice(&[4; 16]);
    previous.extend_from_slice(&[6; 16]);
    previous.extend_from_slice(&1_u64.to_be_bytes());
    previous.extend_from_slice(&[1, 7]);
    for kind in [1_u16, 2, 3, 4, 5, 8, 9] {
        previous.extend_from_slice(&kind.to_be_bytes());
        previous.extend_from_slice(&[kind as u8; 16]);
    }
    assert_eq!(previous.len(), 230);
    let mut target = previous.clone();
    target[3] = 2;
    target[26..34].copy_from_slice(&2_u64.to_be_bytes());
    target[54..62].copy_from_slice(&2_u64.to_be_bytes());
    target[62..78].fill(44);
    for entry in target[104..].chunks_exact_mut(18) {
        let domain = entry[1] + 32;
        entry[2..].fill(domain);
    }
    (previous, target)
}

fn literal_handoff(fixture: &Fixture) -> Vec<u8> {
    let (previous, target) = literal_manifests();
    let mut bytes = vec![1, 10];
    bytes.extend_from_slice(&[7; 16]);
    bytes.extend_from_slice(&1_u64.to_be_bytes());
    bytes.extend_from_slice(&4_u64.to_be_bytes());
    bytes.extend_from_slice(&9_u64.to_be_bytes());
    bytes.extend_from_slice(&expected_closure(&previous, &fixture.originals));
    for manifest in [&previous, &target] {
        bytes.extend_from_slice(&230_u16.to_be_bytes());
        bytes.extend_from_slice(manifest);
    }
    assert_eq!(bytes.len(), 538);
    bytes
}

fn empty_digest(fixture: &Fixture) -> [u8; 32] {
    let mut bytes = b"bigbrotr/initial-empty-attempt-closure/1\0".to_vec();
    bytes.extend_from_slice(&Sha256::digest(literal_handoff(fixture)));
    bytes.extend_from_slice(&Sha256::digest(literal_manifests().1));
    bytes.extend_from_slice(&[7; 16]);
    for number in [2_u64, 0, 0] {
        bytes.extend_from_slice(&number.to_be_bytes());
    }
    Sha256::digest(bytes).into()
}

fn producer_key() -> IngestKey {
    IngestKey::ProducerState {
        producer_slot_id: request(7, 1).producer_slot_id,
        producer_epoch: ProducerEpoch::new(2).unwrap(),
    }
}

fn marker(closing: bool) -> Vec<u8> {
    let mut bytes = vec![if closing { 2 } else { 1 }, 49];
    bytes.extend_from_slice(&[0; 24]);
    if closing {
        bytes.push(1);
    }
    bytes
}

fn closed_rows(open: &Rows) -> Rows {
    let mut rows = open.clone();
    let mut key = vec![1, 49];
    key.extend_from_slice(&[7; 16]);
    key.extend_from_slice(&2_u64.to_be_bytes());
    let row = rows
        .iter_mut()
        .find(|(cf, k, _)| cf == "receipts" && k == &key)
        .unwrap();
    assert_eq!(row.2, marker(false));
    row.2 = marker(true);
    rows
}

fn preserved(
    fixture: &Fixture,
    recovery: &IngestRecovery,
    rows: &Rows,
    sequence: u64,
    originals: &[(String, Vec<u8>)],
) {
    assert_eq!(&kv(&recovery.store), rows);
    assert_eq!(recovery.store.db.latest_sequence_number(), sequence);
    assert_eq!(inputs(&fixture.path()), originals);
    assert_eq!(
        value(&recovery.store, IngestKey::Manifest),
        literal_manifests().1
    );
}

fn retirement(
    fixture: &Fixture,
    recovery: &IngestRecovery,
) -> Result<EmptyInitialRetirement, String> {
    let bytes = literal_handoff(fixture);
    let view = recovery
        .empty_initial_attempt(&bytes)
        .map_err(|e| format!("view: {e:?}"))?;
    let history = view.historical();
    assert_eq!(
        history.manifest().encode().as_bytes(),
        literal_manifests().0
    );
    assert_eq!(
        history.target_manifest().encode().as_bytes(),
        literal_manifests().1
    );
    assert_eq!(history.producer_slot_id(), request(7, 1).producer_slot_id);
    assert_eq!(history.issued_through(), 4);
    assert_eq!(history.last_complete_sequence(), 9);
    assert_eq!(history.closure_digest(), fixture.closure);
    let closed = history
        .observe_closed_producer(grant(&fixture.budget, 4096, 4096))
        .map_err(|e| format!("predecessor: {e:?}"))?;
    let mut source = history
        .source_resolution(closed, 4)
        .map_err(|e| format!("source: {e:?}"))?;
    for (index, original) in fixture.originals.iter().enumerate() {
        let sequence = index as u64 + 1;
        let resolved = history
            .resolve_original_request(
                request(7, sequence),
                input_digest(original),
                recovery_limits(),
                grant(&fixture.budget, 3 * MIB, 0),
                grant(&fixture.budget, 40 * MIB, 0),
                || Ok(()),
            )
            .map_err(|e| format!("request {sequence}: {e:?}"))?;
        let AdmissionOutcome::Durable(receipt) = resolved.outcome() else {
            panic!("the retained signed originals must have positive outcomes")
        };
        assert_receipt(receipt, sequence);
        assert_eq!(resolved.native_outcome().bytes(), original.outcome);
        assert_eq!(resolved.native_outcome().digest(), input_digest(original));
        assert_eq!(resolved.input_bytes() as usize, original.input.len());
        source
            .record(&resolved)
            .map_err(|e| format!("record: {e:?}"))?;
    }
    let source = source.finish().map_err(|e| format!("finish: {e:?}"))?;
    assert_eq!(source.resolved_requests(), 4);
    assert_eq!(source.last_complete_sequence(), 9);
    assert_eq!(source.digest(), fixture.closure);
    if view.is_closing() {
        assert!(matches!(
            history.reconstruct_handoff(&source),
            Err(BootstrapError::StateMismatch)
        ));
    }
    let plan = view
        .reconstruct_retirement(&source)
        .map_err(|e| format!("plan: {e:?}"))?;
    assert_eq!(
        plan.previous_manifest().encode().as_bytes(),
        literal_manifests().0
    );
    assert_eq!(
        plan.target_manifest().encode().as_bytes(),
        literal_manifests().1
    );
    assert_eq!(plan.producer_slot_id(), request(7, 1).producer_slot_id);
    assert_eq!(plan.handoff().as_bytes(), bytes);
    assert_eq!(plan.closure_digest(), empty_digest(fixture));
    assert_eq!(plan.initially_closing(), view.is_closing());
    Ok(plan)
}

#[test]
fn empty_attempt_closes_exactly_one_marker_and_reconstructs_closed_history_after_reopen() {
    let fixture = prepared_fixture();
    assert_eq!(fixture.budget.snapshot().active_permits(), 0);
    let mut recovery = fixture.reopen();
    let before = kv(&recovery.store);
    let sequence = recovery.store.db.latest_sequence_number();
    let originals = inputs(&fixture.path());
    assert_eq!(before.len(), fixture.closed_rows.len() + 3);
    assert_eq!(
        value(&recovery.store, handoff_key()),
        literal_handoff(&fixture)
    );
    assert_eq!(value(&recovery.store, producer_key()), marker(false));
    assert_eq!(
        value(&recovery.store, source_key(2)),
        [1, 2, 0, 0, 0, 0, 0, 0, 0, 0]
    );
    let old_handoff = reconstruct(&fixture, &recovery).unwrap();
    let plan = retirement(&fixture, &recovery).unwrap();
    assert!(!plan.initially_closing());
    preserved(&fixture, &recovery, &before, sequence, &originals);
    let wanted = closed_rows(&before);
    let mut calls = 0;
    let observed = recovery
        .close_empty_initial_attempt(&plan, grant(&fixture.budget, 4096, 4096), || {
            calls += 1;
            Ok(())
        })
        .unwrap();
    assert_eq!(calls, 3);
    assert_eq!(
        observed.retirement().closure_digest(),
        empty_digest(&fixture)
    );
    assert_eq!(
        observed.retirement().handoff().as_bytes(),
        literal_handoff(&fixture)
    );
    drop(observed);
    preserved(&fixture, &recovery, &wanted, sequence + 1, &originals);
    assert!(
        recovery
            .historical_initial(&literal_handoff(&fixture))
            .is_err()
    );
    assert!(matches!(
        recovery.validate_initial_handoff(&old_handoff),
        Err(BootstrapError::StateMismatch)
    ));
    assert!(matches!(
        recovery.apply_initial_handoff(&old_handoff, grant(&fixture.budget, 8192, 8192), || Ok(())),
        Err(BootstrapError::StateMismatch)
    ));
    drop(recovery);

    // Same directory, new native handle. The local OPEN plan may observe the
    // one permitted close write, but cannot return it to OPEN or activate it.
    let mut recovery = fixture.reopen();
    drop(
        recovery
            .close_empty_initial_attempt(&plan, grant(&fixture.budget, 4096, 4096), || Ok(()))
            .unwrap(),
    );
    preserved(&fixture, &recovery, &wanted, sequence + 1, &originals);
    drop(plan);
    drop(recovery);

    let mut recovery = fixture.reopen();
    let closed_plan = retirement(&fixture, &recovery).unwrap();
    assert!(closed_plan.initially_closing());
    drop(
        recovery
            .close_empty_initial_attempt(
                &closed_plan,
                grant(&fixture.budget, 4096, 4096),
                || Ok(()),
            )
            .unwrap(),
    );
    preserved(&fixture, &recovery, &wanted, sequence + 1, &originals);
    assert!(
        recovery
            .historical_initial(&literal_handoff(&fixture))
            .is_err()
    );
}

#[test]
fn empty_attempt_refuses_missing_malformed_or_nonempty_target_state_without_repair() {
    for fault in [
        "missing-producer",
        "wrong-producer",
        "issued",
        "acknowledged",
        "retired",
        "missing-source",
        "source-kind",
        "source-nonzero",
        "fact",
        "pending",
        "request",
        "dictionary",
        "allocation",
        "extra-producer",
        "post-proof-fact",
    ] {
        let fixture = prepared_fixture();
        let mut recovery = fixture.reopen();
        let prior = (fault == "post-proof-fact").then(|| retirement(&fixture, &recovery).unwrap());
        let reference = QualifiedRef {
            allocation_domain: [34; 16].try_into().unwrap(),
            ..reference(DictionaryKind::Observer, 1)
        };
        match fault {
            "missing-producer" => delete(&recovery.store, producer_key()),
            "wrong-producer" => put(&recovery.store, producer_key(), &[2, 49, 0]),
            "issued" | "acknowledged" | "retired" => {
                let mut bytes = marker(false);
                for offset in [9, 17, 25].into_iter().take(match fault {
                    "issued" => 1,
                    "acknowledged" => 2,
                    _ => 3,
                }) {
                    bytes[offset] = 1;
                }
                put(&recovery.store, producer_key(), &bytes);
            }
            "missing-source" => delete(&recovery.store, source_key(2)),
            "source-kind" => put(
                &recovery.store,
                source_key(2),
                &[1, 3, 0, 0, 0, 0, 0, 0, 0, 0],
            ),
            "source-nonzero" => put(
                &recovery.store,
                source_key(2),
                &[1, 2, 0, 0, 0, 0, 0, 0, 0, 1],
            ),
            "fact" | "post-proof-fact" => put(
                &recovery.store,
                IngestKey::Fact {
                    source_record_id: fact_id(2, 1),
                },
                b"unexpected fact",
            ),
            "pending" => put(
                &recovery.store,
                IngestKey::PendingBatch {
                    first: fact_id(2, 1),
                },
                b"unexpected pending",
            ),
            "request" => put(
                &recovery.store,
                IngestKey::Request {
                    request_id: ProducerRequestId {
                        producer_epoch: ProducerEpoch::new(2).unwrap(),
                        ..request(7, 1)
                    },
                },
                b"unexpected request",
            ),
            "dictionary" => put(
                &recovery.store,
                IngestKey::Dictionary { reference },
                b"unexpected mapping",
            ),
            "allocation" => put(
                &recovery.store,
                IngestKey::AllocationStream {
                    dataset_id: reference.dataset_id,
                    dictionary_kind: reference.dictionary_kind,
                    allocation_domain: reference.allocation_domain,
                },
                &[1, 3, 0, 0, 0, 0, 0, 0, 0, 1],
            ),
            "extra-producer" => put(
                &recovery.store,
                IngestKey::ProducerState {
                    producer_slot_id: [8; 16].try_into().unwrap(),
                    producer_epoch: ProducerEpoch::new(2).unwrap(),
                },
                &marker(false),
            ),
            _ => unreachable!(),
        }
        let before = kv(&recovery.store);
        let sequence = recovery.store.db.latest_sequence_number();
        let originals = inputs(&fixture.path());
        assert!(retirement(&fixture, &recovery).is_err(), "{fault}");
        if let Some(prior) = prior {
            assert!(matches!(
                recovery.close_empty_initial_attempt(
                    &prior,
                    grant(&fixture.budget, 4096, 4096),
                    || Ok(())
                ),
                Err(BootstrapError::StateMismatch)
            ));
        }
        preserved(&fixture, &recovery, &before, sequence, &originals);
    }
}

#[test]
fn empty_attempt_rechecks_input_sidecars_without_relying_on_native_sequence_changes() {
    for schedule in ["before-proof", "after-proof", "closing-replay"] {
        let fixture = prepared_fixture();
        let mut recovery = fixture.reopen();
        let plan = if schedule == "before-proof" {
            None
        } else {
            Some(retirement(&fixture, &recovery).unwrap())
        };
        if schedule == "closing-replay" {
            drop(
                recovery
                    .close_empty_initial_attempt(
                        plan.as_ref().unwrap(),
                        grant(&fixture.budget, 4096, 4096),
                        || Ok(()),
                    )
                    .unwrap(),
            );
        }
        let before = kv(&recovery.store);
        let sequence = recovery.store.db.latest_sequence_number();
        std::fs::copy(
            fixture
                .path()
                .join("producer-inputs-v1")
                .join(input_name(1, 1)),
            fixture
                .path()
                .join("producer-inputs-v1")
                .join(input_name(2, 1)),
        )
        .unwrap();
        let originals = inputs(&fixture.path());
        assert_eq!(originals.len(), 5);
        assert!(matches!(
            recovery.empty_initial_attempt(&literal_handoff(&fixture)),
            Err(BootstrapError::RequestInput(RequestInputError::Conflict))
        ));
        if let Some(plan) = plan {
            assert_eq!(
                recovery.validate_empty_initial_retirement(&plan),
                Err(BootstrapError::RequestInput(RequestInputError::Conflict))
            );
            assert!(matches!(
                recovery.close_empty_initial_attempt(
                    &plan,
                    grant(&fixture.budget, 4096, 4096),
                    || Ok(())
                ),
                Err(BootstrapError::RequestInput(RequestInputError::Conflict))
            ));
        }
        preserved(&fixture, &recovery, &before, sequence, &originals);
    }
}

#[test]
fn empty_attempt_local_proof_refuses_equal_identity_other_roots_and_intervening_writes() {
    let first = prepared_fixture();
    let second = prepared_fixture();
    let mut a = first.reopen();
    let mut b = second.reopen();
    let plan = retirement(&first, &a).unwrap();
    let before = kv(&b.store);
    let sequence = b.store.db.latest_sequence_number();
    let originals = inputs(&second.path());
    assert_eq!(before, kv(&a.store));
    assert_eq!(sequence, a.store.db.latest_sequence_number());
    assert!(matches!(
        b.close_empty_initial_attempt(&plan, grant(&second.budget, 4096, 4096), || Ok(())),
        Err(BootstrapError::StateMismatch)
    ));
    preserved(&second, &b, &before, sequence, &originals);
    let local = retirement(&second, &b).unwrap();
    // Identical bytes are still an intervening write. A portable receipt or
    // unchanged row census must not refresh this previously constructed proof.
    put(&b.store, handoff_key(), &literal_handoff(&second));
    assert!(matches!(
        b.close_empty_initial_attempt(&local, grant(&second.budget, 4096, 4096), || Ok(())),
        Err(BootstrapError::StateMismatch)
    ));
    preserved(&second, &b, &before, sequence + 1, &originals);
    let current = retirement(&second, &b).unwrap();
    drop(
        b.close_empty_initial_attempt(&current, grant(&second.budget, 4096, 4096), || Ok(()))
            .unwrap(),
    );
    preserved(&second, &b, &closed_rows(&before), sequence + 2, &originals);

    drop(
        a.close_empty_initial_attempt(&plan, grant(&first.budget, 4096, 4096), || Ok(()))
            .unwrap(),
    );
    // Match B's deliberately advanced sequence without changing A's bytes.
    // The fresh CLOSING proof must still belong to A's actual directory.
    put(&a.store, handoff_key(), &literal_handoff(&first));
    assert_eq!(kv(&a.store), kv(&b.store));
    assert_eq!(
        a.store.db.latest_sequence_number(),
        b.store.db.latest_sequence_number()
    );
    let closing = retirement(&first, &a).unwrap();
    assert!(closing.initially_closing());
    assert!(matches!(
        b.close_empty_initial_attempt(&closing, grant(&second.budget, 4096, 4096), || Ok(())),
        Err(BootstrapError::StateMismatch)
    ));
    preserved(&second, &b, &closed_rows(&before), sequence + 2, &originals);
    drop(a);
    let mut a = first.reopen();
    drop(
        a.close_empty_initial_attempt(&closing, grant(&first.budget, 4096, 4096), || Ok(()))
            .unwrap(),
    );
    preserved(&first, &a, &closed_rows(&before), sequence + 2, &originals);
}

#[test]
fn empty_attempt_authority_and_budget_refusals_distinguish_durable_close_from_absence() {
    for fault in [
        "data-grant",
        "native-grant",
        "authority1",
        "authority2",
        "authority3",
    ] {
        let fixture = prepared_fixture();
        let mut recovery = fixture.reopen();
        let plan = retirement(&fixture, &recovery).unwrap();
        let before = kv(&recovery.store);
        let sequence = recovery.store.db.latest_sequence_number();
        let originals = inputs(&fixture.path());
        let mut calls = 0;
        let result = recovery.close_empty_initial_attempt(
            &plan,
            grant(
                &fixture.budget,
                if fault == "data-grant" { 4095 } else { 4096 },
                if fault == "native-grant" { 4095 } else { 4096 },
            ),
            || {
                calls += 1;
                if fault == format!("authority{calls}") {
                    Err(BootstrapError::AuthorityUnavailable)
                } else {
                    Ok(())
                }
            },
        );
        match fault {
            "data-grant" | "native-grant" => {
                assert!(matches!(
                    result,
                    Err(BootstrapError::InsufficientReservation)
                ));
                assert_eq!(calls, 0);
            }
            _ => {
                assert!(matches!(result, Err(BootstrapError::AuthorityUnavailable)));
                assert_eq!(
                    calls,
                    match fault {
                        "authority1" => 1,
                        "authority2" => 2,
                        _ => 3,
                    }
                );
            }
        }
        let committed = fault == "authority3";
        let wanted = if committed {
            closed_rows(&before)
        } else {
            before.clone()
        };
        preserved(
            &fixture,
            &recovery,
            &wanted,
            sequence + u64::from(committed),
            &originals,
        );
        drop(recovery);
        let mut recovery = fixture.reopen();
        drop(
            recovery
                .close_empty_initial_attempt(&plan, grant(&fixture.budget, 4096, 4096), || Ok(()))
                .unwrap(),
        );
        preserved(
            &fixture,
            &recovery,
            &closed_rows(&before),
            sequence + 1,
            &originals,
        );
    }
}
