//! Actual native result provenance. These reuse frozen signed fixture inputs;
//! generic inputs here deliberately cannot authorize the separate Sync PG head.
use super::*;

fn checkpoint_admit(store: &mut IngestStore, budget: &ResourceBudget) -> StoredOutcome {
    store
        .admit_checkpoint_event(
            PreparedEvent::new(
                request(5, 1),
                intent("E_private", 2, 10_000_000),
                256 * 1024,
                grant(budget, MIB, 0),
            )
            .unwrap(),
            ObservedTime::new(10_000_000),
            false,
            AdmissionLimits::default(),
            grant(budget, 40 * MIB, 10 * MIB),
            None,
            || Ok(()),
        )
        .unwrap()
}
fn checkpoint_certify(store: &mut IngestStore, budget: &ResourceBudget) -> StoredCertification {
    store
        .certify_checkpoint_event(
            PreparedCertification::new(
                intent("E_private", 2, 10_000_000),
                256 * 1024,
                grant(budget, MIB, 0),
            )
            .unwrap(),
            false,
            CertificationLimits::default(),
            grant(budget, 32 * MIB, 0),
            || Ok(()),
        )
        .unwrap()
}

#[test]
fn equal_native_receipts_from_another_root_cannot_mint_a_checkpoint_basis() {
    let (_root, mut store, budget, _, _) = setup(false);
    let (_other_root, mut other, other_budget, _, _) = setup(false);
    let result = checkpoint_admit(&mut store, &budget);
    let other_result = checkpoint_admit(&mut other, &other_budget);
    assert_eq!(
        result.native_outcome().bytes(),
        other_result.native_outcome().bytes()
    );
    assert_eq!(
        store.db.latest_sequence_number(),
        other.db.latest_sequence_number()
    );
    assert!(store.checkpoint_event(&result).is_ok());
    assert!(matches!(
        store.checkpoint_event(&other_result),
        Err(AdmissionError::StateMismatch)
    ));
    assert!(matches!(
        other.checkpoint_event(&result),
        Err(AdmissionError::StateMismatch)
    ));
    // A normal retained replay has equal bytes but no issuance stamp.
    let replay = admit(&mut store, &budget, 1).unwrap();
    assert_eq!(
        replay.native_outcome().bytes(),
        result.native_outcome().bytes()
    );
    assert!(matches!(
        store.checkpoint_event(&replay),
        Err(AdmissionError::InvalidInput)
    ));
}

#[test]
fn certification_basis_is_read_only_and_expires_when_native_state_changes() {
    let (_root, mut store, budget, _, _) = setup(false);
    drop(checkpoint_admit(&mut store, &budget));
    let before = kv(&store);
    let sequence = store.db.latest_sequence_number();
    let result = checkpoint_certify(&mut store, &budget);
    let basis = store.checkpoint_certification(&result).unwrap().unwrap();
    assert_eq!(
        basis.kind(),
        super::super::producer_checkpoint::CheckpointKind::ExistingProof
    );
    assert!(basis.request().is_none());
    assert_eq!(
        basis.intent_digest(),
        intent("E_private", 2, 10_000_000).certification_digest()
    );
    assert!(!basis.dependencies().is_empty());
    assert_eq!(kv(&store), before);
    assert_eq!(store.db.latest_sequence_number(), sequence);
    // Deliberate corruption after issuance; not an allowed GC transition.
    let key = crate::storage::rocks::keys::IngestKey::Fact {
        source_record_id: basis.dependencies()[0].source_record_id,
    }
    .encode()
    .unwrap();
    store
        .db
        .delete_cf(store.db.cf_handle("facts").unwrap(), key.as_bytes())
        .unwrap();
    assert!(matches!(
        store.require_checkpoint_basis(&basis),
        Err(AdmissionError::StateMismatch)
    ));
    let damaged = kv(&store);
    let prepared = PreparedCertification::new(
        intent("E_private", 2, 10_000_000),
        256 * 1024,
        grant(&budget, MIB, 0),
    )
    .unwrap();
    assert!(matches!(
        store.certify_checkpoint_event(
            prepared,
            false,
            CertificationLimits::default(),
            grant(&budget, 32 * MIB, 0),
            || Ok(())
        ),
        Err(AdmissionError::StateMismatch)
    ));
    assert_eq!(kv(&store), damaged);
}

#[test]
fn ordinary_certification_and_needs_admission_do_not_create_a_checkpoint_witness() {
    let (_root, mut store, budget, _, _) = setup(false);
    let before = kv(&store);
    let pending = checkpoint_certify(&mut store, &budget);
    assert!(matches!(
        pending.outcome(),
        CertificationOutcome::NeedsAdmission
    ));
    assert!(store.checkpoint_certification(&pending).unwrap().is_none());
    let ordinary = store
        .certify_event(
            PreparedCertification::new(
                intent("E_private", 2, 10_000_000),
                256 * 1024,
                grant(&budget, MIB, 0),
            )
            .unwrap(),
            false,
            CertificationLimits::default(),
            grant(&budget, 32 * MIB, 0),
            || Ok(()),
        )
        .unwrap();
    assert!(matches!(
        store.checkpoint_certification(&ordinary),
        Err(AdmissionError::InvalidInput)
    ));
    assert_eq!(kv(&store), before);
}

#[test]
fn rejected_signature_has_no_success_dependencies_or_mutation_identity() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/ingest/signed-event-admission.json"
    ))
    .unwrap();
    let operation = fixture["scenarios"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|s| s["operations"].as_array().unwrap())
        .find(|op| op["name"] == "invalid_signature_cannot_backdate")
        .unwrap();
    let intent = fixture_intent(&operation["input"]);
    let provenance = intent.observation().is_some_and(|o| o.witness.is_some());
    let (_root, mut store, budget, _, _) = setup(provenance);
    let digest = intent.certification_digest();
    let before = kv(&store);
    let result = store.certify_checkpoint_event(
        PreparedCertification::new(intent, 256 * 1024, grant(&budget, MIB, 0)).unwrap(),
        provenance,
        CertificationLimits::default(),
        grant(&budget, 32 * MIB, 0),
        || Ok(()),
    );
    let result = result.unwrap();
    assert!(matches!(
        result.outcome(),
        CertificationOutcome::Rejected(RejectionReason::Signature)
    ));
    let basis = store.checkpoint_certification(&result).unwrap().unwrap();
    assert_eq!(
        basis.kind(),
        super::super::producer_checkpoint::CheckpointKind::CertificationRejected
    );
    assert_eq!(basis.intent_digest(), digest);
    assert!(basis.request().is_none());
    assert!(basis.dependencies().is_empty());
    assert_eq!(kv(&store), before);
}

#[test]
fn retained_progress_reader_checks_exact_key_full_hash_and_original_frontier() {
    use crate::storage::rocks::keys::IngestKey;
    use sha2::{Digest, Sha256};
    for case in [
        "valid",
        "absent",
        "wrong-key",
        "payload-corrupt",
        "acknowledged",
        "retired",
    ] {
        let (root, mut store, budget, manifest, native) = setup(false);
        let result = admit(&mut store, &budget, 1).unwrap();
        let AdmissionOutcome::Durable(receipt) = result.outcome() else {
            panic!("signed fixture")
        };
        let mut reference = receipt.dependencies().records()[0];
        let key = IngestKey::Fact {
            source_record_id: reference.source_record_id,
        }
        .encode()
        .unwrap();
        let cf = store.db.cf_handle("facts").unwrap();
        let mut value = store.db.get_cf(cf, key.as_bytes()).unwrap().unwrap();
        match case {
            "absent" => store.db.delete_cf(cf, key.as_bytes()).unwrap(),
            "wrong-key" => {
                reference.source_record_id.sequence = Sequence::new(999).unwrap();
                reference.payload_digest =
                    Digest256::from_bytes(Sha256::digest(&value[34..]).into());
                value[2..34].copy_from_slice(reference.payload_digest.as_bytes());
                let changed = IngestKey::Fact {
                    source_record_id: reference.source_record_id,
                }
                .encode()
                .unwrap();
                store.db.put_cf(cf, changed.as_bytes(), value).unwrap();
            }
            "payload-corrupt" => {
                *value.last_mut().unwrap() ^= 1;
                store.db.put_cf(cf, key.as_bytes(), value).unwrap();
            }
            "acknowledged" | "retired" => {
                let key = IngestKey::ProducerState {
                    producer_slot_id: request(5, 1).producer_slot_id,
                    producer_epoch: ProducerEpoch::new(1).unwrap(),
                }
                .encode()
                .unwrap();
                let mut state = vec![1, 0x31];
                for n in [1u64, 1, u64::from(case == "retired")] {
                    state.extend_from_slice(&n.to_be_bytes());
                }
                store
                    .db
                    .put_cf(
                        store.db.cf_handle("receipts").unwrap(),
                        key.as_bytes(),
                        state,
                    )
                    .unwrap();
            }
            _ => {}
        }
        drop(result);
        drop(store);
        let recovery = IngestRecovery::open(
            &root.path().join("db"),
            manifest,
            native,
            grant(&budget, 0, 128 * MIB),
        )
        .unwrap();
        let scratch = grant(&budget, 4096, 0);
        let view = recovery.progress_read(&scratch).unwrap();
        let before = kv(&recovery.store);
        let frontier = view.require_frontier(
            request(5, 1).producer_slot_id,
            ProducerEpoch::new(1).unwrap(),
            1,
            1,
        );
        if case == "retired" {
            assert!(
                matches!(frontier, Err(AdmissionError::StateMismatch)),
                "{case}"
            );
        } else {
            frontier.unwrap();
            let check = view.require_fact(reference, || Ok(()));
            if matches!(case, "valid" | "acknowledged") {
                check.unwrap();
            } else {
                assert!(
                    matches!(check, Err(AdmissionError::StateMismatch)),
                    "{case}"
                );
            }
        }
        if case == "acknowledged" {
            assert!(matches!(
                view.require_frontier(
                    request(5, 1).producer_slot_id,
                    ProducerEpoch::new(1).unwrap(),
                    0,
                    1
                ),
                Err(AdmissionError::StateMismatch)
            ));
        }
        assert_eq!(kv(&recovery.store), before, "{case}");
    }
}

#[test]
fn retained_progress_reader_refuses_short_grant_cancel_and_changed_sequence() {
    use crate::storage::rocks::keys::IngestKey;
    let (root, mut store, budget, manifest, native) = setup(false);
    let result = admit(&mut store, &budget, 1).unwrap();
    let AdmissionOutcome::Durable(receipt) = result.outcome() else {
        panic!("signed fixture")
    };
    let reference = receipt.dependencies().records()[0];
    drop(result);
    drop(store);
    let recovery = IngestRecovery::open(
        &root.path().join("db"),
        manifest,
        native,
        grant(&budget, 0, 128 * MIB),
    )
    .unwrap();
    let too_small = grant(&budget, 4095, 0);
    assert!(matches!(
        recovery.progress_read(&too_small),
        Err(AdmissionError::InsufficientReservation)
    ));
    let scratch = grant(&budget, 4096, 0);
    let view = recovery.progress_read(&scratch).unwrap();
    let before = kv(&recovery.store);
    assert_eq!(
        view.require_fact(reference, || Err(AdmissionError::AuthorityUnavailable)),
        Err(AdmissionError::AuthorityUnavailable)
    );
    assert_eq!(kv(&recovery.store), before);
    assert!(matches!(
        view.require_frontier(
            request(5, 1).producer_slot_id,
            ProducerEpoch::new(1).unwrap(),
            2,
            2
        ),
        Err(AdmissionError::StateMismatch)
    ));
    let key = IngestKey::Fact {
        source_record_id: reference.source_record_id,
    }
    .encode()
    .unwrap();
    let cf = recovery.store.db.cf_handle("facts").unwrap();
    let bytes = recovery
        .store
        .db
        .get_cf(cf, key.as_bytes())
        .unwrap()
        .unwrap();
    // Equal bytes still advance Rocks sequence: a reader cannot span writes.
    recovery.store.db.put_cf(cf, key.as_bytes(), bytes).unwrap();
    assert_eq!(view.validate(), Err(AdmissionError::StateMismatch));
}
