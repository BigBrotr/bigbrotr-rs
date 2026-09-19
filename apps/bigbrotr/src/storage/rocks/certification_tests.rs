//! Actual native nonmutation; shared frozen signed inputs, no verifier mocks.
use super::*;

fn imported(scope: u64) -> EventIntent {
    EventIntent::import(
        1,
        reference(DictionaryKind::VisibilityScope, scope),
        fixture_event("E_private"),
        reference(DictionaryKind::DatasetLineage, 1),
    )
    .unwrap()
}

#[test]
fn existing_import_certifies_with_a_full_mutation_window_without_new_detail() {
    let (root, mut store, budget, _, _) = setup(false);
    let first = PreparedEvent::new(
        request(5, 1),
        imported(1),
        256 * 1024,
        grant(&budget, MIB, 0),
    )
    .unwrap();
    drop(
        store
            .admit_event(
                first,
                ObservedTime::new(100),
                false,
                AdmissionLimits {
                    max_outstanding_requests: 1,
                    ..AdmissionLimits::default()
                },
                grant(&budget, 40 * MIB, 10 * MIB),
                || Ok(()),
            )
            .unwrap(),
    );
    let before = kv(&store);
    let sequence = store.db.latest_sequence_number();
    let spool = root.path().join("db/producer-inputs-v1");
    let files = || {
        let mut values = std::fs::read_dir(&spool)
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                (entry.file_name(), std::fs::read(entry.path()).unwrap())
            })
            .collect::<Vec<_>>();
        values.sort();
        values
    };
    let originals = files();
    for _ in 0..3 {
        let input =
            PreparedCertification::new(imported(1), 256 * 1024, grant(&budget, MIB, 0)).unwrap();
        let result = store
            .certify_event(
                input,
                false,
                CertificationLimits::default(),
                grant(&budget, 32 * MIB, 0),
                || Ok(()),
            )
            .unwrap();
        let CertificationOutcome::Existing(proof) = result.outcome() else {
            panic!("complete retained import effects must certify without a mutation request")
        };
        assert_eq!(proof.intent_digest(), imported(1).certification_digest());
        assert_eq!(
            proof
                .dependencies()
                .records()
                .iter()
                .map(|r| r.source_record_id.sequence.get())
                .collect::<Vec<_>>(),
            [1, 4, 5, 6, 7]
        );
        assert_eq!(
            proof.protection_context().recovery_generation,
            RecoveryGenerationId::from_bytes([6; 16])
        );
        assert_eq!(proof.protection_context().policy_epoch.get(), 1);
        assert_eq!(store.db.latest_sequence_number(), sequence);
        assert_eq!(kv(&store), before);
        assert_eq!(files(), originals);
    }
}

fn certify(
    store: &mut IngestStore,
    budget: &ResourceBudget,
    intent: EventIntent,
    provenance: bool,
) -> Result<StoredCertification, AdmissionError> {
    store.certify_event(
        PreparedCertification::new(intent, 256 * 1024, grant(budget, MIB, 0))?,
        provenance,
        CertificationLimits::default(),
        grant(budget, 32 * MIB, 0),
        || Ok(()),
    )
}

#[test]
fn signed_corpus_certifies_only_sufficient_effects_without_reinterpreting_representatives() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/ingest/signed-event-admission.json"
    ))
    .unwrap();
    let known_before = [
        "later_alternate_signature_same_scope",
        "exact_retry_returns_original_durable_result",
        "same_request_different_valid_signature_conflicts",
        "same_request_different_observation_conflicts",
    ];
    let mut evaluations = 0;
    for scenario in fixture["scenarios"].as_array().unwrap() {
        let provenance = scenario["provenance_enabled"].as_bool().unwrap();
        let (_root, mut store, budget, _, _) = setup(provenance);
        for operation in scenario["operations"].as_array().unwrap() {
            let name = operation["name"].as_str().unwrap();
            let input = &operation["input"];
            let invalid = name == "invalid_signature_cannot_backdate";
            for after in [false, true] {
                if after {
                    let id = ProducerRequestId {
                        producer_slot_id: input["request_id"]["producer_slot_id"]
                            .as_str()
                            .unwrap()
                            .parse()
                            .unwrap(),
                        producer_epoch: ProducerEpoch::new(
                            input["request_id"]["producer_epoch"].as_u64().unwrap(),
                        )
                        .unwrap(),
                        request_sequence: RequestSequence::new(
                            input["request_id"]["request_sequence"].as_u64().unwrap(),
                        )
                        .unwrap(),
                    };
                    let prepared = PreparedEvent::new(
                        id,
                        fixture_intent(input),
                        256 * 1024,
                        grant(&budget, MIB, 0),
                    )
                    .unwrap();
                    let admitted = store.admit_event(
                        prepared,
                        ObservedTime::new(input["owner_recorded_at_us"].as_i64().unwrap()),
                        provenance,
                        AdmissionLimits::default(),
                        grant(&budget, 40 * MIB, 10 * MIB),
                        || Ok(()),
                    );
                    if operation["expected"]["result"] == "RequestConflict" {
                        assert!(matches!(admitted, Err(AdmissionError::RequestConflict)));
                    } else {
                        drop(admitted.unwrap());
                    }
                }
                let before = kv(&store);
                let sequence = store.db.latest_sequence_number();
                let result =
                    certify(&mut store, &budget, fixture_intent(input), provenance).unwrap();
                match result.outcome() {
                    CertificationOutcome::Existing(proof) => {
                        assert!(
                            !invalid && (after || known_before.contains(&name)),
                            "{name}, after={after}"
                        );
                        assert_eq!(
                            proof.intent_digest(),
                            fixture_intent(input).certification_digest()
                        );
                    }
                    CertificationOutcome::NeedsAdmission => assert!(
                        !after && !invalid && !known_before.contains(&name),
                        "{name}"
                    ),
                    CertificationOutcome::Rejected(reason) => {
                        assert!(invalid && *reason == RejectionReason::Signature, "{name}")
                    }
                }
                assert_eq!(store.db.latest_sequence_number(), sequence);
                assert_eq!(kv(&store), before);
                evaluations += 1;
            }
        }
    }
    assert_eq!(evaluations, 30);
}

#[test]
fn certification_refuses_uncertain_state_bad_grants_and_lost_delivery_authority() {
    let (_root, mut store, budget, _, _) = setup(false);
    drop(admit(&mut store, &budget, 1).unwrap());
    let before = kv(&store);
    let sequence = store.db.latest_sequence_number();
    for at in [1, 2] {
        let mut calls = 0;
        let prepared = PreparedCertification::new(
            intent("E_private", 2, 10_000_000),
            256 * 1024,
            grant(&budget, MIB, 0),
        )
        .unwrap();
        let result = store.certify_event(
            prepared,
            false,
            CertificationLimits::default(),
            grant(&budget, 32 * MIB, 0),
            || {
                calls += 1;
                if calls == at {
                    Err(AdmissionError::AuthorityUnavailable)
                } else {
                    Ok(())
                }
            },
        );
        assert!(matches!(result, Err(AdmissionError::AuthorityUnavailable)));
        assert_eq!(calls, at);
    }
    store.write_uncertain = true;
    assert!(matches!(
        certify(
            &mut store,
            &budget,
            intent("E_private", 2, 10_000_000),
            false
        ),
        Err(AdmissionError::CommitUnknown)
    ));
    store.write_uncertain = false;
    let prepared =
        PreparedCertification::new(imported(1), 256 * 1024, grant(&budget, MIB, 0)).unwrap();
    assert!(matches!(
        store.certify_event(
            prepared,
            false,
            CertificationLimits::default(),
            grant(&budget, 1, 0),
            || Ok(())
        ),
        Err(AdmissionError::InsufficientReservation)
    ));
    assert_eq!(store.db.latest_sequence_number(), sequence);
    assert_eq!(kv(&store), before);
}

#[test]
fn certification_retains_both_input_and_historical_lineage() {
    let mut dictionaries = entries(false);
    dictionaries.push(
        DictionaryEntry::new(
            reference(DictionaryKind::DatasetLineage, 2),
            DictionaryDescriptor::DatasetLineage(DatasetLineageDescriptor {
                format: label("nostr-jsonl"),
                format_version: 1,
                source_dataset_id: None,
                source_artifact_digest: Digest256::from_bytes([89; 32]),
            }),
        )
        .unwrap(),
    );
    let (_root, mut store, budget, _, _) = setup_entries(dictionaries);
    drop(
        store
            .admit_event(
                PreparedEvent::new(
                    request(5, 1),
                    imported(1),
                    256 * 1024,
                    grant(&budget, MIB, 0),
                )
                .unwrap(),
                ObservedTime::new(100),
                false,
                AdmissionLimits::default(),
                grant(&budget, 40 * MIB, 10 * MIB),
                || Ok(()),
            )
            .unwrap(),
    );
    let before = kv(&store);
    let changed = |n| {
        EventIntent::import(
            1,
            reference(DictionaryKind::VisibilityScope, 1),
            fixture_event("E_private"),
            reference(DictionaryKind::DatasetLineage, n),
        )
        .unwrap()
    };
    let result = certify(&mut store, &budget, changed(2), false).unwrap();
    let CertificationOutcome::Existing(proof) = result.outcome() else {
        panic!("both lineages are retained")
    };
    assert_eq!(
        proof
            .dependencies()
            .records()
            .iter()
            .map(|r| r.source_record_id.sequence.get())
            .collect::<Vec<_>>(),
        [1, 4, 5, 6, 7, 8]
    );
    assert!(matches!(
        certify(&mut store, &budget, changed(3), false)
            .unwrap()
            .outcome(),
        CertificationOutcome::NeedsAdmission
    ));
    assert_eq!(kv(&store), before);
}

#[test]
fn certification_retains_input_and_stored_observers_but_requires_the_exact_witness_key() {
    for provenance in [false, true] {
        let mut dictionaries = entries(provenance);
        dictionaries.push(
            DictionaryEntry::new(
                reference(DictionaryKind::Observer, 2),
                DictionaryDescriptor::Observer(ObserverDescriptor {
                    clock: Some(ClockSource::SystemUtc),
                    vantage: Some(label("second-vantage")),
                    software: None,
                    method_id: None,
                    public_key: None,
                    route_class: None,
                    software_version: None,
                }),
            )
            .unwrap(),
        );
        let (_root, mut store, budget, _, _) = setup_entries(dictionaries);
        let observed = |observer, received| {
            EventIntent::network(
                1,
                reference(DictionaryKind::VisibilityScope, 1),
                fixture_event("E_private"),
                LocalObservation {
                    received_at: ObservedTime::new(received),
                    observer_id: reference(DictionaryKind::Observer, observer),
                    clock_quality: ClockQuality::new(
                        reference(DictionaryKind::Observer, observer),
                        DurationUs::new(1),
                        None,
                        0,
                    )
                    .unwrap(),
                    witness: provenance.then_some(WitnessContext {
                        endpoint_id: reference(DictionaryKind::Endpoint, 1),
                        access_scope_id: reference(DictionaryKind::AccessScope, 1),
                        verification_method_id: reference(DictionaryKind::Method, 1),
                    }),
                },
            )
            .unwrap()
        };
        drop(
            store
                .admit_event(
                    PreparedEvent::new(
                        request(5, 1),
                        observed(1, 100),
                        256 * 1024,
                        grant(&budget, MIB, 0),
                    )
                    .unwrap(),
                    ObservedTime::new(100),
                    provenance,
                    AdmissionLimits::default(),
                    grant(&budget, 40 * MIB, 10 * MIB),
                    || Ok(()),
                )
                .unwrap(),
        );
        let before = kv(&store);
        for received in [100, 120] {
            let result = certify(&mut store, &budget, observed(2, received), provenance).unwrap();
            if provenance {
                assert!(matches!(
                    result.outcome(),
                    CertificationOutcome::NeedsAdmission
                ));
            } else {
                let CertificationOutcome::Existing(proof) = result.outcome() else {
                    panic!("receive minimum is scoped, not per observer")
                };
                assert_eq!(
                    proof
                        .dependencies()
                        .records()
                        .iter()
                        .map(|r| r.source_record_id.sequence.get())
                        .collect::<Vec<_>>(),
                    [1, 3, 5, 6, 7, 8, 9]
                );
            }
        }
        assert!(matches!(
            certify(&mut store, &budget, observed(1, 99), provenance)
                .unwrap()
                .outcome(),
            CertificationOutcome::NeedsAdmission
        ));
        assert_eq!(kv(&store), before);
    }
}

#[test]
fn certification_limits_and_dangling_dependencies_never_become_a_proof() {
    use crate::storage::rocks::keys::IngestKey;
    let (_root, mut store, budget, _, _) = setup(true);
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/ingest/signed-event-admission.json"
    ))
    .unwrap();
    let input = &fixture["scenarios"][0]["operations"][0]["input"];
    drop(
        store
            .admit_event(
                PreparedEvent::new(
                    request(5, 1),
                    fixture_intent(input),
                    256 * 1024,
                    grant(&budget, MIB, 0),
                )
                .unwrap(),
                ObservedTime::new(100),
                true,
                AdmissionLimits::default(),
                grant(&budget, 40 * MIB, 10 * MIB),
                || Ok(()),
            )
            .unwrap(),
    );
    let before = kv(&store);
    let limits = CertificationLimits {
        max_dependencies: 8,
        ..CertificationLimits::default()
    };
    assert!(matches!(
        store.certify_event(
            PreparedCertification::new(fixture_intent(input), 256 * 1024, grant(&budget, MIB, 0))
                .unwrap(),
            true,
            limits,
            grant(&budget, 32 * MIB, 0),
            || Ok(())
        ),
        Err(AdmissionError::Limit)
    ));
    assert!(matches!(
        certify(&mut store, &budget, fixture_intent(input), true)
            .unwrap()
            .outcome(),
        CertificationOutcome::Existing(_)
    ));
    assert_eq!(kv(&store), before);
    // A healthy missing input mapping differs from a retained index whose fact
    // was physically removed. This is a corruption fixture, not legitimate GC.
    let mapping = reference(DictionaryKind::VisibilityScope, 2);
    let fact_id = selected(&store, IngestKey::Dictionary { reference: mapping });
    let key = IngestKey::Fact {
        source_record_id: fact_id,
    }
    .encode()
    .unwrap();
    store
        .db
        .delete_cf(
            store.db.cf_handle(key.column_family().name()).unwrap(),
            key.as_bytes(),
        )
        .unwrap();
    let damaged = kv(&store);
    let sequence = store.db.latest_sequence_number();
    assert!(matches!(
        certify(&mut store, &budget, fixture_intent(input), true),
        Err(AdmissionError::StateMismatch)
    ));
    assert_eq!(kv(&store), damaged);
    assert_eq!(store.db.latest_sequence_number(), sequence);
}
