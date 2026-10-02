//! Native append invariants; independent portable-byte checks live in the lab.
use super::bootstrap_tests::{budget, grant, intent, manifest, native, request, scope};
use super::*;
use bigbrotr_model::*;

const MIB: u64 = 1024 * 1024;
fn limits() -> AdmissionLimits {
    AdmissionLimits {
        max_batch_bytes: 256 * 1024,
        ..AdmissionLimits::default()
    }
}
fn bootstrap_limits() -> BootstrapLimits {
    BootstrapLimits {
        max_mappings: 8,
        max_batch_bytes: 256 * 1024,
    }
}
fn next_request(sequence: u64) -> ProducerRequestId {
    ProducerRequestId {
        request_sequence: RequestSequence::new(sequence).unwrap(),
        ..request()
    }
}
fn private_intent() -> EvidenceIntent {
    let private = QualifiedRef {
        ordinal: Ordinal::new(2).unwrap(),
        ..scope()
    };
    EvidenceIntent::new(
        1,
        private,
        vec![
            DictionaryEntry::new(
                private,
                DictionaryDescriptor::Visibility {
                    class: DescriptorLabel::new("private".into()).unwrap(),
                },
            )
            .unwrap(),
        ],
        EvidenceIntentLimits {
            max_entries: 8,
            max_encoded_intent_bytes: 8192,
            max_validation_scratch_bytes: 1024,
        },
    )
    .unwrap()
}

#[test]
fn append_preserves_original_bootstrap_and_replays_original_receipts_after_reopen() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("db");
    let b = budget();
    let mut store =
        IngestStore::initialize(&path, manifest(), native(), grant(&b, 0, 128 * MIB)).unwrap();
    let original = intent("public");
    let first = store
        .bootstrap_dictionaries(
            request(),
            &original,
            ObservedTime::new(10),
            bootstrap_limits(),
            grant(&b, 4 * MIB, MIB),
            || Ok(()),
        )
        .unwrap();
    let original_bytes = first.receipt().encode_persistent(8192).unwrap();
    drop(first);
    let private = private_intent();
    let second = store
        .append_dictionaries(
            next_request(2),
            &private,
            ObservedTime::new(20),
            limits(),
            grant(&b, 4 * MIB, MIB),
            || Ok(()),
        )
        .unwrap();
    let second_bytes = second.receipt().encode_persistent(8192).unwrap();
    assert_eq!(second.receipt().batch().unwrap().first().sequence.get(), 2);
    assert_eq!(second.receipt().dependencies().records().len(), 1);
    drop(second);
    let third = store
        .append_dictionaries(
            next_request(3),
            &private,
            ObservedTime::new(30),
            limits(),
            grant(&b, 4 * MIB, MIB),
            || Ok(()),
        )
        .unwrap();
    assert!(
        third.receipt().batch().is_none(),
        "same mapping must not emit another fact"
    );
    drop(third);
    drop(store);
    let mut store =
        IngestStore::reopen(&path, manifest(), native(), grant(&b, 0, 128 * MIB)).unwrap();
    let before = store.db.latest_sequence_number();
    let first = store
        .bootstrap_dictionaries(
            request(),
            &original,
            ObservedTime::new(999),
            bootstrap_limits(),
            grant(&b, 4 * MIB, MIB),
            || Ok(()),
        )
        .unwrap();
    assert_eq!(
        first.receipt().encode_persistent(8192).unwrap(),
        original_bytes
    );
    drop(first);
    let second = store
        .append_dictionaries(
            next_request(2),
            &private,
            ObservedTime::new(999),
            limits(),
            grant(&b, 4 * MIB, MIB),
            || Ok(()),
        )
        .unwrap();
    assert_eq!(
        second.receipt().encode_persistent(8192).unwrap(),
        second_bytes
    );
    drop(second);
    assert_eq!(store.db.latest_sequence_number(), before);
}

fn evidence(scope: QualifiedRef, entries: Vec<DictionaryEntry>) -> EvidenceIntent {
    EvidenceIntent::new(
        1,
        scope,
        entries,
        EvidenceIntentLimits {
            max_entries: 64,
            max_encoded_intent_bytes: 65536,
            max_validation_scratch_bytes: 8192,
        },
    )
    .unwrap()
}
fn reference(kind: DictionaryKind, ordinal: u64) -> QualifiedRef {
    QualifiedRef {
        dictionary_kind: kind,
        ordinal: Ordinal::new(ordinal).unwrap(),
        allocation_domain: [kind as u8; 16].try_into().unwrap(),
        ..scope()
    }
}
fn observer(ordinal: u64) -> DictionaryEntry {
    DictionaryEntry::new(
        reference(DictionaryKind::Observer, ordinal),
        DictionaryDescriptor::Observer(ObserverDescriptor {
            clock: Some(ClockSource::SystemUtc),
            vantage: Some(DescriptorLabel::new(format!("observer{ordinal}")).unwrap()),
            software: None,
            method_id: None,
            public_key: None,
            route_class: Some(RouteClass::Private),
            software_version: None,
        }),
    )
    .unwrap()
}
fn installed_bootstrap() -> EvidenceIntent {
    evidence(
        reference(DictionaryKind::VisibilityScope, 1),
        ["public", "private"]
            .into_iter()
            .enumerate()
            .map(|(i, class)| {
                DictionaryEntry::new(
                    reference(DictionaryKind::VisibilityScope, i as u64 + 1),
                    DictionaryDescriptor::Visibility {
                        class: DescriptorLabel::new(class.into()).unwrap(),
                    },
                )
                .unwrap()
            })
            .collect(),
    )
}
fn installed_manifest() -> IngestManifest {
    use crate::storage::rocks::manifest::AllocationNamespaces;
    let domains = [
        DictionaryKind::Endpoint,
        DictionaryKind::Observer,
        DictionaryKind::AccessScope,
        DictionaryKind::VisibilityScope,
        DictionaryKind::Method,
        DictionaryKind::Event,
    ]
    .map(|kind| (kind, reference(kind, 1).allocation_domain));
    IngestManifest::new(
        *manifest().identity(),
        AllocationNamespaces::from_entries(&domains).unwrap(),
    )
}
fn private_bundle() -> EvidenceIntent {
    let private = reference(DictionaryKind::VisibilityScope, 3);
    let entries = vec![
        DictionaryEntry::new(
            private,
            DictionaryDescriptor::Visibility {
                class: DescriptorLabel::new("private".into()).unwrap(),
            },
        )
        .unwrap(),
        observer(1),
        observer(2), // Distinct clock/observer exercises one allocation domain twice.
        DictionaryEntry::new(
            reference(DictionaryKind::Endpoint, 1),
            DictionaryDescriptor::Endpoint(
                EndpointDescriptor::new("ws://127.0.0.1:7447/".into()).unwrap(),
            ),
        )
        .unwrap(),
        DictionaryEntry::new(
            reference(DictionaryKind::AccessScope, 1),
            DictionaryDescriptor::AccessScope(AccessScopeDescriptor {
                class: AccessClass::Public,
                label: None,
                visibility_scope_id: private,
            }),
        )
        .unwrap(),
        DictionaryEntry::new(
            reference(DictionaryKind::Method, 1),
            DictionaryDescriptor::EventVerification,
        )
        .unwrap(),
    ];
    evidence(private, entries)
}
fn boot_installed(store: &mut IngestStore, budget: &crate::runtime::ResourceBudget) {
    drop(
        store
            .bootstrap_dictionaries(
                request(),
                &installed_bootstrap(),
                ObservedTime::new(10),
                bootstrap_limits(),
                grant(budget, 4 * MIB, MIB),
                || Ok(()),
            )
            .unwrap(),
    );
}
fn append(
    store: &mut IngestStore,
    budget: &crate::runtime::ResourceBudget,
    sequence: u64,
    value: &EvidenceIntent,
) -> Result<StoredReceipt, AdmissionError> {
    store.append_dictionaries(
        next_request(sequence),
        value,
        ObservedTime::new(20),
        limits(),
        grant(budget, 4 * MIB, MIB),
        || Ok(()),
    )
}

#[test]
fn six_mapping_private_bundle_closes_observation_and_mixed_existing_new_ordinals() {
    use crate::acquisition::observation::ObservationBindings;
    let root = tempfile::tempdir().unwrap();
    let b = budget();
    let mut store = IngestStore::initialize(
        &root.path().join("db"),
        installed_manifest(),
        native(),
        grant(&b, 0, 128 * MIB),
    )
    .unwrap();
    boot_installed(&mut store, &b);
    let bundle = private_bundle();
    let second = append(&mut store, &b, 2, &bundle).unwrap();
    assert_eq!(second.receipt().batch().unwrap().records().len(), 6);
    assert_eq!(second.receipt().batch().unwrap().first().sequence.get(), 3);
    assert_eq!(second.receipt().batch().unwrap().last().sequence.get(), 8);
    let bytes = second.receipt().encode_persistent(16384).unwrap();
    drop(second);
    let replay = append(&mut store, &b, 2, &bundle).unwrap();
    assert_eq!(replay.receipt().encode_persistent(16384).unwrap(), bytes);
    drop(replay);
    let read_limits = CertificationLimits {
        max_fact_bytes: 8192,
        max_dependencies: 16,
    };
    let bindings = ObservationBindings {
        visibility: bundle.visibility_scope_id(),
        observer: &bundle.dictionaries()[1],
        clock: &bundle.dictionaries()[2],
        witness: Some(WitnessContext {
            endpoint_id: reference(DictionaryKind::Endpoint, 1),
            access_scope_id: reference(DictionaryKind::AccessScope, 1),
            verification_method_id: reference(DictionaryKind::Method, 1),
        }),
    };
    let observed = store
        .read_observation(
            bindings,
            read_limits,
            grant(&b, read_limits.required_data_bytes().unwrap(), 0),
            || Ok(()),
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        observed.visibility(),
        reference(DictionaryKind::VisibilityScope, 3)
    );
    drop(observed);
    let mixed = evidence(
        bundle.visibility_scope_id(),
        vec![observer(1), observer(3), observer(2), observer(4)],
    );
    let third = append(&mut store, &b, 3, &mixed).unwrap();
    assert_eq!(third.receipt().batch().unwrap().records().len(), 2);
    assert_eq!(third.receipt().batch().unwrap().first().sequence.get(), 9);
    assert_eq!(third.receipt().batch().unwrap().last().sequence.get(), 10);
    assert_eq!(third.receipt().dependencies().records().len(), 5);
    drop(third);
    drop(append(&mut store, &b, 3, &mixed).unwrap());
    drop(
        store
            .bootstrap_dictionaries(
                request(),
                &installed_bootstrap(),
                ObservedTime::new(999),
                bootstrap_limits(),
                grant(&b, 4 * MIB, MIB),
                || Ok(()),
            )
            .unwrap(),
    );
}

#[test]
fn predictable_invalid_plans_fail_before_staging_and_leave_request_identity_available() {
    let root = tempfile::tempdir().unwrap();
    let b = budget();
    let mut store = IngestStore::initialize(
        &root.path().join("db"),
        installed_manifest(),
        native(),
        grant(&b, 0, 128 * MIB),
    )
    .unwrap();
    boot_installed(&mut store, &b);
    let bundle = private_bundle();
    let hole = reference(DictionaryKind::VisibilityScope, 4);
    let invalid = [
        evidence(
            hole,
            vec![
                DictionaryEntry::new(
                    hole,
                    DictionaryDescriptor::Visibility {
                        class: DescriptorLabel::new("private".into()).unwrap(),
                    },
                )
                .unwrap(),
            ],
        ),
        evidence(
            reference(DictionaryKind::VisibilityScope, 2),
            vec![observer(1)],
        ), // Original private2 has a public envelope.
        evidence(bundle.visibility_scope_id(), vec![observer(1)]), // Missing visibility.
        evidence(
            reference(DictionaryKind::VisibilityScope, 1),
            vec![
                DictionaryEntry::new(
                    reference(DictionaryKind::Endpoint, 1),
                    DictionaryDescriptor::Endpoint(
                        EndpointDescriptor::new("wss://RELAY.example:443/".into()).unwrap(),
                    ),
                )
                .unwrap(),
            ],
        ),
    ];
    let before = store.db.latest_sequence_number();
    let permit = grant(&b, 4 * MIB, MIB);
    for (input, expected) in invalid.iter().zip([
        AdmissionError::InvalidInput,
        AdmissionError::InvalidInput,
        AdmissionError::StateMismatch,
        AdmissionError::InvalidInput,
    ]) {
        assert_eq!(
            store
                .stage_dictionary_append(
                    next_request(2),
                    input,
                    ObservedTime::new(20),
                    limits(),
                    &permit
                )
                .err(),
            Some(expected)
        );
    }
    assert_eq!(store.db.latest_sequence_number(), before);
    // If any rejected input was staged, its different original digest poisons this request.
    drop(
        store
            .stage_dictionary_append(
                next_request(2),
                &bundle,
                ObservedTime::new(20),
                limits(),
                &permit,
            )
            .unwrap(),
    );
    drop(permit);
    drop(append(&mut store, &b, 2, &bundle).unwrap());
}

#[test]
fn closed_producer_resolves_bootstrap_append_and_reuse_without_fresh_writes() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("db");
    let b = budget();
    let mut store = IngestStore::initialize(
        &path,
        installed_manifest(),
        native(),
        grant(&b, 0, 128 * MIB),
    )
    .unwrap();
    boot_installed(&mut store, &b);
    let bundle = private_bundle();
    let second = append(&mut store, &b, 2, &bundle).unwrap();
    let bytes = second.receipt().encode_persistent(16384).unwrap();
    drop(second);
    drop(append(&mut store, &b, 3, &bundle).unwrap());
    drop(store);
    let mut recovery = IngestRecovery::open(
        &path,
        installed_manifest(),
        native(),
        grant(&b, 0, 128 * MIB),
    )
    .unwrap();
    drop(
        recovery
            .close_producer(
                request().producer_slot_id,
                request().producer_epoch,
                grant(&b, 4096, 4096),
                || Ok(()),
            )
            .unwrap(),
    );
    let before = recovery.store.db.latest_sequence_number();
    let recovery_limits = ProducerRecoveryLimits {
        input: codec::request::RequestDecodeLimits {
            values: codec::FactDecodeLimits {
                max_encoded_bytes: 65536,
                max_allocation_bytes: 65536,
                max_tag_nodes: 1,
                max_string_bytes: 8192,
            },
            max_dictionaries: 16,
        },
        bootstrap: bootstrap_limits(),
        event: limits(),
    };
    let bootstrap = installed_bootstrap();
    for (sequence, input) in [(1, &bootstrap), (2, &bundle), (3, &bundle)] {
        let result = recovery
            .resolve_original_request(
                next_request(sequence),
                input.request_digest(next_request(sequence)),
                recovery_limits,
                grant(&b, MIB, 0),
                grant(&b, 4 * MIB, 0),
                || Ok(()),
            )
            .unwrap();
        let AdmissionOutcome::Durable(receipt) = result.outcome() else {
            panic!("dictionary must be durable")
        };
        if sequence == 2 {
            assert_eq!(receipt.encode_persistent(16384).unwrap(), bytes);
        }
        if sequence == 3 {
            assert!(receipt.batch().is_none());
        }
    }
    assert_eq!(recovery.store.db.latest_sequence_number(), before);
    assert_eq!(
        append(&mut recovery.store, &b, 4, &bundle).err(),
        Some(AdmissionError::ProducerClosing)
    );
}

#[test]
fn limits_conflicts_gap_window_and_lost_authority_cannot_change_committed_rows() {
    let root = tempfile::tempdir().unwrap();
    let b = budget();
    let mut store = IngestStore::initialize(
        &root.path().join("db"),
        manifest(),
        native(),
        grant(&b, 0, 128 * MIB),
    )
    .unwrap();
    drop(
        store
            .bootstrap_dictionaries(
                request(),
                &intent("public"),
                ObservedTime::new(10),
                bootstrap_limits(),
                grant(&b, 4 * MIB, MIB),
                || Ok(()),
            )
            .unwrap(),
    );
    let value = private_intent();
    let before = store.db.latest_sequence_number();
    assert_eq!(
        append(&mut store, &b, 3, &value).err(),
        Some(AdmissionError::RequestGap)
    );
    assert_eq!(
        store
            .append_dictionaries(
                next_request(2),
                &value,
                ObservedTime::new(20),
                AdmissionLimits {
                    max_outstanding_requests: 1,
                    ..limits()
                },
                grant(&b, 4 * MIB, MIB),
                || Ok(())
            )
            .err(),
        Some(AdmissionError::WindowFull)
    );
    let mut checks = 0;
    assert_eq!(
        store
            .append_dictionaries(
                next_request(2),
                &value,
                ObservedTime::new(20),
                limits(),
                grant(&b, 4 * MIB, MIB),
                || {
                    checks += 1;
                    if checks == 3 {
                        Err(AdmissionError::AuthorityUnavailable)
                    } else {
                        Ok(())
                    }
                }
            )
            .err(),
        Some(AdmissionError::AuthorityUnavailable)
    );
    assert_eq!(checks, 3);
    assert_eq!(store.db.latest_sequence_number(), before);
    drop(append(&mut store, &b, 2, &value).unwrap());
    let before = store.db.latest_sequence_number();
    let conflict = evidence(
        value.visibility_scope_id(),
        vec![
            DictionaryEntry::new(
                value.visibility_scope_id(),
                DictionaryDescriptor::Visibility {
                    class: DescriptorLabel::new("changed".into()).unwrap(),
                },
            )
            .unwrap(),
        ],
    );
    assert_eq!(
        append(&mut store, &b, 2, &conflict).err(),
        Some(AdmissionError::RequestConflict)
    );
    assert_eq!(
        append(&mut store, &b, 3, &conflict).err(),
        Some(AdmissionError::StateMismatch)
    );
    assert_eq!(
        store
            .append_dictionaries(
                next_request(2),
                &value,
                ObservedTime::new(999),
                AdmissionLimits {
                    max_batch_bytes: 128,
                    ..limits()
                },
                grant(&b, 4 * MIB, MIB),
                || Ok(())
            )
            .err(),
        Some(AdmissionError::Limit)
    );
    assert_eq!(store.db.latest_sequence_number(), before);
    drop(append(&mut store, &b, 2, &value).unwrap());
}

#[test]
fn preview_charges_the_actual_timestamp_before_staging() {
    let root = tempfile::tempdir().unwrap();
    let b = budget();
    let mut store = IngestStore::initialize(
        &root.path().join("db"),
        manifest(),
        native(),
        grant(&b, 0, 128 * MIB),
    )
    .unwrap();
    drop(
        store
            .bootstrap_dictionaries(
                request(),
                &intent("public"),
                ObservedTime::new(10),
                bootstrap_limits(),
                grant(&b, 4 * MIB, MIB),
                || Ok(()),
            )
            .unwrap(),
    );
    let value = private_intent();
    let permit = grant(&b, 4 * MIB, MIB);
    // Locate the physical bound for this one-fact fixture through the actual
    // preview. The same mapping/input with a wider time must not be reserved.
    let minimum = (128..4096)
        .find(|bytes| {
            store
                .stage_dictionary_append(
                    next_request(2),
                    &value,
                    ObservedTime::new(0),
                    AdmissionLimits {
                        max_batch_bytes: *bytes,
                        ..limits()
                    },
                    &permit,
                )
                .is_ok()
        })
        .unwrap();
    assert_eq!(
        store
            .stage_dictionary_append(
                next_request(2),
                &value,
                ObservedTime::new(i64::MAX),
                AdmissionLimits {
                    max_batch_bytes: minimum,
                    ..limits()
                },
                &permit
            )
            .err(),
        Some(AdmissionError::Limit)
    );
    drop(
        store
            .stage_dictionary_append(
                next_request(2),
                &value,
                ObservedTime::new(i64::MAX),
                AdmissionLimits {
                    max_batch_bytes: minimum + 8,
                    ..limits()
                },
                &permit,
            )
            .unwrap(),
    );
    drop(permit);
    drop(
        store
            .append_dictionaries(
                next_request(2),
                &value,
                ObservedTime::new(i64::MAX),
                AdmissionLimits {
                    max_batch_bytes: minimum + 8,
                    ..limits()
                },
                grant(&b, 4 * MIB, MIB),
                || Ok(()),
            )
            .unwrap(),
    );
}

fn rows(store: &IngestStore) -> std::collections::BTreeMap<(&'static str, Vec<u8>), Vec<u8>> {
    let mut result = std::collections::BTreeMap::new();
    for name in ["default", "facts", "receipts"] {
        for row in store.db.iterator_cf(
            store.db.cf_handle(name).unwrap(),
            rocksdb::IteratorMode::Start,
        ) {
            let (key, value) = row.unwrap();
            result.insert((name, key.to_vec()), value.to_vec());
        }
    }
    result
}

#[test]
fn replay_requires_every_original_member_and_the_receipts_own_exact_closure() {
    use crate::storage::rocks::keys::IngestKey;
    let root = tempfile::tempdir().unwrap();
    let b = budget();
    let mut store = IngestStore::initialize(
        &root.path().join("db"),
        installed_manifest(),
        native(),
        grant(&b, 0, 128 * MIB),
    )
    .unwrap();
    boot_installed(&mut store, &b);
    let before = rows(&store);
    let bundle = private_bundle();
    let outcome = append(&mut store, &b, 2, &bundle).unwrap();
    let original = outcome.receipt().clone();
    drop(outcome);
    let committed = rows(&store);
    for ((name, key), value) in &committed {
        if before.get(&(*name, key.clone())) == Some(value) {
            continue;
        }
        let handle = store.db.cf_handle(name).unwrap();
        store.db.delete_cf(handle, key).unwrap();
        let sequence = store.db.latest_sequence_number();
        assert_eq!(
            append(&mut store, &b, 2, &bundle).err(),
            Some(AdmissionError::StateMismatch),
            "missing {name}:{key:?}"
        );
        assert_eq!(store.db.latest_sequence_number(), sequence);
        store
            .db
            .put_cf(store.db.cf_handle(name).unwrap(), key, value)
            .unwrap();
    }
    assert_eq!(rows(&store), committed);
    let bootstrap = store
        .replay_bootstrap(request(), &installed_bootstrap(), bootstrap_limits())
        .unwrap()
        .unwrap();
    let wrong = Receipt::already_durable(
        original.request_id(),
        original.request_digest(),
        *original.protection_context(),
        bootstrap.dependencies().clone(),
    )
    .unwrap();
    let key = IngestKey::Request {
        request_id: next_request(2),
    }
    .encode()
    .unwrap();
    store
        .db
        .put_cf(
            store.db.cf_handle("receipts").unwrap(),
            key.as_bytes(),
            wrong.encode_persistent(16384).unwrap(),
        )
        .unwrap();
    let sequence = store.db.latest_sequence_number();
    assert_eq!(
        append(&mut store, &b, 2, &bundle).err(),
        Some(AdmissionError::StateMismatch)
    );
    assert_eq!(store.db.latest_sequence_number(), sequence);
    store
        .db
        .put_cf(
            store.db.cf_handle("receipts").unwrap(),
            key.as_bytes(),
            original.encode_persistent(16384).unwrap(),
        )
        .unwrap();
    drop(append(&mut store, &b, 2, &bundle).unwrap());
    assert_eq!(rows(&store), committed);
}

#[test]
fn exhausted_allocation_refuses_before_staging_without_reusing_an_ordinal() {
    use crate::storage::rocks::keys::IngestKey;
    let root = tempfile::tempdir().unwrap();
    let b = budget();
    let mut store = IngestStore::initialize(
        &root.path().join("db"),
        manifest(),
        native(),
        grant(&b, 0, 128 * MIB),
    )
    .unwrap();
    drop(
        store
            .bootstrap_dictionaries(
                request(),
                &intent("public"),
                ObservedTime::new(10),
                bootstrap_limits(),
                grant(&b, 4 * MIB, MIB),
                || Ok(()),
            )
            .unwrap(),
    );
    let key = IngestKey::AllocationStream {
        dataset_id: scope().dataset_id,
        dictionary_kind: DictionaryKind::VisibilityScope,
        allocation_domain: scope().allocation_domain,
    }
    .encode()
    .unwrap();
    let mut exhausted = vec![1, 3];
    exhausted.extend_from_slice(&u64::MAX.to_be_bytes());
    store.db.put(key.as_bytes(), exhausted).unwrap();
    let before = rows(&store);
    assert_eq!(
        store
            .stage_dictionary_append(
                next_request(2),
                &private_intent(),
                ObservedTime::new(20),
                limits(),
                &grant(&b, 4 * MIB, MIB)
            )
            .err(),
        Some(AdmissionError::Limit)
    );
    assert_eq!(rows(&store), before);
}
