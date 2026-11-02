//! Real native admission/replay controls using the independent A05 model literals.
use super::admission_tests::inputs;
use super::bootstrap_tests::{budget, grant, intent, manifest, native, request, scope};
use super::*;
use crate::runtime::ResourceBudget;
use crate::storage::rocks::{keys::IngestKey, manifest::AllocationNamespaces};
use bigbrotr_model::{
    codec::{
        FactDecodeLimits,
        request::{MutationIntent, RequestDecodeLimits, decode_request},
    },
    measurement::*,
    *,
};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, path::Path};

const MIB: u64 = 1024 * 1024;
type Rows = BTreeMap<(&'static str, Vec<u8>), Vec<u8>>;

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
fn recovery_limits() -> ProducerRecoveryLimits {
    ProducerRecoveryLimits {
        input: RequestDecodeLimits {
            values: FactDecodeLimits {
                max_encoded_bytes: 65536,
                max_allocation_bytes: 65536,
                max_tag_nodes: 1,
                max_string_bytes: 8192,
            },
            max_dictionaries: 16,
        },
        bootstrap: bootstrap_limits(),
        event: limits(),
    }
}
fn next_request(sequence: u64) -> ProducerRequestId {
    ProducerRequestId {
        request_sequence: RequestSequence::new(sequence).unwrap(),
        ..request()
    }
}
fn reference(kind: DictionaryKind, ordinal: u64) -> QualifiedRef {
    QualifiedRef {
        dictionary_kind: kind,
        ordinal: Ordinal::new(ordinal).unwrap(),
        ..scope()
    }
}
fn installed_manifest() -> IngestManifest {
    IngestManifest::new(
        *manifest().identity(),
        AllocationNamespaces::from_entries(&[
            (DictionaryKind::Endpoint, scope().allocation_domain),
            (DictionaryKind::Observer, scope().allocation_domain),
            (DictionaryKind::AccessScope, scope().allocation_domain),
            (DictionaryKind::VisibilityScope, scope().allocation_domain),
            (DictionaryKind::Method, scope().allocation_domain),
            (DictionaryKind::Route, scope().allocation_domain),
        ])
        .unwrap(),
    )
}
fn evidence(visibility: QualifiedRef, entries: Vec<DictionaryEntry>) -> EvidenceIntent {
    EvidenceIntent::new(
        1,
        visibility,
        entries,
        EvidenceIntentLimits {
            max_entries: 64,
            max_encoded_intent_bytes: 65536,
            max_validation_scratch_bytes: 8192,
        },
    )
    .unwrap()
}
fn entry(kind: DictionaryKind, ordinal: u64, descriptor: DictionaryDescriptor) -> DictionaryEntry {
    DictionaryEntry::new(reference(kind, ordinal), descriptor).unwrap()
}
fn dictionary_bundle(method: DictionaryDescriptor) -> EvidenceIntent {
    let observer = |ordinal| {
        entry(
            DictionaryKind::Observer,
            ordinal,
            DictionaryDescriptor::Observer(ObserverDescriptor {
                clock: Some(ClockSource::SystemUtc),
                vantage: Some(DescriptorLabel::new(format!("observer{ordinal}")).unwrap()),
                software: None,
                method_id: Some(reference(DictionaryKind::Method, 1)),
                public_key: None,
                route_class: Some(RouteClass::Public),
                software_version: None,
            }),
        )
    };
    evidence(
        scope(),
        vec![
            // The method precedes both observers' transitive dependency.
            entry(DictionaryKind::Method, 1, method),
            entry(
                DictionaryKind::Endpoint,
                1,
                DictionaryDescriptor::Endpoint(
                    EndpointDescriptor::new("wss://relay.example/".into()).unwrap(),
                ),
            ),
            entry(
                DictionaryKind::Route,
                1,
                DictionaryDescriptor::DirectRoute(DirectRouteDescriptor {
                    class: DirectRouteClass::Public,
                    label: DescriptorLabel::new("monitor-a".into()).unwrap(),
                }),
            ),
            entry(
                DictionaryKind::AccessScope,
                1,
                DictionaryDescriptor::AccessScope(AccessScopeDescriptor {
                    class: AccessClass::Public,
                    label: None,
                    visibility_scope_id: scope(),
                }),
            ),
            observer(1),
            observer(2),
        ],
    )
}
fn initialize(path: &Path, b: &ResourceBudget, method: DictionaryDescriptor) -> IngestStore {
    let mut store = super::bootstrap_tests::initialize(
        path,
        installed_manifest(),
        native(),
        grant(b, 0, 128 * MIB),
    )
    .unwrap();
    drop(
        store
            .bootstrap_dictionaries(
                request(),
                &intent("public"),
                ObservedTime::new(10),
                bootstrap_limits(),
                grant(b, 4 * MIB, MIB),
                || Ok(()),
            )
            .unwrap(),
    );
    drop(
        store
            .append_dictionaries(
                next_request(2),
                &dictionary_bundle(method),
                ObservedTime::new(20),
                limits(),
                operation_permit(b),
                || Ok(()),
            )
            .unwrap(),
    );
    assert_source(&store, 7);
    assert_producer(&store, 2, false);
    store
}
fn operation_permit(b: &ResourceBudget) -> OwnedPermit {
    grant(
        b,
        limits().required_dictionary_data_bytes().unwrap(),
        limits().required_native_bytes().unwrap(),
    )
}
fn measurement(value: Measurement) -> MeasurementEvidenceIntent {
    MeasurementEvidenceIntent::new(1, scope(), value, 65536).unwrap()
}
fn bytes(text: &str) -> Vec<u8> {
    assert_eq!(text.len() % 2, 0);
    text.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}
fn literal(name: &str) -> Measurement {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../crates/bigbrotr-model/tests/fixtures/measurement-v1.json"
    ))
    .unwrap();
    let row = fixture["valid"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["name"] == name)
        .unwrap();
    let input = bytes(row["preimage_hex"].as_str().unwrap());
    let digest = row["sha256"].as_str().unwrap().parse().unwrap();
    assert_eq!(Digest256::from_bytes(Sha256::digest(&input).into()), digest);
    let fact = Fact::decode_preimage(&input, digest, FactDecodeLimits::default()).unwrap();
    let FactPayload::Measurement(value) = fact.payload() else {
        panic!("not an A05 literal")
    };
    assert_eq!(fact.visibility_scope_id(), scope());
    value.clone()
}
fn with_state(value: &Measurement, state: MeasurementState) -> Measurement {
    Measurement::new(
        value.attempt_id(),
        value.context(),
        value.profile(),
        value.clock_quality(),
        state,
    )
    .unwrap()
}
fn resolution(name: &str, prior: FactRef) -> MeasurementEvidenceIntent {
    let value = literal(name);
    let MeasurementState::Resolution { terminal, .. } = value.state() else {
        panic!("not a resolution")
    };
    // Only replace the owner's actual strong target; scientific literal fields stay intact.
    measurement(with_state(
        &value,
        MeasurementState::Resolution {
            prior_unknown: prior,
            terminal,
        },
    ))
}
fn append(
    store: &mut IngestStore,
    b: &ResourceBudget,
    sequence: u64,
    value: &MeasurementEvidenceIntent,
    recorded_at: i64,
) -> Result<Receipt, AdmissionError> {
    store
        .append_measurement(
            next_request(sequence),
            value,
            ObservedTime::new(recorded_at),
            limits(),
            operation_permit(b),
            None,
            || Ok(()),
        )
        .map(|stored| stored.receipt().clone())
}
fn rows(store: &IngestStore) -> Rows {
    let mut result = BTreeMap::new();
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
#[derive(Debug, PartialEq, Eq)]
struct Snapshot {
    rows: Rows,
    native_sequence: u64,
    inputs: Vec<(String, Vec<u8>)>,
    accounting: PendingAccounting,
    quota: PendingQuotaStatus,
    reserved: Result<u64, PendingQuotaError>,
}
fn snapshot(store: &IngestStore, path: &Path) -> Snapshot {
    Snapshot {
        rows: rows(store),
        native_sequence: store.db.latest_sequence_number(),
        inputs: inputs(path),
        accounting: store.accounting,
        quota: store.pending_quota_status(),
        reserved: store.pending_reserved_bytes(),
    }
}
fn raw(store: &IngestStore, key: IngestKey) -> Option<Vec<u8>> {
    let key = key.encode().unwrap();
    store
        .db
        .get_cf(
            store.db.cf_handle(key.column_family().name()).unwrap(),
            key.as_bytes(),
        )
        .unwrap()
}
fn put_raw(store: &IngestStore, key: IngestKey, value: Option<&[u8]>) {
    let key = key.encode().unwrap();
    let cf = store.db.cf_handle(key.column_family().name()).unwrap();
    if let Some(value) = value {
        store.db.put_cf(cf, key.as_bytes(), value).unwrap();
    } else {
        store.db.delete_cf(cf, key.as_bytes()).unwrap();
    }
}
fn source(sequence: u64) -> SourceRecordId {
    SourceRecordId {
        dataset_id: scope().dataset_id,
        shard_id: 0,
        owner_epoch: OwnerEpoch::new(1).unwrap(),
        sequence: Sequence::new(sequence).unwrap(),
    }
}
fn fact(store: &IngestStore, sequence: u64) -> Fact {
    let raw = raw(
        store,
        IngestKey::Fact {
            source_record_id: source(sequence),
        },
    )
    .unwrap();
    assert_eq!(&raw[..2], &[1, 0x20]);
    let digest = Digest256::from_bytes(Sha256::digest(&raw[34..]).into());
    assert_eq!(&raw[2..34], digest.as_bytes());
    let fact = Fact::decode_preimage(&raw[34..], digest, limits().facts()).unwrap();
    assert_eq!(fact.source_record_id(), source(sequence));
    fact
}
fn index(value: &MeasurementEvidenceIntent) -> IngestKey {
    let dataset_id = scope().dataset_id;
    let attempt_id = value.measurement().attempt_id();
    if value.measurement().prior_unknown().is_some() {
        IngestKey::MeasurementResolution {
            dataset_id,
            attempt_id,
        }
    } else {
        IngestKey::MeasurementAttempt {
            dataset_id,
            attempt_id,
        }
    }
}
fn assert_source(store: &IngestStore, through: u64) {
    let bytes = raw(
        store,
        IngestKey::SourceStream {
            dataset_id: scope().dataset_id,
            shard_id: 0,
            owner_epoch: OwnerEpoch::new(1).unwrap(),
        },
    )
    .unwrap();
    assert_eq!(&bytes[..2], &[1, 2]);
    assert_eq!(&bytes[2..], &through.to_be_bytes());
    assert_eq!(
        store.pending_accounting().unwrap().source_through(),
        through
    );
}
fn assert_producer(store: &IngestStore, issued: u64, closing: bool) {
    let bytes = raw(
        store,
        IngestKey::ProducerState {
            producer_slot_id: request().producer_slot_id,
            producer_epoch: request().producer_epoch,
        },
    )
    .unwrap();
    assert_eq!(&bytes[..2], if closing { &[2, 0x31] } else { &[1, 0x31] });
    assert_eq!(bytes.len(), if closing { 27 } else { 26 });
    assert_eq!(&bytes[2..10], &issued.to_be_bytes());
    assert_eq!(
        &bytes[10..26],
        &[0; 16],
        "neither admission nor recovery acknowledges/retires"
    );
    if closing {
        assert_eq!(bytes[26], 1);
    }
}
fn assert_fact(
    store: &IngestStore,
    value: &MeasurementEvidenceIntent,
    sequence: u64,
    time: i64,
) -> FactRef {
    let fact = fact(store, sequence);
    assert_eq!(fact.recorded_at(), ObservedTime::new(time));
    assert_eq!(fact.visibility_scope_id(), scope());
    assert_eq!(
        fact.payload(),
        &FactPayload::Measurement(value.measurement().clone())
    );
    let bytes = raw(store, index(value)).unwrap();
    assert_eq!(bytes.len(), 70);
    assert_eq!(
        &bytes[..2],
        if value.measurement().prior_unknown().is_some() {
            &[1, 0x0d]
        } else {
            &[1, 0x0c]
        }
    );
    // Inspect the fixed source fields directly, independently of the index encoder.
    assert_eq!(&bytes[2..18], &[1; 16]);
    assert_eq!(&bytes[18..22], &0u32.to_be_bytes());
    assert_eq!(&bytes[22..30], &1u64.to_be_bytes());
    assert_eq!(&bytes[30..38], &sequence.to_be_bytes());
    assert_eq!(&bytes[38..], fact.payload_digest().as_bytes());
    fact.fact_ref()
}
fn assert_closure(store: &IngestStore, receipt: &Receipt, through: u64) {
    let expected: Vec<_> = (1..=through)
        .map(|sequence| fact(store, sequence).fact_ref())
        .collect();
    assert_eq!(receipt.dependencies().records(), expected);
    let context = receipt.protection_context();
    let identity = *installed_manifest().identity();
    assert_eq!(context.dataset_id, identity.dataset_id);
    assert_eq!(context.topology_generation, identity.topology_generation);
    assert_eq!(context.recovery_generation, identity.recovery_generation);
    assert_eq!(context.policy_epoch, identity.policy_epoch);
    assert_eq!(context.protection_class, ProtectionClass::LocalSynced);
}
fn input_for(
    path: &Path,
    receipt: &Receipt,
    value: &MeasurementEvidenceIntent,
) -> (String, Vec<u8>) {
    let mut found = inputs(path).into_iter().filter(|(_, bytes)| {
        Digest256::from_bytes(Sha256::digest(bytes).into()) == receipt.request_digest()
    });
    let (name, bytes) = found
        .next()
        .expect("receipt must retain its exact original input");
    assert!(found.next().is_none());
    let (id, decoded) =
        decode_request(&bytes, receipt.request_digest(), recovery_limits().input).unwrap();
    assert_eq!(id, receipt.request_id());
    assert_eq!(
        decoded,
        MutationIntent::Measurement(measurement(value.measurement().clone()))
    );
    assert_eq!(bytes, value.encode_request_preimage(id, 65536).unwrap());
    (name, bytes)
}
fn assert_no_effects_refusal(
    store: &mut IngestStore,
    path: &Path,
    b: &ResourceBudget,
    sequence: u64,
    value: &MeasurementEvidenceIntent,
    expected: AdmissionError,
) {
    let before = snapshot(store, path);
    let permit = operation_permit(b);
    assert_eq!(
        store
            .stage_measurement_append(
                next_request(sequence),
                value,
                ObservedTime::new(900),
                limits(),
                &permit,
                None
            )
            .err(),
        Some(expected)
    );
    drop(permit);
    assert_eq!(
        snapshot(store, path),
        before,
        "staging a refused measurement had effects"
    );
    assert_eq!(append(store, b, sequence, value, 900).err(), Some(expected));
    assert_eq!(
        snapshot(store, path),
        before,
        "admitting a refused measurement had effects"
    );
}

#[test]
fn unknown_resolution_and_exact_reuse_preserve_original_receipts_across_reopen() {
    for (initial_name, resolution_name) in [
        ("execution-unknown", "resolve-execution-unknown"),
        (
            "completion-unknown-prefix-pending",
            "resolve-completion-unknown",
        ),
    ] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("db");
        let b = budget();
        let mut store = initialize(&path, &b, DictionaryDescriptor::PassiveWsRead);
        let initial = measurement(literal(initial_name));
        let first = append(&mut store, &b, 3, &initial, 30).unwrap();
        let prior = assert_fact(&store, &initial, 8, 30);
        assert_eq!(first.request_id(), next_request(3));
        assert_eq!(first.batch().unwrap().records(), &[prior]);
        assert_closure(&store, &first, 8);
        let initial_index = raw(&store, index(&initial)).unwrap();
        let initial_fact = raw(
            &store,
            IngestKey::Fact {
                source_record_id: source(8),
            },
        )
        .unwrap();
        let changed = measurement(literal("empty-eose-auth-utc-rollback"));
        assert_no_effects_refusal(
            &mut store,
            &path,
            &b,
            3,
            &changed,
            AdmissionError::RequestConflict,
        );
        assert_no_effects_refusal(
            &mut store,
            &path,
            &b,
            4,
            &changed,
            AdmissionError::EvidenceConflict,
        );
        let pending = store.pending_accounting().unwrap();
        let before_reuse = snapshot(&store, &path);
        let reused = append(&mut store, &b, 4, &initial, 40).unwrap();
        assert!(reused.batch().is_none());
        assert_eq!(reused.request_id(), next_request(4));
        assert_ne!(reused.request_digest(), first.request_digest());
        assert_closure(&store, &reused, 8);
        assert_source(&store, 8);
        assert_eq!(store.pending_accounting().unwrap(), pending);
        assert_producer(&store, 4, false);
        assert_eq!(inputs(&path).len(), before_reuse.inputs.len() + 1);
        assert!(store.db.latest_sequence_number() > before_reuse.native_sequence);
        let mut expected_rows = before_reuse.rows.clone();
        for key in [
            IngestKey::Request {
                request_id: next_request(4),
            },
            IngestKey::ProducerState {
                producer_slot_id: request().producer_slot_id,
                producer_epoch: request().producer_epoch,
            },
        ] {
            let encoded = key.encode().unwrap();
            expected_rows.insert(
                (encoded.column_family().name(), encoded.as_bytes().to_vec()),
                raw(&store, key).unwrap(),
            );
        }
        assert_eq!(
            rows(&store),
            expected_rows,
            "reuse changes only its receipt and issued cursor"
        );
        let resolved = resolution(resolution_name, prior);
        let third = append(&mut store, &b, 5, &resolved, 50).unwrap();
        let terminal = assert_fact(&store, &resolved, 9, 50);
        assert_eq!(third.batch().unwrap().records(), &[terminal]);
        assert_closure(&store, &third, 9);
        assert_eq!(resolved.measurement().prior_unknown(), Some(prior));
        assert_eq!(raw(&store, index(&initial)).unwrap(), initial_index);
        assert_eq!(
            raw(
                &store,
                IngestKey::Fact {
                    source_record_id: source(8)
                }
            )
            .unwrap(),
            initial_fact
        );
        let later_initial = append(&mut store, &b, 6, &initial, 60).unwrap();
        let later_resolution = append(&mut store, &b, 7, &resolved, 70).unwrap();
        assert!(later_initial.batch().is_none());
        assert!(later_resolution.batch().is_none());
        assert_closure(&store, &later_initial, 8);
        assert_closure(&store, &later_resolution, 9);
        assert_source(&store, 9);
        assert_producer(&store, 7, false);
        let originals = [
            (3, &initial, first),
            (4, &initial, reused),
            (5, &resolved, third),
            (6, &initial, later_initial),
            (7, &resolved, later_resolution),
        ];
        assert_eq!(inputs(&path).len(), 7);
        for (_, value, receipt) in &originals {
            input_for(&path, receipt, value);
        }
        // Reopen establishes a new native handle, not fresh admission authority.
        for reopen in [false, true] {
            if reopen {
                drop(store);
                store = IngestStore::reopen(
                    &path,
                    installed_manifest(),
                    native(),
                    grant(&b, 0, 128 * MIB),
                )
                .unwrap();
            }
            let before = snapshot(&store, &path);
            for (sequence, value, receipt) in &originals {
                let replay = append(&mut store, &b, *sequence, value, 999).unwrap();
                assert_eq!(replay, *receipt);
                assert_eq!(
                    replay.encode_persistent(65536).unwrap(),
                    receipt.encode_persistent(65536).unwrap()
                );
                assert_eq!(snapshot(&store, &path), before);
            }
        }
    }
}

#[test]
fn initial_terminal_remains_immutable_and_requests_keep_their_own_digest() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("db");
    let b = budget();
    let mut store = initialize(&path, &b, DictionaryDescriptor::PassiveWsRead);
    let terminal = measurement(literal("empty-eose-auth-utc-rollback"));
    let original = append(&mut store, &b, 3, &terminal, 30).unwrap();
    let target = assert_fact(&store, &terminal, 8, 30);
    assert_eq!(original.batch().unwrap().records(), &[target]);
    assert_closure(&store, &original, 8);
    assert!(
        raw(
            &store,
            IngestKey::MeasurementResolution {
                dataset_id: scope().dataset_id,
                attempt_id: terminal.measurement().attempt_id(),
            }
        )
        .is_none()
    );
    let unknown = measurement(literal("execution-unknown"));
    assert_no_effects_refusal(
        &mut store,
        &path,
        &b,
        3,
        &unknown,
        AdmissionError::RequestConflict,
    );
    assert_no_effects_refusal(
        &mut store,
        &path,
        &b,
        4,
        &unknown,
        AdmissionError::EvidenceConflict,
    );
    let changed_terminal = measurement(literal("event-before-timeout"));
    assert_no_effects_refusal(
        &mut store,
        &path,
        &b,
        4,
        &changed_terminal,
        AdmissionError::EvidenceConflict,
    );
    let repeated = append(&mut store, &b, 4, &terminal, 40).unwrap();
    assert!(repeated.batch().is_none());
    assert_ne!(repeated.request_digest(), original.request_digest());
    input_for(&path, &original, &terminal);
    input_for(&path, &repeated, &terminal);
    let bad_resolution = resolution("resolve-execution-unknown", target);
    assert_no_effects_refusal(
        &mut store,
        &path,
        &b,
        5,
        &bad_resolution,
        AdmissionError::EvidenceConflict,
    );
    assert_source(&store, 8);
    assert_producer(&store, 4, false);
}

#[test]
fn resolution_requires_exact_unknown_and_preserves_its_scientific_context() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("db");
    let b = budget();
    let mut store = initialize(&path, &b, DictionaryDescriptor::PassiveWsRead);
    let missing = measurement(literal("resolve-completion-unknown"));
    assert_no_effects_refusal(
        &mut store,
        &path,
        &b,
        3,
        &missing,
        AdmissionError::EvidenceConflict,
    );
    let unknown = measurement(literal("completion-unknown-prefix-pending"));
    drop(append(&mut store, &b, 3, &unknown, 30).unwrap());
    let prior = fact(&store, 8).fact_ref();
    let valid = resolution("resolve-completion-unknown", prior);
    let MeasurementState::Resolution { terminal, .. } = valid.measurement().state() else {
        panic!()
    };
    for altered in [
        FactRef {
            payload_digest: Digest256::from_bytes([0x55; 32]),
            ..prior
        },
        FactRef {
            source_record_id: source(7),
            ..prior
        },
    ] {
        let bad = measurement(with_state(
            valid.measurement(),
            MeasurementState::Resolution {
                prior_unknown: altered,
                terminal,
            },
        ));
        assert_no_effects_refusal(
            &mut store,
            &path,
            &b,
            4,
            &bad,
            AdmissionError::EvidenceConflict,
        );
    }
    for source_record_id in [
        SourceRecordId {
            shard_id: 1,
            ..source(8)
        },
        SourceRecordId {
            owner_epoch: OwnerEpoch::new(2).unwrap(),
            ..source(8)
        },
    ] {
        let bad = measurement(with_state(
            valid.measurement(),
            MeasurementState::Resolution {
                prior_unknown: FactRef {
                    source_record_id,
                    ..prior
                },
                terminal,
            },
        ));
        assert_no_effects_refusal(&mut store, &path, &b, 4, &bad, AdmissionError::InvalidInput);
    }
    let value = valid.measurement();
    let mut context = value.context();
    context.observer_id = reference(DictionaryKind::Observer, 2);
    let quality = value.clock_quality();
    let changed_clock = ClockQuality::new(
        reference(DictionaryKind::Observer, 1),
        quality.resolution_us(),
        quality.uncertainty_us(),
        quality.anomaly_flags(),
    )
    .unwrap();
    let mut changed_terminal = terminal;
    changed_terminal.observed.read.auth_observed = false;
    for changed in [
        Measurement::new(
            value.attempt_id(),
            context,
            value.profile(),
            quality,
            value.state(),
        )
        .unwrap(),
        Measurement::new(
            value.attempt_id(),
            value.context(),
            PassiveReadProfile::first(DurationUs::new(3_000_000)).unwrap(),
            quality,
            value.state(),
        )
        .unwrap(),
        Measurement::new(
            value.attempt_id(),
            value.context(),
            value.profile(),
            changed_clock,
            value.state(),
        )
        .unwrap(),
        with_state(
            value,
            MeasurementState::Resolution {
                prior_unknown: prior,
                terminal: changed_terminal,
            },
        ),
    ] {
        assert_no_effects_refusal(
            &mut store,
            &path,
            &b,
            4,
            &measurement(changed),
            AdmissionError::InvalidInput,
        );
    }
    let accepted = append(&mut store, &b, 4, &valid, 40).unwrap();
    assert_closure(&store, &accepted, 9);
    assert_source(&store, 9);
    // A resolution cannot be used as the named initial unknown either.
    let bad = resolution("resolve-completion-unknown", fact(&store, 9).fact_ref());
    assert_no_effects_refusal(
        &mut store,
        &path,
        &b,
        5,
        &bad,
        AdmissionError::EvidenceConflict,
    );
}

fn refuse_corruption(
    store: &mut IngestStore,
    path: &Path,
    b: &ResourceBudget,
    key: IngestKey,
    originals: &[(u64, &MeasurementEvidenceIntent)],
) {
    let original = raw(store, key).unwrap();
    let mut corrupt = original.clone();
    corrupt[0] ^= 0x80;
    for replacement in [None, Some(corrupt.as_slice())] {
        put_raw(store, key, replacement);
        let before = snapshot(store, path);
        for (sequence, value) in originals {
            assert_eq!(
                append(store, b, *sequence, value, 999).err(),
                Some(AdmissionError::StateMismatch),
                "{key:?}"
            );
            assert_eq!(snapshot(store, path), before, "refusal changed {key:?}");
        }
        put_raw(store, key, Some(&original));
    }
}

#[test]
fn replay_refuses_missing_corrupt_indexes_targets_and_the_receipts_own_exact_closure() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("db");
    let b = budget();
    let mut store = initialize(&path, &b, DictionaryDescriptor::PassiveWsRead);
    let initial = measurement(literal("execution-unknown"));
    let first = append(&mut store, &b, 3, &initial, 30).unwrap();
    let resolved = resolution("resolve-execution-unknown", fact(&store, 8).fact_ref());
    let second = append(&mut store, &b, 4, &resolved, 40).unwrap();
    let committed = rows(&store);
    let originals = [(3, &initial), (4, &resolved)];
    refuse_corruption(&mut store, &path, &b, index(&initial), &originals);
    for sequence in 1..=8 {
        refuse_corruption(
            &mut store,
            &path,
            &b,
            IngestKey::Fact {
                source_record_id: source(sequence),
            },
            &originals,
        );
    }
    for (kind, ordinal) in [
        (DictionaryKind::VisibilityScope, 1),
        (DictionaryKind::Method, 1),
        (DictionaryKind::Endpoint, 1),
        (DictionaryKind::Route, 1),
        (DictionaryKind::AccessScope, 1),
        (DictionaryKind::Observer, 1),
        (DictionaryKind::Observer, 2),
    ] {
        refuse_corruption(
            &mut store,
            &path,
            &b,
            IngestKey::Dictionary {
                reference: reference(kind, ordinal),
            },
            &originals,
        );
    }
    for key in [
        index(&resolved),
        IngestKey::Fact {
            source_record_id: source(9),
        },
    ] {
        refuse_corruption(&mut store, &path, &b, key, &[(4, &resolved)]);
        // A later resolution is not part of the original unknown's closure.
        let bytes = raw(&store, key).unwrap();
        put_raw(&store, key, None);
        let before = snapshot(&store, &path);
        assert_eq!(append(&mut store, &b, 3, &initial, 999).unwrap(), first);
        assert_eq!(snapshot(&store, &path), before);
        put_raw(&store, key, Some(&bytes));
    }
    for (sequence, value, receipt) in [(3, &initial, &first), (4, &resolved, &second)] {
        refuse_corruption(
            &mut store,
            &path,
            &b,
            IngestKey::Request {
                request_id: next_request(sequence),
            },
            &[(sequence, value)],
        );
        refuse_corruption(
            &mut store,
            &path,
            &b,
            IngestKey::PendingBatch {
                first: receipt.batch().unwrap().first(),
            },
            &[(sequence, value)],
        );
        let key = IngestKey::Request {
            request_id: next_request(sequence),
        };
        let bytes = raw(&store, key).unwrap();
        // These are deliberately forged, structurally valid receipts: omit the
        // separate clock fact, and (for resolution) the exact prior unknown.
        for omitted in if sequence == 3 { vec![7] } else { vec![7, 8] } {
            let closure = SourceDependencies::new(
                scope().dataset_id,
                receipt
                    .dependencies()
                    .records()
                    .iter()
                    .copied()
                    .filter(|r| r.source_record_id.sequence.get() != omitted)
                    .collect(),
            )
            .unwrap();
            let wrong = Receipt::new_facts(
                receipt.request_id(),
                receipt.request_digest(),
                *receipt.protection_context(),
                receipt.batch().unwrap().clone(),
                closure,
            )
            .unwrap();
            put_raw(&store, key, Some(&wrong.encode_persistent(65536).unwrap()));
            let before = snapshot(&store, &path);
            assert_eq!(
                append(&mut store, &b, sequence, value, 999).err(),
                Some(AdmissionError::StateMismatch)
            );
            assert_eq!(snapshot(&store, &path), before);
        }
        put_raw(&store, key, Some(&bytes));
    }
    assert_eq!(
        rows(&store),
        committed,
        "each raw corruption is restored exactly"
    );
    assert_eq!(append(&mut store, &b, 3, &initial, 999).unwrap(), first);
    assert_eq!(append(&mut store, &b, 4, &resolved, 999).unwrap(), second);
}

#[test]
fn measurement_rejects_wrong_method_legacy_route_and_access_visibility() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("wrong-method");
    let b = budget();
    let mut store = initialize(&path, &b, DictionaryDescriptor::EventVerification);
    let value = measurement(literal("execution-unknown"));
    assert_no_effects_refusal(
        &mut store,
        &path,
        &b,
        3,
        &value,
        AdmissionError::InvalidInput,
    );
    drop(store);
    let path = root.path().join("exact-types");
    let mut store = initialize(&path, &b, DictionaryDescriptor::PassiveWsRead);
    // The old observer route-class attribution cannot stand in for DirectRoute.
    let route = IngestKey::Dictionary {
        reference: reference(DictionaryKind::Route, 1),
    };
    let original = raw(&store, route).unwrap();
    let observer = raw(
        &store,
        IngestKey::Dictionary {
            reference: reference(DictionaryKind::Observer, 1),
        },
    )
    .unwrap();
    put_raw(&store, route, Some(&observer));
    assert_no_effects_refusal(
        &mut store,
        &path,
        &b,
        3,
        &value,
        AdmissionError::StateMismatch,
    );
    put_raw(&store, route, Some(&original));
    // Admit both private mappings legitimately; this negative is scope equality,
    // not a refusal caused by an absent or malformed dependency.
    let private = reference(DictionaryKind::VisibilityScope, 2);
    let private_visibility = evidence(
        private,
        vec![entry(
            DictionaryKind::VisibilityScope,
            2,
            DictionaryDescriptor::Visibility {
                class: DescriptorLabel::new("private".into()).unwrap(),
            },
        )],
    );
    drop(
        store
            .append_dictionaries(
                next_request(3),
                &private_visibility,
                ObservedTime::new(30),
                limits(),
                operation_permit(&b),
                || Ok(()),
            )
            .unwrap(),
    );
    let private_access = evidence(
        private,
        vec![entry(
            DictionaryKind::AccessScope,
            2,
            DictionaryDescriptor::AccessScope(AccessScopeDescriptor {
                class: AccessClass::Public,
                label: None,
                visibility_scope_id: private,
            }),
        )],
    );
    drop(
        store
            .append_dictionaries(
                next_request(4),
                &private_access,
                ObservedTime::new(40),
                limits(),
                operation_permit(&b),
                || Ok(()),
            )
            .unwrap(),
    );
    assert_source(&store, 9);
    let mut context = value.measurement().context();
    context.access_scope_id = reference(DictionaryKind::AccessScope, 2);
    let changed = measurement(
        Measurement::new(
            value.measurement().attempt_id(),
            context,
            value.measurement().profile(),
            value.measurement().clock_quality(),
            value.measurement().state(),
        )
        .unwrap(),
    );
    assert_no_effects_refusal(
        &mut store,
        &path,
        &b,
        5,
        &changed,
        AdmissionError::InvalidInput,
    );
    // Public access remains usable after all refusals and raw fixture restoration.
    let accepted = append(&mut store, &b, 5, &value, 50).unwrap();
    assert_eq!(accepted.batch().unwrap().first(), source(10));
    assert_producer(&store, 5, false);
}

#[test]
fn measurement_preflight_resource_and_authority_refusals_have_no_effects() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("db");
    let b = budget();
    let mut store = initialize(&path, &b, DictionaryDescriptor::PassiveWsRead);
    let value = measurement(literal("execution-unknown"));
    let before = snapshot(&store, &path);
    let data = limits().required_dictionary_data_bytes().unwrap();
    let native = limits().required_native_bytes().unwrap();
    for (data, native) in [(data - 1, native), (data, native - 1)] {
        let permit = grant(&b, data, native);
        assert_eq!(
            store
                .measurement_pending_reservation(
                    next_request(3),
                    &value,
                    ObservedTime::new(30),
                    limits(),
                    &permit
                )
                .err(),
            Some(AdmissionError::InsufficientReservation)
        );
        assert_eq!(
            store
                .stage_measurement_append(
                    next_request(3),
                    &value,
                    ObservedTime::new(30),
                    limits(),
                    &permit,
                    None
                )
                .err(),
            Some(AdmissionError::InsufficientReservation)
        );
        let mut authority = 0;
        assert_eq!(
            store
                .append_measurement(
                    next_request(3),
                    &value,
                    ObservedTime::new(30),
                    limits(),
                    permit,
                    None,
                    || {
                        authority += 1;
                        Ok(())
                    }
                )
                .err(),
            Some(AdmissionError::InsufficientReservation)
        );
        assert_eq!(authority, 0);
        assert_eq!(snapshot(&store, &path), before);
    }
    for denied_at in [1, 2] {
        let mut authority = 0;
        assert_eq!(
            store
                .append_measurement(
                    next_request(3),
                    &value,
                    ObservedTime::new(30),
                    limits(),
                    operation_permit(&b),
                    None,
                    || {
                        authority += 1;
                        if authority == denied_at {
                            Err(AdmissionError::AuthorityUnavailable)
                        } else {
                            Ok(())
                        }
                    }
                )
                .err(),
            Some(AdmissionError::AuthorityUnavailable)
        );
        assert_eq!(authority, denied_at);
        assert_eq!(snapshot(&store, &path), before);
    }
    assert_eq!(
        b.snapshot().used().data_bytes(),
        PENDING_QUOTA_OWNER_DATA_BYTES
    );
    assert_eq!(b.snapshot().used().native_bytes(), 128 * MIB);
    let accepted = append(&mut store, &b, 3, &value, 30).unwrap();
    assert_eq!(accepted.batch().unwrap().first(), source(8));
    let before = snapshot(&store, &path);
    let mut authority = 0;
    assert_eq!(
        store
            .append_measurement(
                next_request(3),
                &value,
                ObservedTime::new(999),
                limits(),
                operation_permit(&b),
                None,
                || {
                    authority += 1;
                    if authority == 2 {
                        Err(AdmissionError::AuthorityUnavailable)
                    } else {
                        Ok(())
                    }
                }
            )
            .err(),
        Some(AdmissionError::AuthorityUnavailable)
    );
    assert_eq!(authority, 2);
    assert_eq!(snapshot(&store, &path), before);
}

#[test]
fn final_authority_fence_retains_only_staged_input_not_a_measurement_outcome() {
    // Closing is irreversible. Use separate real owners for same-original retry
    // and closing recovery of an input that never acquired an issued outcome.
    for close in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("db");
        let b = budget();
        let mut store = initialize(&path, &b, DictionaryDescriptor::PassiveWsRead);
        let value = measurement(literal("execution-unknown"));
        let before = snapshot(&store, &path);
        assert_eq!(before.reserved, Ok(0));
        let permit = operation_permit(&b);
        let terms = store
            .measurement_pending_reservation(
                next_request(3),
                &value,
                ObservedTime::new(30),
                limits(),
                &permit,
            )
            .unwrap();
        drop(permit);
        assert_eq!(terms.request, next_request(3));
        assert_eq!(terms.allowance_bytes, limits().max_batch_bytes as u64);
        assert_eq!(snapshot(&store, &path), before);
        let mut authority = 0;
        assert_eq!(
            store
                .append_measurement(
                    next_request(3),
                    &value,
                    ObservedTime::new(30),
                    limits(),
                    operation_permit(&b),
                    None,
                    || {
                        authority += 1;
                        if authority == 3 {
                            Err(AdmissionError::AuthorityUnavailable)
                        } else {
                            Ok(())
                        }
                    },
                )
                .err(),
            Some(AdmissionError::AuthorityUnavailable)
        );
        assert_eq!(authority, 3, "the final pre-write fence must be reached");
        assert!(!store.write_uncertain);
        assert!(!store.native_unavailable());
        let staged = snapshot(&store, &path);
        assert_eq!(staged.rows, before.rows);
        assert_eq!(staged.native_sequence, before.native_sequence);
        assert_eq!(staged.accounting, before.accounting);
        assert_eq!(staged.quota, before.quota);
        assert_eq!(staged.reserved, Ok(terms.allowance_bytes));
        assert_source(&store, 7);
        assert_producer(&store, 2, false);
        for key in [
            IngestKey::Request {
                request_id: next_request(3),
            },
            index(&value),
            IngestKey::Fact {
                source_record_id: source(8),
            },
            IngestKey::PendingBatch { first: source(8) },
        ] {
            assert!(raw(&store, key).is_none(), "staging emitted {key:?}");
        }
        assert_eq!(staged.inputs.len(), before.inputs.len() + 1);
        assert!(
            before
                .inputs
                .iter()
                .all(|input| staged.inputs.contains(input))
        );
        let added: Vec<_> = staged
            .inputs
            .iter()
            .filter(|input| !before.inputs.contains(input))
            .collect();
        assert_eq!(added.len(), 1);
        let (_, encoded) = added[0];
        assert_eq!(
            encoded,
            &value
                .encode_request_preimage(next_request(3), 65536)
                .unwrap()
        );
        let digest = Digest256::from_bytes(Sha256::digest(encoded).into());
        assert_eq!(digest, terms.digest);
        assert_eq!(store.staged_pending_digest(next_request(3)), Ok(digest));
        let (id, decoded) = decode_request(encoded, digest, recovery_limits().input).unwrap();
        assert_eq!(id, next_request(3));
        assert_eq!(
            decoded,
            MutationIntent::Measurement(measurement(value.measurement().clone()))
        );
        drop(decoded);
        assert_eq!(
            b.snapshot().used().data_bytes(),
            PENDING_QUOTA_OWNER_DATA_BYTES
        );
        assert_eq!(b.snapshot().used().native_bytes(), 128 * MIB);
        if close {
            drop(store);
            let mut recovery = IngestRecovery::open(
                &path,
                installed_manifest(),
                native(),
                grant(&b, 0, 128 * MIB),
            )
            .unwrap();
            let closing = recovery
                .close_producer(
                    request().producer_slot_id,
                    request().producer_epoch,
                    grant(&b, 4096, 4096),
                    || Ok(()),
                )
                .unwrap();
            assert_eq!(closing.issued_through(), 2);
            assert_eq!(closing.acknowledged_through(), 0);
            drop(closing);
            assert_source(&recovery.store, 7);
            assert_producer(&recovery.store, 2, true);
            assert_eq!(inputs(&path), staged.inputs);
            let closed = snapshot(&recovery.store, &path);
            // A valid typed original survives reopen/closing, yet is neither a
            // durable Measurement nor a trusted execution/start observation.
            let retained = recovery
                .store
                .read_request_input(
                    next_request(3),
                    digest,
                    recovery_limits().input,
                    grant(&b, MIB, 0),
                )
                .unwrap();
            assert_eq!(retained.encoded_bytes() as usize, encoded.len());
            let (id, decoded, permit) = retained.into_parts();
            assert_eq!(id, next_request(3));
            assert_eq!(
                decoded,
                MutationIntent::Measurement(measurement(value.measurement().clone()))
            );
            drop(decoded);
            drop(permit);
            let recovery_limits = recovery_limits();
            assert_eq!(
                recovery
                    .resolve_original_request(
                        next_request(3),
                        digest,
                        recovery_limits,
                        grant(
                            &b,
                            RetainedRequestInput::required_data_bytes(recovery_limits.input)
                                .unwrap(),
                            0
                        ),
                        grant(&b, recovery_limits.required_data_bytes().unwrap(), 0),
                        || Ok(()),
                    )
                    .err(),
                Some(ProducerRecoveryError::Unresolved)
            );
            assert_eq!(snapshot(&recovery.store, &path), closed);
        } else {
            let permit = operation_permit(&b);
            assert_eq!(
                store
                    .measurement_pending_reservation(
                        next_request(3),
                        &value,
                        ObservedTime::new(30),
                        limits(),
                        &permit
                    )
                    .unwrap(),
                terms,
                "same original retains its existing credit terms"
            );
            drop(permit);
            assert_eq!(snapshot(&store, &path), staged);
            let receipt = append(&mut store, &b, 3, &value, 30).unwrap();
            let reference = assert_fact(&store, &value, 8, 30);
            assert_eq!(receipt.batch().unwrap().records(), &[reference]);
            assert_closure(&store, &receipt, 8);
            assert_eq!(receipt.request_digest(), digest);
            assert_eq!(
                inputs(&path),
                staged.inputs,
                "retry reuses the synchronized original"
            );
            assert_eq!(store.pending_reserved_bytes(), Ok(0));
            assert_source(&store, 8);
            assert_producer(&store, 3, false);
            assert!(
                store.pending_accounting().unwrap().pending_bytes()
                    > before.accounting.pending_bytes()
            );
        }
    }
}

fn resolve(
    recovery: &IngestRecovery,
    b: &ResourceBudget,
    receipt: &Receipt,
) -> Result<ResolvedProducerRequest, ProducerRecoveryError> {
    let limits = recovery_limits();
    recovery.resolve_original_request(
        receipt.request_id(),
        receipt.request_digest(),
        limits,
        grant(
            b,
            RetainedRequestInput::required_data_bytes(limits.input).unwrap(),
            0,
        ),
        grant(b, limits.required_data_bytes().unwrap(), 0),
        || Ok(()),
    )
}

#[test]
fn closed_producer_resolves_typed_measurement_originals_without_event_authority() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("db");
    let b = budget();
    let mut store = initialize(&path, &b, DictionaryDescriptor::PassiveWsRead);
    let initial = measurement(literal("execution-unknown"));
    let first = append(&mut store, &b, 3, &initial, 30).unwrap();
    let reused = append(&mut store, &b, 4, &initial, 40).unwrap();
    let resolved = resolution("resolve-execution-unknown", fact(&store, 8).fact_ref());
    let terminal = append(&mut store, &b, 5, &resolved, 50).unwrap();
    let originals = [(&initial, first), (&initial, reused), (&resolved, terminal)];
    drop(store);
    let mut recovery = IngestRecovery::open(
        &path,
        installed_manifest(),
        native(),
        grant(&b, 0, 128 * MIB),
    )
    .unwrap();
    let closing = recovery
        .close_producer(
            request().producer_slot_id,
            request().producer_epoch,
            grant(&b, 4096, 4096),
            || Ok(()),
        )
        .unwrap();
    assert_eq!(closing.issued_through(), 5);
    assert_eq!(closing.acknowledged_through(), 0);
    drop(closing);
    assert_producer(&recovery.store, 5, true);
    let before = snapshot(&recovery.store, &path);
    for (value, receipt) in &originals {
        let (_, encoded) = input_for(&path, receipt, value);
        let retained = recovery
            .store
            .read_request_input(
                receipt.request_id(),
                receipt.request_digest(),
                recovery_limits().input,
                grant(&b, MIB, 0),
            )
            .unwrap();
        let (id, decoded, permit) = retained.into_parts();
        assert_eq!(id, receipt.request_id());
        let MutationIntent::Measurement(decoded) = decoded else {
            panic!("measurement was recovered as another operation")
        };
        assert_eq!(decoded, measurement(value.measurement().clone()));
        drop(decoded);
        drop(permit);
        let result = resolve(&recovery, &b, receipt).unwrap();
        assert!(
            !result.is_event(),
            "Measurement cannot authorize event checkpoint advancement"
        );
        assert_eq!(result.input_bytes() as usize, encoded.len());
        let witness = recovery.measurement_checkpoint(&result).unwrap();
        let basis = witness.basis();
        assert_eq!(
            basis.kind(),
            super::producer_checkpoint::CheckpointKind::InternalEvidence
        );
        assert_eq!(basis.request(), Some(receipt.request_id()));
        assert_eq!(basis.intent_digest(), receipt.request_digest());
        assert_eq!(basis.dependencies(), receipt.dependencies().records());
        witness.require().unwrap();
        assert!(
            recovery.store.require_checkpoint_basis(basis).is_err(),
            "historical recovery evidence cannot enter the live checkpoint/ACK authority path"
        );
        assert_eq!(snapshot(&recovery.store, &path), before);
        let AdmissionOutcome::Durable(actual) = result.outcome() else {
            panic!("not durable")
        };
        assert_eq!(actual, receipt);
        assert_eq!(
            actual.encode_persistent(65536).unwrap(),
            receipt.encode_persistent(65536).unwrap()
        );
        drop(witness);
        drop(result);
        assert_eq!(snapshot(&recovery.store, &path), before);
    }
    assert_eq!(
        append(&mut recovery.store, &b, 6, &initial, 60).err(),
        Some(AdmissionError::ProducerClosing)
    );
    assert_eq!(snapshot(&recovery.store, &path), before);
    let (value, receipt) = &originals[2];
    let key = IngestKey::Request {
        request_id: receipt.request_id(),
    };
    let bytes = raw(&recovery.store, key).unwrap();
    put_raw(&recovery.store, key, None);
    let missing = snapshot(&recovery.store, &path);
    assert_eq!(
        resolve(&recovery, &b, receipt).err(),
        Some(ProducerRecoveryError::Unresolved)
    );
    assert_eq!(snapshot(&recovery.store, &path), missing);
    put_raw(&recovery.store, key, Some(&bytes));
    let (name, bytes) = input_for(&path, receipt, value);
    let original_path = path.join("producer-inputs-v1").join(name);
    let held_path = root.path().join("held-original");
    std::fs::rename(&original_path, &held_path).unwrap();
    let missing = snapshot(&recovery.store, &path);
    assert_eq!(
        resolve(&recovery, &b, receipt).err(),
        Some(ProducerRecoveryError::Input(RequestInputError::Missing))
    );
    assert_eq!(snapshot(&recovery.store, &path), missing);
    std::fs::rename(&held_path, &original_path).unwrap();
    let mut corrupted = bytes.clone();
    *corrupted.last_mut().unwrap() ^= 1;
    std::fs::write(&original_path, &corrupted).unwrap();
    let corrupt = snapshot(&recovery.store, &path);
    assert!(matches!(
        resolve(&recovery, &b, receipt),
        Err(ProducerRecoveryError::Input(RequestInputError::Codec(_)))
    ));
    assert_eq!(snapshot(&recovery.store, &path), corrupt);
    std::fs::write(&original_path, &bytes).unwrap();
    let target_key = index(&initial);
    let target = raw(&recovery.store, target_key).unwrap();
    put_raw(&recovery.store, target_key, None);
    let missing = snapshot(&recovery.store, &path);
    assert_eq!(
        resolve(&recovery, &b, receipt).err(),
        Some(ProducerRecoveryError::Native(AdmissionError::StateMismatch))
    );
    assert_eq!(snapshot(&recovery.store, &path), missing);
    put_raw(&recovery.store, target_key, Some(&target));
    // Raw fixture restoration changes RocksDB sequence numbers; retained logical
    // rows and the original byte spool must still be exact, then resolve again.
    assert_eq!(rows(&recovery.store), before.rows);
    assert_eq!(inputs(&path), before.inputs);
    let after_restore = snapshot(&recovery.store, &path);
    let result = resolve(&recovery, &b, receipt).unwrap();
    assert!(!result.is_event());
    assert_eq!(
        result.outcome(),
        &AdmissionOutcome::Durable(receipt.clone())
    );
    drop(result);
    assert_eq!(snapshot(&recovery.store, &path), after_restore);
}

#[test]
fn historical_measurement_checkpoint_refuses_dictionary_foreign_root_and_stale_replay() {
    let root = tempfile::tempdir().unwrap();
    let b = budget();
    let other_budget = budget();
    let value = measurement(literal("empty-eose-auth-utc-rollback"));
    let mut recoveries = Vec::new();
    let mut receipts = Vec::new();
    for (name, budget) in [("first", &b), ("other", &other_budget)] {
        let path = root.path().join(name);
        let mut store = initialize(&path, budget, DictionaryDescriptor::PassiveWsRead);
        receipts.push(append(&mut store, budget, 3, &value, 30).unwrap());
        drop(store);
        let mut recovery = IngestRecovery::open(
            &path,
            installed_manifest(),
            native(),
            grant(budget, 0, 128 * MIB),
        )
        .unwrap();
        drop(
            recovery
                .close_producer(
                    request().producer_slot_id,
                    request().producer_epoch,
                    grant(budget, 4096, 4096),
                    || Ok(()),
                )
                .unwrap(),
        );
        recoveries.push(recovery);
    }
    assert_eq!(receipts[0], receipts[1]);
    let first = resolve(&recoveries[0], &b, &receipts[0]).unwrap();
    let foreign = resolve(&recoveries[1], &other_budget, &receipts[1]).unwrap();
    assert!(matches!(
        recoveries[0].measurement_checkpoint(&foreign),
        Err(AdmissionError::StateMismatch)
    ));
    let before = snapshot(&recoveries[0].store, &root.path().join("first"));
    let witness = recoveries[0].measurement_checkpoint(&first).unwrap();
    assert_eq!(
        recoveries[0]
            .store
            .producer_state(request().producer_slot_id, request().producer_epoch)
            .unwrap()
            .unwrap()
            .acknowledged,
        0
    );
    assert_eq!(
        snapshot(&recoveries[0].store, &root.path().join("first")),
        before
    );
    let key = IngestKey::Request {
        request_id: receipts[0].request_id(),
    };
    let original = raw(&recoveries[0].store, key).unwrap();
    put_raw(&recoveries[0].store, key, Some(&original));
    assert_eq!(witness.require(), Err(AdmissionError::StateMismatch));
    assert!(matches!(
        recoveries[0].measurement_checkpoint(&first),
        Err(AdmissionError::StateMismatch)
    ));
    drop(witness);
    drop(first);
    drop(foreign);
    let dictionary = recoveries[0]
        .resolve_original_request(
            request(),
            intent("public").request_digest(request()),
            recovery_limits(),
            grant(
                &b,
                RetainedRequestInput::required_data_bytes(recovery_limits().input).unwrap(),
                0,
            ),
            grant(&b, recovery_limits().required_data_bytes().unwrap(), 0),
            || Ok(()),
        )
        .unwrap();
    assert!(!dictionary.is_event());
    assert!(dictionary.measurement().is_none());
    assert!(matches!(
        recoveries[0].measurement_checkpoint(&dictionary),
        Err(AdmissionError::InvalidInput)
    ));
}

#[test]
fn measurement_checkpoint_witness_acknowledges_exact_result_without_reclaiming_detail() {
    // Native mechanics only: test_acknowledge_target does not fabricate a PG
    // confirmation. The installed oracle separately requires the actual C/ACK.
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("db");
    let b = budget();
    let mut store = initialize(&path, &b, DictionaryDescriptor::PassiveWsRead);
    let value = measurement(literal("empty-eose-auth-utc-rollback"));
    let result = store
        .append_checkpoint_measurement(
            next_request(3),
            &value,
            ObservedTime::new(30),
            limits(),
            operation_permit(&b),
            || Ok(()),
        )
        .unwrap();
    let bytes = result.receipt().encode_persistent(65536).unwrap();
    let basis = store.checkpoint_evidence(&result).unwrap();
    assert_eq!(
        basis.kind(),
        super::producer_checkpoint::CheckpointKind::InternalEvidence
    );
    assert_eq!(basis.request(), Some(next_request(3)));
    assert_eq!(basis.intent_digest(), value.request_digest(next_request(3)));
    assert_eq!(basis.dependencies().len(), 8);
    assert_eq!(
        basis.dependencies().last().unwrap().source_record_id,
        source(8)
    );
    assert!(matches!(
        basis.acknowledgement_target(request().producer_slot_id, request().producer_epoch, 2),
        Err(AdmissionError::InvalidInput)
    ));
    let target = basis
        .acknowledgement_target(request().producer_slot_id, request().producer_epoch, 3)
        .unwrap();
    let before = snapshot(&store, &path);
    store
        .test_acknowledge_target(target, &grant(&b, 4096, 4096), || Ok(()))
        .unwrap();
    let credit = store.producer_credit(request().producer_slot_id).unwrap();
    assert_eq!(
        (
            credit.issued_through(),
            credit.acknowledged_through(),
            credit.retired_through()
        ),
        (3, 3, 0)
    );
    let after = snapshot(&store, &path);
    assert_eq!(after.inputs, before.inputs);
    assert_eq!(after.accounting, before.accounting);
    let key = IngestKey::ProducerState {
        producer_slot_id: request().producer_slot_id,
        producer_epoch: request().producer_epoch,
    }
    .encode()
    .unwrap();
    let without_head = |mut rows: Rows| {
        rows.remove(&("receipts", key.as_bytes().to_vec()));
        rows
    };
    assert_eq!(without_head(after.rows), without_head(before.rows));
    drop(result);
    let before = snapshot(&store, &path);
    let replay = store
        .append_checkpoint_measurement(
            next_request(3),
            &value,
            ObservedTime::new(999),
            limits(),
            operation_permit(&b),
            || Ok(()),
        )
        .unwrap();
    assert_eq!(replay.receipt().encode_persistent(65536).unwrap(), bytes);
    let basis = store.checkpoint_evidence(&replay).unwrap();
    let target = basis
        .acknowledgement_target(request().producer_slot_id, request().producer_epoch, 3)
        .unwrap();
    store
        .test_acknowledge_target(target, &grant(&b, 4096, 4096), || Ok(()))
        .unwrap();
    assert_eq!(snapshot(&store, &path), before);
}

#[test]
fn measurement_checkpoint_rejects_ordinary_receipts_other_roots_and_stale_native_heads() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("db");
    let b = budget();
    let mut store = initialize(&path, &b, DictionaryDescriptor::PassiveWsRead);
    let value = measurement(literal("empty-eose-auth-utc-rollback"));
    let ordinary = store
        .append_measurement(
            next_request(3),
            &value,
            ObservedTime::new(30),
            limits(),
            operation_permit(&b),
            None,
            || Ok(()),
        )
        .unwrap();
    assert!(matches!(
        store.checkpoint_evidence(&ordinary),
        Err(AdmissionError::InvalidInput)
    ));
    drop(ordinary);
    let before = snapshot(&store, &path);
    let changed = measurement(literal("event-before-timeout"));
    for (request, intent, expected) in [
        (next_request(3), &changed, AdmissionError::RequestConflict),
        (next_request(5), &value, AdmissionError::RequestGap),
        (
            ProducerRequestId {
                producer_epoch: ProducerEpoch::new(2).unwrap(),
                ..next_request(3)
            },
            &value,
            AdmissionError::InvalidInput,
        ),
    ] {
        assert_eq!(
            store
                .append_checkpoint_measurement(
                    request,
                    intent,
                    ObservedTime::new(999),
                    limits(),
                    operation_permit(&b),
                    || Ok(())
                )
                .err(),
            Some(expected)
        );
        assert_eq!(snapshot(&store, &path), before);
    }
    let result = store
        .append_checkpoint_measurement(
            next_request(3),
            &value,
            ObservedTime::new(30),
            limits(),
            operation_permit(&b),
            || Ok(()),
        )
        .unwrap();
    let other_root = tempfile::tempdir().unwrap();
    let other_b = budget();
    let mut other = initialize(
        &other_root.path().join("db"),
        &other_b,
        DictionaryDescriptor::PassiveWsRead,
    );
    let other_result = other
        .append_checkpoint_measurement(
            next_request(3),
            &value,
            ObservedTime::new(30),
            limits(),
            operation_permit(&other_b),
            || Ok(()),
        )
        .unwrap();
    assert_eq!(
        result.receipt().encode_persistent(65536).unwrap(),
        other_result.receipt().encode_persistent(65536).unwrap()
    );
    assert!(matches!(
        store.checkpoint_evidence(&other_result),
        Err(AdmissionError::StateMismatch)
    ));
    let basis = store.checkpoint_evidence(&result).unwrap();
    for (slot, epoch) in [
        ([99; 16].try_into().unwrap(), request().producer_epoch),
        (request().producer_slot_id, ProducerEpoch::new(2).unwrap()),
    ] {
        assert!(matches!(
            basis.acknowledgement_target(slot, epoch, 3),
            Err(AdmissionError::InvalidInput)
        ));
    }
    let target = basis
        .acknowledgement_target(request().producer_slot_id, request().producer_epoch, 4)
        .unwrap();
    let before = snapshot(&store, &path);
    assert_eq!(
        store.test_acknowledge_target(target, &grant(&b, 4096, 4096), || Ok(())),
        Err(AdmissionError::StateMismatch)
    );
    assert_eq!(snapshot(&store, &path), before);
    // A later genuine request changes the native stamp even without new science.
    drop(append(&mut store, &b, 4, &value, 40).unwrap());
    assert_eq!(
        store.require_checkpoint_basis(&basis),
        Err(AdmissionError::StateMismatch)
    );
    assert!(matches!(
        store.checkpoint_evidence(&result),
        Err(AdmissionError::StateMismatch)
    ));
}

#[test]
fn measurement_checkpoint_refuses_lost_authority_native_uncertainty_and_short_capacity() {
    for latch in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("db");
        let b = budget();
        let mut store = initialize(&path, &b, DictionaryDescriptor::PassiveWsRead);
        let value = measurement(literal("empty-eose-auth-utc-rollback"));
        let before = snapshot(&store, &path);
        assert_eq!(
            store
                .append_checkpoint_measurement(
                    next_request(3),
                    &value,
                    ObservedTime::new(30),
                    limits(),
                    grant(&b, 1, 1),
                    || Ok(())
                )
                .err(),
            Some(AdmissionError::InsufficientReservation)
        );
        assert_eq!(snapshot(&store, &path), before);
        drop(append(&mut store, &b, 3, &value, 30).unwrap());
        let before = snapshot(&store, &path);
        let failure = store.test_background_failure();
        let mut checks = 0;
        let result = store.append_checkpoint_measurement(
            next_request(3),
            &value,
            ObservedTime::new(999),
            limits(),
            operation_permit(&b),
            || {
                checks += 1;
                if checks == 3 {
                    if latch {
                        failure.latch();
                    } else {
                        return Err(AdmissionError::AuthorityUnavailable);
                    }
                }
                Ok(())
            },
        );
        assert_eq!(checks, 3);
        assert_eq!(
            result.err(),
            Some(if latch {
                AdmissionError::CommitUnknown
            } else {
                AdmissionError::AuthorityUnavailable
            })
        );
        assert_eq!(rows(&store), before.rows);
        assert_eq!(inputs(&path), before.inputs);
        assert_eq!(store.db.latest_sequence_number(), before.native_sequence);
    }
}
