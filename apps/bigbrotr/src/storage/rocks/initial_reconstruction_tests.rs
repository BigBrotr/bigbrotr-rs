//! Close/reopen native observations, not separate-process or activation evidence.
//! Expected originals are captured before handoff and checked against the signed
//! fixture and explicit source/dependency sets, never a reconstructed outcome.
use super::producer_recovery_tests::recovery_limits;
use super::*;
use crate::storage::rocks::keys::IngestKey;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

type Rows = Vec<(String, Vec<u8>, Vec<u8>)>;

struct Original {
    input: Vec<u8>,
    outcome: Vec<u8>,
}
struct Fixture {
    root: tempfile::TempDir,
    budget: ResourceBudget,
    native: RocksNativeLimits,
    previous: IngestManifest,
    next: IngestManifest,
    originals: Vec<Original>,
    closed_rows: Rows,
    closure: [u8; 32],
}
impl Fixture {
    fn path(&self) -> PathBuf {
        self.root.path().join("db")
    }
    fn reopen(&self) -> IngestRecovery {
        // The manifest is read from native state, not supplied as an owner
        // identity and not recovered from a retained InitialHandoff object.
        IngestRecovery::inspect(
            &self.path(),
            self.previous.identity().dataset_id,
            self.native,
            grant(&self.budget, 0, 128 * MIB),
        )
        .unwrap()
    }
}

fn evidence(dictionaries: Vec<DictionaryEntry>) -> EvidenceIntent {
    EvidenceIntent::new(
        1,
        reference(DictionaryKind::VisibilityScope, 1),
        dictionaries,
        EvidenceIntentLimits {
            max_entries: 16,
            max_encoded_intent_bytes: 8192,
            max_validation_scratch_bytes: 4096,
        },
    )
    .unwrap()
}

fn successor(previous: IngestManifest) -> IngestManifest {
    let mut identity = *previous.identity();
    identity.owner_epoch = OwnerEpoch::new(2).unwrap();
    identity.topology_generation = TopologyGeneration::new(2);
    identity.process_incarnation = [44; 16].try_into().unwrap();
    let domains: Vec<_> = (1..=9)
        .filter_map(|number| {
            let kind = DictionaryKind::try_from(number).unwrap();
            previous
                .allocation_namespaces()
                .domain(kind)
                .map(|_| (kind, [number as u8 + 32; 16].try_into().unwrap()))
        })
        .collect();
    IngestManifest::for_successor(
        identity,
        AllocationNamespaces::from_entries(&domains).unwrap(),
    )
}

fn value(store: &IngestStore, key: IngestKey) -> Vec<u8> {
    let key = key.encode().unwrap();
    store
        .db
        .get_cf(
            store.db.cf_handle(key.column_family().name()).unwrap(),
            key.as_bytes(),
        )
        .unwrap()
        .unwrap()
}

fn handoff_key() -> IngestKey {
    IngestKey::OwnerHandoff {
        dataset_id: [1; 16].try_into().unwrap(),
        owner_resource_id: [3; 16].try_into().unwrap(),
        predecessor_epoch: OwnerEpoch::new(1).unwrap(),
    }
}

fn input_name(epoch: u64, sequence: u64) -> String {
    format!(
        "{}-{epoch:016x}-{sequence:016x}.cbor",
        request(7, 1).producer_slot_id
    )
}

fn inputs(path: &Path) -> Vec<(String, Vec<u8>)> {
    let mut result: Vec<_> = std::fs::read_dir(path.join("producer-inputs-v1"))
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            assert!(entry.file_type().unwrap().is_file());
            (
                entry.file_name().into_string().unwrap(),
                std::fs::read(entry.path()).unwrap(),
            )
        })
        .collect();
    result.sort();
    result
}

fn input_digest(original: &Original) -> Digest256 {
    Digest256::from_bytes(Sha256::digest(&original.input).into())
}

fn expected_closure(manifest: &[u8], originals: &[Original]) -> [u8; 32] {
    // The owning byte contract is docs/reference/producer-request-inputs.md.
    // Form the complete independent transcript from original bytes; no product
    // closure accumulator or reconstructed result supplies an expected field.
    let mut transcript = b"bigbrotr/initial-source-closure/1\0".to_vec();
    transcript.extend_from_slice(&Sha256::digest(manifest));
    transcript.extend_from_slice(&[7; 16]);
    transcript.extend_from_slice(&1_u64.to_be_bytes());
    transcript.extend_from_slice(&4_u64.to_be_bytes());
    for (index, original) in originals.iter().enumerate() {
        transcript.extend_from_slice(&(index as u64 + 1).to_be_bytes());
        transcript.extend_from_slice(&Sha256::digest(&original.input));
        transcript.extend_from_slice(&Sha256::digest(&original.outcome));
    }
    transcript.extend_from_slice(&9_u64.to_be_bytes());
    Sha256::digest(transcript).into()
}

fn assert_receipt(receipt: &Receipt, sequence: u64) {
    assert_eq!(receipt.request_id(), request(7, sequence));
    assert_eq!(receipt.protection_context().topology_generation.get(), 1);
    assert_eq!(receipt.protection_context().policy_epoch.get(), 1);
    assert_eq!(
        receipt.protection_context().recovery_generation,
        RecoveryGenerationId::from_bytes([6; 16])
    );
    let (batch, dependencies): (Option<(u64, u64)>, &[u64]) = match sequence {
        1 => (Some((1, 4)), &[1, 2, 3, 4]),
        2 => (Some((5, 5)), &[1, 5]),
        3 => (Some((6, 9)), &[1, 2, 3, 6, 7, 8, 9]),
        4 => (None, &[1, 2, 3, 6, 7, 8, 9]),
        _ => unreachable!(),
    };
    assert_eq!(
        receipt
            .batch()
            .map(|batch| (batch.first().sequence.get(), batch.last().sequence.get())),
        batch
    );
    assert_eq!(
        receipt
            .dependencies()
            .records()
            .iter()
            .map(|dependency| {
                let id = dependency.source_record_id;
                assert_eq!(
                    id.dataset_id,
                    reference(DictionaryKind::Observer, 1).dataset_id
                );
                assert_eq!(id.shard_id, 0);
                assert_eq!(id.owner_epoch.get(), 1);
                id.sequence.get()
            })
            .collect::<Vec<_>>(),
        dependencies
    );
}

fn prepared_fixture() -> Fixture {
    initial_fixture(true)
}

fn closed_fixture() -> Fixture {
    initial_fixture(false)
}

fn initial_fixture(complete_handoff: bool) -> Fixture {
    let (root, mut store, budget, previous, native) = setup(false);
    let observer = DictionaryEntry::new(
        reference(DictionaryKind::Observer, 2),
        DictionaryDescriptor::Observer(ObserverDescriptor {
            clock: Some(ClockSource::SystemUtc),
            vantage: Some(label("second-observer")),
            software: None,
            method_id: None,
            public_key: None,
            route_class: None,
            software_version: None,
        }),
    )
    .unwrap();
    let bootstrap = evidence(entries(false));
    let dictionary = evidence(vec![observer.clone()]);
    let append_limits = AdmissionLimits::default();
    let append = store
        .append_dictionaries(
            request(7, 2),
            &dictionary,
            ObservedTime::new(20),
            append_limits,
            grant(
                &budget,
                append_limits.required_dictionary_data_bytes().unwrap(),
                append_limits.required_native_bytes().unwrap(),
            ),
            || Ok(()),
        )
        .unwrap();
    assert_receipt(append.receipt(), 2);
    drop(append);
    for sequence in [3, 4] {
        let prepared = PreparedEvent::new(
            request(7, sequence),
            intent("E_private", 2, 10_000_000),
            256 * 1024,
            grant(&budget, MIB, 0),
        )
        .unwrap();
        let result = store
            .admit_event(
                prepared,
                ObservedTime::new(10_000_000),
                false,
                AdmissionLimits::default(),
                grant(&budget, 40 * MIB, 10 * MIB),
                || Ok(()),
            )
            .unwrap();
        let AdmissionOutcome::Durable(receipt) = result.outcome() else {
            panic!("the independent signed fixture must be durable")
        };
        assert_receipt(receipt, sequence);
    }
    let all = facts(&store);
    assert_eq!(all.len(), 9);
    assert!(matches!(all[4].payload(), FactPayload::Dictionary(entry) if entry == &observer));
    assert!(matches!(all[5].payload(), FactPayload::Dictionary(entry)
        if entry.descriptor() == &DictionaryDescriptor::Event(fixture_event("E_private").id)));
    assert!(
        matches!(all[6].payload(), FactPayload::EventPayload(payload)
        if payload.body == EventBody::from(fixture_event("E_private")))
    );
    assert!(
        matches!(all[7].payload(), FactPayload::EventEligibility(eligibility)
        if eligibility.sig == fixture_event("E_private").sig)
    );
    assert!(
        matches!(all[8].payload(), FactPayload::EventReceive(receive)
        if receive.received_at.get() == 10_000_000
        && receive.observer_id == reference(DictionaryKind::Observer, 1))
    );
    let encoded_inputs = [
        bootstrap
            .encode_request_preimage(request(7, 1), 8192)
            .unwrap(),
        dictionary
            .encode_request_preimage(request(7, 2), 8192)
            .unwrap(),
        intent("E_private", 2, 10_000_000)
            .encode_request_preimage(request(7, 3), 8192)
            .unwrap(),
        intent("E_private", 2, 10_000_000)
            .encode_request_preimage(request(7, 4), 8192)
            .unwrap(),
    ];
    assert_eq!(
        inputs(&root.path().join("db")),
        encoded_inputs
            .iter()
            .enumerate()
            .map(|(index, bytes)| (input_name(1, index as u64 + 1), bytes.clone()))
            .collect::<Vec<_>>()
    );
    let originals: Vec<_> = encoded_inputs
        .into_iter()
        .enumerate()
        .map(|(index, input)| Original {
            input,
            outcome: value(
                &store,
                IngestKey::Request {
                    request_id: request(7, index as u64 + 1),
                },
            ),
        })
        .collect();
    let old_manifest = value(&store, IngestKey::Manifest);
    let closure = expected_closure(&old_manifest, &originals);
    drop(store);
    let mut recovery = IngestRecovery::open(
        &root.path().join("db"),
        previous,
        native,
        grant(&budget, 0, 128 * MIB),
    )
    .unwrap();
    let closed = recovery
        .close_producer(
            request(7, 1).producer_slot_id,
            ProducerEpoch::new(1).unwrap(),
            grant(&budget, 4096, 4096),
            || Ok(()),
        )
        .unwrap();
    let next = successor(previous);
    let closed_rows = kv(&recovery.store);
    {
        let handoff = {
            let mut source = closed.source_resolution(&recovery, 4).unwrap();
            for (index, original) in originals.iter().enumerate() {
                let resolved = recovery
                    .resolve_original_request(
                        request(7, index as u64 + 1),
                        input_digest(original),
                        recovery_limits(),
                        grant(&budget, 3 * MIB, 0),
                        grant(&budget, 40 * MIB, 0),
                        || Ok(()),
                    )
                    .unwrap();
                assert_eq!(resolved.native_outcome().bytes(), original.outcome);
                source.record(&resolved).unwrap();
            }
            let proof = source.finish().unwrap();
            assert_eq!(proof.digest(), closure);
            assert_eq!(proof.last_complete_sequence(), 9);
            InitialHandoff::from_source(&proof, next).unwrap()
        };
        if complete_handoff {
            drop(
                recovery
                    .apply_initial_handoff(&handoff, grant(&budget, 8192, 8192), || Ok(()))
                    .unwrap(),
            );
        }
        // Both the preparation plan and its native proof die before reopen.
    }
    drop(recovery);
    Fixture {
        root,
        budget,
        native,
        previous,
        next,
        originals,
        closed_rows,
        closure,
    }
}

#[path = "initial_gap_tests.rs"]
mod initial_gap_tests;

#[path = "initial_empty_tests.rs"]
mod initial_empty_tests;

#[path = "initial_writer_tests.rs"]
mod initial_writer_tests;

fn reconstruct(fixture: &Fixture, recovery: &IngestRecovery) -> Result<InitialHandoff, String> {
    let bytes = value(&recovery.store, handoff_key());
    let historical = recovery
        .historical_initial(&bytes)
        .map_err(|error| format!("historical view: {error:?}"))?;
    let closing = historical
        .observe_closed_producer(grant(&fixture.budget, 4096, 4096))
        .map_err(|error| format!("closing: {error:?}"))?;
    let mut source = historical
        .source_resolution(closing, 4)
        .map_err(|error| format!("source: {error:?}"))?;
    for (index, original) in fixture.originals.iter().enumerate() {
        let resolved = historical
            .resolve_original_request(
                request(7, index as u64 + 1),
                input_digest(original),
                recovery_limits(),
                grant(&fixture.budget, 3 * MIB, 0),
                grant(&fixture.budget, 40 * MIB, 0),
                || Ok(()),
            )
            .map_err(|error| format!("request {}: {error:?}", index + 1))?;
        source
            .record(&resolved)
            .map_err(|error| format!("record: {error:?}"))?;
    }
    let source = source
        .finish()
        .map_err(|error| format!("finish: {error:?}"))?;
    historical
        .reconstruct_handoff(&source)
        .map_err(|error| format!("reconstruct: {error:?}"))
}

#[test]
fn reopened_prepared_successor_reads_exact_originals_without_retaining_its_local_plan() {
    let fixture = prepared_fixture();
    let recovery = fixture.reopen();
    let before = kv(&recovery.store);
    let sequence = recovery.store.db.latest_sequence_number();
    let original_inputs = inputs(&fixture.path());
    assert_eq!(recovery.manifest(), &fixture.next);
    assert_eq!(
        value(&recovery.store, IngestKey::Manifest),
        fixture.next.encode().as_bytes()
    );
    assert_eq!(
        value(&recovery.store, source_key(2)),
        [1, 2, 0, 0, 0, 0, 0, 0, 0, 0]
    );
    let mut producer_zero = [0; 26];
    producer_zero[..2].copy_from_slice(&[1, 49]);
    assert_eq!(
        value(
            &recovery.store,
            IngestKey::ProducerState {
                producer_slot_id: request(7, 1).producer_slot_id,
                producer_epoch: ProducerEpoch::new(2).unwrap(),
            },
        ),
        producer_zero
    );
    assert_eq!(before.len(), fixture.closed_rows.len() + 3);
    for row in &fixture.closed_rows {
        if row.0 != "default" || row.1 != [1, 1] {
            assert!(before.contains(row), "original row changed: {}", row.0);
        }
    }
    let bytes = value(&recovery.store, handoff_key());
    let historical = recovery.historical_initial(&bytes).unwrap();
    assert_eq!(historical.manifest(), &fixture.previous);
    assert_eq!(historical.target_manifest(), &fixture.next);
    assert_eq!(historical.recovery().manifest(), &fixture.next);
    assert_eq!(
        historical.producer_slot_id(),
        request(7, 1).producer_slot_id
    );
    assert_eq!(historical.issued_through(), 4);
    assert_eq!(historical.last_complete_sequence(), 9);
    assert_eq!(historical.closure_digest(), fixture.closure);
    let closed = historical
        .observe_closed_producer(grant(&fixture.budget, 4096, 4096))
        .unwrap();
    assert_eq!(closed.manifest(), &fixture.previous);
    let mut source = historical.source_resolution(closed, 4).unwrap();
    for (index, original) in fixture.originals.iter().enumerate() {
        let sequence = index as u64 + 1;
        let resolved = historical
            .resolve_original_request(
                request(7, sequence),
                input_digest(original),
                recovery_limits(),
                grant(&fixture.budget, 3 * MIB, 0),
                grant(&fixture.budget, 40 * MIB, 0),
                || Ok(()),
            )
            .unwrap();
        let AdmissionOutcome::Durable(receipt) = resolved.outcome() else {
            panic!("all four original outcomes are durable")
        };
        assert_receipt(receipt, sequence);
        assert_eq!(resolved.native_outcome().bytes(), original.outcome);
        assert_eq!(resolved.native_outcome().digest(), input_digest(original));
        assert_eq!(resolved.input_bytes() as usize, original.input.len());
        assert_eq!(resolved.is_event(), sequence >= 3);
        assert_eq!(
            resolved.native_outcome().manifest_sha256(),
            <[u8; 32]>::from(Sha256::digest(fixture.previous.encode().as_bytes()))
        );
        source.record(&resolved).unwrap();
    }
    let source = source.finish().unwrap();
    assert_eq!(source.resolved_requests(), 4);
    assert_eq!(source.last_complete_sequence(), 9);
    assert_eq!(source.digest(), fixture.closure);
    assert_eq!(source.event_request_mask(), 0b1100);
    assert!(InitialHandoff::from_source(&source, fixture.next).is_err());
    let rebuilt = historical.reconstruct_handoff(&source).unwrap();
    assert_eq!(rebuilt.previous_manifest(), &fixture.previous);
    assert_eq!(rebuilt.next_manifest(), &fixture.next);
    assert_eq!(rebuilt.producer_slot_id(), request(7, 1).producer_slot_id);
    assert_eq!(rebuilt.producer_epoch().get(), 1);
    assert_eq!(rebuilt.issued_through(), 4);
    assert_eq!(rebuilt.last_complete_sequence(), 9);
    assert_eq!(rebuilt.closure_digest(), fixture.closure);
    assert_eq!(rebuilt.encode().as_bytes(), bytes);
    // A second independent observation has no native completion side effect.
    assert_eq!(
        reconstruct(&fixture, &recovery)
            .unwrap()
            .encode()
            .as_bytes(),
        bytes
    );
    assert_eq!(kv(&recovery.store), before);
    assert_eq!(recovery.store.db.latest_sequence_number(), sequence);
    assert_eq!(inputs(&fixture.path()), original_inputs);
    assert_eq!(recovery.manifest(), &fixture.next);
}

fn put(store: &IngestStore, key: IngestKey, bytes: &[u8]) {
    let key = key.encode().unwrap();
    store
        .db
        .put_cf(
            store.db.cf_handle(key.column_family().name()).unwrap(),
            key.as_bytes(),
            bytes,
        )
        .unwrap();
}

fn delete(store: &IngestStore, key: IngestKey) {
    let key = key.encode().unwrap();
    store
        .db
        .delete_cf(
            store.db.cf_handle(key.column_family().name()).unwrap(),
            key.as_bytes(),
        )
        .unwrap();
}

fn source_key(epoch: u64) -> IngestKey {
    IngestKey::SourceStream {
        dataset_id: [1; 16].try_into().unwrap(),
        shard_id: 0,
        owner_epoch: OwnerEpoch::new(epoch).unwrap(),
    }
}

fn fact_id(epoch: u64, sequence: u64) -> SourceRecordId {
    SourceRecordId {
        dataset_id: [1; 16].try_into().unwrap(),
        shard_id: 0,
        owner_epoch: OwnerEpoch::new(epoch).unwrap(),
        sequence: Sequence::new(sequence).unwrap(),
    }
}

#[test]
fn reconstruction_refuses_missing_or_changed_original_evidence_without_repair() {
    for fault in [
        "bootstrap-input",
        "dictionary-input",
        "event-input",
        "receipt",
        "bootstrap-fact",
        "dictionary-fact",
        "event-fact",
        "successor-context",
        "source-counter",
        "closure-digest",
        "recorded-cut",
        "recorded-count",
    ] {
        let fixture = prepared_fixture();
        let recovery = fixture.reopen();
        match fault {
            "bootstrap-input" | "dictionary-input" | "event-input" => {
                let number = match fault {
                    "bootstrap-input" => 1,
                    "dictionary-input" => 2,
                    _ => 3,
                };
                let file = fixture
                    .path()
                    .join("producer-inputs-v1")
                    .join(input_name(1, number));
                if number == 1 {
                    std::fs::remove_file(file).unwrap();
                } else {
                    let mut bytes = std::fs::read(&file).unwrap();
                    bytes[0] ^= 1;
                    std::fs::write(file, bytes).unwrap();
                }
            }
            "receipt" => delete(
                &recovery.store,
                IngestKey::Request {
                    request_id: request(7, 4),
                },
            ),
            "bootstrap-fact" => delete(
                &recovery.store,
                IngestKey::Fact {
                    source_record_id: fact_id(1, 1),
                },
            ),
            "dictionary-fact" => delete(
                &recovery.store,
                IngestKey::Fact {
                    source_record_id: fact_id(1, 5),
                },
            ),
            "event-fact" => delete(
                &recovery.store,
                IngestKey::Fact {
                    source_record_id: fact_id(1, 7),
                },
            ),
            "successor-context" => {
                let original = Receipt::decode_persistent(
                    &fixture.originals[2].outcome,
                    bigbrotr_model::receipt_codec::ReceiptDecodeLimits::default(),
                )
                .unwrap();
                let mut context = *original.protection_context();
                context.topology_generation = TopologyGeneration::new(2);
                let foreign = Receipt::new_facts(
                    original.request_id(),
                    original.request_digest(),
                    context,
                    original.batch().unwrap().clone(),
                    original.dependencies().clone(),
                )
                .unwrap();
                put(
                    &recovery.store,
                    IngestKey::Request {
                        request_id: request(7, 3),
                    },
                    &foreign.encode_persistent(8192).unwrap(),
                );
            }
            "source-counter" => put(
                &recovery.store,
                source_key(1),
                &[1, 2, 0, 0, 0, 0, 0, 0, 0, 10],
            ),
            "closure-digest" | "recorded-cut" | "recorded-count" => {
                let mut bytes = value(&recovery.store, handoff_key());
                let index = match fault {
                    "closure-digest" => 42,
                    "recorded-cut" => 41,
                    _ => 33,
                };
                bytes[index] ^= 1;
                put(&recovery.store, handoff_key(), &bytes);
            }
            _ => unreachable!(),
        }
        drop(recovery);
        let recovery = fixture.reopen();
        let before = kv(&recovery.store);
        let sequence = recovery.store.db.latest_sequence_number();
        let original_inputs = inputs(&fixture.path());
        assert!(reconstruct(&fixture, &recovery).is_err(), "{fault}");
        assert_eq!(kv(&recovery.store), before, "{fault}");
        assert_eq!(
            recovery.store.db.latest_sequence_number(),
            sequence,
            "{fault}"
        );
        assert_eq!(inputs(&fixture.path()), original_inputs, "{fault}");
    }
}

#[test]
fn reconstruction_refuses_nonempty_successor_and_extra_producer_markers() {
    for fault in [
        "source",
        "producer",
        "fact",
        "pending",
        "request",
        "dictionary",
        "allocation",
        "extra-producer",
        "input",
    ] {
        let fixture = prepared_fixture();
        let recovery = fixture.reopen();
        let next_reference = QualifiedRef {
            allocation_domain: [34; 16].try_into().unwrap(),
            ..reference(DictionaryKind::Observer, 1)
        };
        match fault {
            "source" => put(
                &recovery.store,
                source_key(2),
                &[1, 2, 0, 0, 0, 0, 0, 0, 0, 1],
            ),
            "producer" | "extra-producer" => {
                let mut bytes = [0; 26];
                bytes[..2].copy_from_slice(&[1, 49]);
                if fault == "producer" {
                    bytes[9] = 1;
                }
                put(
                    &recovery.store,
                    IngestKey::ProducerState {
                        producer_slot_id: [if fault == "producer" { 7 } else { 8 }; 16]
                            .try_into()
                            .unwrap(),
                        producer_epoch: ProducerEpoch::new(2).unwrap(),
                    },
                    &bytes,
                );
            }
            "fact" => put(
                &recovery.store,
                IngestKey::Fact {
                    source_record_id: fact_id(2, 1),
                },
                b"unexpected target fact",
            ),
            "pending" => put(
                &recovery.store,
                IngestKey::PendingBatch {
                    first: fact_id(2, 1),
                },
                b"unexpected target pending batch",
            ),
            "request" => put(
                &recovery.store,
                IngestKey::Request {
                    request_id: ProducerRequestId {
                        producer_epoch: ProducerEpoch::new(2).unwrap(),
                        ..request(7, 1)
                    },
                },
                b"unexpected target request",
            ),
            "dictionary" => put(
                &recovery.store,
                IngestKey::Dictionary {
                    reference: next_reference,
                },
                b"unexpected target dictionary",
            ),
            "allocation" => put(
                &recovery.store,
                IngestKey::AllocationStream {
                    dataset_id: next_reference.dataset_id,
                    dictionary_kind: next_reference.dictionary_kind,
                    allocation_domain: next_reference.allocation_domain,
                },
                &[1, 3, 0, 0, 0, 0, 0, 0, 0, 1],
            ),
            "input" => {
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
            }
            _ => unreachable!(),
        }
        drop(recovery);
        let recovery = fixture.reopen();
        let before = kv(&recovery.store);
        let sequence = recovery.store.db.latest_sequence_number();
        let original_inputs = inputs(&fixture.path());
        assert!(reconstruct(&fixture, &recovery).is_err(), "{fault}");
        assert_eq!(kv(&recovery.store), before, "{fault}");
        assert_eq!(
            recovery.store.db.latest_sequence_number(),
            sequence,
            "{fault}"
        );
        assert_eq!(inputs(&fixture.path()), original_inputs, "{fault}");
    }
}

#[test]
fn reconstruction_requires_the_exact_retained_handoff_and_current_successor_manifest() {
    let fixture = prepared_fixture();
    let recovery = fixture.reopen();
    let before = kv(&recovery.store);
    let sequence = recovery.store.db.latest_sequence_number();
    let original_inputs = inputs(&fixture.path());
    let bytes = value(&recovery.store, handoff_key());
    for end in [0, 1, 2, 41, 73, bytes.len() - 1] {
        assert!(recovery.historical_initial(&bytes[..end]).is_err());
    }
    for index in [0, 1, 2, 25, 33, 41, 42, bytes.len() - 1] {
        let mut changed = bytes.clone();
        changed[index] ^= 1;
        assert!(
            recovery.historical_initial(&changed).is_err(),
            "byte {index}"
        );
    }
    let mut extra = bytes.clone();
    extra.push(0);
    assert!(recovery.historical_initial(&extra).is_err());
    assert_eq!(kv(&recovery.store), before);
    assert_eq!(recovery.store.db.latest_sequence_number(), sequence);
    assert_eq!(inputs(&fixture.path()), original_inputs);
    // An unrelated old store cannot read a receipt merely supplied by a caller.
    let (_root, store, other_budget, _manifest, _native) = setup(false);
    let other = IngestRecovery { store };
    let before = kv(&other.store);
    let sequence = other.store.db.latest_sequence_number();
    assert!(other.historical_initial(&bytes).is_err());
    assert_eq!(kv(&other.store), before);
    assert_eq!(other.store.db.latest_sequence_number(), sequence);
    drop(other);
    assert_eq!(other_budget.snapshot().active_permits(), 0);
}

#[test]
fn reconstructed_local_proof_cannot_be_transferred_between_identical_native_stores() {
    let first = prepared_fixture();
    let second = prepared_fixture();
    let recovery = first.reopen();
    let other = second.reopen();
    let original_inputs = inputs(&first.path());
    assert_eq!(inputs(&second.path()), original_inputs);
    assert_eq!(kv(&recovery.store), kv(&other.store));
    assert_eq!(
        recovery.store.db.latest_sequence_number(),
        other.store.db.latest_sequence_number()
    );
    let before = kv(&recovery.store);
    let sequence = recovery.store.db.latest_sequence_number();
    let bytes = value(&recovery.store, handoff_key());
    let historical = recovery.historical_initial(&bytes).unwrap();
    let other_historical = other.historical_initial(&bytes).unwrap();
    let closed = historical
        .observe_closed_producer(grant(&first.budget, 4096, 4096))
        .unwrap();
    let mut source = historical.source_resolution(closed, 4).unwrap();
    let other_closed = other_historical
        .observe_closed_producer(grant(&second.budget, 4096, 4096))
        .unwrap();
    let mut other_source = other_historical.source_resolution(other_closed, 4).unwrap();
    for (index, original) in first.originals.iter().enumerate() {
        let resolved = historical
            .resolve_original_request(
                request(7, index as u64 + 1),
                input_digest(original),
                recovery_limits(),
                grant(&first.budget, 3 * MIB, 0),
                grant(&first.budget, 40 * MIB, 0),
                || Ok(()),
            )
            .unwrap();
        // Matching identity, bytes and sequence do not turn another native
        // directory's positive observation into this directory's evidence.
        assert!(other_source.record(&resolved).is_err());
        let own_resolved = other_historical
            .resolve_original_request(
                request(7, index as u64 + 1),
                input_digest(original),
                recovery_limits(),
                grant(&second.budget, 3 * MIB, 0),
                grant(&second.budget, 40 * MIB, 0),
                || Ok(()),
            )
            .unwrap();
        other_source.record(&own_resolved).unwrap();
        source.record(&resolved).unwrap();
    }
    let source = source.finish().unwrap();
    let other_source = other_source.finish().unwrap();
    assert_eq!(other_source.digest(), second.closure);
    assert!(other_historical.reconstruct_handoff(&source).is_err());
    assert_eq!(
        historical
            .reconstruct_handoff(&source)
            .unwrap()
            .encode()
            .as_bytes(),
        bytes
    );
    assert_eq!(kv(&recovery.store), before);
    assert_eq!(kv(&other.store), before);
    assert_eq!(recovery.store.db.latest_sequence_number(), sequence);
    assert_eq!(other.store.db.latest_sequence_number(), sequence);
    assert_eq!(inputs(&first.path()), original_inputs);
    assert_eq!(inputs(&second.path()), original_inputs);
}

#[test]
fn stale_request_observation_cannot_advance_a_new_native_sequence() {
    let fixture = prepared_fixture();
    let recovery = fixture.reopen();
    let bytes = value(&recovery.store, handoff_key());
    let historical = recovery.historical_initial(&bytes).unwrap();
    let closed = historical
        .observe_closed_producer(grant(&fixture.budget, 4096, 4096))
        .unwrap();
    let mut source = historical.source_resolution(closed, 4).unwrap();
    let resolved = historical
        .resolve_original_request(
            request(7, 1),
            input_digest(&fixture.originals[0]),
            recovery_limits(),
            grant(&fixture.budget, 3 * MIB, 0),
            grant(&fixture.budget, 40 * MIB, 0),
            || Ok(()),
        )
        .unwrap();
    let before = kv(&recovery.store);
    let old_sequence = recovery.store.db.latest_sequence_number();
    let original_inputs = inputs(&fixture.path());
    // Privileged fixture injection: identical bytes still create a new Rocks
    // sequence. No product writer is exposed by the historical read API.
    put(&recovery.store, handoff_key(), &bytes);
    let sequence = recovery.store.db.latest_sequence_number();
    assert_eq!(sequence, old_sequence + 1);
    assert!(source.record(&resolved).is_err());
    assert!(source.finish().is_err());
    assert_eq!(
        reconstruct(&fixture, &recovery)
            .unwrap()
            .encode()
            .as_bytes(),
        bytes
    );
    assert_eq!(kv(&recovery.store), before);
    assert_eq!(recovery.store.db.latest_sequence_number(), sequence);
    assert_eq!(inputs(&fixture.path()), original_inputs);
}

#[test]
fn historical_replay_refusals_preserve_identical_positive_retry_and_native_sequence() {
    let fixture = prepared_fixture();
    let recovery = fixture.reopen();
    let before = kv(&recovery.store);
    let sequence = recovery.store.db.latest_sequence_number();
    let original_inputs = inputs(&fixture.path());
    let bytes = value(&recovery.store, handoff_key());
    let historical = recovery.historical_initial(&bytes).unwrap();
    assert_eq!(
        historical
            .observe_closed_producer(grant(&fixture.budget, 4095, 4096))
            .err(),
        Some(BootstrapError::InsufficientReservation)
    );
    assert!(
        recovery
            .resolve_original_request(
                request(7, 1),
                input_digest(&fixture.originals[0]),
                recovery_limits(),
                grant(&fixture.budget, 3 * MIB, 0),
                grant(&fixture.budget, 40 * MIB, 0),
                || Ok(()),
            )
            .is_err()
    );
    for fault in [
        "outcome-grant",
        "input-limit",
        "authority1",
        "authority2",
        "authority3",
    ] {
        let mut calls = 0;
        let mut limits = recovery_limits();
        if fault == "input-limit" {
            limits.input.values.max_encoded_bytes = 1;
        }
        let result = historical.resolve_original_request(
            request(7, 3),
            input_digest(&fixture.originals[2]),
            limits,
            grant(&fixture.budget, 3 * MIB, 0),
            grant(
                &fixture.budget,
                if fault == "outcome-grant" {
                    1
                } else {
                    40 * MIB
                },
                0,
            ),
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
            match fault {
                "outcome-grant" =>
                    calls == 0
                        && matches!(
                            result,
                            Err(ProducerRecoveryError::Native(
                                AdmissionError::InsufficientReservation
                            ))
                        ),
                "input-limit" => matches!(
                    result,
                    Err(ProducerRecoveryError::Input(RequestInputError::Codec(
                        bigbrotr_model::codec::CodecError::InputLimit
                    )))
                ),
                _ => matches!(
                    result,
                    Err(ProducerRecoveryError::Native(
                        AdmissionError::AuthorityUnavailable
                    ))
                ),
            },
            "{fault}"
        );
        assert_eq!(
            reconstruct(&fixture, &recovery)
                .unwrap()
                .encode()
                .as_bytes(),
            bytes
        );
        assert_eq!(kv(&recovery.store), before, "{fault}");
        assert_eq!(
            recovery.store.db.latest_sequence_number(),
            sequence,
            "{fault}"
        );
        assert_eq!(inputs(&fixture.path()), original_inputs, "{fault}");
    }
}
