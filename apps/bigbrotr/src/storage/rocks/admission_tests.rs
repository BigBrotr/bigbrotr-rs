//! Real signed inputs and native engine state, not a mocked signature verifier.
use super::*;
use crate::runtime::{
    ProcessEnvelope, ResourceBudget, ResourceLimits, ResourceRequest, WaiterLimits,
};
use crate::storage::rocks::{
    RocksNativeSettings,
    manifest::{AllocationNamespaces, ManifestIdentity},
};
use bigbrotr_model::*;

const MIB: u64 = 1024 * 1024;
fn reference(kind: DictionaryKind, ordinal: u64) -> QualifiedRef {
    QualifiedRef {
        dataset_id: [1; 16].try_into().unwrap(),
        dictionary_kind: kind,
        allocation_domain: [kind as u8; 16].try_into().unwrap(),
        ordinal: Ordinal::new(ordinal).unwrap(),
    }
}
fn request(slot: u8, sequence: u64) -> ProducerRequestId {
    ProducerRequestId {
        producer_slot_id: [slot; 16].try_into().unwrap(),
        producer_epoch: ProducerEpoch::new(1).unwrap(),
        request_sequence: RequestSequence::new(sequence).unwrap(),
    }
}
fn grant(b: &ResourceBudget, data: u64, native: u64) -> OwnedPermit {
    b.try_acquire(ResourceRequest::data(data, 1, native).unwrap())
        .unwrap()
}
fn fixture_event(name: &str) -> SignedEvent {
    let corpus: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../experiments/identity-contract/signed-fixture.json"
    ))
    .unwrap();
    serde_json::from_value(corpus["events"][name].clone()).unwrap()
}
fn intent(name: &str, scope: u64, received: i64) -> EventIntent {
    EventIntent::network(
        1,
        reference(DictionaryKind::VisibilityScope, scope),
        fixture_event(name),
        LocalObservation {
            received_at: ObservedTime::new(received),
            observer_id: reference(DictionaryKind::Observer, 1),
            clock_quality: ClockQuality::new(
                reference(DictionaryKind::Observer, 1),
                DurationUs::new(1),
                None,
                0,
            )
            .unwrap(),
            witness: None,
        },
    )
    .unwrap()
}
fn setup(
    provenance: bool,
) -> (
    tempfile::TempDir,
    IngestStore,
    ResourceBudget,
    IngestManifest,
    RocksNativeLimits,
) {
    setup_entries(entries(provenance))
}
fn setup_entries(
    entries: Vec<DictionaryEntry>,
) -> (
    tempfile::TempDir,
    IngestStore,
    ResourceBudget,
    IngestManifest,
    RocksNativeLimits,
) {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("db");
    let budget = ResourceBudget::new(
        ProcessEnvelope::new(
            400 * MIB,
            0,
            ResourceLimits::new(128 * MIB, 16, 256 * MIB, 16 * MIB),
            WaiterLimits::new(1, 1),
        )
        .unwrap(),
    );
    let manifest = IngestManifest::new(
        ManifestIdentity {
            dataset_id: [1; 16].try_into().unwrap(),
            topology_generation: TopologyGeneration::new(1),
            owner_resource_id: [3; 16].try_into().unwrap(),
            shard_id: 0,
            owner_epoch: OwnerEpoch::new(1).unwrap(),
            process_incarnation: [4; 16].try_into().unwrap(),
            recovery_generation: RecoveryGenerationId::from_bytes([6; 16]),
            policy_epoch: PolicyEpoch::new(1),
        },
        AllocationNamespaces::from_entries(
            &[
                DictionaryKind::Endpoint,
                DictionaryKind::Observer,
                DictionaryKind::AccessScope,
                DictionaryKind::VisibilityScope,
                DictionaryKind::Method,
                DictionaryKind::Event,
                DictionaryKind::DatasetLineage,
            ]
            .map(|k| (k, reference(k, 1).allocation_domain)),
        )
        .unwrap(),
    );
    let native = RocksNativeLimits::new(RocksNativeSettings {
        block_cache_bytes: 32 * MIB,
        write_buffer_manager_bytes: 16 * MIB,
        per_cf_write_buffer_bytes: 2 * MIB,
        max_total_wal_bytes: 16 * MIB,
        max_open_files: 32,
        max_background_jobs: 2,
        native_allowance_bytes: 128 * MIB,
    })
    .unwrap();
    let mut store =
        IngestStore::initialize(&path, manifest, native, grant(&budget, 0, 128 * MIB)).unwrap();
    let bootstrap = EvidenceIntent::new(
        1,
        reference(DictionaryKind::VisibilityScope, 1),
        entries,
        EvidenceIntentLimits {
            max_entries: 16,
            max_encoded_intent_bytes: 8192,
            max_validation_scratch_bytes: 4096,
        },
    )
    .unwrap();
    drop(
        store
            .bootstrap_dictionaries(
                request(7, 1),
                &bootstrap,
                ObservedTime::new(0),
                BootstrapLimits {
                    max_mappings: 16,
                    max_batch_bytes: MIB as usize,
                },
                grant(&budget, 8 * MIB, 4 * MIB),
                || Ok(()),
            )
            .unwrap(),
    );
    (root, store, budget, manifest, native)
}
#[test]
fn actual_signed_event_commits_payload_scope_and_receive_with_dictionary_and_receipt() {
    let (root, mut store, budget, manifest, native) = setup(false);
    let path = root.path().join("db");
    let input = PreparedEvent::new(
        request(5, 1),
        intent("E_private", 2, 10_000_000),
        256 * 1024,
        grant(&budget, MIB, 0),
    )
    .unwrap();
    let outcome = store
        .admit_event(
            input,
            ObservedTime::new(10_000_000),
            false,
            AdmissionLimits::default(),
            grant(&budget, 40 * MIB, 10 * MIB),
            || Ok(()),
        )
        .unwrap();
    let AdmissionOutcome::Durable(receipt) = outcome.outcome() else {
        panic!("signed event must be durable")
    };
    let batch = receipt.batch().unwrap();
    assert_eq!(batch.records().len(), 4);
    assert_eq!(batch.first().sequence.get(), 5);
    assert_eq!(batch.last().sequence.get(), 8);
    let original = receipt.encode_persistent(8192).unwrap();
    drop(outcome);
    drop(store);
    let mut store =
        IngestStore::reopen(&path, manifest, native, grant(&budget, 0, 128 * MIB)).unwrap();
    let input = PreparedEvent::new(
        request(5, 1),
        intent("E_private", 2, 10_000_000),
        256 * 1024,
        grant(&budget, MIB, 0),
    )
    .unwrap();
    let retry = store
        .admit_event(
            input,
            ObservedTime::new(99),
            false,
            AdmissionLimits::default(),
            grant(&budget, 40 * MIB, 10 * MIB),
            || Ok(()),
        )
        .unwrap();
    let AdmissionOutcome::Durable(receipt) = retry.outcome() else {
        panic!("retry must return original receipt")
    };
    assert_eq!(receipt.encode_persistent(8192).unwrap(), original);
}

fn label(v: &str) -> DescriptorLabel {
    DescriptorLabel::new(v.into()).unwrap()
}
fn entries(provenance: bool) -> Vec<DictionaryEntry> {
    let mut values = vec![
        (
            DictionaryKind::VisibilityScope,
            1,
            DictionaryDescriptor::Visibility {
                class: label("public"),
            },
        ),
        (
            DictionaryKind::VisibilityScope,
            2,
            DictionaryDescriptor::Visibility {
                class: label("private"),
            },
        ),
        (
            DictionaryKind::Observer,
            1,
            DictionaryDescriptor::Observer(ObserverDescriptor {
                clock: Some(ClockSource::SystemUtc),
                vantage: Some(label("fixture-vantage")),
                software: None,
                method_id: None,
                public_key: None,
                route_class: None,
                software_version: None,
            }),
        ),
        (
            DictionaryKind::DatasetLineage,
            1,
            DictionaryDescriptor::DatasetLineage(DatasetLineageDescriptor {
                format: label("nostr-jsonl"),
                format_version: 1,
                source_dataset_id: None,
                source_artifact_digest:
                    "c30658a75b2b913e509b7618f36ebae75fa1d5680dc8fe4e322aae638d8affae"
                        .parse()
                        .unwrap(),
            }),
        ),
    ];
    if provenance {
        values.extend([
            (
                DictionaryKind::Endpoint,
                1,
                DictionaryDescriptor::Endpoint(
                    EndpointDescriptor::new("wss://public.example/".into()).unwrap(),
                ),
            ),
            (
                DictionaryKind::Endpoint,
                2,
                DictionaryDescriptor::Endpoint(
                    EndpointDescriptor::new("wss://private.example/".into()).unwrap(),
                ),
            ),
            (
                DictionaryKind::AccessScope,
                1,
                DictionaryDescriptor::AccessScope(AccessScopeDescriptor {
                    class: AccessClass::Public,
                    label: None,
                    visibility_scope_id: reference(DictionaryKind::VisibilityScope, 1),
                }),
            ),
            (
                DictionaryKind::AccessScope,
                2,
                DictionaryDescriptor::AccessScope(AccessScopeDescriptor {
                    class: AccessClass::Protected,
                    label: Some(label("private-access")),
                    visibility_scope_id: reference(DictionaryKind::VisibilityScope, 2),
                }),
            ),
            (
                DictionaryKind::Method,
                1,
                DictionaryDescriptor::EventVerification,
            ),
        ]);
    }
    values
        .into_iter()
        .map(|(k, n, v)| DictionaryEntry::new(reference(k, n), v).unwrap())
        .collect()
}
fn binding(name: &str) -> QualifiedRef {
    let (kind, ordinal) = match name {
        "public" => (DictionaryKind::VisibilityScope, 1),
        "private" => (DictionaryKind::VisibilityScope, 2),
        "fixture-observer" => (DictionaryKind::Observer, 1),
        "fixture-import-lineage" => (DictionaryKind::DatasetLineage, 1),
        "R_public" => (DictionaryKind::Endpoint, 1),
        "R_private" => (DictionaryKind::Endpoint, 2),
        "public-access" => (DictionaryKind::AccessScope, 1),
        "private-access" => (DictionaryKind::AccessScope, 2),
        "nip01-bip340-full-verification" => (DictionaryKind::Method, 1),
        _ => panic!("unexpected frozen fixture binding"),
    };
    reference(kind, ordinal)
}
fn binding_name(r: QualifiedRef) -> &'static str {
    match (r.dictionary_kind, r.ordinal.get()) {
        (DictionaryKind::VisibilityScope, 1) => "public",
        (DictionaryKind::VisibilityScope, 2) => "private",
        (DictionaryKind::Observer, 1) => "fixture-observer",
        (DictionaryKind::DatasetLineage, 1) => "fixture-import-lineage",
        (DictionaryKind::Endpoint, 1) => "R_public",
        (DictionaryKind::Endpoint, 2) => "R_private",
        (DictionaryKind::AccessScope, 1) => "public-access",
        (DictionaryKind::AccessScope, 2) => "private-access",
        (DictionaryKind::Method, 1) => "nip01-bip340-full-verification",
        _ => panic!("unexpected stored binding"),
    }
}
fn fixture_intent(input: &serde_json::Value) -> EventIntent {
    let mut event = fixture_event(input["event_ref"].as_str().unwrap());
    if let Some(sig) = input["signature_override"].as_str() {
        event.sig = sig.parse().unwrap()
    }
    let scope = binding(input["visibility_scope"].as_str().unwrap());
    if input["origin"] == "import" {
        EventIntent::import(
            1,
            scope,
            event,
            binding(input["import_lineage"].as_str().unwrap()),
        )
        .unwrap()
    } else {
        let o = &input["observation"];
        assert_eq!(o["clock_quality"], "fixture-clock");
        let witness = if o["witness"].is_null() {
            None
        } else {
            let w = &o["witness"];
            Some(WitnessContext {
                endpoint_id: binding(w["endpoint"].as_str().unwrap()),
                access_scope_id: binding(w["access_scope"].as_str().unwrap()),
                verification_method_id: binding(w["verification_method"].as_str().unwrap()),
            })
        };
        EventIntent::network(
            1,
            scope,
            event,
            LocalObservation {
                received_at: ObservedTime::new(o["received_at_us"].as_i64().unwrap()),
                observer_id: binding(o["observer"].as_str().unwrap()),
                clock_quality: ClockQuality::new(
                    binding("fixture-observer"),
                    DurationUs::new(1),
                    None,
                    0,
                )
                .unwrap(),
                witness,
            },
        )
        .unwrap()
    }
}
fn facts(store: &IngestStore) -> Vec<Fact> {
    use crate::storage::rocks::keys::{ColumnFamily, IngestKey, decode_key};
    let cf = store.db.cf_handle("facts").unwrap();
    store
        .db
        .iterator_cf(cf, rocksdb::IteratorMode::Start)
        .filter_map(|row| {
            let (key, value) = row.unwrap();
            if let IngestKey::Fact { source_record_id } =
                decode_key(ColumnFamily::Facts, &key).unwrap()
            {
                assert_eq!(&value[..2], &[1, 0x20]);
                let digest = Digest256::from_bytes(value[2..34].try_into().unwrap());
                let fact = Fact::decode_preimage(
                    &value[34..],
                    digest,
                    bigbrotr_model::codec::FactDecodeLimits::default(),
                )
                .unwrap();
                assert_eq!(fact.source_record_id(), source_record_id);
                Some(fact)
            } else {
                None
            }
        })
        .collect()
}
fn kv(store: &IngestStore) -> Vec<(String, Vec<u8>, Vec<u8>)> {
    ["default", "facts", "receipts"]
        .into_iter()
        .flat_map(|name| {
            store
                .db
                .iterator_cf(
                    store.db.cf_handle(name).unwrap(),
                    rocksdb::IteratorMode::Start,
                )
                .map(move |v| {
                    let (k, v) = v.unwrap();
                    (name.to_string(), k.to_vec(), v.to_vec())
                })
        })
        .collect()
}
fn selected(store: &IngestStore, key: crate::storage::rocks::keys::IngestKey) -> SourceRecordId {
    use crate::storage::rocks::keys::{ColumnFamily, IngestKey, decode_key};
    let encoded = key.encode().unwrap();
    let value = store
        .db
        .get_cf(store.db.cf_handle("default").unwrap(), encoded.as_bytes())
        .unwrap()
        .unwrap();
    assert_eq!(value.len(), 70);
    assert_eq!(value[..2], key.domain().prefix());
    let mut key = vec![1, 0x20];
    key.extend_from_slice(&value[2..38]);
    let IngestKey::Fact { source_record_id } = decode_key(ColumnFamily::Facts, &key).unwrap()
    else {
        panic!()
    };
    assert_eq!(
        facts(store)
            .iter()
            .find(|f| f.source_record_id() == source_record_id)
            .unwrap()
            .payload_digest()
            .as_bytes(),
        &value[38..]
    );
    source_record_id
}
fn scientific(fact: &Fact) -> serde_json::Value {
    use serde_json::json;
    let scope = binding_name(fact.visibility_scope_id());
    match fact.payload() {
        FactPayload::EventPayload(v) => {
            json!({"kind":"event-payload/1","event_id":v.body.event_id.to_string(),"body_digest":v.body.body_digest().to_string()})
        }
        FactPayload::EventEligibility(v) => {
            json!({"kind":"event-eligibility/1","event_id":v.event_id.to_string(),"visibility_scope":scope,"body_digest":v.body_digest.to_string(),"signature":v.sig.to_string(),"first_eligible_at_us":v.first_eligible_at.get(),"origin":v.origin.as_str(),"import_lineage":v.origin.import_lineage().map(binding_name)})
        }
        FactPayload::EventReceive(v) => {
            json!({"kind":"event-receive/1","event_id":v.event_id.to_string(),"visibility_scope":scope,"received_at_us":v.received_at.get(),"observer":binding_name(v.observer_id),"clock_quality":"fixture-clock","corrects":v.corrects.map(|_|"prior_selected:event-receive/1")})
        }
        FactPayload::Provenance(v) => {
            json!({"kind":"provenance/1","event_id":v.event_id.to_string(),"visibility_scope":scope,"endpoint":binding_name(v.endpoint_id),"observer":binding_name(v.observer_id),"access_scope":binding_name(v.access_scope_id),"received_at_us":v.received_at.get(),"clock_quality":"fixture-clock","verification_method":binding_name(v.verification_method_id),"corrects":v.corrects.map(|_|"prior_selected:provenance/1")})
        }
        FactPayload::Dictionary(_) => panic!("not a scientific event fact"),
    }
}
fn assert_inventory(store: &IngestStore, expected: &serde_json::Value) {
    use crate::storage::rocks::keys::IngestKey;
    use serde_json::json;
    let all = facts(store);
    let dataset_id = reference(DictionaryKind::Event, 1).dataset_id;
    let mut payloads = vec![];
    let mut mappings = vec![];
    let mut eligibility = vec![];
    let mut receives = vec![];
    let mut witnesses = vec![];
    for fact in &all {
        match fact.payload() {
            FactPayload::Dictionary(v) => {
                if let DictionaryDescriptor::Event(id) = v.descriptor() {
                    mappings.push(json!(id.to_string()));
                    assert_eq!(
                        selected(
                            store,
                            IngestKey::Dictionary {
                                reference: v.reference()
                            }
                        ),
                        fact.source_record_id()
                    );
                }
            }
            FactPayload::EventPayload(v) => {
                payloads.push(json!({"event_id":v.body.event_id.to_string(),"body_digest":v.body.body_digest().to_string()}));
                assert_eq!(
                    selected(
                        store,
                        IngestKey::EventPayload {
                            dataset_id,
                            event_id: v.body.event_id
                        }
                    ),
                    fact.source_record_id()
                );
            }
            FactPayload::EventEligibility(v) => {
                let mut row = scientific(fact);
                row.as_object_mut().unwrap().remove("kind");
                eligibility.push(row);
                assert_eq!(
                    selected(
                        store,
                        IngestKey::EventEligibility {
                            dataset_id,
                            event_id: v.event_id,
                            visibility_scope: fact.visibility_scope_id()
                        }
                    ),
                    fact.source_record_id()
                );
            }
            FactPayload::EventReceive(v) => {
                if v.corrects.is_some() {
                    continue;
                }
                let key = IngestKey::EventReceive {
                    dataset_id,
                    event_id: v.event_id,
                    visibility_scope: fact.visibility_scope_id(),
                };
                let current = selected(store, key);
                let selected_fact = all
                    .iter()
                    .find(|f| f.source_record_id() == current)
                    .unwrap();
                let FactPayload::EventReceive(chosen) = selected_fact.payload() else {
                    panic!()
                };
                let history: Vec<_> = all
                    .iter()
                    .filter_map(|f| match f.payload() {
                        FactPayload::EventReceive(r)
                            if r.event_id == v.event_id
                                && f.visibility_scope_id() == fact.visibility_scope_id() =>
                        {
                            Some(r.received_at.get())
                        }
                        _ => None,
                    })
                    .collect();
                receives.push(json!({"event_id":v.event_id.to_string(),"visibility_scope":binding_name(fact.visibility_scope_id()),"selected_received_at_us":chosen.received_at.get(),"history_received_at_us":history,"selected_observer":binding_name(chosen.observer_id),"selected_clock_quality":"fixture-clock"}));
            }
            FactPayload::Provenance(v) => {
                if v.corrects.is_some() {
                    continue;
                }
                let key = IngestKey::Provenance {
                    dataset_id,
                    event_id: v.event_id,
                    endpoint_id: v.endpoint_id,
                    observer_id: v.observer_id,
                    access_scope_id: v.access_scope_id,
                    visibility_scope_id: fact.visibility_scope_id(),
                };
                let current = selected(store, key);
                let selected_fact = all
                    .iter()
                    .find(|f| f.source_record_id() == current)
                    .unwrap();
                let FactPayload::Provenance(chosen) = selected_fact.payload() else {
                    panic!()
                };
                let history: Vec<_> = all
                    .iter()
                    .filter_map(|f| match f.payload() {
                        FactPayload::Provenance(r)
                            if r.event_id == v.event_id
                                && r.endpoint_id == v.endpoint_id
                                && r.observer_id == v.observer_id
                                && r.access_scope_id == v.access_scope_id
                                && f.visibility_scope_id() == fact.visibility_scope_id() =>
                        {
                            Some(r.received_at.get())
                        }
                        _ => None,
                    })
                    .collect();
                witnesses.push(json!({"event_id":v.event_id.to_string(),"visibility_scope":binding_name(fact.visibility_scope_id()),"endpoint":binding_name(v.endpoint_id),"observer":binding_name(v.observer_id),"access_scope":binding_name(v.access_scope_id),"selected_received_at_us":chosen.received_at.get(),"history_received_at_us":history,"selected_clock_quality":"fixture-clock","selected_verification_method":binding_name(chosen.verification_method_id)}));
            }
        }
    }
    assert_eq!(
        json!({"event_dictionary_values":mappings,"event_payloads":payloads,"event_eligibility":eligibility,"event_receive":receives,"provenance":witnesses}),
        *expected
    );
}
#[test]
fn all_fifteen_independently_signed_fixture_operations_match_native_inventory() {
    use serde_json::Value;
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/ingest/signed-event-admission.json"
    ))
    .unwrap();
    let mut executed = 0;
    for scenario in fixture["scenarios"].as_array().unwrap() {
        let provenance = scenario["provenance_enabled"].as_bool().unwrap();
        let (_root, mut store, budget, _manifest, _native) = setup(provenance);
        let mut receipts = std::collections::BTreeMap::new();
        for operation in scenario["operations"].as_array().unwrap() {
            let input = &operation["input"];
            let expected = &operation["expected"];
            let before = facts(&store);
            let before_kv = kv(&store);
            let request = ProducerRequestId {
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
                request,
                fixture_intent(input),
                256 * 1024,
                grant(&budget, MIB, 0),
            )
            .unwrap();
            let result = store.admit_event(
                prepared,
                ObservedTime::new(input["owner_recorded_at_us"].as_i64().unwrap()),
                provenance,
                AdmissionLimits::default(),
                grant(&budget, 40 * MIB, 10 * MIB),
                || Ok(()),
            );
            let name = operation["name"].as_str().unwrap();
            if expected["result"] == "RequestConflict" {
                assert!(
                    matches!(result, Err(AdmissionError::RequestConflict)),
                    "{name}"
                );
                assert_eq!(kv(&store), before_kv);
            } else {
                let outcome = result.unwrap();
                match outcome.outcome() {
                    AdmissionOutcome::Rejected(reason) => {
                        assert_eq!(expected["result"], "Invalid", "{name}");
                        assert_eq!(*reason, RejectionReason::Signature);
                    }
                    AdmissionOutcome::Durable(receipt) => {
                        assert_eq!(
                            expected["result"],
                            if receipt.batch().is_some() {
                                "NewFacts"
                            } else {
                                "AlreadyDurable"
                            },
                            "{name}"
                        );
                        let bytes = receipt.encode_persistent(65536).unwrap();
                        if let Some(prior) = expected["same_stored_result_as"].as_str() {
                            assert_eq!(&bytes, receipts.get(prior).unwrap());
                            assert_eq!(kv(&store), before_kv);
                        }
                        receipts.insert(name.to_string(), bytes);
                    }
                }
            }
            let after = facts(&store);
            let new = &after[before.len()..];
            let scientific_facts: Vec<_> = new
                .iter()
                .filter(|f| !matches!(f.payload(), FactPayload::Dictionary(_)))
                .collect();
            let deltas = expected["scientific_deltas"].as_array().unwrap();
            assert_eq!(scientific_facts.len(), deltas.len(), "{name}");
            let dictionaries: Vec<_> = new
                .iter()
                .filter_map(|f| match f.payload() {
                    FactPayload::Dictionary(v) => Some(v),
                    _ => None,
                })
                .collect();
            assert_eq!(
                dictionaries.len(),
                expected["dictionary_deltas"].as_array().unwrap().len()
            );
            for (fact, expected_fact) in scientific_facts.iter().zip(deltas) {
                let mut expected_fact = expected_fact.clone();
                expected_fact
                    .as_object_mut()
                    .unwrap()
                    .remove("body_from_event_ref");
                expected_fact
                    .as_object_mut()
                    .unwrap()
                    .remove("event_ordinal");
                assert_eq!(scientific(fact), expected_fact, "{name}");
                assert_eq!(
                    fact.recorded_at().get(),
                    input["owner_recorded_at_us"].as_i64().unwrap()
                );
                if let Some(corrects) = fact.corrects() {
                    let prior = before
                        .iter()
                        .rev()
                        .find(|f| {
                            f.record_kind() == fact.record_kind()
                                && f.visibility_scope_id() == fact.visibility_scope_id()
                                && f.payload().event_id() == fact.payload().event_id()
                        })
                        .unwrap();
                    assert_eq!(corrects, prior.source_record_id());
                }
                if let FactPayload::EventPayload(v) = fact.payload() {
                    let supplied = fixture_event(input["event_ref"].as_str().unwrap());
                    assert_eq!(v.body, EventBody::from(supplied));
                    assert_eq!(dictionaries[0].reference(), v.event_ordinal);
                }
                match fact.payload() {
                    FactPayload::EventReceive(v) => assert_eq!(
                        v.clock_quality,
                        fixture_intent(input).observation().unwrap().clock_quality
                    ),
                    FactPayload::Provenance(v) => assert_eq!(
                        v.clock_quality,
                        fixture_intent(input).observation().unwrap().clock_quality
                    ),
                    _ => {}
                }
            }
            assert_inventory(
                &store,
                &fixture["expected_inventory_states"][operation["expected_inventory_state_after"]
                    .as_str()
                    .unwrap()],
            );
            if !provenance {
                assert!(
                    after
                        .iter()
                        .all(|f| !matches!(f.payload(), FactPayload::Provenance(_)))
                );
                assert!(after.iter().all(|f| match f.payload() {
                    FactPayload::Dictionary(v) => !matches!(
                        v.descriptor(),
                        DictionaryDescriptor::Endpoint(_)
                            | DictionaryDescriptor::AccessScope(_)
                            | DictionaryDescriptor::EventVerification
                    ),
                    _ => true,
                }));
            }
            executed += 1;
        }
    }
    assert_eq!(executed, 15);
}

fn admit(
    store: &mut IngestStore,
    budget: &ResourceBudget,
    sequence: u64,
) -> Result<StoredOutcome, AdmissionError> {
    let input = PreparedEvent::new(
        request(5, sequence),
        intent("E_private", 2, 10_000_000),
        256 * 1024,
        grant(budget, MIB, 0),
    )
    .unwrap();
    store.admit_event(
        input,
        ObservedTime::new(10_000_000),
        false,
        AdmissionLimits::default(),
        grant(budget, 40 * MIB, 10 * MIB),
        || Ok(()),
    )
}

#[test]
fn retained_replay_with_reduced_dependency_limit_is_not_corrupt_state() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/ingest/signed-event-admission.json"
    ))
    .unwrap();
    let operation = fixture["scenarios"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|s| s["operations"].as_array().unwrap())
        .find(|op| op["name"] == "private_first_signature_a")
        .unwrap();
    let (_root, mut store, budget, _manifest, _native) = setup(true);
    let input = &operation["input"];
    let request = ProducerRequestId {
        producer_slot_id: input["request_id"]["producer_slot_id"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap(),
        producer_epoch: ProducerEpoch::new(1).unwrap(),
        request_sequence: RequestSequence::new(1).unwrap(),
    };
    let replay = |store: &mut IngestStore, max_dependencies| {
        let prepared = PreparedEvent::new(
            request,
            fixture_intent(input),
            256 * 1024,
            grant(&budget, MIB, 0),
        )
        .unwrap();
        store.admit_event(
            prepared,
            ObservedTime::new(10_000_000),
            true,
            AdmissionLimits {
                max_dependencies,
                ..AdmissionLimits::default()
            },
            grant(&budget, 40 * MIB, 10 * MIB),
            || Ok(()),
        )
    };
    let original = replay(&mut store, 64).unwrap();
    let AdmissionOutcome::Durable(receipt) = original.outcome() else {
        panic!("valid signed fixture")
    };
    assert_eq!(receipt.dependencies().records().len(), 11);
    let encoded = receipt.encode_persistent(8192).unwrap();
    drop(original);
    let before = kv(&store);
    assert_eq!(replay(&mut store, 8).err(), Some(AdmissionError::Limit));
    assert_eq!(kv(&store), before);
    let again = replay(&mut store, 64).unwrap();
    let AdmissionOutcome::Durable(receipt) = again.outcome() else {
        panic!("retained result")
    };
    assert_eq!(receipt.encode_persistent(8192).unwrap(), encoded);
    assert_eq!(kv(&store), before);
}
#[test]
fn inconsistent_counter_or_scope_inventory_cannot_recreate_scientific_state() {
    use crate::storage::rocks::keys::IngestKey;
    for fault in ["future-counter", "missing-eligibility", "missing-payload"] {
        let (_root, mut store, budget, _manifest, _native) = setup(false);
        drop(admit(&mut store, &budget, 1).unwrap());
        let cf = store.db.cf_handle("default").unwrap();
        let dataset_id = reference(DictionaryKind::Event, 1).dataset_id;
        let event_id = fixture_event("E_private").id;
        match fault {
            "future-counter" => {
                let key = IngestKey::SourceStream {
                    dataset_id,
                    shard_id: 0,
                    owner_epoch: OwnerEpoch::new(1).unwrap(),
                }
                .encode()
                .unwrap();
                let mut value = vec![1, 2];
                value.extend_from_slice(&100u64.to_be_bytes());
                store.db.put_cf(cf, key.as_bytes(), value).unwrap();
            }
            "missing-eligibility" => {
                let key = IngestKey::EventEligibility {
                    dataset_id,
                    event_id,
                    visibility_scope: reference(DictionaryKind::VisibilityScope, 2),
                }
                .encode()
                .unwrap();
                store.db.delete_cf(cf, key.as_bytes()).unwrap();
            }
            "missing-payload" => {
                let key = IngestKey::EventPayload {
                    dataset_id,
                    event_id,
                }
                .encode()
                .unwrap();
                store.db.delete_cf(cf, key.as_bytes()).unwrap();
            }
            _ => unreachable!(),
        }
        let before = kv(&store);
        assert!(
            matches!(
                admit(&mut store, &budget, 2),
                Err(AdmissionError::StateMismatch)
            ),
            "{fault}"
        );
        assert_eq!(kv(&store), before);
    }
}

#[test]
fn invalid_signature_has_only_strict_operational_detail_and_replays_after_reopen() {
    let (root, mut store, budget, manifest, native) = setup(false);
    let before = facts(&store);
    let invalid = || {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/ingest/signed-event-admission.json"
        ))
        .unwrap();
        let mut input = fixture["scenarios"][0]["operations"][4]["input"].clone();
        input["observation"]["witness"] = serde_json::Value::Null;
        fixture_intent(&input)
    };
    let input =
        PreparedEvent::new(request(5, 1), invalid(), 256 * 1024, grant(&budget, MIB, 0)).unwrap();
    let result = store
        .admit_event(
            input,
            ObservedTime::new(55),
            false,
            AdmissionLimits::default(),
            grant(&budget, 40 * MIB, 10 * MIB),
            || Ok(()),
        )
        .unwrap();
    assert!(matches!(
        result.outcome(),
        AdmissionOutcome::Rejected(RejectionReason::Signature)
    ));
    drop(result);
    assert_eq!(facts(&store), before);
    let golden: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/ingest/rejected-request.json"
    ))
    .unwrap();
    let detail_key = crate::storage::rocks::keys::IngestKey::Request {
        request_id: request(5, 1),
    }
    .encode()
    .unwrap();
    let detail = store
        .db
        .get_cf(
            store.db.cf_handle("receipts").unwrap(),
            detail_key.as_bytes(),
        )
        .unwrap()
        .unwrap();
    let hex: String = detail.iter().map(|b| format!("{b:02x}")).collect();
    assert_eq!(hex, golden["stored_value_hex"]);
    let persisted = kv(&store);
    drop(store);
    let mut store = IngestStore::reopen(
        &root.path().join("db"),
        manifest,
        native,
        grant(&budget, 0, 128 * MIB),
    )
    .unwrap();
    let input =
        PreparedEvent::new(request(5, 1), invalid(), 256 * 1024, grant(&budget, MIB, 0)).unwrap();
    let retry = store
        .admit_event(
            input,
            ObservedTime::new(999),
            false,
            AdmissionLimits::default(),
            grant(&budget, 40 * MIB, 10 * MIB),
            || Ok(()),
        )
        .unwrap();
    assert!(matches!(
        retry.outcome(),
        AdmissionOutcome::Rejected(RejectionReason::Signature)
    ));
    drop(retry);
    assert_eq!(kv(&store), persisted);
    assert!(matches!(
        admit(&mut store, &budget, 1),
        Err(AdmissionError::RequestConflict)
    ));
    assert_eq!(kv(&store), persisted);
    let prefix_length = detail.len() - 89;
    for fault in ["short", "long", "version", "request", "dataset", "reason"] {
        let mut damaged = detail.clone();
        match fault {
            "short" => {
                damaged.pop();
            }
            "long" => damaged.push(0),
            "version" => damaged[prefix_length - 2] = b'2',
            "request" => damaged[prefix_length] ^= 1,
            "dataset" => damaged[prefix_length + 64] ^= 1,
            "reason" => damaged[prefix_length + 88] = 255,
            _ => unreachable!(),
        }
        store
            .db
            .put_cf(
                store.db.cf_handle("receipts").unwrap(),
                detail_key.as_bytes(),
                damaged,
            )
            .unwrap();
        let before = kv(&store);
        let input =
            PreparedEvent::new(request(5, 1), invalid(), 256 * 1024, grant(&budget, MIB, 0))
                .unwrap();
        let result = store.admit_event(
            input,
            ObservedTime::new(1000),
            false,
            AdmissionLimits::default(),
            grant(&budget, 40 * MIB, 10 * MIB),
            || Ok(()),
        );
        let expected = if fault == "reason" {
            AdmissionError::IncompatibleSchema
        } else {
            AdmissionError::StateMismatch
        };
        assert!(matches!(result, Err(error) if error == expected), "{fault}");
        assert_eq!(kv(&store), before, "{fault}");
    }
}
#[test]
fn missing_receipt_or_retained_fact_is_never_a_new_request_or_successful_replay() {
    use crate::storage::rocks::keys::IngestKey;
    for fault in ["receipt", "dependency"] {
        let (_root, mut store, budget, _manifest, _native) = setup(false);
        let outcome = admit(&mut store, &budget, 1).unwrap();
        let AdmissionOutcome::Durable(receipt) = outcome.outcome() else {
            panic!()
        };
        let key = if fault == "receipt" {
            IngestKey::Request {
                request_id: request(5, 1),
            }
        } else {
            IngestKey::Fact {
                source_record_id: receipt.batch().unwrap().records()[1].source_record_id,
            }
        }
        .encode()
        .unwrap();
        drop(outcome);
        store
            .db
            .delete_cf(
                store.db.cf_handle(key.column_family().name()).unwrap(),
                key.as_bytes(),
            )
            .unwrap();
        let before = kv(&store);
        assert!(
            matches!(
                admit(&mut store, &budget, 1),
                Err(AdmissionError::StateMismatch)
            ),
            "{fault}"
        );
        assert_eq!(kv(&store), before);
    }
}
#[test]
fn last_authority_check_and_bounded_outstanding_window_leave_no_partial_effects() {
    let (_root, mut store, budget, _manifest, _native) = setup(false);
    let before = kv(&store);
    let mut checks = 0;
    let input = PreparedEvent::new(
        request(5, 1),
        intent("E_private", 2, 10_000_000),
        256 * 1024,
        grant(&budget, MIB, 0),
    )
    .unwrap();
    let result = store.admit_event(
        input,
        ObservedTime::new(10),
        false,
        AdmissionLimits::default(),
        grant(&budget, 40 * MIB, 10 * MIB),
        || {
            checks += 1;
            if checks == 3 {
                Err(AdmissionError::AuthorityUnavailable)
            } else {
                Ok(())
            }
        },
    );
    assert!(matches!(result, Err(AdmissionError::AuthorityUnavailable)));
    assert_eq!(checks, 3);
    assert_eq!(kv(&store), before);
    drop(admit(&mut store, &budget, 1).unwrap());
    let before = kv(&store);
    let input = PreparedEvent::new(
        request(5, 2),
        intent("E_private", 2, 10_000_000),
        256 * 1024,
        grant(&budget, MIB, 0),
    )
    .unwrap();
    let result = store.admit_event(
        input,
        ObservedTime::new(11),
        false,
        AdmissionLimits {
            max_outstanding_requests: 1,
            ..AdmissionLimits::default()
        },
        grant(&budget, 40 * MIB, 10 * MIB),
        || Ok(()),
    );
    assert!(matches!(result, Err(AdmissionError::WindowFull)));
    assert_eq!(kv(&store), before);
}

#[test]
fn signed_pending_reopens_and_reaches_the_supervised_native_worker_without_acknowledgement() {
    use crate::storage::archive::{
        ArtifactIdentity, Preparation, PreparationLimits, WorkerInstallation,
    };
    use std::{
        sync::atomic::AtomicBool,
        time::{Duration, Instant},
    };
    let installation = WorkerInstallation {
        root: std::env::var_os("BIGBROTR_ARCHIVE_TEST_INSTALLATION")
            .expect("required packaged native worker")
            .into(),
        executable: ArtifactIdentity {
            bytes: std::env::var("BIGBROTR_ARCHIVE_TEST_WORKER_BYTES")
                .unwrap()
                .parse()
                .unwrap(),
            sha256: std::env::var("BIGBROTR_ARCHIVE_TEST_WORKER_SHA256")
                .unwrap()
                .parse()
                .unwrap(),
        },
    };
    let (root, mut store, budget, manifest, native) = setup(false);
    let result = admit(&mut store, &budget, 1).unwrap();
    let AdmissionOutcome::Durable(receipt) = result.outcome() else {
        panic!("signed fixture must commit");
    };
    let first = receipt.batch().unwrap().first();
    let expected = receipt.batch().unwrap().clone();
    drop(result);
    drop(store);
    let store = IngestStore::reopen(
        &root.path().join("db"),
        manifest,
        native,
        grant(&budget, 0, 128 * MIB),
    )
    .unwrap();
    let before = kv(&store);
    let cancel = AtomicBool::new(false);
    let pending = store
        .read_pending_batch(
            first,
            PendingReadLimits {
                max_records: 16,
                max_encoded_bytes: MIB as usize,
                fact: bigbrotr_model::codec::FactDecodeLimits::default(),
            },
            grant(&budget, 16 * MIB, 0),
            &cancel,
        )
        .unwrap()
        .unwrap();
    assert_eq!(pending.batch(), &expected);
    let worker_budget = ResourceBudget::new(
        ProcessEnvelope::new(
            513 * MIB,
            0,
            ResourceLimits::new(0, 1, 512 * MIB, MIB),
            WaiterLimits::new(0, 0),
        )
        .unwrap(),
    );
    let vectors: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/model/archive-signed-pending.json"
    ))
    .unwrap();
    assert_eq!(vectors["vectors"].as_array().unwrap().len(), 2);
    for vector in vectors["vectors"].as_array().unwrap() {
        let topology = TopologyGeneration::new(
            vector["topology_generation"]
                .as_str()
                .unwrap()
                .parse()
                .unwrap(),
        );
        use std::os::unix::fs::PermissionsExt;
        let directory = tempfile::Builder::new()
            .permissions(std::fs::Permissions::from_mode(0o700))
            .tempdir()
            .unwrap();
        let temp = directory.path().canonicalize().unwrap();
        let result = Preparation {
            installation: &installation,
            temp_root: &temp,
            limits: PreparationLimits {
                memory_bytes: 128 * MIB,
                temp_bytes: 64 * MIB,
                threads: 1,
                max_records: 16,
                max_encoded_bytes: MIB as usize,
                native_reserve_bytes: 384 * MIB,
            },
            deadline: Instant::now() + Duration::from_secs(15),
            cancelled: &cancel,
        }
        .inspect_pending(&pending, topology, &worker_budget)
        .unwrap();
        assert_eq!(result.staged_counts, [1, 1, 1, 1, 0]);
        assert_eq!(
            result.archive_batch_digest.to_string(),
            vector["sha256"].as_str().unwrap()
        );
        assert_eq!(worker_budget.snapshot().active_permits(), 0);
        assert_eq!(kv(&store), before);
        assert_eq!(pending.batch(), &expected);
    }
    // Reduced transfer bounds must remain a resource refusal, even though the
    // worker would separately reject the resulting empty/truncated transport.
    use crate::storage::archive::PreparationError;
    use std::os::unix::fs::PermissionsExt;
    let directory = tempfile::Builder::new()
        .permissions(std::fs::Permissions::from_mode(0o700))
        .tempdir()
        .unwrap();
    let temp = directory.path().canonicalize().unwrap();
    let limits = PreparationLimits {
        memory_bytes: 128 * MIB,
        temp_bytes: 64 * MIB,
        threads: 1,
        max_records: 1,
        max_encoded_bytes: MIB as usize,
        native_reserve_bytes: 384 * MIB,
    };
    let result = Preparation {
        installation: &installation,
        temp_root: &temp,
        limits,
        deadline: Instant::now() + Duration::from_secs(15),
        cancelled: &cancel,
    }
    .inspect_pending(&pending, TopologyGeneration::new(1), &worker_budget);
    assert_eq!(
        result.err(),
        Some(PreparationError::Transfer(
            bigbrotr_protocol::archive_batch::Error::Limit
        ))
    );
    assert_eq!(worker_budget.snapshot().active_permits(), 0);
    assert_eq!(kv(&store), before);
    for case in [
        "wrong-executable",
        "cancelled",
        "nonempty-root",
        "no-capacity",
    ] {
        use crate::storage::archive::InstallationError;
        let directory = tempfile::Builder::new()
            .permissions(std::fs::Permissions::from_mode(0o700))
            .tempdir()
            .unwrap();
        let temp = directory.path().canonicalize().unwrap();
        let mut selected = WorkerInstallation {
            root: installation.root.clone(),
            executable: installation.executable,
        };
        let cancel = AtomicBool::new(case == "cancelled");
        let expected_error = match case {
            "wrong-executable" => {
                selected.executable.sha256 = Digest256::from_bytes([0; 32]);
                PreparationError::Installation(InstallationError::HashMismatch)
            }
            "cancelled" => PreparationError::Installation(InstallationError::Cancelled),
            "nonempty-root" => {
                std::fs::write(temp.join("retained"), b"do not overwrite").unwrap();
                PreparationError::InvalidTemporaryRoot
            }
            "no-capacity" => {
                worker_budget.close();
                PreparationError::Unavailable
            }
            _ => unreachable!(),
        };
        let result = Preparation {
            installation: &selected,
            temp_root: &temp,
            limits: PreparationLimits {
                max_records: 16,
                ..limits
            },
            deadline: Instant::now() + Duration::from_secs(15),
            cancelled: &cancel,
        }
        .inspect_pending(&pending, TopologyGeneration::new(1), &worker_budget);
        assert_eq!(result.err(), Some(expected_error), "{case}");
        assert_eq!(worker_budget.snapshot().active_permits(), 0);
        assert_eq!(kv(&store), before);
        if case == "nonempty-root" {
            assert_eq!(
                std::fs::read(temp.join("retained")).unwrap(),
                b"do not overwrite"
            );
        } else {
            assert!(std::fs::read_dir(temp).unwrap().next().is_none());
        }
    }
}

#[path = "certification_tests.rs"]
mod certification_tests;

#[path = "producer_closing_tests.rs"]
mod producer_closing_tests;

#[path = "producer_recovery_tests.rs"]
mod producer_recovery_tests;
