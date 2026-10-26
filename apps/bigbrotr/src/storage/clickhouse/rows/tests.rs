use super::*;
use bigbrotr_model::{
    AccessClass, AccessScopeDescriptor, AllocationDomainId, ClockQuality, ClockSource,
    CompleteThrough, DatasetId, DatasetLineageDescriptor, DescriptorLabel, DictionaryDescriptor,
    DictionaryEntry, DictionaryKind, Digest256, DurationUs, EndpointDescriptor, EventBody,
    EventEligibility, EventId, EventOrigin, EventPayload, EventReceive, FactPayload, NostrTime,
    ObservedTime, ObserverDescriptor, Ordinal, OwnerEpoch, Provenance, PublicKey, RouteClass,
    Sequence, Signature, SourceCut, StreamBound, StreamId, TopologyGeneration,
    codec::FactDecodeLimits,
};
use std::collections::BTreeMap;

#[derive(Deserialize)]
struct ExpectedChunk {
    row_count: u64,
    rows_digest: Digest256,
    row_binary_bytes: u64,
    empty_digest: Digest256,
}
#[derive(Deserialize)]
struct Golden {
    fact_digests: Vec<Digest256>,
    body_digest: Digest256,
    targets: BTreeMap<String, ExpectedChunk>,
}
fn golden() -> Golden {
    serde_json::from_str(include_str!("fixtures/golden.json")).unwrap()
}
fn dataset() -> DatasetId {
    DatasetId::try_from([0x11; 16]).unwrap()
}
fn generation() -> ProjectionGenerationId {
    ProjectionGenerationId::try_from([0x33; 16]).unwrap()
}
fn reference(kind: DictionaryKind, ordinal: u64) -> QualifiedRef {
    QualifiedRef {
        dataset_id: dataset(),
        dictionary_kind: kind,
        allocation_domain: AllocationDomainId::try_from([0x22; 16]).unwrap(),
        ordinal: Ordinal::new(ordinal).unwrap(),
    }
}
fn source(epoch: u64, sequence: u64) -> SourceRecordId {
    SourceRecordId {
        dataset_id: dataset(),
        shard_id: 7,
        owner_epoch: OwnerEpoch::new(epoch).unwrap(),
        sequence: Sequence::new(sequence).unwrap(),
    }
}
fn fact(sequence: u64, payload: FactPayload) -> Fact {
    Fact::new(
        source(3, sequence),
        reference(DictionaryKind::VisibilityScope, 9),
        ObservedTime::new(i64::MIN + sequence as i64),
        payload,
    )
    .unwrap()
}
fn label(value: &str) -> DescriptorLabel {
    DescriptorLabel::new(value.into()).unwrap()
}
fn cut() -> SourceCut {
    SourceCut::new(
        dataset(),
        TopologyGeneration::new(17),
        vec![
            StreamBound {
                stream: StreamId {
                    shard_id: 7,
                    owner_epoch: OwnerEpoch::new(2).unwrap(),
                },
                last_complete_sequence: CompleteThrough::new(100),
            },
            StreamBound {
                stream: StreamId {
                    shard_id: 7,
                    owner_epoch: OwnerEpoch::new(3).unwrap(),
                },
                last_complete_sequence: CompleteThrough::new(5),
            },
        ],
    )
    .unwrap()
}

// These synthetic inputs exercise structural mapping. Cryptographic admission
// remains the protocol owner's responsibility, including its empty-tag refusal.
fn facts() -> [Fact; 5] {
    let event_id = EventId::from_bytes([0xaa; 32]);
    let observer = reference(DictionaryKind::Observer, 5);
    let method = reference(DictionaryKind::Method, 10);
    let dictionary = DictionaryEntry::new(
        observer,
        DictionaryDescriptor::Observer(ObserverDescriptor {
            clock: Some(ClockSource::ExternalUtc),
            vantage: Some(label("lab-a")),
            software: Some(label("BigBrotr")),
            method_id: Some(method),
            public_key: Some(PublicKey::from_bytes([0xbb; 32])),
            route_class: Some(RouteClass::Tor),
            software_version: Some(label("v1")),
        }),
    )
    .unwrap();
    let tags: &[&[&str]] = &[
        &["t", "z", "ignored"],
        &["e", "α"],
        &["t", "a"],
        &["t", "z"],
        &["T", ""],
        &["ab", "skip"],
        &["1", "skip"],
        &["é", "skip"],
        &["t"],
        &[],
        &["p", "first", "second"],
        &["p", "first"],
        &["x", "\0\n"],
    ];
    let body = EventBody {
        event_id,
        pubkey: PublicKey::from_bytes([0xbb; 32]),
        created_at: NostrTime(u64::MAX),
        kind: u16::MAX,
        tags: tags
            .iter()
            .map(|tag| tag.iter().map(|value| (*value).into()).collect())
            .collect(),
        content: "{\"text\": \"still a string\"}\n\0é".into(),
    };
    [
        fact(1, FactPayload::Dictionary(dictionary)),
        fact(
            2,
            FactPayload::EventPayload(EventPayload {
                body,
                event_ordinal: reference(DictionaryKind::Event, 42),
            }),
        ),
        fact(
            3,
            FactPayload::EventEligibility(EventEligibility {
                event_id,
                body_digest: golden().body_digest,
                sig: Signature::from_bytes([0xcc; 64]),
                first_eligible_at: ObservedTime::new(-44),
                origin: EventOrigin::Import(reference(DictionaryKind::DatasetLineage, 11)),
            }),
        ),
        fact(
            4,
            FactPayload::EventReceive(EventReceive {
                event_id,
                received_at: ObservedTime::new(-100),
                observer_id: observer,
                clock_quality: ClockQuality::new(
                    reference(DictionaryKind::Observer, 6),
                    DurationUs::new(1),
                    None,
                    5,
                )
                .unwrap(),
                corrects: Some(source(2, 99)),
            }),
        ),
        fact(
            5,
            FactPayload::Provenance(Provenance {
                event_id,
                endpoint_id: reference(DictionaryKind::Endpoint, 7),
                observer_id: observer,
                access_scope_id: reference(DictionaryKind::AccessScope, 8),
                received_at: ObservedTime::new(-200),
                clock_quality: ClockQuality::new(
                    reference(DictionaryKind::Observer, 6),
                    DurationUs::new(1),
                    Some(DurationUs::new(2)),
                    7,
                )
                .unwrap(),
                verification_method_id: method,
                corrects: Some(source(2, 100)),
            }),
        ),
    ]
}

#[test]
fn all_ten_families_match_independent_canonical_and_wire_vectors() {
    let facts = facts();
    let golden = golden();
    let cut = cut();
    let expected_counts = [1, 1, 1, 1, 1, 1, 1, 6, 1, 1];
    let mut counts = [0; 10];
    let mut hashes = std::array::from_fn(|i| {
        ChunkHasher::new(generation(), IndexTarget::ALL[i], &cut, expected_counts[i])
    });
    for (i, fact) in facts.iter().enumerate() {
        assert_eq!(fact.payload_digest(), golden.fact_digests[i]);
        let rows = FactRows::new(generation(), fact, true, FactDecodeLimits::default()).unwrap();
        for (count, added) in counts.iter_mut().zip(rows.counts()) {
            *count += added;
        }
        rows.hash_into(&mut hashes).unwrap();
    }
    assert_eq!(counts, expected_counts);
    for (target, hash) in IndexTarget::ALL.into_iter().zip(hashes) {
        let actual = hash.finish().unwrap();
        let expected = &golden.targets[target.as_str()];
        assert_eq!(actual.target, target);
        assert_eq!(
            actual.row_count,
            expected.row_count,
            "{} count",
            target.as_str()
        );
        assert_eq!(
            actual.rows_digest,
            expected.rows_digest,
            "{} canonical bytes",
            target.as_str()
        );
        assert_eq!(
            actual.row_binary_bytes,
            expected.row_binary_bytes,
            "{} wire allocation",
            target.as_str()
        );
    }
}

#[test]
fn body_is_borrowed_and_tag_postings_are_exact_sorted_deduplicated_first_values() {
    let facts = facts();
    let rows = FactRows::new(generation(), &facts[1], true, FactDecodeLimits::default()).unwrap();
    let row = rows.events().unwrap();
    let FactPayload::EventPayload(payload) = facts[1].payload() else {
        unreachable!()
    };
    assert!(matches!(row.tags, Cow::Borrowed(_)));
    assert!(matches!(row.content, Cow::Borrowed(_)));
    assert_eq!(row.tags.as_ptr(), payload.body.tags.as_ptr());
    assert_eq!(row.content.as_ptr(), payload.body.content.as_ptr());
    assert_eq!(row.created_at, u64::MAX);
    assert_eq!(row.kind, u16::MAX);
    assert_eq!(row.source, ([0x11; 16], 7, 3, 2));
    assert_eq!(row.visibility_scope, ([0x11; 16], 4, [0x22; 16], 9));
    assert_eq!(row.event_ordinal, ([0x11; 16], 8, [0x22; 16], 42));
    assert_eq!(row.body_digest, *golden().body_digest.as_bytes());
    let postings: Vec<_> = rows
        .event_tag_index()
        .map(|row| {
            assert_eq!(row.source, ([0x11; 16], 7, 3, 2));
            assert_eq!(row.event_id, [0xaa; 32]);
            (
                row.discriminator,
                row.tag_key.into_owned(),
                row.tag_value.into_owned(),
            )
        })
        .collect();
    let expected = [
        ("T", ""),
        ("e", "α"),
        ("p", "first"),
        ("t", "a"),
        ("t", "z"),
        ("x", "\0\n"),
    ];
    assert_eq!(
        postings,
        expected
            .into_iter()
            .enumerate()
            .map(|(i, (key, value))| (i as u64, key.into(), value.into()))
            .collect::<Vec<_>>()
    );
    assert!(rows.tag_reference_bytes() >= 8 * std::mem::size_of::<(&str, &str)>());
    assert_eq!(payload.body.tags.len(), 13);
    assert!(payload.body.tags[9].is_empty());
}

#[test]
fn corrections_keep_previous_epoch_scope_and_distinct_clock_identity() {
    let facts = facts();
    let receive = FactRows::new(generation(), &facts[3], true, FactDecodeLimits::default())
        .unwrap()
        .event_receive_facts()
        .unwrap();
    assert_eq!(receive.corrects, Some(([0x11; 16], 7, 2, 99)));
    assert_eq!(receive.observer_id, ([0x11; 16], 2, [0x22; 16], 5));
    assert_eq!(receive.clock_id, ([0x11; 16], 2, [0x22; 16], 6));
    assert_eq!(
        (
            receive.resolution_us,
            receive.uncertainty_us,
            receive.anomaly_flags
        ),
        (1, None, 5)
    );
    assert_eq!(
        (receive.received_at, receive.recorded_at),
        (-100, i64::MIN + 4)
    );
    let witness = FactRows::new(generation(), &facts[4], true, FactDecodeLimits::default())
        .unwrap()
        .provenance()
        .unwrap();
    assert_eq!(witness.corrects, Some(([0x11; 16], 7, 2, 100)));
    assert_eq!(witness.access_scope_id, ([0x11; 16], 3, [0x22; 16], 8));
    assert_eq!(
        witness.verification_method_id,
        ([0x11; 16], 5, [0x22; 16], 10)
    );
    assert_eq!(
        (
            witness.resolution_us,
            witness.uncertainty_us,
            witness.anomaly_flags
        ),
        (1, Some(2), 7)
    );
}

#[test]
fn off_keeps_eligibility_and_receive_and_explicit_zero_witness_outcomes() {
    let facts = facts();
    let cut = cut();
    let mut counts = [0; 10];
    for fact in &facts[..4] {
        let rows = FactRows::new(generation(), fact, false, FactDecodeLimits::default()).unwrap();
        for (count, added) in counts.iter_mut().zip(rows.counts()) {
            *count += added;
        }
    }
    assert_eq!(counts, [1, 1, 1, 1, 1, 1, 1, 6, 0, 0]);
    assert!(matches!(
        FactRows::new(generation(), &facts[4], false, FactDecodeLimits::default()),
        Err(RowsError::ProvenanceDisabled)
    ));
    let golden = golden();
    for target in IndexTarget::ALL {
        let summary = ChunkHasher::new(generation(), target, &cut, 0)
            .finish()
            .unwrap();
        assert_eq!(
            summary.rows_digest,
            golden.targets[target.as_str()].empty_digest
        );
        assert_eq!(summary.row_count, 0);
        assert_eq!(summary.row_binary_bytes, 0);
    }
    assert_ne!(
        golden.targets["provenance_by_event"].empty_digest,
        golden.targets["provenance_by_relay"].empty_digest
    );
}

#[test]
fn mapping_refuses_fanout_before_reserving_tag_references() {
    let facts = facts();
    let limits = FactDecodeLimits {
        max_tag_nodes: 7,
        ..FactDecodeLimits::default()
    };
    assert!(matches!(
        FactRows::new(generation(), &facts[1], true, limits),
        Err(RowsError::TagLimit)
    ));
    let limits = FactDecodeLimits {
        max_allocation_bytes: 7 * std::mem::size_of::<(&str, &str)>(),
        ..FactDecodeLimits::default()
    };
    assert!(matches!(
        FactRows::new(generation(), &facts[1], true, limits),
        Err(RowsError::TagLimit)
    ));
}

#[test]
fn readback_hash_uses_actual_values_and_refuses_identity_conflicts() {
    let facts = facts();
    let cut = cut();
    let rows = FactRows::new(generation(), &facts[1], true, FactDecodeLimits::default()).unwrap();
    let original = rows.events().unwrap();
    let json = serde_json::to_value(&original).unwrap();
    let mut readback: EventRow<'static> = serde_json::from_value(json).unwrap();
    let mut hash = ChunkHasher::new(generation(), IndexTarget::Events, &cut, 1);
    hash.push(&readback).unwrap();
    assert_eq!(
        hash.finish().unwrap().rows_digest,
        golden().targets["events"].rows_digest
    );
    // The stored payload digest remains unchanged, while actual serving content
    // is corrupted. Verification must detect this even with an intact digest.
    readback.content = Cow::Borrowed("corrupt");
    let mut hash = ChunkHasher::new(generation(), IndexTarget::Events, &cut, 1);
    hash.push(&readback).unwrap();
    assert_ne!(
        hash.finish().unwrap().rows_digest,
        golden().targets["events"].rows_digest
    );
    let mut hash = ChunkHasher::new(generation(), IndexTarget::Events, &cut, 2);
    hash.push(&original).unwrap();
    assert_eq!(hash.push(&readback), Err(RowsError::UnorderedRows));
    assert_eq!(hash.finish(), Err(RowsError::RowCount));
    let mut hash = ChunkHasher::new(generation(), IndexTarget::EventTagIndex, &cut, 1);
    assert_eq!(hash.push(&original), Err(RowsError::WrongTarget));
    let mut hash = ChunkHasher::new(
        ProjectionGenerationId::try_from([0x44; 16]).unwrap(),
        IndexTarget::Events,
        &cut,
        1,
    );
    assert_eq!(hash.push(&original), Err(RowsError::WrongGeneration));
    let mut hash = ChunkHasher::new(generation(), IndexTarget::Events, &cut, 0);
    assert_eq!(hash.push(&original), Err(RowsError::RowCount));
    let mut foreign = original.clone();
    foreign.source.0 = [0x55; 16];
    let mut hash = ChunkHasher::new(generation(), IndexTarget::Events, &cut, 1);
    assert_eq!(hash.push(&foreign), Err(RowsError::OutsideCut));
}

#[test]
fn every_closed_row_supports_clickhouse_gat_read_and_write() {
    fn typed<R: clickhouse::RowRead + clickhouse::RowWrite>() {}
    typed::<DictionaryRow<'static>>();
    typed::<EventRow<'static>>();
    typed::<EligibilityRow<'static>>();
    typed::<ReceiveRow>();
    typed::<EventIndexRow>();
    typed::<TagRow<'static>>();
    typed::<ProvenanceRow>();
    let facts = facts();
    let row = FactRows::new(generation(), &facts[2], true, FactDecodeLimits::default())
        .unwrap()
        .event_eligibility_facts()
        .unwrap();
    let encoded = serde_json::to_value(&row).unwrap();
    assert_eq!(encoded["sig"].as_array().unwrap().len(), 64);
    assert_eq!(encoded["import_lineage"].as_array().unwrap().len(), 1);
    let decoded: EligibilityRow<'static> = serde_json::from_value(encoded.clone()).unwrap();
    assert_eq!(decoded, row);
    let mut invalid = encoded.clone();
    invalid["import_lineage"] =
        serde_json::json!([encoded["import_lineage"][0], encoded["import_lineage"][0]]);
    assert!(serde_json::from_value::<EligibilityRow<'static>>(invalid).is_err());
    let mut invalid = encoded;
    invalid["sig"].as_array_mut().unwrap().pop();
    assert!(serde_json::from_value::<EligibilityRow<'static>>(invalid).is_err());
}

#[test]
fn all_dictionary_descriptor_variants_keep_their_typed_fields_and_nulls() {
    let descriptors = [
        DictionaryDescriptor::Event(EventId::from_bytes([0xaa; 32])),
        DictionaryDescriptor::Visibility {
            class: label("private"),
        },
        DictionaryDescriptor::Endpoint(
            EndpointDescriptor::new("wss://relay.example/path?q=1".into()).unwrap(),
        ),
        DictionaryDescriptor::Observer(ObserverDescriptor {
            clock: None,
            vantage: None,
            software: None,
            method_id: None,
            public_key: None,
            route_class: None,
            software_version: None,
        }),
        DictionaryDescriptor::AccessScope(AccessScopeDescriptor {
            class: AccessClass::Protected,
            label: Some(label("restricted")),
            visibility_scope_id: reference(DictionaryKind::VisibilityScope, 9),
        }),
        DictionaryDescriptor::EventVerification,
        DictionaryDescriptor::DatasetLineage(DatasetLineageDescriptor {
            format: label("native"),
            format_version: 7,
            source_dataset_id: Some(DatasetId::try_from([0x66; 16]).unwrap()),
            source_artifact_digest: Digest256::from_bytes([0x77; 32]),
        }),
    ];
    // Only the listed semantic columns may be populated in each descriptor.
    let expected = [
        serde_json::json!({"event_id": vec![0xaa; 32]}),
        serde_json::json!({"class": "private"}),
        serde_json::json!({"url": "wss://relay.example/path?q=1"}),
        serde_json::json!({}),
        serde_json::json!({"class": "protected", "label": "restricted", "access_visibility_scope": [[vec![0x11; 16], 4, vec![0x22; 16], 9]]}),
        serde_json::json!({"input": "nip01-event/1", "algorithm": "sha256-bip340", "capability": "event-id-and-signature", "output_unit": "verification-result", "failure_taxonomy": "bigbrotr-nip01-verifier/1", "algorithm_version": 1, "protocol_revision": "a2494f4f81d46684e5814a9bf35e2b1df978f955"}),
        serde_json::json!({"format": "native", "format_version": 7, "source_dataset_id": vec![0x66; 16], "source_artifact_digest": vec![0x77; 32]}),
    ];
    for (descriptor, expected) in descriptors.into_iter().zip(expected) {
        let entry = DictionaryEntry::new(reference(descriptor.kind(), 21), descriptor).unwrap();
        let fact = fact(1, FactPayload::Dictionary(entry));
        let row = FactRows::new(generation(), &fact, false, FactDecodeLimits::default())
            .unwrap()
            .dictionary()
            .unwrap();
        let json = serde_json::to_value(&row).unwrap();
        let decoded: DictionaryRow<'static> = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(decoded, row);
        let mut fields = json.as_object().unwrap().clone();
        for name in [
            "projection_generation",
            "source",
            "record_schema_version",
            "visibility_scope",
            "recorded_at",
            "payload_digest",
            "discriminator",
            "dictionary_ref",
            "normalization_version",
            "descriptor_schema",
            "descriptor_version",
            "descriptor_digest",
        ] {
            fields.remove(name);
        }
        fields.retain(|_, value| !value.is_null() && !value.as_array().is_some_and(Vec::is_empty));
        assert_eq!(serde_json::Value::Object(fields), expected);
    }
}

#[test]
fn readback_tags_allow_admitted_wide_facts_above_convenience_defaults() {
    #[derive(Deserialize)]
    struct Inspection {
        #[serde(with = "serde_fields::tags")]
        tags: Cow<'static, [Vec<String>]>,
    }
    // 90,000 nodes exceed FactDecodeLimits::default().max_tag_nodes, but the
    // canonical body is far below the actual two-MiB admission ceiling.
    let mut json = String::from("{\"tags\":[");
    for i in 0..30_000 {
        if i != 0 {
            json.push(',');
        }
        json.push_str("[\"t\",\"\"]");
    }
    json.push_str("]}");
    let result: Inspection = serde_json::from_str(&json).unwrap();
    assert_eq!(result.tags.len(), 30_000);
    assert_eq!(
        result.tags.last().unwrap(),
        &[String::from("t"), String::new()]
    );
}

#[test]
fn readback_tags_reject_large_declared_arrays_before_visiting_elements() {
    use serde::de::value::{Error, SeqDeserializer};
    let mut visited = 0;
    let outer = (0..MAX_CORE_FACT_BYTES + 1).map(|_| {
        visited += 1;
        SeqDeserializer::<_, Error>::new(std::iter::empty::<&str>())
    });
    let result = serde_fields::tags::deserialize(SeqDeserializer::<_, Error>::new(outer));
    assert!(result.is_err());
    assert_eq!(visited, 0);

    let mut visited = 0;
    let strings = (0..MAX_CORE_FACT_BYTES).map(|_| {
        visited += 1;
        ""
    });
    let inner = SeqDeserializer::<_, Error>::new(strings);
    let outer = SeqDeserializer::<_, Error>::new(std::iter::once(inner));
    assert!(serde_fields::tags::deserialize(outer).is_err());
    assert_eq!(visited, 0);
}
