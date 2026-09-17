use bigbrotr_model::{Fact, codec::FactDecodeLimits};

fn bytes(hex: &str) -> Vec<u8> {
    hex.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

#[test]
fn dictionary_facts_decode_independent_literal_preimages() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/dictionary-v1.json")).unwrap();
    let rows = fixture["vectors"].as_array().unwrap();
    assert_eq!(rows.len(), 20);
    for row in rows.iter().filter(|row| row["family"] == "fact") {
        let input = bytes(row["preimage_hex"].as_str().unwrap());
        let digest = row["sha256"].as_str().unwrap().parse().unwrap();
        let fact = Fact::decode_preimage(&input, digest, FactDecodeLimits::default()).unwrap();
        assert_eq!(fact.record_kind(), "dictionary");
        assert_eq!(fact.payload_digest(), digest);
        assert_eq!(fact.encode_preimage(input.len()).unwrap(), input);
    }
}

use bigbrotr_model::{
    AccessClass, AccessScopeDescriptor, AllocationDomainId, ClockSource, DatasetId,
    DatasetLineageDescriptor, DescriptorLabel, DictionaryDescriptor, DictionaryEntry,
    DictionaryKind, Digest256, EndpointDescriptor, EventId, FactPayload, ObservedTime,
    ObserverDescriptor, Ordinal, OwnerEpoch, PublicKey, QualifiedRef, RouteClass, Sequence,
    SourceRecordId,
    codec::{CodecError, DictionaryDecodeLimits},
    dictionary::DictionaryError,
};
use sha2::{Digest, Sha256};

fn digest(value: &[u8]) -> Digest256 {
    Digest256::from_bytes(Sha256::digest(value).into())
}
fn reference(kind: DictionaryKind) -> QualifiedRef {
    QualifiedRef {
        dataset_id: DatasetId::try_from([1; 16]).unwrap(),
        dictionary_kind: kind,
        allocation_domain: AllocationDomainId::try_from([2; 16]).unwrap(),
        ordinal: Ordinal::new(1).unwrap(),
    }
}
fn label(value: &str) -> DescriptorLabel {
    DescriptorLabel::new(value.into()).unwrap()
}
fn descriptor(name: &str) -> DictionaryDescriptor {
    match name {
        "visibility-public" => DictionaryDescriptor::Visibility {
            class: label("public"),
        },
        "event" => DictionaryDescriptor::Event(EventId::from_bytes([0xaa; 32])),
        "endpoint" => DictionaryDescriptor::Endpoint(
            EndpointDescriptor::new("wss://relay.example/path?b=2&a=1".into()).unwrap(),
        ),
        "observer-known" => DictionaryDescriptor::Observer(ObserverDescriptor {
            clock: Some(ClockSource::SystemUtc),
            vantage: Some(label("lab-a")),
            software: Some(label("BigBrotr")),
            method_id: Some(reference(DictionaryKind::Method)),
            public_key: Some(PublicKey::from_bytes([0xbb; 32])),
            route_class: Some(RouteClass::Public),
            software_version: Some(label("0.1.0-dev")),
        }),
        "observer-unknown" => DictionaryDescriptor::Observer(ObserverDescriptor {
            clock: None,
            vantage: None,
            software: None,
            method_id: None,
            public_key: None,
            route_class: None,
            software_version: None,
        }),
        "access-public" | "access-protected" => {
            DictionaryDescriptor::AccessScope(AccessScopeDescriptor {
                class: if name == "access-public" {
                    AccessClass::Public
                } else {
                    AccessClass::Protected
                },
                label: (name == "access-protected").then(|| label("research-a")),
                visibility_scope_id: reference(DictionaryKind::VisibilityScope),
            })
        }
        "event-verification" => DictionaryDescriptor::EventVerification,
        "lineage-native" | "lineage-jsonl" => {
            DictionaryDescriptor::DatasetLineage(DatasetLineageDescriptor {
                format: label(if name == "lineage-native" {
                    "bigbrotr-native"
                } else {
                    "nostr-jsonl"
                }),
                format_version: 1,
                source_dataset_id: (name == "lineage-native")
                    .then(|| DatasetId::try_from([3; 16]).unwrap()),
                source_artifact_digest: Digest256::from_bytes(
                    [if name == "lineage-native" { 0xcc } else { 0xdd }; 32],
                ),
            })
        }
        _ => panic!("unknown fixture"),
    }
}
fn fixtures(family: &str) -> Vec<(String, Vec<u8>, Digest256)> {
    let value: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/dictionary-v1.json")).unwrap();
    value["vectors"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r["family"] == family)
        .map(|r| {
            (
                r["name"].as_str().unwrap().into(),
                bytes(r["preimage_hex"].as_str().unwrap()),
                r["sha256"].as_str().unwrap().parse().unwrap(),
            )
        })
        .collect()
}
fn decode(input: &[u8]) -> Result<DictionaryEntry, CodecError> {
    DictionaryEntry::decode_preimage(input, digest(input), DictionaryDecodeLimits::default())
}
fn source() -> SourceRecordId {
    SourceRecordId {
        dataset_id: reference(DictionaryKind::Event).dataset_id,
        shard_id: 0,
        owner_epoch: OwnerEpoch::new(1).unwrap(),
        sequence: Sequence::new(1).unwrap(),
    }
}
fn position(input: &[u8], part: &[u8]) -> usize {
    input.windows(part.len()).position(|v| v == part).unwrap()
}

#[test]
fn constructors_and_both_domains_match_every_independent_vector() {
    for (name, input, expected) in fixtures("dictionary") {
        let value = descriptor(&name);
        let entry = DictionaryEntry::new(reference(value.kind()), value).unwrap();
        assert_eq!(entry.digest(), expected, "{name}");
        assert_eq!(entry.encode_preimage(input.len()).unwrap(), input, "{name}");
        assert_eq!(decode(&input).unwrap(), entry, "{name}");
        assert_eq!(
            entry.encode_preimage(input.len() - 1),
            Err(CodecError::InputLimit)
        );
        let fact = Fact::new(
            source(),
            reference(DictionaryKind::VisibilityScope),
            ObservedTime::new(-1),
            FactPayload::Dictionary(entry.clone()),
        )
        .unwrap();
        let row = fixtures("fact").into_iter().find(|v| v.0 == name).unwrap();
        assert_eq!(fact.payload_digest(), row.2);
        assert_eq!(fact.encode_preimage(row.1.len()).unwrap(), row.1);
        assert_ne!(entry.digest(), fact.payload_digest());
        assert_eq!(
            fact.payload().event_id(),
            (name == "event").then(|| EventId::from_bytes([0xaa; 32]))
        );
    }
    let original: serde_json::Value = serde_json::from_str(include_str!(
        "../../../experiments/identity-contract/golden-vectors.json"
    ))
    .unwrap();
    let old = original["vectors"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["name"] == "dictionary")
        .unwrap();
    let public = fixtures("dictionary")
        .into_iter()
        .find(|v| v.0 == "visibility-public")
        .unwrap();
    assert_eq!(bytes(old["preimage_hex"].as_str().unwrap()), public.1);
    assert_eq!(old["sha256"].as_str().unwrap(), public.2.to_string());
}

#[test]
fn mappings_require_complete_identity_kind_namespace_and_value_equality() {
    let value = descriptor("event");
    let entry = DictionaryEntry::new(reference(DictionaryKind::Event), value.clone()).unwrap();
    assert!(entry.require_same_mapping(&entry.clone()).is_ok());
    let conflict = DictionaryEntry::new(
        entry.reference(),
        DictionaryDescriptor::Event(EventId::from_bytes([0xbb; 32])),
    )
    .unwrap();
    assert_eq!(
        entry.require_same_mapping(&conflict),
        Err(DictionaryError::MappingConflict)
    );
    for changed in [
        QualifiedRef {
            dataset_id: DatasetId::try_from([9; 16]).unwrap(),
            ..entry.reference()
        },
        QualifiedRef {
            allocation_domain: AllocationDomainId::try_from([9; 16]).unwrap(),
            ..entry.reference()
        },
        QualifiedRef {
            ordinal: Ordinal::new(u64::MAX).unwrap(),
            ..entry.reference()
        },
    ] {
        let other = DictionaryEntry::new(changed, value.clone()).unwrap();
        assert_eq!(
            entry.require_same_mapping(&other),
            Err(DictionaryError::DifferentReference)
        );
        assert_ne!(entry.digest(), other.digest());
        assert_eq!(entry.descriptor(), other.descriptor());
        let encoded = other.encode_preimage(4096).unwrap();
        assert_eq!(decode(&encoded).unwrap(), other);
    }
    for number in 1..=9 {
        let kind = DictionaryKind::try_from(number).unwrap();
        if kind != DictionaryKind::Event {
            assert!(DictionaryEntry::new(reference(kind), value.clone()).is_err());
        }
    }
    let foreign = QualifiedRef {
        dataset_id: DatasetId::try_from([9; 16]).unwrap(),
        ..reference(DictionaryKind::Event)
    };
    assert!(
        Fact::new(
            source(),
            reference(DictionaryKind::VisibilityScope),
            ObservedTime::new(0),
            FactPayload::Dictionary(DictionaryEntry::new(foreign, value).unwrap())
        )
        .is_err()
    );
    for name in ["observer-known", "access-public"] {
        let mut value = descriptor(name);
        match &mut value {
            DictionaryDescriptor::Observer(v) => {
                v.method_id.as_mut().unwrap().dataset_id = foreign.dataset_id
            }
            DictionaryDescriptor::AccessScope(v) => {
                v.visibility_scope_id.dataset_id = foreign.dataset_id
            }
            _ => unreachable!(),
        }
        assert!(DictionaryEntry::new(reference(value.kind()), value).is_err());
        let mut value = descriptor(name);
        match &mut value {
            DictionaryDescriptor::Observer(v) => {
                v.method_id.as_mut().unwrap().dictionary_kind = DictionaryKind::Endpoint
            }
            DictionaryDescriptor::AccessScope(v) => {
                v.visibility_scope_id.dictionary_kind = DictionaryKind::Endpoint
            }
            _ => unreachable!(),
        }
        assert!(DictionaryEntry::new(reference(value.kind()), value).is_err());
    }
}

#[test]
fn closure_contains_actual_dependencies_and_does_not_invent_attribution() {
    for (name, expected) in [
        ("event", vec![DictionaryKind::VisibilityScope]),
        ("observer-unknown", vec![DictionaryKind::VisibilityScope]),
        (
            "observer-known",
            vec![DictionaryKind::VisibilityScope, DictionaryKind::Method],
        ),
        (
            "access-public",
            vec![
                DictionaryKind::VisibilityScope,
                DictionaryKind::VisibilityScope,
            ],
        ),
        ("lineage-native", vec![DictionaryKind::VisibilityScope]),
    ] {
        let value = descriptor(name);
        let fact = Fact::new(
            source(),
            reference(DictionaryKind::VisibilityScope),
            ObservedTime::new(0),
            FactPayload::Dictionary(DictionaryEntry::new(reference(value.kind()), value).unwrap()),
        )
        .unwrap();
        assert_eq!(
            fact.dictionary_refs()
                .map(|v| v.dictionary_kind)
                .collect::<Vec<_>>(),
            expected
        );
    }
    for name in ["access-public", "access-protected", "lineage-native"] {
        let mut value = descriptor(name);
        match &mut value {
            DictionaryDescriptor::AccessScope(v) => {
                v.label = if v.label.is_some() {
                    None
                } else {
                    Some(label("invented"))
                }
            }
            DictionaryDescriptor::DatasetLineage(v) => v.format_version = 0,
            _ => unreachable!(),
        }
        assert!(DictionaryEntry::new(reference(value.kind()), value).is_err());
    }
    for invalid in [
        "",
        " ",
        "credential://abc",
        "user@example",
        "../path",
        "é",
        "line\nbreak",
    ] {
        assert!(DescriptorLabel::new(invalid.into()).is_err());
    }
    assert!(DescriptorLabel::new("a".repeat(64)).is_ok());
    assert!(DescriptorLabel::new("a".repeat(65)).is_err());
    for invalid in [
        "",
        "http://relay.example/",
        "wss:///",
        "wss://user:password@relay.example/",
        "wss://relay.example/#fragment",
        "wss://relay.example/\n",
        "wss://relay.example/with space",
    ] {
        assert!(EndpointDescriptor::new(invalid.into()).is_err());
    }
    let endpoint = EndpointDescriptor::new("wss://relay.example/path?opaque=value".into()).unwrap();
    assert!(!format!("{endpoint:?}").contains("opaque"));
    assert_eq!(endpoint.url(), "wss://relay.example/path?opaque=value");
    // This intentionally remains structural: the consumer must run the single
    // normalizer, credential classification and actual source/export policy.
    assert!(EndpointDescriptor::new("wss://UPPER.example/path".into()).is_ok());
}

// Independent bounded walker for the literal fixture corpus only. It never
// supplies expected product values or processes attacker input in production.
fn heads(input: &[u8], cursor: &mut usize, result: &mut Vec<(usize, usize, u8, u64)>) {
    let start = *cursor;
    let byte = input[*cursor];
    *cursor += 1;
    let major = byte & 0xe0;
    let extra = match byte & 31 {
        0..=23 => 0,
        24 => 1,
        25 => 2,
        26 => 4,
        27 => 8,
        _ => panic!("definite fixture required"),
    };
    let mut argument = u64::from(byte & 31);
    if extra > 0 {
        argument = 0;
        for _ in 0..extra {
            argument = argument * 256 + u64::from(input[*cursor]);
            *cursor += 1;
        }
    }
    result.push((start, *cursor, major, argument));
    match major {
        0 | 0x20 | 0xe0 => (),
        0x40 | 0x60 => *cursor += argument as usize,
        0x80 | 0xa0 => {
            for _ in 0..argument * if major == 0xa0 { 2 } else { 1 } {
                heads(input, cursor, result);
            }
        }
        _ => panic!("fixture type"),
    }
}

#[test]
fn every_truncation_wider_head_missing_null_and_incompatible_shape_is_refused() {
    let mut probes = 0;
    for (_, input, _) in fixtures("dictionary") {
        for length in 0..input.len() {
            assert!(decode(&input[..length]).is_err());
        }
        let mut cursor = b"bigbrotr/dictionary/1\0".len();
        let mut found = Vec::new();
        heads(&input, &mut cursor, &mut found);
        assert_eq!(cursor, input.len());
        for (start, end, major, value) in found {
            if major == 0xe0 && value == 22 {
                let mut missing = input.clone();
                missing.remove(start);
                assert!(decode(&missing).is_err());
                let mut undefined = input.clone();
                undefined[start] = 0xf7;
                assert!(decode(&undefined).is_err());
            } else {
                for extra in [1usize, 2, 4, 8] {
                    if extra < end - start {
                        continue;
                    }
                    let info = match extra {
                        1 => 24,
                        2 => 25,
                        4 => 26,
                        _ => 27,
                    };
                    let mut wide = input.clone();
                    let mut replace = vec![major | info];
                    replace.extend_from_slice(&value.to_be_bytes()[8 - extra..]);
                    wide.splice(start..end, replace);
                    assert_eq!(decode(&wide), Err(CodecError::NonCanonical));
                    probes += 1;
                }
            }
        }
        let mut trailing = input.clone();
        trailing.push(0);
        assert!(decode(&trailing).is_err());
        let mut indefinite = input.clone();
        indefinite[b"bigbrotr/dictionary/1\0".len()] = 0x9f;
        assert!(decode(&indefinite).is_err());
        assert_eq!(
            DictionaryEntry::decode_preimage(
                &input,
                Digest256::from_bytes([0; 32]),
                DictionaryDecodeLimits::default()
            ),
            Err(CodecError::DigestMismatch)
        );
    }
    assert!(
        probes > 0,
        "all literal descriptor heads must have been exercised"
    );
    eprintln!("dictionary non-shortest head probes: {probes}");
}

#[test]
fn closed_maps_reject_missing_unknown_duplicate_reordered_keys_and_new_versions() {
    let corpus = fixtures("dictionary");
    let public = &corpus
        .iter()
        .find(|r| r.0 == "visibility-public")
        .unwrap()
        .1;
    let map = position(public, &bytes("a165636c617373667075626c6963"));
    for replacement in [
        bytes("a0"),                                                     // missing class
        bytes("a1656f74686572667075626c6963"),                           // unknown key
        bytes("a265636c617373667075626c696365636c617373667075626c6963"), // duplicate
        bytes("bf65636c617373667075626c6963ff"),                         // indefinite map
        bytes("bbffffffffffffffff"), // malicious definite map length
        bytes("a165636c617373f6"),   // required label cannot be null
        bytes("a165636c6173737bffffffffffffffff"), // malicious text length
        bytes("a165636c6173738181818181818181818181"), // wrong/deep type
        bytes("a165636c61737361ff"), // invalid UTF-8
    ] {
        let mut input = public[..map].to_vec();
        input.extend(replacement);
        assert!(decode(&input).is_err());
    }
    // QualifiedRef kind is at domain + outer array + reference array + bytes16.
    let kind = b"bigbrotr/dictionary/1\0".len() + 19;
    for unsupported in [0, 1, 5, 6, 7, 10] {
        let mut input = public.clone();
        input[kind] = unsupported;
        assert_eq!(decode(&input), Err(CodecError::IncompatibleSchema));
    }
    let normalization = b"bigbrotr/dictionary/1\0".len() + 38;
    let schema = position(public, b"visibility");
    for offset in [normalization, schema + "visibility".len()] {
        for version in [0, 2] {
            let mut input = public.clone();
            input[offset] = version;
            assert_eq!(decode(&input), Err(CodecError::IncompatibleSchema));
        }
    }
    let mut unknown_schema = public.clone();
    unknown_schema[schema] = b'X';
    assert_eq!(decode(&unknown_schema), Err(CodecError::IncompatibleSchema));
    let unknown = &corpus.iter().find(|r| r.0 == "observer-unknown").unwrap().1;
    let map = position(unknown, &bytes("a765636c6f636bf6"));
    let mut reordered = unknown.clone();
    // Swap complete first two entries while preserving their values and count.
    reordered.splice(map + 1..map + 17, bytes("6776616e74616765f665636c6f636bf6"));
    assert!(decode(&reordered).is_err());
    let method = &corpus
        .iter()
        .find(|r| r.0 == "event-verification")
        .unwrap()
        .1;
    for literal in [
        "nip01-event/1",
        "sha256-bip340",
        "event-id-and-signature",
        "verification-result",
        "bigbrotr-nip01-verifier/1",
        "a2494f4f81d46684e5814a9bf35e2b1df978f955",
    ] {
        let mut altered = method.clone();
        altered[position(method, literal.as_bytes())] = b'X';
        assert_eq!(decode(&altered), Err(CodecError::IncompatibleSchema));
    }
}

#[test]
fn text_and_allocation_limits_precede_allocation_and_unknown_is_not_empty() {
    let corpus = fixtures("dictionary");
    for (name, requested) in [
        ("visibility-public", 6),
        ("observer-known", 5 + 8 + 9),
        ("observer-unknown", 0),
        ("event-verification", 0),
    ] {
        let input = &corpus.iter().find(|r| r.0 == name).unwrap().1;
        let mut limits = DictionaryDecodeLimits {
            max_allocation_bytes: requested,
            ..Default::default()
        };
        assert!(
            DictionaryEntry::decode_preimage(input, digest(input), limits).is_ok(),
            "{name}"
        );
        if requested != 0 {
            limits.max_allocation_bytes -= 1;
            assert_eq!(
                DictionaryEntry::decode_preimage(input, digest(input), limits),
                Err(CodecError::AllocationLimit)
            );
        }
        limits = DictionaryDecodeLimits {
            max_encoded_bytes: input.len() - 1,
            ..Default::default()
        };
        assert_eq!(
            DictionaryEntry::decode_preimage(input, digest(input), limits),
            Err(CodecError::InputLimit)
        );
    }
    let public = &corpus
        .iter()
        .find(|r| r.0 == "visibility-public")
        .unwrap()
        .1;
    let start = position(public, &bytes("667075626c6963"));
    let mut overlong = public[..start].to_vec();
    overlong.extend([0x78, 65]);
    overlong.extend([b'a'; 65]);
    let limits = DictionaryDecodeLimits {
        max_allocation_bytes: 0,
        ..Default::default()
    };
    assert_eq!(
        DictionaryEntry::decode_preimage(&overlong, digest(&overlong), limits),
        Err(CodecError::StringLimit)
    );
    let mut unknown_empty = fixtures("dictionary")
        .into_iter()
        .find(|r| r.0 == "observer-unknown")
        .unwrap()
        .1;
    let empty = position(&unknown_empty, b"vantage") + 7;
    unknown_empty[empty] = 0x60;
    assert_eq!(
        decode(&unknown_empty),
        Err(CodecError::InvalidDictionary(DictionaryError::InvalidLabel))
    );
    for length in [8192, 8193] {
        let url =
            "wss://relay.example/".to_owned() + &"x".repeat(length - "wss://relay.example/".len());
        let result = EndpointDescriptor::new(url);
        assert_eq!(result.is_ok(), length == 8192);
        if let Ok(url) = result {
            let entry = DictionaryEntry::new(
                reference(DictionaryKind::Endpoint),
                DictionaryDescriptor::Endpoint(url),
            )
            .unwrap();
            let encoded = entry.encode_preimage(9000).unwrap();
            assert!(decode(&encoded).is_ok());
            let limits = DictionaryDecodeLimits {
                max_string_bytes: 8191,
                ..Default::default()
            };
            assert_eq!(
                DictionaryEntry::decode_preimage(&encoded, digest(&encoded), limits),
                Err(CodecError::StringLimit)
            );
        }
    }
}

#[test]
fn decoder_rechecks_descriptor_reference_namespace_and_scalar_domains() {
    let corpus = fixtures("dictionary");
    for name in ["observer-known", "access-public"] {
        let input = &corpus.iter().find(|r| r.0 == name).unwrap().1;
        let reference_kind = if name == "observer-known" { 5 } else { 4 };
        let mut marker = bytes("8450");
        marker.extend([1; 16]);
        marker.push(reference_kind);
        let start = position(input, &marker);
        let mut foreign = input.clone();
        foreign[start + 2] = 9;
        assert!(matches!(
            decode(&foreign),
            Err(CodecError::InvalidDictionary(_))
        ));
        let mut wrong = input.clone();
        wrong[start + 18] = 1;
        assert!(matches!(
            decode(&wrong),
            Err(CodecError::InvalidDictionary(_))
        ));
    }
    let input = &corpus.iter().find(|r| r.0 == "lineage-native").unwrap().1;
    let version = position(input, b"format_version") + 14;
    for replacement in [vec![0], vec![0xf6], vec![0x20], vec![0x1a, 0, 1, 0, 0]] {
        let mut invalid = input.clone();
        invalid.splice(version..version + 1, replacement);
        assert!(decode(&invalid).is_err());
    }
    let mut maximum = input.clone();
    maximum.splice(version..version + 1, [0x19, 0xff, 0xff]);
    assert!(decode(&maximum).is_ok());
    let source_dataset = position(input, b"source_dataset_id") + 17 + 1;
    let mut zero = input.clone();
    zero[source_dataset..source_dataset + 16].fill(0);
    assert!(decode(&zero).is_err());
    for (_, input, _) in fixtures("dictionary") {
        let reference = b"bigbrotr/dictionary/1\0".len() + 1;
        for range in [
            reference + 2..reference + 18,
            reference + 20..reference + 36,
        ] {
            let mut zero = input.clone();
            zero[range].fill(0);
            assert!(decode(&zero).is_err());
        }
        let mut zero_ordinal = input.clone();
        zero_ordinal[reference + 36] = 0;
        assert!(decode(&zero_ordinal).is_err());
    }
}

#[test]
fn closed_enums_and_dictionary_fact_namespace_remain_explicit() {
    for route in [
        RouteClass::Public,
        RouteClass::Private,
        RouteClass::Tor,
        RouteClass::I2p,
        RouteClass::Lokinet,
    ] {
        for clock in [ClockSource::SystemUtc, ClockSource::ExternalUtc] {
            let DictionaryDescriptor::Observer(mut value) = descriptor("observer-known") else {
                unreachable!()
            };
            value.route_class = Some(route);
            value.clock = Some(clock);
            value.software_version = Some(label("1.2.3+build.4"));
            let entry = DictionaryEntry::new(
                reference(DictionaryKind::Observer),
                DictionaryDescriptor::Observer(value),
            )
            .unwrap();
            let encoded = entry.encode_preimage(4096).unwrap();
            assert_eq!(decode(&encoded).unwrap(), entry);
            for literal in [route.as_str(), clock.as_str()] {
                let mut unknown = encoded.clone();
                unknown[position(&encoded, literal.as_bytes())] = b'X';
                assert_eq!(decode(&unknown), Err(CodecError::IncompatibleSchema));
            }
        }
    }
    for (_, input, _) in fixtures("fact") {
        for len in 0..input.len() {
            assert!(
                Fact::decode_preimage(
                    &input[..len],
                    digest(&input[..len]),
                    FactDecodeLimits::default()
                )
                .is_err()
            );
        }
    }
    let input = fixtures("fact")
        .into_iter()
        .find(|r| r.0 == "event")
        .unwrap()
        .1;
    let mut marker = bytes("8450");
    marker.extend([1; 16]);
    marker.push(8);
    let start = position(&input, &marker);
    let mut foreign = input.clone();
    foreign[start + 2] = 9;
    assert!(matches!(
        Fact::decode_preimage(&foreign, digest(&foreign), FactDecodeLimits::default()),
        Err(CodecError::InvalidFact(_))
    ));
    let endpoint = DictionaryEntry::new(
        reference(DictionaryKind::Endpoint),
        DictionaryDescriptor::Endpoint(
            EndpointDescriptor::new("wss://relay.example/?q=é".into()).unwrap(),
        ),
    )
    .unwrap();
    let encoded = endpoint.encode_preimage(4096).unwrap();
    assert_eq!(decode(&encoded).unwrap(), endpoint);
    let mut invalid_utf8 = encoded.clone();
    let utf8 = position(&encoded, "é".as_bytes());
    invalid_utf8[utf8] = 0xff;
    assert!(decode(&invalid_utf8).is_err());
}
