use bigbrotr_model::{
    AccessClass, AccessScopeDescriptor, AllocationDomainId, ClockSource, DatasetId,
    DescriptorLabel, DictionaryDescriptor, DictionaryEntry, DictionaryKind, EvidenceIntent,
    EvidenceIntentError, EvidenceIntentLimits, ObserverDescriptor, Ordinal, ProducerEpoch,
    ProducerRequestId, ProducerSlotId, QualifiedRef, RequestSequence, RouteClass,
};
use minicbor::Decoder;
use serde_json::Value;
use std::mem::size_of;

const PREFIX: &[u8] = b"bigbrotr/producer-request/1\0";

fn dataset(byte: u8) -> DatasetId {
    DatasetId::try_from([byte; 16]).unwrap()
}

fn reference_in(dataset_id: DatasetId, kind: DictionaryKind, ordinal: u64) -> QualifiedRef {
    QualifiedRef {
        dataset_id,
        dictionary_kind: kind,
        allocation_domain: AllocationDomainId::try_from([0x22; 16]).unwrap(),
        ordinal: Ordinal::new(ordinal).unwrap(),
    }
}

fn label(value: &str) -> DescriptorLabel {
    DescriptorLabel::new(value.into()).unwrap()
}

fn entries(scope_class: &str, observer_ordinal: u64) -> Vec<DictionaryEntry> {
    let dataset = dataset(0x11);
    let scope = reference_in(dataset, DictionaryKind::VisibilityScope, 1);
    let method = reference_in(dataset, DictionaryKind::Method, 2);
    vec![
        DictionaryEntry::new(
            scope,
            DictionaryDescriptor::Visibility {
                class: label(scope_class),
            },
        )
        .unwrap(),
        DictionaryEntry::new(method, DictionaryDescriptor::EventVerification).unwrap(),
        DictionaryEntry::new(
            reference_in(dataset, DictionaryKind::Observer, observer_ordinal),
            DictionaryDescriptor::Observer(ObserverDescriptor {
                clock: Some(ClockSource::SystemUtc),
                vantage: Some(label("fixture-vantage")),
                software: None,
                method_id: Some(method),
                public_key: None,
                route_class: Some(RouteClass::Public),
                software_version: None,
            }),
        )
        .unwrap(),
        DictionaryEntry::new(
            reference_in(dataset, DictionaryKind::AccessScope, 4),
            DictionaryDescriptor::AccessScope(AccessScopeDescriptor {
                class: AccessClass::Public,
                label: None,
                visibility_scope_id: scope,
            }),
        )
        .unwrap(),
    ]
}

fn limits(entries: usize, intent_bytes: usize) -> EvidenceIntentLimits {
    EvidenceIntentLimits {
        max_entries: entries,
        max_encoded_intent_bytes: intent_bytes,
        max_validation_scratch_bytes: entries.checked_mul(size_of::<QualifiedRef>()).unwrap(),
    }
}

fn request_id() -> ProducerRequestId {
    ProducerRequestId {
        producer_slot_id: ProducerSlotId::try_from([0x33; 16]).unwrap(),
        producer_epoch: ProducerEpoch::new(u64::MAX).unwrap(),
        request_sequence: RequestSequence::new(1).unwrap(),
    }
}

fn vectors() -> Value {
    serde_json::from_str(include_str!("fixtures/evidence-intent-v1.json")).unwrap()
}

fn vector(name: &str) -> Value {
    vectors()["vectors"]
        .as_array()
        .unwrap()
        .iter()
        .find(|value| value["name"] == name)
        .unwrap()
        .clone()
}

fn from_hex(value: &str) -> Vec<u8> {
    let (pairs, remainder) = value.as_bytes().as_chunks::<2>();
    assert!(remainder.is_empty());
    pairs
        .iter()
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

#[test]
fn exact_dictionary_request_matches_independent_literal_bytes_and_digest() {
    let expected = vector("base");
    let intent = EvidenceIntent::new(
        65_535,
        reference_in(dataset(0x11), DictionaryKind::VisibilityScope, 1),
        entries("public", 3),
        limits(4, 780),
    )
    .unwrap();

    assert_eq!(intent.admission_policy_version(), 65_535);
    assert_eq!(intent.dictionaries().len(), 4);
    assert_eq!(intent.encoded_intent_bytes(), 780);
    assert_eq!(intent.dictionaries()[0].descriptor_schema(), "visibility");
    assert_eq!(intent.dictionaries()[3].descriptor_schema(), "access-scope");

    let bytes = intent.encode_request_preimage(request_id(), 853).unwrap();
    assert_eq!(bytes, from_hex(expected["preimage_hex"].as_str().unwrap()));
    assert_eq!(
        bytes.len(),
        expected["request_preimage_bytes"].as_u64().unwrap() as usize
    );
    assert_eq!(
        intent.request_digest(request_id()).to_string(),
        expected["sha256"]
    );

    let mut decoder = Decoder::new(bytes.strip_prefix(PREFIX).unwrap());
    assert_eq!(decoder.array().unwrap(), Some(4));
    assert_eq!(decoder.array().unwrap(), Some(3));
    assert_eq!(decoder.bytes().unwrap(), &[0x33; 16]);
    assert_eq!(decoder.u64().unwrap(), u64::MAX);
    assert_eq!(decoder.u64().unwrap(), 1);
    assert_eq!(decoder.str().unwrap(), "admit-evidence");
    assert_eq!(decoder.u16().unwrap(), 1);
    assert_eq!(decoder.array().unwrap(), Some(3));
    assert_eq!(decoder.u16().unwrap(), 65_535);
    decoder.skip().unwrap();
    assert_eq!(decoder.array().unwrap(), Some(4));
    for _ in 0..4 {
        assert_eq!(decoder.array().unwrap(), Some(3));
        assert_eq!(decoder.str().unwrap(), "dictionary");
        assert_eq!(decoder.u16().unwrap(), 1);
        decoder.skip().unwrap();
    }
    assert_eq!(decoder.position(), bytes.len() - PREFIX.len());
}

#[test]
fn descriptor_reference_and_caller_order_each_change_the_frozen_digest() {
    let scope = reference_in(dataset(0x11), DictionaryKind::VisibilityScope, 1);
    let build = |values| EvidenceIntent::new(65_535, scope, values, limits(4, 1_024)).unwrap();
    let changed_descriptor = build(entries("private", 3));
    let changed_reference = build(entries("public", 5));
    let mut reordered = entries("public", 3);
    reordered.reverse();
    let changed_order = build(reordered);

    for (name, intent) in [
        ("changed-descriptor", changed_descriptor),
        ("changed-reference", changed_reference),
        ("changed-order", changed_order),
    ] {
        assert_eq!(
            intent.request_digest(request_id()).to_string(),
            vector(name)["sha256"]
        );
    }
    assert_ne!(vector("base")["sha256"], vector("changed-order")["sha256"]);
    assert_eq!(
        vectors()["signature_dimension"],
        "not-applicable: dictionary-only evidence records contain no signed event or signature field"
    );
}

#[test]
fn constructor_rejects_empty_foreign_wrong_scope_and_duplicate_definitions() {
    let dataset_id = dataset(0x11);
    let scope = reference_in(dataset_id, DictionaryKind::VisibilityScope, 1);
    assert_eq!(
        EvidenceIntent::new(0, scope, vec![], limits(1, 1_024)),
        Err(EvidenceIntentError::EmptyEntries)
    );

    let foreign = DictionaryEntry::new(
        reference_in(dataset(0x44), DictionaryKind::VisibilityScope, 1),
        DictionaryDescriptor::Visibility {
            class: label("public"),
        },
    )
    .unwrap();
    assert_eq!(
        EvidenceIntent::new(0, scope, vec![foreign], limits(1, 1_024)),
        Err(EvidenceIntentError::ForeignDataset)
    );

    let wrong_scope = reference_in(dataset_id, DictionaryKind::Event, 1);
    assert_eq!(
        EvidenceIntent::new(
            0,
            wrong_scope,
            vec![entries("public", 3).remove(0)],
            limits(1, 1_024)
        ),
        Err(EvidenceIntentError::WrongVisibilityScope)
    );

    let one = entries("public", 3).remove(0);
    assert_eq!(
        EvidenceIntent::new(0, scope, vec![one.clone(), one], limits(2, 1_024)),
        Err(EvidenceIntentError::DuplicateDefinition)
    );
}

#[test]
fn every_count_byte_and_scratch_allocation_is_caller_bounded() {
    let scope = reference_in(dataset(0x11), DictionaryKind::VisibilityScope, 1);
    let base = entries("public", 3);
    assert_eq!(
        EvidenceIntent::new(65_535, scope, base.clone(), limits(3, 1_024)),
        Err(EvidenceIntentError::EntryCountLimit)
    );
    assert_eq!(
        EvidenceIntent::new(65_535, scope, base.clone(), limits(0, 1_024)),
        Err(EvidenceIntentError::EntryCountLimit)
    );
    let mut no_scratch = limits(4, 1_024);
    no_scratch.max_validation_scratch_bytes = 0;
    assert_eq!(
        EvidenceIntent::new(65_535, scope, base.clone(), no_scratch),
        Err(EvidenceIntentError::ValidationScratchLimit)
    );
    assert_eq!(
        EvidenceIntent::new(65_535, scope, base.clone(), limits(4, 779)),
        Err(EvidenceIntentError::EncodedIntentLimit)
    );
    assert_eq!(
        EvidenceIntent::new(65_535, scope, base.clone(), limits(4, 0)),
        Err(EvidenceIntentError::EncodedIntentLimit)
    );
    let intent = EvidenceIntent::new(65_535, scope, base, limits(4, 780)).unwrap();
    assert_eq!(
        intent.encode_request_preimage(request_id(), 852),
        Err(EvidenceIntentError::EncodedRequestLimit)
    );
    assert_eq!(
        intent.encode_request_preimage(request_id(), 0),
        Err(EvidenceIntentError::EncodedRequestLimit)
    );
    assert_eq!(
        intent
            .encode_request_preimage(request_id(), 853)
            .unwrap()
            .len(),
        853
    );
}

#[test]
fn zero_policy_is_lossless_and_equal_descriptors_in_distinct_namespaces_are_not_collapsed() {
    let dataset = dataset(0x11);
    let scope = reference_in(dataset, DictionaryKind::VisibilityScope, 1);
    let first = DictionaryEntry::new(
        reference_in(dataset, DictionaryKind::VisibilityScope, 8),
        DictionaryDescriptor::Visibility {
            class: label("shared"),
        },
    )
    .unwrap();
    let second = DictionaryEntry::new(
        reference_in(dataset, DictionaryKind::VisibilityScope, 9),
        DictionaryDescriptor::Visibility {
            class: label("shared"),
        },
    )
    .unwrap();
    let intent = EvidenceIntent::new(0, scope, vec![first, second], limits(2, 1_024)).unwrap();
    assert_eq!(intent.admission_policy_version(), 0);
    assert_eq!(intent.dictionaries().len(), 2);
    assert_ne!(
        intent.dictionaries()[0].reference(),
        intent.dictionaries()[1].reference()
    );

    let changed_request = ProducerRequestId {
        request_sequence: RequestSequence::new(2).unwrap(),
        ..request_id()
    };
    assert_ne!(
        intent.request_digest(request_id()),
        intent.request_digest(changed_request)
    );
    let (policy, consumed_scope, consumed_entries) = intent.into_parts();
    assert_eq!(policy, 0);
    assert_eq!(consumed_scope, scope);
    assert_eq!(consumed_entries.len(), 2);
}
