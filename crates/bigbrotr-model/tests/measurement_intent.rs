use bigbrotr_model::{
    DatasetId, DictionaryKind, Digest256, EvidenceIntentError, Fact, FactPayload, Measurement,
    MeasurementEvidenceIntent, ProducerEpoch, ProducerRequestId, ProducerSlotId, RequestSequence,
    codec::{
        CodecError, FactDecodeLimits,
        request::{MutationIntent, RequestDecodeLimits, decode_request},
    },
};
use serde_json::Value;
use sha2::{Digest, Sha256};

fn fixture() -> Value {
    serde_json::from_str(include_str!("fixtures/measurement-v1.json")).unwrap()
}
fn bytes(row: &Value) -> Vec<u8> {
    row["preimage_hex"]
        .as_str()
        .unwrap()
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}
fn digest(input: &[u8]) -> Digest256 {
    Digest256::from_bytes(Sha256::digest(input).into())
}
fn row<'a>(fixture: &'a Value, section: &str, name: &str) -> &'a Value {
    fixture[section]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["name"] == name)
        .unwrap()
}
fn request_id() -> ProducerRequestId {
    ProducerRequestId {
        producer_slot_id: ProducerSlotId::try_from([4; 16]).unwrap(),
        producer_epoch: ProducerEpoch::new(2).unwrap(),
        request_sequence: RequestSequence::new(3).unwrap(),
    }
}
fn fact(fixture: &Value) -> Fact {
    let input = bytes(row(fixture, "valid", "empty-eose-auth-utc-rollback"));
    Fact::decode_preimage(&input, digest(&input), FactDecodeLimits::default()).unwrap()
}
fn measurement(fact: &Fact) -> Measurement {
    let FactPayload::Measurement(value) = fact.payload() else {
        panic!("wrong family")
    };
    value.clone()
}

#[test]
fn retained_measurement_requests_match_independent_bytes_across_all_states() {
    let fixture = fixture();
    let rows = fixture["requests"].as_array().unwrap();
    assert_eq!(rows.len(), 19);
    for row in rows {
        let input = bytes(row);
        let expected = row["sha256"].as_str().unwrap().parse().unwrap();
        let (id, decoded) =
            decode_request(&input, expected, RequestDecodeLimits::default()).unwrap();
        let MutationIntent::Measurement(intent) = decoded else {
            panic!("wrong evidence alternative")
        };
        assert_eq!(id.producer_slot_id, request_id().producer_slot_id);
        assert_eq!(id.producer_epoch, request_id().producer_epoch);
        assert_eq!(
            id.request_sequence.get(),
            if row["name"] == "changed-request" {
                4
            } else {
                3
            }
        );
        assert_eq!(intent.request_digest(id), expected);
        assert_eq!(
            intent.request_preimage_len(id, input.len()).unwrap(),
            input.len()
        );
        assert_eq!(
            intent.encode_request_preimage(id, input.len()).unwrap(),
            input
        );
        let (policy, scope, value) = intent.into_parts();
        assert_eq!(policy, if row["name"] == "zero-policy" { 0 } else { 7 });
        assert_eq!(scope.dictionary_kind, DictionaryKind::VisibilityScope);
        assert_eq!(scope.dataset_id, value.context().endpoint_id().dataset_id);
    }
}

#[test]
fn measurement_intent_preserves_full_payload_and_rejects_foreign_or_wrong_scope() {
    let fixture = fixture();
    let fact = fact(&fixture);
    let scope = fact.visibility_scope_id();
    let intent = MeasurementEvidenceIntent::new(7, scope, measurement(&fact), 4096).unwrap();
    let expected = bytes(row(&fixture, "requests", "empty-eose-auth-utc-rollback"));
    assert_eq!(
        intent.encode_request_preimage(request_id(), 4096).unwrap(),
        expected
    );
    assert_eq!(
        intent.measurement(),
        match fact.payload() {
            FactPayload::Measurement(v) => v,
            _ => unreachable!(),
        }
    );
    assert_eq!(
        MeasurementEvidenceIntent::new(
            7,
            bigbrotr_model::QualifiedRef {
                dictionary_kind: DictionaryKind::Endpoint,
                ..scope
            },
            measurement(&fact),
            4096
        ),
        Err(EvidenceIntentError::WrongVisibilityScope)
    );
    assert_eq!(
        MeasurementEvidenceIntent::new(
            7,
            bigbrotr_model::QualifiedRef {
                dataset_id: DatasetId::try_from([9; 16]).unwrap(),
                ..scope
            },
            measurement(&fact),
            4096
        ),
        Err(EvidenceIntentError::ForeignDataset)
    );
    for name in [
        "changed-request",
        "zero-policy",
        "changed-visibility",
        "changed-attempt",
    ] {
        assert_ne!(
            row(&fixture, "requests", name)["sha256"],
            row(&fixture, "requests", "empty-eose-auth-utc-rollback")["sha256"]
        );
    }
}

#[test]
fn measurement_request_budgets_are_checked_before_allocation_and_preserve_typed_dispatch() {
    let fixture = fixture();
    let fact = fact(&fixture);
    let intent =
        MeasurementEvidenceIntent::new(7, fact.visibility_scope_id(), measurement(&fact), 4096)
            .unwrap();
    let size = intent.encoded_intent_bytes();
    assert_eq!(
        MeasurementEvidenceIntent::new(7, fact.visibility_scope_id(), measurement(&fact), size - 1),
        Err(EvidenceIntentError::EncodedIntentLimit)
    );
    assert!(
        MeasurementEvidenceIntent::new(7, fact.visibility_scope_id(), measurement(&fact), size)
            .is_ok()
    );
    let input = bytes(row(&fixture, "requests", "empty-eose-auth-utc-rollback"));
    assert_eq!(
        intent.request_preimage_len(request_id(), input.len() - 1),
        Err(EvidenceIntentError::EncodedRequestLimit)
    );
    assert_eq!(
        intent.encode_request_preimage(request_id(), input.len() - 1),
        Err(EvidenceIntentError::EncodedRequestLimit)
    );
    let required = Measurement::required_allocation_bytes();
    let limits = RequestDecodeLimits {
        values: FactDecodeLimits {
            max_allocation_bytes: required,
            max_tag_nodes: 0,
            max_string_bytes: 0,
            ..FactDecodeLimits::default()
        },
        max_dictionaries: 0,
    };
    assert!(matches!(
        decode_request(&input, digest(&input), limits).unwrap().1,
        MutationIntent::Measurement(_)
    ));
    assert_eq!(
        decode_request(
            &input,
            digest(&input),
            RequestDecodeLimits {
                values: FactDecodeLimits {
                    max_allocation_bytes: required - 1,
                    ..limits.values
                },
                ..limits
            }
        ),
        Err(CodecError::AllocationLimit)
    );
    assert_eq!(
        decode_request(
            &input,
            digest(&input),
            RequestDecodeLimits {
                values: FactDecodeLimits {
                    max_encoded_bytes: input.len() - 1,
                    ..limits.values
                },
                ..limits
            }
        ),
        Err(CodecError::InputLimit)
    );
}

#[test]
fn measurement_request_rejects_unknown_shapes_digest_mismatch_and_noncanonical_input() {
    let fixture = fixture();
    for row in fixture["invalid_requests"].as_array().unwrap() {
        let input = bytes(row);
        assert!(
            decode_request(&input, digest(&input), RequestDecodeLimits::default()).is_err(),
            "{}",
            row["name"]
        );
    }
    let input = bytes(row(&fixture, "requests", "empty-eose-auth-utc-rollback"));
    assert_eq!(
        decode_request(
            &input,
            Digest256::from_bytes([0; 32]),
            RequestDecodeLimits::default()
        ),
        Err(CodecError::DigestMismatch)
    );
    for size in 0..input.len() {
        assert!(
            decode_request(
                &input[..size],
                digest(&input[..size]),
                RequestDecodeLimits::default()
            )
            .is_err()
        );
    }
    let mut trailing = input.clone();
    trailing.push(0);
    assert_eq!(
        decode_request(&trailing, digest(&trailing), RequestDecodeLimits::default()),
        Err(CodecError::NonCanonical)
    );
    let mut wide = input;
    let prefix = b"bigbrotr/producer-request/1\0".len();
    wide.splice(prefix..prefix + 1, [0x98, 4]);
    assert_eq!(
        decode_request(&wide, digest(&wide), RequestDecodeLimits::default()),
        Err(CodecError::NonCanonical)
    );
}
