use bigbrotr_model::{
    Digest256, EventOrigin,
    codec::{
        CodecError,
        request::{MutationIntent, RequestDecodeLimits, decode_request},
    },
};
use serde_json::Value;
use sha2::{Digest, Sha256};

fn bytes(hex: &str) -> Vec<u8> {
    hex.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap())
        .collect()
}
fn hash(input: &[u8]) -> Digest256 {
    Digest256::from_bytes(Sha256::digest(input).into())
}
fn fixtures() -> Vec<(String, Vec<u8>, Digest256)> {
    let core: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/model/core-fingerprints.json"
    ))
    .unwrap();
    let mut rows: Vec<_> = core["vectors"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|v| v["name"].as_str().unwrap().starts_with("request-"))
        .map(|v| {
            (
                v["name"].as_str().unwrap().to_owned(),
                bytes(v["preimage_hex"].as_str().unwrap()),
                v["sha256"].as_str().unwrap().parse().unwrap(),
            )
        })
        .collect();
    let evidence: Value =
        serde_json::from_str(include_str!("fixtures/evidence-intent-v1.json")).unwrap();
    let v = &evidence["vectors"][0];
    rows.push((
        "evidence".into(),
        bytes(v["preimage_hex"].as_str().unwrap()),
        v["sha256"].as_str().unwrap().parse().unwrap(),
    ));
    let rejection: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/ingest/rejected-request.json"
    ))
    .unwrap();
    rows.push((
        "invalid-signature".into(),
        bytes(rejection["request_preimage_hex"].as_str().unwrap()),
        rejection["request_digest"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap(),
    ));
    rows
}
fn decode(input: &[u8]) -> Result<(bigbrotr_model::ProducerRequestId, MutationIntent), CodecError> {
    decode_request(input, hash(input), RequestDecodeLimits::default())
}

#[test]
fn frozen_mutation_requests_preserve_original_identity_and_attribution() {
    let fixtures = fixtures();
    assert_eq!(fixtures.len(), 4);
    for (name, input, digest) in fixtures {
        let (id, intent) = decode_request(&input, digest, RequestDecodeLimits::default()).unwrap();
        assert_eq!(id.request_sequence.get(), 1);
        match intent {
            MutationIntent::Event(intent) => {
                assert_eq!(intent.request_digest(id), digest);
                assert_eq!(
                    intent.encode_request_preimage(id, input.len()).unwrap(),
                    input
                );
                assert_eq!(
                    intent
                        .encode_request_preimage(id, input.len() - 1)
                        .unwrap_err(),
                    CodecError::InputLimit
                );
                assert_eq!(id.producer_epoch.get(), 1);
                if name == "request-import" {
                    assert!(matches!(intent.origin(), EventOrigin::Import(_)));
                    assert_eq!(intent.observation(), None);
                    assert_eq!(intent.event().created_at.0, u64::MAX);
                    assert_eq!(
                        intent.event().tags,
                        [vec!["x"], vec!["x", ""], vec!["x", ""]]
                    );
                    assert_eq!(intent.event().content, "é\n");
                } else {
                    let observation = intent.observation().unwrap();
                    assert_eq!(intent.origin(), EventOrigin::Network);
                    assert_eq!(
                        observation.witness.is_some(),
                        name == "request-network-provenance"
                    );
                }
            }
            MutationIntent::Evidence(intent) => {
                assert_eq!(name, "evidence");
                assert_eq!(id.producer_epoch.get(), u64::MAX);
                assert_eq!(intent.admission_policy_version(), u16::MAX);
                assert_eq!(intent.dictionaries().len(), 4);
                assert_eq!(intent.request_digest(id), digest);
                assert_eq!(
                    intent.encode_request_preimage(id, input.len()).unwrap(),
                    input
                );
            }
        }
    }
}

#[test]
fn truncations_unknown_operations_and_noncanonical_forms_are_refused() {
    for (_, input, _) in fixtures() {
        for n in 0..input.len() {
            assert!(decode(&input[..n]).is_err(), "truncation {n}");
        }
        let prefix = b"bigbrotr/producer-request/1\0".len();
        let mut wide = input.clone();
        wide.splice(prefix..prefix + 1, [0x98, 4]);
        assert_eq!(decode(&wide).unwrap_err(), CodecError::NonCanonical);
        let mut trailing = input.clone();
        trailing.push(0);
        assert!(decode(&trailing).is_err());
        let mut indefinite = input.clone();
        indefinite[prefix] = 0x9f;
        assert!(decode(&indefinite).is_err());
        let mut wrong_profile = input.clone();
        wrong_profile[prefix - 2] = b'2';
        assert_eq!(
            decode(&wrong_profile).unwrap_err(),
            CodecError::IncompatibleSchema
        );
        let mut unknown = input.clone();
        let pos = unknown.windows(5).position(|v| v == b"admit").unwrap();
        unknown[pos] = b'x';
        assert_eq!(
            decode(&unknown).unwrap_err(),
            CodecError::IncompatibleSchema
        );
        assert_eq!(
            decode_request(
                &input,
                Digest256::from_bytes([0; 32]),
                RequestDecodeLimits::default()
            )
            .unwrap_err(),
            CodecError::DigestMismatch
        );
    }
}

#[test]
fn reduced_limits_refuse_without_losing_identical_retry() {
    for (name, input, digest) in fixtures() {
        for class in ["input", "allocation", "string", "count"] {
            let mut limits = RequestDecodeLimits::default();
            let expected = match class {
                "input" => {
                    limits.values.max_encoded_bytes = input.len() - 1;
                    CodecError::InputLimit
                }
                "allocation" => {
                    limits.values.max_allocation_bytes = 0;
                    CodecError::AllocationLimit
                }
                "string" => {
                    limits.values.max_string_bytes = 0;
                    CodecError::StringLimit
                }
                _ if name == "evidence" => {
                    limits.max_dictionaries = 3;
                    CodecError::InputLimit
                }
                _ => {
                    limits.values.max_tag_nodes = 0;
                    CodecError::TagLimit
                }
            };
            assert_eq!(
                decode_request(&input, digest, limits).unwrap_err(),
                expected,
                "{name}/{class}"
            );
            assert!(decode_request(&input, digest, RequestDecodeLimits::default()).is_ok());
        }
    }
}

#[test]
fn real_signed_import_keeps_sequence_two_and_every_original_event_field() {
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/signed-request-v1.json")).unwrap();
    let input = bytes(fixture["preimage_hex"].as_str().unwrap());
    let digest = fixture["sha256"].as_str().unwrap().parse().unwrap();
    let source = include_bytes!("../../../tests/fixtures/ingest/import-event.jsonl");
    assert_eq!(hash(source).to_string(), fixture["source_sha256"]);
    let expected: bigbrotr_model::SignedEvent = serde_json::from_slice(source).unwrap();
    let (id, intent) = decode_request(&input, digest, RequestDecodeLimits::default()).unwrap();
    assert_eq!(id.producer_slot_id.as_bytes(), &[3; 16]);
    assert_eq!(id.producer_epoch.get(), 1);
    assert_eq!(id.request_sequence.get(), 2);
    let MutationIntent::Event(intent) = intent else {
        panic!("not an event")
    };
    assert_eq!(intent.event(), &expected);
    assert_eq!(intent.observation(), None);
    let EventOrigin::Import(lineage) = intent.origin() else {
        panic!("not import")
    };
    assert_eq!(lineage.dictionary_kind as u16, 9);
    assert_eq!(lineage.allocation_domain.as_bytes(), &[9; 16]);
    assert_eq!(
        intent.encode_request_preimage(id, input.len()).unwrap(),
        input
    );
}
