use bigbrotr_model::{DictionaryKind, FactPayload, Ordinal, TopologyGeneration};
use bigbrotr_protocol::{
    archive_cut::CutBound,
    archive_visibility::{VisibilityObservation, VisibilityRead},
};

fn read() -> VisibilityRead {
    VisibilityRead {
        dataset: [1; 16].try_into().unwrap(),
        scope: bigbrotr_model::QualifiedRef {
            dataset_id: [1; 16].try_into().unwrap(),
            dictionary_kind: DictionaryKind::VisibilityScope,
            allocation_domain: [2; 16].try_into().unwrap(),
            ordinal: Ordinal::new(1).unwrap(),
        },
        topology_generation: TopologyGeneration::new(0),
        bounds: vec![CutBound {
            shard_id: 0,
            owner_epoch: 1,
            complete_through: 1,
        }],
        snapshot_id: 2,
    }
}

fn observation() -> VisibilityObservation {
    // Frozen independent CBOR, rather than a production-encoded expectation.
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../bigbrotr-model/tests/fixtures/dictionary-v1.json"
    ))
    .unwrap();
    let vector = fixture["vectors"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["family"] == "fact" && v["name"] == "visibility-public")
        .unwrap();
    VisibilityObservation {
        format: "bigbrotr-native-visibility/1".into(),
        snapshot_id: 2,
        request_digest: read().digest(),
        fact_digest: vector["sha256"].as_str().unwrap().parse().unwrap(),
        preimage_hex: vector["preimage_hex"].as_str().unwrap().into(),
    }
}

#[test]
fn visibility_requires_full_mapping_integrity_and_exact_source_cut() {
    let request = read();
    let actual = observation().verify(&request).unwrap();
    let FactPayload::Dictionary(entry) = actual.payload() else {
        panic!()
    };
    assert_eq!(entry.reference(), request.scope);
    assert_eq!(actual.source_record_id().sequence.get(), 1);
    for case in [
        "zero", "missing", "epoch", "scope", "domain", "snapshot", "topology",
    ] {
        let mut altered = request.clone();
        match case {
            "zero" => altered.bounds[0].complete_through = 0,
            "missing" => altered.bounds.clear(),
            "epoch" => altered.bounds[0].owner_epoch = 2,
            "scope" => altered.scope.ordinal = Ordinal::new(2).unwrap(),
            "domain" => altered.scope.allocation_domain = [3; 16].try_into().unwrap(),
            "snapshot" => altered.snapshot_id = 3,
            _ => altered.topology_generation = TopologyGeneration::new(1),
        }
        assert!(observation().verify(&altered).is_err(), "{case}");
        // Rebinding the response cannot make an excluded source/mapping valid.
        if !matches!(case, "snapshot" | "topology") {
            let mut rebound = observation();
            rebound.request_digest = altered.digest();
            assert!(rebound.verify(&altered).is_err(), "rebound {case}");
        }
    }
    for case in ["format", "digest", "bytes", "uppercase", "oversize"] {
        let mut invalid = observation();
        match case {
            "format" => invalid.format.push('2'),
            "digest" => invalid.fact_digest = bigbrotr_model::Digest256::from_bytes([0; 32]),
            "bytes" => invalid.preimage_hex.push_str("00"),
            "uppercase" => invalid.preimage_hex.make_ascii_uppercase(),
            _ => invalid.preimage_hex = "00".repeat(513),
        }
        assert!(invalid.verify(&request).is_err(), "{case}");
    }
}

#[test]
fn visibility_request_is_closed_and_rejects_invalid_cut_before_io() {
    let request = read();
    let vector: serde_json::Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/protocol/archive-visibility.json"
    ))
    .unwrap();
    assert_eq!(
        serde_json::from_value::<VisibilityRead>(vector["read"].clone()).unwrap(),
        request
    );
    assert_eq!(
        request.digest().to_string(),
        vector["request_digest"].as_str().unwrap()
    );
    assert!(request.validate().is_ok());
    for case in ["kind", "dataset", "snapshot", "epoch", "duplicate", "limit"] {
        let mut invalid = request.clone();
        match case {
            "kind" => invalid.scope.dictionary_kind = DictionaryKind::AccessScope,
            "dataset" => invalid.scope.dataset_id = [4; 16].try_into().unwrap(),
            "snapshot" => invalid.snapshot_id = i64::MAX as u64 + 1,
            "epoch" => invalid.bounds[0].owner_epoch = 0,
            "duplicate" => invalid.bounds.push(invalid.bounds[0]),
            _ => invalid.bounds.resize(1025, invalid.bounds[0]),
        }
        assert!(invalid.validate().is_err(), "{case}");
    }
    let value = serde_json::to_value(&request).unwrap();
    for field in [
        "dataset",
        "scope",
        "snapshot_id",
        "topology_generation",
        "bounds",
    ] {
        let mut missing = value.clone();
        missing.as_object_mut().unwrap().remove(field);
        assert!(
            serde_json::from_value::<VisibilityRead>(missing).is_err(),
            "{field}"
        );
    }
    let mut extra = value;
    extra["authorized"] = serde_json::json!(true);
    assert!(serde_json::from_value::<VisibilityRead>(extra).is_err());
}
