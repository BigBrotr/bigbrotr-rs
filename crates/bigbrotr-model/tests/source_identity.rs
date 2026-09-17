use bigbrotr_model::*;

fn dataset(byte: u8) -> DatasetId {
    DatasetId::try_from([byte; 16]).unwrap()
}
fn stream(shard_id: u32, epoch: u64) -> StreamId {
    StreamId {
        shard_id,
        owner_epoch: OwnerEpoch::new(epoch).unwrap(),
    }
}
fn bound(shard: u32, epoch: u64, last: u64) -> StreamBound {
    StreamBound {
        stream: stream(shard, epoch),
        last_complete_sequence: CompleteThrough::new(last),
    }
}

#[test]
fn references_retain_namespace_and_reject_unknown_kinds() {
    let reference = QualifiedRef {
        dataset_id: dataset(1),
        dictionary_kind: DictionaryKind::VisibilityScope,
        allocation_domain: AllocationDomainId::try_from([2; 16]).unwrap(),
        ordinal: Ordinal::new(u64::MAX).unwrap(),
    };
    let json = serde_json::to_string(&reference).unwrap();
    assert!(json.contains("\"ordinal\":\"18446744073709551615\""));
    assert!(json.contains("\"dictionary_kind\":4"));
    assert_eq!(
        serde_json::from_str::<QualifiedRef>(&json).unwrap(),
        reference
    );
    assert!(
        reference
            .require(dataset(2), DictionaryKind::VisibilityScope)
            .is_err()
    );
    assert!(
        reference
            .require(dataset(1), DictionaryKind::Observer)
            .is_err()
    );
    assert!(
        serde_json::from_str::<QualifiedRef>(
            &json.replace("\"dictionary_kind\":4", "\"dictionary_kind\":10")
        )
        .is_err()
    );
    assert!(
        serde_json::from_str::<QualifiedRef>(&json.replacen('{', "{\"future\":1,", 1)).is_err()
    );
}

#[test]
fn cuts_require_exact_topology_and_never_treat_missing_streams_as_zero() {
    let generation = TopologyGeneration::new(1);
    let cut = SourceCut::new(
        dataset(1),
        generation,
        vec![bound(0, 1, 7), bound(0, 2, 0), bound(1, 1, u64::MAX)],
    )
    .unwrap();
    let streams = [stream(0, 1), stream(0, 2), stream(1, 1)];
    cut.validate_topology(dataset(1), generation, &streams)
        .unwrap();
    assert!(
        cut.validate_topology(dataset(2), generation, &streams)
            .is_err()
    );
    assert!(
        cut.validate_topology(dataset(1), TopologyGeneration::new(2), &streams)
            .is_err()
    );
    assert!(
        cut.validate_topology(dataset(1), generation, &streams[..2])
            .is_err()
    );
    assert!(SourceCut::new(dataset(1), generation, vec![bound(0, 1, 1), bound(0, 1, 2)]).is_err());
    assert!(SourceCut::new(dataset(1), generation, vec![bound(1, 1, 1), bound(0, 1, 1)]).is_err());
    let fact = SourceRecordId {
        dataset_id: dataset(1),
        shard_id: 0,
        owner_epoch: OwnerEpoch::new(1).unwrap(),
        sequence: Sequence::new(7).unwrap(),
    };
    assert!(cut.contains(fact).unwrap());
    assert!(
        !cut.contains(SourceRecordId {
            sequence: Sequence::new(8).unwrap(),
            ..fact
        })
        .unwrap()
    );
    assert!(
        !cut.contains(SourceRecordId {
            owner_epoch: OwnerEpoch::new(2).unwrap(),
            sequence: Sequence::new(1).unwrap(),
            ..fact
        })
        .unwrap()
    );
    assert!(
        cut.contains(SourceRecordId {
            shard_id: 2,
            ..fact
        })
        .is_err()
    );
    assert!(
        cut.contains(SourceRecordId {
            dataset_id: dataset(2),
            ..fact
        })
        .is_err()
    );
}

#[test]
fn admission_ranges_reject_gaps_mixed_namespaces_and_wraparound() {
    let first = SourceRecordId {
        dataset_id: dataset(1),
        shard_id: 0,
        owner_epoch: OwnerEpoch::new(1).unwrap(),
        sequence: Sequence::new(u64::MAX - 1).unwrap(),
    };
    let records = vec![
        FactRef {
            source_record_id: first,
            payload_digest: Digest256::from_bytes([1; 32]),
        },
        FactRef {
            source_record_id: SourceRecordId {
                sequence: Sequence::new(u64::MAX).unwrap(),
                ..first
            },
            payload_digest: Digest256::from_bytes([2; 32]),
        },
    ];
    let batch = AdmissionBatch::new(records.clone()).unwrap();
    assert_eq!(batch.first(), first);
    assert_eq!(batch.last().sequence.get(), u64::MAX);
    assert!(AdmissionBatch::new(vec![]).is_err());
    let mut wrong = records.clone();
    wrong[1].source_record_id.dataset_id = dataset(2);
    assert!(AdmissionBatch::new(wrong).is_err());
    let mut wrong = records.clone();
    wrong[1].source_record_id.sequence = Sequence::new(1).unwrap();
    assert!(AdmissionBatch::new(wrong).is_err());
    let mut wrong = records;
    wrong.push(wrong[0]);
    assert!(AdmissionBatch::new(wrong).is_err());
}

#[test]
fn fingerprints_match_frozen_preimages_and_preserve_body_semantics() {
    let vectors: serde_json::Value = serde_json::from_str(include_str!(
        "../../../experiments/identity-contract/golden-vectors.json"
    ))
    .unwrap();
    let row = |name: &str| {
        vectors["vectors"]
            .as_array()
            .unwrap()
            .iter()
            .find(|v| v["name"] == name)
            .unwrap()
    };
    let event = SignedEvent {
        id: "aa".repeat(32).parse().unwrap(),
        pubkey: "bb".repeat(32).parse().unwrap(),
        created_at: NostrTime(u64::MAX),
        kind: 1,
        tags: vec![
            vec!["x".into()],
            vec!["x".into(), "".into()],
            vec!["x".into(), "".into()],
        ],
        content: "é\n".into(),
        sig: "cc".repeat(64).parse().unwrap(),
    };
    assert_eq!(
        event.body_digest().to_string(),
        row("body-u64-ordered-tags")["sha256"]
    );
    let mut alternative = event.clone();
    alternative.sig = Signature::from_bytes([0; 64]);
    assert_eq!(event.body_digest(), alternative.body_digest());
    alternative.content = "e\u{301}\n".into();
    assert_ne!(event.body_digest(), alternative.body_digest());
    let mut reordered = event.clone();
    reordered.tags.swap(0, 1);
    assert_ne!(event.body_digest(), reordered.body_digest());
    let batch = AdmissionBatch::new(vec![FactRef {
        source_record_id: SourceRecordId {
            dataset_id: dataset(1),
            shard_id: 0,
            owner_epoch: OwnerEpoch::new(1).unwrap(),
            sequence: Sequence::new(1).unwrap(),
        },
        payload_digest: "578671867996ee16e2fc7a109cf0e9a11298f767d083ddc16ddc68ceeed6938e"
            .parse()
            .unwrap(),
    }])
    .unwrap();
    assert_eq!(batch.digest().to_string(), row("admission-batch")["sha256"]);
}
