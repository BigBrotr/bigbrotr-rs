use bigbrotr_model::archive::ArchiveBatchError;
use bigbrotr_model::{
    AdmissionBatch, ArchiveBatch, DatasetId, Digest256, FactRef, OwnerEpoch, Sequence,
    SourceRecordId, TopologyGeneration,
};

fn admission(shard: u32, epoch: u64, first: u64, digests: &[u8]) -> AdmissionBatch {
    AdmissionBatch::new(
        digests
            .iter()
            .enumerate()
            .map(|(offset, digest)| FactRef {
                source_record_id: SourceRecordId {
                    dataset_id: DatasetId::try_from([1; 16]).unwrap(),
                    shard_id: shard,
                    owner_epoch: OwnerEpoch::new(epoch).unwrap(),
                    sequence: Sequence::new(first + offset as u64).unwrap(),
                },
                payload_digest: Digest256::from_bytes([*digest; 32]),
            })
            .collect(),
    )
    .unwrap()
}

#[test]
fn existing_archive_golden_digest_is_unchanged_and_domain_separated() {
    let fact = FactRef {
        source_record_id: admission(0, 1, 1, &[0]).first(),
        payload_digest: "578671867996ee16e2fc7a109cf0e9a11298f767d083ddc16ddc68ceeed6938e"
            .parse()
            .unwrap(),
    };
    let batches = [AdmissionBatch::new(vec![fact]).unwrap()];
    let archive = ArchiveBatch::new(1, TopologyGeneration::new(1), &batches).unwrap();
    assert_eq!(
        archive.digest().to_string(),
        "93773dc6bf483182cce7f955a91ca8f5571bdf074da2f7105f6ca9aa985b333c"
    );
    assert_ne!(archive.digest(), batches[0].digest());
}

#[test]
fn multiple_ranges_preserve_unsigned_extremes_and_literal_fingerprint() {
    let batches = [
        admission(0, 1, 23, &[0x11]),
        admission(0, 1, 24, &[0x22]),
        admission(0, 1, 30, &[0x33]),
        admission(u32::MAX, u64::MAX, u64::MAX, &[0x44]),
    ];
    let archive = ArchiveBatch::new(u16::MAX, TopologyGeneration::new(u64::MAX), &batches).unwrap();
    let vector: serde_json::Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/model/archive-multiple-ranges.json"
    ))
    .unwrap();
    assert_eq!(
        archive.digest().to_string(),
        vector["sha256"].as_str().unwrap()
    );
    assert_eq!(archive.record_count(), 4);
    assert_eq!(archive.range_count(), 3);
    let ranges: Vec<_> = archive
        .ranges()
        .map(|r| {
            (
                r.stream.shard_id,
                r.stream.owner_epoch.get(),
                r.first.get(),
                r.last.get(),
            )
        })
        .collect();
    assert_eq!(
        ranges,
        [
            (0, 1, 23, 24),
            (0, 1, 30, 30),
            (u32::MAX, u64::MAX, u64::MAX, u64::MAX)
        ]
    );
    assert_eq!(
        archive.records().copied().collect::<Vec<_>>(),
        batches
            .iter()
            .flat_map(|b| b.records().iter().copied())
            .collect::<Vec<_>>()
    );
}

#[test]
fn complete_admission_grouping_does_not_change_semantic_membership() {
    let separate = [admission(0, 1, 23, &[0x11]), admission(0, 1, 24, &[0x22])];
    let combined = [admission(0, 1, 23, &[0x11, 0x22])];
    let a = ArchiveBatch::new(1, TopologyGeneration::new(0), &separate).unwrap();
    let b = ArchiveBatch::new(1, TopologyGeneration::new(0), &combined).unwrap();
    assert_eq!(a.digest(), b.digest());
    assert_eq!(a.range_count(), 1);
}

#[test]
fn empty_foreign_reversed_duplicate_and_overlapping_membership_refuse() {
    let topology = TopologyGeneration::new(1);
    assert_eq!(
        ArchiveBatch::new(1, topology, &[]).unwrap_err(),
        ArchiveBatchError::Empty
    );
    for batches in [
        [admission(0, 1, 2, &[1]), admission(0, 1, 1, &[2])],
        [admission(0, 1, 1, &[1]), admission(0, 1, 1, &[2])],
        [admission(0, 1, 1, &[1, 2]), admission(0, 1, 2, &[2, 3])],
        [admission(1, 1, 1, &[1]), admission(0, 2, 1, &[2])],
        [admission(0, 2, 1, &[1]), admission(0, 1, 1, &[2])],
    ] {
        assert_eq!(
            ArchiveBatch::new(1, topology, &batches).unwrap_err(),
            ArchiveBatchError::UnorderedBatches
        );
    }
    let mut foreign = admission(0, 1, 2, &[2]).records().to_vec();
    foreign[0].source_record_id.dataset_id = DatasetId::try_from([2; 16]).unwrap();
    let batches = [
        admission(0, 1, 1, &[1]),
        AdmissionBatch::new(foreign).unwrap(),
    ];
    assert_eq!(
        ArchiveBatch::new(1, topology, &batches).unwrap_err(),
        ArchiveBatchError::ForeignDataset
    );
}

#[test]
fn header_payload_and_stream_changes_are_bound_into_the_digest() {
    let batches = [admission(0, 1, 1, &[1])];
    let topology = TopologyGeneration::new(1);
    let original = ArchiveBatch::new(1, topology, &batches).unwrap().digest();
    assert_ne!(
        original,
        ArchiveBatch::new(2, topology, &batches).unwrap().digest()
    );
    assert_ne!(
        original,
        ArchiveBatch::new(1, TopologyGeneration::new(2), &batches)
            .unwrap()
            .digest()
    );
    for changed in [
        admission(0, 1, 1, &[2]),
        admission(0, 1, 2, &[1]),
        admission(0, 2, 1, &[1]),
        admission(1, 1, 1, &[1]),
    ] {
        assert_ne!(
            original,
            ArchiveBatch::new(1, topology, &[changed]).unwrap().digest()
        );
    }
}
