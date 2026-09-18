use bigbrotr_model::{
    AdmissionBatch, ArchiveBatch, DatasetId, Digest256, FactRef, OwnerEpoch, Sequence,
    SourceRecordId, TopologyGeneration,
};
use bigbrotr_protocol::archive_receipt::{
    ExpectationError, MAX_RANGES, ReceiptExpectation, ReceiptRange,
};
fn admission(shard: u32, epoch: u64, first: u64, hash: u8) -> AdmissionBatch {
    AdmissionBatch::new(vec![FactRef {
        source_record_id: SourceRecordId {
            dataset_id: DatasetId::try_from([1; 16]).unwrap(),
            shard_id: shard,
            owner_epoch: OwnerEpoch::new(epoch).unwrap(),
            sequence: Sequence::new(first).unwrap(),
        },
        payload_digest: Digest256::from_bytes([hash; 32]),
    }])
    .unwrap()
}
fn expectation() -> ReceiptExpectation {
    let batches = [
        admission(0, 1, 23, 0x11),
        admission(0, 1, 24, 0x22),
        admission(0, 1, 30, 0x33),
        admission(u32::MAX, u64::MAX, u64::MAX, 0x44),
    ];
    ReceiptExpectation::from_archive(
        &ArchiveBatch::new(1, TopologyGeneration::new(u64::MAX), &batches).unwrap(),
        [9; 16],
        u64::MAX,
    )
    .unwrap()
}
#[test]
fn retained_membership_preserves_maximal_ranges_unsigned_values_and_closed_roundtrip() {
    let wanted = expectation();
    assert_eq!(wanted.record_count, 4);
    assert_eq!(
        wanted.input_ranges,
        vec![
            ReceiptRange {
                shard_id: 0,
                owner_epoch: 1,
                first_sequence: 23,
                last_sequence: 24
            },
            ReceiptRange {
                shard_id: 0,
                owner_epoch: 1,
                first_sequence: 30,
                last_sequence: 30
            },
            ReceiptRange {
                shard_id: u32::MAX,
                owner_epoch: u64::MAX,
                first_sequence: u64::MAX,
                last_sequence: u64::MAX
            }
        ]
    );
    let encoded = serde_json::to_value(&wanted).unwrap();
    assert_eq!(encoded["topology_generation"], u64::MAX.to_string());
    assert_eq!(
        serde_json::from_value::<ReceiptExpectation>(encoded.clone()).unwrap(),
        wanted
    );
    let mut foreign = encoded;
    foreign["permission"] = "replay".into();
    assert!(serde_json::from_value::<ReceiptExpectation>(foreign).is_err());
}
#[test]
fn zero_nonmaximal_unordered_overlapping_and_count_overflow_refuse() {
    let original = expectation();
    for index in 0..10 {
        let mut value = original.clone();
        match index {
            0 => value.attempt_id = [0; 16],
            1 => value.publication_ordinal = 0,
            2 => value.schema_version = 2,
            3 => value.record_count += 1,
            4 => value.input_ranges[0].owner_epoch = 0,
            5 => value.input_ranges[0].first_sequence = 0,
            6 => value.input_ranges[0].last_sequence = 22,
            7 => value.input_ranges[1].first_sequence = 24,
            8 => value.input_ranges[1].first_sequence = 25,
            9 => value.input_ranges.reverse(),
            _ => unreachable!(),
        }
        assert_eq!(
            value.validate(),
            Err(ExpectationError::InvalidIdentity),
            "case{index}"
        );
    }
    let mut value = original;
    value.input_ranges[0].first_sequence = 1;
    value.input_ranges[0].last_sequence = u64::MAX;
    value.input_ranges.remove(1);
    value.record_count = u64::MAX;
    assert_eq!(value.validate(), Err(ExpectationError::InvalidIdentity));
}
#[test]
fn allocation_limit_is_distinct_from_invalid_membership() {
    let mut value = expectation();
    value.input_ranges = vec![value.input_ranges[0]; MAX_RANGES + 1];
    assert_eq!(value.validate(), Err(ExpectationError::ResourceLimit));
}
