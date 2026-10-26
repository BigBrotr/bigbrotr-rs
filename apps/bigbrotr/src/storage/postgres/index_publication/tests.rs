use super::*;
use bigbrotr_model::{
    CompleteThrough, FactRef, Sequence, StreamBound, StreamId, TopologyGeneration,
};

fn binding(mode: IndexProvenance) -> IndexBinding {
    IndexBinding::new(
        DatasetId::try_from([1; 16]).unwrap(),
        OwnerResourceId::try_from([2; 16]).unwrap(),
        ProjectionGenerationId::try_from([3; 16]).unwrap(),
        ProcessIncarnation::try_from([4; 16]).unwrap(),
        Digest256::from_bytes([5; 32]),
        Digest256::from_bytes([6; 32]),
        mode,
    )
    .unwrap()
}
fn cut(sequence: u64) -> SourceCut {
    SourceCut::new(
        DatasetId::try_from([1; 16]).unwrap(),
        TopologyGeneration::new(0),
        vec![
            StreamBound {
                stream: StreamId {
                    shard_id: 0,
                    owner_epoch: OwnerEpoch::new(1).unwrap(),
                },
                last_complete_sequence: CompleteThrough::new(0),
            },
            StreamBound {
                stream: StreamId {
                    shard_id: 7,
                    owner_epoch: OwnerEpoch::new(1).unwrap(),
                },
                last_complete_sequence: CompleteThrough::new(sequence),
            },
        ],
    )
    .unwrap()
}
fn batch(first: u64) -> AdmissionBatch {
    AdmissionBatch::new(
        (0..2)
            .map(|i| FactRef {
                source_record_id: SourceRecordId {
                    dataset_id: DatasetId::try_from([1; 16]).unwrap(),
                    shard_id: 7,
                    owner_epoch: OwnerEpoch::new(1).unwrap(),
                    sequence: Sequence::new(first + i).unwrap(),
                },
                payload_digest: Digest256::from_bytes([10 + i as u8; 32]),
            })
            .collect(),
    )
    .unwrap()
}
fn chunks() -> [IndexChunk; 10] {
    std::array::from_fn(|i| {
        IndexChunk::new(
            IndexTarget::ALL[i],
            1,
            "all".to_owned(),
            Digest256::from_bytes([30 + i as u8; 32]),
            u64::from(i == 0),
            Digest256::from_bytes([50 + i as u8; 32]),
        )
        .unwrap()
    })
}
fn plan(mode: IndexProvenance) -> IndexPlan {
    IndexPlan::new(binding(mode), 0, cut(0), cut(2), batch(1), chunks()).unwrap()
}

#[test]
fn independent_literal_metadata_and_source_commitments() {
    // Independently encoded literal CBOR admission membership and big-endian PG
    // metadata, not expected values obtained from the production encoder.
    assert_eq!(
        batch(1).digest().to_string(),
        "2f9a7d60f00046f394e093e0b697dd064b7629e21f77486193e07236dec92e74"
    );
    assert_eq!(
        plan(IndexProvenance::LocalFirstWitness)
            .digest()
            .to_string(),
        "aa7f2eef19d03e4e3704714e90e400c10b75bddce94485682b45452adac61e98"
    );
    assert_eq!(
        plan(IndexProvenance::Off).digest().to_string(),
        "4321a18b1a112a16a2d899e6da618dd6abfc7c67bfab89c2aecfcb02df094900"
    );
}

#[test]
fn complete_canonical_membership_and_empty_targets_are_required() {
    let original = chunks();
    let mut wrong = original.clone();
    wrong.swap(0, 1);
    assert!(
        IndexPlan::new(
            binding(IndexProvenance::LocalFirstWitness),
            0,
            cut(0),
            cut(2),
            batch(1),
            wrong
        )
        .is_err()
    );
    let mut wrong = original.clone();
    wrong[9] = wrong[8].clone();
    assert!(
        IndexPlan::new(
            binding(IndexProvenance::LocalFirstWitness),
            0,
            cut(0),
            cut(2),
            batch(1),
            wrong
        )
        .is_err()
    );
    let mut wrong = original.clone();
    wrong[1].chunk_id = wrong[0].chunk_id;
    assert!(
        IndexPlan::new(
            binding(IndexProvenance::LocalFirstWitness),
            0,
            cut(0),
            cut(2),
            batch(1),
            wrong
        )
        .is_err()
    );
    let mut wrong = original.clone();
    wrong[8].row_count = 1;
    assert!(
        IndexPlan::new(
            binding(IndexProvenance::Off),
            0,
            cut(0),
            cut(2),
            batch(1),
            wrong
        )
        .is_err()
    );
    assert_eq!(
        plan(IndexProvenance::Off)
            .chunks()
            .iter()
            .filter(|c| c.row_count() == 0)
            .count(),
        9
    );
}

#[test]
fn no_holes_partial_batches_or_unrelated_stream_advancement() {
    for (before, after, first, revision) in [(0, 3, 2, 0), (0, 1, 1, 0), (1, 3, 2, 0), (2, 4, 1, 1)]
    {
        assert!(
            IndexPlan::new(
                binding(IndexProvenance::LocalFirstWitness),
                revision,
                cut(before),
                cut(after),
                batch(first),
                chunks()
            )
            .is_err()
        );
    }
    let mut after = cut(2).bounds().to_vec();
    after[0].last_complete_sequence = CompleteThrough::new(1);
    let after = SourceCut::new(
        binding(IndexProvenance::Off).dataset(),
        TopologyGeneration::new(0),
        after,
    )
    .unwrap();
    assert!(
        IndexPlan::new(
            binding(IndexProvenance::LocalFirstWitness),
            0,
            cut(0),
            after,
            batch(1),
            chunks()
        )
        .is_err()
    );
    let after = SourceCut::new(
        binding(IndexProvenance::Off).dataset(),
        TopologyGeneration::new(1),
        cut(2).bounds().to_vec(),
    )
    .unwrap();
    assert!(
        IndexPlan::new(
            binding(IndexProvenance::LocalFirstWitness),
            0,
            cut(0),
            after,
            batch(1),
            chunks()
        )
        .is_err()
    );
    assert!(
        IndexPlan::new(
            binding(IndexProvenance::LocalFirstWitness),
            1,
            cut(2),
            cut(4),
            batch(3),
            chunks()
        )
        .is_ok()
    );
}

#[test]
fn installation_schema_partition_and_semantics_are_digest_bound() {
    let original = plan(IndexProvenance::LocalFirstWitness);
    for schema in [0, 2, u16::MAX] {
        assert!(
            IndexChunk::new(
                IndexTarget::Events,
                schema,
                "all".to_owned(),
                Digest256::from_bytes([1; 32]),
                0,
                Digest256::from_bytes([2; 32])
            )
            .is_err()
        );
    }
    for partition in ["", "2026-10", "ALL", "all\0"] {
        assert!(
            IndexChunk::new(
                IndexTarget::Events,
                1,
                partition.to_owned(),
                Digest256::from_bytes([1; 32]),
                0,
                Digest256::from_bytes([2; 32])
            )
            .is_err()
        );
    }
    let mut changed = original.binding;
    changed.storage_incarnation = ProcessIncarnation::try_from([9; 16]).unwrap();
    assert_ne!(
        IndexPlan::new(changed, 0, cut(0), cut(2), batch(1), chunks())
            .unwrap()
            .digest(),
        original.digest()
    );
    let mut changed = original.binding;
    changed.table_identity_digest = Digest256::from_bytes([9; 32]);
    assert_ne!(
        IndexPlan::new(changed, 0, cut(0), cut(2), batch(1), chunks())
            .unwrap()
            .digest(),
        original.digest()
    );
    let mut changed = chunks();
    changed[1].rows_digest = Digest256::from_bytes([9; 32]);
    assert_ne!(
        IndexPlan::new(original.binding, 0, cut(0), cut(2), batch(1), changed)
            .unwrap()
            .digest(),
        original.digest()
    );
    let mut changed = chunks();
    changed[1].row_count = 1;
    assert_ne!(
        IndexPlan::new(original.binding, 0, cut(0), cut(2), batch(1), changed)
            .unwrap()
            .digest(),
        original.digest()
    );
}
