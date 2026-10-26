use super::*;
use crate::storage::clickhouse::rows::tests::{cut, facts, generation};
use bigbrotr_model::{DatasetId, Digest256, OwnerResourceId, ProcessIncarnation};

fn binding(provenance: IndexProvenance) -> IndexBinding {
    IndexBinding::new(
        cut().dataset_id(),
        OwnerResourceId::try_from([0x44; 16]).unwrap(),
        generation(),
        ProcessIncarnation::try_from([0x55; 16]).unwrap(),
        Digest256::from_bytes([0x66; 32]),
        Digest256::from_bytes([0x77; 32]),
        provenance,
    )
    .unwrap()
}
fn before() -> SourceCut {
    let cut = cut();
    let mut bounds = cut.bounds().to_vec();
    bounds[1].last_complete_sequence = CompleteThrough::new(0);
    SourceCut::new(cut.dataset_id(), cut.topology_generation(), bounds).unwrap()
}
fn limits() -> PendingReadLimits {
    PendingReadLimits {
        max_records: 5,
        max_encoded_bytes: 16 * 1024,
        fact: FactDecodeLimits::default(),
    }
}
fn input(count: usize) -> (AdmissionBatch, Vec<Vec<u8>>) {
    let facts = facts();
    (
        AdmissionBatch::new(
            facts[..count]
                .iter()
                .map(|fact| FactRef {
                    source_record_id: fact.source_record_id(),
                    payload_digest: fact.payload_digest(),
                })
                .collect(),
        )
        .unwrap(),
        facts[..count]
            .iter()
            .map(|fact| {
                fact.encode_preimage(limits().fact.max_encoded_bytes)
                    .unwrap()
            })
            .collect(),
    )
}

#[test]
fn complete_plan_matches_existing_independent_ten_family_vectors() {
    let (batch, records) = input(5);
    let (plan, wire) = plan_source(
        binding(IndexProvenance::LocalFirstWitness),
        1,
        before(),
        &batch,
        &records,
        limits(),
        &AtomicBool::new(false),
    )
    .unwrap();
    let golden: serde_json::Value =
        serde_json::from_str(include_str!("../../clickhouse/rows/fixtures/golden.json")).unwrap();
    assert_eq!(plan.before(), &before());
    assert_eq!(plan.after(), &cut());
    assert_eq!(plan.source().first(), batch.first());
    assert_eq!(plan.source().last(), batch.last());
    for (i, chunk) in plan.chunks().iter().enumerate() {
        let expected = &golden["targets"][chunk.target().as_str()];
        assert_eq!(chunk.row_count(), expected["row_count"].as_u64().unwrap());
        assert_eq!(
            chunk.rows_digest().to_string(),
            expected["rows_digest"].as_str().unwrap()
        );
        assert_eq!(chunk.chunk_id(), chunk.rows_digest());
        assert_eq!(wire[i], expected["row_binary_bytes"].as_u64().unwrap());
    }
}

#[test]
fn off_retains_zero_outcomes_and_refuses_a_witness_instead_of_skipping_it() {
    let (batch, records) = input(4);
    let (plan, wire) = plan_source(
        binding(IndexProvenance::Off),
        1,
        before(),
        &batch,
        &records,
        limits(),
        &AtomicBool::new(false),
    )
    .unwrap();
    assert_eq!(plan.after().bounds()[0], before().bounds()[0]);
    assert_eq!(plan.after().bounds()[1].last_complete_sequence.get(), 4);
    assert_eq!(
        plan.chunks().each_ref().map(|chunk| chunk.row_count()),
        [1, 1, 1, 1, 1, 1, 1, 6, 0, 0]
    );
    assert_eq!(&wire[8..], &[0, 0]);
    assert_ne!(
        plan.chunks()[8].rows_digest(),
        plan.chunks()[9].rows_digest()
    );
    let (batch, records) = input(5);
    assert_eq!(
        plan_source(
            binding(IndexProvenance::Off),
            1,
            before(),
            &batch,
            &records,
            limits(),
            &AtomicBool::new(false),
        )
        .unwrap_err(),
        PlanError::Rows(RowsError::ProvenanceDisabled)
    );
}

#[test]
fn gaps_wrong_sources_corrupt_bytes_and_local_limits_cannot_make_a_plan() {
    let (batch, records) = input(5);
    let run = |before, records: &[Vec<u8>], limits| {
        plan_source(
            binding(IndexProvenance::LocalFirstWitness),
            1,
            before,
            &batch,
            records,
            limits,
            &AtomicBool::new(false),
        )
    };
    assert_eq!(
        run(cut(), &records, limits()).unwrap_err(),
        PlanError::InvalidSource
    );
    let foreign = SourceCut::new(
        DatasetId::try_from([0x99; 16]).unwrap(),
        before().topology_generation(),
        before().bounds().to_vec(),
    )
    .unwrap();
    assert_eq!(
        run(foreign, &records, limits()).unwrap_err(),
        PlanError::InvalidSource
    );
    assert_eq!(
        run(before(), &records[..4], limits()).unwrap_err(),
        PlanError::InvalidSource
    );
    let mut swapped = records.clone();
    swapped.swap(0, 1);
    assert!(matches!(
        run(before(), &swapped, limits()),
        Err(PlanError::Decode(_))
    ));
    let mut corrupt = records.clone();
    *corrupt[0].last_mut().unwrap() ^= 1;
    assert!(matches!(
        run(before(), &corrupt, limits()),
        Err(PlanError::Decode(_))
    ));
    assert_eq!(
        run(
            before(),
            &records,
            PendingReadLimits {
                max_records: 4,
                ..limits()
            }
        )
        .unwrap_err(),
        PlanError::SourceLimit
    );
    assert_eq!(
        run(
            before(),
            &records,
            PendingReadLimits {
                max_encoded_bytes: 1,
                ..limits()
            }
        )
        .unwrap_err(),
        PlanError::SourceLimit
    );
    assert_eq!(
        plan_source(
            binding(IndexProvenance::LocalFirstWitness),
            1,
            before(),
            &batch,
            &corrupt,
            limits(),
            &AtomicBool::new(true)
        )
        .unwrap_err(),
        PlanError::Cancelled
    );
}

#[test]
fn allocation_floor_rejects_zero_and_overflow_without_allocating() {
    let valid = limits();
    assert!(required_data_bytes(valid).unwrap() > valid.fact.max_allocation_bytes as u64);
    assert_eq!(
        required_data_bytes(PendingReadLimits {
            max_records: 0,
            ..valid
        }),
        Err(PlanError::InvalidLimits)
    );
    assert_eq!(
        required_data_bytes(PendingReadLimits {
            max_records: usize::MAX,
            ..valid
        }),
        Err(PlanError::InvalidLimits)
    );
}
