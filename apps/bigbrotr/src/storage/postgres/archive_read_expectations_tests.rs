//! Independent input reconstruction and ownership controls. Actual PG statement
//! execution, rollback poisoning and native readback remain separate real tests.
use super::*;
use crate::{
    runtime::{ProcessEnvelope, ResourceBudget, ResourceLimits, ResourceRequest, WaiterLimits},
    storage::rocks::store::PendingBatchHeader,
};
use bigbrotr_model::{
    AdmissionBatch, ArchiveBatch, FactRef, OwnerEpoch, Sequence, SourceRecordId, TopologyGeneration,
};
use bigbrotr_protocol::archive_receipt::{ReceiptExpectation, ReceiptRange};

fn budget() -> ResourceBudget {
    ResourceBudget::new(
        ProcessEnvelope::new(
            READ_METADATA_BYTES,
            0,
            ResourceLimits::new(READ_METADATA_BYTES, 1, 0, 0),
            WaiterLimits::new(1, 1),
        )
        .unwrap(),
    )
}
fn permit(budget: &ResourceBudget, bytes: u64, items: u64) -> OwnedPermit {
    budget
        .try_acquire(ResourceRequest::data(bytes, items, 0).unwrap())
        .unwrap()
}
struct Fixture {
    owner: ArchiveReadExpectations,
    request: ArchiveAttemptRequest,
    headers: Vec<PendingBatchHeader>,
    expectation: ReceiptExpectation,
}
impl Fixture {
    fn new(budget: &ResourceBudget) -> Self {
        let dataset = [1; 16].try_into().unwrap();
        let batch = AdmissionBatch::new(
            (1..=2)
                .map(|n| FactRef {
                    source_record_id: SourceRecordId {
                        dataset_id: dataset,
                        shard_id: 0,
                        owner_epoch: OwnerEpoch::new(1).unwrap(),
                        sequence: Sequence::new(n).unwrap(),
                    },
                    payload_digest: Digest256::from_bytes([9; 32]),
                })
                .collect(),
        )
        .unwrap();
        let archive =
            ArchiveBatch::new(1, TopologyGeneration::new(0), std::slice::from_ref(&batch)).unwrap();
        let request = ArchiveAttemptRequest::new(
            [6; 16],
            [2; 16].try_into().unwrap(),
            [4; 16].try_into().unwrap(),
            [5; 16].try_into().unwrap(),
            7,
            &archive,
        )
        .unwrap();
        let expectation = ReceiptExpectation {
            dataset_id: dataset,
            schema_version: 1,
            topology_generation: TopologyGeneration::new(0),
            batch_digest: archive.digest(),
            record_count: 2,
            input_ranges: vec![ReceiptRange {
                shard_id: 0,
                owner_epoch: 1,
                first_sequence: 1,
                last_sequence: 2,
            }],
            attempt_id: [6; 16],
            publication_ordinal: 8,
        };
        let cut = CutRead {
            dataset_id: dataset,
            topology_generation: TopologyGeneration::new(1),
            bounds: vec![
                CutBound {
                    shard_id: 0,
                    owner_epoch: 1,
                    complete_through: 2,
                },
                CutBound {
                    shard_id: 0,
                    owner_epoch: 2,
                    complete_through: 0,
                },
            ],
            receipts: vec![expectation.clone()],
            snapshot_id: Some(9),
        };
        let selection = ArchiveReadSelection {
            dataset_id: dataset,
            catalog_id: request.catalog(),
            frontier_revision: 2,
            snapshot_id: 9,
            topology_generation: cut.topology_generation,
            bounds: cut.bounds.clone(),
        };
        let current = Current {
            receipt: ArchiveFrontierReceipt {
                operation_id: [10; 16],
                consumer_id: request.catalog(),
                revision: 2,
                snapshot_id: 9,
                request_digest: cut.digest(),
                family_counts: [2, 0, 0, 0, 0],
                created: false,
            },
            topology: cut.topology_generation,
            bounds: cut
                .bounds
                .iter()
                .map(|value| super::super::Bound {
                    value: *value,
                    attestation: (value.complete_through > 0).then_some(request.batch_digest()),
                })
                .collect(),
            attempts: vec![SelectedAttempt {
                id: [6; 16],
                ordinal: 8,
                topology: TopologyGeneration::new(0),
                digest: request.batch_digest(),
                coordinator: request.coordinator(),
                worker: request.worker(),
            }],
        };
        Self {
            owner: ArchiveReadExpectations {
                selection,
                current,
                requests: vec![request.clone()],
                binding: Digest256::from_bytes([7; 32]),
                permit: permit(budget, READ_METADATA_BYTES, 1),
            },
            request,
            headers: vec![PendingBatchHeader::from_batch(&batch)],
            expectation,
        }
    }
    fn input(&self) -> Input<'_> {
        Input {
            request: &self.request,
            headers: &self.headers,
            expectation: &self.expectation,
            binding: self.owner.binding,
        }
    }
    fn read(&self) -> Result<CutRead, Error> {
        self.owner.bind(std::iter::once(self.input()))
    }
    fn zero(mut self) -> ArchiveReadExpectations {
        self.owner.requests.clear();
        self.owner.current.attempts.clear();
        for bound in &mut self.owner.current.bounds {
            bound.value.complete_through = 0;
            bound.attestation = None;
        }
        for bound in &mut self.owner.selection.bounds {
            bound.complete_through = 0;
        }
        self.owner.current.receipt.family_counts = [0; 5];
        self.owner.current.receipt.request_digest = CutRead {
            dataset_id: self.owner.selection.dataset_id,
            topology_generation: self.owner.selection.topology_generation,
            bounds: self.owner.selection.bounds.clone(),
            receipts: Vec::new(),
            snapshot_id: Some(9),
        }
        .digest();
        self.owner
    }
}

#[test]
fn ordinary_expectations_preserve_original_inputs_sparse_ordinals_and_old_snapshot() {
    let f = Fixture::new(&budget());
    let literal: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/storage/archive-inputs-v1.json"
    ))
    .unwrap();
    assert_eq!(
        serde_json::to_value(f.request.batch_digest()).unwrap(),
        literal["archive_digest"]
    );
    assert_eq!(
        serde_json::to_value(f.headers[0].digest()).unwrap(),
        literal["admission_digest"]
    );
    let cut = f.read().unwrap();
    assert_eq!(cut.validate(2), Ok(2));
    assert_eq!(cut.snapshot_id, Some(9));
    assert_eq!(cut.bounds, f.owner.selection().bounds);
    assert_eq!(cut.receipts[0].publication_ordinal, 8);
    assert_eq!(cut.receipts[0].topology_generation.get(), 0);
    assert_eq!(cut.topology_generation.get(), 1);
    assert_eq!(f.owner.requests(), std::slice::from_ref(&f.request));
}

#[test]
fn ordinary_expectations_refuse_missing_extra_foreign_and_corrupt_selected_inputs() {
    let f = Fixture::new(&budget());
    assert_eq!(
        f.owner.bind(std::iter::empty()),
        Err(Error::InvalidMetadata)
    );
    assert_eq!(
        f.owner.bind([f.input(), f.input()].into_iter()),
        Err(Error::InvalidMetadata)
    );
    let mut foreign = f.input();
    foreign.binding = Digest256::from_bytes([11; 32]);
    assert_eq!(
        f.owner.bind(std::iter::once(foreign)),
        Err(Error::InvalidMetadata)
    );
    for mutation in [
        "request",
        "snapshot",
        "digest",
        "count",
        "attestation",
        "zero",
        "range",
        "headers",
    ] {
        let mut f = Fixture::new(&budget());
        match mutation {
            "request" => f.owner.current.attempts[0].worker = [11; 16].try_into().unwrap(),
            "snapshot" => f.owner.current.receipt.snapshot_id = 10,
            "digest" => f.owner.current.receipt.request_digest = Digest256::from_bytes([11; 32]),
            "count" => f.owner.current.receipt.family_counts[1] = 1,
            "attestation" => {
                f.owner.current.bounds[0].attestation = Some(Digest256::from_bytes([11; 32]))
            }
            "zero" => f.owner.current.bounds[1].attestation = Some(f.request.batch_digest()),
            "range" => f.expectation.input_ranges[0].first_sequence = 2,
            "headers" => f.headers.clear(),
            _ => unreachable!(),
        }
        assert_eq!(f.read(), Err(Error::InvalidMetadata), "{mutation}");
    }
}

#[test]
fn ordinary_zero_selection_and_transfer_keep_the_grant_without_inventing_source() {
    let budget = budget();
    let owner = Fixture::new(&budget).zero();
    let selected = owner.selection().clone();
    let bound = owner.bind_inputs(&[]).unwrap();
    assert_eq!(bound.selection(), &selected);
    assert_eq!(bound.receipt().revision, 2);
    assert_eq!(bound.cut().validate(1), Ok(0));
    assert_eq!(bound.cut().bounds.len(), 2);
    assert_eq!(budget.snapshot().used().data_bytes(), READ_METADATA_BYTES);
    let (selection, receipt, cut, grant) = bound.into_parts();
    assert_eq!(selection, selected);
    assert_eq!(receipt.snapshot_id, 9);
    assert_eq!(cut.snapshot_id, Some(9));
    assert_eq!(budget.snapshot().active_permits(), 1);
    drop((selection, receipt, cut));
    assert_eq!(budget.snapshot().active_permits(), 1);
    drop(grant);
    assert_eq!(budget.snapshot().active_permits(), 0);
    let owner = Fixture::new(&budget).owner;
    assert!(matches!(
        owner.bind_inputs(&[]),
        Err(Error::InvalidMetadata)
    ));
    assert_eq!(budget.snapshot().active_permits(), 0);
}

#[test]
fn ordinary_metadata_reservation_and_sentinel_limits_refuse_before_vectors() {
    let budget = budget();
    let grant = permit(&budget, READ_METADATA_BYTES, 1);
    assert_eq!(capacity(1024, 128, &grant), Ok(()));
    assert_eq!(capacity(1025, 128, &grant), Err(Error::ResourceLimit));
    assert_eq!(capacity(1024, 129, &grant), Err(Error::ResourceLimit));
    assert_eq!(capacity(usize::MAX, 0, &grant), Err(Error::ResourceLimit));
    drop(grant);
    for (bytes, items) in [(READ_METADATA_BYTES - 1, 1), (READ_METADATA_BYTES, 0)] {
        assert_eq!(
            capacity(0, 0, &permit(&budget, bytes, items)),
            Err(Error::ResourceLimit)
        );
    }
}
