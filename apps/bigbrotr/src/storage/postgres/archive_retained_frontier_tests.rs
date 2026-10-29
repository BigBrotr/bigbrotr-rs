//! Pure reconstruction tests. They do not claim SQL locks, a positive native
//! lineage, catalog exclusion, receipt presence, publication or a read pin.
use super::*;
use crate::runtime::{
    ProcessEnvelope, ResourceBudget, ResourceLimits, ResourceRequest, WaiterLimits,
};
use bigbrotr_model::{AdmissionBatch, ArchiveBatch, FactRef, OwnerEpoch, Sequence, SourceRecordId};

fn budget() -> ResourceBudget {
    ResourceBudget::new(
        ProcessEnvelope::new(
            2 * METADATA_BYTES,
            0,
            ResourceLimits::new(2 * METADATA_BYTES, 2, 0, 0),
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
fn batch(first: u64, last: u64) -> AdmissionBatch {
    AdmissionBatch::new(
        (first..=last)
            .map(|sequence| FactRef {
                source_record_id: SourceRecordId {
                    dataset_id: [1; 16].try_into().unwrap(),
                    shard_id: 0,
                    owner_epoch: OwnerEpoch::new(1).unwrap(),
                    sequence: Sequence::new(sequence).unwrap(),
                },
                payload_digest: Digest256::from_bytes([9; 32]),
            })
            .collect(),
    )
    .unwrap()
}
struct Fixture {
    request: ArchiveAttemptRequest,
    headers: Vec<PendingBatchHeader>,
    expectation: ReceiptExpectation,
    owner: RetainedArchiveFrontier,
}
impl Fixture {
    fn new(split: bool) -> Self {
        let batches = if split {
            vec![batch(1, 1), batch(2, 2)]
        } else {
            vec![batch(1, 2)]
        };
        let archive = ArchiveBatch::new(1, TopologyGeneration::new(0), &batches).unwrap();
        // Ordinals1..7 may belong to retained failed attempts. Selected receipt8
        // is still a complete source prefix; publication gaps are not fact gaps.
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
            dataset_id: request.dataset(),
            schema_version: 1,
            topology_generation: request.topology_generation(),
            batch_digest: request.batch_digest(),
            record_count: 2,
            input_ranges: vec![ReceiptRange {
                shard_id: 0,
                owner_epoch: 1,
                first_sequence: 1,
                last_sequence: 2,
            }],
            attempt_id: *request.attempt_id(),
            publication_ordinal: request.ordinal(),
        };
        let read = CutRead {
            dataset_id: request.dataset(),
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
        let owner = RetainedArchiveFrontier {
            dataset: request.dataset(),
            catalog: request.catalog(),
            binding: Digest256::from_bytes([7; 32]),
            issued: 8,
            current: Some(Current {
                receipt: ArchiveFrontierReceipt {
                    operation_id: [10; 16],
                    consumer_id: request.catalog(),
                    revision: 2,
                    snapshot_id: 9,
                    request_digest: read.digest(),
                    family_counts: [2, 0, 0, 0, 0],
                    created: false,
                },
                topology: read.topology_generation,
                bounds: read
                    .bounds
                    .iter()
                    .map(|value| Bound {
                        value: *value,
                        attestation: (value.complete_through > 0).then_some(request.batch_digest()),
                    })
                    .collect(),
                attempts: vec![SelectedAttempt {
                    id: *request.attempt_id(),
                    ordinal: request.ordinal(),
                    topology: request.topology_generation(),
                    digest: request.batch_digest(),
                    coordinator: request.coordinator(),
                    worker: request.worker(),
                }],
            }),
            permit: permit(&budget(), METADATA_BYTES, 1),
            phase: Phase::Loaded,
            compacted: None,
            bound: None,
        };
        Self {
            request,
            headers: batches.iter().map(PendingBatchHeader::from_batch).collect(),
            expectation,
            owner,
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
    fn read(&self) -> Result<Option<CutRead>, Error> {
        self.owner.bind(std::iter::once(self.input()))
    }
}

#[test]
fn old_topology_and_sparse_attempts_retain_exact_snapshot_and_complete_headers() {
    let literal: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/storage/archive-inputs-v1.json"
    ))
    .unwrap();
    let f = Fixture::new(false);
    assert_eq!(
        serde_json::to_value(f.request.batch_digest()).unwrap(),
        literal["archive_digest"]
    );
    assert_eq!(
        serde_json::to_value(f.headers[0].digest()).unwrap(),
        literal["admission_digest"]
    );
    let read = f.read().unwrap().unwrap();
    assert_eq!(read.validate(2), Ok(2));
    assert_eq!(read.receipts[0].publication_ordinal, 8);
    assert_eq!(read.receipts[0].topology_generation.get(), 0);
    assert_eq!(read.topology_generation.get(), 1);
    assert_eq!(read.snapshot_id, Some(9));
    assert_eq!(
        f.owner.selected_attempt_ids().unwrap().collect::<Vec<_>>(),
        vec![[6; 16]]
    );
    // Header grouping remains independently meaningful even when the semantic
    // Archive digest and coalesced range are the same.
    let split = Fixture::new(true);
    assert_eq!(split.headers.len(), 2);
    assert_eq!(split.request.batch_digest(), f.request.batch_digest());
    assert_ne!(split.headers, f.headers);
    assert_eq!(split.read().unwrap().unwrap(), read);
}

#[test]
fn missing_duplicate_foreign_or_different_selected_input_cannot_match() {
    let mut f = Fixture::new(false);
    assert_eq!(
        f.owner.bind(std::iter::empty()),
        Err(Error::InvalidMetadata)
    );
    assert_eq!(
        f.owner.bind([f.input(), f.input()].into_iter()),
        Err(Error::InvalidMetadata)
    );
    let mut foreign = f.input();
    foreign.binding = Digest256::from_bytes([8; 32]);
    assert_eq!(
        f.owner.bind(std::iter::once(foreign)),
        Err(Error::InvalidMetadata)
    );
    f.owner.current.as_mut().unwrap().attempts[0].worker = [11; 16].try_into().unwrap();
    assert_eq!(f.read(), Err(Error::InvalidMetadata));
    f.owner.current.as_mut().unwrap().attempts[0].worker = f.request.worker();
    f.owner.current.as_mut().unwrap().attempts[0].topology = TopologyGeneration::new(1);
    assert_eq!(f.read(), Err(Error::InvalidMetadata));
}

#[test]
fn exact_request_snapshot_attestation_and_family_counts_are_bound() {
    let mut f = Fixture::new(false);
    let receipt = f.owner.current_receipt().unwrap().unwrap();
    f.owner.current.as_mut().unwrap().receipt.snapshot_id += 1;
    assert_eq!(f.read(), Err(Error::InvalidMetadata));
    f.owner.current.as_mut().unwrap().receipt = receipt;
    f.owner.current.as_mut().unwrap().receipt.family_counts[1] = 1;
    assert_eq!(f.read(), Err(Error::InvalidMetadata));
    f.owner.current.as_mut().unwrap().receipt = receipt;
    f.owner.current.as_mut().unwrap().bounds[0].attestation = Some(Digest256::from_bytes([0; 32]));
    assert_eq!(f.read(), Err(Error::InvalidMetadata));
    f.owner.current.as_mut().unwrap().bounds[0].attestation = Some(f.request.batch_digest());
    f.owner.current.as_mut().unwrap().bounds[1].attestation = Some(f.request.batch_digest());
    assert_eq!(f.read(), Err(Error::InvalidMetadata));
}

#[test]
fn complete_prefix_refuses_partial_overlap_missing_header_and_range_relabeling() {
    let mut f = Fixture::new(true);
    f.headers.reverse();
    assert_eq!(f.read(), Err(Error::InvalidMetadata));
    f.headers.reverse();
    f.headers.pop();
    assert_eq!(f.read(), Err(Error::InvalidMetadata));
    f.headers.push(PendingBatchHeader::from_batch(&batch(1, 2)));
    assert_eq!(f.read(), Err(Error::InvalidMetadata));
    let mut f = Fixture::new(false);
    f.expectation.input_ranges[0].owner_epoch = 2;
    assert_eq!(f.read(), Err(Error::InvalidMetadata));
    let mut f = Fixture::new(false);
    f.owner.current.as_mut().unwrap().bounds[0]
        .value
        .complete_through = 1;
    assert_eq!(f.read(), Err(Error::InvalidMetadata));
}

#[test]
fn only_complete_terminal_history_is_a_loader_prerequisite() {
    for epoch in [2, 3, 255] {
        assert_eq!(
            require_terminal(SuccessorHistoryState::Terminal, epoch, epoch, epoch, 0),
            Ok(())
        );
    }
    for (state, native, registered, through) in [
        (SuccessorHistoryState::PendingNative, 2, 3, 2),
        (SuccessorHistoryState::PreparedOpen, 3, 3, 2),
        (SuccessorHistoryState::PreparedClosing, 3, 3, 2),
        (SuccessorHistoryState::Terminal, 3, 3, 2),
        (SuccessorHistoryState::Terminal, 2, 3, 2),
        (SuccessorHistoryState::Terminal, 1, 1, 1),
        (SuccessorHistoryState::Terminal, 256, 256, 256),
    ] {
        assert_eq!(
            require_terminal(state, native, registered, through, 0),
            Err(Error::InvalidInput)
        );
    }
    // The actual SQL path only invokes this absence branch when the consumer
    // itself is absent. An existing consumer with any missing LEFT JOIN refuses.
    assert_eq!(require_absent(false), Ok(()));
    assert_eq!(require_absent(true), Err(Error::InvalidMetadata));
}

#[test]
fn compacted_history_is_not_an_archive_frontier_prerequisite() {
    for epoch in [2, 3, 255] {
        for compacted in [1, 7, 64] {
            assert_eq!(
                require_terminal(
                    SuccessorHistoryState::Terminal,
                    epoch,
                    epoch,
                    epoch,
                    compacted
                ),
                Err(Error::InvalidInput)
            );
        }
    }
}

#[test]
fn metadata_ownership_and_whole_job_limit_include_failed_attempts() {
    let budget = budget();
    let grant = permit(&budget, METADATA_BYTES, 1);
    assert_eq!(require_capacity(128, &grant), Ok(()));
    assert_eq!(require_capacity(129, &grant), Err(Error::ResourceLimit));
    drop(grant);
    for (bytes, items) in [(METADATA_BYTES - 1, 1), (METADATA_BYTES, 0)] {
        assert_eq!(
            require_capacity(0, &permit(&budget, bytes, items)),
            Err(Error::ResourceLimit)
        );
    }
    let absent = RetainedArchiveFrontier {
        dataset: [1; 16].try_into().unwrap(),
        catalog: [2; 16].try_into().unwrap(),
        binding: Digest256::from_bytes([7; 32]),
        issued: 128,
        current: None,
        permit: permit(&budget, METADATA_BYTES, 1),
        phase: Phase::Loaded,
        compacted: None,
        bound: None,
    };
    assert!(absent.current_receipt().unwrap().is_none());
    let bound = absent.bind_current_inputs(&[]).unwrap();
    assert!(bound.current_cut().is_none());
    assert_eq!(bound.issued_attempts(), 128);
    assert!(bound.admits_new_attempts(0));
    assert!(!bound.admits_new_attempts(1));
    assert!(!bound.admits_new_attempts(u64::MAX));
    assert_eq!(budget.snapshot().active_permits(), 1);
    assert_eq!(budget.snapshot().used().data_bytes(), METADATA_BYTES);
    drop(bound);
    assert_eq!(budget.snapshot().active_permits(), 0);
}

#[test]
fn compacted_frontier_reservation_and_bind_errors_preserve_outer_check_and_state() {
    // Pure owner lifecycle control: no SQL observation is fabricated by a loader.
    let limits = AdmissionLimits::default();
    let bytes = crate::storage::rocks::store::compacted_outcome_data_bytes(limits).unwrap();
    let effects = ResourceBudget::new(
        ProcessEnvelope::new(
            bytes,
            0,
            ResourceLimits::new(bytes, 1, 0, 0),
            WaiterLimits::new(1, 1),
        )
        .unwrap(),
    );
    let mut f = Fixture::new(false);
    f.owner.compacted =
        Some(SuccessorCompactionCheck::new(limits, permit(&effects, bytes, 1)).unwrap());
    let check = std::ptr::from_ref(f.owner.compacted.as_ref().unwrap());
    let retained = f.owner.current_receipt().unwrap();
    f.owner.phase = Phase::Reserved;
    assert_eq!(f.owner.current_receipt(), Err(Error::InvalidInput));
    assert_eq!(f.owner.issued_attempts(), Err(Error::InvalidInput));
    assert!(f.owner.selected_attempt_ids().is_err());
    assert!(f.owner.bound_current_inputs().is_err());
    assert_eq!(
        f.owner.bind_current_inputs_in_place(&[]),
        Err(Error::InvalidInput)
    );
    assert_eq!(f.owner.phase, Phase::Reserved);
    assert_eq!(effects.snapshot().used().data_bytes(), bytes);
    assert_eq!(effects.snapshot().active_permits(), 1);

    f.owner.phase = Phase::Loaded;
    assert_eq!(
        f.owner.bind_current_inputs_in_place(&[]),
        Err(Error::InvalidMetadata)
    );
    assert_eq!(f.owner.phase, Phase::Loaded);
    assert_eq!(f.owner.current_receipt().unwrap(), retained);
    assert_eq!(
        std::ptr::from_ref(f.owner.compacted.as_ref().unwrap()),
        check
    );
    assert_eq!(effects.snapshot().used().data_bytes(), bytes);
    assert_eq!(effects.snapshot().active_permits(), 1);

    // An independently represented genuinely absent observation may bind empty;
    // Reserved above could not. Binding still does not transfer/drop Check.
    f.owner.current = None;
    f.owner.issued = 0;
    f.owner.bind_current_inputs_in_place(&[]).unwrap();
    assert_eq!(f.owner.phase, Phase::Bound);
    assert!(f.owner.current_receipt().is_err());
    assert!(
        f.owner
            .bound_current_inputs()
            .unwrap()
            .current_receipt()
            .is_none()
    );
    assert_eq!(
        std::ptr::from_ref(f.owner.compacted.as_ref().unwrap()),
        check
    );
    assert_eq!(effects.snapshot().active_permits(), 1);
    assert!(f.owner.bind_current_inputs_in_place(&[]).is_err());
    drop(f);
    assert_eq!(effects.snapshot().active_permits(), 0);
}

#[test]
fn compacted_frontier_requires_exact_dataset_catalog_binding_and_terminal_basis() {
    let f = Fixture::new(false);
    assert_eq!(
        f.owner
            .require_binding(f.owner.dataset, f.owner.catalog, f.owner.binding),
        Ok(())
    );
    assert_eq!(
        f.owner.require_binding(
            [11; 16].try_into().unwrap(),
            f.owner.catalog,
            f.owner.binding
        ),
        Err(Error::InvalidInput)
    );
    assert_eq!(
        f.owner.require_binding(
            f.owner.dataset,
            [11; 16].try_into().unwrap(),
            f.owner.binding
        ),
        Err(Error::InvalidInput)
    );
    assert_eq!(
        f.owner.require_binding(
            f.owner.dataset,
            f.owner.catalog,
            Digest256::from_bytes([11; 32])
        ),
        Err(Error::InvalidInput)
    );
    for count in [1, 7, 64] {
        for epoch in [2, 3, 255] {
            assert_eq!(
                require_terminal_in(
                    SuccessorHistoryState::Terminal,
                    epoch,
                    epoch,
                    epoch,
                    count,
                    true
                ),
                Ok(())
            );
            assert_eq!(
                require_terminal_in(
                    SuccessorHistoryState::Terminal,
                    epoch,
                    epoch,
                    epoch,
                    count,
                    false
                ),
                Err(Error::InvalidInput)
            );
        }
    }
    for (state, native, registered, through, count) in [
        (SuccessorHistoryState::Terminal, 1, 1, 1, 7),
        (SuccessorHistoryState::Terminal, 2, 2, 2, 0),
        (SuccessorHistoryState::Terminal, 2, 2, 2, 65),
        (SuccessorHistoryState::Terminal, 2, 3, 2, 64),
        (SuccessorHistoryState::Terminal, 3, 3, 3, 0),
        (SuccessorHistoryState::Terminal, 3, 3, 3, 65),
        (SuccessorHistoryState::Terminal, 3, 4, 3, 7),
        (SuccessorHistoryState::Terminal, 3, 3, 2, 7),
        (SuccessorHistoryState::PreparedClosing, 3, 3, 3, 7),
    ] {
        assert_eq!(
            require_terminal_in(state, native, registered, through, count, true),
            Err(Error::InvalidInput)
        );
    }
    assert!(
        f.owner.compacted_check().is_err(),
        "ordinary observation is no compacted authority"
    );
}
