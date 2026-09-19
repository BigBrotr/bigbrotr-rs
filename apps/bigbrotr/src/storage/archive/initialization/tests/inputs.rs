use super::*;
use crate::{
    runtime::{
        OwnedPermit, ProcessEnvelope, ResourceBudget, ResourceLimits, ResourceRequest, WaiterLimits,
    },
    storage::{
        archive::{InputError, LocalCoordinator, RetainedInputs},
        postgres::archive_attempt::ArchiveAttemptRequest,
    },
};
use bigbrotr_model::{
    AdmissionBatch, ArchiveBatch, Digest256, FactRef, OwnerEpoch, Sequence, SourceRecordId,
    TopologyGeneration,
};
use bigbrotr_protocol::archive_receipt::ReceiptRange;
use std::sync::atomic::AtomicBool;

fn permit() -> OwnedPermit {
    ResourceBudget::new(
        ProcessEnvelope::new(
            2 << 20,
            1 << 20,
            ResourceLimits::new(1 << 20, 1, 0, 0),
            WaiterLimits::new(0, 0),
        )
        .unwrap(),
    )
    .try_acquire(ResourceRequest::data(1 << 20, 1, 0).unwrap())
    .unwrap()
}
fn batches() -> Vec<AdmissionBatch> {
    [
        (0, 1, 1, 2),
        (0, 1, 3, 3),
        (0, 1, 5, 5),
        (0, 2, 1, 1),
        (2, 1, u64::MAX, u64::MAX),
    ]
    .into_iter()
    .map(|(shard_id, epoch, first, last)| {
        AdmissionBatch::new(
            (first..=last)
                .map(|sequence| FactRef {
                    source_record_id: SourceRecordId {
                        dataset_id: [1; 16].try_into().unwrap(),
                        shard_id,
                        owner_epoch: OwnerEpoch::new(epoch).unwrap(),
                        sequence: Sequence::new(sequence).unwrap(),
                    },
                    payload_digest: Digest256::from_bytes([9; 32]),
                })
                .collect(),
        )
        .unwrap()
    })
    .collect()
}
fn request(batch: &ArchiveBatch<'_>) -> ArchiveAttemptRequest {
    ArchiveAttemptRequest::new(
        [6; 16],
        [2; 16].try_into().unwrap(),
        [4; 16].try_into().unwrap(),
        [5; 16].try_into().unwrap(),
        7,
        batch,
    )
    .unwrap()
}
fn coordinator(identity: &CatalogIdentity, state: &Path, initialize: bool) -> LocalCoordinator {
    LocalCoordinator::from_custody(recover_released(identity), state, initialize).unwrap()
}

#[test]
fn retains_complete_grouping_before_issuance_and_recovers_receipt_without_facts() {
    let (_data, identity) = fixture();
    let (state, _) = fixture();
    let root = state.path().canonicalize().unwrap();
    drop(LocalInitialization::create_for_boot(&identity, [3; 16], BOOT).unwrap());
    let owner = coordinator(&identity, &root, true);
    let binding = Digest256::from_bytes(
        Sha256::digest(fs::read(root.join("coordinator.json")).unwrap()).into(),
    );
    assert_eq!(owner.binding_digest(), binding);
    let inputs = batches();
    let batch = ArchiveBatch::new(1, TopologyGeneration::new(0), &inputs).unwrap();
    let request = request(&batch);
    let cancel = AtomicBool::new(false);
    assert!(
        owner
            .read_inputs(&request, permit(), &cancel)
            .unwrap()
            .is_none()
    );
    let retained = owner
        .retain_inputs(&request, &batch, permit(), &cancel)
        .unwrap();
    assert_eq!(retained.coordinator_binding(), binding);
    assert_eq!(retained.headers().len(), 5);
    assert_eq!(retained.expectation().record_count, 6);
    assert_eq!(
        retained.expectation().input_ranges,
        vec![
            ReceiptRange {
                shard_id: 0,
                owner_epoch: 1,
                first_sequence: 1,
                last_sequence: 3
            },
            ReceiptRange {
                shard_id: 0,
                owner_epoch: 1,
                first_sequence: 5,
                last_sequence: 5
            },
            ReceiptRange {
                shard_id: 0,
                owner_epoch: 2,
                first_sequence: 1,
                last_sequence: 1
            },
            ReceiptRange {
                shard_id: 2,
                owner_epoch: 1,
                first_sequence: u64::MAX,
                last_sequence: u64::MAX
            },
        ]
    );
    assert_eq!(retained.verify_replay(&batch), Ok(()));
    let path = root.join("06060606060606060606060606060606.inputs");
    let original = fs::read(&path).unwrap();
    let retried = owner
        .retain_inputs(&request, &batch, permit(), &cancel)
        .unwrap();
    assert_eq!(fs::read(&path).unwrap(), original);
    drop(retried);
    drop(owner);
    assert!(
        matches!(
            LocalInitialization::recover_for_boot(&identity, BOOT),
            Err(LifetimeError::Occupied)
        ),
        "retained inputs keep coordinator custody during issuance"
    );
    drop(retained);
    drop(batch);
    drop(inputs);
    let owner = coordinator(&identity, &root, false);
    let read = owner
        .read_inputs(&request, permit(), &cancel)
        .unwrap()
        .unwrap();
    assert_eq!(read.expectation().record_count, 6);
    assert_eq!(read.request(), &request);
    assert_eq!(read.coordinator_binding(), binding);
    assert_eq!(
        read.verify_replay(&ArchiveBatch::new(1, TopologyGeneration::new(0), &batches()).unwrap()),
        Ok(())
    );
    assert_eq!(fs::read(&path).unwrap(), original);
}

#[test]
fn refuses_changed_grouping_and_partial_bytes_without_repair() {
    let (_data, identity) = fixture();
    let (state, _) = fixture();
    let root = state.path().canonicalize().unwrap();
    drop(LocalInitialization::create_for_boot(&identity, [3; 16], BOOT).unwrap());
    let owner = coordinator(&identity, &root, true);
    let inputs = batches();
    let batch = ArchiveBatch::new(1, TopologyGeneration::new(0), &inputs).unwrap();
    let request = request(&batch);
    let cancel = AtomicBool::new(false);
    let retained = owner
        .retain_inputs(&request, &batch, permit(), &cancel)
        .unwrap();
    let mut regrouped = vec![
        AdmissionBatch::new(
            inputs[0]
                .records()
                .iter()
                .chain(inputs[1].records())
                .copied()
                .collect(),
        )
        .unwrap(),
    ];
    regrouped.extend(inputs[2..].iter().cloned());
    let regrouped = ArchiveBatch::new(1, TopologyGeneration::new(0), &regrouped).unwrap();
    assert_eq!(
        batch.digest(),
        regrouped.digest(),
        "semantic digest deliberately ignores admission grouping"
    );
    assert_eq!(
        retained.verify_replay(&regrouped),
        Err(InputError::Identity)
    );
    assert!(matches!(
        owner.retain_inputs(&request, &regrouped, permit(), &cancel),
        Err(InputError::Identity)
    ));
    let path = root.join("06060606060606060606060606060606.inputs");
    fs::write(&path, b"partial").unwrap();
    assert!(owner.read_inputs(&request, permit(), &cancel).is_err());
    assert!(
        owner
            .retain_inputs(&request, &batch, permit(), &cancel)
            .is_err()
    );
    assert_eq!(fs::read(&path).unwrap(), b"partial");
    assert!(RetainedInputs::required_data_bytes() <= 1 << 20);
}

#[test]
fn cancellation_short_grants_links_and_oversized_inputs_preserve_state() {
    use std::os::unix::fs::symlink;
    let (_data, identity) = fixture();
    let (state, _) = fixture();
    let root = state.path().canonicalize().unwrap();
    drop(LocalInitialization::create_for_boot(&identity, [3; 16], BOOT).unwrap());
    let owner = coordinator(&identity, &root, true);
    let inputs = batches();
    let batch = ArchiveBatch::new(1, TopologyGeneration::new(0), &inputs).unwrap();
    let request = request(&batch);
    let path = root.join("06060606060606060606060606060606.inputs");
    assert!(matches!(
        owner.retain_inputs(&request, &batch, permit(), &AtomicBool::new(true)),
        Err(InputError::Cancelled)
    ));
    let budget = ResourceBudget::new(
        ProcessEnvelope::new(
            1 << 20,
            0,
            ResourceLimits::new(1 << 20, 1, 0, 0),
            WaiterLimits::new(0, 0),
        )
        .unwrap(),
    );
    for (bytes, items) in [((1 << 20) - 1, 1), (1 << 20, 0)] {
        let small = budget
            .try_acquire(ResourceRequest::data(bytes, items, 0).unwrap())
            .unwrap();
        assert!(matches!(
            owner.retain_inputs(&request, &batch, small, &AtomicBool::new(false)),
            Err(InputError::ResourceLimit)
        ));
        assert_eq!(budget.snapshot().active_permits(), 0);
    }
    assert!(!path.exists());
    let cancel = AtomicBool::new(false);
    drop(
        owner
            .retain_inputs(&request, &batch, permit(), &cancel)
            .unwrap(),
    );
    let original = fs::read(&path).unwrap();
    let copied = root.join("fixture-copy");
    fs::rename(&path, &copied).unwrap();
    symlink(&copied, &path).unwrap();
    assert!(owner.read_inputs(&request, permit(), &cancel).is_err());
    assert!(
        owner
            .retain_inputs(&request, &batch, permit(), &cancel)
            .is_err()
    );
    assert_eq!(fs::read(&copied).unwrap(), original);
    fs::remove_file(&path).unwrap();
    fs::hard_link(&copied, &path).unwrap();
    assert!(owner.read_inputs(&request, permit(), &cancel).is_err());
    fs::remove_file(&path).unwrap();
    fs::rename(&copied, &path).unwrap();
    fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(64 * 1024)
        .unwrap();
    assert!(matches!(
        owner.read_inputs(&request, permit(), &cancel),
        Err(InputError::ResourceLimit)
    ));
    assert_eq!(fs::metadata(&path).unwrap().len(), 64 * 1024);
}

#[test]
fn header_limit_is_inclusive_and_large_counts_do_not_expand_into_fact_bodies() {
    let (_data, identity) = fixture();
    let (state, _) = fixture();
    let root = state.path().canonicalize().unwrap();
    drop(LocalInitialization::create_for_boot(&identity, [3; 16], BOOT).unwrap());
    let owner = coordinator(&identity, &root, true);
    let mut inputs: Vec<_> = (1..=1024)
        .map(|sequence| {
            AdmissionBatch::new(vec![FactRef {
                source_record_id: SourceRecordId {
                    dataset_id: identity.dataset(),
                    shard_id: 0,
                    owner_epoch: OwnerEpoch::new(1).unwrap(),
                    sequence: Sequence::new(sequence).unwrap(),
                },
                payload_digest: Digest256::from_bytes([9; 32]),
            }])
            .unwrap()
        })
        .collect();
    let batch = ArchiveBatch::new(1, TopologyGeneration::new(0), &inputs).unwrap();
    let request = request(&batch);
    let cancel = AtomicBool::new(false);
    let retained = owner
        .retain_inputs(&request, &batch, permit(), &cancel)
        .unwrap();
    assert_eq!(retained.expectation().record_count, 1024);
    assert_eq!(retained.expectation().input_ranges.len(), 1);
    assert_eq!(
        fs::metadata(root.join("06060606060606060606060606060606.inputs"))
            .unwrap()
            .len(),
        63692
    );
    inputs.push(
        AdmissionBatch::new(vec![FactRef {
            source_record_id: SourceRecordId {
                sequence: Sequence::new(1025).unwrap(),
                ..inputs[0].first()
            },
            payload_digest: Digest256::from_bytes([9; 32]),
        }])
        .unwrap(),
    );
    let too_many = ArchiveBatch::new(1, TopologyGeneration::new(0), &inputs).unwrap();
    assert!(matches!(
        owner.retain_inputs(&request, &too_many, permit(), &cancel),
        Err(InputError::ResourceLimit)
    ));
}
