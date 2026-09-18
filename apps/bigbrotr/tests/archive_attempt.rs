use bigbrotr::storage::postgres::archive_attempt::{ArchiveAttemptError, ArchiveAttemptRequest};
use bigbrotr_model::{
    AdmissionBatch, ArchiveBatch, DatasetId, Digest256, FactRef, OwnerEpoch, OwnerResourceId,
    ProcessIncarnation, Sequence, SourceRecordId, TopologyGeneration,
};

#[test]
fn request_binds_archive_membership_and_refuses_invalid_attempt_lifetimes() {
    let input = [AdmissionBatch::new(vec![FactRef {
        source_record_id: SourceRecordId {
            dataset_id: DatasetId::try_from([1; 16]).unwrap(),
            shard_id: 0,
            owner_epoch: OwnerEpoch::new(1).unwrap(),
            sequence: Sequence::new(1).unwrap(),
        },
        payload_digest: Digest256::from_bytes([2; 32]),
    }])
    .unwrap()];
    let catalog = OwnerResourceId::try_from([3; 16]).unwrap();
    let coordinator = ProcessIncarnation::try_from([4; 16]).unwrap();
    let worker = ProcessIncarnation::try_from([5; 16]).unwrap();
    let batch = ArchiveBatch::new(1, TopologyGeneration::new(u64::MAX), &input).unwrap();
    let request =
        ArchiveAttemptRequest::new([6; 16], catalog, coordinator, worker, u64::MAX - 1, &batch)
            .unwrap();
    assert_eq!(request.batch_digest(), batch.digest());
    assert_eq!(request.dataset(), batch.dataset_id());
    assert_eq!(request.topology_generation().get(), u64::MAX);
    assert_eq!(request.ordinal(), u64::MAX);
    assert_eq!(request.attempt_id(), &[6; 16]);
    assert_eq!(
        ArchiveAttemptRequest::new([0; 16], catalog, coordinator, worker, 0, &batch).unwrap_err(),
        ArchiveAttemptError::InvalidInput
    );
    assert_eq!(
        ArchiveAttemptRequest::new([6; 16], catalog, coordinator, coordinator, 0, &batch)
            .unwrap_err(),
        ArchiveAttemptError::InvalidInput
    );
    assert_eq!(
        ArchiveAttemptRequest::new([6; 16], catalog, coordinator, worker, u64::MAX, &batch)
            .unwrap_err(),
        ArchiveAttemptError::Exhausted
    );
    let unsupported = ArchiveBatch::new(2, TopologyGeneration::new(0), &input).unwrap();
    assert_eq!(
        ArchiveAttemptRequest::new([6; 16], catalog, coordinator, worker, 0, &unsupported)
            .unwrap_err(),
        ArchiveAttemptError::InvalidInput
    );
}
