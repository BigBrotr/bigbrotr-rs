//! Real native mechanics with a test-only target handoff. These controls do not
//! fabricate a PostgreSQL confirmation or qualify connected ACK authority.
use super::super::{producer_ack::AckTarget, producer_checkpoint::CheckpointStamp};
use super::*;
use crate::storage::rocks::keys::IngestKey;

fn target(store: &IngestStore, slot: u8, complete: u64) -> AckTarget {
    AckTarget::new(
        CheckpointStamp::capture(store).unwrap(),
        request(slot, 1).producer_slot_id,
        ProducerEpoch::new(1).unwrap(),
        complete,
    )
}
fn acknowledge(store: &mut IngestStore, budget: &ResourceBudget, complete: u64) {
    let target = target(store, 5, complete);
    store
        .test_acknowledge_target(target, &grant(budget, 4096, 4096), || Ok(()))
        .unwrap();
}
fn inputs(root: &std::path::Path) -> Vec<(std::ffi::OsString, Vec<u8>)> {
    let mut rows = std::fs::read_dir(root.join("db/producer-inputs-v1"))
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            (entry.file_name(), std::fs::read(entry.path()).unwrap())
        })
        .collect::<Vec<_>>();
    rows.sort();
    rows
}
fn limited(
    store: &mut IngestStore,
    budget: &ResourceBudget,
    n: u64,
) -> Result<StoredOutcome, AdmissionError> {
    store.admit_event(
        PreparedEvent::new(
            request(5, n),
            intent("E_private", 2, 10_000_000),
            256 * 1024,
            grant(budget, MIB, 0),
        )
        .unwrap(),
        ObservedTime::new(10_000_000),
        false,
        AdmissionLimits {
            max_outstanding_requests: 2,
            ..AdmissionLimits::default()
        },
        grant(budget, 40 * MIB, 10 * MIB),
        || Ok(()),
    )
}

#[test]
fn acknowledgement_writes_only_watermark_once_and_retains_every_original() {
    let (root, mut store, budget, manifest, native) = setup(false);
    drop(admit(&mut store, &budget, 1).unwrap());
    let before = kv(&store);
    let original_inputs = inputs(root.path());
    let sequence = store.db.latest_sequence_number();
    acknowledge(&mut store, &budget, 1);
    let mut expected = before;
    let key = IngestKey::ProducerState {
        producer_slot_id: request(5, 1).producer_slot_id,
        producer_epoch: ProducerEpoch::new(1).unwrap(),
    }
    .encode()
    .unwrap();
    let row = expected
        .iter_mut()
        .find(|r| r.0 == "receipts" && r.1 == key.as_bytes())
        .unwrap();
    row.2 = [
        vec![1, 0x31],
        1_u64.to_be_bytes().to_vec(),
        1_u64.to_be_bytes().to_vec(),
        vec![0; 8],
    ]
    .concat();
    assert_eq!(kv(&store), expected);
    assert_eq!(store.db.latest_sequence_number(), sequence + 1);
    acknowledge(&mut store, &budget, 1);
    assert_eq!(kv(&store), expected);
    assert_eq!(store.db.latest_sequence_number(), sequence + 1);
    assert_eq!(inputs(root.path()), original_inputs);
    assert_eq!(
        store
            .producer_credit(request(5, 1).producer_slot_id)
            .unwrap()
            .outstanding_requests(),
        0
    );
    drop(store);
    let mut recovery = IngestRecovery::open(
        &root.path().join("db"),
        manifest,
        native,
        grant(&budget, 0, 128 * MIB),
    )
    .unwrap();
    let closed = recovery
        .close_producer(
            request(5, 1).producer_slot_id,
            ProducerEpoch::new(1).unwrap(),
            grant(&budget, 4096, 4096),
            || Ok(()),
        )
        .unwrap();
    assert_eq!(closed.acknowledged_through(), 1);
    let scratch = grant(&budget, 4096, 0);
    let view = recovery.progress_read(&scratch).unwrap();
    assert!(
        view.require_frontier(
            request(5, 1).producer_slot_id,
            ProducerEpoch::new(1).unwrap(),
            1,
            1
        )
        .is_ok()
    );
    assert!(
        view.require_frontier(
            request(5, 1).producer_slot_id,
            ProducerEpoch::new(1).unwrap(),
            0,
            1
        )
        .is_err()
    );
    assert_eq!(inputs(root.path()), original_inputs);
}

#[test]
fn bootstrap_ack_releases_its_actual_slot_without_removing_dictionary_receipt() {
    let (root, mut store, budget, _, _) = setup(false);
    let before = kv(&store);
    let original_inputs = inputs(root.path());
    let sequence = store.db.latest_sequence_number();
    let target = target(&store, 7, 1);
    store
        .test_acknowledge_target(target, &grant(&budget, 4096, 4096), || Ok(()))
        .unwrap();
    let key = IngestKey::ProducerState {
        producer_slot_id: request(7, 1).producer_slot_id,
        producer_epoch: ProducerEpoch::new(1).unwrap(),
    }
    .encode()
    .unwrap();
    for row in before.iter().filter(|row| row.1 != key.as_bytes()) {
        assert!(kv(&store).contains(row));
    }
    assert_eq!(store.db.latest_sequence_number(), sequence + 1);
    assert_eq!(inputs(root.path()), original_inputs);
    let credit = store
        .producer_credit(request(7, 1).producer_slot_id)
        .unwrap();
    assert_eq!(
        (credit.issued_through(), credit.acknowledged_through()),
        (1, 1)
    );
    // Before issue, contiguous batch staging is bounded by actual A, not C.
    assert!(store.require_request_capacity(request(7, 3), 2).is_ok());
    assert!(store.require_request_capacity(request(7, 4), 2).is_err());
}

#[test]
fn two_slot_credit_comes_only_from_actual_ack_and_proof_only_does_not_ack() {
    let (root, mut store, budget, _, _) = setup(false);
    drop(limited(&mut store, &budget, 1).unwrap());
    drop(limited(&mut store, &budget, 2).unwrap());
    let before = kv(&store);
    let sequence = store.db.latest_sequence_number();
    let original_inputs = inputs(root.path());
    assert!(matches!(
        limited(&mut store, &budget, 3),
        Err(AdmissionError::WindowFull)
    ));
    let result = store
        .certify_checkpoint_event(
            PreparedCertification::new(
                intent("E_private", 2, 10_000_000),
                256 * 1024,
                grant(&budget, MIB, 0),
            )
            .unwrap(),
            false,
            CertificationLimits::default(),
            grant(&budget, 32 * MIB, 0),
            || Ok(()),
        )
        .unwrap();
    let basis = store.checkpoint_certification(&result).unwrap().unwrap();
    assert_eq!(
        basis.kind(),
        super::super::producer_checkpoint::CheckpointKind::ExistingProof
    );
    assert!(
        basis
            .acknowledgement_target(
                request(5, 1).producer_slot_id,
                ProducerEpoch::new(1).unwrap(),
                2
            )
            .is_err()
    );
    assert_eq!(kv(&store), before);
    assert_eq!(store.db.latest_sequence_number(), sequence);
    assert_eq!(inputs(root.path()), original_inputs);
    drop(result);
    acknowledge(&mut store, &budget, 2);
    assert_eq!(
        store
            .producer_credit(request(5, 1).producer_slot_id)
            .unwrap()
            .outstanding_requests(),
        0
    );
    drop(limited(&mut store, &budget, 3).unwrap());
    let credit = store
        .producer_credit(request(5, 1).producer_slot_id)
        .unwrap();
    assert_eq!(
        (
            credit.issued_through(),
            credit.acknowledged_through(),
            credit.outstanding_requests()
        ),
        (3, 2, 1)
    );
    assert!(store.require_request_capacity(request(5, 4), 2).is_ok());
    assert!(store.require_request_capacity(request(5, 5), 2).is_err());
    assert!(store.require_request_capacity(request(5, 65), 64).is_err());
    for original in original_inputs {
        assert!(inputs(root.path()).contains(&original));
    }
}

#[test]
fn acknowledgement_refuses_foreign_stale_overissued_and_unreserved_targets() {
    let (_root, mut store, budget, _, _) = setup(false);
    let (_other, mut other, other_budget, _, _) = setup(false);
    drop(admit(&mut store, &budget, 1).unwrap());
    drop(admit(&mut other, &other_budget, 1).unwrap());
    let before = kv(&store);
    let sequence = store.db.latest_sequence_number();
    let foreign = target(&other, 5, 1);
    assert_eq!(
        store.test_acknowledge_target(foreign, &grant(&budget, 4096, 4096), || Ok(())),
        Err(AdmissionError::StateMismatch)
    );
    for (complete, data, native) in [(2, 4096, 4096), (1, 4095, 4096), (1, 4096, 4095)] {
        let target = target(&store, 5, complete);
        assert!(
            store
                .test_acknowledge_target(target, &grant(&budget, data, native), || Ok(()))
                .is_err()
        );
    }
    let cancelled = target(&store, 5, 1);
    assert_eq!(
        store.test_acknowledge_target(cancelled, &grant(&budget, 4096, 4096), || Err(
            AdmissionError::AuthorityUnavailable
        )),
        Err(AdmissionError::AuthorityUnavailable)
    );
    assert_eq!(kv(&store), before);
    assert_eq!(store.db.latest_sequence_number(), sequence);
    let stale = target(&store, 5, 1);
    acknowledge(&mut store, &budget, 1);
    let after = kv(&store);
    assert_eq!(
        store.test_acknowledge_target(stale, &grant(&budget, 4096, 4096), || Ok(())),
        Err(AdmissionError::StateMismatch)
    );
    assert_eq!(kv(&store), after);
}

#[test]
fn acknowledgement_postwrite_authority_loss_preserves_ack_and_sequence_mismatch_poison() {
    let (_root, mut store, budget, _, _) = setup(false);
    drop(admit(&mut store, &budget, 1).unwrap());
    let target = target(&store, 5, 1);
    let sequence = store.db.latest_sequence_number();
    let mut checks = 0;
    assert_eq!(
        store.test_acknowledge_target(target, &grant(&budget, 4096, 4096), || {
            checks += 1;
            if checks == 3 {
                Err(AdmissionError::AuthorityUnavailable)
            } else {
                Ok(())
            }
        }),
        Err(AdmissionError::AuthorityUnavailable)
    );
    assert_eq!(checks, 3);
    assert_eq!(store.db.latest_sequence_number(), sequence + 1);
    assert_eq!(
        store
            .producer_credit(request(5, 1).producer_slot_id)
            .unwrap()
            .acknowledged_through(),
        1
    );
    // Exercise the same post-write invariant gate with an unexpected observed
    // sequence; no successful write may leave a plain owner usable in this case.
    assert_eq!(
        store.test_acknowledgement_sequence(sequence + 2),
        Err(AdmissionError::StateMismatch)
    );
    assert!(store.write_uncertain);
    assert_eq!(
        store.producer_credit(request(5, 1).producer_slot_id),
        Err(AdmissionError::CommitUnknown)
    );
}
