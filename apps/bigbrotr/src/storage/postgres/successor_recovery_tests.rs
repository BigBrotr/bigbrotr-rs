//! Pure discriminators only. Real current-backend/lease/COMMIT and positive
//! native replay qualification belongs to the connected consumer.
use super::*;
use crate::storage::rocks::manifest::{AllocationNamespaces, IngestManifest, ManifestIdentity};
use bigbrotr_model::{
    DatasetId, OwnerEpoch, OwnerResourceId, ProcessIncarnation, ProducerEpoch,
    RecoveryGenerationId, RequestSequence, TopologyGeneration,
};

fn head(epoch: u64, reserved: u64, closing: bool) -> Epoch {
    Epoch {
        manifest: IngestManifest::for_repeatable_successor(
            ManifestIdentity {
                dataset_id: DatasetId::try_from([1; 16]).unwrap(),
                topology_generation: TopologyGeneration::new(epoch),
                owner_resource_id: OwnerResourceId::try_from([2; 16]).unwrap(),
                shard_id: 0,
                owner_epoch: OwnerEpoch::new(epoch).unwrap(),
                process_incarnation: ProcessIncarnation::try_from([3; 16]).unwrap(),
                recovery_generation: RecoveryGenerationId::from_bytes([0; 16]),
                policy_epoch: PolicyEpoch::new(1),
            },
            AllocationNamespaces::from_entries(&[]).unwrap(),
        ),
        incoming: [4; 32],
        slot: ProducerSlotId::try_from([5; 16]).unwrap(),
        reserved,
        activated: true,
        producer: if closing {
            ProducerState::Closing
        } else {
            ProducerState::Open
        },
        source: SourceState::Open,
        source_cut: None,
        source_digest: None,
    }
}

#[test]
fn activated_recovery_accepts_only_the_native_first_barrier_gap() {
    let open = head(3, 2, false);
    projection::require_head(&open, 2, false, 32).unwrap();
    projection::require_head(&open, 2, true, 32).unwrap();
    let closed = head(3, 2, true);
    projection::require_head(&closed, 2, true, 32).unwrap();
    assert!(projection::require_head(&closed, 2, false, 32).is_err());
    let mut empty = open;
    empty.activated = false;
    assert!(projection::require_head(&empty, 0, false, 32).is_err());
    let mut source_closed = closed;
    source_closed.source = SourceState::Closed;
    source_closed.source_cut = Some(5);
    source_closed.source_digest = Some([6; 32]);
    assert!(projection::require_head(&source_closed, 2, true, 32).is_err());
    let mut retired = closed;
    retired.producer = ProducerState::Retired;
    assert!(projection::require_head(&retired, 2, true, 32).is_err());
    assert!(projection::require_head(&head(2, 0, true), 0, true, 32).is_err());
    // Recovery at the selected last epoch needs no capacity for epoch256.
    projection::require_head(&head(255, 0, true), 0, true, 1).unwrap();
    assert!(projection::require_head(&head(256, 0, true), 0, true, 1).is_err());
}

#[test]
fn reserved_suffix_is_bounded_but_never_a_complete_prefix() {
    projection::require_head(&head(3, 64, false), 1, false, 64).unwrap();
    assert!(projection::require_head(&head(3, 65, false), 1, false, 64).is_err());
    assert!(projection::require_head(&head(3, 1, false), 2, false, 32).is_err());
    assert!(projection::require_head(&head(3, 0, false), 0, false, 0).is_err());
    for (issued, reserved, tail) in [(1, 2, false), (1, 1, true), (0, 0, true), (0, 1, true)] {
        assert_eq!(
            require_complete(issued, reserved, tail),
            Err(ProducerResolveError::Native(
                ProducerRecoveryError::Unresolved
            ))
        );
    }
    require_complete(0, 0, false).unwrap();
    require_complete(64, 64, false).unwrap();
}

#[test]
fn recovered_retained_count_is_separate_from_acknowledged_outstanding_window() {
    // These are complete credit premises, not inferred by require_head (which
    // checks lifecycle/retained bounds and has no typed-C/native-A arguments).
    // Migration0040 validates reservation<=C+window; native issue requires A.
    for (issued, window) in [(33, 32), (3, 2)] {
        let (acknowledged, complete, reserved) = (1, 1, issued);
        assert!(acknowledged <= complete && complete <= issued);
        assert_eq!(issued - acknowledged, window);
        assert_eq!(reserved, complete + window);
        projection::require_head(&head(3, reserved, false), issued, false, window as u32).unwrap();
        require_complete(issued, reserved, false).unwrap();
    }
}

#[test]
fn compacted_recovery_requires_matching_private_basis() {
    require_recovery_basis(true, true).unwrap();
    require_recovery_basis(false, false).unwrap();
    assert!(require_recovery_basis(false, true).is_err());
    assert!(require_recovery_basis(true, false).is_err());
}

#[test]
fn tail_bytes_are_observation_until_an_exact_reservation_binds_them() {
    let tail = SuccessorRecoveryInput {
        request: ProducerRequestId {
            producer_slot_id: ProducerSlotId::try_from([5; 16]).unwrap(),
            producer_epoch: ProducerEpoch::new(3).unwrap(),
            request_sequence: RequestSequence::new(2).unwrap(),
        },
        digest: Digest256::from_bytes([6; 32]),
        input_bytes: 73,
        reserved: false,
    };
    projection::require_tail_binding(tail, None, [7; 32]).unwrap();
    // A genuinely unreserved input carries no manifest attribution at all.
    projection::require_tail_binding(tail, None, [8; 32]).unwrap();
    let reservation = producer_requests::ProducerReservation {
        request: tail.request,
        digest: tail.digest,
        input_bytes: tail.input_bytes,
        manifest_sha256: [7; 32],
    };
    assert!(projection::require_tail_binding(tail, Some(&reservation), [7; 32]).is_err());
    let bound = SuccessorRecoveryInput {
        reserved: true,
        ..tail
    };
    assert!(projection::require_tail_binding(bound, None, [7; 32]).is_err());
    projection::require_tail_binding(bound, Some(&reservation), [7; 32]).unwrap();
    for changed in [
        producer_requests::ProducerReservation {
            digest: Digest256::from_bytes([9; 32]),
            ..reservation
        },
        producer_requests::ProducerReservation {
            input_bytes: 74,
            ..reservation
        },
        producer_requests::ProducerReservation {
            manifest_sha256: [8; 32],
            ..reservation
        },
        producer_requests::ProducerReservation {
            request: ProducerRequestId {
                request_sequence: RequestSequence::new(1).unwrap(),
                ..reservation.request
            },
            ..reservation
        },
    ] {
        assert!(projection::require_tail_binding(bound, Some(&changed), [7; 32]).is_err());
    }
}

#[test]
fn close_attempt_is_spent_before_any_fallible_dispatch() {
    let mut state = ClosingState::Inspected;
    state.start().unwrap();
    assert_eq!(state, ClosingState::Spent);
    assert_eq!(state.start(), Err(OwnerError::Unavailable));
    state = ClosingState::Closed;
    assert_eq!(state.start(), Err(OwnerError::Unavailable));
}
