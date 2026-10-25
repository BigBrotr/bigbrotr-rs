//! Fixed-shape discriminators. Actual SQL locking/COMMIT/copy preservation and
//! whole native source replay remain connected qualification obligations.
use super::*;
use crate::storage::rocks::manifest::{AllocationNamespaces, IngestManifest, ManifestIdentity};
use bigbrotr_model::{
    DatasetId, OwnerEpoch, OwnerResourceId, ProcessIncarnation, RecoveryGenerationId,
    TopologyGeneration,
};

fn head(epoch: u64) -> Epoch {
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
        reserved: 3,
        activated: true,
        producer: ProducerState::Closing,
        source: SourceState::Closed,
        source_cut: Some(4),
        source_digest: Some([6; 32]),
    }
}
fn summary(epoch: u64) -> LineageEpochSummary {
    LineageEpochSummary {
        epoch,
        terminal_kind: RetainedTerminalKind::Activated,
        resolved_requests: 3,
        last_complete_sequence: 4,
        digest: [6; 32],
        event_request_mask: 6,
    }
}

#[test]
fn source_closed_requires_both_barriers_without_broadening_positive() {
    let closed = head(3);
    projection::require_source_head(&closed, 3, true, 32).unwrap();
    assert!(projection::require_head(&closed, 3, true, 32).is_err());
    assert!(projection::require_source_head(&closed, 3, false, 32).is_err());
    for changed in [
        Epoch {
            producer: ProducerState::Open,
            ..closed
        },
        Epoch {
            producer: ProducerState::Retired,
            ..closed
        },
        Epoch {
            source: SourceState::Closing,
            ..closed
        },
        Epoch {
            source_cut: None,
            ..closed
        },
        Epoch {
            source_digest: None,
            ..closed
        },
        Epoch {
            activated: false,
            ..closed
        },
        Epoch {
            reserved: 4,
            ..closed
        },
    ] {
        assert!(projection::require_source_head(&changed, 3, true, 32).is_err());
    }
    let open = Epoch {
        source: SourceState::Open,
        source_cut: None,
        source_digest: None,
        producer: ProducerState::Open,
        ..closed
    };
    for native_closing in [false, true] {
        projection::require_source_head(&open, 3, native_closing, 32).unwrap();
        projection::require_head(&open, 3, native_closing, 32).unwrap();
    }
    assert!(projection::require_source_head(&head(2), 3, true, 32).is_err());
    projection::require_source_head(&head(255), 3, true, 32).unwrap();
    assert!(projection::require_source_head(&head(256), 3, true, 32).is_err());
}

#[test]
fn source_summary_binds_closed_cut_digest_and_keeps_event_mask_separate() {
    let row = head(4);
    let expected = summary(4);
    // Event presence is not a source-closure workflow/retirement veto.
    projection::require_summary(&expected, &row, 3, 4).unwrap();
    for changed in [
        LineageEpochSummary {
            epoch: 3,
            ..expected
        },
        LineageEpochSummary {
            terminal_kind: RetainedTerminalKind::UnactivatedEmpty,
            ..expected
        },
        LineageEpochSummary {
            resolved_requests: 2,
            ..expected
        },
        LineageEpochSummary {
            last_complete_sequence: 5,
            ..expected
        },
        LineageEpochSummary {
            digest: [7; 32],
            ..expected
        },
    ] {
        assert!(projection::require_summary(&changed, &row, 3, 4).is_err());
    }
    let zero = Epoch {
        reserved: 0,
        source_cut: Some(0),
        ..head(255)
    };
    let zero_summary = LineageEpochSummary {
        epoch: 255,
        resolved_requests: 0,
        last_complete_sequence: 0,
        event_request_mask: 0,
        ..expected
    };
    projection::require_source_head(&zero, 0, true, 1).unwrap();
    projection::require_summary(&zero_summary, &zero, 0, 0).unwrap();
    // Open source obtains the digest only from positive replay, never SQL.
    let open = Epoch {
        source: SourceState::Open,
        source_cut: None,
        source_digest: None,
        ..row
    };
    projection::require_summary(&expected, &open, 3, 4).unwrap();
    assert!(projection::require_summary(&expected, &open, 3, 5).is_err());
}

#[test]
fn publication_allows_only_the_exact_source_change_and_exact_retry() {
    let closed = head(3);
    let before = Epoch {
        source: SourceState::Open,
        source_cut: None,
        source_digest: None,
        ..closed
    };
    let expected = summary(3);
    require_published(&before, &closed, &expected).unwrap();
    require_published(&closed, &closed, &expected).unwrap();
    for changed in [
        Epoch {
            reserved: 4,
            ..closed
        },
        Epoch {
            incoming: [9; 32],
            ..closed
        },
        Epoch {
            slot: ProducerSlotId::try_from([9; 16]).unwrap(),
            ..closed
        },
        Epoch {
            activated: false,
            ..closed
        },
        Epoch {
            producer: ProducerState::Retired,
            ..closed
        },
        Epoch {
            source_cut: Some(5),
            ..closed
        },
        Epoch {
            source_digest: Some([9; 32]),
            ..closed
        },
        Epoch {
            source: SourceState::Closing,
            ..closed
        },
    ] {
        assert!(require_published(&before, &changed, &expected).is_err());
    }
    let open_producer = Epoch {
        producer: ProducerState::Open,
        ..before
    };
    assert!(require_published(&open_producer, &closed, &expected).is_err());
}

#[test]
fn publication_is_spent_before_failure_and_never_rearms() {
    let state = PublicationState::default();
    state.require_available().unwrap();
    state.start().unwrap();
    // The same transition covers cancellation, failed SQL, unknown COMMIT and
    // known success: only fresh inspection can provide another source owner.
    assert_eq!(state.require_available(), Err(OwnerError::Unavailable));
    assert_eq!(state.start(), Err(OwnerError::Unavailable));
    assert_eq!(state.require_available(), Err(OwnerError::Unavailable));
    fn send_sync<T: Send + Sync>() {}
    send_sync::<PublicationState>();
}

#[test]
fn compacted_source_abandonment_refusal_spends_publication() {
    for compacted in [false, true] {
        let publication = PublicationState::default();
        publication.require_available().unwrap();
        assert_eq!(
            publication.start_abandonment(compacted),
            if compacted {
                Err(OwnerError::InvalidInput)
            } else {
                Ok(())
            }
        );
        // Both the accepted original consumer and the refused compacted
        // consumer spend the owner before any transaction or job effect.
        assert_eq!(
            publication.require_available(),
            Err(OwnerError::Unavailable)
        );
        assert_eq!(publication.start(), Err(OwnerError::Unavailable));
        assert_eq!(
            publication.start_abandonment(false),
            Err(OwnerError::Unavailable)
        );
    }
}

#[test]
fn source_cut_is_independent_of_checkpoint_credit_and_initial_floor() {
    let closed = head(3);
    let expected = summary(3);
    let (checkpoint, acknowledged, initial_floor_requests) = (1, 1, 7);
    assert_eq!(checkpoint, acknowledged);
    assert_eq!(
        (expected.resolved_requests, expected.last_complete_sequence),
        (3, 4)
    );
    projection::require_summary(&expected, &closed, 3, 4).unwrap();
    // The existing final ACK kernel uses typed C1. Source publication instead
    // requires the complete current Fact cut4, never C1, issued3 or floor N7.
    for cut in [
        checkpoint,
        expected.resolved_requests,
        initial_floor_requests,
    ] {
        let wrong = LineageEpochSummary {
            last_complete_sequence: cut,
            ..expected
        };
        assert!(projection::require_summary(&wrong, &closed, 3, 4).is_err());
        assert!(require_published(&closed, &closed, &wrong).is_err());
    }
    let wrong_floor_digest = LineageEpochSummary {
        digest: [7; 32],
        ..expected
    };
    assert!(projection::require_summary(&wrong_floor_digest, &closed, 3, 4).is_err());
    assert!(require_published(&closed, &closed, &wrong_floor_digest).is_err());
    require_published(&closed, &closed, &expected).unwrap();
}
