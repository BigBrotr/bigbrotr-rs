//! Pure discriminators for the new terminal action. Actual job selection,
//! immutable gaps, cancellation atomicity and authority waits need SQL engines.
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
fn abandonment_requires_previously_closed_source_without_retirement_veto() {
    for epoch in [3, 4, 255] {
        let row = head(epoch);
        let positive = summary(epoch);
        // Unbound Event classification is intentionally left to retirement.
        require_closed_source(&row, &positive).unwrap();
        for changed in [
            Epoch {
                source: SourceState::Open,
                source_cut: None,
                source_digest: None,
                ..row
            },
            Epoch {
                source: SourceState::Closing,
                ..row
            },
            Epoch {
                producer: ProducerState::Open,
                ..row
            },
            Epoch {
                producer: ProducerState::Retired,
                ..row
            },
            Epoch {
                activated: false,
                ..row
            },
            Epoch { reserved: 4, ..row },
            Epoch {
                source_cut: Some(5),
                ..row
            },
            Epoch {
                source_digest: Some([7; 32]),
                ..row
            },
        ] {
            assert_eq!(
                require_closed_source(&changed, &positive),
                Err(OwnerError::InvalidMetadata)
            );
        }
        for changed in [
            LineageEpochSummary {
                epoch: epoch - 1,
                ..positive
            },
            LineageEpochSummary {
                terminal_kind: RetainedTerminalKind::UnactivatedEmpty,
                ..positive
            },
            LineageEpochSummary {
                resolved_requests: 2,
                ..positive
            },
            LineageEpochSummary {
                last_complete_sequence: 5,
                ..positive
            },
            LineageEpochSummary {
                digest: [7; 32],
                ..positive
            },
        ] {
            assert_eq!(
                require_closed_source(&row, &changed),
                Err(OwnerError::InvalidMetadata)
            );
        }
    }
    for epoch in [2, 256] {
        assert_eq!(
            require_closed_source(&head(epoch), &summary(epoch)),
            Err(OwnerError::InvalidMetadata)
        );
    }
    let zero = Epoch {
        reserved: 0,
        source_cut: Some(0),
        ..head(3)
    };
    let zero_summary = LineageEpochSummary {
        resolved_requests: 0,
        last_complete_sequence: 0,
        event_request_mask: 0,
        ..summary(3)
    };
    require_closed_source(&zero, &zero_summary).unwrap();
}

#[test]
fn abandonment_bound_counts_retained_and_zero_request_claims_together() {
    for (prior, selected, total) in [
        (0, 0, 0),
        (0, 64, 64),
        (64, 0, 64),
        (31, 33, 64),
        (63, 1, 64),
    ] {
        assert_eq!(checked_total(prior, selected), Ok(total));
    }
    for (prior, selected) in [(64, 1), (1, 64), (0, 65), (65, 0), (usize::MAX, 1)] {
        assert_eq!(
            checked_total(prior, selected),
            Err(native_error(BootstrapError::InsufficientReservation))
        );
    }
    // The count is independent of producer requests: an activated zero source
    // still selects every real claim, while a retry counts its retained gaps.
    let zero = Epoch {
        reserved: 0,
        source_cut: Some(0),
        ..head(3)
    };
    let positive = LineageEpochSummary {
        resolved_requests: 0,
        last_complete_sequence: 0,
        event_request_mask: 0,
        ..summary(3)
    };
    require_closed_source(&zero, &positive).unwrap();
    assert_eq!(checked_total(0, 2), Ok(2));
    assert_eq!(checked_total(2, 0), Ok(2));
}
