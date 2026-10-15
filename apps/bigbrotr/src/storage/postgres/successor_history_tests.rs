//! Pure state discriminators, not PostgreSQL/native qualification. Connected
//! fixtures must additionally exercise actual locks, backend and copy bytes.
use super::*;
use crate::storage::rocks::manifest::{AllocationNamespaces, IngestManifest, ManifestIdentity};
use bigbrotr_model::{
    DatasetId, OwnerEpoch, OwnerResourceId, ProcessIncarnation, RecoveryGenerationId,
    TopologyGeneration,
};

fn epoch(epoch: u64, activated: bool, terminal: bool) -> Epoch {
    let identity = ManifestIdentity {
        dataset_id: DatasetId::try_from([1; 16]).unwrap(),
        topology_generation: TopologyGeneration::new(epoch),
        owner_resource_id: OwnerResourceId::try_from([2; 16]).unwrap(),
        shard_id: 0,
        owner_epoch: OwnerEpoch::new(epoch).unwrap(),
        process_incarnation: ProcessIncarnation::try_from([3; 16]).unwrap(),
        recovery_generation: RecoveryGenerationId::from_bytes([0; 16]),
        policy_epoch: PolicyEpoch::new(1),
    };
    let namespaces = AllocationNamespaces::from_entries(&[]).unwrap();
    let manifest = match epoch {
        1 => IngestManifest::new(identity, namespaces),
        2 => IngestManifest::for_successor(identity, namespaces),
        _ => IngestManifest::for_repeatable_successor(identity, namespaces),
    };
    Epoch {
        manifest,
        incoming: [4; 32],
        slot: ProducerSlotId::try_from([5; 16]).unwrap(),
        reserved: 0,
        activated,
        producer: if terminal {
            ProducerState::Retired
        } else {
            ProducerState::Open
        },
        source: if terminal {
            SourceState::Closed
        } else {
            SourceState::Open
        },
        source_cut: terminal.then_some(0),
        source_digest: terminal.then_some([6; 32]),
    }
}

#[test]
fn terminal_genesis_kind_comes_from_activation_even_at_zero() {
    for (activated, kind) in [
        (false, PredecessorTerminalKind::UnactivatedEmpty),
        (true, PredecessorTerminalKind::Activated),
    ] {
        let row = epoch(2, activated, true);
        assert_eq!(
            select_history(2, 0, 0, true, &row, activated).unwrap(),
            Selection {
                state: SuccessorHistoryState::Terminal,
                through: 2,
                head_kind: Some(kind),
            }
        );
        assert!(select_history(2, 0, 0, false, &row, activated).is_err());
        // Legacy prepared2 must use its existing dedicated protocol, not be
        // relabelled as a new-family empty tail by the positive reader.
        assert!(select_history(2, 0, 0, true, &epoch(2, false, false), false).is_err());
    }
}

#[test]
fn pending_and_prepared_closing_do_not_certify_the_target() {
    let prepared = epoch(3, false, false);
    let gap = select_history(2, 4, 12, true, &prepared, true).unwrap();
    assert_eq!(
        gap,
        Selection {
            state: SuccessorHistoryState::PendingNative,
            through: 2,
            head_kind: Some(PredecessorTerminalKind::Activated)
        }
    );
    assert!(select_history(2, 4, 12, false, &prepared, true).is_err());
    for (closing, state) in [
        (false, SuccessorHistoryState::PreparedOpen),
        (true, SuccessorHistoryState::PreparedClosing),
    ] {
        assert_eq!(
            select_history(3, 0, 0, closing, &prepared, false).unwrap(),
            Selection {
                state,
                through: 2,
                head_kind: None
            }
        );
        assert!(select_history(3, 1, 0, closing, &prepared, false).is_err());
        assert!(select_history(3, 0, 1, closing, &prepared, false).is_err());
    }
    let mut partial = prepared;
    partial.producer = ProducerState::Closing;
    assert!(select_history(3, 0, 0, true, &partial, false).is_err());
    partial = prepared;
    partial.source_cut = Some(0);
    assert!(select_history(3, 0, 0, true, &partial, false).is_err());
}

#[test]
fn activated_nonterminal_and_noncontiguous_heads_refuse() {
    for native_closing in [false, true] {
        assert!(select_history(3, 0, 0, native_closing, &epoch(3, true, false), true).is_err());
        let mut row = epoch(3, true, false);
        row.producer = ProducerState::Closing;
        row.source = SourceState::Closed;
        row.source_cut = Some(0);
        row.source_digest = Some([6; 32]);
        assert!(select_history(3, 0, 0, native_closing, &row, true).is_err());
    }
    assert_eq!(pending_target(254, 255).unwrap(), Some(255));
    assert_eq!(pending_target(255, 255).unwrap(), None);
    for (native, registered) in [(1, 2), (2, 1), (2, 4), (3, 2), (255, 256), (256, 256)] {
        assert!(pending_target(native, registered).is_err());
    }
    // The cap prevents the next preparation, not terminal recovery at255.
    assert_eq!(
        select_history(255, 0, 0, true, &epoch(255, true, true), true)
            .unwrap()
            .through,
        255
    );
}

#[test]
fn positive_summary_binds_kind_epoch_count_cut_and_digest() {
    for (number, activated, kind) in [
        (1, true, RetainedTerminalKind::Initial),
        (2, false, RetainedTerminalKind::UnactivatedEmpty),
        (3, true, RetainedTerminalKind::Activated),
    ] {
        let mut row = epoch(number, activated, true);
        if activated {
            row.reserved = 2;
            row.source_cut = Some(5);
        }
        let summary = LineageEpochSummary {
            epoch: number,
            terminal_kind: kind,
            resolved_requests: row.reserved,
            last_complete_sequence: row.source_cut.unwrap(),
            digest: [6; 32],
            event_request_mask: 0,
        };
        row.require_summary(&summary).unwrap();
        for changed in [
            LineageEpochSummary {
                epoch: number + 1,
                ..summary
            },
            LineageEpochSummary {
                resolved_requests: summary.resolved_requests + 1,
                ..summary
            },
            LineageEpochSummary {
                last_complete_sequence: summary.last_complete_sequence + 1,
                ..summary
            },
            LineageEpochSummary {
                digest: [7; 32],
                ..summary
            },
            LineageEpochSummary {
                terminal_kind: if kind == RetainedTerminalKind::Activated {
                    RetainedTerminalKind::UnactivatedEmpty
                } else {
                    RetainedTerminalKind::Activated
                },
                ..summary
            },
        ] {
            assert!(row.require_summary(&changed).is_err());
        }
        row.producer = ProducerState::Closing;
        assert!(row.require_summary(&summary).is_err());
    }
}
