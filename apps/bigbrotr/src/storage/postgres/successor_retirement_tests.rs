//! Pure retirement discriminators. SQL locks, backend/UTC authority, native
//! replay and ambiguous commits require connected qualification separately.
use super::*;
use crate::storage::rocks::manifest::{AllocationNamespaces, IngestManifest, ManifestIdentity};
use bigbrotr_model::{
    DatasetId, OwnerEpoch, OwnerResourceId, ProcessIncarnation, RecoveryGenerationId,
    TopologyGeneration,
};
use retirement_rows::{Receipt, Terminal};

fn head(epoch: u64, reserved: u64) -> Epoch {
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
        producer: ProducerState::Closing,
        source: SourceState::Closed,
        source_cut: Some(5),
        source_digest: Some([6; 32]),
    }
}
fn current(epoch: u64) -> ingest_registry::ControlOwner {
    ingest_registry::ControlOwner {
        resource_id: OwnerResourceId::try_from([7; 16]).unwrap(),
        epoch: OwnerEpoch::new(epoch).unwrap(),
        role_instance_id: [8; 16],
        process_incarnation: ProcessIncarnation::try_from([9; 16]).unwrap(),
    }
}
fn ready() -> Terminal {
    Terminal {
        source_closed_at: 100,
        source_xmin: 41,
        retired_at: None,
        receipt: None,
    }
}
fn terminal() -> Terminal {
    Terminal {
        retired_at: Some(101),
        receipt: Some(Receipt {
            transaction: 73,
            epoch: 15,
            incarnation: [9; 16],
            xmin: 73,
        }),
        ..ready()
    }
}

#[test]
fn retirement_accepts_only_complete_ready_or_unchanged_terminal_heads() {
    for (epoch, count) in [(3, 0), (4, 2), (255, 64)] {
        let row = head(epoch, count);
        retirement_rows::require_head(&row, count, 64, &ready()).unwrap();
        let retired = Epoch {
            producer: ProducerState::Retired,
            ..row
        };
        retirement_rows::require_head(&retired, count, 64, &terminal()).unwrap();
        retirement_rows::require_terminal_head(&row, &retired).unwrap();
        assert!(retirement_rows::require_head(&row, count, 64, &terminal()).is_err());
        assert!(retirement_rows::require_head(&retired, count, 64, &ready()).is_err());
        let altered_source = Epoch {
            source_digest: Some([10; 32]),
            ..retired
        };
        assert!(retirement_rows::require_terminal_head(&row, &altered_source).is_err());
    }
    let row = head(3, 2);
    for invalid in [
        Epoch {
            activated: false,
            ..row
        },
        Epoch {
            source: SourceState::Open,
            ..row
        },
        Epoch {
            source: SourceState::Closing,
            ..row
        },
        Epoch {
            source_cut: None,
            ..row
        },
        Epoch {
            source_digest: None,
            ..row
        },
        Epoch {
            producer: ProducerState::Open,
            ..row
        },
        head(2, 2),
        head(256, 2),
    ] {
        assert!(retirement_rows::require_head(&invalid, 2, 32, &ready()).is_err());
    }
    // No repair of a reservation suffix or adoption of an issued prefix.
    assert!(retirement_rows::require_head(&row, 1, 32, &ready()).is_err());
    assert!(retirement_rows::require_head(&row, 3, 32, &ready()).is_err());
    for limit in [0, 1, 65] {
        assert!(retirement_rows::require_head(&row, 2, limit, &ready()).is_err());
    }
    let retired = Epoch {
        producer: ProducerState::Retired,
        ..row
    };
    for partial in [
        Terminal {
            receipt: None,
            ..terminal()
        },
        Terminal {
            retired_at: None,
            ..terminal()
        },
        Terminal {
            retired_at: Some(99),
            ..terminal()
        },
    ] {
        assert!(retirement_rows::require_head(&retired, 2, 32, &partial).is_err());
    }
}

#[test]
fn native_event_mask_requires_each_exact_binding_but_not_evidence_jobs() {
    let summary = LineageEpochSummary {
        epoch: 3,
        terminal_kind: RetainedTerminalKind::Activated,
        resolved_requests: 3,
        last_complete_sequence: 4,
        digest: [6; 32],
        // Evidence1, AlreadyDurable Event2, rejected Event3: native retains
        // Event classification even when the stored outcome creates no facts.
        event_request_mask: 0b110,
    };
    retirement_rows::require_request_count(&summary, 3).unwrap();
    retirement_rows::require_event_binding(&summary, 0, 1, false).unwrap();
    for offset in [1, 2] {
        assert!(
            retirement_rows::require_event_binding(&summary, offset, offset as u64 + 1, false)
                .is_err()
        );
        retirement_rows::require_event_binding(&summary, offset, offset as u64 + 1, true).unwrap();
    }
    assert!(retirement_rows::require_event_binding(&summary, 0, 2, true).is_err());
    assert!(retirement_rows::require_event_binding(&summary, 64, 65, true).is_err());
    assert!(retirement_rows::require_request_count(&summary, 2).is_err());
    let last_bit = LineageEpochSummary {
        resolved_requests: 64,
        event_request_mask: 1 << 63,
        ..summary
    };
    retirement_rows::require_request_count(&last_bit, 64).unwrap();
    retirement_rows::require_event_binding(&last_bit, 63, 64, true).unwrap();
    assert!(retirement_rows::require_event_binding(&last_bit, 63, 64, false).is_err());
    assert!(retirement_rows::require_request_count(&last_bit, 65).is_err());
    let zero = LineageEpochSummary {
        resolved_requests: 0,
        event_request_mask: 0,
        ..summary
    };
    retirement_rows::require_request_count(&zero, 0).unwrap();
    assert!(
        retirement_rows::require_request_count(
            &LineageEpochSummary {
                event_request_mask: 1,
                ..zero
            },
            0
        )
        .is_err()
    );
}

#[test]
fn terminal_retry_preserves_source_and_original_transaction_under_fresh_control() {
    let before = ready();
    let after = terminal();
    retirement_rows::require_terminal_change(&before, &after, current(15), Some(73)).unwrap();
    // A replacement Control observes the immutable old receipt; it never
    // rewrites the terminal timestamp/issuer/xmin to itself or requires old
    // issuer liveness. The connected classifier proves its exclusion instead.
    retirement_rows::require_terminal_change(&after, &after, current(16), None).unwrap();
    assert!(
        retirement_rows::require_terminal_change(&before, &after, current(16), Some(73)).is_err()
    );
    assert!(
        retirement_rows::require_terminal_change(&before, &after, current(15), Some(74)).is_err()
    );
    assert!(retirement_rows::require_terminal_change(&before, &after, current(15), None).is_err());
    assert!(
        retirement_rows::require_terminal_change(&after, &after, current(16), Some(74)).is_err()
    );
    for changed in [
        Terminal {
            source_closed_at: 99,
            ..after
        },
        Terminal {
            source_xmin: 42,
            ..after
        },
        Terminal {
            retired_at: Some(102),
            ..after
        },
        Terminal {
            receipt: Some(Receipt {
                xmin: 74,
                ..after.receipt.unwrap()
            }),
            ..after
        },
        Terminal {
            receipt: Some(Receipt {
                epoch: 16,
                ..after.receipt.unwrap()
            }),
            ..after
        },
    ] {
        assert!(
            retirement_rows::require_terminal_change(&after, &changed, current(16), None).is_err()
        );
    }
    let wrong_incarnation = Terminal {
        receipt: Some(Receipt {
            incarnation: [10; 16],
            ..after.receipt.unwrap()
        }),
        ..after
    };
    assert!(
        retirement_rows::require_terminal_change(
            &before,
            &wrong_incarnation,
            current(15),
            Some(73)
        )
        .is_err()
    );
    let before_source = Terminal {
        retired_at: Some(99),
        ..after
    };
    assert!(
        retirement_rows::require_terminal_change(&before, &before_source, current(15), Some(73))
            .is_err()
    );
}
