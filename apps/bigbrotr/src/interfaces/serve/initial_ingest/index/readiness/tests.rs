//! Independent metadata/scheduling refusals. These tests create no physical
//! proof, native source, PG owner or recovered writer and do not qualify CH.
use super::*;
use bigbrotr_model::{DatasetId, Digest256, PolicyEpoch, RecoveryGenerationId, TopologyGeneration};

fn manifest() -> IngestManifest {
    IngestManifest::new(
        ManifestIdentity {
            dataset_id: [1; 16].try_into().unwrap(),
            topology_generation: TopologyGeneration::new(7),
            owner_resource_id: [2; 16].try_into().unwrap(),
            shard_id: 0,
            owner_epoch: OwnerEpoch::new(1).unwrap(),
            process_incarnation: [3; 16].try_into().unwrap(),
            recovery_generation: RecoveryGenerationId::from_bytes([4; 16]),
            policy_epoch: PolicyEpoch::new(9),
        },
        AllocationNamespaces::from_entries(&[]).unwrap(),
    )
}
fn cut(
    dataset: DatasetId,
    topology: TopologyGeneration,
    shard: u32,
    epoch: u64,
    through: u64,
) -> SourceCut {
    SourceCut::new(
        dataset,
        topology,
        vec![StreamBound {
            stream: StreamId {
                shard_id: shard,
                owner_epoch: OwnerEpoch::new(epoch).unwrap(),
            },
            last_complete_sequence: CompleteThrough::new(through),
        }],
    )
    .unwrap()
}

#[test]
fn readiness_requires_exact_current_initial_source_cut_not_idle_or_a_partial_vector() {
    let manifest = manifest();
    let id = manifest.identity();
    let exact = cut(id.dataset_id, id.topology_generation, 0, 1, 12);
    assert!(source_matches(&manifest, CompleteThrough::new(12), &exact));
    for wrong in [
        cut(
            [5; 16].try_into().unwrap(),
            id.topology_generation,
            0,
            1,
            12,
        ),
        cut(id.dataset_id, TopologyGeneration::new(8), 0, 1, 12),
        cut(id.dataset_id, id.topology_generation, 1, 1, 12),
        cut(id.dataset_id, id.topology_generation, 0, 2, 12),
        cut(id.dataset_id, id.topology_generation, 0, 1, 11),
        cut(id.dataset_id, id.topology_generation, 0, 1, 13),
    ] {
        assert!(!source_matches(&manifest, CompleteThrough::new(12), &wrong));
    }
    let two = SourceCut::new(
        id.dataset_id,
        id.topology_generation,
        vec![
            exact.bounds()[0],
            StreamBound {
                stream: StreamId {
                    shard_id: 1,
                    owner_epoch: OwnerEpoch::new(1).unwrap(),
                },
                last_complete_sequence: CompleteThrough::new(0),
            },
        ],
    )
    .unwrap();
    assert!(!source_matches(&manifest, CompleteThrough::new(12), &two));
    let successor = IngestManifest::for_successor(*id, *manifest.allocation_namespaces());
    assert!(!source_matches(
        &successor,
        CompleteThrough::new(12),
        &exact
    ));
    let maximum = cut(id.dataset_id, id.topology_generation, 0, 1, u64::MAX);
    assert!(source_matches(
        &manifest,
        CompleteThrough::new(u64::MAX),
        &maximum
    ));
    assert!(!source_matches(
        &manifest,
        CompleteThrough::new(0),
        &maximum
    ));
}

#[test]
fn readiness_capture_refuses_owner_changes_pending_and_inconsistent_published_heads() {
    let owner = IndexOwnerIdentity {
        resource: [6; 16].try_into().unwrap(),
        epoch: OwnerEpoch::new(1).unwrap(),
        incarnation: [7; 16].try_into().unwrap(),
    };
    let state = IndexRecoveryState {
        published_revision: 4,
        published_plan: Some(Digest256::from_bytes([8; 32])),
        pending_plan: None,
    };
    assert!(capture_matches(owner, state, owner, 4));
    for wrong in [
        IndexOwnerIdentity {
            resource: [9; 16].try_into().unwrap(),
            ..owner
        },
        IndexOwnerIdentity {
            epoch: OwnerEpoch::new(2).unwrap(),
            ..owner
        },
        IndexOwnerIdentity {
            incarnation: [10; 16].try_into().unwrap(),
            ..owner
        },
    ] {
        assert!(!capture_matches(wrong, state, owner, 4));
    }
    assert!(!capture_matches(owner, state, owner, 3));
    assert!(!capture_matches(
        owner,
        IndexRecoveryState {
            published_plan: None,
            ..state
        },
        owner,
        4
    ));
    assert!(!capture_matches(
        owner,
        IndexRecoveryState {
            pending_plan: Some(Digest256::from_bytes([11; 32])),
            ..state
        },
        owner,
        4
    ));
    let zero = IndexRecoveryState {
        published_revision: 0,
        published_plan: None,
        pending_plan: None,
    };
    assert!(capture_matches(owner, zero, owner, 0));
    assert!(!capture_matches(
        owner,
        IndexRecoveryState {
            published_revision: 0,
            ..state
        },
        owner,
        0
    ));
}

#[test]
fn readiness_uses_oldest_read_and_a_strict_whole_sweep_window() {
    assert!(!probe_due(Duration::from_nanos(14_999_999_999)));
    assert!(probe_due(Duration::from_secs(15)));
    assert!(within_window(Duration::ZERO));
    assert!(within_window(Duration::from_nanos(29_999_999_999)));
    assert!(!within_window(Duration::from_secs(30)));
    assert!(!within_window(Duration::from_secs(31)));
    assert!(!within_window(Duration::MAX));
    // An otherwise just-finished last query cannot replace the oldest read.
    let oldest_read_age = Duration::from_secs(31);
    let last_query_age = Duration::ZERO;
    assert!(within_window(last_query_age));
    assert!(!within_window(oldest_read_age));
}

#[test]
fn readiness_abandon_does_not_reset_attempt_cooldown_or_erase_a_failure() {
    let now = SuspendAwareClock.read().unwrap();
    let mut state = Readiness::default();
    assert!(state.due(now));
    state.last_attempt = Some(now);
    state.fail(Failure::Dependency);
    state.abandon();
    assert_eq!(state.last_attempt, Some(now));
    assert!(!state.due(now));
    assert_eq!(
        state.observation(false, false, false).failure,
        Some(Failure::Dependency)
    );
    assert!(state.observation(false, false, false).verified.is_none());
}

#[test]
fn readiness_empty_failed_cancelled_and_parked_caches_never_invent_a_verified_cut() {
    let mut state = Readiness::default();
    let first = state.observation(false, false, false);
    assert!(first.building);
    assert!(!first.active);
    assert!(!first.parked);
    assert!(!first.unresolved);
    assert_eq!(first.freshness, Duration::from_secs(30));
    assert!(first.verified.is_none());
    for failure in [
        Failure::Clock,
        Failure::Source,
        Failure::Changed,
        Failure::Expired,
        Failure::Dependency,
        Failure::Capacity,
    ] {
        state.fail(failure);
        let observed = state.observation(false, false, false);
        assert_eq!(observed.failure, Some(failure));
        assert!(!observed.building);
        assert!(observed.verified.is_none());
    }
    state.cancel();
    // Known corruption may park without a surviving remote query; those are
    // distinct operator states, not an inferred unknown outcome.
    assert!(!state.observation(false, true, false).unresolved);
    let cancelled = state.observation(true, true, true);
    assert_eq!(cancelled.failure, Some(Failure::Cancelled));
    assert!(cancelled.active);
    assert!(cancelled.parked);
    assert!(cancelled.unresolved);
    assert!(!cancelled.building);
    assert!(cancelled.verified.is_none());
}
