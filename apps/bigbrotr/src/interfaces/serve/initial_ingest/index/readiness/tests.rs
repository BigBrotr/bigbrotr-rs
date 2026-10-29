//! Independent metadata/scheduling refusals. These tests create no physical
//! proof, PG owner or recovered writer and do not qualify CH. The capacity
//! control uses an actual empty native source only to populate inert cache data.
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
    assert!(source_matches(
        &manifest,
        CompleteThrough::new(12),
        None,
        &exact
    ));
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
        assert!(!source_matches(
            &manifest,
            CompleteThrough::new(12),
            None,
            &wrong
        ));
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
    assert!(!source_matches(
        &manifest,
        CompleteThrough::new(12),
        None,
        &two
    ));
    let successor = IngestManifest::for_successor(*id, *manifest.allocation_namespaces());
    assert!(!source_matches(
        &successor,
        CompleteThrough::new(12),
        None,
        &exact
    ));
    let maximum = cut(id.dataset_id, id.topology_generation, 0, 1, u64::MAX);
    assert!(source_matches(
        &manifest,
        CompleteThrough::new(u64::MAX),
        None,
        &maximum
    ));
    assert!(!source_matches(
        &manifest,
        CompleteThrough::new(0),
        None,
        &maximum
    ));
}

#[test]
fn live_readiness_requires_both_bounds_and_source_advance_invalidates_the_captured_vector() {
    // Structural observations only; no physical proof or live authority is made.
    let original = manifest();
    let mut id = *original.identity();
    id.owner_epoch = OwnerEpoch::new(2).unwrap();
    id.topology_generation = TopologyGeneration::new(1);
    let manifest = IngestManifest::for_successor(id, *original.allocation_namespaces());
    let old = StreamBound {
        stream: StreamId {
            shard_id: 0,
            owner_epoch: OwnerEpoch::new(1).unwrap(),
        },
        last_complete_sequence: CompleteThrough::new(12),
    };
    let new = StreamBound {
        stream: StreamId {
            shard_id: 0,
            owner_epoch: OwnerEpoch::new(2).unwrap(),
        },
        last_complete_sequence: CompleteThrough::new(10),
    };
    let full = SourceCut::new(id.dataset_id, id.topology_generation, vec![old, new]).unwrap();
    assert!(source_matches(
        &manifest,
        CompleteThrough::new(10),
        Some(old),
        &full
    ));
    assert!(!source_matches(
        &manifest,
        CompleteThrough::new(11),
        Some(old),
        &full
    ));
    assert!(!source_matches(
        &manifest,
        CompleteThrough::new(10),
        None,
        &full
    ));
    for bounds in [
        vec![new],
        vec![old],
        vec![
            StreamBound {
                last_complete_sequence: CompleteThrough::new(11),
                ..old
            },
            new,
        ],
        vec![
            old,
            StreamBound {
                last_complete_sequence: CompleteThrough::new(9),
                ..new
            },
        ],
    ] {
        let wrong = SourceCut::new(id.dataset_id, id.topology_generation, bounds).unwrap();
        assert!(!source_matches(
            &manifest,
            CompleteThrough::new(10),
            Some(old),
            &wrong
        ));
    }
    let later = SourceCut::new(id.dataset_id, TopologyGeneration::new(2), vec![old, new]).unwrap();
    assert!(!source_matches(
        &manifest,
        CompleteThrough::new(10),
        Some(old),
        &later
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
        published_handoff: None,
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
            published_handoff: None,
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
        published_handoff: None,
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
fn deferred_work_retries_released_credit_before_idle_yield_and_keeps_observation_clocks() {
    use bigbrotr::runtime::{ProcessEnvelope, WaiterLimits};
    use bigbrotr::storage::rocks::{RocksNativeLimits, RocksNativeSettings};
    let root = tempfile::tempdir().unwrap();
    let native = ResourceBudget::new(
        ProcessEnvelope::new(
            128 * MIB,
            0,
            ResourceLimits::new(0, 1, 128 * MIB, 0),
            WaiterLimits::new(0, 0),
        )
        .unwrap(),
    );
    let limits = RocksNativeLimits::new(RocksNativeSettings {
        block_cache_bytes: 32 * MIB,
        write_buffer_manager_bytes: 16 * MIB,
        per_cf_write_buffer_bytes: 2 * MIB,
        max_total_wal_bytes: 16 * MIB,
        max_open_files: 32,
        max_background_jobs: 2,
        native_allowance_bytes: 128 * MIB,
    })
    .unwrap();
    let store = IngestStore::initialize(
        &root.path().join("db"),
        manifest(),
        limits,
        native
            .try_acquire(ResourceRequest::data(0, 1, 128 * MIB).unwrap())
            .unwrap(),
    )
    .unwrap();
    let source = store
        .observe_source_head(store.manifest(), &AtomicBool::new(false))
        .unwrap();
    let source = Observed::initial(source);
    let now = SuspendAwareClock.read().unwrap();
    let binding = IndexBinding::new(
        source.identity().dataset_id,
        [6; 16].try_into().unwrap(),
        [7; 16].try_into().unwrap(),
        [8; 16].try_into().unwrap(),
        Digest256::from_bytes([9; 32]),
        Digest256::from_bytes([10; 32]),
        IndexProvenance::Off,
    )
    .unwrap();
    // Inert cache fixture, not an engine verification or publication capability.
    let mut state = Readiness {
        last_attempt: Some(now),
        last_tick: Some(now),
        verified: Some(VerifiedObservation {
            source,
            revision: 0,
            generation: binding.generation(),
            storage_incarnation: binding.storage_incarnation(),
            started: now,
        }),
        capture: Some(Capture {
            source,
            binding,
            owner: IndexOwnerIdentity {
                resource: binding.resource(),
                epoch: OwnerEpoch::new(1).unwrap(),
                incarnation: [11; 16].try_into().unwrap(),
            },
            state: IndexRecoveryState {
                published_revision: 0,
                published_plan: None,
                published_handoff: None,
                pending_plan: None,
            },
            profile: Profile {
                delay: INTERVAL,
                source_records: 64,
                source_bytes: MIB as usize,
                rows: 20_000,
                wire_bytes: 16 * MIB,
                query_memory: 256 * MIB,
                provenance: IndexProvenance::Off,
                configured: [64, MIB, 1],
            },
            started: now,
        }),
        ..Readiness::default()
    };
    let parent = ResourceBudget::new(
        ProcessEnvelope::new(
            100,
            0,
            ResourceLimits::new(100, 4, 0, 0),
            WaiterLimits::new(0, 0),
        )
        .unwrap(),
    );
    let metadata = parent
        .try_acquire(ResourceRequest::data(10, 1, 0).unwrap())
        .unwrap();
    let physical = parent
        .try_acquire(ResourceRequest::data(60, 1, 0).unwrap())
        .unwrap();
    let request = ResourceLimits::new(60, 1, 0, 0);
    assert_eq!(
        parent.try_reserve(request).unwrap_err(),
        AcquireError::Unavailable
    );
    state.yield_for_capacity(true, true, false); // Active owner cannot lose its capture.
    drop(physical); // Model returned result credit; this creates no Index worker.
    let round = parent.try_reserve(request).unwrap(); // Retry fits with metadata retained.
    state.yield_for_capacity(false, false, false); // No failed retry, no idle yield.
    state.yield_for_capacity(true, false, true); // Unknown/parked owner remains untouched.
    assert_eq!(state.capture.as_ref().unwrap().started, now);
    assert_eq!(state.capture.as_ref().unwrap().source, source);
    assert_eq!(state.last_attempt, Some(now));
    assert_eq!(state.verified.unwrap().started, now);
    assert!(!state.due(now));
    drop(round);
    // Only a genuinely failed application retry at the idle boundary may yield.
    let held = parent
        .try_acquire(ResourceRequest::data(80, 1, 0).unwrap())
        .unwrap();
    assert_eq!(
        parent.try_reserve(request).unwrap_err(),
        AcquireError::Unavailable
    );
    state.yield_for_capacity(true, false, false);
    assert!(state.capture.is_none());
    assert_eq!(state.last_attempt, Some(now));
    assert_eq!(state.last_tick, Some(now));
    assert_eq!(state.verified.unwrap().source, source);
    assert_eq!(state.verified.unwrap().started, now);
    // A known connection refusal on a retained publication retry invalidates
    // availability, including while the next retry is active. It cannot turn
    // the historical cut into a new observation or reset the scan cooldown.
    state.fail(Failure::Dependency);
    for active in [false, true] {
        let observed = state.observation(active, false, false);
        assert_eq!(observed.failure, Some(Failure::Dependency));
        assert_eq!(observed.active, active);
        assert!(!observed.parked && !observed.unresolved);
        assert_eq!(observed.verified.unwrap().source, source);
        assert_eq!(observed.verified.unwrap().started, now);
        assert_eq!(state.last_attempt, Some(now));
        assert_eq!(state.last_tick, Some(now));
        assert!(!state.due(now));
    }
    drop((metadata, held));
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
