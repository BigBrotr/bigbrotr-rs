//! Cache/state controls only: synthetic query metadata is never a physical proof.
use super::*;
use bigbrotr::runtime::{ProcessEnvelope, ResourceLimits, WaiterLimits};
use bigbrotr_model::{DictionaryKind, EventId, Ordinal, QualifiedRef, TopologyGeneration};
use bigbrotr_protocol::archive_cut::CutBound;

fn identity() -> SnapshotIdentity {
    SnapshotIdentity {
        dataset_id: [1; 16].try_into().unwrap(),
        process_incarnation: [2; 16].try_into().unwrap(),
        config_revision: "7".to_owned().try_into().unwrap(),
        policy_revision: "9".to_owned().try_into().unwrap(),
    }
}
fn budget() -> ResourceBudget {
    ResourceBudget::new(
        ProcessEnvelope::new(
            4 << 20,
            1 << 20,
            ResourceLimits::new(2 << 20, 8, 0, 1 << 20),
            WaiterLimits::new(0, 0),
        )
        .unwrap(),
    )
}
fn query(count: usize, through: u64) -> EventQuery {
    EventQuery {
        dataset: identity().dataset_id,
        event_id: EventId::from_bytes([3; 32]),
        scope: QualifiedRef {
            dataset_id: identity().dataset_id,
            dictionary_kind: DictionaryKind::VisibilityScope,
            allocation_domain: [4; 16].try_into().unwrap(),
            ordinal: Ordinal::new(1).unwrap(),
        },
        topology_generation: TopologyGeneration::new(1),
        snapshot_id: 5,
        bounds: (0..count)
            .map(|n| CutBound {
                shard_id: n as u32,
                owner_epoch: 1,
                complete_through: through,
            })
            .collect(),
    }
}
fn value(cache: &Observations) -> serde_json::Value {
    serde_json::to_value(
        cache
            .snapshot(identity(), 77, SuspendAwareClock.read().unwrap())
            .unwrap(),
    )
    .unwrap()
}
fn success(cache: &mut Observations, kind: ReadCapability, count: usize, through: u64) {
    cache.begin(kind);
    cache.observed(kind, &query(count, through), 3);
    cache.finish(kind, true);
}
#[test]
fn ordinary_status_does_not_infer_readiness_from_enabled_or_absent_owners() {
    let budget = budget();
    let cache = Observations::new(&budget, identity().dataset_id, true, false).unwrap();
    let actual = value(&cache);
    assert_eq!(actual["identity"]["process_incarnation"], "02".repeat(16));
    assert_eq!(actual["identity"]["config_revision"], "7");
    assert_eq!(actual["identity"]["policy_revision"], "9");
    assert!(actual["source"]["cut"].is_null());
    assert_eq!(actual["source"]["freshness"], "unknown");
    assert_eq!(actual["roles"].as_array().unwrap().len(), 1);
    assert_eq!(actual["roles"][0]["role"], "control");
    assert_eq!(actual["capabilities"][0]["state"], "building");
    assert_eq!(actual["capabilities"].as_array().unwrap().len(), 1);
    let empty = Observations::new(&budget, identity().dataset_id, false, false).unwrap();
    assert!(value(&empty)["capabilities"].as_array().unwrap().is_empty());
    assert!(actual.get("pending_bytes").is_none());
    let mut foreign = identity();
    foreign.dataset_id = [9; 16].try_into().unwrap();
    assert!(matches!(
        cache.snapshot(foreign, 77, SuspendAwareClock.read().unwrap()),
        Err(SessionError::PolicyChanged)
    ));
}
#[test]
fn ordinary_last_success_requires_terminal_cleanup_and_never_claims_full_readiness() {
    let budget = budget();
    let mut cache = Observations::new(&budget, identity().dataset_id, true, true).unwrap();
    success(&mut cache, ReadCapability::History, 1, 11);
    let actual = value(&cache);
    assert_eq!(actual["capabilities"][0]["state"], "stale");
    assert_eq!(actual["capabilities"][0]["freshness"], "fresh");
    assert_eq!(actual["capabilities"][0]["time"]["observed_at_us"], "77");
    assert_eq!(
        actual["capabilities"][0]["published"]["cut"]["bounds"][0]["last_complete_sequence"],
        "11"
    );
    cache.begin(ReadCapability::History);
    assert_eq!(value(&cache)["capabilities"][0]["state"], "building");
    cache.observed(ReadCapability::History, &query(1, 22), 4);
    // The native observation alone is insufficient before actual cleanup/join.
    cache.finish(ReadCapability::History, false);
    let failed = value(&cache);
    assert_eq!(failed["capabilities"][0]["state"], "unavailable");
    assert_eq!(
        failed["capabilities"][0]["published"]["cut"]["bounds"][0]["last_complete_sequence"],
        "11"
    );
    cache.begin(ReadCapability::History);
    assert_eq!(value(&cache)["capabilities"][0]["state"], "unavailable");
    cache.observed(ReadCapability::History, &query(1, 33), 5);
    cache.finish(ReadCapability::History, true);
    let retried = value(&cache);
    assert_eq!(retried["capabilities"][0]["state"], "stale");
    assert_eq!(
        retried["capabilities"][0]["published"]["cut"]["bounds"][0]["last_complete_sequence"],
        "33"
    );
    assert_eq!(retried["capabilities"][1]["state"], "building");
    // A reservation auth/CAS denial begins no physical attempt. Finishing the
    // outer execute wrapper must leave the selected export unobserved.
    cache.finish(ReadCapability::Export, true);
    assert_eq!(value(&cache)["capabilities"][1]["state"], "building");
    success(&mut cache, ReadCapability::Export, 1, 44);
    let original_start = cache.export.last.as_ref().unwrap().started;
    // Later completion auth/CAS denial discards its new candidate, preserving
    // both the earlier successful cut and its original monotonic evidence age.
    cache.begin(ReadCapability::Export);
    cache.observed(ReadCapability::Export, &query(1, 55), 6);
    cache.discard_attempt(ReadCapability::Export);
    cache.finish(ReadCapability::Export, true);
    assert_eq!(cache.export.last.as_ref().unwrap().started, original_start);
    let discarded = value(&cache);
    assert_eq!(discarded["capabilities"][1]["state"], "stale");
    assert_eq!(
        discarded["capabilities"][1]["published"]["cut"]["bounds"][0]["last_complete_sequence"],
        "44"
    );
    // Discarding another authority refusal cannot erase a previous real failure.
    cache.begin(ReadCapability::Export);
    cache.finish(ReadCapability::Export, false);
    cache.begin(ReadCapability::Export);
    cache.discard_attempt(ReadCapability::Export);
    cache.finish(ReadCapability::Export, true);
    assert_eq!(value(&cache)["capabilities"][1]["state"], "unavailable");
}
#[test]
fn ordinary_observation_freshness_uses_thirty_seconds_without_promoting_scope() {
    let budget = budget();
    let mut cache = Observations::new(&budget, identity().dataset_id, true, true).unwrap();
    success(&mut cache, ReadCapability::Export, 1, 11);
    for (age, expected) in [
        (Duration::ZERO, Freshness::Fresh),
        (Duration::from_secs(30), Freshness::Stale),
        (
            Duration::from_secs(30) + Duration::from_micros(1),
            Freshness::Stale,
        ),
    ] {
        let observation = cache
            .export
            .observation(CapabilityKind::SignedEventExport, Some(age), 77)
            .unwrap();
        assert_eq!(observation.freshness, expected);
        assert_eq!(observation.state, CapabilityState::Stale);
        assert_eq!(observation.reason, StatusReason::VerificationStale);
    }
}
#[test]
fn ordinary_cache_checks_aggregate_before_response_clone_and_retains_its_grant() {
    let budget = budget();
    let mut cache = Observations::new(&budget, identity().dataset_id, true, true).unwrap();
    assert_eq!(budget.snapshot().active_permits(), 1);
    assert!(
        4 * MAX_SNAPSHOT_STREAM_BOUNDS * std::mem::size_of::<StreamBound>()
            + std::mem::size_of::<Observations>()
            + 16 * 1024
            <= CACHE_BYTES as usize
    );
    success(&mut cache, ReadCapability::History, 512, 11);
    success(&mut cache, ReadCapability::Export, 512, 22);
    assert!(
        cache
            .snapshot(identity(), 77, SuspendAwareClock.read().unwrap())
            .is_ok()
    );
    success(&mut cache, ReadCapability::Export, 513, 22);
    assert!(matches!(
        cache.snapshot(identity(), 77, SuspendAwareClock.read().unwrap()),
        Err(SessionError::Capacity)
    ));
    drop(cache);
    assert_eq!(budget.snapshot().active_permits(), 0);
}
#[test]
fn ordinary_cache_refuses_oversize_foreign_or_malformed_cut_without_erasing_last_success() {
    let budget = budget();
    let mut cache = Observations::new(&budget, identity().dataset_id, true, false).unwrap();
    success(&mut cache, ReadCapability::History, 1, 11);
    let mut foreign = query(1, 22);
    foreign.dataset = [9; 16].try_into().unwrap();
    let mut malformed = query(1, 22);
    malformed.bounds[0].owner_epoch = 0;
    for bad in [
        query(MAX_SNAPSHOT_STREAM_BOUNDS + 1, 22),
        foreign,
        malformed,
    ] {
        cache.begin(ReadCapability::History);
        cache.observed(ReadCapability::History, &bad, 4);
        cache.finish(ReadCapability::History, true);
        let actual = value(&cache);
        assert_eq!(actual["capabilities"][0]["state"], "unavailable");
        assert_eq!(
            actual["capabilities"][0]["published"]["cut"]["bounds"][0]["last_complete_sequence"],
            "11"
        );
    }
}
