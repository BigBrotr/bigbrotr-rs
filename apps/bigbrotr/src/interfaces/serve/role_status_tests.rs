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
fn catalog() -> OwnerResourceId {
    [5; 16].try_into().unwrap()
}
fn selection(count: usize, through: u64) -> ArchiveReadSelection {
    let query = query(count, through);
    ArchiveReadSelection {
        dataset_id: query.dataset,
        catalog_id: catalog(),
        frontier_revision: 3,
        snapshot_id: query.snapshot_id,
        topology_generation: query.topology_generation,
        bounds: query.bounds,
    }
}
fn full_success(cache: &mut Observations, selected: &ArchiveReadSelection) {
    cache.begin_probe();
    cache.note_current(selected);
    cache.observed_probe(selected);
    cache.finish_probe(true);
}
fn selected_success(
    cache: &mut Observations,
    kind: ReadCapability,
    selected: &ArchiveReadSelection,
) {
    let mut query = query(0, 0);
    query.dataset = selected.dataset_id;
    query.snapshot_id = selected.snapshot_id;
    query.topology_generation = selected.topology_generation;
    query.bounds.clone_from(&selected.bounds);
    cache.begin(kind);
    cache.observed(
        kind,
        &query,
        selected.catalog_id,
        selected.frontier_revision,
    );
    cache.finish(kind, true);
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
    cache.observed(kind, &query(count, through), catalog(), 3);
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
    cache.observed(ReadCapability::History, &query(1, 22), catalog(), 4);
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
    cache.observed(ReadCapability::History, &query(1, 33), catalog(), 5);
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
    let destination_started = SuspendAwareClock.read().unwrap();
    cache.begin_at(ReadCapability::Export, destination_started);
    assert_eq!(
        cache.export.attempt.as_ref().unwrap().started,
        destination_started
    );
    cache.observed(ReadCapability::Export, &query(1, 55), catalog(), 6);
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
fn ordinary_observation_freshness_uses_oldest_required_evidence_at_thirty_seconds() {
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
            .observation(
                CapabilityKind::SignedEventExport,
                Some(age),
                ProbeView {
                    slot: &cache.probe,
                    changed: cache.changed,
                    age: None,
                },
                77,
            )
            .unwrap();
        assert_eq!(observation.freshness, expected);
        assert_eq!(observation.state, CapabilityState::Stale);
        assert_eq!(observation.reason, StatusReason::VerificationStale);
    }
    full_success(&mut cache, &selection(1, 11));
    for (operation_age, full_age, state, age_us) in [
        (
            Duration::ZERO,
            Duration::from_micros(29_999_999),
            CapabilityState::Ready,
            "29999999",
        ),
        (
            Duration::from_micros(29_999_999),
            Duration::ZERO,
            CapabilityState::Ready,
            "29999999",
        ),
        (
            Duration::ZERO,
            Duration::from_secs(30),
            CapabilityState::Stale,
            "30000000",
        ),
        (
            Duration::from_secs(30),
            Duration::ZERO,
            CapabilityState::Stale,
            "30000000",
        ),
    ] {
        let observation = cache
            .export
            .observation(
                CapabilityKind::SignedEventExport,
                Some(operation_age),
                ProbeView {
                    slot: &cache.probe,
                    changed: false,
                    age: Some(full_age),
                },
                79,
            )
            .unwrap();
        assert_eq!(observation.state, state);
        let value = serde_json::to_value(observation).unwrap();
        assert_eq!(value["time"]["age_us"], age_us);
        assert_eq!(value["time"]["observed_at_us"], "79");
    }
}
#[test]
fn ordinary_cache_checks_aggregate_before_response_clone_and_retains_its_grant() {
    let budget = budget();
    let mut cache = Observations::new(&budget, identity().dataset_id, true, true).unwrap();
    assert_eq!(budget.snapshot().active_permits(), 1);
    assert!(
        8 * MAX_SNAPSHOT_STREAM_BOUNDS * std::mem::size_of::<StreamBound>()
            + std::mem::size_of::<Observations>()
            + 16 * 1024
            <= CACHE_BYTES as usize
    );
    success(&mut cache, ReadCapability::History, 512, 11);
    success(&mut cache, ReadCapability::Export, 512, 22);
    full_success(&mut cache, &selection(MAX_SNAPSHOT_STREAM_BOUNDS, 11));
    cache.begin_probe();
    cache.observed_probe(&selection(MAX_SNAPSHOT_STREAM_BOUNDS, 11));
    for kind in [ReadCapability::History, ReadCapability::Export] {
        cache.begin(kind);
        cache.observed(kind, &query(MAX_SNAPSHOT_STREAM_BOUNDS, 11), catalog(), 3);
    }
    assert!(
        cache
            .snapshot(identity(), 77, SuspendAwareClock.read().unwrap())
            .is_ok()
    );
    cache.discard_probe();
    cache.discard_attempt(ReadCapability::History);
    cache.discard_attempt(ReadCapability::Export);
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
        cache.observed(ReadCapability::History, &bad, catalog(), 4);
        cache.finish(ReadCapability::History, true);
        let actual = value(&cache);
        assert_eq!(actual["capabilities"][0]["state"], "unavailable");
        assert_eq!(
            actual["capabilities"][0]["published"]["cut"]["bounds"][0]["last_complete_sequence"],
            "11"
        );
    }
}

#[test]
fn ordinary_ready_requires_both_joined_full_proof_and_relevant_completed_operation() {
    let budget = budget();
    let mut cache = Observations::new(&budget, identity().dataset_id, true, true).unwrap();
    let selected = selection(1, 11);
    full_success(&mut cache, &selected);
    assert_eq!(value(&cache)["capabilities"][0]["state"], "building");
    assert_eq!(value(&cache)["capabilities"][1]["state"], "building");
    success(&mut cache, ReadCapability::History, 1, 11);
    assert_eq!(value(&cache)["capabilities"][0]["state"], "ready");
    cache.begin(ReadCapability::Export);
    cache.observed(ReadCapability::Export, &query(1, 11), catalog(), 3);
    assert_eq!(value(&cache)["capabilities"][1]["state"], "building");
    cache.finish(ReadCapability::Export, false);
    full_success(&mut cache, &selected);
    // Archive success cannot erase an observed output-path failure.
    assert_eq!(value(&cache)["capabilities"][1]["state"], "unavailable");
    success(&mut cache, ReadCapability::Export, 1, 11);
    assert_eq!(value(&cache)["capabilities"][1]["state"], "ready");

    cache.begin_probe();
    let started = cache.probe.attempt.as_ref().unwrap().started;
    cache.begin_probe();
    assert_eq!(cache.probe.attempt.as_ref().unwrap().started, started);
    cache.observed_probe(&selected);
    for cap in value(&cache)["capabilities"].as_array().unwrap() {
        assert_eq!(cap["state"], "building");
    }
    cache.finish_probe(false); // Physical success without cleanup/join is insufficient.
    success(&mut cache, ReadCapability::History, 1, 11);
    success(&mut cache, ReadCapability::Export, 1, 11);
    for cap in value(&cache)["capabilities"].as_array().unwrap() {
        assert_eq!(cap["state"], "unavailable");
    }
    cache.begin_probe();
    cache.observed_probe(&selected);
    cache.discard_probe(); // A known authority refusal cannot erase the failure.
    cache.finish_probe(true);
    assert_eq!(value(&cache)["capabilities"][0]["state"], "unavailable");
    full_success(&mut cache, &selected);
    let actual = value(&cache);
    for cap in actual["capabilities"].as_array().unwrap() {
        assert_eq!(cap["state"], "ready");
    }
    assert!(actual["source"]["cut"].is_null());
    assert_eq!(actual["source"]["freshness"], "unknown");
}

#[test]
fn ordinary_full_proof_matches_every_selection_coordinate_not_an_old_pinned_export() {
    for changed in [
        "catalog", "snapshot", "revision", "topology", "through", "shard", "epoch",
    ] {
        let budget = budget();
        let mut cache = Observations::new(&budget, identity().dataset_id, true, true).unwrap();
        let original = selection(1, 11);
        success(&mut cache, ReadCapability::History, 1, 11);
        success(&mut cache, ReadCapability::Export, 1, 11);
        full_success(&mut cache, &original);
        let mut current = original.clone();
        match changed {
            "catalog" => current.catalog_id = [6; 16].try_into().unwrap(),
            "snapshot" => current.snapshot_id = 6,
            "revision" => current.frontier_revision = 4,
            "topology" => current.topology_generation = TopologyGeneration::new(2),
            "through" => current.bounds[0].complete_through = 12,
            "shard" => current.bounds[0].shard_id = 1,
            "epoch" => current.bounds[0].owner_epoch = 2,
            _ => unreachable!(),
        }
        full_success(&mut cache, &current);
        // This new successful export really read its old pinned selection. It
        // cannot replace the independently loaded current Archive selection.
        selected_success(&mut cache, ReadCapability::Export, &original);
        let actual = value(&cache);
        for cap in actual["capabilities"].as_array().unwrap() {
            assert_eq!(cap["state"], "stale", "{changed}");
            assert_eq!(cap["reason"], "verification_stale", "{changed}");
        }
        selected_success(&mut cache, ReadCapability::Export, &current);
        assert_eq!(
            value(&cache)["capabilities"][1]["state"],
            "ready",
            "{changed}"
        );
        assert_eq!(
            value(&cache)["capabilities"][0]["state"],
            "stale",
            "{changed}"
        );
    }
}

#[test]
fn ordinary_changed_current_selection_cannot_revive_an_older_full_proof() {
    let budget = budget();
    let mut cache = Observations::new(&budget, identity().dataset_id, true, false).unwrap();
    let original = selection(1, 11);
    let current = selection(1, 22);
    success(&mut cache, ReadCapability::History, 1, 11);
    full_success(&mut cache, &original);
    let start = cache.probe.last.as_ref().unwrap().started;
    cache.note_current(&current);
    assert_eq!(value(&cache)["capabilities"][0]["state"], "stale");
    assert_eq!(
        value(&cache)["capabilities"][0]["reason"],
        "verification_stale"
    );
    assert_eq!(cache.probe.last.as_ref().unwrap().started, start);
    cache.note_current(&original);
    assert_eq!(value(&cache)["capabilities"][0]["state"], "stale");
    cache.begin_probe();
    cache.observed_probe(&original);
    cache.note_current(&current); // The final PG comparison saw a newer head.
    cache.finish_probe(true);
    assert_eq!(value(&cache)["capabilities"][0]["state"], "stale");
    assert!(cache.probe.failure.is_none());
    full_success(&mut cache, &current);
    assert_eq!(value(&cache)["capabilities"][0]["state"], "stale");
    selected_success(&mut cache, ReadCapability::History, &current);
    assert_eq!(value(&cache)["capabilities"][0]["state"], "ready");
}

#[test]
fn ordinary_invalid_current_metadata_refuses_without_reviving_an_inflight_old_candidate() {
    let budget = budget();
    let mut cache = Observations::new(&budget, identity().dataset_id, true, false).unwrap();
    let selected = selection(1, 11);
    for change in [
        "foreign",
        "oversize",
        "zero_epoch",
        "snapshot",
        "revision",
        "duplicate",
    ] {
        success(&mut cache, ReadCapability::History, 1, 11);
        full_success(&mut cache, &selected);
        let mut invalid = selected.clone();
        match change {
            "foreign" => invalid.dataset_id = [9; 16].try_into().unwrap(),
            "oversize" => invalid.bounds = selection(MAX_SNAPSHOT_STREAM_BOUNDS + 1, 11).bounds,
            "zero_epoch" => invalid.bounds[0].owner_epoch = 0,
            "snapshot" => invalid.snapshot_id = 0,
            "revision" => invalid.frontier_revision = 0,
            "duplicate" => invalid.bounds.push(invalid.bounds[0]),
            _ => unreachable!(),
        }
        cache.begin_probe();
        cache.observed_probe(&selected);
        cache.note_current(&invalid);
        cache.finish_probe(true);
        let actual = value(&cache);
        assert_eq!(
            actual["capabilities"][0]["state"], "unavailable",
            "{change}"
        );
        assert_eq!(
            actual["capabilities"][0]["published"]["cut"]["bounds"][0]["last_complete_sequence"],
            "11"
        );
        success(&mut cache, ReadCapability::History, 1, 11);
        assert_eq!(
            value(&cache)["capabilities"][0]["state"],
            "unavailable",
            "{change}"
        );
    }
}
