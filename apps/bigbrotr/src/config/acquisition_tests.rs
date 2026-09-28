use super::*;
use crate::config::resolve;
use bigbrotr::{
    runtime::{ProcessEnvelope, ResourceBudget, ResourceLimits, ResourceRequest, WaiterLimits},
    storage::rocks::store::{PreparedCertification, PreparedEvent},
};
use bigbrotr_model::{
    ClockQuality, DictionaryKind, DurationUs, EventIntent, LocalObservation, ObservedTime, Ordinal,
    ProducerEpoch, ProducerRequestId, QualifiedRef, RequestSequence, SignedEvent, WitnessContext,
    acquisition::CollectionSelector,
};
use bigbrotr_protocol::selector::decode_selector;
use serde_json::json;

const LIMIT: usize = 256 * 1024;

fn root() -> Resolved {
    resolve::parse(include_str!("../../../../config/recipes/archive-only.toml")).unwrap()
}

fn reference(kind: DictionaryKind) -> QualifiedRef {
    QualifiedRef {
        dataset_id: [1; 16].try_into().unwrap(),
        dictionary_kind: kind,
        allocation_domain: [2; 16].try_into().unwrap(),
        ordinal: Ordinal::new(1).unwrap(),
    }
}

fn observation(provenance: bool) -> LocalObservation {
    let observer = reference(DictionaryKind::Observer);
    LocalObservation {
        received_at: ObservedTime::new(456),
        observer_id: observer,
        clock_quality: ClockQuality::new(observer, DurationUs::new(1), None, 0).unwrap(),
        witness: provenance.then(|| WitnessContext {
            endpoint_id: reference(DictionaryKind::Endpoint),
            access_scope_id: reference(DictionaryKind::AccessScope),
            verification_method_id: reference(DictionaryKind::Method),
        }),
    }
}

fn selector(floor: u64, ephemeral: &str) -> CollectionSelector {
    decode_selector(
        format!(
            r#"{{"version":1,"filters":[{{}}],"history_since":{floor},"ephemeral":"{ephemeral}"}}"#
        )
        .as_bytes(),
    )
    .unwrap()
}

fn event() -> SignedEvent {
    serde_json::from_slice(include_bytes!(
        "../../../../tests/fixtures/ingest/import-event.jsonl"
    ))
    .unwrap()
}

fn intent(event: SignedEvent, observation: LocalObservation) -> EventIntent {
    EventIntent::network(
        1,
        reference(DictionaryKind::VisibilityScope),
        event,
        observation,
    )
    .unwrap()
}

fn budget() -> ResourceBudget {
    ResourceBudget::new(
        ProcessEnvelope::new(
            32 * 1024 * 1024,
            0,
            ResourceLimits::new(32 * 1024 * 1024, 16, 0, 0),
            WaiterLimits::new(0, 0),
        )
        .unwrap(),
    )
}

fn prepared(intent: EventIntent, limit: usize, budget: &ResourceBudget) -> PreparedEvent {
    PreparedEvent::new(
        ProducerRequestId {
            producer_slot_id: [3; 16].try_into().unwrap(),
            producer_epoch: ProducerEpoch::new(1).unwrap(),
            request_sequence: RequestSequence::new(1).unwrap(),
        },
        intent,
        limit,
        budget
            .try_acquire(ResourceRequest::data(1024 * 1024, 1, 0).unwrap())
            .unwrap(),
    )
    .unwrap()
}

fn check_both(
    policy: &SyncArrivalPolicy<'_>,
    accepted: &Resolved,
    intent: EventIntent,
    limit: usize,
) -> bool {
    let seed = PolicySeed::from_resolved(accepted).unwrap();
    let budget = budget();
    let admission = prepared(intent.clone(), limit, &budget);
    let certification = PreparedCertification::new(
        intent,
        limit,
        budget
            .try_acquire(ResourceRequest::data(1024 * 1024, 1, 0).unwrap())
            .unwrap(),
    )
    .unwrap();
    let first = policy.admission(seed.document(), seed.digest(), &admission);
    let second = policy.certification(seed.document(), seed.digest(), &certification);
    assert_eq!(
        first, second,
        "mutation and read-only authorization must agree"
    );
    first.is_ok()
}

#[test]
fn arrival_requires_exact_verification_limit_without_turning_bad_signatures_into_policy_errors() {
    let root = root();
    let endpoint = Endpoint::parse("wss://relay.example/events").unwrap();
    let selector = selector(0, "retain");
    let observation = observation(true);
    let policy = SyncArrivalPolicy {
        root: &root,
        endpoint: &endpoint,
        route: RouteSelection::Public {},
        selector: &selector,
        visibility: reference(DictionaryKind::VisibilityScope),
        observation,
        provenance_enabled: true,
    };
    assert!(check_both(
        &policy,
        &root,
        intent(event(), observation),
        LIMIT
    ));
    for wrong in [8, LIMIT - 1, LIMIT + 1] {
        assert!(!check_both(
            &policy,
            &root,
            intent(event(), observation),
            wrong
        ));
    }
    let mut forged = event();
    forged.sig = bigbrotr_model::Signature::from_bytes([0; 64]);
    assert!(check_both(
        &policy,
        &root,
        intent(forged, observation),
        LIMIT
    ));
}

#[test]
fn explicit_collection_can_narrow_but_cannot_widen_either_accepted_policy() {
    let root = root();
    let endpoint = Endpoint::parse("wss://relay.example/events").unwrap();
    let observation = observation(true);
    for (floor, ephemeral, accepted_floor, accepted_ephemeral, allowed) in [
        (0, "retain", 0, "retain", true),
        (100, "retain", 99, "retain", true),
        (100, "drop", 100, "retain", true),
        (100, "drop", 100, "drop", true),
        (99, "drop", 100, "drop", false),
        (100, "retain", 100, "drop", false),
        (u64::MAX, "drop", u64::MAX, "drop", true),
    ] {
        let selector = selector(floor, ephemeral);
        let policy = SyncArrivalPolicy {
            root: &root,
            endpoint: &endpoint,
            route: RouteSelection::Public {},
            selector: &selector,
            visibility: reference(DictionaryKind::VisibilityScope),
            observation,
            provenance_enabled: true,
        };
        let mut accepted = root.clone();
        accepted
            .values
            .insert("sync.history_since".into(), json!(accepted_floor));
        accepted
            .values
            .insert("sync.ephemeral".into(), json!(accepted_ephemeral));
        let seed = PolicySeed::from_resolved(&accepted).unwrap();
        assert_eq!(
            authorize_sync_collection(
                &root,
                seed.document(),
                seed.digest(),
                &endpoint,
                RouteSelection::Public {},
                &selector
            )
            .is_ok(),
            allowed,
            "the collection gate must also run before issuing a request"
        );
        assert_eq!(
            check_both(&policy, &accepted, intent(event(), observation), LIMIT),
            allowed
        );
    }
}

#[test]
fn provenance_is_an_exact_objective_and_arrival_attribution_cannot_be_substituted() {
    let root = root();
    let endpoint = Endpoint::parse("wss://relay.example/events").unwrap();
    let selector = selector(0, "retain");
    for enabled in [false, true] {
        let observation = observation(enabled);
        let policy = SyncArrivalPolicy {
            root: &root,
            endpoint: &endpoint,
            route: RouteSelection::Public {},
            selector: &selector,
            visibility: reference(DictionaryKind::VisibilityScope),
            observation,
            provenance_enabled: enabled,
        };
        let mut accepted = root.clone();
        accepted.values.insert(
            "sync.provenance".into(),
            json!(if enabled {
                "local_first_witness"
            } else {
                "off"
            }),
        );
        assert!(check_both(
            &policy,
            &accepted,
            intent(event(), observation),
            LIMIT
        ));
        let mut other = observation;
        other.received_at = ObservedTime::new(455);
        assert!(!check_both(
            &policy,
            &accepted,
            intent(event(), other),
            LIMIT
        ));
        let mut changed_visibility = policy.visibility;
        changed_visibility.allocation_domain = [9; 16].try_into().unwrap();
        assert!(!check_both(
            &policy,
            &accepted,
            EventIntent::network(1, changed_visibility, event(), observation).unwrap(),
            LIMIT
        ));
        if let Some(witness) = observation.witness {
            for changed in [
                WitnessContext {
                    endpoint_id: QualifiedRef {
                        ordinal: Ordinal::new(2).unwrap(),
                        ..witness.endpoint_id
                    },
                    ..witness
                },
                WitnessContext {
                    access_scope_id: QualifiedRef {
                        ordinal: Ordinal::new(2).unwrap(),
                        ..witness.access_scope_id
                    },
                    ..witness
                },
                WitnessContext {
                    verification_method_id: QualifiedRef {
                        ordinal: Ordinal::new(2).unwrap(),
                        ..witness.verification_method_id
                    },
                    ..witness
                },
            ] {
                assert!(!check_both(
                    &policy,
                    &accepted,
                    intent(
                        event(),
                        LocalObservation {
                            witness: Some(changed),
                            ..observation
                        }
                    ),
                    LIMIT
                ));
            }
        }
        let wrong_flag = SyncArrivalPolicy {
            provenance_enabled: !enabled,
            ..policy
        };
        assert!(!check_both(
            &wrong_flag,
            &accepted,
            intent(event(), observation),
            LIMIT
        ));
        other = observation;
        other.observer_id.ordinal = Ordinal::new(2).unwrap();
        assert!(!check_both(
            &policy,
            &accepted,
            intent(event(), other),
            LIMIT
        ));
        other = observation;
        other.clock_quality =
            ClockQuality::new(other.observer_id, DurationUs::new(2), None, 0).unwrap();
        assert!(!check_both(
            &policy,
            &accepted,
            intent(event(), other),
            LIMIT
        ));
        let imported = EventIntent::import(
            1,
            policy.visibility,
            event(),
            reference(DictionaryKind::DatasetLineage),
        )
        .unwrap();
        assert!(!check_both(&policy, &accepted, imported, LIMIT));
        accepted.values.insert(
            "sync.provenance".into(),
            json!(if enabled {
                "off"
            } else {
                "local_first_witness"
            }),
        );
        assert!(!check_both(
            &policy,
            &accepted,
            intent(event(), observation),
            LIMIT
        ));
    }
}

#[test]
fn fresh_arrival_rechecks_service_endpoint_bans_and_current_private_roots() {
    let mut root = root();
    let endpoint = Endpoint::parse("wss://relay.example/events").unwrap();
    let selector = selector(0, "retain");
    let observation = observation(true);
    let policy = SyncArrivalPolicy {
        root: &root,
        endpoint: &endpoint,
        route: RouteSelection::Public {},
        selector: &selector,
        visibility: reference(DictionaryKind::VisibilityScope),
        observation,
        provenance_enabled: true,
    };
    for (key, value) in [
        ("sync.enabled", json!(false)),
        ("network.public.enabled", json!(false)),
        (
            "relay_policy.banned",
            json!([{"endpoint":"wss://relay.example/events"}]),
        ),
        ("relay_policy.banned", json!([{"host":"relay.example"}])),
    ] {
        let mut accepted = root.clone();
        accepted.values.insert(key.into(), value);
        assert!(!check_both(
            &policy,
            &accepted,
            intent(event(), observation),
            LIMIT
        ));
    }
    let mut accepted = root.clone();
    accepted
        .values
        .insert("sync.endpoints.mode".into(), json!("fixed"));
    accepted.values.insert(
        "sync.endpoints.urls".into(),
        json!(["wss://relay.example/other"]),
    );
    assert!(!check_both(
        &policy,
        &accepted,
        intent(event(), observation),
        LIMIT
    ));
    accepted = root.clone();
    accepted.policy_version = PolicyDocumentVersion::V1;
    assert!(!check_both(
        &policy,
        &accepted,
        intent(event(), observation),
        LIMIT
    ));
    root.values.insert(
        "network.private.rules".into(),
        json!([
            {"endpoint":"ws://127.0.0.1:7447/private", "cidr":"127.0.0.1/32", "port":7447}
        ]),
    );
    let endpoint = Endpoint::parse("ws://127.0.0.1:7447/private").unwrap();
    let policy = SyncArrivalPolicy {
        root: &root,
        endpoint: &endpoint,
        route: RouteSelection::Private {
            cidr: "127.0.0.1/32".parse().unwrap(),
        },
        selector: &selector,
        visibility: reference(DictionaryKind::VisibilityScope),
        observation,
        provenance_enabled: true,
    };
    assert!(check_both(
        &policy,
        &root,
        intent(event(), observation),
        LIMIT
    ));
    let accepted = root.clone();
    let mut restricted = root.clone();
    restricted
        .values
        .insert("network.private.rules".into(), json!([]));
    let policy = SyncArrivalPolicy {
        root: &restricted,
        ..policy
    };
    assert!(!check_both(
        &policy,
        &accepted,
        intent(event(), observation),
        LIMIT
    ));
}
