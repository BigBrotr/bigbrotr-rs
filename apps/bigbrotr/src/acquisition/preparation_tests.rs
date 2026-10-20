//! Test-only construction lives under the protocol owner. No production API can
//! manufacture a retained request/receive sample or relabel its raw event.
use super::*;
use crate::{
    acquisition::{
        observation::StoredObservation,
        preparation::CapturedEventInput,
        relay_source::{RelaySourceBinding, StoredRelaySource},
        scope::StoredAcquisitionScope,
    },
    network::RouteSelection,
    runtime::{ProcessEnvelope, ResourceLimits, WaiterLimits},
};
use bigbrotr_model::{
    DictionaryKind, Ordinal, PolicyEpoch, QualifiedRef, acquisition::AcquisitionScope,
};
use bigbrotr_protocol::{endpoint::Endpoint, selector::decode_selector};
use sha2::{Digest, Sha256};

#[test]
fn preparation_binds_original_raw_event_receive_sample_and_certified_intent() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../tests/fixtures/acquisition/selector-events.json"
    ))
    .unwrap();
    let selector = decode_selector(&serde_json::to_vec(&fixture["collection"]).unwrap()).unwrap();
    let request_selector =
        decode_selector(&serde_json::to_vec(&fixture["request"]).unwrap()).unwrap();
    let budget = ResourceBudget::new(
        ProcessEnvelope::new(
            64 * 1024 * 1024,
            0,
            ResourceLimits::new(48 * 1024 * 1024, 32, 0, 0),
            WaiterLimits::new(0, 0),
        )
        .unwrap(),
    );
    let grant = |bytes| {
        budget
            .try_acquire(ResourceRequest::data(bytes, 1, 0).unwrap())
            .unwrap()
    };
    let dataset = [1; 16].try_into().unwrap();
    let visibility = QualifiedRef {
        dataset_id: dataset,
        dictionary_kind: DictionaryKind::VisibilityScope,
        allocation_domain: [5; 16].try_into().unwrap(),
        ordinal: Ordinal::new(1).unwrap(),
    };
    let observer = QualifiedRef {
        dictionary_kind: DictionaryKind::Observer,
        ..visibility
    };
    let scope = StoredAcquisitionScope::new(
        [6; 16].try_into().unwrap(),
        AcquisitionScope::new(
            dataset,
            [2; 16].try_into().unwrap(),
            [3; 16].try_into().unwrap(),
            [4; 16].try_into().unwrap(),
            visibility,
            PolicyEpoch::new(1),
        )
        .unwrap(),
        selector,
        grant(65536),
    );
    let source = StoredRelaySource::new(
        RelaySourceBinding {
            endpoint_id: scope.scope().endpoint(),
            access_scope_id: scope.scope().access(),
            enrollment_generation: 1.try_into().unwrap(),
            route_id: [7; 16].try_into().unwrap(),
        },
        Endpoint::parse("wss://relay.example/").unwrap(),
        RouteSelection::Public {},
        grant(128 * 1024),
    );
    let attribution = StoredObservation {
        visibility,
        observer,
        clock: observer,
        observer_route: None,
        clock_route: None,
        witness: None,
        permit: grant(128 * 1024),
    };
    let job = [8; 16].try_into().unwrap();
    let sample = ReceiveClock::new().unwrap().sample().unwrap();
    // Extra literal whitespace remains part of raw identity, not normalized JSON.
    let json = format!(
        " {} ",
        serde_json::to_string(&fixture["events"][0]["event"]).unwrap()
    )
    .into_bytes();
    let captured = RetainedRelayEvent {
        json,
        sequence: 17,
        received: sample,
        request: ActiveRequest {
            id: RequestId([b'a'; 48]),
            filters: request_selector.filters(),
            limit: Some(3),
        },
        limits: EventLimits::default(),
        permit: grant(65536),
    };
    let make = || {
        CapturedEventInput::prepare(
            &captured,
            job,
            &scope,
            &source,
            &attribution,
            grant(10 * 1024 * 1024),
        )
        .unwrap()
    };
    let (intent, binding, _permit) = make().into_parts();
    assert_eq!(
        binding.input_sha256,
        <[u8; 32]>::from(Sha256::digest(captured.json()))
    );
    assert_eq!(binding.request, [b'a'; 48]);
    assert_eq!(binding.wire_sequence, 17);
    assert_eq!(binding.observed_at_us, sample.received_at().get());
    assert_eq!(binding.certification_digest, intent.certification_digest());
    assert_eq!(binding.source, source.binding());
    assert_eq!(binding.job, job);
    assert_eq!(binding.scope, scope.identity());
    let original =
        bigbrotr_protocol::decode_event_json(captured.json(), captured.limits()).unwrap();
    assert_eq!(intent.event(), &original);
    let prepared =
        crate::storage::rocks::store::PreparedCertification::from_captured(make(), 256 * 1024)
            .unwrap();
    assert_eq!(prepared.intent_digest(), binding.certification_digest);
    assert_eq!(prepared.verified_event().unwrap().event(), &original);
}
