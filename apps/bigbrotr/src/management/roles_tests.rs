use super::*;
use crate::runtime::{
    ProcessEnvelope, ResourceBudget, ResourceLimits, ResourceRequest, WaiterLimits,
};
use bigbrotr_model::{CompleteThrough, OwnerEpoch, StreamBound, StreamId, TopologyGeneration};

fn identity() -> SnapshotIdentity {
    SnapshotIdentity {
        dataset_id: [1; 16].try_into().unwrap(),
        process_incarnation: [2; 16].try_into().unwrap(),
        config_revision: "18446744073709551615".to_owned().try_into().unwrap(),
        policy_revision: "9007199254740993".to_owned().try_into().unwrap(),
    }
}
fn unknown_source() -> SourceObservation {
    SourceObservation {
        cut: None,
        freshness: Freshness::Unknown,
        time: None,
        reason: StatusReason::NotObserved,
    }
}
fn roles() -> Vec<RoleObservation> {
    vec![RoleObservation {
        role: RoleKind::Control,
        state: RoleState::Ready,
        reason: StatusReason::Current,
    }]
}
fn capability() -> CapabilityObservation {
    CapabilityObservation {
        capability: CapabilityKind::IndexPublication,
        state: CapabilityState::Stale,
        reason: StatusReason::VerificationStale,
        freshness: Freshness::Stale,
        time: Some(ObservationTime::new(i64::MIN, Duration::from_micros(u64::MAX)).unwrap()),
        published: Some(PublishedCut {
            revision: "9".to_owned().try_into().unwrap(),
            cut: make_cut(1),
        }),
        generation: Some([3; 16].try_into().unwrap()),
        storage_incarnation: Some([4; 16].try_into().unwrap()),
    }
}
fn make_cut(count: usize) -> SourceCut {
    SourceCut::new(
        identity().dataset_id,
        TopologyGeneration::new(u64::MAX),
        (0..count)
            .map(|n| StreamBound {
                stream: StreamId {
                    shard_id: u32::MAX - count as u32 + n as u32,
                    owner_epoch: OwnerEpoch::new(u64::MAX).unwrap(),
                },
                last_complete_sequence: CompleteThrough::new(u64::MAX),
            })
            .collect(),
    )
    .unwrap()
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
#[test]
fn role_observation_keeps_unknown_source_and_stale_verified_cut_without_authority() {
    let snapshot = RoleSnapshot::new(
        identity(),
        i64::MAX,
        unknown_source(),
        roles(),
        vec![capability()],
    )
    .unwrap();
    let actual = serde_json::to_value(snapshot).unwrap();
    let expected: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../tests/fixtures/management/role-status.json"
    ))
    .unwrap();
    assert_eq!(actual, expected);
}
#[test]
fn role_observation_refuses_foreign_cuts_duplicates_and_inconsistent_freshness() {
    let mut foreign = capability();
    foreign.published.as_mut().unwrap().cut = SourceCut::new(
        [5; 16].try_into().unwrap(),
        TopologyGeneration::new(1),
        vec![],
    )
    .unwrap();
    assert!(RoleSnapshot::new(identity(), 0, unknown_source(), roles(), vec![foreign]).is_err());
    assert!(
        RoleSnapshot::new(
            identity(),
            0,
            unknown_source(),
            roles(),
            vec![capability(), capability()]
        )
        .is_err()
    );
    let mut duplicate = roles();
    duplicate.extend(roles());
    assert!(RoleSnapshot::new(identity(), 0, unknown_source(), duplicate, vec![]).is_err());
    assert!(RoleSnapshot::new(identity(), 0, unknown_source(), vec![], vec![]).is_err());
    let mut unknown_ready = capability();
    unknown_ready.state = CapabilityState::Ready;
    assert!(
        RoleSnapshot::new(
            identity(),
            0,
            unknown_source(),
            roles(),
            vec![unknown_ready]
        )
        .is_err()
    );
    let mut unknown = unknown_source();
    unknown.freshness = Freshness::Fresh;
    assert!(RoleSnapshot::new(identity(), 0, unknown, roles(), vec![]).is_err());
    let mut wrong_kind = capability();
    wrong_kind.capability = CapabilityKind::SignedEventExport;
    assert!(RoleSnapshot::new(identity(), 0, unknown_source(), roles(), vec![wrong_kind]).is_err());
    assert!(ObservationTime::new(0, Duration::MAX).is_err());
}
#[test]
fn role_observation_full_stream_bound_encodes_without_truncation_and_holds_grant() {
    let budget = budget();
    let permit = budget
        .try_acquire(ResourceRequest::data(RESPONSE_RESERVATION, 1, 0).unwrap())
        .unwrap();
    let mut source = unknown_source();
    source.cut = Some(make_cut(MAX_INDEX_STREAMS));
    source.freshness = Freshness::Fresh;
    source.time = Some(ObservationTime::new(0, Duration::ZERO).unwrap());
    let snapshot = RoleSnapshot::new(identity(), 0, source, roles(), vec![]).unwrap();
    let body = snapshot.encode(permit).unwrap();
    assert!(body.as_ref().len() <= RESPONSE_BYTES);
    let value: serde_json::Value = serde_json::from_slice(body.as_ref()).unwrap();
    assert_eq!(
        value["source"]["cut"]["bounds"].as_array().unwrap().len(),
        MAX_INDEX_STREAMS
    );
    assert_eq!(budget.snapshot().active_permits(), 1);
    drop(body);
    assert_eq!(budget.snapshot().active_permits(), 0);
    let mut source = unknown_source();
    source.cut = Some(make_cut(MAX_INDEX_STREAMS));
    source.freshness = Freshness::Fresh;
    source.time = Some(ObservationTime::new(0, Duration::ZERO).unwrap());
    assert!(matches!(
        RoleSnapshot::new(identity(), 0, source, roles(), vec![capability()]),
        Err(SessionError::Capacity)
    ));
    let permit = budget
        .try_acquire(ResourceRequest::data(1, 1, 0).unwrap())
        .unwrap();
    assert!(
        RoleSnapshot::new(identity(), 0, unknown_source(), roles(), vec![])
            .unwrap()
            .encode(permit)
            .is_err()
    );
    assert_eq!(budget.snapshot().active_permits(), 0);
}
