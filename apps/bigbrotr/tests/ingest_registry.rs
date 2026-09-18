use bigbrotr::storage::postgres::ingest_registry::{
    AttemptState, ControlOwner, InitialRegistration, RegistrationAttempt, RegistryError,
};
use bigbrotr_model::{
    AllocationDomainId, DatasetId, DictionaryKind, Digest256, OwnerEpoch, OwnerResourceId,
    ProcessIncarnation, ProducerSlotId, TopologyGeneration,
};

fn specification() -> InitialRegistration {
    InitialRegistration {
        dataset_id: DatasetId::try_from([1; 16]).unwrap(),
        instance_id: [2; 16],
        control: ControlOwner {
            resource_id: OwnerResourceId::try_from([3; 16]).unwrap(),
            epoch: OwnerEpoch::new(u64::MAX).unwrap(),
            role_instance_id: [4; 16],
            process_incarnation: ProcessIncarnation::try_from([5; 16]).unwrap(),
        },
        ingest_resource_id: OwnerResourceId::try_from([6; 16]).unwrap(),
        ingest_role_instance_id: [7; 16],
        process_incarnation: ProcessIncarnation::try_from([5; 16]).unwrap(),
        shard_id: u32::MAX,
        config_revision: u64::MAX,
        policy_revision: u64::MAX,
        config_digest: Digest256::from_bytes([8; 32]),
        policy_digest: Digest256::from_bytes([9; 32]),
        previous_topology: Some(TopologyGeneration::new(u64::MAX - 1)),
        allocation_domains: vec![
            (
                DictionaryKind::VisibilityScope,
                AllocationDomainId::try_from([10; 16]).unwrap(),
            ),
            (
                DictionaryKind::Event,
                AllocationDomainId::try_from([11; 16]).unwrap(),
            ),
        ],
        producer_slot_id: ProducerSlotId::try_from([12; 16]).unwrap(),
        outstanding_request_limit: 2,
        created_at_us: 0,
        lease_expires_at_us: 60_000_000,
    }
}

#[test]
fn full_unsigned_coordinates_and_colocated_process_are_preserved() {
    let spec = specification();
    assert_eq!(spec.target_topology().unwrap().get(), u64::MAX);
    let attempt = RegistrationAttempt::new(spec).unwrap();
    assert_eq!(attempt.state(), AttemptState::Ready);
    assert_eq!(attempt.specification().control.epoch.get(), u64::MAX);
    assert_eq!(attempt.specification().config_revision, u64::MAX);
    assert_eq!(attempt.specification().shard_id, u32::MAX);
}

#[test]
fn missing_topology_is_distinct_from_empty_topology_zero() {
    let mut spec = specification();
    spec.previous_topology = None;
    assert_eq!(spec.target_topology().unwrap().get(), 0);
    spec.previous_topology = Some(TopologyGeneration::new(0));
    assert_eq!(spec.target_topology().unwrap().get(), 1);
    spec.previous_topology = Some(TopologyGeneration::new(u64::MAX));
    assert!(matches!(
        RegistrationAttempt::new(spec),
        Err(RegistryError::Exhausted)
    ));
}

#[test]
fn malformed_initial_specifications_are_refused_without_sql() {
    let changes: &[fn(&mut InitialRegistration)] = &[
        |s| s.instance_id = [0; 16],
        |s| s.control.role_instance_id = [0; 16],
        |s| s.ingest_role_instance_id = [0; 16],
        |s| s.ingest_resource_id = s.control.resource_id,
        |s| s.ingest_role_instance_id = s.control.role_instance_id,
        |s| s.config_revision = 0,
        |s| s.policy_revision = 0,
        |s| s.outstanding_request_limit = 0,
        |s| s.lease_expires_at_us = s.created_at_us,
        |s| s.lease_expires_at_us = s.created_at_us - 1,
        |s| s.allocation_domains.reverse(),
        |s| s.allocation_domains.push(s.allocation_domains[1]),
        |s| s.allocation_domains[1].1 = s.allocation_domains[0].1,
    ];
    for change in changes {
        let mut spec = specification();
        change(&mut spec);
        assert!(matches!(
            RegistrationAttempt::new(spec),
            Err(RegistryError::InvalidInput)
        ));
    }
}
