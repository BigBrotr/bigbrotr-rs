//! Fixed binding and one-shot component checks only. Native custody, SQL waits,
//! COMMIT ambiguity and final delivery still require the connected consumer.
use super::*;
use crate::storage::rocks::{
    manifest::{AllocationNamespaces, ManifestIdentity},
    succession::{PredecessorTerminalKind, SuccessionFields},
};
use bigbrotr_model::{
    AllocationDomainId, DictionaryKind, OwnerEpoch, OwnerResourceId, ProcessIncarnation,
    ProducerEpoch, RecoveryGenerationId, TopologyGeneration,
};

fn binding(epoch: u64) -> LiveSuccessorBinding {
    let identity = ManifestIdentity {
        dataset_id: DatasetId::try_from([1; 16]).unwrap(),
        topology_generation: TopologyGeneration::new(epoch - 1),
        owner_resource_id: OwnerResourceId::try_from([2; 16]).unwrap(),
        shard_id: 0,
        owner_epoch: OwnerEpoch::new(epoch - 1).unwrap(),
        process_incarnation: ProcessIncarnation::try_from([3; 16]).unwrap(),
        recovery_generation: RecoveryGenerationId::from_bytes([0; 16]),
        policy_epoch: PolicyEpoch::new(7),
    };
    let namespaces = |byte| {
        AllocationNamespaces::from_entries(&[(
            DictionaryKind::DatasetLineage,
            AllocationDomainId::try_from([byte; 16]).unwrap(),
        )])
        .unwrap()
    };
    let previous_manifest = if epoch == 3 {
        IngestManifest::for_successor(identity, namespaces(5))
    } else {
        IngestManifest::for_repeatable_successor(identity, namespaces(5))
    };
    let next_manifest = IngestManifest::for_repeatable_successor(
        ManifestIdentity {
            owner_epoch: OwnerEpoch::new(epoch).unwrap(),
            topology_generation: TopologyGeneration::new(epoch),
            process_incarnation: ProcessIncarnation::try_from([4; 16]).unwrap(),
            ..identity
        },
        namespaces(6),
    );
    LiveSuccessorBinding {
        intent: Intent {
            record: SuccessionRecord::new(SuccessionFields {
                terminal_kind: PredecessorTerminalKind::Activated,
                producer_slot_id: ProducerSlotId::try_from([7; 16]).unwrap(),
                predecessor_epoch: ProducerEpoch::new(epoch - 1).unwrap(),
                issued_through: 2,
                last_complete_sequence: 5,
                closure_digest: [8; 32],
                incoming_handoff_sha256: [9; 32],
                previous_manifest,
                next_manifest,
            })
            .unwrap(),
            role: [10; 16],
            policy: PolicySnapshot {
                config_revision: 5,
                policy_revision: 7,
                config_digest: [11; 32],
                policy_digest: [12; 32],
            },
            created: 100,
            deadline: 20_000_100,
            limit: 2,
            preparer: ingest_registry::ControlOwner {
                resource_id: OwnerResourceId::try_from([13; 16]).unwrap(),
                epoch: OwnerEpoch::new(20).unwrap(),
                role_instance_id: [14; 16],
                process_incarnation: ProcessIncarnation::try_from([15; 16]).unwrap(),
            },
        },
        instance: [16; 16],
        activated_at_us: 101,
        transaction: 201,
    }
}

#[test]
fn successor_activation_attempt_is_spent_before_any_result() {
    let mut state = ActivationState::Ready;
    state.start().unwrap();
    // There is deliberately no success/error-dependent reset: cancellation or
    // unknown COMMIT must not turn the same local candidate into a new attempt.
    assert_eq!(state, ActivationState::Spent);
    assert_eq!(state.start(), Err(OwnerError::Unavailable));
    let mut ineligible = ActivationState::Spent;
    assert_eq!(ineligible.start(), Err(OwnerError::Unavailable));
}

#[test]
fn successor_binding_requires_full_target_manifest_and_actual_control() {
    let binding = binding(3);
    let manifest = binding.intent.record.fields().next_manifest;
    let identity = *manifest.identity();
    let current = binding.control();
    binding.require_manifest(&manifest, current).unwrap();
    for other in [
        ingest_registry::ControlOwner {
            resource_id: OwnerResourceId::try_from([20; 16]).unwrap(),
            ..current
        },
        ingest_registry::ControlOwner {
            epoch: OwnerEpoch::new(21).unwrap(),
            ..current
        },
        ingest_registry::ControlOwner {
            role_instance_id: [21; 16],
            ..current
        },
        ingest_registry::ControlOwner {
            process_incarnation: ProcessIncarnation::try_from([22; 16]).unwrap(),
            ..current
        },
    ] {
        assert_eq!(
            binding.require_manifest(&manifest, other),
            Err(OwnerError::InvalidInput)
        );
    }
    for other in [
        ManifestIdentity {
            dataset_id: DatasetId::try_from([23; 16]).unwrap(),
            ..identity
        },
        ManifestIdentity {
            owner_resource_id: OwnerResourceId::try_from([24; 16]).unwrap(),
            ..identity
        },
        ManifestIdentity {
            owner_epoch: OwnerEpoch::new(4).unwrap(),
            ..identity
        },
        ManifestIdentity {
            process_incarnation: ProcessIncarnation::try_from([25; 16]).unwrap(),
            ..identity
        },
        ManifestIdentity {
            topology_generation: TopologyGeneration::new(4),
            ..identity
        },
        ManifestIdentity {
            shard_id: 1,
            ..identity
        },
        ManifestIdentity {
            recovery_generation: RecoveryGenerationId::from_bytes([26; 16]),
            ..identity
        },
        ManifestIdentity {
            policy_epoch: PolicyEpoch::new(8),
            ..identity
        },
    ] {
        let changed =
            IngestManifest::for_repeatable_successor(other, *manifest.allocation_namespaces());
        assert_eq!(
            binding.require_manifest(&changed, current),
            Err(OwnerError::InvalidInput)
        );
    }
    for changed in [
        IngestManifest::for_successor(identity, *manifest.allocation_namespaces()),
        IngestManifest::for_repeatable_successor(
            identity,
            AllocationNamespaces::from_entries(&[]).unwrap(),
        ),
        binding.intent.record.fields().previous_manifest,
    ] {
        assert_eq!(
            binding.require_manifest(&changed, current),
            Err(OwnerError::InvalidInput)
        );
    }
}

#[test]
fn successor_binding_keeps_dynamic_epoch_and_accepted_window() {
    for epoch in [3, 4, 255] {
        let binding = binding(epoch);
        assert_eq!(binding.epoch(), epoch);
        assert_eq!(binding.limit(), 2);
        assert_eq!(binding.policy().policy_revision, 7);
        assert_eq!(binding.policy().config_revision, 5);
        assert_eq!(binding.instance(), [16; 16]);
        assert_eq!(binding.slot(), ProducerSlotId::try_from([7; 16]).unwrap());
        assert_eq!(binding.dataset(), DatasetId::try_from([1; 16]).unwrap());
    }
    // Boundary checks use the observed remaining interval, not the immutable
    // original preparation deadline or a fresh post-COMMIT scheduling window.
    assert_eq!(positive_remaining(1).unwrap(), 1);
    assert_eq!(positive_remaining(i64::MAX).unwrap(), i64::MAX as u64);
    for value in [0, -1, i64::MIN] {
        assert_eq!(positive_remaining(value), Err(OwnerError::Unavailable));
    }
}
