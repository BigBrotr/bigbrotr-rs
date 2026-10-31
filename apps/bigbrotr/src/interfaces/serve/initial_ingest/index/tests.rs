use super::*;
use bigbrotr::runtime::{ProcessEnvelope, WaiterLimits, policy_document::PolicyDocumentVersion};
use bigbrotr_model::{
    DatasetId, DictionaryKind, Digest256, PolicyEpoch, RecoveryGenerationId, TopologyGeneration,
};
use serde_json::json;
use std::collections::BTreeSet;

fn root() -> Resolved {
    // Resolved callback inputs only. This neither activates policy nor creates
    // a source, custody, physical proof or PostgreSQL publication authority.
    Resolved {
        keys: vec![],
        values: json!({
            "indexer.enabled":true,
            "indexer.flush_interval":1_000_000,
            "indexer.batch_max_rows":20_000,
            "indexer.batch_max_bytes":16*1024*1024,
            "indexer.query_memory":256*1024*1024,
            "indexer.insert_workers":2,
            "sync.provenance":"local_first_witness"
        })
        .as_object()
        .unwrap()
        .clone()
        .into_iter()
        .collect(),
        sources: Default::default(),
        local_roles: BTreeSet::from(["index".into()]),
        dependencies: vec![],
        document_sha256: String::new(),
        policy_version: PolicyDocumentVersion::CURRENT,
    }
}

#[test]
fn accepted_profile_preserves_provenance_and_both_documents_actual_ceilings() {
    let mut input = root();
    let on = profile(&input).unwrap();
    assert_eq!(on.provenance, IndexProvenance::LocalFirstWitness);
    assert_eq!((on.source_records, on.source_bytes), (64, 1024 * 1024));
    assert_eq!(
        (on.rows, on.wire_bytes, on.delay),
        (20_000, 16 * 1024 * 1024, Duration::from_secs(1))
    );
    let mut small = root();
    small
        .values
        .insert("indexer.batch_max_rows".into(), json!(4));
    let small = profile(&small).unwrap();
    // The installed signed-event admission has five source facts, projecting
    // into separate families. It must fit the source reader while each target
    // still refuses a fifth row under the accepted four-row insert ceiling.
    assert!(small.source().max_records >= 5);
    assert_eq!(small.rows, 4);
    let mut counts = [4, 1, 1, 1, 1, 1, 1, 0, 1, 1];
    let wire = counts.map(|count| count * 128);
    check_family_bounds(&counts, &wire, small).unwrap();
    counts[0] = 5;
    assert!(check_family_bounds(&counts, &wire, small).is_err());
    input.values.insert("sync.provenance".into(), json!("off"));
    let off = profile(&input).unwrap();
    assert_eq!(off.provenance, IndexProvenance::Off);
    assert_ne!(on, off);
    input
        .values
        .insert("sync.provenance".into(), json!("unknown"));
    assert!(profile(&input).is_err());
    for (key, value) in [
        ("indexer.batch_max_rows", 40_000),
        ("indexer.batch_max_bytes", 32 * 1024 * 1024),
        ("indexer.insert_workers", 3),
    ] {
        let mut input = root();
        input.values.insert(key.into(), json!(value));
        let changed = profile(&input).unwrap();
        assert_eq!((changed.rows, changed.wire_bytes), (on.rows, on.wire_bytes));
        assert_ne!(
            changed, on,
            "different accepted ceilings must not collapse to the same profile"
        );
    }
    for (key, value) in [
        ("indexer.flush_interval", 0),
        ("indexer.batch_max_rows", 0),
        ("indexer.batch_max_bytes", WIRE_OVERHEAD),
        ("indexer.query_memory", 0),
        ("indexer.insert_workers", 0),
    ] {
        let mut input = root();
        input.values.insert(key.into(), json!(value));
        assert!(profile(&input).is_err(), "{key}");
    }
    let mut input = root();
    input.values.insert("indexer.enabled".into(), json!(false));
    assert!(
        profile(&input).is_err(),
        "an explicit role cannot override disabled accepted policy"
    );
    let mut input = root();
    input.local_roles.clear();
    assert!(profile(&input).is_err());
}

fn registration() -> InitialRegistration {
    InitialRegistration {
        dataset_id: [1; 16].try_into().unwrap(),
        instance_id: [2; 16],
        control: ControlOwner {
            resource_id: [3; 16].try_into().unwrap(),
            epoch: OwnerEpoch::new(1).unwrap(),
            role_instance_id: [4; 16],
            process_incarnation: [5; 16].try_into().unwrap(),
        },
        ingest_resource_id: [6; 16].try_into().unwrap(),
        ingest_role_instance_id: [7; 16],
        process_incarnation: [8; 16].try_into().unwrap(),
        shard_id: 0,
        config_revision: 1,
        policy_revision: 1,
        config_digest: Digest256::from_bytes([9; 32]),
        policy_digest: Digest256::from_bytes([10; 32]),
        previous_topology: Some(TopologyGeneration::new(0)),
        allocation_domains: (1..=9)
            .map(|k| {
                (
                    DictionaryKind::try_from(k).unwrap(),
                    [u8::try_from(k).unwrap(); 16].try_into().unwrap(),
                )
            })
            .collect(),
        producer_slot_id: [11; 16].try_into().unwrap(),
        outstanding_request_limit: 32,
        request_receipt_retention: 64,
        created_at_us: 100,
        lease_expires_at_us: 1_000_000,
    }
}
fn identity(spec: &InitialRegistration) -> ManifestIdentity {
    ManifestIdentity {
        dataset_id: spec.dataset_id,
        topology_generation: TopologyGeneration::new(1),
        owner_resource_id: spec.ingest_resource_id,
        shard_id: 0,
        owner_epoch: OwnerEpoch::new(1).unwrap(),
        process_incarnation: spec.process_incarnation,
        recovery_generation: RecoveryGenerationId::from_bytes([12; 16]),
        policy_epoch: PolicyEpoch::new(1),
    }
}

#[test]
fn source_validation_refuses_foreign_successor_policy_and_allocation_identity() {
    let spec = registration();
    let id = identity(&spec);
    let namespaces = AllocationNamespaces::from_entries(&spec.allocation_domains).unwrap();
    validate_source(&IngestManifest::new(id, namespaces), &spec).unwrap();
    let mut changed = id;
    changed.dataset_id = [20; 16].try_into().unwrap();
    let mut cases = vec![changed];
    changed = id;
    changed.owner_resource_id = [21; 16].try_into().unwrap();
    cases.push(changed);
    changed = id;
    changed.process_incarnation = [22; 16].try_into().unwrap();
    cases.push(changed);
    changed = id;
    changed.shard_id = 1;
    cases.push(changed);
    changed = id;
    changed.owner_epoch = OwnerEpoch::new(2).unwrap();
    cases.push(changed);
    changed = id;
    changed.topology_generation = TopologyGeneration::new(2);
    cases.push(changed);
    changed = id;
    changed.policy_epoch = PolicyEpoch::new(2);
    cases.push(changed);
    for changed in cases {
        assert!(validate_source(&IngestManifest::new(changed, namespaces), &spec).is_err());
    }
    let mut domains = spec.allocation_domains.clone();
    domains[0].1 = [23; 16].try_into().unwrap();
    let namespaces = AllocationNamespaces::from_entries(&domains).unwrap();
    assert!(validate_source(&IngestManifest::new(id, namespaces), &spec).is_err());
}

#[test]
fn initial_cut_has_no_publication_and_advances_only_the_same_complete_stream() {
    let id = identity(&registration());
    let zero = zero_cut(id).unwrap();
    assert_eq!(zero.bounds()[0].last_complete_sequence.get(), 0);
    assert_eq!(next_source(id, &zero).unwrap().sequence.get(), 1);
    let cut = |dataset, topology, stream, through| {
        SourceCut::new(
            dataset,
            topology,
            vec![StreamBound {
                stream,
                last_complete_sequence: CompleteThrough::new(through),
            }],
        )
        .unwrap()
    };
    let stream = zero.bounds()[0].stream;
    let next = cut(id.dataset_id, id.topology_generation, stream, 12);
    assert_eq!(next_source(id, &next).unwrap().sequence.get(), 13);
    assert!(
        next_source(
            id,
            &cut(id.dataset_id, id.topology_generation, stream, u64::MAX)
        )
        .is_err()
    );
    assert!(
        next_source(
            id,
            &cut(
                DatasetId::try_from([24; 16]).unwrap(),
                id.topology_generation,
                stream,
                12
            )
        )
        .is_err()
    );
    assert!(
        next_source(
            id,
            &cut(id.dataset_id, TopologyGeneration::new(2), stream, 12)
        )
        .is_err()
    );
    let successor = StreamId {
        owner_epoch: OwnerEpoch::new(2).unwrap(),
        ..stream
    };
    assert!(
        next_source(
            id,
            &cut(id.dataset_id, id.topology_generation, successor, 12)
        )
        .is_err()
    );
    let two = SourceCut::new(
        id.dataset_id,
        id.topology_generation,
        vec![
            zero.bounds()[0],
            StreamBound {
                stream: successor,
                last_complete_sequence: CompleteThrough::new(0),
            },
        ],
    )
    .unwrap();
    assert!(next_source(id, &two).is_err());
    let mut id = id;
    id.owner_epoch = OwnerEpoch::new(2).unwrap();
    assert!(zero_cut(id).is_err());
}

#[test]
fn all_ten_family_outcomes_obey_counts_zero_and_checked_wire_ceiling_before_prepare() {
    let profile = profile(&root()).unwrap();
    let mut counts = [0; 10];
    let mut wire = [0; 10];
    check_family_bounds(&counts, &wire, profile).unwrap();
    for i in 0..10 {
        counts[i] = profile.rows;
        wire[i] = profile.wire_bytes - WIRE_OVERHEAD;
        check_family_bounds(&counts, &wire, profile).unwrap();
        counts[i] += 1;
        assert!(check_family_bounds(&counts, &wire, profile).is_err());
        counts[i] -= 1;
        wire[i] += 1;
        assert!(check_family_bounds(&counts, &wire, profile).is_err());
        wire[i] = u64::MAX;
        assert!(check_family_bounds(&counts, &wire, profile).is_err());
        counts[i] = 0;
        wire[i] = 1;
        assert!(check_family_bounds(&counts, &wire, profile).is_err());
        wire[i] = 0;
    }
}

#[test]
fn reservations_distinguish_unavailable_from_impossible_and_do_not_wait() {
    let profile = profile(&root()).unwrap();
    let needed = profile.required_application().unwrap();
    eprintln!(
        "{}",
        json!({"format":"bigbrotr-initial-budget-requirements/1","role":"index",
        "scope":"compiled-finite-profile-not-a-capacity-claim",
        "application":{"data_bytes":needed.data_bytes(),"items":needed.items(),"native_bytes":needed.native_bytes(),"control_bytes":needed.control_bytes()},
        "retained_data_bytes":prepare::PREPARATION_BYTES+METADATA_BYTES,
        "physical_data_bytes":profile.physical().unwrap().required_local_data_bytes().unwrap(),
        "source_data_bytes":profile.source().required_data_bytes().unwrap(),
        "planner_data_bytes":plan::required_data_bytes(profile.source()).unwrap(),
        "recovery_metadata_bytes":required_recovery_metadata_bytes()})
    );
    assert_eq!(needed.items(), 5);
    assert!(needed.data_bytes() > prepare::PREPARATION_BYTES + METADATA_BYTES);
    assert!(
        needed.data_bytes()
            >= prepare::PREPARATION_BYTES
                + METADATA_BYTES
                + profile
                    .physical()
                    .unwrap()
                    .required_local_data_bytes()
                    .unwrap()
                + required_recovery_metadata_bytes()
    );
    assert!(needed.native_bytes() > 0);
    let budget = ResourceBudget::new(
        ProcessEnvelope::new(
            64,
            0,
            ResourceLimits::new(64, 1, 0, 0),
            WaiterLimits::new(0, 0),
        )
        .unwrap(),
    );
    let held = admitted(&budget, 64, 0).unwrap().unwrap();
    assert!(admitted(&budget, 1, 0).unwrap().is_none());
    assert!(admitted(&budget, 65, 0).is_err());
    assert_eq!(budget.snapshot().active_permits(), 1);
    drop(held);
    assert!(admitted(&budget, 64, 0).unwrap().is_some());
}

#[test]
fn cancellation_and_expiry_refuse_before_dispatch() {
    let cancel = AtomicBool::new(false);
    let later = Instant::now() + Duration::from_secs(60);
    live_window(later, &cancel).unwrap();
    assert!(live_window(Instant::now(), &cancel).is_err());
    cancel.store(true, Ordering::Release);
    assert!(live_window(later, &cancel).is_err());
}

#[test]
fn startup_capacity_includes_retained_control_inputs_and_index_preparation() {
    let profile = profile(&root()).unwrap();
    for (payload, fits) in [(640 * MIB, false), (2 * 1024 * MIB, true)] {
        let budget = ResourceBudget::new(
            ProcessEnvelope::new(
                payload + 256 * MIB,
                0,
                ResourceLimits::new(payload, 16, 256 * MIB, 0),
                WaiterLimits::new(0, 0),
            )
            .unwrap(),
        );
        // Independently specified retained plaintext Control, startup inputs,
        // preparation and actor metadata. Their real permits stay live.
        let control = admitted(&budget, 528 * MIB + MIB / 2 + 8, 0)
            .unwrap()
            .unwrap();
        let inputs = admitted(&budget, 24 * MIB, 0).unwrap().unwrap();
        let preparation = admitted(&budget, 16 * MIB, 0).unwrap().unwrap();
        let metadata = admitted(&budget, MIB, 0).unwrap().unwrap();
        let before = budget.snapshot().used();
        assert_eq!(profile.require_startup_capacity(&budget).is_ok(), fits);
        assert_eq!(budget.snapshot().used(), before);
        assert_eq!(budget.snapshot().active_permits(), 4);
        drop((control, inputs, preparation, metadata));
        assert_eq!(budget.snapshot().active_permits(), 0);
    }
}
