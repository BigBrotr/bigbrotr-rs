//! Real connected admission fixture for the isolated publication experiment.
use super::*;
use bigbrotr::storage::{
    postgres::{
        control_owner::{InitialBatchRequest, InitialEventRequest},
        ingest_registry::{InitialRegistration, RegistrationAttempt, RegistrationObservation},
    },
    rocks::{
        RocksNativeLimits, RocksNativeSettings,
        manifest::{AllocationNamespaces, IngestManifest, ManifestIdentity},
        store::{
            AdmissionLimits, AdmissionOutcome, BootstrapLimits, IngestStore, PendingReadLimits,
            PreparedEvent, RejectionReason,
        },
    },
};
use bigbrotr_model::*;

pub fn reference(kind: DictionaryKind) -> QualifiedRef {
    QualifiedRef {
        dataset_id: dataset(),
        dictionary_kind: kind,
        allocation_domain: [kind as u8; 16].try_into().unwrap(),
        ordinal: Ordinal::new(1).unwrap(),
    }
}
fn domains() -> Vec<(DictionaryKind, AllocationDomainId)> {
    [
        DictionaryKind::Endpoint,
        DictionaryKind::Observer,
        DictionaryKind::AccessScope,
        DictionaryKind::VisibilityScope,
        DictionaryKind::Method,
        DictionaryKind::Event,
    ]
    .map(|k| (k, reference(k).allocation_domain))
    .into()
}
fn dictionaries() -> Vec<DictionaryEntry> {
    use DictionaryKind as K;
    [
        (
            K::VisibilityScope,
            DictionaryDescriptor::Visibility {
                class: DescriptorLabel::new("public".into()).unwrap(),
            },
        ),
        (
            K::Endpoint,
            DictionaryDescriptor::Endpoint(
                EndpointDescriptor::new("wss://public.example/".into()).unwrap(),
            ),
        ),
        (
            K::Observer,
            DictionaryDescriptor::Observer(ObserverDescriptor {
                clock: Some(ClockSource::SystemUtc),
                vantage: Some(DescriptorLabel::new("fixture-vantage".into()).unwrap()),
                software: None,
                method_id: None,
                public_key: None,
                route_class: None,
                software_version: None,
            }),
        ),
        (
            K::AccessScope,
            DictionaryDescriptor::AccessScope(AccessScopeDescriptor {
                class: AccessClass::Public,
                label: None,
                visibility_scope_id: reference(K::VisibilityScope),
            }),
        ),
        (K::Method, DictionaryDescriptor::EventVerification),
    ]
    .into_iter()
    .map(|(kind, value)| DictionaryEntry::new(reference(kind), value).unwrap())
    .collect()
}
fn native() -> RocksNativeLimits {
    RocksNativeLimits::new(RocksNativeSettings {
        block_cache_bytes: 32 * MIB,
        write_buffer_manager_bytes: 16 * MIB,
        per_cf_write_buffer_bytes: 2 * MIB,
        max_total_wal_bytes: 16 * MIB,
        max_open_files: 32,
        max_background_jobs: 2,
        native_allowance_bytes: 128 * MIB,
    })
    .unwrap()
}
pub fn event() -> SignedEvent {
    let corpus: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../experiments/identity-contract/signed-fixture.json"
    ))
    .unwrap();
    serde_json::from_value(corpus["events"]["E_public"].clone()).unwrap()
}
pub fn pending(
    store: &IngestStore,
    sequence: u64,
    budget: &ResourceBudget,
    cancel: &AtomicBool,
) -> bigbrotr::storage::rocks::store::PendingBatch {
    store
        .read_pending_batch(
            SourceRecordId {
                dataset_id: dataset(),
                shard_id: 0,
                owner_epoch: OwnerEpoch::new(1).unwrap(),
                sequence: Sequence::new(sequence).unwrap(),
            },
            PendingReadLimits {
                max_records: 16,
                max_encoded_bytes: MIB as usize,
                fact: bigbrotr_model::codec::FactDecodeLimits::default(),
            },
            grant(budget, 16 * MIB, 0),
            cancel,
        )
        .unwrap()
        .unwrap()
}
pub fn logical_state(path: &Path) -> (usize, String) {
    // Independent raw native read after the writable owner has closed. Include
    // every key/value in all three families, not just pending markers/counts.
    let db = rocksdb::DB::open_cf_for_read_only(
        &rocksdb::Options::default(),
        path,
        ["default", "facts", "receipts"],
        false,
    )
    .unwrap();
    let mut digest = Sha256::new();
    let mut count = 0;
    for cf in ["default", "facts", "receipts"] {
        digest.update(cf.as_bytes());
        for row in db.iterator_cf(db.cf_handle(cf).unwrap(), rocksdb::IteratorMode::Start) {
            let (key, value) = row.unwrap();
            count += 1;
            assert!(count < 2048 && value.len() < MIB as usize);
            for bytes in [&key, &value] {
                digest.update((bytes.len() as u64).to_be_bytes());
                digest.update(bytes);
            }
        }
    }
    (count, hex(&digest.finalize()))
}

pub async fn prepare(
    owner: &mut ControlSession,
    policy: bigbrotr::storage::postgres::control_owner::PolicySnapshot,
    observer: &tokio_postgres::Client,
    path: &Path,
    budget: &ResourceBudget,
    cancel: &AtomicBool,
) -> IngestStore {
    let now: i64 = observer
        .query_one(
            "SELECT (EXTRACT(EPOCH FROM pg_catalog.clock_timestamp())*1000000)::bigint",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    let spec = InitialRegistration {
        dataset_id: dataset(),
        instance_id: [2; 16],
        control: owner.identity(),
        ingest_resource_id: [61; 16].try_into().unwrap(),
        ingest_role_instance_id: [62; 16],
        process_incarnation: [63; 16].try_into().unwrap(),
        shard_id: 0,
        config_revision: 1,
        policy_revision: 1,
        config_digest: Digest256::from_bytes(policy.config_digest),
        policy_digest: Digest256::from_bytes(policy.policy_digest),
        previous_topology: None,
        allocation_domains: domains(),
        producer_slot_id: [66; 16].try_into().unwrap(),
        outstanding_request_limit: 4,
        created_at_us: now,
        lease_expires_at_us: now + 60_000_000,
    };
    assert_eq!(
        owner
            .register_ingest(&mut RegistrationAttempt::new(spec.clone()).unwrap())
            .await
            .unwrap(),
        RegistrationObservation::Matching { current: true }
    );
    let manifest = IngestManifest::new(
        ManifestIdentity {
            dataset_id: dataset(),
            topology_generation: TopologyGeneration::new(0),
            owner_resource_id: spec.ingest_resource_id,
            shard_id: 0,
            owner_epoch: OwnerEpoch::new(1).unwrap(),
            process_incarnation: spec.process_incarnation,
            recovery_generation: RecoveryGenerationId::from_bytes([67; 16]),
            policy_epoch: PolicyEpoch::new(1),
        },
        AllocationNamespaces::from_entries(&spec.allocation_domains).unwrap(),
    );
    let mut store =
        IngestStore::initialize(path, manifest, native(), grant(budget, 0, 128 * MIB)).unwrap();
    let scope = reference(DictionaryKind::VisibilityScope);
    let intent = EvidenceIntent::new(
        1,
        scope,
        dictionaries(),
        EvidenceIntentLimits {
            max_entries: 8,
            max_encoded_intent_bytes: 8192,
            max_validation_scratch_bytes: 4096,
        },
    )
    .unwrap();
    let receipt = owner
        .bootstrap_initial_dictionaries(
            &mut store,
            InitialBatchRequest {
                ingest_root: path,
                registration: &spec,
                intent: &intent,
                recorded_at: ObservedTime::new(0),
                limits: BootstrapLimits {
                    max_mappings: 8,
                    max_batch_bytes: MIB as usize,
                },
                policy_ttl: Duration::from_secs(5),
                cancelled: cancel,
            },
            grant(budget, 8 * MIB, 4 * MIB),
            validate,
        )
        .await
        .unwrap();
    assert_eq!(receipt.receipt().batch().unwrap().first().sequence.get(), 1);
    assert_eq!(receipt.receipt().batch().unwrap().last().sequence.get(), 5);
    drop(receipt);
    let observation = LocalObservation {
        received_at: ObservedTime::new(10_000_000),
        observer_id: reference(DictionaryKind::Observer),
        clock_quality: ClockQuality::new(
            reference(DictionaryKind::Observer),
            DurationUs::new(1),
            None,
            0,
        )
        .unwrap(),
        witness: Some(WitnessContext {
            endpoint_id: reference(DictionaryKind::Endpoint),
            access_scope_id: reference(DictionaryKind::AccessScope),
            verification_method_id: reference(DictionaryKind::Method),
        }),
    };
    let mut successful = None;
    for (sequence, bad) in [(2, "signature"), (3, "id"), (4, "valid"), (4, "retry")] {
        let mut input = event();
        if bad == "signature" {
            input.sig = Signature::from_bytes([0; 64]);
        }
        if bad == "id" {
            input.id = EventId::from_bytes([0; 32]);
        }
        let prepared = PreparedEvent::new(
            ProducerRequestId {
                producer_slot_id: spec.producer_slot_id,
                producer_epoch: ProducerEpoch::new(1).unwrap(),
                request_sequence: RequestSequence::new(sequence).unwrap(),
            },
            EventIntent::network(1, scope, input, observation).unwrap(),
            256 * 1024,
            grant(budget, MIB, 0),
        )
        .unwrap();
        let result = owner
            .admit_initial_event(
                &mut store,
                InitialEventRequest {
                    ingest_root: path,
                    registration: &spec,
                    event: prepared,
                    provenance_enabled: true,
                    recorded_at: ObservedTime::new(if bad == "retry" { 99 } else { 10_000_000 }),
                    limits: AdmissionLimits {
                        max_outstanding_requests: 4,
                        ..AdmissionLimits::default()
                    },
                    policy_ttl: Duration::from_secs(5),
                    cancelled: cancel,
                },
                grant(budget, 40 * MIB, 10 * MIB),
                |document, digest, candidate| {
                    validate(document, digest)?;
                    // This closed fixture source authorizes exactly this network arrival,
                    // including its rejection controls. It is not installed source policy.
                    if candidate.visibility_scope_id() != scope
                        || candidate.origin() != EventOrigin::Network
                        || candidate.observation() != Some(&observation)
                        || candidate.verification_limit_bytes() != 256 * 1024
                    {
                        return Err(OwnerError::InvalidPolicy);
                    }
                    if matches!(bad, "valid" | "retry")
                        && candidate
                            .verified_event()
                            .is_none_or(|v| v.event() != &event())
                    {
                        return Err(OwnerError::InvalidPolicy);
                    }
                    Ok(())
                },
            )
            .await
            .unwrap();
        match bad {
            "signature" => assert!(matches!(
                result.outcome(),
                AdmissionOutcome::Rejected(RejectionReason::Signature)
            )),
            "id" => assert!(matches!(
                result.outcome(),
                AdmissionOutcome::Rejected(RejectionReason::EventId)
            )),
            _ => {
                let AdmissionOutcome::Durable(receipt) = result.outcome() else {
                    panic!("signed fixture rejected")
                };
                assert_eq!(receipt.batch().unwrap().first().sequence.get(), 6);
                assert_eq!(receipt.batch().unwrap().last().sequence.get(), 10);
                let bytes = receipt.encode_persistent(8192).unwrap();
                if let Some(previous) = &successful {
                    assert_eq!(&bytes, previous);
                } else {
                    successful = Some(bytes);
                }
            }
        }
    }
    drop(store);
    let before = logical_state(path);
    let store = IngestStore::reopen(path, manifest, native(), grant(budget, 0, 128 * MIB)).unwrap();
    emit(
        serde_json::json!({"event":"signed-reopened","rocks_rows":before.0,"rocks_digest":before.1,"rejections":["Signature","EventId"],"exact_admission_retry":true}),
    );
    store
}
