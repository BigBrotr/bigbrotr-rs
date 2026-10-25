//! Finite isolated engine acceptance client, never an installation entrypoint.
use bigbrotr::{
    runtime::{
        OwnedPermit, ProcessEnvelope, ResourceBudget, ResourceLimits, ResourceRequest, WaiterLimits,
    },
    storage::{
        postgres::{
            control_owner::{
                ClaimRequest, ControlSession, InitialBatchError, InitialBatchRequest, OwnerError,
            },
            ingest_registry::{InitialRegistration, RegistrationAttempt, RegistrationObservation},
        },
        rocks::{
            RocksNativeLimits, RocksNativeSettings,
            manifest::{AllocationNamespaces, IngestManifest, ManifestIdentity},
            store::{BootstrapLimits, IngestStore, PendingReadLimits},
        },
    },
};
use bigbrotr_model::{
    DescriptorLabel, DictionaryDescriptor, DictionaryEntry, DictionaryKind, Digest256,
    EvidenceIntent, EvidenceIntentLimits, ObservedTime, Ordinal, OwnerEpoch, PolicyEpoch,
    QualifiedRef, RecoveryGenerationId, TopologyGeneration,
};
use std::{cell::Cell, io::Write, path::Path, sync::atomic::AtomicBool, time::Duration};
use tokio_postgres::{Config, NoTls};

#[allow(dead_code)]
#[path = "support/acquisition_config.rs"]
mod config;
#[path = "support/ingest_closing.rs"]
mod ingest_closing;
#[path = "support/ingest_compacted_fixture.rs"]
mod ingest_compacted_fixture;
#[path = "support/ingest_dictionary_append.rs"]
mod ingest_dictionary_append;
#[path = "support/ingest_dictionary_faults.rs"]
mod ingest_dictionary_faults;
#[path = "support/ingest_event.rs"]
mod ingest_event;
#[path = "support/ingest_history.rs"]
mod ingest_history;
#[path = "support/ingest_initial_empty.rs"]
mod ingest_initial_empty;
#[path = "support/ingest_initial_history.rs"]
mod ingest_initial_history;
#[path = "support/ingest_lease.rs"]
mod ingest_lease;
#[path = "support/ingest_observation.rs"]
mod ingest_observation;
#[path = "support/ingest_recovery.rs"]
mod ingest_recovery;
#[path = "support/ingest_recovery_process.rs"]
mod ingest_recovery_process;
#[path = "support/ingest_relay_source.rs"]
mod ingest_relay_source;
#[path = "support/ingest_snapshot.rs"]
mod ingest_snapshot;
#[path = "support/ingest_socket_arrival.rs"]
mod ingest_socket_arrival;
#[path = "support/ingest_source_cleanup.rs"]
mod ingest_source_cleanup;
#[path = "support/ingest_source_faults.rs"]
mod ingest_source_faults;
#[path = "support/ingest_source_races.rs"]
mod ingest_source_races;
#[path = "support/ingest_source_transport.rs"]
mod ingest_source_transport;
#[path = "support/ingest_succession.rs"]
mod ingest_succession;
#[path = "support/ingest_successor_history.rs"]
mod ingest_successor_history;
#[path = "support/ingest_visibility.rs"]
mod ingest_visibility;
#[path = "support/installed_relay_setup.rs"]
mod installed_relay_setup;

const MIB: u64 = 1024 * 1024;
fn emit(value: serde_json::Value) {
    println!("{value}");
    std::io::stdout().flush().unwrap();
}
fn hex(value: &[u8]) -> String {
    value.iter().map(|v| format!("{v:02x}")).collect()
}
fn validate(document: &str, digest: [u8; 32]) -> Result<(), OwnerError> {
    validate_version(document, digest, false)
}
fn validate_version(document: &str, digest: [u8; 32], v2: bool) -> Result<(), OwnerError> {
    let value: serde_json::Value =
        serde_json::from_str(document).map_err(|_| OwnerError::InvalidPolicy)?;
    let mut expected: serde_json::Value = serde_json::from_str(include_str!(
        "../../../experiments/configuration-inspection/policy.json"
    ))
    .unwrap();
    let fingerprint = if v2 {
        expected["format"] = serde_json::json!("bigbrotr-product-policy/2");
        expected["settings"]["relay_policy.banned"] = serde_json::json!([]);
        "5916c8d71ed33b8339c00af4595cb5284d8e149a64875ee9c757e0de39e94ed1"
    } else {
        "da36c9bdae45e20a4a99f61e82e3ce1f46f6352ba0bddbf23b2ecf2879d94ea9"
    };
    if value != expected || hex(&digest) != fingerprint {
        return Err(OwnerError::InvalidPolicy);
    }
    Ok(())
}
fn limits() -> RocksNativeLimits {
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
fn grant(b: &ResourceBudget, data: u64, native: u64) -> OwnedPermit {
    b.try_acquire(ResourceRequest::data(data, 1, native).unwrap())
        .unwrap()
}
fn raw_state(path: &Path) -> (Vec<serde_json::Value>, u64) {
    // Independent raw native observation, deliberately outside the product API.
    let db = rocksdb::DB::open_cf_for_read_only(
        &rocksdb::Options::default(),
        path,
        ["default", "facts", "receipts"],
        false,
    )
    .unwrap();
    let mut rows = Vec::new();
    for cf in ["default", "facts", "receipts"] {
        for row in db.iterator_cf(db.cf_handle(cf).unwrap(), rocksdb::IteratorMode::Start) {
            let (key, value) = row.unwrap();
            assert!(rows.len() < 2048 && value.len() < MIB as usize);
            rows.push(serde_json::json!({"cf":cf,"key":hex(&key),"value":hex(&value)}));
        }
    }
    (rows, db.latest_sequence_number())
}
fn dump(path: &Path) {
    let (rows, _) = raw_state(path);
    emit(serde_json::json!({"event":"dump","rows":rows}));
}
fn fixture_document() -> String {
    // The expected fixture digest names compact JSON, not the pretty source.
    serde_json::to_string(
        &serde_json::from_str::<serde_json::Value>(include_str!(
            "../../../experiments/configuration-inspection/policy.json"
        ))
        .unwrap(),
    )
    .unwrap()
}
#[test]
fn setup_preserves_the_independent_frozen_policy_digest() {
    use sha2::{Digest, Sha256};
    let document = fixture_document();
    let digest = Sha256::new()
        .chain_update(b"bigbrotr/product-policy/1\0")
        .chain_update(document.as_bytes())
        .finalize()
        .into();
    validate(&document, digest).unwrap();
}

async fn setup(mut config: Config, target: u32) {
    use bigbrotr::storage::postgres::migration_runner;
    config.user("postgres");
    let (mut admin, driver) = config.connect(NoTls).await.unwrap();
    let driver = tokio::spawn(driver);
    migration_runner::run(&mut admin, target, true, &Cell::new(false))
        .await
        .unwrap();
    admin.batch_execute("GRANT USAGE ON SCHEMA control TO bigbrotr_control; GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA control TO bigbrotr_control; GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA control TO bigbrotr_control").await.unwrap();
    drop(admin);
    driver.await.unwrap().unwrap();
    config
        .user("bigbrotr_control")
        .password(std::fs::read("/secrets/bigbrotr_control").unwrap());
    initialize_fixture(config).await;
    emit(serde_json::json!({"event":"setup", "target":target}));
}

async fn initialize_fixture(config: Config) {
    use bigbrotr::storage::postgres::{
        activation::{ActivationRequest, activate_initial},
        initialization::{InitialConfiguration, initialize},
    };
    use sha2::{Digest, Sha256};
    let (mut client, driver) = config.connect(NoTls).await.unwrap();
    let driver = tokio::spawn(driver);
    let document = fixture_document();
    let dataset = [1; 16].try_into().unwrap();
    let instance = [2; 16].try_into().unwrap();
    initialize(
        &mut client,
        InitialConfiguration {
            dataset,
            instance,
            authority_mode: "managed",
            document: &document,
            document_digest: Sha256::new()
                .chain_update(b"bigbrotr/product-policy/1\0")
                .chain_update(document.as_bytes())
                .finalize()
                .into(),
            source_digest: [3; 32],
        },
        &Cell::new(false),
    )
    .await
    .unwrap();
    activate_initial(
        &mut client,
        ActivationRequest {
            dataset,
            instance,
            request_id: [4; 16],
            expected_revision: 1,
            authority_mode: "managed",
        },
        |d, h| {
            validate(d, h).map_err(|_| {
                bigbrotr::storage::postgres::activation::ActivationError::PolicyInvalid
            })
        },
        &Cell::new(false),
    )
    .await
    .unwrap();
    drop(client);
    driver.await.unwrap().unwrap();
}

async fn run(mut config: Config, mode: &str, path: &Path) {
    // The explicit v2 case retains the independent frozen settings and literal
    // digest; it does not derive expected policy from the product codec.
    let source_mode = mode.starts_with("source-");
    let observation_mode = mode.starts_with("source-observation-");
    let socket_mode = mode.starts_with("source-socket-");
    let dictionary_fault = mode.starts_with("dictionary-fault-");
    let dictionary_intern = mode == "source-socket-dictionary-intern";
    let dictionary_append =
        dictionary_intern || mode == "source-socket-dictionary-append" || dictionary_fault;
    let witness = socket_mode || dictionary_fault || (observation_mode && mode.ends_with("-on"));
    let source_fault = mode.starts_with("source-fault-");
    let direct_config = config.clone();
    let v2 = mode == "write-v2" || source_mode || dictionary_fault || mode.starts_with("lease-");
    let mode = if mode == "write-v2" { "write" } else { mode };
    let (observer, observer_driver) = config.connect(NoTls).await.unwrap();
    let observer_driver = tokio::spawn(observer_driver);
    let row=observer.query_one("SELECT dataset_id,instance_id,(EXTRACT(EPOCH FROM pg_catalog.clock_timestamp())*1000000)::bigint FROM control.datasets",&[]).await.unwrap();
    let dataset = <[u8; 16]>::try_from(row.get::<_, &[u8]>(0))
        .unwrap()
        .try_into()
        .unwrap();
    let instance: bigbrotr_model::DatasetId = <[u8; 16]>::try_from(row.get::<_, &[u8]>(1))
        .unwrap()
        .try_into()
        .unwrap();
    let now: i64 = row.get(2);
    let arrival_root = (mode.starts_with("source-arrival-") || mode.starts_with("source-job-"))
        .then(|| crate::config::fixture_root(dataset, *instance.as_bytes()));
    let validate = |document: &str, digest| match &arrival_root {
        Some(root) => crate::config::validate_fixture_policy(document, digest, root),
        None => validate_version(document, digest, v2),
    };
    if matches!(
        mode,
        "reservation-commit-lost"
            | "outcome-commit-lost"
            | "event-bindings-commit-lost"
            | "event-bindings-checkpoints-commit-lost"
    ) || source_fault
        || dictionary_fault
        || mode.starts_with("source-transport-cleanup-")
        || mode.starts_with("source-transport-race-")
        || mode.starts_with("source-transport-job-")
    {
        // The fault proxy accepts only loopback in the shared PG namespace.
        config.hostaddr("127.0.0.1".parse().unwrap()).port(6543);
    }
    let (client, driver) = config.connect(NoTls).await.unwrap();
    let driver = tokio::spawn(driver);
    let (mut owner, policy) = ControlSession::acquire(
        client,
        ClaimRequest {
            dataset,
            instance,
            authority_mode: "managed".into(),
            role_instance_id: [if mode == "inspect" { 32 } else { 31 }; 16],
            incarnation: [if mode == "inspect" { 42 } else { 41 }; 16]
                .try_into()
                .unwrap(),
            lease_ttl: Duration::from_secs(if mode == "lease-control-expired" {
                2
            } else {
                60
            }),
        },
        validate,
        &Cell::new(false),
    )
    .await
    .unwrap();
    if mode == "inspect" {
        let request = bigbrotr_model::ProducerRequestId {
            producer_slot_id: [66; 16].try_into().unwrap(),
            producer_epoch: bigbrotr_model::ProducerEpoch::new(1).unwrap(),
            request_sequence: bigbrotr_model::RequestSequence::new(1).unwrap(),
        };
        let observed = owner.read_producer_reservation(request).await.unwrap();
        let outcome = owner.read_producer_outcome(request).await.unwrap();
        emit(
            serde_json::json!({"event":"reservation-observed", "control_epoch":owner.identity().epoch.get(),
            "present":observed.is_some(),"digest":observed.map(|r| r.digest.to_string()),
            "input_bytes":observed.map(|r| r.input_bytes), "manifest":observed.map(|r|hex(&r.manifest_sha256)),
            "outcome":outcome.map(|r|serde_json::json!({"kind":r.kind as i16,"policy":r.policy_epoch.get().to_string(),"digest":r.request_digest,"sha256":hex(&r.outcome_sha256),"bytes":r.outcome_bytes}))}),
        );
        owner.release().await.unwrap();
        drop(observer);
        driver.await.unwrap().unwrap();
        observer_driver.await.unwrap().unwrap();
        return;
    }
    let event_mode = mode.starts_with("event-");
    let spec = InitialRegistration {
        dataset_id: dataset,
        instance_id: *instance.as_bytes(),
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
        allocation_domains: if witness {
            [
                (DictionaryKind::Endpoint, 70),
                (DictionaryKind::Observer, 65),
                (DictionaryKind::AccessScope, 71),
                (DictionaryKind::VisibilityScope, 64),
                (DictionaryKind::Method, 72),
                (DictionaryKind::Event, 68),
            ]
            .map(|(kind, domain)| (kind, [domain; 16].try_into().unwrap()))
            .to_vec()
        } else if source_mode {
            vec![
                (DictionaryKind::Observer, [65; 16].try_into().unwrap()),
                (
                    DictionaryKind::VisibilityScope,
                    [64; 16].try_into().unwrap(),
                ),
                (DictionaryKind::Event, [68; 16].try_into().unwrap()),
            ]
        } else if event_mode {
            vec![
                (
                    DictionaryKind::VisibilityScope,
                    [64; 16].try_into().unwrap(),
                ),
                (DictionaryKind::Event, [68; 16].try_into().unwrap()),
                (DictionaryKind::DatasetLineage, [69; 16].try_into().unwrap()),
            ]
        } else {
            vec![(
                DictionaryKind::VisibilityScope,
                [64; 16].try_into().unwrap(),
            )]
        },
        producer_slot_id: [66; 16].try_into().unwrap(),
        outstanding_request_limit: if dictionary_intern {
            5
        } else if dictionary_append {
            4
        } else if matches!(mode, "event-outcomes" | "event-recovery-already")
            || mode.starts_with("event-bindings")
        {
            3
        } else {
            2
        },
        created_at_us: now,
        lease_expires_at_us: now + 60_000_000,
    };
    let mut registration = RegistrationAttempt::new(spec.clone()).unwrap();
    assert_eq!(
        owner.register_ingest(&mut registration).await.unwrap(),
        RegistrationObservation::Matching { current: true }
    );
    let manifest = IngestManifest::new(
        ManifestIdentity {
            dataset_id: dataset,
            topology_generation: TopologyGeneration::new(0),
            owner_resource_id: spec.ingest_resource_id,
            shard_id: 0,
            owner_epoch: OwnerEpoch::new(1).unwrap(),
            process_incarnation: spec.process_incarnation,
            recovery_generation: RecoveryGenerationId::from_bytes([67; 16]),
            policy_epoch: PolicyEpoch::new(if mode == "wrong-manifest" { 2 } else { 1 }),
        },
        AllocationNamespaces::from_entries(&spec.allocation_domains).unwrap(),
    );
    let scope = QualifiedRef {
        dataset_id: dataset,
        dictionary_kind: DictionaryKind::VisibilityScope,
        allocation_domain: [64; 16].try_into().unwrap(),
        ordinal: Ordinal::new(1).unwrap(),
    };
    let mut dictionaries: Vec<_> = ["public", "private"]
        .into_iter()
        .enumerate()
        .map(|(i, class)| {
            DictionaryEntry::new(
                QualifiedRef {
                    ordinal: Ordinal::new(i as u64 + 1).unwrap(),
                    ..scope
                },
                DictionaryDescriptor::Visibility {
                    class: DescriptorLabel::new(class.into()).unwrap(),
                },
            )
            .unwrap()
        })
        .collect();
    if event_mode {
        dictionaries.push(
            DictionaryEntry::new(
                QualifiedRef {
                    dictionary_kind: DictionaryKind::DatasetLineage,
                    allocation_domain: [69; 16].try_into().unwrap(),
                    ordinal: Ordinal::new(1).unwrap(),
                    ..scope
                },
                DictionaryDescriptor::DatasetLineage(bigbrotr_model::DatasetLineageDescriptor {
                    format: DescriptorLabel::new("nostr-jsonl".into()).unwrap(),
                    format_version: 1,
                    source_dataset_id: None,
                    source_artifact_digest: ingest_event::source_digest(mode),
                }),
            )
            .unwrap(),
        );
    }
    if source_mode && !dictionary_append {
        dictionaries.push(
            DictionaryEntry::new(
                QualifiedRef {
                    dictionary_kind: DictionaryKind::Observer,
                    allocation_domain: [65; 16].try_into().unwrap(),
                    ordinal: Ordinal::new(1).unwrap(),
                    ..scope
                },
                DictionaryDescriptor::Observer(bigbrotr_model::ObserverDescriptor {
                    clock: Some(bigbrotr_model::ClockSource::SystemUtc),
                    vantage: Some(DescriptorLabel::new("source-fixture".into()).unwrap()),
                    software: None,
                    method_id: None,
                    public_key: None,
                    route_class: None,
                    software_version: None,
                }),
            )
            .unwrap(),
        );
    }
    if (observation_mode || socket_mode) && !dictionary_append {
        ingest_observation::dictionaries(
            &mut dictionaries,
            scope,
            witness,
            socket_mode || mode == "source-observation-private-on",
            if socket_mode {
                "ws://127.0.0.1:7447/private"
            } else {
                "wss://source.example/events"
            },
        );
    }
    let intent = EvidenceIntent::new(
        1,
        scope,
        dictionaries,
        EvidenceIntentLimits {
            max_entries: if witness { 6 } else { 3 },
            max_encoded_intent_bytes: 8192,
            max_validation_scratch_bytes: 1024,
        },
    )
    .unwrap();
    let budget = ResourceBudget::new(
        ProcessEnvelope::new(
            241 * MIB,
            0,
            // Socket work overlaps retained scope/visibility/observation grants.
            ResourceLimits::new(96 * MIB, if socket_mode { 16 } else { 8 }, 144 * MIB, MIB),
            WaiterLimits::new(1, 1),
        )
        .unwrap(),
    );
    let _inputs = grant(&budget, 24 * MIB, 0);
    let mut store =
        IngestStore::initialize(path, manifest, limits(), grant(&budget, 0, 128 * MIB)).unwrap();
    if mode == "retired" {
        observer.execute("UPDATE control.producer_epochs SET state='closing' WHERE producer_slot_id=$1::bytea AND producer_epoch=1", &[&&spec.producer_slot_id.as_bytes()[..]]).await.unwrap();
    }
    let cancelled = AtomicBool::new(mode == "cancelled");
    let wrong_root = path.parent().expect("absolute lab directory");
    let request = || InitialBatchRequest {
        ingest_root: if mode == "wrong-root" {
            wrong_root
        } else {
            path
        },
        registration: &spec,
        intent: &intent,
        recorded_at: ObservedTime::new(123),
        limits: BootstrapLimits {
            max_mappings: 8,
            max_batch_bytes: MIB as usize,
        },
        policy_ttl: Duration::from_secs(2),
        cancelled: &cancelled,
    };
    emit(
        serde_json::json!({"event":"initialized","dataset":dataset,"manifest":hex(manifest.encode().as_bytes())}),
    );
    if mode.starts_with("event-closing-") || mode.starts_with("event-recovery-") {
        ingest_closing::run(
            config, owner, driver, &observer, store, &spec, &intent, path, &budget, mode, scope,
        )
        .await;
        drop(observer);
        tokio::time::timeout(Duration::from_secs(5), observer_driver)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        return;
    }
    let mut validated = 0;
    let result = owner
        .bootstrap_initial_dictionaries(
            &mut store,
            request(),
            grant(&budget, 8 * MIB, 4 * MIB),
            |document, digest| {
                validate(document, digest)?;
                validated += 1;
                if validated == 3 {
                    if mode == "reserved" {
                        emit(serde_json::json!({"event":"reserved-before-native"}));
                        std::thread::sleep(Duration::from_secs(120));
                    }
                    if mode == "reservation-refused" {
                        return Err(OwnerError::InvalidPolicy);
                    }
                }
                if validated == 2 {
                    if mode == "before" {
                        emit(serde_json::json!({"event":"before-native"}));
                        std::thread::sleep(Duration::from_secs(120));
                    }
                    if mode == "expired" {
                        std::thread::sleep(Duration::from_secs(3));
                    }
                }
                Ok(())
            },
        )
        .await;
    if dictionary_fault {
        drop(result.unwrap());
        ingest_dictionary_faults::run(
            direct_config,
            owner,
            driver,
            &observer,
            store,
            &spec,
            path,
            &budget,
            mode,
            &intent,
        )
        .await;
        drop(observer);
        ingest_closing::join(observer_driver, false).await;
        return;
    } else if socket_mode {
        assert_eq!(
            result.unwrap().receipt().batch().unwrap().records().len(),
            if dictionary_append { 2 } else { 6 }
        );
        let intent = if dictionary_append {
            let (reopened, appended) = ingest_dictionary_append::run(
                &mut owner,
                &observer,
                store,
                &spec,
                path,
                &budget,
                &intent,
                dictionary_intern,
            )
            .await;
            store = reopened;
            appended
        } else {
            intent
        };
        ingest_socket_arrival::run(
            &mut owner, &observer, &mut store, &spec, path, &budget, mode, &intent,
        )
        .await;
        drop(store);
        drop(owner);
        drop(observer);
        ingest_closing::join(driver, false).await;
        ingest_closing::join(observer_driver, false).await;
        return;
    } else if observation_mode {
        let bootstrap = result.unwrap();
        assert_eq!(
            bootstrap.receipt().batch().unwrap().records().len(),
            if witness { 6 } else { 3 }
        );
        drop(bootstrap);
        ingest_observation::run(
            &mut owner, &observer, &mut store, &spec, path, &budget, mode, &intent,
        )
        .await;
        drop(store);
        drop(owner);
        drop(observer);
        ingest_closing::join(driver, false).await;
        ingest_closing::join(observer_driver, false).await;
        return;
    } else if source_mode {
        let bootstrap = result.unwrap();
        assert_eq!(bootstrap.receipt().batch().unwrap().records().len(), 3);
        drop(bootstrap);
        ingest_relay_source::run(
            direct_config,
            owner,
            driver,
            &observer,
            store,
            &spec,
            path,
            &budget,
            mode,
            scope,
        )
        .await;
        drop(observer);
        ingest_closing::join(observer_driver, false).await;
        return;
    } else if mode.starts_with("lease-") {
        drop(result.unwrap());
        store = ingest_lease::run(owner, &observer, store, &spec, path, &budget, mode, scope).await;
    } else if mode.starts_with("visibility") {
        let bootstrap = result.unwrap();
        assert_eq!(bootstrap.receipt().batch().unwrap().records().len(), 2);
        drop(bootstrap);
        store = ingest_visibility::run(owner, &observer, store, &spec, path, &budget, mode, scope)
            .await;
    } else if event_mode {
        let bootstrap = result.unwrap();
        assert_eq!(bootstrap.receipt().batch().unwrap().records().len(), 3);
        drop(bootstrap);
        ingest_event::run(
            owner, &observer, &mut store, &spec, path, &budget, mode, scope,
        )
        .await;
    } else if matches!(mode, "write" | "after") {
        let receipt = result.unwrap();
        assert_eq!(validated, 4);
        let bytes = receipt.receipt().encode_persistent(8192).unwrap();
        let metadata = owner
            .read_producer_reservation(receipt.receipt().request_id())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            metadata.digest,
            intent.request_digest(receipt.receipt().request_id())
        );
        let original_row = observer
            .query_one(
                "SELECT row_to_json(r)::text, xmin::text FROM control.producer_requests r",
                &[],
            )
            .await
            .unwrap();
        let original_row = (
            original_row.get::<_, String>(0),
            original_row.get::<_, String>(1),
        );
        let original_outcome = observer
            .query_one(
                "SELECT row_to_json(r)::text, xmin::text FROM control.producer_outcomes r",
                &[],
            )
            .await
            .unwrap();
        let original_outcome = (
            original_outcome.get::<_, String>(0),
            original_outcome.get::<_, String>(1),
        );
        let copied = owner
            .read_producer_outcome(receipt.receipt().request_id())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            copied.kind,
            bigbrotr::storage::rocks::store::NativeOutcomeKind::NewFacts
        );
        assert_eq!(copied.request_digest, metadata.digest);
        assert_eq!(copied.policy_epoch.get(), 1);
        let first = receipt.receipt().batch().unwrap().first();
        assert_eq!(receipt.receipt().batch().unwrap().records().len(), 2);
        if mode == "after" {
            emit(serde_json::json!({"event":"after-native-before-receipt"}));
            tokio::time::sleep(Duration::from_secs(120)).await;
        }
        drop(receipt);
        drop(store);
        store =
            IngestStore::reopen(path, manifest, limits(), grant(&budget, 0, 128 * MIB)).unwrap();
        let retry = owner
            .bootstrap_initial_dictionaries(
                &mut store,
                InitialBatchRequest {
                    recorded_at: ObservedTime::new(456),
                    ..request()
                },
                grant(&budget, 8 * MIB, 4 * MIB),
                validate,
            )
            .await
            .unwrap();
        assert_eq!(retry.receipt().encode_persistent(8192).unwrap(), bytes);
        let retried_row = observer
            .query_one(
                "SELECT row_to_json(r)::text, xmin::text FROM control.producer_requests r",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            (
                retried_row.get::<_, String>(0),
                retried_row.get::<_, String>(1)
            ),
            original_row
        );
        let retried_outcome = observer
            .query_one(
                "SELECT row_to_json(r)::text, xmin::text FROM control.producer_outcomes r",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            (
                retried_outcome.get::<_, String>(0),
                retried_outcome.get::<_, String>(1)
            ),
            original_outcome
        );
        drop(retry);
        let pending = store
            .read_pending_batch(
                first,
                PendingReadLimits {
                    max_records: 8,
                    max_encoded_bytes: 8192,
                    fact: bigbrotr_model::codec::FactDecodeLimits {
                        max_encoded_bytes: 8192,
                        max_allocation_bytes: 65536,
                        max_tag_nodes: 1,
                        max_string_bytes: 1024,
                    },
                },
                grant(&budget, MIB, 0),
                &cancelled,
            )
            .unwrap()
            .unwrap();
        assert_eq!(pending.records().len(), 2);
        emit(
            serde_json::json!({"event":"receipt","bytes":hex(&bytes),"facts":pending.records().iter().map(|r|hex(r)).collect::<Vec<_>>(),"replay_exact":true}),
        );
        drop(pending);
        owner.release().await.unwrap();
    } else {
        let error = match result {
            Err(e) => e,
            Ok(_) => panic!("negative control accepted"),
        };
        match mode {
            "outcome-commit-lost" => assert_eq!(
                error,
                InitialBatchError::Outcome(
                    bigbrotr::storage::postgres::producer_outcomes::OutcomeError::CommitUnknown
                )
            ),
            "outcome-rollback" | "outcome-refused" => assert_eq!(
                error,
                InitialBatchError::Outcome(
                    bigbrotr::storage::postgres::producer_outcomes::OutcomeError::Backend
                )
            ),
            "reservation-refused" => {
                assert_eq!(error, InitialBatchError::Control(OwnerError::InvalidPolicy))
            }
            "reservation-commit-lost" => assert_eq!(
                error,
                InitialBatchError::Reservation(
                    bigbrotr::storage::postgres::producer_requests::ReservationError::CommitUnknown
                )
            ),
            "reservation-rollback" => assert_eq!(
                error,
                InitialBatchError::Reservation(
                    bigbrotr::storage::postgres::producer_requests::ReservationError::Backend
                )
            ),
            "cancelled" => assert_eq!(
                error,
                InitialBatchError::Native(
                    bigbrotr::storage::rocks::store::BootstrapError::AuthorityUnavailable
                )
            ),
            "expired" => assert_eq!(error, InitialBatchError::Control(OwnerError::Unavailable)),
            "retired" => assert_eq!(
                error,
                InitialBatchError::Registry(
                    bigbrotr::storage::postgres::ingest_registry::RegistryError::InvalidMetadata
                )
            ),
            "wrong-manifest" | "wrong-root" => {
                assert_eq!(error, InitialBatchError::Control(OwnerError::InvalidInput))
            }
            _ => panic!("unknown mode"),
        }
        emit(
            serde_json::json!({"event":"refused","error":format!("{error:?}"),"validated":validated}),
        );
        drop(owner);
    }
    drop(store);
    drop(observer);
    let ended = tokio::time::timeout(Duration::from_secs(5), driver)
        .await
        .unwrap()
        .unwrap();
    if matches!(
        mode,
        "reservation-commit-lost"
            | "outcome-commit-lost"
            | "event-bindings-commit-lost"
            | "event-bindings-checkpoints-commit-lost"
    ) {
        assert!(ended.is_err());
    } else {
        ended.unwrap();
    }
    tokio::time::timeout(Duration::from_secs(5), observer_driver)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}
fn main() {
    let args: Vec<_> = std::env::args().collect();
    if args.len() == 6 && args[1] == "compacted-history-fixture" {
        ingest_compacted_fixture::mutate_history(
            Path::new(&args[2]),
            &args[3],
            &args[4],
            Path::new(&args[5]),
        );
        return;
    }
    if args.len() == 5 && args[1] == "compacted-reader-fixture" {
        ingest_compacted_fixture::mutate(Path::new(&args[2]), &args[3], &args[4]);
        return;
    }
    if args.len() == 3 && args[1] == "dump-state" {
        let (rows, sequence) = raw_state(Path::new(&args[2]));
        emit(serde_json::json!({"event":"dump-state","rows":rows,"sequence":sequence}));
        return;
    }
    if args.len() == 3 && args[1] == "dump" {
        dump(Path::new(&args[2]));
        return;
    }
    if args.len() == 4 && args[1] == "history-corrupt" {
        ingest_initial_history::corrupt(Path::new(&args[2]), &args[3]);
        return;
    }
    if args.len() == 4 && args[1] == "activated-corrupt" {
        ingest_initial_history::corrupt_activated(Path::new(&args[2]), &args[3]);
        return;
    }
    let empty_lock = matches!(
        args[1].as_str(),
        "empty-claim-commit"
            | "empty-claim-rollback"
            | "empty-retirement-first"
            | "empty-control-expiry"
    );
    let successor_gate = matches!(
        args[1].as_str(),
        "successor-preparer-claim"
            | "successor-live-guards"
            | "successor-live-manifest"
            | "successor-live-compacted-terminal-import-gate"
            | "successor-activated-acquisition-expiry"
            | "successor-activated-retirement-expiry"
            | "successor-activated-retirement-compacted-cancel"
            | "successor-activated-retirement-compacted-expiry"
            | "successor-activated-retirement-compacted-gate"
    );
    let transition_proxy = args.len() == 7
        && matches!(
            args[1].as_str(),
            "successor-prepare"
                | "successor-compacted-prepare"
                | "successor-compacted-empty"
                | "successor-empty"
                | "successor-lineage-live"
                | "successor-lineage-live-new"
                | "successor-lineage-retire"
        );
    assert_eq!(
        args.len(),
        if args[1] == "successor-lineage-compacted-fault" {
            9
        } else if args[1] == "successor-archive-attempt-holder" {
            7
        } else if args[1] == "successor-lineage-retire-wait" {
            8
        } else if args[1].starts_with("history-")
            || args[1] == "empty-retirement-first"
            || transition_proxy
        {
            7
        } else if empty_lock
            || successor_gate
            || args[1] == "successor-lineage-history"
            || args[1] == "successor-lineage-compacted-history"
            || args[1] == "successor-compacted-gap-only"
            || args[1] == "successor-original-gap-only"
            || args[1] == "successor-archive-attempt-cap"
            || matches!(
                args[1].as_str(),
                "successor-prepare"
                    | "successor-compacted-prepare"
                    | "successor-compacted-gap"
                    | "successor-compacted-empty"
                    | "successor-compacted-empty-retry"
                    | "successor-empty"
                    | "successor-empty-retry"
                    | "successor-gap"
                    | "successor-lineage-live"
                    | "successor-lineage-live-new"
                    | "successor-lineage-recover"
                    | "successor-lineage-source"
                    | "successor-lineage-abandon"
                    | "successor-lineage-acquisition-claims"
                    | "successor-lineage-retire"
            )
            || matches!(
                args[1].as_str(),
                "successor-commit-before"
                    | "successor-commit-after"
                    | "successor-live-commit-before"
                    | "successor-live-commit-after"
                    | "successor-live-renew-before"
                    | "successor-live-renew-after"
                    | "successor-live-reservation-after"
                    | "successor-live-compacted-progress-wire"
                    | "successor-live-compacted-progress-reservation-after"
                    | "successor-live-compacted-progress-outcome-before"
                    | "successor-live-compacted-progress-outcome-after"
                    | "successor-live-original-progress-wire"
                    | "successor-gap-loss-before"
                    | "successor-gap-loss-after"
                    | "empty-commit-before"
                    | "empty-commit-after"
            )
        {
            6
        } else {
            5
        }
    );
    let mut config = Config::new();
    config
        .host(
            if args[1].starts_with("history-")
                || args[1].starts_with("successor-")
                || args[1].starts_with("empty-")
            {
                "127.0.0.1"
            } else {
                "postgres"
            },
        )
        .dbname(&args[2])
        .user("bigbrotr_control")
        .password(std::fs::read(&args[3]).unwrap())
        .application_name("bigbrotr-ingest-bootstrap-probe")
        .connect_timeout(Duration::from_secs(5));
    // This isolated process is itself the retained blocking native owner worker.
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            if let Some(target) = args[1].strip_prefix("setup-") {
                setup(config, target.parse().unwrap()).await;
            } else if args[1] == "initialize-fixture" {
                initialize_fixture(config).await;
            } else if args[1] == "installed-relay-setup" {
                tokio::time::timeout(Duration::from_secs(10), installed_relay_setup::run(config))
                    .await
                    .unwrap();
            } else if args[1].starts_with("history-") {
                ingest_history::run(
                    config,
                    &args[1],
                    Path::new(&args[4]),
                    args[5].parse().unwrap(),
                    ingest_history::configured_instance(&args[6]),
                )
                .await;
            } else if args[1].starts_with("recovery-process-") {
                ingest_recovery_process::run(config, &args[1], Path::new(&args[4])).await;
            } else if args[1].starts_with("empty-") {
                ingest_initial_empty::run(
                    config,
                    Path::new(&args[4]),
                    &args[1],
                    args.get(if empty_lock { 6 } else { 5 })
                        .map(|port| port.parse().expect("explicit proxy port")),
                    empty_lock.then(|| Path::new(&args[5])),
                )
                .await;
            } else if args[1] == "successor-history" {
                ingest_initial_history::run(config, Path::new(&args[4])).await;
            } else if args[1] == "successor-compacted-prepared-recover" {
                ingest_initial_history::run_compacted(config, Path::new(&args[4])).await;
            } else if args[1] == "successor-original-gap-only" {
                ingest_succession::run_original_gap(
                    config,
                    Path::new(&args[4]),
                    Path::new(&args[5]),
                )
                .await;
            } else if args[1] == "successor-compacted-gap-only" {
                ingest_succession::run_compacted_gap(
                    config,
                    Path::new(&args[4]),
                    Path::new(&args[5]),
                )
                .await;
            } else if args[1] == "successor-lineage-retire-wait" {
                ingest_successor_history::transition::run_retirement_wait(
                    config,
                    Path::new(&args[4]),
                    args[5].parse().expect("explicit metadata byte capacity"),
                    &args[6],
                    Path::new(&args[7]),
                )
                .await;
            } else if matches!(
                args[1].as_str(),
                "successor-prepare"
                    | "successor-compacted-prepare"
                    | "successor-compacted-gap"
                    | "successor-compacted-empty"
                    | "successor-compacted-empty-retry"
                    | "successor-empty"
                    | "successor-empty-retry"
                    | "successor-gap"
                    | "successor-lineage-live"
                    | "successor-lineage-live-new"
                    | "successor-lineage-recover"
                    | "successor-lineage-source"
                    | "successor-lineage-abandon"
                    | "successor-lineage-acquisition-claims"
                    | "successor-lineage-retire"
            ) {
                ingest_successor_history::transition::run(
                    config,
                    Path::new(&args[4]),
                    args[5].parse().expect("explicit metadata byte capacity"),
                    &args[1],
                    transition_proxy.then(|| args[6].parse().expect("explicit Control proxy port")),
                )
                .await;
            } else if matches!(
                args[1].as_str(),
                "successor-archive-attempt-holder" | "successor-archive-attempt-cap"
            ) {
                ingest_successor_history::archive_attempts::run(
                    config,
                    Path::new(&args[4]),
                    args[5].parse().unwrap(),
                    args.get(6).map(Path::new),
                )
                .await;
            } else if args[1] == "successor-lineage-history" {
                ingest_successor_history::run(
                    config,
                    Path::new(&args[4]),
                    args[5].parse().expect("explicit metadata byte capacity"),
                )
                .await;
            } else if args[1] == "successor-lineage-compacted-fault" {
                ingest_successor_history::run_compacted_fault(
                    config,
                    Path::new(&args[4]),
                    args[5].parse().expect("explicit metadata byte capacity"),
                    args[6].parse().expect("explicit Control proxy port"),
                    &args[7],
                    Path::new(&args[8]),
                )
                .await;
            } else if args[1] == "successor-lineage-compacted-history" {
                ingest_successor_history::run_compacted(
                    config,
                    Path::new(&args[4]),
                    args[5].parse().expect("explicit metadata byte capacity"),
                )
                .await;
            } else if matches!(
                args[1].as_str(),
                "successor-gap-recover" | "successor-gap-cancelled"
            ) {
                ingest_initial_history::run_gap(
                    config,
                    Path::new(&args[4]),
                    args[1] == "successor-gap-cancelled",
                )
                .await;
            } else if matches!(
                args[1].as_str(),
                "successor-gap-policy"
                    | "successor-gap-loss-before"
                    | "successor-gap-loss-after"
                    | "successor-prepared-recover"
            ) {
                ingest_initial_history::run_qualification(
                    config,
                    Path::new(&args[4]),
                    &args[1],
                    args.get(5)
                        .map(|port| port.parse().expect("explicit proxy port")),
                )
                .await;
            } else if args[1].starts_with("successor-") {
                ingest_succession::run(
                    config,
                    &args[1],
                    Path::new(&args[4]),
                    args.get(5)
                        .filter(|_| !successor_gate)
                        .map(|port| port.parse().expect("explicit proxy port")),
                    successor_gate.then(|| Path::new(&args[5])),
                )
                .await;
            } else {
                run(config, &args[1], Path::new(&args[4])).await;
            }
        });
}
