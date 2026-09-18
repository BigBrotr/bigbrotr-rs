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

const MIB: u64 = 1024 * 1024;
fn emit(value: serde_json::Value) {
    println!("{value}");
    std::io::stdout().flush().unwrap();
}
fn hex(value: &[u8]) -> String {
    value.iter().map(|v| format!("{v:02x}")).collect()
}
fn validate(document: &str, digest: [u8; 32]) -> Result<(), OwnerError> {
    let value: serde_json::Value =
        serde_json::from_str(document).map_err(|_| OwnerError::InvalidPolicy)?;
    let expected: serde_json::Value = serde_json::from_str(include_str!(
        "../../../experiments/configuration-inspection/policy.json"
    ))
    .unwrap();
    if value != expected
        || hex(&digest) != "da36c9bdae45e20a4a99f61e82e3ce1f46f6352ba0bddbf23b2ecf2879d94ea9"
    {
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
fn dump(path: &Path) {
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
    emit(serde_json::json!({"event":"dump","rows":rows}));
}
async fn run(config: Config, mode: &str, path: &Path) {
    let (observer, observer_driver) = config.connect(NoTls).await.unwrap();
    let observer_driver = tokio::spawn(observer_driver);
    let row=observer.query_one("SELECT dataset_id,instance_id,(EXTRACT(EPOCH FROM pg_catalog.clock_timestamp())*1000000)::bigint FROM control.datasets",&[]).await.unwrap();
    let dataset = <[u8; 16]>::try_from(row.get::<_, &[u8]>(0))
        .unwrap()
        .try_into()
        .unwrap();
    let instance = <[u8; 16]>::try_from(row.get::<_, &[u8]>(1))
        .unwrap()
        .try_into()
        .unwrap();
    let now: i64 = row.get(2);
    let (client, driver) = config.connect(NoTls).await.unwrap();
    let driver = tokio::spawn(driver);
    let (mut owner, policy) = ControlSession::acquire(
        client,
        ClaimRequest {
            dataset,
            instance,
            authority_mode: "managed".into(),
            role_instance_id: [31; 16],
            incarnation: [41; 16].try_into().unwrap(),
            lease_ttl: Duration::from_secs(60),
        },
        validate,
        &Cell::new(false),
    )
    .await
    .unwrap();
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
        allocation_domains: vec![(
            DictionaryKind::VisibilityScope,
            [64; 16].try_into().unwrap(),
        )],
        producer_slot_id: [66; 16].try_into().unwrap(),
        outstanding_request_limit: 2,
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
    let intent = EvidenceIntent::new(
        1,
        scope,
        ["public", "private"]
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
            .collect(),
        EvidenceIntentLimits {
            max_entries: 2,
            max_encoded_intent_bytes: 8192,
            max_validation_scratch_bytes: 1024,
        },
    )
    .unwrap();
    let budget = ResourceBudget::new(
        ProcessEnvelope::new(
            193 * MIB,
            0,
            ResourceLimits::new(48 * MIB, 8, 144 * MIB, MIB),
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
    let request = || InitialBatchRequest {
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
    let mut validated = 0;
    let result = owner
        .bootstrap_initial_dictionaries(
            &mut store,
            request(),
            grant(&budget, 8 * MIB, 4 * MIB),
            |document, digest| {
                validate(document, digest)?;
                validated += 1;
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
    if matches!(mode, "write" | "after") {
        let receipt = result.unwrap();
        assert_eq!(validated, 2);
        let bytes = receipt.receipt().encode_persistent(8192).unwrap();
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
            "wrong-manifest" => {
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
    tokio::time::timeout(Duration::from_secs(5), driver)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), observer_driver)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}
fn main() {
    let args: Vec<_> = std::env::args().collect();
    if args.len() == 3 && args[1] == "dump" {
        dump(Path::new(&args[2]));
        return;
    }
    assert_eq!(args.len(), 5);
    let mut config = Config::new();
    config
        .host("postgres")
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
        .block_on(run(config, &args[1], Path::new(&args[4])));
}
