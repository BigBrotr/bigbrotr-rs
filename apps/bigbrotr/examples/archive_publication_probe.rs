//! Isolated signed Rocks -> real Control/FreshWriter -> supervised PG publisher.
//! No installed source policy, protection, ACK or reclamation is implemented here.
use bigbrotr::{
    runtime::{
        OwnedPermit, ProcessEnvelope, ResourceBudget, ResourceLimits, ResourceRequest, WaiterLimits,
    },
    storage::{
        archive::{
            ArtifactIdentity, CatalogRoute, CatalogTls, Preparation, PreparationLimits,
            PublicationError, WorkerInstallation,
        },
        postgres::{
            activation::{ActivationRequest, activate_initial},
            archive_attempt::ArchiveAttemptRequest,
            catalog_guard::{self, CatalogIdentity},
            connection_settings::PlaintextBoundary,
            control_owner::{ClaimRequest, ControlSession, OwnerError},
            initialization::{InitialConfiguration, initialize},
            migration_runner,
        },
    },
};
use bigbrotr_model::{
    ArchiveBatch, DatasetId, OwnerResourceId, ProcessIncarnation, TopologyGeneration,
};
use sha2::{Digest, Sha256};
use std::{
    cell::Cell,
    fs,
    io::Write,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    sync::atomic::AtomicBool,
    time::{Duration, Instant},
};
use tokio_postgres::{Client, Config, NoTls};
use zeroize::Zeroizing;
#[path = "support/publication_ingest.rs"]
mod ingest;
const MIB: u64 = 1024 * 1024;
fn dataset() -> DatasetId {
    [1; 16].try_into().unwrap()
}
fn emit(value: serde_json::Value) {
    println!("{value}");
    std::io::stdout().flush().unwrap();
}
fn grant(budget: &ResourceBudget, data: u64, native: u64) -> OwnedPermit {
    budget
        .try_acquire(ResourceRequest::data(data, 1, native).unwrap())
        .unwrap()
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|v| format!("{v:02x}")).collect()
}
fn validate(document: &str, digest: [u8; 32]) -> Result<(), OwnerError> {
    let expected = include_str!("../../../experiments/configuration-inspection/policy.json");
    let value: serde_json::Value =
        serde_json::from_str(document).map_err(|_| OwnerError::InvalidPolicy)?;
    let expected_digest: [u8; 32] = Sha256::new()
        .chain_update(b"bigbrotr/product-policy/1\0")
        .chain_update(expected)
        .finalize()
        .into();
    if value != serde_json::from_str::<serde_json::Value>(expected).unwrap()
        || digest != expected_digest
    {
        return Err(OwnerError::InvalidPolicy);
    }
    Ok(())
}
async fn connect(
    database: &str,
    role: &str,
) -> (
    Client,
    tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>,
) {
    let mut config = Config::new();
    config
        .host("127.0.0.1")
        .dbname(database)
        .user(role)
        .password(fs::read(format!("/secrets/{role}")).unwrap())
        .application_name("bigbrotr-signed-publication-probe")
        .connect_timeout(Duration::from_secs(5));
    let (client, connection) = config.connect(NoTls).await.unwrap();
    (client, tokio::spawn(connection))
}
async fn initialize_control(database: &str) {
    let (mut admin, driver) = connect(database, "postgres").await;
    assert_eq!(
        migration_runner::run(&mut admin, 9, true, &Cell::new(false))
            .await
            .unwrap()
            .applied,
        8
    );
    admin.batch_execute("GRANT USAGE ON SCHEMA control TO bigbrotr_control; GRANT SELECT ON ALL TABLES IN SCHEMA control TO bigbrotr_control; GRANT INSERT,UPDATE ON control.datasets,control.principals,control.config_revisions,control.policy_revisions,control.management_audit,control.management_commands,control.role_instances,control.owner_epochs,control.topology_generations,control.source_streams,control.topology_streams,control.allocation_domains,control.producer_slots,control.producer_epochs TO bigbrotr_control; GRANT INSERT ON control.archive_attempts TO bigbrotr_control; GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA control TO bigbrotr_control;").await.unwrap();
    drop(admin);
    driver.await.unwrap().unwrap();
    let (mut application, driver) = connect(database, "bigbrotr_control").await;
    let document = include_str!("../../../experiments/configuration-inspection/policy.json");
    let digest = Sha256::new()
        .chain_update(b"bigbrotr/product-policy/1\0")
        .chain_update(document)
        .finalize()
        .into();
    initialize(
        &mut application,
        InitialConfiguration {
            dataset: dataset(),
            instance: [2; 16].try_into().unwrap(),
            authority_mode: "managed",
            document,
            document_digest: digest,
            source_digest: [3; 32],
        },
        &Cell::new(false),
    )
    .await
    .unwrap();
    activate_initial(
        &mut application,
        ActivationRequest {
            dataset: dataset(),
            instance: [2; 16].try_into().unwrap(),
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
    drop(application);
    driver.await.unwrap().unwrap();
}
fn preparation<'a>(
    installation: &'a WorkerInstallation,
    temp: &'a Path,
    cancel: &'a AtomicBool,
) -> Preparation<'a> {
    Preparation {
        installation,
        temp_root: temp,
        limits: PreparationLimits {
            memory_bytes: 128 * MIB,
            temp_bytes: 64 * MIB,
            threads: 1,
            max_records: 16,
            max_encoded_bytes: MIB as usize,
            native_reserve_bytes: 384 * MIB,
        },
        deadline: Instant::now() + Duration::from_secs(30),
        cancelled: cancel,
    }
}
fn directory(path: PathBuf) -> PathBuf {
    fs::create_dir(&path).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
    path
}
async fn native_state(client: &Client) -> (i64, i64) {
    let row=client.query_one("SELECT (SELECT count(*) FROM public.ducklake_snapshot),(SELECT count(*) FROM public.ducklake_data_file)",&[]).await.unwrap();
    (row.get(0), row.get(1))
}
async fn run(mode: &str, port: u16) {
    let index = match mode {
        "normal" => 0u8,
        "before" => 1,
        "after" => 2,
        _ => panic!("closed case"),
    };
    let database = format!("bbpub_{mode}");
    let control = format!("bbpub_control_{mode}");
    let home = directory(PathBuf::from(format!("/out/signed_{mode}")));
    let data = directory(home.join("kind=7"));
    let data_root = format!("{}/", data.display());
    let cancel = AtomicBool::new(false);
    let budget = ResourceBudget::new(
        ProcessEnvelope::new(
            400 * MIB,
            0,
            ResourceLimits::new(128 * MIB, 16, 256 * MIB, 16 * MIB),
            WaiterLimits::new(0, 0),
        )
        .unwrap(),
    );
    let inputs = grant(&budget, 32 * MIB, 0);
    let worker_budget = ResourceBudget::new(
        ProcessEnvelope::new(
            513 * MIB,
            0,
            ResourceLimits::new(0, 1, 512 * MIB, MIB),
            WaiterLimits::new(0, 0),
        )
        .unwrap(),
    );
    let ready: serde_json::Value =
        serde_json::from_slice(&fs::read("/out/native-tests/ready.json").unwrap()).unwrap();
    let installation = WorkerInstallation {
        root: "/out/archive-worker".into(),
        executable: ArtifactIdentity {
            bytes: fs::metadata("/out/archive-worker/bigbrotr-archive-worker")
                .unwrap()
                .len(),
            sha256: ready["worker"].as_str().unwrap().parse().unwrap(),
        },
    };
    let password = Zeroizing::new(fs::read_to_string("/secrets/BootstrapMixed").unwrap());
    assert!(password.bytes().all(|v| v.is_ascii_hexdigit()));
    let dsn = Zeroizing::new(format!(
        "hostaddr=127.0.0.1 port=5432 dbname={database} user=BootstrapMixed password={} sslmode=disable connect_timeout=5",
        password.as_str()
    ));
    preparation(&installation, &directory(home.join("init-temp")), &cancel)
        .initialize_catalog(&dsn, &data_root, &worker_budget)
        .unwrap();
    let catalog: OwnerResourceId = [90; 16].try_into().unwrap();
    let identity = CatalogIdentity::local(dataset(), catalog, &data_root).unwrap();
    let (mut admin, admin_driver) = connect(&database, "postgres").await;
    assert!(
        catalog_guard::install(&mut admin, &identity, "BootstrapMixed", &Cell::new(false))
            .await
            .unwrap()
    );
    assert_eq!(native_state(&admin).await, (2, 0));
    initialize_control(&control).await;
    let (client, driver) = connect(&control, "bigbrotr_control").await;
    let (mut owner, policy) = ControlSession::acquire(
        client,
        ClaimRequest {
            dataset: dataset(),
            instance: [2; 16].try_into().unwrap(),
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
    let (observer, observer_driver) = connect(&control, "bigbrotr_control").await;
    let path = home.join("rocks");
    let store = ingest::prepare(&mut owner, policy, &observer, &path, &budget, &cancel).await;
    let mut expected_pending = Vec::new();
    for first in [1, 6] {
        let p = ingest::pending(&store, first, &budget, &cancel);
        expected_pending.push(serde_json::json!({"first":first,"last":p.batch().last().sequence.get(),"admission_digest":p.batch().digest(),"archive_digest":ArchiveBatch::new(1,TopologyGeneration::new(0),std::slice::from_ref(p.batch())).unwrap().digest(),"facts":p.batch().records().iter().zip(p.records()).map(|(r,b)|serde_json::json!({"sequence":r.source_record_id.sequence.get(),"digest":r.payload_digest,"preimage_hex":b.iter().map(|v|format!("{v:02x}")).collect::<String>()})).collect::<Vec<_>>()}));
    }
    emit(serde_json::json!({"event":"retained-inputs","batches":expected_pending}));
    let mut outcomes = Vec::new();
    for (ordinal, first) in [(1u8, 6), (2, 1), (3, 6), (4, 6)] {
        if ordinal == 4 && mode != "normal" {
            break;
        }
        owner.refresh(validate).await.unwrap();
        let pending = ingest::pending(&store, first, &budget, &cancel);
        let archive = ArchiveBatch::new(
            1,
            TopologyGeneration::new(0),
            std::slice::from_ref(pending.batch()),
        )
        .unwrap();
        let n = 180 + index * 8 + ordinal;
        let request = ArchiveAttemptRequest::new(
            [n; 16],
            catalog,
            ProcessIncarnation::try_from([80; 16]).unwrap(),
            [n + 1; 16].try_into().unwrap(),
            u64::from(ordinal - 1),
            &archive,
        )
        .unwrap();
        let receipt = owner
            .register_archive_attempt(&request, &Cell::new(false))
            .await
            .unwrap();
        assert!(receipt.created);
        let writer = catalog_guard::provision_and_activate(
            &mut admin,
            &identity,
            &receipt,
            &Cell::new(false),
        )
        .await
        .unwrap();
        let before = native_state(&admin).await;
        let route = CatalogRoute {
            address: "127.0.0.1".parse().unwrap(),
            port: if ordinal == 3 { port } else { 5432 },
            database: &database,
            tls: CatalogTls::LocalPlaintext(PlaintextBoundary::Loopback),
        };
        let result = preparation(
            &installation,
            &directory(home.join(format!("publish-{ordinal}-temp"))),
            &cancel,
        )
        .publish_pending(&pending, writer, &route, &worker_budget);
        let success = ordinal == 2 || (ordinal == 3 && mode == "normal");
        if success {
            assert_eq!(result.unwrap().batch_digest, archive.digest());
        } else {
            assert!(
                matches!(result, Err(PublicationError::OutcomeUnknown)),
                "unexpected publication outcome: {result:?}"
            );
        }
        assert_eq!(worker_budget.snapshot().active_permits(), 0);
        // Sealing and fresh inspection are separate from positive child reap.
        // The fixture never retries an ambiguous third publication or deletes files.
        assert!(
            catalog_guard::seal(&mut admin, &identity, &receipt, &Cell::new(false))
                .await
                .unwrap()
        );
        catalog_guard::inspect(&mut admin, &identity).await.unwrap();
        let after = native_state(&admin).await;
        let expected = match ordinal {
            1 => before,
            2 => (3, 2),
            3 if mode == "before" => (3, 2),
            3 => (4, 8),
            4 => before,
            _ => unreachable!(),
        };
        assert_eq!(after, expected);
        outcomes.push(serde_json::json!({"ordinal":ordinal,"first":first,"success":success,"snapshots":after.0,"files":after.1,"attempt_id":format!("{n:02x}").repeat(16)}));
    }
    drop(store);
    let after = ingest::logical_state(&path);
    emit(
        serde_json::json!({"event":"terminal","mode":mode,"outcomes":outcomes,"rocks_rows":after.0,"rocks_digest":after.1,"worker_active_permits":worker_budget.snapshot().active_permits()}),
    );
    owner.release().await.unwrap();
    drop(observer);
    drop(admin);
    driver.await.unwrap().unwrap();
    observer_driver.await.unwrap().unwrap();
    admin_driver.await.unwrap().unwrap();
    drop(inputs);
    assert_eq!(budget.snapshot().active_permits(), 0);
}
fn main() {
    let args: Vec<_> = std::env::args().collect();
    assert_eq!(args.len(), 3);
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            tokio::time::timeout(
                Duration::from_secs(150),
                run(&args[1], args[2].parse().unwrap()),
            )
            .await
            .unwrap();
        });
}
