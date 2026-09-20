//! Isolated signed Rocks -> real Control/FreshWriter -> supervised PG publisher.
//! No installed source policy, protection, ACK or reclamation is implemented here.
use bigbrotr::{
    runtime::{
        OwnedPermit, ProcessEnvelope, ResourceBudget, ResourceLimits, ResourceRequest, WaiterLimits,
    },
    storage::{
        archive::{
            ArtifactIdentity, CatalogReaderCredentials, CatalogRoute, CatalogTls, LocalAttempt,
            LocalCoordinator, Preparation, PreparationLimits, PublicationError, ReceiptError,
            ReceiptObservation, RetainedInputs, WorkerInstallation,
        },
        postgres::{
            activation::{ActivationRequest, activate_initial},
            archive_attempt::ArchiveAttemptRequest,
            catalog_guard::{self, CatalogIdentity},
            connection_settings::{ConnectionSettings, PlaintextBoundary, TlsSettings},
            control_owner::{ClaimRequest, ControlSession, OwnerError},
            initialization::{InitialConfiguration, initialize},
            migration_runner, transport,
        },
    },
};
use bigbrotr_model::{
    ArchiveBatch, DatasetId, OwnerResourceId, ProcessIncarnation, TopologyGeneration,
};
use bigbrotr_protocol::archive_receipt::ReceiptExpectation;
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
use tokio_postgres::Client;
use zeroize::Zeroizing;
#[path = "support/publication_custody.rs"]
mod custody;
#[path = "support/publication_ingest.rs"]
mod ingest;
#[path = "support/publication_installation.rs"]
mod installed_catalog;
#[path = "support/publication_inputs.rs"]
mod publication_inputs;
#[path = "support/publication_protocol.rs"]
mod publication_protocol;
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
    let expected = include_str!("../../../experiments/archive-engine/receipt-policy.json");
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
    let (client, connection) = connection(database, role, 5432).await;
    (client, tokio::spawn(connection))
}
async fn connection(database: &str, role: &str, port: u16) -> (Client, transport::Driver) {
    // The shared product preparer requires a private credential descriptor. The
    // lab's PG container also reads /secrets, so copy only this fixture credential
    // to an exclusive mode0600 file, then unlink it after preparation (before connect).
    let mut secret = tempfile::NamedTempFile::new_in("/out").unwrap();
    secret
        .write_all(&fs::read(format!("/secrets/{role}")).unwrap())
        .unwrap();
    let reference = format!("file:{}", secret.path().display());
    let prepared = ConnectionSettings {
        endpoint: &format!("127.0.0.1:{port}"),
        database,
        user: role,
        password: bigbrotr::secrets::SecretFile::parse(&reference).unwrap(),
        connect_timeout: Duration::from_secs(5),
        tls: TlsSettings::LocalPlaintext {
            boundary: PlaintextBoundary::Loopback,
        },
    }
    .prepare()
    .unwrap();
    drop(secret);
    transport::connect(prepared).await.unwrap()
}
async fn provision(
    database: &str,
    custody: &LocalAttempt,
    identity: &CatalogIdentity,
    receipt: &bigbrotr::storage::postgres::archive_attempt::ArchiveAttemptReceipt,
    report_activation: bool,
) -> catalog_guard::FreshWriter {
    provision_as(
        "postgres",
        database,
        custody,
        identity,
        receipt,
        report_activation,
    )
    .await
}
async fn provision_as(
    login: &str,
    database: &str,
    custody: &LocalAttempt,
    identity: &CatalogIdentity,
    receipt: &bigbrotr::storage::postgres::archive_attempt::ArchiveAttemptReceipt,
    report_activation: bool,
) -> catalog_guard::FreshWriter {
    let (mut manager, connection) = connection(database, login, 5432).await;
    // The dedicated transport itself holds custody before any provisioning SQL.
    let driver = tokio::spawn(custody.retain_catalog_connection(connection).unwrap());
    if report_activation {
        let pid: i32 = manager
            .query_one("SELECT pg_backend_pid()", &[])
            .await
            .unwrap()
            .get(0);
        emit(serde_json::json!({"event":"activation-begin","backend_pid":pid}));
    }
    let writer =
        catalog_guard::provision_and_activate(&mut manager, identity, receipt, &Cell::new(false))
            .await
            .unwrap();
    drop(manager);
    driver.await.unwrap().unwrap();
    writer
}
async fn initialize_control(database: &str, installed: bool) {
    let (mut admin, driver) = connect(database, "postgres").await;
    assert_eq!(
        migration_runner::run(
            &mut admin,
            if installed { 14 } else { 9 },
            true,
            &Cell::new(false)
        )
        .await
        .unwrap()
        .applied,
        if installed { 13 } else { 8 }
    );
    admin.batch_execute("GRANT USAGE ON SCHEMA control TO bigbrotr_control; GRANT SELECT ON ALL TABLES IN SCHEMA control TO bigbrotr_control; GRANT INSERT,UPDATE ON control.datasets,control.principals,control.config_revisions,control.policy_revisions,control.management_audit,control.management_commands,control.role_instances,control.owner_epochs,control.topology_generations,control.source_streams,control.topology_streams,control.allocation_domains,control.producer_slots,control.producer_epochs TO bigbrotr_control; GRANT INSERT ON control.archive_attempts TO bigbrotr_control; GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA control TO bigbrotr_control;").await.unwrap();
    if installed {
        admin
            .batch_execute("GRANT INSERT ON control.archive_coordinators, control.producer_requests, control.producer_outcomes TO bigbrotr_control; GRANT UPDATE ON control.jobs TO bigbrotr_control")
            .await
            .unwrap();
    }
    drop(admin);
    driver.await.unwrap().unwrap();
    let (mut application, driver) = connect(database, "bigbrotr_control").await;
    let document = include_str!("../../../experiments/archive-engine/receipt-policy.json");
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
async fn barrier(path: &Path, seconds: u64) {
    let end = Instant::now() + Duration::from_secs(seconds);
    while !path.try_exists().unwrap() {
        assert!(Instant::now() < end, "fixture barrier deadline");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
async fn native_state(client: &Client) -> (i64, i64) {
    let row=client.query_one("SELECT (SELECT count(*) FROM public.ducklake_snapshot),(SELECT count(*) FROM public.ducklake_data_file)",&[]).await.unwrap();
    (row.get(0), row.get(1))
}
async fn run(mode: &str, port: u16, installed: bool, event_read: bool) {
    assert!(!installed || !mode.starts_with("custody_"));
    let manager_login = if installed {
        "CatalogManager"
    } else {
        "postgres"
    };
    let index = match mode {
        "normal" => 0u8,
        "large" => 7,
        "before" => 1,
        "after" => 2,
        "owner_first" => 3,
        "held_commit" => 4,
        "held_rollback" => 5,
        "custody_worker"
        | "custody_cancel"
        | "custody_manager"
        | "custody_prelock_live"
        | "custody_prelock_dead" => 6,
        _ => panic!("closed case"),
    };
    let racing = matches!(mode, "owner_first" | "held_commit" | "held_rollback");
    let database = format!("bbpub_{mode}");
    let control = format!("bbpub_control_{mode}");
    let home = directory(PathBuf::from(format!("/out/signed_{mode}")));
    let data = directory(home.join("kind=7"));
    let custody_root = directory(home.join("lifetimes"));
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
    if installed {
        installed_catalog::initialize(&home, &database, &data_root);
    } else {
        let password = Zeroizing::new(fs::read_to_string("/secrets/BootstrapMixed").unwrap());
        assert!(password.bytes().all(|v| v.is_ascii_hexdigit()));
        let dsn = Zeroizing::new(format!(
            "hostaddr=127.0.0.1 port=5432 dbname={database} user=BootstrapMixed password={} sslmode=disable connect_timeout=5",
            password.as_str()
        ));
        preparation(&installation, &directory(home.join("init-temp")), &cancel)
            .initialize_catalog(&dsn, &data_root, &worker_budget)
            .unwrap();
    }
    let catalog: OwnerResourceId = [90; 16].try_into().unwrap();
    let identity = CatalogIdentity::local(dataset(), catalog, &data_root).unwrap();
    let coordinator = installed.then(|| {
        LocalCoordinator::initialize(&identity, &directory(home.join("archive-state"))).unwrap()
    });
    let (mut admin, admin_driver) = connect(&database, "postgres").await;
    if !installed {
        assert!(
            catalog_guard::install(&mut admin, &identity, "BootstrapMixed", &Cell::new(false))
                .await
                .unwrap()
        );
    }
    assert_eq!(native_state(&admin).await, (2, 0));
    // Qualification-only reader role is independent of every writer lifetime.
    // No catalog mutations, schema creation, TEMP or privileged guard inspection.
    admin.batch_execute(&format!("REVOKE TEMP ON DATABASE {database} FROM PUBLIC; GRANT CONNECT ON DATABASE {database} TO bigbrotr_receipt_reader; GRANT USAGE ON SCHEMA public,bigbrotr_archive TO bigbrotr_receipt_reader; GRANT SELECT ON ALL TABLES IN SCHEMA public TO bigbrotr_receipt_reader; GRANT SELECT ON bigbrotr_archive.installation,bigbrotr_archive.scientific_schema TO bigbrotr_receipt_reader;")).await.unwrap();
    let reader_password =
        Zeroizing::new(fs::read_to_string("/secrets/bigbrotr_receipt_reader").unwrap());
    // Privileged infrastructure setup ends here. Runtime catalog operations use
    // only the separately authenticated configured manager from this point.
    drop(admin);
    admin_driver.await.unwrap().unwrap();
    let (mut admin, catalog_connection) = connection(&database, manager_login, 5432).await;
    let admin_driver = if let Some(coordinator) = &coordinator {
        tokio::spawn(coordinator.retain_connection(catalog_connection).unwrap())
    } else {
        tokio::spawn(catalog_connection)
    };
    if installed {
        let row = admin.query_one("SELECT current_user::text,session_user::text,rolsuper,rolcreaterole,rolinherit FROM pg_roles WHERE rolname=current_user", &[]).await.unwrap();
        assert_eq!(row.get::<_, String>(0), "CatalogManager");
        assert_eq!(row.get::<_, String>(1), "CatalogManager");
        assert!(!row.get::<_, bool>(2) && row.get::<_, bool>(3) && !row.get::<_, bool>(4));
        fs::write(home.join("runtime-manager.json"), serde_json::to_vec(&serde_json::json!({"current_user":row.get::<_, String>(0),"session_user":row.get::<_, String>(1),"superuser":false,"createrole":true,"inherit":false})).unwrap()).unwrap();
    }
    initialize_control(&control, installed).await;
    let (client, connection) = connection(&control, "bigbrotr_control", 5432).await;
    let driver = if let Some(coordinator) = &coordinator {
        tokio::spawn(coordinator.retain_connection(connection).unwrap())
    } else {
        tokio::spawn(connection)
    };
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
    if let Some(coordinator) = &coordinator {
        assert!(
            owner
                .initialize_archive_coordinator(coordinator, &Cell::new(false))
                .await
                .unwrap()
        );
        assert!(
            !owner
                .initialize_archive_coordinator(coordinator, &Cell::new(false))
                .await
                .unwrap()
        );
        owner
            .require_archive_coordinator(coordinator)
            .await
            .unwrap();
    }
    let path = home.join("rocks");
    let store = ingest::prepare(
        &mut owner,
        policy,
        &observer,
        &path,
        &budget,
        &cancel,
        mode == "large",
    )
    .await;
    let mut expected_pending = Vec::new();
    for first in [1, 6] {
        let p = ingest::pending(&store, first, &budget, &cancel);
        expected_pending.push(serde_json::json!({"first":first,"last":p.batch().last().sequence.get(),"admission_digest":p.batch().digest(),"archive_digest":ArchiveBatch::new(1,TopologyGeneration::new(0),std::slice::from_ref(p.batch())).unwrap().digest(),"facts":p.batch().records().iter().zip(p.records()).map(|(r,b)|serde_json::json!({"sequence":r.source_record_id.sequence.get(),"digest":r.payload_digest,"preimage_hex":b.iter().map(|v|format!("{v:02x}")).collect::<String>()})).collect::<Vec<_>>()}));
    }
    emit(serde_json::json!({"event":"retained-inputs","batches":expected_pending}));
    if mode.starts_with("custody_") {
        let pending = ingest::pending(&store, 1, &budget, &cancel);
        let request = custody::request(mode, pending.batch());
        let receipt = owner
            .register_archive_attempt(&request, &Cell::new(false))
            .await
            .unwrap();
        // The observation process can acquire a new Control owner immediately;
        // this release does not release archive journal or catalog custody.
        owner.release().await.unwrap();
        custody::Case {
            home: &home,
            database: &database,
            identity: &identity,
            installation: &installation,
            worker_budget: &worker_budget,
        }
        .run(mode, port, receipt, &pending)
        .await;
        drop(store);
        let after = ingest::logical_state(&path);
        emit(
            serde_json::json!({"event":"custody-terminal","rocks_rows":after.0,"rocks_digest":after.1}),
        );
        return;
    }
    let mut outcomes = Vec::new();
    let mut successor: Option<(
        bigbrotr::storage::postgres::archive_attempt::ArchiveAttemptReceipt,
        catalog_guard::FreshWriter,
        LocalAttempt,
    )> = None;
    for (ordinal, first) in [(1u8, 6), (2, 1), (3, 6), (4, 6)] {
        if ordinal == 4 && matches!(mode, "before" | "after") {
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
        let expected_digest = archive.digest();
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
        let retained = coordinator.as_ref().map(|coordinator| {
            let retained = coordinator
                .retain_inputs(
                    &request,
                    &archive,
                    grant(&budget, RetainedInputs::required_data_bytes(), 0),
                    &cancel,
                )
                .unwrap();
            retained.verify_replay(&archive).unwrap();
            CatalogRoute {
                address: "127.0.0.1".parse().unwrap(),
                port: 5432,
                database: &database,
                tls: CatalogTls::LocalPlaintext(PlaintextBoundary::Loopback),
            }
            .validate_receipt(
                retained.expectation(),
                &identity,
                CatalogReaderCredentials {
                    login: "bigbrotr_receipt_reader",
                    password: &reader_password,
                },
            )
            .unwrap();
            retained
        });
        let (receipt, writer, custody) = if let Some((receipt, writer, custody)) = successor.take()
        {
            assert_eq!(ordinal, 4);
            assert_eq!(receipt.request(), &request);
            (receipt, writer, custody)
        } else {
            let receipt = if let Some(inputs) = &retained {
                owner
                    .register_retained_archive_attempt(inputs, &Cell::new(false))
                    .await
            } else {
                owner
                    .register_archive_attempt(&request, &Cell::new(false))
                    .await
            }
            .unwrap();
            let custody = LocalAttempt::create(&custody_root, &receipt, &identity).unwrap();
            if installed && !event_read && mode == "normal" && ordinal == 1 {
                installed_catalog::delegation_checks(
                    &home, &database, &custody, &identity, &receipt,
                )
                .await;
            }
            let writer = provision_as(
                manager_login,
                &database,
                &custody,
                &identity,
                &receipt,
                false,
            )
            .await;
            (receipt, writer, custody)
        };
        assert!(receipt.created);
        if let Some(retained) = &retained {
            assert_eq!(retained.request(), receipt.request());
        }
        let expected_receipt = coordinator.is_none().then(|| {
            ReceiptExpectation::from_archive(&archive, *request.attempt_id(), receipt.ordinal)
                .unwrap()
        });
        let before = native_state(&admin).await;
        let route = CatalogRoute {
            address: "127.0.0.1".parse().unwrap(),
            port: if ordinal == 3 { port } else { 5432 },
            database: &database,
            tls: CatalogTls::LocalPlaintext(PlaintextBoundary::Loopback),
        };
        let temp = directory(home.join(format!("publish-{ordinal}-temp")));
        let result = if ordinal == 3 && racing {
            let next = ArchiveAttemptRequest::new(
                [n + 1; 16],
                catalog,
                [80; 16].try_into().unwrap(),
                [n + 2; 16].try_into().unwrap(),
                3,
                &archive,
            )
            .unwrap();
            let next_inputs = coordinator.as_ref().map(|coordinator| {
                let retained = coordinator
                    .retain_inputs(
                        &next,
                        &archive,
                        grant(&budget, RetainedInputs::required_data_bytes(), 0),
                        &cancel,
                    )
                    .unwrap();
                CatalogRoute {
                    address: "127.0.0.1".parse().unwrap(),
                    port: 5432,
                    database: &database,
                    tls: CatalogTls::LocalPlaintext(PlaintextBoundary::Loopback),
                }
                .validate_receipt(
                    retained.expectation(),
                    &identity,
                    CatalogReaderCredentials {
                        login: "bigbrotr_receipt_reader",
                        password: &reader_password,
                    },
                )
                .unwrap();
                retained
            });
            let next_receipt = if let Some(inputs) = &next_inputs {
                owner
                    .register_retained_archive_attempt(inputs, &Cell::new(false))
                    .await
            } else {
                owner
                    .register_archive_attempt(&next, &Cell::new(false))
                    .await
            }
            .unwrap();
            assert!(next_receipt.created);
            if let Some(retained) = &next_inputs {
                assert_eq!(retained.request(), next_receipt.request());
            }
            drop(next_inputs);
            let copied_installation = WorkerInstallation {
                root: installation.root.clone(),
                executable: installation.executable,
            };
            let copied_budget = worker_budget.clone();
            let copied_database = database.clone();
            // Keep the actual spawning thread alive through positive child reap.
            // Control's connection driver remains runnable during native work.
            let child =
                tokio::task::spawn_blocking(move || {
                    let route = CatalogRoute {
                        address: "127.0.0.1".parse().unwrap(),
                        port,
                        database: &copied_database,
                        tls: CatalogTls::LocalPlaintext(PlaintextBoundary::Loopback),
                    };
                    preparation(&copied_installation, &temp, &AtomicBool::new(false))
                        .publish_pending(&pending, writer, custody, &route, &copied_budget)
                });
            barrier(&home.join("activate"), 15).await;
            let next_custody =
                LocalAttempt::create(&custody_root, &next_receipt, &identity).unwrap();
            let next_writer = provision_as(
                manager_login,
                &database,
                &next_custody,
                &identity,
                &next_receipt,
                true,
            )
            .await;
            successor = Some((next_receipt, next_writer, next_custody));
            emit(serde_json::json!({"event":"activation-complete","ordinal":4}));
            child.await.unwrap()
        } else {
            preparation(&installation, &temp, &cancel).publish_pending(
                &pending,
                writer,
                custody,
                &route,
                &worker_budget,
            )
        };
        let success = ordinal == 2
            || (ordinal == 3 && matches!(mode, "normal" | "large" | "held_commit"))
            || (ordinal == 4 && matches!(mode, "owner_first" | "held_rollback"));
        if success {
            assert_eq!(result.unwrap().batch_digest, expected_digest);
        } else {
            assert!(
                matches!(result, Err(PublicationError::OutcomeUnknown)),
                "unexpected publication outcome: {result:?}"
            );
        }
        assert_eq!(worker_budget.snapshot().active_permits(), 0);
        // Sealing and fresh inspection are separate from positive child reap.
        // A historical seal must preserve the already active successor. The
        // harness independently reads the receipt before permitting its launch.
        assert_eq!(
            catalog_guard::seal(&mut admin, &identity, &receipt, &Cell::new(false))
                .await
                .unwrap(),
            !(ordinal == 3 && racing)
        );
        let _recovered_custody = LocalAttempt::recover(&custody_root, &receipt, &identity).unwrap();
        catalog_guard::inspect(&mut admin, &identity).await.unwrap();
        let reader_route = CatalogRoute {
            address: "127.0.0.1".parse().unwrap(),
            port: 5432,
            database: &database,
            tls: CatalogTls::LocalPlaintext(PlaintextBoundary::Loopback),
        };
        // Drop creation state: fresh file decoding supplies the expectation
        // even after pending moved into and left the actual native worker.
        drop(retained);
        let recovered_inputs = coordinator.as_ref().map(|coordinator| {
            coordinator
                .read_inputs(
                    receipt.request(),
                    grant(&budget, RetainedInputs::required_data_bytes(), 0),
                    &cancel,
                )
                .unwrap()
                .unwrap()
        });
        let wanted = recovered_inputs
            .as_ref()
            .map(RetainedInputs::expectation)
            .or(expected_receipt.as_ref())
            .unwrap();
        if installed {
            emit(serde_json::json!({"event":"inputs-reopened","expected":wanted}));
        }
        let observed = preparation(
            &installation,
            &directory(home.join(format!("receipt-{ordinal}-temp"))),
            &cancel,
        )
        .inspect_receipt(
            wanted,
            &identity,
            &reader_route,
            CatalogReaderCredentials {
                login: "bigbrotr_receipt_reader",
                password: &reader_password,
            },
            &worker_budget,
        );
        let receipt_state = if ordinal == 4 && !success {
            assert_eq!(observed, Err(ReceiptError::ObservationUnknown));
            "conflict"
        } else if success || ordinal == 3 && mode == "after" {
            assert_eq!(observed, Ok(ReceiptObservation::Present));
            "present"
        } else {
            assert_eq!(observed, Ok(ReceiptObservation::Absent));
            "absent"
        };
        assert_eq!(worker_budget.snapshot().active_permits(), 0);
        let after = native_state(&admin).await;
        let expected = match ordinal {
            1 => before,
            2 => (3, 2),
            3 if matches!(mode, "before" | "owner_first" | "held_rollback") => (3, 2),
            3 => (4, 8),
            4 if success => (4, 8),
            4 => before,
            _ => unreachable!(),
        };
        assert_eq!(after, expected);
        outcomes.push(serde_json::json!({"ordinal":ordinal,"first":first,"success":success,"snapshots":after.0,"files":after.1,"attempt_id":format!("{n:02x}").repeat(16),"receipt":receipt_state}));
        if ordinal == 3 && racing {
            emit(serde_json::json!({"event":"old-excluded","worker_active_permits":0}));
            barrier(&home.join("readback-complete"), 60).await;
        }
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
    assert!(
        args.len() == 3
            || args.len() == 4
                && (args[2] == "resolve"
                    || matches!(args[3].as_str(), "--installed" | "--event-read")
                    || args[1] == "protocol")
    );
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            if args[1] == "protocol" && args[2] == "check" {
                tokio::time::timeout(Duration::from_secs(120), publication_protocol::run())
                    .await
                    .unwrap();
                return;
            }
            if args[1] == "protocol" {
                tokio::time::timeout(
                    Duration::from_secs(90),
                    publication_protocol::commit_loss(&args[2], args[3].parse().unwrap()),
                )
                .await
                .unwrap();
                return;
            }
            if args[2] == "inputs" {
                tokio::time::timeout(
                    Duration::from_secs(90),
                    publication_inputs::inspect(&args[1]),
                )
                .await
                .unwrap();
                return;
            }
            if matches!(args[2].as_str(), "observe" | "seal" | "resolve") {
                tokio::time::timeout(
                    Duration::from_secs(15),
                    custody::observe(&args[1], &args[2], args.get(3).map(String::as_str)),
                )
                .await
                .unwrap();
                return;
            }
            tokio::time::timeout(
                Duration::from_secs(150),
                run(
                    &args[1],
                    args[2].parse().unwrap(),
                    args.get(3)
                        .is_some_and(|s| matches!(s.as_str(), "--installed" | "--event-read")),
                    args.get(3).is_some_and(|s| s == "--event-read"),
                ),
            )
            .await
            .unwrap();
        });
}
