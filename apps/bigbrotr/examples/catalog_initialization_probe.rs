//! Actual privileged provisioning fixture. Isolated owned PostgreSQL only.
use bigbrotr::{
    runtime::{ProcessEnvelope, ResourceBudget, ResourceLimits, WaiterLimits},
    storage::{
        archive::{
            ArtifactIdentity, LocalInitialization, Preparation, PreparationLimits,
            WorkerInstallation,
        },
        postgres::catalog_guard::{self, CatalogIdentity, InstallationRoles, initialization},
    },
};
use std::{
    cell::Cell,
    fs,
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    sync::atomic::AtomicBool,
    time::{Duration, Instant},
};
use tokio_postgres::{Config, NoTls};
use zeroize::Zeroizing;

fn main() {
    let args: Vec<_> = std::env::args().collect();
    assert!((4..=6).contains(&args.len()) || args.len() == 10);
    let database = &args[1];
    let suffix = &args[2];
    let nonce: u8 = args[3].parse().unwrap();
    let port: u16 = args.get(4).map(|p| p.parse().unwrap()).unwrap_or(5432);
    let mode = args.get(5).map(String::as_str).unwrap_or("reserve");
    assert!(matches!(
        mode,
        "reserve"
            | "initialize"
            | "recover"
            | "unprivileged"
            | "observe"
            | "fresh-observe"
            | "retire"
            | "retire-cancel"
            | "install"
            | "drain"
            | "drain-other-database"
    ));
    let expects_identity = matches!(mode, "retire" | "retire-cancel" | "install");
    assert_eq!(expects_identity, args.len() == 10);
    let recovering = matches!(mode, "recover" | "observe") || expects_identity;
    assert!(nonce > 0);
    for name in [database, suffix] {
        assert!(!name.is_empty() && name.bytes().all(|c| c.is_ascii_lowercase() || c == b'_'));
    }
    let root = PathBuf::from(format!("/out/initialize_{database}_{suffix}"));
    if !recovering {
        fs::create_dir(&root).unwrap();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    }
    let identity = CatalogIdentity::local(
        [1; 16].try_into().unwrap(),
        [2; 16].try_into().unwrap(),
        &format!("{}/", root.display()),
    )
    .unwrap();
    let custody = if recovering {
        LocalInitialization::recover(&identity).unwrap()
    } else {
        LocalInitialization::create(&identity, [nonce; 16]).unwrap()
    };
    assert_eq!(custody.initialization_id(), &[nonce; 16]);
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
        let mut config = Config::new();
        let user = if mode=="unprivileged" { "bigbrotr_control" } else { "postgres" };
        config.host("127.0.0.1").port(port).dbname(database).user(user)
            .application_name("bigbrotr-initialization-probe")
            .password(fs::read(format!("/secrets/{user}")).unwrap()).connect_timeout(Duration::from_secs(5));
        let (mut client, connection) = config.connect(NoTls).await.unwrap();
        let driver = tokio::spawn(custody.retain_catalog_connection(connection).unwrap());
        let commit = Cell::new(false);
        if matches!(mode, "observe" | "fresh-observe") || expects_identity {
            let operation = async {
                if expects_identity {
                    let expected = initialization::Reservation {
                        database_oid: args[6].parse().unwrap(),
                        reservation_oid: args[7].parse().unwrap(),
                        initialization_id: [nonce; 16],
                        bootstrap_login: format!("bb_init_{}", format!("{nonce:02x}").repeat(16)),
                        bootstrap_oid: args[8].parse().unwrap(),
                        manager_login: "CatalogManager".into(),
                        manager_oid: args[9].parse().unwrap(),
                    };
                    if mode == "install" {
                        // This fixture calls install only after its native initializer
                        // has joined and a separate exact retirement succeeded. A fresh
                        // server observation additionally checks no submitted SQL remains.
                        let remaining: i64 = client.query_one(
                            "SELECT count(*) FROM pg_stat_activity WHERE usesysid=$1::oid",
                            &[&expected.bootstrap_oid],
                        ).await.unwrap().get(0);
                        assert_eq!(remaining, 0);
                        catalog_guard::install_for_manager(&mut client, &identity, InstallationRoles {
                            bootstrap_login: &expected.bootstrap_login, bootstrap_oid: expected.bootstrap_oid,
                            manager_login: &expected.manager_login, manager_oid: expected.manager_oid,
                        }, &commit).await.map(|changed| serde_json::json!({"status":"installed","changed":changed}))
                    } else {
                        initialization::retire_after_local_custody(&mut client, &custody, &expected, &commit)
                            .await.map(|changed| serde_json::json!({"status":"retired", "changed":changed}))
                    }
                } else {
                    initialization::observe_after_local_custody(&mut client, &custody, "CatalogManager")
                        .await.map(|observed| match observed {
                            None => serde_json::json!({"status":"absent"}),
                            Some(observed) => {
                                let mut value = reservation_value(&observed.reservation, "observed");
                                value["bootstrap_retired"] = observed.bootstrap_retired.into();
                                value
                            }
                        })
                }
            };
            let seconds = if mode == "retire-cancel" { 2 } else { 30 };
            let outcome = tokio::time::timeout(Duration::from_secs(seconds), operation).await;
            let cancelled = outcome.is_err();
            assert!(!cancelled || mode == "retire-cancel");
            let report = match outcome {
                Ok(Ok(value)) => value,
                Ok(Err(error)) => serde_json::json!({"status":"refused", "reason":format!("{error:?}")}),
                Err(_) => serde_json::json!({"status":"cancelled"}),
            };
            drop(client);
            let driver_ok = if cancelled {
                driver.abort();
                let result = tokio::time::timeout(Duration::from_secs(5), driver).await.unwrap();
                assert!(result.is_err_and(|error| error.is_cancelled()));
                false
            } else { driver.await.unwrap().is_ok() };
            assert!(LocalInitialization::recover(&identity).is_err());
            println!("{}", serde_json::json!({"result":report,"commit_started":commit.get(),"driver_ok":driver_ok}));
            return;
        }
        let outcome = tokio::time::timeout(Duration::from_secs(30),
            initialization::reserve(&mut client, &custody, "CatalogManager", &commit)).await.unwrap();
        if matches!(mode, "drain" | "drain-other-database") {
            let fresh = outcome.unwrap();
            let mut value = reservation_value(fresh.reservation(), "reserved");
            let target = if mode == "drain-other-database" { "postgres" } else { database };
            value["residual_backend"] = queued_backend(fresh, target, &client, &custody).await;
            drop(client);
            assert!(driver.await.unwrap().is_ok());
            assert!(LocalInitialization::recover(&identity).is_err());
            println!("{}", serde_json::json!({"result":value, "commit_started":commit.get(), "driver_ok":true}));
            return;
        }
        drop(client);
        let driver_ok = driver.await.unwrap().is_ok();
        let report = match outcome {
            Ok(fresh) => {
                let mut value = reservation_value(fresh.reservation(), "reserved");
                if mode == "initialize" {
                    initialize(fresh, database, suffix, &custody);
                    value["native_initialized"] = true.into();
                } else { drop(fresh); }
                value
            }
            Err(error) => serde_json::json!({"status":"refused", "reason":format!("{error:?}")}),
        };
        assert!(LocalInitialization::recover(&identity).is_err());
        println!("{}", serde_json::json!({"result":report, "commit_started":commit.get(),"driver_ok":driver_ok}));
    });
}

/// Deliberate lab residual-session injection, not a native initialization run.
/// A separately controlled PG connection holds this advisory key. Queue actual
/// bootstrap SQL, independently observe its wait, then destroy/join the selected
/// tokio driver. The CLI must drain the remaining server backend after custody
/// is released, including when that backend connected to another database.
async fn queued_backend(
    fresh: initialization::FreshBootstrap,
    database: &str,
    observer: &tokio_postgres::Client,
    custody: &LocalInitialization,
) -> serde_json::Value {
    let config = fresh.with_login(|login, password| {
        let mut config = Config::new();
        config
            .host("127.0.0.1")
            .port(5432)
            .dbname(database)
            .user(login)
            .password(password)
            .application_name("bigbrotr-residual-bootstrap")
            .connect_timeout(Duration::from_secs(5));
        config
    });
    let (client, connection) = config.connect(NoTls).await.unwrap();
    drop(config);
    let driver = tokio::spawn(custody.retain_catalog_connection(connection).unwrap());
    let pid: i32 = client
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    let query = tokio::spawn(async move {
        client
            .batch_execute("SELECT pg_advisory_lock(1128419913,1)")
            .await
    });
    let observed = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let row = observer.query_opt(
                "SELECT backend_start::text FROM pg_stat_activity WHERE pid=$1 AND application_name='bigbrotr-residual-bootstrap' AND state='active' AND wait_event_type='Lock' AND wait_event='advisory'",
                &[&pid],
            ).await.unwrap();
            if let Some(row) = row { break row.get::<_, String>(0); }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }).await.unwrap();
    query.abort();
    assert!(query.await.unwrap_err().is_cancelled());
    driver.abort();
    assert!(driver.await.unwrap_err().is_cancelled());
    serde_json::json!({"pid":pid,"backend_start":observed,"database":database,
        "queued_sql_observed":true,"actual_driver_destroyed_and_joined":true})
}

fn reservation_value(r: &initialization::Reservation, status: &str) -> serde_json::Value {
    serde_json::json!({"status":status, "database_oid":r.database_oid,
        "reservation_oid":r.reservation_oid,"bootstrap_login":r.bootstrap_login,
        "bootstrap_oid":r.bootstrap_oid,"manager_oid":r.manager_oid,
        "manager_login":r.manager_login,"initialization_id":r.initialization_id})
}

fn initialize(
    fresh: initialization::FreshBootstrap,
    database: &str,
    suffix: &str,
    custody: &LocalInitialization,
) {
    const MIB: u64 = 1024 * 1024;
    let budget = ResourceBudget::new(
        ProcessEnvelope::new(
            513 * MIB,
            0,
            ResourceLimits::new(0, 1, 512 * MIB, MIB),
            WaiterLimits::new(0, 0),
        )
        .unwrap(),
    );
    let root = PathBuf::from("/out/archive-worker");
    let ready: serde_json::Value =
        serde_json::from_slice(&fs::read("/out/native-tests/ready.json").unwrap()).unwrap();
    let installation = WorkerInstallation {
        executable: ArtifactIdentity {
            bytes: fs::metadata(root.join("bigbrotr-archive-worker"))
                .unwrap()
                .len(),
            sha256: ready["worker"].as_str().unwrap().parse().unwrap(),
        },
        root,
    };
    let temp = PathBuf::from(format!("/out/initialize_temp_{database}_{suffix}"));
    fs::create_dir(&temp).unwrap();
    fs::set_permissions(&temp, fs::Permissions::from_mode(0o700)).unwrap();
    let cancel = AtomicBool::new(false);
    fresh.with_login(|login,password| {
        let dsn = Zeroizing::new(format!("host=127.0.0.1 port=5432 dbname={database} user={login} password={password} sslmode=disable connect_timeout=5"));
        Preparation { installation: &installation, temp_root: &temp,
            limits: PreparationLimits { memory_bytes:128*MIB,temp_bytes:64*MIB,threads:1,
                max_records:1,max_encoded_bytes:1,native_reserve_bytes:384*MIB },
            deadline:Instant::now()+Duration::from_secs(30),cancelled:&cancel,
        }.initialize_reserved_catalog(&dsn,custody,&budget).unwrap();
    });
    assert_eq!(budget.snapshot().active_permits(), 0);
}
