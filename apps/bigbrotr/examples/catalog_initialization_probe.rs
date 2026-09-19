//! Actual privileged provisioning fixture. Isolated owned PostgreSQL only.
use bigbrotr::{
    runtime::{ProcessEnvelope, ResourceBudget, ResourceLimits, WaiterLimits},
    storage::{
        archive::{
            ArtifactIdentity, LocalInitialization, Preparation, PreparationLimits,
            WorkerInstallation,
        },
        postgres::catalog_guard::{CatalogIdentity, initialization},
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
    assert!((4..=6).contains(&args.len()));
    let database = &args[1];
    let suffix = &args[2];
    let nonce: u8 = args[3].parse().unwrap();
    let port: u16 = args.get(4).map(|p| p.parse().unwrap()).unwrap_or(5432);
    let mode = args.get(5).map(String::as_str).unwrap_or("reserve");
    assert!(matches!(
        mode,
        "reserve" | "initialize" | "recover" | "unprivileged"
    ));
    assert!(nonce > 0);
    for name in [database, suffix] {
        assert!(!name.is_empty() && name.bytes().all(|c| c.is_ascii_lowercase() || c == b'_'));
    }
    let root = PathBuf::from(format!("/out/initialize_{database}_{suffix}"));
    if mode != "recover" {
        fs::create_dir(&root).unwrap();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    }
    let identity = CatalogIdentity::local(
        [1; 16].try_into().unwrap(),
        [2; 16].try_into().unwrap(),
        &format!("{}/", root.display()),
    )
    .unwrap();
    let custody = if mode == "recover" {
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
        let outcome = tokio::time::timeout(Duration::from_secs(30),
            initialization::reserve(&mut client, &custody, "CatalogManager", &commit)).await.unwrap();
        drop(client);
        let driver_ok = driver.await.unwrap().is_ok();
        let report = match outcome {
            Ok(fresh) => {
                let r = fresh.reservation();
                let mut value = serde_json::json!({"status":"reserved", "database_oid":r.database_oid,
                    "reservation_oid":r.reservation_oid,"bootstrap_login":r.bootstrap_login,
                    "bootstrap_oid":r.bootstrap_oid,"manager_oid":r.manager_oid,
                    "manager_login":r.manager_login,"initialization_id":r.initialization_id});
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
