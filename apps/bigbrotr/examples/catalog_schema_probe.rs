//! Finite supervised schema initialization fixture; dedicated isolated PG only.
use bigbrotr::{
    runtime::{ProcessEnvelope, ResourceBudget, ResourceLimits, WaiterLimits},
    storage::{
        archive::{
            ArtifactIdentity, LifetimeError, LocalInitialization, Preparation, PreparationError,
            PreparationLimits, WorkerInstallation,
        },
        postgres::catalog_guard::CatalogIdentity,
    },
};
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    sync::atomic::AtomicBool,
    time::{Duration, Instant},
};
use zeroize::Zeroizing;

fn main() {
    let args: Vec<_> = std::env::args().collect();
    assert_eq!(args.len(), 4);
    let database = &args[1];
    let mode = &args[2];
    let expect_success = args[3] == "success";
    assert!(
        database
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b == b'_')
    );
    assert!(matches!(
        mode.as_str(),
        "initialize" | "bootstrap" | "reserved" | "driver-custody"
    ));
    let data_root = format!("/out/schema_{database}/");
    let identity = CatalogIdentity::local(
        [1; 16].try_into().unwrap(),
        [2; 16].try_into().unwrap(),
        &data_root,
    )
    .unwrap();
    let custody = matches!(mode.as_str(), "reserved" | "driver-custody")
        .then(|| LocalInitialization::create(&identity, [3; 16]).unwrap());
    let password = Zeroizing::new(fs::read_to_string("/secrets/BootstrapMixed").unwrap());
    assert!(password.bytes().all(|b| b.is_ascii_hexdigit()));
    let dsn = Zeroizing::new(format!(
        "host=127.0.0.1 port=5432 dbname={database} user=BootstrapMixed password={} sslmode=disable connect_timeout=5",
        password.as_str()
    ));
    if mode == "driver-custody" {
        verify_driver(custody.unwrap(), &identity, &dsn);
        return;
    }
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
    let info: serde_json::Value =
        serde_json::from_slice(&fs::read("/out/native-tests/ready.json").unwrap()).unwrap();
    let installation = WorkerInstallation {
        executable: ArtifactIdentity {
            bytes: fs::metadata(root.join("bigbrotr-archive-worker"))
                .unwrap()
                .len(),
            sha256: info["worker"].as_str().unwrap().parse().unwrap(),
        },
        root,
    };
    let temp = PathBuf::from(format!(
        "/out/schema_temp_{}_{}",
        database,
        std::process::id()
    ));
    fs::create_dir(&temp).unwrap();
    fs::set_permissions(&temp, fs::Permissions::from_mode(0o700)).unwrap();
    let cancel = AtomicBool::new(false);
    let preparation = Preparation {
        installation: &installation,
        temp_root: &temp,
        limits: PreparationLimits {
            memory_bytes: 128 * MIB,
            temp_bytes: 64 * MIB,
            threads: 1,
            max_records: 1,
            max_encoded_bytes: 1,
            native_reserve_bytes: 384 * MIB,
        },
        deadline: Instant::now() + Duration::from_secs(30),
        cancelled: &cancel,
    };
    let result = if mode == "reserved" {
        preparation.initialize_reserved_catalog(&dsn, custody.as_ref().unwrap(), &budget)
    } else if mode == "initialize" {
        preparation.initialize_catalog(&dsn, &data_root, &budget)
    } else {
        preparation.bootstrap_catalog(&dsn, &data_root, &budget)
    };
    assert_eq!(
        result.is_ok(),
        expect_success,
        "unexpected native result: {result:?}"
    );
    assert_eq!(budget.snapshot().active_permits(), 0);
    if let Some(custody) = custody {
        assert_eq!(
            LocalInitialization::recover(&identity).err(),
            Some(LifetimeError::Occupied)
        );
        drop(custody);
        let recovered = LocalInitialization::recover(&identity).unwrap();
        assert_eq!(recovered.initialization_id(), &[3; 16]);
        let retry = temp.with_extension("retry");
        fs::create_dir(&retry).unwrap();
        fs::set_permissions(&retry, fs::Permissions::from_mode(0o700)).unwrap();
        let retry_result = Preparation {
            installation: &installation,
            temp_root: &retry,
            limits: PreparationLimits {
                memory_bytes: 128 * MIB,
                temp_bytes: 64 * MIB,
                threads: 1,
                max_records: 1,
                max_encoded_bytes: 1,
                native_reserve_bytes: 384 * MIB,
            },
            deadline: Instant::now() + Duration::from_secs(30),
            cancelled: &cancel,
        }
        .initialize_reserved_catalog(&dsn, &recovered, &budget);
        assert_eq!(
            retry_result,
            Err(PreparationError::Custody(LifetimeError::Identity))
        );
        assert_eq!(budget.snapshot().active_permits(), 0);
    }
    println!(
        "{}",
        serde_json::json!({"success":result.is_ok(),"active_permits":0,"outcome":format!("{result:?}")})
    );
}

fn verify_driver(custody: LocalInitialization, identity: &CatalogIdentity, dsn: &str) {
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
        let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls).await.unwrap();
        let held = custody.retain_catalog_connection(connection).unwrap();
        drop(custody);
        assert_eq!(LocalInitialization::recover(identity).err(), Some(LifetimeError::Occupied));
        let driver = tokio::spawn(held);
        let value: i32 = client.query_one("SELECT 42::integer", &[]).await.unwrap().get(0);
        assert_eq!(value, 42);
        assert_eq!(LocalInitialization::recover(identity).err(), Some(LifetimeError::Occupied));
        drop(client);
        driver.await.unwrap().unwrap();
        let recovered = LocalInitialization::recover(identity).unwrap();
        assert_eq!(recovered.initialization_id(), &[3; 16]);
        println!("{}", serde_json::json!({"status":"PASS", "unpolled":"occupied", "live":"occupied", "query":value, "joined":"reacquired"}));
    });
}
