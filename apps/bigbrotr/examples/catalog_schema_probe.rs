//! Finite supervised schema initialization fixture; dedicated isolated PG only.
use bigbrotr::{
    runtime::{ProcessEnvelope, ResourceBudget, ResourceLimits, WaiterLimits},
    storage::archive::{ArtifactIdentity, Preparation, PreparationLimits, WorkerInstallation},
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
    assert!(matches!(mode.as_str(), "initialize" | "bootstrap"));
    let password = Zeroizing::new(fs::read_to_string("/secrets/BootstrapMixed").unwrap());
    assert!(password.bytes().all(|b| b.is_ascii_hexdigit()));
    let dsn = Zeroizing::new(format!(
        "host=127.0.0.1 port=5432 dbname={database} user=BootstrapMixed password={} sslmode=disable connect_timeout=5",
        password.as_str()
    ));
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
    let data_root = format!("/out/schema_{database}/");
    let result = if mode == "initialize" {
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
    println!(
        "{}",
        serde_json::json!({"success":result.is_ok(),"active_permits":0,"outcome":format!("{result:?}")})
    );
}
