//! Actual PG credential/provision/seal and supervised native authentication fixture.
//! Synthetic retained intents; no scientific publication or persistent launch claim.
#[path = "support/catalog.rs"]
mod fixture;
use bigbrotr::{
    runtime::{ProcessEnvelope, ResourceBudget, ResourceLimits, ResourceRequest, WaiterLimits},
    storage::{
        archive::{ArtifactIdentity, Preparation, PreparationLimits, WorkerInstallation},
        postgres::catalog_guard::{self, Activation, CatalogIdentity, GuardError},
    },
};
use fixture::{catalog, connect, dataset, emit, intent, retained};
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
const MIB: u64 = 1024 * 1024;

fn budget() -> ResourceBudget {
    ResourceBudget::new(
        ProcessEnvelope::new(
            577 * MIB,
            0,
            ResourceLimits::new(64 * MIB, 2, 512 * MIB, MIB),
            WaiterLimits::new(0, 0),
        )
        .unwrap(),
    )
}
fn config(database: &str, role: &str, password: &str) -> Config {
    let mut config = Config::new();
    config
        .host("127.0.0.1")
        .dbname(database)
        .user(role)
        .password(password)
        .application_name("bigbrotr-credential-auth-probe")
        .connect_timeout(Duration::from_secs(5));
    config
}
fn native(database: &str, role: &str, password: &str, budget: &ResourceBudget) {
    // Fixture infrastructure and generated hex credentials are trusted. Product
    // deployment callers must supply their already validated route/TLS policy.
    assert!(role.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_'));
    assert!(password.bytes().all(|b| b.is_ascii_hexdigit()));
    let dsn = Zeroizing::new(format!(
        "host=127.0.0.1 port=5432 dbname={database} user={role} password={password} sslmode=disable connect_timeout=5"
    ));
    let bundle = PathBuf::from("/out/archive-worker");
    let info: serde_json::Value =
        serde_json::from_slice(&fs::read("/out/native-tests/ready.json").unwrap()).unwrap();
    let installation = WorkerInstallation {
        executable: ArtifactIdentity {
            bytes: fs::metadata(bundle.join("bigbrotr-archive-worker"))
                .unwrap()
                .len(),
            sha256: info["worker"].as_str().unwrap().parse().unwrap(),
        },
        root: bundle,
    };
    let temp = PathBuf::from(format!("/out/temp_{database}_{role}"));
    fs::create_dir(&temp).unwrap();
    fs::set_permissions(&temp, fs::Permissions::from_mode(0o700)).unwrap();
    let cancel = AtomicBool::new(false);
    Preparation {
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
    }
    .bootstrap_catalog(&dsn, &format!("/out/archive_{database}/"), budget)
    .unwrap();
}
async fn lifecycle(database: &str, identity: &CatalogIdentity) {
    let budget = budget();
    let grant = budget
        .try_acquire(ResourceRequest::data(64 * MIB, 1, 0).unwrap())
        .unwrap();
    let (mut manager, manager_driver) = connect(database, "CatalogManager", 5432).await;
    assert!(
        manager
            .query_one(
                "SELECT NOT rolsuper AND rolcreaterole FROM pg_roles WHERE rolname=current_user",
                &[]
            )
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    manager
        .batch_execute("SET createrole_self_grant='inherit,set'")
        .await
        .unwrap();
    let first = retained(1).await;
    let login = catalog_guard::writer_login(&first);
    let fresh =
        catalog_guard::provision_and_activate(&mut manager, identity, &first, &Cell::new(false))
            .await
            .unwrap();
    assert_eq!(fresh.identity(), identity);
    assert_eq!(fresh.request(), first.request());
    assert_eq!(format!("{fresh:?}"), "FreshWriter([redacted])");
    // Only this one direct client receives lifetime1. Lifetime2 goes to the
    // native child below; passwords never enter fixture output or files.
    let credentials = fresh.with_login(|role, password| config(database, role, password));
    let wrong = config(database, &login, "incorrect-negative-control")
        .connect(NoTls)
        .await
        .err()
        .unwrap();
    assert_eq!(wrong.code().unwrap().code(), "28P01");
    let (writer, writer_driver) = credentials.connect(NoTls).await.unwrap();
    let writer_driver = tokio::spawn(writer_driver);
    writer
        .batch_execute("DELETE FROM public.ducklake_snapshot WHERE false")
        .await
        .unwrap();
    emit(serde_json::json!({"event":"authenticated", "wrong_password_sqlstate":"28P01"}));
    assert!(matches!(
        catalog_guard::provision_and_activate(&mut manager, identity, &first, &Cell::new(false))
            .await,
        Err(GuardError::Conflict)
    ));
    assert_eq!(
        catalog_guard::activate(&mut manager, identity, &first, &Cell::new(false))
            .await
            .unwrap(),
        Activation::Observed {
            current: true,
            enabled: true
        }
    );
    let (admin, admin_driver) = connect(database, "postgres", 5432).await;
    let memberships = admin.query("SELECT p.rolname::text,m.admin_option,m.inherit_option,m.set_option FROM pg_auth_members m JOIN pg_roles p ON p.oid=m.member WHERE m.roleid=$1::name::regrole", &[&login]).await.unwrap();
    assert_eq!(memberships.len(), 1);
    assert_eq!(memberships[0].get::<_, String>(0), "CatalogManager");
    assert!(memberships[0].get::<_, bool>(1));
    assert!(!memberships[0].get::<_, bool>(2));
    assert!(!memberships[0].get::<_, bool>(3));
    for (label, edit, restore) in [
        (
            "inherit",
            format!("GRANT {login} TO \"CatalogManager\" WITH INHERIT TRUE"),
            format!("GRANT {login} TO \"CatalogManager\" WITH INHERIT FALSE"),
        ),
        (
            "set",
            format!("GRANT {login} TO \"CatalogManager\" WITH SET TRUE"),
            format!("GRANT {login} TO \"CatalogManager\" WITH SET FALSE"),
        ),
        (
            "foreign-member",
            format!("GRANT {login} TO \"OtherRole\" WITH ADMIN TRUE, INHERIT FALSE, SET FALSE"),
            format!("REVOKE {login} FROM \"OtherRole\""),
        ),
        (
            "writer-member",
            format!("GRANT \"OtherRole\" TO {login} WITH INHERIT FALSE, SET FALSE"),
            format!("REVOKE \"OtherRole\" FROM {login}"),
        ),
    ] {
        admin.batch_execute(&edit).await.unwrap();
        assert_eq!(
            catalog_guard::inspect(&mut manager, identity).await,
            Err(GuardError::UnsafeRole)
        );
        admin.batch_execute(&restore).await.unwrap();
        catalog_guard::inspect(&mut manager, identity)
            .await
            .unwrap();
        emit(serde_json::json!({"event":"membership-refused", "case":label}));
    }
    assert!(
        catalog_guard::seal(&mut manager, identity, &first, &Cell::new(false))
            .await
            .unwrap()
    );
    assert!(
        !catalog_guard::seal(&mut manager, identity, &first, &Cell::new(false))
            .await
            .unwrap()
    );
    let refused = writer
        .batch_execute("DELETE FROM public.ducklake_snapshot WHERE false")
        .await
        .unwrap_err();
    assert_eq!(refused.code().unwrap().code(), "55000");
    let refused_login = credentials.connect(NoTls).await.err().unwrap();
    assert!(matches!(
        refused_login.code().unwrap().code(),
        "28P01" | "28000"
    ));
    drop(credentials);
    drop(writer);
    writer_driver.await.unwrap().unwrap();
    emit(
        serde_json::json!({"event":"sealed", "old_backend_sqlstate":"55000", "new_login_sqlstate":refused_login.code().unwrap().code()}),
    );
    let second = retained(2).await;
    let fresh =
        catalog_guard::provision_and_activate(&mut manager, identity, &second, &Cell::new(false))
            .await
            .unwrap();
    assert!(
        !catalog_guard::seal(&mut manager, identity, &first, &Cell::new(false))
            .await
            .unwrap()
    );
    fresh.with_login(|role, password| native(database, role, password, &budget));
    assert_eq!(budget.snapshot().active_permits(), 1);
    emit(serde_json::json!({"event":"native-reaped", "active_permits":1}));
    assert!(matches!(
        catalog_guard::provision_and_activate(&mut manager, identity, &second, &Cell::new(false))
            .await,
        Err(GuardError::Conflict)
    ));
    assert!(
        catalog_guard::seal(&mut manager, identity, &second, &Cell::new(false))
            .await
            .unwrap()
    );
    let state = admin.query_one("SELECT o.ordinal::text,NOT o.enabled,(SELECT count(*) FROM bigbrotr_archive.attempts a JOIN pg_authid r ON r.oid=a.login_oid WHERE NOT r.rolcanlogin AND r.rolpassword IS NULL) FROM bigbrotr_archive.owner o", &[]).await.unwrap();
    assert_eq!(state.get::<_, String>(0), "2");
    assert!(state.get::<_, bool>(1));
    assert_eq!(state.get::<_, i64>(2), 2);
    drop(manager);
    manager_driver.await.unwrap();
    drop(admin);
    admin_driver.await.unwrap();
    drop(grant);
    assert_eq!(budget.snapshot().active_permits(), 0);
    emit(serde_json::json!({"event":"lifecycle", "status":"PASS", "sealed_roles":2}));
}
async fn run() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    assert_eq!(args.len(), 4);
    let (mode, database) = (&args[0], &args[1]);
    assert!(
        database.starts_with("bbcat_")
            && database.len() <= 30
            && database
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
    );
    let ordinal: u8 = args[2].parse().unwrap();
    let port: u16 = args[3].parse().unwrap();
    let identity = CatalogIdentity::native_only_local(
        dataset(),
        catalog(),
        &format!("/out/archive_{database}/"),
    )
    .unwrap();
    match mode.as_str() {
        "bootstrap" => {
            native(
                database,
                "BootstrapMixed",
                &fs::read_to_string("/secrets/BootstrapMixed").unwrap(),
                &budget(),
            );
            emit(serde_json::json!({"event":"bootstrap", "status":"PASS"}));
        }
        "lifecycle" => lifecycle(database, &identity).await,
        "install" | "provision" | "seal" => {
            let receipt = if mode == "install" {
                None
            } else {
                Some(intent(ordinal).await)
            };
            let budget = budget();
            let _grant = budget
                .try_acquire(ResourceRequest::data(64 * MIB, 1, 0).unwrap())
                .unwrap();
            let (mut manager, driver) = connect(database, "CatalogManager", port).await;
            let row = manager.query_one("SELECT pg_backend_pid(),backend_start::text FROM pg_stat_activity WHERE pid=pg_backend_pid()", &[]).await.unwrap();
            emit(
                serde_json::json!({"event":"connected", "pid":row.get::<_,i32>(0), "start":row.get::<_,String>(1)}),
            );
            let commit = Cell::new(false);
            let result = match mode.as_str() {
                "install" => {
                    catalog_guard::install(&mut manager, &identity, "BootstrapMixed", &commit)
                        .await
                        .map(|b| if b { "installed" } else { "existing" })
                }
                "provision" => catalog_guard::provision_and_activate(
                    &mut manager,
                    &identity,
                    receipt.as_ref().unwrap(),
                    &commit,
                )
                .await
                .map(|_| "provisioned-discarded"),
                _ => {
                    catalog_guard::seal(&mut manager, &identity, receipt.as_ref().unwrap(), &commit)
                        .await
                        .map(|b| if b { "sealed" } else { "historical" })
                }
            };
            let outcome = match result {
                Ok(v) => v,
                Err(GuardError::Conflict) => "conflict",
                Err(GuardError::CommitUnknown) => "unknown",
                Err(e) => panic!("unexpected guard refusal: {e}"),
            };
            emit(
                serde_json::json!({"event":"outcome", "outcome":outcome, "commit_started":commit.get()}),
            );
            drop(manager);
            driver.await.unwrap();
        }
        _ => panic!("unknown fixture mode"),
    }
}
fn main() {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            tokio::time::timeout(Duration::from_secs(120), run())
                .await
                .expect("finite credential fixture")
        });
}
