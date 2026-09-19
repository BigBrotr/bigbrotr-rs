//! Actual installed CLI prerequisite for the signed runtime-manager consumer.
use bigbrotr::storage::{
    archive::LocalAttempt,
    postgres::{
        archive_attempt::ArchiveAttemptReceipt,
        catalog_guard::{self, CatalogIdentity, GuardError},
    },
};
use std::{
    cell::Cell,
    fs::{self, OpenOptions},
    io::Write,
    os::unix::fs::OpenOptionsExt,
    path::Path,
    process::Command,
};

pub async fn delegation_checks(
    home: &Path,
    database: &str,
    custody: &LocalAttempt,
    identity: &CatalogIdentity,
    receipt: &ArchiveAttemptReceipt,
) {
    for privilege in ["connect", "usage"] {
        super::emit(serde_json::json!({"event":format!("{privilege}-ready")}));
        super::barrier(&home.join(format!("{privilege}-revoked")), 15).await;
        let (mut manager, connection) = super::connection(database, "CatalogManager", 5432).await;
        let driver = tokio::spawn(custody.retain_catalog_connection(connection).unwrap());
        let commit = Cell::new(false);
        let result =
            catalog_guard::provision_and_activate(&mut manager, identity, receipt, &commit).await;
        drop(manager);
        driver.await.unwrap().unwrap();
        assert!(
            matches!(result, Err(GuardError::UnsafeRole)),
            "{privilege}: {result:?}"
        );
        assert!(!commit.get());
        super::emit(
            serde_json::json!({"event":format!("{privilege}-refused"),"error":"UnsafeRole","commit_started":false}),
        );
        super::barrier(&home.join(format!("{privilege}-restored")), 15).await;
    }
}

pub fn initialize(home: &Path, database: &str, data_root: &str) {
    let scratch = super::directory(home.join("initialize-scratch"));
    for (name, role) in [("initializer", "postgres"), ("manager", "CatalogManager")] {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(home.join(name))
            .unwrap();
        file.write_all(&fs::read(format!("/secrets/{role}")).unwrap())
            .unwrap();
    }
    let config = home.join("installed.toml");
    fs::write(
        &config,
        format!(
            r#"[config]
schema_version=1
recipe="archive-only"
authority_mode="managed"
[topology]
local_roles=["control","discoverer","validator","monitor","sync","ingest","archive"]
instance_id="{instance}"
dataset_id="{dataset}"
[archive]
catalog_id="{catalog}"
[storage]
archive_root="{data_root}"
scratch_root="{scratch}"
[postgres.catalog]
endpoint="127.0.0.1:5432"
database="{database}"
user="CatalogManager"
password_ref="file:{home}/manager"
"#,
            instance = "02".repeat(16),
            dataset = "01".repeat(16),
            catalog = "5a".repeat(16),
            scratch = scratch.display(),
            home = home.display(),
        ),
    )
    .unwrap();
    // The outer isolated harness bounds and owns this complete fixture process.
    // No DB driver exists yet, so this synchronous prerequisite cannot starve one.
    let output = Command::new("/out/native-tests/bigbrotr")
        .env_clear()
        .args(["archive", "initialize", "--file"])
        .arg(&config)
        .args([
            "--initializer-user",
            "postgres",
            "--initializer-password-ref",
        ])
        .arg(format!("file:{}/initializer", home.display()))
        .args(["--worker-root", "/out/archive-worker"])
        .output()
        .unwrap();
    assert!(output.stdout.len() <= 4096 && output.stderr.len() <= 4096);
    assert!(output.status.success(), "installed initializer failed");
    assert!(output.stderr.is_empty());
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["format"], "bigbrotr-catalog-initialization/1");
    assert_eq!(report["new_installation"], true);
    assert_eq!(report["runtime_authorization"], "not-granted");
    fs::write(home.join("installed-initialization.json"), output.stdout).unwrap();
}
