//! Actual privileged provisioning fixture. Isolated owned PostgreSQL only.
use bigbrotr::storage::{
    archive::LocalInitialization,
    postgres::catalog_guard::{CatalogIdentity, initialization},
};
use std::{cell::Cell, fs, os::unix::fs::PermissionsExt, path::PathBuf, time::Duration};
use tokio_postgres::{Config, NoTls};

fn main() {
    let args: Vec<_> = std::env::args().collect();
    assert_eq!(args.len(), 4);
    let database = &args[1];
    let suffix = &args[2];
    let nonce: u8 = args[3].parse().unwrap();
    assert!(nonce > 0);
    for name in [database, suffix] {
        assert!(!name.is_empty() && name.bytes().all(|c| c.is_ascii_lowercase() || c == b'_'));
    }
    let root = PathBuf::from(format!("/out/initialize_{database}_{suffix}"));
    fs::create_dir(&root).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    let identity = CatalogIdentity::local(
        [1; 16].try_into().unwrap(),
        [2; 16].try_into().unwrap(),
        &format!("{}/", root.display()),
    )
    .unwrap();
    let custody = LocalInitialization::create(&identity, [nonce; 16]).unwrap();
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
        let mut config = Config::new();
        config.host("127.0.0.1").port(5432).dbname(database).user("postgres")
            .password(fs::read("/secrets/postgres").unwrap()).connect_timeout(Duration::from_secs(5));
        let (mut client, connection) = config.connect(NoTls).await.unwrap();
        let driver = tokio::spawn(custody.retain_catalog_connection(connection).unwrap());
        let commit = Cell::new(false);
        let outcome = tokio::time::timeout(Duration::from_secs(30),
            initialization::reserve(&mut client, &custody, "CatalogManager", &commit)).await.unwrap();
        drop(client);
        driver.await.unwrap().unwrap();
        let report = match outcome {
            Ok(fresh) => {
                let r = fresh.reservation();
                let value = serde_json::json!({"status":"reserved", "database_oid":r.database_oid,
                    "reservation_oid":r.reservation_oid,"bootstrap_login":r.bootstrap_login,
                    "bootstrap_oid":r.bootstrap_oid,"manager_oid":r.manager_oid});
                drop(fresh);
                value
            }
            Err(error) => serde_json::json!({"status":"refused", "reason":format!("{error:?}")}),
        };
        assert!(LocalInitialization::recover(&identity).is_err());
        println!("{}", serde_json::json!({"result":report, "commit_started":commit.get()}));
    });
}
