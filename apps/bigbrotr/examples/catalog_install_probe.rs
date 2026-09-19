//! Actual finite install/inspection calls in an isolated PostgreSQL laboratory.
#[allow(dead_code)]
#[path = "support/catalog.rs"]
mod fixture;
use bigbrotr::{
    runtime::{ProcessEnvelope, ResourceBudget, ResourceLimits, ResourceRequest, WaiterLimits},
    storage::postgres::catalog_guard::{self, CatalogIdentity, InstallationRoles},
};
use serde::Deserialize;
use std::{cell::Cell, fs, time::Duration};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    database: String,
    mode: String,
    expected: String,
    bootstrap_oid: u32,
    manager_oid: u32,
    #[serde(default)]
    port: Option<u16>,
}

async fn run(input: Input) {
    assert!(
        input.database.starts_with("bbcat_")
            && input
                .database
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b == b'_')
    );
    let identity = CatalogIdentity::local(
        fixture::dataset(),
        fixture::catalog(),
        &format!("/out/schema_{}/", input.database),
    )
    .unwrap();
    let budget = ResourceBudget::new(
        ProcessEnvelope::new(
            64 << 20,
            0,
            ResourceLimits::new(64 << 20, 1, 0, 0),
            WaiterLimits::new(0, 0),
        )
        .unwrap(),
    );
    let grant = budget
        .try_acquire(ResourceRequest::data(64 << 20, 1, 0).unwrap())
        .unwrap();
    let role = if matches!(input.mode.as_str(), "inspect-manager" | "install-manager") {
        "CatalogManager"
    } else {
        "postgres"
    };
    let (mut client, driver) =
        fixture::connect(&input.database, role, input.port.unwrap_or(5432)).await;
    let superuser: bool = client
        .query_one(
            "SELECT rolsuper FROM pg_roles WHERE rolname=current_user",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    let commit = Cell::new(false);
    let result = match input.mode.as_str() {
        "install" | "install-manager" => catalog_guard::install_for_manager(
            &mut client,
            &identity,
            InstallationRoles {
                bootstrap_login: "BootstrapMixed",
                bootstrap_oid: input.bootstrap_oid,
                manager_login: "CatalogManager",
                manager_oid: input.manager_oid,
            },
            &commit,
        )
        .await
        .map(|v| if v { "installed" } else { "existing" }),
        "inspect-manager" => catalog_guard::inspect(&mut client, &identity)
            .await
            .map(|()| "inspected"),
        _ => panic!("unknown fixture mode"),
    };
    let outcome = result
        .map(str::to_owned)
        .unwrap_or_else(|e| format!("{e:?}"));
    drop(client);
    driver.await.unwrap();
    drop(grant);
    assert_eq!(budget.snapshot().active_permits(), 0);
    fixture::emit(
        serde_json::json!({"outcome":outcome,"commit_started":commit.get(),
        "session_superuser":superuser,"active_permits":budget.snapshot().active_permits()}),
    );
    if input.expected == "installed-or-existing" {
        assert!(matches!(outcome.as_str(), "installed" | "existing"));
    } else {
        assert_eq!(outcome, input.expected);
    }
}

fn main() {
    let args: Vec<_> = std::env::args_os().collect();
    assert_eq!(args.len(), 2);
    let input: Input = serde_json::from_slice(&fs::read(&args[1]).unwrap()).unwrap();
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            tokio::time::timeout(Duration::from_secs(40), run(input))
                .await
                .unwrap();
        });
}
