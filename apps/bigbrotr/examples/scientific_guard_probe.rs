//! Isolated real-PG scientific compatibility fixture. No publication claim.
#[allow(dead_code)]
#[path = "support/catalog.rs"]
mod fixture;
use bigbrotr::{
    runtime::{ProcessEnvelope, ResourceBudget, ResourceLimits, ResourceRequest, WaiterLimits},
    storage::postgres::catalog_guard::{self, CatalogIdentity},
};
use std::{cell::Cell, time::Duration};

async fn run() {
    let args: Vec<_> = std::env::args().skip(1).collect();
    assert_eq!(args.len(), 3);
    let (mode, database, expected) = (&args[0], &args[1], &args[2]);
    assert!(
        database.starts_with("bbcat_")
            && database
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b == b'_')
    );
    let budget = ResourceBudget::new(
        ProcessEnvelope::new(
            64 * 1024 * 1024,
            0,
            ResourceLimits::new(64 * 1024 * 1024, 1, 0, 0),
            WaiterLimits::new(0, 0),
        )
        .unwrap(),
    );
    let grant = budget
        .try_acquire(ResourceRequest::data(64 * 1024 * 1024, 1, 0).unwrap())
        .unwrap();
    let identity = CatalogIdentity::local(
        fixture::dataset(),
        fixture::catalog(),
        &format!("/out/schema_{database}/"),
    )
    .unwrap();
    let receipt = if mode == "activate" {
        Some(fixture::intent(1).await)
    } else {
        None
    };
    let (mut client, driver) = fixture::connect(database, "postgres", 5432).await;
    let commit = Cell::new(false);
    let result = match mode.as_str() {
        "install" => catalog_guard::install(&mut client, &identity, "BootstrapMixed", &commit)
            .await
            .map(|v| if v { "installed" } else { "existing" }),
        "inspect" => catalog_guard::inspect(&mut client, &identity)
            .await
            .map(|()| "inspected"),
        "activate" => catalog_guard::provision_and_activate(
            &mut client,
            &identity,
            receipt.as_ref().unwrap(),
            &commit,
        )
        .await
        .map(|_| "provisioned-discarded"),
        _ => panic!("unknown mode"),
    };
    let outcome = match result {
        Ok(v) => v.to_owned(),
        Err(e) => format!("{e:?}"),
    };
    fixture::emit(serde_json::json!({"outcome":outcome,"commit_started":commit.get()}));
    assert_eq!(&outcome, expected);
    drop(client);
    driver.await.unwrap();
    drop(grant);
    assert_eq!(budget.snapshot().active_permits(), 0);
}
fn main() {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            tokio::time::timeout(Duration::from_secs(45), run())
                .await
                .unwrap();
        });
}
