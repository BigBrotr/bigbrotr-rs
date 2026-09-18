//! Isolated real-PG catalog recovery fixture, driven by an external fault proxy.
//! Synthetic retained input; never claims scientific publication or worker launch.
#[path = "support/catalog.rs"]
mod fixture;
use bigbrotr::storage::postgres::catalog_guard::{self, Activation, CatalogIdentity, GuardError};
use fixture::{catalog, connect, dataset, emit, retained};
use std::{cell::Cell, time::Duration};

async fn run() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    assert_eq!(args.len(), 4);
    let (mode, database) = (&args[0], &args[1]);
    assert!(matches!(
        mode.as_str(),
        "install" | "activate" | "cancel" | "inspect"
    ));
    assert!(
        database.starts_with("bbcat_")
            && database.len() <= 30
            && database
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b == b'_' || b.is_ascii_digit())
    );
    let ordinal: u8 = args[2].parse().unwrap();
    let port: u16 = args[3].parse().unwrap();
    let receipt = if matches!(mode.as_str(), "activate" | "cancel") {
        Some(retained(ordinal).await)
    } else {
        assert_eq!(ordinal, 0);
        None
    };
    let (mut client, mut driver) = connect(database, "postgres", port).await;
    let backend = client.query_one(
        "SELECT pg_backend_pid(),backend_start::text FROM pg_stat_activity WHERE pid=pg_backend_pid()", &[],
    ).await.unwrap();
    emit(
        serde_json::json!({"event":"connected", "pid":backend.get::<_,i32>(0),
        "start":backend.get::<_,String>(1)}),
    );
    let identity = CatalogIdentity::native_only_local(
        dataset(),
        catalog(),
        &format!("/out/archive_{database}/"),
    )
    .unwrap();
    let commit = Cell::new(false);
    let operation = async {
        match mode.as_str() {
            "install" => catalog_guard::install(&mut client, &identity, "BootstrapMixed", &commit)
                .await
                .map(|created| if created { "installed" } else { "existing" }),
            "inspect" => catalog_guard::inspect(&mut client, &identity)
                .await
                .map(|()| "inspected"),
            _ => {
                catalog_guard::activate(&mut client, &identity, receipt.as_ref().unwrap(), &commit)
                    .await
                    .map(|result| match result {
                        Activation::Activated => "activated",
                        Activation::Observed {
                            current: true,
                            enabled: true,
                        } => "current",
                        Activation::Observed {
                            current: false,
                            enabled: false,
                        } => "historical",
                        Activation::Observed {
                            current: true,
                            enabled: false,
                        } => "sealed",
                        _ => panic!("invalid activation observation"),
                    })
            }
        }
    };
    let outcome = if mode == "cancel" {
        assert!(
            tokio::time::timeout(Duration::from_secs(2), operation)
                .await
                .is_err()
        );
        assert!(commit.get(), "cancellation must occur at the held COMMIT");
        "cancelled"
    } else {
        match operation.await {
            Ok(outcome) => outcome,
            Err(GuardError::CommitUnknown) => "unknown",
            Err(GuardError::Conflict) => "conflict",
            Err(GuardError::Backend) => "backend-refusal",
            Err(error) => panic!("unexpected guard outcome: {error}"),
        }
    };
    emit(serde_json::json!({"event":"outcome", "outcome":outcome,"commit_started":commit.get()}));
    // A dropped operation is not server exclusion. The controller observes the
    // still-held backend before releasing/closing the proxy; no client reuse.
    drop(client);
    if tokio::time::timeout(Duration::from_secs(5), &mut driver)
        .await
        .is_err()
    {
        driver.abort();
        let _ = driver.await;
        emit(serde_json::json!({"event":"joined","aborted":true}));
    } else {
        emit(serde_json::json!({"event":"joined","aborted":false}));
    }
}
fn main() {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            tokio::time::timeout(Duration::from_secs(45), run())
                .await
                .unwrap()
        });
}
