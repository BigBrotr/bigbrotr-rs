//! Actual fresh-persistence initialization and captured-cut integration fixture.
use super::*;
use bigbrotr::storage::postgres::catalog_guard::CatalogCapture;

pub async fn initialize(
    owner: &mut ControlSession,
    manager: &mut Client,
    coordinator: &LocalCoordinator,
) {
    let capture = CatalogCapture::open(manager, coordinator, Duration::from_secs(30))
        .await
        .unwrap();
    assert!(
        owner
            .initialize_archive_persistence(&capture, &Cell::new(false))
            .await
            .unwrap()
    );
    assert!(
        !owner
            .initialize_archive_persistence(&capture, &Cell::new(false))
            .await
            .unwrap()
    );
    capture.finish().await.unwrap();
}

#[allow(clippy::too_many_arguments)]
pub async fn inspect(
    mode: &str,
    owner: &mut ControlSession,
    manager: &mut Client,
    coordinator: &LocalCoordinator,
    inputs: &[RetainedInputs],
    installation: &WorkerInstallation,
    home: &Path,
    budget: &ResourceBudget,
    database: &str,
    password: &str,
    cancel: &AtomicBool,
) {
    use bigbrotr_protocol::archive_cut::{CutBound, CutRead};
    assert!(matches!(mode, "normal" | "history"));
    owner.refresh(validate).await.unwrap();
    let capture = CatalogCapture::open(manager, coordinator, Duration::from_secs(120))
        .await
        .unwrap();
    let read = CutRead {
        dataset_id: dataset(),
        topology_generation: TopologyGeneration::new(0),
        bounds: vec![CutBound {
            shard_id: 0,
            owner_epoch: 1,
            complete_through: if mode == "history" { 12 } else { 10 },
        }],
        receipts: inputs.iter().map(|i| i.expectation().clone()).collect(),
        snapshot_id: Some(capture.snapshot_id()),
    };
    let observed = preparation(installation, &directory(home.join("cut-temp")), cancel)
        .inspect_cut(
            read,
            coordinator.identity(),
            &CatalogRoute {
                address: "127.0.0.1".parse().unwrap(),
                port: 5432,
                database,
                tls: CatalogTls::LocalPlaintext(PlaintextBoundary::Loopback),
            },
            CatalogReaderCredentials {
                login: "bigbrotr_receipt_reader",
                password,
            },
            budget,
            12,
        )
        .unwrap();
    let captured = capture.accept(observed).await.unwrap();
    assert_eq!(
        captured.observed().observation().family_counts,
        if mode == "history" {
            [6, 1, 1, 2, 2]
        } else {
            [6, 1, 1, 1, 1]
        }
    );
    captured.verify_barrier().await.unwrap();
    emit(
        serde_json::json!({"event":"captured-cut", "observation":captured.observed().observation(),"read":captured.observed().read(),"frontier":"not-implemented","pin":"not-implemented"}),
    );
    drop(captured);
    capture.finish().await.unwrap();
}
