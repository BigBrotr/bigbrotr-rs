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
    prefix: bool,
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
            complete_through: if mode == "history" && !prefix { 12 } else { 10 },
        }],
        receipts: inputs.iter().map(|i| i.expectation().clone()).collect(),
        snapshot_id: Some(capture.snapshot_id()),
    };
    let observed = preparation(
        installation,
        &directory(home.join(if prefix {
            "cut-prefix-temp"
        } else {
            "cut-temp"
        })),
        cancel,
    )
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
        if mode == "history" && !prefix {
            [6, 1, 1, 2, 2]
        } else {
            [6, 1, 1, 1, 1]
        }
    );
    captured.verify_barrier().await.unwrap();
    let expected_revision = u64::from(mode == "history" && !prefix);
    let operation_id = if prefix { [0xf0; 16] } else { [0xf1; 16] };
    if expected_revision == 1 {
        let historical = owner
            .read_archive_frontier(coordinator.identity().catalog(), [0xf0; 16])
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (
                historical.revision,
                historical.snapshot_id,
                historical.created
            ),
            (1, 3, false)
        );
        assert_eq!(captured.observed().observation().snapshot_id, 4);
        emit(
            serde_json::json!({"event":"historical-frontier","operation_id":hex(&historical.operation_id),"revision":historical.revision,"snapshot_id":historical.snapshot_id,"request_digest":historical.request_digest,"family_counts":historical.family_counts}),
        );
    }
    let frontier = owner
        .publish_archive_frontier(
            &captured,
            inputs,
            operation_id,
            expected_revision,
            &Cell::new(false),
        )
        .await
        .unwrap();
    assert!(frontier.created);
    assert_eq!(frontier.revision, expected_revision + 1);
    let retry = owner
        .publish_archive_frontier(
            &captured,
            inputs,
            operation_id,
            expected_revision,
            &Cell::new(false),
        )
        .await
        .unwrap();
    assert_eq!(
        retry,
        bigbrotr::storage::postgres::control_owner::ArchiveFrontierReceipt {
            created: false,
            ..frontier
        }
    );
    emit(
        serde_json::json!({"event":if prefix {"captured-prefix"} else {"captured-cut"}, "observation":captured.observed().observation(),"read":captured.observed().read(),"frontier":{"operation_id":hex(&frontier.operation_id),"revision":frontier.revision,"snapshot_id":frontier.snapshot_id,"request_digest":frontier.request_digest},"pin":"not-implemented"}),
    );
    drop(captured);
    capture.finish().await.unwrap();
}
