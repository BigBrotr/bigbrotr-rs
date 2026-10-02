//! Actual fresh-persistence initialization and captured-cut integration fixture.
use super::*;
use bigbrotr::storage::postgres::catalog_guard::CatalogCapture;

async fn pin_state(mode: &str) -> serde_json::Value {
    let (client, driver) = connect(&format!("bbpub_control_{mode}"), "postgres").await;
    let text: String = client.query_one("SELECT json_build_object('leases',(SELECT json_agg(t ORDER BY lease_id) FROM (SELECT *,xmin::text AS xmin FROM control.snapshot_leases)t),'bindings',(SELECT json_agg(t ORDER BY lease_id) FROM (SELECT *,xmin::text AS xmin FROM control.archive_snapshot_pins)t),'claims',(SELECT json_agg(t ORDER BY claim_id) FROM (SELECT *,xmin::text AS xmin FROM control.retention_claims)t))::text", &[]).await.unwrap().get(0);
    drop(client);
    driver.await.unwrap().unwrap();
    serde_json::from_str(&text).unwrap()
}

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
    assert!(matches!(mode, "normal" | "large" | "history"));
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
        coordinator,
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
        WorkerBudgets::combined(budget),
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
        serde_json::json!({"event":if prefix {"captured-prefix"} else {"captured-cut"}, "observation":captured.observed().observation(),"read":captured.observed().read(),"frontier":{"operation_id":hex(&frontier.operation_id),"revision":frontier.revision,"snapshot_id":frontier.snapshot_id,"request_digest":frontier.request_digest},"pin":"not-granted"}),
    );
    drop(captured);
    capture.finish().await.unwrap();
    if mode == "history" && !prefix {
        // An old frontier must never be silently rebound to the current head.
        let historical =
            CatalogCapture::at_snapshot(manager, coordinator, 3, Duration::from_secs(30))
                .await
                .unwrap();
        assert_eq!(historical.snapshot_id(), 3);
        historical.verify().await.unwrap();
        let request = bigbrotr::storage::postgres::control_owner::ArchivePinRequest {
            lease_id: [0xf2; 16],
            frontier_revision: 1,
            ttl: Duration::from_secs(10),
        };
        let pin = owner
            .register_archive_pin(&historical, &request, &Cell::new(false))
            .await
            .unwrap();
        assert!(pin.created && !pin.released);
        assert_eq!(
            (pin.frontier_revision, pin.snapshot_id, pin.lease_revision),
            (1, 3, 1)
        );
        let registered = pin_state(mode).await;
        let retry = owner
            .register_archive_pin(&historical, &request, &Cell::new(false))
            .await
            .unwrap();
        assert_eq!(
            retry,
            bigbrotr::storage::postgres::control_owner::ArchivePinReceipt {
                created: false,
                ..pin
            }
        );
        assert_eq!(
            owner
                .read_archive_pin(coordinator.identity().catalog(), request.lease_id)
                .await
                .unwrap(),
            Some(retry)
        );
        let retried = pin_state(mode).await;
        assert_eq!(retried, registered);
        let old_cut = preparation(
            installation,
            &directory(home.join("historical-cut-temp")),
            cancel,
        )
        .inspect_cut(
            CutRead {
                dataset_id: dataset(),
                topology_generation: TopologyGeneration::new(0),
                bounds: vec![CutBound {
                    shard_id: 0,
                    owner_epoch: 1,
                    complete_through: 10,
                }],
                receipts: inputs
                    .iter()
                    .filter(|input| input.request().ordinal() <= 3)
                    .map(|input| input.expectation().clone())
                    .collect(),
                snapshot_id: Some(3),
            },
            coordinator,
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
            WorkerBudgets::combined(budget),
            12,
        )
        .unwrap();
        let old_observation = old_cut.observation().clone();
        historical.verify().await.unwrap();
        assert!(matches!(
            historical.accept(old_cut).await,
            Err(bigbrotr::storage::postgres::catalog_guard::GuardError::Conflict)
        ));
        let released = owner
            .release_archive_pin(&pin, &Cell::new(false))
            .await
            .unwrap();
        assert!(released.released && !released.created);
        assert_eq!(released.lease_revision, 2);
        let released_state = pin_state(mode).await;
        let old_retry = owner
            .register_archive_pin(&historical, &request, &Cell::new(false))
            .await
            .unwrap();
        assert_eq!(
            old_retry, released,
            "an acquisition retry cannot resurrect a released pin"
        );
        let released_retry = pin_state(mode).await;
        assert_eq!(released_state, released_retry);
        emit(
            serde_json::json!({"event":"pin-history", "registered":registered,
            "retried":retried, "released":released_state, "released_retry":released_retry}),
        );
        historical.finish().await.unwrap();
        assert!(matches!(
            CatalogCapture::at_snapshot(manager, coordinator, 5, Duration::from_secs(30)).await,
            Err(bigbrotr::storage::postgres::catalog_guard::GuardError::Conflict)
        ));
        emit(
            serde_json::json!({"event":"historical-capture", "snapshot_id":3,
            "latest":4,"missing_snapshot_refused":5,"old_observation":old_observation,
            "historical_publication_refused":true}),
        );
    }
}
