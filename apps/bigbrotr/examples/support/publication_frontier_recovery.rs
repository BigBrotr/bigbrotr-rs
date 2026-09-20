//! Restart/fault fixture consuming existing signed admission and native history.
//! No production hook, new admission, replay, pin, or scientific file mutation.
use super::*;
use bigbrotr::storage::postgres::catalog_guard::CatalogCapture;
use bigbrotr_protocol::archive_cut::{CutBound, CutRead};

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    case: String,
    operation: u8,
    expected_revision: u64,
    control_port: u16,
    lookup: bool,
    wait: bool,
    cancel: bool,
}
fn receipt(
    value: bigbrotr::storage::postgres::control_owner::ArchiveFrontierReceipt,
) -> serde_json::Value {
    serde_json::json!({"operation_id":hex(&value.operation_id),"revision":value.revision,
        "snapshot_id":value.snapshot_id,"request_digest":value.request_digest,
        "family_counts":value.family_counts,"created":value.created})
}
pub async fn run(mode: &str, path: &Path) {
    assert_eq!(mode, "normal");
    assert!(fs::metadata(path).unwrap().len() <= 1024);
    let request: Request = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
    assert!(!request.case.is_empty() && request.case.len() <= 48);
    assert!(
        request
            .case
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    );
    assert_ne!(request.operation, 0);
    let home = PathBuf::from("/out/signed_normal");
    let scratch = directory(home.join(format!("frontier-{}", request.case)));
    let catalog = [90; 16].try_into().unwrap();
    let identity =
        CatalogIdentity::local(dataset(), catalog, "/out/signed_normal/kind=7/").unwrap();
    let coordinator = LocalCoordinator::open(&identity, &home.join("archive-state")).unwrap();
    let (client, control_connection) = connection(
        "bbpub_control_normal",
        "bigbrotr_control",
        request.control_port,
    )
    .await;
    let driver = tokio::spawn(coordinator.retain_connection(control_connection).unwrap());
    let control_pid: i32 = client
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    let control_start: String = client
        .query_one(
            "SELECT backend_start::text FROM pg_stat_activity WHERE pid=pg_backend_pid()",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    let mut incarnation = [0; 16];
    let mut role = [0; 16];
    getrandom::fill(&mut incarnation).unwrap();
    getrandom::fill(&mut role).unwrap();
    let (mut owner, _) = ControlSession::acquire(
        client,
        ClaimRequest {
            dataset: dataset(),
            instance: [2; 16].try_into().unwrap(),
            authority_mode: "managed".into(),
            role_instance_id: role,
            incarnation: incarnation.try_into().unwrap(),
            lease_ttl: Duration::from_secs(60),
        },
        validate,
        &Cell::new(false),
    )
    .await
    .unwrap();
    owner
        .require_archive_coordinator(&coordinator)
        .await
        .unwrap();
    if request.lookup {
        let found = owner
            .read_archive_frontier(catalog, [request.operation; 16])
            .await
            .unwrap();
        owner.release().await.unwrap();
        driver.await.unwrap().unwrap();
        emit(
            serde_json::json!({"event":"frontier-lookup","receipt":found.map(receipt),"drivers_joined":true}),
        );
        return;
    }
    let budget = ResourceBudget::new(
        ProcessEnvelope::new(
            516 * MIB,
            0,
            // Two retained input handles coexist with the native cut request.
            ResourceLimits::new(3 * MIB, 8, 512 * MIB, MIB),
            WaiterLimits::new(0, 0),
        )
        .unwrap(),
    );
    let cancel = AtomicBool::new(false);
    let mut scan = owner.begin_archive_attempt_scan(catalog).await.unwrap();
    let attempts = owner.read_archive_attempt_page(&mut scan).await.unwrap();
    assert_eq!(attempts.len(), 4);
    assert!(scan.is_complete());
    // Frozen signed fixture: ordinal1 lacks dictionaries; ordinal4 conflicts.
    // Select the known complete successful groups, not the implementation result.
    let inputs: Vec<_> = attempts
        .iter()
        .filter(|a| matches!(a.ordinal, 2 | 3))
        .map(|a| {
            coordinator
                .read_inputs(
                    a.request(),
                    grant(&budget, RetainedInputs::required_data_bytes(), 0),
                    &cancel,
                )
                .unwrap()
                .unwrap()
        })
        .collect();
    assert_eq!(inputs.len(), 2);
    let ready: serde_json::Value =
        serde_json::from_slice(&fs::read("/out/native-tests/ready.json").unwrap()).unwrap();
    let installation = WorkerInstallation {
        root: "/out/archive-worker".into(),
        executable: ArtifactIdentity {
            bytes: fs::metadata("/out/archive-worker/bigbrotr-archive-worker")
                .unwrap()
                .len(),
            sha256: ready["worker"].as_str().unwrap().parse().unwrap(),
        },
    };
    let (mut manager, connection) = connection("bbpub_normal", "CatalogManager", 5432).await;
    let manager_driver = tokio::spawn(coordinator.retain_connection(connection).unwrap());
    let manager_pid: i32 = manager
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    let manager_start: String = manager
        .query_one(
            "SELECT backend_start::text FROM pg_stat_activity WHERE pid=pg_backend_pid()",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    let capture = CatalogCapture::open(&mut manager, &coordinator, Duration::from_secs(60))
        .await
        .unwrap();
    let password = Zeroizing::new(fs::read_to_string("/secrets/bigbrotr_receipt_reader").unwrap());
    let observed = preparation(&installation, &scratch, &cancel)
        .inspect_cut(
            CutRead {
                dataset_id: dataset(),
                topology_generation: TopologyGeneration::new(0),
                bounds: vec![CutBound {
                    shard_id: 0,
                    owner_epoch: 1,
                    complete_through: 10,
                }],
                receipts: inputs.iter().map(|i| i.expectation().clone()).collect(),
                snapshot_id: Some(capture.snapshot_id()),
            },
            &coordinator,
            &CatalogRoute {
                address: "127.0.0.1".parse().unwrap(),
                port: 5432,
                database: "bbpub_normal",
                tls: CatalogTls::LocalPlaintext(PlaintextBoundary::Loopback),
            },
            CatalogReaderCredentials {
                login: "bigbrotr_receipt_reader",
                password: &password,
            },
            &budget,
            12,
        )
        .unwrap();
    let captured = capture.accept(observed).await.unwrap();
    emit(
        serde_json::json!({"event":"frontier-ready","control_pid":control_pid,"manager_pid":manager_pid,
            "control_start":control_start,"manager_start":manager_start,"incarnation":hex(&incarnation)}),
    );
    if request.wait {
        barrier(&scratch.join("continue"), 20).await;
    }
    let commit_started = Cell::new(false);
    let started = Instant::now();
    let result = {
        let publish = owner.publish_archive_frontier(
            &captured,
            &inputs,
            [request.operation; 16],
            request.expected_revision,
            &commit_started,
        );
        tokio::pin!(publish);
        tokio::select! {
            value = &mut publish => Some(value),
            () = async {
                if !request.cancel { std::future::pending::<()>().await; }
                barrier(&scratch.join("cancel"),20).await;
            } => None,
        }
    };
    let (outcome, value) = match result {
        Some(Ok(value)) => ("published".to_owned(), Some(receipt(value))),
        Some(Err(error)) => (format!("{error:?}"), None),
        None => ("cancelled".to_owned(), None),
    };
    if outcome == "cancelled" {
        emit(
            serde_json::json!({"event":"frontier-cancelled","commit_started":commit_started.get()}),
        );
    }
    let elapsed_ms = started.elapsed().as_millis();
    drop(captured);
    let capture_closed = capture.finish().await.is_ok();
    drop(manager);
    let manager_closed = manager_driver.await.unwrap().is_ok();
    // Poisoned/cancelled sessions are dropped, never reused or silently reconnected.
    drop(owner);
    let control_closed = driver.await.unwrap().is_ok();
    drop(inputs);
    assert_eq!(budget.snapshot().active_permits(), 0);
    emit(
        serde_json::json!({"event":"frontier-result","outcome":outcome,"receipt":value,
        "commit_started":commit_started.get(),"elapsed_ms":elapsed_ms,"capture_closed":capture_closed,
        "manager_closed":manager_closed,"control_closed":control_closed,"drivers_joined":true,"active_permits":0}),
    );
}
