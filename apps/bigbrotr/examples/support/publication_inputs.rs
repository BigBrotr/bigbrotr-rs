//! Fresh-process observation using only retained Control intent and local headers.
//! No Rocks open, reissuance, writer credentials, replay or protection operation.
use super::*;

pub async fn inspect(mode: &str) {
    assert!(matches!(
        mode,
        "normal" | "before" | "after" | "owner_first" | "held_commit" | "held_rollback"
    ));
    let home = PathBuf::from(format!("/out/signed_{mode}"));
    let catalog = [90; 16].try_into().unwrap();
    let identity =
        CatalogIdentity::local(dataset(), catalog, &format!("{}/kind=7/", home.display())).unwrap();
    let coordinator = LocalCoordinator::open(&identity, &home.join("archive-state")).unwrap();
    let (client, connection) =
        connection(&format!("bbpub_control_{mode}"), "bigbrotr_control", 5432).await;
    let driver = tokio::spawn(coordinator.retain_connection(connection).unwrap());
    let mut incarnation = [0; 16];
    let mut role = [0; 16];
    getrandom::fill(&mut incarnation).unwrap();
    getrandom::fill(&mut role).unwrap();
    let (mut control, _) = ControlSession::acquire(
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
    let budget = ResourceBudget::new(
        ProcessEnvelope::new(
            515 * MIB,
            0,
            ResourceLimits::new(2 * MIB, 8, 512 * MIB, MIB),
            WaiterLimits::new(0, 0),
        )
        .unwrap(),
    );
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
    let password = Zeroizing::new(fs::read_to_string("/secrets/bigbrotr_receipt_reader").unwrap());
    let database = format!("bbpub_{mode}");
    let route = CatalogRoute {
        address: "127.0.0.1".parse().unwrap(),
        port: 5432,
        database: &database,
        tls: CatalogTls::LocalPlaintext(PlaintextBoundary::Loopback),
    };
    let cancel = AtomicBool::new(false);
    let mut scan = control.begin_archive_attempt_scan(catalog).await.unwrap();
    let rows = control.read_archive_attempt_page(&mut scan).await.unwrap();
    assert_eq!(
        rows.len(),
        if matches!(mode, "before" | "after") {
            3
        } else {
            4
        }
    );
    for row in rows {
        let inputs = coordinator
            .read_inputs(
                row.request(),
                grant(&budget, RetainedInputs::required_data_bytes(), 0),
                &cancel,
            )
            .unwrap()
            .unwrap();
        let outcome = preparation(
            &installation,
            &directory(home.join(format!("restart-receipt-{}", row.ordinal))),
            &cancel,
        )
        .inspect_receipt(
            inputs.expectation(),
            &identity,
            &route,
            CatalogReaderCredentials {
                login: "bigbrotr_receipt_reader",
                password: &password,
            },
            &budget,
        );
        let outcome = match outcome {
            Ok(ReceiptObservation::Absent) => "absent",
            Ok(ReceiptObservation::Present) => "present",
            Err(ReceiptError::ObservationUnknown) => "conflict",
            value => panic!("unexpected observation: {value:?}"),
        };
        emit(
            serde_json::json!({"event":"inputs-restart","expected":inputs.expectation(),"receipt":outcome}),
        );
    }
    assert!(
        control
            .read_archive_attempt_page(&mut scan)
            .await
            .unwrap()
            .is_empty()
    );
    control.release().await.unwrap();
    driver.await.unwrap().unwrap();
    assert_eq!(budget.snapshot().active_permits(), 0);
}
