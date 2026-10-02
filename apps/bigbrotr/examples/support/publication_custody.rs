//! Qualification-only custody schedules. Fixture files contain public identity,
//! never credentials. Parent death is injected externally into the exact PID.
use super::*;
use bigbrotr::storage::{
    archive::LifetimeError, postgres::archive_attempt::ArchiveAttemptReceipt,
    rocks::store::PendingBatch,
};
use bigbrotr_model::AdmissionBatch;

fn attempt_id(mode: &str) -> [u8; 16] {
    let id = match mode {
        "custody_worker" => 240,
        "custody_cancel" => 241,
        "custody_manager" => 242,
        "custody_prelock_live" => 243,
        "custody_prelock_dead" => 244,
        _ => panic!("closed custody fixture"),
    };
    [id; 16]
}

pub fn request(mode: &str, batch: &AdmissionBatch) -> ArchiveAttemptRequest {
    let attempt = attempt_id(mode);
    ArchiveAttemptRequest::new(
        attempt,
        [90; 16].try_into().unwrap(),
        [80; 16].try_into().unwrap(),
        [attempt[0] + 1; 16].try_into().unwrap(),
        0,
        &ArchiveBatch::new(1, TopologyGeneration::new(0), std::slice::from_ref(batch)).unwrap(),
    )
    .unwrap()
}

pub struct Case<'a> {
    pub home: &'a Path,
    pub database: &'a str,
    pub identity: &'a CatalogIdentity,
    pub installation: &'a WorkerInstallation,
    pub worker_budget: &'a ResourceBudget,
}
impl Case<'_> {
    pub async fn run(
        &self,
        mode: &str,
        port: u16,
        receipt: ArchiveAttemptReceipt,
        pending: &PendingBatch,
    ) {
        let root = self.home.join("lifetimes");
        let custody = LocalAttempt::create(&root, &receipt, self.identity).unwrap();
        emit(
            serde_json::json!({"event":"custody-created","parent_pid":std::process::id(),"attempt":hex(receipt.request().attempt_id())}),
        );
        if mode == "custody_worker" {
            let writer = provision(self.database, &custody, self.identity, &receipt, false).await;
            let route = CatalogRoute {
                address: "127.0.0.1".parse().unwrap(),
                port,
                database: self.database,
                tls: CatalogTls::LocalPlaintext(PlaintextBoundary::Loopback),
            };
            let result = preparation(
                self.installation,
                &directory(self.home.join("custody-worker-temp")),
                &AtomicBool::new(false),
            )
            .publish_pending(
                pending,
                writer,
                custody,
                &route,
                WorkerBudgets::combined(self.worker_budget),
            );
            panic!("parent-death fixture was not killed: {result:?}");
        }
        let (mut manager, connection) = connection(self.database, "postgres", port).await;
        let driver = if mode == "custody_cancel" {
            let query = manager.query_one("SELECT 9471::integer", &[]);
            tokio::pin!(query);
            assert!(
                tokio::time::timeout(Duration::from_millis(100), &mut query)
                    .await
                    .is_err(),
                "the returned driver must not execute queued SQL before custody is attached",
            );
            let driver = tokio::spawn(custody.retain_catalog_connection(connection).unwrap());
            let row = tokio::time::timeout(Duration::from_secs(5), &mut query)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(row.get::<_, i32>(0), 9471);
            emit(serde_json::json!({"event":"unpolled-driver-verified","value":9471}));
            driver
        } else {
            tokio::spawn(custody.retain_catalog_connection(connection).unwrap())
        };
        let abort = driver.abort_handle();
        let pid: i32 = manager
            .query_one("SELECT pg_backend_pid()", &[])
            .await
            .unwrap()
            .get(0);
        emit(serde_json::json!({"event":"manager-connected","backend_pid":pid}));
        let committing = Cell::new(false);
        if mode.starts_with("custody_prelock_") {
            assert_eq!(
                catalog_guard::resolve_after_local_custody(
                    &mut manager,
                    self.identity,
                    &receipt,
                    &custody,
                    &committing,
                )
                .await,
                Err(catalog_guard::GuardError::InvalidInput),
            );
            assert!(!committing.get());
            emit(serde_json::json!({"event":"fresh-custody-refused"}));
            if mode == "custody_prelock_live" {
                let writer = catalog_guard::provision_and_activate(
                    &mut manager,
                    self.identity,
                    &receipt,
                    &committing,
                )
                .await
                .unwrap();
                assert!(committing.get());
                drop(writer); // Negative control: never launch this credential.
                drop(manager);
                driver.await.unwrap().unwrap();
            } else {
                let cancel_path = self.home.join("cancel-operation");
                {
                    let operation = catalog_guard::provision_and_activate(
                        &mut manager,
                        self.identity,
                        &receipt,
                        &committing,
                    );
                    tokio::pin!(operation);
                    tokio::select! {
                        _ = barrier(&cancel_path, 15) => {},
                        outcome = &mut operation => panic!("owner hold missing: {}", outcome.is_ok()),
                    }
                }
                assert!(
                    !committing.get(),
                    "owner query precedes all provisioning mutations"
                );
                drop(manager);
                driver.abort();
                assert!(driver.await.unwrap_err().is_cancelled());
            }
            drop(custody);
            emit(
                serde_json::json!({"event":"prelock-driver-terminal","committing":committing.get()}),
            );
            return;
        }
        let cancel_path = self.home.join("cancel-operation");
        {
            let operation = catalog_guard::provision_and_activate(
                &mut manager,
                self.identity,
                &receipt,
                &committing,
            );
            tokio::pin!(operation);
            tokio::select! {
                _ = barrier(&cancel_path, 15) => {},
                outcome = &mut operation => panic!("COMMIT hold missing: {}", outcome.is_ok()),
            }
        }
        assert!(
            committing.get(),
            "must cancel the actual provisioning COMMIT"
        );
        drop(manager);
        drop(driver); // Explicitly detach: the actual driver, not this handle, owns custody.
        drop(custody);
        assert!(matches!(
            LocalAttempt::recover(&root, &receipt, self.identity),
            Err(LifetimeError::Occupied)
        ));
        emit(serde_json::json!({"event":"cancelled-driver-retained","backend_pid":pid}));
        barrier(&self.home.join("destroy-transport"), 15).await;
        assert_eq!(
            mode, "custody_cancel",
            "manager-death fixture must be killed externally"
        );
        abort.abort();
        let until = Instant::now() + Duration::from_secs(2);
        let _recovered = loop {
            match LocalAttempt::recover(&root, &receipt, self.identity) {
                Ok(value) => break value,
                Err(LifetimeError::Occupied) if Instant::now() < until => {
                    tokio::time::sleep(Duration::from_millis(2)).await
                }
                _ => panic!("destroyed transport did not release custody"),
            }
        };
        emit(
            serde_json::json!({"event":"transport-destroyed-custody-recovered","backend_pid":pid}),
        );
        // The proxy still owns the server connection/queued COMMIT. Local
        // reacquisition MUST NOT imply that transaction has resolved.
        barrier(&self.home.join("finish-custody"), 15).await;
    }
}

pub async fn observe(mode: &str, action: &str, catalog_override: Option<&str>) {
    assert!(catalog_override.is_none_or(|db| {
        action == "resolve"
            && db.starts_with("bbmiss_")
            && db
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
    }));
    let home = PathBuf::from(format!("/out/signed_{mode}"));
    let catalog = [90; 16].try_into().unwrap();
    let identity =
        CatalogIdentity::local(dataset(), catalog, &format!("{}/kind=7/", home.display())).unwrap();
    let (client, driver) = connect(&format!("bbpub_control_{mode}"), "bigbrotr_control").await;
    // Each observation is a new real process/Control claim. Retained role
    // instances cannot be reused, even after an explicit release.
    let mut role_instance_id = [0; 16];
    let mut incarnation = [0; 16];
    getrandom::fill(&mut role_instance_id).unwrap();
    getrandom::fill(&mut incarnation).unwrap();
    let (mut owner, _) = ControlSession::acquire(
        client,
        ClaimRequest {
            dataset: dataset(),
            instance: [2; 16].try_into().unwrap(),
            authority_mode: "managed".into(),
            role_instance_id,
            incarnation: incarnation.try_into().unwrap(),
            lease_ttl: Duration::from_secs(15),
        },
        validate,
        &Cell::new(false),
    )
    .await
    .unwrap();
    let receipt = owner
        .read_archive_attempt(catalog, attempt_id(mode))
        .await
        .unwrap()
        .unwrap();
    assert!(!receipt.created && receipt.latest_issued);
    owner.release().await.unwrap();
    driver.await.unwrap().unwrap();
    match LocalAttempt::recover(&home.join("lifetimes"), &receipt, &identity) {
        Err(LifetimeError::Occupied) if action == "observe" => {
            emit(serde_json::json!({"event":"custody-observed","state":"occupied"}))
        }
        Ok(custody) => {
            emit(serde_json::json!({"event":"custody-observed","state":"recovered"}));
            let rocks = ingest::logical_state(&home.join("rocks"));
            emit(serde_json::json!({"event":"retained-rocks","rows":rocks.0,"digest":rocks.1}));
            if action == "seal" {
                let (mut manager, connection) =
                    connection(&format!("bbpub_{mode}"), "postgres", 5432).await;
                let driver = tokio::spawn(custody.retain_catalog_connection(connection).unwrap());
                let pid: i32 = manager
                    .query_one("SELECT pg_backend_pid()", &[])
                    .await
                    .unwrap()
                    .get(0);
                emit(serde_json::json!({"event":"seal-begin","backend_pid":pid}));
                let result =
                    catalog_guard::seal(&mut manager, &identity, &receipt, &Cell::new(false)).await;
                emit(serde_json::json!({"event":"seal-result","disabled":result.unwrap()}));
                drop(manager);
                driver.await.unwrap().unwrap();
            }
            if action == "resolve" {
                let database = format!("bbpub_{mode}");
                let (mut manager, connection) =
                    connection(catalog_override.unwrap_or(&database), "postgres", 5432).await;
                let driver = tokio::spawn(custody.retain_catalog_connection(connection).unwrap());
                let committing = Cell::new(false);
                let pid: i32 = manager
                    .query_one("SELECT pg_backend_pid()", &[])
                    .await
                    .unwrap()
                    .get(0);
                emit(serde_json::json!({"event":"resolve-begin","backend_pid":pid}));
                let result = catalog_guard::resolve_after_local_custody(
                    &mut manager,
                    &identity,
                    &receipt,
                    &custody,
                    &committing,
                )
                .await;
                let state = match result {
                    Ok(catalog_guard::Resolution::NotImported) => "not-imported",
                    Ok(catalog_guard::Resolution::Sealed {
                        disabled_current: true,
                    }) => "sealed-changed",
                    Ok(catalog_guard::Resolution::Sealed {
                        disabled_current: false,
                    }) => "sealed-unchanged",
                    Err(catalog_guard::GuardError::Conflict) => "conflict",
                    Err(catalog_guard::GuardError::InvalidInput) => "invalid-input",
                    Err(catalog_guard::GuardError::IncompatibleLayout) => "incompatible-layout",
                    Err(error) => panic!("unexpected resolution error: {error:?}"),
                };
                emit(
                    serde_json::json!({"event":"resolve-result","state":state,"commit_started":committing.get()}),
                );
                drop(manager);
                driver.await.unwrap().unwrap();
            }
        }
        outcome => panic!("unexpected custody observation: {:?}", outcome.err()),
    }
}
