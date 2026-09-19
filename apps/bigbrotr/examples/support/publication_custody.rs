//! Qualification-only custody schedules. Fixture files contain public identity,
//! never credentials. Parent death is injected externally into the exact PID.
use super::*;
use bigbrotr::storage::{
    archive::LifetimeError, postgres::archive_attempt::ArchiveAttemptReceipt,
    rocks::store::PendingBatch,
};
use bigbrotr_model::{AdmissionBatch, FactRef};

pub fn request(mode: &str, batch: &AdmissionBatch) -> ArchiveAttemptRequest {
    let id = match mode {
        "custody_worker" => 240,
        "custody_cancel" => 241,
        "custody_manager" => 242,
        _ => panic!("closed custody fixture"),
    };
    ArchiveAttemptRequest::new(
        [id; 16],
        [90; 16].try_into().unwrap(),
        [80; 16].try_into().unwrap(),
        [id + 1; 16].try_into().unwrap(),
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
        fs::write(
            self.home.join("custody-input.json"),
            serde_json::to_vec(pending.batch().records()).unwrap(),
        )
        .unwrap();
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
            .publish_pending(pending, writer, custody, &route, self.worker_budget);
            panic!("parent-death fixture was not killed: {result:?}");
        }
        let (mut manager, connection) = connection_config(self.database, "postgres")
            .port(port)
            .connect(NoTls)
            .await
            .unwrap();
        let driver = tokio::spawn(custody.retain_catalog_connection(connection).unwrap());
        let abort = driver.abort_handle();
        let pid: i32 = manager
            .query_one("SELECT pg_backend_pid()", &[])
            .await
            .unwrap()
            .get(0);
        emit(serde_json::json!({"event":"manager-connected","backend_pid":pid}));
        let committing = Cell::new(false);
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

pub async fn observe(mode: &str, action: &str) {
    let home = PathBuf::from(format!("/out/signed_{mode}"));
    let bytes = fs::read(home.join("custody-input.json")).unwrap();
    assert!(bytes.len() < 32768);
    let records: Vec<FactRef> = serde_json::from_slice(&bytes).unwrap();
    let request = request(mode, &AdmissionBatch::new(records).unwrap());
    let identity = CatalogIdentity::local(
        dataset(),
        request.catalog(),
        &format!("{}/kind=7/", home.display()),
    )
    .unwrap();
    let (client, driver) = connect(&format!("bbpub_control_{mode}"), "bigbrotr_control").await;
    let (mut owner, _) = ControlSession::acquire(
        client,
        ClaimRequest {
            dataset: dataset(),
            instance: [2; 16].try_into().unwrap(),
            authority_mode: "managed".into(),
            role_instance_id: [32; 16],
            incarnation: [42; 16].try_into().unwrap(),
            lease_ttl: Duration::from_secs(15),
        },
        validate,
        &Cell::new(false),
    )
    .await
    .unwrap();
    let receipt = owner
        .register_archive_attempt(&request, &Cell::new(false))
        .await
        .unwrap();
    assert!(!receipt.created && receipt.latest_issued);
    owner.release().await.unwrap();
    driver.await.unwrap().unwrap();
    match LocalAttempt::recover(&home.join("lifetimes"), &receipt, &identity) {
        Err(LifetimeError::Occupied) if action == "observe" => {
            emit(serde_json::json!({"event":"custody-observed","state":"occupied"}))
        }
        Ok(_custody) => {
            emit(serde_json::json!({"event":"custody-observed","state":"recovered"}));
            let rocks = ingest::logical_state(&home.join("rocks"));
            emit(serde_json::json!({"event":"retained-rocks","rows":rocks.0,"digest":rocks.1}));
            if action == "seal" {
                let (mut manager, driver) = connect(&format!("bbpub_{mode}"), "postgres").await;
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
        }
        outcome => panic!("unexpected custody observation: {:?}", outcome.err()),
    }
}
