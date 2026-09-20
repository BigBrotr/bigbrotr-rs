//! Finite pin fault consumer over the existing signed snapshot; no native writes.
use super::*;
use bigbrotr::storage::postgres::{
    catalog_guard::CatalogCapture,
    control_owner::{ArchivePinReceipt, ArchivePinRequest},
};

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    case: String,
    operation: u8,
    control_port: u16,
    action: String,
    ttl_ms: u64,
    wait: bool,
    cancel: bool,
}
fn receipt(value: ArchivePinReceipt) -> serde_json::Value {
    serde_json::json!({"lease_id":hex(&value.lease_id),"catalog_id":hex(value.catalog_id.as_bytes()),
        "frontier_revision":value.frontier_revision,"snapshot_id":value.snapshot_id,
        "lease_revision":value.lease_revision,"acquired_at_us":value.acquired_at_us,
        "updated_at_us":value.updated_at_us,"expires_at_us":value.expires_at_us,
        "released":value.released,"created":value.created})
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
    assert!(matches!(
        request.action.as_str(),
        "register" | "lookup" | "release" | "recover"
    ));
    assert!(request.operation != 0 && (1..=120000).contains(&request.ttl_ms));
    let home = PathBuf::from("/out/signed_normal");
    let scratch = directory(home.join(format!("pin-{}", request.case)));
    let catalog = [90; 16].try_into().unwrap();
    let identity =
        CatalogIdentity::local(dataset(), catalog, "/out/signed_normal/kind=7/").unwrap();
    if request.action == "recover" {
        recover(request, &scratch, &home, &identity).await;
        return;
    }
    let coordinator = LocalCoordinator::open(&identity, &home.join("archive-state")).unwrap();
    let (client, control_connection) = connection(
        "bbpub_control_normal",
        "bigbrotr_control",
        request.control_port,
    )
    .await;
    let driver = tokio::spawn(coordinator.retain_connection(control_connection).unwrap());
    let row = client
        .query_one(
            "SELECT pid,backend_start::text FROM pg_stat_activity WHERE pid=pg_backend_pid()",
            &[],
        )
        .await
        .unwrap();
    let control_pid: i32 = row.get(0);
    let control_start: String = row.get(1);
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
    if request.action == "lookup" || request.action == "release" {
        let found = owner
            .read_archive_pin(catalog, [request.operation; 16])
            .await
            .unwrap();
        let committing = Cell::new(false);
        let (outcome, value) = if request.action == "release" {
            match owner
                .release_archive_pin(&found.unwrap(), &committing)
                .await
            {
                Ok(value) => ("released".to_owned(), Some(receipt(value))),
                Err(error) => (format!("{error:?}"), None),
            }
        } else {
            ("lookup".to_owned(), found.map(receipt))
        };
        drop(owner);
        let control_closed = driver.await.unwrap().is_ok();
        emit(
            serde_json::json!({"event":"pin-result","outcome":outcome,"receipt":value,
            "commit_started":committing.get(),"control_closed":control_closed,"drivers_joined":true}),
        );
        return;
    }
    let (mut manager, connection) = connection("bbpub_normal", "CatalogManager", 5432).await;
    let manager_driver = tokio::spawn(coordinator.retain_connection(connection).unwrap());
    let row = manager
        .query_one(
            "SELECT pid,backend_start::text FROM pg_stat_activity WHERE pid=pg_backend_pid()",
            &[],
        )
        .await
        .unwrap();
    let manager_pid: i32 = row.get(0);
    let manager_start: String = row.get(1);
    let capture =
        CatalogCapture::at_snapshot(&mut manager, &coordinator, 3, Duration::from_secs(60))
            .await
            .unwrap();
    emit(
        serde_json::json!({"event":"pin-ready","control_pid":control_pid,"manager_pid":manager_pid,
        "control_start":control_start,"manager_start":manager_start,"incarnation":hex(&incarnation),
        "role_instance_id":hex(&role)}),
    );
    if request.wait {
        barrier(&scratch.join("continue"), 20).await;
    }
    let commit_started = Cell::new(false);
    let started = Instant::now();
    let pin = ArchivePinRequest {
        lease_id: [request.operation; 16],
        frontier_revision: 1,
        ttl: Duration::from_millis(request.ttl_ms),
    };
    let result = {
        let register = owner.register_archive_pin(&capture, &pin, &commit_started);
        tokio::pin!(register);
        tokio::select! {
            value = &mut register => Some(value),
            () = async {
                if !request.cancel { std::future::pending::<()>().await; }
                barrier(&scratch.join("cancel"),20).await;
            } => None,
        }
    };
    let (outcome, value) = match result {
        Some(Ok(value)) => ("registered".to_owned(), Some(receipt(value))),
        Some(Err(error)) => (format!("{error:?}"), None),
        None => ("cancelled".to_owned(), None),
    };
    if outcome == "cancelled" {
        emit(serde_json::json!({"event":"pin-cancelled","commit_started":commit_started.get()}));
    }
    let elapsed_ms = started.elapsed().as_millis();
    let capture_closed = capture.finish().await.is_ok();
    drop(manager);
    let manager_closed = manager_driver.await.unwrap().is_ok();
    drop(owner);
    let control_closed = driver.await.unwrap().is_ok();
    emit(
        serde_json::json!({"event":"pin-result","outcome":outcome,"receipt":value,
        "commit_started":commit_started.get(),"elapsed_ms":elapsed_ms,"capture_closed":capture_closed,
        "manager_closed":manager_closed,"control_closed":control_closed,"drivers_joined":true}),
    );
}

async fn recover(request: Request, scratch: &Path, home: &Path, identity: &CatalogIdentity) {
    use bigbrotr::storage::archive::{LifetimeError, LocalPinRecovery};
    let recovery = match LocalPinRecovery::open(identity, &home.join("archive-state")) {
        Ok(value) => value,
        Err(error) => {
            emit(
                serde_json::json!({"event":"pin-recovery-refused","outcome":format!("{error:?}"),"commit_started":false}),
            );
            return;
        }
    };
    assert!(matches!(
        LocalCoordinator::open(identity, &home.join("archive-state")),
        Err(LifetimeError::Occupied)
    ));
    let (client, connection) = connection(
        "bbpub_control_normal",
        "bigbrotr_control",
        request.control_port,
    )
    .await;
    let driver = tokio::spawn(recovery.retain_connection(connection).unwrap());
    let control: (i32, String) = {
        let row = client
            .query_one(
                "SELECT pid,backend_start::text FROM pg_stat_activity WHERE pid=pg_backend_pid()",
                &[],
            )
            .await
            .unwrap();
        (row.get(0), row.get(1))
    };
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
    let (mut manager, connection) = super::connection("bbpub_normal", "CatalogManager", 5432).await;
    let manager_driver = tokio::spawn(recovery.retain_connection(connection).unwrap());
    let row = manager
        .query_one(
            "SELECT pid,backend_start::text FROM pg_stat_activity WHERE pid=pg_backend_pid()",
            &[],
        )
        .await
        .unwrap();
    let manager_pid: i32 = row.get(0);
    let manager_start: String = row.get(1);
    emit(
        serde_json::json!({"event":"pin-ready","control_pid":control.0,"control_start":control.1,
        "manager_pid":manager_pid,"manager_start":manager_start,"incarnation":hex(&incarnation),"role_instance_id":hex(&role)}),
    );
    if request.wait {
        barrier(&scratch.join("continue"), 20).await;
    }
    let committing = Cell::new(false);
    let result = owner
        .recover_archive_pin(
            &recovery,
            &mut manager,
            [request.operation; 16],
            Duration::from_secs(60),
            &committing,
        )
        .await;
    let (outcome, value) = match result {
        Ok(value) => ("recovered".to_owned(), Some(receipt(value))),
        Err(error) => (format!("{error:?}"), None),
    };
    drop(recovery);
    // The actual transports keep the recovery custody after its wrapper has
    // gone. Only their terminal result permits ordinary startup.
    assert!(matches!(
        LocalCoordinator::open(identity, &home.join("archive-state")),
        Err(LifetimeError::Occupied)
    ));
    drop(manager);
    let manager_closed = manager_driver.await.unwrap().is_ok();
    drop(owner);
    let control_closed = driver.await.unwrap().is_ok();
    drop(LocalCoordinator::open(identity, &home.join("archive-state")).unwrap());
    emit(
        serde_json::json!({"event":"pin-result","outcome":outcome,"receipt":value,
        "commit_started":committing.get(),"manager_closed":manager_closed,"control_closed":control_closed,
        "drivers_joined":true,"ordinary_reacquired":true}),
    );
}
