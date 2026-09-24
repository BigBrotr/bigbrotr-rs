//! Real catalog loss between completion's two checks, with an observed SQL wait.
use bigbrotr::{
    export::execution::WrittenExport,
    management::sessions::SessionError,
    storage::postgres::{catalog_guard::CatalogCapture, control_owner::ControlSession},
};
use serde_json::json;
use std::{cell::Cell, fs, time::Duration};
use tokio::task::JoinHandle;
use tokio_postgres::Client;

async fn rows(client: &Client, job: &[u8]) -> serde_json::Value {
    let row = client.query_one("SELECT jsonb_build_object('job',to_jsonb(j),'xmin',j.xmin::text,'artifacts',(SELECT count(*) FROM control.artifacts),'files',(SELECT count(*) FROM control.artifact_files),'units',(SELECT count(*) FROM control.export_execution_units))::text FROM control.jobs j WHERE job_id=$1::bytea", &[&job]).await.unwrap();
    serde_json::from_str(row.get::<_, &str>(0)).unwrap()
}

pub(super) async fn exercise(
    owner: &mut ControlSession,
    written: &WrittenExport,
    capture: &CatalogCapture<'_>,
    reader_pin: [u8; 16],
    catalog_pid: i32,
    catalog_driver: &mut JoinHandle<Result<(), tokio_postgres::Error>>,
) {
    let (mut locker, locker_connection) =
        super::connection("bbpub_control_normal", "postgres").await;
    let locker_driver = tokio::spawn(locker_connection);
    let (observer, observer_connection) =
        super::connection("bbpub_control_normal", "postgres").await;
    let observer_driver = tokio::spawn(observer_connection);
    let locker_pid: i32 = locker
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    let identity = owner.identity();
    let control_pid: i32 = observer.query_one("SELECT control_backend_pid FROM control.owner_epochs WHERE resource_id=$1::bytea AND epoch=$2::text::numeric AND process_incarnation=$3::bytea AND state='active'", &[&&identity.resource_id.as_bytes()[..], &identity.epoch.get().to_string(), &&identity.process_incarnation.as_bytes()[..]]).await.unwrap().get(0);
    assert_ne!(catalog_pid, control_pid);
    assert_ne!(locker_pid, control_pid);
    let expected = written.job().clone();
    let before = rows(&observer, expected.selection().job().as_bytes()).await;
    assert_eq!(before["artifacts"], 0);
    assert_eq!(before["files"], 0);
    let lock = locker.transaction().await.unwrap();
    // SHARE permits the historical lookup's ACCESS SHARE; ACCESS EXCLUSIVE
    // would stop before the actual INSERT and exercise the wrong boundary.
    lock.batch_execute("LOCK TABLE control.artifacts IN SHARE MODE")
        .await
        .unwrap();
    let mut evidence = json!({"status":"INCOMPLETE","job":expected.selection().job(),
        "artifact":expected.reservation().unwrap().artifact(),"control_pid":control_pid,
        "catalog_pid":catalog_pid,"locker_pid":locker_pid,"before":before,
        "reader_pin":bigbrotr_model::ProcessIncarnation::try_from(reader_pin).unwrap().to_string()});
    let path = "/out/rollback-observation.json";
    let started = Cell::new(false);
    let complete = owner.complete_export(written, capture, reader_pin, &started);
    let fault = async {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        loop {
            let blocked: bool = observer.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND state='active' AND wait_event_type='Lock' AND query LIKE 'INSERT INTO control.artifacts(%' AND $2=ANY(pg_blocking_pids(pid)))", &[&control_pid, &locker_pid]).await.unwrap().get(0);
            if blocked {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "actual artifact INSERT was not observed waiting for the intended locker"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        evidence["insert_wait_observed"] = json!(true);
        fs::write(path, serde_json::to_vec_pretty(&evidence).unwrap()).unwrap();
        let terminated: bool = observer
            .query_one("SELECT pg_terminate_backend($1)", &[&catalog_pid])
            .await
            .unwrap()
            .get(0);
        assert!(terminated);
        let joined = tokio::time::timeout(Duration::from_secs(2), catalog_driver)
            .await
            .unwrap()
            .unwrap();
        evidence["catalog_driver_joined"] = json!(true);
        evidence["catalog_driver_error"] = json!(joined.is_err());
        let absent: bool = observer
            .query_one(
                "SELECT NOT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1)",
                &[&catalog_pid],
            )
            .await
            .unwrap()
            .get(0);
        assert!(absent);
        evidence["catalog_backend_absent"] = json!(true);
        fs::write(path, serde_json::to_vec_pretty(&evidence).unwrap()).unwrap();
        lock.rollback().await.unwrap();
    };
    let (completion, ()) = tokio::time::timeout(Duration::from_secs(8), async {
        tokio::join!(complete, fault)
    })
    .await
    .unwrap();
    assert_eq!(completion, Err(SessionError::ArchiveUnavailable));
    assert!(!started.get(), "completion COMMIT must not have begun");
    evidence["completion"] = json!("ArchiveUnavailable");
    evidence["commit_started"] = json!(started.get());
    let after = rows(&observer, expected.selection().job().as_bytes()).await;
    evidence["after"] = after.clone();
    fs::write(path, serde_json::to_vec_pretty(&evidence).unwrap()).unwrap();
    assert_eq!(
        after, before,
        "failed completion must preserve rows and xmin"
    );
    // Row equality alone cannot detect an uncollected rollback: a transaction's
    // Drop can queue it. Reusing this exact session distinguishes the fix.
    let resolved = owner.read_export_job(expected.selection().job()).await;
    evidence["same_session_resolution"] = json!(format!(
        "{:?}",
        resolved.as_ref().map(|v| v.as_ref().map(|j| j.state()))
    ));
    fs::write(path, serde_json::to_vec_pretty(&evidence).unwrap()).unwrap();
    assert_eq!(resolved, Ok(Some(expected)));
    drop(locker);
    locker_driver.await.unwrap().unwrap();
    drop(observer);
    observer_driver.await.unwrap().unwrap();
    evidence["status"] = json!("PASS");
    fs::write(path, serde_json::to_vec_pretty(&evidence).unwrap()).unwrap();
    super::case("catalog-loss-inside-completion-rolls-back-and-preserves-control-session");
}
