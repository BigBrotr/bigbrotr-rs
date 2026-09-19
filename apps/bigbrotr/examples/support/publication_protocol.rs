//! Synthetic native Control/custody checks. No scientific publication in this
//! fixture; the separate signed schedules consume the same bound issuance API.
use super::*;
use bigbrotr::storage::{
    archive::LocalInitialization, postgres::archive_attempt::ArchiveAttemptError,
};
use bigbrotr_model::{AdmissionBatch, Digest256, FactRef, OwnerEpoch, Sequence, SourceRecordId};
#[path = "publication_protocol_faults.rs"]
mod faults;
pub use faults::commit_loss;

type Driver = tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>;

async fn claim(database: &str, local: &LocalCoordinator, n: u8) -> (ControlSession, Driver) {
    claim_port(database, local, n, 5432).await
}
async fn claim_port(
    database: &str,
    local: &LocalCoordinator,
    n: u8,
    port: u16,
) -> (ControlSession, Driver) {
    let (client, connection) = connection(database, "bigbrotr_control", port).await;
    let driver = tokio::spawn(local.retain_connection(connection).unwrap());
    let (owner, _) = ControlSession::acquire(
        client,
        ClaimRequest {
            dataset: dataset(),
            instance: [2; 16].try_into().unwrap(),
            authority_mode: "managed".into(),
            role_instance_id: [n; 16],
            incarnation: [n + 1; 16].try_into().unwrap(),
            lease_ttl: Duration::from_secs(60),
        },
        validate,
        &Cell::new(false),
    )
    .await
    .unwrap();
    (owner, driver)
}
async fn close(owner: ControlSession, driver: Driver) {
    drop(owner);
    driver.await.unwrap().unwrap();
}
async fn snapshot(database: &str) -> serde_json::Value {
    let (admin, driver) = connect(database, "postgres").await;
    let text: String = admin.query_one("SELECT json_build_object('bindings',(SELECT coalesce(json_agg(t),'[]') FROM (SELECT c.*,xmin::text AS xmin FROM control.archive_coordinators c)t),'attempts',(SELECT coalesce(json_agg(t ORDER BY ordinal),'[]') FROM (SELECT a.*,xmin::text AS xmin FROM control.archive_attempts a)t))::text", &[]).await.unwrap().get(0);
    drop(admin);
    driver.await.unwrap().unwrap();
    serde_json::from_str(&text).unwrap()
}
fn recorded(name: &str, state: serde_json::Value) {
    emit(serde_json::json!({"event":"protocol-case","name":name,"state":state}));
}

async fn prepare(database: &str, installed: bool) {
    let (admin, driver) = connect("postgres", "postgres").await;
    admin
        .batch_execute(&format!("CREATE DATABASE {database}"))
        .await
        .unwrap();
    drop(admin);
    driver.await.unwrap().unwrap();
    initialize_control(database, installed).await;
    let (admin, driver) = connect(database, "postgres").await;
    admin.batch_execute("BEGIN; INSERT INTO control.topology_generations(dataset_id,generation,created_at_us) VALUES(decode(repeat('01',16),'hex'),0,0); UPDATE control.topology_generations SET sealed=true; UPDATE control.datasets SET topology_generation=0; COMMIT;").await.unwrap();
    drop(admin);
    driver.await.unwrap().unwrap();
}

pub async fn run() {
    let home = directory("/out/coordinator-protocol".into());
    let data = directory(home.join("data"));
    let identity = CatalogIdentity::local(
        dataset(),
        [90; 16].try_into().unwrap(),
        &format!("{}/", data.display()),
    )
    .unwrap();
    drop(LocalInitialization::create(&identity, [3; 16]).unwrap());
    let local = LocalCoordinator::initialize(&identity, &directory(home.join("state"))).unwrap();
    let budget = ResourceBudget::new(
        ProcessEnvelope::new(
            2 * MIB,
            MIB,
            ResourceLimits::new(MIB, 1, 0, 0),
            WaiterLimits::new(0, 0),
        )
        .unwrap(),
    );
    let batches = [AdmissionBatch::new(vec![FactRef {
        source_record_id: SourceRecordId {
            dataset_id: dataset(),
            shard_id: 0,
            owner_epoch: OwnerEpoch::new(1).unwrap(),
            sequence: Sequence::new(1).unwrap(),
        },
        payload_digest: Digest256::from_bytes([8; 32]),
    }])
    .unwrap()];
    let batch = ArchiveBatch::new(1, TopologyGeneration::new(0), &batches).unwrap();
    let request = ArchiveAttemptRequest::new(
        [6; 16],
        [90; 16].try_into().unwrap(),
        [4; 16].try_into().unwrap(),
        [5; 16].try_into().unwrap(),
        0,
        &batch,
    )
    .unwrap();
    for database in ["bbprotocol_current", "bbprotocol_old"] {
        prepare(database, database == "bbprotocol_current").await;
    }
    let database = "bbprotocol_current";
    let before = snapshot(database).await;
    assert_eq!(before, serde_json::json!({"bindings":[],"attempts":[]}));
    let (mut owner, driver) = claim(database, &local, 10).await;
    assert_eq!(
        owner.require_archive_coordinator(&local).await,
        Err(ArchiveAttemptError::Prerequisite)
    );
    assert_eq!(
        owner
            .initialize_archive_coordinator(&local, &Cell::new(false))
            .await,
        Err(ArchiveAttemptError::Unavailable)
    );
    close(owner, driver).await;
    assert_eq!(snapshot(database).await, before);
    recorded("ordinary-missing-refuses-and-poisons", before.clone());
    let (mut owner, driver) = claim(database, &local, 12).await;
    assert_eq!(
        owner
            .register_archive_attempt(&request, &Cell::new(false))
            .await,
        Err(ArchiveAttemptError::Prerequisite)
    );
    close(owner, driver).await;
    assert_eq!(snapshot(database).await, before);
    recorded("old-unbound-api-refused-after-migration", before.clone());
    let (mut owner, driver) = claim(database, &local, 14).await;
    let commit = Cell::new(false);
    assert!(
        owner
            .initialize_archive_coordinator(&local, &commit)
            .await
            .unwrap()
    );
    assert!(commit.get());
    let bound = snapshot(database).await;
    commit.set(false);
    assert!(
        !owner
            .initialize_archive_coordinator(&local, &commit)
            .await
            .unwrap()
    );
    assert!(!commit.get());
    assert_eq!(snapshot(database).await, bound);
    recorded(
        "explicit-binding-identical-retry-preserves-xmin",
        bound.clone(),
    );
    let retained = local
        .retain_inputs(
            &request,
            &batch,
            grant(&budget, MIB, 0),
            &AtomicBool::new(false),
        )
        .unwrap();
    let receipt = owner
        .register_retained_archive_attempt(&retained, &Cell::new(false))
        .await
        .unwrap();
    assert!(receipt.created);
    let issued = snapshot(database).await;
    assert_eq!(issued["bindings"], bound["bindings"]);
    let again = owner
        .register_retained_archive_attempt(&retained, &Cell::new(false))
        .await
        .unwrap();
    assert!(!again.created && again.request() == receipt.request());
    assert!(
        !owner
            .initialize_archive_coordinator(&local, &Cell::new(false))
            .await
            .unwrap()
    );
    assert_eq!(snapshot(database).await, issued);
    recorded(
        "bound-issuance-and-both-retries-preserve-history",
        issued.clone(),
    );
    owner.release().await.unwrap();
    driver.await.unwrap().unwrap();
    let (mut owner, driver) = claim(database, &local, 16).await;
    owner.require_archive_coordinator(&local).await.unwrap();
    assert!(
        !owner
            .register_retained_archive_attempt(&retained, &Cell::new(false))
            .await
            .unwrap()
            .created
    );
    assert_eq!(snapshot(database).await, issued);
    owner.release().await.unwrap();
    driver.await.unwrap().unwrap();
    recorded(
        "fresh-control-epoch-preserves-original-binding-and-attempt",
        issued.clone(),
    );
    drop(retained);
    let other_data = directory(home.join("other-data"));
    let other_identity = CatalogIdentity::local(
        dataset(),
        identity.catalog(),
        &format!("{}/", other_data.display()),
    )
    .unwrap();
    drop(LocalInitialization::create(&other_identity, [7; 16]).unwrap());
    let other = LocalCoordinator::initialize(&other_identity, &directory(home.join("other-state")))
        .unwrap();
    let (mut owner, driver) = claim(database, &other, 18).await;
    assert_eq!(
        owner.require_archive_coordinator(&other).await,
        Err(ArchiveAttemptError::Conflict)
    );
    close(owner, driver).await;
    let other_inputs = other
        .retain_inputs(
            &request,
            &batch,
            grant(&budget, MIB, 0),
            &AtomicBool::new(false),
        )
        .unwrap();
    let (mut owner, driver) = claim(database, &other, 24).await;
    assert_eq!(
        owner
            .register_retained_archive_attempt(&other_inputs, &Cell::new(false))
            .await,
        Err(ArchiveAttemptError::Conflict)
    );
    close(owner, driver).await;
    assert_eq!(snapshot(database).await, issued);
    drop(other_inputs);
    recorded(
        "same-dataset-catalog-different-local-binding-refused",
        issued.clone(),
    );
    let (admin, driver) = connect(database, "postgres").await;
    for statement in [
        "UPDATE control.archive_coordinators SET protocol_version=1",
        "DELETE FROM control.archive_coordinators",
        "TRUNCATE control.archive_coordinators",
    ] {
        let error = admin.batch_execute(statement).await.unwrap_err();
        assert_eq!(error.code().unwrap().code(), "23514");
    }
    drop(admin);
    driver.await.unwrap().unwrap();
    assert_eq!(snapshot(database).await, issued);
    recorded("binding-update-delete-truncate-refused", issued);

    // Complete old issuance first, then apply the actual additive migration.
    // Existing rows stay byte/logically intact; no NULL is filled in by inference.
    let database = "bbprotocol_old";
    let (mut owner, driver) = claim(database, &local, 20).await;
    assert!(
        owner
            .register_archive_attempt(&request, &Cell::new(false))
            .await
            .unwrap()
            .created
    );
    owner.release().await.unwrap();
    driver.await.unwrap().unwrap();
    let (mut admin, driver) = connect(database, "postgres").await;
    let old: String = admin
        .query_one(
            "SELECT row_to_json(a)::text FROM control.archive_attempts a",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        migration_runner::run(&mut admin, 10, true, &Cell::new(false))
            .await
            .unwrap()
            .applied,
        1
    );
    admin.batch_execute("GRANT SELECT,INSERT ON control.archive_coordinators TO bigbrotr_control; GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA control TO bigbrotr_control").await.unwrap();
    drop(admin);
    driver.await.unwrap().unwrap();
    let before = snapshot(database).await;
    let mut old_after = before["attempts"][0].clone();
    assert!(
        old_after
            .as_object_mut()
            .unwrap()
            .remove("coordinator_binding_sha256")
            .unwrap()
            .is_null()
    );
    old_after.as_object_mut().unwrap().remove("xmin");
    assert_eq!(
        old_after,
        serde_json::from_str::<serde_json::Value>(&old).unwrap()
    );
    let (mut owner, driver) = claim(database, &local, 22).await;
    assert_eq!(
        owner
            .initialize_archive_coordinator(&local, &Cell::new(false))
            .await,
        Err(ArchiveAttemptError::Conflict)
    );
    close(owner, driver).await;
    assert_eq!(snapshot(database).await, before);
    recorded("old-history-survives-upgrade-and-cannot-be-adopted", before);
    assert_eq!(budget.snapshot().active_permits(), 0);
    faults::races(&local, &request).await;
}
