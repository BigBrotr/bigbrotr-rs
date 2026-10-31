//! Isolated PG16.15 acceptance of the actual Control issuance adapter.
//! Synthetic input; no catalog activation or signed publication claim.
use bigbrotr::storage::postgres::{
    activation::{ActivationRequest, activate_initial},
    archive_attempt::{ArchiveAttemptError, ArchiveAttemptRequest},
    control_owner::{ClaimRequest, ControlSession, OwnerError},
    initialization::{InitialConfiguration, initialize},
    migration_runner,
};
use bigbrotr_model::{
    AdmissionBatch, ArchiveBatch, DatasetId, Digest256, FactRef, OwnerEpoch, OwnerResourceId,
    ProcessIncarnation, Sequence, SourceRecordId, TopologyGeneration,
};
use sha2::{Digest, Sha256};
use std::{cell::Cell, time::Duration};
use tokio::task::JoinHandle;
use tokio_postgres::{Client, Config, NoTls};

type Driver = JoinHandle<Result<(), tokio_postgres::Error>>;
async fn connect(role: &str) -> (Client, Driver) {
    connect_database(role, "attempts").await
}
async fn connect_database(role: &str, database: &str) -> (Client, Driver) {
    let mut config = Config::new();
    config
        .host("postgres")
        .dbname(database)
        .user(role)
        .password(std::fs::read(format!("/secrets/{role}")).unwrap())
        .connect_timeout(Duration::from_secs(5));
    let (client, driver) = config.connect(NoTls).await.unwrap();
    (client, tokio::spawn(driver))
}
async fn join(driver: Driver) {
    let _ = tokio::time::timeout(Duration::from_secs(5), driver)
        .await
        .unwrap()
        .unwrap();
}
fn dataset() -> DatasetId {
    DatasetId::try_from([1; 16]).unwrap()
}
fn instance() -> DatasetId {
    DatasetId::try_from([2; 16]).unwrap()
}
fn validate(document: &str, digest: [u8; 32]) -> Result<(), OwnerError> {
    // Fixture-only validator. Production callers validate the complete roots.
    let expected = include_str!("../../../tests/fixtures/configuration/policy-v3.json");
    if serde_json::from_str::<serde_json::Value>(document).map_err(|_| OwnerError::InvalidPolicy)?
        != serde_json::from_str::<serde_json::Value>(expected).unwrap()
        || digest
            != <[u8; 32]>::from(
                Sha256::new()
                    .chain_update(b"bigbrotr/product-policy/3\0")
                    .chain_update(expected)
                    .finalize(),
            )
    {
        return Err(OwnerError::InvalidPolicy);
    }
    Ok(())
}
async fn owner(n: u8) -> (ControlSession, Driver) {
    let (client, driver) = connect("bigbrotr_control").await;
    let (owner, _) = ControlSession::acquire(
        client,
        ClaimRequest {
            dataset: dataset(),
            instance: instance(),
            authority_mode: "managed".into(),
            role_instance_id: [n; 16],
            incarnation: ProcessIncarnation::try_from([n + 1; 16]).unwrap(),
            lease_ttl: Duration::from_secs(60),
        },
        validate,
        &Cell::new(false),
    )
    .await
    .unwrap();
    (owner, driver)
}
fn request(n: u8, predecessor: u64, payload: u8, catalog: u8) -> ArchiveAttemptRequest {
    let input = [AdmissionBatch::new(vec![FactRef {
        source_record_id: SourceRecordId {
            dataset_id: dataset(),
            shard_id: 0,
            owner_epoch: OwnerEpoch::new(1).unwrap(),
            sequence: Sequence::new(1).unwrap(),
        },
        payload_digest: Digest256::from_bytes([payload; 32]),
    }])
    .unwrap()];
    ArchiveAttemptRequest::new(
        [n; 16],
        OwnerResourceId::try_from([catalog; 16]).unwrap(),
        ProcessIncarnation::try_from([80; 16]).unwrap(),
        ProcessIncarnation::try_from([n + 1; 16]).unwrap(),
        predecessor,
        &ArchiveBatch::new(1, TopologyGeneration::new(0), &input).unwrap(),
    )
    .unwrap()
}
async fn migration_boundaries() {
    let (mut fresh, driver) = connect_database("postgres", "fresh").await;
    assert_eq!(
        migration_runner::run(&mut fresh, 9, true, &Cell::new(false))
            .await
            .unwrap()
            .applied,
        8
    );
    drop(fresh);
    join(driver).await;
    let (mut legacy, driver) = connect_database("postgres", "legacy").await;
    migration_runner::run(&mut legacy, 8, true, &Cell::new(false))
        .await
        .unwrap();
    legacy.batch_execute("INSERT INTO control.datasets(dataset_id,instance_id,created_at_us) VALUES(decode(repeat('01',16),'hex'),decode(repeat('02',16),'hex'),0); INSERT INTO control.role_instances(role_instance_id,dataset_id,role,created_at_us) VALUES(decode(repeat('03',16),'hex'),decode(repeat('01',16),'hex'),'archive',0); INSERT INTO control.owner_epochs(resource_id,epoch,dataset_id,role,role_instance_id,process_incarnation,acquired_at_us,lease_expires_at_us,state) VALUES(decode(repeat('04',16),'hex'),1,decode(repeat('01',16),'hex'),'archive',decode(repeat('03',16),'hex'),decode(repeat('05',16),'hex'),0,1,'active');").await.unwrap();
    let started = Cell::new(false);
    assert!(matches!(
        migration_runner::run(&mut legacy, 9, true, &started).await,
        Err(migration_runner::RunError::UnqualifiedArchiveHistory)
    ));
    assert!(!started.get());
    let row=legacy.query_one("SELECT (SELECT count(*) FROM control.schema_versions), to_regclass('control.archive_attempts') IS NULL, (SELECT count(*) FROM control.owner_epochs WHERE role='archive' AND state='active')",&[]).await.unwrap();
    assert_eq!(row.get::<_, i64>(0), 7);
    assert!(row.get::<_, bool>(1));
    assert_eq!(row.get::<_, i64>(2), 1);
    drop(legacy);
    join(driver).await;
}

async fn run(include_scan: bool) {
    migration_boundaries().await;
    let (mut admin, admin_driver) = connect("postgres").await;
    // Exercise eligible forward upgrade, unchanged old ledger, and exact replay.
    assert_eq!(
        migration_runner::run(&mut admin, 8, true, &Cell::new(false))
            .await
            .unwrap()
            .applied,
        7
    );
    let old:Vec<(i64,Vec<u8>)>=admin.query("SELECT migration::bigint, checksum::bytea FROM control.schema_versions ORDER BY migration",&[]).await.unwrap().iter().map(|r|(r.get(0),r.get(1))).collect();
    assert_eq!(
        migration_runner::run(&mut admin, 9, false, &Cell::new(false))
            .await
            .unwrap()
            .plan
            .pending
            .len(),
        1
    );
    assert_eq!(
        migration_runner::run(&mut admin, 9, true, &Cell::new(false))
            .await
            .unwrap()
            .applied,
        1
    );
    assert_eq!(
        migration_runner::run(&mut admin, 9, true, &Cell::new(false))
            .await
            .unwrap()
            .applied,
        0
    );
    let retained:Vec<(i64,Vec<u8>)>=admin.query("SELECT migration::bigint, checksum::bytea FROM control.schema_versions WHERE migration<>9 ORDER BY migration",&[]).await.unwrap().iter().map(|r|(r.get(0),r.get(1))).collect();
    assert_eq!(old, retained);
    admin.batch_execute("GRANT USAGE ON SCHEMA control TO bigbrotr_control; GRANT SELECT ON ALL TABLES IN SCHEMA control TO bigbrotr_control; GRANT INSERT, UPDATE ON control.datasets,control.principals,control.config_revisions,control.policy_revisions,control.management_audit,control.management_commands,control.role_instances,control.owner_epochs TO bigbrotr_control; GRANT INSERT ON control.archive_attempts TO bigbrotr_control; GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA control TO bigbrotr_control;").await.unwrap();
    let (mut application, application_driver) = connect("bigbrotr_control").await;
    let document = include_str!("../../../tests/fixtures/configuration/policy-v3.json");
    let digest: [u8; 32] = Sha256::new()
        .chain_update(b"bigbrotr/product-policy/3\0")
        .chain_update(document.as_bytes())
        .finalize()
        .into();
    initialize(
        &mut application,
        InitialConfiguration {
            dataset: dataset(),
            instance: instance(),
            authority_mode: "managed",
            document,
            document_digest: digest,
            source_digest: [3; 32],
        },
        |document, digest| {
            validate(document, digest).map_err(|_| {
                bigbrotr::storage::postgres::initialization::InitializeError::InvalidInput
            })
        },
        &Cell::new(false),
    )
    .await
    .unwrap();
    activate_initial(
        &mut application,
        ActivationRequest {
            dataset: dataset(),
            instance: instance(),
            request_id: [4; 16],
            expected_revision: 1,
            authority_mode: "managed",
        },
        |d, h| {
            validate(d, h).map_err(|_| {
                bigbrotr::storage::postgres::activation::ActivationError::PolicyInvalid
            })
        },
        &Cell::new(false),
    )
    .await
    .unwrap();
    drop(application);
    join(application_driver).await;
    admin.batch_execute("BEGIN; INSERT INTO control.topology_generations(dataset_id,generation,created_at_us) VALUES(decode(repeat('01',16),'hex'),0,0); UPDATE control.topology_generations SET sealed=true; UPDATE control.datasets SET topology_generation=0; COMMIT;").await.unwrap();
    let first = request(20, 0, 7, 90);
    let second = request(22, 1, 7, 90);
    let (mut current, driver) = owner(40).await;
    assert_eq!(
        current
            .read_archive_attempt(first.catalog(), *first.attempt_id())
            .await
            .unwrap(),
        None,
    );
    let empty = current
        .begin_archive_attempt_scan(first.catalog())
        .await
        .unwrap();
    assert_eq!(empty.high_water(), 0);
    assert!(empty.is_complete());
    let commit = Cell::new(false);
    let initial = current
        .register_archive_attempt(&first, &commit)
        .await
        .unwrap();
    assert!(commit.get());
    assert!(initial.created && initial.latest_issued);
    assert_eq!(initial.ordinal, 1);
    commit.set(false);
    let retry = current
        .register_archive_attempt(&first, &commit)
        .await
        .unwrap();
    assert!(!commit.get());
    assert!(!retry.created && retry.latest_issued);
    assert_eq!(retry.ordinal, 1);
    let retained_before: String = admin.query_one("SELECT row_to_json(t)::text FROM (SELECT a.*,a.xmin::text FROM control.archive_attempts a WHERE ordinal=1)t", &[]).await.unwrap().get(0);
    assert_eq!(
        current
            .read_archive_attempt(first.catalog(), *first.attempt_id())
            .await
            .unwrap(),
        Some(retry.clone()),
        "retained intent must be observed without submitting the original request",
    );
    let retained_after: String = admin.query_one("SELECT row_to_json(t)::text FROM (SELECT a.*,a.xmin::text FROM control.archive_attempts a WHERE ordinal=1)t", &[]).await.unwrap().get(0);
    assert_eq!(retained_before, retained_after);
    assert!(
        current
            .read_archive_attempt(first.catalog(), [33; 16])
            .await
            .unwrap()
            .is_none()
    );
    let next = current
        .register_archive_attempt(&second, &Cell::new(false))
        .await
        .unwrap();
    assert!(next.created && next.latest_issued);
    assert_eq!(next.ordinal, 2);
    let historical = current
        .register_archive_attempt(&first, &Cell::new(false))
        .await
        .unwrap();
    assert!(!historical.created && !historical.latest_issued);
    assert_eq!(historical.ordinal, 1);
    assert_eq!(
        current
            .read_archive_attempt(first.catalog(), *first.attempt_id())
            .await
            .unwrap(),
        Some(historical.clone()),
    );
    // Retain committed history after closing the actual session; a successor
    // resolves the original issuer's exact row, not a reminted request.
    drop(current);
    join(driver).await;
    let (mut successor, driver) = owner(42).await;
    assert_eq!(
        successor
            .read_archive_attempt(first.catalog(), *first.attempt_id())
            .await
            .unwrap(),
        Some(historical.clone()),
        "fresh Control session must recover the exact original identity",
    );
    assert_eq!(
        successor
            .register_archive_attempt(&first, &Cell::new(false))
            .await
            .unwrap(),
        historical
    );
    successor.release().await.unwrap();
    join(driver).await;
    for (n, bad) in [
        (44, request(20, 0, 8, 90)),
        (46, request(24, 0, 7, 90)),
        (48, request(24, 2, 7, 91)),
        (50, request(20, 1, 7, 90)),
    ] {
        let (mut owner, driver) = owner(n).await;
        let commit = Cell::new(false);
        assert_eq!(
            owner.register_archive_attempt(&bad, &commit).await,
            Err(ArchiveAttemptError::Conflict)
        );
        assert!(!commit.get());
        assert_eq!(
            owner.register_archive_attempt(&second, &commit).await,
            Err(ArchiveAttemptError::Unavailable)
        );
        drop(owner);
        join(driver).await;
    }
    let (mut final_owner, final_driver) = owner(52).await;
    assert_eq!(
        final_owner
            .register_archive_attempt(&second, &Cell::new(false))
            .await
            .unwrap()
            .ordinal,
        2
    );
    // Raw SQL by another application backend cannot borrow copied issuer fields.
    let (forger, forger_driver) = connect("bigbrotr_control").await;
    let active = final_owner.identity();
    let error=forger.execute("INSERT INTO control.archive_attempts SELECT decode(repeat('31',16),'hex'),dataset_id,catalog_id,3,2,schema_version,topology_generation,batch_digest,coordinator_incarnation,decode(repeat('32',16),'hex'),$1::bytea,$2::text::numeric,issuer_role,$3::bytea,issued_at_us FROM control.archive_attempts WHERE ordinal=2",&[&&active.resource_id.as_bytes()[..],&active.epoch.get().to_string(),&&active.process_incarnation.as_bytes()[..]]).await.unwrap_err();
    assert_eq!(error.code().unwrap().code(), "23514");
    drop(forger);
    join(forger_driver).await;
    for sql in [
        "UPDATE control.archive_attempts SET issued_at_us=0",
        "DELETE FROM control.archive_attempts",
        "TRUNCATE control.archive_attempts",
    ] {
        let error = admin.batch_execute(sql).await.unwrap_err();
        assert_eq!(error.code().unwrap().code(), "23514");
    }
    let rows=admin.query("SELECT ordinal::text, expected_predecessor::text, encode(attempt_id,'hex'), encode(worker_incarnation,'hex'), issuer_epoch::text FROM control.archive_attempts ORDER BY control.archive_attempts.ordinal",&[]).await.unwrap();
    assert_eq!(rows.len(), 2);
    for (row, n, ordinal) in [(&rows[0], 20u8, 1u64), (&rows[1], 22, 2)] {
        assert_eq!(row.get::<_, String>(0), ordinal.to_string());
        assert_eq!(row.get::<_, String>(1), (ordinal - 1).to_string());
        assert_eq!(row.get::<_, String>(2), format!("{n:02x}").repeat(16));
        assert_eq!(row.get::<_, String>(3), format!("{:02x}", n + 1).repeat(16));
        assert_eq!(row.get::<_, String>(4), "1");
    }
    final_owner.release().await.unwrap();
    join(final_driver).await;
    let (mut cancelled, cancelled_driver) = owner(54).await;
    let block = admin.transaction().await.unwrap();
    block
        .batch_execute("LOCK TABLE control.archive_attempts IN ACCESS EXCLUSIVE MODE")
        .await
        .unwrap();
    let pending = request(24, 2, 7, 90);
    let started = Cell::new(false);
    assert!(
        tokio::time::timeout(
            Duration::from_millis(250),
            cancelled.register_archive_attempt(&pending, &started)
        )
        .await
        .is_err()
    );
    assert!(!started.get());
    assert_eq!(
        cancelled.register_archive_attempt(&pending, &started).await,
        Err(ArchiveAttemptError::Unavailable)
    );
    block.rollback().await.unwrap();
    drop(cancelled);
    join(cancelled_driver).await;
    assert_eq!(
        admin
            .query_one("SELECT count(*) FROM control.archive_attempts", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        2
    );
    // Epoch is numeric even though the bounded wire representation is text.
    // Reacquisition must cross 9 -> 10 -> 11 without choosing the lexical max.
    for (n, expected_epoch) in [(56, 9), (58, 10), (60, 11), (62, 12)] {
        let (next, d) = owner(n).await;
        assert_eq!(next.identity().epoch.get(), expected_epoch);
        next.release().await.unwrap();
        join(d).await;
    }
    let (mut reader, driver) = owner(64).await;
    assert_eq!(
        reader.read_archive_attempt(first.catalog(), [0; 16]).await,
        Err(ArchiveAttemptError::InvalidInput)
    );
    assert_eq!(
        reader
            .read_archive_attempt(reader.identity().resource_id, *first.attempt_id())
            .await,
        Err(ArchiveAttemptError::InvalidInput)
    );
    assert_eq!(
        reader
            .read_archive_attempt(first.catalog(), *second.attempt_id())
            .await
            .unwrap()
            .unwrap()
            .request(),
        &second
    );
    assert_eq!(
        reader
            .read_archive_attempt(
                OwnerResourceId::try_from([91; 16]).unwrap(),
                *first.attempt_id()
            )
            .await,
        Err(ArchiveAttemptError::Conflict)
    );
    assert_eq!(
        reader
            .read_archive_attempt(first.catalog(), *first.attempt_id())
            .await,
        Err(ArchiveAttemptError::Unavailable)
    );
    drop(reader);
    join(driver).await;
    let (mut reader, driver) = owner(66).await;
    let block = admin.transaction().await.unwrap();
    block
        .batch_execute("LOCK TABLE control.archive_attempts IN ACCESS EXCLUSIVE MODE")
        .await
        .unwrap();
    {
        let read = reader.read_archive_attempt(first.catalog(), *first.attempt_id());
        tokio::pin!(read);
        let observed = async {
            loop {
                block
                    .query_one("SELECT pg_stat_clear_snapshot()", &[])
                    .await
                    .unwrap();
                let blocked = block.query_opt("SELECT json_build_object('event','retained-read-blocker','waiter',a.pid,'waiter_start',a.backend_start,'blocker',h.pid,'blocker_start',h.backend_start,'wait_event',a.wait_event)::text FROM pg_stat_activity a JOIN pg_stat_activity h ON h.pid=pg_backend_pid() WHERE a.datname=current_database() AND a.usename='bigbrotr_control' AND h.pid=ANY(pg_blocking_pids(a.pid)) AND a.wait_event_type='Lock'", &[]).await.unwrap();
                if let Some(blocked) = blocked {
                    println!("{}", blocked.get::<_, &str>(0));
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        };
        tokio::select! {
            outcome = &mut read => panic!("read escaped actual table blocker: {outcome:?}"),
            observed = tokio::time::timeout(Duration::from_secs(3), observed) => observed.unwrap(),
        }
    }
    assert_eq!(
        reader
            .read_archive_attempt(first.catalog(), *first.attempt_id())
            .await,
        Err(ArchiveAttemptError::Unavailable)
    );
    block.rollback().await.unwrap();
    drop(reader);
    join(driver).await;
    let (mut reader, driver) = owner(68).await;
    assert_eq!(
        reader
            .read_archive_attempt(first.catalog(), *first.attempt_id())
            .await
            .unwrap(),
        Some(historical)
    );
    reader.release().await.unwrap();
    join(driver).await;
    if include_scan {
        scan_cases(&mut admin).await;
    }
    drop(admin);
    join(admin_driver).await;
    println!(
        "{}",
        serde_json::json!({"status":"PASS","initial_rows":2,"rows":if include_scan { 134 } else { 2 },"ordinals":[1,2],"issuer_epochs":[1,1],"retained_read":"exact current/historical identity, fresh owner, absence, wrong catalog, no mutation and actual blocked cancellation/poison/retry","scope":"actual adapter issuance/read/replay/CAS/refusals and immutable SQL history; not catalog fencing or lost wire COMMIT"})
    );
}
async fn scan_cases(admin: &mut Client) {
    let catalog = OwnerResourceId::try_from([90; 16]).unwrap();
    let (mut current, driver) = owner(140).await;
    // Literal identities are independent of the SQL decoder and scan cursor.
    let mut expected = vec![[20; 16], [22; 16]];
    for n in 100..230 {
        current
            .register_archive_attempt(&request(n, expected.len() as u64, 7, 90), &Cell::new(false))
            .await
            .unwrap();
        expected.push([n; 16]);
    }
    let mut scan = current.begin_archive_attempt_scan(catalog).await.unwrap();
    assert_eq!(scan.high_water(), 132);
    assert_eq!(scan.observed_through(), 0);
    // Later issuance must neither extend this scan nor falsify latest_issued.
    for (n, predecessor) in [(230, 132), (231, 133)] {
        current
            .register_archive_attempt(&request(n, predecessor, 7, 90), &Cell::new(false))
            .await
            .unwrap();
    }
    let before: String = admin.query_one("SELECT json_agg(t ORDER BY ordinal)::text FROM (SELECT a.*, a.xmin::text FROM control.archive_attempts a)t", &[]).await.unwrap().get(0);
    let mut found = Vec::new();
    let mut lengths = Vec::new();
    while !scan.is_complete() {
        let page = current.read_archive_attempt_page(&mut scan).await.unwrap();
        lengths.push(page.len());
        for receipt in page {
            assert!(!receipt.created && !receipt.latest_issued);
            assert_eq!(receipt.ordinal, found.len() as u64 + 1);
            found.push(*receipt.request().attempt_id());
        }
        // Each page releases its PG transaction, allowing the actual authority
        // refresh between pages without changing the immutable cutoff.
        current.poll_policy(validate).await.unwrap();
    }
    assert_eq!(lengths, [64, 64, 4]);
    assert_eq!(found, expected);
    assert!(
        current
            .read_archive_attempt_page(&mut scan)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(scan.observed_through(), 132);
    let after: String = admin.query_one("SELECT json_agg(t ORDER BY ordinal)::text FROM (SELECT a.*, a.xmin::text FROM control.archive_attempts a)t", &[]).await.unwrap().get(0);
    assert_eq!(
        before, after,
        "scan must not mutate any intent, including xmin"
    );
    let mut stale = current.begin_archive_attempt_scan(catalog).await.unwrap();
    assert_eq!(stale.high_water(), 134);
    current.release().await.unwrap();
    join(driver).await;
    let (mut successor, driver) = owner(142).await;
    assert_eq!(
        successor.read_archive_attempt_page(&mut stale).await,
        Err(ArchiveAttemptError::InvalidInput)
    );
    let mut resumed = successor.begin_archive_attempt_scan(catalog).await.unwrap();
    let mut last = None;
    while !resumed.is_complete() {
        last = successor
            .read_archive_attempt_page(&mut resumed)
            .await
            .unwrap()
            .pop();
    }
    let last = last.unwrap();
    assert_eq!(last.ordinal, 134);
    assert!(last.latest_issued);
    successor.release().await.unwrap();
    join(driver).await;
    // Privileged corruption fixtures represent an incompatible restore. Never
    // hide historical catalog mismatch using a catalog filter or skip a hole.
    for (n, mutation, restore, error) in [
        (
            144,
            "UPDATE control.archive_attempts SET catalog_id=decode(repeat('5b',16),'hex') WHERE ordinal=65",
            "UPDATE control.archive_attempts SET catalog_id=decode(repeat('5a',16),'hex') WHERE ordinal=65",
            ArchiveAttemptError::Conflict,
        ),
        (
            146,
            "UPDATE control.archive_attempts SET ordinal=1000,expected_predecessor=999 WHERE ordinal=65",
            "UPDATE control.archive_attempts SET ordinal=65,expected_predecessor=64 WHERE ordinal=1000",
            ArchiveAttemptError::InvalidMetadata,
        ),
    ] {
        admin.batch_execute(&format!("BEGIN; ALTER TABLE control.archive_attempts DISABLE TRIGGER USER; {mutation}; ALTER TABLE control.archive_attempts ENABLE TRIGGER USER; COMMIT")).await.unwrap();
        let (mut reader, driver) = owner(n).await;
        let mut scan = reader.begin_archive_attempt_scan(catalog).await.unwrap();
        assert_eq!(
            reader
                .read_archive_attempt_page(&mut scan)
                .await
                .unwrap()
                .len(),
            64
        );
        assert_eq!(
            reader.read_archive_attempt_page(&mut scan).await,
            Err(error)
        );
        assert_eq!(
            scan.observed_through(),
            64,
            "failed page must not advance cursor"
        );
        assert_eq!(
            reader.read_archive_attempt_page(&mut scan).await,
            Err(ArchiveAttemptError::Unavailable)
        );
        drop(reader);
        join(driver).await;
        admin.batch_execute(&format!("BEGIN; ALTER TABLE control.archive_attempts DISABLE TRIGGER USER; {restore}; ALTER TABLE control.archive_attempts ENABLE TRIGGER USER; COMMIT")).await.unwrap();
    }
    let (mut reader, driver) = owner(148).await;
    let mut scan = reader.begin_archive_attempt_scan(catalog).await.unwrap();
    let block = admin.transaction().await.unwrap();
    block
        .batch_execute("LOCK TABLE control.archive_attempts IN ACCESS EXCLUSIVE MODE")
        .await
        .unwrap();
    {
        let page = reader.read_archive_attempt_page(&mut scan);
        tokio::pin!(page);
        let observed = async {
            loop {
                block
                    .query_one("SELECT pg_stat_clear_snapshot()", &[])
                    .await
                    .unwrap();
                let row = block.query_opt("SELECT json_build_object('event','scan-page-blocker','waiter',pid,'waiter_start',backend_start,'blocker',pg_backend_pid(),'wait_event',wait_event)::text FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control' AND pg_backend_pid()=ANY(pg_blocking_pids(pid)) AND wait_event_type='Lock'", &[]).await.unwrap();
                if let Some(row) = row {
                    println!("{}", row.get::<_, &str>(0));
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        };
        tokio::select! {
            outcome = &mut page => panic!("page escaped blocker: {outcome:?}"),
            result = tokio::time::timeout(Duration::from_secs(3), observed) => result.unwrap(),
        }
    }
    assert_eq!(scan.observed_through(), 0);
    assert_eq!(
        reader.read_archive_attempt_page(&mut scan).await,
        Err(ArchiveAttemptError::Unavailable)
    );
    block.rollback().await.unwrap();
    drop(reader);
    join(driver).await;
    // Independent final SQL verifies cardinality, endpoints and immutable rows.
    let actual: String = admin.query_one("SELECT json_agg(t ORDER BY ordinal)::text FROM (SELECT a.*, a.xmin::text FROM control.archive_attempts a)t", &[]).await.unwrap().get(0);
    let normalize = |text: &str| {
        let mut rows: Vec<serde_json::Value> = serde_json::from_str(text).unwrap();
        for row in &mut rows {
            row.as_object_mut().unwrap().remove("xmin");
        }
        rows
    };
    assert_eq!(normalize(&actual), normalize(&before));
    println!(
        "{}",
        serde_json::json!({"event":"archive-attempt-scan", "status":"PASS", "rows":134, "pages":[64,64,4], "cutoff":132, "last_ordinal":134, "scope":"fixed cutoff, complete history, stale owner, historical catalog mismatch, gap, cancellation, unchanged intents"})
    );
}

async fn seed_runtime(database: &str) {
    let (admin, driver) = connect_database("postgres", database).await;
    admin.batch_execute("BEGIN; INSERT INTO control.topology_generations(dataset_id,generation,created_at_us) VALUES(decode(repeat('01',16),'hex'),0,0); UPDATE control.topology_generations SET sealed=true; UPDATE control.datasets SET topology_generation=0; COMMIT;").await.unwrap();
    let (client, connection) = connect_database("bigbrotr_control", database).await;
    let (mut owner, _) = ControlSession::acquire(
        client,
        ClaimRequest {
            dataset: dataset(),
            instance: instance(),
            authority_mode: "managed".into(),
            role_instance_id: [150; 16],
            incarnation: ProcessIncarnation::try_from([151; 16]).unwrap(),
            lease_ttl: Duration::from_secs(60),
        },
        |document, digest| {
            // Fixture seeding only. Actual serve independently validates the full
            // accepted policy generated by its real initialization/activation CLI.
            let value: serde_json::Value = serde_json::from_str(document).unwrap();
            assert_eq!(value["settings"]["administration.enabled"], false);
            let compact = serde_json::to_vec(&value).unwrap();
            let actual: [u8; 32] = Sha256::new()
                .chain_update(b"bigbrotr/product-policy/3\0")
                .chain_update(compact)
                .finalize()
                .into();
            assert_eq!(actual, digest);
            Ok(())
        },
        &Cell::new(false),
    )
    .await
    .unwrap();
    for (predecessor, n) in (100..230).enumerate() {
        owner
            .register_archive_attempt(&request(n, predecessor as u64, 7, 90), &Cell::new(false))
            .await
            .unwrap();
    }
    owner.release().await.unwrap();
    join(connection).await;
    drop(admin);
    join(driver).await;
    println!(
        "{}",
        serde_json::json!({"status":"PASS", "event":"runtime-fixture-issued", "rows":130})
    );
}

fn main() {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            tokio::time::timeout(Duration::from_secs(120), async {
                let args: Vec<String> = std::env::args().skip(1).collect();
                if let [mode, database] = args.as_slice() {
                    assert_eq!(mode, "--seed-runtime");
                    seed_runtime(database).await;
                } else {
                    match args.as_slice() {
                        [] => run(false).await,
                        [mode] if mode == "--scan" => run(true).await,
                        _ => panic!("expected --scan or --seed-runtime DATABASE"),
                    }
                }
            })
            .await
            .expect("finite native probe");
        });
}
