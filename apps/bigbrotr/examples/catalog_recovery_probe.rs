//! Isolated real-PG catalog recovery fixture, driven by an external fault proxy.
//! Synthetic retained input; never claims scientific publication or worker launch.
use bigbrotr::storage::postgres::{
    archive_attempt::{ArchiveAttemptReceipt, ArchiveAttemptRequest},
    catalog_guard::{self, Activation, CatalogIdentity, GuardError},
    control_owner::{ClaimRequest, ControlSession},
};
use bigbrotr_model::{
    AdmissionBatch, ArchiveBatch, DatasetId, Digest256, FactRef, OwnerEpoch, OwnerResourceId,
    ProcessIncarnation, Sequence, SourceRecordId, TopologyGeneration,
};
use std::{cell::Cell, io::Write, time::Duration};
use tokio_postgres::{Client, Config, NoTls};

fn emit(value: serde_json::Value) {
    println!("{value}");
    std::io::stdout().flush().unwrap();
}
async fn connect(database: &str, role: &str, port: u16) -> (Client, tokio::task::JoinHandle<()>) {
    let mut config = Config::new();
    config
        .host("127.0.0.1")
        .port(port)
        .dbname(database)
        .user(role)
        .password(std::fs::read(format!("/secrets/{role}")).unwrap())
        .application_name("bigbrotr-catalog-recovery-probe")
        .connect_timeout(Duration::from_secs(5));
    let (client, connection) = config.connect(NoTls).await.unwrap();
    (
        client,
        tokio::spawn(async move {
            let _ = connection.await;
        }),
    )
}
fn dataset() -> DatasetId {
    DatasetId::try_from([1; 16]).unwrap()
}
fn catalog() -> OwnerResourceId {
    OwnerResourceId::try_from([90; 16]).unwrap()
}
async fn retained(ordinal: u8) -> ArchiveAttemptReceipt {
    assert!(matches!(ordinal, 1 | 2));
    let input = [AdmissionBatch::new(vec![FactRef {
        source_record_id: SourceRecordId {
            dataset_id: dataset(),
            shard_id: 0,
            owner_epoch: OwnerEpoch::new(1).unwrap(),
            sequence: Sequence::new(1).unwrap(),
        },
        payload_digest: Digest256::from_bytes([7; 32]),
    }])
    .unwrap()];
    let n = if ordinal == 1 { 20 } else { 22 };
    let request = ArchiveAttemptRequest::new(
        [n; 16],
        catalog(),
        ProcessIncarnation::try_from([80; 16]).unwrap(),
        ProcessIncarnation::try_from([n + 1; 16]).unwrap(),
        u64::from(ordinal - 1),
        &ArchiveBatch::new(1, TopologyGeneration::new(0), &input).unwrap(),
    )
    .unwrap();
    let (client, driver) = connect("attempts", "bigbrotr_control", 5432).await;
    let mut role = [0; 16];
    let mut process = [0; 16];
    getrandom::fill(&mut role).unwrap();
    getrandom::fill(&mut process).unwrap();
    let (mut session, _) = ControlSession::acquire(
        client,
        ClaimRequest {
            dataset: dataset(),
            instance: DatasetId::try_from([2; 16]).unwrap(),
            authority_mode: "managed".into(),
            role_instance_id: role,
            incarnation: ProcessIncarnation::try_from(process).unwrap(),
            lease_ttl: Duration::from_secs(60),
        },
        |_, _| Ok(()),
        &Cell::new(false),
    )
    .await
    .unwrap();
    let receipt = session
        .register_archive_attempt(&request, &Cell::new(false))
        .await
        .unwrap();
    assert!(
        !receipt.created,
        "fixture must resolve the original actual Control intent"
    );
    session.release().await.unwrap();
    driver.await.unwrap();
    receipt
}
async fn run() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    assert_eq!(args.len(), 4);
    let (mode, database) = (&args[0], &args[1]);
    assert!(matches!(
        mode.as_str(),
        "install" | "activate" | "cancel" | "inspect"
    ));
    assert!(
        database.starts_with("bbcat_")
            && database.len() <= 30
            && database
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b == b'_' || b.is_ascii_digit())
    );
    let ordinal: u8 = args[2].parse().unwrap();
    let port: u16 = args[3].parse().unwrap();
    let receipt = if matches!(mode.as_str(), "activate" | "cancel") {
        Some(retained(ordinal).await)
    } else {
        assert_eq!(ordinal, 0);
        None
    };
    let (mut client, mut driver) = connect(database, "postgres", port).await;
    let backend = client.query_one(
        "SELECT pg_backend_pid(),backend_start::text FROM pg_stat_activity WHERE pid=pg_backend_pid()", &[],
    ).await.unwrap();
    emit(
        serde_json::json!({"event":"connected", "pid":backend.get::<_,i32>(0),
        "start":backend.get::<_,String>(1)}),
    );
    let identity =
        CatalogIdentity::local(dataset(), catalog(), &format!("/out/archive_{database}/")).unwrap();
    let commit = Cell::new(false);
    let operation = async {
        match mode.as_str() {
            "install" => catalog_guard::install(&mut client, &identity, "BootstrapMixed", &commit)
                .await
                .map(|created| if created { "installed" } else { "existing" }),
            "inspect" => catalog_guard::inspect(&mut client, &identity)
                .await
                .map(|()| "inspected"),
            _ => {
                catalog_guard::activate(&mut client, &identity, receipt.as_ref().unwrap(), &commit)
                    .await
                    .map(|result| match result {
                        Activation::Activated => "activated",
                        Activation::Observed {
                            current: true,
                            enabled: true,
                        } => "current",
                        Activation::Observed {
                            current: false,
                            enabled: false,
                        } => "historical",
                        Activation::Observed {
                            current: true,
                            enabled: false,
                        } => "sealed",
                        _ => panic!("invalid activation observation"),
                    })
            }
        }
    };
    let outcome = if mode == "cancel" {
        assert!(
            tokio::time::timeout(Duration::from_secs(2), operation)
                .await
                .is_err()
        );
        assert!(commit.get(), "cancellation must occur at the held COMMIT");
        "cancelled"
    } else {
        match operation.await {
            Ok(outcome) => outcome,
            Err(GuardError::CommitUnknown) => "unknown",
            Err(GuardError::Conflict) => "conflict",
            Err(GuardError::Backend) => "backend-refusal",
            Err(error) => panic!("unexpected guard outcome: {error}"),
        }
    };
    emit(serde_json::json!({"event":"outcome", "outcome":outcome,"commit_started":commit.get()}));
    // A dropped operation is not server exclusion. The controller observes the
    // still-held backend before releasing/closing the proxy; no client reuse.
    drop(client);
    if tokio::time::timeout(Duration::from_secs(5), &mut driver)
        .await
        .is_err()
    {
        driver.abort();
        let _ = driver.await;
        emit(serde_json::json!({"event":"joined","aborted":true}));
    } else {
        emit(serde_json::json!({"event":"joined","aborted":false}));
    }
}
fn main() {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            tokio::time::timeout(Duration::from_secs(45), run())
                .await
                .unwrap()
        });
}
