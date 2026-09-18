//! Shared actual Control intent fixture for isolated catalog probes.
use bigbrotr::storage::postgres::{
    archive_attempt::{ArchiveAttemptReceipt, ArchiveAttemptRequest},
    control_owner::{ClaimRequest, ControlSession},
};
use bigbrotr_model::{
    AdmissionBatch, ArchiveBatch, DatasetId, Digest256, FactRef, OwnerEpoch, OwnerResourceId,
    ProcessIncarnation, Sequence, SourceRecordId, TopologyGeneration,
};
use std::{cell::Cell, io::Write, time::Duration};
use tokio_postgres::{Client, Config, NoTls};

pub fn emit(value: serde_json::Value) {
    println!("{value}");
    std::io::stdout().flush().unwrap();
}
pub async fn connect(
    database: &str,
    role: &str,
    port: u16,
) -> (Client, tokio::task::JoinHandle<()>) {
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
pub fn dataset() -> DatasetId {
    DatasetId::try_from([1; 16]).unwrap()
}
pub fn catalog() -> OwnerResourceId {
    OwnerResourceId::try_from([90; 16]).unwrap()
}
pub async fn intent(ordinal: u8) -> ArchiveAttemptReceipt {
    assert!((1..=100).contains(&ordinal));
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
    let n = 18 + 2 * ordinal;
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
    session.release().await.unwrap();
    driver.await.unwrap();
    receipt
}
pub async fn retained(ordinal: u8) -> ArchiveAttemptReceipt {
    let receipt = intent(ordinal).await;
    assert!(
        !receipt.created,
        "fixture must resolve the original actual Control intent"
    );
    receipt
}
