//! Isolated actual PostgreSQL/session client. No installed Sync claim.
use bigbrotr::{
    acquisition::job::{AcquisitionCommand, JobDuration},
    management::{
        credentials::HASH_RESERVATION,
        grants::Revision,
        sessions::{SessionError, SessionLifetime},
    },
    runtime::{ProcessEnvelope, ResourceBudget, ResourceLimits, ResourceRequest, WaiterLimits},
    storage::postgres::control_owner::{ClaimRequest, ControlSession, OwnerError},
};
use bigbrotr_model::{AcquisitionScopeId, CommandKey, DatasetId, ProcessIncarnation};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{cell::Cell, time::Duration};
use tokio_postgres::{Config, NoTls};

fn key(n: u8) -> CommandKey {
    [n; 16].try_into().unwrap()
}
fn revision(n: u64) -> Revision {
    n.to_string().try_into().unwrap()
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    tokio::time::timeout(Duration::from_secs(30), run())
        .await
        .expect("finite probe");
}
async fn run() {
    let args: Vec<_> = std::env::args().collect();
    assert_eq!(args.len(), 5);
    let mode = args[4].as_str();
    assert!(matches!(
        mode,
        "pass" | "no-grant" | "stale-policy" | "rollback" | "expired" | "disabled" | "old-schema"
    ));
    let expected: Value = serde_json::from_slice(&std::fs::read(&args[3]).unwrap()).unwrap();
    let digest: [u8; 32] = Sha256::new()
        .chain_update(b"bigbrotr/product-policy/2\0")
        .chain_update(serde_json::to_vec(&expected).unwrap())
        .finalize()
        .into();
    let validate = |document: &str, actual: [u8; 32]| {
        if serde_json::from_str::<Value>(document).ok().as_ref() == Some(&expected)
            && actual == digest
        {
            Ok(())
        } else {
            Err(OwnerError::InvalidPolicy)
        }
    };
    let mut config = Config::new();
    config
        .host("127.0.0.1")
        .user("bigbrotr_control")
        .dbname(&args[1])
        .password(std::fs::read(&args[2]).unwrap());
    let (client, connection) = config.connect(NoTls).await.unwrap();
    let driver = tokio::spawn(connection);
    let row = client
        .query_one(
            "SELECT dataset_id::bytea,instance_id::bytea FROM control.datasets",
            &[],
        )
        .await
        .unwrap();
    let id =
        |i| DatasetId::try_from(<[u8; 16]>::try_from(row.get::<_, &[u8]>(i)).unwrap()).unwrap();
    let (dataset, instance) = (id(0), id(1));
    let rows = client
        .query(
            "SELECT scope_id::bytea FROM control.acquisition_scopes ORDER BY scope_id LIMIT 1",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let scope_id =
        AcquisitionScopeId::try_from(<[u8; 16]>::try_from(rows[0].get::<_, &[u8]>(0)).unwrap())
            .unwrap();
    let (mut owner, _) = ControlSession::acquire(
        client,
        ClaimRequest {
            dataset,
            instance,
            authority_mode: "managed".into(),
            role_instance_id: [244; 16],
            incarnation: ProcessIncarnation::try_from([244; 16]).unwrap(),
            lease_ttl: Duration::from_secs(60),
        },
        validate,
        &Cell::new(false),
    )
    .await
    .unwrap();
    let budget = ResourceBudget::new(
        ProcessEnvelope::new(
            HASH_RESERVATION,
            0,
            ResourceLimits::new(HASH_RESERVATION, 1, 0, 0),
            WaiterLimits::new(0, 0),
        )
        .unwrap(),
    );
    let mut hash_permit = budget
        .try_acquire(ResourceRequest::data(HASH_RESERVATION, 1, 0).unwrap())
        .unwrap();
    let challenge = owner
        .begin_browser_login("alice", "127.0.0.1".parse().unwrap())
        .await
        .unwrap();
    let verified = challenge
        .verify(b"Independent owner password 2026", &mut hash_permit)
        .unwrap();
    drop(hash_permit);
    let session = owner
        .finish_browser_login(verified, SessionLifetime::default())
        .await
        .unwrap();
    let command = AcquisitionCommand {
        scope_id,
        deadline_us: JobDuration::new(if mode == "expired" { 1 } else { 120_000_000 }).unwrap(),
        expected_config_revision: revision(1),
        expected_policy_revision: revision(if mode == "stale-policy" { 2 } else { 1 }),
    };
    let committed = Cell::new(false);
    let result = owner
        .accept_acquisition(
            &session.session,
            &session.csrf,
            key(77),
            &command,
            &committed,
        )
        .await;
    match mode {
        "no-grant" => assert_eq!(result, Err(SessionError::Forbidden)),
        "stale-policy" => assert_eq!(result, Err(SessionError::Conflict)),
        "disabled" => assert_eq!(result, Err(SessionError::Conflict)),
        "old-schema" => assert_eq!(result, Err(SessionError::Prerequisite)),
        "expired" => {
            assert_eq!(result, Err(SessionError::Denied));
            assert!(committed.get());
        }
        "rollback" => {
            assert_eq!(result, Err(SessionError::Backend));
            assert!(committed.get());
        }
        "pass" => {
            let receipt = result.unwrap();
            assert!(receipt.created && committed.get());
            let mut replay = receipt.clone();
            replay.created = false;
            assert_eq!(
                owner
                    .accept_acquisition(
                        &session.session,
                        &session.csrf,
                        key(77),
                        &command,
                        &Cell::new(false)
                    )
                    .await
                    .unwrap(),
                replay
            );
            let mut changed = command.clone();
            changed.deadline_us = JobDuration::new(120_000_001).unwrap();
            assert_eq!(
                owner
                    .accept_acquisition(
                        &session.session,
                        &session.csrf,
                        key(77),
                        &changed,
                        &Cell::new(false)
                    )
                    .await,
                Err(SessionError::Conflict)
            );
            println!("{}", json!({"event":"receipt","value":receipt}));
        }
        _ => unreachable!(),
    }
    if !matches!(mode, "rollback" | "old-schema") {
        owner.release().await.unwrap();
    } else {
        drop(owner);
    }
    driver.await.unwrap().unwrap();
    assert_eq!(budget.snapshot().active_permits(), 0);
    println!(
        "{}",
        json!({"event":"terminal","mode":mode,"status":"PASS","actual_driver_joined":true})
    );
}
