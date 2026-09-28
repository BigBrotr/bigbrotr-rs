//! Isolated actual PostgreSQL/session client. No installed Sync claim.
use bigbrotr::{
    acquisition::job::{AcquisitionCommand, JobDuration},
    management::{
        credentials::HASH_RESERVATION,
        grants::Revision,
        sessions::{IssuedSession, SessionError, SessionLifetime},
    },
    runtime::{ProcessEnvelope, ResourceBudget, ResourceLimits, ResourceRequest, WaiterLimits},
    storage::postgres::control_owner::{ClaimRequest, ControlSession, OwnerError},
};
use bigbrotr_model::{AcquisitionScopeId, CommandKey, DatasetId, ProcessIncarnation};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{cell::Cell, path::Path, time::Duration};
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
    assert!(matches!(args.len(), 5 | 8));
    let fault = args.len() == 8;
    let mode = args[4].as_str();
    if fault {
        assert!(matches!(
            mode,
            "fault-accept"
                | "fault-unknown"
                | "fault-cancel"
                | "fault-owner-expiry"
                | "fault-policy"
                | "fault-permission"
                | "fault-disabled"
                | "fault-replay"
                | "fault-replay-permission"
                | "fault-replay-policy"
                | "fault-busy"
        ));
    } else {
        assert!(matches!(
            mode,
            "pass"
                | "no-grant"
                | "stale-policy"
                | "rollback"
                | "expired"
                | "disabled"
                | "old-schema"
        ));
    }
    let nonce: u8 = if fault { args[6].parse().unwrap() } else { 244 };
    assert!(!fault || (50..=99).contains(&nonce));
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
        .port(if fault {
            args[5].parse().unwrap()
        } else {
            5432
        })
        .user("bigbrotr_control")
        .application_name(format!("acquisition-job-fixture-{nonce}"))
        .dbname(&args[1])
        .password(std::fs::read(&args[2]).unwrap());
    let (client, connection) = config.connect(NoTls).await.unwrap();
    let driver = tokio::spawn(connection);
    let row = client
        .query_one(
            "SELECT dataset_id::bytea,instance_id::bytea,pg_backend_pid() FROM control.datasets",
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
    let claim_started = Cell::new(false);
    let claimed = ControlSession::acquire(
        client,
        ClaimRequest {
            dataset,
            instance,
            authority_mode: "managed".into(),
            role_instance_id: [nonce; 16],
            incarnation: ProcessIncarnation::try_from([nonce; 16]).unwrap(),
            lease_ttl: Duration::from_secs(if mode == "fault-owner-expiry" { 3 } else { 60 }),
        },
        validate,
        &claim_started,
    )
    .await;
    if mode == "fault-busy" {
        assert!(matches!(claimed, Err(OwnerError::Busy)) && !claim_started.get());
        driver.await.unwrap().unwrap();
        println!("{}", json!({"event":"busy","actual_driver_joined":true}));
        return;
    }
    let (mut owner, policy) = claimed.unwrap();
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
    if fault {
        println!(
            "{}",
            json!({"event":"ready","pid":row.get::<_,i32>(2),
            "epoch":owner.identity().epoch.get(),"config":policy.config_revision,
            "policy":policy.policy_revision,"session_expires":session.observation.expires_at_us})
        );
        gate(&args[7], "go").await;
        let outcome = fault_operation(&mut owner, &session, &command, mode, &args[7]).await;
        drop(owner);
        let driver_ok = driver.await.unwrap().is_ok();
        assert_eq!(budget.snapshot().active_permits(), 0);
        println!(
            "{}",
            json!({"event":"done","outcome":outcome,
            "actual_driver_joined":true,"driver_ok":driver_ok})
        );
        return;
    }
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

// Fixture-only barriers leave the actual current-thread PG driver polling.
// Every call is bounded by main's finite timeout; no barrier reaches the product.
async fn gate(prefix: &str, suffix: &str) {
    while !Path::new(&format!("{prefix}.{suffix}")).exists() {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn fault_operation(
    owner: &mut ControlSession,
    session: &IssuedSession,
    command: &AcquisitionCommand,
    mode: &str,
    prefix: &str,
) -> Value {
    let started = Cell::new(false);
    let operation =
        owner.accept_acquisition(&session.session, &session.csrf, key(77), command, &started);
    if mode == "fault-cancel" {
        assert!(
            tokio::time::timeout(Duration::from_secs(2), operation)
                .await
                .is_err()
        );
        assert!(started.get());
        assert_eq!(
            owner
                .accept_acquisition(
                    &session.session,
                    &session.csrf,
                    key(77),
                    command,
                    &Cell::new(false)
                )
                .await,
            Err(SessionError::Unavailable)
        );
        println!("{}", json!({"event":"cancelled","commit_started":true}));
        gate(prefix, "finish").await;
        return json!({"result":"cancelled","commit_started":true});
    }
    let result = operation.await;
    let refusal = match mode {
        "fault-unknown" => Some((SessionError::CommitUnknown, true, false)),
        "fault-owner-expiry" => Some((SessionError::Denied, true, true)),
        "fault-policy" => Some((SessionError::PolicyChanged, false, false)),
        "fault-permission" => Some((SessionError::Forbidden, false, true)),
        "fault-disabled" => Some((SessionError::Conflict, false, true)),
        _ => None,
    };
    if let Some((error, commit_started, available)) = refusal {
        assert_eq!(result, Err(error));
        assert_eq!(started.get(), commit_started);
        if !available {
            assert_eq!(
                owner
                    .accept_acquisition(
                        &session.session,
                        &session.csrf,
                        key(77),
                        command,
                        &Cell::new(false)
                    )
                    .await,
                Err(SessionError::Unavailable)
            );
        }
        return json!({"result":format!("{error:?}"),"commit_started":started.get()});
    }
    let receipt = result.unwrap();
    assert_eq!(receipt.created, mode == "fault-accept");
    assert_eq!(started.get(), receipt.created);
    if matches!(mode, "fault-replay-permission" | "fault-replay-policy") {
        let absent = Cell::new(false);
        assert_eq!(
            owner
                .accept_acquisition(&session.session, &session.csrf, key(78), command, &absent)
                .await,
            Err(if mode == "fault-replay-permission" {
                SessionError::Forbidden
            } else {
                SessionError::Conflict
            })
        );
        assert!(!absent.get());
    }
    json!({"receipt":receipt,"commit_started":started.get()})
}
