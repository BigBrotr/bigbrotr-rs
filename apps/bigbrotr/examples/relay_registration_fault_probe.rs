//! Finite authenticated source/job command client. File barriers are lab-only.
use bigbrotr::{
    acquisition::job::AcquisitionCommand,
    management::{
        acquisition_scope::ScopeCommand,
        credentials::HASH_RESERVATION,
        grants::Revision,
        jobs::JobCommand,
        relay_registration::RegistrationCommand,
        sessions::{BrowserSecret, SessionError, SessionLifetime},
    },
    runtime::{
        OwnedPermit, ProcessEnvelope, ResourceBudget, ResourceLimits, ResourceRequest, WaiterLimits,
    },
    storage::postgres::control_owner::{ClaimRequest, ControlSession, OwnerError},
};
use bigbrotr_model::{CommandKey, DatasetId, ProcessIncarnation};
use sha2::{Digest, Sha256};
use std::{cell::Cell, path::Path, time::Duration};
use tokio_postgres::{Config, NoTls};

// This fixture authenticates the two exact retained documents independently.
// It is not the product closed-policy parser and grants no network authority.
fn validate_fixture_policy(document: &str, digest: [u8; 32]) -> Result<(), OwnerError> {
    let supplied: serde_json::Value =
        serde_json::from_str(document).map_err(|_| OwnerError::InvalidPolicy)?;
    let documents: [serde_json::Value; 2] = serde_json::from_slice(
        &std::fs::read(std::env::args().nth(3).expect("fixture policies path"))
            .expect("fixture policies"),
    )
    .expect("two independent fixture policies");
    if !documents.contains(&supplied) {
        return Err(OwnerError::InvalidPolicy);
    }
    let expected: [u8; 32] = Sha256::new()
        .chain_update(b"bigbrotr/product-policy/3\0")
        .chain_update(serde_json::to_vec(&supplied).unwrap())
        .finalize()
        .into();
    if digest != expected {
        return Err(OwnerError::InvalidPolicy);
    }
    Ok(())
}

fn permit() -> OwnedPermit {
    ResourceBudget::new(
        ProcessEnvelope::new(
            HASH_RESERVATION,
            0,
            ResourceLimits::new(HASH_RESERVATION, 1, 0, 0),
            WaiterLimits::new(0, 0),
        )
        .unwrap(),
    )
    .try_acquire(ResourceRequest::data(HASH_RESERVATION, 1, 0).unwrap())
    .unwrap()
}
async fn gate(path: &str, suffix: &str) {
    while !Path::new(&format!("{path}.{suffix}")).exists() {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
fn revision(value: u64) -> Revision {
    Revision::try_from(value.to_string()).unwrap()
}

// These real command consumers share only the finite fault/lifetime schedule.
// Independent Python oracles reconstruct their distinct persisted meanings.
enum Command {
    Endpoint(RegistrationCommand),
    Scope(ScopeCommand),
    Acquisition(AcquisitionCommand),
}
impl Command {
    async fn accept(
        &self,
        owner: &mut ControlSession,
        token: &BrowserSecret,
        csrf: &BrowserSecret,
        key: CommandKey,
        started: &Cell<bool>,
    ) -> Result<serde_json::Value, SessionError> {
        match self {
            Self::Endpoint(command) => owner
                .accept_relay_registration(token, csrf, key, command, started)
                .await
                .map(|receipt| serde_json::json!(receipt)),
            Self::Scope(command) => owner
                .accept_acquisition_scope(token, csrf, key, command, started)
                .await
                .map(|receipt| serde_json::json!(receipt)),
            Self::Acquisition(command) => owner
                .accept_acquisition(token, csrf, key, command, started)
                .await
                .map(|receipt| serde_json::json!(receipt)),
        }
    }
}
#[tokio::main(flavor = "current_thread")]
async fn main() {
    tokio::time::timeout(Duration::from_secs(45), run())
        .await
        .expect("finite registration fault probe");
}
async fn run() {
    let args: Vec<_> = std::env::args().collect();
    assert!(matches!(args.len(), 8 | 9));
    let kind = args.get(8).map(String::as_str).unwrap_or("endpoint");
    assert!(matches!(kind, "endpoint" | "scope" | "job"));
    let mode = args[4].as_str();
    let nonce: u8 = args[6].parse().unwrap();
    assert!((10..=99).contains(&nonce));
    let mut config = Config::new();
    config
        .host("127.0.0.1")
        .port(args[5].parse().unwrap())
        .user("bigbrotr_control")
        .application_name(format!("registration-fixture-{nonce}"))
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
    let identity = |column| {
        DatasetId::try_from(<[u8; 16]>::try_from(row.get::<_, &[u8]>(column)).unwrap()).unwrap()
    };
    let claim_started = Cell::new(false);
    let claimed = ControlSession::acquire(
        client,
        ClaimRequest {
            dataset: identity(0),
            instance: identity(1),
            authority_mode: "managed".into(),
            role_instance_id: [nonce; 16],
            incarnation: ProcessIncarnation::try_from([nonce; 16]).unwrap(),
            lease_ttl: Duration::from_secs(if mode == "owner-expiry" { 3 } else { 30 }),
        },
        validate_fixture_policy,
        &claim_started,
    )
    .await;
    if mode == "busy" {
        assert!(matches!(claimed, Err(OwnerError::Busy)));
        assert!(!claim_started.get());
        driver.await.unwrap().unwrap();
        println!(
            "{}",
            serde_json::json!({"event":"busy","commit_started":false})
        );
        return;
    }
    let (mut owner, policy) = claimed.unwrap();
    owner.require_browser_sessions().await.unwrap();
    match kind {
        "scope" => owner.require_acquisition_scope().await.unwrap(),
        "job" => owner.require_acquisition_jobs().await.unwrap(),
        _ => owner.require_relay_registration().await.unwrap(),
    }
    let challenge = owner
        .begin_browser_login("alice", "127.0.0.1".parse().unwrap())
        .await
        .unwrap();
    let verified = challenge
        .verify(b"Independent owner password 2026", &mut permit())
        .unwrap();
    let lifetime = if mode == "session-expiry" {
        SessionLifetime::new(Duration::from_secs(3), Duration::from_secs(3)).unwrap()
    } else {
        SessionLifetime::default()
    };
    let session = owner
        .finish_browser_login(verified, lifetime)
        .await
        .unwrap();
    // Fixed original command heads permit historical retry after a new owner
    // has accepted changed heads; they must not silently rebase the old intent.
    let command = if kind == "job" {
        let path = Path::new(&args[3]).with_file_name("job-command.json");
        let JobCommand::Acquisition(command) =
            serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
        else {
            panic!("expected finite acquisition fixture");
        };
        Command::Acquisition(command)
    } else if kind == "scope" {
        let path = Path::new(&args[3]).with_file_name("command.json");
        Command::Scope(ScopeCommand::decode(&std::fs::read(path).unwrap()).unwrap())
    } else {
        Command::Endpoint(RegistrationCommand {
            url: bigbrotr_protocol::endpoint::Endpoint::parse(
                "WS://127.0.0.1:7447/a/../installed-fixture",
            )
            .unwrap(),
            expected_config_revision: revision(1),
            expected_policy_revision: revision(1),
        })
    };
    let key = CommandKey::try_from([49; 16]).unwrap();
    println!(
        "{}",
        serde_json::json!({"event":"ready","pid":row.get::<_,i32>(2),
            "epoch":owner.identity().epoch.get(),"config":policy.config_revision,
            "policy":policy.policy_revision,"session_expires":session.observation.expires_at_us})
    );
    // Always wait: the controller snapshots durable rows after real login and
    // before this operation, independently of production decoding/digest code.
    gate(&args[7], "go").await;
    let started = Cell::new(false);
    let outcome = if mode == "cancel" {
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            command.accept(&mut owner, &session.session, &session.csrf, key, &started),
        )
        .await;
        assert!(result.is_err() && started.get());
        assert_eq!(
            owner.require_browser_sessions().await,
            Err(SessionError::Unavailable)
        );
        println!(
            "{}",
            serde_json::json!({"event":"cancelled","commit_started":true})
        );
        gate(&args[7], "finish").await;
        serde_json::json!({"result":"cancelled","commit_started":true})
    } else {
        let result = command
            .accept(&mut owner, &session.session, &session.csrf, key, &started)
            .await;
        let expected = match mode {
            "unknown" => Some((SessionError::CommitUnknown, true, false)),
            "owner-expiry" | "session-expiry" => Some((SessionError::Denied, true, true)),
            "policy" => Some((SessionError::PolicyChanged, false, false)),
            "revoked" | "inactive" | "authentication" => Some((SessionError::Denied, false, true)),
            "permission" => Some((SessionError::Forbidden, false, true)),
            "accept" => None,
            _ => panic!("unknown fault mode"),
        };
        if let Some((error, commit, available)) = expected {
            assert_eq!(result, Err(error));
            assert_eq!(started.get(), commit);
            assert_eq!(
                owner.require_browser_sessions().await,
                if available {
                    Ok(())
                } else {
                    Err(SessionError::Unavailable)
                }
            );
            serde_json::json!({"result":error.to_string(),"commit_started":commit,"available":available})
        } else {
            let receipt = result.unwrap();
            assert_eq!(started.get(), receipt["created"].as_bool().unwrap());
            serde_json::json!({"receipt":receipt,"commit_started":started.get()})
        }
    };
    drop(owner);
    let driver_ok = driver.await.unwrap().is_ok();
    println!(
        "{}",
        serde_json::json!({"event":"done","outcome":outcome,
        "driver_joined":true,"driver_ok":driver_ok})
    );
}
