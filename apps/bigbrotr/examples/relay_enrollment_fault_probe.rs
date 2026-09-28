//! Finite actual enrollment client. File barriers belong only to its native lab.
use bigbrotr::{
    management::{
        credentials::HASH_RESERVATION,
        grants::Revision,
        relay_enrollment::EnrollmentCommand,
        sessions::{SessionError, SessionLifetime},
    },
    network::{DirectPolicy, PrivateRule},
    runtime::{
        OwnedPermit, ProcessEnvelope, ResourceBudget, ResourceLimits, ResourceRequest, WaiterLimits,
    },
    storage::postgres::control_owner::{
        ClaimRequest, ControlSession, EnrollmentRoutes, OwnerError,
    },
};
use bigbrotr_model::{
    AllocationDomainId, CommandKey, DatasetId, DictionaryKind, EndpointId, Ordinal,
    ProcessIncarnation, QualifiedRef,
};
use std::{cell::Cell, path::Path, time::Duration};
use tokio_postgres::{Config, NoTls};
#[path = "support/relay_policy.rs"]
mod relay_policy;

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
#[tokio::main(flavor = "current_thread")]
async fn main() {
    tokio::time::timeout(Duration::from_secs(45), run())
        .await
        .expect("finite enrollment fault probe");
}
async fn run() {
    let args: Vec<_> = std::env::args().collect();
    assert_eq!(args.len(), 8);
    let mode = args[4].as_str();
    let nonce: u8 = args[6].parse().unwrap();
    assert!((10..=99).contains(&nonce));
    let mut config = Config::new();
    config
        .host("127.0.0.1")
        .port(args[5].parse().unwrap())
        .user("bigbrotr_control")
        .application_name(format!("enrollment-fixture-{nonce}"))
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
    let endpoint = client
        .query_one(
            "SELECT endpoint_id::bytea FROM control.relay_endpoints
             WHERE url_bytes=convert_to('wss://fixture.example/private?x=1&x=2','UTF8')",
            &[],
        )
        .await
        .unwrap();
    let endpoint =
        EndpointId::try_from(<[u8; 16]>::try_from(endpoint.get::<_, &[u8]>(0)).unwrap()).unwrap();
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
        relay_policy::validate,
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
    let roots = DirectPolicy::private(vec![
        PrivateRule::hostname("fixture.example", "10.2.0.0/16", 443).unwrap(),
    ])
    .unwrap();
    let routes = EnrollmentRoutes {
        public_enabled: true,
        private_ceiling: Some(&roots),
    };
    let command = EnrollmentCommand {
        endpoint_id: endpoint,
        visibility: QualifiedRef {
            dataset_id: identity(0),
            dictionary_kind: DictionaryKind::VisibilityScope,
            allocation_domain: AllocationDomainId::try_from([55; 16]).unwrap(),
            ordinal: Ordinal::new(99).unwrap(),
        },
        route: serde_json::from_str(r#"{"class":"private","cidr":"10.2.3.0/24"}"#).unwrap(),
        enabled: true,
        expected_generation: None,
        expected_config_revision: revision(policy.config_revision),
        expected_policy_revision: revision(policy.policy_revision),
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
            owner.accept_relay_enrollment(
                &session.session,
                &session.csrf,
                key,
                &command,
                routes,
                &started,
            ),
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
        let result = owner
            .accept_relay_enrollment(
                &session.session,
                &session.csrf,
                key,
                &command,
                routes,
                &started,
            )
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
            assert_eq!(started.get(), receipt.created);
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
