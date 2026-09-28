//! Finite actual-adapter fault probe. Gates belong to the isolated test harness.
use bigbrotr::storage::postgres::control_owner::{
    ClaimRequest, ControlSession, EndpointRegistration, OwnerError, RelayRegistryError,
};
use bigbrotr_model::{DatasetId, ProcessIncarnation};
use bigbrotr_protocol::endpoint::Endpoint;
use std::{cell::Cell, path::Path, time::Duration};
use tokio_postgres::{Config, NoTls};

#[path = "support/relay_policy.rs"]
mod relay_policy;

async fn gate(path: &str, suffix: &str) {
    while !Path::new(&format!("{path}.{suffix}")).exists() {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn receipt(value: EndpointRegistration) -> serde_json::Value {
    serde_json::json!({
        "id":value.identity.to_string(),"created":value.created,
        "time":value.registered_at_us,"config":value.config_revision,
        "policy":value.policy_revision,"epoch":value.registered_by.epoch.get(),
        "resource":value.registered_by.resource_id.to_string(),
        "incarnation":value.registered_by.process_incarnation.to_string(),
        "role_instance":value.registered_by.role_instance_id,
    })
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    tokio::time::timeout(Duration::from_secs(40), run())
        .await
        .expect("finite fault probe");
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
        .application_name(format!("relay-fixture-{nonce}"))
        .dbname(&args[1])
        .password(std::fs::read(&args[2]).unwrap());
    let (client, connection) = config.connect(NoTls).await.unwrap();
    let driver = tokio::spawn(connection);
    let row = client
        .query_one(
            "SELECT dataset_id::bytea, instance_id::bytea, pg_backend_pid() FROM control.datasets",
            &[],
        )
        .await
        .unwrap();
    let identity = |index| {
        DatasetId::try_from(<[u8; 16]>::try_from(row.get::<_, &[u8]>(index)).unwrap()).unwrap()
    };
    let claim_started = Cell::new(false);
    let result = ControlSession::acquire(
        client,
        ClaimRequest {
            dataset: identity(0),
            instance: identity(1),
            authority_mode: "managed".into(),
            role_instance_id: [nonce; 16],
            incarnation: ProcessIncarnation::try_from([nonce; 16]).unwrap(),
            lease_ttl: Duration::from_secs(match mode {
                "held-expired" => 3,
                "existing-expired" => 1,
                _ => 30,
            }),
        },
        relay_policy::validate,
        &claim_started,
    )
    .await;
    if mode == "busy" {
        assert!(matches!(result, Err(OwnerError::Busy)));
        assert!(!claim_started.get());
        driver.await.unwrap().unwrap();
        println!(
            "{}",
            serde_json::json!({"event":"busy","commit_started":false})
        );
        return;
    }
    let (mut owner, policy) = result.unwrap();
    assert!(claim_started.get());
    println!(
        "{}",
        serde_json::json!({"event":"ready","pid":row.get::<_,i32>(2),
        "epoch":owner.identity().epoch.get(),"config":policy.config_revision,
        "policy":policy.policy_revision})
    );
    if matches!(mode, "policy" | "existing-expired") {
        gate(&args[7], "go").await;
    }
    let endpoint = Endpoint::parse(match mode {
        "overfull" => "wss://fixture.example/collision/0",
        "existing" | "existing-expired" => "wss://fixture.example/a/~?b=2&a=1",
        _ => "wss://fixture.example/fault",
    })
    .unwrap();
    let started = Cell::new(false);
    let outcome = if mode == "cancel" {
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            owner.register_relay_endpoint(&endpoint, &started),
        )
        .await;
        assert!(result.is_err() && started.get());
        assert_eq!(
            owner
                .register_relay_endpoint(&endpoint, &Cell::new(false))
                .await,
            Err(RelayRegistryError::Unavailable)
        );
        println!(
            "{}",
            serde_json::json!({"event":"cancelled","commit_started":true})
        );
        // Keep both session and transport driver alive until the controller has
        // observed real exclusion and chooses the retained COMMIT's outcome.
        gate(&args[7], "finish").await;
        serde_json::json!({"result":"cancelled","commit_started":true})
    } else {
        let result = owner.register_relay_endpoint(&endpoint, &started).await;
        let expected = match mode {
            "unknown" => Some((RelayRegistryError::CommitUnknown, true)),
            "held-expired" => Some((RelayRegistryError::AuthorityExpired, true)),
            "policy" => Some((RelayRegistryError::PolicyChanged, false)),
            "overfull" | "overfull-new" => Some((RelayRegistryError::InvalidMetadata, false)),
            "insert" | "existing" | "existing-expired" => None,
            _ => panic!("unknown probe mode"),
        };
        if let Some((error, committed)) = expected {
            assert_eq!(result, Err(error));
            assert_eq!(started.get(), committed);
            assert_eq!(
                owner
                    .register_relay_endpoint(&endpoint, &Cell::new(false))
                    .await,
                Err(RelayRegistryError::Unavailable)
            );
            serde_json::json!({"result":error.to_string(),"commit_started":started.get()})
        } else {
            let value = result.unwrap();
            assert_eq!(started.get(), value.created);
            serde_json::json!({"receipt":receipt(value),"commit_started":started.get()})
        }
    };
    // A lost/released response can end the driver with an error; joining, not
    // its success bit, establishes that this probe retained transport custody.
    drop(owner);
    let driver_ok = driver.await.unwrap().is_ok();
    println!(
        "{}",
        serde_json::json!({"event":"done","outcome":outcome,"driver_joined":true,"driver_ok":driver_ok})
    );
}
