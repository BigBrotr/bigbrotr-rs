//! Isolated actual PostgreSQL adapter acceptance, never an installation command.
use bigbrotr::storage::postgres::control_owner::{
    ClaimRequest, ControlSession, OwnerError, RelayRegistryError,
};
use bigbrotr_model::{DatasetId, ProcessIncarnation};
use bigbrotr_protocol::endpoint::Endpoint;
use sha2::{Digest, Sha256};
use std::{cell::Cell, time::Duration};
use tokio_postgres::{Config, NoTls};

fn validate(document: &str, digest: [u8; 32]) -> Result<(), OwnerError> {
    // Fixture policy is supplied independently by the laboratory. This does not
    // replace production root/closed-schema validation or grant network access.
    let supplied: serde_json::Value =
        serde_json::from_str(document).map_err(|_| OwnerError::InvalidPolicy)?;
    let bytes = std::fs::read(std::env::args().nth(3).expect("fixture policy path"))
        .expect("fixture policy");
    let expected: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let canonical = serde_json::to_vec(&expected).unwrap();
    let expected_digest: [u8; 32] = Sha256::new()
        .chain_update(b"bigbrotr/product-policy/1\0")
        .chain_update(canonical)
        .finalize()
        .into();
    if supplied != expected || digest != expected_digest {
        return Err(OwnerError::InvalidPolicy);
    }
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    tokio::time::timeout(Duration::from_secs(45), run())
        .await
        .expect("finite probe");
}

async fn run() {
    let args: Vec<_> = std::env::args().collect();
    let mut config = Config::new();
    config
        .host("postgres")
        .user("bigbrotr_control")
        .dbname(&args[1]);
    config.password(std::fs::read(&args[2]).unwrap());
    let (client, connection) = config.connect(NoTls).await.unwrap();
    let driver = tokio::spawn(connection);
    let row = client
        .query_one(
            "SELECT dataset_id::bytea, instance_id::bytea FROM control.datasets",
            &[],
        )
        .await
        .unwrap();
    let dataset =
        DatasetId::try_from(<[u8; 16]>::try_from(row.get::<_, &[u8]>(0)).unwrap()).unwrap();
    let instance =
        DatasetId::try_from(<[u8; 16]>::try_from(row.get::<_, &[u8]>(1)).unwrap()).unwrap();
    let claim = |n| ClaimRequest {
        dataset,
        instance,
        authority_mode: "managed".into(),
        role_instance_id: [n; 16],
        incarnation: ProcessIncarnation::try_from([n; 16]).unwrap(),
        lease_ttl: if args.get(4).is_some_and(|v| v == "expired") {
            Duration::from_millis(250)
        } else {
            Duration::from_secs(30)
        },
    };
    let (mut owner, _) = ControlSession::acquire(client, claim(101), validate, &Cell::new(false))
        .await
        .unwrap();
    let mode = args.get(4).map_or("basic", String::as_str);
    if mode == "expired" {
        tokio::time::sleep(Duration::from_millis(400)).await;
        let started = Cell::new(false);
        let endpoint = Endpoint::parse("wss://fixture.example/expired").unwrap();
        assert_eq!(
            owner.register_relay_endpoint(&endpoint, &started).await,
            Err(RelayRegistryError::AuthorityExpired)
        );
        assert!(started.get());
        assert_eq!(
            owner
                .register_relay_endpoint(&endpoint, &Cell::new(false))
                .await,
            Err(RelayRegistryError::Unavailable)
        );
        drop(owner);
        driver.await.unwrap().unwrap();
        println!(
            "{}",
            serde_json::json!({"status":"PASS","case":"expired","commit_started":true})
        );
        return;
    }
    if mode == "collision" {
        let mut original = None;
        let mut ids = std::collections::BTreeSet::new();
        for n in 0..64 {
            let endpoint =
                Endpoint::parse(&format!("wss://fixture.example/collision/{n}")).unwrap();
            let receipt = owner
                .register_relay_endpoint(&endpoint, &Cell::new(false))
                .await
                .unwrap();
            assert!(receipt.created && ids.insert(receipt.identity));
            if n == 0 {
                original = Some(receipt);
            }
        }
        let endpoint = Endpoint::parse("wss://fixture.example/collision/0").unwrap();
        let mut expected = original.unwrap();
        expected.created = false;
        assert_eq!(
            owner
                .register_relay_endpoint(&endpoint, &Cell::new(false))
                .await
                .unwrap(),
            expected
        );
        let started = Cell::new(false);
        let extra = Endpoint::parse("wss://fixture.example/collision/64").unwrap();
        assert_eq!(
            owner.register_relay_endpoint(&extra, &started).await,
            Err(RelayRegistryError::Capacity)
        );
        assert!(!started.get());
        assert_eq!(
            owner
                .register_relay_endpoint(&endpoint, &Cell::new(false))
                .await,
            Err(RelayRegistryError::Unavailable)
        );
        drop(owner);
        driver.await.unwrap().unwrap();
        println!(
            "{}",
            serde_json::json!({"status":"PASS","case":"collision","distinct_ids":ids.len(),"original_id":expected.identity.to_string()})
        );
        return;
    }
    assert_eq!(mode, "basic");
    let original = Endpoint::parse("WSS://Fixture.Example:443/a/%7e?b=2&a=1").unwrap();
    assert_eq!(original.as_str(), "wss://fixture.example/a/~?b=2&a=1");
    let started = Cell::new(false);
    let first = owner
        .register_relay_endpoint(&original, &started)
        .await
        .unwrap();
    assert!(started.get() && first.created);
    let replay = owner
        .register_relay_endpoint(
            &Endpoint::parse("wss://fixture.example/a/~?b=2&a=1").unwrap(),
            &Cell::new(false),
        )
        .await
        .unwrap();
    assert!(!replay.created);
    assert_eq!(first.identity, replay.identity);
    assert_eq!(first.registered_at_us, replay.registered_at_us);
    let mut ids = vec![first.identity.to_string()];
    for url in [
        "wss://fixture.example/a/~?a=1&b=2",
        "ws://fixture.example/a/~?b=2&a=1",
        "wss://fixture.example/b/~?b=2&a=1",
    ] {
        let result = owner
            .register_relay_endpoint(&Endpoint::parse(url).unwrap(), &Cell::new(false))
            .await
            .unwrap();
        assert!(result.created);
        assert!(!ids.contains(&result.identity.to_string()));
        ids.push(result.identity.to_string());
    }
    let mut long = String::from("wss://fixture.example/");
    for n in 0..256_u32 {
        use std::fmt::Write;
        for byte in Sha256::digest(n.to_be_bytes()) {
            write!(&mut long, "{byte:02x}").unwrap();
        }
    }
    long.truncate(8192);
    let endpoint = Endpoint::parse(&long).unwrap();
    assert_eq!(endpoint.as_str().len(), 8192);
    let long_result = owner
        .register_relay_endpoint(&endpoint, &Cell::new(false))
        .await
        .unwrap();
    assert!(long_result.created);
    assert!(!ids.contains(&long_result.identity.to_string()));
    owner.release().await.unwrap();
    driver.await.unwrap().unwrap();

    let (client, connection) = config.connect(NoTls).await.unwrap();
    let driver = tokio::spawn(connection);
    let (mut successor, _) =
        ControlSession::acquire(client, claim(102), validate, &Cell::new(false))
            .await
            .unwrap();
    let recovered = successor
        .register_relay_endpoint(&original, &Cell::new(false))
        .await
        .unwrap();
    assert!(!recovered.created);
    let mut expected = first;
    expected.created = false;
    assert_eq!(recovered, expected);
    successor.release().await.unwrap();
    driver.await.unwrap().unwrap();
    println!(
        "{}",
        serde_json::json!({"status":"PASS", "short_ids":ids,"long_id":long_result.identity.to_string(),"registered_at_us":first.registered_at_us})
    );
}
