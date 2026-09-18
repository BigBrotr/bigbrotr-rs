//! Isolated real-adapter acceptance client; no product startup/authentication API.
use bigbrotr::storage::postgres::{
    control_owner::{ClaimRequest, ControlSession, OwnerError},
    ingest_registry::{
        InitialRegistration, RegistrationAttempt, RegistrationObservation, RegistryError,
    },
};
use bigbrotr_model::{
    AllocationDomainId, DatasetId, DictionaryKind, Digest256, OwnerResourceId, ProcessIncarnation,
    ProducerSlotId,
};
use std::{cell::Cell, io::Write, time::Duration};
use tokio::task::JoinHandle;
use tokio_postgres::{Client, Config, NoTls};

type Driver = JoinHandle<Result<(), tokio_postgres::Error>>;
async fn connect(config: &Config) -> (Client, Driver) {
    let (client, driver) = config.connect(NoTls).await.expect("isolated connection");
    (client, tokio::spawn(driver))
}
async fn join(driver: Driver) {
    // Broken wires are expected in the backend-termination case; actual join is
    // mandatory, not interpreted as a successful transaction or rollback.
    let _ = tokio::time::timeout(Duration::from_secs(5), driver)
        .await
        .expect("driver termination")
        .expect("driver task");
}
fn validate(document: &str, digest: [u8; 32]) -> Result<(), OwnerError> {
    // Independent frozen fixture validation, not the product's root validator.
    let value: serde_json::Value =
        serde_json::from_str(document).map_err(|_| OwnerError::InvalidPolicy)?;
    let frozen: serde_json::Value = serde_json::from_str(include_str!(
        "../../../experiments/configuration-inspection/policy.json"
    ))
    .unwrap();
    if value != frozen
        || digest
            != [
                0xda, 0x36, 0xc9, 0xbd, 0xae, 0x45, 0xe2, 0x0a, 0x4a, 0x99, 0xf6, 0x1e, 0x82, 0xe3,
                0xce, 0x1f, 0x46, 0xf6, 0x35, 0x2b, 0xa0, 0xbd, 0xdb, 0xf2, 0x3b, 0x2e, 0xcf, 0x28,
                0x79, 0xd9, 0x4e, 0xa9,
            ]
    {
        Err(OwnerError::InvalidPolicy)
    } else {
        Ok(())
    }
}
fn request(dataset: DatasetId, instance: DatasetId, n: u8) -> ClaimRequest {
    ClaimRequest {
        dataset,
        instance,
        authority_mode: "managed".into(),
        role_instance_id: [n; 16],
        incarnation: ProcessIncarnation::try_from([n + 10; 16]).unwrap(),
        lease_ttl: Duration::from_secs(60),
    }
}
fn emit(value: serde_json::Value) {
    println!("{value}");
    std::io::stdout().flush().expect("bounded probe output");
}

async fn failure_boundary(config: Config, mode: &str) {
    let (client, driver) = connect(&config).await;
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
    let n = if mode == "recover" { 32 } else { 31 };
    let started = Cell::new(false);
    let acquisition =
        ControlSession::acquire(client, request(dataset, instance, n), validate, &started);
    if mode == "cancel-held" {
        assert!(
            tokio::time::timeout(Duration::from_millis(1500), acquisition)
                .await
                .is_err()
        );
        assert!(started.get());
        emit(serde_json::json!({"event":"cancelled","commit_started":true}));
        join(driver).await;
        return;
    }
    let result = acquisition.await;
    if mode == "busy" {
        assert!(matches!(result, Err(OwnerError::Busy)));
        assert!(!started.get());
        join(driver).await;
        emit(serde_json::json!({"event":"busy","commit_started":false}));
        return;
    }
    if matches!(mode, "lost-before" | "lost-after") {
        assert!(matches!(result, Err(OwnerError::CommitUnknown)));
        assert!(started.get());
        join(driver).await;
        emit(serde_json::json!({"event":"unknown","commit_started":true}));
        return;
    }
    let (mut owner, _) = result.unwrap();
    let epoch = owner.identity().epoch.get();
    if matches!(mode, "hold-live" | "hold-killed") {
        assert_eq!(epoch, 1);
        emit(serde_json::json!({"event":"holding","epoch":epoch,"ttl_seconds":60}));
        // The external driver freezes this actual process beyond its persisted
        // TTL, attempts takeover, and optionally excludes its PG backend.
        tokio::time::sleep(Duration::from_secs(70)).await;
        if mode == "hold-killed" {
            assert!(owner.refresh(validate).await.is_err());
            assert_eq!(owner.refresh(validate).await, Err(OwnerError::Unavailable));
            drop(owner);
        } else {
            owner.refresh(validate).await.unwrap();
            owner.release().await.unwrap();
        }
    } else if mode == "recover" {
        owner.refresh(validate).await.unwrap();
        owner.release().await.unwrap();
    } else {
        assert_eq!(mode, "claim");
        // Intentional abrupt client lifetime: history remains active so another
        // process must establish exclusion and advance the epoch on recovery.
        drop(owner);
    }
    join(driver).await;
    emit(serde_json::json!({"event":"complete","mode":mode,"epoch":epoch}));
}

async fn run(config: Config, mode: &str) {
    let (observer, observer_driver) = connect(&config).await;
    let row = observer
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
    let started = Cell::new(false);
    if mode != "lifecycle" {
        let (client, driver) = connect(&config).await;
        let mut req = request(dataset, instance, 31);
        match mode {
            "wrong-dataset" => req.dataset = DatasetId::try_from([99; 16]).unwrap(),
            "wrong-instance" => req.instance = DatasetId::try_from([99; 16]).unwrap(),
            "wrong-mode" => req.authority_mode = "declarative".into(),
            "no-policy" | "invalid-policy" => (),
            _ => panic!("unknown mode"),
        }
        let error = match ControlSession::acquire(client, req, validate, &started).await {
            Ok(_) => panic!("unexpected owner"),
            Err(e) => e,
        };
        assert!(!started.get());
        assert_eq!(
            error,
            if mode.starts_with("wrong-") && mode != "wrong-mode" {
                OwnerError::IncompatibleDeployment
            } else {
                OwnerError::InvalidPolicy
            }
        );
        join(driver).await;
        println!(
            "{}",
            serde_json::json!({"mode":mode,"result":"refused","reason":format!("{error:?}")})
        );
    } else {
        let (first_client, first_driver) = connect(&config).await;
        let first_pid: i32 = first_client
            .query_one("SELECT pg_backend_pid()", &[])
            .await
            .unwrap()
            .get(0);
        let (mut first, policy) = ControlSession::acquire(
            first_client,
            request(dataset, instance, 31),
            validate,
            &started,
        )
        .await
        .unwrap();
        assert!(started.get());
        assert_eq!((policy.config_revision, policy.policy_revision), (1, 1));
        let old = first.identity();
        assert_eq!(old.epoch.get(), 1);
        let (competitor, competitor_driver) = connect(&config).await;
        assert!(matches!(
            ControlSession::acquire(
                competitor,
                request(dataset, instance, 32),
                validate,
                &Cell::new(false)
            )
            .await,
            Err(OwnerError::Busy)
        ));
        join(competitor_driver).await;
        let forged = observer.query_one("SELECT control.require_control_owner($1::bytea, $2::bytea, '1'::numeric, $3::bytea)", &[&&dataset.as_bytes()[..], &&old.resource_id.as_bytes()[..], &&old.process_incarnation.as_bytes()[..]]).await.unwrap_err();
        assert_eq!(forged.code().unwrap().code(), "23514");
        assert_eq!(first.refresh(validate).await.unwrap(), policy);
        assert!(
            observer
                .query_one("SELECT pg_terminate_backend($1, 5000)", &[&first_pid])
                .await
                .unwrap()
                .get::<_, bool>(0)
        );
        join(first_driver).await;
        let (second_client, second_driver) = connect(&config).await;
        let (mut second, _) = ControlSession::acquire(
            second_client,
            request(dataset, instance, 32),
            validate,
            &Cell::new(false),
        )
        .await
        .unwrap();
        assert_eq!(second.identity().resource_id, old.resource_id);
        assert_eq!(second.identity().epoch.get(), 2);
        assert!(first.refresh(validate).await.is_err());
        assert_eq!(first.refresh(validate).await, Err(OwnerError::Unavailable));
        drop(first);
        assert_eq!(second.refresh(validate).await.unwrap(), policy);
        second.release().await.unwrap();
        join(second_driver).await;
        let (third_client, third_driver) = connect(&config).await;
        let (mut third, _) = ControlSession::acquire(
            third_client,
            request(dataset, instance, 33),
            validate,
            &Cell::new(false),
        )
        .await
        .unwrap();
        assert_eq!(third.identity().resource_id, old.resource_id);
        assert_eq!(third.identity().epoch.get(), 3);
        let spec = InitialRegistration {
            dataset_id: dataset,
            instance_id: *instance.as_bytes(),
            control: third.identity(),
            ingest_resource_id: OwnerResourceId::try_from([61; 16]).unwrap(),
            ingest_role_instance_id: [62; 16],
            process_incarnation: ProcessIncarnation::try_from([63; 16]).unwrap(),
            shard_id: 0,
            config_revision: 1,
            policy_revision: 1,
            config_digest: Digest256::from_bytes(policy.config_digest),
            policy_digest: Digest256::from_bytes(policy.policy_digest),
            previous_topology: None,
            allocation_domains: vec![
                (
                    DictionaryKind::VisibilityScope,
                    AllocationDomainId::try_from([64; 16]).unwrap(),
                ),
                (
                    DictionaryKind::Event,
                    AllocationDomainId::try_from([65; 16]).unwrap(),
                ),
            ],
            producer_slot_id: ProducerSlotId::try_from([66; 16]).unwrap(),
            outstanding_request_limit: 2,
            created_at_us: 100,
            lease_expires_at_us: 60_000_100,
        };
        let (mut copied_connection, copied_driver) = connect(&config).await;
        let mut copied = RegistrationAttempt::new(spec.clone()).unwrap();
        assert_eq!(
            copied.register(&mut copied_connection).await,
            Err(RegistryError::ControlMismatch)
        );
        drop(copied_connection);
        join(copied_driver).await;
        let mut attempt = RegistrationAttempt::new(spec).unwrap();
        assert_eq!(
            third.register_ingest(&mut attempt).await,
            Ok(RegistrationObservation::Matching { current: true })
        );
        third.release().await.unwrap();
        join(third_driver).await;
        println!(
            "{}",
            serde_json::json!({"mode":mode,"result":"pass","control_epochs":3,"copied_backend_refused":true,"stale_backend_refused":true,"ingest_registered":true})
        );
    }
    drop(observer);
    join(observer_driver).await;
}
fn main() {
    let args: Vec<_> = std::env::args().collect();
    assert!((4..=5).contains(&args.len()));
    let mut config = Config::new();
    config
        .host(if args.len() == 5 {
            "127.0.0.1"
        } else {
            "postgres"
        })
        .dbname(&args[2])
        .user("bigbrotr_control")
        .password(std::fs::read(&args[3]).expect("protected lab credential"))
        .connect_timeout(Duration::from_secs(5))
        .application_name("bigbrotr-control-owner-probe");
    if let Some(port) = args.get(4) {
        config.port(port.parse().expect("isolated proxy port"));
    }
    let boundary = matches!(
        args[1].as_str(),
        "claim"
            | "recover"
            | "busy"
            | "lost-before"
            | "lost-after"
            | "cancel-held"
            | "hold-live"
            | "hold-killed"
    );
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            tokio::time::timeout(Duration::from_secs(180), async {
                if boundary {
                    failure_boundary(config, &args[1]).await;
                } else {
                    run(config, &args[1]).await;
                }
            })
            .await
            .expect("finite probe deadline")
        });
}
