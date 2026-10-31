//! Isolated PostgreSQL acceptance client, not a product command or owner grant.
//! Uses synthetic identities and an already migrated/seeded disposable database.
use bigbrotr::storage::postgres::ingest_registry::{
    self, AttemptState, ControlOwner, InitialRegistration, RegistrationAttempt,
    RegistrationObservation as Observation, RegistryError,
};
use bigbrotr_model::{
    AllocationDomainId, DatasetId, DictionaryKind, Digest256, OwnerEpoch, OwnerResourceId,
    ProcessIncarnation, ProducerSlotId,
};
use std::{io::Write, path::Path, time::Duration};
use tokio::{task::JoinHandle, time::timeout};
use tokio_postgres::{Client, Config, NoTls};

type ProbeResult<T> = Result<T, &'static str>;

fn specification() -> InitialRegistration {
    InitialRegistration {
        dataset_id: DatasetId::try_from([1; 16]).unwrap(),
        instance_id: [2; 16],
        control: ControlOwner {
            resource_id: OwnerResourceId::try_from([3; 16]).unwrap(),
            epoch: OwnerEpoch::new(1).unwrap(),
            role_instance_id: [4; 16],
            process_incarnation: ProcessIncarnation::try_from([5; 16]).unwrap(),
        },
        ingest_resource_id: OwnerResourceId::try_from([6; 16]).unwrap(),
        ingest_role_instance_id: [7; 16],
        process_incarnation: ProcessIncarnation::try_from([5; 16]).unwrap(),
        shard_id: u32::MAX,
        config_revision: 1,
        policy_revision: 1,
        config_digest: Digest256::from_bytes([8; 32]),
        policy_digest: Digest256::from_bytes([9; 32]),
        previous_topology: None,
        allocation_domains: vec![
            (
                DictionaryKind::VisibilityScope,
                AllocationDomainId::try_from([10; 16]).unwrap(),
            ),
            (
                DictionaryKind::Event,
                AllocationDomainId::try_from([11; 16]).unwrap(),
            ),
        ],
        producer_slot_id: ProducerSlotId::try_from([12; 16]).unwrap(),
        outstanding_request_limit: 2,
        request_receipt_retention: 64,
        created_at_us: 100,
        lease_expires_at_us: 60_000_100,
    }
}

fn emit(event: &str, result: &str) {
    println!(
        "{}",
        serde_json::json!({"format":"bigbrotr-ingest-registry-probe/1", "event":event, "result":result})
    );
    std::io::stdout().flush().expect("probe output unavailable");
}

struct Connection {
    client: Client,
    driver: JoinHandle<Result<(), tokio_postgres::Error>>,
}
impl Connection {
    async fn open(base: &Config, port: u16) -> ProbeResult<Self> {
        let mut config = base.clone();
        config.port(port);
        let (client, connection) = timeout(Duration::from_secs(10), config.connect(NoTls))
            .await
            .map_err(|_| "connect-timeout")?
            .map_err(|_| "connect-failed")?;
        let driver = tokio::spawn(connection);
        Ok(Self { client, driver })
    }
    async fn close(self) -> ProbeResult<()> {
        drop(self.client);
        // A deliberately broken wire may return an error; joining is still
        // required and does not establish the database transaction's outcome.
        timeout(Duration::from_secs(5), self.driver)
            .await
            .map_err(|_| "driver-not-terminal")?
            .map_err(|_| "driver-panicked")?
            .ok();
        Ok(())
    }
}

async fn run(mode: &str, base: Config, port: u16) -> ProbeResult<()> {
    let spec = specification();
    let mut connection = Connection::open(&base, port).await?;
    let mut attempt = RegistrationAttempt::new(spec.clone()).map_err(|_| "invalid-fixture")?;
    match mode {
        "absent" => {
            if ingest_registry::inspect(&mut connection.client, &spec).await
                != Ok(Observation::Absent)
            {
                return Err("expected-absent");
            }
        }
        "rejects" => {
            let mut wrong = spec.clone();
            wrong.instance_id = [99; 16];
            if ingest_registry::inspect(&mut connection.client, &wrong).await
                != Err(RegistryError::WrongDeployment)
            {
                return Err("wrong-deployment-not-refused");
            }
            wrong = spec.clone();
            wrong.control.epoch = OwnerEpoch::new(u64::MAX).unwrap();
            if ingest_registry::inspect(&mut connection.client, &wrong).await
                != Err(RegistryError::ControlMismatch)
            {
                return Err("full-u64-control-mismatch-not-refused");
            }
            wrong = spec.clone();
            wrong.policy_digest = Digest256::from_bytes([99; 32]);
            if ingest_registry::inspect(&mut connection.client, &wrong).await
                != Err(RegistryError::HeadsChanged)
            {
                return Err("policy-digest-not-refused");
            }
            let row = connection
                .client
                .query_one("SELECT $1::text::numeric::text", &[&u64::MAX.to_string()])
                .await
                .map_err(|_| "numeric-roundtrip-failed")?;
            if row.get::<_, &str>(0) != "18446744073709551615" {
                return Err("numeric-narrowed");
            }
        }
        "register" | "repeat" => {
            if attempt.register(&mut connection.client).await
                != Ok(Observation::Matching { current: true })
            {
                return Err("registration-not-matching");
            }
            if attempt.state() != AttemptState::Complete
                || attempt.register(&mut connection.client).await
                    != Err(RegistryError::AttemptUnresolved)
            {
                return Err("completed-attempt-reused");
            }
            if ingest_registry::inspect(&mut connection.client, &spec).await
                != Ok(Observation::Matching { current: true })
            {
                return Err("committed-registry-not-observed");
            }
        }
        "conflict" => {
            let mut changed = spec.clone();
            changed.outstanding_request_limit = 3;
            let mut changed = RegistrationAttempt::new(changed).unwrap();
            if changed.register(&mut connection.client).await != Ok(Observation::Conflict) {
                return Err("changed-registration-not-conflict");
            }
        }
        "migration-role" => {
            if attempt.register(&mut connection.client).await
                != Err(RegistryError::MigrationPrivileges)
            {
                return Err("migration-role-not-refused");
            }
        }
        "stale" => {
            if ingest_registry::inspect(&mut connection.client, &spec).await
                != Ok(Observation::Matching { current: false })
            {
                return Err("stale-registry-not-observed");
            }
        }
        "lost-before" | "lost-after" | "cancel-held" => {
            if mode == "cancel-held" {
                if timeout(
                    Duration::from_millis(1500),
                    attempt.register(&mut connection.client),
                )
                .await
                .is_ok()
                {
                    return Err("held-registration-did-not-time-out");
                }
            } else if attempt.register(&mut connection.client).await
                != Err(RegistryError::CommitUnknown)
            {
                return Err("lost-commit-not-unknown");
            }
            if attempt.state() != AttemptState::CommitDispatched {
                return Err("commit-dispatch-state-lost");
            }
            if attempt.register(&mut connection.client).await
                != Err(RegistryError::AttemptUnresolved)
            {
                return Err("unresolved-attempt-retried");
            }
            emit("commit-unknown", "observed");
            let mut fresh = Connection::open(&base, 5432).await?;
            if mode == "cancel-held" {
                // The external driver must observe this resolver actually
                // waiting on the old transaction's lock, then release/close
                // the held COMMIT. No elapsed-time inference supplies absence.
                emit("resolving-held", "started");
            }
            let observed = attempt
                .resolve(&mut fresh.client)
                .await
                .map_err(|_| "resolution-failed")?;
            let expected = if mode == "lost-before" {
                Observation::Absent
            } else {
                Observation::Matching { current: true }
            };
            if observed != expected {
                return Err("resolved-outcome-mismatch");
            }
            if observed == Observation::Absent
                && attempt.register(&mut fresh.client).await
                    != Ok(Observation::Matching { current: true })
            {
                return Err("same-identity-retry-failed");
            }
            fresh.close().await?;
        }
        _ => return Err("unknown-mode"),
    }
    connection.close().await?;
    emit(mode, "PASS");
    Ok(())
}

fn main() {
    let result = (|| -> ProbeResult<()> {
        let args: Vec<_> = std::env::args().collect();
        if args.len() != 6 {
            return Err("arguments");
        }
        let mode = &args[1];
        let database = &args[2];
        if !database.starts_with("bb_registry_")
            || database.len() > 63
            || !database
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        {
            return Err("not-a-registry-lab-database");
        }
        let port: u16 = args[3].parse().map_err(|_| "port")?;
        let role = match args[4].as_str() {
            "application" => "bigbrotr_control",
            "migrator" => "bigbrotr_migrator",
            _ => return Err("role"),
        };
        let secret = Path::new(&args[5]);
        let metadata = secret.metadata().map_err(|_| "secret-metadata")?;
        if !metadata.is_file() || metadata.len() != 64 {
            return Err("synthetic-secret-shape");
        }
        let password = std::fs::read(secret).map_err(|_| "secret-read")?;
        if password.len() != 64 || !password.iter().all(u8::is_ascii_hexdigit) {
            return Err("synthetic-secret-shape");
        }
        let mut config = Config::new();
        config
            .host("127.0.0.1")
            .user(role)
            .password(password)
            .dbname(database)
            .application_name("bigbrotr-registry-probe")
            .connect_timeout(Duration::from_secs(5));
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| "runtime")?;
        runtime.block_on(async {
            timeout(Duration::from_secs(40), run(mode, config, port))
                .await
                .map_err(|_| "probe-deadline")?
        })
    })();
    if let Err(reason) = result {
        emit("failure", reason);
        std::process::exit(3);
    }
}
