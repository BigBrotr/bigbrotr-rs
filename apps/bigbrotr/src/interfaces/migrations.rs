use bigbrotr::{
    runtime::{ProcessEnvelope, ResourceBudget, ResourceLimits, ResourceRequest, WaiterLimits},
    secrets::SecretFile,
    storage::postgres::{
        connection_settings::{
            ConnectionSettings, PlaintextBoundary, PreparedConnection, PreparedTransport,
            TlsSettings,
        },
        migration_runner::{self, RunError, RunResult},
        migrations,
    },
};
use clap::{Arg, ArgMatches, Command};
use serde_json::{Value, json};
use std::{
    cell::Cell,
    future::Future,
    io::Write,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::Path,
    pin::Pin,
    process::ExitCode,
    time::Duration,
};

const CONNECT_DEADLINE: Duration = Duration::from_secs(10);
const OPERATION_DEADLINE: Duration = Duration::from_secs(60);
const DRIVER_DRAIN: Duration = Duration::from_secs(2);

pub fn command() -> Command {
    let mut command = Command::new("migrate")
        .about("Explicit PostgreSQL control-schema inspection and migration")
        .after_help("Control schema only; no dataset initialization or engine upgrade. Apply requires separate migration credentials. Never infer rollback from a lost COMMIT response.")
        .subcommand_required(true).disable_help_subcommand(true);
    for (action, description) in [
        (
            "inspect",
            "Inspect the current control ledger without durable changes",
        ),
        (
            "plan",
            "Inspect and report the exact forward control-schema steps",
        ),
        (
            "apply",
            "Apply the exact control schema and receipt in one transaction",
        ),
    ] {
        command = command.subcommand(
            Command::new(action)
                .about(description)
                .arg(
                    Arg::new("file")
                        .long("file")
                        .required(true)
                        .value_name("CONFIG"),
                )
                .arg(
                    Arg::new("user")
                        .long("user")
                        .required(true)
                        .value_name("DATABASE_ROLE"),
                )
                .arg(
                    Arg::new("password-ref")
                        .long("password-ref")
                        .required(true)
                        .value_name("FILE_REFERENCE"),
                )
                .arg(
                    Arg::new("target")
                        .long("target")
                        .value_parser(clap::value_parser!(u32).range(1..))
                        .value_name("SCHEMA_VERSION"),
                ),
        );
    }
    command
}

type CliResult<T> = Result<T, (u8, String)>;
pub fn execute(matches: &ArgMatches) -> ExitCode {
    match run(matches) {
        Ok((report, code)) => {
            let mut stdout = std::io::stdout().lock();
            if writeln!(
                stdout,
                "{}",
                serde_json::to_string_pretty(&report).expect("finite migration metadata")
            )
            .and_then(|()| stdout.flush())
            .is_err()
            {
                eprintln!(
                    "error: could not write migration result; inspect the same target before retry"
                );
                return ExitCode::FAILURE;
            }
            ExitCode::from(code)
        }
        Err((code, reason)) => {
            eprintln!("error: {reason}");
            ExitCode::from(code)
        }
    }
}

fn run(matches: &ArgMatches) -> CliResult<(Value, u8)> {
    let (action, args) = matches.subcommand().expect("required migration command");
    let config = crate::config::load(Path::new(args.get_one::<String>("file").unwrap()))?;
    let target = args.get_one::<u32>("target").copied().unwrap_or_else(|| {
        migrations::catalog()
            .last()
            .expect("nonempty migration catalog")
            .version
    });
    migrations::plan(&[], target).map_err(|e| (3, e.to_string()))?;
    let user = args.get_one::<String>("user").unwrap();
    if action == "apply" && config.text("postgres.control.user") == Some(user.as_str()) {
        return Err((3, "apply requires a dedicated migration role, separate from the configured application role".into()));
    }
    let password = secret(args.get_one::<String>("password-ref").unwrap())?;
    let present = |key: &str| {
        config
            .text(key)
            .ok_or_else(|| (3, format!("{key}: required for explicit control migration")))
    };
    let tls = match present("postgres.control.tls.mode")? {
        "verify-full" => TlsSettings::VerifyFull {
            ca_file: Path::new(present("postgres.control.tls.ca_file")?),
            server_name: present("postgres.control.tls.server_name")?,
            client_certificate: config
                .text("postgres.control.tls.client_cert_ref")
                .map(secret)
                .transpose()?,
            client_private_key: config
                .text("postgres.control.tls.client_key_ref")
                .map(secret)
                .transpose()?,
        },
        "local-plaintext" => TlsSettings::LocalPlaintext {
            boundary: match present("postgres.control.tls.local_boundary")? {
                "loopback" => PlaintextBoundary::Loopback,
                "private-bridge" => PlaintextBoundary::PrivateBridge,
                _ => return Err((3, "invalid control database plaintext boundary".into())),
            },
        },
        _ => return Err((3, "invalid control database TLS mode".into())),
    };
    let settings = ConnectionSettings {
        endpoint: present("postgres.control.endpoint")?,
        database: present("postgres.control.database")?,
        user,
        password,
        connect_timeout: Duration::from_secs(5),
        tls,
    };
    // Conservative initial per-command allowance, not a measured RSS ceiling.
    // This finite command admits one connection and one transaction at a time.
    let input_bytes = u64::try_from(settings.preparation_input_reservation_bytes())
        .map_err(|_| (3, "credential reservation exceeds platform range".into()))?;
    let reservation = input_bytes
        .checked_mul(8)
        .and_then(|n| n.checked_add(16 * 1024 * 1024))
        .ok_or_else(|| (3, "migration memory reservation overflow".into()))?;
    let number = |name: &str| config.number(&format!("runtime.processes.application.{name}"));
    let envelope = ProcessEnvelope::new(
        number("memory_max_bytes"),
        number("baseline_bytes"),
        ResourceLimits::new(
            number("payload_bytes"),
            1,
            number("native_bytes"),
            number("completion_reserve_bytes"),
        ),
        WaiterLimits::new(0, 0),
    )
    .map_err(|_| (3, "invalid application memory envelope".into()))?;
    let budget = ResourceBudget::new(envelope);
    let _grant = budget
        .try_acquire(ResourceRequest::data(reservation, 1, 0).expect("positive reservation"))
        .map_err(|_| {
            (
                3,
                "migration preparation does not fit the declared payload grant".into(),
            )
        })?;
    let prepared = settings.prepare().map_err(|error| (3, error.to_string()))?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| (1, "cannot start finite migration executor".into()))?;
    let result = runtime.block_on(execute_connection(prepared, target, action == "apply"));
    // This is a finite executable command. An uncancellable resolver can outlive
    // this runtime grace period only until this process exits; no new work is
    // accepted and no background daemon receives recycled credits here.
    runtime.shutdown_timeout(DRIVER_DRAIN);
    match result {
        Ok(result) => Ok((report(action, result), 0)),
        Err(RunError::CommitUnknown) => Ok((
            json!({
                "format":"bigbrotr-control-migration/1", "scope":"postgres-control",
                "status":"outcome-unknown", "target_version":target.to_string(),
                "resolution":"inspect or reapply this exact target under the migration lock; no rollback inferred"
            }),
            4,
        )),
        Err(error) => Err((1, error.to_string())),
    }
}

fn secret(reference: &str) -> CliResult<SecretFile<'_>> {
    SecretFile::parse(reference)
        .map_err(|_| (3, "invalid protected migration credential reference".into()))
}

type Driver = Pin<Box<dyn Future<Output = Result<(), tokio_postgres::Error>> + Send>>;
async fn execute_connection(
    prepared: PreparedConnection,
    target: u32,
    apply: bool,
) -> Result<RunResult, RunError> {
    let (mut config, transport) = prepared.into_parts();
    let connect = async {
        let pair: (tokio_postgres::Client, Driver) = match transport {
            PreparedTransport::LocalPlaintext(boundary) => {
                pin_plaintext_route(&mut config, boundary).await?;
                let (client, driver) = config
                    .connect(tokio_postgres::NoTls)
                    .await
                    .map_err(|_| RunError::Backend)?;
                (client, Box::pin(driver))
            }
            PreparedTransport::VerifyFull(connector) => {
                let (client, driver) = config
                    .connect(connector)
                    .await
                    .map_err(|_| RunError::Backend)?;
                (client, Box::pin(driver))
            }
        };
        Ok::<_, RunError>(pair)
    };
    let (mut client, driver) = tokio::time::timeout(CONNECT_DEADLINE, connect)
        .await
        .map_err(|_| RunError::Backend)??;
    let mut driver = tokio::spawn(driver);
    let commit_started = Cell::new(false);
    let result = match tokio::time::timeout(
        OPERATION_DEADLINE,
        migration_runner::run(&mut client, target, apply, &commit_started),
    )
    .await
    {
        Ok(result) => result,
        Err(_) if commit_started.get() => Err(RunError::CommitUnknown),
        Err(_) => Err(RunError::Backend),
    };
    drop(client);
    if tokio::time::timeout(DRIVER_DRAIN, &mut driver)
        .await
        .is_err()
    {
        driver.abort();
        let _ = driver.await;
    }
    // Joining this local driver closes local ownership only. A failed COMMIT
    // remains unknown until the next serialized ledger inspection resolves it.
    result
}

async fn pin_plaintext_route(
    config: &mut tokio_postgres::Config,
    boundary: PlaintextBoundary,
) -> Result<(), RunError> {
    let [tokio_postgres::config::Host::Tcp(host)] = config.get_hosts() else {
        return Err(RunError::PlaintextRoute);
    };
    let [port] = config.get_ports() else {
        return Err(RunError::PlaintextRoute);
    };
    let address = if host == "localhost" {
        // Deterministic loopback without an ambient resolver alias. An IPv6-only
        // database can explicitly select [::1] in its endpoint.
        vetted_address(
            [SocketAddr::new(Ipv4Addr::LOCALHOST.into(), *port)],
            boundary,
        )?
    } else if let Ok(address) = host.parse::<IpAddr>() {
        vetted_address([SocketAddr::new(address, *port)], boundary)?
    } else {
        let addresses = tokio::net::lookup_host((host.as_str(), *port))
            .await
            .map_err(|_| RunError::Backend)?;
        vetted_address(addresses, boundary)?
    };
    // hostaddr prevents a second DNS resolution between policy and connect.
    config.hostaddr(address);
    Ok(())
}

fn vetted_address(
    addresses: impl IntoIterator<Item = SocketAddr>,
    boundary: PlaintextBoundary,
) -> Result<IpAddr, RunError> {
    let mut selected = None;
    for (index, address) in addresses.into_iter().enumerate() {
        let ip = address.ip().to_canonical();
        let allowed = match boundary {
            PlaintextBoundary::Loopback => ip.is_loopback(),
            PlaintextBoundary::PrivateBridge => match ip {
                IpAddr::V4(ip) => ip.is_private(),
                IpAddr::V6(ip) => ip.is_unique_local(),
            },
        };
        if index >= 16 || !allowed {
            return Err(RunError::PlaintextRoute);
        }
        selected.get_or_insert(ip);
    }
    selected.ok_or(RunError::PlaintextRoute)
}

fn report(action: &str, result: RunResult) -> Value {
    let current = if result.applied > 0 {
        Some(result.plan.target)
    } else {
        result.plan.from
    };
    json!({
        "format":"bigbrotr-control-migration/1", "scope":"postgres-control",
        "status":match action { "inspect" => "inspected", "plan" => "planned", _ if result.applied > 0 => "applied", _ => "current" },
        "source_version":result.plan.from.map(|n| n.to_string()),
        "current_version":current.map(|n| n.to_string()),
        "target_version":result.plan.target.to_string(), "applied_count":result.applied.to_string(),
        "steps":result.plan.pending.iter().map(|step| json!({"version":step.version.to_string(),"name":step.name,"sha256":step.checksum().iter().map(|b|format!("{b:02x}")).collect::<String>()})).collect::<Vec<_>>(),
        "dataset_initialization":"not-performed", "release_upgrade_qualification":"not-performed"
    })
}

#[cfg(test)]
mod route_tests {
    use super::*;

    #[test]
    fn plaintext_requires_all_bounded_answers_inside_the_selected_boundary() {
        for (boundary, allowed, forbidden) in [
            (PlaintextBoundary::Loopback, "127.0.0.1", "10.0.0.2"),
            (PlaintextBoundary::PrivateBridge, "172.18.0.2", "8.8.8.8"),
            (PlaintextBoundary::PrivateBridge, "fd00::2", "fe80::2"),
        ] {
            let local = SocketAddr::new(allowed.parse().unwrap(), 5432);
            let external = SocketAddr::new(forbidden.parse().unwrap(), 5432);
            assert_eq!(vetted_address([local], boundary).unwrap(), local.ip());
            assert!(vetted_address([local, external], boundary).is_err());
            assert!(vetted_address([external, local], boundary).is_err());
            assert!(vetted_address([], boundary).is_err());
            assert!(vetted_address([local; 16], boundary).is_ok());
            assert!(vetted_address([local; 17], boundary).is_err());
        }
        let mapped = SocketAddr::new("::ffff:127.0.0.1".parse().unwrap(), 5432);
        assert_eq!(
            vetted_address([mapped], PlaintextBoundary::Loopback).unwrap(),
            IpAddr::V4(Ipv4Addr::LOCALHOST)
        );
        assert!(vetted_address([mapped], PlaintextBoundary::PrivateBridge).is_err());
    }
}
