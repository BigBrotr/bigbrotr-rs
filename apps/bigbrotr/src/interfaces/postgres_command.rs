//! Shared finite PostgreSQL command preparation and owned transport lifecycle.
use bigbrotr::storage::postgres::transport;
use bigbrotr::{
    runtime::{
        OwnedPermit, ProcessEnvelope, ResourceBudget, ResourceLimits, ResourceRequest, WaiterLimits,
    },
    secrets::SecretFile,
    storage::postgres::connection_settings::{
        ConnectionSettings, PlaintextBoundary, PreparedConnection, TlsSettings,
    },
};
use std::{path::Path, time::Duration};
use tokio_postgres::Client;
pub(super) use transport::ConnectError;
pub(super) const DRIVER_DRAIN: Duration = Duration::from_secs(2);
type CliResult<T> = Result<T, (u8, String)>;
pub(super) type DriverHandle = tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>;
pub(super) fn prepare(
    config: &crate::config::Resolved,
    user: &str,
    password_reference: &str,
    extra_reservation: u64,
) -> CliResult<(PreparedConnection, OwnedPermit)> {
    let password = secret(password_reference)?;
    let present = |key: &str| {
        config
            .text(key)
            .ok_or_else(|| (3, format!("{key}: required for explicit Control command")))
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
        .and_then(|n| n.checked_add(extra_reservation))
        .ok_or_else(|| (3, "Control command memory reservation overflow".into()))?;
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
    let grant = budget
        .try_acquire(ResourceRequest::data(reservation, 1, 0).expect("positive reservation"))
        .map_err(|_| {
            (
                3,
                "Control preparation does not fit the declared payload grant".into(),
            )
        })?;
    let prepared = settings.prepare().map_err(|error| (3, error.to_string()))?;
    Ok((prepared, grant))
}

fn secret(reference: &str) -> CliResult<SecretFile<'_>> {
    SecretFile::parse(reference).map_err(|_| {
        (
            3,
            "invalid protected PostgreSQL credential reference".into(),
        )
    })
}

pub(super) async fn connect(
    prepared: PreparedConnection,
) -> Result<(Client, DriverHandle), ConnectError> {
    let (client, driver) = transport::connect(prepared).await?;
    Ok((client, tokio::spawn(driver)))
}

pub(super) async fn close(client: Client, driver: DriverHandle) {
    drop(client);
    join(driver).await;
}

/// The opaque Control owner has already dropped its client. Retain the caller's
/// resource grant through this terminal join, including timeout cancellation.
pub(super) async fn join(mut driver: DriverHandle) {
    if tokio::time::timeout(DRIVER_DRAIN, &mut driver)
        .await
        .is_err()
    {
        driver.abort();
        let _ = driver.await;
    }
}
