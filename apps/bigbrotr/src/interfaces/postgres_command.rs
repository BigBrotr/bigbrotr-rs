//! Shared finite PostgreSQL command preparation and owned transport lifecycle.
use bigbrotr::{
    runtime::{
        OwnedPermit, ProcessEnvelope, ResourceBudget, ResourceLimits, ResourceRequest, WaiterLimits,
    },
    secrets::SecretFile,
    storage::postgres::connection_settings::{
        ConnectionSettings, PlaintextBoundary, PreparedConnection, PreparedTransport, TlsSettings,
    },
};
use std::{
    future::Future,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::Path,
    pin::Pin,
    time::Duration,
};
use tokio_postgres::Client;
const CONNECT_DEADLINE: Duration = Duration::from_secs(10);
pub(super) const DRIVER_DRAIN: Duration = Duration::from_secs(2);
type CliResult<T> = Result<T, (u8, String)>;
type Driver = Pin<Box<dyn Future<Output = Result<(), tokio_postgres::Error>> + Send>>;
pub(super) type DriverHandle = tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>;
#[derive(Debug)]
pub(super) enum ConnectError {
    Backend,
    PlaintextRoute,
}
impl std::fmt::Display for ConnectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Backend => "PostgreSQL connection unavailable",
            Self::PlaintextRoute => "resolved PostgreSQL plaintext route is outside its declared local boundary or exceeds the address limit",
        })
    }
}
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
    let (mut config, transport) = prepared.into_parts();
    let connect = async {
        let pair: (tokio_postgres::Client, Driver) = match transport {
            PreparedTransport::LocalPlaintext(boundary) => {
                pin_plaintext_route(&mut config, boundary).await?;
                let (client, driver) = config
                    .connect(tokio_postgres::NoTls)
                    .await
                    .map_err(|_| ConnectError::Backend)?;
                (client, Box::pin(driver))
            }
            PreparedTransport::VerifyFull(connector) => {
                let (client, driver) = config
                    .connect(connector)
                    .await
                    .map_err(|_| ConnectError::Backend)?;
                (client, Box::pin(driver))
            }
        };
        Ok::<_, ConnectError>(pair)
    };
    let (client, driver) = tokio::time::timeout(CONNECT_DEADLINE, connect)
        .await
        .map_err(|_| ConnectError::Backend)??;
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

async fn pin_plaintext_route(
    config: &mut tokio_postgres::Config,
    boundary: PlaintextBoundary,
) -> Result<(), ConnectError> {
    let [tokio_postgres::config::Host::Tcp(host)] = config.get_hosts() else {
        return Err(ConnectError::PlaintextRoute);
    };
    let [port] = config.get_ports() else {
        return Err(ConnectError::PlaintextRoute);
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
            .map_err(|_| ConnectError::Backend)?;
        vetted_address(addresses, boundary)?
    };
    // hostaddr prevents a second DNS resolution between policy and connect.
    config.hostaddr(address);
    Ok(())
}

fn vetted_address(
    addresses: impl IntoIterator<Item = SocketAddr>,
    boundary: PlaintextBoundary,
) -> Result<IpAddr, ConnectError> {
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
            return Err(ConnectError::PlaintextRoute);
        }
        selected.get_or_insert(ip);
    }
    selected.ok_or(ConnectError::PlaintextRoute)
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
