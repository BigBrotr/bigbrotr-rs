//! Finite connection establishment for trusted PostgreSQL infrastructure routes.
//! Returns the actual unpolled driver: its owner can attach descriptor custody
//! and resource ownership before spawning it. This module starts no driver task.
use super::connection_settings::{PlaintextBoundary, PreparedConnection, PreparedTransport};
use std::{
    future::Future,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    pin::Pin,
    time::Duration,
};
use tokio_postgres::Client;

const CONNECT_DEADLINE: Duration = Duration::from_secs(10);

/// The actual connection future, not a detachable task handle. Keep its resource
/// reservation through completion/destruction. A query timeout alone does not
/// stop queued SQL, and closing this driver does not prove a remote rollback.
/// DNS may use the runtime's blocking resolver; its process envelope remains the
/// caller's responsibility, including after a connection deadline expires.
pub type Driver = Pin<Box<dyn Future<Output = Result<(), tokio_postgres::Error>> + Send>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectError {
    Backend,
    PlaintextRoute,
    Route,
}
impl std::fmt::Display for ConnectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Backend => "PostgreSQL connection unavailable",
            Self::PlaintextRoute => "resolved PostgreSQL plaintext route is outside its declared local boundary or exceeds the address limit",
            Self::Route => "PostgreSQL route is empty, oversized or differs from the pinned endpoint",
        })
    }
}
impl std::error::Error for ConnectError {}
/// One selected infrastructure route, reusable only for the same endpoint.
/// The fields are private so callers cannot substitute an unvalidated address.
#[derive(Clone)]
pub struct PinnedRoute {
    host: String,
    port: u16,
    address: IpAddr,
}
impl PinnedRoute {
    pub fn address(&self) -> IpAddr {
        self.address
    }
    pub fn port(&self) -> u16 {
        self.port
    }
}
/// Establish authentication under the prepared TLS/plaintext policy, pinning
/// plaintext DNS answers before connection. The caller owns cancellation and
/// must retain the returned driver before issuing SQL. No reconnect or fallback.
pub async fn connect(prepared: PreparedConnection) -> Result<(Client, Driver), ConnectError> {
    if matches!(
        prepared.security(),
        super::connection_settings::ConnectionSecurity::VerifyFull { .. }
    ) {
        // Existing finite/service consumers retain tokio-postgres's address
        // selection. Only consumers sharing a route with native use the new pin.
        let (config, PreparedTransport::VerifyFull(connector)) = prepared.into_parts() else {
            unreachable!("prepared TLS security matches its transport")
        };
        let (client, driver) = tokio::time::timeout(CONNECT_DEADLINE, config.connect(connector))
            .await
            .map_err(|_| ConnectError::Backend)?
            .map_err(|_| ConnectError::Backend)?;
        return Ok((client, Box::pin(driver)));
    }
    let (client, driver, _) = connect_pinned(prepared, None).await?;
    Ok((client, driver))
}

/// Pin DNS once across a finite operation's fresh connections and native client.
/// TLS continues to authenticate the explicitly configured server identity.
pub async fn connect_pinned(
    prepared: PreparedConnection,
    previous: Option<&PinnedRoute>,
) -> Result<(Client, Driver, PinnedRoute), ConnectError> {
    let (mut config, transport) = prepared.into_parts();
    let connect = async {
        let boundary = match &transport {
            PreparedTransport::LocalPlaintext(boundary) => Some(*boundary),
            PreparedTransport::VerifyFull(_) => None,
        };
        let route = pin_route(&mut config, boundary, previous).await?;
        let pair: (tokio_postgres::Client, Driver) = match transport {
            PreparedTransport::LocalPlaintext(_) => {
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
        Ok::<_, ConnectError>((pair.0, pair.1, route))
    };
    tokio::time::timeout(CONNECT_DEADLINE, connect)
        .await
        .map_err(|_| ConnectError::Backend)?
}

async fn pin_route(
    config: &mut tokio_postgres::Config,
    boundary: Option<PlaintextBoundary>,
    previous: Option<&PinnedRoute>,
) -> Result<PinnedRoute, ConnectError> {
    let [tokio_postgres::config::Host::Tcp(host)] = config.get_hosts() else {
        return Err(ConnectError::Route);
    };
    let [port] = config.get_ports() else {
        return Err(ConnectError::Route);
    };
    let address = if let Some(previous) = previous {
        if previous.host != *host || previous.port != *port {
            return Err(ConnectError::Route);
        }
        select_address([SocketAddr::new(previous.address, *port)], boundary)?
    } else if host == "localhost" {
        // Deterministic loopback without an ambient resolver alias. An IPv6-only
        // database can explicitly select [::1] in its endpoint.
        select_address(
            [SocketAddr::new(Ipv4Addr::LOCALHOST.into(), *port)],
            boundary,
        )?
    } else if let Ok(address) = host.parse::<IpAddr>() {
        select_address([SocketAddr::new(address, *port)], boundary)?
    } else {
        let addresses = tokio::net::lookup_host((host.as_str(), *port))
            .await
            .map_err(|_| ConnectError::Backend)?;
        select_address(addresses, boundary)?
    };
    let route = PinnedRoute {
        host: host.clone(),
        port: *port,
        address,
    };
    // hostaddr prevents a second DNS resolution between policy and connect.
    config.hostaddr(address);
    Ok(route)
}

fn select_address(
    addresses: impl IntoIterator<Item = SocketAddr>,
    boundary: Option<PlaintextBoundary>,
) -> Result<IpAddr, ConnectError> {
    let mut selected = None;
    for (index, address) in addresses.into_iter().enumerate() {
        let ip = address.ip().to_canonical();
        let allowed = match boundary {
            None => true,
            Some(PlaintextBoundary::Loopback) => ip.is_loopback(),
            Some(PlaintextBoundary::PrivateBridge) => match ip {
                IpAddr::V4(ip) => ip.is_private(),
                IpAddr::V6(ip) => ip.is_unique_local(),
            },
        };
        if index >= 16 || !allowed {
            return Err(if boundary.is_some() {
                ConnectError::PlaintextRoute
            } else {
                ConnectError::Route
            });
        }
        selected.get_or_insert(ip);
    }
    selected.ok_or(if boundary.is_some() {
        ConnectError::PlaintextRoute
    } else {
        ConnectError::Route
    })
}

#[cfg(test)]
mod route_tests {
    use super::*;

    fn vetted_address(
        addresses: impl IntoIterator<Item = SocketAddr>,
        boundary: PlaintextBoundary,
    ) -> Result<IpAddr, ConnectError> {
        select_address(addresses, Some(boundary))
    }

    #[tokio::test(flavor = "current_thread")]
    async fn reuse_binds_endpoint_and_rechecks_the_plaintext_boundary_without_dns() {
        let route = PinnedRoute {
            host: "does-not-resolve.invalid".into(),
            port: 443,
            address: "127.0.0.1".parse().unwrap(),
        };
        let config = || {
            let mut c = tokio_postgres::Config::new();
            c.host(&route.host).port(443);
            c
        };
        let mut c = config();
        assert_eq!(
            pin_route(&mut c, None, Some(&route))
                .await
                .unwrap()
                .address(),
            route.address()
        );
        assert_eq!(c.get_hostaddrs(), &[route.address()]);
        assert!(matches!(
            pin_route(
                &mut config(),
                Some(PlaintextBoundary::PrivateBridge),
                Some(&route)
            )
            .await,
            Err(ConnectError::PlaintextRoute)
        ));
        let mut wrong = tokio_postgres::Config::new();
        wrong.host("different.invalid").port(443);
        assert!(matches!(
            pin_route(&mut wrong, None, Some(&route)).await,
            Err(ConnectError::Route)
        ));
        let mut wrong = tokio_postgres::Config::new();
        wrong.host(&route.host).port(5432);
        assert!(matches!(
            pin_route(&mut wrong, None, Some(&route)).await,
            Err(ConnectError::Route)
        ));
    }

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
