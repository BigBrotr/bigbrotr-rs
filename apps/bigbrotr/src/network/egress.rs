use super::{
    CancelAttempt, DirectPolicy, DirectStream, DnsError, DnsResolver, PolicyError, Target,
    TlsPolicy,
    lifetime::Gate,
    stream::{Guarded, Wire},
};
use crate::runtime::{ResourceBudget, ResourceRequest, policy_authority::ActionWindow};
use std::{fmt, io, net::SocketAddr};
use tokio::{net::TcpStream, time::Instant};

const CONNECTION_BYTES: u64 = 4 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransportError {
    Authority,
    Cancelled,
    Deadline,
    Budget,
    Dns(DnsError),
    Policy(PolicyError),
    Connect,
    TlsConfiguration,
    Tls,
    Operation,
}
impl fmt::Display for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Authority => "acquisition action lifetime unavailable",
            Self::Cancelled => "acquisition operation cancelled",
            Self::Deadline => "acquisition operation deadline exceeded",
            Self::Budget => "acquisition connection resources unavailable",
            Self::Dns(_) => "acquisition DNS resolution refused or unavailable",
            Self::Policy(_) => "acquisition destination refused by route policy",
            Self::Connect => "pinned acquisition connection unavailable",
            Self::TlsConfiguration => "invalid acquisition TLS trust roots",
            Self::Tls => "acquisition TLS authentication failed or unavailable",
            Self::Operation => "acquisition protocol operation failed",
        })
    }
}
impl std::error::Error for TransportError {}

/// A finite temporal/route binding, NOT an authenticated grant. The calling
/// Control adapter must first authorize this exact endpoint, route and action;
/// constructing an ActionWindow establishes only its conservative lifetime.
/// Dropping the returned handle revokes this attempt permanently.
pub struct DirectAttempt {
    target: Target,
    policy: DirectPolicy,
    deadline: Instant,
    gate: Gate,
}
impl DirectAttempt {
    pub fn new(
        target: Target,
        policy: DirectPolicy,
        window: ActionWindow,
        deadline: Instant,
    ) -> (Self, CancelAttempt) {
        let (gate, cancel) = Gate::new(window, deadline);
        (
            Self {
                target,
                policy,
                deadline,
                gate,
            },
            cancel,
        )
    }
}

/// Shared direct TCP/TLS owner. No pool, task, retry, proxy or ambient resolver.
/// The configured resource budget supplies connection bytes/items across callers;
/// host/endpoint scheduling quotas and FD reserve remain the supervisor's gates.
pub struct DirectConnector {
    dns: DnsResolver,
    tls: TlsPolicy,
    resources: ResourceBudget,
}
impl DirectConnector {
    pub fn new(dns: DnsResolver, tls: TlsPolicy, resources: ResourceBudget) -> Self {
        Self {
            dns,
            tls,
            resources,
        }
    }

    /// The async callback borrows, rather than owns, the stream. Its protocol
    /// buffers/CPU must have their own finite bounds; it must cooperate with the
    /// runtime. Cancellation cannot interrupt arbitrary synchronous user code.
    pub async fn run<T>(
        &self,
        attempt: DirectAttempt,
        operation: impl for<'a> AsyncFnOnce(&'a mut DirectStream) -> io::Result<T>,
    ) -> Result<T, TransportError> {
        attempt.gate.check()?;
        let _resources = self
            .resources
            .try_acquire(
                ResourceRequest::data(CONNECTION_BYTES, 1, 0)
                    .expect("fixed connection reservation"),
            )
            .map_err(|_| TransportError::Budget)?;
        attempt
            .gate
            .supervise(async {
                let resolution = self
                    .dns
                    .resolve(&attempt.target, attempt.deadline)
                    .await
                    .map_err(TransportError::Dns)?;
                let filtered = attempt
                    .policy
                    .filter(&attempt.target, resolution.addresses())
                    .map_err(TransportError::Policy)?;
                let selected = SocketAddr::new(filtered.addresses()[0], attempt.target.port());
                attempt.gate.check()?;
                let socket = TcpStream::connect(selected)
                    .await
                    .map_err(|_| TransportError::Connect)?;
                if socket.peer_addr().map_err(|_| TransportError::Connect)? != selected {
                    return Err(TransportError::Connect);
                }
                socket
                    .set_nodelay(true)
                    .map_err(|_| TransportError::Connect)?;
                let socket = Guarded {
                    socket,
                    gate: attempt.gate.clone(),
                };
                let wire = if attempt.target.endpoint().as_str().starts_with("wss://") {
                    let name =
                        rustls::pki_types::ServerName::try_from(attempt.target.host().to_owned())
                            .map_err(|_| TransportError::TlsConfiguration)?;
                    let tls = self
                        .tls
                        .0
                        .connect_with(name, socket, |session| {
                            session.set_buffer_limit(Some(64 * 1024));
                        })
                        .await
                        .map_err(|_| TransportError::Tls)?;
                    Wire::Tls(Box::new(tls))
                } else {
                    Wire::Plain(socket)
                };
                let mut stream = DirectStream {
                    inner: Guarded {
                        socket: wire,
                        gate: attempt.gate.clone(),
                    },
                    endpoint: attempt.target.endpoint().clone(),
                    peer: selected,
                };
                attempt.gate.check()?;
                operation(&mut stream)
                    .await
                    .map_err(|_| TransportError::Operation)
            })
            .await
    }
}
