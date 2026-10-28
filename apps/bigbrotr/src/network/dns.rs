//! Bounded DNS over TCP to explicitly trusted infrastructure. Each exchange is
//! polled by the caller and owns its socket; no resolver driver is spawned.

mod response;

use super::{Target, address, policy::direct_hostname};
use crate::runtime::{ResourceBudget, ResourceRequest};
use hickory_proto::{
    op::{Message, MessageType, OpCode, Query},
    rr::{Name, RecordType},
};
use std::{
    fmt,
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::Semaphore,
    time::{Instant, timeout, timeout_at},
};

const LOOKUP_MEMORY_BYTES: u64 = 4 * 1024 * 1024;
const MAX_FRAME_BYTES: usize = 4096;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DnsError {
    InvalidConfiguration,
    Budget,
    Deadline,
    NamePolicy,
    ResponseLimit,
    InvalidResponse,
    Unavailable,
    NoRecords,
}

#[cfg(test)]
mod reservation_tests {
    use super::*;
    use crate::runtime::{ProcessEnvelope, ResourceLimits, WaiterLimits};
    use bigbrotr_protocol::endpoint::Endpoint;

    #[tokio::test]
    async fn funded_view_shares_the_original_dns_gate_and_closes_with_its_scope() {
        let parent = ResourceBudget::new(
            ProcessEnvelope::new(
                16 * 1024 * 1024,
                0,
                ResourceLimits::new(16 * 1024 * 1024, 4, 0, 0),
                WaiterLimits::new(0, 0),
            )
            .unwrap(),
        );
        let dns = DnsResolver::new("127.0.0.1:53".parse().unwrap(), parent.clone()).unwrap();
        let scope = parent
            .try_reserve(ResourceLimits::new(LOOKUP_MEMORY_BYTES, 1, 0, 0))
            .unwrap();
        let view = dns.within(scope.budget().clone());
        let target = Target::new(Endpoint::parse("ws://127.0.0.1:80/").unwrap()).unwrap();
        let held = dns.connection.try_acquire().unwrap();
        assert!(matches!(
            view.resolve(&target, Instant::now() + Duration::from_secs(1))
                .await,
            Err(DnsError::Budget)
        ));
        drop(held);
        assert_eq!(
            view.resolve(&target, Instant::now() + Duration::from_secs(1))
                .await
                .unwrap()
                .addresses(),
            &["127.0.0.1".parse::<IpAddr>().unwrap()]
        );
        drop(scope);
        assert!(matches!(
            view.resolve(&target, Instant::now() + Duration::from_secs(1))
                .await,
            Err(DnsError::Budget)
        ));
        assert!(
            dns.resolve(&target, Instant::now() + Duration::from_secs(1))
                .await
                .is_ok()
        );
        assert_eq!(parent.snapshot().active_permits(), 0);
    }
}

impl fmt::Display for DnsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidConfiguration => "invalid trusted DNS resolver address",
            Self::Budget => "DNS resource reservation unavailable",
            Self::Deadline => "DNS resolution deadline exceeded",
            Self::NamePolicy => "DNS name requires another route",
            Self::ResponseLimit => "DNS response or work exceeds its finite allowance",
            Self::InvalidResponse => "DNS response does not match the complete request",
            Self::Unavailable => "DNS resolver unavailable",
            Self::NoRecords => "DNS returned no address records",
        })
    }
}
impl std::error::Error for DnsError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DnsFamilyOutcome {
    Answered,
    NoRecords,
    Unavailable,
    Literal,
}

#[derive(Debug)]
pub struct DnsResolution {
    addresses: Vec<IpAddr>,
    pub ipv4: DnsFamilyOutcome,
    pub ipv6: DnsFamilyOutcome,
}
impl DnsResolution {
    /// Untrusted results: the direct-route policy must still filter every address.
    pub fn addresses(&self) -> &[IpAddr] {
        &self.addresses
    }
}

/// One host-local DNS owner. Share this value across acquisition modules, never
/// create a resolver per service to multiply its connection reservation.
pub struct DnsResolver {
    server: SocketAddr,
    resources: ResourceBudget,
    connection: Arc<Semaphore>,
}

impl DnsResolver {
    /// The offline configuration parser uses the same numeric-server rules.
    /// Validation performs no lookup or connection and grants no destination access.
    pub fn validate_server(server: SocketAddr) -> Result<(), DnsError> {
        if server.port() == 0
            || !address::unicast(address::canonical(server.ip()))
            || matches!(server, SocketAddr::V6(v6) if v6.scope_id() != 0 || v6.flowinfo() != 0)
        {
            return Err(DnsError::InvalidConfiguration);
        }
        Ok(())
    }

    pub fn new(server: SocketAddr, resources: ResourceBudget) -> Result<Self, DnsError> {
        Self::validate_server(server)?;
        Ok(Self {
            server,
            resources,
            connection: Arc::new(Semaphore::new(1)),
        })
    }

    pub(super) fn within(&self, resources: ResourceBudget) -> Self {
        Self {
            server: self.server,
            resources,
            connection: self.connection.clone(),
        }
    }

    /// Resolves once under one shrinking deadline. Cancellation drops the active
    /// TCP socket before the resource/connection guards; there are no wait queues.
    /// Current policy/route authority and the outer socket budget remain the
    /// acquiring caller's responsibility, including before this DNS disclosure.
    pub async fn resolve(
        &self,
        target: &Target,
        deadline: Instant,
    ) -> Result<DnsResolution, DnsError> {
        if Instant::now() >= deadline {
            return Err(DnsError::Deadline);
        }
        let _connection = self
            .connection
            .try_acquire()
            .map_err(|_| DnsError::Budget)?;
        let _resources = self
            .resources
            .try_acquire(
                ResourceRequest::data(LOOKUP_MEMORY_BYTES, 1, 0).expect("nonempty DNS reservation"),
            )
            .map_err(|_| DnsError::Budget)?;
        if let Some(address) = target.literal_address() {
            return Ok(DnsResolution {
                addresses: vec![address],
                ipv4: DnsFamilyOutcome::Literal,
                ipv6: DnsFamilyOutcome::Literal,
            });
        }
        timeout_at(deadline, self.lookup(target.host()))
            .await
            .map_err(|_| DnsError::Deadline)?
    }

    async fn lookup(&self, host: &str) -> Result<DnsResolution, DnsError> {
        let name = Name::from_ascii(format!("{host}.")).map_err(|_| DnsError::NamePolicy)?;
        response::check_name(&name)?;
        let mut addresses = Vec::new();
        let mut outcomes = [DnsFamilyOutcome::NoRecords; 2];
        // Families are sequential: one owned socket, at most nine exchanges per
        // family (original name plus eight aliases), under the outer deadline.
        // Fatal policy/encoding/limit failures cannot hide behind family success.
        for (index, kind) in [RecordType::A, RecordType::AAAA].into_iter().enumerate() {
            match self.lookup_family(name.clone(), kind).await {
                Ok(found) if !found.is_empty() => {
                    if addresses.len() + found.len() > 64 {
                        return Err(DnsError::ResponseLimit);
                    }
                    addresses.extend(found);
                    outcomes[index] = DnsFamilyOutcome::Answered;
                }
                Ok(_) | Err(DnsError::NoRecords) => {}
                Err(DnsError::Unavailable) => outcomes[index] = DnsFamilyOutcome::Unavailable,
                Err(error) => return Err(error),
            }
        }
        if addresses.is_empty() {
            return Err(if outcomes.contains(&DnsFamilyOutcome::Unavailable) {
                DnsError::Unavailable
            } else {
                DnsError::NoRecords
            });
        }
        Ok(DnsResolution {
            addresses,
            ipv4: outcomes[0],
            ipv6: outcomes[1],
        })
    }

    async fn lookup_family(&self, name: Name, kind: RecordType) -> Result<Vec<IpAddr>, DnsError> {
        let mut chain = response::Chain::new(name);
        loop {
            let mut id = [0; 2];
            getrandom::fill(&mut id).map_err(|_| DnsError::Unavailable)?;
            let mut request =
                Message::new(u16::from_ne_bytes(id), MessageType::Query, OpCode::Query);
            request.metadata.recursion_desired = true;
            request
                .queries
                .push(Query::query(chain.terminal().clone(), kind));
            let message = timeout(Duration::from_secs(2), self.exchange(&request))
                .await
                .map_err(|_| DnsError::Unavailable)??;
            match chain.accept(&message, kind)? {
                response::Answer::Addresses(addresses) => return Ok(addresses),
                response::Answer::Alias => continue,
                response::Answer::NoRecords => return Err(DnsError::NoRecords),
            }
        }
    }

    async fn exchange(&self, request: &Message) -> Result<Message, DnsError> {
        let bytes = request.to_vec().map_err(|_| DnsError::InvalidResponse)?;
        let mut socket = TcpStream::connect(self.server)
            .await
            .map_err(|_| DnsError::Unavailable)?;
        socket
            .write_u16(bytes.len() as u16)
            .await
            .map_err(|_| DnsError::Unavailable)?;
        socket
            .write_all(&bytes)
            .await
            .map_err(|_| DnsError::Unavailable)?;
        let length = usize::from(socket.read_u16().await.map_err(|_| DnsError::Unavailable)?);
        if !(12..=MAX_FRAME_BYTES).contains(&length) {
            return Err(DnsError::ResponseLimit);
        }
        let mut frame = vec![0; length];
        socket
            .read_exact(&mut frame)
            .await
            .map_err(|_| DnsError::Unavailable)?;
        // Returning, error, timeout or cancellation drops this socket before any
        // resolution resource guard. There is no pool, driver or spawned task.
        response::decode(&frame, request)
    }
}
