use super::address::{self, Prefix};
use bigbrotr_protocol::endpoint::Endpoint;
use std::{fmt, net::IpAddr};

/// Static destination refusal; contains no endpoint path or query.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PolicyError {
    InvalidHost,
    InvalidPrivateRule,
    OverlayRouteRequired,
    AnswerLimit,
    NoAllowedAddress,
}

impl fmt::Display for PolicyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidHost => "invalid direct destination hostname",
            Self::InvalidPrivateRule => "invalid bounded private destination rule",
            Self::OverlayRouteRequired => {
                "destination requires a separately configured overlay route"
            }
            Self::AnswerLimit => "DNS answer count exceeds the direct-route limit",
            Self::NoAllowedAddress => "no destination address satisfies the selected route",
        })
    }
}

impl std::error::Error for PolicyError {}

/// Parsed direct-route destination. Construction neither resolves nor authorizes
/// a connection. Keep the original Endpoint for Host/SNI and wire path/query.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Target {
    endpoint: Endpoint,
    host: String,
    port: u16,
    literal: Option<IpAddr>,
}

impl Target {
    pub fn new(endpoint: Endpoint) -> Result<Self, PolicyError> {
        let parsed = url::Url::parse(endpoint.as_str()).expect("normalized Endpoint");
        let (host, literal) = match parsed.host().expect("normalized host") {
            url::Host::Domain(host) => {
                direct_hostname(host)?;
                (host.to_owned(), None)
            }
            url::Host::Ipv4(address) => (address.to_string(), Some(IpAddr::V4(address))),
            url::Host::Ipv6(address) => (
                address.to_string(),
                Some(address::canonical(IpAddr::V6(address))),
            ),
        };
        Ok(Self {
            port: parsed.port_or_known_default().expect("ws/wss port"),
            endpoint,
            host,
            literal,
        })
    }

    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }
    pub fn host(&self) -> &str {
        &self.host
    }
    pub const fn port(&self) -> u16 {
        self.port
    }
    pub const fn literal_address(&self) -> Option<IpAddr> {
        self.literal
    }
}

pub(super) fn direct_hostname(host: &str) -> Result<(), PolicyError> {
    if host.len() > 253
        || host.is_empty()
        || !host.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        })
    {
        return Err(PolicyError::InvalidHost);
    }
    if ["onion", "i2p", "loki", "snode"].contains(&host.rsplit('.').next().unwrap()) {
        return Err(PolicyError::OverlayRouteRequired);
    }
    Ok(())
}

#[derive(Clone, Debug)]
enum PrivateSubject {
    Endpoint(Endpoint),
    Hostname(String),
}

/// One explicit subject, canonical CIDR and port. No suffix or wildcard rules.
#[derive(Clone, Debug)]
pub struct PrivateRule {
    subject: PrivateSubject,
    prefix: Prefix,
    port: u16,
}

impl PrivateRule {
    pub fn endpoint(endpoint: Endpoint, cidr: &str, port: u16) -> Result<Self, PolicyError> {
        if Target::new(endpoint.clone())?.port() != port {
            return Err(PolicyError::InvalidPrivateRule);
        }
        Self::new(PrivateSubject::Endpoint(endpoint), cidr, port)
    }

    /// Requires the normalized ASCII hostname, never a URL or an IP literal.
    pub fn hostname(host: &str, cidr: &str, port: u16) -> Result<Self, PolicyError> {
        direct_hostname(host)?;
        if !matches!(url::Host::parse(host), Ok(url::Host::Domain(_))) {
            return Err(PolicyError::InvalidPrivateRule);
        }
        Self::new(PrivateSubject::Hostname(host.to_owned()), cidr, port)
    }

    fn new(subject: PrivateSubject, cidr: &str, port: u16) -> Result<Self, PolicyError> {
        if cidr.len() > 43 || port == 0 {
            return Err(PolicyError::InvalidPrivateRule);
        }
        Ok(Self {
            subject,
            prefix: Prefix::parse(cidr).ok_or(PolicyError::InvalidPrivateRule)?,
            port,
        })
    }

    fn allows(&self, target: &Target, address: IpAddr) -> bool {
        self.port == target.port
            && self.prefix.contains(address)
            && (!address::exact_exception_required(address) || self.prefix.single_address())
            && match &self.subject {
                PrivateSubject::Endpoint(endpoint) => endpoint == &target.endpoint,
                PrivateSubject::Hostname(host) => host == &target.host,
            }
    }
}

/// Public and private are separate choices; refusal never falls back to another
/// route. Enrollment, bans, access scope and current authority are checked by the
/// socket owner in addition to this static destination policy.
#[derive(Clone, Debug)]
pub struct DirectPolicy {
    private: Option<Vec<PrivateRule>>,
}

impl DirectPolicy {
    pub const fn public() -> Self {
        Self { private: None }
    }

    pub fn private(rules: Vec<PrivateRule>) -> Result<Self, PolicyError> {
        if rules.is_empty() || rules.len() > 256 {
            return Err(PolicyError::InvalidPrivateRule);
        }
        Ok(Self {
            private: Some(rules),
        })
    }

    /// Filters all answers from one bounded resolution. The returned IPs must be
    /// pinned for dialing, never passed back through a hostname connector.
    pub fn filter(
        &self,
        target: &Target,
        answers: &[IpAddr],
    ) -> Result<FilteredAddresses, PolicyError> {
        if answers.len() > 64 {
            return Err(PolicyError::AnswerLimit);
        }
        let mut addresses = Vec::with_capacity(answers.len());
        let mut excluded = 0;
        for &answer in answers {
            let address = address::canonical(answer);
            let allowed = address::unicast(address)
                && target.literal.is_none_or(|literal| literal == address)
                && match &self.private {
                    None => address::public(address),
                    Some(rules) => rules.iter().any(|rule| rule.allows(target, address)),
                };
            if !allowed {
                excluded += 1;
            } else if !addresses.contains(&address) {
                addresses.push(address);
            }
        }
        if addresses.is_empty() {
            return Err(PolicyError::NoAllowedAddress);
        }
        Ok(FilteredAddresses {
            addresses,
            excluded,
        })
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct FilteredAddresses {
    addresses: Vec<IpAddr>,
    excluded: usize,
}

impl FilteredAddresses {
    pub fn addresses(&self) -> &[IpAddr] {
        &self.addresses
    }
    pub const fn excluded(&self) -> usize {
        self.excluded
    }
}
