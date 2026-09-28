//! Explicit direct route selection. These values and static policies are not
//! enrollment, live authority, visibility allocation or evidence of reachability.
use super::{DirectPolicy, PolicyError, Target, address::Prefix};
use bigbrotr_protocol::endpoint::Endpoint;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use std::{fmt, net::IpAddr, str::FromStr};

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct PrivateNetwork(Prefix);

impl FromStr for PrivateNetwork {
    type Err = PolicyError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() > 43 {
            return Err(PolicyError::InvalidPrivateRule);
        }
        Prefix::parse(value)
            .map(Self)
            .ok_or(PolicyError::InvalidPrivateRule)
    }
}
impl fmt::Debug for PrivateNetwork {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PrivateNetwork([redacted])")
    }
}
impl Serialize for PrivateNetwork {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(&self.0)
    }
}
impl<'de> Deserialize<'de> for PrivateNetwork {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl de::Visitor<'_> for Visitor {
            type Value = PrivateNetwork;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a bounded private CIDR with clear host bits")
            }
            fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
                value.parse().map_err(E::custom)
            }
        }
        deserializer.deserialize_str(Visitor)
    }
}

/// First direct profile: one public route or one endpoint-bound private CIDR.
/// Protected authentication, multi-rule routes and overlays require their own
/// real consumers; unsupported route classes are refused rather than downgraded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "class", rename_all = "lowercase", deny_unknown_fields)]
pub enum RouteSelection {
    Public {},
    Private { cidr: PrivateNetwork },
}

impl RouteSelection {
    /// Decode the exact version-1 operational route. No caller may replace
    /// a retained route with its own selection. This establishes shape only;
    /// current roots, enrollment and source visibility still authorize use.
    pub fn from_canonical_bytes(bytes: &[u8]) -> Result<Self, PolicyError> {
        let (base, bits) = match bytes {
            [0] => return Ok(Self::Public {}),
            [1, 4, bits, a, b, c, d] => (IpAddr::from([*a, *b, *c, *d]), *bits),
            [1, 6, bits, octets @ ..] if octets.len() == 16 => {
                let address: [u8; 16] = octets.try_into().expect("checked exact length");
                (IpAddr::from(address), *bits)
            }
            _ => return Err(PolicyError::InvalidPrivateRule),
        };
        let prefix = Prefix::new(base, bits).ok_or(PolicyError::InvalidPrivateRule)?;
        Ok(Self::Private {
            cidr: PrivateNetwork(prefix),
        })
    }

    /// Stable exact bytes used by the operational command identity. Public is
    /// [0]; private is [1, address family 4/6, prefix bits, network octets].
    pub fn canonical_bytes(self) -> Vec<u8> {
        match self {
            Self::Public {} => vec![0],
            Self::Private { cidr } => {
                let mut bytes = vec![1];
                bytes.extend(cidr.0.bytes());
                bytes
            }
        }
    }

    /// Fresh enablement must fit one complete root rule, not just a sample IP.
    /// Withdrawal and historical receipt resolution must not call this method.
    pub fn validate_enrollment(
        self,
        endpoint: &Endpoint,
        public_enabled: bool,
        ceiling: Option<&DirectPolicy>,
    ) -> Result<(), PolicyError> {
        let target = Target::new(endpoint.clone())?;
        let allowed = match self {
            Self::Public {} => public_enabled,
            Self::Private { cidr } => ceiling.is_some_and(|root| root.covers(&target, cidr.0)),
        };
        allowed.then_some(()).ok_or(PolicyError::NoAllowedAddress)
    }

    /// Intersect with the *current* roots before each actual attempt. The two
    /// metadata-exception requirements remain independent; merging CIDRs would
    /// accidentally upgrade a broad root when the selection is one address.
    pub fn policy(
        self,
        endpoint: &Endpoint,
        public_enabled: bool,
        ceiling: Option<&DirectPolicy>,
    ) -> Result<DirectPolicy, PolicyError> {
        Target::new(endpoint.clone())?;
        match self {
            Self::Public {} if public_enabled => Ok(DirectPolicy::select_public(endpoint.clone())),
            Self::Public {} => Err(PolicyError::NoAllowedAddress),
            Self::Private { cidr } => ceiling
                .ok_or(PolicyError::NoAllowedAddress)?
                .select_private(endpoint.clone(), cidr.0),
        }
    }
}
