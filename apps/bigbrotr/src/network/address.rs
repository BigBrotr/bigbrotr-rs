//! Public address profile, reviewed against the IANA registries on 2026-09-28.
//! See the acquisition contract for the deliberately conservative special-purpose
//! exclusions. This is destination policy, not a claim of Internet reachability.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

pub(super) fn canonical(address: IpAddr) -> IpAddr {
    match address {
        IpAddr::V6(address) => address
            .to_ipv4_mapped()
            .map_or(IpAddr::V6(address), IpAddr::V4),
        other => other,
    }
}

pub(super) fn unicast(address: IpAddr) -> bool {
    !address.is_unspecified()
        && !address.is_multicast()
        && address != IpAddr::V4(Ipv4Addr::BROADCAST)
}

pub(super) fn exact_exception_required(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => {
            address.is_link_local()
                || address == Ipv4Addr::new(100, 100, 100, 200)
                || address == Ipv4Addr::new(168, 63, 129, 16)
        }
        IpAddr::V6(address) => {
            address.is_unicast_link_local()
                || v6_contains(v6(0xfd00, 0xec2, 0), 32, address)
                || address == Ipv6Addr::new(0xfd20, 0xce, 0, 0, 0, 0, 0, 0x254)
        }
    }
}

pub(super) fn public(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => !V4_EXCLUDED
            .iter()
            .any(|&(base, bits)| v4_contains(base, bits, address)),
        IpAddr::V6(address) => {
            V6_ALLOCATED
                .iter()
                .any(|&(base, bits)| v6_contains(base, bits, address))
                && !V6_EXCLUDED
                    .iter()
                    .any(|&(base, bits)| v6_contains(base, bits, address))
        }
    }
}

fn v4_contains(base: Ipv4Addr, bits: u8, address: Ipv4Addr) -> bool {
    let mask = u32::MAX.checked_shl(32 - u32::from(bits)).unwrap_or(0);
    u32::from(address) & mask == u32::from(base)
}

fn v6_contains(base: Ipv6Addr, bits: u8, address: Ipv6Addr) -> bool {
    let mask = u128::MAX.checked_shl(128 - u32::from(bits)).unwrap_or(0);
    u128::from(address) & mask == u128::from(base)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Prefix {
    base: IpAddr,
    bits: u8,
}

impl Prefix {
    pub(super) fn parse(value: &str) -> Option<Self> {
        let (base, bits) = value.split_once('/')?;
        let base: IpAddr = base.parse().ok()?;
        // Mapped destinations use IPv4 rules. Refuse a second spelling for the
        // private prefix instead of silently changing its prefix length.
        if bits.len() > 1 && bits.starts_with('0') {
            return None;
        }
        if bits.is_empty() || !bits.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        let bits: u8 = bits.parse().ok()?;
        Self::new(base, bits)
    }

    pub(super) fn new(base: IpAddr, bits: u8) -> Option<Self> {
        if canonical(base) != base {
            return None;
        }
        let prefix = Self { base, bits };
        let host_bits_clear = match base {
            IpAddr::V4(address) if bits <= 32 => {
                let mask = u32::MAX.checked_shl(32 - u32::from(bits)).unwrap_or(0);
                u32::from(address) & !mask == 0
            }
            IpAddr::V6(address) if bits <= 128 => {
                let mask = u128::MAX.checked_shl(128 - u32::from(bits)).unwrap_or(0);
                u128::from(address) & !mask == 0
            }
            _ => false,
        };
        host_bits_clear.then_some(prefix)
    }

    pub(super) fn contains(self, address: IpAddr) -> bool {
        match (self.base, address) {
            (IpAddr::V4(base), IpAddr::V4(address)) => v4_contains(base, self.bits, address),
            (IpAddr::V6(base), IpAddr::V6(address)) => v6_contains(base, self.bits, address),
            _ => false,
        }
    }

    pub(super) fn single_address(self) -> bool {
        self.bits == if self.base.is_ipv4() { 32 } else { 128 }
    }

    pub(super) fn contains_prefix(self, other: Self) -> bool {
        self.bits <= other.bits && self.contains(other.base)
    }

    pub(super) fn exact_exception(self) -> bool {
        self.single_address() && exact_exception_required(self.base)
    }

    pub(super) fn bytes(self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(18);
        bytes.extend_from_slice(&[if self.base.is_ipv4() { 4 } else { 6 }, self.bits]);
        match self.base {
            IpAddr::V4(address) => bytes.extend_from_slice(&address.octets()),
            IpAddr::V6(address) => bytes.extend_from_slice(&address.octets()),
        }
        bytes
    }
}

impl std::fmt::Display for Prefix {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.base, self.bits)
    }
}

const V4_EXCLUDED: &[(Ipv4Addr, u8)] = &[
    (Ipv4Addr::new(0, 0, 0, 0), 8),
    (Ipv4Addr::new(10, 0, 0, 0), 8),
    (Ipv4Addr::new(100, 64, 0, 0), 10),
    (Ipv4Addr::new(127, 0, 0, 0), 8),
    (Ipv4Addr::new(168, 63, 129, 16), 32), // Azure platform virtual address
    (Ipv4Addr::new(169, 254, 0, 0), 16),
    (Ipv4Addr::new(172, 16, 0, 0), 12),
    (Ipv4Addr::new(192, 0, 0, 0), 24),
    (Ipv4Addr::new(192, 0, 2, 0), 24),
    (Ipv4Addr::new(192, 31, 196, 0), 24),
    (Ipv4Addr::new(192, 52, 193, 0), 24),
    (Ipv4Addr::new(192, 88, 99, 0), 24),
    (Ipv4Addr::new(192, 168, 0, 0), 16),
    (Ipv4Addr::new(192, 175, 48, 0), 24),
    (Ipv4Addr::new(198, 18, 0, 0), 15),
    (Ipv4Addr::new(198, 51, 100, 0), 24),
    (Ipv4Addr::new(203, 0, 113, 0), 24),
    (Ipv4Addr::new(224, 0, 0, 0), 4), // multicast
    (Ipv4Addr::new(240, 0, 0, 0), 4), // reserved, including limited broadcast
];

// Only the first three segments are nonzero in these IANA prefixes.
const fn v6(a: u16, b: u16, c: u16) -> Ipv6Addr {
    Ipv6Addr::new(a, b, c, 0, 0, 0, 0, 0)
}

// Allocated global unicast prefixes, excluding IANA-reserved gaps. Suballocations
// inside these RIR ranges do not require a per-connection registry lookup.
const V6_ALLOCATED: &[(Ipv6Addr, u8)] = &[
    (v6(0x2001, 0, 0), 23),
    (v6(0x2001, 0x200, 0), 23),
    (v6(0x2001, 0x400, 0), 23),
    (v6(0x2001, 0x600, 0), 23),
    (v6(0x2001, 0x800, 0), 22),
    (v6(0x2001, 0xc00, 0), 23),
    (v6(0x2001, 0xe00, 0), 23),
    (v6(0x2001, 0x1200, 0), 23),
    (v6(0x2001, 0x1400, 0), 22),
    (v6(0x2001, 0x1800, 0), 23),
    (v6(0x2001, 0x1a00, 0), 23),
    (v6(0x2001, 0x1c00, 0), 22),
    (v6(0x2001, 0x2000, 0), 19),
    (v6(0x2001, 0x4000, 0), 23),
    (v6(0x2001, 0x4200, 0), 23),
    (v6(0x2001, 0x4400, 0), 23),
    (v6(0x2001, 0x4600, 0), 23),
    (v6(0x2001, 0x4800, 0), 23),
    (v6(0x2001, 0x4a00, 0), 23),
    (v6(0x2001, 0x4c00, 0), 23),
    (v6(0x2001, 0x5000, 0), 20),
    (v6(0x2001, 0x8000, 0), 19),
    (v6(0x2001, 0xa000, 0), 20),
    (v6(0x2001, 0xb000, 0), 20),
    (v6(0x2002, 0, 0), 16),
    (v6(0x2003, 0, 0), 18),
    (v6(0x2400, 0, 0), 12),
    (v6(0x2410, 0, 0), 12),
    (v6(0x2600, 0, 0), 12),
    (v6(0x2610, 0, 0), 23),
    (v6(0x2620, 0, 0), 23),
    (v6(0x2630, 0, 0), 12),
    (v6(0x2800, 0, 0), 12),
    (v6(0x2a00, 0, 0), 12),
    (v6(0x2a10, 0, 0), 12),
    (v6(0x2c00, 0, 0), 12),
];

const V6_EXCLUDED: &[(Ipv6Addr, u8)] = &[
    (v6(0x2001, 0, 0), 23),         // protocol assignments, including Teredo
    (v6(0x2001, 0xdb8, 0), 32),     // documentation
    (v6(0x2002, 0, 0), 16),         // 6to4 can encode a denied IPv4 destination
    (v6(0x2620, 0x4f, 0x8000), 48), // AS112 service
];
