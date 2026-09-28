//! Guarded acquisition destinations. Static route policy is not runtime authority.

mod address;
mod dns;
mod policy;

pub use dns::{DnsError, DnsFamilyOutcome, DnsResolution, DnsResolver};
pub use policy::{DirectPolicy, FilteredAddresses, PolicyError, PrivateRule, Target};
