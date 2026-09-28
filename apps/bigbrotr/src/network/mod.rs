//! Guarded acquisition destinations. Static route policy is not runtime authority.

mod address;
mod bans;
mod dns;
mod egress;
mod lifetime;
mod policy;
mod route;
mod stream;
mod tls;

pub use bans::BanSet;
pub use dns::{DnsError, DnsFamilyOutcome, DnsResolution, DnsResolver};
pub use egress::{DirectAttempt, DirectConnector, TransportError};
pub use lifetime::CancelAttempt;
pub use policy::{DirectPolicy, FilteredAddresses, PolicyError, PrivateRule, Target};
pub use route::{PrivateNetwork, RouteSelection};
pub use stream::DirectStream;
pub use tls::TlsPolicy;
