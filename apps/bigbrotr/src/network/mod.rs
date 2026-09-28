//! Guarded acquisition destinations. Static route policy is not runtime authority.

mod address;
mod dns;
mod egress;
mod lifetime;
mod policy;
mod stream;
mod tls;

pub use dns::{DnsError, DnsFamilyOutcome, DnsResolution, DnsResolver};
pub use egress::{DirectAttempt, DirectConnector, TransportError};
pub use lifetime::CancelAttempt;
pub use policy::{DirectPolicy, FilteredAddresses, PolicyError, PrivateRule, Target};
pub use stream::DirectStream;
pub use tls::TlsPolicy;
