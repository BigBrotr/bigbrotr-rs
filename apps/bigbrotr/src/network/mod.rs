//! Guarded acquisition destinations. Static route policy is not runtime authority.

mod address;
mod bans;
mod connect;
mod dns;
mod egress;
mod lifetime;
mod observation;
mod policy;
mod route;
mod stream;
mod tls;
mod websocket;

pub use bans::BanSet;
pub use dns::{DnsError, DnsFamilyOutcome, DnsResolution, DnsResolver};
pub use egress::{DirectAttempt, DirectConnector, TransportError};
pub use lifetime::CancelAttempt;
pub use observation::{
    ConnectErrorClass, ConnectFailure, ConnectFailureStage, ConnectSubmission, FirstIo,
    FirstIoTarget, OBSERVED_CHECK_TIMEOUT, OBSERVED_MAX_EVENT_CANDIDATES,
    OBSERVED_MAX_MESSAGE_FRAMED_BYTES, OBSERVED_MAX_RX_LOGICAL_MESSAGES,
    OBSERVED_MAX_RX_PHYSICAL_FRAMES, OBSERVED_MAX_RX_WS_FRAMED_BYTES,
    OBSERVED_MAX_TX_WS_FRAMED_BYTES, OBSERVED_OPEN_TIMEOUT, ObservedSessionLimit, ObservedStop,
    TransportInstant, TransportSnapshot, TransportTrace, WebSocketCounters,
};
pub use policy::{DirectPolicy, FilteredAddresses, PolicyError, PrivateRule, Target};
pub use route::{PrivateNetwork, RouteSelection};
pub use stream::DirectStream;
pub use tls::TlsPolicy;

pub use websocket::{ReceivedMessage, RelayWebSocket, WebSocketError};
