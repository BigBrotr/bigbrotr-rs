//! Lossless signed fields. Deserialization does not establish cryptographic validity.
pub mod event;
pub mod identity;

pub use event::{NostrTime, SignedEvent};
pub use identity::{EventId, PublicKey, Signature};
