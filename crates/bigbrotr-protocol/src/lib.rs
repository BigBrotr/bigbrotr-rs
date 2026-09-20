//! Bounded protocol values. Verification is independent of an event-ID cache.
#[cfg(secp256k1_fuzz)]
compile_error!("BigBrotr must not use the dependency's intentionally broken fuzz cryptography");

mod bounds;
pub mod event;

pub use bounds::EventLimits;
pub use event::{EventError, VerifiedEvent, verify_json, verify_owned};
pub mod archive_batch;
pub mod archive_receipt;
pub mod endpoint;

pub mod archive_cut;
pub mod archive_event;
pub mod archive_membership;
pub mod archive_visibility;
