//! Durable database reservation and fresh bootstrap handoff for explicit initialization.
use super::{GuardError, identifier};
use crate::storage::archive::LocalInitialization;
use std::{cell::Cell, fmt};
use tokio_postgres::Client;
use zeroize::Zeroizing;

/// Observation data, never authority to recreate or re-enable a bootstrap login.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reservation {
    pub database_oid: u32,
    pub reservation_oid: u32,
    pub initialization_id: [u8; 16],
    pub bootstrap_login: String,
    pub bootstrap_oid: u32,
    pub manager_login: String,
    pub manager_oid: u32,
}

/// Newly committed bootstrap material. Observation cannot construct this type.
pub struct FreshBootstrap {
    reservation: Reservation,
    password: Zeroizing<String>,
}
impl fmt::Debug for FreshBootstrap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("FreshBootstrap([redacted])")
    }
}
impl FreshBootstrap {
    pub fn reservation(&self) -> &Reservation {
        &self.reservation
    }
    /// Consume once into the same custodied native lifetime. No argv/env/logs.
    pub fn with_login<T>(self, handoff: impl FnOnce(&str, &str) -> T) -> T {
        handoff(&self.reservation.bootstrap_login, &self.password)
    }
}

/// One provisioning attempt under fresh local custody. The caller must wrap its
/// actual PostgreSQL driver before polling it and preserve custody/grants through
/// terminal join. No fresh secret is returned on cancellation or unknown COMMIT.
pub async fn reserve(
    _client: &mut Client,
    custody: &LocalInitialization,
    manager_login: &str,
    _commit_started: &Cell<bool>,
) -> Result<FreshBootstrap, GuardError> {
    identifier(manager_login)?;
    custody
        .take_provisioning()
        .map_err(|_| GuardError::InvalidInput)?;
    // TDD baseline: the practical fixture must fail before reservation exists.
    Err(GuardError::Conflict)
}
