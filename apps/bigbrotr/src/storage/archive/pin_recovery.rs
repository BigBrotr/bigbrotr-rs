//! Fresh local exclusion reserved exclusively for old snapshot-pin recovery.
use super::{LifetimeError, LocalCoordinator};
use crate::storage::postgres::catalog_guard::CatalogIdentity;
use std::{future::Future, path::Path};

/// Cannot be constructed from an existing coordinator or used to launch readers.
/// All retained transports must end before ordinary startup can reacquire custody.
/// This proves local descriptor exclusion, not current SQL/policy authority.
pub struct LocalPinRecovery {
    coordinator: LocalCoordinator,
}
impl LocalPinRecovery {
    pub fn open(identity: &CatalogIdentity, root: &Path) -> Result<Self, LifetimeError> {
        let mut coordinator = LocalCoordinator::open(identity, root)?;
        // No reference or clone escapes before this permanent restriction.
        coordinator.custody.restrict_to_pin_recovery();
        Ok(Self { coordinator })
    }
    pub fn identity(&self) -> &CatalogIdentity {
        self.coordinator.identity()
    }
    pub fn retain_connection<F: Future + Unpin>(
        &self,
        future: F,
    ) -> Result<impl Future<Output = F::Output> + use<F>, LifetimeError> {
        self.coordinator.retain_connection(future)
    }
    pub(crate) fn coordinator(&self) -> &LocalCoordinator {
        &self.coordinator
    }
}
