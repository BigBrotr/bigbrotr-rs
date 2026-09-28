//! Retained collection intent. Loading this data grants no source or admission authority.
use crate::runtime::OwnedPermit;
use bigbrotr_model::{
    AcquisitionScopeId,
    acquisition::{AcquisitionScope, CollectionSelector},
};

/// The selector keeps its parser reservation until all retained allocations drop.
/// Deliberately not Clone: a consumer borrows the intent under its owner's grant.
pub struct StoredAcquisitionScope {
    identity: AcquisitionScopeId,
    scope: AcquisitionScope,
    selector: CollectionSelector,
    permit: OwnedPermit,
}

impl StoredAcquisitionScope {
    pub(crate) fn new(
        identity: AcquisitionScopeId,
        scope: AcquisitionScope,
        selector: CollectionSelector,
        permit: OwnedPermit,
    ) -> Self {
        Self {
            identity,
            scope,
            selector,
            permit,
        }
    }

    pub fn identity(&self) -> AcquisitionScopeId {
        self.identity
    }

    pub fn scope(&self) -> AcquisitionScope {
        self.scope
    }

    pub fn selector(&self) -> &CollectionSelector {
        &self.selector
    }

    /// Conservative retained reservation, not a measurement of allocator/RSS use.
    pub fn reserved_bytes(&self) -> u64 {
        self.permit.request().units().data_bytes()
    }
}
