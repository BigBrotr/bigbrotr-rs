//! Retained native-owner funding for the finite connected intake probes.
use bigbrotr::{
    runtime::{OwnedPermit, ResourceBudget, ResourceRequest},
    storage::rocks::store::PENDING_QUOTA_OWNER_DATA_BYTES,
};

pub fn grant(budget: &ResourceBudget, native_bytes: u64) -> OwnedPermit {
    // This same permit stays with the DB through close. An operation's scratch
    // grant cannot fund the owner's Rust reservation ledger.
    budget
        .try_acquire(
            ResourceRequest::data(PENDING_QUOTA_OWNER_DATA_BYTES, 1, native_bytes).unwrap(),
        )
        .unwrap()
}
