//! Opaque physical observations. Only the ClickHouse owner constructs them after
//! real identity/schema/full-row checks. A surviving PG digest cannot construct a
//! capability. Every proof retains the same close-only custody description until
//! its later fenced SQL use, preventing release/reacquisition between boundaries.
use crate::storage::postgres::{IndexBinding, IndexOwnerIdentity, IndexRecoveryState};
use bigbrotr_model::Digest256;
use std::fs::File;

pub struct VerifiedInstallation {
    binding: IndexBinding,
    _custody: File,
}
impl VerifiedInstallation {
    pub(crate) fn binding(&self) -> &IndexBinding {
        &self.binding
    }
    pub(super) fn observed(binding: IndexBinding, custody: File) -> Self {
        Self {
            binding,
            _custody: custody,
        }
    }
}

pub struct VerifiedIndexPlan {
    binding: IndexBinding,
    plan_digest: Digest256,
    engine_receipt_digest: Digest256,
    _custody: File,
}
impl VerifiedIndexPlan {
    pub(crate) fn binding(&self) -> &IndexBinding {
        &self.binding
    }
    pub(crate) fn plan_digest(&self) -> Digest256 {
        self.plan_digest
    }
    pub(crate) fn engine_receipt_digest(&self) -> Digest256 {
        self.engine_receipt_digest
    }
    pub(super) fn observed(
        binding: IndexBinding,
        plan_digest: Digest256,
        engine_receipt_digest: Digest256,
        custody: File,
    ) -> Self {
        Self {
            binding,
            plan_digest,
            engine_receipt_digest,
            _custody: custody,
        }
    }
}

pub struct VerifiedIndexRecovery {
    binding: IndexBinding,
    previous_owner: IndexOwnerIdentity,
    state: IndexRecoveryState,
    recovery_digest: Digest256,
    _custody: File,
}
impl VerifiedIndexRecovery {
    pub(crate) fn binding(&self) -> &IndexBinding {
        &self.binding
    }
    pub(crate) fn previous_owner(&self) -> IndexOwnerIdentity {
        self.previous_owner
    }
    pub(crate) fn state(&self) -> IndexRecoveryState {
        self.state
    }
    pub(crate) fn recovery_digest(&self) -> Digest256 {
        self.recovery_digest
    }
    pub(super) fn observed(
        binding: IndexBinding,
        previous_owner: IndexOwnerIdentity,
        state: IndexRecoveryState,
        recovery_digest: Digest256,
        custody: File,
    ) -> Self {
        Self {
            binding,
            previous_owner,
            state,
            recovery_digest,
            _custody: custody,
        }
    }
}
