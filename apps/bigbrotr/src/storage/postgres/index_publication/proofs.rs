//! The sole integration seam to independently owned physical proof constructors.
//! No public constructor or test escape hatch manufactures these capabilities.
use super::*;
pub(super) use crate::storage::{
    clickhouse::verified::{VerifiedIndexPlan, VerifiedIndexRecovery, VerifiedInstallation},
    indexer::custody::HeldIndexCustody,
};

pub(super) fn live(binding: &IndexBinding, custody: &HeldIndexCustody) -> Result<(), IndexError> {
    if custody.binding() != binding {
        return Err(IndexError::Conflict);
    }
    custody.check_live().map_err(|_| IndexError::Unavailable)
}
pub(super) fn installation(
    binding: &IndexBinding,
    custody: &HeldIndexCustody,
    proof: &VerifiedInstallation,
) -> Result<(), IndexError> {
    live(binding, custody)?;
    if proof.binding() != binding {
        return Err(IndexError::Conflict);
    }
    Ok(())
}
pub(super) fn publication(
    plan: &IndexPlan,
    custody: &HeldIndexCustody,
    proof: &VerifiedIndexPlan,
) -> Result<Digest256, IndexError> {
    live(plan.binding(), custody)?;
    if proof.binding() != plan.binding()
        || proof.plan_digest() != plan.digest()
        || proof.engine_receipt_digest().as_bytes() == &[0; 32]
    {
        return Err(IndexError::Conflict);
    }
    Ok(proof.engine_receipt_digest())
}
pub(super) fn recovery(
    binding: &IndexBinding,
    custody: &HeldIndexCustody,
    proof: &VerifiedIndexRecovery,
) -> Result<(IndexOwnerIdentity, IndexRecoveryState, Digest256), IndexError> {
    live(binding, custody)?;
    if proof.binding() != binding
        || proof.previous_owner().resource != binding.resource()
        || proof.recovery_digest().as_bytes() == &[0; 32]
        || !proof.state().valid_head()
    {
        return Err(IndexError::Conflict);
    }
    Ok((
        proof.previous_owner(),
        proof.state(),
        proof.recovery_digest(),
    ))
}
