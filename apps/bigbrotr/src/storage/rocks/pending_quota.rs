//! Logical pending credit belongs to the retained exclusive native owner.
//! This is neither filesystem funding nor permission to resume after pressure.
use super::{IngestStore, PendingAccountingError};
use bigbrotr_model::{Digest256, ProducerRequestId};
use sha2::{Digest, Sha256};
use std::{
    fmt,
    sync::Arc,
    time::{Duration, Instant},
};

const MAX_REQUESTS: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PendingQuotaError {
    InvalidPolicy,
    Unfunded,
    RecoveryBlocked,
    PolicyMismatch,
    Pressure,
    Conflict,
    InsufficientAllowance,
    Limit,
    Accounting(PendingAccountingError),
    WriteUncertain,
}
impl fmt::Display for PendingQuotaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "logical pending admission refused: {self:?}")
    }
}
impl std::error::Error for PendingQuotaError {}

/// Exact accepted configuration/security identity and its checked projection.
/// Construction grants no authority; the connected owner validates the accepted
/// documents before binding this value to its actual native manifest/custody.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingQuotaPolicy {
    high_bytes: u64,
    low_bytes: u64,
    max_unit_bytes: u64,
    config_revision: u64,
    config_digest: Digest256,
    policy_revision: u64,
    policy_digest: Digest256,
    stable_duration: Duration,
}
impl PendingQuotaPolicy {
    #[allow(
        clippy::too_many_arguments,
        reason = "one immutable accepted policy identity"
    )]
    pub fn new(
        high_bytes: u64,
        low_bytes: u64,
        max_unit_bytes: u64,
        config_revision: u64,
        config_digest: Digest256,
        policy_revision: u64,
        policy_digest: Digest256,
        stable_duration: Duration,
    ) -> Result<Self, PendingQuotaError> {
        if low_bytes == 0
            || low_bytes >= high_bytes
            || max_unit_bytes == 0
            || max_unit_bytes >= high_bytes
            || config_revision == 0
            || policy_revision == 0
            || stable_duration.is_zero()
        {
            return Err(PendingQuotaError::InvalidPolicy);
        }
        Ok(Self {
            high_bytes,
            low_bytes,
            max_unit_bytes,
            config_revision,
            config_digest,
            policy_revision,
            policy_digest,
            stable_duration,
        })
    }
    pub const fn high_bytes(self) -> u64 {
        self.high_bytes
    }
    pub const fn low_bytes(self) -> u64 {
        self.low_bytes
    }
    pub const fn max_unit_bytes(self) -> u64 {
        self.max_unit_bytes
    }
}

/// A bounded request for credit, not a grant or a native staging witness.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingReservationRequest {
    pub request: ProducerRequestId,
    pub digest: Digest256,
    pub allowance_bytes: u64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PendingQuotaStatus {
    Unfunded,
    RecoveryBlocked,
    Available,
    Pressure,
}

pub(super) enum PendingQuota {
    Unfunded,
    RecoveryBlocked,
    Bound(BoundQuota),
}
pub(super) struct BoundQuota {
    policy: PendingQuotaPolicy,
    manifest_sha256: [u8; 32],
    pressure: bool,
    below_low_since: Option<Instant>,
    entries: Vec<Reservation>,
}
#[derive(Clone, Copy)]
struct Reservation {
    terms: PendingReservationRequest,
    // Set before the first possible side effect, including directory creation.
    // An error, cancellation or dropped PG borrow can never clear this debt.
    staging_started: bool,
    staged: bool,
}

/// Private proof minted from the owner's ledger after original staging.
/// Drop has no effect on the ledger, including on every failed native path.
pub(super) struct PendingFunding {
    terms: PendingReservationRequest,
    policy: PendingQuotaPolicy,
    manifest_sha256: [u8; 32],
    custody: Arc<()>,
}

impl IngestStore {
    pub fn pending_quota_status(&self) -> PendingQuotaStatus {
        match &self.pending_quota {
            PendingQuota::Unfunded => PendingQuotaStatus::Unfunded,
            PendingQuota::RecoveryBlocked => PendingQuotaStatus::RecoveryBlocked,
            PendingQuota::Bound(quota) if quota.pressure => PendingQuotaStatus::Pressure,
            PendingQuota::Bound(_) => PendingQuotaStatus::Available,
        }
    }
    pub fn pending_reserved_bytes(&self) -> Result<u64, PendingQuotaError> {
        self.quota()?.entries.iter().try_fold(0u64, |sum, entry| {
            sum.checked_add(entry.terms.allowance_bytes)
                .ok_or(PendingQuotaError::Limit)
        })
    }
    /// Identical binding preserves every unresolved obligation and pressure.
    /// Raw reopen requires positive recovery; census alone cannot discard an
    /// original staged before a missing native receipt or SQL reservation.
    pub fn bind_pending_quota(
        &mut self,
        policy: PendingQuotaPolicy,
    ) -> Result<(), PendingQuotaError> {
        self.require_pending_accounting()
            .map_err(PendingQuotaError::Accounting)?;
        let manifest_sha256 = self.quota_manifest();
        match &self.pending_quota {
            PendingQuota::Unfunded => {
                self.pending_quota = PendingQuota::Bound(BoundQuota {
                    policy,
                    manifest_sha256,
                    pressure: false,
                    below_low_since: None,
                    entries: Vec::new(),
                });
                Ok(())
            }
            PendingQuota::RecoveryBlocked => Err(PendingQuotaError::RecoveryBlocked),
            PendingQuota::Bound(quota)
                if quota.policy == policy && quota.manifest_sha256 == manifest_sha256 =>
            {
                Ok(())
            }
            PendingQuota::Bound(_) => Err(PendingQuotaError::PolicyMismatch),
        }
    }
    /// Atomic aggregate preflight before the first original or PG reservation.
    /// Credit stays here when the connected adapter returns or is cancelled.
    pub fn reserve_pending_requests(
        &mut self,
        requests: &[PendingReservationRequest],
    ) -> Result<(), PendingQuotaError> {
        self.require_pending_accounting()
            .map_err(PendingQuotaError::Accounting)?;
        let quota = self.quota()?;
        if requests.len() > MAX_REQUESTS {
            return Err(PendingQuotaError::Limit);
        }
        let mut additional = 0u64;
        let mut count = 0usize;
        for (index, request) in requests.iter().enumerate() {
            if request.allowance_bytes > quota.policy.max_unit_bytes
                || request.request.producer_epoch.get()
                    != self.manifest.identity().owner_epoch.get()
            {
                return Err(PendingQuotaError::InsufficientAllowance);
            }
            if requests[..index]
                .iter()
                .any(|prior| prior.request == request.request)
            {
                return Err(PendingQuotaError::Conflict);
            }
            if let Some(entry) = quota
                .entries
                .iter()
                .find(|entry| entry.terms.request == request.request)
            {
                if entry.terms.digest != request.digest {
                    return Err(PendingQuotaError::Conflict);
                }
                if entry.terms.allowance_bytes < request.allowance_bytes {
                    return Err(PendingQuotaError::InsufficientAllowance);
                }
            } else {
                count += 1;
                additional = additional
                    .checked_add(request.allowance_bytes)
                    .ok_or(PendingQuotaError::Limit)?;
            }
        }
        if quota
            .entries
            .len()
            .checked_add(count)
            .is_none_or(|n| n > MAX_REQUESTS)
        {
            return Err(PendingQuotaError::Limit);
        }
        let total = self
            .pending_reserved_bytes()?
            .checked_add(self.accounting.pending_bytes())
            .and_then(|n| n.checked_add(additional));
        if additional > 0 && (quota.pressure || total.is_none_or(|n| n > quota.policy.high_bytes)) {
            let quota = self.quota_mut()?;
            quota.pressure = true;
            quota.below_low_since = None;
            return Err(PendingQuotaError::Pressure);
        }
        let quota = self.quota_mut()?;
        quota
            .entries
            .try_reserve_exact(count)
            .map_err(|_| PendingQuotaError::Limit)?;
        for request in requests {
            if !quota
                .entries
                .iter()
                .any(|entry| entry.terms.request == request.request)
            {
                quota.entries.push(Reservation {
                    terms: *request,
                    staging_started: false,
                    staged: false,
                });
            }
        }
        Ok(())
    }
    fn quota_manifest(&self) -> [u8; 32] {
        Sha256::digest(self.manifest.encode().as_bytes()).into()
    }
    fn quota(&self) -> Result<&BoundQuota, PendingQuotaError> {
        if self.native_unavailable() {
            return Err(PendingQuotaError::WriteUncertain);
        }
        match &self.pending_quota {
            PendingQuota::Unfunded => Err(PendingQuotaError::Unfunded),
            PendingQuota::RecoveryBlocked => Err(PendingQuotaError::RecoveryBlocked),
            PendingQuota::Bound(quota) if quota.manifest_sha256 == self.quota_manifest() => {
                Ok(quota)
            }
            PendingQuota::Bound(_) => Err(PendingQuotaError::PolicyMismatch),
        }
    }
    fn quota_mut(&mut self) -> Result<&mut BoundQuota, PendingQuotaError> {
        self.quota()?;
        let PendingQuota::Bound(quota) = &mut self.pending_quota else {
            unreachable!("checked quota")
        };
        Ok(quota)
    }
    pub(super) fn begin_pending_staging(
        &mut self,
        request: ProducerRequestId,
        bytes: &[u8],
    ) -> Result<(), PendingQuotaError> {
        let digest = Digest256::from_bytes(Sha256::digest(bytes).into());
        let quota = self.quota_mut()?;
        let entry = quota
            .entries
            .iter_mut()
            .find(|entry| entry.terms.request == request)
            .ok_or(PendingQuotaError::Unfunded)?;
        if entry.terms.digest != digest {
            return Err(PendingQuotaError::Conflict);
        }
        entry.staging_started = true;
        Ok(())
    }
    pub(super) fn finish_pending_staging(
        &mut self,
        request: ProducerRequestId,
    ) -> Result<(), PendingQuotaError> {
        let quota = self.quota_mut()?;
        let entry = quota
            .entries
            .iter_mut()
            .find(|entry| entry.terms.request == request)
            .ok_or(PendingQuotaError::Unfunded)?;
        if !entry.staging_started {
            return Err(PendingQuotaError::Unfunded);
        }
        entry.staged = true;
        Ok(())
    }
    pub(super) fn pending_funding(
        &self,
        request: ProducerRequestId,
        digest: Digest256,
    ) -> Result<PendingFunding, PendingQuotaError> {
        let quota = self.quota()?;
        let entry = quota
            .entries
            .iter()
            .find(|entry| entry.terms.request == request)
            .ok_or(PendingQuotaError::Unfunded)?;
        if entry.terms.digest != digest {
            return Err(PendingQuotaError::Conflict);
        }
        if !entry.staged {
            return Err(PendingQuotaError::Unfunded);
        }
        Ok(PendingFunding {
            terms: entry.terms,
            policy: quota.policy,
            manifest_sha256: quota.manifest_sha256,
            custody: Arc::clone(&self._reservation.custody),
        })
    }
    pub(super) fn validate_pending_funding(
        &self,
        funding: &PendingFunding,
        delta: u64,
    ) -> Result<(), PendingQuotaError> {
        let quota = self.quota()?;
        let held = self.pending_funding(funding.terms.request, funding.terms.digest)?;
        if !Arc::ptr_eq(&funding.custody, &self._reservation.custody)
            || funding.manifest_sha256 != held.manifest_sha256
            || funding.policy != held.policy
            || funding.terms != held.terms
        {
            return Err(PendingQuotaError::Conflict);
        }
        if delta > held.terms.allowance_bytes {
            return Err(PendingQuotaError::InsufficientAllowance);
        }
        if self
            .accounting
            .pending_bytes()
            .checked_add(self.pending_reserved_bytes()?)
            .is_none_or(|n| n > quota.policy.high_bytes)
        {
            return Err(PendingQuotaError::Pressure);
        }
        Ok(())
    }
    pub(super) fn settle_pending_funding(&mut self, funding: PendingFunding) {
        // Called only after known synchronized success and atomic P advancement.
        let PendingQuota::Bound(quota) = &mut self.pending_quota else {
            unreachable!("validated quota")
        };
        let index = quota
            .entries
            .iter()
            .position(|entry| entry.terms == funding.terms)
            .expect("validated retained funding");
        quota.entries.remove(index);
    }
}
