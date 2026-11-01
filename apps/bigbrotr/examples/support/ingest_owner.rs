//! Retained native-owner funding for the finite connected intake probes.
use bigbrotr::{
    runtime::{OwnedPermit, ResourceBudget, ResourceRequest},
    storage::{
        postgres::control_owner::{
            ControlSession, InitialEventError, InitialPendingQuotaRequest, OwnerError,
        },
        postgres::ingest_registry::InitialRegistration,
        rocks::store::{
            INITIAL_QUOTA_RECONCILIATION_DATA_BYTES, IngestStore, PENDING_QUOTA_OWNER_DATA_BYTES,
            PendingQuotaError, PendingQuotaStatus, RetainedRequestInput,
        },
    },
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

pub async fn reconcile_initial_pending_quota(
    owner: &mut ControlSession,
    store: &mut IngestStore,
    request: InitialPendingQuotaRequest<'_>,
    budget: &ResourceBudget,
    validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
) -> Result<PendingQuotaStatus, InitialEventError> {
    let data_grant = |bytes| {
        budget
            .try_acquire(ResourceRequest::data(bytes, 1, 0).unwrap())
            .unwrap()
    };
    let scratch = data_grant(INITIAL_QUOTA_RECONCILIATION_DATA_BYTES);
    let input =
        data_grant(RetainedRequestInput::required_data_bytes(request.limits.input).unwrap());
    let outcome = data_grant(request.limits.required_data_bytes().unwrap());
    owner
        .reconcile_initial_pending_quota(store, request, scratch, input, outcome, validate)
        .await
}

pub async fn wait_for_initial_pending_quota(
    store: &mut IngestStore,
    registration: &InitialRegistration,
) -> Result<(), PendingQuotaError> {
    let status = store.observe_pending_quota()?;
    let pending = store
        .pending_accounting()
        .map_err(PendingQuotaError::Accounting)?
        .pending_bytes()
        .checked_add(store.pending_reserved_bytes()?)
        .ok_or(PendingQuotaError::Limit)?;
    if pending >= registration.intake_policy.pending_low() {
        return Err(PendingQuotaError::Pressure);
    }
    if status == PendingQuotaStatus::Pressure {
        // No PG operation or authority lease is retained here. Exclusive
        // foreground custody preserves P while the configured interval elapses.
        tokio::time::sleep(registration.intake_policy.stable_duration()).await;
    }
    // Reobserve the real quota and sticky native failure guard after waiting.
    // Remaining pressure or unknown state is a finite refusal, never a retry.
    match store.observe_pending_quota()? {
        PendingQuotaStatus::Available => Ok(()),
        PendingQuotaStatus::Pressure => Err(PendingQuotaError::Pressure),
        PendingQuotaStatus::Unfunded => Err(PendingQuotaError::Unfunded),
        PendingQuotaStatus::RecoveryBlocked => Err(PendingQuotaError::RecoveryBlocked),
    }
}
