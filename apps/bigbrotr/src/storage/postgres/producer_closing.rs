//! Monotone native closing under current authority, targeting historical identity.
use super::*;
use crate::{
    runtime::{
        OwnedPermit,
        policy_authority::{LeaseResponse, PolicyLeaseTracker, PolicyLeaseTtl},
    },
    storage::rocks::{
        manifest::AllocationNamespaces,
        store::{BootstrapError, IngestRecovery, ProducerClosing, SourceResolution},
    },
};
use bigbrotr_model::{PolicyEpoch, ProducerEpoch};
use std::{
    path::Path,
    sync::atomic::{AtomicBool, Ordering},
};

/// Trusted factory coordinates and complete original registration, not a grant
/// to impersonate its Ingest incarnation or issue any new native request.
pub struct ProducerCloseRequest<'a> {
    pub ingest_root: &'a Path,
    pub registration: &'a ingest_registry::InitialRegistration,
    pub policy_ttl: Duration,
    pub cancelled: &'a AtomicBool,
}

/// Any failure after native dispatch can leave a synchronized barrier. Retry
/// the identical historical target under exclusion; never compensate by opening
/// the producer or treat a failed SQL commit as a negative native outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProducerOperationError<E> {
    Control(OwnerError),
    Registry(ingest_registry::RegistryError),
    Native(E),
    Reservation(super::super::producer_requests::ReservationError),
    Outcome(super::super::producer_outcomes::OutcomeError),
    CommitUnknown,
    DeliveryUnknown,
}
impl<E: fmt::Debug> fmt::Display for ProducerOperationError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "producer operation unavailable: {self:?}")
    }
}
impl<E: fmt::Debug> std::error::Error for ProducerOperationError<E> {}
impl<E> From<OwnerError> for ProducerOperationError<E> {
    fn from(value: OwnerError) -> Self {
        Self::Control(value)
    }
}

pub type ProducerCloseError = ProducerOperationError<BootstrapError>;

/// Both ends are needed: a durable reservation can precede a native admission.
/// This retained observation is not a resolved prefix or retirement permission.
pub struct InitialProducerClosing {
    native: ProducerClosing,
    reserved_through: u64,
}
impl InitialProducerClosing {
    pub fn native(&self) -> &ProducerClosing {
        &self.native
    }
    pub fn issued_through(&self) -> u64 {
        self.native.issued_through()
    }
    pub fn reserved_through(&self) -> u64 {
        self.reserved_through
    }
    pub fn source_resolution(
        self,
        recovery: &IngestRecovery,
    ) -> Result<SourceResolution<'_>, BootstrapError> {
        self.native
            .source_resolution(recovery, self.reserved_through)
    }
}

impl ControlSession {
    /// Installs/verifies the native barrier before confirming PG's scheduling
    /// veto. This does not resolve retained outcomes, advance ACK, close the
    /// source or activate a successor. Keep the bounded owner worker and database
    /// driver retained and joined; dropping this future cannot cancel native FFI.
    pub async fn close_initial_producer(
        &mut self,
        recovery: &mut IngestRecovery,
        input: ProducerCloseRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<InitialProducerClosing, ProducerCloseError> {
        if input.cancelled.load(Ordering::Relaxed) {
            return Err(ProducerCloseError::Native(
                BootstrapError::AuthorityUnavailable,
            ));
        }
        let spec = input.registration;
        let (tx, mut lease, current_epoch) = self
            .begin_producer_recovery(
                recovery,
                input.ingest_root,
                spec,
                input.policy_ttl,
                ingest_registry::RegistryLocks::Producer,
                validate,
            )
            .await?;
        // begin_producer_recovery already holds producer UPDATE. Closing and
        // reservations serialize on that row; a later reservation must see the
        // closing veto. Do not derive the recovery interval from native issued.
        const BOUND: &str = "
            SELECT reserved_through_request::text
            FROM control.producer_epochs
            WHERE producer_slot_id=$1::bytea AND producer_epoch=1";
        let row = tx
            .query_one(BOUND, &[&&spec.producer_slot_id.as_bytes()[..]])
            .await
            .map_err(backend)?;
        let reserved_through = row
            .try_get::<_, &str>(0)
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|n| *n <= u64::from(spec.outstanding_request_limit))
            .ok_or(OwnerError::InvalidMetadata)?;
        let closed = recovery
            .close_producer(
                spec.producer_slot_id,
                ProducerEpoch::new(1).expect("initial epoch"),
                permit,
                || {
                    if input.cancelled.load(Ordering::Relaxed) {
                        return Err(BootstrapError::AuthorityUnavailable);
                    }
                    lease
                        .check_delivery(current_epoch)
                        .map_err(|_| BootstrapError::AuthorityUnavailable)
                },
            )
            .map_err(ProducerCloseError::Native)?;
        if closed.issued_through() > reserved_through {
            // The native barrier may now be durable; preserve it on refusal.
            return Err(OwnerError::InvalidMetadata.into());
        }
        // Already-closing PG rows still require the native operation above. Do
        // not rewrite them or mistake PG's state for an attested native bound.
        tx.execute("UPDATE control.producer_epochs SET state='closing' WHERE producer_slot_id=$1::bytea AND producer_epoch=1 AND state='open'", &[&&spec.producer_slot_id.as_bytes()[..]]).await.map_err(backend)?;
        if input.cancelled.load(Ordering::Relaxed) || lease.check_delivery(current_epoch).is_err() {
            return Err(ProducerCloseError::DeliveryUnknown);
        }
        tx.commit()
            .await
            .map_err(|_| ProducerCloseError::CommitUnknown)?;
        if input.cancelled.load(Ordering::Relaxed)
            || self.client.is_closed()
            || lease.check_delivery(current_epoch).is_err()
        {
            return Err(ProducerCloseError::DeliveryUnknown);
        }
        self.available = true;
        Ok(InitialProducerClosing {
            native: closed,
            reserved_through,
        })
    }

    /// Shared current-Control / immutable historical-target lock and lease order.
    /// The caller retains native exclusion and cannot turn this into admission.
    pub(super) async fn begin_producer_recovery<'a, E>(
        &'a mut self,
        recovery: &IngestRecovery,
        ingest_root: &Path,
        spec: &ingest_registry::InitialRegistration,
        policy_ttl: Duration,
        locks: ingest_registry::RegistryLocks,
        mut validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<(Transaction<'a>, PolicyLeaseTracker, PolicyEpoch), ProducerOperationError<E>> {
        let identity = *recovery.manifest().identity();
        let domains = AllocationNamespaces::from_entries(&spec.allocation_domains)
            .map_err(|_| OwnerError::InvalidInput)?;
        if !recovery.matches_configured_root(ingest_root)
            || spec.dataset_id != self.request.dataset
            || spec.instance_id != *self.request.instance.as_bytes()
            || identity.dataset_id != spec.dataset_id
            || identity.owner_resource_id != spec.ingest_resource_id
            || identity.shard_id != spec.shard_id
            || identity.owner_epoch.get() != 1
            || identity.process_incarnation != spec.process_incarnation
            || identity.topology_generation
                != spec
                    .target_topology()
                    .map_err(ProducerOperationError::Registry)?
            || identity.policy_epoch.get() != spec.policy_revision
            || recovery.manifest().allocation_namespaces() != &domains
        {
            return Err(OwnerError::InvalidInput.into());
        }
        // Start before all coordination I/O. Historical policy is the lower
        // bound, while the authenticated current snapshot supplies this action's
        // actual epoch. Its lease belongs to the current Control incarnation.
        let mut lease = PolicyLeaseTracker::new(
            spec.dataset_id,
            self.identity.process_incarnation,
            identity.policy_epoch,
            PolicyLeaseTtl::new(policy_ttl).map_err(|_| OwnerError::InvalidInput)?,
        )
        .map_err(|_| OwnerError::Unavailable)?;
        let ticket = lease.begin_refresh().map_err(|_| OwnerError::Unavailable)?;
        self.begin_operation()?;
        let tx = application_transaction(&mut self.client, 14)
            .await
            .map_err(transaction_error)?;
        require(&tx, self.request.dataset, self.identity).await?;
        let policy = snapshot(&tx, &self.request, &mut validate).await?;
        ingest_registry::require_closing_registration(&tx, spec, locks)
            .await
            .map_err(ProducerOperationError::Registry)?;
        // Old Ingest scheduling expiry cannot prevent monotone recovery. Actual
        // local exclusion is held by IngestRecovery, not this scheduling lease.
        let remaining: i64 = tx.query_one("SELECT lease_expires_at_us - (EXTRACT(EPOCH FROM pg_catalog.clock_timestamp()) * 1000000)::bigint FROM control.owner_epochs WHERE resource_id=$1::bytea AND epoch=$2::text::numeric", &[&&self.identity.resource_id.as_bytes()[..], &self.identity.epoch.get().to_string()]).await.map_err(backend)?.try_get(0).map_err(backend)?;
        let remaining = u64::try_from(remaining)
            .ok()
            .filter(|n| *n > 0)
            .ok_or(OwnerError::Unavailable)?;
        let current_epoch = PolicyEpoch::new(policy.policy_revision);
        lease
            .complete_refresh(
                &ticket,
                LeaseResponse {
                    dataset: spec.dataset_id,
                    process_incarnation: self.identity.process_incarnation,
                    policy_epoch: current_epoch,
                    ttl: policy_ttl.min(Duration::from_micros(remaining)),
                },
            )
            .map_err(|_| OwnerError::Unavailable)?;
        Ok((tx, lease, current_epoch))
    }
}
