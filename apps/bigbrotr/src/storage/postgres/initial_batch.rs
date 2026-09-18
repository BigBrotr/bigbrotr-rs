//! Connected initial native batch under the dedicated Control backend.
use super::*;
use crate::{
    runtime::{
        OwnedPermit,
        policy_authority::{LeaseResponse, PolicyLeaseTracker, PolicyLeaseTtl},
    },
    storage::rocks::{
        manifest::AllocationNamespaces,
        store::{BootstrapError, BootstrapLimits, IngestStore, StoredReceipt},
    },
};
use bigbrotr_model::{
    EvidenceIntent, ObservedTime, PolicyEpoch, ProducerEpoch, ProducerRequestId, RequestSequence,
};
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InitialBatchError {
    Control(OwnerError),
    Registry(ingest_registry::RegistryError),
    Native(BootstrapError),
    /// Native work was dispatched. Its retained request must be resolved even
    /// when later coordination cleanup or output authorization is unavailable.
    DeliveryUnknown,
}
impl fmt::Display for InitialBatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "initial native batch unavailable: {self:?}")
    }
}
impl std::error::Error for InitialBatchError {}
impl From<OwnerError> for InitialBatchError {
    fn from(value: OwnerError) -> Self {
        Self::Control(value)
    }
}

/// Expected data, not a grant. Both original input and whole PG document buffers
/// require caller reservations, in addition to the transferred batch permit.
pub struct InitialBatchRequest<'a> {
    pub registration: &'a ingest_registry::InitialRegistration,
    pub intent: &'a EvidenceIntent,
    pub recorded_at: ObservedTime,
    pub limits: BootstrapLimits,
    pub policy_ttl: Duration,
    pub cancelled: &'a AtomicBool,
}

impl ControlSession {
    /// One initial dictionary bootstrap, including exact retained replay.
    /// Run on a dedicated bounded owner worker: native FFI is synchronous and
    /// cannot be cancelled by dropping a future. Retain/join that worker and the
    /// PG driver before close, replacement or releasing their reservations.
    /// The validator checks both complete documents against actual static roots.
    /// No public network handler may supply an arbitrary validator or root.
    pub async fn bootstrap_initial_dictionaries(
        &mut self,
        store: &mut IngestStore,
        input: InitialBatchRequest<'_>,
        permit: OwnedPermit,
        mut validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<StoredReceipt, InitialBatchError> {
        let spec = input.registration;
        let identity = *store.manifest().identity();
        let domains = AllocationNamespaces::from_entries(&spec.allocation_domains)
            .map_err(|_| OwnerError::InvalidInput)?;
        if spec.dataset_id != self.request.dataset
            || spec.instance_id != *self.request.instance.as_bytes()
            || spec.control != self.identity
            || identity.dataset_id != spec.dataset_id
            || identity.owner_resource_id != spec.ingest_resource_id
            || identity.shard_id != spec.shard_id
            || identity.owner_epoch.get() != 1
            || identity.process_incarnation != spec.process_incarnation
            || identity.topology_generation
                != spec
                    .target_topology()
                    .map_err(InitialBatchError::Registry)?
            || identity.policy_epoch.get() != spec.policy_revision
            || store.manifest().allocation_namespaces() != &domains
        {
            return Err(OwnerError::InvalidInput.into());
        }
        let mut lease = PolicyLeaseTracker::new(
            spec.dataset_id,
            spec.process_incarnation,
            identity.policy_epoch,
            PolicyLeaseTtl::new(input.policy_ttl).map_err(|_| OwnerError::InvalidInput)?,
        )
        .map_err(|_| OwnerError::Unavailable)?;
        let ticket = lease.begin_refresh().map_err(|_| OwnerError::Unavailable)?;
        self.begin_operation()?;
        let tx = application_transaction(&mut self.client, 7)
            .await
            .map_err(transaction_error)?;
        require(&tx, self.request.dataset, self.identity).await?;
        let policy = snapshot(&tx, &self.request, &mut validate).await?;
        if policy.config_revision != spec.config_revision
            || policy.policy_revision != spec.policy_revision
            || policy.config_digest != *spec.config_digest.as_bytes()
            || policy.policy_digest != *spec.policy_digest.as_bytes()
        {
            return Err(OwnerError::InvalidPolicy.into());
        }
        ingest_registry::require_initial_registration(&tx, spec)
            .await
            .map_err(InitialBatchError::Registry)?;
        // observe() already holds row locks through native completion, including
        // the producer closing barrier. A scheduling TTL alone is no exclusion.
        let remaining:i64 = tx.query_one("SELECT LEAST(c.lease_expires_at_us, i.lease_expires_at_us) - (EXTRACT(EPOCH FROM pg_catalog.clock_timestamp()) * 1000000)::bigint FROM control.owner_epochs c CROSS JOIN control.owner_epochs i WHERE c.resource_id = $1::bytea AND c.epoch = $2::text::numeric AND i.resource_id = $3::bytea AND i.epoch = 1", &[&&spec.control.resource_id.as_bytes()[..],&spec.control.epoch.get().to_string(),&&spec.ingest_resource_id.as_bytes()[..]])
            .await.map_err(backend)?.try_get(0).map_err(backend)?;
        let remaining = u64::try_from(remaining)
            .ok()
            .filter(|v| *v > 0)
            .ok_or(OwnerError::Unavailable)?;
        lease
            .complete_refresh(
                &ticket,
                LeaseResponse {
                    dataset: spec.dataset_id,
                    process_incarnation: spec.process_incarnation,
                    policy_epoch: PolicyEpoch::new(policy.policy_revision),
                    ttl: input.policy_ttl.min(Duration::from_micros(remaining)),
                },
            )
            .map_err(|_| OwnerError::Unavailable)?;
        let result = store.bootstrap_dictionaries(
            ProducerRequestId {
                producer_slot_id: spec.producer_slot_id,
                producer_epoch: ProducerEpoch::new(1).map_err(|_| OwnerError::InvalidInput)?,
                request_sequence: RequestSequence::new(1).map_err(|_| OwnerError::InvalidInput)?,
            },
            input.intent,
            input.recorded_at,
            input.limits,
            permit,
            || {
                if input.cancelled.load(Ordering::Relaxed) {
                    return Err(BootstrapError::AuthorityUnavailable);
                }
                lease
                    .check_delivery(identity.policy_epoch)
                    .map_err(|_| BootstrapError::AuthorityUnavailable)
            },
        );
        // This transaction observes/fences; it makes no scientific PG writes.
        // A failure here cannot roll back a successful synchronized Rocks batch.
        tx.rollback()
            .await
            .map_err(|_| InitialBatchError::DeliveryUnknown)?;
        let receipt = result.map_err(InitialBatchError::Native)?;
        if input.cancelled.load(Ordering::Relaxed)
            || self.client.is_closed()
            || lease.check_delivery(identity.policy_epoch).is_err()
        {
            return Err(InitialBatchError::DeliveryUnknown);
        }
        self.available = true;
        Ok(receipt)
    }
}
