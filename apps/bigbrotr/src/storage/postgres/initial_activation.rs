//! Continuous local initial-writer activation. SQL records the boundary; only
//! the retained native candidate can become an admission handle after COMMIT.
use super::*;
use crate::{
    runtime::{
        OwnedPermit,
        policy_authority::{LeaseResponse, PolicyLeaseTracker, PolicyLeaseTtl},
    },
    storage::rocks::{
        manifest::IngestManifest,
        store::{
            ActiveInitialWriter, BootstrapError, IngestStore, InitialHandoff,
            InitialWriterCandidate, ObservedHandoff,
        },
    },
};
use bigbrotr_model::{DictionaryKind, PolicyEpoch, ProducerSlotId};
use std::sync::atomic::{AtomicBool, Ordering};

/// This object cannot be reconstructed from a receipt, manifest or registration.
/// Native exclusion and the bounded activation metadata remain owned together.
pub struct LiveInitialWriter {
    pub(super) native: ActiveInitialWriter,
    pub(super) binding: LiveInitialBinding,
    _permit: OwnedPermit,
}
pub(super) struct LiveInitialBinding {
    pub(super) previous: ingest_registry::InitialRegistration,
    pub(super) intent: initial_history::Intent,
    pub(super) control: ingest_registry::ControlOwner,
    activated_at_us: i64,
}
impl LiveInitialWriter {
    pub fn manifest(&self) -> &IngestManifest {
        self.native.manifest()
    }
    pub fn handoff(&self) -> &InitialHandoff {
        self.native.handoff()
    }
    pub fn producer_slot_id(&self) -> ProducerSlotId {
        self.native.producer_slot_id()
    }
    pub fn role_instance_id(&self) -> [u8; 16] {
        self.native.role_instance_id()
    }
    pub fn previous_registration(&self) -> &ingest_registry::InitialRegistration {
        &self.binding.previous
    }
    pub fn activated_at_us(&self) -> i64 {
        self.binding.activated_at_us
    }
}
impl LiveInitialBinding {
    pub(super) fn require_store(
        &self,
        store: &IngestStore,
        control: ingest_registry::ControlOwner,
    ) -> Result<(), OwnerError> {
        if control != self.control || store.manifest() != self.intent.record.next_manifest() {
            return Err(OwnerError::InvalidInput);
        }
        Ok(())
    }
    pub(super) async fn require(&self, tx: &Transaction<'_>) -> Result<u64, OwnerError> {
        let resource = self.previous.ingest_resource_id.as_bytes().as_slice();
        let remaining: i64 = tx
            .query_one(
                "SELECT control.require_active_initial_writer($1::bytea)",
                &[&resource],
            )
            .await
            .map_err(backend)?
            .try_get(0)
            .map_err(backend)?;
        let valid: bool = tx
            .query_one(
                "SELECT activated_by_resource=$2::bytea AND activated_by_epoch=$3::text::numeric
                AND activated_by_incarnation=$4::bytea AND activated_at_us=$5::bigint
                AND handoff_sha256=sha256($6::bytea)
             FROM control.initial_ingest_activations WHERE owner_resource_id=$1::bytea",
                &[
                    &resource,
                    &&self.control.resource_id.as_bytes()[..],
                    &self.control.epoch.get().to_string(),
                    &&self.control.process_incarnation.as_bytes()[..],
                    &self.activated_at_us,
                    &&self.intent.handoff[..],
                ],
            )
            .await
            .map_err(backend)?
            .try_get(0)
            .map_err(backend)?;
        if !valid {
            return Err(OwnerError::InvalidMetadata);
        }
        // The SQL classifier validates state; compare the complete finite native
        // namespace set independently, including each allocation-domain byte.
        let expected = self.intent.record.next_manifest().allocation_namespaces();
        let rows = tx
            .query(
                "SELECT dictionary_kind,allocation_domain FROM control.allocation_domains
            WHERE dataset_id=$1::bytea AND owner_resource_id=$2::bytea AND owner_epoch=2
            ORDER BY dictionary_kind,allocation_domain LIMIT 10 FOR SHARE",
                &[&&self.previous.dataset_id.as_bytes()[..], &resource],
            )
            .await
            .map_err(backend)?;
        let mut position = 0;
        for n in 1_u16..=9 {
            let kind = DictionaryKind::try_from(n).map_err(|_| OwnerError::InvalidMetadata)?;
            if let Some(domain) = expected.domain(kind) {
                let row = rows.get(position).ok_or(OwnerError::InvalidMetadata)?;
                if row.try_get::<_, i16>(0).map_err(backend)? != kind as i16
                    || row.try_get::<_, &[u8]>(1).map_err(backend)? != domain.as_bytes()
                {
                    return Err(OwnerError::InvalidMetadata);
                }
                position += 1;
            }
        }
        if rows.len() != position {
            return Err(OwnerError::InvalidMetadata);
        }
        u64::try_from(remaining)
            .ok()
            .filter(|v| *v > 0)
            .ok_or(OwnerError::Unavailable)
    }
}

impl ControlSession {
    /// Preparation remains non-authorizing. The extra candidate check binds its
    /// fresh private identities to the very native owner that supplied proof.
    pub async fn prepare_live_initial_successor(
        &mut self,
        candidate: &mut InitialWriterCandidate,
        input: InitialSuccessionRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<ObservedHandoff, InitialSuccessionError> {
        if candidate.role_instance_id() != input.role_instance_id {
            return Err(OwnerError::InvalidInput.into());
        }
        candidate
            .require_handoff(input.handoff)
            .map_err(InitialSuccessionError::Native)?;
        self.prepare_initial_successor(candidate.recovery_mut(), input, permit, validate)
            .await
    }

    /// Consumes the only local candidate, including every failure path. A lost
    /// COMMIT returns no writer and poisons Control. Recovery observes durable
    /// metadata; it cannot adopt the attempted incarnation.
    pub async fn activate_initial_writer(
        &mut self,
        candidate: InitialWriterCandidate,
        handoff: InitialHandoff,
        input: InitialHistoryRequest<'_>,
        permit: OwnedPermit,
        mut validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<LiveInitialWriter, InitialSuccessionError> {
        if permit.request().units().data_bytes() < 128 * 1024
            || permit.request().units().native_bytes() < 8192
            || permit.request().units().items() == 0
        {
            return Err(InitialSuccessionError::Native(
                BootstrapError::InsufficientReservation,
            ));
        }
        if input.cancelled.load(Ordering::Relaxed) {
            return Err(InitialSuccessionError::Native(
                BootstrapError::AuthorityUnavailable,
            ));
        }
        candidate
            .require_prepared_handoff(&handoff)
            .map_err(InitialSuccessionError::Native)?;
        let target = *handoff.next_manifest().identity();
        if !candidate
            .recovery()
            .matches_configured_root(input.ingest_root)
            || target.dataset_id != self.request.dataset
        {
            return Err(OwnerError::InvalidInput.into());
        }
        let mut lease = PolicyLeaseTracker::new(
            self.request.dataset,
            target.process_incarnation,
            target.policy_epoch,
            PolicyLeaseTtl::new(input.policy_ttl).map_err(|_| OwnerError::InvalidInput)?,
        )
        .map_err(|_| OwnerError::Unavailable)?;
        let ticket = lease.begin_refresh().map_err(|_| OwnerError::Unavailable)?;
        self.begin_operation()?;
        let tx = application_transaction(&mut self.client, 32)
            .await
            .map_err(transaction_error)?;
        lock_control(&tx, self.identity).await?;
        require(&tx, self.request.dataset, self.identity).await?;
        let policy = snapshot(&tx, &self.request, &mut validate).await?;
        let intent = initial_history::read_intent(&tx, candidate.recovery()).await?;
        if intent.handoff != handoff.encode().as_bytes()
            || intent.role != candidate.role_instance_id()
            || intent.prepared_resource != *self.identity.resource_id.as_bytes()
            || intent.prepared_epoch != self.identity.epoch.get()
            || intent.prepared_incarnation != *self.identity.process_incarnation.as_bytes()
            || intent.policy != policy
            || policy.policy_revision != target.policy_epoch.get()
        {
            return Err(OwnerError::InvalidMetadata.into());
        }
        let previous = ingest_registry::reconstruct_historical(
            &tx,
            *self.request.instance.as_bytes(),
            handoff.previous_manifest(),
        )
        .await
        .map_err(InitialSuccessionError::Registry)?;
        initial_history::require_intent(
            &tx,
            &previous,
            &intent,
            self.identity,
            target.policy_epoch,
        )
        .await?;
        let remaining = remaining(&tx, self.identity, previous.ingest_resource_id).await?;
        lease
            .complete_refresh(
                &ticket,
                LeaseResponse {
                    dataset: self.request.dataset,
                    process_incarnation: target.process_incarnation,
                    policy_epoch: target.policy_epoch,
                    ttl: input.policy_ttl.min(Duration::from_micros(remaining)),
                },
            )
            .map_err(|_| OwnerError::Unavailable)?;
        candidate
            .require_prepared_handoff(&handoff)
            .map_err(InitialSuccessionError::Native)?;
        check(&mut lease, target.policy_epoch, input.cancelled)?;
        let activated_at_us: i64=tx.query_one(
            "INSERT INTO control.initial_ingest_activations(dataset_id,owner_resource_id,target_epoch,target_incarnation,
                target_role_instance_id,producer_slot_id,producer_epoch,previous_manifest_sha256,target_manifest_sha256,handoff_sha256,
                prepared_by_resource,prepared_by_epoch,prepared_by_incarnation,activated_by_resource,activated_by_epoch,
                activated_by_incarnation,config_revision,policy_revision,config_digest,policy_digest,activated_at_us)
             SELECT dataset_id,owner_resource_id,target_epoch,target_incarnation,target_role_instance_id,producer_slot_id,producer_epoch,
                previous_manifest_sha256,target_manifest_sha256,handoff_sha256,prepared_by_resource,prepared_by_epoch,prepared_by_incarnation,
                $2::bytea,$3::text::numeric,$4::bytea,config_revision,policy_revision,config_digest,policy_digest,0
             FROM control.initial_ingest_successions WHERE owner_resource_id=$1::bytea RETURNING activated_at_us",
            &[&&target.owner_resource_id.as_bytes()[..],&&self.identity.resource_id.as_bytes()[..],
              &self.identity.epoch.get().to_string(),&&self.identity.process_incarnation.as_bytes()[..]])
            .await.map_err(backend)?.try_get(0).map_err(backend)?;
        check(&mut lease, target.policy_epoch, input.cancelled)?;
        tx.commit()
            .await
            .map_err(|_| InitialSuccessionError::CommitUnknown)?;
        check(&mut lease, target.policy_epoch, input.cancelled)?;
        let binding = LiveInitialBinding {
            previous,
            intent,
            control: self.identity,
            activated_at_us,
        };
        // Reacquire current custody without extending the pre-COMMIT deadline.
        let tx = application_transaction(&mut self.client, 32)
            .await
            .map_err(transaction_error)?;
        require(&tx, self.request.dataset, self.identity).await?;
        if snapshot(&tx, &self.request, &mut validate).await? != policy {
            return Err(OwnerError::InvalidPolicy.into());
        }
        binding.require(&tx).await?;
        candidate
            .require_prepared_handoff(&handoff)
            .map_err(InitialSuccessionError::Native)?;
        check(&mut lease, target.policy_epoch, input.cancelled)?;
        let native = candidate
            .activate(handoff)
            .map_err(InitialSuccessionError::Native)?;
        tx.rollback()
            .await
            .map_err(|_| InitialSuccessionError::DeliveryUnknown)?;
        check(&mut lease, target.policy_epoch, input.cancelled)?;
        if self.client.is_closed() {
            return Err(InitialSuccessionError::DeliveryUnknown);
        }
        self.available = true;
        Ok(LiveInitialWriter {
            native,
            binding,
            _permit: permit,
        })
    }

    /// Refresh Control first. This changes only the still-live Ingest deadline;
    /// activation and preparation receipts stay immutable. Never revives expiry.
    pub async fn renew_live_initial_ingest(
        &mut self,
        writer: &LiveInitialWriter,
        policy_ttl: Duration,
        mut validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
        commit_started: &Cell<bool>,
    ) -> Result<(), OwnerError> {
        let binding = &writer.binding;
        binding.require_store(writer.native.store(), self.identity)?;
        if binding.previous.dataset_id != self.request.dataset
            || binding.previous.instance_id != *self.request.instance.as_bytes()
        {
            return Err(OwnerError::InvalidInput);
        }
        let id = writer.manifest().identity();
        let mut lease = PolicyLeaseTracker::new(
            id.dataset_id,
            id.process_incarnation,
            id.policy_epoch,
            PolicyLeaseTtl::new(policy_ttl).map_err(|_| OwnerError::InvalidInput)?,
        )
        .map_err(|_| OwnerError::Unavailable)?;
        let ticket = lease.begin_refresh().map_err(|_| OwnerError::Unavailable)?;
        self.begin_operation()?;
        let tx = application_transaction(&mut self.client, 32)
            .await
            .map_err(transaction_error)?;
        require(&tx, self.request.dataset, self.identity).await?;
        if snapshot(&tx, &self.request, &mut validate).await? != binding.intent.policy {
            return Err(OwnerError::InvalidPolicy);
        }
        tx.query_one("SELECT epoch FROM control.owner_epochs WHERE resource_id=$1::bytea AND epoch=2 FOR UPDATE",
            &[&&id.owner_resource_id.as_bytes()[..]]).await.map_err(backend)?;
        let remaining = binding.require(&tx).await?;
        lease
            .complete_refresh(
                &ticket,
                LeaseResponse {
                    dataset: id.dataset_id,
                    process_incarnation: id.process_incarnation,
                    policy_epoch: id.policy_epoch,
                    ttl: policy_ttl.min(Duration::from_micros(remaining)),
                },
            )
            .map_err(|_| OwnerError::Unavailable)?;
        let changed=tx.execute("WITH observed AS MATERIALIZED (SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint AS now_us)
            UPDATE control.owner_epochs i SET lease_expires_at_us=c.lease_expires_at_us FROM control.owner_epochs c,observed
            WHERE c.resource_id=$1::bytea AND c.epoch=$2::text::numeric AND i.resource_id=$3::bytea AND i.epoch=2
                AND c.lease_expires_at_us>observed.now_us AND i.lease_expires_at_us>observed.now_us",
            &[&&self.identity.resource_id.as_bytes()[..],&self.identity.epoch.get().to_string(),&&id.owner_resource_id.as_bytes()[..]])
            .await.map_err(backend)?;
        if changed != 1 {
            return Err(OwnerError::Unavailable);
        }
        lease
            .check_delivery(id.policy_epoch)
            .map_err(|_| OwnerError::Unavailable)?;
        commit_started.set(true);
        tx.commit().await.map_err(|_| OwnerError::CommitUnknown)?;
        if self.client.is_closed() || lease.check_delivery(id.policy_epoch).is_err() {
            return Err(OwnerError::Unavailable);
        }
        self.available = true;
        Ok(())
    }
}
async fn lock_control(
    tx: &Transaction<'_>,
    control: ingest_registry::ControlOwner,
) -> Result<(), OwnerError> {
    tx.query_one("SELECT epoch FROM control.owner_epochs WHERE resource_id=$1::bytea AND epoch=$2::text::numeric FOR UPDATE",
        &[&&control.resource_id.as_bytes()[..],&control.epoch.get().to_string()]).await.map_err(backend)?;
    Ok(())
}
async fn remaining(
    tx: &Transaction<'_>,
    control: ingest_registry::ControlOwner,
    resource: OwnerResourceId,
) -> Result<u64, OwnerError> {
    let us:i64=tx.query_one("SELECT LEAST(c.lease_expires_at_us,i.lease_expires_at_us)-(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint
        FROM control.owner_epochs c CROSS JOIN control.owner_epochs i WHERE c.resource_id=$1::bytea AND c.epoch=$2::text::numeric
            AND i.resource_id=$3::bytea AND i.epoch=2",
        &[&&control.resource_id.as_bytes()[..],&control.epoch.get().to_string(),&&resource.as_bytes()[..]])
        .await.map_err(backend)?.try_get(0).map_err(backend)?;
    u64::try_from(us)
        .ok()
        .filter(|v| *v > 0)
        .ok_or(OwnerError::Unavailable)
}
fn check(
    lease: &mut PolicyLeaseTracker,
    epoch: PolicyEpoch,
    cancelled: &AtomicBool,
) -> Result<(), InitialSuccessionError> {
    if cancelled.load(Ordering::Relaxed) || lease.check_delivery(epoch).is_err() {
        Err(InitialSuccessionError::DeliveryUnknown)
    } else {
        Ok(())
    }
}
