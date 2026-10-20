//! Continuous candidate activation. The caller retains native custody on every
//! error, including unknown COMMIT and failed final delivery. No receipt loader
//! or reconstructed gap can manufacture this envelope or an admitted writer.
use super::*;
use crate::{
    runtime::policy_authority::{LeaseResponse, PolicyLeaseTtl},
    storage::rocks::{
        manifest::IngestManifest,
        store::{
            ActiveSuccessorWriter, AdmissionLimits, DictionaryPreparation,
            NativeSuccessorActivation,
        },
    },
};
use bigbrotr_model::{ProducerSlotId, QualifiedRef};
use std::sync::atomic::AtomicBool;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ActivationState {
    Ready,
    Spent,
}
impl ActivationState {
    fn start(&mut self) -> Result<(), OwnerError> {
        if *self != Self::Ready {
            return Err(OwnerError::Unavailable);
        }
        *self = Self::Spent;
        Ok(())
    }
}

/// Infallible native-first custody move. Invalid or already spent preparation is
/// retained too, but can never activate. Keep this owner ahead of Control in the
/// supervisor's drop order until success moves native into the returned writer.
pub struct SuccessorActivation {
    native: NativeSuccessorActivation,
    intent: Intent,
    history_control: ingest_registry::ControlOwner,
    state: ActivationState,
}
impl SuccessorActivation {
    pub fn new(preparation: SuccessorPreparation) -> Self {
        let state = if preparation.is_prepared() {
            ActivationState::Ready
        } else {
            ActivationState::Spent
        };
        let history_control = preparation.history.control();
        Self {
            native: NativeSuccessorActivation::new(
                preparation.candidate,
                preparation.history.into_lineage(),
                preparation.plan.native,
            ),
            intent: preparation.plan.intent,
            history_control,
            state,
        }
    }
    /// None only after successful delivery has transferred the same native owner.
    pub fn recovery(&self) -> Option<&IngestRecovery> {
        self.native.recovery()
    }
    pub fn record(&self) -> &SuccessionRecord {
        &self.intent.record
    }
}

/// Non-reconstructible native frontier and immutable SQL activation binding.
/// The original lineage allocation/permit remains owned by the native wrapper.
pub struct LiveSuccessorWriter {
    pub(in crate::storage::postgres::control_owner) native: ActiveSuccessorWriter,
    pub(in crate::storage::postgres::control_owner) binding: LiveSuccessorBinding,
}
#[derive(Clone, Copy)]
pub(in crate::storage::postgres::control_owner) struct LiveSuccessorBinding {
    intent: Intent,
    instance: [u8; 16],
    activated_at_us: i64,
    transaction: u64,
}
impl LiveSuccessorWriter {
    pub fn producer_credit(
        &self,
    ) -> Result<crate::storage::rocks::store::ProducerCredit, AdmissionError> {
        self.native.producer_credit()
    }
    pub fn manifest(&self) -> &IngestManifest {
        self.native.manifest()
    }
    pub fn record(&self) -> &SuccessionRecord {
        self.native.record()
    }
    pub fn producer_slot_id(&self) -> ProducerSlotId {
        self.native.producer_slot_id()
    }
    pub fn role_instance_id(&self) -> [u8; 16] {
        self.native.role_instance_id()
    }
    pub fn activated_at_us(&self) -> i64 {
        self.binding.activated_at_us
    }
    pub fn policy_snapshot(&self) -> PolicySnapshot {
        self.binding.intent.policy
    }
    pub fn outstanding_request_limit(&self) -> u32 {
        self.binding.intent.limit
    }
    pub fn prepare_dictionaries<'a>(
        &'a self,
        visibility: QualifiedRef,
        limits: AdmissionLimits,
        permit: OwnedPermit,
        cancelled: &'a AtomicBool,
    ) -> Result<DictionaryPreparation<'a>, AdmissionError> {
        self.native
            .prepare_dictionaries(visibility, limits, permit, cancelled)
    }
}

// No independent history or metadata grant. These fixed bindings, both the
// transient intent projection and the native hashing scratch fit the original
// lineage's 128KiB scratch; transport/policy documents retain separate grants.
const _: () = assert!(
    size_of::<SuccessorActivation>()
        + size_of::<LiveSuccessorWriter>()
        + size_of::<Intent>()
        + 16 * 1024
        < LINEAGE_SCRATCH_BYTES as usize
);

impl LiveSuccessorBinding {
    pub(in crate::storage::postgres::control_owner) fn dataset(&self) -> DatasetId {
        self.intent
            .record
            .fields()
            .next_manifest
            .identity()
            .dataset_id
    }
    pub(in crate::storage::postgres::control_owner) fn instance(&self) -> [u8; 16] {
        self.instance
    }
    pub(in crate::storage::postgres::control_owner) fn slot(&self) -> ProducerSlotId {
        self.intent.record.fields().producer_slot_id
    }
    pub(in crate::storage::postgres::control_owner) fn epoch(&self) -> u64 {
        self.intent.epoch()
    }
    pub(in crate::storage::postgres::control_owner) fn control(
        &self,
    ) -> ingest_registry::ControlOwner {
        self.intent.preparer
    }
    pub(in crate::storage::postgres::control_owner) fn policy(&self) -> PolicySnapshot {
        self.intent.policy
    }
    pub(in crate::storage::postgres::control_owner) fn limit(&self) -> u32 {
        self.intent.limit
    }
    pub(in crate::storage::postgres::control_owner) fn require_manifest(
        &self,
        manifest: &IngestManifest,
        control: ingest_registry::ControlOwner,
    ) -> Result<(), OwnerError> {
        if control != self.control() || manifest != &self.intent.record.fields().next_manifest {
            return Err(OwnerError::InvalidInput);
        }
        Ok(())
    }
    pub(in crate::storage::postgres::control_owner) async fn lock_control(
        &self,
        tx: &Transaction<'_>,
    ) -> Result<(), OwnerError> {
        rows::lock_control(tx, self.control())
            .await
            .map_err(sql_error)
    }
    pub(in crate::storage::postgres::control_owner) async fn require(
        &self,
        tx: &Transaction<'_>,
    ) -> Result<u64, OwnerError> {
        // The complete flat classifier locks and validates every retained parent;
        // no epoch-specific registration is fabricated or old loader weakened.
        let remaining = rows::require_target(tx, &self.intent, "active")
            .await
            .map_err(sql_error)?
            .ok_or(OwnerError::InvalidMetadata)?;
        rows::require_intent(tx, &self.intent)
            .await
            .map_err(sql_error)?;
        let f = self.intent.record.fields();
        let id = f.next_manifest.identity();
        let manifest = f.next_manifest.encode();
        let handoff = self.intent.record.encode();
        let valid: bool = tx.query_one("SELECT ROW(dataset_id,target_incarnation,target_role_instance_id,
            producer_slot_id,producer_epoch,target_manifest_sha256,handoff_sha256,
            activated_by_resource,activated_by_epoch,activated_by_incarnation,activated_at_us,activation_transaction)
            IS NOT DISTINCT FROM ROW($3::bytea,$4::bytea,$5::bytea,$6::bytea,$2::text::numeric,
                sha256($7::bytea),sha256($8::bytea),$9::bytea,$10::text::numeric,$11::bytea,$12::bigint,$13::text::xid8)
            AND activation_transaction<>pg_current_xact_id()
            FROM control.ingest_activations WHERE owner_resource_id=$1::bytea AND target_epoch=$2::text::numeric", &[
            &&id.owner_resource_id.as_bytes()[..],&self.epoch().to_string(),&&id.dataset_id.as_bytes()[..],
            &&id.process_incarnation.as_bytes()[..],&&self.intent.role[..],&&f.producer_slot_id.as_bytes()[..],
            &manifest.as_bytes(),&handoff.as_bytes(),&&self.control().resource_id.as_bytes()[..],
            &self.control().epoch.get().to_string(),&&self.control().process_incarnation.as_bytes()[..],
            &self.activated_at_us,&self.transaction.to_string(),
        ]).await.map_err(backend)?.try_get(0).map_err(backend)?;
        if !valid {
            return Err(OwnerError::InvalidMetadata);
        }
        positive_remaining(remaining)
    }
}

impl ControlSession {
    pub async fn activate_successor_writer(
        &mut self,
        owner: &mut SuccessorActivation,
        input: InitialHistoryRequest<'_>,
        mut validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<LiveSuccessorWriter, ProducerResolveError> {
        let result = async {
            // Spend before cancellation, allocation validation or any I/O. A
            // future dropped at an await also leaves this envelope spent.
            owner.state.start()?;
            require_control(owner.history_control, owner.intent.preparer, self.identity)?;
            owner.native.require_prepared(input.cancelled).map_err(native_error)?;
            let recovery = owner.recovery().ok_or(OwnerError::Unavailable)?;
            let intent = owner.intent;
            let id = *intent.record.fields().next_manifest.identity();
            if !recovery.matches_configured_root(input.ingest_root) || id.dataset_id != self.request.dataset {
                return Err(OwnerError::InvalidInput.into());
            }
            let current = self.identity;
            let instance = *self.request.instance.as_bytes();
            let mut lease = PolicyLeaseTracker::new(id.dataset_id,id.process_incarnation,id.policy_epoch,
                PolicyLeaseTtl::new(input.policy_ttl).map_err(|_|OwnerError::InvalidInput)?)
                .map_err(|_|OwnerError::Unavailable)?;
            let ticket = lease.begin_refresh().map_err(|_|OwnerError::Unavailable)?;
            self.begin_operation()?;
            let tx = application_transaction(&mut self.client,38).await.map_err(transaction_error)?;
            rows::lock_control(&tx,current).await?;
            require(&tx,id.dataset_id,current).await?;
            if snapshot(&tx,&self.request,&mut validate).await? != intent.policy {
                return Err(OwnerError::InvalidPolicy.into());
            }
            rows::require_intent(&tx,&intent).await?;
            rows::require_target(&tx,&intent,"prepared").await?;
            let remaining = remaining(&tx,&intent).await?;
            lease.complete_refresh(&ticket,LeaseResponse {dataset:id.dataset_id,
                process_incarnation:id.process_incarnation,policy_epoch:id.policy_epoch,
                ttl:input.policy_ttl.min(Duration::from_micros(remaining))})
                .map_err(|_|OwnerError::Unavailable)?;
            owner.native.require_prepared(input.cancelled).map_err(native_error)?;
            initial_history::check_delivery(&mut lease,id.policy_epoch,input.cancelled)?;
            let row = tx.query_one("INSERT INTO control.ingest_activations(dataset_id,owner_resource_id,target_epoch,
                target_incarnation,target_role_instance_id,producer_slot_id,producer_epoch,target_manifest_sha256,handoff_sha256,
                activated_by_resource,activated_by_epoch,activated_by_incarnation,activated_at_us)
                SELECT dataset_id,owner_resource_id,target_epoch,target_incarnation,target_role_instance_id,producer_slot_id,
                    producer_epoch,target_manifest_sha256,handoff_sha256,prepared_by_resource,prepared_by_epoch,prepared_by_incarnation,0
                FROM control.ingest_successions WHERE owner_resource_id=$1::bytea AND target_epoch=$2::text::numeric
                RETURNING activated_at_us,activation_transaction::text",&[&&id.owner_resource_id.as_bytes()[..],&intent.epoch().to_string()])
                .await.map_err(backend)?;
            let binding = LiveSuccessorBinding { intent,instance,activated_at_us:row.try_get(0).map_err(backend)?,
                transaction:ingest_registry::decimal(row.try_get(1).map_err(backend)?)
                    .map_err(|_|OwnerError::InvalidMetadata)? };
            drop(row);
            initial_history::check_delivery(&mut lease,id.policy_epoch,input.cancelled)?;
            tx.commit().await.map_err(|_|ProducerResolveError::CommitUnknown)?;
            initial_history::check_delivery(&mut lease,id.policy_epoch,input.cancelled)?;

            // A fresh SQL observation never restarts the pre-COMMIT monotonic
            // lease. Promotion is postponed until this rollback is known.
            let tx = application_transaction(&mut self.client,38).await.map_err(transaction_error)?;
            rows::lock_control(&tx,current).await?;
            require(&tx,id.dataset_id,current).await?;
            if snapshot(&tx,&self.request,&mut validate).await? != intent.policy {
                return Err(OwnerError::InvalidPolicy.into());
            }
            binding.require(&tx).await?;
            owner.native.require_prepared(input.cancelled).map_err(native_error)?;
            initial_history::check_delivery(&mut lease,id.policy_epoch,input.cancelled)?;
            tx.rollback().await.map_err(|_|ProducerResolveError::DeliveryUnknown)?;
            // The callback follows native's last complete validation and precedes
            // its infallible custody transfer. Nothing fallible follows the take.
            let native = owner.native.activate(input.cancelled,|| {
                if self.client.is_closed() { return Err(BootstrapError::AuthorityUnavailable); }
                native_authority(&mut lease,id.policy_epoch,input.cancelled)
            }).map_err(native_error)?;
            Ok(LiveSuccessorWriter {native,binding})
        }.await;
        self.available = result.is_ok();
        result
    }

    /// Synchronize the still-live target deadline to current Control. This may
    /// shorten it; immutable preparation/activation coordinates never change.
    pub async fn renew_live_successor_ingest(
        &mut self,
        writer: &LiveSuccessorWriter,
        policy_ttl: Duration,
        mut validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
        commit_started: &Cell<bool>,
    ) -> Result<(), OwnerError> {
        let binding = &writer.binding;
        binding.require_manifest(writer.manifest(), self.identity)?;
        if binding.dataset() != self.request.dataset
            || binding.instance() != *self.request.instance.as_bytes()
        {
            return Err(OwnerError::InvalidInput);
        }
        let cancelled = AtomicBool::new(false);
        writer
            .native
            .validate(&cancelled)
            .map_err(|_| OwnerError::InvalidMetadata)?;
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
        let tx = application_transaction(&mut self.client, 38)
            .await
            .map_err(transaction_error)?;
        binding.lock_control(&tx).await?;
        require(&tx, self.request.dataset, self.identity).await?;
        if snapshot(&tx, &self.request, &mut validate).await? != binding.policy() {
            return Err(OwnerError::InvalidPolicy);
        }
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
        let changed = tx.execute("WITH observed AS MATERIALIZED(SELECT (extract(epoch FROM clock_timestamp())*1000000)::bigint AS now_us)
            UPDATE control.owner_epochs i SET lease_expires_at_us=c.lease_expires_at_us FROM control.owner_epochs c,observed
            WHERE c.resource_id=$1::bytea AND c.epoch=$2::text::numeric AND i.resource_id=$3::bytea AND i.epoch=$4::text::numeric
                AND c.lease_expires_at_us>observed.now_us AND i.lease_expires_at_us>observed.now_us",&[
            &&self.identity.resource_id.as_bytes()[..],&self.identity.epoch.get().to_string(),
            &&id.owner_resource_id.as_bytes()[..],&binding.epoch().to_string()]).await.map_err(backend)?;
        if changed != 1 {
            return Err(OwnerError::Unavailable);
        }
        binding.require(&tx).await?;
        writer
            .native
            .validate(&cancelled)
            .map_err(|_| OwnerError::InvalidMetadata)?;
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

async fn remaining(tx: &Transaction<'_>, intent: &Intent) -> Result<u64, OwnerError> {
    let resource = intent
        .record
        .fields()
        .next_manifest
        .identity()
        .owner_resource_id;
    let value:i64=tx.query_one("SELECT least(c.lease_expires_at_us,i.lease_expires_at_us)-(extract(epoch FROM clock_timestamp())*1000000)::bigint
        FROM control.owner_epochs c CROSS JOIN control.owner_epochs i WHERE c.resource_id=$1::bytea AND c.epoch=$2::text::numeric
            AND i.resource_id=$3::bytea AND i.epoch=$4::text::numeric",&[
        &&intent.preparer.resource_id.as_bytes()[..],&intent.preparer.epoch.get().to_string(),
        &&resource.as_bytes()[..],&intent.epoch().to_string()]).await.map_err(backend)?.try_get(0).map_err(backend)?;
    positive_remaining(value)
}
fn positive_remaining(value: i64) -> Result<u64, OwnerError> {
    u64::try_from(value)
        .ok()
        .filter(|n| *n > 0)
        .ok_or(OwnerError::Unavailable)
}
fn sql_error(error: ProducerResolveError) -> OwnerError {
    match error {
        ProducerResolveError::Control(error) => error,
        _ => OwnerError::InvalidMetadata,
    }
}

#[cfg(test)]
#[path = "successor_activation_tests.rs"]
mod tests;
