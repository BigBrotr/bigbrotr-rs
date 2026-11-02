//! Serialized initial-owner reclamation. The PostgreSQL prepare, native floor,
//! authenticated input delta and SQL promotion keep one unavailable-on-error
//! session and retain all operation grants through final delivery.
use super::*;
use crate::{
    runtime::ResourceBudget,
    storage::rocks::store::{PrefixRetirement, ProducerRecoveryLimits},
};

pub struct InitialPrefixRequest<'a> {
    pub ingest_root: &'a std::path::Path,
    pub registration: &'a ingest_registry::InitialRegistration,
    pub policy_ttl: Duration,
    pub cancelled: &'a AtomicBool,
}
impl InitialPrefixRequest<'_> {
    pub const REQUIRED_METADATA_DATA_BYTES: u64 = PrefixRetirement::DATA_BYTES;
    pub const REQUIRED_METADATA_NATIVE_BYTES: u64 = PrefixRetirement::NATIVE_BYTES;

    fn operation(&self) -> InitialIngestContext<'_> {
        InitialIngestContext {
            ingest_root: self.ingest_root,
            authority: NativeAuthority::Initial(self.registration),
            cancelled: self.cancelled,
            policy_ttl: self.policy_ttl,
            request: None,
            source: None,
            monitor: None,
            network_event: false,
            job: None,
            progress: true,
        }
    }
}

/// Reporting only, after confirmed finalization. Scientific pending bytes and
/// batch count are unchanged by receipt/input reclamation. No growth credit or
/// native/recovery capability can be reconstructed from this value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerPrefixReceipt {
    pub generation: u64,
    pub base_sha256: [u8; 32],
    pub previous_retired_through: u64,
    pub retired_through: u64,
    pub source_through: u64,
    pub pending_batches: u64,
    pub pending_bytes: u64,
}

impl ControlSession {
    /// Called at the initial owner's serialized safe point, after any mutation
    /// and its confirmed checkpoint/ACK. The complete captured startup setting
    /// controls scheduling; this method never polls a separate numeric value.
    /// None reports only that its retained-count threshold was not reached.
    /// It grants no permission to ignore unresolved work or a capacity refusal.
    ///
    /// Keep the outer deadline, blocking native custody, connection driver and
    /// every permit through cancellation/termination. Any failed/unknown stage
    /// leaves this session unavailable; fresh recovery must classify the exact
    /// prepared/active slots and native floor before effects or dispatch.
    #[allow(
        clippy::too_many_arguments,
        reason = "separate retained grants for progress, replay, native metadata and original input reads"
    )]
    pub async fn reclaim_initial_prefix(
        &mut self,
        store: &mut IngestStore,
        input: InitialPrefixRequest<'_>,
        limits: ProducerRecoveryLimits,
        budget: &ResourceBudget,
        progress: &ProgressGrant,
        effects: &OwnedPermit,
        metadata: OwnedPermit,
        mut validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<Option<ProducerPrefixReceipt>, InitialEventError> {
        if effects.request().units().items() == 0
            || effects.request().units().data_bytes()
                < limits
                    .required_data_bytes()
                    .map_err(InitialEventError::Native)?
            || metadata.request().units().items() == 0
            || metadata.request().units().data_bytes() < PrefixRetirement::DATA_BYTES
            || metadata.request().units().native_bytes() < PrefixRetirement::NATIVE_BYTES
        {
            return Err(InitialEventError::Native(
                AdmissionError::InsufficientReservation,
            ));
        }
        let context =
            NativeAuthority::Initial(input.registration).progress_context(store.manifest());
        let control = self.identity;
        let policy = PolicyEpoch::new(input.registration.policy_revision);
        let (tx, mut lease) = self
            .begin_initial_native_operation::<AdmissionError>(
                &NativeOwner::Store(&mut *store),
                input.operation(),
                &mut validate,
            )
            .await?;
        let enabled: bool = tx.query_one(
            "SELECT EXISTS(SELECT 1 FROM control.schema_versions WHERE engine='postgres-control' AND migration=42)", &[],
        ).await.map_err(backend)?.try_get(0).map_err(backend)?;
        if !enabled {
            return Err(OwnerError::InvalidMetadata.into());
        }
        let pending = producer_progress::prefix::prepare(
            &tx,
            context,
            store,
            control,
            policy,
            progress,
            input.registration.intake_policy.request_receipt_retention(),
            limits,
            budget,
            effects,
            metadata,
            input.cancelled,
            || producer_progress::recovery::authority(&mut lease, policy, input.cancelled),
        )
        .await
        .map_err(InitialEventError::Progress)?;
        let Some(pending) = pending else {
            tx.rollback().await.map_err(backend)?;
            initial_history::check_delivery(&mut lease, policy, input.cancelled)
                .map_err(InitialEventError::Compacted)?;
            if self.client.is_closed() || store.native_unavailable() {
                return Err(InitialEventError::DeliveryUnknown);
            }
            self.available = true;
            return Ok(None);
        };
        initial_history::check_delivery(&mut lease, policy, input.cancelled)
            .map_err(InitialEventError::Compacted)?;
        if store.native_unavailable() {
            return Err(InitialEventError::Native(AdmissionError::CommitUnknown));
        }
        let confirmed = pending
            .commit(tx)
            .await
            .map_err(InitialEventError::Progress)?;
        if self.client.is_closed() || store.native_unavailable() {
            return Err(InitialEventError::DeliveryUnknown);
        }
        initial_history::check_delivery(&mut lease, policy, input.cancelled)
            .map_err(InitialEventError::Compacted)?;
        let plan = confirmed.plan();
        let base = plan.base();
        let receipt = ProducerPrefixReceipt {
            generation: base.generation,
            base_sha256: plan.reference().sha256,
            previous_retired_through: plan.old_retired(),
            retired_through: base.retired,
            source_through: base.source,
            pending_batches: base.batches,
            pending_bytes: plan.pending_bytes(),
        };
        // Reacquire current Control/dataset/Ingest/producer locks after prepare
        // COMMIT. Never carry the old transaction's authority across that gap.
        let (tx, mut lease) = self
            .begin_initial_native_stage::<AdmissionError>(
                &NativeOwner::Store(&mut *store),
                input.operation(),
                false,
                &mut validate,
            )
            .await?;
        let unlinked = store
            .retire_open_prefix(confirmed, input.cancelled, || {
                producer_progress::recovery::authority(&mut lease, policy, input.cancelled)
            })
            .map_err(InitialEventError::Native)?;
        producer_progress::prefix::finalize(tx, store, unlinked, control, input.cancelled, || {
            producer_progress::recovery::authority(&mut lease, policy, input.cancelled)
        })
        .await
        .map_err(InitialEventError::Progress)?;
        if self.client.is_closed() || store.native_unavailable() {
            return Err(InitialEventError::DeliveryUnknown);
        }
        initial_history::check_delivery(&mut lease, policy, input.cancelled)
            .map_err(InitialEventError::Compacted)?;
        self.available = true;
        Ok(Some(receipt))
    }
}
