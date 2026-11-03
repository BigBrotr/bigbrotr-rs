//! Pre-wire and terminal commits on the same current acquisition/native owner.
use super::producer_progress::quantum::{self, LinearQuantum, QuantumCapture, QuantumGrant};
use super::*;
use crate::acquisition::{quantum::QuantumError, scope::StoredAcquisitionScope};

pub enum LinearQuantumSource<'a> {
    Initial(
        &'a mut IngestStore,
        &'a ingest_registry::InitialRegistration,
    ),
    Live(&'a mut LiveInitialWriter),
    Successor(&'a mut LiveSuccessorWriter),
}
impl<'a> LinearQuantumSource<'a> {
    fn split(self) -> (NativeOwner<'a>, NativeAuthority<'a>) {
        match self {
            Self::Initial(store, registration) => (
                NativeOwner::Store(store),
                NativeAuthority::Initial(registration),
            ),
            Self::Live(writer) => (
                NativeOwner::Store(writer.native.store_mut()),
                NativeAuthority::Live(&writer.binding),
            ),
            Self::Successor(writer) => (
                NativeOwner::successor(&mut writer.native, writer.check.as_ref()),
                NativeAuthority::Successor(&writer.binding),
            ),
        }
    }
}
pub struct LinearQuantumRequest<'a> {
    pub ingest_root: &'a std::path::Path,
    pub job: bigbrotr_model::JobId,
    pub scope: &'a StoredAcquisitionScope,
    pub source: RelaySourceBinding,
    pub window_width_seconds: u64,
    pub query_budget: u16,
    pub policy_ttl: Duration,
    pub cancelled: &'a AtomicBool,
}
impl LinearQuantumRequest<'_> {
    fn operation<'a>(
        &'a self,
        authority: NativeAuthority<'a>,
    ) -> Result<InitialIngestContext<'a>, InitialEventError> {
        let acquisition =
            AcquisitionEventContext::for_job(self.job, self.scope, self.scope.selector().filters())
                .map_err(InitialEventError::Scope)?;
        Ok(InitialIngestContext {
            ingest_root: self.ingest_root,
            authority,
            cancelled: self.cancelled,
            policy_ttl: self.policy_ttl,
            request: None,
            source: event_source(
                Some(self.source),
                Some(acquisition),
                self.scope.scope().visibility(),
                bigbrotr_model::EventOrigin::Network,
                None,
            )?,
            monitor: None,
            network_event: true,
            job: None,
            progress: true,
        })
    }
}
impl ControlSession {
    pub async fn plan_linear_quantum(
        &mut self,
        source: LinearQuantumSource<'_>,
        input: LinearQuantumRequest<'_>,
        grant: QuantumGrant,
        mut validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<Option<LinearQuantum>, InitialEventError> {
        let (mut native, authority) = source.split();
        let context = authority.progress_context(native.manifest());
        let control = self.identity;
        let policy = PolicyEpoch::new(authority.policy().policy_revision);
        let (tx, mut lease) = self
            .begin_initial_native_operation::<AdmissionError>(
                &native,
                input.operation(authority)?,
                &mut validate,
            )
            .await?;
        let job = super::super::acquisition_claims::read(&tx, context.dataset, input.job)
            .await
            .map_err(|_| InitialEventError::Quantum(QuantumError::Context))?
            .ok_or(InitialEventError::Quantum(QuantumError::Context))?;
        if job.scope != input.scope.identity() {
            return Err(InitialEventError::Quantum(QuantumError::Context));
        }
        let permit = grant.permit.clone();
        let read = native
            .quantum_progress_read(&permit, input.cancelled)
            .map_err(InitialEventError::Native)?;
        let mut traversal = quantum::next_window(
            &tx,
            &read,
            context,
            &job,
            input.scope,
            input.source,
            control,
            input.window_width_seconds,
            input.query_budget,
            &mut || producer_progress::recovery::authority(&mut lease, policy, input.cancelled),
        )
        .await
        .map_err(InitialEventError::Quantum)?;
        let mut local_written = false;
        while let Some(span) = traversal.geometry.exclusion(input.scope.selector()) {
            quantum::exclude(
                &tx,
                context,
                &job,
                input.scope.identity(),
                input.source,
                control,
                &mut traversal,
                span,
            )
            .await
            .map_err(InitialEventError::Quantum)?;
            local_written = true;
        }
        let plan = if traversal.geometry.settled
            || traversal.geometry.spent == traversal.geometry.budget
        {
            None
        } else {
            let geometry = traversal.geometry;
            let window = crate::acquisition::quantum::LinearWindow::retained(
                input.scope.selector(),
                job.accepted_at_us,
                geometry.lower,
                geometry.upper,
                geometry.horizon,
            )
            .map_err(InitialEventError::Quantum)?;
            Some(
                quantum::prepare(
                    &tx,
                    context,
                    &job,
                    input.scope.identity(),
                    input.source,
                    window,
                    traversal,
                    control,
                    grant,
                )
                .await
                .map_err(InitialEventError::Quantum)?,
            )
        };
        producer_progress::recovery::require_read(
            &tx,
            read,
            native.manifest(),
            context.slot,
            &permit,
            None,
            || producer_progress::recovery::authority(&mut lease, policy, input.cancelled),
        )
        .await
        .map_err(InitialEventError::Progress)?;
        native.validate_delivery(input.cancelled, &mut lease, policy)?;
        initial_history::check_delivery(&mut lease, policy, input.cancelled)
            .map_err(InitialEventError::Compacted)?;
        if plan.is_some() || local_written {
            tx.commit()
                .await
                .map_err(|_| InitialEventError::Quantum(QuantumError::CommitUnknown))?;
        } else {
            tx.rollback()
                .await
                .map_err(|_| InitialEventError::DeliveryUnknown)?;
        }
        if self.client.is_closed() {
            return Err(InitialEventError::DeliveryUnknown);
        }
        initial_history::check_delivery(&mut lease, policy, input.cancelled)
            .map_err(InitialEventError::Compacted)?;
        native.validate_delivery(input.cancelled, &mut lease, policy)?;
        native.delivered();
        self.available = true;
        Ok(plan)
    }
    pub async fn finish_linear_quantum(
        &mut self,
        source: LinearQuantumSource<'_>,
        input: LinearQuantumRequest<'_>,
        plan: &LinearQuantum,
        capture: QuantumCapture,
        mut validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<(), InitialEventError> {
        let (mut native, authority) = source.split();
        let context = authority.progress_context(native.manifest());
        if context != plan.context
            || input.job != plan.job
            || input.scope.identity() != plan.scope
            || plan.profile().map_err(InitialEventError::Quantum)?
                != (input.window_width_seconds, input.query_budget)
        {
            return Err(InitialEventError::Quantum(QuantumError::Context));
        }
        let policy = PolicyEpoch::new(authority.policy().policy_revision);
        let (tx, mut lease) = self
            .begin_initial_native_operation::<AdmissionError>(
                &native,
                input.operation(authority)?,
                &mut validate,
            )
            .await?;
        let read = native
            .quantum_progress_read(&plan.permit, input.cancelled)
            .map_err(InitialEventError::Native)?;
        producer_progress::recovery::require_read(
            &tx,
            read,
            native.manifest(),
            context.slot,
            &plan.permit,
            None,
            || producer_progress::recovery::authority(&mut lease, policy, input.cancelled),
        )
        .await
        .map_err(InitialEventError::Progress)?;
        quantum::finish(&tx, plan, capture)
            .await
            .map_err(InitialEventError::Quantum)?;
        native.validate_delivery(input.cancelled, &mut lease, policy)?;
        initial_history::check_delivery(&mut lease, policy, input.cancelled)
            .map_err(InitialEventError::Compacted)?;
        tx.commit()
            .await
            .map_err(|_| InitialEventError::Quantum(QuantumError::CommitUnknown))?;
        if self.client.is_closed() {
            return Err(InitialEventError::DeliveryUnknown);
        }
        initial_history::check_delivery(&mut lease, policy, input.cancelled)
            .map_err(InitialEventError::Compacted)?;
        native.validate_delivery(input.cancelled, &mut lease, policy)?;
        native.delivered();
        self.available = true;
        Ok(())
    }
}

impl ControlSession {
    /// Finalize only explicit seals beneath this actual newly activated writer.
    /// A reconstructed activation row, a fresh Control epoch, and ordinary claim
    /// are deliberately insufficient to reach this operation.
    pub async fn adopt_linear_continuations(
        &mut self,
        source: LinearQuantumSource<'_>,
        input: InitialHistoryRequest<'_>,
        grant: QuantumGrant,
        mut validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<u64, InitialEventError> {
        let activation: [u8; 32] = match &source {
            LinearQuantumSource::Initial(_, _) => return Err(OwnerError::InvalidInput.into()),
            LinearQuantumSource::Live(writer) => {
                Sha256::digest(writer.handoff().encode().as_bytes()).into()
            }
            LinearQuantumSource::Successor(writer) => {
                Sha256::digest(writer.record().encode().as_bytes()).into()
            }
        };
        let (mut native, authority) = source.split();
        let context = authority.progress_context(native.manifest());
        let control = self.identity;
        let policy = PolicyEpoch::new(authority.policy().policy_revision);
        let (tx, mut lease) = self
            .begin_initial_native_operation::<AdmissionError>(
                &native,
                InitialIngestContext {
                    ingest_root: input.ingest_root,
                    authority,
                    cancelled: input.cancelled,
                    policy_ttl: input.policy_ttl,
                    request: None,
                    source: None,
                    monitor: None,
                    network_event: false,
                    job: None,
                    progress: true,
                },
                &mut validate,
            )
            .await?;
        let rows=tx.query("SELECT substring(continuation_id FROM 1 FOR 17),substring(job_id FROM 1 FOR 17),previous_job_revision::text, \
            producer_epoch::text,substring(manifest_sha256 FROM 1 FOR 33),shard_id,last_quantum_number::text, \
            enrollment_generation::text,substring(route_id FROM 1 FOR 17) \
            FROM control.acquisition_continuations WHERE producer_slot_id=$1 AND producer_epoch<$2::text::numeric \
                AND adopted_at_us IS NULL ORDER BY job_id LIMIT 65",
            &[&&context.slot.as_bytes()[..],&context.epoch.to_string()]).await.map_err(backend)?;
        if rows.len() > 64 {
            return Err(OwnerError::InvalidMetadata.into());
        }
        let read = native
            .quantum_progress_read(&grant.permit, input.cancelled)
            .map_err(InitialEventError::Native)?;
        let resource = *native.manifest().identity().owner_resource_id.as_bytes();
        let mut adopted = 0;
        for row in rows {
            let job_id: bigbrotr_model::JobId = fixed::<16>(&row, 1)?
                .try_into()
                .map_err(|_| OwnerError::InvalidMetadata)?;
            tx.query_one(
                "SELECT control.require_acquisition_continuation_authority($1,$2)",
                &[&&context.dataset.as_bytes()[..], &&job_id.as_bytes()[..]],
            )
            .await
            .map_err(backend)?;
            let job = super::super::acquisition_claims::read(&tx, context.dataset, job_id)
                .await
                .map_err(|_| OwnerError::InvalidMetadata)?
                .ok_or(OwnerError::InvalidMetadata)?;
            let previous = number(&row, 2)?;
            let epoch = number(&row, 3)?;
            let manifest = fixed::<32>(&row, 4)?;
            let shard: u32 = u32::try_from(row.try_get::<_, i64>(5).map_err(backend)?)
                .map_err(|_| OwnerError::InvalidMetadata)?;
            let historic = producer_progress::Context {
                dataset: context.dataset,
                slot: context.slot,
                epoch,
                shard,
                manifest,
            };
            // Compare the exact ancestor and target with the actual activated
            // writer before using any old page's dependency union.
            let linked: bool = tx
                .query_one(
                    "SELECT control.acquisition_continuation_covers($1,$2,$3::text::numeric) \
                AND sha256(control.ingest_lineage_manifest_at($4,$3::text::numeric))=$5 \
                AND sha256(control.ingest_lineage_manifest_at($4,$6::text::numeric))=$7 \
                AND sha256(control.ingest_lineage_handoff_at($4,$6::text::numeric))=$8",
                    &[
                        &&job_id.as_bytes()[..],
                        &&context.slot.as_bytes()[..],
                        &epoch.to_string(),
                        &&resource[..],
                        &&manifest[..],
                        &context.epoch.to_string(),
                        &&context.manifest[..],
                        &&activation[..],
                    ],
                )
                .await
                .map_err(backend)?
                .try_get(0)
                .map_err(backend)?;
            if !linked || job.revision != previous {
                return Err(OwnerError::InvalidMetadata.into());
            }
            let (binding, cut) = super::super::acquisition_continuation::authenticate(
                &tx,
                &read,
                historic,
                &job,
                &grant,
                &mut || producer_progress::recovery::authority(&mut lease, policy, input.cancelled),
            )
            .await
            .map_err(InitialEventError::Quantum)?;
            if cut != super::super::acquisition_continuation::cut_number(&row, 6)?
                || binding.enrollment_generation.get() != number(&row, 7)?
                || binding.route_id.as_bytes() != &fixed::<16>(&row, 8)?
            {
                return Err(OwnerError::InvalidMetadata.into());
            }
            let changed=tx.execute("UPDATE control.acquisition_continuations SET target_epoch=$2::text::numeric,target_manifest_sha256=$3::bytea, \
                activation_sha256=$4::bytea,adopting_resource=$5::bytea,adopting_epoch=$6::text::numeric,adopting_incarnation=$7::bytea, \
                adopted_job_revision=previous_job_revision+1,adopted_at_us=(extract(epoch FROM clock_timestamp())*1000000)::bigint, \
                adoption_transaction=pg_current_xact_id() WHERE continuation_id=$1::bytea AND adopted_at_us IS NULL",
                &[&&fixed::<16>(&row,0)?[..],&context.epoch.to_string(),&&context.manifest[..],&&activation[..],
                    &&control.resource_id.as_bytes()[..],&control.epoch.get().to_string(),&&control.process_incarnation.as_bytes()[..]])
                .await.map_err(backend)?;
            let changed_job=tx.execute("UPDATE control.jobs SET job_revision=job_revision+1,owner_resource_id=$3::bytea,owner_epoch=$4::text::numeric,owner_incarnation=$5::bytea \
                WHERE job_id=$1::bytea AND state='queued' AND job_revision=$2::text::numeric AND checkpoint_revision=0",
                &[&&job_id.as_bytes()[..],&previous.to_string(),&&control.resource_id.as_bytes()[..],&control.epoch.get().to_string(),&&control.process_incarnation.as_bytes()[..]])
                .await.map_err(backend)?;
            if (changed, changed_job) != (1, 1) {
                return Err(OwnerError::InvalidMetadata.into());
            }
            adopted += 1;
        }
        read.validate().map_err(InitialEventError::Native)?;
        native.validate_delivery(input.cancelled, &mut lease, policy)?;
        initial_history::check_delivery(&mut lease, policy, input.cancelled)
            .map_err(InitialEventError::Compacted)?;
        if adopted > 0 {
            tx.commit()
                .await
                .map_err(|_| InitialEventError::Quantum(QuantumError::CommitUnknown))?;
        } else {
            tx.rollback()
                .await
                .map_err(|_| InitialEventError::DeliveryUnknown)?;
        }
        initial_history::check_delivery(&mut lease, policy, input.cancelled)
            .map_err(InitialEventError::Compacted)?;
        native.validate_delivery(input.cancelled, &mut lease, policy)?;
        if self.client.is_closed() {
            return Err(InitialEventError::DeliveryUnknown);
        }
        native.delivered();
        self.available = true;
        Ok(adopted)
    }
}
