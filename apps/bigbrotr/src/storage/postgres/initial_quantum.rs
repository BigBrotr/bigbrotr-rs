//! Pre-wire and terminal commits on the same current acquisition/native owner.
use super::producer_progress::quantum::{self, LinearQuantum, QuantumCapture, QuantumGrant};
use super::*;
use crate::acquisition::{
    quantum::{LinearWindow, QuantumError},
    scope::StoredAcquisitionScope,
};

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
        let window = match LinearWindow::first(
            input.scope.selector(),
            job.accepted_at_us,
            input.window_width_seconds,
        ) {
            Ok(window) => window,
            Err(QuantumError::Interval) => {
                // No query or persistent plan exists for an empty intersection.
                // The job remains pending; another scope may use the next turn.
                tx.rollback()
                    .await
                    .map_err(|_| InitialEventError::DeliveryUnknown)?;
                initial_history::check_delivery(&mut lease, policy, input.cancelled)
                    .map_err(InitialEventError::Compacted)?;
                native.validate_delivery(input.cancelled, &mut lease, policy)?;
                if self.client.is_closed() {
                    return Err(InitialEventError::DeliveryUnknown);
                }
                native.delivered();
                self.available = true;
                return Ok(None);
            }
            Err(error) => return Err(InitialEventError::Quantum(error)),
        };
        let plan = quantum::prepare(
            &tx,
            context,
            &job,
            input.scope.identity(),
            input.source,
            window,
            control,
            grant,
        )
        .await
        .map_err(InitialEventError::Quantum)?;
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
        Ok(Some(plan))
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
        if context != plan.context || input.job != plan.job || input.scope.identity() != plan.scope
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
