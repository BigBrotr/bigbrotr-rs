//! Pre-wire and terminal commits on the same current acquisition/native owner.
use super::producer_progress::quantum::{self, LinearQuantum, QuantumCapture, QuantumGrant};
use super::*;
use crate::acquisition::{quantum::QuantumError, scope::StoredAcquisitionScope};
use std::sync::atomic::Ordering;

/// Only the immutable prefix may drain a requested stop. The first INSERT is
/// admitted immediately after `before_write`, with no intervening await. From
/// that point every authority/delivery check sees the original stop token.
struct PlanCancellation<'a> {
    requested: &'a AtomicBool,
    reading: AtomicBool,
    writing: AtomicBool,
}
impl<'a> PlanCancellation<'a> {
    fn new(requested: &'a AtomicBool) -> Option<Self> {
        if requested.load(Ordering::Acquire) {
            return None;
        }
        Some(Self {
            requested,
            reading: AtomicBool::new(false),
            writing: AtomicBool::new(false),
        })
    }
    fn token(&self) -> &AtomicBool {
        if self.writing.load(Ordering::Acquire) {
            self.requested
        } else {
            &self.reading
        }
    }
    fn stopped_before_write(&self) -> bool {
        !self.writing.load(Ordering::Acquire) && self.requested.load(Ordering::Acquire)
    }
    fn before_write(
        &self,
        check: impl FnOnce(&AtomicBool) -> Result<(), AdmissionError>,
    ) -> Result<bool, QuantumError> {
        // Genuine authority failure precedes a simultaneous orderly stop.
        check(self.token()).map_err(QuantumError::Native)?;
        if self.stopped_before_write() {
            return Ok(false);
        }
        self.writing.store(true, Ordering::Release);
        Ok(true)
    }
}

/// A read result supplies no reusable custody until rollback and both delivery
/// checks have succeeded. The caller retains its original operation deadline.
async fn rollback_plan_read(
    rollback: impl std::future::Future<Output = Result<(), InitialEventError>>,
    mut validate: impl FnMut() -> Result<(), InitialEventError>,
) -> Result<(), InitialEventError> {
    validate()?;
    rollback.await?;
    validate()
}

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
        let Some(cancellation) = PlanCancellation::new(input.cancelled) else {
            return Ok(None);
        };
        let (mut native, authority) = source.split();
        let context = authority.progress_context(native.manifest());
        let control = self.identity;
        let policy = PolicyEpoch::new(authority.policy().policy_revision);
        let mut operation = input.operation(authority)?;
        operation.cancelled = cancellation.token();
        let (tx, mut lease) = self
            .begin_initial_native_operation::<AdmissionError>(&native, operation, &mut validate)
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
            .quantum_progress_read(&permit, cancellation.token())
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
            &mut || {
                producer_progress::recovery::authority(&mut lease, policy, cancellation.token())
            },
        )
        .await
        .map_err(InitialEventError::Quantum)?;
        while let Some(span) = traversal.geometry.exclusion(input.scope.selector()) {
            let written = quantum::exclude(
                &tx,
                context,
                &job,
                input.scope.identity(),
                input.source,
                control,
                &mut traversal,
                span,
                || {
                    cancellation.before_write(|token| {
                        producer_progress::recovery::authority(&mut lease, policy, token)
                    })
                },
            )
            .await
            .map_err(InitialEventError::Quantum)?;
            if !written {
                break;
            }
        }
        let plan = if cancellation.stopped_before_write()
            || traversal.geometry.settled
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
                || {
                    cancellation.before_write(|token| {
                        producer_progress::recovery::authority(&mut lease, policy, token)
                    })
                },
            )
            .await
            .map_err(InitialEventError::Quantum)?
        };
        producer_progress::recovery::require_read(
            &tx,
            read,
            native.manifest(),
            context.slot,
            &permit,
            None,
            || producer_progress::recovery::authority(&mut lease, policy, cancellation.token()),
        )
        .await
        .map_err(InitialEventError::Progress)?;
        if !cancellation.writing.load(Ordering::Acquire) {
            rollback_plan_read(
                async {
                    tx.rollback()
                        .await
                        .map_err(|_| InitialEventError::DeliveryUnknown)
                },
                || {
                    native.validate_delivery(cancellation.token(), &mut lease, policy)?;
                    initial_history::check_delivery(&mut lease, policy, cancellation.token())
                        .map_err(InitialEventError::Compacted)
                },
            )
            .await?;
            if self.client.is_closed() {
                return Err(InitialEventError::DeliveryUnknown);
            }
            native.delivered();
            self.available = true;
            return Ok(None);
        }
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

#[cfg(test)]
mod plan_stop_tests {
    use super::*;
    use std::{
        cell::Cell,
        future::Future,
        pin::pin,
        task::{Context, Poll, Waker},
    };

    fn authority(token: &AtomicBool) -> Result<(), AdmissionError> {
        if token.load(Ordering::Acquire) {
            Err(AdmissionError::AuthorityUnavailable)
        } else {
            Ok(())
        }
    }

    #[test]
    fn stopped_plan_never_admits_its_read_prefix() {
        assert!(PlanCancellation::new(&AtomicBool::new(true)).is_none());
    }

    #[tokio::test]
    async fn stop_during_plan_read_drains_rollback_without_admitting_write() {
        let requested = AtomicBool::new(false);
        let cancellation = PlanCancellation::new(&requested).unwrap();
        let writes = Cell::new(0);
        let validations = Cell::new(0);
        let rollbacks = Cell::new(0);
        let (read_done, read_wait) = tokio::sync::oneshot::channel();
        let (rollback_done, rollback_wait) = tokio::sync::oneshot::channel();
        let mut operation = pin!(async {
            read_wait.await.unwrap();
            authority(cancellation.token()).unwrap();
            if cancellation.before_write(authority).unwrap() {
                writes.set(writes.get() + 1);
            }
            rollback_plan_read(
                async {
                    rollbacks.set(rollbacks.get() + 1);
                    rollback_wait.await.unwrap();
                    Ok(())
                },
                || {
                    validations.set(validations.get() + 1);
                    authority(cancellation.token()).map_err(InitialEventError::Native)
                },
            )
            .await
        });
        let mut context = Context::from_waker(Waker::noop());
        assert!(operation.as_mut().poll(&mut context).is_pending());
        requested.store(true, Ordering::Release);
        read_done.send(()).unwrap();
        assert!(operation.as_mut().poll(&mut context).is_pending());
        assert_eq!(
            (writes.get(), rollbacks.get(), validations.get()),
            (0, 1, 1)
        );
        rollback_done.send(()).unwrap();
        assert_eq!(operation.as_mut().poll(&mut context), Poll::Ready(Ok(())));
        assert_eq!(
            (writes.get(), rollbacks.get(), validations.get()),
            (0, 1, 2)
        );
        assert!(cancellation.stopped_before_write());
    }

    #[tokio::test]
    async fn stopped_plan_preserves_authentication_and_rollback_failures() {
        for failure in [
            AdmissionError::StateMismatch,
            AdmissionError::CommitUnknown,
            AdmissionError::AuthorityUnavailable,
        ] {
            let requested = AtomicBool::new(false);
            let cancellation = PlanCancellation::new(&requested).unwrap();
            requested.store(true, Ordering::Release);
            assert_eq!(
                cancellation.before_write(|_| Err(failure)),
                Err(QuantumError::Native(failure))
            );
            for after_rollback in [false, true] {
                let polled = Cell::new(false);
                let validations = Cell::new(0);
                assert_eq!(
                    rollback_plan_read(
                        async {
                            polled.set(true);
                            Ok(())
                        },
                        || {
                            validations.set(validations.get() + 1);
                            if after_rollback && validations.get() == 1 {
                                Ok(())
                            } else {
                                Err(InitialEventError::Native(failure))
                            }
                        },
                    )
                    .await,
                    Err(InitialEventError::Native(failure))
                );
                assert_eq!(polled.get(), after_rollback);
            }
        }
        let validations = Cell::new(0);
        assert_eq!(
            rollback_plan_read(async { Err(InitialEventError::DeliveryUnknown) }, || {
                validations.set(validations.get() + 1);
                Ok(())
            },)
            .await,
            Err(InitialEventError::DeliveryUnknown)
        );
        assert_eq!(validations.get(), 1);
    }

    #[tokio::test]
    async fn settled_plan_stop_during_rollback_keeps_the_read_token() {
        let requested = AtomicBool::new(false);
        let cancellation = PlanCancellation::new(&requested).unwrap();
        let validations = Cell::new(0);
        let (done, wait) = tokio::sync::oneshot::channel();
        let mut rollback = pin!(rollback_plan_read(
            async {
                wait.await.unwrap();
                Ok(())
            },
            || {
                validations.set(validations.get() + 1);
                authority(cancellation.token()).map_err(InitialEventError::Native)
            },
        ));
        let mut context = Context::from_waker(Waker::noop());
        assert!(rollback.as_mut().poll(&mut context).is_pending());
        assert_eq!(validations.get(), 1);
        requested.store(true, Ordering::Release);
        done.send(()).unwrap();
        assert_eq!(rollback.as_mut().poll(&mut context), Poll::Ready(Ok(())));
        assert_eq!(validations.get(), 2);
        assert!(cancellation.stopped_before_write());
        assert!(!std::ptr::eq(cancellation.token(), &requested));
    }

    #[tokio::test]
    async fn stopped_plan_cannot_outlive_policy_during_rollback() {
        use crate::runtime::policy_authority::{LeaseError, installed_with_clock};
        for expire_before in [true, false] {
            let requested = AtomicBool::new(false);
            let cancellation = PlanCancellation::new(&requested).unwrap();
            requested.store(true, Ordering::Release);
            let (mut lease, clock) = installed_with_clock();
            let expired = clock.0.get().unwrap() + Duration::from_secs(6);
            if expire_before {
                clock.0.set(Ok(expired));
            }
            let rollbacks = Cell::new(0);
            let result = rollback_plan_read(
                async {
                    rollbacks.set(rollbacks.get() + 1);
                    clock.0.set(Ok(expired));
                    Ok(())
                },
                || {
                    authority(cancellation.token()).map_err(InitialEventError::Native)?;
                    lease.check_delivery(PolicyEpoch::new(1)).map_err(|_| {
                        InitialEventError::Native(AdmissionError::AuthorityUnavailable)
                    })
                },
            )
            .await;
            assert_eq!(
                result,
                Err(InitialEventError::Native(
                    AdmissionError::AuthorityUnavailable
                ))
            );
            assert_eq!(rollbacks.get(), usize::from(!expire_before));
            assert_eq!(
                lease.check_delivery(PolicyEpoch::new(1)),
                Err(LeaseError::Expired)
            );
        }
    }

    #[test]
    fn first_plan_effect_restores_real_stop_for_every_later_boundary() {
        let requested = AtomicBool::new(false);
        let cancellation = PlanCancellation::new(&requested).unwrap();
        assert!(!std::ptr::eq(cancellation.token(), &requested));
        assert_eq!(cancellation.before_write(authority), Ok(true));
        assert!(std::ptr::eq(cancellation.token(), &requested));
        requested.store(true, Ordering::Release);
        assert!(!cancellation.stopped_before_write());
        assert_eq!(
            cancellation.before_write(authority),
            Err(QuantumError::Native(AdmissionError::AuthorityUnavailable))
        );
        assert_eq!(
            authority(cancellation.token()),
            Err(AdmissionError::AuthorityUnavailable)
        );
    }
}
