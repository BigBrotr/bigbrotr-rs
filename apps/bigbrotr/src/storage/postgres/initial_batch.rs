//! Connected initial native batch under the dedicated Control backend.
use super::super::import_checkpoints::{self, ImportCheckpointError};
use super::super::input_bindings::{self, ImportBatch, ImportJobBinding, InputBindingError};
use super::super::producer_outcomes::{self, OutcomeError};
use super::super::producer_requests::{self, ReservationError};
use super::super::producer_sources::{self, CurrentSourceCheck, RequestPhase, SourceBindingError};
use super::*;
use crate::{
    acquisition::{
        relay_source::RelaySourceBinding,
        scope::{AcquisitionEventContext, ScopeEventError},
    },
    runtime::{
        OwnedPermit,
        policy_authority::{LeaseResponse, PolicyLeaseTracker, PolicyLeaseTtl},
    },
    storage::rocks::{
        manifest::AllocationNamespaces,
        store::{
            AdmissionError, AdmissionLimits, BootstrapError, BootstrapLimits, CertificationLimits,
            IngestStore, PreparedCertification, PreparedEvent, StoredCertification, StoredOutcome,
            StoredReceipt, StoredVisibility,
        },
    },
};
use bigbrotr_model::{
    EvidenceIntent, ObservedTime, PolicyEpoch, ProducerEpoch, ProducerRequestId, QualifiedRef,
    RequestSequence,
};
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InitialOperationError<E> {
    Control(OwnerError),
    Registry(ingest_registry::RegistryError),
    Native(E),
    Reservation(ReservationError),
    Source(SourceBindingError),
    Scope(ScopeEventError),
    InputBinding(InputBindingError),
    ImportCheckpoint(ImportCheckpointError),
    /// The native result exists; this error never authorizes another request ID.
    Outcome(OutcomeError),
    /// Native work was dispatched but later cleanup/delivery is unavailable.
    /// Mutations require original-request resolution; reads must be evaluated
    /// again under current authority.
    DeliveryUnknown,
}
impl<E: fmt::Debug> fmt::Display for InitialOperationError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "initial native batch unavailable: {self:?}")
    }
}
impl<E: fmt::Debug> std::error::Error for InitialOperationError<E> {}
impl<E> From<OwnerError> for InitialOperationError<E> {
    fn from(value: OwnerError) -> Self {
        Self::Control(value)
    }
}

pub type InitialBatchError = InitialOperationError<BootstrapError>;
pub type InitialEventError = InitialOperationError<AdmissionError>;

fn event_source<'a>(
    binding: Option<RelaySourceBinding>,
    acquisition: Option<AcquisitionEventContext<'a>>,
    visibility: QualifiedRef,
    origin: bigbrotr_model::EventOrigin,
    event: Option<&bigbrotr_protocol::VerifiedEvent>,
) -> Result<Option<CurrentSourceCheck<'a>>, InitialEventError> {
    let Some(binding) = binding else {
        if acquisition.is_some() {
            return Err(OwnerError::InvalidInput.into());
        }
        return Ok(None);
    };
    if !matches!(origin, bigbrotr_model::EventOrigin::Network) {
        return Err(OwnerError::InvalidInput.into());
    }
    if let (Some(context), Some(event)) = (acquisition, event) {
        context
            .check(binding, visibility, event.event())
            .map_err(InitialEventError::Scope)?;
    }
    // Invalid signatures still take the native rejection path. They cannot gain
    // effects; current relational source/scope checks below apply even to them.
    Ok(Some(CurrentSourceCheck {
        binding,
        visibility,
        scope: acquisition.map(|c| c.scope()),
        job: acquisition.and_then(|c| c.job()),
    }))
}

/// Static factory coordinates, not request-supplied authority.
struct InitialIngestContext<'a> {
    ingest_root: &'a std::path::Path,
    registration: &'a ingest_registry::InitialRegistration,
    policy_ttl: Duration,
    request: Option<(ProducerRequestId, RequestPhase)>,
    source: Option<CurrentSourceCheck<'a>>,
    network_event: bool,
    job: Option<ImportJobBinding>,
}

pub struct InitialEventRequest<'a> {
    pub ingest_root: &'a std::path::Path,
    pub registration: &'a ingest_registry::InitialRegistration,
    pub event: PreparedEvent,
    /// Original operational source, supplied by the trusted acquisition owner.
    /// None is reserved for imports and legacy unbound fixtures, not an escape
    /// from current source permission. This data alone is never a socket grant.
    pub relay_source: Option<RelaySourceBinding>,
    /// The actual subscription owner retains this immutable collection/request
    /// predicate through both phases. None is only for prior unbound consumers.
    pub acquisition: Option<AcquisitionEventContext<'a>>,
    pub provenance_enabled: bool,
    pub recorded_at: ObservedTime,
    pub limits: AdmissionLimits,
    pub policy_ttl: Duration,
    pub cancelled: &'a AtomicBool,
}

/// Trusted exact-source read request; no producer mutation identity or job
/// completion follows from it. Resolve previous ambiguous Admit work first.
pub struct InitialCertificationRequest<'a> {
    pub ingest_root: &'a std::path::Path,
    pub registration: &'a ingest_registry::InitialRegistration,
    pub event: PreparedCertification,
    pub relay_source: Option<RelaySourceBinding>,
    pub acquisition: Option<AcquisitionEventContext<'a>>,
    pub provenance_enabled: bool,
    pub limits: CertificationLimits,
    pub policy_ttl: Duration,
    pub cancelled: &'a AtomicBool,
}

/// Read one committed descriptor under current initial-owner authority. This
/// neither grants acquisition permission nor allocates a producer request.
pub struct InitialVisibilityRequest<'a> {
    pub ingest_root: &'a std::path::Path,
    pub registration: &'a ingest_registry::InitialRegistration,
    pub reference: QualifiedRef,
    pub limits: CertificationLimits,
    pub policy_ttl: Duration,
    pub cancelled: &'a AtomicBool,
}

/// Complete event-only JSONL batch supplied by a trusted source/policy consumer.
/// The prepared events retain their separate memory grants while being staged.
/// This API binds existing job metadata; it does not accept an operator job.
pub struct InitialImportBatchRequest<'a> {
    pub ingest_root: &'a std::path::Path,
    pub registration: &'a ingest_registry::InitialRegistration,
    pub batch: &'a ImportBatch<'a>,
    pub events: &'a [PreparedEvent],
    pub limits: AdmissionLimits,
    pub policy_ttl: Duration,
    pub cancelled: &'a AtomicBool,
}

/// Expected data, not a grant. Both original input and whole PG document buffers
/// require caller reservations, in addition to the transferred batch permit.
pub struct InitialBatchRequest<'a> {
    /// Selected by the trusted static-root validator, never a remote request.
    pub ingest_root: &'a std::path::Path,
    pub registration: &'a ingest_registry::InitialRegistration,
    pub intent: &'a EvidenceIntent,
    pub recorded_at: ObservedTime,
    pub limits: BootstrapLimits,
    pub policy_ttl: Duration,
    pub cancelled: &'a AtomicBool,
}

impl ControlSession {
    /// Run on the retained native owner worker, like certification. The result
    /// contains immutable data; source authorization remains a separate check.
    pub async fn read_initial_visibility(
        &mut self,
        store: &mut IngestStore,
        input: InitialVisibilityRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<Option<StoredVisibility>, InitialEventError> {
        if input.cancelled.load(Ordering::Relaxed) {
            return Err(InitialEventError::Native(
                AdmissionError::AuthorityUnavailable,
            ));
        }
        let identity = *store.manifest().identity();
        let (tx, mut lease) = self
            .begin_initial_native_operation(
                store,
                InitialIngestContext {
                    ingest_root: input.ingest_root,
                    registration: input.registration,
                    policy_ttl: input.policy_ttl,
                    request: None,
                    source: None,
                    network_event: false,
                    job: None,
                },
                validate,
            )
            .await?;
        let result = store.read_visibility(input.reference, input.limits, permit, || {
            if input.cancelled.load(Ordering::Relaxed) {
                return Err(AdmissionError::AuthorityUnavailable);
            }
            lease
                .check_delivery(identity.policy_epoch)
                .map_err(|_| AdmissionError::AuthorityUnavailable)
        });
        tx.rollback()
            .await
            .map_err(|_| InitialEventError::DeliveryUnknown)?;
        if input.cancelled.load(Ordering::Relaxed)
            || self.client.is_closed()
            || lease.check_delivery(identity.policy_epoch).is_err()
        {
            return Err(InitialEventError::DeliveryUnknown);
        }
        self.available = true;
        result.map_err(InitialEventError::Native)
    }

    /// Same current source/policy/owner checks as admission, with no input
    /// staging, producer reservation or outcome/progress writes. A successful
    /// rollback releases the read transaction before final delivery validation.
    pub async fn certify_initial_event(
        &mut self,
        store: &mut IngestStore,
        input: InitialCertificationRequest<'_>,
        permit: OwnedPermit,
        mut validate: impl FnMut(&str, [u8; 32], &PreparedCertification) -> Result<(), OwnerError>,
    ) -> Result<StoredCertification, InitialEventError> {
        if input.cancelled.load(Ordering::Relaxed) {
            return Err(InitialEventError::Native(
                AdmissionError::AuthorityUnavailable,
            ));
        }
        let source = event_source(
            input.relay_source,
            input.acquisition,
            input.event.visibility_scope_id(),
            input.event.origin(),
            input.event.verified_event(),
        )?;
        let identity = *store.manifest().identity();
        let (tx, mut lease) = self
            .begin_initial_native_operation(
                store,
                InitialIngestContext {
                    ingest_root: input.ingest_root,
                    registration: input.registration,
                    policy_ttl: input.policy_ttl,
                    request: None,
                    source,
                    network_event: matches!(
                        input.event.origin(),
                        bigbrotr_model::EventOrigin::Network
                    ),
                    job: None,
                },
                |document, digest| validate(document, digest, &input.event),
            )
            .await?;
        let result = store.certify_event(
            input.event,
            input.provenance_enabled,
            input.limits,
            permit,
            || {
                if input.cancelled.load(Ordering::Relaxed) {
                    return Err(AdmissionError::AuthorityUnavailable);
                }
                lease
                    .check_delivery(identity.policy_epoch)
                    .map_err(|_| AdmissionError::AuthorityUnavailable)
            },
        );
        tx.rollback()
            .await
            .map_err(|_| InitialEventError::DeliveryUnknown)?;
        if input.cancelled.load(Ordering::Relaxed)
            || self.client.is_closed()
            || lease.check_delivery(identity.policy_epoch).is_err()
        {
            return Err(InitialEventError::DeliveryUnknown);
        }
        // A completed read refusal does not make a new commit ambiguous. The
        // caller may correct its limits and re-evaluate on this current session.
        self.available = true;
        result.map_err(InitialEventError::Native)
    }

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
        let request = ProducerRequestId {
            producer_slot_id: spec.producer_slot_id,
            producer_epoch: ProducerEpoch::new(1).map_err(|_| OwnerError::InvalidInput)?,
            request_sequence: RequestSequence::new(1).map_err(|_| OwnerError::InvalidInput)?,
        };
        let (reservation_tx, mut reservation_lease) = self
            .begin_initial_native_operation(
                store,
                InitialIngestContext {
                    ingest_root: input.ingest_root,
                    registration: spec,
                    policy_ttl: input.policy_ttl,
                    request: Some((request, RequestPhase::Reservation)),
                    source: None,
                    network_event: false,
                    job: None,
                },
                &mut validate,
            )
            .await?;
        let staged = store
            .stage_initial_dictionaries(request, input.intent, input.limits, &permit)
            .map_err(InitialBatchError::Native)?;
        producer_requests::reserve(&reservation_tx, &staged, spec.control, None, None, None)
            .await
            .map_err(InitialBatchError::Reservation)?;
        if input.cancelled.load(Ordering::Relaxed) {
            return Err(InitialBatchError::Native(
                BootstrapError::AuthorityUnavailable,
            ));
        }
        if reservation_lease
            .check_delivery(identity.policy_epoch)
            .is_err()
        {
            return Err(OwnerError::Unavailable.into());
        }
        reservation_tx
            .commit()
            .await
            .map_err(|_| InitialBatchError::Reservation(ReservationError::CommitUnknown))?;
        self.available = true;
        // COMMIT precedes native dispatch. Acquire a new complete policy/registry
        // snapshot and a new local lease, accounting for both PG round trips.
        let (tx, mut lease) = self
            .begin_initial_native_operation(
                store,
                InitialIngestContext {
                    ingest_root: input.ingest_root,
                    registration: spec,
                    policy_ttl: input.policy_ttl,
                    request: Some((request, RequestPhase::Dispatch)),
                    source: None,
                    network_event: false,
                    job: None,
                },
                &mut validate,
            )
            .await?;
        let result = store.bootstrap_dictionaries(
            request,
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
        let receipt = match result {
            Ok(receipt) => receipt,
            Err(error) => {
                tx.rollback()
                    .await
                    .map_err(|_| InitialBatchError::DeliveryUnknown)?;
                return Err(InitialBatchError::Native(error));
            }
        };
        if input.cancelled.load(Ordering::Relaxed)
            || lease.check_delivery(identity.policy_epoch).is_err()
        {
            return Err(InitialBatchError::DeliveryUnknown);
        }
        producer_outcomes::record(
            &tx,
            &staged,
            receipt
                .native_outcome()
                .ok_or(InitialBatchError::Outcome(OutcomeError::InvalidMetadata))?,
            spec.control,
        )
        .await
        .map_err(InitialBatchError::Outcome)?;
        if input.cancelled.load(Ordering::Relaxed)
            || lease.check_delivery(identity.policy_epoch).is_err()
        {
            return Err(InitialBatchError::DeliveryUnknown);
        }
        tx.commit()
            .await
            .map_err(|_| InitialBatchError::Outcome(OutcomeError::CommitUnknown))?;
        if input.cancelled.load(Ordering::Relaxed)
            || self.client.is_closed()
            || lease.check_delivery(identity.policy_epoch).is_err()
        {
            return Err(InitialBatchError::DeliveryUnknown);
        }
        self.available = true;
        Ok(receipt)
    }
    /// The trusted validator must authorize this exact source, scope, origin and
    /// provenance choice against both complete current documents and static roots.
    /// This is not a remote caller callback or an installed policy implementation.
    /// The prepared verifier result alone grants no admission or visibility.
    pub async fn admit_initial_event(
        &mut self,
        store: &mut IngestStore,
        input: InitialEventRequest<'_>,
        permit: OwnedPermit,
        mut validate: impl FnMut(&str, [u8; 32], &PreparedEvent) -> Result<(), OwnerError>,
    ) -> Result<StoredOutcome, InitialEventError> {
        let spec = input.registration;
        if input.event.request_id().producer_slot_id != spec.producer_slot_id
            || input.event.request_id().producer_epoch.get() != 1
            || input.limits.max_outstanding_requests > u64::from(spec.outstanding_request_limit)
        {
            return Err(OwnerError::InvalidInput.into());
        }
        let source = event_source(
            input.relay_source,
            input.acquisition,
            input.event.visibility_scope_id(),
            input.event.origin(),
            input.event.verified_event(),
        )?;
        let identity = *store.manifest().identity();
        let (reservation_tx, mut reservation_lease) = self
            .begin_initial_native_operation(
                store,
                InitialIngestContext {
                    ingest_root: input.ingest_root,
                    registration: spec,
                    policy_ttl: input.policy_ttl,
                    request: Some((input.event.request_id(), RequestPhase::Reservation)),
                    source,
                    network_event: matches!(
                        input.event.origin(),
                        bigbrotr_model::EventOrigin::Network
                    ),
                    job: None,
                },
                |document, digest| validate(document, digest, &input.event),
            )
            .await?;
        let staged = input
            .event
            .stage(store, input.limits, &permit)
            .map_err(InitialEventError::Native)?;
        producer_requests::reserve(
            &reservation_tx,
            &staged,
            spec.control,
            input.relay_source,
            input.acquisition.map(|context| context.scope().identity()),
            input
                .acquisition
                .and_then(|context| context.job())
                .map(|binding| binding.job),
        )
        .await
        .map_err(InitialEventError::Reservation)?;
        if input.cancelled.load(Ordering::Relaxed) {
            return Err(InitialEventError::Native(
                AdmissionError::AuthorityUnavailable,
            ));
        }
        if reservation_lease
            .check_delivery(identity.policy_epoch)
            .is_err()
        {
            return Err(OwnerError::Unavailable.into());
        }
        reservation_tx
            .commit()
            .await
            .map_err(|_| InitialEventError::Reservation(ReservationError::CommitUnknown))?;
        self.available = true;
        let (tx, mut lease) = self
            .begin_initial_native_operation(
                store,
                InitialIngestContext {
                    ingest_root: input.ingest_root,
                    registration: spec,
                    policy_ttl: input.policy_ttl,
                    request: Some((input.event.request_id(), RequestPhase::Dispatch)),
                    source,
                    network_event: matches!(
                        input.event.origin(),
                        bigbrotr_model::EventOrigin::Network
                    ),
                    job: None,
                },
                |document, digest| validate(document, digest, &input.event),
            )
            .await?;
        let result = store.admit_event(
            input.event,
            input.recorded_at,
            input.provenance_enabled,
            input.limits,
            permit,
            || {
                if input.cancelled.load(Ordering::Relaxed) {
                    return Err(AdmissionError::AuthorityUnavailable);
                }
                lease
                    .check_delivery(identity.policy_epoch)
                    .map_err(|_| AdmissionError::AuthorityUnavailable)
            },
        );
        let outcome = match result {
            Ok(outcome) => outcome,
            Err(error) => {
                tx.rollback()
                    .await
                    .map_err(|_| InitialEventError::DeliveryUnknown)?;
                return Err(InitialEventError::Native(error));
            }
        };
        if input.cancelled.load(Ordering::Relaxed)
            || lease.check_delivery(identity.policy_epoch).is_err()
        {
            return Err(InitialEventError::DeliveryUnknown);
        }
        producer_outcomes::record(&tx, &staged, outcome.native_outcome(), spec.control)
            .await
            .map_err(InitialEventError::Outcome)?;
        import_checkpoints::close_record(&tx, outcome.native_outcome(), spec.control)
            .await
            .map_err(InitialEventError::ImportCheckpoint)?;
        // SQL awaits can consume the lease or observe cancellation after native
        // completion. Do not publish its copy/progress with expired authority.
        if input.cancelled.load(Ordering::Relaxed)
            || lease.check_delivery(identity.policy_epoch).is_err()
        {
            return Err(InitialEventError::DeliveryUnknown);
        }
        tx.commit()
            .await
            .map_err(|_| InitialEventError::Outcome(OutcomeError::CommitUnknown))?;
        if input.cancelled.load(Ordering::Relaxed)
            || self.client.is_closed()
            || lease.check_delivery(identity.policy_epoch).is_err()
        {
            return Err(InitialEventError::DeliveryUnknown);
        }
        self.available = true;
        Ok(outcome)
    }

    /// Synchronize all original inputs, then atomically bind their complete
    /// source-position list and PG reservations before any native event dispatch.
    /// An error can retain unreserved staging; it cannot authorize changed retry
    /// bytes. CommitUnknown requires observing/retrying this exact batch.
    pub async fn reserve_initial_import_batch(
        &mut self,
        store: &mut IngestStore,
        input: InitialImportBatchRequest<'_>,
        permit: OwnedPermit,
        mut validate: impl FnMut(&str, [u8; 32], &PreparedEvent) -> Result<(), OwnerError>,
    ) -> Result<(), InitialEventError> {
        let spec = input.registration;
        if input.events.len() != input.batch.record_count()
            || input.limits.max_outstanding_requests > u64::from(spec.outstanding_request_limit)
            || permit.request().units().data_bytes()
                < input
                    .limits
                    .required_data_bytes()
                    .map_err(InitialEventError::Native)?
                    .checked_add(64 * 1024)
                    .ok_or(InitialEventError::Native(AdmissionError::Limit))?
        {
            return Err(InitialEventError::InputBinding(
                InputBindingError::InvalidInput,
            ));
        }
        let first = input.events[0].request_id().request_sequence.get();
        for (index, event) in input.events.iter().enumerate() {
            if event.request_id().producer_slot_id != spec.producer_slot_id
                || event.request_id().producer_epoch.get() != 1
                || first.checked_add(index as u64)
                    != Some(event.request_id().request_sequence.get())
                || event.verified_event().is_none()
                || !matches!(event.origin(), bigbrotr_model::EventOrigin::Import(_))
                || event.observation().is_some()
            {
                return Err(InitialEventError::InputBinding(
                    InputBindingError::InvalidInput,
                ));
            }
        }
        let identity = *store.manifest().identity();
        let (tx, mut lease) = self
            .begin_initial_native_operation(
                store,
                InitialIngestContext {
                    ingest_root: input.ingest_root,
                    registration: spec,
                    policy_ttl: input.policy_ttl,
                    request: None,
                    source: None,
                    network_event: false,
                    job: Some(input.batch.job),
                },
                |document, digest| {
                    for event in input.events {
                        validate(document, digest, event)?;
                    }
                    Ok(())
                },
            )
            .await?;
        let mut staged = Vec::new();
        staged
            .try_reserve_exact(input.events.len())
            .map_err(|_| InitialEventError::Native(AdmissionError::Allocation))?;
        for event in input.events {
            if input.cancelled.load(Ordering::Relaxed) {
                return Err(InitialEventError::Native(
                    AdmissionError::AuthorityUnavailable,
                ));
            }
            staged.push(
                event
                    .stage(store, input.limits, &permit)
                    .map_err(InitialEventError::Native)?,
            );
        }
        input_bindings::record_batch(&tx, input.batch, &staged, spec.control)
            .await
            .map_err(InitialEventError::InputBinding)?;
        for request in &staged {
            producer_requests::reserve(&tx, request, spec.control, None, None, None)
                .await
                .map_err(InitialEventError::Reservation)?;
        }
        if input.cancelled.load(Ordering::Relaxed)
            || lease.check_delivery(identity.policy_epoch).is_err()
        {
            return Err(InitialEventError::Native(
                AdmissionError::AuthorityUnavailable,
            ));
        }
        tx.commit()
            .await
            .map_err(|_| InitialEventError::InputBinding(InputBindingError::CommitUnknown))?;
        self.available = true;
        Ok(())
    }

    async fn begin_initial_native_operation<'a, E>(
        &'a mut self,
        store: &IngestStore,
        input: InitialIngestContext<'_>,
        mut validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<(Transaction<'a>, PolicyLeaseTracker), InitialOperationError<E>> {
        let spec = input.registration;
        let identity = *store.manifest().identity();
        let domains = AllocationNamespaces::from_entries(&spec.allocation_domains)
            .map_err(|_| OwnerError::InvalidInput)?;
        if !store.matches_configured_root(input.ingest_root)
            || spec.dataset_id != self.request.dataset
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
                    .map_err(InitialOperationError::Registry)?
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
        let tx = application_transaction(&mut self.client, 14)
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
        if let Some((request, phase)) = input.request {
            producer_sources::require_binding(
                &tx,
                spec.dataset_id,
                request,
                input.source.map(|s| s.binding),
                input
                    .source
                    .and_then(|s| s.scope.map(|scope| scope.identity())),
                input.source.and_then(|s| s.job).map(|binding| binding.job),
                phase,
            )
            .await
            .map_err(InitialOperationError::Source)?;
        }
        producer_sources::require_event_source(&tx, input.network_event, input.source)
            .await
            .map_err(InitialOperationError::Source)?;
        let job_remaining = if let Some(check) = input.source {
            let remaining =
                producer_sources::require_job(&tx, spec.dataset_id, check.binding, check.job)
                    .await
                    .map_err(InitialOperationError::Source)?;
            producer_sources::require_current(&tx, spec.dataset_id, check)
                .await
                .map_err(InitialOperationError::Source)?;
            remaining
        } else {
            None
        };
        if let Some(job) = input.job {
            if job.config_revision != spec.config_revision
                || job.policy_revision != spec.policy_revision
            {
                return Err(OwnerError::InvalidPolicy.into());
            }
            input_bindings::lock_job(&tx, spec.dataset_id, spec.control, job)
                .await
                .map_err(InitialOperationError::InputBinding)?;
        }
        if let Some((request, _)) = input.request {
            input_bindings::lock_request_job(
                &tx,
                spec.dataset_id,
                spec.control,
                request,
                spec.config_revision,
                spec.policy_revision,
            )
            .await
            .map_err(InitialOperationError::InputBinding)?;
        }
        ingest_registry::require_initial_registration(&tx, spec)
            .await
            .map_err(InitialOperationError::Registry)?;
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
                    ttl: input
                        .policy_ttl
                        .min(Duration::from_micros(remaining))
                        .min(job_remaining.unwrap_or(input.policy_ttl)),
                },
            )
            .map_err(|_| OwnerError::Unavailable)?;
        Ok((tx, lease))
    }
}
