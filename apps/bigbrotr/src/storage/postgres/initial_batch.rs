//! Connected initial native batch under the dedicated Control backend.
use super::super::import_checkpoints::{self, ImportCheckpointError};
use super::super::input_bindings::{self, ImportBatch, ImportJobBinding, InputBindingError};
use super::super::producer_outcomes::{self, OutcomeError};
use super::super::producer_requests::{self, ReservationError};
use super::super::producer_sources::{self, CurrentSourceCheck, RequestPhase, SourceBindingError};
use super::*;
use crate::{
    acquisition::{
        observation::{ObservationBindings, StoredObservation},
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
    EvidenceIntent, ObservedTime, PolicyEpoch, ProducerEpoch, ProducerRequestId, ProducerSlotId,
    QualifiedRef, RequestSequence,
};
use std::sync::atomic::{AtomicBool, Ordering};

#[path = "initial_dispatch.rs"]
mod dispatch;
use super::producer_progress::{self, ProducerProgressError, ProgressGrant, SyncProgress};
use dispatch::NativeOwner;
#[path = "initial_progress.rs"]
mod progress_operations;

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
    Progress(ProducerProgressError),
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
    authority: NativeAuthority<'a>,
    policy_ttl: Duration,
    request: Option<(ProducerRequestId, RequestPhase)>,
    source: Option<CurrentSourceCheck<'a>>,
    network_event: bool,
    job: Option<ImportJobBinding>,
    progress: bool,
}

#[derive(Clone, Copy)]
enum NativeAuthority<'a> {
    Initial(&'a ingest_registry::InitialRegistration),
    Live(&'a initial_activation::LiveInitialBinding),
    Successor(&'a successor_transition::LiveSuccessorBinding),
}
impl<'a> NativeAuthority<'a> {
    fn progress_context(
        self,
        manifest: &crate::storage::rocks::manifest::IngestManifest,
    ) -> producer_progress::Context {
        producer_progress::Context {
            dataset: self.dataset(),
            slot: self.slot(),
            epoch: self.epoch(),
            manifest: Sha256::digest(manifest.encode().as_bytes()).into(),
        }
    }
    fn dataset(self) -> DatasetId {
        match self {
            Self::Initial(s) => s.dataset_id,
            Self::Live(b) => b.previous.dataset_id,
            Self::Successor(b) => b.dataset(),
        }
    }
    fn instance(self) -> [u8; 16] {
        match self {
            Self::Initial(s) => s.instance_id,
            Self::Live(b) => b.previous.instance_id,
            Self::Successor(b) => b.instance(),
        }
    }
    fn slot(self) -> ProducerSlotId {
        match self {
            Self::Initial(s) => s.producer_slot_id,
            Self::Live(b) => b.previous.producer_slot_id,
            Self::Successor(b) => b.slot(),
        }
    }
    fn schema(self) -> u32 {
        match self {
            Self::Initial(_) => 14,
            Self::Live(_) => 32,
            Self::Successor(_) => 38,
        }
    }
    fn control(self) -> ingest_registry::ControlOwner {
        match self {
            Self::Initial(s) => s.control,
            Self::Live(b) => b.control,
            Self::Successor(b) => b.control(),
        }
    }
    fn epoch(self) -> u64 {
        match self {
            Self::Initial(_) => 1,
            Self::Live(_) => 2,
            Self::Successor(b) => b.epoch(),
        }
    }
    fn limit(self) -> u32 {
        match self {
            Self::Initial(s) => s.outstanding_request_limit,
            Self::Live(b) => b.intent.request_limit,
            Self::Successor(b) => b.limit(),
        }
    }
    fn policy(self) -> PolicySnapshot {
        match self {
            Self::Initial(s) => PolicySnapshot {
                config_revision: s.config_revision,
                policy_revision: s.policy_revision,
                config_digest: *s.config_digest.as_bytes(),
                policy_digest: *s.policy_digest.as_bytes(),
            },
            Self::Live(b) => b.intent.policy,
            Self::Successor(b) => b.policy(),
        }
    }
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

/// Live-writer payload; authority belongs exclusively to the retained writer.
pub struct LiveInitialEventRequest<'a> {
    pub ingest_root: &'a std::path::Path,
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

/// Live read payload; current authority comes from the retained writer.
pub struct LiveInitialCertificationRequest<'a> {
    pub ingest_root: &'a std::path::Path,
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

/// Committed descriptor read without allocating a producer request.
pub struct LiveInitialVisibilityRequest<'a> {
    pub ingest_root: &'a std::path::Path,
    pub reference: QualifiedRef,
    pub limits: CertificationLimits,
    pub policy_ttl: Duration,
    pub cancelled: &'a AtomicBool,
}

/// Committed local observer/clock and optional no-auth witness descriptors.
/// Missing mappings are not allocated or guessed by this read operation.
pub struct InitialObservationRequest<'a> {
    pub ingest_root: &'a std::path::Path,
    pub registration: &'a ingest_registry::InitialRegistration,
    pub bindings: ObservationBindings<'a>,
    pub limits: CertificationLimits,
    pub policy_ttl: Duration,
    pub cancelled: &'a AtomicBool,
}

/// Read existing attribution; missing mappings are never allocated here.
pub struct LiveInitialObservationRequest<'a> {
    pub ingest_root: &'a std::path::Path,
    pub bindings: ObservationBindings<'a>,
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

/// Fixed references selected by the trusted serialized owner before reservation.
/// The caller retains original intent and accepted-document allocations. A retry
/// must preserve every reference, even if current interning would choose another.
pub struct InitialDictionaryAppendRequest<'a> {
    pub ingest_root: &'a std::path::Path,
    pub registration: &'a ingest_registry::InitialRegistration,
    pub request_sequence: RequestSequence,
    pub intent: &'a EvidenceIntent,
    pub recorded_at: ObservedTime,
    pub limits: AdmissionLimits,
    pub policy_ttl: Duration,
    pub cancelled: &'a AtomicBool,
}

/// Live-writer payload; authority belongs exclusively to the retained writer.
pub struct LiveInitialDictionaryAppendRequest<'a> {
    pub ingest_root: &'a std::path::Path,
    pub request_sequence: RequestSequence,
    pub intent: &'a EvidenceIntent,
    pub recorded_at: ObservedTime,
    pub limits: AdmissionLimits,
    pub policy_ttl: Duration,
    pub cancelled: &'a AtomicBool,
}

// Payloads carry no authority or epoch1 registration. Both live families use
// the same data shape; the opaque writer selects the checked authority branch.
pub type LiveSuccessorEventRequest<'a> = LiveInitialEventRequest<'a>;
pub type LiveSuccessorDictionaryAppendRequest<'a> = LiveInitialDictionaryAppendRequest<'a>;
pub type LiveSuccessorCertificationRequest<'a> = LiveInitialCertificationRequest<'a>;
pub type LiveSuccessorVisibilityRequest<'a> = LiveInitialVisibilityRequest<'a>;
pub type LiveSuccessorObservationRequest<'a> = LiveInitialObservationRequest<'a>;

impl ControlSession {
    /// Run on the same retained native owner as admission. This read establishes
    /// immutable attribution only; job/source authority is checked separately.
    pub async fn read_initial_observation(
        &mut self,
        store: &mut IngestStore,
        input: InitialObservationRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<Option<StoredObservation>, InitialEventError> {
        self.read_native_observation(
            NativeOwner::Store(store),
            NativeAuthority::Initial(input.registration),
            LiveInitialObservationRequest {
                ingest_root: input.ingest_root,
                bindings: input.bindings,
                limits: input.limits,
                policy_ttl: input.policy_ttl,
                cancelled: input.cancelled,
            },
            permit,
            validate,
        )
        .await
    }

    pub async fn read_live_initial_observation(
        &mut self,
        writer: &mut LiveInitialWriter,
        input: LiveInitialObservationRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<Option<StoredObservation>, InitialEventError> {
        self.read_native_observation(
            NativeOwner::Store(writer.native.store_mut()),
            NativeAuthority::Live(&writer.binding),
            input,
            permit,
            validate,
        )
        .await
    }

    async fn read_native_observation(
        &mut self,
        mut store: NativeOwner<'_>,
        authority: NativeAuthority<'_>,
        input: LiveInitialObservationRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<Option<StoredObservation>, InitialEventError> {
        if input.cancelled.load(Ordering::Relaxed) {
            return Err(InitialEventError::Native(
                AdmissionError::AuthorityUnavailable,
            ));
        }
        let identity = *store.manifest().identity();
        let (tx, mut lease) = self
            .begin_initial_native_operation(
                &store,
                InitialIngestContext {
                    progress: false,
                    ingest_root: input.ingest_root,
                    authority,
                    policy_ttl: input.policy_ttl,
                    request: None,
                    source: None,
                    network_event: false,
                    job: None,
                },
                validate,
            )
            .await?;
        let cancelled = input.cancelled;
        let result = store.read_observation(input, permit, || {
            if cancelled.load(Ordering::Relaxed) {
                return Err(AdmissionError::AuthorityUnavailable);
            }
            lease
                .check_delivery(identity.policy_epoch)
                .map_err(|_| AdmissionError::AuthorityUnavailable)
        });
        tx.rollback()
            .await
            .map_err(|_| InitialEventError::DeliveryUnknown)?;
        if cancelled.load(Ordering::Relaxed)
            || self.client.is_closed()
            || lease.check_delivery(identity.policy_epoch).is_err()
        {
            return Err(InitialEventError::DeliveryUnknown);
        }
        self.available = true;
        result.map_err(InitialEventError::Native)
    }

    /// Run on the retained native owner worker, like certification. The result
    /// contains immutable data; source authorization remains a separate check.
    pub async fn read_initial_visibility(
        &mut self,
        store: &mut IngestStore,
        input: InitialVisibilityRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<Option<StoredVisibility>, InitialEventError> {
        self.read_native_visibility(
            NativeOwner::Store(store),
            NativeAuthority::Initial(input.registration),
            LiveInitialVisibilityRequest {
                ingest_root: input.ingest_root,
                reference: input.reference,
                limits: input.limits,
                policy_ttl: input.policy_ttl,
                cancelled: input.cancelled,
            },
            permit,
            validate,
        )
        .await
    }

    pub async fn read_live_initial_visibility(
        &mut self,
        writer: &mut LiveInitialWriter,
        input: LiveInitialVisibilityRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<Option<StoredVisibility>, InitialEventError> {
        self.read_native_visibility(
            NativeOwner::Store(writer.native.store_mut()),
            NativeAuthority::Live(&writer.binding),
            input,
            permit,
            validate,
        )
        .await
    }

    async fn read_native_visibility(
        &mut self,
        mut store: NativeOwner<'_>,
        authority: NativeAuthority<'_>,
        input: LiveInitialVisibilityRequest<'_>,
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
                &store,
                InitialIngestContext {
                    progress: false,
                    ingest_root: input.ingest_root,
                    authority,
                    policy_ttl: input.policy_ttl,
                    request: None,
                    source: None,
                    network_event: false,
                    job: None,
                },
                validate,
            )
            .await?;
        let result = store.read_visibility(&input, permit, || {
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
        validate: impl FnMut(&str, [u8; 32], &PreparedCertification) -> Result<(), OwnerError>,
    ) -> Result<StoredCertification, InitialEventError> {
        self.certify_native_event(
            NativeOwner::Store(store),
            NativeAuthority::Initial(input.registration),
            LiveInitialCertificationRequest {
                ingest_root: input.ingest_root,
                event: input.event,
                relay_source: input.relay_source,
                acquisition: input.acquisition,
                provenance_enabled: input.provenance_enabled,
                limits: input.limits,
                policy_ttl: input.policy_ttl,
                cancelled: input.cancelled,
            },
            permit,
            None,
            validate,
        )
        .await
    }

    pub async fn certify_live_initial_event(
        &mut self,
        writer: &mut LiveInitialWriter,
        input: LiveInitialCertificationRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32], &PreparedCertification) -> Result<(), OwnerError>,
    ) -> Result<StoredCertification, InitialEventError> {
        self.certify_native_event(
            NativeOwner::Store(writer.native.store_mut()),
            NativeAuthority::Live(&writer.binding),
            input,
            permit,
            None,
            validate,
        )
        .await
    }

    async fn certify_native_event(
        &mut self,
        mut store: NativeOwner<'_>,
        authority: NativeAuthority<'_>,
        input: LiveInitialCertificationRequest<'_>,
        permit: OwnedPermit,
        progress: Option<&SyncProgress>,
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
        let event_progress = progress
            .map(|p| p.bind(input.event.captured(), source))
            .transpose()
            .map_err(InitialEventError::Progress)?;
        let progress_context = progress.map(|_| authority.progress_context(store.manifest()));
        let identity = *store.manifest().identity();
        let (tx, mut lease) = self
            .begin_initial_native_operation(
                &store,
                InitialIngestContext {
                    progress: progress.is_some(),
                    ingest_root: input.ingest_root,
                    authority,
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
        let mut progress_state = if progress.is_some() {
            Some(
                producer_progress::prepare(
                    &tx,
                    progress_context.expect("progress context"),
                    event_progress,
                    None,
                    0,
                )
                .await
                .map_err(InitialEventError::Progress)?,
            )
        } else {
            None
        };
        let cancelled = input.cancelled;
        let result = store.certify_event(input, permit, progress.is_some(), || {
            if cancelled.load(Ordering::Relaxed) {
                return Err(AdmissionError::AuthorityUnavailable);
            }
            lease
                .check_delivery(identity.policy_epoch)
                .map_err(|_| AdmissionError::AuthorityUnavailable)
        });
        let basis = if let (Some(_), Ok(result)) = (&progress_state, &result) {
            store
                .checkpoint_certification(result)
                .map_err(InitialEventError::Native)?
        } else {
            None
        };
        if let Some(basis) = basis {
            producer_progress::record(
                &tx,
                progress_context.expect("progress context"),
                progress_state.as_mut().expect("prepared progress"),
                event_progress,
                &basis,
                authority.control(),
            )
            .await
            .map_err(InitialEventError::Progress)?;
            store
                .require_checkpoint_basis(&basis)
                .map_err(InitialEventError::Native)?;
            if cancelled.load(Ordering::Relaxed)
                || lease.check_delivery(identity.policy_epoch).is_err()
            {
                return Err(InitialEventError::DeliveryUnknown);
            }
            tx.commit()
                .await
                .map_err(|_| InitialEventError::Progress(ProducerProgressError::CommitUnknown))?;
        } else {
            tx.rollback()
                .await
                .map_err(|_| InitialEventError::DeliveryUnknown)?;
        }
        if cancelled.load(Ordering::Relaxed)
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
    async fn bootstrap_native_dictionaries(
        &mut self,
        store: &mut IngestStore,
        input: InitialBatchRequest<'_>,
        permit: OwnedPermit,
        progress: Option<&ProgressGrant>,
        mut validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<StoredReceipt, InitialBatchError> {
        let spec = input.registration;
        let identity = *store.manifest().identity();
        let progress_context =
            progress.map(|_| NativeAuthority::Initial(spec).progress_context(store.manifest()));
        let request = ProducerRequestId {
            producer_slot_id: spec.producer_slot_id,
            producer_epoch: ProducerEpoch::new(1).map_err(|_| OwnerError::InvalidInput)?,
            request_sequence: RequestSequence::new(1).map_err(|_| OwnerError::InvalidInput)?,
        };
        let (reservation_tx, mut reservation_lease) = self
            .begin_initial_native_operation(
                &NativeOwner::Store(&mut *store),
                InitialIngestContext {
                    progress: progress.is_some(),
                    ingest_root: input.ingest_root,
                    authority: NativeAuthority::Initial(spec),
                    policy_ttl: input.policy_ttl,
                    request: Some((request, RequestPhase::Reservation)),
                    source: None,
                    network_event: false,
                    job: None,
                },
                &mut validate,
            )
            .await?;
        let mut progress_state = if progress.is_some() {
            input
                .limits
                .required_data_bytes()
                .map_err(InitialBatchError::Native)?;
            let bytes = input
                .intent
                .request_preimage_len(request, input.limits.max_batch_bytes)
                .map_err(|_| InitialBatchError::Progress(ProducerProgressError::Limit))?
                as u64;
            Some(
                producer_progress::prepare(
                    &reservation_tx,
                    progress_context.expect("progress context"),
                    None,
                    Some((1, bytes)),
                    input.limits.max_mappings,
                )
                .await
                .map_err(InitialBatchError::Progress)?,
            )
        } else {
            None
        };
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
                &NativeOwner::Store(&mut *store),
                InitialIngestContext {
                    progress: progress.is_some(),
                    ingest_root: input.ingest_root,
                    authority: NativeAuthority::Initial(spec),
                    policy_ttl: input.policy_ttl,
                    request: Some((request, RequestPhase::Dispatch)),
                    source: None,
                    network_event: false,
                    job: None,
                },
                &mut validate,
            )
            .await?;
        if let Some(state) = &progress_state {
            producer_progress::recheck(&tx, progress_context.expect("progress context"), state)
                .await
                .map_err(InitialBatchError::Progress)?;
        }
        let require_authority = || {
            if input.cancelled.load(Ordering::Relaxed) {
                return Err(BootstrapError::AuthorityUnavailable);
            }
            lease
                .check_delivery(identity.policy_epoch)
                .map_err(|_| BootstrapError::AuthorityUnavailable)
        };
        let result = if progress.is_some() {
            store.bootstrap_checkpoint_dictionaries(
                request,
                input.intent,
                input.recorded_at,
                input.limits,
                permit,
                require_authority,
            )
        } else {
            store.bootstrap_dictionaries(
                request,
                input.intent,
                input.recorded_at,
                input.limits,
                permit,
                require_authority,
            )
        };
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
        if let Some(state) = &mut progress_state {
            let basis = store
                .checkpoint_evidence(&receipt)
                .map_err(|_| InitialBatchError::Progress(ProducerProgressError::InvalidContext))?;
            producer_progress::record(
                &tx,
                progress_context.expect("progress context"),
                state,
                None,
                &basis,
                spec.control,
            )
            .await
            .map_err(InitialBatchError::Progress)?;
            store
                .require_checkpoint_basis(&basis)
                .map_err(|_| InitialBatchError::Progress(ProducerProgressError::InvalidContext))?;
        }
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
    /// Append fixed dictionary references on the same retained initial owner.
    /// This does not authorize sockets, change job scopes or invent attribution.
    /// Run on a dedicated bounded owner worker: native FFI is synchronous and
    /// cannot be cancelled by dropping a future. Retain/join that worker and the
    /// PG driver before close, replacement or releasing their reservations.
    /// The validator checks both complete documents against actual static roots.
    /// No public network handler may supply an arbitrary validator or root.
    pub async fn append_initial_dictionaries(
        &mut self,
        store: &mut IngestStore,
        input: InitialDictionaryAppendRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<StoredReceipt, InitialEventError> {
        self.append_native_dictionaries(
            NativeOwner::Store(store),
            NativeAuthority::Initial(input.registration),
            LiveInitialDictionaryAppendRequest {
                ingest_root: input.ingest_root,
                request_sequence: input.request_sequence,
                intent: input.intent,
                recorded_at: input.recorded_at,
                limits: input.limits,
                policy_ttl: input.policy_ttl,
                cancelled: input.cancelled,
            },
            permit,
            None,
            validate,
        )
        .await
    }
    pub async fn append_live_initial_dictionaries(
        &mut self,
        writer: &mut LiveInitialWriter,
        input: LiveInitialDictionaryAppendRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<StoredReceipt, InitialEventError> {
        self.append_native_dictionaries(
            NativeOwner::Store(writer.native.store_mut()),
            NativeAuthority::Live(&writer.binding),
            input,
            permit,
            None,
            validate,
        )
        .await
    }

    async fn append_native_dictionaries(
        &mut self,
        mut store: NativeOwner<'_>,
        authority: NativeAuthority<'_>,
        input: LiveInitialDictionaryAppendRequest<'_>,
        permit: OwnedPermit,
        progress: Option<&ProgressGrant>,
        mut validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<StoredReceipt, InitialEventError> {
        let control = authority.control();
        let identity = *store.manifest().identity();
        let progress_context = progress.map(|_| authority.progress_context(store.manifest()));
        let request = ProducerRequestId {
            producer_slot_id: authority.slot(),
            producer_epoch: ProducerEpoch::new(authority.epoch())
                .map_err(|_| OwnerError::InvalidInput)?,
            request_sequence: input.request_sequence,
        };
        if (authority.epoch() == 1 && input.request_sequence.get() <= 1)
            || input.limits.max_outstanding_requests > u64::from(authority.limit())
        {
            return Err(OwnerError::InvalidInput.into());
        }
        let (reservation_tx, mut reservation_lease) = self
            .begin_initial_native_operation(
                &store,
                InitialIngestContext {
                    progress: progress.is_some(),
                    ingest_root: input.ingest_root,
                    authority,
                    policy_ttl: input.policy_ttl,
                    request: Some((request, RequestPhase::Reservation)),
                    source: None,
                    network_event: false,
                    job: None,
                },
                &mut validate,
            )
            .await?;
        let mut progress_state = if progress.is_some() {
            input
                .limits
                .required_dictionary_data_bytes()
                .map_err(InitialEventError::Native)?;
            let bytes = input
                .intent
                .request_preimage_len(request, input.limits.max_batch_bytes)
                .map_err(|_| InitialEventError::Progress(ProducerProgressError::Limit))?
                as u64;
            Some(
                producer_progress::prepare(
                    &reservation_tx,
                    progress_context.expect("progress context"),
                    None,
                    Some((request.request_sequence.get(), bytes)),
                    input.limits.max_dependencies,
                )
                .await
                .map_err(InitialEventError::Progress)?,
            )
        } else {
            None
        };
        let staged = store
            .stage_dictionary_append(request, &input, &permit)
            .map_err(InitialEventError::Native)?;
        producer_requests::reserve(&reservation_tx, &staged, control, None, None, None)
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
        // COMMIT precedes native dispatch. Acquire a new complete policy/registry
        // snapshot and a new local lease, accounting for both PG round trips.
        let (tx, mut lease) = self
            .begin_initial_native_operation(
                &store,
                InitialIngestContext {
                    progress: progress.is_some(),
                    ingest_root: input.ingest_root,
                    authority,
                    policy_ttl: input.policy_ttl,
                    request: Some((request, RequestPhase::Dispatch)),
                    source: None,
                    network_event: false,
                    job: None,
                },
                &mut validate,
            )
            .await?;
        if let Some(state) = &progress_state {
            producer_progress::recheck(&tx, progress_context.expect("progress context"), state)
                .await
                .map_err(InitialEventError::Progress)?;
        }
        let result = store.append_dictionaries(request, &input, permit, progress.is_some(), || {
            if input.cancelled.load(Ordering::Relaxed) {
                return Err(AdmissionError::AuthorityUnavailable);
            }
            lease
                .check_delivery(identity.policy_epoch)
                .map_err(|_| AdmissionError::AuthorityUnavailable)
        });
        let receipt = match result {
            Ok(receipt) => receipt,
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
        producer_outcomes::record(
            &tx,
            &staged,
            receipt
                .native_outcome()
                .ok_or(InitialEventError::Outcome(OutcomeError::InvalidMetadata))?,
            control,
        )
        .await
        .map_err(InitialEventError::Outcome)?;
        if let Some(state) = &mut progress_state {
            let basis = store
                .checkpoint_evidence(&receipt)
                .map_err(InitialEventError::Native)?;
            producer_progress::record(
                &tx,
                progress_context.expect("progress context"),
                state,
                None,
                &basis,
                control,
            )
            .await
            .map_err(InitialEventError::Progress)?;
            store
                .require_checkpoint_basis(&basis)
                .map_err(InitialEventError::Native)?;
        }
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
        validate: impl FnMut(&str, [u8; 32], &PreparedEvent) -> Result<(), OwnerError>,
    ) -> Result<StoredOutcome, InitialEventError> {
        self.admit_native_event(
            NativeOwner::Store(store),
            NativeAuthority::Initial(input.registration),
            LiveInitialEventRequest {
                ingest_root: input.ingest_root,
                event: input.event,
                relay_source: input.relay_source,
                acquisition: input.acquisition,
                provenance_enabled: input.provenance_enabled,
                recorded_at: input.recorded_at,
                limits: input.limits,
                policy_ttl: input.policy_ttl,
                cancelled: input.cancelled,
            },
            permit,
            None,
            validate,
        )
        .await
    }
    pub async fn admit_live_initial_event(
        &mut self,
        writer: &mut LiveInitialWriter,
        input: LiveInitialEventRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32], &PreparedEvent) -> Result<(), OwnerError>,
    ) -> Result<StoredOutcome, InitialEventError> {
        self.admit_native_event(
            NativeOwner::Store(writer.native.store_mut()),
            NativeAuthority::Live(&writer.binding),
            input,
            permit,
            None,
            validate,
        )
        .await
    }

    async fn admit_native_event(
        &mut self,
        mut store: NativeOwner<'_>,
        authority: NativeAuthority<'_>,
        input: LiveInitialEventRequest<'_>,
        permit: OwnedPermit,
        progress: Option<&SyncProgress>,
        mut validate: impl FnMut(&str, [u8; 32], &PreparedEvent) -> Result<(), OwnerError>,
    ) -> Result<StoredOutcome, InitialEventError> {
        let control = authority.control();
        if input.event.request_id().producer_slot_id != authority.slot()
            || input.event.request_id().producer_epoch.get() != authority.epoch()
            || input.limits.max_outstanding_requests > u64::from(authority.limit())
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
        let event_progress = progress
            .map(|p| p.bind(input.event.captured(), source))
            .transpose()
            .map_err(InitialEventError::Progress)?;
        let progress_context = progress.map(|_| authority.progress_context(store.manifest()));
        let identity = *store.manifest().identity();
        let (reservation_tx, mut reservation_lease) = self
            .begin_initial_native_operation(
                &store,
                InitialIngestContext {
                    progress: progress.is_some(),
                    ingest_root: input.ingest_root,
                    authority,
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
        let mut progress_state = if progress.is_some() {
            Some(
                producer_progress::prepare(
                    &reservation_tx,
                    progress_context.expect("progress context"),
                    event_progress,
                    Some((
                        input.event.request_id().request_sequence.get(),
                        input.event.retained_input_bytes() as u64,
                    )),
                    input.limits.max_dependencies,
                )
                .await
                .map_err(InitialEventError::Progress)?,
            )
        } else {
            None
        };
        let staged = store
            .stage_event(&input.event, input.limits, &permit, input.cancelled)
            .map_err(InitialEventError::Native)?;
        let scope = input.acquisition.map(|context| context.scope().identity());
        let job = input
            .acquisition
            .and_then(|context| context.job())
            .map(|binding| binding.job);
        if let Some(event) = event_progress {
            producer_requests::reserve_progress(
                &reservation_tx,
                &staged,
                control,
                input.relay_source,
                scope,
                job,
                event.position(),
            )
            .await
            .map_err(InitialEventError::Reservation)?;
        } else {
            producer_requests::reserve(
                &reservation_tx,
                &staged,
                control,
                input.relay_source,
                scope,
                job,
            )
            .await
            .map_err(InitialEventError::Reservation)?;
        }
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
                &store,
                InitialIngestContext {
                    progress: progress.is_some(),
                    ingest_root: input.ingest_root,
                    authority,
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
        if let Some(state) = &progress_state {
            producer_progress::recheck(&tx, progress_context.expect("progress context"), state)
                .await
                .map_err(InitialEventError::Progress)?;
        }
        let cancelled = input.cancelled;
        let result = store.admit_event(input, permit, progress.is_some(), || {
            if cancelled.load(Ordering::Relaxed) {
                return Err(AdmissionError::AuthorityUnavailable);
            }
            lease
                .check_delivery(identity.policy_epoch)
                .map_err(|_| AdmissionError::AuthorityUnavailable)
        });
        let outcome = match result {
            Ok(outcome) => outcome,
            Err(error) => {
                tx.rollback()
                    .await
                    .map_err(|_| InitialEventError::DeliveryUnknown)?;
                return Err(InitialEventError::Native(error));
            }
        };
        if cancelled.load(Ordering::Relaxed) || lease.check_delivery(identity.policy_epoch).is_err()
        {
            return Err(InitialEventError::DeliveryUnknown);
        }
        producer_outcomes::record(&tx, &staged, outcome.native_outcome(), control)
            .await
            .map_err(InitialEventError::Outcome)?;
        import_checkpoints::close_record(&tx, outcome.native_outcome(), control)
            .await
            .map_err(InitialEventError::ImportCheckpoint)?;
        if let Some(state) = &mut progress_state {
            let basis = store
                .checkpoint_event(&outcome)
                .map_err(InitialEventError::Native)?;
            producer_progress::record(
                &tx,
                progress_context.expect("progress context"),
                state,
                event_progress,
                &basis,
                control,
            )
            .await
            .map_err(InitialEventError::Progress)?;
            store
                .require_checkpoint_basis(&basis)
                .map_err(InitialEventError::Native)?;
        }
        // SQL awaits can consume the lease or observe cancellation after native
        // completion. Do not publish its copy/progress with expired authority.
        if cancelled.load(Ordering::Relaxed) || lease.check_delivery(identity.policy_epoch).is_err()
        {
            return Err(InitialEventError::DeliveryUnknown);
        }
        tx.commit()
            .await
            .map_err(|_| InitialEventError::Outcome(OutcomeError::CommitUnknown))?;
        if cancelled.load(Ordering::Relaxed)
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
                &NativeOwner::Store(&mut *store),
                InitialIngestContext {
                    progress: false,
                    ingest_root: input.ingest_root,
                    authority: NativeAuthority::Initial(spec),
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
        store: &NativeOwner<'_>,
        input: InitialIngestContext<'_>,
        mut validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<(Transaction<'a>, PolicyLeaseTracker), InitialOperationError<E>> {
        let authority = input.authority;
        let control = authority.control();
        let expected_policy = authority.policy();
        let identity = *store.manifest().identity();
        if !store.matches_configured_root(input.ingest_root)
            || authority.dataset() != self.request.dataset
            || authority.instance() != *self.request.instance.as_bytes()
            || control != self.identity
        {
            return Err(OwnerError::InvalidInput.into());
        }
        match authority {
            NativeAuthority::Initial(spec) => {
                let domains = AllocationNamespaces::from_entries(&spec.allocation_domains)
                    .map_err(|_| OwnerError::InvalidInput)?;
                if !matches!(store, NativeOwner::Store(_))
                    || store.manifest().key_schema() != 1
                    || identity.dataset_id != authority.dataset()
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
            }
            NativeAuthority::Live(binding) => match store {
                NativeOwner::Store(store) => binding.require_store(store, self.identity)?,
                NativeOwner::Successor(_) => return Err(OwnerError::InvalidInput.into()),
            },
            NativeAuthority::Successor(binding) => match store {
                NativeOwner::Successor(_) => {
                    binding.require_manifest(store.manifest(), self.identity)?
                }
                NativeOwner::Store(_) => return Err(OwnerError::InvalidInput.into()),
            },
        }
        let mut lease = PolicyLeaseTracker::new(
            authority.dataset(),
            identity.process_incarnation,
            identity.policy_epoch,
            PolicyLeaseTtl::new(input.policy_ttl).map_err(|_| OwnerError::InvalidInput)?,
        )
        .map_err(|_| OwnerError::Unavailable)?;
        let ticket = lease.begin_refresh().map_err(|_| OwnerError::Unavailable)?;
        self.begin_operation()?;
        let tx = application_transaction(
            &mut self.client,
            if input.progress {
                39
            } else {
                authority.schema()
            },
        )
        .await
        .map_err(transaction_error)?;
        if let NativeAuthority::Successor(binding) = authority {
            binding.lock_control(&tx).await?;
        }
        require(&tx, self.request.dataset, self.identity).await?;
        let policy = snapshot(&tx, &self.request, &mut validate).await?;
        if policy != expected_policy {
            return Err(OwnerError::InvalidPolicy.into());
        }
        if let NativeAuthority::Successor(binding) = authority {
            // Acquire ordered lineage parents before any job lock. The complete
            // check below samples authority again after every intervening wait.
            binding.require(&tx).await?;
        }
        if let Some((request, phase)) = input.request {
            producer_sources::require_binding(
                &tx,
                authority.dataset(),
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
                producer_sources::require_job(&tx, authority.dataset(), check.binding, check.job)
                    .await
                    .map_err(InitialOperationError::Source)?;
            producer_sources::require_current(&tx, authority.dataset(), check)
                .await
                .map_err(InitialOperationError::Source)?;
            remaining
        } else {
            None
        };
        if let Some(job) = input.job {
            if job.config_revision != expected_policy.config_revision
                || job.policy_revision != expected_policy.policy_revision
            {
                return Err(OwnerError::InvalidPolicy.into());
            }
            input_bindings::lock_job(&tx, authority.dataset(), control, job)
                .await
                .map_err(InitialOperationError::InputBinding)?;
        }
        if let Some((request, _)) = input.request {
            input_bindings::lock_request_job(
                &tx,
                authority.dataset(),
                control,
                request,
                expected_policy.config_revision,
                expected_policy.policy_revision,
            )
            .await
            .map_err(InitialOperationError::InputBinding)?;
        }
        let remaining = match authority {
            NativeAuthority::Initial(spec) => {
                ingest_registry::require_initial_registration(&tx, spec)
                    .await
                    .map_err(InitialOperationError::Registry)?;
                let remaining:i64 = tx.query_one("SELECT LEAST(c.lease_expires_at_us, i.lease_expires_at_us) - (EXTRACT(EPOCH FROM pg_catalog.clock_timestamp()) * 1000000)::bigint FROM control.owner_epochs c CROSS JOIN control.owner_epochs i WHERE c.resource_id = $1::bytea AND c.epoch = $2::text::numeric AND i.resource_id = $3::bytea AND i.epoch = 1", &[&&control.resource_id.as_bytes()[..],&control.epoch.get().to_string(),&&spec.ingest_resource_id.as_bytes()[..]])
                    .await.map_err(backend)?.try_get(0).map_err(backend)?;
                u64::try_from(remaining)
                    .ok()
                    .filter(|v| *v > 0)
                    .ok_or(OwnerError::Unavailable)?
            }
            NativeAuthority::Live(binding) => binding.require(&tx).await?,
            NativeAuthority::Successor(binding) => binding.require(&tx).await?,
        };
        lease
            .complete_refresh(
                &ticket,
                LeaseResponse {
                    dataset: authority.dataset(),
                    process_incarnation: identity.process_incarnation,
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

impl ControlSession {
    pub async fn read_live_successor_observation(
        &mut self,
        writer: &mut LiveSuccessorWriter,
        input: LiveSuccessorObservationRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<Option<StoredObservation>, InitialEventError> {
        self.read_native_observation(
            NativeOwner::Successor(&mut writer.native),
            NativeAuthority::Successor(&writer.binding),
            input,
            permit,
            validate,
        )
        .await
    }
    pub async fn read_live_successor_visibility(
        &mut self,
        writer: &mut LiveSuccessorWriter,
        input: LiveSuccessorVisibilityRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<Option<StoredVisibility>, InitialEventError> {
        self.read_native_visibility(
            NativeOwner::Successor(&mut writer.native),
            NativeAuthority::Successor(&writer.binding),
            input,
            permit,
            validate,
        )
        .await
    }
    pub async fn certify_live_successor_event(
        &mut self,
        writer: &mut LiveSuccessorWriter,
        input: LiveSuccessorCertificationRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32], &PreparedCertification) -> Result<(), OwnerError>,
    ) -> Result<StoredCertification, InitialEventError> {
        self.certify_native_event(
            NativeOwner::Successor(&mut writer.native),
            NativeAuthority::Successor(&writer.binding),
            input,
            permit,
            None,
            validate,
        )
        .await
    }
    pub async fn append_live_successor_dictionaries(
        &mut self,
        writer: &mut LiveSuccessorWriter,
        input: LiveSuccessorDictionaryAppendRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<StoredReceipt, InitialEventError> {
        self.append_native_dictionaries(
            NativeOwner::Successor(&mut writer.native),
            NativeAuthority::Successor(&writer.binding),
            input,
            permit,
            None,
            validate,
        )
        .await
    }
    pub async fn admit_live_successor_event(
        &mut self,
        writer: &mut LiveSuccessorWriter,
        input: LiveSuccessorEventRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32], &PreparedEvent) -> Result<(), OwnerError>,
    ) -> Result<StoredOutcome, InitialEventError> {
        self.admit_native_event(
            NativeOwner::Successor(&mut writer.native),
            NativeAuthority::Successor(&writer.binding),
            input,
            permit,
            None,
            validate,
        )
        .await
    }
}
