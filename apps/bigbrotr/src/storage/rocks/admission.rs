//! Serialized event admission. Only a connected storage authority may dispatch it.
use super::bootstrap::{Prepared, frame, read_options};
use super::producer_recovery::ReplayContext;
use super::{BootstrapError, BootstrapLimits, IngestStore, PendingBatchHeader};
use crate::{
    runtime::OwnedPermit,
    storage::rocks::keys::{ColumnFamily, IngestKey, decode_key},
};
use bigbrotr_model::*;
use bigbrotr_model::{codec::FactDecodeLimits, receipt_codec::ReceiptDecodeLimits};
use bigbrotr_protocol::{EventError, VerifiedEvent, verify_owned};
use sha2::{Digest, Sha256};
use std::{fmt, mem::size_of, sync::atomic::AtomicBool};

const MAX_INPUT: usize = 16 * 1024 * 1024;
const REJECTION_PREFIX: &[u8] = b"bigbrotr/rejection/1\0";
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionError {
    InvalidLimits,
    InsufficientReservation,
    InvalidInput,
    IncompatibleSchema,
    Limit,
    Allocation,
    EngineRead,
    StateMismatch,
    RequestConflict,
    RequestGap,
    WindowFull,
    RetiredRequest,
    ProducerClosing,
    AuthorityUnavailable,
    RequestInput(super::RequestInputError),
    CommitUnknown,
}
impl fmt::Display for AdmissionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "event admission unavailable: {self:?}")
    }
}
impl std::error::Error for AdmissionError {}
impl From<BootstrapError> for AdmissionError {
    fn from(e: BootstrapError) -> Self {
        match e {
            BootstrapError::InvalidLimits => Self::InvalidLimits,
            BootstrapError::InsufficientReservation => Self::InsufficientReservation,
            BootstrapError::InvalidIntent => Self::InvalidInput,
            BootstrapError::Allocation => Self::Allocation,
            BootstrapError::Limit => Self::Limit,
            BootstrapError::EngineRead => Self::EngineRead,
            BootstrapError::StateMismatch => Self::StateMismatch,
            BootstrapError::RequestConflict => Self::RequestConflict,
            BootstrapError::ProducerClosing => Self::ProducerClosing,
            BootstrapError::AuthorityUnavailable => Self::AuthorityUnavailable,
            BootstrapError::CommitUnknown => Self::CommitUnknown,
            BootstrapError::RequestInput(error) => Self::RequestInput(error),
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RejectionReason {
    EmptyTag = 1,
    EventId = 2,
    PublicKey = 3,
    Signature = 4,
    EventSize = 5,
}
#[derive(Debug, PartialEq, Eq)]
pub enum AdmissionOutcome {
    Durable(Receipt),
    Rejected(RejectionReason),
}
pub struct StoredOutcome {
    outcome: AdmissionOutcome,
    native_outcome: super::NativeOutcomeCopy,
    _permit: OwnedPermit,
}
impl StoredOutcome {
    pub(super) fn new(
        outcome: AdmissionOutcome,
        native_outcome: super::NativeOutcomeCopy,
        permit: OwnedPermit,
    ) -> Self {
        Self {
            outcome,
            native_outcome,
            _permit: permit,
        }
    }
    pub fn outcome(&self) -> &AdmissionOutcome {
        &self.outcome
    }
    pub(in crate::storage) fn native_outcome(&self) -> &super::NativeOutcomeCopy {
        &self.native_outcome
    }
}

/// Verification owns the actual buffers and its grant. No unchecked constructor
/// or Deserialize exists; expensive crypto can run before the serialized owner.
pub struct PreparedEvent {
    request: ProducerRequestId,
    digest: Digest256,
    scope: QualifiedRef,
    origin: EventOrigin,
    observation: Option<LocalObservation>,
    event: Option<Result<VerifiedEvent, RejectionReason>>,
    request_preimage: Vec<u8>,
    allocation_bytes: usize,
    verification_limit_bytes: usize,
    _permit: OwnedPermit,
}
impl PreparedEvent {
    pub(in crate::storage) fn stage(
        &self,
        store: &mut IngestStore,
        limits: AdmissionLimits,
        permit: &OwnedPermit,
    ) -> Result<super::StagedProducerRequest, AdmissionError> {
        self.stage_in(store, limits, permit, None)
    }
    pub(super) fn stage_successor(
        &self,
        store: &mut IngestStore,
        limits: AdmissionLimits,
        permit: &OwnedPermit,
        cancelled: &AtomicBool,
    ) -> Result<super::StagedProducerRequest, AdmissionError> {
        self.stage_in(store, limits, permit, Some(cancelled))
    }
    fn stage_in(
        &self,
        store: &mut IngestStore,
        limits: AdmissionLimits,
        permit: &OwnedPermit,
        cancelled: Option<&AtomicBool>,
    ) -> Result<super::StagedProducerRequest, AdmissionError> {
        store.require_successor_operation(cancelled)?;
        if permit.request().units().data_bytes() < limits.required_data_bytes()?
            || permit.request().units().native_bytes() < limits.required_native_bytes()?
            || permit.request().units().items() == 0
        {
            return Err(AdmissionError::InsufficientReservation);
        }
        if store.write_uncertain {
            return Err(AdmissionError::CommitUnknown);
        }
        if self.scope.dataset_id != store.manifest.identity().dataset_id {
            return Err(AdmissionError::InvalidInput);
        }
        store.require_admission_owner(Some(self.request))?;
        store.require_open_producer(self.request)?;
        store.stage_admission_input(self.request, self.digest, &self.request_preimage, cancelled)
    }

    pub fn new(
        request: ProducerRequestId,
        intent: EventIntent,
        max_event_bytes: usize,
        permit: OwnedPermit,
    ) -> Result<Self, AdmissionError> {
        let mut left = input_allowance(&intent, max_event_bytes, &permit, size_of::<Self>())?;
        let allocation_limit = input_capacity(&permit);
        let request_preimage = intent
            .encode_request_preimage(request, left)
            .map_err(BootstrapError::from)?;
        left = left
            .checked_sub(request_preimage.capacity())
            .ok_or(AdmissionError::InsufficientReservation)?;
        let allocation_bytes = allocation_limit - left;
        let digest = intent.request_digest(request);
        let scope = intent.visibility_scope_id();
        let origin = intent.origin();
        let observation = intent.observation().copied();
        let event = verify_event(intent.into_event(), max_event_bytes)?;
        Ok(Self {
            request,
            digest,
            scope,
            origin,
            observation,
            event: Some(event),
            request_preimage,
            allocation_bytes,
            verification_limit_bytes: max_event_bytes,
            _permit: permit,
        })
    }
    /// The connected policy owner must match the verification limit to the
    /// current configuration. A caller cannot choose a larger validation policy.
    pub fn verification_limit_bytes(&self) -> usize {
        self.verification_limit_bytes
    }
    pub fn verified_event(&self) -> Option<&VerifiedEvent> {
        self.event.as_ref()?.as_ref().ok()
    }
    pub fn request_id(&self) -> ProducerRequestId {
        self.request
    }
    pub fn visibility_scope_id(&self) -> QualifiedRef {
        self.scope
    }
    pub fn observation(&self) -> Option<&LocalObservation> {
        self.observation.as_ref()
    }
    pub fn origin(&self) -> EventOrigin {
        self.origin
    }
}
/// Input accounting shared by mutation and read-only certification. The latter
/// never allocates a mutation preimage or constructs a ProducerRequestId.
pub(super) fn input_capacity(permit: &OwnedPermit) -> usize {
    usize::try_from(permit.request().units().data_bytes())
        .unwrap_or(usize::MAX)
        .min(MAX_INPUT)
}
pub(super) fn input_allowance(
    intent: &EventIntent,
    max_event_bytes: usize,
    permit: &OwnedPermit,
    value_bytes: usize,
) -> Result<usize, AdmissionError> {
    if max_event_bytes == 0 || max_event_bytes > 4 * 1024 * 1024 {
        return Err(AdmissionError::InvalidLimits);
    }
    if intent.admission_policy_version() != 1 {
        return Err(AdmissionError::IncompatibleSchema);
    }
    // Spare capacities count, including empty arrays. Check each aggregate
    // before descending so hostile typed input cannot cause unbounded work.
    let event = intent.event();
    let mut left = usize::try_from(permit.request().units().data_bytes())
        .unwrap_or(usize::MAX)
        .min(MAX_INPUT);
    let mut charge = |bytes: usize| {
        left = left
            .checked_sub(bytes)
            .ok_or(AdmissionError::InsufficientReservation)?;
        Ok::<_, AdmissionError>(())
    };
    if permit.request().units().items() == 0 {
        return Err(AdmissionError::InsufficientReservation);
    }
    charge(value_bytes)?;
    charge(event.content.capacity())?;
    charge(
        event
            .tags
            .capacity()
            .checked_mul(size_of::<Vec<String>>())
            .ok_or(AdmissionError::Limit)?,
    )?;
    for tag in &event.tags {
        charge(
            tag.capacity()
                .checked_mul(size_of::<String>())
                .ok_or(AdmissionError::Limit)?,
        )?;
        for value in tag {
            charge(value.capacity())?;
        }
    }
    Ok(left)
}
pub(super) fn verify_event(
    event: SignedEvent,
    max_event_bytes: usize,
) -> Result<Result<VerifiedEvent, RejectionReason>, AdmissionError> {
    Ok(match verify_owned(event, max_event_bytes) {
        Ok(event) => Ok(event),
        Err(error) => Err(match error {
            EventError::EmptyTag => RejectionReason::EmptyTag,
            EventError::EventIdMismatch => RejectionReason::EventId,
            EventError::InvalidPublicKey => RejectionReason::PublicKey,
            EventError::InvalidSignature => RejectionReason::Signature,
            EventError::EventSizeLimit => RejectionReason::EventSize,
            _ => return Err(AdmissionError::InvalidInput),
        }),
    })
}

#[derive(Debug, Clone, Copy)]
pub struct AdmissionLimits {
    pub max_batch_bytes: usize,
    pub max_dependencies: usize,
    pub max_outstanding_requests: u64,
}
impl Default for AdmissionLimits {
    fn default() -> Self {
        Self {
            max_batch_bytes: 4 * 1024 * 1024,
            max_dependencies: 64,
            max_outstanding_requests: 64,
        }
    }
}
impl AdmissionLimits {
    pub fn required_data_bytes(self) -> Result<u64, AdmissionError> {
        if self.max_batch_bytes == 0
            || self.max_batch_bytes > 4 * 1024 * 1024
            || self.max_dependencies < 8
            || self.max_dependencies > 256
            || self.max_outstanding_requests == 0
            || self.max_outstanding_requests > 64
        {
            return Err(AdmissionError::InvalidLimits);
        }
        Ok((8 * self.max_batch_bytes
            + super::outcome_copy::HANDOFF_DATA_BYTES
            + super::request_inputs::STAGING_OVERHEAD_BYTES
            + self.max_dependencies * (size_of::<FactRef>() + 2 * size_of::<QualifiedRef>())
            + Prepared::entry_allocation_bytes(16)
            + 4 * size_of::<Fact>()
            + size_of::<Receipt>()) as u64)
    }
    pub fn required_native_bytes(self) -> Result<u64, AdmissionError> {
        self.required_data_bytes()?;
        Ok((2 * self.max_batch_bytes + 1024) as u64)
    }
    pub(super) fn facts(self) -> FactDecodeLimits {
        FactDecodeLimits {
            max_encoded_bytes: self.max_batch_bytes.min(2 * 1024 * 1024),
            max_allocation_bytes: 4 * self.max_batch_bytes,
            max_tag_nodes: self.max_batch_bytes,
            max_string_bytes: self.max_batch_bytes,
        }
    }
    fn receipts(self) -> ReceiptDecodeLimits {
        ReceiptDecodeLimits {
            max_encoded_bytes: self.max_batch_bytes,
            max_allocation_bytes: 2 * self.max_dependencies * size_of::<FactRef>(),
            max_batch_records: 5,
            max_dependencies: self.max_dependencies,
        }
    }
}
pub(super) struct ReadBudget {
    bytes: usize,
    reads: usize,
}
impl ReadBudget {
    pub(super) fn dictionaries(limits: AdmissionLimits) -> Self {
        Self {
            bytes: 16 * limits.max_batch_bytes,
            reads: 16 * limits.max_dependencies + 32,
        }
    }
    pub(super) fn new(limits: AdmissionLimits) -> Self {
        Self {
            bytes: 8 * limits.max_batch_bytes,
            reads: 4 * limits.max_dependencies + 32,
        }
    }
    pub(super) fn charge(&mut self, bytes: usize) -> Result<(), AdmissionError> {
        self.bytes = self.bytes.checked_sub(bytes).ok_or(AdmissionError::Limit)?;
        self.reads = self.reads.checked_sub(1).ok_or(AdmissionError::Limit)?;
        Ok(())
    }
}
impl IngestStore {
    /// Current source/producer/policy authority must be retained by the connected
    /// caller through this native call. A plain manifest is never such a grant.
    #[allow(
        clippy::too_many_arguments,
        reason = "internal serialized transaction with distinct owned grants and authority boundary"
    )]
    pub(in crate::storage) fn admit_event(
        &mut self,
        input: PreparedEvent,
        recorded_at: ObservedTime,
        provenance: bool,
        limits: AdmissionLimits,
        permit: OwnedPermit,
        authority: impl FnMut() -> Result<(), AdmissionError>,
    ) -> Result<StoredOutcome, AdmissionError> {
        self.admit_event_in(
            input,
            recorded_at,
            provenance,
            limits,
            permit,
            None,
            authority,
        )
    }
    #[allow(
        clippy::too_many_arguments,
        reason = "same serialized kernel with explicit successor cancellation"
    )]
    pub(super) fn admit_successor_event(
        &mut self,
        input: PreparedEvent,
        recorded_at: ObservedTime,
        provenance: bool,
        limits: AdmissionLimits,
        permit: OwnedPermit,
        cancelled: &AtomicBool,
        authority: impl FnMut() -> Result<(), AdmissionError>,
    ) -> Result<StoredOutcome, AdmissionError> {
        self.admit_event_in(
            input,
            recorded_at,
            provenance,
            limits,
            permit,
            Some(cancelled),
            authority,
        )
    }
    #[allow(
        clippy::too_many_arguments,
        reason = "shared legacy/successor operation with distinct owned grants"
    )]
    fn admit_event_in(
        &mut self,
        mut input: PreparedEvent,
        recorded_at: ObservedTime,
        provenance: bool,
        limits: AdmissionLimits,
        permit: OwnedPermit,
        cancelled: Option<&AtomicBool>,
        mut authority: impl FnMut() -> Result<(), AdmissionError>,
    ) -> Result<StoredOutcome, AdmissionError> {
        self.require_successor_operation(cancelled)?;
        if permit.request().units().data_bytes() < limits.required_data_bytes()?
            || permit.request().units().native_bytes() < limits.required_native_bytes()?
            || permit.request().units().items() == 0
        {
            return Err(AdmissionError::InsufficientReservation);
        }
        if input.allocation_bytes > limits.facts().max_allocation_bytes {
            return Err(AdmissionError::Limit);
        }
        authority()?;
        if self.write_uncertain {
            return Err(AdmissionError::CommitUnknown);
        }
        let identity = *self.manifest.identity();
        self.require_admission_owner(Some(input.request))?;
        if input.scope.dataset_id != identity.dataset_id
            || input
                .observation
                .is_some_and(|o| o.witness.is_some() != provenance)
        {
            return Err(AdmissionError::InvalidInput);
        }
        let mut reads = ReadBudget::new(limits);
        let producer_key = IngestKey::ProducerState {
            producer_slot_id: input.request.producer_slot_id,
            producer_epoch: input.request.producer_epoch,
        };
        let producer = self.admission_value(producer_key, 27, &mut reads)?;
        let producer = producer
            .as_deref()
            .map(super::producer_closing::ProducerState::decode)
            .transpose()?;
        if (identity.owner_epoch.get() == 2 || self.live_successor.is_some()) && producer.is_none()
        {
            return Err(AdmissionError::StateMismatch);
        }
        let producer = producer.unwrap_or_default();
        if producer.closing {
            return Err(AdmissionError::ProducerClosing);
        }
        let (issued, ack, retired) = (producer.issued, producer.acknowledged, producer.retired);
        let sequence = input.request.request_sequence.get();
        let request_key = IngestKey::Request {
            request_id: input.request,
        };
        if sequence <= retired {
            return Err(AdmissionError::RetiredRequest);
        }
        if let Some((outcome, native_outcome)) = self.replay_event(
            input.request,
            input.digest,
            input
                .verified_event()
                .map(|event| super::receipt_effects::ReplayEventIntent {
                    event: event.event(),
                    scope: input.scope,
                    origin: input.origin,
                    observation: input.observation,
                }),
            matches!(input.event, Some(Err(RejectionReason::EventSize))).then(|| {
                // Verification discarded the original owned body. Reuse that
                // still-retained input grant; do not charge its reconstruction
                // to the concurrent fact/receipt operation grant.
                (
                    input.request_preimage.as_slice(),
                    input_capacity(&input._permit).saturating_sub(
                        input.request_preimage.capacity() + size_of::<PreparedEvent>(),
                    ),
                )
            }),
            limits,
            &mut reads,
        )? {
            if sequence > issued {
                return Err(AdmissionError::StateMismatch);
            }
            authority()?;
            return Ok(StoredOutcome {
                outcome,
                native_outcome,
                _permit: permit,
            });
        }
        if sequence <= issued {
            return Err(AdmissionError::StateMismatch);
        }
        if issued.checked_add(1) != Some(sequence) {
            return Err(AdmissionError::RequestGap);
        }
        if issued - ack >= limits.max_outstanding_requests {
            return Err(AdmissionError::WindowFull);
        }
        let mut writes = Prepared::new(BootstrapLimits {
            max_mappings: 16,
            max_batch_bytes: limits.max_batch_bytes,
        })?;
        let outcome = match input.event.take().ok_or(AdmissionError::InvalidInput)? {
            Err(reason) => {
                if identity.owner_epoch.get() == 2 || self.live_successor.is_some() {
                    self.admission_source_boundary(limits, &mut reads)?;
                }
                AdmissionOutcome::Rejected(reason)
            }
            Ok(event) => self.prepare_event_effects(
                &input,
                event,
                recorded_at,
                limits,
                &mut reads,
                &mut writes,
            )?,
        };
        let detail = match &outcome {
            AdmissionOutcome::Durable(receipt) => receipt
                .encode_persistent(limits.max_batch_bytes)
                .map_err(|_| AdmissionError::Limit)?,
            AdmissionOutcome::Rejected(reason) => {
                encode_rejection(&input, identity.dataset_id, identity.policy_epoch, *reason)?
            }
        };
        let native_outcome = match &outcome {
            AdmissionOutcome::Durable(receipt) => {
                super::NativeOutcomeCopy::receipt(receipt, &self.manifest)?
            }
            AdmissionOutcome::Rejected(_) => super::NativeOutcomeCopy::rejection(
                input.request,
                input.digest,
                identity.policy_epoch,
                &self.manifest,
                frame(&[], &[&detail])?,
            ),
        };
        writes.put(request_key, detail)?;
        writes.put(
            producer_key,
            frame(
                &[1, 0x31],
                &[
                    &sequence.to_be_bytes(),
                    &ack.to_be_bytes(),
                    &retired.to_be_bytes(),
                ],
            )?,
        )?;
        // The shared native commit marks the handle uncertain before FFI, uses
        // sync+WAL, and retains all buffers/permits until native completion.
        authority()?;
        self.retain_admission_input(input.request, &input.request_preimage, cancelled)?;
        let source = match &outcome {
            AdmissionOutcome::Durable(receipt) => {
                receipt.batch().map(|batch| batch.last().sequence.get())
            }
            AdmissionOutcome::Rejected(_) => None,
        };
        self.commit_admission(writes, input.request, source, cancelled, || {
            authority().map_err(|_| BootstrapError::AuthorityUnavailable)
        })?;
        Ok(StoredOutcome {
            outcome,
            native_outcome,
            _permit: permit,
        })
    }

    /// Reads the original committed result; current minima never replace it.
    pub(super) fn replay_event(
        &self,
        request: ProducerRequestId,
        digest: Digest256,
        input: Option<super::receipt_effects::ReplayEventIntent<'_>>,
        size_limited_input: Option<(&[u8], usize)>,
        limits: AdmissionLimits,
        reads: &mut ReadBudget,
    ) -> Result<Option<(AdmissionOutcome, super::NativeOutcomeCopy)>, AdmissionError> {
        self.replay_event_in(
            request,
            digest,
            input,
            size_limited_input,
            limits,
            reads,
            ReplayContext {
                manifest: &self.manifest,
                current_manifest: &self.manifest,
            },
        )
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "explicit read interpretation and independently bounded inputs"
    )]
    pub(super) fn replay_event_in(
        &self,
        request: ProducerRequestId,
        digest: Digest256,
        input: Option<super::receipt_effects::ReplayEventIntent<'_>>,
        size_limited_input: Option<(&[u8], usize)>,
        limits: AdmissionLimits,
        reads: &mut ReadBudget,
        context: ReplayContext<'_>,
    ) -> Result<Option<(AdmissionOutcome, super::NativeOutcomeCopy)>, AdmissionError> {
        if &self.manifest != context.current_manifest {
            return Err(AdmissionError::StateMismatch);
        }
        let identity = *context.manifest.identity();
        let Some(value) = self.admission_value(
            IngestKey::Request {
                request_id: request,
            },
            limits.max_batch_bytes,
            reads,
        )?
        else {
            return Ok(None);
        };
        let (outcome, native_outcome) = if value.starts_with(REJECTION_PREFIX) {
            let (outcome, policy) = read_rejection(&value, request, digest, identity.dataset_id)?;
            (
                outcome,
                super::NativeOutcomeCopy::rejection(
                    request,
                    digest,
                    policy,
                    context.manifest,
                    value,
                ),
            )
        } else {
            let receipt = Receipt::decode_persistent(&value, limits.receipts())
                .map_err(BootstrapError::from)?;
            if receipt.request_id() != request
                || receipt.protection_context().dataset_id != identity.dataset_id
            {
                return Err(AdmissionError::StateMismatch);
            }
            if receipt.request_digest() != digest {
                return Err(AdmissionError::RequestConflict);
            }
            if let Some(input) = input {
                self.require_receipt_effects(&receipt, input, limits, reads)?;
            } else {
                use bigbrotr_model::codec::request::{
                    MutationIntent, RequestDecodeLimits, decode_request,
                };
                let (bytes, allocation) =
                    size_limited_input.ok_or(AdmissionError::StateMismatch)?;
                let mut values = limits.facts();
                // The retained request includes signature/origin fields absent
                // from the payload fact. Its independently bounded preimage can
                // exceed the fact decoder's 2 MiB maximum by those envelope bytes.
                values.max_encoded_bytes = bytes.len().min(MAX_INPUT);
                values.max_allocation_bytes = allocation;
                let (found, intent) = decode_request(
                    bytes,
                    digest,
                    RequestDecodeLimits {
                        values,
                        max_dictionaries: 1,
                    },
                )
                .map_err(BootstrapError::from)?;
                let MutationIntent::Event(intent) = intent else {
                    return Err(AdmissionError::StateMismatch);
                };
                if found != request {
                    return Err(AdmissionError::StateMismatch);
                }
                // A smaller current verification policy cannot reinterpret an
                // already committed result. Its full original input is already
                // owned; no sidecar creation or new native dispatch is involved.
                let scope = intent.visibility_scope_id();
                let origin = intent.origin();
                let observation = intent.observation().copied();
                let verified =
                    verify_event(intent.into_event(), 4 * 1024 * 1024)?.map_err(|reason| {
                        if reason == RejectionReason::EventSize {
                            AdmissionError::Limit
                        } else {
                            AdmissionError::StateMismatch
                        }
                    })?;
                self.require_receipt_effects(
                    &receipt,
                    super::receipt_effects::ReplayEventIntent {
                        event: verified.event(),
                        scope,
                        origin,
                        observation,
                    },
                    limits,
                    reads,
                )?;
            }
            if let Some(batch) = receipt.batch() {
                let value = self
                    .admission_value(
                        IngestKey::PendingBatch {
                            first: batch.first(),
                        },
                        128,
                        reads,
                    )?
                    .ok_or(AdmissionError::StateMismatch)?;
                if value != PendingBatchHeader::from_batch(batch).encode() {
                    return Err(AdmissionError::StateMismatch);
                }
            }
            let copy = super::NativeOutcomeCopy::receipt(&receipt, context.manifest)?;
            (AdmissionOutcome::Durable(receipt), copy)
        };
        Ok(Some((outcome, native_outcome)))
    }

    pub(super) fn admission_value(
        &self,
        key: IngestKey,
        max: usize,
        reads: &mut ReadBudget,
    ) -> Result<Option<Vec<u8>>, AdmissionError> {
        let encoded = key.encode().map_err(|_| AdmissionError::InvalidInput)?;
        let cf = self
            .db
            .cf_handle(encoded.column_family().name())
            .ok_or(AdmissionError::EngineRead)?;
        let value = self
            .db
            .get_pinned_cf_opt(cf, encoded.as_bytes(), &read_options())
            .map_err(|_| AdmissionError::EngineRead)?;
        reads.charge(value.as_ref().map_or(0, |v| v.len()))?;
        let Some(value) = value else { return Ok(None) };
        if value.len() > max {
            return Err(AdmissionError::Limit);
        }
        let mut copy = Vec::new();
        copy.try_reserve_exact(value.len())
            .map_err(|_| AdmissionError::Allocation)?;
        copy.extend_from_slice(&value);
        Ok(Some(copy))
    }
    pub(super) fn admission_fact(
        &self,
        r: FactRef,
        limits: AdmissionLimits,
        reads: &mut ReadBudget,
    ) -> Result<Fact, AdmissionError> {
        if let Some(frontier) = &self.live_successor {
            frontier.require_fact(self, r.source_record_id)?;
        }
        let value = self
            .admission_value(
                IngestKey::Fact {
                    source_record_id: r.source_record_id,
                },
                limits.facts().max_encoded_bytes + 34,
                reads,
            )?
            .ok_or(AdmissionError::StateMismatch)?;
        if value.len() < 34
            || value[..2] != [1, 0x20]
            || value[2..34] != r.payload_digest.as_bytes()[..]
        {
            return Err(AdmissionError::StateMismatch);
        }
        let fact = Fact::decode_preimage(&value[34..], r.payload_digest, limits.facts())
            .map_err(BootstrapError::from)?;
        if fact.source_record_id() != r.source_record_id {
            return Err(AdmissionError::StateMismatch);
        }
        Ok(fact)
    }
    pub(super) fn indexed_fact(
        &self,
        key: IngestKey,
        limits: AdmissionLimits,
        reads: &mut ReadBudget,
    ) -> Result<Option<Fact>, AdmissionError> {
        let Some(value) = self.admission_value(key, 70, reads)? else {
            return Ok(None);
        };
        if value.len() != 70 || value[..2] != key.domain().prefix() {
            return Err(AdmissionError::StateMismatch);
        }
        let mut source = [0u8; 38];
        source[..2].copy_from_slice(&[1, 0x20]);
        source[2..].copy_from_slice(&value[2..38]);
        let IngestKey::Fact { source_record_id } =
            decode_key(ColumnFamily::Facts, &source).map_err(|_| AdmissionError::StateMismatch)?
        else {
            return Err(AdmissionError::StateMismatch);
        };
        let digest = Digest256::from_bytes(
            value[38..]
                .try_into()
                .map_err(|_| AdmissionError::StateMismatch)?,
        );
        let fact = self.admission_fact(
            FactRef {
                source_record_id,
                payload_digest: digest,
            },
            limits,
            reads,
        )?;
        if source_record_id.dataset_id != self.manifest.identity().dataset_id {
            return Err(AdmissionError::StateMismatch);
        }
        Ok(Some(fact))
    }
    pub(super) fn dictionary_dependency(
        &self,
        reference: QualifiedRef,
        limits: AdmissionLimits,
        reads: &mut ReadBudget,
        dependencies: &mut Vec<FactRef>,
    ) -> Result<DictionaryEntry, AdmissionError> {
        self.lookup_dictionary_dependency(reference, limits, reads, dependencies)?
            .ok_or(AdmissionError::StateMismatch)
    }
    pub(super) fn lookup_dictionary_dependency(
        &self,
        reference: QualifiedRef,
        limits: AdmissionLimits,
        reads: &mut ReadBudget,
        dependencies: &mut Vec<FactRef>,
    ) -> Result<Option<DictionaryEntry>, AdmissionError> {
        let Some(fact) = self.indexed_fact(IngestKey::Dictionary { reference }, limits, reads)?
        else {
            return Ok(None);
        };
        let FactPayload::Dictionary(entry) = fact.payload() else {
            return Err(AdmissionError::StateMismatch);
        };
        if entry.reference() != reference {
            return Err(AdmissionError::StateMismatch);
        }
        let prior = dependencies.contains(&fact.fact_ref());
        add_dependency(dependencies, fact.fact_ref(), limits)?;
        if !prior {
            for dependency in fact.dictionary_refs() {
                if dependency != reference {
                    self.dictionary_dependency(dependency, limits, reads, dependencies)?;
                }
            }
        }
        Ok(Some(entry.clone()))
    }
    pub(super) fn retain_admission_fact(
        &self,
        fact: &Fact,
        limits: AdmissionLimits,
        reads: &mut ReadBudget,
        dependencies: &mut Vec<FactRef>,
    ) -> Result<(), AdmissionError> {
        add_dependency(dependencies, fact.fact_ref(), limits)?;
        for reference in fact.dictionary_refs() {
            self.dictionary_dependency(reference, limits, reads, dependencies)?;
        }
        Ok(())
    }
    fn prepare_event_effects(
        &self,
        input: &PreparedEvent,
        event: VerifiedEvent,
        recorded_at: ObservedTime,
        limits: AdmissionLimits,
        reads: &mut ReadBudget,
        writes: &mut Prepared,
    ) -> Result<AdmissionOutcome, AdmissionError> {
        let identity = *self.manifest.identity();
        let dataset_id = identity.dataset_id;
        let event_id = event.event().id;
        let sig = event.event().sig;
        let body_digest = event.body_digest();
        let mut dependencies = Vec::new();
        dependencies
            .try_reserve_exact(limits.max_dependencies)
            .map_err(|_| AdmissionError::Allocation)?;
        self.dictionary_dependency(input.scope, limits, reads, &mut dependencies)?;
        if let EventOrigin::Import(lineage) = input.origin {
            self.dictionary_dependency(lineage, limits, reads, &mut dependencies)?;
        }
        if let Some(observation) = input.observation {
            self.dictionary_dependency(observation.observer_id, limits, reads, &mut dependencies)?;
            self.dictionary_dependency(
                observation.clock_quality.clock_id(),
                limits,
                reads,
                &mut dependencies,
            )?;
            if let Some(witness) = observation.witness {
                self.dictionary_dependency(witness.endpoint_id, limits, reads, &mut dependencies)?;
                let access = self.dictionary_dependency(
                    witness.access_scope_id,
                    limits,
                    reads,
                    &mut dependencies,
                )?;
                if !matches!(access.descriptor(),DictionaryDescriptor::AccessScope(v) if v.visibility_scope_id==input.scope)
                {
                    return Err(AdmissionError::InvalidInput);
                }
                let method = self.dictionary_dependency(
                    witness.verification_method_id,
                    limits,
                    reads,
                    &mut dependencies,
                )?;
                if method.descriptor() != &DictionaryDescriptor::EventVerification {
                    return Err(AdmissionError::InvalidInput);
                }
            }
        }
        // Initial bootstrap or an explicitly activated empty successor supplies
        // the source boundary. Missing state never invents a new stream.
        let source_key = IngestKey::SourceStream {
            dataset_id,
            shard_id: identity.shard_id,
            owner_epoch: identity.owner_epoch,
        };
        let last = self.admission_source_boundary(limits, reads)?;
        let mut writer = EventWrites {
            store: self,
            writes,
            limits,
            reads,
            dependencies,
            records: Vec::new(),
            next: last,
            scope: input.scope,
            recorded_at,
        };
        writer
            .records
            .try_reserve_exact(5)
            .map_err(|_| AdmissionError::Allocation)?;
        let payload_key = IngestKey::EventPayload {
            dataset_id,
            event_id,
        };
        let payload = self.indexed_fact(payload_key, limits, writer.reads)?;
        let has_payload = payload.is_some();
        if let Some(fact) = payload {
            let event_ordinal = matching_payload(&fact, &event)?;
            writer.retain(&fact)?;
            let mapping = self.dictionary_dependency(
                event_ordinal,
                limits,
                writer.reads,
                &mut writer.dependencies,
            )?;
            if mapping.descriptor() != &DictionaryDescriptor::Event(event_id) {
                return Err(AdmissionError::StateMismatch);
            }
        } else {
            let domain = self
                .manifest
                .allocation_namespaces()
                .domain(DictionaryKind::Event)
                .ok_or(AdmissionError::InvalidInput)?;
            let allocation = IngestKey::AllocationStream {
                dataset_id,
                dictionary_kind: DictionaryKind::Event,
                allocation_domain: domain,
            };
            let high = self
                .dictionary_counter(allocation, writer.reads)?
                .unwrap_or(0);
            let reference = QualifiedRef {
                dataset_id,
                dictionary_kind: DictionaryKind::Event,
                allocation_domain: domain,
                ordinal: Ordinal::new(high.checked_add(1).ok_or(AdmissionError::Limit)?)
                    .map_err(|_| AdmissionError::Limit)?,
            };
            let mapping = DictionaryEntry::new(reference, DictionaryDescriptor::Event(event_id))
                .map_err(|_| AdmissionError::InvalidInput)?;
            let descriptor = mapping
                .encode_descriptor(1024)
                .map_err(|_| AdmissionError::Limit)?;
            let descriptor_digest = Digest256::from_bytes(Sha256::digest(&descriptor).into());
            let reverse = IngestKey::DictionaryReverseCandidate {
                dataset_id,
                dictionary_kind: DictionaryKind::Event,
                descriptor_digest,
                reference,
            };
            // Hash lookup is only a candidate. A matching complete descriptor
            // with no payload index is inconsistent, never permission to admit
            // a second payload under another ordinal.
            let encoded = reverse.encode().map_err(|_| AdmissionError::InvalidInput)?;
            let prefix = &encoded.as_bytes()[..52];
            let cf = self
                .db
                .cf_handle("default")
                .ok_or(AdmissionError::EngineRead)?;
            let mut iterator = self.db.raw_iterator_cf_opt(cf, read_options());
            iterator.seek(prefix);
            while let Some(key) = iterator.key().filter(|k| k.starts_with(prefix)) {
                let value = iterator.value().ok_or(AdmissionError::EngineRead)?;
                writer.reads.charge(key.len() + value.len())?;
                decode_key(ColumnFamily::Default, key)
                    .map_err(|_| AdmissionError::StateMismatch)?;
                if value.len() < 2 || value[..2] != [1, 5] {
                    return Err(AdmissionError::StateMismatch);
                }
                if value[2..] == descriptor {
                    return Err(AdmissionError::StateMismatch);
                }
                iterator.next();
            }
            iterator.status().map_err(|_| AdmissionError::EngineRead)?;
            drop(iterator);
            if self
                .admission_value(IngestKey::Dictionary { reference }, 70, writer.reads)?
                .is_some()
            {
                return Err(AdmissionError::StateMismatch);
            }
            let fact = writer.emit(FactPayload::Dictionary(mapping))?;
            writer.index(IngestKey::Dictionary { reference }, fact)?;
            writer
                .writes
                .put(reverse, frame(&[1, 5], &[&descriptor])?)?;
            writer.writes.put(
                allocation,
                frame(&[1, 3], &[&reference.ordinal.get().to_be_bytes()])?,
            )?;
            let fact = writer.emit(FactPayload::EventPayload(EventPayload {
                body: event.into_event().into(),
                event_ordinal: reference,
            }))?;
            writer.index(payload_key, fact)?;
        }
        let eligibility_key = IngestKey::EventEligibility {
            dataset_id,
            event_id,
            visibility_scope: input.scope,
        };
        let eligibility = self.indexed_fact(eligibility_key, limits, writer.reads)?;
        let has_eligibility = eligibility.is_some();
        match eligibility {
            Some(fact) => {
                if !has_payload
                    || fact.visibility_scope_id() != input.scope
                    || !matches!(fact.payload(),FactPayload::EventEligibility(v) if v.event_id==event_id && v.body_digest==body_digest)
                {
                    return Err(AdmissionError::StateMismatch);
                }
                writer.retain(&fact)?;
            }
            None => {
                let fact = writer.emit(FactPayload::EventEligibility(EventEligibility {
                    event_id,
                    body_digest,
                    sig,
                    first_eligible_at: recorded_at,
                    origin: input.origin,
                }))?;
                writer.index(eligibility_key, fact)?;
            }
        }
        if let Some(observation) = input.observation {
            let receive_key = IngestKey::EventReceive {
                dataset_id,
                event_id,
                visibility_scope: input.scope,
            };
            let receive = self.indexed_fact(receive_key, limits, writer.reads)?;
            let mut corrects = None;
            let mut emit = true;
            if let Some(fact) = receive {
                let FactPayload::EventReceive(value) = fact.payload() else {
                    return Err(AdmissionError::StateMismatch);
                };
                if !has_payload
                    || !has_eligibility
                    || value.event_id != event_id
                    || fact.visibility_scope_id() != input.scope
                {
                    return Err(AdmissionError::StateMismatch);
                }
                emit = observation.received_at < value.received_at;
                corrects = Some(fact.source_record_id());
                writer.retain(&fact)?;
            }
            if emit {
                let fact = writer.emit(FactPayload::EventReceive(EventReceive {
                    event_id,
                    received_at: observation.received_at,
                    observer_id: observation.observer_id,
                    clock_quality: observation.clock_quality,
                    corrects,
                }))?;
                writer.index(receive_key, fact)?;
            }
            if let Some(witness) = observation.witness {
                let key = IngestKey::Provenance {
                    dataset_id,
                    event_id,
                    endpoint_id: witness.endpoint_id,
                    observer_id: observation.observer_id,
                    access_scope_id: witness.access_scope_id,
                    visibility_scope_id: input.scope,
                };
                let prior = self.indexed_fact(key, limits, writer.reads)?;
                let mut corrects = None;
                let mut emit = true;
                if let Some(fact) = prior {
                    let FactPayload::Provenance(value) = fact.payload() else {
                        return Err(AdmissionError::StateMismatch);
                    };
                    if !has_payload
                        || !has_eligibility
                        || value.event_id != event_id
                        || fact.visibility_scope_id() != input.scope
                        || value.endpoint_id != witness.endpoint_id
                        || value.observer_id != observation.observer_id
                        || value.access_scope_id != witness.access_scope_id
                    {
                        return Err(AdmissionError::StateMismatch);
                    }
                    emit = observation.received_at < value.received_at;
                    corrects = Some(fact.source_record_id());
                    writer.retain(&fact)?;
                }
                if emit {
                    let fact = writer.emit(FactPayload::Provenance(Provenance {
                        event_id,
                        endpoint_id: witness.endpoint_id,
                        observer_id: observation.observer_id,
                        access_scope_id: witness.access_scope_id,
                        received_at: observation.received_at,
                        clock_quality: observation.clock_quality,
                        verification_method_id: witness.verification_method_id,
                        corrects,
                    }))?;
                    writer.index(key, fact)?;
                }
            }
        }
        writer
            .dependencies
            .sort_unstable_by_key(|r| r.source_record_id);
        let dependencies = SourceDependencies::new(dataset_id, writer.dependencies)
            .map_err(|_| AdmissionError::StateMismatch)?;
        let context = ProtectionContext {
            dataset_id,
            topology_generation: identity.topology_generation,
            protection_class: ProtectionClass::LocalSynced,
            recovery_generation: identity.recovery_generation,
            policy_epoch: identity.policy_epoch,
        };
        let receipt = if writer.records.is_empty() {
            Receipt::already_durable(input.request, input.digest, context, dependencies)
        } else {
            let batch =
                AdmissionBatch::new(writer.records).map_err(|_| AdmissionError::StateMismatch)?;
            writer.writes.put(
                source_key,
                frame(&[1, 2], &[&batch.last().sequence.get().to_be_bytes()])?,
            )?;
            writer.writes.put(
                IngestKey::PendingBatch {
                    first: batch.first(),
                },
                PendingBatchHeader::from_batch(&batch).encode().to_vec(),
            )?;
            Receipt::new_facts(input.request, input.digest, context, batch, dependencies)
        }
        .map_err(|_| AdmissionError::StateMismatch)?;
        Ok(AdmissionOutcome::Durable(receipt))
    }

    /// Source counters have a different zero rule from allocation counters.
    /// Only this actual live successor can retain an explicit empty source, even
    /// after no-fact requests advanced its producer. Facts/pending must be absent.
    pub(super) fn admission_source_boundary(
        &self,
        limits: AdmissionLimits,
        reads: &mut ReadBudget,
    ) -> Result<u64, AdmissionError> {
        let identity = self.manifest.identity();
        let value = self
            .admission_value(
                IngestKey::SourceStream {
                    dataset_id: identity.dataset_id,
                    shard_id: identity.shard_id,
                    owner_epoch: identity.owner_epoch,
                },
                10,
                reads,
            )?
            .ok_or(AdmissionError::StateMismatch)?;
        if value.len() != 10 || value[..2] != [1, 2] {
            return Err(AdmissionError::StateMismatch);
        }
        let last = number(&value[2..])?;
        let source_record_id = SourceRecordId {
            dataset_id: identity.dataset_id,
            shard_id: identity.shard_id,
            owner_epoch: identity.owner_epoch,
            sequence: Sequence::new(last.max(1)).map_err(|_| AdmissionError::StateMismatch)?,
        };
        if last == 0 {
            if identity.owner_epoch.get() != 2 && self.live_successor.is_none() {
                return Err(AdmissionError::StateMismatch);
            }
            self.require_admission_owner(None)?;
            for key in [
                IngestKey::Fact { source_record_id },
                IngestKey::PendingBatch {
                    first: source_record_id,
                },
            ] {
                reads.charge(0)?;
                self.handoff_empty_prefix(key, 8)?;
            }
            return Ok(0);
        }
        let value = self
            .admission_value(
                IngestKey::Fact { source_record_id },
                limits.facts().max_encoded_bytes + 34,
                reads,
            )?
            .ok_or(AdmissionError::StateMismatch)?;
        if value.len() < 34 || value[..2] != [1, 0x20] {
            return Err(AdmissionError::StateMismatch);
        }
        let digest = Digest256::from_bytes(value[2..34].try_into().expect("checked length"));
        let fact = Fact::decode_preimage(&value[34..], digest, limits.facts())
            .map_err(BootstrapError::from)?;
        if fact.source_record_id() != source_record_id {
            return Err(AdmissionError::StateMismatch);
        }
        Ok(last)
    }
}
struct EventWrites<'a> {
    store: &'a IngestStore,
    writes: &'a mut Prepared,
    limits: AdmissionLimits,
    reads: &'a mut ReadBudget,
    dependencies: Vec<FactRef>,
    records: Vec<FactRef>,
    next: u64,
    scope: QualifiedRef,
    recorded_at: ObservedTime,
}
impl EventWrites<'_> {
    fn retain(&mut self, fact: &Fact) -> Result<(), AdmissionError> {
        self.store
            .retain_admission_fact(fact, self.limits, self.reads, &mut self.dependencies)
    }
    fn emit(&mut self, payload: FactPayload) -> Result<FactRef, AdmissionError> {
        let identity = self.store.manifest.identity();
        self.next = self.next.checked_add(1).ok_or(AdmissionError::Limit)?;
        let id = SourceRecordId {
            dataset_id: identity.dataset_id,
            shard_id: identity.shard_id,
            owner_epoch: identity.owner_epoch,
            sequence: Sequence::new(self.next).map_err(|_| AdmissionError::Limit)?,
        };
        if self
            .store
            .admission_value(
                IngestKey::Fact {
                    source_record_id: id,
                },
                self.limits.max_batch_bytes,
                self.reads,
            )?
            .is_some()
        {
            return Err(AdmissionError::StateMismatch);
        }
        let fact = Fact::new(id, self.scope, self.recorded_at, payload)
            .map_err(|_| AdmissionError::InvalidInput)?;
        let reference = fact.fact_ref();
        let bytes = fact
            .encode_preimage(self.limits.facts().max_encoded_bytes)
            .map_err(|_| AdmissionError::Limit)?;
        self.writes.put(
            IngestKey::Fact {
                source_record_id: id,
            },
            frame(&[1, 0x20], &[reference.payload_digest.as_bytes(), &bytes])?,
        )?;
        if self.records.len() == 5 {
            return Err(AdmissionError::Limit);
        }
        self.records.push(reference);
        add_dependency(&mut self.dependencies, reference, self.limits)?;
        Ok(reference)
    }
    fn index(&mut self, key: IngestKey, r: FactRef) -> Result<(), AdmissionError> {
        let source = IngestKey::Fact {
            source_record_id: r.source_record_id,
        }
        .encode()
        .map_err(|_| AdmissionError::InvalidInput)?;
        self.writes.put(
            key,
            frame(
                &key.domain().prefix(),
                &[&source.as_bytes()[2..], r.payload_digest.as_bytes()],
            )?,
        )?;
        Ok(())
    }
}
pub(super) fn matching_payload(
    fact: &Fact,
    event: &VerifiedEvent,
) -> Result<QualifiedRef, AdmissionError> {
    matching_signed_payload(fact, event.event())
}
pub(super) fn matching_signed_payload(
    fact: &Fact,
    supplied: &SignedEvent,
) -> Result<QualifiedRef, AdmissionError> {
    let FactPayload::EventPayload(payload) = fact.payload() else {
        return Err(AdmissionError::StateMismatch);
    };
    let body = &payload.body;
    if body.event_id != supplied.id
        || body.pubkey != supplied.pubkey
        || body.created_at != supplied.created_at
        || body.kind != supplied.kind
        || body.tags != supplied.tags
        || body.content != supplied.content
    {
        return Err(AdmissionError::StateMismatch);
    }
    Ok(payload.event_ordinal)
}
pub(super) fn add_dependency(
    values: &mut Vec<FactRef>,
    value: FactRef,
    limits: AdmissionLimits,
) -> Result<(), AdmissionError> {
    if let Some(found) = values
        .iter()
        .find(|v| v.source_record_id == value.source_record_id)
    {
        if found != &value {
            return Err(AdmissionError::StateMismatch);
        }
    } else {
        if values.len() >= limits.max_dependencies {
            return Err(AdmissionError::Limit);
        }
        values.push(value);
    }
    Ok(())
}
fn number(bytes: &[u8]) -> Result<u64, AdmissionError> {
    Ok(u64::from_be_bytes(
        bytes
            .try_into()
            .map_err(|_| AdmissionError::StateMismatch)?,
    ))
}
fn encode_rejection(
    input: &PreparedEvent,
    dataset: DatasetId,
    policy: PolicyEpoch,
    reason: RejectionReason,
) -> Result<Vec<u8>, AdmissionError> {
    let key = IngestKey::Request {
        request_id: input.request,
    }
    .encode()
    .map_err(|_| AdmissionError::InvalidInput)?;
    Ok(frame(
        REJECTION_PREFIX,
        &[
            &key.as_bytes()[2..],
            input.digest.as_bytes(),
            dataset.as_bytes(),
            &policy.get().to_be_bytes(),
            &[reason as u8],
        ],
    )?)
}
fn read_rejection(
    bytes: &[u8],
    request: ProducerRequestId,
    digest: Digest256,
    dataset: DatasetId,
) -> Result<(AdmissionOutcome, PolicyEpoch), AdmissionError> {
    let value = bytes
        .strip_prefix(REJECTION_PREFIX)
        .ok_or(AdmissionError::StateMismatch)?;
    let key = IngestKey::Request {
        request_id: request,
    }
    .encode()
    .map_err(|_| AdmissionError::InvalidInput)?;
    if value.len() != 89
        || value[..32] != key.as_bytes()[2..]
        || value[64..80] != dataset.as_bytes()[..]
    {
        return Err(AdmissionError::StateMismatch);
    }
    if value[32..64] != digest.as_bytes()[..] {
        return Err(AdmissionError::RequestConflict);
    }
    let reason = match value[88] {
        1 => RejectionReason::EmptyTag,
        2 => RejectionReason::EventId,
        3 => RejectionReason::PublicKey,
        4 => RejectionReason::Signature,
        5 => RejectionReason::EventSize,
        _ => return Err(AdmissionError::IncompatibleSchema),
    };
    Ok((
        AdmissionOutcome::Rejected(reason),
        PolicyEpoch::new(number(&value[80..88])?),
    ))
}
