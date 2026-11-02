//! Measurement-only bridge over the existing owner/staging/reservation/outcome
//! protocol. The original source is historical; current Native/Control policy,
//! producer fencing and retained-input identity remain mandatory on both sides
//! of reservation COMMIT. No new socket can be issued by these methods.
use super::*;
use bigbrotr_model::MeasurementEvidenceIntent;

pub struct InitialMeasurementRequest<'a> {
    pub registration: &'a ingest_registry::InitialRegistration,
    pub evidence: LiveMeasurementRequest<'a>,
}
pub struct LiveMeasurementRequest<'a> {
    pub ingest_root: &'a std::path::Path,
    pub binding: MonitorEvidenceBinding,
    pub request_sequence: RequestSequence,
    pub intent: &'a MeasurementEvidenceIntent,
    pub recorded_at: ObservedTime,
    pub limits: AdmissionLimits,
    pub policy_ttl: Duration,
    pub cancelled: &'a AtomicBool,
}
/// Private separation from source-bound acquisition progress. This tag only
/// selects the generic checkpoint reducer; it never authorizes network work.
#[derive(Clone, Copy)]
pub(super) enum MonitorOperation {
    Evidence(MonitorEvidenceBinding, bool),
    Checkpoint(MonitorEvidenceBinding, bool),
}
impl MonitorOperation {
    pub(super) fn evidence(self) -> (MonitorEvidenceBinding, bool) {
        match self {
            Self::Evidence(binding, resolution) | Self::Checkpoint(binding, resolution) => {
                (binding, resolution)
            }
        }
    }
    pub(super) fn checkpoint(self) -> bool {
        matches!(self, Self::Checkpoint(..))
    }
}

impl ControlSession {
    /// Dedicated native owner worker only. Caller retains original intent and
    /// policy buffers plus the transferred operation grant. Cancellation/drop
    /// does not join synchronous FFI; the supervisor retains custody until joined.
    pub async fn append_initial_measurement(
        &mut self,
        store: &mut IngestStore,
        input: InitialMeasurementRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<StoredReceipt, InitialEventError> {
        self.append_native_measurement(
            NativeOwner::Store(store),
            NativeAuthority::Initial(input.registration),
            input.evidence,
            permit,
            None,
            validate,
        )
        .await
    }
    /// Generic producer completion only: no Sync position, job or acquisition
    /// progress. The borrowed grant funds the complete SQL checkpoint and ACK.
    pub async fn append_initial_measurement_with_progress(
        &mut self,
        store: &mut IngestStore,
        input: InitialMeasurementRequest<'_>,
        permit: OwnedPermit,
        progress: &ProgressGrant,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<StoredReceipt, InitialEventError> {
        self.append_native_measurement(
            NativeOwner::Store(store),
            NativeAuthority::Initial(input.registration),
            input.evidence,
            permit,
            Some(progress),
            validate,
        )
        .await
    }
    pub async fn append_live_initial_measurement(
        &mut self,
        writer: &mut LiveInitialWriter,
        input: LiveMeasurementRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<StoredReceipt, InitialEventError> {
        self.append_native_measurement(
            NativeOwner::Store(writer.native.store_mut()),
            NativeAuthority::Live(&writer.binding),
            input,
            permit,
            None,
            validate,
        )
        .await
    }
    pub async fn append_live_successor_measurement(
        &mut self,
        writer: &mut LiveSuccessorWriter,
        input: LiveMeasurementRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<StoredReceipt, InitialEventError> {
        self.append_native_measurement(
            NativeOwner::successor(&mut writer.native, writer.check.as_ref()),
            NativeAuthority::Successor(&writer.binding),
            input,
            permit,
            None,
            validate,
        )
        .await
    }
    async fn append_native_measurement(
        &mut self,
        mut store: NativeOwner<'_>,
        authority: NativeAuthority<'_>,
        input: LiveMeasurementRequest<'_>,
        permit: OwnedPermit,
        progress: Option<&ProgressGrant>,
        mut validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<StoredReceipt, InitialEventError> {
        // Only the initial Monitor owner opts into generic ACK.
        // Later live generations keep their existing evidence-only contract.
        if progress.is_some() && !matches!(authority, NativeAuthority::Initial(_)) {
            return Err(OwnerError::InvalidInput.into());
        }
        let progress_context = progress.map(|_| authority.progress_context(store.manifest()));
        let control = authority.control();
        let identity = *store.manifest().identity();
        if !input.binding.matches(input.intent)
            || input.binding.intent().dataset() != authority.dataset()
            || (authority.epoch() == 1 && input.request_sequence.get() <= 1)
            || input.limits.max_outstanding_requests > u64::from(authority.limit())
        {
            return Err(OwnerError::InvalidInput.into());
        }
        let request = ProducerRequestId {
            producer_slot_id: authority.slot(),
            producer_epoch: ProducerEpoch::new(authority.epoch())
                .map_err(|_| OwnerError::InvalidInput)?,
            request_sequence: input.request_sequence,
        };
        store
            .require_unretired_prefix(request)
            .map_err(InitialEventError::Native)?;
        if input.cancelled.load(Ordering::Relaxed) || !self.available {
            return Err(OwnerError::Unavailable.into());
        }
        prepare_logical_intake::<AdmissionError>(&mut store, authority)?;
        let pending = store
            .pending_measurement(request, &input, &permit)
            .map_err(InitialEventError::Native)?;
        reserve_logical_intake::<AdmissionError>(&mut store, &[pending])?;
        let resolution = input.intent.measurement().prior_unknown().is_some();
        let context = |phase| InitialIngestContext {
            cancelled: input.cancelled,
            progress: progress.is_some(),
            ingest_root: input.ingest_root,
            authority,
            policy_ttl: input.policy_ttl,
            request: Some((request, phase)),
            source: None,
            monitor: Some(if progress.is_some() {
                MonitorOperation::Checkpoint(input.binding, resolution)
            } else {
                MonitorOperation::Evidence(input.binding, resolution)
            }),
            network_event: false,
            job: None,
        };
        let (reservation_tx, mut reservation_lease) = self
            .begin_initial_native_operation(
                &store,
                context(RequestPhase::Reservation),
                &mut validate,
            )
            .await?;
        // Capacity and the exact prior head are checked before any original
        // file or PG reservation. A complete outcome cannot hide a progress gap.
        let mut progress_state = if progress.is_some() {
            let bytes = input
                .intent
                .request_preimage_len(request, input.limits.max_batch_bytes)
                .map_err(|_| InitialEventError::Progress(ProducerProgressError::Limit))?
                as u64;
            Some(
                producer_progress::prepare(
                    &reservation_tx,
                    progress_context.expect("measurement progress context"),
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
        if let Some(state) = &progress_state {
            store
                .require_progress_base(
                    state,
                    progress_context.expect("measurement progress context"),
                )
                .map_err(InitialEventError::Progress)?;
        }
        let staged = store
            .stage_measurement(request, &input, &permit)
            .map_err(InitialEventError::Native)?;
        producer_requests::reserve_monitor(
            &reservation_tx,
            &staged,
            control,
            input.binding,
            resolution,
        )
        .await
        .map_err(InitialEventError::Reservation)?;
        store.validate_measurement_delivery(
            input.cancelled,
            &mut reservation_lease,
            identity.policy_epoch,
        )?;
        reservation_tx
            .commit()
            .await
            .map_err(|_| InitialEventError::Reservation(ReservationError::CommitUnknown))?;
        store.validate_measurement_delivery(
            input.cancelled,
            &mut reservation_lease,
            identity.policy_epoch,
        )?;
        if self.client.is_closed() {
            return Err(InitialEventError::DeliveryUnknown);
        }
        self.available = true;
        let (tx, mut lease) = self
            .begin_initial_native_operation(&store, context(RequestPhase::Dispatch), &mut validate)
            .await?;
        if let Some(state) = &progress_state {
            producer_progress::recheck(
                &tx,
                progress_context.expect("measurement progress context"),
                state,
            )
            .await
            .map_err(InitialEventError::Progress)?;
        }
        let result = store.append_measurement(request, &input, permit, progress.is_some(), || {
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
        store.validate_measurement_delivery(input.cancelled, &mut lease, identity.policy_epoch)?;
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
        store.validate_measurement_delivery(input.cancelled, &mut lease, identity.policy_epoch)?;
        let pending_ack = if let Some(state) = &mut progress_state {
            let basis = store
                .checkpoint_evidence(&receipt)
                .map_err(InitialEventError::Native)?;
            producer_progress::record(
                &tx,
                progress_context.expect("measurement progress context"),
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
            Some(
                state
                    .pending_acknowledgement(
                        progress_context.expect("measurement progress context"),
                        &basis,
                    )
                    .map_err(InitialEventError::Progress)?,
            )
        } else {
            None
        };
        store.validate_measurement_delivery(input.cancelled, &mut lease, identity.policy_epoch)?;
        let confirmed = match pending_ack {
            Some(pending) => Some(
                pending
                    .commit(tx)
                    .await
                    .map_err(|_| InitialEventError::Outcome(OutcomeError::CommitUnknown))?,
            ),
            None => {
                tx.commit()
                    .await
                    .map_err(|_| InitialEventError::Outcome(OutcomeError::CommitUnknown))?;
                None
            }
        };
        store.validate_measurement_delivery(input.cancelled, &mut lease, identity.policy_epoch)?;
        if self.client.is_closed() {
            return Err(InitialEventError::DeliveryUnknown);
        }
        if let Some(confirmed) = confirmed {
            store
                .acknowledge_checkpoint(
                    confirmed,
                    progress.expect("measurement ACK grant").permit(),
                    input.cancelled,
                    || {
                        if input.cancelled.load(Ordering::Relaxed) {
                            return Err(AdmissionError::AuthorityUnavailable);
                        }
                        lease
                            .check_delivery(identity.policy_epoch)
                            .map_err(|_| AdmissionError::AuthorityUnavailable)
                    },
                )
                .map_err(InitialEventError::Native)?;
        }
        store.validate_measurement_delivery(input.cancelled, &mut lease, identity.policy_epoch)?;
        if self.client.is_closed() {
            return Err(InitialEventError::DeliveryUnknown);
        }
        store.delivered();
        self.available = true;
        Ok(receipt)
    }
}
impl NativeOwner<'_> {
    fn validate_measurement_delivery(
        &self,
        cancelled: &AtomicBool,
        lease: &mut PolicyLeaseTracker,
        policy: PolicyEpoch,
    ) -> Result<(), InitialEventError> {
        if cancelled.load(Ordering::Relaxed) || lease.check_delivery(policy).is_err() {
            return Err(InitialEventError::DeliveryUnknown);
        }
        self.validate_delivery(cancelled, lease, policy)
    }

    fn pending_measurement(
        &self,
        request: ProducerRequestId,
        input: &LiveMeasurementRequest<'_>,
        permit: &OwnedPermit,
    ) -> Result<PendingReservationRequest, AdmissionError> {
        match self {
            Self::Store(s) => s.measurement_pending_reservation(
                request,
                input.intent,
                input.recorded_at,
                input.limits,
                permit,
            ),
            Self::Successor(s) => s.measurement_pending_reservation(
                request,
                input.intent,
                input.recorded_at,
                input.limits,
                permit,
            ),
        }
    }
    fn stage_measurement(
        &mut self,
        request: ProducerRequestId,
        input: &LiveMeasurementRequest<'_>,
        permit: &OwnedPermit,
    ) -> Result<crate::storage::rocks::store::StagedProducerRequest, AdmissionError> {
        // The optional Native flag selects successor fencing. Initial owners
        // use None; this bridge checks cancellation at its authority fences.
        match self {
            Self::Store(s) => s.stage_measurement_append(
                request,
                input.intent,
                input.recorded_at,
                input.limits,
                permit,
                None,
            ),
            Self::Successor(s) => s.stage_measurement_append(
                request,
                input.intent,
                input.recorded_at,
                input.limits,
                permit,
                input.cancelled,
            ),
        }
    }
    fn append_measurement(
        &mut self,
        request: ProducerRequestId,
        input: &LiveMeasurementRequest<'_>,
        permit: OwnedPermit,
        checkpoint: bool,
        authority: impl FnMut() -> Result<(), AdmissionError>,
    ) -> Result<StoredReceipt, AdmissionError> {
        match self {
            Self::Store(s) if checkpoint => s.append_checkpoint_measurement(
                request,
                input.intent,
                input.recorded_at,
                input.limits,
                permit,
                authority,
            ),
            Self::Successor(_) if checkpoint => Err(AdmissionError::InvalidInput),
            Self::Store(s) => s.append_measurement(
                request,
                input.intent,
                input.recorded_at,
                input.limits,
                permit,
                None,
                authority,
            ),
            Self::Successor(s) => s.append_measurement(
                request,
                input.intent,
                input.recorded_at,
                input.limits,
                permit,
                input.cancelled,
                authority,
            ),
        }
    }
}
