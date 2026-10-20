//! Positive recovery of an activated producer. No historical incarnation is
//! adopted, and neither this metadata nor its closing proof authorizes a writer.
use super::*;
use crate::{
    runtime::OwnedPermit,
    storage::{
        postgres::{producer_outcomes, producer_requests},
        rocks::store::{
            ActivatedInitial, ActivatedProducerClosing, AdmissionError, BootstrapError,
            IngestRecovery, ProducerRecoveryError, ProducerRecoveryLimits, ResolvedProducerRequest,
        },
    },
};
use bigbrotr_model::{DictionaryKind, ProducerRequestId, ProducerSlotId};
use std::{
    path::Path,
    sync::atomic::{AtomicBool, Ordering},
};

/// Retained immutable SQL/native identity, before the native closing barrier.
/// The owning caller retains the exclusive native handle separately.
pub struct ActivatedInitialRecovery {
    native: ActivatedInitial,
    metadata: Metadata,
}
/// The positive native barrier and its grants remain owned throughout replay.
/// Deliberately has no source-resolution or admission conversion.
pub struct ClosedActivatedInitialProducer {
    pub(super) native: ActivatedProducerClosing,
    pub(super) metadata: Metadata,
}
pub(super) struct Metadata {
    previous: ingest_registry::InitialRegistration,
    intent: initial_history::Intent,
    reserved_through: u64,
    purpose: Purpose,
    _permit: OwnedPermit,
}
impl Metadata {
    pub(super) fn target_role(&self) -> &[u8; 16] {
        &self.intent.role
    }
}
/// Only the distinct public factories select a protocol. No caller supplies a
/// relaxed validator or converts an ordinary recovery result into source proof.
#[derive(Clone, Copy)]
pub(super) enum Purpose {
    Positive,
    Source,
    Retirement,
}
impl Purpose {
    fn schema(self) -> u32 {
        match self {
            Self::Positive => 34,
            Self::Source => 35,
            Self::Retirement => 37,
        }
    }
}
pub(super) struct TargetState {
    pub(super) reserved_through: u64,
    pub(super) producer_closing: bool,
    pub(super) source_closed: bool,
}
impl ActivatedInitialRecovery {
    pub fn producer_slot_id(&self) -> ProducerSlotId {
        self.native.producer_slot_id()
    }
    pub fn issued_through(&self) -> u64 {
        self.native.issued_through()
    }
    pub fn reserved_through(&self) -> u64 {
        self.metadata.reserved_through
    }
}
impl ClosedActivatedInitialProducer {
    pub fn producer_slot_id(&self) -> ProducerSlotId {
        self.native.producer_slot_id()
    }
    pub fn issued_through(&self) -> u64 {
        self.native.issued_through()
    }
    pub fn reserved_through(&self) -> u64 {
        self.metadata.reserved_through
    }
}

/// One original target request, without a fabricated epoch2 registration.
pub struct ActivatedInitialResolveRequest<'a> {
    pub ingest_root: &'a Path,
    pub request: ProducerRequestId,
    pub limits: ProducerRecoveryLimits,
    pub policy_ttl: Duration,
    pub cancelled: &'a AtomicBool,
}

impl ControlSession {
    pub async fn inspect_activated_initial_producer(
        &mut self,
        recovery: &IngestRecovery,
        input: InitialHistoryRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<ActivatedInitialRecovery, InitialSuccessionError> {
        self.inspect_activated_producer(recovery, input, permit, Purpose::Positive, validate)
            .await
    }

    pub(super) async fn inspect_activated_producer(
        &mut self,
        recovery: &IngestRecovery,
        input: InitialHistoryRequest<'_>,
        permit: OwnedPermit,
        purpose: Purpose,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<ActivatedInitialRecovery, InitialSuccessionError> {
        if permit.request().units().data_bytes() < 128 * 1024
            || permit.request().units().items() == 0
        {
            return Err(InitialSuccessionError::Native(
                BootstrapError::InsufficientReservation,
            ));
        }
        let current = self.identity;
        let instance = *self.request.instance.as_bytes();
        let (tx, mut lease, epoch) = self
            .begin_initial_history_locked(recovery, &input, 2, purpose.schema(), validate)
            .await?;
        let intent = initial_history::read_intent(&tx, recovery).await?;
        let native = ActivatedInitial::inspect(recovery, intent.record)
            .map_err(InitialSuccessionError::Native)?;
        let previous =
            ingest_registry::reconstruct_historical(&tx, instance, native.previous_manifest())
                .await
                .map_err(InitialSuccessionError::Registry)?;
        initial_history::require_intent_identity(&tx, &previous, &intent, current, epoch).await?;
        let state = require_target(&tx, &previous, &intent, purpose).await?;
        if native.issued_through() > state.reserved_through
            || (state.producer_closing && !native.is_closing())
            || (matches!(purpose, Purpose::Source | Purpose::Retirement)
                && native.issued_through() != state.reserved_through)
            || (matches!(purpose, Purpose::Retirement) && !native.is_closing())
        {
            return Err(OwnerError::InvalidMetadata.into());
        }
        native
            .validate(recovery)
            .map_err(InitialSuccessionError::Native)?;
        // Both predecessor and activated target can retain typed progress. The
        // unactivated-target protocol still keeps its separate empty veto.
        for manifest in [native.previous_manifest(), native.manifest()] {
            producer_progress::recovery::require(
                &tx,
                recovery,
                manifest,
                native.producer_slot_id(),
                &permit,
                || producer_progress::recovery::authority(&mut lease, epoch, input.cancelled),
            )
            .await?;
        }
        initial_history::finish(tx, &mut lease, epoch, input.cancelled).await?;
        if self.client.is_closed() {
            return Err(InitialSuccessionError::DeliveryUnknown);
        }
        self.available = true;
        Ok(ActivatedInitialRecovery {
            native,
            metadata: Metadata {
                previous,
                intent,
                reserved_through: state.reserved_through,
                purpose,
                _permit: permit,
            },
        })
    }

    /// Native closes before SQL. Any error after dispatch retains that durable
    /// possibility; an unknown COMMIT never restores this Control session.
    pub async fn close_activated_initial_producer(
        &mut self,
        recovery: &mut IngestRecovery,
        inspected: ActivatedInitialRecovery,
        input: InitialHistoryRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<ClosedActivatedInitialProducer, InitialSuccessionError> {
        if matches!(inspected.metadata.purpose, Purpose::Retirement) {
            return Err(OwnerError::InvalidInput.into());
        }
        if permit.request().units().data_bytes() < 4096
            || permit.request().units().native_bytes() < 4096
            || permit.request().units().items() == 0
        {
            return Err(InitialSuccessionError::Native(
                BootstrapError::InsufficientReservation,
            ));
        }
        inspected
            .native
            .validate(recovery)
            .map_err(InitialSuccessionError::Native)?;
        require_deployment(&inspected.metadata, self)?;
        let current = self.identity;
        let (tx, mut lease, epoch) = self
            .begin_initial_history_locked(
                recovery,
                &input,
                2,
                inspected.metadata.purpose.schema(),
                validate,
            )
            .await?;
        require_metadata(&tx, &inspected.metadata, current, epoch).await?;
        initial_history::check_delivery(&mut lease, epoch, input.cancelled)?;
        let native = inspected
            .native
            .close_producer(recovery, permit, || {
                if input.cancelled.load(Ordering::Relaxed) {
                    return Err(BootstrapError::AuthorityUnavailable);
                }
                lease
                    .check_delivery(epoch)
                    .map_err(|_| BootstrapError::AuthorityUnavailable)
            })
            .map_err(InitialSuccessionError::Native)?;
        tx.execute("UPDATE control.producer_epochs SET state='closing' WHERE producer_slot_id=$1::bytea AND producer_epoch=2 AND state='open'",
            &[&&native.producer_slot_id().as_bytes()[..]]).await.map_err(backend)?;
        native
            .validate(recovery)
            .map_err(InitialSuccessionError::Native)?;
        initial_history::check_delivery(&mut lease, epoch, input.cancelled)?;
        tx.commit()
            .await
            .map_err(|_| InitialSuccessionError::CommitUnknown)?;
        initial_history::check_delivery(&mut lease, epoch, input.cancelled)?;
        if self.client.is_closed() {
            return Err(InitialSuccessionError::DeliveryUnknown);
        }
        self.available = true;
        Ok(ClosedActivatedInitialProducer {
            native,
            metadata: inspected.metadata,
        })
    }

    /// Retirement observes an existing barrier only. It cannot call the native
    /// close kernel or turn an OPEN target into a supported retirement attempt.
    pub(super) async fn observe_activated_producer(
        &mut self,
        recovery: &IngestRecovery,
        inspected: ActivatedInitialRecovery,
        input: InitialHistoryRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<ClosedActivatedInitialProducer, InitialSuccessionError> {
        if !matches!(inspected.metadata.purpose, Purpose::Retirement) {
            return Err(OwnerError::InvalidInput.into());
        }
        if permit.request().units().data_bytes() < 4096
            || permit.request().units().native_bytes() < 4096
            || permit.request().units().items() == 0
        {
            return Err(InitialSuccessionError::Native(
                BootstrapError::InsufficientReservation,
            ));
        }
        inspected
            .native
            .validate(recovery)
            .map_err(InitialSuccessionError::Native)?;
        require_deployment(&inspected.metadata, self)?;
        let current = self.identity;
        let (tx, mut lease, epoch) = self
            .begin_initial_history_locked(recovery, &input, 2, 37, validate)
            .await?;
        require_metadata(&tx, &inspected.metadata, current, epoch).await?;
        initial_history::check_delivery(&mut lease, epoch, input.cancelled)?;
        let native = inspected
            .native
            .observe_closed_producer(recovery, permit, || {
                if input.cancelled.load(Ordering::Relaxed) {
                    return Err(BootstrapError::AuthorityUnavailable);
                }
                lease
                    .check_delivery(epoch)
                    .map_err(|_| BootstrapError::AuthorityUnavailable)
            })
            .map_err(InitialSuccessionError::Native)?;
        native
            .validate(recovery)
            .map_err(InitialSuccessionError::Native)?;
        initial_history::finish(tx, &mut lease, epoch, input.cancelled).await?;
        if self.client.is_closed() {
            return Err(InitialSuccessionError::DeliveryUnknown);
        }
        self.available = true;
        Ok(ClosedActivatedInitialProducer {
            native,
            metadata: inspected.metadata,
        })
    }

    /// Stream one positively resolved original. Existing copies retain their
    /// original recorder and xmin; only a missing exact copy is inserted.
    #[allow(
        clippy::too_many_arguments,
        reason = "distinct retained closing proof and per-request input/outcome grants"
    )]
    pub async fn resolve_activated_initial_request(
        &mut self,
        recovery: &IngestRecovery,
        closed: &ClosedActivatedInitialProducer,
        input: ActivatedInitialResolveRequest<'_>,
        input_permit: OwnedPermit,
        outcome_permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<ResolvedProducerRequest, ProducerResolveError> {
        if input.request.producer_slot_id != closed.producer_slot_id()
            || input.request.producer_epoch.get() != 2
            || input.request.request_sequence.get() > closed.reserved_through()
        {
            return Err(OwnerError::InvalidInput.into());
        }
        require_deployment(&closed.metadata, self)?;
        closed
            .native
            .validate(recovery)
            .map_err(|e| ProducerResolveError::Native(AdmissionError::from(e).into()))?;
        let current = self.identity;
        let request = InitialHistoryRequest {
            ingest_root: input.ingest_root,
            policy_ttl: input.policy_ttl,
            cancelled: input.cancelled,
        };
        let (tx, mut lease, epoch) = self
            .begin_initial_history_locked(
                recovery,
                &request,
                2,
                closed.metadata.purpose.schema(),
                validate,
            )
            .await?;
        let state = require_metadata(&tx, &closed.metadata, current, epoch)
            .await
            .map_err(initial_history::history_resolve_error)?;
        if !state.producer_closing {
            return Err(OwnerError::InvalidMetadata.into());
        }
        let reservation =
            producer_requests::read(&tx, closed.metadata.previous.dataset_id, input.request)
                .await
                .map_err(ProducerResolveError::Reservation)?
                .ok_or(ProducerResolveError::Native(
                    ProducerRecoveryError::Unresolved,
                ))?;
        if reservation.manifest_sha256
            != <[u8; 32]>::from(Sha256::digest(closed.native.manifest().encode().as_bytes()))
        {
            return Err(OwnerError::InvalidMetadata.into());
        }
        let resolved = closed
            .native
            .resolve_original_request(
                recovery,
                input.request,
                reservation.digest,
                input.limits,
                input_permit,
                outcome_permit,
                || {
                    if input.cancelled.load(Ordering::Relaxed) {
                        return Err(AdmissionError::AuthorityUnavailable);
                    }
                    lease
                        .check_delivery(epoch)
                        .map_err(|_| AdmissionError::AuthorityUnavailable)
                },
            )
            .map_err(ProducerResolveError::Native)?;
        if matches!(closed.metadata.purpose, Purpose::Retirement) {
            if resolved.input_bytes() != reservation.input_bytes {
                return Err(ProducerResolveError::Outcome(
                    producer_outcomes::OutcomeError::Conflict,
                ));
            }
            producer_retirement::require_retirement_copy(
                &tx,
                &resolved,
                input.request,
                reservation.manifest_sha256,
            )
            .await?;
        } else if state.source_closed {
            producer_outcomes::require_recovered(&tx, &reservation, &resolved)
                .await
                .map_err(ProducerResolveError::Outcome)?;
        } else {
            producer_outcomes::record_recovered(&tx, &reservation, &resolved, current)
                .await
                .map_err(ProducerResolveError::Outcome)?;
        }
        initial_history::check_delivery(&mut lease, epoch, input.cancelled)?;
        tx.commit()
            .await
            .map_err(|_| ProducerResolveError::CommitUnknown)?;
        initial_history::check_delivery(&mut lease, epoch, input.cancelled)?;
        if self.client.is_closed() {
            return Err(ProducerResolveError::DeliveryUnknown);
        }
        self.available = true;
        Ok(resolved)
    }
}

pub(super) fn require_deployment(
    metadata: &Metadata,
    owner: &ControlSession,
) -> Result<(), OwnerError> {
    if metadata.previous.dataset_id != owner.request.dataset
        || metadata.previous.instance_id != *owner.request.instance.as_bytes()
    {
        return Err(OwnerError::InvalidInput);
    }
    Ok(())
}
pub(super) async fn require_metadata(
    tx: &Transaction<'_>,
    metadata: &Metadata,
    current: ingest_registry::ControlOwner,
    epoch: bigbrotr_model::PolicyEpoch,
) -> Result<TargetState, InitialSuccessionError> {
    initial_history::require_intent_identity(
        tx,
        &metadata.previous,
        &metadata.intent,
        current,
        epoch,
    )
    .await?;
    let state = require_target(tx, &metadata.previous, &metadata.intent, metadata.purpose).await?;
    if state.reserved_through != metadata.reserved_through {
        return Err(OwnerError::InvalidMetadata.into());
    }
    Ok(state)
}

async fn require_target(
    tx: &Transaction<'_>,
    previous: &ingest_registry::InitialRegistration,
    intent: &initial_history::Intent,
    purpose: Purpose,
) -> Result<TargetState, InitialSuccessionError> {
    tx.query_one(
        match purpose {
            Purpose::Positive => "SELECT control.require_activated_initial_recovery($1::bytea)",
            Purpose::Source => "SELECT control.require_activated_initial_source($1::bytea)",
            Purpose::Retirement => "SELECT control.require_activated_initial_retirement($1::bytea)",
        },
        &[&&previous.ingest_resource_id.as_bytes()[..]],
    )
    .await
    .map_err(backend)?;
    // Positive exact identities, not merely the SQL kind/count census. Both
    // historical manifests are immutable and the result allocation is bounded.
    let rows = tx.query("SELECT owner_epoch::text,dictionary_kind,substring(allocation_domain FROM 1 FOR 17)
        FROM control.allocation_domains WHERE dataset_id=$1::bytea AND owner_resource_id=$2::bytea
        AND owner_epoch IN (1,2) ORDER BY owner_epoch,dictionary_kind,allocation_domain LIMIT 19 FOR SHARE",
        &[&&previous.dataset_id.as_bytes()[..],&&previous.ingest_resource_id.as_bytes()[..]]).await.map_err(backend)?;
    if rows.len() != 18 {
        return Err(OwnerError::InvalidMetadata.into());
    }
    for (index, row) in rows.iter().enumerate() {
        let epoch = if index < 9 { "1" } else { "2" };
        let kind = DictionaryKind::try_from((index % 9 + 1) as u16)
            .map_err(|_| OwnerError::InvalidMetadata)?;
        let manifest = if index < 9 {
            intent.record.previous_manifest()
        } else {
            intent.record.next_manifest()
        };
        let expected = manifest
            .allocation_namespaces()
            .domain(kind)
            .ok_or(OwnerError::InvalidMetadata)?;
        if row.try_get::<_, &str>(0).map_err(backend)? != epoch
            || row.try_get::<_, i16>(1).map_err(backend)? != kind as i16
            || row.try_get::<_, &[u8]>(2).map_err(backend)? != expected.as_bytes()
        {
            return Err(OwnerError::InvalidMetadata.into());
        }
    }
    let row = tx
        .query_one(
            "SELECT p.reserved_through_request::text,(p.state='closing' OR ($2::boolean AND p.state='retired')),s.state='closed'
        FROM control.producer_epochs p JOIN control.source_streams s
            ON s.dataset_id=p.dataset_id AND s.owner_resource_id=p.owner_resource_id AND s.owner_epoch=p.owner_epoch
        WHERE p.producer_slot_id=$1::bytea AND p.producer_epoch=2",
            &[&&previous.producer_slot_id.as_bytes()[..], &matches!(purpose, Purpose::Retirement)],
        )
        .await
        .map_err(backend)?;
    let reserved = row
        .try_get::<_, &str>(0)
        .map_err(backend)?
        .parse()
        .map_err(|_| OwnerError::InvalidMetadata)?;
    Ok(TargetState {
        reserved_through: reserved,
        producer_closing: row.try_get(1).map_err(backend)?,
        source_closed: row.try_get(2).map_err(backend)?,
    })
}
