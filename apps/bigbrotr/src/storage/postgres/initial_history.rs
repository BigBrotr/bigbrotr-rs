//! Read-only predecessor reconstruction beneath an exact prepared successor.
//! Retained metadata and positive witnesses never grant an admission lifetime.
use super::super::producer_requests;
use super::*;
use crate::{
    runtime::{
        OwnedPermit,
        policy_authority::{LeaseResponse, PolicyLeaseTracker, PolicyLeaseTtl},
    },
    storage::rocks::store::{
        AdmissionError, BootstrapError, HistoricalInitial, IngestRecovery, InitialHandoff,
        InitialHandoffRecord, ProducerRecoveryError, ResolvedProducerRequest, ResolvedSource,
        SourceResolution,
    },
};
use bigbrotr_model::PolicyEpoch;
use std::{
    path::Path,
    sync::atomic::{AtomicBool, Ordering},
};

pub struct InitialHistoryRequest<'a> {
    pub ingest_root: &'a Path,
    pub policy_ttl: Duration,
    pub cancelled: &'a AtomicBool,
}

/// The borrow retains actual native exclusion. The metadata permit covers the
/// bounded intent and registration; neither these bytes nor this type is a lease.
pub struct PreparedInitialHistory<'a> {
    native: HistoricalInitial<'a>,
    registration: ingest_registry::InitialRegistration,
    intent: Intent,
    _permit: OwnedPermit,
}
impl<'a> PreparedInitialHistory<'a> {
    pub fn registration(&self) -> &ingest_registry::InitialRegistration {
        &self.registration
    }
    pub fn issued_through(&self) -> u64 {
        self.native.issued_through()
    }
    pub fn source_resolution(
        &self,
        permit: OwnedPermit,
    ) -> Result<SourceResolution<'a>, BootstrapError> {
        let closing = self.native.observe_closed_producer(permit)?;
        self.native
            .source_resolution(closing, self.native.issued_through())
    }
    pub fn reconstruct_handoff(
        &self,
        source: &ResolvedSource<'_>,
    ) -> Result<InitialHandoff, BootstrapError> {
        self.native.reconstruct_handoff(source)
    }
}

pub(super) struct Intent {
    pub(super) handoff: Vec<u8>,
    pub(super) record: InitialHandoffRecord,
    pub(super) role: [u8; 16],
    created_at_us: i64,
    lease_expires_at_us: i64,
    pub(super) request_limit: u32,
    pub(super) prepared_resource: [u8; 16],
    pub(super) prepared_epoch: u64,
    pub(super) prepared_incarnation: [u8; 16],
    pub(super) policy: PolicySnapshot,
}

impl ControlSession {
    /// Inspect an already completed native preparation in a new actual owner.
    /// Missing intent, native epoch1 and inconsistent state refuse. The separate
    /// gap adapter handles a pending epoch1 transition; neither path activates
    /// a writer or retires the empty attempted incarnation.
    pub async fn inspect_prepared_initial_history<'a>(
        &mut self,
        recovery: &'a IngestRecovery,
        input: InitialHistoryRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<PreparedInitialHistory<'a>, InitialSuccessionError> {
        if permit.request().units().data_bytes() < 128 * 1024
            || permit.request().units().items() == 0
        {
            return Err(InitialSuccessionError::Native(
                BootstrapError::InsufficientReservation,
            ));
        }
        let instance = *self.request.instance.as_bytes();
        let current = self.identity;
        let (tx, mut lease, epoch) = self
            .begin_initial_history(recovery, &input, 2, validate)
            .await?;
        let intent = read_intent(&tx, recovery).await?;
        let native = recovery
            .historical_initial(&intent.handoff)
            .map_err(InitialSuccessionError::Native)?;
        let registration =
            ingest_registry::reconstruct_historical(&tx, instance, native.manifest())
                .await
                .map_err(InitialSuccessionError::Registry)?;
        let history = PreparedInitialHistory {
            native,
            registration,
            intent,
            _permit: permit,
        };
        require_intent(&tx, &history.registration, &history.intent, current, epoch).await?;
        finish(tx, &mut lease, epoch, input.cancelled).await?;
        if self.client.is_closed() {
            return Err(InitialSuccessionError::DeliveryUnknown);
        }
        self.available = true;
        Ok(history)
    }

    /// Revalidate one original input and its already-existing PG outcome copy.
    /// This cannot synthesize missing detail, advance a checkpoint or admit work.
    pub async fn inspect_prepared_initial_request(
        &mut self,
        history: &PreparedInitialHistory<'_>,
        input: ProducerResolveRequest<'_>,
        input_permit: OwnedPermit,
        outcome_permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<ResolvedProducerRequest, ProducerResolveError> {
        if input.registration != &history.registration
            || input.request.producer_slot_id != history.native.producer_slot_id()
            || input.request.producer_epoch.get() != 1
            || input.request.request_sequence.get() > history.native.issued_through()
        {
            return Err(OwnerError::InvalidInput.into());
        }
        let current = self.identity;
        let request = InitialHistoryRequest {
            ingest_root: input.ingest_root,
            policy_ttl: input.policy_ttl,
            cancelled: input.cancelled,
        };
        let (tx, mut lease, epoch) = self
            .begin_initial_history(history.native.recovery(), &request, 2, validate)
            .await?;
        require_intent(&tx, &history.registration, &history.intent, current, epoch)
            .await
            .map_err(history_resolve_error)?;
        let resolved = resolve_original(
            &tx,
            &history.native,
            &history.registration,
            &input,
            input_permit,
            outcome_permit,
            &mut lease,
            epoch,
        )
        .await?;
        finish(tx, &mut lease, epoch, input.cancelled).await?;
        if self.client.is_closed() {
            return Err(ProducerResolveError::DeliveryUnknown);
        }
        self.available = true;
        Ok(resolved)
    }

    pub(super) async fn begin_initial_history<'a, E>(
        &'a mut self,
        recovery: &IngestRecovery,
        input: &InitialHistoryRequest<'_>,
        owner_epoch: u64,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<(Transaction<'a>, PolicyLeaseTracker, PolicyEpoch), ProducerOperationError<E>> {
        self.begin_initial_history_locked(recovery, input, owner_epoch, 30, validate)
            .await
    }

    pub(super) async fn begin_empty_initial_history<'a, E>(
        &'a mut self,
        recovery: &IngestRecovery,
        input: &InitialHistoryRequest<'_>,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<(Transaction<'a>, PolicyLeaseTracker, PolicyEpoch), ProducerOperationError<E>> {
        self.begin_initial_history_locked(recovery, input, 2, 31, validate)
            .await
    }

    pub(super) async fn begin_initial_history_locked<'a, E>(
        &'a mut self,
        recovery: &IngestRecovery,
        input: &InitialHistoryRequest<'_>,
        owner_epoch: u64,
        required_schema: u32,
        mut validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<(Transaction<'a>, PolicyLeaseTracker, PolicyEpoch), ProducerOperationError<E>> {
        let identity = recovery.manifest().identity();
        if input.cancelled.load(Ordering::Relaxed) {
            if required_schema == 37 {
                // The retirement protocol never returns reusable authority after
                // cancellation, including before the first SQL transaction.
                self.available = false;
            }
            return Err(OwnerError::Unavailable.into());
        }
        if !recovery.matches_configured_root(input.ingest_root)
            || identity.dataset_id != self.request.dataset
            || identity.owner_epoch.get() != owner_epoch
            || !matches!(
                (owner_epoch, recovery.manifest().key_schema()),
                (1, 1) | (2, 2)
            )
        {
            return Err(OwnerError::InvalidInput.into());
        }
        let mut lease = PolicyLeaseTracker::new(
            self.request.dataset,
            self.identity.process_incarnation,
            identity.policy_epoch,
            PolicyLeaseTtl::new(input.policy_ttl).map_err(|_| OwnerError::InvalidInput)?,
        )
        .map_err(|_| OwnerError::Unavailable)?;
        let ticket = lease.begin_refresh().map_err(|_| OwnerError::Unavailable)?;
        self.begin_operation()?;
        let tx = application_transaction(&mut self.client, required_schema)
            .await
            .map_err(transaction_error)?;
        if required_schema >= 31 {
            // Generic zero-request import claims take SHARE on this Control.
            // Exclude them before dataset/target custody and any absence check.
            tx.query_one(
                "SELECT epoch FROM control.owner_epochs WHERE resource_id=$1::bytea
                 AND epoch=$2::text::numeric FOR UPDATE",
                &[
                    &&self.identity.resource_id.as_bytes()[..],
                    &self.identity.epoch.get().to_string(),
                ],
            )
            .await
            .map_err(backend)?;
        }
        require(&tx, self.request.dataset, self.identity).await?;
        // snapshot takes dataset UPDATE before historical parent/producer locks.
        let policy = snapshot(&tx, &self.request, &mut validate).await?;
        if policy.policy_revision < identity.policy_epoch.get() {
            return Err(OwnerError::InvalidPolicy.into());
        }
        let remaining: i64 = tx
            .query_one(
                "SELECT lease_expires_at_us-(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint
             FROM control.owner_epochs WHERE resource_id=$1::bytea AND epoch=$2::text::numeric",
                &[
                    &&self.identity.resource_id.as_bytes()[..],
                    &self.identity.epoch.get().to_string(),
                ],
            )
            .await
            .map_err(backend)?
            .try_get(0)
            .map_err(backend)?;
        let remaining = u64::try_from(remaining)
            .ok()
            .filter(|n| *n > 0)
            .ok_or(OwnerError::Unavailable)?;
        let epoch = PolicyEpoch::new(policy.policy_revision);
        lease
            .complete_refresh(
                &ticket,
                LeaseResponse {
                    dataset: self.request.dataset,
                    process_incarnation: self.identity.process_incarnation,
                    policy_epoch: epoch,
                    ttl: input.policy_ttl.min(Duration::from_micros(remaining)),
                },
            )
            .map_err(|_| OwnerError::Unavailable)?;
        check_delivery(&mut lease, epoch, input.cancelled)?;
        Ok((tx, lease, epoch))
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "distinct native context, original metadata and retained input/outcome grants"
)]
pub(super) async fn resolve_original(
    tx: &Transaction<'_>,
    native: &HistoricalInitial<'_>,
    registration: &ingest_registry::InitialRegistration,
    input: &ProducerResolveRequest<'_>,
    input_permit: OwnedPermit,
    outcome_permit: OwnedPermit,
    lease: &mut PolicyLeaseTracker,
    epoch: PolicyEpoch,
) -> Result<ResolvedProducerRequest, ProducerResolveError> {
    if input.registration != registration
        || input.request.producer_slot_id != native.producer_slot_id()
        || input.request.producer_epoch.get() != 1
        || input.request.request_sequence.get() > native.issued_through()
    {
        return Err(OwnerError::InvalidInput.into());
    }
    let reservation = producer_requests::read(tx, registration.dataset_id, input.request)
        .await
        .map_err(ProducerResolveError::Reservation)?
        .ok_or(ProducerResolveError::Native(
            ProducerRecoveryError::Unresolved,
        ))?;
    let manifest: [u8; 32] = Sha256::digest(native.manifest().encode().as_bytes()).into();
    if reservation.manifest_sha256 != manifest {
        return Err(OwnerError::InvalidMetadata.into());
    }
    let resolved = native
        .resolve_original_request(
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
    if resolved.input_bytes() != reservation.input_bytes
        || resolved.native_outcome().manifest_sha256() != manifest
    {
        return Err(ProducerResolveError::Outcome(
            super::super::producer_outcomes::OutcomeError::Conflict,
        ));
    }
    producer_retirement::require_retirement_copy(tx, &resolved, input.request, manifest).await?;
    Ok(resolved)
}

pub(super) async fn read_intent(
    tx: &Transaction<'_>,
    recovery: &IngestRecovery,
) -> Result<Intent, InitialSuccessionError> {
    let id = recovery.manifest().identity();
    let row = tx.query_opt(
        "SELECT substring(handoff FROM 1 FOR 611),substring(target_role_instance_id FROM 1 FOR 17),
                created_at_us,lease_expires_at_us,outstanding_request_limit::bigint,
                substring(prepared_by_resource FROM 1 FOR 17),left(prepared_by_epoch::text,21),
                substring(prepared_by_incarnation FROM 1 FOR 17),left(config_revision::text,21),
                left(policy_revision::text,21),substring(config_digest FROM 1 FOR 33),
                substring(policy_digest FROM 1 FOR 33)
         FROM control.initial_ingest_successions
         WHERE owner_resource_id=$1::bytea AND dataset_id=$2::bytea",
        &[&&id.owner_resource_id.as_bytes()[..], &&id.dataset_id.as_bytes()[..]])
        .await.map_err(backend)?.ok_or(OwnerError::InvalidMetadata)?;
    let handoff: &[u8] = row.try_get(0).map_err(backend)?;
    if handoff.len() > 610 {
        return Err(OwnerError::InvalidMetadata.into());
    }
    let nonzero = |index| -> Result<[u8; 16], OwnerError> {
        let id = fixed(&row, index)?;
        if id == [0; 16] {
            return Err(OwnerError::InvalidMetadata);
        }
        Ok(id)
    };
    let intent = Intent {
        handoff: handoff.to_vec(),
        record: InitialHandoffRecord::decode(handoff).map_err(InitialSuccessionError::Native)?,
        role: nonzero(1)?,
        created_at_us: row.try_get(2).map_err(backend)?,
        lease_expires_at_us: row.try_get(3).map_err(backend)?,
        request_limit: u32::try_from(row.try_get::<_, i64>(4).map_err(backend)?)
            .map_err(|_| OwnerError::InvalidMetadata)?,
        prepared_resource: nonzero(5)?,
        prepared_epoch: number(&row, 6)?,
        prepared_incarnation: nonzero(7)?,
        policy: PolicySnapshot {
            config_revision: number(&row, 8)?,
            policy_revision: number(&row, 9)?,
            config_digest: fixed(&row, 10)?,
            policy_digest: fixed(&row, 11)?,
        },
    };
    if intent.lease_expires_at_us <= intent.created_at_us
        || !(1..=64).contains(&intent.request_limit)
    {
        return Err(OwnerError::InvalidMetadata.into());
    }
    Ok(intent)
}

/// The same exact immutable attribution is required before reading completed
/// history or dispatching an existing pending preparation. No current head is
/// substituted for the target's original policy, times or preparing Control.
pub(super) async fn require_intent(
    tx: &Transaction<'_>,
    s: &ingest_registry::InitialRegistration,
    i: &Intent,
    current: ingest_registry::ControlOwner,
    current_epoch: PolicyEpoch,
) -> Result<(), InitialSuccessionError> {
    require_intent_identity(tx, s, i, current, current_epoch).await?;
    let expected: Vec<_> = (1_u16..=9)
        .filter_map(|n| {
            let kind = bigbrotr_model::DictionaryKind::try_from(n).expect("assigned bounded kind");
            i.record
                .next_manifest()
                .allocation_namespaces()
                .domain(kind)
                .map(|domain| (kind, domain))
        })
        .collect();
    initial_succession::require_prepared_target(tx, s, i.role, &expected).await
}

/// Immutable original attribution shared by strict preparation and exact empty
/// retirement. Every caller separately validates its own complete target shape.
pub(super) async fn require_intent_identity(
    tx: &Transaction<'_>,
    s: &ingest_registry::InitialRegistration,
    i: &Intent,
    current: ingest_registry::ControlOwner,
    current_epoch: PolicyEpoch,
) -> Result<(), InitialSuccessionError> {
    let h = &i.record;
    if current_epoch.get() < i.policy.policy_revision {
        return Err(OwnerError::InvalidPolicy.into());
    }
    if s.producer_slot_id != h.producer_slot_id()
        || i.policy.policy_revision != h.next_manifest().identity().policy_epoch.get()
    {
        return Err(OwnerError::InvalidMetadata.into());
    }
    ingest_registry::require_retirement_registration(tx, s)
        .await
        .map_err(InitialSuccessionError::Registry)?;
    let old = h.previous_manifest().encode();
    let target = h.next_manifest().encode();
    let next = h.next_manifest().identity();
    let valid: bool = tx.query_one(
        "SELECT dataset_id=$2::bytea AND owner_role='ingest' AND target_epoch=2 AND producer_epoch=2
             AND target_incarnation=$3::bytea AND target_role_instance_id=$4::bytea
             AND producer_slot_id=$5::bytea AND shard_id=$6::bigint
             AND previous_topology=$7::text::numeric AND target_topology=$8::text::numeric
             AND issued_through=$9::text::numeric AND last_complete_sequence=$10::text::numeric
             AND closure_digest=$11::bytea AND previous_manifest=$12::bytea AND target_manifest=$13::bytea
             AND previous_manifest_sha256=sha256($12::bytea) AND target_manifest_sha256=sha256($13::bytea)
             AND handoff=$14::bytea AND handoff_sha256=sha256($14::bytea)
             AND config_revision=$15::text::numeric AND policy_revision=$16::text::numeric
             AND config_digest=$17::bytea AND policy_digest=$18::bytea
             AND outstanding_request_limit=$19::bigint AND created_at_us=$20::bigint
             AND lease_expires_at_us=$21::bigint AND prepared_by_resource=$22::bytea
             AND prepared_by_epoch=$23::text::numeric AND prepared_by_incarnation=$24::bytea
             AND prepared_by_role='control'
         FROM control.initial_ingest_successions WHERE owner_resource_id=$1::bytea FOR SHARE",
        &[&&s.ingest_resource_id.as_bytes()[..],&&s.dataset_id.as_bytes()[..],&&next.process_incarnation.as_bytes()[..],
          &&i.role[..],&&s.producer_slot_id.as_bytes()[..],&i64::from(s.shard_id),
          &h.previous_manifest().identity().topology_generation.get().to_string(),&next.topology_generation.get().to_string(),
          &h.issued_through().to_string(),&h.last_complete_sequence().to_string(),&&h.closure_digest()[..],
          &old.as_bytes(),&target.as_bytes(),&&i.handoff[..],&i.policy.config_revision.to_string(),
          &i.policy.policy_revision.to_string(),&&i.policy.config_digest[..],&&i.policy.policy_digest[..],
          &i64::from(i.request_limit),&i.created_at_us,&i.lease_expires_at_us,&&i.prepared_resource[..],
          &i.prepared_epoch.to_string(),&&i.prepared_incarnation[..]])
        .await.map_err(backend)?.try_get(0).map_err(backend)?;
    if !valid {
        return Err(OwnerError::InvalidMetadata.into());
    }
    let attributed: bool = tx.query_one(
        "SELECT o.dataset_id=$4::bytea AND o.role='control' AND r.dataset_id=o.dataset_id AND r.role='control'
             AND octet_length(o.role_instance_id)=16 AND o.role_instance_id<>decode(repeat('00',16),'hex')
             AND o.control_fence_version=1 AND o.control_backend_pid>0 AND o.control_backend_start IS NOT NULL
             AND ((o.state='active' AND o.resource_id=$5::bytea AND o.epoch=$6::text::numeric
                    AND o.process_incarnation=$7::bytea AND o.role_instance_id=$8::bytea AND r.retired_at_us IS NULL)
                  OR (o.state='released' AND o.resource_id=$5::bytea AND o.epoch<$6::text::numeric
                    AND o.exclusion_method='storage_fenced' AND octet_length(o.exclusion_digest)=32
                    AND o.released_at_us IS NOT NULL AND r.retired_at_us IS NOT NULL))
         FROM control.owner_epochs o JOIN control.role_instances r ON r.role_instance_id=o.role_instance_id
         WHERE o.resource_id=$1::bytea AND o.epoch=$2::text::numeric AND o.process_incarnation=$3::bytea FOR SHARE OF o,r",
        &[&&i.prepared_resource[..],&i.prepared_epoch.to_string(),&&i.prepared_incarnation[..],&&s.dataset_id.as_bytes()[..],
          &&current.resource_id.as_bytes()[..],&current.epoch.get().to_string(),&&current.process_incarnation.as_bytes()[..],
          &&current.role_instance_id[..]])
        .await.map_err(backend)?.try_get(0).map_err(backend)?;
    if !attributed {
        return Err(OwnerError::InvalidMetadata.into());
    }
    let policy = tx.query_one(
        "SELECT c.schema_version,p.schema_version,left(c.document->>'format',64),left(p.document->>'format',64)
         FROM control.config_revisions c JOIN control.policy_revisions p
           ON p.dataset_id=c.dataset_id AND p.config_revision=c.revision
         WHERE c.dataset_id=$1::bytea AND c.revision=$2::text::numeric AND p.revision=$3::text::numeric
           AND c.document_digest=$4::bytea AND p.document_digest=$5::bytea",
        &[&&s.dataset_id.as_bytes()[..],&i.policy.config_revision.to_string(),&i.policy.policy_revision.to_string(),
          &&i.policy.config_digest[..],&&i.policy.policy_digest[..]]).await.map_err(backend)?;
    for (schema, format) in [(0, 2), (1, 3)] {
        crate::runtime::policy_document::PolicyDocumentVersion::from_metadata(
            policy.try_get(schema).map_err(backend)?,
            policy.try_get(format).map_err(backend)?,
        )
        .map_err(|_| OwnerError::InvalidMetadata)?;
    }
    Ok(())
}

pub(super) fn check_delivery<E>(
    lease: &mut PolicyLeaseTracker,
    epoch: PolicyEpoch,
    cancelled: &AtomicBool,
) -> Result<(), ProducerOperationError<E>> {
    if cancelled.load(Ordering::Relaxed) || lease.check_delivery(epoch).is_err() {
        Err(ProducerOperationError::DeliveryUnknown)
    } else {
        Ok(())
    }
}
pub(super) async fn finish<E>(
    tx: Transaction<'_>,
    lease: &mut PolicyLeaseTracker,
    epoch: PolicyEpoch,
    cancelled: &AtomicBool,
) -> Result<(), ProducerOperationError<E>> {
    check_delivery(lease, epoch, cancelled)?;
    tx.rollback()
        .await
        .map_err(|_| ProducerOperationError::DeliveryUnknown)?;
    check_delivery(lease, epoch, cancelled)
}
pub(super) fn history_resolve_error(error: InitialSuccessionError) -> ProducerResolveError {
    match error {
        ProducerOperationError::Control(e) => ProducerOperationError::Control(e),
        ProducerOperationError::Registry(e) => ProducerOperationError::Registry(e),
        ProducerOperationError::Native(e) => {
            ProducerOperationError::Native(AdmissionError::from(e).into())
        }
        ProducerOperationError::Reservation(e) => ProducerOperationError::Reservation(e),
        ProducerOperationError::Outcome(e) => ProducerOperationError::Outcome(e),
        ProducerOperationError::CommitUnknown => ProducerOperationError::CommitUnknown,
        ProducerOperationError::DeliveryUnknown => ProducerOperationError::DeliveryUnknown,
    }
}
