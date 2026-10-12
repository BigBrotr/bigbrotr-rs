//! Retire an exact empty preparation; never adopt its attempted incarnation.
use super::*;
use crate::{
    runtime::OwnedPermit,
    storage::rocks::store::{
        BootstrapError, EmptyInitialAttempt, EmptyInitialRetirement, IngestRecovery,
        ResolvedProducerRequest, ResolvedSource, SourceResolution,
    },
};
use bigbrotr_model::DictionaryKind;
use std::sync::atomic::Ordering;

/// Borrowed native custody and exact retained metadata, before positive replay.
pub struct PreparedEmptyInitialAttempt<'a> {
    native: EmptyInitialAttempt<'a>,
    registration: ingest_registry::InitialRegistration,
    intent: initial_history::Intent,
    permit: OwnedPermit,
}
impl<'a> PreparedEmptyInitialAttempt<'a> {
    pub fn registration(&self) -> &ingest_registry::InitialRegistration {
        &self.registration
    }
    pub fn issued_through(&self) -> u64 {
        self.native.historical().issued_through()
    }
    pub fn source_resolution(
        &self,
        permit: OwnedPermit,
    ) -> Result<SourceResolution<'a>, BootstrapError> {
        let history = self.native.historical();
        let closing = history.observe_closed_producer(permit)?;
        history.source_resolution(closing, history.issued_through())
    }
    /// End the source borrow before mutable dispatch. The plan retains the
    /// metadata grant and a positive proof bound to this directory and sequence.
    pub fn reconstruct_retirement(
        self,
        source: &ResolvedSource<'_>,
    ) -> Result<InitialEmptyRetirementPlan, BootstrapError> {
        let retirement = self.native.reconstruct_retirement(source)?;
        if retirement.handoff().as_bytes() != self.intent.handoff
            || retirement.closure_digest() != expected_digest(&self.intent)
        {
            return Err(BootstrapError::StateMismatch);
        }
        Ok(InitialEmptyRetirementPlan {
            retirement,
            registration: self.registration,
            intent: self.intent,
            _permit: self.permit,
        })
    }
}

/// Exact immutable intent and positive native proof, with no writer conversion.
pub struct InitialEmptyRetirementPlan {
    retirement: EmptyInitialRetirement,
    registration: ingest_registry::InitialRegistration,
    intent: initial_history::Intent,
    _permit: OwnedPermit,
}
impl InitialEmptyRetirementPlan {
    pub fn retirement(&self) -> &EmptyInitialRetirement {
        &self.retirement
    }
}

impl ControlSession {
    pub async fn inspect_empty_initial_attempt<'a>(
        &mut self,
        recovery: &'a IngestRecovery,
        input: InitialHistoryRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<PreparedEmptyInitialAttempt<'a>, InitialSuccessionError> {
        require_metadata_grant(&permit)?;
        let current = self.identity;
        let instance = *self.request.instance.as_bytes();
        let (tx, mut lease, epoch) = self
            .begin_empty_initial_history(recovery, &input, validate)
            .await?;
        let intent = initial_history::read_intent(&tx, recovery).await?;
        let native = recovery
            .empty_initial_attempt(&intent.handoff)
            .map_err(InitialSuccessionError::Native)?;
        let registration =
            ingest_registry::reconstruct_historical(&tx, instance, native.historical().manifest())
                .await
                .map_err(InitialSuccessionError::Registry)?;
        initial_history::require_intent_identity(&tx, &registration, &intent, current, epoch)
            .await?;
        let terminal =
            require_target(&tx, &registration, &intent, expected_digest(&intent)).await?;
        if terminal.is_some() && !native.is_closing() {
            return Err(OwnerError::InvalidMetadata.into());
        }
        initial_history::finish(tx, &mut lease, epoch, input.cancelled).await?;
        if self.client.is_closed() {
            return Err(InitialSuccessionError::DeliveryUnknown);
        }
        self.available = true;
        Ok(PreparedEmptyInitialAttempt {
            native,
            registration,
            intent,
            permit,
        })
    }

    /// Read one original request through the same positive resolver as ordinary
    /// prepared history, while validating the separate empty-retirement shape.
    pub async fn inspect_empty_initial_request(
        &mut self,
        history: &PreparedEmptyInitialAttempt<'_>,
        input: ProducerResolveRequest<'_>,
        input_permit: OwnedPermit,
        outcome_permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<ResolvedProducerRequest, ProducerResolveError> {
        let current = self.identity;
        let request = InitialHistoryRequest {
            ingest_root: input.ingest_root,
            policy_ttl: input.policy_ttl,
            cancelled: input.cancelled,
        };
        let native = history.native.historical();
        let (tx, mut lease, epoch) = self
            .begin_empty_initial_history(native.recovery(), &request, validate)
            .await?;
        initial_history::require_intent_identity(
            &tx,
            &history.registration,
            &history.intent,
            current,
            epoch,
        )
        .await
        .map_err(initial_history::history_resolve_error)?;
        let terminal = require_target(
            &tx,
            &history.registration,
            &history.intent,
            expected_digest(&history.intent),
        )
        .await
        .map_err(initial_history::history_resolve_error)?;
        if terminal.is_some() && !history.native.is_closing() {
            return Err(OwnerError::InvalidMetadata.into());
        }
        let resolved = initial_history::resolve_original(
            &tx,
            native,
            &history.registration,
            &input,
            input_permit,
            outcome_permit,
            &mut lease,
            epoch,
        )
        .await?;
        initial_history::finish(tx, &mut lease, epoch, input.cancelled).await?;
        if self.client.is_closed() {
            return Err(ProducerResolveError::DeliveryUnknown);
        }
        self.available = true;
        Ok(resolved)
    }

    /// Close native first, then atomically retire every target SQL parent. Any
    /// later error may leave native closing or a committed terminal transaction.
    /// Retain/join native and driver custody; fresh recovery rebuilds the proof.
    pub async fn retire_empty_initial_attempt(
        &mut self,
        recovery: &mut IngestRecovery,
        plan: &InitialEmptyRetirementPlan,
        input: InitialHistoryRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<i64, InitialSuccessionError> {
        require_metadata_grant(&permit)?;
        if permit.request().units().native_bytes() < 4096 {
            return Err(InitialSuccessionError::Native(
                BootstrapError::InsufficientReservation,
            ));
        }
        if input.cancelled.load(Ordering::Relaxed) {
            return Err(InitialSuccessionError::Native(
                BootstrapError::AuthorityUnavailable,
            ));
        }
        if !recovery.matches_configured_root(input.ingest_root)
            || plan.registration.dataset_id != self.request.dataset
            || plan.registration.instance_id != *self.request.instance.as_bytes()
        {
            return Err(OwnerError::InvalidInput.into());
        }
        recovery
            .validate_empty_initial_retirement(&plan.retirement)
            .map_err(InitialSuccessionError::Native)?;
        let native_closing = recovery
            .empty_initial_attempt(&plan.intent.handoff)
            .map_err(InitialSuccessionError::Native)?
            .is_closing();
        let current = self.identity;
        let (tx, mut lease, epoch) = self
            .begin_empty_initial_history(recovery, &input, validate)
            .await?;
        initial_history::require_intent_identity(
            &tx,
            &plan.registration,
            &plan.intent,
            current,
            epoch,
        )
        .await?;
        let digest = plan.retirement.closure_digest();
        let terminal = require_target(&tx, &plan.registration, &plan.intent, digest).await?;
        if terminal.is_some() && !native_closing {
            return Err(OwnerError::InvalidMetadata.into());
        }
        initial_history::check_delivery(&mut lease, epoch, input.cancelled)?;
        let _closing = recovery
            .close_empty_initial_attempt(&plan.retirement, permit, || {
                if input.cancelled.load(Ordering::Relaxed) {
                    return Err(BootstrapError::AuthorityUnavailable);
                }
                lease
                    .check_delivery(epoch)
                    .map_err(|_| BootstrapError::AuthorityUnavailable)
            })
            .map_err(InitialSuccessionError::Native)?;
        let retired_at = match terminal {
            Some(time) => time,
            None => {
                let time = retire_target(&tx, &plan.registration, &plan.intent, digest).await?;
                if require_target(&tx, &plan.registration, &plan.intent, digest).await?
                    != Some(time)
                {
                    return Err(OwnerError::InvalidMetadata.into());
                }
                time
            }
        };
        initial_history::check_delivery(&mut lease, epoch, input.cancelled)?;
        tx.commit()
            .await
            .map_err(|_| InitialSuccessionError::CommitUnknown)?;
        initial_history::check_delivery(&mut lease, epoch, input.cancelled)?;
        if self.client.is_closed() {
            return Err(InitialSuccessionError::DeliveryUnknown);
        }
        self.available = true;
        Ok(retired_at)
    }
}

fn require_metadata_grant(permit: &OwnedPermit) -> Result<(), InitialSuccessionError> {
    if permit.request().units().data_bytes() < 128 * 1024 || permit.request().units().items() == 0 {
        return Err(InitialSuccessionError::Native(
            BootstrapError::InsufficientReservation,
        ));
    }
    Ok(())
}

/// Expected checksum for read-only SQL classification, not a native witness.
/// Dispatch instead supplies the independently proven native plan's digest.
fn expected_digest(intent: &initial_history::Intent) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"bigbrotr/initial-empty-attempt-closure/1\0");
    digest.update(Sha256::digest(&intent.handoff));
    digest.update(Sha256::digest(
        intent.record.next_manifest().encode().as_bytes(),
    ));
    digest.update(intent.record.producer_slot_id().as_bytes());
    for value in [2_u64, 0, 0] {
        digest.update(value.to_be_bytes());
    }
    digest.finalize().into()
}

async fn require_target(
    tx: &Transaction<'_>,
    registration: &ingest_registry::InitialRegistration,
    intent: &initial_history::Intent,
    digest: [u8; 32],
) -> Result<Option<i64>, InitialSuccessionError> {
    let dataset = &registration.dataset_id.as_bytes().as_slice();
    let resource = &registration.ingest_resource_id.as_bytes().as_slice();
    tx.query_one(
        "SELECT epoch FROM control.owner_epochs WHERE resource_id=$1::bytea AND epoch=2 FOR UPDATE",
        &[resource],
    )
    .await
    .map_err(backend)?;
    tx.query_one("SELECT role_instance_id FROM control.role_instances WHERE role_instance_id=$1::bytea FOR UPDATE", &[&&intent.role[..]]).await.map_err(backend)?;
    let domains = tx
        .query(
            "SELECT dictionary_kind,substring(allocation_domain FROM 1 FOR 17)
         FROM control.allocation_domains WHERE dataset_id=$1::bytea
           AND owner_resource_id=$2::bytea AND owner_epoch=2
         ORDER BY dictionary_kind,allocation_domain LIMIT 10 FOR UPDATE",
            &[dataset, resource],
        )
        .await
        .map_err(backend)?;
    let expected = (1_u16..=9).filter_map(|n| {
        let kind = DictionaryKind::try_from(n).expect("assigned bounded kind");
        intent
            .record
            .next_manifest()
            .allocation_namespaces()
            .domain(kind)
            .map(|domain| (kind, domain))
    });
    let mut actual = domains.iter();
    for (kind, domain) in expected {
        let row = actual.next().ok_or(OwnerError::InvalidMetadata)?;
        if row.try_get::<_, i16>(0).map_err(backend)? != kind as i16
            || row.try_get::<_, &[u8]>(1).map_err(backend)? != domain.as_bytes()
        {
            return Err(OwnerError::InvalidMetadata.into());
        }
    }
    if actual.next().is_some() {
        return Err(OwnerError::InvalidMetadata.into());
    }
    tx.query_one("SELECT owner_epoch FROM control.source_streams WHERE dataset_id=$1::bytea AND shard_id=$2::bigint AND owner_epoch=2 FOR UPDATE", &[dataset, &i64::from(registration.shard_id)]).await.map_err(backend)?;
    tx.query_one("SELECT producer_epoch FROM control.producer_epochs WHERE producer_slot_id=$1::bytea AND producer_epoch=2 FOR UPDATE", &[&&registration.producer_slot_id.as_bytes()[..]]).await.map_err(backend)?;
    tx.query_one(
        "SELECT control.require_empty_initial_retirement($1::bytea,$2::bytea)",
        &[resource, &&digest[..]],
    )
    .await
    .map_err(backend)?
    .try_get(0)
    .map_err(|e| backend(e).into())
}

async fn retire_target(
    tx: &Transaction<'_>,
    registration: &ingest_registry::InitialRegistration,
    intent: &initial_history::Intent,
    digest: [u8; 32],
) -> Result<i64, InitialSuccessionError> {
    let dataset = &registration.dataset_id.as_bytes().as_slice();
    let resource = &registration.ingest_resource_id.as_bytes().as_slice();
    let slot = &registration.producer_slot_id.as_bytes().as_slice();
    let shard = i64::from(registration.shard_id);
    let clock = tx
        .query_one(
            "SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,created_at_us
         FROM control.initial_ingest_successions WHERE owner_resource_id=$1::bytea",
            &[resource],
        )
        .await
        .map_err(backend)?;
    let now: i64 = clock.try_get(0).map_err(backend)?;
    if now < clock.try_get::<_, i64>(1).map_err(backend)? {
        return Err(OwnerError::InvalidMetadata.into());
    }
    let source_closing = tx.execute("UPDATE control.source_streams SET state='closing' WHERE dataset_id=$1::bytea AND shard_id=$2::bigint AND owner_epoch=2 AND state='open'", &[dataset,&shard]).await.map_err(backend)?;
    let source_closed = tx.execute("UPDATE control.source_streams SET state='closed',closed_at_us=$3::bigint,last_complete_sequence=0,closure_digest=$4::bytea WHERE dataset_id=$1::bytea AND shard_id=$2::bigint AND owner_epoch=2 AND state='closing'", &[dataset,&shard,&now,&&digest[..]]).await.map_err(backend)?;
    let producer_closing = tx.execute("UPDATE control.producer_epochs SET state='closing' WHERE producer_slot_id=$1::bytea AND producer_epoch=2 AND state='open'", &[slot]).await.map_err(backend)?;
    let producer_retired = tx.execute("UPDATE control.producer_epochs SET state='retired',retired_at_us=$2::bigint,closure_digest=$3::bytea WHERE producer_slot_id=$1::bytea AND producer_epoch=2 AND state='closing'", &[slot,&now,&&digest[..]]).await.map_err(backend)?;
    let domains = tx.execute("UPDATE control.allocation_domains SET state='closed',closed_at_us=$3::bigint,closure_digest=$4::bytea WHERE dataset_id=$1::bytea AND owner_resource_id=$2::bytea AND owner_epoch=2 AND state='active'", &[dataset,resource,&now,&&digest[..]]).await.map_err(backend)?;
    let owner = tx.execute("UPDATE control.owner_epochs SET state='released',released_at_us=$2::bigint,exclusion_method='storage_fenced',exclusion_digest=$3::bytea WHERE resource_id=$1::bytea AND epoch=2 AND state='active'", &[resource,&now,&&digest[..]]).await.map_err(backend)?;
    let role = tx.execute("UPDATE control.role_instances SET retired_at_us=$2::bigint WHERE role_instance_id=$1::bytea AND retired_at_us IS NULL", &[&&intent.role[..],&now]).await.map_err(backend)?;
    let expected_domains = (1_u16..=9)
        .filter(|n| {
            intent
                .record
                .next_manifest()
                .allocation_namespaces()
                .domain(DictionaryKind::try_from(*n).expect("assigned bounded kind"))
                .is_some()
        })
        .count() as u64;
    if [
        source_closing,
        source_closed,
        producer_closing,
        producer_retired,
        owner,
        role,
    ] != [1; 6]
        || domains != expected_domains
    {
        return Err(OwnerError::InvalidMetadata.into());
    }
    Ok(now)
}
