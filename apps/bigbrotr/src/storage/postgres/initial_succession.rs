//! Prepared initial succession. Its durable intent and native witness are data,
//! never activation, an admission handle, or permission to reuse an incarnation.
use super::*;
use crate::{
    runtime::{
        OwnedPermit,
        policy_authority::{LeaseResponse, PolicyLeaseTracker, PolicyLeaseTtl},
    },
    storage::rocks::{
        manifest::AllocationNamespaces,
        store::{BootstrapError, IngestRecovery, InitialHandoff, ObservedHandoff},
    },
};
use bigbrotr_model::{AllocationDomainId, DictionaryKind, PolicyEpoch};
use std::{
    path::Path,
    sync::atomic::{AtomicBool, Ordering},
};

pub type InitialSuccessionError = ProducerOperationError<BootstrapError>;

/// Exact preparation identity. Preserve these fields after an unknown COMMIT.
/// The current Control claimant may change; the original preparing attribution
/// is retained. A replacement process cannot use this value to activate the
/// attempted target incarnation. Empty-attempt retirement is a later consumer.
pub struct InitialSuccessionRequest<'a> {
    pub ingest_root: &'a Path,
    pub previous_registration: &'a ingest_registry::InitialRegistration,
    pub handoff: &'a InitialHandoff,
    pub role_instance_id: [u8; 16],
    pub created_at_us: i64,
    pub lease_expires_at_us: i64,
    pub outstanding_request_limit: u32,
    pub policy_ttl: Duration,
    pub cancelled: &'a AtomicBool,
}

impl ControlSession {
    /// Atomically prepare PG metadata, confirm COMMIT, then apply the exact
    /// native handoff under renewed row custody and the original bounded policy
    /// deadline. This operation deliberately has no activation phase.
    ///
    /// Keep the bounded owner worker, native exclusion and connection driver
    /// retained and joined. Cancellation poisons this Control session. A lost
    /// prepare COMMIT never dispatches native work; retry requires a fresh actual
    /// Control claim and the identical request. The existing-only gap adapter
    /// reconstructs a lost plan from retained intent and positive source proof.
    /// Retirement of the unactivated attempt remains separate required work;
    /// neither absence nor observed metadata grants admission.
    pub async fn prepare_initial_successor(
        &mut self,
        recovery: &mut IngestRecovery,
        input: InitialSuccessionRequest<'_>,
        permit: OwnedPermit,
        mut validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<ObservedHandoff, InitialSuccessionError> {
        if input.cancelled.load(Ordering::Relaxed) {
            return Err(InitialSuccessionError::Native(
                BootstrapError::AuthorityUnavailable,
            ));
        }
        if permit.request().units().data_bytes() < 128 * 1024
            || permit.request().units().native_bytes() < 8192
            || permit.request().units().items() == 0
        {
            return Err(InitialSuccessionError::Native(
                BootstrapError::InsufficientReservation,
            ));
        }
        let domains = validate_input(&input)?;
        let previous = input.handoff.previous_manifest();
        let target = input.handoff.next_manifest();
        if !recovery.matches_configured_root(input.ingest_root)
            || (recovery.manifest() != previous && recovery.manifest() != target)
            || input.previous_registration.dataset_id != self.request.dataset
            || input.previous_registration.instance_id != *self.request.instance.as_bytes()
            || input.role_instance_id == self.identity.role_instance_id
            || target.identity().owner_resource_id == self.identity.resource_id
        {
            return Err(OwnerError::InvalidInput.into());
        }
        recovery
            .validate_initial_handoff(input.handoff)
            .map_err(InitialSuccessionError::Native)?;
        let mut lease = PolicyLeaseTracker::new(
            self.request.dataset,
            self.identity.process_incarnation,
            previous.identity().policy_epoch,
            PolicyLeaseTtl::new(input.policy_ttl).map_err(|_| OwnerError::InvalidInput)?,
        )
        .map_err(|_| OwnerError::Unavailable)?;
        let ticket = lease.begin_refresh().map_err(|_| OwnerError::Unavailable)?;
        self.begin_operation()?;
        let tx = application_transaction(&mut self.client, 30)
            .await
            .map_err(transaction_error)?;
        require(&tx, self.request.dataset, self.identity).await?;
        let policy = snapshot(&tx, &self.request, &mut validate).await?;
        if policy.policy_revision < target.identity().policy_epoch.get() {
            return Err(OwnerError::InvalidPolicy.into());
        }
        ingest_registry::require_retirement_registration(&tx, input.previous_registration)
            .await
            .map_err(InitialSuccessionError::Registry)?;
        let existing = observe_intent(&tx, &input).await?;
        if !existing {
            if recovery.manifest() != previous
                || target.identity().policy_epoch.get() != policy.policy_revision
            {
                return Err(OwnerError::InvalidMetadata.into());
            }
            insert_registry(&tx, &input, &domains).await?;
            insert_intent(&tx, &input, policy, self.identity).await?;
        }
        require_prepared_target(
            &tx,
            input.previous_registration,
            input.role_instance_id,
            &domains,
        )
        .await?;
        let current_epoch = PolicyEpoch::new(policy.policy_revision);
        let remaining: i64 = tx.query_one(
            "SELECT lease_expires_at_us - (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint
             FROM control.owner_epochs WHERE resource_id=$1::bytea AND epoch=$2::text::numeric",
            &[&&self.identity.resource_id.as_bytes()[..], &self.identity.epoch.get().to_string()],
        ).await.map_err(backend)?.try_get(0).map_err(backend)?;
        let remaining = u64::try_from(remaining)
            .ok()
            .filter(|n| *n > 0)
            .ok_or(OwnerError::Unavailable)?;
        lease
            .complete_refresh(
                &ticket,
                LeaseResponse {
                    dataset: self.request.dataset,
                    process_incarnation: self.identity.process_incarnation,
                    policy_epoch: current_epoch,
                    ttl: input.policy_ttl.min(Duration::from_micros(remaining)),
                },
            )
            .map_err(|_| OwnerError::Unavailable)?;
        check_delivery(&mut lease, current_epoch, input.cancelled)?;
        tx.commit()
            .await
            .map_err(|_| InitialSuccessionError::CommitUnknown)?;
        check_delivery(&mut lease, current_epoch, input.cancelled)?;

        // Reacquire custody after the confirmed prepare COMMIT. A conflicting
        // mutation cannot race native dispatch, and this does not extend the
        // policy deadline that began before the first coordination operation.
        let tx = application_transaction(&mut self.client, 30)
            .await
            .map_err(transaction_error)?;
        require(&tx, self.request.dataset, self.identity).await?;
        if snapshot(&tx, &self.request, &mut validate).await? != policy {
            return Err(OwnerError::InvalidPolicy.into());
        }
        ingest_registry::require_retirement_registration(&tx, input.previous_registration)
            .await
            .map_err(InitialSuccessionError::Registry)?;
        require_prepared_target(
            &tx,
            input.previous_registration,
            input.role_instance_id,
            &domains,
        )
        .await?;
        check_delivery(&mut lease, current_epoch, input.cancelled)?;
        let observed = recovery
            .apply_initial_handoff(input.handoff, permit, || {
                if input.cancelled.load(Ordering::Relaxed) {
                    return Err(BootstrapError::AuthorityUnavailable);
                }
                lease
                    .check_delivery(current_epoch)
                    .map_err(|_| BootstrapError::AuthorityUnavailable)
            })
            .map_err(InitialSuccessionError::Native)?;
        tx.rollback()
            .await
            .map_err(|_| InitialSuccessionError::DeliveryUnknown)?;
        check_delivery(&mut lease, current_epoch, input.cancelled)?;
        if self.client.is_closed() {
            return Err(InitialSuccessionError::DeliveryUnknown);
        }
        self.available = true;
        Ok(observed)
    }
}

fn validate_input(
    input: &InitialSuccessionRequest<'_>,
) -> Result<Vec<(DictionaryKind, AllocationDomainId)>, InitialSuccessionError> {
    let s = input.previous_registration;
    let previous = input.handoff.previous_manifest();
    let old = previous.identity();
    let target = input.handoff.next_manifest();
    let next = target.identity();
    let old_domains = AllocationNamespaces::from_entries(&s.allocation_domains)
        .map_err(|_| OwnerError::InvalidInput)?;
    if old.dataset_id != s.dataset_id
        || old.owner_resource_id != s.ingest_resource_id
        || old.shard_id != s.shard_id
        || old.owner_epoch.get() != 1
        || old.process_incarnation != s.process_incarnation
        || old.topology_generation
            != s.target_topology()
                .map_err(InitialSuccessionError::Registry)?
        || old.policy_epoch.get() != s.policy_revision
        || previous.allocation_namespaces() != &old_domains
        || input.handoff.producer_slot_id() != s.producer_slot_id
        || input.handoff.producer_epoch().get() != 1
        || input.handoff.issued_through() > 64
        || next.owner_epoch.get() != 2
        || target.key_schema() != 2
        || input.role_instance_id == [0; 16]
        || input.role_instance_id == s.ingest_role_instance_id
        || input.created_at_us < s.created_at_us
        || input.lease_expires_at_us <= input.created_at_us
        || !(1..=64).contains(&input.outstanding_request_limit)
    {
        return Err(OwnerError::InvalidInput.into());
    }
    let mut domains = Vec::with_capacity(9);
    for number in 1_u16..=9 {
        let kind = DictionaryKind::try_from(number).map_err(|_| OwnerError::InvalidInput)?;
        if let Some(domain) = target.allocation_namespaces().domain(kind) {
            if domains.iter().any(|(_, previous)| *previous == domain)
                || s.allocation_domains
                    .iter()
                    .any(|(_, previous)| *previous == domain)
            {
                return Err(OwnerError::InvalidInput.into());
            }
            domains.push((kind, domain));
        }
    }
    Ok(domains)
}

fn check_delivery(
    lease: &mut PolicyLeaseTracker,
    epoch: PolicyEpoch,
    cancelled: &AtomicBool,
) -> Result<(), InitialSuccessionError> {
    if cancelled.load(Ordering::Relaxed) || lease.check_delivery(epoch).is_err() {
        Err(InitialSuccessionError::DeliveryUnknown)
    } else {
        Ok(())
    }
}

async fn observe_intent(
    tx: &Transaction<'_>,
    input: &InitialSuccessionRequest<'_>,
) -> Result<bool, InitialSuccessionError> {
    let old = input.handoff.previous_manifest().encode();
    let target = input.handoff.next_manifest().encode();
    let handoff = input.handoff.encode();
    let row = tx.query_opt(
        "SELECT dataset_id=$2::bytea AND previous_manifest=$3::bytea AND target_manifest=$4::bytea
                AND handoff=$5::bytea AND target_role_instance_id=$6::bytea
                AND created_at_us=$7::bigint AND lease_expires_at_us=$8::bigint
                AND outstanding_request_limit=$9::bigint AND producer_slot_id=$10::bytea
                AND target_epoch=2 AND producer_epoch=2 AND target_incarnation=$11::bytea
                AND shard_id=$12::bigint AND previous_topology=$13::text::numeric
                AND target_topology=$14::text::numeric AND issued_through=$15::text::numeric
                AND last_complete_sequence=$16::text::numeric AND closure_digest=$17::bytea
                AND policy_revision=$18::text::numeric
         FROM control.initial_ingest_successions WHERE owner_resource_id=$1::bytea FOR SHARE",
        &[&&input.previous_registration.ingest_resource_id.as_bytes()[..],
          &&input.previous_registration.dataset_id.as_bytes()[..], &old.as_bytes(), &target.as_bytes(),
          &handoff.as_bytes(), &&input.role_instance_id[..], &input.created_at_us,
          &input.lease_expires_at_us, &i64::from(input.outstanding_request_limit),
          &&input.handoff.producer_slot_id().as_bytes()[..],
          &&input.handoff.next_manifest().identity().process_incarnation.as_bytes()[..],
          &i64::from(input.previous_registration.shard_id),
          &input.handoff.previous_manifest().identity().topology_generation.get().to_string(),
          &input.handoff.next_manifest().identity().topology_generation.get().to_string(),
          &input.handoff.issued_through().to_string(),
          &input.handoff.last_complete_sequence().to_string(),
          &&input.handoff.closure_digest()[..],
          &input.handoff.next_manifest().identity().policy_epoch.get().to_string()],
    ).await.map_err(backend)?;
    match row {
        None => Ok(false),
        Some(row) if row.try_get::<_, bool>(0).map_err(backend)? => Ok(true),
        Some(_) => Err(OwnerError::InvalidMetadata.into()),
    }
}

pub(super) async fn require_prepared_target(
    tx: &Transaction<'_>,
    s: &ingest_registry::InitialRegistration,
    role_instance_id: [u8; 16],
    expected_domains: &[(DictionaryKind, AllocationDomainId)],
) -> Result<(), InitialSuccessionError> {
    let dataset = &s.dataset_id.as_bytes().as_slice();
    let resource = &s.ingest_resource_id.as_bytes().as_slice();
    // Same parent-before-dependent order as retirement/renewal. Existing rows
    // cannot disappear under their immutable history guards.
    tx.query_one(
        "SELECT epoch FROM control.owner_epochs WHERE resource_id=$1::bytea AND epoch=2 FOR SHARE",
        &[resource],
    )
    .await
    .map_err(backend)?;
    tx.query_one("SELECT role_instance_id FROM control.role_instances WHERE role_instance_id=$1::bytea FOR SHARE", &[&&role_instance_id[..]]).await.map_err(backend)?;
    let domains = tx
        .query(
            "SELECT dictionary_kind,allocation_domain FROM control.allocation_domains
        WHERE dataset_id=$1::bytea AND owner_resource_id=$2::bytea AND owner_epoch=2
        ORDER BY dictionary_kind,allocation_domain LIMIT 10 FOR SHARE",
            &[dataset, resource],
        )
        .await
        .map_err(backend)?;
    if domains.len() != expected_domains.len() {
        return Err(OwnerError::InvalidMetadata.into());
    }
    for (row, (kind, domain)) in domains.iter().zip(expected_domains) {
        if row.try_get::<_, i16>(0).map_err(backend)? != *kind as i16
            || row.try_get::<_, &[u8]>(1).map_err(backend)? != domain.as_bytes()
        {
            return Err(OwnerError::InvalidMetadata.into());
        }
    }
    tx.query_one("SELECT owner_epoch FROM control.source_streams WHERE dataset_id=$1::bytea AND shard_id=$2::bigint AND owner_epoch=2 FOR SHARE", &[dataset,&i64::from(s.shard_id)]).await.map_err(backend)?;
    tx.query_one("SELECT producer_epoch FROM control.producer_epochs WHERE producer_slot_id=$1::bytea AND producer_epoch=2 FOR UPDATE", &[&&s.producer_slot_id.as_bytes()[..]]).await.map_err(backend)?;
    tx.query_one(
        "SELECT control.require_prepared_initial_successor($1::bytea)",
        &[resource],
    )
    .await
    .map_err(backend)?;
    Ok(())
}

async fn insert_registry(
    tx: &Transaction<'_>,
    input: &InitialSuccessionRequest<'_>,
    domains: &[(DictionaryKind, AllocationDomainId)],
) -> Result<(), InitialSuccessionError> {
    let s = input.previous_registration;
    let next = input.handoff.next_manifest().identity();
    let dataset = &s.dataset_id.as_bytes().as_slice();
    let resource = &s.ingest_resource_id.as_bytes().as_slice();
    let role = &input.role_instance_id.as_slice();
    let incarnation = &next.process_incarnation.as_bytes().as_slice();
    let previous = input
        .handoff
        .previous_manifest()
        .identity()
        .topology_generation
        .get()
        .to_string();
    let target = next.topology_generation.get().to_string();
    tx.execute("INSERT INTO control.role_instances(role_instance_id,dataset_id,role,created_at_us) VALUES($1::bytea,$2::bytea,'ingest',$3::bigint)", &[role,dataset,&input.created_at_us]).await.map_err(backend)?;
    tx.execute("INSERT INTO control.owner_epochs(resource_id,epoch,dataset_id,role,role_instance_id,process_incarnation,acquired_at_us,lease_expires_at_us,state) VALUES($1::bytea,2,$2::bytea,'ingest',$3::bytea,$4::bytea,$5::bigint,$6::bigint,'active')", &[resource,dataset,role,incarnation,&input.created_at_us,&input.lease_expires_at_us]).await.map_err(backend)?;
    tx.execute("INSERT INTO control.topology_generations(dataset_id,generation,previous_generation,created_at_us) VALUES($1::bytea,$2::text::numeric,$3::text::numeric,$4::bigint)", &[dataset,&target,&previous,&input.created_at_us]).await.map_err(backend)?;
    tx.execute("INSERT INTO control.source_streams(dataset_id,shard_id,owner_epoch,owner_resource_id,owner_incarnation,opened_at_us) VALUES($1::bytea,$2::bigint,2,$3::bytea,$4::bytea,$5::bigint)", &[dataset,&i64::from(s.shard_id),resource,incarnation,&input.created_at_us]).await.map_err(backend)?;
    tx.execute("INSERT INTO control.topology_streams(dataset_id,topology_generation,shard_id,owner_epoch,handoff_last_complete_sequence) VALUES($1::bytea,$2::text::numeric,$3::bigint,1,$4::text::numeric),($1::bytea,$2::text::numeric,$3::bigint,2,NULL)", &[dataset,&target,&i64::from(s.shard_id),&input.handoff.last_complete_sequence().to_string()]).await.map_err(backend)?;
    for &(kind, domain) in domains {
        tx.execute("INSERT INTO control.allocation_domains(dataset_id,dictionary_kind,allocation_domain,owner_resource_id,owner_epoch,owner_incarnation,created_at_us) VALUES($1::bytea,$2::smallint,$3::bytea,$4::bytea,2,$5::bytea,$6::bigint)", &[dataset,&(kind as i16),&domain.as_bytes().as_slice(),resource,incarnation,&input.created_at_us]).await.map_err(backend)?;
    }
    tx.execute("INSERT INTO control.producer_epochs(producer_slot_id,producer_epoch,dataset_id,producer_role_instance_id,producer_role,producer_incarnation,owner_resource_id,owner_epoch,owner_incarnation,outstanding_request_limit,opened_at_us) VALUES($1::bytea,2,$2::bytea,$3::bytea,'ingest',$4::bytea,$5::bytea,2,$4::bytea,$6::bigint,$7::bigint)", &[&&s.producer_slot_id.as_bytes()[..],dataset,role,incarnation,resource,&i64::from(input.outstanding_request_limit),&input.created_at_us]).await.map_err(backend)?;
    tx.execute("UPDATE control.topology_generations SET sealed=true WHERE dataset_id=$1::bytea AND generation=$2::text::numeric", &[dataset,&target]).await.map_err(backend)?;
    let changed = tx.execute("UPDATE control.datasets SET topology_generation=$2::text::numeric WHERE dataset_id=$1::bytea AND topology_generation=$3::text::numeric", &[dataset,&target,&previous]).await.map_err(backend)?;
    if changed != 1 {
        return Err(OwnerError::InvalidMetadata.into());
    }
    Ok(())
}

async fn insert_intent(
    tx: &Transaction<'_>,
    input: &InitialSuccessionRequest<'_>,
    policy: PolicySnapshot,
    issuer: ingest_registry::ControlOwner,
) -> Result<(), InitialSuccessionError> {
    let s = input.previous_registration;
    let previous = input.handoff.previous_manifest().encode();
    let target = input.handoff.next_manifest().encode();
    let next = input.handoff.next_manifest().identity();
    let handoff = input.handoff.encode();
    tx.execute("INSERT INTO control.initial_ingest_successions(
        dataset_id,owner_resource_id,target_epoch,target_incarnation,target_role_instance_id,
        producer_slot_id,producer_epoch,shard_id,previous_topology,target_topology,issued_through,
        last_complete_sequence,closure_digest,previous_manifest,target_manifest,
        previous_manifest_sha256,target_manifest_sha256,handoff,handoff_sha256,
        config_revision,policy_revision,config_digest,policy_digest,outstanding_request_limit,
        created_at_us,lease_expires_at_us,prepared_by_resource,prepared_by_epoch,prepared_by_incarnation)
        VALUES($1::bytea,$2::bytea,2,$3::bytea,$4::bytea,$5::bytea,2,$6::bigint,$7::text::numeric,
            $8::text::numeric,$9::text::numeric,$10::text::numeric,$11::bytea,$12::bytea,$13::bytea,
            sha256($12::bytea),sha256($13::bytea),$14::bytea,sha256($14::bytea),
            $15::text::numeric,$16::text::numeric,$17::bytea,$18::bytea,$19::bigint,
            $20::bigint,$21::bigint,$22::bytea,$23::text::numeric,$24::bytea)",
        &[&&s.dataset_id.as_bytes()[..],&&s.ingest_resource_id.as_bytes()[..],
          &&next.process_incarnation.as_bytes()[..],&&input.role_instance_id[..],
          &&s.producer_slot_id.as_bytes()[..],&i64::from(s.shard_id),
          &input.handoff.previous_manifest().identity().topology_generation.get().to_string(),
          &next.topology_generation.get().to_string(),&input.handoff.issued_through().to_string(),
          &input.handoff.last_complete_sequence().to_string(),&&input.handoff.closure_digest()[..],
          &previous.as_bytes(),&target.as_bytes(),&handoff.as_bytes(),
          &policy.config_revision.to_string(),&policy.policy_revision.to_string(),
          &&policy.config_digest[..],&&policy.policy_digest[..],&i64::from(input.outstanding_request_limit),
          &input.created_at_us,&input.lease_expires_at_us,&&issuer.resource_id.as_bytes()[..],
          &issuer.epoch.get().to_string(),&&issuer.process_incarnation.as_bytes()[..]],
    ).await.map_err(backend)?;
    Ok(())
}
