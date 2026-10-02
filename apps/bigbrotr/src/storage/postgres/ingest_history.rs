//! Reconstruct attribution from retained history, never from current heads.
use super::*;
use crate::storage::rocks::{manifest::IngestManifest, store::IngestRecovery};

/// Read the original initial registration while the caller retains exclusive
/// native custody. The result is metadata, not current authority. Connected
/// recovery must revalidate it before effects; fresh admission is not permitted.
/// The caller owns the metadata budget, overall deadline and connection driver.
/// After cancellation discard the connection and join its driver before retry.
pub async fn load_historical(
    client: &mut Client,
    instance_id: [u8; 16],
    recovery: &IngestRecovery,
) -> Result<InitialRegistration, RegistryError> {
    let manifest = recovery.manifest();
    if instance_id == [0; 16]
        || manifest.identity().owner_epoch.get() != 1
        || manifest.identity().policy_epoch.get() == 0
    {
        return Err(RegistryError::InvalidInput);
    }
    let tx = transaction(client).await?;
    let result = async {
        validate_backend(&tx).await?;
        let spec = reconstruct(&tx, instance_id, manifest).await?;
        spec.validate()
            .map_err(|_| RegistryError::InvalidMetadata)?;
        if !matches!(
            observe(&tx, &spec).await?.observation,
            RegistrationObservation::Matching { .. }
        ) {
            return Err(RegistryError::InvalidMetadata);
        }
        Ok(spec)
    }
    .await;
    // There is no durable effect or ambiguous COMMIT in this read. Confirm the
    // transaction ended before returning either metadata or a normal refusal.
    tx.rollback().await.map_err(backend)?;
    result
}

async fn reconstruct(
    tx: &Transaction<'_>,
    instance_id: [u8; 16],
    manifest: &IngestManifest,
) -> Result<InitialRegistration, RegistryError> {
    let native = manifest.identity();
    let dataset = &native.dataset_id.as_bytes().as_slice();
    let deployment = tx
        .query_opt(
            "SELECT substring(instance_id FROM 1 FOR 17) FROM control.datasets
              WHERE dataset_id = $1::bytea FOR SHARE",
            &[dataset],
        )
        .await
        .map_err(backend)?
        .ok_or(RegistryError::WrongDeployment)?;
    if nonzero_id(deployment.try_get(0).map_err(backend)?)? != instance_id {
        return Err(RegistryError::WrongDeployment);
    }
    // Read only bounded identity fields. Other historical fields are immutable;
    // observe subsequently takes the established dataset/Control/Ingest/producer
    // lock order and checks the entire relation closure. Renewal may have changed
    // the retained expiry; it remains observed scheduling data, never a lease.
    const HISTORY: &str = "
        SELECT substring(i.role_instance_id FROM 1 FOR 17) AS ingest_role,
               i.acquired_at_us, i.lease_expires_at_us,
               left(c.revision::text,21) AS config_revision,
               substring(c.document_digest FROM 1 FOR 33) AS config_digest,
               substring(p.document_digest FROM 1 FOR 33) AS policy_digest,
               left(t.previous_generation::text,21) AS previous_topology,
               substring(e.producer_slot_id FROM 1 FOR 17) AS producer_slot,
               e.outstanding_request_limit::bigint AS request_limit,
               substring(s.registered_by_resource_id FROM 1 FOR 17) AS control_resource,
               left(s.registered_by_epoch::text,21) AS control_epoch,
               substring(s.registered_by_incarnation FROM 1 FOR 17) AS control_incarnation,
               substring(o.role_instance_id FROM 1 FOR 17) AS control_role
          FROM control.owner_epochs i
          JOIN control.producer_epochs e
            ON e.dataset_id = i.dataset_id AND e.owner_resource_id = i.resource_id
           AND e.owner_epoch = i.epoch
          JOIN control.producer_slots s ON s.producer_slot_id = e.producer_slot_id
          JOIN control.owner_epochs o ON o.resource_id = s.registered_by_resource_id
           AND o.epoch = s.registered_by_epoch
          JOIN control.policy_revisions p ON p.dataset_id = i.dataset_id
           AND p.revision = $3::text::numeric
          JOIN control.config_revisions c ON c.dataset_id = p.dataset_id
           AND c.revision = p.config_revision
          JOIN control.topology_generations t ON t.dataset_id = i.dataset_id
           AND t.generation = $4::text::numeric
         WHERE i.dataset_id = $1::bytea AND i.resource_id = $2::bytea AND i.epoch = 1
         LIMIT 2";
    let rows = tx
        .query(
            HISTORY,
            &[
                dataset,
                &&native.owner_resource_id.as_bytes()[..],
                &native.policy_epoch.get().to_string(),
                &native.topology_generation.get().to_string(),
            ],
        )
        .await
        .map_err(backend)?;
    let [row] = rows.as_slice() else {
        return Err(RegistryError::InvalidMetadata);
    };
    let previous_topology = row
        .try_get::<_, Option<&str>>("previous_topology")
        .map_err(backend)?
        .map(decimal)
        .transpose()?
        .map(TopologyGeneration::new);
    let mut allocation_domains = Vec::with_capacity(MAX_DOMAINS);
    for value in 1..=MAX_DOMAINS as u16 {
        let kind = DictionaryKind::try_from(value).expect("closed dictionary kind range");
        if let Some(domain) = manifest.allocation_namespaces().domain(kind) {
            allocation_domains.push((kind, domain));
        }
    }
    Ok(InitialRegistration {
        dataset_id: native.dataset_id,
        instance_id,
        control: ControlOwner {
            resource_id: identity(row, "control_resource")?
                .try_into()
                .map_err(|_| RegistryError::InvalidMetadata)?,
            epoch: OwnerEpoch::new(number(row, "control_epoch")?)
                .map_err(|_| RegistryError::InvalidMetadata)?,
            role_instance_id: identity(row, "control_role")?,
            process_incarnation: identity(row, "control_incarnation")?
                .try_into()
                .map_err(|_| RegistryError::InvalidMetadata)?,
        },
        ingest_resource_id: native.owner_resource_id,
        ingest_role_instance_id: identity(row, "ingest_role")?,
        process_incarnation: native.process_incarnation,
        shard_id: native.shard_id,
        config_revision: number(row, "config_revision")?,
        policy_revision: native.policy_epoch.get(),
        config_digest: Digest256::from_bytes(fixed_bytes(
            row.try_get("config_digest").map_err(backend)?,
        )?),
        policy_digest: Digest256::from_bytes(fixed_bytes(
            row.try_get("policy_digest").map_err(backend)?,
        )?),
        previous_topology,
        allocation_domains,
        producer_slot_id: identity(row, "producer_slot")?
            .try_into()
            .map_err(|_| RegistryError::InvalidMetadata)?,
        outstanding_request_limit: u32::try_from(
            row.try_get::<_, i64>("request_limit").map_err(backend)?,
        )
        .map_err(|_| RegistryError::InvalidMetadata)?,
        created_at_us: row.try_get("acquired_at_us").map_err(backend)?,
        lease_expires_at_us: row.try_get("lease_expires_at_us").map_err(backend)?,
    })
}

fn identity(row: &Row, column: &str) -> Result<[u8; 16], RegistryError> {
    nonzero_id(row.try_get(column).map_err(backend)?)
}
fn number(row: &Row, column: &str) -> Result<u64, RegistryError> {
    decimal(row.try_get(column).map_err(backend)?)
}
