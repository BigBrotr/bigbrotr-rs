//! Reconstruct attribution from retained history, never from current heads.
use super::*;
use crate::storage::postgres::control_owner::OwnerError;
use crate::storage::rocks::{manifest::IngestManifest, store::IngestRecovery};

pub const MAX_HISTORICAL_DOCUMENT_BYTES: usize = 10 * 1024 * 1024;

/// Read the original initial registration while the caller retains exclusive
/// native custody. The result is metadata, not current authority. Connected
/// recovery must revalidate it before effects; fresh admission is not permitted.
/// The caller owns the complete original-document/restore budget, metadata
/// budget, overall deadline and connection driver. The validator must restore
/// both complete accepted original bodies and recompute their digests, without
/// replacing captured current-policy values or issuing grants from old policy.
/// After cancellation discard the connection and join its driver before retry.
pub async fn load_historical(
    client: &mut Client,
    instance_id: [u8; 16],
    recovery: &IngestRecovery,
    max_document_bytes: usize,
    mut validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
) -> Result<InitialRegistration, RegistryError> {
    // Validate before the first transaction/query. This is a transport bound,
    // not a policy value or permission to omit any accepted setting.
    checked_document_limit(max_document_bytes)?;
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
        let spec = reconstruct_bounded(
            &tx,
            instance_id,
            manifest,
            max_document_bytes,
            &mut validate,
        )
        .await?;
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

pub(in crate::storage::postgres) async fn reconstruct(
    tx: &Transaction<'_>,
    instance_id: [u8; 16],
    manifest: &IngestManifest,
    validate: &mut impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
) -> Result<InitialRegistration, RegistryError> {
    reconstruct_bounded(
        tx,
        instance_id,
        manifest,
        MAX_HISTORICAL_DOCUMENT_BYTES,
        validate,
    )
    .await
}

async fn reconstruct_bounded(
    tx: &Transaction<'_>,
    instance_id: [u8; 16],
    manifest: &IngestManifest,
    max_document_bytes: usize,
    validate: &mut impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
) -> Result<InitialRegistration, RegistryError> {
    let document_limit = checked_document_limit(max_document_bytes)?;
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
    // Read bounded identity fields and both complete original policy bodies.
    // Digest metadata alone cannot detect a changed body with a stale digest.
    // The caller's existing policy-document grant remains owned across this
    // restoration, sequentially after its current-policy snapshot validation.
    // Other historical fields are immutable;
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
               substring(o.role_instance_id FROM 1 FOR 17) AS control_role,
               left(jsonb_build_object(
                   'ingest.pending_max_bytes',p.document->'settings'->'ingest.pending_max_bytes',
                   'ingest.pending_high_fraction',p.document->'settings'->'ingest.pending_high_fraction',
                   'ingest.pending_low_fraction',p.document->'settings'->'ingest.pending_low_fraction',
                   'ingest.group_commit_max_bytes',p.document->'settings'->'ingest.group_commit_max_bytes',
                   'runtime.disk_resume_margin_bytes',p.document->'settings'->'runtime.disk_resume_margin_bytes',
                   'runtime.pressure_stable_duration',p.document->'settings'->'runtime.pressure_stable_duration',
                   'ingest.request_receipt_retention',p.document->'settings'->'ingest.request_receipt_retention'
               )::text,2049) AS intake_policy,
               c.document#>'{settings,ingest.pending_max_bytes}'=p.document#>'{settings,ingest.pending_max_bytes}'
               AND c.document#>'{settings,ingest.pending_high_fraction}'=p.document#>'{settings,ingest.pending_high_fraction}'
               AND c.document#>'{settings,ingest.pending_low_fraction}'=p.document#>'{settings,ingest.pending_low_fraction}'
               AND c.document#>'{settings,ingest.group_commit_max_bytes}'=p.document#>'{settings,ingest.group_commit_max_bytes}'
               AND c.document#>'{settings,runtime.disk_resume_margin_bytes}'=p.document#>'{settings,runtime.disk_resume_margin_bytes}'
               AND c.document#>'{settings,runtime.pressure_stable_duration}'=p.document#>'{settings,runtime.pressure_stable_duration}'
               AND c.document#>'{settings,ingest.request_receipt_retention}'=p.document#>'{settings,ingest.request_receipt_retention}' AS intake_matches,
               c.schema_version AS config_schema,p.schema_version AS policy_schema,
               CASE WHEN octet_length(c.document::text)<=$5::integer
                    THEN c.document::text END AS config_document,
               CASE WHEN octet_length(p.document::text)<=$5::integer
                    THEN p.document::text END AS policy_document
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
                &document_limit,
            ],
        )
        .await
        .map_err(backend)?;
    let [row] = rows.as_slice() else {
        return Err(RegistryError::InvalidMetadata);
    };
    let config_digest = fixed_bytes(row.try_get("config_digest").map_err(backend)?)?;
    let policy_digest = fixed_bytes(row.try_get("policy_digest").map_err(backend)?)?;
    for (document, schema, digest) in [
        ("config_document", "config_schema", config_digest),
        ("policy_document", "policy_schema", policy_digest),
    ] {
        let document = row
            .try_get::<_, &str>(document)
            .map_err(|_| RegistryError::InvalidMetadata)?;
        crate::runtime::policy_document::PolicyDocumentVersion::require_metadata(
            document,
            row.try_get(schema).map_err(backend)?,
        )
        .map_err(|_| RegistryError::InvalidMetadata)?;
        validate(document, digest).map_err(|_| RegistryError::InvalidMetadata)?;
    }
    if row
        .try_get::<_, Option<bool>>("intake_matches")
        .map_err(backend)?
        != Some(true)
    {
        return Err(RegistryError::InvalidMetadata);
    }
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
        config_digest: Digest256::from_bytes(config_digest),
        policy_digest: Digest256::from_bytes(policy_digest),
        previous_topology,
        allocation_domains,
        producer_slot_id: identity(row, "producer_slot")?
            .try_into()
            .map_err(|_| RegistryError::InvalidMetadata)?,
        outstanding_request_limit: u32::try_from(
            row.try_get::<_, i64>("request_limit").map_err(backend)?,
        )
        .map_err(|_| RegistryError::InvalidMetadata)?,
        intake_policy: {
            let projection = row.try_get::<_, &str>("intake_policy").map_err(backend)?;
            if projection.len() > 2048 {
                return Err(RegistryError::InvalidMetadata);
            }
            let settings =
                serde_json::from_str(projection).map_err(|_| RegistryError::InvalidMetadata)?;
            CapturedIntakePolicy::capture(&settings).map_err(|_| RegistryError::InvalidMetadata)?
        },
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

fn checked_document_limit(bytes: usize) -> Result<i32, RegistryError> {
    if !(1..=MAX_HISTORICAL_DOCUMENT_BYTES).contains(&bytes) {
        return Err(RegistryError::InvalidInput);
    }
    i32::try_from(bytes).map_err(|_| RegistryError::InvalidInput)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn historical_document_limit_has_a_checked_closed_range() {
        for limit in [1, 16 * 1024, MAX_HISTORICAL_DOCUMENT_BYTES] {
            assert_eq!(checked_document_limit(limit).unwrap() as usize, limit);
        }
        for limit in [0, MAX_HISTORICAL_DOCUMENT_BYTES + 1, usize::MAX] {
            assert_eq!(
                checked_document_limit(limit),
                Err(RegistryError::InvalidInput)
            );
        }
    }
}
