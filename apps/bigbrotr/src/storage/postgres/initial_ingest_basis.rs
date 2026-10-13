//! Bounded construction data for explicit initial creation or live succession.
use super::*;
use bigbrotr_model::TopologyGeneration;

/// Observed data, never permission to open, register or write. Registration and
/// every native operation independently recheck the complete selected identity.
pub struct InitialIngestBasis {
    pub policy: PolicySnapshot,
    pub previous_topology: Option<TopologyGeneration>,
    pub created_at_us: i64,
    pub lease_expires_at_us: i64,
}

/// Current construction data for a new local candidate after initial retirement.
/// This observation cannot adopt a prepared attempt or authorize native writes.
pub struct InitialSuccessorBasis {
    pub policy: PolicySnapshot,
    pub created_at_us: i64,
    pub lease_expires_at_us: i64,
}

impl ControlSession {
    /// Read the real accepted documents, empty topology and server clock while
    /// this Control owner remains fenced. No owner or directory is created here.
    /// The caller retains its whole-operation deadline, document budget and
    /// actual driver through cancellation; a cancelled session cannot be reused.
    pub async fn initial_ingest_basis(
        &mut self,
        mut validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<InitialIngestBasis, OwnerError> {
        self.begin_operation()?;
        let tx = application_transaction(&mut self.client, 14)
            .await
            .map_err(transaction_error)?;
        require(&tx, self.request.dataset, self.identity).await?;
        let policy = snapshot(&tx, &self.request, &mut validate).await?;
        const BASIS: &str = "
            SELECT left(d.topology_generation::text, 21),
                   d.topology_generation IS NULL OR t.sealed,
                   NOT EXISTS (SELECT 1 FROM control.source_streams s
                                WHERE s.dataset_id = d.dataset_id),
                   (EXTRACT(EPOCH FROM pg_catalog.clock_timestamp()) * 1000000)::bigint,
                   o.lease_expires_at_us
              FROM control.datasets d
              JOIN control.owner_epochs o ON o.resource_id = $2::bytea
                   AND o.epoch = $3::text::numeric
              LEFT JOIN control.topology_generations t ON t.dataset_id = d.dataset_id
                   AND t.generation = d.topology_generation
             WHERE d.dataset_id = $1::bytea";
        let row = tx
            .query_one(
                BASIS,
                &[
                    &&self.request.dataset.as_bytes()[..],
                    &&self.identity.resource_id.as_bytes()[..],
                    &self.identity.epoch.get().to_string(),
                ],
            )
            .await
            .map_err(backend)?;
        if !row
            .try_get::<_, bool>(1)
            .map_err(|_| OwnerError::InvalidMetadata)?
            || !row
                .try_get::<_, bool>(2)
                .map_err(|_| OwnerError::InvalidMetadata)?
        {
            return Err(OwnerError::IncompatibleDeployment);
        }
        let previous_topology = row
            .try_get::<_, Option<&str>>(0)
            .map_err(|_| OwnerError::InvalidMetadata)?
            .map(|value| {
                if value.is_empty()
                    || value.len() > 20
                    || (value.len() > 1 && value.starts_with('0'))
                    || !value.bytes().all(|byte| byte.is_ascii_digit())
                {
                    return Err(OwnerError::InvalidMetadata);
                }
                value
                    .parse()
                    .map(TopologyGeneration::new)
                    .map_err(|_| OwnerError::InvalidMetadata)
            })
            .transpose()?;
        let created_at_us: i64 = row.try_get(3).map_err(backend)?;
        let lease_expires_at_us: i64 = row.try_get(4).map_err(backend)?;
        if lease_expires_at_us <= created_at_us {
            return Err(OwnerError::Unavailable);
        }
        tx.rollback().await.map_err(backend)?;
        if self.client.is_closed() {
            return Err(OwnerError::Unavailable);
        }
        self.available = true;
        Ok(InitialIngestBasis {
            policy,
            previous_topology,
            created_at_us,
            lease_expires_at_us,
        })
    }

    /// Observe the fully retired initial predecessor under actual current
    /// Control custody. The caller separately owns and positively replays native
    /// history; preparation and activation repeat their complete authority checks.
    /// Keep the whole-operation deadline, document grant and driver until joined.
    pub async fn initial_successor_basis(
        &mut self,
        previous: &ingest_registry::InitialRegistration,
        mut validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<InitialSuccessorBasis, OwnerError> {
        if previous.dataset_id != self.request.dataset
            || previous.instance_id != *self.request.instance.as_bytes()
        {
            return Err(OwnerError::InvalidInput);
        }
        let topology = previous
            .target_topology()
            .map_err(|_| OwnerError::InvalidMetadata)?;
        self.begin_operation()?;
        let tx = application_transaction(&mut self.client, 32)
            .await
            .map_err(transaction_error)?;
        require(&tx, self.request.dataset, self.identity).await?;
        let policy = snapshot(&tx, &self.request, &mut validate).await?;
        ingest_registry::require_retirement_registration(&tx, previous)
            .await
            .map_err(|error| match error {
                ingest_registry::RegistryError::Backend => OwnerError::Backend,
                _ => OwnerError::InvalidMetadata,
            })?;
        // Registry identity alone accepts closing and terminal history. This
        // consumer requires the complete terminal relation and no attempted
        // target, while leaving every historical row unchanged.
        const RETIRED: &str = "
            SELECT (o.state='released' AND o.exclusion_method='storage_fenced'
                    AND p.state='retired' AND s.state='closed'
                    AND o.exclusion_digest=p.closure_digest
                    AND s.closure_digest=p.closure_digest
                    AND o.released_at_us=p.retired_at_us
                    AND r.retired_at_us=p.retired_at_us
                    AND s.closed_at_us<=p.retired_at_us
                    AND d.topology_generation=$5::text::numeric AND t.sealed
                    AND NOT EXISTS (SELECT 1 FROM control.allocation_domains a
                        WHERE a.dataset_id=$1::bytea AND a.owner_resource_id=$2::bytea
                            AND a.owner_epoch=1 AND (a.state<>'closed'
                                OR a.closure_digest IS DISTINCT FROM p.closure_digest
                                OR a.closed_at_us IS DISTINCT FROM p.retired_at_us))
                    AND NOT EXISTS (SELECT 1 FROM control.owner_epochs
                        WHERE resource_id=$2::bytea AND epoch<>1)
                    AND NOT EXISTS (SELECT 1 FROM control.initial_ingest_successions
                        WHERE owner_resource_id=$2::bytea)
                    AND NOT EXISTS (SELECT 1 FROM control.initial_ingest_activations
                        WHERE owner_resource_id=$2::bytea)) IS TRUE,
                   p.closure_digest, p.retired_at_us
              FROM control.datasets d
              JOIN control.topology_generations t ON t.dataset_id=d.dataset_id
                   AND t.generation=d.topology_generation
              JOIN control.owner_epochs o ON o.resource_id=$2::bytea AND o.epoch=1
              JOIN control.role_instances r ON r.role_instance_id=o.role_instance_id
              JOIN control.producer_epochs p ON p.producer_slot_id=$3::bytea
                   AND p.producer_epoch=1
              JOIN control.source_streams s ON s.dataset_id=d.dataset_id
                   AND s.shard_id=$4::bigint AND s.owner_epoch=1
             WHERE d.dataset_id=$1::bytea";
        let row = tx
            .query_one(
                RETIRED,
                &[
                    &&previous.dataset_id.as_bytes()[..],
                    &&previous.ingest_resource_id.as_bytes()[..],
                    &&previous.producer_slot_id.as_bytes()[..],
                    &i64::from(previous.shard_id),
                    &topology.get().to_string(),
                ],
            )
            .await
            .map_err(backend)?;
        if !row.try_get::<_, bool>(0).map_err(backend)? {
            return Err(OwnerError::InvalidMetadata);
        }
        let digest = fixed::<32>(&row, 1)?;
        let retired_at_us: i64 = row.try_get(2).map_err(backend)?;
        tx.query_one(
            "SELECT control.require_initial_retirement($1::bytea,1,$2::bytea)",
            &[&&previous.producer_slot_id.as_bytes()[..], &&digest[..]],
        )
        .await
        .map_err(backend)?;
        // Sample UTC only after every historical/workflow lock wait. It is
        // construction data, not a lease extended by returning this response.
        let row = tx
            .query_one(
                "SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,
                        lease_expires_at_us FROM control.owner_epochs
                 WHERE resource_id=$1::bytea AND epoch=$2::text::numeric",
                &[
                    &&self.identity.resource_id.as_bytes()[..],
                    &self.identity.epoch.get().to_string(),
                ],
            )
            .await
            .map_err(backend)?;
        let created_at_us: i64 = row.try_get(0).map_err(backend)?;
        let lease_expires_at_us: i64 = row.try_get(1).map_err(backend)?;
        if created_at_us < retired_at_us || lease_expires_at_us <= created_at_us {
            return Err(OwnerError::Unavailable);
        }
        tx.rollback().await.map_err(backend)?;
        if self.client.is_closed() {
            return Err(OwnerError::Unavailable);
        }
        self.available = true;
        Ok(InitialSuccessorBasis {
            policy,
            created_at_us,
            lease_expires_at_us,
        })
    }
}
