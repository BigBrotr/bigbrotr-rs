//! Bounded construction data for an explicitly requested first native owner.
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
}
