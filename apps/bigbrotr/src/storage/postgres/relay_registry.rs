//! Exact private operational identities. Registration grants no network action,
//! enrollment, reader visibility or immutable archive dictionary allocation.
use super::{ControlSession, fixed, fresh_id, number, require, transaction_error};
use crate::storage::postgres::{
    ingest_registry::ControlOwner, initialization::application_transaction,
};
use bigbrotr_model::{DatasetId, EndpointId, OwnerEpoch, OwnerResourceId, ProcessIncarnation};
use bigbrotr_protocol::endpoint::Endpoint;
use std::{cell::Cell, fmt};
use tokio_postgres::{Row, Transaction, error::SqlState};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelayRegistryError {
    Unavailable,
    Prerequisite,
    Backend,
    InvalidMetadata,
    PolicyChanged,
    Capacity,
    Entropy,
    AuthorityExpired,
    CommitUnknown,
}
impl fmt::Display for RelayRegistryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "relay registration failed: {self:?}")
    }
}
impl std::error::Error for RelayRegistryError {}
fn backend(_: tokio_postgres::Error) -> RelayRegistryError {
    RelayRegistryError::Backend
}

/// Original immutable attribution. `created` describes only this transaction;
/// after an ambiguous COMMIT, replay cannot identify which caller inserted it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EndpointRegistration {
    pub identity: EndpointId,
    pub created: bool,
    pub registered_at_us: i64,
    pub config_revision: u64,
    pub policy_revision: u64,
    pub registered_by: ControlOwner,
}

const CANDIDATE_COUNT: &str = "
    SELECT count(*) FROM (
        SELECT endpoint_id FROM control.relay_endpoints
        WHERE dataset_id = $1::bytea AND normalization_version = 1
            AND candidate_digest = control.relay_endpoint_candidate($2::bytea)
        LIMIT 65
    ) AS bounded_bucket";

const FIND_EXACT: &str = "
    SELECT substring(e.endpoint_id FROM 1 FOR 17), e.registered_at_us,
        left(e.config_revision::text, 21), left(e.policy_revision::text, 21),
        substring(e.owner_resource_id FROM 1 FOR 17), left(e.owner_epoch::text, 21),
        substring(e.owner_incarnation FROM 1 FOR 17), substring(o.role_instance_id FROM 1 FOR 17)
    FROM control.relay_endpoints e
    JOIN control.owner_epochs o ON o.resource_id = e.owner_resource_id AND o.epoch = e.owner_epoch
    WHERE e.dataset_id = $1::bytea AND e.normalization_version = 1
        AND e.candidate_digest = control.relay_endpoint_candidate($2::bytea)
        AND e.url_bytes = $2::bytea
    LIMIT 2";

const INSERT: &str = "
    INSERT INTO control.relay_endpoints (
        endpoint_id, dataset_id, normalization_version, url_bytes, registered_at_us,
        config_revision, policy_revision, owner_resource_id, owner_epoch, owner_incarnation
    ) VALUES (
        $1::bytea, $2::bytea, 1, $3::bytea,
        (EXTRACT(EPOCH FROM pg_catalog.clock_timestamp()) * 1000000)::bigint,
        $4::text::numeric, $5::text::numeric, $6::bytea, $7::text::numeric, $8::bytea
    )";

impl ControlSession {
    /// Internal get-or-create under this connection's actual Control fence. The
    /// caller owns its finite deadline, transport driver and reservation; a
    /// cancelled or unsuccessful operation poisons the session. Fresh ownership
    /// plus the same normalized URL resolves an ambiguous insertion.
    ///
    /// Only scalar/fixed fields return from SQL: the bounded collision bucket is
    /// compared byte-for-byte in PostgreSQL, without fetching up to 65 full URLs.
    pub async fn register_relay_endpoint(
        &mut self,
        endpoint: &Endpoint,
        commit_started: &Cell<bool>,
    ) -> Result<EndpointRegistration, RelayRegistryError> {
        self.begin_operation()
            .map_err(|_| RelayRegistryError::Unavailable)?;
        let tx = application_transaction(&mut self.client, 3)
            .await
            .map_err(|error| match transaction_error(error) {
                super::OwnerError::Backend => RelayRegistryError::Backend,
                _ => RelayRegistryError::Prerequisite,
            })?;
        require(&tx, self.request.dataset, self.identity)
            .await
            .map_err(|_| RelayRegistryError::Unavailable)?;
        let dataset = &self.request.dataset.as_bytes()[..];
        // Separate READ COMMITTED statement: candidate lookup sees every insert
        // committed before this lock, even when acquiring it had to wait.
        let head = tx
            .query_one(
                "
            SELECT left(config_revision::text, 21), left(policy_revision::text, 21)
            FROM control.datasets WHERE dataset_id = $1::bytea FOR UPDATE",
                &[&dataset],
            )
            .await
            .map_err(backend)?;
        if number(&head, 0).map_err(|_| RelayRegistryError::InvalidMetadata)?
            != self.validated_policy.config_revision
            || number(&head, 1).map_err(|_| RelayRegistryError::InvalidMetadata)?
                != self.validated_policy.policy_revision
        {
            return Err(RelayRegistryError::PolicyChanged);
        }
        let result = register_in_locked_transaction(
            &tx,
            self.request.dataset,
            self.identity,
            self.validated_policy,
            endpoint,
        )
        .await?;
        if !result.created {
            tx.rollback().await.map_err(backend)?;
            self.available = true;
            return Ok(result);
        }
        commit_started.set(true);
        tx.commit().await.map_err(|error| {
            if error.code() == Some(&SqlState::from_code("BB001")) {
                RelayRegistryError::AuthorityExpired
            } else {
                RelayRegistryError::CommitUnknown
            }
        })?;
        self.available = true;
        Ok(result)
    }
}

/// Concrete transaction-local registry operation. The caller already holds the
/// actual Control fence and dataset lock and validated the current policy heads.
/// It retains responsibility for authentication, COMMIT, rollback and poisoning.
/// Both callers use this same bounded byte-exact collision lookup.
pub(super) async fn register_in_locked_transaction(
    tx: &Transaction<'_>,
    dataset_id: DatasetId,
    owner: ControlOwner,
    policy: super::PolicySnapshot,
    endpoint: &Endpoint,
) -> Result<EndpointRegistration, RelayRegistryError> {
    let dataset = &dataset_id.as_bytes()[..];
    let url = endpoint.as_str().as_bytes();
    let count: i64 = tx
        .query_one(CANDIDATE_COUNT, &[&dataset, &url])
        .await
        .map_err(backend)?
        .try_get(0)
        .map_err(|_| RelayRegistryError::InvalidMetadata)?;
    if !(0..=64).contains(&count) {
        return Err(RelayRegistryError::InvalidMetadata);
    }
    let existing = tx
        .query(FIND_EXACT, &[&dataset, &url])
        .await
        .map_err(backend)?;
    if let [row] = existing.as_slice() {
        return decode(row, false);
    }
    if !existing.is_empty() {
        return Err(RelayRegistryError::InvalidMetadata);
    }
    if count == 64 {
        return Err(RelayRegistryError::Capacity);
    }
    // Generate only after absence. A random PK collision refuses this
    // transaction; never hide another invariant with ON CONFLICT DO NOTHING.
    let identity = fresh_id().map_err(|_| RelayRegistryError::Entropy)?;
    tx.execute(
        INSERT,
        &[
            &&identity[..],
            &dataset,
            &url,
            &policy.config_revision.to_string(),
            &policy.policy_revision.to_string(),
            &&owner.resource_id.as_bytes()[..],
            &owner.epoch.get().to_string(),
            &&owner.process_incarnation.as_bytes()[..],
        ],
    )
    .await
    .map_err(backend)?;
    let rows = tx
        .query(FIND_EXACT, &[&dataset, &url])
        .await
        .map_err(backend)?;
    let [row] = rows.as_slice() else {
        return Err(RelayRegistryError::InvalidMetadata);
    };
    decode(row, true)
}

fn decode(row: &Row, created: bool) -> Result<EndpointRegistration, RelayRegistryError> {
    let invalid = |_| RelayRegistryError::InvalidMetadata;
    Ok(EndpointRegistration {
        identity: EndpointId::try_from(fixed::<16>(row, 0).map_err(invalid)?)
            .map_err(|_| RelayRegistryError::InvalidMetadata)?,
        created,
        registered_at_us: row
            .try_get(1)
            .map_err(|_| RelayRegistryError::InvalidMetadata)?,
        config_revision: number(row, 2).map_err(invalid)?,
        policy_revision: number(row, 3).map_err(invalid)?,
        registered_by: ControlOwner {
            resource_id: OwnerResourceId::try_from(fixed::<16>(row, 4).map_err(invalid)?)
                .map_err(|_| RelayRegistryError::InvalidMetadata)?,
            epoch: OwnerEpoch::new(number(row, 5).map_err(invalid)?)
                .map_err(|_| RelayRegistryError::InvalidMetadata)?,
            process_incarnation: ProcessIncarnation::try_from(
                fixed::<16>(row, 6).map_err(invalid)?,
            )
            .map_err(|_| RelayRegistryError::InvalidMetadata)?,
            role_instance_id: fixed(row, 7).map_err(invalid)?,
        },
    })
}
