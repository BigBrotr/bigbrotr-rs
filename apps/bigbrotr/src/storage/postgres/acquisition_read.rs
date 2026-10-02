//! Read immutable scope data; fresh work must separately recheck current authority.
use super::{ControlSession, OwnerError, fixed, number, require, transaction_error};
use crate::{
    acquisition::{
        relay_source::{RelaySourceBinding, StoredRelaySource},
        scope::StoredAcquisitionScope,
    },
    runtime::OwnedPermit,
    storage::postgres::initialization::application_transaction,
};
use bigbrotr_model::{
    AcquisitionScopeId, DatasetId, DictionaryKind, Ordinal, PolicyEpoch, QualifiedRef,
    acquisition::AcquisitionScope,
};
use bigbrotr_protocol::{
    EventLimits,
    selector::{DecodeError, decode_normalized_selector},
};
use std::num::NonZeroU64;
use tokio_postgres::Row;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScopeReadError {
    Unavailable,
    Prerequisite,
    Backend,
    InvalidMetadata,
    Capacity,
}
impl std::fmt::Display for ScopeReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "acquisition scope read failed: {self:?}")
    }
}
impl std::error::Error for ScopeReadError {}
fn backend(_: tokio_postgres::Error) -> ScopeReadError {
    ScopeReadError::Backend
}

const READ_SCOPE: &str = "
    SELECT substring(s.endpoint_id FROM 1 FOR 17),
        substring(s.selector_id FROM 1 FOR 17),
        substring(s.access_scope_id FROM 1 FOR 17), s.visibility_kind,
        substring(s.visibility_domain FROM 1 FOR 17),
        left(s.visibility_ordinal::text,21), left(s.policy_revision::text,21),
        d.profile_version, octet_length(d.document),
        substring(d.document FROM 1 FOR 2097153)
    FROM control.acquisition_scopes s
    LEFT JOIN control.acquisition_selectors d
        ON d.dataset_id=s.dataset_id AND d.selector_id=s.selector_id
    WHERE s.dataset_id=$1::bytea AND s.scope_id=$2::bytea";

const READ_SOURCE: &str = "
    SELECT left(generation::text,21), substring(route_id FROM 1 FOR 17)
    FROM control.relay_enrollments
    WHERE dataset_id=$1::bytea AND endpoint_id=$2::bytea
        AND access_scope_id=$3::bytea AND enabled";

impl ControlSession {
    /// Current metadata for the exact loaded scope. No source authority is
    /// retained across this read; socket and native consumers recheck it.
    pub async fn read_acquisition_source(
        &mut self,
        scope: &StoredAcquisitionScope,
        permit: OwnedPermit,
    ) -> Result<Option<StoredRelaySource>, ScopeReadError> {
        if permit.request().units().data_bytes() < StoredRelaySource::REQUIRED_BYTES
            || permit.request().units().items() == 0
        {
            return Err(ScopeReadError::Capacity);
        }
        let scope = scope.scope();
        if scope.dataset() != self.request.dataset {
            return Err(ScopeReadError::InvalidMetadata);
        }
        self.begin_operation()
            .map_err(|_| ScopeReadError::Unavailable)?;
        let tx = application_transaction(&mut self.client, 4)
            .await
            .map_err(|error| match transaction_error(error) {
                OwnerError::Backend => ScopeReadError::Backend,
                _ => ScopeReadError::Prerequisite,
            })?;
        require(&tx, self.request.dataset, self.identity)
            .await
            .map_err(|_| ScopeReadError::Unavailable)?;
        let row = tx
            .query_opt(
                READ_SOURCE,
                &[
                    &&scope.dataset().as_bytes()[..],
                    &&scope.endpoint().as_bytes()[..],
                    &&scope.access().as_bytes()[..],
                ],
            )
            .await
            .map_err(backend)?;
        let result = if let Some(row) = row {
            let invalid = |_| ScopeReadError::InvalidMetadata;
            let binding = RelaySourceBinding {
                endpoint_id: scope.endpoint(),
                access_scope_id: scope.access(),
                enrollment_generation: NonZeroU64::new(number(&row, 0).map_err(invalid)?)
                    .ok_or(ScopeReadError::InvalidMetadata)?,
                route_id: fixed::<16>(&row, 1)
                    .map_err(invalid)?
                    .try_into()
                    .map_err(|_| ScopeReadError::InvalidMetadata)?,
            };
            // Destination rows are immutable. A concurrent enrollment change
            // leaves this original tuple intact; dispatch rechecks its generation.
            let (endpoint, route) =
                super::relay_source::load_destination(&tx, self.request.dataset, binding)
                    .await
                    .map_err(|error| match error {
                        OwnerError::Backend => ScopeReadError::Backend,
                        _ => ScopeReadError::InvalidMetadata,
                    })?;
            Some(StoredRelaySource::new(binding, endpoint, route, permit))
        } else {
            None
        };
        tx.rollback().await.map_err(backend)?;
        if self.client.is_closed() {
            return Err(ScopeReadError::Unavailable);
        }
        self.available = true;
        Ok(result)
    }

    /// Load historical or current intent without making it current authority.
    /// Reserve the selector's8MiB parser grant and one item before entry; the
    /// result retains it. The connection owner separately retains its PG framing
    /// reservation (including a2MiB row) and joins the driver on cancellation.
    /// Cancellation/error after begin poisons session reuse, even for a read.
    pub async fn read_acquisition_scope(
        &mut self,
        identity: AcquisitionScopeId,
        permit: OwnedPermit,
    ) -> Result<Option<StoredAcquisitionScope>, ScopeReadError> {
        if permit.request().units().data_bytes() < EventLimits::default().parse_bytes as u64
            || permit.request().units().items() == 0
        {
            return Err(ScopeReadError::Capacity);
        }
        self.begin_operation()
            .map_err(|_| ScopeReadError::Unavailable)?;
        let tx = application_transaction(&mut self.client, 4)
            .await
            .map_err(|error| match transaction_error(error) {
                OwnerError::Backend => ScopeReadError::Backend,
                _ => ScopeReadError::Prerequisite,
            })?;
        require(&tx, self.request.dataset, self.identity)
            .await
            .map_err(|_| ScopeReadError::Unavailable)?;
        let row = tx
            .query_opt(
                READ_SCOPE,
                &[
                    &&self.request.dataset.as_bytes()[..],
                    &&identity.as_bytes()[..],
                ],
            )
            .await
            .map_err(backend)?;
        let result = row
            .map(|row| decode_row(row, self.request.dataset, identity, permit))
            .transpose()?;
        tx.rollback().await.map_err(backend)?;
        if self.client.is_closed() {
            return Err(ScopeReadError::Unavailable);
        }
        self.available = true;
        Ok(result)
    }
}

fn decode_row(
    row: Row,
    dataset: DatasetId,
    identity: AcquisitionScopeId,
    permit: OwnedPermit,
) -> Result<StoredAcquisitionScope, ScopeReadError> {
    let invalid = |_| ScopeReadError::InvalidMetadata;
    let id = |index| fixed::<16>(&row, index).map_err(invalid);
    let number = |index| number(&row, index).map_err(invalid);
    if row
        .try_get::<_, i16>(3)
        .map_err(|_| ScopeReadError::InvalidMetadata)?
        != 4
        || row
            .try_get::<_, i16>(7)
            .map_err(|_| ScopeReadError::InvalidMetadata)?
            != 1
    {
        return Err(ScopeReadError::InvalidMetadata);
    }
    let scope = AcquisitionScope::new(
        dataset,
        id(0)?
            .try_into()
            .map_err(|_| ScopeReadError::InvalidMetadata)?,
        id(1)?
            .try_into()
            .map_err(|_| ScopeReadError::InvalidMetadata)?,
        id(2)?
            .try_into()
            .map_err(|_| ScopeReadError::InvalidMetadata)?,
        QualifiedRef {
            dataset_id: dataset,
            dictionary_kind: DictionaryKind::VisibilityScope,
            allocation_domain: id(4)?
                .try_into()
                .map_err(|_| ScopeReadError::InvalidMetadata)?,
            ordinal: Ordinal::new(number(5)?).map_err(|_| ScopeReadError::InvalidMetadata)?,
        },
        PolicyEpoch::new(number(6)?),
    )
    .map_err(|_| ScopeReadError::InvalidMetadata)?;
    let length: i32 = row
        .try_get(8)
        .map_err(|_| ScopeReadError::InvalidMetadata)?;
    let document: &[u8] = row
        .try_get(9)
        .map_err(|_| ScopeReadError::InvalidMetadata)?;
    if !(1..=2 * 1024 * 1024).contains(&length) || document.len() != length as usize {
        return Err(ScopeReadError::InvalidMetadata);
    }
    let selector = decode_normalized_selector(document).map_err(|error| match error {
        DecodeError::Bounds(_) => ScopeReadError::Capacity,
        _ => ScopeReadError::InvalidMetadata,
    })?;
    Ok(StoredAcquisitionScope::new(
        identity, scope, selector, permit,
    ))
}
