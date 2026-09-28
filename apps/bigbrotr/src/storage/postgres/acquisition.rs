//! Immutable operational scope registration. No job, network or coverage grant.
use super::{ControlSession, fixed, fresh_id, number, require, transaction_error};
use crate::storage::postgres::{
    ingest_registry::ControlOwner, initialization::application_transaction,
};
use bigbrotr_model::{
    AccessScopeId, AcquisitionScopeId, DatasetId, DictionaryKind, EndpointId, PolicyEpoch,
    QualifiedRef, SelectorId,
    acquisition::{AcquisitionScope, CollectionSelector},
};
use bigbrotr_protocol::selector::encode_selector;
use std::{cell::Cell, fmt};
use tokio_postgres::{Transaction, error::SqlState};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScopeRegistrationError {
    Unavailable,
    Prerequisite,
    Backend,
    InvalidInput,
    InvalidMetadata,
    PolicyChanged,
    Capacity,
    Entropy,
    AuthorityExpired,
    CommitUnknown,
}
impl fmt::Display for ScopeRegistrationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "acquisition registration failed: {self:?}")
    }
}
impl std::error::Error for ScopeRegistrationError {}
fn backend(_: tokio_postgres::Error) -> ScopeRegistrationError {
    ScopeRegistrationError::Backend
}

pub struct ScopeIntent<'a> {
    pub selector: &'a CollectionSelector,
    pub endpoint: EndpointId,
    pub access: AccessScopeId,
    pub visibility: QualifiedRef,
    /// Retain this exact epoch across ambiguous retries; never silently rebind.
    pub policy_epoch: PolicyEpoch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScopeRegistration {
    pub identity: AcquisitionScopeId,
    pub scope: AcquisitionScope,
    /// This transaction inserted the row; a replay cannot attribute a prior
    /// ambiguous insertion to any particular caller.
    pub created: bool,
}

struct Attribution {
    dataset: DatasetId,
    config: u64,
    policy: u64,
    owner: ControlOwner,
}

const FIND_SELECTOR: &str = "
    SELECT substring(selector_id FROM 1 FOR 17)
    FROM control.acquisition_selectors
    WHERE dataset_id=$1::bytea AND profile_version=1
        AND candidate_digest=control.acquisition_selector_candidate($2::bytea)
        AND document=$2::bytea
    LIMIT 2";
const COUNT_SELECTORS: &str = "
    SELECT count(*) FROM (
        SELECT selector_id FROM control.acquisition_selectors
        WHERE dataset_id=$1::bytea AND profile_version=1
            AND candidate_digest=control.acquisition_selector_candidate($2::bytea)
        LIMIT 65
    ) AS bounded_bucket";
const INSERT_SELECTOR: &str = "
    INSERT INTO control.acquisition_selectors (
        selector_id, dataset_id, profile_version, document, registered_at_us,
        config_revision, policy_revision, owner_resource_id, owner_epoch, owner_incarnation
    ) VALUES (
        $1::bytea, $2::bytea, 1, $3::bytea,
        (EXTRACT(EPOCH FROM pg_catalog.clock_timestamp()) * 1000000)::bigint,
        $4::text::numeric, $5::text::numeric, $6::bytea, $7::text::numeric, $8::bytea
    )";
const FIND_SCOPE: &str = "
    SELECT substring(scope_id FROM 1 FOR 17)
    FROM control.acquisition_scopes
    WHERE dataset_id=$1::bytea AND endpoint_id=$2::bytea AND selector_id=$3::bytea
        AND access_scope_id=$4::bytea AND visibility_kind=4
        AND visibility_domain=$5::bytea AND visibility_ordinal=$6::text::numeric
        AND policy_revision=$7::text::numeric
    LIMIT 2";
const INSERT_SCOPE: &str = "
    INSERT INTO control.acquisition_scopes (
        scope_id, dataset_id, endpoint_id, selector_id, access_scope_id,
        visibility_domain, visibility_ordinal, policy_revision, config_revision,
        owner_resource_id, owner_epoch, owner_incarnation, registered_at_us
    ) VALUES (
        $1::bytea, $2::bytea, $3::bytea, $4::bytea, $5::bytea,
        $6::bytea, $7::text::numeric, $8::text::numeric, $9::text::numeric,
        $10::bytea, $11::text::numeric, $12::bytea,
        (EXTRACT(EPOCH FROM pg_catalog.clock_timestamp()) * 1000000)::bigint
    )";

impl ControlSession {
    /// Register explicit intent at its requested policy epoch, or resolve an
    /// existing original row. A historical result never authorizes fresh work.
    /// This does not fill legacy policy fields or validate any future job. The
    /// caller owns a deadline, driver and memory grants through completion.
    /// Cancellation/failed I/O poisons the session; retry exact intent through a
    /// fresh owner to resolve an ambiguous COMMIT. No mutation of old scopes.
    pub async fn register_acquisition_scope(
        &mut self,
        input: ScopeIntent<'_>,
        commit_started: &Cell<bool>,
    ) -> Result<ScopeRegistration, ScopeRegistrationError> {
        if input.visibility.dataset_id != self.request.dataset
            || input.visibility.dictionary_kind != DictionaryKind::VisibilityScope
            || input.policy_epoch.get() == 0
        {
            return Err(ScopeRegistrationError::InvalidInput);
        }
        let document =
            encode_selector(input.selector).map_err(|_| ScopeRegistrationError::Capacity)?;
        let context = Attribution {
            dataset: self.request.dataset,
            config: self.validated_policy.config_revision,
            policy: self.validated_policy.policy_revision,
            owner: self.identity,
        };
        self.begin_operation()
            .map_err(|_| ScopeRegistrationError::Unavailable)?;
        let tx = application_transaction(&mut self.client, 4)
            .await
            .map_err(|error| match transaction_error(error) {
                super::OwnerError::Backend => ScopeRegistrationError::Backend,
                _ => ScopeRegistrationError::Prerequisite,
            })?;
        require(&tx, context.dataset, context.owner)
            .await
            .map_err(|_| ScopeRegistrationError::Unavailable)?;
        let dataset = context.dataset.as_bytes().as_slice();
        // A separate READ COMMITTED lookup after this lock sees any registration
        // committed while we waited. The same lock serializes policy changes.
        let head = tx
            .query_one(
                "
            SELECT left(config_revision::text,21), left(policy_revision::text,21)
            FROM control.datasets WHERE dataset_id=$1::bytea FOR UPDATE",
                &[&dataset],
            )
            .await
            .map_err(backend)?;
        if number(&head, 0).map_err(|_| ScopeRegistrationError::InvalidMetadata)? != context.config
            || number(&head, 1).map_err(|_| ScopeRegistrationError::InvalidMetadata)?
                != context.policy
        {
            return Err(ScopeRegistrationError::PolicyChanged);
        }
        let (selector, new_selector) = choose_selector(&tx, context.dataset, &document).await?;
        let scope = AcquisitionScope::new(
            context.dataset,
            input.endpoint,
            selector,
            input.access,
            input.visibility,
            input.policy_epoch,
        )
        .map_err(|_| ScopeRegistrationError::InvalidInput)?;
        let endpoint = input.endpoint.as_bytes().as_slice();
        let selector = selector.as_bytes().as_slice();
        let access = input.access.as_bytes().as_slice();
        let domain = input.visibility.allocation_domain.as_bytes().as_slice();
        let ordinal = input.visibility.ordinal.get().to_string();
        let policy = input.policy_epoch.get().to_string();
        // A newly generated ID is not a registered selector. Never let an
        // accidental PK collision resolve another selector's existing scope.
        let rows = if new_selector {
            Vec::new()
        } else {
            tx.query(
                FIND_SCOPE,
                &[
                    &dataset, &endpoint, &selector, &access, &domain, &ordinal, &policy,
                ],
            )
            .await
            .map_err(backend)?
        };
        if let [row] = rows.as_slice() {
            let identity = AcquisitionScopeId::try_from(
                fixed::<16>(row, 0).map_err(|_| ScopeRegistrationError::InvalidMetadata)?,
            )
            .map_err(|_| ScopeRegistrationError::InvalidMetadata)?;
            tx.rollback().await.map_err(backend)?;
            self.available = true;
            return Ok(ScopeRegistration {
                identity,
                scope,
                created: false,
            });
        }
        if !rows.is_empty() {
            return Err(ScopeRegistrationError::InvalidMetadata);
        }
        // Exact historical identity resolves before fresh-epoch checks. A lost
        // COMMIT followed by policy change cannot create a replacement scope.
        if input.policy_epoch.get() != context.policy {
            return Err(ScopeRegistrationError::PolicyChanged);
        }
        if new_selector {
            insert_selector(&tx, &context, scope.selector(), &document).await?;
        }
        let identity =
            AcquisitionScopeId::try_from(fresh_id().map_err(|_| ScopeRegistrationError::Entropy)?)
                .map_err(|_| ScopeRegistrationError::Entropy)?;
        tx.execute(
            INSERT_SCOPE,
            &[
                &&identity.as_bytes()[..],
                &dataset,
                &endpoint,
                &selector,
                &access,
                &domain,
                &ordinal,
                &policy,
                &context.config.to_string(),
                &&context.owner.resource_id.as_bytes()[..],
                &context.owner.epoch.get().to_string(),
                &&context.owner.process_incarnation.as_bytes()[..],
            ],
        )
        .await
        .map_err(backend)?;
        commit_started.set(true);
        tx.commit().await.map_err(|error| {
            if error.code() == Some(&SqlState::from_code("BB001")) {
                ScopeRegistrationError::AuthorityExpired
            } else {
                ScopeRegistrationError::CommitUnknown
            }
        })?;
        self.available = true;
        Ok(ScopeRegistration {
            identity,
            scope,
            created: true,
        })
    }
}

async fn choose_selector(
    tx: &Transaction<'_>,
    dataset: DatasetId,
    document: &[u8],
) -> Result<(SelectorId, bool), ScopeRegistrationError> {
    let dataset = dataset.as_bytes().as_slice();
    let count: i64 = tx
        .query_one(COUNT_SELECTORS, &[&dataset, &document])
        .await
        .map_err(backend)?
        .try_get(0)
        .map_err(|_| ScopeRegistrationError::InvalidMetadata)?;
    if !(0..=64).contains(&count) {
        return Err(ScopeRegistrationError::InvalidMetadata);
    }
    let rows = tx
        .query(FIND_SELECTOR, &[&dataset, &document])
        .await
        .map_err(backend)?;
    if let [row] = rows.as_slice() {
        return SelectorId::try_from(
            fixed::<16>(row, 0).map_err(|_| ScopeRegistrationError::InvalidMetadata)?,
        )
        .map(|id| (id, false))
        .map_err(|_| ScopeRegistrationError::InvalidMetadata);
    }
    if !rows.is_empty() {
        return Err(ScopeRegistrationError::InvalidMetadata);
    }
    if count == 64 {
        return Err(ScopeRegistrationError::Capacity);
    }
    let identity = SelectorId::try_from(fresh_id().map_err(|_| ScopeRegistrationError::Entropy)?)
        .map_err(|_| ScopeRegistrationError::Entropy)?;
    Ok((identity, true))
}

async fn insert_selector(
    tx: &Transaction<'_>,
    context: &Attribution,
    identity: SelectorId,
    document: &[u8],
) -> Result<(), ScopeRegistrationError> {
    let dataset = context.dataset.as_bytes().as_slice();
    tx.execute(
        INSERT_SELECTOR,
        &[
            &&identity.as_bytes()[..],
            &dataset,
            &document,
            &context.config.to_string(),
            &context.policy.to_string(),
            &&context.owner.resource_id.as_bytes()[..],
            &context.owner.epoch.get().to_string(),
            &&context.owner.process_incarnation.as_bytes()[..],
        ],
    )
    .await
    .map_err(backend)?;
    Ok(())
}
