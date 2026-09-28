//! Immutable operational source binding, separate from native scientific bytes.
use crate::acquisition::{
    job::AcquisitionJobBinding, relay_source::RelaySourceBinding, scope::StoredAcquisitionScope,
};
use bigbrotr_model::{
    AcquisitionScopeId, DatasetId, DictionaryKind, JobId, ProducerRequestId, QualifiedRef,
};
use tokio_postgres::Transaction;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceBindingError {
    Backend,
    Prerequisite,
    Conflict,
    MissingReservation,
    Unavailable,
}
impl std::fmt::Display for SourceBindingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "producer source binding unavailable: {self:?}")
    }
}
impl std::error::Error for SourceBindingError {}
fn backend(_: tokio_postgres::Error) -> SourceBindingError {
    SourceBindingError::Backend
}

#[derive(Clone, Copy)]
pub(super) enum RequestPhase {
    Reservation,
    Dispatch,
}
#[derive(Clone, Copy)]
pub(super) struct CurrentSourceCheck<'a> {
    pub binding: RelaySourceBinding,
    pub visibility: QualifiedRef,
    pub scope: Option<&'a StoredAcquisitionScope>,
    pub job: Option<AcquisitionJobBinding>,
}

/// A Network origin cannot escape job checks by omitting every source field.
/// Bootstrap/import have no source obligation; historical recovery never calls
/// this fresh-effect preflight. Earlier qualified foundations remain usable.
pub(super) async fn require_event_source(
    tx: &Transaction<'_>,
    network: bool,
    source: Option<CurrentSourceCheck<'_>>,
) -> Result<(), SourceBindingError> {
    if network
        && installed(tx).await?.2
        && !source.is_some_and(|s| s.scope.is_some() && s.job.is_some())
    {
        return Err(SourceBindingError::Conflict);
    }
    Ok(())
}

/// Establish job locks before enrollment/Ingest/producer locks. The returned
/// duration is sampled by PostgreSQL after waits. Consumers cap a lease whose
/// local refresh ticket began before SQL, so later I/O cannot extend this time.
pub(super) async fn require_job(
    tx: &Transaction<'_>,
    dataset: DatasetId,
    source: RelaySourceBinding,
    binding: Option<AcquisitionJobBinding>,
) -> Result<Option<std::time::Duration>, SourceBindingError> {
    let job_installed = installed(tx).await?.2;
    let Some(binding) = binding else {
        return if job_installed {
            Err(SourceBindingError::Conflict)
        } else {
            Ok(None)
        };
    };
    if !job_installed {
        return Err(SourceBindingError::Prerequisite);
    }
    const REQUIRE_JOB: &str = "
        SELECT control.require_acquisition_execution(
            $1::bytea, $2::bytea, $3::bytea, $4::bytea, $5::bytea)";
    let remaining: i64 = tx
        .query_one(
            REQUIRE_JOB,
            &[
                &&dataset.as_bytes()[..],
                &&binding.job.as_bytes()[..],
                &&binding.scope.as_bytes()[..],
                &&source.endpoint_id.as_bytes()[..],
                &&source.access_scope_id.as_bytes()[..],
            ],
        )
        .await
        .map_err(|error| match error.code().map(|code| code.code()) {
            Some("BB001") => SourceBindingError::Unavailable,
            Some("23514") => SourceBindingError::Conflict,
            _ => SourceBindingError::Backend,
        })?
        .try_get(0)
        .map_err(backend)?;
    let remaining = u64::try_from(remaining)
        .ok()
        .filter(|v| *v > 0)
        .ok_or(SourceBindingError::Unavailable)?;
    Ok(Some(std::time::Duration::from_micros(remaining)))
}

async fn installed(tx: &Transaction<'_>) -> Result<(bool, bool, bool), SourceBindingError> {
    // application_transaction has already checked the complete immutable ledger
    // and holds the shared migration lock. Older unbound consumers remain usable.
    let row = tx
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM control.schema_versions WHERE migration=20),
            EXISTS (SELECT 1 FROM control.schema_versions WHERE migration=21),
            EXISTS (SELECT 1 FROM control.schema_versions WHERE migration=24)",
            &[],
        )
        .await
        .map_err(backend)?;
    Ok((
        row.try_get(0).map_err(backend)?,
        row.try_get(1).map_err(backend)?,
        row.try_get(2).map_err(backend)?,
    ))
}

const MATCH_BINDING: &str = "
    SELECT relay_endpoint_id IS NOT DISTINCT FROM $5::bytea
        AND relay_access_scope_id IS NOT DISTINCT FROM $6::bytea
        AND relay_enrollment_generation IS NOT DISTINCT FROM $7::text::numeric
        AND relay_route_id IS NOT DISTINCT FROM $8::bytea
    FROM control.producer_requests
    WHERE dataset_id=$1::bytea AND producer_slot_id=$2::bytea
        AND producer_epoch=$3::text::numeric AND request_sequence=$4::text::numeric";
const MATCH_SCOPED_BINDING: &str = "
    SELECT relay_endpoint_id IS NOT DISTINCT FROM $5::bytea
        AND relay_access_scope_id IS NOT DISTINCT FROM $6::bytea
        AND relay_enrollment_generation IS NOT DISTINCT FROM $7::text::numeric
        AND relay_route_id IS NOT DISTINCT FROM $8::bytea
        AND acquisition_scope_id IS NOT DISTINCT FROM $9::bytea
    FROM control.producer_requests
    WHERE dataset_id=$1::bytea AND producer_slot_id=$2::bytea
        AND producer_epoch=$3::text::numeric AND request_sequence=$4::text::numeric";
const MATCH_VISIBILITY: &str = "
    SELECT access_class='public' AND dictionary_kind=4
        AND allocation_domain=$3::bytea AND ordinal=$4::text::numeric
    FROM control.relay_access_contexts
    WHERE dataset_id=$1::bytea AND access_scope_id=$2::bytea";
const MATCH_JOB_BINDING: &str = "
    SELECT relay_endpoint_id IS NOT DISTINCT FROM $5::bytea
        AND relay_access_scope_id IS NOT DISTINCT FROM $6::bytea
        AND relay_enrollment_generation IS NOT DISTINCT FROM $7::text::numeric
        AND relay_route_id IS NOT DISTINCT FROM $8::bytea
        AND acquisition_scope_id IS NOT DISTINCT FROM $9::bytea
        AND acquisition_job_id IS NOT DISTINCT FROM $10::bytea
    FROM control.producer_requests
    WHERE dataset_id=$1::bytea AND producer_slot_id=$2::bytea
        AND producer_epoch=$3::text::numeric AND request_sequence=$4::text::numeric";

/// Compare omissions too: a bound request cannot be retried through the legacy
/// unbound path, and a legacy row cannot be adopted by a new relay binding.
pub(super) async fn require_binding(
    tx: &Transaction<'_>,
    dataset: DatasetId,
    request: ProducerRequestId,
    binding: Option<RelaySourceBinding>,
    scope: Option<AcquisitionScopeId>,
    job: Option<JobId>,
    phase: RequestPhase,
) -> Result<(), SourceBindingError> {
    let epoch = request.producer_epoch.get().to_string();
    let sequence = request.request_sequence.get().to_string();
    let (source_installed, scope_installed, job_installed) = installed(tx).await?;
    if (scope.is_some() && !scope_installed) || (job.is_some() && !job_installed) {
        return Err(SourceBindingError::Prerequisite);
    }
    let row = if source_installed {
        let endpoint = binding
            .as_ref()
            .map(|s| s.endpoint_id.as_bytes().as_slice());
        let access = binding
            .as_ref()
            .map(|s| s.access_scope_id.as_bytes().as_slice());
        let generation = binding.map(|s| s.enrollment_generation.get().to_string());
        let route = binding.as_ref().map(|s| s.route_id.as_bytes().as_slice());
        let scope = scope.as_ref().map(|s| s.as_bytes().as_slice());
        let job = job.as_ref().map(|j| j.as_bytes().as_slice());
        let dataset = &dataset.as_bytes()[..];
        let producer = &request.producer_slot_id.as_bytes()[..];
        let params: &[&(dyn tokio_postgres::types::ToSql + Sync)] = &[
            &dataset,
            &producer,
            &epoch,
            &sequence,
            &endpoint,
            &access,
            &generation,
            &route,
            &scope,
            &job,
        ];
        let (query, params) = if job_installed {
            (MATCH_JOB_BINDING, params)
        } else if scope_installed {
            (MATCH_SCOPED_BINDING, &params[..9])
        } else {
            (MATCH_BINDING, &params[..8])
        };
        tx.query_opt(query, params).await.map_err(backend)?
    } else {
        if binding.is_some() {
            return Err(SourceBindingError::Prerequisite);
        }
        tx.query_opt(
            "SELECT true FROM control.producer_requests WHERE dataset_id=$1::bytea
            AND producer_slot_id=$2::bytea AND producer_epoch=$3::text::numeric
            AND request_sequence=$4::text::numeric",
            &[
                &&dataset.as_bytes()[..],
                &&request.producer_slot_id.as_bytes()[..],
                &epoch,
                &sequence,
            ],
        )
        .await
        .map_err(backend)?
    };
    match row {
        Some(row) if !row.try_get::<_, bool>(0).map_err(backend)? => {
            Err(SourceBindingError::Conflict)
        }
        None if matches!(phase, RequestPhase::Dispatch) => {
            Err(SourceBindingError::MissingReservation)
        }
        _ => Ok(()),
    }
}

/// Control and dataset are already locked; take domain locks before Ingest and
/// producer locks. This check is repeated after reservation COMMIT, not cached
/// by policy_epoch. Policy, committed descriptor and socket custody remain the
/// trusted acquisition consumer's responsibility.
pub(super) async fn require_current(
    tx: &Transaction<'_>,
    dataset: DatasetId,
    check: CurrentSourceCheck<'_>,
) -> Result<(), SourceBindingError> {
    let (source_installed, scope_installed, _) = installed(tx).await?;
    if !source_installed || (check.scope.is_some() && !scope_installed) {
        return Err(SourceBindingError::Prerequisite);
    }
    require_current_enrollment(tx, dataset, check.binding, Some(check.visibility)).await?;
    if let Some(scope) = check.scope {
        require_current_scope(tx, dataset, check.binding, check.visibility, scope).await?;
    }
    Ok(())
}

async fn require_current_scope(
    tx: &Transaction<'_>,
    dataset: DatasetId,
    source: RelaySourceBinding,
    visibility: QualifiedRef,
    stored: &StoredAcquisitionScope,
) -> Result<(), SourceBindingError> {
    let scope = stored.scope();
    if scope.dataset() != dataset
        || scope.endpoint() != source.endpoint_id
        || scope.access() != source.access_scope_id
        || scope.visibility() != visibility
    {
        return Err(SourceBindingError::Conflict);
    }
    const REQUIRE_SCOPE: &str = "
        SELECT control.require_acquisition_scope($1::bytea, $2::bytea, $3::bytea, $4::bytea)";
    tx.query_one(
        REQUIRE_SCOPE,
        &[
            &&dataset.as_bytes()[..],
            &&stored.identity().as_bytes()[..],
            &&source.endpoint_id.as_bytes()[..],
            &&source.access_scope_id.as_bytes()[..],
        ],
    )
    .await
    .map_err(|error| {
        if error.as_db_error().is_some_and(|e| {
            e.code() == &tokio_postgres::error::SqlState::CHECK_VIOLATION
                && e.message() == "acquisition scope is not current for this source"
        }) {
            SourceBindingError::Unavailable
        } else {
            SourceBindingError::Backend
        }
    })?;
    // A trusted loader supplied this value. Still compare its complete identity
    // to the immutable row; the same scope ID cannot substitute another context.
    const MATCH_SCOPE: &str = "
        SELECT selector_id=$3::bytea AND visibility_kind=4
            AND visibility_domain=$4::bytea AND visibility_ordinal=$5::text::numeric
            AND policy_revision=$6::text::numeric
        FROM control.acquisition_scopes WHERE dataset_id=$1::bytea AND scope_id=$2::bytea";
    let row = tx
        .query_one(
            MATCH_SCOPE,
            &[
                &&dataset.as_bytes()[..],
                &&stored.identity().as_bytes()[..],
                &&scope.selector().as_bytes()[..],
                &&visibility.allocation_domain.as_bytes()[..],
                &visibility.ordinal.get().to_string(),
                &scope.policy_epoch().get().to_string(),
            ],
        )
        .await
        .map_err(backend)?;
    if !row.try_get::<_, bool>(0).map_err(backend)? {
        return Err(SourceBindingError::Conflict);
    }
    Ok(())
}

pub(super) async fn require_current_enrollment(
    tx: &Transaction<'_>,
    dataset: DatasetId,
    source: RelaySourceBinding,
    visibility: Option<QualifiedRef>,
) -> Result<(), SourceBindingError> {
    let visibility = visibility
        .filter(|v| v.dataset_id == dataset && v.dictionary_kind == DictionaryKind::VisibilityScope)
        .ok_or(SourceBindingError::Conflict)?;
    const REQUIRE_SOURCE: &str = "
        SELECT control.require_relay_source(
            $1::bytea, $2::bytea, $3::bytea, $4::text::numeric, $5::bytea)";
    tx.query_one(
        REQUIRE_SOURCE,
        &[
            &&dataset.as_bytes()[..],
            &&source.endpoint_id.as_bytes()[..],
            &&source.access_scope_id.as_bytes()[..],
            &source.enrollment_generation.get().to_string(),
            &&source.route_id.as_bytes()[..],
        ],
    )
    .await
    .map_err(|error| {
        if error.as_db_error().is_some_and(|e| {
            e.code() == &tokio_postgres::error::SqlState::CHECK_VIOLATION
                && e.message() == "relay source enrollment is not current and enabled"
        }) {
            SourceBindingError::Unavailable
        } else {
            SourceBindingError::Backend
        }
    })?;
    let matches = tx
        .query_opt(
            MATCH_VISIBILITY,
            &[
                &&dataset.as_bytes()[..],
                &&source.access_scope_id.as_bytes()[..],
                &&visibility.allocation_domain.as_bytes()[..],
                &visibility.ordinal.get().to_string(),
            ],
        )
        .await
        .map_err(backend)?
        .map(|row| row.try_get::<_, bool>(0))
        .transpose()
        .map_err(backend)?;
    if matches != Some(true) {
        return Err(SourceBindingError::Conflict);
    }
    Ok(())
}
