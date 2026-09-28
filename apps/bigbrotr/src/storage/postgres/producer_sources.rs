//! Immutable operational source binding, separate from native scientific bytes.
use crate::acquisition::relay_source::RelaySourceBinding;
use bigbrotr_model::{DatasetId, DictionaryKind, ProducerRequestId, QualifiedRef};
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
pub(super) struct RequestSourceCheck {
    pub request: ProducerRequestId,
    pub binding: Option<RelaySourceBinding>,
    pub visibility: Option<QualifiedRef>,
    pub phase: RequestPhase,
}

async fn installed(tx: &Transaction<'_>) -> Result<bool, SourceBindingError> {
    // application_transaction has already checked the complete immutable ledger
    // and holds the shared migration lock. Older unbound consumers remain usable.
    tx.query_one(
        "SELECT EXISTS (SELECT 1 FROM control.schema_versions WHERE migration=20)",
        &[],
    )
    .await
    .map_err(backend)?
    .try_get(0)
    .map_err(backend)
}

const MATCH_BINDING: &str = "
    SELECT relay_endpoint_id IS NOT DISTINCT FROM $5::bytea
        AND relay_access_scope_id IS NOT DISTINCT FROM $6::bytea
        AND relay_enrollment_generation IS NOT DISTINCT FROM $7::text::numeric
        AND relay_route_id IS NOT DISTINCT FROM $8::bytea
    FROM control.producer_requests
    WHERE dataset_id=$1::bytea AND producer_slot_id=$2::bytea
        AND producer_epoch=$3::text::numeric AND request_sequence=$4::text::numeric";
const MATCH_VISIBILITY: &str = "
    SELECT access_class='public' AND dictionary_kind=4
        AND allocation_domain=$3::bytea AND ordinal=$4::text::numeric
    FROM control.relay_access_contexts
    WHERE dataset_id=$1::bytea AND access_scope_id=$2::bytea";

/// Compare omissions too: a bound request cannot be retried through the legacy
/// unbound path, and a legacy row cannot be adopted by a new relay binding.
pub(super) async fn require_binding(
    tx: &Transaction<'_>,
    dataset: DatasetId,
    request: ProducerRequestId,
    binding: Option<RelaySourceBinding>,
    phase: RequestPhase,
) -> Result<(), SourceBindingError> {
    let epoch = request.producer_epoch.get().to_string();
    let sequence = request.request_sequence.get().to_string();
    let row = if installed(tx).await? {
        let endpoint = binding
            .as_ref()
            .map(|s| s.endpoint_id.as_bytes().as_slice());
        let access = binding
            .as_ref()
            .map(|s| s.access_scope_id.as_bytes().as_slice());
        let generation = binding.map(|s| s.enrollment_generation.get().to_string());
        let route = binding.as_ref().map(|s| s.route_id.as_bytes().as_slice());
        tx.query_opt(
            MATCH_BINDING,
            &[
                &&dataset.as_bytes()[..],
                &&request.producer_slot_id.as_bytes()[..],
                &epoch,
                &sequence,
                &endpoint,
                &access,
                &generation,
                &route,
            ],
        )
        .await
        .map_err(backend)?
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
    check: RequestSourceCheck,
) -> Result<(), SourceBindingError> {
    require_binding(tx, dataset, check.request, check.binding, check.phase).await?;
    let Some(source) = check.binding else {
        return Ok(());
    };
    let visibility = check
        .visibility
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
