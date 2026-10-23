//! Read an already reclaimed initial retirement. This cannot install a floor,
//! remove detail, acknowledge a request or replace original-input recovery.
use super::*;
use bigbrotr::storage::{
    postgres::control_owner::CompactedInitialRetirement,
    rocks::store::{
        COMPACTED_PREFIX_METADATA_BYTES, RetainedCompactedPrefix, compacted_outcome_data_bytes,
    },
};

pub(in crate::interfaces::serve::initial_ingest) fn inspect(
    budget: &ResourceBudget,
    recovery: &IngestRecovery,
    spec: &ingest_registry::InitialRegistration,
    cancelled: &std::sync::atomic::AtomicBool,
) -> Result<Option<RetainedCompactedPrefix>> {
    if recovery
        .closed_producer_floor(spec.producer_slot_id)
        .map_err(|e| (4, e.to_string()))?
        .is_none()
    {
        return Ok(None);
    }
    let limits = limits().event;
    let outcome_bytes = compacted_outcome_data_bytes(limits).map_err(|e| (3, e.to_string()))?;
    let metadata = grant(budget, COMPACTED_PREFIX_METADATA_BYTES, 0)?;
    // Check the later SQL + outcome overlap before claiming Control. Native
    // census uses the same outcome allowance sequentially; its decoded facts
    // and grant do not survive inspect_initial.
    drop((
        grant(budget, CompactedInitialRetirement::REQUIRED_DATA_BYTES, 0)?,
        grant(budget, outcome_bytes, 0)?,
    ));
    RetainedCompactedPrefix::inspect_initial(
        recovery,
        spec.producer_slot_id,
        limits,
        metadata,
        grant(budget, outcome_bytes, 0)?,
        cancelled,
    )
    .map(Some)
    .map_err(|e| (4, e.to_string()))
}

#[allow(
    clippy::too_many_arguments,
    reason = "existing supervision, native custody and borrowed history have distinct owners"
)]
pub(super) async fn retirement(
    root: &Resolved,
    budget: &ResourceBudget,
    progress: &watch::Sender<Progress>,
    cancellation: &Cancellation,
    owner: &mut ControlSession,
    recovery: &IngestRecovery,
    prefix: &RetainedCompactedPrefix,
    spec: &ingest_registry::InitialRegistration,
) -> Result<Value> {
    let limits = limits().event;
    let outcome_bytes = compacted_outcome_data_bytes(limits).map_err(|e| (3, e.to_string()))?;
    let timeout = duration(root, "renewal_timeout");
    let validate = |document: &str, digest| {
        super::super::super::accepted_policy(root, document, digest).map(|_| ())
    };
    phase(progress, timeout)?;
    let retired = bounded(timeout, cancellation, async {
        owner
            .inspect_initial_compacted_retirement(
                recovery,
                prefix,
                ProducerCloseRequest {
                    ingest_root: Path::new(required(root, "storage.ingest_root")?),
                    registration: spec,
                    policy_ttl: duration(root, "authorization_ttl"),
                    cancelled: &cancellation.requested,
                },
                limits,
                grant(budget, CompactedInitialRetirement::REQUIRED_DATA_BYTES, 0)?,
                grant(budget, outcome_bytes, 0)?,
                validate,
            )
            .await
            .map_err(|e| (4, e.to_string()))
    })
    .await?;
    Ok(json!({
        "format":"bigbrotr-ingest-recovery/1", "state":"initial-retired",
        "dataset_id":spec.dataset_id, "owner_resource_id":spec.ingest_resource_id,
        "producer_slot_id":spec.producer_slot_id, "producer_epoch":"1",
        "issued_through":retired.issued_through().to_string(),
        "reserved_through":retired.issued_through().to_string(),
        "resolved_requests":"0", "compacted_requests":retired.retired_through().to_string(),
        "recovery_basis":"closed-producer-floor-v3",
        "source_closure":{
            "last_complete_sequence":retired.last_complete_sequence().to_string(),
            "sha256":bigbrotr_model::Digest256::from_bytes(retired.digest())
        },
        "ingest_retirement":{
            "allocation_domains":spec.allocation_domains.len().to_string(),
            "acquisition_gaps":retired.acquisition_gaps().to_string(),
            "owner_release":"committed",
            "native_acknowledged":retired.acknowledged_through().to_string(),
            "native_retired":retired.retired_through().to_string()
        },
        "native":"compacted", "owner_release":"committed", "ready":false
    }))
}
