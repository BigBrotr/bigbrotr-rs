//! Retire one prepared empty attempt without adopting its writer incarnation.
use super::*;
use bigbrotr::storage::postgres::control_owner::InitialHistoryRequest;
use bigbrotr_model::Digest256;

const METADATA_BYTES: u64 = 128 * 1024;

/// Check both overlapping phases before claiming Control. The same single
/// worker retains metadata through replay and then through native/SQL dispatch.
pub(super) fn preflight(budget: &ResourceBudget) -> Result<()> {
    let _metadata = grant(budget, METADATA_BYTES, 0)?;
    let source = grant(budget, 4096, 4096)?;
    drop(replay_grants(budget)?);
    drop(source);
    drop(grant(budget, METADATA_BYTES, 4096)?);
    Ok(())
}

pub(super) async fn connected(
    root: &Resolved,
    budget: &ResourceBudget,
    progress: &watch::Sender<Progress>,
    cancellation: &Cancellation,
    recovery: &mut IngestRecovery,
    client: tokio_postgres::Client,
) -> Result<(ControlSession, Value)> {
    // This explicit action cannot complete a native epoch1 preparation gap.
    // Reject its incompatible native shape before claiming a new Control epoch.
    let identity = recovery.manifest().identity();
    if identity.owner_epoch.get() != 2 || recovery.manifest().key_schema() != 2 {
        return Err((
            4,
            "empty-attempt retirement requires native owner epoch2".into(),
        ));
    }
    let instance: DatasetId = required(root, "topology.instance_id")?
        .parse()
        .map_err(|_| (3, "invalid instance".into()))?;
    let path = Path::new(required(root, "storage.ingest_root")?);
    let timeout = duration(root, "renewal_timeout");
    let validate = |document: &str, digest| {
        super::super::super::accepted_policy(root, document, digest).map(|_| ())
    };
    let fresh = || super::super::super::fresh_process_incarnation().map_err(|e| (1, e.to_string()));
    let request = ClaimRequest {
        dataset: identity.dataset_id,
        instance,
        authority_mode: required(root, "config.authority_mode")?.into(),
        role_instance_id: *fresh()?.as_bytes(),
        incarnation: fresh()?,
        lease_ttl: duration(root, "lease_ttl"),
    };
    let commit = Cell::new(false);
    phase(progress, timeout)?;
    let (mut owner, _) = bounded(timeout, cancellation, async {
        ControlSession::acquire(client, request, validate, &commit)
            .await
            .map_err(owner_error)
    })
    .await?;
    let history_request = || InitialHistoryRequest {
        ingest_root: path,
        policy_ttl: duration(root, "authorization_ttl"),
        cancelled: &cancellation.requested,
    };
    phase(progress, timeout)?;
    let history = bounded(timeout, cancellation, async {
        owner
            .inspect_empty_initial_attempt(
                recovery,
                history_request(),
                grant(budget, METADATA_BYTES, 0)?,
                validate,
            )
            .await
            .map_err(|e| (4, e.to_string()))
    })
    .await?;
    let issued = history.issued_through();
    let domains = history.registration().allocation_domains.len();
    phase(progress, timeout)?;
    let mut prefix = history
        .source_resolution(grant(budget, 4096, 4096)?)
        .map_err(|e| (4, e.to_string()))?;
    for sequence in 1..=issued {
        phase(progress, timeout)?;
        bounded(timeout, cancellation, async {
            owner.refresh(validate).await.map_err(owner_error)
        })
        .await?;
        let (input, outcome) = replay_grants(budget)?;
        phase(progress, timeout)?;
        let result = bounded(timeout, cancellation, async {
            let spec = history.registration();
            owner
                .inspect_empty_initial_request(
                    &history,
                    ProducerResolveRequest {
                        ingest_root: path,
                        registration: spec,
                        request: ProducerRequestId {
                            producer_slot_id: spec.producer_slot_id,
                            producer_epoch: ProducerEpoch::new(1).expect("original producer"),
                            request_sequence: RequestSequence::new(sequence)
                                .expect("nonzero sequence"),
                        },
                        limits: limits(),
                        policy_ttl: duration(root, "authorization_ttl"),
                        cancelled: &cancellation.requested,
                    },
                    input,
                    outcome,
                    validate,
                )
                .await
                .map_err(|e| (4, e.to_string()))
        })
        .await?;
        prefix.record(&result).map_err(|e| (4, e.to_string()))?;
        drop(result);
    }
    phase(progress, timeout)?;
    let source = prefix.finish().map_err(|e| (4, e.to_string()))?;
    let resolved = source.resolved_requests();
    let original_closure = json!({
        "producer_epoch":"1", "issued_through":issued.to_string(),
        "resolved_requests":resolved.to_string(),
        "last_complete_sequence":source.last_complete_sequence().to_string(),
        "sha256":Digest256::from_bytes(source.digest())});
    let plan = history
        .reconstruct_retirement(&source)
        .map_err(|e| (4, e.to_string()))?;
    // The owned positive plan keeps its metadata grant and actual root/sequence
    // guards; no borrowed witness crosses mutable native dispatch.
    drop(source);
    phase(progress, timeout)?;
    bounded(timeout, cancellation, async {
        owner.refresh(validate).await.map_err(owner_error)
    })
    .await?;
    phase(progress, timeout)?;
    let retired_at = bounded(timeout, cancellation, async {
        owner
            .retire_empty_initial_attempt(
                recovery,
                &plan,
                history_request(),
                grant(budget, METADATA_BYTES, 4096)?,
                validate,
            )
            .await
            .map_err(|e| (4, e.to_string()))
    })
    .await?;
    let retirement = plan.retirement();
    let target = retirement.target_manifest().identity();
    // own() closes native and releases Control; run() collects the real driver
    // before emitting this successful report. An error keeps outcomes unknown.
    let report = json!({"format":"bigbrotr-ingest-recovery/1",
        "state":"empty-attempt-retired", "dataset_id":target.dataset_id,
        "owner_resource_id":target.owner_resource_id,
        "producer_slot_id":retirement.producer_slot_id(), "producer_epoch":"2",
        "issued_through":"0", "reserved_through":"0",
        "resolved_original_requests":resolved.to_string(),
        "original_source_closure":original_closure,
        "empty_attempt_retirement":{
            "owner_epoch":"2", "incarnation":target.process_incarnation,
            "retired_at_us":retired_at.to_string(),
            "sha256":Digest256::from_bytes(retirement.closure_digest()),
            "allocation_domains":domains.to_string(), "native_acknowledged":"0",
            "native_retired":"0", "owner_release":"committed"},
        "native":"closed", "owner_release":"committed", "ready":false});
    Ok((owner, report))
}
