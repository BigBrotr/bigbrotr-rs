//! Prove the complete activated request prefix before publishing source2 closure.
use super::*;

#[allow(
    clippy::too_many_arguments,
    reason = "shared source lifecycle with distinct retained grants and terminal action"
)]
pub(super) async fn resolve(
    root: &Resolved,
    budget: &ResourceBudget,
    progress: &watch::Sender<Progress>,
    cancellation: &Cancellation,
    custody: &mut Custody,
    metadata: OwnedPermit,
    closing: OwnedPermit,
    mode: Mode,
) -> Result<Value> {
    let native = custody.native.as_mut().expect("exclusive native owner");
    let owner = custody.owner.as_mut().expect("claimed Control");
    let identity = *native.manifest().identity();
    let timeout = duration(root, "renewal_timeout");
    let path = Path::new(required(root, "storage.ingest_root")?);
    let validate = |document: &str, digest| {
        super::super::super::super::accepted_policy(root, document, digest).map(|_| ())
    };
    let request = || InitialHistoryRequest {
        ingest_root: path,
        policy_ttl: duration(root, "authorization_ttl"),
        cancelled: &cancellation.requested,
    };
    phase(progress, timeout)?;
    let observed = bounded(timeout, cancellation, async {
        owner
            .inspect_activated_initial_source(native, request(), metadata, validate)
            .await
            .map_err(|e| (4, e.to_string()))
    })
    .await?;
    phase(progress, timeout)?;
    let closed = bounded(timeout, cancellation, async {
        owner
            .close_activated_initial_source_producer(native, observed, request(), closing, validate)
            .await
            .map_err(|e| (4, e.to_string()))
    })
    .await?;
    let mut prefix = closed
        .source_resolution(native)
        .map_err(|e| (4, e.to_string()))?;
    for (epoch, through) in [
        (1, closed.predecessor_issued_through()),
        (2, closed.reserved_through()),
    ] {
        for sequence in 1..=through {
            phase(progress, timeout)?;
            bounded(timeout, cancellation, async {
                owner.refresh(validate).await.map_err(owner_error)
            })
            .await?;
            let (input, outcome) = replay_grants(budget)?;
            phase(progress, timeout)?;
            let original = bounded(timeout, cancellation, async {
                let input_request = ActivatedInitialResolveRequest {
                    ingest_root: path,
                    request: ProducerRequestId {
                        producer_slot_id: closed.producer_slot_id(),
                        producer_epoch: ProducerEpoch::new(epoch).expect("retained producer epoch"),
                        request_sequence: RequestSequence::new(sequence).expect("nonzero sequence"),
                    },
                    limits: limits(),
                    policy_ttl: duration(root, "authorization_ttl"),
                    cancelled: &cancellation.requested,
                };
                let result = if epoch == 1 {
                    owner
                        .resolve_activated_initial_source_predecessor_request(
                            native,
                            &closed,
                            input_request,
                            input,
                            outcome,
                            validate,
                        )
                        .await
                } else {
                    owner
                        .resolve_activated_initial_source_request(
                            native,
                            &closed,
                            input_request,
                            input,
                            outcome,
                            validate,
                        )
                        .await
                };
                result.map_err(|e| (4, e.to_string()))
            })
            .await?;
            prefix.record(&original).map_err(|e| (4, e.to_string()))?;
            drop(original);
        }
    }
    let source = prefix.finish().map_err(|e| (4, e.to_string()))?;
    phase(progress, timeout)?;
    bounded(timeout, cancellation, async {
        owner.refresh(validate).await.map_err(owner_error)
    })
    .await?;
    phase(progress, timeout)?;
    bounded(timeout, cancellation, async {
        owner
            .close_activated_initial_source(
                &closed,
                &source,
                request(),
                grant(budget, 64 * 1024, 0)?,
                validate,
            )
            .await
            .map_err(|e| (4, e.to_string()))
    })
    .await?;
    // The enclosing worker closes native and releases Control; the parent joins
    // its actual driver before publishing this successful terminal report.
    let mut report = json!({"format":"bigbrotr-ingest-recovery/1",
        "state":"activated-source-closed", "dataset_id":identity.dataset_id,
        "owner_resource_id":identity.owner_resource_id,
        "producer_slot_id":closed.producer_slot_id(), "producer_epoch":"2",
        "issued_through":closed.issued_through().to_string(),
        "reserved_through":closed.reserved_through().to_string(),
        "resolved_requests":source.resolved_requests().to_string(),
        "source_closure":{"last_complete_sequence":source.last_complete_sequence().to_string(),
            "sha256":Digest256::from_bytes(source.digest())},
        "native":"closed", "owner_release":"committed", "ready":false});
    if mode == Mode::AbandonAcquisition {
        phase(progress, timeout)?;
        bounded(timeout, cancellation, async {
            owner.refresh(validate).await.map_err(owner_error)
        })
        .await?;
        phase(progress, timeout)?;
        let abandoned = bounded(timeout, cancellation, async {
            owner
                .abandon_activated_initial_acquisition(
                    &closed,
                    &source,
                    request(),
                    grant(budget, 128 * 1024, 0)?,
                    validate,
                )
                .await
                .map_err(|e| (4, e.to_string()))
        })
        .await?;
        report["state"] = json!("activated-acquisition-abandoned");
        report["acquisition_abandonment"] = json!({
            "jobs":abandoned.to_string(), "reason":"activated_initial_producer_abandoned",
            "coverage":"unknown"});
    }
    let replay = source
        .into_recovery_acknowledgement()
        .map_err(|e| (4, e.to_string()))?;
    drop(closed);
    acknowledge(root, budget, progress, cancellation, owner, native, replay).await?;
    Ok(report)
}
