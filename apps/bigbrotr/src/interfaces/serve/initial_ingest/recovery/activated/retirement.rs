//! Retire only an already closed, fully resolved activated lifetime.
use super::*;

pub(super) async fn resolve(
    root: &Resolved,
    budget: &ResourceBudget,
    progress: &watch::Sender<Progress>,
    cancellation: &Cancellation,
    custody: &mut Custody,
    metadata: OwnedPermit,
    closing: OwnedPermit,
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
    let inspected = bounded(timeout, cancellation, async {
        owner
            .inspect_activated_initial_retirement_with_compacted(
                native,
                request(),
                metadata,
                limits().event,
                compacted_effect_grant(budget)?,
                validate,
            )
            .await
            .map_err(|e| (4, e.to_string()))
    })
    .await?;
    phase(progress, timeout)?;
    let closed = bounded(timeout, cancellation, async {
        owner
            .observe_activated_initial_retirement(native, inspected, request(), closing, validate)
            .await
            .map_err(|e| (4, e.to_string()))
    })
    .await?;
    let compacted = closed.has_compacted_initial();
    let mut prefix = if compacted {
        closed.compacted_source_resolution(native, &cancellation.requested)
    } else {
        closed.source_resolution(native)
    }
    .map_err(|e| (4, e.to_string()))?;
    let compacted_requests = if compacted {
        phase(progress, timeout)?;
        bounded(timeout, cancellation, async {
            owner
                .inspect_activated_retirement_compacted_predecessor(
                    native,
                    &closed,
                    request(),
                    limits().event,
                    compacted_effect_grant(budget)?,
                    &mut prefix,
                    validate,
                )
                .await
                .map_err(|e| (4, e.to_string()))
        })
        .await?
    } else {
        0
    };
    for (epoch, through) in [
        (
            1,
            if compacted {
                0
            } else {
                closed.predecessor_issued_through()
            },
        ),
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
                        .inspect_activated_initial_retirement_predecessor_request(
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
                        .inspect_activated_initial_retirement_request(
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
    let gaps = bounded(timeout, cancellation, async {
        owner
            .retire_activated_initial_producer_with_compacted(
                &closed,
                &source,
                request(),
                grant(budget, 128 * 1024, 0)?,
                limits().event,
                compacted_effect_grant(budget)?,
                validate,
            )
            .await
            .map_err(|e| (4, e.to_string()))
    })
    .await?;
    // The checked retirement context requires the exact nine-domain identity.
    // The enclosing owner closes native/releases Control; its supervisor joins
    // the transport before publishing this successful report.
    let mut report = json!({"format":"bigbrotr-ingest-recovery/1",
        "state":"activated-initial-retired", "dataset_id":identity.dataset_id,
        "owner_resource_id":identity.owner_resource_id,
        "producer_slot_id":closed.producer_slot_id(), "producer_epoch":"2",
        "issued_through":closed.issued_through().to_string(),
        "reserved_through":closed.reserved_through().to_string(),
        "resolved_requests":source.resolved_requests().to_string(),
        "source_closure":{"last_complete_sequence":source.last_complete_sequence().to_string(),
            "sha256":Digest256::from_bytes(source.digest())},
        "ingest_retirement":{"allocation_domains":"9", "acquisition_gaps":gaps.to_string(),
            "owner_release":"committed", "native_acknowledged":"0", "native_retired":"0"},
        "native":"closed", "owner_release":"committed", "ready":false});
    if compacted {
        report["recovery_basis"] = json!("closed-producer-floor-v3");
        report["compacted_requests"] = json!(compacted_requests.to_string());
    }
    let replay = source
        .into_recovery_acknowledgement()
        .map_err(|e| (4, e.to_string()))?;
    drop(closed);
    let receipt = acknowledge(root, budget, progress, cancellation, owner, native, replay).await?;
    report["ingest_retirement"]["native_acknowledged"] =
        json!(receipt.acknowledged_through().to_string());
    Ok(report)
}
