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
    let native = custody.native.as_ref().expect("exclusive native owner");
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
            .inspect_activated_initial_retirement(native, request(), metadata, validate)
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
    let mut prefix = closed
        .source_resolution(native)
        .map_err(|e| (4, e.to_string()))?;
    for sequence in 1..=closed.reserved_through() {
        phase(progress, timeout)?;
        bounded(timeout, cancellation, async {
            owner.refresh(validate).await.map_err(owner_error)
        })
        .await?;
        let (input, outcome) = replay_grants(budget)?;
        phase(progress, timeout)?;
        let resolved = bounded(timeout, cancellation, async {
            owner
                .inspect_activated_initial_retirement_request(
                    native,
                    &closed,
                    ActivatedInitialResolveRequest {
                        ingest_root: path,
                        request: ProducerRequestId {
                            producer_slot_id: closed.producer_slot_id(),
                            producer_epoch: ProducerEpoch::new(2).expect("activated producer"),
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
        prefix.record(&resolved).map_err(|e| (4, e.to_string()))?;
        drop(resolved);
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
            .retire_activated_initial_producer(
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
    // The checked retirement context requires the exact nine-domain identity.
    // The enclosing owner closes native/releases Control; its supervisor joins
    // the transport before publishing this successful report.
    Ok(json!({"format":"bigbrotr-ingest-recovery/1",
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
        "native":"closed", "owner_release":"committed", "ready":false}))
}
