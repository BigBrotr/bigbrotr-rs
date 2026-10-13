//! Finite positive epoch2 recovery; observations never become a live writer.
use super::*;
use bigbrotr::storage::postgres::control_owner::{
    ActivatedInitialResolveRequest, InitialHistoryRequest,
};

const METADATA_BYTES: u64 = 128 * 1024;

/// Field order preserves native-first teardown on errors and unwinding too.
/// The parent independently retains the actual transport driver and its grant.
struct Custody {
    native: Option<IngestRecovery>,
    owner: Option<ControlSession>,
    client: Option<tokio_postgres::Client>,
}

pub(super) fn run(root: Resolved, output: &mut LifecycleOutput) -> Result<()> {
    let budget = owner_budget(&root, false, false, 2)?;
    supervision::run_owned(
        root,
        budget,
        output,
        "activated Ingest recovery",
        false,
        1,
        own,
    )
}

async fn own(
    root: Resolved,
    budget: ResourceBudget,
    progress: watch::Sender<Progress>,
    cancellation: Arc<Cancellation>,
    driver_custody: supervision::DriverCustody,
) -> Result<Value> {
    let path = Path::new(required(&root, "storage.ingest_root")?);
    let dataset: DatasetId = required(&root, "topology.dataset_id")?
        .parse()
        .map_err(|_| (3, "invalid dataset".into()))?;
    let timeout = duration(&root, "renewal_timeout");
    if cancellation.requested.load(Ordering::Acquire) {
        return Err((
            4,
            "activated Ingest recovery cancelled before native open".into(),
        ));
    }
    let native = IngestRecovery::inspect(
        path,
        dataset,
        native_limits(),
        grant(&budget, 0, native_limits().native_allowance_bytes())?,
    )
    .map_err(|e| (4, e.to_string()))?;
    if native.manifest().identity().owner_epoch.get() != 2 || native.manifest().key_schema() != 2 {
        return Err((
            4,
            "activated Ingest recovery requires native owner epoch2".into(),
        ));
    }
    let mut custody = Custody {
        native: Some(native),
        owner: None,
        client: None,
    };
    phase(&progress, timeout)?;
    let (prepared, control_grant) = postgres_command::prepare_control_shared(
        &root,
        required(&root, "postgres.control.user")?,
        required(&root, "postgres.control.password_ref")?,
        &budget,
    )?;
    // The sole worker retains both metadata and closing proof during each
    // input/outcome replay. Prove that full overlap before any claim or barrier.
    let metadata = grant(&budget, METADATA_BYTES, 0)?;
    let closing = grant(&budget, 4096, 4096)?;
    drop(replay_grants(&budget)?);
    phase(&progress, timeout)?;
    let (client, connection) = bounded(timeout, &cancellation, async {
        transport::connect(prepared)
            .await
            .map_err(|e| (1, e.to_string()))
    })
    .await?;
    custody.client = Some(client);
    let driver = tokio::spawn(connection);
    if let Err((driver, grant)) = driver_custody.send((driver, control_grant)) {
        drop(custody);
        postgres_command::join(driver).await;
        drop(grant);
        return Err((4, "activated Ingest recovery supervisor unavailable".into()));
    }
    let instance: DatasetId = required(&root, "topology.instance_id")?
        .parse()
        .map_err(|_| (3, "invalid instance".into()))?;
    let fresh = || super::super::super::fresh_process_incarnation().map_err(|e| (1, e.to_string()));
    let claim = ClaimRequest {
        dataset,
        instance,
        authority_mode: required(&root, "config.authority_mode")?.into(),
        role_instance_id: *fresh()?.as_bytes(),
        incarnation: fresh()?,
        lease_ttl: duration(&root, "lease_ttl"),
    };
    let validate = |document: &str, digest| {
        super::super::super::accepted_policy(&root, document, digest).map(|_| ())
    };
    phase(&progress, timeout)?;
    let (owner, _) = bounded(timeout, &cancellation, async {
        ControlSession::acquire(
            custody.client.take().expect("connected client"),
            claim,
            validate,
            &Cell::new(false),
        )
        .await
        .map_err(owner_error)
    })
    .await?;
    custody.owner = Some(owner);
    let report = resolve(
        &root,
        &budget,
        &progress,
        &cancellation,
        &mut custody,
        metadata,
        closing,
    )
    .await?;
    phase(&progress, duration(&root, "graceful_shutdown_timeout"))?;
    drop(custody.native.take());
    bounded(
        duration(&root, "graceful_shutdown_timeout"),
        &cancellation,
        async {
            custody
                .owner
                .take()
                .expect("claimed Control")
                .release()
                .await
                .map_err(owner_error)
        },
    )
    .await?;
    Ok(report)
}

async fn resolve(
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
        super::super::super::accepted_policy(root, document, digest).map(|_| ())
    };
    let request = || InitialHistoryRequest {
        ingest_root: path,
        policy_ttl: duration(root, "authorization_ttl"),
        cancelled: &cancellation.requested,
    };
    phase(progress, timeout)?;
    let observed = bounded(timeout, cancellation, async {
        owner
            .inspect_activated_initial_producer(native, request(), metadata, validate)
            .await
            .map_err(|e| (4, e.to_string()))
    })
    .await?;
    phase(progress, timeout)?;
    let closed = bounded(timeout, cancellation, async {
        owner
            .close_activated_initial_producer(native, observed, request(), closing, validate)
            .await
            .map_err(|e| (4, e.to_string()))
    })
    .await?;
    let mut resolved = 0_u64;
    for sequence in 1..=closed.reserved_through() {
        phase(progress, timeout)?;
        bounded(timeout, cancellation, async {
            owner.refresh(validate).await.map_err(owner_error)
        })
        .await?;
        let (input, outcome) = replay_grants(budget)?;
        phase(progress, timeout)?;
        drop(
            bounded(timeout, cancellation, async {
                owner
                    .resolve_activated_initial_request(
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
            .await?,
        );
        resolved += 1;
    }
    // The enclosing worker closes native and releases Control; the parent joins
    // its actual driver before publishing this successful terminal report.
    Ok(json!({"format":"bigbrotr-ingest-recovery/1",
        "state":"activated-positive-outcomes-resolved", "dataset_id":identity.dataset_id,
        "owner_resource_id":identity.owner_resource_id,
        "producer_slot_id":closed.producer_slot_id(), "producer_epoch":"2",
        "issued_through":closed.issued_through().to_string(),
        "reserved_through":closed.reserved_through().to_string(),
        "resolved_requests":resolved.to_string(), "source_closure":"not-performed",
        "native":"closed", "owner_release":"committed", "ready":false}))
}
