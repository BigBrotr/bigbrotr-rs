//! Finite epoch2 recovery and source closure under one native/Control lifecycle.
use super::*;
use bigbrotr::storage::postgres::control_owner::{
    ActivatedInitialResolveRequest, InitialHistoryRequest,
};
use bigbrotr::storage::rocks::{
    store::compacted_outcome_data_bytes, succession::SuccessionProfile,
};

mod retirement;
mod source;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Mode {
    Resolve,
    CloseSource,
    AbandonAcquisition,
    RetireInitial,
}

/// Field order preserves native-first teardown on errors and unwinding too.
/// The parent independently retains the actual transport driver and its grant.
struct Custody {
    native: Option<IngestRecovery>,
    owner: Option<ControlSession>,
    client: Option<tokio_postgres::Client>,
}

pub(super) fn run(root: Resolved, mode: Mode, output: &mut LifecycleOutput) -> Result<()> {
    let budget = owner_budget(&root, false, false, 2)?;
    supervision::run_owned(
        root,
        budget,
        output,
        "activated Ingest recovery",
        false,
        1,
        move |root, budget, progress, cancellation, driver| {
            own(root, budget, progress, cancellation, driver, mode)
        },
    )
}

async fn own(
    root: Resolved,
    budget: ResourceBudget,
    progress: watch::Sender<Progress>,
    cancellation: Arc<Cancellation>,
    driver_custody: supervision::DriverCustody,
    mode: Mode,
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
    let metadata_bytes = SuccessionProfile::for_recovery(2, None)
        .map_err(|e| (3, e.to_string()))?
        .required_metadata_bytes();
    let metadata = grant(&budget, metadata_bytes, 0)?;
    let closing = grant(&budget, 4096, 4096)?;
    drop(replay_grants(&budget)?);
    if mode == Mode::Resolve {
        drop(compacted_effect_grant(&budget)?);
    }
    if matches!(mode, Mode::CloseSource | Mode::AbandonAcquisition) {
        // Publication overlaps metadata and closing, after dropping replay data.
        let _transaction = grant(&budget, 64 * 1024, 0)?;
        drop(compacted_effect_grant(&budget)?);
    }
    if matches!(mode, Mode::AbandonAcquisition | Mode::RetireInitial) {
        // Each terminal operation retains metadata and the native closing proof.
        let _transaction = grant(&budget, 128 * 1024, 0)?;
        drop(compacted_effect_grant(&budget)?);
    }
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
    {
        let commit = Cell::new(false);
        let acquisition = ControlSession::acquire(
            custody.client.take().expect("connected client"),
            claim,
            validate,
            &commit,
        );
        tokio::pin!(acquisition);
        let mut guard = supervision::ClaimExit(Some(&mut custody.native));
        let (owner, _) = bounded(timeout, &cancellation, async {
            acquisition.as_mut().await.map_err(owner_error)
        })
        .await?;
        custody.owner = Some(owner);
        guard.0 = None;
    }
    let report = match mode {
        Mode::Resolve => {
            resolve(
                &root,
                &budget,
                &progress,
                &cancellation,
                &mut custody,
                metadata,
                closing,
            )
            .await?
        }
        Mode::CloseSource | Mode::AbandonAcquisition => {
            source::resolve(
                &root,
                &budget,
                &progress,
                &cancellation,
                &mut custody,
                metadata,
                closing,
                mode,
            )
            .await?
        }
        Mode::RetireInitial => {
            retirement::resolve(
                &root,
                &budget,
                &progress,
                &cancellation,
                &mut custody,
                metadata,
                closing,
            )
            .await?
        }
    };
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
            .inspect_activated_initial_producer_with_compacted(
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
            .close_activated_initial_producer(native, observed, request(), closing, validate)
            .await
            .map_err(|e| (4, e.to_string()))
    })
    .await?;
    let compacted = closed.has_compacted_initial();
    let mut prefix = if closed.issued_through() == closed.reserved_through() {
        Some(
            if compacted {
                closed.compacted_acknowledgement_resolution(native, &cancellation.requested)
            } else {
                closed.acknowledgement_resolution(native)
            }
            .map_err(|e| (4, e.to_string()))?,
        )
    } else {
        None
    };
    let compacted_requests = if compacted {
        phase(progress, timeout)?;
        bounded(timeout, cancellation, async {
            owner
                .resolve_activated_compacted_predecessor(
                    native,
                    &closed,
                    request(),
                    limits().event,
                    compacted_effect_grant(budget)?,
                    prefix.as_mut(),
                    validate,
                )
                .await
                .map_err(|e| (4, e.to_string()))
        })
        .await?
    } else {
        0
    };
    let mut resolved = 0_u64;
    // Original ancestry replays before the current lifetime. Compacted ancestry
    // has completed its distinct canonical phase above, never a head-only shortcut.
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
                        .resolve_activated_initial_predecessor_request(
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
                        .resolve_activated_initial_request(
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
            if let Some(prefix) = &mut prefix {
                prefix.record(&original).map_err(|e| (4, e.to_string()))?;
            }
            drop(original);
            if epoch == 2 {
                resolved += 1;
            }
        }
    }
    // The enclosing worker closes native and releases Control; the parent joins
    // its actual driver before publishing this successful terminal report.
    let mut report = json!({"format":"bigbrotr-ingest-recovery/1",
        "state":"activated-positive-outcomes-resolved", "dataset_id":identity.dataset_id,
        "owner_resource_id":identity.owner_resource_id,
        "producer_slot_id":closed.producer_slot_id(), "producer_epoch":"2",
        "issued_through":closed.issued_through().to_string(),
        "reserved_through":closed.reserved_through().to_string(),
        "resolved_requests":resolved.to_string(), "source_closure":"not-performed",
        "native":"closed", "owner_release":"committed", "ready":false});
    if compacted {
        report["recovery_basis"] = json!("closed-producer-floor-v3");
        report["compacted_requests"] = json!(compacted_requests.to_string());
    }
    if let Some(prefix) = prefix {
        let replay = prefix.finish().map_err(|e| (4, e.to_string()))?;
        drop(closed);
        acknowledge(root, budget, progress, cancellation, owner, native, replay).await?;
    }
    Ok(report)
}

fn compacted_effect_grant(budget: &ResourceBudget) -> Result<OwnedPermit> {
    grant(
        budget,
        compacted_outcome_data_bytes(limits().event).map_err(|e| (3, e.to_string()))?,
        0,
    )
}
