//! Finite positive producer recovery under exclusive native custody.
use super::*;
use bigbrotr::storage::{
    postgres::{
        control_owner::{ProducerCloseRequest, ProducerResolveRequest},
        ingest_registry,
    },
    rocks::store::{
        AdmissionLimits, BootstrapLimits, IngestRecovery, ProducerRecoveryLimits,
        RetainedRequestInput,
    },
};
use bigbrotr_model::{
    ProducerEpoch, ProducerRequestId, RequestSequence,
    codec::{FactDecodeLimits, request::RequestDecodeLimits},
};
use tokio::sync::oneshot;

mod activated;
mod empty;
pub(super) mod replay;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::interfaces::serve) enum Action {
    Resolve,
    CloseSource,
    AbandonAcquisition,
    RetireInitial,
    RetireEmptyAttempt,
    ActivatedSuccessor,
    CloseActivatedSource,
}

pub(super) fn limits() -> ProducerRecoveryLimits {
    ProducerRecoveryLimits {
        input: RequestDecodeLimits {
            values: FactDecodeLimits {
                max_encoded_bytes: 16 * MIB as usize,
                max_allocation_bytes: 64 * MIB as usize,
                max_tag_nodes: 4 * MIB as usize,
                max_string_bytes: 16 * MIB as usize,
            },
            max_dictionaries: 256,
        },
        bootstrap: BootstrapLimits {
            max_mappings: 256,
            max_batch_bytes: 4 * MIB as usize,
        },
        event: AdmissionLimits {
            max_dependencies: 256,
            ..AdmissionLimits::default()
        },
    }
}

pub(super) fn replay_grants(budget: &ResourceBudget) -> Result<(OwnedPermit, OwnedPermit)> {
    let limits = limits();
    let input = RetainedRequestInput::required_data_bytes(limits.input).map_err(|e| {
        (
            3,
            format!("Ingest recovery input reservation refused: {e:?}"),
        )
    })?;
    let outcome = limits
        .required_data_bytes()
        .map_err(|e| (3, e.to_string()))?;
    Ok((grant(budget, input, 0)?, grant(budget, outcome, 0)?))
}

pub(in crate::interfaces::serve) fn run(
    root: Resolved,
    action: Action,
    output: &mut LifecycleOutput,
) -> Result<()> {
    if !root.local("control") || !root.local("ingest") {
        return Err((
            3,
            "Ingest recovery requires local Control and Ingest".into(),
        ));
    }
    match action {
        Action::ActivatedSuccessor => {
            return activated::run(root, activated::Mode::Resolve, output);
        }
        Action::CloseActivatedSource => {
            return activated::run(root, activated::Mode::CloseSource, output);
        }
        _ => {}
    }
    let budget = budget(&root, false, false)?;
    supervision::run_owned(
        root,
        budget,
        output,
        "Ingest recovery",
        false,
        1,
        move |root, budget, progress, cancellation, driver| {
            own(root, budget, progress, cancellation, driver, action)
        },
    )
}

async fn own(
    root: Resolved,
    budget: ResourceBudget,
    progress: watch::Sender<Progress>,
    cancellation: Arc<Cancellation>,
    driver_custody: oneshot::Sender<(postgres_command::DriverHandle, OwnedPermit)>,
    action: Action,
) -> Result<Value> {
    let path = Path::new(required(&root, "storage.ingest_root")?);
    let dataset: DatasetId = required(&root, "topology.dataset_id")?
        .parse()
        .map_err(|_| (3, "invalid dataset".into()))?;
    let timeout = duration(&root, "renewal_timeout");
    if cancellation.requested.load(Ordering::Acquire) {
        return Err((4, "Ingest recovery cancelled before native open".into()));
    }
    let mut recovery = IngestRecovery::inspect(
        path,
        dataset,
        native_limits(),
        grant(&budget, 0, native_limits().native_allowance_bytes())?,
    )
    .map_err(|e| (4, e.to_string()))?;
    phase(&progress, timeout)?;
    let (prepared, control_grant) = postgres_command::prepare_control_shared(
        &root,
        required(&root, "postgres.control.user")?,
        required(&root, "postgres.control.password_ref")?,
        &budget,
    )?;
    // Check overlapping worst-case replay grants before a claim or barrier.
    // There is only this worker, so later per-request acquisition cannot race
    // another consumer. Never retain a vector of requests or decoded outcomes.
    if action == Action::RetireEmptyAttempt {
        empty::preflight(&budget)?;
    } else {
        drop(replay_grants(&budget)?);
    }
    phase(&progress, timeout)?;
    let (client, connection) = bounded(timeout, &cancellation, async {
        transport::connect(prepared)
            .await
            .map_err(|e| (1, e.to_string()))
    })
    .await?;
    let driver = tokio::spawn(connection);
    if let Err((driver, grant)) = driver_custody.send((driver, control_grant)) {
        drop(recovery);
        drop(client);
        postgres_command::join(driver).await;
        drop(grant);
        return Err((4, "Ingest recovery supervisor unavailable".into()));
    }
    let result = connected(
        &root,
        &budget,
        &progress,
        &cancellation,
        &mut recovery,
        client,
        action,
    )
    .await;
    // The only blocking native destructor stays on this joined owner thread.
    // The independent parent retains the actual driver through that destructor.
    drop(recovery);
    let (owner, report) = result?;
    phase(&progress, duration(&root, "graceful_shutdown_timeout"))?;
    bounded(
        duration(&root, "graceful_shutdown_timeout"),
        &cancellation,
        async { owner.release().await.map_err(owner_error) },
    )
    .await?;
    Ok(report)
}

async fn connected(
    root: &Resolved,
    budget: &ResourceBudget,
    progress: &watch::Sender<Progress>,
    cancellation: &Cancellation,
    recovery: &mut IngestRecovery,
    mut client: tokio_postgres::Client,
    action: Action,
) -> Result<(ControlSession, Value)> {
    if action == Action::RetireEmptyAttempt {
        return empty::connected(root, budget, progress, cancellation, recovery, client).await;
    }
    let instance: DatasetId = required(root, "topology.instance_id")?
        .parse()
        .map_err(|_| (3, "invalid instance".into()))?;
    let timeout = duration(root, "renewal_timeout");
    let path = Path::new(required(root, "storage.ingest_root")?);
    let validate =
        |document: &str, digest| super::super::accepted_policy(root, document, digest).map(|_| ());
    phase(progress, timeout)?;
    let spec = bounded(timeout, cancellation, async {
        ingest_registry::load_historical(&mut client, *instance.as_bytes(), recovery)
            .await
            .map_err(|e| (4, e.to_string()))
    })
    .await?;
    let close_request = || ProducerCloseRequest {
        ingest_root: path,
        registration: &spec,
        policy_ttl: duration(root, "authorization_ttl"),
        cancelled: &cancellation.requested,
    };
    let fresh = || super::super::fresh_process_incarnation().map_err(|e| (1, e.to_string()));
    let request = ClaimRequest {
        dataset: spec.dataset_id,
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
    phase(progress, timeout)?;
    let closed = bounded(timeout, cancellation, async {
        let permit = grant(budget, 4096, 4096)?;
        let result = if action == Action::RetireInitial {
            owner
                .observe_initial_retirement(recovery, close_request(), permit, validate)
                .await
        } else {
            owner
                .close_initial_producer(recovery, close_request(), permit, validate)
                .await
        };
        result.map_err(|e| (4, e.to_string()))
    })
    .await?;
    let issued = closed.issued_through();
    let reserved = closed.reserved_through();
    let mut prefix = if action != Action::Resolve {
        if issued != reserved {
            return Err((4, "source closure has unresolved reserved requests".into()));
        }
        Some(
            closed
                .source_resolution(recovery)
                .map_err(|e| (4, e.to_string()))?,
        )
    } else {
        drop(closed);
        None
    };
    let resolved = replay::originals(
        root,
        budget,
        progress,
        cancellation,
        &mut owner,
        recovery,
        &spec,
        reserved,
        prefix.as_mut(),
        if action == Action::RetireInitial {
            replay::Mode::InspectRetirement
        } else {
            replay::Mode::Resolve
        },
        validate,
    )
    .await?;
    let mut report = json!({"format":"bigbrotr-ingest-recovery/1",
        "state":"positive-outcomes-resolved", "dataset_id":spec.dataset_id,
        "owner_resource_id":spec.ingest_resource_id, "producer_slot_id":spec.producer_slot_id,
        "producer_epoch":"1", "issued_through":issued.to_string(),
        "reserved_through":reserved.to_string(), "resolved_requests":resolved.to_string(),
        "native":"closed", "owner_release":"committed", "ready":false});
    if let Some(prefix) = prefix {
        let source = prefix.finish().map_err(|e| (4, e.to_string()))?;
        phase(progress, timeout)?;
        bounded(timeout, cancellation, async {
            owner.refresh(validate).await.map_err(owner_error)
        })
        .await?;
        phase(progress, timeout)?;
        if action == Action::RetireInitial {
            let gaps = bounded(timeout, cancellation, async {
                owner
                    .retire_initial_producer(
                        &source,
                        close_request(),
                        grant(budget, 128 * 1024, 0)?,
                        validate,
                    )
                    .await
                    .map_err(|e| (4, e.to_string()))
            })
            .await?;
            report["ingest_retirement"] = json!({
                "allocation_domains":spec.allocation_domains.len().to_string(),
                "acquisition_gaps":gaps.to_string(), "owner_release":"committed",
                "native_acknowledged":"0", "native_retired":"0"});
        } else {
            bounded(timeout, cancellation, async {
                owner
                    .close_initial_source(
                        &source,
                        close_request(),
                        grant(budget, 64 * 1024, 0)?,
                        validate,
                    )
                    .await
                    .map_err(|e| (4, e.to_string()))
            })
            .await?;
        }
        if action == Action::AbandonAcquisition {
            phase(progress, timeout)?;
            bounded(timeout, cancellation, async {
                owner.refresh(validate).await.map_err(owner_error)
            })
            .await?;
            phase(progress, timeout)?;
            let abandoned = bounded(timeout, cancellation, async {
                owner
                    .abandon_initial_acquisition(
                        &source,
                        close_request(),
                        grant(budget, 128 * 1024, 0)?,
                        validate,
                    )
                    .await
                    .map_err(|e| (4, e.to_string()))
            })
            .await?;
            report["acquisition_abandonment"] = json!({
                "jobs":abandoned.to_string(), "reason":"initial_producer_abandoned",
                "coverage":"unknown"});
        }
        report["state"] = json!(match action {
            Action::RetireInitial => "initial-retired",
            Action::AbandonAcquisition => "acquisition-abandoned",
            Action::CloseSource | Action::Resolve => "source-closed",
            Action::RetireEmptyAttempt
            | Action::ActivatedSuccessor
            | Action::CloseActivatedSource => {
                unreachable!("separate successor recovery")
            }
        });
        report["source_closure"] = json!({"last_complete_sequence":source.last_complete_sequence().to_string(),
            "sha256":bigbrotr_model::Digest256::from_bytes(source.digest())});
    }
    Ok((owner, report))
}
