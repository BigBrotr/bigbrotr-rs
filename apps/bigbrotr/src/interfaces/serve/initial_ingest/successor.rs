//! Explicit installed 1→2 lifetime; every native handle stays on this worker.
use super::*;
use bigbrotr::storage::{
    postgres::{
        control_owner::{
            CompactedInitialRetirement, InitialHistoryRequest, InitialSuccessionRequest,
            ProducerCloseRequest,
        },
        ingest_registry,
    },
    rocks::store::{IngestRecovery, InitialWriterCandidate, compacted_outcome_data_bytes},
};

pub(in crate::interfaces::serve) fn run(
    root: Resolved,
    sync: bool,
    index: bool,
    output: &mut LifecycleOutput,
) -> Result<()> {
    selected_roots(&root)?;
    // One additional retained activation metadata grant remains with the live
    // capability throughout every Sync/HTTP operation and native destruction.
    let budget = owner_budget(&root, sync, false, 1 + if index { index::ITEMS } else { 0 })?;
    supervision::run_owned(
        root,
        budget,
        output,
        "initial successor",
        true,
        2,
        move |root, budget, progress, cancellation, driver| {
            own(root, budget, progress, cancellation, driver, sync, index)
        },
    )
}

/// Also protects errors/panic before claiming Control. The driver and its grant
/// are independently owned by the parent once a transport has been created.
struct Unclaimed {
    index: Option<index::Index>,
    native: Option<NativeOwner>,
    client: Option<tokio_postgres::Client>,
}

async fn own(
    root: Resolved,
    budget: ResourceBudget,
    progress: watch::Sender<Progress>,
    cancellation: Arc<Cancellation>,
    driver_custody: supervision::DriverCustody,
    selected_sync: bool,
    selected_index: bool,
) -> Result<Value> {
    let path = Path::new(required(&root, "storage.ingest_root")?);
    let dataset: DatasetId = required(&root, "topology.dataset_id")?
        .parse()
        .map_err(|_| (3, "invalid dataset".into()))?;
    let instance: DatasetId = required(&root, "topology.instance_id")?
        .parse()
        .map_err(|_| (3, "invalid instance".into()))?;
    let timeout = duration(&root, "renewal_timeout");
    let ttl = duration(&root, "authorization_ttl");
    let _inputs = grant(&budget, 24 * MIB, 0)?;
    let activation = grant(&budget, 128 * 1024, 8192)?;
    if cancellation.requested.load(Ordering::Acquire) {
        return Err((4, "initial successor cancelled before native open".into()));
    }
    let recovery = IngestRecovery::inspect(
        path,
        dataset,
        native_limits(),
        grant(&budget, 0, native_limits().native_allowance_bytes())?,
    )
    .map_err(|e| (4, e.to_string()))?;
    if recovery.manifest().identity().owner_epoch.get() != 1 {
        return Err((
            4,
            "initial successor requires native epoch1; retained later state is not adopted".into(),
        ));
    }
    let candidate = InitialWriterCandidate::new(recovery).map_err(|e| (4, e.to_string()))?;
    let index_driver = IndexDriverCustody::new();
    let mut unclaimed = Unclaimed {
        index: None,
        native: Some(NativeOwner::Candidate(Box::new(candidate))),
        client: None,
    };
    // Retain the existing Index journal before opening either credential.
    unclaimed.index = selected_index
        .then(|| index::Index::retain(&root, &budget, &index_driver))
        .transpose()?;
    phase(&progress, timeout)?;
    let (prepared, control_grant) = postgres_command::prepare_control_shared(
        &root,
        required(&root, "postgres.control.user")?,
        required(&root, "postgres.control.password_ref")?,
        &budget,
    )?;
    let mut sync = selected_sync
        .then(|| sync::Sync::prepare(&root, &budget, Profile::Successor))
        .transpose()?;
    if selected_sync {
        sync::preflight(&budget)?;
    }
    phase(&progress, timeout)?;
    let (client, connection) = bounded(timeout, &cancellation, async {
        transport::connect(prepared)
            .await
            .map_err(|e| (1, e.to_string()))
    })
    .await?;
    unclaimed.client = Some(client);
    let driver = tokio::spawn(index_driver.retain_connection(connection));
    if let Err((driver, grant)) = driver_custody.send((driver, control_grant)) {
        drop(unclaimed);
        postgres_command::join(driver).await;
        drop(grant);
        return Err((4, "initial successor supervisor unavailable".into()));
    }
    let NativeOwner::Candidate(candidate) = unclaimed.native.as_ref().expect("native owner") else {
        unreachable!("only candidate before claim")
    };
    phase(&progress, timeout)?;
    let spec = bounded(timeout, &cancellation, async {
        ingest_registry::load_historical(
            unclaimed.client.as_mut().expect("connected client"),
            *instance.as_bytes(),
            candidate.recovery(),
        )
        .await
        .map_err(|e| (4, e.to_string()))
    })
    .await?;
    // Select only the actually inspected floor. Reserve its complete native
    // census/SQL outcome overlap before claiming Control, without reserving an
    // original-input replay that this branch cannot perform.
    let compacted_prefix = recovery::compacted::inspect(
        &budget,
        candidate.recovery(),
        &spec,
        &cancellation.requested,
    )?;
    let compacted = compacted_prefix.is_some();

    {
        let _preparation = grant(&budget, 128 * 1024, 8192)?;
        if compacted {
            drop(grant(
                &budget,
                compacted_outcome_data_bytes(recovery::limits().event)
                    .map_err(|e| (3, e.to_string()))?,
                0,
            )?);
        } else {
            let _source = grant(&budget, 4096, 4096)?;
            drop(recovery::replay_grants(&budget)?);
        }
    }
    let fresh = || super::super::fresh_process_incarnation().map_err(|e| (1, e.to_string()));
    let claim = ClaimRequest {
        dataset,
        instance,
        authority_mode: required(&root, "config.authority_mode")?.into(),
        role_instance_id: *fresh()?.as_bytes(),
        incarnation: fresh()?,
        lease_ttl: duration(&root, "lease_ttl"),
    };
    let mut outstanding = u64::MAX;
    phase(&progress, timeout)?;
    let (owner, accepted) = bounded(timeout, &cancellation, async {
        ControlSession::acquire(
            unclaimed.client.take().expect("unclaimed client"),
            claim,
            |doc, digest| {
                let restored = super::super::accepted_policy(&root, doc, digest)?;
                accepted_owner_profile(&restored, Profile::Successor)?;
                if let Some(sync) = &mut sync {
                    sync.accept(&restored)?;
                }
                if let Some(index) = &mut unclaimed.index {
                    index.accept(&restored)?;
                }
                outstanding =
                    outstanding.min(restored.number("ingest.producer_outstanding_requests"));
                Ok(())
            },
            &Cell::new(false),
        )
        .await
        .map_err(owner_error)
    })
    .await?;
    let mut custody = Custody {
        index: unclaimed.index.take(),
        store: unclaimed.native.take(),
        sync,
        archive: None,
        owner,
    };
    let window = u32::try_from(outstanding)
        .map_err(|_| (3, "successor producer window unavailable".into()))?;
    let validate = |doc: &str, digest| validate_owner(&root, Profile::Successor, doc, digest);
    let NativeOwner::Candidate(candidate) = custody.store.as_ref().expect("native owner") else {
        unreachable!("candidate retained before preparation")
    };
    let (handoff, predecessor, basis) = if let Some(prefix) = &compacted_prefix {
        phase(&progress, timeout)?;
        let limits = recovery::limits().event;
        let outcome_bytes = compacted_outcome_data_bytes(limits).map_err(|e| (3, e.to_string()))?;
        let retired = bounded(timeout, &cancellation, async {
            custody
                .owner
                .inspect_initial_compacted_retirement(
                    candidate.recovery(),
                    prefix,
                    ProducerCloseRequest {
                        ingest_root: path,
                        registration: &spec,
                        policy_ttl: ttl,
                        cancelled: &cancellation.requested,
                    },
                    limits,
                    grant(&budget, CompactedInitialRetirement::REQUIRED_DATA_BYTES, 0)?,
                    grant(&budget, outcome_bytes, 0)?,
                    validate,
                )
                .await
                .map_err(|e| (4, e.to_string()))
        })
        .await?;
        phase(&progress, timeout)?;
        let basis = bounded(timeout, &cancellation, async {
            custody
                .owner
                .initial_successor_basis(&spec, validate)
                .await
                .map_err(owner_error)
        })
        .await?;
        if basis.policy != accepted {
            return Err((4, "accepted policy changed during successor startup".into()));
        }
        let handoff = retired
            .handoff_candidate(
                candidate,
                PolicyEpoch::new(basis.policy.policy_revision),
                &cancellation.requested,
            )
            .map_err(|e| (4, e.to_string()))?;
        let predecessor = json!({"producer_epoch":"1", "issued_through":retired.issued_through().to_string(),
            "reserved_through":retired.issued_through().to_string(), "resolved_requests":"0",
            "compacted_requests":retired.retired_through().to_string(), "recovery_basis":"closed-producer-floor-v3",
            "last_complete_sequence":retired.last_complete_sequence().to_string(),
            "sha256":Digest256::from_bytes(retired.digest()).to_string()});
        (handoff, predecessor, basis)
    } else {
        phase(&progress, timeout)?;
        let closed = bounded(timeout, &cancellation, async {
            custody
                .owner
                .observe_initial_retirement(
                    candidate.recovery(),
                    ProducerCloseRequest {
                        ingest_root: path,
                        registration: &spec,
                        policy_ttl: ttl,
                        cancelled: &cancellation.requested,
                    },
                    grant(&budget, 4096, 4096)?,
                    validate,
                )
                .await
                .map_err(|e| (4, e.to_string()))
        })
        .await?;
        let issued = closed.issued_through();
        let reserved = closed.reserved_through();
        if issued != reserved {
            return Err((
                4,
                "successor predecessor has unresolved reserved requests".into(),
            ));
        }
        let mut prefix = closed
            .source_resolution(candidate.recovery())
            .map_err(|e| (4, e.to_string()))?;
        let resolved = recovery::replay::originals(
            &root,
            &budget,
            &progress,
            &cancellation,
            &mut custody.owner,
            candidate.recovery(),
            &spec,
            reserved,
            Some(recovery::replay::Prefix::Source(&mut prefix)),
            recovery::replay::Mode::InspectRetirement,
            validate,
        )
        .await?;
        let source = prefix.finish().map_err(|e| (4, e.to_string()))?;
        phase(&progress, timeout)?;
        let basis = bounded(timeout, &cancellation, async {
            custody
                .owner
                .initial_successor_basis(&spec, validate)
                .await
                .map_err(owner_error)
        })
        .await?;
        if basis.policy != accepted {
            return Err((4, "accepted policy changed during successor startup".into()));
        }
        let predecessor = json!({"producer_epoch":"1", "issued_through":issued.to_string(),
        "reserved_through":reserved.to_string(), "resolved_requests":resolved.to_string(),
        "last_complete_sequence":source.last_complete_sequence().to_string(),
        "sha256":Digest256::from_bytes(source.digest()).to_string()});
        let handoff = candidate
            .handoff(&source, PolicyEpoch::new(basis.policy.policy_revision))
            .map_err(|e| (4, e.to_string()))?;
        drop(source);
        (handoff, predecessor, basis)
    };
    drop(compacted_prefix);
    if let Some(index) = custody.index.as_ref() {
        phase(&progress, timeout)?;
        bounded(
            timeout,
            &cancellation,
            index.preflight_live_predecessor(&mut custody.owner, &handoff),
        )
        .await?;
    }
    let NativeOwner::Candidate(candidate) = custody.store.as_mut().expect("native owner") else {
        unreachable!("candidate retained before preparation")
    };
    let role_instance_id = candidate.role_instance_id();
    phase(&progress, timeout)?;
    drop(
        bounded(timeout, &cancellation, async {
            let input = InitialSuccessionRequest {
                ingest_root: path,
                previous_registration: &spec,
                handoff: &handoff,
                role_instance_id,
                created_at_us: basis.created_at_us,
                lease_expires_at_us: basis.lease_expires_at_us,
                outstanding_request_limit: window,
                policy_ttl: ttl,
                cancelled: &cancellation.requested,
            };
            let metadata = grant(&budget, 128 * 1024, 8192)?;
            let result = if compacted {
                let limits = recovery::limits().event;
                let bytes = compacted_outcome_data_bytes(limits).map_err(|e| (3, e.to_string()))?;
                custody
                    .owner
                    .prepare_live_compacted_initial_successor(
                        candidate,
                        input,
                        limits,
                        metadata,
                        grant(&budget, bytes, 0)?,
                        validate,
                    )
                    .await
            } else {
                custody
                    .owner
                    .prepare_live_initial_successor(candidate, input, metadata, validate)
                    .await
            };
            result.map_err(|e| (4, e.to_string()))
        })
        .await?,
    );
    phase(&progress, timeout)?;
    let NativeOwner::Candidate(candidate) = custody.store.take().expect("native owner") else {
        unreachable!("activation consumes candidate")
    };
    let writer = bounded(timeout, &cancellation, async {
        let input = InitialHistoryRequest {
            ingest_root: path,
            policy_ttl: ttl,
            cancelled: &cancellation.requested,
        };
        let result = if compacted {
            let limits = recovery::limits().event;
            let bytes = compacted_outcome_data_bytes(limits).map_err(|e| (3, e.to_string()))?;
            custody
                .owner
                .activate_compacted_initial_writer(
                    *candidate,
                    handoff,
                    input,
                    limits,
                    activation,
                    grant(&budget, bytes, 0)?,
                    validate,
                )
                .await
        } else {
            custody
                .owner
                .activate_initial_writer(*candidate, handoff, input, activation, validate)
                .await
        };
        result.map_err(|e| (4, e.to_string()))
    })
    .await?;
    custody.store = Some(NativeOwner::Live(Box::new(writer)));
    let NativeOwner::Live(writer) = custody.store.as_ref().expect("activated writer") else {
        unreachable!()
    };
    let index_report = if let Some(index) = custody.index.as_mut() {
        Some(
            index
                .recover_live_initial(&root, &progress, &cancellation, &mut custody.owner, writer)
                .await?,
        )
    } else {
        None
    };
    let id = writer.manifest().identity();
    let policy = writer.policy_snapshot();
    let mut report = json!({"format":"bigbrotr-initial-successor/1", "state":"activated",
        "dataset_id":id.dataset_id, "owner_resource_id":id.owner_resource_id,
        "producer_slot_id":writer.producer_slot_id(), "producer_epoch":"2",
        "process_incarnation":id.process_incarnation, "topology_generation":id.topology_generation.get().to_string(),
        "config_revision":policy.config_revision.to_string(), "policy_revision":policy.policy_revision.to_string(),
        "config_sha256":Digest256::from_bytes(policy.config_digest).to_string(),
        "policy_sha256":Digest256::from_bytes(policy.policy_digest).to_string(),
        "outstanding_request_limit":writer.outstanding_request_limit().to_string(),
        "created_at_us":basis.created_at_us.to_string(), "initial_lease_expires_at_us":basis.lease_expires_at_us.to_string(),
        "activated_at_us":writer.activated_at_us().to_string(), "predecessor":predecessor,
        "native_profile":1, "native_allowance_bytes":128*MIB,
        "acquisition":if selected_sync { "finite-initial-successor" } else { "not-started" }, "ready":false});
    if let Some(index) = index_report {
        report["index"] = index;
    }
    commands::maintain(
        &mut custody,
        &spec,
        &root,
        &budget,
        &progress,
        &cancellation,
        report,
    )
    .await?;
    phase(&progress, duration(&root, "graceful_shutdown_timeout"))?;
    drop(custody.index.take());
    drop(custody.store.take());
    tokio::time::timeout(
        duration(&root, "graceful_shutdown_timeout"),
        custody.owner.release(),
    )
    .await
    .map_err(|_| (4, "Control release deadline after native close".into()))?
    .map_err(owner_error)?;
    Ok(
        json!({"format":"bigbrotr-initial-successor/1", "state":"stopped", "native":"closed",
        "owner_release":"committed", "source_closure":"not-performed", "producer_epoch":"2", "ready":false}),
    )
}
