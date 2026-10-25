//! Installed continuous succession from a positively verified terminal lineage.
//! Historical inspection never adopts a candidate or recreates a live writer.
use super::*;
use bigbrotr::storage::{
    postgres::control_owner::{
        InitialHistoryRequest, PolicySnapshot, SuccessorActivation, SuccessorHistory,
        SuccessorHistoryState, SuccessorPreparation, SuccessorPreparationPlan,
        SuccessorPreparationRequest, SuccessorReplayRequest,
    },
    rocks::{
        store::{
            IngestRecovery, LineageEpochSummary, SuccessorCandidate, compacted_outcome_data_bytes,
        },
        succession::MAX_LINEAGE_METADATA_BYTES,
    },
};
use std::time::Duration;

// All startup native envelopes precede the client/Control fields in drop order.
// They stay here while bounded futures only borrow them, including on unknown
// preparation/activation COMMIT, cancellation, deadline or panic.
#[derive(Default)]
struct Startup {
    candidate: Option<SuccessorCandidate>,
    history: Option<SuccessorHistory>,
    preparation: Option<SuccessorPreparation>,
    activation: Option<SuccessorActivation>,
    client: Option<tokio_postgres::Client>,
    owner: Option<ControlSession>,
}

pub(in crate::interfaces::serve) fn run(
    root: Resolved,
    sync: bool,
    output: &mut LifecycleOutput,
) -> Result<()> {
    selected_roots(&root)?;
    // Metadata survives into the live frontier. Preparation's additional grant
    // overlaps it until the infallible activation-envelope transfer.
    let budget = owner_budget(&root, sync, false, 3)?;
    supervision::run_owned(
        root,
        budget,
        output,
        "Ingest successor",
        true,
        3,
        move |root, budget, progress, cancellation, driver| {
            own(root, budget, progress, cancellation, driver, sync)
        },
    )
}

async fn own(
    root: Resolved,
    budget: ResourceBudget,
    progress: watch::Sender<Progress>,
    cancellation: Arc<Cancellation>,
    driver_custody: supervision::DriverCustody,
    selected_sync: bool,
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
    // The existing input-memory allowance is separate from the persistent
    // 64-file/32MiB spool bound. Succession reclaims neither input nor history.
    let _inputs = grant(&budget, 24 * MIB, 0)?;
    let metadata = grant(&budget, MAX_LINEAGE_METADATA_BYTES, 0)?;
    let mutation = grant(&budget, 8192, 8192)?;
    if cancellation.requested.load(Ordering::Acquire) {
        return Err((4, "successor cancelled before native open".into()));
    }
    let recovery = IngestRecovery::inspect(
        path,
        dataset,
        native_limits(),
        grant(&budget, 0, native_limits().native_allowance_bytes())?,
    )
    .map_err(error)?;
    // Construction can fail and consume recovery, so it precedes Control.
    // All identities are minted internally; no recorded attempt is adopted.
    let mut startup = Startup {
        candidate: Some(SuccessorCandidate::new(recovery).map_err(error)?),
        ..Startup::default()
    };
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
    // Admit the real overlaps before claiming Control or preparing the target.
    // Input/outcome replay and final effect census are serial; neither overlaps
    // live HTTP/Sync work. No receipt vector or second lineage is allocated.
    // The compacted Check remains live alongside every later request/Sync
    // grant. Admit that overlap now, before Control or any SQL preparation.
    let live_effects = compacted_effect_grant(&budget)?;
    drop(recovery::replay_grants(&budget)?);
    drop(effect_grant(&budget)?);
    if selected_sync {
        sync::preflight(&budget)?;
    }
    drop(live_effects);
    phase(&progress, timeout)?;
    let (client, connection) = bounded(timeout, &cancellation, async {
        transport::connect(prepared)
            .await
            .map_err(|e| (1, e.to_string()))
    })
    .await?;
    startup.client = Some(client);
    let driver = tokio::spawn(connection);
    if let Err((driver, grant)) = driver_custody.send((driver, control_grant)) {
        drop(startup);
        postgres_command::join(driver).await;
        drop(grant);
        return Err((4, "successor supervisor unavailable".into()));
    }
    let fresh = || super::super::fresh_process_incarnation().map_err(|e| (1, e.to_string()));
    let request = ClaimRequest {
        dataset,
        instance,
        authority_mode: required(&root, "config.authority_mode")?.into(),
        role_instance_id: *fresh()?.as_bytes(),
        incarnation: fresh()?,
        lease_ttl: duration(&root, "lease_ttl"),
    };
    let mut window = 64_u32;
    phase(&progress, timeout)?;
    let policy = {
        let commit_started = Cell::new(false);
        let acquisition = ControlSession::acquire(
            startup.client.take().expect("connected client"),
            request,
            |document, digest| {
                let accepted = super::super::accepted_policy(&root, document, digest)?;
                accepted_owner_profile(&accepted, Profile::Successor)?;
                if let Some(sync) = &mut sync {
                    sync.accept(&accepted)?;
                }
                window = window.min(
                    accepted
                        .number("ingest.producer_outstanding_requests")
                        .min(64) as u32,
                );
                if u64::from(window) < Profile::Successor.minimum(selected_sync) {
                    return Err(OwnerError::InvalidPolicy);
                }
                Ok(())
            },
            &commit_started,
        );
        tokio::pin!(acquisition);
        let mut claim_exit = supervision::ClaimExit(Some(&mut startup.candidate));
        let (owner, policy) = bounded(timeout, &cancellation, async {
            acquisition.as_mut().await.map_err(owner_error)
        })
        .await?;
        startup.owner = Some(owner);
        claim_exit.0 = None;
        policy
    };
    let work = Work {
        root: &root,
        path,
        budget: &budget,
        progress: &progress,
        cancellation: &cancellation,
        timeout,
        ttl,
        policy,
    };
    let owner = startup.owner.as_mut().expect("claimed Control");
    let candidate = startup.candidate.as_ref().expect("fresh candidate");
    startup.history = Some(
        work.operation(async {
            owner
                .inspect_successor_history_with_compacted(
                    candidate.recovery(),
                    work.request(),
                    recovery::limits().event,
                    metadata,
                    compacted_effect_grant(&budget)?,
                    |d, h| work.validate(d, h),
                )
                .await
                .map_err(error)
        })
        .await?,
    );
    let history = startup.history.as_ref().expect("retained history");
    if history.state() != SuccessorHistoryState::Terminal {
        return Err((
            4,
            "successor activation requires a fully terminal predecessor".into(),
        ));
    }
    let (plan, predecessor, compacted) = work.plan(owner, candidate, history, window).await?;
    let plan_policy = plan.policy();
    let created = plan.created_at_us();
    let deadline = plan.lease_expires_at_us();
    startup.preparation = Some(SuccessorPreparation::new(
        startup
            .candidate
            .take()
            .expect("continuously retained candidate"),
        startup.history.take().expect("retained history"),
        plan,
    ));
    if plan_policy != policy {
        return Err((
            4,
            "accepted policy changed before successor preparation".into(),
        ));
    }
    work.operation(async {
        let preparation = startup.preparation.as_mut().expect("retained preparation");
        if compacted.is_some() {
            owner
                .prepare_compacted_successor(preparation, work.request(), mutation, |d, h| {
                    work.validate(d, h)
                })
                .await
                .map_err(error)
        } else {
            owner
                .prepare_successor(preparation, work.request(), mutation, |d, h| {
                    work.validate(d, h)
                })
                .await
                .map_err(error)
        }
    })
    .await?;
    // This move is infallible. Every subsequent failure still leaves native
    // custody ahead of Control; only successful activation transfers a writer.
    startup.activation = Some(SuccessorActivation::new(
        startup.preparation.take().expect("confirmed preparation"),
    ));
    let writer = work
        .operation(async {
            let activation = startup.activation.as_mut().expect("retained activation");
            if compacted.is_some() {
                owner
                    .activate_compacted_successor_writer(activation, work.request(), |d, h| {
                        work.validate(d, h)
                    })
                    .await
                    .map_err(error)
            } else {
                owner
                    .activate_successor_writer(activation, work.request(), |d, h| {
                        work.validate(d, h)
                    })
                    .await
                    .map_err(error)
            }
        })
        .await?;
    let id = *writer.manifest().identity();
    let slot = writer.producer_slot_id();
    let mut report = json!({"format":"bigbrotr-ingest-successor/1","state":"activated",
        "dataset_id":id.dataset_id,"owner_resource_id":id.owner_resource_id,
        "producer_slot_id":slot,"producer_epoch":id.owner_epoch.get().to_string(),
        "process_incarnation":id.process_incarnation,
        "role_instance_id":format!("{:032x}", u128::from_be_bytes(writer.role_instance_id())),
        "activated_at_us":writer.activated_at_us().to_string(),
        "prepared_at_us":created.to_string(),"lease_expires_at_us":deadline.to_string(),
        "config_revision":policy.config_revision.to_string(),"policy_revision":policy.policy_revision.to_string(),
        "outstanding_request_limit":window.to_string(),
        "predecessor":{"producer_epoch":predecessor.epoch.to_string(),
            "resolved_requests":predecessor.resolved_requests.to_string(),
            "last_complete_sequence":predecessor.last_complete_sequence.to_string(),
            "sha256":Digest256::from_bytes(predecessor.digest)},
        "acquisition":if selected_sync {"finite-successor"} else {"not-started"},"ready":false});
    if let Some(count) = compacted {
        report["recovery_basis"] = json!("closed-producer-floor-v3");
        report["compacted_requests"] = json!(count.to_string());
    }
    let mut custody = Custody {
        store: Some(NativeOwner::Successor(Box::new(writer))),
        sync,
        archive: None,
        owner: startup.owner.take().expect("same activating Control"),
    };
    drop(startup);
    commands::maintain_successor(
        &mut custody,
        &root,
        &budget,
        &progress,
        &cancellation,
        report,
    )
    .await?;
    let grace = duration(&root, "graceful_shutdown_timeout");
    phase(&progress, grace)?;
    drop(custody.store.take());
    // A requested graceful stop must still release Control after native closes.
    // The parent retains its driver/grant through the actual joined result.
    tokio::time::timeout(grace, custody.owner.release())
        .await
        .map_err(|_| {
            (
                4,
                "Control release deadline after successor native close".into(),
            )
        })?
        .map_err(owner_error)?;
    Ok(
        json!({"format":"bigbrotr-ingest-successor/1","state":"stopped",
        "dataset_id":id.dataset_id,"owner_resource_id":id.owner_resource_id,
        "producer_slot_id":slot,"producer_epoch":id.owner_epoch.get().to_string(),
        "native":"closed","owner_release":"committed","source_closure":"not-performed","ready":false}),
    )
}

struct Work<'a> {
    root: &'a Resolved,
    path: &'a Path,
    budget: &'a ResourceBudget,
    progress: &'a watch::Sender<Progress>,
    cancellation: &'a Cancellation,
    timeout: Duration,
    ttl: Duration,
    policy: PolicySnapshot,
}
impl Work<'_> {
    fn request(&self) -> InitialHistoryRequest<'_> {
        InitialHistoryRequest {
            ingest_root: self.path,
            policy_ttl: self.ttl,
            cancelled: &self.cancellation.requested,
        }
    }
    fn validate(&self, document: &str, digest: [u8; 32]) -> std::result::Result<(), OwnerError> {
        validate_owner(self.root, Profile::Successor, document, digest)
    }
    async fn operation<T>(&self, future: impl Future<Output = Result<T>>) -> Result<T> {
        phase(self.progress, self.timeout)?;
        bounded(self.timeout, self.cancellation, future).await
    }
    async fn refresh(&self, owner: &mut ControlSession) -> Result<()> {
        let policy = self
            .operation(async {
                owner
                    .refresh(|d, h| self.validate(d, h))
                    .await
                    .map_err(owner_error)
            })
            .await?;
        if policy != self.policy {
            return Err((
                4,
                "accepted policy changed during successor history replay".into(),
            ));
        }
        Ok(())
    }
    async fn plan(
        &self,
        owner: &mut ControlSession,
        candidate: &SuccessorCandidate,
        history: &SuccessorHistory,
        window: u32,
    ) -> Result<(SuccessorPreparationPlan, LineageEpochSummary, Option<u64>)> {
        let mut resolution = history
            .resolution(candidate.recovery(), &self.cancellation.requested)
            .map_err(error)?;
        while resolution.current_epoch().is_some() {
            if resolution.needs_compacted_initial() {
                self.refresh(owner).await?;
                self.operation(async {
                    owner
                        .inspect_successor_compacted_initial(
                            &mut resolution,
                            self.request(),
                            recovery::limits().event,
                            compacted_effect_grant(self.budget)?,
                            |d, h| self.validate(d, h),
                        )
                        .await
                        .map_err(error)
                })
                .await?;
                continue;
            }
            while resolution.next_request().is_some() {
                self.refresh(owner).await?;
                let (input, outcome) = recovery::replay_grants(self.budget)?;
                self.operation(async {
                    owner
                        .inspect_next_successor_request(
                            &mut resolution,
                            SuccessorReplayRequest {
                                ingest_root: self.path,
                                limits: recovery::limits(),
                                policy_ttl: self.ttl,
                                cancelled: &self.cancellation.requested,
                            },
                            input,
                            outcome,
                            |d, h| self.validate(d, h),
                        )
                        .await
                        .map_err(error)
                })
                .await?;
            }
            self.refresh(owner).await?;
            self.operation(async {
                owner
                    .finish_successor_history_epoch(&mut resolution, self.request(), |d, h| {
                        self.validate(d, h)
                    })
                    .await
                    .map_err(error)
            })
            .await?;
        }
        self.refresh(owner).await?;
        let proof = self
            .operation(async {
                owner
                    .finish_successor_history(
                        resolution,
                        self.request(),
                        recovery::limits().event,
                        if history.lineage().has_compacted_initial() {
                            compacted_effect_grant(self.budget)?
                        } else {
                            effect_grant(self.budget)?
                        },
                        |d, h| self.validate(d, h),
                    )
                    .await
                    .map_err(error)
            })
            .await?;
        let predecessor = proof.last_epoch();
        let compacted = history
            .lineage()
            .has_compacted_initial()
            .then(|| proof.compacted_initial_requests());
        let plan = self
            .operation(async {
                let input = SuccessorPreparationRequest {
                    history: self.request(),
                    lease_duration: self.ttl,
                    outstanding_request_limit: window,
                };
                if compacted.is_some() {
                    owner
                        .plan_compacted_successor_preparation(
                            candidate,
                            &proof,
                            input,
                            recovery::limits().event,
                            compacted_effect_grant(self.budget)?,
                            |d, h| self.validate(d, h),
                        )
                        .await
                        .map_err(error)
                } else {
                    owner
                        .plan_successor_preparation(candidate, &proof, input, |d, h| {
                            self.validate(d, h)
                        })
                        .await
                        .map_err(error)
                }
            })
            .await?;
        Ok((plan, predecessor, compacted))
    }
}

fn compacted_effect_grant(budget: &ResourceBudget) -> Result<OwnedPermit> {
    grant(
        budget,
        compacted_outcome_data_bytes(recovery::limits().event).map_err(error)?,
        0,
    )
}

fn effect_grant(budget: &ResourceBudget) -> Result<OwnedPermit> {
    grant(
        budget,
        recovery::limits().required_data_bytes().map_err(error)?,
        0,
    )
}
fn error(error: impl std::fmt::Display) -> (u8, String) {
    (4, format!("successor operation unavailable: {error}"))
}
