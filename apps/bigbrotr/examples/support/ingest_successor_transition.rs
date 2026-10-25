//! Finite actual-Control successor preparation and unactivated recovery.
//! Each invocation owns one native lifetime. The explicit live child retains
//! its candidate through activation; recovery commands never adopt one.
use super::*;
use bigbrotr::storage::{
    postgres::control_owner::{
        SuccessorEmptyPlan, SuccessorGapPlan, SuccessorPreparation, SuccessorPreparationRequest,
    },
    rocks::{store::SuccessorCandidate, succession::SuccessionRecord},
};

#[path = "ingest_successor_acquisition.rs"]
mod acquisition;
#[path = "ingest_successor_live.rs"]
mod live;
#[path = "ingest_successor_recovery.rs"]
mod recovery;
#[path = "ingest_successor_retirement.rs"]
mod retirement;
#[path = "ingest_successor_retirement_wait.rs"]
mod retirement_wait;
#[path = "ingest_successor_source.rs"]
mod source;

// Keep every possible native owner before Control in the custody drop order.
// In particular, failed/unknown prepare leaves the spent Preparation here.
struct TransitionCustody {
    native: Option<Native>,
    owner: Option<ControlSession>,
}
struct Native {
    recovery: Option<IngestRecovery>,
    candidate: Option<SuccessorCandidate>,
    preparation: Option<SuccessorPreparation>,
    history: Option<SuccessorHistory>,
    gap: Option<SuccessorGapPlan>,
    empty: Option<SuccessorEmptyPlan>,
    live: live::LiveCustody,
    activated_recovery: Option<recovery::ActivatedSuccessorRecovery>,
    source_recovery: Option<source::ActivatedSuccessorSourceRecovery>,
    retirement: Option<retirement::ActivatedSuccessorRetirement>,
}
impl Native {
    fn recovery(&self) -> &IngestRecovery {
        if let Some(candidate) = &self.candidate {
            candidate.recovery()
        } else if let Some(preparation) = &self.preparation {
            preparation.recovery()
        } else {
            self.recovery.as_ref().expect("one retained native owner")
        }
    }
}

pub async fn run(
    config: Config,
    config_file: &Path,
    metadata_bytes: u64,
    command: &str,
    proxy_port: Option<u16>,
) {
    run_inner(
        config,
        config_file,
        metadata_bytes,
        command,
        proxy_port,
        None,
    )
    .await;
}

pub async fn run_retirement_wait(
    config: Config,
    config_file: &Path,
    metadata_bytes: u64,
    mode: &str,
    prefix: &Path,
) {
    let wait = retirement_wait::Wait::new(mode, prefix);
    run_inner(
        config,
        config_file,
        metadata_bytes,
        "successor-lineage-retire",
        None,
        Some(wait),
    )
    .await;
}

async fn run_inner(
    config: Config,
    config_file: &Path,
    metadata_bytes: u64,
    command: &str,
    proxy_port: Option<u16>,
    retirement_wait: Option<retirement_wait::Wait>,
) {
    assert!(metadata_bytes <= MAX_LINEAGE_METADATA_BYTES);
    let compacted = matches!(
        command,
        "successor-compacted-prepare"
            | "successor-compacted-gap"
            | "successor-compacted-empty"
            | "successor-compacted-empty-retry"
            | "successor-compacted-live"
            | "successor-compacted-acquisition-claims"
    );
    let operation = match command {
        "successor-prepare" | "successor-compacted-prepare" => "prepare",
        "successor-empty" | "successor-compacted-empty" => "empty",
        "successor-empty-retry" | "successor-compacted-empty-retry" => "empty-retry",
        "successor-gap" | "successor-compacted-gap" => "gap",
        "successor-lineage-live" => "live",
        "successor-lineage-live-new" | "successor-compacted-live" => "live-new",
        "successor-lineage-recover" => "recover",
        "successor-lineage-source" => "source",
        "successor-lineage-abandon" => "abandon",
        "successor-lineage-acquisition-claims" | "successor-compacted-acquisition-claims" => {
            "acquisition-claims"
        }
        "successor-lineage-retire" => "retire",
        _ => panic!("unknown finite successor operation"),
    };
    assert!(
        proxy_port.is_none()
            || matches!(
                operation,
                "prepare" | "empty" | "live" | "live-new" | "retire"
            )
    );
    assert!(retirement_wait.is_none() || (operation == "retire" && proxy_port.is_none()));
    assert!(proxy_port.is_none_or(|port| port != 0));
    let mutation_bytes = match operation {
        "prepare" | "gap" | "live" | "live-new" | "acquisition-claims" => 8192,
        "empty" | "recover" | "source" | "abandon" | "retire" => 4096,
        _ => 0,
    };
    let root =
        crate::config::resolve::parse(&std::fs::read_to_string(config_file).unwrap()).unwrap();
    assert_eq!(config.get_dbname(), root.text("postgres.control.database"));
    assert!(root.local("control") && root.local("ingest"));
    let path = PathBuf::from(root.text("storage.ingest_root").unwrap());
    let dataset: DatasetId = root.text("topology.dataset_id").unwrap().parse().unwrap();
    let instance =
        crate::ingest_history::configured_instance(root.text("topology.instance_id").unwrap());
    let n = |field: &str| root.number(&format!("runtime.processes.application.{field}"));
    let budget = ResourceBudget::new(
        ProcessEnvelope::new(
            n("memory_max_bytes"),
            n("baseline_bytes"),
            ResourceLimits::new(
                n("payload_bytes"),
                if command == "successor-compacted-live" {
                    9
                } else {
                    8
                },
                n("native_bytes"),
                n("completion_reserve_bytes"),
            ),
            WaiterLimits::new(0, 0),
        )
        .unwrap(),
    );
    // These are finite observation/transport grants, separate from the one
    // product lineage grant. A raw snapshot is bounded by the existing fixture
    // census limits and its emitted frame limit, not a physical-RSS guarantee.
    let _observations = grant(&budget, 32 * MIB, 0);
    let _transport = grant(&budget, 4 * MIB, 0);
    let metadata = grant(&budget, metadata_bytes, 0);
    let recovery =
        IngestRecovery::inspect(&path, dataset, limits(), grant(&budget, 0, 128 * MIB)).unwrap();
    // Candidate construction can fail and consume recovery. It therefore runs
    // before any actual Control acquisition. No observed identity is adopted.
    let live = matches!(operation, "live" | "live-new");
    let acquisition = matches!(operation, "abandon" | "acquisition-claims");
    let claims = operation == "acquisition-claims";
    let native = if operation == "prepare" || live || claims {
        let candidate = SuccessorCandidate::new(recovery).unwrap();
        emit_candidate(&candidate);
        Native {
            recovery: None,
            candidate: Some(candidate),
            preparation: None,
            history: None,
            gap: None,
            empty: None,
            live: live::LiveCustody::default(),
            activated_recovery: None,
            source_recovery: None,
            retirement: None,
        }
    } else {
        Native {
            recovery: Some(recovery),
            candidate: None,
            preparation: None,
            history: None,
            gap: None,
            empty: None,
            live: live::LiveCustody::default(),
            activated_recovery: None,
            source_recovery: None,
            retirement: None,
        }
    };
    let mut custody = TransitionCustody {
        native: Some(native),
        owner: None,
    };
    let mutation_permit =
        (mutation_bytes != 0).then(|| grant(&budget, mutation_bytes, mutation_bytes));
    // The explicit capacity does not claim a pending target exists. SQL/native
    // inspection decides that. Preflight the largest live overlap before claim.
    drop(replay_grants(&budget));
    drop(effect_grant(&budget));
    if compacted {
        let _canonical = compacted_grant(&budget);
        if live {
            live::preflight(
                &budget,
                root.number("ingest.producer_outstanding_requests"),
                true,
            );
        }
        if claims {
            // The delivered compacted writer retains this Check while the
            // synchronous password hash owns its separate65MiB grant.
            acquisition::preflight(&budget, true);
        }
        emit(serde_json::json!({"event":"lineage-transition-compacted",
            "command":command,"compacted_outcome_bytes":compacted_outcome_data_bytes(recovery_limits().event).unwrap()}));
    }
    if live && !compacted {
        live::preflight(&budget, 32, false);
    }
    if acquisition && !compacted {
        acquisition::preflight(&budget, claims);
    }
    if operation == "retire" {
        retirement::preflight(&budget);
    }
    emit(
        serde_json::json!({"event":"lineage-history-resources","metadata_bytes":metadata_bytes,
        "metadata_scratch_bytes":LINEAGE_SCRATCH_BYTES,"metadata_entry_bytes":LINEAGE_ENTRY_BYTES,
        "input_bytes":RetainedRequestInput::required_data_bytes(recovery_limits().input).unwrap(),
        "outcome_bytes":recovery_limits().required_data_bytes().unwrap(),
        "effect_bytes":recovery_limits().required_data_bytes().unwrap(),
        "native_bytes":128*MIB,"transport_bytes":4*MIB,"observation_bytes":32*MIB,
        "operation_timeout_ms":10000,"frame_bytes":FRAME_BYTES}),
    );
    emit(
        serde_json::json!({"event":"lineage-transition-resources","operation":operation,
        "mutation_data_bytes":mutation_bytes,"mutation_native_bytes":mutation_bytes,
        "lease_ttl_us":CONTROL_TTL.as_micros().to_string(),"outstanding_request_limit":u32::try_from(root.number("ingest.producer_outstanding_requests")).unwrap()}),
    );
    let (observer, observer_connection) =
        tokio::time::timeout(Duration::from_secs(10), config.connect(NoTls))
            .await
            .expect("observer connection outcome unknown")
            .unwrap();
    let observer_driver = tokio::spawn(observer_connection);
    let probe = Probe {
        root,
        path,
        budget,
        observer,
        database: config.get_dbname().unwrap().into(),
        cancelled: AtomicBool::new(false),
    };
    probe.bounded("observer-settings",probe.observer.batch_execute(
        "SET max_parallel_workers_per_gather=0; SET statement_timeout='8s'; SET lock_timeout='5s'"))
        .await.unwrap();
    let mut role = [0; 16];
    let mut incarnation = [0; 16];
    getrandom::fill(&mut role).unwrap();
    getrandom::fill(&mut incarnation).unwrap();
    assert_ne!(role, [0; 16]);
    assert_ne!(incarnation, [0; 16]);
    emit(
        serde_json::json!({"event":"lineage-history-control","phase":"claim-planned",
        "role":hex(&role),"incarnation":hex(&incarnation),"lease_ttl_us":CONTROL_TTL.as_micros().to_string()}),
    );
    probe.snapshot("before-claim").await;
    let mut control_config = config.clone();
    if let Some(port) = proxy_port {
        // This finite-only transport seam forwards the actual Control session.
        // The already connected observer always stays on the direct endpoint.
        control_config.port(port);
        emit(serde_json::json!({"event":"lineage-transition-transport",
            "control_port":port,"observer_port":5432,"proxied":true}));
    }
    let (client, connection) = probe
        .bounded("connect", control_config.connect(NoTls))
        .await
        .unwrap();
    let driver = tokio::spawn(connection);
    let (owner, policy) = probe
        .bounded(
            "claim",
            ControlSession::acquire(
                client,
                ClaimRequest {
                    dataset,
                    instance: instance.try_into().unwrap(),
                    authority_mode: probe.root.text("config.authority_mode").unwrap().into(),
                    role_instance_id: role,
                    incarnation: incarnation.try_into().unwrap(),
                    lease_ttl: CONTROL_TTL,
                },
                |d, h| probe.validate(d, h),
                &Cell::new(false),
            ),
        )
        .await
        .unwrap();
    custody.owner = Some(owner);
    let owner = custody.owner.as_ref().unwrap();
    emit(
        serde_json::json!({"event":"lineage-history-control","phase":"claimed",
        "resource":hex(owner.identity().resource_id.as_bytes()),"epoch":owner.identity().epoch.get(),
        "role":hex(&role),"incarnation":hex(&incarnation)}),
    );
    probe.policy(owner, "acquired", None, None, policy);
    let backend = probe.backend(owner).await;
    probe.snapshot("after-claim").await;
    let result = if operation == "retire" {
        probe
            .retire_successor(
                custody.owner.as_mut().unwrap(),
                custody.native.as_mut().unwrap(),
                metadata,
                mutation_permit.unwrap(),
                retirement_wait.as_ref(),
            )
            .await
    } else if claims {
        probe
            .acquisition_claims(
                custody.owner.as_mut().unwrap(),
                custody.native.as_mut().unwrap(),
                metadata,
                mutation_permit.unwrap(),
                compacted,
            )
            .await
    } else if operation == "abandon" {
        probe
            .abandon_successor_acquisition(
                custody.owner.as_mut().unwrap(),
                custody.native.as_mut().unwrap(),
                metadata,
                mutation_permit.unwrap(),
            )
            .await
    } else if live {
        probe
            .live(
                custody.owner.as_mut().unwrap(),
                custody.native.as_mut().unwrap(),
                metadata,
                mutation_permit.unwrap(),
                operation == "live-new",
                compacted,
            )
            .await
    } else if operation == "recover" {
        probe
            .recover_successor(
                custody.owner.as_mut().unwrap(),
                custody.native.as_mut().unwrap(),
                metadata,
                mutation_permit.unwrap(),
            )
            .await
    } else if operation == "source" {
        probe
            .close_successor_source(
                custody.owner.as_mut().unwrap(),
                custody.native.as_mut().unwrap(),
                metadata,
                mutation_permit.unwrap(),
            )
            .await
    } else {
        probe
            .transition(
                custody.owner.as_mut().unwrap(),
                custody.native.as_mut().unwrap(),
                metadata,
                mutation_permit,
                operation,
                compacted,
            )
            .await
    };
    emit(
        serde_json::json!({"event":if operation=="retire" {"lineage-retirement-result"}else if acquisition {"lineage-acquisition-result"}else if live {"lineage-live-result"}else if operation=="recover" {"lineage-recovery-result"}else if operation=="source" {"lineage-source-result"}else{"lineage-transition-result"},"operation":operation,
        "stage":result.as_ref().err().map_or("complete",|e|e.stage),
        "epoch":result.as_ref().err().and_then(|e|e.epoch),"sequence":result.as_ref().err().and_then(|e|e.sequence),
        "error":result.as_ref().err().map(|e|&e.error),"observation":result.as_ref().ok()}),
    );
    probe
        .snapshot(if operation == "retire" {
            "after-retirement"
        } else if acquisition {
            "after-acquisition"
        } else if operation == "recover" {
            "after-recovery"
        } else if operation == "source" {
            "after-source"
        } else {
            "after-transition"
        })
        .await;
    // Every history witness has ended. Native closes before the actual owner
    // release/drop and joined driver; failed calls never manufacture release.
    let activated = custody.native.as_ref().unwrap().live.activated();
    drop(custody.native.take());
    emit(serde_json::json!({"event":"lineage-history-native","state":"closed"}));
    probe.snapshot("before-control-end").await;
    let owner = custody.owner.take().unwrap();
    let identity = owner.identity();
    if result.is_ok() {
        probe.bounded("release", owner.release()).await.unwrap();
    } else {
        drop(owner);
    }
    let driver_result = tokio::time::timeout(Duration::from_secs(5), driver)
        .await
        .expect("Control driver join outcome unknown")
        .expect("Control driver panicked");
    let failed = driver_result.is_err();
    emit(
        serde_json::json!({"event":"lineage-history-driver","driver":"control","failed":failed,"joined":true}),
    );
    // Preserve the observed outcome before refusing an unrelated driver failure.
    // The independent fault reader also checks the exact operation error and
    // real wire boundary; a proxy argument alone never certifies injected loss.
    assert!(!failed || (proxy_port.is_some() && result.is_err()));
    probe.absent(&backend).await;
    emit(serde_json::json!({"event":"lineage-history-control",
        "phase":if result.is_ok(){"released-and-joined"}else{"dropped-and-joined"},
        "resource":hex(identity.resource_id.as_bytes()),"epoch":identity.epoch.get()}));
    probe.snapshot("after-control-end").await;
    probe.snapshot("final").await;
    drop(probe);
    crate::ingest_closing::join(observer_driver, false).await;
    emit(
        serde_json::json!({"event":"lineage-history-driver","driver":"observer","failed":false,"joined":true}),
    );
    if operation == "retire" {
        emit(
            serde_json::json!({"event":"lineage-retirement-complete","operation":operation,
            "native":"closed","control":if result.is_ok(){"released-and-joined"}else{"dropped-and-joined"},
            "observer":"joined","success":result.is_ok(),"source_closure":false,"workflow":false,
            "retirement":result.is_ok(),"admission":false,"activation":false}),
        );
    } else if acquisition {
        emit(
            serde_json::json!({"event":"lineage-acquisition-complete","operation":operation,
            "native":"closed","control":if result.is_ok(){"released-and-joined"}else{"dropped-and-joined"},
            "observer":"joined","success":result.is_ok(),"writer":false,"activation":activated,
            "admission":false,"source_closure":false,"workflow":operation=="abandon" && result.is_ok(),"retirement":false}),
        );
    } else if live {
        emit(
            serde_json::json!({"event":"lineage-live-complete","operation":operation,"native":"closed",
            "control":if result.is_ok(){"released-and-joined"}else{"dropped-and-joined"},
            "observer":"joined","success":result.is_ok(),"writer":false,"activation":activated,
            "admission":result.as_ref().ok().map(|_|true)}),
        );
    } else if operation == "recover" {
        emit(
            serde_json::json!({"event":"lineage-recovery-complete","operation":operation,"native":"closed",
            "control":if result.is_ok(){"released-and-joined"}else{"dropped-and-joined"},
            "observer":"joined","success":result.is_ok(),"admission":false,"activation":false,
            "source_closure":false,"workflow":false,"retirement":false}),
        );
    } else if operation == "source" {
        emit(
            serde_json::json!({"event":"lineage-source-complete","operation":operation,"native":"closed",
            "control":if result.is_ok(){"released-and-joined"}else{"dropped-and-joined"},
            "observer":"joined","success":result.is_ok(),"admission":false,"activation":false,
            "source_closure":result.is_ok(),"workflow":false,"retirement":false}),
        );
    } else {
        emit(
            serde_json::json!({"event":"lineage-transition-complete","operation":operation,"native":"closed",
        "control":if result.is_ok(){"released-and-joined"}else{"dropped-and-joined"},
        "observer":"joined","success":result.is_ok(),"admission":false,"activation":false}),
        );
    }
}
impl Probe {
    async fn transition(
        &self,
        owner: &mut ControlSession,
        native: &mut Native,
        metadata: OwnedPermit,
        mutation_permit: Option<OwnedPermit>,
        operation: &str,
        compacted: bool,
    ) -> Result<serde_json::Value, Failure> {
        let history = self
            .load_profile(owner, native.recovery(), metadata, compacted)
            .await?;
        let proof = self
            .replay_profile(owner, native.recovery(), &history, compacted, None)
            .await?;
        match operation {
            "prepare" => {
                let candidate = native.candidate.as_ref().unwrap();
                let plan = if compacted {
                    self.bounded(
                        "plan",
                        owner.plan_compacted_successor_preparation(
                            candidate,
                            &proof,
                            SuccessorPreparationRequest {
                                history: self.request(),
                                lease_duration: CONTROL_TTL,
                                outstanding_request_limit: u32::try_from(
                                    self.root.number("ingest.producer_outstanding_requests"),
                                )
                                .unwrap(),
                            },
                            recovery_limits().event,
                            compacted_grant(&self.budget),
                            |d, h| self.validate(d, h),
                        ),
                    )
                    .await
                } else {
                    self.bounded(
                        "plan",
                        owner.plan_successor_preparation(
                            candidate,
                            &proof,
                            SuccessorPreparationRequest {
                                history: self.request(),
                                lease_duration: CONTROL_TTL,
                                outstanding_request_limit: u32::try_from(
                                    self.root.number("ingest.producer_outstanding_requests"),
                                )
                                .unwrap(),
                            },
                            |d, h| self.validate(d, h),
                        ),
                    )
                    .await
                }
                .map_err(|e| Failure::at("plan", None, None, e))?;
                let policy = plan.policy();
                let created = plan.created_at_us();
                let deadline = plan.lease_expires_at_us();
                let role = candidate.role_instance_id();
                let candidate = native.candidate.take().unwrap();
                native.preparation = Some(SuccessorPreparation::new(candidate, history, plan));
                let preparation = native.preparation.as_mut().unwrap();
                emit(
                    serde_json::json!({"event":"lineage-transition-plan","operation":operation,
                    "record":record_value(preparation.record()),"role":hex(&role),
                    "created_at_us":created.to_string(),
                    "lease_expires_at_us":deadline.to_string(),
                    "outstanding_request_limit":u32::try_from(self.root.number("ingest.producer_outstanding_requests")).unwrap(),
                    "policy":{"config_revision":policy.config_revision,"policy_revision":policy.policy_revision,
                        "config_digest":hex(&policy.config_digest),"policy_digest":hex(&policy.policy_digest)}}),
                );
                self.snapshot("planned").await;
                // The owner remains in native custody on every Result, including
                // CommitUnknown. Save the actual state before classifying it.
                let result = if compacted {
                    self.bounded(
                        "prepare",
                        owner.prepare_compacted_successor(
                            preparation,
                            self.request(),
                            mutation_permit.unwrap(),
                            |d, h| self.validate(d, h),
                        ),
                    )
                    .await
                } else {
                    self.bounded(
                        "prepare",
                        owner.prepare_successor(
                            preparation,
                            self.request(),
                            mutation_permit.unwrap(),
                            |d, h| self.validate(d, h),
                        ),
                    )
                    .await
                };
                self.snapshot("transition-returned").await;
                result.map_err(|e| Failure::at("prepare", None, None, e))?;
                let prepared = preparation.is_prepared();
                let record = record_value(preparation.record());
                emit(
                    serde_json::json!({"event":"lineage-transition-observed","operation":operation,
                    "prepared":prepared,"record":record,"activation":false}),
                );
                assert!(prepared);
                Ok(serde_json::json!({"prepared":prepared,
                    "target_epoch":preparation.record().fields().next_manifest.identity().owner_epoch.get(),
                    "retired_at_us":null}))
            }
            "gap" => {
                let plan = if compacted {
                    self.bounded(
                        "plan",
                        owner.plan_prepared_compacted_successor_gap(
                            native.recovery(),
                            &proof,
                            self.request(),
                            recovery_limits().event,
                            compacted_grant(&self.budget),
                            |d, h| self.validate(d, h),
                        ),
                    )
                    .await
                } else {
                    self.bounded(
                        "plan",
                        owner.plan_prepared_successor_gap(
                            native.recovery(),
                            &proof,
                            self.request(),
                            |d, h| self.validate(d, h),
                        ),
                    )
                    .await
                }
                .map_err(|e| Failure::at("plan", None, None, e))?;
                native.history = Some(history);
                native.gap = Some(plan);
                let history = native.history.as_ref().expect("retained history");
                let plan = native.gap.as_ref().expect("retained transition plan");
                emit(
                    serde_json::json!({"event":"lineage-transition-plan","operation":operation,
                    "record":record_value(plan.record()),"candidate":false}),
                );
                self.snapshot("planned").await;
                let result = if compacted {
                    self.bounded(
                        "gap",
                        owner.complete_prepared_compacted_successor_gap(
                            native.recovery.as_mut().unwrap(),
                            history,
                            plan,
                            self.request(),
                            mutation_permit.unwrap(),
                            |d, h| self.validate(d, h),
                        ),
                    )
                    .await
                } else {
                    self.bounded(
                        "gap",
                        owner.complete_prepared_successor_gap(
                            native.recovery.as_mut().unwrap(),
                            history,
                            plan,
                            self.request(),
                            mutation_permit.unwrap(),
                            |d, h| self.validate(d, h),
                        ),
                    )
                    .await
                };
                self.snapshot("transition-returned").await;
                let observed = result.map_err(|e| Failure::at("gap", None, None, e))?;
                emit(
                    serde_json::json!({"event":"lineage-transition-observed","operation":operation,
                    "record":record_value(observed.record()),"candidate":false,"activation":false}),
                );
                assert_eq!(observed.record(), plan.record());
                Ok(serde_json::json!({"prepared":false,
                    "target_epoch":observed.record().fields().next_manifest.identity().owner_epoch.get(),
                    "retired_at_us":null}))
            }
            "empty" => {
                let plan = if compacted {
                    self.bounded(
                        "plan",
                        owner.plan_compacted_empty_successor_retirement(
                            native.recovery(),
                            &proof,
                            self.request(),
                            recovery_limits().event,
                            compacted_grant(&self.budget),
                            |d, h| self.validate(d, h),
                        ),
                    )
                    .await
                } else {
                    self.bounded(
                        "plan",
                        owner.plan_empty_successor_retirement(
                            native.recovery(),
                            &proof,
                            self.request(),
                            |d, h| self.validate(d, h),
                        ),
                    )
                    .await
                }
                .map_err(|e| Failure::at("plan", None, None, e))?;
                native.history = Some(history);
                native.empty = Some(plan);
                let history = native.history.as_ref().expect("retained history");
                let plan = native.empty.as_ref().expect("retained transition plan");
                let target = plan
                    .record()
                    .fields()
                    .next_manifest
                    .identity()
                    .owner_epoch
                    .get();
                emit(
                    serde_json::json!({"event":"lineage-transition-plan","operation":operation,
                    "record":record_value(plan.record()),"closure_digest":hex(&plan.closure_digest())}),
                );
                self.snapshot("planned").await;
                let result = if compacted {
                    self.bounded(
                        "empty",
                        owner.retire_compacted_empty_successor(
                            native.recovery.as_mut().unwrap(),
                            history,
                            plan,
                            self.request(),
                            mutation_permit.unwrap(),
                            |d, h| self.validate(d, h),
                        ),
                    )
                    .await
                } else {
                    self.bounded(
                        "empty",
                        owner.retire_empty_successor(
                            native.recovery.as_mut().unwrap(),
                            history,
                            plan,
                            self.request(),
                            mutation_permit.unwrap(),
                            |d, h| self.validate(d, h),
                        ),
                    )
                    .await
                };
                self.snapshot("transition-returned").await;
                let retired = result.map_err(|e| Failure::at("empty", None, None, e))?;
                emit(
                    serde_json::json!({"event":"lineage-transition-observed","operation":operation,
                    "target_epoch":target,"retired_at_us":retired.to_string(),
                    "closure_digest":hex(&plan.closure_digest())}),
                );
                Ok(serde_json::json!({"prepared":false,"target_epoch":target,
                    "retired_at_us":retired.to_string()}))
            }
            "empty-retry" => {
                let target = history.native_epoch();
                let digest = proof.last_epoch().digest;
                emit(
                    serde_json::json!({"event":"lineage-transition-plan","operation":operation,
                    "target_epoch":target,"closure_digest":hex(&digest),"read_only":true}),
                );
                self.snapshot("planned").await;
                let result = if compacted {
                    self.bounded(
                        "empty-retry",
                        owner.observe_retired_compacted_empty_successor(
                            native.recovery(),
                            &proof,
                            self.request(),
                            recovery_limits().event,
                            compacted_grant(&self.budget),
                            |d, h| self.validate(d, h),
                        ),
                    )
                    .await
                } else {
                    self.bounded(
                        "empty-retry",
                        owner.observe_retired_empty_successor(
                            native.recovery(),
                            &proof,
                            self.request(),
                            |d, h| self.validate(d, h),
                        ),
                    )
                    .await
                };
                self.snapshot("transition-returned").await;
                let retired = result.map_err(|e| Failure::at("empty-retry", None, None, e))?;
                emit(
                    serde_json::json!({"event":"lineage-transition-observed","operation":operation,
                    "target_epoch":target,"retired_at_us":retired.to_string(),
                    "closure_digest":hex(&digest),"read_only":true}),
                );
                Ok(serde_json::json!({"prepared":false,"target_epoch":target,
                    "retired_at_us":retired.to_string()}))
            }
            _ => unreachable!("closed finite command set"),
        }
    }
}

fn emit_candidate(candidate: &SuccessorCandidate) {
    emit(serde_json::json!({"event":"lineage-transition-candidate",
        "incarnation":hex(candidate.process_incarnation().as_bytes()),"role":hex(&candidate.role_instance_id()),
        "namespaces":(1..=9u16).filter_map(|number| {
            let kind=DictionaryKind::try_from(number).unwrap();
            candidate.allocation_namespaces().domain(kind)
                .map(|domain|serde_json::json!({"kind":number,"id":hex(domain.as_bytes())}))
        }).collect::<Vec<_>>()}));
}
fn record_value(record: &SuccessionRecord) -> serde_json::Value {
    use sha2::{Digest, Sha256};
    let encoded = record.encode();
    let fields = record.fields();
    serde_json::json!({"bytes":hex(encoded.as_bytes()),"sha256":hex(&Sha256::digest(encoded.as_bytes())),
        "predecessor_epoch":fields.predecessor_epoch.get(),
        "target_epoch":fields.next_manifest.identity().owner_epoch.get()})
}
