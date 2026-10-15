//! Finite actual-Control successor preparation and unactivated recovery.
//! Each invocation owns one native lifetime. There is no writer delivery,
//! activation, raw registry mutation, fault hook or in-process reopening.
use super::*;
use bigbrotr::storage::{
    postgres::control_owner::{SuccessorPreparation, SuccessorPreparationRequest},
    rocks::{store::SuccessorCandidate, succession::SuccessionRecord},
};

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

pub async fn run(config: Config, config_file: &Path, metadata_bytes: u64, command: &str) {
    assert!(metadata_bytes <= MAX_LINEAGE_METADATA_BYTES);
    let operation = match command {
        "successor-prepare" => "prepare",
        "successor-empty" => "empty",
        "successor-empty-retry" => "empty-retry",
        "successor-gap" => "gap",
        _ => panic!("unknown finite successor operation"),
    };
    let mutation_bytes = match operation {
        "prepare" | "gap" => 8192,
        "empty" => 4096,
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
                8,
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
    let native = if operation == "prepare" {
        let candidate = SuccessorCandidate::new(recovery).unwrap();
        emit_candidate(&candidate);
        Native {
            recovery: None,
            candidate: Some(candidate),
            preparation: None,
        }
    } else {
        Native {
            recovery: Some(recovery),
            candidate: None,
            preparation: None,
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
        "lease_ttl_us":CONTROL_TTL.as_micros().to_string(),"outstanding_request_limit":32}),
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
    let (client, connection) = probe
        .bounded("connect", config.connect(NoTls))
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
    let result = probe
        .transition(
            custody.owner.as_mut().unwrap(),
            custody.native.as_mut().unwrap(),
            metadata,
            mutation_permit,
            operation,
        )
        .await;
    emit(
        serde_json::json!({"event":"lineage-transition-result","operation":operation,
        "stage":result.as_ref().err().map_or("complete",|e|e.stage),
        "epoch":result.as_ref().err().and_then(|e|e.epoch),"sequence":result.as_ref().err().and_then(|e|e.sequence),
        "error":result.as_ref().err().map(|e|&e.error),"observation":result.as_ref().ok()}),
    );
    probe.snapshot("after-transition").await;
    // Every history witness has ended. Native closes before the actual owner
    // release/drop and joined driver; failed calls never manufacture release.
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
    crate::ingest_closing::join(driver, false).await;
    emit(
        serde_json::json!({"event":"lineage-history-driver","driver":"control","failed":false,"joined":true}),
    );
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
    emit(
        serde_json::json!({"event":"lineage-transition-complete","operation":operation,"native":"closed",
        "control":if result.is_ok(){"released-and-joined"}else{"dropped-and-joined"},
        "observer":"joined","success":result.is_ok(),"admission":false,"activation":false}),
    );
}
impl Probe {
    async fn transition(
        &self,
        owner: &mut ControlSession,
        native: &mut Native,
        metadata: OwnedPermit,
        mutation_permit: Option<OwnedPermit>,
        operation: &str,
    ) -> Result<serde_json::Value, Failure> {
        let history = self.load(owner, native.recovery(), metadata).await?;
        let proof = self.replay(owner, native.recovery(), &history).await?;
        match operation {
            "prepare" => {
                let candidate = native.candidate.as_ref().unwrap();
                let plan = self
                    .bounded(
                        "plan",
                        owner.plan_successor_preparation(
                            candidate,
                            &proof,
                            SuccessorPreparationRequest {
                                history: self.request(),
                                lease_duration: CONTROL_TTL,
                                outstanding_request_limit: 32,
                            },
                            |d, h| self.validate(d, h),
                        ),
                    )
                    .await
                    .map_err(|e| Failure::at("plan", None, None, e))?;
                let policy = plan.policy();
                emit(
                    serde_json::json!({"event":"lineage-transition-plan","operation":operation,
                    "record":record_value(plan.record()),"role":hex(&candidate.role_instance_id()),
                    "created_at_us":plan.created_at_us().to_string(),
                    "lease_expires_at_us":plan.lease_expires_at_us().to_string(),
                    "outstanding_request_limit":32,
                    "policy":{"config_revision":policy.config_revision,"policy_revision":policy.policy_revision,
                        "config_digest":hex(&policy.config_digest),"policy_digest":hex(&policy.policy_digest)}}),
                );
                self.snapshot("planned").await;
                let candidate = native.candidate.take().unwrap();
                native.preparation = Some(SuccessorPreparation::new(candidate, history, plan));
                let preparation = native.preparation.as_mut().unwrap();
                // The owner remains in native custody on every Result, including
                // CommitUnknown. Save the actual state before classifying it.
                let result = self
                    .bounded(
                        "prepare",
                        owner.prepare_successor(
                            preparation,
                            self.request(),
                            mutation_permit.unwrap(),
                            |d, h| self.validate(d, h),
                        ),
                    )
                    .await;
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
                let plan = self
                    .bounded(
                        "plan",
                        owner.plan_prepared_successor_gap(
                            native.recovery(),
                            &proof,
                            self.request(),
                            |d, h| self.validate(d, h),
                        ),
                    )
                    .await
                    .map_err(|e| Failure::at("plan", None, None, e))?;
                emit(
                    serde_json::json!({"event":"lineage-transition-plan","operation":operation,
                    "record":record_value(plan.record()),"candidate":false}),
                );
                self.snapshot("planned").await;
                let result = self
                    .bounded(
                        "gap",
                        owner.complete_prepared_successor_gap(
                            native.recovery.as_mut().unwrap(),
                            &history,
                            &plan,
                            self.request(),
                            mutation_permit.unwrap(),
                            |d, h| self.validate(d, h),
                        ),
                    )
                    .await;
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
                let plan = self
                    .bounded(
                        "plan",
                        owner.plan_empty_successor_retirement(
                            native.recovery(),
                            &proof,
                            self.request(),
                            |d, h| self.validate(d, h),
                        ),
                    )
                    .await
                    .map_err(|e| Failure::at("plan", None, None, e))?;
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
                let result = self
                    .bounded(
                        "empty",
                        owner.retire_empty_successor(
                            native.recovery.as_mut().unwrap(),
                            &history,
                            &plan,
                            self.request(),
                            mutation_permit.unwrap(),
                            |d, h| self.validate(d, h),
                        ),
                    )
                    .await;
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
                let result = self
                    .bounded(
                        "empty-retry",
                        owner.observe_retired_empty_successor(
                            native.recovery(),
                            &proof,
                            self.request(),
                            |d, h| self.validate(d, h),
                        ),
                    )
                    .await;
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
