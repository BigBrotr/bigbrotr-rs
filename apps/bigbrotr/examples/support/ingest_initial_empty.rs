//! Finite config-only reconstruction and retirement of an empty preparation.
//! This acceptance client never activates or inherits the attempted writer.
use super::*;
use bigbrotr::storage::{
    postgres::control_owner::{
        InitialEmptyRetirementPlan, InitialHistoryRequest, PolicySnapshot,
        PreparedEmptyInitialAttempt, ProducerResolveRequest,
    },
    rocks::store::{
        AdmissionLimits, AdmissionOutcome, IngestRecovery, ProducerRecoveryLimits, ResolvedSource,
        RetainedRequestInput, SourceResolution,
    },
};
use bigbrotr_model::{
    DatasetId, ProducerEpoch, ProducerRequestId, RequestSequence,
    codec::{FactDecodeLimits, request::RequestDecodeLimits},
};
use std::{fmt::Debug, future::Future, path::PathBuf, sync::atomic::Ordering};

const CONTROL_TTL: Duration = Duration::from_secs(20);
type Driver = tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>;

struct Probe {
    root: config::Resolved,
    path: PathBuf,
    budget: ResourceBudget,
    observer: tokio_postgres::Client,
    cancelled: AtomicBool,
    database: String,
    commit_loss: bool,
}
struct Backend {
    pid: i32,
    start: String,
    database: String,
    control_epoch: u64,
}
impl Backend {
    fn emit(&self, present: bool) {
        emit(
            serde_json::json!({"event":"successor-backend","stage":"empty",
            "pid":self.pid,"backend_start":self.start,"database":self.database,
            "control_epoch":self.control_epoch,"present":present}),
        );
    }
}
struct Refusal {
    stage: &'static str,
    sequence: Option<u64>,
    error: String,
}
impl Refusal {
    fn at(stage: &'static str, sequence: Option<u64>, error: impl Debug) -> Self {
        Self {
            stage,
            sequence,
            error: format!("{error:?}"),
        }
    }
}
async fn bounded<T>(future: impl Future<Output = T>) -> T {
    // Timeout is unknown infrastructure outcome, never a successful refusal.
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .expect("empty retirement probe deadline; preserve unknown outcomes")
}

pub async fn run(config: Config, config_file: &Path, command: &str, proxy_port: Option<u16>) {
    assert!(matches!(
        command,
        "empty-retire"
            | "empty-inspect"
            | "empty-cancelled"
            | "empty-commit-before"
            | "empty-commit-after"
    ));
    let commit_loss = matches!(command, "empty-commit-before" | "empty-commit-after");
    assert_eq!(commit_loss, proxy_port.is_some());
    assert!(proxy_port.is_none_or(|port| port != 0));
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
    let _metadata = grant(&budget, 16 * MIB, 0);
    let mut recovery =
        IngestRecovery::inspect(&path, dataset, limits(), grant(&budget, 0, 128 * MIB)).unwrap();
    let (observer, observer_driver) = bounded(config.connect(NoTls)).await.unwrap();
    let observer_driver = tokio::spawn(observer_driver);
    let probe = Probe {
        root,
        path,
        budget,
        observer,
        cancelled: AtomicBool::new(false),
        database: config.get_dbname().unwrap().into(),
        commit_loss,
    };
    drop(probe.replay_grants());
    let mut role = [0; 16];
    let mut incarnation = [0; 16];
    getrandom::fill(&mut role).unwrap();
    getrandom::fill(&mut incarnation).unwrap();
    assert_ne!(role, [0; 16]);
    assert_ne!(incarnation, [0; 16]);
    emit(
        serde_json::json!({"event":"successor-control","stage":"empty","phase":"claim-planned",
        "role":hex(&role),"incarnation":hex(&incarnation),"lease_ttl_us":CONTROL_TTL.as_micros().to_string()}),
    );
    probe.snapshot("before-claim").await;
    // Only the dedicated Control transport crosses the fault proxy. The direct
    // observer independently sees committed state and the actual backend tuple.
    let mut owner_config = config;
    if let Some(port) = proxy_port {
        assert!(owner_config.get_ports().is_empty());
        owner_config.port(port);
    }
    let (client, driver) = bounded(owner_config.connect(NoTls)).await.unwrap();
    let driver = tokio::spawn(driver);
    let (mut owner, policy) = bounded(ControlSession::acquire(
        client,
        ClaimRequest {
            dataset,
            instance: instance.try_into().unwrap(),
            authority_mode: probe.root.text("config.authority_mode").unwrap().into(),
            role_instance_id: role,
            incarnation: incarnation.try_into().unwrap(),
            lease_ttl: CONTROL_TTL,
        },
        |document, digest| probe.validate(document, digest),
        &Cell::new(false),
    ))
    .await
    .unwrap();
    emit(
        serde_json::json!({"event":"successor-control","stage":"empty","phase":"claimed",
        "resource":hex(owner.identity().resource_id.as_bytes()),"epoch":owner.identity().epoch.get(),
        "role":hex(&role),"incarnation":hex(&incarnation)}),
    );
    probe.emit_policy(&owner, "acquired", None, policy);
    let backend = probe.observe_backend(&owner).await;
    probe.snapshot("after-claim").await;
    probe.snapshot("before-read").await;
    let result = probe.retire(&mut owner, &mut recovery, command).await;
    match &result {
        Ok(()) => emit(
            serde_json::json!({"event":"successor-empty-result","stage":"complete","sequence":null,"error":null}),
        ),
        Err(error) => emit(
            serde_json::json!({"event":"successor-empty-result","stage":error.stage,"sequence":error.sequence,"error":error.error}),
        ),
    }
    if result.is_err() {
        probe.observe_reuse(&mut owner).await;
    }
    probe.snapshot("after-read").await;
    // Drop the retained native worker before ending the actual Control owner.
    drop(recovery);
    probe
        .end_control(owner, driver, result.is_ok(), &backend)
        .await;
    probe.snapshot("final").await;
    emit(
        serde_json::json!({"event":"successor-empty-complete","command":command,"native":"closed",
        "control":if result.is_ok() {"released-and-joined"} else {"dropped-and-joined"},
        "admission":false,"success":result.is_ok()}),
    );
    drop(probe);
    crate::ingest_closing::join(observer_driver, false).await;
    if command == "empty-cancelled" || commit_loss {
        let Err(error) = result else {
            panic!("retirement refusal fixture unexpectedly completed")
        };
        assert_eq!(error.stage, "retirement");
        assert_eq!(error.sequence, None);
        assert_eq!(
            error.error,
            if commit_loss {
                "CommitUnknown"
            } else {
                "Native(AuthorityUnavailable)"
            }
        );
    }
}

impl Probe {
    fn validate(&self, document: &str, digest: [u8; 32]) -> Result<(), OwnerError> {
        crate::config::validate_fixture_policy(document, digest, &self.root)
    }
    fn request(&self) -> InitialHistoryRequest<'_> {
        InitialHistoryRequest {
            ingest_root: &self.path,
            policy_ttl: Duration::from_micros(self.root.number("runtime.authorization_ttl")),
            cancelled: &self.cancelled,
        }
    }
    fn replay_grants(&self) -> (OwnedPermit, OwnedPermit) {
        let limits = recovery_limits();
        (
            grant(
                &self.budget,
                RetainedRequestInput::required_data_bytes(limits.input).unwrap(),
                0,
            ),
            grant(&self.budget, limits.required_data_bytes().unwrap(), 0),
        )
    }
    fn emit_policy(
        &self,
        owner: &ControlSession,
        phase: &str,
        sequence: Option<u64>,
        policy: PolicySnapshot,
    ) {
        emit(
            serde_json::json!({"event":"successor-policy","stage":"empty","phase":phase,"sequence":sequence,
            "control_epoch":owner.identity().epoch.get(),"config_revision":policy.config_revision,
            "policy_revision":policy.policy_revision,"config_digest":hex(&policy.config_digest),"policy_digest":hex(&policy.policy_digest)}),
        );
    }
    async fn snapshot(&self, suffix: &str) {
        let state = bounded(crate::ingest_snapshot::state(&self.observer, &self.path)).await;
        let (rows, sequence) = raw_state(&self.path);
        assert_eq!(state["sequence"].as_u64(), Some(sequence));
        emit(
            serde_json::json!({"event":"successor-state","stage":format!("empty-{suffix}"),"state":state,"rows":rows}),
        );
    }
    async fn observe_backend(&self, owner: &ControlSession) -> Backend {
        let identity = owner.identity();
        let row = bounded(self.observer.query_one(
            "SELECT json_build_object('pid',o.control_backend_pid,'backend_start',o.control_backend_start,'database',a.datname)::text
             FROM control.owner_epochs o JOIN pg_catalog.pg_stat_activity a
               ON a.pid=o.control_backend_pid AND a.backend_start=o.control_backend_start
             WHERE o.resource_id=$1::bytea AND o.epoch=$2::text::numeric
               AND o.process_incarnation=$3::bytea AND o.role='control' AND o.state='active'
               AND a.datname=current_database()",
            &[&&identity.resource_id.as_bytes()[..], &identity.epoch.get().to_string(), &&identity.process_incarnation.as_bytes()[..]]
        )).await.unwrap();
        let value: serde_json::Value = serde_json::from_str(row.get::<_, &str>(0)).unwrap();
        let backend = Backend {
            pid: i32::try_from(value["pid"].as_i64().unwrap()).unwrap(),
            start: value["backend_start"].as_str().unwrap().into(),
            database: value["database"].as_str().unwrap().into(),
            control_epoch: identity.epoch.get(),
        };
        assert!(backend.pid > 0 && !backend.start.is_empty());
        assert_eq!(backend.database, self.database);
        backend.emit(true);
        backend
    }
    async fn observe_reuse(&self, owner: &mut ControlSession) {
        let result =
            bounded(owner.refresh(|document, digest| self.validate(document, digest))).await;
        if self.commit_loss {
            assert_eq!(result, Err(OwnerError::Unavailable));
        }
        match result {
            Ok(policy) => {
                emit(
                    serde_json::json!({"event":"successor-empty-control-reuse","available":true,"error":null}),
                );
                self.emit_policy(owner, "reuse", None, policy);
            }
            Err(error) => {
                assert_eq!(error, OwnerError::Unavailable);
                emit(
                    serde_json::json!({"event":"successor-empty-control-reuse","available":false,"error":"Unavailable"}),
                );
            }
        }
        self.snapshot("after-reuse").await;
    }
    async fn end_control(
        &self,
        owner: ControlSession,
        driver: Driver,
        release: bool,
        backend: &Backend,
    ) {
        self.snapshot("before-control-end").await;
        let identity = owner.identity();
        assert!(!self.commit_loss || !release);
        if release {
            bounded(owner.release()).await.unwrap();
        } else {
            drop(owner);
        }
        // A SQL refusal keeps a clean transport; the COMMIT-loss proxy closes
        // it. Join the actual result before separately observing server absence.
        crate::ingest_closing::join(driver, self.commit_loss).await;
        emit(
            serde_json::json!({"event":"successor-driver","stage":"empty","failed":self.commit_loss,"joined":true}),
        );
        bounded(async {
            loop {
                let present: bool = self.observer.query_one(
                    "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity
                     WHERE pid=$1::integer AND backend_start=$2::text::timestamptz AND datname=$3::text)",
                    &[&backend.pid, &backend.start, &backend.database],
                ).await.unwrap().get(0);
                if !present { break; }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }).await;
        backend.emit(false);
        emit(
            serde_json::json!({"event":"successor-control","stage":"empty",
            "phase":if release {"released-and-joined"} else {"dropped-and-joined"},
            "resource":hex(identity.resource_id.as_bytes()),"epoch":identity.epoch.get()}),
        );
        self.snapshot("after-control-end").await;
    }
    async fn retire(
        &self,
        owner: &mut ControlSession,
        recovery: &mut IngestRecovery,
        command: &str,
    ) -> Result<(), Refusal> {
        let history = bounded(owner.inspect_empty_initial_attempt(
            recovery,
            self.request(),
            grant(&self.budget, 128 * 1024, 0),
            |document, digest| self.validate(document, digest),
        ))
        .await
        .map_err(|e| Refusal::at("empty-load", None, e))?;
        emit_registration(history.registration());
        self.snapshot("loaded").await;
        let source = history
            .source_resolution(grant(&self.budget, 4096, 4096))
            .map_err(|e| Refusal::at("source-start", None, e))?;
        self.snapshot("source-started").await;
        let source = self.resolve_originals(owner, &history, source).await?;
        let plan = history
            .reconstruct_retirement(&source)
            .map_err(|e| Refusal::at("reconstruct", None, e))?;
        let retirement = plan.retirement();
        emit(
            serde_json::json!({"event":"successor-empty-proof","issued_through":source.resolved_requests(),
            "last_complete_sequence":source.last_complete_sequence(),"closure_digest":hex(&source.digest()),
            "previous_manifest":hex(retirement.previous_manifest().encode().as_bytes()),
            "target_manifest":hex(retirement.target_manifest().encode().as_bytes()),"handoff":hex(retirement.handoff().as_bytes()),
            "empty_closure_digest":hex(&retirement.closure_digest()),"initially_closing":retirement.initially_closing()}),
        );
        drop(source);
        self.snapshot("proof").await;
        if command == "empty-inspect" {
            return Ok(());
        }
        if command == "empty-cancelled" {
            self.cancelled.store(true, Ordering::Relaxed);
            emit(serde_json::json!({"event":"successor-empty-cancellation","cancelled":true}));
        }
        self.snapshot("before-retirement").await;
        let result = self.dispatch(owner, recovery, &plan).await;
        if let Ok(time) = result {
            Self::emit_retirement(&plan, "initial", time);
        }
        self.snapshot("after-retirement").await;
        let retired_at = result.map_err(|e| Refusal::at("retirement", None, e))?;
        self.snapshot("before-retry").await;
        let retry = self.dispatch(owner, recovery, &plan).await;
        if let Ok(time) = retry {
            Self::emit_retirement(&plan, "retry", time);
        }
        self.snapshot("after-retry").await;
        assert_eq!(
            retry.map_err(|e| Refusal::at("retry", None, e))?,
            retired_at
        );
        Ok(())
    }
    async fn dispatch(
        &self,
        owner: &mut ControlSession,
        recovery: &mut IngestRecovery,
        plan: &InitialEmptyRetirementPlan,
    ) -> Result<i64, bigbrotr::storage::postgres::control_owner::InitialSuccessionError> {
        bounded(owner.retire_empty_initial_attempt(
            recovery,
            plan,
            self.request(),
            grant(&self.budget, 128 * 1024, 4096),
            |document, digest| self.validate(document, digest),
        ))
        .await
    }
    fn emit_retirement(plan: &InitialEmptyRetirementPlan, attempt: &str, time: i64) {
        emit(
            serde_json::json!({"event":"successor-empty-retirement","attempt":attempt,"retired_at_us":time,
            "closure_digest":hex(&plan.retirement().closure_digest()),"handoff":hex(plan.retirement().handoff().as_bytes()),"admission":false}),
        );
    }
    async fn resolve_originals<'a>(
        &self,
        owner: &mut ControlSession,
        history: &PreparedEmptyInitialAttempt<'a>,
        mut source: SourceResolution<'a>,
    ) -> Result<ResolvedSource<'a>, Refusal> {
        let spec = history.registration();
        assert!(history.issued_through() <= 64);
        for sequence in 1..=history.issued_through() {
            let policy = bounded(owner.refresh(|document, digest| self.validate(document, digest)))
                .await
                .map_err(|e| Refusal::at("policy-refresh", Some(sequence), e))?;
            self.emit_policy(owner, "refreshed", Some(sequence), policy);
            self.snapshot(&format!("refreshed-{sequence}")).await;
            let (input, outcome) = self.replay_grants();
            let resolved = bounded(owner.inspect_empty_initial_request(
                history,
                ProducerResolveRequest {
                    ingest_root: &self.path,
                    registration: spec,
                    request: ProducerRequestId {
                        producer_slot_id: spec.producer_slot_id,
                        producer_epoch: ProducerEpoch::new(1).unwrap(),
                        request_sequence: RequestSequence::new(sequence).unwrap(),
                    },
                    limits: recovery_limits(),
                    policy_ttl: self.request().policy_ttl,
                    cancelled: &self.cancelled,
                },
                input,
                outcome,
                |document, digest| self.validate(document, digest),
            ))
            .await
            .map_err(|e| Refusal::at("request", Some(sequence), e))?;
            let (outcome, receipt, rejection) = match resolved.outcome() {
                AdmissionOutcome::Durable(receipt) => (
                    "durable",
                    Some(hex(&receipt.encode_persistent(64 * 1024).unwrap())),
                    None,
                ),
                AdmissionOutcome::Rejected(reason) => {
                    ("rejected", None, Some(format!("{reason:?}")))
                }
            };
            source
                .record(&resolved)
                .map_err(|e| Refusal::at("source-record", Some(sequence), e))?;
            emit(
                serde_json::json!({"event":"successor-empty-request","sequence":sequence,"outcome":outcome,"receipt":receipt,"rejection":rejection}),
            );
            drop(resolved);
            self.snapshot(&format!("resolved-{sequence}")).await;
        }
        source
            .finish()
            .map_err(|e| Refusal::at("source-finish", None, e))
    }
}

fn emit_registration(s: &InitialRegistration) {
    emit(serde_json::json!({"event":"successor-empty-registration",
        "dataset_id":hex(s.dataset_id.as_bytes()),"instance_id":hex(&s.instance_id),
        "control_resource":hex(s.control.resource_id.as_bytes()),"control_epoch":s.control.epoch.get(),
        "control_role_instance":hex(&s.control.role_instance_id),"control_incarnation":hex(s.control.process_incarnation.as_bytes()),
        "ingest_resource":hex(s.ingest_resource_id.as_bytes()),"ingest_role_instance":hex(&s.ingest_role_instance_id),
        "ingest_incarnation":hex(s.process_incarnation.as_bytes()),"shard_id":s.shard_id,
        "config_revision":s.config_revision,"policy_revision":s.policy_revision,
        "config_digest":hex(s.config_digest.as_bytes()),"policy_digest":hex(s.policy_digest.as_bytes()),
        "previous_topology":s.previous_topology.map(|v|v.get()),
        "allocation_domains":s.allocation_domains.iter().map(|(kind,id)|serde_json::json!([*kind as u16,hex(id.as_bytes())])).collect::<Vec<_>>(),
        "producer_slot":hex(s.producer_slot_id.as_bytes()),"window":s.outstanding_request_limit,
        "created_at_us":s.created_at_us,"lease_expires_at_us":s.lease_expires_at_us}));
}
fn recovery_limits() -> ProducerRecoveryLimits {
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
