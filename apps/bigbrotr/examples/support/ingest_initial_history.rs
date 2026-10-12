//! Finite connected reconstruction in a new process, never admission.
//! Only configuration and retained PostgreSQL/native state cross the boundary.
use super::*;
use bigbrotr::storage::{
    postgres::control_owner::{
        InitialHistoryRequest, PolicySnapshot, PreparedInitialHistory, ProducerCloseRequest,
        ProducerResolveRequest,
    },
    rocks::store::{
        AdmissionLimits, AdmissionOutcome, IngestRecovery, InitialHandoff, ProducerRecoveryLimits,
        ResolvedSource, RetainedRequestInput, SourceResolution,
    },
};
use bigbrotr_model::{
    DatasetId, ProducerEpoch, ProducerRequestId, RequestSequence,
    codec::{FactDecodeLimits, request::RequestDecodeLimits},
};
use std::{fmt::Debug, future::Future, path::PathBuf, sync::atomic::Ordering};

type Driver = tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>;
const CONTROL_TTL: Duration = Duration::from_secs(20);

#[derive(Clone, Copy)]
enum Mode {
    History,
    Gap { cancel_before_completion: bool },
}
impl Mode {
    fn name(self) -> &'static str {
        match self {
            Self::History => "history",
            Self::Gap { .. } => "gap",
        }
    }
    fn event(self, suffix: &str) -> String {
        format!("successor-{}-{suffix}", self.name())
    }
}

#[derive(Clone, Copy)]
enum Loss {
    BeforeNative,
    AfterNative,
}

#[derive(Clone, Copy)]
struct Qualification {
    select_route: bool,
    loss: Option<Loss>,
    proxy_port: Option<u16>,
}

struct Backend {
    pid: i32,
    start: String,
    database: String,
    control_epoch: u64,
}
impl Backend {
    fn emit(&self, mode: Mode, present: bool) {
        emit(
            serde_json::json!({"event":"successor-backend","stage":mode.name(),
            "pid":self.pid,"backend_start":self.start,"database":self.database,
            "control_epoch":self.control_epoch,"present":present}),
        );
    }
}

struct Probe {
    mode: Mode,
    root: config::Resolved,
    path: PathBuf,
    budget: ResourceBudget,
    observer: tokio_postgres::Client,
    cancelled: AtomicBool,
    qualification: Option<Qualification>,
    database: String,
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

// An infrastructure timeout is not an expected product refusal or rollback.
// The enclosing laboratory preserves partial stdout and independently cleans
// up this finite process. Never report a successful release after timeout.
async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .expect("history probe deadline; preserve unknown outcomes")
}

/// Deliberate corruption of an isolated prepared fixture before the reader
/// starts. This raw engine path is never reachable from a product operation.
pub fn corrupt(path: &Path, mutation: &str) {
    use bigbrotr::storage::rocks::keys::IngestKey;
    assert!(matches!(mutation, "missing-handoff" | "target-nonzero"));
    let (before_rows, before_sequence) = raw_state(path);
    let db = rocksdb::DB::open_cf(
        &rocksdb::Options::default(),
        path,
        ["default", "facts", "receipts"],
    )
    .unwrap();
    let cf = db.cf_handle("default").unwrap();
    let manifest = IngestManifest::decode(&db.get_cf(cf, [1, 1]).unwrap().unwrap()).unwrap();
    assert_eq!(manifest.key_schema(), 2);
    let id = manifest.identity();
    assert_eq!(id.owner_epoch.get(), 2);
    let key = match mutation {
        "missing-handoff" => IngestKey::OwnerHandoff {
            dataset_id: id.dataset_id,
            owner_resource_id: id.owner_resource_id,
            predecessor_epoch: OwnerEpoch::new(1).unwrap(),
        },
        "target-nonzero" => IngestKey::SourceStream {
            dataset_id: id.dataset_id,
            shard_id: id.shard_id,
            owner_epoch: id.owner_epoch,
        },
        _ => unreachable!(),
    }
    .encode()
    .unwrap();
    let before = db.get_cf(cf, key.as_bytes()).unwrap().unwrap();
    let after = if mutation == "target-nonzero" {
        assert_eq!(before, [1, 2, 0, 0, 0, 0, 0, 0, 0, 0]);
        Some(vec![1, 2, 0, 0, 0, 0, 0, 0, 0, 1])
    } else {
        assert!((286..=610).contains(&before.len()));
        assert_eq!(before[..2], [1, 10]);
        None
    };
    emit(
        serde_json::json!({"event":"successor-history-corruption","phase":"before",
        "mutation":mutation,"cf":"default","key":hex(key.as_bytes()),"value":hex(&before),
        "sequence":before_sequence,"rows":before_rows}),
    );
    let mut options = rocksdb::WriteOptions::default();
    options.set_sync(true);
    if let Some(after) = &after {
        db.put_cf_opt(cf, key.as_bytes(), after, &options).unwrap();
    } else {
        db.delete_cf_opt(cf, key.as_bytes(), &options).unwrap();
    }
    drop(db);
    let (rows, sequence) = raw_state(path);
    assert_eq!(sequence, before_sequence.checked_add(1).unwrap());
    emit(
        serde_json::json!({"event":"successor-history-corruption","phase":"after",
        "mutation":mutation,"cf":"default","key":hex(key.as_bytes()),
        "value":after.as_deref().map(hex),"sequence":sequence,"rows":rows}),
    );
}

pub async fn run(config: Config, config_file: &Path) {
    run_mode(config, config_file, Mode::History, None).await;
}

pub async fn run_gap(config: Config, config_file: &Path, cancel_before_completion: bool) {
    run_mode(
        config,
        config_file,
        Mode::Gap {
            cancel_before_completion,
        },
        None,
    )
    .await;
}

pub async fn run_qualification(
    config: Config,
    config_file: &Path,
    command: &str,
    proxy_port: Option<u16>,
) {
    let loss = match command {
        "successor-gap-loss-before" => Some(Loss::BeforeNative),
        "successor-gap-loss-after" => Some(Loss::AfterNative),
        "successor-gap-policy" | "successor-prepared-recover" => None,
        _ => panic!("unknown initial gap qualification"),
    };
    assert_eq!(loss.is_some(), proxy_port.is_some());
    assert!(proxy_port.is_none_or(|port| port != 0));
    run_mode(
        config,
        config_file,
        Mode::Gap {
            cancel_before_completion: false,
        },
        Some(Qualification {
            select_route: command == "successor-prepared-recover",
            loss,
            proxy_port,
        }),
    )
    .await;
}

async fn run_mode(
    config: Config,
    config_file: &Path,
    mut mode: Mode,
    qualification: Option<Qualification>,
) {
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
    if let Some(qualification) = qualification {
        // This dispatch is only a finite fixture. The selected product adapter
        // still proves the complete retained history under a fresh Control.
        let manifest = recovery.manifest();
        let route = match (manifest.identity().owner_epoch.get(), manifest.key_schema()) {
            (1, 1) => Mode::Gap {
                cancel_before_completion: false,
            },
            (2, 2) if qualification.select_route => Mode::History,
            _ => panic!("unsupported actual prepared recovery state"),
        };
        mode = route;
        emit(
            serde_json::json!({"event":"successor-recovery-selection","route":mode.name(),
            "owner_epoch":manifest.identity().owner_epoch.get(),"key_schema":manifest.key_schema(),
            "manifest":hex(manifest.encode().as_bytes()),"admission":false}),
        );
    }
    let (observer, observer_driver) = bounded(config.connect(NoTls)).await.unwrap();
    let observer_driver = tokio::spawn(observer_driver);
    let probe = Probe {
        mode,
        root,
        path,
        budget,
        observer,
        cancelled: AtomicBool::new(false),
        qualification,
        database: config.get_dbname().unwrap().into(),
    };
    drop(probe.replay_grants());
    let mut role = [0; 16];
    let mut incarnation = [0; 16];
    getrandom::fill(&mut role).unwrap();
    getrandom::fill(&mut incarnation).unwrap();
    assert_ne!(role, [0; 16]);
    assert_ne!(incarnation, [0; 16]);
    emit(
        serde_json::json!({"event":"successor-control","stage":mode.name(),"phase":"claim-planned",
        "role":hex(&role),"incarnation":hex(&incarnation),"lease_ttl_us":CONTROL_TTL.as_micros().to_string()}),
    );
    probe.snapshot("before-claim").await;
    // The observer remains direct. Only this dedicated owner connection crosses
    // the bounded laboratory proxy, and no connection or plan crosses processes.
    let mut owner_config = config;
    if let Some(port) = qualification.and_then(|value| value.proxy_port) {
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
        serde_json::json!({"event":"successor-control","stage":mode.name(),"phase":"claimed",
        "resource":hex(owner.identity().resource_id.as_bytes()),"epoch":owner.identity().epoch.get(),
        "role":hex(&role),"incarnation":hex(&incarnation)}),
    );
    probe.emit_policy(&owner, "acquired", None, policy);
    let backend = if qualification.is_some() {
        Some(probe.observe_backend(&owner).await)
    } else {
        None
    };
    probe.snapshot("after-claim").await;
    probe.snapshot("before-read").await;
    let result = match mode {
        Mode::History => probe.read(&mut owner, &recovery).await,
        Mode::Gap {
            cancel_before_completion,
        } => {
            probe
                .complete_gap(&mut owner, &mut recovery, cancel_before_completion)
                .await
        }
    };
    match &result {
        Ok(()) => emit(serde_json::json!({"event":mode.event("result"),
            "stage":"complete","sequence":null,"error":null})),
        Err(error) => emit(serde_json::json!({"event":mode.event("result"),
            "stage":error.stage,"sequence":error.sequence,"error":error.error})),
    }
    if qualification.is_some_and(|value| value.loss.is_some()) && result.is_err() {
        probe.check_poisoned(&mut owner).await;
    }
    probe.snapshot("after-read").await;
    // All borrowed historical witnesses have ended. Close/join native custody
    // before releasing the actual Control owner, matching the installed order.
    drop(recovery);
    probe
        .end_control(owner, driver, result.is_ok(), backend.as_ref())
        .await;
    probe.snapshot("final").await;
    emit(
        serde_json::json!({"event":mode.event("complete"),"native":"closed",
        "control":if result.is_ok() {"released-and-joined"} else {"dropped-and-joined"},
        "admission":false,"success":result.is_ok()}),
    );
    drop(probe);
    crate::ingest_closing::join(observer_driver, false).await;
    if let Some(loss) = qualification.and_then(|value| value.loss) {
        let Err(error) = result else {
            panic!("delivery-loss fixture unexpectedly completed");
        };
        assert_eq!(error.stage, "completion");
        assert_eq!(error.sequence, None);
        assert_eq!(
            error.error,
            match loss {
                Loss::BeforeNative => "Control(Backend)",
                Loss::AfterNative => "DeliveryUnknown",
            }
        );
    }
}

impl Probe {
    fn emit_policy(
        &self,
        owner: &ControlSession,
        phase: &str,
        sequence: Option<u64>,
        policy: PolicySnapshot,
    ) {
        if self.qualification.is_some() {
            emit(
                serde_json::json!({"event":"successor-policy","stage":self.mode.name(),
                "phase":phase,"sequence":sequence,"control_epoch":owner.identity().epoch.get(),
                "config_revision":policy.config_revision,"policy_revision":policy.policy_revision,
                "config_digest":hex(&policy.config_digest),"policy_digest":hex(&policy.policy_digest)}),
            );
        }
    }

    async fn observe_backend(&self, owner: &ControlSession) -> Backend {
        let identity = owner.identity();
        let row = bounded(self.observer.query_one(
            "SELECT json_build_object('pid',o.control_backend_pid,
                'backend_start',o.control_backend_start,'database',a.datname)::text
             FROM control.owner_epochs o JOIN pg_catalog.pg_stat_activity a
               ON a.pid=o.control_backend_pid AND a.backend_start=o.control_backend_start
             WHERE o.resource_id=$1::bytea AND o.epoch=$2::text::numeric
               AND o.process_incarnation=$3::bytea AND o.role='control'
               AND o.state='active' AND a.datname=current_database()",
            &[
                &&identity.resource_id.as_bytes()[..],
                &identity.epoch.get().to_string(),
                &&identity.process_incarnation.as_bytes()[..],
            ],
        ))
        .await
        .unwrap();
        let value: serde_json::Value = serde_json::from_str(row.get::<_, &str>(0)).unwrap();
        let backend = Backend {
            pid: i32::try_from(value["pid"].as_i64().unwrap()).unwrap(),
            start: value["backend_start"].as_str().unwrap().into(),
            database: value["database"].as_str().unwrap().into(),
            control_epoch: identity.epoch.get(),
        };
        assert!(backend.pid > 0 && !backend.start.is_empty());
        assert_eq!(backend.database, self.database);
        backend.emit(self.mode, true);
        backend
    }

    async fn require_backend_absent(&self, backend: &Backend) {
        // A closed client does not prove server exclusion. Observe the exact
        // frozen tuple without terminating it or changing its retained row.
        bounded(async {
            loop {
                let present: bool = self
                    .observer
                    .query_one(
                        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity
                         WHERE pid=$1::integer AND backend_start=$2::text::timestamptz
                           AND datname=$3::text)",
                        &[&backend.pid, &backend.start, &backend.database],
                    )
                    .await
                    .unwrap()
                    .get(0);
                if !present {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await;
        backend.emit(self.mode, false);
    }

    async fn check_poisoned(&self, owner: &mut ControlSession) {
        assert_eq!(
            bounded(owner.refresh(|document, digest| self.validate(document, digest))).await,
            Err(OwnerError::Unavailable)
        );
        emit(
            serde_json::json!({"event":"successor-session-poisoned","stage":self.mode.name(),
            "control_epoch":owner.identity().epoch.get()}),
        );
    }

    fn validate(&self, document: &str, digest: [u8; 32]) -> Result<(), OwnerError> {
        crate::config::validate_fixture_policy(document, digest, &self.root)
    }
    fn policy_ttl(&self) -> Duration {
        Duration::from_micros(self.root.number("runtime.authorization_ttl"))
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
    async fn snapshot(&self, stage: &str) {
        let stage = format!("{}-{stage}", self.mode.name());
        let state = bounded(crate::ingest_snapshot::state(&self.observer, &self.path)).await;
        let (rows, sequence) = raw_state(&self.path);
        assert_eq!(state["sequence"].as_u64(), Some(sequence));
        emit(
            serde_json::json!({"event":"successor-state","stage":stage,"state":state,"rows":rows}),
        );
    }
    async fn end_control(
        &self,
        owner: ControlSession,
        driver: Driver,
        release: bool,
        backend: Option<&Backend>,
    ) {
        self.snapshot("before-control-end").await;
        let identity = owner.identity();
        if release {
            bounded(owner.release()).await.unwrap();
        } else {
            // A refused read may have poisoned this session. Do not attempt a
            // release or invent terminal SQL state for that actual claimant.
            drop(owner);
        }
        let failed = self.qualification.is_some_and(|value| value.loss.is_some());
        assert!(!failed || !release);
        crate::ingest_closing::join(driver, failed).await;
        if let Some(backend) = backend {
            emit(
                serde_json::json!({"event":"successor-driver","stage":self.mode.name(),
                "failed":failed,"joined":true}),
            );
            self.require_backend_absent(backend).await;
        }
        emit(
            serde_json::json!({"event":"successor-control","stage":self.mode.name(),
            "phase":if release {"released-and-joined"} else {"dropped-and-joined"},
            "resource":hex(identity.resource_id.as_bytes()),"epoch":identity.epoch.get()}),
        );
        self.snapshot("after-control-end").await;
    }
    async fn read(
        &self,
        owner: &mut ControlSession,
        recovery: &IngestRecovery,
    ) -> Result<(), Refusal> {
        let history = bounded(owner.inspect_prepared_initial_history(
            recovery,
            InitialHistoryRequest {
                ingest_root: &self.path,
                policy_ttl: self.policy_ttl(),
                cancelled: &self.cancelled,
            },
            grant(&self.budget, 128 * 1024, 0),
            |document, digest| self.validate(document, digest),
        ))
        .await
        .map_err(|error| Refusal::at("history-load", None, error))?;
        let spec = history.registration();
        emit_registration(spec, &self.mode.event("registration"));
        self.snapshot("loaded").await;
        let source = history
            .source_resolution(grant(&self.budget, 4096, 4096))
            .map_err(|error| Refusal::at("source-start", None, error))?;
        let source = self
            .resolve_originals(
                owner,
                recovery,
                spec,
                source,
                history.issued_through(),
                Some(&history),
            )
            .await?;
        let handoff = history
            .reconstruct_handoff(&source)
            .map_err(|error| Refusal::at("reconstruct", None, error))?;
        self.emit_proof(&source, &handoff);
        self.snapshot("proof").await;
        Ok(())
    }
    async fn complete_gap(
        &self,
        owner: &mut ControlSession,
        recovery: &mut IngestRecovery,
        cancel_before_completion: bool,
    ) -> Result<(), Refusal> {
        let gap = bounded(owner.inspect_prepared_initial_gap(
            recovery,
            InitialHistoryRequest {
                ingest_root: &self.path,
                policy_ttl: self.policy_ttl(),
                cancelled: &self.cancelled,
            },
            grant(&self.budget, 128 * 1024, 0),
            |document, digest| self.validate(document, digest),
        ))
        .await
        .map_err(|error| Refusal::at("gap-load", None, error))?;
        let spec = gap.registration();
        emit_registration(spec, &self.mode.event("registration"));
        self.snapshot("loaded").await;
        let closing = bounded(owner.observe_initial_retirement(
            recovery,
            ProducerCloseRequest {
                ingest_root: &self.path,
                registration: spec,
                policy_ttl: self.policy_ttl(),
                cancelled: &self.cancelled,
            },
            grant(&self.budget, 4096, 4096),
            |document, digest| self.validate(document, digest),
        ))
        .await
        .map_err(|error| Refusal::at("source-start", None, error))?;
        let source = closing
            .source_resolution(recovery)
            .map_err(|error| Refusal::at("source-start", None, error))?;
        self.snapshot("source-started").await;
        let source = self
            .resolve_originals(owner, recovery, spec, source, gap.issued_through(), None)
            .await?;
        let plan = gap
            .reconstruct_handoff(&source)
            .map_err(|error| Refusal::at("reconstruct", None, error))?;
        self.emit_proof(&source, plan.handoff());
        // Only the owned, positively reconstructed plan crosses dispatch. Its
        // metadata grant and actual root/sequence guard survive this borrow.
        drop(source);
        self.snapshot("proof").await;
        if cancel_before_completion {
            self.cancelled.store(true, Ordering::Relaxed);
            emit(serde_json::json!({"event":self.mode.event("cancellation"),"cancelled":true}));
        }
        self.snapshot("before-completion").await;
        let completion = bounded(owner.complete_prepared_initial_gap(
            recovery,
            &plan,
            InitialHistoryRequest {
                ingest_root: &self.path,
                policy_ttl: self.policy_ttl(),
                cancelled: &self.cancelled,
            },
            grant(&self.budget, 8192, 8192),
            |document, digest| self.validate(document, digest),
        ))
        .await;
        if let Ok(observed) = &completion {
            emit(serde_json::json!({"event":self.mode.event("completion"),
                "handoff":hex(observed.handoff().encode().as_bytes()),"admission":false}));
        }
        // Capture both returned success and returned refusal. A timeout above
        // remains an unknown process outcome with its partial evidence retained.
        self.snapshot("after-completion").await;
        completion.map_err(|error| Refusal::at("completion", None, error))?;
        Ok(())
    }
    async fn resolve_originals<'a>(
        &self,
        owner: &mut ControlSession,
        recovery: &'a IngestRecovery,
        spec: &InitialRegistration,
        mut source: SourceResolution<'a>,
        issued: u64,
        history: Option<&PreparedInitialHistory<'a>>,
    ) -> Result<ResolvedSource<'a>, Refusal> {
        assert!(issued <= 64);
        for sequence in 1..=issued {
            let policy = bounded(owner.refresh(|document, digest| self.validate(document, digest)))
                .await
                .map_err(|error| Refusal::at("policy-refresh", Some(sequence), error))?;
            self.emit_policy(owner, "refreshed", Some(sequence), policy);
            self.snapshot(&format!("refreshed-{sequence}")).await;
            let (input, outcome) = self.replay_grants();
            let request = ProducerResolveRequest {
                ingest_root: &self.path,
                registration: spec,
                request: ProducerRequestId {
                    producer_slot_id: spec.producer_slot_id,
                    producer_epoch: ProducerEpoch::new(1).unwrap(),
                    request_sequence: RequestSequence::new(sequence).unwrap(),
                },
                limits: recovery_limits(),
                policy_ttl: self.policy_ttl(),
                cancelled: &self.cancelled,
            };
            let resolved = if let Some(history) = history {
                bounded(owner.inspect_prepared_initial_request(
                    history,
                    request,
                    input,
                    outcome,
                    |document, digest| self.validate(document, digest),
                ))
                .await
            } else {
                bounded(owner.inspect_initial_retirement_request(
                    recovery,
                    request,
                    input,
                    outcome,
                    |document, digest| self.validate(document, digest),
                ))
                .await
            }
            .map_err(|error| Refusal::at("request", Some(sequence), error))?;
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
                .map_err(|error| Refusal::at("source-record", Some(sequence), error))?;
            emit(
                serde_json::json!({"event":self.mode.event("request"),"sequence":sequence,
                "outcome":outcome,"receipt":receipt,"rejection":rejection}),
            );
            drop(resolved);
            self.snapshot(&format!("resolved-{sequence}")).await;
        }
        source
            .finish()
            .map_err(|error| Refusal::at("source-finish", None, error))
    }
    fn emit_proof(&self, source: &ResolvedSource<'_>, handoff: &InitialHandoff) {
        emit(serde_json::json!({"event":self.mode.event("proof"),
            "issued_through":source.resolved_requests(),"last_complete_sequence":source.last_complete_sequence(),
            "closure_digest":hex(&source.digest()),"previous_manifest":hex(handoff.previous_manifest().encode().as_bytes()),
            "target_manifest":hex(handoff.next_manifest().encode().as_bytes()),"handoff":hex(handoff.encode().as_bytes())}));
    }
}

fn emit_registration(s: &InitialRegistration, event: &str) {
    emit(serde_json::json!({"event":event,
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
