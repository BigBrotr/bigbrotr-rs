//! Finite prepared-only succession of the retained signed, retired initial seed.
//! The local proof stays in this process; this is not process-restart recovery.
use super::*;
use bigbrotr::storage::{
    postgres::{
        control_owner::{
            InitialSuccessionError, InitialSuccessionRequest, ProducerCloseRequest,
            ProducerResolveRequest,
        },
        ingest_registry,
    },
    rocks::store::{
        AdmissionLimits, IngestRecovery, InitialHandoff, ProducerRecoveryLimits,
        RetainedRequestInput,
    },
};
use bigbrotr_model::{
    DatasetId, ProducerEpoch, ProducerRequestId, RequestSequence,
    codec::{FactDecodeLimits, request::RequestDecodeLimits},
};
use std::{future::Future, path::PathBuf};

type Driver = tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>;
const CONTROL_TTL: Duration = Duration::from_secs(20);

struct Probe {
    config: Config,
    initial_proxy_port: Option<u16>,
    root: config::Resolved,
    path: PathBuf,
    budget: ResourceBudget,
    observer: tokio_postgres::Client,
    cancelled: AtomicBool,
}

struct Attempt {
    handoff: InitialHandoff,
    role: [u8; 16],
    created_at_us: i64,
    lease_expires_at_us: i64,
    window: u32,
}

/// The actual PostgreSQL incarnation observed before disrupting the transport.
/// A PID alone could select a later backend and cannot establish exclusion.
struct Backend {
    pid: i32,
    start: String,
    database: String,
    control_epoch: u64,
}
impl Backend {
    fn emit(&self, present: bool) {
        emit(
            serde_json::json!({"event":"successor-backend","stage":"initial",
            "pid":self.pid,"backend_start":self.start,"database":self.database,
            "control_epoch":self.control_epoch,"present":present}),
        );
    }
}

// The laboratory supplies the process deadline too. A timeout is a failed run
// with retained unknown state, never a claimed SQL rollback or native absence.
async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .expect("successor probe deadline; preserve all outcomes")
}

pub async fn run(config: Config, mode: &str, config_file: &Path, proxy_port: Option<u16>) {
    assert!(matches!(
        mode,
        "successor-normal"
            | "successor-gap"
            | "successor-gap-only"
            | "successor-old-schema"
            | "successor-rollback"
            | "successor-commit-before"
            | "successor-commit-after"
    ));
    let commit_fault = matches!(mode, "successor-commit-before" | "successor-commit-after");
    assert_eq!(proxy_port.is_some(), commit_fault);
    if let Some(port) = proxy_port {
        assert_ne!(port, 0);
        assert!(
            config.get_ports().is_empty(),
            "one proxy port for the initial host"
        );
    }
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
    // Metadata and finite raw laboratory snapshots coexist with one replay.
    let _metadata = grant(&budget, 16 * MIB, 0);
    let mut recovery =
        IngestRecovery::inspect(&path, dataset, limits(), grant(&budget, 0, 128 * MIB)).unwrap();
    let (mut observer, observer_driver) = bounded(config.connect(NoTls)).await.unwrap();
    let observer_driver = tokio::spawn(observer_driver);
    let spec = bounded(ingest_registry::load_historical(
        &mut observer,
        instance,
        &recovery,
    ))
    .await
    .unwrap();
    let probe = Probe {
        config,
        initial_proxy_port: proxy_port,
        root,
        path,
        budget,
        observer,
        cancelled: AtomicBool::new(false),
    };
    // Reserve the same worst-case decode/replay envelope as installed recovery
    // before the first claim. Resolve one original at a time, never a vector.
    drop(probe.replay_grants());
    let (mut owner, mut driver) = probe.acquire("initial", &spec, 1).await;
    let attempt = probe.plan(&mut owner, &recovery, &spec).await;
    probe.snapshot("before-prepare").await;

    match mode {
        "successor-commit-before" | "successor-commit-after" => {
            // Claim is COMMIT 1, observation 2, four refresh/inspection pairs
            // 3..10, final policy refresh 11, and preparation 12. Tuple reads
            // and all snapshots use the independent direct observer connection.
            let backend = probe.observe_backend(&owner).await;
            let error = probe
                .prepare(
                    &mut owner,
                    &mut recovery,
                    &spec,
                    &attempt,
                    "commit-unknown",
                    false,
                    false,
                )
                .await
                .expect_err("lost preparation COMMIT response");
            assert_eq!(error, InitialSuccessionError::CommitUnknown);
            probe.check_poisoned(&mut owner).await;
            // These are observations only. Client/driver termination cannot
            // distinguish a committed intent from a still-live SQL transaction.
            probe.snapshot("commit-unknown").await;
            probe.end_control("unknown", owner, driver, false).await;
            probe.require_backend_absent(&backend).await;
            (owner, driver) = probe.acquire("completion", &spec, 2).await;
            probe
                .prepare(
                    &mut owner,
                    &mut recovery,
                    &spec,
                    &attempt,
                    "completed",
                    false,
                    false,
                )
                .await
                .unwrap();
            probe.snapshot("completed").await;
        }
        "successor-normal" => {
            probe
                .prepare(
                    &mut owner,
                    &mut recovery,
                    &spec,
                    &attempt,
                    "prepared",
                    false,
                    false,
                )
                .await
                .unwrap();
            probe.snapshot("prepared").await;
            probe
                .prepare(
                    &mut owner,
                    &mut recovery,
                    &spec,
                    &attempt,
                    "same-control-retry",
                    false,
                    false,
                )
                .await
                .unwrap();
            probe.snapshot("same-control-retry").await;
            probe.end_control("first", owner, driver, true).await;
            (owner, driver) = probe.acquire("fresh", &spec, 2).await;
            probe
                .prepare(
                    &mut owner,
                    &mut recovery,
                    &spec,
                    &attempt,
                    "fresh-control-retry",
                    false,
                    false,
                )
                .await
                .unwrap();
            probe.snapshot("fresh-control-retry").await;
        }
        "successor-gap" | "successor-gap-only" => {
            let error = probe
                .prepare(
                    &mut owner,
                    &mut recovery,
                    &spec,
                    &attempt,
                    "prepared-native-old",
                    true,
                    false,
                )
                .await
                .expect_err("post-prepare policy refusal");
            assert_eq!(
                error,
                InitialSuccessionError::Control(OwnerError::InvalidPolicy)
            );
            probe.check_poisoned(&mut owner).await;
            probe.snapshot("prepared-native-old").await;
            if mode == "successor-gap-only" {
                // End this real preparation process with the confirmed SQL
                // intent and old native store. A later process receives only
                // their durable state, never this retained local plan.
                drop(recovery);
                probe.end_control("final", owner, driver, false).await;
                probe.snapshot("final").await;
                emit(serde_json::json!({"event":"successor-complete","mode":mode,
                    "native":"closed","control":"dropped-and-joined","admission":false}));
                drop(probe);
                crate::ingest_closing::join(observer_driver, false).await;
                return;
            }
            probe.end_control("gap", owner, driver, false).await;
            (owner, driver) = probe.acquire("conflict", &spec, 2).await;
            let error = probe
                .prepare(
                    &mut owner,
                    &mut recovery,
                    &spec,
                    &attempt,
                    "conflict-refused",
                    false,
                    true,
                )
                .await
                .expect_err("changed target role refused");
            assert_eq!(
                error,
                InitialSuccessionError::Control(OwnerError::InvalidMetadata)
            );
            probe.check_poisoned(&mut owner).await;
            probe.snapshot("conflict-refused").await;
            probe.end_control("conflict", owner, driver, false).await;
            (owner, driver) = probe.acquire("completion", &spec, 3).await;
            probe
                .prepare(
                    &mut owner,
                    &mut recovery,
                    &spec,
                    &attempt,
                    "completed",
                    false,
                    false,
                )
                .await
                .unwrap();
            probe.snapshot("completed").await;
        }
        "successor-old-schema" | "successor-rollback" => {
            let stage = if mode == "successor-old-schema" {
                "old-schema-refused"
            } else {
                "rollback-refused"
            };
            let error = probe
                .prepare(
                    &mut owner,
                    &mut recovery,
                    &spec,
                    &attempt,
                    stage,
                    false,
                    false,
                )
                .await
                .expect_err("required refusal");
            assert_eq!(
                error,
                InitialSuccessionError::Control(if mode == "successor-old-schema" {
                    OwnerError::IncompatibleDeployment
                } else {
                    OwnerError::Backend
                })
            );
            probe.check_poisoned(&mut owner).await;
            probe.snapshot(stage).await;
            drop(recovery);
            probe.end_control("final", owner, driver, false).await;
            probe.snapshot("final").await;
            emit(serde_json::json!({"event":"successor-complete","mode":mode,
                "native":"closed","control":"dropped-and-joined","admission":false}));
            drop(probe);
            crate::ingest_closing::join(observer_driver, false).await;
            return;
        }
        _ => unreachable!(),
    }
    drop(recovery);
    probe.end_control("final", owner, driver, true).await;
    probe.snapshot("final").await;
    emit(serde_json::json!({"event":"successor-complete","mode":mode,
        "native":"closed","control":"released-and-joined","admission":false}));
    drop(probe);
    crate::ingest_closing::join(observer_driver, false).await;
}

impl Probe {
    fn validate(&self, document: &str, digest: [u8; 32]) -> Result<(), OwnerError> {
        crate::config::validate_fixture_policy(document, digest, &self.root)
    }

    fn policy_ttl(&self) -> Duration {
        Duration::from_micros(self.root.number("runtime.authorization_ttl"))
    }

    async fn snapshot(&self, stage: &str) {
        let state = bounded(crate::ingest_snapshot::state(&self.observer, &self.path)).await;
        let (rows, sequence) = raw_state(&self.path);
        assert_eq!(state["sequence"].as_u64(), Some(sequence));
        emit(serde_json::json!({"event":"successor-state","stage":stage,
            "state":state,"rows":rows}));
    }

    async fn acquire(
        &self,
        stage: &str,
        spec: &InitialRegistration,
        ordinal: u8,
    ) -> (ControlSession, Driver) {
        assert!((1..=3).contains(&ordinal));
        let role = [210 + ordinal; 16];
        let incarnation = [220 + ordinal; 16];
        emit(
            serde_json::json!({"event":"successor-control","stage":stage,
            "phase":"claim-planned","role":hex(&role),"incarnation":hex(&incarnation),
            "lease_ttl_us":CONTROL_TTL.as_micros().to_string()}),
        );
        self.snapshot(&format!("{stage}-before-claim")).await;
        let mut config = self.config.clone();
        if ordinal == 1
            && let Some(port) = self.initial_proxy_port
        {
            config.port(port);
        }
        let (client, driver) = bounded(config.connect(NoTls)).await.unwrap();
        let driver = tokio::spawn(driver);
        let (owner, _) = bounded(ControlSession::acquire(
            client,
            ClaimRequest {
                dataset: spec.dataset_id,
                instance: spec.instance_id.try_into().unwrap(),
                authority_mode: self.root.text("config.authority_mode").unwrap().into(),
                role_instance_id: role,
                incarnation: incarnation.try_into().unwrap(),
                lease_ttl: CONTROL_TTL,
            },
            |doc, digest| self.validate(doc, digest),
            &Cell::new(false),
        ))
        .await
        .unwrap();
        emit(
            serde_json::json!({"event":"successor-control","stage":stage,
            "phase":"claimed","resource":hex(owner.identity().resource_id.as_bytes()),
            "epoch":owner.identity().epoch.get(),"role":hex(&role),
            "incarnation":hex(&incarnation)}),
        );
        self.snapshot(&format!("{stage}-after-claim")).await;
        (owner, driver)
    }

    async fn end_control(&self, stage: &str, owner: ControlSession, driver: Driver, release: bool) {
        self.snapshot(&format!("{stage}-before-control-end")).await;
        let identity = owner.identity();
        if release {
            bounded(owner.release()).await.unwrap();
        } else {
            // A failed operation poisons the session. Dropping it closes the
            // actual backend; it does not mark its retained PG row released.
            drop(owner);
        }
        let failed = self.initial_proxy_port.is_some() && stage == "unknown";
        assert!(!failed || !release);
        crate::ingest_closing::join(driver, failed).await;
        if failed {
            emit(serde_json::json!({"event":"successor-driver","stage":stage,
                "failed":true,"joined":true}));
        }
        emit(
            serde_json::json!({"event":"successor-control","stage":stage,
            "phase":if release {"released-and-joined"} else {"dropped-and-joined"},
            "resource":hex(identity.resource_id.as_bytes()),"epoch":identity.epoch.get()}),
        );
        self.snapshot(&format!("{stage}-after-control-end")).await;
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
        // Preserve the server's JSON timestamp representation used by the full
        // owner-row snapshots; the later predicate parses this exact value.
        let value: serde_json::Value = serde_json::from_str(row.get::<_, &str>(0)).unwrap();
        let backend = Backend {
            pid: i32::try_from(value["pid"].as_i64().unwrap()).unwrap(),
            start: value["backend_start"].as_str().unwrap().into(),
            database: value["database"].as_str().unwrap().into(),
            control_epoch: identity.epoch.get(),
        };
        assert!(backend.pid > 0 && !backend.start.is_empty());
        assert_eq!(Some(backend.database.as_str()), self.config.get_dbname());
        backend.emit(true);
        backend
    }

    async fn require_backend_absent(&self, backend: &Backend) {
        // Poll the exact frozen tuple on the direct observer. This neither
        // terminates a backend nor infers server rollback from a dead client.
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
        backend.emit(false);
    }

    async fn check_poisoned(&self, owner: &mut ControlSession) {
        assert_eq!(
            bounded(owner.refresh(|doc, digest| self.validate(doc, digest))).await,
            Err(OwnerError::Unavailable)
        );
        emit(serde_json::json!({"event":"successor-session-poisoned",
            "control_epoch":owner.identity().epoch.get()}));
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

    async fn plan(
        &self,
        owner: &mut ControlSession,
        recovery: &IngestRecovery,
        spec: &InitialRegistration,
    ) -> Attempt {
        let closed = bounded(owner.observe_initial_retirement(
            recovery,
            ProducerCloseRequest {
                ingest_root: &self.path,
                registration: spec,
                policy_ttl: self.policy_ttl(),
                cancelled: &self.cancelled,
            },
            grant(&self.budget, 4096, 4096),
            |doc, digest| self.validate(doc, digest),
        ))
        .await
        .unwrap();
        assert_eq!(closed.issued_through(), 4, "frozen signed retired seed");
        assert_eq!(closed.reserved_through(), 4);
        let mut prefix = closed.source_resolution(recovery).unwrap();
        for sequence in 1..=4 {
            bounded(owner.refresh(|doc, digest| self.validate(doc, digest)))
                .await
                .unwrap();
            let (input, outcome) = self.replay_grants();
            let resolved = bounded(owner.inspect_initial_retirement_request(
                recovery,
                ProducerResolveRequest {
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
                },
                input,
                outcome,
                |doc, digest| self.validate(doc, digest),
            ))
            .await
            .unwrap();
            prefix.record(&resolved).unwrap();
            drop(resolved);
        }
        let source = prefix.finish().unwrap();
        assert_eq!(source.last_complete_sequence(), 12);
        let policy = bounded(owner.refresh(|doc, digest| self.validate(doc, digest)))
            .await
            .unwrap();
        let domains: Vec<_> = spec
            .allocation_domains
            .iter()
            .map(|(kind, _)| (*kind, [160 + *kind as u8; 16].try_into().unwrap()))
            .collect();
        let previous = recovery.manifest();
        let next = IngestManifest::for_successor(
            ManifestIdentity {
                owner_epoch: OwnerEpoch::new(2).unwrap(),
                topology_generation: TopologyGeneration::new(
                    previous
                        .identity()
                        .topology_generation
                        .get()
                        .checked_add(1)
                        .unwrap(),
                ),
                process_incarnation: [202; 16].try_into().unwrap(),
                policy_epoch: PolicyEpoch::new(policy.policy_revision),
                ..*previous.identity()
            },
            AllocationNamespaces::from_entries(&domains).unwrap(),
        );
        let handoff = InitialHandoff::from_source(&source, next).unwrap();
        drop(source);
        let created_at_us: i64 = bounded(self.observer.query_one(
            "SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint",
            &[],
        ))
        .await
        .unwrap()
        .get(0);
        let attempt = Attempt {
            handoff,
            role: [201; 16],
            created_at_us,
            lease_expires_at_us: created_at_us.checked_add(60_000_000).unwrap(),
            window: spec.outstanding_request_limit,
        };
        emit(serde_json::json!({"event":"successor-plan",
            "dataset":hex(spec.dataset_id.as_bytes()),"instance":hex(&spec.instance_id),
            "resource":hex(spec.ingest_resource_id.as_bytes()),"producer_slot":hex(spec.producer_slot_id.as_bytes()),
            "previous_epoch":1,"target_epoch":2,"shard":spec.shard_id,
            "previous_topology":previous.identity().topology_generation.get(),
            "target_topology":next.identity().topology_generation.get(),
            "target_incarnation":hex(next.identity().process_incarnation.as_bytes()),
            "recovery_generation":hex(next.identity().recovery_generation.as_bytes()),
            "target_role":hex(&attempt.role),"created_at_us":attempt.created_at_us,
            "lease_expires_at_us":attempt.lease_expires_at_us,"window":attempt.window,
            "policy_revision":policy.policy_revision,"config_revision":policy.config_revision,
            "policy_digest":hex(&policy.policy_digest),"config_digest":hex(&policy.config_digest),
            "domains":domains.iter().map(|(kind, domain)|serde_json::json!([*kind as u16,hex(domain.as_bytes())])).collect::<Vec<_>>(),
            "issued_through":attempt.handoff.issued_through(),
            "last_complete_sequence":attempt.handoff.last_complete_sequence(),
            "closure_digest":hex(&attempt.handoff.closure_digest()),
            "previous_manifest":hex(previous.encode().as_bytes()),
            "target_manifest":hex(next.encode().as_bytes()),
            "handoff":hex(attempt.handoff.encode().as_bytes())}));
        attempt
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "finite fixture selects an explicit phase fault"
    )]
    async fn prepare(
        &self,
        owner: &mut ControlSession,
        recovery: &mut IngestRecovery,
        spec: &InitialRegistration,
        attempt: &Attempt,
        stage: &str,
        gap: bool,
        conflict: bool,
    ) -> Result<(), InitialSuccessionError> {
        let role = if conflict { [203; 16] } else { attempt.role };
        emit(
            serde_json::json!({"event":"successor-attempt","stage":stage,
            "role":hex(&role),"control_epoch":owner.identity().epoch.get()}),
        );
        let mut validations = 0;
        let result = bounded(owner.prepare_initial_successor(
            recovery,
            InitialSuccessionRequest {
                ingest_root: &self.path,
                previous_registration: spec,
                handoff: &attempt.handoff,
                role_instance_id: role,
                created_at_us: attempt.created_at_us,
                lease_expires_at_us: attempt.lease_expires_at_us,
                outstanding_request_limit: attempt.window,
                policy_ttl: self.policy_ttl(),
                cancelled: &self.cancelled,
            },
            grant(&self.budget, 128 * 1024, 8192),
            |document, digest| {
                self.validate(document, digest)?;
                validations += 1;
                if gap && validations == 3 {
                    // Both first-transaction documents were really validated.
                    // This callback runs after its confirmed preparation COMMIT.
                    return Err(OwnerError::InvalidPolicy);
                }
                Ok(())
            },
        ))
        .await;
        emit(serde_json::json!({"event":"successor-result","stage":stage,
            "control_epoch":owner.identity().epoch.get(),"validations":validations,
            "error":result.as_ref().err().map(|error|format!("{error:?}"))}));
        if gap {
            assert_eq!(validations, 3);
        } else if result.is_ok() {
            assert_eq!(validations, 4);
        } else if stage == "old-schema-refused" {
            assert_eq!(validations, 0);
        } else {
            assert_eq!(validations, 2);
        }
        result.map(|observed| {
            assert_eq!(
                observed.handoff().encode().as_bytes(),
                attempt.handoff.encode().as_bytes()
            );
            drop(observed);
        })
    }
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
