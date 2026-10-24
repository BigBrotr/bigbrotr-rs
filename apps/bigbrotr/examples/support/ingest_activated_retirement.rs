//! Finite final-retirement waits with continuously retained native source proof.
//! Ordinary expiry remains strict; explicit compacted modes confirm the ancestor
//! before genuine target replay. The harness owns locks and any post-proof drift.
use super::*;
use bigbrotr::storage::postgres::control_owner::{
    ActivatedInitialResolveRequest, InitialHistoryRequest,
};
use bigbrotr::storage::rocks::store::AdmissionOutcome;
use std::sync::atomic::Ordering;

pub(super) const MODE: &str = "successor-activated-retirement-expiry";

pub(super) fn is_compacted_mode(mode: &str) -> bool {
    matches!(
        mode,
        "successor-activated-retirement-compacted-cancel"
            | "successor-activated-retirement-compacted-expiry"
            | "successor-activated-retirement-compacted-gate"
    )
}

struct Failure {
    stage: &'static str,
    sequence: Option<u64>,
    error: String,
}
impl Failure {
    fn at(stage: &'static str, sequence: Option<u64>, error: impl std::fmt::Debug) -> Self {
        Self {
            stage,
            sequence,
            error: format!("{error:?}"),
        }
    }
}

async fn operation<T, E: std::fmt::Debug>(
    stage: &'static str,
    sequence: Option<u64>,
    future: impl Future<Output = Result<T, E>>,
) -> Result<T, Failure> {
    match tokio::time::timeout(Duration::from_secs(10), future).await {
        Ok(result) => result.map_err(|error| Failure::at(stage, sequence, error)),
        Err(_) => Err(Failure {
            stage,
            sequence,
            error: "TimeoutOutcomeUnknown".into(),
        }),
    }
}

pub(super) async fn run(
    probe: Probe,
    recovery: IngestRecovery,
    observer_driver: Driver,
    gate: &Path,
    instance: DatasetId,
) {
    run_in(probe, recovery, observer_driver, gate, instance, None).await;
}

pub(super) async fn run_compacted(
    probe: Probe,
    recovery: IngestRecovery,
    observer_driver: Driver,
    instance: DatasetId,
    wait: Wait,
) {
    let gate = wait.prefix.clone();
    run_in(
        probe,
        recovery,
        observer_driver,
        &gate,
        instance,
        Some(wait),
    )
    .await;
}

async fn run_in(
    probe: Probe,
    recovery: IngestRecovery,
    observer_driver: Driver,
    gate: &Path,
    instance: DatasetId,
    wait: Option<Wait>,
) {
    if let Some(wait) = &wait {
        emit(
            serde_json::json!({"event":"retirement-compacted-mode","command":wait.command,
            "mode":wait.mode,"metadata_bytes":132096,"original_predecessor_replay":false,
            "native_writes":false,"refresh_after_proof":false}),
        );
    }
    bounded(
        probe
            .observer
            .batch_execute("SET max_parallel_workers_per_gather=0"),
    )
    .await
    .unwrap();
    assert_eq!(recovery.manifest().identity().owner_epoch.get(), 2);
    // Preclaim overlap: retained metadata + closing proof coexist with one
    // original replay, final retirement, never a vector.
    let metadata = grant(
        &probe.budget,
        if wait.is_some() { 132096 } else { 128 * 1024 },
        0,
    );
    let closing = grant(&probe.budget, 4096, 4096);
    drop(probe.replay_grants());
    let final_permit = grant(&probe.budget, 128 * 1024, 0);
    let effects = wait.as_ref().map(|_| effect_grant(&probe));
    drop((final_permit, effects));
    let (mut owner, driver) = probe
        .acquire_identity(
            "retirement-expiry",
            recovery.manifest().identity().dataset_id,
            instance,
            3,
        )
        .await;
    let backend = probe.observe_backend_at(&owner, "retirement-expiry").await;
    let result = exercise(
        &probe,
        &mut owner,
        &recovery,
        gate,
        metadata,
        closing,
        wait.as_ref(),
    )
    .await;
    let mut result_event = serde_json::json!({"event":"retirement-expiry-result",
        "stage":result.as_ref().err().map_or("retirement", |e|e.stage),
        "sequence":result.as_ref().err().and_then(|e|e.sequence),
        "gaps":result.as_ref().ok(),"error":result.as_ref().err().map(|e|&e.error)});
    if let Some(wait) = &wait {
        result_event["command"] = serde_json::json!(wait.command);
        result_event["mode"] = serde_json::json!(wait.mode);
    }
    emit(result_event);
    let expected = result.as_ref().is_err_and(|error| {
        error.stage == "retirement"
            && error.sequence.is_none()
            && error.error
                == if wait.as_ref().is_some_and(|w| w.mode == "cancel") {
                    "Native(AuthorityUnavailable)"
                } else {
                    "Control(Backend)"
                }
    });
    let returned = if wait.is_some() {
        compacted_clock(&probe, &owner).await
    } else {
        clock(&probe, &owner).await
    };
    emit_clock("returned", &returned);
    let expired = returned
        .as_ref()
        .is_ok_and(|time| time.server_us >= time.expires_us);
    let deadline_unchanged = wait.as_ref().is_none_or(|wait| {
        wait.emit_clock("returned", &returned);
        returned
            .as_ref()
            .is_ok_and(|time| Some(time.expires_us) == wait.deadline.get())
    });
    probe.snapshot("retirement-expiry-after-retirement").await;
    let reuse = operation(
        "reuse",
        None,
        owner.refresh(|doc, digest| probe.validate(doc, digest)),
    )
    .await;
    emit(
        serde_json::json!({"event":"retirement-expiry-reuse","available":reuse.is_ok(),
        "error":reuse.as_ref().err().map(|e|&e.error)}),
    );
    let poisoned = reuse
        .as_ref()
        .is_err_and(|error| error.error == "Unavailable");
    probe.snapshot("retirement-expiry-after-reuse").await;
    drop(recovery);
    probe
        .snapshot("retirement-expiry-final-before-control-end")
        .await;
    let identity = owner.identity();
    drop(owner); // No release is claimed after the refused operation.
    let joined = join(driver, "control").await;
    emit(
        serde_json::json!({"event":"successor-control","stage":"retirement-expiry",
        "phase":if joined {"dropped-and-joined"} else {"failed-see-custody-events"},
        "resource":hex(identity.resource_id.as_bytes()),"epoch":identity.epoch.get()}),
    );
    let absent = operation("backend-absence", None, async {
        loop {
            let present: bool = probe.observer.query_one(
                "SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_stat_activity
                 WHERE pid=$1::integer AND backend_start=$2::text::timestamptz AND datname=$3::text)",
                &[&backend.pid, &backend.start, &backend.database],
            ).await?.get(0);
            if !present { break; }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        Ok::<(), tokio_postgres::Error>(())
    }).await;
    emit(
        serde_json::json!({"event":"retirement-expiry-backend-absence",
        "error":absent.as_ref().err().map(|e|&e.error)}),
    );
    if absent.is_ok() {
        backend.emit(false);
    }
    probe
        .snapshot("retirement-expiry-final-after-control-end")
        .await;
    probe.snapshot("retirement-expiry-final").await;
    let probe_cancelled = probe.cancelled.load(Ordering::Relaxed);
    drop(probe);
    let observer_joined = join(observer_driver, "observer").await;
    let custody = joined && absent.is_ok() && observer_joined;
    if let Some(wait) = &wait {
        // A gate-only run reports the actual Result. Its harness must prove the
        // exact mutation and rejection; an arbitrary refusal is not a PASS.
        let operation_returned = result.is_ok()
            || result.as_ref().is_err_and(|e| {
                e.stage == "retirement"
                    && e.sequence.is_none()
                    && e.error != "TimeoutOutcomeUnknown"
            });
        let expected_refusal = match wait.mode {
            "cancel" => Some(expected && probe_cancelled && poisoned),
            "expiry" => Some(expected && expired && poisoned),
            "gate" => None,
            _ => unreachable!(),
        };
        let complete = custody
            && deadline_unchanged
            && !wait.invalid_signal.get()
            && operation_returned
            && expected_refusal.unwrap_or(true);
        emit(
            serde_json::json!({"event":"retirement-expiry-complete","command":wait.command,
            "mode":wait.mode,"native":"closed","control":if joined {"dropped-and-joined"}
                else {"failed-see-custody-events"},"admission":false,
            "expected_refusal":expected_refusal,"custody_confirmed":custody,
            "operation_returned":operation_returned,"cancelled":probe_cancelled,
            "deadline_unchanged":deadline_unchanged,"invalid_signal":wait.invalid_signal.get()}),
        );
        assert!(complete, "preserved compacted final-job schedule failure");
    } else {
        let passed = expected && expired && poisoned && custody;
        emit(
            serde_json::json!({"event":"retirement-expiry-complete","command":MODE,
            "native":"closed","control":if joined {"dropped-and-joined"} else {"failed-see-custody-events"},
            "admission":false,"expected_refusal":passed}),
        );
        assert!(
            passed,
            "preserved final-job UTC expiry qualification failure"
        );
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "existing finite expiry call with explicit compacted wait"
)]
async fn exercise(
    probe: &Probe,
    owner: &mut ControlSession,
    recovery: &IngestRecovery,
    gate: &Path,
    metadata: OwnedPermit,
    closing: OwnedPermit,
    wait: Option<&Wait>,
) -> Result<u64, Failure> {
    let request = || InitialHistoryRequest {
        ingest_root: &probe.path,
        policy_ttl: probe.policy_ttl(),
        cancelled: &probe.cancelled,
    };
    let validate = |doc: &str, digest| probe.validate(doc, digest);
    let observed = operation("load", None, async {
        if wait.is_some() {
            owner
                .inspect_activated_initial_retirement_with_compacted(
                    recovery,
                    request(),
                    metadata,
                    recovery_limits().event,
                    effect_grant(probe),
                    validate,
                )
                .await
        } else {
            owner
                .inspect_activated_initial_retirement(recovery, request(), metadata, validate)
                .await
        }
    })
    .await?;
    probe.snapshot("retirement-expiry-loaded").await;
    if wait.is_some()
        && (observed.issued_through() != observed.reserved_through()
            || observed.issued_through() > 1)
    {
        return Err(Failure::at(
            "load",
            None,
            "FixtureExpectedZeroOrOneOriginal",
        ));
    }
    if wait.is_none() && (observed.issued_through(), observed.reserved_through()) != (2, 2) {
        return Err(Failure::at("load", None, "FixtureExpectedTwoOriginals"));
    }
    let closed = operation(
        "producer-observe",
        None,
        owner.observe_activated_initial_retirement(
            recovery,
            observed,
            request(),
            closing,
            validate,
        ),
    )
    .await?;
    probe.snapshot("retirement-expiry-producer-observed").await;
    let mut prefix = if wait.is_some() {
        if !closed.has_compacted_initial() {
            return Err(Failure::at(
                "source-start",
                None,
                "FixtureExpectedCompactedAncestor",
            ));
        }
        closed.compacted_source_resolution(recovery, &probe.cancelled)
    } else {
        closed.source_resolution(recovery)
    }
    .map_err(|error| Failure::at("source-start", None, error))?;
    let compacted_requests = if let Some(wait) = wait {
        let count = operation(
            "compacted-predecessor",
            None,
            owner.inspect_activated_retirement_compacted_predecessor(
                recovery,
                &closed,
                request(),
                recovery_limits().event,
                effect_grant(probe),
                &mut prefix,
                validate,
            ),
        )
        .await?;
        emit(
            serde_json::json!({"event":"retirement-compacted-predecessor",
            "command":wait.command,"mode":wait.mode,"compacted_requests":count,
            "original_requests_replayed":0,"needs_compacted_initial":prefix.needs_compacted_initial()}),
        );
        probe
            .snapshot("retirement-expiry-compacted-predecessor")
            .await;
        if count == 0 || prefix.needs_compacted_initial() {
            return Err(Failure::at(
                "compacted-predecessor",
                None,
                "FixturePendingAncestor",
            ));
        }
        count
    } else {
        0
    };
    for sequence in 1..=closed.reserved_through() {
        operation("refresh", Some(sequence), owner.refresh(validate)).await?;
        probe
            .snapshot(&format!("retirement-expiry-refreshed-{sequence}"))
            .await;
        let (input, outcome) = probe.replay_grants();
        let resolved = operation(
            "request",
            Some(sequence),
            owner.inspect_activated_initial_retirement_request(
                recovery,
                &closed,
                ActivatedInitialResolveRequest {
                    ingest_root: &probe.path,
                    request: ProducerRequestId {
                        producer_slot_id: closed.producer_slot_id(),
                        producer_epoch: ProducerEpoch::new(2).unwrap(),
                        request_sequence: RequestSequence::new(sequence).unwrap(),
                    },
                    limits: recovery_limits(),
                    policy_ttl: probe.policy_ttl(),
                    cancelled: &probe.cancelled,
                },
                input,
                outcome,
                validate,
            ),
        )
        .await?;
        let (outcome, receipt, rejection) = match resolved.outcome() {
            AdmissionOutcome::Durable(receipt) => (
                "durable",
                Some(hex(&receipt.encode_persistent(64 * 1024).map_err(
                    |error| Failure::at("request-encoding", Some(sequence), error),
                )?)),
                None,
            ),
            AdmissionOutcome::Rejected(reason) => ("rejected", None, Some(format!("{reason:?}"))),
        };
        emit(
            serde_json::json!({"event":"retirement-expiry-request","sequence":sequence,
            "outcome":outcome,"receipt":receipt,"rejection":rejection}),
        );
        prefix
            .record(&resolved)
            .map_err(|error| Failure::at("source-record", Some(sequence), error))?;
        drop(resolved);
        probe
            .snapshot(&format!("retirement-expiry-resolved-{sequence}"))
            .await;
    }
    if wait.is_some() {
        // Last refresh precedes the completed source proof and every gate.
        operation("final-refresh", None, owner.refresh(validate)).await?;
        probe.snapshot("retirement-expiry-final-refreshed").await;
    }
    let source = prefix
        .finish()
        .map_err(|error| Failure::at("source-finish", None, error))?;
    emit(
        serde_json::json!({"event":"retirement-expiry-proof","resolved_requests":source.resolved_requests(),
        "last_complete_sequence":source.last_complete_sequence(),"sha256":hex(&source.digest())}),
    );
    if let Some(wait) = wait {
        if source.compacted_requests() != compacted_requests {
            return Err(Failure::at(
                "source-finish",
                None,
                "FixtureCompactedCountChanged",
            ));
        }
        emit(serde_json::json!({"event":"retirement-compacted-proof",
            "command":wait.command,"mode":wait.mode,"compacted_requests":source.compacted_requests(),
            "original_requests_replayed":source.resolved_requests(),
            "last_complete_sequence":source.last_complete_sequence(),"sha256":hex(&source.digest())}));
        wait.gate(probe, owner).await?;
        wait.dispatch(probe, owner).await?;
        return wait
            .pending(
                probe,
                operation(
                    "retirement",
                    None,
                    owner.retire_activated_initial_producer_with_compacted(
                        &closed,
                        &source,
                        request(),
                        grant(&probe.budget, 128 * 1024, 0),
                        recovery_limits().event,
                        effect_grant(probe),
                        validate,
                    ),
                ),
            )
            .await;
    }
    operation("final-refresh", None, owner.refresh(validate)).await?;
    probe.snapshot("retirement-expiry-final-refreshed").await;
    let refreshed = clock(probe, owner).await;
    emit_clock("refreshed", &refreshed);
    let refreshed = refreshed?;
    emit(
        serde_json::json!({"event":"retirement-expiry-gate","gate":"before-retirement","phase":"waiting"}),
    );
    let file = gate.with_extension("go");
    operation("gate", None, async {
        while !file.try_exists()? {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        if !file.is_file() {
            return Err(std::io::Error::other("gate must be a regular file"));
        }
        Ok::<(), std::io::Error>(())
    })
    .await?;
    emit(
        serde_json::json!({"event":"retirement-expiry-gate","gate":"before-retirement","phase":"released"}),
    );
    // Outside any product transaction, observe this genuine lease approaching
    // expiry. No refresh or SQL mutation may replace it before retirement.
    let aged = tokio::time::timeout(Duration::from_secs(22), async {
        loop {
            let time = clock(probe, owner).await?;
            if time.expires_us != refreshed.expires_us {
                return Err(Failure::at("preage", None, "FixtureLeaseChanged"));
            }
            if time.expires_us - time.server_us <= 2_000_000 {
                return Ok(time);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        Err(Failure {
            stage: "preage",
            sequence: None,
            error: "TimeoutOutcomeUnknown".into(),
        })
    });
    emit_clock("dispatch", &aged);
    let aged = aged?;
    if !(1_000_000..=2_000_000).contains(&(aged.expires_us - aged.server_us)) {
        return Err(Failure::at(
            "preage",
            None,
            "FixtureMissedLiveDispatchWindow",
        ));
    }
    operation(
        "retirement",
        None,
        owner.retire_activated_initial_producer(
            &closed,
            &source,
            request(),
            grant(&probe.budget, 128 * 1024, 0),
            validate,
        ),
    )
    .await
}

struct Clock {
    server_us: i64,
    expires_us: i64,
}
async fn clock(probe: &Probe, owner: &ControlSession) -> Result<Clock, Failure> {
    let identity = owner.identity();
    operation("clock", None, async {
        let row = probe
            .observer
            .query_one(
                "SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,lease_expires_at_us
             FROM control.owner_epochs WHERE resource_id=$1::bytea AND epoch=$2::text::numeric",
                &[
                    &&identity.resource_id.as_bytes()[..],
                    &identity.epoch.get().to_string(),
                ],
            )
            .await?;
        Ok::<Clock, tokio_postgres::Error>(Clock {
            server_us: row.get(0),
            expires_us: row.get(1),
        })
    })
    .await
}
fn emit_clock(stage: &str, result: &Result<Clock, Failure>) {
    emit(
        serde_json::json!({"event":"retirement-expiry-clock","stage":stage,
        "server_us":result.as_ref().ok().map(|t|t.server_us),
        "lease_expires_at_us":result.as_ref().ok().map(|t|t.expires_us),
        "remaining_us":result.as_ref().ok().map(|t|t.expires_us-t.server_us),
        "error":result.as_ref().err().map(|e|&e.error)}),
    );
}
async fn join(mut driver: Driver, stage: &str) -> bool {
    let (result, aborted) = match tokio::time::timeout(Duration::from_secs(5), &mut driver).await {
        Ok(result) => (result, false),
        Err(_) => {
            driver.abort();
            (driver.await, true)
        }
    };
    let error = match result {
        Ok(Ok(())) => None,
        Ok(Err(error)) => Some(format!("{error:?}")),
        Err(error) => Some(format!("{error:?}")),
    };
    emit(
        serde_json::json!({"event":"retirement-expiry-driver","stage":stage,
        "joined":true,"failed":error.is_some(),"aborted":aborted,"error":error}),
    );
    error.is_none() && !aborted
}

fn effect_grant(probe: &Probe) -> OwnedPermit {
    grant(
        &probe.budget,
        compacted_outcome_data_bytes(recovery_limits().event).unwrap(),
        0,
    )
}

// Finite fixture gates follow the existing successor retirement wait protocol.
// The harness owns any real job lock and any SQL drift; this client only reads
// exact empty signal files and the genuine current Control clock.
pub(super) struct Wait {
    command: &'static str,
    mode: &'static str,
    prefix: PathBuf,
    go: PathBuf,
    cancel: PathBuf,
    deadline: Cell<Option<i64>>,
    invalid_signal: Cell<bool>,
}
impl Wait {
    pub(super) fn new(command: &str, prefix: &Path) -> Self {
        let (command, mode) = match command {
            "successor-activated-retirement-compacted-cancel" => {
                ("successor-activated-retirement-compacted-cancel", "cancel")
            }
            "successor-activated-retirement-compacted-expiry" => {
                ("successor-activated-retirement-compacted-expiry", "expiry")
            }
            "successor-activated-retirement-compacted-gate" => {
                ("successor-activated-retirement-compacted-gate", "gate")
            }
            _ => panic!("explicit compacted terminal mode required"),
        };
        let text = prefix.to_str().expect("UTF-8 gate prefix");
        assert!(prefix.is_absolute() && text.len() <= 512 && prefix.file_name().is_some());
        assert!(prefix.components().all(|part| matches!(
            part,
            std::path::Component::RootDir | std::path::Component::Normal(_)
        )));
        assert!(
            std::fs::symlink_metadata(prefix.parent().unwrap())
                .unwrap()
                .file_type()
                .is_dir()
        );
        let path = |suffix: &str| {
            let mut path = prefix.as_os_str().to_owned();
            path.push(suffix);
            PathBuf::from(path)
        };
        let value = Self {
            command,
            mode,
            prefix: prefix.into(),
            go: path(".go"),
            cancel: path(".cancel"),
            deadline: Cell::new(None),
            invalid_signal: Cell::new(false),
        };
        // Called by dispatch before native inspection or Control acquisition.
        assert!(!signal_present(&value.go).expect("valid absent go signal"));
        assert!(!signal_present(&value.cancel).expect("valid absent cancel signal"));
        value
    }

    async fn gate(&self, probe: &Probe, owner: &ControlSession) -> Result<(), Failure> {
        let observed = compacted_clock(probe, owner).await;
        self.emit_clock("proof", &observed);
        self.deadline.set(Some(observed?.expires_us));
        self.emit_gate("waiting");
        operation("gate", None, async {
            loop {
                if signal_present(&self.cancel)? {
                    return Err(std::io::Error::other("cancel preceded final dispatch"));
                }
                if signal_present(&self.go)? {
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await?;
        self.emit_gate("released");
        Ok(())
    }

    async fn dispatch(&self, probe: &Probe, owner: &ControlSession) -> Result<(), Failure> {
        let observed = tokio::time::timeout(Duration::from_secs(22), async {
            loop {
                let time = compacted_clock(probe, owner).await?;
                if Some(time.expires_us) != self.deadline.get() {
                    return Err(Failure::at("preage", None, "FixtureLeaseChanged"));
                }
                if self.mode != "expiry" || time.expires_us - time.server_us <= 2_000_000 {
                    return Ok(time);
                }
                let sleep_us =
                    u64::try_from((time.expires_us - time.server_us - 2_000_000).min(1_000_000))
                        .expect("positive preage interval");
                tokio::time::sleep(Duration::from_micros(sleep_us)).await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            Err(Failure {
                stage: "preage",
                sequence: None,
                error: "TimeoutOutcomeUnknown".into(),
            })
        });
        self.emit_clock("dispatch", &observed);
        let time = observed?;
        if (self.mode == "expiry"
            && !(1_000_000..=2_000_000).contains(&(time.expires_us - time.server_us)))
            || (self.mode != "expiry" && time.server_us >= time.expires_us)
        {
            return Err(Failure::at(
                "preage",
                None,
                "FixtureMissedLiveDispatchWindow",
            ));
        }
        if signal_present(&self.cancel).map_err(|e| Failure::at("gate", None, e))? {
            return Err(Failure::at("gate", None, "CancelBeforeDispatch"));
        }
        Ok(())
    }

    async fn pending<T>(&self, probe: &Probe, future: impl Future<Output = T>) -> T {
        if self.mode != "cancel" {
            return future.await;
        }
        tokio::pin!(future);
        let mut signalled = false;
        loop {
            tokio::select! {
                // Cancellation never drops or substitutes the actual operation.
                // Its existing ten-second timeout remains an unknown outcome.
                biased;
                result = &mut future => return result,
                () = tokio::time::sleep(Duration::from_millis(5)), if !signalled => {
                    match signal_present(&self.cancel) {
                        Ok(false) => (),
                        Ok(true) => {
                            probe.cancelled.store(true, Ordering::Relaxed);
                            signalled = true;
                            emit(serde_json::json!({"event":"retirement-compacted-cancel",
                                "command":self.command,"mode":self.mode,"phase":"applied",
                                "cancelled":probe.cancelled.load(Ordering::Relaxed)}));
                        }
                        Err(error) => {
                            // Preserve the actual future and Result even for an
                            // invalid fixture signal, then fail the fixture gate.
                            self.invalid_signal.set(true);
                            signalled = true;
                            emit(serde_json::json!({"event":"retirement-compacted-cancel",
                                "command":self.command,"mode":self.mode,"phase":"invalid-signal",
                                "error":format!("{error:?}")}));
                        }
                    }
                }
            }
        }
    }

    fn emit_gate(&self, phase: &str) {
        emit(
            serde_json::json!({"event":"retirement-expiry-gate","command":self.command,
            "mode":self.mode,"gate":"before-retirement","phase":phase,"prefix":self.prefix,
            "go":self.go,"cancel":self.cancel,"gate_timeout_ms":10000,"poll_ms":5}),
        );
    }

    fn emit_clock(&self, stage: &str, result: &Result<Clock, Failure>) {
        emit(
            serde_json::json!({"event":"retirement-compacted-clock","command":self.command,
            "mode":self.mode,"stage":stage,"server_us":result.as_ref().ok().map(|t|t.server_us),
            "lease_expires_at_us":result.as_ref().ok().map(|t|t.expires_us),
            "remaining_us":result.as_ref().ok().map(|t|t.expires_us-t.server_us),
            "error":result.as_ref().err().map(|e|&e.error)}),
        );
    }
}

fn signal_present(path: &Path) -> std::io::Result<bool> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() && metadata.len() == 0 => Ok(true),
        Ok(_) => Err(std::io::Error::other(
            "signal must be an empty regular nonsymlink file",
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

async fn compacted_clock(probe: &Probe, owner: &ControlSession) -> Result<Clock, Failure> {
    let id = owner.identity();
    operation("clock", None, async {
        let row = probe
            .observer
            .query_one(
                "SELECT (EXTRACT(EPOCH FROM pg_catalog.clock_timestamp())*1000000)::bigint,
                o.lease_expires_at_us
             FROM control.owner_epochs o JOIN pg_catalog.pg_stat_activity a
               ON a.pid=o.control_backend_pid AND a.backend_start=o.control_backend_start
             WHERE o.resource_id=$1::bytea AND o.epoch=$2::text::numeric
               AND o.process_incarnation=$3::bytea AND o.role='control' AND o.state='active'
               AND a.datname=current_database()",
                &[
                    &&id.resource_id.as_bytes()[..],
                    &id.epoch.get().to_string(),
                    &&id.process_incarnation.as_bytes()[..],
                ],
            )
            .await?;
        Ok::<Clock, tokio_postgres::Error>(Clock {
            server_us: row.get(0),
            expires_us: row.get(1),
        })
    })
    .await
}
