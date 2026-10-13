//! Real Control UTC expiry after the final claimed-job row wait. This finite
//! client holds the native source proof continuously; it never rewrites a lease.
use super::*;
use bigbrotr::storage::postgres::control_owner::{
    ActivatedInitialResolveRequest, InitialHistoryRequest,
};
use bigbrotr::storage::rocks::store::AdmissionOutcome;

pub(super) const MODE: &str = "successor-activated-acquisition-expiry";

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
    mut recovery: IngestRecovery,
    observer_driver: Driver,
    gate: &Path,
    instance: DatasetId,
) {
    assert_eq!(recovery.manifest().identity().owner_epoch.get(), 2);
    // Preclaim overlap: retained metadata + closing proof coexist with one
    // original replay, source publication or final abandonment, never a vector.
    let metadata = grant(&probe.budget, 128 * 1024, 0);
    let closing = grant(&probe.budget, 4096, 4096);
    drop(probe.replay_grants());
    drop(grant(&probe.budget, 128 * 1024, 0));
    let (mut owner, driver) = probe
        .acquire_identity(
            "expiry",
            recovery.manifest().identity().dataset_id,
            instance,
            3,
        )
        .await;
    let backend = probe.observe_backend_at(&owner, "expiry").await;
    let result = exercise(&probe, &mut owner, &mut recovery, gate, metadata, closing).await;
    emit(serde_json::json!({"event":"activated-expiry-result",
        "stage":result.as_ref().err().map_or("abandonment", |e|e.stage),
        "sequence":result.as_ref().err().and_then(|e|e.sequence),
        "jobs":result.as_ref().ok(),"error":result.as_ref().err().map(|e|&e.error)}));
    let expected = result.as_ref().is_err_and(|error| {
        error.stage == "abandonment"
            && error.sequence.is_none()
            && error.error == "Control(Unavailable)"
    });
    let returned = clock(&probe, &owner).await;
    emit_clock("returned", &returned);
    let expired = returned
        .as_ref()
        .is_ok_and(|time| time.server_us >= time.expires_us);
    probe.snapshot("expiry-after-abandonment").await;
    let reuse = operation(
        "reuse",
        None,
        owner.refresh(|doc, digest| probe.validate(doc, digest)),
    )
    .await;
    emit(
        serde_json::json!({"event":"activated-expiry-reuse","available":reuse.is_ok(),
        "error":reuse.as_ref().err().map(|e|&e.error)}),
    );
    let poisoned = reuse
        .as_ref()
        .is_err_and(|error| error.error == "Unavailable");
    probe.snapshot("expiry-after-reuse").await;
    drop(recovery);
    probe.snapshot("expiry-final-before-control-end").await;
    let identity = owner.identity();
    drop(owner); // No release is claimed after the refused operation.
    let joined = join(driver, "control").await;
    emit(
        serde_json::json!({"event":"successor-control","stage":"expiry",
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
        serde_json::json!({"event":"activated-expiry-backend-absence",
        "error":absent.as_ref().err().map(|e|&e.error)}),
    );
    if absent.is_ok() {
        backend.emit(false);
    }
    probe.snapshot("expiry-final-after-control-end").await;
    probe.snapshot("expiry-final").await;
    drop(probe);
    let observer_joined = join(observer_driver, "observer").await;
    let passed = expected && expired && poisoned && joined && absent.is_ok() && observer_joined;
    emit(
        serde_json::json!({"event":"activated-expiry-complete","command":MODE,
        "native":"closed","control":if joined {"dropped-and-joined"} else {"failed-see-custody-events"},
        "admission":false,"expected_refusal":passed}),
    );
    assert!(
        passed,
        "preserved final-job UTC expiry qualification failure"
    );
}

async fn exercise(
    probe: &Probe,
    owner: &mut ControlSession,
    recovery: &mut IngestRecovery,
    gate: &Path,
    metadata: OwnedPermit,
    closing: OwnedPermit,
) -> Result<u64, Failure> {
    let request = || InitialHistoryRequest {
        ingest_root: &probe.path,
        policy_ttl: probe.policy_ttl(),
        cancelled: &probe.cancelled,
    };
    let validate = |doc: &str, digest| probe.validate(doc, digest);
    let observed = operation(
        "load",
        None,
        owner.inspect_activated_initial_source(recovery, request(), metadata, validate),
    )
    .await?;
    probe.snapshot("expiry-loaded").await;
    if (observed.issued_through(), observed.reserved_through()) != (2, 2) {
        return Err(Failure::at("load", None, "FixtureExpectedTwoOriginals"));
    }
    let closed = operation(
        "producer-close",
        None,
        owner.close_activated_initial_source_producer(
            recovery,
            observed,
            request(),
            closing,
            validate,
        ),
    )
    .await?;
    probe.snapshot("expiry-producer-closed").await;
    let mut prefix = closed
        .source_resolution(recovery)
        .map_err(|error| Failure::at("source-start", None, error))?;
    for sequence in 1..=closed.reserved_through() {
        operation("refresh", Some(sequence), owner.refresh(validate)).await?;
        probe
            .snapshot(&format!("expiry-refreshed-{sequence}"))
            .await;
        let (input, outcome) = probe.replay_grants();
        let resolved = operation(
            "request",
            Some(sequence),
            owner.resolve_activated_initial_source_request(
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
            serde_json::json!({"event":"activated-expiry-request","sequence":sequence,
            "outcome":outcome,"receipt":receipt,"rejection":rejection}),
        );
        prefix
            .record(&resolved)
            .map_err(|error| Failure::at("source-record", Some(sequence), error))?;
        drop(resolved);
        probe.snapshot(&format!("expiry-resolved-{sequence}")).await;
    }
    let source = prefix
        .finish()
        .map_err(|error| Failure::at("source-finish", None, error))?;
    emit(
        serde_json::json!({"event":"activated-expiry-proof","resolved_requests":source.resolved_requests(),
        "last_complete_sequence":source.last_complete_sequence(),"sha256":hex(&source.digest())}),
    );
    operation("source-refresh", None, owner.refresh(validate)).await?;
    probe.snapshot("expiry-source-refreshed").await;
    operation(
        "source-close",
        None,
        owner.close_activated_initial_source(
            &closed,
            &source,
            request(),
            grant(&probe.budget, 64 * 1024, 0),
            validate,
        ),
    )
    .await?;
    probe.snapshot("expiry-source-closed").await;
    operation("final-refresh", None, owner.refresh(validate)).await?;
    probe.snapshot("expiry-final-refreshed").await;
    let refreshed = clock(probe, owner).await;
    emit_clock("refreshed", &refreshed);
    let refreshed = refreshed?;
    emit(
        serde_json::json!({"event":"activated-expiry-gate","gate":"before-abandonment","phase":"waiting"}),
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
        serde_json::json!({"event":"activated-expiry-gate","gate":"before-abandonment","phase":"released"}),
    );
    // Outside any product transaction, observe this genuine lease approaching
    // expiry. No refresh or SQL mutation may replace it before abandonment.
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
        "abandonment",
        None,
        owner.abandon_activated_initial_acquisition(
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
        serde_json::json!({"event":"activated-expiry-clock","stage":stage,
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
        serde_json::json!({"event":"activated-expiry-driver","stage":stage,
        "joined":true,"failed":error.is_some(),"aborted":aborted,"error":error}),
    );
    error.is_none() && !aborted
}
