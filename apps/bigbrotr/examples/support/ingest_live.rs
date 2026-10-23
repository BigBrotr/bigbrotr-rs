//! Finite connected activation from the signed retired seed. This is not an
//! installed import executor or recovery of a departed activated incarnation.
use super::*;
use bigbrotr::storage::{
    postgres::control_owner::{
        InitialEventError, InitialHistoryRequest, LiveInitialDictionaryAppendRequest,
        LiveInitialEventRequest, LiveInitialWriter,
    },
    rocks::store::{AdmissionOutcome, PreparedEvent, RejectionReason, StoredReceipt},
};
use bigbrotr_model::{DatasetLineageDescriptor, EventIntent, SignedEvent};
use sha2::{Digest, Sha256};

#[path = "ingest_live_durable.rs"]
mod durable;
#[path = "ingest_live_refusals.rs"]
mod refusals;

pub(super) fn is_mode(mode: &str) -> bool {
    durable::Mode::parse(mode).is_some()
        || matches!(
            mode,
            "successor-live-zero"
                | "successor-live-compacted-history-seed"
                | "successor-live-rejected"
                | "successor-live-guards"
                | "successor-live-normal"
                | "successor-live-off"
                | "successor-live-commit-before"
                | "successor-live-commit-after"
                | "successor-live-manifest"
                | "successor-live-renew-control"
                | "successor-live-renew-expired"
                | "successor-live-renew-before"
                | "successor-live-renew-after"
                | "successor-live-reservation-after"
        )
}

pub(super) async fn run(
    probe: Probe,
    recovery: IngestRecovery,
    spec: InitialRegistration,
    observer_driver: Driver,
    mode: &str,
    gate_prefix: Option<&Path>,
) {
    assert!(is_mode(mode));
    let mut candidate = InitialWriterCandidate::new(recovery).unwrap();
    let (mut owner, driver) = probe.acquire("initial", &spec, 1).await;
    let attempt = probe
        .plan(
            &mut owner,
            candidate.recovery(),
            &spec,
            Some(&candidate),
            Duration::from_secs(if mode == "successor-live-renew-expired" {
                5
            } else {
                60
            }),
        )
        .await;
    probe.snapshot("before-prepare").await;
    let observed = bounded(owner.prepare_live_initial_successor(
        &mut candidate,
        InitialSuccessionRequest {
            ingest_root: &probe.path,
            previous_registration: &spec,
            handoff: &attempt.handoff,
            role_instance_id: attempt.role,
            created_at_us: attempt.created_at_us,
            lease_expires_at_us: attempt.lease_expires_at_us,
            outstanding_request_limit: attempt.window,
            policy_ttl: probe.policy_ttl(),
            cancelled: &probe.cancelled,
        },
        grant(&probe.budget, 128 * 1024, 8192),
        |doc, digest| probe.validate(doc, digest),
    ))
    .await
    .unwrap();
    assert_eq!(
        observed.handoff().encode().as_bytes(),
        attempt.handoff.encode().as_bytes()
    );
    drop(observed);
    probe.snapshot("prepared").await;
    if mode == "successor-live-guards" {
        guards(&probe, &mut owner, gate_prefix.unwrap(), false).await;
    }
    // Exact backend custody is observed before the potential COMMIT ambiguity.
    // The PG observer and native snapshots bypass the deliberately faulted wire.
    let backend = probe.observe_backend(&owner).await;
    emit(
        serde_json::json!({"event":"live-activation-start","mode":mode,
        "control_epoch":owner.identity().epoch.get()}),
    );
    let activation = bounded(owner.activate_initial_writer(
        candidate,
        attempt.handoff,
        InitialHistoryRequest {
            ingest_root: &probe.path,
            policy_ttl: probe.policy_ttl(),
            cancelled: &probe.cancelled,
        },
        grant(&probe.budget, 128 * 1024, 8192),
        |doc, digest| probe.validate(doc, digest),
    ))
    .await;
    let mut writer = match activation {
        Ok(writer) => {
            assert!(
                !matches!(
                    mode,
                    "successor-live-commit-before" | "successor-live-commit-after"
                ),
                "activation fault must prevent writer delivery"
            );
            emit(
                serde_json::json!({"event":"live-activation-result","writer":true,
                "activated_at_us":writer.activated_at_us(),"error":null}),
            );
            writer
        }
        Err(error) => {
            assert!(matches!(
                mode,
                "successor-live-commit-before" | "successor-live-commit-after"
            ));
            assert_eq!(error, InitialSuccessionError::CommitUnknown);
            emit(
                serde_json::json!({"event":"live-activation-result","writer":false,
                "error":format!("{error:?}")}),
            );
            probe.check_poisoned(&mut owner).await;
            probe.snapshot("activation-unknown").await;
            probe.end_control("unknown", owner, driver, false).await;
            probe.require_backend_absent(&backend).await;
            probe.snapshot("final").await;
            emit(serde_json::json!({"event":"live-complete","mode":mode,
                "native":"closed","writer":false,"admission":false}));
            drop(probe);
            crate::ingest_closing::join(observer_driver, false).await;
            return;
        }
    };
    assert_eq!(writer.manifest().identity().owner_epoch.get(), 2);
    assert_eq!(writer.producer_slot_id(), spec.producer_slot_id);
    probe.snapshot("activated-zero").await;
    if mode == "successor-live-reservation-after" {
        reservation_after(&probe, owner, driver, writer, &spec, &backend).await;
        drop(probe);
        crate::ingest_closing::join(observer_driver, false).await;
        return;
    }
    if mode == "successor-live-rejected" {
        reject(&probe, &mut owner, &mut writer, &spec).await;
    }
    if let Some(durable_mode) = durable::Mode::parse(mode) {
        let passed = durable::run(&probe, &mut owner, &mut writer, &spec, durable_mode).await;
        if durable_mode.limit().is_some() || durable_mode == durable::Mode::ZeroClaim {
            let prefix = if durable_mode == durable::Mode::ZeroClaim {
                "zero-claim"
            } else {
                "limit"
            };
            drop(writer);
            let stopped = finish_limits(&probe, owner, driver, &backend, passed, prefix).await;
            probe.snapshot("final").await;
            drop(probe);
            let observer = join_limit_driver(observer_driver, "observer", prefix).await;
            emit(serde_json::json!({"event":"live-complete","mode":mode,
                "native":"closed","control":if passed && stopped {
                    "released-and-joined"
                } else { "failed-see-custody-events" },
                "writer":false,"admission":durable_mode != durable::Mode::ZeroClaim,"passed":passed && stopped && observer}));
            assert!(
                passed && stopped && observer,
                "preserved count fixture failure"
            );
            return;
        }
    }
    if refusals::is_renewal(mode) {
        refusals::renewal(&probe, (owner, driver), writer, &spec, backend, mode).await;
        drop(probe);
        crate::ingest_closing::join(observer_driver, false).await;
        return;
    }
    if mode == "successor-live-manifest" {
        guards(&probe, &mut owner, gate_prefix.unwrap(), true).await;
    }
    if mode == "successor-live-guards" {
        bounded(owner.refresh(|doc, digest| probe.validate(doc, digest)))
            .await
            .unwrap();
        probe.snapshot("live-control-refreshed").await;
        let commit_started = Cell::new(false);
        bounded(owner.renew_live_initial_ingest(
            &writer,
            probe.policy_ttl(),
            |doc, digest| probe.validate(doc, digest),
            &commit_started,
        ))
        .await
        .unwrap();
        assert!(commit_started.get());
        emit(serde_json::json!({"event":"live-renewal-result",
            "commit_started":true,"error":null}));
        probe.snapshot("live-ingest-renewed").await;
    }
    if mode == "successor-live-compacted-history-seed" {
        compacted_history_dictionary(&probe, &mut owner, &mut writer, &spec).await;
    }
    if !matches!(
        mode,
        "successor-live-zero"
            | "successor-live-compacted-history-seed"
            | "successor-live-rejected"
            | "successor-live-already-durable"
            | "successor-live-acquisition-claims"
    ) {
        admit(
            &probe,
            &mut owner,
            &mut writer,
            &spec,
            mode != "successor-live-off",
        )
        .await;
    }
    drop(writer);
    probe.end_control("final", owner, driver, true).await;
    probe.require_backend_absent(&backend).await;
    probe.snapshot("final").await;
    emit(serde_json::json!({"event":"live-complete","mode":mode,
        "native":"closed","control":"released-and-joined",
        "writer":false,"admission":mode != "successor-live-zero"}));
    drop(probe);
    crate::ingest_closing::join(observer_driver, false).await;
    if mode == "successor-live-compacted-history-seed" {
        emit(
            serde_json::json!({"event":"compacted-history-seed-complete",
            "native":"closed","control":"released-and-joined","observer":"joined",
            "requests":1,"events":0}),
        );
    }
}

// The count and zero-claim fixtures use this failure-aware ending. Every operation error
// has already been emitted; release/driver errors remain separate observations.
async fn finish_limits(
    probe: &Probe,
    owner: ControlSession,
    driver: Driver,
    backend: &Backend,
    passed: bool,
    prefix: &str,
) -> bool {
    probe.snapshot("final-before-control-end").await;
    let identity = owner.identity();
    let released = if passed {
        let result = durable::limit_call(owner.release()).await;
        emit(
            serde_json::json!({"event":format!("live-{prefix}-release"), "attempted":true,
            "error":result.as_ref().err()}),
        );
        result.is_ok()
    } else {
        drop(owner);
        emit(
            serde_json::json!({"event":format!("live-{prefix}-release"), "attempted":false,
            "error":null}),
        );
        false
    };
    let joined = join_limit_driver(driver, "control", prefix).await;
    emit(
        serde_json::json!({"event":"successor-control","stage":"final",
        "phase":if released && joined {"released-and-joined"} else {"failed-see-custody-events"},
        "resource":hex(identity.resource_id.as_bytes()),"epoch":identity.epoch.get()}),
    );
    // Driver termination is not server-side exclusion. Observe the same frozen
    // backend tuple, and preserve observation failure without asserting first.
    let absent = durable::limit_call(async {
        loop {
            let present: bool = probe
                .observer
                .query_one(
                    "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity
                 WHERE pid=$1::integer AND backend_start=$2::text::timestamptz
                   AND datname=$3::text)",
                    &[&backend.pid, &backend.start, &backend.database],
                )
                .await?
                .get(0);
            if !present {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        Ok::<(), tokio_postgres::Error>(())
    })
    .await;
    emit(
        serde_json::json!({"event":format!("live-{prefix}-backend-absence"),
        "error":absent.as_ref().err()}),
    );
    if absent.is_ok() {
        backend.emit(false);
    }
    probe.snapshot("final-after-control-end").await;
    released && joined && absent.is_ok()
}

async fn join_limit_driver(mut driver: Driver, stage: &str, prefix: &str) -> bool {
    let (result, aborted) = match tokio::time::timeout(Duration::from_secs(5), &mut driver).await {
        Ok(result) => (result, false),
        Err(_) => {
            // This is our connection task only. Reap the abort; do not detach an
            // unknown client or infer PostgreSQL rollback from task cancellation.
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
        serde_json::json!({"event":format!("live-{prefix}-driver"),"stage":stage,
        "joined":true,"failed":error.is_some(),"aborted":aborted,"error":error}),
    );
    error.is_none() && !aborted
}

async fn guard_gate(prefix: &Path, gate: &str, extension: &str) {
    // Laboratory coordination only, with no SQL transaction held while waiting.
    let path = prefix.with_extension(extension);
    emit(serde_json::json!({"event":"live-guards-gate","gate":gate,"phase":"waiting"}));
    bounded(async {
        while !path.try_exists().unwrap() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(path.is_file());
    })
    .await;
    emit(serde_json::json!({"event":"live-guards-gate","gate":gate,"phase":"released"}));
}

async fn guards(probe: &Probe, owner: &mut ControlSession, prefix: &Path, manifest: bool) {
    let (schema, count, install, remove, event, refreshed, clean) = if manifest {
        (
            "live_manifest_fixture",
            2,
            "manifest-install",
            "manifest-remove",
            "live-manifest-result",
            "manifest-refreshed",
            "manifest-clean",
        )
    } else {
        (
            "live_guard_fixture",
            14,
            "install",
            "remove",
            "live-guards-result",
            "guards-refreshed",
            "guards-clean",
        )
    };
    guard_gate(prefix, install, "go").await;
    bounded(owner.refresh(|doc, digest| probe.validate(doc, digest)))
        .await
        .unwrap();
    let identity = owner.identity();
    let observed = bounded(probe.observer.query_one(
        &format!(
            "SELECT count(*)::bigint, coalesce(bool_and(g.status='PASS'
            AND g.backend_pid=c.control_backend_pid AND g.backend_start=c.control_backend_start
            AND g.database_name=current_database() AND g.database_user='bigbrotr_control'
            AND g.replication_role='origin' AND g.control_epoch=c.epoch),false)
         FROM {schema}.cases g CROSS JOIN control.owner_epochs c
         WHERE c.resource_id=$1::bytea AND c.epoch=$2::text::numeric"
        ),
        &[
            &&identity.resource_id.as_bytes()[..],
            &identity.epoch.get().to_string(),
        ],
    ))
    .await
    .unwrap();
    assert_eq!(observed.get::<_, i64>(0), count);
    assert!(observed.get::<_, bool>(1));
    let run = bounded(probe.observer.query_one(
        &format!("SELECT status,executions FROM {schema}.run WHERE singleton"),
        &[],
    ))
    .await
    .unwrap();
    assert_eq!(run.get::<_, &str>(0), "complete");
    assert_eq!(run.get::<_, i32>(1), 1);
    emit(serde_json::json!({"event":event,"cases":count,"passed":true}));
    probe.snapshot(refreshed).await;
    guard_gate(prefix, remove, "done").await;
    let removed: bool = bounded(probe.observer.query_one(
        &format!(
            "SELECT NOT EXISTS(SELECT 1 FROM pg_catalog.pg_trigger
            WHERE tgname='{schema}_refresh' AND tgrelid='control.owner_epochs'::regclass)
            AND to_regprocedure('{schema}.exercise()') IS NULL"
        ),
        &[],
    ))
    .await
    .unwrap()
    .get(0);
    assert!(removed, "only the temporary fixture hook must be removed");
    probe.snapshot(clean).await;
}

fn public_scope(previous: &InitialRegistration) -> QualifiedRef {
    // These coordinates come from the frozen original installed fixture. They
    // are expected inputs, not inferred from the effects of this new operation.
    QualifiedRef {
        dataset_id: previous.dataset_id,
        dictionary_kind: DictionaryKind::VisibilityScope,
        allocation_domain: previous
            .allocation_domains
            .iter()
            .find(|(kind, _)| *kind == DictionaryKind::VisibilityScope)
            .unwrap()
            .1,
        ordinal: Ordinal::new(3).unwrap(),
    }
}

async fn reject(
    probe: &Probe,
    owner: &mut ControlSession,
    writer: &mut LiveInitialWriter,
    previous: &InitialRegistration,
) {
    let (mut expected, sequence) = raw_state(&probe.path);
    assert_eq!((expected.len(), sequence), (52, 62));
    let inputs = probe.path.join("producer-inputs-v1");
    assert_eq!(std::fs::read_dir(&inputs).unwrap().count(), 4);
    let scope = public_scope(previous);
    // This typed coordinate is deliberately not allocated. A rejected import
    // never dereferences its lineage or produces facts/progress. This is a
    // finite exact-input grant, not an installed import or acquisition job.
    let lineage = QualifiedRef {
        dictionary_kind: DictionaryKind::DatasetLineage,
        allocation_domain: writer
            .manifest()
            .allocation_namespaces()
            .domain(DictionaryKind::DatasetLineage)
            .unwrap(),
        ordinal: Ordinal::new(1).unwrap(),
        ..scope
    };
    let corpus = include_bytes!("../../../../experiments/identity-contract/signed-fixture.json");
    let fixture: serde_json::Value = serde_json::from_slice(corpus).unwrap();
    let mut event: SignedEvent =
        serde_json::from_value(fixture["events"]["E_public"].clone()).unwrap();
    // Frozen E_public_invalid_signature in signed-event-admission.json changes
    // only signature byte0, 0x94 -> 0x95. The signed body and event ID stay exact.
    event.sig = "9544d1fab8095f14d43137f841a400f0648cd614515f53bb5545749e373056381ce2963d43fdb5d10d9294846da6fc895d1fdfaf3417399a5c2699414877c180"
        .parse()
        .unwrap();
    let request = ProducerRequestId {
        producer_slot_id: writer.producer_slot_id(),
        producer_epoch: ProducerEpoch::new(2).unwrap(),
        request_sequence: RequestSequence::new(1).unwrap(),
    };
    let intent = EventIntent::import(1, scope, event.clone(), lineage).unwrap();
    let preimage = intent.encode_request_preimage(request, 8192).unwrap();
    let digest = intent.request_digest(request);
    emit(
        serde_json::json!({"event":"live-rejected-input","request":1,
        "fixture":"E_public_invalid_signature","event_id":event.id.to_string(),
        "supplied_signature":event.sig.to_string(),"preimage":hex(&preimage),
        "request_digest":digest.to_string(),"provenance":false}),
    );
    let prepared =
        PreparedEvent::new(request, intent, 256 * 1024, grant(&probe.budget, MIB, 0)).unwrap();
    assert!(prepared.verified_event().is_none());
    let limits = AdmissionLimits {
        max_outstanding_requests: u64::from(writer.outstanding_request_limit()),
        max_dependencies: 256,
        ..AdmissionLimits::default()
    };
    let mut validations = 0;
    let result = bounded(owner.admit_live_initial_event(
        writer,
        LiveInitialEventRequest {
            ingest_root: &probe.path,
            event: prepared,
            relay_source: None,
            acquisition: None,
            provenance_enabled: false,
            recorded_at: ObservedTime::new(900_000_001),
            limits,
            policy_ttl: probe.policy_ttl(),
            cancelled: &probe.cancelled,
        },
        grant(
            &probe.budget,
            limits.required_data_bytes().unwrap(),
            limits.required_native_bytes().unwrap(),
        ),
        |doc, digest, candidate| {
            probe.validate(doc, digest)?;
            // The only PreparedEvent entering this call is the frozen local
            // input above; rejected bodies have no verified_event accessor.
            if candidate.request_id() != request
                || candidate.verification_limit_bytes() != 256 * 1024
                || candidate.visibility_scope_id() != scope
                || candidate.origin() != bigbrotr_model::EventOrigin::Import(lineage)
                || candidate.observation().is_some()
                || candidate.verified_event().is_some()
            {
                return Err(OwnerError::InvalidPolicy);
            }
            validations += 1;
            Ok(())
        },
    ))
    .await;
    // Retain the complete actual state before asserting the expected outcome.
    probe.snapshot("rejected").await;
    let outcome = result.unwrap();
    assert_eq!(
        outcome.outcome(),
        &AdmissionOutcome::Rejected(RejectionReason::Signature)
    );
    assert_eq!(validations, 4);
    drop(outcome);
    let producer_key = hex(&[
        &[1, 0x31][..],
        request.producer_slot_id.as_bytes(),
        &2_u64.to_be_bytes(),
    ]
    .concat());
    let request_key = hex(&[
        &[1, 0x30][..],
        request.producer_slot_id.as_bytes(),
        &2_u64.to_be_bytes(),
        &1_u64.to_be_bytes(),
    ]
    .concat());
    let marker = expected
        .iter_mut()
        .find(|r| r["cf"] == "receipts" && r["key"] == producer_key)
        .unwrap();
    assert_eq!(marker["value"], hex(&[&[1, 0x31][..], &[0; 24]].concat()));
    marker["value"] = hex(&[&[1, 0x31][..], &1_u64.to_be_bytes(), &[0; 16]].concat()).into();
    let rejection = [
        &b"bigbrotr/rejection/1\0"[..],
        request.producer_slot_id.as_bytes(),
        &2_u64.to_be_bytes(),
        &1_u64.to_be_bytes(),
        digest.as_bytes(),
        previous.dataset_id.as_bytes(),
        &writer
            .manifest()
            .identity()
            .policy_epoch
            .get()
            .to_be_bytes(),
        &[4],
    ]
    .concat();
    expected.push(serde_json::json!({"cf":"receipts","key":request_key,"value":hex(&rejection)}));
    expected.sort_unstable_by(|a, b| {
        (a["cf"].as_str().unwrap(), a["key"].as_str().unwrap())
            .cmp(&(b["cf"].as_str().unwrap(), b["key"].as_str().unwrap()))
    });
    let (actual, after_sequence) = raw_state(&probe.path);
    assert_eq!((actual.len(), after_sequence), (53, 64));
    assert_eq!(
        actual, expected,
        "only rejection and producer issued marker change"
    );
    let input = inputs.join(format!(
        "{}-0000000000000002-0000000000000001.cbor",
        request.producer_slot_id
    ));
    assert_eq!(std::fs::read(input).unwrap(), preimage);
    assert_eq!(std::fs::read_dir(inputs).unwrap().count(), 5);
    emit(
        serde_json::json!({"event":"live-rejection","request":1,"reason":"Signature",
        "outcome":actual.iter().find(|r| r["cf"] == "receipts" && r["key"] == request_key).unwrap()["value"],
        "validations":validations}),
    );
}

fn first_dictionary(writer: &LiveInitialWriter, previous: &InitialRegistration) -> EvidenceIntent {
    let scope = public_scope(previous);
    dictionary_for_scope(writer, scope)
}

fn dictionary_for_scope(writer: &LiveInitialWriter, scope: QualifiedRef) -> EvidenceIntent {
    let lineage = QualifiedRef {
        dictionary_kind: DictionaryKind::DatasetLineage,
        allocation_domain: writer
            .manifest()
            .allocation_namespaces()
            .domain(DictionaryKind::DatasetLineage)
            .unwrap(),
        ordinal: Ordinal::new(1).unwrap(),
        ..scope
    };
    let corpus = include_bytes!("../../../../experiments/identity-contract/signed-fixture.json");
    EvidenceIntent::new(
        1,
        scope,
        vec![
            DictionaryEntry::new(
                lineage,
                DictionaryDescriptor::DatasetLineage(DatasetLineageDescriptor {
                    format: DescriptorLabel::new("bigbrotr-signed-fixture".into()).unwrap(),
                    format_version: 1,
                    source_dataset_id: None,
                    source_artifact_digest: Digest256::from_bytes(Sha256::digest(corpus).into()),
                }),
            )
            .unwrap(),
        ],
        EvidenceIntentLimits {
            max_entries: 1,
            max_encoded_intent_bytes: 8192,
            max_validation_scratch_bytes: 4096,
        },
    )
    .unwrap()
}

async fn compacted_history_dictionary(
    probe: &Probe,
    owner: &mut ControlSession,
    writer: &mut LiveInitialWriter,
    previous: &InitialRegistration,
) {
    assert!(probe.compacted_history_seed);
    let mut scope = public_scope(previous);
    assert_eq!(
        hex(scope.dataset_id.as_bytes()),
        "309d9507c5b5c39e9947e1406482d519"
    );
    assert_eq!(
        hex(scope.allocation_domain.as_bytes()),
        "0e2a2cc69f18bd685ff795bd57faf3c0"
    );
    scope.ordinal = Ordinal::new(1).unwrap();
    // The dfbb ACK seed has public visibility at1:1; the legacy seed's3
    // coordinate is private here. Bind the frozen source/index bytes before
    // appending, independently of what this new admission returns.
    let (rows, sequence) = raw_state(&probe.path);
    assert_eq!((rows.len(), sequence), (91, 117));
    let fact_key = "0120309d9507c5b5c39e9947e1406482d5190000000000000000000000010000000000000001";
    let fact = rows
        .iter()
        .find(|r| r["cf"] == "facts" && r["key"] == fact_key)
        .unwrap();
    assert!(
        fact["value"]
            .as_str()
            .unwrap()
            .starts_with("01202885c061c7e1e4c3db59da67b32540440f465827ddc8932b37719fec8d92d204")
    );
    let index_key =
        "0104309d9507c5b5c39e9947e1406482d51900040e2a2cc69f18bd685ff795bd57faf3c00000000000000001";
    let index = rows
        .iter()
        .find(|r| r["cf"] == "default" && r["key"] == index_key)
        .unwrap();
    assert_eq!(
        index["value"],
        "0104309d9507c5b5c39e9947e1406482d51900000000000000000000000100000000000000012885c061c7e1e4c3db59da67b32540440f465827ddc8932b37719fec8d92d204"
    );
    let intent = dictionary_for_scope(writer, scope);
    let limits = AdmissionLimits {
        max_outstanding_requests: 2,
        max_dependencies: 256,
        ..AdmissionLimits::default()
    };
    let receipt = append_first_dictionary(probe, owner, writer, &intent, limits)
        .await
        .unwrap();
    emit(
        serde_json::json!({"event":"live-receipt","stage":"dictionary","request":1,
        "receipt":hex(&receipt.receipt().encode_persistent(65536).unwrap())}),
    );
    drop(receipt);
    probe.snapshot("dictionary").await;
}

async fn append_first_dictionary(
    probe: &Probe,
    owner: &mut ControlSession,
    writer: &mut LiveInitialWriter,
    intent: &EvidenceIntent,
    limits: AdmissionLimits,
) -> Result<StoredReceipt, InitialEventError> {
    bounded(owner.append_live_initial_dictionaries(
        writer,
        LiveInitialDictionaryAppendRequest {
            ingest_root: &probe.path,
            request_sequence: RequestSequence::new(1).unwrap(),
            intent,
            recorded_at: ObservedTime::new(900_000_001),
            limits,
            policy_ttl: probe.policy_ttl(),
            cancelled: &probe.cancelled,
        },
        grant(
            &probe.budget,
            limits.required_dictionary_data_bytes().unwrap(),
            limits.required_native_bytes().unwrap(),
        ),
        |doc, digest| probe.validate(doc, digest),
    ))
    .await
}

async fn reservation_after(
    probe: &Probe,
    mut owner: ControlSession,
    driver: Driver,
    mut writer: LiveInitialWriter,
    previous: &InitialRegistration,
    backend: &Backend,
) {
    assert!(probe.initial_proxy_port.is_some());
    // Preparation12 and activation13 are complete. The first dictionary
    // reservation is COMMIT14; no refresh or native dispatch precedes it.
    let before = raw_state(&probe.path);
    assert_eq!((before.0.len(), before.1), (52, 62));
    let intent = first_dictionary(&writer, previous);
    let request = ProducerRequestId {
        producer_slot_id: writer.producer_slot_id(),
        producer_epoch: ProducerEpoch::new(2).unwrap(),
        request_sequence: RequestSequence::new(1).unwrap(),
    };
    let preimage = intent.encode_request_preimage(request, 8192).unwrap();
    let limits = AdmissionLimits {
        max_outstanding_requests: u64::from(writer.outstanding_request_limit()),
        max_dependencies: 256,
        ..AdmissionLimits::default()
    };
    emit(
        serde_json::json!({"event":"live-reservation-start", "request":1,
        "preimage":hex(&preimage), "request_digest":intent.request_digest(request),
        "commit_number":14}),
    );
    let result = append_first_dictionary(probe, &mut owner, &mut writer, &intent, limits).await;
    let reservation_unknown = matches!(
        &result,
        Err(InitialEventError::Reservation(
            bigbrotr::storage::postgres::producer_requests::ReservationError::CommitUnknown
        ))
    );
    emit(
        serde_json::json!({"event":"live-reservation-result", "request":1,
        "error":result.as_ref().err().map(|error|format!("{error:?}")),
        "native_dispatch":reservation_unknown.then_some(false)}),
    );
    probe.snapshot("reservation-unknown").await;
    assert!(reservation_unknown);
    drop(result);
    assert_eq!(
        raw_state(&probe.path),
        before,
        "no native dispatch after lost reservation reply"
    );
    let inputs = probe.path.join("producer-inputs-v1");
    assert_eq!(std::fs::read_dir(&inputs).unwrap().count(), 5);
    assert_eq!(
        std::fs::read(inputs.join(format!(
            "{}-0000000000000002-0000000000000001.cbor",
            request.producer_slot_id
        )))
        .unwrap(),
        preimage
    );
    probe.check_poisoned(&mut owner).await;
    drop(writer);
    probe.end_control("unknown", owner, driver, false).await;
    probe.require_backend_absent(backend).await;
    probe.snapshot("final").await;
    emit(
        serde_json::json!({"event":"live-complete", "mode":"successor-live-reservation-after",
        "native":"closed", "control":"dropped-and-joined", "writer":false, "admission":false}),
    );
}

async fn admit(
    probe: &Probe,
    owner: &mut ControlSession,
    writer: &mut LiveInitialWriter,
    previous: &InitialRegistration,
    provenance: bool,
) {
    let scope = public_scope(previous);
    let intent = first_dictionary(writer, previous);
    let lineage = intent.dictionaries()[0].reference();
    let corpus = include_bytes!("../../../../experiments/identity-contract/signed-fixture.json");
    let fixture: serde_json::Value = serde_json::from_slice(corpus).unwrap();
    let public: SignedEvent =
        serde_json::from_value(fixture["events"]["E_public"].clone()).unwrap();
    let alternate: SignedEvent =
        serde_json::from_value(fixture["events"]["E_private"].clone()).unwrap();
    assert_eq!(public.id, alternate.id);
    assert_ne!(public.sig, alternate.sig);
    let limits = AdmissionLimits {
        max_outstanding_requests: u64::from(previous.outstanding_request_limit),
        max_dependencies: 256,
        ..AdmissionLimits::default()
    };
    let receipt = append_first_dictionary(probe, owner, writer, &intent, limits)
        .await
        .unwrap();
    let batch = receipt.receipt().batch().unwrap();
    assert_eq!(batch.first().owner_epoch.get(), 2);
    assert_eq!(batch.first().sequence.get(), 1);
    assert_eq!(batch.records().len(), 1);
    emit(
        serde_json::json!({"event":"live-receipt","stage":"dictionary",
        "request":1,"receipt":hex(&receipt.receipt().encode_persistent(65536).unwrap())}),
    );
    drop(receipt);
    probe.snapshot("dictionary").await;
    let mut original_receipt = None;
    for (stage, sequence, event, recorded_at) in [
        ("new-event", 2, &public, 900_000_002),
        ("same-request", 2, &public, 900_000_003),
        ("same-id-alternate-signature", 3, &alternate, 900_000_004),
    ] {
        let prepared = PreparedEvent::new(
            ProducerRequestId {
                producer_slot_id: writer.producer_slot_id(),
                producer_epoch: ProducerEpoch::new(2).unwrap(),
                request_sequence: RequestSequence::new(sequence).unwrap(),
            },
            EventIntent::import(1, scope, event.clone(), lineage).unwrap(),
            256 * 1024,
            grant(&probe.budget, MIB, 0),
        )
        .unwrap();
        let outcome = bounded(owner.admit_live_initial_event(
            writer,
            LiveInitialEventRequest {
                ingest_root: &probe.path,
                event: prepared,
                relay_source: None,
                acquisition: None,
                provenance_enabled: provenance,
                recorded_at: ObservedTime::new(recorded_at),
                limits,
                policy_ttl: probe.policy_ttl(),
                cancelled: &probe.cancelled,
            },
            grant(
                &probe.budget,
                limits.required_data_bytes().unwrap(),
                limits.required_native_bytes().unwrap(),
            ),
            |doc, digest, candidate| {
                probe.validate(doc, digest)?;
                // A narrow laboratory import grant for the exact frozen event.
                // No acquisition job, online socket or installed import is claimed.
                if candidate.verification_limit_bytes() != 256 * 1024
                    || candidate.visibility_scope_id() != scope
                    || candidate.origin() != bigbrotr_model::EventOrigin::Import(lineage)
                    || candidate.observation().is_some()
                    || candidate
                        .verified_event()
                        .is_none_or(|v| v.event() != event)
                {
                    return Err(OwnerError::InvalidPolicy);
                }
                Ok(())
            },
        ))
        .await
        .unwrap();
        let AdmissionOutcome::Durable(receipt) = outcome.outcome() else {
            panic!("frozen signed event must be durable");
        };
        let bytes = receipt.encode_persistent(65536).unwrap();
        if sequence == 2 {
            assert_eq!(receipt.batch().unwrap().first().sequence.get(), 2);
            assert_eq!(receipt.batch().unwrap().records().len(), 3);
            if let Some(original) = &original_receipt {
                assert_eq!(original, &bytes);
            } else {
                original_receipt = Some(bytes.clone());
            }
        } else {
            assert!(receipt.batch().is_none());
        }
        emit(serde_json::json!({"event":"live-receipt","stage":stage,
            "request":sequence,"receipt":hex(&bytes),"event_id":event.id.to_string(),
            "supplied_signature":event.sig.to_string(),"provenance":provenance}));
        drop(outcome);
        probe.snapshot(stage).await;
    }
}
