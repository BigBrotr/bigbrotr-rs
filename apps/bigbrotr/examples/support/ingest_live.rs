//! Finite connected activation from the signed retired seed. This is not an
//! installed import executor or recovery of a departed activated incarnation.
use super::*;
use bigbrotr::storage::{
    postgres::control_owner::{
        InitialHistoryRequest, LiveInitialDictionaryAppendRequest, LiveInitialEventRequest,
        LiveInitialWriter,
    },
    rocks::store::{AdmissionOutcome, PreparedEvent},
};
use bigbrotr_model::{DatasetLineageDescriptor, EventIntent, SignedEvent};
use sha2::{Digest, Sha256};

pub(super) fn is_mode(mode: &str) -> bool {
    matches!(
        mode,
        "successor-live-zero"
            | "successor-live-guards"
            | "successor-live-normal"
            | "successor-live-off"
            | "successor-live-commit-before"
            | "successor-live-commit-after"
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
        .plan(&mut owner, candidate.recovery(), &spec, Some(&candidate))
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
    if let Some(prefix) = gate_prefix {
        assert_eq!(mode, "successor-live-guards");
        guards(&probe, &mut owner, prefix).await;
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
                probe.initial_proxy_port.is_none(),
                "fault must prevent writer delivery"
            );
            emit(
                serde_json::json!({"event":"live-activation-result","writer":true,
                "activated_at_us":writer.activated_at_us(),"error":null}),
            );
            writer
        }
        Err(error) => {
            assert!(probe.initial_proxy_port.is_some());
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
    if mode != "successor-live-zero" {
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

async fn guards(probe: &Probe, owner: &mut ControlSession, prefix: &Path) {
    guard_gate(prefix, "install", "go").await;
    bounded(owner.refresh(|doc, digest| probe.validate(doc, digest)))
        .await
        .unwrap();
    let identity = owner.identity();
    let observed = bounded(probe.observer.query_one(
        "SELECT count(*)::bigint, coalesce(bool_and(g.status='PASS'
            AND g.backend_pid=c.control_backend_pid AND g.backend_start=c.control_backend_start
            AND g.database_name=current_database() AND g.database_user='bigbrotr_control'
            AND g.replication_role='origin' AND g.control_epoch=c.epoch),false)
         FROM live_guard_fixture.cases g CROSS JOIN control.owner_epochs c
         WHERE c.resource_id=$1::bytea AND c.epoch=$2::text::numeric",
        &[
            &&identity.resource_id.as_bytes()[..],
            &identity.epoch.get().to_string(),
        ],
    ))
    .await
    .unwrap();
    assert_eq!(observed.get::<_, i64>(0), 14);
    assert!(observed.get::<_, bool>(1));
    let run = bounded(probe.observer.query_one(
        "SELECT status,executions FROM live_guard_fixture.run WHERE singleton",
        &[],
    ))
    .await
    .unwrap();
    assert_eq!(run.get::<_, &str>(0), "complete");
    assert_eq!(run.get::<_, i32>(1), 1);
    emit(serde_json::json!({"event":"live-guards-result","cases":14,"passed":true}));
    probe.snapshot("guards-refreshed").await;
    guard_gate(prefix, "remove", "done").await;
    let removed: bool = bounded(probe.observer.query_one(
        "SELECT NOT EXISTS(SELECT 1 FROM pg_catalog.pg_trigger
            WHERE tgname='live_guard_fixture_refresh' AND tgrelid='control.owner_epochs'::regclass)
            AND to_regprocedure('live_guard_fixture.exercise()') IS NULL",
        &[],
    ))
    .await
    .unwrap()
    .get(0);
    assert!(removed, "only the temporary fixture hook must be removed");
    probe.snapshot("guards-clean").await;
}

async fn admit(
    probe: &Probe,
    owner: &mut ControlSession,
    writer: &mut LiveInitialWriter,
    previous: &InitialRegistration,
    provenance: bool,
) {
    // These coordinates come from the frozen original installed fixture. They
    // are expected inputs, not inferred from the effects of this new operation.
    let scope = QualifiedRef {
        dataset_id: previous.dataset_id,
        dictionary_kind: DictionaryKind::VisibilityScope,
        allocation_domain: previous
            .allocation_domains
            .iter()
            .find(|(kind, _)| *kind == DictionaryKind::VisibilityScope)
            .unwrap()
            .1,
        ordinal: Ordinal::new(3).unwrap(),
    };
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
    let public: SignedEvent =
        serde_json::from_value(fixture["events"]["E_public"].clone()).unwrap();
    let alternate: SignedEvent =
        serde_json::from_value(fixture["events"]["E_private"].clone()).unwrap();
    assert_eq!(public.id, alternate.id);
    assert_ne!(public.sig, alternate.sig);
    let intent = EvidenceIntent::new(
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
    .unwrap();
    let limits = AdmissionLimits {
        max_outstanding_requests: u64::from(previous.outstanding_request_limit),
        max_dependencies: 256,
        ..AdmissionLimits::default()
    };
    let receipt = bounded(owner.append_live_initial_dictionaries(
        writer,
        LiveInitialDictionaryAppendRequest {
            ingest_root: &probe.path,
            request_sequence: RequestSequence::new(1).unwrap(),
            intent: &intent,
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
