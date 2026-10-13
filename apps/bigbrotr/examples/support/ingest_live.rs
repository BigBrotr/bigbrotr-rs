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
