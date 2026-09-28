//! Actual PG/Rocks consumer of registered collection intent. The laboratory owns
//! fixed source attribution; this is not installed socket or scheduling evidence.
use super::*;
use bigbrotr::{
    acquisition::scope::{AcquisitionEventContext, ScopeEventError},
    management::sessions::IssuedSession,
    storage::{
        postgres::control_owner::{InitialCertificationRequest, ScopeIntent},
        rocks::store::{AdmissionOutcome, CertificationOutcome, PreparedCertification},
    },
};
use bigbrotr_model::PolicyEpoch;
use bigbrotr_protocol::selector::decode_selector;

#[allow(
    clippy::too_many_arguments,
    reason = "finite fixture retains existing engine/owner handles"
)]
pub async fn run(
    owner: &mut ControlSession,
    observer: &tokio_postgres::Client,
    store: &mut IngestStore,
    spec: &InitialRegistration,
    path: &Path,
    budget: &ResourceBudget,
    mode: &str,
    source: RelaySourceBinding,
    enrollment: &EnrollmentCommand,
    session: &IssuedSession,
    policy_root: Option<&crate::config::Resolved>,
) -> bool {
    assert!(matches!(
        mode,
        "source-scope-pass"
            | "source-scope-changed"
            | "source-scope-omitted"
            | "source-scope-legacy-adopt"
            | "source-scope-outside-collection"
            | "source-scope-outside-request"
            | "source-scope-no-source"
            | "source-scope-certify-withdrawn"
            | "source-scope-old-source-schema"
            | "source-scope-old-binding-schema"
            | "source-scope-invalid"
            | "source-scope-invalid-withdrawn"
            | "source-scope-invalid-retained"
            | "source-scope-dispatch-refused"
            | "source-scope-policy-provenance"
            | "source-scope-policy-limit"
            | "source-scope-policy-floor"
            | "source-scope-policy-ephemeral"
            | "source-scope-policy-second"
    ));
    let visibility = enrollment.visibility;
    let mut event: SignedEvent = serde_json::from_slice(include_bytes!(
        "../../../../tests/fixtures/ingest/import-event.jsonl"
    ))
    .unwrap();
    let invalid = matches!(
        mode,
        "source-scope-invalid" | "source-scope-invalid-withdrawn"
    );
    if invalid {
        event.sig = bigbrotr_model::Signature::from_bytes([0; 64]);
    }
    let observer_ref = QualifiedRef {
        dictionary_kind: DictionaryKind::Observer,
        allocation_domain: [65; 16].try_into().unwrap(),
        ordinal: Ordinal::new(1).unwrap(),
        ..visibility
    };
    let observation = LocalObservation {
        received_at: ObservedTime::new(456),
        observer_id: observer_ref,
        clock_quality: ClockQuality::new(observer_ref, DurationUs::new(1), None, 0).unwrap(),
        witness: None,
    };
    let intent = || EventIntent::network(1, visibility, event.clone(), observation).unwrap();
    let cancelled = AtomicBool::new(false);
    if mode == "source-scope-old-source-schema" {
        let before = ingest_snapshot::state(observer, path).await;
        let error = certify(
            owner,
            store,
            spec,
            path,
            budget,
            &cancelled,
            source,
            None,
            intent(),
            policy_root,
        )
        .await
        .err();
        assert_eq!(
            error,
            Some(InitialEventError::Source(SourceBindingError::Prerequisite))
        );
        assert_eq!(ingest_snapshot::state(observer, path).await, before);
        emit(
            serde_json::json!({"event":"scope-finished", "mode":mode, "status":"PASS", "zero_writes":true}),
        );
        return false;
    }
    let original = if mode == "source-scope-outside-collection" {
        "[2]"
    } else {
        "[1]"
    };
    let mut stored = Vec::new();
    for kinds in [original, "[1,2]"] {
        let document = format!(
            r#"{{"version":1,"filters":[{{"kinds":{kinds}}}],"history_since":0,"ephemeral":"retain"}}"#
        );
        let selector = decode_selector(document.as_bytes()).unwrap();
        let registered = owner
            .register_acquisition_scope(
                ScopeIntent {
                    selector: &selector,
                    endpoint: source.endpoint_id,
                    access: source.access_scope_id,
                    visibility,
                    policy_epoch: PolicyEpoch::new(1),
                },
                &Cell::new(false),
            )
            .await
            .unwrap();
        stored.push(
            owner
                .read_acquisition_scope(registered.identity, grant(budget, 8 * MIB, 0))
                .await
                .unwrap()
                .unwrap(),
        );
    }
    assert_ne!(stored[0].identity(), stored[1].identity());
    emit(
        serde_json::json!({"event":"scope-identities", "original":stored[0].identity(),
        "overlapping":stored[1].identity()}),
    );
    let predicate = if mode == "source-scope-outside-request" {
        "[2]"
    } else {
        "[1,2]"
    };
    let request_document = format!(
        r#"{{"version":1,"filters":[{{"kinds":{predicate}}}],"history_since":0,"ephemeral":"retain"}}"#
    );
    let request_predicate = decode_selector(request_document.as_bytes()).unwrap();
    if mode == "source-scope-old-binding-schema" {
        let before = ingest_snapshot::state(observer, path).await;
        let error = certify(
            owner,
            store,
            spec,
            path,
            budget,
            &cancelled,
            source,
            Some(AcquisitionEventContext::new(&stored[0], request_predicate.filters()).unwrap()),
            intent(),
            policy_root,
        )
        .await
        .err();
        assert_eq!(
            error,
            Some(InitialEventError::Source(SourceBindingError::Prerequisite))
        );
        assert_eq!(ingest_snapshot::state(observer, path).await, before);
        emit(
            serde_json::json!({"event":"scope-finished", "mode":mode, "status":"PASS", "zero_writes":true}),
        );
        return false;
    }
    let request = ProducerRequestId {
        producer_slot_id: spec.producer_slot_id,
        producer_epoch: ProducerEpoch::new(1).unwrap(),
        request_sequence: RequestSequence::new(2).unwrap(),
    };
    let limits = AdmissionLimits {
        max_outstanding_requests: 2,
        ..AdmissionLimits::default()
    };
    let input = |scope: Option<usize>, binding| InitialEventRequest {
        ingest_root: path,
        registration: spec,
        event: PreparedEvent::new(request, intent(), 256 * 1024, grant(budget, MIB, 0)).unwrap(),
        relay_source: binding,
        acquisition: scope.map(|index| {
            AcquisitionEventContext::new(&stored[index], request_predicate.filters()).unwrap()
        }),
        provenance_enabled: false,
        recorded_at: ObservedTime::new(1234),
        limits,
        policy_ttl: Duration::from_secs(2),
        cancelled: &cancelled,
    };
    let authorize = |document: &str, digest, candidate: &PreparedEvent| {
        if let Some(root) = policy_root {
            let endpoint = Endpoint::parse("wss://source.example/events").unwrap();
            let context =
                AcquisitionEventContext::new(&stored[0], request_predicate.filters()).unwrap();
            let result = crate::config::acquisition::SyncArrivalPolicy {
                root,
                endpoint: &endpoint,
                route: RouteSelection::Public {},
                selector: context.scope().selector(),
                visibility: context.scope().scope().visibility(),
                observation,
                provenance_enabled: false,
            }
            .admission(document, digest, candidate);
            emit(
                serde_json::json!({"event":"arrival-policy", "operation":"admission",
                "digest":hex(&digest), "accepted":result.is_ok()}),
            );
            return result;
        }
        validate_version(document, digest, true)?;
        let expected = if invalid { None } else { Some(&event) };
        if candidate.verified_event().map(|v| v.event()) != expected
            || candidate.verification_limit_bytes() != 256 * 1024
            || candidate.visibility_scope_id() != visibility
            || candidate.origin() != bigbrotr_model::EventOrigin::Network
            || candidate.observation() != Some(&observation)
        {
            return Err(OwnerError::InvalidPolicy);
        }
        Ok(())
    };
    let admission_permit = || {
        grant(
            budget,
            limits.required_data_bytes().unwrap(),
            limits.required_native_bytes().unwrap(),
        )
    };
    let replay = matches!(
        mode,
        "source-scope-pass"
            | "source-scope-changed"
            | "source-scope-omitted"
            | "source-scope-legacy-adopt"
            | "source-scope-certify-withdrawn"
            | "source-scope-invalid-retained"
    );
    let mut receipt_bytes = None;
    if replay {
        if mode == "source-scope-pass" {
            let before = ingest_snapshot::state(observer, path).await;
            let result = certify(
                owner,
                store,
                spec,
                path,
                budget,
                &cancelled,
                source,
                Some(
                    AcquisitionEventContext::new(&stored[0], request_predicate.filters()).unwrap(),
                ),
                intent(),
                policy_root,
            )
            .await
            .unwrap();
            assert!(matches!(
                result.outcome(),
                CertificationOutcome::NeedsAdmission
            ));
            drop(result);
            assert_eq!(ingest_snapshot::state(observer, path).await, before);
            emit(serde_json::json!({"event":"scope-needs-admission", "zero_writes":true}));
        }
        let selected = if mode == "source-scope-legacy-adopt" {
            None
        } else {
            Some(0)
        };
        let result = owner
            .admit_initial_event(
                store,
                input(selected, Some(source)),
                admission_permit(),
                authorize,
            )
            .await
            .unwrap();
        let AdmissionOutcome::Durable(receipt) = result.outcome() else {
            panic!("signed fixture rejected");
        };
        receipt_bytes = Some(receipt.encode_persistent(65536).unwrap());
    }
    if matches!(
        mode,
        "source-scope-certify-withdrawn" | "source-scope-invalid-withdrawn"
    ) {
        owner
            .accept_relay_enrollment(
                &session.session,
                &session.csrf,
                [3; 16].try_into().unwrap(),
                &EnrollmentCommand {
                    enabled: false,
                    expected_generation: Some(Revision::try_from("1".to_owned()).unwrap()),
                    ..enrollment.clone()
                },
                EnrollmentRoutes {
                    public_enabled: true,
                    private_ceiling: None,
                },
                &Cell::new(false),
            )
            .await
            .unwrap();
    }
    if mode == "source-scope-certify-withdrawn" {
        let before = ingest_snapshot::state(observer, path).await;
        let error = certify(
            owner,
            store,
            spec,
            path,
            budget,
            &cancelled,
            source,
            Some(AcquisitionEventContext::new(&stored[0], request_predicate.filters()).unwrap()),
            intent(),
            policy_root,
        )
        .await
        .err();
        assert_eq!(
            error,
            Some(InitialEventError::Source(SourceBindingError::Unavailable))
        );
        assert_eq!(ingest_snapshot::state(observer, path).await, before);
        emit(
            serde_json::json!({"event":"scope-finished", "mode":mode, "status":"PASS", "zero_writes":true}),
        );
        return false;
    }
    if mode == "source-scope-invalid-retained" {
        let before = ingest_snapshot::state(observer, path).await;
        let mut forged = event.clone();
        forged.sig = bigbrotr_model::Signature::from_bytes([0; 64]);
        let result = certify(
            owner,
            store,
            spec,
            path,
            budget,
            &cancelled,
            source,
            Some(AcquisitionEventContext::new(&stored[0], request_predicate.filters()).unwrap()),
            EventIntent::network(1, visibility, forged, observation).unwrap(),
            policy_root,
        )
        .await
        .unwrap();
        assert!(matches!(
            result.outcome(),
            CertificationOutcome::Rejected(
                bigbrotr::storage::rocks::store::RejectionReason::Signature
            )
        ));
        drop(result);
        assert_eq!(ingest_snapshot::state(observer, path).await, before);
        emit(
            serde_json::json!({"event":"scope-finished", "mode":mode, "status":"PASS", "zero_writes":true}),
        );
        return true;
    }
    let before = ingest_snapshot::state(observer, path).await;
    emit(serde_json::json!({"event":"scope-before", "state":before}));
    let selected = match mode {
        "source-scope-omitted" => None,
        "source-scope-changed" => Some(1),
        _ => Some(0),
    };
    let binding = if mode == "source-scope-no-source" {
        None
    } else {
        Some(source)
    };
    let result = owner
        .admit_initial_event(
            store,
            input(selected, binding),
            admission_permit(),
            authorize,
        )
        .await;
    if mode == "source-scope-invalid" {
        let result = result.unwrap();
        assert!(matches!(
            result.outcome(),
            AdmissionOutcome::Rejected(bigbrotr::storage::rocks::store::RejectionReason::Signature)
        ));
        drop(result);
        let retained = ingest_snapshot::state(observer, path).await;
        // Rejection adds an operational record and advances the Rocks sequence.
        // The independent complete-byte oracle checks unchanged scientific facts.
        assert_eq!(
            retained["native_rows"].as_u64().unwrap(),
            before["native_rows"].as_u64().unwrap() + 1
        );
        let retry = owner
            .admit_initial_event(
                store,
                input(Some(0), Some(source)),
                admission_permit(),
                authorize,
            )
            .await
            .unwrap();
        assert!(matches!(
            retry.outcome(),
            AdmissionOutcome::Rejected(bigbrotr::storage::rocks::store::RejectionReason::Signature)
        ));
        drop(retry);
        assert_eq!(ingest_snapshot::state(observer, path).await, retained);
        emit(
            serde_json::json!({"event":"scope-finished", "mode":mode, "state":retained, "status":"PASS", "zero_writes":false, "retry_zero_writes":true}),
        );
        return true;
    } else if mode == "source-scope-pass" {
        let result = result.unwrap();
        let AdmissionOutcome::Durable(receipt) = result.outcome() else {
            panic!("retry rejected");
        };
        assert_eq!(
            receipt.encode_persistent(65536).unwrap(),
            receipt_bytes.unwrap()
        );
        drop(result);
        let proof = certify(
            owner,
            store,
            spec,
            path,
            budget,
            &cancelled,
            source,
            Some(AcquisitionEventContext::new(&stored[0], request_predicate.filters()).unwrap()),
            intent(),
            policy_root,
        )
        .await
        .unwrap();
        assert!(matches!(proof.outcome(), CertificationOutcome::Existing(_)));
    } else {
        let expected = match mode {
            "source-scope-changed" | "source-scope-omitted" | "source-scope-legacy-adopt" => {
                InitialEventError::Source(SourceBindingError::Conflict)
            }
            "source-scope-outside-collection" => {
                InitialEventError::Scope(ScopeEventError::OutsideCollection)
            }
            "source-scope-outside-request" => {
                InitialEventError::Scope(ScopeEventError::OutsideRequest)
            }
            "source-scope-no-source" => InitialEventError::Control(OwnerError::InvalidInput),
            "source-scope-invalid-withdrawn" | "source-scope-dispatch-refused" => {
                InitialEventError::Source(SourceBindingError::Unavailable)
            }
            name if name.starts_with("source-scope-policy-") => {
                InitialEventError::Control(OwnerError::InvalidPolicy)
            }
            _ => unreachable!(),
        };
        assert_eq!(result.err(), Some(expected));
    }
    let after = ingest_snapshot::state(observer, path).await;
    if mode == "source-scope-dispatch-refused" {
        for field in ["native_rows", "native_sha256", "sequence"] {
            assert_eq!(after[field], before[field]);
        }
        let counts = observer
            .query_one(
                "SELECT (SELECT count(*) FROM control.producer_requests WHERE request_sequence=2),
            (SELECT count(*) FROM control.producer_outcomes WHERE request_sequence=2)",
                &[],
            )
            .await
            .unwrap();
        assert_eq!((counts.get::<_, i64>(0), counts.get::<_, i64>(1)), (1, 0));
    } else {
        assert_eq!(after, before);
    }
    emit(
        serde_json::json!({"event":"scope-finished", "mode":mode, "state":after, "status":"PASS", "zero_writes":mode != "source-scope-dispatch-refused"}),
    );
    mode == "source-scope-pass"
}

#[allow(
    clippy::too_many_arguments,
    reason = "actual read uses the existing fixture owner and source context"
)]
async fn certify(
    owner: &mut ControlSession,
    store: &mut IngestStore,
    spec: &InitialRegistration,
    path: &Path,
    budget: &ResourceBudget,
    cancelled: &AtomicBool,
    source: RelaySourceBinding,
    acquisition: Option<AcquisitionEventContext<'_>>,
    intent: EventIntent,
    policy_root: Option<&crate::config::Resolved>,
) -> Result<bigbrotr::storage::rocks::store::StoredCertification, InitialEventError> {
    let limits = CertificationLimits {
        max_fact_bytes: 1024,
        max_dependencies: 8,
    };
    let expected_event = intent.event().clone();
    // This literal invalid fixture preserves the real event ID/body while
    // replacing only its signature. No verifier output defines the expectation.
    let invalid = expected_event.sig.as_bytes() == &[0; 64];
    let expected_observation = intent.observation().copied();
    owner
        .certify_initial_event(
            store,
            InitialCertificationRequest {
                ingest_root: path,
                registration: spec,
                event: PreparedCertification::new(intent, 256 * 1024, grant(budget, MIB, 0))
                    .unwrap(),
                relay_source: Some(source),
                acquisition,
                provenance_enabled: false,
                limits,
                policy_ttl: Duration::from_secs(2),
                cancelled,
            },
            grant(budget, limits.required_data_bytes().unwrap(), 0),
            |document, digest, candidate| {
                if let Some(root) = policy_root {
                    let context = acquisition.expect("current Sync policy needs exact scope");
                    let endpoint = Endpoint::parse("wss://source.example/events").unwrap();
                    let result = crate::config::acquisition::SyncArrivalPolicy {
                        root,
                        endpoint: &endpoint,
                        route: RouteSelection::Public {},
                        selector: context.scope().selector(),
                        visibility: context.scope().scope().visibility(),
                        observation: expected_observation.unwrap(),
                        provenance_enabled: false,
                    }
                    .certification(document, digest, candidate);
                    emit(
                        serde_json::json!({"event":"arrival-policy", "operation":"certification",
                        "digest":hex(&digest), "accepted":result.is_ok()}),
                    );
                    return result;
                }
                validate_version(document, digest, true)?;
                let expected = if invalid { None } else { Some(&expected_event) };
                if candidate.verification_limit_bytes() != 256 * 1024
                    || candidate.verified_event().map(|event| event.event()) != expected
                    || candidate.observation() != expected_observation.as_ref()
                {
                    return Err(OwnerError::InvalidPolicy);
                }
                Ok(())
            },
        )
        .await
}
