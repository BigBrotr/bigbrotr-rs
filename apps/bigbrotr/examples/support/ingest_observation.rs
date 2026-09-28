//! Fixed actual Control/native observation reader. No socket or admission claim.
use super::*;
use bigbrotr::{
    acquisition::{observation::ObservationBindings, receive_clock::ReceiveClock},
    network::RouteSelection,
    storage::{
        postgres::control_owner::{InitialEventError, InitialObservationRequest},
        rocks::store::{AdmissionError, CertificationLimits},
    },
};
use bigbrotr_model::{AccessClass, AccessScopeDescriptor, EndpointDescriptor, WitnessContext};
use bigbrotr_protocol::endpoint::Endpoint;
use std::sync::atomic::Ordering;

fn reference(scope: QualifiedRef, kind: DictionaryKind, domain: u8) -> QualifiedRef {
    QualifiedRef {
        dictionary_kind: kind,
        allocation_domain: [domain; 16].try_into().unwrap(),
        ordinal: Ordinal::new(1).unwrap(),
        ..scope
    }
}
pub fn dictionaries(
    entries: &mut Vec<DictionaryEntry>,
    scope: QualifiedRef,
    witness: bool,
    private: bool,
) {
    if private {
        for (index, class) in ["private", "public"].into_iter().enumerate() {
            entries[index] = DictionaryEntry::new(
                entries[index].reference(),
                DictionaryDescriptor::Visibility {
                    class: DescriptorLabel::new(class.into()).unwrap(),
                },
            )
            .unwrap();
        }
    }
    if witness {
        for (kind, domain, descriptor) in [
            (
                DictionaryKind::Endpoint,
                70,
                DictionaryDescriptor::Endpoint(
                    EndpointDescriptor::new("wss://source.example/events".into()).unwrap(),
                ),
            ),
            (
                DictionaryKind::AccessScope,
                71,
                DictionaryDescriptor::AccessScope(AccessScopeDescriptor {
                    class: AccessClass::Public,
                    label: None,
                    visibility_scope_id: scope,
                }),
            ),
            (
                DictionaryKind::Method,
                72,
                DictionaryDescriptor::EventVerification,
            ),
        ] {
            entries.push(DictionaryEntry::new(reference(scope, kind, domain), descriptor).unwrap());
        }
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "finite probe borrows the one current owner and native store"
)]
pub async fn run(
    owner: &mut ControlSession,
    observer: &tokio_postgres::Client,
    store: &mut IngestStore,
    spec: &InitialRegistration,
    path: &Path,
    budget: &ResourceBudget,
    mode: &str,
    intent: &EvidenceIntent,
) {
    let scope = intent.visibility_scope_id();
    let who = &intent.dictionaries()[2];
    let witness = mode.ends_with("-on");
    let bindings = ObservationBindings {
        visibility: scope,
        observer: who,
        clock: who,
        witness: witness.then_some(WitnessContext {
            endpoint_id: reference(scope, DictionaryKind::Endpoint, 70),
            access_scope_id: reference(scope, DictionaryKind::AccessScope, 71),
            verification_method_id: reference(scope, DictionaryKind::Method, 72),
        }),
    };
    let limits = CertificationLimits {
        max_fact_bytes: 8192,
        max_dependencies: 16,
    };
    let bytes = limits.required_data_bytes().unwrap();
    let cancelled = AtomicBool::new(false);
    let request = |bindings, limits| InitialObservationRequest {
        ingest_root: path,
        registration: spec,
        bindings,
        limits,
        policy_ttl: Duration::from_secs(2),
        cancelled: &cancelled,
    };
    let validate = |d: &str, h| validate_version(d, h, true);
    let before = ingest_snapshot::state(observer, path).await;
    emit(serde_json::json!({"event":"observation-before", "state":before}));
    let baseline = budget.snapshot().used().data_bytes();
    if matches!(
        mode,
        "source-observation-off" | "source-observation-on" | "source-observation-private-on"
    ) {
        let small = CertificationLimits {
            max_fact_bytes: 32,
            ..limits
        };
        assert_eq!(
            owner
                .read_initial_observation(
                    store,
                    request(bindings, small),
                    grant(budget, bytes, 0),
                    validate
                )
                .await
                .err(),
            Some(InitialEventError::Native(AdmissionError::Limit))
        );
        for _ in 0..2 {
            let retained = owner
                .read_initial_observation(
                    store,
                    request(bindings, limits),
                    grant(budget, bytes, 0),
                    validate,
                )
                .await
                .unwrap()
                .unwrap();
            assert_eq!(budget.snapshot().used().data_bytes(), baseline + bytes);
            let mut clock = ReceiveClock::new().unwrap();
            let sample = clock.sample().unwrap();
            let endpoint = Endpoint::parse("wss://source.example/events").unwrap();
            let observation = retained
                .received(&endpoint, RouteSelection::Public {}, sample)
                .unwrap();
            assert_eq!(observation.observer_id, who.reference());
            assert_eq!(observation.witness, bindings.witness);
            assert_eq!(observation.received_at, sample.received_at());
            assert_eq!(observation.clock_quality.uncertainty_us(), None);
            emit(
                serde_json::json!({"event":"observation-read", "observer":observation.observer_id,
                "received_at":observation.received_at, "clock":observation.clock_quality,
                "visibility":retained.visibility(), "witness":witness}),
            );
            drop(retained);
            assert_eq!(budget.snapshot().used().data_bytes(), baseline);
        }
        let mut absent = bindings;
        absent.witness = Some(WitnessContext {
            endpoint_id: QualifiedRef {
                ordinal: Ordinal::new(2).unwrap(),
                ..reference(scope, DictionaryKind::Endpoint, 70)
            },
            access_scope_id: reference(scope, DictionaryKind::AccessScope, 71),
            verification_method_id: reference(scope, DictionaryKind::Method, 72),
        });
        assert!(
            owner
                .read_initial_observation(
                    store,
                    request(absent, limits),
                    grant(budget, bytes, 0),
                    validate
                )
                .await
                .unwrap()
                .is_none()
        );
    } else {
        let mut validations = 0;
        let error = owner
            .read_initial_observation(
                store,
                request(bindings, limits),
                grant(budget, bytes, 0),
                |document, digest| {
                    validate(document, digest)?;
                    validations += 1;
                    if validations == 2 {
                        match mode {
                            "source-observation-expired" => {
                                std::thread::sleep(Duration::from_secs(3))
                            }
                            "source-observation-cancelled" => {
                                cancelled.store(true, Ordering::Relaxed)
                            }
                            "source-observation-policy" => return Err(OwnerError::InvalidPolicy),
                            _ => panic!("unknown observation fixture"),
                        }
                    }
                    Ok(())
                },
            )
            .await
            .err()
            .unwrap();
        assert_eq!(validations, 2);
        assert_eq!(
            error,
            match mode {
                "source-observation-expired" => InitialEventError::Control(OwnerError::Unavailable),
                "source-observation-cancelled" => InitialEventError::DeliveryUnknown,
                "source-observation-policy" =>
                    InitialEventError::Control(OwnerError::InvalidPolicy),
                _ => unreachable!(),
            }
        );
        emit(serde_json::json!({"event":"observation-refused", "error":format!("{error:?}")}));
    }
    assert_eq!(budget.snapshot().used().data_bytes(), baseline);
    let after = ingest_snapshot::state(observer, path).await;
    assert_eq!(after, before);
    emit(
        serde_json::json!({"event":"observation-finished", "mode":mode, "status":"PASS", "state":after}),
    );
}
