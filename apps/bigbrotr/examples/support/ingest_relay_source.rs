//! Finite real enrollment/admission consumer. No socket or acquisition grant is
//! claimed: the signed bytes and trusted source attribution are fixed fixtures.
use super::*;
use bigbrotr::{
    acquisition::relay_source::RelaySourceBinding,
    management::{
        credentials::HASH_RESERVATION, grants::Revision, relay_enrollment::EnrollmentCommand,
        sessions::SessionLifetime,
    },
    network::RouteSelection,
    storage::{
        postgres::{
            control_owner::{
                EnrollmentRoutes, InitialEventError, InitialEventRequest, InitialVisibilityRequest,
            },
            producer_sources::SourceBindingError,
        },
        rocks::store::{AdmissionLimits, CertificationLimits, PreparedEvent},
    },
};
use bigbrotr_model::{
    ClockQuality, DurationUs, EventIntent, LocalObservation, ProducerEpoch, ProducerRequestId,
    RequestSequence, SignedEvent,
};
use bigbrotr_protocol::endpoint::Endpoint;

#[path = "ingest_scope_binding.rs"]
mod scope_binding;

#[allow(
    clippy::too_many_arguments,
    reason = "finite fixture retains actual owner and native handles"
)]
pub async fn run(
    config: Config,
    mut owner: ControlSession,
    driver: tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>,
    observer: &tokio_postgres::Client,
    mut store: IngestStore,
    spec: &InitialRegistration,
    path: &Path,
    budget: &ResourceBudget,
    mode: &str,
    scope: QualifiedRef,
) {
    if mode.starts_with("source-transport") {
        crate::ingest_source_transport::run(
            owner, driver, observer, store, spec, path, budget, mode, scope,
        )
        .await;
        return;
    }
    let validate = |document: &str, digest| validate_version(document, digest, true);
    let revision = |n: u64| Revision::try_from(n.to_string()).unwrap();
    let hash_budget = ResourceBudget::new(
        ProcessEnvelope::new(
            HASH_RESERVATION,
            0,
            ResourceLimits::new(HASH_RESERVATION, 1, 0, 0),
            WaiterLimits::new(0, 0),
        )
        .unwrap(),
    );
    let verified = owner
        .begin_browser_login("alice", "127.0.0.1".parse().unwrap())
        .await
        .unwrap()
        .verify(
            b"Independent owner password 2026",
            &mut grant(&hash_budget, HASH_RESERVATION, 0),
        )
        .unwrap();
    let session = owner
        .finish_browser_login(verified, SessionLifetime::default())
        .await
        .unwrap();
    let endpoint = owner
        .register_relay_endpoint(
            &Endpoint::parse("wss://source.example/events").unwrap(),
            &Cell::new(false),
        )
        .await
        .unwrap();
    let mut command = EnrollmentCommand {
        endpoint_id: endpoint.identity,
        visibility: scope,
        route: RouteSelection::Public {},
        enabled: true,
        expected_generation: None,
        expected_config_revision: revision(1),
        expected_policy_revision: revision(1),
    };
    let routes = EnrollmentRoutes {
        public_enabled: true,
        private_ceiling: None,
    };
    let receipt = owner
        .accept_relay_enrollment(
            &session.session,
            &session.csrf,
            [1; 16].try_into().unwrap(),
            &command,
            routes,
            &Cell::new(false),
        )
        .await
        .unwrap();
    let source = RelaySourceBinding {
        endpoint_id: receipt.endpoint_id,
        access_scope_id: receipt.access_scope_id,
        enrollment_generation: receipt.generation.get().try_into().unwrap(),
        route_id: receipt.route_id,
    };
    let second = owner
        .register_relay_endpoint(
            &Endpoint::parse("wss://other.example/events").unwrap(),
            &Cell::new(false),
        )
        .await
        .unwrap();
    let other = owner
        .accept_relay_enrollment(
            &session.session,
            &session.csrf,
            [2; 16].try_into().unwrap(),
            &EnrollmentCommand {
                endpoint_id: second.identity,
                ..command.clone()
            },
            routes,
            &Cell::new(false),
        )
        .await
        .unwrap();
    let other = RelaySourceBinding {
        endpoint_id: other.endpoint_id,
        access_scope_id: other.access_scope_id,
        enrollment_generation: other.generation.get().try_into().unwrap(),
        route_id: other.route_id,
    };
    let cancelled = AtomicBool::new(false);
    let limits = AdmissionLimits {
        max_outstanding_requests: 2,
        ..AdmissionLimits::default()
    };
    let read_limits = CertificationLimits {
        max_fact_bytes: 1024,
        max_dependencies: 8,
    };
    let committed = owner
        .read_initial_visibility(
            &mut store,
            InitialVisibilityRequest {
                ingest_root: path,
                registration: spec,
                reference: scope,
                limits: read_limits,
                policy_ttl: Duration::from_secs(2),
                cancelled: &cancelled,
            },
            grant(budget, read_limits.required_data_bytes().unwrap(), 0),
            validate,
        )
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(committed.entry().descriptor(), DictionaryDescriptor::Visibility { class } if class.as_str()=="public")
    );
    drop(committed);
    if mode.starts_with("source-scope-") {
        let available = scope_binding::run(
            &mut owner, observer, &mut store, spec, path, budget, mode, source, &command, &session,
        )
        .await;
        if available {
            owner.release().await.unwrap();
        } else {
            drop(owner);
        }
        crate::ingest_closing::join(driver, false).await;
        return;
    }
    if mode.starts_with("source-fault-") {
        crate::ingest_source_faults::run(
            config, owner, driver, observer, store, spec, path, budget, mode, source, command,
        )
        .await;
        return;
    }
    if matches!(mode, "source-withdrawn" | "source-stale-generation") {
        command.enabled = false;
        command.expected_generation = Some(revision(1));
        owner
            .accept_relay_enrollment(
                &session.session,
                &session.csrf,
                [3; 16].try_into().unwrap(),
                &command,
                routes,
                &Cell::new(false),
            )
            .await
            .unwrap();
        if mode == "source-stale-generation" {
            command.enabled = true;
            command.expected_generation = Some(revision(2));
            owner
                .accept_relay_enrollment(
                    &session.session,
                    &session.csrf,
                    [4; 16].try_into().unwrap(),
                    &command,
                    routes,
                    &Cell::new(false),
                )
                .await
                .unwrap();
        }
    }
    let event: SignedEvent = serde_json::from_slice(include_bytes!(
        "../../../../tests/fixtures/ingest/import-event.jsonl"
    ))
    .unwrap();
    let observer_ref = QualifiedRef {
        dictionary_kind: DictionaryKind::Observer,
        allocation_domain: [65; 16].try_into().unwrap(),
        ordinal: Ordinal::new(1).unwrap(),
        ..scope
    };
    let observation = LocalObservation {
        received_at: ObservedTime::new(456),
        observer_id: observer_ref,
        clock_quality: ClockQuality::new(observer_ref, DurationUs::new(1), None, 0).unwrap(),
        witness: None,
    };
    let request = ProducerRequestId {
        producer_slot_id: spec.producer_slot_id,
        producer_epoch: ProducerEpoch::new(1).unwrap(),
        request_sequence: RequestSequence::new(2).unwrap(),
    };
    let input = |binding, visibility| InitialEventRequest {
        ingest_root: path,
        registration: spec,
        event: PreparedEvent::new(
            request,
            EventIntent::network(1, visibility, event.clone(), observation).unwrap(),
            256 * 1024,
            grant(budget, MIB, 0),
        )
        .unwrap(),
        relay_source: binding,
        acquisition: None,
        provenance_enabled: false,
        recorded_at: ObservedTime::new(1234),
        limits,
        policy_ttl: Duration::from_secs(2),
        cancelled: &cancelled,
    };
    let authorize = |document: &str, digest, candidate: &PreparedEvent| {
        validate(document, digest)?;
        // Independent fixture policy accepts only these exact signed bytes and
        // observer, not arbitrary caller attribution or production network input.
        if candidate
            .verified_event()
            .is_none_or(|e| e.event() != &event)
            || candidate.observation() != Some(&observation)
        {
            return Err(OwnerError::InvalidPolicy);
        }
        Ok(())
    };
    let replay_case = matches!(
        mode,
        "source-pass" | "source-omitted" | "source-changed" | "source-legacy-adopt"
    );
    let mut original_receipt = None;
    if replay_case {
        let binding = if mode == "source-legacy-adopt" {
            None
        } else {
            Some(source)
        };
        let result = owner
            .admit_initial_event(
                &mut store,
                input(binding, scope),
                grant(
                    budget,
                    limits.required_data_bytes().unwrap(),
                    limits.required_native_bytes().unwrap(),
                ),
                authorize,
            )
            .await
            .unwrap();
        let bigbrotr::storage::rocks::store::AdmissionOutcome::Durable(receipt) = result.outcome()
        else {
            panic!("signed fixture rejected");
        };
        original_receipt = Some(receipt.encode_persistent(65536).unwrap());
        drop(result);
    }
    let before = ingest_snapshot::state(observer, path).await;
    emit(serde_json::json!({"event":"source-before", "mode":mode, "state":before}));
    let selected = match mode {
        "source-omitted" => None,
        "source-changed" => Some(other),
        _ => Some(source),
    };
    let visibility = if mode == "source-visibility" {
        QualifiedRef {
            ordinal: Ordinal::new(2).unwrap(),
            ..scope
        }
    } else {
        scope
    };
    let result = owner
        .admit_initial_event(
            &mut store,
            input(selected, visibility),
            grant(
                budget,
                limits.required_data_bytes().unwrap(),
                limits.required_native_bytes().unwrap(),
            ),
            authorize,
        )
        .await;
    if mode == "source-pass" {
        let result = result.unwrap();
        let bigbrotr::storage::rocks::store::AdmissionOutcome::Durable(receipt) = result.outcome()
        else {
            panic!("exact retry rejected");
        };
        assert_eq!(
            receipt.encode_persistent(65536).unwrap(),
            original_receipt.unwrap()
        );
    } else {
        let expected = match mode {
            "source-omitted" | "source-changed" | "source-legacy-adopt" | "source-visibility" => {
                SourceBindingError::Conflict
            }
            "source-withdrawn" | "source-stale-generation" | "source-dispatch-refused" => {
                SourceBindingError::Unavailable
            }
            _ => panic!("unknown source mode"),
        };
        assert_eq!(result.err(), Some(InitialEventError::Source(expected)));
    }
    let after = ingest_snapshot::state(observer, path).await;
    if mode == "source-dispatch-refused" {
        for field in ["native_rows", "native_sha256", "sequence"] {
            assert_eq!(after[field], before[field]);
        }
        let counts = observer
            .query_one(
                "SELECT
            (SELECT count(*) FROM control.producer_requests WHERE request_sequence=2),
            (SELECT count(*) FROM control.producer_outcomes WHERE request_sequence=2)",
                &[],
            )
            .await
            .unwrap();
        assert_eq!((counts.get::<_, i64>(0), counts.get::<_, i64>(1)), (1, 0));
    } else {
        assert_eq!(after, before);
    }
    emit(serde_json::json!({"event":"source-after", "mode":mode, "state":after, "status":"PASS"}));
    if mode == "source-pass" {
        owner.release().await.unwrap();
    } else {
        drop(owner);
    }
    crate::ingest_closing::join(driver, false).await;
}
