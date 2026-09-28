//! Real finite jobs at the initial signed-event admission/certification boundary.
//! Complete expected native bytes are independently encoded by the Python lab.
use super::*;
use bigbrotr::{
    acquisition::{
        job::{AcquisitionCommand, JobDuration},
        scope::AcquisitionEventContext,
    },
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
    reason = "fixture retains actual engine and owner custody"
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
    root: &crate::config::Resolved,
) -> bool {
    let case = mode.strip_prefix("source-job-").unwrap();
    assert!(matches!(
        case,
        "pass"
            | "changed"
            | "omitted"
            | "unbound"
            | "invalid-unbound"
            | "certify-unbound"
            | "accepted"
            | "scope"
            | "expired"
            | "certify-expired"
            | "withdrawn"
            | "old-schema"
            | "dispatch"
    ));
    let mut scopes = Vec::new();
    for kinds in ["[1]", "[1,2]"] {
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
                    visibility: enrollment.visibility,
                    policy_epoch: PolicyEpoch::new(1),
                },
                &Cell::new(false),
            )
            .await
            .unwrap();
        scopes.push(
            owner
                .read_acquisition_scope(registered.identity, grant(budget, 8 * MIB, 0))
                .await
                .unwrap()
                .unwrap(),
        );
    }
    let mut jobs = Vec::new();
    for key in [77u8, 78] {
        let command = AcquisitionCommand {
            scope_id: scopes[0].identity(),
            deadline_us: JobDuration::new(if case.contains("expired") {
                5_000_000
            } else {
                120_000_000
            })
            .unwrap(),
            expected_config_revision: Revision::try_from("1".to_owned()).unwrap(),
            expected_policy_revision: Revision::try_from("1".to_owned()).unwrap(),
        };
        let receipt = owner
            .accept_acquisition(
                &session.session,
                &session.csrf,
                [key; 16].try_into().unwrap(),
                &command,
                &Cell::new(false),
            )
            .await
            .unwrap();
        assert!(receipt.created);
        if case != "accepted" {
            owner
                .claim_acquisition(receipt.job_id, &Cell::new(false))
                .await
                .unwrap();
        }
        jobs.push(receipt.job_id);
    }
    emit(
        serde_json::json!({"event":"execution-identities", "jobs":jobs,
        "scopes":[scopes[0].identity(),scopes[1].identity()]}),
    );
    let context = |job, scope: usize| {
        AcquisitionEventContext::for_job(job, &scopes[scope], scopes[0].selector().filters())
            .unwrap()
    };
    let visibility = enrollment.visibility;
    let mut event: SignedEvent = serde_json::from_slice(include_bytes!(
        "../../../../tests/fixtures/ingest/import-event.jsonl"
    ))
    .unwrap();
    if case == "invalid-unbound" {
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
    let request = ProducerRequestId {
        producer_slot_id: spec.producer_slot_id,
        producer_epoch: ProducerEpoch::new(1).unwrap(),
        request_sequence: RequestSequence::new(2).unwrap(),
    };
    let cancelled = AtomicBool::new(false);
    let limits = AdmissionLimits {
        max_outstanding_requests: 2,
        ..AdmissionLimits::default()
    };
    let input = |acquisition, source| InitialEventRequest {
        ingest_root: path,
        registration: spec,
        event: PreparedEvent::new(request, intent(), 256 * 1024, grant(budget, MIB, 0)).unwrap(),
        relay_source: source,
        acquisition,
        provenance_enabled: false,
        recorded_at: ObservedTime::new(1234),
        limits,
        policy_ttl: Duration::from_secs(2),
        cancelled: &cancelled,
    };
    let endpoint = Endpoint::parse("wss://source.example/events").unwrap();
    let arrival = crate::config::acquisition::SyncArrivalPolicy {
        root,
        endpoint: &endpoint,
        route: RouteSelection::Public {},
        selector: scopes[0].selector(),
        visibility,
        observation,
        provenance_enabled: false,
    };
    let permit = || {
        grant(
            budget,
            limits.required_data_bytes().unwrap(),
            limits.required_native_bytes().unwrap(),
        )
    };
    let mut first_receipt = None;
    if matches!(
        case,
        "pass" | "changed" | "omitted" | "certify-expired" | "withdrawn"
    ) {
        let result = owner
            .admit_initial_event(
                store,
                input(Some(context(jobs[0], 0)), Some(source)),
                permit(),
                |d, h, e| arrival.admission(d, h, e),
            )
            .await
            .unwrap();
        let AdmissionOutcome::Durable(receipt) = result.outcome() else {
            panic!("signed fixture rejected")
        };
        first_receipt = Some(receipt.encode_persistent(65536).unwrap());
    }
    if case == "withdrawn" {
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
    if case.contains("expired") {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
        loop {
            let expired: bool = observer.query_one("SELECT deadline_at_us <= (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint FROM control.acquisition_jobs WHERE job_id=$1::bytea",
                &[&&jobs[0].as_bytes()[..]]).await.unwrap().get(0);
            if expired {
                break;
            }
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        emit(serde_json::json!({"event":"execution-expired", "job":jobs[0]}));
    }
    let before = ingest_snapshot::state(observer, path).await;
    emit(serde_json::json!({"event":"execution-before","state":before}));
    let selected = match case {
        "changed" => Some(context(jobs[1], 0)),
        "scope" => Some(context(jobs[0], 1)),
        "omitted" => {
            Some(AcquisitionEventContext::new(&scopes[0], scopes[0].selector().filters()).unwrap())
        }
        "unbound" | "invalid-unbound" | "certify-unbound" => None,
        _ => Some(context(jobs[0], 0)),
    };
    let selected_source = if case.contains("unbound") {
        None
    } else {
        Some(source)
    };
    let expected = match case {
        "expired" | "certify-expired" | "withdrawn" | "dispatch" => SourceBindingError::Unavailable,
        "old-schema" => SourceBindingError::Prerequisite,
        _ => SourceBindingError::Conflict,
    };
    if matches!(case, "certify-expired" | "certify-unbound") {
        let result = certify(
            owner,
            store,
            spec,
            path,
            budget,
            &cancelled,
            selected_source,
            selected,
            intent(),
            &arrival,
        )
        .await;
        assert_eq!(result.err(), Some(InitialEventError::Source(expected)));
    } else {
        let result = owner
            .admit_initial_event(
                store,
                input(selected, selected_source),
                permit(),
                |d, h, e| arrival.admission(d, h, e),
            )
            .await;
        if case == "pass" {
            let result = result.unwrap();
            let AdmissionOutcome::Durable(receipt) = result.outcome() else {
                panic!("retry rejected")
            };
            assert_eq!(
                receipt.encode_persistent(65536).unwrap(),
                first_receipt.unwrap()
            );
            drop(result);
            let proof = certify(
                owner,
                store,
                spec,
                path,
                budget,
                &cancelled,
                Some(source),
                selected,
                intent(),
                &arrival,
            )
            .await
            .unwrap();
            assert!(matches!(proof.outcome(), CertificationOutcome::Existing(_)));
        } else {
            assert_eq!(result.err(), Some(InitialEventError::Source(expected)));
        }
    }
    let after = ingest_snapshot::state(observer, path).await;
    if case == "dispatch" {
        for field in ["native_rows", "native_sha256", "sequence"] {
            assert_eq!(after[field], before[field]);
        }
    } else {
        assert_eq!(after, before);
    }
    emit(
        serde_json::json!({"event":"execution-finished","case":case,"state":after,
        "status":"PASS","zero_writes":case!="dispatch"}),
    );
    case == "pass"
}

#[allow(
    clippy::too_many_arguments,
    reason = "fixture borrows actual engine and retained intent"
)]
async fn certify(
    owner: &mut ControlSession,
    store: &mut IngestStore,
    spec: &InitialRegistration,
    path: &Path,
    budget: &ResourceBudget,
    cancelled: &AtomicBool,
    source: Option<RelaySourceBinding>,
    acquisition: Option<AcquisitionEventContext<'_>>,
    intent: EventIntent,
    policy: &crate::config::acquisition::SyncArrivalPolicy<'_>,
) -> Result<bigbrotr::storage::rocks::store::StoredCertification, InitialEventError> {
    let limits = CertificationLimits {
        max_fact_bytes: 1024,
        max_dependencies: 8,
    };
    owner
        .certify_initial_event(
            store,
            InitialCertificationRequest {
                ingest_root: path,
                registration: spec,
                event: PreparedCertification::new(intent, 256 * 1024, grant(budget, MIB, 0))
                    .unwrap(),
                relay_source: source,
                acquisition,
                provenance_enabled: false,
                limits,
                policy_ttl: Duration::from_secs(2),
                cancelled,
            },
            grant(budget, limits.required_data_bytes().unwrap(), 0),
            |d, h, e| policy.certification(d, h, e),
        )
        .await
}
