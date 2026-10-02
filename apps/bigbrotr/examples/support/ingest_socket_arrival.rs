//! Real signed socket -> retained observation -> current job/policy -> event effect.
//! The Python laboratory owns independent native/input/receipt expectations.
use super::*;
use base64::{Engine, engine::general_purpose::STANDARD};
use bigbrotr::{
    acquisition::{
        job::{AcquisitionCommand, AcquisitionJobBinding, JobDuration},
        observation::ObservationBindings,
        receive_clock::ReceiveClock,
        relay_source::RelaySourceBinding,
        scope::AcquisitionEventContext,
        session::{RelaySession, RetainedRelayEvent, SessionMessage},
    },
    management::{
        credentials::HASH_RESERVATION, grants::Revision, relay_enrollment::EnrollmentCommand,
        sessions::SessionLifetime,
    },
    network::{DirectConnector, DirectPolicy, DnsResolver, PrivateRule, RouteSelection, TlsPolicy},
    storage::{
        postgres::control_owner::{
            EnrollmentRoutes, InitialCertificationRequest, InitialEventRequest,
            InitialObservationRequest, InitialVisibilityRequest, RelaySourceRequest, ScopeIntent,
        },
        rocks::store::{
            AdmissionLimits, AdmissionOutcome, CertificationLimits, CertificationOutcome,
            PreparedCertification, PreparedEvent, RejectionReason,
        },
    },
};
use bigbrotr_model::{
    EventIntent, LocalObservation, ProducerEpoch, ProducerRequestId, RequestSequence,
    WitnessContext,
};
use bigbrotr_protocol::{
    EventLimits, decode_event_json, endpoint::Endpoint, selector::decode_selector,
    wire::RelayMessage,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{Instant, timeout},
};

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros()
        .try_into()
        .unwrap()
}

async fn peer(listener: &TcpListener) {
    let (mut socket, _) = listener.accept().await.unwrap();
    let mut request = Vec::new();
    while !request.ends_with(b"\r\n\r\n") {
        assert!(request.len() < 4096);
        request.push(socket.read_u8().await.unwrap());
    }
    let request = std::str::from_utf8(&request).unwrap();
    assert!(request.starts_with("GET /private HTTP/1.1\r\n"));
    assert!(request.contains("Host: 127.0.0.1:7447\r\n"));
    let key = request
        .lines()
        .find_map(|s| s.strip_prefix("Sec-WebSocket-Key: "))
        .unwrap();
    let digest = ring::digest::digest(
        &ring::digest::SHA1_FOR_LEGACY_USE_ONLY,
        format!("{key}258EAFA5-E914-47DA-95CA-C5AB0DC85B11").as_bytes(),
    );
    let accept = STANDARD.encode(digest.as_ref());
    let response = format!(
        "HTTP/1.1 101 Switching Protocols\r\n\
         Upgrade: websocket\r\n\
         Connection: Upgrade\r\n\
         Sec-WebSocket-Accept: {accept}\r\n\r\n"
    );
    socket.write_all(response.as_bytes()).await.unwrap();
    let request = command(&mut socket).await;
    assert_eq!(request.as_array().unwrap().len(), 3);
    assert_eq!(request[0], "REQ");
    assert_eq!(request[2], serde_json::json!({"kinds":[1],"limit":3}));
    let id = request[1].as_str().unwrap();
    let original: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../tests/fixtures/ingest/import-event.jsonl"
    ))
    .unwrap();
    let mut forged = original.clone();
    forged["sig"] = serde_json::json!("00".repeat(64));
    for value in [&original, &forged, &original] {
        frame(&mut socket, &format!(r#"["EVENT","{id}",{value}]"#)).await;
    }
    frame(&mut socket, &format!(r#"["EOSE","{id}"]"#)).await;
    assert_eq!(command(&mut socket).await, serde_json::json!(["CLOSE", id]));
    let mut close = [0; 6];
    socket.read_exact(&mut close).await.unwrap();
    assert_eq!(&close[..2], &[0x88, 0x80]);
    socket.write_all(&[0x88, 0]).await.unwrap();
    // The server closes its sending side after the Close handshake. Waiting
    // for client EOF first deadlocks a client that correctly awaits server EOF.
    socket.shutdown().await.unwrap();
    assert_eq!(socket.read(&mut [0; 1]).await.unwrap(), 0);
}
async fn command(socket: &mut TcpStream) -> serde_json::Value {
    assert_eq!(socket.read_u8().await.unwrap(), 0x81);
    let flags = socket.read_u8().await.unwrap();
    assert_ne!(flags & 0x80, 0);
    let count = match flags & 0x7f {
        126 => usize::from(socket.read_u16().await.unwrap()),
        127 => panic!("oversized command"),
        v => usize::from(v),
    };
    assert!(count < 4096);
    let mut mask = [0; 4];
    socket.read_exact(&mut mask).await.unwrap();
    let mut bytes = vec![0; count];
    socket.read_exact(&mut bytes).await.unwrap();
    for (i, b) in bytes.iter_mut().enumerate() {
        *b ^= mask[i % 4];
    }
    serde_json::from_slice(&bytes).unwrap()
}
async fn frame(socket: &mut TcpStream, text: &str) {
    if text.len() < 126 {
        socket.write_all(&[0x81, text.len() as u8]).await.unwrap();
    } else {
        socket.write_all(&[0x81, 126]).await.unwrap();
        socket
            .write_u16(text.len().try_into().unwrap())
            .await
            .unwrap();
    }
    socket.write_all(text.as_bytes()).await.unwrap();
}

#[allow(
    clippy::too_many_arguments,
    reason = "fixture retains one actual native/Control owner"
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
    assert!(matches!(
        mode,
        "source-socket-pass" | "source-socket-dictionary-append"
    ));
    let event_sequence = if mode == "source-socket-dictionary-append" {
        4
    } else {
        2
    };
    // Both fixtures contain exactly one mapping of each attribution kind. The
    // append fixture deliberately preserves the original two-map bootstrap.
    let entry = |kind| {
        let mut matching = intent
            .dictionaries()
            .iter()
            .filter(|e| e.reference().dictionary_kind == kind);
        let value = matching.next().unwrap();
        assert!(matching.next().is_none());
        value
    };
    let endpoint = Endpoint::parse("ws://127.0.0.1:7447/private").unwrap();
    let route = RouteSelection::Private {
        cidr: "127.0.0.1/32".parse().unwrap(),
    };
    let deployment = include_str!("../../../../config/recipes/archive-only.toml")
        .replace(
            "22222222222222222222222222222222",
            &hex(spec.dataset_id.as_bytes()),
        )
        .replace("11111111111111111111111111111111", &hex(&spec.instance_id));
    let private = "\n[network.private]\nrules=[{\
        endpoint='ws://127.0.0.1:7447/private',cidr='127.0.0.1/32',port=7447}]\n";
    let root = crate::config::resolve::parse(&format!("{deployment}{private}")).unwrap();
    let validate = |d: &str, h| crate::config::validate_fixture_policy(d, h, &root);
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
    let browser = owner
        .finish_browser_login(verified, SessionLifetime::default())
        .await
        .unwrap();
    let endpoint_id = owner
        .register_relay_endpoint(&endpoint, &Cell::new(false))
        .await
        .unwrap()
        .identity;
    let visibility = intent.visibility_scope_id();
    let ceiling = DirectPolicy::private(vec![
        PrivateRule::endpoint(endpoint.clone(), "127.0.0.1/32", 7447).unwrap(),
    ])
    .unwrap();
    let enrolled = owner
        .accept_relay_enrollment(
            &browser.session,
            &browser.csrf,
            [1; 16].try_into().unwrap(),
            &EnrollmentCommand {
                endpoint_id,
                visibility,
                route,
                enabled: true,
                expected_generation: None,
                expected_config_revision: Revision::try_from("1".to_owned()).unwrap(),
                expected_policy_revision: Revision::try_from("1".to_owned()).unwrap(),
            },
            EnrollmentRoutes {
                public_enabled: true,
                private_ceiling: Some(&ceiling),
            },
            &Cell::new(false),
        )
        .await
        .unwrap();
    let source = RelaySourceBinding {
        endpoint_id,
        access_scope_id: enrolled.access_scope_id,
        enrollment_generation: enrolled.generation.get().try_into().unwrap(),
        route_id: enrolled.route_id,
    };
    let limits = CertificationLimits {
        max_fact_bytes: 8192,
        max_dependencies: 16,
    };
    let cancelled = AtomicBool::new(false);
    let visible = owner
        .read_initial_visibility(
            store,
            InitialVisibilityRequest {
                ingest_root: path,
                registration: spec,
                reference: visibility,
                limits,
                policy_ttl: Duration::from_secs(2),
                cancelled: &cancelled,
            },
            grant(budget, limits.required_data_bytes().unwrap(), 0),
            validate,
        )
        .await
        .unwrap()
        .unwrap();
    let attribution = owner
        .read_initial_observation(
            store,
            InitialObservationRequest {
                ingest_root: path,
                registration: spec,
                bindings: ObservationBindings {
                    visibility,
                    observer: entry(DictionaryKind::Observer),
                    clock: entry(DictionaryKind::Observer),
                    witness: Some(WitnessContext {
                        endpoint_id: entry(DictionaryKind::Endpoint).reference(),
                        access_scope_id: entry(DictionaryKind::AccessScope).reference(),
                        verification_method_id: entry(DictionaryKind::Method).reference(),
                    }),
                },
                limits,
                policy_ttl: Duration::from_secs(2),
                cancelled: &cancelled,
            },
            grant(budget, limits.required_data_bytes().unwrap(), 0),
            validate,
        )
        .await
        .unwrap()
        .unwrap();
    let selector = decode_selector(
        br#"{"version":1,"filters":[{"kinds":[1]}],"history_since":0,"ephemeral":"retain"}"#,
    )
    .unwrap();
    let registered = owner
        .register_acquisition_scope(
            ScopeIntent {
                selector: &selector,
                endpoint: endpoint_id,
                access: source.access_scope_id,
                visibility,
                policy_epoch: PolicyEpoch::new(1),
            },
            &Cell::new(false),
        )
        .await
        .unwrap();
    let scope = owner
        .read_acquisition_scope(registered.identity, grant(budget, 8 * MIB, 0))
        .await
        .unwrap()
        .unwrap();
    let job = owner
        .accept_acquisition(
            &browser.session,
            &browser.csrf,
            [77; 16].try_into().unwrap(),
            &AcquisitionCommand {
                scope_id: scope.identity(),
                deadline_us: JobDuration::new(30_000_000).unwrap(),
                expected_config_revision: Revision::try_from("1".to_owned()).unwrap(),
                expected_policy_revision: Revision::try_from("1".to_owned()).unwrap(),
            },
            &Cell::new(false),
        )
        .await
        .unwrap()
        .job_id;
    owner
        .claim_acquisition(job, &Cell::new(false))
        .await
        .unwrap();
    let before = ingest_snapshot::state(observer, path).await;
    emit(
        serde_json::json!({"event":"socket-before","state":before,"job":job,"scope":scope.identity()}),
    );
    let listener = TcpListener::bind("127.0.0.1:7447").await.unwrap();
    let connector = DirectConnector::new(
        DnsResolver::new("127.0.0.1:9".parse().unwrap(), budget.clone()).unwrap(),
        TlsPolicy::new(&[]).unwrap(),
        budget.clone(),
    );
    let receive = owner.run_relay_source(
        RelaySourceRequest {
            source,
            acquisition: Some(AcquisitionJobBinding {
                job,
                scope: scope.identity(),
            }),
            visibility: &visible,
            policy_ttl: Duration::from_secs(5),
            deadline: Instant::now() + Duration::from_secs(5),
        },
        &connector,
        |d, h, actual_endpoint, actual_route| {
            if actual_endpoint != &endpoint || actual_route != route {
                return Err(OwnerError::InvalidMetadata);
            }
            crate::config::acquisition::authorize_sync_collection(
                &root,
                d,
                h,
                actual_endpoint,
                actual_route,
                scope.selector(),
            )
        },
        async |stream| {
            assert_eq!(stream.endpoint(), &endpoint);
            let before = now();
            let mut session = RelaySession::connect(
                stream,
                budget,
                EventLimits::default(),
                1.try_into().unwrap(),
            )
            .await?;
            let id = session.open(scope.selector().filters(), Some(3)).await?;
            // A finite fixture batch; production scheduling/EOSE joins remain separate.
            let batch_credit = grant(
                budget,
                (3 * size_of::<(RetainedRelayEvent<'_>, LocalObservation)>() + size_of::<Vec<()>>())
                    as u64,
                0,
            );
            let mut arrivals = Vec::with_capacity(3);
            for _ in 0..3 {
                let SessionMessage::Relay(message) = session.receive().await?.unwrap() else {
                    panic!("EVENT")
                };
                let captured = message.retain_event(id, budget).unwrap();
                let observation = attribution
                    .received(&endpoint, route, captured.received())
                    .unwrap();
                assert!((before..=now()).contains(&observation.received_at.get()));
                arrivals.push((captured, observation));
            }
            let SessionMessage::Relay(message) = session.receive().await?.unwrap() else {
                panic!("EOSE")
            };
            assert!(matches!(
                message.envelope().message(),
                RelayMessage::Eose { .. }
            ));
            assert_eq!(message.request().unwrap().id(), id);
            session.close_request(id).await?;
            session.close().await?;
            Ok((arrivals, batch_credit))
        },
    );
    let (arrivals, ()) = timeout(Duration::from_secs(6), async {
        tokio::join!(receive, peer(&listener))
    })
    .await
    .unwrap();
    // Only the successful guarded source return can deliver retained bytes.
    let (arrivals, batch_credit) = arrivals.unwrap();
    assert_eq!(ingest_snapshot::state(observer, path).await, before);
    for (index, (captured, observation)) in arrivals.iter().enumerate() {
        let context =
            AcquisitionEventContext::for_job(job, &scope, captured.request().filters()).unwrap();
        let arrival = crate::config::acquisition::SyncArrivalPolicy {
            root: &root,
            endpoint: &endpoint,
            route,
            selector: scope.selector(),
            visibility,
            observation: *observation,
            provenance_enabled: true,
        };
        emit(
            serde_json::json!({"event":"socket-captured","index":index,"sequence":captured.sequence(),"received_at":observation.received_at,"clock":observation.clock_quality}),
        );
        let before = ingest_snapshot::state(observer, path).await;
        let prepared = {
            let permit = grant(budget, 9 * MIB, 0);
            let decoded = decode_event_json(captured.json(), captured.limits()).unwrap();
            PreparedCertification::new(
                EventIntent::network(1, visibility, decoded, *observation).unwrap(),
                256 * 1024,
                permit,
            )
            .unwrap()
        };
        let certified = owner
            .certify_initial_event(
                store,
                InitialCertificationRequest {
                    ingest_root: path,
                    registration: spec,
                    event: prepared,
                    relay_source: Some(source),
                    acquisition: Some(context),
                    provenance_enabled: true,
                    limits,
                    policy_ttl: Duration::from_secs(2),
                    cancelled: &cancelled,
                },
                grant(budget, limits.required_data_bytes().unwrap(), 0),
                |d, h, e| arrival.certification(d, h, e),
            )
            .await
            .unwrap();
        assert_eq!(ingest_snapshot::state(observer, path).await, before);
        match index {
            0 => assert_eq!(certified.outcome(), &CertificationOutcome::NeedsAdmission),
            1 => assert_eq!(
                certified.outcome(),
                &CertificationOutcome::Rejected(RejectionReason::Signature)
            ),
            2 => assert!(matches!(
                certified.outcome(),
                CertificationOutcome::Existing(_)
            )),
            _ => unreachable!(),
        }
        drop(certified);
        if index == 0 {
            let permit = grant(budget, 9 * MIB, 0);
            let decoded = decode_event_json(captured.json(), captured.limits()).unwrap();
            let request = ProducerRequestId {
                producer_slot_id: spec.producer_slot_id,
                producer_epoch: ProducerEpoch::new(1).unwrap(),
                request_sequence: RequestSequence::new(event_sequence).unwrap(),
            };
            let event = PreparedEvent::new(
                request,
                EventIntent::network(1, visibility, decoded, *observation).unwrap(),
                256 * 1024,
                permit,
            )
            .unwrap();
            let limits = AdmissionLimits {
                max_outstanding_requests: u64::from(spec.outstanding_request_limit),
                ..AdmissionLimits::default()
            };
            let recorded = ReceiveClock::new().unwrap().sample().unwrap().received_at();
            let result = owner
                .admit_initial_event(
                    store,
                    InitialEventRequest {
                        ingest_root: path,
                        registration: spec,
                        event,
                        relay_source: Some(source),
                        acquisition: Some(context),
                        provenance_enabled: true,
                        recorded_at: recorded,
                        limits,
                        policy_ttl: Duration::from_secs(2),
                        cancelled: &cancelled,
                    },
                    grant(
                        budget,
                        limits.required_data_bytes().unwrap(),
                        limits.required_native_bytes().unwrap(),
                    ),
                    |d, h, e| arrival.admission(d, h, e),
                )
                .await
                .unwrap();
            let AdmissionOutcome::Durable(receipt) = result.outcome() else {
                panic!("signed EVENT rejected")
            };
            emit(
                serde_json::json!({"event":"socket-admitted","recorded_at":recorded,"receipt":hex(&receipt.encode_persistent(65536).unwrap())}),
            );
        }
    }
    drop(arrivals);
    drop(batch_credit);
    emit(
        serde_json::json!({"event":"socket-finished","status":"PASS","state":ingest_snapshot::state(observer,path).await}),
    );
}
