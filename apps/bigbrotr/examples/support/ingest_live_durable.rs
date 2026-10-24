//! Genuine finite network resend of the frozen predecessor event. The new job,
//! session and request are real; no installed scheduler or job completion is claimed.
use super::*;
use base64::{Engine, engine::general_purpose::STANDARD};
use bigbrotr::{
    acquisition::{
        job::{AcquisitionCommand, AcquisitionJobBinding, JobDuration},
        observation::ObservationBindings,
        relay_source::StoredRelaySource,
        scope::AcquisitionEventContext,
        session::{RelaySession, SessionMessage},
    },
    management::{
        credentials::HASH_RESERVATION,
        grants::Revision,
        sessions::{IssuedSession, SessionLifetime},
    },
    network::{DirectConnector, DnsResolver, RouteSelection, TlsPolicy},
    storage::{
        postgres::control_owner::{
            LiveInitialObservationRequest, LiveInitialVisibilityRequest, RelaySourceRequest,
        },
        rocks::store::CertificationLimits,
    },
};
use bigbrotr_model::{
    AcquisitionScopeId,
    codec::request::{MutationIntent, decode_request},
};
use bigbrotr_protocol::{EventLimits, decode_event_json, wire::RelayMessage};
use std::io::Read;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::Instant,
};

const EVENT: &str = include_str!("../../../../tests/fixtures/ingest/import-event.jsonl");
const ENDPOINT: &str = "ws://127.0.0.1:7447/installed-fixture";

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Mode {
    AlreadyDurable,
    ExtraIntents,
    Limit64,
    Limit65,
    ZeroClaim,
    CompactedTerminalEvent,
    CompactedZeroClaim,
}
impl Mode {
    pub(super) fn parse(mode: &str) -> Option<Self> {
        match mode {
            "successor-live-already-durable" => Some(Self::AlreadyDurable),
            "successor-live-acquisition-claims" => Some(Self::ExtraIntents),
            "successor-live-acquisition-limit64" => Some(Self::Limit64),
            "successor-live-acquisition-limit65" => Some(Self::Limit65),
            "successor-live-acquisition-zero-claim" => Some(Self::ZeroClaim),
            "successor-live-compacted-terminal-event" => Some(Self::CompactedTerminalEvent),
            "successor-live-compacted-terminal-zero-claim" => Some(Self::CompactedZeroClaim),
            _ => None,
        }
    }
    pub(super) fn limit(self) -> Option<u8> {
        match self {
            Self::Limit64 => Some(64),
            Self::Limit65 => Some(65),
            Self::AlreadyDurable
            | Self::ExtraIntents
            | Self::ZeroClaim
            | Self::CompactedTerminalEvent
            | Self::CompactedZeroClaim => None,
        }
    }
    pub(super) fn is_zero_claim(self) -> bool {
        matches!(self, Self::ZeroClaim | Self::CompactedZeroClaim)
    }
}

fn original(
    probe: &Probe,
    previous: &InitialRegistration,
    sequence: u64,
    modern: bool,
) -> MutationIntent {
    let expected = match (modern, sequence) {
        (false, 3) => "08c9b935c7c101c704f7c741847f26baad8063438fea836aff1678e4070240e5",
        (false, 4) => "19820e2cea792c8780de0d8542baa76dabbdb611ab96237eed2230a228d82977",
        (true, 3) => "a1992bc4cc2b57578e06da9ab64cdfe44d8c117a394e8fdb38355e955a64eee9",
        (true, 5) => "4add1a3691de324ebdc7202debd5f0187cbd895a997d16ec499cdfea16827a26",
        _ => unreachable!(),
    };
    // The compacted fixture retains attribution originals separately from the
    // native spool. They supply a fixed dictionary/observation recipe, never
    // a reconstructed epoch1 input or an input to the compacted proof scanner.
    let directory = if modern {
        probe.path.parent().unwrap().join("terminal-seed-inputs")
    } else {
        probe.path.join("producer-inputs-v1")
    };
    let path = directory.join(format!(
        "{}-0000000000000001-{sequence:016x}.cbor",
        previous.producer_slot_id
    ));
    assert!(std::fs::symlink_metadata(&path).unwrap().is_file());
    // The already retained metadata grant covers these small frozen inputs.
    // The previous full positive replay remains the historical proof.
    let mut bytes = Vec::new();
    std::fs::File::open(path)
        .unwrap()
        .take(8193)
        .read_to_end(&mut bytes)
        .unwrap();
    assert!(bytes.len() <= 8192);
    let digest = Digest256::from_bytes(Sha256::digest(&bytes).into());
    assert_eq!(digest.to_string(), expected);
    let (request, intent) = decode_request(
        &bytes,
        digest,
        RequestDecodeLimits {
            values: FactDecodeLimits {
                max_encoded_bytes: 8192,
                max_allocation_bytes: 32 * 1024,
                max_tag_nodes: 32,
                max_string_bytes: 8192,
            },
            max_dictionaries: 4,
        },
    )
    .unwrap();
    assert_eq!(request.producer_slot_id, previous.producer_slot_id);
    assert_eq!(request.producer_epoch.get(), 1);
    assert_eq!(request.request_sequence.get(), sequence);
    intent
}

pub(super) async fn run(
    probe: &Probe,
    owner: &mut ControlSession,
    writer: &mut LiveInitialWriter,
    previous: &InitialRegistration,
    mode: Mode,
) -> bool {
    if mode.is_zero_claim() {
        return zero_claim(probe, owner, mode == Mode::CompactedZeroClaim).await;
    }
    assert!(
        probe.root.local("sync"),
        "explicit finite network role selection"
    );
    let modern = mode == Mode::CompactedTerminalEvent;
    assert!(!modern || probe.compacted_initial);
    let (mut expected_rows, before_sequence) = raw_state(&probe.path);
    assert_eq!(
        (expected_rows.len(), before_sequence),
        if modern { (84, 125) } else { (52, 62) }
    );
    let input_directory = probe.path.join("producer-inputs-v1");
    assert_eq!(
        std::fs::read_dir(&input_directory).unwrap().count(),
        if modern { 0 } else { 4 }
    );
    let MutationIntent::Evidence(dictionaries) = original(probe, previous, 3, modern) else {
        panic!("frozen attribution request3");
    };
    let MutationIntent::Event(old_event) =
        original(probe, previous, if modern { 5 } else { 4 }, modern)
    else {
        panic!("frozen signed network attribution");
    };
    let signed: SignedEvent = serde_json::from_str(EVENT).unwrap();
    if modern {
        assert_eq!(
            hex(previous.dataset_id.as_bytes()),
            "309d9507c5b5c39e9947e1406482d519"
        );
        assert_eq!(
            old_event.event().id.to_string(),
            "32be96c6ec271705225aad466eebc4f8352c65960e619164be86eae0d442a791"
        );
        let event_key = hex(&[
            &[1, 6][..],
            previous.dataset_id.as_bytes(),
            signed.id.as_bytes(),
        ]
        .concat());
        assert!(
            !expected_rows
                .iter()
                .any(|r| r["cf"] == "default" && r["key"] == event_key)
        );
    } else {
        assert_eq!(old_event.event(), &signed);
    }
    assert_eq!(old_event.origin(), bigbrotr_model::EventOrigin::Network);
    let old_observation = *old_event.observation().unwrap();
    assert!(old_observation.witness.is_some());
    let visibility = old_event.visibility_scope_id();
    assert_eq!(visibility, public_scope(previous)); // both pinned scopes are private ordinal3
    if modern {
        assert_eq!(
            hex(visibility.allocation_domain.as_bytes()),
            "0e2a2cc69f18bd685ff795bd57faf3c0"
        );
        assert_eq!(visibility.ordinal.get(), 3);
    }
    assert_eq!(dictionaries.visibility_scope_id(), visibility);
    let observer_entry = dictionaries
        .dictionaries()
        .iter()
        .find(|entry| entry.reference() == old_observation.observer_id)
        .unwrap();
    let clock_entry = dictionaries
        .dictionaries()
        .iter()
        .find(|entry| entry.reference() == old_observation.clock_quality.clock_id())
        .unwrap();

    let scope_id: AcquisitionScopeId = if modern {
        "422b7d8b33d4b55977806fb711e2d082"
    } else {
        "a39723d7c49af2c99498fb2f59c03b16"
    }
    .parse()
    .unwrap();
    let scope = bounded(owner.read_acquisition_scope(scope_id, grant(&probe.budget, 8 * MIB, 0)))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(scope.scope().visibility(), visibility);
    assert_eq!(scope.scope().policy_epoch().get(), 1);
    let source = bounded(owner.read_acquisition_source(
        &scope,
        grant(&probe.budget, StoredRelaySource::REQUIRED_BYTES, 0),
    ))
    .await
    .unwrap()
    .unwrap();
    assert_eq!(source.endpoint().as_str(), ENDPOINT);
    assert_eq!(
        source.route(),
        RouteSelection::Private {
            cidr: "127.0.0.1/32".parse().unwrap()
        }
    );

    let verified = bounded(owner.begin_browser_login("alice", "127.0.0.1".parse().unwrap()))
        .await
        .unwrap()
        .verify(
            b"Independent owner password 2026",
            &mut grant(&probe.budget, HASH_RESERVATION, 0),
        )
        .unwrap();
    let browser = bounded(owner.finish_browser_login(verified, SessionLifetime::default()))
        .await
        .unwrap();
    let policy = bounded(owner.refresh(|doc, digest| probe.validate(doc, digest)))
        .await
        .unwrap();
    assert_eq!((policy.config_revision, policy.policy_revision), (1, 1));
    let accepted = bounded(owner.accept_acquisition(
        &browser.session,
        &browser.csrf,
        [0xa6; 16].try_into().unwrap(),
        &AcquisitionCommand {
            scope_id,
            deadline_us: JobDuration::new(30_000_000).unwrap(),
            expected_config_revision: Revision::try_from("1".to_owned()).unwrap(),
            expected_policy_revision: Revision::try_from("1".to_owned()).unwrap(),
        },
        &Cell::new(false),
    ))
    .await
    .unwrap();
    assert!(accepted.created);
    let job = bounded(owner.claim_acquisition(accepted.job_id, &Cell::new(false)))
        .await
        .unwrap();
    assert_eq!(job.revision, 2);
    assert_eq!(job.checkpoint_revision, 0);
    assert_eq!(job.owner.unwrap().epoch, owner.identity().epoch);
    emit(
        serde_json::json!({"event":"live-durable-job","job":job.id,"scope":scope_id,
        "accepted_at_us":job.accepted_at_us,"deadline_at_us":job.deadline_at_us,
        "control_epoch":owner.identity().epoch.get(),"request_key":hex(&[0xa6;16]),
        "provenance":true,"existing_dictionaries":true}),
    );
    probe.snapshot("durable-job-claimed").await;

    let read_limits = CertificationLimits {
        max_fact_bytes: 8192,
        max_dependencies: 16,
    };
    let visible = bounded(owner.read_live_initial_visibility(
        writer,
        LiveInitialVisibilityRequest {
            ingest_root: &probe.path,
            reference: visibility,
            limits: read_limits,
            policy_ttl: probe.policy_ttl(),
            cancelled: &probe.cancelled,
        },
        grant(&probe.budget, read_limits.required_data_bytes().unwrap(), 0),
        |doc, digest| probe.validate(doc, digest),
    ))
    .await
    .unwrap()
    .unwrap();
    if modern {
        emit(serde_json::json!({"event":"live-terminal-certification",
            "kind":"visibility","available":true}));
    }
    let observation = bounded(owner.read_live_initial_observation(
        writer,
        LiveInitialObservationRequest {
            ingest_root: &probe.path,
            bindings: ObservationBindings {
                visibility,
                observer: observer_entry,
                clock: clock_entry,
                witness: old_observation.witness,
            },
            limits: read_limits,
            policy_ttl: probe.policy_ttl(),
            cancelled: &probe.cancelled,
        },
        grant(&probe.budget, read_limits.required_data_bytes().unwrap(), 0),
        |doc, digest| probe.validate(doc, digest),
    ))
    .await
    .unwrap()
    .unwrap();
    if modern {
        emit(serde_json::json!({"event":"live-terminal-certification",
            "kind":"observation","available":true}));
    }

    // Account the socket/session/captured-message overlap in the parent before
    // subdividing it. The independent peer is finite laboratory scaffolding.
    let _network_reservation = grant(&probe.budget, 64 * MIB, 0);
    let network = ResourceBudget::new(
        ProcessEnvelope::new(
            64 * MIB,
            0,
            ResourceLimits::new(64 * MIB, 16, 0, 0),
            WaiterLimits::new(0, 0),
        )
        .unwrap(),
    );
    let listener = TcpListener::bind("127.0.0.1:7447").await.unwrap();
    let connector = DirectConnector::new(
        DnsResolver::new("127.0.0.1:9".parse().unwrap(), network.clone()).unwrap(),
        TlsPolicy::new(&[]).unwrap(),
        network.clone(),
    );
    let receive = owner.run_relay_source(
        RelaySourceRequest {
            source: source.binding(),
            acquisition: Some(AcquisitionJobBinding {
                job: job.id,
                scope: scope_id,
            }),
            visibility: &visible,
            policy_ttl: probe.policy_ttl(),
            deadline: Instant::now() + Duration::from_secs(5),
        },
        &connector,
        |doc, digest, endpoint, route| {
            assert_eq!(endpoint, source.endpoint());
            assert_eq!(route, source.route());
            crate::config::acquisition::authorize_sync_collection(
                &probe.root,
                doc,
                digest,
                endpoint,
                route,
                scope.selector(),
            )
        },
        async |stream| {
            let mut session = RelaySession::connect(
                stream,
                &network,
                EventLimits::default(),
                1.try_into().unwrap(),
            )
            .await?;
            let id = session.open(scope.selector().filters(), Some(1)).await?;
            let SessionMessage::Relay(message) = session.receive().await?.unwrap() else {
                panic!("one correlated signed EVENT");
            };
            let captured = message.retain_event(id, &network).unwrap();
            let received = observation
                .received(source.endpoint(), source.route(), captured.received())
                .unwrap();
            assert!(received.received_at > old_observation.received_at);
            let SessionMessage::Relay(message) = session.receive().await?.unwrap() else {
                panic!("EOSE");
            };
            assert!(matches!(
                message.envelope().message(),
                RelayMessage::Eose { .. }
            ));
            assert_eq!(message.request().unwrap().id(), id);
            session.close_request(id).await?;
            session.close().await?;
            Ok((captured, received))
        },
    );
    // Only the actual receive operation can return an error. Preserve that
    // error immediately instead of waiting for a peer that may never connect.
    let arrived = bounded(async {
        tokio::try_join!(receive, async {
            peer(&listener).await;
            Ok(())
        })
    })
    .await;
    if modern && let Err(error) = &arrived {
        emit(serde_json::json!({"event":"live-terminal-receive-error",
            "error":format!("{error:?}")}));
    }
    let ((captured, received), ()) = arrived.unwrap();
    drop(listener);
    drop(connector);
    drop(visible);
    drop(observation);
    probe.snapshot("durable-received").await;

    let event = decode_event_json(captured.json(), captured.limits()).unwrap();
    assert_eq!(event, signed);
    let request = ProducerRequestId {
        producer_slot_id: writer.producer_slot_id(),
        producer_epoch: ProducerEpoch::new(2).unwrap(),
        request_sequence: RequestSequence::new(1).unwrap(),
    };
    let intent = EventIntent::network(1, visibility, event, received).unwrap();
    let preimage = intent.encode_request_preimage(request, 8192).unwrap();
    emit(serde_json::json!({"event":"live-durable-input","request":1,
        "event_id":signed.id,"preimage":hex(&preimage),
        "request_digest":intent.request_digest(request),"provenance":true,
        "wire_sequence":captured.sequence(),"wire_request":captured.request().id().as_str(),
        "received_at_us":received.received_at.get(),
        "predecessor_received_at_us":old_observation.received_at.get()}));
    let prepared =
        PreparedEvent::new(request, intent, 256 * 1024, grant(&probe.budget, MIB, 0)).unwrap();
    let arrival_policy = crate::config::acquisition::SyncArrivalPolicy {
        root: &probe.root,
        endpoint: source.endpoint(),
        route: source.route(),
        selector: scope.selector(),
        visibility,
        observation: received,
        provenance_enabled: true,
    };
    let limits = AdmissionLimits {
        max_outstanding_requests: u64::from(writer.outstanding_request_limit()),
        max_dependencies: 256,
        ..AdmissionLimits::default()
    };
    let result = bounded(
        owner.admit_live_initial_event(
            writer,
            LiveInitialEventRequest {
                ingest_root: &probe.path,
                event: prepared,
                relay_source: Some(source.binding()),
                acquisition: Some(
                    AcquisitionEventContext::for_job(job.id, &scope, captured.request().filters())
                        .unwrap(),
                ),
                provenance_enabled: true,
                recorded_at: received.received_at,
                limits,
                policy_ttl: probe.policy_ttl(),
                cancelled: &probe.cancelled,
            },
            grant(
                &probe.budget,
                limits.required_data_bytes().unwrap(),
                limits.required_native_bytes().unwrap(),
            ),
            |doc, digest, candidate| arrival_policy.admission(doc, digest, candidate),
        ),
    )
    .await;
    // Preserve effects even when the expected positive result is not delivered.
    probe
        .snapshot(if modern {
            "terminal-event"
        } else {
            "already-durable"
        })
        .await;
    let outcome = result.unwrap();
    let AdmissionOutcome::Durable(receipt) = outcome.outcome() else {
        panic!("the retained signed event must remain positively durable");
    };
    if modern {
        assert_eq!(receipt.batch().unwrap().records().len(), 5);
        let dependencies = receipt.dependencies().records();
        assert_eq!(dependencies.len(), 10);
        for (dependency, (epoch, sequence)) in dependencies
            .iter()
            .zip((3..=7).map(|n| (1, n)).chain((1..=5).map(|n| (2, n))))
        {
            let source = dependency.source_record_id;
            assert_eq!(source.dataset_id, previous.dataset_id);
            assert_eq!(source.shard_id, previous.shard_id);
            assert_eq!(
                (source.owner_epoch.get(), source.sequence.get()),
                (epoch, sequence)
            );
        }
        let encoded = receipt.encode_persistent(65536).unwrap();
        let (actual, sequence) = raw_state(&probe.path);
        // Five new scientific facts and their exact indices are independently
        // constructed by the Python oracle. Here bind the predicted physical
        // count and preserve the complete in-process snapshot for that checker.
        assert_eq!((actual.len(), sequence), (98, 141));
        assert_eq!(std::fs::read_dir(&input_directory).unwrap().count(), 1);
        assert_eq!(
            std::fs::read(input_directory.join(format!(
                "{}-0000000000000002-0000000000000001.cbor",
                request.producer_slot_id
            )))
            .unwrap(),
            preimage
        );
        emit(
            serde_json::json!({"event":"live-terminal-event","request":1,
            "receipt":hex(&encoded),"dependencies":10,"source_cut":5,
            "native_rows":98,"native_sequence":141,"peer_joined":true,
            "job_complete":false,"session_revoked":false}),
        );
        return true;
    }
    assert!(receipt.batch().is_none());
    assert_eq!(receipt.dependencies().records().len(), 10);
    for (dependency, sequence) in receipt.dependencies().records().iter().zip(3..=12) {
        let source_record = dependency.source_record_id;
        assert_eq!(source_record.dataset_id, previous.dataset_id);
        assert_eq!(source_record.shard_id, previous.shard_id);
        assert_eq!(source_record.owner_epoch.get(), 1);
        assert_eq!(source_record.sequence.get(), sequence);
    }
    let encoded = receipt.encode_persistent(65536).unwrap();
    emit(
        serde_json::json!({"event":"live-already-durable","request":1,
        "receipt":hex(&encoded),"dependencies":10,"source_cut":0,
        "peer_joined":true,"job_complete":false,"session_revoked":false}),
    );
    drop(outcome);
    let producer_key = hex(&[
        &[1, 0x31][..],
        request.producer_slot_id.as_bytes(),
        &2_u64.to_be_bytes(),
    ]
    .concat());
    let marker = expected_rows
        .iter_mut()
        .find(|r| r["cf"] == "receipts" && r["key"] == producer_key)
        .unwrap();
    assert_eq!(marker["value"], hex(&[&[1, 0x31][..], &[0; 24]].concat()));
    marker["value"] = hex(&[&[1, 0x31][..], &1_u64.to_be_bytes(), &[0; 16]].concat()).into();
    let request_key = hex(&[
        &[1, 0x30][..],
        request.producer_slot_id.as_bytes(),
        &2_u64.to_be_bytes(),
        &1_u64.to_be_bytes(),
    ]
    .concat());
    expected_rows
        .push(serde_json::json!({"cf":"receipts","key":request_key,"value":hex(&encoded)}));
    expected_rows.sort_unstable_by(|a, b| {
        (a["cf"].as_str().unwrap(), a["key"].as_str().unwrap())
            .cmp(&(b["cf"].as_str().unwrap(), b["key"].as_str().unwrap()))
    });
    let (actual, sequence) = raw_state(&probe.path);
    assert_eq!((actual.len(), sequence), (53, 64));
    assert_eq!(
        actual, expected_rows,
        "only request receipt and issued marker change"
    );
    assert_eq!(
        std::fs::read(input_directory.join(format!(
            "{}-0000000000000002-0000000000000001.cbor",
            request.producer_slot_id
        )))
        .unwrap(),
        preimage
    );
    assert_eq!(std::fs::read_dir(&input_directory).unwrap().count(), 5);
    if mode == Mode::ExtraIntents {
        // Keep the actual activator and authenticated session alive. A queued
        // claim with no producer requests must be discoverable independently
        // of request bindings; the other accepted intent must remain unclaimed.
        let before_inputs = input_bytes(&input_directory, 5);
        let refreshed = bounded(owner.refresh(|doc, digest| probe.validate(doc, digest))).await;
        probe.snapshot("claims-control-refreshed").await;
        let policy = refreshed.unwrap();
        assert_eq!((policy.config_revision, policy.policy_revision), (1, 1));
        let mut jobs = Vec::with_capacity(2);
        for (purpose, key, stage) in [
            ("zero-request", 0xa7, "claims-zero-accepted"),
            ("unclaimed", 0xa8, "claims-unclaimed-accepted"),
        ] {
            let started = Cell::new(false);
            let accepted = bounded(owner.accept_acquisition(
                &browser.session,
                &browser.csrf,
                [key; 16].try_into().unwrap(),
                &AcquisitionCommand {
                    scope_id,
                    deadline_us: JobDuration::new(30_000_000).unwrap(),
                    expected_config_revision: Revision::try_from("1".to_owned()).unwrap(),
                    expected_policy_revision: Revision::try_from("1".to_owned()).unwrap(),
                },
                &started,
            ))
            .await;
            emit(
                serde_json::json!({"event":"live-extra-acceptance","purpose":purpose,
                "receipt":accepted.as_ref().ok(),"error":accepted.as_ref().err().map(|e|format!("{e:?}")),
                "commit_started":started.get()}),
            );
            probe.snapshot(stage).await;
            let accepted = accepted.unwrap();
            assert!(accepted.created && started.get());
            jobs.push(accepted.job_id);
            if purpose == "zero-request" {
                let started = Cell::new(false);
                let claimed = bounded(owner.claim_acquisition(accepted.job_id, &started)).await;
                let observed = claimed.as_ref().ok().map(|job| {
                    serde_json::json!({"id":job.id,"scope":job.scope,"state":job.state,
                        "job_revision":job.revision,"checkpoint_revision":job.checkpoint_revision,
                        "accepted_at_us":job.accepted_at_us,"deadline_at_us":job.deadline_at_us,
                        "owner":job.owner.map(|identity|serde_json::json!({"resource":identity.resource,
                            "epoch":identity.epoch.get(),"incarnation":identity.incarnation}))})
                });
                emit(
                    serde_json::json!({"event":"live-extra-claim","job":observed,
                    "error":claimed.as_ref().err().map(|e|format!("{e:?}")),
                    "commit_started":started.get()}),
                );
                probe.snapshot("claims-zero-claimed").await;
                let claimed = claimed.unwrap();
                assert!(started.get());
                assert_eq!(claimed.state, bigbrotr::management::jobs::JobState::Queued);
                assert_eq!((claimed.revision, claimed.checkpoint_revision), (2, 0));
                let identity = claimed.owner.unwrap();
                assert_eq!(identity.resource, owner.identity().resource_id);
                assert_eq!(identity.epoch, owner.identity().epoch);
                assert_eq!(identity.incarnation, owner.identity().process_incarnation);
            }
        }
        let (after_rows, after_sequence) = raw_state(&probe.path);
        let after_inputs = input_bytes(&input_directory, 5);
        emit(serde_json::json!({"event":"live-extra-intents",
            "claimed_job":jobs[0],"unclaimed_job":jobs[1],"extra_executions":0,
            "native_rows":after_rows.len(),"native_sequence":after_sequence,
            "inputs_before":before_inputs,"inputs_after":after_inputs}));
        assert_eq!((after_rows, after_sequence), (actual, sequence));
        assert_eq!(before_inputs, after_inputs);
    }
    if let Some(total) = mode.limit() {
        return limit_intents(probe, owner, &browser, scope_id, total).await;
    }
    true
}

// Calls in the new count fixture preserve a timeout as an unknown outcome. The
// caller closes native custody and joins the actual driver before asserting.
pub(super) async fn limit_call<T, E: std::fmt::Debug>(
    future: impl Future<Output = Result<T, E>>,
) -> Result<T, String> {
    match tokio::time::timeout(Duration::from_secs(10), future).await {
        Ok(result) => result.map_err(|error| format!("{error:?}")),
        Err(_) => Err("TimeoutOutcomeUnknown".into()),
    }
}

async fn limit_intents(
    probe: &Probe,
    owner: &mut ControlSession,
    browser: &IssuedSession,
    scope_id: AcquisitionScopeId,
    total: u8,
) -> bool {
    // The original a6 job is claim one. Only one new receipt/job is retained at
    // once; fixed-width b0 keys identify the bounded extra claims independently.
    // Snapshot count is constant, avoiding quadratic full-table output.
    assert!(matches!(total, 64 | 65));
    let directory = probe.path.join("producer-inputs-v1");
    let before_inputs = input_bytes(&directory, 5);
    let before_native = raw_state(&probe.path);
    probe.snapshot("limits-before").await;
    let mut created = 0_u8;
    let mut passed = true;
    for ordinal in 2..=total {
        let refreshed = limit_call(owner.refresh(|doc, digest| probe.validate(doc, digest))).await;
        let policy = refreshed.as_ref().ok().map(|policy| {
            serde_json::json!({"config_revision":policy.config_revision,
                "policy_revision":policy.policy_revision,
                "config_digest":hex(&policy.config_digest),
                "policy_digest":hex(&policy.policy_digest)})
        });
        emit(
            serde_json::json!({"event":"live-limit-refresh","ordinal":ordinal,
            "policy":policy,"error":refreshed.as_ref().err()}),
        );
        let Ok(policy) = refreshed else {
            passed = false;
            break;
        };
        if (policy.config_revision, policy.policy_revision) != (1, 1) {
            passed = false;
            break;
        }
        let mut key = [0xb0; 16];
        key[15] = ordinal;
        let started = Cell::new(false);
        let accepted = limit_call(owner.accept_acquisition(
            &browser.session,
            &browser.csrf,
            key.try_into().unwrap(),
            &AcquisitionCommand {
                scope_id,
                deadline_us: JobDuration::new(30_000_000).unwrap(),
                expected_config_revision: Revision::try_from("1".to_owned()).unwrap(),
                expected_policy_revision: Revision::try_from("1".to_owned()).unwrap(),
            },
            &started,
        ))
        .await;
        emit(
            serde_json::json!({"event":"live-limit-acceptance","ordinal":ordinal,
            "request_key":hex(&key),"receipt":accepted.as_ref().ok(),
            "error":accepted.as_ref().err(),"commit_started":started.get()}),
        );
        let Ok(accepted) = accepted else {
            passed = false;
            break;
        };
        if !accepted.created || !started.get() {
            passed = false;
            break;
        }
        let started = Cell::new(false);
        let claimed = limit_call(owner.claim_acquisition(accepted.job_id, &started)).await;
        let observed = claimed.as_ref().ok().map(|job| {
            serde_json::json!({"id":job.id,"scope":job.scope,"state":job.state,
                "job_revision":job.revision,"checkpoint_revision":job.checkpoint_revision,
                "accepted_at_us":job.accepted_at_us,"deadline_at_us":job.deadline_at_us,
                "owner":job.owner.map(|identity|serde_json::json!({"resource":identity.resource,
                    "epoch":identity.epoch.get(),"incarnation":identity.incarnation}))})
        });
        emit(
            serde_json::json!({"event":"live-limit-claim","ordinal":ordinal,
            "job":observed,"error":claimed.as_ref().err(),"commit_started":started.get()}),
        );
        let Ok(claimed) = claimed else {
            passed = false;
            break;
        };
        let correct_owner = claimed.owner.is_some_and(|identity| {
            identity.resource == owner.identity().resource_id
                && identity.epoch == owner.identity().epoch
                && identity.incarnation == owner.identity().process_incarnation
        });
        if !started.get()
            || claimed.id != accepted.job_id
            || claimed.scope != scope_id
            || claimed.state != bigbrotr::management::jobs::JobState::Queued
            || (claimed.revision, claimed.checkpoint_revision) != (2, 0)
            || !correct_owner
        {
            passed = false;
            break;
        }
        created += 1;
    }
    probe.snapshot("limits-after").await;
    let after_native = raw_state(&probe.path);
    let after_inputs = input_bytes(&directory, 5);
    passed &= created == total - 1
        && before_native == after_native
        && (after_native.0.len(), after_native.1) == (53, 64)
        && before_inputs == after_inputs;
    emit(
        serde_json::json!({"event":"live-limit-intents","total_jobs":total,
        "created_claims":created,"extra_executions":0,
        "native_rows":after_native.0.len(),"native_sequence":after_native.1,
        "inputs_before":before_inputs,"inputs_after":after_inputs,"passed":passed}),
    );
    passed
}

async fn zero_claim(probe: &Probe, owner: &mut ControlSession, compacted: bool) -> bool {
    assert!(!compacted || probe.compacted_initial);
    let directory = probe.path.join("producer-inputs-v1");
    let before_native = raw_state(&probe.path);
    let input_count = if compacted { 0 } else { 4 };
    let before_inputs = input_bytes(&directory, input_count);
    probe.snapshot("zero-claim-before").await;
    let result = zero_claim_work(probe, owner, compacted).await;
    // Even a failed or timed-out typed call retains its effects before teardown.
    probe.snapshot("zero-claim-queued").await;
    let after_native = raw_state(&probe.path);
    let after_inputs = input_bytes(&directory, input_count);
    let passed = result.is_ok()
        && (before_native.0.len(), before_native.1) == if compacted { (84, 125) } else { (52, 62) }
        && before_native == after_native
        && before_inputs == after_inputs;
    emit(serde_json::json!({"event":"live-zero-claim-state",
        "error":result.as_ref().err(),"native_rows":after_native.0.len(),
        "native_sequence":after_native.1,"inputs_before":before_inputs,
        "inputs_after":after_inputs,"executions":0,"passed":passed}));
    passed
}

async fn zero_claim_work(
    probe: &Probe,
    owner: &mut ControlSession,
    compacted: bool,
) -> Result<(), String> {
    let scope_id: AcquisitionScopeId = if compacted {
        "422b7d8b33d4b55977806fb711e2d082"
    } else {
        "a39723d7c49af2c99498fb2f59c03b16"
    }
    .parse()
    .unwrap();
    let login = limit_call(owner.begin_browser_login("alice", "127.0.0.1".parse().unwrap())).await;
    emit(
        serde_json::json!({"event":"live-zero-claim-login","phase":"begin",
        "error":login.as_ref().err()}),
    );
    let verified = login?
        .verify(
            b"Independent owner password 2026",
            &mut grant(&probe.budget, HASH_RESERVATION, 0),
        )
        .map_err(|e| format!("{e:?}"));
    emit(
        serde_json::json!({"event":"live-zero-claim-login","phase":"verify",
        "error":verified.as_ref().err()}),
    );
    let browser =
        limit_call(owner.finish_browser_login(verified?, SessionLifetime::default())).await;
    emit(
        serde_json::json!({"event":"live-zero-claim-login","phase":"finish",
        "error":browser.as_ref().err()}),
    );
    let browser = browser?;
    let refreshed = limit_call(owner.refresh(|doc, digest| probe.validate(doc, digest))).await;
    emit(serde_json::json!({"event":"live-zero-claim-refresh",
        "policy":refreshed.as_ref().ok().map(|p|serde_json::json!({
            "config_revision":p.config_revision,"policy_revision":p.policy_revision,
            "config_digest":hex(&p.config_digest),"policy_digest":hex(&p.policy_digest)})),
        "error":refreshed.as_ref().err()}));
    probe.snapshot("zero-claim-refreshed").await;
    let policy = refreshed?;
    if (policy.config_revision, policy.policy_revision) != (1, 1) {
        return Err("unexpected accepted policy".into());
    }
    let started = Cell::new(false);
    let accepted = limit_call(owner.accept_acquisition(
        &browser.session,
        &browser.csrf,
        [0xa6; 16].try_into().unwrap(),
        &AcquisitionCommand {
            scope_id,
            deadline_us: JobDuration::new(30_000_000).unwrap(),
            expected_config_revision: Revision::try_from("1".to_owned()).unwrap(),
            expected_policy_revision: Revision::try_from("1".to_owned()).unwrap(),
        },
        &started,
    ))
    .await;
    emit(
        serde_json::json!({"event":"live-zero-claim-acceptance","request_key":hex(&[0xa6;16]),
        "receipt":accepted.as_ref().ok(),"error":accepted.as_ref().err(),"commit_started":started.get()}),
    );
    probe.snapshot("zero-claim-accepted").await;
    let accepted = accepted?;
    if !started.get() || !accepted.created {
        return Err("new acceptance required".into());
    }
    let started = Cell::new(false);
    let claimed = limit_call(owner.claim_acquisition(accepted.job_id, &started)).await;
    emit(serde_json::json!({"event":"live-zero-claim-claim",
        "job":claimed.as_ref().ok().map(|j|serde_json::json!({"id":j.id,"scope":j.scope,
            "state":j.state,"job_revision":j.revision,"checkpoint_revision":j.checkpoint_revision,
            "accepted_at_us":j.accepted_at_us,"deadline_at_us":j.deadline_at_us,
            "owner":j.owner.map(|i|serde_json::json!({"resource":i.resource,"epoch":i.epoch.get(),
                "incarnation":i.incarnation}))})),"error":claimed.as_ref().err(),"commit_started":started.get()}));
    let claimed = claimed?;
    if !started.get()
        || claimed.id != accepted.job_id
        || claimed.scope != scope_id
        || claimed.state != bigbrotr::management::jobs::JobState::Queued
        || (claimed.revision, claimed.checkpoint_revision) != (2, 0)
        || !claimed.owner.is_some_and(|i| {
            i.resource == owner.identity().resource_id
                && i.epoch == owner.identity().epoch
                && i.incarnation == owner.identity().process_incarnation
        })
    {
        return Err("exact live zero-request claim required".into());
    }
    Ok(())
}

fn input_bytes(directory: &Path, expected: usize) -> Vec<serde_json::Value> {
    let mut inputs: Vec<_> = std::fs::read_dir(directory)
        .unwrap()
        .take(expected + 1)
        .map(|entry| {
            let entry = entry.unwrap();
            assert!(entry.file_type().unwrap().is_file());
            let mut bytes = Vec::new();
            std::fs::File::open(entry.path())
                .unwrap()
                .take(8193)
                .read_to_end(&mut bytes)
                .unwrap();
            assert!(bytes.len() <= 8192);
            serde_json::json!({"name":entry.file_name().to_str().unwrap(),"bytes":hex(&bytes)})
        })
        .collect();
    assert_eq!(inputs.len(), expected);
    inputs.sort_unstable_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
    inputs
}

async fn peer(listener: &TcpListener) {
    let (mut socket, _) = listener.accept().await.unwrap();
    let mut request = Vec::new();
    while !request.ends_with(b"\r\n\r\n") {
        assert!(request.len() < 4096);
        request.push(socket.read_u8().await.unwrap());
    }
    let request = std::str::from_utf8(&request).unwrap();
    assert!(request.starts_with("GET /installed-fixture HTTP/1.1\r\n"));
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
    socket.write_all(format!("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n").as_bytes()).await.unwrap();
    let request = command(&mut socket).await;
    assert_eq!(request.as_array().unwrap().len(), 3);
    assert_eq!(request[0], "REQ");
    assert_eq!(request[2], serde_json::json!({"kinds":[1],"limit":1}));
    let id = request[1].as_str().unwrap();
    frame(
        &mut socket,
        &format!(r#"["EVENT","{id}",{}]"#, EVENT.trim()),
    )
    .await;
    frame(&mut socket, &format!(r#"["EOSE","{id}"]"#)).await;
    emit(
        serde_json::json!({"event":"live-durable-wire","request":request,
        "signed_event":serde_json::from_str::<serde_json::Value>(EVENT).unwrap(),"events":1,"eose":true}),
    );
    assert_eq!(command(&mut socket).await, serde_json::json!(["CLOSE", id]));
    let mut close = [0; 6];
    socket.read_exact(&mut close).await.unwrap();
    assert_eq!(&close[..2], &[0x88, 0x80]);
    socket.write_all(&[0x88, 0]).await.unwrap();
    socket.shutdown().await.unwrap();
    assert_eq!(socket.read(&mut [0; 1]).await.unwrap(), 0);
    emit(serde_json::json!({"event":"live-durable-peer-closed","eof":true}));
}

async fn command(socket: &mut TcpStream) -> serde_json::Value {
    assert_eq!(socket.read_u8().await.unwrap(), 0x81);
    let flags = socket.read_u8().await.unwrap();
    assert_ne!(flags & 0x80, 0);
    let count = match flags & 0x7f {
        126 => usize::from(socket.read_u16().await.unwrap()),
        127 => panic!("oversized fixture command"),
        value => usize::from(value),
    };
    assert!(count < 4096);
    let mut mask = [0; 4];
    socket.read_exact(&mut mask).await.unwrap();
    let mut bytes = vec![0; count];
    socket.read_exact(&mut bytes).await.unwrap();
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte ^= mask[index % 4];
    }
    serde_json::from_slice(&bytes).unwrap()
}

async fn frame(socket: &mut TcpStream, text: &str) {
    assert!(text.len() < 4096);
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
