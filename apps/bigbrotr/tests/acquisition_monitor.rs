//! Independent literal WS peers; no PG grant or event-admission result is mocked.
#[path = "network_websocket/support.rs"]
#[allow(dead_code)]
mod support;

use bigbrotr::{
    acquisition::monitor::{AttemptSpec, CaptureError, PreparedPassiveRead},
    network::{TransportError, TransportSnapshot},
};
use bigbrotr_model::{
    ClockQuality, DictionaryKind, DurationUs, EventId, Ordinal, QualifiedRef, measurement::*,
};
use bigbrotr_protocol::endpoint::Endpoint;
use serde_json::{Value, json};
use std::{
    io,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::timeout,
};

fn spec() -> AttemptSpec {
    let reference = |kind, ordinal| QualifiedRef {
        dataset_id: [1; 16].try_into().unwrap(),
        dictionary_kind: kind,
        allocation_domain: [2; 16].try_into().unwrap(),
        ordinal: Ordinal::new(ordinal).unwrap(),
    };
    AttemptSpec {
        attempt_id: [3; 16].try_into().unwrap(),
        context: MeasurementContext {
            subject: MeasurementSubject::RelayEndpoint(reference(DictionaryKind::Endpoint, 1)),
            observer_id: reference(DictionaryKind::Observer, 1),
            route_id: reference(DictionaryKind::Route, 1),
            access_scope_id: reference(DictionaryKind::AccessScope, 1),
            method_id: reference(DictionaryKind::Method, 1),
        },
        clock_quality: ClockQuality::new(
            reference(DictionaryKind::Observer, 2),
            DurationUs::new(1),
            None,
            0,
        )
        .unwrap(),
        effective_authority_us: DurationUs::new(5_000_000),
    }
}
fn corpus(name: &str) -> Value {
    let corpus: Value = serde_json::from_str(include_str!(
        "../../../experiments/identity-contract/signed-fixture.json"
    ))
    .unwrap();
    corpus["events"][name].clone()
}
fn frame(text: &str) -> Vec<u8> {
    let mut bytes = vec![0x81];
    match text.len() {
        0..=125 => bytes.push(text.len() as u8),
        126..=65535 => {
            bytes.push(126);
            bytes.extend((text.len() as u16).to_be_bytes());
        }
        _ => {
            bytes.push(127);
            bytes.extend((text.len() as u64).to_be_bytes());
        }
    }
    bytes.extend(text.as_bytes());
    bytes
}
async fn client_frame(stream: &mut TcpStream) -> (u8, Vec<u8>) {
    let first = stream.read_u8().await.unwrap();
    let second = stream.read_u8().await.unwrap();
    assert_ne!(second & 0x80, 0, "client frames are masked");
    let length = match second & 127 {
        126 => usize::from(stream.read_u16().await.unwrap()),
        127 => panic!("unexpected large passive command"),
        length => usize::from(length),
    };
    assert!(length <= 4096);
    let mut mask = [0; 4];
    stream.read_exact(&mut mask).await.unwrap();
    let mut bytes = vec![0; length];
    stream.read_exact(&mut bytes).await.unwrap();
    for (i, byte) in bytes.iter_mut().enumerate() {
        *byte ^= mask[i % 4];
    }
    (first, bytes)
}
async fn request(stream: &mut TcpStream) -> String {
    let (opcode, bytes) = client_frame(stream).await;
    assert_eq!(opcode, 0x81);
    let value: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(value.as_array().unwrap().len(), 3);
    assert_eq!(value[0], "REQ");
    assert_eq!(value[2], json!({"kinds":[1],"limit":1}));
    let id = value[1].as_str().unwrap();
    assert_eq!(id.len(), 48);
    id.to_owned()
}
async fn eof(stream: &mut TcpStream) {
    match stream.read(&mut [0; 1]).await {
        Ok(0) => {}
        Err(error) if error.kind() == io::ErrorKind::ConnectionReset => {}
        result => panic!("expected owned socket closure without another command: {result:?}"),
    }
}
async fn cleanup(stream: &mut TcpStream, id: &str, nip_close: bool) {
    if nip_close {
        let (opcode, bytes) = client_frame(stream).await;
        assert_eq!(opcode, 0x81);
        assert_eq!(
            serde_json::from_slice::<Value>(&bytes).unwrap(),
            json!(["CLOSE", id])
        );
    }
    // No AUTH response, publication or second REQ is permitted.
    assert_eq!(client_frame(stream).await, (0x88, vec![]));
    match stream.write_all(&[0x88, 0]).await {
        Ok(()) => {
            stream.shutdown().await.unwrap();
        }
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::BrokenPipe | io::ErrorKind::ConnectionReset
            ) => {}
        Err(error) => panic!("fixture close reply failed: {error}"),
    }
    eof(stream).await;
}
#[derive(Clone, Copy)]
enum Script {
    EventEose,
    EmptyEose,
    Closed,
    Forged,
    WrongKind,
    ValidThenInvalid,
    CandidateCap,
    NormalizedCap,
    RxClaim,
}

async fn exchange(
    script: Script,
) -> (
    TerminalMeasurement,
    TransportSnapshot,
    Result<(), TransportError>,
) {
    exchange_finishing(script, false).await
}

async fn exchange_finishing(
    script: Script,
    unclassified: bool,
) -> (
    TerminalMeasurement,
    TransportSnapshot,
    Result<(), TransportError>,
) {
    let support::Connection {
        listener,
        resources,
        connector,
        attempt,
        cancel: _cancel,
    } = support::connection().await;
    let address = listener.local_addr().unwrap();
    let endpoint = Endpoint::parse(&format!("ws://{address}/events?order=1&order=2")).unwrap();
    let prepared = PreparedPassiveRead::prepare(&resources, &endpoint, spec()).unwrap();
    let connector = prepared.connector(&connector);
    let peer = async {
        let (mut stream, _) = listener.accept().await.unwrap();
        let response = support::handshake(&mut stream, address).await;
        stream.write_all(response.as_bytes()).await.unwrap();
        let id = request(&mut stream).await;
        let valid = serde_json::to_string_pretty(&corpus("E_private")).unwrap();
        let mut invalid = corpus("E_private");
        invalid["sig"] = json!("00".repeat(64));
        let invalid = serde_json::to_string(&invalid).unwrap();
        let event = |subscription: &str, value: &str| {
            frame(&format!("[\"EVENT\",\"{subscription}\",{value}]"))
        };
        let eose = || frame(&format!("[\"EOSE\",\"{id}\"]"));
        let frames = match script {
            Script::EventEose => [
                event(&id, &valid),
                frame("[\"EOSE\",\"unrelated\"]"),
                frame("[\"AUTH\",\"never-retained\"]"),
                eose(),
            ]
            .concat(),
            Script::EmptyEose => [
                frame("[\"AUTH\",\"never-retained\"]"),
                frame("[\"EOSE\",\"unrelated\"]"),
                eose(),
            ]
            .concat(),
            Script::Closed => [
                frame("[\"CLOSED\",\"unrelated\",\"stale\"]"),
                frame("[\"AUTH\",\"never-retained\"]"),
                frame(&format!("[\"CLOSED\",\"{id}\",\"scoped-refusal\"]")),
            ]
            .concat(),
            Script::Forged => [event(&id, &invalid), eose()].concat(),
            Script::WrongKind => [
                event(&id, &serde_json::to_string(&corpus("A_old")).unwrap()),
                eose(),
            ]
            .concat(),
            Script::ValidThenInvalid => [event(&id, &valid), event(&id, &invalid), eose()].concat(),
            Script::CandidateCap => [
                event("unrelated", "{}"),
                event("unrelated", "{}"),
                event("unrelated", "{}"),
                event("unrelated", "{}"),
            ]
            .concat(),
            Script::NormalizedCap => {
                let mut oversized = corpus("E_private");
                oversized["content"] = json!("x".repeat(300_000));
                event(&id, &serde_json::to_string(&oversized).unwrap())
            }
            Script::RxClaim => {
                let mut claim = vec![0x81, 127];
                claim.extend((1u64 << 40).to_be_bytes());
                claim.extend([0; 128]); // Already read tail, never consumed payload.
                claim
            }
        };
        stream.write_all(&frames).await.unwrap();
        if matches!(script, Script::RxClaim) {
            eof(&mut stream).await;
        } else {
            cleanup(&mut stream, &id, !matches!(script, Script::Closed)).await;
        }
    };
    let consumer = connector.run_observed(attempt, prepared.transport_trace(), async |stream| {
        prepared.read(stream).await
    });
    let (result, ()) = timeout(Duration::from_secs(4), async {
        tokio::join!(consumer, peer)
    })
    .await
    .unwrap();
    let snapshot = prepared.transport_trace().snapshot();
    let captured = (if unclassified {
        prepared.finish_unclassified()
    } else {
        prepared.finish(result)
    })
    .unwrap()
    .expect("actual returned connect syscall");
    let MeasurementState::Terminal(terminal) = captured.measurement().state() else {
        panic!("not terminal")
    };
    assert_eq!(
        captured.measurement().profile().effective_authority_us(),
        DurationUs::new(5_000_000)
    );
    assert_eq!(
        terminal.observed.counters.rx_ws_bytes,
        snapshot.websocket.rx_ws_framed_bytes_read
    );
    assert_eq!(
        terminal.observed.counters.rx_payload_bytes,
        snapshot.websocket.rx_ws_payload_bytes_consumed
    );
    assert_eq!(
        terminal.observed.counters.tx_ws_bytes,
        snapshot.websocket.tx_ws_framed_bytes_written
    );
    assert_eq!(snapshot.relay_connect_calls, 1);
    assert_eq!(snapshot.resolver_connect_calls, 0);
    drop(captured);
    assert_eq!(resources.snapshot().active_permits(), 0);
    (terminal, snapshot, result)
}

#[tokio::test]
async fn signed_passive_read_keeps_original_decoder_anchor_and_exact_request() {
    let (terminal, snapshot, result) = exchange(Script::EventEose).await;
    result.unwrap();
    assert_eq!(terminal.termination, MeasurementTermination::Eose);
    assert_eq!(terminal.teardown, TeardownObservation::Completed);
    assert!(terminal.observed.read.eose_prefix_verified);
    assert!(terminal.observed.read.auth_observed);
    let first = terminal.observed.read.first_valid_event.unwrap();
    assert_eq!(
        first.event_id,
        "a669974f5d6f76e4668b0be874435f14be799c04f0e70e5ec3d0967f1babf6bb"
            .parse::<EventId>()
            .unwrap()
    );
    let delivered = snapshot.first_message_delivered.unwrap();
    assert_eq!(
        first.received.observed_at.map(|at| at.get()),
        delivered.unix_us
    );
    assert_eq!(
        first.received.elapsed_us.unwrap().get() as u128,
        (delivered.monotonic - snapshot.first_io.unwrap().returned_at.monotonic).as_micros()
    );
    assert!(first.verified.elapsed_us >= first.received.elapsed_us);
    assert!(first.verified.elapsed_us <= terminal.observed.read.eose.unwrap().elapsed_us);
    assert_eq!(terminal.observed.counters.event_candidates, 1);
}

#[tokio::test]
async fn empty_eose_and_only_correlated_closed_remain_distinct_without_fake_events() {
    for script in [Script::EmptyEose, Script::Closed] {
        let (terminal, _, result) = exchange(script).await;
        result.unwrap();
        assert!(terminal.observed.read.first_valid_event.is_none());
        assert_eq!(terminal.observed.counters.event_candidates, 0);
        assert!(terminal.observed.read.auth_observed);
        if matches!(script, Script::Closed) {
            assert_eq!(terminal.termination, MeasurementTermination::Closed);
            assert!(terminal.observed.read.correlated_closed);
            assert!(terminal.observed.read.eose.is_none());
            assert!(!terminal.observed.read.eose_prefix_verified);
        } else {
            assert_eq!(terminal.termination, MeasurementTermination::Eose);
            assert!(!terminal.observed.read.correlated_closed);
            assert!(terminal.observed.read.eose_prefix_verified);
        }
    }
}

#[tokio::test]
async fn invalid_signature_and_wrong_signed_kind_cannot_become_a_positive_eose() {
    for script in [Script::Forged, Script::WrongKind, Script::ValidThenInvalid] {
        let (terminal, _, result) = exchange(script).await;
        assert!(result.is_err());
        assert_eq!(
            terminal.termination,
            MeasurementTermination::Failed {
                phase: MeasurementPhase::Verification,
                class: MeasurementFailure::InvalidEvent,
            }
        );
        assert!(terminal.observed.read.eose.is_none());
        assert!(!terminal.observed.read.eose_prefix_verified);
        assert_eq!(
            terminal.observed.read.first_valid_event.is_some(),
            matches!(script, Script::ValidThenInvalid)
        );
    }
}

#[tokio::test]
async fn unrelated_events_and_normalized_size_have_their_own_fixed_limits() {
    for (script, expected) in [
        (Script::CandidateCap, MeasurementLimit::EventCandidates),
        (
            Script::NormalizedCap,
            MeasurementLimit::NormalizedEventBytes,
        ),
    ] {
        let (terminal, _, result) = exchange(script).await;
        assert!(result.is_err());
        assert_eq!(
            terminal.termination,
            MeasurementTermination::Limit(expected)
        );
        assert!(terminal.observed.read.first_valid_event.is_none());
        assert_eq!(
            terminal.observed.counters.event_candidates,
            if matches!(script, Script::CandidateCap) {
                4
            } else {
                1
            }
        );
    }
}

#[tokio::test]
async fn rejected_claim_and_unused_tail_map_actual_reads_without_inventing_cap_reception() {
    let (terminal, trace, result) = exchange(Script::RxClaim).await;
    assert!(result.is_err());
    assert_eq!(
        terminal.termination,
        MeasurementTermination::Limit(MeasurementLimit::RxWsBytes)
    );
    assert_eq!(trace.websocket.rx_ws_framed_bytes_consumed, 10);
    assert!(trace.websocket.rx_ws_framed_bytes_read > trace.websocket.rx_ws_framed_bytes_consumed);
    assert_eq!(terminal.observed.counters.rx_payload_bytes, 0);
    assert!(terminal.observed.counters.rx_ws_bytes < PassiveReadProfile::MAX_RX_WS_BYTES);
}

#[tokio::test]
async fn first_valid_event_survives_actual_cancellation_or_owner_authority_loss() {
    for authority in [false, true] {
        let support::Connection {
            listener,
            resources,
            connector,
            attempt,
            cancel,
        } = support::connection().await;
        let address = listener.local_addr().unwrap();
        let endpoint = Endpoint::parse(&format!("ws://{address}/events?order=1&order=2")).unwrap();
        let prepared = PreparedPassiveRead::prepare(&resources, &endpoint, spec()).unwrap();
        let connector = prepared.connector(&connector);
        let owner = AtomicBool::new(true);
        let live = || owner.load(Ordering::Acquire);
        let attempt = attempt.with_liveness_check(&live);
        let peer = async {
            let (mut stream, _) = listener.accept().await.unwrap();
            let response = support::handshake(&mut stream, address).await;
            stream.write_all(response.as_bytes()).await.unwrap();
            let id = request(&mut stream).await;
            stream
                .write_all(&frame(&format!(
                    "[\"EVENT\",\"{id}\",{}]",
                    corpus("E_private")
                )))
                .await
                .unwrap();
            eof(&mut stream).await;
        };
        let interrupt = async {
            while prepared
                .transport_trace()
                .snapshot()
                .first_message_delivered
                .is_none()
            {
                tokio::task::yield_now().await;
            }
            // Text delivery, parsing and sequential verification run in one
            // poll until the driver's explicit post-verification yield.
            if authority {
                owner.store(false, Ordering::Release);
            } else {
                drop(cancel);
                return;
            }
            // Keep the unique cancel handle alive through this authority case.
            tokio::time::sleep(Duration::from_millis(100)).await;
            drop(cancel);
        };
        let consumer =
            connector.run_observed(attempt, prepared.transport_trace(), async |stream| {
                prepared.read(stream).await
            });
        let (result, (), ()) = timeout(Duration::from_secs(3), async {
            tokio::join!(consumer, peer, interrupt)
        })
        .await
        .unwrap();
        assert_eq!(
            result,
            Err(if authority {
                TransportError::Authority
            } else {
                TransportError::Cancelled
            })
        );
        let captured = prepared.finish(result).unwrap().unwrap();
        let MeasurementState::Terminal(terminal) = captured.measurement().state() else {
            panic!()
        };
        assert_eq!(
            terminal.termination,
            if authority {
                MeasurementTermination::AuthorityInterruption(MeasurementPhase::ResponseRead)
            } else {
                MeasurementTermination::Cancelled(MeasurementPhase::ResponseRead)
            }
        );
        assert!(terminal.observed.read.first_valid_event.is_some());
        assert!(terminal.observed.read.eose.is_none());
        assert_eq!(terminal.observed.counters.event_candidates, 1);
        drop(captured);
        assert_eq!(resources.snapshot().active_permits(), 0);
    }
}

#[tokio::test]
async fn an_earlier_io_deadline_is_distinct_from_the_five_second_authority_ceiling() {
    use bigbrotr::{
        network::{DirectAttempt, DirectPolicy, PrivateRule, Target},
        runtime::policy_authority::{LeaseResponse, PolicyLeaseTracker, PolicyLeaseTtl},
    };
    use bigbrotr_model::{DatasetId, PolicyEpoch, ProcessIncarnation};
    let support::Connection {
        listener,
        resources,
        connector,
        attempt: unused,
        cancel: _unused_cancel,
    } = support::connection().await;
    drop(unused);
    let address = listener.local_addr().unwrap();
    let endpoint = Endpoint::parse(&format!("ws://{address}/events?order=1&order=2")).unwrap();
    let prepared = PreparedPassiveRead::prepare(&resources, &endpoint, spec()).unwrap();
    let connector = prepared.connector(&connector);
    let dataset = DatasetId::try_from([1; 16]).unwrap();
    let process_incarnation = ProcessIncarnation::try_from([2; 16]).unwrap();
    let policy_epoch = PolicyEpoch::new(1);
    let ttl = Duration::from_secs(5);
    let mut tracker = PolicyLeaseTracker::new(
        dataset,
        process_incarnation,
        policy_epoch,
        PolicyLeaseTtl::new(ttl).unwrap(),
    )
    .unwrap();
    let ticket = tracker.begin_refresh().unwrap();
    tracker
        .complete_refresh(
            &ticket,
            LeaseResponse {
                dataset,
                process_incarnation,
                policy_epoch,
                ttl,
            },
        )
        .unwrap();
    let window = tracker
        .begin_action(policy_epoch)
        .unwrap()
        .complete(ttl)
        .unwrap();
    let policy = DirectPolicy::private(vec![
        PrivateRule::endpoint(endpoint.clone(), "127.0.0.1/32", address.port()).unwrap(),
    ])
    .unwrap();
    let (attempt, _cancel) = DirectAttempt::new(
        Target::new(endpoint).unwrap(),
        policy,
        window,
        tokio::time::Instant::now() + Duration::from_secs(2),
    );
    let peer = async {
        let (mut stream, _) = listener.accept().await.unwrap();
        let response = support::handshake(&mut stream, address).await;
        stream.write_all(response.as_bytes()).await.unwrap();
        let id = request(&mut stream).await;
        stream
            .write_all(&frame(&format!(
                "[\"EVENT\",\"{id}\",{}]",
                corpus("E_private")
            )))
            .await
            .unwrap();
        eof(&mut stream).await;
    };
    let consumer = connector.run_observed(attempt, prepared.transport_trace(), async |stream| {
        prepared.read(stream).await
    });
    let (result, ()) = timeout(Duration::from_secs(4), async {
        tokio::join!(consumer, peer)
    })
    .await
    .unwrap();
    assert_eq!(result, Err(TransportError::Deadline));
    let captured = prepared.finish(result).unwrap().unwrap();
    let MeasurementState::Terminal(terminal) = captured.measurement().state() else {
        panic!()
    };
    assert_eq!(
        terminal.termination,
        MeasurementTermination::Deadline(MeasurementPhase::ResponseRead)
    );
    assert!(terminal.observed.read.first_valid_event.is_some());
    assert!(terminal.observed.read.eose.is_none());
    assert_eq!(
        captured.measurement().profile().effective_authority_us(),
        DurationUs::new(5_000_000)
    );
    drop(captured);
    assert_eq!(resources.snapshot().active_permits(), 0);
}

#[tokio::test]
async fn eose_observed_before_cleanup_survives_later_socket_cancellation() {
    let support::Connection {
        listener,
        resources,
        connector,
        attempt,
        cancel,
    } = support::connection().await;
    let address = listener.local_addr().unwrap();
    let endpoint = Endpoint::parse(&format!("ws://{address}/events?order=1&order=2")).unwrap();
    let prepared = PreparedPassiveRead::prepare(&resources, &endpoint, spec()).unwrap();
    let connector = prepared.connector(&connector);
    let (closing, closed) = tokio::sync::oneshot::channel();
    let peer = async {
        let (mut stream, _) = listener.accept().await.unwrap();
        let response = support::handshake(&mut stream, address).await;
        stream.write_all(response.as_bytes()).await.unwrap();
        let id = request(&mut stream).await;
        stream
            .write_all(&frame(&format!("[\"EOSE\",\"{id}\"]")))
            .await
            .unwrap();
        let (opcode, bytes) = client_frame(&mut stream).await;
        assert_eq!(opcode, 0x81);
        assert_eq!(
            serde_json::from_slice::<Value>(&bytes).unwrap(),
            json!(["CLOSE", id])
        );
        assert_eq!(client_frame(&mut stream).await, (0x88, vec![]));
        closing.send(()).unwrap();
        eof(&mut stream).await;
    };
    let interrupt = async {
        closed.await.unwrap();
        drop(cancel);
    };
    let consumer = connector.run_observed(attempt, prepared.transport_trace(), async |stream| {
        prepared.read(stream).await
    });
    let (result, (), ()) = timeout(Duration::from_secs(3), async {
        tokio::join!(consumer, peer, interrupt)
    })
    .await
    .unwrap();
    assert_eq!(result, Err(TransportError::Cancelled));
    let captured = prepared.finish(result).unwrap().unwrap();
    let MeasurementState::Terminal(terminal) = captured.measurement().state() else {
        panic!()
    };
    assert_eq!(terminal.termination, MeasurementTermination::Eose);
    assert_eq!(terminal.teardown, TeardownObservation::Interrupted);
    assert!(terminal.observed.read.eose_prefix_verified);
    assert!(terminal.observed.read.first_valid_event.is_none());
    drop(captured);
    assert_eq!(resources.snapshot().active_permits(), 0);
}

#[tokio::test]
async fn cancellation_before_actual_connect_returns_no_measurement_and_opens_no_socket() {
    let support::Connection {
        listener,
        resources,
        connector,
        attempt,
        cancel,
    } = support::connection().await;
    let address = listener.local_addr().unwrap();
    let endpoint = Endpoint::parse(&format!("ws://{address}/events?order=1&order=2")).unwrap();
    let prepared = PreparedPassiveRead::prepare(&resources, &endpoint, spec()).unwrap();
    let connector = prepared.connector(&connector);
    drop(cancel);
    let result = connector
        .run_observed(attempt, prepared.transport_trace(), async |stream| {
            prepared.read(stream).await
        })
        .await;
    assert_eq!(result, Err(TransportError::Cancelled));
    assert!(prepared.transport_trace().snapshot().first_io.is_none());
    assert!(prepared.finish(result).unwrap().is_none());
    // A bounded accept remains pending; no ordinary network retry was started.
    assert!(
        timeout(Duration::from_millis(20), listener.accept())
            .await
            .is_err()
    );
    assert_eq!(resources.snapshot().active_permits(), 0);
}

#[tokio::test]
async fn a_second_capture_cannot_consume_another_attempts_observed_stream() {
    use bigbrotr::runtime::{ProcessEnvelope, ResourceBudget, ResourceLimits, WaiterLimits};
    let support::Connection {
        listener,
        resources,
        connector,
        attempt,
        cancel: _cancel,
    } = support::connection().await;
    let address = listener.local_addr().unwrap();
    let endpoint = Endpoint::parse(&format!("ws://{address}/events?order=1&order=2")).unwrap();
    let prepared = PreparedPassiveRead::prepare(&resources, &endpoint, spec()).unwrap();
    let other_parent = ResourceBudget::new(
        ProcessEnvelope::new(
            64 * 1024 * 1024,
            0,
            ResourceLimits::new(64 * 1024 * 1024, 8, 0, 0),
            WaiterLimits::new(0, 0),
        )
        .unwrap(),
    );
    let other = PreparedPassiveRead::prepare(&other_parent, &endpoint, spec()).unwrap();
    let connector = prepared.connector(&connector);
    let peer = async {
        let (mut stream, _) = listener.accept().await.unwrap();
        eof(&mut stream).await;
    };
    let consumer = connector.run_observed(attempt, prepared.transport_trace(), async |stream| {
        other.read(stream).await
    });
    let (result, ()) = timeout(Duration::from_secs(3), async {
        tokio::join!(consumer, peer)
    })
    .await
    .unwrap();
    assert_eq!(result, Err(TransportError::Operation));
    assert!(other.transport_trace().snapshot().first_io.is_none());
    assert!(matches!(other.finish(result), Err(CaptureError::Binding)));
    drop(prepared);
    assert_eq!(resources.snapshot().active_permits(), 0);
    assert_eq!(other_parent.snapshot().active_permits(), 0);
}

#[tokio::test]
async fn unclassified_outer_refusal_preserves_known_terminal_but_never_invents_a_cause() {
    // Component finalization boundary only; this does not claim an exercised PG
    // failure. The independently observed EOSE survives an unclassified caller.
    let (terminal, _, result) = exchange_finishing(Script::EmptyEose, true).await;
    result.unwrap();
    assert_eq!(terminal.termination, MeasurementTermination::Eose);
    assert_eq!(terminal.teardown, TeardownObservation::Completed);
    let support::Connection {
        listener,
        resources,
        connector,
        attempt,
        cancel: _cancel,
    } = support::connection().await;
    let address = listener.local_addr().unwrap();
    let endpoint = Endpoint::parse(&format!("ws://{address}/events?order=1&order=2")).unwrap();
    let prepared = PreparedPassiveRead::prepare(&resources, &endpoint, spec()).unwrap();
    let connector = prepared.connector(&connector);
    let peer = async {
        let (mut stream, _) = listener.accept().await.unwrap();
        eof(&mut stream).await;
    };
    // Actual external connect returned, but no passive protocol terminal exists.
    let consumer = connector.run_observed(attempt, prepared.transport_trace(), async |_stream| {
        Err::<(), _>(io::Error::other("unclassified local caller refusal"))
    });
    let (result, ()) = timeout(Duration::from_secs(3), async {
        tokio::join!(consumer, peer)
    })
    .await
    .unwrap();
    assert_eq!(result, Err(TransportError::Operation));
    assert!(prepared.transport_trace().snapshot().first_io.is_some());
    assert!(prepared.transport_trace().snapshot().stop.is_none());
    assert!(matches!(
        prepared.finish_unclassified(),
        Err(CaptureError::TerminalUnavailable)
    ));
    assert_eq!(resources.snapshot().active_permits(), 0);
}
