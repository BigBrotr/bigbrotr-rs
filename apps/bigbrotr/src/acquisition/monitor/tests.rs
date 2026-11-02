use super::*;
use crate::{
    network::*,
    runtime::{ProcessEnvelope, WaiterLimits},
};
use bigbrotr_model::{DictionaryKind, Ordinal, QualifiedRef, measurement::*};
use std::time::Duration;
use tokio::time::Instant;

fn reference(kind: DictionaryKind, ordinal: u64) -> QualifiedRef {
    QualifiedRef {
        dataset_id: [1; 16].try_into().unwrap(),
        dictionary_kind: kind,
        allocation_domain: [2; 16].try_into().unwrap(),
        ordinal: Ordinal::new(ordinal).unwrap(),
    }
}
fn spec(authority: u64) -> AttemptSpec {
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
        effective_authority_us: DurationUs::new(authority),
    }
}
fn budget(bytes: u64) -> ResourceBudget {
    ResourceBudget::new(
        ProcessEnvelope::new(
            bytes,
            0,
            ResourceLimits::new(bytes, PreparedPassiveRead::REQUIRED_ITEMS, 0, 0),
            WaiterLimits::new(0, 0),
        )
        .unwrap(),
    )
}
fn template() -> Measurement {
    let spec = spec(4_000_000);
    Measurement::new(
        spec.attempt_id,
        spec.context,
        PassiveReadProfile::first(spec.effective_authority_us).unwrap(),
        spec.clock_quality,
        MeasurementState::ExecutionUnknown,
    )
    .unwrap()
}
fn terminal(value: &Measurement) -> TerminalMeasurement {
    let MeasurementState::Terminal(terminal) = value.state() else {
        panic!("not terminal")
    };
    terminal
}

#[test]
fn parent_capacity_and_profile_are_checked_before_any_attempt_or_trace_io() {
    let endpoint = Endpoint::parse("wss://relay.example/").unwrap();
    let parent = budget(PreparedPassiveRead::REQUIRED_DATA_BYTES - 1);
    assert!(matches!(
        PreparedPassiveRead::prepare(&parent, &endpoint, spec(4_000_000)),
        Err(CaptureError::Budget)
    ));
    assert_eq!(parent.snapshot().active_permits(), 0);
    let parent = budget(PreparedPassiveRead::REQUIRED_DATA_BYTES);
    for authority in [0, 5_000_001] {
        assert!(matches!(
            PreparedPassiveRead::prepare(&parent, &endpoint, spec(authority)),
            Err(CaptureError::Profile)
        ));
        assert_eq!(parent.snapshot().active_permits(), 0);
    }
    let mut invalid = spec(4_000_000);
    invalid.context.route_id = reference(DictionaryKind::Observer, 1);
    assert!(matches!(
        PreparedPassiveRead::prepare(&parent, &endpoint, invalid),
        Err(CaptureError::Profile)
    ));
    assert_eq!(parent.snapshot().active_permits(), 0);
    let prepared = PreparedPassiveRead::prepare(&parent, &endpoint, spec(4_000_000)).unwrap();
    assert_eq!(
        parent.snapshot().used().data_bytes(),
        PreparedPassiveRead::REQUIRED_DATA_BYTES
    );
    assert_eq!(
        prepared.template.profile().effective_authority_us(),
        DurationUs::new(4_000_000)
    );
    assert!(prepared.trace.snapshot().first_io.is_none());
    assert!(
        prepared
            .finish(Err(TransportError::Authority))
            .unwrap()
            .is_none()
    );
    assert_eq!(parent.snapshot().active_permits(), 0);
}

#[test]
fn trace_binding_uses_allocation_identity_and_no_pre_io_failure_becomes_a_sample() {
    let endpoint = Endpoint::parse("ws://127.0.0.1/").unwrap();
    for error in [
        TransportError::Authority,
        TransportError::Cancelled,
        TransportError::Budget,
        TransportError::Connect,
        TransportError::Deadline,
    ] {
        let parent = budget(PreparedPassiveRead::REQUIRED_DATA_BYTES);
        let prepared = PreparedPassiveRead::prepare(&parent, &endpoint, spec(5_000_000)).unwrap();
        assert!(prepared.trace.same_capture(prepared.transport_trace()));
        assert!(!prepared.trace.same_capture(&TransportTrace::new()));
        assert!(prepared.finish(Err(error)).unwrap().is_none());
        assert_eq!(parent.snapshot().active_permits(), 0);
    }
}

#[test]
fn mapper_keeps_actual_read_bytes_distinct_from_consumed_payload_and_potential_lengths() {
    // Synthetic mapping oracle, not runtime start evidence. A refused physical
    // header and its unread buffered tail discriminate read from consumed bytes.
    let first = TransportInstant {
        monotonic: Instant::now(),
        unix_us: Some(0),
    };
    let trace = TransportSnapshot {
        first_io: Some(FirstIo {
            target: FirstIoTarget::RelayConnect,
            returned_at: first,
            submission: ConnectSubmission::InProgress,
        }),
        websocket: WebSocketCounters {
            rx_physical_frames_started: 1,
            rx_complete_headers: 1,
            rx_last_declared_payload_bytes: Some(1 << 40),
            rx_last_header_bytes: Some(10),
            rx_declared_ws_framed_bytes: Some((1 << 40) + 10),
            rx_ws_framed_bytes_read: 137,
            rx_ws_framed_bytes_consumed: 10,
            rx_ws_payload_bytes_consumed: 0,
            rx_ws_framed_bytes_forwarded: 0,
            ..WebSocketCounters::default()
        },
        ..TransportSnapshot::default()
    };
    let state = State {
        terminal: Some((
            MeasurementTermination::Limit(MeasurementLimit::RxWsBytes),
            TransportInstant {
                monotonic: first.monotonic + Duration::from_micros(2),
                unix_us: None,
            },
        )),
        ..State::default()
    };
    let value = liveness::measurement(&template(), &trace, &state).unwrap();
    let terminal = terminal(&value);
    assert_eq!(terminal.observed.counters.rx_ws_bytes, 137);
    assert_eq!(terminal.observed.counters.rx_payload_bytes, 0);
    assert_ne!(
        terminal.observed.counters.rx_ws_bytes,
        trace.websocket.rx_ws_framed_bytes_consumed
    );
    assert_eq!(
        terminal.termination,
        MeasurementTermination::Limit(MeasurementLimit::RxWsBytes)
    );
    assert!(terminal.observed.counters.rx_ws_bytes < PassiveReadProfile::MAX_RX_WS_BYTES);
    assert_eq!(terminal.ended.observed_at, None);
}

#[test]
fn returned_witness_roles_and_classes_never_invent_a_completed_open_phase() {
    // Synthetic mapper inputs only. Production snapshots can acquire this
    // witness solely inside the Network connect-return hook.
    let at = TransportInstant {
        monotonic: Instant::now(),
        unix_us: Some(0),
    };
    for (target, destination) in [
        (
            FirstIoTarget::ResolverConnect,
            ConnectDestination::DnsResolver,
        ),
        (FirstIoTarget::RelayConnect, ConnectDestination::Relay),
    ] {
        for (submission, expected) in [
            (ConnectSubmission::Connected, ConnectCallResult::Accepted),
            (ConnectSubmission::InProgress, ConnectCallResult::Accepted),
            (
                ConnectSubmission::ImmediateError(crate::network::ConnectErrorClass::Denied),
                ConnectCallResult::ImmediateError(
                    bigbrotr_model::measurement::ConnectErrorClass::Permission,
                ),
            ),
            (
                ConnectSubmission::ImmediateError(crate::network::ConnectErrorClass::Resource),
                ConnectCallResult::ImmediateError(
                    bigbrotr_model::measurement::ConnectErrorClass::Resources,
                ),
            ),
            (
                ConnectSubmission::ImmediateError(
                    crate::network::ConnectErrorClass::AddressUnavailable,
                ),
                ConnectCallResult::ImmediateError(
                    bigbrotr_model::measurement::ConnectErrorClass::Address,
                ),
            ),
            (
                ConnectSubmission::ImmediateError(crate::network::ConnectErrorClass::Refused),
                ConnectCallResult::ImmediateError(
                    bigbrotr_model::measurement::ConnectErrorClass::Refused,
                ),
            ),
            (
                ConnectSubmission::ImmediateError(crate::network::ConnectErrorClass::Unreachable),
                ConnectCallResult::ImmediateError(
                    bigbrotr_model::measurement::ConnectErrorClass::Unreachable,
                ),
            ),
            (
                ConnectSubmission::ImmediateError(crate::network::ConnectErrorClass::Other),
                ConnectCallResult::ImmediateError(
                    bigbrotr_model::measurement::ConnectErrorClass::Other,
                ),
            ),
        ] {
            let trace = TransportSnapshot {
                first_io: Some(FirstIo {
                    target,
                    returned_at: at,
                    submission,
                }),
                ..TransportSnapshot::default()
            };
            let state = State {
                terminal: Some((
                    MeasurementTermination::Failed {
                        phase: MeasurementPhase::SocketSetup,
                        class: MeasurementFailure::LocalResources,
                    },
                    at,
                )),
                ..State::default()
            };
            let value = liveness::measurement(&template(), &trace, &state).unwrap();
            let terminal = terminal(&value);
            assert_eq!(terminal.observed.execution.destination, destination);
            assert_eq!(terminal.observed.execution.result, expected);
            assert_eq!(
                terminal.observed.execution.observed_at,
                Some(bigbrotr_model::ObservedTime::new(0))
            );
            assert_eq!(terminal.ended.elapsed_us, Some(DurationUs::new(0)));
            assert!(terminal.observed.open.tcp_connected.is_none());
            assert!(terminal.observed.open.tls_completed.is_none());
            assert!(terminal.observed.open.websocket_opened.is_none());
            assert!(terminal.observed.read.request_flushed.is_none());
        }
    }
}

#[test]
fn mapper_preserves_null_zero_utc_rollback_and_separate_decoder_verification_times() {
    let anchor = Instant::now();
    let at = |us, wall| TransportInstant {
        monotonic: anchor + Duration::from_micros(us),
        unix_us: wall,
    };
    let trace = TransportSnapshot {
        first_io: Some(FirstIo {
            target: FirstIoTarget::ResolverConnect,
            returned_at: at(0, None),
            submission: ConnectSubmission::Connected,
        }),
        relay_connected: Some(at(1, Some(0))),
        websocket_upgrade_validated: Some(at(2, Some(-4))),
        first_text_write_flushed: Some(at(3, None)),
        websocket: WebSocketCounters {
            rx_physical_frames_started: 4,
            rx_logical_messages_delivered: 4,
            rx_ws_framed_bytes_read: 100,
            rx_ws_framed_bytes_consumed: 90,
            rx_ws_payload_bytes_consumed: 64,
            tx_ws_framed_bytes_written: 40,
            ..WebSocketCounters::default()
        },
        ..TransportSnapshot::default()
    };
    let state = State {
        request_started: true,
        candidates: 1,
        first_event: Some(ValidEvent {
            id: EventId::from_bytes([0xaa; 32]),
            delivered: at(4, Some(-5)),
            verified: at(5, Some(-9)),
        }),
        eose: Some(at(6, Some(0))),
        prefix_verified: true,
        auth: true,
        terminal: Some((MeasurementTermination::Eose, at(7, None))),
        ..State::default()
    };
    let value = liveness::measurement(&template(), &trace, &state).unwrap();
    let terminal = terminal(&value);
    let observed = terminal.observed;
    assert_eq!(
        observed.execution.destination,
        ConnectDestination::DnsResolver
    );
    assert_eq!(observed.execution.observed_at, None);
    assert_eq!(
        observed.open.tcp_connected.unwrap().observed_at,
        Some(bigbrotr_model::ObservedTime::new(0))
    );
    assert_eq!(
        observed.open.websocket_opened.unwrap().observed_at,
        Some(bigbrotr_model::ObservedTime::new(-4))
    );
    assert_eq!(observed.read.request_flushed.unwrap().observed_at, None);
    let event = observed.read.first_valid_event.unwrap();
    assert_eq!(
        event.received.observed_at,
        Some(bigbrotr_model::ObservedTime::new(-5))
    );
    assert_eq!(event.received.elapsed_us, Some(DurationUs::new(4)));
    assert_eq!(
        event.verified.observed_at,
        Some(bigbrotr_model::ObservedTime::new(-9))
    );
    assert_eq!(event.verified.elapsed_us, Some(DurationUs::new(5)));
    assert_eq!(
        observed.read.eose.unwrap().observed_at,
        Some(bigbrotr_model::ObservedTime::new(0))
    );
    assert_eq!(terminal.ended.observed_at, None);
    assert_eq!(terminal.ended.elapsed_us, Some(DurationUs::new(7)));
    let mut inverted = state;
    inverted.first_event.as_mut().unwrap().verified = at(3, Some(100));
    assert!(matches!(
        liveness::measurement(&template(), &trace, &inverted),
        Err(CaptureError::Observation)
    ));
}

#[test]
fn authority_cancel_deadline_and_post_submit_local_refusals_remain_distinct() {
    let mut trace = TransportSnapshot::default();
    let phase = MeasurementPhase::ResponseRead;
    for (stop, expected) in [
        (
            ObservedStop::Authority,
            MeasurementTermination::AuthorityInterruption(phase),
        ),
        (
            ObservedStop::Cancelled,
            MeasurementTermination::Cancelled(phase),
        ),
        (
            ObservedStop::CheckDeadline,
            MeasurementTermination::Deadline(phase),
        ),
    ] {
        trace.stop = Some(stop);
        assert_eq!(
            liveness::transport(TransportError::Operation, &trace, Some(phase), false),
            expected
        );
    }
    trace.stop = None;
    trace.last_connect_failure = Some(ConnectFailure {
        target: FirstIoTarget::RelayConnect,
        stage: ConnectFailureStage::Immediate,
        class: crate::network::ConnectErrorClass::Denied,
    });
    assert_eq!(
        liveness::transport(TransportError::Connect, &trace, None, false),
        MeasurementTermination::Failed {
            phase: MeasurementPhase::TcpConnect,
            class: MeasurementFailure::LocalPermission
        }
    );
    trace.last_connect_failure.as_mut().unwrap().stage = ConnectFailureStage::Preparation;
    assert_eq!(
        liveness::transport(TransportError::Connect, &trace, None, false),
        MeasurementTermination::Failed {
            phase: MeasurementPhase::SocketSetup,
            class: MeasurementFailure::LocalPermission
        }
    );
    // A stale resolver failure cannot relabel a later actual TLS wait.
    trace.relay_connected = Some(TransportInstant {
        monotonic: Instant::now(),
        unix_us: None,
    });
    trace.last_connect_failure.as_mut().unwrap().target = FirstIoTarget::ResolverConnect;
    assert_eq!(
        liveness::transport(TransportError::Authority, &trace, None, true),
        MeasurementTermination::AuthorityInterruption(MeasurementPhase::Tls)
    );
}
