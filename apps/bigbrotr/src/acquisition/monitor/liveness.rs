use super::{CaptureError, State};
use crate::{
    acquisition::session::SessionError,
    network::{
        ConnectErrorClass as NetConnectError, ConnectFailureStage, ConnectSubmission, DnsError,
        FirstIoTarget, ObservedSessionLimit, ObservedStop, TransportError, TransportInstant,
        TransportSnapshot, WebSocketError,
    },
};
use bigbrotr_model::{DurationUs, Measurement, ObservedTime, measurement::*};
use bigbrotr_protocol::{EventError, wire::WireError};

fn stop(reason: ObservedStop, phase: MeasurementPhase) -> MeasurementTermination {
    match reason {
        ObservedStop::Authority => MeasurementTermination::AuthorityInterruption(phase),
        ObservedStop::Cancelled => MeasurementTermination::Cancelled(phase),
        ObservedStop::OpenDeadline | ObservedStop::CheckDeadline => {
            MeasurementTermination::Deadline(phase)
        }
        ObservedStop::Clock => failed(phase, MeasurementFailure::LocalConfiguration),
        ObservedStop::Session(limit) => MeasurementTermination::Limit(match limit {
            ObservedSessionLimit::RxPhysicalFrames => MeasurementLimit::RxPhysicalFrames,
            ObservedSessionLimit::RxLogicalMessages => MeasurementLimit::RxMessages,
            ObservedSessionLimit::RxWsFramedBytes => MeasurementLimit::RxWsBytes,
            ObservedSessionLimit::TxWsFramedBytes => MeasurementLimit::TxWsBytes,
        }),
    }
}
fn failed(phase: MeasurementPhase, class: MeasurementFailure) -> MeasurementTermination {
    MeasurementTermination::Failed { phase, class }
}
pub(super) fn transport(
    error: TransportError,
    trace: &TransportSnapshot,
    phase: Option<MeasurementPhase>,
    secure: bool,
) -> MeasurementTermination {
    let phase = phase.unwrap_or_else(|| {
        if trace.relay_connected.is_some() {
            return if secure && trace.tls_completed.is_none() {
                MeasurementPhase::Tls
            } else {
                MeasurementPhase::WebSocket
            };
        }
        if error == TransportError::Connect
            && let Some(failure) = trace.last_connect_failure
        {
            return if failure.stage == ConnectFailureStage::Preparation {
                MeasurementPhase::SocketSetup
            } else if failure.target == FirstIoTarget::ResolverConnect {
                MeasurementPhase::Dns
            } else {
                MeasurementPhase::TcpConnect
            };
        }
        if matches!(
            error,
            TransportError::Tls | TransportError::TlsConfiguration
        ) {
            MeasurementPhase::Tls
        } else if trace.relay_connect_calls > 0 {
            MeasurementPhase::TcpConnect
        } else {
            MeasurementPhase::Dns
        }
    });
    if let Some(reason) = trace.stop {
        return stop(reason, phase);
    }
    match error {
        TransportError::Authority => MeasurementTermination::AuthorityInterruption(phase),
        TransportError::Cancelled => MeasurementTermination::Cancelled(phase),
        TransportError::Deadline | TransportError::Dns(DnsError::Deadline) => {
            MeasurementTermination::Deadline(phase)
        }
        TransportError::Budget | TransportError::Dns(DnsError::Budget) => {
            failed(phase, MeasurementFailure::LocalResources)
        }
        TransportError::Dns(DnsError::ResponseLimit) => {
            MeasurementTermination::Limit(MeasurementLimit::DnsBounds)
        }
        TransportError::Dns(_) => failed(phase, MeasurementFailure::DnsUnavailable),
        TransportError::Policy(_) | TransportError::TlsConfiguration => {
            failed(phase, MeasurementFailure::LocalConfiguration)
        }
        TransportError::Tls => failed(phase, MeasurementFailure::TlsFailed),
        TransportError::Connect => failed(
            phase,
            match trace.last_connect_failure.map(|failure| failure.class) {
                Some(NetConnectError::Denied) => MeasurementFailure::LocalPermission,
                Some(NetConnectError::Resource) => MeasurementFailure::LocalResources,
                _ => MeasurementFailure::ConnectFailed,
            },
        ),
        TransportError::Operation => failed(phase, MeasurementFailure::Transport),
    }
}
pub(super) fn session(
    error: SessionError,
    trace: &TransportSnapshot,
    phase: MeasurementPhase,
) -> MeasurementTermination {
    if let Some(reason) = trace.stop {
        return stop(reason, phase);
    }
    match error {
        SessionError::Budget | SessionError::WebSocket(WebSocketError::Budget) => {
            failed(phase, MeasurementFailure::LocalResources)
        }
        SessionError::InvalidLimits
        | SessionError::Clock
        | SessionError::Capacity
        | SessionError::Exhausted
        | SessionError::Request(_) => failed(phase, MeasurementFailure::LocalConfiguration),
        SessionError::WebSocket(WebSocketError::MessageLimit)
        | SessionError::Envelope(WireError::Bounds(EventError::WireLimit)) => {
            MeasurementTermination::Limit(MeasurementLimit::MessageFramedBytes)
        }
        SessionError::WebSocket(WebSocketError::Handshake) => {
            failed(phase, MeasurementFailure::UpgradeFailed)
        }
        SessionError::Envelope(_)
        | SessionError::BinaryMessage
        | SessionError::WebSocket(WebSocketError::Protocol) => {
            failed(phase, MeasurementFailure::Decode)
        }
        SessionError::Unavailable | SessionError::WebSocket(_) => {
            failed(phase, MeasurementFailure::Transport)
        }
    }
}
pub(super) fn event(error: EventError) -> MeasurementTermination {
    match error {
        EventError::EventSizeLimit => {
            MeasurementTermination::Limit(MeasurementLimit::NormalizedEventBytes)
        }
        EventError::WireLimit => {
            MeasurementTermination::Limit(MeasurementLimit::MessageFramedBytes)
        }
        EventError::InvalidLimits => failed(
            MeasurementPhase::Verification,
            MeasurementFailure::LocalConfiguration,
        ),
        EventError::DepthLimit
        | EventError::NodeLimit
        | EventError::StringLimit
        | EventError::ArenaLimit
        | EventError::InvalidJson => {
            failed(MeasurementPhase::Verification, MeasurementFailure::Decode)
        }
        _ => failed(
            MeasurementPhase::Verification,
            MeasurementFailure::InvalidEvent,
        ),
    }
}
pub(super) fn teardown(trace: &TransportSnapshot) -> TeardownObservation {
    if matches!(
        trace.stop,
        Some(
            ObservedStop::Authority
                | ObservedStop::Cancelled
                | ObservedStop::OpenDeadline
                | ObservedStop::CheckDeadline
        )
    ) {
        TeardownObservation::Interrupted
    } else {
        TeardownObservation::Failed
    }
}
fn moment(
    at: TransportInstant,
    anchor: TransportInstant,
) -> Result<MeasurementMoment, CaptureError> {
    let elapsed = at
        .monotonic
        .checked_duration_since(anchor.monotonic)
        .ok_or(CaptureError::Clock)?;
    Ok(MeasurementMoment {
        observed_at: at.unix_us.map(ObservedTime::new),
        elapsed_us: u64::try_from(elapsed.as_micros()).ok().map(DurationUs::new),
    })
}
pub(super) fn measurement(
    template: &Measurement,
    trace: &TransportSnapshot,
    state: &State,
) -> Result<Measurement, CaptureError> {
    let first = trace.first_io.ok_or(CaptureError::Binding)?;
    let (termination, ended) = state.terminal.ok_or(CaptureError::Binding)?;
    let at = |value| moment(value, first.returned_at);
    let observed = ObservedAttempt {
        execution: ObservedExecution {
            destination: match first.target {
                FirstIoTarget::ResolverConnect => ConnectDestination::DnsResolver,
                FirstIoTarget::RelayConnect => ConnectDestination::Relay,
            },
            result: match first.submission {
                ConnectSubmission::Connected | ConnectSubmission::InProgress => {
                    ConnectCallResult::Accepted
                }
                ConnectSubmission::ImmediateError(error) => {
                    ConnectCallResult::ImmediateError(match error {
                        NetConnectError::Denied => ConnectErrorClass::Permission,
                        NetConnectError::Resource => ConnectErrorClass::Resources,
                        NetConnectError::AddressUnavailable => ConnectErrorClass::Address,
                        NetConnectError::Refused => ConnectErrorClass::Refused,
                        NetConnectError::Unreachable => ConnectErrorClass::Unreachable,
                        NetConnectError::Other => ConnectErrorClass::Other,
                    })
                }
            },
            observed_at: first.returned_at.unix_us.map(ObservedTime::new),
        },
        open: OpenObservation {
            tcp_connected: trace.relay_connected.map(at).transpose()?,
            tls_completed: trace.tls_completed.map(at).transpose()?,
            websocket_opened: trace.websocket_upgrade_validated.map(at).transpose()?,
        },
        read: ReadObservation {
            request_flushed: if state.request_started {
                trace.first_text_write_flushed.map(at).transpose()?
            } else {
                None
            },
            first_valid_event: state
                .first_event
                .map(|event| {
                    Ok::<_, CaptureError>(FirstValidEvent {
                        event_id: event.id,
                        received: at(event.delivered)?,
                        verified: at(event.verified)?,
                    })
                })
                .transpose()?,
            eose: state.eose.map(at).transpose()?,
            eose_prefix_verified: state.prefix_verified,
            auth_observed: state.auth,
            correlated_closed: state.closed,
        },
        counters: MeasurementCounters {
            rx_physical_frames: trace.websocket.rx_physical_frames_started,
            rx_messages: trace.websocket.rx_logical_messages_delivered,
            event_candidates: state.candidates,
            rx_ws_bytes: trace.websocket.rx_ws_framed_bytes_read,
            rx_payload_bytes: trace.websocket.rx_ws_payload_bytes_consumed,
            tx_ws_bytes: trace.websocket.tx_ws_framed_bytes_written,
        },
    };
    Measurement::new(
        template.attempt_id(),
        template.context(),
        template.profile(),
        template.clock_quality(),
        MeasurementState::Terminal(TerminalMeasurement {
            observed,
            ended: at(ended)?,
            termination,
            teardown: state.teardown,
        }),
    )
    .map_err(|_| CaptureError::Observation)
}
