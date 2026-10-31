//! One finite wire quantum. Loss cannot be repaired by a subsequent EOSE.
use super::*;
use bigbrotr::{
    acquisition::{
        quantum::{RequestTerminal, TerminalKind},
        session::{RelaySession, RetainedRelayEvent, SessionError, SessionMessage},
    },
    network::{DirectStream, WebSocketError},
};
use bigbrotr_model::{LocalObservation, acquisition::Filter};
use bigbrotr_protocol::wire::RelayMessage;
#[derive(Clone, Copy)]
pub(super) enum ReceiveFailure {
    Budget,
    Context,
    Session(SessionError),
}

pub(super) struct Arrivals<'a> {
    pub events: Vec<(RetainedRelayEvent<'a>, LocalObservation)>,
    pub terminal: RequestTerminal,
    pub teardown_complete: bool,
    pub(super) _permit: OwnedPermit,
}
impl std::fmt::Debug for ReceiveFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Budget => f.write_str("local receive budget"),
            Self::Context => f.write_str("local receive context"),
            Self::Session(error) => f.debug_tuple("local receive session").field(error).finish(),
        }
    }
}
fn interruption(error: SessionError) -> std::result::Result<TerminalKind, ReceiveFailure> {
    use bigbrotr_protocol::wire::WireError;
    match error {
        SessionError::Envelope(WireError::Bounds(EventError::InvalidLimits))
        | SessionError::WebSocket(WebSocketError::Budget)
        | SessionError::Budget
        | SessionError::Capacity
        | SessionError::Exhausted
        | SessionError::Unavailable
        | SessionError::InvalidLimits
        | SessionError::Clock
        | SessionError::Request(_) => Err(ReceiveFailure::Session(error)),
        SessionError::Envelope(_)
        | SessionError::BinaryMessage
        | SessionError::WebSocket(WebSocketError::Protocol) => Ok(TerminalKind::Decode),
        SessionError::WebSocket(WebSocketError::MessageLimit) => Ok(TerminalKind::FrameCap),
        SessionError::WebSocket(WebSocketError::Handshake | WebSocketError::Unavailable) => {
            Ok(TerminalKind::Transport)
        }
    }
}
#[allow(
    clippy::too_many_arguments,
    reason = "distinct finite wire and retained capture owners"
)]
pub(super) async fn collect<'a>(
    stream: &mut DirectStream<'_>,
    budget: &ResourceBudget,
    filters: &'a [Filter],
    attribution: &StoredObservation,
    route: bigbrotr::network::RouteSelection,
    max_event_bytes: usize,
    soft: Instant,
    cancel: &Cancellation,
) -> std::result::Result<Arrivals<'a>, ReceiveFailure> {
    let permit = budget
        .try_acquire(
            ResourceRequest::data(
                (3 * size_of::<(RetainedRelayEvent<'_>, LocalObservation)>()
                    + size_of::<Arrivals<'_>>()
                    + stream.endpoint().as_str().len()) as u64,
                1,
                0,
            )
            .expect("finite arrivals"),
        )
        .map_err(|_| ReceiveFailure::Budget)?;
    let mut terminal_permit = Some(
        budget
            .try_acquire(
                ResourceRequest::data(QuantumGrant::terminal_bytes(), 1, 0)
                    .expect("finite terminal"),
            )
            .map_err(|_| ReceiveFailure::Budget)?,
    );
    let mut events = Vec::with_capacity(3);
    let endpoint = stream.endpoint().clone();
    let mut request = None;
    let mut terminal = None;
    let mut kind = TerminalKind::FrameCap;
    let mut teardown_complete = false;
    // Dropping this future destroys the failed protocol session; DirectConnector
    // then destroys the actual socket and rolls back before native work begins.
    let protocol = async {
        let mut session = match RelaySession::connect(
            stream,
            budget,
            EventLimits {
                event_bytes: max_event_bytes,
                ..EventLimits::default()
            },
            1.try_into().unwrap(),
        )
        .await
        {
            Ok(value) => value,
            Err(error) => {
                kind = interruption(error)?;
                return Ok::<_, ReceiveFailure>(());
            }
        };
        let id = match session.open(filters, Some(3)).await {
            Ok(id) => id,
            Err(error) => {
                kind = interruption(error)?;
                return Ok(());
            }
        };
        request = Some(id);
        for _ in 0..32 {
            let message = match session.receive().await {
                Ok(Some(value)) => value,
                Ok(None) => {
                    kind = TerminalKind::Disconnected;
                    break;
                }
                Err(error) => {
                    kind = interruption(error)?;
                    break;
                }
            };
            let SessionMessage::Relay(message) = message else {
                continue;
            };
            if message.request().is_none_or(|value| value.id() != id) {
                continue;
            }
            match message.envelope().message() {
                RelayMessage::Event { .. } => {
                    if events.len() == 3 {
                        kind = TerminalKind::EventCap;
                        break;
                    }
                    let captured = message
                        .retain_event(id, budget)
                        .map_err(|_| ReceiveFailure::Budget)?;
                    let observed = attribution
                        .received(&endpoint, route, captured.received())
                        .map_err(|_| ReceiveFailure::Context)?;
                    events.push((captured, observed));
                }
                RelayMessage::Eose { .. } | RelayMessage::Closed { .. } => {
                    terminal = Some(
                        message
                            .retain_terminal(id, terminal_permit.take().expect("one terminal"))
                            .map_err(|_| ReceiveFailure::Context)?,
                    );
                    break;
                }
                _ => {}
            }
        }
        // Teardown failure is recorded separately and never overwrites EOSE.
        teardown_complete =
            session.close_request(id).await.is_ok() && session.close().await.is_ok();
        Ok(())
    };
    tokio::select! {
        biased;
        _=cancel.wait()=>{kind=TerminalKind::Cancelled;},
        result=tokio::time::timeout_at(soft,protocol)=>{
            match result {Ok(result)=>result?,Err(_)=>kind=TerminalKind::Deadline}
        }
    }
    let terminal = match terminal {
        Some(value) => value,
        None => RequestTerminal::interruption(
            kind,
            request,
            terminal_permit.take().expect("local terminal grant"),
        )
        .map_err(|_| ReceiveFailure::Context)?,
    };
    Ok(Arrivals {
        events,
        terminal,
        teardown_complete,
        _permit: permit,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn local_failure_is_not_a_peer_transport_terminal() {
        for error in [
            SessionError::Budget,
            SessionError::Capacity,
            SessionError::Clock,
            SessionError::Exhausted,
            SessionError::Unavailable,
            SessionError::InvalidLimits,
            SessionError::WebSocket(WebSocketError::Budget),
        ] {
            assert!(interruption(error).is_err());
        }
        for (error, terminal) in [
            (SessionError::BinaryMessage, TerminalKind::Decode),
            (
                SessionError::WebSocket(WebSocketError::Protocol),
                TerminalKind::Decode,
            ),
            (
                SessionError::WebSocket(WebSocketError::MessageLimit),
                TerminalKind::FrameCap,
            ),
            (
                SessionError::WebSocket(WebSocketError::Unavailable),
                TerminalKind::Transport,
            ),
        ] {
            assert!(matches!(interruption(error),Ok(actual) if actual==terminal));
        }
    }
}
