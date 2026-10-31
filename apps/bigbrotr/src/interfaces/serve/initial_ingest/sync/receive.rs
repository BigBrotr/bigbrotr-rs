//! One finite wire quantum. Loss cannot be repaired by a subsequent EOSE.
use super::*;
use bigbrotr::{
    acquisition::{
        quantum::{RequestTerminal, TerminalKind},
        session::{RelaySession, RetainedRelayEvent, SessionError, SessionMessage},
    },
    network::DirectStream,
};
use bigbrotr_model::{LocalObservation, acquisition::Filter};
use bigbrotr_protocol::wire::RelayMessage;
use std::io;

pub(super) struct Arrivals<'a> {
    pub events: Vec<(RetainedRelayEvent<'a>, LocalObservation)>,
    pub terminal: RequestTerminal,
    pub teardown_complete: bool,
    pub(super) _permit: OwnedPermit,
}
fn interruption(error: SessionError) -> TerminalKind {
    match error {
        SessionError::Envelope(_) | SessionError::BinaryMessage => TerminalKind::Decode,
        _ => TerminalKind::Transport,
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
) -> io::Result<Arrivals<'a>> {
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
        .map_err(|e| io::Error::other(format!("arrival budget: {e:?}")))?;
    let mut terminal_permit = Some(
        budget
            .try_acquire(
                ResourceRequest::data(QuantumGrant::terminal_bytes(), 1, 0)
                    .expect("finite terminal"),
            )
            .map_err(|e| io::Error::other(format!("terminal budget: {e:?}")))?,
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
                kind = interruption(error);
                return Ok::<_, io::Error>(());
            }
        };
        let id = match session.open(filters, Some(3)).await {
            Ok(id) => id,
            Err(error) => {
                kind = interruption(error);
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
                    kind = interruption(error);
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
                        .map_err(|e| io::Error::other(format!("retained event: {e:?}")))?;
                    let observed = attribution
                        .received(&endpoint, route, captured.received())
                        .map_err(|e| io::Error::other(format!("received observation: {e:?}")))?;
                    events.push((captured, observed));
                }
                RelayMessage::Eose { .. } | RelayMessage::Closed { .. } => {
                    terminal = Some(
                        message
                            .retain_terminal(id, terminal_permit.take().expect("one terminal"))
                            .map_err(io::Error::other)?,
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
        .map_err(io::Error::other)?,
    };
    Ok(Arrivals {
        events,
        terminal,
        teardown_complete,
        _permit: permit,
    })
}
