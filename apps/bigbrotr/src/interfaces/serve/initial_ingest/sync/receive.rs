//! One finite wire quantum. Cancellation retires the whole protocol session.
use super::*;
use bigbrotr::{
    acquisition::session::{RelaySession, RetainedRelayEvent, SessionMessage},
    network::DirectStream,
};
use bigbrotr_model::LocalObservation;
use bigbrotr_protocol::wire::RelayMessage;
use std::io;

pub(super) struct Arrivals<'a> {
    pub events: Vec<(RetainedRelayEvent<'a>, LocalObservation)>,
    _permit: OwnedPermit,
}

pub(super) async fn collect<'a>(
    stream: &mut DirectStream<'_>,
    budget: &ResourceBudget,
    scope: &'a StoredAcquisitionScope,
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
            .expect("finite arrival batch"),
        )
        .map_err(|e| io::Error::other(format!("arrival batch budget: {e:?}")))?;
    let mut arrivals = Arrivals {
        events: Vec::with_capacity(3),
        _permit: permit,
    };
    let endpoint = stream.endpoint().clone();
    // Handshake, REQ, all frames and graceful close share the original soft
    // cutoff. On timeout the future drops its failed session before returning;
    // the enclosing DirectConnector then drops the actual socket before SQL.
    let protocol = async {
        let mut session = RelaySession::connect(
            stream,
            budget,
            EventLimits {
                event_bytes: max_event_bytes,
                ..EventLimits::default()
            },
            1.try_into().unwrap(),
        )
        .await?;
        let id = session.open(scope.selector().filters(), Some(3)).await?;
        // Bound irrelevant frames too: a malicious peer cannot spend the whole
        // quantum sending notices without an explicit finite work limit.
        for _ in 0..32 {
            let Some(message) = session.receive().await? else {
                return Ok::<_, io::Error>(());
            };
            let SessionMessage::Relay(message) = message else {
                continue;
            };
            if message.request().is_none_or(|request| request.id() != id) {
                continue;
            }
            match message.envelope().message() {
                RelayMessage::Event { .. } if arrivals.events.len() < 3 => {
                    let captured = message
                        .retain_event(id, budget)
                        .map_err(|e| io::Error::other(format!("retained event: {e:?}")))?;
                    let observed = attribution
                        .received(&endpoint, route, captured.received())
                        .map_err(|e| io::Error::other(format!("received observation: {e:?}")))?;
                    arrivals.events.push((captured, observed));
                }
                RelayMessage::Eose { .. } | RelayMessage::Closed { .. } => break,
                _ => {}
            }
        }
        session.close_request(id).await?;
        session.close().await?;
        Ok(())
    };
    tokio::select! {
        biased;
        _ = cancel.wait() => {},
        result = tokio::time::timeout_at(soft, protocol) => {
            if let Ok(result) = result { result?; }
        },
    }
    Ok(arrivals)
}
