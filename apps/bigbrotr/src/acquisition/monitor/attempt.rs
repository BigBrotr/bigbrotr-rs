use super::{PreparedPassiveRead, ValidEvent, liveness};
use crate::{
    acquisition::session::{RelaySession, SessionMessage},
    network::{DirectStream, ReceivedMessage, TransportInstant},
};
use bigbrotr_model::measurement::{
    MeasurementFailure, MeasurementLimit, MeasurementPhase, MeasurementTermination,
    PassiveReadProfile, TeardownObservation,
};
use bigbrotr_protocol::{EventLimits, verify_json, wire::RelayMessage};
use std::io;

impl PreparedPassiveRead {
    /// Borrow only the stream for this prepared attempt. Sequential bounded
    /// verification joins each preceding EVENT before interpreting own EOSE.
    pub async fn read(&self, stream: &mut DirectStream<'_>) -> io::Result<()> {
        let snapshot = self.trace.snapshot();
        {
            let mut state = self.state();
            if state.entered
                || !stream.observed_by(&self.trace)
                || stream.endpoint() != &self.endpoint
                || snapshot.first_io.is_none()
                || snapshot.websocket_upgrade_validated.is_some()
            {
                state.invalid_binding = true;
                return Err(io::Error::other("passive read attempt binding refused"));
            }
            state.entered = true;
            state.phase = Some(MeasurementPhase::WebSocket);
        }
        let mut session =
            match RelaySession::connect_observed(stream, self.reservation.budget()).await {
                Ok(session) => session,
                Err(error) => {
                    self.terminal(liveness::session(
                        error,
                        &self.trace.snapshot(),
                        MeasurementPhase::WebSocket,
                    ));
                    return Err(error.into());
                }
            };
        self.phase(MeasurementPhase::RequestWrite);
        self.state().request_started = true;
        let request = match session.open(&self.filters, Some(1)).await {
            Ok(request) => request,
            Err(error) => {
                self.terminal(liveness::session(
                    error,
                    &self.trace.snapshot(),
                    MeasurementPhase::RequestWrite,
                ));
                return Err(error.into());
            }
        };
        let positive = loop {
            self.phase(MeasurementPhase::ResponseRead);
            let received = match session.receive().await {
                Ok(Some(message)) => message,
                Ok(None) => {
                    self.terminal(MeasurementTermination::Failed {
                        phase: MeasurementPhase::ResponseRead,
                        class: MeasurementFailure::Disconnected,
                    });
                    return Err(io::Error::other("passive relay disconnected"));
                }
                Err(error) => {
                    self.terminal(liveness::session(
                        error,
                        &self.trace.snapshot(),
                        MeasurementPhase::ResponseRead,
                    ));
                    return Err(error.into());
                }
            };
            let SessionMessage::Relay(received) = received else {
                if matches!(
                    received,
                    SessionMessage::Control {
                        message: ReceivedMessage::Close { .. },
                        ..
                    }
                ) {
                    self.terminal(MeasurementTermination::Failed {
                        phase: MeasurementPhase::ResponseRead,
                        class: MeasurementFailure::Disconnected,
                    });
                    return Err(io::Error::other("passive WebSocket closed"));
                }
                continue;
            };
            let own = received
                .request()
                .is_some_and(|active| active.id() == request);
            let delivery = self
                .trace
                .snapshot()
                .last_message_delivered
                .ok_or_else(|| io::Error::other("passive decoder delivery unavailable"))?;
            match received.envelope().message() {
                RelayMessage::Auth { .. } => self.state().auth = true,
                RelayMessage::Event { json, .. } => {
                    let candidates = {
                        let mut state = self.state();
                        state.candidates += 1;
                        state.candidates
                    };
                    if candidates > PassiveReadProfile::MAX_EVENT_CANDIDATES {
                        self.terminal(MeasurementTermination::Limit(
                            MeasurementLimit::EventCandidates,
                        ));
                        break false;
                    }
                    if !own {
                        continue;
                    }
                    self.phase(MeasurementPhase::Verification);
                    match verify_json(json.get().as_bytes(), EventLimits::default()) {
                        Ok(event) if event.event().kind == 1 => {
                            self.state().first_event.get_or_insert(ValidEvent {
                                id: event.event().id,
                                delivered: delivery,
                                verified: TransportInstant::now(),
                            });
                        }
                        Ok(_) => {
                            self.terminal(MeasurementTermination::Failed {
                                phase: MeasurementPhase::Verification,
                                class: MeasurementFailure::InvalidEvent,
                            });
                            break false;
                        }
                        Err(error) => {
                            self.terminal(liveness::event(error));
                            break false;
                        }
                    }
                    // The enclosing guard rechecks authority before the next
                    // poll. No event body or verification task outlives this arm.
                    self.phase(MeasurementPhase::ResponseRead);
                    tokio::task::yield_now().await;
                }
                RelayMessage::Eose { .. } if own => {
                    let mut state = self.state();
                    state.eose = Some(delivery);
                    state.prefix_verified = true;
                    drop(state);
                    self.terminal(MeasurementTermination::Eose);
                    break true;
                }
                RelayMessage::Closed { .. } if own => {
                    self.state().closed = true;
                    self.terminal(MeasurementTermination::Closed);
                    break true;
                }
                _ => {}
            }
        };
        // Freeze the first terminal before any CLOSE/WS cleanup or subsequent
        // Control source-delivery fence. Final mapping rereads all byte totals.
        self.state().teardown_started = true;
        if let Err(error) = session.close_request(request).await {
            self.state().teardown = liveness::teardown(&self.trace.snapshot());
            return Err(error.into());
        }
        match session.close().await {
            Ok(()) => self.state().teardown = TeardownObservation::Completed,
            Err(error) => {
                self.state().teardown = liveness::teardown(&self.trace.snapshot());
                return Err(error.into());
            }
        }
        if positive {
            Ok(())
        } else {
            Err(io::Error::other("passive read refused a candidate"))
        }
    }
}
