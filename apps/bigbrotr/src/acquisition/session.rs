//! Bounded request generations on one already authorized endpoint/access socket.
//! Correlation and EOSE are not admission, job authority or coverage completion.
use crate::{
    acquisition::receive_clock::{ReceiveClock, ReceiveTime},
    network::{DirectStream, ReceivedMessage, RelayWebSocket, WebSocketError},
    runtime::{OwnedPermit, ResourceBudget, ResourceRequest},
};
use bigbrotr_model::acquisition::Filter;
use bigbrotr_protocol::{
    EventLimits,
    wire::{
        RelayEnvelope, RelayMessage, RequestEncodeError, WireError, decode_relay, encode_request,
    },
};
use std::{fmt, io, num::NonZeroU16};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionError {
    Budget,
    Capacity,
    Exhausted,
    Unavailable,
    InvalidLimits,
    Clock,
    BinaryMessage,
    Request(RequestEncodeError),
    Envelope(WireError),
    WebSocket(WebSocketError),
}
impl fmt::Display for SessionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "relay session unavailable: {self:?}")
    }
}
impl std::error::Error for SessionError {}
impl From<SessionError> for io::Error {
    fn from(error: SessionError) -> Self {
        Self::other(error)
    }
}

/// Random connection nonce plus a monotonically increasing generation, encoded
/// as 48 ASCII hex characters. Private construction prevents arbitrary CLOSE IDs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestId([u8; 48]);
impl RequestId {
    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.0).expect("generated ASCII request ID")
    }
}

/// Borrow the immutable request until its caller's scope/selector owner drops.
/// The consumer must associate this ID with its retained job/scope and check
/// collection membership as well as these exact wire filters before admission.
#[derive(Clone, Copy)]
pub struct ActiveRequest<'request> {
    id: RequestId,
    filters: &'request [Filter],
    limit: Option<u64>,
}
impl<'request> ActiveRequest<'request> {
    pub fn id(self) -> RequestId {
        self.id
    }
    pub fn filters(self) -> &'request [Filter] {
        self.filters
    }
    pub fn limit(self) -> Option<u64> {
        self.limit
    }
}

/// Session attribution cannot be relabeled before capture.
/// ```compile_fail
/// use bigbrotr::acquisition::session::{ReceivedRelay, ActiveRequest};
/// fn relabel<'a>(message: &mut ReceivedRelay<'_, 'a>, saved: ActiveRequest<'a>) {
///     message.request = Some(saved);
/// }
/// ```
pub struct ReceivedRelay<'message, 'request> {
    /// Connection wire order, including intervening WebSocket control messages.
    sequence: u64,
    /// Local complete-message receipt, sampled before envelope parsing.
    received: ReceiveTime,
    envelope: RelayEnvelope<'message>,
    /// None means uncorrelated or retired work, never an admission candidate.
    request: Option<ActiveRequest<'request>>,
    limits: &'message EventLimits,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaptureError {
    NotEvent,
    Uncorrelated,
    Budget,
}

/// One owned EVENT value with its original local receive sample and exact
/// request predicate. This is neither verified data nor source/job authority.
/// Keep the scope/filter owner alive; preparation needs its own decoding grant.
pub struct RetainedRelayEvent<'request> {
    json: Vec<u8>,
    sequence: u64,
    received: ReceiveTime,
    request: ActiveRequest<'request>,
    limits: EventLimits,
    // Release credit only after the copied input is destroyed.
    permit: OwnedPermit,
}
impl<'request> ReceivedRelay<'_, 'request> {
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
    pub fn received(&self) -> ReceiveTime {
        self.received
    }
    pub fn envelope(&self) -> &RelayEnvelope<'_> {
        &self.envelope
    }
    pub fn request(&self) -> Option<ActiveRequest<'request>> {
        self.request
    }
    pub fn retain_terminal(
        &self,
        expected: RequestId,
        permit: OwnedPermit,
    ) -> Result<super::quantum::RequestTerminal, super::quantum::QuantumError> {
        use super::quantum::{QuantumError, RequestTerminal, TerminalKind};
        if self.request.is_none_or(|request| request.id != expected)
            || permit.request().units().data_bytes()
                < (self.envelope.json().len() + size_of::<RequestTerminal>()) as u64
        {
            return Err(QuantumError::Context);
        }
        let (kind, finish, more, auth) = match self.envelope.message() {
            RelayMessage::Eose { hints, .. } => (
                TerminalKind::Eose,
                hints
                    .as_ref()
                    .is_some_and(|values| values.iter().any(|v| v.as_ref() == "finish")),
                hints
                    .as_ref()
                    .is_some_and(|values| values.iter().any(|v| v.as_ref() == "more")),
                hints
                    .as_ref()
                    .is_some_and(|values| values.iter().any(|v| v.as_ref() == "auth")),
            ),
            RelayMessage::Closed { .. } => (TerminalKind::Closed, false, false, false),
            _ => return Err(QuantumError::Context),
        };
        let active = self.request.ok_or(QuantumError::Context)?;
        let predicate = encode_request(
            "000000000000000000000000000000000000000000000000",
            active.filters,
            active.limit,
            self.limits.wire_bytes,
        )
        .map_err(|_| QuantumError::Predicate)?;
        use sha2::Digest;
        let predicate = Some(sha2::Sha256::digest(predicate.as_bytes()).into());
        Ok(RequestTerminal {
            kind,
            predicate,
            request: Some(expected),
            sequence: Some(self.sequence),
            received_at_us: self.received.received_at().get(),
            raw: self.envelope.json().as_bytes().to_vec(),
            finish,
            more,
            auth,
            _permit: permit,
        })
    }
    /// Reserve the copied input before allocating it. A failed reservation leaves
    /// the borrowed response available for bounded backpressure or interruption.
    /// Return this value through the guarded source result, never an outer side
    /// channel that could survive a failed source rollback/delivery check.
    pub fn retain_event(
        &self,
        expected: RequestId,
        resources: &ResourceBudget,
    ) -> Result<RetainedRelayEvent<'request>, CaptureError> {
        let RelayMessage::Event { json, .. } = self.envelope.message() else {
            return Err(CaptureError::NotEvent);
        };
        let request = self
            .request
            .filter(|request| request.id == expected)
            .ok_or(CaptureError::Uncorrelated)?;
        let input = json.get().as_bytes();
        let bytes = input
            .len()
            .checked_add(size_of::<RetainedRelayEvent<'_>>())
            .ok_or(CaptureError::Budget)?;
        let permit = resources
            .try_acquire(
                ResourceRequest::data(bytes as u64, 1, 0).map_err(|_| CaptureError::Budget)?,
            )
            .map_err(|_| CaptureError::Budget)?;
        let mut json = Vec::new();
        json.try_reserve_exact(input.len())
            .map_err(|_| CaptureError::Budget)?;
        json.extend_from_slice(input);
        Ok(RetainedRelayEvent {
            json,
            sequence: self.sequence,
            received: self.received,
            request,
            limits: *self.limits,
            permit,
        })
    }
}
impl<'request> RetainedRelayEvent<'request> {
    pub fn json(&self) -> &[u8] {
        &self.json
    }
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
    pub fn received(&self) -> ReceiveTime {
        self.received
    }
    pub fn request(&self) -> ActiveRequest<'request> {
        self.request
    }
    pub fn limits(&self) -> EventLimits {
        self.limits
    }
    pub fn reserved_bytes(&self) -> u64 {
        self.permit.request().units().data_bytes()
    }
}
pub enum SessionMessage<'message, 'request> {
    Relay(ReceivedRelay<'message, 'request>),
    Control {
        sequence: u64,
        message: ReceivedMessage<'message>,
    },
}

/// The borrowed response retains decode credit until it is dropped. The consumer
/// must classify or join preceding work before interpreting EOSE. Copying data
/// for queued verification/admission needs a separate owned grant.
/// Errors or cancellation during wire work retire this complete session. The
/// enclosing connector owns finite deadlines, authority and actual socket closure.
pub struct RelaySession<'borrow, 'owner, 'request> {
    socket: RelayWebSocket<'borrow, 'owner>,
    requests: Vec<ActiveRequest<'request>>,
    nonce: [u8; 16],
    generation: u64,
    received: u64,
    max_requests: usize,
    limits: EventLimits,
    failed: bool,
    clock: ReceiveClock,
    // Protocol buffers, registry and borrowed response must retire before credit.
    _permit: OwnedPermit,
}
impl<'borrow, 'owner, 'request> RelaySession<'borrow, 'owner, 'request> {
    pub async fn connect(
        stream: &'borrow mut DirectStream<'owner>,
        resources: &ResourceBudget,
        limits: EventLimits,
        max_requests: NonZeroU16,
    ) -> Result<Self, SessionError> {
        Self::connect_inner(stream, resources, limits, max_requests, false).await
    }

    /// The fixed passive profile on an already observed, authorized stream.
    /// Default sessions retain their existing limits and constructor behavior.
    pub async fn connect_observed(
        stream: &'borrow mut DirectStream<'owner>,
        resources: &ResourceBudget,
    ) -> Result<Self, SessionError> {
        Self::connect_inner(
            stream,
            resources,
            EventLimits::default(),
            NonZeroU16::new(1).expect("one passive request"),
            true,
        )
        .await
    }

    async fn connect_inner(
        stream: &'borrow mut DirectStream<'owner>,
        resources: &ResourceBudget,
        limits: EventLimits,
        max_requests: NonZeroU16,
        observed: bool,
    ) -> Result<Self, SessionError> {
        limits.validate().map_err(|_| SessionError::InvalidLimits)?;
        let count = usize::from(max_requests.get());
        let bytes = limits
            .wire_bytes
            .checked_add(limits.parse_bytes)
            .and_then(|n| n.checked_add(count * size_of::<ActiveRequest<'_>>()))
            .and_then(|n| n.checked_add(size_of::<Self>()))
            .ok_or(SessionError::Budget)?;
        let permit = resources
            .try_acquire(
                ResourceRequest::data(bytes as u64, u64::from(max_requests.get()) + 1, 0)
                    .map_err(|_| SessionError::Budget)?,
            )
            .map_err(|_| SessionError::Budget)?;
        let mut requests = Vec::new();
        requests
            .try_reserve_exact(count)
            .map_err(|_| SessionError::Budget)?;
        let mut nonce = [0; 16];
        getrandom::fill(&mut nonce).map_err(|_| SessionError::Unavailable)?;
        let clock = ReceiveClock::new().map_err(|_| SessionError::Clock)?;
        let socket = if observed {
            RelayWebSocket::connect_observed(stream, resources).await
        } else {
            RelayWebSocket::connect(stream, resources, limits.wire_bytes).await
        }
        .map_err(SessionError::WebSocket)?;
        Ok(Self {
            socket,
            requests,
            nonce,
            generation: 0,
            received: 0,
            max_requests: count,
            limits,
            failed: false,
            clock,
            _permit: permit,
        })
    }

    pub async fn open(
        &mut self,
        filters: &'request [Filter],
        limit: Option<u64>,
    ) -> Result<RequestId, SessionError> {
        self.require_live()?;
        if self.requests.len() == self.max_requests {
            return Err(SessionError::Capacity);
        }
        self.generation = self
            .generation
            .checked_add(1)
            .ok_or(SessionError::Exhausted)?;
        let mut bytes = [0; 48];
        for (index, byte) in self
            .nonce
            .iter()
            .chain(self.generation.to_be_bytes().iter())
            .enumerate()
        {
            bytes[2 * index] = b"0123456789abcdef"[usize::from(byte >> 4)];
            bytes[2 * index + 1] = b"0123456789abcdef"[usize::from(byte & 15)];
        }
        let id = RequestId(bytes);
        let encoded = encode_request(id.as_str(), filters, limit, self.limits.wire_bytes)
            .map_err(SessionError::Request)?;
        self.failed = true;
        self.socket
            .send_text(&encoded)
            .await
            .map_err(SessionError::WebSocket)?;
        self.requests.push(ActiveRequest { id, filters, limit });
        self.failed = false;
        Ok(id)
    }

    /// A retired or foreign handle emits no frame. In particular, closing and
    /// reopening the same local slot never lets the old handle close its successor.
    pub async fn close_request(&mut self, id: RequestId) -> Result<bool, SessionError> {
        self.require_live()?;
        let Some(index) = self.requests.iter().position(|request| request.id == id) else {
            return Ok(false);
        };
        let command = format!("[\"CLOSE\",\"{}\"]", id.as_str());
        self.failed = true;
        self.socket
            .send_text(&command)
            .await
            .map_err(SessionError::WebSocket)?;
        self.requests.swap_remove(index);
        self.failed = false;
        Ok(true)
    }

    pub async fn receive(&mut self) -> Result<Option<SessionMessage<'_, 'request>>, SessionError> {
        self.require_live()?;
        self.failed = true;
        let Some(message) = self
            .socket
            .receive()
            .await
            .map_err(SessionError::WebSocket)?
        else {
            return Ok(None);
        };
        let received = self.clock.sample().map_err(|_| SessionError::Clock)?;
        self.received = self
            .received
            .checked_add(1)
            .ok_or(SessionError::Exhausted)?;
        let sequence = self.received;
        match message {
            ReceivedMessage::Text(text) => {
                let envelope =
                    decode_relay(text.as_bytes(), self.limits).map_err(SessionError::Envelope)?;
                let (subscription, closed) = match envelope.message() {
                    RelayMessage::Event { subscription, .. }
                    | RelayMessage::Eose { subscription, .. } => {
                        (Some(subscription.as_ref()), false)
                    }
                    RelayMessage::Closed { subscription, .. } => {
                        (Some(subscription.as_ref()), true)
                    }
                    _ => (None, false),
                };
                let index = subscription
                    .and_then(|id| self.requests.iter().position(|r| r.id.as_str() == id));
                let request = index.map(|i| self.requests[i]);
                if closed && let Some(index) = index {
                    self.requests.swap_remove(index);
                }
                self.failed = false;
                Ok(Some(SessionMessage::Relay(ReceivedRelay {
                    sequence,
                    received,
                    envelope,
                    request,
                    limits: &self.limits,
                })))
            }
            ReceivedMessage::Binary(_) => Err(SessionError::BinaryMessage),
            message => {
                self.failed = matches!(message, ReceivedMessage::Close { .. });
                Ok(Some(SessionMessage::Control { sequence, message }))
            }
        }
    }

    pub async fn close(self) -> Result<(), SessionError> {
        self.require_live()?;
        self.socket.close().await.map_err(SessionError::WebSocket)
    }

    fn require_live(&self) -> Result<(), SessionError> {
        if self.failed {
            Err(SessionError::Unavailable)
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
#[path = "preparation_tests.rs"]
mod preparation_tests;

#[cfg(test)]
mod quantum_capture_tests {
    use super::*;
    use crate::acquisition::quantum::{QuantumError, RequestTerminal, TerminalKind};
    use crate::runtime::{ProcessEnvelope, ResourceLimits, WaiterLimits};
    use bigbrotr_model::{NostrTime, acquisition::FilterFields};
    use sha2::{Digest, Sha256};

    #[test]
    fn terminal_keeps_empty_unknown_and_contradictory_hints_and_actual_empty_request_predicate() {
        let budget = ResourceBudget::new(
            ProcessEnvelope::new(
                32 * 1024 * 1024,
                0,
                ResourceLimits::new(24 * 1024 * 1024, 8, 0, 0),
                WaiterLimits::new(0, 0),
            )
            .unwrap(),
        );
        let grant = || {
            budget
                .try_acquire(ResourceRequest::data(3 * 1024 * 1024, 1, 0).unwrap())
                .unwrap()
        };
        let filters = [Filter::new(FilterFields {
            since: Some(NostrTime(0)),
            until: Some(NostrTime(86_399)),
            ..FilterFields::default()
        })
        .unwrap()];
        let id = RequestId([b'a'; 48]);
        let limits = EventLimits::default();
        for (tail, flags) in [
            ("", (false, false, false)),
            (",[]", (false, false, false)),
            (",[\"unknown\"]", (false, false, false)),
            (",[\"finish\",\"more\",\"auth\"]", (true, true, true)),
        ] {
            let raw = format!("[\"EOSE\",\"{}\"{tail}]", id.as_str());
            let relay = ReceivedRelay {
                sequence: 7,
                received: ReceiveClock::new().unwrap().sample().unwrap(),
                envelope: decode_relay(raw.as_bytes(), limits).unwrap(),
                request: Some(ActiveRequest {
                    id,
                    filters: &filters,
                    limit: Some(3),
                }),
                limits: &limits,
            };
            let captured = relay.retain_terminal(id, grant()).unwrap();
            assert_eq!(captured.kind, TerminalKind::Eose);
            assert_eq!(captured.raw, raw.as_bytes());
            assert_eq!((captured.finish, captured.more, captured.auth), flags);
            let exact=b"[\"REQ\",\"000000000000000000000000000000000000000000000000\",{\"since\":0,\"until\":86399,\"limit\":3}]";
            assert_eq!(captured.predicate, Some(Sha256::digest(exact).into()));
            assert!(matches!(
                relay.retain_terminal(RequestId([b'b'; 48]), grant()),
                Err(QuantumError::Context)
            ));
        }
        assert!(matches!(
            RequestTerminal::interruption(TerminalKind::Eose, Some(id), grant()),
            Err(QuantumError::Context)
        ));
        assert!(matches!(
            RequestTerminal::interruption(TerminalKind::Closed, Some(id), grant()),
            Err(QuantumError::Context)
        ));
        assert_eq!(budget.snapshot().active_permits(), 0);
    }
}
