//! Bounded request generations on one already authorized endpoint/access socket.
//! Correlation and EOSE are not admission, job authority or coverage completion.
use crate::{
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

pub struct ReceivedRelay<'message, 'request> {
    /// Connection wire order, including intervening WebSocket control messages.
    pub sequence: u64,
    pub envelope: RelayEnvelope<'message>,
    /// None means uncorrelated or retired work, never an admission candidate.
    pub request: Option<ActiveRequest<'request>>,
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
        let socket = RelayWebSocket::connect(stream, resources, limits.wire_bytes)
            .await
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
                    envelope,
                    request,
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
