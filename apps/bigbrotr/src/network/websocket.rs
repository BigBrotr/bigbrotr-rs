//! Finite protocol consumer of an already authorized, guarded TCP/TLS stream.
mod input;

use super::DirectStream;
use crate::runtime::{OwnedPermit, ResourceBudget, ResourceRequest};
use futures_util::{Sink, Stream};
use input::Input;
use std::{fmt, future::poll_fn, io, pin::Pin};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{Error, Message, client::IntoClientRequest, protocol::WebSocketConfig},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WebSocketError {
    Budget,
    MessageLimit,
    Handshake,
    Protocol,
    Unavailable,
}
impl fmt::Display for WebSocketError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Budget => "WebSocket resources unavailable",
            Self::MessageLimit => "WebSocket framed message exceeds its limit",
            Self::Handshake => "WebSocket handshake refused",
            Self::Protocol => "WebSocket protocol refused",
            Self::Unavailable => "WebSocket connection unavailable",
        })
    }
}
impl std::error::Error for WebSocketError {}
impl From<WebSocketError> for io::Error {
    fn from(error: WebSocketError) -> Self {
        Self::new(io::ErrorKind::InvalidData, error)
    }
}
fn classify(error: Error) -> WebSocketError {
    match error {
        Error::Io(error) => error
            .get_ref()
            .and_then(|error| error.downcast_ref::<WebSocketError>())
            .copied()
            .unwrap_or(WebSocketError::Unavailable),
        Error::Capacity(_) | Error::WriteBufferFull(_) => WebSocketError::MessageLimit,
        _ => WebSocketError::Protocol,
    }
}

/// Borrowed projections prevent upstream Bytes/Message clones from escaping the
/// protocol reservation. Copying into an admission queue needs its own budget.
pub enum ReceivedMessage<'a> {
    Text(&'a str),
    Binary(&'a [u8]),
    Ping(&'a [u8]),
    Pong(&'a [u8]),
    Close { code: Option<u16>, reason: &'a str },
}

/// No background driver, split halves or raw socket escape. Cancel the enclosing
/// connector operation to retire a partial handshake/read and its socket together.
pub struct RelayWebSocket<'borrow, 'owner> {
    socket: WebSocketStream<Input<'borrow, 'owner>>,
    received: Option<Message>,
    failed: bool,
    limit: usize,
    // Drop all protocol storage above before releasing its reservation.
    _permit: OwnedPermit,
}
impl<'borrow, 'owner> RelayWebSocket<'borrow, 'owner> {
    pub async fn connect(
        stream: &'borrow mut DirectStream<'owner>,
        resources: &ResourceBudget,
        max_message_bytes: usize,
    ) -> Result<Self, WebSocketError> {
        // Covers growing upstream frame/message/output buffers and handshake
        // copies. This conservative reservation is not a measured RSS bound.
        let bytes = (max_message_bytes as u64)
            .checked_mul(8)
            .and_then(|bytes| bytes.checked_add(1024 * 1024))
            .ok_or(WebSocketError::Budget)?;
        let permit = resources
            .try_acquire(ResourceRequest::data(bytes, 1, 0).map_err(|_| WebSocketError::Budget)?)
            .map_err(|_| WebSocketError::Budget)?;
        let request = stream
            .endpoint()
            .as_str()
            .into_client_request()
            .map_err(|_| WebSocketError::Handshake)?;
        let config = WebSocketConfig::default()
            .read_buffer_size(4096)
            .write_buffer_size(0)
            .max_write_buffer_size(
                max_message_bytes
                    .checked_add(1024)
                    .ok_or(WebSocketError::Budget)?,
            )
            .max_message_size(Some(max_message_bytes))
            .max_frame_size(Some(max_message_bytes));
        let (mut socket, response) = tokio_tungstenite::client_async_with_config(
            request,
            Input::new(stream, max_message_bytes),
            Some(config),
        )
        .await
        .map_err(|_| WebSocketError::Handshake)?;
        let headers = response.headers();
        if headers.contains_key("sec-websocket-extensions")
            || headers.contains_key("sec-websocket-protocol")
            || ["upgrade", "connection", "sec-websocket-accept"]
                .iter()
                .any(|name| headers.get_all(*name).iter().count() != 1)
        {
            return Err(WebSocketError::Handshake);
        }
        socket
            .get_ref()
            .check()
            .map_err(|_| WebSocketError::Unavailable)?;
        socket.get_mut().enable();
        Ok(Self {
            socket,
            received: None,
            failed: false,
            limit: max_message_bytes,
            _permit: permit,
        })
    }

    pub async fn send_text(&mut self, text: &str) -> Result<(), WebSocketError> {
        if self.failed {
            return Err(WebSocketError::Unavailable);
        }
        self.failed = true;
        let header = match text.len() {
            0..=125 => 6,
            126..=65535 => 8,
            _ => 14,
        };
        if text
            .len()
            .checked_add(header)
            .is_none_or(|wire| wire > self.limit)
        {
            return Err(WebSocketError::MessageLimit);
        }
        poll_fn(|cx| {
            self.socket
                .get_ref()
                .check()
                .map_err(|_| WebSocketError::Unavailable)?;
            Pin::new(&mut self.socket).poll_ready(cx).map_err(classify)
        })
        .await?;
        Pin::new(&mut self.socket)
            .start_send(Message::text(text))
            .map_err(classify)?;
        poll_fn(|cx| {
            self.socket
                .get_ref()
                .check()
                .map_err(|_| WebSocketError::Unavailable)?;
            Pin::new(&mut self.socket).poll_flush(cx).map_err(classify)
        })
        .await?;
        self.failed = false;
        Ok(())
    }

    /// Send an empty close, then drive the peer reply and EOF. The enclosing
    /// attempt supplies the finite deadline; cancellation drops this consumer.
    pub async fn close(mut self) -> Result<(), WebSocketError> {
        if self.failed {
            return Err(WebSocketError::Unavailable);
        }
        if self.limit < 6 {
            return Err(WebSocketError::MessageLimit);
        }
        self.socket.get_mut().start_close();
        poll_fn(|cx| {
            self.socket
                .get_ref()
                .check()
                .map_err(|_| WebSocketError::Unavailable)?;
            Pin::new(&mut self.socket).poll_close(cx).map_err(classify)
        })
        .await?;
        while self.receive().await?.is_some() {}
        Ok(())
    }

    pub async fn receive(&mut self) -> Result<Option<ReceivedMessage<'_>>, WebSocketError> {
        if self.failed {
            return Err(WebSocketError::Unavailable);
        }
        // A cancelled read cannot be resumed as an unrelated protocol operation.
        self.failed = true;
        self.received = None;
        let next = poll_fn(|cx| {
            self.socket
                .get_ref()
                .check()
                .map_err(|_| WebSocketError::Unavailable)?;
            Pin::new(&mut self.socket)
                .poll_next(cx)
                .map(|item| item.transpose().map_err(classify))
        })
        .await?;
        if matches!(next, Some(Message::Ping(_) | Message::Close(_))) {
            // Automatic replies otherwise wait for a later read. Application
            // backpressure must not leave the peer waiting for Pong or Close.
            poll_fn(|cx| {
                self.socket
                    .get_ref()
                    .check()
                    .map_err(|_| WebSocketError::Unavailable)?;
                Pin::new(&mut self.socket).poll_flush(cx).map_err(classify)
            })
            .await?;
        }
        self.received = next;
        self.failed = false;
        match self.received.as_ref() {
            Some(Message::Text(text)) => Ok(Some(ReceivedMessage::Text(text.as_str()))),
            Some(Message::Binary(bytes)) => Ok(Some(ReceivedMessage::Binary(bytes))),
            Some(Message::Ping(bytes)) => Ok(Some(ReceivedMessage::Ping(bytes))),
            Some(Message::Pong(bytes)) => Ok(Some(ReceivedMessage::Pong(bytes))),
            Some(Message::Close(close)) => Ok(Some(ReceivedMessage::Close {
                code: close.as_ref().map(|close| close.code.into()),
                reason: close.as_ref().map_or("", |close| close.reason.as_str()),
            })),
            Some(Message::Frame(_)) => {
                self.failed = true;
                Err(WebSocketError::Protocol)
            }
            None => Ok(None),
        }
    }
}
