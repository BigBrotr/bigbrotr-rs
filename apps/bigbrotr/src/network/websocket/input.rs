//! The upstream codec allocates after a complete header. Withhold that header
//! until its declared payload and actual framing fit the whole-message budget.
#[cfg(test)]
mod tests;

use super::WebSocketError;
use crate::network::DirectStream;
use std::{
    io,
    io::Cursor,
    pin::Pin,
    task::{Context, Poll, ready},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_tungstenite::tungstenite::{
    handshake::{client::Response, machine::TryParse},
    protocol::frame::{
        FrameHeader,
        coding::{CloseCode, Control, Data, OpCode},
    },
};

const HTTP_BYTES: usize = 64 * 1024;
const READ_BYTES: usize = 4096;
const WORK_TURNS: usize = 32;

pub(super) struct Input<'borrow, 'owner> {
    stream: &'borrow mut DirectStream<'owner>,
    bytes: Vec<u8>,
    position: usize,
    http: Http,
    enabled: bool,
    reply_controls: bool,
    reads: usize,
    turns: usize,
    header: [u8; 14],
    header_len: usize,
    frame: Frame,
    message_bytes: Option<u64>,
    limit: u64,
}

enum Http {
    Collect,
    Forward { end: usize },
    Finished,
}

enum Frame {
    Header,
    CloseCode {
        header_size: usize,
        payload: u64,
    },
    ForwardHeader {
        position: usize,
        payload: u64,
        final_data: bool,
    },
    Payload {
        remaining: u64,
        final_data: bool,
    },
}

impl<'borrow, 'owner> Input<'borrow, 'owner> {
    pub(super) fn new(stream: &'borrow mut DirectStream<'owner>, limit: usize) -> Self {
        Self {
            stream,
            bytes: Vec::with_capacity(HTTP_BYTES + READ_BYTES),
            position: 0,
            http: Http::Collect,
            enabled: false,
            reply_controls: true,
            reads: 0,
            turns: WORK_TURNS,
            header: [0; 14],
            header_len: 0,
            frame: Frame::Header,
            message_bytes: None,
            limit: limit as u64,
        }
    }

    pub(super) fn enable(&mut self) {
        self.enabled = true;
    }

    pub(super) fn start_close(&mut self) {
        // Upstream stops automatic replies once our Close is queued. A peer
        // acknowledgement must fit its incoming size, not a nonexistent echo.
        self.reply_controls = false;
    }

    pub(super) fn check(&self) -> io::Result<()> {
        self.stream.inner.gate.check_io()
    }

    fn turn(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if self.turns == 0 {
            self.turns = WORK_TURNS;
            cx.waker().wake_by_ref();
            Poll::Pending
        } else {
            self.turns -= 1;
            Poll::Ready(())
        }
    }

    fn read_more(&mut self, cx: &mut Context<'_>, handshake: bool) -> Poll<io::Result<bool>> {
        ready!(self.turn(cx));
        self.check()?;
        let mut chunk = [0; READ_BYTES];
        let mut output = ReadBuf::new(&mut chunk);
        ready!(Pin::new(&mut *self.stream).poll_read(cx, &mut output))?;
        let input = output.filled();
        if input.is_empty() {
            return Poll::Ready(Ok(false));
        }
        if handshake {
            self.reads += 1;
            let length = self.bytes.len() + input.len();
            // Coalescing must not hide the original reads from handshake work bounds.
            if length > HTTP_BYTES + READ_BYTES
                || self.reads > 512
                || (self.reads > 64 && self.reads * 128 > length)
            {
                return Poll::Ready(Err(WebSocketError::Handshake.into()));
            }
        } else {
            debug_assert_eq!(self.position, self.bytes.len());
            self.bytes.clear();
            self.position = 0;
        }
        self.bytes.extend_from_slice(input);
        Poll::Ready(Ok(true))
    }

    fn accept_header(&mut self, header: FrameHeader, payload: u64, size: usize) -> io::Result<()> {
        if header.mask.is_some() || header.rsv1 || header.rsv2 || header.rsv3 {
            return Err(WebSocketError::Protocol.into());
        }
        let framed = payload
            .checked_add(size as u64)
            .ok_or(WebSocketError::MessageLimit)?;
        let final_data = match header.opcode {
            OpCode::Data(data) => {
                let previous = match (data, self.message_bytes) {
                    (Data::Continue, Some(previous)) => previous,
                    (Data::Text | Data::Binary, None) => 0,
                    _ => return Err(WebSocketError::Protocol.into()),
                };
                let total = previous
                    .checked_add(framed)
                    .ok_or(WebSocketError::MessageLimit)?;
                if total > self.limit {
                    return Err(WebSocketError::MessageLimit.into());
                }
                self.message_bytes = Some(total);
                header.is_final
            }
            OpCode::Control(Control::Close | Control::Ping | Control::Pong) => {
                if !header.is_final || payload > 125 {
                    return Err(WebSocketError::Protocol.into());
                }
                if framed > self.limit {
                    return Err(WebSocketError::MessageLimit.into());
                }
                // Automatic client replies add a four-byte mask. Enforce the
                // same wire limit before the codec queues Pong or Close.
                if self.reply_controls
                    && header.opcode != OpCode::Control(Control::Pong)
                    && payload + 6 > self.limit
                {
                    return Err(WebSocketError::MessageLimit.into());
                }
                false
            }
            _ => return Err(WebSocketError::Protocol.into()),
        };
        if header.opcode == OpCode::Control(Control::Close) {
            if payload == 1 {
                return Err(WebSocketError::Protocol.into());
            }
            if payload >= 2 {
                // Unmasked server headers occupy at most ten bytes. Reuse the
                // fixed arena for the code before upstream can normalize it.
                debug_assert!(size + 2 <= self.header.len());
                self.frame = Frame::CloseCode {
                    header_size: size,
                    payload,
                };
                return Ok(());
            }
        }
        self.frame = Frame::ForwardHeader {
            position: 0,
            payload,
            final_data,
        };
        Ok(())
    }

    fn finish_frame(&mut self, final_data: bool) {
        if final_data {
            self.message_bytes = None;
        }
        self.header_len = 0;
        self.frame = Frame::Header;
    }

    fn read(&mut self, cx: &mut Context<'_>, output: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        self.check()?;
        if output.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        loop {
            ready!(self.turn(cx));
            match self.http {
                Http::Collect => {
                    if !ready!(self.read_more(cx, true))? {
                        return Poll::Ready(Err(io::ErrorKind::UnexpectedEof.into()));
                    }
                    match Response::try_parse(&self.bytes).map_err(|_| WebSocketError::Handshake)? {
                        Some((end, response)) => {
                            drop(response);
                            if end > HTTP_BYTES {
                                return Poll::Ready(Err(WebSocketError::Handshake.into()));
                            }
                            self.http = Http::Forward { end };
                        }
                        None if self.bytes.len() >= HTTP_BYTES => {
                            return Poll::Ready(Err(WebSocketError::Handshake.into()));
                        }
                        None => continue,
                    }
                }
                Http::Forward { end } => {
                    let count = (end - self.position).min(output.remaining());
                    output.put_slice(&self.bytes[self.position..self.position + count]);
                    self.position += count;
                    if self.position == end {
                        self.http = Http::Finished;
                    }
                    return Poll::Ready(Ok(()));
                }
                Http::Finished if !self.enabled => {
                    return Poll::Ready(Err(WebSocketError::Handshake.into()));
                }
                Http::Finished => match self.frame {
                    Frame::CloseCode {
                        header_size,
                        payload,
                    } => {
                        if self.header_len == header_size + 2 {
                            let code = u16::from_be_bytes([
                                self.header[header_size],
                                self.header[header_size + 1],
                            ]);
                            if !CloseCode::from(code).is_allowed() {
                                return Poll::Ready(Err(WebSocketError::Protocol.into()));
                            }
                            self.frame = Frame::ForwardHeader {
                                position: 0,
                                payload: payload - 2,
                                final_data: false,
                            };
                            continue;
                        }
                        if self.position == self.bytes.len() && !ready!(self.read_more(cx, false))?
                        {
                            return Poll::Ready(Err(io::ErrorKind::UnexpectedEof.into()));
                        }
                        self.header[self.header_len] = self.bytes[self.position];
                        self.header_len += 1;
                        self.position += 1;
                    }
                    Frame::Header => {
                        let mut cursor = Cursor::new(&self.header[..self.header_len]);
                        if let Some((header, payload)) =
                            FrameHeader::parse(&mut cursor).map_err(|_| WebSocketError::Protocol)?
                        {
                            // Upstream accepts non-minimal encodings. Count consumed bytes,
                            // never FrameHeader::len(), which canonicalizes them.
                            let size = cursor.position() as usize;
                            self.accept_header(header, payload, size)?;
                            continue;
                        }
                        if self.header_len == self.header.len() {
                            return Poll::Ready(Err(WebSocketError::Protocol.into()));
                        }
                        if self.position == self.bytes.len() && !ready!(self.read_more(cx, false))?
                        {
                            return Poll::Ready(if self.header_len == 0 {
                                Ok(()) // Let the codec distinguish a closing handshake from reset.
                            } else {
                                Err(io::ErrorKind::UnexpectedEof.into())
                            });
                        }
                        self.header[self.header_len] = self.bytes[self.position];
                        self.header_len += 1;
                        self.position += 1;
                    }
                    Frame::ForwardHeader {
                        position,
                        payload,
                        final_data,
                    } => {
                        let count = (self.header_len - position).min(output.remaining());
                        output.put_slice(&self.header[position..position + count]);
                        self.frame = if position + count == self.header_len {
                            Frame::Payload {
                                remaining: payload,
                                final_data,
                            }
                        } else {
                            Frame::ForwardHeader {
                                position: position + count,
                                payload,
                                final_data,
                            }
                        };
                        return Poll::Ready(Ok(()));
                    }
                    Frame::Payload {
                        remaining: 0,
                        final_data,
                    } => self.finish_frame(final_data),
                    Frame::Payload {
                        remaining,
                        final_data,
                    } => {
                        if self.position == self.bytes.len() && !ready!(self.read_more(cx, false))?
                        {
                            return Poll::Ready(Err(io::ErrorKind::UnexpectedEof.into()));
                        }
                        let count = (self.bytes.len() - self.position)
                            .min(output.remaining())
                            .min(remaining as usize);
                        output.put_slice(&self.bytes[self.position..self.position + count]);
                        self.position += count;
                        self.frame = Frame::Payload {
                            remaining: remaining - count as u64,
                            final_data,
                        };
                        return Poll::Ready(Ok(()));
                    }
                },
            }
        }
    }
}

impl AsyncRead for Input<'_, '_> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.get_mut().read(cx, output)
    }
}
impl AsyncWrite for Input<'_, '_> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut *self.get_mut().stream).poll_write(cx, bytes)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.get_mut().stream).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.get_mut().stream).poll_shutdown(cx)
    }
}
