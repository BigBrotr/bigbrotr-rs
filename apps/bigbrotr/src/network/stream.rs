use super::lifetime::Gate;
use bigbrotr_protocol::endpoint::Endpoint;
use std::{
    io,
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpStream,
};

/// Guarded stream with no escape to its socket or TLS configuration. It is only
/// borrowed inside DirectConnector::run; the owner drops it before releasing its
/// connection reservation, including on cancellation or a failed callback.
pub struct DirectStream<'owner> {
    pub(super) inner: Guarded<'owner, Wire<'owner>>,
    pub(super) endpoint: Endpoint,
    pub(super) peer: SocketAddr,
}
impl DirectStream<'_> {
    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }
    pub const fn peer_addr(&self) -> SocketAddr {
        self.peer
    }
}

pub(super) enum Wire<'owner> {
    Plain(Guarded<'owner, TcpStream>),
    Tls(Box<tokio_rustls::client::TlsStream<Guarded<'owner, TcpStream>>>),
}
pub(super) struct Guarded<'owner, S> {
    pub socket: S,
    pub gate: Gate<'owner>,
}
impl<S: AsyncRead + Unpin> AsyncRead for Guarded<'_, S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.gate.check_io()?;
        Pin::new(&mut this.socket).poll_read(cx, buf)
    }
}
impl<S: AsyncWrite + Unpin> AsyncWrite for Guarded<'_, S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        this.gate.check_io()?;
        Pin::new(&mut this.socket).poll_write(cx, buf)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.gate.check_io()?;
        Pin::new(&mut this.socket).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.gate.check_io()?;
        Pin::new(&mut this.socket).poll_shutdown(cx)
    }
}
impl AsyncRead for Wire<'_> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(socket) => Pin::new(socket).poll_read(cx, buf),
            Self::Tls(socket) => Pin::new(socket.as_mut()).poll_read(cx, buf),
        }
    }
}
impl AsyncWrite for Wire<'_> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(socket) => Pin::new(socket).poll_write(cx, buf),
            Self::Tls(socket) => Pin::new(socket.as_mut()).poll_write(cx, buf),
        }
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(socket) => Pin::new(socket).poll_flush(cx),
            Self::Tls(socket) => Pin::new(socket.as_mut()).poll_flush(cx),
        }
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(socket) => Pin::new(socket).poll_shutdown(cx),
            Self::Tls(socket) => Pin::new(socket.as_mut()).poll_shutdown(cx),
        }
    }
}
impl AsyncRead for DirectStream<'_> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}
impl AsyncWrite for DirectStream<'_> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}
