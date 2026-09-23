//! The socket, not the handler queue, is the final authority dispatch boundary.
use bigbrotr::runtime::policy_authority::{ActionWindow, DeliveryWindow};
use std::{
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpStream,
};

#[derive(Clone)]
pub(super) struct DeliveryGate(Arc<Mutex<Option<DeliveryWindow>>>);
impl DeliveryGate {
    pub fn new(window: DeliveryWindow) -> Self {
        Self(Arc::new(Mutex::new(Some(window))))
    }
    pub fn renew(&self, window: DeliveryWindow) -> io::Result<()> {
        let mut current = self.0.lock().map_err(|_| denied())?;
        if current
            .as_mut()
            .is_none_or(|old| old.renew(window).is_err())
        {
            *current = None;
            return Err(denied());
        }
        Ok(())
    }
    pub fn revoke(&self) {
        if let Ok(mut current) = self.0.lock() {
            *current = None;
        }
    }
    fn check(&self) -> io::Result<()> {
        let mut current = self.0.lock().map_err(|_| denied())?;
        if current
            .as_mut()
            .is_none_or(|window| window.check().is_err())
        {
            *current = None;
            return Err(denied());
        }
        Ok(())
    }
}
fn denied() -> io::Error {
    io::Error::new(
        io::ErrorKind::PermissionDenied,
        "private delivery authority unavailable",
    )
}

enum ActionState {
    Unbound,
    Bound {
        action: ActionWindow,
        pin: Option<ActionWindow>,
    },
    Closed,
}
#[derive(Clone)]
pub(crate) struct ActionGate(Arc<Mutex<ActionState>>);
impl ActionGate {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(ActionState::Unbound)))
    }
    pub fn bind(&self, mut window: ActionWindow) -> io::Result<()> {
        let mut state = self.0.lock().map_err(|_| denied())?;
        if !matches!(*state, ActionState::Unbound) || window.check().is_err() {
            *state = ActionState::Closed;
            return Err(denied());
        }
        *state = ActionState::Bound {
            action: window,
            pin: None,
        };
        Ok(())
    }
    pub fn constrain_pin(&self, window: Option<ActionWindow>) -> io::Result<()> {
        let mut state = self.0.lock().map_err(|_| denied())?;
        if let (ActionState::Bound { action, pin }, Some(mut window)) = (&mut *state, window)
            && pin.is_none()
            && action.check().is_ok()
            && window.check().is_ok()
        {
            *pin = Some(window);
            return Ok(());
        }
        *state = ActionState::Closed;
        Err(denied())
    }
    pub fn check(&self) -> io::Result<()> {
        let mut state = self.0.lock().map_err(|_| denied())?;
        let valid = match &mut *state {
            ActionState::Unbound => true,
            ActionState::Bound { action, pin } => {
                action.check().is_ok() && pin.as_mut().is_none_or(|pin| pin.check().is_ok())
            }
            ActionState::Closed => false,
        };
        if valid {
            Ok(())
        } else {
            *state = ActionState::Closed;
            Err(denied())
        }
    }
}

pub(super) struct GuardedSocket {
    socket: TcpStream,
    gate: DeliveryGate,
    action: ActionGate,
}
impl GuardedSocket {
    pub fn new(socket: TcpStream, gate: DeliveryGate, action: ActionGate) -> Self {
        Self {
            socket,
            gate,
            action,
        }
    }
}
impl AsyncRead for GuardedSocket {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().socket).poll_read(cx, buf)
    }
}
impl AsyncWrite for GuardedSocket {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        this.gate.check()?;
        this.action.check()?;
        Pin::new(&mut this.socket).poll_write(cx, buf)
    }
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        this.gate.check()?;
        this.action.check()?;
        Pin::new(&mut this.socket).poll_write_vectored(cx, bufs)
    }
    fn is_write_vectored(&self) -> bool {
        self.socket.is_write_vectored()
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.gate.check()?;
        this.action.check()?;
        Pin::new(&mut this.socket).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // The concrete raw TCP socket has no user-space buffer to flush here.
        Pin::new(&mut self.get_mut().socket).poll_shutdown(cx)
    }
}
