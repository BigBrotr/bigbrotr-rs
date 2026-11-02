//! Fixed, attempt-owned transport diagnostics. Constructors and snapshots grant
//! no dispatch, measurement, clock-quality or scientific evidence authority.
use super::{TransportError, WebSocketError};
use std::{
    alloc::Layout,
    io,
    sync::{Arc, Mutex, atomic::AtomicUsize},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::time::Instant;

pub const OBSERVED_OPEN_TIMEOUT: Duration = Duration::from_secs(10);
pub const OBSERVED_CHECK_TIMEOUT: Duration = Duration::from_secs(15);
pub const OBSERVED_MAX_RX_PHYSICAL_FRAMES: u64 = 64;
pub const OBSERVED_MAX_RX_LOGICAL_MESSAGES: u64 = 32;
/// Enforced by the owning event-verification consumer, not the WS decoder.
pub const OBSERVED_MAX_EVENT_CANDIDATES: u64 = 3;
pub const OBSERVED_MAX_RX_WS_FRAMED_BYTES: u64 = 4 * 1024 * 1024;
pub const OBSERVED_MAX_TX_WS_FRAMED_BYTES: u64 = 64 * 1024;
pub const OBSERVED_MAX_MESSAGE_FRAMED_BYTES: usize = 2 * 1024 * 1024;

/// Local observation after the named operation, never kernel arrival time.
/// A missing wall value preserves the observation with an unusable wall clock.
#[derive(Clone, Copy, Debug)]
pub struct TransportInstant {
    pub monotonic: Instant,
    pub unix_us: Option<i64>,
}
impl TransportInstant {
    pub(crate) fn now() -> Self {
        let monotonic = Instant::now();
        let unix_us = signed_unix_us(SystemTime::now());
        Self { monotonic, unix_us }
    }
}
fn signed_unix_us(value: SystemTime) -> Option<i64> {
    let nanos = match value.duration_since(UNIX_EPOCH) {
        Ok(duration) => i128::try_from(duration.as_nanos()).ok()?,
        Err(error) => i128::try_from(error.duration().as_nanos())
            .ok()?
            .checked_neg()?,
    };
    // Match ReceiveClock: floor signed time, so epoch - 1ns is -1us.
    i64::try_from(nanos.div_euclid(1000)).ok()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FirstIoTarget {
    ResolverConnect,
    RelayConnect,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectErrorClass {
    Refused,
    Unreachable,
    Denied,
    AddressUnavailable,
    Resource,
    Other,
}
impl ConnectErrorClass {
    pub(super) fn of(error: &io::Error) -> Self {
        match error.kind() {
            io::ErrorKind::ConnectionRefused => Self::Refused,
            io::ErrorKind::NetworkUnreachable | io::ErrorKind::HostUnreachable => Self::Unreachable,
            io::ErrorKind::PermissionDenied => Self::Denied,
            io::ErrorKind::AddrNotAvailable | io::ErrorKind::AddrInUse => Self::AddressUnavailable,
            io::ErrorKind::OutOfMemory | io::ErrorKind::StorageFull => Self::Resource,
            _ => Self::Other,
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectSubmission {
    Connected,
    InProgress,
    ImmediateError(ConnectErrorClass),
}
/// The externally-directed connect syscall returned. Accepted submission is
/// distinct from an immediate error; neither proves a packet or remote receipt.
#[derive(Clone, Copy, Debug)]
pub struct FirstIo {
    pub target: FirstIoTarget,
    pub returned_at: TransportInstant,
    pub submission: ConnectSubmission,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectFailureStage {
    Preparation,
    Immediate,
    Registration,
    Readiness,
    SocketStatus,
    Completion,
    Peer,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectFailure {
    pub target: FirstIoTarget,
    pub stage: ConnectFailureStage,
    pub class: ConnectErrorClass,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObservedSessionLimit {
    RxPhysicalFrames,
    RxLogicalMessages,
    RxWsFramedBytes,
    TxWsFramedBytes,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObservedStop {
    Authority,
    Cancelled,
    OpenDeadline,
    CheckDeadline,
    Clock,
    Session(ObservedSessionLimit),
}

#[derive(Clone, Copy, Debug)]
pub struct WebSocketCounters {
    /// First bytes consumed for physical headers, including a refused header.
    /// Detection of frame 65 can leave 65 starts but only 64 complete
    /// headers. Fragments and controls each start a physical frame.
    pub rx_physical_frames_started: u64,
    /// Complete raw header claims, including invalid opcodes/flags and claims
    /// refused before forwarding. A partial refused header is not complete.
    pub rx_complete_headers: u64,
    pub rx_last_declared_payload_bytes: Option<u64>,
    pub rx_last_header_bytes: Option<u8>,
    /// Sum of complete headers plus their declared payload lengths, including
    /// rejected claims. This potential length can exceed the configured cap;
    /// it is not actual reception. None means arithmetic overflow.
    pub rx_declared_ws_framed_bytes: Option<u64>,
    /// Actual lower plaintext WS bytes read, including buffered future/rejected
    /// traffic. HTTP upgrade bytes are excluded; a coalesced WS tail is included.
    pub rx_ws_framed_bytes_read: u64,
    /// Actual bytes consumed by the physical-frame guard, including rejected
    /// headers/close codes. Can be smaller than read bytes when input is buffered.
    pub rx_ws_framed_bytes_consumed: u64,
    /// Actual payload octets consumed by the guard, excluding every physical
    /// header. Includes fragments, controls and partial/rejected Close codes or
    /// payloads; buffered unread payload and declared potential lengths add none.
    pub rx_ws_payload_bytes_consumed: u64,
    /// Actual bytes forwarded to the WS codec after the guard's checks. Can be
    /// smaller than consumed bytes when a header/close code is refused.
    pub rx_ws_framed_bytes_forwarded: u64,
    /// Complete messages emitted by the decoder, including controls, Binary and
    /// unrelated application messages. Fragments form one logical message;
    /// rejected frames and EOF produce none. Counted before automatic replies
    /// and application parsing, even if that later reply/parse fails.
    pub rx_logical_messages_delivered: u64,
    /// Actual successful WS plaintext writes, including automatic control and
    /// teardown traffic; excludes HTTP. This is not lower TLS/network proof.
    pub tx_ws_framed_bytes_written: u64,
}
impl Default for WebSocketCounters {
    fn default() -> Self {
        Self {
            rx_physical_frames_started: 0,
            rx_complete_headers: 0,
            rx_last_declared_payload_bytes: None,
            rx_last_header_bytes: None,
            rx_declared_ws_framed_bytes: Some(0),
            rx_ws_framed_bytes_read: 0,
            rx_ws_framed_bytes_consumed: 0,
            rx_ws_payload_bytes_consumed: 0,
            rx_ws_framed_bytes_forwarded: 0,
            rx_logical_messages_delivered: 0,
            tx_ws_framed_bytes_written: 0,
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct TransportSnapshot {
    pub connector_entered: Option<TransportInstant>,
    pub requested_deadline: Option<Instant>,
    /// Minimum of the existing caller deadline and the nominal 15s check bound.
    /// The independent suspend-aware policy window can expire sooner.
    pub effective_check_deadline: Option<Instant>,
    pub open_deadline: Option<Instant>,
    pub first_io: Option<FirstIo>,
    pub resolver_connect_calls: u32,
    pub relay_connect_calls: u32,
    pub last_connect_failure: Option<ConnectFailure>,
    pub relay_connected: Option<TransportInstant>,
    pub tls_first_lower_read: Option<TransportInstant>,
    pub tls_first_lower_write: Option<TransportInstant>,
    pub tls_completed: Option<TransportInstant>,
    pub websocket_upgrade_validated: Option<TransportInstant>,
    /// First text write and flush complete. The owning passive consumer binds
    /// these bytes to its one REQ; this transport does not parse or authorize it.
    pub first_text_write_flushed: Option<TransportInstant>,
    /// Complete WS decoder delivery, before automatic replies and JSON parsing/
    /// event verification. A later automatic reply failure does not erase it.
    pub first_message_delivered: Option<TransportInstant>,
    pub last_message_delivered: Option<TransportInstant>,
    pub websocket: WebSocketCounters,
    pub stop: Option<ObservedStop>,
}
#[derive(Default)]
struct State {
    snapshot: TransportSnapshot,
    tls_active: bool,
}

/// Keep this handle outside the operation future. Error, cancellation and drop
/// cannot erase anchors already captured into its one fixed-size state.
#[derive(Clone, Default)]
pub struct TransportTrace(Arc<Mutex<State>>);
impl TransportTrace {
    pub fn new() -> Self {
        Self::default()
    }
    pub(crate) fn same_capture(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
    /// Arc allocation layout: its two atomic reference counts followed by the
    /// full Mutex<State>, including the mutex/poison state and alignment padding.
    /// Excludes allocator bookkeeping and this handle's inline pointer.
    pub const fn retained_bytes() -> usize {
        match Layout::new::<[AtomicUsize; 2]>().extend(Layout::new::<Mutex<State>>()) {
            Ok((layout, _)) => layout.pad_to_align().size(),
            Err(_) => panic!("fixed transport trace allocation layout"),
        }
    }
    fn update<T>(&self, apply: impl FnOnce(&mut State) -> T) -> T {
        // Diagnostics do not revive authority. Preserve observations even if a
        // prior diagnostic user panicked while holding this private mutex.
        apply(&mut self.0.lock().unwrap_or_else(|error| error.into_inner()))
    }
    pub fn snapshot(&self) -> TransportSnapshot {
        self.update(|state| state.snapshot)
    }
    pub(super) fn begin(&self, deadline: Instant) -> Result<(), TransportError> {
        let at = TransportInstant::now();
        self.update(|state| {
            if state.snapshot.connector_entered.is_some() {
                return Err(TransportError::Operation);
            }
            let open = at
                .monotonic
                .checked_add(OBSERVED_OPEN_TIMEOUT)
                .ok_or(TransportError::Operation)?;
            let check = at
                .monotonic
                .checked_add(OBSERVED_CHECK_TIMEOUT)
                .ok_or(TransportError::Operation)?;
            state.snapshot.connector_entered = Some(at);
            state.snapshot.requested_deadline = Some(deadline);
            state.snapshot.effective_check_deadline = Some(deadline.min(check));
            state.snapshot.open_deadline = Some(open);
            state.snapshot.websocket.rx_declared_ws_framed_bytes = Some(0);
            if at.unix_us.is_none() {
                state.snapshot.stop = Some(ObservedStop::Clock);
                return Err(TransportError::Operation);
            }
            Ok(())
        })
    }
    pub(super) fn prepare_connect(&self) -> Result<(), TransportError> {
        if TransportInstant::now().unix_us.is_none() {
            self.stop(ObservedStop::Clock);
            return Err(TransportError::Operation);
        }
        Ok(())
    }
    pub(super) fn stop(&self, reason: ObservedStop) {
        self.update(|state| {
            state.snapshot.stop.get_or_insert(reason);
        });
    }
    pub(super) fn check_deadline(&self) -> Result<(), TransportError> {
        self.check_deadline_at(Instant::now())
    }
    fn check_deadline_at(&self, now: Instant) -> Result<(), TransportError> {
        self.update(|state| {
            let snapshot = &mut state.snapshot;
            if snapshot.stop == Some(ObservedStop::Clock) {
                return Err(TransportError::Operation);
            }
            let reason = if snapshot.websocket_upgrade_validated.is_none()
                && snapshot.open_deadline.is_some_and(|end| {
                    now >= end
                        && snapshot
                            .effective_check_deadline
                            .is_none_or(|check| end <= check)
                }) {
                Some(ObservedStop::OpenDeadline)
            } else if snapshot
                .effective_check_deadline
                .is_some_and(|end| now >= end)
            {
                Some(ObservedStop::CheckDeadline)
            } else {
                None
            };
            if let Some(reason) = reason {
                snapshot.stop.get_or_insert(reason);
                Err(TransportError::Deadline)
            } else {
                Ok(())
            }
        })
    }
    pub(super) fn connect_returned(&self, target: FirstIoTarget, submission: ConnectSubmission) {
        let returned_at = TransportInstant::now();
        self.update(|state| {
            state.snapshot.first_io.get_or_insert(FirstIo {
                target,
                returned_at,
                submission,
            });
            let count = match target {
                FirstIoTarget::ResolverConnect => &mut state.snapshot.resolver_connect_calls,
                FirstIoTarget::RelayConnect => &mut state.snapshot.relay_connect_calls,
            };
            *count = count.saturating_add(1);
        });
    }
    pub(super) fn connect_failed(
        &self,
        target: FirstIoTarget,
        stage: ConnectFailureStage,
        error: &io::Error,
    ) {
        self.update(|state| {
            state.snapshot.last_connect_failure = Some(ConnectFailure {
                target,
                stage,
                class: ConnectErrorClass::of(error),
            })
        });
    }
    pub(super) fn relay_connected(&self) {
        self.update(|state| state.snapshot.relay_connected = Some(TransportInstant::now()));
    }
    pub(super) fn tls_phase(&self) -> TlsPhase {
        self.update(|state| state.tls_active = true);
        TlsPhase(self.clone())
    }
    pub(super) fn tls_io(&self, write: bool, bytes: usize) {
        if bytes == 0 {
            return;
        }
        self.update(|state| {
            if state.tls_active {
                let slot = if write {
                    &mut state.snapshot.tls_first_lower_write
                } else {
                    &mut state.snapshot.tls_first_lower_read
                };
                slot.get_or_insert_with(TransportInstant::now);
            }
        });
    }
    pub(super) fn tls_completed(&self) {
        self.update(|state| state.snapshot.tls_completed = Some(TransportInstant::now()));
    }
    pub(super) fn upgrade_validated(&self) -> Result<(), TransportError> {
        let at = TransportInstant::now();
        self.check_deadline_at(at.monotonic)?;
        self.update(|state| state.snapshot.websocket_upgrade_validated = Some(at));
        Ok(())
    }
    pub(super) fn text_flushed(&self) {
        self.update(|state| {
            state
                .snapshot
                .first_text_write_flushed
                .get_or_insert_with(TransportInstant::now);
        });
    }
    fn limit(state: &mut State, limit: ObservedSessionLimit) -> WebSocketError {
        state
            .snapshot
            .stop
            .get_or_insert(ObservedStop::Session(limit));
        WebSocketError::SessionLimit
    }
    pub(super) fn start_frame(&self) -> Result<(), WebSocketError> {
        self.update(|state| {
            state.snapshot.websocket.rx_physical_frames_started = state
                .snapshot
                .websocket
                .rx_physical_frames_started
                .saturating_add(1);
            if state.snapshot.websocket.rx_physical_frames_started > OBSERVED_MAX_RX_PHYSICAL_FRAMES
            {
                Err(Self::limit(state, ObservedSessionLimit::RxPhysicalFrames))
            } else {
                Ok(())
            }
        })
    }
    pub(super) fn declared_frame(&self, payload: u64, header: usize) -> Result<(), WebSocketError> {
        self.update(|state| {
            let counters = &mut state.snapshot.websocket;
            counters.rx_complete_headers += 1;
            counters.rx_last_declared_payload_bytes = Some(payload);
            counters.rx_last_header_bytes = u8::try_from(header).ok();
            counters.rx_declared_ws_framed_bytes = counters
                .rx_declared_ws_framed_bytes
                .and_then(|total| total.checked_add(header as u64))
                .and_then(|total| total.checked_add(payload));
            if counters
                .rx_declared_ws_framed_bytes
                .is_none_or(|total| total > OBSERVED_MAX_RX_WS_FRAMED_BYTES)
            {
                Err(Self::limit(state, ObservedSessionLimit::RxWsFramedBytes))
            } else {
                Ok(())
            }
        })
    }
    pub(super) fn rx_remaining(&self) -> usize {
        self.update(|state| {
            OBSERVED_MAX_RX_WS_FRAMED_BYTES
                .saturating_sub(state.snapshot.websocket.rx_ws_framed_bytes_read)
                as usize
        })
    }
    pub(super) fn rx_read(&self, bytes: usize) {
        self.update(|state| state.snapshot.websocket.rx_ws_framed_bytes_read += bytes as u64);
    }
    pub(super) fn rx_consumed(&self, bytes: usize) {
        self.update(|state| state.snapshot.websocket.rx_ws_framed_bytes_consumed += bytes as u64);
    }
    pub(super) fn rx_payload_consumed(&self, bytes: usize) {
        self.update(|state| state.snapshot.websocket.rx_ws_payload_bytes_consumed += bytes as u64);
    }
    pub(super) fn rx_forwarded(&self, bytes: usize) {
        self.update(|state| state.snapshot.websocket.rx_ws_framed_bytes_forwarded += bytes as u64);
    }
    pub(super) fn rx_exhausted(&self) -> WebSocketError {
        self.update(|state| Self::limit(state, ObservedSessionLimit::RxWsFramedBytes))
    }
    pub(super) fn tx_allow(&self, bytes: usize) -> Result<(), WebSocketError> {
        self.update(|state| {
            if (bytes as u64)
                > OBSERVED_MAX_TX_WS_FRAMED_BYTES
                    .saturating_sub(state.snapshot.websocket.tx_ws_framed_bytes_written)
            {
                Err(Self::limit(state, ObservedSessionLimit::TxWsFramedBytes))
            } else {
                Ok(())
            }
        })
    }
    pub(super) fn tx_written(&self, bytes: usize) {
        self.update(|state| state.snapshot.websocket.tx_ws_framed_bytes_written += bytes as u64);
    }
    pub(super) fn before_message(&self) -> Result<(), WebSocketError> {
        self.update(|state| {
            if state.snapshot.websocket.rx_logical_messages_delivered
                >= OBSERVED_MAX_RX_LOGICAL_MESSAGES
            {
                Err(Self::limit(state, ObservedSessionLimit::RxLogicalMessages))
            } else {
                Ok(())
            }
        })
    }
    pub(super) fn message_delivered(&self) {
        self.update(|state| {
            let at = TransportInstant::now();
            state.snapshot.websocket.rx_logical_messages_delivered += 1;
            state.snapshot.first_message_delivered.get_or_insert(at);
            state.snapshot.last_message_delivered = Some(at);
        });
    }
}
pub(super) struct TlsPhase(TransportTrace);
impl Drop for TlsPhase {
    fn drop(&mut self) {
        self.0.update(|state| state.tls_active = false);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signed_utc_microseconds_floor_both_sides_of_epoch() {
        for (nanos, expected) in [(0, 0), (1, 0), (999, 0), (1000, 1), (1001, 1)] {
            assert_eq!(
                signed_unix_us(UNIX_EPOCH + Duration::from_nanos(nanos)),
                Some(expected)
            );
        }
        for (nanos, expected) in [(1, -1), (999, -1), (1000, -1), (1001, -2)] {
            assert_eq!(
                signed_unix_us(UNIX_EPOCH - Duration::from_nanos(nanos)),
                Some(expected)
            );
        }
    }

    #[test]
    fn retained_charge_includes_the_whole_mutex_and_arc_counts() {
        let charged = TransportTrace::retained_bytes();
        assert!(
            charged >= std::mem::size_of::<Mutex<State>>() + 2 * std::mem::size_of::<AtomicUsize>()
        );
        assert_eq!(charged % std::mem::align_of::<Mutex<State>>(), 0);
        assert_eq!(charged % std::mem::align_of::<AtomicUsize>(), 0);
    }

    #[test]
    fn open_bound_starts_at_entry_and_only_validated_upgrade_disarms_it() {
        let trace = TransportTrace::new();
        trace
            .begin(Instant::now() + Duration::from_secs(60))
            .unwrap();
        let snapshot = trace.snapshot();
        let started = snapshot.connector_entered.unwrap().monotonic;
        assert_eq!(
            snapshot.open_deadline.unwrap() - started,
            Duration::from_secs(10)
        );
        assert_eq!(
            snapshot.effective_check_deadline.unwrap() - started,
            Duration::from_secs(15)
        );
        assert!(
            trace
                .check_deadline_at(started + Duration::from_secs(9))
                .is_ok()
        );
        assert_eq!(
            trace.check_deadline_at(started + Duration::from_secs(20)),
            Err(TransportError::Deadline)
        );
        assert_eq!(trace.snapshot().stop, Some(ObservedStop::OpenDeadline));
        let trace = TransportTrace::new();
        trace
            .begin(Instant::now() + Duration::from_secs(60))
            .unwrap();
        let started = trace.snapshot().connector_entered.unwrap().monotonic;
        trace.upgrade_validated().unwrap();
        assert!(
            trace
                .check_deadline_at(started + Duration::from_secs(10))
                .is_ok()
        );
        assert_eq!(
            trace.check_deadline_at(started + Duration::from_secs(15)),
            Err(TransportError::Deadline)
        );
        assert_eq!(trace.snapshot().stop, Some(ObservedStop::CheckDeadline));
        assert!(
            trace
                .begin(Instant::now() + Duration::from_secs(60))
                .is_err()
        );
        assert_eq!(
            trace.snapshot().connector_entered.unwrap().monotonic,
            started
        );
    }
    #[test]
    fn cumulative_claims_and_overflow_do_not_inflate_any_actual_byte_counter() {
        let trace = TransportTrace::new();
        trace
            .begin(Instant::now() + Duration::from_secs(5))
            .unwrap();
        trace.declared_frame(2 * 1024 * 1024 - 10, 10).unwrap();
        trace.declared_frame(2 * 1024 * 1024 - 10, 10).unwrap();
        assert_eq!(
            trace.declared_frame(0, 2),
            Err(WebSocketError::SessionLimit)
        );
        let captured = trace.snapshot().websocket;
        assert_eq!(
            captured.rx_declared_ws_framed_bytes,
            Some(4 * 1024 * 1024 + 2)
        );
        assert_eq!(
            (
                captured.rx_ws_framed_bytes_read,
                captured.rx_ws_framed_bytes_consumed,
                captured.rx_ws_framed_bytes_forwarded
            ),
            (0, 0, 0)
        );
        let trace = TransportTrace::new();
        trace
            .begin(Instant::now() + Duration::from_secs(5))
            .unwrap();
        assert_eq!(
            trace.declared_frame(u64::MAX, 10),
            Err(WebSocketError::SessionLimit)
        );
        let captured = trace.snapshot().websocket;
        assert_eq!(captured.rx_last_declared_payload_bytes, Some(u64::MAX));
        assert_eq!(captured.rx_declared_ws_framed_bytes, None);
        assert_eq!(captured.rx_ws_framed_bytes_read, 0);
        assert_eq!(captured.rx_ws_payload_bytes_consumed, 0);
    }
}
