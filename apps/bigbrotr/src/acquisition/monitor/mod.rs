//! One already-authorized passive read. RAM metadata is neither a durable start
//! nor admission authority; the connected owner separately fences its delivery.
mod attempt;
mod liveness;

use crate::{
    network::{
        DirectConnector, TransportError, TransportInstant, TransportSnapshot, TransportTrace,
    },
    runtime::{OwnedPermit, ReservedBudget, ResourceBudget, ResourceLimits, ResourceRequest},
};
use bigbrotr_model::acquisition::{Filter, FilterFields};
use bigbrotr_model::{
    ClockQuality, DurationUs, EventId, Measurement, MeasurementAttemptId,
    measurement::{
        MeasurementContext, MeasurementPhase, MeasurementState, MeasurementTermination,
        PassiveReadProfile, TeardownObservation,
    },
};
use bigbrotr_protocol::{EventLimits, endpoint::Endpoint};
use std::{
    fmt,
    sync::{Mutex, MutexGuard},
};

const MIB: u64 = 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaptureError {
    Budget,
    Profile,
    Binding,
    Clock,
    Observation,
    TerminalUnavailable,
}
impl fmt::Display for CaptureError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "passive read capture unavailable: {self:?}")
    }
}
impl std::error::Error for CaptureError {}

/// Immutable attribution supplied after the real owner checks its mappings.
/// Constructing this tuple grants no enrollment, route, clock or socket authority.
pub struct AttemptSpec {
    pub attempt_id: MeasurementAttemptId,
    pub context: MeasurementContext,
    pub clock_quality: ClockQuality,
    pub effective_authority_us: DurationUs,
}

#[derive(Clone, Copy)]
struct ValidEvent {
    id: EventId,
    delivered: TransportInstant,
    verified: TransportInstant,
}
struct State {
    entered: bool,
    invalid_binding: bool,
    phase: Option<MeasurementPhase>,
    request_started: bool,
    candidates: u64,
    first_event: Option<ValidEvent>,
    eose: Option<TransportInstant>,
    prefix_verified: bool,
    auth: bool,
    closed: bool,
    terminal: Option<(MeasurementTermination, TransportInstant)>,
    teardown: TeardownObservation,
    teardown_started: bool,
}
impl Default for State {
    fn default() -> Self {
        Self {
            entered: false,
            invalid_binding: false,
            phase: None,
            request_started: false,
            candidates: 0,
            first_event: None,
            eose: None,
            prefix_verified: false,
            auth: false,
            closed: false,
            terminal: None,
            teardown: TeardownObservation::NotAttempted,
            teardown_started: false,
        }
    }
}

/// Retain outside the guarded connector future, including its error path. No raw
/// EVENT or AUTH challenge escapes; only one fixed observation record survives.
pub struct PreparedPassiveRead {
    template: Measurement,
    endpoint: Endpoint,
    filters: [Filter; 1],
    trace: TransportTrace,
    state: Mutex<State>,
    // Verification is funded before any external IO, never from ambient capacity.
    _verification: OwnedPermit,
    capture: OwnedPermit,
    reservation: ReservedBudget,
}
impl PreparedPassiveRead {
    // Existing owners: connection4MiB, DNS4MiB, WS(8*2+1)=17MiB,
    // session(2+8)=10MiB, verifier8MiB. Fixed metadata/registry/output overlap
    // uses the final128KiB. This is logical admission capacity, not an RSS claim.
    pub const REQUIRED_DATA_BYTES: u64 = 43 * MIB + 128 * 1024;
    pub const REQUIRED_ITEMS: u64 = 8;

    pub fn prepare(
        parent: &ResourceBudget,
        endpoint: &Endpoint,
        spec: AttemptSpec,
    ) -> Result<Self, CaptureError> {
        let reservation = parent
            .try_reserve(ResourceLimits::new(
                Self::REQUIRED_DATA_BYTES,
                Self::REQUIRED_ITEMS,
                0,
                0,
            ))
            .map_err(|_| CaptureError::Budget)?;
        let fixed = size_of::<Self>()
            + size_of::<RetainedCapture>()
            + TransportTrace::retained_bytes()
            + bigbrotr_protocol::endpoint::MAX_ENDPOINT_BYTES
            + 2 * Measurement::required_allocation_bytes()
            + size_of::<u16>();
        let capture = reservation
            .budget()
            .try_acquire(
                ResourceRequest::data(fixed as u64, 1, 0).map_err(|_| CaptureError::Budget)?,
            )
            .map_err(|_| CaptureError::Budget)?;
        let verification = reservation
            .budget()
            .try_acquire(
                ResourceRequest::data(EventLimits::default().parse_bytes as u64, 1, 0)
                    .map_err(|_| CaptureError::Budget)?,
            )
            .map_err(|_| CaptureError::Budget)?;
        let profile = PassiveReadProfile::first(spec.effective_authority_us)
            .map_err(|_| CaptureError::Profile)?;
        // A checked template validates reference shape, not execution/recovery.
        let template = Measurement::new(
            spec.attempt_id,
            spec.context,
            profile,
            spec.clock_quality,
            MeasurementState::ExecutionUnknown,
        )
        .map_err(|_| CaptureError::Profile)?;
        let filter = Filter::new(FilterFields {
            kinds: Some(vec![1]),
            ..FilterFields::default()
        })
        .map_err(|_| CaptureError::Profile)?;
        Ok(Self {
            template,
            endpoint: endpoint.clone(),
            filters: [filter],
            trace: TransportTrace::new(),
            state: Mutex::new(State::default()),
            _verification: verification,
            capture,
            reservation,
        })
    }

    /// Use for this attempt's already-authorized run_observed callback only.
    pub fn connector(&self, shared: &DirectConnector) -> DirectConnector {
        shared.within(&self.reservation)
    }
    /// Read-only binding for the actual guarded connector. The read driver
    /// verifies the stream carries this exact private trace allocation.
    pub fn transport_trace(&self) -> &TransportTrace {
        &self.trace
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|error| error.into_inner())
    }
    fn terminal(&self, reason: MeasurementTermination) {
        self.state()
            .terminal
            .get_or_insert_with(|| (reason, TransportInstant::now()));
    }
    fn phase(&self, phase: MeasurementPhase) {
        self.state().phase = Some(phase);
    }

    /// Consume only after the enclosing connector/Control future has joined.
    /// Its actual outer error supplies an otherwise unobserved terminal cause.
    /// A known first terminal survives a later cleanup/delivery refusal. A trace
    /// with no returned external connect syscall produces no A05 sample.
    #[allow(
        clippy::result_large_err,
        reason = "return fixed observations without allocating on an uncertain IO error path"
    )]
    pub fn finish(
        self,
        outcome: Result<(), TransportError>,
    ) -> Result<Option<CapturedMeasurement>, CaptureFailure> {
        self.finish_inner(Some(outcome))
    }

    /// A bare outer Control refusal supplies no remote/local terminal cause.
    /// Preserve an earlier driver terminal or actual trace stop; otherwise keep
    /// that uncertainty explicit instead of inventing a timeout/transport error.
    #[allow(
        clippy::result_large_err,
        reason = "return fixed observations without allocating on an uncertain IO error path"
    )]
    pub fn finish_unclassified(self) -> Result<Option<CapturedMeasurement>, CaptureFailure> {
        self.finish_inner(None)
    }

    #[allow(
        clippy::result_large_err,
        reason = "return fixed observations without allocating on an uncertain IO error path"
    )]
    fn finish_inner(
        self,
        outcome: Option<Result<(), TransportError>>,
    ) -> Result<Option<CapturedMeasurement>, CaptureFailure> {
        let Self {
            template,
            endpoint,
            filters,
            trace,
            state,
            _verification,
            capture,
            reservation,
        } = self;
        let secure = endpoint.as_str().starts_with("wss://");
        let retained = RetainedCapture {
            template,
            trace: trace.snapshot(),
            state: state
                .into_inner()
                .unwrap_or_else(|error| error.into_inner()),
            permit: capture,
        };
        // The source future has joined. Close its child admission and release
        // completed IO/verifier storage, retaining only the fixed capture grant.
        drop(endpoint);
        drop(filters);
        drop(trace);
        drop(_verification);
        drop(reservation);
        retained.finalize(outcome, secure)
    }
}

/// Immutable first verified EVENT metadata; neither its body nor a retained
/// relay-event/source capability crosses the finalization boundary.
#[derive(Clone, Copy, Debug)]
pub struct CapturedEvent {
    pub event_id: EventId,
    pub delivered: TransportInstant,
    pub verified: TransportInstant,
}

/// Fixed raw prefix retained even if clocks, binding or model validation refuse
/// finalization. It makes no terminal/completion-unknown or recording claim.
#[derive(Clone, Copy, Debug)]
pub struct ReadPrefix {
    pub request_started: bool,
    pub event_candidates: u64,
    pub first_valid_event: Option<CapturedEvent>,
    pub eose: Option<TransportInstant>,
    pub eose_prefix_verified: bool,
    pub auth_observed: bool,
    pub correlated_closed: bool,
}

/// Known fixed observations survive an unresolved finalization with their
/// original grant. No raw event body, AUTH challenge or completed IO owner stays
/// here, and this RAM carrier cannot establish a durable start or authority.
pub struct RetainedCapture {
    template: Measurement,
    trace: TransportSnapshot,
    state: State,
    permit: OwnedPermit,
}
impl RetainedCapture {
    pub fn attempt_id(&self) -> MeasurementAttemptId {
        self.template.attempt_id()
    }
    pub fn context(&self) -> MeasurementContext {
        self.template.context()
    }
    pub fn profile(&self) -> PassiveReadProfile {
        self.template.profile()
    }
    pub fn clock_quality(&self) -> ClockQuality {
        self.template.clock_quality()
    }
    pub fn transport(&self) -> &TransportSnapshot {
        &self.trace
    }
    pub fn phase(&self) -> Option<MeasurementPhase> {
        self.state.phase
    }
    pub fn terminal(&self) -> Option<(MeasurementTermination, TransportInstant)> {
        self.state.terminal
    }
    pub fn teardown(&self) -> TeardownObservation {
        self.state.teardown
    }
    pub fn binding_refused(&self) -> bool {
        self.state.invalid_binding
    }
    pub fn read_prefix(&self) -> ReadPrefix {
        ReadPrefix {
            request_started: self.state.request_started,
            event_candidates: self.state.candidates,
            first_valid_event: self.state.first_event.map(|event| CapturedEvent {
                event_id: event.id,
                delivered: event.delivered,
                verified: event.verified,
            }),
            eose: self.state.eose,
            eose_prefix_verified: self.state.prefix_verified,
            auth_observed: self.state.auth,
            correlated_closed: self.state.closed,
        }
    }
    pub fn reserved_bytes(&self) -> u64 {
        self.permit.request().units().data_bytes()
    }
    pub fn reserved_items(&self) -> u64 {
        self.permit.request().units().items()
    }

    fn refuse(self, kind: CaptureError) -> CaptureFailure {
        CaptureFailure {
            kind,
            observations: self,
        }
    }
    #[allow(
        clippy::result_large_err,
        reason = "return fixed observations without allocating on an uncertain IO error path"
    )]
    fn finalize(
        mut self,
        outcome: Option<Result<(), TransportError>>,
        secure: bool,
    ) -> Result<Option<CapturedMeasurement>, CaptureFailure> {
        if self.state.invalid_binding {
            return Err(self.refuse(CaptureError::Binding));
        }
        if self.trace.first_io.is_none() {
            return Ok(None);
        }
        if self.state.terminal.is_none() {
            let error = match outcome {
                Some(Err(error)) => error,
                Some(Ok(())) => return Err(self.refuse(CaptureError::Binding)),
                None if self.trace.stop.is_some() => TransportError::Operation,
                None => return Err(self.refuse(CaptureError::TerminalUnavailable)),
            };
            let reason = liveness::transport(error, &self.trace, self.state.phase, secure);
            self.state.terminal = Some((reason, TransportInstant::now()));
        }
        if self.state.teardown_started && self.state.teardown == TeardownObservation::NotAttempted {
            self.state.teardown = TeardownObservation::Interrupted;
        }
        let value = match liveness::measurement(&self.template, &self.trace, &self.state) {
            Ok(value) => value,
            Err(kind) => return Err(self.refuse(kind)),
        };
        Ok(Some(CapturedMeasurement {
            value,
            _permit: self.permit,
        }))
    }
}

/// Finalization refusal with all known fixed observations still owned. Dropping
/// this error is an explicit owner decision; receiving it never erases knowledge.
pub struct CaptureFailure {
    kind: CaptureError,
    observations: RetainedCapture,
}
impl CaptureFailure {
    pub fn kind(&self) -> CaptureError {
        self.kind
    }
    pub fn observations(&self) -> &RetainedCapture {
        &self.observations
    }
    pub fn into_parts(self) -> (CaptureError, RetainedCapture) {
        (self.kind, self.observations)
    }
}
impl fmt::Debug for CaptureFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CaptureFailure")
            .field("kind", &self.kind)
            .field("reserved_bytes", &self.observations.reserved_bytes())
            .finish_non_exhaustive()
    }
}
impl fmt::Display for CaptureFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.kind, f)
    }
}
impl std::error::Error for CaptureFailure {}

/// Fixed measurement metadata plus its retained allocation credit. It is never
/// an event-admission/checkpoint result or a current historical-recording grant.
pub struct CapturedMeasurement {
    value: Measurement,
    _permit: OwnedPermit,
}
impl CapturedMeasurement {
    pub fn measurement(&self) -> &Measurement {
        &self.value
    }
    pub fn into_parts(self) -> (Measurement, OwnedPermit) {
        (self.value, self._permit)
    }
    pub fn reserved_bytes(&self) -> u64 {
        self._permit.request().units().data_bytes()
    }
    pub fn reserved_items(&self) -> u64 {
        self._permit.request().units().items()
    }
}

#[cfg(test)]
mod tests;
