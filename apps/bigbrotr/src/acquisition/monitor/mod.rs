//! One already-authorized passive read. RAM metadata is neither a durable start
//! nor admission authority; the connected owner separately fences its delivery.
mod attempt;
mod liveness;

use crate::{
    network::{DirectConnector, TransportError, TransportInstant, TransportTrace},
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
    pub fn finish(
        self,
        outcome: Result<(), TransportError>,
    ) -> Result<Option<CapturedMeasurement>, CaptureError> {
        self.finish_inner(Some(outcome))
    }

    /// A bare outer Control refusal supplies no remote/local terminal cause.
    /// Preserve an earlier driver terminal or actual trace stop; otherwise keep
    /// that uncertainty explicit instead of inventing a timeout/transport error.
    pub fn finish_unclassified(self) -> Result<Option<CapturedMeasurement>, CaptureError> {
        self.finish_inner(None)
    }

    fn finish_inner(
        self,
        outcome: Option<Result<(), TransportError>>,
    ) -> Result<Option<CapturedMeasurement>, CaptureError> {
        let snapshot = self.trace.snapshot();
        let mut state = self.state();
        if state.invalid_binding {
            return Err(CaptureError::Binding);
        }
        if snapshot.first_io.is_none() {
            return Ok(None);
        }
        if state.terminal.is_none() {
            let error = match outcome {
                Some(Err(error)) => error,
                Some(Ok(())) => return Err(CaptureError::Binding),
                None if snapshot.stop.is_some() => TransportError::Operation,
                None => return Err(CaptureError::TerminalUnavailable),
            };
            let reason = liveness::transport(
                error,
                &snapshot,
                state.phase,
                self.endpoint.as_str().starts_with("wss://"),
            );
            state.terminal = Some((reason, TransportInstant::now()));
        }
        if state.teardown_started && state.teardown == TeardownObservation::NotAttempted {
            state.teardown = TeardownObservation::Interrupted;
        }
        let value = liveness::measurement(&self.template, &snapshot, &state)?;
        drop(state);
        Ok(Some(CapturedMeasurement {
            value,
            _permit: self.capture,
        }))
    }
}

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
}

#[cfg(test)]
mod tests;
