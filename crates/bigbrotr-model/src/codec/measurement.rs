use super::{CodecError, FactReader};
use crate::{
    Digest256, DurationUs, EventId, FactRef, Measurement, MeasurementAttemptId, ObservedTime,
    measurement::*,
};

impl FactReader<'_> {
    fn measurement_moment(&mut self) -> Result<MeasurementMoment, CodecError> {
        self.array(2)?;
        Ok(MeasurementMoment {
            observed_at: self.nullable(|r| Ok(ObservedTime::new(r.decoder.i64()?)))?,
            elapsed_us: self.nullable(|r| Ok(DurationUs::new(r.decoder.u64()?)))?,
        })
    }
    fn measurement_phase(&mut self) -> Result<MeasurementPhase, CodecError> {
        Ok(match self.decoder.u8()? {
            0 => MeasurementPhase::Dns,
            1 => MeasurementPhase::SocketSetup,
            2 => MeasurementPhase::TcpConnect,
            3 => MeasurementPhase::Tls,
            4 => MeasurementPhase::WebSocket,
            5 => MeasurementPhase::RequestWrite,
            6 => MeasurementPhase::ResponseRead,
            7 => MeasurementPhase::Verification,
            8 => MeasurementPhase::Teardown,
            _ => return Err(CodecError::IncompatibleSchema),
        })
    }
    fn measurement_observed(&mut self) -> Result<ObservedAttempt, CodecError> {
        self.array(4)?;
        self.array(3)?;
        let destination = match self.decoder.u8()? {
            0 => ConnectDestination::DnsResolver,
            1 => ConnectDestination::Relay,
            _ => return Err(CodecError::IncompatibleSchema),
        };
        let size = self.decoder.array()?;
        let result = match (size, self.decoder.u8()?) {
            (Some(1), 0) => ConnectCallResult::Accepted,
            (Some(2), 1) => ConnectCallResult::ImmediateError(match self.decoder.u8()? {
                0 => ConnectErrorClass::Permission,
                1 => ConnectErrorClass::Resources,
                2 => ConnectErrorClass::Address,
                3 => ConnectErrorClass::Refused,
                4 => ConnectErrorClass::Unreachable,
                5 => ConnectErrorClass::TimedOut,
                6 => ConnectErrorClass::Other,
                _ => return Err(CodecError::IncompatibleSchema),
            }),
            _ => return Err(CodecError::IncompatibleSchema),
        };
        let observed_at = self.nullable(|r| Ok(ObservedTime::new(r.decoder.i64()?)))?;
        self.array(3)?;
        let open = OpenObservation {
            tcp_connected: self.nullable(Self::measurement_moment)?,
            tls_completed: self.nullable(Self::measurement_moment)?,
            websocket_opened: self.nullable(Self::measurement_moment)?,
        };
        self.array(6)?;
        let read = ReadObservation {
            request_flushed: self.nullable(Self::measurement_moment)?,
            first_valid_event: self.nullable(|r| {
                r.array(3)?;
                Ok(FirstValidEvent {
                    event_id: EventId::from_bytes(r.fixed()?),
                    received: r.measurement_moment()?,
                    verified: r.measurement_moment()?,
                })
            })?,
            eose: self.nullable(Self::measurement_moment)?,
            eose_prefix_verified: self.decoder.bool()?,
            auth_observed: self.decoder.bool()?,
            correlated_closed: self.decoder.bool()?,
        };
        self.array(6)?;
        let counters = MeasurementCounters {
            rx_physical_frames: self.decoder.u64()?,
            rx_messages: self.decoder.u64()?,
            event_candidates: self.decoder.u64()?,
            rx_ws_bytes: self.decoder.u64()?,
            rx_payload_bytes: self.decoder.u64()?,
            tx_ws_bytes: self.decoder.u64()?,
        };
        Ok(ObservedAttempt {
            execution: ObservedExecution {
                destination,
                result,
                observed_at,
            },
            open,
            read,
            counters,
        })
    }
    fn measurement_terminal(&mut self) -> Result<TerminalMeasurement, CodecError> {
        self.array(4)?;
        let observed = self.measurement_observed()?;
        let ended = self.measurement_moment()?;
        let size = self.decoder.array()?;
        let termination = match (size, self.decoder.u8()?) {
            (Some(1), 0) => MeasurementTermination::Eose,
            (Some(1), 1) => MeasurementTermination::Closed,
            (Some(2), 2) => MeasurementTermination::Deadline(self.measurement_phase()?),
            (Some(2), 3) => {
                MeasurementTermination::AuthorityInterruption(self.measurement_phase()?)
            }
            (Some(2), 4) => MeasurementTermination::Cancelled(self.measurement_phase()?),
            (Some(3), 5) => {
                let phase = self.measurement_phase()?;
                let class = match self.decoder.u8()? {
                    0 => MeasurementFailure::LocalPermission,
                    1 => MeasurementFailure::LocalResources,
                    2 => MeasurementFailure::LocalConfiguration,
                    3 => MeasurementFailure::DnsUnavailable,
                    4 => MeasurementFailure::ConnectFailed,
                    5 => MeasurementFailure::TlsFailed,
                    6 => MeasurementFailure::UpgradeFailed,
                    7 => MeasurementFailure::Transport,
                    8 => MeasurementFailure::Decode,
                    9 => MeasurementFailure::InvalidEvent,
                    10 => MeasurementFailure::Disconnected,
                    _ => return Err(CodecError::IncompatibleSchema),
                };
                MeasurementTermination::Failed { phase, class }
            }
            (Some(2), 6) => MeasurementTermination::Limit(match self.decoder.u8()? {
                0 => MeasurementLimit::MessageFramedBytes,
                1 => MeasurementLimit::NormalizedEventBytes,
                2 => MeasurementLimit::RxPhysicalFrames,
                3 => MeasurementLimit::RxMessages,
                4 => MeasurementLimit::EventCandidates,
                5 => MeasurementLimit::RxWsBytes,
                6 => MeasurementLimit::TxWsBytes,
                7 => MeasurementLimit::DnsBounds,
                _ => return Err(CodecError::IncompatibleSchema),
            }),
            _ => return Err(CodecError::IncompatibleSchema),
        };
        let teardown = match self.decoder.u8()? {
            0 => TeardownObservation::NotAttempted,
            1 => TeardownObservation::Completed,
            2 => TeardownObservation::Failed,
            3 => TeardownObservation::Interrupted,
            _ => return Err(CodecError::IncompatibleSchema),
        };
        Ok(TerminalMeasurement {
            observed,
            ended,
            termination,
            teardown,
        })
    }
    pub(super) fn measurement(&mut self) -> Result<Measurement, CodecError> {
        self.charge(Measurement::required_allocation_bytes())?;
        self.array(5)?;
        let attempt =
            MeasurementAttemptId::try_from(self.fixed()?).map_err(|_| CodecError::InvalidValue)?;
        self.array(5)?;
        self.array(2)?;
        if self.decoder.u8()? != 0 {
            return Err(CodecError::IncompatibleSchema);
        }
        let context = MeasurementContext {
            subject: MeasurementSubject::RelayEndpoint(self.reference()?),
            observer_id: self.reference()?,
            route_id: self.reference()?,
            access_scope_id: self.reference()?,
            method_id: self.reference()?,
        };
        self.array(24)?;
        for field in PassiveReadProfile::WIRE_FIELDS {
            if self.decoder.u64()? != field {
                return Err(CodecError::IncompatibleSchema);
            }
        }
        let profile = PassiveReadProfile::first(DurationUs::new(self.decoder.u64()?))
            .map_err(|_| CodecError::InvalidValue)?;
        let clock = self.clock()?;
        let size = self.decoder.array()?;
        let state = match (size, self.decoder.u8()?) {
            (Some(1), 0) => MeasurementState::ExecutionUnknown,
            (Some(2), 1) => MeasurementState::CompletionUnknown(self.measurement_observed()?),
            (Some(2), 2) => MeasurementState::Terminal(self.measurement_terminal()?),
            (Some(3), 3) => {
                self.array(2)?;
                let prior_unknown = FactRef {
                    source_record_id: self.source()?,
                    payload_digest: Digest256::from_bytes(self.fixed()?),
                };
                MeasurementState::Resolution {
                    prior_unknown,
                    terminal: self.measurement_terminal()?,
                }
            }
            _ => return Err(CodecError::IncompatibleSchema),
        };
        Measurement::new(attempt, context, profile, clock, state).map_err(|error| match error {
            MeasurementError::AllocationFailed => CodecError::AllocationFailed,
            _ => CodecError::InvalidValue,
        })
    }
}
