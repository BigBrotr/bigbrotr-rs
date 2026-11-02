//! Closed private A05 row transport and exact canonical reconstruction. These
//! structural values grant no measurement, principal, cut or retention authority.
use crate::archive_cut::{CutObservation, CutRead};
use bigbrotr_model::{
    ClockQuality, ConnectCallResult, ConnectDestination, ConnectErrorClass, Digest256, DurationUs,
    EventId, Fact, FactPayload, FactRef, FirstValidEvent, Measurement, MeasurementAttemptId,
    MeasurementContext, MeasurementCounters, MeasurementFailure, MeasurementLimit,
    MeasurementMoment, MeasurementPhase, MeasurementState, MeasurementSubject,
    MeasurementTermination, ObservedAttempt, ObservedExecution, ObservedTime, OpenObservation,
    PassiveReadProfile, QualifiedRef, ReadObservation, SourceRecordId, TeardownObservation,
    TerminalMeasurement,
};
use serde::{Deserialize, Serialize};

fn required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

pub const FORMAT: &str = "bigbrotr-native-measurement/1";
/// Fixed typed rows contain no source text or variable child lists. This bound
/// covers their private JSON representation, not native vector/engine memory.
pub const MAX_ROW_BYTES: usize = 16 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MeasurementRead {
    pub cut: CutRead,
    pub fact: FactRef,
}
impl MeasurementRead {
    pub fn validate(&self, max_facts: usize) -> Result<(), MeasurementReadError> {
        self.cut.validate(max_facts).map_err(|error| match error {
            crate::archive_receipt::ExpectationError::ResourceLimit => {
                MeasurementReadError::ResourceLimit
            }
            _ => MeasurementReadError::InvalidInput,
        })?;
        if self.cut.snapshot_id.is_none() || !contains(&self.cut, self.fact.source_record_id) {
            return Err(MeasurementReadError::InvalidInput);
        }
        Ok(())
    }
}
pub fn contains(cut: &CutRead, source: SourceRecordId) -> bool {
    source.dataset_id == cut.dataset_id
        && cut
            .bounds
            .binary_search_by_key(&(source.shard_id, source.owner_epoch.get()), |b| b.stream())
            .is_ok_and(|i| source.sequence.get() <= cut.bounds[i].complete_through)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MeasurementReadError {
    InvalidInput,
    Missing,
    Mismatch,
    ResourceLimit,
    Native,
}
impl std::fmt::Display for MeasurementReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "private measurement observation failed: {self:?}")
    }
}
impl std::error::Error for MeasurementReadError {}
type Error = MeasurementReadError;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MeasurementObservation {
    pub format: String,
    pub cut: CutObservation,
    pub fact: MeasurementFactRow,
}
impl MeasurementObservation {
    pub fn validate(&self, read: &MeasurementRead, max_facts: usize) -> Result<Fact, Error> {
        read.validate(max_facts)?;
        self.cut
            .validate(&read.cut, max_facts)
            .map_err(|_| Error::Mismatch)?;
        if self.format != FORMAT
            || self.cut.family_counts[crate::archive_batch::ARCHIVE_FAMILY_COUNT - 1] == 0
        {
            return Err(Error::Mismatch);
        }
        let fact = self.fact.to_fact()?;
        if fact.fact_ref() != read.fact {
            return Err(Error::Mismatch);
        }
        Ok(fact)
    }
}

/// Complete original envelope; the digest is recomputed from the typed payload
/// rather than accepted merely because a stored digest column matches.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MeasurementFactRow {
    pub source_record_id: SourceRecordId,
    pub record_schema_version: u16,
    pub record_kind: String,
    pub visibility_scope_id: QualifiedRef,
    pub payload_digest: Digest256,
    pub recorded_at: ObservedTime,
    pub payload: MeasurementRow,
}
impl MeasurementFactRow {
    pub fn to_fact(&self) -> Result<Fact, Error> {
        if self.record_schema_version != 1 || self.record_kind != "measurement" {
            return Err(Error::Mismatch);
        }
        let fact = Fact::new(
            self.source_record_id,
            self.visibility_scope_id,
            self.recorded_at,
            FactPayload::Measurement(self.payload.to_measurement()?),
        )
        .map_err(|_| Error::Mismatch)?;
        if fact.payload_digest() != self.payload_digest {
            return Err(Error::Mismatch);
        }
        Ok(fact)
    }
    pub fn from_fact(fact: &Fact) -> Result<Self, Error> {
        let FactPayload::Measurement(value) = fact.payload() else {
            return Err(Error::Mismatch);
        };
        Ok(Self {
            source_record_id: fact.source_record_id(),
            record_schema_version: fact.record_schema_version(),
            record_kind: "measurement".into(),
            visibility_scope_id: fact.visibility_scope_id(),
            payload_digest: fact.payload_digest(),
            recorded_at: fact.recorded_at(),
            payload: MeasurementRow::from_measurement(value),
        })
    }
}

/// Closed physical interpretation. All nullable variant fields are checked when
/// reconstructing the model; extra fields and unsupported enum codes refuse.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MeasurementRow {
    pub attempt_id: MeasurementAttemptId,
    pub subject_kind: u16,
    pub endpoint_id: QualifiedRef,
    pub observer_id: QualifiedRef,
    pub route_id: QualifiedRef,
    pub access_scope_id: QualifiedRef,
    pub method_id: QualifiedRef,
    /// Exact declared order in measurement.json; not independently mutable knobs.
    pub profile: [u64; 24],
    pub clock_quality: ClockQuality,
    pub state_kind: u16,
    #[serde(deserialize_with = "required_option")]
    pub prior_unknown: Option<FactRef>,
    #[serde(deserialize_with = "required_option")]
    pub observed: Option<ObservedRow>,
    #[serde(deserialize_with = "required_option")]
    pub ended: Option<MomentRow>,
    #[serde(deserialize_with = "required_option")]
    pub termination: Option<TerminationRow>,
    #[serde(deserialize_with = "required_option")]
    pub teardown: Option<u16>,
}

fn profile(authority: u64) -> [u64; 24] {
    use PassiveReadProfile as P;
    [
        1,
        P::BASE_CADENCE_US,
        P::OVERALL_TIMEOUT_US,
        P::OPEN_TIMEOUT_US,
        1,
        1,
        1,
        1,
        1,
        1,
        0,
        P::MAX_MESSAGE_FRAMED_BYTES,
        P::MAX_NORMALIZED_EVENT_BYTES,
        P::MAX_RX_PHYSICAL_FRAMES,
        P::MAX_RX_MESSAGES,
        P::MAX_EVENT_CANDIDATES,
        P::MAX_RX_WS_BYTES,
        P::MAX_TX_WS_BYTES,
        2_000_000,
        9,
        4096,
        64,
        0,
        authority,
    ]
}
impl MeasurementRow {
    pub fn from_measurement(value: &Measurement) -> Self {
        let context = value.context();
        let (state_kind, prior_unknown, observed, terminal) = match value.state() {
            MeasurementState::ExecutionUnknown => (0, None, None, None),
            MeasurementState::CompletionUnknown(observed) => (1, None, Some(observed), None),
            MeasurementState::Terminal(terminal) => {
                (2, None, Some(terminal.observed), Some(terminal))
            }
            MeasurementState::Resolution {
                prior_unknown,
                terminal,
            } => (
                3,
                Some(prior_unknown),
                Some(terminal.observed),
                Some(terminal),
            ),
        };
        Self {
            attempt_id: value.attempt_id(),
            subject_kind: 0,
            endpoint_id: context.endpoint_id(),
            observer_id: context.observer_id,
            route_id: context.route_id,
            access_scope_id: context.access_scope_id,
            method_id: context.method_id,
            profile: profile(value.profile().effective_authority_us().get()),
            clock_quality: value.clock_quality(),
            state_kind,
            prior_unknown,
            observed: observed.map(ObservedRow::from),
            ended: terminal.map(|v| v.ended.into()),
            termination: terminal.map(|v| v.termination.into()),
            teardown: terminal.map(|v| v.teardown as u16),
        }
    }
    pub fn to_measurement(self) -> Result<Measurement, Error> {
        if self.subject_kind != 0 || self.profile != profile(self.profile[23]) {
            return Err(Error::Mismatch);
        }
        let profile = PassiveReadProfile::first(DurationUs::new(self.profile[23]))
            .map_err(|_| Error::Mismatch)?;
        let context = MeasurementContext {
            subject: MeasurementSubject::RelayEndpoint(self.endpoint_id),
            observer_id: self.observer_id,
            route_id: self.route_id,
            access_scope_id: self.access_scope_id,
            method_id: self.method_id,
        };
        let state = match (
            self.state_kind,
            self.prior_unknown,
            self.observed,
            self.ended,
            self.termination,
            self.teardown,
        ) {
            (0, None, None, None, None, None) => MeasurementState::ExecutionUnknown,
            (1, None, Some(observed), None, None, None) => {
                MeasurementState::CompletionUnknown(observed.into_observed()?)
            }
            (
                kind @ (2 | 3),
                prior,
                Some(observed),
                Some(ended),
                Some(termination),
                Some(teardown),
            ) => {
                let terminal = TerminalMeasurement {
                    observed: observed.into_observed()?,
                    ended: ended.into(),
                    termination: termination.into_termination()?,
                    teardown: match teardown {
                        0 => TeardownObservation::NotAttempted,
                        1 => TeardownObservation::Completed,
                        2 => TeardownObservation::Failed,
                        3 => TeardownObservation::Interrupted,
                        _ => return Err(Error::Mismatch),
                    },
                };
                match (kind, prior) {
                    (2, None) => MeasurementState::Terminal(terminal),
                    (3, Some(prior_unknown)) => MeasurementState::Resolution {
                        prior_unknown,
                        terminal,
                    },
                    _ => return Err(Error::Mismatch),
                }
            }
            _ => return Err(Error::Mismatch),
        };
        Measurement::new(self.attempt_id, context, profile, self.clock_quality, state)
            .map_err(|_| Error::Mismatch)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MomentRow {
    #[serde(deserialize_with = "required_option")]
    pub observed_at: Option<ObservedTime>,
    #[serde(deserialize_with = "required_option")]
    pub elapsed_us: Option<DurationUs>,
}
impl From<MeasurementMoment> for MomentRow {
    fn from(value: MeasurementMoment) -> Self {
        Self {
            observed_at: value.observed_at,
            elapsed_us: value.elapsed_us,
        }
    }
}
impl From<MomentRow> for MeasurementMoment {
    fn from(value: MomentRow) -> Self {
        Self {
            observed_at: value.observed_at,
            elapsed_us: value.elapsed_us,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionRow {
    pub destination: u16,
    pub result: u16,
    #[serde(deserialize_with = "required_option")]
    pub error: Option<u16>,
    #[serde(deserialize_with = "required_option")]
    pub observed_at: Option<ObservedTime>,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpenRow {
    #[serde(deserialize_with = "required_option")]
    pub tcp_connected: Option<MomentRow>,
    #[serde(deserialize_with = "required_option")]
    pub tls_completed: Option<MomentRow>,
    #[serde(deserialize_with = "required_option")]
    pub websocket_opened: Option<MomentRow>,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventRow {
    pub event_id: EventId,
    pub received: MomentRow,
    pub verified: MomentRow,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadRow {
    #[serde(deserialize_with = "required_option")]
    pub request_flushed: Option<MomentRow>,
    #[serde(deserialize_with = "required_option")]
    pub first_valid_event: Option<EventRow>,
    #[serde(deserialize_with = "required_option")]
    pub eose: Option<MomentRow>,
    pub eose_prefix_verified: bool,
    pub auth_observed: bool,
    pub correlated_closed: bool,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CountersRow {
    pub rx_physical_frames: u64,
    pub rx_messages: u64,
    pub event_candidates: u64,
    pub rx_ws_bytes: u64,
    pub rx_payload_bytes: u64,
    pub tx_ws_bytes: u64,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservedRow {
    pub execution: ExecutionRow,
    pub open: OpenRow,
    pub read: ReadRow,
    pub counters: CountersRow,
}

impl From<ObservedAttempt> for ObservedRow {
    fn from(value: ObservedAttempt) -> Self {
        let execution = ExecutionRow {
            destination: value.execution.destination as u16,
            result: match value.execution.result {
                ConnectCallResult::Accepted => 0,
                ConnectCallResult::ImmediateError(_) => 1,
            },
            error: match value.execution.result {
                ConnectCallResult::Accepted => None,
                ConnectCallResult::ImmediateError(class) => Some(class as u16),
            },
            observed_at: value.execution.observed_at,
        };
        let open = value.open;
        let read = value.read;
        let c = value.counters;
        Self {
            execution,
            open: OpenRow {
                tcp_connected: open.tcp_connected.map(Into::into),
                tls_completed: open.tls_completed.map(Into::into),
                websocket_opened: open.websocket_opened.map(Into::into),
            },
            read: ReadRow {
                request_flushed: read.request_flushed.map(Into::into),
                first_valid_event: read.first_valid_event.map(|v| EventRow {
                    event_id: v.event_id,
                    received: v.received.into(),
                    verified: v.verified.into(),
                }),
                eose: read.eose.map(Into::into),
                eose_prefix_verified: read.eose_prefix_verified,
                auth_observed: read.auth_observed,
                correlated_closed: read.correlated_closed,
            },
            counters: CountersRow {
                rx_physical_frames: c.rx_physical_frames,
                rx_messages: c.rx_messages,
                event_candidates: c.event_candidates,
                rx_ws_bytes: c.rx_ws_bytes,
                rx_payload_bytes: c.rx_payload_bytes,
                tx_ws_bytes: c.tx_ws_bytes,
            },
        }
    }
}
impl ObservedRow {
    fn into_observed(self) -> Result<ObservedAttempt, Error> {
        let destination = match self.execution.destination {
            0 => ConnectDestination::DnsResolver,
            1 => ConnectDestination::Relay,
            _ => return Err(Error::Mismatch),
        };
        let result = match (self.execution.result, self.execution.error) {
            (0, None) => ConnectCallResult::Accepted,
            (1, Some(value)) => ConnectCallResult::ImmediateError(match value {
                0 => ConnectErrorClass::Permission,
                1 => ConnectErrorClass::Resources,
                2 => ConnectErrorClass::Address,
                3 => ConnectErrorClass::Refused,
                4 => ConnectErrorClass::Unreachable,
                5 => ConnectErrorClass::TimedOut,
                6 => ConnectErrorClass::Other,
                _ => return Err(Error::Mismatch),
            }),
            _ => return Err(Error::Mismatch),
        };
        let read = self.read;
        let c = self.counters;
        Ok(ObservedAttempt {
            execution: ObservedExecution {
                destination,
                result,
                observed_at: self.execution.observed_at,
            },
            open: OpenObservation {
                tcp_connected: self.open.tcp_connected.map(Into::into),
                tls_completed: self.open.tls_completed.map(Into::into),
                websocket_opened: self.open.websocket_opened.map(Into::into),
            },
            read: ReadObservation {
                request_flushed: read.request_flushed.map(Into::into),
                first_valid_event: read.first_valid_event.map(|v| FirstValidEvent {
                    event_id: v.event_id,
                    received: v.received.into(),
                    verified: v.verified.into(),
                }),
                eose: read.eose.map(Into::into),
                eose_prefix_verified: read.eose_prefix_verified,
                auth_observed: read.auth_observed,
                correlated_closed: read.correlated_closed,
            },
            counters: MeasurementCounters {
                rx_physical_frames: c.rx_physical_frames,
                rx_messages: c.rx_messages,
                event_candidates: c.event_candidates,
                rx_ws_bytes: c.rx_ws_bytes,
                rx_payload_bytes: c.rx_payload_bytes,
                tx_ws_bytes: c.tx_ws_bytes,
            },
        })
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TerminationRow {
    pub kind: u16,
    #[serde(deserialize_with = "required_option")]
    pub phase: Option<u16>,
    #[serde(deserialize_with = "required_option")]
    pub failure: Option<u16>,
    #[serde(deserialize_with = "required_option")]
    pub limit_code: Option<u16>,
}
impl From<MeasurementTermination> for TerminationRow {
    fn from(value: MeasurementTermination) -> Self {
        let (kind, phase, failure, limit_code) = match value {
            MeasurementTermination::Eose => (0, None, None, None),
            MeasurementTermination::Closed => (1, None, None, None),
            MeasurementTermination::Deadline(phase) => (2, Some(phase as u16), None, None),
            MeasurementTermination::AuthorityInterruption(phase) => {
                (3, Some(phase as u16), None, None)
            }
            MeasurementTermination::Cancelled(phase) => (4, Some(phase as u16), None, None),
            MeasurementTermination::Failed { phase, class } => {
                (5, Some(phase as u16), Some(class as u16), None)
            }
            MeasurementTermination::Limit(limit) => (6, None, None, Some(limit as u16)),
        };
        Self {
            kind,
            phase,
            failure,
            limit_code,
        }
    }
}
impl TerminationRow {
    fn into_termination(self) -> Result<MeasurementTermination, Error> {
        fn phase(value: u16) -> Result<MeasurementPhase, Error> {
            Ok(match value {
                0 => MeasurementPhase::Dns,
                1 => MeasurementPhase::SocketSetup,
                2 => MeasurementPhase::TcpConnect,
                3 => MeasurementPhase::Tls,
                4 => MeasurementPhase::WebSocket,
                5 => MeasurementPhase::RequestWrite,
                6 => MeasurementPhase::ResponseRead,
                7 => MeasurementPhase::Verification,
                8 => MeasurementPhase::Teardown,
                _ => return Err(Error::Mismatch),
            })
        }
        Ok(
            match (self.kind, self.phase, self.failure, self.limit_code) {
                (0, None, None, None) => MeasurementTermination::Eose,
                (1, None, None, None) => MeasurementTermination::Closed,
                (2, Some(value), None, None) => MeasurementTermination::Deadline(phase(value)?),
                (3, Some(value), None, None) => {
                    MeasurementTermination::AuthorityInterruption(phase(value)?)
                }
                (4, Some(value), None, None) => MeasurementTermination::Cancelled(phase(value)?),
                (5, Some(value), Some(class), None) => MeasurementTermination::Failed {
                    phase: phase(value)?,
                    class: match class {
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
                        _ => return Err(Error::Mismatch),
                    },
                },
                (6, None, None, Some(value)) => MeasurementTermination::Limit(match value {
                    0 => MeasurementLimit::MessageFramedBytes,
                    1 => MeasurementLimit::NormalizedEventBytes,
                    2 => MeasurementLimit::RxPhysicalFrames,
                    3 => MeasurementLimit::RxMessages,
                    4 => MeasurementLimit::EventCandidates,
                    5 => MeasurementLimit::RxWsBytes,
                    6 => MeasurementLimit::TxWsBytes,
                    7 => MeasurementLimit::DnsBounds,
                    _ => return Err(Error::Mismatch),
                }),
                _ => return Err(Error::Mismatch),
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bigbrotr_model::codec::FactDecodeLimits;

    fn literals() -> serde_json::Value {
        serde_json::from_str(include_str!(
            "../../bigbrotr-model/tests/fixtures/measurement-v1.json"
        ))
        .unwrap()
    }
    fn bytes(text: &str) -> Vec<u8> {
        text.as_bytes()
            .as_chunks::<2>()
            .0
            .iter()
            .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap())
            .collect()
    }
    fn literal(index: usize) -> Fact {
        let fixture = literals();
        let row = &fixture["valid"][index];
        Fact::decode_preimage(
            &bytes(row["preimage_hex"].as_str().unwrap()),
            row["sha256"].as_str().unwrap().parse().unwrap(),
            FactDecodeLimits::default(),
        )
        .unwrap()
    }

    #[test]
    fn all_independent_a05_vectors_roundtrip_exact_original_canonical_bytes() {
        for vector in literals()["valid"].as_array().unwrap() {
            let expected = bytes(vector["preimage_hex"].as_str().unwrap());
            let original = Fact::decode_preimage(
                &expected,
                vector["sha256"].as_str().unwrap().parse().unwrap(),
                FactDecodeLimits::default(),
            )
            .unwrap();
            let row = MeasurementFactRow::from_fact(&original).unwrap();
            let wire = serde_json::to_vec(&row).unwrap();
            assert!(wire.len() <= MAX_ROW_BYTES);
            let actual: MeasurementFactRow = serde_json::from_slice(&wire).unwrap();
            assert_eq!(
                actual.to_fact().unwrap().encode_preimage(8192).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn actual_typed_payload_source_and_null_presence_cannot_hide_behind_stored_digest() {
        let original = MeasurementFactRow::from_fact(&literal(2)).unwrap();
        for change in 0..12 {
            let mut bad = original.clone();
            match change {
                0 => bad.record_kind = "event-receive".into(),
                1 => bad.record_schema_version = 2,
                2 => bad.payload.subject_kind = 1,
                3 => bad.payload.profile[13] += 1,
                4 => bad.payload.state_kind = 99,
                5 => bad.payload.prior_unknown = Some(literal(0).fact_ref()),
                6 => bad.payload.ended = None,
                7 => bad.payload.termination.as_mut().unwrap().phase = Some(0),
                8 => bad.payload.teardown = Some(99),
                9 => bad.payload.observed.as_mut().unwrap().counters.rx_ws_bytes += 1,
                10 => bad.source_record_id.sequence = bigbrotr_model::Sequence::new(99).unwrap(),
                11 => bad.recorded_at = ObservedTime::new(i64::MIN),
                _ => unreachable!(),
            }
            assert!(bad.to_fact().is_err(), "change {change}");
        }
        // Presence of a request is scientific even when both independent clock
        // samples are unknown. It cannot collapse to a missing request.
        let mut row = original;
        row.payload.observed.as_mut().unwrap().read.request_flushed = Some(MomentRow {
            observed_at: None,
            elapsed_us: None,
        });
        let encoded = serde_json::to_value(&row).unwrap();
        assert_eq!(
            encoded["payload"]["observed"]["read"]["request_flushed"],
            serde_json::json!({"observed_at":null,"elapsed_us":null})
        );
        let mut missing = row.clone();
        missing
            .payload
            .observed
            .as_mut()
            .unwrap()
            .read
            .request_flushed = None;
        assert_ne!(serde_json::to_value(missing).unwrap(), encoded);
        let mut unknown = encoded.clone();
        unknown["payload"]["unexpected"] = true.into();
        assert!(serde_json::from_value::<MeasurementFactRow>(unknown).is_err());
        let mut missing_null = encoded.clone();
        missing_null["payload"]
            .as_object_mut()
            .unwrap()
            .remove("prior_unknown");
        assert!(serde_json::from_value::<MeasurementFactRow>(missing_null).is_err());
        let mut missing_sample = encoded.clone();
        missing_sample["payload"]["observed"]["read"]["request_flushed"]
            .as_object_mut()
            .unwrap()
            .remove("observed_at");
        assert!(serde_json::from_value::<MeasurementFactRow>(missing_sample).is_err());
        for profile in [serde_json::json!([1, 2]), serde_json::json!(vec![0; 25])] {
            let mut wrong = encoded.clone();
            wrong["payload"]["profile"] = profile;
            assert!(serde_json::from_value::<MeasurementFactRow>(wrong).is_err());
        }
    }

    #[test]
    fn enum_fields_refuse_unknown_or_cross_variant_parameters() {
        let observed = MeasurementFactRow::from_fact(&literal(2))
            .unwrap()
            .payload
            .observed
            .unwrap();
        for change in 0..4 {
            let mut bad = observed;
            match change {
                0 => bad.execution.destination = 2,
                1 => bad.execution.result = 2,
                2 => bad.execution.error = Some(0),
                3 => {
                    bad.execution.result = 1;
                    bad.execution.error = Some(7);
                }
                _ => unreachable!(),
            }
            assert!(bad.into_observed().is_err());
        }
        for bad in [
            TerminationRow {
                kind: 7,
                phase: None,
                failure: None,
                limit_code: None,
            },
            TerminationRow {
                kind: 2,
                phase: Some(9),
                failure: None,
                limit_code: None,
            },
            TerminationRow {
                kind: 5,
                phase: Some(0),
                failure: Some(11),
                limit_code: None,
            },
            TerminationRow {
                kind: 6,
                phase: None,
                failure: None,
                limit_code: Some(8),
            },
            TerminationRow {
                kind: 0,
                phase: Some(0),
                failure: None,
                limit_code: None,
            },
        ] {
            assert!(bad.into_termination().is_err());
        }
    }
}
