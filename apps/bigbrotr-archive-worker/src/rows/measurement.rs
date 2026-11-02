//! One compound A05 row. Inline children share the original source/digest and
//! never allocate their own fact identity or independent measurement vote.
use super::*;
use arrow::array::BooleanArray;
use bigbrotr_model::{
    ConnectCallResult, FirstValidEvent, Measurement, MeasurementMoment, MeasurementState,
    MeasurementTermination, ObservedAttempt, PassiveReadProfile, TerminalMeasurement,
};

impl Rows {
    pub fn measurements(facts: &[&Fact], limits: RowLimits) -> Result<Self, RowError> {
        let values = metadata::select(facts, limits, |payload| match payload {
            FactPayload::Measurement(value) => Some(value),
            _ => None,
        })?;
        let mut columns = envelope(facts)?;
        columns.extend([
            (
                "attempt_id",
                fixed(values.iter().map(|v| *v.attempt_id().as_bytes()))?,
            ),
            ("subject_kind", u16s(values.iter().map(|_| Some(0)))),
            (
                "endpoint_id",
                qualified(values.iter().map(|v| v.context().endpoint_id()))?,
            ),
            (
                "observer_id",
                qualified(values.iter().map(|v| v.context().observer_id))?,
            ),
            (
                "route_id",
                qualified(values.iter().map(|v| v.context().route_id))?,
            ),
            (
                "access_scope_id",
                qualified(values.iter().map(|v| v.context().access_scope_id))?,
            ),
            (
                "method_id",
                qualified(values.iter().map(|v| v.context().method_id))?,
            ),
            ("profile", profile(&values)?),
            (
                "clock_quality",
                metadata::clock(values.iter().map(|v| v.clock_quality()))?,
            ),
            (
                "state_kind",
                u16s(values.iter().map(|v| {
                    Some(match v.state() {
                        MeasurementState::ExecutionUnknown => 0,
                        MeasurementState::CompletionUnknown(_) => 1,
                        MeasurementState::Terminal(_) => 2,
                        MeasurementState::Resolution { .. } => 3,
                    })
                })),
            ),
            ("prior_unknown", prior(&values)?),
            (
                "observed",
                observations(values.iter().map(|v| observed(v)))?,
            ),
            (
                "ended",
                moments(values.iter().map(|v| terminal(v).map(|v| v.ended)))?,
            ),
            ("termination", terminations(&values)?),
            (
                "teardown",
                u16s(
                    values
                        .iter()
                        .map(|v| terminal(v).map(|v| v.teardown as u16)),
                ),
            ),
        ]);
        Self::finish(
            columns,
            &[
                "prior_unknown",
                "observed",
                "ended",
                "termination",
                "teardown",
            ],
        )
    }
}

fn u16s(values: impl Iterator<Item = Option<u16>>) -> ArrayRef {
    Arc::new(UInt16Array::from_iter(values))
}
fn u64s(values: impl Iterator<Item = Option<u64>>) -> ArrayRef {
    Arc::new(UInt64Array::from_iter(values))
}
fn bools(values: impl Iterator<Item = Option<bool>>) -> ArrayRef {
    Arc::new(BooleanArray::from_iter(values))
}
fn terminal(value: &Measurement) -> Option<TerminalMeasurement> {
    match value.state() {
        MeasurementState::Terminal(value)
        | MeasurementState::Resolution {
            terminal: value, ..
        } => Some(value),
        MeasurementState::ExecutionUnknown | MeasurementState::CompletionUnknown(_) => None,
    }
}
fn observed(value: &Measurement) -> Option<ObservedAttempt> {
    match value.state() {
        MeasurementState::ExecutionUnknown => None,
        MeasurementState::CompletionUnknown(value) => Some(value),
        MeasurementState::Terminal(value)
        | MeasurementState::Resolution {
            terminal: value, ..
        } => Some(value.observed),
    }
}

fn profile(values: &[&Measurement]) -> Result<ArrayRef, RowError> {
    use PassiveReadProfile as P;
    let fixed = [
        ("version", 1),
        ("base_cadence_us", P::BASE_CADENCE_US),
        ("overall_timeout_us", P::OVERALL_TIMEOUT_US),
        ("open_timeout_us", P::OPEN_TIMEOUT_US),
        ("relay_connections", 1),
        ("requests", 1),
        ("active_requests", 1),
        ("tries", 1),
        ("sole_filter_kind", 1),
        ("request_limit", 1),
        ("compression_disabled_code", 0),
        ("max_message_framed_bytes", P::MAX_MESSAGE_FRAMED_BYTES),
        ("max_normalized_event_bytes", P::MAX_NORMALIZED_EVENT_BYTES),
        ("max_rx_physical_frames", P::MAX_RX_PHYSICAL_FRAMES),
        ("max_rx_messages", P::MAX_RX_MESSAGES),
        ("max_event_candidates", P::MAX_EVENT_CANDIDATES),
        ("max_rx_ws_bytes", P::MAX_RX_WS_BYTES),
        ("max_tx_ws_bytes", P::MAX_TX_WS_BYTES),
        ("dns_exchange_timeout_us", 2_000_000),
        ("dns_max_exchanges_per_family", 9),
        ("dns_max_frame_bytes", 4096),
        ("dns_max_addresses", 64),
        ("auth_response_disabled_code", 0),
    ];
    let mut columns: Columns = fixed
        .into_iter()
        .map(|(name, value)| (name, u64s(values.iter().map(move |_| Some(value)))))
        .collect();
    columns.push((
        "effective_authority_ceiling_us",
        u64s(
            values
                .iter()
                .map(|v| Some(v.profile().effective_authority_us().get())),
        ),
    ));
    nullable_structure(columns, &[], None)
}

fn prior(values: &[&Measurement]) -> Result<ArrayRef, RowError> {
    nullable_structure(
        vec![
            (
                "source_record_id",
                optional_source(
                    values
                        .iter()
                        .map(|v| v.prior_unknown().map(|v| v.source_record_id)),
                )?,
            ),
            (
                "payload_digest",
                fixed_options(
                    values
                        .iter()
                        .map(|v| v.prior_unknown().map(|v| *v.payload_digest.as_bytes())),
                )?,
            ),
        ],
        &[],
        Some(values.iter().map(|v| v.prior_unknown().is_some()).collect()),
    )
}

fn moments(
    values: impl ExactSizeIterator<Item = Option<MeasurementMoment>> + Clone,
) -> Result<ArrayRef, RowError> {
    // Preserve parent validity even when BOTH samples of a known moment are
    // null. None is an unobserved phase; Some(null,null) is an observed phase.
    nullable_structure(
        vec![
            (
                "observed_at",
                Arc::new(Int64Array::from_iter(
                    values
                        .clone()
                        .map(|v| v.and_then(|v| v.observed_at.map(|v| v.get()))),
                )) as ArrayRef,
            ),
            (
                "elapsed_us",
                u64s(
                    values
                        .clone()
                        .map(|v| v.and_then(|v| v.elapsed_us.map(|v| v.get()))),
                ),
            ),
        ],
        &["observed_at", "elapsed_us"],
        Some(values.map(|v| v.is_some()).collect()),
    )
}

fn observations(
    values: impl ExactSizeIterator<Item = Option<ObservedAttempt>> + Clone,
) -> Result<ArrayRef, RowError> {
    let execution = nullable_structure(
        vec![
            (
                "destination",
                u16s(
                    values
                        .clone()
                        .map(|v| v.map(|v| v.execution.destination as u16)),
                ),
            ),
            (
                "result",
                u16s(values.clone().map(|v| {
                    v.map(|v| match v.execution.result {
                        ConnectCallResult::Accepted => 0,
                        ConnectCallResult::ImmediateError(_) => 1,
                    })
                })),
            ),
            (
                "error",
                u16s(values.clone().map(|v| {
                    v.and_then(|v| match v.execution.result {
                        ConnectCallResult::Accepted => None,
                        ConnectCallResult::ImmediateError(class) => Some(class as u16),
                    })
                })),
            ),
            (
                "observed_at",
                Arc::new(Int64Array::from_iter(values.clone().map(|v| {
                    v.and_then(|v| v.execution.observed_at.map(|v| v.get()))
                }))) as ArrayRef,
            ),
        ],
        &["error", "observed_at"],
        Some(values.clone().map(|v| v.is_some()).collect()),
    )?;
    let open = nullable_structure(
        vec![
            (
                "tcp_connected",
                moments(values.clone().map(|v| v.and_then(|v| v.open.tcp_connected)))?,
            ),
            (
                "tls_completed",
                moments(values.clone().map(|v| v.and_then(|v| v.open.tls_completed)))?,
            ),
            (
                "websocket_opened",
                moments(
                    values
                        .clone()
                        .map(|v| v.and_then(|v| v.open.websocket_opened)),
                )?,
            ),
        ],
        &["tcp_connected", "tls_completed", "websocket_opened"],
        Some(values.clone().map(|v| v.is_some()).collect()),
    )?;
    let events = values
        .clone()
        .map(|v| v.and_then(|v| v.read.first_valid_event));
    let read = nullable_structure(
        vec![
            (
                "request_flushed",
                moments(
                    values
                        .clone()
                        .map(|v| v.and_then(|v| v.read.request_flushed)),
                )?,
            ),
            ("first_valid_event", first_events(events)?),
            (
                "eose",
                moments(values.clone().map(|v| v.and_then(|v| v.read.eose)))?,
            ),
            (
                "eose_prefix_verified",
                bools(
                    values
                        .clone()
                        .map(|v| v.map(|v| v.read.eose_prefix_verified)),
                ),
            ),
            (
                "auth_observed",
                bools(values.clone().map(|v| v.map(|v| v.read.auth_observed))),
            ),
            (
                "correlated_closed",
                bools(values.clone().map(|v| v.map(|v| v.read.correlated_closed))),
            ),
        ],
        &["request_flushed", "first_valid_event", "eose"],
        Some(values.clone().map(|v| v.is_some()).collect()),
    )?;
    let counters = nullable_structure(
        vec![
            (
                "rx_physical_frames",
                u64s(
                    values
                        .clone()
                        .map(|v| v.map(|v| v.counters.rx_physical_frames)),
                ),
            ),
            (
                "rx_messages",
                u64s(values.clone().map(|v| v.map(|v| v.counters.rx_messages))),
            ),
            (
                "event_candidates",
                u64s(
                    values
                        .clone()
                        .map(|v| v.map(|v| v.counters.event_candidates)),
                ),
            ),
            (
                "rx_ws_bytes",
                u64s(values.clone().map(|v| v.map(|v| v.counters.rx_ws_bytes))),
            ),
            (
                "rx_payload_bytes",
                u64s(
                    values
                        .clone()
                        .map(|v| v.map(|v| v.counters.rx_payload_bytes)),
                ),
            ),
            (
                "tx_ws_bytes",
                u64s(values.clone().map(|v| v.map(|v| v.counters.tx_ws_bytes))),
            ),
        ],
        &[],
        Some(values.clone().map(|v| v.is_some()).collect()),
    )?;
    nullable_structure(
        vec![
            ("execution", execution),
            ("open", open),
            ("read", read),
            ("counters", counters),
        ],
        &[],
        Some(values.map(|v| v.is_some()).collect()),
    )
}

fn first_events(
    values: impl ExactSizeIterator<Item = Option<FirstValidEvent>> + Clone,
) -> Result<ArrayRef, RowError> {
    nullable_structure(
        vec![
            (
                "event_id",
                fixed_options(values.clone().map(|v| v.map(|v| *v.event_id.as_bytes())))?,
            ),
            (
                "received",
                moments(values.clone().map(|v| v.map(|v| v.received)))?,
            ),
            (
                "verified",
                moments(values.clone().map(|v| v.map(|v| v.verified)))?,
            ),
        ],
        &[],
        Some(values.map(|v| v.is_some()).collect()),
    )
}

fn terminations(values: &[&Measurement]) -> Result<ArrayRef, RowError> {
    fn fields(v: MeasurementTermination) -> (u16, Option<u16>, Option<u16>, Option<u16>) {
        match v {
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
        }
    }
    let values = values
        .iter()
        .map(|v| terminal(v).map(|v| fields(v.termination)));
    nullable_structure(
        vec![
            ("kind", u16s(values.clone().map(|v| v.map(|v| v.0)))),
            ("phase", u16s(values.clone().map(|v| v.and_then(|v| v.1)))),
            ("failure", u16s(values.clone().map(|v| v.and_then(|v| v.2)))),
            (
                "limit_code",
                u16s(values.clone().map(|v| v.and_then(|v| v.3))),
            ),
        ],
        &["phase", "failure", "limit_code"],
        Some(values.map(|v| v.is_some()).collect()),
    )
}
