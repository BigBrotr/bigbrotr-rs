use super::{clock, reference, source};
use crate::{FactRef, Measurement, measurement::*};
use minicbor::{
    Encoder,
    encode::{Error, Write},
};

fn utc<W: Write>(
    value: Option<crate::ObservedTime>,
    e: &mut Encoder<W>,
) -> Result<(), Error<W::Error>> {
    match value {
        Some(v) => e.i64(v.get())?.ok(),
        None => e.null()?.ok(),
    }
}
fn moment<W: Write>(value: MeasurementMoment, e: &mut Encoder<W>) -> Result<(), Error<W::Error>> {
    e.array(2)?;
    utc(value.observed_at, e)?;
    match value.elapsed_us {
        Some(v) => e.u64(v.get())?.ok(),
        None => e.null()?.ok(),
    }
}
fn optional_moment<W: Write>(
    value: Option<MeasurementMoment>,
    e: &mut Encoder<W>,
) -> Result<(), Error<W::Error>> {
    match value {
        Some(v) => moment(v, e),
        None => e.null()?.ok(),
    }
}
fn observed<W: Write>(value: ObservedAttempt, e: &mut Encoder<W>) -> Result<(), Error<W::Error>> {
    e.array(4)?
        .array(3)?
        .u8(value.execution.destination as u8)?;
    match value.execution.result {
        ConnectCallResult::Accepted => {
            e.array(1)?.u8(0)?;
        }
        ConnectCallResult::ImmediateError(class) => {
            e.array(2)?.u8(1)?.u8(class as u8)?;
        }
    }
    utc(value.execution.observed_at, e)?;
    e.array(3)?;
    optional_moment(value.open.tcp_connected, e)?;
    optional_moment(value.open.tls_completed, e)?;
    optional_moment(value.open.websocket_opened, e)?;
    e.array(6)?;
    optional_moment(value.read.request_flushed, e)?;
    match value.read.first_valid_event {
        Some(v) => {
            e.array(3)?.bytes(v.event_id.as_bytes())?;
            moment(v.received, e)?;
            moment(v.verified, e)?;
        }
        None => {
            e.null()?;
        }
    }
    optional_moment(value.read.eose, e)?;
    e.bool(value.read.eose_prefix_verified)?
        .bool(value.read.auth_observed)?
        .bool(value.read.correlated_closed)?;
    e.array(6)?;
    for count in value.counters.fields() {
        e.u64(count)?;
    }
    Ok(())
}
fn terminal<W: Write>(
    value: TerminalMeasurement,
    e: &mut Encoder<W>,
) -> Result<(), Error<W::Error>> {
    e.array(4)?;
    observed(value.observed, e)?;
    moment(value.ended, e)?;
    match value.termination {
        MeasurementTermination::Eose => {
            e.array(1)?.u8(0)?;
        }
        MeasurementTermination::Closed => {
            e.array(1)?.u8(1)?;
        }
        MeasurementTermination::Deadline(phase) => {
            e.array(2)?.u8(2)?.u8(phase as u8)?;
        }
        MeasurementTermination::AuthorityInterruption(phase) => {
            e.array(2)?.u8(3)?.u8(phase as u8)?;
        }
        MeasurementTermination::Cancelled(phase) => {
            e.array(2)?.u8(4)?.u8(phase as u8)?;
        }
        MeasurementTermination::Failed { phase, class } => {
            e.array(3)?.u8(5)?.u8(phase as u8)?.u8(class as u8)?;
        }
        MeasurementTermination::Limit(limit) => {
            e.array(2)?.u8(6)?.u8(limit as u8)?;
        }
    }
    e.u8(value.teardown as u8)?.ok()
}
fn fact_ref<W: Write>(value: FactRef, e: &mut Encoder<W>) -> Result<(), Error<W::Error>> {
    e.array(2)?;
    source(value.source_record_id, e)?;
    e.bytes(value.payload_digest.as_bytes())?.ok()
}
pub(super) fn encode<W: Write>(
    value: &Measurement,
    e: &mut Encoder<W>,
) -> Result<(), Error<W::Error>> {
    e.array(5)?.bytes(value.attempt_id().as_bytes())?;
    let context = value.context();
    e.array(5)?.array(2)?.u8(0)?;
    reference(context.endpoint_id(), e)?;
    for reference_value in [
        context.observer_id,
        context.route_id,
        context.access_scope_id,
        context.method_id,
    ] {
        reference(reference_value, e)?;
    }
    e.array(24)?;
    for field in PassiveReadProfile::WIRE_FIELDS {
        e.u64(field)?;
    }
    e.u64(value.profile().effective_authority_us().get())?;
    clock(value.clock_quality(), e)?;
    match value.state() {
        MeasurementState::ExecutionUnknown => {
            e.array(1)?.u8(0)?;
        }
        MeasurementState::CompletionUnknown(v) => {
            e.array(2)?.u8(1)?;
            observed(v, e)?;
        }
        MeasurementState::Terminal(v) => {
            e.array(2)?.u8(2)?;
            terminal(v, e)?;
        }
        MeasurementState::Resolution {
            prior_unknown,
            terminal: v,
        } => {
            e.array(3)?.u8(3)?;
            fact_ref(prior_unknown, e)?;
            terminal(v, e)?;
        }
    }
    Ok(())
}
