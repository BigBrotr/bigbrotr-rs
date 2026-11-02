//! Fixed nested values for the one compound A05 projection row. All children
//! retain their parent's source/digest/discriminator and add no separate vote.
use super::{
    ReferenceTuple, RowsError, SourceTuple, digest::ValueWriter, reference_tuple, source_tuple,
};
use bigbrotr_model::Measurement;
use bigbrotr_protocol::archive_measurement as transport;
use serde::{Deserialize, Serialize};

type TerminationTuple = (u16, Option<u16>, Option<u16>, Option<u16>);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MeasurementData {
    pub attempt_id: [u8; 16],
    pub subject_kind: u16,
    pub endpoint_id: ReferenceTuple,
    pub observer_id: ReferenceTuple,
    pub route_id: ReferenceTuple,
    pub access_scope_id: ReferenceTuple,
    pub method_id: ReferenceTuple,
    pub profile: [u64; 24],
    pub clock_quality: (ReferenceTuple, u64, Option<u64>, u32),
    pub state_kind: u16,
    pub prior_unknown: Option<(SourceTuple, [u8; 32])>,
    pub observed: Option<Observed>,
    pub ended: Option<Moment>,
    pub termination: Option<TerminationTuple>,
    pub teardown: Option<u16>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(into = "MomentTuple", from = "MomentTuple")]
pub struct Moment {
    pub observed_at: Option<i64>,
    pub elapsed_us: Option<u64>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(into = "OpenTuple", from = "OpenTuple")]
pub struct Open {
    pub tcp_connected: Option<Moment>,
    pub tls_completed: Option<Moment>,
    pub websocket_opened: Option<Moment>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(into = "FirstEventTuple", from = "FirstEventTuple")]
pub struct FirstEvent {
    pub event_id: [u8; 32],
    pub received: Moment,
    pub verified: Moment,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(into = "ReadTuple", from = "ReadTuple")]
pub struct Read {
    pub request_flushed: Option<Moment>,
    pub first_valid_event: Option<FirstEvent>,
    pub eose: Option<Moment>,
    pub eose_prefix_verified: bool,
    pub auth_observed: bool,
    pub correlated_closed: bool,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Observed {
    pub execution: (u16, u16, Option<u16>, Option<i64>),
    pub open: Open,
    pub read: Read,
    pub counters: (u64, u64, u64, u64, u64, u64),
}

// The pinned RowBinary binding treats SerializeStruct as its top-level row,
// rather than entering a nested Tuple validator. Explicit fixed tuples keep the
// compound values typed and validated on both serialization and readback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Child<T>(Option<T>);
impl<T: Serialize> Serialize for Child<T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        super::serde_fields::optional_tuple::serialize(&self.0, serializer)
    }
}
impl<'de, T: Deserialize<'de>> Deserialize<'de> for Child<T> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        super::serde_fields::optional_tuple::deserialize(deserializer).map(Self)
    }
}
type MomentTuple = (Option<i64>, Option<u64>);
impl From<Moment> for MomentTuple {
    fn from(v: Moment) -> Self {
        (v.observed_at, v.elapsed_us)
    }
}
impl From<MomentTuple> for Moment {
    fn from(v: MomentTuple) -> Self {
        Self {
            observed_at: v.0,
            elapsed_us: v.1,
        }
    }
}
type OpenTuple = (Child<Moment>, Child<Moment>, Child<Moment>);
impl From<Open> for OpenTuple {
    fn from(v: Open) -> Self {
        (
            Child(v.tcp_connected),
            Child(v.tls_completed),
            Child(v.websocket_opened),
        )
    }
}
impl From<OpenTuple> for Open {
    fn from(v: OpenTuple) -> Self {
        Self {
            tcp_connected: v.0.0,
            tls_completed: v.1.0,
            websocket_opened: v.2.0,
        }
    }
}
type FirstEventTuple = ([u8; 32], Moment, Moment);
impl From<FirstEvent> for FirstEventTuple {
    fn from(v: FirstEvent) -> Self {
        (v.event_id, v.received, v.verified)
    }
}
impl From<FirstEventTuple> for FirstEvent {
    fn from(v: FirstEventTuple) -> Self {
        Self {
            event_id: v.0,
            received: v.1,
            verified: v.2,
        }
    }
}
type ReadTuple = (
    Child<Moment>,
    Child<FirstEvent>,
    Child<Moment>,
    bool,
    bool,
    bool,
);
impl From<Read> for ReadTuple {
    fn from(v: Read) -> Self {
        (
            Child(v.request_flushed),
            Child(v.first_valid_event),
            Child(v.eose),
            v.eose_prefix_verified,
            v.auth_observed,
            v.correlated_closed,
        )
    }
}
impl From<ReadTuple> for Read {
    fn from(v: ReadTuple) -> Self {
        Self {
            request_flushed: v.0.0,
            first_valid_event: v.1.0,
            eose: v.2.0,
            eose_prefix_verified: v.3,
            auth_observed: v.4,
            correlated_closed: v.5,
        }
    }
}
impl From<transport::MomentRow> for Moment {
    fn from(value: transport::MomentRow) -> Self {
        Self {
            observed_at: value.observed_at.map(|v| v.get()),
            elapsed_us: value.elapsed_us.map(|v| v.get()),
        }
    }
}
impl From<transport::ObservedRow> for Observed {
    fn from(value: transport::ObservedRow) -> Self {
        let open = value.open;
        let read = value.read;
        let counters = value.counters;
        Self {
            execution: (
                value.execution.destination,
                value.execution.result,
                value.execution.error,
                value.execution.observed_at.map(|v| v.get()),
            ),
            open: Open {
                tcp_connected: open.tcp_connected.map(Into::into),
                tls_completed: open.tls_completed.map(Into::into),
                websocket_opened: open.websocket_opened.map(Into::into),
            },
            read: Read {
                request_flushed: read.request_flushed.map(Into::into),
                first_valid_event: read.first_valid_event.map(|v| FirstEvent {
                    event_id: *v.event_id.as_bytes(),
                    received: v.received.into(),
                    verified: v.verified.into(),
                }),
                eose: read.eose.map(Into::into),
                eose_prefix_verified: read.eose_prefix_verified,
                auth_observed: read.auth_observed,
                correlated_closed: read.correlated_closed,
            },
            counters: (
                counters.rx_physical_frames,
                counters.rx_messages,
                counters.event_candidates,
                counters.rx_ws_bytes,
                counters.rx_payload_bytes,
                counters.tx_ws_bytes,
            ),
        }
    }
}
impl MeasurementData {
    pub(super) fn new(value: &Measurement) -> Self {
        let value = transport::MeasurementRow::from_measurement(value);
        let quality = value.clock_quality;
        Self {
            attempt_id: *value.attempt_id.as_bytes(),
            subject_kind: value.subject_kind,
            endpoint_id: reference_tuple(value.endpoint_id),
            observer_id: reference_tuple(value.observer_id),
            route_id: reference_tuple(value.route_id),
            access_scope_id: reference_tuple(value.access_scope_id),
            method_id: reference_tuple(value.method_id),
            profile: value.profile,
            clock_quality: (
                reference_tuple(quality.clock_id()),
                quality.resolution_us().get(),
                quality.uncertainty_us().map(|v| v.get()),
                quality.anomaly_flags(),
            ),
            state_kind: value.state_kind,
            prior_unknown: value.prior_unknown.map(|v| {
                (
                    source_tuple(v.source_record_id),
                    *v.payload_digest.as_bytes(),
                )
            }),
            observed: value.observed.map(Into::into),
            ended: value.ended.map(Into::into),
            termination: value
                .termination
                .map(|v| (v.kind, v.phase, v.failure, v.limit_code)),
            teardown: value.teardown,
        }
    }
}

fn optional<T>(
    out: &mut ValueWriter,
    value: &Option<T>,
    encode: impl FnOnce(&mut ValueWriter, &T) -> Result<(), RowsError>,
) -> Result<(), RowsError> {
    // Both Nullable scalar flags and Array(Tuple) lengths0/1 take one wire byte;
    // the canonical value remains null or the complete semantic child.
    out.charge(1)?;
    match value {
        Some(value) => encode(out, value),
        None => {
            out.null();
            Ok(())
        }
    }
}
fn optional_i64(out: &mut ValueWriter, value: &Option<i64>) -> Result<(), RowsError> {
    optional(out, value, |out, value| out.i64(value))
}
impl Moment {
    fn encode(out: &mut ValueWriter, value: &Self) -> Result<(), RowsError> {
        out.array(2);
        optional_i64(out, &value.observed_at)?;
        out.optional_u64(&value.elapsed_us)
    }
}
impl Open {
    fn encode(out: &mut ValueWriter, value: &Self) -> Result<(), RowsError> {
        out.array(3);
        for moment in [
            &value.tcp_connected,
            &value.tls_completed,
            &value.websocket_opened,
        ] {
            optional(out, moment, Moment::encode)?;
        }
        Ok(())
    }
}
impl Read {
    fn encode(out: &mut ValueWriter, value: &Self) -> Result<(), RowsError> {
        out.array(6);
        optional(out, &value.request_flushed, Moment::encode)?;
        optional(out, &value.first_valid_event, |out, event| {
            out.array(3);
            out.bytes(&event.event_id)?;
            Moment::encode(out, &event.received)?;
            Moment::encode(out, &event.verified)
        })?;
        optional(out, &value.eose, Moment::encode)?;
        for value in [
            value.eose_prefix_verified,
            value.auth_observed,
            value.correlated_closed,
        ] {
            out.boolean(value)?;
        }
        Ok(())
    }
}
impl ValueWriter {
    pub(super) fn measurement_profile(&mut self, value: &[u64; 24]) -> Result<(), RowsError> {
        self.array(24);
        for number in value {
            self.u64(number)?;
        }
        Ok(())
    }
    pub(super) fn measurement_clock(
        &mut self,
        value: &(ReferenceTuple, u64, Option<u64>, u32),
    ) -> Result<(), RowsError> {
        self.array(4);
        self.reference(&value.0)?;
        self.u64(&value.1)?;
        self.optional_u64(&value.2)?;
        self.u32(&value.3)
    }
    pub(super) fn measurement_prior(
        &mut self,
        value: &Option<(SourceTuple, [u8; 32])>,
    ) -> Result<(), RowsError> {
        optional(self, value, |out, (source, digest)| {
            out.array(2);
            out.source(source)?;
            out.bytes(digest)
        })
    }
    pub(super) fn measurement_execution(
        &mut self,
        value: &Option<(u16, u16, Option<u16>, Option<i64>)>,
    ) -> Result<(), RowsError> {
        optional(self, value, |out, v| {
            out.array(4);
            out.u16(&v.0)?;
            out.u16(&v.1)?;
            out.optional_u16(&v.2)?;
            optional_i64(out, &v.3)
        })
    }
    pub(super) fn measurement_open(&mut self, value: &Option<Open>) -> Result<(), RowsError> {
        optional(self, value, Open::encode)
    }
    pub(super) fn measurement_read(&mut self, value: &Option<Read>) -> Result<(), RowsError> {
        optional(self, value, Read::encode)
    }
    pub(super) fn measurement_counters(
        &mut self,
        value: &Option<(u64, u64, u64, u64, u64, u64)>,
    ) -> Result<(), RowsError> {
        optional(self, value, |out, v| {
            out.array(6);
            for n in [v.0, v.1, v.2, v.3, v.4, v.5] {
                out.u64(&n)?;
            }
            Ok(())
        })
    }
    pub(super) fn measurement_moment(&mut self, value: &Option<Moment>) -> Result<(), RowsError> {
        optional(self, value, Moment::encode)
    }
    pub(super) fn measurement_termination(
        &mut self,
        value: &Option<TerminationTuple>,
    ) -> Result<(), RowsError> {
        optional(self, value, |out, v| {
            out.array(4);
            out.u16(&v.0)?;
            out.optional_u16(&v.1)?;
            out.optional_u16(&v.2)?;
            out.optional_u16(&v.3)
        })
    }
}
