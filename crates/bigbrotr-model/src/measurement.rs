//! Closed passive-read A05 evidence. Values establish structure, never that I/O
//! happened, a signature verified, a start was synced, or a producer is authorized.
use crate::{
    ClockQuality, DictionaryDescriptor, DictionaryEntry, DictionaryKind, DurationUs, EventId, Fact,
    FactPayload, FactRef, MeasurementAttemptId, ObservedTime, QualifiedRef, source::SourceError,
};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MeasurementError {
    Reference(SourceError),
    Profile,
    Observation,
    MonotonicOrder,
    DictionaryDependency,
    Resolution,
    AllocationFailed,
}
impl fmt::Display for MeasurementError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Reference(_) => "measurement reference has an incompatible namespace",
            Self::Profile => "unsupported or invalid passive-read profile",
            Self::Observation => "inconsistent passive-read observations",
            Self::MonotonicOrder => "measurement monotonic observations are out of order",
            Self::DictionaryDependency => "measurement requires exact closed dictionary mappings",
            Self::Resolution => "resolution does not preserve the named unknown attempt",
            Self::AllocationFailed => "unable to reserve the fixed measurement record",
        })
    }
}
impl std::error::Error for MeasurementError {}
impl From<SourceError> for MeasurementError {
    fn from(value: SourceError) -> Self {
        Self::Reference(value)
    }
}

/// The first profile is fixed, not a bag of independently adjustable settings.
/// All constants are carried inline in the canonical record. Effective authority
/// may be shorter; this value neither renews nor bypasses the five-second lease.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PassiveReadProfile {
    effective_authority_us: DurationUs,
}
impl PassiveReadProfile {
    pub const BASE_CADENCE_US: u64 = 300_000_000;
    pub const OVERALL_TIMEOUT_US: u64 = 15_000_000;
    pub const OPEN_TIMEOUT_US: u64 = 10_000_000;
    pub const MAX_MESSAGE_FRAMED_BYTES: u64 = 2_097_152;
    pub const MAX_NORMALIZED_EVENT_BYTES: u64 = 262_144;
    pub const MAX_RX_PHYSICAL_FRAMES: u64 = 64;
    pub const MAX_RX_MESSAGES: u64 = 32;
    pub const MAX_EVENT_CANDIDATES: u64 = 3;
    pub const MAX_RX_WS_BYTES: u64 = 4_194_304;
    pub const MAX_TX_WS_BYTES: u64 = 65_536;
    // Exact order is specified in contracts/archive/v1/measurement.json.
    pub(crate) const WIRE_FIELDS: [u64; 23] = [
        1,
        Self::BASE_CADENCE_US,
        Self::OVERALL_TIMEOUT_US,
        Self::OPEN_TIMEOUT_US,
        1,
        1,
        1,
        1,
        1,
        1,
        0,
        Self::MAX_MESSAGE_FRAMED_BYTES,
        Self::MAX_NORMALIZED_EVENT_BYTES,
        Self::MAX_RX_PHYSICAL_FRAMES,
        Self::MAX_RX_MESSAGES,
        Self::MAX_EVENT_CANDIDATES,
        Self::MAX_RX_WS_BYTES,
        Self::MAX_TX_WS_BYTES,
        2_000_000,
        9,
        4096,
        64,
        0,
    ];
    pub fn first(effective_authority_us: DurationUs) -> Result<Self, MeasurementError> {
        if effective_authority_us.get() == 0 || effective_authority_us.get() > 5_000_000 {
            return Err(MeasurementError::Profile);
        }
        Ok(Self {
            effective_authority_us,
        })
    }
    pub const fn effective_authority_us(self) -> DurationUs {
        self.effective_authority_us
    }
}

/// Other selected subjects, including RecipientMetadataHttp, require their real
/// P04-T14 producer/consumer contract before a new schema makes them constructible.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MeasurementSubject {
    RelayEndpoint(QualifiedRef),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MeasurementContext {
    pub subject: MeasurementSubject,
    pub observer_id: QualifiedRef,
    pub route_id: QualifiedRef,
    pub access_scope_id: QualifiedRef,
    pub method_id: QualifiedRef,
}
impl MeasurementContext {
    pub const fn endpoint_id(self) -> QualifiedRef {
        match self.subject {
            MeasurementSubject::RelayEndpoint(endpoint) => endpoint,
        }
    }
}

/// UTC is nullable and may regress. Elapsed time uses a separate monotonic clock
/// anchored after the first externally directed connect syscall returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MeasurementMoment {
    pub observed_at: Option<ObservedTime>,
    pub elapsed_us: Option<DurationUs>,
}
impl MeasurementMoment {
    fn no_later_than(self, later: Self) -> Result<(), MeasurementError> {
        if let (Some(a), Some(b)) = (self.elapsed_us, later.elapsed_us)
            && a > b
        {
            return Err(MeasurementError::MonotonicOrder);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ConnectDestination {
    DnsResolver = 0,
    Relay = 1,
}
/// Closed portable classes; no raw errno, hostname, challenge or exception text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ConnectErrorClass {
    Permission = 0,
    Resources = 1,
    Address = 2,
    Refused = 3,
    Unreachable = 4,
    TimedOut = 5,
    Other = 6,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectCallResult {
    Accepted,
    ImmediateError(ConnectErrorClass),
}
/// A structural claim about the actual returned syscall, including immediate
/// errors. Constructors do not supply trusted transport evidence. This is never
/// proof of a packet, remote receipt, durable start, or successful connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObservedExecution {
    pub destination: ConnectDestination,
    pub result: ConnectCallResult,
    pub observed_at: Option<ObservedTime>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct OpenObservation {
    pub tcp_connected: Option<MeasurementMoment>,
    pub tls_completed: Option<MeasurementMoment>,
    pub websocket_opened: Option<MeasurementMoment>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FirstValidEvent {
    pub event_id: EventId,
    /// Original complete-message delivery, before parsing/verification; not a
    /// kernel arrival timestamp. Later verification cannot retimestamp it.
    pub received: MeasurementMoment,
    pub verified: MeasurementMoment,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ReadObservation {
    pub request_flushed: Option<MeasurementMoment>,
    pub first_valid_event: Option<FirstValidEvent>,
    /// Correlated EOSE decoder delivery. The ordered prefix may verify later.
    pub eose: Option<MeasurementMoment>,
    /// Positive completeness requires this separate barrier, including an empty
    /// prefix. It never retimestamps EOSE to verification completion.
    pub eose_prefix_verified: bool,
    /// An observed challenge is not a global access refusal; no response is signed.
    pub auth_observed: bool,
    pub correlated_closed: bool,
}
/// Actual observed counts, including control/unrelated/rejected/teardown input.
/// A declared payload length is not consumed bytes. A cap is an admission bound,
/// not permission to clip observations; a rejecting header/read may cross it.
/// Counters have independent meanings and are not inferred from one another.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MeasurementCounters {
    pub rx_physical_frames: u64,
    pub rx_messages: u64,
    pub event_candidates: u64,
    pub rx_ws_bytes: u64,
    pub rx_payload_bytes: u64,
    pub tx_ws_bytes: u64,
}
impl MeasurementCounters {
    pub(crate) const fn fields(self) -> [u64; 6] {
        [
            self.rx_physical_frames,
            self.rx_messages,
            self.event_candidates,
            self.rx_ws_bytes,
            self.rx_payload_bytes,
            self.tx_ws_bytes,
        ]
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObservedAttempt {
    pub execution: ObservedExecution,
    pub open: OpenObservation,
    pub read: ReadObservation,
    pub counters: MeasurementCounters,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MeasurementPhase {
    Dns = 0,
    SocketSetup = 1,
    TcpConnect = 2,
    Tls = 3,
    WebSocket = 4,
    RequestWrite = 5,
    ResponseRead = 6,
    Verification = 7,
    Teardown = 8,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MeasurementFailure {
    LocalPermission = 0,
    LocalResources = 1,
    LocalConfiguration = 2,
    DnsUnavailable = 3,
    ConnectFailed = 4,
    TlsFailed = 5,
    UpgradeFailed = 6,
    Transport = 7,
    Decode = 8,
    InvalidEvent = 9,
    Disconnected = 10,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MeasurementLimit {
    MessageFramedBytes = 0,
    NormalizedEventBytes = 1,
    RxPhysicalFrames = 2,
    RxMessages = 3,
    EventCandidates = 4,
    RxWsBytes = 5,
    TxWsBytes = 6,
    DnsBounds = 7,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MeasurementTermination {
    Eose,
    Closed,
    Deadline(MeasurementPhase),
    AuthorityInterruption(MeasurementPhase),
    Cancelled(MeasurementPhase),
    Failed {
        phase: MeasurementPhase,
        class: MeasurementFailure,
    },
    Limit(MeasurementLimit),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TeardownObservation {
    NotAttempted = 0,
    Completed = 1,
    Failed = 2,
    Interrupted = 3,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TerminalMeasurement {
    pub observed: ObservedAttempt,
    /// Actual terminal observation; unavailable UTC/monotonic samples stay null.
    pub ended: MeasurementMoment,
    pub termination: MeasurementTermination,
    pub teardown: TeardownObservation,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MeasurementState {
    ExecutionUnknown,
    CompletionUnknown(ObservedAttempt),
    Terminal(TerminalMeasurement),
    Resolution {
        prior_unknown: FactRef,
        terminal: TerminalMeasurement,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MeasurementFields {
    attempt_id: MeasurementAttemptId,
    context: MeasurementContext,
    profile: PassiveReadProfile,
    clock_quality: ClockQuality,
    state: MeasurementState,
}
/// One checked allocation keeps the large fixed record out of every unrelated
/// Fact variant. Callers must fund input, this allocation and any clone before
/// construction/copy; logical bytes exclude allocator overhead and are not RSS.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Measurement {
    fields: Box<[MeasurementFields; 1]>,
}
impl Measurement {
    pub const fn required_allocation_bytes() -> usize {
        std::mem::size_of::<MeasurementFields>()
    }
    pub fn heap_allocation_bytes(&self) -> usize {
        Self::required_allocation_bytes()
    }
    pub fn new(
        attempt_id: MeasurementAttemptId,
        context: MeasurementContext,
        profile: PassiveReadProfile,
        clock_quality: ClockQuality,
        state: MeasurementState,
    ) -> Result<Self, MeasurementError> {
        let fields = MeasurementFields {
            attempt_id,
            context,
            profile,
            clock_quality,
            state,
        };
        for (reference, kind) in measurement_references(context, clock_quality) {
            reference.require(context.endpoint_id().dataset_id, kind)?;
        }
        if let MeasurementState::Resolution { prior_unknown, .. } = state
            && prior_unknown.source_record_id.dataset_id != context.endpoint_id().dataset_id
        {
            return Err(SourceError::ForeignDataset.into());
        }
        match state {
            MeasurementState::ExecutionUnknown => {}
            MeasurementState::CompletionUnknown(observed) => {
                observed.validate()?;
            }
            MeasurementState::Terminal(terminal)
            | MeasurementState::Resolution { terminal, .. } => terminal.validate()?,
        }
        let mut storage = Vec::new();
        storage
            .try_reserve_exact(1)
            .map_err(|_| MeasurementError::AllocationFailed)?;
        storage.push(fields);
        let fields = storage
            .into_boxed_slice()
            .try_into()
            .map_err(|_| MeasurementError::AllocationFailed)?;
        Ok(Self { fields })
    }
    pub fn attempt_id(&self) -> MeasurementAttemptId {
        self.fields[0].attempt_id
    }
    pub fn context(&self) -> MeasurementContext {
        self.fields[0].context
    }
    pub fn profile(&self) -> PassiveReadProfile {
        self.fields[0].profile
    }
    pub fn clock_quality(&self) -> ClockQuality {
        self.fields[0].clock_quality
    }
    pub fn state(&self) -> MeasurementState {
        self.fields[0].state
    }
    pub fn prior_unknown(&self) -> Option<FactRef> {
        match self.state() {
            MeasurementState::Resolution { prior_unknown, .. } => Some(prior_unknown),
            _ => None,
        }
    }
    /// One typed walker for all six payload dictionary references. Clock identity
    /// occurs only inside ClockQuality, never duplicated in stable context.
    pub fn references(&self) -> impl Iterator<Item = (QualifiedRef, DictionaryKind)> {
        measurement_references(self.context(), self.clock_quality())
    }
    /// Resolve exact complete mappings. This is shape/dependency validation, not
    /// visibility, allocation, policy, transport or source authority.
    pub fn validate_dictionary_dependencies<'a>(
        &self,
        mut resolve: impl FnMut(QualifiedRef) -> Option<&'a DictionaryEntry>,
    ) -> Result<(), MeasurementError> {
        for (reference, kind) in self.references() {
            let entry = resolve(reference).ok_or(MeasurementError::DictionaryDependency)?;
            let valid = entry.reference() == reference
                && matches!(
                    (kind, entry.descriptor()),
                    (DictionaryKind::Endpoint, DictionaryDescriptor::Endpoint(_))
                        | (DictionaryKind::Observer, DictionaryDescriptor::Observer(_))
                        | (DictionaryKind::Route, DictionaryDescriptor::DirectRoute(_))
                        | (
                            DictionaryKind::AccessScope,
                            DictionaryDescriptor::AccessScope(_)
                        )
                        | (DictionaryKind::Method, DictionaryDescriptor::PassiveWsRead)
                );
            if !valid {
                return Err(MeasurementError::DictionaryDependency);
            }
        }
        Ok(())
    }
}

fn measurement_references(
    context: MeasurementContext,
    clock_quality: ClockQuality,
) -> impl Iterator<Item = (QualifiedRef, DictionaryKind)> {
    use DictionaryKind as K;
    std::iter::once((context.endpoint_id(), K::Endpoint))
        .chain(std::iter::once((context.observer_id, K::Observer)))
        .chain(std::iter::once((context.route_id, K::Route)))
        .chain(std::iter::once((context.access_scope_id, K::AccessScope)))
        .chain(std::iter::once((context.method_id, K::Method)))
        .chain(std::iter::once((clock_quality.clock_id(), K::Observer)))
}

impl ObservedAttempt {
    fn validate(self) -> Result<(), MeasurementError> {
        let open = self.open;
        let read = self.read;
        if (open.tls_completed.is_some() || open.websocket_opened.is_some())
            && open.tcp_connected.is_none()
            || (read.request_flushed.is_some() && open.websocket_opened.is_none())
            || (read.first_valid_event.is_some() || read.eose.is_some() || read.correlated_closed)
                && read.request_flushed.is_none()
            || read.auth_observed && open.websocket_opened.is_none()
            || read.eose_prefix_verified && read.eose.is_none()
            || self.execution.destination == ConnectDestination::Relay
                && matches!(self.execution.result, ConnectCallResult::ImmediateError(_))
                && open.tcp_connected.is_some()
        {
            return Err(MeasurementError::Observation);
        }
        let mut previous = None;
        for current in [
            open.tcp_connected,
            open.tls_completed,
            open.websocket_opened,
            read.request_flushed,
        ]
        .into_iter()
        .flatten()
        {
            if let Some(prior) = previous {
                MeasurementMoment::no_later_than(prior, current)?;
            }
            if current.elapsed_us.is_some() {
                previous = Some(current);
            }
        }
        if let Some(event) = read.first_valid_event {
            if let Some(prior) = previous {
                prior.no_later_than(event.received)?;
                prior.no_later_than(event.verified)?;
            }
            event.received.no_later_than(event.verified)?;
            if let Some(eose) = read.eose {
                event.received.no_later_than(eose)?;
            }
        }
        if let (Some(prior), Some(eose)) = (previous, read.eose) {
            prior.no_later_than(eose)?;
        }
        Ok(())
    }
    fn preserves(self, earlier: Self) -> bool {
        fn kept<T: PartialEq>(old: Option<T>, new: Option<T>) -> bool {
            old.is_none() || old == new
        }
        fn moment(old: Option<MeasurementMoment>, new: Option<MeasurementMoment>) -> bool {
            old.is_none()
                || matches!((old, new), (Some(a), Some(b))
                if kept(a.observed_at, b.observed_at) && kept(a.elapsed_us, b.elapsed_us))
        }
        let event_preserved = earlier.read.first_valid_event.is_none()
            || matches!((earlier.read.first_valid_event, self.read.first_valid_event), (Some(a), Some(b))
                if a.event_id == b.event_id && moment(Some(a.received), Some(b.received))
                    && moment(Some(a.verified), Some(b.verified)));
        self.execution.destination == earlier.execution.destination
            && self.execution.result == earlier.execution.result
            && kept(earlier.execution.observed_at, self.execution.observed_at)
            && moment(earlier.open.tcp_connected, self.open.tcp_connected)
            && moment(earlier.open.tls_completed, self.open.tls_completed)
            && moment(earlier.open.websocket_opened, self.open.websocket_opened)
            && moment(earlier.read.request_flushed, self.read.request_flushed)
            && event_preserved
            && moment(earlier.read.eose, self.read.eose)
            && (!earlier.read.eose_prefix_verified || self.read.eose_prefix_verified)
            && (!earlier.read.auth_observed || self.read.auth_observed)
            && (!earlier.read.correlated_closed || self.read.correlated_closed)
            && earlier
                .counters
                .fields()
                .into_iter()
                .zip(self.counters.fields())
                .all(|(a, b)| a <= b)
    }
}
impl TerminalMeasurement {
    fn validate(self) -> Result<(), MeasurementError> {
        self.observed.validate()?;
        match self.termination {
            MeasurementTermination::Eose if !self.observed.read.eose_prefix_verified => {
                return Err(MeasurementError::Observation);
            }
            MeasurementTermination::Closed if !self.observed.read.correlated_closed => {
                return Err(MeasurementError::Observation);
            }
            _ => {}
        }
        let event = self.observed.read.first_valid_event;
        for moment in [
            self.observed.open.tcp_connected,
            self.observed.open.tls_completed,
            self.observed.open.websocket_opened,
            self.observed.read.request_flushed,
            event.map(|v| v.received),
            event.map(|v| v.verified),
            self.observed.read.eose,
        ]
        .into_iter()
        .flatten()
        {
            moment.no_later_than(self.ended)?;
        }
        Ok(())
    }
}

impl Fact {
    /// Compare the exact named unknown and immutable context. The consumer must
    /// FIRST select the cut and establish target availability/authority. This
    /// helper does not resolve competing terminals or select a last writer.
    pub fn validate_measurement_resolution(&self, prior: &Fact) -> Result<(), MeasurementError> {
        let (FactPayload::Measurement(current), FactPayload::Measurement(previous)) =
            (self.payload(), prior.payload())
        else {
            return Err(MeasurementError::Resolution);
        };
        let MeasurementState::Resolution {
            prior_unknown,
            terminal,
        } = current.state()
        else {
            return Err(MeasurementError::Resolution);
        };
        if prior_unknown != prior.fact_ref()
            || self.visibility_scope_id() != prior.visibility_scope_id()
            || self.source_record_id().dataset_id != prior.source_record_id().dataset_id
            || current.attempt_id() != previous.attempt_id()
            || current.context() != previous.context()
            || current.profile() != previous.profile()
            || current.clock_quality().clock_id() != previous.clock_quality().clock_id()
        {
            return Err(MeasurementError::Resolution);
        }
        match previous.state() {
            MeasurementState::ExecutionUnknown => Ok(()),
            MeasurementState::CompletionUnknown(observed)
                if terminal.observed.preserves(observed) =>
            {
                Ok(())
            }
            _ => Err(MeasurementError::Resolution),
        }
    }
}
