//! Full bounded retained history: geometry follows authenticated page evidence.
use super::*;
use crate::acquisition::{
    quantum::{Exclusion, Geometry, Settlement},
    scope::StoredAcquisitionScope,
};
use crate::storage::rocks::store::{AdmissionError, ProgressRead};
use bigbrotr_model::EventId;
use bigbrotr_protocol::wire::{RelayMessage, decode_relay};
use futures_util::TryStreamExt;
use tokio_postgres::Row;

pub(super) const MAX_BOUNDARY_IDS: usize = 3 * 128;
pub(super) struct Boundary {
    second: Option<u64>,
    ids: Vec<EventId>,
}
impl Boundary {
    pub(super) fn new() -> Result<Self, QuantumError> {
        let mut ids = Vec::new();
        ids.try_reserve_exact(MAX_BOUNDARY_IDS)
            .map_err(|_| QuantumError::Capacity)?;
        Ok(Self { second: None, ids })
    }
    pub(super) fn settle(
        &mut self,
        geometry: &Geometry,
        terminal: &TerminalState,
        targets: &[Option<(EventId, u64)>; 3],
    ) -> Result<Settlement, QuantumError> {
        let oldest = targets.iter().flatten().map(|(_, time)| *time).min();
        if terminal.gap().is_some() || (terminal.retained > 0 && oldest.is_none()) {
            self.second = None;
            self.ids.clear();
            return Ok(Settlement {
                disposition: "retry",
                continuation: geometry.upper,
                gap: None,
            });
        }
        let Some(oldest) = oldest else {
            self.second = None;
            self.ids.clear();
            return Ok(Settlement {
                disposition: "window_closed",
                continuation: geometry.window_end,
                gap: None,
            });
        };
        if self.second != Some(oldest) {
            self.second = Some(oldest);
            self.ids.clear();
        }
        let mut added = false;
        for (id, time) in targets.iter().flatten() {
            if *time != oldest {
                continue;
            }
            if let Err(index) = self.ids.binary_search(id) {
                if self.ids.len() == MAX_BOUNDARY_IDS {
                    return Err(QuantumError::Context);
                }
                self.ids.insert(index, *id);
                added = true;
            }
        }
        if added {
            Ok(Settlement {
                disposition: "overlap",
                continuation: oldest.checked_add(1).ok_or(QuantumError::Context)?,
                gap: None,
            })
        } else {
            self.second = None;
            self.ids.clear();
            Ok(Settlement {
                disposition: "saturated_second",
                continuation: oldest,
                gap: Some((oldest, oldest.checked_add(1).ok_or(QuantumError::Context)?)),
            })
        }
    }
}
#[derive(Debug, PartialEq, Eq)]
struct DecodedEffects {
    ordering: bool,
    targets: [Option<(EventId, u64)>; 3],
}
// Historical authentication retains only these fixed scalars beside the full
// Fact decode grant. Count decoder/return overlap, matching state, the current
// reference/result and fixed counters/slice/range/closure headers. It allocates
// neither the live Boundary IDs nor a wire parser.
pub(in crate::storage::postgres::control_owner) const HISTORICAL_SCALAR_BYTES: usize = 2
    * size_of::<DecodedEffects>()
    + size_of::<[bool; 3]>()
    + size_of::<Option<bigbrotr_model::SourceRecordId>>()
    + size_of::<FactRef>()
    + 2 * size_of::<Option<(EventId, u64)>>()
    + 16 * size_of::<u64>();
pub(in crate::storage::postgres::control_owner) struct Traversal {
    pub geometry: Geometry,
    pub(super) boundary: Boundary,
    pub entries: u16,
}
fn number(row: &Row, column: &str) -> Result<u64, QuantumError> {
    let text: &str = row.try_get(column).map_err(failure)?;
    let value = text.parse::<u64>().map_err(|_| QuantumError::Context)?;
    if value.to_string() != text {
        return Err(QuantumError::Context);
    }
    Ok(value)
}
pub(super) fn retained_settlement(row: &Row) -> Result<Settlement, QuantumError> {
    let disposition: &str = row.try_get("disposition").map_err(failure)?;
    let disposition = match disposition {
        "retry" => "retry",
        "overlap" => "overlap",
        "saturated_second" => "saturated_second",
        "window_closed" => "window_closed",
        _ => return Err(QuantumError::Context),
    };
    let gap = match (
        row.try_get::<_, Option<&str>>("gap_lower")
            .map_err(failure)?,
        row.try_get::<_, Option<&str>>("gap_upper")
            .map_err(failure)?,
    ) {
        (None, None) => None,
        (Some(a), Some(b)) => Some((
            a.parse().map_err(|_| QuantumError::Context)?,
            b.parse().map_err(|_| QuantumError::Context)?,
        )),
        _ => return Err(QuantumError::Context),
    };
    Ok(Settlement {
        disposition,
        continuation: number(row, "continuation")?,
        gap,
    })
}
/// Native authority is an explicit precondition of the geometry fold. All rows,
/// including earlier windows and local entries, are consumed before any no-op.
#[allow(
    clippy::too_many_arguments,
    reason = "same locked job/source/native context"
)]
pub(in crate::storage::postgres::control_owner) async fn next_window(
    tx: &Transaction<'_>,
    native: &ProgressRead<'_>,
    context: Context,
    job: &AcquisitionJob,
    scope: &StoredAcquisitionScope,
    source: RelaySourceBinding,
    control: ControlOwner,
    width: u64,
    budget: u16,
    authority: &mut impl FnMut() -> Result<(), AdmissionError>,
) -> Result<Traversal, QuantumError> {
    let geometry = Geometry::new(
        scope.selector().history_since().0,
        LinearWindow::frozen_horizon(scope.selector(), job.accepted_at_us)?,
        width,
        budget,
    )?;
    let mut traversal = Traversal {
        geometry,
        boundary: Boundary::new()?,
        entries: 0,
    };
    let epoch = context.epoch.to_string();
    let scope_id = scope.identity();
    let params: &[&(dyn tokio_postgres::types::ToSql + Sync)] = &[
        &&job.id.as_bytes()[..],
        &&context.dataset.as_bytes()[..],
        &&scope_id.as_bytes()[..],
        &source.enrollment_generation.get().to_string(),
        &&source.route_id.as_bytes()[..],
        &&context.slot.as_bytes()[..],
        &epoch,
        &&context.manifest[..],
        &&control.resource_id.as_bytes()[..],
        &control.epoch.get().to_string(),
        &&control.process_incarnation.as_bytes()[..],
    ];
    let stream=tx.query_raw("SELECT substring(quantum_id FROM 1 FOR 17) AS quantum_id,
        quantum_number::text AS number, interval_lower::text AS lower,interval_upper::text AS upper,
        horizon::text AS horizon, window_end::text AS window_end,window_width::text AS width,
        query_budget,query_number,entry_kind,disposition,gap_lower::text AS gap_lower,gap_upper::text AS gap_upper,
        quantum_profile=2 AND dataset_id=$2 AND scope_id=$3 AND enrollment_generation=$4::text::numeric
        AND route_id=$5 AND producer_slot_id=$6 AND producer_epoch=$7::text::numeric AND manifest_sha256=$8
        AND issuer_resource=$9 AND issuer_epoch=$10::text::numeric AND issuer_role='control'
        AND issuer_incarnation=$11 AS identity_valid,
        left(terminal_kind,13) AS terminal_kind,substring(terminal_frame FROM 1 FOR 2097153) AS frame,
        substring(request_id FROM 1 FOR 49) AS request,terminal_sequence::text AS sequence,
        substring(request_predicate FROM 1 FOR 2097153) AS predicate,substring(predicate_sha256 FROM 1 FOR 33) AS predicate_digest,
        teardown_complete,retained_events,classified_prefix,substring(effect_records FROM 1 FOR 280) AS effects,
        substring(dependencies FROM 1 FOR 55297) AS dependencies,result_class,gap_reason,
        continuation_upper::text AS continuation,checkpoint_before<=checkpoint_after AS checkpoint_valid
        FROM control.acquisition_quanta WHERE job_id=$1 ORDER BY quantum_number LIMIT 386",
        params.iter().copied()).await.map_err(failure)?;
    futures_util::pin_mut!(stream);
    while let Some(row) = stream.try_next().await.map_err(failure)? {
        authority().map_err(QuantumError::Native)?;
        if traversal.entries as usize > 3 * usize::from(budget) {
            return Err(QuantumError::Context);
        }
        traversal.entries += 1;
        if !row.try_get::<_, bool>("identity_valid").map_err(failure)?
            || number(&row, "number")? != u64::from(traversal.entries)
            || number(&row, "horizon")? != geometry.horizon
            || number(&row, "width")? != width
            || row.try_get::<_, i16>("query_budget").map_err(failure)? != budget as i16
        {
            return Err(QuantumError::Context);
        }
        let lower = number(&row, "lower")?;
        let upper = number(&row, "upper")?;
        let end = number(&row, "window_end")?;
        let spent = u16::try_from(row.try_get::<_, i16>("query_number").map_err(failure)?)
            .map_err(|_| QuantumError::Context)?;
        if row.try_get::<_, &str>("entry_kind").map_err(failure)? == "selector_exclusion" {
            let span = traversal
                .geometry
                .exclusion(scope.selector())
                .ok_or(QuantumError::Context)?;
            if (span.lower, span.upper, span.window_end) != (lower, upper, end)
                || spent != traversal.geometry.spent
                || row.try_get::<_, &str>("disposition").map_err(failure)?
                    != if span.empty_target {
                        "empty_target"
                    } else {
                        "selector_excluded"
                    }
            {
                return Err(QuantumError::Context);
            }
            traversal.geometry.exclude(span)?;
            traversal.boundary.second = None;
            traversal.boundary.ids.clear();
            continue;
        }
        if row.try_get::<_, &str>("entry_kind").map_err(failure)? != "request" {
            return Err(QuantumError::Context);
        }
        traversal.geometry.request(lower, upper, end, spent)?;
        let window = LinearWindow::retained(
            scope.selector(),
            job.accepted_at_us,
            lower,
            upper,
            geometry.horizon,
        )?;
        validate_page(native, &row, &window, &mut traversal, authority)?;
    }
    Ok(traversal)
}
fn validate_page(
    native: &ProgressRead<'_>,
    row: &Row,
    window: &LinearWindow,
    traversal: &mut Traversal,
    authority: &mut impl FnMut() -> Result<(), AdmissionError>,
) -> Result<(), QuantumError> {
    let id: &[u8] = row.try_get("quantum_id").map_err(failure)?;
    if id.len() != 16 {
        return Err(QuantumError::Context);
    }
    if row.try_get::<_, &[u8]>("predicate").map_err(failure)? != window.predicate()
        || row
            .try_get::<_, &[u8]>("predicate_digest")
            .map_err(failure)?
            != window.predicate_digest()
    {
        return Err(QuantumError::Context);
    }
    let kind = terminal_kind(
        row.try_get::<_, Option<&str>>("terminal_kind")
            .map_err(failure)?
            .ok_or(QuantumError::Context)?,
    )?;
    let retained: i16 = row.try_get("retained_events").map_err(failure)?;
    let prefix: i16 = row.try_get("classified_prefix").map_err(failure)?;
    if !(0..=3).contains(&retained)
        || !(0..=retained).contains(&prefix)
        || !row
            .try_get::<_, bool>("checkpoint_valid")
            .map_err(failure)?
    {
        return Err(QuantumError::Context);
    }
    let decoded = decode_effects(
        row.try_get("effects").map_err(failure)?,
        retained as u16,
        prefix as u16,
        window.lower(),
        window.upper(),
    )?;
    let frame: &[u8] = row.try_get("frame").map_err(failure)?;
    let request: Option<&[u8]> = row.try_get("request").map_err(failure)?;
    if request.is_some_and(|value| {
        value.len() != 48
            || !value
                .iter()
                .all(|v| v.is_ascii_digit() || (b'a'..=b'f').contains(v))
    }) {
        return Err(QuantumError::Context);
    }
    let mut flags = (false, false, false);
    let sequence: Option<&str> = row.try_get("sequence").map_err(failure)?;
    if matches!(kind, TerminalKind::Eose | TerminalKind::Closed) {
        let request = request.ok_or(QuantumError::Context)?;
        let sequence = sequence
            .ok_or(QuantumError::Context)?
            .parse::<u64>()
            .map_err(|_| QuantumError::Context)?;
        let effects: &[u8] = row.try_get("effects").map_err(failure)?;
        let last = effects
            .as_chunks::<93>()
            .0
            .iter()
            .map(|r| u64::from_be_bytes(r[2..10].try_into().expect("effect")))
            .max()
            .unwrap_or(0);
        if sequence <= last {
            return Err(QuantumError::Context);
        }
        let envelope = decode_relay(frame, bigbrotr_protocol::EventLimits::default())
            .map_err(|_| QuantumError::Context)?;
        match (kind, envelope.message()) {
            (
                TerminalKind::Eose,
                RelayMessage::Eose {
                    subscription,
                    hints,
                },
            ) if subscription.as_bytes() == request => {
                let has = |wanted: &str| {
                    hints
                        .as_ref()
                        .is_some_and(|v| v.iter().any(|v| v.as_ref() == wanted))
                };
                flags = (has("finish"), has("more"), has("auth"));
            }
            (TerminalKind::Closed, RelayMessage::Closed { subscription, .. })
                if subscription.as_bytes() == request => {}
            _ => return Err(QuantumError::Context),
        }
    } else if !frame.is_empty() || sequence.is_some() || (retained > 0 && request.is_none()) {
        return Err(QuantumError::Context);
    }
    // Authenticate against this row's union BEFORE any boundary or geometry use.
    require_payloads(
        row.try_get("dependencies").map_err(failure)?,
        &decoded.targets,
        |reference| {
            native
                .event_payload_time(reference, &mut *authority)
                .map_err(QuantumError::Native)
        },
    )?;
    let terminal = TerminalState {
        kind,
        teardown: row.try_get("teardown_complete").map_err(failure)?,
        prefix: prefix as usize,
        retained: retained as usize,
        ordering: decoded.ordering,
        finish: flags.0,
        more: flags.1,
        auth: flags.2,
    };
    let settlement = traversal
        .boundary
        .settle(&traversal.geometry, &terminal, &decoded.targets)?;
    let (class, gap) = page_result(&terminal, &decoded.targets, &settlement);
    if row.try_get::<_, &str>("result_class").map_err(failure)? != class
        || row
            .try_get::<_, Option<&str>>("gap_reason")
            .map_err(failure)?
            != gap
        || retained_settlement(row)? != settlement
    {
        return Err(QuantumError::Context);
    }
    traversal.geometry.finish(settlement)
}
pub(super) fn page_result(
    terminal: &TerminalState,
    targets: &[Option<(EventId, u64)>; 3],
    settlement: &Settlement,
) -> (&'static str, Option<&'static str>) {
    if settlement.disposition == "saturated_second" {
        (terminal.class(), Some("saturated_second"))
    } else if terminal.retained > 0
        && targets.iter().all(Option::is_none)
        && terminal.gap().is_none()
    {
        ("interrupted", Some("no_verified_boundary"))
    } else {
        (terminal.class(), terminal.gap())
    }
}
fn terminal_kind(value: &str) -> Result<TerminalKind, QuantumError> {
    Ok(match value {
        "eose" => TerminalKind::Eose,
        "closed" => TerminalKind::Closed,
        "deadline" => TerminalKind::Deadline,
        "cancelled" => TerminalKind::Cancelled,
        "frame_cap" => TerminalKind::FrameCap,
        "event_cap" => TerminalKind::EventCap,
        "transport" => TerminalKind::Transport,
        "decode" => TerminalKind::Decode,
        "disconnected" => TerminalKind::Disconnected,
        _ => return Err(QuantumError::Context),
    })
}

fn require_payloads(
    dependencies: &[u8],
    targets: &[Option<(EventId, u64)>; 3],
    mut lookup: impl FnMut(FactRef) -> Result<Option<(EventId, u64)>, QuantumError>,
) -> Result<(), QuantumError> {
    if dependencies.len() > 768 * 72 || !dependencies.len().is_multiple_of(72) {
        return Err(QuantumError::Context);
    }
    let mut matched = [false; 3];
    let mut previous = None;
    for bytes in dependencies.as_chunks::<72>().0 {
        let reference = rows::decode_dependency(bytes).map_err(|_| QuantumError::Context)?;
        if previous.is_some_and(|old| old >= reference.source_record_id) {
            return Err(QuantumError::Context);
        }
        previous = Some(reference.source_record_id);
        let found = lookup(reference)?;
        for (index, target) in targets.iter().enumerate() {
            matched[index] |= target.is_some() && *target == found;
        }
    }
    if targets
        .iter()
        .zip(matched)
        .any(|(target, found)| target.is_some() && !found)
    {
        return Err(QuantumError::Context);
    }
    Ok(())
}

fn decode_effects(
    bytes: &[u8],
    retained: u16,
    prefix: u16,
    lower: u64,
    upper: u64,
) -> Result<DecodedEffects, QuantumError> {
    if bytes.len() > 3 * 93 || !bytes.len().is_multiple_of(93) {
        return Err(QuantumError::Context);
    }
    let (mut ordinal, mut wire, mut classified, mut previous_time, mut ordering) =
        (0, 0, 0, None, false);
    let mut targets = [None; 3];
    for record in bytes.as_chunks::<93>().0 {
        let next = u16::from_be_bytes(record[..2].try_into().expect("effect"));
        let sequence = u64::from_be_bytes(record[2..10].try_into().expect("effect"));
        let kind = u16::from_be_bytes(record[42..44].try_into().expect("effect"));
        let mutation = u64::from_be_bytes(record[44..52].try_into().expect("effect"));
        let time = u64::from_be_bytes(record[52..60].try_into().expect("effect"));
        if next <= ordinal
            || next > retained
            || sequence <= wire
            || !(2..=6).contains(&kind)
            || record[60] > 1
            || ((kind <= 4) != (mutation > 0))
        {
            return Err(QuantumError::Context);
        }
        if record[60] == 1 {
            if next != classified + 1 {
                return Err(QuantumError::Context);
            }
            classified += 1;
        }
        if matches!(kind, 2 | 3 | 5) {
            if !(lower..upper).contains(&time) {
                return Err(QuantumError::Context);
            }
            ordering |= previous_time.is_some_and(|previous| time > previous);
            previous_time = Some(time);
            targets[usize::from(next) - 1] = Some((
                EventId::from_bytes(record[61..93].try_into().expect("effect")),
                time,
            ));
        }
        ordinal = next;
        wire = sequence;
    }
    if classified != prefix {
        return Err(QuantumError::Context);
    }
    Ok(DecodedEffects { ordering, targets })
}

/// Redacted observation only. This repeats the same bounded geometry reducer;
/// unlike planning, it supplies no native capability or physical-integrity claim.
pub(in crate::storage::postgres::control_owner) async fn observe_status(
    tx: &Transaction<'_>,
    job: bigbrotr_model::JobId,
) -> Result<Option<serde_json::Value>, QuantumError> {
    let params: &[&(dyn tokio_postgres::types::ToSql + Sync)] = &[&&job.as_bytes()[..]];
    let stream=tx.query_raw("SELECT quantum_profile,quantum_number::text AS number,entry_kind,disposition,
        encode(scope_id,'hex') AS scope,interval_lower::text AS lower,interval_upper::text AS upper,
        window_end::text AS window_end,window_width::text AS width,horizon::text AS horizon,
        query_budget,query_number,result_class,terminal_kind,gap_reason,
        continuation_upper::text AS continuation,gap_lower::text AS gap_lower,gap_upper::text AS gap_upper,
        retained_events,classified_prefix FROM control.acquisition_quanta WHERE job_id=$1
        ORDER BY quantum_number LIMIT 386",params.iter().copied()).await.map_err(failure)?;
    futures_util::pin_mut!(stream);
    let mut geometry: Option<Geometry> = None;
    let mut result = None;
    let mut count = 0u16;
    let mut scope: Option<String> = None;
    while let Some(row) = stream.try_next().await.map_err(failure)? {
        count += 1;
        let lower = number(&row, "lower")?;
        let upper = number(&row, "upper")?;
        let end = number(&row, "window_end")?;
        let horizon = number(&row, "horizon")?;
        let width = number(&row, "width")?;
        let budget = u16::try_from(row.try_get::<_, i16>("query_budget").map_err(failure)?)
            .map_err(|_| QuantumError::Context)?;
        let spent = u16::try_from(row.try_get::<_, i16>("query_number").map_err(failure)?)
            .map_err(|_| QuantumError::Context)?;
        let current_scope: &str = row.try_get("scope").map_err(failure)?;
        if geometry.is_none() {
            geometry = Some(Geometry::new(lower, horizon, width, budget)?);
            scope = Some(current_scope.to_owned());
        }
        let geometry = geometry.as_mut().expect("first row initialized");
        if row.try_get::<_, i16>("quantum_profile").map_err(failure)? != 2
            || number(&row, "number")? != u64::from(count)
            || (horizon, width, budget) != (geometry.horizon, geometry.width, geometry.budget)
            || scope.as_deref() != Some(current_scope)
            || count > 3 * budget + 1
        {
            return Err(QuantumError::Context);
        }
        let entry: &str = row.try_get("entry_kind").map_err(failure)?;
        let disposition: Option<&str> = row.try_get("disposition").map_err(failure)?;
        match entry {
            "request" => {
                geometry.request(lower, upper, end, spent)?;
                if disposition.is_some() {
                    geometry.finish(retained_settlement(&row)?)?;
                }
            }
            "selector_exclusion" => {
                if spent != geometry.spent
                    || !matches!(disposition, Some("selector_excluded" | "empty_target"))
                {
                    return Err(QuantumError::Context);
                }
                geometry.exclude(Exclusion {
                    lower,
                    upper,
                    window_end: end,
                    empty_target: disposition == Some("empty_target"),
                })?;
            }
            _ => return Err(QuantumError::Context),
        }
        let pending =
            (!geometry.settled).then(|| [geometry.lower.to_string(), geometry.upper.to_string()]);
        let value = serde_json::json!({
            "format":"bigbrotr-linear-quantum-status/2","quantum_number":count.to_string(),"scope_id":current_scope,
            "entry_kind":entry,"disposition":disposition,"lower":lower.to_string(),"upper":upper.to_string(),
            "window_end":end.to_string(),"horizon":horizon.to_string(),"window_width":width.to_string(),
            "query_budget":budget,"queries_spent":geometry.spent,"attempted_prefix":geometry.attempted_prefix.to_string(),
            "gap_count":geometry.gap_count,"pending_work":geometry.pending_work(),"pending_interval":pending,
            "result":row.try_get::<_,Option<&str>>("result_class").map_err(failure)?.unwrap_or("pending"),
            "terminal":row.try_get::<_,Option<&str>>("terminal_kind").map_err(failure)?,
            "gap":row.try_get::<_,Option<&str>>("gap_reason").map_err(failure)?,
            "continuation_upper":row.try_get::<_,Option<&str>>("continuation").map_err(failure)?,
            "retained_events":row.try_get::<_,Option<i16>>("retained_events").map_err(failure)?,
            "classified_prefix":row.try_get::<_,Option<i16>>("classified_prefix").map_err(failure)?,"coverage":"pending"
        });
        if serde_json::to_vec(&value)
            .map_err(|_| QuantumError::Context)?
            .len()
            > 1024
        {
            return Err(QuantumError::Context);
        }
        result = Some(value);
    }
    Ok(result)
}

pub(super) fn authenticate_saved_effects(
    native: &ProgressRead<'_>,
    effects: &[u8],
    retained: i16,
    prefix: i16,
    interval: std::ops::Range<u64>,
    dependencies: &[u8],
    authority: &mut impl FnMut() -> Result<(), AdmissionError>,
) -> Result<(), QuantumError> {
    if !(0..=3).contains(&retained)
        || !(0..=retained).contains(&prefix)
        || interval.start >= interval.end
    {
        return Err(QuantumError::Context);
    }
    let decoded = decode_effects(
        effects,
        retained as u16,
        prefix as u16,
        interval.start,
        interval.end,
    )?;
    require_payloads(dependencies, &decoded.targets, |reference| {
        native
            .event_payload_time(reference, &mut *authority)
            .map_err(QuantumError::Native)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn effect(ordinal: u16, sequence: u64, kind: u16, time: u64, prefix: u8) -> [u8; 93] {
        let mut record = [0; 93];
        record[..2].copy_from_slice(&ordinal.to_be_bytes());
        record[2..10].copy_from_slice(&sequence.to_be_bytes());
        record[42..44].copy_from_slice(&kind.to_be_bytes());
        if kind <= 4 {
            record[44..52].copy_from_slice(&1u64.to_be_bytes());
        }
        record[52..60].copy_from_slice(&time.to_be_bytes());
        record[60] = prefix;
        record[61..].copy_from_slice(&[1; 32]);
        record
    }
    fn summary(
        bytes: &[u8],
        retained: u16,
        prefix: u16,
        window: &LinearWindow,
    ) -> Result<(Option<u64>, bool), QuantumError> {
        decode_effects(bytes, retained, prefix, window.lower(), window.upper()).map(|value| {
            (
                value.targets.iter().flatten().map(|(_, time)| *time).min(),
                value.ordering,
            )
        })
    }
    #[test]
    fn retained_effects_distinguish_valid_times_ordering_holes_and_invalid_shape() {
        use bigbrotr_model::acquisition::{
            CollectionSelector, EphemeralPolicy, Filter, FilterFields,
        };
        let selector = CollectionSelector::new(
            vec![Filter::new(FilterFields::default()).unwrap()],
            bigbrotr_model::NostrTime(0),
            EphemeralPolicy::Retain,
        )
        .unwrap();
        let window = LinearWindow::first(&selector, 200_000_000_000, 86_400).unwrap();
        let known = effect(1, 2, 5, 1, 1);
        assert_eq!(summary(&known, 1, 1, &window), Ok((Some(1), false)));
        let mut ascending = known.to_vec();
        ascending.extend(effect(2, 3, 2, 2, 1));
        assert_eq!(summary(&ascending, 2, 2, &window), Ok((Some(1), true)));
        for bad in [
            effect(0, 1, 5, 1, 1),
            effect(1, 0, 5, 1, 1),
            effect(1, 1, 1, 1, 1),
            effect(1, 1, 5, 86_400, 1),
            effect(2, 1, 5, 1, 1),
            effect(1, 1, 5, 1, 2),
        ] {
            assert_eq!(summary(&bad, 2, 1, &window), Err(QuantumError::Context));
        }
        assert_eq!(
            summary(&known[..60], 1, 1, &window),
            Err(QuantumError::Context)
        );
        assert_eq!(summary(&known, 1, 0, &window), Err(QuantumError::Context));
    }
    #[test]
    fn cursor_uses_only_exact_payload_pairs_in_this_pages_dependencies() {
        use bigbrotr_model::acquisition::{
            CollectionSelector, EphemeralPolicy, Filter, FilterFields,
        };
        let selector = CollectionSelector::new(
            vec![Filter::new(FilterFields::default()).unwrap()],
            bigbrotr_model::NostrTime(0),
            EphemeralPolicy::Retain,
        )
        .unwrap();
        let window = LinearWindow::first(&selector, 200_000_000_000, 100).unwrap();
        let id = |value| EventId::from_bytes([value; 32]);
        let dependency = |sequence: u64| {
            let mut bytes = [0; 72];
            bytes[..16].copy_from_slice(&[1; 16]);
            bytes[24..32].copy_from_slice(&1u64.to_be_bytes());
            bytes[32..40].copy_from_slice(&sequence.to_be_bytes());
            bytes[40..].copy_from_slice(&[2; 32]);
            bytes
        };
        let refs = [dependency(1), dependency(2)].concat();
        // This tests the actual row-to-native-pair reducer, not native authority.
        // Separate Rocks cases qualify the lookup's integrity/identity boundary.
        let lookup = |reference: FactRef| {
            Ok(match reference.source_record_id.sequence.get() {
                1 => Some((id(1), 80)),
                2 => Some((id(2), 60)),
                3 => Some((id(3), 20)),
                _ => None,
            })
        };
        let mut second = effect(2, 2, 5, 60, 1);
        second[61..].copy_from_slice(id(2).as_bytes());
        let good = [effect(1, 1, 2, 80, 1), second].concat();
        let decoded = decode_effects(&good, 2, 2, window.lower(), window.upper()).unwrap();
        require_payloads(&refs, &decoded.targets, lookup).unwrap();
        assert_eq!(
            window.continuation(
                decoded
                    .targets
                    .iter()
                    .flatten()
                    .map(|(_, time)| *time)
                    .min()
            ),
            61
        );

        let mut forged = good.clone();
        forged[93 + 52..93 + 60].copy_from_slice(&20u64.to_be_bytes());
        let decoded = decode_effects(&forged, 2, 2, window.lower(), window.upper()).unwrap();
        assert_eq!(
            window.continuation(
                decoded
                    .targets
                    .iter()
                    .flatten()
                    .map(|(_, time)| *time)
                    .min()
            ),
            21
        ); // Formula alone accepts this.
        assert_eq!(
            require_payloads(&refs, &decoded.targets, lookup),
            Err(QuantumError::Context)
        );
        forged[93 + 61..93 + 93].copy_from_slice(id(3).as_bytes());
        let decoded = decode_effects(&forged, 2, 2, window.lower(), window.upper()).unwrap();
        // The correct pair exists globally at3, but not in this row's union.
        assert_eq!(
            require_payloads(&refs, &decoded.targets, lookup),
            Err(QuantumError::Context)
        );

        let proof = decode_effects(
            &effect(1, 1, 5, 80, 1),
            1,
            1,
            window.lower(),
            window.upper(),
        )
        .unwrap();
        require_payloads(&dependency(1), &proof.targets, lookup).unwrap();
        assert_eq!(
            require_payloads(&dependency(1), &proof.targets, |_| Ok(None)),
            Err(QuantumError::Context)
        );
        assert_eq!(
            require_payloads(&[], &proof.targets, lookup),
            Err(QuantumError::Context)
        );
        assert_eq!(
            require_payloads(&refs, &proof.targets, |_| Err(QuantumError::Native(
                AdmissionError::StateMismatch
            ))),
            Err(QuantumError::Native(AdmissionError::StateMismatch))
        );
        assert_eq!(
            decode_effects(&good[..61], 1, 1, window.lower(), window.upper()),
            Err(QuantumError::Context)
        );
    }
    #[test]
    fn same_second_accumulates_ids_and_interruption_resets_comparison() {
        let mut boundary = Boundary::new().unwrap();
        let geometry = Geometry::new(0, 100, 100, 128).unwrap();
        let terminal = TerminalState {
            kind: TerminalKind::Eose,
            teardown: true,
            prefix: 1,
            retained: 1,
            ordering: false,
            finish: false,
            more: false,
            auth: false,
        };
        let a = Some((EventId::from_bytes([1; 32]), 23));
        let b = Some((EventId::from_bytes([2; 32]), 23));
        assert_eq!(
            boundary
                .settle(&geometry, &terminal, &[a, None, None])
                .unwrap()
                .disposition,
            "overlap"
        );
        assert_eq!(
            boundary
                .settle(&geometry, &terminal, &[b, None, None])
                .unwrap()
                .disposition,
            "overlap"
        );
        assert_eq!(boundary.ids.len(), 2);
        assert_eq!(
            boundary
                .settle(&geometry, &terminal, &[a, b, None])
                .unwrap(),
            Settlement {
                disposition: "saturated_second",
                continuation: 23,
                gap: Some((23, 24))
            }
        );
        assert!(boundary.ids.is_empty());
        boundary
            .settle(&geometry, &terminal, &[a, None, None])
            .unwrap();
        let interrupted = TerminalState {
            kind: TerminalKind::Closed,
            ..terminal
        };
        assert_eq!(
            boundary
                .settle(&geometry, &interrupted, &[a, None, None])
                .unwrap()
                .disposition,
            "retry"
        );
        assert_eq!(
            boundary
                .settle(&geometry, &terminal, &[a, None, None])
                .unwrap()
                .disposition,
            "overlap"
        );
        let older = Some((EventId::from_bytes([3; 32]), 1));
        boundary
            .settle(&geometry, &terminal, &[older, None, None])
            .unwrap();
        assert_eq!(boundary.second, Some(1));
        assert_eq!(boundary.ids.len(), 1);
    }
    #[test]
    fn rejection_only_is_retry_and_only_actual_empty_closes_window() {
        let mut boundary = Boundary::new().unwrap();
        let geometry = Geometry::new(0, 100, 100, 128).unwrap();
        let rejected = TerminalState {
            kind: TerminalKind::Eose,
            teardown: true,
            prefix: 1,
            retained: 1,
            ordering: false,
            finish: false,
            more: false,
            auth: false,
        };
        let settlement = boundary.settle(&geometry, &rejected, &[None; 3]).unwrap();
        assert_eq!(settlement.disposition, "retry");
        assert_eq!(
            page_result(&rejected, &[None; 3], &settlement),
            ("interrupted", Some("no_verified_boundary"))
        );
        let empty = TerminalState {
            prefix: 0,
            retained: 0,
            ..rejected
        };
        assert_eq!(
            boundary
                .settle(&geometry, &empty, &[None; 3])
                .unwrap()
                .disposition,
            "window_closed"
        );
    }
}
