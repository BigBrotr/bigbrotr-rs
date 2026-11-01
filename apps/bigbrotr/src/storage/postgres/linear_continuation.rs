//! Consume retained page evidence under the existing acquisition job lock.
//! A stopped walk remains pending; no empty page or repeated second is coverage.
use super::*;
use crate::acquisition::scope::StoredAcquisitionScope;
use crate::storage::rocks::store::{AdmissionError, ProgressRead};
use bigbrotr_model::EventId;
use bigbrotr_protocol::wire::{RelayMessage, decode_relay};
use tokio_postgres::Row;

#[derive(Clone, Copy)]
struct Completed {
    number: u64,
    lower: u64,
    upper: u64,
    horizon: u64,
    continuation: u64,
    oldest: Option<u64>,
    interrupted: bool,
}

#[derive(Debug, PartialEq, Eq)]
struct DecodedEffects {
    oldest: Option<u64>,
    ordering: bool,
    targets: [Option<(EventId, u64)>; 3],
}

fn number(row: &Row, column: &str) -> Result<u64, QuantumError> {
    let text: &str = row.try_get(column).map_err(failure)?;
    let value = text.parse::<u64>().map_err(|_| QuantumError::Context)?;
    if value.to_string() != text {
        return Err(QuantumError::Context);
    }
    Ok(value)
}

#[allow(
    clippy::too_many_arguments,
    reason = "same locked job, source and fenced native/Control context as prepare"
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
    authority: &mut impl FnMut() -> Result<(), AdmissionError>,
) -> Result<Option<LinearWindow>, QuantumError> {
    // Look up by job alone. Filtering by compatible identity would hide a
    // retained incompatible/unfinished obligation and incorrectly restart it.
    let latest = read(
        tx, native, context, job, scope, source, control, None, authority,
    )
    .await?;
    let Some(latest) = latest else {
        return match LinearWindow::first(scope.selector(), job.accepted_at_us, width) {
            Ok(window) => Ok(Some(window)),
            Err(QuantumError::Interval) => Ok(None),
            Err(error) => Err(error),
        };
    };
    let previous = if latest.number > 1 {
        Some(
            read(
                tx,
                native,
                context,
                job,
                scope,
                source,
                control,
                Some(latest.number - 1),
                authority,
            )
            .await?
            .ok_or(QuantumError::Context)?,
        )
    } else {
        None
    };
    let upper = choose(&latest, previous.as_ref())?;
    upper
        .map(|upper| {
            LinearWindow::retained(
                scope.selector(),
                job.accepted_at_us,
                latest.lower,
                upper,
                latest.horizon,
            )
        })
        .transpose()
}

fn choose(latest: &Completed, previous: Option<&Completed>) -> Result<Option<u64>, QuantumError> {
    if latest.number == 0 || (latest.number == 1) != previous.is_none() {
        return Err(QuantumError::Context);
    }
    if let Some(previous) = previous {
        if previous.number.checked_add(1) != Some(latest.number)
            || (previous.lower, previous.horizon) != (latest.lower, latest.horizon)
            || (!previous.interrupted && previous.oldest.is_none())
            || latest.upper != previous.continuation
        {
            return Err(QuantumError::Context);
        }
    }
    if latest.interrupted {
        return Ok(Some(latest.upper));
    }
    // The first returned boundary is allowed its overlap, even at upper-1.
    // A second successful page exposing no older time stops this bounded slice.
    // Canonical-ID-aware saturated-second partitioning belongs to later T05 work.
    if latest.oldest.is_none()
        || previous
            .is_some_and(|previous| !previous.interrupted && previous.oldest == latest.oldest)
    {
        Ok(None)
    } else {
        Ok(Some(latest.continuation))
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "retained identity is checked against the same current planner inputs"
)]
async fn read(
    tx: &Transaction<'_>,
    native: &ProgressRead<'_>,
    context: Context,
    job: &AcquisitionJob,
    scope: &StoredAcquisitionScope,
    source: RelaySourceBinding,
    control: ControlOwner,
    wanted: Option<u64>,
    authority: &mut impl FnMut() -> Result<(), AdmissionError>,
) -> Result<Option<Completed>, QuantumError> {
    let wanted = wanted.map(|value| value.to_string());
    // One bounded row at a time. Predicates are compared in SQL after canonical
    // reconstruction; neither two maximum predicates nor terminal decodes overlap.
    let row = tx.query_opt("SELECT substring(quantum_id FROM 1 FOR 17) AS quantum_id, left(quantum_number::text,21) AS number,
        left(interval_lower::text,21) AS lower, left(interval_upper::text,21) AS upper,
        left(horizon::text,21) AS horizon,
        quantum_profile=1 AND dataset_id=$2 AND scope_id=$3 AND enrollment_generation=$4::text::numeric
            AND route_id=$5 AND producer_slot_id=$6 AND producer_epoch=$7::text::numeric
            AND manifest_sha256=$8 AND issuer_resource=$9 AND issuer_epoch=$10::text::numeric
            AND issuer_role='control' AND issuer_incarnation=$11 AS identity_valid,
        left(terminal_kind,13) AS terminal_kind, substring(terminal_frame FROM 1 FOR 2097153) AS frame,
        substring(request_id FROM 1 FOR 49) AS request, left(terminal_sequence::text,21) AS sequence,
        teardown_complete, retained_events, classified_prefix,
        substring(effect_records FROM 1 FOR 280) AS effects,
        substring(dependencies FROM 1 FOR 55297) AS dependencies,
        left(result_class,26) AS result_class, left(gap_reason,20) AS gap_reason,
        left(continuation_upper::text,21) AS continuation,
        checkpoint_before<=checkpoint_after AS checkpoint_valid
        FROM control.acquisition_quanta WHERE job_id=$1 AND ($12::text IS NULL OR quantum_number=$12::text::numeric)
        ORDER BY quantum_number DESC LIMIT 1",
        &[&&job.id.as_bytes()[..], &&context.dataset.as_bytes()[..], &&scope.identity().as_bytes()[..],
            &source.enrollment_generation.get().to_string(), &&source.route_id.as_bytes()[..],
            &&context.slot.as_bytes()[..], &context.epoch.to_string(), &&context.manifest[..],
            &&control.resource_id.as_bytes()[..], &control.epoch.get().to_string(),
            &&control.process_incarnation.as_bytes()[..], &wanted]).await.map_err(failure)?;
    let Some(row) = row else { return Ok(None) };
    if !row.try_get::<_, bool>("identity_valid").map_err(failure)? {
        return Err(QuantumError::Context);
    }
    let number = number(&row, "number")?;
    let lower = self::number(&row, "lower")?;
    let upper = self::number(&row, "upper")?;
    let horizon = self::number(&row, "horizon")?;
    let window =
        LinearWindow::retained(scope.selector(), job.accepted_at_us, lower, upper, horizon)?;
    let id: &[u8] = row.try_get("quantum_id").map_err(failure)?;
    if id.len() != 16 {
        return Err(QuantumError::Context);
    }
    let predicate_valid: bool = tx
        .query_one(
            "SELECT request_predicate=$2 AND predicate_sha256=$3
        FROM control.acquisition_quanta WHERE quantum_id=$1",
            &[&id, &window.predicate(), &&window.predicate_digest()[..]],
        )
        .await
        .map_err(failure)?
        .try_get(0)
        .map_err(failure)?;
    if !predicate_valid || number == 0 {
        return Err(QuantumError::Context);
    }
    // An unfinished/uncertain commit is never a new cursor, even if its current
    // identity happens to match. The normal error path poisons this owner.
    let kind: Option<&str> = row.try_get("terminal_kind").map_err(failure)?;
    let kind = terminal_kind(kind.ok_or(QuantumError::Context)?)?;
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
    let effects: &[u8] = row.try_get("effects").map_err(failure)?;
    let DecodedEffects {
        oldest,
        ordering,
        targets,
    } = decode_effects(effects, retained as u16, prefix as u16, &window)?;
    let frame: &[u8] = row.try_get("frame").map_err(failure)?;
    let request: Option<&[u8]> = row.try_get("request").map_err(failure)?;
    if request.is_some_and(|value| {
        value.len() != 48
            || value
                .iter()
                .any(|value| !value.is_ascii_digit() && !(b'a'..=b'f').contains(value))
    }) {
        return Err(QuantumError::Context);
    }
    let mut flags = (false, false, false);
    let sequence: Option<&str> = row.try_get("sequence").map_err(failure)?;
    if matches!(kind, TerminalKind::Eose | TerminalKind::Closed) {
        let request = request
            .filter(|value| {
                value.len() == 48
                    && value
                        .iter()
                        .all(|value| value.is_ascii_digit() || (b'a'..=b'f').contains(value))
            })
            .ok_or(QuantumError::Context)?;
        let terminal_sequence = sequence
            .ok_or(QuantumError::Context)?
            .parse::<u64>()
            .map_err(|_| QuantumError::Context)?;
        let last = effects
            .chunks_exact(93)
            .map(|record| u64::from_be_bytes(record[2..10].try_into().expect("effect")))
            .max()
            .unwrap_or(0);
        if terminal_sequence <= last {
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
                        .is_some_and(|values| values.iter().any(|value| value.as_ref() == wanted))
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
    // Terminal decode allocations have ended before native Fact decoding uses
    // the same reserved scratch. Match only this quantum's exact dependency
    // union, never global body membership or another page's cumulative closure.
    let dependencies: &[u8] = row.try_get("dependencies").map_err(failure)?;
    require_payloads(dependencies, &targets, |reference| {
        native
            .event_payload_time(reference, &mut *authority)
            .map_err(QuantumError::Native)
    })?;
    let terminal = TerminalState {
        kind,
        teardown: row.try_get("teardown_complete").map_err(failure)?,
        prefix: prefix as usize,
        retained: retained as usize,
        ordering,
        finish: flags.0,
        more: flags.1,
        auth: flags.2,
    };
    let gap: Option<&str> = row.try_get("gap_reason").map_err(failure)?;
    let class: &str = row.try_get("result_class").map_err(failure)?;
    let continuation = self::number(&row, "continuation")?;
    if gap != terminal.gap()
        || class != terminal.class()
        || continuation
            != if gap.is_some() {
                upper
            } else {
                window.continuation(oldest)
            }
    {
        return Err(QuantumError::Context);
    }
    Ok(Some(Completed {
        number,
        lower,
        upper,
        horizon,
        continuation,
        oldest,
        interrupted: gap.is_some(),
    }))
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
    if dependencies.len() > 768 * 72 || dependencies.len() % 72 != 0 {
        return Err(QuantumError::Context);
    }
    let mut matched = [false; 3];
    let mut previous = None;
    for bytes in dependencies.chunks_exact(72) {
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
    window: &LinearWindow,
) -> Result<DecodedEffects, QuantumError> {
    if bytes.len() > 3 * 93 || bytes.len() % 93 != 0 {
        return Err(QuantumError::Context);
    }
    let (mut ordinal, mut wire, mut classified, mut oldest, mut previous_time, mut ordering) =
        (0, 0, 0, None, None, false);
    let mut targets = [None; 3];
    for record in bytes.chunks_exact(93) {
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
            if !(window.lower()..window.upper()).contains(&time) {
                return Err(QuantumError::Context);
            }
            oldest = Some(oldest.map_or(time, |old: u64| old.min(time)));
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
    Ok(DecodedEffects {
        oldest,
        ordering,
        targets,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn page(number: u64, upper: u64, oldest: Option<u64>) -> Completed {
        Completed {
            number,
            lower: 0,
            upper,
            horizon: 200_000,
            continuation: oldest.map_or(upper, |time| time + 1),
            oldest,
            interrupted: false,
        }
    }

    #[test]
    fn overlap_short_or_known_pages_and_stop_only_after_returned_boundary_repeats() {
        let first = page(1, 86_400, Some(1));
        assert_eq!(choose(&first, None), Ok(Some(2)));
        let older = page(2, 2, Some(0));
        assert_eq!(choose(&older, Some(&first)), Ok(Some(1)));
        let repeated_zero = page(3, 1, Some(0));
        assert_eq!(choose(&repeated_zero, Some(&older)), Ok(None));
        let at_upper = page(1, 1, Some(0));
        assert_eq!(choose(&at_upper, None), Ok(Some(1)));
        assert_eq!(choose(&page(2, 1, Some(0)), Some(&at_upper)), Ok(None));
        assert_eq!(choose(&page(1, 86_400, None), None), Ok(None));
    }

    #[test]
    fn known_interruption_retries_exact_interval_and_malformed_chain_refuses() {
        let first = Completed {
            interrupted: true,
            ..page(1, 86_400, None)
        };
        assert_eq!(choose(&first, None), Ok(Some(86_400)));
        let resumed = page(2, 86_400, Some(1));
        assert_eq!(choose(&resumed, Some(&first)), Ok(Some(2)));
        for bad in [
            Completed {
                number: 3,
                ..resumed
            },
            Completed {
                lower: 1,
                ..resumed
            },
            Completed {
                horizon: 199_999,
                ..resumed
            },
            Completed {
                upper: 2,
                ..resumed
            },
        ] {
            assert_eq!(choose(&bad, Some(&first)), Err(QuantumError::Context));
        }
        assert_eq!(choose(&resumed, None), Err(QuantumError::Context));
        assert_eq!(
            choose(&page(2, 86_400, Some(1)), Some(&page(1, 86_400, None))),
            Err(QuantumError::Context)
        );
    }

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
        decode_effects(bytes, retained, prefix, window).map(|value| (value.oldest, value.ordering))
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
        let decoded = decode_effects(&good, 2, 2, &window).unwrap();
        require_payloads(&refs, &decoded.targets, lookup).unwrap();
        assert_eq!(window.continuation(decoded.oldest), 61);

        let mut forged = good.clone();
        forged[93 + 52..93 + 60].copy_from_slice(&20u64.to_be_bytes());
        let decoded = decode_effects(&forged, 2, 2, &window).unwrap();
        assert_eq!(window.continuation(decoded.oldest), 21); // Formula alone accepts this.
        assert_eq!(
            require_payloads(&refs, &decoded.targets, lookup),
            Err(QuantumError::Context)
        );
        forged[93 + 61..93 + 93].copy_from_slice(id(3).as_bytes());
        let decoded = decode_effects(&forged, 2, 2, &window).unwrap();
        // The correct pair exists globally at3, but not in this row's union.
        assert_eq!(
            require_payloads(&refs, &decoded.targets, lookup),
            Err(QuantumError::Context)
        );

        let proof = decode_effects(&effect(1, 1, 5, 80, 1), 1, 1, &window).unwrap();
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
        let repeated = page(2, 81, Some(80));
        assert_eq!(choose(&repeated, Some(&page(1, 100, Some(80)))), Ok(None));
        assert_eq!(
            require_payloads(&[], &proof.targets, lookup)
                .and_then(|()| choose(&repeated, Some(&page(1, 100, Some(80))))),
            Err(QuantumError::Context)
        );
        assert_eq!(
            decode_effects(&good[..61], 1, 1, &window),
            Err(QuantumError::Context)
        );
    }
}
