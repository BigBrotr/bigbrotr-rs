//! One frozen linear interval. A request plan is neither execution nor coverage.
use crate::{acquisition::session::RequestId, runtime::OwnedPermit};
use bigbrotr_model::{
    NostrTime,
    acquisition::{CollectionSelector, Filter},
};
use bigbrotr_protocol::{EventLimits, wire::encode_request};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminalKind {
    Eose,
    Closed,
    Deadline,
    Cancelled,
    FrameCap,
    EventCap,
    Transport,
    Decode,
    Disconnected,
}

impl TerminalKind {
    pub(crate) fn code(self) -> &'static str {
        match self {
            Self::Eose => "eose",
            Self::Closed => "closed",
            Self::Deadline => "deadline",
            Self::Cancelled => "cancelled",
            Self::FrameCap => "frame_cap",
            Self::EventCap => "event_cap",
            Self::Transport => "transport",
            Self::Decode => "decode",
            Self::Disconnected => "disconnected",
        }
    }
}

/// Constructed from a correlated protocol message or an actual request handle.
/// Original remote text remains private; a local interruption invents no EOSE.
pub struct RequestTerminal {
    pub(crate) kind: TerminalKind,
    pub(crate) request: Option<RequestId>,
    pub(crate) sequence: Option<u64>,
    pub(crate) predicate: Option<[u8; 32]>,
    pub(crate) received_at_us: i64,
    pub(crate) raw: Vec<u8>,
    pub(crate) finish: bool,
    pub(crate) more: bool,
    pub(crate) auth: bool,
    pub(crate) _permit: OwnedPermit,
}
impl RequestTerminal {
    pub fn kind(&self) -> TerminalKind {
        self.kind
    }
    pub fn interruption(
        kind: TerminalKind,
        request: Option<RequestId>,
        permit: OwnedPermit,
    ) -> Result<Self, QuantumError> {
        if matches!(kind, TerminalKind::Eose | TerminalKind::Closed)
            || permit.request().units().data_bytes() < std::mem::size_of::<Self>() as u64
        {
            return Err(QuantumError::Context);
        }
        let received_at_us = super::receive_clock::ReceiveClock::new()
            .and_then(|mut clock| clock.sample())
            .map_err(|_| QuantumError::Context)?
            .received_at()
            .get();
        Ok(Self {
            kind,
            request,
            sequence: None,
            predicate: None,
            received_at_us,
            raw: Vec::new(),
            finish: false,
            more: false,
            auth: false,
            _permit: permit,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuantumError {
    Interval,
    Predicate,
    Capacity,
    Context,
    Backend,
    Native(crate::storage::rocks::store::AdmissionError),
    CommitUnknown,
}
impl std::fmt::Display for QuantumError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "linear quantum unavailable: {self:?}")
    }
}
impl std::error::Error for QuantumError {}

/// An exclusive timestamp upper bound; AfterMax does not wrap to zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Upper {
    Finite(u64),
    AfterMax,
}
impl Upper {
    pub fn inclusive(self) -> Option<u64> {
        match self {
            Self::Finite(value) => value.checked_sub(1),
            Self::AfterMax => Some(u64::MAX),
        }
    }
}

pub struct LinearWindow {
    lower: u64,
    upper: u64,
    horizon: u64,
    filters: Vec<Filter>,
    predicate: Vec<u8>,
    predicate_digest: [u8; 32],
}
impl LinearWindow {
    /// H excludes the current signed-time second. The pass horizon never moves
    /// when a later request or a restarted observer reads this obligation.
    pub fn first(
        selector: &CollectionSelector,
        accepted_at_us: i64,
        width: u64,
    ) -> Result<Self, QuantumError> {
        let horizon = Self::frozen_horizon(selector, accepted_at_us)?;
        let lower = selector.history_since().0;
        let upper = lower.saturating_add(width).min(horizon);
        Self::from_interval(selector, lower, upper, horizon)
    }

    /// Reconstruct a retained interval without applying the current window width.
    /// The caller must compare its retained predicate bytes and digest separately.
    pub(crate) fn retained(
        selector: &CollectionSelector,
        accepted_at_us: i64,
        lower: u64,
        upper: u64,
        horizon: u64,
    ) -> Result<Self, QuantumError> {
        if horizon != Self::frozen_horizon(selector, accepted_at_us)? {
            return Err(QuantumError::Interval);
        }
        Self::from_interval(selector, lower, upper, horizon)
    }

    pub(crate) fn frozen_horizon(
        selector: &CollectionSelector,
        accepted_at_us: i64,
    ) -> Result<u64, QuantumError> {
        let accepted_horizon = u64::try_from(accepted_at_us.div_euclid(1_000_000))
            .map_err(|_| QuantumError::Interval)?;
        // The union ends at its latest applicable conjunction. A maximum
        // inclusive bound is AfterMax; intersect before converting to finite H.
        selector
            .filters()
            .iter()
            .map(|filter| {
                filter
                    .fields()
                    .until
                    .and_then(|value| value.0.checked_add(1))
                    .unwrap_or(accepted_horizon)
                    .min(accepted_horizon)
            })
            .max()
            .ok_or(QuantumError::Interval)
    }

    fn from_interval(
        selector: &CollectionSelector,
        lower: u64,
        upper: u64,
        horizon: u64,
    ) -> Result<Self, QuantumError> {
        if lower < selector.history_since().0 || lower >= upper || upper > horizon {
            return Err(QuantumError::Interval);
        }
        let mut filters = Vec::with_capacity(selector.filters().len());
        for original in selector.filters() {
            let mut fields = original.fields().clone();
            let since = fields.since.map_or(lower, |value| value.0.max(lower));
            let until = fields
                .until
                .map_or(upper - 1, |value| value.0.min(upper - 1));
            if since > until {
                continue;
            }
            fields.since = Some(NostrTime(since));
            fields.until = Some(NostrTime(until));
            filters.push(Filter::new(fields).map_err(|_| QuantumError::Predicate)?);
        }
        if filters.is_empty() {
            return Err(QuantumError::Interval);
        }
        // A fixed placeholder excludes the unpredictable connection nonce from
        // predicate identity while preserving the exact serialized filter union.
        let predicate = encode_request(
            "000000000000000000000000000000000000000000000000",
            &filters,
            Some(3),
            EventLimits::default().wire_bytes,
        )
        .map_err(|_| QuantumError::Predicate)?
        .into_bytes();
        let predicate_digest = Sha256::digest(&predicate).into();
        Ok(Self {
            lower,
            upper,
            horizon,
            filters,
            predicate,
            predicate_digest,
        })
    }
    pub fn lower(&self) -> u64 {
        self.lower
    }
    pub fn upper(&self) -> u64 {
        self.upper
    }
    pub fn horizon(&self) -> u64 {
        self.horizon
    }
    pub fn filters(&self) -> &[Filter] {
        &self.filters
    }
    pub(crate) fn predicate(&self) -> &[u8] {
        &self.predicate
    }
    pub(crate) fn predicate_digest(&self) -> [u8; 32] {
        self.predicate_digest
    }
    /// Overlap the oldest verified second, including zero. This is pending
    /// continuation, never proof that a short or all-known page finished.
    pub fn continuation(&self, oldest: Option<u64>) -> u64 {
        oldest
            .filter(|value| *value >= self.lower && *value < self.upper)
            .and_then(|value| value.checked_add(1))
            .unwrap_or(self.upper)
    }
}

/// Geometry only: applying an entry never authenticates its effects. The planner
/// must validate its complete retained stream and each row's native payload union
/// before using this reducer. Status uses the same reducer for observations only.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Geometry {
    pub origin: u64,
    pub lower: u64,
    pub upper: u64,
    pub window_end: u64,
    pub horizon: u64,
    pub width: u64,
    pub budget: u16,
    pub spent: u16,
    pub attempted_prefix: u64,
    pub gap_count: u16,
    pub settled: bool,
    pub pending: bool,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Settlement {
    pub disposition: &'static str,
    pub continuation: u64,
    pub gap: Option<(u64, u64)>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Exclusion {
    pub lower: u64,
    pub upper: u64,
    pub window_end: u64,
    pub empty_target: bool,
}
impl Geometry {
    pub fn new(origin: u64, horizon: u64, width: u64, budget: u16) -> Result<Self, QuantumError> {
        if width == 0 || !(1..=128).contains(&budget) {
            return Err(QuantumError::Interval);
        }
        let end = origin.saturating_add(width).min(horizon);
        Ok(Self {
            origin,
            lower: origin,
            upper: end.max(origin),
            window_end: end.max(origin),
            horizon,
            width,
            budget,
            spent: 0,
            attempted_prefix: origin.min(horizon),
            gap_count: 0,
            settled: false,
            pending: false,
        })
    }
    pub fn pending_work(&self) -> &'static str {
        if self.pending {
            "request_pending"
        } else if self.settled {
            "settled"
        } else if self.spent == self.budget {
            "budget_deferred"
        } else {
            "query_ready"
        }
    }
    pub fn request(
        &mut self,
        lower: u64,
        upper: u64,
        end: u64,
        spent: u16,
    ) -> Result<(), QuantumError> {
        if self.settled
            || self.pending
            || self.spent == self.budget
            || (lower, upper, end) != (self.lower, self.upper, self.window_end)
            || lower >= upper
            || spent != self.spent + 1
        {
            return Err(QuantumError::Context);
        }
        self.spent = spent;
        self.pending = true;
        Ok(())
    }
    fn close(&mut self, end: u64) {
        self.attempted_prefix = end;
        self.lower = end;
        self.window_end = end.saturating_add(self.width).min(self.horizon);
        self.upper = self.window_end;
        self.settled = end == self.horizon;
    }
    pub fn finish(&mut self, value: Settlement) -> Result<(), QuantumError> {
        if !self.pending {
            return Err(QuantumError::Context);
        }
        match value.disposition {
            "retry" if value.continuation == self.upper && value.gap.is_none() => {}
            "overlap"
                if self.lower < value.continuation
                    && value.continuation <= self.upper
                    && value.gap.is_none() =>
            {
                self.upper = value.continuation;
            }
            "saturated_second" => {
                let (lower, upper) = value.gap.ok_or(QuantumError::Context)?;
                if lower < self.lower
                    || lower >= self.upper
                    || lower.checked_add(1) != Some(upper)
                    || value.continuation != lower
                {
                    return Err(QuantumError::Context);
                }
                self.gap_count = self.gap_count.checked_add(1).ok_or(QuantumError::Context)?;
                if lower == self.lower {
                    self.close(self.window_end);
                } else {
                    self.upper = lower;
                }
            }
            "window_closed" if value.gap.is_none() && value.continuation == self.window_end => {
                self.close(self.window_end);
            }
            _ => return Err(QuantumError::Context),
        }
        self.pending = false;
        Ok(())
    }
    /// Time algebra over the bounded conjunction set, including empty future
    /// windows in one grid calculation. No loop proportional to signed time.
    pub fn exclusion(&self, selector: &CollectionSelector) -> Option<Exclusion> {
        if self.settled || self.pending {
            return None;
        }
        if self.origin >= self.horizon {
            return Some(Exclusion {
                lower: self.horizon,
                upper: self.horizon,
                window_end: self.horizon,
                empty_target: true,
            });
        }
        let intersects = |lower: u64, upper: u64| {
            selector.filters().iter().any(|filter| {
                let fields = filter.fields();
                let begin = fields.since.map_or(lower, |time| time.0.max(lower));
                let end = fields
                    .until
                    .and_then(|time| time.0.checked_add(1))
                    .unwrap_or(upper)
                    .min(upper);
                begin < end
            })
        };
        if intersects(self.lower, self.upper) {
            return None;
        }
        let mut end = self.window_end;
        if self.upper == self.window_end {
            let next = selector
                .filters()
                .iter()
                .filter_map(|filter| {
                    let fields = filter.fields();
                    let begin = fields.since.map_or(end, |time| time.0.max(end));
                    let last = fields
                        .until
                        .and_then(|time| time.0.checked_add(1))
                        .unwrap_or(self.horizon)
                        .min(self.horizon);
                    (begin < last).then_some(begin)
                })
                .min();
            end = next.map_or(self.horizon, |time| {
                self.origin + ((time - self.origin) / self.width) * self.width
            });
            end = end.max(self.window_end).min(self.horizon);
        }
        Some(Exclusion {
            lower: self.lower,
            upper: if self.upper == self.window_end {
                end
            } else {
                self.upper
            },
            window_end: end,
            empty_target: false,
        })
    }
    pub fn exclude(&mut self, span: Exclusion) -> Result<(), QuantumError> {
        if self.pending || self.settled {
            return Err(QuantumError::Context);
        }
        if span.empty_target {
            if self.origin < self.horizon
                || (span.lower, span.upper, span.window_end)
                    != (self.horizon, self.horizon, self.horizon)
            {
                return Err(QuantumError::Context);
            }
            self.settled = true;
            return Ok(());
        }
        if span.lower != self.lower {
            return Err(QuantumError::Context);
        }
        if span.lower >= span.upper
            || span.window_end > self.horizon
            || (self.upper < self.window_end
                && (span.upper, span.window_end) != (self.upper, self.window_end))
            || (self.upper == self.window_end
                && (span.upper != span.window_end
                    || span.window_end < self.window_end
                    || (span.window_end != self.horizon
                        && (span.window_end - self.origin) % self.width != 0)))
        {
            return Err(QuantumError::Context);
        }
        self.close(span.window_end);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bigbrotr_model::acquisition::{EphemeralPolicy, FilterFields};
    fn selector(lower: u64, bounds: &[(Option<u64>, Option<u64>)]) -> CollectionSelector {
        CollectionSelector::new(
            bounds
                .iter()
                .map(|(since, until)| {
                    Filter::new(FilterFields {
                        since: since.map(NostrTime),
                        until: until.map(NostrTime),
                        ..FilterFields::default()
                    })
                    .unwrap()
                })
                .collect(),
            NostrTime(lower),
            EphemeralPolicy::Retain,
        )
        .unwrap()
    }
    fn apply(
        geometry: &mut Geometry,
        disposition: &'static str,
        continuation: u64,
        gap: Option<(u64, u64)>,
    ) {
        geometry
            .request(
                geometry.lower,
                geometry.upper,
                geometry.window_end,
                geometry.spent + 1,
            )
            .unwrap();
        geometry
            .finish(Settlement {
                disposition,
                continuation,
                gap,
            })
            .unwrap();
    }
    #[test]
    fn three_windows_keep_original_end_and_contiguous_attempted_prefix_with_gaps() {
        let mut pass = Geometry::new(0, 259200, 86400, 128).unwrap();
        apply(&mut pass, "overlap", 2, None);
        assert_eq!(
            (pass.upper, pass.window_end, pass.attempted_prefix),
            (2, 86400, 0)
        );
        apply(&mut pass, "saturated_second", 1, Some((1, 2)));
        assert_eq!((pass.upper, pass.window_end, pass.gap_count), (1, 86400, 1));
        apply(&mut pass, "window_closed", 86400, None);
        assert_eq!(
            (pass.lower, pass.upper, pass.attempted_prefix),
            (86400, 172800, 86400)
        );
        apply(&mut pass, "window_closed", 172800, None);
        apply(&mut pass, "overlap", 172802, None);
        apply(
            &mut pass,
            "saturated_second",
            172801,
            Some((172801, 172802)),
        );
        apply(&mut pass, "window_closed", 259200, None);
        assert!(pass.settled);
        assert_eq!(
            (pass.spent, pass.gap_count, pass.attempted_prefix),
            (7, 2, 259200)
        );
    }
    #[test]
    fn fixed_query_budget_counts_pending_retries_and_complete_pass_wins() {
        let mut pass = Geometry::new(0, 10, 10, 128).unwrap();
        for _ in 0..127 {
            apply(&mut pass, "retry", 10, None);
        }
        pass.request(0, 10, 10, 128).unwrap();
        assert_eq!(pass.pending_work(), "request_pending");
        pass.finish(Settlement {
            disposition: "retry",
            continuation: 10,
            gap: None,
        })
        .unwrap();
        assert_eq!(pass.pending_work(), "budget_deferred");
        assert!(pass.request(0, 10, 10, 129).is_err());
        let mut last = Geometry::new(0, 10, 10, 1).unwrap();
        apply(&mut last, "window_closed", 10, None);
        assert_eq!(last.pending_work(), "settled");
        assert!(Geometry::new(0, 10, 10, 129).is_err());
        assert!(Geometry::new(0, 10, 10, 0).is_err());
    }
    #[test]
    fn exclusions_coalesce_empty_grid_windows_and_close_original_end_after_stall() {
        let collection = selector(0, &[(Some(23), Some(86399))]);
        let mut pass = Geometry::new(0, 86400, 86400, 128).unwrap();
        apply(&mut pass, "overlap", 24, None);
        apply(&mut pass, "saturated_second", 23, Some((23, 24)));
        let excluded = pass.exclusion(&collection).unwrap();
        assert_eq!(
            excluded,
            Exclusion {
                lower: 0,
                upper: 23,
                window_end: 86400,
                empty_target: false
            }
        );
        pass.exclude(excluded).unwrap();
        assert!(pass.settled);
        assert_eq!(pass.attempted_prefix, 86400);
        let distant = selector(0, &[(Some(1_000_000_001), None)]);
        let mut sparse = Geometry::new(0, 2_000_000_000, 86400, 128).unwrap();
        let span = sparse.exclusion(&distant).unwrap();
        assert_eq!(span.upper, (1_000_000_001 / 86400) * 86400);
        sparse.exclude(span).unwrap();
        assert!(sparse.exclusion(&distant).is_none());
        let empty = selector(100, &[(None, Some(10))]);
        let mut pass = Geometry::new(100, 11, 86400, 128).unwrap();
        let span = pass.exclusion(&empty).unwrap();
        assert!(span.empty_target);
        assert_eq!(span.lower, span.upper);
        pass.exclude(span).unwrap();
        assert!(pass.settled);
        assert_eq!(pass.attempted_prefix, 11);
    }

    #[test]
    fn frozen_first_window_preserves_empty_intersections_and_checked_seconds() {
        let window = LinearWindow::first(
            &selector(0, &[(None, None), (Some(86_400), None)]),
            200_000_999_999,
            86_400,
        )
        .unwrap();
        assert_eq!(
            (window.lower(), window.upper(), window.horizon()),
            (0, 86_400, 200_000)
        );
        assert_eq!(window.filters().len(), 1);
        assert_eq!(window.predicate(),br#"["REQ","000000000000000000000000000000000000000000000000",{"since":0,"until":86399,"limit":3}]"#);
        assert_eq!(window.continuation(Some(0)), 1);
        assert_eq!(window.continuation(Some(23)), 24);
        assert_eq!(window.continuation(None), 86_400);
        assert!(
            LinearWindow::first(&selector(3, &[(Some(4), Some(4))]), 3_999_999, 86_400).is_err()
        );
        assert!(LinearWindow::first(&selector(0, &[(None, None)]), -1, 86_400).is_err());
        assert!(LinearWindow::first(&selector(0, &[(None, None)]), 2_999_999, 0).is_err());
        let at_zero =
            LinearWindow::first(&selector(0, &[(None, Some(0))]), 20_000_999, 86_400).unwrap();
        assert_eq!((at_zero.upper(), at_zero.horizon()), (1, 1));
        assert_eq!(at_zero.filters()[0].fields().until, Some(NostrTime(0)));
        let maximum =
            LinearWindow::first(&selector(0, &[(None, Some(u64::MAX))]), 2_999_999, 86_400)
                .unwrap();
        assert_eq!((maximum.upper(), maximum.horizon()), (2, 2));
        assert_eq!(maximum.filters()[0].fields().until, Some(NostrTime(1)));
        assert!(LinearWindow::first(&selector(0, &[(None, None)]), 999_999, 86_400).is_err());
        assert_eq!(Upper::Finite(0).inclusive(), None);
        assert_eq!(Upper::Finite(1).inclusive(), Some(0));
        assert_eq!(Upper::AfterMax.inclusive(), Some(u64::MAX));
    }

    #[test]
    fn retained_window_preserves_its_exclusive_upper_across_width_changes() {
        let selector = selector(10, &[(None, None)]);
        let accepted_at_us = 100_999_999;
        assert_eq!(
            LinearWindow::first(&selector, accepted_at_us, 80)
                .unwrap()
                .upper(),
            90
        );
        assert_eq!(
            LinearWindow::first(&selector, accepted_at_us, 5)
                .unwrap()
                .upper(),
            15
        );
        let window = LinearWindow::retained(&selector, accepted_at_us, 10, 37, 100).unwrap();
        assert_eq!(
            (window.lower(), window.upper(), window.horizon()),
            (10, 37, 100)
        );
        assert_eq!(window.predicate(),br#"["REQ","000000000000000000000000000000000000000000000000",{"since":10,"until":36,"limit":3}]"#);
    }

    #[test]
    fn retained_window_preserves_zero_and_after_max_selector_bounds() {
        let at_zero =
            LinearWindow::retained(&selector(0, &[(None, Some(0))]), 20_000_999, 0, 1, 1).unwrap();
        assert_eq!(
            (at_zero.lower(), at_zero.upper(), at_zero.horizon()),
            (0, 1, 1)
        );
        assert_eq!(at_zero.predicate(),br#"["REQ","000000000000000000000000000000000000000000000000",{"since":0,"until":0,"limit":3}]"#);
        let maximum =
            LinearWindow::retained(&selector(0, &[(None, Some(u64::MAX))]), 2_999_999, 0, 2, 2)
                .unwrap();
        assert_eq!(maximum.filters()[0].fields().until, Some(NostrTime(1)));
    }

    #[test]
    fn retained_window_refuses_changed_bounds_and_empty_intersections() {
        let collection = selector(10, &[(None, Some(80))]);
        let valid = LinearWindow::retained(&collection, 100_999_999, 10, 81, 81).unwrap();
        assert_eq!((valid.upper(), valid.horizon()), (81, 81));
        for (accepted_at_us, lower, upper, horizon) in [
            (100_999_999, 10, 37, 80),
            (100_999_999, 10, 37, 82),
            (80_999_999, 10, 37, 81),
            (-1, 10, 37, 81),
            (100_999_999, 9, 37, 81),
            (100_999_999, 10, 10, 81),
            (100_999_999, 10, 9, 81),
            (100_999_999, 10, 82, 81),
        ] {
            assert!(matches!(
                LinearWindow::retained(&collection, accepted_at_us, lower, upper, horizon),
                Err(QuantumError::Interval)
            ));
        }
        assert!(matches!(
            LinearWindow::retained(&selector(0, &[(None, None)]), 999_999, 0, 0, 0),
            Err(QuantumError::Interval)
        ));
        assert!(matches!(
            LinearWindow::retained(
                &selector(10, &[(Some(30), Some(80))]),
                100_999_999,
                10,
                20,
                81,
            ),
            Err(QuantumError::Interval)
        ));
    }

    #[test]
    fn retained_window_intersects_conjunctions_with_exact_predicate_and_digest() {
        let selector = CollectionSelector::new(
            vec![
                Filter::new(FilterFields {
                    kinds: Some(vec![3, 1, 3]),
                    tags: [('t', vec!["z".to_owned(), "a".to_owned(), "a".to_owned()])]
                        .into_iter()
                        .collect(),
                    since: Some(NostrTime(5)),
                    until: Some(NostrTime(12)),
                    ..FilterFields::default()
                })
                .unwrap(),
                Filter::new(FilterFields {
                    kinds: Some(vec![2]),
                    since: Some(NostrTime(14)),
                    until: Some(NostrTime(35)),
                    ..FilterFields::default()
                })
                .unwrap(),
                Filter::new(FilterFields {
                    kinds: Some(vec![4]),
                    since: Some(NostrTime(40)),
                    ..FilterFields::default()
                })
                .unwrap(),
            ],
            NostrTime(10),
            EphemeralPolicy::Retain,
        )
        .unwrap();
        let window = LinearWindow::retained(&selector, 100_999_999, 10, 30, 100).unwrap();
        assert_eq!(window.filters().len(), 2);
        assert_eq!(window.predicate(),br##"["REQ","000000000000000000000000000000000000000000000000",{"kinds":[1,3],"#t":["a","z"],"since":10,"until":12,"limit":3},{"kinds":[2],"since":14,"until":29,"limit":3}]"##);
        assert_eq!(
            window.predicate_digest(),
            [
                0xf6, 0x88, 0x73, 0x93, 0x77, 0xf6, 0x4c, 0xca, 0xcd, 0x7d, 0xaa, 0x16, 0x8d, 0x26,
                0xdf, 0x4d, 0xfb, 0x09, 0xe8, 0x56, 0x96, 0x80, 0x99, 0xcb, 0xbd, 0xed, 0x2c, 0xa5,
                0x30, 0x8e, 0x44, 0xb0,
            ]
        );
    }
}
