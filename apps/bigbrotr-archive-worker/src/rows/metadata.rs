use super::*;
use arrow::array::StringArray;
use bigbrotr_model::ClockQuality;

impl Rows {
    pub fn eligibility(facts: &[&Fact], limits: RowLimits) -> Result<Self, RowError> {
        let values = select(facts, limits, |v| match v {
            FactPayload::EventEligibility(v) => Some(v),
            _ => None,
        })?;
        let mut columns = envelope(facts)?;
        columns.extend([
            (
                "event_id",
                fixed(values.iter().map(|v| *v.event_id.as_bytes()))?,
            ),
            (
                "body_digest",
                fixed(values.iter().map(|v| *v.body_digest.as_bytes()))?,
            ),
            ("sig", fixed(values.iter().map(|v| *v.sig.as_bytes()))?),
            (
                "first_eligible_at",
                Arc::new(Int64Array::from_iter_values(
                    values.iter().map(|v| v.first_eligible_at.get()),
                )) as ArrayRef,
            ),
            (
                "origin",
                Arc::new(StringArray::from_iter_values(
                    values.iter().map(|v| v.origin.as_str()),
                )),
            ),
            (
                "import_lineage",
                optional_qualified(values.iter().map(|v| v.origin.import_lineage()))?,
            ),
        ]);
        Self::finish(columns, &["import_lineage"])
    }
    pub fn receive(facts: &[&Fact], limits: RowLimits) -> Result<Self, RowError> {
        let values = select(facts, limits, |v| match v {
            FactPayload::EventReceive(v) => Some(v),
            _ => None,
        })?;
        let mut columns = envelope(facts)?;
        columns.extend([
            (
                "event_id",
                fixed(values.iter().map(|v| *v.event_id.as_bytes()))?,
            ),
            (
                "received_at",
                Arc::new(Int64Array::from_iter_values(
                    values.iter().map(|v| v.received_at.get()),
                )) as ArrayRef,
            ),
            (
                "observer_id",
                qualified(values.iter().map(|v| v.observer_id))?,
            ),
            (
                "clock_quality",
                clock(values.iter().map(|v| v.clock_quality))?,
            ),
            (
                "corrects",
                optional_source(values.iter().map(|v| v.corrects))?,
            ),
        ]);
        Self::finish(columns, &["corrects"])
    }
    pub fn provenance(facts: &[&Fact], limits: RowLimits) -> Result<Self, RowError> {
        let values = select(facts, limits, |v| match v {
            FactPayload::Provenance(v) => Some(v),
            _ => None,
        })?;
        let mut columns = envelope(facts)?;
        columns.extend([
            (
                "event_id",
                fixed(values.iter().map(|v| *v.event_id.as_bytes()))?,
            ),
            (
                "endpoint_id",
                qualified(values.iter().map(|v| v.endpoint_id))?,
            ),
            (
                "observer_id",
                qualified(values.iter().map(|v| v.observer_id))?,
            ),
            (
                "access_scope_id",
                qualified(values.iter().map(|v| v.access_scope_id))?,
            ),
            (
                "received_at",
                Arc::new(Int64Array::from_iter_values(
                    values.iter().map(|v| v.received_at.get()),
                )) as ArrayRef,
            ),
            (
                "clock_quality",
                clock(values.iter().map(|v| v.clock_quality))?,
            ),
            (
                "verification_method_id",
                qualified(values.iter().map(|v| v.verification_method_id))?,
            ),
            (
                "corrects",
                optional_source(values.iter().map(|v| v.corrects))?,
            ),
        ]);
        Self::finish(columns, &["corrects"])
    }
}

fn select<'a, T>(
    facts: &[&'a Fact],
    limits: RowLimits,
    get: impl Fn(&'a FactPayload) -> Option<&'a T>,
) -> Result<Vec<&'a T>, RowError> {
    validate_rows(facts, limits)?;
    if facts.iter().any(|v| get(v.payload()).is_none()) {
        return Err(RowError::WrongFamily);
    }
    let mut values = Vec::new();
    values
        .try_reserve_exact(facts.len())
        .map_err(|_| RowError::Allocation)?;
    for fact in facts {
        values.push(get(fact.payload()).ok_or(RowError::WrongFamily)?);
    }
    Ok(values)
}
fn clock(
    values: impl ExactSizeIterator<Item = ClockQuality> + Clone,
) -> Result<ArrayRef, RowError> {
    nullable_structure(
        vec![
            ("clock_id", qualified(values.clone().map(|v| v.clock_id()))?),
            (
                "resolution_us",
                Arc::new(UInt64Array::from_iter_values(
                    values.clone().map(|v| v.resolution_us().get()),
                )),
            ),
            (
                "uncertainty_us",
                Arc::new(UInt64Array::from_iter(
                    values.clone().map(|v| v.uncertainty_us().map(|v| v.get())),
                )),
            ),
            (
                "anomaly_flags",
                Arc::new(UInt32Array::from_iter_values(
                    values.map(|v| v.anomaly_flags()),
                )),
            ),
        ],
        &["uncertainty_us"],
        None,
    )
}
