//! Select scope eligibility before consulting the shared event body.
use super::*;
use bigbrotr_protocol::archive_event::{
    EventMatch, EventQuery, EventQueryObservation, EventSelection, QUERY_FORMAT,
};

// No Debug/Serialize: the catalog holds the private reader credential.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventQueryRequest {
    catalog: crate::catalog::BootstrapRequest,
    catalog_id: bigbrotr_model::OwnerResourceId,
    expected: EventQuery,
}
impl EventQueryRequest {
    pub fn read(input: impl std::io::Read) -> Result<Self, EventReadError> {
        let request: Self = read_input(input)?;
        request
            .catalog
            .validate()
            .map_err(|_| EventReadError::InvalidInput)?;
        request
            .expected
            .validate(bigbrotr_protocol::archive_event::MAX_EVENT_ROW_BYTES)?;
        Ok(request)
    }
}
impl crate::engine::Engine {
    /// Caller retains the certified pin and live authorization through delivery.
    /// Absence is scoped to the exact selection, not an archive-wide certificate.
    pub fn query_event(
        self,
        request: &EventQueryRequest,
        max_bytes: usize,
    ) -> Result<EventQueryObservation, EventReadError> {
        request.expected.validate(max_bytes)?;
        self.attach_readonly_catalog(
            &request.catalog,
            request.expected.dataset,
            request.catalog_id,
            Some(request.expected.snapshot_id),
        )
        .map_err(|_| EventReadError::Native)?;
        lookup_attached(&self.connection, &request.expected, max_bytes)
    }
}

fn select_reference(
    c: &Connection,
    query: &EventQuery,
    eligibility: bool,
) -> Result<FactRef, EventReadError> {
    // Only closed typed hex identities and decimal integers enter this SQL.
    // LIMIT2 bounds the result, not scan cost; supervision owns the native job's
    // deadline and memory. Public admission still needs its scanned-byte bound.
    // No untrusted SQL or caller-supplied table name.
    let prefixes: Vec<_> = query.bounds.iter().filter(|b| b.complete_through > 0).map(|b| format!(
        "(source_record_id.shard_id={} AND source_record_id.owner_epoch={} AND source_record_id.sequence BETWEEN 1 AND {})",
        b.shard_id, b.owner_epoch, b.complete_through,
    )).collect();
    if prefixes.is_empty() {
        return Err(EventReadError::Missing);
    }
    let (table, scope) = if eligibility {
        (
            "event_eligibility_facts",
            format!(
                " AND visibility_scope_id.dataset_id=from_hex('{}') AND visibility_scope_id.dictionary_kind=4 AND visibility_scope_id.allocation_domain=from_hex('{}') AND visibility_scope_id.ordinal={}",
                query.scope.dataset_id,
                query.scope.allocation_domain,
                query.scope.ordinal.get(),
            ),
        )
    } else {
        ("events", String::new())
    };
    let sql = format!(
        "SELECT left(to_json(struct_pack(source_record_id:={},payload_digest:=lower(hex(payload_digest)))),1025) FROM archive.main.{table} WHERE event_id=from_hex('{}') AND source_record_id.dataset_id=from_hex('{}') AND ({}){scope} LIMIT 2",
        source("source_record_id"),
        query.event_id,
        query.dataset,
        prefixes.join(" OR "),
    );
    let mut statement = c.prepare(&sql).map_err(|_| EventReadError::Native)?;
    let stream = statement
        .stream_arrow([])
        .map_err(|_| EventReadError::Native)?;
    let mut result = None;
    for batch in stream {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or(EventReadError::Native)?;
        for i in 0..column.len() {
            if result.is_some() || column.is_null(i) {
                return Err(EventReadError::Mismatch);
            }
            // Valid identities have fixed byte lengths. Refuse corrupt oversized IDs
            // before decoding; the enclosing native memory cap remains separate.
            if column.value(i).len() > 1024 {
                return Err(EventReadError::ResourceLimit);
            }
            let reference: FactRef =
                serde_json::from_str(column.value(i)).map_err(|_| EventReadError::Mismatch)?;
            if !query.contains(reference.source_record_id) {
                return Err(EventReadError::Mismatch);
            }
            result = Some(reference);
        }
    }
    result.ok_or(EventReadError::Missing)
}

pub(super) fn lookup_attached(
    c: &Connection,
    query: &EventQuery,
    max_bytes: usize,
) -> Result<EventQueryObservation, EventReadError> {
    query.validate(max_bytes)?;
    c.execute_batch("BEGIN TRANSACTION")
        .map_err(|_| EventReadError::Native)?;
    let snapshot_id: u64 = c
        .query_row(
            "SELECT id FROM ducklake_current_snapshot('archive')",
            [],
            |r| r.get(0),
        )
        .map_err(|_| EventReadError::Native)?;
    if snapshot_id != query.snapshot_id {
        return Err(EventReadError::Mismatch);
    }
    // A private-first payload is shared storage, never evidence that the event
    // was eligible in the requested scope. Do not hydrate it before this check.
    let result = match select_reference(c, query, true) {
        Ok(eligibility) => EventSelection::Present {
            matched: Box::new(hydrate(c, query, eligibility, max_bytes)?),
        },
        Err(EventReadError::Missing) => EventSelection::Absent {},
        Err(error) => return Err(error),
    };
    c.execute_batch("COMMIT")
        .map_err(|_| EventReadError::Native)?;
    Ok(EventQueryObservation {
        format: QUERY_FORMAT.into(),
        snapshot_id,
        request_digest: query.digest(),
        result,
    })
    // Error destroys this one-shot engine; a failed transaction is never reused.
}

fn hydrate(
    c: &Connection,
    query: &EventQuery,
    eligibility: FactRef,
    max_bytes: usize,
) -> Result<EventMatch, EventReadError> {
    let payload = select_reference(c, query, false).map_err(|error| match error {
        EventReadError::Missing => EventReadError::Mismatch,
        other => other,
    })?;
    let snapshot_id = query.snapshot_id;
    let read = EventRead {
        dataset: query.dataset,
        event_id: query.event_id,
        scope: query.scope,
        payload,
        eligibility,
        snapshot_id: Some(snapshot_id),
    };
    read.validate(max_bytes)?;
    let eligibility_fact = read_eligibility(c, &read, max_bytes)?;
    let payload_fact = read_payload(c, &read, max_bytes)?;
    let event = join(&payload_fact, &eligibility_fact, &read, max_bytes)?;
    Ok(EventMatch {
        snapshot_id,
        event,
        payload,
        eligibility,
    })
}
