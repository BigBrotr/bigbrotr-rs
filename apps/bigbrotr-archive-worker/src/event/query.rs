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
    #[serde(default = "default_scan_bytes")]
    scan_bytes: u64,
}
fn default_scan_bytes() -> u64 {
    256 * 1024 * 1024
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
        lookup_bounded(
            &self.connection,
            &request.expected,
            max_bytes,
            request.scan_bytes,
        )
    }
}

fn select_reference(
    c: &Connection,
    query: &EventQuery,
    eligibility: bool,
) -> Result<FactRef, EventReadError> {
    // Only closed typed hex identities and decimal integers enter this SQL.
    // LIMIT2 bounds the result. The separate physical-input admission bounds
    // both table passes; supervision owns deadline and memory.
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

#[cfg(test)]
pub(super) fn lookup_attached(
    c: &Connection,
    query: &EventQuery,
    max_bytes: usize,
) -> Result<EventQueryObservation, EventReadError> {
    lookup_bounded(c, query, max_bytes, default_scan_bytes())
}
fn lookup_bounded(
    c: &Connection,
    query: &EventQuery,
    max_bytes: usize,
    scan_bytes: u64,
) -> Result<EventQueryObservation, EventReadError> {
    query.validate(max_bytes)?;
    if scan_bytes == 0 || scan_bytes > default_scan_bytes() {
        return Err(EventReadError::ResourceLimit);
    }
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
    admit_scan(c, snapshot_id, scan_bytes, 2048)?;
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

/// Charge two complete file passes (selection and hydration) for both families,
/// before predicate pruning. This is a physical input admission bound, not a
/// claim about OS read amplification or a global archive-integrity certificate.
/// The exact DuckLake function materializes its metadata list at bind time;
/// that allocation remains under the independent native/OS memory allowance.
pub(super) fn admit_scan(
    c: &Connection,
    snapshot: u64,
    maximum: u64,
    max_files: usize,
) -> Result<(), EventReadError> {
    use std::{os::unix::fs::MetadataExt, path::Path};
    let mut charged = 0_u64;
    let mut count = 0_usize;
    for table in ["events", "event_eligibility_facts"] {
        let sql = format!(
            "SELECT left(data_file,4097),data_file_size_bytes,left(delete_file,4097),delete_file_size_bytes FROM ducklake_list_files('archive','{table}',schema=>'main',snapshot_version=>{snapshot}) LIMIT {}",
            max_files + 1
        );
        let mut statement = c.prepare(&sql).map_err(|_| EventReadError::Native)?;
        let mut rows = statement.query([]).map_err(|_| EventReadError::Native)?;
        while let Some(row) = rows.next().map_err(|_| EventReadError::Native)? {
            for i in [0, 2] {
                let file: Option<String> = row.get(i).map_err(|_| EventReadError::Native)?;
                let size: Option<u64> = row.get(i + 1).map_err(|_| EventReadError::Native)?;
                let (file, size) = match (file, size) {
                    (None, None) if i == 2 => continue,
                    (Some(file), Some(size)) => (file, size),
                    _ => return Err(EventReadError::Mismatch),
                };
                count = count.checked_add(1).ok_or(EventReadError::ResourceLimit)?;
                if count > max_files || file.len() > 4096 {
                    return Err(EventReadError::ResourceLimit);
                }
                let path = Path::new(&file);
                let metadata =
                    std::fs::symlink_metadata(path).map_err(|_| EventReadError::Native)?;
                if !path.is_absolute()
                    || !metadata.is_file()
                    || metadata.nlink() != 1
                    || metadata.len() != size
                    || size == 0
                    || path.canonicalize().ok().as_deref() != Some(path)
                {
                    return Err(EventReadError::Mismatch);
                }
                charged = size
                    .checked_mul(2)
                    .and_then(|v| charged.checked_add(v))
                    .filter(|v| *v <= maximum)
                    .ok_or(EventReadError::ResourceLimit)?;
            }
        }
    }
    Ok(())
}
