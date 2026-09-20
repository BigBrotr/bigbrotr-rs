//! Exact-reference historical hydration; caller owns retention and read authority.
use bigbrotr_model::{
    Digest256, EventBody, EventEligibility, EventId, EventOrigin, EventPayload, Fact, FactPayload,
    FactRef, NostrTime, ObservedTime, PublicKey, QualifiedRef, Signature, SignedEvent,
    SourceRecordId,
};
use duckdb::{
    Connection,
    arrow::array::{Array, StringArray},
};
use serde::{Deserialize, de::DeserializeOwned};

pub use bigbrotr_protocol::archive_event::{EventObservation, EventRead, EventReadError};

// No Debug/Serialize: the catalog holds the private reader credential.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventRequest {
    catalog: crate::catalog::BootstrapRequest,
    catalog_id: bigbrotr_model::OwnerResourceId,
    expected: EventRead,
}
impl EventRequest {
    pub fn read(input: impl std::io::Read) -> Result<Self, EventReadError> {
        use std::io::Read;
        let mut bytes = Vec::new();
        input
            .take(32769)
            .read_to_end(&mut bytes)
            .map_err(|_| EventReadError::InvalidInput)?;
        if bytes.len() > 32768 {
            return Err(EventReadError::ResourceLimit);
        }
        let request: Self =
            serde_json::from_slice(&bytes).map_err(|_| EventReadError::InvalidInput)?;
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
    /// The parent owns current read authority and retention. This finite private
    /// read certifies neither scope authorization nor a complete scientific cut.
    pub fn read_event(
        self,
        request: &EventRequest,
        max_bytes: usize,
    ) -> Result<EventObservation, EventReadError> {
        request.expected.validate(max_bytes)?;
        self.attach_readonly_catalog(
            &request.catalog,
            request.expected.dataset,
            request.catalog_id,
        )
        .map_err(|_| EventReadError::Native)?;
        read_attached(&self.connection, &request.expected, max_bytes)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeFact<P> {
    source: SourceRecordId,
    scope: QualifiedRef,
    recorded: ObservedTime,
    digest: Digest256,
    schema: u16,
    kind: String,
    payload: P,
}
impl<P> NativeFact<P> {
    fn into_fact(
        self,
        expected: FactRef,
        decode: impl FnOnce(P) -> Result<FactPayload, EventReadError>,
    ) -> Result<Fact, EventReadError> {
        let payload = decode(self.payload)?;
        if self.schema != 1
            || self.kind != payload.record_kind()
            || self.source != expected.source_record_id
            || self.digest != expected.payload_digest
        {
            return Err(EventReadError::Mismatch);
        }
        let fact = Fact::new(self.source, self.scope, self.recorded, payload)
            .map_err(|_| EventReadError::Mismatch)?;
        if fact.payload_digest() != expected.payload_digest {
            return Err(EventReadError::Mismatch);
        }
        Ok(fact)
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeBody {
    event_id: EventId,
    pubkey: PublicKey,
    created_at: NostrTime,
    kind: u16,
    tags: Vec<Vec<String>>,
    content: String,
    event_ordinal: QualifiedRef,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeEligibility {
    event_id: EventId,
    body_digest: Digest256,
    sig: Signature,
    first_eligible_at: ObservedTime,
    origin: String,
    import_lineage: Option<QualifiedRef>,
}
fn qualified(column: &str) -> String {
    format!(
        "struct_pack(dataset_id:=lower(hex({column}.dataset_id)),dictionary_kind:={column}.dictionary_kind,allocation_domain:=lower(hex({column}.allocation_domain)),ordinal:={column}.ordinal::VARCHAR)"
    )
}
fn source(column: &str) -> String {
    format!(
        "struct_pack(dataset_id:=lower(hex({column}.dataset_id)),shard_id:={column}.shard_id,owner_epoch:={column}.owner_epoch::VARCHAR,sequence:={column}.sequence::VARCHAR)"
    )
}

/// Decode exactly one full row, selecting by SourceRecordId ONLY. Selecting by
/// digest would hide a conflicting duplicate. LIMIT2 detects duplicates rather
/// than choosing an arbitrary representative. No missing row proves absence.
fn one<P: DeserializeOwned>(
    c: &Connection,
    table: &str,
    payload: &str,
    expected: FactRef,
    max_bytes: usize,
) -> Result<NativeFact<P>, EventReadError> {
    let id = expected.source_record_id;
    let json = format!(
        "to_json(struct_pack(source:={},scope:={},recorded:=recorded_at::VARCHAR,digest:=lower(hex(payload_digest)),schema:=record_schema_version,kind:=record_kind,payload:={payload}))",
        source("source_record_id"),
        qualified("visibility_scope_id")
    );
    // Clamping before Arrow conversion bounds the returned vector even for bad
    // stored text. Native JSON construction still belongs to the worker's wider
    // memory grant. UTF-8 character clamping may return up to4 times this byte cap.
    let sql = format!(
        "SELECT left({json},{}) FROM archive.main.{table} WHERE source_record_id.dataset_id=from_hex('{}') AND source_record_id.shard_id={} AND source_record_id.owner_epoch={} AND source_record_id.sequence={} LIMIT 2",
        max_bytes + 1,
        id.dataset_id,
        id.shard_id,
        id.owner_epoch.get(),
        id.sequence.get()
    );
    let mut statement = c.prepare(&sql).map_err(|_| EventReadError::Native)?;
    let stream = statement
        .stream_arrow([])
        .map_err(|_| EventReadError::Native)?;
    let mut result = None;
    for batch in stream {
        if batch.num_rows() > 2 {
            return Err(EventReadError::Mismatch);
        }
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or(EventReadError::Native)?;
        for i in 0..column.len() {
            if result.is_some() || column.is_null(i) {
                return Err(EventReadError::Mismatch);
            }
            let text = column.value(i);
            if text.len() > max_bytes {
                return Err(EventReadError::ResourceLimit);
            }
            result = Some(serde_json::from_str(text).map_err(|_| EventReadError::Mismatch)?);
        }
    }
    result.ok_or(EventReadError::Missing)
}

fn read_payload(
    c: &Connection,
    read: &EventRead,
    max_bytes: usize,
) -> Result<Fact, EventReadError> {
    let fields = format!(
        "struct_pack(event_id:=lower(hex(event_id)),pubkey:=lower(hex(pubkey)),created_at:=created_at,kind:=kind,tags:=tags,content:=content,event_ordinal:={})",
        qualified("event_ordinal")
    );
    one::<NativeBody>(c, "events", &fields, read.payload, max_bytes)?.into_fact(read.payload, |v| {
        Ok(FactPayload::EventPayload(EventPayload {
            body: EventBody {
                event_id: v.event_id,
                pubkey: v.pubkey,
                created_at: v.created_at,
                kind: v.kind,
                tags: v.tags,
                content: v.content,
            },
            event_ordinal: v.event_ordinal,
        }))
    })
}
fn read_eligibility(
    c: &Connection,
    read: &EventRead,
    max_bytes: usize,
) -> Result<Fact, EventReadError> {
    let fields = format!(
        "struct_pack(event_id:=lower(hex(event_id)),body_digest:=lower(hex(body_digest)),sig:=lower(hex(sig)),first_eligible_at:=first_eligible_at::VARCHAR,origin:=origin,import_lineage:=CASE WHEN import_lineage IS NULL THEN NULL ELSE {} END)",
        qualified("import_lineage")
    );
    one::<NativeEligibility>(
        c,
        "event_eligibility_facts",
        &fields,
        read.eligibility,
        max_bytes,
    )?
    .into_fact(read.eligibility, |v| {
        let origin = match (v.origin.as_str(), v.import_lineage) {
            ("network", None) => EventOrigin::Network,
            ("import", Some(lineage)) => EventOrigin::Import(lineage),
            _ => return Err(EventReadError::Mismatch),
        };
        Ok(FactPayload::EventEligibility(EventEligibility {
            event_id: v.event_id,
            body_digest: v.body_digest,
            sig: v.sig,
            first_eligible_at: v.first_eligible_at,
            origin,
        }))
    })
}
fn join(
    payload: &Fact,
    eligibility: &Fact,
    read: &EventRead,
    max_bytes: usize,
) -> Result<SignedEvent, EventReadError> {
    let (FactPayload::EventPayload(body), FactPayload::EventEligibility(scope)) =
        (payload.payload(), eligibility.payload())
    else {
        return Err(EventReadError::Mismatch);
    };
    if eligibility.visibility_scope_id() != read.scope
        || scope.event_id != read.event_id
        || body.body.event_id != read.event_id
        || body.body.body_digest() != scope.body_digest
    {
        return Err(EventReadError::Mismatch);
    }
    bigbrotr_protocol::verify_owned(body.body.clone().with_signature(scope.sig), max_bytes)
        .map(|e| e.into_event())
        .map_err(|e| {
            if e.is_limit() {
                EventReadError::ResourceLimit
            } else {
                EventReadError::Mismatch
            }
        })
}

fn read_attached(
    c: &Connection,
    read: &EventRead,
    max_bytes: usize,
) -> Result<EventObservation, EventReadError> {
    read.validate(max_bytes)?;
    c.execute_batch("BEGIN TRANSACTION")
        .map_err(|_| EventReadError::Native)?;
    // DuckLake captures and retains this actual snapshot in the same transaction
    // used by BOTH scientific lookups. A max(snapshot) on another connection is
    // not a witness for these reads. No continuation/persistent pin is minted.
    let snapshot_id = c
        .query_row(
            "SELECT id FROM ducklake_current_snapshot('archive')",
            [],
            |r| r.get(0),
        )
        .map_err(|_| EventReadError::Native)?;
    let payload = read_payload(c, read, max_bytes)?;
    let eligibility = read_eligibility(c, read, max_bytes)?;
    let event = join(&payload, &eligibility, read, max_bytes)?;
    c.execute_batch("COMMIT")
        .map_err(|_| EventReadError::Native)?;
    Ok(EventObservation { snapshot_id, event })
    // Error leaves the private engine's transaction to destruction. This function
    // is not exposed for reuse of a failed connection.
}
#[cfg(test)]
mod tests;
