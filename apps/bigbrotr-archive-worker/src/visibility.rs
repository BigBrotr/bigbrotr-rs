//! Exact visibility mapping lookup. Class labels never confer permission.
use crate::event::{qualified, source};
use bigbrotr_model::{
    DescriptorLabel, DictionaryDescriptor, DictionaryEntry, Digest256, Fact, FactPayload,
    ObservedTime, QualifiedRef, SourceRecordId,
};
use bigbrotr_protocol::archive_visibility::{
    FORMAT, MAX_FACT_BYTES, MAX_NATIVE_ROW_BYTES, VisibilityObservation, VisibilityRead,
    VisibilityReadError as Error,
};
use duckdb::{
    Connection,
    arrow::array::{Array, StringArray},
};
use serde::Deserialize;
use std::io::Read;

// Catalog credentials have no printable/serializable implementation.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VisibilityRequest {
    catalog: crate::catalog::BootstrapRequest,
    catalog_id: bigbrotr_model::OwnerResourceId,
    expected: VisibilityRead,
}
impl VisibilityRequest {
    pub fn read(input: impl Read) -> Result<Self, Error> {
        let mut bytes = Vec::new();
        input
            .take(32769)
            .read_to_end(&mut bytes)
            .map_err(|_| Error::InvalidInput)?;
        if bytes.len() > 32768 {
            return Err(Error::ResourceLimit);
        }
        let request: Self = serde_json::from_slice(&bytes).map_err(|_| Error::InvalidInput)?;
        request
            .catalog
            .validate()
            .map_err(|_| Error::InvalidInput)?;
        request.expected.validate()?;
        Ok(request)
    }
}
impl crate::engine::Engine {
    pub fn read_visibility(
        self,
        request: &VisibilityRequest,
    ) -> Result<VisibilityObservation, Error> {
        request.expected.validate()?;
        self.attach_readonly_catalog(
            &request.catalog,
            request.expected.dataset,
            request.catalog_id,
            Some(request.expected.snapshot_id),
        )
        .map_err(|_| Error::Native)?;
        lookup_attached(&self.connection, &request.expected)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeVisibility {
    source: SourceRecordId,
    scope: QualifiedRef,
    recorded: ObservedTime,
    digest: Digest256,
    schema: u16,
    kind: String,
    reference: QualifiedRef,
    normalization: u16,
    descriptor_schema: String,
    descriptor_version: u16,
    class: String,
    inactive_empty: bool,
}
impl NativeVisibility {
    fn fact(self, read: &VisibilityRead) -> Result<Fact, Error> {
        if self.schema != 1
            || self.kind != "dictionary"
            || self.reference != read.scope
            || self.normalization != 1
            || self.descriptor_schema != "visibility"
            || self.descriptor_version != 1
            || !self.inactive_empty
            || !read.contains(self.source)
        {
            return Err(Error::Mismatch);
        }
        let entry = DictionaryEntry::new(
            self.reference,
            DictionaryDescriptor::Visibility {
                class: DescriptorLabel::new(self.class).map_err(|_| Error::Mismatch)?,
            },
        )
        .map_err(|_| Error::Mismatch)?;
        let fact = Fact::new(
            self.source,
            self.scope,
            self.recorded,
            FactPayload::Dictionary(entry),
        )
        .map_err(|_| Error::Mismatch)?;
        if fact.payload_digest() != self.digest {
            return Err(Error::Mismatch);
        }
        Ok(fact)
    }
}

pub(crate) fn lookup_attached(
    c: &Connection,
    read: &VisibilityRead,
) -> Result<VisibilityObservation, Error> {
    read.validate()?;
    c.execute_batch("BEGIN TRANSACTION")
        .map_err(|_| Error::Native)?;
    let snapshot_id: u64 = c
        .query_row(
            "SELECT id FROM ducklake_current_snapshot('archive')",
            [],
            |r| r.get(0),
        )
        .map_err(|_| Error::Native)?;
    if snapshot_id != read.snapshot_id {
        return Err(Error::Mismatch);
    }
    let prefixes:Vec<_>=read.bounds.iter().filter(|b| b.complete_through>0).map(|b|format!(
        "(source_record_id.shard_id={} AND source_record_id.owner_epoch={} AND source_record_id.sequence BETWEEN 1 AND {})",
        b.shard_id,b.owner_epoch,b.complete_through)).collect();
    if prefixes.is_empty() {
        return Err(Error::Missing);
    }
    // Every dormant variant field must be NULL. Ignoring these fields would
    // verify a different logical row while dropping unsupported stored content.
    let inactive = [
        "event_id",
        "url",
        "clock",
        "vantage",
        "software",
        "method_id",
        "public_key",
        "route_class",
        "software_version",
        "label",
        "visibility_scope_id",
        "input",
        "algorithm",
        "capability",
        "output_unit",
        "failure_taxonomy",
        "algorithm_version",
        "protocol_revision",
        "format",
        "format_version",
        "source_dataset_id",
        "source_artifact_digest",
    ]
    .map(|name| format!("descriptor_value.{name} IS NULL"))
    .join(" AND ");
    let fields = format!(
        "struct_pack(source:={},scope:={},recorded:=recorded_at::VARCHAR,digest:=lower(hex(payload_digest)),schema:=record_schema_version,kind:=record_kind,reference:={},normalization:=normalization_version,descriptor_schema:=descriptor_schema,descriptor_version:=descriptor_version,class:=descriptor_value.class,inactive_empty:=({inactive}))",
        source("source_record_id"),
        qualified("visibility_scope_id"),
        qualified("reference")
    );
    // Result-bound only: full scan admission remains a separate read-core gate.
    // All interpolated input is closed typed hex/decimal, never caller SQL.
    let sql = format!(
        "SELECT left(to_json({fields}),{}) FROM archive.main.dictionaries WHERE reference.dataset_id=from_hex('{}') AND reference.dictionary_kind=4 AND reference.allocation_domain=from_hex('{}') AND reference.ordinal={} AND source_record_id.dataset_id=from_hex('{}') AND ({}) LIMIT 2",
        MAX_NATIVE_ROW_BYTES + 1,
        read.scope.dataset_id,
        read.scope.allocation_domain,
        read.scope.ordinal.get(),
        read.dataset,
        prefixes.join(" OR ")
    );
    let selected = one(c, &sql)?.fact(read)?;
    // A different mapping can illegally reuse this source identity. The first
    // predicate would hide it; hydrate by source alone in the SAME snapshot.
    let id = selected.source_record_id();
    let sql = format!(
        "SELECT left(to_json({fields}),{}) FROM archive.main.dictionaries WHERE source_record_id.dataset_id=from_hex('{}') AND source_record_id.shard_id={} AND source_record_id.owner_epoch={} AND source_record_id.sequence={} LIMIT 2",
        MAX_NATIVE_ROW_BYTES + 1,
        id.dataset_id,
        id.shard_id,
        id.owner_epoch.get(),
        id.sequence.get()
    );
    let fact = one(c, &sql)?.fact(read)?;
    if fact != selected {
        return Err(Error::Mismatch);
    }
    let preimage = fact.encode_preimage(MAX_FACT_BYTES)?;
    let observation = VisibilityObservation {
        format: FORMAT.into(),
        snapshot_id,
        request_digest: read.digest(),
        fact_digest: fact.payload_digest(),
        preimage_hex: preimage.iter().map(|b| format!("{b:02x}")).collect(),
    };
    c.execute_batch("COMMIT").map_err(|_| Error::Native)?;
    Ok(observation)
    // A failed one-shot engine is destroyed with its transaction, never reused.
}

fn one(c: &Connection, sql: &str) -> Result<NativeVisibility, Error> {
    let mut statement = c.prepare(sql).map_err(|_| Error::Native)?;
    let stream = statement.stream_arrow([]).map_err(|_| Error::Native)?;
    let mut row = None;
    for batch in stream {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or(Error::Native)?;
        for i in 0..column.len() {
            if row.is_some() || column.is_null(i) {
                return Err(Error::Mismatch);
            }
            let text = column.value(i);
            if text.len() > MAX_NATIVE_ROW_BYTES {
                return Err(Error::ResourceLimit);
            }
            row = Some(serde_json::from_str(text).map_err(|_| Error::Mismatch)?);
        }
    }
    row.ok_or(Error::Missing)
}
