//! Bounded typed columnar values, not admission or publication authority.
use arrow::{
    array::{
        Array, ArrayRef, FixedSizeBinaryBuilder, Int64Array, ListBuilder, StringBuilder,
        StructArray, UInt16Array, UInt32Array, UInt64Array,
    },
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use bigbrotr_model::{Fact, FactPayload, QualifiedRef, SourceRecordId};
use std::{fmt, sync::Arc};

// Below the pinned engine's vector size. Each fresh batch owns exactly its tag
// children, avoiding repeated conversion of children retained by a parent slice.
pub const MAX_ROWS: usize = 1024;

/// Independent bounds on rows, UTF-8 payload bytes and tag containers/strings.
/// Native conversion, allocator overhead and engine memory need a separate grant.
#[derive(Debug, Clone, Copy)]
pub struct RowLimits {
    pub max_rows: usize,
    pub max_text_bytes: usize,
    pub max_tag_nodes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowError {
    InvalidLimits,
    RowLimit,
    TextLimit,
    TagLimit,
    WrongFamily,
    Allocation,
    Arrow,
    NativeAppend,
}
impl fmt::Display for RowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "archive row conversion failed: {self:?}")
    }
}
impl std::error::Error for RowError {}

pub struct EventRows {
    batch: RecordBatch,
}
impl EventRows {
    pub fn encode(facts: &[&Fact], limits: RowLimits) -> Result<Self, RowError> {
        let totals = preflight(facts, limits)?;
        let mut values = Vec::new();
        values
            .try_reserve_exact(facts.len())
            .map_err(|_| RowError::Allocation)?;
        for fact in facts {
            let FactPayload::EventPayload(value) = fact.payload() else {
                return Err(RowError::WrongFamily);
            };
            values.push(value);
        }
        let mut content = StringBuilder::with_capacity(facts.len(), totals.content_bytes);
        let strings = StringBuilder::with_capacity(totals.strings, totals.tag_bytes);
        let inner = ListBuilder::with_capacity(strings, totals.tags)
            .with_field(Arc::new(Field::new("item", DataType::Utf8, false)));
        let item = Arc::new(Field::new("item", DataType::Utf8, false));
        let mut tags = ListBuilder::with_capacity(inner, facts.len())
            .with_field(Arc::new(Field::new("item", DataType::List(item), false)));
        for value in &values {
            content.append_value(&value.body.content);
            for tag in &value.body.tags {
                for text in tag {
                    tags.values().values().append_value(text);
                }
                tags.values().append(true);
            }
            tags.append(true);
        }
        let mut columns = envelope(facts)?;
        columns.extend([
            (
                "event_id",
                fixed(values.iter().map(|v| *v.body.event_id.as_bytes()))?,
            ),
            (
                "pubkey",
                fixed(values.iter().map(|v| *v.body.pubkey.as_bytes()))?,
            ),
            (
                "created_at",
                Arc::new(UInt64Array::from_iter_values(
                    values.iter().map(|v| v.body.created_at.0),
                )) as ArrayRef,
            ),
            (
                "kind",
                Arc::new(UInt16Array::from_iter_values(
                    values.iter().map(|v| v.body.kind),
                )),
            ),
            ("tags", Arc::new(tags.finish())),
            ("content", Arc::new(content.finish())),
            (
                "event_ordinal",
                qualified(values.iter().map(|v| v.event_ordinal))?,
            ),
        ]);
        let (fields, arrays) = fields_and_arrays(columns);
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
            .map_err(|_| RowError::Arrow)?;
        Ok(Self { batch })
    }
    /// Arrow's retained array memory accounting, not process RSS or a native
    /// grant. Input facts and native converted vectors coexist with this value.
    pub fn array_memory_bytes(&self) -> usize {
        self.batch.get_array_memory_size()
    }
    /// The caller owns the table schema and surrounding transaction. Explicit
    /// flush is mandatory: the binding's Drop discards native flush errors.
    pub fn append(self, appender: &mut duckdb::Appender<'_>) -> Result<(), RowError> {
        appender
            .append_record_batch(self.batch)
            .map_err(|_| RowError::NativeAppend)?;
        appender.flush().map_err(|_| RowError::NativeAppend)
    }
}

#[derive(Default)]
struct Totals {
    tags: usize,
    strings: usize,
    content_bytes: usize,
    tag_bytes: usize,
}

fn preflight(facts: &[&Fact], limits: RowLimits) -> Result<Totals, RowError> {
    if limits.max_rows == 0
        || limits.max_rows > MAX_ROWS
        || limits.max_text_bytes > i32::MAX as usize
        || limits.max_tag_nodes > i32::MAX as usize
    {
        return Err(RowError::InvalidLimits);
    }
    if facts.is_empty() || facts.len() > limits.max_rows {
        return Err(RowError::RowLimit);
    }
    let mut total = Totals::default();
    // No output allocation until the entire slice has passed these checks.
    let mut text_left = limits.max_text_bytes;
    let mut nodes_left = limits.max_tag_nodes;
    for fact in facts {
        let FactPayload::EventPayload(value) = fact.payload() else {
            return Err(RowError::WrongFamily);
        };
        text_left = text_left
            .checked_sub(value.body.content.len())
            .ok_or(RowError::TextLimit)?;
        total.content_bytes += value.body.content.len();
        nodes_left = nodes_left
            .checked_sub(value.body.tags.len())
            .ok_or(RowError::TagLimit)?;
        total.tags += value.body.tags.len();
        for tag in &value.body.tags {
            nodes_left = nodes_left
                .checked_sub(tag.len())
                .ok_or(RowError::TagLimit)?;
            total.strings += tag.len();
            for text in tag {
                text_left = text_left
                    .checked_sub(text.len())
                    .ok_or(RowError::TextLimit)?;
                total.tag_bytes += text.len();
            }
        }
    }
    Ok(total)
}

type Columns = Vec<(&'static str, ArrayRef)>;

fn fields_and_arrays(columns: Columns) -> (Vec<Field>, Vec<ArrayRef>) {
    columns
        .into_iter()
        .map(|(name, array)| (Field::new(name, array.data_type().clone(), false), array))
        .unzip()
}
fn structure(columns: Columns) -> Result<ArrayRef, RowError> {
    let (fields, arrays) = fields_and_arrays(columns);
    StructArray::try_new(fields.into(), arrays, None)
        .map(|v| Arc::new(v) as ArrayRef)
        .map_err(|_| RowError::Arrow)
}
fn fixed<const N: usize>(
    values: impl ExactSizeIterator<Item = [u8; N]>,
) -> Result<ArrayRef, RowError> {
    let width = i32::try_from(N).map_err(|_| RowError::Arrow)?;
    let mut builder = FixedSizeBinaryBuilder::with_capacity(values.len(), width);
    for value in values {
        builder.append_value(value).map_err(|_| RowError::Arrow)?;
    }
    Ok(Arc::new(builder.finish()))
}
fn source(
    values: impl ExactSizeIterator<Item = SourceRecordId> + Clone,
) -> Result<ArrayRef, RowError> {
    structure(vec![
        (
            "dataset_id",
            fixed(values.clone().map(|v| *v.dataset_id.as_bytes()))?,
        ),
        (
            "shard_id",
            Arc::new(UInt32Array::from_iter_values(
                values.clone().map(|v| v.shard_id),
            )),
        ),
        (
            "owner_epoch",
            Arc::new(UInt64Array::from_iter_values(
                values.clone().map(|v| v.owner_epoch.get()),
            )),
        ),
        (
            "sequence",
            Arc::new(UInt64Array::from_iter_values(
                values.map(|v| v.sequence.get()),
            )),
        ),
    ])
}
fn qualified(
    values: impl ExactSizeIterator<Item = QualifiedRef> + Clone,
) -> Result<ArrayRef, RowError> {
    structure(vec![
        (
            "dataset_id",
            fixed(values.clone().map(|v| *v.dataset_id.as_bytes()))?,
        ),
        (
            "dictionary_kind",
            Arc::new(UInt16Array::from_iter_values(
                values.clone().map(|v| v.dictionary_kind as u16),
            )),
        ),
        (
            "allocation_domain",
            fixed(values.clone().map(|v| *v.allocation_domain.as_bytes()))?,
        ),
        (
            "ordinal",
            Arc::new(UInt64Array::from_iter_values(
                values.map(|v| v.ordinal.get()),
            )),
        ),
    ])
}
fn envelope(facts: &[&Fact]) -> Result<Columns, RowError> {
    let mut kind = StringBuilder::with_capacity(facts.len(), facts.len() * "event-payload".len());
    for fact in facts {
        kind.append_value(fact.record_kind());
    }
    Ok(vec![
        (
            "source_record_id",
            source(facts.iter().map(|f| f.source_record_id()))?,
        ),
        (
            "record_schema_version",
            Arc::new(UInt16Array::from_iter_values(
                facts.iter().map(|f| f.record_schema_version()),
            )),
        ),
        ("record_kind", Arc::new(kind.finish())),
        (
            "visibility_scope_id",
            qualified(facts.iter().map(|f| f.visibility_scope_id()))?,
        ),
        (
            "payload_digest",
            fixed(facts.iter().map(|f| *f.payload_digest().as_bytes()))?,
        ),
        (
            "recorded_at",
            Arc::new(Int64Array::from_iter_values(
                facts.iter().map(|f| f.recorded_at().get()),
            )),
        ),
    ])
}

#[cfg(test)]
mod tests;
