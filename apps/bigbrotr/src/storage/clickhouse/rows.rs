//! Closed initial serving rows. Each mapping borrows one decoded source fact;
//! only sorted tag references are allocated. These are private scientific rows,
//! not authorized public read models or evidence of completed publication.
//!
//! Source/reference columns are native ClickHouse Tuples. Optional tuples use
//! Array(Tuple), restricted to zero or one element: Nullable(Tuple) is not a
//! supported ClickHouse type. Other options use Nullable scalar columns.
use bigbrotr_model::{Fact, ProjectionGenerationId, QualifiedRef, SourceRecordId};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

use crate::storage::postgres::IndexTarget;

mod digest;
mod mapping;
mod serde_fields;

pub use digest::{ChunkHasher, ChunkSummary, ProjectionRow};
pub use mapping::FactRows;

#[cfg(test)]
mod tests;

/// Dataset, shard, owner epoch, sequence. Every component remains qualified.
pub type SourceTuple = ([u8; 16], u32, u64, u64);
/// Dataset, dictionary kind, allocation domain, ordinal.
pub type ReferenceTuple = ([u8; 16], u16, [u8; 16], u64);

pub const SCHEMA_VERSION: u16 = 1;
pub const PARTITION: &str = "all";
/// AdmissionLimits::facts caps every encoded core fact at 2 MiB. Each tag
/// array/string consumes at least one canonical byte; retained text cannot be
/// larger than the original preimage. This bound does not narrow admitted tags
/// to the smaller convenience defaults in FactDecodeLimits.
pub const MAX_CORE_FACT_BYTES: usize = 2 * 1024 * 1024;
pub const MAX_TAG_READBACK_ALLOCATION_BYTES: usize =
    MAX_CORE_FACT_BYTES * (std::mem::size_of::<String>() + std::mem::size_of::<Vec<String>>() + 1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowsError {
    TagLimit,
    Allocation,
    ProvenanceDisabled,
    WrongTarget,
    WrongGeneration,
    OutsideCut,
    UnorderedRows,
    RowCount,
    SizeOverflow,
    IncompatibleProfile,
}
impl std::fmt::Display for RowsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "initial serving row mapping failed: {self:?}")
    }
}
impl std::error::Error for RowsError {}

#[derive(Clone, Copy)]
struct Envelope {
    projection_generation: [u8; 16],
    source: SourceTuple,
    record_schema_version: u16,
    visibility_scope: ReferenceTuple,
    recorded_at: i64,
    payload_digest: [u8; 32],
}
impl Envelope {
    fn new(generation: ProjectionGenerationId, fact: &Fact) -> Self {
        Self {
            projection_generation: *generation.as_bytes(),
            source: source_tuple(fact.source_record_id()),
            record_schema_version: fact.record_schema_version(),
            visibility_scope: reference_tuple(fact.visibility_scope_id()),
            recorded_at: fact.recorded_at().get(),
            payload_digest: *fact.payload_digest().as_bytes(),
        }
    }
}

fn source_tuple(value: SourceRecordId) -> SourceTuple {
    (
        *value.dataset_id.as_bytes(),
        value.shard_id,
        value.owner_epoch.get(),
        value.sequence.get(),
    )
}
fn reference_tuple(value: QualifiedRef) -> ReferenceTuple {
    (
        *value.dataset_id.as_bytes(),
        value.dictionary_kind as u16,
        *value.allocation_domain.as_bytes(),
        value.ordinal.get(),
    )
}

// The small closed declaration keeps the serialized column list, canonical
// row-value recipe and common immutable envelope together. It does not register
// arbitrary payloads or choose table schemas dynamically.
macro_rules! row {
    ($name:ident $(<$life:lifetime>)?, [$($target:ident),+], {
        $($(#[$attribute:meta])* $field:ident: $ty:ty => $encoding:ident),* $(,)?
    }) => {
        #[derive(Debug, Clone, PartialEq, Eq, clickhouse::Row, Serialize, Deserialize)]
        pub struct $name $(<$life>)? {
            pub projection_generation: [u8; 16],
            pub source: SourceTuple,
            pub record_schema_version: u16,
            pub visibility_scope: ReferenceTuple,
            pub recorded_at: i64,
            pub payload_digest: [u8; 32],
            pub discriminator: u64,
            $($(#[$attribute])* pub $field: $ty,)*
        }
        impl $(<$life>)? digest::sealed::Sealed for $name $(<$life>)? {}
        impl $(<$life>)? ProjectionRow for $name $(<$life>)? {
            fn projection_generation(&self) -> [u8; 16] { self.projection_generation }
            fn source(&self) -> SourceTuple { self.source }
            fn discriminator(&self) -> u64 { self.discriminator }
            fn supports(target: IndexTarget) -> bool {
                matches!(target, $(IndexTarget::$target)|+)
            }
            fn encode_value(&self, value: &mut digest::ValueWriter) -> Result<(), RowsError> {
                value.array(7 + [$(stringify!($field)),*].len() as u64);
                value.bytes(&self.projection_generation)?;
                value.source(&self.source)?;
                value.u16(&self.record_schema_version)?;
                value.reference(&self.visibility_scope)?;
                value.i64(&self.recorded_at)?;
                value.bytes(&self.payload_digest)?;
                value.u64(&self.discriminator)?;
                $(value.$encoding(&self.$field)?;)*
                Ok(())
            }
        }
    };
}

row!(DictionaryRow<'a>, [Dictionaries], {
    dictionary_ref: ReferenceTuple => reference,
    normalization_version: u16 => u16,
    descriptor_schema: Cow<'a, str> => text,
    descriptor_version: u16 => u16,
    descriptor_digest: [u8; 32] => bytes,
    event_id: Option<[u8; 32]> => optional_bytes,
    class: Option<Cow<'a, str>> => optional_text,
    url: Option<Cow<'a, str>> => optional_text,
    clock: Option<Cow<'a, str>> => optional_text,
    vantage: Option<Cow<'a, str>> => optional_text,
    software: Option<Cow<'a, str>> => optional_text,
    #[serde(with = "serde_fields::optional_tuple")]
    method_id: Option<ReferenceTuple> => optional_reference,
    public_key: Option<[u8; 32]> => optional_bytes,
    route_class: Option<Cow<'a, str>> => optional_text,
    software_version: Option<Cow<'a, str>> => optional_text,
    label: Option<Cow<'a, str>> => optional_text,
    #[serde(with = "serde_fields::optional_tuple")]
    access_visibility_scope: Option<ReferenceTuple> => optional_reference,
    input: Option<Cow<'a, str>> => optional_text,
    algorithm: Option<Cow<'a, str>> => optional_text,
    capability: Option<Cow<'a, str>> => optional_text,
    output_unit: Option<Cow<'a, str>> => optional_text,
    failure_taxonomy: Option<Cow<'a, str>> => optional_text,
    algorithm_version: Option<u16> => optional_u16,
    protocol_revision: Option<Cow<'a, str>> => optional_text,
    format: Option<Cow<'a, str>> => optional_text,
    format_version: Option<u16> => optional_u16,
    source_dataset_id: Option<[u8; 16]> => optional_bytes,
    source_artifact_digest: Option<[u8; 32]> => optional_bytes,
});

row!(EventRow<'a>, [Events], {
    event_id: [u8; 32] => bytes,
    pubkey: [u8; 32] => bytes,
    created_at: u64 => u64,
    kind: u16 => u16,
    #[serde(with = "serde_fields::tags")]
    tags: Cow<'a, [Vec<String>]> => tags,
    content: Cow<'a, str> => text,
    event_ordinal: ReferenceTuple => reference,
    body_digest: [u8; 32] => bytes,
});

row!(EligibilityRow<'a>, [EventEligibilityFacts], {
    event_id: [u8; 32] => bytes,
    body_digest: [u8; 32] => bytes,
    #[serde(with = "serde_fields::signature")]
    sig: [u8; 64] => bytes,
    first_eligible_at: i64 => i64,
    origin: Cow<'a, str> => text,
    #[serde(with = "serde_fields::optional_tuple")]
    import_lineage: Option<ReferenceTuple> => optional_reference,
});

row!(ReceiveRow, [EventReceiveFacts], {
    event_id: [u8; 32] => bytes,
    received_at: i64 => i64,
    observer_id: ReferenceTuple => reference,
    clock_id: ReferenceTuple => reference,
    resolution_us: u64 => u64,
    uncertainty_us: Option<u64> => optional_u64,
    anomaly_flags: u32 => u32,
    #[serde(with = "serde_fields::optional_tuple")]
    corrects: Option<SourceTuple> => optional_source,
});

// Same narrow content, three independent table orders and completion outcomes.
row!(EventIndexRow, [EventIdIndex, EventAuthorIndex, EventTimeIndex], {
    event_id: [u8; 32] => bytes,
    pubkey: [u8; 32] => bytes,
    created_at: u64 => u64,
    kind: u16 => u16,
    body_digest: [u8; 32] => bytes,
});

row!(TagRow<'a>, [EventTagIndex], {
    event_id: [u8; 32] => bytes,
    tag_key: Cow<'a, str> => text,
    tag_value: Cow<'a, str> => text,
});

// Both orders retain the full immutable witness, including earlier corrections.
row!(ProvenanceRow, [ProvenanceByEvent, ProvenanceByRelay], {
    event_id: [u8; 32] => bytes,
    endpoint_id: ReferenceTuple => reference,
    observer_id: ReferenceTuple => reference,
    access_scope_id: ReferenceTuple => reference,
    received_at: i64 => i64,
    clock_id: ReferenceTuple => reference,
    resolution_us: u64 => u64,
    uncertainty_us: Option<u64> => optional_u64,
    anomaly_flags: u32 => u32,
    verification_method_id: ReferenceTuple => reference,
    #[serde(with = "serde_fields::optional_tuple")]
    corrects: Option<SourceTuple> => optional_source,
});
