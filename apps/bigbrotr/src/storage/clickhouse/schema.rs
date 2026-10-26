//! Closed first serving layout. These declarations are inputs to real inspection,
//! never installation or durability evidence by themselves. One Atomic database
//! contains exactly these ten append-only tables, without native projections.
use crate::storage::postgres::{IndexBinding, IndexTarget};
use bigbrotr_model::Digest256;
use sha2::{Digest, Sha256};

pub const SERVER_VERSION: &str = "26.8.35.10";
pub const MAX_TABLE_MANIFEST_BYTES: usize = 4096;
const SOURCE: &str = "Tuple(FixedString(16), UInt32, UInt64, UInt64)";
const REFERENCE: &str = "Tuple(FixedString(16), UInt16, FixedString(16), UInt64)";
const OPTIONAL_SOURCE: &str = "Array(Tuple(FixedString(16), UInt32, UInt64, UInt64))";
const OPTIONAL_REFERENCE: &str = "Array(Tuple(FixedString(16), UInt16, FixedString(16), UInt64))";
const SETTINGS: &str =
    "fsync_after_insert = 1, fsync_part_directory = 1, non_replicated_deduplication_window = 0";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaError {
    InvalidDatabase,
    InvalidUuid,
    Binding,
    Manifest,
}

/// Supplied before local custody is created; actual engine UUIDs must equal this
/// manifest. UUID allocation/retention belongs to explicit instance initialization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexTables {
    database: String,
    uuids: [[u8; 16]; 10],
    identity_digest: Digest256,
}
impl IndexTables {
    pub fn new(database: String, uuids: [[u8; 16]; 10]) -> Result<Self, SchemaError> {
        if database.is_empty()
            || database.len() > 63
            || !database.as_bytes()[0].is_ascii_alphabetic()
            || !database
                .bytes()
                .all(|v| v.is_ascii_alphanumeric() || v == b'_')
            || matches!(
                database.as_str(),
                "system" | "INFORMATION_SCHEMA" | "information_schema"
            )
        {
            return Err(SchemaError::InvalidDatabase);
        }
        for (i, uuid) in uuids.iter().enumerate() {
            if *uuid == [0; 16] || uuids[..i].contains(uuid) {
                return Err(SchemaError::InvalidUuid);
            }
        }
        let mut value = Self {
            database,
            uuids,
            identity_digest: Digest256::from_bytes([0; 32]),
        };
        let mut hash = Sha256::new();
        hash.update(b"bigbrotr/index-table-manifest/1\0");
        hash_text(&mut hash, SERVER_VERSION);
        hash_text(&mut hash, &value.database);
        hash_text(&mut hash, "Atomic");
        for target in IndexTarget::ALL {
            hash_text(&mut hash, target.as_str());
            hash.update(value.uuid(target));
            hash_text(&mut hash, &engine_expression(target));
            hash.update((columns(target).count() as u16).to_be_bytes());
            for column in columns(target) {
                hash_text(&mut hash, column.name);
                hash_text(&mut hash, column.ty);
            }
        }
        value.identity_digest = Digest256::from_bytes(hash.finalize().into());
        Ok(value)
    }
    /// Closed, bounded, non-secret manifest retained by the filesystem owner.
    pub fn encode_manifest(&self) -> Result<Vec<u8>, SchemaError> {
        let manifest = TablesManifest {
            format: "bigbrotr-index-tables/1".into(),
            server_version: SERVER_VERSION.into(),
            database: self.database.clone(),
            table_uuids: self.uuids.map(uuid_text),
        };
        let bytes = serde_json::to_vec(&manifest).map_err(|_| SchemaError::Manifest)?;
        if bytes.len() > MAX_TABLE_MANIFEST_BYTES {
            return Err(SchemaError::Manifest);
        }
        Ok(bytes)
    }
    pub fn decode_manifest(bytes: &[u8]) -> Result<Self, SchemaError> {
        if bytes.len() > MAX_TABLE_MANIFEST_BYTES {
            return Err(SchemaError::Manifest);
        }
        let manifest: TablesManifest =
            serde_json::from_slice(bytes).map_err(|_| SchemaError::Manifest)?;
        if manifest.format != "bigbrotr-index-tables/1" || manifest.server_version != SERVER_VERSION
        {
            return Err(SchemaError::Manifest);
        }
        let mut uuids = [[0; 16]; 10];
        for (target, text) in uuids.iter_mut().zip(manifest.table_uuids) {
            *target = parse_uuid(&text)?;
        }
        Self::new(manifest.database, uuids)
    }
    pub fn database(&self) -> &str {
        &self.database
    }
    pub fn uuid(&self, target: IndexTarget) -> [u8; 16] {
        self.uuids[target as usize]
    }
    pub fn identity_digest(&self) -> Digest256 {
        self.identity_digest
    }
    pub(crate) fn check_binding(&self, binding: &IndexBinding) -> Result<(), SchemaError> {
        if binding.table_identity_digest() != self.identity_digest {
            return Err(SchemaError::Binding);
        }
        Ok(())
    }
    pub(crate) fn qualified(&self, target: IndexTarget) -> String {
        format!("`{}`.`{}`", self.database, target.as_str())
    }
    /// Fresh-only DDL: no IF NOT EXISTS/adoption, replacement, ALTER or cleanup.
    /// The caller verifies an existing empty Atomic database before dispatch.
    pub(crate) fn create_table(&self, target: IndexTarget) -> String {
        let fields = columns(target)
            .map(|c| format!("`{}` {}", c.name, c.ty))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "CREATE TABLE {} UUID '{}' ({fields}) ENGINE = {}",
            self.qualified(target),
            uuid_text(self.uuid(target)),
            engine_expression(target)
        )
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct TablesManifest {
    format: String,
    server_version: String,
    database: String,
    table_uuids: [String; 10],
}
fn parse_uuid(text: &str) -> Result<[u8; 16], SchemaError> {
    let bytes = text.as_bytes();
    if bytes.len() != 36 {
        return Err(SchemaError::InvalidUuid);
    }
    let mut decoded = [0; 16];
    let mut offset = 0;
    for (position, value) in bytes.iter().copied().enumerate() {
        if [8, 13, 18, 23].contains(&position) {
            if value != b'-' {
                return Err(SchemaError::InvalidUuid);
            }
            continue;
        }
        let nibble = match value {
            b'0'..=b'9' => value - b'0',
            b'a'..=b'f' => value - b'a' + 10,
            _ => return Err(SchemaError::InvalidUuid),
        };
        decoded[offset / 2] = (decoded[offset / 2] << 4) | nibble;
        offset += 1;
    }
    Ok(decoded)
}

fn hash_text(hash: &mut Sha256, text: &str) {
    hash.update((text.len() as u32).to_be_bytes());
    hash.update(text.as_bytes());
}
pub(crate) fn uuid_text(bytes: [u8; 16]) -> String {
    let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..]
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Column {
    pub name: &'static str,
    pub ty: &'static str,
}
macro_rules! cols {
    ($($name:ident : $ty:expr),* $(,)?) => { &[$(Column{name:stringify!($name),ty:$ty}),*] };
}
const COMMON: &[Column] = cols![
    projection_generation: "FixedString(16)", source: SOURCE,
    record_schema_version: "UInt16", visibility_scope: REFERENCE,
    recorded_at: "Int64", payload_digest: "FixedString(32)", discriminator: "UInt64",
];
const DICTIONARY: &[Column] = cols![
    dictionary_ref: REFERENCE, normalization_version: "UInt16", descriptor_schema: "String",
    descriptor_version: "UInt16", descriptor_digest: "FixedString(32)",
    event_id: "Nullable(FixedString(32))", class: "Nullable(String)", url: "Nullable(String)",
    clock: "Nullable(String)", vantage: "Nullable(String)", software: "Nullable(String)",
    method_id: OPTIONAL_REFERENCE, public_key: "Nullable(FixedString(32))",
    route_class: "Nullable(String)", software_version: "Nullable(String)", label: "Nullable(String)",
    access_visibility_scope: OPTIONAL_REFERENCE, input: "Nullable(String)", algorithm: "Nullable(String)",
    capability: "Nullable(String)", output_unit: "Nullable(String)", failure_taxonomy: "Nullable(String)",
    algorithm_version: "Nullable(UInt16)", protocol_revision: "Nullable(String)", format: "Nullable(String)",
    format_version: "Nullable(UInt16)", source_dataset_id: "Nullable(FixedString(16))",
    source_artifact_digest: "Nullable(FixedString(32))",
];
const EVENT: &[Column] = cols![
    event_id: "FixedString(32)", pubkey: "FixedString(32)", created_at: "UInt64", kind: "UInt16",
    tags: "Array(Array(String))", content: "String", event_ordinal: REFERENCE, body_digest: "FixedString(32)",
];
const ELIGIBILITY: &[Column] = cols![
    event_id: "FixedString(32)", body_digest: "FixedString(32)", sig: "FixedString(64)",
    first_eligible_at: "Int64", origin: "String", import_lineage: OPTIONAL_REFERENCE,
];
const RECEIVE: &[Column] = cols![
    event_id: "FixedString(32)", received_at: "Int64", observer_id: REFERENCE, clock_id: REFERENCE,
    resolution_us: "UInt64", uncertainty_us: "Nullable(UInt64)", anomaly_flags: "UInt32", corrects: OPTIONAL_SOURCE,
];
const EVENT_INDEX: &[Column] = cols![
    event_id: "FixedString(32)", pubkey: "FixedString(32)", created_at: "UInt64", kind: "UInt16", body_digest: "FixedString(32)",
];
const TAG: &[Column] = cols![event_id: "FixedString(32)", tag_key: "String", tag_value: "String"];
const PROVENANCE: &[Column] = cols![
    event_id: "FixedString(32)", endpoint_id: REFERENCE, observer_id: REFERENCE, access_scope_id: REFERENCE,
    received_at: "Int64", clock_id: REFERENCE, resolution_us: "UInt64", uncertainty_us: "Nullable(UInt64)",
    anomaly_flags: "UInt32", verification_method_id: REFERENCE, corrects: OPTIONAL_SOURCE,
];
pub(crate) fn columns(target: IndexTarget) -> impl Iterator<Item = Column> {
    let payload = match target {
        IndexTarget::Dictionaries => DICTIONARY,
        IndexTarget::Events => EVENT,
        IndexTarget::EventEligibilityFacts => ELIGIBILITY,
        IndexTarget::EventReceiveFacts => RECEIVE,
        IndexTarget::EventIdIndex | IndexTarget::EventAuthorIndex | IndexTarget::EventTimeIndex => {
            EVENT_INDEX
        }
        IndexTarget::EventTagIndex => TAG,
        IndexTarget::ProvenanceByEvent | IndexTarget::ProvenanceByRelay => PROVENANCE,
    };
    COMMON.iter().chain(payload).copied()
}
pub(crate) fn field_list(target: IndexTarget) -> String {
    columns(target)
        .map(|c| format!("`{}`", c.name))
        .collect::<Vec<_>>()
        .join(", ")
}
pub(crate) fn sort_key(target: IndexTarget) -> &'static str {
    match target {
        IndexTarget::Dictionaries => "projection_generation, dictionary_ref, source, discriminator",
        IndexTarget::Events | IndexTarget::EventTimeIndex => {
            "projection_generation, created_at, event_id, source, discriminator"
        }
        IndexTarget::EventEligibilityFacts | IndexTarget::EventReceiveFacts => {
            "projection_generation, event_id, visibility_scope, source, discriminator"
        }
        IndexTarget::EventIdIndex => "projection_generation, event_id, source, discriminator",
        IndexTarget::EventAuthorIndex => {
            "projection_generation, pubkey, created_at, event_id, source, discriminator"
        }
        IndexTarget::EventTagIndex => {
            "projection_generation, tag_key, tag_value, event_id, source, discriminator"
        }
        IndexTarget::ProvenanceByEvent => {
            "projection_generation, event_id, endpoint_id, observer_id, access_scope_id, visibility_scope, source, discriminator"
        }
        IndexTarget::ProvenanceByRelay => {
            "projection_generation, endpoint_id, event_id, observer_id, access_scope_id, visibility_scope, source, discriminator"
        }
    }
}
pub(crate) fn engine_expression(target: IndexTarget) -> String {
    format!(
        "MergeTree PARTITION BY 'all' ORDER BY ({}) SETTINGS {SETTINGS}",
        sort_key(target)
    )
}

#[cfg(test)]
#[path = "schema/tests.rs"]
mod tests;
