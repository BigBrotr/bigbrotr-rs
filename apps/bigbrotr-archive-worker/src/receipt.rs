//! Fresh, read-only receipt observation. Neither outcome grants replay or ACK.
use crate::{catalog::BootstrapRequest, engine::Engine};
use bigbrotr_model::{DatasetId, Digest256, OwnerResourceId, TopologyGeneration};
use bigbrotr_protocol::archive_receipt::{
    ExpectationError, MAX_CHUNK_ROWS, MAX_INPUT_BYTES, MAX_RANGES, MAX_ROW_BYTES,
    ReceiptExpectation, ReceiptRange,
};
use duckdb::{
    Connection,
    arrow::array::{Array, BooleanArray, StringArray},
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{fmt, io::Read};

pub mod group;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReceiptError {
    InvalidInput,
    ResourceLimit,
    Configuration,
    WrongCatalog,
    Mismatch,
    Native,
}
impl fmt::Display for ReceiptError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "private archive receipt failed: {self:?}")
    }
}
impl std::error::Error for ReceiptError {}
impl From<ExpectationError> for ReceiptError {
    fn from(error: ExpectationError) -> Self {
        match error {
            ExpectationError::InvalidIdentity => Self::InvalidInput,
            ExpectationError::ResourceLimit => Self::ResourceLimit,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReceiptObservation {
    Absent,
    Present,
}

// No Debug/Serialize: the catalog request contains the private reader credential.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReceiptRequest {
    catalog: BootstrapRequest,
    catalog_id: OwnerResourceId,
    expected: ReceiptExpectation,
}
impl ReceiptRequest {
    pub fn read(input: impl Read) -> Result<Self, ReceiptError> {
        let mut bytes = Vec::new();
        input
            .take((MAX_INPUT_BYTES + 1) as u64)
            .read_to_end(&mut bytes)
            .map_err(|_| ReceiptError::InvalidInput)?;
        if bytes.len() > MAX_INPUT_BYTES {
            return Err(ReceiptError::ResourceLimit);
        }
        let request: Self =
            serde_json::from_slice(&bytes).map_err(|_| ReceiptError::InvalidInput)?;
        request
            .catalog
            .validate()
            .map_err(|_| ReceiptError::InvalidInput)?;
        request.expected.validate()?;
        Ok(request)
    }
    pub fn expected(&self) -> &ReceiptExpectation {
        &self.expected
    }
}
fn literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}
fn hex(value: &[u8]) -> String {
    value.iter().map(|v| format!("{v:02x}")).collect()
}

pub(crate) fn readonly_archive_options(
    root: &str,
    snapshot: Option<u64>,
) -> Result<String, ReceiptError> {
    let snapshot = match snapshot {
        Some(id) if id <= i64::MAX as u64 => format!(", SNAPSHOT_VERSION {id}"),
        Some(_) => return Err(ReceiptError::InvalidInput),
        None => String::new(),
    };
    Ok(format!(
        "DATA_PATH {}, DATA_INLINING_ROW_LIMIT 0, AUTOMATIC_MIGRATION false, CREATE_IF_NOT_EXISTS false, READ_ONLY{snapshot}",
        literal(root)
    ))
}

impl Engine {
    /// The caller separately establishes old-lifetime exclusion and full guard
    /// compatibility. Immutable binding checks here refuse a misrouted catalog;
    /// they do not duplicate privileged guard inspection or grant a new writer.
    pub fn inspect_receipt(
        self,
        request: &ReceiptRequest,
        max_receipts: usize,
    ) -> Result<ReceiptObservation, ReceiptError> {
        self.attach_readonly_catalog(
            &request.catalog,
            request.expected.dataset_id,
            request.catalog_id,
            None,
        )?;
        inspect_attached(&self.connection, &request.expected, max_receipts)
        // Engine destruction closes every native client before the response.
    }
}

impl Engine {
    pub(crate) fn attach_readonly_catalog(
        &self,
        catalog: &BootstrapRequest,
        dataset: DatasetId,
        catalog_id: OwnerResourceId,
        snapshot: Option<u64>,
    ) -> Result<(), ReceiptError> {
        let options = readonly_archive_options(catalog.data_root(), snapshot)?;
        let root = std::path::Path::new(catalog.data_root());
        if root.canonicalize().ok().as_deref() != Some(root) || !root.is_dir() {
            return Err(ReceiptError::InvalidInput);
        }
        self.configure_catalog(catalog)
            .map_err(|_| ReceiptError::Configuration)?;
        self.connection.execute_batch("ATTACH '' AS bigbrotr_receipt (TYPE postgres, SECRET 'bigbrotr_bootstrap', SECRET_STORAGE_TABLE '', READ_ONLY)")
            .map_err(|_| ReceiptError::Configuration)?;
        let guard = hex(&Sha256::digest(include_bytes!(
            "../../../migrations/catalog/0001_writer_guard.sql"
        )));
        let science = hex(&Sha256::digest(include_bytes!(
            "../../../migrations/catalog/0002_scientific_schema.sql"
        )));
        let declaration = hex(&Sha256::digest(include_bytes!(
            "../../../contracts/archive/v1/schema.json"
        )));
        let layout = hex(&Sha256::digest(include_bytes!(
            "../../../contracts/archive/v1/native-layout.json"
        )));
        let query = format!(
            "SELECT (SELECT count(*)=1 AND count(*) FILTER(WHERE (singleton AND protocol=1 AND dataset_id=decode('{}','hex') AND catalog_id=decode('{}','hex') AND data_root={} AND sql_checksum=decode('{guard}','hex')) IS TRUE)=1 FROM bigbrotr_archive.installation) AND (SELECT count(*)=1 AND count(*) FILTER(WHERE (singleton AND format_version=1 AND initialization_snapshot=1 AND declaration_checksum=decode('{declaration}','hex') AND native_layout_checksum=decode('{layout}','hex') AND sql_checksum=decode('{science}','hex')) IS TRUE)=1 FROM bigbrotr_archive.scientific_schema) AS valid",
            dataset,
            catalog_id,
            literal(catalog.data_root())
        );
        let valid: bool = self
            .connection
            .query_row(
                &format!(
                    "SELECT valid FROM postgres_query('bigbrotr_receipt', {})",
                    literal(&query)
                ),
                [],
                |r| r.get(0),
            )
            .map_err(|_| ReceiptError::Configuration)?;
        if !valid {
            return Err(ReceiptError::WrongCatalog);
        }
        self.connection
            .execute_batch("DETACH bigbrotr_receipt")
            .map_err(|_| ReceiptError::Native)?;
        self.connection.execute_batch(&format!("ATTACH 'ducklake:postgres:' AS archive ({options}, META_SECRET_STORAGE_TABLE '', META_SECRET 'bigbrotr_bootstrap')"))
            .map_err(|_| ReceiptError::Configuration)?;
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeReceipt {
    dataset_id: DatasetId,
    schema_version: u16,
    topology_generation: u64,
    batch_digest: Digest256,
    record_count: u64,
    input_ranges: Vec<ReceiptRange>,
    attempt_id: OwnerResourceId,
    publication_ordinal: u64,
}
impl NativeReceipt {
    fn decode(text: &str) -> Result<ReceiptExpectation, ReceiptError> {
        let row: Self = serde_json::from_str(text).map_err(|_| ReceiptError::Mismatch)?;
        let expected = ReceiptExpectation {
            dataset_id: row.dataset_id,
            schema_version: row.schema_version,
            topology_generation: TopologyGeneration::new(row.topology_generation),
            batch_digest: row.batch_digest,
            record_count: row.record_count,
            input_ranges: row.input_ranges,
            attempt_id: *row.attempt_id.as_bytes(),
            publication_ordinal: row.publication_ordinal,
        };
        expected.validate().map_err(|error| match error {
            ExpectationError::InvalidIdentity => ReceiptError::Mismatch,
            ExpectationError::ResourceLimit => ReceiptError::ResourceLimit,
        })?;
        Ok(expected)
    }
}
pub(crate) fn inspect_attached(
    c: &Connection,
    expected: &ReceiptExpectation,
    max_receipts: usize,
) -> Result<ReceiptObservation, ReceiptError> {
    scan_limit(expected, max_receipts)?;
    c.execute_batch("BEGIN TRANSACTION")
        .map_err(|_| ReceiptError::Native)?;
    let result = scan_attached(c, expected, max_receipts)?;
    c.execute_batch("COMMIT")
        .map_err(|_| ReceiptError::Native)?;
    Ok(result)
}

/// The caller owns the surrounding transaction and its exact snapshot.
pub(crate) fn scan_attached(
    c: &Connection,
    expected: &ReceiptExpectation,
    max_receipts: usize,
) -> Result<ReceiptObservation, ReceiptError> {
    scan_limit(expected, max_receipts)?;
    let mut found = false;
    scan_receipts(c, expected.dataset_id, max_receipts, |actual| {
        if actual.batch_digest == expected.batch_digest
            || actual.attempt_id == expected.attempt_id
            || actual.publication_ordinal == expected.publication_ordinal
        {
            if found || &actual != expected {
                return Err(ReceiptError::Mismatch);
            }
            found = true;
        }
        Ok(())
    })?;
    Ok(if found {
        ReceiptObservation::Present
    } else {
        ReceiptObservation::Absent
    })
}

/// One bounded full scan shared by single-batch and complete-cut observation.
/// The caller holds one native transaction through every downstream family read.
pub(crate) fn scan_receipts(
    c: &Connection,
    dataset: DatasetId,
    max_receipts: usize,
    mut visit: impl FnMut(ReceiptExpectation) -> Result<(), ReceiptError>,
) -> Result<(), ReceiptError> {
    let maximum = max_receipts
        .checked_add(1)
        .filter(|_| max_receipts > 0)
        .ok_or(ReceiptError::InvalidInput)?;
    // No target predicate: count/existence and filtered scans can skip missing
    // receipt objects using catalog statistics. Consume actual receipt fields
    // through EOF, including after a match, to detect late duplicates/failures.
    // Limits are refusal, never a truncated Absent result.
    let sql = format!(
        "SELECT coalesce(len(input_ranges)>{MAX_RANGES},false), CASE WHEN octet_length(dataset_id)=16 AND octet_length(batch_digest)=32 AND octet_length(attempt_id)=16 AND len(input_ranges)<={MAX_RANGES} THEN left(to_json(struct_pack(dataset_id:=lower(hex(dataset_id)),schema_version:=schema_version,topology_generation:=topology_generation,batch_digest:=lower(hex(batch_digest)),record_count:=record_count,input_ranges:=input_ranges,attempt_id:=lower(hex(attempt_id)),publication_ordinal:=publication_ordinal)),{}) ELSE NULL END FROM archive.main.archive_commits LIMIT {maximum}",
        MAX_ROW_BYTES + 1
    );
    let mut statement = c.prepare(&sql).map_err(|_| ReceiptError::Native)?;
    let stream = statement
        .stream_arrow([])
        .map_err(|_| ReceiptError::Native)?;
    let mut scanned = 0usize;
    for batch in stream {
        if batch.num_rows() > MAX_CHUNK_ROWS {
            return Err(ReceiptError::ResourceLimit);
        }
        let oversized = batch
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or(ReceiptError::Native)?;
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or(ReceiptError::Native)?;
        for row in 0..values.len() {
            scanned += 1;
            if scanned > max_receipts || oversized.value(row) {
                return Err(ReceiptError::ResourceLimit);
            }
            if values.is_null(row) {
                return Err(ReceiptError::Mismatch);
            }
            let text = values.value(row);
            if text.len() > MAX_ROW_BYTES {
                return Err(ReceiptError::ResourceLimit);
            }
            let actual = NativeReceipt::decode(text)?;
            if actual.dataset_id != dataset {
                return Err(ReceiptError::Mismatch);
            }
            visit(actual)?;
        }
    }
    drop(statement);
    Ok(())
}

fn scan_limit(expected: &ReceiptExpectation, max_receipts: usize) -> Result<usize, ReceiptError> {
    let maximum = max_receipts
        .checked_add(1)
        .filter(|_| max_receipts > 0)
        .ok_or(ReceiptError::InvalidInput)?;
    expected.validate()?;
    Ok(maximum)
}

#[cfg(test)]
#[path = "receipt_tests.rs"]
mod tests;
