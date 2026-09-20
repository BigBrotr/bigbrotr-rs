//! One-snapshot census of retained source identities and original native files.
//! Scientific payload digests are compared, not recomputed from every body here.
use crate::{catalog::BootstrapRequest, engine::Engine, receipt, schema};
use bigbrotr_model::{
    AdmissionBatch, ArchiveBatch, Digest256, FactRef, OwnerEpoch, Sequence, SourceRecordId,
};
use bigbrotr_protocol::{
    archive_membership::{FORMAT, MAX_PATH_BYTES, MembershipObservation, MembershipRead},
    archive_receipt::{MAX_CHUNK_ROWS, MAX_INPUT_BYTES, ReceiptExpectation},
};
use duckdb::{
    Connection,
    arrow::{
        array::{Array, BinaryArray, BooleanArray, StringArray, UInt32Array, UInt64Array},
        record_batch::RecordBatch,
    },
    params,
};
use receipt::{ReceiptError, ReceiptObservation};
use serde::Deserialize;
use std::{
    io::Read,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
};

// No Debug/Serialize: catalog contains the private reader credential.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MembershipRequest {
    catalog: BootstrapRequest,
    catalog_id: bigbrotr_model::OwnerResourceId,
    expected: MembershipRead,
}
impl MembershipRequest {
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
        request.expected.receipt.validate()?;
        Ok(request)
    }
}
impl Engine {
    pub fn inspect_membership(
        self,
        request: &MembershipRequest,
        max_receipts: usize,
        max_facts: usize,
    ) -> Result<MembershipObservation, ReceiptError> {
        request.expected.validate(max_facts)?;
        self.attach_readonly_catalog(
            &request.catalog,
            request.expected.receipt.dataset_id,
            request.catalog_id,
            request.expected.snapshot_id,
        )?;
        inspect_attached(
            &self.connection,
            &request.expected,
            Path::new(request.catalog.data_root()),
            max_receipts,
            max_facts,
        )
    }
}

fn column<T: Array + 'static>(batch: &RecordBatch, index: usize) -> Result<&T, ReceiptError> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<T>()
        .ok_or(ReceiptError::Native)
}
fn original_path(root: &Path, table: &str, attempt: &[u8; 16]) -> PathBuf {
    let attempt: String = attempt.iter().map(|b| format!("{b:02x}")).collect();
    root.join("main")
        .join(table)
        .join(format!("prepared-{attempt}/data.parquet"))
}
fn require_file(path: &Path) -> Result<(), ReceiptError> {
    let metadata = std::fs::symlink_metadata(path).map_err(|_| ReceiptError::Mismatch)?;
    // The catalog's filename is lexical. Canonicalize every parent too so a
    // relocated attempt cannot qualify through a directory symlink. Concurrent
    // privileged namespace mutation remains outside the operator-owned boundary.
    if !metadata.is_file()
        || metadata.len() == 0
        || metadata.nlink() != 1
        || path.canonicalize().ok().as_deref() != Some(path)
    {
        return Err(ReceiptError::Mismatch);
    }
    Ok(())
}

/// Consume the exact returned membership using the existing canonical archive
/// recipe. Maximal ranges do not recreate original admission grouping: the
/// retained caller input separately establishes complete admission boundaries.
fn require_membership(
    expected: &ReceiptExpectation,
    mut records: Vec<FactRef>,
) -> Result<(), ReceiptError> {
    if records.len() as u64 != expected.record_count {
        return Err(ReceiptError::Mismatch);
    }
    records.sort_unstable_by_key(|r| r.source_record_id);
    let mut records = records.into_iter();
    let mut batches = Vec::new();
    batches
        .try_reserve_exact(expected.input_ranges.len())
        .map_err(|_| ReceiptError::ResourceLimit)?;
    for range in &expected.input_ranges {
        let count = usize::try_from(range.last_sequence - range.first_sequence + 1)
            .map_err(|_| ReceiptError::ResourceLimit)?;
        let mut batch = Vec::new();
        batch
            .try_reserve_exact(count)
            .map_err(|_| ReceiptError::ResourceLimit)?;
        for sequence in range.first_sequence..=range.last_sequence {
            let record = records.next().ok_or(ReceiptError::Mismatch)?;
            let id = record.source_record_id;
            if id.dataset_id != expected.dataset_id
                || id.shard_id != range.shard_id
                || id.owner_epoch.get() != range.owner_epoch
                || id.sequence.get() != sequence
            {
                return Err(ReceiptError::Mismatch);
            }
            batch.push(record);
        }
        batches.push(AdmissionBatch::new(batch).map_err(|_| ReceiptError::Mismatch)?);
    }
    if records.next().is_some() {
        return Err(ReceiptError::Mismatch);
    }
    let archive = ArchiveBatch::new(
        expected.schema_version,
        expected.topology_generation,
        &batches,
    )
    .map_err(|_| ReceiptError::Mismatch)?;
    if archive.digest() != expected.batch_digest {
        return Err(ReceiptError::Mismatch);
    }
    Ok(())
}

pub(crate) fn inspect_attached(
    c: &Connection,
    read: &MembershipRead,
    root: &Path,
    max_receipts: usize,
    max_facts: usize,
) -> Result<MembershipObservation, ReceiptError> {
    read.validate(max_facts)?;
    let expected = &read.receipt;
    let count = usize::try_from(expected.record_count).map_err(|_| ReceiptError::ResourceLimit)?;
    let mut records = Vec::new();
    records
        .try_reserve_exact(count)
        .map_err(|_| ReceiptError::ResourceLimit)?;
    c.execute_batch("BEGIN TRANSACTION")
        .map_err(|_| ReceiptError::Native)?;
    let snapshot_id: u64 = c
        .query_row(
            "SELECT id FROM ducklake_current_snapshot('archive')",
            [],
            |r| r.get(0),
        )
        .map_err(|_| ReceiptError::Native)?;
    if read.snapshot_id.is_some_and(|id| id != snapshot_id) {
        return Err(ReceiptError::Mismatch);
    }
    if receipt::scan_attached(c, expected, max_receipts)? != ReceiptObservation::Present {
        return Err(ReceiptError::Mismatch);
    }
    let receipt_path = original_path(root, "archive_commits", &expected.attempt_id);
    require_file(&receipt_path)?;
    let mut statement = c
        .prepare(&format!(
            "SELECT left(filename,{}) FROM archive.main.archive_commits WHERE attempt_id=? OR filename=? LIMIT 2",
            MAX_PATH_BYTES + 1
        ))
        .map_err(|_| ReceiptError::Native)?;
    let paths = statement
        .query_map(
            params![
                expected.attempt_id.as_slice(),
                receipt_path.to_str().ok_or(ReceiptError::InvalidInput)?
            ],
            |r| r.get::<_, String>(0),
        )
        .map_err(|_| ReceiptError::Native)?;
    let mut found = 0;
    for path in paths {
        let path = path.map_err(|_| ReceiptError::Native)?;
        if path.len() > MAX_PATH_BYTES {
            return Err(ReceiptError::ResourceLimit);
        }
        if Path::new(&path) != receipt_path {
            return Err(ReceiptError::Mismatch);
        }
        found += 1;
    }
    if found != 1 {
        return Err(ReceiptError::Mismatch);
    }
    drop(statement);
    let ranges = expected.input_ranges.iter().map(|r| format!(
        "(source_record_id.shard_id={} AND source_record_id.owner_epoch={} AND source_record_id.sequence BETWEEN {} AND {})",
        r.shard_id, r.owner_epoch, r.first_sequence, r.last_sequence)).collect::<Vec<_>>().join(" OR ");
    let mut family_counts = [0u64; 5];
    for (index, (table, kind)) in schema::TABLES
        .into_iter()
        .zip([
            "dictionary",
            "event-payload",
            "event-eligibility",
            "event-receive",
            "provenance",
        ])
        .enumerate()
    {
        let path = original_path(root, table, &expected.attempt_id);
        // Read fixed identity columns, not a count that metadata could answer.
        // A detection row remains even when earlier families exhaust membership.
        // Include foreign/out-of-range rows in the original file too: merely
        // filtering expected identities would hide surplus original membership.
        // The native engine/process budget still covers metadata binding work.
        let sql = format!(
            "SELECT source_record_id.shard_id,source_record_id.owner_epoch,source_record_id.sequence,CASE WHEN octet_length(payload_digest)=32 THEN payload_digest END,left(filename,{}),record_schema_version=1 AND record_kind='{kind}' AND source_record_id.dataset_id=from_hex('{dataset}') FROM archive.main.{table} WHERE (source_record_id.dataset_id=from_hex('{dataset}') AND ({ranges})) OR filename=? LIMIT {}",
            MAX_PATH_BYTES + 1,
            count - records.len() + 1,
            dataset = expected.dataset_id,
        );
        let mut statement = c.prepare(&sql).map_err(|_| ReceiptError::Native)?;
        let stream = statement
            .stream_arrow(params![path.to_str().ok_or(ReceiptError::InvalidInput)?])
            .map_err(|_| ReceiptError::Native)?;
        for batch in stream {
            if batch.num_rows() > MAX_CHUNK_ROWS {
                return Err(ReceiptError::ResourceLimit);
            }
            let shard = column::<UInt32Array>(&batch, 0)?;
            let epoch = column::<UInt64Array>(&batch, 1)?;
            let sequence = column::<UInt64Array>(&batch, 2)?;
            let digest = column::<BinaryArray>(&batch, 3)?;
            let filename = column::<StringArray>(&batch, 4)?;
            let valid = column::<BooleanArray>(&batch, 5)?;
            for i in 0..batch.num_rows() {
                if records.len() == count
                    || batch.columns().iter().any(|c| c.is_null(i))
                    || !valid.value(i)
                {
                    return Err(ReceiptError::Mismatch);
                }
                if filename.value(i).len() > MAX_PATH_BYTES {
                    return Err(ReceiptError::ResourceLimit);
                }
                if Path::new(filename.value(i)) != path {
                    return Err(ReceiptError::Mismatch);
                }
                records.push(FactRef {
                    source_record_id: SourceRecordId {
                        dataset_id: expected.dataset_id,
                        shard_id: shard.value(i),
                        owner_epoch: OwnerEpoch::new(epoch.value(i))
                            .map_err(|_| ReceiptError::Mismatch)?,
                        sequence: Sequence::new(sequence.value(i))
                            .map_err(|_| ReceiptError::Mismatch)?,
                    },
                    payload_digest: Digest256::from_bytes(
                        digest
                            .value(i)
                            .try_into()
                            .map_err(|_| ReceiptError::Mismatch)?,
                    ),
                });
                family_counts[index] += 1;
            }
        }
        if family_counts[index] != 0 {
            require_file(&path)?;
        }
    }
    require_membership(expected, records)?;
    let observation = MembershipObservation {
        format: FORMAT.into(),
        snapshot_id,
        batch_digest: expected.batch_digest,
        attempt_id: expected.attempt_id,
        publication_ordinal: expected.publication_ordinal,
        family_counts,
        files: 1 + family_counts.iter().filter(|n| **n != 0).count() as u32,
    };
    observation.validate(read)?;
    c.execute_batch("COMMIT")
        .map_err(|_| ReceiptError::Native)?;
    Ok(observation)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn membership_matches_literal_digest_across_gaps_streams_and_unsigned_edges() {
        let vector: serde_json::Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/protocol/archive-membership.json"
        ))
        .unwrap();
        let read: MembershipRead = serde_json::from_value(vector["read"].clone()).unwrap();
        read.validate(4).unwrap();
        let records: Vec<FactRef> = serde_json::from_value(vector["records"].clone()).unwrap();
        let mut shuffled = records.clone();
        shuffled.reverse();
        assert_eq!(require_membership(&read.receipt, shuffled), Ok(()));
        for case in [
            "same-count-gap",
            "duplicate",
            "digest",
            "dataset",
            "epoch",
            "extra",
            "missing",
        ] {
            let mut altered = records.clone();
            match case {
                "same-count-gap" => {
                    altered[1].source_record_id.sequence = Sequence::new(25).unwrap()
                }
                "duplicate" => altered[1] = altered[0],
                "digest" => altered[2].payload_digest = Digest256::from_bytes([0xff; 32]),
                "dataset" => altered[2].source_record_id.dataset_id = [2; 16].try_into().unwrap(),
                "epoch" => altered[2].source_record_id.owner_epoch = OwnerEpoch::new(2).unwrap(),
                "extra" => altered.push(altered[0]),
                "missing" => {
                    altered.pop();
                }
                _ => unreachable!(),
            }
            assert_eq!(
                require_membership(&read.receipt, altered),
                Err(ReceiptError::Mismatch),
                "{case}"
            );
        }
    }
}
