//! Complete-prefix membership and reference closure inside one native snapshot.
//! The caller separately owns topology, persistence, fencing and retention.
use crate::{
    catalog::BootstrapRequest,
    engine::Engine,
    membership,
    receipt::{self, ReceiptError},
    schema,
};
use bigbrotr_protocol::{
    archive_cut::{CutObservation, CutRead, FORMAT},
    archive_receipt::MAX_INPUT_BYTES,
};
use duckdb::Connection;
use serde::Deserialize;
use std::{io::Read, path::Path};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CutRequest {
    catalog: BootstrapRequest,
    catalog_id: bigbrotr_model::OwnerResourceId,
    expected: CutRead,
}
impl CutRequest {
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
        request.expected.validate(usize::MAX)?;
        Ok(request)
    }
}
impl Engine {
    pub fn inspect_cut(
        self,
        request: &CutRequest,
        max_receipts: usize,
        max_facts: usize,
    ) -> Result<CutObservation, ReceiptError> {
        request.expected.validate(max_facts)?;
        self.attach_readonly_catalog(
            &request.catalog,
            request.expected.dataset_id,
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

pub(crate) fn inspect_attached(
    c: &Connection,
    read: &CutRead,
    root: &Path,
    max_receipts: usize,
    max_facts: usize,
) -> Result<CutObservation, ReceiptError> {
    read.validate(max_facts)?;
    if max_receipts == 0 || max_receipts == usize::MAX {
        return Err(ReceiptError::InvalidInput);
    }
    if read.receipts.len() > max_receipts {
        return Err(ReceiptError::ResourceLimit);
    }
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
    let mut found = vec![false; read.receipts.len()];
    receipt::scan_receipts(c, read.dataset_id, max_receipts, |actual| {
        for (i, expected) in read.receipts.iter().enumerate() {
            if actual.batch_digest == expected.batch_digest
                || actual.attempt_id == expected.attempt_id
                || actual.publication_ordinal == expected.publication_ordinal
            {
                if found[i] || &actual != expected {
                    return Err(ReceiptError::Mismatch);
                }
                found[i] = true;
            }
        }
        Ok(())
    })?;
    if found.contains(&false) {
        return Err(ReceiptError::Mismatch);
    }
    let mut counts = [0u64; 5];
    for expected in &read.receipts {
        let batch = membership::scan_members(c, expected, root, max_facts)?;
        for (count, n) in counts.iter_mut().zip(batch) {
            *count = count.checked_add(n).ok_or(ReceiptError::Mismatch)?;
        }
    }
    reference_closure(c, read, counts)?;
    let observation = CutObservation {
        format: FORMAT.into(),
        snapshot_id,
        request_digest: read.digest(),
        family_counts: counts,
    };
    observation.validate(read, max_facts)?;
    c.execute_batch("COMMIT")
        .map_err(|_| ReceiptError::Native)?;
    Ok(observation)
}

fn refuses(c: &Connection, rows: &str) -> Result<(), ReceiptError> {
    let invalid: bool = c
        .query_row(&format!("SELECT EXISTS({rows})"), [], |r| r.get(0))
        .map_err(|_| ReceiptError::Native)?;
    if invalid {
        Err(ReceiptError::Mismatch)
    } else {
        Ok(())
    }
}

pub(crate) fn reference_closure(
    c: &Connection,
    read: &CutRead,
    counts: [u64; 5],
) -> Result<(), ReceiptError> {
    let ranges = read.bounds.iter().filter(|b| b.complete_through != 0).map(|b|format!(
        "(source_record_id.shard_id={} AND source_record_id.owner_epoch={} AND source_record_id.sequence BETWEEN 1 AND {})",
        b.shard_id,b.owner_epoch,b.complete_through)).collect::<Vec<_>>().join(" OR ");
    let ranges = if ranges.is_empty() { "false" } else { &ranges };
    // Views let the native optimizer project only dependency columns. Per-family
    // limits have one detection row; membership was independently read above.
    // Native metadata, joins, descriptor equality and spill remain inside the
    // worker's memory/temp grant and the parent's finite supervised deadline.
    for (table, count) in schema::TABLES.into_iter().zip(counts) {
        let limit = count.checked_add(1).ok_or(ReceiptError::ResourceLimit)?;
        c.execute_batch(&format!("CREATE TEMP VIEW cut_{table} AS SELECT * FROM archive.main.{table} WHERE source_record_id.dataset_id=from_hex('{}') AND ({ranges}) LIMIT {limit}",read.dataset_id)).map_err(|_| ReceiptError::Native)?;
        let actual: u64 = c
            .query_row(&format!("SELECT count(*) FROM cut_{table}"), [], |r| {
                r.get(0)
            })
            .map_err(|_| ReceiptError::Native)?;
        if actual != count {
            return Err(ReceiptError::Mismatch);
        }
    }
    let mut refs = schema::TABLES
        .iter()
        .map(|table| {
            format!("SELECT visibility_scope_id AS reference,4 AS expected_kind FROM cut_{table}")
        })
        .collect::<Vec<_>>();
    refs.extend([
        "SELECT descriptor_value.method_id,5 FROM cut_dictionaries WHERE descriptor_schema='observer' AND descriptor_value.method_id IS NOT NULL",
        "SELECT descriptor_value.visibility_scope_id,4 FROM cut_dictionaries WHERE descriptor_schema='access-scope'",
        "SELECT event_ordinal,8 FROM cut_events",
        "SELECT import_lineage,9 FROM cut_event_eligibility_facts WHERE origin='import'",
        "SELECT observer_id,2 FROM cut_event_receive_facts",
        "SELECT clock_quality.clock_id,2 FROM cut_event_receive_facts",
        "SELECT endpoint_id,1 FROM cut_provenance_facts",
        "SELECT observer_id,2 FROM cut_provenance_facts",
        "SELECT access_scope_id,3 FROM cut_provenance_facts",
        "SELECT clock_quality.clock_id,2 FROM cut_provenance_facts",
        "SELECT verification_method_id,5 FROM cut_provenance_facts",
    ].into_iter().map(str::to_owned));
    c.execute_batch(&format!(
        "CREATE TEMP VIEW cut_required_refs AS {}",
        refs.join(" UNION ALL ")
    ))
    .map_err(|_| ReceiptError::Native)?;
    let valid_key = format!(
        "reference IS NOT NULL AND reference.dataset_id=from_hex('{}') AND octet_length(reference.allocation_domain)=16 AND reference.allocation_domain<>from_hex(repeat('00',16)) AND reference.ordinal>0",
        read.dataset_id
    );
    refuses(
        c,
        &format!(
            "SELECT 1 FROM cut_required_refs r WHERE ({valid_key} AND reference.dictionary_kind=expected_kind) IS NOT TRUE OR NOT EXISTS(SELECT 1 FROM cut_dictionaries d WHERE d.reference=r.reference)"
        ),
    )?;
    // Repeated identical mappings in distinct source facts are permitted; a
    // conflicting value under one qualified name is never chosen arbitrarily.
    refuses(
        c,
        "SELECT 1 FROM (SELECT DISTINCT reference,normalization_version,descriptor_schema,descriptor_version,descriptor_value FROM cut_dictionaries) GROUP BY reference HAVING count(*)>1",
    )?;
    refuses(
        c,
        &format!(
            "SELECT 1 FROM cut_dictionaries WHERE ({valid_key} AND normalization_version=1 AND descriptor_version=1 AND reference.dictionary_kind=CASE descriptor_schema WHEN 'endpoint' THEN 1 WHEN 'observer' THEN 2 WHEN 'access-scope' THEN 3 WHEN 'visibility' THEN 4 WHEN 'event-verification' THEN 5 WHEN 'event' THEN 8 WHEN 'dataset-lineage' THEN 9 ELSE 0 END) IS NOT TRUE"
        ),
    )?;
    refuses(
        c,
        "SELECT 1 FROM cut_events e WHERE octet_length(event_id)<>32 OR NOT EXISTS(SELECT 1 FROM cut_dictionaries d WHERE d.reference=e.event_ordinal AND d.descriptor_schema='event' AND d.descriptor_value.event_id=e.event_id)",
    )?;
    refuses(
        c,
        "SELECT 1 FROM cut_events GROUP BY event_id HAVING count(*)<>1",
    )?;
    refuses(
        c,
        "SELECT 1 FROM cut_event_eligibility_facts e WHERE ((origin='network' AND import_lineage IS NULL) OR (origin='import' AND import_lineage IS NOT NULL)) IS NOT TRUE OR NOT EXISTS(SELECT 1 FROM cut_events p WHERE p.event_id=e.event_id)",
    )?;
    refuses(
        c,
        "SELECT 1 FROM cut_event_eligibility_facts GROUP BY event_id,visibility_scope_id HAVING count(*)<>1",
    )?;
    for table in ["event_receive_facts", "provenance_facts"] {
        refuses(
            c,
            &format!(
                "SELECT 1 FROM cut_{table} f WHERE NOT EXISTS(SELECT 1 FROM cut_event_eligibility_facts e WHERE e.event_id=f.event_id AND e.visibility_scope_id=f.visibility_scope_id)"
            ),
        )?;
        let key = if table == "provenance_facts" {
            " AND p.endpoint_id=f.endpoint_id AND p.observer_id=f.observer_id AND p.access_scope_id=f.access_scope_id"
        } else {
            ""
        };
        // Source order exists only within one shard/epoch. Across streams the
        // referenced target and earlier observation time establish the edge.
        refuses(
            c,
            &format!(
                "SELECT 1 FROM cut_{table} f WHERE corrects IS NOT NULL AND ((corrects.shard_id=source_record_id.shard_id AND corrects.owner_epoch=source_record_id.owner_epoch AND corrects.sequence>=source_record_id.sequence) OR NOT EXISTS(SELECT 1 FROM cut_{table} p WHERE p.source_record_id=f.corrects AND p.event_id=f.event_id AND p.visibility_scope_id=f.visibility_scope_id AND p.received_at>f.received_at{key}))"
            ),
        )?;
    }
    Ok(())
}
