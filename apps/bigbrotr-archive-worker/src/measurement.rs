//! Exact typed A05 hydration and strong unknown-resolution closure. No reduction
//! selects a winner or counts children; the caller owns read and cut authority.
use crate::{
    catalog::BootstrapRequest,
    event::{qualified, source},
};
use bigbrotr_model::{FactRef, OwnerResourceId};
use bigbrotr_protocol::archive_measurement::{
    FORMAT, MAX_ROW_BYTES, MeasurementFactRow, MeasurementObservation, MeasurementRead,
    MeasurementReadError as Error,
};
use duckdb::{
    Connection,
    arrow::array::{Array, StringArray},
};
use serde::Deserialize;
use std::{io::Read, path::Path};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MeasurementRequest {
    catalog: BootstrapRequest,
    catalog_id: OwnerResourceId,
    expected: MeasurementRead,
}
impl MeasurementRequest {
    pub fn read(input: impl Read) -> Result<Self, Error> {
        let mut bytes = Vec::new();
        input
            .take((bigbrotr_protocol::archive_receipt::MAX_INPUT_BYTES + 1) as u64)
            .read_to_end(&mut bytes)
            .map_err(|_| Error::InvalidInput)?;
        if bytes.len() > bigbrotr_protocol::archive_receipt::MAX_INPUT_BYTES {
            return Err(Error::ResourceLimit);
        }
        let request: Self = serde_json::from_slice(&bytes).map_err(|_| Error::InvalidInput)?;
        request
            .catalog
            .validate()
            .map_err(|_| Error::InvalidInput)?;
        request.expected.validate(usize::MAX)?;
        Ok(request)
    }
}

impl crate::engine::Engine {
    /// One selected complete cut and one exact original record in the SAME read
    /// transaction. This grants no public disclosure or installed read pin.
    pub fn read_measurement(
        self,
        request: &MeasurementRequest,
        max_receipts: usize,
        max_facts: usize,
    ) -> Result<MeasurementObservation, Error> {
        let read = &request.expected;
        read.validate(max_facts)?;
        self.attach_readonly_catalog(
            &request.catalog,
            read.cut.dataset_id,
            request.catalog_id,
            read.cut.snapshot_id,
        )
        .map_err(|_| Error::Native)?;
        let c = &self.connection;
        c.execute_batch("BEGIN TRANSACTION")
            .map_err(|_| Error::Native)?;
        let cut = crate::cut::inspect_in_transaction(
            c,
            &read.cut,
            Path::new(request.catalog.data_root()),
            max_receipts,
            max_facts,
        )
        .map_err(|error| match error {
            crate::receipt::ReceiptError::ResourceLimit => Error::ResourceLimit,
            crate::receipt::ReceiptError::InvalidInput => Error::InvalidInput,
            crate::receipt::ReceiptError::Native => Error::Native,
            _ => Error::Mismatch,
        })?;
        let fact = read_one(c, "cut_measurement_facts", read.fact)?;
        let observation = MeasurementObservation {
            format: FORMAT.into(),
            cut,
            fact,
        };
        observation.validate(read, max_facts)?;
        c.execute_batch("COMMIT").map_err(|_| Error::Native)?;
        Ok(observation)
    }
}

fn moment(column: &str) -> String {
    format!(
        "CASE WHEN {column} IS NULL THEN NULL ELSE struct_pack(observed_at:={column}.observed_at::VARCHAR,elapsed_us:={column}.elapsed_us::VARCHAR) END"
    )
}
fn optional(column: &str, value: String) -> String {
    format!("CASE WHEN {column} IS NULL THEN NULL ELSE {value} END")
}
fn row_json(prefix: &str) -> String {
    let q = |name: &str| qualified(&format!("{prefix}.{name}"));
    let profile = [
        "version",
        "base_cadence_us",
        "overall_timeout_us",
        "open_timeout_us",
        "relay_connections",
        "requests",
        "active_requests",
        "tries",
        "sole_filter_kind",
        "request_limit",
        "compression_disabled_code",
        "max_message_framed_bytes",
        "max_normalized_event_bytes",
        "max_rx_physical_frames",
        "max_rx_messages",
        "max_event_candidates",
        "max_rx_ws_bytes",
        "max_tx_ws_bytes",
        "dns_exchange_timeout_us",
        "dns_max_exchanges_per_family",
        "dns_max_frame_bytes",
        "dns_max_addresses",
        "auth_response_disabled_code",
        "effective_authority_ceiling_us",
    ]
    .map(|name| format!("{prefix}.profile.{name}"))
    .join(",");
    let prior = optional(
        &format!("{prefix}.prior_unknown"),
        format!(
            "struct_pack(source_record_id:={},payload_digest:=lower(hex({prefix}.prior_unknown.payload_digest)))",
            source(&format!("{prefix}.prior_unknown.source_record_id"))
        ),
    );
    let clock = format!(
        "struct_pack(clock_id:={},resolution_us:={prefix}.clock_quality.resolution_us::VARCHAR,uncertainty_us:={prefix}.clock_quality.uncertainty_us::VARCHAR,anomaly_flags:={prefix}.clock_quality.anomaly_flags)",
        q("clock_quality.clock_id")
    );
    let execution = format!(
        "struct_pack(destination:={prefix}.observed.execution.destination,result:={prefix}.observed.execution.result,error:={prefix}.observed.execution.error,observed_at:={prefix}.observed.execution.observed_at::VARCHAR)"
    );
    let open = format!(
        "struct_pack(tcp_connected:={},tls_completed:={},websocket_opened:={})",
        moment(&format!("{prefix}.observed.open.tcp_connected")),
        moment(&format!("{prefix}.observed.open.tls_completed")),
        moment(&format!("{prefix}.observed.open.websocket_opened"))
    );
    let event_prefix = format!("{prefix}.observed.read.first_valid_event");
    let event = optional(
        &event_prefix,
        format!(
            "struct_pack(event_id:=lower(hex({event_prefix}.event_id)),received:={},verified:={})",
            moment(&format!("{event_prefix}.received")),
            moment(&format!("{event_prefix}.verified"))
        ),
    );
    let read = format!(
        "struct_pack(request_flushed:={},first_valid_event:={event},eose:={},eose_prefix_verified:={prefix}.observed.read.eose_prefix_verified,auth_observed:={prefix}.observed.read.auth_observed,correlated_closed:={prefix}.observed.read.correlated_closed)",
        moment(&format!("{prefix}.observed.read.request_flushed")),
        moment(&format!("{prefix}.observed.read.eose"))
    );
    let observed = optional(
        &format!("{prefix}.observed"),
        format!(
            "struct_pack(execution:={execution},open:={open},read:={read},counters:={prefix}.observed.counters)"
        ),
    );
    let payload = format!(
        "struct_pack(attempt_id:=lower(hex({prefix}.attempt_id)),subject_kind:={prefix}.subject_kind,endpoint_id:={},observer_id:={},route_id:={},access_scope_id:={},method_id:={},profile:=list_value({profile}),clock_quality:={clock},state_kind:={prefix}.state_kind,prior_unknown:={prior},observed:={observed},ended:={},termination:={prefix}.termination,teardown:={prefix}.teardown)",
        q("endpoint_id"),
        q("observer_id"),
        q("route_id"),
        q("access_scope_id"),
        q("method_id"),
        moment(&format!("{prefix}.ended"))
    );
    format!(
        "to_json(struct_pack(source_record_id:={},record_schema_version:={prefix}.record_schema_version,record_kind:={prefix}.record_kind,visibility_scope_id:={},payload_digest:=lower(hex({prefix}.payload_digest)),recorded_at:={prefix}.recorded_at::VARCHAR,payload:={payload}))",
        source(&format!("{prefix}.source_record_id")),
        q("visibility_scope_id")
    )
}
fn decode(text: &str) -> Result<MeasurementFactRow, Error> {
    if text.len() > MAX_ROW_BYTES {
        return Err(Error::ResourceLimit);
    }
    let row: MeasurementFactRow = serde_json::from_str(text).map_err(|_| Error::Mismatch)?;
    Ok(row)
}
fn relation(value: &str) -> Result<&str, Error> {
    match value {
        "stage.measurement_facts"
        | "publication_measurements"
        | "cut_measurement_facts"
        | "archive.main.measurement_facts" => Ok(value),
        _ => Err(Error::InvalidInput),
    }
}

pub(crate) fn read_one(
    c: &Connection,
    table: &str,
    expected: FactRef,
) -> Result<MeasurementFactRow, Error> {
    let table = relation(table)?;
    let id = expected.source_record_id;
    // Select only by complete source identity. Filtering by digest would hide a
    // conflicting stored duplicate instead of refusing it. All values are bounded
    // canonical identifiers, and the relation is a closed compiled choice.
    let sql = format!(
        "SELECT left({}, {}) FROM {table} f WHERE f.source_record_id.dataset_id=from_hex('{}') AND f.source_record_id.shard_id={} AND f.source_record_id.owner_epoch={} AND f.source_record_id.sequence={} LIMIT 2",
        row_json("f"),
        MAX_ROW_BYTES + 1,
        id.dataset_id,
        id.shard_id,
        id.owner_epoch.get(),
        id.sequence.get()
    );
    let mut statement = c.prepare(&sql).map_err(|_| Error::Native)?;
    let stream = statement.stream_arrow([]).map_err(|_| Error::Native)?;
    let mut found = None;
    for batch in stream {
        if batch.num_rows() > 2 {
            return Err(Error::Mismatch);
        }
        let rows = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or(Error::Native)?;
        for i in 0..rows.len() {
            if rows.is_null(i) || found.is_some() {
                return Err(Error::Mismatch);
            }
            let row = decode(rows.value(i))?;
            if row.to_fact()?.fact_ref() != expected {
                return Err(Error::Mismatch);
            }
            found = Some(row);
        }
    }
    found.ok_or(Error::Missing)
}

/// The caller first selects actual complete input membership/cut. This scan
/// retains at most the current/prior fixed rows, never a list of payloads.
pub(crate) fn validate_rows(
    c: &Connection,
    current: &str,
    targets: &str,
    dictionaries: &str,
    max_rows: usize,
) -> Result<(), Error> {
    let current = relation(current)?;
    let targets = relation(targets)?;
    if !matches!(
        dictionaries,
        "publication_dictionaries" | "cut_dictionaries"
    ) {
        return Err(Error::InvalidInput);
    }
    let limit = max_rows.checked_add(1).ok_or(Error::ResourceLimit)?;
    let invalid: bool = c.query_row(&format!("SELECT EXISTS(SELECT 1 FROM {current} m WHERE NOT EXISTS(SELECT 1 FROM {dictionaries} d WHERE d.reference=m.access_scope_id AND d.descriptor_schema='access-scope' AND d.normalization_version=1 AND d.descriptor_version=1 AND d.descriptor_value.visibility_scope_id=m.visibility_scope_id))"),[],|r|r.get(0)).map_err(|_| Error::Native)?;
    if invalid {
        return Err(Error::Mismatch);
    }
    for (field, schema) in [
        ("endpoint_id", "endpoint"),
        ("observer_id", "observer"),
        ("route_id", "direct-route"),
        ("method_id", "passive-ws-read"),
        ("clock_quality.clock_id", "observer"),
    ] {
        let invalid: bool = c.query_row(&format!("SELECT EXISTS(SELECT 1 FROM {current} m WHERE NOT EXISTS(SELECT 1 FROM {dictionaries} d WHERE d.reference=m.{field} AND d.descriptor_schema='{schema}' AND d.normalization_version=1 AND d.descriptor_version=1))"),[],|r|r.get(0)).map_err(|_| Error::Native)?;
        if invalid {
            return Err(Error::Mismatch);
        }
    }
    let sql = format!(
        "SELECT left({},{}),CASE WHEN p.source_record_id IS NULL THEN NULL ELSE left({},{}) END FROM {current} f LEFT JOIN {targets} p ON f.prior_unknown.source_record_id=p.source_record_id ORDER BY f.source_record_id LIMIT {limit}",
        row_json("f"),
        MAX_ROW_BYTES + 1,
        row_json("p"),
        MAX_ROW_BYTES + 1
    );
    let mut statement = c.prepare(&sql).map_err(|_| Error::Native)?;
    let stream = statement.stream_arrow([]).map_err(|_| Error::Native)?;
    let mut seen = 0usize;
    let mut last = None;
    for batch in stream {
        if batch.num_rows() > bigbrotr_protocol::archive_receipt::MAX_CHUNK_ROWS {
            return Err(Error::ResourceLimit);
        }
        let current = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or(Error::Native)?;
        let prior = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or(Error::Native)?;
        for i in 0..batch.num_rows() {
            seen = seen
                .checked_add(1)
                .filter(|n| *n <= max_rows)
                .ok_or(Error::ResourceLimit)?;
            if current.is_null(i) {
                return Err(Error::Mismatch);
            }
            let row = decode(current.value(i))?;
            let fact = row.to_fact()?;
            if last.is_some_and(|previous| previous >= fact.source_record_id()) {
                return Err(Error::Mismatch);
            }
            last = Some(fact.source_record_id());
            match (row.payload.prior_unknown, prior.is_null(i)) {
                (None, true) => {}
                (Some(_), false) => fact
                    .validate_measurement_resolution(&decode(prior.value(i))?.to_fact()?)
                    .map_err(|_| Error::Mismatch)?,
                _ => return Err(Error::Mismatch),
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{Engine, EngineLimits};
    use crate::rows::{RowLimits, Rows};

    fn engine(path: &Path) -> Engine {
        Engine::open(
            &path.canonicalize().unwrap(),
            EngineLimits {
                memory_bytes: 128 * 1024 * 1024,
                temp_bytes: 64 * 1024 * 1024,
                threads: 1,
            },
        )
        .unwrap()
    }
    fn limits() -> RowLimits {
        RowLimits {
            max_rows: 1024,
            max_text_bytes: 4 * 1024 * 1024,
            max_tag_nodes: 100_000,
        }
    }
    use bigbrotr_model::{
        Fact, FactPayload, MeasurementMoment, MeasurementState, ObservedTime,
        codec::FactDecodeLimits,
    };

    fn frozen() -> Vec<Fact> {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../crates/bigbrotr-model/tests/fixtures/measurement-v1.json"
        ))
        .unwrap();
        fixture["valid"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| {
                let bytes: Vec<_> = row["preimage_hex"]
                    .as_str()
                    .unwrap()
                    .as_bytes()
                    .as_chunks::<2>()
                    .0
                    .iter()
                    .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap())
                    .collect();
                Fact::decode_preimage(
                    &bytes,
                    row["sha256"].as_str().unwrap().parse().unwrap(),
                    FactDecodeLimits::default(),
                )
                .unwrap()
            })
            .collect()
    }
    fn append(c: &Connection, facts: &[Fact]) {
        Rows::measurements(&facts.iter().collect::<Vec<_>>(), limits())
            .unwrap()
            .append(&mut c.appender_to_db("measurement_facts", "stage").unwrap())
            .unwrap();
    }

    #[test]
    fn literal_compound_states_survive_native_parquet_close_and_fresh_typed_hydration() {
        let facts = frozen();
        let writer_temp = tempfile::tempdir().unwrap();
        let writer = engine(writer_temp.path());
        crate::schema::create_stage(&writer.connection).unwrap();
        append(&writer.connection, &facts);
        let artifact = tempfile::tempdir().unwrap();
        let file = artifact.path().join("measurement.parquet");
        writer
            .connection
            .execute(
                "COPY stage.measurement_facts TO ? (FORMAT PARQUET)",
                [file.to_str().unwrap()],
            )
            .unwrap();
        drop(writer);
        let reader_temp = tempfile::tempdir().unwrap();
        let reader = engine(reader_temp.path());
        reader
            .connection
            .execute_batch(&format!(
                "CREATE VIEW cut_measurement_facts AS SELECT * FROM read_parquet('{}')",
                file.display()
            ))
            .unwrap();
        for expected in &facts {
            let actual = read_one(
                &reader.connection,
                "cut_measurement_facts",
                expected.fact_ref(),
            )
            .unwrap()
            .to_fact()
            .unwrap();
            // Each expected digest and canonical preimage is from the maintained
            // independent literal author, not a native or projection encoder.
            assert_eq!(
                actual.encode_preimage(8192).unwrap(),
                expected.encode_preimage(8192).unwrap()
            );
        }
        let counts:String=reader.connection.query_row("SELECT string_agg(state_kind::VARCHAR,',' ORDER BY state_kind) FROM (SELECT DISTINCT state_kind FROM cut_measurement_facts)",[],|r|r.get(0)).unwrap();
        assert_eq!(counts, "0,1,2,3");
        // Null parent masking must not become a fabricated all-zero child.
        let missing:(bool,bool,bool,bool)=reader.connection.query_row("SELECT observed IS NULL,ended IS NULL,termination IS NULL,teardown IS NULL FROM cut_measurement_facts WHERE state_kind=0",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).unwrap();
        assert_eq!(missing, (true, true, true, true));
    }

    #[test]
    fn known_null_samples_zero_and_signed_extrema_stay_distinct() {
        let base = frozen().remove(2);
        let FactPayload::Measurement(value) = base.payload() else {
            unreachable!()
        };
        let MeasurementState::Terminal(mut terminal) = value.state() else {
            unreachable!()
        };
        terminal.observed.read.request_flushed = Some(MeasurementMoment {
            observed_at: None,
            elapsed_us: None,
        });
        terminal.ended.observed_at = Some(ObservedTime::new(i64::MIN));
        let changed = Fact::new(
            base.source_record_id(),
            base.visibility_scope_id(),
            ObservedTime::new(i64::MAX),
            FactPayload::Measurement(
                bigbrotr_model::Measurement::new(
                    value.attempt_id(),
                    value.context(),
                    value.profile(),
                    value.clock_quality(),
                    MeasurementState::Terminal(terminal),
                )
                .unwrap(),
            ),
        )
        .unwrap();
        let temp = tempfile::tempdir().unwrap();
        let writer = engine(temp.path());
        crate::schema::create_stage(&writer.connection).unwrap();
        append(&writer.connection, std::slice::from_ref(&changed));
        let file = temp.path().join("presence.parquet");
        writer
            .connection
            .execute(
                "COPY stage.measurement_facts TO ? (FORMAT PARQUET)",
                [file.to_str().unwrap()],
            )
            .unwrap();
        drop(writer);
        let reader_temp = tempfile::tempdir().unwrap();
        let reader = engine(reader_temp.path());
        reader
            .connection
            .execute_batch(&format!(
                "CREATE VIEW cut_measurement_facts AS SELECT * FROM read_parquet('{}')",
                file.display()
            ))
            .unwrap();
        let row = read_one(
            &reader.connection,
            "cut_measurement_facts",
            changed.fact_ref(),
        )
        .unwrap();
        assert_eq!(row.recorded_at.get(), i64::MAX);
        assert_eq!(
            row.payload.ended.unwrap().observed_at.unwrap().get(),
            i64::MIN
        );
        let request = row.payload.observed.unwrap().read.request_flushed.unwrap();
        assert!(request.observed_at.is_none() && request.elapsed_us.is_none());
    }

    #[test]
    fn exact_source_limit_two_refuses_duplicates_and_changed_actual_payload() {
        let facts = frozen();
        let fact = &facts[2];
        let temp = tempfile::tempdir().unwrap();
        let writer = engine(temp.path());
        crate::schema::create_stage(&writer.connection).unwrap();
        append(&writer.connection, std::slice::from_ref(fact));
        writer
            .connection
            .execute_batch(
                "CREATE VIEW cut_measurement_facts AS SELECT * FROM stage.measurement_facts",
            )
            .unwrap();
        assert_eq!(
            read_one(
                &writer.connection,
                "cut_measurement_facts",
                facts[0].fact_ref()
            )
            .unwrap_err(),
            Error::Missing
        );
        let mut bad = fact.fact_ref();
        bad.payload_digest = bigbrotr_model::Digest256::from_bytes([0; 32]);
        assert_eq!(
            read_one(&writer.connection, "cut_measurement_facts", bad).unwrap_err(),
            Error::Mismatch
        );
        writer.connection.execute_batch("UPDATE stage.measurement_facts SET observed=struct_update(observed,counters:=struct_update(observed.counters,rx_ws_bytes:=observed.counters.rx_ws_bytes+1))").unwrap();
        assert_eq!(
            read_one(&writer.connection, "cut_measurement_facts", fact.fact_ref()).unwrap_err(),
            Error::Mismatch
        );
        writer
            .connection
            .execute_batch("DELETE FROM stage.measurement_facts")
            .unwrap();
        append(&writer.connection, std::slice::from_ref(fact));
        // A conflicting digest cannot be hidden by selecting WHERE digest=expected.
        writer.connection.execute_batch("INSERT INTO stage.measurement_facts SELECT * REPLACE (from_hex(repeat('ff',32)) AS payload_digest) FROM stage.measurement_facts").unwrap();
        assert_eq!(
            read_one(&writer.connection, "cut_measurement_facts", fact.fact_ref()).unwrap_err(),
            Error::Mismatch
        );
    }
}
