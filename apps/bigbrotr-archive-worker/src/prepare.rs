//! Private, discardable native staging. No catalog mutation or durable receipt.
use crate::engine::Engine;
use crate::rows::{RowError, RowLimits, Rows};
use bigbrotr_model::{AdmissionBatch, FactPayload};
use bigbrotr_protocol::archive_batch::{self, Limits};
use std::{fmt, io::Read, sync::atomic::AtomicBool};

use crate::schema;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrepareError {
    Transfer(archive_batch::Error),
    Rows(RowError),
    NativeStage,
}
impl fmt::Display for PrepareError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "private archive preparation failed: {self:?}")
    }
}
impl std::error::Error for PrepareError {}
impl From<archive_batch::Error> for PrepareError {
    fn from(e: archive_batch::Error) -> Self {
        Self::Transfer(e)
    }
}
impl From<RowError> for PrepareError {
    fn from(e: RowError) -> Self {
        Self::Rows(e)
    }
}

pub struct PreparedBatch {
    engine: Engine,
    batch: AdmissionBatch,
}
impl PreparedBatch {
    /// Consumes the engine. Failure drops all partial private staging; even a
    /// successful result is only prepared input for a future fenced publisher.
    /// One fact and one Arrow batch coexist; the engine owns aggregate staging
    /// under its separately granted memory/spill limits. No source ACK is issued.
    pub fn prepare(
        engine: Engine,
        input: impl Read,
        limits: Limits,
        cancelled: &AtomicBool,
    ) -> Result<Self, PrepareError> {
        let rows = RowLimits {
            max_rows: 1,
            max_text_bytes: limits.fact.max_encoded_bytes,
            max_tag_nodes: limits.fact.max_tag_nodes,
        };
        if rows.max_text_bytes > i32::MAX as usize || rows.max_tag_nodes > i32::MAX as usize {
            return Err(archive_batch::Error::InvalidLimits.into());
        }
        let mut input = archive_batch::BatchReader::new(input, limits, cancelled)?;
        schema::create_stage(&engine.connection).map_err(|_| PrepareError::NativeStage)?;
        while let Some(fact) = input.next_fact(cancelled)? {
            let facts = [&fact];
            let (name, converted) = match fact.payload() {
                FactPayload::Dictionary(_) => ("dictionaries", Rows::dictionaries(&facts, rows)),
                FactPayload::EventPayload(_) => ("events", Rows::events(&facts, rows)),
                FactPayload::EventEligibility(_) => {
                    ("event_eligibility_facts", Rows::eligibility(&facts, rows))
                }
                FactPayload::EventReceive(_) => {
                    ("event_receive_facts", Rows::receive(&facts, rows))
                }
                FactPayload::Provenance(_) => ("provenance_facts", Rows::provenance(&facts, rows)),
            };
            converted?.append(
                &mut engine
                    .connection
                    .appender_to_db(name, "stage")
                    .map_err(|_| PrepareError::NativeStage)?,
            )?;
        }
        let batch = input.finish(cancelled)?;
        Ok(Self { engine, batch })
    }
    pub fn batch(&self) -> &AdmissionBatch {
        &self.batch
    }
    /// Read actual native relation counts for bounded diagnostics. This is not
    /// content verification, a catalog receipt or publication authority.
    pub fn counts(&self) -> Result<[u64; 5], PrepareError> {
        let mut counts = [0; 5];
        for (i, table) in schema::TABLES.iter().enumerate() {
            counts[i] = self
                .engine
                .connection
                .query_row(&format!("SELECT count(*) FROM stage.{table}"), [], |r| {
                    r.get(0)
                })
                .map_err(|_| PrepareError::NativeStage)?;
        }
        Ok(counts)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::EngineLimits;
    use bigbrotr_model::codec::FactDecodeLimits;
    fn input() -> Vec<u8> {
        let v: serde_json::Value = serde_json::from_str(include_str!(
            "../../../crates/bigbrotr-protocol/tests/fixtures/archive/single.json"
        ))
        .unwrap();
        v["wire_hex"]
            .as_str()
            .unwrap()
            .as_bytes()
            .as_chunks::<2>()
            .0
            .iter()
            .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap())
            .collect()
    }
    fn limits() -> Limits {
        Limits {
            max_records: 16,
            max_encoded_bytes: 1024 * 1024,
            fact: FactDecodeLimits::default(),
        }
    }
    fn engine(path: &std::path::Path) -> Engine {
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
    #[test]
    fn canonical_handoff_reaches_actual_native_staging_with_unsigned_and_nested_values() {
        let temp = tempfile::tempdir().unwrap();
        let prepared = PreparedBatch::prepare(
            engine(temp.path()),
            input().as_slice(),
            limits(),
            &AtomicBool::new(false),
        )
        .unwrap();
        assert_eq!(prepared.batch().first().sequence.get(), 2);
        let row=prepared.engine.connection.query_row("SELECT created_at, kind, tags::VARCHAR, content, hex(event_id), source_record_id.sequence FROM stage.events", [],|r| Ok((r.get::<_,u64>(0)?,r.get::<_,u16>(1)?,r.get::<_,String>(2)?,r.get::<_,String>(3)?,r.get::<_,String>(4)?,r.get::<_,u64>(5)?))).unwrap();
        assert_eq!(
            row,
            (
                u64::MAX,
                1,
                "[[x], [x, ''], [x, '']]".into(),
                "é\n".into(),
                "AA".repeat(32),
                2
            )
        );
        assert_eq!(
            prepared
                .engine
                .connection
                .query_row(
                    "SELECT count(*) FROM stage.event_eligibility_facts",
                    [],
                    |r| r.get::<_, u64>(0)
                )
                .unwrap(),
            0
        );
    }
    #[test]
    fn malformed_tail_cannot_return_a_prepared_batch() {
        let temp = tempfile::tempdir().unwrap();
        let mut input = input();
        input.push(0);
        assert!(
            PreparedBatch::prepare(
                engine(temp.path()),
                input.as_slice(),
                limits(),
                &AtomicBool::new(false)
            )
            .is_err()
        );
    }
    #[test]
    fn every_family_uses_its_typed_staging_relation() {
        use bigbrotr_model::{Fact, Sequence};
        let mut facts = Vec::new();
        for file in [
            include_str!("../../../crates/bigbrotr-model/tests/fixtures/dictionary-v1.json"),
            include_str!("../../../tests/fixtures/model/core-fingerprints.json"),
        ] {
            let values: serde_json::Value = serde_json::from_str(file).unwrap();
            for value in values["vectors"].as_array().unwrap() {
                let bytes: Vec<_> = value["preimage_hex"]
                    .as_str()
                    .unwrap()
                    .as_bytes()
                    .as_chunks::<2>()
                    .0
                    .iter()
                    .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap())
                    .collect();
                if !bytes.starts_with(b"bigbrotr/fact/1\0") {
                    continue;
                }
                let fact = Fact::decode_preimage(
                    &bytes,
                    value["sha256"].as_str().unwrap().parse().unwrap(),
                    FactDecodeLimits::default(),
                )
                .unwrap();
                // Rebind only sequence to construct one synthetic contiguous
                // preparation batch; this is not a valid allocated dataset.
                let mut source = fact.source_record_id();
                source.sequence = Sequence::new(facts.len() as u64 + 1).unwrap();
                facts.push(
                    Fact::new(
                        source,
                        fact.visibility_scope_id(),
                        fact.recorded_at(),
                        fact.payload().clone(),
                    )
                    .unwrap(),
                );
            }
        }
        assert_eq!(facts.len(), 14);
        let batch = AdmissionBatch::new(facts.iter().map(Fact::fact_ref).collect()).unwrap();
        let records: Vec<_> = facts
            .iter()
            .map(|f| f.encode_preimage(8192).unwrap())
            .collect();
        let mut wire = Vec::new();
        let cancel = AtomicBool::new(false);
        archive_batch::write_batch(&batch, &records, &mut wire, limits(), &cancel).unwrap();
        let temp = tempfile::tempdir().unwrap();
        let prepared =
            PreparedBatch::prepare(engine(temp.path()), wire.as_slice(), limits(), &cancel)
                .unwrap();
        assert_eq!(prepared.batch(), &batch);
        assert_eq!(prepared.counts().unwrap(), [10, 1, 1, 1, 1]);
        let ordinal: u64=prepared.engine.connection.query_row("SELECT descriptor_value.method_id.ordinal FROM stage.dictionaries WHERE descriptor_schema='observer' AND descriptor_value.method_id IS NOT NULL",[],|r|r.get(0)).unwrap();
        assert_eq!(ordinal, 1);
        let row:(String,u16,u64)=prepared.engine.connection.query_row("SELECT origin, import_lineage.dictionary_kind, import_lineage.ordinal FROM stage.event_eligibility_facts",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).unwrap();
        assert_eq!(row, ("import".into(), 9, 1));
    }
}
