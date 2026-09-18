//! One finite, closed native publication. The caller owns fresh guarded lifetime
//! authority, retained source admission and all ambiguous-outcome recovery.
use crate::{catalog::BootstrapRequest, prepare::PreparedBatch, schema};
use bigbrotr_model::{ArchiveBatch, Digest256, TopologyGeneration};
use duckdb::{Connection, params};
use serde::Deserialize;
use std::{fmt, io::Read};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublicationError {
    InvalidInput,
    Identity,
    Configuration,
    ExistingData,
    MissingDictionary,
    ConflictingDictionary,
    Native,
    CommitUnknown,
}
impl fmt::Display for PublicationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "private archive publication failed: {self:?}")
    }
}
impl std::error::Error for PublicationError {}

// No Debug/Serialize: this private value contains a credential-bearing DSN.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicationRequest {
    catalog: BootstrapRequest,
    topology_generation: TopologyGeneration,
    batch_digest: Digest256,
    attempt_id: [u8; 16],
    publication_ordinal: u64,
}
impl PublicationRequest {
    /// A u32 big-endian byte length precedes closed JSON (at most32KiB), then
    /// exactly one existing admission frame. Never consume that following frame.
    pub fn read(input: &mut impl Read) -> Result<Self, PublicationError> {
        let mut length = [0; 4];
        input
            .read_exact(&mut length)
            .map_err(|_| PublicationError::InvalidInput)?;
        let length = u32::from_be_bytes(length) as usize;
        if length == 0 || length > 32768 {
            return Err(PublicationError::InvalidInput);
        }
        let mut bytes = vec![0; length];
        input
            .read_exact(&mut bytes)
            .map_err(|_| PublicationError::InvalidInput)?;
        let value: Self =
            serde_json::from_slice(&bytes).map_err(|_| PublicationError::InvalidInput)?;
        value
            .catalog
            .validate()
            .map_err(|_| PublicationError::InvalidInput)?;
        if value.attempt_id == [0; 16] || value.publication_ordinal == 0 {
            return Err(PublicationError::InvalidInput);
        }
        Ok(value)
    }
}

impl PreparedBatch {
    /// Consume only completely decoded/staged input. No success or failure here
    /// grants protection, source ACK, reclamation or permission to retry.
    pub fn publish(self, request: PublicationRequest) -> Result<Digest256, PublicationError> {
        self.validate_publication(&request)?;
        self.engine
            .attach_existing_catalog(&request.catalog)
            .map_err(|_| PublicationError::Configuration)?;
        self.publish_attached(&request)
        // Engine destruction closes native clients before the finite response.
    }

    fn validate_publication(&self, request: &PublicationRequest) -> Result<(), PublicationError> {
        let archive = ArchiveBatch::new(
            1,
            request.topology_generation,
            std::slice::from_ref(&self.batch),
        )
        .map_err(|_| PublicationError::Identity)?;
        if archive.digest() != request.batch_digest {
            return Err(PublicationError::Identity);
        }
        Ok(())
    }

    fn publish_attached(
        &self,
        request: &PublicationRequest,
    ) -> Result<Digest256, PublicationError> {
        self.validate_publication(request)?;
        let c = &self.engine.connection;
        c.execute_batch("BEGIN TRANSACTION")
            .map_err(|_| PublicationError::Native)?;
        let first = self.batch.first();
        let exists: bool = c.query_row("SELECT EXISTS(SELECT 1 FROM archive.main.archive_commits WHERE batch_digest=? OR attempt_id=? OR publication_ordinal=?)",
            params![request.batch_digest.as_bytes().as_slice(), request.attempt_id.as_slice(), request.publication_ordinal], |r|r.get(0)).map_err(|_| PublicationError::Native)?;
        if exists {
            return Err(PublicationError::ExistingData);
        }
        for table in schema::TABLES {
            let exists: bool = c.query_row(&format!("SELECT EXISTS(SELECT 1 FROM archive.main.{table} WHERE source_record_id.dataset_id=? AND source_record_id.shard_id=? AND source_record_id.owner_epoch=? AND source_record_id.sequence BETWEEN ? AND ?)"),
                params![first.dataset_id.as_bytes().as_slice(), first.shard_id, first.owner_epoch.get(), first.sequence.get(), self.batch.last().sequence.get()], |r|r.get(0)).map_err(|_| PublicationError::Native)?;
            if exists {
                return Err(PublicationError::ExistingData);
            }
        }
        dictionary_closure(c)?;
        // These are native catalog options, part of this same fenced transaction.
        // The pinned implementation updates its in-memory options immediately.
        c.execute_batch("CALL ducklake_set_option('archive','parquet_compression','zstd'); CALL ducklake_set_option('archive','parquet_compression_level',3); CALL ducklake_set_option('archive','parquet_row_group_size',64000); CALL ducklake_set_option('archive','target_file_size','256MiB')").map_err(|_| PublicationError::Native)?;
        for table in schema::TABLES {
            c.execute_batch(&format!(
                "INSERT INTO archive.main.{table} SELECT * FROM stage.{table}"
            ))
            .map_err(|_| PublicationError::Native)?;
        }
        // A single complete admission currently has one canonical range. Scalar
        // parameters build the nested native value; no interpolated input SQL.
        c.execute("INSERT INTO archive.main.archive_commits VALUES (?,1,?,?,?,[struct_pack(shard_id := ?::UINTEGER, owner_epoch := ?::UBIGINT, first_sequence := ?::UBIGINT, last_sequence := ?::UBIGINT)],?,?)",
            params![first.dataset_id.as_bytes().as_slice(), request.topology_generation.get(), request.batch_digest.as_bytes().as_slice(), self.batch.records().len() as u64, first.shard_id, first.owner_epoch.get(), first.sequence.get(), self.batch.last().sequence.get(), request.attempt_id.as_slice(), request.publication_ordinal])
            .map_err(|_| PublicationError::Native)?;
        c.execute_batch("COMMIT")
            .map_err(|_| PublicationError::CommitUnknown)?;
        Ok(request.batch_digest)
    }
}

fn dictionary_closure(c: &Connection) -> Result<(), PublicationError> {
    // Restrict historical work to references required/defined by this bounded
    // stage. Equal mappings may recur as distinct immutable source facts; a
    // changed descriptor for the same qualified name is refused. Fact visibility
    // remains in its envelope and is not part of dictionary mapping equality.
    c.execute_batch("CREATE TEMP VIEW publication_dictionaries AS SELECT reference,normalization_version,descriptor_schema,descriptor_version,descriptor_value FROM stage.dictionaries UNION ALL SELECT reference,normalization_version,descriptor_schema,descriptor_version,descriptor_value FROM archive.main.dictionaries WHERE reference IN (SELECT struct_pack(dataset_id := dataset_id,dictionary_kind := dictionary_kind,allocation_domain := allocation_domain,ordinal := ordinal) FROM stage.required_refs UNION SELECT reference FROM stage.dictionaries)").map_err(|_| PublicationError::Native)?;
    let missing: bool = c.query_row("SELECT EXISTS(SELECT 1 FROM stage.required_refs r WHERE NOT EXISTS(SELECT 1 FROM publication_dictionaries d WHERE d.reference=struct_pack(dataset_id := r.dataset_id,dictionary_kind := r.dictionary_kind,allocation_domain := r.allocation_domain,ordinal := r.ordinal)))", [], |r|r.get(0)).map_err(|_| PublicationError::Native)?;
    if missing {
        return Err(PublicationError::MissingDictionary);
    }
    let conflicting: bool = c.query_row("SELECT EXISTS(SELECT 1 FROM (SELECT DISTINCT * FROM publication_dictionaries) GROUP BY reference HAVING count(*)>1)", [], |r|r.get(0)).map_err(|_| PublicationError::Native)?;
    if conflicting {
        return Err(PublicationError::ConflictingDictionary);
    }
    Ok(())
}

#[cfg(test)]
#[path = "publication_tests.rs"]
mod tests;
