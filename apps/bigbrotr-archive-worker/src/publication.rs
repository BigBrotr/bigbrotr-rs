//! One finite, closed native publication. The caller owns fresh guarded lifetime
//! authority, retained source admission and all ambiguous-outcome recovery.
use crate::{catalog::BootstrapRequest, prepare::PreparedBatch, schema};
use bigbrotr_model::{ArchiveBatch, Digest256, TopologyGeneration};
use duckdb::{Connection, params};
use rustix::fs::{Mode, OFlags, open};
use serde::Deserialize;
use std::{
    fmt,
    fs::{self, File, OpenOptions},
    io::Read,
    os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt},
    path::Path,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublicationError {
    InvalidInput,
    Identity,
    Configuration,
    ExistingData,
    MissingDictionary,
    ConflictingDictionary,
    PreparedFiles,
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
        let first = self.batch.first();
        // Private receipt staging precedes the archive transaction. DuckDB may
        // not modify two attached databases in one transaction. This in-memory
        // row is neither visible catalog state nor an acknowledgement.
        c.execute("INSERT INTO stage.archive_commits VALUES (?,1,?,?,?,[struct_pack(shard_id := ?::UINTEGER, owner_epoch := ?::UBIGINT, first_sequence := ?::UBIGINT, last_sequence := ?::UBIGINT)],?,?)",
            params![first.dataset_id.as_bytes().as_slice(), request.topology_generation.get(), request.batch_digest.as_bytes().as_slice(), self.batch.records().len() as u64, first.shard_id, first.owner_epoch.get(), first.sequence.get(), self.batch.last().sequence.get(), request.attempt_id.as_slice(), request.publication_ordinal])
            .map_err(|_| PublicationError::Native)?;
        c.execute_batch("BEGIN TRANSACTION")
            .map_err(|_| PublicationError::Native)?;
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
        let counts = self.counts().map_err(|_| PublicationError::Native)?;
        for (table, count) in schema::TABLES
            .into_iter()
            .zip(counts)
            .chain([("archive_commits", 1)])
        {
            if count != 0 {
                prepare_and_register(c, request, table)?;
            }
        }
        c.execute_batch("COMMIT")
            .map_err(|_| PublicationError::CommitUnknown)?;
        Ok(request.batch_digest)
    }
}

fn prepare_and_register(
    c: &Connection,
    request: &PublicationRequest,
    table: &str,
) -> Result<(), PublicationError> {
    use fmt::Write;
    let mut attempt = String::with_capacity(32);
    for byte in request.attempt_id {
        write!(attempt, "{byte:02x}").expect("String write");
    }
    // The scientific guard binds exactly these schema/table paths. Native path
    // relativization is lexical, so canonicalize the actual parent first. The
    // installation remains an operator-owned boundary against privileged races.
    let root = Path::new(request.catalog.data_root());
    // Retain every parent before creating its child, including existing parents:
    // an earlier interrupted attempt may not have synchronized their entries.
    let root_handle = open_directory(root)?;
    let main = root.join("main");
    let main_handle = create_directory(&main)?;
    let table_root = main.join(table);
    let table_handle = create_directory(&table_root)?;
    let directory = table_root.join(format!("prepared-{attempt}"));
    // Exclusive creation refuses reuse after partial/ambiguous work. Application
    // error handling retains completed files, including after known rollback;
    // a failed native COPY may remove its own incomplete output before returning.
    // Positive fencing and fresh receipt/file closure own later orphan cleanup.
    fs::DirBuilder::new()
        .mode(0o700)
        .create(&directory)
        .map_err(|_| PublicationError::PreparedFiles)?;
    let directory_handle = open_directory(&directory)?;
    let file = directory.join("data.parquet");
    // Keep the original descriptor from before native writes. Reopening only
    // after COPY closes its writer can miss an already-observed writeback error.
    let held = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&file)
        .map_err(|_| PublicationError::PreparedFiles)?;
    let text = file.to_str().ok_or(PublicationError::PreparedFiles)?;
    let literal = format!("'{}'", text.replace('\'', "''"));
    // The native default replaces an existing destination with a temporary
    // inode. Direct output preserves the exclusively created caller-owned file.
    c.execute_batch(&format!("COPY (SELECT * FROM stage.{table}) TO {literal} (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, ROW_GROUP_SIZE 64000, USE_TMP_FILE false)"))
        .map_err(|_| PublicationError::Native)?;
    require_same_file(&held, &file)?;
    held.sync_all()
        .map_err(|_| PublicationError::PreparedFiles)?;
    for parent in [directory_handle, table_handle, main_handle, root_handle] {
        parent
            .sync_all()
            .map_err(|_| PublicationError::PreparedFiles)?;
    }
    // Both scientific and receipt files are registered as caller-owned. Normal
    // INSERT's native error cleanup can delete files after a lost PG COMMIT reply.
    c.execute_batch(&format!("CALL ducklake_add_data_files('archive','{table}',[{literal}],schema=>'main',hive_partitioning=>false,allow_missing=>false,ignore_extra_columns=>false)"))
        .map_err(|_| PublicationError::Native)
}

fn open_directory(path: &Path) -> Result<File, PublicationError> {
    if path.canonicalize().ok().as_deref() != Some(path) {
        return Err(PublicationError::PreparedFiles);
    }
    open(
        path,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map(File::from)
    .map_err(|_| PublicationError::PreparedFiles)
}

fn create_directory(path: &Path) -> Result<File, PublicationError> {
    match fs::create_dir(path) {
        Ok(()) => (),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => (),
        Err(_) => return Err(PublicationError::PreparedFiles),
    }
    // Inspect before any child creation, including when the parent existed.
    open_directory(path)
}

fn require_same_file(held: &File, path: &Path) -> Result<(), PublicationError> {
    let original = held
        .metadata()
        .map_err(|_| PublicationError::PreparedFiles)?;
    let named = fs::symlink_metadata(path).map_err(|_| PublicationError::PreparedFiles)?;
    if !original.is_file()
        || !named.is_file()
        || original.nlink() != 1
        || named.nlink() != 1
        || original.len() == 0
        || (original.dev(), original.ino()) != (named.dev(), named.ino())
    {
        return Err(PublicationError::PreparedFiles);
    }
    Ok(())
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
