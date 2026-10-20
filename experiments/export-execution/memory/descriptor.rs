//! Trusted finite fixture inputs, not a product configuration or SQL authority.
use super::*;
use serde::Deserialize;
use std::io::Read;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Connection {
    pub endpoint: String,
    pub database: String,
    pub user: String,
    pub password_ref: String,
}
impl Connection {
    pub fn settings(&self) -> Result<ConnectionSettings<'_>> {
        Ok(ConnectionSettings {
            endpoint: &self.endpoint,
            database: &self.database,
            user: &self.user,
            password: SecretFile::parse(&self.password_ref).map_err(error)?,
            connect_timeout: Duration::from_secs(5),
            tls: TlsSettings::LocalPlaintext {
                boundary: PlaintextBoundary::Loopback,
            },
        })
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Document {
    pub path: PathBuf,
    pub sha256: Digest256,
    pub stored_digest: Digest256,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Limits {
    pub memory_max_bytes: u64,
    pub baseline_bytes: u64,
    pub data_bytes: u64,
    pub native_bytes: u64,
    pub control_bytes: u64,
    pub engine_memory_bytes: u64,
    pub native_reserve_bytes: u64,
    pub row_bytes: usize,
    pub scan_bytes: u64,
    pub temp_bytes: u64,
    pub export_bytes: u64,
    pub disk_reserve_bytes: u64,
    pub query_ms: u64,
}
impl Limits {
    pub fn budget(&self) -> Result<ResourceBudget> {
        let overlap = (self.row_bytes as u64)
            .checked_add(2048)
            .and_then(|n| n.checked_mul(64))
            .ok_or("row arithmetic")?;
        if self.row_bytes == 0
            || self.row_bytes > 4 * MIB as usize
            || self.scan_bytes == 0
            || self.scan_bytes > 256 * MIB
            || self.engine_memory_bytes == 0
            || self.temp_bytes == 0
            || self.native_reserve_bytes < 16 * MIB + overlap
            || !(1..=4000).contains(&self.query_ms)
            || self.export_bytes == 0
            || self.disk_reserve_bytes == 0
        {
            return Err("finite limits outside the current read API".into());
        }
        self.export_limits()?;
        Ok(ResourceBudget::new(
            ProcessEnvelope::new(
                self.memory_max_bytes,
                self.baseline_bytes,
                ResourceLimits::new(self.data_bytes, 16, self.native_bytes, self.control_bytes),
                WaiterLimits::new(0, 0),
            )
            .map_err(error)?,
        ))
    }
    pub fn export_limits(&self) -> Result<ExportLimits> {
        ExportLimits::new(
            self.export_bytes,
            self.export_bytes
                .checked_mul(2)
                .ok_or("export arithmetic")?,
        )
        .map_err(error)
    }
    pub fn preparation(&self) -> PreparationLimits {
        PreparationLimits {
            memory_bytes: self.engine_memory_bytes,
            temp_bytes: self.temp_bytes,
            threads: 1,
            max_records: 2,
            max_encoded_bytes: self.row_bytes,
            native_reserve_bytes: self.native_reserve_bytes,
        }
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Case {
    pub format: String,
    pub name: String,
    pub dataset: DatasetId,
    pub instance: DatasetId,
    pub job: JobId,
    pub event_id: bigbrotr_model::EventId,
    pub control: Connection,
    pub catalog: Connection,
    pub reader_user: String,
    pub reader_password_ref: String,
    pub config: Document,
    pub policy: Document,
    pub data_root: String,
    pub state_root: PathBuf,
    pub destination: PathBuf,
    pub scratch: PathBuf,
    pub ledger: PathBuf,
    pub worker_root: PathBuf,
    pub worker_bytes: u64,
    pub worker_sha256: Digest256,
    pub limits: Limits,
}
impl Case {
    pub fn read(path: &Path) -> Result<Self> {
        let value: Self = serde_json::from_slice(&bounded(path, 64 * 1024)?).map_err(error)?;
        if value.format != "bigbrotr-export-measure/1"
            || value.name.is_empty()
            || value.name.len() > 64
            || !value
                .name
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'-')
            || value.worker_bytes == 0
        {
            return Err("invalid measurement descriptor".into());
        }
        for path in [
            &value.state_root,
            &value.destination,
            &value.scratch,
            &value.ledger,
            &value.worker_root,
            &value.config.path,
            &value.policy.path,
        ] {
            if !path.is_absolute() {
                return Err("fixture paths must be absolute".into());
            }
        }
        Ok(value)
    }
    pub fn installation(&self) -> WorkerInstallation {
        WorkerInstallation {
            root: self.worker_root.clone(),
            executable: ArtifactIdentity {
                bytes: self.worker_bytes,
                sha256: self.worker_sha256,
            },
        }
    }
}
pub(super) fn bounded(path: &Path, maximum: usize) -> Result<Vec<u8>> {
    let file = fs::File::open(path).map_err(error)?;
    if !file.metadata().map_err(error)?.is_file() {
        return Err("expected regular fixture file".into());
    }
    let mut bytes = Vec::new();
    file.take(maximum as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(error)?;
    if bytes.len() > maximum {
        return Err("fixture file exceeds byte bound".into());
    }
    Ok(bytes)
}
pub(super) struct Documents(Vec<(serde_json::Value, [u8; 32])>);
impl Documents {
    pub fn read(case: &Case) -> Result<Self> {
        Self::read_pair(&case.config, &case.policy)
    }
    pub fn read_pair(config: &Document, policy: &Document) -> Result<Self> {
        let mut docs = Vec::new();
        for input in [config, policy] {
            let bytes = bounded(&input.path, 64 * 1024)?;
            if Digest256::from_bytes(Sha256::digest(&bytes).into()) != input.sha256 {
                return Err("trusted policy fixture hash differs".into());
            }
            docs.push((
                serde_json::from_slice(&bytes).map_err(error)?,
                *input.stored_digest.as_bytes(),
            ));
        }
        Ok(Self(docs))
    }
    pub fn validate(&self, text: &str, digest: [u8; 32]) -> std::result::Result<(), OwnerError> {
        if text.len() > 64 * 1024 {
            return Err(OwnerError::InvalidPolicy);
        }
        let value: serde_json::Value =
            serde_json::from_str(text).map_err(|_| OwnerError::InvalidPolicy)?;
        if self
            .0
            .iter()
            .any(|(expected, hash)| *expected == value && *hash == digest)
        {
            Ok(())
        } else {
            Err(OwnerError::InvalidPolicy)
        }
    }
}
