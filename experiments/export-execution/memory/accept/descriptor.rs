//! Exact setup input; no job ID or purported native/SQL authority is supplied.
use super::*;
use crate::memory::descriptor::{Document, Limits, bounded};
use serde::Deserialize;

pub(super) const EVENT_ID: &str =
    "05e909d2c260edbb52e21e3813a492ade70523c8493fb0ae5a9a37a04354db31";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Case {
    pub format: String,
    pub name: String,
    pub dataset: DatasetId,
    pub instance: DatasetId,
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
    pub username: String,
    pub password_ref: String,
    pub command_key: CommandKey,
    pub command: ExportCommand,
}
impl Case {
    pub fn read(path: &Path) -> Result<Self> {
        let value: Self = serde_json::from_slice(&bounded(path, 64 * 1024)?).map_err(error)?;
        if value.format != "bigbrotr-export-accept-setup/1"
            || value.name.is_empty()
            || value.name.len() > 64
            || !value
                .name
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'-')
            || value.worker_bytes == 0
            || value.limits.row_bytes != MIB as usize
            || value
                .command
                .validate(value.dataset)
                .map_err(error)?
                .to_string()
                != EVENT_ID
        {
            return Err("invalid acceptance setup descriptor".into());
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
                return Err("setup paths must be absolute".into());
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
