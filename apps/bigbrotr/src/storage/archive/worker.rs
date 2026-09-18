use super::{InstallationError, ProcessError, WorkerInstallation};
use crate::{
    runtime::{ResourceBudget, ResourceRequest},
    storage::rocks::store::PendingBatch,
};
use bigbrotr_model::{
    ArchiveBatch, Digest256, SourceRecordId, TopologyGeneration, codec::FactDecodeLimits,
};
use bigbrotr_protocol::archive_batch::Limits;
use std::{
    fs,
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::Path,
    sync::atomic::AtomicBool,
    time::Instant,
};

#[derive(Debug, Clone, Copy)]
pub struct PreparationLimits {
    pub memory_bytes: u64,
    pub temp_bytes: u64,
    pub threads: u16,
    pub max_records: usize,
    pub max_encoded_bytes: usize,
    /// Complete child allowance beyond configured engine memory, including
    /// decoded/Arrow values, signed startup, native stacks and allocator cost.
    pub native_reserve_bytes: u64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreparationError {
    InvalidLimits,
    InvalidTemporaryRoot,
    Unavailable,
    Installation(InstallationError),
    Process(ProcessError),
    Transfer(bigbrotr_protocol::archive_batch::Error),
    InvalidResult,
}
#[derive(Debug)]
pub struct PreparationInspection {
    pub staged_counts: [u64; 5],
    /// Schema-1 semantic input identity, never a durable publication receipt.
    pub archive_batch_digest: Digest256,
}

pub struct Preparation<'a> {
    pub installation: &'a WorkerInstallation,
    pub temp_root: &'a Path,
    pub limits: PreparationLimits,
    pub deadline: Instant,
    pub cancelled: &'a AtomicBool,
}
impl Preparation<'_> {
    /// Private discardable staging only. No archive acknowledgement or reclaim.
    /// The caller owns the private temporary root and filesystem reservation.
    /// TerminationUnknown forbids root cleanup/retry until positive exclusion.
    /// Topology is supplied explicitly: the store's creation manifest cannot
    /// establish the publisher's current topology or grant publication authority.
    pub fn inspect_pending(
        self,
        pending: &PendingBatch,
        topology_generation: TopologyGeneration,
        budget: &ResourceBudget,
    ) -> Result<PreparationInspection, PreparationError> {
        let transfer = Limits {
            max_records: self.limits.max_records,
            max_encoded_bytes: self.limits.max_encoded_bytes,
            fact: FactDecodeLimits::default(),
        };
        let reader_bytes = transfer
            .required_data_bytes()
            .map_err(|_| PreparationError::InvalidLimits)?;
        let native_bytes = self
            .limits
            .memory_bytes
            .checked_add(self.limits.native_reserve_bytes)
            .ok_or(PreparationError::InvalidLimits)?;
        if self.limits.memory_bytes == 0
            || self.limits.temp_bytes == 0
            || self.limits.threads == 0
            || self.limits.native_reserve_bytes <= reader_bytes as u64
        {
            return Err(PreparationError::InvalidLimits);
        }
        self.check_temporary_root()?;
        // Immediate whole-job admission: a failed second grant drops the first,
        // without joining a queue while retaining a scarce upstream resource.
        let native = budget
            .try_acquire(
                ResourceRequest::data(0, 1, native_bytes)
                    .map_err(|_| PreparationError::InvalidLimits)?,
            )
            .map_err(|_| PreparationError::Unavailable)?;
        // The pre-exec hasher uses a 64 KiB block; retain control capacity for
        // verification, bounded pipe capture and result validation together.
        let control = budget
            .try_acquire(
                ResourceRequest::control(128 * 1024)
                    .map_err(|_| PreparationError::InvalidLimits)?,
            )
            .map_err(|_| PreparationError::Unavailable)?;
        let mut command = self
            .installation
            .verified_command(self.deadline, self.cancelled)
            .map_err(PreparationError::Installation)?;
        command
            .current_dir(self.temp_root)
            .arg("inspect-batch")
            .arg(&self.installation.root)
            .arg(self.temp_root)
            .args([
                self.limits.memory_bytes.to_string(),
                self.limits.temp_bytes.to_string(),
                self.limits.threads.to_string(),
                self.limits.max_records.to_string(),
                self.limits.max_encoded_bytes.to_string(),
            ]);
        let mut process = super::process::Process::spawn(
            &mut command,
            native,
            control,
            self.deadline,
            self.cancelled,
        )
        .map_err(PreparationError::Process)?;
        let transfer_result = pending.write_archive_batch(&mut process, transfer, self.cancelled);
        let transport_error = process.failure();
        // Always finish/reap even after a failed write. A transport error cannot
        // release the native grant while the reader is still alive.
        let output = process.finish();
        if matches!(output, Err(ProcessError::TerminationUnknown)) {
            return Err(PreparationError::Process(ProcessError::TerminationUnknown));
        }
        transfer_result.map_err(|error| {
            transport_error
                .map(PreparationError::Process)
                .unwrap_or(PreparationError::Transfer(error))
        })?;
        let output = output.map_err(PreparationError::Process)?;
        let value: serde_json::Value =
            serde_json::from_slice(output).map_err(|_| PreparationError::InvalidResult)?;
        let field = |name: &str| {
            value
                .get(name)
                .cloned()
                .ok_or(PreparationError::InvalidResult)
        };
        let first: SourceRecordId =
            serde_json::from_value(field("first")?).map_err(|_| PreparationError::InvalidResult)?;
        let last: SourceRecordId =
            serde_json::from_value(field("last")?).map_err(|_| PreparationError::InvalidResult)?;
        let digest: Digest256 = serde_json::from_value(field("batch_digest")?)
            .map_err(|_| PreparationError::InvalidResult)?;
        let staged_counts: [u64; 5] = serde_json::from_value(field("staged_counts")?)
            .map_err(|_| PreparationError::InvalidResult)?;
        if value["format"] != "bigbrotr-private-preparation/1"
            || value.as_object().map(|m| m.len()) != Some(6)
            || first != pending.batch().first()
            || last != pending.batch().last()
            || digest != pending.batch().digest()
            || value["facts"].as_u64() != Some(pending.batch().records().len() as u64)
            || staged_counts
                .iter()
                .try_fold(0u64, |n, count| n.checked_add(*count))
                != Some(pending.batch().records().len() as u64)
        {
            return Err(PreparationError::InvalidResult);
        }
        let archive = ArchiveBatch::new(
            1,
            topology_generation,
            std::slice::from_ref(pending.batch()),
        )
        .map_err(|_| PreparationError::InvalidResult)?;
        Ok(PreparationInspection {
            staged_counts,
            archive_batch_digest: archive.digest(),
        })
    }
    fn check_temporary_root(&self) -> Result<(), PreparationError> {
        let root = self.temp_root;
        let metadata =
            fs::symlink_metadata(root).map_err(|_| PreparationError::InvalidTemporaryRoot)?;
        if !root.is_absolute()
            || root.canonicalize().ok().as_deref() != Some(root)
            || !metadata.is_dir()
            || metadata.permissions().mode() & 0o077 != 0
            || metadata.uid() != rustix::process::geteuid().as_raw()
            || root.starts_with(&self.installation.root)
            || self.installation.root.starts_with(root)
            || fs::read_dir(root)
                .map_err(|_| PreparationError::InvalidTemporaryRoot)?
                .next()
                .is_some()
        {
            return Err(PreparationError::InvalidTemporaryRoot);
        }
        Ok(())
    }
}
