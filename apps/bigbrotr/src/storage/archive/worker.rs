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
    InvalidBootstrap,
    InvalidTemporaryRoot,
    Custody(super::LifetimeError),
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

/// Concrete process lanes, including when the caller already retains native
/// state such as Rocks in its application process. Neither lane may borrow
/// unused capacity from the other. The child owns both grants until actual reap.
#[derive(Clone, Copy)]
pub struct WorkerBudgets<'a> {
    pub application: &'a ResourceBudget,
    pub native: &'a ResourceBudget,
}
impl<'a> WorkerBudgets<'a> {
    /// Existing finite commands use a prevalidated aggregate with disjoint lanes.
    pub fn combined(budget: &'a ResourceBudget) -> Self {
        Self {
            application: budget,
            native: budget,
        }
    }
}
enum Custody<'a> {
    None,
    Archive(&'a super::LocalAttempt),
    Initialization(&'a super::LocalInitialization),
    Reader(&'a super::ReaderCustody),
}

impl<'a> Preparation<'a> {
    /// Consume the only freshly committed credential into its custodied child.
    /// The route is the same pinned endpoint used by the reserving connection.
    pub fn initialize_fresh_catalog(
        self,
        fresh: crate::storage::postgres::catalog_guard::initialization::FreshBootstrap,
        custody: &super::LocalInitialization,
        route: &super::CatalogRoute<'_>,
        budget: &ResourceBudget,
    ) -> Result<(), PreparationError> {
        if fresh.reservation().initialization_id != *custody.initialization_id() {
            return Err(PreparationError::InvalidBootstrap);
        }
        fresh.with_login(|login, password| {
            let (dsn, tls) = route
                .connection(login, password, "bigbrotr-archive-initializer")
                .map_err(|_| PreparationError::InvalidBootstrap)?;
            self.initialize_reserved_catalog_with_tls(&dsn, custody, budget, tls)
        })
    }

    /// Check the actual private empty scratch directory before durable admission.
    pub fn validate_temporary_root(&self) -> Result<(), PreparationError> {
        self.check_temporary_root()
    }

    /// Explicit finite native catalog initialization/attach. The caller supplies
    /// a trusted, already validated infrastructure route/TLS policy and keeps
    /// credentials out of argv/environment/logs. This success proves joined
    /// native completion only: installation must still inspect freshness and
    /// ownership, and an ambiguous result must not authorize another lifetime.
    pub fn bootstrap_catalog(
        self,
        dsn: &str,
        data_root: &str,
        budget: &ResourceBudget,
    ) -> Result<(), PreparationError> {
        self.catalog_operation(
            "bootstrap-catalog",
            "bigbrotr-native-bootstrap/1",
            dsn,
            data_root,
            budget,
        )
    }

    /// Explicit fresh scientific schema creation. The operator must exclusively
    /// reserve an empty database/root first. Existing databases refuse; resolve
    /// ambiguity through a fresh privileged inspection instead of blind retry.
    /// Joined DDL completion is not a schema binding, guard or archive receipt.
    pub fn initialize_catalog(
        self,
        dsn: &str,
        data_root: &str,
        budget: &ResourceBudget,
    ) -> Result<(), PreparationError> {
        self.catalog_operation(
            "initialize-catalog",
            "bigbrotr-native-initialize/1",
            dsn,
            data_root,
            budget,
        )
    }

    /// Initialize only the permanently reserved root, retaining the same locked
    /// descriptor in the actual native child. Database/credential reservation and
    /// subsequent retirement/install are still the caller's separate obligations.
    pub fn initialize_reserved_catalog(
        self,
        dsn: &str,
        custody: &super::LocalInitialization,
        budget: &ResourceBudget,
    ) -> Result<(), PreparationError> {
        self.initialize_reserved_catalog_with_tls(dsn, custody, budget, None)
    }

    fn initialize_reserved_catalog_with_tls(
        self,
        dsn: &str,
        custody: &super::LocalInitialization,
        budget: &ResourceBudget,
        tls: Option<super::client_identity::NativeTlsHandoff>,
    ) -> Result<(), PreparationError> {
        #[derive(serde::Serialize)]
        struct Input<'a> {
            dsn: &'a str,
            data_root: &'a str,
            custody: super::initialization::NativeCustody,
        }
        let root = custody.identity().root();
        super::bootstrap::Input {
            dsn,
            data_root: root,
        }
        .validate()?;
        // The largest positive descriptor bounds the actual serialized input
        // before consuming the reservation's one launch or starting any child.
        super::bootstrap::wire_fits(&Input {
            dsn,
            data_root: root,
            custody: custody
                .native_custody(i32::MAX)
                .map_err(PreparationError::Custody)?,
        })?;
        let (process, proof) = self.start_owned(
            "initialize-reserved-catalog",
            &[],
            WorkerBudgets::combined(budget),
            Custody::Initialization(custody),
            tls,
        )?;
        let proof = proof.ok_or(PreparationError::Custody(super::LifetimeError::Identity))?;
        catalog_result(
            process,
            &Input {
                dsn,
                data_root: root,
                custody: proof,
            },
            "bigbrotr-native-reserved-initialize/1",
        )
    }

    fn catalog_operation(
        self,
        mode: &str,
        result_format: &str,
        dsn: &str,
        data_root: &str,
        budget: &ResourceBudget,
    ) -> Result<(), PreparationError> {
        let input = super::bootstrap::Input { dsn, data_root };
        input.validate()?;
        let process = self.start(mode, &[], budget)?;
        catalog_result(process, &input, result_format)
    }

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
        if self.limits.native_reserve_bytes <= reader_bytes as u64 {
            return Err(PreparationError::InvalidLimits);
        }
        let mut process = self.start(
            "inspect-batch",
            &[
                self.limits.max_records.to_string(),
                self.limits.max_encoded_bytes.to_string(),
            ],
            budget,
        )?;
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
        self.inspect_result(output, pending, topology_generation)
    }

    pub(super) fn start(
        &self,
        mode: &str,
        extra: &[String],
        budget: &ResourceBudget,
    ) -> Result<super::process::Process<'a>, PreparationError> {
        self.start_with_custody(mode, extra, WorkerBudgets::combined(budget), None, None)
    }

    pub(super) fn start_with_custody(
        &self,
        mode: &str,
        extra: &[String],
        budgets: WorkerBudgets<'_>,
        custody: Option<&super::LocalAttempt>,
        tls: Option<super::client_identity::NativeTlsHandoff>,
    ) -> Result<super::process::Process<'a>, PreparationError> {
        self.start_owned(
            mode,
            extra,
            budgets,
            custody.map_or(Custody::None, Custody::Archive),
            tls,
        )
        .map(|(process, _)| process)
    }

    pub(super) fn start_reader(
        &self,
        mode: &str,
        extra: &[String],
        budgets: WorkerBudgets<'_>,
        custody: &super::ReaderCustody,
        tls: Option<super::client_identity::NativeTlsHandoff>,
    ) -> Result<super::process::Process<'a>, PreparationError> {
        self.start_owned(mode, extra, budgets, Custody::Reader(custody), tls)
            .map(|(process, _)| process)
    }

    fn start_owned(
        &self,
        mode: &str,
        extra: &[String],
        budgets: WorkerBudgets<'_>,
        custody: Custody<'_>,
        tls: Option<super::client_identity::NativeTlsHandoff>,
    ) -> Result<
        (
            super::process::Process<'a>,
            Option<super::initialization::NativeCustody>,
        ),
        PreparationError,
    > {
        let native_bytes = self
            .limits
            .memory_bytes
            .checked_add(self.limits.native_reserve_bytes)
            .ok_or(PreparationError::InvalidLimits)?;
        if self.limits.memory_bytes == 0
            || self.limits.temp_bytes == 0
            || self.limits.threads == 0
            || self.limits.native_reserve_bytes == 0
        {
            return Err(PreparationError::InvalidLimits);
        }
        self.check_temporary_root()?;
        // Immediate whole-job admission: a failed second grant drops the first,
        // without joining a queue while retaining a scarce upstream resource.
        let native = budgets
            .native
            .try_acquire(
                ResourceRequest::data(0, 1, native_bytes)
                    .map_err(|_| PreparationError::InvalidLimits)?,
            )
            .map_err(|_| PreparationError::Unavailable)?;
        // The pre-exec hasher uses a 64 KiB block; retain control capacity for
        // verification, bounded pipe capture and result validation together.
        let control = budgets
            .application
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
            .arg(mode)
            .arg(&self.installation.root)
            .arg(self.temp_root)
            .args([
                self.limits.memory_bytes.to_string(),
                self.limits.temp_bytes.to_string(),
                self.limits.threads.to_string(),
            ])
            .args(extra);
        let proof = match custody {
            Custody::None => None,
            Custody::Archive(custody) => {
                custody
                    .inherit(&mut command)
                    .map_err(PreparationError::Custody)?;
                None
            }
            Custody::Initialization(custody) => Some(
                custody
                    .inherit(&mut command)
                    .map_err(PreparationError::Custody)?,
            ),
            Custody::Reader(custody) => {
                custody
                    .inherit(&mut command)
                    .map_err(PreparationError::Custody)?;
                None
            }
        };
        let tls = tls.map(|tls| tls.inherit(&mut command));
        let process = super::process::Process::spawn_with_tls(
            &mut command,
            native,
            control,
            self.deadline,
            self.cancelled,
            tls,
        )
        .map_err(PreparationError::Process)?;
        Ok((process, proof))
    }

    fn inspect_result(
        &self,
        output: &[u8],
        pending: &PendingBatch,
        topology_generation: TopologyGeneration,
    ) -> Result<PreparationInspection, PreparationError> {
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

fn catalog_result(
    mut process: super::process::Process<'_>,
    input: &impl serde::Serialize,
    result_format: &str,
) -> Result<(), PreparationError> {
    let transfer = serde_json::to_writer(&mut process, input);
    let transport_error = process.failure();
    let output = process.finish();
    if matches!(output, Err(ProcessError::TerminationUnknown)) {
        return Err(PreparationError::Process(ProcessError::TerminationUnknown));
    }
    transfer.map_err(|_| {
        transport_error
            .map(PreparationError::Process)
            .unwrap_or(PreparationError::InvalidBootstrap)
    })?;
    super::bootstrap::decode_response(output.map_err(PreparationError::Process)?, result_format)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::{ProcessEnvelope, ResourceLimits, WaiterLimits};
    use std::time::Duration;

    const MIB: u64 = 1024 * 1024;
    const CONTROL: u64 = 128 * 1024;

    fn budget(native: u64, control: u64) -> ResourceBudget {
        ResourceBudget::new(
            ProcessEnvelope::new(
                native + control,
                0,
                ResourceLimits::new(0, 2, native, control),
                WaiterLimits::new(0, 0),
            )
            .unwrap(),
        )
    }

    fn refuse_before_verification(budgets: WorkerBudgets<'_>) {
        let temporary = tempfile::tempdir().unwrap();
        fs::set_permissions(temporary.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let root = temporary.path().canonicalize().unwrap();
        // Admission must refuse before installation inspection, much less spawn.
        // If it reaches this missing executable, the error discriminator changes.
        let installation = WorkerInstallation {
            root: root.with_extension("absent-worker"),
            executable: super::super::ArtifactIdentity {
                bytes: 1,
                sha256: Digest256::from_bytes([1; 32]),
            },
        };
        let cancel = AtomicBool::new(false);
        let preparation = Preparation {
            installation: &installation,
            temp_root: &root,
            limits: PreparationLimits {
                memory_bytes: 128 * MIB,
                native_reserve_bytes: 384 * MIB,
                temp_bytes: 64 * MIB,
                threads: 1,
                max_records: 16,
                max_encoded_bytes: MIB as usize,
            },
            deadline: Instant::now() + Duration::from_secs(5),
            cancelled: &cancel,
        };
        assert!(matches!(
            preparation.start_with_custody("inspect-batch", &[], budgets, None, None),
            Err(PreparationError::Unavailable)
        ));
    }

    #[test]
    fn exhausted_application_control_returns_the_child_grant_before_launch() {
        let application = budget(0, CONTROL);
        let native = budget(512 * MIB, 0);
        let occupied = application
            .try_acquire(ResourceRequest::control(1).unwrap())
            .unwrap();
        refuse_before_verification(WorkerBudgets {
            application: &application,
            native: &native,
        });
        assert_eq!(application.snapshot().active_permits(), 1);
        assert_eq!(application.snapshot().used().control_bytes(), 1);
        assert_eq!(native.snapshot().active_permits(), 0);
        assert_eq!(native.snapshot().used().native_bytes(), 0);
        drop(occupied);
    }

    #[test]
    fn exhausted_child_allowance_never_charges_application_control() {
        let application = budget(0, CONTROL);
        let native = budget(512 * MIB, 0);
        let occupied = native
            .try_acquire(ResourceRequest::data(0, 1, 1).unwrap())
            .unwrap();
        refuse_before_verification(WorkerBudgets {
            application: &application,
            native: &native,
        });
        assert_eq!(native.snapshot().active_permits(), 1);
        assert_eq!(native.snapshot().used().native_bytes(), 1);
        assert_eq!(application.snapshot().active_permits(), 0);
        assert_eq!(application.snapshot().used().control_bytes(), 0);
        drop(occupied);
    }
}
