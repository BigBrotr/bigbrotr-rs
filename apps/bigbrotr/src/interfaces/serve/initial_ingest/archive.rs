//! Initial local publisher: complete pending batches, then a captured frontier.
//! No pending ACK, source closure, replay adoption or index readiness is implied.
pub(super) mod native;
use super::commands::CommandOwner;
use super::*;
use crate::interfaces::archive_preflight as preflight;
use bigbrotr::{
    secrets::{SecretBytes, SecretFile},
    storage::{
        archive::{
            CatalogReaderCredentials, LocalAttempt, LocalCoordinator, Preparation,
            PreparationLimits, ReaderCustody, RetainedInputs, WorkerBudgets,
        },
        postgres::{
            archive_attempt::ArchiveAttemptRequest,
            catalog_guard::{self, CatalogCapture},
        },
        rocks::store::{PendingBatch, PendingReadLimits},
    },
};
use bigbrotr_model::{ArchiveBatch, Sequence, SourceRecordId, codec::FactDecodeLimits};
use bigbrotr_protocol::archive_cut::{CutBound, CutRead};
use std::time::Duration;

// One pending input, reader input, catalog connection, native TLS and cut result,
// plus up to one compact retained input per outstanding initial producer request.
pub(super) const ITEMS: u64 = 5;
const RECORDS: usize = 64;
const ENCODED: usize = 1024 * 1024;
const OPERATION: Duration = Duration::from_secs(30);

pub(super) struct Archive {
    pub(super) coordinator: LocalCoordinator,
    native: ResourceBudget,
    reader: Arc<native::Reader>,
    inputs: Vec<RetainedInputs>,
    next: u64,
    revision: u64,
    profile: Option<(Duration, u64)>,
}
fn error(error: impl std::fmt::Debug) -> (u8, String) {
    (
        4,
        format!("initial Archive refused; preserve original state: {error:?}"),
    )
}
impl Archive {
    pub(super) fn prepare(
        root: &Resolved,
        budget: &ResourceBudget,
        deadline: Instant,
        cancelled: &AtomicBool,
    ) -> Result<Self> {
        let identity = root.catalog_identity().map_err(error)?;
        let state = root.archive_state_path().map_err(error)?;
        let coordinator = LocalCoordinator::open(&identity, &state).map_err(error)?;
        let n = |field: &str| root.number(&format!("runtime.processes.native.{field}"));
        let native = ResourceBudget::new(
            ProcessEnvelope::new(
                n("memory_max_bytes"),
                n("baseline_bytes"),
                ResourceLimits::new(
                    n("payload_bytes"),
                    1,
                    n("native_bytes"),
                    n("completion_reserve_bytes"),
                ),
                WaiterLimits::new(0, 0),
            )
            .map_err(error)?,
        );
        // Only the fixed minimum qualified worker is checked here. Managed
        // seed policy is not authority; both accepted documents are checked
        // after Control acquisition, before fresh Ingest registration.
        let admitted = native
            .try_acquire(ResourceRequest::data(0, 1, 512 * MIB).map_err(error)?)
            .map_err(error)?;
        drop(admitted);
        let prepared = native::reader(root, budget)?;
        let mut startup = root.clone();
        startup
            .values
            .insert("archive.worker_memory_bytes".into(), json!(512 * MIB));
        let mut limits = preflight::limits(&startup)?;
        limits.max_records = 1;
        // Verify the packaged worker/scratch and actual catalog prerequisites
        // before registering this fresh Ingest lifetime. The joined preparer is
        // still under the parent's original deadline and cancellation watchdog.
        let (installation, scratch, _) = preflight::worker(
            &startup,
            &identity,
            None,
            "initial-startup",
            limits,
            deadline.into_std(),
            cancelled,
        )?;
        let reader = tokio::runtime::Handle::current().block_on(native::finite(
            deadline,
            cancelled,
            native::check_reader(
                root,
                &coordinator,
                prepared,
                Preparation {
                    installation: &installation,
                    temp_root: &scratch,
                    limits,
                    deadline: deadline.into_std(),
                    cancelled,
                },
                WorkerBudgets {
                    application: budget,
                    native: &native,
                },
            ),
        ))?;
        Ok(Self {
            coordinator,
            reader,
            native,
            inputs: Vec::new(),
            next: 1,
            revision: 0,
            profile: None,
        })
    }

    pub(super) fn accept(&mut self, root: &Resolved) -> std::result::Result<(), OwnerError> {
        let profile = validate(root)?;
        if self.profile.is_some_and(|prior| prior != profile) {
            return Err(OwnerError::InvalidPolicy);
        }
        let _admitted = self
            .native
            .try_acquire(
                ResourceRequest::data(0, 1, profile.1).map_err(|_| OwnerError::InvalidPolicy)?,
            )
            .map_err(|_| OwnerError::InvalidPolicy)?;
        self.profile = Some(profile);
        Ok(())
    }
    pub(super) fn delay(&self) -> Duration {
        self.profile.expect("accepted archive profile").0
    }

    fn runtime_root(&self, root: &Resolved) -> Resolved {
        let mut root = root.clone();
        root.values.insert(
            "archive.worker_memory_bytes".into(),
            json!(self.profile.expect("accepted archive profile").1),
        );
        root
    }

    pub(super) async fn require(&self, owner: &mut ControlSession) -> Result<()> {
        owner
            .require_archive_coordinator(&self.coordinator)
            .await
            .map_err(error)?;
        owner
            .require_initial_archive(&self.coordinator)
            .await
            .map_err(error)?;
        Ok(())
    }

    /// The native owner reads one whole batch; no producer sequence is used as a
    /// scientific source bound. Advance only after the captured cut is committed.
    pub(super) async fn publish(
        &mut self,
        store: &IngestStore,
        budget: &ResourceBudget,
        state: &mut CommandOwner<'_>,
        due: &mut Instant,
        management: &mut Option<management::Management>,
        cancelled: &AtomicBool,
    ) -> Result<bool> {
        let limits = PendingReadLimits {
            max_records: RECORDS,
            max_encoded_bytes: ENCODED,
            fact: FactDecodeLimits::default(),
        };
        let first = SourceRecordId {
            dataset_id: state.initial_registration()?.dataset_id,
            shard_id: state.initial_registration()?.shard_id,
            owner_epoch: OwnerEpoch::new(1).unwrap(),
            sequence: Sequence::new(self.next).map_err(error)?,
        };
        let Some(pending) = store
            .read_pending_batch(
                first,
                limits,
                grant(budget, limits.required_data_bytes().map_err(error)?, 0)?,
                cancelled,
            )
            .map_err(error)?
        else {
            return Ok(false);
        };
        let end = pending.batch().last().sequence.get();
        let last = end
            .checked_add(1)
            .ok_or_else(|| error("source sequence exhausted"))?;
        if self.inputs.len() >= state.initial_registration()?.outstanding_request_limit as usize {
            return Err(error("initial complete-batch window exhausted"));
        }
        self.inputs.try_reserve(1).map_err(error)?;
        let deadline = Instant::now() + OPERATION;
        let root = self.runtime_root(state.root);
        let app = budget.clone();
        let identity = self.coordinator.identity().clone();
        let prepared = state
            .blocking(due, management, deadline, move |cancel| {
                native::publisher(&root, &app, &identity, deadline, cancel)
            })
            .await?;
        let archive = ArchiveBatch::new(
            1,
            state
                .initial_registration()?
                .target_topology()
                .map_err(error)?,
            std::slice::from_ref(pending.batch()),
        )
        .map_err(error)?;
        let fresh = || super::super::fresh_process_incarnation().map_err(error);
        let request = ArchiveAttemptRequest::new(
            *fresh()?.as_bytes(),
            self.coordinator.identity().catalog(),
            state.owner.identity().process_incarnation,
            fresh()?,
            self.inputs.len() as u64,
            &archive,
        )
        .map_err(error)?;
        let retained = self
            .coordinator
            .retain_inputs(
                &request,
                &archive,
                grant(budget, RetainedInputs::required_data_bytes(), 0)?,
                cancelled,
            )
            .map_err(error)?;
        // Check the prospective COMPLETE cut, including all prior receipts,
        // before issuing this attempt or allowing any native publication. Use
        // the largest legal snapshot solely as a conservative wire-size bound.
        {
            let _cut_input = grant(budget, MIB, 0)?;
            let mut read = self.cut_read(state.initial_registration()?, end, i64::MAX as u64)?;
            read.receipts.push(retained.expectation().clone());
            let mut limits = preflight::limits(&self.runtime_root(state.root))?;
            limits.max_records = read.receipts.len();
            limits.max_encoded_bytes = ENCODED;
            native::validate_cut(&self.reader, self.coordinator.identity(), &read, limits)?;
        }
        let receipt = bounded(
            state.operation_window(deadline)?,
            state.cancellation,
            async {
                state
                    .owner
                    .register_retained_archive_attempt(&retained, &Cell::new(false))
                    .await
                    .map_err(error)
            },
        )
        .await?;
        let custody = LocalAttempt::create(
            self.coordinator.state_root(),
            &receipt,
            self.coordinator.identity(),
        )
        .map_err(error)?;
        let app = budget.clone();
        let native = self.native.clone();
        let reader = self.reader.clone();
        let identity = self.coordinator.identity().clone();
        state
            .blocking(due, management, deadline, move |cancel| {
                native::publish(
                    app, native, identity, receipt, custody, pending, prepared, reader, deadline,
                    cancel,
                )
            })
            .await?;
        self.inputs.push(retained);
        self.capture(budget, state, due, management, end).await?;
        self.next = last;
        Ok(true)
    }

    async fn capture(
        &mut self,
        budget: &ResourceBudget,
        state: &mut CommandOwner<'_>,
        due: &mut Instant,
        management: &mut Option<management::Management>,
        end: u64,
    ) -> Result<()> {
        let deadline = Instant::now() + OPERATION;
        let root = self.runtime_root(state.root);
        let app = budget.clone();
        let prepared = state
            .blocking(due, management, deadline, move |_| {
                postgres_command::prepare_catalog_as(
                    &root,
                    required(&root, "postgres.catalog.user")?,
                    required(&root, "postgres.catalog.password_ref")?,
                    &app,
                )
            })
            .await?;
        let (mut manager, connection) = state
            .external(due, management, deadline, async {
                native::connect_manager(prepared.0, &self.reader).await
            })
            .await?;
        let connection = Box::pin(
            self.coordinator
                .retain_connection(connection)
                .map_err(error)?,
        );
        let result = native::drive(connection, async move {
            let result = async {
                let reader_login = required(state.root, "postgres.catalog.read_user")?.to_owned();
                state
                    .external(due, management, deadline, async {
                        catalog_guard::inspect_reader(
                            &mut manager,
                            self.coordinator.identity(),
                            &reader_login,
                        )
                        .await
                        .map_err(error)
                    })
                    .await?;
                let capture = state
                    .external(due, management, deadline, async {
                        CatalogCapture::open(
                            &mut manager,
                            &self.coordinator,
                            deadline.saturating_duration_since(Instant::now()),
                        )
                        .await
                        .map_err(error)
                    })
                    .await?;
                let read =
                    self.cut_read(state.initial_registration()?, end, capture.snapshot_id())?;
                let reader = self.reader.clone();
                let custody = self.coordinator.reader_custody().map_err(error)?;
                let root = self.runtime_root(state.root);
                let app = budget.clone();
                let native = self.native.clone();
                let observed = state
                    .blocking(due, management, deadline, move |cancel| {
                        native::cut(root, app, native, reader, custody, read, deadline, cancel)
                    })
                    .await?;
                let captured = state
                    .external(due, management, deadline, async {
                        capture.accept(observed).await.map_err(error)
                    })
                    .await?;
                let operation = super::super::fresh_process_incarnation().map_err(error)?;
                let maximum = state.operation_window(deadline)?;
                let frontier = bounded(maximum, state.cancellation, async {
                    state
                        .owner
                        .publish_archive_frontier(
                            &captured,
                            &self.inputs,
                            *operation.as_bytes(),
                            self.revision,
                            &Cell::new(false),
                        )
                        .await
                        .map_err(error)
                })
                .await?;
                self.revision = frontier.revision;
                drop(captured);
                state
                    .external(due, management, deadline, async {
                        capture.finish().await.map_err(error)
                    })
                    .await?;
                Ok(())
            }
            .await;
            drop(manager);
            result
        })
        .await;
        drop(prepared.1);
        result
    }
    fn cut_read(&self, spec: &InitialRegistration, end: u64, snapshot: u64) -> Result<CutRead> {
        Ok(CutRead {
            dataset_id: spec.dataset_id,
            topology_generation: spec.target_topology().map_err(error)?,
            bounds: vec![CutBound {
                shard_id: spec.shard_id,
                owner_epoch: 1,
                complete_through: end,
            }],
            receipts: self
                .inputs
                .iter()
                .map(|i| i.expectation().clone())
                .collect(),
            snapshot_id: Some(snapshot),
        })
    }
}

// The finite initial adapter uses the qualified native file profile and one
// worker at a time (within the configured preparation concurrency ceiling).
pub(super) fn validate(root: &Resolved) -> std::result::Result<(Duration, u64), OwnerError> {
    let delay = Duration::from_micros(root.number("archive.max_publish_delay"));
    let bytes = root.number("archive.worker_memory_bytes");
    if !root.local("archive")
        || !root.flag("archive.publisher.enabled")
        || root.number("archive.row_group_rows") != 64000
        || root.text("archive.compression") != Some("zstd-3")
        || root.number("archive.target_file_bytes") != 256 * MIB
        || bytes < 512 * MIB
        || root.number("archive.prepare_workers") == 0
        || delay.is_zero()
        || root.number("ingest.producer_outstanding_requests")
            > bigbrotr_protocol::archive_cut::MAX_BATCHES as u64
    {
        return Err(OwnerError::InvalidPolicy);
    }
    Ok((delay, bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn selected_initial_window_cannot_outlive_the_complete_cut_protocol() {
        let mut root = crate::config::load(Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../config/recipes/archive-only.toml"
        )))
        .unwrap();
        for count in [32, 65, 128] {
            root.values
                .insert("ingest.producer_outstanding_requests".into(), json!(count));
            assert!(validate(&root).is_ok(), "{count}");
        }
        root.values
            .insert("ingest.producer_outstanding_requests".into(), json!(129));
        assert!(matches!(validate(&root), Err(OwnerError::InvalidPolicy)));
    }
}
