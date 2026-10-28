//! Initial local publisher: complete pending batches, then a captured frontier.
//! No pending ACK, source closure, replay adoption or index readiness is implied.
pub(super) mod native;
use super::admission::Admission;
use super::commands::CommandOwner;
use super::*;
use crate::interfaces::archive_preflight as preflight;
use bigbrotr::{
    runtime::policy_authority::{AuthorityClock, SuspendAwareClock},
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
// plus one compact retained input per retained initial producer request.
pub(super) const ITEMS: u64 = 5;
const RECORDS: usize = 64;
const ENCODED: usize = 1024 * 1024;
const OPERATION: Duration = Duration::from_secs(30);
const RETAINED_BATCHES: usize =
    bigbrotr::storage::rocks::store::RETAINED_PRODUCER_REQUEST_LIMIT as usize;
const _: () = assert!(RETAINED_BATCHES <= bigbrotr_protocol::archive_cut::MAX_BATCHES);

pub(super) struct Archive {
    parked: Option<ParkedAttempt>,
    pub(super) coordinator: LocalCoordinator,
    native: ResourceBudget,
    reader: Arc<native::Reader>,
    inputs: Vec<RetainedInputs>,
    next: u64,
    revision: u64,
    profile: Option<(Duration, u64)>,
    observation: Option<readiness::ArchiveObservation>,
}
struct ParkedAttempt {
    // The attempt remains issued in Control. Keep its original descriptor and
    // charged immutable inputs until this lifetime ends, without another dial.
    _custody: Box<LocalAttempt>,
    _inputs: RetainedInputs,
}
pub(super) struct Reservation {
    application: ReservedBudget,
    native: ReservedBudget,
}
fn error(error: impl std::fmt::Debug) -> (u8, String) {
    (
        4,
        format!("initial Archive refused; preserve original state: {error:?}"),
    )
}
fn source_limits() -> PendingReadLimits {
    PendingReadLimits {
        max_records: RECORDS,
        max_encoded_bytes: ENCODED,
        fact: FactDecodeLimits::default(),
    }
}
fn requirements(root: &Resolved) -> Result<(ResourceLimits, ResourceLimits)> {
    let limits = preflight::limits(root)?;
    let native = limits
        .memory_bytes
        .checked_add(limits.native_reserve_bytes)
        .ok_or_else(|| error("native reservation overflow"))?;
    // Conservatively overlap the pending source, catalog transport, retained
    // immutable input, prospective cut and observed cut. Publish and capture
    // use the same allowance; no new parent admission follows an attempt.
    let data = source_limits()
        .required_data_bytes()
        .map_err(error)?
        .checked_add(postgres_command::catalog_reservation(
            root,
            postgres_command::CATALOG_EXTRA_BYTES,
        )?)
        .and_then(|n| n.checked_add(RetainedInputs::required_data_bytes()))
        .and_then(|n| n.checked_add(2 * MIB))
        .ok_or_else(|| error("application reservation overflow"))?;
    Ok((
        ResourceLimits::new(data, ITEMS, 0, 128 * 1024),
        ResourceLimits::new(0, 1, native, 0),
    ))
}
impl Archive {
    pub(super) fn native_snapshot(&self) -> bigbrotr::runtime::BudgetSnapshot {
        self.native.snapshot()
    }
    pub(super) fn reserve(
        &self,
        root: &Resolved,
        budget: &ResourceBudget,
    ) -> Result<Admission<Reservation>> {
        let (application, native) = requirements(&self.runtime_root(root))?;
        let application = match admission::reserve(budget, application, "application")? {
            Admission::Granted(value) => value,
            Admission::Deferred(refusal) => return Ok(Admission::Deferred(refusal)),
        };
        let native = match admission::reserve(&self.native, native, "native")? {
            Admission::Granted(value) => value,
            Admission::Deferred(refusal) => return Ok(Admission::Deferred(refusal)),
        };
        Ok(Admission::Granted(Reservation {
            application,
            native,
        }))
    }
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
            parked: None,
            coordinator,
            reader,
            native,
            inputs: Vec::new(),
            next: 1,
            revision: 0,
            profile: None,
            observation: None,
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
        self.profile
            .expect("accepted archive profile")
            .0
            .min(readiness::FRESHNESS / 2)
    }

    pub(super) fn observation(&self) -> Option<readiness::ArchiveObservation> {
        self.observation
    }

    pub(super) fn parked(&self) -> bool {
        self.parked.is_some()
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
        reservation: Reservation,
        state: &mut CommandOwner<'_>,
        due: &mut Instant,
        management: &mut Option<management::Management>,
        cancelled: &AtomicBool,
    ) -> Result<bool> {
        if self.parked() {
            // The timer is disabled too, but no caller can retry an issued
            // attempt by invoking this method again in the same lifetime.
            return Ok(false);
        }
        let budget = reservation.application.budget();
        let native = reservation.native.budget();
        let limits = source_limits();
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
            // Recheck the complete physical cut only at a coalesced interval.
            // An empty pending lookup alone proves neither an empty source nor
            // continued availability of an already published Archive.
            if let Some(previous) = self.observation {
                let now = SuspendAwareClock.read().map_err(error)?;
                if now.elapsed_since(previous.started).map_err(error)? >= readiness::FRESHNESS / 2 {
                    self.capture(
                        &reservation,
                        state,
                        due,
                        management,
                        previous.through,
                        false,
                    )
                    .await?;
                }
            }
            return Ok(false);
        };
        let end = pending.batch().last().sequence.get();
        let last = end
            .checked_add(1)
            .ok_or_else(|| error("source sequence exhausted"))?;
        if self.inputs.len() >= RETAINED_BATCHES {
            return Err(error("initial retained complete-batch limit exhausted"));
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
        let worker_budget = native.clone();
        let reader = self.reader.clone();
        let identity = self.coordinator.identity().clone();
        let published = state
            .blocking(due, management, deadline, move |cancel| {
                native::publish(
                    app,
                    worker_budget,
                    identity,
                    receipt,
                    custody,
                    pending,
                    prepared,
                    reader,
                    deadline,
                    cancel,
                )
            })
            .await?;
        match published {
            native::Publication::Complete => {}
            native::Publication::ConnectionRefused(custody) => {
                // blocking returned only after the actual task joined. Control
                // remains usable only if its independent authority/cancellation
                // window still allows delivery; an error never becomes a park.
                state.check_external_completion(deadline)?;
                self.parked = Some(ParkedAttempt {
                    _custody: custody,
                    _inputs: retained,
                });
                return Ok(false);
            }
        }
        self.inputs.push(retained);
        self.capture(&reservation, state, due, management, end, true)
            .await?;
        self.next = last;
        Ok(true)
    }

    async fn capture(
        &mut self,
        reservation: &Reservation,
        state: &mut CommandOwner<'_>,
        due: &mut Instant,
        management: &mut Option<management::Management>,
        end: u64,
        publish: bool,
    ) -> Result<()> {
        let budget = reservation.application.budget();
        let native = reservation.native.budget();
        // Freshness includes preparation, every physical read and cleanup.
        let started = SuspendAwareClock.read().map_err(error)?;
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
                let native = native.clone();
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
                let maximum = state.operation_window(deadline)?;
                if publish {
                    let operation = super::super::fresh_process_incarnation().map_err(error)?;
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
                } else {
                    // This read-only probe cannot create a frontier or adopt
                    // a different native snapshot. Recheck the actual PG head
                    // inside the retained catalog barrier after all rows.
                    let selected = bounded(maximum, state.cancellation, async {
                        state
                            .owner
                            .read_current_archive_selection(self.coordinator.identity().catalog())
                            .await
                            .map_err(error)
                    })
                    .await?
                    .ok_or_else(|| error("published Archive selection missing"))?;
                    let spec = state.initial_registration()?;
                    if selected.dataset_id != spec.dataset_id
                        || selected.catalog_id != self.coordinator.identity().catalog()
                        || selected.frontier_revision != self.revision
                        || selected.snapshot_id != capture.snapshot_id()
                        || selected.topology_generation != spec.target_topology().map_err(error)?
                        || selected.bounds
                            != [CutBound {
                                shard_id: spec.shard_id,
                                owner_epoch: 1,
                                complete_through: end,
                            }]
                    {
                        return Err(error(
                            "published Archive selection changed during observation",
                        ));
                    }
                }
                drop(captured);
                state
                    .external(due, management, deadline, async {
                        capture.finish().await.map_err(error)
                    })
                    .await?;
                let spec = state.initial_registration()?;
                self.observation = Some(readiness::ArchiveObservation {
                    dataset: spec.dataset_id,
                    topology: spec.target_topology().map_err(error)?,
                    shard: spec.shard_id,
                    revision: self.revision,
                    through: end,
                    started,
                });
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
    {
        return Err(OwnerError::InvalidPolicy);
    }
    Ok((delay, bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn funded_publication_includes_catalog_cut_completion_and_retained_input() {
        let root = crate::config::load(Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../config/recipes/archive-only.toml"
        )))
        .unwrap();
        let (app, native) = requirements(&root).unwrap();
        eprintln!(
            "{}",
            json!({"format":"bigbrotr-initial-budget-requirements/1","role":"archive",
            "scope":"compiled-finite-profile-not-a-capacity-claim",
            "application":{"data_bytes":app.data_bytes(),"items":app.items(),"native_bytes":app.native_bytes(),"control_bytes":app.control_bytes()},
            "native":{"data_bytes":native.data_bytes(),"items":native.items(),"native_bytes":native.native_bytes(),"control_bytes":native.control_bytes()},
            "retained_reader_data_bytes":MIB,"retained_input_data_bytes":RetainedInputs::required_data_bytes()})
        );
        // Literal complete catalog allowance: password source and copies,
        // transport16MiB, operation64MiB. This must not regress to preparation
        // alone. Public recipe uses the declared plaintext catalog route.
        let catalog = 8 * (64 * 1024 + 1) + 80 * MIB;
        assert_eq!(
            app.data_bytes(),
            source_limits().required_data_bytes().unwrap() + catalog + 3 * MIB
        );
        assert_eq!((app.items(), app.control_bytes()), (5, 128 * 1024));
        assert_eq!(
            native,
            ResourceLimits::new(0, 1, root.number("archive.worker_memory_bytes"), 0)
        );
        let parent = ResourceBudget::new(
            ProcessEnvelope::new(
                app.data_bytes() + app.control_bytes(),
                0,
                app,
                WaiterLimits::new(0, 0),
            )
            .unwrap(),
        );
        let scope = parent.try_reserve(app).unwrap();
        let input = grant(scope.budget(), MIB, 0).unwrap();
        let transport = grant(scope.budget(), catalog, 0).unwrap();
        let cut = grant(scope.budget(), MIB, 0).unwrap();
        let worker = scope
            .budget()
            .try_acquire(ResourceRequest::control(128 * 1024).unwrap())
            .unwrap();
        assert!(
            parent
                .try_acquire(ResourceRequest::data(1, 1, 0).unwrap())
                .is_err()
        );
        drop((transport, cut, worker));
        drop(scope);
        assert_eq!(parent.snapshot().used(), ResourceLimits::new(MIB, 1, 0, 0));
        drop(input);
        assert_eq!(parent.snapshot().active_permits(), 0);
    }
    #[test]
    fn active_request_window_does_not_define_archive_retention() {
        let mut root = crate::config::load(Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../config/recipes/archive-only.toml"
        )))
        .unwrap();
        for count in [2, 32, 65, 128, 129] {
            root.values
                .insert("ingest.producer_outstanding_requests".into(), json!(count));
            assert!(validate(&root).is_ok(), "{count}");
        }
    }
}
