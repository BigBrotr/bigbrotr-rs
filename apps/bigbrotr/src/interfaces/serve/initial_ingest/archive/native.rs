//! Concrete catalog transports and native work owned by the joined publisher.
use super::*;
use bigbrotr::storage::{
    archive::{NativeClientIdentity, ObservedCut, WorkerInstallation},
    postgres::{archive_attempt::ArchiveAttemptReceipt, catalog_guard::CatalogIdentity},
};
use std::path::PathBuf;

pub(in crate::interfaces::serve::initial_ingest) mod catch_up;

pub(in crate::interfaces::serve::initial_ingest) struct Publisher {
    connection: PreparedConnection,
    grant: OwnedPermit,
    installation: WorkerInstallation,
    scratch: PathBuf,
    limits: PreparationLimits,
}
pub(in crate::interfaces::serve::initial_ingest) fn publisher(
    root: &Resolved,
    budget: &ResourceBudget,
    identity: &CatalogIdentity,
    deadline: Instant,
    cancel: &AtomicBool,
) -> Result<Publisher> {
    let (connection, grant) = postgres_command::prepare_catalog_as(
        root,
        required(root, "postgres.catalog.user")?,
        required(root, "postgres.catalog.password_ref")?,
        budget,
    )?;
    let mut limits = preflight::limits(root)?;
    limits.max_records = RECORDS;
    limits.max_encoded_bytes = ENCODED;
    let (installation, scratch, _) = preflight::worker(
        root,
        identity,
        None,
        "initial-publication",
        limits,
        deadline.into_std(),
        cancel,
    )?;
    Ok(Publisher {
        connection,
        grant,
        installation,
        scratch,
        limits,
    })
}

#[allow(clippy::too_many_arguments)]
pub(in crate::interfaces::serve::initial_ingest) fn publish(
    app: ResourceBudget,
    native: ResourceBudget,
    identity: CatalogIdentity,
    receipt: ArchiveAttemptReceipt,
    custody: LocalAttempt,
    pending: PendingBatch,
    prepared: Publisher,
    reader: Arc<Reader>,
    deadline: Instant,
    cancel: &AtomicBool,
) -> Result<()> {
    let runtime = tokio::runtime::Handle::current();
    runtime.block_on(async {
        let (mut manager, connection, _pinned) = finite(deadline, cancel, async {
            transport::connect_pinned(prepared.connection, Some(&reader.pinned))
                .await
                .map_err(error)
        })
        .await?;
        let connection = Box::pin(
            custody
                .retain_catalog_connection(connection)
                .map_err(error)?,
        );
        let result = drive(connection, async move {
            let result = async {
                let writer = finite(deadline, cancel, async {
                    catalog_guard::provision_and_activate(
                        &mut manager,
                        &identity,
                        &receipt,
                        &Cell::new(false),
                    )
                    .await
                    .map_err(error)
                })
                .await?;
                // The manager has no outstanding SQL while this thread supervises
                // the native child. The parent's independent Control loop continues.
                Preparation {
                    installation: &prepared.installation,
                    temp_root: &prepared.scratch,
                    limits: prepared.limits,
                    deadline: deadline.into_std(),
                    cancelled: cancel,
                }
                .publish_pending(
                    &pending,
                    writer,
                    custody,
                    &reader.route.borrowed(),
                    WorkerBudgets {
                        application: &app,
                        native: &native,
                    },
                )
                .map_err(error)?;
                finite(deadline, cancel, async {
                    catalog_guard::seal(&mut manager, &identity, &receipt, &Cell::new(false))
                        .await
                        .map_err(error)
                })
                .await?;
                Ok(())
            }
            .await;
            drop(manager);
            result
        })
        .await;
        drop(prepared.grant);
        result
    })
}

pub(in crate::interfaces::serve::initial_ingest) struct Reader {
    route: preflight::NativeRoute,
    pinned: transport::PinnedRoute,
    login: String,
    password: SecretBytes,
    _permit: OwnedPermit,
}
pub(in crate::interfaces::serve::initial_ingest) struct PreparedReader {
    pub(super) connection: PreparedConnection,
    pub(super) grant: OwnedPermit,
    password: SecretBytes,
    tls: Option<NativeClientIdentity>,
    permit: OwnedPermit,
}
impl Reader {
    pub(super) fn connect_input(
        root: &Resolved,
        pinned: transport::PinnedRoute,
        password: SecretBytes,
        tls: Option<NativeClientIdentity>,
        permit: OwnedPermit,
    ) -> Result<Reader> {
        Ok(Reader {
            route: preflight::NativeRoute::new(root, &pinned, tls)?,
            pinned,
            login: required(root, "postgres.catalog.read_user")?.to_owned(),
            password,
            _permit: permit,
        })
    }
}
pub(in crate::interfaces::serve::initial_ingest) fn reader(
    root: &Resolved,
    budget: &ResourceBudget,
) -> Result<PreparedReader> {
    required(root, "postgres.catalog.read_user")?;
    let (connection, grant) = postgres_command::prepare_catalog_as(
        root,
        required(root, "postgres.catalog.user")?,
        required(root, "postgres.catalog.password_ref")?,
        budget,
    )?;
    let permit = super::grant(budget, MIB, 0)?;
    let password = SecretFile::parse(required(root, "postgres.catalog.read_password_ref")?)
        .and_then(|file| file.read(4096))
        .map_err(error)?;
    std::str::from_utf8(password.as_bytes()).map_err(error)?;
    let tls = preflight::native_tls(root, budget)?;
    Ok(PreparedReader {
        connection,
        grant,
        password,
        tls,
        permit,
    })
}
pub(super) async fn check_reader(
    root: &Resolved,
    coordinator: &LocalCoordinator,
    prepared: PreparedReader,
    preparation: Preparation<'_>,
    budgets: WorkerBudgets<'_>,
) -> Result<Arc<Reader>> {
    let (mut manager, connection, pinned) = transport::connect_pinned(prepared.connection, None)
        .await
        .map_err(error)?;
    let connection = Box::pin(coordinator.retain_connection(connection).map_err(error)?);
    let result = drive(connection, async move {
        let result = async {
            catalog_guard::inspect_reader(
                &mut manager,
                coordinator.identity(),
                required(root, "postgres.catalog.read_user")?,
            )
            .await
            .map_err(error)?;
            let capture = CatalogCapture::open(&mut manager, coordinator, OPERATION)
                .await
                .map_err(error)?;
            if capture.snapshot_id() != 1 {
                return Err(error(
                    "initial Archive requires the original empty snapshot",
                ));
            }
            let reader = Reader::connect_input(
                root,
                pinned,
                prepared.password,
                prepared.tls,
                prepared.permit,
            )?;
            let empty = CutRead {
                dataset_id: coordinator.identity().dataset(),
                topology_generation: bigbrotr_model::TopologyGeneration::new(0),
                bounds: Vec::new(),
                receipts: Vec::new(),
                snapshot_id: Some(1),
            };
            // Authenticate the SAME retained native reader credential, observe
            // the empty scientific catalog, and actually join the child before
            // any fresh Ingest effects. Manager privilege inspection alone does
            // not establish that the reader password works.
            let observed = preparation
                .inspect_cut(
                    empty,
                    coordinator.reader_custody().map_err(error)?,
                    &reader.route.borrowed(),
                    CatalogReaderCredentials {
                        login: &reader.login,
                        password: std::str::from_utf8(reader.password.as_bytes()).map_err(error)?,
                    },
                    budgets,
                    1,
                )
                .map_err(error)?;
            let captured = capture.accept(observed).await.map_err(error)?;
            drop(captured);
            capture.finish().await.map_err(error)?;
            Ok(Arc::new(reader))
        }
        .await;
        drop(manager);
        result
    })
    .await;
    drop(prepared.grant);
    result
}
pub(in crate::interfaces::serve::initial_ingest) fn validate_cut(
    reader: &Reader,
    identity: &CatalogIdentity,
    read: &CutRead,
    limits: PreparationLimits,
) -> Result<()> {
    bigbrotr::storage::archive::validate_cut_limits(limits, read, usize::MAX).map_err(error)?;
    reader
        .route
        .borrowed()
        .validate_cut(
            read,
            identity,
            CatalogReaderCredentials {
                login: &reader.login,
                password: std::str::from_utf8(reader.password.as_bytes()).map_err(error)?,
            },
        )
        .map_err(error)
}

#[allow(clippy::too_many_arguments)]
pub(in crate::interfaces::serve::initial_ingest) fn cut(
    root: Resolved,
    app: ResourceBudget,
    native: ResourceBudget,
    reader: Arc<Reader>,
    custody: ReaderCustody,
    read: CutRead,
    deadline: Instant,
    cancel: &AtomicBool,
) -> Result<ObservedCut> {
    let scan_limit = read.receipts.len().max(1);
    cut_with_scan_limit(
        root, app, native, reader, custody, read, scan_limit, deadline, cancel,
    )
}

/// The unfiltered worker scans all catalog receipts, even for a shorter selected
/// prefix (including an empty cut). The finite caller admits the complete issued
/// inventory separately; legacy callers retain their original scan bound.
#[allow(clippy::too_many_arguments)]
pub(in crate::interfaces::serve::initial_ingest) fn cut_with_scan_limit(
    root: Resolved,
    app: ResourceBudget,
    native: ResourceBudget,
    reader: Arc<Reader>,
    custody: ReaderCustody,
    read: CutRead,
    scan_limit: usize,
    deadline: Instant,
    cancel: &AtomicBool,
) -> Result<ObservedCut> {
    let max_facts = cut_scan_capacity(&read, scan_limit)?;
    let mut limits = preflight::limits(&root)?;
    limits.max_records = scan_limit;
    limits.max_encoded_bytes = ENCODED;
    let (installation, scratch, _) = preflight::worker(
        &root,
        custody.identity(),
        None,
        "initial-cut",
        limits,
        deadline.into_std(),
        cancel,
    )?;
    Preparation {
        installation: &installation,
        temp_root: &scratch,
        limits,
        deadline: deadline.into_std(),
        cancelled: cancel,
    }
    .inspect_cut(
        read,
        custody,
        &reader.route.borrowed(),
        CatalogReaderCredentials {
            login: &reader.login,
            password: std::str::from_utf8(reader.password.as_bytes()).map_err(error)?,
        },
        WorkerBudgets {
            application: &app,
            native: &native,
        },
        max_facts,
    )
    .map_err(error)
}

/// Zero selected facts still need a nonzero bounded scan allowance. The full
/// receipt-table inventory is independent of this selected scientific prefix.
fn cut_scan_capacity(read: &CutRead, scan_limit: usize) -> Result<usize> {
    if scan_limit == 0 || scan_limit > 128 || scan_limit < read.receipts.len() {
        return Err(error("complete receipt scan limit"));
    }
    usize::try_from(
        read.receipts
            .iter()
            .try_fold(0u64, |n, r| n.checked_add(r.record_count))
            .ok_or_else(|| error("cut count overflow"))?,
    )
    .map(|count| count.max(1))
    .map_err(error)
}

/// Poll the actual transport, never a detachable task. The work owns its Client
/// and destroys it on all ordinary exits. Unwind drops both futures; the custody
/// wrapper destroys its transport before closing the retained descriptor.
pub(in crate::interfaces::serve::initial_ingest) async fn drive<T>(
    mut connection: transport::Driver,
    work: impl Future<Output = Result<T>>,
) -> Result<T> {
    tokio::pin!(work);
    let (result, ended) = tokio::select! {
        biased;
        result = &mut work => (result, false),
        _ = &mut connection => {
            // Work may be collecting a blocking child. Losing SQL must not drop
            // that future/JoinHandle; its original deadline and cleanup remain.
            ((&mut work).await, true)
        },
    };
    if ended {
        return result.and_then(|_| Err(error("catalog transport ended before work")));
    }
    // Client Drop queues termination. A bounded drain followed by actual future
    // destruction is transport exclusion, never evidence of remote rollback.
    let drain = tokio::time::timeout(postgres_command::DRIVER_DRAIN, &mut connection).await;
    if !matches!(drain, Ok(Ok(()))) {
        return Err(error("catalog transport did not close cleanly"));
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_cut_admits_the_full_catalog_inventory_without_inventing_facts() {
        let value: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../../../../tests/fixtures/protocol/archive-cut.json"
        ))
        .unwrap();
        let mut read: CutRead = serde_json::from_value(value["read"].clone()).unwrap();
        assert!(cut_scan_capacity(&read, 1).is_err());
        assert_eq!(cut_scan_capacity(&read, 128).unwrap(), 9);
        read.receipts.clear();
        for bound in &mut read.bounds {
            bound.complete_through = 0;
        }
        assert_eq!(cut_scan_capacity(&read, 128).unwrap(), 1);
        assert_eq!(read.validate(1), Ok(0));
        assert!(cut_scan_capacity(&read, 0).is_err());
        assert!(cut_scan_capacity(&read, 129).is_err());
    }

    #[tokio::test]
    async fn transport_loss_still_collects_the_owned_work() {
        let finished = Arc::new(AtomicBool::new(false));
        let child_finished = finished.clone();
        let mut child = tokio::task::spawn_blocking(move || {
            std::thread::sleep(Duration::from_millis(25));
            child_finished.store(true, Ordering::Release);
        });
        let driver: transport::Driver = Box::pin(async { Ok(()) });
        let result = drive(driver, async {
            (&mut child).await.map_err(error)?;
            Ok(())
        })
        .await;
        assert!(result.is_err());
        assert!(child.is_finished());
        assert!(finished.load(Ordering::Acquire));
    }
}
pub(in crate::interfaces::serve::initial_ingest) async fn connect_manager(
    prepared: PreparedConnection,
    reader: &Reader,
) -> Result<(tokio_postgres::Client, transport::Driver)> {
    let (client, driver, _) = transport::connect_pinned(prepared, Some(&reader.pinned))
        .await
        .map_err(error)?;
    Ok((client, driver))
}

pub(in crate::interfaces::serve::initial_ingest) async fn finite<T>(
    deadline: Instant,
    cancel: &AtomicBool,
    work: impl Future<Output = Result<T>>,
) -> Result<T> {
    tokio::pin!(work);
    loop {
        if cancel.load(Ordering::Acquire) || Instant::now() >= deadline {
            return Err(error("catalog operation cancelled or expired"));
        }
        tokio::select! {
            biased;
            result = &mut work => return result,
            _ = tokio::time::sleep(Duration::from_millis(25)) => {},
        }
    }
}
