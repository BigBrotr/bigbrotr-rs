//! Concrete catalog transports and native work owned by the joined publisher.
use super::*;
use bigbrotr::storage::{
    archive::{NativeClientIdentity, ObservedCut, WorkerInstallation},
    postgres::{archive_attempt::ArchiveAttemptReceipt, catalog_guard::CatalogIdentity},
};
use std::path::PathBuf;

pub(super) struct Publisher {
    connection: PreparedConnection,
    grant: OwnedPermit,
    installation: WorkerInstallation,
    scratch: PathBuf,
    limits: PreparationLimits,
}
pub(super) fn publisher(
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
pub(super) fn publish(
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

pub(super) struct Reader {
    route: preflight::NativeRoute,
    pinned: transport::PinnedRoute,
    login: String,
    password: SecretBytes,
    _permit: OwnedPermit,
}
pub(super) struct PreparedReader {
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
pub(super) fn reader(root: &Resolved, budget: &ResourceBudget) -> Result<PreparedReader> {
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
            capture.finish().await.map_err(error)?;
            Reader::connect_input(
                root,
                pinned,
                prepared.password,
                prepared.tls,
                prepared.permit,
            )
            .map(Arc::new)
        }
        .await;
        drop(manager);
        result
    })
    .await;
    drop(prepared.grant);
    result
}
pub(super) fn validate_cut(
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
pub(super) fn cut(
    root: Resolved,
    app: ResourceBudget,
    native: ResourceBudget,
    reader: Arc<Reader>,
    custody: ReaderCustody,
    read: CutRead,
    deadline: Instant,
    cancel: &AtomicBool,
) -> Result<ObservedCut> {
    let mut limits = preflight::limits(&root)?;
    limits.max_records = read.receipts.len().max(1);
    limits.max_encoded_bytes = ENCODED;
    let max_facts = usize::try_from(
        read.receipts
            .iter()
            .try_fold(0u64, |n, r| n.checked_add(r.record_count))
            .ok_or_else(|| error("cut count overflow"))?,
    )
    .map_err(error)?;
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

/// Poll the actual transport, never a detachable task. The work owns its Client
/// and destroys it on all ordinary exits. Unwind drops both futures; the custody
/// wrapper destroys its transport before closing the retained descriptor.
pub(super) async fn drive<T>(
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
pub(super) async fn connect_manager(
    prepared: PreparedConnection,
    reader: &Reader,
) -> Result<(tokio_postgres::Client, transport::Driver)> {
    let (client, driver, _) = transport::connect_pinned(prepared, Some(&reader.pinned))
        .await
        .map_err(error)?;
    Ok((client, driver))
}

pub(super) async fn finite<T>(
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
