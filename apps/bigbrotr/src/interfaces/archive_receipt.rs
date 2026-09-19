//! Finite fresh observation under the shared Control supervisor. No replay grant.
use super::{archive_preflight as preflight, postgres_command};
use crate::config::Resolved;
use bigbrotr::{
    runtime::{ResourceBudget, ResourceRequest},
    secrets::SecretFile,
    storage::{
        archive::{CatalogReaderCredentials, LocalCoordinator, Preparation, RetainedInputs},
        postgres::{archive_attempt::ArchiveAttemptReceipt, catalog_guard, transport},
    },
};
use clap::{Arg, ArgMatches, Command};
use serde_json::{Value, json};
use std::{
    future::Future,
    path::PathBuf,
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant},
};

type Result<T> = std::result::Result<T, (u8, String)>;
pub(super) const DEADLINE: Duration = Duration::from_secs(120);

pub(super) struct Options {
    pub attempt: [u8; 16],
    max_receipts: usize,
    worker_root: Option<PathBuf>,
}
impl Options {
    pub(super) fn parse(args: &ArgMatches) -> Result<Self> {
        let text = args.get_one::<String>("attempt-id").unwrap();
        let id: bigbrotr_model::DatasetId = text.parse().map_err(|_| {
            (
                3,
                "attempt ID must be nonzero lowercase hexadecimal (32 digits)".into(),
            )
        })?;
        Ok(Self {
            attempt: *id.as_bytes(),
            max_receipts: *args.get_one::<u32>("max-receipts").unwrap() as usize,
            worker_root: args.get_one::<PathBuf>("worker-root").cloned(),
        })
    }
    pub(super) fn validate_roots(config: &Resolved) -> Result<()> {
        config.catalog_identity().map_err(|e| (3, e))?;
        if !config.local("control") {
            return Err((
                3,
                "receipt inspection requires the local Control role".into(),
            ));
        }
        for key in [
            "postgres.catalog.read_user",
            "postgres.catalog.read_password_ref",
        ] {
            required(config, key)?;
        }
        Ok(())
    }
}

pub(super) fn command() -> Command {
    Command::new("receipt-inspect")
        .about("Observe one retained attempt through a fresh native catalog reader")
        .arg(Arg::new("file").long("file").required(true).value_name("PATH"))
        .arg(Arg::new("attempt-id").long("attempt-id").required(true).value_name("HEX32"))
        .arg(Arg::new("max-receipts").long("max-receipts").default_value("128")
            .value_parser(clap::value_parser!(u32).range(1..=u32::MAX as i64 - 1))
            .help("Whole-catalog receipt scan cap; must fit the accepted native allowance"))
        .arg(Arg::new("worker-root").long("worker-root").value_parser(clap::value_parser!(PathBuf)))
        .after_help("Stop the running coordinator first. Requires local Control/Archive, existing SQL and local coordinator bindings, retained complete inputs, catalog manager inspection and separate SELECT-only read_user/read_password_ref. Keeps Control authority supervised throughout finite work. Present/absent is observation only: no writer exclusion, replay, protection or reclamation. Missing inputs and incomplete scans are unresolved. Scratch is retained. Exit: 0 observed and owner released, 1 unavailable/I/O, 2 usage, 3 preflight refusal, 4 cancelled or unresolved outcome.")
}

fn required<'a>(config: &'a Resolved, key: &str) -> Result<&'a str> {
    config
        .text(key)
        .ok_or_else(|| (3, format!("{key}: required for receipt inspection")))
}

/// Runs on one owned blocking task. Its small SQL executor and actual native
/// supervisor terminate before this returns; the caller must join on every exit.
pub(super) fn inspect(
    config: Resolved,
    coordinator: LocalCoordinator,
    receipt: ArchiveAttemptReceipt,
    options: Options,
    budget: ResourceBudget,
    cancelled: &AtomicBool,
) -> Result<Value> {
    let deadline = Instant::now() + DEADLINE;
    let identity = coordinator.identity();
    let mut limits = preflight::limits(&config)?;
    limits.max_records = options.max_receipts;
    // Use the same admission floor as the actual reader, before credential I/O
    // or scratch creation. Overflow never becomes an implicitly smaller scan.
    bigbrotr::storage::archive::validate_receipt_limits(limits)
        .map_err(|e| (3, format!("receipt scan allowance refused: {e:?}")))?;
    let grant = budget
        .try_acquire(ResourceRequest::data(RetainedInputs::required_data_bytes(), 1, 0).unwrap())
        .map_err(|_| (3, "retained receipt inputs exceed the shared budget".into()))?;
    let inputs = coordinator
        .read_inputs(receipt.request(), grant, cancelled)
        .map_err(|e| (4, format!("retained receipt inputs unresolved: {e:?}")))?
        .ok_or_else(|| {
            (
                4,
                "retained receipt inputs missing; observation unresolved".into(),
            )
        })?;
    let tls = preflight::native_tls(&config, &budget)?;
    preflight::admit_native(&config, &budget)?;
    let (installation, scratch, _) = preflight::worker(
        &config,
        identity,
        options.worker_root,
        "receipt",
        limits,
        deadline,
        cancelled,
    )?;
    let manager = postgres_command::prepare_catalog_as(
        &config,
        required(&config, "postgres.catalog.user")?,
        required(&config, "postgres.catalog.password_ref")?,
        &budget,
    )?;
    let reader = required(&config, "postgres.catalog.read_user")?;
    let executor = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| (1, "cannot start finite catalog inspection executor".into()))?;
    let pinned = executor.block_on(async {
        let (prepared, grant) = manager;
        let (mut client, driver, pinned) = bounded(cancelled, deadline, async {
            transport::connect_pinned(prepared, None)
                .await
                .map_err(|e| (4, e.to_string()))
        })
        .await?;
        // Retain the actual unpolled driver, before the first SQL request.
        let driver = tokio::spawn(coordinator.retain_connection(driver).map_err(|e| {
            (
                4,
                format!("cannot retain catalog inspection custody: {e:?}"),
            )
        })?);
        let result = bounded(cancelled, deadline, async {
            catalog_guard::inspect_reader(&mut client, identity, reader)
                .await
                .map_err(|e| (4, format!("receipt catalog/reader inspection refused: {e}")))
        })
        .await;
        postgres_command::close(client, driver).await;
        drop(grant);
        result.map(|()| pinned)
    });
    executor.shutdown_timeout(postgres_command::DRIVER_DRAIN);
    let route = preflight::NativeRoute::new(&config, &pinned?, tls)?;
    // Dedicated plaintext material is charged until the actual child joins.
    // The manager credential is never handed to the native process.
    let _credential = budget
        .try_acquire(ResourceRequest::data(1024 * 1024, 1, 0).unwrap())
        .map_err(|_| (3, "receipt credential exceeds the shared budget".into()))?;
    let password = SecretFile::parse(required(&config, "postgres.catalog.read_password_ref")?)
        .and_then(|s| s.read(4096))
        .map_err(|_| (3, "cannot read protected receipt credential".into()))?;
    let password = std::str::from_utf8(password.as_bytes())
        .map_err(|_| (3, "invalid receipt credential encoding".into()))?;
    let observation = Preparation {
        installation: &installation,
        temp_root: &scratch,
        limits,
        deadline,
        cancelled,
    }
    .inspect_receipt(
        inputs.expectation(),
        identity,
        &route.borrowed(),
        CatalogReaderCredentials {
            login: reader,
            password,
        },
        &budget,
    )
    .map_err(|e| {
        (
            4,
            format!("receipt observation unresolved: {e:?}; preserve scratch and retained inputs"),
        )
    })?;
    let state = match observation {
        bigbrotr::storage::archive::ReceiptObservation::Present => "present",
        bigbrotr::storage::archive::ReceiptObservation::Absent => "absent",
    };
    let request = receipt.request();
    Ok(json!({"format":"bigbrotr-archive-receipt/1", "state":state,
        "dataset_id":request.dataset(), "catalog_id":request.catalog(),
        "attempt_id":options.attempt.iter().map(|v|format!("{v:02x}")).collect::<String>(),
        "publication_ordinal":request.ordinal().to_string(), "batch_digest":request.batch_digest(),
        "max_receipts":options.max_receipts, "catalog_inspection":"verified", "reader_privileges":"select-only",
        "writer_exclusion":"not-performed", "replay_authority":"not-granted",
        "source_protection":"not-granted", "ready":false}))
}

async fn bounded<T>(
    cancelled: &AtomicBool,
    deadline: Instant,
    future: impl Future<Output = Result<T>>,
) -> Result<T> {
    let stopped = async {
        loop {
            if cancelled.load(Ordering::Acquire) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    };
    tokio::select! {
        biased;
        _ = stopped => Err((4, "receipt catalog inspection cancelled".into())),
        value = tokio::time::timeout(deadline.saturating_duration_since(Instant::now()).min(Duration::from_secs(30)), future) =>
            value.unwrap_or_else(|_| Err((4, "receipt catalog inspection deadline exceeded".into()))),
    }
}
