//! Explicit finite initializer. Runtime startup never enters this path.
use super::{lifecycle_output::LifecycleOutput, postgres_command};
use bigbrotr::{
    runtime::{OwnedPermit, ResourceBudget},
    storage::{
        archive::{LocalInitialization, Preparation},
        postgres::{
            catalog_guard::{self, CatalogIdentity, InstallationRoles, initialization},
            connection_settings::PreparedConnection,
            transport::{self, PinnedRoute},
        },
    },
};
use serde_json::{Value, json};
use std::{
    cell::Cell,
    future::Future,
    path::PathBuf,
    process::ExitCode,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::signal::unix::{Signal, SignalKind, signal};
mod preflight;

type Result<T> = std::result::Result<T, (u8, String)>;
const SQL_DEADLINE: Duration = Duration::from_secs(30);
const NATIVE_DEADLINE: Duration = Duration::from_secs(120);

pub(super) fn command(name: &'static str) -> clap::Command {
    let command = clap::Command::new(name)
        .about(if name == "initialize" { "Explicitly initialize a fresh reserved local archive" }
            else { "Resolve a retained archive initialization without launching another worker" })
        .arg(clap::Arg::new("file").long("file").required(true)
            .value_parser(clap::value_parser!(PathBuf)).value_name("PATH"))
        .arg(clap::Arg::new("initializer-user").long("initializer-user").required(true))
        .arg(clap::Arg::new("initializer-password-ref").long("initializer-password-ref").required(true))
        .after_help("Uses a separate privileged initializer and the configured runtime manager. Requires same-boot Linux ext4 custody. Preserves permanent reservations and partial/unknown outcomes; recovery never relaunches native initialization. Exit codes: 0 installed and freshly inspected by manager, 1 local/output failure, 2 usage, 3 configuration/preflight refusal, 4 unresolved or incompatible state, 130/143 interrupted.");
    if name == "initialize" {
        command.arg(
            clap::Arg::new("worker-root")
                .long("worker-root")
                .value_parser(clap::value_parser!(PathBuf))
                .help(
                    "Canonical installed bundle; defaults to archive-worker beside this executable",
                ),
        )
    } else {
        command
    }
}

pub(super) fn execute(args: &clap::ArgMatches, initialize: bool) -> ExitCode {
    let mut output = match LifecycleOutput::start() {
        Ok(output) => output,
        Err(_) => return ExitCode::FAILURE,
    };
    let result = execute_inner(args, initialize);
    let code = match result {
        Ok(report) => {
            output.emit(report, false);
            0
        }
        Err((code, reason)) => {
            output.emit(format_args!("error: {reason}"), true);
            code
        }
    };
    // All coordinator clients and supervisors have joined before this bounded
    // diagnostic drain. TerminationUnknown may still retain a child custodian;
    // that outcome preserves the reservation and grants no further operation.
    let delivered = output.finish();
    std::process::exit(i32::from(if delivered { code } else { 1 }));
}

fn execute_inner(args: &clap::ArgMatches, initialize: bool) -> Result<Value> {
    let config = crate::config::load(args.get_one::<PathBuf>("file").unwrap())?;
    let identity = config.catalog_identity().map_err(|e| (3, e))?;
    let user = args.get_one::<String>("initializer-user").unwrap();
    let password = args.get_one::<String>("initializer-password-ref").unwrap();
    let manager = required(&config, "postgres.catalog.user")?;
    if manager.is_empty() || manager.len() > 63 || manager.chars().any(char::is_control) {
        return Err((
            3,
            "catalog manager login must contain 1 to 63 bytes without control characters".into(),
        ));
    }
    if user == manager || user.is_empty() || user.len() > 63 || user.chars().any(char::is_control) {
        return Err((
            3,
            "a separate initializer login of at most 63 bytes is required".into(),
        ));
    }
    let budget = preflight::budget(&config)?;
    let executor = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(2)
        .build()
        .map_err(|_| (1, "cannot start finite archive executor".into()))?;
    let result = executor.block_on(async {
        let mut stop = Stop::new()?;
        let pinned = if initialize {
            initialize_fresh(&config, &identity, args, user, password, &budget, &mut stop).await?
        } else {
            None
        };
        recover(
            &config,
            &identity,
            user,
            password,
            &budget,
            pinned.as_ref(),
            &mut stop,
        )
        .await
    });
    // Every blocking native supervisor was explicitly joined, including on stop.
    // Only bounded resolver work without SQL/custody can remain in the runtime.
    executor.shutdown_timeout(postgres_command::DRIVER_DRAIN);
    result
}

fn required<'a>(config: &'a crate::config::Resolved, key: &str) -> Result<&'a str> {
    config
        .text(key)
        .ok_or_else(|| (3, format!("{key}: required for archive initialization")))
}

struct Stop {
    interrupt: Signal,
    terminate: Signal,
    stopped: Option<u8>,
}
impl Stop {
    fn new() -> Result<Self> {
        Ok(Self {
            interrupt: signal(SignalKind::interrupt())
                .map_err(|_| (1, "cannot install interrupt handler".into()))?,
            terminate: signal(SignalKind::terminate())
                .map_err(|_| (1, "cannot install termination handler".into()))?,
            stopped: None,
        })
    }
    async fn run<T>(
        &mut self,
        duration: Duration,
        future: impl Future<Output = Result<T>>,
    ) -> Result<T> {
        if let Some(code) = self.stopped {
            return Err((
                code,
                "archive operation interrupted; use archive recover".into(),
            ));
        }
        tokio::select! {
            biased;
            _ = self.interrupt.recv() => { self.stopped = Some(130); Err((130, "archive operation interrupted; use archive recover".into())) },
            _ = self.terminate.recv() => { self.stopped = Some(143); Err((143, "archive operation interrupted; use archive recover".into())) },
            result = tokio::time::timeout(duration, future) => result.unwrap_or_else(|_| Err((4, "archive operation deadline exceeded; outcome unresolved, use archive recover".into()))),
        }
    }
}

struct Session {
    client: tokio_postgres::Client,
    driver: postgres_command::DriverHandle,
    _grant: OwnedPermit,
    route: PinnedRoute,
}
impl Session {
    async fn open(
        prepared: (PreparedConnection, OwnedPermit),
        custody: &LocalInitialization,
        pinned: Option<&PinnedRoute>,
        stop: &mut Stop,
    ) -> Result<Self> {
        let (prepared, grant) = prepared;
        let (client, driver, route) = stop
            .run(SQL_DEADLINE, async {
                transport::connect_pinned(prepared, pinned)
                    .await
                    .map_err(|e| (4, e.to_string()))
            })
            .await?;
        Self::retain(client, driver, route, grant, custody)
    }

    fn retain(
        client: tokio_postgres::Client,
        driver: transport::Driver,
        route: PinnedRoute,
        grant: OwnedPermit,
        custody: &LocalInitialization,
    ) -> Result<Self> {
        // The actual unpolled driver receives custody before any SQL can queue.
        let driver = custody
            .retain_catalog_connection(driver)
            .map_err(|e| (4, format!("cannot retain catalog custody: {e:?}")))?;
        Ok(Self {
            client,
            driver: tokio::spawn(driver),
            _grant: grant,
            route,
        })
    }
    async fn close(self) {
        postgres_command::close(self.client, self.driver).await;
    }
}

async fn initialize_fresh(
    config: &crate::config::Resolved,
    identity: &CatalogIdentity,
    args: &clap::ArgMatches,
    user: &str,
    password: &str,
    budget: &ResourceBudget,
    stop: &mut Stop,
) -> Result<Option<PinnedRoute>> {
    // Freeze explicit native client material before a durable reservation;
    // libpq receives sealed inherited descriptors, never ambient key paths.
    let native_identity = preflight::native_tls(config, budget)?;
    let limits = preflight::limits(config)?;
    preflight::admit_native(config, budget)?;
    let (installation, scratch, nonce) = preflight::worker(config, identity, args)?;
    let manager = postgres_command::prepare_catalog_as(
        config,
        required(config, "postgres.catalog.user")?,
        required(config, "postgres.catalog.password_ref")?,
        budget,
    )?;
    drop(manager);
    let (initializer, grant) =
        postgres_command::prepare_catalog_as(config, user, password, budget)?;
    let (client, driver, pinned) = stop
        .run(SQL_DEADLINE, async {
            transport::connect_pinned(initializer, None)
                .await
                .map_err(|e| (4, e.to_string()))
        })
        .await?;
    let native_route = preflight::NativeRoute::new(config, &pinned, native_identity)?;
    let custody = LocalInitialization::create(identity, nonce)
        .map_err(|e| (4, format!("fresh local archive reservation refused: {e:?}")))?;
    let mut session = Session::retain(client, driver, pinned.clone(), grant, &custody)?;
    let started = Cell::new(false);
    let fresh = stop
        .run(SQL_DEADLINE, async {
            initialization::reserve(
                &mut session.client,
                &custody,
                required(config, "postgres.catalog.user")?,
                &started,
            )
            .await
            .map_err(|e| {
                (
                    4,
                    format!("catalog reservation unresolved or refused: {e}; use archive recover"),
                )
            })
        })
        .await;
    session.close().await;
    let fresh = fresh?;
    let native_budget = budget.clone();
    let cancelled = Arc::new(AtomicBool::new(false));
    let child_cancelled = Arc::clone(&cancelled);
    let mut child = tokio::task::spawn_blocking(move || {
        let preparation = Preparation {
            installation: &installation,
            temp_root: &scratch,
            limits,
            deadline: Instant::now() + NATIVE_DEADLINE,
            cancelled: &child_cancelled,
        };
        let result = preparation.initialize_fresh_catalog(
            fresh,
            &custody,
            &native_route.borrowed(),
            &native_budget,
        );
        // Keep custody alive through the complete supervisor, not just the write.
        (custody, result)
    });
    let finished = stop
        .run(NATIVE_DEADLINE + Duration::from_secs(5), async {
            Ok((&mut child).await)
        })
        .await;
    let (custody, result) = match finished {
        Ok(finished) => finished.map_err(|_| {
            (
                4,
                "native supervisor failed; initialization remains unresolved".into(),
            )
        })?,
        Err(error) => {
            cancelled.store(true, Ordering::Release);
            // Aborting this JoinHandle cannot cancel a running blocking task.
            // Never release ownership while the actual supervisor remains alive.
            let _ = child.await;
            return Err(error);
        }
    };
    drop(custody);
    result.map_err(|e| {
        (
            4,
            format!("native initialization unresolved or refused: {e:?}; use archive recover"),
        )
    })?;
    Ok(Some(pinned))
}

async fn recover(
    config: &crate::config::Resolved,
    identity: &CatalogIdentity,
    user: &str,
    password: &str,
    budget: &ResourceBudget,
    pinned: Option<&PinnedRoute>,
    stop: &mut Stop,
) -> Result<Value> {
    let prepared = postgres_command::prepare_catalog_as(config, user, password, budget)?;
    let custody = LocalInitialization::recover(identity)
        .map_err(|e| (4, format!("local initialization recovery refused: {e:?}")))?;
    let mut session = Session::open(prepared, &custody, pinned, stop).await?;
    let pinned = session.route.clone();
    let result = stop.run(SQL_DEADLINE, async {
        let observed = initialization::observe_after_local_custody(&mut session.client, &custody,
            required(config, "postgres.catalog.user")?).await.map_err(guard)?
            .ok_or_else(|| (4, "no current database reservation; preserve the local marker and choose a new database/root pair".into()))?;
        let expected = observed.reservation;
        initialization::retire_after_local_custody(&mut session.client, &custody, &expected, &Cell::new(false))
            .await.map_err(guard)?;
        // Each implicit transaction gives fresh statistics. Positive local
        // client destruction precedes this drain; NOLOGIN alone is insufficient.
        loop {
            let active: bool = session.client.query_one(
                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE usesysid=$1::oid)",
                &[&expected.bootstrap_oid],
            ).await.map_err(|_| guard(catalog_guard::GuardError::Backend))?
                .try_get(0).map_err(|_| guard(catalog_guard::GuardError::Backend))?;
            if !active { break; }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let observed = initialization::observe_after_local_custody(&mut session.client, &custody,
            &expected.manager_login).await.map_err(guard)?
            .ok_or_else(|| guard(catalog_guard::GuardError::Conflict))?;
        if observed.reservation != expected || !observed.bootstrap_retired {
            return Err(guard(catalog_guard::GuardError::Conflict));
        }
        if observed.database_pristine {
            return Err((4, "bootstrap retired; pristine reservation is stranded: preserve it and choose a new database/root pair".into()));
        }
        let installed = catalog_guard::install_for_manager(&mut session.client, identity,
            InstallationRoles {
                bootstrap_login: &expected.bootstrap_login, bootstrap_oid: expected.bootstrap_oid,
                manager_login: &expected.manager_login, manager_oid: expected.manager_oid,
            }, &Cell::new(false)).await.map_err(guard)?;
        Ok(installed)
    }).await;
    session.close().await;
    let installed = result?;
    let prepared = postgres_command::prepare_catalog_as(
        config,
        required(config, "postgres.catalog.user")?,
        required(config, "postgres.catalog.password_ref")?,
        budget,
    )?;
    let mut manager = Session::open(prepared, &custody, Some(&pinned), stop).await?;
    let inspected = stop
        .run(SQL_DEADLINE, async {
            catalog_guard::inspect(&mut manager.client, identity)
                .await
                .map_err(guard)
        })
        .await;
    manager.close().await;
    inspected?;
    Ok(
        json!({"format":"bigbrotr-catalog-initialization/1", "status":"installed",
        "new_installation":installed, "dataset_id":identity.dataset(), "catalog_id":identity.catalog(),
        "bootstrap":"retired", "manager_inspection":"verified", "runtime_authorization":"not-granted",
        "ready":false}),
    )
}

fn guard(error: catalog_guard::GuardError) -> (u8, String) {
    (
        4,
        format!("{error}; initialization remains retained, use archive recover"),
    )
}
