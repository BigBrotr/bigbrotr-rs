//! First persistent role consumer. Every exit owns transport termination.
mod cursor_keys;
mod export_jobs;
mod history;
mod initial_ingest;
mod native_grants;
mod resume_export;
use super::{
    archive_preflight, archive_receipt, lifecycle_output::LifecycleOutput, management,
    postgres_command,
};
use crate::config::{Resolved, policy::PolicySeed};
use bigbrotr::{
    runtime::{
        ResourceBudget,
        ownership::fresh_process_incarnation,
        policy_authority::{LeaseResponse, PolicyLeaseTracker, PolicyLeaseTtl},
    },
    storage::postgres::{
        archive_attempt::ArchiveAttemptError,
        control_owner::{ClaimRequest, ControlSession, OwnerError, PolicySnapshot},
    },
};
use bigbrotr_model::{DatasetId, PolicyEpoch, ProcessIncarnation};
use clap::{Arg, ArgMatches, Command};
use serde_json::{Value, json};
use std::{
    cell::Cell,
    collections::BTreeSet,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    signal::unix::{Signal, SignalKind, signal},
    time::Instant,
};

type Result<T> = std::result::Result<T, (u8, String)>;

enum Operation {
    Serve,
    InitializeCoordinator,
    InspectReceipt,
    RecoverIngest,
}

pub(crate) fn ingest_command() -> Command {
    Command::new("ingest")
        .about("Explicit recovery of a retained local Ingest store")
        .subcommand_required(true)
        .disable_help_subcommand(true)
        .subcommand(Command::new("recover")
            .about("Close an explicitly selected producer and resolve retained positive outcomes")
            .arg(Arg::new("file").long("file").required(true).value_name("PATH"))
            .arg(Arg::new("close-source").long("close-source").action(clap::ArgAction::SetTrue)
                .help("Also close the initial source after complete positive resolution; does not retire or activate a successor"))
            .arg(Arg::new("abandon-acquisition").long("abandon-acquisition").action(clap::ArgAction::SetTrue)
                .conflicts_with("close-source")
                .help("Close the initial source and explicitly abandon its original queued acquisition intents; retains coverage gaps"))
            .arg(Arg::new("retire-initial").long("retire-initial").action(clap::ArgAction::SetTrue)
                .conflicts_with_all(["close-source", "abandon-acquisition"])
                .help("Retire the initial producer/domains and release Ingest after explicit source closure and acquisition abandonment; retains all detail"))
            .arg(Arg::new("retire-empty-attempt").long("retire-empty-attempt").action(clap::ArgAction::SetTrue)
                .conflicts_with_all(["close-source", "abandon-acquisition", "retire-initial"])
                .help("Retire an existing empty prepared attempt after verifying every original input and outcome; preserves all detail"))
            .arg(Arg::new("activated-successor").long("activated-successor").action(clap::ArgAction::SetTrue)
                .conflicts_with_all(["close-source", "abandon-acquisition", "retire-initial", "retire-empty-attempt"])
                .help("Close the already activated epoch2 producer and resolve retained positive outcomes under fresh Control; leaves its source open and returns no writer"))
            .arg(Arg::new("close-activated-source").long("close-activated-source").action(clap::ArgAction::SetTrue)
                .conflicts_with_all(["close-source", "abandon-acquisition", "retire-initial", "retire-empty-attempt", "activated-successor"])
                .help("Close source2 after complete positive resolution; exact closed retries preserve existing copies and no producer retirement is performed"))
            .after_help("Stop the original native owner first. Requires local Control/Ingest, the existing native store, retained original inputs and an initialized Control database. Uses fresh Control authority and current accepted policy. Preserves unresolved reservations; never infers noncommit. Starts no listener or acquisition. Default recovery leaves the source open; explicit --close-source requires all original requests and a complete native source prefix. Explicit --abandon-acquisition also records whole-intent coverage gaps and cancels up to64 original queued acquisition jobs under migration0027. Explicit --retire-initial requires migration0029 and already complete closure/abandonment; it refuses import obligations and unbound historical Events. Explicit --retire-empty-attempt requires migration0031 and the retained preparation intent; it verifies predecessor history and retires only the unactivated empty epoch2 attempt. Explicit --activated-successor requires migration0034 and the exact retained activation; it closes producer2 and resolves positive outcomes under fresh Control/current policy, preserving original inputs and recorder attribution. It leaves source2 open and cannot resume acquisition. Missing originals, unresolved workflow claims or mixed terminal state refuse. Explicit --close-activated-source requires migration0035 and proves the complete original request/source2 prefix before closing it. Closed retries require existing exact outcome copies; producer and workflow retirement remain separate. Activated source closure is under qualification. No mode acknowledges, reclaims receipts or activates a successor. Exit: 0 requested recovery completed and Control released, 1 I/O, 2 usage, 3 invalid configuration/policy, 4 cancelled/unresolved/unknown outcome. Retry the same root and mode after inspecting retained state."))
}

pub(crate) fn recover_ingest(args: &ArgMatches) -> ! {
    execute_operation(
        args.subcommand_matches("recover").unwrap(),
        Operation::RecoverIngest,
    )
}

pub(super) fn coordinator_command() -> Command {
    Command::new("coordinator-initialize")
        .about("Bind fresh Archive coordination and qualified local persistence explicitly")
        .arg(Arg::new("file").long("file").required(true).value_name("PATH"))
        .after_help("Requires local Control and Archive, the existing local state binding, migration0015 and an accepted policy. This finite command starts no listener. Uses configured Control and catalog-manager credentials. New persistence participation requires empty Control/native attempt history, the exact initial catalog with no registered data. Unregistered orphan files remain untouched. Exact retries preserve attribution; older archives need explicit recovery. No migration or readiness is implied. Stop the running coordinator first. Exit: 0 bound and owner released, 1 unavailable/I/O, 2 usage, 3 invalid/prerequisite/conflict, 4 cancelled or unknown outcome; retry identically after resolving custody.")
}

pub(super) fn initialize_coordinator(args: &ArgMatches) -> ! {
    execute_operation(args, Operation::InitializeCoordinator)
}
pub(super) fn inspect_receipt(args: &ArgMatches) -> ! {
    execute_operation(args, Operation::InspectReceipt)
}

pub(crate) fn command() -> Command {
    Command::new("serve")
        .about("Run Control or an explicitly selected Control/Ingest lifetime")
        .arg(Arg::new("file").long("file").required(true).value_name("PATH"))
        .arg(Arg::new("roles").long("roles").value_delimiter(',').num_args(1..).value_name("ROLE,..."))
        .arg(Arg::new("initialize-ingest").long("initialize-ingest").action(clap::ArgAction::SetTrue)
            .conflicts_with("activate-initial-successor")
            .help("Explicit fresh-only native initialization under retained Control; select control,ingest with optional sync and archive roles. Keeps ownership until stopped; accepted administration exposes sessions, source registration, enrollment, acquisition scopes and finite job acceptance/status. Explicit Sync selects the bounded initial acquisition executor, without coverage or job-completion claims. Explicit Archive publishes complete pending batches and captures their frontier; requires initialized fresh catalog/coordinator, reader credentials and installed worker. This publication profile is under qualification"))
        .arg(Arg::new("activate-initial-successor").long("activate-initial-successor").action(clap::ArgAction::SetTrue)
            .help("Explicitly prepare and activate epoch2 from a fully retired initial Ingest lifetime; select control,ingest with optional sync. Requires retained original inputs and complete positive predecessor outcomes. Uses current accepted policy and keeps native custody until stopped. Existing epoch2, missing retirement proof and selected Archive refuse. Starts no Archive publication; general successor recovery is not implemented. This profile is under qualification"))
        .after_help("Requires explicit initialization, migrations and initial policy activation. Configured read.cursor.keyring_ref loads existing keys before connecting and requires migration0602 plus SELECT on cursor_keyrings/cursor_key_materials. This verifies enrolled local material only, granting no cursor authority; it applies even to headless Control. Accepted administration.enabled starts the loopback private session and scope-grant API (login, me, logout, reauthentication, grant acceptance); migration0812 and session/grant/audit SQL privileges are required before binding. Grant revocation and historical acceptance retries do not need Archive. Fresh enablement requires explicit archive.reader.enabled, local Control/Archive custody, separate catalog reader credentials and the installed worker bundle. Missing native prerequisites refuse enablement; allocation reads do not authorize history delivery. The initial private GET/POST /v1/resources/event_history profile additionally requires a live session, explicit CSRF and history.read for its exact scope; one exact ID/snapshot, configured resource limits, and a certified archive pin apply. This profile is under qualification. POST /management/v1/jobs accepts private exact-ID signed JSONL export jobs; configured initialized export.destination requires migration0813 and job/pin/source-lock privileges before binding. Historical receipts on the available protocol need no current destination or native reader. Destination checks run on supervised blocking work, retained through Control driver join. Selected export.destination plus archive.reader.enabled also requires migration0501 and execution privileges before startup, including headless Control. POST /management/v1/jobs/{job_id}/resume is under qualification with migration0814 and explicit SQL privileges; it queues the same paused unit after current grants/destination/CAS, without a fresh reader pin. Historical retries preserve the original receipt. The initial automatic exact-ID signed JSONL executor is under qualification; it uses current actor grants and the original retained snapshot. Receipts report acceptance only, never completed restrictive drain. Accepted policy changes require restart in this slice. SIGINT/SIGTERM stop admission and join HTTP/password work before owner release. Exit: 0 stopped, 1 unavailable/I/O, 2 usage, 3 invalid configuration/policy, 4 cancelled or unknown durable outcome.")
}

pub(crate) fn execute(args: &ArgMatches) -> ! {
    execute_operation(args, Operation::Serve)
}

fn execute_operation(args: &ArgMatches, operation: Operation) -> ! {
    let Ok(mut output) = LifecycleOutput::start() else {
        std::process::exit(1);
    };
    let outcome = (|| {
        let root = crate::config::load(Path::new(args.get_one::<String>("file").unwrap()))?;
        if matches!(operation, Operation::RecoverIngest) {
            use initial_ingest::recovery::Action;
            let action = if args.get_flag("close-activated-source") {
                Action::CloseActivatedSource
            } else if args.get_flag("activated-successor") {
                Action::ActivatedSuccessor
            } else if args.get_flag("retire-empty-attempt") {
                Action::RetireEmptyAttempt
            } else if args.get_flag("retire-initial") {
                Action::RetireInitial
            } else if args.get_flag("abandon-acquisition") {
                Action::AbandonAcquisition
            } else if args.get_flag("close-source") {
                Action::CloseSource
            } else {
                Action::Resolve
            };
            return initial_ingest::recovery::run(root, action, &mut output);
        }
        if matches!(operation, Operation::InitializeCoordinator) {
            root.catalog_identity().map_err(|e| (3, e))?;
            if !root.local("control") {
                return Err((
                    3,
                    "coordinator initialization requires the local Control role".into(),
                ));
            }
            return run(root, &mut output, operation, None);
        }
        if matches!(operation, Operation::InspectReceipt) {
            archive_receipt::Options::validate_roots(&root)?;
            let options = archive_receipt::Options::parse(args)?;
            return run(root, &mut output, operation, Some(options));
        }
        let requested: Vec<&str> = args
            .get_many::<String>("roles")
            .map(|values| values.map(String::as_str).collect())
            .unwrap_or_else(|| root.local_roles.iter().map(String::as_str).collect());
        let distinct: BTreeSet<_> = requested.iter().copied().collect();
        if distinct.is_empty()
            || distinct.len() != requested.len()
            || distinct.iter().any(|role| !root.local(role))
        {
            return Err((
                3,
                "invalid role selection: require distinct roles assigned in the deployment root"
                    .into(),
            ));
        }
        if args.get_flag("activate-initial-successor") {
            if distinct != BTreeSet::from(["control", "ingest"])
                && distinct != BTreeSet::from(["control", "ingest", "sync"])
            {
                return Err((
                    3,
                    "initial successor requires control,ingest with optional sync; Archive is unsupported"
                        .into(),
                ));
            }
            let sync = distinct.contains("sync");
            return initial_ingest::successor::run(root, sync, &mut output);
        }
        if args.get_flag("initialize-ingest") {
            let sync = distinct.contains("sync");
            let archive = distinct.contains("archive");
            if distinct != BTreeSet::from(["control", "ingest"])
                && distinct != BTreeSet::from(["control", "ingest", "sync"])
                && distinct != BTreeSet::from(["control", "ingest", "archive"])
                && distinct != BTreeSet::from(["control", "ingest", "sync", "archive"])
            {
                return Err((
                    3,
                    "initial Ingest requires control,ingest with optional sync and archive roles"
                        .into(),
                ));
            }
            return initial_ingest::run(root, sync, archive, &mut output);
        }
        if distinct.iter().any(|role| *role != "control") {
            return Err((
                3,
                "selected role is not implemented by this runtime; select only control explicitly"
                    .into(),
            ));
        }
        run(root, &mut output, operation, None)
    })();
    let mut code = match outcome {
        Ok(()) => 0,
        Err((code, message)) => {
            output.emit(format_args!("error: {message}"), true);
            code
        }
    };
    if !output.finish() && code == 0 {
        code = 1;
    }
    // run() has closed/joined backend transports. Exit also terminates a blocked
    // diagnostics thread or a password worker whose bounded join failed; that
    // latter path cannot report graceful owner release.
    std::process::exit(i32::from(code));
}

struct Stop {
    interrupt: Signal,
    terminate: Signal,
}
impl Stop {
    fn new() -> Result<Self> {
        let open =
            |kind| signal(kind).map_err(|_| (1, "cannot install runtime stop handler".into()));
        Ok(Self {
            interrupt: open(SignalKind::interrupt())?,
            terminate: open(SignalKind::terminate())?,
        })
    }
    async fn requested(&mut self) {
        // recv is cancellation safe; registration remains until process exit.
        tokio::select! {
            biased;
            _ = self.terminate.recv() => {},
            _ = self.interrupt.recv() => {},
        }
    }
}

struct ArchiveRun {
    coordinator: Option<bigbrotr::storage::archive::LocalCoordinator>,
    receipt: Option<(archive_receipt::Options, ResourceBudget)>,
    initialization_budget: Option<ResourceBudget>,
    management_budget: Option<ResourceBudget>,
    management_assets: Option<management::Assets>,
    export_destination: Option<Arc<bigbrotr::export::destination::LocalDestination>>,
    export_custody: export_jobs::Custody,
    cursor_keys: Option<Arc<bigbrotr::read::keyring::Keyring>>,
}

fn run(
    root: Resolved,
    output: &mut LifecycleOutput,
    operation: Operation,
    receipt: Option<archive_receipt::Options>,
) -> Result<()> {
    // Acquire before Control startup or any future issuance. The stable archive
    // anchor prevents choosing another state root to bypass an old coordinator.
    // Retain it through every actual driver join, including cancelled startup.
    let coordinator = if root.local("archive") {
        let identity = root.catalog_identity().map_err(|e| (3, e))?;
        let state = root.archive_state_path().map_err(|e| (4, e))?;
        Some(bigbrotr::storage::archive::LocalCoordinator::open(&identity, &state)
            .map_err(|e| (4, format!("archive coordinator custody refused: {e:?}; explicit archive state-initialize is required")))?)
    } else {
        None
    };
    let required = |name| {
        root.text(name)
            .ok_or_else(|| (3, "missing Control deployment root".into()))
    };
    let dataset: DatasetId = required("topology.dataset_id")?
        .parse()
        .map_err(|_| (3, "invalid dataset identity".into()))?;
    let instance: DatasetId = required("topology.instance_id")?
        .parse()
        .map_err(|_| (3, "invalid instance identity".into()))?;
    let incarnation = fresh_process_incarnation().map_err(|e| (1, e.to_string()))?;
    let role = fresh_process_incarnation().map_err(|e| (1, e.to_string()))?;
    // Both complete stored documents are revalidated against an immutable copy
    // of startup roots. The file is never reread or implicitly activated.
    // Control, retained inputs, native TLS, reader credential and worker overlap.
    // Five bounded items share disjoint application/native process lanes.
    let receipt = receipt
        .map(|options| Ok((options, archive_preflight::budget(&root, 5)?)))
        .transpose()?;
    let initialization_budget = matches!(operation, Operation::InitializeCoordinator)
        .then(|| archive_preflight::budget(&root, 2))
        .transpose()?;
    // Accepted managed policy, not file drift, selects the eventual listener.
    // Prepare one envelope even when the file currently says disabled.
    let management_budget = matches!(operation, Operation::Serve)
        .then(|| management::budget(&root))
        .transpose()?;
    let cursor_keys = management_budget
        .as_ref()
        .map(|budget| cursor_keys::load(&root, dataset, budget))
        .transpose()?
        .flatten();
    // Explicitly configured bundles are startup prerequisites even when the
    // later accepted policy disables the listener. Never read disk under lease.
    let management_assets = management_budget
        .as_ref()
        .map(|budget| management::prepare_assets(&root, budget))
        .transpose()?
        .flatten();
    let export_destination = matches!(operation, Operation::Serve)
        .then(|| super::export::prepare_destination(&root))
        .transpose()?
        .flatten();
    let export_custody = Arc::new(std::sync::Mutex::new(None));
    let shared_budget = receipt
        .as_ref()
        .map(|(_, budget)| budget)
        .or(initialization_budget.as_ref())
        .or(management_budget.as_ref());
    let (prepared, grant) = if let Some(budget) = shared_budget {
        postgres_command::prepare_control_shared(
            &root,
            required("postgres.control.user")?,
            required("postgres.control.password_ref")?,
            budget,
        )?
    } else {
        postgres_command::prepare(
            &root,
            required("postgres.control.user")?,
            required("postgres.control.password_ref")?,
            512 * 1024 * 1024,
        )?
    };
    let request = ClaimRequest {
        dataset,
        instance,
        authority_mode: required("config.authority_mode")?.to_owned(),
        role_instance_id: *role.as_bytes(),
        incarnation,
        lease_ttl: duration(&root, "lease_ttl"),
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(2)
        .build()
        .map_err(|_| (1, "cannot start Control runtime".into()))?;
    let result = runtime.block_on(async {
        let mut stop = Stop::new()?;
        let (client, driver) = tokio::select! {
            biased;
            _ = stop.requested() => return Err((4, "Control startup cancelled before connection".into())),
            result = bigbrotr::storage::postgres::transport::connect(prepared) => result.map_err(|e| (1, e.to_string()))?,
        };
        // Attach the actual unpolled driver before any SQL can be queued. This
        // also preserves exclusion if future supervision code drops its owner.
        let driver = if let Some(custody) = &coordinator {
            tokio::spawn(custody.retain_connection(driver).map_err(|e| (4, format!("cannot retain archive coordinator custody: {e:?}")))?)
        } else { tokio::spawn(driver) };
        let outcome = supervise(client, request, &root, &mut stop, output, ArchiveRun { coordinator, receipt, initialization_budget, management_budget, management_assets,
            export_destination: export_destination.clone(), export_custody: export_custody.clone(),
            cursor_keys: cursor_keys.clone() }, operation).await;
        // No error, cancellation, output failure or unknown COMMIT can bypass
        // the join. Dropping the request/session alone does not free its grant.
        postgres_command::join(driver).await;
        // The session/operation may already be gone. Its snapshot and charged
        // bytes remain alive until the actual transport is terminal.
        drop(cursor_keys);
        // The checked observation/SQL future may already have been dropped.
        // Original destination custody survives every unknown COMMIT and join.
        drop(export_custody);
        drop(export_destination);
        outcome
    });
    runtime.shutdown_timeout(postgres_command::DRIVER_DRAIN);
    let result = result.map(|value| output.emit(value, false));
    drop(grant);
    result
}

async fn supervise(
    client: tokio_postgres::Client,
    request: ClaimRequest,
    root: &Resolved,
    stop: &mut Stop,
    output: &mut LifecycleOutput,
    archive: ArchiveRun,
    operation: Operation,
) -> Result<Value> {
    let dataset = request.dataset;
    let incarnation = request.incarnation;
    let ttl = duration(root, "authorization_ttl");
    let mut lease = PolicyLeaseTracker::new(
        dataset,
        incarnation,
        PolicyEpoch::new(1),
        PolicyLeaseTtl::new(ttl).map_err(|e| (3, e.to_string()))?,
    )
    .map_err(|e| (1, e.to_string()))?;
    let mut accepted = None;
    let validate = |document: &str, digest| {
        accepted = Some(accepted_policy(root, document, digest)?);
        Ok(())
    };
    let ticket = lease.begin_refresh().map_err(|e| (1, e.to_string()))?;
    let mut renewed = Instant::now();
    let commit = Cell::new(false);
    let acquired = tokio::select! {
        biased;
        _ = stop.requested() => Err(if commit.get() { OwnerError::CommitUnknown } else { OwnerError::Unavailable }),
        result = tokio::time::timeout(duration(root, "renewal_timeout"), ControlSession::acquire(client, request, validate, &commit)) =>
            result.unwrap_or_else(|_| Err(if commit.get() { OwnerError::CommitUnknown } else { OwnerError::Backend })),
    };
    let (mut owner, mut snapshot) = acquired.map_err(owner_error)?;
    let accepted_snapshot = snapshot;
    lease
        .complete_refresh(&ticket, response(dataset, incarnation, snapshot, ttl))
        .map_err(|e| (1, e.to_string()))?;
    lease
        .check_delivery(lease.known_epoch())
        .map_err(|e| (1, e.to_string()))?;
    let identity = owner.identity();
    if let Some(keys) = archive.cursor_keys {
        tokio::select! {
            biased;
            _ = stop.requested() => return Err((4, "cursor key enrollment check cancelled".into())),
            result = tokio::time::timeout(duration(root, "renewal_timeout")
                .min(lease.remaining_validity().map_err(|e| (4,e.to_string()))?), owner.bind_cursor_keys(keys)) => {
                result.map_err(|_| (4, "cursor key enrollment check deadline exceeded".into()))?
                    .map_err(|error| (owner_error(error).0, format!("cursor key enrollment refused: {error}")))?;
            }
        }
        lease
            .check_delivery(lease.known_epoch())
            .map_err(|e| (4, e.to_string()))?;
    }
    if let Some(coordinator) = &archive.coordinator {
        let commit = Cell::new(false);
        let binding = async {
            if matches!(operation, Operation::InitializeCoordinator) {
                owner
                    .initialize_archive_coordinator(coordinator, &commit)
                    .await
            } else {
                owner
                    .require_archive_coordinator(coordinator)
                    .await
                    .map(|()| false)
            }
        };
        let created = tokio::select! {
            biased;
            _ = stop.requested() => Err(if commit.get() { ArchiveAttemptError::CommitUnknown } else { ArchiveAttemptError::Unavailable }),
            result = tokio::time::timeout(duration(root, "renewal_timeout").min(lease.remaining_validity().map_err(|e| (1, e.to_string()))?), binding) =>
                result.unwrap_or_else(|_| Err(if commit.get() { ArchiveAttemptError::CommitUnknown } else { ArchiveAttemptError::Backend })),
        }.map_err(|error| {
            let (code, _) = attempt_error(error);
            (code, format!("archive coordinator protocol refused: {error:?}; explicit coordinator-initialize requires matching state and empty pre-protocol history"))
        })?;
        lease
            .check_delivery(lease.known_epoch())
            .map_err(|e| (1, e.to_string()))?;
        if matches!(operation, Operation::InitializeCoordinator) {
            let accepted = accepted
                .as_ref()
                .ok_or_else(|| (3, "accepted initialization policy unavailable".into()))?;
            let persistence_created = initialize_persistence(
                accepted,
                &mut owner,
                coordinator,
                archive
                    .initialization_budget
                    .as_ref()
                    .expect("initializer budget"),
                stop,
                duration(root, "renewal_timeout")
                    .min(lease.remaining_validity().map_err(|e| (4, e.to_string()))?),
            )
            .await?;
            lease
                .check_delivery(lease.known_epoch())
                .map_err(|e| (4, e.to_string()))?;
            tokio::time::timeout(duration(root, "graceful_shutdown_timeout"), owner.release())
                .await
                .unwrap_or(Err(OwnerError::CommitUnknown))
                .map_err(owner_error)?;
            return Ok(
                json!({"format":"bigbrotr-coordinator-initialization/1", "state":"bound",
                "created":created, "dataset_id":dataset, "catalog_id":coordinator.identity().catalog(),
                "binding_sha256":coordinator.binding_digest().to_string(), "owner_release":"committed",
                "catalog_inspection":"verified", "persistence_protocol":1,
                "persistence_created":persistence_created, "publication_authority":"not-granted", "ready":false}),
            );
        }
    }
    if let Some((options, budget)) = archive.receipt {
        let coordinator = archive
            .coordinator
            .expect("receipt roots require local Archive");
        let receipt = tokio::select! {
            biased;
            _ = stop.requested() => return Err((4, "receipt request lookup cancelled".into())),
            value = tokio::time::timeout(duration(root, "renewal_timeout")
                .min(lease.remaining_validity().map_err(|e| (1,e.to_string()))?),
                owner.read_archive_attempt(coordinator.identity().catalog(), options.attempt)) =>
                value.map_err(|_| (4, "receipt request lookup deadline exceeded".into()))?.map_err(attempt_error)?,
        }.ok_or_else(|| (4, "retained Control attempt missing; receipt observation unresolved".into()))?;
        let accepted = accepted.ok_or_else(|| (3, "accepted receipt policy unavailable".into()))?;
        lease
            .check_delivery(lease.known_epoch())
            .map_err(|e| (4, e.to_string()))?;
        let cancelled = Arc::new(AtomicBool::new(false));
        let child_cancelled = Arc::clone(&cancelled);
        let mut child = tokio::task::spawn_blocking(move || {
            archive_receipt::inspect(
                accepted,
                coordinator,
                receipt,
                options,
                budget,
                &child_cancelled,
            )
        });
        let deadline = Instant::now() + archive_receipt::DEADLINE;
        let mut joined = false;
        let mut outcome = async {
            loop {
                let remaining = lease.remaining_validity().map_err(|e| (4,e.to_string()))?;
                let period = (remaining / 2).min(duration(root,"heartbeat_interval").saturating_sub(renewed.elapsed()));
                tokio::select! {
                    biased;
                    _ = stop.requested() => return Err((4, "receipt inspection cancelled; observation unresolved".into())),
                    _ = tokio::time::sleep_until(deadline) => return Err((4, "receipt inspection deadline exceeded; observation unresolved".into())),
                    result = &mut child => {
                        joined = true;
                        lease.check_delivery(PolicyEpoch::new(snapshot.policy_revision)).map_err(|e| (4,e.to_string()))?;
                        return result.map_err(|_| (4, "receipt supervisor failed; observation unresolved".into()))?;
                    },
                    _ = tokio::time::sleep(period) => {},
                }
                let current = refresh_authority(&mut owner, &mut lease, &mut renewed, dataset, root, stop).await?;
                if current != snapshot {
                    return Err((4, "accepted policy changed during receipt inspection; retry under current policy".into()));
                }
            }
        }.await;
        // Cancellation cannot abort spawn_blocking. Join even when a task has
        // finished between polls, and preserve unknown native termination detail.
        cancelled.store(true, Ordering::Release);
        if !joined {
            let cleanup = match child.await {
                Ok(Ok(_)) => None,
                Ok(Err((_, detail))) => Some(detail),
                Err(_) => Some("receipt supervisor join failed".into()),
            };
            if let (Err((_, reason)), Some(detail)) = (&mut outcome, cleanup) {
                reason.push_str("; ");
                reason.push_str(&detail);
            }
        }
        let mut report = outcome?;
        tokio::time::timeout(duration(root, "graceful_shutdown_timeout"), owner.release())
            .await
            .unwrap_or(Err(OwnerError::CommitUnknown))
            .map_err(owner_error)?;
        report["owner_release"] = json!("committed");
        report["policy_revision"] = json!(snapshot.policy_revision.to_string());
        return Ok(report);
    }
    // Control owns the retained issuance history even when Archive is omitted
    // from this process selection. Inventory is observation, never a terminal
    // result: every observed intent still needs its catalog/custody resolution.
    let archive_inventory = if root.local("archive") {
        let catalog = root.catalog_identity().map_err(|e| (3, e))?.catalog();
        let mut scan = tokio::select! {
            biased;
            _ = stop.requested() => return Err((4, "archive intent inventory cancelled".into())),
            result = tokio::time::timeout(
                duration(root, "renewal_timeout").min(lease.remaining_validity().map_err(|e| (1, e.to_string()))?),
                owner.begin_archive_attempt_scan(catalog)
            ) => result.map_err(|_| (1, "archive intent inventory deadline exceeded".into()))?
                .map_err(attempt_error)?,
        };
        let mut pages = 0_u64;
        while !scan.is_complete() {
            refresh_authority(&mut owner, &mut lease, &mut renewed, dataset, root, stop).await?;
            // Drop each bounded page before polling again; no history-sized
            // collection or transaction survives a supervision boundary.
            tokio::select! {
                biased;
                _ = stop.requested() => return Err((4, "archive intent inventory cancelled".into())),
                result = tokio::time::timeout(
                    duration(root, "renewal_timeout").min(lease.remaining_validity().map_err(|e| (1, e.to_string()))?),
                    owner.read_archive_attempt_page(&mut scan)
                ) => { result.map_err(|_| (1, "archive intent inventory deadline exceeded".into()))?
                    .map_err(attempt_error)?; },
            }
            pages += 1;
        }
        snapshot =
            refresh_authority(&mut owner, &mut lease, &mut renewed, dataset, root, stop).await?;
        json!({"state":"observed", "catalog_id":catalog,
            "issued_through":scan.high_water().to_string(), "observed_through":scan.observed_through().to_string(),
            "pages":pages, "resolution":"not-performed", "publication_authority":"not-granted"})
    } else {
        json!({"state":"not-selected", "resolution":"not-performed", "publication_authority":"not-granted"})
    };
    if snapshot != accepted_snapshot {
        return Err((
            4,
            "accepted policy changed during startup; restart required".into(),
        ));
    }
    let accepted =
        Arc::new(accepted.ok_or_else(|| (3, "accepted runtime policy unavailable".into()))?);
    let asset_observation = archive
        .management_assets
        .as_ref()
        .map(management::Assets::observation);
    let shared_budget = archive
        .management_budget
        .as_ref()
        .expect("serve budget")
        .clone();
    let execute_exports =
        archive.export_destination.is_some() && accepted.flag("archive.reader.enabled");
    if execute_exports {
        tokio::select! {
            biased;
            _ = stop.requested() => return Err((4,"export startup cancelled".into())),
            result = tokio::time::timeout(duration(root,"renewal_timeout").min(lease.remaining_validity().map_err(|e| (4,e.to_string()))?),owner.require_export_execution()) =>
                result.map_err(|_| (4,"export prerequisites deadline".into()))?.map_err(|e| (3,format!("export prerequisites refused: {e:?}")))?,
        }
    }
    let mut management = if accepted.flag("administration.enabled") {
        let mut export_jobs = false;
        let mut export_resume = false;
        let mut job_pages = false;
        tokio::select! {
            biased;
            _ = stop.requested() => return Err((4,"private session startup cancelled".into())),
            result = tokio::time::timeout(duration(root,"renewal_timeout")
                .min(lease.remaining_validity().map_err(|e| (4,e.to_string()))?), async {
                    owner.require_browser_sessions().await?;
                    owner.require_scope_grants().await?;
                    export_jobs = owner.export_jobs_available().await?;
                    export_resume = owner.export_resume_available().await?;
                    job_pages = owner.job_pages_available().await?;
                    if archive.export_destination.is_some() && !export_jobs {
                        return Err(bigbrotr::management::sessions::SessionError::Prerequisite);
                    }
                    if accepted.flag("archive.reader.enabled") {
                        owner.require_archive_reader().await.map_err(|_| bigbrotr::management::sessions::SessionError::Prerequisite)?;
                    }
                    Ok::<_, bigbrotr::management::sessions::SessionError>(())
                }) => {
                result.map_err(|_| (4,"private session prerequisites deadline".into()))?
                    .map_err(|e| (3,format!("private session prerequisites refused: {e:?}")))?;
            }
        }
        lease
            .check_delivery(lease.known_epoch())
            .map_err(|e| (4, e.to_string()))?;
        Some(
            management::Management::start(
                &accepted,
                archive.management_budget.expect("serve budget"),
                lease
                    .delivery_window(lease.known_epoch())
                    .map_err(|e| (4, e.to_string()))?,
                archive.management_assets,
                management::Profile::Operational {
                    export_jobs,
                    export_resume,
                    job_pages,
                },
            )
            .await?,
        )
    } else {
        drop(archive.management_assets);
        None
    };
    let listeners = if management.is_some() {
        json!([{"kind":"private-session-http", "host":accepted.text("administration.listen.host"),
            "port":accepted.number("administration.listen.port"), "profile":"trusted-host-loopback",
            "private_assets":asset_observation}])
    } else {
        json!([])
    };
    output.emit(
        json!({"format":"bigbrotr-control-runtime/1", "state":"idle", "role":"control",
        "dataset_id":dataset, "process_incarnation":incarnation,
        "owner_resource_id":identity.resource_id, "owner_epoch":identity.epoch.get().to_string(),
        "config_revision":snapshot.config_revision.to_string(), "policy_revision":snapshot.policy_revision.to_string(),
        "archive_intents":archive_inventory, "listeners":listeners, "native_stores":[], "cursor_keyring":cursor_keys::observation(owner.enrolled_cursor_keys()), "export_execution":if execute_exports { "selected-under-qualification" } else { "not-selected" }, "authority_scope":"Control custody; each private action and export independently reauthorized"}),
        false,
    );
    let heartbeat = duration(root, "heartbeat_interval");
    let outcome = async {
    let mut refresh_due = Instant::now() + (lease.remaining_validity().map_err(|e| (1,e.to_string()))? / 2)
        .min(heartbeat.saturating_sub(renewed.elapsed()));
    // The dispatch loop and its native consumers share one authority context.
    // In particular, a stop consumed by a nested consumer remains visible here.
    let mut supervisor = native_grants::Supervisor {
        owner: &mut owner,
        lease: &mut lease,
        renewed: &mut renewed,
        refresh_due: &mut refresh_due,
        dataset,
        snapshot,
        root,
        stop,
        management: &mut management,
        stopping: false,
    };
    let mut export_after = None;
    let mut export_due = Instant::now();
    loop {
        supervisor.lease.check_delivery(supervisor.lease.known_epoch()).map_err(|e| (1,e.to_string()))?;
        // A slow successful response already consumed part of the request-start
        // lifetime. Neither that delay nor output handling may extend authority.
        tokio::select! {
            biased;
            _ = supervisor.stop.requested() => return Ok(()),
            // Renewal wins a continuously ready queue. HTTP cannot starve it.
            _ = tokio::time::sleep_until(*supervisor.refresh_due) => {
                let current = {
                    let started = Cell::new(false);
                    let renew = supervisor.renewed.elapsed() >= heartbeat;
                    let refresh = async {
                        started.set(true);
                        collect_refresh(supervisor.owner, supervisor.lease, supervisor.renewed, dataset, root, renew).await
                    };
                    tokio::pin!(refresh);
                    tokio::select! {
                        biased;
                        _ = supervisor.stop.requested() => {
                            if let Some(management) = supervisor.management.as_ref() { management.stop_admission(); }
                            // Keep the same admitted operation and its original
                            // deadline. Dropping it would poison an otherwise
                            // healthy Control session. Never resume dispatch or
                            // renew delivery after consuming this stop signal.
                            if started.get() { refresh.await?; }
                            return Ok(());
                        },
                        result = &mut refresh => result?,
                    }
                };
                if current != snapshot {
                    return Err((4,"accepted policy changed; private runtime restart required".into()));
                }
                if let Some(management) = supervisor.management.as_ref() {
                    management.renew_delivery(supervisor.lease.delivery_window(supervisor.lease.known_epoch()).map_err(|e| (4,e.to_string()))?)?;
                }
                // Preserve this absolute deadline across every HTTP command.
                // Recreating half of the remaining lifetime for each ready item
                // would repeatedly postpone renewal under continuous traffic.
                *supervisor.refresh_due = Instant::now() + (supervisor.lease.remaining_validity().map_err(|e| (1,e.to_string()))? / 2)
                    .min(heartbeat.saturating_sub(supervisor.renewed.elapsed()));
            },
            _ = tokio::time::sleep_until(export_due), if execute_exports => {
                let maximum = duration(root,"renewal_timeout").min(supervisor.lease.remaining_validity().map_err(|e| (4,e.to_string()))? / 2);
                let selected = tokio::select! {
                    biased;
                    _ = supervisor.stop.requested() => return Err((4,"export queue lookup cancelled; Control unavailable".into())),
                    result = tokio::time::timeout(maximum,supervisor.owner.next_export_job(export_after)) => result.map_err(|_| (4,"export queue lookup deadline".into()))?.map_err(|e| (4,format!("export queue unavailable: {e:?}")))?,
                };
                // Advance even after a known denial; reset only on exhaustion.
                export_after = selected;
                if let Some(id) = selected {
                    export_jobs::execute(id,accepted.clone(),shared_budget.clone(),archive.export_destination.as_ref().expect("selected destination").clone(),archive.coordinator.as_ref().expect("selected reader"),&archive.export_custody,&mut supervisor).await?;
                    if supervisor.stopping { return Ok(()); }
                }
                // The ready job wins HTTP at most once per bounded interval;
                // native supervision continues servicing existing Control work.
                export_due = Instant::now() + Duration::from_millis(250);
            },
            work = next_management(supervisor.management) => {
                supervisor.lease.check_delivery(supervisor.lease.known_epoch()).map_err(|e| (4,e.to_string()))?;
                let maximum = duration(root,"renewal_timeout").min(supervisor.lease.remaining_validity().map_err(|e| (4,e.to_string()))? / 2);
                let response = tokio::select! {
                    biased;
                    _ = supervisor.stop.requested() => return Err((4,"private session operation cancelled; outcome unresolved".into())),
                    result = supervisor.management.as_mut().expect("management work source").execute(work,supervisor.owner,supervisor.lease,maximum,accepted.flag("archive.reader.enabled"),archive.export_destination.as_ref()) => result?,
                };
                let response = match response {
                    management::Execution::Complete(response) => response,
                    management::Execution::Resume(work) => {
                        let response = resume_export::execute(work, &shared_budget, &mut supervisor).await?;
                        if supervisor.stopping { return Ok(()); }
                        Some(response)
                    }
                    management::Execution::History(work) => {
                        history::execute(work,accepted.clone(),shared_budget.clone(),archive.coordinator.as_ref(),&mut supervisor).await?;
                        if supervisor.stopping { return Ok(()); }
                        None
                    }
                    management::Execution::Archive(work) => {
                        let response = native_grants::execute(work,accepted.clone(),shared_budget.clone(),
                            archive.coordinator.as_ref().expect("selected local reader"),&mut supervisor).await?;
                        // The nested supervisor consumed the signal. Cleanup is
                        // now known complete; never admit or deliver more work.
                        if supervisor.stopping { return Ok(()); }
                        Some(response)
                    }
                };
                // Tokio deadlines alone do not detect every suspend/resume.
                // Keep the response/grant until the authority clock agrees.
                supervisor.lease.check_delivery(supervisor.lease.known_epoch()).map_err(|e| (4,e.to_string()))?;
                if let Some(response) = response { response.deliver(); }
            },
        }
    }}.await;
    // Every error after binding takes this path. Actual sockets and password
    // tasks terminate before releasing the owner or dropping its SQL session.
    if let Some(management) = &mut management {
        management
            .stop(duration(root, "graceful_shutdown_timeout"))
            .await?;
    }
    outcome?;
    tokio::time::timeout(duration(root, "graceful_shutdown_timeout"), owner.release())
        .await
        .unwrap_or(Err(OwnerError::CommitUnknown))
        .map_err(owner_error)?;
    Ok(
        json!({"format":"bigbrotr-control-runtime/1", "state":"stopped", "role":"control",
        "owner_epoch":identity.epoch.get().to_string(), "policy_revision":snapshot.policy_revision.to_string(),
        "release":"committed"}),
    )
}

async fn next_management(management: &mut Option<management::Management>) -> management::Work {
    match management {
        Some(management) => management.next().await,
        None => std::future::pending().await,
    }
}

async fn initialize_persistence(
    config: &Resolved,
    owner: &mut ControlSession,
    coordinator: &bigbrotr::storage::archive::LocalCoordinator,
    budget: &ResourceBudget,
    stop: &mut Stop,
    remaining: Duration,
) -> Result<bool> {
    use bigbrotr::storage::postgres::{catalog_guard::CatalogCapture, transport};
    let deadline = Instant::now() + remaining.min(Duration::from_secs(30));
    let required = |key| {
        config
            .text(key)
            .ok_or_else(|| (3, format!("{key}: required for persistence initialization")))
    };
    let (prepared, grant) = postgres_command::prepare_catalog_as(
        config,
        required("postgres.catalog.user")?,
        required("postgres.catalog.password_ref")?,
        budget,
    )?;
    let (mut manager, connection) = tokio::select! {
        biased;
        _ = stop.requested() => return Err((4,"persistence initialization cancelled before catalog connection".into())),
        result = tokio::time::timeout_at(deadline, transport::connect(prepared)) =>
            result.map_err(|_| (4,"persistence catalog connection deadline".into()))?
                .map_err(|_| (1,"persistence catalog connection unavailable".into()))?,
    };
    let driver = tokio::spawn(
        coordinator
            .retain_connection(connection)
            .map_err(|_| (4, "cannot retain persistence catalog custody".into()))?,
    );
    let commit = Cell::new(false);
    let outcome = {
        let operation = async {
            let duration = deadline.saturating_duration_since(Instant::now());
            let capture = CatalogCapture::open(&mut manager, coordinator, duration)
                .await
                .map_err(|e| (4, format!("persistence catalog barrier refused: {e:?}")))?;
            let created = owner
                .initialize_archive_persistence(&capture, &commit)
                .await
                .map_err(attempt_error)?;
            capture.finish().await.map_err(|_| {
                (
                    4,
                    "persistence catalog barrier termination unresolved".into(),
                )
            })?;
            Ok(created)
        };
        tokio::select! {
            biased;
            _ = stop.requested() => Err((4,format!("persistence initialization cancelled; commit_started={}",commit.get()))),
            result = tokio::time::timeout_at(deadline, operation) => result.unwrap_or_else(|_| Err((4,format!("persistence initialization deadline; commit_started={}",commit.get())))),
        }
    };
    // Dropping a cancelled operation only queues rollback; retain the actual
    // driver's custody and grant through transport destruction before returning.
    drop(manager);
    postgres_command::join(driver).await;
    drop(grant);
    outcome
}

async fn refresh_authority(
    owner: &mut ControlSession,
    lease: &mut PolicyLeaseTracker,
    renewed: &mut Instant,
    dataset: DatasetId,
    root: &Resolved,
    stop: &mut Stop,
) -> Result<PolicySnapshot> {
    let renew = renewed.elapsed() >= duration(root, "heartbeat_interval");
    tokio::select! {
        biased;
        _ = stop.requested() => Err(owner_error(if renew { OwnerError::CommitUnknown } else { OwnerError::Unavailable })),
        result = collect_refresh(owner, lease, renewed, dataset, root, renew) => result,
    }
}

/// Collect one policy operation without resetting its request-start authority
/// or timeout. Callers choose cancellation; only the idle loop drains on stop.
async fn collect_refresh(
    owner: &mut ControlSession,
    lease: &mut PolicyLeaseTracker,
    renewed: &mut Instant,
    dataset: DatasetId,
    root: &Resolved,
    renew: bool,
) -> Result<PolicySnapshot> {
    // A paused process cannot revive its old lease before checking expiry. This
    // same path supervises bounded startup pages and the subsequent idle loop.
    lease
        .check_delivery(lease.known_epoch())
        .map_err(|e| (1, e.to_string()))?;
    let ticket = lease.begin_refresh().map_err(|e| (1, e.to_string()))?;
    let refresh_started = Instant::now();
    let validate = |document: &str, digest| validate_policy(root, document, digest);
    let operation = async {
        if renew {
            owner.refresh(validate).await
        } else {
            owner.poll_policy(validate).await
        }
    };
    let ttl = duration(root, "authorization_ttl");
    let snapshot = tokio::time::timeout(
        duration(root, "renewal_timeout")
            .min(lease.remaining_validity().map_err(|e| (1, e.to_string()))?),
        operation,
    )
    .await
    .unwrap_or(Err(if renew {
        OwnerError::CommitUnknown
    } else {
        OwnerError::Backend
    }))
    .map_err(owner_error)?;
    lease
        .complete_refresh(
            &ticket,
            response(
                // These bindings belong to the original accepted tracker/session.
                dataset,
                owner.identity().process_incarnation,
                snapshot,
                ttl,
            ),
        )
        .map_err(|e| (1, e.to_string()))?;
    if renew {
        *renewed = refresh_started;
    }
    Ok(snapshot)
}

fn attempt_error(error: ArchiveAttemptError) -> (u8, String) {
    let code = match error {
        ArchiveAttemptError::Backend => 1,
        ArchiveAttemptError::Unavailable | ArchiveAttemptError::CommitUnknown => 4,
        _ => 3,
    };
    (code, format!("archive intent inventory refused: {error:?}"))
}

fn duration(root: &Resolved, name: &str) -> Duration {
    Duration::from_micros(root.number(&format!("runtime.{name}")))
}
fn response(
    dataset: DatasetId,
    process_incarnation: ProcessIncarnation,
    snapshot: PolicySnapshot,
    ttl: Duration,
) -> LeaseResponse {
    LeaseResponse {
        dataset,
        process_incarnation,
        ttl,
        policy_epoch: PolicyEpoch::new(snapshot.policy_revision),
    }
}
fn owner_error(error: OwnerError) -> (u8, String) {
    let code = match error {
        OwnerError::CommitUnknown | OwnerError::Unavailable => 4,
        OwnerError::Backend => 1,
        _ => 3,
    };
    (code, error.to_string())
}
fn validate_policy(
    root: &Resolved,
    document: &str,
    digest: [u8; 32],
) -> std::result::Result<(), OwnerError> {
    accepted_policy(root, document, digest).map(|_| ())
}
fn accepted_policy(
    root: &Resolved,
    document: &str,
    digest: [u8; 32],
) -> std::result::Result<Resolved, OwnerError> {
    PolicySeed::restore(document, digest, root.clone()).map_err(|_| OwnerError::InvalidPolicy)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_ingest_startup_requires_an_explicit_flag() {
        let ordinary = command()
            .try_get_matches_from(["serve", "--file", "/unused"])
            .unwrap();
        assert!(!ordinary.get_flag("initialize-ingest"));
        assert!(!ordinary.get_flag("activate-initial-successor"));
        let explicit = command()
            .try_get_matches_from([
                "serve",
                "--file",
                "/unused",
                "--roles",
                "control,ingest",
                "--initialize-ingest",
            ])
            .unwrap();
        assert!(explicit.get_flag("initialize-ingest"));
        assert!(!explicit.get_flag("activate-initial-successor"));
        let successor = command()
            .try_get_matches_from([
                "serve",
                "--file",
                "/unused",
                "--roles",
                "control,ingest,sync",
                "--activate-initial-successor",
            ])
            .unwrap();
        assert!(successor.get_flag("activate-initial-successor"));
        assert!(!successor.get_flag("initialize-ingest"));
    }

    #[test]
    fn archive_reader_selection_is_explicit_offline_and_preserves_policy_bytes() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("root.toml");
        let load = |text: &str| {
            std::fs::write(&path, text).unwrap();
            crate::config::load(&path)
        };
        let base = include_str!("../../../../config/recipes/archive-only.toml");
        let disabled = format!("{base}\n[archive.reader]\nenabled=false\n");
        let original = load(&disabled).unwrap();
        let seed = PolicySeed::from_resolved(&original).unwrap();
        let enabled = disabled.replace(
            "[archive.reader]\nenabled=false",
            "[archive.reader]\nenabled=true",
        );
        let reader = "read_user='allocation_reader'\nread_password_ref='file:/nonexistent/allocation-password'\n";
        let complete = enabled.replace(
            "[postgres.catalog]\n",
            &format!("[postgres.catalog]\n{reader}"),
        );
        let root = load(&complete).unwrap();
        assert!(root.flag("archive.reader.enabled"));
        assert_eq!(
            PolicySeed::from_resolved(&root).unwrap().document(),
            seed.document()
        );
        assert!(
            accepted_policy(&root, seed.document(), seed.digest())
                .unwrap()
                .flag("archive.reader.enabled")
        );
        assert!(load(&enabled).is_err());
        for missing in [
            "read_user='allocation_reader'\n",
            "read_password_ref='file:/nonexistent/allocation-password'\n",
        ] {
            assert!(load(&complete.replace(missing, "")).is_err());
        }
        let existing = include_str!("../../../../config/recipes/serve-existing.toml");
        assert!(load(&format!("{existing}\n[archive.reader]\nenabled=true\n")).is_err());
        // A declaration never opens the nonexistent password or enables a publisher.
        let reader_only = complete.replace(
            "[archive.publisher]\nenabled = true",
            "[archive.publisher]\nenabled = false",
        );
        assert!(
            !load(&reader_only)
                .unwrap()
                .flag("archive.publisher.enabled")
        );
    }

    #[test]
    fn accepted_administration_gate_is_selected_from_policy_despite_file_drift() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("root.toml");
        let enabled = include_str!("../../../../config/recipes/archive-only.toml");
        std::fs::write(&path, enabled).unwrap();
        let root = crate::config::load(&path).unwrap();
        let seed = PolicySeed::from_resolved(&root).unwrap();
        assert!(
            accepted_policy(&root, seed.document(), seed.digest())
                .unwrap()
                .flag("administration.enabled")
        );
        let disabled = enabled.replace(
            "[administration]\nenabled = true",
            "[administration]\nenabled = false",
        );
        assert_ne!(enabled, disabled);
        std::fs::write(&path, &disabled).unwrap();
        let candidate = crate::config::load(&path).unwrap();
        let accepted = PolicySeed::from_resolved(&candidate).unwrap();
        // Managed file drift must not substitute the file gate for accepted policy.
        assert!(
            !accepted_policy(&root, accepted.document(), accepted.digest())
                .unwrap()
                .flag("administration.enabled")
        );
        assert!(
            accepted_policy(&candidate, seed.document(), seed.digest())
                .unwrap()
                .flag("administration.enabled")
        );
    }
}
