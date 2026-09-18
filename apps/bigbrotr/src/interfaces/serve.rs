//! First persistent role consumer. Every exit owns transport termination.
use super::postgres_command;
use crate::config::{Resolved, policy::PolicySeed};
use bigbrotr::{
    runtime::{
        ownership::fresh_process_incarnation,
        policy_authority::{LeaseResponse, PolicyLeaseTracker, PolicyLeaseTtl},
    },
    storage::postgres::control_owner::{ClaimRequest, ControlSession, OwnerError, PolicySnapshot},
};
use bigbrotr_model::{DatasetId, PolicyEpoch, ProcessIncarnation};
use clap::{Arg, ArgMatches, Command};
use serde_json::{Value, json};
use std::{
    cell::Cell, collections::BTreeSet, io::Write, path::Path, process::ExitCode, time::Duration,
};
use tokio::{
    signal::unix::{Signal, SignalKind, signal},
    time::Instant,
};

type Result<T> = std::result::Result<T, (u8, String)>;

pub(crate) fn command() -> Command {
    Command::new("serve")
        .about("Run explicitly assigned roles; the initial runtime implements Control only")
        .arg(Arg::new("file").long("file").required(true).value_name("PATH"))
        .arg(Arg::new("roles").long("roles").value_delimiter(',').num_args(1..).value_name("ROLE,..."))
        .after_help("Requires explicit initialization, migrations and initial policy activation. SIGINT/SIGTERM stop the owner. No listeners or native stores are started by the Control-only slice. Exit: 0 stopped, 1 unavailable/I/O, 2 usage, 3 invalid configuration/policy, 4 cancelled or unknown durable outcome.")
}

pub(crate) fn execute(args: &ArgMatches) -> ExitCode {
    let outcome = (|| {
        let root = crate::config::load(Path::new(args.get_one::<String>("file").unwrap()))?;
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
        if distinct.iter().any(|role| *role != "control") {
            return Err((
                3,
                "selected role is not implemented by this runtime; select only control explicitly"
                    .into(),
            ));
        }
        run(root)
    })();
    match outcome {
        Ok(()) => ExitCode::SUCCESS,
        Err((code, message)) => {
            eprintln!("error: {message}");
            ExitCode::from(code)
        }
    }
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

fn run(root: Resolved) -> Result<()> {
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
    let (prepared, grant) = postgres_command::prepare(
        &root,
        required("postgres.control.user")?,
        required("postgres.control.password_ref")?,
        512 * 1024 * 1024,
    )?;
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
        .build()
        .map_err(|_| (1, "cannot start Control runtime".into()))?;
    let result = runtime.block_on(async {
        let mut stop = Stop::new()?;
        let (client, driver) = tokio::select! {
            biased;
            _ = stop.requested() => return Err((4, "Control startup cancelled before connection".into())),
            result = postgres_command::connect(prepared) => result.map_err(|e| (1, e.to_string()))?,
        };
        let outcome = supervise(client, request, &root, &mut stop).await;
        // No error, cancellation, output failure or unknown COMMIT can bypass
        // the join. Dropping the request/session alone does not free its grant.
        postgres_command::join(driver).await;
        outcome
    });
    runtime.shutdown_timeout(postgres_command::DRIVER_DRAIN);
    let result = result.and_then(emit);
    drop(grant);
    result
}

async fn supervise(
    client: tokio_postgres::Client,
    request: ClaimRequest,
    root: &Resolved,
    stop: &mut Stop,
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
    let validate = |document: &str, digest| {
        PolicySeed::restore(document, digest, root.clone())
            .map(|_| ())
            .map_err(|_| OwnerError::InvalidPolicy)
    };
    let ticket = lease.begin_refresh().map_err(|e| (1, e.to_string()))?;
    let commit = Cell::new(false);
    let acquired = tokio::select! {
        biased;
        _ = stop.requested() => Err(if commit.get() { OwnerError::CommitUnknown } else { OwnerError::Unavailable }),
        result = tokio::time::timeout(duration(root, "renewal_timeout"), ControlSession::acquire(client, request, validate, &commit)) =>
            result.unwrap_or_else(|_| Err(if commit.get() { OwnerError::CommitUnknown } else { OwnerError::Backend })),
    };
    let (mut owner, mut snapshot) = acquired.map_err(owner_error)?;
    lease
        .complete_refresh(&ticket, response(dataset, incarnation, snapshot, ttl))
        .map_err(|e| (1, e.to_string()))?;
    lease
        .check_delivery(lease.known_epoch())
        .map_err(|e| (1, e.to_string()))?;
    let identity = owner.identity();
    emit(
        json!({"format":"bigbrotr-control-runtime/1", "state":"idle", "role":"control",
        "dataset_id":dataset, "process_incarnation":incarnation,
        "owner_resource_id":identity.resource_id, "owner_epoch":identity.epoch.get().to_string(),
        "config_revision":snapshot.config_revision.to_string(), "policy_revision":snapshot.policy_revision.to_string(),
        "listeners":[], "native_stores":[], "authority_scope":"local Control only; no principal/action grant"}),
    )?;
    let heartbeat = duration(root, "heartbeat_interval");
    let period = (ttl / 2).min(heartbeat);
    let mut renewed = Instant::now();
    loop {
        tokio::select! {
            biased;
            _ = stop.requested() => {
                // This owner has no admitted data work. Release remains fenced
                // until the transaction and driver are terminal.
                tokio::time::timeout(duration(root, "graceful_shutdown_timeout"), owner.release()).await
                    .unwrap_or(Err(OwnerError::CommitUnknown)).map_err(owner_error)?;
                return Ok(json!({"format":"bigbrotr-control-runtime/1", "state":"stopped", "role":"control",
                    "owner_epoch":identity.epoch.get().to_string(), "policy_revision":snapshot.policy_revision.to_string(),
                    "release":"committed"}));
            },
            _ = tokio::time::sleep(period) => {},
        }
        // A paused process cannot revive its old lease before checking expiry.
        lease
            .check_delivery(lease.known_epoch())
            .map_err(|e| (1, e.to_string()))?;
        let ticket = lease.begin_refresh().map_err(|e| (1, e.to_string()))?;
        let renew = renewed.elapsed() >= heartbeat;
        let operation = async {
            if renew {
                owner.refresh(validate).await
            } else {
                owner.poll_policy(validate).await
            }
        };
        let refreshed = tokio::select! {
            biased;
            _ = stop.requested() => Err(if renew { OwnerError::CommitUnknown } else { OwnerError::Unavailable }),
            result = tokio::time::timeout(duration(root, "renewal_timeout").min(ttl), operation) =>
                result.unwrap_or(Err(if renew { OwnerError::CommitUnknown } else { OwnerError::Backend })),
        };
        snapshot = refreshed.map_err(owner_error)?;
        lease
            .complete_refresh(&ticket, response(dataset, incarnation, snapshot, ttl))
            .map_err(|e| (1, e.to_string()))?;
        if renew {
            renewed = Instant::now();
        }
    }
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
fn emit(value: Value) -> Result<()> {
    let mut out = std::io::stdout().lock();
    writeln!(out, "{value}")
        .and_then(|()| out.flush())
        .map_err(|_| (1, "cannot write Control lifecycle output".into()))
}
