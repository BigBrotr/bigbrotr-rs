//! First persistent role consumer. Every exit owns transport termination.
use super::{lifecycle_output::LifecycleOutput, postgres_command};
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
use std::{cell::Cell, collections::BTreeSet, path::Path, time::Duration};
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
        .after_help("Requires explicit initialization, migrations and initial policy activation with administration.enabled=false. SIGINT/SIGTERM stop the owner. No listeners or native stores are started by the Control-only slice. Exit: 0 stopped, 1 unavailable/I/O, 2 usage, 3 invalid configuration/policy, 4 cancelled or unknown durable outcome.")
}

pub(crate) fn execute(args: &ArgMatches) -> ! {
    let Ok(mut output) = LifecycleOutput::start() else {
        std::process::exit(1);
    };
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
        run(root, &mut output)
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
    // run() already closed/joined every backend and released its grants. Exit
    // also terminates a blocked diagnostics-only thread; it cannot outlive us.
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

fn run(root: Resolved, output: &mut LifecycleOutput) -> Result<()> {
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
        let outcome = supervise(client, request, &root, &mut stop, output).await;
        // No error, cancellation, output failure or unknown COMMIT can bypass
        // the join. Dropping the request/session alone does not free its grant.
        postgres_command::join(driver).await;
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
    let validate = |document: &str, digest| validate_policy(root, document, digest);
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
    lease
        .complete_refresh(&ticket, response(dataset, incarnation, snapshot, ttl))
        .map_err(|e| (1, e.to_string()))?;
    lease
        .check_delivery(lease.known_epoch())
        .map_err(|e| (1, e.to_string()))?;
    let identity = owner.identity();
    output.emit(
        json!({"format":"bigbrotr-control-runtime/1", "state":"idle", "role":"control",
        "dataset_id":dataset, "process_incarnation":incarnation,
        "owner_resource_id":identity.resource_id, "owner_epoch":identity.epoch.get().to_string(),
        "config_revision":snapshot.config_revision.to_string(), "policy_revision":snapshot.policy_revision.to_string(),
        "listeners":[], "native_stores":[], "authority_scope":"local Control only; no principal/action grant"}),
        false,
    );
    let heartbeat = duration(root, "heartbeat_interval");
    loop {
        let remaining = lease.remaining_validity().map_err(|e| (1, e.to_string()))?;
        // A slow successful response already consumed part of the request-start
        // lifetime. Neither that delay nor output handling may extend authority.
        let period = (remaining / 2).min(heartbeat.saturating_sub(renewed.elapsed()));
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
        let refresh_started = Instant::now();
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
            renewed = refresh_started;
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
fn validate_policy(
    root: &Resolved,
    document: &str,
    digest: [u8; 32],
) -> std::result::Result<(), OwnerError> {
    let accepted = PolicySeed::restore(document, digest, root.clone())
        .map_err(|_| OwnerError::InvalidPolicy)?;
    // The administration listener belongs to Control. Until it is implemented,
    // an accepted enablement cannot be silently replaced with an idle backend.
    if accepted.flag("administration.enabled") {
        return Err(OwnerError::InvalidPolicy);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepted_administration_gate_cannot_be_silently_ignored() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("root.toml");
        let enabled = include_str!("../../../../config/recipes/archive-only.toml");
        std::fs::write(&path, enabled).unwrap();
        let root = crate::config::load(&path).unwrap();
        let seed = PolicySeed::from_resolved(&root).unwrap();
        assert_eq!(
            validate_policy(&root, seed.document(), seed.digest()),
            Err(OwnerError::InvalidPolicy)
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
        assert_eq!(
            validate_policy(&root, accepted.document(), accepted.digest()),
            Ok(())
        );
    }
}
