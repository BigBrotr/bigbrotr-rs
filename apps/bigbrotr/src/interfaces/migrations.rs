use super::postgres_command::DRIVER_DRAIN;
use bigbrotr::storage::postgres::{
    connection_settings::PreparedConnection,
    migration_runner::{self, RunError, RunResult},
    migrations,
};
use clap::{Arg, ArgMatches, Command};
use serde_json::{Value, json};
use std::{cell::Cell, io::Write, path::Path, process::ExitCode, time::Duration};

const OPERATION_DEADLINE: Duration = Duration::from_secs(60);

pub fn command() -> Command {
    let mut command = Command::new("migrate")
        .about("Explicit PostgreSQL control-schema inspection and migration")
        .after_help("Control schema only; no dataset initialization or engine upgrade. Apply requires separate migration credentials. Never infer rollback from a lost COMMIT response.")
        .subcommand_required(true).disable_help_subcommand(true);
    for (action, description) in [
        (
            "inspect",
            "Inspect the current control ledger without durable changes",
        ),
        (
            "plan",
            "Inspect and report the exact forward control-schema steps",
        ),
        (
            "apply",
            "Apply the exact control schema and receipt in one transaction",
        ),
    ] {
        command = command.subcommand(
            Command::new(action)
                .about(description)
                .arg(
                    Arg::new("file")
                        .long("file")
                        .required(true)
                        .value_name("CONFIG"),
                )
                .arg(
                    Arg::new("user")
                        .long("user")
                        .required(true)
                        .value_name("DATABASE_ROLE"),
                )
                .arg(
                    Arg::new("password-ref")
                        .long("password-ref")
                        .required(true)
                        .value_name("FILE_REFERENCE"),
                )
                .arg(
                    Arg::new("target")
                        .long("target")
                        .value_parser(clap::value_parser!(u32).range(1..))
                        .value_name("SCHEMA_VERSION"),
                ),
        );
    }
    command
}

type CliResult<T> = Result<T, (u8, String)>;
pub fn execute(matches: &ArgMatches) -> ExitCode {
    match run(matches) {
        Ok((report, code)) => {
            let mut stdout = std::io::stdout().lock();
            if writeln!(
                stdout,
                "{}",
                serde_json::to_string_pretty(&report).expect("finite migration metadata")
            )
            .and_then(|()| stdout.flush())
            .is_err()
            {
                eprintln!(
                    "error: could not write migration result; inspect the same target before retry"
                );
                return ExitCode::FAILURE;
            }
            ExitCode::from(code)
        }
        Err((code, reason)) => {
            eprintln!("error: {reason}");
            ExitCode::from(code)
        }
    }
}

fn run(matches: &ArgMatches) -> CliResult<(Value, u8)> {
    let (action, args) = matches.subcommand().expect("required migration command");
    let config = crate::config::load(Path::new(args.get_one::<String>("file").unwrap()))?;
    let target = args.get_one::<u32>("target").copied().unwrap_or_else(|| {
        migrations::catalog()
            .last()
            .expect("nonempty migration catalog")
            .version
    });
    migrations::plan(&[], target).map_err(|e| (3, e.to_string()))?;
    let user = args.get_one::<String>("user").unwrap();
    if action == "apply" && config.text("postgres.control.user") == Some(user.as_str()) {
        return Err((3, "apply requires a dedicated migration role, separate from the configured application role".into()));
    }
    let (prepared, _grant) = super::postgres_command::prepare(
        &config,
        user,
        args.get_one::<String>("password-ref").unwrap(),
        0,
    )?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| (1, "cannot start finite migration executor".into()))?;
    let result = runtime.block_on(execute_connection(prepared, target, action == "apply"));
    // This is a finite executable command. An uncancellable resolver can outlive
    // this runtime grace period only until this process exits; no new work is
    // accepted and no background daemon receives recycled credits here.
    runtime.shutdown_timeout(DRIVER_DRAIN);
    match result {
        Ok(result) => Ok((report(action, result), 0)),
        Err(RunError::CommitUnknown) => Ok((
            json!({
                "format":"bigbrotr-control-migration/1", "scope":"postgres-control",
                "status":"outcome-unknown", "target_version":target.to_string(),
                "resolution":"inspect or reapply this exact target under the migration lock; no rollback inferred"
            }),
            4,
        )),
        Err(error) => Err((1, error.to_string())),
    }
}

async fn execute_connection(
    prepared: PreparedConnection,
    target: u32,
    apply: bool,
) -> Result<RunResult, RunError> {
    let (mut client, driver) =
        super::postgres_command::connect(prepared)
            .await
            .map_err(|error| match error {
                super::postgres_command::ConnectError::Backend => RunError::Backend,
                super::postgres_command::ConnectError::PlaintextRoute => RunError::PlaintextRoute,
            })?;
    let commit_started = Cell::new(false);
    let result = match tokio::time::timeout(
        OPERATION_DEADLINE,
        migration_runner::run(&mut client, target, apply, &commit_started),
    )
    .await
    {
        Ok(result) => result,
        Err(_) if commit_started.get() => Err(RunError::CommitUnknown),
        Err(_) => Err(RunError::Backend),
    };
    super::postgres_command::close(client, driver).await;
    // Joining this local driver closes local ownership only. A failed COMMIT
    // remains unknown until the next serialized ledger inspection resolves it.
    result
}

fn report(action: &str, result: RunResult) -> Value {
    let current = if result.applied > 0 {
        Some(result.plan.target)
    } else {
        result.plan.from
    };
    json!({
        "format":"bigbrotr-control-migration/1", "scope":"postgres-control",
        "status":match action { "inspect" => "inspected", "plan" => "planned", _ if result.applied > 0 => "applied", _ => "current" },
        "source_version":result.plan.from.map(|n| n.to_string()),
        "current_version":current.map(|n| n.to_string()),
        "target_version":result.plan.target.to_string(), "applied_count":result.applied.to_string(),
        "steps":result.plan.pending.iter().map(|step| json!({"version":step.version.to_string(),"name":step.name,"sha256":step.checksum().iter().map(|b|format!("{b:02x}")).collect::<String>()})).collect::<Vec<_>>(),
        "dataset_initialization":"not-performed", "release_upgrade_qualification":"not-performed"
    })
}
