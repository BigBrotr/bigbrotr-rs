use bigbrotr::storage::archive::WorkerInstallation;
use std::{
    io::{self, Write},
    path::PathBuf,
    process::ExitCode,
    sync::atomic::AtomicBool,
    time::{Duration, Instant},
};

pub fn command() -> clap::Command {
    clap::Command::new("archive")
        .about("Initialize, recover and inspect the local archive")
        .subcommand_required(true)
        .disable_help_subcommand(true)
        .subcommand(super::catalog_inspection::command())
        .subcommand(super::archive_receipt::command())
        .subcommand(super::archive_receipt::membership_command())
        .subcommand(super::archive_initialization::command("initialize"))
        .subcommand(super::archive_initialization::command("recover"))
        .subcommand(super::serve::coordinator_command())
        .subcommand(clap::Command::new("state-initialize")
            .about("Explicitly bind persistent coordinator state to an existing local archive reservation")
            .arg(clap::Arg::new("file").long("file").required(true).value_parser(clap::value_parser!(PathBuf)))
            .after_help("Same-boot Linux ext4 only. Requires an existing canonical private state directory. No database connection, catalog recovery, attempt adoption or publication authority. Preserve incomplete/conflicting state. Exit: 0 bound, 1 I/O, 2 usage, 3 invalid configuration, 4 unavailable/incompatible custody."))
        .subcommand(clap::Command::new("verify-worker")
            .about("Verify the installed worker and offline native assets against this build")
            .arg(clap::Arg::new("root").long("root").value_parser(clap::value_parser!(PathBuf))
                .help("Canonical absolute bundle directory; default: archive-worker beside this executable"))
            .after_help("Reads bundle bytes only. No engine, catalog or readiness check. Requires a main executable with an embedded worker identity from the trusted builder. Exit codes: 0 verified, 1 output I/O failure, 2 usage, 4 verification refused."))
}

pub fn execute(matches: &clap::ArgMatches) -> ExitCode {
    for name in ["receipt-inspect", "batch-inspect"] {
        if let Some(args) = matches.subcommand_matches(name) {
            super::serve::inspect_receipt(args);
        }
    }
    if let Some(args) = matches.subcommand_matches("coordinator-initialize") {
        super::serve::initialize_coordinator(args);
    }
    if let Some(args) = matches.subcommand_matches("state-initialize") {
        let Ok(mut output) = super::lifecycle_output::LifecycleOutput::start() else {
            return ExitCode::FAILURE;
        };
        let result = (|| {
            let config = crate::config::load(args.get_one::<PathBuf>("file").unwrap())?;
            let identity = config.catalog_identity().map_err(|e| (3, e))?;
            let root = config.archive_state_path().map_err(|e| (4, e))?;
            let _custody =
                bigbrotr::storage::archive::LocalCoordinator::initialize(&identity, &root)
                    .map_err(|e| {
                        (
                            4,
                            format!("archive coordinator state binding refused: {e:?}"),
                        )
                    })?;
            Ok::<_, (u8, String)>(
                serde_json::json!({"format":"bigbrotr-coordinator-state/1", "state":"bound",
                "dataset_id":identity.dataset(), "catalog_id":identity.catalog(), "catalog_inspection":"not-performed",
                "attempt_adoption":"not-performed", "publication_authority":"not-granted", "ready":false}),
            )
        })();
        let code = match result {
            Ok(value) => {
                output.emit(value, false);
                0
            }
            Err((code, error)) => {
                output.emit(format_args!("error: {error}"), true);
                code
            }
        };
        let delivered = output.finish();
        std::process::exit(i32::from(if delivered { code } else { 1 }));
    }
    for name in ["initialize", "recover"] {
        if let Some(args) = matches.subcommand_matches(name) {
            return super::archive_initialization::execute(args, name == "initialize");
        }
    }
    if let Some(args) = matches.subcommand_matches("inspect") {
        return super::catalog_inspection::execute(args);
    }
    let matches = matches.subcommand_matches("verify-worker").unwrap();
    let root = matches.get_one::<PathBuf>("root").cloned().or_else(|| {
        std::env::current_exe()
            .ok()?
            .parent()
            .map(|p| p.join("archive-worker"))
    });
    let installation = root
        .ok_or("cannot locate installed bundle")
        .and_then(|root| {
            WorkerInstallation::packaged(root).map_err(|_| "missing embedded worker identity")
        });
    let result = installation.and_then(|installation| {
        installation.verify(Instant::now() + Duration::from_secs(30), &AtomicBool::new(false))
            .map_err(|_| "bundle bytes, paths or permissions do not match this build")?;
        Ok(serde_json::json!({
            "format": "bigbrotr-worker-verification/1",
            "result": "verified",
            "target": env!("BIGBROTR_BUILD_TARGET"),
            "executable": {"bytes": installation.executable.bytes, "sha256": installation.executable.sha256.to_string()},
            "scope": "bundle bytes only; no engine, catalog or readiness check"
        }))
    });
    match result {
        Ok(value) => {
            if writeln!(io::stdout().lock(), "{value}").is_ok() {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            }
        }
        Err(reason) => {
            let _ = writeln!(
                io::stderr().lock(),
                "error: archive worker verification refused: {reason}"
            );
            ExitCode::from(4)
        }
    }
}
