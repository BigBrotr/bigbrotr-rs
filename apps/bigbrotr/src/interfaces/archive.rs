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
        .about("Inspect installed archive prerequisites")
        .subcommand_required(true)
        .disable_help_subcommand(true)
        .subcommand(super::catalog_inspection::command())
        .subcommand(clap::Command::new("verify-worker")
            .about("Verify the installed worker and offline native assets against this build")
            .arg(clap::Arg::new("root").long("root").value_parser(clap::value_parser!(PathBuf))
                .help("Canonical absolute bundle directory; default: archive-worker beside this executable"))
            .after_help("Reads bundle bytes only. No engine, catalog or readiness check. Requires a main executable with an embedded worker identity from the trusted builder. Exit codes: 0 verified, 1 output I/O failure, 2 usage, 4 verification refused."))
}

pub fn execute(matches: &clap::ArgMatches) -> ExitCode {
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
