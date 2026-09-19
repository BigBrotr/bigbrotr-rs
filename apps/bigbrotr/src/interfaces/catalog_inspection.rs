//! Explicit privileged catalog inspection; no bootstrap or publication fallback.
use super::postgres_command;
use bigbrotr::{runtime::OwnedPermit, storage::postgres::catalog_guard};
use serde_json::{Value, json};
use std::{
    io::{self, Write},
    path::PathBuf,
    process::ExitCode,
    time::Duration,
};

pub(super) fn command() -> clap::Command {
    clap::Command::new("inspect")
        .about("Verify the configured catalog identity, guard and scientific schema")
        .arg(clap::Arg::new("file").long("file").required(true)
            .value_parser(clap::value_parser!(PathBuf)).value_name("PATH"))
        .after_help("Requires a selected local Archive role and its protected PostgreSQL catalog manager credential. Takes a finite owner-row lock and rolls back the inspection transaction. Does not initialize, activate or inspect data files. Exit codes: 0 verified metadata, 1 local/output I/O, 2 usage, 3 configuration/credential refusal, 4 unavailable or incompatible catalog.")
}

pub(super) fn execute(args: &clap::ArgMatches) -> ExitCode {
    match inspect(args.get_one::<PathBuf>("file").unwrap()) {
        Ok((report, _grant)) => {
            if writeln!(io::stdout().lock(), "{report}").is_ok() {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            }
        }
        Err((code, reason)) => {
            let _ = writeln!(io::stderr().lock(), "error: {reason}");
            ExitCode::from(code)
        }
    }
}

fn inspect(path: &std::path::Path) -> Result<(Value, OwnedPermit), (u8, String)> {
    let config = crate::config::load(path)?;
    let identity = config.catalog_identity().map_err(|e| (3, e))?;
    // The guard bounds object definitions and logical columns before transfer.
    // Retain its conservative inspection allowance through actual driver join
    // and output; this per-command grant is not a process-wide RSS certificate.
    let (prepared, grant) = postgres_command::prepare_catalog(&config, 64 * 1024 * 1024)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| (1, "cannot start finite catalog executor".into()))?;
    let result = runtime.block_on(async {
        let (mut client, driver) = postgres_command::connect(prepared)
            .await
            .map_err(|e| (4, e.to_string()))?;
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            catalog_guard::inspect(&mut client, &identity),
        )
        .await;
        postgres_command::close(client, driver).await;
        result
            .map_err(|_| (4, "catalog inspection deadline exceeded".into()))?
            .map_err(|e| (4, e.to_string()))
    });
    runtime.shutdown_timeout(postgres_command::DRIVER_DRAIN);
    result?;
    Ok((
        json!({
            "format": "bigbrotr-catalog-inspection/1", "status": "verified",
            "dataset_id": identity.dataset(), "catalog_id": identity.catalog(),
            "data_root": identity.root(), "scientific_schema_version": 1,
            "scope": "catalog identity, commit guard and logical schema metadata",
            "file_contents": "not-checked", "runtime_authorization": "not-granted",
            "ready": false
        }),
        grant,
    ))
}
