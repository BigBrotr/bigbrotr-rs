//! Explicit local secret preparation. Control registration is a separate effect.
use bigbrotr::{
    read::keyring::{KEYRING_RESERVATION, Keyring},
    runtime::{ProcessEnvelope, ResourceBudget, ResourceLimits, ResourceRequest, WaiterLimits},
};
use clap::{Arg, ArgMatches, Command};
use std::{path::PathBuf, process::ExitCode};

pub(super) fn command() -> Command {
    let mut command = Command::new("cursor-keyring")
        .about("Prepare or inspect protected local continuation keys")
        .subcommand_required(true)
        .disable_help_subcommand(true);
    for (name, about) in [
        (
            "prepare",
            "Persist local keys once; identical complete retry keeps the same keys",
        ),
        (
            "inspect",
            "Read an existing local keyring without creating or repairing it",
        ),
    ] {
        command = command.subcommand(Command::new(name).about(about)
            .arg(Arg::new("file").long("file").required(true).value_parser(clap::value_parser!(PathBuf)))
            .after_help("Requires read.cursor.keyring_ref in the deployment file and an existing dedicated mode0700 directory owned by this user. Symlinks are refused. Preparation refuses partial or conflicting state; inspection may read the last complete final file beside staging. Successful preparation syncs exact keys and directory entries; complete retry preserves keys. Exclude external provisioning writers during preparation. No database contact, Control registration, activation or runtime authority. Output omits secret bytes and paths. Exit:0 operation completed,1 configuration/output I/O,2 usage,3 configuration/budget,4 unavailable/conflicting key storage."));
    }
    command
}

pub(super) fn execute(args: &ArgMatches) -> ExitCode {
    let Ok(mut output) = super::lifecycle_output::LifecycleOutput::start() else {
        return ExitCode::FAILURE;
    };
    let result = (|| {
        let (operation, args) = args.subcommand().expect("required operation");
        let config = crate::config::load(args.get_one::<PathBuf>("file").unwrap())?;
        let reference = config
            .text("read.cursor.keyring_ref")
            .ok_or_else(|| (3, "missing read.cursor.keyring_ref".into()))?;
        let dataset = config
            .text("topology.dataset_id")
            .expect("validated dataset")
            .parse()
            .map_err(|_| (3, "invalid dataset identity".into()))?;
        let number = |field: &str| config.number(&format!("runtime.processes.application.{field}"));
        let envelope = ProcessEnvelope::new(
            number("memory_max_bytes"),
            number("baseline_bytes"),
            ResourceLimits::new(
                number("payload_bytes"),
                1,
                number("native_bytes"),
                number("completion_reserve_bytes"),
            ),
            WaiterLimits::new(0, 0),
        )
        .map_err(|_| (3, "invalid application memory envelope".into()))?;
        let permit = ResourceBudget::new(envelope)
            .try_acquire(
                ResourceRequest::data(KEYRING_RESERVATION, 1, 0).expect("positive reservation"),
            )
            .map_err(|_| {
                (
                    3,
                    "cursor keys do not fit the declared payload grant".into(),
                )
            })?;
        let keyring = if operation == "prepare" {
            Keyring::prepare(reference, dataset, permit)
        } else {
            Keyring::open(reference, dataset, permit)
        }
        .map_err(|error| (4, error.to_string()))?;
        let hex = |bytes: [u8; 16]| format!("{:032x}", u128::from_be_bytes(bytes));
        Ok::<_, (u8, String)>(serde_json::json!({
            "format":"bigbrotr-cursor-keyring-observation/1",
            "state":if operation == "prepare" { "prepared-local" } else { "inspected-local" },
            "dataset_id":dataset, "keyring_id":hex(keyring.id()),
            "key_ids":keyring.key_ids().map(hex).collect::<Vec<_>>(),
            "fingerprint_sha256":keyring.fingerprint(), "control_registration":"not-checked",
            "cursor_authority":"not-granted"
        }))
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
    // Key bytes and their reservation have dropped before the finite output drain.
    let delivered = output.finish();
    std::process::exit(i32::from(if delivered { code } else { 1 }));
}
