//! Explicit local secret preparation. Control registration is a separate effect.
use super::postgres_command;
use bigbrotr::{
    read::keyring::{KEYRING_RESERVATION, Keyring, PreparedKeyring},
    runtime::{ProcessEnvelope, ResourceBudget, ResourceLimits, ResourceRequest, WaiterLimits},
    storage::postgres::cursor_registration::{self, KeyOrigin, RegistrationError},
};
use bigbrotr_model::DatasetId;
use clap::{Arg, ArgMatches, Command};
use std::{cell::Cell, path::PathBuf, process::ExitCode, time::Duration};

pub(super) fn command() -> Command {
    let mut command = Command::new("cursor-keyring")
        .about("Prepare, inspect, register or enroll protected continuation keys")
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
        (
            "prepare-key",
            "Add one local key to an existing ring; same key ID retries identical bytes",
        ),
    ] {
        let mut subcommand = Command::new(name).about(about)
            .arg(Arg::new("file").long("file").required(true).value_parser(clap::value_parser!(PathBuf)))
            .after_help("Requires read.cursor.keyring_ref in the deployment file and an existing dedicated mode0700 directory owned by this user. Symlinks are refused. Preparation refuses partial or conflicting state; inspection may read the last complete final file beside staging. Successful preparation syncs exact keys and directory entries; complete retry preserves keys. Exclude external provisioning writers during preparation. Prepare-key requires an existing complete keyfile and a nonzero32-character lowercase hex key ID; retry the same ID after interruption. Prepare in one location, then distribute those exact protected bytes. No database contact, Control registration, activation or runtime authority. Output omits secret bytes and paths. Exit:0 operation completed,1 configuration/output I/O,2 usage,3 configuration/budget,4 unavailable/conflicting key storage.");
        if name == "prepare-key" {
            subcommand = subcommand.arg(
                Arg::new("key-id")
                    .long("key-id")
                    .required(true)
                    .value_parser(key_id),
            );
        }
        command = command.subcommand(subcommand);
    }
    command.subcommand(Command::new("register")
        .about("Register already prepared keys once in Control; no activation")
        .arg(Arg::new("file").long("file").required(true).value_parser(clap::value_parser!(PathBuf)))
        .after_help("Requires migration0601 and the protected Control application credential. Re-syncs only existing complete local keys before connection; missing or partial keys refuse. Identical retry resolves the original immutable registration and audit. Conflicting keys require explicit recovery, never regeneration. No key activation, runtime authority or worker-distribution proof. Keep keys and driver reservation until connection closure. Exit:0 registered/resolved,1 connection/output failure,2 usage,3 configuration/schema/conflict,4 local key unavailable or COMMIT outcome unknown; retry identical keys on a fresh connection."))
        .subcommand(Command::new("enroll-key")
        .about("Enroll one prepared key in Control; no activation")
        .arg(Arg::new("file").long("file").required(true).value_parser(clap::value_parser!(PathBuf)))
        .arg(Arg::new("key-id").long("key-id").required(true).value_parser(key_id))
        .after_help("Requires migration0602, an initially registered ring and the protected Control application credential. Re-syncs an existing complete keyfile and requires the selected key before connecting. The public key ID permanently binds its material fingerprint. Identical retry returns original initial or additional enrollment attribution even after other local keys change. No distribution, activation or issuance authority. Exit:0 enrolled/resolved,1 connection/output failure,2 usage,3 configuration/schema/conflict,4 local key unavailable or COMMIT unknown. Retain keys and retry the identical key on a fresh connection after an unknown outcome."))
}

fn key_id(text: &str) -> Result<[u8; 16], String> {
    if text.len() != 32
        || !text
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err("expected a nonzero32-character lowercase hexadecimal key ID".into());
    }
    let id = u128::from_str_radix(text, 16).expect("bounded hexadecimal ID");
    if id == 0 {
        return Err("key ID must be nonzero".into());
    }
    Ok(id.to_be_bytes())
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
        if matches!(operation, "register" | "enroll-key") {
            let selected = if operation == "enroll-key" {
                Some(*args.get_one::<[u8; 16]>("key-id").expect("required key ID"))
            } else {
                None
            };
            return register(&config, reference, dataset, selected);
        }
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
        let keyring = match operation {
            "prepare" => Keyring::prepare(reference, dataset, permit),
            "prepare-key" => Keyring::prepare_key(
                reference,
                dataset,
                *args.get_one::<[u8; 16]>("key-id").expect("required key ID"),
                permit,
            ),
            _ => Keyring::open(reference, dataset, permit),
        }
        .map_err(|error| (4, error.to_string()))?;
        let hex = |bytes: [u8; 16]| format!("{:032x}", u128::from_be_bytes(bytes));
        Ok::<_, (u8, String)>(serde_json::json!({
            "format":"bigbrotr-cursor-keyring-observation/1",
            "state":if operation == "inspect" { "inspected-local" } else { "prepared-local" },
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

fn register(
    config: &crate::config::Resolved,
    reference: &str,
    dataset: DatasetId,
    selected: Option<[u8; 16]>,
) -> Result<serde_json::Value, (u8, String)> {
    let required = |key: &str| {
        config
            .text(key)
            .ok_or_else(|| (3, format!("{key}: required for cursor registration")))
    };
    let instance = required("topology.instance_id")?
        .parse()
        .map_err(|_| (3, "invalid instance identity".into()))?;
    let (connection, permit) = postgres_command::prepare(
        config,
        required("postgres.control.user")?,
        required("postgres.control.password_ref")?,
        KEYRING_RESERVATION,
    )?;
    // One combined charge accounts for connection/transaction and secret bytes.
    // The prepared snapshot owns it until the real driver join below completes.
    let prepared = PreparedKeyring::existing(reference, dataset, permit)
        .map_err(|error| (4, error.to_string()))?;
    let selected_binding = if let Some(id) = selected {
        Some(
            prepared
                .keys()
                .bindings()
                .find(|(candidate, _)| *candidate == id)
                .ok_or_else(|| {
                    (
                        3,
                        "selected cursor key is not present in the prepared keyfile".into(),
                    )
                })?,
        )
    } else {
        None
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| (1, "cannot start finite Control executor".into()))?;
    let result = runtime.block_on(async {
        let (mut client, driver) = postgres_command::connect(connection)
            .await
            .map_err(|_| RegistrationError::Backend)?;
        let started = Cell::new(false);
        let result = match tokio::time::timeout(Duration::from_secs(30), async {
            if let Some(id) = selected {
                cursor_registration::enroll(&mut client, instance, &prepared, id, &started)
                    .await
                    .map(|result| (result.receipt, Some(result.origin)))
            } else {
                cursor_registration::register(&mut client, instance, &prepared, &started)
                    .await
                    .map(|receipt| (receipt, None))
            }
        })
        .await
        {
            Ok(result) => result,
            Err(_) if started.get() => Err(RegistrationError::CommitUnknown),
            Err(_) => Err(RegistrationError::Backend),
        };
        postgres_command::close(client, driver).await;
        result
    });
    runtime.shutdown_timeout(postgres_command::DRIVER_DRAIN);
    let hex = |bytes: [u8; 16]| format!("{:032x}", u128::from_be_bytes(bytes));
    let keys = prepared.keys();
    match result {
        Ok((receipt, origin)) => {
            let mut report = if let Some((id, digest)) = selected_binding {
                serde_json::json!({
                    "format":"bigbrotr-cursor-key-enrollment/1", "dataset_id":dataset,
                    "keyring_id":hex(keys.id()), "key_id":hex(id), "material_fingerprint_sha256":digest,
                    "cursor_authority":"not-granted",
                    "state":if receipt.created {"enrolled"} else {"already-enrolled"},
                    "origin":match origin.expect("enrollment origin") {KeyOrigin::Initial => "initial", KeyOrigin::Additional => "additional"},
                    "enrolled_at_us":receipt.registered_at_us.to_string()
                })
            } else {
                serde_json::json!({
                    "format":"bigbrotr-cursor-registration/1", "dataset_id":dataset,
                    "keyring_id":hex(keys.id()), "key_ids":keys.key_ids().map(hex).collect::<Vec<_>>(),
                    "initial_file_fingerprint_sha256":keys.fingerprint(), "cursor_authority":"not-granted",
                    "state":if receipt.created {"registered"} else {"already-registered"},
                    "registered_at_us":receipt.registered_at_us.to_string()
                })
            };
            report["actor_id"] = serde_json::to_value(receipt.actor).expect("fixed identity");
            report["audit_id"] = hex(receipt.audit_id).into();
            Ok(report)
        }
        Err(RegistrationError::CommitUnknown) => Err((4, "cursor key registration/enrollment COMMIT outcome unknown; retain keys and retry the identical input on a fresh connection".into())),
        Err(error) => Err((match error {
            RegistrationError::Backend => 1,
            RegistrationError::Entropy => 4,
            _ => 3,
        }, error.to_string())),
    }
}
