use super::postgres_command;
use bigbrotr::{
    management::credentials::{HASH_RESERVATION, OwnerToken, PasswordCredential, validate_name},
    runtime::OwnedPermit,
    secrets::SecretFile,
    storage::postgres::enrollment::{self, Deployment, EnrollmentError, Operation},
};
use bigbrotr_model::DatasetId;
use clap::{Arg, ArgMatches, Command};
use serde_json::{Value, json};
use std::{cell::Cell, io::Write, path::Path, process::ExitCode, time::Duration};

pub fn command() -> Command {
    let file = || {
        Arg::new("file")
            .long("file")
            .required(true)
            .value_name("CONFIG")
    };
    let id = |name: &'static str| Arg::new(name).long(name).required(true).value_name("HEX32");
    let command = Command::new("access")
        .about("Local account sessions, enrollment and deployment credential tools")
        .subcommand_required(true)
        .disable_help_subcommand(true)
        .subcommand(super::self_inspection::command())
        .subcommand(super::cursor_keyring::command())
        .subcommand(
            Command::new("owner-token")
                .subcommand_required(true)
                .disable_help_subcommand(true)
                .subcommand(
                    Command::new("issue")
                        .about("Issue once for the intended account; stdout contains the secret")
                        .arg(file())
                        .arg(id("request-id"))
                        .arg(Arg::new("username").long("username").required(true)),
                )
                .subcommand(
                    Command::new("status")
                        .about("Resolve enrollment state without exposing a secret")
                        .arg(file()),
                )
                .subcommand(
                    Command::new("revoke")
                        .about("Revoke a pending challenge before explicit reissue")
                        .arg(file())
                        .arg(id("challenge-id")),
                ),
        )
        .subcommand(
            Command::new("enroll-owner")
                .about("Consume one owner token and create its intended local account")
                .arg(file())
                .arg(id("challenge-id"))
                .arg(
                    Arg::new("token-ref")
                        .long("token-ref")
                        .required(true)
                        .value_name("file:/ABSOLUTE/PATH"),
                )
                .arg(
                    Arg::new("password-ref")
                        .long("password-ref")
                        .required(true)
                        .value_name("file:/ABSOLUTE/PATH"),
                ),
        );
    super::session_client::commands(command)
}
type Result<T> = std::result::Result<T, (u8, String)>;
pub fn execute(matches: &ArgMatches) -> ExitCode {
    if let Some((action @ ("login" | "logout"), args)) = matches.subcommand() {
        return super::session_client::execute(action, args);
    }
    if let Some(args) = matches.subcommand_matches("cursor-keyring") {
        return super::cursor_keyring::execute(args);
    }
    if let Some(args) = matches.subcommand_matches("me") {
        return super::self_inspection::execute(args);
    }
    match run(matches) {
        Ok((report, code, _permit)) => {
            let mut stdout = std::io::stdout().lock();
            if serde_json::to_writer(&mut stdout, &report).is_err()
                || stdout
                    .write_all(b"\n")
                    .and_then(|()| stdout.flush())
                    .is_err()
            {
                eprintln!(
                    "error: owner operation output unavailable; resolve state before retry, issued secret cannot be recovered"
                );
                return ExitCode::FAILURE;
            }
            ExitCode::from(code)
        }
        Err((code, message)) => {
            eprintln!("error: {message}");
            ExitCode::from(code)
        }
    }
}
fn run(matches: &ArgMatches) -> Result<(Value, u8, OwnedPermit)> {
    let (action, args) = matches.subcommand().expect("required action");
    let (action, args) = if action == "owner-token" {
        args.subcommand().expect("required token action")
    } else {
        (action, args)
    };
    let input = |key: &str| {
        args.get_one::<String>(key)
            .map(String::as_str)
            .expect("required argument")
    };
    let id = if action == "status" {
        None
    } else {
        let key = if action == "issue" {
            "request-id"
        } else {
            "challenge-id"
        };
        Some(
            *input(key)
                .parse::<DatasetId>()
                .map_err(|_| {
                    (
                        3,
                        "invalid nonzero bytes16 request/challenge identity".into(),
                    )
                })?
                .as_bytes(),
        )
    };
    if action == "issue" {
        validate_name(input("username")).map_err(|error| (3, error.to_string()))?;
    }
    let root = crate::config::load(Path::new(input("file")))?;
    let required = |key: &str| {
        root.text(key)
            .ok_or_else(|| (3, format!("{key}: required for local enrollment")))
    };
    let deployment = Deployment {
        dataset: required("topology.dataset_id")?
            .parse()
            .map_err(|_| (3, "invalid dataset identity".into()))?,
        instance: required("topology.instance_id")?
            .parse()
            .map_err(|_| (3, "invalid instance identity".into()))?,
    };
    let (prepared, mut grant) = postgres_command::prepare(
        &root,
        required("postgres.control.user")?,
        required("postgres.control.password_ref")?,
        HASH_RESERVATION,
    )?;
    // This finite local command has one synchronous CPU job and retains its
    // complete reservation. No detached worker survives cancellation/return.
    let token = if action == "enroll-owner" {
        let secret = SecretFile::parse(input("token-ref"))
            .and_then(|file| file.read(64))
            .map_err(|_| (3, "cannot read protected owner token".into()))?;
        Some(OwnerToken::parse_hex(secret.as_bytes()).map_err(|error| (3, error.to_string()))?)
    } else {
        None
    };
    let credential = if action == "enroll-owner" {
        let password = SecretFile::parse(input("password-ref"))
            .and_then(|file| file.read(1024))
            .map_err(|_| (3, "cannot read protected enrollment password".into()))?;
        Some(
            PasswordCredential::create(password.as_bytes(), &mut grant)
                .map_err(|error| (3, error.to_string()))?,
        )
    } else {
        None
    };
    let operation = match action {
        "issue" => Operation::Issue {
            request_id: id.unwrap(),
            username: input("username"),
        },
        "status" => Operation::Status,
        "revoke" => Operation::Revoke {
            challenge_id: id.unwrap(),
        },
        "enroll-owner" => Operation::Enroll {
            challenge_id: id.unwrap(),
            token: token.as_ref().unwrap(),
            credential: credential.as_ref().unwrap(),
        },
        _ => unreachable!("closed command"),
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| (1, "cannot start finite Control executor".into()))?;
    let result = runtime.block_on(async {
        let (mut client, driver) = postgres_command::connect(prepared)
            .await
            .map_err(|_| EnrollmentError::Backend)?;
        let started = Cell::new(false);
        let result = match tokio::time::timeout(
            Duration::from_secs(30),
            enrollment::execute(&mut client, deployment, operation, &started),
        )
        .await
        {
            Ok(result) => result,
            Err(_) if started.get() => Err(EnrollmentError::CommitUnknown),
            Err(_) => Err(EnrollmentError::Backend),
        };
        postgres_command::close(client, driver).await;
        result
    });
    runtime.shutdown_timeout(postgres_command::DRIVER_DRAIN);
    let hex = |bytes: [u8; 16]| bytes.iter().map(|b| format!("{b:02x}")).collect::<String>();
    let (report, code) = match result {
        Ok(receipt) => {
            let code = if matches!(
                receipt.status,
                "invalid-token" | "expired" | "exhausted" | "terminal"
            ) {
                3
            } else {
                0
            };
            let secret = receipt.token.as_ref().map(OwnerToken::expose_hex);
            (
                json!({"format":"bigbrotr-owner-enrollment/1","dataset_id":deployment.dataset,"status":receipt.status,
                "challenge_id":receipt.challenge.map(hex),"principal_id":receipt.principal,"username":receipt.username,
                "expires_at_us":receipt.expires_at_us.map(|n|n.to_string()),"failed_attempts":receipt.failed_attempts,
                "audit_id":receipt.audit.map(hex),"token":secret.as_ref().map(|s|s.as_str()),
                "runtime_authorization":"not-granted","session":"not-issued"}),
                code,
            )
        }
        Err(EnrollmentError::CommitUnknown) => (
            json!({"format":"bigbrotr-owner-enrollment/1","dataset_id":deployment.dataset,"status":"outcome-unknown","challenge_id":id.map(hex),"resolution":"resolve status under enrollment serialization; issuance secret is unavailable after a lost response, explicitly revoke before reissue","runtime_authorization":"not-granted","session":"not-issued"}),
            4,
        ),
        Err(error) => {
            return Err((
                if matches!(error, EnrollmentError::Backend | EnrollmentError::Entropy) {
                    1
                } else {
                    3
                },
                error.to_string(),
            ));
        }
    };
    Ok((report, code, grant))
}
