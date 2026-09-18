use super::postgres_command;
use bigbrotr::{
    management::credentials::{HASH_RESERVATION, validate_name},
    runtime::OwnedPermit,
    secrets::SecretFile,
    storage::postgres::{
        authentication::{self, AuthenticationError},
        enrollment::Deployment,
    },
};
use clap::{Arg, ArgMatches, Command};
use serde_json::{Value, json};
use std::{io::Write, path::Path, process::ExitCode, time::Duration};

pub(super) fn command() -> Command {
    Command::new("me")
        .about("Verify a local account password and inspect its stored grants")
        .after_help("Requires protected Control credentials. Read-only observation; no session or runtime authority is issued.")
        .arg(Arg::new("file").long("file").required(true).value_name("CONFIG"))
        .arg(Arg::new("username").long("username").required(true))
        .arg(Arg::new("password-ref").long("password-ref").required(true).value_name("file:/ABSOLUTE/PATH"))
}
pub(super) fn execute(args: &ArgMatches) -> ExitCode {
    match run(args) {
        Ok((report, _permit)) => {
            let mut stdout = std::io::stdout().lock();
            if serde_json::to_writer(&mut stdout, &report).is_err()
                || stdout
                    .write_all(b"\n")
                    .and_then(|()| stdout.flush())
                    .is_err()
            {
                eprintln!("error: local account observation output unavailable");
                return ExitCode::FAILURE;
            }
            ExitCode::SUCCESS
        }
        Err((code, message)) => {
            eprintln!("error: {message}");
            ExitCode::from(code)
        }
    }
}
fn run(args: &ArgMatches) -> Result<(Value, OwnedPermit), (u8, String)> {
    let input = |key: &str| {
        args.get_one::<String>(key)
            .map(String::as_str)
            .expect("required argument")
    };
    validate_name(input("username")).map_err(|_| (3, AuthenticationError::Denied.to_string()))?;
    let root = crate::config::load(Path::new(input("file")))?;
    let required = |key: &str| {
        root.text(key)
            .ok_or_else(|| (3, format!("{key}: required for local account inspection")))
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
    let password = SecretFile::parse(input("password-ref"))
        .and_then(|file| file.read(1024))
        .map_err(|_| (3, "cannot read protected account password".into()))?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| (1, "cannot start finite Control executor".into()))?;
    let result = runtime.block_on(async {
        let (mut client, driver) = postgres_command::connect(prepared)
            .await
            .map_err(|_| AuthenticationError::Backend)?;
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            authentication::inspect_self(
                &mut client,
                deployment,
                input("username"),
                password.as_bytes(),
                &mut grant,
            ),
        )
        .await
        .unwrap_or(Err(AuthenticationError::Backend));
        postgres_command::close(client, driver).await;
        result
    });
    runtime.shutdown_timeout(postgres_command::DRIVER_DRAIN);
    let account = result.map_err(|error| {
        (
            if matches!(
                error,
                AuthenticationError::Backend | AuthenticationError::Capacity
            ) {
                1
            } else {
                3
            },
            error.to_string(),
        )
    })?;
    Ok((
        json!({"format":"bigbrotr-local-account/1", "dataset_id":deployment.dataset,
        "principal_id":account.principal, "username":input("username"),
        "credential_revision":account.credential_revision.to_string(), "grant_revision":account.grant_revision.to_string(),
        "security_revision":account.security_revision.map(|n|n.to_string()),
        "stored_actions":account.stored_actions, "resource_kind":"dataset", "resource_id":deployment.dataset,
        "runtime_authorization":"not-granted", "session":"not-issued"}),
        grant,
    ))
}
