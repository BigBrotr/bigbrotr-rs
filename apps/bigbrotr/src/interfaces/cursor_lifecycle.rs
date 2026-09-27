//! Finite protected installation commands, never an HTTP authorization path.
use super::{cursor_keyring::key_id, postgres_command};
use bigbrotr::{
    read::keyring::{KEYRING_RESERVATION, PreparedKeyring},
    storage::postgres::cursor_lifecycle::{self, Action, LifecycleError, Request},
};
use bigbrotr_model::{DatasetId, Digest256};
use clap::{Arg, ArgMatches, Command};
use std::{cell::Cell, path::PathBuf, time::Duration};

pub(super) fn commands(mut command: Command) -> Command {
    for name in ["activate-key", "revoke-key"] {
        command = command.subcommand(Command::new(name)
            .about("Accept an explicit cursor key transition while Control is stopped")
            .arg(Arg::new("file").long("file").required(true).value_parser(clap::value_parser!(PathBuf)))
            .arg(Arg::new("key-id").long("key-id").required(true).value_parser(key_id))
            .arg(Arg::new("request-id").long("request-id").required(true).value_parser(key_id))
            .arg(Arg::new("expected-revision").long("expected-revision").required(true).value_parser(revision))
            .after_help("Requires migration0604, an enrolled ring and protected installation credentials. Stop Control cleanly first; an expired but unreleased owner still refuses new transitions. Expected revision is zero before the first transition. Activation requires matching local material; revocation is permanent, even for a never-activated key. Historical identical request resolves before current revision/readiness checks, including after restart. Retain the request and keys on an unknown COMMIT and retry exactly on a fresh connection. This receipt is not worker-distribution proof or a cursor access grant. Exit:0 accepted/resolved,1 connection/output failure,2 usage,3 configuration/schema/conflict/unavailable,4 local keys/entropy/COMMIT unknown."));
    }
    command
}
fn revision(text: &str) -> Result<u64, String> {
    text.parse::<u64>()
        .ok()
        .filter(|value| value.to_string() == text)
        .ok_or_else(|| "expected canonical unsigned64 decimal revision".into())
}

pub(super) fn execute(
    config: &crate::config::Resolved,
    reference: &str,
    dataset: DatasetId,
    operation: &str,
    args: &ArgMatches,
) -> Result<serde_json::Value, (u8, String)> {
    let request = Request {
        request_id: *args
            .get_one::<[u8; 16]>("request-id")
            .expect("required request ID"),
        key_id: *args.get_one::<[u8; 16]>("key-id").expect("required key ID"),
        expected_revision: *args
            .get_one::<u64>("expected-revision")
            .expect("required revision"),
        action: if operation == "activate-key" {
            Action::Activate
        } else {
            Action::Revoke
        },
    };
    let required = |key: &str| {
        config
            .text(key)
            .ok_or_else(|| (3, format!("{key}: required for cursor lifecycle")))
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
    let prepared = PreparedKeyring::existing(reference, dataset, permit)
        .map_err(|error| (4, error.to_string()))?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| (1, "cannot start finite Control executor".into()))?;
    let result = runtime.block_on(async {
        let (mut client, driver) = postgres_command::connect(connection)
            .await
            .map_err(|_| LifecycleError::Backend)?;
        let started = Cell::new(false);
        let result = match tokio::time::timeout(
            Duration::from_secs(30),
            cursor_lifecycle::transition(&mut client, instance, &prepared, request, &started),
        )
        .await
        {
            Ok(result) => result,
            Err(_) if started.get() => Err(LifecycleError::CommitUnknown),
            Err(_) => Err(LifecycleError::Backend),
        };
        postgres_command::close(client, driver).await;
        result
    });
    // Prepared secret bytes/combined reservation remain alive through the actual
    // driver join and executor shutdown, including unknown commit and timeout.
    runtime.shutdown_timeout(postgres_command::DRIVER_DRAIN);
    let hex = |bytes: [u8; 16]| format!("{:032x}", u128::from_be_bytes(bytes));
    match result {
        Ok(receipt) => Ok(serde_json::json!({
            "format":"bigbrotr-cursor-key-lifecycle/1",
            "state":if receipt.created { "accepted" } else { "already-accepted" },
            "dataset_id":dataset, "keyring_id":hex(prepared.keys().id()),
            "request_id":hex(request.request_id), "action":request.action.as_str(),
            "key_id":hex(request.key_id), "expected_revision":request.expected_revision.to_string(),
            "revision":receipt.revision.to_string(), "active_key_id":receipt.active_key.map(hex),
            "actor_id":receipt.actor, "audit_id":hex(receipt.audit_id),
            "accepted_at_us":receipt.accepted_at_us.to_string(),
            "request_digest_sha256":Digest256::from_bytes(receipt.request_digest),
            "cursor_authority":"not-granted"
        })),
        Err(LifecycleError::CommitUnknown) => Err((4, "cursor key lifecycle COMMIT outcome unknown; retain keys and retry the identical request on a fresh connection".into())),
        Err(error) => Err((match error {
            LifecycleError::Backend => 1,
            LifecycleError::Entropy => 4,
            _ => 3,
        }, error.to_string())),
    }
}
