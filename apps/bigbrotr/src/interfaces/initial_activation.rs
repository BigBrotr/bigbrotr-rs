use super::postgres_command;
use crate::config::{Resolved, policy::PolicySeed};
use bigbrotr::{
    runtime::OwnedPermit,
    runtime::policy_document::PolicyDocumentVersion,
    storage::postgres::activation::{self, ActivationError, ActivationRequest},
};
use bigbrotr_model::DatasetId;
use serde_json::{Value, json};
use std::{cell::Cell, time::Duration};

type CliResult<T> = Result<T, (u8, String)>;

pub(crate) fn parse_request(id: &str, revision: &str) -> CliResult<([u8; 16], u64)> {
    let id: DatasetId = id
        .parse()
        .map_err(|_| (3, "invalid activation request ID".into()))?;
    let revision = revision
        .parse::<u64>()
        .ok()
        .filter(|n| *n > 0 && n.to_string() == revision)
        .ok_or_else(|| (3, "invalid expected configuration revision".into()))?;
    Ok((*id.as_bytes(), revision))
}

pub(crate) fn activate(
    root: Resolved,
    request_id: [u8; 16],
    expected_revision: u64,
) -> CliResult<(Value, u8, OwnedPermit)> {
    let required = |key: &str| {
        root.text(key)
            .ok_or_else(|| (3, format!("{key}: required for initial activation")))
    };
    let dataset: DatasetId = required("topology.dataset_id")?
        .parse()
        .map_err(|_| (3, "invalid dataset identity".into()))?;
    let instance: DatasetId = required("topology.instance_id")?
        .parse()
        .map_err(|_| (3, "invalid instance identity".into()))?;
    let mode = required("config.authority_mode")?.to_owned();
    let (prepared, grant) = postgres_command::prepare(
        &root,
        required("postgres.control.user")?,
        required("postgres.control.password_ref")?,
        512 * 1024 * 1024,
    )?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| (1, "cannot start finite Control executor".into()))?;
    let result = runtime.block_on(async {
        let (mut client, driver) = postgres_command::connect(prepared)
            .await
            .map_err(|_| ActivationError::Backend)?;
        let commit_started = Cell::new(false);
        let validate = |document: &str, digest| {
            let version = PolicyDocumentVersion::from_document(document)
                .map_err(|_| ActivationError::PolicyInvalid)?;
            if mode == "declarative"
                && !PolicySeed::matches_accepted(&root, version, digest)
                    .map_err(|_| ActivationError::PolicyInvalid)?
            {
                return Err(ActivationError::PolicyInvalid);
            }
            PolicySeed::restore(document, digest, root)
                .map(|_| ())
                .map_err(|_| ActivationError::PolicyInvalid)
        };
        let result = match tokio::time::timeout(
            Duration::from_secs(30),
            activation::activate_initial(
                &mut client,
                ActivationRequest {
                    dataset,
                    instance,
                    request_id,
                    expected_revision,
                    authority_mode: &mode,
                },
                validate,
                &commit_started,
            ),
        )
        .await
        {
            Ok(result) => result,
            Err(_) if commit_started.get() => Err(ActivationError::CommitUnknown),
            Err(_) => Err(ActivationError::Backend),
        };
        postgres_command::close(client, driver).await;
        result
    });
    runtime.shutdown_timeout(postgres_command::DRIVER_DRAIN);
    let hex = |bytes: &[u8]| bytes.iter().map(|b| format!("{b:02x}")).collect::<String>();
    let (report, code) = match result {
        Ok(receipt) => (
            json!({"format":"bigbrotr-initial-activation/1",
            "status":if receipt.created {"activated"} else {"already-activated"},
            "dataset_id":dataset,"instance_id":instance,"request_id":hex(&request_id),
            "config_revision":expected_revision.to_string(),"security_revision":"1",
            "installation_actor":receipt.actor,"audit_id":hex(&receipt.audit_id),
            "policy_sha256":hex(&receipt.document_digest),"request_sha256":hex(&receipt.request_digest),
            "initial_barrier":"no-prior-runtime","runtime_authorization":"not-granted"}),
            0,
        ),
        Err(ActivationError::CommitUnknown) => (
            json!({"format":"bigbrotr-initial-activation/1",
            "status":"outcome-unknown","dataset_id":dataset,"instance_id":instance,
            "request_id":hex(&request_id),"config_revision":expected_revision.to_string(),
            "resolution":"retry activate-initial with the same request ID, expected revision and deployment roots",
            "runtime_authorization":"not-granted"}),
            4,
        ),
        Err(error) => {
            return Err((
                if error == ActivationError::Backend {
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
