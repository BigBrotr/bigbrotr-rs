use super::postgres_command;
use crate::config::{Resolved, policy::PolicySeed};
use bigbrotr::runtime::OwnedPermit;
use bigbrotr::storage::postgres::{configuration, configuration::ConfigurationObservation};
use bigbrotr_model::DatasetId;
use serde_json::{Value, json};
use std::time::Duration;

type CliResult<T> = Result<T, (u8, String)>;

pub(crate) fn inspect(root: Resolved) -> CliResult<(Value, u8, OwnedPermit)> {
    let required = |key: &str| {
        root.text(key)
            .ok_or_else(|| (3, format!("{key}: required for Control inspection")))
    };
    let dataset: DatasetId = required("topology.dataset_id")?
        .parse()
        .map_err(|_| (3, "invalid dataset identity".into()))?;
    let instance: DatasetId = required("topology.instance_id")?
        .parse()
        .map_err(|_| (3, "invalid instance identity".into()))?;
    // Conservative finite parse/diagnostic allowance, not a measured RSS ceiling.
    // Held through connection termination and accepted-document validation/output.
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
            .map_err(|error| (1, error.to_string()))?;
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            configuration::inspect(&mut client, dataset, *instance.as_bytes()),
        )
        .await;
        postgres_command::close(client, driver).await;
        result
            .map_err(|_| (1, "Control inspection deadline exceeded".into()))?
            .map_err(|error| (1, error.to_string()))
    });
    runtime.shutdown_timeout(postgres_command::DRIVER_DRAIN);
    let (report, code) = report(root, result?)?;
    Ok((report, code, grant))
}

fn report(root: Resolved, observation: ConfigurationObservation) -> CliResult<(Value, u8)> {
    let accepted = match observation {
        ConfigurationObservation::NoDataset => {
            return Ok((
                json!({"format":"bigbrotr-config-inspection/1","status":"dataset-not-initialized","activation":"not-performed","authorization":"not-granted"}),
                4,
            ));
        }
        ConfigurationObservation::NoConfiguration => {
            return Ok((
                json!({"format":"bigbrotr-config-inspection/1","status":"configuration-not-initialized","activation":"not-performed","authorization":"not-granted"}),
                4,
            ));
        }
        ConfigurationObservation::Accepted(accepted) => accepted,
    };
    if root.text("config.authority_mode") != Some(accepted.authority_mode.as_str()) {
        return Err((
            3,
            "deployment authority mode disagrees with accepted Control state".into(),
        ));
    }
    let seed = PolicySeed::from_resolved(&root).map_err(|error| (3, error))?;
    let policy_differs = seed.digest() != accepted.document_digest;
    let hex = |bytes: &[u8]| bytes.iter().map(|b| format!("{b:02x}")).collect::<String>();
    let source_differs = root.document_sha256 != hex(&accepted.source_digest);
    let effective = PolicySeed::restore(&accepted.document, accepted.document_digest, root)
        .map_err(|error| (3, error))?;
    let mut metadata = effective.report(true).map_err(|error| (3, error))?;
    Ok((
        json!({"format":"bigbrotr-config-inspection/1","status":"accepted-configuration-observed",
        "authority_mode":accepted.authority_mode,"desired_revision":accepted.revision.to_string(),
        "security_revision":accepted.security_revision.map(|n| n.to_string()),"applied_revision":null,
        "accepted_policy_sha256":hex(&accepted.document_digest),"source_file_differs":source_differs,
        "candidate_policy_differs":policy_differs,"file_policy_use":if accepted.authority_mode == "managed" {"unused-initialization-input"} else {"staged-until-explicit-activation"},
        "activation":"not-performed","authorization":"not-granted","snapshot":"single-statement-observation",
        "settings":metadata["settings"].take()}),
        0,
    ))
}
