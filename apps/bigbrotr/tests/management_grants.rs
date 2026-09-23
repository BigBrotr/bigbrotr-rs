use bigbrotr::management::grants::{CommandKey, GrantCommand};
use bigbrotr_model::DatasetId;
use serde_json::Value;
fn vectors() -> Value {
    serde_json::from_str(include_str!(
        "../../../tests/fixtures/management/scope-grant.json"
    ))
    .unwrap()
}
#[test]
fn scope_grant_command_matches_independent_fixed_width_bindings() {
    for vector in vectors()["vectors"].as_array().unwrap() {
        let command: GrantCommand = serde_json::from_value(vector["command"].clone()).unwrap();
        let key: CommandKey = vector["command_key"].as_str().unwrap().parse().unwrap();
        let digest = command
            .digest(
                vector["dataset"].as_str().unwrap().parse().unwrap(),
                vector["actor"].as_str().unwrap().parse().unwrap(),
                key,
            )
            .unwrap();
        assert_eq!(digest.to_string(), vector["sha256"].as_str().unwrap());
        assert_eq!(serde_json::to_value(command).unwrap(), vector["command"]);
    }
}
#[test]
fn grant_input_refuses_ambiguous_revisions_unqualified_scopes_and_future_allocations() {
    let sample = vectors()["vectors"][0]["command"].clone();
    let dataset: DatasetId = "01".repeat(16).parse().unwrap();
    for bad in [
        Value::from(1),
        Value::from("01"),
        Value::from("0"),
        Value::from("18446744073709551616"),
    ] {
        let mut value = sample.clone();
        value["expected_grant_revision"] = bad;
        assert!(serde_json::from_value::<GrantCommand>(value).is_err());
    }
    for case in [
        "kind",
        "dataset",
        "unallocated",
        "revoke-allocation",
        "unknown-field",
        "unknown-action",
        "omitted-allocation",
    ] {
        let mut value = sample.clone();
        match case {
            "kind" => value["scope"]["dictionary_kind"] = 3.into(),
            "dataset" => value["scope"]["dataset_id"] = "02".repeat(16).into(),
            "unallocated" => value["allocation"] = Value::Null,
            "revoke-allocation" => value["enabled"] = false.into(),
            "unknown-field" => value["sql"] = "SELECT 1".into(),
            "omitted-allocation" => {
                value.as_object_mut().unwrap().remove("allocation");
            }
            _ => value["action"] = "data.read".into(),
        }
        assert!(
            serde_json::from_value::<GrantCommand>(value)
                .map(|v| v.validate(dataset).is_err())
                .unwrap_or(true),
            "{case}"
        );
    }
    for bad in [
        "00".repeat(16),
        "AB".repeat(16),
        "01".repeat(15),
        "0g".repeat(16),
    ] {
        assert!(bad.parse::<CommandKey>().is_err());
    }
}

#[test]
fn proof_fingerprint_cannot_be_reused_for_another_actor_key_or_grant_change() {
    let vector = vectors()["vectors"][0].clone();
    let command: GrantCommand = serde_json::from_value(vector["command"].clone()).unwrap();
    let dataset = vector["dataset"].as_str().unwrap().parse().unwrap();
    let actor = vector["actor"].as_str().unwrap().parse().unwrap();
    let key = vector["command_key"].as_str().unwrap().parse().unwrap();
    let digest = command.digest(dataset, actor, key).unwrap();
    assert_ne!(
        command
            .digest(dataset, "09".repeat(16).parse().unwrap(), key)
            .unwrap(),
        digest
    );
    assert_ne!(
        command
            .digest(dataset, actor, "09".repeat(16).parse().unwrap())
            .unwrap(),
        digest
    );
    for path in [
        "/principal_id",
        "/scope/allocation_domain",
        "/scope/ordinal",
        "/expected_config_revision",
        "/expected_policy_revision",
        "/expected_grant_revision",
        "/allocation/catalog_id",
        "/allocation/frontier_revision",
    ] {
        let mut changed = vector["command"].clone();
        *changed.pointer_mut(path).unwrap() = if path.ends_with("_id") || path.ends_with("domain") {
            "09".repeat(16).into()
        } else {
            "23".into()
        };
        let changed: GrantCommand = serde_json::from_value(changed).unwrap();
        assert_ne!(
            changed.digest(dataset, actor, key).unwrap(),
            digest,
            "{path}"
        );
    }
}

#[test]
fn scoped_export_actions_have_distinct_command_authority() {
    let vector = vectors()["vectors"][0].clone();
    let dataset = vector["dataset"].as_str().unwrap().parse().unwrap();
    let actor = vector["actor"].as_str().unwrap().parse().unwrap();
    let key = vector["command_key"].as_str().unwrap().parse().unwrap();
    let mut digests = std::collections::BTreeSet::new();
    for action in ["history.read", "export", "private-export"] {
        let mut value = vector["command"].clone();
        value["action"] = action.into();
        let command: GrantCommand = serde_json::from_value(value).unwrap();
        assert_eq!(command.action.as_str(), action);
        assert!(digests.insert(command.digest(dataset, actor, key).unwrap().to_string()));
    }
    // None of the data actions permits publication, signing or generic job control.
    for action in [
        "artifact.publish",
        "artifact.withdraw",
        "sign",
        "roles.control",
        "export.all",
    ] {
        let mut value = vector["command"].clone();
        value["action"] = action.into();
        assert!(serde_json::from_value::<GrantCommand>(value).is_err());
    }
}
