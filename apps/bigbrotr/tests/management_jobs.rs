//! Request identity only. Actual atomic acceptance/retention needs PostgreSQL.
use bigbrotr::management::jobs::JobCommand;
use serde_json::Value;

fn vectors() -> Value {
    serde_json::from_str(include_str!(
        "../../../tests/fixtures/management/export-command.json"
    ))
    .unwrap()
}

#[test]
fn export_command_matches_independent_fixed_width_identity() {
    let vectors = vectors();
    for v in vectors["vectors"].as_array().unwrap() {
        let command: JobCommand = serde_json::from_value(v["command"].clone()).unwrap();
        let digest = command
            .digest(
                v["dataset"].as_str().unwrap().parse().unwrap(),
                v["actor"].as_str().unwrap().parse().unwrap(),
                v["command_key"].as_str().unwrap().parse().unwrap(),
            )
            .unwrap();
        assert_eq!(digest.to_string(), v["sha256"]);
    }
    assert_eq!(
        vectors["vectors"][0]["sha256"],
        vectors["vectors"][2]["sha256"]
    );
}

#[test]
fn export_identity_cannot_change_actor_key_inputs_or_destination() {
    let v = vectors()["vectors"][0].clone();
    let dataset = v["dataset"].as_str().unwrap().parse().unwrap();
    let actor = v["actor"].as_str().unwrap().parse().unwrap();
    let key = v["command_key"].as_str().unwrap().parse().unwrap();
    let parse = |v: Value| serde_json::from_value::<JobCommand>(v).unwrap();
    let command = parse(v["command"].clone());
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
        "/query/scope/allocation_domain",
        "/query/scope/ordinal",
        "/query/filters/0/ids/0",
        "/query/page_size",
        "/allocation/catalog_id",
        "/allocation/frontier_revision",
        "/destination/id",
        "/destination/binding_sha256",
        "/expected_config_revision",
        "/expected_policy_revision",
    ] {
        let mut changed = v["command"].clone();
        let field = changed.pointer_mut(path).unwrap();
        *field = match path {
            "/query/page_size" => 9.into(),
            "/query/filters/0/ids/0" | "/destination/binding_sha256" => "09".repeat(32).into(),
            _ if path.ends_with("domain") || path.ends_with("id") => "09".repeat(16).into(),
            _ => "9".into(),
        };
        assert_ne!(
            parse(changed).digest(dataset, actor, key).unwrap(),
            digest,
            "{path}"
        );
    }
}

#[test]
fn export_never_silently_discards_unsupported_or_ambiguous_input() {
    let v = vectors()["vectors"][0].clone();
    let dataset = v["dataset"].as_str().unwrap().parse().unwrap();
    let actor = v["actor"].as_str().unwrap().parse().unwrap();
    let key = v["command_key"].as_str().unwrap().parse().unwrap();
    for (path, value) in [
        ("/kind", Value::from("backup")),
        ("/format", "analytical-projection".into()),
        ("/query/mode", "traversal".into()),
        ("/query/cursor", "retained".into()),
        ("/query/projection", serde_json::json!([])),
        ("/query/scope/dataset_id", "09".repeat(16).into()),
        ("/query/scope/dictionary_kind", 3.into()),
        ("/query/filters/0/ids", serde_json::json!([])),
        ("/query/page_size", 0.into()),
        ("/destination/id", "00".repeat(16).into()),
        ("/destination/binding_sha256", "06".repeat(31).into()),
        ("/allocation/frontier_revision", "01".into()),
        ("/expected_config_revision", 1.into()),
        ("/expected_policy_revision", "18446744073709551616".into()),
    ] {
        let mut changed = v["command"].clone();
        *changed.pointer_mut(path).unwrap() = value;
        assert!(
            serde_json::from_value::<JobCommand>(changed)
                .map(|c| c.digest(dataset, actor, key).is_err())
                .unwrap_or(true),
            "{path}"
        );
    }
    for object in ["", "/query", "/allocation", "/destination"] {
        let mut changed = v["command"].clone();
        changed
            .pointer_mut(object)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert("path".into(), "/host/private".into());
        assert!(
            serde_json::from_value::<JobCommand>(changed).is_err(),
            "{object}"
        );
    }
    let wire = serde_json::to_string(&v["command"]).unwrap();
    assert!(
        serde_json::from_str::<JobCommand>(&wire.replace(
            "\"kind\":\"export\"",
            "\"kind\":\"export\",\"kind\":\"export\""
        ))
        .is_err()
    );
}
