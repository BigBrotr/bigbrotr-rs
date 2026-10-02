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

#[test]
fn unknown_fields_are_rejected_before_loading_their_nested_values() {
    // An internally tagged enum first buffers the entire unknown value into
    // Serde Content. Small wire inputs can then exceed the64KiB request grant.
    // A counted byte reader discriminates immediate shape refusal from buffering
    // followed by refusal; unknown values must never be loaded into any tree.
    for prefix in [r#"{"kind":"export","x":"#, r#"{"x":"#] {
        let body = format!("{prefix}[{}]}}", vec!["[0]"; 1500].join(","));
        assert!(body.len() < 8192);
        let mut reader = std::io::Cursor::new(body.as_bytes());
        assert!(serde_json::from_reader::<_, JobCommand>(&mut reader).is_err());
        assert!(
            reader.position() <= prefix.len() as u64,
            "unknown nested value was consumed: {}bytes",
            reader.position()
        );
    }
}

#[test]
fn unsupported_projection_is_refused_before_loading_its_vector() {
    let mut command = vectors()["vectors"][0]["command"].clone();
    command["query"]["projection"] = serde_json::json!(vec![""; 2049]);
    let body = command.to_string();
    assert!(body.len() < 8192);
    let value_start = body.find("\"projection\":").unwrap() + "\"projection\":".len();
    let mut reader = std::io::Cursor::new(body.as_bytes());
    assert!(serde_json::from_reader::<_, JobCommand>(&mut reader).is_err());
    assert!(reader.position() <= value_start as u64 + 1);
}

#[test]
fn finite_acquisition_uses_the_same_typed_job_envelope_and_original_digest() {
    let body = r#"{"scope_id":"04040404040404040404040404040404","deadline_us":"120000000","expected_config_revision":"5","expected_policy_revision":"6","kind":"acquisition"}"#;
    let command: JobCommand = serde_json::from_str(body).unwrap();
    assert_eq!(
        serde_json::to_value(&command).unwrap(),
        serde_json::from_str::<Value>(body).unwrap()
    );
    assert_eq!(
        command
            .digest(
                [1; 16].try_into().unwrap(),
                [2; 16].try_into().unwrap(),
                [3; 16].try_into().unwrap()
            )
            .unwrap()
            .to_string(),
        "9f03583ab32e8152a0b96190c83af4f3096386c1e9d6405fed978bce13a80301"
    );

    // Neither kind may accept fields from the other variant, even before kind.
    for (field, value) in [
        ("format", serde_json::json!("signed-events-jsonl")),
        ("query", vectors()["vectors"][0]["command"]["query"].clone()),
        (
            "allocation",
            vectors()["vectors"][0]["command"]["allocation"].clone(),
        ),
        (
            "destination",
            vectors()["vectors"][0]["command"]["destination"].clone(),
        ),
    ] {
        let mut changed: Value = serde_json::from_str(body).unwrap();
        changed[field] = value;
        assert!(
            serde_json::from_value::<JobCommand>(changed).is_err(),
            "{field}"
        );
    }
    for (field, value) in [
        ("scope_id", "04040404040404040404040404040404"),
        ("deadline_us", "1"),
    ] {
        let mut changed = vectors()["vectors"][0]["command"].clone();
        changed[field] = value.into();
        assert!(
            serde_json::from_value::<JobCommand>(changed).is_err(),
            "{field}"
        );
    }
    for duration in [
        serde_json::json!(0),
        serde_json::json!("0"),
        serde_json::json!("01"),
        serde_json::json!("86400000001"),
    ] {
        let mut changed: Value = serde_json::from_str(body).unwrap();
        changed["deadline_us"] = duration;
        assert!(serde_json::from_value::<JobCommand>(changed).is_err());
    }
    assert!(
        serde_json::from_str::<JobCommand>(&body.replace(
            "\"kind\":\"acquisition\"",
            "\"kind\":\"acquisition\",\"kind\":\"acquisition\""
        ))
        .is_err()
    );
    assert!(serde_json::from_str::<JobCommand>("[]").is_err());
}

#[test]
fn acquisition_status_is_typed_redacted_and_keeps_exact_revision_strings() {
    use bigbrotr::management::jobs::{JobKind, JobObservation, JobState};
    let observation = JobObservation {
        kind: JobKind::Acquisition,
        job_id: [9; 16].try_into().unwrap(),
        state: JobState::Accepted,
        job_revision: 1,
        checkpoint_revision: 0,
        created_at_us: i64::MIN,
        completed_at_us: None,
        observed_at_us: i64::MAX,
        reason: None,
    };
    assert_eq!(
        serde_json::to_value(observation).unwrap(),
        serde_json::json!({
            "format":"bigbrotr-job-status/1", "job_id":"09".repeat(16), "kind":"acquisition",
            "state":"accepted", "job_revision":"1", "checkpoint_revision":"0",
            "created_at_us":"-9223372036854775808", "completed_at_us":null,
            "observed_at_us":"9223372036854775807", "reason_code":null, "authority":"observation-only"
        })
    );
}
