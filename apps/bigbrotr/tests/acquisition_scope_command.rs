use bigbrotr::management::{acquisition_scope::ScopeCommand, sessions::SessionError};
use bigbrotr_model::{CommandKey, DatasetId, PrincipalId};
use serde_json::{Value, json};

fn input() -> Value {
    json!({
        "endpoint_id": "44".repeat(16),
        "access_scope_id": "55".repeat(16),
        "visibility": {
            "dataset_id": "11".repeat(16), "dictionary_kind": 4,
            "allocation_domain": "66".repeat(16), "ordinal": "7"
        },
        "selector": {
            "version": 1, "filters": [{"#t": ["β", "a", "a"], "kinds": [2, 1, 1]}],
            "history_since": 0, "ephemeral": "retain"
        },
        "expected_config_revision": "2", "expected_policy_revision": "3"
    })
}
fn decode(value: &Value) -> Result<ScopeCommand, SessionError> {
    ScopeCommand::decode(&serde_json::to_vec(value).unwrap())
}
fn identities() -> (DatasetId, PrincipalId, CommandKey) {
    (
        "11".repeat(16).parse().unwrap(),
        "22".repeat(16).parse().unwrap(),
        "33".repeat(16).parse().unwrap(),
    )
}

#[test]
fn canonical_tuple_matches_independent_complete_command_digest() {
    let (dataset, actor, key) = identities();
    let command = decode(&input()).unwrap();
    assert_eq!(command.document(), "{\"version\":1,\"filters\":[{\"kinds\":[1,2],\"#t\":[\"a\",\"β\"]}],\"history_since\":0,\"ephemeral\":\"retain\"}".as_bytes());
    let digest = command.digest(dataset, actor, key).unwrap();
    assert_eq!(
        digest.to_string(),
        "1eff839b464c9d0ddaa594c6776a9e07594a6ed93f1b4fdc43836f1ee0f71579"
    );
    let mut retry = input();
    retry["selector"]["filters"] =
        json!([{"kinds":[1,2],"#t":["a","β"]},{"#t":["β","a"],"kinds":[2,1]}]);
    assert_eq!(
        decode(&retry).unwrap().digest(dataset, actor, key).unwrap(),
        digest
    );
    for field in ["endpoint_id", "access_scope_id"] {
        let mut changed = input();
        changed[field] = json!("77".repeat(16));
        assert_ne!(
            decode(&changed)
                .unwrap()
                .digest(dataset, actor, key)
                .unwrap(),
            digest
        );
    }
    for field in ["expected_config_revision", "expected_policy_revision"] {
        let mut changed = input();
        changed[field] = json!("8");
        assert_ne!(
            decode(&changed)
                .unwrap()
                .digest(dataset, actor, key)
                .unwrap(),
            digest
        );
    }
    let mut changed = input();
    changed["selector"]["history_since"] = json!(1);
    assert_ne!(
        decode(&changed)
            .unwrap()
            .digest(dataset, actor, key)
            .unwrap(),
        digest
    );
    let mut changed = input();
    changed["visibility"]["ordinal"] = json!("8");
    assert_ne!(
        decode(&changed)
            .unwrap()
            .digest(dataset, actor, key)
            .unwrap(),
        digest
    );
    assert_ne!(
        command
            .digest(dataset, "77".repeat(16).parse().unwrap(), key)
            .unwrap(),
        digest
    );
    assert_ne!(
        command
            .digest(dataset, actor, "77".repeat(16).parse().unwrap())
            .unwrap(),
        digest
    );
    assert_eq!(
        command.digest("77".repeat(16).parse().unwrap(), actor, key),
        Err(SessionError::InvalidInput)
    );
}

#[test]
fn closed_object_and_selector_keep_exact_validation() {
    for field in [
        "endpoint_id",
        "access_scope_id",
        "visibility",
        "selector",
        "expected_config_revision",
        "expected_policy_revision",
    ] {
        let mut missing = input();
        missing.as_object_mut().unwrap().remove(field);
        assert!(matches!(decode(&missing), Err(SessionError::InvalidInput)));
    }
    for (field, value) in [
        ("actor_id", json!("22".repeat(16))),
        ("policy_epoch", json!("3")),
        ("expected_config_revision", json!(2)),
        ("expected_policy_revision", json!("03")),
    ] {
        let mut bad = input();
        bad[field] = value;
        assert!(matches!(decode(&bad), Err(SessionError::InvalidInput)));
    }
    let mut bad = input();
    bad["visibility"] = json!(["11".repeat(16), 4, "66".repeat(16), "7"]);
    assert!(matches!(decode(&bad), Err(SessionError::InvalidInput)));
    let mut bad = input();
    bad["visibility"]["dictionary_kind"] = json!(3);
    assert!(matches!(decode(&bad), Err(SessionError::InvalidInput)));
    let mut bad = input();
    bad["selector"]["filters"] = json!([]);
    assert!(matches!(decode(&bad), Err(SessionError::InvalidInput)));
    let mut bad = input();
    bad["selector"]["search"] = json!("unsupported");
    assert!(matches!(decode(&bad), Err(SessionError::InvalidInput)));
    let raw = serde_json::to_string(&input()).unwrap();
    let duplicate = raw.replacen("\"selector\":", "\"selector\":{},\"selector\":", 1);
    assert!(matches!(
        ScopeCommand::decode(duplicate.as_bytes()),
        Err(SessionError::InvalidInput)
    ));
    let duplicate = raw.replace("\"kinds\":[2,1,1]", "\"kinds\":[2,1,1],\"kinds\":[1]");
    assert_ne!(duplicate, raw);
    assert!(matches!(
        ScopeCommand::decode(duplicate.as_bytes()),
        Err(SessionError::InvalidInput)
    ));
    assert!(matches!(
        ScopeCommand::decode(b"[]"),
        Err(SessionError::InvalidInput)
    ));
}

#[test]
fn envelope_and_selector_have_distinct_bounded_input_limits() {
    let mut body = serde_json::to_vec(&input()).unwrap();
    body.resize(2 * 1024 * 1024 + 1024, b' ');
    assert!(ScopeCommand::decode(&body).is_ok());
    body.push(b' ');
    assert!(matches!(
        ScopeCommand::decode(&body),
        Err(SessionError::Capacity)
    ));
    // Whitespace is valid but still consumes the selector's own wire allowance.
    let raw = serde_json::to_string(&input()).unwrap();
    let selector = serde_json::to_string(&input()["selector"]).unwrap();
    let oversized = selector.replacen(
        '{',
        &format!("{{{}", " ".repeat(2 * 1024 * 1024 + 1 - selector.len())),
        1,
    );
    let body = raw.replace(&selector, &oversized);
    assert!(body.len() < 2 * 1024 * 1024 + 1024);
    assert!(matches!(
        ScopeCommand::decode(body.as_bytes()),
        Err(SessionError::Capacity)
    ));
}

#[test]
fn diagnostics_do_not_expose_selector_values() {
    let mut value = input();
    value["selector"]["filters"][0]["#t"] = json!(["private selector marker"]);
    let command = decode(&value).unwrap();
    assert!(!format!("{command:?}").contains("private selector marker"));
}
