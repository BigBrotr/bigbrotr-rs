use bigbrotr::management::relay_registration::RegistrationCommand;
use bigbrotr_model::{CommandKey, DatasetId, PrincipalId};
use serde_json::{Value, json};

fn input() -> Value {
    json!({
        "url": "WSS://EXAMPLE.test.:443/a/../%62?b=2&a=%7e",
        "expected_config_revision": "2",
        "expected_policy_revision": "3"
    })
}

#[test]
fn normalized_registration_matches_independent_command_bytes() {
    let dataset: DatasetId = "11".repeat(16).parse().unwrap();
    let actor: PrincipalId = "22".repeat(16).parse().unwrap();
    let key: CommandKey = "33".repeat(16).parse().unwrap();
    let command: RegistrationCommand = serde_json::from_value(input()).unwrap();
    assert_eq!(command.url.as_str(), "wss://example.test/b?b=2&a=%7e");
    let digest = command.digest(dataset, actor, key);
    assert_eq!(
        digest.to_string(),
        "f7c4a08943d596774195e290e0304b30374d5fc761ffafdd752b31a7af955927"
    );
    let normalized = serde_json::to_value(&command).unwrap();
    assert_eq!(normalized["url"], command.url.as_str());
    let retry: RegistrationCommand = serde_json::from_value(normalized).unwrap();
    assert_eq!(retry.digest(dataset, actor, key), digest);
    for (field, value) in [
        ("url", "wss://example.test/b?b=2&a=%7E"),
        ("url", "wss://example.test/b?a=%7e&b=2"),
        ("url", "wss://example.test/b?"),
        ("url", "wss://example.test/b"),
        ("expected_config_revision", "4"),
        ("expected_policy_revision", "4"),
    ] {
        let mut changed = input();
        changed[field] = json!(value);
        let changed: RegistrationCommand = serde_json::from_value(changed).unwrap();
        assert_ne!(changed.digest(dataset, actor, key), digest);
    }
    assert_ne!(
        command.digest("44".repeat(16).parse().unwrap(), actor, key),
        digest
    );
    assert_ne!(
        command.digest(dataset, "44".repeat(16).parse().unwrap(), key),
        digest
    );
    assert_ne!(
        command.digest(dataset, actor, "44".repeat(16).parse().unwrap()),
        digest
    );
}

#[test]
fn closed_input_requires_valid_endpoint_and_exact_revisions() {
    for field in [
        "url",
        "expected_config_revision",
        "expected_policy_revision",
    ] {
        let mut value = input();
        value.as_object_mut().unwrap().remove(field);
        assert!(serde_json::from_value::<RegistrationCommand>(value).is_err());
    }
    for url in [
        "https://example.test/",
        "ws://name:secret@example.test/",
        "ws://example.test/#private",
        "ws://127.1/",
        "ws://example.test/%zz",
    ] {
        let mut value = input();
        value["url"] = json!(url);
        assert!(serde_json::from_value::<RegistrationCommand>(value).is_err());
    }
    let mut unknown = input();
    unknown["actor_id"] = json!("22".repeat(16));
    assert!(serde_json::from_value::<RegistrationCommand>(unknown).is_err());
    let duplicate = r#"{"url":"ws://a.test/","url":"ws://b.test/","expected_config_revision":"1","expected_policy_revision":"1"}"#;
    assert!(serde_json::from_str::<RegistrationCommand>(duplicate).is_err());
    for value in [
        json!(2),
        json!("02"),
        json!("0"),
        json!("18446744073709551616"),
    ] {
        let mut command = input();
        command["expected_config_revision"] = value;
        assert!(serde_json::from_value::<RegistrationCommand>(command).is_err());
    }
}

#[test]
fn endpoint_byte_limit_survives_command_decoding() {
    let prefix = "wss://example.test/";
    let mut value = input();
    value["url"] = json!(format!("{prefix}{}", "a".repeat(8192 - prefix.len())));
    let command: RegistrationCommand = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(command.url.as_str().len(), 8192);
    value["url"] = json!(format!("{prefix}{}", "a".repeat(8193 - prefix.len())));
    assert!(serde_json::from_value::<RegistrationCommand>(value).is_err());
    assert!(!format!("{command:?}").contains(&"a".repeat(64)));
}
