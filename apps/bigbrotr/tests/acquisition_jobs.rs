use bigbrotr::acquisition::job::{AcquisitionCommand, JobDuration};
use bigbrotr_model::{CommandKey, DatasetId, PrincipalId};
use serde_json::{Value, json};

fn command() -> Value {
    json!({
        "scope_id": "04040404040404040404040404040404",
        "deadline_us": "120000000",
        "expected_config_revision": "5",
        "expected_policy_revision": "6"
    })
}

#[test]
fn finite_acquisition_identity_matches_an_independent_literal_vector() {
    let command: AcquisitionCommand = serde_json::from_value(command()).unwrap();
    let dataset = DatasetId::try_from([1; 16]).unwrap();
    let actor = PrincipalId::try_from([2; 16]).unwrap();
    let key = CommandKey::try_from([3; 16]).unwrap();
    assert_eq!(
        command.digest(dataset, actor, key).to_string(),
        "9f03583ab32e8152a0b96190c83af4f3096386c1e9d6405fed978bce13a80301"
    );
    assert_eq!(command.deadline_us.get(), 120_000_000);
}

#[test]
fn command_requires_canonical_bounded_duration_and_closed_fields() {
    for duration in [
        json!("0"),
        json!("-1"),
        json!("01"),
        json!("1.0"),
        json!(1),
        Value::Null,
        json!("86400000001"),
        json!("18446744073709551615"),
    ] {
        let mut value = command();
        value["deadline_us"] = duration;
        assert!(serde_json::from_value::<AcquisitionCommand>(value).is_err());
    }
    for duration in [1, 86_400_000_000] {
        assert_eq!(JobDuration::new(duration).unwrap().get(), duration);
    }
    assert!(JobDuration::new(0).is_err());
    assert!(JobDuration::new(86_400_000_001).is_err());
    for missing in [
        "scope_id",
        "deadline_us",
        "expected_config_revision",
        "expected_policy_revision",
    ] {
        let mut value = command();
        value.as_object_mut().unwrap().remove(missing);
        assert!(serde_json::from_value::<AcquisitionCommand>(value).is_err());
    }
    let mut value = command();
    value["endpoint"] = json!("wss://different.example");
    assert!(serde_json::from_value::<AcquisitionCommand>(value).is_err());
    let duplicate = serde_json::to_string(&command()).unwrap().replace(
        "\"deadline_us\":\"120000000\"",
        "\"deadline_us\":\"120000000\",\"deadline_us\":\"120000000\"",
    );
    assert!(serde_json::from_str::<AcquisitionCommand>(&duplicate).is_err());
}

#[test]
fn original_command_binds_scope_duration_revisions_actor_dataset_and_key() {
    let dataset = DatasetId::try_from([1; 16]).unwrap();
    let actor = PrincipalId::try_from([2; 16]).unwrap();
    let key = CommandKey::try_from([3; 16]).unwrap();
    let original: AcquisitionCommand = serde_json::from_value(command()).unwrap();
    let digest = original.digest(dataset, actor, key);
    assert_ne!(
        digest,
        original.digest([9; 16].try_into().unwrap(), actor, key)
    );
    assert_ne!(
        digest,
        original.digest(dataset, [9; 16].try_into().unwrap(), key)
    );
    assert_ne!(
        digest,
        original.digest(dataset, actor, [9; 16].try_into().unwrap())
    );
    for (field, value) in [
        ("scope_id", "09090909090909090909090909090909"),
        ("deadline_us", "120000001"),
        ("expected_config_revision", "7"),
        ("expected_policy_revision", "7"),
    ] {
        let mut value_to_parse = command();
        value_to_parse[field] = json!(value);
        let changed: AcquisitionCommand = serde_json::from_value(value_to_parse).unwrap();
        assert_ne!(digest, changed.digest(dataset, actor, key), "{field}");
    }
}

#[test]
fn a_finite_command_is_an_object_not_a_positional_array() {
    let array = json!(["04040404040404040404040404040404", "120000000", "5", "6"]);
    assert!(serde_json::from_value::<AcquisitionCommand>(array).is_err());
}
