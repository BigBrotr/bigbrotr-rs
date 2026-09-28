use bigbrotr::management::relay_enrollment::EnrollmentCommand;
use bigbrotr_model::{CommandKey, DatasetId, PrincipalId};
use serde_json::{Value, json};
fn input() -> Value {
    json!({
        "endpoint_id":"44444444444444444444444444444444",
        "visibility":{"dataset_id":"11111111111111111111111111111111","dictionary_kind":4,"allocation_domain":"55555555555555555555555555555555","ordinal":"7"},
        "route":{"class":"private","cidr":"10.2.0.0/16"},"enabled":true,
        "expected_generation":null,"expected_config_revision":"2","expected_policy_revision":"3"
    })
}
#[test]
fn command_identity_matches_independent_bytes_and_binds_semantic_inputs() {
    let dataset: DatasetId = "11111111111111111111111111111111".parse().unwrap();
    let actor: PrincipalId = "22222222222222222222222222222222".parse().unwrap();
    let key: CommandKey = "33333333333333333333333333333333".parse().unwrap();
    let base = input();
    let command: EnrollmentCommand = serde_json::from_value(base.clone()).unwrap();
    let digest = command.digest(dataset, actor, key).unwrap();
    assert_eq!(
        digest.to_string(),
        "ca6fb3f165d2b94638c6a50ae5223bf1cad0299b0304049b16ad76b84bdead32"
    );
    for (pointer, value) in [
        ("/endpoint_id", json!("66666666666666666666666666666666")),
        (
            "/visibility/allocation_domain",
            json!("66666666666666666666666666666666"),
        ),
        ("/visibility/ordinal", json!("8")),
        ("/route/cidr", json!("10.3.0.0/16")),
        ("/expected_generation", json!("1")),
        ("/expected_config_revision", json!("4")),
        ("/expected_policy_revision", json!("4")),
    ] {
        let mut changed_value = base.clone();
        *changed_value.pointer_mut(pointer).unwrap() = value;
        let changed: EnrollmentCommand = serde_json::from_value(changed_value).unwrap();
        assert_ne!(
            changed.digest(dataset, actor, key).unwrap(),
            digest,
            "{pointer}"
        );
    }
    let other_actor: PrincipalId = "77777777777777777777777777777777".parse().unwrap();
    let other_key: CommandKey = "77777777777777777777777777777777".parse().unwrap();
    assert_ne!(command.digest(dataset, other_actor, key).unwrap(), digest);
    assert_ne!(command.digest(dataset, actor, other_key).unwrap(), digest);
    let mut enabled = command.clone();
    enabled.expected_generation = Some("1".to_owned().try_into().unwrap());
    let enabled_digest = enabled.digest(dataset, actor, key).unwrap();
    enabled.enabled = false;
    assert_ne!(enabled.digest(dataset, actor, key).unwrap(), enabled_digest);
    let mut ipv6 = input();
    ipv6["route"]["cidr"] = json!("FD12:0000:0000:0000:0000:0000:0000:0000/16");
    let first: EnrollmentCommand = serde_json::from_value(ipv6.clone()).unwrap();
    ipv6["route"]["cidr"] = json!("fd12::/16");
    let second: EnrollmentCommand = serde_json::from_value(ipv6).unwrap();
    assert_eq!(
        first.digest(dataset, actor, key).unwrap(),
        second.digest(dataset, actor, key).unwrap()
    );
}
#[test]
fn unknown_fields_absent_cas_wrong_dataset_and_nonvisibility_scope_refuse() {
    let dataset: DatasetId = "11111111111111111111111111111111".parse().unwrap();
    let mut value = input();
    value.as_object_mut().unwrap().remove("expected_generation");
    assert!(serde_json::from_value::<EnrollmentCommand>(value).is_err());
    let mut value = input();
    value["extra"] = json!(true);
    assert!(serde_json::from_value::<EnrollmentCommand>(value).is_err());
    for (pointer, replacement) in [
        (
            "/visibility/dataset_id",
            json!("66666666666666666666666666666666"),
        ),
        ("/visibility/dictionary_kind", json!(1)),
        ("/enabled", json!(false)),
    ] {
        let mut value = input();
        *value.pointer_mut(pointer).unwrap() = replacement;
        let command: EnrollmentCommand = serde_json::from_value(value).unwrap();
        assert!(command.validate(dataset).is_err());
    }
}
