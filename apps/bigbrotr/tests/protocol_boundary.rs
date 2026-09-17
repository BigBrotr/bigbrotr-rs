use serde_json::{Value, json};
use std::{
    io::Write,
    process::{Command, Output, Stdio},
};

fn run(input: &[u8]) -> Output {
    let directory = tempfile::tempdir().unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_bigbrotr"))
        .args(["event", "verify"])
        .env_clear()
        .current_dir(directory.path())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    // An over-limit reader can close early; its actual exit is the oracle.
    let _ = child.stdin.take().unwrap().write_all(input);
    let output = child.wait_with_output().unwrap();
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    output
}

fn examples() -> Value {
    serde_json::from_str(include_str!(
        "../../../experiments/identity-contract/signed-fixture.json"
    ))
    .unwrap()
}

fn check(input: &[u8], exit: i32, reason: &str) {
    let out = run(input);
    assert_eq!(
        out.status.code(),
        Some(exit),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(out.stdout.is_empty());
    assert!(
        String::from_utf8_lossy(&out.stderr).contains(reason),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn frozen_signed_history_and_alternate_signatures_survive_the_real_cli() {
    let corpus = examples();
    assert_eq!(corpus["events"].as_object().unwrap().len(), 15);
    assert_eq!(
        corpus["events"]["E_private"]["id"],
        corpus["events"]["E_public"]["id"]
    );
    assert_ne!(
        corpus["events"]["E_private"]["sig"],
        corpus["events"]["E_public"]["sig"]
    );
    for event in corpus["events"].as_object().unwrap().values() {
        let out = run(&serde_json::to_vec(event).unwrap());
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(out.stderr.is_empty());
        assert_eq!(
            serde_json::from_slice::<Value>(&out.stdout).unwrap(),
            *event
        );
    }
}

#[test]
fn signed_unsigned_extremes_empty_tags_and_unicode_round_trip_without_loss() {
    let corpus: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/protocol/signed-extremes.json"
    ))
    .unwrap();
    for row in corpus["events"].as_object().unwrap().values() {
        let out = run(&serde_json::to_vec(&row["event"]).unwrap());
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&out.stdout).unwrap(),
            row["event"]
        );
    }
    check(
        &serde_json::to_vec(&corpus["invalid_structure"]["empty-tag"]).unwrap(),
        3,
        "empty-tag",
    );
}

#[test]
fn known_ids_do_not_authorize_changed_payloads_or_signatures() {
    let original = examples()["events"]["E_public"].clone();
    let mut event = original.clone();
    event["content"] = json!("changed while retaining the claimed ID");
    check(&serde_json::to_vec(&event).unwrap(), 3, "event-id-mismatch");
    event = original.clone();
    event["sig"] = json!("00".repeat(64));
    check(&serde_json::to_vec(&event).unwrap(), 3, "invalid-signature");
    event = original.clone();
    event["tags"].as_array_mut().unwrap().reverse();
    check(&serde_json::to_vec(&event).unwrap(), 3, "event-id-mismatch");
}

#[test]
fn malformed_signed_fields_and_duplicate_keys_are_refused() {
    let event = examples()["events"]["E_public"].clone();
    let bytes = serde_json::to_string(&event).unwrap();
    let array = json!([
        event["id"],
        event["pubkey"],
        event["created_at"],
        event["kind"],
        event["tags"],
        event["content"],
        event["sig"]
    ]);
    check(&serde_json::to_vec(&array).unwrap(), 3, "invalid-json");
    for field in [
        "id",
        "pubkey",
        "sig",
        "created_at",
        "kind",
        "tags",
        "content",
    ] {
        let duplicate = format!("{{{field:?}:{},{}", event[field], &bytes[1..]);
        check(duplicate.as_bytes(), 3, "invalid-json");
        let mut missing = event.clone();
        missing.as_object_mut().unwrap().remove(field);
        check(&serde_json::to_vec(&missing).unwrap(), 3, "invalid-json");
    }
    for (field, values) in [
        (
            "created_at",
            vec!["-1", "1.0", "1e0", "18446744073709551616", "null", "\"1\""],
        ),
        ("kind", vec!["65536", "-1", "1.0"]),
        ("tags", vec!["[null]", "[[1]]", "null"]),
        ("content", vec!["{}", "null"]),
        ("sig", vec!["\"00\"", "null"]),
    ] {
        let mut fields = event.as_object().unwrap().clone();
        fields.remove(field);
        let rest = serde_json::to_string(&fields).unwrap();
        for value in values {
            check(
                format!("{{{field:?}:{value},{}", &rest[1..]).as_bytes(),
                3,
                "invalid-json",
            );
        }
    }
    let mut uppercase = event.clone();
    uppercase["id"] = json!(event["id"].as_str().unwrap().to_uppercase());
    check(&serde_json::to_vec(&uppercase).unwrap(), 3, "invalid-json");
    check(format!("{bytes} null").as_bytes(), 3, "invalid-json");
}

#[test]
fn unknown_wire_fields_are_bounded_and_do_not_become_signed_fields() {
    let event = examples()["events"]["E_public"].clone();
    let mut extended = event.clone();
    extended["unknown"] = json!({"array": [null, true, "value"]});
    let output = run(&serde_json::to_vec(&extended).unwrap());
    assert!(output.status.success());
    assert_eq!(
        serde_json::from_slice::<Value>(&output.stdout).unwrap(),
        event
    );
    let mut invalid_utf8 = b"{\"unknown\":\"\xff\",".to_vec();
    invalid_utf8.extend_from_slice(&serde_json::to_vec(&event).unwrap()[1..]);
    check(&invalid_utf8, 3, "invalid-json");
    let mut deep = String::from("{\"unknown\":");
    deep.push_str(&"[".repeat(33));
    deep.push('0');
    deep.push_str(&"]".repeat(33));
    deep.push('}');
    check(deep.as_bytes(), 4, "depth-limit");
    check(&vec![b' '; 2 * 1024 * 1024 + 1], 4, "wire-limit");
    let tiny = format!("{{\"unknown\":[{}null]}}", "null,".repeat(65_536));
    check(tiny.as_bytes(), 4, "node-limit");
    let mut oversized = event;
    oversized["content"] = json!("x".repeat(256 * 1024));
    check(
        &serde_json::to_vec(&oversized).unwrap(),
        4,
        "event-size-limit",
    );
}
