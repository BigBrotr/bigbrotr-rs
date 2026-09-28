use serde_json::Value;
use std::{fs, process::Command};

const ROOT: &str = include_str!("../../../config/recipes/serve-existing.toml");

fn show(rules: &str) -> std::process::Output {
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("instance.toml");
    fs::write(&file, format!("{ROOT}\n[network.private]\nrules={rules}\n")).unwrap();
    Command::new(env!("CARGO_BIN_EXE_bigbrotr"))
        .args(["config", "show", "--file"])
        .arg(file)
        .env_clear()
        .output()
        .unwrap()
}

fn accepted(rules: &str) -> Value {
    let output = show(rules);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stderr.is_empty());
    serde_json::from_slice(&output.stdout).unwrap()
}

#[test]
fn private_ceiling_is_explicit_redacted_and_outside_stored_policy() {
    let empty = accepted("[]");
    let explicit = accepted(
        "[{endpoint='ws://127.0.0.1:7447/PRIVATE_CANARY?key=CANARY',cidr='127.0.0.1/32',port=7447}]",
    );
    assert_eq!(empty["initial_policy"], explicit["initial_policy"]);
    assert_ne!(empty["document_sha256"], explicit["document_sha256"]);
    let setting = &explicit["settings"]["network.private.rules"];
    assert_eq!(setting["value"], "[redacted]");
    assert_eq!(setting["deployment_locked"], true);
    assert_eq!(setting["reload"], "restart");
    assert_eq!(setting["applicability"], "staged-unselected");
    assert_eq!(explicit["authority"]["activation"], "not-performed");
    assert!(!explicit.to_string().contains("CANARY"));
}

#[test]
fn rule_shape_and_semantics_are_checked_even_without_acquisition() {
    for rules in [
        "{}",
        "[1]",
        "[{}]",
        "[{hostname='relay.example',cidr='127.0.0.1/32'}]",
        "[{hostname='relay.example',endpoint='wss://relay.example/',cidr='127.0.0.1/32',port=443}]",
        "[{hostname='relay.example',cidr='127.0.0.1/32',port=443,secret='PRIVATE_CANARY'}]",
        "[{hostname='*.example',cidr='127.0.0.1/32',port=443}]",
        "[{hostname='RELAY.example',cidr='127.0.0.1/32',port=443}]",
        "[{hostname='hidden.onion',cidr='127.0.0.1/32',port=443}]",
        "[{hostname='127.0.0.1',cidr='127.0.0.1/32',port=443}]",
        "[{hostname='relay.example',cidr='127.0.0.1/24',port=443}]",
        "[{hostname='relay.example',cidr='::ffff:127.0.0.1/128',port=443}]",
        "[{hostname='relay.example',cidr='127.0.0.1/32',port=0}]",
        "[{hostname='relay.example',cidr='127.0.0.1/32',port=65536}]",
        "[{hostname='relay.example',cidr='127.0.0.1/32',port='443'}]",
        "[{endpoint='ws://127.0.0.1/',cidr='127.0.0.1/32',port=443}]",
        "[{endpoint='https://relay.example/PRIVATE_CANARY',cidr='127.0.0.1/32',port=443}]",
        "[{endpoint='wss://user:PRIVATE_CANARY@relay.example/',cidr='127.0.0.1/32',port=443}]",
    ] {
        let output = show(rules);
        assert_eq!(output.status.code(), Some(3), "{rules}");
        assert!(output.stdout.is_empty());
        assert!(!String::from_utf8_lossy(&output.stderr).contains("CANARY"));
    }
    accepted(
        "[{hostname='relay.example',cidr='10.0.0.0/8',port=443},{endpoint='ws://[::1]:7447/',cidr='::1/128',port=7447}]",
    );
}

#[test]
fn private_rule_count_is_bounded_without_silent_truncation() {
    let rule = "{hostname='relay.example',cidr='10.0.0.0/8',port=443}";
    // Duplicate rules do not enlarge authority, but still consume the bound.
    accepted(&format!("[{}]", vec![rule; 256].join(",")));
    assert_eq!(
        show(&format!("[{}]", vec![rule; 257].join(",")))
            .status
            .code(),
        Some(3)
    );
}
