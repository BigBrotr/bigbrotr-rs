//! Offline candidate semantics; these tests do not grant acquisition authority.
use serde_json::Value;
use std::{fs, process::Command};

const ROOT: &str = include_str!("../../../config/recipes/serve-existing.toml");

fn show(rules: &str) -> std::process::Output {
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("instance.toml");
    assert_eq!(ROOT.matches("[relay_policy]\n").count(), 1);
    fs::write(
        &file,
        ROOT.replace(
            "[relay_policy]\n",
            &format!("[relay_policy]\nbanned={rules}\n"),
        ),
    )
    .unwrap();
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
fn bans_are_versioned_redacted_ordinary_policy_even_when_workers_are_unselected() {
    let empty = accepted("[]");
    let denied = accepted("[{endpoint='wss://relay.example/PRIVATE_CANARY?token=CANARY'}]");
    assert_eq!(
        denied["initial_policy"]["format"],
        "bigbrotr-product-policy/2"
    );
    assert_ne!(
        empty["initial_policy"]["sha256"],
        denied["initial_policy"]["sha256"]
    );
    let setting = &denied["settings"]["relay_policy.banned"];
    assert_eq!(setting["value"], "[redacted]");
    assert_eq!(setting["deployment_locked"], false);
    assert_eq!(setting["reload"], "live-revocation-barrier");
    assert_eq!(setting["applicability"], "staged-unselected");
    assert_eq!(denied["authority"]["activation"], "not-performed");
    assert!(!denied.to_string().contains("CANARY"));
}

#[test]
fn ban_set_identity_normalizes_subjects_order_and_duplicates() {
    let normalized = accepted(
        "[{endpoint='wss://relay.example/a?x=1&x=2'},{host='xn--bcher-kva.example'},{host='127.0.0.1'}]",
    );
    let equivalent = accepted(
        "[{host='[::ffff:127.0.0.1]'},{host='BÜCHER.example.'},{endpoint='WSS://RELAY.example.:443/x/../a?x=1&x=2'},{host='127.0.0.1'}]",
    );
    assert_eq!(normalized["initial_policy"], equivalent["initial_policy"]);
    assert_ne!(normalized["document_sha256"], equivalent["document_sha256"]);
    for different in [
        "[{endpoint='ws://relay.example/a?x=1&x=2'},{host='xn--bcher-kva.example'},{host='127.0.0.1'}]",
        "[{endpoint='wss://relay.example/a/?x=1&x=2'},{host='xn--bcher-kva.example'},{host='127.0.0.1'}]",
        "[{endpoint='wss://relay.example/a?x=2&x=1'},{host='xn--bcher-kva.example'},{host='127.0.0.1'}]",
        "[{host='relay.example'},{host='xn--bcher-kva.example'},{host='127.0.0.1'}]",
    ] {
        assert_ne!(
            normalized["initial_policy"]["sha256"],
            accepted(different)["initial_policy"]["sha256"]
        );
    }
}

#[test]
fn closed_ban_rules_reject_ambiguous_or_broader_matching_without_echoing_values() {
    for rules in [
        "{}",
        "[1]",
        "[{}]",
        "[{host=1}]",
        "[{endpoint='wss://relay.example/',host='relay.example'}]",
        "[{host='relay.example',suffix=true}]",
        "[{host='*.example'}]",
        "[{host='relay.example:443'}]",
        "[{host='https://relay.example/'}]",
        "[{host='relay.example/path'}]",
        "[{host='127.1'}]",
        "[{host='0177.0.0.1'}]",
        "[{host='0x7f000001'}]",
        "[{host='::1'}]",
        "[{host='[fe80::1%25eth0]'}]",
        "[{host='a..example'}]",
        "[{host='-a.example'}]",
        "[{host='a_.example'}]",
        "[{host='a.example..'}]",
        "[{endpoint='https://relay.example/PRIVATE_CANARY'}]",
        "[{endpoint='wss://user:PRIVATE_CANARY@relay.example/'}]",
    ] {
        let output = show(rules);
        assert_eq!(output.status.code(), Some(3), "{rules}");
        assert!(output.stdout.is_empty());
        assert!(!String::from_utf8_lossy(&output.stderr).contains("CANARY"));
    }
    accepted("[{host='[::1]'},{host='relay.onion'},{host='relay.i2p'}]");
}
