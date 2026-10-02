//! Installed transport roots: offline configuration and actual filesystem/TLS.
use serde_json::Value;
use std::{fs, process::Command};

const ROOT: &str = include_str!("../../../config/recipes/serve-existing.toml");

fn show(extra: &str) -> std::process::Output {
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("instance.toml");
    fs::write(&file, format!("{ROOT}\n{extra}\n")).unwrap();
    Command::new(env!("CARGO_BIN_EXE_bigbrotr"))
        .args(["config", "show", "--file"])
        .arg(file)
        .env_clear()
        .output()
        .unwrap()
}

fn accepted(extra: &str) -> Value {
    let output = show(extra);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

#[test]
fn explicit_dns_root_is_offline_and_preserves_accepted_policy_identity() {
    let original = accepted("");
    assert!(original["settings"]["network.dns.resolver"]["value"].is_null());
    for address in ["127.0.0.1:5353", "[::1]:53", "8.8.8.8:53"] {
        let current = accepted(&format!("[network.dns]\nresolver='{address}'"));
        assert_eq!(original["initial_policy"], current["initial_policy"]);
        let setting = &current["settings"]["network.dns.resolver"];
        assert_eq!(setting["value"], address);
        assert_eq!(setting["deployment_locked"], true);
        assert_eq!(setting["reload"], "restart");
        assert_eq!(setting["applicability"], "staged-unselected");
    }
}

#[test]
fn invalid_numeric_dns_is_refused_even_without_selected_acquisition() {
    for address in [
        "resolver.example:53",
        "127.0.0.1",
        "127.0.0.1:0",
        "0.0.0.0:53",
        "224.0.0.1:53",
        "255.255.255.255:53",
        "[::]:53",
        "[ff02::1]:53",
        "[fe80::1%1]:53",
        "[::ffff:224.0.0.1]:53",
        "CANARY",
    ] {
        let output = show(&format!("[network.dns]\nresolver='{address}'"));
        assert_eq!(output.status.code(), Some(3), "{address}");
        assert!(output.stdout.is_empty());
        assert!(!String::from_utf8_lossy(&output.stderr).contains("CANARY"));
    }
}

#[test]
fn root_paths_are_bounded_without_reading_certificates_offline() {
    let paths = |count| {
        (0..count)
            .map(|n| format!("'/absent/acquisition-{n}.der'"))
            .collect::<Vec<_>>()
            .join(",")
    };
    let original = accepted("");
    let current = accepted(&format!("[network.tls]\nextra_roots=[{}]", paths(128)));
    assert_eq!(original["initial_policy"], current["initial_policy"]);
    assert_eq!(
        show(&format!("[network.tls]\nextra_roots=[{}]", paths(129)))
            .status
            .code(),
        Some(3)
    );
}
