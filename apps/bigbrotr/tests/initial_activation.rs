//! Process boundary controls; database transitions are exercised on native PG.
use std::{fs, process::Command};

#[test]
fn activation_requires_the_protected_control_dependency() {
    let root = tempfile::tempdir().unwrap();
    let file = root.path().join("instance.toml");
    fs::write(
        &file,
        include_str!("../../../config/recipes/archive-only.toml").replace(
            "file:/run/secrets/bigbrotr-control",
            "file:/absent/ACTIVATION_SECRET_CANARY",
        ),
    )
    .unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_bigbrotr"))
        .args(["config", "activate-initial", "--file"])
        .arg(&file)
        .args([
            "--request-id",
            "abababababababababababababababab",
            "--expected-revision",
            "1",
        ])
        .env_clear()
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(3));
    assert!(output.stdout.is_empty());
    let error = String::from_utf8(output.stderr).unwrap();
    assert!(error.contains("protected PostgreSQL password"), "{error}");
    assert!(!error.contains("CANARY"));
}

#[test]
fn activation_rejects_noncanonical_request_fields_before_io() {
    for (request, revision) in [
        ("00000000000000000000000000000000", "1"),
        ("ABABABABABABABABABABABABABABABAB", "1"),
        ("abababababababababababababababab", "01"),
        ("abababababababababababababababab", "0"),
        ("abababababababababababababababab", "18446744073709551616"),
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_bigbrotr"))
            .args([
                "config",
                "activate-initial",
                "--file",
                "/absent/CONFIG_CANARY",
                "--request-id",
                request,
                "--expected-revision",
                revision,
            ])
            .env_clear()
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(3));
        assert!(output.stdout.is_empty());
        assert!(!String::from_utf8_lossy(&output.stderr).contains("CANARY"));
    }
}
