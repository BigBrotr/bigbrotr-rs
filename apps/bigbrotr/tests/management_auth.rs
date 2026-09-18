use std::{fs, process::Command};

#[test]
fn local_self_inspection_requires_protected_control_credentials() {
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    fs::write(
        &config,
        include_str!("../../../config/recipes/archive-only.toml").replace(
            "file:/run/secrets/bigbrotr-control",
            "file:/absent/AUTH_DATABASE_CANARY",
        ),
    )
    .unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_bigbrotr"))
        .args([
            "access",
            "me",
            "--username",
            "alice",
            "--password-ref",
            "file:/absent/AUTH_PASSWORD_CANARY",
            "--file",
        ])
        .arg(config)
        .env_clear()
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(3));
    assert!(output.stdout.is_empty());
    let error = String::from_utf8(output.stderr).unwrap();
    assert!(error.contains("protected PostgreSQL password"), "{error}");
    assert!(!error.contains("CANARY"));
}
