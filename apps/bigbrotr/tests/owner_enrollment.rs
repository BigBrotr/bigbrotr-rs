use std::{fs, process::Command};

#[test]
fn owner_token_requires_explicit_control_credentials_without_disclosing_them() {
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    fs::write(
        &config,
        include_str!("../../../config/recipes/archive-only.toml").replace(
            "file:/run/secrets/bigbrotr-control",
            "file:/absent/OWNER_SECRET_CANARY",
        ),
    )
    .unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_bigbrotr"))
        .args([
            "access",
            "owner-token",
            "issue",
            "--username",
            "alice",
            "--request-id",
            "31313131313131313131313131313131",
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
    assert!(!error.contains("OWNER_SECRET_CANARY"));
}
