//! Early control-schema entrypoint controls. Real PG transactions run separately.
use std::{
    fs,
    path::Path,
    process::{Command, Output},
};

fn run(root: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_bigbrotr"))
        .args(args)
        .current_dir(root)
        .env_clear()
        .output()
        .unwrap()
}

#[test]
fn migration_help_is_explicit_and_accepts_no_plaintext_password_option() {
    let root = tempfile::tempdir().unwrap();
    let help = run(root.path(), &["migrate", "--help"]);
    assert!(
        help.status.success(),
        "{}",
        String::from_utf8_lossy(&help.stderr)
    );
    let text = String::from_utf8(help.stdout).unwrap();
    for action in ["inspect", "plan", "apply"] {
        assert!(text.contains(action));
    }
    let invalid = run(root.path(), &["migrate", "apply", "--password", "CANARY"]);
    assert_eq!(invalid.status.code(), Some(2));
    assert!(!String::from_utf8_lossy(&invalid.stderr).contains("CANARY"));
    assert_eq!(fs::read_dir(root.path()).unwrap().count(), 0);
}

#[test]
fn migration_requires_an_explicit_valid_configuration_before_any_credential_load() {
    let root = tempfile::tempdir().unwrap();
    for action in ["inspect", "plan", "apply"] {
        let result = run(
            root.path(),
            &[
                "migrate",
                action,
                "--file",
                "missing.toml",
                "--user",
                "migrator",
                "--password-ref",
                "file:/missing/CANARY",
            ],
        );
        assert_eq!(result.status.code(), Some(1));
        assert!(!String::from_utf8_lossy(&result.stderr).contains("CANARY"));
        assert!(result.stdout.is_empty());
    }
    assert_eq!(fs::read_dir(root.path()).unwrap().count(), 0);
}

#[test]
fn unknown_target_and_application_role_fail_before_loading_credentials() {
    let root = tempfile::tempdir().unwrap();
    assert!(
        run(
            root.path(),
            &[
                "config",
                "init",
                "--recipe",
                "archive-only",
                "--output",
                "config.toml"
            ]
        )
        .status
        .success()
    );
    for (user, target, expected) in [
        (
            "migration_owner",
            "2",
            "unsupported control migration target 2",
        ),
        ("bigbrotr_control", "1", "dedicated migration role"),
    ] {
        let result = run(
            root.path(),
            &[
                "migrate",
                "apply",
                "--file",
                "config.toml",
                "--user",
                user,
                "--password-ref",
                "file:/missing/CANARY",
                "--target",
                target,
            ],
        );
        assert_eq!(result.status.code(), Some(3));
        let error = String::from_utf8(result.stderr).unwrap();
        assert!(error.contains(expected), "{error}");
        assert!(!error.contains("CANARY"));
        assert!(result.stdout.is_empty());
    }
    assert_eq!(fs::read_dir(root.path()).unwrap().count(), 1);
}
