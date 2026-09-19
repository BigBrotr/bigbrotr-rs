//! Actual executable refusals before opening protected credentials or engines.
use std::{fs, process::Command};

#[test]
fn explicit_control_role_reaches_credentials_but_never_activates_other_roles() {
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("root.toml");
    let recipe = include_str!("../../../config/recipes/archive-only.toml").replace(
        "file:/run/secrets/bigbrotr-control",
        "file:/nonexistent/bigbrotr-runtime-credential",
    );
    fs::write(&file, &recipe).unwrap();
    let invoke = |roles: Option<&str>| {
        let mut command = Command::new(env!("CARGO_BIN_EXE_bigbrotr"));
        command.env_clear().args(["serve", "--file"]).arg(&file);
        if let Some(roles) = roles {
            command.args(["--roles", roles]);
        }
        command.output().unwrap()
    };
    for roles in [None, Some("control,ingest")] {
        let result = invoke(roles);
        assert_eq!(result.status.code(), Some(3));
        assert!(result.stdout.is_empty());
        assert!(String::from_utf8_lossy(&result.stderr).contains("role is not implemented"));
    }
    for roles in ["read", "control,control"] {
        let result = invoke(Some(roles));
        assert_eq!(result.status.code(), Some(3));
        assert!(result.stdout.is_empty());
        assert!(String::from_utf8_lossy(&result.stderr).contains("role selection"));
    }
    let result = invoke(Some("control"));
    assert_eq!(result.status.code(), Some(4));
    assert!(result.stdout.is_empty());
    assert!(String::from_utf8_lossy(&result.stderr).contains("archive state root is unavailable"));
    assert!(!String::from_utf8_lossy(&result.stderr).contains("/nonexistent"));
    assert_eq!(fs::read_to_string(&file).unwrap(), recipe);
    assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 1);
    // A Control-only deployment has no local Archive custody prerequisite.
    let mut control: toml::Table = recipe.parse().unwrap();
    control["topology"]["local_roles"] = toml::Value::Array(vec!["control".into()]);
    fs::write(&file, toml::to_string(&control).unwrap()).unwrap();
    let result = invoke(Some("control"));
    assert_eq!(result.status.code(), Some(3));
    assert!(String::from_utf8_lossy(&result.stderr).contains("protected PostgreSQL password"));
}
