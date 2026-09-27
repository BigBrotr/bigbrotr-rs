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

#[test]
fn configured_cursor_keys_are_loaded_read_only_before_control_credentials() {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    let file = root.join("root.toml");
    let keys = root.join("keys");
    fs::create_dir(&keys).unwrap();
    fs::set_permissions(&keys, fs::Permissions::from_mode(0o700)).unwrap();
    let mut config: toml::Table = include_str!("../../../config/recipes/archive-only.toml")
        .parse()
        .unwrap();
    config["topology"]["local_roles"] = toml::Value::Array(vec!["control".into()]);
    config["postgres"]["control"]["password_ref"] =
        format!("file:{}/absent-password", root.display()).into();
    let original = toml::to_string(&config).unwrap();
    let selected = format!(
        "{original}\n[read.cursor]\nkeyring_ref='file:{}/ring'\n",
        keys.display()
    );
    fs::write(&file, &selected).unwrap();
    let invoke = |args: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_bigbrotr"))
            .env_clear()
            .args(args)
            .arg("--file")
            .arg(&file)
            .output()
            .unwrap()
    };
    let missing = invoke(&["serve"]);
    assert_eq!(missing.status.code(), Some(3));
    assert!(missing.stdout.is_empty());
    let error = String::from_utf8_lossy(&missing.stderr);
    assert!(
        error.contains("cursor keyring refused: NotInitialized"),
        "{error}"
    );
    assert!(!error.contains(root.to_str().unwrap()));
    assert_eq!(fs::read_dir(&keys).unwrap().count(), 0);
    let prepared = invoke(&["access", "cursor-keyring", "prepare"]);
    assert!(
        prepared.status.success(),
        "{}",
        String::from_utf8_lossy(&prepared.stderr)
    );
    let bytes = fs::read(keys.join("ring")).unwrap();
    let inode = fs::metadata(keys.join("ring")).unwrap().ino();
    fs::write(keys.join("retained.partial"), b"independent sentinel").unwrap();
    // A valid final snapshot beside staging is loadable without a re-prepare.
    let loaded = invoke(&["serve"]);
    assert_eq!(loaded.status.code(), Some(3));
    assert!(String::from_utf8_lossy(&loaded.stderr).contains("protected PostgreSQL password"));
    assert_eq!(fs::read(keys.join("ring")).unwrap(), bytes);
    assert_eq!(fs::metadata(keys.join("ring")).unwrap().ino(), inode);
    assert_eq!(
        fs::read(keys.join("retained.partial")).unwrap(),
        b"independent sentinel"
    );
    assert_eq!(fs::read_dir(&keys).unwrap().count(), 2);
    // Selecting no consumer still reaches the pre-existing credential boundary.
    fs::write(&file, original).unwrap();
    let unselected = invoke(&["serve"]);
    assert_eq!(unselected.status.code(), Some(3));
    assert!(String::from_utf8_lossy(&unselected.stderr).contains("protected PostgreSQL password"));
}
