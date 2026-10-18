//! Actual executable refusals before opening protected credentials or engines.
use std::{fs, process::Command};

#[test]
fn ingest_lifetime_flags_conflict_before_configuration_io() {
    for (selected, other) in [
        ("--initialize-ingest", "--activate-initial-successor"),
        ("--initialize-ingest", "--activate-successor"),
        ("--activate-initial-successor", "--activate-successor"),
    ] {
        let result = Command::new(env!("CARGO_BIN_EXE_bigbrotr"))
            .env_clear()
            .args([
                "serve",
                "--file",
                "/nonexistent/successor-root.toml",
                selected,
                other,
            ])
            .output()
            .unwrap();
        assert_eq!(result.status.code(), Some(2));
        assert!(result.stdout.is_empty());
        let error = String::from_utf8_lossy(&result.stderr);
        assert!(error.contains("cannot be used with"), "{error}");
        assert!(error.contains(selected) && error.contains(other));
    }
}

#[test]
fn successor_checks_selected_roles_and_native_state_before_credentials() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    let file = root.join("root.toml");
    let store = root.join("absent-store");
    let mut config: toml::Table = include_str!("../../../config/recipes/archive-only.toml")
        .parse()
        .unwrap();
    config["storage"]["ingest_root"] = store.to_str().unwrap().into();
    config["postgres"]["control"]["password_ref"] = "file:/nonexistent/successor-password".into();
    let original = toml::to_string(&config).unwrap();
    fs::write(&file, &original).unwrap();
    for (flag, required_roles) in [
        (
            "--activate-initial-successor",
            "initial successor requires control,ingest",
        ),
        ("--activate-successor", "successor requires control,ingest"),
    ] {
        let invoke = |roles: Option<&str>| {
            let mut command = Command::new(env!("CARGO_BIN_EXE_bigbrotr"));
            command
                .env_clear()
                .args(["serve", flag, "--file"])
                .arg(&file);
            if let Some(roles) = roles {
                command.args(["--roles", roles]);
            }
            command.output().unwrap()
        };
        // The deployment may assign Archive; this lifetime cannot select it.
        for roles in [
            None,
            Some("control"),
            Some("ingest"),
            Some("control,ingest,archive"),
        ] {
            let result = invoke(roles);
            assert_eq!(result.status.code(), Some(3));
            assert!(result.stdout.is_empty());
            let error = String::from_utf8_lossy(&result.stderr);
            assert!(error.contains(required_roles), "{error}");
            assert!(!error.contains("protected PostgreSQL password"));
            assert!(!store.exists());
        }
        for roles in ["control,control", "control,ingest,read"] {
            let result = invoke(Some(roles));
            assert_eq!(result.status.code(), Some(3));
            assert!(result.stdout.is_empty());
            assert!(String::from_utf8_lossy(&result.stderr).contains("invalid role selection"));
            assert!(!store.exists());
        }
        let result = invoke(Some("control,ingest"));
        assert_eq!(result.status.code(), Some(4));
        assert!(result.stdout.is_empty());
        let error = String::from_utf8_lossy(&result.stderr);
        assert!(
            error.contains("initialized store or CURRENT is absent"),
            "{error}"
        );
        assert!(!error.contains("protected PostgreSQL password"));
        assert!(!store.exists());
        assert_eq!(fs::read_to_string(&file).unwrap(), original);
        assert_eq!(fs::read_dir(&root).unwrap().count(), 1);
    }
}

#[test]
fn explicit_initial_ingest_refuses_omitted_roles_and_unsupported_consumers_before_io() {
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("root.toml");
    let mut config: toml::Table = include_str!("../../../config/recipes/archive-only.toml")
        .parse()
        .unwrap();
    // Static composition still requires Archive; this explicit process selection
    // starts only the first Control/Ingest lifetime.
    config["topology"]["local_roles"] =
        toml::Value::Array(vec!["control".into(), "ingest".into(), "archive".into()]);
    config["postgres"]["control"]["password_ref"] =
        "file:/nonexistent/initial-ingest-password".into();
    let invoke = |roles: &str| {
        Command::new(env!("CARGO_BIN_EXE_bigbrotr"))
            .env_clear()
            .args(["serve", "--file"])
            .arg(&file)
            .args(["--initialize-ingest", "--roles", roles])
            .output()
            .unwrap()
    };
    fs::write(&file, toml::to_string(&config).unwrap()).unwrap();
    for roles in [
        "control",
        "ingest",
        "archive",
        "control,archive",
        "ingest,archive",
    ] {
        let wrong = invoke(roles);
        assert_eq!(wrong.status.code(), Some(3));
        assert!(wrong.stdout.is_empty());
        assert!(
            String::from_utf8_lossy(&wrong.stderr)
                .contains("requires control,ingest with optional sync and archive roles")
        );
    }
    let administration = invoke("control,ingest");
    assert_eq!(administration.status.code(), Some(3));
    assert!(
        String::from_utf8_lossy(&administration.stderr).contains("protected PostgreSQL password")
    );
    assert!(administration.stdout.is_empty());
    config["administration"]["enabled"] = false.into();
    let minimal = toml::to_string(&config).unwrap();
    for selected in [
        "\n[read.cursor]\nkeyring_ref='file:/nonexistent/ring'\n",
        "\n[administration.assets]\ndirectory='/nonexistent/assets'\n",
    ] {
        fs::write(&file, format!("{minimal}{selected}")).unwrap();
        let refused = invoke("control,ingest");
        assert_eq!(refused.status.code(), Some(3));
        assert!(
            String::from_utf8_lossy(&refused.stderr).contains("cursor keyring or private assets")
        );
        assert!(refused.stdout.is_empty());
    }
    fs::write(&file, &minimal).unwrap();
    let accepted = invoke("control,ingest");
    assert_eq!(accepted.status.code(), Some(3));
    assert!(String::from_utf8_lossy(&accepted.stderr).contains("protected PostgreSQL password"));
    assert!(accepted.stdout.is_empty());
    assert_eq!(fs::read_to_string(&file).unwrap(), minimal);
    assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 1);
}

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

#[test]
fn explicit_ingest_recovery_preserves_missing_store_before_credentials() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    let file = root.join("root.toml");
    let store = root.join("absent-store");
    let mut config: toml::Table = include_str!("../../../config/recipes/archive-only.toml")
        .parse()
        .unwrap();
    config["storage"]["ingest_root"] = store.to_str().unwrap().into();
    config["postgres"]["control"]["password_ref"] = "file:/nonexistent/recovery-password".into();
    fs::write(&file, toml::to_string(&config).unwrap()).unwrap();
    for flag in [
        None,
        Some("--close-source"),
        Some("--abandon-acquisition"),
        Some("--retire-initial"),
        Some("--retire-empty-attempt"),
        Some("--activated-successor"),
        Some("--close-activated-source"),
        Some("--abandon-activated-acquisition"),
        Some("--retire-activated-initial"),
        Some("--successor"),
        Some("--close-successor-source"),
        Some("--abandon-successor-acquisition"),
        Some("--retire-successor"),
        Some("--retire-empty-successor"),
    ] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_bigbrotr"));
        command
            .env_clear()
            .args(["ingest", "recover", "--file"])
            .arg(&file);
        if let Some(flag) = flag {
            command.arg(flag);
        }
        let result = command.output().unwrap();
        assert_eq!(result.status.code(), Some(4));
        assert!(result.stdout.is_empty());
        let error = String::from_utf8_lossy(&result.stderr);
        assert!(
            error.contains("initialized store or CURRENT is absent"),
            "{error}"
        );
        assert!(!error.contains("protected PostgreSQL password"));
        assert!(!store.exists());
    }
    assert_eq!(fs::read_dir(&root).unwrap().count(), 1);
}

#[test]
fn explicit_recovery_modes_conflict_before_configuration_io() {
    let modes = [
        "--close-source",
        "--abandon-acquisition",
        "--retire-initial",
        "--retire-empty-attempt",
        "--activated-successor",
        "--close-activated-source",
        "--abandon-activated-acquisition",
        "--retire-activated-initial",
        "--successor",
        "--close-successor-source",
        "--abandon-successor-acquisition",
        "--retire-successor",
        "--retire-empty-successor",
    ];
    for (index, &selected) in modes.iter().enumerate() {
        for &other in &modes[index + 1..] {
            let result = Command::new(env!("CARGO_BIN_EXE_bigbrotr"))
                .env_clear()
                .args([
                    "ingest",
                    "recover",
                    "--file",
                    "/nonexistent/recovery-root.toml",
                    selected,
                    other,
                ])
                .output()
                .unwrap();
            assert_eq!(result.status.code(), Some(2));
            assert!(result.stdout.is_empty());
            let error = String::from_utf8_lossy(&result.stderr);
            assert!(error.contains("cannot be used with"), "{error}");
            assert!(error.contains(selected) && error.contains(other));
        }
    }
}
