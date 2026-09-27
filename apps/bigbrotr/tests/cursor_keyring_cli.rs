//! Actual process boundary: explicit preparation, offline roots and retry.
use serde_json::Value;
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::Path,
    process::{Command, Output},
};

fn run(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_bigbrotr"))
        .args(args)
        .env_clear()
        .output()
        .unwrap()
}
fn observe(output: Output) -> Value {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stderr.is_empty());
    serde_json::from_slice(&output.stdout).unwrap()
}
fn action(operation: &str, config: &Path) -> Output {
    run(&[
        "access",
        "cursor-keyring",
        operation,
        "--file",
        config.to_str().unwrap(),
    ])
}
fn config(root: &Path) -> (std::path::PathBuf, String) {
    let output = run(&["config", "example", "--recipe", "archive-only"]);
    assert!(output.status.success());
    let original = String::from_utf8(output.stdout).unwrap();
    let file = root.join("instance.toml");
    fs::write(&file, &original).unwrap();
    (file, original)
}

#[test]
fn configured_reference_is_offline_redacted_and_excluded_from_accepted_policy() {
    let root = tempfile::tempdir().unwrap();
    let (file, original) = config(root.path());
    let baseline = observe(run(&["config", "show", "--file", file.to_str().unwrap()]));
    let missing = root
        .path()
        .canonicalize()
        .unwrap()
        .join("not-created/keyring");
    fs::write(
        &file,
        format!(
            "{original}\n[read.cursor]\nkeyring_ref='file:{}'\n",
            missing.display()
        ),
    )
    .unwrap();
    let staged = observe(run(&["config", "show", "--file", file.to_str().unwrap()]));
    assert_eq!(staged["initial_policy"], baseline["initial_policy"]);
    assert_eq!(
        staged["settings"]["read.cursor.keyring_ref"]["value"],
        "[redacted]"
    );
    assert_eq!(
        staged["settings"]["read.cursor.keyring_ref"]["deployment_locked"],
        true
    );
    assert!(!missing.parent().unwrap().exists());
    let inspect = action("inspect", &file);
    assert_eq!(inspect.status.code(), Some(4));
    assert!(inspect.stdout.is_empty());
    assert!(!String::from_utf8_lossy(&inspect.stderr).contains(missing.to_str().unwrap()));
    assert!(!missing.parent().unwrap().exists());
}

#[test]
fn prepare_and_inspect_survive_process_exit_without_contacting_control() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let (file, original) = config(&root);
    let absent = action("prepare", &file);
    assert_eq!(absent.status.code(), Some(3));
    assert!(absent.stdout.is_empty());
    let keys = root.join("keys");
    fs::create_dir(&keys).unwrap();
    fs::set_permissions(&keys, fs::Permissions::from_mode(0o700)).unwrap();
    fs::write(
        &file,
        format!(
            "{original}\n[read.cursor]\nkeyring_ref='file:{}/keyring'\n",
            keys.display()
        ),
    )
    .unwrap();
    assert_eq!(action("inspect", &file).status.code(), Some(4));
    assert_eq!(fs::read_dir(&keys).unwrap().count(), 0);
    let first = observe(action("prepare", &file));
    assert_eq!(first["format"], "bigbrotr-cursor-keyring-observation/1");
    assert_eq!(first["state"], "prepared-local");
    assert_eq!(first["control_registration"], "not-checked");
    assert_eq!(first["cursor_authority"], "not-granted");
    assert_eq!(first["dataset_id"], "22222222222222222222222222222222");
    assert_eq!(first["key_ids"].as_array().unwrap().len(), 1);
    assert_eq!(first.as_object().unwrap().len(), 8);
    let original = fs::read(keys.join("keyring")).unwrap();
    let mut inspected = first.clone();
    inspected["state"] = "inspected-local".into();
    assert_eq!(observe(action("inspect", &file)), inspected);
    assert_eq!(observe(action("prepare", &file)), first);
    assert!(fs::read(keys.join("keyring")).unwrap() == original);
    assert_eq!(fs::read_dir(&keys).unwrap().count(), 1);
}

#[test]
fn register_requires_existing_complete_keys_before_any_database_connection() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let (file, original) = config(&root);
    let password = root.join("pg-password");
    fs::write(&password, b"public test password").unwrap();
    fs::set_permissions(&password, fs::Permissions::from_mode(0o600)).unwrap();
    let keys = root.join("keys");
    fs::create_dir(&keys).unwrap();
    fs::set_permissions(&keys, fs::Permissions::from_mode(0o700)).unwrap();
    fs::write(
        &file,
        format!(
            "{}\n[read.cursor]\nkeyring_ref='file:{}/keyring'\n",
            original.replace(
                "file:/run/secrets/bigbrotr-control",
                &format!("file:{}", password.display())
            ),
            keys.display()
        ),
    )
    .unwrap();
    let missing = action("register", &file);
    assert_eq!(
        missing.status.code(),
        Some(4),
        "{}",
        String::from_utf8_lossy(&missing.stderr)
    );
    assert!(String::from_utf8_lossy(&missing.stderr).contains("NotInitialized"));
    assert!(missing.stdout.is_empty());
    assert_eq!(fs::read_dir(&keys).unwrap().count(), 0);
    fs::write(keys.join("unresolved.partial"), b"public retained sentinel").unwrap();
    let partial = action("register", &file);
    assert_eq!(partial.status.code(), Some(4));
    assert!(String::from_utf8_lossy(&partial.stderr).contains("Incomplete"));
    assert!(partial.stdout.is_empty());
    assert!(!keys.join("keyring").exists());
    assert_eq!(
        fs::read(keys.join("unresolved.partial")).unwrap(),
        b"public retained sentinel"
    );
}

#[test]
fn prepare_key_is_an_explicit_offline_idempotent_addition_not_initialization() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let (file, original) = config(&root);
    let directory = root.join("keys");
    fs::create_dir(&directory).unwrap();
    fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();
    fs::write(
        &file,
        format!(
            "{original}\n[read.cursor]\nkeyring_ref='file:{}/keyring'\n",
            directory.display()
        ),
    )
    .unwrap();
    let add = |id| {
        run(&[
            "access",
            "cursor-keyring",
            "prepare-key",
            "--file",
            file.to_str().unwrap(),
            "--key-id",
            id,
        ])
    };
    let key_id = "55555555555555555555555555555555";
    assert_eq!(add(key_id).status.code(), Some(4));
    assert!(!directory.join("keyring").exists());
    let first = observe(action("prepare", &file));
    let extended = observe(add(key_id));
    assert_eq!(extended["format"], "bigbrotr-cursor-keyring-observation/1");
    assert_eq!(extended["state"], "prepared-local");
    assert_eq!(extended["cursor_authority"], "not-granted");
    assert_eq!(extended["control_registration"], "not-checked");
    assert_eq!(extended["keyring_id"], first["keyring_id"]);
    assert_eq!(extended["key_ids"][0], first["key_ids"][0]);
    assert_eq!(extended["key_ids"][1], key_id);
    assert_eq!(extended.as_object().unwrap().len(), 8);
    let bytes = fs::read(directory.join("keyring")).unwrap();
    assert_eq!(observe(add(key_id)), extended);
    for invalid in [
        "",
        "0",
        "00000000000000000000000000000000",
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        "gggggggggggggggggggggggggggggggg",
    ] {
        assert_eq!(add(invalid).status.code(), Some(2));
    }
    assert_eq!(fs::read(directory.join("keyring")).unwrap(), bytes);
}
