//! Installed catalog selection is explicit, deployment-locked and checked offline.
use serde_json::Value;
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
fn coordinator_root_is_separate_deployment_state_without_offline_io() {
    let dir = tempfile::tempdir().unwrap();
    let mut doc = example(dir.path(), "archive-only");
    let first = show(dir.path(), &doc);
    assert_eq!(
        first["settings"]["storage.archive_state_root"]["deployment_locked"],
        true
    );
    doc["storage"]["archive_state_root"] = "/unavailable/private-state".into();
    let changed = show(dir.path(), &doc);
    assert_eq!(first["initial_policy"], changed["initial_policy"]);
    for root in [
        "/var/lib/bigbrotr/archive/child",
        "/var/lib",
        "/var/lib/bigbrotr/ingest",
        "/var/lib/bigbrotr/scratch/child",
    ] {
        doc["storage"]["archive_state_root"] = root.into();
        write(dir.path(), &doc);
        let result = run(
            dir.path(),
            &["config", "validate", "--file", "instance.toml"],
        );
        assert_eq!(result.status.code(), Some(3), "{result:?}");
    }
    let doc = example(dir.path(), "serve-existing");
    write(dir.path(), &doc);
    let result = run(
        dir.path(),
        &["archive", "state-initialize", "--file", "instance.toml"],
    );
    assert_eq!(result.status.code(), Some(3), "{result:?}");
    assert!(String::from_utf8_lossy(&result.stderr).contains("local Archive role"));
    assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
}
fn example(root: &Path, recipe: &str) -> toml::Table {
    let output = run(root, &["config", "example", "--recipe", recipe]);
    assert!(output.status.success(), "{output:?}");
    String::from_utf8(output.stdout).unwrap().parse().unwrap()
}
fn write(root: &Path, document: &toml::Table) {
    fs::write(
        root.join("instance.toml"),
        toml::to_string(document).unwrap(),
    )
    .unwrap();
}
fn show(root: &Path, document: &toml::Table) -> Value {
    write(root, document);
    let result = run(root, &["config", "show", "--file", "instance.toml"]);
    assert!(result.status.success(), "{result:?}");
    serde_json::from_slice(&result.stdout).unwrap()
}

#[test]
fn selected_archive_requires_an_explicit_identity_without_connecting() {
    let dir = tempfile::tempdir().unwrap();
    let mut doc = example(dir.path(), "archive-only");
    doc.get_mut("archive")
        .unwrap()
        .as_table_mut()
        .unwrap()
        .remove("catalog_id");
    write(dir.path(), &doc);
    let result = run(
        dir.path(),
        &["config", "validate", "--file", "instance.toml"],
    );
    assert_eq!(result.status.code(), Some(3), "{result:?}");
    assert!(
        String::from_utf8(result.stderr)
            .unwrap()
            .contains("archive.catalog_id")
    );
    assert!(result.stdout.is_empty());
    assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
}

#[test]
fn catalog_identity_is_a_root_and_does_not_rewrite_the_policy_seed() {
    let dir = tempfile::tempdir().unwrap();
    let mut doc = example(dir.path(), "archive-only");
    let first = show(dir.path(), &doc);
    assert_eq!(
        first["settings"]["archive.catalog_id"]["deployment_locked"],
        true
    );
    assert_eq!(
        first["settings"]["archive.catalog_id"]["value"],
        "33333333333333333333333333333333"
    );
    doc["archive"].as_table_mut().unwrap().insert(
        "catalog_id".into(),
        "44444444444444444444444444444444".into(),
    );
    let second = show(dir.path(), &doc);
    assert_eq!(first["initial_policy"], second["initial_policy"]);
    assert_ne!(first["document_sha256"], second["document_sha256"]);
}

#[test]
fn unselected_catalog_can_be_absent_but_staged_identity_stays_typed() {
    let dir = tempfile::tempdir().unwrap();
    let mut doc = example(dir.path(), "serve-existing");
    doc["archive"].as_table_mut().unwrap().remove("catalog_id");
    show(dir.path(), &doc);
    doc["archive"].as_table_mut().unwrap().insert(
        "catalog_id".into(),
        "00000000000000000000000000000000".into(),
    );
    write(dir.path(), &doc);
    let result = run(
        dir.path(),
        &["config", "validate", "--file", "instance.toml"],
    );
    assert_eq!(result.status.code(), Some(3));
    assert!(
        String::from_utf8(result.stderr)
            .unwrap()
            .contains("archive.catalog_id")
    );
}

#[test]
fn catalog_inspect_refuses_unselected_role_before_opening_credentials() {
    let dir = tempfile::tempdir().unwrap();
    let doc = example(dir.path(), "serve-existing");
    write(dir.path(), &doc);
    let result = run(
        dir.path(),
        &["archive", "inspect", "--file", "instance.toml"],
    );
    assert_eq!(result.status.code(), Some(3), "{result:?}");
    assert!(
        String::from_utf8(result.stderr)
            .unwrap()
            .contains("local Archive role")
    );
    assert!(result.stdout.is_empty());
}

#[test]
fn selected_archive_uses_native_path_rules_without_inspecting_the_filesystem() {
    let dir = tempfile::tempdir().unwrap();
    let mut doc = example(dir.path(), "archive-only");
    for path in [
        "/",
        "/data//archive",
        "/data/archive*",
        "/data/arch?ve",
        "/data/[archive]",
    ] {
        doc["storage"]["archive_root"] = path.into();
        write(dir.path(), &doc);
        let result = run(
            dir.path(),
            &["config", "validate", "--file", "instance.toml"],
        );
        assert_eq!(result.status.code(), Some(3), "{path}: {result:?}");
        assert!(
            String::from_utf8(result.stderr)
                .unwrap()
                .contains("storage.archive_root")
        );
    }
    for path in ["/absent/archive", "/absent/archive/"] {
        doc["storage"]["archive_root"] = path.into();
        show(dir.path(), &doc);
    }
    assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
}

#[test]
fn insufficient_inspection_grant_refuses_before_reading_credentials() {
    let dir = tempfile::tempdir().unwrap();
    let mut doc = example(dir.path(), "archive-only");
    let runtime: toml::Table = "[processes.application]\npayload_bytes='64MiB'\n"
        .parse()
        .unwrap();
    doc.insert("runtime".into(), runtime.into());
    write(dir.path(), &doc);
    let result = run(
        dir.path(),
        &["archive", "inspect", "--file", "instance.toml"],
    );
    assert_eq!(result.status.code(), Some(3), "{result:?}");
    assert!(
        String::from_utf8(result.stderr)
            .unwrap()
            .contains("preparation does not fit")
    );
    assert!(result.stdout.is_empty());
}

#[test]
fn initialization_generates_distinct_explicit_catalog_candidates() {
    let dir = tempfile::tempdir().unwrap();
    let mut identities = std::collections::BTreeSet::new();
    for name in ["first.toml", "second.toml"] {
        let result = run(
            dir.path(),
            &[
                "config",
                "init",
                "--recipe",
                "archive-only",
                "--output",
                name,
            ],
        );
        assert!(result.status.success(), "{result:?}");
        let doc: toml::Table = fs::read_to_string(dir.path().join(name))
            .unwrap()
            .parse()
            .unwrap();
        let identity = doc["archive"]["catalog_id"].as_str().unwrap();
        assert_ne!(identity, "33333333333333333333333333333333");
        assert!(identities.insert(identity.to_owned()));
        assert_ne!(doc["archive"]["catalog_id"], doc["topology"]["dataset_id"]);
        assert_ne!(doc["archive"]["catalog_id"], doc["topology"]["instance_id"]);
        show(dir.path(), &doc);
    }
}

#[test]
fn explicit_initialization_and_recovery_refuse_unselected_archive() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), &example(dir.path(), "serve-existing"));
    for command in ["initialize", "recover"] {
        let result = run(
            dir.path(),
            &[
                "archive",
                command,
                "--file",
                "instance.toml",
                "--initializer-user",
                "installer",
                "--initializer-password-ref",
                "file:/absent/installer",
            ],
        );
        assert_eq!(result.status.code(), Some(3), "{result:?}");
        assert!(
            String::from_utf8(result.stderr)
                .unwrap()
                .contains("local Archive role")
        );
        assert!(result.stdout.is_empty());
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
    }
}

#[test]
fn initialize_refuses_manager_as_initializer_before_reading_credentials() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), &example(dir.path(), "archive-only"));
    let result = run(
        dir.path(),
        &[
            "archive",
            "initialize",
            "--file",
            "instance.toml",
            "--initializer-user",
            "bigbrotr_catalog",
            "--initializer-password-ref",
            "file:/absent/installer",
        ],
    );
    assert_eq!(result.status.code(), Some(3), "{result:?}");
    assert!(
        String::from_utf8(result.stderr)
            .unwrap()
            .contains("separate initializer")
    );
    assert!(result.stdout.is_empty());
    assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
}

#[test]
fn initialize_rejects_unusable_manager_and_native_client_identity_before_reservation() {
    let dir = tempfile::tempdir().unwrap();
    for expected in [
        "catalog manager login",
        "native catalog client identity refused",
    ] {
        let mut doc = example(dir.path(), "archive-only");
        if expected == "catalog manager login" {
            doc["postgres"]["catalog"]["user"] = "m".repeat(80).into();
        } else {
            let tls = doc["postgres"]["catalog"]["tls"].as_table_mut().unwrap();
            tls.remove("local_boundary");
            for (key, value) in [
                ("mode", "verify-full"),
                ("server_name", "catalog.example"),
                ("ca_file", "/absent/ca.pem"),
                ("client_cert_ref", "file:/absent/cert"),
                ("client_key_ref", "file:/absent/key"),
            ] {
                tls.insert(key.into(), value.into());
            }
        }
        write(dir.path(), &doc);
        let result = run(
            dir.path(),
            &[
                "archive",
                "initialize",
                "--file",
                "instance.toml",
                "--initializer-user",
                "installer",
                "--initializer-password-ref",
                "file:/absent/installer",
            ],
        );
        assert_eq!(result.status.code(), Some(3), "{result:?}");
        assert!(String::from_utf8(result.stderr).unwrap().contains(expected));
        assert!(result.stdout.is_empty());
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
    }
}
