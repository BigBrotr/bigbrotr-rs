use serde_json::Value;
use std::{fs, process::Command};

fn inspect(document: &str) -> Value {
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("instance.toml");
    fs::write(&file, document).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_bigbrotr"))
        .args(["config", "show", "--file"])
        .arg(file)
        .env_clear()
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stderr.is_empty());
    serde_json::from_slice(&output.stdout).unwrap()
}

const ROOT: &str = include_str!("../../../config/recipes/archive-only.toml");

#[test]
fn normalized_policy_seed_is_distinct_from_source_and_deployment_roots() {
    let original = inspect(ROOT);
    let seed = &original["initial_policy"];
    assert_eq!(seed["format"], "bigbrotr-product-policy/2");
    assert_eq!(seed["status"], "candidate-only");
    assert_eq!(seed["sha256"].as_str().unwrap().len(), 64);
    let changed_root = ROOT
        .replace(
            "22222222222222222222222222222222",
            "33333333333333333333333333333333",
        )
        .replace(
            "file:/run/secrets/bigbrotr-control",
            "file:/private/SECRET_SENTINEL",
        );
    let changed = inspect(&changed_root);
    assert_ne!(original["document_sha256"], changed["document_sha256"]);
    assert_eq!(seed, &changed["initial_policy"]);
    assert!(!changed.to_string().contains("SECRET_SENTINEL"));
    assert_eq!(changed["authority"]["desired_revision"], Value::Null);
    assert_eq!(changed["authority"]["activation"], "not-performed");
}

#[test]
fn policy_seed_freezes_normalized_defaults_and_changes_with_effective_policy() {
    let original = inspect(ROOT);
    let equivalent = inspect(&format!(
        "{ROOT}\n# Different source spelling\n[ingest]\nmax_event_bytes='262144B'\n"
    ));
    assert_ne!(original["document_sha256"], equivalent["document_sha256"]);
    assert_eq!(original["initial_policy"], equivalent["initial_policy"]);
    let changed = inspect(&format!("{ROOT}\n[ingest]\nmax_event_bytes='512KiB'\n"));
    assert_ne!(
        original["initial_policy"]["sha256"],
        changed["initial_policy"]["sha256"]
    );
}

#[test]
fn policy_seed_preserves_the_full_unsigned_domain() {
    let maximum =
        inspect(&ROOT.replace("[sync]\n", "[sync]\nhistory_since='18446744073709551615'\n"));
    let nearby =
        inspect(&ROOT.replace("[sync]\n", "[sync]\nhistory_since='18446744073709551614'\n"));
    assert_eq!(
        maximum["settings"]["sync.history_since"]["value"],
        "18446744073709551615"
    );
    assert_ne!(
        maximum["initial_policy"]["sha256"],
        nearby["initial_policy"]["sha256"]
    );
}

#[test]
fn policy_seed_normalizes_zero_fractions_and_retains_unselected_settings() {
    let zero = inspect(&ROOT.replace("[monitor]\n", "[monitor]\nschedule_jitter=0\n"));
    let negative_zero = inspect(&ROOT.replace("[monitor]\n", "[monitor]\nschedule_jitter=-0.0\n"));
    assert_eq!(zero["initial_policy"], negative_zero["initial_policy"]);
    assert!(ROOT.contains("[ranker]\nenabled = false"));
    let changed = inspect(&ROOT.replace("[ranker]\nenabled = false", "[ranker]\nenabled = true"));
    assert_ne!(
        inspect(ROOT)["initial_policy"]["sha256"],
        changed["initial_policy"]["sha256"]
    );
    assert_eq!(
        changed["settings"]["ranker.enabled"]["applicability"],
        "staged-unselected"
    );
}

#[test]
fn durable_inspection_is_explicit_and_cannot_succeed_without_control() {
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("instance.toml");
    // Keep a valid candidate but point its protected credential at an absent file.
    // Offline show still succeeds; inspect must execute its live dependency path.
    fs::write(
        &file,
        ROOT.replace(
            "file:/run/secrets/bigbrotr-control",
            "file:/absent/BIGBROTR_TEST_MISSING_SECRET",
        ),
    )
    .unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_bigbrotr"))
        .args(["config", "inspect", "--file"])
        .arg(file)
        .env_clear()
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(3));
    assert!(output.stdout.is_empty());
    let error = String::from_utf8(output.stderr).unwrap();
    assert!(error.contains("protected PostgreSQL password"), "{error}");
    assert!(!error.contains("BIGBROTR_TEST_MISSING_SECRET"));
}

#[test]
fn durable_initialization_is_explicit_and_cannot_succeed_without_control() {
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("instance.toml");
    // Keep a valid candidate but point its protected credential at an absent file.
    // Offline show still succeeds; inspect must execute its live dependency path.
    fs::write(
        &file,
        ROOT.replace(
            "file:/run/secrets/bigbrotr-control",
            "file:/absent/BIGBROTR_TEST_MISSING_SECRET",
        ),
    )
    .unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_bigbrotr"))
        .args(["config", "initialize", "--file"])
        .arg(file)
        .env_clear()
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(3));
    assert!(output.stdout.is_empty());
    let error = String::from_utf8(output.stderr).unwrap();
    assert!(error.contains("protected PostgreSQL password"), "{error}");
    assert!(!error.contains("BIGBROTR_TEST_MISSING_SECRET"));
}
