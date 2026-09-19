//! The installed command cannot obtain executable authority from runtime input.
use std::{fs, process::Command};

#[test]
fn installed_verifier_refuses_missing_bundle_without_trusting_runtime_hashes() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    fs::write(root.join("build-result.json"), br#"{"archive_worker":{"bytes":3,"sha256":"ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"}}"#).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_bigbrotr"))
        .args(["archive", "verify-worker", "--root"])
        .arg(&root)
        .env_clear()
        .env("BIGBROTR_ARCHIVE_WORKER_BYTES", "3")
        .env(
            "BIGBROTR_ARCHIVE_WORKER_SHA256",
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
        )
        .current_dir(&root)
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(4), "{:?}", output);
    assert!(output.stdout.is_empty());
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .contains("archive worker verification refused")
    );
    assert_eq!(fs::read_dir(&root).unwrap().count(), 1);
}

#[test]
fn build_information_explicitly_reports_worker_binding_or_its_absence() {
    let output = Command::new(env!("CARGO_BIN_EXE_bigbrotr"))
        .arg("build-info")
        .env_clear()
        .output()
        .unwrap();
    assert!(output.status.success());
    let info: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let binding = info
        .get("archive_worker")
        .expect("explicit worker binding, even if null");
    if !binding.is_null() {
        assert!(binding["bytes"].as_u64().unwrap() > 0);
        assert_eq!(binding["sha256"].as_str().unwrap().len(), 64);
    }
}
