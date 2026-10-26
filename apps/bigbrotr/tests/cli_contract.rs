use sha2::{Digest, Sha256};
use std::{fs, path::Path, process::Command};

fn run(arguments: &[&str], directory: &Path) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_bigbrotr"))
        .args(arguments)
        .current_dir(directory)
        .env_clear()
        .output()
        .expect("run actual binary without runtime tools or configuration")
}

#[test]
fn help_and_version_work_outside_the_checkout_without_creating_files() {
    let directory = tempfile::tempdir().unwrap();
    for arguments in [["--help"], ["--version"]] {
        let output = run(&arguments, directory.path());
        assert!(output.status.success());
        assert!(output.stderr.is_empty());
        assert!(
            String::from_utf8(output.stdout)
                .unwrap()
                .contains("BigBrotr")
        );
    }
    assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 0);
}

#[test]
fn invalid_input_is_actionable_and_goes_to_stderr() {
    let directory = tempfile::tempdir().unwrap();
    for arguments in [vec!["--not-a-real-option"], vec!["serve"], vec![]] {
        let output = run(&arguments, directory.path());
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        assert!(String::from_utf8(output.stderr).unwrap().contains("Usage:"));
    }
    assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 0);
}

#[test]
fn build_information_binds_actual_inputs_and_exposes_only_implemented_contracts() {
    let directory = tempfile::tempdir().unwrap();
    let output = run(&["build-info"], directory.path());
    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    let info: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(info["format"], "bigbrotr-build/1");
    assert_eq!(info["product"], "BigBrotr");
    assert_eq!(info["version"], env!("CARGO_PKG_VERSION"));
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    for file in [
        "Cargo.lock",
        "rust-toolchain.toml",
        "contracts/release/dependencies.json",
        "contracts/protocol/versions.json",
        "contracts/protocol/nip01-event.json",
    ] {
        let expected: String = Sha256::digest(fs::read(root.join(file)).unwrap())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        assert_eq!(info["inputs_sha256"][file], expected);
    }
    assert!(info["rustc"].as_str().unwrap().contains("rustc 1.98.1"));
    assert!(!info["target"].as_str().unwrap().is_empty());
    assert_eq!(
        info["capabilities"],
        serde_json::json!(["event-verification", "config-static"])
    );
    assert_eq!(info["schemas"]["build_info"], "bigbrotr-build/1");
    assert_eq!(info["schemas"]["signed_event"], "nip01-event/1");
    assert_eq!(info["schemas"]["config_static"], "bigbrotr-config-static/1");
    assert_eq!(info["schemas"]["doctor"], "bigbrotr-doctor/1");
    assert_eq!(
        info["protocol_registry_format"],
        "bigbrotr-protocol-versions/1"
    );
    // The protocol registry owns the scope of the advertised capabilities.
    let versions: serde_json::Value =
        serde_json::from_slice(&fs::read(root.join("contracts/protocol/versions.json")).unwrap())
            .unwrap();
    assert_eq!(
        info["capability_scope"].as_str().unwrap(),
        versions["scope"].as_str().unwrap()
    );
    assert_eq!(
        info["protocols"]["event_verifier"],
        "bigbrotr-nip01-verifier/1"
    );
    assert_eq!(
        info["protocols"]["nostr_nips"],
        "a2494f4f81d46684e5814a9bf35e2b1df978f955"
    );
    assert!(matches!(
        info["source"]["state"].as_str(),
        Some("unrecorded" | "builder-supplied")
    ));
}
