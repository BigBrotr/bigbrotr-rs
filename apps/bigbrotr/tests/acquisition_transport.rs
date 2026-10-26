//! Installed transport roots: offline configuration and actual filesystem/TLS.
use bigbrotr::{
    network::{TlsPolicy, TransportError},
    runtime::{
        OwnedPermit, ProcessEnvelope, ResourceBudget, ResourceLimits, ResourceRequest, WaiterLimits,
    },
};
use serde_json::Value;
use std::{fs, process::Command};

const CA: &[u8] = include_bytes!("../../../tests/fixtures/relay-lifecycle/transport/ca.der");

fn budget() -> ResourceBudget {
    ResourceBudget::new(
        ProcessEnvelope::new(
            8 * 1024 * 1024,
            0,
            ResourceLimits::new(8 * 1024 * 1024, 2, 0, 0),
            WaiterLimits::new(0, 0),
        )
        .unwrap(),
    )
}
fn grant(budget: &ResourceBudget, bytes: u64) -> OwnedPermit {
    budget
        .try_acquire(ResourceRequest::data(bytes, 1, 0).unwrap())
        .unwrap()
}

const ROOT: &str = include_str!("../../../config/recipes/serve-existing.toml");

fn show(extra: &str) -> std::process::Output {
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("instance.toml");
    fs::write(&file, format!("{ROOT}\n{extra}\n")).unwrap();
    Command::new(env!("CARGO_BIN_EXE_bigbrotr"))
        .args(["config", "show", "--file"])
        .arg(file)
        .env_clear()
        .output()
        .unwrap()
}

fn accepted(extra: &str) -> Value {
    let output = show(extra);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

#[test]
fn explicit_dns_root_is_offline_and_preserves_accepted_policy_identity() {
    let original = accepted("");
    assert!(original["settings"]["network.dns.resolver"]["value"].is_null());
    for address in ["127.0.0.1:5353", "[::1]:53", "8.8.8.8:53"] {
        let current = accepted(&format!("[network.dns]\nresolver='{address}'"));
        assert_eq!(original["initial_policy"], current["initial_policy"]);
        let setting = &current["settings"]["network.dns.resolver"];
        assert_eq!(setting["value"], address);
        assert_eq!(setting["deployment_locked"], true);
        assert_eq!(setting["reload"], "restart");
        assert_eq!(setting["applicability"], "staged-unselected");
    }
}

#[test]
fn invalid_numeric_dns_is_refused_even_without_selected_acquisition() {
    for address in [
        "resolver.example:53",
        "127.0.0.1",
        "127.0.0.1:0",
        "0.0.0.0:53",
        "224.0.0.1:53",
        "255.255.255.255:53",
        "[::]:53",
        "[ff02::1]:53",
        "[fe80::1%1]:53",
        "[::ffff:224.0.0.1]:53",
        "CANARY",
    ] {
        let output = show(&format!("[network.dns]\nresolver='{address}'"));
        assert_eq!(output.status.code(), Some(3), "{address}");
        assert!(output.stdout.is_empty());
        assert!(!String::from_utf8_lossy(&output.stderr).contains("CANARY"));
    }
}

#[test]
fn root_paths_are_bounded_without_reading_certificates_offline() {
    let paths = |count| {
        (0..count)
            .map(|n| format!("'/absent/acquisition-{n}.der'"))
            .collect::<Vec<_>>()
            .join(",")
    };
    let original = accepted("");
    let current = accepted(&format!("[network.tls]\nextra_roots=[{}]", paths(128)));
    assert_eq!(original["initial_policy"], current["initial_policy"]);
    assert_eq!(
        show(&format!("[network.tls]\nextra_roots=[{}]", paths(129)))
            .status
            .code(),
        Some(3)
    );
}

#[test]
fn actual_cli_matches_the_generated_transport_consumers() {
    for (command, expected) in [
        (
            "schema",
            include_bytes!("../../../contracts/config/schema.json").as_slice(),
        ),
        (
            "reference",
            include_bytes!("../../../docs/reference/configuration.md").as_slice(),
        ),
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_bigbrotr"))
            .args(["config", command])
            .env_clear()
            .output()
            .unwrap();
        assert!(output.status.success());
        assert_eq!(output.stdout, expected, "{command}");
    }
}

#[test]
fn actual_der_roots_retain_one_grant_through_all_tls_copies() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().canonicalize().unwrap().join("ca.der");
    fs::write(&path, CA).unwrap();
    let budget = budget();
    // Independent exact lower bound; production constants are not the oracle.
    assert!(matches!(
        TlsPolicy::from_der_files(&[&path], grant(&budget, 4_194_303)),
        Err(TransportError::Budget)
    ));
    assert_eq!(budget.snapshot().active_permits(), 0);
    let policy =
        TlsPolicy::from_der_files(&vec![path.as_path(); 128], grant(&budget, 4_194_304)).unwrap();
    let copy = policy.clone();
    drop(policy);
    fs::remove_file(&path).unwrap();
    assert_eq!(budget.snapshot().active_permits(), 1);
    drop(copy);
    assert_eq!(budget.snapshot().active_permits(), 0);
    fs::write(&path, CA).unwrap();
    assert!(matches!(
        TlsPolicy::from_der_files(&vec![path.as_path(); 129], grant(&budget, 4_194_304)),
        Err(TransportError::TlsConfiguration)
    ));
    assert_eq!(budget.snapshot().active_permits(), 0);
}

#[test]
fn malformed_der_and_unsafe_file_kinds_release_the_grant() {
    use std::os::unix::fs::{FileTypeExt, symlink};
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    let path = root.join("ca.der");
    let budget = budget();
    let refuse = |path: &std::path::Path| {
        assert!(
            matches!(
                TlsPolicy::from_der_files(&[path], grant(&budget, 4_194_304)),
                Err(TransportError::TlsConfiguration)
            ),
            "{}",
            path.display()
        );
        assert_eq!(budget.snapshot().active_permits(), 0);
    };
    for bytes in [
        vec![],
        b"bad".to_vec(),
        [CA, &[0]].concat(),
        [CA, CA].concat(),
        b"-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n".to_vec(),
        vec![0; 1_048_577],
    ] {
        fs::write(&path, bytes).unwrap();
        refuse(&path);
    }
    fs::write(&path, CA).unwrap();
    let link = root.join("link");
    symlink(&path, &link).unwrap();
    refuse(&link);
    let parent = root.join("alias");
    symlink(&root, &parent).unwrap();
    refuse(&parent.join("ca.der"));
    let fifo = root.join("fifo");
    // rustix's mknodat is unavailable on Apple targets. Keep the real FIFO
    // refusal on every supported native test host through its POSIX utility.
    let created = Command::new("mkfifo")
        .args(["-m", "400"])
        .arg(&fifo)
        .output()
        .expect("mkfifo is required for the native FIFO refusal test");
    assert!(
        created.status.success(),
        "{}",
        String::from_utf8_lossy(&created.stderr)
    );
    assert!(fs::symlink_metadata(&fifo).unwrap().file_type().is_fifo());
    refuse(&fifo);
    refuse(&root);
    refuse(&root.join("missing"));
    refuse(std::path::Path::new("relative.der"));
    // Errors did not leave stale state or prevent loading a valid root.
    drop(TlsPolicy::from_der_files(&[&path], grant(&budget, 4_194_304)).unwrap());
    assert_eq!(budget.snapshot().active_permits(), 0);
}

#[test]
fn total_der_bytes_are_bounded_independently_of_file_count() {
    // Trust-anchor extraction accepts a lenient serial and does not check a
    // root's self-signature. Enlarge only that ignored field, retaining the
    // original subject/key. This synthetic parser fixture is never a TLS peer
    // credential or a claim that an altered certificate has a valid signature.
    fn split(input: &[u8], tag: u8) -> (&[u8], &[u8]) {
        assert_eq!(input[0], tag);
        let (length, header) = if input[1] < 128 {
            (usize::from(input[1]), 2)
        } else {
            let count = usize::from(input[1] & 127);
            assert!((1..=2).contains(&count));
            (
                input[2..2 + count]
                    .iter()
                    .fold(0, |n, byte| n * 256 + usize::from(*byte)),
                2 + count,
            )
        };
        (&input[header..header + length], &input[header + length..])
    }
    fn der(tag: u8, content: &[u8]) -> Vec<u8> {
        let length = u16::try_from(content.len()).unwrap().to_be_bytes();
        let mut bytes = vec![tag, 0x82, length[0], length[1]];
        bytes.extend_from_slice(content);
        bytes
    }
    let (certificate, trailing) = split(CA, 0x30);
    assert!(trailing.is_empty());
    let (tbs, signature) = split(certificate, 0x30);
    let (_, after_version) = split(tbs, 0xa0);
    let version = &tbs[..tbs.len() - after_version.len()];
    let (_, after_serial) = split(after_version, 2);
    let mut body = version.to_vec();
    body.extend(der(2, &vec![1; 9000]));
    body.extend_from_slice(after_serial);
    let mut certificate = der(0x30, &body);
    certificate.extend_from_slice(signature);
    let certificate = der(0x30, &certificate);
    assert!(certificate.len() * 100 < 1_048_576);
    assert!(certificate.len() * 128 > 1_048_576);
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().canonicalize().unwrap().join("large.der");
    fs::write(&path, certificate).unwrap();
    let budget = budget();
    drop(TlsPolicy::from_der_files(&vec![path.as_path(); 100], grant(&budget, 4_194_304)).unwrap());
    assert!(matches!(
        TlsPolicy::from_der_files(&vec![path.as_path(); 128], grant(&budget, 4_194_304)),
        Err(TransportError::TlsConfiguration)
    ));
    assert_eq!(budget.snapshot().active_permits(), 0);
}
