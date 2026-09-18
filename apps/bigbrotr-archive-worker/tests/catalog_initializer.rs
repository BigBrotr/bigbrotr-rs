use std::process::{Command, Stdio};

#[test]
fn initializer_has_a_closed_private_input_boundary() {
    let temp = tempfile::tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_bigbrotr-archive-worker"))
        .env(
            "BIGBROTR_ARCHIVE_PARENT_PID",
            std::process::id().to_string(),
        )
        .args(["initialize-catalog"])
        .arg(std::env::var_os("BIGBROTR_ARCHIVE_TEST_BUNDLE").expect("pinned bundle"))
        .arg(temp.path().canonicalize().unwrap())
        .args(["134217728", "67108864", "1"])
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert_eq!(
        String::from_utf8(output.stderr).unwrap().trim(),
        "archive catalog bootstrap failed: InvalidInput"
    );
}
