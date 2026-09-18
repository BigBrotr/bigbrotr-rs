use std::process::{Command, Stdio};

#[test]
fn missing_malformed_or_foreign_parent_refuses_before_engine_effects() {
    let bundle = std::env::var_os("BIGBROTR_ARCHIVE_TEST_BUNDLE").expect("required pinned bundle");
    // Our parent is not the parent of our child. This gives a real existing but
    // incorrect process identity, without depending on an unused numeric PID.
    let foreign = rustix::process::getppid()
        .unwrap()
        .as_raw_nonzero()
        .to_string();
    for expected in [
        None,
        Some("0"),
        Some("-1"),
        Some("2147483648"),
        Some("invalid"),
        Some(foreign.as_str()),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_bigbrotr-archive-worker"));
        command.env_remove("BIGBROTR_ARCHIVE_PARENT_PID");
        if let Some(expected) = expected {
            command.env("BIGBROTR_ARCHIVE_PARENT_PID", expected);
        }
        let result = command
            .arg("inspect-engine")
            .arg(&bundle)
            .arg(temp.path().canonicalize().unwrap())
            .args(["134217728", "67108864", "1"])
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert!(!result.status.success());
        assert!(result.stdout.is_empty());
        assert_eq!(result.stderr, b"private archive parent unavailable\n");
        assert_eq!(std::fs::read_dir(temp.path()).unwrap().count(), 0);
    }
}
