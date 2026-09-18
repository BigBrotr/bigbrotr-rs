use std::{
    io::Write,
    process::{Command, Stdio},
};

fn wire() -> Vec<u8> {
    let v: serde_json::Value = serde_json::from_str(include_str!(
        "../../../crates/bigbrotr-protocol/tests/fixtures/archive/single.json"
    ))
    .unwrap();
    v["wire_hex"]
        .as_str()
        .unwrap()
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap())
        .collect()
}
fn run(bytes: &[u8]) -> std::process::Output {
    let temp = tempfile::tempdir().unwrap();
    let bundle = std::env::var_os("BIGBROTR_ARCHIVE_TEST_BUNDLE").expect("required pinned bundle");
    let mut child = Command::new(env!("CARGO_BIN_EXE_bigbrotr-archive-worker"))
        .env(
            "BIGBROTR_ARCHIVE_PARENT_PID",
            std::process::id().to_string(),
        )
        .arg("inspect-batch")
        .arg(bundle)
        .arg(temp.path().canonicalize().unwrap())
        .args(["134217728", "67108864", "1", "16", "1048576"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    input.write_all(bytes).unwrap();
    drop(input);
    child.wait_with_output().unwrap()
}
#[test]
fn actual_worker_accepts_complete_batch_and_refuses_truncation_without_a_result() {
    let wire = wire();
    let result = run(&wire);
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(report["format"], "bigbrotr-private-preparation/1");
    assert_eq!(report["facts"], 1);
    assert_eq!(report["staged_counts"], serde_json::json!([0, 1, 0, 0, 0]));
    assert_eq!(report["first"]["sequence"], "2");
    let result = run(&wire[..wire.len() - 1]);
    assert!(!result.status.success());
    assert!(result.stdout.is_empty());
    assert!(
        String::from_utf8(result.stderr)
            .unwrap()
            .contains("Incomplete")
    );
}
