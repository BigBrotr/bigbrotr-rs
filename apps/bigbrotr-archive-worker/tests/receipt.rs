use std::{
    io::Write,
    process::{Command, Stdio},
};

fn run(input: &[u8]) -> std::process::Output {
    let temp = tempfile::tempdir().unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_bigbrotr-archive-worker"))
        .env(
            "BIGBROTR_ARCHIVE_PARENT_PID",
            std::process::id().to_string(),
        )
        .arg("inspect-receipt")
        .arg(std::env::var_os("BIGBROTR_ARCHIVE_TEST_BUNDLE").expect("pinned bundle"))
        .arg(temp.path().canonicalize().unwrap())
        .args(["134217728", "67108864", "1", "16"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(input).unwrap();
    child.wait_with_output().unwrap()
}

#[test]
fn receipt_refuses_invalid_closed_input_without_connecting() {
    for (input, error) in [
        (vec![], "InvalidInput"),
        (b"{}".to_vec(), "InvalidInput"),
        (b"null".to_vec(), "InvalidInput"),
        (vec![b' '; 32769], "ResourceLimit"),
    ] {
        let result = run(&input);
        assert!(!result.status.success());
        assert!(result.stdout.is_empty());
        assert_eq!(
            String::from_utf8(result.stderr).unwrap().trim(),
            format!("private archive receipt failed: {error}")
        );
    }
}
