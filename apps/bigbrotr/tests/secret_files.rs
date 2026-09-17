use bigbrotr::secrets::{SecretError, SecretFile};
use std::{
    fs,
    os::unix::fs::{PermissionsExt, symlink},
    path::Path,
};

fn fixture() -> tempfile::TempDir {
    tempfile::tempdir().unwrap()
}
fn reference(root: &Path, name: &str, content: &[u8], mode: u32) -> String {
    let path = root.canonicalize().unwrap().join(name);
    fs::write(&path, content).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(mode)).unwrap();
    format!("file:{}", path.display())
}

#[test]
fn exact_bytes_include_whitespace_and_debug_never_discloses_them() {
    let dir = fixture();
    let input = reference(dir.path(), "private-canary", b"\0CANARY\n\r ", 0o600);
    let secret = SecretFile::parse(&input).unwrap().read(32).unwrap();
    assert_eq!(secret.as_bytes(), b"\0CANARY\n\r ");
    assert!(secret.allocated_bytes() >= 33);
    assert_eq!(format!("{secret:?}"), "SecretBytes([redacted])");
    assert_eq!(
        format!("{:?}", SecretFile::parse(&input).unwrap()),
        "SecretFile([redacted])"
    );
}

#[test]
fn empty_and_exact_limit_are_preserved_excess_and_invalid_limit_refused() {
    let dir = fixture();
    for bytes in [b"".as_slice(), b"1234".as_slice()] {
        let input = reference(dir.path(), "secret", bytes, 0o400);
        assert_eq!(
            SecretFile::parse(&input)
                .unwrap()
                .read(4)
                .unwrap()
                .as_bytes(),
            bytes
        );
        fs::set_permissions(dir.path().join("secret"), fs::Permissions::from_mode(0o600)).unwrap();
    }
    let input = reference(dir.path(), "secret", b"12345", 0o600);
    let file = SecretFile::parse(&input).unwrap();
    assert_eq!(file.read(4).unwrap_err(), SecretError::TooLarge);
    assert_eq!(file.read(0).unwrap_err(), SecretError::InvalidLimit);
    assert_eq!(
        file.read(usize::MAX).unwrap_err(),
        SecretError::InvalidLimit
    );
}

#[test]
fn group_world_access_and_executable_secret_are_refused() {
    let dir = fixture();
    for mode in [0o640, 0o604, 0o644, 0o700, 0o4600] {
        let input = reference(dir.path(), "secret", b"CANARY", mode);
        assert_eq!(
            SecretFile::parse(&input).unwrap().read(64).unwrap_err(),
            SecretError::Permissions
        );
    }
}

#[test]
fn final_and_parent_symlinks_are_refused_without_following_targets() {
    let dir = fixture();
    let root = dir.path().canonicalize().unwrap();
    reference(&root, "target", b"CANARY", 0o600);
    symlink(root.join("target"), root.join("link")).unwrap();
    fs::create_dir(root.join("real")).unwrap();
    reference(&root.join("real"), "secret", b"CANARY", 0o600);
    symlink(root.join("real"), root.join("alias")).unwrap();
    for path in [root.join("link"), root.join("alias/secret")] {
        assert_eq!(
            SecretFile::parse(&format!("file:{}", path.display()))
                .unwrap()
                .read(64)
                .unwrap_err(),
            SecretError::Open
        );
    }
    assert_eq!(fs::read(root.join("target")).unwrap(), b"CANARY");
}

#[test]
fn fifo_directory_and_device_are_refused_without_waiting_for_a_writer() {
    let dir = fixture();
    let root = dir.path().canonicalize().unwrap();
    assert!(
        std::process::Command::new("mkfifo")
            .arg(root.join("fifo"))
            .status()
            .unwrap()
            .success()
    );
    for path in [
        root.join("fifo"),
        root.clone(),
        Path::new("/dev/null").to_owned(),
    ] {
        let name = format!("file:{}", path.display());
        let (send, receive) = std::sync::mpsc::sync_channel(1);
        let worker = std::thread::spawn(move || {
            send.send(SecretFile::parse(&name).unwrap().read(64).unwrap_err())
                .unwrap();
        });
        assert_eq!(
            receive
                .recv_timeout(std::time::Duration::from_secs(2))
                .expect("special-file refusal must not wait for a peer"),
            SecretError::NotRegular
        );
        worker.join().unwrap();
    }
}

#[test]
fn named_reference_syntax_is_bounded_and_errors_do_not_echo_inputs() {
    for value in [
        "secret",
        "env:CANARY",
        "file:relative",
        "file:/",
        "file:/a/",
        "file:/a/../b",
        "file:/./b",
        "file:/a\0b",
        "file:/a\nb",
    ] {
        let error = SecretFile::parse(value).unwrap_err();
        assert_eq!(error, SecretError::Reference);
        assert_eq!(
            error.to_string(),
            "invalid bounded absolute local-file secret reference"
        );
    }
    assert_eq!(
        SecretFile::parse(&format!("file:/{}", "a".repeat(8193))).unwrap_err(),
        SecretError::Reference
    );
    let missing = SecretFile::parse("file:/does-not-exist/CANARY")
        .unwrap()
        .read(64)
        .unwrap_err();
    assert_eq!(missing, SecretError::Open);
    assert!(!missing.to_string().contains("CANARY"));
}

#[test]
fn readable_foreign_owned_file_is_refused_before_content_read() {
    use std::os::unix::fs::MetadataExt;
    let path = Path::new("/etc/passwd").canonicalize().unwrap();
    assert_ne!(
        path.metadata().unwrap().uid(),
        rustix::process::geteuid().as_raw(),
        "this ownership scenario requires an unprivileged test account"
    );
    let name = format!("file:{}", path.display());
    assert_eq!(
        SecretFile::parse(&name).unwrap().read(65536).unwrap_err(),
        SecretError::Owner
    );
}
