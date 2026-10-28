//! Process-scoped umask coverage, isolated from parallel in-process flock owners.
//! Keep this target to the single parent/child scenario; CLOEXEC releases an
//! inherited lock only at exec, so a spawning sibling can delay lock release.
#[path = "support/export_files.rs"]
mod fixture;
use bigbrotr::export::{destination::LocalDestination, files::ExportUnit};
use fixture::{ID, cut, destination, input, manifest};
use std::{
    fs,
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::Path,
    sync::{Arc, atomic::AtomicBool},
};

#[test]
fn restrictive_umask_changes_only_new_files_and_directories() {
    const ROOT: &str = "BIGBROTR_EXPORT_FILES_UMASK_TEST_ROOT";
    if let Some(root) = std::env::var_os(ROOT) {
        let destination = Arc::new(
            LocalDestination::open(
                Path::new(&root),
                [1; 16].try_into().unwrap(),
                [0x40; 16].try_into().unwrap(),
            )
            .unwrap(),
        );
        let mut unit = ExportUnit::acquire(destination, ID.parse().unwrap()).unwrap();
        let event = input();
        let cut = cut();
        let m = manifest(&cut, &event);
        unit.write(&m, Some(&event), 4096, 8192, &AtomicBool::new(false))
            .unwrap();
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let (root, _destination) = destination(temp.path());
    let output = std::process::Command::new("/bin/sh").args([
        "-c", "umask 0777; exec \"$1\" --exact restrictive_umask_changes_only_new_files_and_directories",
        "export-files-test"]).arg(std::env::current_exe().unwrap()).env_clear().env(ROOT, &root).output().unwrap();
    assert!(output.status.success(), "{output:?}");
    assert_eq!(fs::metadata(root.join(ID)).unwrap().mode() & 0o7777, 0o700);
    for name in ["events.jsonl", "manifest.json"] {
        assert_eq!(
            fs::metadata(root.join(ID).join(name)).unwrap().mode() & 0o7777,
            0o600
        );
    }
    // Existing permission conflicts are preserved, never repaired by a retry.
    fs::set_permissions(root.join(ID), fs::Permissions::from_mode(0o755)).unwrap();
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "restrictive_umask_changes_only_new_files_and_directories",
        ])
        .env_clear()
        .env(ROOT, &root)
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert_eq!(fs::metadata(root.join(ID)).unwrap().mode() & 0o7777, 0o755);
}
